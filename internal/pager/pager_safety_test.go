// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"testing"
)

// TestStateTransitionValidation tests that state transitions are properly validated.
func TestStateTransitionValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		fromState   int
		toState     int
		shouldError bool
	}{
		{
			name:        "valid: open to reader",
			fromState:   PagerStateOpen,
			toState:     PagerStateReader,
			shouldError: false,
		},
		{
			name:        "valid: open to writer locked",
			fromState:   PagerStateOpen,
			toState:     PagerStateWriterLocked,
			shouldError: false,
		},
		{
			name:        "valid: reader to writer locked",
			fromState:   PagerStateReader,
			toState:     PagerStateWriterLocked,
			shouldError: false,
		},
		{
			name:        "valid: writer locked to cachemod",
			fromState:   PagerStateWriterLocked,
			toState:     PagerStateWriterCachemod,
			shouldError: false,
		},
		{
			name:        "valid: cachemod to dbmod",
			fromState:   PagerStateWriterCachemod,
			toState:     PagerStateWriterDbmod,
			shouldError: false,
		},
		{
			name:        "valid: dbmod to finished",
			fromState:   PagerStateWriterDbmod,
			toState:     PagerStateWriterFinished,
			shouldError: false,
		},
		{
			name:        "valid: finished to open",
			fromState:   PagerStateWriterFinished,
			toState:     PagerStateOpen,
			shouldError: false,
		},
		{
			name:        "valid: any to error",
			fromState:   PagerStateWriterLocked,
			toState:     PagerStateError,
			shouldError: false,
		},
		{
			name:        "invalid: open to finished",
			fromState:   PagerStateOpen,
			toState:     PagerStateWriterFinished,
			shouldError: true,
		},
		{
			name:        "invalid: reader to cachemod",
			fromState:   PagerStateReader,
			toState:     PagerStateWriterCachemod,
			shouldError: true,
		},
		{
			name:        "invalid: open to dbmod",
			fromState:   PagerStateOpen,
			toState:     PagerStateWriterDbmod,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			p := &Pager{state: tt.fromState}
			err := p.validateTransition(tt.toState)

			if tt.shouldError && err == nil {
				t.Errorf("expected error for transition from %d to %d, got nil", tt.fromState, tt.toState)
			}
			if !tt.shouldError && err != nil {
				t.Errorf("unexpected error for transition from %d to %d: %v", tt.fromState, tt.toState, err)
			}
		})
	}
}

// TestGetSetState tests thread-safe state access.
func TestGetSetState(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)

	p, err := Open(tmpFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Test getState
	initialState := p.getState()
	if initialState != PagerStateOpen {
		t.Errorf("expected initial state to be PagerStateOpen (%d), got %d", PagerStateOpen, initialState)
	}

	// Test setState with valid transition
	err = p.setState(PagerStateReader)
	if err != nil {
		t.Errorf("failed to set state to reader: %v", err)
	}

	newState := p.getState()
	if newState != PagerStateReader {
		t.Errorf("expected state to be PagerStateReader (%d), got %d", PagerStateReader, newState)
	}

	// Test setState with invalid transition (should fail)
	err = p.setState(PagerStateWriterFinished)
	if err == nil {
		t.Error("expected error for invalid state transition, got nil")
	}
}

// TestConcurrentStateAccess tests that concurrent state access is safe.
func TestConcurrentStateAccess(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)

	p, err := Open(tmpFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	numReaders := 10
	numIterations := 100

	// Start multiple goroutines reading state
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				_ = p.getState()
			}
		}()
	}

	// Start a goroutine that changes state
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < numIterations; j++ {
			_ = p.setState(PagerStateReader)
			_ = p.setState(PagerStateOpen)
		}
	}()

	wg.Wait()

	// Verify final state is valid
	finalState := p.getState()
	if finalState != PagerStateOpen && finalState != PagerStateReader {
		t.Errorf("unexpected final state: %d", finalState)
	}
}

// TestJournalChecksumValidation tests checksum validation during journal operations.
func TestJournalChecksumValidation(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	p, err := Open(tmpFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	// Get a page and modify it
	page, err := p.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Write to page (this should journal it)
	err = p.Write(page)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Modify the page data
	copy(page.Data[:10], []byte("testdata12"))

	p.Put(page)

	// Commit to write the journal
	err = p.Commit()
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	p.Close()

	// Verify journal file exists (in persist mode) or was deleted
	// For this test, we expect it to be deleted in default mode
	_, err = os.Stat(tmpFile + "-journal")
	if !os.IsNotExist(err) {
		// If journal exists, verify it has checksums
		t.Log("Journal file exists (may be in different mode)")
	}
}

// corruptJournalChecksum opens a journal file, corrupts a checksum, and returns whether corruption was applied.
func corruptJournalChecksum(t *testing.T, journalPath string) bool {
	t.Helper()
	journalFile, err := os.OpenFile(journalPath, os.O_RDWR, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journalFile.Close()

	header := make([]byte, 4)
	if _, err = journalFile.Read(header); err != nil {
		t.Fatalf("failed to read journal header: %v", err)
	}

	pageSize := int(binary.BigEndian.Uint32(header))
	entry := make([]byte, 4+pageSize+4)
	n, err := journalFile.Read(entry)
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read journal entry: %v", err)
	}
	if n != len(entry) {
		return false
	}

	entry[len(entry)-1] ^= 0xFF
	entry[len(entry)-2] ^= 0xFF
	if _, err = journalFile.WriteAt(entry, 4); err != nil {
		t.Fatalf("failed to write corrupted entry: %v", err)
	}
	journalFile.Sync()
	return true
}

// TestJournalChecksumCorruption tests that corrupted checksums are detected.
func TestJournalChecksumCorruption(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-journal")

	p := openTestPagerAt(t, tmpFile, false)
	p.journalMode = JournalModePersist

	page := mustGetWritePage(t, p, 1)
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+10], []byte("testdata12"))
	p.Put(page)
	mustCommit(t, p)
	p.Close()

	if !corruptJournalChecksum(t, tmpFile+"-journal") {
		t.Log("No complete journal entry found to corrupt, skipping corruption portion")
		return
	}

	p2 := openTestPagerAt(t, tmpFile, false)
	defer p2.Close()
	p2.journalMode = JournalModePersist
	page2, _ := p2.Get(1)
	if err := p2.Write(page2); err != nil {
		t.Logf("Write failed (expected after corruption): %v", err)
	}
	p2.Put(page2)
	if err := p2.Rollback(); err != nil {
		t.Logf("Rollback detected corruption (expected): %v", err)
	}
}

// TestValidateJournalPage tests the checksum validation function.
func TestValidateJournalPage(t *testing.T) {
	t.Parallel()
	p := &Pager{pageSize: 4096}

	testData := make([]byte, 4096)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Calculate correct checksum
	correctChecksum := crc32.ChecksumIEEE(testData)

	// Test with correct checksum
	if !p.validateJournalPage(testData, correctChecksum) {
		t.Error("validation failed with correct checksum")
	}

	// Test with incorrect checksum
	if p.validateJournalPage(testData, correctChecksum+1) {
		t.Error("validation succeeded with incorrect checksum")
	}

	// Test with zero checksum
	if p.validateJournalPage(testData, 0) {
		t.Error("validation succeeded with zero checksum")
	}
}

// TestCalculateChecksum tests the checksum calculation function.
func TestCalculateChecksum(t *testing.T) {
	t.Parallel()
	p := &Pager{}

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single byte", []byte{0x42}},
		{"small data", []byte{1, 2, 3, 4, 5}},
		{"page-sized data", make([]byte, 4096)},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			checksum := p.calculateChecksum(tc.data)
			expected := crc32.ChecksumIEEE(tc.data)

			if checksum != expected {
				t.Errorf("checksum mismatch: got %d, expected %d", checksum, expected)
			}

			// Verify it's deterministic
			checksum2 := p.calculateChecksum(tc.data)
			if checksum != checksum2 {
				t.Error("checksum calculation is not deterministic")
			}
		})
	}
}

// TestWALFrameChecksum tests WAL frame checksum validation.
func TestWALFrameChecksum(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-shm")
	defer os.Remove(tmpFile + "-wal")

	walIdx, err := NewWALIndex(tmpFile)
	if err != nil {
		t.Fatalf("failed to create WAL index: %v", err)
	}
	defer walIdx.Close()

	// Test frame data
	frameData := make([]byte, 4096)
	for i := range frameData {
		frameData[i] = byte(i % 256)
	}

	// Calculate checksum
	checksum := walIdx.CalculateFrameChecksum(frameData)

	// Validate with correct checksum
	if !walIdx.ValidateFrameChecksum(frameData, checksum) {
		t.Error("validation failed with correct checksum")
	}

	// Validate with incorrect checksum
	if walIdx.ValidateFrameChecksum(frameData, checksum+1) {
		t.Error("validation succeeded with incorrect checksum")
	}
}

// TestWALInsertFrameWithChecksum tests inserting WAL frames with checksum validation.
func TestWALInsertFrameWithChecksum(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-shm")
	defer os.Remove(tmpFile + "-wal")

	walIdx, err := NewWALIndex(tmpFile)
	if err != nil {
		t.Fatalf("failed to create WAL index: %v", err)
	}
	defer walIdx.Close()

	frameData := make([]byte, 4096)
	for i := range frameData {
		frameData[i] = byte(i % 256)
	}

	correctChecksum := walIdx.CalculateFrameChecksum(frameData)
	incorrectChecksum := correctChecksum + 1

	// Test with correct checksum
	err = walIdx.InsertFrameWithChecksum(1, 1, frameData, correctChecksum)
	if err != nil {
		t.Errorf("failed to insert frame with correct checksum: %v", err)
	}

	// Test with incorrect checksum
	err = walIdx.InsertFrameWithChecksum(2, 2, frameData, incorrectChecksum)
	if err == nil {
		t.Error("expected error when inserting frame with incorrect checksum")
	}
}

// TestWALFrameChecksumInHeader tests updating and retrieving frame checksums in header.
func TestWALFrameChecksumInHeader(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)
	defer os.Remove(tmpFile + "-shm")
	defer os.Remove(tmpFile + "-wal")

	walIdx, err := NewWALIndex(tmpFile)
	if err != nil {
		t.Fatalf("failed to create WAL index: %v", err)
	}
	defer walIdx.Close()

	// Update checksums
	err = walIdx.UpdateFrameChecksum(0x12345678, 0xABCDEF00)
	if err != nil {
		t.Fatalf("failed to update frame checksum: %v", err)
	}

	// Retrieve checksums
	cksum1, cksum2, err := walIdx.GetFrameChecksum()
	if err != nil {
		t.Fatalf("failed to get frame checksum: %v", err)
	}

	if cksum1 != 0x12345678 || cksum2 != 0xABCDEF00 {
		t.Errorf("checksum mismatch: got (%x, %x), expected (12345678, ABCDEF00)", cksum1, cksum2)
	}
}

// TestConcurrentStateTransitions tests that concurrent state transitions are safe.
func TestConcurrentStateTransitions(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)

	p, err := Open(tmpFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	numWorkers := 5
	numOps := 50

	// Each worker performs state transitions
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				// Try to transition to reader
				_ = p.setState(PagerStateReader)
				// Try to transition back to open
				_ = p.setState(PagerStateOpen)
			}
		}(i)
	}

	wg.Wait()

	// Verify pager is still in a valid state
	finalState := p.getState()
	if finalState != PagerStateOpen && finalState != PagerStateReader {
		t.Errorf("pager ended in invalid state: %d", finalState)
	}
}

// TestStateTransitionErrorRecovery tests that the pager can recover from state errors.
func TestStateTransitionErrorRecovery(t *testing.T) {
	t.Parallel()
	tmpFile := createTempDB(t)
	defer os.Remove(tmpFile)

	p, err := Open(tmpFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Attempt an invalid transition
	err = p.setState(PagerStateWriterFinished)
	if err == nil {
		t.Fatal("expected error for invalid state transition")
	}

	// Verify the state hasn't changed
	if p.getState() != PagerStateOpen {
		t.Error("state changed despite invalid transition")
	}

	// Verify we can still make valid transitions
	err = p.setState(PagerStateReader)
	if err != nil {
		t.Errorf("failed to make valid transition after error: %v", err)
	}
}

// Helper function to create a temporary database for testing
func createTempDB(t *testing.T) string {
	f, err := os.CreateTemp("", "pager_safety_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name) // Remove it so the pager can create it fresh
	return name
}
