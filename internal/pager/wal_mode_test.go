// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestWALModeSwitch tests switching to and from WAL mode
func TestWALModeSwitch(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create pager with delete journal mode
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	// Verify initial mode
	if pager.GetJournalMode() != JournalModeDelete {
		t.Errorf("Expected delete mode, got %d", pager.GetJournalMode())
	}

	// Switch to WAL mode
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	// Verify WAL mode is active
	if pager.GetJournalMode() != JournalModeWAL {
		t.Errorf("Expected WAL mode, got %d", pager.GetJournalMode())
	}

	// Verify WAL files were created
	walFile := dbFile + "-wal"
	shmFile := dbFile + "-shm"

	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Errorf("WAL file was not created")
	}

	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Errorf("WAL index file was not created")
	}

	// Switch back to delete mode
	if err := pager.SetJournalMode(JournalModeDelete); err != nil {
		t.Fatalf("Failed to switch back to delete mode: %v", err)
	}

	// Verify mode changed
	if pager.GetJournalMode() != JournalModeDelete {
		t.Errorf("Expected delete mode after switch, got %d", pager.GetJournalMode())
	}
}

// TestWALModeWriteRead tests writing and reading in WAL mode
func TestWALModeWriteRead(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)
	mustSetJournalMode(t, p, JournalModeWAL)

	testData := []byte("Hello WAL mode!")
	pgno := walAllocWriteCommit(t, p, testData)

	// Read page back
	page := mustGetPage(t, p, pgno)
	defer p.Put(page)

	if string(page.Data[:len(testData)]) != string(testData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, page.Data[:len(testData)])
	}
}

// walCheckpointVerifyPages verifies page data after checkpoint.
func walCheckpointVerifyPages(t *testing.T, p *Pager, pageNumbers []Pgno) {
	t.Helper()
	for i, pgno := range pageNumbers {
		page := mustGetPage(t, p, pgno)
		for j := 0; j < len(page.Data); j++ {
			expected := byte((i + j) % 256)
			if page.Data[j] != expected {
				t.Errorf("Page %d data mismatch at offset %d: expected %d, got %d", pgno, j, expected, page.Data[j])
				break
			}
		}
		p.Put(page)
	}
}

// TestWALModeCheckpoint tests checkpointing in WAL mode
func TestWALModeCheckpoint(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)

	// Write multiple pages
	pageNumbers := writeTestPages(t, p, 10)

	if p.wal.FrameCount() == 0 {
		t.Errorf("Expected WAL to have frames")
	}

	mustCheckpoint(t, p)
	p.Close()

	pager2 := openTestPagerAt(t, dbFile, false)
	defer pager2.Close()

	walCheckpointVerifyPages(t, pager2, pageNumbers)
}

// TestWALModeRecovery tests recovery from WAL on database open
func TestWALModeRecovery(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	p := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p, JournalModeWAL)

	testData := []byte("Recovery test data")
	pgno := walAllocWriteCommit(t, p, testData)
	p.Close()

	// Reopen database - should recover from WAL
	pager2 := openTestPagerAt(t, dbFile, false)
	defer pager2.Close()

	page2 := mustGetPage(t, pager2, pgno)
	defer pager2.Put(page2)

	if string(page2.Data[:len(testData)]) != string(testData) {
		t.Errorf("Data not recovered correctly: expected %q, got %q", testData, page2.Data[:len(testData)])
	}
}

// walConcurrentReader reads all pages and verifies data, sending errors to channel.
func walConcurrentReader(dbFile string, pageNumbers []Pgno, errors chan<- error) {
	readerPager, err := Open(dbFile, true)
	if err != nil {
		errors <- err
		return
	}
	defer readerPager.Close()

	for i, pgno := range pageNumbers {
		page, err := readerPager.Get(pgno)
		if err != nil {
			errors <- err
			return
		}
		for j := 0; j < len(page.Data); j++ {
			expected := byte((i + j) % 256)
			if page.Data[j] != expected {
				errors <- fmt.Errorf("page %d mismatch at %d", pgno, j)
				readerPager.Put(page)
				return
			}
		}
		readerPager.Put(page)
		time.Sleep(1 * time.Millisecond)
	}
}

// TestWALModeConcurrentReads tests concurrent readers in WAL mode
func TestWALModeConcurrentReads(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	p := openTestPagerAt(t, dbFile, false)
	mustSetJournalMode(t, p, JournalModeWAL)
	pageNumbers := writeTestPages(t, p, 10)
	p.Close()

	numReaders := 5
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for reader := 0; reader < numReaders; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			walConcurrentReader(dbFile, pageNumbers, errors)
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Reader error: %v", err)
	}
}

// TestWALModeMultipleVersions tests reading the latest version of a page
func TestWALModeMultipleVersions(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)
	mustSetJournalMode(t, p, JournalModeWAL)

	// Allocate a page with first version
	pgno := walAllocWriteCommit(t, p, []byte("Version 1"))

	// Write second and third versions
	walWriteAndCommit(t, p, pgno, []byte("Version 2"))
	version3 := []byte("Version 3")
	walWriteAndCommit(t, p, pgno, version3)

	// Read page and verify we get the latest version
	page := mustGetPage(t, p, pgno)
	defer p.Put(page)

	if string(page.Data[:len(version3)]) != string(version3) {
		t.Errorf("Expected latest version %q, got %q", version3, page.Data[:len(version3)])
	}
}

// walCheckpointTest represents a declarative WAL checkpoint test
type walCheckpointTest struct {
	name        string
	mode        CheckpointMode
	writeAfter  bool
	verifyTrunc bool
}

// walWritePages writes test pages to the pager
func walWritePages(t *testing.T, pager *Pager, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if err := pager.BeginWrite(); err != nil {
			t.Fatalf("Failed to begin write: %v", err)
		}

		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}

		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}

		for j := 0; j < len(page.Data); j++ {
			page.Data[j] = byte((i + j) % 256)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
		pager.Put(page)

		if err := pager.Commit(); err != nil {
			t.Fatalf("Failed to commit: %v", err)
		}
	}
}

// walExecCheckpoint executes a checkpoint and verifies it
func walExecCheckpoint(t *testing.T, pager *Pager, mode CheckpointMode, dbFile string, verifyTrunc bool) {
	t.Helper()
	if err := pager.CheckpointMode(mode); err != nil {
		t.Errorf("%v checkpoint failed: %v", mode, err)
	}

	if verifyTrunc {
		walFile := dbFile + "-wal"
		info, err := os.Stat(walFile)
		if err != nil {
			t.Fatalf("WAL file should exist: %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("WAL file should be empty, got size %d", info.Size())
		}
	}
}

// TestWALModeCheckpointModes tests different checkpoint modes
func TestWALModeCheckpointModes(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("Failed to switch to WAL mode: %v", err)
	}

	walWritePages(t, pager, 5)

	tests := []walCheckpointTest{
		{name: "Passive", mode: CheckpointPassive, writeAfter: false, verifyTrunc: false},
		{name: "Full", mode: CheckpointFull, writeAfter: false, verifyTrunc: false},
		{name: "Restart", mode: CheckpointRestart, writeAfter: true, verifyTrunc: false},
		{name: "Truncate", mode: CheckpointTruncate, writeAfter: false, verifyTrunc: true},
	}

	for _, tt := range tests {
		walExecCheckpoint(t, pager, tt.mode, dbFile, tt.verifyTrunc)

		if tt.writeAfter {
			walWritePages(t, pager, 1)
		}
	}
}

// TestWALModeRollback tests rollback in WAL mode
func TestWALModeRollback(t *testing.T) {
	t.Parallel()

	p := openTestPager(t)
	mustSetJournalMode(t, p, JournalModeWAL)

	originalData := []byte("Original data")
	pgno := walAllocWriteCommit(t, p, originalData)

	// Start new transaction and modify data
	walWriteAndCommit_noCommit(t, p, pgno, []byte("Modified data"))

	// Rollback should succeed without error
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	// After rollback, pager should be in a clean state
	if p.state != PagerStateOpen {
		t.Errorf("pager state after rollback = %d, want %d (PagerStateOpen)", p.state, PagerStateOpen)
	}

	// Should be able to read the page without error
	page, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d) after rollback error = %v", pgno, err)
	}
	p.Put(page)

	p.Close()
}

// walWriteAndCommit_noCommit writes data to a page but does NOT commit.
func walWriteAndCommit_noCommit(t *testing.T, p *Pager, pgno Pgno, data []byte) {
	t.Helper()
	mustBeginWrite(t, p)
	page := mustGetPage(t, p, pgno)
	copy(page.Data, data)
	mustWritePage(t, p, page)
	p.Put(page)
}
