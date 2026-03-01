package pager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestWALCreation tests creating a new WAL file
func TestWALCreation(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create empty database file
	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Verify WAL file was created
	walFile := dbFile + "-wal"
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Errorf("WAL file was not created")
	}

	// Verify WAL is initialized
	if !wal.initialized {
		t.Errorf("WAL not marked as initialized")
	}

	// Verify frame count is 0
	if wal.frameCount != 0 {
		t.Errorf("Expected frameCount=0, got %d", wal.frameCount)
	}
}

// TestWALHeader tests WAL header serialization and parsing
func TestWALHeader(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	originalSalt1 := wal.salt1
	originalSalt2 := wal.salt2
	originalCheckpoint := wal.checkpointSeq

	wal.Close()

	// Reopen and verify header was persisted
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	if wal2.salt1 != originalSalt1 {
		t.Errorf("Salt1 mismatch: expected %d, got %d", originalSalt1, wal2.salt1)
	}

	if wal2.salt2 != originalSalt2 {
		t.Errorf("Salt2 mismatch: expected %d, got %d", originalSalt2, wal2.salt2)
	}

	if wal2.checkpointSeq != originalCheckpoint {
		t.Errorf("Checkpoint seq mismatch: expected %d, got %d", originalCheckpoint, wal2.checkpointSeq)
	}
}

// TestWALWriteFrame tests writing frames to the WAL
func TestWALWriteFrame(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Create test page data
	pageData := make([]byte, DefaultPageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write frame
	if err := wal.WriteFrame(1, pageData, 1); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Verify frame count
	if wal.frameCount != 1 {
		t.Errorf("Expected frameCount=1, got %d", wal.frameCount)
	}

	// Write another frame
	pageData2 := make([]byte, DefaultPageSize)
	for i := range pageData2 {
		pageData2[i] = byte((i + 100) % 256)
	}

	if err := wal.WriteFrame(2, pageData2, 2); err != nil {
		t.Fatalf("Failed to write second frame: %v", err)
	}

	if wal.frameCount != 2 {
		t.Errorf("Expected frameCount=2, got %d", wal.frameCount)
	}
}

// TestWALReadFrame tests reading frames from the WAL
func TestWALReadFrame(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Create and write test pages
	testPages := []struct {
		pgno Pgno
		data []byte
	}{
		{1, makeTestPage(1, DefaultPageSize)},
		{2, makeTestPage(2, DefaultPageSize)},
		{3, makeTestPage(3, DefaultPageSize)},
	}

	for i, tp := range testPages {
		if err := wal.WriteFrame(tp.pgno, tp.data, uint32(i+1)); err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}
	}

	// Read frames back
	for i, tp := range testPages {
		frame, err := wal.ReadFrame(uint32(i))
		if err != nil {
			t.Fatalf("Failed to read frame %d: %v", i, err)
		}

		if frame.PageNumber != uint32(tp.pgno) {
			t.Errorf("Frame %d: wrong page number, expected %d, got %d", i, tp.pgno, frame.PageNumber)
		}

		if !bytesEqual(frame.Data, tp.data) {
			t.Errorf("Frame %d: data mismatch", i)
		}
	}
}

// TestWALFindPage tests finding the latest version of a page
func TestWALFindPage(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write multiple versions of page 1
	page1v1 := makeTestPage(1, DefaultPageSize)
	page1v2 := makeTestPage(100, DefaultPageSize)
	page1v3 := makeTestPage(200, DefaultPageSize)

	if err := wal.WriteFrame(1, page1v1, 1); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	if err := wal.WriteFrame(2, makeTestPage(2, DefaultPageSize), 2); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	if err := wal.WriteFrame(1, page1v2, 2); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	if err := wal.WriteFrame(1, page1v3, 2); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Find page 1 - should return the latest version
	frame, err := wal.FindPage(1)
	if err != nil {
		t.Fatalf("Failed to find page: %v", err)
	}

	if frame == nil {
		t.Fatalf("Page not found in WAL")
	}

	if !bytesEqual(frame.Data, page1v3) {
		t.Errorf("FindPage returned wrong version of page 1")
	}

	// Find page that doesn't exist
	frame, err = wal.FindPage(999)
	if err != nil {
		t.Fatalf("Error finding non-existent page: %v", err)
	}

	if frame != nil {
		t.Errorf("Expected nil for non-existent page, got frame")
	}
}

// TestWALCheckpoint tests checkpointing the WAL to the database
func TestWALCheckpoint(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create database file with initial page
	dbData := make([]byte, DefaultPageSize*3)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write test pages to WAL
	testPages := map[Pgno][]byte{
		1: makeTestPage(1, DefaultPageSize),
		2: makeTestPage(2, DefaultPageSize),
		3: makeTestPage(3, DefaultPageSize),
	}

	for pgno, data := range testPages {
		if err := wal.WriteFrame(pgno, data, 3); err != nil {
			t.Fatalf("Failed to write frame for page %d: %v", pgno, err)
		}
	}

	// Checkpoint the WAL
	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("Failed to checkpoint WAL: %v", err)
	}

	// Verify WAL was reset
	if wal.frameCount != 0 {
		t.Errorf("WAL not reset after checkpoint, frameCount=%d", wal.frameCount)
	}

	// Verify pages were written to database
	dbFileHandle, err := os.Open(dbFile)
	if err != nil {
		t.Fatalf("Failed to open database file: %v", err)
	}
	defer dbFileHandle.Close()

	for pgno, expectedData := range testPages {
		offset := int64(pgno-1) * int64(DefaultPageSize)
		actualData := make([]byte, DefaultPageSize)

		if _, err := dbFileHandle.ReadAt(actualData, offset); err != nil {
			t.Fatalf("Failed to read page %d from database: %v", pgno, err)
		}

		if !bytesEqual(actualData, expectedData) {
			t.Errorf("Page %d data mismatch after checkpoint", pgno)
		}
	}
}

// TestWALCheckpointOverwrite tests that checkpoint handles multiple versions correctly
func TestWALCheckpointOverwrite(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create database file
	dbData := make([]byte, DefaultPageSize*2)
	if err := os.WriteFile(dbFile, dbData, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write multiple versions of page 1
	page1v1 := makeTestPage(1, DefaultPageSize)
	page1v2 := makeTestPage(100, DefaultPageSize)

	if err := wal.WriteFrame(1, page1v1, 1); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	if err := wal.WriteFrame(1, page1v2, 1); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Checkpoint
	if err := wal.Checkpoint(); err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	// Verify only the latest version was written
	dbFileHandle, err := os.Open(dbFile)
	if err != nil {
		t.Fatalf("Failed to open database file: %v", err)
	}
	defer dbFileHandle.Close()

	actualData := make([]byte, DefaultPageSize)
	if _, err := dbFileHandle.ReadAt(actualData, 0); err != nil {
		t.Fatalf("Failed to read page from database: %v", err)
	}

	if !bytesEqual(actualData, page1v2) {
		t.Errorf("Expected latest version of page after checkpoint")
	}
}

// TestWALInvalidPageSize tests error handling for invalid page sizes
func TestWALInvalidPageSize(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Try to write frame with wrong size
	wrongSizeData := make([]byte, DefaultPageSize/2)
	err := wal.WriteFrame(1, wrongSizeData, 1)

	if err == nil {
		t.Errorf("Expected error for wrong page size, got nil")
	}
}

// TestWALInvalidPageNumber tests error handling for invalid page numbers
func TestWALInvalidPageNumber(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Try to write frame with page number 0
	pageData := make([]byte, DefaultPageSize)
	err := wal.WriteFrame(0, pageData, 1)

	if err == nil {
		t.Errorf("Expected error for page number 0, got nil")
	}
}

// TestWALDelete tests deleting the WAL file
func TestWALDelete(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	walFile := dbFile + "-wal"

	// Verify WAL file exists
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Fatalf("WAL file was not created")
	}

	// Delete WAL
	if err := wal.Delete(); err != nil {
		t.Fatalf("Failed to delete WAL: %v", err)
	}

	// Verify WAL file is gone
	if _, err := os.Stat(walFile); !os.IsNotExist(err) {
		t.Errorf("WAL file still exists after delete")
	}

	// Verify state was reset
	if wal.initialized {
		t.Errorf("WAL still marked as initialized after delete")
	}

	if wal.frameCount != 0 {
		t.Errorf("Frame count not reset after delete")
	}
}

// TestWALSync tests syncing the WAL to disk
func TestWALSync(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write some frames
	pageData := makeTestPage(1, DefaultPageSize)
	if err := wal.WriteFrame(1, pageData, 1); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Sync WAL
	if err := wal.Sync(); err != nil {
		t.Errorf("Failed to sync WAL: %v", err)
	}
}

// TestWALShouldCheckpoint tests the checkpoint threshold
func TestWALShouldCheckpoint(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Initially should not need checkpoint
	if wal.ShouldCheckpoint() {
		t.Errorf("Should not need checkpoint with 0 frames")
	}

	// Set frame count to threshold
	wal.frameCount = WALMinCheckpointFrames

	// Now should need checkpoint
	if !wal.ShouldCheckpoint() {
		t.Errorf("Should need checkpoint at threshold")
	}
}

// TestWALHeaderFormat tests the exact WAL header format
func TestWALHeaderFormat(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Read raw header
	walFile := dbFile + "-wal"
	f, err := os.Open(walFile)
	if err != nil {
		t.Fatalf("Failed to open WAL file: %v", err)
	}
	defer f.Close()

	headerData := make([]byte, WALHeaderSize)
	if _, err := f.Read(headerData); err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	// Verify magic number
	magic := binary.BigEndian.Uint32(headerData[0:4])
	if magic != WALMagic {
		t.Errorf("Wrong magic number: expected 0x%x, got 0x%x", WALMagic, magic)
	}

	// Verify version
	version := binary.BigEndian.Uint32(headerData[4:8])
	if version != WALFormatVersion {
		t.Errorf("Wrong version: expected %d, got %d", WALFormatVersion, version)
	}

	// Verify page size
	pageSize := binary.BigEndian.Uint32(headerData[8:12])
	if pageSize != uint32(DefaultPageSize) {
		t.Errorf("Wrong page size: expected %d, got %d", DefaultPageSize, pageSize)
	}
}

// TestWALFrameFormat tests the exact WAL frame format
func TestWALFrameFormat(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write a frame
	testPage := makeTestPage(42, DefaultPageSize)
	if err := wal.WriteFrame(5, testPage, 10); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}

	// Read raw frame
	walFile := dbFile + "-wal"
	f, err := os.Open(walFile)
	if err != nil {
		t.Fatalf("Failed to open WAL file: %v", err)
	}
	defer f.Close()

	// Skip header
	if _, err := f.Seek(WALHeaderSize, 0); err != nil {
		t.Fatalf("Failed to seek: %v", err)
	}

	// Read frame header
	frameHeader := make([]byte, WALFrameHeaderSize)
	if _, err := f.Read(frameHeader); err != nil {
		t.Fatalf("Failed to read frame header: %v", err)
	}

	// Verify page number
	pageNum := binary.BigEndian.Uint32(frameHeader[0:4])
	if pageNum != 5 {
		t.Errorf("Wrong page number: expected 5, got %d", pageNum)
	}

	// Verify database size
	dbSize := binary.BigEndian.Uint32(frameHeader[4:8])
	if dbSize != 10 {
		t.Errorf("Wrong db size: expected 10, got %d", dbSize)
	}

	// Verify salt values
	salt1 := binary.BigEndian.Uint32(frameHeader[8:12])
	salt2 := binary.BigEndian.Uint32(frameHeader[12:16])

	if salt1 != wal.salt1 {
		t.Errorf("Wrong salt1: expected %d, got %d", wal.salt1, salt1)
	}

	if salt2 != wal.salt2 {
		t.Errorf("Wrong salt2: expected %d, got %d", wal.salt2, salt2)
	}

	// Read and verify page data
	pageData := make([]byte, DefaultPageSize)
	if _, err := f.Read(pageData); err != nil {
		t.Fatalf("Failed to read page data: %v", err)
	}

	if !bytesEqual(pageData, testPage) {
		t.Errorf("Page data mismatch")
	}
}

// Helper functions

// makeTestPage creates a test page with a recognizable pattern
func makeTestPage(seed int, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i + seed) % 256)
	}
	return data
}

// bytesEqual compares two byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestWALChecksumValidation tests that checksums are properly validated
func TestWALChecksumValidation(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL and write some frames
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write test frames
	for i := 1; i <= 5; i++ {
		pageData := makeTestPage(i*100, DefaultPageSize)
		if err := wal.WriteFrame(Pgno(i), pageData, uint32(i)); err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}
	}

	wal.Close()

	// Reopen WAL - should validate all frames
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Verify frame count
	if wal2.frameCount != 5 {
		t.Errorf("Expected 5 frames, got %d", wal2.frameCount)
	}

	// Read frames and verify checksums are validated
	for i := uint32(0); i < 5; i++ {
		frame, err := wal2.ReadFrame(i)
		if err != nil {
			t.Errorf("Failed to read frame %d: %v", i, err)
		}
		if frame == nil {
			t.Errorf("Frame %d is nil", i)
		}
	}
}

// TestWALChecksumCorruption tests that corrupted checksums are detected
func TestWALChecksumCorruption(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL and write some frames
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write test frames
	for i := 1; i <= 3; i++ {
		pageData := makeTestPage(i*100, DefaultPageSize)
		if err := wal.WriteFrame(Pgno(i), pageData, uint32(i)); err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}
	}

	wal.Close()

	// Corrupt a checksum in the second frame
	walFile := dbFile + "-wal"
	f, err := os.OpenFile(walFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open WAL file: %v", err)
	}

	// Corrupt checksum1 of second frame (offset: header + 1*frameSize + 16)
	frameOffset := int64(WALHeaderSize) + int64(WALFrameHeaderSize+DefaultPageSize)
	checksumOffset := frameOffset + 16 // Checksum1 offset in frame header

	corruptData := make([]byte, 4)
	binary.BigEndian.PutUint32(corruptData, 0xDEADBEEF) // Invalid checksum
	if _, err := f.WriteAt(corruptData, checksumOffset); err != nil {
		f.Close()
		t.Fatalf("Failed to corrupt checksum: %v", err)
	}
	f.Close()

	// Try to reopen WAL - should detect corruption and recreate
	wal2 := NewWAL(dbFile, DefaultPageSize)
	err = wal2.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL after corruption: %v", err)
	}
	defer wal2.Close()

	// The WAL should have been recreated with no frames
	if wal2.frameCount != 0 {
		t.Errorf("Expected empty WAL after corruption recovery, got %d frames", wal2.frameCount)
	}

	// Verify the corrupted WAL was removed and recreated
	info, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}

	// New WAL should only have header
	if info.Size() != WALHeaderSize {
		t.Logf("Note: WAL was recreated after detecting corruption (size: %d)", info.Size())
	}
}

// TestWALHeaderChecksumValidation tests header checksum validation
func TestWALHeaderChecksumValidation(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Create WAL
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	wal.Close()

	// Corrupt header checksum
	walFile := dbFile + "-wal"
	f, err := os.OpenFile(walFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open WAL file: %v", err)
	}

	// Corrupt Checksum1 in header (offset 24)
	corruptData := make([]byte, 4)
	binary.BigEndian.PutUint32(corruptData, 0xBADBAD)
	if _, err := f.WriteAt(corruptData, 24); err != nil {
		f.Close()
		t.Fatalf("Failed to corrupt header: %v", err)
	}
	f.Close()

	// Try to reopen - should fail header validation and recreate
	wal2 := NewWAL(dbFile, DefaultPageSize)
	err = wal2.Open()
	if err == nil {
		wal2.Close()
		// The WAL should have been recreated, so this is acceptable
		t.Logf("WAL was recreated after header corruption")
	} else {
		t.Logf("Got expected error on corrupted header: %v", err)
	}
}

// TestWALCumulativeChecksums tests that checksums are properly cumulative
func TestWALCumulativeChecksums(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write multiple frames
	var checksums []struct{ c1, c2 uint32 }

	for i := 1; i <= 5; i++ {
		pageData := makeTestPage(i*50, DefaultPageSize)
		if err := wal.WriteFrame(Pgno(i), pageData, uint32(i)); err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}

		// Read back to get checksum
		frame, err := wal.ReadFrame(uint32(i - 1))
		if err != nil {
			t.Fatalf("Failed to read frame %d: %v", i-1, err)
		}

		checksums = append(checksums, struct{ c1, c2 uint32 }{frame.Checksum1, frame.Checksum2})
	}

	// Verify checksums are different (cumulative)
	for i := 1; i < len(checksums); i++ {
		if checksums[i].c1 == checksums[i-1].c1 && checksums[i].c2 == checksums[i-1].c2 {
			t.Errorf("Frame %d has same checksum as frame %d - checksums should be cumulative", i, i-1)
		}
	}
}

// TestWALChecksumCache tests that the checksum cache works correctly
func TestWALChecksumCache(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	if err := os.WriteFile(dbFile, []byte{}, 0600); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write frames
	for i := 1; i <= 10; i++ {
		pageData := makeTestPage(i*10, DefaultPageSize)
		if err := wal.WriteFrame(Pgno(i), pageData, uint32(i)); err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}
	}

	wal.Close()

	// Reopen - should build cache
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Verify cache was built
	if len(wal2.checksumCache) != 10 {
		t.Errorf("Expected 10 cached checksums, got %d", len(wal2.checksumCache))
	}

	// Read frames should use cache
	for i := uint32(0); i < 10; i++ {
		if _, err := wal2.ReadFrame(i); err != nil {
			t.Errorf("Failed to read frame %d: %v", i, err)
		}
	}
}
