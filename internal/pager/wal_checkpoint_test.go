package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// Test helper functions

func createTestWALForCheckpoint(t *testing.T) (*WAL, string) {
	t.Helper()

	// Create temp directory
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create empty database file with proper size for writing
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Write enough space for test pages (allocate 100 pages)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		if _, err := f.Write(emptyPage); err != nil {
			t.Fatalf("Failed to write empty pages: %v", err)
		}
	}
	f.Close()

	// Create WAL
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	return wal, dbFile
}

func writeTestFrameToWAL(t *testing.T, wal *WAL, pageNum Pgno, content byte, dbSize uint32) {
	t.Helper()

	pageData := make([]byte, wal.pageSize)
	// Fill page with a recognizable pattern
	for i := range pageData {
		pageData[i] = content
	}

	if err := wal.WriteFrame(pageNum, pageData, dbSize); err != nil {
		t.Fatalf("Failed to write frame: %v", err)
	}
}

func verifyPageInDatabase(t *testing.T, dbFile string, pageNum Pgno, expectedContent byte, pageSize int) {
	t.Helper()

	f, err := os.Open(dbFile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer f.Close()

	pageData := make([]byte, pageSize)
	offset := int64(pageNum-1) * int64(pageSize)
	n, err := f.ReadAt(pageData, offset)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}
	if n != pageSize {
		t.Fatalf("Read incomplete page: got %d bytes, expected %d", n, pageSize)
	}

	// Verify content (check first few bytes as sample)
	for i := 0; i < 100 && i < len(pageData); i++ {
		if pageData[i] != expectedContent {
			t.Errorf("Page %d byte %d mismatch: got 0x%02x, expected 0x%02x",
				pageNum, i, pageData[i], expectedContent)
			break
		}
	}
}

// Basic checkpoint mode tests

func TestCheckpointMode_Constants(t *testing.T) {
	// Verify checkpoint mode constants are distinct
	modes := []CheckpointMode{
		CheckpointPassive,
		CheckpointFull,
		CheckpointRestart,
		CheckpointTruncate,
	}

	seen := make(map[CheckpointMode]bool)
	for _, mode := range modes {
		if seen[mode] {
			t.Errorf("Duplicate checkpoint mode value: %d", mode)
		}
		seen[mode] = true
	}

	// Verify they have expected values
	if CheckpointPassive != 0 {
		t.Errorf("CheckpointPassive should be 0, got %d", CheckpointPassive)
	}
	if CheckpointFull != 1 {
		t.Errorf("CheckpointFull should be 1, got %d", CheckpointFull)
	}
	if CheckpointRestart != 2 {
		t.Errorf("CheckpointRestart should be 2, got %d", CheckpointRestart)
	}
	if CheckpointTruncate != 3 {
		t.Errorf("CheckpointTruncate should be 3, got %d", CheckpointTruncate)
	}
}

func TestCheckpointPassive_EmptyWAL(t *testing.T) {
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	checkpointed, remaining, err := wal.CheckpointWithMode(CheckpointPassive)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != 0 {
		t.Errorf("Expected 0 frames checkpointed, got %d", checkpointed)
	}

	if remaining != 0 {
		t.Errorf("Expected 0 frames remaining, got %d", remaining)
	}
}

func TestCheckpointPassive_SingleFrame(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write a frame
	pageNum := Pgno(1)
	writeTestFrameToWAL(t, wal, pageNum, 0xAB, 1)

	// Verify frame was written to WAL
	if wal.FrameCount() != 1 {
		t.Fatalf("Expected 1 frame in WAL, got %d", wal.FrameCount())
	}

	// Checkpoint
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointPassive)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != 1 {
		t.Errorf("Expected 1 frame checkpointed, got %d", checkpointed)
	}

	// Verify data was written to database
	verifyPageInDatabase(t, dbFile, pageNum, 0xAB, DefaultPageSize)
}

func TestCheckpointPassive_MultipleFrames(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write multiple frames
	numFrames := 5
	for i := 1; i <= numFrames; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*10), uint32(i))
	}

	// Checkpoint
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointPassive)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numFrames {
		t.Errorf("Expected %d frames checkpointed, got %d", numFrames, checkpointed)
	}

	// Verify all pages were written to database
	for i := 1; i <= numFrames; i++ {
		verifyPageInDatabase(t, dbFile, Pgno(i), byte(i*10), DefaultPageSize)
	}
}

func TestCheckpointFull_SingleFrame(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write a frame
	pageNum := Pgno(1)
	writeTestFrameToWAL(t, wal, pageNum, 0xCD, 1)

	// Full checkpoint
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != 1 {
		t.Errorf("Expected 1 frame checkpointed, got %d", checkpointed)
	}

	// Verify data was written to database
	verifyPageInDatabase(t, dbFile, pageNum, 0xCD, DefaultPageSize)
}

func TestCheckpointFull_MultipleFrames(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write multiple frames
	numFrames := 10
	for i := 1; i <= numFrames; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*20), uint32(i))
	}

	// Full checkpoint
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numFrames {
		t.Errorf("Expected %d frames checkpointed, got %d", numFrames, checkpointed)
	}

	// Verify all pages were written to database
	for i := 1; i <= numFrames; i++ {
		verifyPageInDatabase(t, dbFile, Pgno(i), byte(i*20), DefaultPageSize)
	}
}

func TestCheckpointRestart_ResetsWAL(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	numFrames := 3
	for i := 1; i <= numFrames; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*30), uint32(i))
	}

	// Get checkpoint sequence before
	initialSeq := wal.checkpointSeq

	// Restart checkpoint
	checkpointed, remaining, err := wal.CheckpointWithMode(CheckpointRestart)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numFrames {
		t.Errorf("Expected %d frames checkpointed, got %d", numFrames, checkpointed)
	}

	if remaining != 0 {
		t.Errorf("Expected 0 frames remaining after restart, got %d", remaining)
	}

	// Verify data was written to database
	for i := 1; i <= numFrames; i++ {
		verifyPageInDatabase(t, dbFile, Pgno(i), byte(i*30), DefaultPageSize)
	}

	// Verify WAL was reset
	if wal.FrameCount() != 0 {
		t.Errorf("WAL should be empty after restart, got %d frames", wal.FrameCount())
	}

	// Verify checkpoint sequence was incremented
	if wal.checkpointSeq != initialSeq+1 {
		t.Errorf("Checkpoint sequence should be incremented: got %d, expected %d",
			wal.checkpointSeq, initialSeq+1)
	}

	// Verify WAL file still exists and has just the header
	info, err := os.Stat(wal.filename)
	if err != nil {
		t.Fatalf("WAL file should still exist: %v", err)
	}

	if info.Size() != WALHeaderSize {
		t.Errorf("WAL file size should be %d (header only), got %d",
			WALHeaderSize, info.Size())
	}
}

func TestCheckpointTruncate_RemovesWAL(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	walFilename := wal.filename

	// Write frames
	numFrames := 3
	for i := 1; i <= numFrames; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*40), uint32(i))
	}

	// Truncate checkpoint
	checkpointed, remaining, err := wal.CheckpointWithMode(CheckpointTruncate)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numFrames {
		t.Errorf("Expected %d frames checkpointed, got %d", numFrames, checkpointed)
	}

	if remaining != 0 {
		t.Errorf("Expected 0 frames remaining after truncate, got %d", remaining)
	}

	// Verify data was written to database
	for i := 1; i <= numFrames; i++ {
		verifyPageInDatabase(t, dbFile, Pgno(i), byte(i*40), DefaultPageSize)
	}

	// Verify WAL was truncated
	if wal.FrameCount() != 0 {
		t.Errorf("WAL should be empty after truncate, got %d frames", wal.FrameCount())
	}

	// Verify WAL file was truncated to zero
	info, err := os.Stat(walFilename)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if info.Size() != 0 {
		t.Errorf("WAL file should be truncated to 0 bytes, got %d", info.Size())
	}

	// File handle should be closed
	if wal.file != nil {
		t.Error("WAL file should be closed after truncate")
	}
}

func TestCheckpointInvalidMode(t *testing.T) {
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Invalid checkpoint mode
	_, _, err := wal.CheckpointWithMode(CheckpointMode(999))
	if err != ErrCheckpointInvalidMode {
		t.Errorf("Expected ErrCheckpointInvalidMode, got: %v", err)
	}
}

// Test checkpointing with updated pages

func TestCheckpoint_UpdatedPages(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	pageNum := Pgno(1)

	// Write initial version
	writeTestFrameToWAL(t, wal, pageNum, 0xAA, 1)

	// Update the page
	writeTestFrameToWAL(t, wal, pageNum, 0xBB, 1)

	// Update again
	writeTestFrameToWAL(t, wal, pageNum, 0xCC, 1)

	// Checkpoint - should write the latest version
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// We wrote 3 frames but only 1 unique page
	if checkpointed != 1 {
		t.Errorf("Expected 1 unique page checkpointed, got %d", checkpointed)
	}

	// Verify the latest version is in the database
	verifyPageInDatabase(t, dbFile, pageNum, 0xCC, DefaultPageSize)
}

// Test CheckpointWithInfo

func TestCheckpointWithInfo_Passive(t *testing.T) {
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	for i := 1; i <= 5; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i), uint32(i))
	}

	// Checkpoint with info
	info, err := wal.CheckpointWithInfo(CheckpointPassive)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if info == nil {
		t.Fatal("CheckpointInfo should not be nil")
	}

	if info.FramesCheckpointed != 5 {
		t.Errorf("Expected 5 frames checkpointed, got %d", info.FramesCheckpointed)
	}

	if info.WALSizeBefore == 0 {
		t.Error("WAL size before should be non-zero")
	}

	t.Logf("Checkpoint info: checkpointed=%d, remaining=%d, size before=%d, size after=%d",
		info.FramesCheckpointed, info.FramesRemaining, info.WALSizeBefore, info.WALSizeAfter)
}

func TestCheckpointWithInfo_Restart(t *testing.T) {
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i), uint32(i))
	}

	// Checkpoint with info
	info, err := wal.CheckpointWithInfo(CheckpointRestart)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if info.FramesCheckpointed != 3 {
		t.Errorf("Expected 3 frames checkpointed, got %d", info.FramesCheckpointed)
	}

	if info.FramesRemaining != 0 {
		t.Errorf("Expected 0 frames remaining, got %d", info.FramesRemaining)
	}

	// After restart, WAL should only have header
	if info.WALSizeAfter != WALHeaderSize {
		t.Errorf("WAL size after restart should be %d, got %d",
			WALHeaderSize, info.WALSizeAfter)
	}
}

func TestCheckpointWithInfo_Truncate(t *testing.T) {
	wal, _ := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write frames
	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i), uint32(i))
	}

	// Checkpoint with info
	info, err := wal.CheckpointWithInfo(CheckpointTruncate)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if info.FramesCheckpointed != 3 {
		t.Errorf("Expected 3 frames checkpointed, got %d", info.FramesCheckpointed)
	}

	if info.FramesRemaining != 0 {
		t.Errorf("Expected 0 frames remaining, got %d", info.FramesRemaining)
	}

	// After truncate, WAL should be empty
	if info.WALSizeAfter != 0 {
		t.Errorf("WAL size after truncate should be 0, got %d", info.WALSizeAfter)
	}
}

// Test reopening after checkpoint

func TestCheckpoint_ReopenAfterRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "test.db")

	// Create database file
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 10; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	// Create WAL and write frames
	wal := NewWAL(dbFile, DefaultPageSize)
	if err := wal.Open(); err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	for i := 1; i <= 3; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i*50), uint32(i))
	}

	// Checkpoint and close
	_, _, err = wal.CheckpointWithMode(CheckpointRestart)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
	wal.Close()

	// Reopen WAL
	wal2 := NewWAL(dbFile, DefaultPageSize)
	if err := wal2.Open(); err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// WAL should be empty after restart
	if wal2.FrameCount() != 0 {
		t.Errorf("Reopened WAL should be empty, got %d frames", wal2.FrameCount())
	}

	// Should be able to write new frames
	writeTestFrameToWAL(t, wal2, 5, 0xEE, 5)

	if wal2.FrameCount() != 1 {
		t.Errorf("Expected 1 frame in reopened WAL, got %d", wal2.FrameCount())
	}
}

// Test large WAL checkpoint

func TestCheckpoint_LargeWAL(t *testing.T) {
	wal, dbFile := createTestWALForCheckpoint(t)
	defer wal.Close()

	// Write many frames
	numPages := 50
	for i := 1; i <= numPages; i++ {
		writeTestFrameToWAL(t, wal, Pgno(i), byte(i%256), uint32(i))
	}

	// Checkpoint all
	checkpointed, _, err := wal.CheckpointWithMode(CheckpointFull)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != numPages {
		t.Errorf("Expected %d frames checkpointed, got %d", numPages, checkpointed)
	}

	// Spot check some pages
	testPages := []Pgno{1, 10, 25, 40, 50}
	for _, pageNum := range testPages {
		verifyPageInDatabase(t, dbFile, pageNum, byte(pageNum%256), DefaultPageSize)
	}
}

// Benchmark tests

func BenchmarkCheckpointPassive(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointPassive)
		b.StopTimer()

		wal.Close()
		os.Truncate(dbFile+"-wal", 0)
	}
}

func BenchmarkCheckpointFull(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointFull)
		b.StopTimer()

		wal.Close()
		os.Truncate(dbFile+"-wal", 0)
	}
}

func BenchmarkCheckpointRestart(b *testing.B) {
	tempDir := b.TempDir()
	dbFile := filepath.Join(tempDir, "bench.db")

	f, _ := os.Create(dbFile)
	emptyPage := make([]byte, DefaultPageSize)
	for i := 0; i < 100; i++ {
		f.Write(emptyPage)
	}
	f.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		wal := NewWAL(dbFile, DefaultPageSize)
		wal.Open()

		// Write 10 frames
		for j := 1; j <= 10; j++ {
			pageData := make([]byte, DefaultPageSize)
			wal.WriteFrame(Pgno(j), pageData, uint32(j))
		}

		b.StartTimer()
		wal.CheckpointWithMode(CheckpointRestart)
		b.StopTimer()

		wal.Close()
	}
}
