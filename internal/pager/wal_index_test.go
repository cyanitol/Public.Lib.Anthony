package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// tempWALIndexFile creates a temporary file path for testing
func tempWALIndexFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}

// TestNewWALIndex tests creating a new WAL index
func TestNewWALIndex(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	if !idx.IsInitialized() {
		t.Error("WAL index should be initialized")
	}

	if idx.header == nil {
		t.Fatal("WAL index header is nil")
	}

	if idx.header.Version != WALIndexVersion {
		t.Errorf("Version = %d, want %d", idx.header.Version, WALIndexVersion)
	}

	if idx.header.IsInit != 1 {
		t.Errorf("IsInit = %d, want 1", idx.header.IsInit)
	}

	// Check that the file was created
	shmFile := filename + "-shm"
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Error("WAL index file was not created")
	}
}

// TestWALIndex_InsertAndFind tests inserting and finding frames
func TestWALIndex_InsertAndFind(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Insert some frames
	testCases := []struct {
		pgno    uint32
		frameNo uint32
	}{
		{1, 100},
		{2, 101},
		{3, 102},
		{10, 103},
		{100, 104},
	}

	for _, tc := range testCases {
		if err := idx.InsertFrame(tc.pgno, tc.frameNo); err != nil {
			t.Fatalf("InsertFrame(%d, %d) error = %v", tc.pgno, tc.frameNo, err)
		}
	}

	// Find the frames
	for _, tc := range testCases {
		frameNo, err := idx.FindFrame(tc.pgno)
		if err != nil {
			t.Fatalf("FindFrame(%d) error = %v", tc.pgno, err)
		}
		if frameNo != tc.frameNo {
			t.Errorf("FindFrame(%d) = %d, want %d", tc.pgno, frameNo, tc.frameNo)
		}
	}

	// Try to find a non-existent page
	_, err = idx.FindFrame(999)
	if err != ErrFrameNotFound {
		t.Errorf("FindFrame(999) error = %v, want ErrFrameNotFound", err)
	}
}

// TestWALIndex_UpdateFrame tests updating an existing frame
func TestWALIndex_UpdateFrame(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Insert a frame
	if err := idx.InsertFrame(1, 100); err != nil {
		t.Fatalf("InsertFrame(1, 100) error = %v", err)
	}

	// Verify the initial frame
	frameNo, err := idx.FindFrame(1)
	if err != nil {
		t.Fatalf("FindFrame(1) error = %v", err)
	}
	if frameNo != 100 {
		t.Errorf("FindFrame(1) = %d, want 100", frameNo)
	}

	// Update the frame (insert newer frame for same page)
	if err := idx.InsertFrame(1, 200); err != nil {
		t.Fatalf("InsertFrame(1, 200) error = %v", err)
	}

	// Verify the updated frame
	frameNo, err = idx.FindFrame(1)
	if err != nil {
		t.Fatalf("FindFrame(1) error = %v", err)
	}
	if frameNo != 200 {
		t.Errorf("FindFrame(1) = %d, want 200", frameNo)
	}
}

// TestWALIndex_MaxFrame tests tracking the maximum frame number
func TestWALIndex_MaxFrame(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Initially, max frame should be 0
	if maxFrame := idx.GetMaxFrame(); maxFrame != 0 {
		t.Errorf("GetMaxFrame() = %d, want 0", maxFrame)
	}

	// Insert frames in order
	for i := uint32(1); i <= 10; i++ {
		if err := idx.InsertFrame(i, i*10); err != nil {
			t.Fatalf("InsertFrame(%d, %d) error = %v", i, i*10, err)
		}
	}

	// Max frame should be 100
	if maxFrame := idx.GetMaxFrame(); maxFrame != 100 {
		t.Errorf("GetMaxFrame() = %d, want 100", maxFrame)
	}

	// Insert a frame with a higher frame number
	if err := idx.InsertFrame(20, 500); err != nil {
		t.Fatalf("InsertFrame(20, 500) error = %v", err)
	}

	// Max frame should now be 500
	if maxFrame := idx.GetMaxFrame(); maxFrame != 500 {
		t.Errorf("GetMaxFrame() = %d, want 500", maxFrame)
	}
}

// TestWALIndex_ReadMarks tests setting and getting read marks
func TestWALIndex_ReadMarks(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Initially, all read marks should be 0
	for i := 0; i < WALIndexMaxReaders; i++ {
		mark, err := idx.GetReadMark(i)
		if err != nil {
			t.Fatalf("GetReadMark(%d) error = %v", i, err)
		}
		if mark != 0 {
			t.Errorf("GetReadMark(%d) = %d, want 0", i, mark)
		}
	}

	// Set read marks for different readers
	testCases := []struct {
		reader int
		frame  uint32
	}{
		{0, 100},
		{1, 200},
		{2, 300},
		{3, 400},
		{4, 500},
	}

	for _, tc := range testCases {
		if err := idx.SetReadMark(tc.reader, tc.frame); err != nil {
			t.Fatalf("SetReadMark(%d, %d) error = %v", tc.reader, tc.frame, err)
		}
	}

	// Verify the read marks
	for _, tc := range testCases {
		mark, err := idx.GetReadMark(tc.reader)
		if err != nil {
			t.Fatalf("GetReadMark(%d) error = %v", tc.reader, err)
		}
		if mark != tc.frame {
			t.Errorf("GetReadMark(%d) = %d, want %d", tc.reader, mark, tc.frame)
		}
	}

	// Test invalid reader IDs
	if err := idx.SetReadMark(-1, 100); err != ErrInvalidReader {
		t.Errorf("SetReadMark(-1, 100) error = %v, want ErrInvalidReader", err)
	}

	if err := idx.SetReadMark(WALIndexMaxReaders, 100); err != ErrInvalidReader {
		t.Errorf("SetReadMark(%d, 100) error = %v, want ErrInvalidReader", WALIndexMaxReaders, err)
	}

	if _, err := idx.GetReadMark(-1); err != ErrInvalidReader {
		t.Errorf("GetReadMark(-1) error = %v, want ErrInvalidReader", err)
	}

	if _, err := idx.GetReadMark(WALIndexMaxReaders); err != ErrInvalidReader {
		t.Errorf("GetReadMark(%d) error = %v, want ErrInvalidReader", WALIndexMaxReaders, err)
	}
}

// TestWALIndex_PageCount tests setting and getting the page count
func TestWALIndex_PageCount(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Initially, page count should be 0
	if nPage := idx.GetPageCount(); nPage != 0 {
		t.Errorf("GetPageCount() = %d, want 0", nPage)
	}

	// Set page count
	if err := idx.SetPageCount(100); err != nil {
		t.Fatalf("SetPageCount(100) error = %v", err)
	}

	// Verify page count
	if nPage := idx.GetPageCount(); nPage != 100 {
		t.Errorf("GetPageCount() = %d, want 100", nPage)
	}

	// Update page count
	if err := idx.SetPageCount(200); err != nil {
		t.Fatalf("SetPageCount(200) error = %v", err)
	}

	// Verify updated page count
	if nPage := idx.GetPageCount(); nPage != 200 {
		t.Errorf("GetPageCount() = %d, want 200", nPage)
	}
}

// TestWALIndex_Clear tests clearing the WAL index
func TestWALIndex_Clear(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Insert some frames
	for i := uint32(1); i <= 10; i++ {
		if err := idx.InsertFrame(i, i*10); err != nil {
			t.Fatalf("InsertFrame(%d, %d) error = %v", i, i*10, err)
		}
	}

	// Set some read marks
	if err := idx.SetReadMark(1, 100); err != nil {
		t.Fatalf("SetReadMark(1, 100) error = %v", err)
	}

	// Verify data exists
	if _, err := idx.FindFrame(5); err != nil {
		t.Fatalf("FindFrame(5) error = %v (should exist)", err)
	}

	if maxFrame := idx.GetMaxFrame(); maxFrame != 100 {
		t.Errorf("GetMaxFrame() = %d, want 100", maxFrame)
	}

	// Clear the index
	if err := idx.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	// Verify everything is cleared
	if _, err := idx.FindFrame(5); err != ErrFrameNotFound {
		t.Errorf("FindFrame(5) after Clear() error = %v, want ErrFrameNotFound", err)
	}

	if maxFrame := idx.GetMaxFrame(); maxFrame != 0 {
		t.Errorf("GetMaxFrame() after Clear() = %d, want 0", maxFrame)
	}

	mark, err := idx.GetReadMark(1)
	if err != nil {
		t.Fatalf("GetReadMark(1) after Clear() error = %v", err)
	}
	if mark != 0 {
		t.Errorf("GetReadMark(1) after Clear() = %d, want 0", mark)
	}
}

// TestWALIndex_ChangeCounter tests the change counter
func TestWALIndex_ChangeCounter(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Get initial change counter
	initialChange := idx.GetChangeCounter()

	// Insert a frame (should increment change counter)
	if err := idx.InsertFrame(1, 100); err != nil {
		t.Fatalf("InsertFrame(1, 100) error = %v", err)
	}

	change1 := idx.GetChangeCounter()
	if change1 <= initialChange {
		t.Errorf("Change counter not incremented after InsertFrame: %d <= %d", change1, initialChange)
	}

	// Set a read mark (should increment change counter)
	if err := idx.SetReadMark(1, 100); err != nil {
		t.Fatalf("SetReadMark(1, 100) error = %v", err)
	}

	change2 := idx.GetChangeCounter()
	if change2 <= change1 {
		t.Errorf("Change counter not incremented after SetReadMark: %d <= %d", change2, change1)
	}

	// Set page count (should increment change counter)
	if err := idx.SetPageCount(100); err != nil {
		t.Fatalf("SetPageCount(100) error = %v", err)
	}

	change3 := idx.GetChangeCounter()
	if change3 <= change2 {
		t.Errorf("Change counter not incremented after SetPageCount: %d <= %d", change3, change2)
	}
}

// TestWALIndex_CloseAndReopen tests closing and reopening the WAL index
func TestWALIndex_CloseAndReopen(t *testing.T) {
	filename := tempWALIndexFile(t)

	// Create and populate the index
	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}

	// Insert some frames
	for i := uint32(1); i <= 5; i++ {
		if err := idx.InsertFrame(i, i*10); err != nil {
			t.Fatalf("InsertFrame(%d, %d) error = %v", i, i*10, err)
		}
	}

	// Set a read mark
	if err := idx.SetReadMark(1, 50); err != nil {
		t.Fatalf("SetReadMark(1, 50) error = %v", err)
	}

	// Close the index
	if err := idx.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen the index
	idx2, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() (reopen) error = %v", err)
	}
	defer idx2.Close()

	// Verify data persisted
	for i := uint32(1); i <= 5; i++ {
		frameNo, err := idx2.FindFrame(i)
		if err != nil {
			t.Fatalf("FindFrame(%d) after reopen error = %v", i, err)
		}
		expectedFrame := i * 10
		if frameNo != expectedFrame {
			t.Errorf("FindFrame(%d) after reopen = %d, want %d", i, frameNo, expectedFrame)
		}
	}

	// Verify read mark persisted
	mark, err := idx2.GetReadMark(1)
	if err != nil {
		t.Fatalf("GetReadMark(1) after reopen error = %v", err)
	}
	if mark != 50 {
		t.Errorf("GetReadMark(1) after reopen = %d, want 50", mark)
	}
}

// TestWALIndex_InvalidPageNum tests inserting/finding invalid page numbers
func TestWALIndex_InvalidPageNum(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Try to insert page 0 (invalid)
	if err := idx.InsertFrame(0, 100); err != ErrInvalidPageNum {
		t.Errorf("InsertFrame(0, 100) error = %v, want ErrInvalidPageNum", err)
	}

	// Try to find page 0 (invalid)
	if _, err := idx.FindFrame(0); err != ErrInvalidPageNum {
		t.Errorf("FindFrame(0) error = %v, want ErrInvalidPageNum", err)
	}
}

// TestWALIndex_MultipleFrames tests inserting many frames to test hash collisions
func TestWALIndex_MultipleFrames(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Insert many frames to potentially cause hash collisions
	numPages := 100
	for i := 1; i <= numPages; i++ {
		pgno := uint32(i)
		frameNo := uint32(i * 100)
		if err := idx.InsertFrame(pgno, frameNo); err != nil {
			t.Fatalf("InsertFrame(%d, %d) error = %v", pgno, frameNo, err)
		}
	}

	// Verify all frames can be found
	for i := 1; i <= numPages; i++ {
		pgno := uint32(i)
		expectedFrame := uint32(i * 100)
		frameNo, err := idx.FindFrame(pgno)
		if err != nil {
			t.Fatalf("FindFrame(%d) error = %v", pgno, err)
		}
		if frameNo != expectedFrame {
			t.Errorf("FindFrame(%d) = %d, want %d", pgno, frameNo, expectedFrame)
		}
	}
}

// TestWALIndex_Delete tests deleting the WAL index file
func TestWALIndex_Delete(t *testing.T) {
	filename := tempWALIndexFile(t)
	shmFile := filename + "-shm"

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(shmFile); os.IsNotExist(err) {
		t.Fatal("WAL index file was not created")
	}

	// Close the index first (required before delete)
	if err := idx.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Create a new instance to call Delete
	idx2, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}

	// Delete the index
	if err := idx2.Delete(); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(shmFile); !os.IsNotExist(err) {
		t.Error("WAL index file was not deleted")
	}
}

// TestWALIndex_Concurrent tests concurrent access to the WAL index
func TestWALIndex_Concurrent(t *testing.T) {
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Use goroutines to simulate concurrent access
	const numGoroutines = 10
	const numOpsPerGoroutine = 100

	errCh := make(chan error, numGoroutines*numOpsPerGoroutine)
	doneCh := make(chan bool, numGoroutines)

	// Writer goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			for i := 0; i < numOpsPerGoroutine; i++ {
				pgno := uint32(id*numOpsPerGoroutine + i + 1)
				frameNo := uint32((id*numOpsPerGoroutine + i + 1) * 10)
				if err := idx.InsertFrame(pgno, frameNo); err != nil {
					errCh <- err
					return
				}
			}
			doneCh <- true
		}(g)
	}

	// Wait for all goroutines to complete
	for g := 0; g < numGoroutines; g++ {
		<-doneCh
	}
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent operation error: %v", err)
	}

	// Verify all frames were inserted correctly
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < numOpsPerGoroutine; i++ {
			pgno := uint32(g*numOpsPerGoroutine + i + 1)
			expectedFrame := uint32((g*numOpsPerGoroutine + i + 1) * 10)
			frameNo, err := idx.FindFrame(pgno)
			if err != nil {
				t.Errorf("FindFrame(%d) error = %v", pgno, err)
				continue
			}
			if frameNo != expectedFrame {
				t.Errorf("FindFrame(%d) = %d, want %d", pgno, frameNo, expectedFrame)
			}
		}
	}
}

// BenchmarkWALIndex_InsertFrame benchmarks frame insertion
func BenchmarkWALIndex_InsertFrame(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench.db")

	idx, err := NewWALIndex(filename)
	if err != nil {
		b.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pgno := uint32(i%10000 + 1) // Reuse page numbers to test updates
		frameNo := uint32(i + 1)
		if err := idx.InsertFrame(pgno, frameNo); err != nil {
			b.Fatalf("InsertFrame() error = %v", err)
		}
	}
}

// BenchmarkWALIndex_FindFrame benchmarks frame lookup
func BenchmarkWALIndex_FindFrame(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench.db")

	idx, err := NewWALIndex(filename)
	if err != nil {
		b.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	// Pre-populate with frames
	numFrames := 1000
	for i := 1; i <= numFrames; i++ {
		if err := idx.InsertFrame(uint32(i), uint32(i*10)); err != nil {
			b.Fatalf("InsertFrame() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pgno := uint32(i%numFrames + 1)
		if _, err := idx.FindFrame(pgno); err != nil {
			b.Fatalf("FindFrame() error = %v", err)
		}
	}
}

// BenchmarkWALIndex_ReadMark benchmarks read mark operations
func BenchmarkWALIndex_ReadMark(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench.db")

	idx, err := NewWALIndex(filename)
	if err != nil {
		b.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := i % WALIndexMaxReaders
		frame := uint32(i + 1)
		if err := idx.SetReadMark(reader, frame); err != nil {
			b.Fatalf("SetReadMark() error = %v", err)
		}
	}
}
