// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"os"
	"testing"
)

// TestWALIndexCoverage_FreshFileInitializeHeader opens a brand-new file and
// verifies that initializeHeader runs (IsInit==0 branch in readHeader).
func TestWALIndexCoverage_FreshFileInitializeHeader(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	if !idx.IsInitialized() {
		t.Error("expected WAL index to be initialized")
	}
	if idx.header == nil {
		t.Fatal("expected header to be non-nil")
	}
	if idx.header.IsInit != 1 {
		t.Errorf("header.IsInit = %d, want 1", idx.header.IsInit)
	}
	if idx.header.Version != WALIndexVersion {
		t.Errorf("header.Version = %d, want %d", idx.header.Version, WALIndexVersion)
	}
}

// TestWALIndexCoverage_ValidateAndFixHeader writes a file with a mismatched
// version in the header, then reopens it to trigger validateAndFixHeader's
// reinitialization branch.
func TestWALIndexCoverage_ValidateAndFixHeader(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)
	shmFile := filename + "-shm"

	// Create a normal index first.
	idx := mustOpenWALIndex(t, filename)
	mustCloseWALIndex(t, idx)

	// Corrupt the version field in the shm file (set to non-zero, non-matching value).
	corruptHeaderVersion(t, shmFile, 0xDEADBEEF)

	// Reopen: readHeader will see IsInit==1 (already set) but a mismatched
	// version, so validateAndFixHeader should reinitialize.
	idx2, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() after version corruption error = %v", err)
	}
	defer idx2.Close()

	if idx2.header.Version != WALIndexVersion {
		t.Errorf("after fix: header.Version = %d, want %d", idx2.header.Version, WALIndexVersion)
	}
}

// corruptHeaderVersion writes a bad version uint32 at offset 0 of the shm file.
func corruptHeaderVersion(t *testing.T, shmFile string, badVersion uint32) {
	t.Helper()
	f, err := os.OpenFile(shmFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open shm for corruption: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, badVersion)
	if _, err := f.WriteAt(buf, 0); err != nil {
		t.Fatalf("write bad version: %v", err)
	}
}

// TestWALIndexCoverage_InsertFindDeleteLifecycle exercises Insert, Find, and
// Delete in sequence, verifying correct behaviour at each stage.
func TestWALIndexCoverage_InsertFindDeleteLifecycle(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}

	mustInsertFrame(t, idx, 1, 100)
	mustInsertFrame(t, idx, 2, 200)

	frameNo, err := idx.FindFrame(1)
	if err != nil {
		t.Fatalf("FindFrame(1) error = %v", err)
	}
	if frameNo != 100 {
		t.Errorf("FindFrame(1) = %d, want 100", frameNo)
	}

	if err := idx.Delete(); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	shmFile := filename + "-shm"
	if _, err := os.Stat(shmFile); !os.IsNotExist(err) {
		t.Error("shm file should be removed after Delete()")
	}
}

// TestWALIndexCoverage_ClearAndReuse exercises the Clear path (clearHashTable
// + syncMmap) and then re-inserts frames to confirm the index is reusable.
func TestWALIndexCoverage_ClearAndReuse(t *testing.T) {
	t.Parallel()
	idx, _ := openTestWALIndex(t)

	insertFrameRange(t, idx, 20, 1)

	if err := idx.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	if _, err := idx.FindFrame(10); err != ErrFrameNotFound {
		t.Errorf("FindFrame(10) after Clear() = %v, want ErrFrameNotFound", err)
	}
	if max := idx.GetMaxFrame(); max != 0 {
		t.Errorf("GetMaxFrame() after Clear() = %d, want 0", max)
	}

	// Re-use: insert new frames after clearing.
	mustInsertFrame(t, idx, 5, 999)
	frameNo, err := idx.FindFrame(5)
	if err != nil {
		t.Fatalf("FindFrame(5) after re-insert error = %v", err)
	}
	if frameNo != 999 {
		t.Errorf("FindFrame(5) after re-insert = %d, want 999", frameNo)
	}
}

// TestWALIndexCoverage_SyncMmap exercises syncMmap indirectly via multiple
// writes that each call writeHeader → syncMmap.
func TestWALIndexCoverage_SyncMmap(t *testing.T) {
	t.Parallel()
	idx, _ := openTestWALIndex(t)

	for i := uint32(1); i <= 5; i++ {
		mustInsertFrame(t, idx, i, i*10)
		mustSetReadMark(t, idx, int(i-1), i*10)
	}

	if max := idx.GetMaxFrame(); max != 50 {
		t.Errorf("GetMaxFrame() = %d, want 50", max)
	}
}

// TestWALIndexCoverage_DeleteAlreadyClosed exercises the Delete path when the
// file has already been created and uses a fresh open so Close runs inside Delete.
func TestWALIndexCoverage_DeleteAlreadyClosed(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx := mustOpenWALIndex(t, filename)
	mustInsertFrame(t, idx, 1, 10)

	if err := idx.Delete(); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	shmFile := filename + "-shm"
	if _, err := os.Stat(shmFile); !os.IsNotExist(err) {
		t.Error("shm file should not exist after Delete()")
	}
}

// TestWALIndexCoverage_InitializeFileViaOpen verifies that a file smaller
// than minSize triggers initializeFile (the file-growth branch in open()).
func TestWALIndexCoverage_InitializeFileViaOpen(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)
	shmFile := filename + "-shm"

	// Write a tiny shm file so that open() must call initializeFile.
	if err := os.WriteFile(shmFile, []byte{0, 1, 2, 3}, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	if !idx.IsInitialized() {
		t.Error("expected WAL index to be initialized after tiny-file open")
	}
}
