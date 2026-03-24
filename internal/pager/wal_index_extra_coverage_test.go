// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"errors"
	"os"
	"testing"
)

// TestWALIndexExtra_InitializeHeader exercises the initializeHeader path in
// readHeader (the IsInit==0 branch). We build a WALIndex by hand so that
// its mmap region has IsInit=0, then call readHeader directly.
func TestWALIndexExtra_InitializeHeader(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	// Open normally to get a properly-sized mmap file.
	idx := mustOpenWALIndex(t, filename)
	defer idx.Close()

	// Force IsInit to 0 in the mmap so readHeader will call initializeHeader.
	// Byte offset 12 in the header layout is IsInit (after Version(4)+Unused(4)+Change(4)).
	const isInitOffset = 12
	if len(idx.mmap) < WALIndexHeaderSize {
		t.Fatal("mmap too small")
	}
	idx.mmap[isInitOffset] = 0
	idx.header.IsInit = 0

	// readHeader should now enter the IsInit==0 branch and call initializeHeader.
	if err := idx.readHeader(); err != nil {
		t.Fatalf("readHeader() with IsInit=0: unexpected error = %v", err)
	}

	if idx.header.IsInit != 1 {
		t.Errorf("after initializeHeader: header.IsInit = %d, want 1", idx.header.IsInit)
	}
	if idx.header.Version != WALIndexVersion {
		t.Errorf("after initializeHeader: header.Version = %d, want %d", idx.header.Version, WALIndexVersion)
	}
}

// TestWALIndexExtra_ReadHeaderCorrupt exercises the ErrWALIndexCorrupt branch
// in readHeader when len(mmap) < WALIndexHeaderSize.
func TestWALIndexExtra_ReadHeaderCorrupt(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx := mustOpenWALIndex(t, filename)
	defer idx.Close()

	// Replace the mmap slice with something too small.
	idx.mmap = make([]byte, WALIndexHeaderSize-1)

	if err := idx.readHeader(); !errors.Is(err, ErrWALIndexCorrupt) {
		t.Errorf("readHeader() with short mmap = %v, want ErrWALIndexCorrupt", err)
	}
}

// TestWALIndexExtra_SyncMmapNil exercises the w.mmap == nil branch in syncMmap.
func TestWALIndexExtra_SyncMmapNil(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx := mustOpenWALIndex(t, filename)
	// Close first to avoid the deferred Close double-free; we will call
	// syncMmap directly after zeroing the mmap pointer.
	defer idx.Close()

	// Temporarily set mmap to nil.
	saved := idx.mmap
	idx.mmap = nil

	err := idx.syncMmap()
	// Restore so Close() can succeed.
	idx.mmap = saved

	if err == nil {
		t.Error("syncMmap() with nil mmap: expected error, got nil")
	}
}

// TestWALIndexExtra_DeleteNonExistentFile exercises the os.Remove path in Delete
// when the file has already been removed externally (os.IsNotExist branch).
func TestWALIndexExtra_DeleteNonExistentFile(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)

	idx := mustOpenWALIndex(t, filename)

	// Remove the shm file while the WALIndex is still open so that os.Remove
	// inside Delete will encounter a non-existent file (IsNotExist), which
	// the code ignores.
	shmFile := filename + "-shm"
	if err := os.Remove(shmFile); err != nil {
		t.Fatalf("pre-remove shm: %v", err)
	}

	// Delete should succeed even though the file is already gone.
	if err := idx.Delete(); err != nil {
		t.Errorf("Delete() with pre-removed file: unexpected error = %v", err)
	}
}

// TestWALIndexExtra_DeleteRemoveError exercises the error return path in Delete
// when os.Remove fails with a non-IsNotExist error. We point filename at a
// non-empty directory so that Remove returns "directory not empty".
func TestWALIndexExtra_DeleteRemoveError(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create a non-empty subdirectory whose path will be used as w.filename.
	// os.Remove on a non-empty directory fails with a syscall error that is
	// not IsNotExist, so Delete must return that error.
	dirTarget := tmpDir + "/notafile-shm"
	if err := os.Mkdir(dirTarget, 0700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	// Place a file inside to make it non-empty.
	child := dirTarget + "/child"
	if err := os.WriteFile(child, []byte("x"), 0600); err != nil {
		t.Fatalf("WriteFile child: %v", err)
	}

	// Construct a WALIndex whose file/mmap are already nil (Close() is a no-op)
	// but whose filename resolves to the non-empty directory path.
	idx := &WALIndex{
		filename: dirTarget,
	}

	err := idx.Delete()
	if err == nil {
		t.Error("Delete() on a non-empty directory filename: expected error, got nil")
	}
}
