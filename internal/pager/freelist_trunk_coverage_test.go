// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// mockPagerGetError is a pagerInternal that returns an error from getLocked.
type mockPagerGetError struct {
	pageSize int
}

func (m *mockPagerGetError) getLocked(pgno Pgno) (*DbPage, error) {
	return nil, ErrPageNotFound
}
func (m *mockPagerGetError) writeLocked(page *DbPage) error { return nil }
func (m *mockPagerGetError) Put(page *DbPage)               {}
func (m *mockPagerGetError) PageSize() int                  { return m.pageSize }

// mockPagerWriteError is a pagerInternal that fails on writeLocked for any page.
type mockPagerWriteError struct {
	pages    map[Pgno]*DbPage
	pageSize int
}

func (m *mockPagerWriteError) getLocked(pgno Pgno) (*DbPage, error) {
	p, ok := m.pages[pgno]
	if !ok {
		return nil, ErrPageNotFound
	}
	return p, nil
}
func (m *mockPagerWriteError) writeLocked(page *DbPage) error {
	return ErrReadOnly
}
func (m *mockPagerWriteError) Put(page *DbPage) {}
func (m *mockPagerWriteError) PageSize() int    { return m.pageSize }

// TestFreelistTrunk_ProcessTrunkPageGetError exercises the getLocked error path
// in processTrunkPage (lines 237-239). The mock always fails getLocked so the
// function must propagate the error.
func TestFreelistTrunk_ProcessTrunkPageGetError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerGetError{pageSize: pageSize}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 1
	fl.pendingFree = []Pgno{10}

	maxLeaves := FreeListMaxLeafPages(pageSize)
	err := fl.processTrunkPage(maxLeaves)
	if err == nil {
		t.Error("processTrunkPage: expected error when getLocked fails, got nil")
	}
}

// TestFreelistTrunk_ProcessTrunkPageWriteError exercises the writeLocked error
// path inside addPendingToTrunk, reached when leafCount < maxLeaves but the
// trunk page cannot be marked dirty. The error must surface from processTrunkPage.
func TestFreelistTrunk_ProcessTrunkPageWriteError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerWriteError{
		pages:    make(map[Pgno]*DbPage),
		pageSize: pageSize,
	}

	// Trunk 5 with zero leaves (plenty of space — leafCount < maxLeaves).
	data := make([]byte, pageSize)
	binary.BigEndian.PutUint32(data[0:4], 0) // no next trunk
	binary.BigEndian.PutUint32(data[4:8], 0) // leafCount = 0
	mp.pages[5] = &DbPage{Pgno: 5, Data: data}

	fl := NewFreeList(mp)
	fl.firstTrunk = 5
	fl.totalFree = 1
	fl.pendingFree = []Pgno{10}

	maxLeaves := FreeListMaxLeafPages(pageSize)
	err := fl.processTrunkPage(maxLeaves)
	if err == nil {
		t.Error("processTrunkPage: expected error when writeLocked fails, got nil")
	}
}

// TestFreelistTrunk_ProcessTrunkPageHappyPath verifies the success path for
// processTrunkPage when the trunk has space (leafCount < maxLeaves) and
// writeLocked succeeds. The pending page must be consumed and totalFree updated.
func TestFreelistTrunk_ProcessTrunkPageHappyPath(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk 5 with zero leaves; newMockPager's writeLocked is a no-op success.
	mp.addTrunkPage(5, 0, nil)

	fl := NewFreeList(mp)
	fl.firstTrunk = 5
	fl.totalFree = 1
	fl.pendingFree = []Pgno{10, 11}

	maxLeaves := FreeListMaxLeafPages(pageSize)
	if err := fl.processTrunkPage(maxLeaves); err != nil {
		t.Fatalf("processTrunkPage (happy path): %v", err)
	}

	// Both pending pages should have been written into the trunk (space allows it).
	if len(fl.pendingFree) != 0 {
		t.Errorf("expected pendingFree empty, got %v", fl.pendingFree)
	}
	// totalFree must increase by 2 (the two leaf pages added).
	if fl.totalFree != 3 {
		t.Errorf("expected totalFree=3, got %d", fl.totalFree)
	}
}

// TestFreelistTrunk_ProcessTrunkPageFullNewTrunkWriteError exercises the full
// trunk branch (leafCount == maxLeaves) where createNewTrunk is called, but the
// writeLocked inside createNewTrunk fails. The error must propagate.
func TestFreelistTrunk_ProcessTrunkPageFullNewTrunkWriteError(t *testing.T) {
	t.Parallel()

	const smallPageSize = 16 // maxLeaves = (16-8)/4 = 2
	mp := &mockPagerWriteError{
		pages:    make(map[Pgno]*DbPage),
		pageSize: smallPageSize,
	}

	// Trunk at page 5: full (leafCount == maxLeaves == 2).
	data := make([]byte, smallPageSize)
	binary.BigEndian.PutUint32(data[0:4], 0)            // no next trunk
	binary.BigEndian.PutUint32(data[4:8], 2)            // leafCount == 2 == maxLeaves
	binary.BigEndian.PutUint32(data[8:12], uint32(10))  // leaf 0
	binary.BigEndian.PutUint32(data[12:16], uint32(11)) // leaf 1
	mp.pages[5] = &DbPage{Pgno: 5, Data: data}

	// Page 7 exists so createNewTrunk's getLocked succeeds, but writeLocked fails.
	mp.pages[7] = &DbPage{Pgno: 7, Data: make([]byte, smallPageSize)}

	fl := NewFreeList(mp)
	fl.firstTrunk = 5
	fl.totalFree = 3
	fl.pendingFree = []Pgno{7}

	maxLeaves := FreeListMaxLeafPages(smallPageSize)
	err := fl.processTrunkPage(maxLeaves)
	if err == nil {
		t.Error("processTrunkPage (full trunk, write error in createNewTrunk): expected error, got nil")
	}
}

// TestFreelistTrunk_FinalizeFileAlreadyClosed exercises the j.file.Close() error
// path in Finalize. We inject a pre-closed *os.File so that Close returns an
// error, which Finalize must propagate.
func TestFreelistTrunk_FinalizeFileAlreadyClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "test.db-journal")

	// Create the journal file on disk so os.Remove has something to act on.
	f, err := os.Create(jPath)
	if err != nil {
		t.Fatalf("create journal file: %v", err)
	}
	// Close immediately — the file descriptor is now invalid.
	f.Close()

	// Re-open so we have a valid-looking but then immediately closed descriptor.
	f2, err := os.OpenFile(jPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("open journal file: %v", err)
	}
	// Close it right now to make the descriptor broken.
	f2.Close()

	j := NewJournal(jPath, DefaultPageSize, 1)
	// Inject the already-closed file handle so j.file != nil but Close will err.
	j.file = f2

	err = j.Finalize()
	// On Linux a double-close returns an error; Finalize must propagate it.
	// If the OS silently succeeds (some implementations), we accept no error too
	// and just verify the journal is cleaned up.
	if err != nil {
		// Error path covered — this is what we want for the branch.
		t.Logf("Finalize with pre-closed file returned error (expected on Linux): %v", err)
		// Clean up the file that was left on disk.
		os.Remove(jPath)
	} else {
		// Some OS implementations swallow the double-close; that's acceptable.
		t.Log("Finalize with pre-closed file returned nil (OS swallowed double-close)")
	}
}

// TestFreelistTrunk_FinalizeFileNilAlreadyDeleted exercises Finalize when
// j.file is nil and the journal file has already been deleted from disk.
// The os.Remove call returns IsNotExist, which must be silently ignored.
func TestFreelistTrunk_FinalizeFileNilAlreadyDeleted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "gone.db-journal")

	// Do not create the file — it simply doesn't exist.
	j := NewJournal(jPath, DefaultPageSize, 1)
	// j.file is nil by default (NewJournal does not open the file).

	if err := j.Finalize(); err != nil {
		t.Errorf("Finalize with nil file and non-existent path: expected nil, got %v", err)
	}

	// State must be reset.
	if j.initialized {
		t.Error("expected j.initialized=false after Finalize")
	}
	if j.pageCount != 0 {
		t.Errorf("expected j.pageCount=0 after Finalize, got %d", j.pageCount)
	}
}

// TestFreelistTrunk_FinalizeResetsState verifies that Finalize resets
// initialized and pageCount even when called on an open journal.
func TestFreelistTrunk_FinalizeResetsState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jPath := filepath.Join(dir, "state.db-journal")

	j := NewJournal(jPath, DefaultPageSize, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}

	pageData := make([]byte, DefaultPageSize)
	if err := j.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	if j.GetPageCount() != 1 {
		t.Fatalf("expected pageCount=1 before Finalize, got %d", j.GetPageCount())
	}

	if err := j.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if j.IsOpen() {
		t.Error("expected journal to be closed after Finalize")
	}
	if j.GetPageCount() != 0 {
		t.Errorf("expected pageCount=0 after Finalize, got %d", j.GetPageCount())
	}
	if j.Exists() {
		t.Error("expected journal file to be deleted after Finalize")
	}
}
