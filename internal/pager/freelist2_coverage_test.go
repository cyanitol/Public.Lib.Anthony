// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"testing"
)

// TestFreelist2_FreeMultipleEmptySlice exercises FreeMultiple with an empty
// slice, confirming the loop body is never entered and the list stays empty.
func TestFreelist2_FreeMultipleEmptySlice(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	fl := NewFreeList(mp)

	if err := fl.FreeMultiple(nil); err != nil {
		t.Errorf("FreeMultiple(nil): unexpected error: %v", err)
	}
	if !fl.IsEmpty() {
		t.Error("expected free list to be empty after FreeMultiple(nil)")
	}

	if err := fl.FreeMultiple([]Pgno{}); err != nil {
		t.Errorf("FreeMultiple([]Pgno{}): unexpected error: %v", err)
	}
	if !fl.IsEmpty() {
		t.Error("expected free list to be empty after FreeMultiple([]Pgno{})")
	}
}

// TestFreelist2_FreeMultipleFlushError exercises the error return path inside
// FreeMultiple when flushPending fails. This covers the branch at line 192-194
// (the "return err" after flushPending fails within the loop).
//
// Strategy: use a mock whose getLocked fails so that when the pending list
// reaches maxPending and flushPending is called, processTrunkPage fails trying
// to read fl.firstTrunk — propagating the error out of FreeMultiple.
func TestFreelist2_FreeMultipleFlushError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	// mockPagerGetError always returns ErrPageNotFound from getLocked.
	mp := &mockPagerGetError{pageSize: pageSize}

	fl := NewFreeList(mp)
	fl.maxPending = 2 // flush triggers after 2 pages
	// Give fl a firstTrunk so flushPending doesn't try createNewTrunk.
	// With firstTrunk != 0 and getLocked failing, processTrunkPage will error.
	fl.firstTrunk = 99

	pages := []Pgno{10, 11, 12}
	err := fl.FreeMultiple(pages)
	if err == nil {
		t.Error("FreeMultiple: expected error when flushPending fails, got nil")
	}
}

// TestFreelist2_FlushPendingProcessTrunkError exercises the error return in
// flushPending when processTrunkPage returns an error (covers the branch at
// line 226-228 in flushPending). Uses the same getLocked-failing mock.
func TestFreelist2_FlushPendingProcessTrunkError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerGetError{pageSize: pageSize}

	fl := NewFreeList(mp)
	fl.firstTrunk = 5 // non-zero so flushPending enters the processTrunkPage branch
	fl.totalFree = 1
	fl.pendingFree = []Pgno{10}

	err := fl.Flush()
	if err == nil {
		t.Error("Flush/flushPending: expected error when processTrunkPage getLocked fails, got nil")
	}
}

// TestFreelist2_IterateGetLockedError exercises the getLocked error path in
// Iterate (lines 380-382). The pending list is empty so the loop over
// pendingFree is a no-op, then the on-disk iteration starts with trunkPgno!=0
// but getLocked immediately fails.
func TestFreelist2_IterateGetLockedError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerGetError{pageSize: pageSize}

	fl := NewFreeList(mp)
	fl.firstTrunk = 3 // non-zero so the on-disk loop is entered

	err := fl.Iterate(func(pgno Pgno) bool { return true })
	if err == nil {
		t.Error("Iterate: expected error when getLocked fails on trunk page, got nil")
	}
}

// TestFreelist2_IterateEmptyFreelist exercises the trunkPgno==0 path in
// Iterate: the on-disk loop body is never entered and nil is returned.
func TestFreelist2_IterateEmptyFreelist(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)
	fl := NewFreeList(mp)
	// firstTrunk == 0, no pending pages — completely empty.

	var visited []Pgno
	err := fl.Iterate(func(pgno Pgno) bool {
		visited = append(visited, pgno)
		return true
	})
	if err != nil {
		t.Errorf("Iterate on empty freelist: unexpected error: %v", err)
	}
	if len(visited) != 0 {
		t.Errorf("Iterate on empty freelist: expected 0 visits, got %d", len(visited))
	}
}

// TestFreelist2_AllocateFromDiskWriteLockedError exercises the writeLocked
// error path in allocateFromDisk (lines 153-155). The trunk page has leaf
// pages (leafCount > 0), so allocateFromDisk tries to call writeLocked to
// mark the trunk dirty, which must fail and be returned.
func TestFreelist2_AllocateFromDiskWriteLockedError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerWriteError{
		pages:    make(map[Pgno]*DbPage),
		pageSize: pageSize,
	}

	// Build a trunk page at pgno 2 with one leaf page (pgno 5).
	data := make([]byte, pageSize)
	binary.BigEndian.PutUint32(data[0:4], 0)          // no next trunk
	binary.BigEndian.PutUint32(data[4:8], 1)          // leafCount = 1
	binary.BigEndian.PutUint32(data[8:12], uint32(5)) // leaf[0] = page 5
	mp.pages[2] = &DbPage{Pgno: 2, Data: data}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 2 // trunk + 1 leaf

	pgno, err := fl.allocateFromDisk()
	if err == nil {
		t.Errorf("allocateFromDisk: expected error when writeLocked fails, got pgno=%d", pgno)
	}
}

// TestFreelist2_AllocateFromDiskGetLockedError exercises the getLocked error
// path in allocateFromDisk (lines 135-137).
func TestFreelist2_AllocateFromDiskGetLockedError(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := &mockPagerGetError{pageSize: pageSize}

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 1

	pgno, err := fl.allocateFromDisk()
	if err == nil {
		t.Errorf("allocateFromDisk: expected error when getLocked fails, got pgno=%d", pgno)
	}
}

// TestFreelist2_AllocateFromDiskNoLeaves exercises the "no leaf pages" branch
// in allocateFromDisk (leafCount == 0): the trunk page itself is allocated
// and firstTrunk advances to nextTrunk.
func TestFreelist2_AllocateFromDiskNoLeaves(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	mp := newMockPager(pageSize)

	// Trunk at page 2 with 0 leaves, pointing to next trunk at page 3.
	// Page 3 also exists as the second trunk.
	mp.addTrunkPage(2, 3, nil)
	mp.addTrunkPage(3, 0, nil)

	fl := NewFreeList(mp)
	fl.firstTrunk = 2
	fl.totalFree = 2

	pgno, err := fl.allocateFromDisk()
	if err != nil {
		t.Fatalf("allocateFromDisk (no leaves): unexpected error: %v", err)
	}
	// The trunk page itself (2) is returned.
	if pgno != 2 {
		t.Errorf("expected allocated pgno=2 (the trunk), got %d", pgno)
	}
	// firstTrunk should now point to the next trunk.
	if fl.firstTrunk != 3 {
		t.Errorf("expected firstTrunk=3 after trunk allocation, got %d", fl.firstTrunk)
	}
	if fl.totalFree != 1 {
		t.Errorf("expected totalFree=1, got %d", fl.totalFree)
	}
}
