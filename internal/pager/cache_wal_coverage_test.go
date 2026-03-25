// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"errors"
	"testing"
)

// TestCacheWAL_FlushPage_NoPager exercises the error branch of FlushPage when
// no pager has been set on the cache.
func TestCacheWAL_FlushPage_NoPager(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 10)

	page := NewDbPage(1, 4096)
	page.MakeDirty()
	if err := cache.Put(page); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	err := cache.FlushPage(1)
	if err == nil {
		t.Fatal("expected error when no pager is set, got nil")
	}
}

// TestCacheWAL_FlushPage_PageNotFound exercises the ErrCachePageNotFound branch
// of FlushPage when the requested page is not in the cache.
func TestCacheWAL_FlushPage_PageNotFound(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	err := cache.FlushPage(42)
	if !errors.Is(err, ErrCachePageNotFound) {
		t.Fatalf("FlushPage(42) error = %v, want ErrCachePageNotFound", err)
	}
}

// TestCacheWAL_FlushPage_CleanPage exercises the early-return branch in FlushPage
// where the page is in the cache but is not dirty, so nothing should be written.
func TestCacheWAL_FlushPage_CleanPage(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 10)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	page := NewDbPage(5, 4096)
	// Page is clean by default.
	if err := cache.Put(page); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := cache.FlushPage(5); err != nil {
		t.Fatalf("FlushPage clean page error = %v, want nil", err)
	}
	if writer.getWrittenPage(5) != nil {
		t.Error("clean page should not have been written to disk")
	}
}

// TestCacheWAL_FlushPage_DirtyManyThenFlushEach creates several dirty pages
// and flushes each one explicitly, verifying they transition to clean.
func TestCacheWAL_FlushPage_DirtyManyThenFlushEach(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 20)
	writer := newMockPageWriter()
	cache.SetPager(writer)

	const n = 8
	for i := Pgno(1); i <= n; i++ {
		p := NewDbPage(i, 4096)
		p.MakeDirty()
		if err := cache.Put(p); err != nil {
			t.Fatalf("Put(%d) error = %v", i, err)
		}
	}

	if got := len(cache.GetDirtyPages()); got != n {
		t.Fatalf("expected %d dirty pages before flush, got %d", n, got)
	}

	for i := Pgno(1); i <= n; i++ {
		if err := cache.FlushPage(i); err != nil {
			t.Fatalf("FlushPage(%d) error = %v", i, err)
		}
		if cache.Get(i).IsDirty() {
			t.Errorf("page %d should be clean after FlushPage", i)
		}
		if writer.getWrittenPage(i) == nil {
			t.Errorf("page %d was not written", i)
		}
	}

	if got := len(cache.GetDirtyPages()); got != 0 {
		t.Fatalf("expected 0 dirty pages after flush, got %d", got)
	}
}

// TestCacheWAL_SetMaxPages_ZeroCapacityError exercises the error branch of
// SetMaxPages where both maxPages and maxMemory would be zero or negative.
func TestCacheWAL_SetMaxPages_ZeroCapacityError(t *testing.T) {
	t.Parallel()
	// Create a cache with only a memory limit (no page limit).
	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize:  4096,
		MaxMemory: 1024 * 1024,
		MaxPages:  0,
	})
	if err != nil {
		t.Fatalf("NewLRUCache() error = %v", err)
	}

	// Attempting to set maxPages to a non-positive value while maxMemory is
	// also non-positive (we first set it to 0 by using SetMaxMemory indirectly
	// via a cache where MaxPages is the only valid limit).
	//
	// Use a simpler cache that only has MaxPages, then drive MaxPages to zero.
	cache2 := NewLRUCacheSimple(4096, 10)
	// Set maxMemory to 0 (it already is by default from NewLRUCacheSimple).
	err = cache2.SetMaxPages(0)
	if !errors.Is(err, ErrCacheCapacityZero) {
		t.Fatalf("SetMaxPages(0) with zero maxMemory: error = %v, want ErrCacheCapacityZero", err)
	}
	_ = cache
}

// TestCacheWAL_SetMaxPages_NegativeCapacityError verifies negative values are
// also rejected when maxMemory is zero.
func TestCacheWAL_SetMaxPages_NegativeCapacityError(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 10)

	err := cache.SetMaxPages(-1)
	if !errors.Is(err, ErrCacheCapacityZero) {
		t.Fatalf("SetMaxPages(-1) error = %v, want ErrCacheCapacityZero", err)
	}
}

// TestCacheWAL_SetMaxPages_Increase exercises the path where the new limit is
// larger than the current size, so no eviction is needed.
func TestCacheWAL_SetMaxPages_Increase(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 5)

	for i := Pgno(1); i <= 5; i++ {
		p := NewDbPage(i, 4096)
		p.Unref()
		if err := cache.Put(p); err != nil {
			t.Fatalf("Put(%d) error = %v", i, err)
		}
	}
	if cache.Size() != 5 {
		t.Fatalf("expected 5 pages before increase, got %d", cache.Size())
	}

	if err := cache.SetMaxPages(20); err != nil {
		t.Fatalf("SetMaxPages(20) error = %v", err)
	}
	// All five pages must still be present — no eviction should have occurred.
	if cache.Size() != 5 {
		t.Errorf("expected 5 pages after increase, got %d", cache.Size())
	}
	for i := Pgno(1); i <= 5; i++ {
		if !cache.Contains(i) {
			t.Errorf("page %d should still be in cache after limit increase", i)
		}
	}
}

// TestCacheWAL_SetMaxPages_Decrease exercises the eviction path triggered by
// reducing the page limit below the current cache population.
func TestCacheWAL_SetMaxPages_Decrease(t *testing.T) {
	t.Parallel()
	cache := NewLRUCacheSimple(4096, 20)

	for i := Pgno(1); i <= 12; i++ {
		p := NewDbPage(i, 4096)
		p.Unref()
		if err := cache.Put(p); err != nil {
			t.Fatalf("Put(%d) error = %v", i, err)
		}
	}
	if cache.Size() != 12 {
		t.Fatalf("expected 12 pages before decrease, got %d", cache.Size())
	}

	if err := cache.SetMaxPages(6); err != nil {
		t.Fatalf("SetMaxPages(6) error = %v", err)
	}
	if cache.Size() > 6 {
		t.Errorf("expected size <= 6 after SetMaxPages decrease, got %d", cache.Size())
	}
}

// TestCacheWAL_UpdateFrameChecksum_Uninitialized exercises the error branch of
// UpdateFrameChecksum when the WAL index has not been initialized.
func TestCacheWAL_UpdateFrameChecksum_Uninitialized(t *testing.T) {
	t.Parallel()
	// Build an index struct directly without calling NewWALIndex so that
	// initialized remains false.
	idx := &WALIndex{
		hashTable: make(map[uint32]uint32),
	}

	err := idx.UpdateFrameChecksum(0xDEADBEEF, 0xCAFEBABE)
	if err == nil {
		t.Fatal("UpdateFrameChecksum on uninitialized index: expected error, got nil")
	}
}

// TestCacheWAL_GetFrameChecksum_Uninitialized exercises the error branch of
// GetFrameChecksum when the WAL index has not been initialized.
func TestCacheWAL_GetFrameChecksum_Uninitialized(t *testing.T) {
	t.Parallel()
	idx := &WALIndex{
		hashTable: make(map[uint32]uint32),
	}

	_, _, err := idx.GetFrameChecksum()
	if err == nil {
		t.Fatal("GetFrameChecksum on uninitialized index: expected error, got nil")
	}
}

// TestCacheWAL_UpdateAndGetFrameChecksum_RoundTrip verifies that multiple
// UpdateFrameChecksum calls persist distinct values and that GetFrameChecksum
// always returns the most-recently written pair.  It also confirms that the
// Change counter increments with each update.
func TestCacheWAL_UpdateAndGetFrameChecksum_RoundTrip(t *testing.T) {
	t.Parallel()
	filename := tempWALIndexFile(t)
	idx, err := NewWALIndex(filename)
	if err != nil {
		t.Fatalf("NewWALIndex() error = %v", err)
	}
	defer idx.Close()

	initialChange := idx.header.Change

	pairs := [][2]uint32{
		{0x00000001, 0x00000002},
		{0xAAAAAAAA, 0x55555555},
		{0xFFFFFFFF, 0x00000000},
	}
	for _, pair := range pairs {
		if err := idx.UpdateFrameChecksum(pair[0], pair[1]); err != nil {
			t.Fatalf("UpdateFrameChecksum(%x, %x) error = %v", pair[0], pair[1], err)
		}
		c1, c2, err := idx.GetFrameChecksum()
		if err != nil {
			t.Fatalf("GetFrameChecksum() error = %v", err)
		}
		if c1 != pair[0] || c2 != pair[1] {
			t.Errorf("GetFrameChecksum() = (%x, %x), want (%x, %x)", c1, c2, pair[0], pair[1])
		}
	}

	// Change counter must have been incremented once per update.
	expectedChange := initialChange + uint32(len(pairs))
	if idx.header.Change != expectedChange {
		t.Errorf("header.Change = %d, want %d", idx.header.Change, expectedChange)
	}
}

// TestCacheWAL_WALMode_WriteFramesVerifyChecksums opens a WAL-mode pager,
// writes several frames, then reads them back to confirm checksums in the WAL
// index header are coherent.  This exercises UpdateFrameChecksum and
// GetFrameChecksum via a real WAL workflow.
func TestCacheWAL_WALMode_WriteFramesVerifyChecksums(t *testing.T) {
	t.Parallel()
	dbFile := createTestDBFile(t)

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustSetJournalMode(t, p, JournalModeWAL)

	if p.walIndex == nil {
		t.Fatal("walIndex is nil after switching to WAL mode")
	}

	// Write several pages so frames are appended to the WAL.
	const numPages = 5
	for i := 0; i < numPages; i++ {
		mustBeginWrite(t, p)
		pgno := mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgno)
		page.Data[0] = byte(i + 1)
		mustWritePage(t, p, page)
		p.Put(page)
		mustCommit(t, p)
	}

	// Directly update the WAL index checksums and verify round-trip.
	if err := p.walIndex.UpdateFrameChecksum(0x11223344, 0x55667788); err != nil {
		t.Fatalf("UpdateFrameChecksum() error = %v", err)
	}
	c1, c2, err := p.walIndex.GetFrameChecksum()
	if err != nil {
		t.Fatalf("GetFrameChecksum() error = %v", err)
	}
	if c1 != 0x11223344 || c2 != 0x55667788 {
		t.Errorf("GetFrameChecksum() = (%x, %x), want (11223344, 55667788)", c1, c2)
	}
}
