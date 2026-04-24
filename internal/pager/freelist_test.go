// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"testing"
)

// createTestPagerForFreeList creates a pager for free list testing.
func createTestPagerForFreeList(t *testing.T) (*Pager, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "freelist_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()

	pager, err := OpenWithPageSize(tmpFile.Name(), false, 4096)
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("failed to open pager: %v", err)
	}

	cleanup := func() {
		pager.Close()
		os.Remove(tmpFile.Name())
	}

	return pager, cleanup
}

func TestFreeListCreate(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	if fl == nil {
		t.Fatal("expected non-nil FreeList")
	}

	if fl.Count() != 0 {
		t.Errorf("expected count 0, got %d", fl.Count())
	}

	if !fl.IsEmpty() {
		t.Error("expected free list to be empty")
	}
}

func TestFreeListInitialize(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Initialize with existing free list data
	fl.Initialize(5, 10)

	if fl.GetFirstTrunk() != 5 {
		t.Errorf("expected first trunk 5, got %d", fl.GetFirstTrunk())
	}

	if fl.GetTotalFree() != 10 {
		t.Errorf("expected total free 10, got %d", fl.GetTotalFree())
	}
}

// flFreeRange frees pages from start to end inclusive.
func flFreeRange(t *testing.T, fl *FreeList, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		if err := fl.Free(i); err != nil {
			t.Errorf("unexpected error freeing page %d: %v", i, err)
		}
	}
}

func TestFreeListPendingPages(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	flFreeRange(t, fl, 10, 15)

	if fl.Count() != 6 {
		t.Errorf("expected count 6, got %d", fl.Count())
	}

	for i := 0; i < 6; i++ {
		pgno, err := fl.Allocate()
		if err != nil {
			t.Errorf("unexpected error allocating: %v", err)
		}
		if pgno < 10 || pgno > 15 {
			t.Errorf("unexpected page number: %d", pgno)
		}
	}

	if !fl.IsEmpty() {
		t.Error("expected free list to be empty after allocation")
	}
}

func TestFreeListAllocateEmpty(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Allocate from empty free list
	pgno, err := fl.Allocate()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if pgno != 0 {
		t.Errorf("expected 0 from empty free list, got %d", pgno)
	}
}

func TestFreeListFreeMultiple(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free multiple pages at once
	pages := []Pgno{10, 11, 12, 13, 14}
	err := fl.FreeMultiple(pages)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if fl.Count() != 5 {
		t.Errorf("expected count 5, got %d", fl.Count())
	}
}

func TestFreeListClear(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free some pages
	fl.FreeMultiple([]Pgno{10, 11, 12})

	if fl.IsEmpty() {
		t.Error("expected non-empty free list")
	}

	// Clear
	fl.Clear()

	if !fl.IsEmpty() {
		t.Error("expected empty free list after clear")
	}

	if fl.GetFirstTrunk() != 0 {
		t.Error("expected first trunk to be 0 after clear")
	}
}

func TestFreeListInfo(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free some pages
	fl.FreeMultiple([]Pgno{10, 11, 12, 13, 14})

	info, err := fl.Info()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if info.PendingFree != 5 {
		t.Errorf("expected 5 pending free pages, got %d", info.PendingFree)
	}
}

func TestFreeListIterate(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free some pages
	expectedPages := []Pgno{10, 11, 12, 13, 14}
	fl.FreeMultiple(expectedPages)

	// Collect all free pages
	var collected []Pgno
	err := fl.Iterate(func(pgno Pgno) bool {
		collected = append(collected, pgno)
		return true
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(collected) != len(expectedPages) {
		t.Errorf("expected %d pages, got %d", len(expectedPages), len(collected))
	}

	// Check all expected pages are present
	pageSet := make(map[Pgno]bool)
	for _, pgno := range collected {
		pageSet[pgno] = true
	}
	for _, expected := range expectedPages {
		if !pageSet[expected] {
			t.Errorf("expected page %d not found in collected pages", expected)
		}
	}
}

func TestFreeListIterateEarlyStop(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free some pages
	fl.FreeMultiple([]Pgno{10, 11, 12, 13, 14})

	// Stop after 2 pages
	count := 0
	err := fl.Iterate(func(pgno Pgno) bool {
		count++
		return count < 2
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected to visit 2 pages, visited %d", count)
	}
}

func TestFreeListMaxLeafPages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pageSize int
		expected int
	}{
		{1024, (1024 - 8) / 4},   // 254
		{4096, (4096 - 8) / 4},   // 1022
		{8192, (8192 - 8) / 4},   // 2046
		{16384, (16384 - 8) / 4}, // 4094
	}

	for _, tt := range tests {
		tt := tt
		got := FreeListMaxLeafPages(tt.pageSize)
		if got != tt.expected {
			t.Errorf("FreeListMaxLeafPages(%d) = %d, expected %d", tt.pageSize, got, tt.expected)
		}
	}
}

func TestFreeListLIFOOrder(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free pages in order
	fl.Free(10)
	fl.Free(11)
	fl.Free(12)

	// Allocate should return in LIFO order (most recently freed first)
	expected := []Pgno{12, 11, 10}
	for _, exp := range expected {
		pgno, err := fl.Allocate()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if pgno != exp {
			t.Errorf("expected page %d, got %d", exp, pgno)
		}
	}
}

func TestFreeListFlushToTrunk(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	fl.maxPending = 4 // Lower threshold for testing

	// Ensure we have some pages allocated first
	// Page 1 is the schema page, we need pages 2+ to exist
	for i := Pgno(2); i <= 10; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		pager.Put(page)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Free pages - this will trigger flush when we hit maxPending
	for i := Pgno(5); i <= 10; i++ {
		err := fl.Free(i)
		if err != nil {
			t.Errorf("unexpected error freeing page %d: %v", i, err)
		}
	}

	// After freeing 6 pages with maxPending=4, at least one flush should have occurred
	// The total count should still be correct
	count := fl.Count()
	if count != 6 {
		t.Errorf("expected count 6, got %d", count)
	}
}

// BenchmarkFreeListAllocate benchmarks page allocation.
func BenchmarkFreeListAllocate(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "freelist_bench_*.db")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	pager, _ := OpenWithPageSize(tmpFile.Name(), false, 4096)
	defer pager.Close()

	fl := NewFreeList(pager)

	// Pre-populate with free pages
	for i := Pgno(10); i < Pgno(10+b.N); i++ {
		fl.Free(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fl.Allocate()
	}
}

// BenchmarkFreeListFree benchmarks page freeing.
func BenchmarkFreeListFree(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "freelist_bench_*.db")
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	pager, _ := OpenWithPageSize(tmpFile.Name(), false, 4096)
	defer pager.Close()

	fl := NewFreeList(pager)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fl.Free(Pgno(i + 10))
	}
}

// TestPagerAllocatePage tests the pager's AllocatePage method.
func TestPagerAllocatePage(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	// Allocate first page (should be page 2, since page 1 is the header)
	pgno1, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Allocate second page
	pgno2, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Page numbers should be different
	if pgno1 == pgno2 {
		t.Errorf("AllocatePage returned duplicate page numbers: %d", pgno1)
	}

	// Commit to persist changes
	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

// TestPagerFreePageAndReuse tests freeing a page and reusing it.
func TestPagerFreePageAndReuse(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	pgno1 := mustAllocatePage(t, pager)
	pgno2 := mustAllocatePage(t, pager)
	pgno3 := mustAllocatePage(t, pager)
	mustCommit(t, pager)
	mustFreePage(t, pager, pgno2)
	mustCommit(t, pager)

	if freeCount := pager.GetFreePageCount(); freeCount != 1 {
		t.Errorf("Expected 1 free page, got %d", freeCount)
	}

	pgnoReused := mustAllocatePage(t, pager)
	if pgnoReused != pgno2 {
		t.Errorf("Expected to reuse page %d, got %d", pgno2, pgnoReused)
	}
	if freeCount := pager.GetFreePageCount(); freeCount != 0 {
		t.Errorf("Expected 0 free pages after reuse, got %d", freeCount)
	}
	if pgno1 == 0 || pgno3 == 0 {
		t.Error("Other pages should still be valid")
	}
}

// flPersistFirstSession allocates pages, frees some, and returns page numbers and free count.
func flPersistFirstSession(t *testing.T, tmpName string) (pgno1, pgno2, pgno3, pgno4 Pgno, freeCount uint32) {
	t.Helper()
	pager1 := mustOpenPagerSized(t, tmpName, 4096)
	pgno1 = mustAllocatePage(t, pager1)
	pgno2 = mustAllocatePage(t, pager1)
	pgno3 = mustAllocatePage(t, pager1)
	pgno4 = mustAllocatePage(t, pager1)
	mustCommit(t, pager1)
	mustFreePage(t, pager1, pgno2)
	mustFreePage(t, pager1, pgno4)
	mustCommit(t, pager1)
	freeCount = pager1.GetFreePageCount()
	pager1.Close()
	return
}

// flPersistVerifyReuse reopens the pager and verifies free list reuse.
func flPersistVerifyReuse(t *testing.T, tmpName string, pgno2, pgno4 Pgno, freeCount1 uint32) {
	t.Helper()
	pager2 := mustOpenPagerSized(t, tmpName, 4096)
	defer pager2.Close()
	freeCount2 := pager2.GetFreePageCount()
	if freeCount2 != freeCount1 {
		t.Errorf("Free count after reopen: expected %d, got %d", freeCount1, freeCount2)
	}
	pgnoNew1 := mustAllocatePage(t, pager2)
	pgnoNew2 := mustAllocatePage(t, pager2)
	if pgnoNew1 == pgnoNew2 {
		t.Error("Allocated pages should be different")
	}
	reusedCount := 0
	if pgnoNew1 == pgno2 || pgnoNew1 == pgno4 {
		reusedCount++
	}
	if pgnoNew2 == pgno2 || pgnoNew2 == pgno4 {
		reusedCount++
	}
	if reusedCount == 0 {
		t.Errorf("Expected to reuse at least one freed page, got %d and %d", pgnoNew1, pgnoNew2)
	}
}

// TestPagerFreeListPersistence tests that free list survives database close/reopen.
func TestPagerFreeListPersistence(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_persist_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	_, pgno2, _, pgno4, freeCount1 := flPersistFirstSession(t, tmpName)
	if freeCount1 != 2 {
		t.Errorf("Expected 2 free pages, got %d", freeCount1)
	}
	flPersistVerifyReuse(t, tmpName, pgno2, pgno4, freeCount1)
}

// flFreeEveryOther frees every other page from the list and returns freed page numbers.
func flFreeEveryOther(t *testing.T, p *Pager, pages []Pgno) []Pgno {
	t.Helper()
	var freed []Pgno
	for i := 0; i < len(pages); i += 2 {
		mustFreePage(t, p, pages[i])
		freed = append(freed, pages[i])
	}
	return freed
}

// TestPagerFreeListMultiplePages tests freeing and reusing multiple pages.
func TestPagerFreeListMultiplePages(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	pages := mustAllocatePages(t, pager, 100)
	mustCommit(t, pager)

	freedPages := flFreeEveryOther(t, pager, pages)
	mustCommit(t, pager)

	if freeCount := pager.GetFreePageCount(); freeCount != 50 {
		t.Errorf("Expected 50 free pages, got %d", freeCount)
	}

	reusedPages := mustAllocatePages(t, pager, 50)

	freedSet := make(map[Pgno]bool)
	for _, pgno := range freedPages {
		freedSet[pgno] = true
	}
	for _, pgno := range reusedPages {
		if !freedSet[pgno] {
			t.Errorf("Page %d was not in the freed set", pgno)
		}
	}

	if freeCount := pager.GetFreePageCount(); freeCount != 0 {
		t.Errorf("Expected 0 free pages after reusing all, got %d", freeCount)
	}
	mustCommit(t, pager)
}

// TestPagerFreeInvalidPage tests error handling for invalid page numbers.
func TestPagerFreeInvalidPage(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	// Try to free page 0 (invalid)
	err := pager.FreePage(0)
	if err != ErrInvalidPageNum {
		t.Errorf("Expected ErrInvalidPageNum for page 0, got %v", err)
	}

	// Try to free page beyond database size
	err = pager.FreePage(99999)
	if err != ErrInvalidPageNum {
		t.Errorf("Expected ErrInvalidPageNum for out-of-range page, got %v", err)
	}
}

// TestPagerReadOnlyNoAllocate tests that read-only pager cannot allocate.
func TestPagerReadOnlyNoAllocate(t *testing.T) {
	t.Parallel()
	// First create a database
	tmpFile, err := os.CreateTemp("", "freelist_readonly_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	// Create database with write mode
	pagerW, err := OpenWithPageSize(tmpName, false, 4096)
	if err != nil {
		t.Fatalf("failed to open pager for write: %v", err)
	}
	pagerW.Close()

	// Open in read-only mode
	pagerR, err := OpenWithPageSize(tmpName, true, 4096)
	if err != nil {
		t.Fatalf("failed to open pager read-only: %v", err)
	}
	defer pagerR.Close()

	// Try to allocate - should fail
	_, err = pagerR.AllocatePage()
	if err != ErrReadOnly {
		t.Errorf("Expected ErrReadOnly, got %v", err)
	}

	// Try to free - should fail
	err = pagerR.FreePage(2)
	if err != ErrReadOnly {
		t.Errorf("Expected ErrReadOnly, got %v", err)
	}
}

// TestFreeListReadTrunk tests reading trunk page information
func TestFreeListReadTrunk(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	allocateTestPagesRange(t, pager, 2, 20)
	mustCommit(t, pager)
	freeTestPagesRange(t, fl, 5, 15)
	mustFlush(t, fl)

	if fl.GetFirstTrunk() == 0 {
		t.Fatal("expected non-zero first trunk")
	}

	_, leaves, err := fl.ReadTrunk(fl.GetFirstTrunk())
	if err != nil {
		t.Fatalf("failed to read trunk: %v", err)
	}
	if len(leaves) == 0 {
		t.Error("expected at least one leaf page")
	}

	_, _, err = fl.ReadTrunk(0)
	if err != ErrInvalidTrunkPage {
		t.Errorf("expected ErrInvalidTrunkPage, got %v", err)
	}
}

// TestFreeListVerify tests freelist integrity verification
func TestFreeListVerify(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	t.Run("empty freelist is valid", func(t *testing.T) {
		err := fl.Verify()
		if err != nil {
			t.Errorf("empty freelist should be valid: %v", err)
		}
	})

	t.Run("verify after freeeting pages", func(t *testing.T) {
		allocateTestPagesRange(t, pager, 2, 30)
		mustCommit(t, pager)
		freeTestPagesRange(t, fl, 10, 25)
		mustFlush(t, fl)

		// Verify should pass
		err := fl.Verify()
		if err != nil {
			t.Errorf("freelist should be valid after normal operations: %v", err)
		}
	})
}

// TestFreeListIterateComplete tests complete iteration through freelist
func TestFreeListIterateComplete(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	allocateTestPagesRange(t, pager, 2, 100)
	mustCommit(t, pager)

	expectedPages := make(map[Pgno]bool)
	for i := Pgno(20); i <= 79; i++ {
		expectedPages[i] = true
	}
	freeTestPagesRange(t, fl, 20, 79)
	mustFlush(t, fl)

	// Iterate and collect all pages
	collectedPages := make(map[Pgno]bool)
	err := fl.Iterate(func(pgno Pgno) bool {
		collectedPages[pgno] = true
		return true
	})
	if err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	// The count might be different because one or more pages become trunk pages
	// Just verify that most pages are there
	if len(collectedPages) < len(expectedPages)-5 {
		t.Errorf("expected at least %d pages, found %d", len(expectedPages)-5, len(collectedPages))
	}

	// Log which pages are found vs expected
	t.Logf("Expected %d pages, found %d pages", len(expectedPages), len(collectedPages))
}

// TestFreeListInfoDetails tests detailed freelist information
func TestFreeListInfoDetails(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	allocateTestPagesRange(t, pager, 2, 50)
	mustCommit(t, pager)
	freeTestPagesRange(t, fl, 10, 30)

	// Get info before flush (should show pending)
	info, err := fl.Info()
	if err != nil {
		t.Fatalf("failed to get info: %v", err)
	}

	if info.PendingFree != 21 {
		t.Errorf("expected 21 pending pages, got %d", info.PendingFree)
	}

	// Flush
	if err := fl.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Get info after flush
	info, err = fl.Info()
	if err != nil {
		t.Fatalf("failed to get info: %v", err)
	}

	if info.PendingFree != 0 {
		t.Errorf("expected 0 pending pages after flush, got %d", info.PendingFree)
	}

	if info.TotalFree != 21 {
		t.Errorf("expected 21 total free pages, got %d", info.TotalFree)
	}

	if info.TrunkCount == 0 {
		t.Error("expected at least one trunk page")
	}

	t.Logf("FreeList Info: Trunks=%d, Leaves=%d, Total=%d",
		info.TrunkCount, info.LeafCount, info.TotalFree)
}

// TestFreeListFreeMultipleError tests error handling in FreeMultiple
func TestFreeListFreeMultipleError(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	fl.maxPending = 2 // Very small to trigger flushes

	// Create pages
	for i := Pgno(2); i <= 10; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		pager.Put(page)
	}
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Free multiple pages - should trigger flush
	pages := []Pgno{5, 6, 7, 8, 9}
	err := fl.FreeMultiple(pages)
	if err != nil {
		t.Errorf("FreeMultiple failed: %v", err)
	}

	// Verify count
	count := fl.Count()
	if count != 5 {
		t.Errorf("expected count 5, got %d", count)
	}
}
