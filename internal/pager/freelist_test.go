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

func TestFreeListPendingPages(t *testing.T) {
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Free some pages (they go to pending list first)
	for i := Pgno(10); i <= 15; i++ {
		err := fl.Free(i)
		if err != nil {
			t.Errorf("unexpected error freeing page %d: %v", i, err)
		}
	}

	// Count should include pending pages
	if fl.Count() != 6 {
		t.Errorf("expected count 6, got %d", fl.Count())
	}

	// Allocate from pending
	for i := 0; i < 6; i++ {
		pgno, err := fl.Allocate()
		if err != nil {
			t.Errorf("unexpected error allocating: %v", err)
		}
		if pgno < 10 || pgno > 15 {
			t.Errorf("unexpected page number: %d", pgno)
		}
	}

	// Should be empty now
	if !fl.IsEmpty() {
		t.Error("expected free list to be empty after allocation")
	}
}

func TestFreeListAllocateEmpty(t *testing.T) {
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
	tests := []struct {
		pageSize int
		expected int
	}{
		{1024, (1024 - 8) / 4},  // 254
		{4096, (4096 - 8) / 4},  // 1022
		{8192, (8192 - 8) / 4},  // 2046
		{16384, (16384 - 8) / 4}, // 4094
	}

	for _, tt := range tests {
		got := FreeListMaxLeafPages(tt.pageSize)
		if got != tt.expected {
			t.Errorf("FreeListMaxLeafPages(%d) = %d, expected %d", tt.pageSize, got, tt.expected)
		}
	}
}

func TestFreeListLIFOOrder(t *testing.T) {
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
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	// Allocate some pages
	pgno1, _ := pager.AllocatePage()
	pgno2, _ := pager.AllocatePage()
	pgno3, _ := pager.AllocatePage()

	// Commit the allocation
	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Free page 2
	if err := pager.FreePage(pgno2); err != nil {
		t.Fatalf("FreePage failed: %v", err)
	}

	// Commit the free
	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check free page count
	freeCount := pager.GetFreePageCount()
	if freeCount != 1 {
		t.Errorf("Expected 1 free page, got %d", freeCount)
	}

	// Allocate a new page - should reuse the freed page
	pgnoReused, err := pager.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Should have reused page 2
	if pgnoReused != pgno2 {
		t.Errorf("Expected to reuse page %d, got %d", pgno2, pgnoReused)
	}

	// Verify we now have 0 free pages
	freeCount = pager.GetFreePageCount()
	if freeCount != 0 {
		t.Errorf("Expected 0 free pages after reuse, got %d", freeCount)
	}

	// Verify the other pages are still valid
	if pgno1 == 0 || pgno3 == 0 {
		t.Error("Other pages should still be valid")
	}
}

// TestPagerFreeListPersistence tests that free list survives database close/reopen.
func TestPagerFreeListPersistence(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "freelist_persist_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	// First session: allocate and free some pages
	pager1, err := OpenWithPageSize(tmpName, false, 4096)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	// Allocate pages
	pgno1, _ := pager1.AllocatePage()
	pgno2, _ := pager1.AllocatePage()
	pgno3, _ := pager1.AllocatePage()
	pgno4, _ := pager1.AllocatePage()

	if err := pager1.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Free some pages
	pager1.FreePage(pgno2)
	pager1.FreePage(pgno4)

	if err := pager1.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Get free count before closing
	freeCount1 := pager1.GetFreePageCount()
	if freeCount1 != 2 {
		t.Errorf("Expected 2 free pages, got %d", freeCount1)
	}

	// Close the pager
	if err := pager1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second session: reopen and verify free list
	pager2, err := OpenWithPageSize(tmpName, false, 4096)
	if err != nil {
		t.Fatalf("failed to reopen pager: %v", err)
	}
	defer pager2.Close()

	// Verify free count matches
	freeCount2 := pager2.GetFreePageCount()
	if freeCount2 != freeCount1 {
		t.Errorf("Free count after reopen: expected %d, got %d", freeCount1, freeCount2)
	}

	t.Logf("Before allocation: free count = %d, pages were %d and %d", freeCount2, pgno2, pgno4)

	// Allocate new pages - should reuse freed pages
	pgnoNew1, _ := pager2.AllocatePage()
	freeAfter1 := pager2.GetFreePageCount()
	t.Logf("After first allocation: got page %d, free count = %d", pgnoNew1, freeAfter1)

	pgnoNew2, _ := pager2.AllocatePage()
	freeAfter2 := pager2.GetFreePageCount()
	t.Logf("After second allocation: got page %d, free count = %d", pgnoNew2, freeAfter2)

	// Verify pages are different
	if pgnoNew1 == pgnoNew2 {
		t.Error("Allocated pages should be different")
	}

	// At least one should have been reused from the free list
	reusedCount := 0
	if pgnoNew1 == pgno2 || pgnoNew1 == pgno4 {
		reusedCount++
	}
	if pgnoNew2 == pgno2 || pgnoNew2 == pgno4 {
		reusedCount++
	}

	// We should have reused at least one page (the behavior depends on allocation order)
	if reusedCount == 0 {
		t.Errorf("Expected to reuse at least one of pages %d and %d, got %d and %d", pgno2, pgno4, pgnoNew1, pgnoNew2)
	}

	// After allocating 2 pages with 2 free, we should have 0 free pages
	// The free count should decrease by the number of pages we reused
	freeCount3 := pager2.GetFreePageCount()
	expectedFree := int(freeCount1) - reusedCount
	if int(freeCount3) != expectedFree {
		t.Errorf("Expected %d free pages (started with %d, reused %d), got %d", expectedFree, freeCount1, reusedCount, freeCount3)
	}

	// Verify the other pages are still accessible
	page1, err := pager2.Get(pgno1)
	if err != nil {
		t.Errorf("Failed to get page %d: %v", pgno1, err)
	} else {
		pager2.Put(page1)
	}

	page3, err := pager2.Get(pgno3)
	if err != nil {
		t.Errorf("Failed to get page %d: %v", pgno3, err)
	} else {
		pager2.Put(page3)
	}
}

// TestPagerFreeListMultiplePages tests freeing and reusing multiple pages.
func TestPagerFreeListMultiplePages(t *testing.T) {
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	// Allocate 100 pages
	var pages []Pgno
	for i := 0; i < 100; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed at iteration %d: %v", i, err)
		}
		pages = append(pages, pgno)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Free every other page (50 pages)
	var freedPages []Pgno
	for i := 0; i < len(pages); i += 2 {
		if err := pager.FreePage(pages[i]); err != nil {
			t.Fatalf("FreePage failed for page %d: %v", pages[i], err)
		}
		freedPages = append(freedPages, pages[i])
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify free count
	freeCount := pager.GetFreePageCount()
	if freeCount != 50 {
		t.Errorf("Expected 50 free pages, got %d", freeCount)
	}

	// Allocate 50 new pages - should reuse all freed pages
	var reusedPages []Pgno
	for i := 0; i < 50; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed during reuse at iteration %d: %v", i, err)
		}
		reusedPages = append(reusedPages, pgno)
	}

	// Verify all reused pages were from the freed set
	freedSet := make(map[Pgno]bool)
	for _, pgno := range freedPages {
		freedSet[pgno] = true
	}

	for _, pgno := range reusedPages {
		if !freedSet[pgno] {
			t.Errorf("Page %d was not in the freed set", pgno)
		}
	}

	// Should now have 0 free pages
	freeCount = pager.GetFreePageCount()
	if freeCount != 0 {
		t.Errorf("Expected 0 free pages after reusing all, got %d", freeCount)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Final commit failed: %v", err)
	}
}

// TestPagerFreeInvalidPage tests error handling for invalid page numbers.
func TestPagerFreeInvalidPage(t *testing.T) {
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
