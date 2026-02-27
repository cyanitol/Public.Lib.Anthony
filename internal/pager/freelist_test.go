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
