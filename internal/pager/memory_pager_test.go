package pager

import (
	"testing"
)

func TestMemoryPagerOpen(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	if mp.PageSize() != DefaultPageSize {
		t.Errorf("got page size %d, want %d", mp.PageSize(), DefaultPageSize)
	}

	if mp.PageCount() != 1 {
		t.Errorf("got page count %d, want 1", mp.PageCount())
	}
}

func TestMemoryPagerGetPage(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Get page 1 (should exist with header)
	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("failed to get page 1: %v", err)
	}
	defer mp.Put(page)

	if page.Pgno != 1 {
		t.Errorf("got page number %d, want 1", page.Pgno)
	}

	// Verify header magic string
	magic := string(page.Data[0:16])
	if magic != "SQLite format 3\x00" {
		t.Errorf("invalid header magic: %q", magic)
	}
}

func TestMemoryPagerAllocate(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate a new page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	if pgno != 2 {
		t.Errorf("got page number %d, want 2", pgno)
	}

	if mp.PageCount() != 2 {
		t.Errorf("got page count %d, want 2", mp.PageCount())
	}
}

func TestMemoryPagerWriteAndRead(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	// Get the page
	page, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Mark it writeable
	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Write some data
	testData := []byte("Hello, Memory Pager!")
	copy(page.Data, testData)

	mp.Put(page)

	// Commit the transaction
	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Read the page back
	page2, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page again: %v", err)
	}
	defer mp.Put(page2)

	// Verify data
	if string(page2.Data[:len(testData)]) != string(testData) {
		t.Errorf("got data %q, want %q", page2.Data[:len(testData)], testData)
	}
}

func TestMemoryPagerTransaction(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate and write to a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	page, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("Original Data")
	copy(page.Data, testData)
	mp.Put(page)

	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start a new transaction and modify the page
	page, err = mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	modifiedData := []byte("Modified Data")
	copy(page.Data, modifiedData)
	mp.Put(page)

	// Rollback the transaction
	if err := mp.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Read the page again - should have original data
	page, err = mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page after rollback: %v", err)
	}
	defer mp.Put(page)

	if string(page.Data[:len(testData)]) != string(testData) {
		t.Errorf("after rollback got data %q, want %q", page.Data[:len(testData)], testData)
	}
}

func TestMemoryPagerSavepoint(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate and write to a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	// Begin write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	page, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	originalData := []byte("Original")
	copy(page.Data, originalData)
	mp.Put(page)

	// Create a savepoint
	if err := mp.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Modify the page
	page, err = mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	modifiedData := []byte("Modified")
	copy(page.Data, modifiedData)
	mp.Put(page)

	// Rollback to savepoint
	if err := mp.RollbackTo("sp1"); err != nil {
		t.Fatalf("failed to rollback to savepoint: %v", err)
	}

	// Read the page - should have original data
	page, err = mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer mp.Put(page)

	if string(page.Data[:len(originalData)]) != string(originalData) {
		t.Errorf("after savepoint rollback got %q, want %q", page.Data[:len(originalData)], originalData)
	}
}

func TestMemoryPagerFreePage(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Free the page
	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("failed to free page: %v", err)
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit after free: %v", err)
	}

	// Allocate again - should reuse the freed page
	newPgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page again: %v", err)
	}

	if newPgno != pgno {
		t.Errorf("expected to reuse page %d, but got %d", pgno, newPgno)
	}
}

func TestMemoryPagerMultiplePages(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Allocate multiple pages
	pageCount := 100
	pgnos := make([]Pgno, pageCount)

	for i := 0; i < pageCount; i++ {
		pgno, err := mp.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		pgnos[i] = pgno
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify we can read all pages
	for i, pgno := range pgnos {
		page, err := mp.Get(pgno)
		if err != nil {
			t.Fatalf("failed to get page %d (pgno %d): %v", i, pgno, err)
		}
		mp.Put(page)
	}

	// Check final page count
	if mp.PageCount() != Pgno(pageCount+1) { // +1 for the header page
		t.Errorf("got page count %d, want %d", mp.PageCount(), pageCount+1)
	}
}

func TestMemoryPagerInvalidPageSize(t *testing.T) {
	// Test invalid page sizes
	invalidSizes := []int{0, 256, 513, 100000}

	for _, size := range invalidSizes {
		_, err := OpenMemory(size)
		if err == nil {
			t.Errorf("expected error for page size %d, got nil", size)
		}
	}
}

func TestMemoryPagerReadOnly(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	if mp.IsReadOnly() {
		t.Error("memory pager should not be read-only by default")
	}
}

func TestMemoryPagerClose(t *testing.T) {
	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}

	// Allocate and write to a page
	pgno, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	page, err := mp.Get(pgno)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("Test Data")
	copy(page.Data, testData)
	mp.Put(page)

	// Close without committing
	if err := mp.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Verify pager is closed (subsequent operations should fail or be safe)
	// The memory should be released
}

func TestMemoryPagerInterface(t *testing.T) {
	// Verify MemoryPager implements PagerInterface
	var _ PagerInterface = (*MemoryPager)(nil)

	mp, err := OpenMemory(DefaultPageSize)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Test all interface methods
	var pager PagerInterface = mp

	if pager.PageSize() != DefaultPageSize {
		t.Errorf("PageSize() = %d, want %d", pager.PageSize(), DefaultPageSize)
	}

	if pager.IsReadOnly() {
		t.Error("IsReadOnly() = true, want false")
	}

	if pager.PageCount() != 1 {
		t.Errorf("PageCount() = %d, want 1", pager.PageCount())
	}

	header := pager.GetHeader()
	if header == nil {
		t.Error("GetHeader() = nil, want non-nil")
	}

	freeCount := pager.GetFreePageCount()
	if freeCount != 0 {
		t.Errorf("GetFreePageCount() = %d, want 0", freeCount)
	}
}
