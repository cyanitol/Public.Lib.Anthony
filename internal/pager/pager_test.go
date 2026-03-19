// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tempFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}

func TestOpen_NewDatabase(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	if pager.pageSize != DefaultPageSize {
		t.Errorf("pageSize = %d, want %d", pager.pageSize, DefaultPageSize)
	}

	if pager.readOnly {
		t.Error("pager should not be read-only")
	}

	if pager.state != PagerStateOpen {
		t.Errorf("state = %d, want %d", pager.state, PagerStateOpen)
	}

	// Check that file was created
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}

	// Check header
	header := pager.GetHeader()
	if header == nil {
		t.Fatal("header is nil")
	}

	if header.GetPageSize() != DefaultPageSize {
		t.Errorf("header page size = %d, want %d", header.GetPageSize(), DefaultPageSize)
	}
}

func TestOpen_ExistingDatabase(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager1 := openTestPagerAt(t, filename, false)
	testData := []byte("Test data")
	mustWritePageAtOffset(t, pager1, 1, DatabaseHeaderSize, testData)
	mustCommit(t, pager1)
	pager1.Close()

	pager2 := openTestPagerAt(t, filename, false)
	defer pager2.Close()
	readData := mustReadPageAtOffset(t, pager2, 1, DatabaseHeaderSize, len(testData))
	if !bytes.Equal(readData, testData) {
		t.Errorf("Read data = %v, want %v", readData, testData)
	}
}

func TestOpen_ReadOnly(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	// Create database
	pager1, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	pager1.Close()

	// Open read-only
	pager2, err := Open(filename, true)
	if err != nil {
		t.Fatalf("Open() read-only error = %v", err)
	}
	defer pager2.Close()

	if !pager2.IsReadOnly() {
		t.Error("pager should be read-only")
	}

	// Try to write (should fail)
	page, err := pager2.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	err = pager2.Write(page)
	if err == nil {
		t.Error("Write() on read-only pager should fail")
	}
}

func TestOpen_NonExistentReadOnly(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	_, err := Open(filename, true)
	if err == nil {
		t.Error("Open() read-only on non-existent file should fail")
	}
}

func TestOpenWithPageSize(t *testing.T) {
	t.Parallel()
	pageSizes := []int{512, 1024, 2048, 4096, 8192}

	for _, pageSize := range pageSizes {
		t.Run("pagesize_"+string(rune(pageSize)), func(t *testing.T) {
			filename := tempFile(t)

			pager, err := OpenWithPageSize(filename, false, pageSize)
			if err != nil {
				t.Fatalf("OpenWithPageSize() error = %v", err)
			}
			defer pager.Close()

			if pager.PageSize() != pageSize {
				t.Errorf("PageSize() = %d, want %d", pager.PageSize(), pageSize)
			}
		})
	}
}

func TestOpenWithPageSize_InvalidSize(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	invalidSizes := []int{0, 256, 4000, 131072}

	for _, size := range invalidSizes {
		t.Run("invalid_size_"+string(rune(size)), func(t *testing.T) {
			_, err := OpenWithPageSize(filename, false, size)
			if err == nil {
				t.Error("OpenWithPageSize() with invalid size should fail")
			}
		})
	}
}

func TestPager_Get(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Get page 1
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get(1) error = %v", err)
	}

	if page.Pgno != 1 {
		t.Errorf("page.Pgno = %d, want 1", page.Pgno)
	}

	if page.GetRefCount() < 1 {
		t.Error("page should have positive reference count")
	}

	pager.Put(page)
}

func TestPager_Get_InvalidPageNumber(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Get page 0 (invalid)
	_, err = pager.Get(0)
	if err == nil {
		t.Error("Get(0) should fail")
	}
}

func TestPager_WriteAndCommit(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Get page
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Write to page
	testData := []byte("Hello, World!")
	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := page.Write(DatabaseHeaderSize, testData); err != nil {
		t.Fatalf("page.Write() error = %v", err)
	}

	// Commit
	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Verify state
	if pager.state != PagerStateOpen {
		t.Errorf("state after commit = %d, want %d", pager.state, PagerStateOpen)
	}
}

func TestPager_WriteAndRollback(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager := openTestPagerAt(t, filename, false)
	defer pager.Close()

	originalData := []byte("Original data")
	mustWritePageAtOffset(t, pager, 1, DatabaseHeaderSize, originalData)
	mustCommit(t, pager)

	// Start new transaction and modify
	modifiedData := []byte("Modified data")
	mustWritePageAtOffset(t, pager, 1, DatabaseHeaderSize, modifiedData)
	mustRollback(t, pager)

	readData := mustReadPageAtOffset(t, pager, 1, DatabaseHeaderSize, len(originalData))
	if !bytes.Equal(readData, originalData) {
		t.Errorf("Data after rollback = %v, want %v", readData, originalData)
	}
}

// pagerWritePageNumPages writes byte(i) to each page at appropriate offset (header-aware).
func pagerWritePageNumPages(t *testing.T, pager *Pager, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		offset := DatabaseHeaderSize
		if i > 1 {
			offset = 0
		}
		mustWritePageAtOffset(t, pager, Pgno(i), offset, []byte{byte(i)})
	}
}

// pagerVerifyPageNumPages verifies byte(i) on each page at appropriate offset.
func pagerVerifyPageNumPages(t *testing.T, pager *Pager, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		offset := DatabaseHeaderSize
		if i > 1 {
			offset = 0
		}
		data := mustReadPageAtOffset(t, pager, Pgno(i), offset, 1)
		if data[0] != byte(i) {
			t.Errorf("Page %d data = %d, want %d", i, data[0], i)
		}
	}
}

func TestPager_MultiplePages(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager := openTestPagerAt(t, filename, false)
	defer pager.Close()

	numPages := 10
	pagerWritePageNumPages(t, pager, numPages)
	mustCommit(t, pager)
	pagerVerifyPageNumPages(t, pager, numPages)

	if pager.PageCount() != Pgno(numPages) {
		t.Errorf("PageCount() = %d, want %d", pager.PageCount(), numPages)
	}
}

func TestPager_Close(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := pager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify state
	if pager.file != nil {
		t.Error("file should be nil after Close()")
	}
}

func TestPager_CloseWithActiveTransaction(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Start transaction
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Close should rollback
	if err := pager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify journal was cleaned up
	if _, err := os.Stat(pager.journalFilename); !os.IsNotExist(err) {
		t.Error("Journal file should be deleted after Close()")
	}
}

func TestPager_Cache(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Get page (first time - read from disk)
	page1, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Get same page again (should come from cache)
	page2, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() second time error = %v", err)
	}

	// Should be same page object
	if page1 != page2 {
		t.Error("Second Get() should return cached page")
	}

	pager.Put(page1)
	pager.Put(page2)
}

func TestPager_PageCount(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// New database should have at least 1 page
	if pager.PageCount() < 1 {
		t.Errorf("Initial PageCount() = %d, want >= 1", pager.PageCount())
	}
}

func TestPager_CommitWithoutWrite(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Commit without any writes should fail
	err = pager.Commit()
	if err == nil {
		t.Error("Commit() without transaction should fail")
	}
}

func TestPager_RollbackWithoutWrite(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Rollback without any writes should fail
	err = pager.Rollback()
	if err == nil {
		t.Error("Rollback() without transaction should fail")
	}
}

func TestPager_HeaderUpdates(t *testing.T) {
	t.Parallel()
	filename := tempFile(t)

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	initialChangeCounter := pager.header.FileChangeCounter

	// Write and commit
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := page.Write(DatabaseHeaderSize, []byte("test")); err != nil {
		t.Fatalf("page.Write() error = %v", err)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Verify change counter was incremented after commit
	if pager.header.FileChangeCounter <= initialChangeCounter {
		t.Errorf("FileChangeCounter not incremented after commit: got %d, initial %d",
			pager.header.FileChangeCounter, initialChangeCounter)
	}

	pager.Close()
}

func BenchmarkPager_Get(b *testing.B) {
	filename := filepath.Join(b.TempDir(), "bench.db")

	pager, err := Open(filename, false)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Pre-populate some pages
	for i := 1; i <= 100; i++ {
		page, _ := pager.Get(Pgno(i))
		pager.Put(page)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, _ := pager.Get(Pgno((i % 100) + 1))
		pager.Put(page)
	}
}

func BenchmarkPager_Write(b *testing.B) {
	filename := filepath.Join(b.TempDir(), "bench.db")

	pager, err := Open(filename, false)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	page, err := pager.Get(1)
	if err != nil {
		b.Fatalf("Get() error = %v", err)
	}

	data := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pager.Write(page)
		_ = page.Write(DatabaseHeaderSize, data)
	}
}

func BenchmarkPager_Commit(b *testing.B) {
	filename := filepath.Join(b.TempDir(), "bench.db")

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		pager, _ := Open(filename, false)
		page, _ := pager.Get(1)
		_ = pager.Write(page)
		_ = page.Write(DatabaseHeaderSize, []byte("data"))
		b.StartTimer()

		_ = pager.Commit()

		b.StopTimer()
		pager.Close()
		b.StartTimer()
	}
}
