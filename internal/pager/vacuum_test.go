// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// vacWriteTestPages writes value byte(i) to each page at appropriate offset.
func vacWriteTestPages(t *testing.T, pager *Pager, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		offset := 0
		if i == 1 {
			offset = DatabaseHeaderSize
		}
		mustWritePageAtOffset(t, pager, i, offset, []byte{byte(i)})
	}
}

// vacVerifyTestPages verifies value byte(i) on each page at appropriate offset.
func vacVerifyTestPages(t *testing.T, pager *Pager, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		offset := 0
		if i == 1 {
			offset = DatabaseHeaderSize
		}
		data := mustReadPageAtOffset(t, pager, i, offset, 1)
		if data[0] != byte(i) {
			t.Errorf("Page %d data after vacuum = %d, want %d", i, data[0], i)
		}
	}
}

func TestVacuum_BasicOperation(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	pager := openTestPagerAt(t, filename, false)
	// Start from page 2: page 1 is reserved for sqlite_master btree header
	vacWriteTestPages(t, pager, 2, 10)
	mustCommit(t, pager)

	initialSize := pager.dbSize
	if err := pager.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}

	vacVerifyTestPages(t, pager, 2, 10)

	if pager.dbSize > initialSize {
		t.Errorf("Database size after vacuum = %d, want <= %d", pager.dbSize, initialSize)
	}
	pager.Close()
}

// vacFreeHalfPages frees pages from start to end via the freelist.
func vacFreeHalfPages(t *testing.T, pager *Pager, start, end int) {
	t.Helper()
	if pager.freeList == nil {
		return
	}
	for i := start; i <= end; i++ {
		if err := pager.freeList.Free(Pgno(i)); err != nil {
			t.Logf("Free(%d) warning: %v", i, err)
		}
	}
	mustFlush(t, pager.freeList)
	mustCommit(t, pager)
}

func TestVacuum_AfterManyDeletes(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	pager := openTestPagerAt(t, filename, false)
	allocateTestPagesRange(t, pager, 1, 100)
	mustCommit(t, pager)

	sizeBeforeDelete := pager.dbSize
	vacFreeHalfPages(t, pager, 50, 100)

	if err := pager.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}

	freeCountAfter := uint32(0)
	if pager.freeList != nil {
		freeCountAfter = pager.freeList.Count()
	}
	if freeCountAfter != 0 {
		t.Errorf("Free pages after vacuum = %d, want 0", freeCountAfter)
	}
	if pager.dbSize >= sizeBeforeDelete {
		t.Logf("Warning: Database size not reduced: before=%d, after=%d", sizeBeforeDelete, pager.dbSize)
	}
	pager.Close()
}

// vacIntoWritePage writes a test page with data
func vacIntoWritePage(t *testing.T, pager *Pager, pgno Pgno, value byte) {
	t.Helper()
	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d) error = %v", pgno, err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write(page %d) error = %v", pgno, err)
	}

	offset := 0
	if pgno == 1 {
		offset = DatabaseHeaderSize
	}

	if err := page.Write(offset, []byte{value}); err != nil {
		t.Fatalf("page.Write(page %d) error = %v", pgno, err)
	}

	pager.Put(page)
}

// vacIntoVerifyPage verifies a page contains expected value
func vacIntoVerifyPage(t *testing.T, pager *Pager, pgno Pgno, expected byte) {
	t.Helper()
	page, err := pager.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d) error = %v", pgno, err)
	}
	defer pager.Put(page)

	offset := 0
	if pgno == 1 {
		offset = DatabaseHeaderSize
	}

	readData, err := page.Read(offset, 1)
	if err != nil {
		t.Fatalf("page.Read(page %d) error = %v", pgno, err)
	}

	if readData[0] != expected {
		t.Errorf("Page %d data = %d, want %d", pgno, readData[0], expected)
	}
}

func TestVacuum_Into(t *testing.T) {
	t.Parallel()
	sourceFile := filepath.Join(t.TempDir(), "source.db")
	targetFile := filepath.Join(t.TempDir(), "target.db")

	pager, err := Open(sourceFile, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Start from page 2: page 1 is reserved for sqlite_master btree header
	for i := Pgno(2); i <= 5; i++ {
		vacIntoWritePage(t, pager, i, byte(i*2))
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	opts := &VacuumOptions{IntoFile: targetFile}
	if err := pager.Vacuum(opts); err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}

	pager.Close()

	if _, err := os.Stat(targetFile); os.IsNotExist(err) {
		t.Fatal("Target file was not created")
	}

	targetPager, err := Open(targetFile, false)
	if err != nil {
		t.Fatalf("Open(target) error = %v", err)
	}
	defer targetPager.Close()

	for i := Pgno(2); i <= 5; i++ {
		vacIntoVerifyPage(t, targetPager, i, byte(i*2))
	}

	sourcePager, err := Open(sourceFile, false)
	if err != nil {
		t.Fatalf("Open(source) after vacuum error = %v", err)
	}
	defer sourcePager.Close()

	vacIntoVerifyPage(t, sourcePager, 2, 4)
}

func TestVacuum_ReadOnlyDatabase(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	// Create database
	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	pager.Close()

	// Reopen as read-only
	pager, err = Open(filename, true)
	if err != nil {
		t.Fatalf("Open(readonly) error = %v", err)
	}
	defer pager.Close()

	// VACUUM should fail on read-only database
	opts := &VacuumOptions{}
	err = pager.Vacuum(opts)
	if err != ErrReadOnly {
		t.Errorf("Vacuum() on read-only database error = %v, want %v", err, ErrReadOnly)
	}
}

func TestVacuum_DuringTransaction(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// Start a transaction
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// VACUUM should fail during transaction
	opts := &VacuumOptions{}
	err = pager.Vacuum(opts)
	if err != ErrTransactionOpen {
		t.Errorf("Vacuum() during transaction error = %v, want %v", err, ErrTransactionOpen)
	}

	pager.Put(page)
}

// vacWritePatternPages writes pattern + page digit to each page.
func vacWritePatternPages(t *testing.T, pager *Pager, pattern []byte, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		offset := 0
		if i == 1 {
			offset = DatabaseHeaderSize
		}
		data := append(pattern, byte('0'+i%10))
		mustWritePageAtOffset(t, pager, i, offset, data)
	}
}

// vacVerifyPatternPages verifies pattern + page digit on each page.
func vacVerifyPatternPages(t *testing.T, pager *Pager, pattern []byte, start, end Pgno) {
	t.Helper()
	for i := start; i <= end; i++ {
		offset := 0
		if i == 1 {
			offset = DatabaseHeaderSize
		}
		readData := mustReadPageAtOffset(t, pager, i, offset, len(pattern)+1)
		expectedData := append(pattern, byte('0'+i%10))
		for j := range expectedData {
			if readData[j] != expectedData[j] {
				t.Errorf("Page %d byte %d after vacuum = %d, want %d", i, j, readData[j], expectedData[j])
			}
		}
	}
}

func TestVacuum_DataIntegrity(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	pager := openTestPagerAt(t, filename, false)
	pattern := []byte("INTEGRITY_TEST_PATTERN_")
	// Start from page 2: page 1 is reserved for sqlite_master btree header
	vacWritePatternPages(t, pager, pattern, 2, 20)
	mustCommit(t, pager)

	if err := pager.Vacuum(&VacuumOptions{}); err != nil {
		t.Fatalf("Vacuum() error = %v", err)
	}
	vacVerifyPatternPages(t, pager, pattern, 2, 20)
	pager.Close()
}

func TestVacuum_EmptyDatabase(t *testing.T) {
	t.Parallel()
	filename := filepath.Join(t.TempDir(), "test.db")

	pager, err := Open(filename, false)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer pager.Close()

	// VACUUM on empty database should succeed
	opts := &VacuumOptions{}
	if err := pager.Vacuum(opts); err != nil {
		t.Errorf("Vacuum() on empty database error = %v", err)
	}

	// Database should still be valid
	if pager.dbSize < 1 {
		t.Errorf("Database size after vacuum = %d, want >= 1", pager.dbSize)
	}
}
