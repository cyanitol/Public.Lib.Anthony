// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"testing"
)

func TestJournalCreation(t *testing.T) {
	t.Parallel()
	journalFile := "test_journal.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	if !journal.IsOpen() {
		t.Error("journal should be open")
	}

	if journal.Exists() {
		if err := journal.Close(); err != nil {
			t.Fatalf("failed to close journal: %v", err)
		}
	}
}

func TestJournalWriteOriginal(t *testing.T) {
	t.Parallel()
	journalFile := "test_write_original.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Create test page data
	pageData := make([]byte, DefaultPageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write original page to journal
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	// Verify page count
	if journal.GetPageCount() != 1 {
		t.Errorf("expected page count 1, got %d", journal.GetPageCount())
	}

	// Write another page
	if err := journal.WriteOriginal(2, pageData); err != nil {
		t.Fatalf("failed to write second original: %v", err)
	}

	if journal.GetPageCount() != 2 {
		t.Errorf("expected page count 2, got %d", journal.GetPageCount())
	}
}

func TestJournalRollback(t *testing.T) {
	t.Parallel()
	t.Skip("Journal rollback not yet fully implemented")
	dbFile := "test_rollback.db"
	journalFile := dbFile + "-journal"
	defer os.Remove(dbFile)
	defer os.Remove(journalFile)

	pager := openTestPagerAt(t, dbFile, false)

	// Write initial data
	page := mustGetWritePage(t, pager, 1)
	originalData := make([]byte, len(page.Data))
	copy(originalData, page.Data)
	originalData[0] = 0xAA
	originalData[100] = 0xBB
	copy(page.Data, originalData)
	pager.Put(page)
	mustCommit(t, pager)

	// Start new transaction and modify
	mustBeginWrite(t, pager)
	page = mustGetWritePage(t, pager, 1)
	page.Data[0] = 0xFF
	page.Data[100] = 0xFF
	pager.Put(page)
	mustRollback(t, pager)

	// Verify data was restored
	page = mustGetPage(t, pager, 1)
	defer pager.Put(page)
	if page.Data[0] != 0xAA {
		t.Errorf("data not restored: expected 0xAA, got 0x%02X", page.Data[0])
	}
	if page.Data[100] != 0xBB {
		t.Errorf("data not restored: expected 0xBB, got 0x%02X", page.Data[100])
	}
	pager.Close()
}

func TestJournalFinalize(t *testing.T) {
	t.Parallel()
	journalFile := "test_finalize.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Write some data
	pageData := make([]byte, DefaultPageSize)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	// Finalize should delete the journal
	if err := journal.Finalize(); err != nil {
		t.Fatalf("failed to finalize: %v", err)
	}

	// Journal file should be deleted
	if journal.Exists() {
		t.Error("journal file should be deleted after finalize")
	}
}

func TestJournalDelete(t *testing.T) {
	t.Parallel()
	journalFile := "test_delete.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Delete journal
	if err := journal.Delete(); err != nil {
		t.Fatalf("failed to delete journal: %v", err)
	}

	// Journal should not exist
	if journal.Exists() {
		t.Error("journal should not exist after delete")
	}

	// Should not be open
	if journal.IsOpen() {
		t.Error("journal should not be open after delete")
	}
}

func TestJournalValidation(t *testing.T) {
	t.Parallel()
	t.Skip("Journal validation not yet fully implemented")
	journalFile := "test_validation.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	// Non-existent journal should not be valid
	valid, err := journal.IsValid()
	if err != nil {
		t.Fatalf("failed to check validity: %v", err)
	}
	if valid {
		t.Error("non-existent journal should not be valid")
	}

	// Create and write valid journal
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	pageData := make([]byte, DefaultPageSize)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	if err := journal.Sync(); err != nil {
		t.Fatalf("failed to sync journal: %v", err)
	}

	journal.Close()

	// Now it should be valid
	valid, err = journal.IsValid()
	if err != nil {
		t.Fatalf("failed to check validity: %v", err)
	}
	if !valid {
		t.Error("journal should be valid")
	}
}

func TestJournalTruncate(t *testing.T) {
	t.Parallel()
	journalFile := "test_truncate.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Write some data
	pageData := make([]byte, DefaultPageSize)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	journal.Close()

	// Truncate
	if err := journal.Truncate(); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	// File should exist but be empty
	info, err := os.Stat(journalFile)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %v", err)
		}
	} else if info.Size() != 0 {
		t.Errorf("journal should be empty after truncate, got size %d", info.Size())
	}
}

func TestJournalZeroHeader(t *testing.T) {
	t.Parallel()
	journalFile := "test_zero_header.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Write some data
	pageData := make([]byte, DefaultPageSize)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	journal.Close()

	// Zero header
	if err := journal.ZeroHeader(); err != nil {
		t.Fatalf("failed to zero header: %v", err)
	}

	// Journal should not be valid anymore
	valid, err := journal.IsValid()
	if err != nil {
		t.Fatalf("failed to check validity: %v", err)
	}
	if valid {
		t.Error("journal with zeroed header should not be valid")
	}
}

// journalWriteMarkerPages writes byte(i) to page i for range [1, count].
func journalWriteMarkerPages(t *testing.T, pager *Pager, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		mustGetWritePageData(t, pager, Pgno(i), byte(i))
	}
}

// journalVerifyMarkerPages verifies byte(i) on page i for range [1, count].
func journalVerifyMarkerPages(t *testing.T, pager *Pager, count int, expected func(int) byte) {
	t.Helper()
	for i := 1; i <= count; i++ {
		page := mustGetPage(t, pager, Pgno(i))
		if page.Data[0] != expected(i) {
			t.Errorf("page %d: expected %d, got %d", i, expected(i), page.Data[0])
		}
		pager.Put(page)
	}
}

func TestJournalMultiplePages(t *testing.T) {
	t.Parallel()
	t.Skip("Journal multiple pages not yet fully implemented")
	dbFile := "test_multi_pages.db"
	journalFile := dbFile + "-journal"
	defer os.Remove(dbFile)
	defer os.Remove(journalFile)

	pager := openTestPagerAt(t, dbFile, false)
	defer pager.Close()

	mustBeginWrite(t, pager)
	journalWriteMarkerPages(t, pager, 3)
	mustCommit(t, pager)

	// Verify and modify again
	mustBeginWrite(t, pager)
	journalVerifyMarkerPages(t, pager, 3, func(i int) byte { return byte(i) })
	for i := 1; i <= 3; i++ {
		mustGetWritePageData(t, pager, Pgno(i), 0xFF)
	}
	mustRollback(t, pager)

	journalVerifyMarkerPages(t, pager, 3, func(i int) byte { return byte(i) })
}

func TestJournalInvalidPageSize(t *testing.T) {
	t.Parallel()
	journalFile := "test_invalid_size.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Try to write with wrong size
	wrongSizeData := make([]byte, DefaultPageSize-1)
	if err := journal.WriteOriginal(1, wrongSizeData); err == nil {
		t.Error("expected error when writing wrong size data")
	}

	// Correct size should work
	correctSizeData := make([]byte, DefaultPageSize)
	if err := journal.WriteOriginal(1, correctSizeData); err != nil {
		t.Errorf("failed to write correct size data: %v", err)
	}
}

// TestJournalSync tests journal sync operation
func TestJournalSync(t *testing.T) {
	t.Parallel()
	journalFile := "test_sync.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	// Sync on closed journal should fail
	err := journal.Sync()
	if err == nil {
		t.Error("expected error syncing closed journal")
	}

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Write some data
	pageData := make([]byte, DefaultPageSize)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write original: %v", err)
	}

	// Sync should succeed
	if err := journal.Sync(); err != nil {
		t.Errorf("failed to sync journal: %v", err)
	}
}

// TestJournalRollbackReal tests actual journal rollback with pager
func TestJournalRollbackReal(t *testing.T) {
	t.Parallel()
	dbFile := "test_rollback_real.db"
	journalFile := dbFile + "-journal"
	defer os.Remove(dbFile)
	defer os.Remove(journalFile)

	pager := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, pager)
	page := mustGetPage(t, pager, 1)

	testData := make([]byte, len(page.Data))
	copy(testData, page.Data)
	testData[120] = 0xAA
	testData[200] = 0xBB
	copy(page.Data, testData)
	mustWritePage(t, pager, page)
	pager.Put(page)
	mustCommit(t, pager)

	pageSize := pager.PageSize()
	pageCount := pager.PageCount()
	pager.Close()

	// Create journal with test data
	journal := mustOpenJournal(t, journalFile, pageSize, pageCount)
	journal.WriteOriginal(1, testData)
	journal.Close()

	// Reopen and rollback
	pager = openTestPagerAt(t, dbFile, false)
	defer pager.Close()
	journal = mustOpenJournal(t, journalFile, pageSize, pageCount)
	if err := journal.Rollback(pager); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}
	journal.Close()
}

// TestJournalUpdatePageCount tests the updatePageCount method
func TestJournalUpdatePageCount(t *testing.T) {
	t.Parallel()
	journalFile := "test_update_count.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, DefaultPageSize, 1)

	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Write some pages
	pageData := make([]byte, DefaultPageSize)
	for i := 1; i <= 3; i++ {
		if err := journal.WriteOriginal(uint32(i), pageData); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
	}

	// Update page count
	if err := journal.updatePageCount(); err != nil {
		t.Errorf("failed to update page count: %v", err)
	}

	// Read header to verify
	header, err := journal.readHeader()
	if err != nil {
		t.Errorf("failed to read header: %v", err)
	}

	if header.PageCount != 3 {
		t.Errorf("expected page count 3, got %d", header.PageCount)
	}
}

// TestJournalCalculateChecksum tests checksum calculation
func TestJournalCalculateChecksum(t *testing.T) {
	t.Parallel()
	journal := NewJournal("test.db-journal", DefaultPageSize, 1)

	// Test with various data patterns
	tests := []struct {
		name     string
		pageNum  uint32
		dataSize int
		pattern  byte
	}{
		{"zeros", 1, DefaultPageSize, 0x00},
		{"ones", 2, DefaultPageSize, 0xFF},
		{"alternating", 3, DefaultPageSize, 0xAA},
		{"small", 4, 100, 0x55},
		{"not divisible by 4", 5, 1001, 0x12},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataSize)
			for i := range data {
				data[i] = tt.pattern
			}

			checksum1 := journal.calculateChecksum(tt.pageNum, data)
			checksum2 := journal.calculateChecksum(tt.pageNum, data)

			// Same data should produce same checksum
			if checksum1 != checksum2 {
				t.Errorf("checksums differ: %x vs %x", checksum1, checksum2)
			}

			// Different page numbers should produce different checksums
			checksum3 := journal.calculateChecksum(tt.pageNum+1, data)
			if checksum1 == checksum3 {
				t.Error("different page numbers produced same checksum")
			}

			// Different data should produce different checksums
			data[0] ^= 0xFF
			checksum4 := journal.calculateChecksum(tt.pageNum, data)
			if checksum1 == checksum4 {
				t.Error("different data produced same checksum")
			}
		})
	}
}
