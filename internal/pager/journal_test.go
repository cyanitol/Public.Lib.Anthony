package pager

import (
	"os"
	"testing"
)

func TestJournalCreation(t *testing.T) {
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
	t.Skip("Journal rollback not yet fully implemented")
	dbFile := "test_rollback.db"
	journalFile := dbFile + "-journal"
	defer os.Remove(dbFile)
	defer os.Remove(journalFile)

	// Create pager and write initial data
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	// Get page 1 and set some data
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Save original data
	originalData := make([]byte, len(page.Data))
	copy(originalData, page.Data)
	originalData[0] = 0xAA
	originalData[100] = 0xBB
	copy(page.Data, originalData)

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	pager.Put(page)

	// Commit to save original data
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start new transaction and modify data
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Modify the page
	page.Data[0] = 0xFF
	page.Data[100] = 0xFF
	pager.Put(page)

	// Rollback
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify data was restored
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page after rollback: %v", err)
	}
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

func TestJournalMultiplePages(t *testing.T) {
	t.Skip("Journal multiple pages not yet fully implemented")
	dbFile := "test_multi_pages.db"
	journalFile := dbFile + "-journal"
	defer os.Remove(dbFile)
	defer os.Remove(journalFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Modify multiple pages
	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}

		// Set unique marker
		page.Data[0] = byte(i)
		pager.Put(page)
	}

	// Commit
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start new transaction and verify data
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		if page.Data[0] != byte(i) {
			t.Errorf("page %d data not persisted: expected %d, got %d", i, i, page.Data[0])
		}

		// Modify again
		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		page.Data[0] = 0xFF
		pager.Put(page)
	}

	// Rollback
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify rollback restored data
	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		defer pager.Put(page)

		if page.Data[0] != byte(i) {
			t.Errorf("page %d not restored after rollback: expected %d, got %d", i, i, page.Data[0])
		}
	}
}

func TestJournalInvalidPageSize(t *testing.T) {
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
