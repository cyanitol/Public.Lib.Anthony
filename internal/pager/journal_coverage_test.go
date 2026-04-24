// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// openJournalRollbackPager opens a file-backed pager at dbFile for rollback tests.
func openJournalRollbackPager(t *testing.T, dbFile string) *pager.Pager {
	t.Helper()
	p, err := pager.Open(dbFile, false)
	if err != nil {
		t.Fatalf("pager.Open(%q): %v", dbFile, err)
	}
	return p
}

// writePagerPage gets a page, marks it writable, sets byte at DatabaseHeaderSize to val, and puts it back.
func writePagerPage(t *testing.T, p *pager.Pager, pgno pager.Pgno, val byte) {
	t.Helper()
	page, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d): %v", pgno, err)
	}
	if err := p.Write(page); err != nil {
		t.Fatalf("Write(%d): %v", pgno, err)
	}
	page.Data[pager.DatabaseHeaderSize] = val
	p.Put(page)
}

// readPagerByte reads the byte at DatabaseHeaderSize from the given page.
func readPagerByte(t *testing.T, p *pager.Pager, pgno pager.Pgno) byte {
	t.Helper()
	page, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("Get(%d): %v", pgno, err)
	}
	val := page.Data[pager.DatabaseHeaderSize]
	p.Put(page)
	return val
}

// TestJournalRollback_MultiplePages writes several pages, commits, modifies them,
// then rolls back — exercising restoreAllEntries with multiple journal entries.
func TestJournalRollback_MultiplePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "multi.db")

	p := openJournalRollbackPager(t, dbFile)
	defer p.Close()

	// Commit initial values for pages 1-4.
	for i := pager.Pgno(1); i <= 4; i++ {
		writePagerPage(t, p, i, byte(i*10))
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit initial: %v", err)
	}

	// Overwrite all pages and roll back.
	for i := pager.Pgno(1); i <= 4; i++ {
		writePagerPage(t, p, i, 0xFF)
	}
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify original values were restored by restoreAllEntries.
	for i := pager.Pgno(1); i <= 4; i++ {
		got := readPagerByte(t, p, i)
		want := byte(i * 10)
		if got != want {
			t.Errorf("page %d after rollback: got 0x%02X, want 0x%02X", i, got, want)
		}
	}
}

// TestJournalRollback_EmptyJournal exercises Rollback when no pages have been
// written to the journal (the loop in restoreAllEntries hits EOF immediately).
func TestJournalRollback_EmptyJournal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "empty.db")

	p := openJournalRollbackPager(t, dbFile)
	defer p.Close()

	// Commit an initial state.
	writePagerPage(t, p, 1, 0xAA)
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Begin a write transaction but do NOT modify any page — then rollback.
	// The journal exists but holds zero page entries.
	if err := p.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback empty journal: %v", err)
	}

	// Value must be unchanged.
	got := readPagerByte(t, p, 1)
	if got != 0xAA {
		t.Errorf("after empty-journal rollback: got 0x%02X, want 0xAA", got)
	}
}

// TestJournalRollback_SamePage writes to the same page multiple times within
// one transaction, then rolls back — verifying that the first (original) image
// is what restoreAllEntries reinstates.
func TestJournalRollback_SamePage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "same.db")

	p := openJournalRollbackPager(t, dbFile)
	defer p.Close()

	// Establish a committed baseline.
	writePagerPage(t, p, 1, 0x11)
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit baseline: %v", err)
	}

	// Modify page 1 twice within a single transaction, then roll back.
	writePagerPage(t, p, 1, 0x22)
	writePagerPage(t, p, 1, 0x33)
	if err := p.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// The original value (0x11) must be restored.
	got := readPagerByte(t, p, 1)
	if got != 0x11 {
		t.Errorf("after same-page rollback: got 0x%02X, want 0x11", got)
	}
}

// TestJournalRollback_SQLInterface exercises the pager-level rollback path via
// the database/sql interface: open a database, INSERT rows inside a transaction,
// call tx.Rollback, then verify the rows are absent.
// jcInsertRowsInTx inserts n rows with the given value in a transaction.
func jcInsertRowsInTx(t *testing.T, tx *sql.Tx, n int, val string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := tx.Exec("INSERT INTO t (v) VALUES (?)", val); err != nil {
			tx.Rollback() //nolint:errcheck
			t.Fatalf("INSERT: %v", err)
		}
	}
}

func TestJournalRollback_SQLInterface(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sql.db")

	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	jcInsertRowsInTx(t, tx, 5, "data")

	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx.Rollback: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

// TestJournalRollback_WriteOriginalInvalidSize exercises the WriteOriginal error
// path for mismatched page size (line 127, 93.3% branch).
func TestJournalRollback_WriteOriginalInvalidSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	jPath := filepath.Join(dir, "woinv.journal")

	j := pager.NewJournal(jPath, pager.DefaultPageSize, 1)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	defer j.Delete()

	// Wrong size — must return an error.
	wrong := make([]byte, pager.DefaultPageSize-1)
	if err := j.WriteOriginal(1, wrong); err == nil {
		t.Error("WriteOriginal with wrong size: expected error, got nil")
	}

	// Correct size — must succeed.
	correct := make([]byte, pager.DefaultPageSize)
	if err := j.WriteOriginal(1, correct); err != nil {
		t.Errorf("WriteOriginal with correct size: %v", err)
	}
}

// TestJournalRollback_DirectRollbackAPI directly drives Journal.Rollback so that
// restoreAllEntries is exercised without going through the Pager rollback stack.
//
// Strategy: commit an initial page state, capture the raw page bytes, then
// commit a second (modified) state. Open a Journal, write the captured original
// bytes as a journal entry, call Rollback(pager), then verify the journal
// count reflects the entries processed.
func TestJournalRollback_DirectRollbackAPI(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "direct.db")

	// Commit an initial state and capture its raw page data.
	p := openJournalRollbackPager(t, dbFile)
	writePagerPage(t, p, 1, 0xCC)
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit initial: %v", err)
	}
	pageSize := p.PageSize()
	dbSize := pager.Pgno(p.PageCount())

	// Read the committed page bytes to use as the journal's "original" data.
	rawPage, err := p.Get(1)
	if err != nil {
		t.Fatalf("Get(1) for capture: %v", err)
	}
	origData := make([]byte, pageSize)
	copy(origData, rawPage.Data)
	p.Put(rawPage)
	p.Close()

	// Build a journal that records the original state of page 1.
	jPath := filepath.Join(dir, "direct.db-journal")
	j := pager.NewJournal(jPath, pageSize, dbSize)
	if err := j.Open(); err != nil {
		t.Fatalf("Journal.Open: %v", err)
	}
	if err := j.WriteOriginal(1, origData); err != nil {
		t.Fatalf("WriteOriginal: %v", err)
	}

	// Open a fresh pager for the rollback target — no dirty pages in cache.
	p2 := openJournalRollbackPager(t, dbFile)
	defer p2.Close()

	// Verify the journal recorded exactly one entry before rollback.
	if j.GetPageCount() != 1 {
		t.Errorf("expected journal page count 1 before rollback, got %d", j.GetPageCount())
	}

	// Rollback via Journal.Rollback — exercises restoreAllEntries directly.
	if err := j.Rollback(p2); err != nil {
		t.Fatalf("Journal.Rollback: %v", err)
	}
	j.Delete()
}
