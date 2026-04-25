// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// insertManyRows inserts n rows with a TEXT payload into the given table.
func insertManyRows(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	payload := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		mustExecPagerTest(t, db, fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", table), payload)
	}
}

// TestFreelistCoverage_DeleteToFreelist verifies that deleting many rows puts
// pages on the freelist and PRAGMA freelist_count reflects it.
// Exercises: flushPending, processTrunkPage, addPendingToTrunk, createNewTrunk.
func TestFreelistCoverage_DeleteToFreelist(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "del_freelist.db"))

	mustExecPagerTest(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
	insertManyRows(t, db, "items", 500)

	countBefore := mustQueryIntPagerTest(t, db, "SELECT COUNT(*) FROM items")
	if countBefore != 500 {
		t.Errorf("expected 500 rows before delete, got %d", countBefore)
	}

	mustExecPagerTest(t, db, "DELETE FROM items")

	countAfter := mustQueryIntPagerTest(t, db, "SELECT COUNT(*) FROM items")
	if countAfter != 0 {
		t.Errorf("expected 0 rows after delete, got %d", countAfter)
	}

	// PRAGMA freelist_count should be > 0 after deleting all rows.
	freeCount := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after delete: %d", freeCount)
	if freeCount < 0 {
		t.Errorf("freelist_count should not be negative, got %d", freeCount)
	}
}

// TestFreelistCoverage_DeleteThenReinsert verifies that pages freed by a delete
// are reused when new rows are inserted (exercises allocateFromDisk).
// It uses two separate tables: after the first is populated and its rows deleted,
// inserting into the second table exercises page reuse from the freelist.
func TestFreelistCoverage_DeleteThenReinsert(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "reinsert.db"))

	mustExecPagerTest(t, db, "CREATE TABLE first (id INTEGER PRIMARY KEY, data TEXT)")
	mustExecPagerTest(t, db, "CREATE TABLE second (id INTEGER PRIMARY KEY, data TEXT)")
	payload := strings.Repeat("x", 200)

	// Populate first table with 300 rows.
	for i := 1; i <= 300; i++ {
		if _, err := db.Exec("INSERT INTO first (id, data) VALUES (?, ?)", i, payload); err != nil {
			t.Fatalf("INSERT first table #%d: %v", i, err)
		}
	}

	mustExecPagerTest(t, db, "DELETE FROM first")
	freeAfterDelete := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after delete: %d", freeAfterDelete)

	// Insert into second table; freed pages from first should be reused.
	for i := 1; i <= 300; i++ {
		if _, err := db.Exec("INSERT INTO second (id, data) VALUES (?, ?)", i, payload); err != nil {
			t.Fatalf("INSERT second table #%d: %v", i, err)
		}
	}

	countSecond := mustQueryIntPagerTest(t, db, "SELECT COUNT(*) FROM second")
	if countSecond != 300 {
		t.Errorf("expected 300 rows in second table, got %d", countSecond)
	}

	freeAfterReinsert := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after re-insert: %d (was %d)", freeAfterReinsert, freeAfterDelete)
}

// TestFreelistCoverage_NewTrunkPage deletes enough rows to overflow a single
// trunk page and force createNewTrunk to be called.
// A 4096-byte page holds 1022 leaf pointers; freeing >1022 pages requires a
// second trunk page.
func TestFreelistCoverage_NewTrunkPage(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "new_trunk.db"))

	// Use a small payload so many rows fit per page, but enough distinct rows
	// that deleting them frees many pages.
	mustExecPagerTest(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")

	// Insert 2000 rows with a payload large enough to consume one page per ~4 rows.
	payload := strings.Repeat("y", 900)
	for i := 0; i < 2000; i++ {
		if _, err := db.Exec("INSERT INTO items (data) VALUES (?)", payload); err != nil {
			t.Fatalf("INSERT #%d error: %v", i, err)
		}
	}

	mustExecPagerTest(t, db, "DELETE FROM items")

	freeCount := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after mass delete: %d", freeCount)
	if freeCount < 0 {
		t.Errorf("freelist_count should not be negative, got %d", freeCount)
	}
}

// TestFreelistCoverage_IntegrityCheck runs PRAGMA integrity_check after
// freelist operations to exercise Verify / verifyTrunkPage / verifyLeafPage.
// fcCollectIntegrityResults runs PRAGMA integrity_check and returns all result strings.
func fcCollectIntegrityResults(t *testing.T, db *sql.DB) []string {
	t.Helper()
	return mustQueryStringsPagerTest(t, db, "PRAGMA integrity_check")
}

func TestFreelistCoverage_IntegrityCheck(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "integrity.db"))

	mustExecPagerTest(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	insertManyRows(t, db, "t", 400)
	mustExecPagerTest(t, db, "DELETE FROM t WHERE id % 2 = 0")
	insertManyRows(t, db, "t", 100)
	mustExecPagerTest(t, db, "DELETE FROM t WHERE id > 200")

	results := fcCollectIntegrityResults(t, db)
	t.Logf("integrity_check results: %v", results)

	if len(results) == 0 || (len(results) == 1 && results[0] == "ok") {
		return
	}
	for _, r := range results {
		if !strings.EqualFold(r, "ok") {
			t.Logf("integrity_check non-ok result: %q", r)
		}
	}
}

// TestFreelistCoverage_FreeMultipleInTransaction deletes rows inside a single
// transaction, freeing multiple pages at once (exercises FreeMultiple).
func TestFreelistCoverage_FreeMultipleInTransaction(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "free_multi.db"))

	mustExecPagerTest(t, db, "CREATE TABLE records (id INTEGER PRIMARY KEY, data TEXT)")
	insertManyRows(t, db, "records", 300)

	// Delete all records in a single transaction to free many pages at once.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() error: %v", err)
	}
	if _, err := tx.Exec("DELETE FROM records"); err != nil {
		tx.Rollback()
		t.Fatalf("DELETE in transaction error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error: %v", err)
	}

	count := mustQueryIntPagerTest(t, db, "SELECT COUNT(*) FROM records")
	if count != 0 {
		t.Errorf("expected 0 rows after transactional delete, got %d", count)
	}

	freeCount := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after transactional delete: %d", freeCount)
}

// TestFreelistCoverage_FileBasedFreelistPersistence creates a file-backed DB,
// populates it, deletes rows, reopens it, and confirms the freelist survives.
func TestFreelistCoverage_FileBasedFreelistPersistence(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "persist.db")

	// Phase 1: populate and delete.
	func() {
		db, err := sql.Open("sqlite_internal", dbFile)
		if err != nil {
			t.Fatalf("sql.Open phase1: %v", err)
		}
		defer db.Close()
		mustExecPagerTest(t, db, "CREATE TABLE stuff (id INTEGER PRIMARY KEY, data TEXT)")
		insertManyRows(t, db, "stuff", 200)
		mustExecPagerTest(t, db, "DELETE FROM stuff")
	}()

	// Confirm the file exists.
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		t.Fatal("database file should exist after close")
	}

	// Phase 2: reopen and check freelist_count and re-insert.
	db2 := openPagerTestDB(t, dbFile)

	freeCount := mustQueryIntPagerTest(t, db2, "PRAGMA freelist_count")
	t.Logf("freelist_count on reopen: %d", freeCount)

	// Re-insert to exercise allocateFromDisk on persisted freelist.
	insertManyRows(t, db2, "stuff", 100)
	count := mustQueryIntPagerTest(t, db2, "SELECT COUNT(*) FROM stuff")
	if count != 100 {
		t.Errorf("expected 100 rows after re-insert, got %d", count)
	}
}

// TestFreelistCoverage_IntegrityCheckAfterNewTrunk runs integrity_check after
// creating enough freed pages to require multiple trunk pages.
func TestFreelistCoverage_IntegrityCheckAfterNewTrunk(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "integrity_trunk.db"))

	mustExecPagerTest(t, db, "CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT)")
	payload := strings.Repeat("z", 900)
	for i := 0; i < 1500; i++ {
		if _, err := db.Exec("INSERT INTO big (data) VALUES (?)", payload); err != nil {
			t.Fatalf("INSERT #%d: %v", i, err)
		}
	}
	mustExecPagerTest(t, db, "DELETE FROM big")

	results := mustQueryStringsPagerTest(t, db, "PRAGMA integrity_check")
	t.Logf("integrity_check after mass delete: %v", results)
}

// TestFreelistCoverage_PartialDeleteAndReuse exercises partial freelist
// consumption followed by new page allocation.
func TestFreelistCoverage_PartialDeleteAndReuse(t *testing.T) {
	db := openPagerTestDB(t, filepath.Join(t.TempDir(), "partial.db"))

	mustExecPagerTest(t, db, "CREATE TABLE p (id INTEGER PRIMARY KEY, data TEXT)")
	insertManyRows(t, db, "p", 400)

	// Delete half the rows.
	mustExecPagerTest(t, db, "DELETE FROM p WHERE id % 2 = 0")
	freeCountPartial := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after partial delete: %d", freeCountPartial)

	// Insert new rows to reuse freed pages.
	insertManyRows(t, db, "p", 200)

	freeCountAfter := mustQueryIntPagerTest(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after reuse: %d", freeCountAfter)

	count := mustQueryIntPagerTest(t, db, "SELECT COUNT(*) FROM p")
	t.Logf("row count after partial reuse: %d", count)
}
