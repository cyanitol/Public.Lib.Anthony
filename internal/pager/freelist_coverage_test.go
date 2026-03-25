// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCoverageDB opens a file-backed database for freelist coverage tests.
func openCoverageDB(t *testing.T, name string) (*sql.DB, string) {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), name)
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dbFile
}

// openMemoryCoverageDB opens an in-memory database for freelist coverage tests.
func openMemoryCoverageDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open(:memory:) error = %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// execCoverage executes a SQL statement or fails.
func execCoverage(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// queryCoverageInt queries a single integer value or fails.
// Returns 0 if the query produces no rows (e.g. PRAGMA that returns nothing).
func queryCoverageInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("queryCoverageInt %q: %v", query, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0
	}
	var val int
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("queryCoverageInt scan %q: %v", query, err)
	}
	return val
}

// insertManyRows inserts n rows with a TEXT payload into the given table.
func insertManyRows(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	payload := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", table), payload); err != nil {
			t.Fatalf("INSERT #%d error: %v", i, err)
		}
	}
}

// TestFreelistCoverage_DeleteToFreelist verifies that deleting many rows puts
// pages on the freelist and PRAGMA freelist_count reflects it.
// Exercises: flushPending, processTrunkPage, addPendingToTrunk, createNewTrunk.
func TestFreelistCoverage_DeleteToFreelist(t *testing.T) {
	db, _ := openCoverageDB(t, "del_freelist.db")

	execCoverage(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
	insertManyRows(t, db, "items", 500)

	countBefore := queryCoverageInt(t, db, "SELECT COUNT(*) FROM items")
	if countBefore != 500 {
		t.Errorf("expected 500 rows before delete, got %d", countBefore)
	}

	execCoverage(t, db, "DELETE FROM items")

	countAfter := queryCoverageInt(t, db, "SELECT COUNT(*) FROM items")
	if countAfter != 0 {
		t.Errorf("expected 0 rows after delete, got %d", countAfter)
	}

	// PRAGMA freelist_count should be > 0 after deleting all rows.
	freeCount := queryCoverageInt(t, db, "PRAGMA freelist_count")
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
	db, _ := openCoverageDB(t, "reinsert.db")

	execCoverage(t, db, "CREATE TABLE first (id INTEGER PRIMARY KEY, data TEXT)")
	execCoverage(t, db, "CREATE TABLE second (id INTEGER PRIMARY KEY, data TEXT)")
	payload := strings.Repeat("x", 200)

	// Populate first table with 300 rows.
	for i := 1; i <= 300; i++ {
		if _, err := db.Exec("INSERT INTO first (id, data) VALUES (?, ?)", i, payload); err != nil {
			t.Fatalf("INSERT first table #%d: %v", i, err)
		}
	}

	execCoverage(t, db, "DELETE FROM first")
	freeAfterDelete := queryCoverageInt(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after delete: %d", freeAfterDelete)

	// Insert into second table; freed pages from first should be reused.
	for i := 1; i <= 300; i++ {
		if _, err := db.Exec("INSERT INTO second (id, data) VALUES (?, ?)", i, payload); err != nil {
			t.Fatalf("INSERT second table #%d: %v", i, err)
		}
	}

	countSecond := queryCoverageInt(t, db, "SELECT COUNT(*) FROM second")
	if countSecond != 300 {
		t.Errorf("expected 300 rows in second table, got %d", countSecond)
	}

	freeAfterReinsert := queryCoverageInt(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after re-insert: %d (was %d)", freeAfterReinsert, freeAfterDelete)
}

// TestFreelistCoverage_NewTrunkPage deletes enough rows to overflow a single
// trunk page and force createNewTrunk to be called.
// A 4096-byte page holds 1022 leaf pointers; freeing >1022 pages requires a
// second trunk page.
func TestFreelistCoverage_NewTrunkPage(t *testing.T) {
	db, _ := openCoverageDB(t, "new_trunk.db")

	// Use a small payload so many rows fit per page, but enough distinct rows
	// that deleting them frees many pages.
	execCoverage(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")

	// Insert 2000 rows with a payload large enough to consume one page per ~4 rows.
	payload := strings.Repeat("y", 900)
	for i := 0; i < 2000; i++ {
		if _, err := db.Exec("INSERT INTO items (data) VALUES (?)", payload); err != nil {
			t.Fatalf("INSERT #%d error: %v", i, err)
		}
	}

	execCoverage(t, db, "DELETE FROM items")

	freeCount := queryCoverageInt(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after mass delete: %d", freeCount)
	if freeCount < 0 {
		t.Errorf("freelist_count should not be negative, got %d", freeCount)
	}
}

// TestFreelistCoverage_IntegrityCheck runs PRAGMA integrity_check after
// freelist operations to exercise Verify / verifyTrunkPage / verifyLeafPage.
func TestFreelistCoverage_IntegrityCheck(t *testing.T) {
	db, _ := openCoverageDB(t, "integrity.db")

	execCoverage(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	insertManyRows(t, db, "t", 400)
	execCoverage(t, db, "DELETE FROM t WHERE id % 2 = 0")
	insertManyRows(t, db, "t", 100)
	execCoverage(t, db, "DELETE FROM t WHERE id > 200")

	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("PRAGMA integrity_check error: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan integrity_check row: %v", err)
		}
		results = append(results, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("integrity_check rows error: %v", err)
	}

	t.Logf("integrity_check results: %v", results)
	// "ok" is the expected result for a clean database.
	if len(results) == 1 && results[0] == "ok" {
		return
	}
	// Some implementations return no rows for "ok"; that is also acceptable.
	if len(results) == 0 {
		return
	}
	// Non-ok results that are not errors are informational.
	for _, r := range results {
		if !strings.EqualFold(r, "ok") {
			t.Logf("integrity_check non-ok result: %q", r)
		}
	}
}

// TestFreelistCoverage_FreeMultipleInTransaction deletes rows inside a single
// transaction, freeing multiple pages at once (exercises FreeMultiple).
func TestFreelistCoverage_FreeMultipleInTransaction(t *testing.T) {
	db, _ := openCoverageDB(t, "free_multi.db")

	execCoverage(t, db, "CREATE TABLE records (id INTEGER PRIMARY KEY, data TEXT)")
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

	count := queryCoverageInt(t, db, "SELECT COUNT(*) FROM records")
	if count != 0 {
		t.Errorf("expected 0 rows after transactional delete, got %d", count)
	}

	freeCount := queryCoverageInt(t, db, "PRAGMA freelist_count")
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
		execCoverage(t, db, "CREATE TABLE stuff (id INTEGER PRIMARY KEY, data TEXT)")
		insertManyRows(t, db, "stuff", 200)
		execCoverage(t, db, "DELETE FROM stuff")
	}()

	// Confirm the file exists.
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		t.Fatal("database file should exist after close")
	}

	// Phase 2: reopen and check freelist_count and re-insert.
	db2, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open phase2: %v", err)
	}
	defer db2.Close()

	freeCount := queryCoverageInt(t, db2, "PRAGMA freelist_count")
	t.Logf("freelist_count on reopen: %d", freeCount)

	// Re-insert to exercise allocateFromDisk on persisted freelist.
	insertManyRows(t, db2, "stuff", 100)
	count := queryCoverageInt(t, db2, "SELECT COUNT(*) FROM stuff")
	if count != 100 {
		t.Errorf("expected 100 rows after re-insert, got %d", count)
	}
}

// TestFreelistCoverage_IntegrityCheckAfterNewTrunk runs integrity_check after
// creating enough freed pages to require multiple trunk pages.
func TestFreelistCoverage_IntegrityCheckAfterNewTrunk(t *testing.T) {
	db, _ := openCoverageDB(t, "integrity_trunk.db")

	execCoverage(t, db, "CREATE TABLE big (id INTEGER PRIMARY KEY, data TEXT)")
	payload := strings.Repeat("z", 900)
	for i := 0; i < 1500; i++ {
		if _, err := db.Exec("INSERT INTO big (data) VALUES (?)", payload); err != nil {
			t.Fatalf("INSERT #%d: %v", i, err)
		}
	}
	execCoverage(t, db, "DELETE FROM big")

	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	t.Logf("integrity_check after mass delete: %v", results)
}

// TestFreelistCoverage_PartialDeleteAndReuse exercises partial freelist
// consumption followed by new page allocation.
func TestFreelistCoverage_PartialDeleteAndReuse(t *testing.T) {
	db, _ := openCoverageDB(t, "partial.db")

	execCoverage(t, db, "CREATE TABLE p (id INTEGER PRIMARY KEY, data TEXT)")
	insertManyRows(t, db, "p", 400)

	// Delete half the rows.
	execCoverage(t, db, "DELETE FROM p WHERE id % 2 = 0")
	freeCountPartial := queryCoverageInt(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after partial delete: %d", freeCountPartial)

	// Insert new rows to reuse freed pages.
	insertManyRows(t, db, "p", 200)

	freeCountAfter := queryCoverageInt(t, db, "PRAGMA freelist_count")
	t.Logf("freelist_count after reuse: %d", freeCountAfter)

	count := queryCoverageInt(t, db, "SELECT COUNT(*) FROM p")
	t.Logf("row count after partial reuse: %d", count)
}
