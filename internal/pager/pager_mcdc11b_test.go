// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

// SQL-level MC/DC tests for the pager package, batch 11b.
// These tests use the full SQL driver stack to exercise pager operations
// through realistic query paths:
//   - VACUUM: copies database to temp file and back.
//   - WAL: PRAGMA journal_mode=WAL enables WAL then reads/writes exercise WAL paths.
//   - Savepoints: SAVEPOINT / RELEASE / ROLLBACK TO.
//   - Transactions: BEGIN / SELECT / COMMIT.
//   - WAL checkpoint: PRAGMA wal_checkpoint.

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// mcdc11bOpen opens a file-based SQLite database and returns a cleanup function.
func mcdc11bOpen(t *testing.T, name string) (*sql.DB, string) {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), name)
	db, err := sql.Open("sqlite_internal", dbFile)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", dbFile, err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dbFile
}

// mcdc11bExec executes a SQL statement and fails on error.
func mcdc11bExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// mcdc11bQueryInt queries a single integer and fails on error.
func mcdc11bQueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	var v int
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("queryInt %q: %v", query, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// VACUUM — exercises vacuum.go paths through the SQL layer
// ---------------------------------------------------------------------------

func TestMCDC11B_SQL_Vacuum_Basic(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "vacuum_basic.db")

	mcdc11bExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 1; i <= 20; i++ {
		mcdc11bExec(t, db, "INSERT INTO t (v) VALUES (?)", "row")
	}
	mcdc11bExec(t, db, "DELETE FROM t WHERE id > 10")
	mcdc11bExec(t, db, "VACUUM")

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if n != 10 {
		t.Errorf("count after VACUUM = %d, want 10", n)
	}
}

func TestMCDC11B_SQL_Vacuum_IntoFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "src.db")
	dstFile := filepath.Join(dir, "dst.db")

	db, err := sql.Open("sqlite_internal", srcFile)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	mcdc11bExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 1; i <= 10; i++ {
		mcdc11bExec(t, db, "INSERT INTO items (data) VALUES (?)", "data")
	}

	if _, err := db.Exec("VACUUM INTO ?", dstFile); err != nil {
		t.Fatalf("VACUUM INTO: %v", err)
	}

	if _, err := os.Stat(dstFile); os.IsNotExist(err) {
		t.Fatal("VACUUM INTO did not create target file")
	}

	dstDB, err := sql.Open("sqlite_internal", dstFile)
	if err != nil {
		t.Fatalf("sql.Open(dst): %v", err)
	}
	defer dstDB.Close()

	n := mcdc11bQueryInt(t, dstDB, "SELECT COUNT(*) FROM items")
	if n != 10 {
		t.Errorf("dst count = %d, want 10", n)
	}
}

// ---------------------------------------------------------------------------
// WAL — exercises transaction.go enableWALMode / disableWALMode
// ---------------------------------------------------------------------------

func TestMCDC11B_SQL_WAL_EnableAndWrite(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "wal_enable.db")

	// Enable WAL mode.
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode=WAL: %v", err)
	}
	t.Logf("journal_mode after WAL pragma: %s", mode)

	mcdc11bExec(t, db, "CREATE TABLE wal_t (id INTEGER PRIMARY KEY, val TEXT)")
	mcdc11bExec(t, db, "INSERT INTO wal_t (val) VALUES (?)", "walrow")

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM wal_t")
	if n != 1 {
		t.Errorf("count in WAL mode = %d, want 1", n)
	}
}

func TestMCDC11B_SQL_WAL_MultipleWrites(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "wal_multi.db")

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode=WAL: %v", err)
	}

	mcdc11bExec(t, db, "CREATE TABLE wt (id INTEGER PRIMARY KEY, n INTEGER)")
	for i := 1; i <= 5; i++ {
		mcdc11bExec(t, db, "INSERT INTO wt (n) VALUES (?)", i)
	}

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM wt")
	if n != 5 {
		t.Errorf("count = %d, want 5", n)
	}
}

// ---------------------------------------------------------------------------
// WAL checkpoint — exercises wal_checkpoint.go via PRAGMA wal_checkpoint
// ---------------------------------------------------------------------------

func TestMCDC11B_SQL_WAL_Checkpoint(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "wal_ckpt.db")

	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode=WAL: %v", err)
	}

	mcdc11bExec(t, db, "CREATE TABLE ck (id INTEGER PRIMARY KEY, v TEXT)")
	for i := 1; i <= 5; i++ {
		mcdc11bExec(t, db, "INSERT INTO ck (v) VALUES (?)", "ckrow")
	}

	// PRAGMA wal_checkpoint triggers the checkpoint path.
	rows, err := db.Query("PRAGMA wal_checkpoint")
	if err != nil {
		t.Fatalf("PRAGMA wal_checkpoint: %v", err)
	}
	defer rows.Close()
	// Drain results.
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Logf("wal_checkpoint rows error (may be OK): %v", err)
	}

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM ck")
	if n != 5 {
		t.Errorf("count after checkpoint = %d, want 5", n)
	}
}

// ---------------------------------------------------------------------------
// Savepoints — exercises savepoint.go restoreToSavepoint via SQL
// ---------------------------------------------------------------------------

func TestMCDC11B_SQL_Savepoint_ReleaseCommit(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "sp_release.db")

	mcdc11bExec(t, db, "CREATE TABLE sp_t (id INTEGER PRIMARY KEY, v INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec("INSERT INTO sp_t (v) VALUES (1)"); err != nil {
		tx.Rollback()
		t.Fatalf("INSERT: %v", err)
	}

	if _, err := tx.Exec("SAVEPOINT sp1"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp1: %v", err)
	}

	if _, err := tx.Exec("INSERT INTO sp_t (v) VALUES (2)"); err != nil {
		tx.Rollback()
		t.Fatalf("INSERT after savepoint: %v", err)
	}

	if _, err := tx.Exec("RELEASE sp1"); err != nil {
		tx.Rollback()
		t.Fatalf("RELEASE sp1: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM sp_t")
	if n != 2 {
		t.Errorf("count after RELEASE savepoint = %d, want 2", n)
	}
}

func TestMCDC11B_SQL_Savepoint_RollbackTo(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "sp_rollback.db")

	mcdc11bExec(t, db, "CREATE TABLE sp_rb (id INTEGER PRIMARY KEY, v INTEGER)")
	mcdc11bExec(t, db, "INSERT INTO sp_rb (v) VALUES (10)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec("SAVEPOINT sp_rb1"); err != nil {
		tx.Rollback()
		t.Fatalf("SAVEPOINT sp_rb1: %v", err)
	}

	// Insert inside savepoint.
	if _, err := tx.Exec("INSERT INTO sp_rb (v) VALUES (20)"); err != nil {
		tx.Rollback()
		t.Fatalf("INSERT inside savepoint: %v", err)
	}

	// Roll back to savepoint — exercises restoreToSavepoint path.
	if _, err := tx.Exec("ROLLBACK TO sp_rb1"); err != nil {
		tx.Rollback()
		t.Fatalf("ROLLBACK TO sp_rb1: %v", err)
	}

	if _, err := tx.Exec("RELEASE sp_rb1"); err != nil {
		tx.Rollback()
		t.Fatalf("RELEASE sp_rb1: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Only the original row (v=10) should exist; v=20 was rolled back.
	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM sp_rb")
	if n != 1 {
		t.Errorf("count after ROLLBACK TO savepoint = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Transactions — exercises BeginRead / transaction state paths
// ---------------------------------------------------------------------------

func TestMCDC11B_SQL_Transaction_BeginSelectCommit(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "tx_read.db")

	mcdc11bExec(t, db, "CREATE TABLE tx_t (id INTEGER PRIMARY KEY, v TEXT)")
	mcdc11bExec(t, db, "INSERT INTO tx_t (v) VALUES (?)", "hello")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	var v string
	if err := tx.QueryRow("SELECT v FROM tx_t WHERE id = 1").Scan(&v); err != nil {
		tx.Rollback()
		t.Fatalf("SELECT in transaction: %v", err)
	}
	if v != "hello" {
		t.Errorf("SELECT returned %q, want %q", v, "hello")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestMCDC11B_SQL_Transaction_MultipleReadsAndWrites(t *testing.T) {
	t.Parallel()
	db, _ := mcdc11bOpen(t, "tx_rw.db")

	mcdc11bExec(t, db, "CREATE TABLE rw (id INTEGER PRIMARY KEY, n INTEGER)")

	// Interleave reads and writes to exercise transaction state transitions.
	for i := 1; i <= 5; i++ {
		mcdc11bExec(t, db, "INSERT INTO rw (n) VALUES (?)", i)
	}

	n := mcdc11bQueryInt(t, db, "SELECT COUNT(*) FROM rw")
	if n != 5 {
		t.Errorf("count = %d, want 5", n)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	rows, err := tx.Query("SELECT n FROM rw ORDER BY n")
	if err != nil {
		tx.Rollback()
		t.Fatalf("SELECT: %v", err)
	}

	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			tx.Rollback()
			t.Fatalf("Scan: %v", err)
		}
		vals = append(vals, v)
	}
	rows.Close()

	if len(vals) != 5 {
		t.Errorf("got %d rows, want 5", len(vals))
	}

	if _, err := tx.Exec("UPDATE rw SET n = n + 100"); err != nil {
		tx.Rollback()
		t.Fatalf("UPDATE: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	sum := mcdc11bQueryInt(t, db, "SELECT SUM(n) FROM rw")
	// Sum should be (101+102+103+104+105) = 515
	if sum != 515 {
		t.Errorf("sum after UPDATE = %d, want 515", sum)
	}
}
