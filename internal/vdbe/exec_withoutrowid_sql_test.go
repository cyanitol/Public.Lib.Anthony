// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func withoutRowIDOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func withoutRowIDExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func withoutRowIDExecErr(t *testing.T, db *sql.DB, q string) error {
	t.Helper()
	_, err := db.Exec(q)
	return err
}

func withoutRowIDQueryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

func withoutRowIDQueryStr(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	var s string
	if err := db.QueryRow(q).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return s
}

// itoaWR is a minimal int-to-string helper used by the WITHOUT ROWID SQL tests.
func itoaWR(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

// ---------------------------------------------------------------------------
// SQL-level tests — exercise the WITHOUT ROWID code paths end-to-end
// ---------------------------------------------------------------------------

// TestExecWithoutRowIDBasicInsert exercises execInsertWithoutRowID and
// performInsertWithCompositeKey via a basic WITHOUT ROWID table.
func TestExecWithoutRowIDBasicInsert(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('hello', 1)")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('world', 2)")

	count := withoutRowIDQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestExecWithoutRowIDSelectBack verifies that inserted rows can be read back.
func TestExecWithoutRowIDSelectBack(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('alpha', 10)")

	got := withoutRowIDQueryStr(t, db, "SELECT a FROM t WHERE b=10")
	if got != "alpha" {
		t.Errorf("expected 'alpha', got %q", got)
	}
}

// TestExecWithoutRowIDPKConflictDefault verifies that a duplicate PRIMARY KEY
// raises an error under the default conflict mode.
func TestExecWithoutRowIDPKConflictDefault(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('x', 1)")

	err := withoutRowIDExecErr(t, db, "INSERT INTO t VALUES('x', 1)")
	if err == nil {
		t.Error("expected PRIMARY KEY constraint error on duplicate WITHOUT ROWID insert")
	}
}

// TestExecWithoutRowIDInsertOrIgnore exercises resolveCompositeConflict with
// conflictModeIgnore: a duplicate insert must be silently skipped.
func TestExecWithoutRowIDInsertOrIgnore(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('dup', 5)")
	withoutRowIDExec(t, db, "INSERT OR IGNORE INTO t VALUES('dup', 5)")

	count := withoutRowIDQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if count != 1 {
		t.Errorf("expected 1 row after OR IGNORE duplicate, got %d", count)
	}
}

// TestExecWithoutRowIDInsertOrReplace exercises resolveCompositeConflict with
// conflictModeReplace and deleteAndRetryComposite: old row must be replaced.
func TestExecWithoutRowIDInsertOrReplace(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('key1', 100)")
	withoutRowIDExec(t, db, "INSERT OR REPLACE INTO t VALUES('key1', 200)")

	count := withoutRowIDQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if count != 1 {
		t.Errorf("expected 1 row after REPLACE, got %d", count)
	}

	got := withoutRowIDQueryInt(t, db, "SELECT v FROM t WHERE k='key1'")
	if got != 200 {
		t.Errorf("expected v=200 after REPLACE, got %d", got)
	}
}

// TestExecWithoutRowIDMultipleRows exercises inserting and counting many rows.
func TestExecWithoutRowIDMultipleRows(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(a INT, b INT, PRIMARY KEY(a,b)) WITHOUT ROWID")
	for i := 0; i < 5; i++ {
		withoutRowIDExec(t, db, "INSERT INTO t VALUES("+itoaWR(i)+", "+itoaWR(i*10)+")")
	}

	count := withoutRowIDQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

// TestExecWithoutRowIDUpdate exercises the UPDATE path on a WITHOUT ROWID table
// (exercises execInsertWithoutRowID with isUpdate=true).
func TestExecWithoutRowIDUpdate(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db, "CREATE TABLE t(k TEXT PRIMARY KEY, v INT) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('a', 1)")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('b', 2)")
	withoutRowIDExec(t, db, "UPDATE t SET v=99 WHERE k='a'")

	got := withoutRowIDQueryInt(t, db, "SELECT v FROM t WHERE k='a'")
	if got != 99 {
		t.Errorf("expected v=99 after UPDATE, got %d", got)
	}
	other := withoutRowIDQueryInt(t, db, "SELECT v FROM t WHERE k='b'")
	if other != 2 {
		t.Errorf("expected v=2 for k='b', got %d", other)
	}
}

// TestExecWithoutRowIDThreeColumnPK exercises a three-column composite PK.
func TestExecWithoutRowIDThreeColumnPK(t *testing.T) {
	db := withoutRowIDOpenDB(t)
	defer db.Close()

	withoutRowIDExec(t, db,
		"CREATE TABLE t(a TEXT, b INT, c INT, d TEXT, PRIMARY KEY(a,b,c)) WITHOUT ROWID")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('x', 1, 2, 'data1')")
	withoutRowIDExec(t, db, "INSERT INTO t VALUES('x', 1, 3, 'data2')")

	count := withoutRowIDQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}
