// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func ciSQLOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func ciSQLExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestConstraintsExecInternal_UpdateSelfUniqueMultiCol performs an UPDATE that
// does not change any values on a table with a UNIQUE(a,b) index.  The
// multi-column uniqueness scan must skip the row being updated (skipRowid
// path in checkMultiColRow); without that skip the UPDATE would produce a
// spurious UNIQUE constraint error against the row's own values.
func TestConstraintsExecInternal_UpdateSelfUniqueMultiCol(t *testing.T) {
	db := ciSQLOpenDB(t)
	defer db.Close()

	ciSQLExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	ciSQLExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a, b)")
	ciSQLExec(t, db, "INSERT INTO t VALUES(1, 'x')")
	ciSQLExec(t, db, "INSERT INTO t VALUES(2, 'y')")

	// No-op update — exercises checkMultiColRow skipRowid branch.
	ciSQLExec(t, db, "UPDATE t SET a=a, b=b WHERE a=1")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows after no-op UPDATE, got %d", count)
	}
}

// TestConstraintsExecInternal_UpdateSelfUniqueMultiColMultipleRows exercises
// the skipRowid path when there are multiple rows, ensuring the scanner visits
// all rows and skips only the one being updated.
func TestConstraintsExecInternal_UpdateSelfUniqueMultiColMultipleRows(t *testing.T) {
	db := ciSQLOpenDB(t)
	defer db.Close()

	ciSQLExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	ciSQLExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a, b)")
	ciSQLExec(t, db, "INSERT INTO t VALUES(10, 'alpha')")
	ciSQLExec(t, db, "INSERT INTO t VALUES(20, 'beta')")
	ciSQLExec(t, db, "INSERT INTO t VALUES(30, 'gamma')")

	// No-op update on the middle row.
	ciSQLExec(t, db, "UPDATE t SET a=a WHERE a=20")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}
