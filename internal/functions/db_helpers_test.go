// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openTestDB opens the shared in-memory database used by function tests.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", "file::memory:?mode=memory")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// queryOneString runs a single-column query and returns the string result.
// It returns ("", true) when the value is NULL.
func queryOneString(t *testing.T, db *sql.DB, query string, args ...interface{}) (string, bool) {
	t.Helper()
	row := db.QueryRow(query, args...)
	var s sql.NullString
	if err := row.Scan(&s); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if !s.Valid {
		return "", true
	}
	return s.String, false
}

// queryFloat scans a single float64 column.
func queryFloat(t *testing.T, db *sql.DB, query string, args ...interface{}) float64 {
	t.Helper()
	row := db.QueryRow(query, args...)
	var f float64
	if err := row.Scan(&f); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return f
}

// assertDateQuery checks a SQL query returns a non-NULL string equal to want.
func assertDateQuery(t *testing.T, db *sql.DB, sqlStr, want string) {
	t.Helper()
	got, isNull := queryOneString(t, db, sqlStr)
	if isNull {
		t.Fatalf("%s returned NULL", sqlStr)
	}
	if got != want {
		t.Errorf("%s = %q, want %q", sqlStr, got, want)
	}
}
