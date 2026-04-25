// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openPagerTestDB opens a sqlite_internal database for pager_test packages.
func openPagerTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", path)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", path, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// openPagerTestMemoryDB opens an in-memory sqlite_internal database.
func openPagerTestMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	return openPagerTestDB(t, ":memory:")
}

// mustExecPagerTest executes a statement or fails the test.
func mustExecPagerTest(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// logExecPagerTest executes a statement and logs any error as non-fatal.
func logExecPagerTest(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Logf("exec %q: %v (non-fatal)", query, err)
	}
}

// mustQueryIntPagerTest queries a single integer value or fails.
func mustQueryIntPagerTest(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0
	}
	var value int
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("scan %q: %v", query, err)
	}
	return value
}

// mustQueryStringsPagerTest queries a single-column result set as strings.
func mustQueryStringsPagerTest(t *testing.T, db *sql.DB, query string, args ...interface{}) []string {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		results = append(results, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err %q: %v", query, err)
	}
	return results
}
