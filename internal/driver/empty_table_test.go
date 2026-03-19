// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestEmptyTableQuery tests that querying an empty table returns
// a valid result set with zero rows and no error.
func TestEmptyTableQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Open database - driver will create a new database file
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create an empty table
	_, err = db.Exec("CREATE TABLE empty_test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Query the empty table - should not return an error
	rows, err := db.Query("SELECT * FROM empty_test")
	if err != nil {
		t.Fatalf("Query() returned error for empty table: %v", err)
	}
	defer rows.Close()

	// Check that we can get columns (table schema exists)
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() returned error: %v", err)
	}

	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}

	// Iterate over rows - should have zero iterations, no error
	rowCount := 0
	for rows.Next() {
		rowCount++
	}

	if rowCount != 0 {
		t.Errorf("expected 0 rows from empty table, got %d", rowCount)
	}

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() returned error after iteration: %v", err)
	}

	t.Log("Successfully queried empty table with no errors")
}

// verifyColumnNames checks that the column names from rows match expected.
func verifyColumnNames(t *testing.T, rows *sql.Rows, expected []string) {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns() failed: %v", err)
	}
	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(cols))
	}
	for i, exp := range expected {
		if cols[i] != exp {
			t.Errorf("column %d: expected %q, got %q", i, exp, cols[i])
		}
	}
}

// TestEmptyTableWithColumns tests that an empty table returns correct column info
func TestEmptyTableWithColumns(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE test_cols (
		id INTEGER PRIMARY KEY,
		name TEXT,
		age INTEGER,
		score REAL
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	rows, err := db.Query("SELECT name, age FROM test_cols")
	if err != nil {
		t.Fatalf("Query() failed: %v", err)
	}
	defer rows.Close()

	verifyColumnNames(t, rows, []string{"name", "age"})

	if rows.Next() {
		t.Error("Next() should return false for empty table")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("rows.Err() should be nil, got: %v", err)
	}
}
