// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// Comprehensive statement testing to improve coverage

// compStmtOpenAndExec opens a database and executes setup statements.
func compStmtOpenAndExec(t *testing.T, stmts ...string) *sql.DB {
	t.Helper()
	dbFile := t.TempDir() + "/test.db"
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	return db
}

func TestComplexSelectQuery(t *testing.T) {
	db := compStmtOpenAndExec(t,
		"CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, category TEXT)",
		"INSERT INTO products VALUES (1, 'Product A', 100, 'Electronics')",
		"INSERT INTO products VALUES (2, 'Product B', 200, 'Electronics')",
		"INSERT INTO products VALUES (3, 'Product C', 150, 'Clothing')",
		"INSERT INTO products VALUES (4, 'Product D', 300, 'Electronics')",
		"INSERT INTO products VALUES (5, 'Product E', 50, 'Clothing')",
	)
	defer db.Close()

	rows, err := db.Query("SELECT name, price FROM products WHERE category = ? ORDER BY price DESC LIMIT 2", "Electronics")
	if err != nil {
		t.Fatalf("Complex SELECT failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string
		var price int
		if err := rows.Scan(&name, &price); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("got %d results, want 2", count)
	}
}

func TestSelectWithExpressions(t *testing.T) {
	dbFile := t.TempDir() + "/test_select_expr.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test SELECT with literal expressions
	var result int
	err = db.QueryRow("SELECT 1 + 2 + 3").Scan(&result)
	if err != nil {
		t.Errorf("SELECT expression failed: %v", err)
	}
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}
}

func TestSelectWithMultipleWhereClauses(t *testing.T) {
	dbFile := t.TempDir() + "/test_where_multiple.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value INTEGER, status TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 10, 'active')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (2, 20, 'inactive')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (3, 30, 'active')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test SELECT with AND condition
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test WHERE value > ? AND status = ?", 15, "active").Scan(&count)
	if err != nil {
		t.Errorf("SELECT with AND failed: %v", err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestUpdateMultipleColumns(t *testing.T) {
	dbFile := t.TempDir() + "/test_update_multi.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1, 'old', 10)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Update multiple columns
	_, err = db.Exec("UPDATE test SET name = ?, value = ? WHERE id = ?", "new", 20, 1)
	if err != nil {
		t.Errorf("UPDATE multiple columns failed: %v", err)
	}

	// Verify
	var name string
	var value int
	err = db.QueryRow("SELECT name, value FROM test WHERE id = 1").Scan(&name, &value)
	if err != nil {
		t.Errorf("SELECT failed: %v", err)
	}
	if name != "new" || value != 20 {
		t.Errorf("got (%s, %d), want (new, 20)", name, value)
	}
}

func TestDeleteAll(t *testing.T) {
	dbFile := t.TempDir() + "/test_delete_all.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Delete all rows
	_, err = db.Exec("DELETE FROM test")
	if err != nil {
		t.Errorf("DELETE all failed: %v", err)
	}

	// Verify all rows deleted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestSelectWithComparison(t *testing.T) {
	dbFile := t.TempDir() + "/test_comparison.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	for i := 1; i <= 10; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test various comparison operators
	tests := []struct {
		query    string
		expected int
	}{
		{"SELECT COUNT(*) FROM test WHERE value = 50", 1},
		{"SELECT COUNT(*) FROM test WHERE value > 50", 5},
		{"SELECT COUNT(*) FROM test WHERE value < 50", 4},
		{"SELECT COUNT(*) FROM test WHERE value >= 50", 6},
		{"SELECT COUNT(*) FROM test WHERE value <= 50", 5},
		{"SELECT COUNT(*) FROM test WHERE value != 50", 9},
	}

	for _, tt := range tests {
		var count int
		err = db.QueryRow(tt.query).Scan(&count)
		if err != nil {
			t.Errorf("Query failed: %v", err)
			continue
		}
		if count != tt.expected {
			t.Errorf("query %q: count = %d, want %d", tt.query, count, tt.expected)
		}
	}
}

func TestInsertMultipleRows(t *testing.T) {
	dbFile := t.TempDir() + "/test_insert_multi.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Prepare statement for multiple inserts
	stmt, err := db.Prepare("INSERT INTO test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Insert multiple rows using prepared statement
	for i := 1; i <= 100; i++ {
		_, err = stmt.Exec(i, "test")
		if err != nil {
			t.Errorf("INSERT %d failed: %v", i, err)
		}
	}

	// Verify count
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 100 {
		t.Errorf("count = %d, want 100", count)
	}
}

func TestSelectDistinct(t *testing.T) {
	db := compStmtOpenAndExec(t,
		"CREATE TABLE test (category TEXT)",
		"INSERT INTO test VALUES ('A')",
		"INSERT INTO test VALUES ('B')",
		"INSERT INTO test VALUES ('A')",
		"INSERT INTO test VALUES ('C')",
		"INSERT INTO test VALUES ('B')",
		"INSERT INTO test VALUES ('A')",
	)
	defer db.Close()

	rows, err := db.Query("SELECT DISTINCT category FROM test")
	if err != nil {
		t.Fatalf("SELECT DISTINCT failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var cat string
		if err := rows.Scan(&cat); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("got %d distinct categories, want 3", count)
	}
}

func TestTransactionCommitWithData(t *testing.T) {
	db := compStmtOpenAndExec(t, "CREATE TABLE test (id INTEGER, value INTEGER)")
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	for _, q := range []string{
		"INSERT INTO test VALUES (1, 100)",
		"INSERT INTO test VALUES (2, 200)",
	} {
		if _, err := tx.Exec(q); err != nil {
			tx.Rollback()
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count); err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// compStmtSetupTable5 creates a table with 5 rows and returns the DB.
func compStmtSetupTable5(t *testing.T, dbFile string) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	if _, err = db.Exec("CREATE TABLE test (id INTEGER, value INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, i*10); err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}
	return db
}

// compStmtQueryInts queries a single int column.
func compStmtQueryInts(t *testing.T, db *sql.DB, query string) []int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	return vals
}

func TestOrderByDescending(t *testing.T) {
	dbFile := t.TempDir() + "/test_order_desc.db"
	db := compStmtSetupTable5(t, dbFile)
	defer db.Close()

	values := compStmtQueryInts(t, db, "SELECT value FROM test ORDER BY value DESC")
	if len(values) != 5 {
		t.Errorf("got %d values, want 5", len(values))
	}
	for i := 0; i < len(values)-1; i++ {
		if values[i] < values[i+1] {
			t.Errorf("values not in descending order: %v", values)
			break
		}
	}
}
