// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// Comprehensive statement testing to improve coverage

func TestComplexSelectQuery(t *testing.T) {
	dbFile := "test_complex_select.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, category TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	products := []struct {
		id       int
		name     string
		price    int
		category string
	}{
		{1, "Product A", 100, "Electronics"},
		{2, "Product B", 200, "Electronics"},
		{3, "Product C", 150, "Clothing"},
		{4, "Product D", 300, "Electronics"},
		{5, "Product E", 50, "Clothing"},
	}

	for _, p := range products {
		_, err = db.Exec("INSERT INTO products VALUES (?, ?, ?, ?)", p.id, p.name, p.price, p.category)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test SELECT with WHERE, ORDER BY, and LIMIT
	rows, err := db.Query("SELECT name, price FROM products WHERE category = ? ORDER BY price DESC LIMIT 2", "Electronics")
	if err != nil {
		t.Errorf("Complex SELECT failed: %v", err)
		return
	}
	defer rows.Close()

	var results []struct {
		name  string
		price int
	}
	for rows.Next() {
		var r struct {
			name  string
			price int
		}
		if err := rows.Scan(&r.name, &r.price); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSelectWithExpressions(t *testing.T) {
	dbFile := "test_select_expr.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_where_multiple.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_update_multi.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_delete_all.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_comparison.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_insert_multi.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_distinct.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (category TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	categories := []string{"A", "B", "A", "C", "B", "A"}
	for _, cat := range categories {
		_, err = db.Exec("INSERT INTO test VALUES (?)", cat)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test DISTINCT
	rows, err := db.Query("SELECT DISTINCT category FROM test")
	if err != nil {
		t.Errorf("SELECT DISTINCT failed: %v", err)
		return
	}
	defer rows.Close()

	var categories_found []string
	for rows.Next() {
		var cat string
		if err := rows.Scan(&cat); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		categories_found = append(categories_found, cat)
	}

	if len(categories_found) != 3 {
		t.Errorf("got %d distinct categories, want 3", len(categories_found))
	}
}

func TestTransactionCommitWithData(t *testing.T) {
	dbFile := "test_tx_commit_data.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert data in transaction
	_, err = tx.Exec("INSERT INTO test VALUES (1, 100)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT failed: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test VALUES (2, 200)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT failed: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify data was committed
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Errorf("SELECT COUNT failed: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestOrderByDescending(t *testing.T) {
	dbFile := "test_order_desc.db"
	defer os.Remove(dbFile)

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

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// ORDER BY DESC
	rows, err := db.Query("SELECT value FROM test ORDER BY value DESC")
	if err != nil {
		t.Errorf("ORDER BY DESC failed: %v", err)
		return
	}
	defer rows.Close()

	var values []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		values = append(values, v)
	}

	if len(values) != 5 {
		t.Errorf("got %d values, want 5", len(values))
	}

	// Verify descending order
	for i := 0; i < len(values)-1; i++ {
		if values[i] < values[i+1] {
			t.Errorf("values not in descending order: %v", values)
			break
		}
	}
}
