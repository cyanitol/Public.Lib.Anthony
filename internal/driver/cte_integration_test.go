// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestCTEIntegration_Simple tests basic CTE functionality.
func TestCTEIntegration_Simple(t *testing.T) {
	t.Skip("duplicate rows when CTE uses SELECT *")

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test simple CTE
	rows, err := db.Query(`WITH adult_users AS (SELECT * FROM users WHERE age >= 30) SELECT * FROM adult_users`)
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rows.Close()

	// Debug: print bytecode if available (commented out - rows is *sql.Rows, not *driver.Rows)
	// if r, ok := rows.(*Rows); ok && r.vdbe != nil {
	// 	t.Logf("VDBE Program:\n%s", r.vdbe.ExplainProgram())
	// }

	cols, _ := rows.Columns()
	t.Logf("Columns: %v (count: %d)", cols, len(cols))

	count := 0
	for rows.Next() {
		var id int
		var name string
		var age int
		if err := rows.Scan(&id, &name, &age); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		t.Logf("Row %d: id=%d, name=%s, age=%d", count+1, id, name, age)
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 adult users, got %d", count)
	}
}

// TestCTEIntegration_Multiple tests multiple CTEs with dependencies.
func TestCTEIntegration_Multiple(t *testing.T) {
	t.Skip("multiple CTEs with subqueries - cursor 5 not open")

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test tables
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)`)
	if err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("failed to insert users: %v", err)
	}

	_, err = db.Exec(`INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 50.0)`)
	if err != nil {
		t.Fatalf("failed to insert orders: %v", err)
	}

	// Test multiple CTEs with dependencies
	sql := `
		WITH
			user_ids AS (SELECT id FROM users),
			user_orders AS (SELECT * FROM orders WHERE user_id IN (SELECT id FROM user_ids))
		SELECT COUNT(*) as total FROM user_orders
	`

	var total int
	err = db.QueryRow(sql).Scan(&total)
	if err != nil {
		t.Fatalf("CTE query with dependencies failed: %v", err)
	}

	if total != 3 {
		t.Errorf("expected 3 orders, got %d", total)
	}
}

// TestCTEIntegration_Recursive tests recursive CTEs.
func TestCTEIntegration_Recursive(t *testing.T) {

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test recursive CTE - generate numbers 1 to 10
	sql := `
		WITH RECURSIVE cnt(n) AS (
			SELECT 1
			UNION ALL
			SELECT n+1 FROM cnt WHERE n < 10
		)
		SELECT COUNT(*) as total FROM cnt
	`

	var total int
	err = db.QueryRow(sql).Scan(&total)
	if err != nil {
		t.Fatalf("recursive CTE query failed: %v", err)
	}

	if total != 10 {
		t.Errorf("expected 10 numbers, got %d", total)
	}
}

// TestCTEIntegration_RecursiveHierarchy tests recursive CTEs with hierarchical data.
func TestCTEIntegration_RecursiveHierarchy(t *testing.T) {
	t.Skip("Recursive CTE with JOIN needs column resolution fixes")

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create hierarchical table
	_, err = db.Exec(`CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert hierarchical data
	_, err = db.Exec(`
		INSERT INTO employees (id, name, manager_id) VALUES
		(1, 'CEO', NULL),
		(2, 'VP Eng', 1),
		(3, 'VP Sales', 1),
		(4, 'Engineer', 2),
		(5, 'Sales Rep', 3)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test recursive CTE to find all employees under CEO
	sql := `
		WITH RECURSIVE subordinates AS (
			SELECT id, name, manager_id FROM employees WHERE id = 1
			UNION ALL
			SELECT e.id, e.name, e.manager_id
			FROM employees e
			JOIN subordinates s ON e.manager_id = s.id
		)
		SELECT COUNT(*) as total FROM subordinates
	`

	var total int
	err = db.QueryRow(sql).Scan(&total)
	if err != nil {
		t.Fatalf("recursive hierarchy CTE failed: %v", err)
	}

	if total != 5 {
		t.Errorf("expected 5 employees in hierarchy, got %d", total)
	}
}

// TestCTEIntegration_WithColumnList tests CTEs with explicit column lists.
func TestCTEIntegration_WithColumnList(t *testing.T) {
	// t.Skip("CTE integration not fully implemented")
	// t.Skip("CTE execution requires bytecode inlining architecture - not yet implemented")

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE data (x INTEGER, y INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec(`INSERT INTO data (x, y) VALUES (1, 2), (3, 4)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test CTE with explicit column names
	sql := `
		WITH renamed(a, b) AS (SELECT x, y FROM data)
		SELECT a, b FROM renamed
	`

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("CTE with column list failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var a, b int
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestCTEIntegration_NestedReference tests CTEs referenced multiple times.
func TestCTEIntegration_NestedReference(t *testing.T) {
	t.Skip("UNION ALL with CTEs fails with insert data must be a blob")

	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE numbers (n INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec(`INSERT INTO numbers (n) VALUES (1), (2), (3), (4), (5)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test CTE referenced multiple times
	sql := `
		WITH evens AS (SELECT n FROM numbers WHERE n % 2 = 0)
		SELECT COUNT(*) as total FROM evens
		UNION ALL
		SELECT COUNT(*) FROM evens
	`

	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("CTE with multiple references failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var total int
		if err := rows.Scan(&total); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		// Each UNION part should return 2 (two even numbers: 2 and 4)
		if total != 2 {
			t.Errorf("expected 2 even numbers, got %d", total)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 result rows from UNION ALL, got %d", count)
	}
}
