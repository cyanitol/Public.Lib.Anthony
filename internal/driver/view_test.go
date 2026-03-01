// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

func TestViewBasicOperations(t *testing.T) {
	// Create temporary database
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		email TEXT,
		active INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO users (id, name, email, active) VALUES
		(1, 'Alice', 'alice@example.com', 1),
		(2, 'Bob', 'bob@example.com', 1),
		(3, 'Charlie', 'charlie@example.com', 0)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create a view
	_, err = db.Exec(`CREATE VIEW active_users AS
		SELECT id, name, email FROM users WHERE active = 1`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(`SELECT * FROM active_users ORDER BY id`)
	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	// Verify results
	count := 0
	expectedNames := []string{"Alice", "Bob"}
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if count >= len(expectedNames) {
			t.Fatalf("Too many rows returned")
		}
		if name != expectedNames[count] {
			t.Errorf("Row %d: expected name %q, got %q", count, expectedNames[count], name)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	// Drop the view
	_, err = db.Exec(`DROP VIEW active_users`)
	if err != nil {
		t.Fatalf("Failed to drop view: %v", err)
	}

	// Verify view is gone (query should fail)
	_, err = db.Query(`SELECT * FROM active_users`)
	if err == nil {
		t.Error("Expected error querying dropped view")
	}
}

func TestViewWithColumnList(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create view with explicit column names
	_, err = db.Exec(`CREATE VIEW product_view(product_id, product_name, product_price) AS
		SELECT id, name, price FROM products`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(`SELECT product_name, product_price FROM product_view ORDER BY product_id`)
	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	// Verify we can access columns by their view names
	count := 0
	for rows.Next() {
		var name string
		var price float64
		if err := rows.Scan(&name, &price); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestViewIfNotExists(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE items (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create view
	_, err = db.Exec(`CREATE VIEW item_view AS SELECT * FROM items`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Try to create again without IF NOT EXISTS (should fail)
	_, err = db.Exec(`CREATE VIEW item_view AS SELECT * FROM items`)
	if err == nil {
		t.Error("Expected error creating duplicate view")
	}

	// Try with IF NOT EXISTS (should succeed)
	_, err = db.Exec(`CREATE VIEW IF NOT EXISTS item_view AS SELECT * FROM items`)
	if err != nil {
		t.Errorf("CREATE VIEW IF NOT EXISTS failed: %v", err)
	}
}

func TestDropViewIfExists(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to drop non-existent view without IF EXISTS (should fail)
	_, err = db.Exec(`DROP VIEW nonexistent_view`)
	if err == nil {
		t.Error("Expected error dropping non-existent view")
	}

	// Try with IF EXISTS (should succeed)
	_, err = db.Exec(`DROP VIEW IF EXISTS nonexistent_view`)
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS failed: %v", err)
	}
}

func TestViewWithJoin(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)`)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')`)
	if err != nil {
		t.Fatalf("Failed to insert customers: %v", err)
	}

	_, err = db.Exec(`INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)`)
	if err != nil {
		t.Fatalf("Failed to insert orders: %v", err)
	}

	// Create view with JOIN
	_, err = db.Exec(`CREATE VIEW customer_orders AS
		SELECT c.name, o.total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Query the view
	rows, err := db.Query(`SELECT * FROM customer_orders ORDER BY total`)
	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	// Verify results
	count := 0
	for rows.Next() {
		var name string
		var total float64
		if err := rows.Scan(&name, &total); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

func TestViewReferencingView(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE numbers (value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(`INSERT INTO numbers VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create first view
	_, err = db.Exec(`CREATE VIEW even_numbers AS SELECT value FROM numbers WHERE value % 2 = 0`)
	if err != nil {
		t.Fatalf("Failed to create first view: %v", err)
	}

	// Create second view that references first view
	_, err = db.Exec(`CREATE VIEW large_even_numbers AS SELECT value FROM even_numbers WHERE value > 5`)
	if err != nil {
		t.Fatalf("Failed to create second view: %v", err)
	}

	// Query the nested view
	rows, err := db.Query(`SELECT * FROM large_even_numbers ORDER BY value`)
	if err != nil {
		t.Fatalf("Failed to query nested view: %v", err)
	}
	defer rows.Close()

	// Verify results (should be 6, 8, 10)
	expected := []int{6, 8, 10}
	count := 0
	for rows.Next() {
		var value int
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if count >= len(expected) {
			t.Fatalf("Too many rows returned")
		}
		if value != expected[count] {
			t.Errorf("Row %d: expected %d, got %d", count, expected[count], value)
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

func TestTemporaryView(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`CREATE TABLE data (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create temporary view
	_, err = db.Exec(`CREATE TEMP VIEW temp_view AS SELECT * FROM data`)
	if err != nil {
		t.Fatalf("Failed to create temporary view: %v", err)
	}

	// Verify we can query it
	_, err = db.Query(`SELECT * FROM temp_view`)
	if err != nil {
		t.Fatalf("Failed to query temporary view: %v", err)
	}

	// Note: In a full implementation, temp views would be session-specific
	// and would be dropped when the connection closes
}

// Helper function to generate temporary filenames
func tempFilename() string {
	f, err := os.CreateTemp("", "anthony_test_*.db")
	if err != nil {
		panic(err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)
	return name
}
