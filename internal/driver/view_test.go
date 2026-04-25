// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"testing"
)

// viewExecOrFatal executes a statement or fails.
func viewExecOrFatal(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec failed (%s): %v", stmt, err)
	}
}

// viewVerifyNames queries and verifies name column values match expected.
func viewVerifyNames(t *testing.T, db *sql.DB, query string, expectedNames []string) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if count < len(expectedNames) && name != expectedNames[count] {
			t.Errorf("Row %d: expected name %q, got %q", count, expectedNames[count], name)
		}
		count++
	}
	if count != len(expectedNames) {
		t.Errorf("Expected %d rows, got %d", len(expectedNames), count)
	}
}

func TestViewBasicOperations(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	viewExecOrFatal(t, db, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, active INTEGER)`)
	viewExecOrFatal(t, db, `INSERT INTO users (id, name, email, active) VALUES
		(1, 'Alice', 'alice@example.com', 1),
		(2, 'Bob', 'bob@example.com', 1),
		(3, 'Charlie', 'charlie@example.com', 0)`)
	viewExecOrFatal(t, db, `CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE active = 1`)

	viewVerifyNames(t, db, `SELECT * FROM active_users ORDER BY id`, []string{"Alice", "Bob"})

	viewExecOrFatal(t, db, `DROP VIEW active_users`)
	_, err = db.Query(`SELECT * FROM active_users`)
	if err == nil {
		t.Error("Expected error querying dropped view")
	}
}

// viewTestSetupProducts creates a products table and inserts sample data.
func viewTestSetupProducts(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT,
		price REAL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
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

	viewTestSetupProducts(t, db)

	_, err = db.Exec(`CREATE VIEW product_view(product_id, product_name, product_price) AS
		SELECT id, name, price FROM products`)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	rows, err := db.Query(`SELECT product_name, product_price FROM product_view ORDER BY product_id`)
	if err != nil {
		t.Fatalf("Failed to query view: %v", err)
	}
	defer rows.Close()

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

// viewCountRows counts the number of rows returned by a query.
func viewCountRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	return count
}

func TestViewWithJoin(t *testing.T) {
	tmpfile := tempFilename()
	defer os.Remove(tmpfile)

	db, err := sql.Open("sqlite_internal", tmpfile)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	viewExecOrFatal(t, db, `CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)`)
	viewExecOrFatal(t, db, `CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)`)
	viewExecOrFatal(t, db, `INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')`)
	viewExecOrFatal(t, db, `INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)`)
	viewExecOrFatal(t, db, `CREATE VIEW customer_orders AS
		SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id`)

	if count := viewCountRows(t, db, `SELECT * FROM customer_orders ORDER BY total`); count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

// viewVerifyInts queries and verifies integer column values match expected.
func viewVerifyInts(t *testing.T, db *sql.DB, query string, expected []int) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var value int
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if count < len(expected) && value != expected[count] {
			t.Errorf("Row %d: expected %d, got %d", count, expected[count], value)
		}
		count++
	}
	if count != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), count)
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

	viewExecOrFatal(t, db, `CREATE TABLE numbers (value INTEGER)`)
	viewExecOrFatal(t, db, `INSERT INTO numbers VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)`)
	viewExecOrFatal(t, db, `CREATE VIEW even_numbers AS SELECT value FROM numbers WHERE value % 2 = 0`)
	viewExecOrFatal(t, db, `CREATE VIEW large_even_numbers AS SELECT value FROM even_numbers WHERE value > 5`)

	viewVerifyInts(t, db, `SELECT * FROM large_even_numbers ORDER BY value`, []int{6, 8, 10})
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
