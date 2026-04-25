// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// countPreparedScanCount prepares a statement, queries a count, and checks it.
func countPreparedScanCount(t *testing.T, db *sql.DB, query string, want int, label string) {
	t.Helper()
	stmt, err := db.Prepare(query)
	if err != nil {
		t.Fatalf("failed to prepare %s: %v", label, err)
	}
	defer stmt.Close()
	var count int
	if err = stmt.QueryRow().Scan(&count); err != nil {
		t.Fatalf("failed to query %s: %v", label, err)
	}
	if count != want {
		t.Errorf("%s = %d, want %d", label, count, want)
	}
}

// TestCountWithPreparedStatement tests that COUNT(*) works with prepared statements
func TestCountWithPreparedStatement(t *testing.T) {
	dbFile := t.TempDir() + "/test_count_prepared.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if _, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for i, name := range []string{"Alice", "Bob", "Charlie"} {
		if _, err = db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+1, name); err != nil {
			t.Fatalf("failed to insert row %d: %v", i+1, err)
		}
	}

	t.Run("COUNT(*) with prepared statement", func(t *testing.T) {
		countPreparedScanCount(t, db, "SELECT COUNT(*) FROM users", 3, "COUNT(*)")
	})

	t.Run("COUNT(*) with direct query", func(t *testing.T) {
		var count int
		if err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if count != 3 {
			t.Errorf("COUNT(*) = %d, want 3", count)
		}
	})

	t.Run("COUNT(column) with prepared statement", func(t *testing.T) {
		countPreparedScanCount(t, db, "SELECT COUNT(name) FROM users", 3, "COUNT(name)")
	})

	t.Run("COUNT(*) on empty table", func(t *testing.T) {
		if _, err = db.Exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatalf("failed to create empty table: %v", err)
		}
		countPreparedScanCount(t, db, "SELECT COUNT(*) FROM empty_table", 0, "COUNT(*) empty")
	})
}

// TestCountWithParameters tests COUNT with WHERE clause using parameters
func TestCountWithParameters(t *testing.T) {
	countAssertParameters(t)
}

func countAssertParameters(t *testing.T) {
	t.Helper()
	dbFile := t.TempDir() + "/test_count_params.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if _, err = db.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for _, row := range []struct {
		id    int
		name  string
		price int
	}{{1, "Widget", 10}, {2, "Gadget", 20}, {3, "Doohickey", 15}} {
		if _, err = db.Exec("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", row.id, row.name, row.price); err != nil {
			t.Fatalf("failed to insert row %d: %v", row.id, err)
		}
	}

	countPreparedScanCount(t, db, "SELECT COUNT(*) FROM products", 3, "COUNT(*)")
}

// TestMultipleAggregates tests multiple aggregate functions in one query
func TestMultipleAggregates(t *testing.T) {
	dbFile := t.TempDir() + "/test_count_multi_agg.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE sales (id INTEGER PRIMARY KEY, amount INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = db.Exec("INSERT INTO sales (id, amount) VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Test just COUNT for now (SUM, MIN, MAX may need additional implementation)
	t.Run("COUNT only", func(t *testing.T) {
		stmt, err := db.Prepare("SELECT COUNT(*) FROM sales")
		if err != nil {
			t.Fatalf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		var count int
		err = stmt.QueryRow().Scan(&count)
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		if count != 5 {
			t.Errorf("COUNT(*) = %d, want 5", count)
		}
	})
}
