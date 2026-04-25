// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

func windowFuncSetupDB(t *testing.T, create string, inserts []string) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if _, err := db.Exec(create); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	for _, ins := range inserts {
		if _, err := db.Exec(ins); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	return db
}

func windowFuncVerifyInt64(t *testing.T, rows *sql.Rows, expected []int64) {
	t.Helper()
	i := 0
	for rows.Next() {
		var val int64
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if i < len(expected) && val != expected[i] {
			t.Errorf("Row %d: expected %d, got %d", i, expected[i], val)
		}
		i++
	}
	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
}

// TestWindowFunctionRowNumber tests basic row_number() window function
func TestWindowFunctionRowNumber(t *testing.T) {
	db := windowFuncSetupDB(t,
		"CREATE TABLE users (id INTEGER, name TEXT)",
		[]string{
			"INSERT INTO users (id, name) VALUES (1, 'Alice')",
			"INSERT INTO users (id, name) VALUES (2, 'Bob')",
			"INSERT INTO users (id, name) VALUES (3, 'Charlie')",
		})
	defer db.Close()

	rows, err := db.Query("SELECT row_number() OVER (ORDER BY id) FROM users")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}
	defer rows.Close()

	windowFuncVerifyInt64(t, rows, []int64{1, 2, 3})
}

// TestWindowFunctionRowNumberWithColumns tests row_number() alongside regular columns
func windowFuncVerifyNameAndRowNum(t *testing.T, rows *sql.Rows, names []string) {
	t.Helper()
	i := 0
	for rows.Next() {
		var name string
		var rowNum int64
		if err := rows.Scan(&name, &rowNum); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if i < len(names) {
			if rowNum != int64(i+1) {
				t.Errorf("Row %d: expected row_number=%d, got %d", i, i+1, rowNum)
			}
			if name != names[i] {
				t.Errorf("Row %d: expected name=%s, got %s", i, names[i], name)
			}
		}
		i++
	}
	if i != len(names) {
		t.Errorf("Expected %d rows, got %d", len(names), i)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
}

func TestWindowFunctionRowNumberWithColumns(t *testing.T) {
	db := windowFuncSetupDB(t,
		"CREATE TABLE products (id INTEGER, name TEXT, price REAL)",
		[]string{
			"INSERT INTO products (id, name, price) VALUES (10, 'Widget', 9.99)",
			"INSERT INTO products (id, name, price) VALUES (20, 'Gadget', 19.99)",
			"INSERT INTO products (id, name, price) VALUES (30, 'Doohickey', 14.99)",
		})
	defer db.Close()

	rows, err := db.Query("SELECT name, row_number() OVER (ORDER BY id) FROM products")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}
	defer rows.Close()

	windowFuncVerifyNameAndRowNum(t, rows, []string{"Widget", "Gadget", "Doohickey"})
}
