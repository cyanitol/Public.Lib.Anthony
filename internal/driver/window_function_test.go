package driver

import (
	"database/sql"
	"testing"
)

// TestWindowFunctionRowNumber tests basic row_number() window function
func TestWindowFunctionRowNumber(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (3, 'Charlie')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test row_number() with ORDER BY
	rows, err := db.Query("SELECT row_number() OVER (ORDER BY id) FROM users")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}
	defer rows.Close()

	expected := []int64{1, 2, 3}
	i := 0

	for rows.Next() {
		var rowNum int64
		if err := rows.Scan(&rowNum); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("Too many rows returned")
		}

		if rowNum != expected[i] {
			t.Errorf("Row %d: expected row_number=%d, got %d", i, expected[i], rowNum)
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

// TestWindowFunctionRowNumberWithColumns tests row_number() alongside regular columns
func TestWindowFunctionRowNumberWithColumns(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []struct {
		id    int
		name  string
		price float64
	}{
		{10, "Widget", 9.99},
		{20, "Gadget", 19.99},
		{30, "Doohickey", 14.99},
	}

	for _, td := range testData {
		_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", td.id, td.name, td.price)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test row_number() with other columns
	rows, err := db.Query("SELECT name, row_number() OVER (ORDER BY id) FROM products")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var name string
		var rowNum int64

		if err := rows.Scan(&name, &rowNum); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(testData) {
			t.Fatalf("Too many rows returned")
		}

		expectedRowNum := int64(i + 1)
		if rowNum != expectedRowNum {
			t.Errorf("Row %d: expected row_number=%d, got %d", i, expectedRowNum, rowNum)
		}

		if name != testData[i].name {
			t.Errorf("Row %d: expected name=%s, got %s", i, testData[i].name, name)
		}

		i++
	}

	if i != len(testData) {
		t.Errorf("Expected %d rows, got %d", len(testData), i)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}
}
