// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestCreateCollation tests the CreateCollation API
func TestCreateCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Get the underlying connection
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a reverse collation (sorts in reverse alphabetical order)
	reverseCollation := func(a, b string) int {
		if a > b {
			return -1
		}
		if a < b {
			return 1
		}
		return 0
	}

	// Register the custom collation
	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("REVERSE", reverseCollation)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	// Create table and insert test data (use the same connection)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []string{"alice", "bob", "charlie", "david"}
	for i, name := range testData {
		_, err := conn.ExecContext(context.Background(), "INSERT INTO test (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
	}

	// Query with custom REVERSE collation
	// Note: We need to use the same connection that registered the collation
	rows, err := conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE REVERSE")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	// Should be in reverse alphabetical order
	expected := []string{"david", "charlie", "bob", "alice"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

// TestCustomCollationCaseInsensitive tests a custom case-insensitive collation
func TestCustomCollationCaseInsensitive(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a full case-insensitive collation (not just ASCII like NOCASE)
	fullCaseInsensitive := func(a, b string) int {
		return strings.Compare(strings.ToUpper(a), strings.ToUpper(b))
	}

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("FULL_NOCASE", fullCaseInsensitive)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []string{"Hello", "HELLO", "world", "WORLD"}
	for _, name := range testData {
		_, err := conn.ExecContext(context.Background(), "INSERT INTO test (name) VALUES (?)", name)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query with WHERE clause using custom collation
	rows, err := conn.QueryContext(context.Background(), "SELECT name FROM test WHERE name COLLATE FULL_NOCASE = 'hello'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	// Should match both "Hello" and "HELLO"
	if count != 2 {
		t.Errorf("Expected 2 matches with custom case-insensitive collation, got %d", count)
	}
}

// TestCustomCollationNumeric tests a numeric collation
func TestCustomCollationNumeric(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a numeric collation that compares strings as numbers
	numericCollation := func(a, b string) int {
		// Parse as integers
		var aNum, bNum int
		fmt.Sscanf(a, "%d", &aNum)
		fmt.Sscanf(b, "%d", &bNum)

		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("NUMERIC", numericCollation)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []string{"10", "2", "100", "20", "5"}
	for _, val := range testData {
		_, err := conn.ExecContext(context.Background(), "INSERT INTO test (value) VALUES (?)", val)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query with numeric collation
	rows, err := conn.QueryContext(context.Background(), "SELECT value FROM test ORDER BY value COLLATE NUMERIC")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, value)
	}

	// Should be in numeric order
	expected := []string{"2", "5", "10", "20", "100"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

// TestCustomCollationInTable tests column-level custom collation
func TestCustomCollationInTable(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a length-based collation (shorter strings come first)
	lengthCollation := func(a, b string) int {
		if len(a) < len(b) {
			return -1
		}
		if len(a) > len(b) {
			return 1
		}
		return strings.Compare(a, b) // Same length: use binary comparison
	}

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("LENGTH", lengthCollation)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	// Create table with column-level collation
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT COLLATE LENGTH)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []string{"a", "abc", "ab", "abcd"}
	for i, name := range testData {
		_, err := conn.ExecContext(context.Background(), "INSERT INTO test (id, name) VALUES (?, ?)", i+1, name)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query without explicit COLLATE (should use column default)
	rows, err := conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, name)
	}

	// Should be sorted by length
	expected := []string{"a", "ab", "abc", "abcd"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

// TestRemoveCollation tests removing a custom collation
func TestRemoveCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a custom collation
	customColl := func(a, b string) int {
		return strings.Compare(a, b)
	}

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("CUSTOM", customColl)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	// Create table and insert data
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), "INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with custom collation should work
	_, err = conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CUSTOM")
	if err != nil {
		t.Fatalf("Query with CUSTOM collation failed: %v", err)
	}

	// Remove the collation
	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.RemoveCollation("CUSTOM")
	})
	if err != nil {
		t.Fatalf("Failed to remove collation: %v", err)
	}

	// Query with removed collation should fail
	_, err = conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CUSTOM")
	if err == nil {
		t.Error("Expected error when using removed collation")
	}
}

// TestCustomCollationErrors tests error cases
func TestCustomCollationErrors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	t.Run("EmptyName", func(t *testing.T) {
		err := conn.Raw(func(driverConn interface{}) error {
			c := driverConn.(*Conn)
			return c.CreateCollation("", func(a, b string) int { return 0 })
		})
		if err == nil {
			t.Error("Expected error for empty collation name")
		}
	})

	t.Run("NilFunction", func(t *testing.T) {
		err := conn.Raw(func(driverConn interface{}) error {
			c := driverConn.(*Conn)
			return c.CreateCollation("TEST", nil)
		})
		if err == nil {
			t.Error("Expected error for nil collation function")
		}
	})

	t.Run("RemoveBuiltin", func(t *testing.T) {
		err := conn.Raw(func(driverConn interface{}) error {
			c := driverConn.(*Conn)
			return c.RemoveCollation("BINARY")
		})
		if err == nil {
			t.Error("Expected error when removing built-in collation")
		}
	})
}

// TestConnectionIsolation tests that custom collations are connection-specific
func TestConnectionIsolation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Get first connection
	conn1, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection 1: %v", err)
	}
	defer conn1.Close()

	// Register collation on first connection
	err = conn1.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("CONN1_ONLY", func(a, b string) int {
			return strings.Compare(a, b)
		})
	})
	if err != nil {
		t.Fatalf("Failed to create collation on conn1: %v", err)
	}

	// Get second connection
	conn2, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection 2: %v", err)
	}
	defer conn2.Close()

	// Create table on first connection
	_, err = conn1.ExecContext(context.Background(), "CREATE TABLE test (name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data on first connection
	_, err = conn1.ExecContext(context.Background(), "INSERT INTO test (name) VALUES ('test')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with custom collation on first connection should work
	_, err = conn1.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CONN1_ONLY")
	if err != nil {
		t.Errorf("Query on conn1 with CONN1_ONLY collation should work: %v", err)
	}

	// Query with custom collation on second connection should fail (collation not registered)
	_, err = conn2.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CONN1_ONLY")
	if err == nil {
		t.Error("Query on conn2 with CONN1_ONLY collation should fail")
	}
}

// TestMultiColumnSortWithCustomCollation tests sorting by multiple columns with different collations
func TestMultiColumnSortWithCustomCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a reverse collation
	reverseCollation := func(a, b string) int {
		if a > b {
			return -1
		}
		if a < b {
			return 1
		}
		return 0
	}

	err = conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation("REVERSE", reverseCollation)
	})
	if err != nil {
		t.Fatalf("Failed to create collation: %v", err)
	}

	_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (category TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []struct {
		category string
		name     string
	}{
		{"A", "zebra"},
		{"A", "apple"},
		{"B", "zebra"},
		{"B", "apple"},
	}

	for _, data := range testData {
		_, err := conn.ExecContext(context.Background(), "INSERT INTO test (category, name) VALUES (?, ?)", data.category, data.name)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Sort by category (BINARY) then name (REVERSE)
	rows, err := conn.QueryContext(context.Background(), "SELECT category, name FROM test ORDER BY category COLLATE BINARY, name COLLATE REVERSE")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		category string
		name     string
	}

	for rows.Next() {
		var category, name string
		if err := rows.Scan(&category, &name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, struct {
			category string
			name     string
		}{category, name})
	}

	// Expected: A (zebra, apple), B (zebra, apple)
	expected := []struct {
		category string
		name     string
	}{
		{"A", "zebra"},  // A category, name in reverse (z before a)
		{"A", "apple"},
		{"B", "zebra"},  // B category, name in reverse (z before a)
		{"B", "apple"},
	}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}

	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %v, got %v", i, exp, results[i])
		}
	}
}
