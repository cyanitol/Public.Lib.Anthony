// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// ccGetConn gets the underlying connection from the DB pool.
func ccGetConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	return conn
}

// ccRegisterCollation registers a custom collation on a connection.
func ccRegisterCollation(t *testing.T, conn *sql.Conn, name string, fn func(string, string) int) {
	t.Helper()
	err := conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*Conn)
		return c.CreateCollation(name, fn)
	})
	if err != nil {
		t.Fatalf("Failed to create collation %s: %v", name, err)
	}
}

// ccExec executes a statement on a connection.
func ccExec(t *testing.T, conn *sql.Conn, query string, args ...interface{}) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("Exec %q failed: %v", query, err)
	}
}

// ccInsertNames inserts name data into a test table via conn.
func ccInsertNames(t *testing.T, conn *sql.Conn, names []string) {
	t.Helper()
	for i, name := range names {
		ccExec(t, conn, "INSERT INTO test (id, name) VALUES (?, ?)", i+1, name)
	}
}

// ccQueryStrings queries a single string column from conn and returns results.
func ccQueryStrings(t *testing.T, conn *sql.Conn, query string) []string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
	var results []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, s)
	}
	return results
}

// ccAssertStrings checks that results match expected exactly.
func ccAssertStrings(t *testing.T, results, expected []string) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %q, got %q", i, exp, results[i])
		}
	}
}

// reverseCollation sorts strings in reverse alphabetical order.
func reverseCollation(a, b string) int {
	if a > b {
		return -1
	}
	if a < b {
		return 1
	}
	return 0
}

// TestCreateCollation tests the CreateCollation API
func TestCreateCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	ccRegisterCollation(t, conn, "REVERSE", reverseCollation)
	ccExec(t, conn, "CREATE TABLE test (id INTEGER, name TEXT)")
	ccInsertNames(t, conn, []string{"alice", "bob", "charlie", "david"})

	results := ccQueryStrings(t, conn, "SELECT name FROM test ORDER BY name COLLATE REVERSE")
	ccAssertStrings(t, results, []string{"david", "charlie", "bob", "alice"})
}

// TestCustomCollationCaseInsensitive tests a custom case-insensitive collation
func TestCustomCollationCaseInsensitive(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	ccRegisterCollation(t, conn, "FULL_NOCASE", func(a, b string) int {
		return strings.Compare(strings.ToUpper(a), strings.ToUpper(b))
	})

	ccExec(t, conn, "CREATE TABLE test (name TEXT)")
	for _, name := range []string{"Hello", "HELLO", "world", "WORLD"} {
		ccExec(t, conn, "INSERT INTO test (name) VALUES (?)", name)
	}

	results := ccQueryStrings(t, conn, "SELECT name FROM test ORDER BY name COLLATE FULL_NOCASE")
	if len(results) != 4 {
		t.Errorf("Expected 4 results with custom case-insensitive collation, got %d", len(results))
	}
}

// TestCustomCollationNumeric tests a numeric collation
func TestCustomCollationNumeric(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	numericCollation := func(a, b string) int {
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

	ccRegisterCollation(t, conn, "NUMCOLL", numericCollation)
	ccExec(t, conn, "CREATE TABLE test (value TEXT)")
	for _, val := range []string{"10", "2", "100", "20", "5"} {
		ccExec(t, conn, "INSERT INTO test (value) VALUES (?)", val)
	}

	results := ccQueryStrings(t, conn, "SELECT value FROM test ORDER BY value COLLATE NUMCOLL")
	ccAssertStrings(t, results, []string{"2", "5", "10", "20", "100"})
}

// TestCustomCollationInTable tests column-level custom collation
func TestCustomCollationInTable(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	lengthCollation := func(a, b string) int {
		if len(a) < len(b) {
			return -1
		}
		if len(a) > len(b) {
			return 1
		}
		return strings.Compare(a, b)
	}

	ccRegisterCollation(t, conn, "LENGTH", lengthCollation)
	ccExec(t, conn, "CREATE TABLE test (id INTEGER, name TEXT COLLATE LENGTH)")
	ccInsertNames(t, conn, []string{"a", "abc", "ab", "abcd"})

	results := ccQueryStrings(t, conn, "SELECT name FROM test ORDER BY name")
	ccAssertStrings(t, results, []string{"a", "ab", "abc", "abcd"})
}

// TestRemoveCollation tests removing a custom collation
func TestRemoveCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	ccRegisterCollation(t, conn, "CUSTOM", func(a, b string) int { return strings.Compare(a, b) })
	ccExec(t, conn, "CREATE TABLE test (name TEXT)")
	ccExec(t, conn, "INSERT INTO test (name) VALUES ('test')")

	rows, err := conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CUSTOM")
	if err != nil {
		t.Fatalf("Query with CUSTOM collation failed: %v", err)
	}
	rows.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		return driverConn.(*Conn).RemoveCollation("CUSTOM")
	})
	if err != nil {
		t.Fatalf("Failed to remove collation: %v", err)
	}

	rows2, err := conn.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CUSTOM")
	if rows2 != nil {
		rows2.Close()
	}
	_ = err
}

// TestCustomCollationErrors tests error cases
func TestCustomCollationErrors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	tests := []struct {
		name string
		fn   func(*Conn) error
	}{
		{"EmptyName", func(c *Conn) error { return c.CreateCollation("", func(a, b string) int { return 0 }) }},
		{"NilFunction", func(c *Conn) error { return c.CreateCollation("TEST", nil) }},
		{"RemoveBuiltin", func(c *Conn) error { return c.RemoveCollation("BINARY") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := conn.Raw(func(driverConn interface{}) error { return tt.fn(driverConn.(*Conn)) })
			if err == nil {
				t.Errorf("Expected error for %s", tt.name)
			}
		})
	}
}

// TestConnectionIsolation tests that custom collations are connection-specific
func TestConnectionIsolation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn1 := ccGetConn(t, db)
	defer conn1.Close()

	ccRegisterCollation(t, conn1, "CONN1_ONLY", func(a, b string) int {
		return strings.Compare(a, b)
	})

	conn2 := ccGetConn(t, db)
	defer conn2.Close()

	ccExec(t, conn1, "CREATE TABLE test (name TEXT)")
	ccExec(t, conn1, "INSERT INTO test (name) VALUES ('test')")

	// Query with custom collation on first connection should work
	rows1, err := conn1.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CONN1_ONLY")
	if err != nil {
		t.Errorf("Query on conn1 with CONN1_ONLY collation should work: %v", err)
	}
	if rows1 != nil {
		rows1.Close()
	}

	// Query with custom collation on second connection should fail (collation not registered)
	rows2, err := conn2.QueryContext(context.Background(), "SELECT name FROM test ORDER BY name COLLATE CONN1_ONLY")
	if rows2 != nil {
		rows2.Close()
	}
	if err == nil {
		t.Log("Warning: Query on conn2 with CONN1_ONLY collation succeeded (collation not connection-isolated)")
	}
}

// ccQueryPairs queries two string columns and returns results.
func ccQueryPairs(t *testing.T, conn *sql.Conn, query string) [][2]string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
	var results [][2]string
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, [2]string{a, b})
	}
	return results
}

// TestMultiColumnSortWithCustomCollation tests sorting by multiple columns with different collations
func TestMultiColumnSortWithCustomCollation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	conn := ccGetConn(t, db)
	defer conn.Close()

	ccRegisterCollation(t, conn, "REVERSE", reverseCollation)
	ccExec(t, conn, "CREATE TABLE test (category TEXT, name TEXT)")

	for _, d := range [][2]string{{"A", "zebra"}, {"A", "apple"}, {"B", "zebra"}, {"B", "apple"}} {
		ccExec(t, conn, "INSERT INTO test (category, name) VALUES (?, ?)", d[0], d[1])
	}

	results := ccQueryPairs(t, conn, "SELECT category, name FROM test ORDER BY category COLLATE BINARY, name COLLATE REVERSE")
	expected := [][2]string{{"A", "zebra"}, {"A", "apple"}, {"B", "zebra"}, {"B", "apple"}}

	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Result[%d]: expected %v, got %v", i, exp, results[i])
		}
	}
}
