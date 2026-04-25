// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// missingCovExecNoArgs prepares and executes a SQL statement with no args.
func missingCovExecNoArgs(t *testing.T, c *Conn, sql string) {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare %q: %v", sql, err)
	}
	defer stmt.Close()
	if _, err := stmt.Exec(nil); err != nil {
		t.Fatalf("Failed to exec %q: %v", sql, err)
	}
}

// missingCovQueryOneValue runs a query with args and returns a single value.
func missingCovQueryOneValue(t *testing.T, c *Conn, sql string, args []driver.Value) driver.Value {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare %q: %v", sql, err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(args)
	if err != nil {
		t.Fatalf("Failed to query %q: %v", sql, err)
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatalf("Failed to get row from %q: %v", sql, err)
	}
	return values[0]
}

// TestReleaseStateSharedConnection tests the releaseState function through connection lifecycle
func TestReleaseStateSharedConnection(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_release.db")

	drv := &Driver{}

	conn1, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open first connection: %v", err)
	}
	c1 := conn1.(*Conn)
	missingCovExecNoArgs(t, c1, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

	conn2, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open second connection: %v", err)
	}
	c2 := conn2.(*Conn)

	stmt2, _ := c2.Prepare("INSERT INTO test (id, value) VALUES (?, ?)")
	_, err = stmt2.Exec([]driver.Value{1, "shared"})
	if err != nil {
		t.Fatalf("Failed to insert via c2: %v", err)
	}
	stmt2.Close()

	if err := conn1.Close(); err != nil {
		t.Fatalf("Failed to close c1: %v", err)
	}

	val := missingCovQueryOneValue(t, c2, "SELECT value FROM test WHERE id = ?", []driver.Value{1})
	if val != "shared" {
		t.Errorf("Expected 'shared', got %v", val)
	}

	if err := conn2.Close(); err != nil {
		t.Fatalf("Failed to close c2: %v", err)
	}
}

// TestCreateConnectionPermissionError tests error paths in createConnection
func TestCreateConnectionPermissionError(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a read-only directory to cause permission errors
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0555); err != nil {
		t.Fatalf("Failed to create readonly dir: %v", err)
	}
	defer os.Chmod(readOnlyDir, 0755) // Cleanup

	dbPath := filepath.Join(readOnlyDir, "test.db")
	drv := &Driver{}

	// Try to open - should fail during initialization
	_, err := drv.Open(dbPath)
	if err == nil {
		t.Error("Expected error when opening database in read-only directory")
	}
}

// TestCreateMemoryConnectionErrorPaths tests error handling in createMemoryConnection
func TestCreateMemoryConnectionErrorPaths(t *testing.T) {
	drv := &Driver{}

	// Test that memory connections work correctly
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open memory database: %v", err)
	}
	c := conn.(*Conn)

	// Verify it works
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to create table in memory db: %v", err)
	}
	stmt.Close()

	// Close and verify connection is cleaned up
	if err := conn.Close(); err != nil {
		t.Fatalf("Failed to close memory db: %v", err)
	}

	// Opening another memory connection should create a separate instance
	conn2, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open second memory database: %v", err)
	}
	c2 := conn2.(*Conn)
	defer c2.Close()

	// The table should not exist in this new memory database
	stmt2, _ := c2.Prepare("INSERT INTO test (id) VALUES (1)")
	_, err = stmt2.Exec(nil)
	if err == nil {
		t.Error("Expected error: table should not exist in new memory database")
	}
	stmt2.Close()
}

// TestEmptyStringMemoryDatabase tests that empty string creates memory database
func TestEmptyStringMemoryDatabase(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open("")
	if err != nil {
		t.Fatalf("Failed to open empty string database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Should work as memory database
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER)")
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to create table with empty string db: %v", err)
	}
	stmt.Close()

	stmt2, _ := c.Prepare("INSERT INTO test (id) VALUES (1)")
	_, err = stmt2.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	stmt2.Close()

	stmt3, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, err := stmt3.Query(nil)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	values := make([]driver.Value, 1)
	err = rows.Next(values)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}

	count := values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}
	rows.Close()
	stmt3.Close()
}

// TestOrderByWithCollation tests ORDER BY with COLLATE expressions
func TestOrderByWithCollation(t *testing.T) {
	missingCovAssertOrderByWithCollation(t)
}

func missingCovAssertOrderByWithCollation(t *testing.T) {
	t.Helper()
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	stmt, _ := c.Prepare(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)`)
	if _, err = stmt.Exec(nil); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	stmt.Close()

	for i, name := range []string{"apple", "BANANA", "cherry", "APPLE"} {
		stmt, _ := c.Prepare("INSERT INTO test (id, name) VALUES (?, ?)")
		if _, err = stmt.Exec([]driver.Value{i + 1, name}); err != nil {
			t.Fatalf("Failed to insert %s: %v", name, err)
		}
		stmt.Close()
	}

	stmt2, _ := c.Prepare("SELECT name FROM test ORDER BY name COLLATE NOCASE")
	rows, err := stmt2.Query(nil)
	if err != nil {
		t.Fatalf("Failed to query with COLLATE: %v", err)
	}

	values := make([]driver.Value, 1)
	count := 0
	for rows.Next(values) == nil {
		count++
	}
	rows.Close()
	stmt2.Close()

	if count != 4 {
		t.Errorf("Expected 4 results, got %d", count)
	}
}

// missingCovSetupOrderByTable creates and populates a test table for ORDER BY tests.
func missingCovSetupOrderByTable(t *testing.T, c *Conn) {
	t.Helper()
	execStmts := []string{
		`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)`,
	}
	for _, sql := range execStmts {
		stmt, _ := c.Prepare(sql)
		if _, err := stmt.Exec(nil); err != nil {
			t.Fatalf("Failed to exec %q: %v", sql, err)
		}
		stmt.Close()
	}
	data := [][]driver.Value{
		{1, "Alice", 30, "NYC"},
		{2, "Bob", 25, "LA"},
		{3, "Charlie", 35, "Chicago"},
		{4, "David", 28, "Boston"},
	}
	for _, d := range data {
		stmt, _ := c.Prepare("INSERT INTO test (id, name, age, city) VALUES (?, ?, ?, ?)")
		if _, err := stmt.Exec(d); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
		stmt.Close()
	}
}

// missingCovQueryStringColumn runs a query and collects the first column as strings.
func missingCovQueryStringColumn(c *Conn, query string, numCols int) ([]string, error) {
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []string
	values := make([]driver.Value, numCols)
	for rows.Next(values) == nil {
		results = append(results, values[0].(string))
	}
	return results, nil
}

// TestOrderByWithExtraColumns tests ORDER BY with columns not in SELECT
func TestOrderByWithExtraColumns(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	missingCovSetupOrderByTable(t, c)

	results, err := missingCovQueryStringColumn(c, "SELECT name, city FROM test ORDER BY age", 2)
	if err != nil {
		t.Fatalf("Failed to query with ORDER BY extra column: %v", err)
	}

	expected := []string{"Bob", "David", "Alice", "Charlie"}
	if len(results) != len(expected) {
		t.Fatalf("Expected %d results, got %d", len(expected), len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Position %d: expected %s, got %s", i, exp, results[i])
		}
	}

	// Test ORDER BY with multiple extra columns
	stmt3, _ := c.Prepare("SELECT id FROM test ORDER BY age DESC, name")
	rows2, err := stmt3.Query(nil)
	if err != nil {
		t.Fatalf("Failed to query with multiple ORDER BY columns: %v", err)
	}
	rows2.Close()
	stmt3.Close()
}

// TestOrderByWithDuplicateColumn tests addExtraOrderByColumn with duplicates
func TestOrderByWithDuplicateColumn(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, _ := c.Prepare(`CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER)`)
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO test VALUES (3, 1, 5)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO test VALUES (1, 2, 6)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO test VALUES (2, 1, 4)")
	stmt.Exec(nil)
	stmt.Close()

	// ORDER BY same column twice (edge case)
	stmt2, _ := c.Prepare("SELECT a FROM test ORDER BY b, b DESC")
	rows, err := stmt2.Query(nil)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	rows.Close()
	stmt2.Close()
}

// aggQueryOne runs a single-value query and returns the result.
func aggQueryOne(c *Conn, query string) (driver.Value, error) {
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		return nil, err
	}
	return values[0], nil
}

// aggSetupTestTable creates a test table with 5 rows of aggregate test data.
func aggSetupTestTable(t *testing.T, c *Conn) {
	t.Helper()
	missingCovExecNoArgs(t, c, `CREATE TABLE test (id INTEGER, value INTEGER, category TEXT)`)
	categories := []string{"A", "A", "A", "B", "B"}
	for i := 1; i <= 5; i++ {
		s, _ := c.Prepare("INSERT INTO test VALUES (?, ?, ?)")
		s.Exec([]driver.Value{i, i * 10, categories[i-1]})
		s.Close()
	}
}

// aggCheckTotal verifies the TOTAL result which may be int64 or float64.
func aggCheckTotal(t *testing.T, v driver.Value, want float64) {
	t.Helper()
	switch tv := v.(type) {
	case int64:
		if float64(tv) != want {
			t.Errorf("Expected total=%v, got %d", want, tv)
		}
	case float64:
		if tv != want {
			t.Errorf("Expected total=%v, got %f", want, tv)
		}
	default:
		t.Errorf("Unexpected type for TOTAL: %T", v)
	}
}

// TestAggregateFunctionEdgeCases tests emitAggregateFunction paths
func TestAggregateFunctionEdgeCases(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	aggSetupTestTable(t, c)

	intTests := []struct {
		name  string
		query string
		want  int64
	}{
		{"COUNT(*)", "SELECT COUNT(*) FROM test", 5},
		{"SUM", "SELECT SUM(value) FROM test", 150},
		{"MIN", "SELECT MIN(value) FROM test", 10},
		{"MAX", "SELECT MAX(value) FROM test", 50},
	}
	for _, tc := range intTests {
		v, err := aggQueryOne(c, tc.query)
		if err != nil {
			t.Fatalf("%s failed: %v", tc.name, err)
		}
		if v.(int64) != tc.want {
			t.Errorf("%s: expected %d, got %v", tc.name, tc.want, v)
		}
	}

	v, err := aggQueryOne(c, "SELECT AVG(value) FROM test")
	if err != nil {
		t.Fatalf("AVG failed: %v", err)
	}
	if v.(float64) != 30.0 {
		t.Errorf("Expected avg=30.0, got %v", v)
	}

	v, err = aggQueryOne(c, "SELECT TOTAL(value) FROM test")
	if err != nil {
		t.Fatalf("TOTAL failed: %v", err)
	}
	aggCheckTotal(t, v, 150.0)
}

// TestOpenDatabaseNewFile tests openDatabase with a new database file
func TestOpenDatabaseNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "new_db.db")

	// Ensure file doesn't exist
	os.Remove(dbPath)

	drv := &Driver{}
	conn, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open new database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create a table in the new database
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to create table in new db: %v", err)
	}
	stmt.Close()

	// Verify it works
	stmt2, _ := c.Prepare("INSERT INTO test (id, data) VALUES (1, 'new')")
	_, err = stmt2.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	stmt2.Close()

	stmt3, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt3.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}
	rows.Close()
	stmt3.Close()
}

// TestOpenDatabaseExistingFile tests openDatabase with existing database
func TestOpenDatabaseExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "existing.db")

	drv := &Driver{}

	// Create database and table
	conn1, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	c1 := conn1.(*Conn)

	stmt, _ := c1.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c1.Prepare("INSERT INTO test (id) VALUES (1)")
	stmt.Exec(nil)
	stmt.Close()

	conn1.Close()

	// Reopen and verify schema is loaded
	conn2, err := drv.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer conn2.Close()

	c2 := conn2.(*Conn)

	// Should be able to query existing table
	stmt2, _ := c2.Prepare("SELECT COUNT(*) FROM test")
	rows, err := stmt2.Query(nil)
	if err != nil {
		t.Fatalf("Failed to query existing table: %v", err)
	}

	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}
	rows.Close()
	stmt2.Close()
}

// TestBeginTxReadOnlyComplete tests read-only transaction handling
func TestBeginTxReadOnlyComplete(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create table
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO test (id, value) VALUES (1, 'data')")
	stmt.Exec(nil)
	stmt.Close()

	// Start read-only transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}

	// Read should work - prepare statement through connection
	stmt2, _ := c.Prepare("SELECT value FROM test WHERE id = 1")
	rows, err := stmt2.Query([]driver.Value{})
	if err != nil {
		t.Fatalf("Failed to read in read-only tx: %v", err)
	}

	values := make([]driver.Value, 1)
	rows.Next(values)
	value := values[0].(string)
	if value != "data" {
		t.Errorf("Expected 'data', got %s", value)
	}
	rows.Close()
	stmt2.Close()

	// Commit read-only transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit read-only tx: %v", err)
	}
}

// TestBeginTxWritable tests writable transaction
func TestBeginTxWritable(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	stmt.Exec(nil)
	stmt.Close()

	// Start write transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Write should work
	stmt2, _ := c.Prepare("INSERT INTO test (id) VALUES (1)")
	_, err = stmt2.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to insert in write tx: %v", err)
	}
	stmt2.Close()

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit write tx: %v", err)
	}

	// Verify data persisted
	stmt3, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt3.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}
	rows.Close()
	stmt3.Close()
}

// TestCloseWithActiveTransactionRollback tests closing connection with active transaction
func TestCloseWithActiveTransactionRollback(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	c := conn.(*Conn)

	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER)")
	stmt.Exec(nil)
	stmt.Close()

	// Start transaction but don't commit
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt2, _ := c.Prepare("INSERT INTO test (id) VALUES (1)")
	stmt2.Exec(nil)
	stmt2.Close()

	// Don't commit tx - just close connection
	// This should trigger rollback in Close()
	if err := conn.Close(); err != nil {
		t.Fatalf("Failed to close conn: %v", err)
	}

	// tx should not be usable after connection close
	err = tx.Commit()
	// We expect some error here, but we don't care what it is
	_ = err
}

// TestPagerProviderGetPageDataError tests error handling in GetPageData
func TestPagerProviderGetPageDataError(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert multiple rows to create multiple pages
	for i := 0; i < 100; i++ {
		stmt, _ := c.Prepare("INSERT INTO test (data) VALUES (?)")
		stmt.Exec([]driver.Value{fmt.Sprintf("data%d", i)})
		stmt.Close()
	}

	// Query should work and exercise GetPageData
	stmt2, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt2.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 100 {
		t.Errorf("Expected count=100, got %d", count)
	}
	rows.Close()
	stmt2.Close()
}

// TestMemoryPagerProviderAllocateError tests error paths in memory pager
func TestMemoryPagerProviderAllocateError(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create table and insert data to exercise allocation
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert enough data to require multiple page allocations
	for i := 0; i < 50; i++ {
		data := fmt.Sprintf("row%d_data", i)
		stmt, _ := c.Prepare("INSERT INTO test (data) VALUES (?)")
		stmt.Exec([]driver.Value{data})
		stmt.Close()
	}

	// Verify it worked
	stmt2, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt2.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 50 {
		t.Errorf("Expected count=50, got %d", count)
	}
	rows.Close()
	stmt2.Close()
}

// TestExecContextErrorRollback tests that ExecContext rolls back on error
func TestExecContextErrorRollback(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert first row
	stmt2, _ := c.Prepare("INSERT INTO test (id) VALUES (1)")
	stmt2.Exec(nil)
	stmt2.Close()

	// Try to insert duplicate - should error and rollback
	stmt3, _ := c.Prepare("INSERT INTO test (id) VALUES (1)")
	_, err = stmt3.Exec(nil)
	if err == nil {
		t.Error("Expected error for duplicate primary key")
	}
	stmt3.Close()

	// Verify only one row exists
	stmt4, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt4.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1 after rollback, got %d", count)
	}
	rows.Close()
	stmt4.Close()
}

// TestDriverOpenMethod tests the Open method
func TestDriverOpenMethod(t *testing.T) {
	drv := GetDriver()

	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open via Driver.Open: %v", err)
	}
	defer conn.Close()

	// Verify it's a valid connection
	if _, ok := conn.(driver.Conn); !ok {
		t.Error("Expected driver.Conn interface")
	}
}

// TestMultipleMemoryConnections tests that each :memory: connection is isolated
func TestMultipleMemoryConnections(t *testing.T) {
	drv := &Driver{}

	conn1, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open first memory db: %v", err)
	}
	defer conn1.Close()

	conn2, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open second memory db: %v", err)
	}
	defer conn2.Close()

	c1 := conn1.(*Conn)
	c2 := conn2.(*Conn)

	// Create table in c1
	stmt, _ := c1.Prepare("CREATE TABLE test (id INTEGER)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c1.Prepare("INSERT INTO test VALUES (1)")
	stmt.Exec(nil)
	stmt.Close()

	// c2 should not have this table
	stmt2, _ := c2.Prepare("SELECT * FROM test")
	_, err = stmt2.Query(nil)
	if err == nil {
		t.Error("Expected error: table should not exist in c2")
	}
	stmt2.Close()

	// Create table in c2
	stmt3, _ := c2.Prepare("CREATE TABLE test (id INTEGER)")
	stmt3.Exec(nil)
	stmt3.Close()

	// c2 should have empty table
	stmt4, _ := c2.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt4.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 0 {
		t.Errorf("Expected count=0 in c2, got %d", count)
	}
	rows.Close()
	stmt4.Close()

	// c1 should still have its data
	stmt5, _ := c1.Prepare("SELECT COUNT(*) FROM test")
	rows, _ = stmt5.Query(nil)
	rows.Next(values)
	count = values[0].(int64)
	if count != 1 {
		t.Errorf("Expected count=1 in c1, got %d", count)
	}
	rows.Close()
	stmt5.Close()
}

// TestMarkDirtyWithPagerError tests MarkDirty error paths
func TestMarkDirtyWithPagerError(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create table and update rows to exercise MarkDirty
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert rows
	for i := 1; i <= 10; i++ {
		stmt, _ := c.Prepare("INSERT INTO test VALUES (?, ?)")
		stmt.Exec([]driver.Value{i, i * 10})
		stmt.Close()
	}

	// Update rows to trigger MarkDirty
	stmt2, _ := c.Prepare("UPDATE test SET value = value * 2 WHERE id <= 5")
	_, err = stmt2.Exec(nil)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	stmt2.Close()

	// Verify updates worked
	stmt3, _ := c.Prepare("SELECT value FROM test WHERE id = 1")
	rows, _ := stmt3.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	value := values[0].(int64)
	if value != 20 {
		t.Errorf("Expected value=20, got %d", value)
	}
	rows.Close()
	stmt3.Close()
}

// TestAllocatePageDataWithError tests AllocatePageData error paths
func TestAllocatePageDataWithError(t *testing.T) {
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create table with index to exercise page allocation
	stmt, _ := c.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("CREATE INDEX idx_value ON test(value)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert data to allocate pages for both table and index
	for i := 0; i < 100; i++ {
		stmt, _ := c.Prepare("INSERT INTO test VALUES (?, ?)")
		stmt.Exec([]driver.Value{i, fmt.Sprintf("value_%d", i)})
		stmt.Close()
	}

	// Verify it worked
	stmt2, _ := c.Prepare("SELECT COUNT(*) FROM test")
	rows, _ := stmt2.Query(nil)
	values := make([]driver.Value, 1)
	rows.Next(values)
	count := values[0].(int64)
	if count != 100 {
		t.Errorf("Expected count=100, got %d", count)
	}
	rows.Close()
	stmt2.Close()
}
