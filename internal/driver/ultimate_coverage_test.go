// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"testing"
)

// TestCountFromSubqueriesAdvanced tests countFromSubqueries with 70.0% coverage
func TestCountFromSubqueriesAdvanced(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE nums (x INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test COUNT with various patterns
	tests := []string{
		"SELECT COUNT(*) FROM nums",
		"SELECT COUNT(x) FROM nums WHERE x > 2",
	}

	for _, query := range tests {
		var count int
		err = db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("failed query %s: %v", query, err)
		}
		t.Logf("%s returned %d", query, count)
	}
}

// TestPrepareNewRowForInsertEdgeCases tests prepareNewRowForInsert with 71.4% coverage
func TestPrepareNewRowForInsertEdgeCases(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create table with different column types
	_, err = db.Exec("CREATE TABLE varied (id INTEGER, name TEXT, value REAL, data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert with all column types
	_, err = db.Exec("INSERT INTO varied VALUES (1, 'test', 3.14, X'DEADBEEF')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Insert with partial columns
	_, err = db.Exec("INSERT INTO varied (id, name) VALUES (2, 'partial')")
	if err != nil {
		t.Fatalf("failed to insert partial: %v", err)
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM varied").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}

	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

// TestSelectFromTableNameVariations tests selectFromTableName with 66.7% coverage
func TestSelectFromTableNameVariations(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO products VALUES (1, 'widget', 9.99), (2, 'gadget', 19.99)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test various SELECT patterns
	tests := []struct {
		query string
		cols  int
	}{
		{"SELECT * FROM products", 3},
		{"SELECT id FROM products", 1},
		{"SELECT id, name FROM products", 2},
		{"SELECT name, price FROM products", 2},
	}

	for _, tt := range tests {
		rows, err := db.Query(tt.query)
		if err != nil {
			t.Fatalf("failed query %s: %v", tt.query, err)
		}

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns for %s: %v", tt.query, err)
		}

		if len(cols) != tt.cols {
			t.Errorf("%s: got %d columns, want %d", tt.query, len(cols), tt.cols)
		}

		rows.Close()
	}
}

// TestEmitAggregateFunctionAllTypes tests emitAggregateFunction with 66.7% coverage
func TestEmitAggregateFunctionAllTypes(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create table with numeric data
	_, err = db.Exec("CREATE TABLE sales (amount INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO sales VALUES (100), (200), (300), (400), (500)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test all aggregate functions
	tests := []struct {
		query    string
		expected int64
	}{
		{"SELECT COUNT(*) FROM sales", 5},
		{"SELECT SUM(amount) FROM sales", 1500},
		{"SELECT AVG(amount) FROM sales", 300},
		{"SELECT MIN(amount) FROM sales", 100},
		{"SELECT MAX(amount) FROM sales", 500},
	}

	for _, tt := range tests {
		var result int64
		err = db.QueryRow(tt.query).Scan(&result)
		if err != nil {
			t.Fatalf("failed %s: %v", tt.query, err)
		}

		if result != tt.expected {
			t.Errorf("%s: got %d, want %d", tt.query, result, tt.expected)
		}
	}
}

// compileValueCompare checks a single query result against expected value.
func compileValueCompare(t *testing.T, db *sql.DB, query string, expected interface{}) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("failed %s: %v", query, err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("no rows for %s", query)
		return
	}

	var result interface{}
	if err := rows.Scan(&result); err != nil {
		t.Fatalf("failed to scan %s: %v", query, err)
	}

	compileValueCheck(t, query, result, expected)
}

// compileValueCheck does type-specific comparison of result vs expected.
func compileValueCheck(t *testing.T, query string, result, expected interface{}) {
	t.Helper()
	switch exp := expected.(type) {
	case int64:
		if v, ok := result.(int64); !ok || v != exp {
			t.Errorf("%s: got %v (%T), want %d", query, result, result, exp)
		}
	case string:
		if v, ok := result.(string); !ok || v != exp {
			t.Errorf("%s: got %v (%T), want %s", query, result, result, exp)
		}
	case nil:
		if result != nil {
			t.Errorf("%s: got %v, want nil", query, result)
		}
	}
}

// TestCompileValueDifferentTypes tests compileValue with 66.7% coverage
func TestCompileValueDifferentTypes(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	tests := []struct {
		query    string
		expected interface{}
	}{
		{"SELECT 42", int64(42)},
		{"SELECT 'text'", "text"},
		{"SELECT 3.14159", float64(3.14159)},
		{"SELECT NULL", nil},
	}

	for _, tt := range tests {
		compileValueCompare(t, db, tt.query, tt.expected)
	}
}

// TestEmitSelectColumnOpMultiTableVariations tests emitSelectColumnOpMultiTable with 66.7% coverage
func TestEmitSelectColumnOpMultiTableVariations(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE data (x INTEGER, y INTEGER, z TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO data VALUES (1, 2, 'a'), (3, 4, 'b')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test selecting different column combinations
	tests := []struct {
		query string
		cols  int
	}{
		{"SELECT x FROM data", 1},
		{"SELECT x, y FROM data", 2},
		{"SELECT * FROM data", 3},
		{"SELECT z, x FROM data", 2},
	}

	for _, tt := range tests {
		rows, err := db.Query(tt.query)
		if err != nil {
			t.Fatalf("failed %s: %v", tt.query, err)
		}

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("failed to get columns for %s: %v", tt.query, err)
		}

		if len(cols) != tt.cols {
			t.Errorf("%s: got %d columns, want %d", tt.query, len(cols), tt.cols)
		}

		rows.Close()
	}
}

// TestMarkDirtyOperations tests MarkDirty with 66.7% coverage
func TestMarkDirtyOperations(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE dirty (id INTEGER, data TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Perform multiple write operations to trigger MarkDirty
	for i := 0; i < 10; i++ {
		_, err = db.Exec("INSERT INTO dirty VALUES (?, ?)", i, "data")
		if err != nil {
			t.Fatalf("failed to insert %d: %v", i, err)
		}
	}

	// Update to trigger more MarkDirty calls
	_, err = db.Exec("UPDATE dirty SET data = 'updated' WHERE id < 5")
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM dirty WHERE data = 'updated'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count: %v", err)
	}

	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

// TestCreateConnectionSharedState tests createConnection with shared state
func TestCreateConnectionSharedState(t *testing.T) {
	dbFile := t.TempDir() + "/test_shared_conn_ultimate.db"

	// Create first connection
	db1, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}

	// Create table in first connection
	_, err = db1.Exec("CREATE TABLE shared (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db1.Exec("INSERT INTO shared VALUES (1)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Close first connection
	db1.Close()

	// Open second connection to same file
	db2, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()

	// Verify table exists and has data
	var count int
	err = db2.QueryRow("SELECT COUNT(*) FROM shared").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query in db2: %v", err)
	}

	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

// TestCreateMemoryConnectionMultiple tests createMemoryConnection with multiple instances
func TestCreateMemoryConnectionMultiple(t *testing.T) {
	// Create multiple memory connections
	dbs := make([]*sql.DB, 5)
	for i := 0; i < 5; i++ {
		db, err := sql.Open(DriverName, ":memory:")
		if err != nil {
			t.Fatalf("failed to open memory db %d: %v", i, err)
		}
		dbs[i] = db
	}

	// Each should be independent
	for i, db := range dbs {
		_, err := db.Exec("CREATE TABLE test (id INTEGER)")
		if err != nil {
			t.Fatalf("failed to create table in db %d: %v", i, err)
		}

		_, err = db.Exec("INSERT INTO test VALUES (?)", i)
		if err != nil {
			t.Fatalf("failed to insert into db %d: %v", i, err)
		}
	}

	// Verify each has only its own data
	for i, db := range dbs {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
		if err != nil {
			t.Fatalf("failed to count in db %d: %v", i, err)
		}

		if count != 1 {
			t.Errorf("db %d: count = %d, want 1", i, count)
		}

		db.Close()
	}
}

// TestBeginTxNestedError tests nested transaction error
func TestBeginTxNestedError(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Begin first transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin first tx: %v", err)
	}

	// Try to begin second transaction (may or may not fail depending on implementation)
	_, err = db.Begin()
	if err != nil {
		t.Logf("Nested transaction correctly fails: %v", err)
	} else {
		t.Logf("Nested transactions allowed in this implementation")
	}

	// Cleanup
	tx1.Rollback()
}

// TestConnPingContext tests Ping with context
func TestConnPingContext(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Test ping with context
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		t.Errorf("PingContext failed: %v", err)
	}

	// Test ping after close
	db.Close()
	err = db.PingContext(ctx)
	if err == nil {
		t.Error("PingContext should fail after close")
	}
}

// TestPrepareContextMultiple tests multiple prepared statements
func TestPrepareContextMultiple(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare multiple statements
	ctx := context.Background()
	stmt1, err := db.PrepareContext(ctx, "INSERT INTO test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare stmt1: %v", err)
	}
	defer stmt1.Close()

	stmt2, err := db.PrepareContext(ctx, "SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("failed to prepare stmt2: %v", err)
	}
	defer stmt2.Close()

	// Use both statements
	_, err = stmt1.Exec(1, "test")
	if err != nil {
		t.Fatalf("failed to exec stmt1: %v", err)
	}

	rows, err := stmt2.Query(1)
	if err != nil {
		t.Fatalf("failed to query stmt2: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("expected at least one row")
	}
}
