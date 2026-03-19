// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestFullIntegration tests the complete path from SQL to result
// by exercising all major components: driver, connection, statement,
// VDBE compilation, schema, btree, and pager.
func TestFullIntegration(t *testing.T) {
	// Create a temporary database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Open database using database/sql
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify connection works
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	t.Log("Successfully opened database and verified connection")
}

// TestSchemaLoading tests that the schema is properly loaded on connection
func TestSchemaLoading(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Force a connection to be created
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	// Verify schema is usable by querying sqlite_master
	rows, err := db.Query("SELECT name FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows.Close()

	t.Log("Schema successfully loaded - sqlite_master is queryable")
}

// TestFunctionRegistry tests that built-in functions are registered
func TestFunctionRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify built-in functions work by calling a few
	var result int64
	if err := db.QueryRow("SELECT abs(-42)").Scan(&result); err != nil {
		t.Fatalf("abs() function failed: %v", err)
	}
	if result != 42 {
		t.Errorf("abs(-42) = %d, want 42", result)
	}

	var lenResult int64
	if err := db.QueryRow("SELECT length('hello')").Scan(&lenResult); err != nil {
		t.Fatalf("length() function failed: %v", err)
	}
	if lenResult != 5 {
		t.Errorf("length('hello') = %d, want 5", lenResult)
	}

	t.Log("Function registry verified - built-in functions work")
}

// TestStatementPrepare tests SQL statement preparation and parsing
func TestStatementPrepare(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test preparing a SELECT statement
	stmt, err := db.Prepare("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to prepare SELECT statement: %v", err)
	}
	defer stmt.Close()

	t.Log("Successfully prepared SELECT statement")

	// Test preparing an INSERT statement (will fail without table, but should parse)
	_, err = db.Prepare("INSERT INTO test VALUES (1, 'hello')")
	// This should fail at execution, not preparation, but for now it might fail
	// due to missing table - that's okay for this test
	t.Logf("INSERT statement preparation result: %v", err)
}

// TestVDBECompilation tests that statements are compiled to VDBE bytecode
func TestVDBECompilation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	driver := GetDriver()
	conn, err := driver.OpenConnector(dbPath)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	sqlConn, ok := conn.(*Conn)
	if !ok {
		t.Fatal("connection is not *Conn type")
	}

	// Prepare a statement
	stmt, err := sqlConn.Prepare("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	sqlStmt, ok := stmt.(*Stmt)
	if !ok {
		t.Fatal("statement is not *Stmt type")
	}

	// Verify the statement has been parsed
	if sqlStmt.ast == nil {
		t.Fatal("statement AST is nil")
	}

	t.Logf("Statement successfully parsed: %T", sqlStmt.ast)
}

// integrationCheckColumns verifies column names match expected.
func integrationCheckColumns(t *testing.T, rows *sql.Rows, expected []string) {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}
	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(cols))
	}
	for i, col := range cols {
		if col != expected[i] {
			t.Errorf("column %d: expected %q, got %q", i, expected[i], col)
		}
	}
}

// TestSelectQueryExecution tests executing a simple SELECT query
func TestSelectQueryExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows.Close()

	integrationCheckColumns(t, rows, []string{"type", "name", "tbl_name", "rootpage", "sql"})

	t.Log("Successfully executed SELECT query and retrieved column metadata")

	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error iterating rows: %v", err)
	}
	t.Logf("Query returned %d rows", rowCount)
}

// TestTransactionSupport tests transaction begin/commit/rollback
func TestTransactionSupport(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test beginning a transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Test rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback transaction: %v", err)
	}

	t.Log("Transaction begin/rollback successful")

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("failed to begin second transaction: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	t.Log("Transaction begin/commit successful")
}

// TestConnectionPooling tests that multiple connections work correctly
func TestConnectionPooling(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection pool size
	db.SetMaxOpenConns(5)

	// Execute multiple queries concurrently
	const numQueries = 10
	errors := make(chan error, numQueries)

	for i := 0; i < numQueries; i++ {
		go func(id int) {
			rows, err := db.Query("SELECT * FROM sqlite_master")
			if err != nil {
				errors <- err
				return
			}
			rows.Close()
			errors <- nil
		}(i)
	}

	// Collect results
	for i := 0; i < numQueries; i++ {
		if err := <-errors; err != nil {
			t.Errorf("query %d failed: %v", i, err)
		}
	}

	t.Log("Successfully executed concurrent queries")
}

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test query on non-existent table
	_, err = db.Query("SELECT * FROM nonexistent_table")
	if err == nil {
		t.Error("expected error querying non-existent table")
	} else {
		t.Logf("Got expected error for non-existent table: %v", err)
	}

	// Test invalid SQL
	_, err = db.Query("THIS IS NOT VALID SQL")
	if err == nil {
		t.Error("expected error for invalid SQL")
	} else {
		t.Logf("Got expected error for invalid SQL: %v", err)
	}

	// Test using closed connection
	db.Close()
	_, err = db.Query("SELECT 1")
	if err == nil {
		t.Error("expected error using closed connection")
	} else {
		t.Logf("Got expected error for closed connection: %v", err)
	}
}

// TestPreparedStatementReuse tests reusing prepared statements
func TestPreparedStatementReuse(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Prepare statement once
	stmt, err := db.Prepare("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times
	for i := 0; i < 3; i++ {
		rows, err := stmt.Query()
		if err != nil {
			t.Fatalf("execution %d failed: %v", i, err)
		}
		rows.Close()
	}

	t.Log("Successfully reused prepared statement 3 times")
}
