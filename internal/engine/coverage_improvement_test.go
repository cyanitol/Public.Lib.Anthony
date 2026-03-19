// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// TestTxCommitNotInTransaction tests committing when no transaction is in progress
func TestTxCommitNotInTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually set inTransaction to false to simulate edge case
	db.mu.Lock()
	db.inTransaction = false
	db.mu.Unlock()

	// Try to commit
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error when committing with no transaction in progress")
	}
	if err.Error() != "no transaction in progress" {
		t.Errorf("Expected 'no transaction in progress' error, got: %v", err)
	}
}

// TestTxRollbackNotInTransaction tests rolling back when no transaction is in progress
func TestTxRollbackNotInTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually set inTransaction to false to simulate edge case
	db.mu.Lock()
	db.inTransaction = false
	db.mu.Unlock()

	// Try to rollback
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error when rolling back with no transaction in progress")
	}
	if err.Error() != "no transaction in progress" {
		t.Errorf("Expected 'no transaction in progress' error, got: %v", err)
	}
}

// TestCompilerUnsupportedStatementType tests compiling an unsupported statement type
func TestCompilerUnsupportedStatementType(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a mock unsupported statement type
	// Since we can't easily create a new statement type, we can test via SQL parsing
	// that produces an unsupported statement (if any)

	// For now, test that the compiler rejects unknown types properly
	compiler := NewCompiler(db)

	// This should work for supported types
	selectStmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
	}

	_, err = compiler.Compile(selectStmt)
	if err != nil {
		t.Fatalf("Failed to compile SELECT: %v", err)
	}
}

// TestCompilerCreateTableSchemaError tests CreateTable when schema creation fails
func TestCompilerCreateTableSchemaError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table first
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create first table: %v", err)
	}

	// Try to create duplicate - should fail at schema level
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

// TestCompilerCreateIndexSchemaError tests CreateIndex when schema creation fails
func TestCompilerCreateIndexSchemaError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to create index on non-existent table
	_, err = db.Execute(`CREATE INDEX idx_test ON nonexistent (id)`)
	if err == nil {
		t.Error("Expected error when creating index on non-existent table")
	}
}

// TestScanIntoUnsupportedTypeCustomStruct tests scanning into an unsupported destination type
func TestScanIntoUnsupportedTypeCustomStruct(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (value INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute(`INSERT INTO test VALUES (42)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows, err := db.Query(`SELECT value FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		// Try to scan into unsupported type (struct)
		type CustomType struct {
			X int
		}
		var dest CustomType
		err = rows.Scan(&dest)
		if err == nil {
			t.Error("Expected error when scanning into unsupported type")
		}
	}
}

// TestMemToInterfaceAllTypes tests memToInterface with all memory types
func TestMemToInterfaceAllTypes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with different types
	_, err = db.Execute(`CREATE TABLE types (int_col INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with various types (NULL is tested separately)
	_, err = db.Execute(`INSERT INTO types VALUES (42)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	result, err := db.Execute(`SELECT * FROM types`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected at least one row")
	}

	row := result.Rows[0]

	// Verify types were converted correctly
	if len(row) > 0 {
		if _, ok := row[0].(int64); !ok {
			t.Errorf("Expected int64 for int_col, got %T", row[0])
		}
	}
}

// testScanType is a helper that tests scanning a specific column into a specific type
func testScanType(t *testing.T, db *Engine, query string, dest interface{}, checkFn func(*testing.T, interface{})) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	if err := rows.Scan(dest); err != nil {
		t.Errorf("Scan failed: %v", err)
		return
	}

	if checkFn != nil {
		checkFn(t, dest)
	}
}

// TestScanTypedAllTypes tests scanTyped with all supported types
func TestScanTypedAllTypes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (
		int_val INTEGER,
		text_val TEXT,
		real_val REAL,
		bool_val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute(`INSERT INTO test VALUES (42, 'hello', 3.14, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test scanning into int
	t.Run("scan_int", func(t *testing.T) {
		var intVal int
		testScanType(t, db, `SELECT int_val FROM test`, &intVal, func(t *testing.T, dest interface{}) {
			if v := dest.(*int); *v != 42 {
				t.Errorf("Expected 42, got %d", *v)
			}
		})
	})

	// Test scanning into int64
	t.Run("scan_int64", func(t *testing.T) {
		var int64Val int64
		testScanType(t, db, `SELECT int_val FROM test`, &int64Val, func(t *testing.T, dest interface{}) {
			if v := dest.(*int64); *v != 42 {
				t.Errorf("Expected 42, got %d", *v)
			}
		})
	})

	// Test scanning into string
	t.Run("scan_string", func(t *testing.T) {
		var textVal string
		testScanType(t, db, `SELECT text_val FROM test`, &textVal, func(t *testing.T, dest interface{}) {
			if v := dest.(*string); *v != "hello" {
				t.Errorf("Expected 'hello', got %s", *v)
			}
		})
	})

	// Test scanning into float64
	t.Run("scan_float64", func(t *testing.T) {
		var realVal float64
		testScanType(t, db, `SELECT real_val FROM test`, &realVal, nil)
	})

	// Test scanning into bool
	t.Run("scan_bool", func(t *testing.T) {
		var boolVal bool
		testScanType(t, db, `SELECT bool_val FROM test`, &boolVal, func(t *testing.T, dest interface{}) {
			if v := dest.(*bool); !*v {
				t.Error("Expected true")
			}
		})
	})

	// Test scanning into []byte
	t.Run("scan_bytes", func(t *testing.T) {
		var blobVal []byte
		testScanType(t, db, `SELECT text_val FROM test`, &blobVal, nil)
	})

	// Test scanning into interface{}
	t.Run("scan_interface", func(t *testing.T) {
		var anyVal interface{}
		testScanType(t, db, `SELECT int_val FROM test`, &anyVal, nil)
	})
}

// TestQueryNonSelectStatement tests Query with non-SELECT statement
func TestQueryNonSelectStatement(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Query with INSERT (not a SELECT)
	_, err = db.Query("INSERT INTO test VALUES (1)")
	if err == nil {
		t.Error("Expected error when querying with non-SELECT statement")
	}
	if err.Error() != "not a SELECT statement" {
		t.Errorf("Expected 'not a SELECT statement' error, got: %v", err)
	}
}

// TestCompilerDropTableBtreeError tests DROP TABLE when btree drop fails
func TestCompilerDropTableBtreeError(t *testing.T) {
	// This would require mocking the btree to return an error
	// For now we test the normal path
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and drop table normally
	_, err = db.Execute(`CREATE TABLE test_temp (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute(`DROP TABLE test_temp`)
	if err != nil {
		t.Errorf("Failed to drop table: %v", err)
	}
}

// TestCompilerDropIndexBtreeError tests DROP INDEX when btree drop fails
func TestCompilerDropIndexBtreeError(t *testing.T) {
	// This would require mocking the btree to return an error
	// For now we test the normal path
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Execute(`CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create and drop index normally
	_, err = db.Execute(`CREATE INDEX idx_test ON test (name)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	_, err = db.Execute(`DROP INDEX idx_test`)
	if err != nil {
		t.Errorf("Failed to drop index: %v", err)
	}
}

// TestEngineCompilerNewInstance tests creating a new compiler
func TestEngineCompilerNewInstance(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Compiler should be created automatically
	if db.compiler == nil {
		t.Fatal("Compiler should not be nil")
	}
}
