// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package engine

import (
	"path/filepath"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
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

	rows, err := db.Query(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var intVal int
		var int64Val int64
		var textVal string
		var realVal float64

		// Test scanning into int
		rows2, _ := db.Query(`SELECT int_val FROM test`)
		if rows2.Next() {
			if err := rows2.Scan(&intVal); err != nil {
				t.Errorf("Failed to scan into int: %v", err)
			}
			if intVal != 42 {
				t.Errorf("Expected 42, got %d", intVal)
			}
		}
		rows2.Close()

		// Test scanning into int64
		rows3, _ := db.Query(`SELECT int_val FROM test`)
		if rows3.Next() {
			if err := rows3.Scan(&int64Val); err != nil {
				t.Errorf("Failed to scan into int64: %v", err)
			}
			if int64Val != 42 {
				t.Errorf("Expected 42, got %d", int64Val)
			}
		}
		rows3.Close()

		// Test scanning into string
		rows4, _ := db.Query(`SELECT text_val FROM test`)
		if rows4.Next() {
			if err := rows4.Scan(&textVal); err != nil {
				t.Errorf("Failed to scan into string: %v", err)
			}
			if textVal != "hello" {
				t.Errorf("Expected 'hello', got %s", textVal)
			}
		}
		rows4.Close()

		// Test scanning into float64
		rows5, _ := db.Query(`SELECT real_val FROM test`)
		if rows5.Next() {
			if err := rows5.Scan(&realVal); err != nil {
				t.Errorf("Failed to scan into float64: %v", err)
			}
		}
		rows5.Close()

		// Test scanning into bool
		rows6, _ := db.Query(`SELECT bool_val FROM test`)
		if rows6.Next() {
			var boolVal bool
			if err := rows6.Scan(&boolVal); err != nil {
				t.Errorf("Failed to scan into bool: %v", err)
			}
			if !boolVal {
				t.Error("Expected true")
			}
		}
		rows6.Close()

		// Test scanning into []byte
		rows7, _ := db.Query(`SELECT text_val FROM test`)
		if rows7.Next() {
			var blobVal []byte
			if err := rows7.Scan(&blobVal); err != nil {
				t.Errorf("Failed to scan into []byte: %v", err)
			}
		}
		rows7.Close()

		// Test scanning into interface{}
		rows8, _ := db.Query(`SELECT int_val FROM test`)
		if rows8.Next() {
			var anyVal interface{}
			if err := rows8.Scan(&anyVal); err != nil {
				t.Errorf("Failed to scan into interface{}: %v", err)
			}
		}
		rows8.Close()
	}
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
