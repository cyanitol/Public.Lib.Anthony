package engine

import (
	"io"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// TestExec tests the Exec method.
func TestExec(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
}

// TestBegin tests Begin transaction method.
func TestBegin(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if tx == nil {
		t.Fatal("Transaction is nil")
	}
	if tx.done {
		t.Error("Transaction should not be done")
	}

	// Try to begin another transaction (should fail)
	_, err = db.Begin()
	if err == nil {
		t.Error("Should not be able to begin transaction while one is active")
	}
}

// TestTxCommitTwice tests committing transaction twice.
func TestTxCommitTwice(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Mark transaction as done manually for this test
	tx.done = true

	// Try to commit (should fail)
	err = tx.Commit()
	if err == nil {
		t.Error("Should not be able to commit finished transaction")
	}
}

// TestTxRollbackTwice tests rolling back transaction twice.
func TestTxRollbackTwice(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Mark transaction as done manually for this test
	tx.done = true

	// Try to rollback (should fail)
	err = tx.Rollback()
	if err == nil {
		t.Error("Should not be able to rollback finished transaction")
	}
}

// TestTxExecute tests executing statements in a transaction.
func TestTxExecute(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to execute in transaction: %v", err)
	}

	// Mark transaction as done and try to execute
	tx.done = true
	_, err = tx.Execute("SELECT 1")
	if err == nil {
		t.Error("Should not be able to execute in finished transaction")
	}
}

// TestTxQuery tests querying in a transaction.
func TestTxQuery(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to query in transaction: %v", err)
	}
	defer rows.Close()

	// Mark transaction as done and try to query
	tx.done = true
	_, err = tx.Query("SELECT 1")
	if err == nil {
		t.Error("Should not be able to query in finished transaction")
	}
}

// TestTxExec tests Exec in a transaction.
func TestTxExec(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to exec in transaction: %v", err)
	}

	// Mark transaction as done and try to exec
	tx.done = true
	_, err = tx.Exec("SELECT 1")
	if err == nil {
		t.Error("Should not be able to exec in finished transaction")
	}
}

// TestPreparedStmtExecute tests PreparedStmt.Execute.
func TestPreparedStmtExecute(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.Execute()
	if err != nil {
		t.Fatalf("Failed to execute prepared statement: %v", err)
	}
	if result == nil {
		t.Fatal("Result is nil")
	}

	// Close and try to execute again
	stmt.Close()
	_, err = stmt.Execute()
	if err == nil {
		t.Error("Should not be able to execute closed statement")
	}
}

// TestPreparedStmtQuery tests PreparedStmt.Query.
func TestPreparedStmtQuery(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Failed to query prepared statement: %v", err)
	}
	defer rows.Close()

	// Close statement and try to query again
	stmt.Close()
	_, err = stmt.Query()
	if err == nil {
		t.Error("Should not be able to query closed statement")
	}
}

// TestPreparedStmtCloseTwice tests closing PreparedStmt twice.
func TestPreparedStmtCloseTwice(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("Failed to close statement: %v", err)
	}

	// Close again should be safe
	err = stmt.Close()
	if err != nil {
		t.Errorf("Closing statement twice should not error: %v", err)
	}
}

// TestPreparedStmtSQLText tests PreparedStmt.SQL.
func TestPreparedStmtSQLText(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	sql := "SELECT 1"
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	if stmt.SQL() != sql {
		t.Errorf("Expected SQL '%s', got '%s'", sql, stmt.SQL())
	}
}

// TestBindParams tests bindParams.
func TestBindParams(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Test binding parameters (currently a no-op)
	err = stmt.bindParams([]interface{}{1, "test", 3.14})
	if err != nil {
		t.Errorf("bindParams should not error: %v", err)
	}
}

// TestQueryRowScan tests QueryRow.Scan.
func TestQueryRowScan(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test successful scan
	var value int
	err = db.QueryRow("SELECT 1").Scan(&value)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to scan: %v", err)
	}

	// Test scan with error (non-existent table)
	err = db.QueryRow("SELECT * FROM nonexistent").Scan(&value)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

// TestGetSchema tests GetSchema method.
func TestGetSchema(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	schema := db.GetSchema()
	if schema == nil {
		t.Fatal("Schema is nil")
	}
}

// TestGetPager tests GetPager method.
func TestGetPager(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	pager := db.GetPager()
	if pager == nil {
		t.Fatal("Pager is nil")
	}
}

// TestGetBtree tests GetBtree method.
func TestGetBtree(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	btree := db.GetBtree()
	if btree == nil {
		t.Fatal("Btree is nil")
	}
}

// TestLoadSchema tests loadSchema method.
func TestLoadSchema(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.loadSchema()
	if err != nil {
		t.Errorf("loadSchema should not error: %v", err)
	}
}

// TestMemToInterface tests memToInterface conversion.
func TestMemToInterface(t *testing.T) {
	tests := []struct {
		name     string
		mem      *vdbe.Mem
		expected interface{}
	}{
		{
			name:     "nil mem",
			mem:      nil,
			expected: nil,
		},
		{
			name:     "null mem",
			mem:      vdbe.NewMem(),
			expected: nil,
		},
		{
			name:     "int mem",
			mem:      func() *vdbe.Mem { m := vdbe.NewMem(); m.SetInt(42); return m }(),
			expected: int64(42),
		},
		{
			name:     "real mem",
			mem:      func() *vdbe.Mem { m := vdbe.NewMem(); m.SetReal(3.14); return m }(),
			expected: 3.14,
		},
		{
			name:     "string mem",
			mem:      func() *vdbe.Mem { m := vdbe.NewMem(); m.SetStr("hello"); return m }(),
			expected: "hello",
		},
		{
			name:     "blob mem",
			mem:      func() *vdbe.Mem { m := vdbe.NewMem(); m.SetBlob([]byte{1, 2, 3}); return m }(),
			expected: []byte{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := memToInterface(tt.mem)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("Expected nil, got %v", result)
				}
				return
			}

			// For byte slices, compare differently
			if expectedBytes, ok := tt.expected.([]byte); ok {
				resultBytes, ok := result.([]byte)
				if !ok {
					t.Errorf("Expected byte slice, got %T", result)
					return
				}
				if len(expectedBytes) != len(resultBytes) {
					t.Errorf("Expected %v, got %v", expectedBytes, resultBytes)
					return
				}
				for i := range expectedBytes {
					if expectedBytes[i] != resultBytes[i] {
						t.Errorf("Expected %v, got %v", expectedBytes, resultBytes)
						return
					}
				}
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestScanIntoUnsupportedDestType tests scanning into unsupported destination type.
func TestScanIntoUnsupportedDestType(t *testing.T) {
	mem := vdbe.NewMem()
	mem.SetInt(42)

	// Unsupported type
	var dest struct{}
	err := scanInto(mem, &dest)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

// TestScanIntoNil tests scanning from nil mem.
func TestScanIntoNil(t *testing.T) {
	var dest int
	err := scanInto(nil, &dest)
	if err == nil {
		t.Error("Expected error for nil mem")
	}
}

// TestRowsNextError tests Rows.Next with an error.
func TestRowsNextError(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Test Next returns false when no rows
	if rows.Next() {
		t.Error("Expected no rows")
	}

	// Test calling Next again after done
	if rows.Next() {
		t.Error("Expected no rows after done")
	}
}

// TestRowsScanNoCurrentRow tests Rows.Scan with no current row.
func TestRowsScanNoCurrentRow(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	var value int
	err = rows.Scan(&value)
	if err == nil {
		t.Error("Expected error for scanning without current row")
	}
}

// TestRowsScanWrongNumberOfDest tests Rows.Scan with wrong number of destinations.
func TestRowsScanWrongNumberOfDest(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected row")
	}

	// Try to scan into wrong number of destinations
	var v1, v2 int
	err = rows.Scan(&v1, &v2)
	if err == nil {
		t.Error("Expected error for wrong number of destinations")
	}
}

// TestRowsCloseTwice tests closing Rows twice.
func TestRowsCloseTwice(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Failed to close rows: %v", err)
	}

	// Close again should be safe
	err = rows.Close()
	if err != nil {
		t.Errorf("Closing rows twice should not error: %v", err)
	}
}

// TestCloseWithTransaction tests closing database with active transaction.
func TestCloseWithTransaction(t *testing.T) {
	t.Skip("Transaction management needs pager-level transaction support")
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Start transaction flag (doesn't actually start pager transaction)
	_, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Close should succeed (no actual pager transaction to rollback)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

// TestOpenExistingDatabase tests opening an existing database.
func TestOpenExistingDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Create database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	db.Close()

	// Reopen database
	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()
}

// TestQueryNonSelect tests Query with non-SELECT statement.
func TestQueryNonSelect(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Query("CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("Expected error for non-SELECT statement")
	}
}

// TestPrepareNoStatements tests Prepare with no statements.
func TestPrepareNoStatements(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Prepare("")
	if err == nil {
		t.Error("Expected error for empty SQL")
	}
}

// TestExecuteNoStatements tests Execute with no statements.
func TestExecuteNoStatements(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	result, err := db.Execute("")
	if err != nil {
		t.Fatalf("Execute with empty SQL should not error: %v", err)
	}
	if result.RowCount() != 0 {
		t.Error("Expected empty result")
	}
}

// TestQueryNoStatements tests Query with no statements.
func TestQueryNoStatements(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Query("")
	if err == nil {
		t.Error("Expected error for empty SQL")
	}
}
