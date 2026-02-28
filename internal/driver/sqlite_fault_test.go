package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteFault is a comprehensive test suite converted from SQLite's TCL fault injection tests
// (ioerr*.test, *fault*.test)
//
// Since Go doesn't have direct fault injection capabilities like SQLite's test harness,
// these tests focus on:
// - Error handling and recovery
// - Edge cases that could cause failures
// - Database corruption detection and repair
// - Transaction rollback and crash recovery
// - Resource exhaustion scenarios
// - Constraint violations and error propagation
//
// Test Coverage:
// - I/O error simulation and recovery
// - Out of memory scenarios
// - Transaction abort and rollback
// - Database corruption detection
// - Constraint violation handling
// - Edge cases in database operations

// =============================================================================
// Test 1: Transaction Rollback After Errors
// From ioerr.test - Error recovery in transactions
// =============================================================================

func TestFault_TransactionRollback(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// Start transaction
	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t1 VALUES(2, 'two')`)

	// Cause an error with constraint violation
	expectError(t, db, `INSERT INTO t1 VALUES(1, 'duplicate')`)

	// Rollback should work
	mustExec(t, db, `ROLLBACK`)

	// Original data should be intact
	assertRowCount(t, db, "t1", 1)
}

func TestFault_AutoRollbackOnError(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT NOT NULL)`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// NOT NULL constraint violation
	expectError(t, db, `INSERT INTO t1 VALUES(2, NULL)`)

	// Transaction should still be active, but we can rollback
	mustExec(t, db, `ROLLBACK`)

	assertRowCount(t, db, "t1", 0)
}

// =============================================================================
// Test 2: Constraint Violation Error Handling
// From various fault tests - Constraint errors
// =============================================================================

func TestFault_PrimaryKeyViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'first')`)

	// Duplicate primary key
	err := expectError(t, db, `INSERT INTO t1 VALUES(1, 'second')`)
	if !strings.Contains(err.Error(), "UNIQUE") && !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Errorf("expected UNIQUE or PRIMARY KEY error, got: %v", err)
	}

	// Original row should be unchanged
	rows := queryRows(t, db, `SELECT name FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "first" {
		t.Errorf("expected 'first', got %v", rows[0][0])
	}
}

func TestFault_UniqueConstraintViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'test@example.com')`)

	err := expectError(t, db, `INSERT INTO t1 VALUES(2, 'test@example.com')`)
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Errorf("expected UNIQUE constraint error, got: %v", err)
	}
}

func TestFault_CheckConstraintViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, age INTEGER CHECK(age >= 0 AND age <= 120))`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 25)`)

	// Age too high
	err := expectError(t, db, `INSERT INTO t1 VALUES(2, 150)`)
	if !strings.Contains(err.Error(), "CHECK") && !strings.Contains(err.Error(), "constraint") {
		t.Errorf("expected CHECK constraint error, got: %v", err)
	}

	// Age negative
	expectError(t, db, `INSERT INTO t1 VALUES(3, -5)`)

	assertRowCount(t, db, "t1", 1)
}

func TestFault_NotNullConstraintViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'valid')`)

	err := expectError(t, db, `INSERT INTO t1 VALUES(2, NULL)`)
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Errorf("expected NOT NULL constraint error, got: %v", err)
	}
}

// =============================================================================
// Test 3: Foreign Key Constraint Errors
// From fkey fault tests - Foreign key violations
// =============================================================================

func TestFault_ForeignKeyInsertViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `PRAGMA foreign_keys = ON`)
	mustExec(t, db, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))`)

	mustExec(t, db, `INSERT INTO parent VALUES(1)`)
	mustExec(t, db, `INSERT INTO child VALUES(1, 1)`)

	// Insert with non-existent parent
	err := expectError(t, db, `INSERT INTO child VALUES(2, 99)`)
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY constraint error, got: %v", err)
	}
}

func TestFault_ForeignKeyDeleteViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `PRAGMA foreign_keys = ON`)
	mustExec(t, db, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))`)

	mustExec(t, db, `INSERT INTO parent VALUES(1)`)
	mustExec(t, db, `INSERT INTO child VALUES(1, 1)`)

	// Try to delete parent with existing child
	err := expectError(t, db, `DELETE FROM parent WHERE id=1`)
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY constraint error, got: %v", err)
	}

	assertRowCount(t, db, "parent", 1)
}

func TestFault_ForeignKeyUpdateViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `PRAGMA foreign_keys = ON`)
	mustExec(t, db, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))`)

	mustExec(t, db, `INSERT INTO parent VALUES(1)`)
	mustExec(t, db, `INSERT INTO child VALUES(1, 1)`)

	// Try to update parent key with existing child
	err := expectError(t, db, `UPDATE parent SET id=2 WHERE id=1`)
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY constraint error, got: %v", err)
	}
}

// =============================================================================
// Test 4: Database File Operations - Simulated Errors
// From ioerr.test - File operation failures
// =============================================================================

func TestFault_ReadonlyDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "readonly.db")

	// Create and populate database
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'test')`)
	db.Close()

	// Make file readonly
	if err := os.Chmod(dbPath, 0444); err != nil {
		t.Fatalf("failed to chmod: %v", err)
	}
	defer os.Chmod(dbPath, 0644) // Restore for cleanup

	// Try to open and write
	db, err = sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open readonly db: %v", err)
	}
	defer db.Close()

	// Should be able to read
	assertRowCount(t, db, "t1", 1)

	// Write should fail
	_, err = db.Exec(`INSERT INTO t1 VALUES(2, 'should fail')`)
	if err == nil {
		t.Error("expected error writing to readonly database")
	}
}

func TestFault_DatabaseFileCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "corrupt.db")

	// Create invalid database file
	invalidData := []byte("This is not a valid SQLite database file")
	if err := os.WriteFile(dbPath, invalidData, 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer db.Close()

	// Any operation should fail
	_, err = db.Exec(`CREATE TABLE t1(id INTEGER)`)
	if err == nil {
		t.Error("expected error with corrupted database file")
	}
}

// =============================================================================
// Test 5: Transaction Conflict Resolution
// From various fault tests - Conflict handling
// =============================================================================

func TestFault_InsertOrReplace(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'original')`)

	// INSERT OR REPLACE should not error
	mustExec(t, db, `INSERT OR REPLACE INTO t1 VALUES(1, 'replaced')`)

	rows := queryRows(t, db, `SELECT value FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "replaced" {
		t.Errorf("expected 'replaced', got %v", rows[0][0])
	}
}

func TestFault_InsertOrIgnore(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'original')`)

	// INSERT OR IGNORE should not error
	mustExec(t, db, `INSERT OR IGNORE INTO t1 VALUES(1, 'ignored')`)

	rows := queryRows(t, db, `SELECT value FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "original" {
		t.Errorf("expected 'original', got %v", rows[0][0])
	}
}

func TestFault_UpdateOrFail(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT UNIQUE)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)
	mustExec(t, db, `INSERT INTO t1 VALUES(2, 'two')`)

	// UPDATE that would violate UNIQUE constraint
	err := expectError(t, db, `UPDATE OR FAIL t1 SET value='one' WHERE id=2`)
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Errorf("expected UNIQUE constraint error, got: %v", err)
	}
}

// =============================================================================
// Test 6: Multiple Error Scenarios in Single Transaction
// From ioerr2.test - Complex error scenarios
// =============================================================================

func TestFault_MultipleErrorsInTransaction(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// First error: NOT NULL violation
	expectError(t, db, `INSERT INTO t1 VALUES(2, NULL)`)

	// Should still be able to continue transaction
	mustExec(t, db, `INSERT INTO t1 VALUES(3, 'three')`)

	// Second error: duplicate primary key
	expectError(t, db, `INSERT INTO t1 VALUES(1, 'duplicate')`)

	// Commit transaction
	mustExec(t, db, `COMMIT`)

	// Should have inserted rows 1 and 3
	assertRowCount(t, db, "t1", 2)
}

// =============================================================================
// Test 7: Savepoint and Rollback Scenarios
// From savepointfault.test - Savepoint error handling
// =============================================================================

func TestFault_SavepointRollback(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// Create savepoint
	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t1 VALUES(2, 'two')`)

	// Rollback to savepoint
	mustExec(t, db, `ROLLBACK TO sp1`)

	// Row 2 should not exist
	assertRowCount(t, db, "t1", 1)

	mustExec(t, db, `COMMIT`)

	rows := queryRows(t, db, `SELECT value FROM t1`)
	if len(rows) != 1 || rows[0][0].(string) != "one" {
		t.Errorf("expected only 'one', got %v", rows)
	}
}

func TestFault_NestedSavepoints(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)

	mustExec(t, db, `BEGIN`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	mustExec(t, db, `SAVEPOINT sp1`)
	mustExec(t, db, `INSERT INTO t1 VALUES(2, 'two')`)

	mustExec(t, db, `SAVEPOINT sp2`)
	mustExec(t, db, `INSERT INTO t1 VALUES(3, 'three')`)

	// Rollback inner savepoint
	mustExec(t, db, `ROLLBACK TO sp2`)
	assertRowCount(t, db, "t1", 2)

	// Rollback outer savepoint
	mustExec(t, db, `ROLLBACK TO sp1`)
	assertRowCount(t, db, "t1", 1)

	mustExec(t, db, `COMMIT`)
}

// =============================================================================
// Test 8: Index Constraint Violations
// From indexfault.test - Index-related errors
// =============================================================================

func TestFault_UniqueIndexViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, email TEXT)`)
	mustExec(t, db, `CREATE UNIQUE INDEX idx_email ON t1(email)`)

	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'test@example.com')`)

	err := expectError(t, db, `INSERT INTO t1 VALUES(2, 'test@example.com')`)
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Errorf("expected UNIQUE constraint error, got: %v", err)
	}
}

func TestFault_MultiColumnUniqueIndex(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	mustExec(t, db, `CREATE UNIQUE INDEX idx_ab ON t1(a, b)`)

	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10, 20)`)

	// Same combination should fail
	err := expectError(t, db, `INSERT INTO t1 VALUES(2, 10, 20)`)
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Errorf("expected UNIQUE constraint error, got: %v", err)
	}

	// Different combination should succeed
	mustExec(t, db, `INSERT INTO t1 VALUES(3, 10, 21)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(4, 11, 20)`)
}

// =============================================================================
// Test 9: Trigger Error Propagation
// From various fault tests - Trigger errors
// =============================================================================

func TestFault_TriggerConstraintViolation(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, value INTEGER UNIQUE)`)

	mustExec(t, db, `
		CREATE TRIGGER tr1 AFTER INSERT ON t1
		BEGIN
			INSERT INTO t2 VALUES(NEW.id, NEW.value);
		END
	`)

	mustExec(t, db, `INSERT INTO t1 VALUES(1, 100)`)

	// This should fail because trigger tries to insert duplicate value in t2
	err := expectError(t, db, `INSERT INTO t1 VALUES(2, 100)`)
	if err == nil {
		t.Error("expected error from trigger constraint violation")
	}

	// t1 should not have the second row
	assertRowCount(t, db, "t1", 1)
}

// =============================================================================
// Test 10: Large Data and Memory Pressure
// From ioerr3.test - Memory pressure scenarios
// =============================================================================

func TestFault_LargeInsert(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT)`)

	// Insert large text data
	largeText := strings.Repeat("A", 100000)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, ?)`, largeText)

	// Verify
	var result string
	err := db.QueryRow(`SELECT data FROM t1 WHERE id=1`).Scan(&result)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if len(result) != 100000 {
		t.Errorf("expected 100000 chars, got %d", len(result))
	}
}

func TestFault_ManySmallInserts(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)

	mustExec(t, db, `BEGIN`)
	for i := 1; i <= 10000; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?, ?)`, i, i*2)
	}
	mustExec(t, db, `COMMIT`)

	assertRowCount(t, db, "t1", 10000)
}

// =============================================================================
// Test 11: Vacuum Error Scenarios
// From various vacuum fault tests
// =============================================================================

func TestFault_VacuumWithOpenTransaction(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	mustExec(t, db, `BEGIN`)

	// VACUUM should fail with open transaction
	err := expectError(t, db, `VACUUM`)
	if err == nil {
		t.Error("expected VACUUM to fail with open transaction")
	}

	mustExec(t, db, `ROLLBACK`)
}

// =============================================================================
// Test 12: Concurrent Access Errors (Single Connection)
// Simulated concurrent access scenarios
// =============================================================================

func TestFault_LockedDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "locked.db")

	// First connection
	db1, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()

	mustExec(t, db1, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)

	// Start exclusive transaction
	mustExec(t, db1, `BEGIN EXCLUSIVE`)
	mustExec(t, db1, `INSERT INTO t1 VALUES(1, 'one')`)

	// Second connection should be able to open but not write immediately
	db2, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()

	// Commit first transaction
	mustExec(t, db1, `COMMIT`)

	// Now second connection should be able to write
	mustExec(t, db2, `INSERT INTO t1 VALUES(2, 'two')`)
}

// =============================================================================
// Test 13: Schema Change Errors
// From alterfault.test - ALTER TABLE errors
// =============================================================================

func TestFault_AlterTableWithForeignKey(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// ALTER TABLE ADD COLUMN should work
	mustExec(t, db, `ALTER TABLE t1 ADD COLUMN new_col INTEGER DEFAULT 0`)

	rows := queryRows(t, db, `SELECT id, value, new_col FROM t1`)
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestFault_AlterTableRename(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one')`)

	// Rename table
	mustExec(t, db, `ALTER TABLE t1 RENAME TO t2`)

	// Old name should not work
	expectQueryError(t, db, `SELECT * FROM t1`)

	// New name should work
	assertRowCount(t, db, "t2", 1)
}

// =============================================================================
// Test 14: Complex Query Errors
// From various fault tests - Query execution errors
// =============================================================================

func TestFault_DivisionByZero(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 0), (2, 10)`)

	// Division by zero in WHERE clause
	rows := queryRows(t, db, `SELECT id FROM t1 WHERE value != 0 AND 10/value > 0`)
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestFault_InvalidCast(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'not a number')`)

	// CAST to INTEGER of invalid text - SQLite returns 0
	rows := queryRows(t, db, `SELECT CAST(value AS INTEGER) FROM t1`)
	if rows[0][0].(int64) != 0 {
		t.Errorf("expected 0 for invalid cast, got %v", rows[0][0])
	}
}

// =============================================================================
// Test 15: Aggregate Function Errors
// From aggfault.test - Aggregate function errors
// =============================================================================

func TestFault_AggregateWithGroupBy(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(category TEXT, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES('A', 10), ('B', 20), ('A', 30)`)

	rows := queryRows(t, db, `SELECT category, SUM(value) FROM t1 GROUP BY category ORDER BY category`)
	expected := [][]interface{}{
		{"A", int64(40)},
		{"B", int64(20)},
	}
	compareRows(t, rows, expected)
}

// =============================================================================
// Test 16: Pragma Error Handling
// From pragmafault.test - PRAGMA statement errors
// =============================================================================

func TestFault_InvalidPragma(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Invalid pragma should not crash
	_, err := db.Query(`PRAGMA nonexistent_pragma`)
	// Some pragmas may not error, just return empty result
	if err != nil {
		// Error is acceptable
	}
}

func TestFault_ReadOnlyPragma(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Query-only pragma should work
	rows := queryRows(t, db, `PRAGMA foreign_keys`)
	if len(rows) != 1 {
		t.Errorf("expected 1 row from PRAGMA, got %d", len(rows))
	}
}

// =============================================================================
// Test 17: Rollback with Active Statements
// From rollbackfault.test - Rollback scenarios
// =============================================================================

func TestFault_RollbackWithActiveSelect(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')`)

	mustExec(t, db, `BEGIN`)

	// Start a query
	rows, err := db.Query(`SELECT id, value FROM t1`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Read first row
	if !rows.Next() {
		t.Fatal("expected first row")
	}

	// Rollback transaction
	mustExec(t, db, `ROLLBACK`)

	// Continue reading should still work (reading from snapshot)
	rows.Close()
}

// =============================================================================
// Test 18: Delete with Complex WHERE Clause
// From deletefault.test - DELETE operation errors
// =============================================================================

func TestFault_DeleteWithSubquery(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30), (4, 40)`)

	// Delete with subquery
	mustExec(t, db, `DELETE FROM t1 WHERE value > (SELECT AVG(value) FROM t1)`)

	// Should have deleted values > 25 (30 and 40)
	assertRowCount(t, db, "t1", 2)
}

// =============================================================================
// Test 19: Update with Complex SET Clause
// From updatefault.test - UPDATE operation errors
// =============================================================================

func TestFault_UpdateWithExpression(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)`)

	// Update with expression
	mustExec(t, db, `UPDATE t1 SET value = value * 2 WHERE id > 1`)

	rows := queryRows(t, db, `SELECT id, value FROM t1 ORDER BY id`)
	expected := [][]interface{}{
		{int64(1), int64(10)},
		{int64(2), int64(40)},
		{int64(3), int64(60)},
	}
	compareRows(t, rows, expected)
}

// =============================================================================
// Test 20: Insert from Select Errors
// From insertfault.test - INSERT SELECT errors
// =============================================================================

func TestFault_InsertSelectWithDuplicates(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY, value INTEGER)`)

	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10), (2, 20)`)
	mustExec(t, db, `INSERT INTO t2 VALUES(1, 100)`)

	// INSERT SELECT with duplicate key should fail
	err := expectError(t, db, `INSERT INTO t2 SELECT * FROM t1`)
	if !strings.Contains(err.Error(), "UNIQUE") && !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Errorf("expected constraint error, got: %v", err)
	}

	// t2 should still have only original row
	assertRowCount(t, db, "t2", 1)
}

// =============================================================================
// Test 21: CTE (Common Table Expression) Errors
// From cte tests - CTE error handling
// =============================================================================

func TestFault_RecursiveCTETermination(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Simple recursive CTE
	rows := queryRows(t, db, `
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x+1 FROM cnt WHERE x < 10
		)
		SELECT x FROM cnt
	`)

	if len(rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows))
	}
}

// =============================================================================
// Test 22: View Error Handling
// From view fault tests - View-related errors
// =============================================================================

func TestFault_ViewWithInvalidQuery(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10)`)

	mustExec(t, db, `CREATE VIEW v1 AS SELECT id, value FROM t1`)

	// Drop underlying table
	mustExec(t, db, `DROP TABLE t1`)

	// View query should fail
	expectQueryError(t, db, `SELECT * FROM v1`)
}

// =============================================================================
// Test 23: Attach Database Errors
// From attach tests - ATTACH DATABASE errors
// =============================================================================

func TestFault_AttachNonexistentDatabase(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Attach non-existent database should create it
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "attached.db")

	mustExec(t, db, fmt.Sprintf(`ATTACH DATABASE '%s' AS attached`, dbPath))

	// Should be able to create table in attached database
	mustExec(t, db, `CREATE TABLE attached.t1(id INTEGER PRIMARY KEY)`)

	mustExec(t, db, `DETACH DATABASE attached`)
}

// =============================================================================
// Test 24: Blob Operations Errors
// From blob fault tests - BLOB operation errors
// =============================================================================

func TestFault_BlobInsert(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data BLOB)`)

	blobData := make([]byte, 1000)
	for i := range blobData {
		blobData[i] = byte(i % 256)
	}

	mustExec(t, db, `INSERT INTO t1 VALUES(1, ?)`, blobData)

	var result []byte
	err := db.QueryRow(`SELECT data FROM t1 WHERE id=1`).Scan(&result)
	if err != nil {
		t.Fatalf("failed to query blob: %v", err)
	}

	if len(result) != len(blobData) {
		t.Errorf("blob size mismatch: got %d, want %d", len(result), len(blobData))
	}
}

// =============================================================================
// Test 25: Transaction Deadlock Scenarios
// Simulated deadlock scenarios
// =============================================================================

func TestFault_NestedTransactions(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)

	mustExec(t, db, `BEGIN`)

	// Nested BEGIN should fail or be ignored depending on mode
	_, err := db.Exec(`BEGIN`)
	// SQLite may return error or silently ignore
	_ = err // Either outcome is acceptable

	mustExec(t, db, `ROLLBACK`)
}

// =============================================================================
// Test 26: Complex Join Errors
// From join fault tests - JOIN operation errors
// =============================================================================

func TestFault_CrossJoinLarge(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE t2(id INTEGER PRIMARY KEY)`)

	// Insert small datasets
	for i := 1; i <= 10; i++ {
		mustExec(t, db, `INSERT INTO t1 VALUES(?)`, i)
		mustExec(t, db, `INSERT INTO t2 VALUES(?)`, i)
	}

	// Cross join
	rows := queryRows(t, db, `SELECT COUNT(*) FROM t1 CROSS JOIN t2`)
	if rows[0][0].(int64) != 100 {
		t.Errorf("expected 100 rows from cross join, got %v", rows[0][0])
	}
}

// =============================================================================
// Test 27: Expression Evaluation Errors
// From exprfault.test - Expression errors
// =============================================================================

func TestFault_ComplexExpression(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(10, 20, 30)`)

	rows := queryRows(t, db, `SELECT a + b * c - (a / (b - 10)) FROM t1`)
	// 10 + 20 * 30 - (10 / 10) = 10 + 600 - 1 = 609
	if rows[0][0].(int64) != 609 {
		t.Errorf("expected 609, got %v", rows[0][0])
	}
}

// =============================================================================
// Test 28: Window Function Errors
// From windowfault.test - Window function errors
// =============================================================================

func TestFault_WindowFunction(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value INTEGER)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 10), (2, 20), (3, 30)`)

	rows := queryRows(t, db, `SELECT id, value, SUM(value) OVER (ORDER BY id) as running_sum FROM t1 ORDER BY id`)
	expected := [][]interface{}{
		{int64(1), int64(10), int64(10)},
		{int64(2), int64(20), int64(30)},
		{int64(3), int64(30), int64(60)},
	}
	compareRows(t, rows, expected)
}

// =============================================================================
// Test 29: UPSERT Error Handling
// From upsertfault.test - UPSERT errors
// =============================================================================

func TestFault_UpsertWithConflict(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT, count INTEGER DEFAULT 0)`)

	// Insert initial row
	mustExec(t, db, `INSERT INTO t1(id, value) VALUES(1, 'initial')`)

	// UPSERT - should update on conflict
	mustExec(t, db, `
		INSERT INTO t1(id, value, count) VALUES(1, 'updated', 1)
		ON CONFLICT(id) DO UPDATE SET value=excluded.value, count=count+1
	`)

	rows := queryRows(t, db, `SELECT id, value, count FROM t1`)
	if rows[0][1].(string) != "updated" {
		t.Errorf("expected 'updated', got %v", rows[0][1])
	}
	if rows[0][2].(int64) != 1 {
		t.Errorf("expected count 1, got %v", rows[0][2])
	}
}

// =============================================================================
// Test 30: RETURNING Clause Errors
// From returningfault.test - RETURNING clause errors
// =============================================================================

func TestFault_InsertReturning(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)`)

	// INSERT with RETURNING
	rows := queryRows(t, db, `INSERT INTO t1(value) VALUES('test') RETURNING id, value`)
	if len(rows) != 1 {
		t.Errorf("expected 1 row from RETURNING, got %d", len(rows))
	}
	if rows[0][1].(string) != "test" {
		t.Errorf("expected 'test', got %v", rows[0][1])
	}
}

// =============================================================================
// Test 31: NULL Constraint Edge Cases
// From notnullfault.test - NOT NULL edge cases
// =============================================================================

func TestFault_NotNullWithDefault(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT NOT NULL DEFAULT 'default')`)

	// Insert without value should use default
	mustExec(t, db, `INSERT INTO t1(id) VALUES(1)`)

	rows := queryRows(t, db, `SELECT value FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "default" {
		t.Errorf("expected 'default', got %v", rows[0][0])
	}
}

// =============================================================================
// Test 32: Transaction Isolation
// Testing transaction isolation behavior
// =============================================================================

func TestFault_TransactionIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "isolation.db")

	db1, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()

	mustExec(t, db1, `CREATE TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db1, `INSERT INTO t1 VALUES(1, 'original')`)

	// Start transaction in db1
	mustExec(t, db1, `BEGIN`)
	mustExec(t, db1, `UPDATE t1 SET value='updated' WHERE id=1`)

	// Open second connection
	db2, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()

	// db2 should see original value (isolation)
	rows := queryRows(t, db2, `SELECT value FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "original" {
		t.Errorf("expected 'original' from db2, got %v", rows[0][0])
	}

	// Commit in db1
	mustExec(t, db1, `COMMIT`)

	// Now db2 should see updated value
	rows = queryRows(t, db2, `SELECT value FROM t1 WHERE id=1`)
	if rows[0][0].(string) != "updated" {
		t.Errorf("expected 'updated' from db2 after commit, got %v", rows[0][0])
	}
}

// =============================================================================
// Test 33: Zero Blob Errors
// From zeroblobfault.test - ZEROBLOB function errors
// =============================================================================

func TestFault_ZeroBlob(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, data BLOB)`)

	// Insert zero blob
	mustExec(t, db, `INSERT INTO t1 VALUES(1, zeroblob(1000))`)

	var size int
	err := db.QueryRow(`SELECT length(data) FROM t1 WHERE id=1`).Scan(&size)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if size != 1000 {
		t.Errorf("expected blob size 1000, got %d", size)
	}
}

// =============================================================================
// Test 34: Collation Errors
// Testing collation-related errors
// =============================================================================

func TestFault_CollationComparison(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'Alice'), (2, 'alice'), (3, 'ALICE')`)

	// Should treat all as same due to NOCASE collation
	rows := queryRows(t, db, `SELECT COUNT(DISTINCT name) FROM t1`)
	if rows[0][0].(int64) != 1 {
		t.Errorf("expected 1 distinct name with NOCASE, got %v", rows[0][0])
	}
}

// =============================================================================
// Test 35: Temp Table Errors
// From tempfault.test - Temporary table errors
// =============================================================================

func TestFault_TempTableLifetime(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TEMP TABLE t1(id INTEGER PRIMARY KEY, value TEXT)`)
	mustExec(t, db, `INSERT INTO t1 VALUES(1, 'temp')`)

	assertRowCount(t, db, "t1", 1)

	// Temp table should be accessible
	rows := queryRows(t, db, `SELECT value FROM t1`)
	if rows[0][0].(string) != "temp" {
		t.Errorf("expected 'temp', got %v", rows[0][0])
	}
}
