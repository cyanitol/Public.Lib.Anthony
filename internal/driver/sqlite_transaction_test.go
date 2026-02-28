package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// setupTransactionTestDB creates a temporary test database for transaction tests
func setupTransactionTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db
}

// setupTransactionTestDBWithPath creates a test database at a specific path
func setupTransactionTestDBWithPath(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, path)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// execSQLStmts executes SQL statements and returns error if any
func execSQLStmts(db *sql.DB, stmts ...string) error {
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute %q: %w", stmt, err)
		}
	}
	return nil
}

// queryRowsWithError executes a query and returns results as a slice of interfaces
func queryRowsWithError(db *sql.DB, query string, args ...interface{}) ([][]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		results = append(results, values)
	}

	return results, rows.Err()
}

// =============================================================================
// Basic Transaction Tests (from trans.test)
// =============================================================================

// TestTransBasicBeginEnd tests basic BEGIN/END transaction syntax
// Converted from trans.test lines 66-76
func TestTransBasicBeginEnd(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Test BEGIN and END
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("END/COMMIT failed: %v", err)
	}
}

// TestTransBasicBeginCommit tests BEGIN TRANSACTION / COMMIT TRANSACTION
// Converted from trans.test lines 77-84
func TestTransBasicBeginCommit(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// SQLite allows BEGIN TRANSACTION
	if _, err := db.Exec("BEGIN TRANSACTION"); err != nil {
		t.Errorf("BEGIN TRANSACTION failed: %v", err)
	}

	if _, err := db.Exec("COMMIT TRANSACTION"); err != nil {
		t.Errorf("COMMIT TRANSACTION failed: %v", err)
	}
}

// TestTransBasicBeginRollback tests BEGIN / ROLLBACK TRANSACTION
// Converted from trans.test lines 85-92
func TestTransBasicBeginRollback(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Test ROLLBACK TRANSACTION with optional name
	if _, err := db.Exec("BEGIN TRANSACTION 'foo'"); err != nil {
		t.Errorf("BEGIN TRANSACTION 'foo' failed: %v", err)
	}

	if _, err := db.Exec("ROLLBACK TRANSACTION 'foo'"); err != nil {
		t.Errorf("ROLLBACK TRANSACTION 'foo' failed: %v", err)
	}
}

// TestTransBasicQuery tests queries within transaction
// Converted from trans.test lines 93-102
func TestTransBasicQuery(t *testing.T) {
	t.Skip("DDL and DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create tables
	if err := execSQLStmts(db,
		"CREATE TABLE one(a int PRIMARY KEY, b text)",
		"INSERT INTO one VALUES(1,'one')",
		"INSERT INTO one VALUES(2,'two')",
		"INSERT INTO one VALUES(3,'three')",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Execute transaction with queries
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	rows, err := queryRowsWithError(db, "SELECT a FROM one ORDER BY a")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("COMMIT failed: %v", err)
	}

	// Verify results: should have 3 rows
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}

// TestTransCommitWithoutBegin tests COMMIT without active transaction
// Converted from trans.test lines 199-210
func TestTransCommitWithoutBegin(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// COMMIT without BEGIN should fail
	_, err := db.Exec("COMMIT")
	if err == nil {
		t.Error("Expected error: cannot commit - no transaction is active")
	} else if !strings.Contains(err.Error(), "no transaction is active") {
		t.Errorf("Wrong error message: %v", err)
	}
}

// TestTransRollbackWithoutBegin tests ROLLBACK without active transaction
// Converted from trans.test lines 205-210
func TestTransRollbackWithoutBegin(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// ROLLBACK without BEGIN should fail
	_, err := db.Exec("ROLLBACK")
	if err == nil {
		t.Error("Expected error: cannot rollback - no transaction is active")
	} else if !strings.Contains(err.Error(), "no transaction is active") {
		t.Errorf("Wrong error message: %v", err)
	}
}

// TestTransNestedTransactionError tests that nested transactions are not allowed
// Converted from trans.test lines 224-243
func TestTransNestedTransactionError(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Start first transaction
	if _, err := db.Exec("BEGIN TRANSACTION"); err != nil {
		t.Fatalf("First BEGIN failed: %v", err)
	}

	// Try to start nested transaction - should fail
	_, err := db.Exec("BEGIN TRANSACTION")
	if err == nil {
		t.Error("Expected error: cannot start a transaction within a transaction")
	} else if !strings.Contains(err.Error(), "cannot start a transaction within a transaction") {
		t.Errorf("Wrong error message: %v", err)
	}

	// Clean up
	db.Exec("END TRANSACTION")
}

// =============================================================================
// Transaction Data Persistence Tests
// =============================================================================

// TestTransCommitPersistence tests that committed data persists
// Converted from trans.test lines 276-313
func TestTransCommitPersistence(t *testing.T) {
	t.Skip("DDL and DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Begin transaction
	if _, err := db.Exec("BEGIN TRANSACTION"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Create table
	if _, err := db.Exec("CREATE TABLE one(a text, b int)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	if _, err := db.Exec("INSERT INTO one(a,b) VALUES('hello', 1)"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Commit
	if _, err := db.Exec("COMMIT"); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify table exists after commit
	rows, err := queryRowsWithError(db, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) == 0 {
		t.Error("Table should exist after COMMIT")
	}
}

// TestTransRollbackDiscard tests that rolled back data is discarded
// Converted from trans.test lines 304-314
func TestTransRollbackDiscard(t *testing.T) {
	t.Skip("DDL and DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Begin transaction
	if _, err := db.Exec("BEGIN TRANSACTION"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Create table
	if _, err := db.Exec("CREATE TABLE one(a text, b int)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	if _, err := db.Exec("INSERT INTO one(a,b) VALUES('hello', 1)"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Rollback
	if _, err := db.Exec("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Verify table does not exist after rollback
	rows, err := queryRowsWithError(db, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) > 0 {
		t.Error("Table should not exist after ROLLBACK")
	}

	// Verify query on non-existent table fails
	_, err = db.Query("SELECT a,b FROM one ORDER BY b")
	if err == nil {
		t.Error("Expected error querying non-existent table")
	}
}

// TestTransSchemaChangesCommit tests CREATE/DROP with commit
// Converted from trans.test lines 318-369
func TestTransSchemaChangesCommit(t *testing.T) {
	t.Skip("DDL not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Transaction 1: Create table and index
	if err := execSQLStmts(db,
		"BEGIN TRANSACTION",
		"CREATE TABLE t1(a int, b int, c int)",
		"CREATE INDEX i1 ON t1(a)",
		"COMMIT",
	); err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify table and index exist
	rows, err := queryRowsWithError(db, "SELECT name FROM sqlite_master WHERE type='table' OR type='index' ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have i1 and t1
	if len(rows) != 2 {
		t.Errorf("Expected 2 schema objects, got %d", len(rows))
	}
}

// TestTransSchemaChangesRollback tests CREATE/DROP with rollback
// Converted from trans.test lines 350-369
func TestTransSchemaChangesRollback(t *testing.T) {
	t.Skip("DDL not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create initial state
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a int, b int, c int)",
		"CREATE INDEX i1 ON t1(a)",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Transaction: Create new objects and drop old ones, then rollback
	if err := execSQLStmts(db,
		"BEGIN TRANSACTION",
		"CREATE TABLE t2(a int, b int, c int)",
		"CREATE INDEX i2a ON t2(a)",
		"CREATE INDEX i2b ON t2(b)",
		"DROP TABLE t1",
	); err != nil {
		t.Fatalf("Transaction setup failed: %v", err)
	}

	// Rollback
	if _, err := db.Exec("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Verify original table and index remain
	rows, err := queryRowsWithError(db, "SELECT name FROM sqlite_master WHERE type='table' OR type='index' ORDER BY name")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have i1 and t1 (original objects)
	if len(rows) != 2 {
		t.Errorf("Expected 2 schema objects after rollback, got %d", len(rows))
	}
}

// =============================================================================
// Transaction Isolation Tests (from trans2.test)
// =============================================================================

// TestTransIsolationBasic tests basic transaction isolation
// Converted from trans2.test concept
func TestTransIsolationBasic(t *testing.T) {
	t.Skip("Multi-connection isolation not yet fully implemented")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Open two connections
	db1 := setupTransactionTestDBWithPath(t, dbPath)
	defer db1.Close()

	db2 := setupTransactionTestDBWithPath(t, dbPath)
	defer db2.Close()

	// Setup table in db1
	execSQL(t, db1,
		"CREATE TABLE t1(a int, b int)",
		"INSERT INTO t1 VALUES(1, 2)",
	)

	// Begin transaction in db1
	tx1, err := db1.Begin()
	if err != nil {
		t.Fatalf("BEGIN on db1 failed: %v", err)
	}

	// Insert in tx1
	if _, err := tx1.Exec("INSERT INTO t1 VALUES(3, 4)"); err != nil {
		t.Fatalf("INSERT in tx1 failed: %v", err)
	}

	// db2 should not see uncommitted changes
	rows := queryRows(t, db2, "SELECT COUNT(*) FROM t1")

	// Should only see original row
	if len(rows) > 0 {
		count := rows[0][0]
		if count != int64(1) {
			t.Errorf("Expected 1 row visible to db2, got %v", count)
		}
	}

	// Commit tx1
	if err := tx1.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Now db2 should see both rows
	rows = queryRows(t, db2, "SELECT COUNT(*) FROM t1")

	if len(rows) > 0 {
		count := rows[0][0]
		if count != int64(2) {
			t.Errorf("Expected 2 rows visible to db2 after commit, got %v", count)
		}
	}
}

// =============================================================================
// Transaction with Pending Statements (from trans3.test)
// =============================================================================

// TestTransCommitWithPendingStatement tests COMMIT while statement is active
// Converted from trans3.test lines 22-53
func TestTransCommitWithPendingStatement(t *testing.T) {
	t.Skip("Pending statement handling not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(x)",
		"INSERT INTO t1 VALUES(1)",
		"INSERT INTO t1 VALUES(2)",
		"INSERT INTO t1 VALUES(3)",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert data
	if _, err := tx.Exec("INSERT INTO t1 VALUES(4)"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query with pending cursor
	rows, err := tx.Query("SELECT * FROM t1 LIMIT 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Try to commit while cursor is open
	// This should succeed in SQLite (commit happens after cursor closes)
	commitErr := tx.Commit()

	rows.Close()

	if commitErr != nil {
		t.Errorf("COMMIT with pending statement failed: %v", commitErr)
	}
}

// TestTransRollbackWithPendingStatement tests ROLLBACK while statement is active
// Converted from trans3.test lines 54-73
func TestTransRollbackWithPendingStatement(t *testing.T) {
	t.Skip("Pending statement handling not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(x)",
		"INSERT INTO t1 VALUES(1)",
		"INSERT INTO t1 VALUES(2)",
		"INSERT INTO t1 VALUES(3)",
		"INSERT INTO t1 VALUES(4)",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Begin transaction and create table
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	if _, err := tx.Exec("CREATE TABLE xyzzy(abc)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	if _, err := tx.Exec("INSERT INTO t1 VALUES(5)"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query with pending cursor
	rows, err := tx.Query("SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Try to rollback while cursor is open
	rollbackErr := tx.Rollback()

	// Cursor should fail after rollback
	if rows.Next() {
		t.Error("Expected cursor to fail after ROLLBACK")
	}

	rows.Close()

	if rollbackErr != nil {
		t.Logf("ROLLBACK error (expected): %v", rollbackErr)
	}

	// Verify table does not exist (was rolled back)
	_, err = db.Query("SELECT * FROM xyzzy")
	if err == nil {
		t.Error("Table xyzzy should not exist after ROLLBACK")
	}
}

// =============================================================================
// SAVEPOINT Tests (from savepoint.test)
// =============================================================================

// TestSavepointBasicSyntax tests basic SAVEPOINT/RELEASE/ROLLBACK TO syntax
// Converted from savepoint.test lines 26-100
func TestSavepointBasicSyntax(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Test SAVEPOINT and RELEASE
	if _, err := db.Exec("SAVEPOINT sp1"); err != nil {
		t.Errorf("SAVEPOINT sp1 failed: %v", err)
	}

	if _, err := db.Exec("RELEASE sp1"); err != nil {
		t.Errorf("RELEASE sp1 failed: %v", err)
	}

	// Test SAVEPOINT and ROLLBACK TO
	if _, err := db.Exec("SAVEPOINT sp1"); err != nil {
		t.Errorf("SAVEPOINT sp1 failed: %v", err)
	}

	if _, err := db.Exec("ROLLBACK TO sp1"); err != nil {
		t.Errorf("ROLLBACK TO sp1 failed: %v", err)
	}

	// Clean up
	db.Exec("COMMIT")
}

// TestSavepointNested tests nested savepoints
// Converted from savepoint.test lines 44-90
func TestSavepointNested(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create nested savepoints
	if err := execSQLStmts(db,
		"SAVEPOINT sp1",
		"SAVEPOINT sp2",
	); err != nil {
		t.Fatalf("Failed to create nested savepoints: %v", err)
	}

	// Release outer savepoint (releases all)
	if _, err := db.Exec("RELEASE sp1"); err != nil {
		t.Errorf("RELEASE sp1 failed: %v", err)
	}

	// Should be in auto-commit mode now
	// Test with another set
	if err := execSQLStmts(db,
		"SAVEPOINT sp1",
		"SAVEPOINT sp2",
		"RELEASE sp2",
	); err != nil {
		t.Fatalf("Failed second nested savepoint test: %v", err)
	}

	// sp1 should still be active
	if _, err := db.Exec("RELEASE sp1"); err != nil {
		t.Errorf("RELEASE sp1 (second) failed: %v", err)
	}
}

// TestSavepointRollbackData tests ROLLBACK TO savepoint with data changes
// Converted from savepoint.test lines 107-183
func TestSavepointRollbackData(t *testing.T) {
	t.Skip("DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a, b, c)",
		"BEGIN",
		"INSERT INTO t1 VALUES(1, 2, 3)",
		"SAVEPOINT one",
		"UPDATE t1 SET a = 2, b = 3, c = 4",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Verify updated values
	rows, err := queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 1 || rows[0][0] != int64(2) {
		t.Error("Expected updated values")
	}

	// Rollback to savepoint
	if _, err := db.Exec("ROLLBACK TO one"); err != nil {
		t.Fatalf("ROLLBACK TO one failed: %v", err)
	}

	// Verify original values restored
	rows, err = queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 1 || rows[0][0] != int64(1) {
		t.Error("Expected original values after rollback")
	}

	// Clean up
	db.Exec("ROLLBACK")
}

// TestSavepointMultipleLevels tests multiple nested savepoint levels
// Converted from savepoint.test lines 137-162
func TestSavepointMultipleLevels(t *testing.T) {
	t.Skip("DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a, b, c)",
		"BEGIN",
		"INSERT INTO t1 VALUES(1, 2, 3)",
		"SAVEPOINT one",
		"INSERT INTO t1 VALUES(7, 8, 9)",
		"SAVEPOINT two",
		"INSERT INTO t1 VALUES(10, 11, 12)",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Should have 3 rows
	rows, err := queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Rollback to level two
	if _, err := db.Exec("ROLLBACK TO two"); err != nil {
		t.Fatalf("ROLLBACK TO two failed: %v", err)
	}

	// Should have 2 rows
	rows, err = queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows after ROLLBACK TO two, got %d", len(rows))
	}

	// Rollback to level one
	if _, err := db.Exec("ROLLBACK TO one"); err != nil {
		t.Fatalf("ROLLBACK TO one failed: %v", err)
	}

	// Should have 1 row
	rows, err = queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("Expected 1 row after ROLLBACK TO one, got %d", len(rows))
	}

	// Clean up
	db.Exec("ROLLBACK")
}

// TestSavepointRelease tests RELEASE savepoint
// Converted from savepoint.test lines 171-183
func TestSavepointRelease(t *testing.T) {
	t.Skip("DML not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a, b, c)",
		"BEGIN",
		"INSERT INTO t1 VALUES(1, 2, 3)",
		"SAVEPOINT one",
		"INSERT INTO t1 VALUES('a', 'b', 'c')",
		"SAVEPOINT two",
		"INSERT INTO t1 VALUES('d', 'e', 'f')",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Release savepoint two
	if _, err := db.Exec("RELEASE two"); err != nil {
		t.Fatalf("RELEASE two failed: %v", err)
	}

	// Data should still be there
	rows, err := queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows after RELEASE, got %d", len(rows))
	}

	// Rollback entire transaction
	if _, err := db.Exec("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// All data should be gone
	rows, err = queryRowsWithError(db, "SELECT * FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows after ROLLBACK, got %d", len(rows))
	}
}

// TestSavepointErrors tests error conditions for savepoints
// Converted from savepoint.test lines 300-333
func TestSavepointErrors(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create a savepoint
	if _, err := db.Exec("SAVEPOINT abc"); err != nil {
		t.Fatalf("SAVEPOINT abc failed: %v", err)
	}

	// Release it
	if _, err := db.Exec("RELEASE abc"); err != nil {
		t.Fatalf("RELEASE abc failed: %v", err)
	}

	// Try to release it again - should fail
	_, err := db.Exec("RELEASE abc")
	if err == nil {
		t.Error("Expected error: no such savepoint: abc")
	} else if !strings.Contains(err.Error(), "no such savepoint") {
		t.Errorf("Wrong error message: %v", err)
	}

	// Create savepoint abc
	if _, err := db.Exec("SAVEPOINT abc"); err != nil {
		t.Fatalf("SAVEPOINT abc failed: %v", err)
	}

	// Try to rollback to non-existent savepoint
	_, err = db.Exec("ROLLBACK TO def")
	if err == nil {
		t.Error("Expected error: no such savepoint: def")
	} else if !strings.Contains(err.Error(), "no such savepoint") {
		t.Errorf("Wrong error message: %v", err)
	}

	// Clean up
	db.Exec("RELEASE abc")
}

// TestSavepointCaseSensitivity tests case-insensitive savepoint names
// Converted from savepoint.test lines 553-560
func TestSavepointCaseSensitivity(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create savepoint with one case
	if _, err := db.Exec("SAVEPOINT \"save1\""); err != nil {
		t.Fatalf("SAVEPOINT failed: %v", err)
	}

	// Release with different case (should work - case insensitive)
	if _, err := db.Exec("RELEASE save1"); err != nil {
		t.Errorf("RELEASE save1 failed: %v", err)
	}
}

// TestSavepointWithWhitespace tests savepoint names with whitespace
// Converted from savepoint.test lines 557-560
func TestSavepointWithWhitespace(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create savepoint with whitespace in name
	if _, err := db.Exec("SAVEPOINT \"Including whitespace \""); err != nil {
		t.Fatalf("SAVEPOINT with whitespace failed: %v", err)
	}

	// Release with slightly different case/spacing
	if _, err := db.Exec("RELEASE \"including Whitespace \""); err != nil {
		t.Errorf("RELEASE failed: %v", err)
	}
}

// =============================================================================
// BEGIN IMMEDIATE and BEGIN EXCLUSIVE Tests
// =============================================================================

// TestTransBeginImmediate tests BEGIN IMMEDIATE
func TestTransBeginImmediate(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// BEGIN IMMEDIATE should succeed
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Errorf("BEGIN IMMEDIATE failed: %v", err)
	}

	// Clean up
	db.Exec("COMMIT")
}

// TestTransBeginExclusive tests BEGIN EXCLUSIVE
func TestTransBeginExclusive(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// BEGIN EXCLUSIVE should succeed
	if _, err := db.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Errorf("BEGIN EXCLUSIVE failed: %v", err)
	}

	// Clean up
	db.Exec("COMMIT")
}

// TestTransBeginDeferred tests BEGIN DEFERRED (default)
func TestTransBeginDeferred(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// BEGIN DEFERRED should succeed
	if _, err := db.Exec("BEGIN DEFERRED"); err != nil {
		t.Errorf("BEGIN DEFERRED failed: %v", err)
	}

	// Clean up
	db.Exec("COMMIT")
}

// =============================================================================
// Auto-commit Behavior Tests
// =============================================================================

// TestTransAutoCommitAfterCommit tests auto-commit is restored after COMMIT
func TestTransAutoCommitAfterCommit(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Should be able to begin another transaction (auto-commit restored)
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN after COMMIT failed: %v", err)
	}

	if err := tx2.Rollback(); err != nil {
		t.Errorf("ROLLBACK failed: %v", err)
	}
}

// TestTransAutoCommitAfterRollback tests auto-commit is restored after ROLLBACK
func TestTransAutoCommitAfterRollback(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Should be able to begin another transaction (auto-commit restored)
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN after ROLLBACK failed: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Errorf("COMMIT failed: %v", err)
	}
}

// TestTransNoAutoCommitDuringSavepoints tests auto-commit with savepoints
func TestTransNoAutoCommitDuringSavepoints(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Create nested savepoints
	if _, err := db.Exec("SAVEPOINT sp1"); err != nil {
		t.Fatalf("SAVEPOINT sp1 failed: %v", err)
	}

	if _, err := db.Exec("SAVEPOINT sp2"); err != nil {
		t.Fatalf("SAVEPOINT sp2 failed: %v", err)
	}

	// Release inner savepoint
	if _, err := db.Exec("RELEASE sp2"); err != nil {
		t.Fatalf("RELEASE sp2 failed: %v", err)
	}

	// sp1 still active - trying to BEGIN should fail
	_, err := db.Begin()
	if err == nil {
		t.Error("Expected error: BEGIN while savepoint active")
	}

	// Release outer savepoint
	if _, err := db.Exec("RELEASE sp1"); err != nil {
		t.Fatalf("RELEASE sp1 failed: %v", err)
	}

	// Now should be in auto-commit - BEGIN should succeed
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("BEGIN after releasing savepoints failed: %v", err)
	} else {
		tx.Rollback()
	}
}

// =============================================================================
// Error Recovery Tests
// =============================================================================

// TestTransErrorRecoveryCommit tests commit after statement error
func TestTransErrorRecoveryCommit(t *testing.T) {
	t.Skip("Error recovery not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a PRIMARY KEY, b)",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert valid data
	if _, err := tx.Exec("INSERT INTO t1 VALUES(1, 'one')"); err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Try to insert duplicate - should fail
	_, err = tx.Exec("INSERT INTO t1 VALUES(1, 'duplicate')")
	if err == nil {
		t.Error("Expected constraint error")
	}

	// Transaction should still be active and can commit
	if err := tx.Commit(); err != nil {
		t.Errorf("COMMIT after error failed: %v", err)
	}

	// Verify first insert persisted
	rows, err := queryRowsWithError(db, "SELECT COUNT(*) FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) > 0 && rows[0][0] != int64(1) {
		t.Error("Expected 1 row after commit")
	}
}

// TestTransErrorRecoveryRollback tests rollback after statement error
func TestTransErrorRecoveryRollback(t *testing.T) {
	t.Skip("Error recovery not yet fully implemented")
	db := setupTransactionTestDB(t)
	defer db.Close()

	// Setup
	if err := execSQLStmts(db,
		"CREATE TABLE t1(a PRIMARY KEY, b)",
		"INSERT INTO t1 VALUES(1, 'one')",
	); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert valid data
	if _, err := tx.Exec("INSERT INTO t1 VALUES(2, 'two')"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Try to insert duplicate - should fail
	_, err = tx.Exec("INSERT INTO t1 VALUES(1, 'duplicate')")
	if err == nil {
		t.Error("Expected constraint error")
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("ROLLBACK after error failed: %v", err)
	}

	// Verify only original data exists
	rows, err := queryRowsWithError(db, "SELECT COUNT(*) FROM t1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) > 0 && rows[0][0] != int64(1) {
		t.Error("Expected 1 row after rollback")
	}
}

// =============================================================================
// Driver-level Transaction Tests
// =============================================================================

// TestDriverTxBeginCommit tests driver-level Begin and Commit
func TestDriverTxBeginCommit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Verify transaction interface
	if _, ok := tx.(driver.Tx); !ok {
		t.Error("Transaction does not implement driver.Tx")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestDriverTxBeginRollback tests driver-level Begin and Rollback
func TestDriverTxBeginRollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}
}

// TestDriverTxReadOnly tests read-only transaction at driver level
func TestDriverTxReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin read-only transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	ourTx := tx.(*Tx)

	// Verify read-only
	if !ourTx.IsReadOnly() {
		t.Error("Transaction should be read-only")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestDriverTxReadWrite tests read-write transaction at driver level
func TestDriverTxReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin read-write transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{
		ReadOnly: false,
	})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	ourTx := tx.(*Tx)

	// Verify not read-only
	if ourTx.IsReadOnly() {
		t.Error("Transaction should not be read-only")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestDriverTxDoubleCommit tests double commit at driver level
func TestDriverTxDoubleCommit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// First commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("First commit failed: %v", err)
	}

	// Second commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error on double commit")
	}
}

// TestDriverTxDoubleRollback tests double rollback at driver level
func TestDriverTxDoubleRollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// First rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("First rollback failed: %v", err)
	}

	// Second rollback should be safe (idempotent)
	if err := tx.Rollback(); err != nil {
		t.Errorf("Second rollback should be safe: %v", err)
	}
}

// TestDriverTxState tests transaction state tracking
func TestDriverTxState(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	ourTx := tx.(*Tx)

	// Should not be closed initially
	if ourTx.IsClosed() {
		t.Error("Transaction should not be closed initially")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Should be closed after commit
	if !ourTx.IsClosed() {
		t.Error("Transaction should be closed after commit")
	}
}

// =============================================================================
// Additional Coverage Tests
// =============================================================================

// TestTransMultipleSequential tests multiple sequential transactions
func TestTransMultipleSequential(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	// First transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("First BEGIN failed: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("First COMMIT failed: %v", err)
	}

	// Second transaction
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Second BEGIN failed: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("Second COMMIT failed: %v", err)
	}

	// Third transaction with rollback
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Third BEGIN failed: %v", err)
	}

	if err := tx3.Rollback(); err != nil {
		t.Fatalf("Third ROLLBACK failed: %v", err)
	}
}

// TestTransCommitAfterRollback tests commit after rollback fails
func TestTransCommitAfterRollback(t *testing.T) {
	db := setupTransactionTestDB(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Rollback first
	if err := tx.Rollback(); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Try to commit - should fail
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error committing after rollback")
	}
}

// TestTransRollbackAfterCommit tests rollback after commit is safe
func TestTransRollbackAfterCommit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Commit first
	if err := tx.Commit(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Rollback after commit should be safe (idempotent)
	if err := tx.Rollback(); err != nil {
		t.Errorf("ROLLBACK after COMMIT should be safe: %v", err)
	}
}

// TestSavepointOnClosedTransaction tests savepoint operations on closed transaction
func TestSavepointOnClosedTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	ourTx := tx.(*Tx)

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Try savepoint operations on closed transaction
	if err := ourTx.Savepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn for Savepoint on closed tx, got: %v", err)
	}

	if err := ourTx.ReleaseSavepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn for ReleaseSavepoint on closed tx, got: %v", err)
	}

	if err := ourTx.RollbackToSavepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("Expected ErrBadConn for RollbackToSavepoint on closed tx, got: %v", err)
	}
}

// TestSavepointOnReadOnlyTransaction tests savepoint operations on read-only transaction
func TestSavepointOnReadOnlyTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	ourTx := tx.(*Tx)

	// Try savepoint operations on read-only transaction
	err = ourTx.Savepoint("sp1")
	if err == nil {
		t.Error("Expected error creating savepoint in read-only transaction")
	} else if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("Wrong error for Savepoint: %v", err)
	}

	err = ourTx.ReleaseSavepoint("sp1")
	if err == nil {
		t.Error("Expected error releasing savepoint in read-only transaction")
	} else if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("Wrong error for ReleaseSavepoint: %v", err)
	}

	err = ourTx.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("Expected error rolling back savepoint in read-only transaction")
	} else if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("Wrong error for RollbackToSavepoint: %v", err)
	}
}

// TestSavepointSequence tests a sequence of savepoint operations
func TestSavepointSequence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	d := &Driver{}
	conn, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	tx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback()

	ourTx := tx.(*Tx)

	// Create savepoint
	if err := ourTx.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint sp1 failed: %v", err)
	}

	// Create nested savepoint
	if err := ourTx.Savepoint("sp2"); err != nil {
		t.Fatalf("Savepoint sp2 failed: %v", err)
	}

	// Rollback to sp2
	if err := ourTx.RollbackToSavepoint("sp2"); err != nil {
		t.Fatalf("Rollback to sp2 failed: %v", err)
	}

	// Create sp2 again
	if err := ourTx.Savepoint("sp2"); err != nil {
		t.Fatalf("Savepoint sp2 (second) failed: %v", err)
	}

	// Release sp2
	if err := ourTx.ReleaseSavepoint("sp2"); err != nil {
		t.Fatalf("Release sp2 failed: %v", err)
	}

	// Release sp1
	if err := ourTx.ReleaseSavepoint("sp1"); err != nil {
		t.Fatalf("Release sp1 failed: %v", err)
	}
}
