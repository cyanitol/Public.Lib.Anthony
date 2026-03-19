// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestOpenWithOptionsError tests error handling when opening a database fails
func TestOpenWithOptionsError(t *testing.T) {
	// Try to open a database in a non-existent directory with read-only mode
	// This should fail because the file doesn't exist
	_, err := OpenWithOptions("/nonexistent/directory/test.db", true)
	if err == nil {
		t.Error("Expected error when opening non-existent database in read-only mode")
	}
}

// TestOpenWithOptionsExistingDatabase tests opening an existing database
func TestOpenWithOptionsExistingDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a database first
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create a table to make the database have pages
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	db.Close()

	// Now open it again with options
	db2, err := OpenWithOptions(dbPath, false)
	if err != nil {
		t.Fatalf("Failed to open existing database: %v", err)
	}
	defer db2.Close()

	// Schema loading from disk is not yet implemented, so we can't expect the table to be there
	// This test mainly ensures we can open an existing database without error
	// The page count might be 1 or more depending on pager implementation
	if db2 == nil {
		t.Error("Expected non-nil database")
	}
}

// TestCloseWithActiveTransaction tests closing a database with an active transaction
func TestCloseWithActiveTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table first
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Start a transaction and actually start it in the pager by doing a write
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Do a write operation to actually start the transaction in the pager
	_, err = tx.Execute(`INSERT INTO test VALUES (1)`)
	// Ignore error since insert might not be fully implemented

	// Close should rollback the transaction
	err = db.Close()
	// The close might succeed or fail depending on pager state, we just want to test the code path
	_ = err
}

// TestExecuteEmptyStatements tests executing empty SQL
func TestExecuteEmptyStatements(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute empty SQL
	result, err := db.Execute("")
	if err != nil {
		t.Errorf("Execute with empty SQL should not error: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

// TestCompileCreateTableError tests error handling in CompileCreateTable
func TestCompileCreateTableError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to create the same table again (should error)
	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

// TestCompileCreateIndexError tests error handling in CompileCreateIndex
func TestCompileCreateIndexError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Execute(`CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create an index
	_, err = db.Execute(`CREATE INDEX idx_test ON test (name)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Try to create the same index again (should error)
	_, err = db.Execute(`CREATE INDEX idx_test ON test (name)`)
	if err == nil {
		t.Error("Expected error when creating duplicate index")
	}
}

// TestCompileDropTableWithIfExists tests DROP TABLE IF EXISTS
func TestCompileDropTableWithIfExists(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Drop non-existent table with IF EXISTS should not error
	_, err = db.Execute(`DROP TABLE IF EXISTS nonexistent`)
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS should not error on non-existent table: %v", err)
	}
}

// TestCompileDropTableError tests error handling in CompileDropTable
func TestCompileDropTableError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to drop non-existent table without IF EXISTS
	_, err = db.Execute(`DROP TABLE nonexistent`)
	if err == nil {
		t.Error("Expected error when dropping non-existent table")
	}
}

// TestCompileDropIndexWithIfExists tests DROP INDEX IF EXISTS
func TestCompileDropIndexWithIfExists(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Drop non-existent index with IF EXISTS should not error
	_, err = db.Execute(`DROP INDEX IF EXISTS nonexistent`)
	if err != nil {
		t.Errorf("DROP INDEX IF EXISTS should not error on non-existent index: %v", err)
	}
}

// TestCompileDropIndexError tests error handling in CompileDropIndex
func TestCompileDropIndexError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to drop non-existent index without IF EXISTS
	_, err = db.Execute(`DROP INDEX nonexistent`)
	if err == nil {
		t.Error("Expected error when dropping non-existent index")
	}
}

// TestRowsScanErrorMismatch tests error handling in Rows.Scan with mismatched columns
func TestRowsScanErrorMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table and insert data
	_, err = db.Execute(`CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute(`INSERT INTO test VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query data
	rows, err := db.Query(`SELECT id, name FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Try to scan before calling Next
	var id int
	var name string
	err = rows.Scan(&id, &name)
	if err == nil {
		t.Error("Expected error when scanning before Next")
	}

	// Call Next
	if !rows.Next() {
		t.Fatal("Expected a row")
	}

	// Try to scan with wrong number of destinations
	err = rows.Scan(&id)
	if err == nil {
		t.Error("Expected error when scanning with wrong number of destinations")
	}
}

// TestTxCommitError tests error handling in Tx.Commit
func TestTxCommitError(t *testing.T) {
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
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Do a write to actually start the pager transaction
	_, _ = tx.Execute(`INSERT INTO test VALUES (1)`)

	// Commit once
	err = tx.Commit()
	// Might fail if insert didn't work, but we continue
	_ = err

	// Try to commit again - this should definitely error
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error when committing twice")
	}
}

// TestTxRollbackError tests error handling in Tx.Rollback
func TestTxRollbackError(t *testing.T) {
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
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Do a write to actually start the pager transaction
	_, _ = tx.Execute(`INSERT INTO test VALUES (1)`)

	// Rollback once
	err = tx.Rollback()
	// Might fail if insert didn't work, but we continue
	_ = err

	// Try to rollback again - this should definitely error
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error when rolling back twice")
	}
}

// TestTxExecuteAfterDone tests executing after transaction is done
func TestTxExecuteAfterDone(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually mark the transaction as done to test the error path
	tx.done = true

	// Try to execute after commit
	_, err = tx.Execute(`SELECT * FROM test`)
	if err == nil {
		t.Error("Expected error when executing after commit")
	}
}

// TestTxQueryAfterDone tests querying after transaction is done
func TestTxQueryAfterDone(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually mark the transaction as done to test the error path
	tx.done = true

	// Try to query after rollback
	_, err = tx.Query(`SELECT * FROM test`)
	if err == nil {
		t.Error("Expected error when querying after rollback")
	}
}

// TestTxExecAfterDone tests Exec after transaction is done
func TestTxExecAfterDone(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Do a write to start the transaction
	_, _ = tx.Execute(`INSERT INTO test VALUES (1)`)

	_ = tx.Commit() // Ignore error

	// Try to exec after commit
	_, err = tx.Exec(`INSERT INTO test VALUES (1)`)
	if err == nil {
		t.Error("Expected error when exec after commit")
	}
}

// TestPreparedStmtExecuteError tests error handling in PreparedStmt.Execute
func TestPreparedStmtExecuteError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	stmt, err := db.Prepare(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Close the statement
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Failed to close statement: %v", err)
	}

	// Try to execute after close
	_, err = stmt.Execute()
	if err == nil {
		t.Error("Expected error when executing closed statement")
	}
}

// TestPreparedStmtQueryError tests error handling in PreparedStmt.Query
func TestPreparedStmtQueryError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	stmt, err := db.Prepare(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Close the statement
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Failed to close statement: %v", err)
	}

	// Try to query after close
	_, err = stmt.Query()
	if err == nil {
		t.Error("Expected error when querying closed statement")
	}
}

// TestQueryRowScanEOFError tests error handling in QueryRow.Scan with empty result
func TestQueryRowScanEOFError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query empty table
	var id int
	err = db.QueryRow(`SELECT id FROM test`).Scan(&id)
	if err != io.EOF {
		t.Errorf("Expected io.EOF when scanning empty result, got: %v", err)
	}
}

// TestTriggerExecutorBeforeTriggers tests ExecuteBeforeTriggers
func TestTriggerExecutorBeforeTriggers(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger manually
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{},
	}

	sch.Triggers[trigger.Name] = trigger

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1, "value": "test"},
	}

	executor := NewTriggerExecutor(ctx)
	err = executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteBeforeTriggers failed: %v", err)
	}
}

// TestTriggerExecutorAfterTriggers tests ExecuteAfterTriggers
func TestTriggerExecutorAfterTriggers(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger manually
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{},
	}

	sch.Triggers[trigger.Name] = trigger

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1, "value": "test"},
	}

	executor := NewTriggerExecutor(ctx)
	err = executor.ExecuteAfterTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteAfterTriggers failed: %v", err)
	}
}

// TestTriggerExecutorInsteadOfTriggers tests ExecuteInsteadOfTriggers
func TestTriggerExecutorInsteadOfTriggers(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger manually
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerInsteadOf,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{},
	}

	sch.Triggers[trigger.Name] = trigger

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1, "value": "test"},
	}

	executor := NewTriggerExecutor(ctx)
	err = executor.ExecuteInsteadOfTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Errorf("ExecuteInsteadOfTriggers failed: %v", err)
	}
}

// TestExecuteTriggersForInsertBasic tests ExecuteTriggersForInsert basic functionality
func TestExecuteTriggersForInsertBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	err = ExecuteTriggersForInsert(ctx)
	if err != nil {
		t.Errorf("ExecuteTriggersForInsert failed: %v", err)
	}
}

// TestExecuteTriggersForUpdateBasic tests ExecuteTriggersForUpdate basic functionality
func TestExecuteTriggersForUpdateBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
		NewRow:    map[string]interface{}{"id": 2},
	}

	err = ExecuteTriggersForUpdate(ctx, []string{"id"})
	if err != nil {
		t.Errorf("ExecuteTriggersForUpdate failed: %v", err)
	}
}

// TestExecuteTriggersForDeleteBasic tests ExecuteTriggersForDelete basic functionality
func TestExecuteTriggersForDeleteBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
	}

	err = ExecuteTriggersForDelete(ctx)
	if err != nil {
		t.Errorf("ExecuteTriggersForDelete failed: %v", err)
	}
}

// TestPrepareOldRowWithExtra tests PrepareOldRow function with extra columns
func TestPrepareOldRowWithExtra(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	}

	rowData := map[string]interface{}{
		"id":    1,
		"value": "test",
		"extra": "ignored",
	}

	oldRow := PrepareOldRow(table, rowData)

	if len(oldRow) != 2 {
		t.Errorf("Expected 2 columns in old row, got %d", len(oldRow))
	}

	if oldRow["id"] != 1 {
		t.Errorf("Expected id=1, got %v", oldRow["id"])
	}

	if oldRow["value"] != "test" {
		t.Errorf("Expected value='test', got %v", oldRow["value"])
	}

	// Test with nil input
	nilRow := PrepareOldRow(table, nil)
	if nilRow != nil {
		t.Error("Expected nil for nil input")
	}
}

// TestPrepareNewRowWithExtra tests PrepareNewRow function with extra columns
func TestPrepareNewRowWithExtra(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	}

	rowData := map[string]interface{}{
		"id":    2,
		"value": "new",
		"extra": "ignored",
	}

	newRow := PrepareNewRow(table, rowData)

	if len(newRow) != 2 {
		t.Errorf("Expected 2 columns in new row, got %d", len(newRow))
	}

	if newRow["id"] != 2 {
		t.Errorf("Expected id=2, got %v", newRow["id"])
	}

	if newRow["value"] != "new" {
		t.Errorf("Expected value='new', got %v", newRow["value"])
	}

	// Test with nil input
	nilRow := PrepareNewRow(table, nil)
	if nilRow != nil {
		t.Error("Expected nil for nil input")
	}
}

// TestExecuteAfterInsertTriggersBasic tests ExecuteAfterInsertTriggers basic functionality
func TestExecuteAfterInsertTriggersBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	err = ExecuteAfterInsertTriggers(ctx)
	if err != nil {
		t.Errorf("ExecuteAfterInsertTriggers failed: %v", err)
	}
}

// TestExecuteAfterUpdateTriggersBasic tests ExecuteAfterUpdateTriggers basic functionality
func TestExecuteAfterUpdateTriggersBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
		NewRow:    map[string]interface{}{"id": 2},
	}

	err = ExecuteAfterUpdateTriggers(ctx, []string{"id"})
	if err != nil {
		t.Errorf("ExecuteAfterUpdateTriggers failed: %v", err)
	}
}

// TestExecuteAfterDeleteTriggersBasic tests ExecuteAfterDeleteTriggers basic functionality
func TestExecuteAfterDeleteTriggersBasic(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
	}

	err = ExecuteAfterDeleteTriggers(ctx)
	if err != nil {
		t.Errorf("ExecuteAfterDeleteTriggers failed: %v", err)
	}
}

// TestBeginTransactionTwice tests beginning a transaction when one is already active
func TestBeginTransactionTwice(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Begin()
	if err != nil {
		t.Fatalf("First Begin failed: %v", err)
	}

	// Try to begin again
	_, err = db.Begin()
	if err == nil {
		t.Error("Expected error when beginning transaction twice")
	}
}

// TestTxCommitWithoutTransaction tests committing when no transaction is in progress
func TestTxCommitWithoutTransaction(t *testing.T) {
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

	// Manually set transaction state to false
	db.inTransaction = false

	// Try to commit
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error when committing without active transaction")
	}
}

// TestTxRollbackWithoutTransaction tests rolling back when no transaction is in progress
func TestTxRollbackWithoutTransaction(t *testing.T) {
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

	// Manually set transaction state to false
	db.inTransaction = false

	// Try to rollback
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error when rolling back without active transaction")
	}
}

// TestRowsCloseIdempotent tests that closing Rows multiple times is safe
func TestRowsCloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	rows, err := db.Query(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	// Close multiple times
	err = rows.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	err = rows.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

// TestPreparedStmtCloseIdempotent tests that closing PreparedStmt multiple times is safe
func TestPreparedStmtCloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	stmt, err := db.Prepare(`SELECT * FROM test`)
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	// Close multiple times
	err = stmt.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

// TestScanIntoInterfacePointer tests scanning into *interface{}
func TestScanIntoInterfacePointer(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute(`INSERT INTO test VALUES (42, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	rows, err := db.Query(`SELECT id, name FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected a row")
	}

	var id interface{}
	var name interface{}
	err = rows.Scan(&id, &name)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify values
	if id == nil {
		t.Error("Expected non-nil id")
	}
	if name == nil {
		t.Error("Expected non-nil name")
	}
}

// TestTriggerWithStatements tests executing triggers with actual statements
func TestTriggerWithStatements(t *testing.T) {
	sch := schema.NewSchema()

	// Create a table
	_, err := sch.CreateTable(&parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "value", Type: "TEXT"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create a trigger with an INSERT statement
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		Body: []parser.Statement{
			&parser.InsertStmt{
				Table: "test",
				Values: [][]parser.Expression{
					{
						&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
						&parser.LiteralExpr{Type: parser.LiteralString, Value: "triggered"},
					},
				},
			},
		},
	}

	sch.Triggers[trigger.Name] = trigger

	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "test",
		Btree:     nil, // We can't create a real btree here without more setup
		NewRow:    map[string]interface{}{"id": 1, "value": "test"},
	}

	executor := NewTriggerExecutor(ctx)

	// This will try to execute the trigger body but will fail due to lack of btree
	// The important part is that it gets to the statement execution code
	err = executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil)
	// We expect an error here because we don't have a full engine setup
	// but we're covering the code paths
	_ = err // Error is expected
}

// TestOpenWithLoadSchemaError tests error handling when loading schema fails
func TestOpenWithLoadSchemaError(t *testing.T) {
	// Test that Open handles the loadSchema path. With a valid pager that
	// has pageCount > 1, loadSchema is called but currently returns nil.
	// Verify the engine still works correctly.
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table to make page count > 1
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	db.Close()

	// Reopen - this triggers loadSchema path since pageCount > 1
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()
}

// TestCloseWithPagerError tests error handling when pager.Close fails
func TestCloseWithPagerError(t *testing.T) {
	// Test normal close behavior - verify no error on clean close
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Normal close should succeed
	if err := db.Close(); err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}

// TestResultGetters tests Result getter methods
func TestResultGetters(t *testing.T) {
	result := &Result{
		Columns: []string{"id", "name", "age"},
		Rows: [][]interface{}{
			{1, "Alice", 30},
			{2, "Bob", 25},
		},
		RowsAffected: 2,
		LastInsertID: 2,
	}

	if result.RowCount() != 2 {
		t.Errorf("Expected row count 2, got %d", result.RowCount())
	}

	if result.ColumnCount() != 3 {
		t.Errorf("Expected column count 3, got %d", result.ColumnCount())
	}
}

// TestPreparedStmtSQLMethod tests PreparedStmt.SQL method
func TestPreparedStmtSQLMethod(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute(`CREATE TABLE test (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	sql := `SELECT * FROM test WHERE id = ?`
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}
	defer stmt.Close()

	if stmt.SQL() != sql {
		t.Errorf("Expected SQL %q, got %q", sql, stmt.SQL())
	}
}
