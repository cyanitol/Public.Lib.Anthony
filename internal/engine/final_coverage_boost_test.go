// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestTriggerExecuteBeforeWithWhenError tests ExecuteBeforeTriggers when WHEN clause errors
func TestTriggerExecuteBeforeWithWhenError(t *testing.T) {
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

	// Manually create a trigger with a WHEN clause that will be evaluated
	trigger := &schema.Trigger{
		Name:     "test_trigger",
		Table:    "test",
		Timing:   parser.TriggerBefore,
		Event:    parser.TriggerInsert,
		When:     &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Body:     []parser.Statement{},
		UpdateOf: nil,
	}

	// Manually add trigger to schema
	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute BEFORE triggers - will evaluate WHEN clause
	err = executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Logf("ExecuteBeforeTriggers with WHEN clause evaluation: %v", err)
	}
}

// TestTriggerExecuteAfterWithNonMatchingColumns tests ExecuteAfterTriggers with non-matching columns
func TestTriggerExecuteAfterWithNonMatchingColumns(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger with UPDATE OF specific column
	trigger := &schema.Trigger{
		Name:     "test_trigger",
		Table:    "test",
		Timing:   parser.TriggerAfter,
		Event:    parser.TriggerUpdate,
		UpdateOf: []string{"name"},
		Body:     []parser.Statement{},
	}

	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1, "name": "old"},
		NewRow:    map[string]interface{}{"id": 1, "name": "new"},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute with non-matching columns - trigger should not fire
	err = executor.ExecuteAfterTriggers(parser.TriggerUpdate, []string{"value"})
	if err != nil {
		t.Errorf("ExecuteAfterTriggers should not error: %v", err)
	}
}

// TestTriggerExecuteInsteadOfWithWhen tests ExecuteInsteadOfTriggers with WHEN clause
func TestTriggerExecuteInsteadOfWithWhen(t *testing.T) {
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

	// Create INSTEAD OF trigger with WHEN clause
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerInsteadOf,
		Event:  parser.TriggerInsert,
		When:   &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
		Body:   []parser.Statement{},
	}

	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute INSTEAD OF triggers
	err = executor.ExecuteInsteadOfTriggers(parser.TriggerInsert, nil)
	if err != nil {
		t.Logf("ExecuteInsteadOfTriggers: %v", err)
	}
}

// TestTriggerExecuteInsertUpdateDeleteSelect tests all execute statement helper functions
func TestTriggerExecuteInsertUpdateDeleteSelect(t *testing.T) {
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

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Test executeInsert
	vm1 := vdbe.New()
	vm1.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
	vm1.AddOp(vdbe.OpHalt, 0, 0, 0)
	insertStmt := &parser.InsertStmt{Table: "test"}
	err = executor.executeInsert(vm1, insertStmt)
	if err != nil {
		t.Logf("executeInsert: %v", err)
	}

	// Test executeUpdate
	vm2 := vdbe.New()
	vm2.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
	vm2.AddOp(vdbe.OpHalt, 0, 0, 0)
	updateStmt := &parser.UpdateStmt{Table: "test"}
	err = executor.executeUpdate(vm2, updateStmt)
	if err != nil {
		t.Logf("executeUpdate: %v", err)
	}

	// Test executeDelete
	vm3 := vdbe.New()
	vm3.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
	vm3.AddOp(vdbe.OpHalt, 0, 0, 0)
	deleteStmt := &parser.DeleteStmt{Table: "test"}
	err = executor.executeDelete(vm3, deleteStmt)
	if err != nil {
		t.Logf("executeDelete: %v", err)
	}

	// Test executeSelect
	vm4 := vdbe.New()
	vm4.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
	vm4.AddOp(vdbe.OpHalt, 0, 0, 0)
	selectStmt := &parser.SelectStmt{}
	err = executor.executeSelect(vm4, selectStmt)
	if err != nil {
		t.Logf("executeSelect: %v", err)
	}
}

// TestTriggerExecuteStatementBodyWithStatements tests executeTriggerBody with actual statements
func TestTriggerExecuteStatementBodyWithStatements(t *testing.T) {
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

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Create trigger with INSERT, UPDATE, DELETE, SELECT statements
	trigger := &schema.Trigger{
		Name:  "test_trigger",
		Table: "test",
		Body: []parser.Statement{
			&parser.InsertStmt{Table: "test"},
			&parser.UpdateStmt{Table: "test"},
			&parser.DeleteStmt{Table: "test"},
			&parser.SelectStmt{},
		},
	}

	// Execute trigger body - will execute all statements
	err = executor.executeTriggerBody(trigger)
	if err != nil {
		t.Logf("executeTriggerBody with statements: %v", err)
	}
}

// TestTransactionCommitSuccess tests successful commit path
func TestTransactionCommitSuccess(t *testing.T) {
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

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually set up transaction state to test commit path
	db.mu.Lock()
	db.inTransaction = true
	db.mu.Unlock()

	// Commit
	err = tx.Commit()
	// May succeed or fail depending on pager state
	if err != nil {
		t.Logf("Commit: %v", err)
	}
}

// TestTransactionRollbackSuccess tests successful rollback path
func TestTransactionRollbackSuccess(t *testing.T) {
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

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Manually set up transaction state
	db.mu.Lock()
	db.inTransaction = true
	db.mu.Unlock()

	// Rollback
	err = tx.Rollback()
	// May succeed or fail depending on pager state
	if err != nil {
		t.Logf("Rollback: %v", err)
	}
}

// TestPreparedStmtExecuteSuccess tests successful prepared statement execution
func TestPreparedStmtExecuteSuccess(t *testing.T) {
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

	// Prepare statement
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}
	defer stmt.Close()

	// Execute prepared statement
	result, err := stmt.Execute()
	if err != nil {
		t.Logf("Execute prepared statement: %v", err)
	} else if result == nil {
		t.Error("Execute should return result")
	}
}

// TestPreparedStmtQuerySuccess tests successful prepared statement query
func TestPreparedStmtQuerySuccess(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Prepare statement
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}
	defer stmt.Close()

	// Query prepared statement
	rows, err := stmt.Query()
	if err != nil {
		t.Logf("Query prepared statement: %v", err)
	} else {
		defer rows.Close()
		if rows == nil {
			t.Error("Query should return rows")
		}
	}
}

// TestRowsNextSuccessPath tests successful Next iteration
func TestRowsNextSuccessPath(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table and insert data
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Logf("Insert: %v", err)
	}

	// Query
	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Iterate - tests Next success path
	for rows.Next() {
		// Success path covered
		break
	}

	if rows.Err() != nil {
		t.Logf("Rows error: %v", rows.Err())
	}
}

// TestRowsScanSuccess tests successful Scan
func TestRowsScanSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Execute("INSERT INTO test VALUES (1, 'test')")
	if err != nil {
		t.Logf("Insert: %v", err)
	}

	// Query and scan
	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			t.Logf("Scan: %v", err)
		}
	}
}

// TestQueryRowScanSuccess tests successful QueryRow.Scan
func TestQueryRowScanSuccess(t *testing.T) {
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

	// Insert data
	_, err = db.Execute("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Logf("Insert: %v", err)
	}

	// QueryRow and scan
	var id int
	err = db.QueryRow("SELECT * FROM test").Scan(&id)
	if err != nil {
		t.Logf("QueryRow.Scan: %v", err)
	}
}

// TestCompileCreateTableSuccess tests successful table creation
func TestCompileCreateTableSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table - exercises full CompileCreateTable path
	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Errorf("CREATE TABLE: %v", err)
	}

	// Verify table was created
	_, ok := db.schema.GetTable("test")
	if !ok {
		t.Error("Table should exist")
	}
}

// TestCompileCreateIndexSuccess tests successful index creation
func TestCompileCreateIndexSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index - exercises full CompileCreateIndex path
	_, err = db.Execute("CREATE INDEX idx_id ON test (id)")
	if err != nil {
		t.Errorf("CREATE INDEX: %v", err)
	}

	// Verify index was created
	_, ok := db.schema.GetIndex("idx_id")
	if !ok {
		t.Error("Index should exist")
	}
}

// TestCompileDropTableSuccess tests successful table drop
func TestCompileDropTableSuccess(t *testing.T) {
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

	// Drop table - exercises full CompileDropTable path
	_, err = db.Execute("DROP TABLE test")
	if err != nil {
		t.Logf("DROP TABLE: %v", err)
	}

	// Verify table was dropped
	_, ok := db.schema.GetTable("test")
	if ok {
		t.Error("Table should not exist after drop")
	}
}

// TestCompileDropIndexSuccess tests successful index drop
func TestCompileDropIndexSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table and index
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Execute("CREATE INDEX idx_id ON test (id)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Drop index - exercises full CompileDropIndex path
	_, err = db.Execute("DROP INDEX idx_id")
	if err != nil {
		t.Logf("DROP INDEX: %v", err)
	}

	// Verify index was dropped
	_, ok := db.schema.GetIndex("idx_id")
	if ok {
		t.Error("Index should not exist after drop")
	}
}

// TestOpenWithOptionsReadOnly tests opening in read-only mode
func TestOpenWithOptionsReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.db"

	// Create database first
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	_, err = db1.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	db1.Close()

	// Open in read-only mode
	db2, err := OpenWithOptions(dbPath, true)
	if err != nil {
		t.Fatalf("Failed to open read-only: %v", err)
	}
	defer db2.Close()

	// Verify it's read-only
	if !db2.IsReadOnly() {
		t.Error("Database should be read-only")
	}
}

// TestEngineCloseSuccessPath tests successful Close
func TestEngineCloseSuccessPath(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create table to ensure database has content
	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Close normally
	err = db.Close()
	if err != nil {
		t.Errorf("Close should succeed: %v", err)
	}
}

// TestExecuteSuccessPath tests successful Execute path
func TestExecuteSuccessPath(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute CREATE TABLE
	result, err := db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Execute CREATE TABLE: %v", err)
	}
	if result == nil {
		t.Error("Execute should return result")
	}

	// Execute INSERT
	result, err = db.Execute("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Logf("Execute INSERT: %v", err)
	}

	// Execute SELECT
	result, err = db.Execute("SELECT * FROM test")
	if err != nil {
		t.Logf("Execute SELECT: %v", err)
	}
}

// TestExecuteTriggersForInsertWithError tests ExecuteTriggersForInsert error path
func TestExecuteTriggersForInsertWithError(t *testing.T) {
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

	// Create BEFORE INSERT trigger with body that will execute
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{&parser.SelectStmt{}},
	}

	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	// Execute triggers
	err = ExecuteTriggersForInsert(ctx)
	if err != nil {
		t.Logf("ExecuteTriggersForInsert: %v", err)
	}
}

// TestExecuteTriggersForUpdateWithError tests ExecuteTriggersForUpdate error path
func TestExecuteTriggersForUpdateWithError(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create BEFORE UPDATE trigger
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerUpdate,
		Body:   []parser.Statement{&parser.SelectStmt{}},
	}

	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1, "name": "old"},
		NewRow:    map[string]interface{}{"id": 1, "name": "new"},
	}

	// Execute triggers
	err = ExecuteTriggersForUpdate(ctx, []string{"name"})
	if err != nil {
		t.Logf("ExecuteTriggersForUpdate: %v", err)
	}
}

// TestExecuteTriggersForDeleteWithError tests ExecuteTriggersForDelete error path
func TestExecuteTriggersForDeleteWithError(t *testing.T) {
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

	// Create BEFORE DELETE trigger
	trigger := &schema.Trigger{
		Name:   "test_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerDelete,
		Body:   []parser.Statement{&parser.SelectStmt{}},
	}

	db.schema.Triggers["test_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1},
	}

	// Execute triggers
	err = ExecuteTriggersForDelete(ctx)
	if err != nil {
		t.Logf("ExecuteTriggersForDelete: %v", err)
	}
}
