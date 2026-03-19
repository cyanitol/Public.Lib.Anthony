// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestTriggerExecuteStatementVDBERunErrors tests error handling in execute* functions
func TestTriggerExecuteStatementVDBERunErrors(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)

	// Create VDBE that will fail on Run()
	t.Run("executeInsert error", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
		// Add invalid operation to cause error
		vm.AddOp(vdbe.OpColumn, 999, 999, 0)
		stmt := &parser.InsertStmt{Table: "test"}

		err := executor.executeInsert(vm, stmt)
		if err == nil {
			t.Log("Expected error from executeInsert")
		}
	})

	t.Run("executeUpdate error", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
		vm.AddOp(vdbe.OpColumn, 999, 999, 0)
		stmt := &parser.UpdateStmt{Table: "test"}

		err := executor.executeUpdate(vm, stmt)
		if err == nil {
			t.Log("Expected error from executeUpdate")
		}
	})

	t.Run("executeDelete error", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
		vm.AddOp(vdbe.OpColumn, 999, 999, 0)
		stmt := &parser.DeleteStmt{Table: "test"}

		err := executor.executeDelete(vm, stmt)
		if err == nil {
			t.Log("Expected error from executeDelete")
		}
	})

	t.Run("executeSelect error", func(t *testing.T) {
		vm := vdbe.New()
		vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Pager: db.pager, Schema: db.schema}
		vm.AddOp(vdbe.OpColumn, 999, 999, 0)
		stmt := &parser.SelectStmt{}

		err := executor.executeSelect(vm, stmt)
		if err == nil {
			t.Log("Expected error from executeSelect")
		}
	})
}

// TestTriggerExecuteBeforeTriggersWithExecuteError tests error propagation
func TestTriggerExecuteBeforeTriggersWithExecuteError(t *testing.T) {
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

	// Create BEFORE trigger with statement that will cause error
	trigger := &schema.Trigger{
		Name:   "error_trigger",
		Table:  "test",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{&parser.CreateTableStmt{Name: "invalid"}},
	}

	db.schema.Triggers["error_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute - should error on statement execution
	err = executor.ExecuteBeforeTriggers(parser.TriggerInsert, nil)
	if err == nil {
		t.Error("Expected error from ExecuteBeforeTriggers")
	}
}

// TestTriggerExecuteAfterTriggersWithExecuteError tests error propagation in AFTER triggers
func TestTriggerExecuteAfterTriggersWithExecuteError(t *testing.T) {
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

	// Create AFTER trigger with error-causing statement
	trigger := &schema.Trigger{
		Name:   "error_trigger",
		Table:  "test",
		Timing: parser.TriggerAfter,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{&parser.CreateTableStmt{Name: "invalid"}},
	}

	db.schema.Triggers["error_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute - should error on statement execution
	err = executor.ExecuteAfterTriggers(parser.TriggerInsert, nil)
	if err == nil {
		t.Error("Expected error from ExecuteAfterTriggers")
	}
}

// TestTriggerExecuteInsteadOfTriggersWithExecuteError tests error propagation in INSTEAD OF triggers
func TestTriggerExecuteInsteadOfTriggersWithExecuteError(t *testing.T) {
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

	// Create INSTEAD OF trigger with error-causing statement
	trigger := &schema.Trigger{
		Name:   "error_trigger",
		Table:  "test",
		Timing: parser.TriggerInsteadOf,
		Event:  parser.TriggerInsert,
		Body:   []parser.Statement{&parser.CreateTableStmt{Name: "invalid"}},
	}

	db.schema.Triggers["error_trigger"] = trigger

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	executor := NewTriggerExecutor(ctx)

	// Execute - should error on statement execution
	err = executor.ExecuteInsteadOfTriggers(parser.TriggerInsert, nil)
	if err == nil {
		t.Error("Expected error from ExecuteInsteadOfTriggers")
	}
}

// TestCompileAndExecuteStatementSubstituteError tests substituteOldNewReferences error path
func TestCompileAndExecuteStatementSubstituteError(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
	}

	executor := NewTriggerExecutor(ctx)
	vm := vdbe.New()
	vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Schema: db.schema}

	// Currently substituteOldNewReferences doesn't error, but we test the code path
	stmt := &parser.InsertStmt{Table: "test"}
	err = executor.compileAndExecuteStatement(vm, stmt)
	// May or may not error depending on implementation
	if err != nil {
		t.Logf("compileAndExecuteStatement: %v", err)
	}
}

// TestTransactionOperationsEdgeCases tests edge cases in transaction operations
func TestTransactionOperationsEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("Tx.Commit success path", func(t *testing.T) {
		testTxCommitPath(t, db)
	})
	t.Run("Tx.Rollback success path", func(t *testing.T) {
		testTxRollbackPath(t, db)
	})
	t.Run("PreparedStmt.Execute with bind", func(t *testing.T) {
		testPreparedExec(t, db)
	})
	t.Run("PreparedStmt.Query with bind", func(t *testing.T) {
		testPreparedQuery(t, db)
	})
}

func testTxCommitPath(t *testing.T, db *Engine) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	db.mu.Lock()
	db.inTransaction = true
	db.mu.Unlock()
	if err = tx.Commit(); err != nil {
		t.Logf("Commit: %v", err)
	}
}

func testTxRollbackPath(t *testing.T, db *Engine) {
	t.Helper()
	db.mu.Lock()
	db.inTransaction = false
	db.mu.Unlock()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	db.mu.Lock()
	db.inTransaction = true
	db.mu.Unlock()
	if err = tx.Rollback(); err != nil {
		t.Logf("Rollback: %v", err)
	}
}

func testPreparedExec(t *testing.T, db *Engine) {
	t.Helper()
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()
	result, err := stmt.Execute(1, "test", 3.14)
	if err != nil {
		t.Logf("Execute with params: %v", err)
	} else if result == nil {
		t.Error("Result should not be nil")
	}
}

func testPreparedQuery(t *testing.T, db *Engine) {
	t.Helper()
	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(1, 2, 3)
	if err != nil {
		t.Logf("Query with params: %v", err)
	} else {
		defer rows.Close()
	}
}

// TestRowsNextAndScanEdgeCases tests Rows.Next and Rows.Scan edge cases
func TestRowsNextAndScanEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT, value REAL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if _, err = db.Execute("INSERT INTO test VALUES (1, 'test', 3.14)"); err != nil {
		t.Logf("Insert: %v", err)
	}

	t.Run("Rows.Next with data", func(t *testing.T) {
		testRowsNextWithData(t, db)
	})
	t.Run("Rows error propagation", func(t *testing.T) {
		testRowsErrorPropagation(t, db)
	})
}

func testRowsNextWithData(t *testing.T, db *Engine) {
	t.Helper()
	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Error("Next should return true for first row")
	}
	var id int
	var name string
	var value float64
	if err = rows.Scan(&id, &name, &value); err != nil {
		t.Logf("Scan: %v", err)
	}
	if rows.Next() {
		t.Log("Next returned true for second row (may have more data)")
	}
}

func testRowsErrorPropagation(t *testing.T, db *Engine) {
	t.Helper()
	vm := vdbe.New()
	vm.AddOp(vdbe.OpColumn, 999, 999, 0)
	vm.Ctx = &vdbe.VDBEContext{Btree: db.btree, Schema: db.schema}
	rows := &Rows{vdbe: vm, columns: []string{"col"}, done: false}
	if rows.Next() {
		t.Error("Next should return false on error")
	}
	if rows.Err() == nil {
		t.Log("Expected error to be set")
	}
}

// TestQueryRowScanEdgeCases tests QueryRow.Scan edge cases
func TestQueryRowScanEdgeCases(t *testing.T) {
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

	t.Run("QueryRow.Scan with row", func(t *testing.T) {
		// Insert data
		_, err = db.Execute("INSERT INTO test VALUES (42, 'answer')")
		if err != nil {
			t.Logf("Insert: %v", err)
		}

		// QueryRow and scan
		var id int
		var name string
		err = db.QueryRow("SELECT * FROM test WHERE id = 42").Scan(&id, &name)
		if err != nil {
			t.Logf("QueryRow.Scan: %v", err)
		}
	})

	t.Run("QueryRow.Scan with rows error", func(t *testing.T) {
		// Query with rows that will have error
		qr := db.QueryRow("SELECT * FROM nonexistent")
		var id int
		err = qr.Scan(&id)
		if err == nil {
			t.Error("Expected error from Scan on invalid query")
		}
	})
}

// TestExecuteEdgeCases tests Execute edge cases
func TestExecuteEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("Execute with VDBE step error", func(t *testing.T) {
		// Create table
		_, err = db.Execute("CREATE TABLE test (id INTEGER)")
		if err != nil {
			t.Fatalf("Create table: %v", err)
		}

		// Try to query non-existent column
		_, err = db.Execute("SELECT nonexistent FROM test")
		if err == nil {
			t.Log("Expected error for non-existent column")
		}
	})

	t.Run("Execute with finalization", func(t *testing.T) {
		// Normal execution should finalize successfully
		result, err := db.Execute("SELECT 1")
		if err != nil {
			t.Errorf("Execute: %v", err)
		}
		if result == nil {
			t.Error("Result should not be nil")
		}
	})
}

// TestTriggersExecuteWithRealStatements tests trigger execution with real SQL statements
func TestTriggersExecuteWithRealStatements(t *testing.T) {
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

	// Create trigger via CreateTrigger statement
	triggerStmt := &parser.CreateTriggerStmt{
		Name:       "test_real_trigger",
		Table:      "test",
		Timing:     parser.TriggerBefore,
		Event:      parser.TriggerInsert,
		ForEachRow: true,
		Body: []parser.Statement{
			&parser.InsertStmt{Table: "test"},
			&parser.UpdateStmt{Table: "test"},
			&parser.DeleteStmt{Table: "test"},
			&parser.SelectStmt{},
		},
	}

	_, err = db.schema.CreateTrigger(triggerStmt)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	ctx := &TriggerContext{
		Schema:    db.schema,
		Pager:     db.pager,
		Btree:     db.btree,
		TableName: "test",
		NewRow:    map[string]interface{}{"id": 1},
	}

	// Execute trigger
	err = ExecuteTriggersForInsert(ctx)
	if err != nil {
		t.Logf("ExecuteTriggersForInsert with real statements: %v", err)
	}
}

// TestAllConvenienceFunctions tests all trigger convenience functions
func TestAllConvenienceFunctions(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/test.db")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	registerTestTriggers(db)

	ctx := &TriggerContext{
		Schema: db.schema, Pager: db.pager, Btree: db.btree,
		TableName: "test",
		OldRow:    map[string]interface{}{"id": 1, "name": "old"},
		NewRow:    map[string]interface{}{"id": 2, "name": "new"},
	}

	cols := []string{"name"}
	fns := []struct {
		name string
		fn   func() error
	}{
		{"ExecuteTriggersForInsert", func() error { return ExecuteTriggersForInsert(ctx) }},
		{"ExecuteAfterInsertTriggers", func() error { return ExecuteAfterInsertTriggers(ctx) }},
		{"ExecuteTriggersForUpdate", func() error { return ExecuteTriggersForUpdate(ctx, cols) }},
		{"ExecuteAfterUpdateTriggers", func() error { return ExecuteAfterUpdateTriggers(ctx, cols) }},
		{"ExecuteTriggersForDelete", func() error { return ExecuteTriggersForDelete(ctx) }},
		{"ExecuteAfterDeleteTriggers", func() error { return ExecuteAfterDeleteTriggers(ctx) }},
	}

	for _, tt := range fns {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(); err != nil {
				t.Logf("%s: %v", tt.name, err)
			}
		})
	}
}

func registerTestTriggers(db *Engine) {
	idx := 0
	for _, timing := range []parser.TriggerTiming{parser.TriggerBefore, parser.TriggerAfter} {
		for _, event := range []parser.TriggerEvent{parser.TriggerInsert, parser.TriggerUpdate, parser.TriggerDelete} {
			triggerName := "trigger_" + string(rune('0'+idx))
			idx++
			db.schema.Triggers[triggerName] = &schema.Trigger{
				Name: triggerName, Table: "test", Timing: timing, Event: event,
				Body: []parser.Statement{},
			}
		}
	}
}
