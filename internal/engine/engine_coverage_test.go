// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ─────────────────────────────────────────────────────────────────────────────
// compiler.go — error branches
// ─────────────────────────────────────────────────────────────────────────────

// TestCompileInsert_TableNotFound covers the early-return when the table is absent.
func TestCompileInsert_TableNotFound(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	_, err := c.CompileInsert(&parser.InsertStmt{Table: "no_such_table"})
	if err == nil {
		t.Fatal("expected error for missing table in CompileInsert")
	}
}

// TestCompileUpdate_TableNotFound covers validateAndGetTable returning error in CompileUpdate.
func TestCompileUpdate_TableNotFound(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	_, err := c.CompileUpdate(&parser.UpdateStmt{
		Table: "ghost",
		Sets: []parser.Assignment{
			{Column: "x", Value: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing table in CompileUpdate")
	}
}

// TestCompileUpdate_BadWhereExpr covers the GenerateExpr error in compileUpdateWhere.
// A SubqueryExpr with nil Select causes GenerateExpr to return an error immediately.
func TestCompileUpdate_BadWhereExpr(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE upd_bad_where (id INTEGER)`)
	c := NewCompiler(db)
	_, err := c.CompileUpdate(&parser.UpdateStmt{
		Table: "upd_bad_where",
		Sets: []parser.Assignment{
			{Column: "id", Value: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
		// SubqueryExpr with nil Select causes an immediate GenerateExpr error.
		Where: &parser.SubqueryExpr{},
	})
	if err == nil {
		t.Fatal("expected error for nil-Select SubqueryExpr in WHERE of CompileUpdate")
	}
}

// TestCompileUpdate_BadSetExpr covers the GenerateExpr error in applySetAssignments.
// The SET value is a SubqueryExpr with nil Select that fails GenerateExpr.
func TestCompileUpdate_BadSetExpr(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE upd_bad_set (id INTEGER)`)
	c := NewCompiler(db)
	_, err := c.CompileUpdate(&parser.UpdateStmt{
		Table: "upd_bad_set",
		Sets: []parser.Assignment{
			// Column "id" exists (so GetColumnIndex succeeds at line 583), but its
			// value expression causes GenerateExpr to fail at line 589-591.
			{Column: "id", Value: &parser.SubqueryExpr{}},
		},
	})
	if err == nil {
		t.Fatal("expected error for nil-Select SubqueryExpr as SET value in CompileUpdate")
	}
}

// TestCompileDelete_TableNotFound covers validateAndGetTable returning error in CompileDelete.
func TestCompileDelete_TableNotFound(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	_, err := c.CompileDelete(&parser.DeleteStmt{Table: "ghost"})
	if err == nil {
		t.Fatal("expected error for missing table in CompileDelete")
	}
}

// TestCompileDelete_BadWhereExpr covers the GenerateExpr error in compileDeleteWhere.
func TestCompileDelete_BadWhereExpr(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE del_bad_where (id INTEGER)`)
	c := NewCompiler(db)
	_, err := c.CompileDelete(&parser.DeleteStmt{
		Table: "del_bad_where",
		// SubqueryExpr with nil Select causes an immediate GenerateExpr error.
		Where: &parser.SubqueryExpr{},
	})
	if err == nil {
		t.Fatal("expected error for nil-Select SubqueryExpr in WHERE of CompileDelete")
	}
}

// TestCompileSelect_BadWhereExpr covers the GenerateExpr error in compileWhereClause
// and compileWhereAndColumns.
func TestCompileSelect_BadWhereExpr(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE sel_bad_where (id INTEGER)`)
	c := NewCompiler(db)
	_, err := c.CompileSelect(&parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "sel_bad_where"}},
		},
		// SubqueryExpr with nil Select causes GenerateExpr to fail.
		Where: &parser.SubqueryExpr{},
	})
	if err == nil {
		t.Fatal("expected error for nil-Select SubqueryExpr in WHERE of CompileSelect")
	}
}

// TestCompileDropTable_IfExists covers the IfExists=true early-return in CompileDropTable.
func TestCompileDropTable_IfExists(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	vm, err := c.CompileDropTable(&parser.DropTableStmt{
		Name:     "nonexistent_tbl",
		IfExists: true,
	})
	if err != nil {
		t.Fatalf("expected no error for DROP TABLE IF EXISTS on missing table: %v", err)
	}
	if vm == nil {
		t.Fatal("expected non-nil VDBE")
	}
}

// TestCompileDropTable_NotFound covers the error branch when IfExists=false.
func TestCompileDropTable_NotFound(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	_, err := c.CompileDropTable(&parser.DropTableStmt{
		Name:     "nonexistent_tbl",
		IfExists: false,
	})
	if err == nil {
		t.Fatal("expected error for DROP TABLE on missing table")
	}
}

// TestCompileDropTable_Success covers the normal btree.DropTable + schema.DropTable path.
func TestCompileDropTable_Success(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE to_drop (id INTEGER)`)
	c := NewCompiler(db)
	vm, err := c.CompileDropTable(&parser.DropTableStmt{Name: "to_drop"})
	if err != nil {
		t.Fatalf("CompileDropTable success: %v", err)
	}
	if vm == nil {
		t.Fatal("expected non-nil VDBE from CompileDropTable")
	}
}

// TestCompileDropIndex_IfExists covers the IfExists=true early-return in CompileDropIndex.
func TestCompileDropIndex_IfExists(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	vm, err := c.CompileDropIndex(&parser.DropIndexStmt{
		Name:     "nonexistent_idx",
		IfExists: true,
	})
	if err != nil {
		t.Fatalf("expected no error for DROP INDEX IF EXISTS on missing index: %v", err)
	}
	if vm == nil {
		t.Fatal("expected non-nil VDBE")
	}
}

// TestCompileDropIndex_NotFound covers the error branch when IfExists=false.
func TestCompileDropIndex_NotFound(t *testing.T) {
	db := newTestDB(t)
	c := NewCompiler(db)
	_, err := c.CompileDropIndex(&parser.DropIndexStmt{
		Name:     "nonexistent_idx",
		IfExists: false,
	})
	if err == nil {
		t.Fatal("expected error for DROP INDEX on missing index")
	}
}

// TestCompileDropIndex_Success covers the normal btree.DropTable + schema.DropIndex path.
func TestCompileDropIndex_Success(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE idx_owner (id INTEGER)`)
	mustExec(t, db, `CREATE INDEX idx_to_drop ON idx_owner (id)`)
	c := NewCompiler(db)
	vm, err := c.CompileDropIndex(&parser.DropIndexStmt{Name: "idx_to_drop"})
	if err != nil {
		t.Fatalf("CompileDropIndex success: %v", err)
	}
	if vm == nil {
		t.Fatal("expected non-nil VDBE from CompileDropIndex")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// engine.go — OpenWithOptions error path
// ─────────────────────────────────────────────────────────────────────────────

// TestOpenWithOptions_FailBadPath covers the pager.Open failure when the parent
// directory does not exist.
func TestOpenWithOptions_FailBadPath(t *testing.T) {
	_, err := OpenWithOptions("/nonexistent/deep/path/test.db", false)
	if err == nil {
		t.Fatal("expected error opening db at non-existent path")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// engine.go — Close with active transaction
// ─────────────────────────────────────────────────────────────────────────────

// TestClose_WithActiveTransaction covers the inTransaction=true branch in Close.
func TestClose_WithActiveTransaction(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "close_tx.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	// Close without committing — triggers the rollback path in Close.
	if err := db.Close(); err != nil {
		t.Fatalf("Close with active transaction: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// result.go — PreparedStmt error branches
// ─────────────────────────────────────────────────────────────────────────────

// TestPreparedStmt_ExecuteOnClosed covers the closed=true guard in PreparedStmt.Execute.
func TestPreparedStmt_ExecuteOnClosed(t *testing.T) {
	db := newTestDB(t)
	ps, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = ps.Execute()
	if err == nil {
		t.Fatal("expected error executing closed PreparedStmt")
	}
}

// TestPreparedStmt_QueryOnClosed covers the closed=true guard in PreparedStmt.Query.
func TestPreparedStmt_QueryOnClosed(t *testing.T) {
	db := newTestDB(t)
	ps, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = ps.Query()
	if err == nil {
		t.Fatal("expected error querying closed PreparedStmt")
	}
}

// TestPreparedStmt_ExecuteSuccess exercises the full Execute path.
func TestPreparedStmt_ExecuteSuccess(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE ps_exec (id INTEGER)`)
	ps, err := db.Prepare(`INSERT INTO ps_exec (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("Prepare INSERT: %v", err)
	}
	defer ps.Close()
	_, err = ps.Execute()
	if err != nil {
		t.Fatalf("PreparedStmt.Execute: %v", err)
	}
}

// TestPreparedStmt_QuerySuccess exercises the full Query path.
func TestPreparedStmt_QuerySuccess(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE ps_query (id INTEGER)`)
	ps, err := db.Prepare(`SELECT id FROM ps_query`)
	if err != nil {
		t.Fatalf("Prepare SELECT: %v", err)
	}
	defer ps.Close()
	rows, err := ps.Query()
	if err != nil {
		t.Fatalf("PreparedStmt.Query: %v", err)
	}
	rows.Close()
}

// TestQueryRow_ScanNoRows covers the io.EOF return in QueryRow.Scan when no rows exist.
func TestQueryRow_ScanNoRows(t *testing.T) {
	db := newTestDB(t)
	mustExec(t, db, `CREATE TABLE qr_empty (id INTEGER)`)
	qr := db.QueryRow("SELECT id FROM qr_empty")
	var id int
	err := qr.Scan(&id)
	if err == nil {
		t.Fatal("expected EOF for empty result set in QueryRow.Scan")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// result.go — Tx double-commit / double-rollback
// ─────────────────────────────────────────────────────────────────────────────

// TestTx_CommitAlreadyDone covers the tx.done guard in Commit.
func TestTx_CommitAlreadyDone(t *testing.T) {
	db := newTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("expected error committing already-finished transaction")
	}
}

// TestTx_RollbackAlreadyDone covers the tx.done guard in Rollback.
func TestTx_RollbackAlreadyDone(t *testing.T) {
	db := newTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("first Rollback: %v", err)
	}
	if err := tx.Rollback(); err == nil {
		t.Fatal("expected error rolling back already-finished transaction")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// trigger.go — ExecuteTriggersForInsert/Update/Delete error return paths
// ─────────────────────────────────────────────────────────────────────────────

// makeTriggerCtxWithUnsupportedBody returns a TriggerContext whose trigger body
// contains a *parser.BeginStmt — unsupported by compileAndExecuteStatement —
// which causes it to error and propagates through ExecuteBeforeTriggers.
func makeTriggerCtxWithUnsupportedBody(tableName string, timing parser.TriggerTiming, event parser.TriggerEvent) *TriggerContext {
	sch := schema.NewSchema()
	sch.Tables[tableName] = &schema.Table{
		Name:    tableName,
		Columns: []*schema.Column{{Name: "id"}},
	}
	if sch.Triggers == nil {
		sch.Triggers = make(map[string]*schema.Trigger)
	}
	sch.Triggers["trg_bad"] = &schema.Trigger{
		Name:   "trg_bad",
		Table:  tableName,
		Timing: timing,
		Event:  event,
		Body:   []parser.Statement{&parser.BeginStmt{}},
	}
	return &TriggerContext{
		Schema:    sch,
		TableName: tableName,
		NewRow:    map[string]interface{}{"id": int64(1)},
		OldRow:    map[string]interface{}{"id": int64(1)},
	}
}

// TestExecuteTriggersForInsert_ErrorReturn covers the "return err" line in
// ExecuteTriggersForInsert (line 509-511).
func TestExecuteTriggersForInsert_ErrorReturn(t *testing.T) {
	ctx := makeTriggerCtxWithUnsupportedBody("t", parser.TriggerBefore, parser.TriggerInsert)
	err := ExecuteTriggersForInsert(ctx)
	if err == nil {
		t.Fatal("expected error from BEFORE INSERT trigger with unsupported body")
	}
}

// TestExecuteTriggersForUpdate_ErrorReturn covers the "return err" line in
// ExecuteTriggersForUpdate (line 530-532).
func TestExecuteTriggersForUpdate_ErrorReturn(t *testing.T) {
	ctx := makeTriggerCtxWithUnsupportedBody("t", parser.TriggerBefore, parser.TriggerUpdate)
	err := ExecuteTriggersForUpdate(ctx, nil)
	if err == nil {
		t.Fatal("expected error from BEFORE UPDATE trigger with unsupported body")
	}
}

// TestExecuteTriggersForDelete_ErrorReturn covers the "return err" line in
// ExecuteTriggersForDelete (line 548-550).
func TestExecuteTriggersForDelete_ErrorReturn(t *testing.T) {
	ctx := makeTriggerCtxWithUnsupportedBody("t", parser.TriggerBefore, parser.TriggerDelete)
	err := ExecuteTriggersForDelete(ctx)
	if err == nil {
		t.Fatal("expected error from BEFORE DELETE trigger with unsupported body")
	}
}

// TestSubstituteInInsert_ValuesError covers the error path inside substituteInInsert
// when an expression in VALUES fails substitution (NEW is nil).
func TestSubstituteInInsert_ValuesError(t *testing.T) {
	te := newTestExecutor(nil, nil)
	stmt := &parser.InsertStmt{
		Table:   "t",
		Columns: []string{"id"},
		Values: [][]parser.Expression{
			{&parser.IdentExpr{Table: "NEW", Name: "id"}},
		},
	}
	_, err := te.SubstituteOldNewReferences(stmt)
	if err == nil {
		t.Fatal("expected error substituting NEW.id when NewRow is nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// trigger.go:48-50 — executeTriggers WHEN clause error
// ─────────────────────────────────────────────────────────────────────────────

// TestExecuteTriggers_WhenClauseEvaluationError covers line 48-50 in executeTriggers:
// the path where trigger.ShouldExecuteTrigger returns an error.
// Using an IdentExpr with Table="INVALID" as the WHEN clause causes evaluateWhenClause
// to return an error (unknown table qualifier).
func TestExecuteTriggers_WhenClauseEvaluationError(t *testing.T) {
	sch := schema.NewSchema()
	sch.Tables["t"] = &schema.Table{Name: "t", Columns: []*schema.Column{{Name: "id"}}}
	if sch.Triggers == nil {
		sch.Triggers = make(map[string]*schema.Trigger)
	}
	sch.Triggers["trg_when_eval_err"] = &schema.Trigger{
		Name:   "trg_when_eval_err",
		Table:  "t",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerInsert,
		// IdentExpr with table="INVALID" causes ShouldExecuteTrigger to return error.
		When:   &parser.IdentExpr{Table: "INVALID", Name: "col"},
		Body:   []parser.Statement{&parser.InsertStmt{Table: "t"}},
	}
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "t",
		NewRow:    map[string]interface{}{"id": int64(1)},
	}
	err := ExecuteTriggersForInsert(ctx)
	if err == nil {
		t.Fatal("expected error from WHEN clause evaluation failure")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// trigger.go:139-141 — compileAndExecuteStatement SubstituteOldNewReferences error
// ─────────────────────────────────────────────────────────────────────────────

// TestCompileAndExecuteStatement_SubstituteError covers lines 139-141 in
// compileAndExecuteStatement when SubstituteOldNewReferences returns an error.
// A DELETE trigger whose body has an INSERT referencing NEW (nil for DELETE) triggers this.
func TestCompileAndExecuteStatement_SubstituteError(t *testing.T) {
	sch := schema.NewSchema()
	sch.Tables["t"] = &schema.Table{Name: "t", Columns: []*schema.Column{{Name: "id"}}}
	if sch.Triggers == nil {
		sch.Triggers = make(map[string]*schema.Trigger)
	}
	sch.Triggers["trg_delete_new_ref"] = &schema.Trigger{
		Name:   "trg_delete_new_ref",
		Table:  "t",
		Timing: parser.TriggerBefore,
		Event:  parser.TriggerDelete,
		// Body references NEW.id in an INSERT — but for DELETE triggers NEW is nil.
		Body: []parser.Statement{
			&parser.InsertStmt{
				Table:   "t",
				Columns: []string{"id"},
				Values: [][]parser.Expression{
					{&parser.IdentExpr{Table: "NEW", Name: "id"}},
				},
			},
		},
	}
	ctx := &TriggerContext{
		Schema:    sch,
		TableName: "t",
		OldRow:    map[string]interface{}{"id": int64(1)},
		// NewRow is intentionally nil — DELETE trigger has no NEW record.
	}
	err := ExecuteTriggersForDelete(ctx)
	if err == nil {
		t.Fatal("expected error when DELETE trigger body references NEW (nil)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// compiler.go:774-776 — CompileDropTable btree.DropTable error via rootPage=0
// ─────────────────────────────────────────────────────────────────────────────

// TestCompileDropTable_BtreeError covers lines 774-776 in CompileDropTable:
// btree.DropTable returns an error when rootPage=0.
// We inject a schema entry with RootPage=0 directly to reach this path.
func TestCompileDropTable_BtreeError(t *testing.T) {
	db := newTestDB(t)
	// Directly inject a table with RootPage=0 into the schema.
	// btree.DropTable(0) returns "invalid root page 0".
	db.schema.Tables["zero_page_tbl"] = &schema.Table{
		Name:     "zero_page_tbl",
		RootPage: 0,
		Columns:  []*schema.Column{{Name: "id"}},
	}
	c := NewCompiler(db)
	_, err := c.CompileDropTable(&parser.DropTableStmt{Name: "zero_page_tbl"})
	if err == nil {
		t.Fatal("expected btree error for DROP TABLE with rootPage=0")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// compiler.go:804-806 — CompileDropIndex btree.DropTable error via rootPage=0
// ─────────────────────────────────────────────────────────────────────────────

// TestCompileDropIndex_BtreeError covers lines 804-806 in CompileDropIndex:
// btree.DropTable returns an error when rootPage=0.
// We inject a schema index entry with RootPage=0 directly.
func TestCompileDropIndex_BtreeError(t *testing.T) {
	db := newTestDB(t)
	if db.schema.Indexes == nil {
		db.schema.Indexes = make(map[string]*schema.Index)
	}
	db.schema.Indexes["zero_page_idx"] = &schema.Index{
		Name:     "zero_page_idx",
		Table:    "t",
		RootPage: 0,
	}
	c := NewCompiler(db)
	_, err := c.CompileDropIndex(&parser.DropIndexStmt{Name: "zero_page_idx"})
	if err == nil {
		t.Fatal("expected btree error for DROP INDEX with rootPage=0")
	}
}
