// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestMCDC3_PreparedStmtExecuteClosed covers the ps.closed=true branch in
// PreparedStmt.Execute (Execute at 72.7%).
func TestMCDC3_PreparedStmtExecuteClosed(t *testing.T) {
	t.Parallel()
	// MC/DC for PreparedStmt.Execute:
	//   C1: ps.closed=true → "statement is closed" error
	ps := &PreparedStmt{closed: true}
	_, err := ps.Execute()
	if err == nil {
		t.Fatal("Execute on closed PreparedStmt should return error")
	}
	if !strings.Contains(err.Error(), "statement is closed") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_PreparedStmtQueryClosedDirect covers the ps.closed=true branch in
// PreparedStmt.Query via direct struct construction (Query at 77.8%).
func TestMCDC3_PreparedStmtQueryClosedDirect(t *testing.T) {
	t.Parallel()
	// MC/DC for PreparedStmt.Query:
	//   C1: ps.closed=true → "statement is closed" error
	ps := &PreparedStmt{closed: true}
	_, err := ps.Query()
	if err == nil {
		t.Fatal("Query on closed PreparedStmt should return error")
	}
	if !strings.Contains(err.Error(), "statement is closed") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_ScanUnsupportedDestType covers the unsupported destination type
// branch in scanInto (Scan at 87.5%).
func TestMCDC3_ScanUnsupportedDestType(t *testing.T) {
	t.Parallel()
	// MC/DC for scanInto:
	//   C1: dest is not *interface{} and not a typed pointer → "unsupported scan destination type"
	mem := vdbe.NewMem()
	mem.SetInt(1)
	type customType struct{ x int }
	dest := &customType{}
	err := scanInto(mem, dest)
	if err == nil {
		t.Fatal("scanInto with unsupported type should return error")
	}
	if !strings.Contains(err.Error(), "unsupported scan destination type") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_ScanNilDest covers the nil destination path in scanInto.
func TestMCDC3_ScanNilDest(t *testing.T) {
	t.Parallel()
	// MC/DC for scanInto:
	//   untyped nil dest falls through to "unsupported scan destination type"
	mem := vdbe.NewMem()
	mem.SetInt(42)
	err := scanInto(mem, nil)
	if err == nil {
		t.Fatal("scanInto with nil dest should return error")
	}
}

// TestMCDC3_CommitTxDone covers the tx.done=true branch in Tx.Commit
// (Commit at 91.7%) via direct struct instantiation.
func TestMCDC3_CommitTxDone(t *testing.T) {
	t.Parallel()
	// MC/DC for Tx.Commit:
	//   C1: tx.done=true → "transaction already finished"
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/txdone.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx := &Tx{done: true, engine: db}
	err = tx.Commit()
	if err == nil {
		t.Fatal("Commit on done Tx should return error")
	}
	if !strings.Contains(err.Error(), "transaction already finished") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_RollbackTxDone covers the tx.done=true branch in Tx.Rollback
// (Rollback at 91.7%) via direct struct instantiation.
func TestMCDC3_RollbackTxDone(t *testing.T) {
	t.Parallel()
	// MC/DC for Tx.Rollback:
	//   C1: tx.done=true → "transaction already finished"
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/txdone2.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx := &Tx{done: true, engine: db}
	err = tx.Rollback()
	if err == nil {
		t.Fatal("Rollback on done Tx should return error")
	}
	if !strings.Contains(err.Error(), "transaction already finished") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_CommitNoTransactionInProgress covers the inTransaction=false branch
// in Tx.Commit when tx.done=false but engine has no active transaction.
func TestMCDC3_CommitNoTransactionInProgress(t *testing.T) {
	t.Parallel()
	// MC/DC for Tx.Commit:
	//   C2: tx.done=false AND engine.inTransaction=false → "no transaction in progress"
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/notxn.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Construct Tx with done=false but engine has no active transaction.
	tx := &Tx{done: false, engine: db}
	err = tx.Commit()
	if err == nil {
		t.Fatal("Commit with no active transaction should return error")
	}
	if !strings.Contains(err.Error(), "no transaction in progress") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_RollbackNoTransactionInProgress covers the inTransaction=false branch
// in Tx.Rollback when tx.done=false but engine has no active transaction.
func TestMCDC3_RollbackNoTransactionInProgress(t *testing.T) {
	t.Parallel()
	// MC/DC for Tx.Rollback:
	//   C2: tx.done=false AND engine.inTransaction=false → "no transaction in progress"
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/notxn2.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx := &Tx{done: false, engine: db}
	err = tx.Rollback()
	if err == nil {
		t.Fatal("Rollback with no active transaction should return error")
	}
	if !strings.Contains(err.Error(), "no transaction in progress") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_CompileInsertNonexistentTable covers the table-not-found error path
// in CompileInsert directly via Compiler (CompileInsert at 90.9%).
func TestMCDC3_CompileInsertNonexistentTable(t *testing.T) {
	t.Parallel()
	// MC/DC for CompileInsert:
	//   C1: schema.GetTable returns false → "table not found" error
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/ci.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	c := NewCompiler(db)
	stmt := &parser.InsertStmt{
		Table: "ghost_table",
		Values: [][]parser.Expression{
			{&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}
	_, err = c.CompileInsert(stmt)
	if err == nil {
		t.Fatal("CompileInsert on nonexistent table should return error")
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_CompileCreateTableDuplicateDirect covers the schema.CreateTable error
// path in CompileCreateTable (CompileCreateTable at 90.9%).
func TestMCDC3_CompileCreateTableDuplicateDirect(t *testing.T) {
	t.Parallel()
	// MC/DC for CompileCreateTable:
	//   C1: schema.CreateTable returns error → error propagated
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/cct.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err = db.Execute("CREATE TABLE dup2_t (id INTEGER)"); err != nil {
		t.Fatalf("first CREATE TABLE: %v", err)
	}
	_, err = db.Execute("CREATE TABLE dup2_t (id INTEGER)")
	if err == nil {
		t.Fatal("second CREATE TABLE with same name should return error")
	}
}

// TestMCDC3_CompileDropTableNotFound covers the table-not-found error path
// in CompileDropTable without IF EXISTS (CompileDropTable at 92.9%).
func TestMCDC3_CompileDropTableNotFound(t *testing.T) {
	t.Parallel()
	// MC/DC for CompileDropTable:
	//   C1: table not found AND IfExists=false → "table not found" error
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/dt.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("DROP TABLE absent_table")
	if err == nil {
		t.Fatal("DROP TABLE on nonexistent table should return error")
	}
	if !strings.Contains(err.Error(), "table not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_CompileDropIndexNotFound covers the index-not-found error path
// in CompileDropIndex without IF EXISTS (CompileDropIndex at 92.9%).
func TestMCDC3_CompileDropIndexNotFound(t *testing.T) {
	t.Parallel()
	// MC/DC for CompileDropIndex:
	//   C1: index not found AND IfExists=false → "index not found" error
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/di.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Execute("DROP INDEX absent_idx")
	if err == nil {
		t.Fatal("DROP INDEX on nonexistent index should return error")
	}
	if !strings.Contains(err.Error(), "index not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMCDC3_OpenWithOptionsBadPath covers the error path in OpenWithOptions
// when the file path is invalid (OpenWithOptions at 76.9%).
func TestMCDC3_OpenWithOptionsBadPath(t *testing.T) {
	t.Parallel()
	// MC/DC for OpenWithOptions:
	//   C1: pager.Open returns error → error propagated
	_, err := OpenWithOptions("/nonexistent_dir_xyz/sub/db.db", false)
	if err == nil {
		t.Fatal("OpenWithOptions with bad path should return error")
	}
}

// TestMCDC3_CloseNoTransaction covers the Close path when inTransaction=false
// (no rollback branch taken) (Close at 77.8%).
func TestMCDC3_CloseNoTransaction(t *testing.T) {
	t.Parallel()
	// MC/DC for Close:
	//   C1: e.inTransaction=false → skip rollback branch, close pager directly
	tmpDir := t.TempDir()
	db, err := Open(tmpDir + "/close.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("Close with no active transaction: %v", err)
	}
}
