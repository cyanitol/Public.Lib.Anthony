// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC 11 — internal driver injection tests
//
// Targets:
//   multi_stmt.go:133    execSingleStmt          (70.0%) — compile error return path
//   conn.go:727          RegisterVirtualTableModule  (71.4%) — closed + nil registry
//   conn.go:751          UnregisterVirtualTableModule (71.4%) — closed + nil registry
//   conn.go:418          reloadSchemaAfterRollback    (72.7%) — nil btree early return
//   compile_dml.go:713   affinityToOpCastCode     (71.4%) — BLOB and default cases
//   compile_dml.go:1919  generatedExprForColumn   (71.4%) — empty expr + parse error
//   compile_select_agg.go:784 emitBinaryOp        (87.5%) — default error case
//   stmt_window_helpers.go:329 resolveWindowStateIdx (71.4%) — nil windowStateMap

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// execSingleStmt — compile error path
// ---------------------------------------------------------------------------

// TestMCDC11_ExecSingleStmt_CompileError exercises the compile-error return
// in execSingleStmt. A Stmt with nil ast falls through to dispatchDDLOrTxn's
// default branch which returns "unsupported statement type: <nil>".
func TestMCDC11_ExecSingleStmt_CompileError(t *testing.T) {
	t.Parallel()

	// Minimal conn — stmtCache nil prevents cache path, mutex is zero-value valid.
	c := &Conn{}
	ms := &MultiStmt{conn: c}
	stmt := &Stmt{conn: c, ast: nil}

	_, err := ms.execSingleStmt(context.Background(), stmt, nil)
	if err == nil {
		t.Error("expected compile error for nil ast, got nil")
	}
}

// ---------------------------------------------------------------------------
// RegisterVirtualTableModule — closed connection path
// ---------------------------------------------------------------------------

// TestMCDC11_RegisterVirtualTableModule_Closed exercises the c.closed branch
// in RegisterVirtualTableModule, which returns driver.ErrBadConn.
func TestMCDC11_RegisterVirtualTableModule_Closed(t *testing.T) {
	t.Parallel()

	c := &Conn{closed: true}
	err := c.RegisterVirtualTableModule("mod", nil)
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// UnregisterVirtualTableModule — closed + nil registry paths
// ---------------------------------------------------------------------------

// TestMCDC11_UnregisterVirtualTableModule_Closed exercises the c.closed branch.
func TestMCDC11_UnregisterVirtualTableModule_Closed(t *testing.T) {
	t.Parallel()

	c := &Conn{closed: true}
	err := c.UnregisterVirtualTableModule("mod")
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got %v", err)
	}
}

// TestMCDC11_UnregisterVirtualTableModule_NilRegistry exercises the nil
// vtabRegistry branch in UnregisterVirtualTableModule.
func TestMCDC11_UnregisterVirtualTableModule_NilRegistry(t *testing.T) {
	t.Parallel()

	// vtabRegistry is nil by default in zero-value Conn.
	c := &Conn{}
	err := c.UnregisterVirtualTableModule("no_such_module")
	if err == nil {
		t.Error("expected error for nil vtabRegistry, got nil")
	}
}

// ---------------------------------------------------------------------------
// reloadSchemaAfterRollback — nil btree early return
// ---------------------------------------------------------------------------

// TestMCDC11_ReloadSchemaAfterRollback_NilBtree exercises the early-return
// when c.btree == nil (the btree==nil branch of the nil-guard).
func TestMCDC11_ReloadSchemaAfterRollback_NilBtree(t *testing.T) {
	t.Parallel()

	// btree is nil → function returns immediately without panic.
	c := &Conn{}
	c.reloadSchemaAfterRollback() // must not panic
}

// ---------------------------------------------------------------------------
// RegisterVirtualTableModule — nil registry initialization path
// ---------------------------------------------------------------------------

// TestMCDC11_RegisterVirtualTableModule_NilRegistryInit exercises the
// c.vtabRegistry == nil branch that creates a new ModuleRegistry on demand.
func TestMCDC11_RegisterVirtualTableModule_NilRegistryInit(t *testing.T) {
	t.Parallel()

	// vtabRegistry is nil by default → the function initializes it before use.
	c := &Conn{}
	err := c.RegisterVirtualTableModule("mcdc11_test_mod", nil)
	if err != nil {
		t.Errorf("unexpected error registering module with nil registry: %v", err)
	}
}

// ---------------------------------------------------------------------------
// affinityToOpCastCode — BLOB and default (none) cases
// These cases are guarded by the call site (only TEXT/INT/REAL/NUMERIC reach
// the function), so they are dead code reachable only via direct call.
// ---------------------------------------------------------------------------

// TestMCDC11_AffinityToOpCastCode_Blob exercises the BLOB affinity case.
func TestMCDC11_AffinityToOpCastCode_Blob(t *testing.T) {
	t.Parallel()
	if got := affinityToOpCastCode(schema.AffinityBlob); got != 1 {
		t.Errorf("AffinityBlob: expected 1, got %d", got)
	}
}

// TestMCDC11_AffinityToOpCastCode_None exercises the default (AffinityNone) case.
func TestMCDC11_AffinityToOpCastCode_None(t *testing.T) {
	t.Parallel()
	if got := affinityToOpCastCode(schema.AffinityNone); got != 0 {
		t.Errorf("AffinityNone: expected 0, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// generatedExprForColumn — empty expr and parse-error paths
// ---------------------------------------------------------------------------

// TestMCDC11_GeneratedExprForColumn_Empty exercises the empty GeneratedExpr path.
func TestMCDC11_GeneratedExprForColumn_Empty(t *testing.T) {
	t.Parallel()
	col := &schema.Column{GeneratedExpr: ""}
	expr := generatedExprForColumn(col)
	if expr == nil {
		t.Error("expected non-nil literal NULL expression")
	}
}

// TestMCDC11_GeneratedExprForColumn_ParseError exercises the parse-error path
// by providing a malformed expression string.
func TestMCDC11_GeneratedExprForColumn_ParseError(t *testing.T) {
	t.Parallel()
	col := &schema.Column{GeneratedExpr: "<<<invalid expr>>>"}
	expr := generatedExprForColumn(col)
	// Parse fails → returns literal NULL, not nil.
	if expr == nil {
		t.Error("expected non-nil literal NULL on parse error")
	}
}

// ---------------------------------------------------------------------------
// emitBinaryOp — default (unsupported operator) error path
// ---------------------------------------------------------------------------

// TestMCDC11_EmitBinaryOp_Default exercises the default branch in emitBinaryOp
// by calling it with an operator not in the arithmetic switch (OpConcat = ||).
// This path is dead code from SQL since the SQL path only calls emitBinaryOp
// with arithmetic operators, but the branch must be exercised for MC/DC coverage.
func TestMCDC11_EmitBinaryOp_Default(t *testing.T) {
	t.Parallel()
	s := &Stmt{}
	vm := vdbe.New()
	err := s.emitBinaryOp(vm, parser.OpConcat, 1, 2, 3)
	if err == nil {
		t.Error("expected error for unsupported operator, got nil")
	}
}

// ---------------------------------------------------------------------------
// resolveWindowStateIdx — nil windowStateMap early return
// ---------------------------------------------------------------------------

// TestMCDC11_ResolveWindowStateIdx_NilMap exercises the nil-windowStateMap
// branch in resolveWindowStateIdx which returns 0 immediately.
func TestMCDC11_ResolveWindowStateIdx_NilMap(t *testing.T) {
	t.Parallel()
	// s.windowStateMap is nil by default in zero-value Stmt.
	s := &Stmt{}
	over := &parser.WindowSpec{}
	fn := &parser.FunctionExpr{Name: "ROW_NUMBER", Over: over}
	idx := s.resolveWindowStateIdx(fn, nil)
	if idx != 0 {
		t.Errorf("expected 0 for nil windowStateMap, got %d", idx)
	}
}
