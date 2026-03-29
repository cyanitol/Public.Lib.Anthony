// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC 21b — internal driver tests for low-coverage helper functions.
//
// Targets:
//   compile_select_agg.go:62  loadCountValueReg  — len(fnExpr.Args)==0 branch
//   compile_tvf.go:103        evalLiteralExpr    — VariableExpr and default paths
//   compile_helpers.go:667    buildMultiTableColumnNames — non-ident expr branch
//   compile_helpers.go:418    createTableInfoFromRef — schema-qualified and temp paths

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// loadCountValueReg — len(fnExpr.Args)==0 branch
// ---------------------------------------------------------------------------

// TestMCDC21b_LoadCountValueReg_EmptyArgs exercises the branch at line 68
// where len(fnExpr.Args)==0, causing loadAggregateColumnValue to return ok=false
// and then the no-args fast path to emit OpAddImm.
func TestMCDC21b_LoadCountValueReg_EmptyArgs(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	vm := vdbe.New()
	vm.AllocMemory(10)

	// Pre-add an instruction so vm.Program is non-empty (line 69 reads the last instruction).
	vm.AddOp(vdbe.OpAddImm, 5, 1, 0)

	gen := expr.NewCodeGenerator(vm)

	// FunctionExpr with no Args, no Star — loadAggregateColumnValue returns false
	// because len(fnExpr.Args)==0, then loadCountValueReg hits the len==0 branch.
	fnExpr := &parser.FunctionExpr{
		Name: "count",
		Args: nil,
		Star: false,
	}

	before := len(vm.Program)
	reg, skipAddr := s.loadCountValueReg(vm, fnExpr, nil, "", gen)

	// The no-args path emits one OpAddImm and returns (0, 0).
	if reg != 0 || skipAddr != 0 {
		t.Errorf("expected (0,0) for empty args, got (%d,%d)", reg, skipAddr)
	}
	if len(vm.Program) <= before {
		t.Error("expected at least one instruction emitted by the no-args path")
	}
}

// TestMCDC21b_LoadCountValueReg_NonIdentArg exercises the branch at line 73
// where len(fnExpr.Args)>0 but Args[0] is not an IdentExpr (so
// loadAggregateColumnValue returns ok=false), and falls through to GenerateExpr.
func TestMCDC21b_LoadCountValueReg_NonIdentArg(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	// Use a LiteralExpr as the arg — not an IdentExpr, so loadAggregateColumnValue
	// returns ok=false, and len(Args)>0, so GenerateExpr is called.
	fnExpr := &parser.FunctionExpr{
		Name: "count",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
		Star: false,
	}

	reg, _ := s.loadCountValueReg(vm, fnExpr, nil, "", gen)
	// GenerateExpr succeeds and returns a valid register.
	if reg < 0 {
		t.Errorf("expected non-negative register, got %d", reg)
	}
}

// ---------------------------------------------------------------------------
// evalLiteralExpr — VariableExpr and default paths
// ---------------------------------------------------------------------------

// TestMCDC21b_EvalLiteralExpr_LiteralInteger exercises the LiteralInteger path.
func TestMCDC21b_EvalLiteralExpr_LiteralInteger(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"}
	val, err := evalLiteralExpr(lit, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val == nil {
		t.Error("expected non-nil Value for integer literal")
	}
}

// TestMCDC21b_EvalLiteralExpr_LiteralFloat exercises the LiteralFloat path.
func TestMCDC21b_EvalLiteralExpr_LiteralFloat(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "3.14"}
	val, err := evalLiteralExpr(lit, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val == nil {
		t.Error("expected non-nil Value for float literal")
	}
}

// TestMCDC21b_EvalLiteralExpr_LiteralString exercises the LiteralString path.
func TestMCDC21b_EvalLiteralExpr_LiteralString(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}
	val, err := evalLiteralExpr(lit, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val == nil {
		t.Error("expected non-nil Value for string literal")
	}
}

// TestMCDC21b_EvalLiteralExpr_LiteralNull exercises the LiteralNull (default) path.
func TestMCDC21b_EvalLiteralExpr_LiteralNull(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Type: parser.LiteralNull, Value: ""}
	val, err := evalLiteralExpr(lit, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val == nil {
		t.Error("expected non-nil (null) Value for null literal")
	}
}

// TestMCDC21b_EvalLiteralExpr_VariableExpr exercises the VariableExpr path
// with a positional bind parameter.
func TestMCDC21b_EvalLiteralExpr_VariableExpr(t *testing.T) {
	t.Parallel()

	varExpr := &parser.VariableExpr{Name: ""}
	args := []driver.NamedValue{
		{Ordinal: 1, Value: int64(99)},
	}
	val, err := evalLiteralExpr(varExpr, args)
	if err != nil {
		t.Fatalf("unexpected error for VariableExpr: %v", err)
	}
	if val == nil {
		t.Error("expected non-nil Value for VariableExpr")
	}
}

// TestMCDC21b_EvalLiteralExpr_VariableExpr_NoArgs exercises the VariableExpr
// path when no args are provided — should return an error.
func TestMCDC21b_EvalLiteralExpr_VariableExpr_NoArgs(t *testing.T) {
	t.Parallel()

	varExpr := &parser.VariableExpr{Name: "x"}
	_, err := evalLiteralExpr(varExpr, nil)
	if err == nil {
		t.Error("expected error for VariableExpr with no args")
	}
}

// TestMCDC21b_EvalLiteralExpr_Default exercises the default (unknown) path.
func TestMCDC21b_EvalLiteralExpr_Default(t *testing.T) {
	t.Parallel()

	// Use an IdentExpr — not a LiteralExpr or VariableExpr, so hits default.
	ident := &parser.IdentExpr{Name: "col"}
	_, err := evalLiteralExpr(ident, nil)
	if err == nil {
		t.Error("expected error for unsupported expression type in evalLiteralExpr")
	}
}

// ---------------------------------------------------------------------------
// buildMultiTableColumnNames — non-ident expr branch (column%d fallback)
// ---------------------------------------------------------------------------

// TestMCDC21b_BuildMultiTableColumnNames_NonIdentExpr exercises the fallback
// branch at line 682 where the expression is not an IdentExpr.
func TestMCDC21b_BuildMultiTableColumnNames_NonIdentExpr(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{
		{Name: "a"},
		{Name: "b"},
	}}
	tables := []stmtTableInfo{
		{name: "t", table: tbl, cursorIdx: 0},
	}

	// A LiteralExpr is not an IdentExpr — falls to the column%d branch.
	cols := []parser.ResultColumn{
		{
			Expr:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			Alias: "",
			Star:  false,
		},
	}

	names := s.buildMultiTableColumnNames(cols, tables)
	if len(names) != 1 {
		t.Fatalf("expected 1 name, got %d: %v", len(names), names)
	}
	if names[0] != "column1" {
		t.Errorf("expected 'column1', got %q", names[0])
	}
}

// TestMCDC21b_BuildMultiTableColumnNames_Alias exercises the alias branch.
func TestMCDC21b_BuildMultiTableColumnNames_Alias(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{{Name: "x"}}}
	tables := []stmtTableInfo{{name: "t", table: tbl, cursorIdx: 0}}

	cols := []parser.ResultColumn{
		{
			Expr:  &parser.IdentExpr{Name: "x"},
			Alias: "my_alias",
			Star:  false,
		},
	}

	names := s.buildMultiTableColumnNames(cols, tables)
	if len(names) != 1 || names[0] != "my_alias" {
		t.Errorf("expected ['my_alias'], got %v", names)
	}
}

// TestMCDC21b_BuildMultiTableColumnNames_Star exercises the SELECT * branch.
func TestMCDC21b_BuildMultiTableColumnNames_Star(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{
		{Name: "id"},
		{Name: "name"},
	}}
	tables := []stmtTableInfo{{name: "t", table: tbl, cursorIdx: 0}}

	cols := []parser.ResultColumn{
		{Star: true},
	}

	names := s.buildMultiTableColumnNames(cols, tables)
	if len(names) != 2 || names[0] != "id" || names[1] != "name" {
		t.Errorf("expected [id name], got %v", names)
	}
}

// TestMCDC21b_BuildMultiTableColumnNames_IdentExpr exercises the IdentExpr branch.
func TestMCDC21b_BuildMultiTableColumnNames_IdentExpr(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{{Name: "val"}}}
	tables := []stmtTableInfo{{name: "t", table: tbl, cursorIdx: 0}}

	cols := []parser.ResultColumn{
		{
			Expr:  &parser.IdentExpr{Name: "val"},
			Alias: "",
			Star:  false,
		},
	}

	names := s.buildMultiTableColumnNames(cols, tables)
	if len(names) != 1 || names[0] != "val" {
		t.Errorf("expected ['val'], got %v", names)
	}
}

// ---------------------------------------------------------------------------
// createTableInfoFromRef — schema-qualified not-found and temp table paths
// ---------------------------------------------------------------------------

// TestMCDC21b_CreateTableInfoFromRef_SchemaQualifiedNotFound exercises the
// tableRef.Schema != "" error path (line 422).
func TestMCDC21b_CreateTableInfoFromRef_SchemaQualifiedNotFound(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	ref := parser.TableOrSubquery{
		Schema:    "nonexistent_schema",
		TableName: "nonexistent_table",
	}

	_, err := s.createTableInfoFromRef(ref, 0)
	if err == nil {
		t.Error("expected error for schema-qualified nonexistent table")
	}
}

// TestMCDC21b_CreateTableInfoFromRef_UnqualifiedNotFound exercises the
// unqualified not-found error path (line 424).
func TestMCDC21b_CreateTableInfoFromRef_UnqualifiedNotFound(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	s := stmtFor(conn)

	ref := parser.TableOrSubquery{
		Schema:    "",
		TableName: "no_such_table",
	}

	_, err := s.createTableInfoFromRef(ref, 0)
	if err == nil {
		t.Error("expected error for unqualified nonexistent table")
	}
}

// TestMCDC21b_CreateTableInfoFromRef_Success exercises the happy path with
// an existing table and an alias.
func TestMCDC21b_CreateTableInfoFromRef_Success(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	if err := conn.ExecDDL("CREATE TABLE ref_test (id INTEGER)"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := stmtFor(conn)

	ref := parser.TableOrSubquery{
		TableName: "ref_test",
		Alias:     "rt",
	}

	info, err := s.createTableInfoFromRef(ref, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.name != "rt" {
		t.Errorf("expected alias 'rt', got %q", info.name)
	}
	if info.cursorIdx != 2 {
		t.Errorf("expected cursorIdx=2, got %d", info.cursorIdx)
	}
}

// TestMCDC21b_CreateTableInfoFromRef_NoAlias exercises the no-alias path where
// tableAlias falls back to TableName.
func TestMCDC21b_CreateTableInfoFromRef_NoAlias(t *testing.T) {
	t.Parallel()

	conn := openMemConn(t)
	if err := conn.ExecDDL("CREATE TABLE noalias_tbl (id INTEGER)"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := stmtFor(conn)

	ref := parser.TableOrSubquery{
		TableName: "noalias_tbl",
		Alias:     "",
	}

	info, err := s.createTableInfoFromRef(ref, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.name != "noalias_tbl" {
		t.Errorf("expected name 'noalias_tbl', got %q", info.name)
	}
}
