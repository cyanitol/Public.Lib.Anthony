// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// MC/DC 20b — internal driver tests for low-coverage helper functions.
//
// Targets:
//   collation_helpers.go:11  resolveExprCollation  (90%) — ParenExpr, IdentExpr nil table
//   compile_compound.go:662  emitLoadValue         (85.7%) — []byte and default paths
//   compile_dml.go:1143      dmlToInt64            (80%) — int path and default path

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// resolveExprCollation — ParenExpr and IdentExpr with nil table
// ---------------------------------------------------------------------------

// TestMCDC20b_ResolveExprCollation_ParenExpr exercises the ParenExpr branch.
func TestMCDC20b_ResolveExprCollation_ParenExpr(t *testing.T) {
	t.Parallel()

	inner := &parser.CollateExpr{Collation: "nocase", Expr: &parser.LiteralExpr{}}
	paren := &parser.ParenExpr{Expr: inner}

	got := resolveExprCollation(paren, nil)
	if got != "NOCASE" {
		t.Errorf("expected NOCASE, got %q", got)
	}
}

// TestMCDC20b_ResolveExprCollation_IdentNilTable exercises the IdentExpr nil-table path.
func TestMCDC20b_ResolveExprCollation_IdentNilTable(t *testing.T) {
	t.Parallel()

	ident := &parser.IdentExpr{Name: "col1"}
	got := resolveExprCollation(ident, nil)
	if got != "" {
		t.Errorf("expected empty string for nil table, got %q", got)
	}
}

// TestMCDC20b_ResolveExprCollation_IdentNotFound exercises IdentExpr with a
// table that does not contain the column.
func TestMCDC20b_ResolveExprCollation_IdentNotFound(t *testing.T) {
	t.Parallel()

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{
		{Name: "a", Collation: "BINARY"},
	}}
	ident := &parser.IdentExpr{Name: "nonexistent"}
	got := resolveExprCollation(ident, tbl)
	if got != "" {
		t.Errorf("expected empty for missing column, got %q", got)
	}
}

// TestMCDC20b_ResolveExprCollation_IdentFound exercises the IdentExpr found path.
func TestMCDC20b_ResolveExprCollation_IdentFound(t *testing.T) {
	t.Parallel()

	tbl := &schema.Table{Name: "t", Columns: []*schema.Column{
		{Name: "name", Collation: "NOCASE"},
	}}
	ident := &parser.IdentExpr{Name: "name"}
	got := resolveExprCollation(ident, tbl)
	if got != "NOCASE" {
		t.Errorf("expected NOCASE, got %q", got)
	}
}

// TestMCDC20b_ResolveExprCollation_Default exercises the default (other expr) path.
func TestMCDC20b_ResolveExprCollation_Default(t *testing.T) {
	t.Parallel()

	lit := &parser.LiteralExpr{Value: "test"}
	got := resolveExprCollation(lit, nil)
	if got != "" {
		t.Errorf("expected empty for literal expr, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// emitLoadValue — []byte and default paths
// ---------------------------------------------------------------------------

// TestMCDC20b_EmitLoadValue_Blob exercises the []byte branch in emitLoadValue.
func TestMCDC20b_EmitLoadValue_Blob(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	vm.AllocMemory(4)
	// []byte value triggers OpBlob path.
	emitLoadValue(vm, []byte{0xDE, 0xAD, 0xBE, 0xEF}, 1)
	// Verify an instruction was added without panic.
	if len(vm.Program) == 0 {
		t.Error("expected at least one instruction")
	}
}

// TestMCDC20b_EmitLoadValue_Default exercises the default branch with an unknown type.
func TestMCDC20b_EmitLoadValue_Default(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	vm.AllocMemory(4)
	// A struct{} value is not handled → default → OpNull.
	emitLoadValue(vm, struct{}{}, 1)
	if len(vm.Program) == 0 {
		t.Error("expected at least one instruction for default case")
	}
}

// ---------------------------------------------------------------------------
// dmlToInt64 — int and default paths
// ---------------------------------------------------------------------------

// TestMCDC20b_DmlToInt64_Int exercises the int (not int64) branch.
func TestMCDC20b_DmlToInt64_Int(t *testing.T) {
	t.Parallel()

	v, ok := dmlToInt64(int(42))
	if !ok {
		t.Error("expected ok=true for int")
	}
	if v != 42 {
		t.Errorf("expected 42, got %d", v)
	}
}

// TestMCDC20b_DmlToInt64_Default exercises the default (unrecognized type) branch.
func TestMCDC20b_DmlToInt64_Default(t *testing.T) {
	t.Parallel()

	_, ok := dmlToInt64("a string")
	if ok {
		t.Error("expected ok=false for string")
	}
}

// TestMCDC20b_DmlToInt64_Float64 exercises the float64 branch.
func TestMCDC20b_DmlToInt64_Float64(t *testing.T) {
	t.Parallel()

	v, ok := dmlToInt64(float64(3.7))
	if !ok {
		t.Error("expected ok=true for float64")
	}
	if v != 3 {
		t.Errorf("expected 3 (truncated), got %d", v)
	}
}
