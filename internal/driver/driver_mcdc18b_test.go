// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestMCDC18b_FindOpInitTarget_Empty covers the return 0 path in
// findOpInitTarget when the program contains no OpInit instruction.
func TestMCDC18b_FindOpInitTarget_Empty(t *testing.T) {
	t.Parallel()
	// MC/DC for findOpInitTarget:
	//   C1: no OpInit found → return 0
	vm := vdbe.New()

	result := findOpInitTarget(vm)
	if result != 0 {
		t.Errorf("expected 0 for empty program, got %d", result)
	}
}

// TestMCDC18b_FindOpInitTarget_Found covers the OpInit found path in
// findOpInitTarget; verifies the P2 value is returned.
func TestMCDC18b_FindOpInitTarget_Found(t *testing.T) {
	t.Parallel()
	// MC/DC for findOpInitTarget:
	//   C2: OpInit instruction present → return its P2
	vm := vdbe.New()
	// Add a non-OpInit instruction first, then OpInit.
	vm.AddOp(vdbe.OpNoop, 0, 0, 0)
	vm.AddOp(vdbe.OpInit, 0, 7, 0) // P2=7 is the jump target

	result := findOpInitTarget(vm)
	if result != 7 {
		t.Errorf("expected P2=7 from OpInit, got %d", result)
	}
}

// TestMCDC18b_NegateValue_String covers the default (non-numeric) path in
// negateValue with a string value; expects the value returned unchanged.
func TestMCDC18b_NegateValue_String(t *testing.T) {
	t.Parallel()
	// MC/DC for negateValue:
	//   C1: value is string → returned unchanged
	in := "hello"
	out := negateValue(in)
	if out != in {
		t.Errorf("negateValue(%q) = %v, want %q", in, out, in)
	}
}

// TestMCDC18b_NegateValue_Int64 covers the int64 case in negateValue.
func TestMCDC18b_NegateValue_Int64(t *testing.T) {
	t.Parallel()
	// MC/DC for negateValue:
	//   C2: value is int64 → negated
	in := int64(5)
	out := negateValue(in)
	got, ok := out.(int64)
	if !ok {
		t.Fatalf("negateValue(int64(5)) returned %T, want int64", out)
	}
	if got != int64(-5) {
		t.Errorf("negateValue(int64(5)) = %d, want -5", got)
	}
}

// TestMCDC18b_NegateValue_Float64 covers the float64 case in negateValue.
func TestMCDC18b_NegateValue_Float64(t *testing.T) {
	t.Parallel()
	// MC/DC for negateValue:
	//   C3: value is float64 → negated
	in := float64(3.14)
	out := negateValue(in)
	got, ok := out.(float64)
	if !ok {
		t.Fatalf("negateValue(float64(3.14)) returned %T, want float64", out)
	}
	if got != float64(-3.14) {
		t.Errorf("negateValue(float64(3.14)) = %f, want -3.14", got)
	}
}

// TestMCDC18b_NegateValue_Nil covers the nil case in negateValue; the default
// branch returns the value unchanged, so nil in → nil out.
func TestMCDC18b_NegateValue_Nil(t *testing.T) {
	t.Parallel()
	// MC/DC for negateValue:
	//   C4: value is nil → returned unchanged (default branch)
	out := negateValue(nil)
	if out != nil {
		t.Errorf("negateValue(nil) = %v, want nil", out)
	}
}

// TestMCDC18b_GetVirtualTable_NotRegistered covers the error path in
// getVirtualTable when the table's VirtualTable field does not implement
// vtab.VirtualTable (i.e., it is nil or the wrong type).
func TestMCDC18b_GetVirtualTable_NotRegistered(t *testing.T) {
	t.Parallel()
	// MC/DC for getVirtualTable:
	//   C1: table.VirtualTable does not implement vtab.VirtualTable → error returned
	tbl := &schema.Table{
		Name:         "ghost",
		IsVirtual:    true,
		VirtualTable: nil, // nil does not satisfy the vtab.VirtualTable interface
	}

	vt, err := getVirtualTable(tbl)
	if err == nil {
		t.Error("getVirtualTable with nil VirtualTable field should return error")
	}
	if vt != nil {
		t.Errorf("expected nil VirtualTable on error, got %v", vt)
	}
}

// TestMCDC18b_CopySelectStmtShallow covers copySelectStmtShallow, verifying
// it returns a distinct pointer with equal field values (shallow copy).
func TestMCDC18b_CopySelectStmtShallow(t *testing.T) {
	t.Parallel()
	// MC/DC for copySelectStmtShallow:
	//   C1: stmt != nil → shallow copy returned

	orig := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
	}

	copied := copySelectStmtShallow(orig)

	if copied == nil {
		t.Fatal("copySelectStmtShallow returned nil for non-nil input")
	}
	if copied == orig {
		t.Error("copySelectStmtShallow returned the same pointer, expected a new copy")
	}
	if len(copied.Columns) != len(orig.Columns) {
		t.Errorf("copied Columns len=%d, want %d", len(copied.Columns), len(orig.Columns))
	}
}

// TestMCDC18b_CopySelectStmtShallow_Nil covers the nil guard in copySelectStmtShallow.
func TestMCDC18b_CopySelectStmtShallow_Nil(t *testing.T) {
	t.Parallel()
	// MC/DC for copySelectStmtShallow:
	//   C2: stmt == nil → nil returned
	if got := copySelectStmtShallow(nil); got != nil {
		t.Errorf("copySelectStmtShallow(nil) = %v, want nil", got)
	}
}

// TestMCDC18b_FindAggregateInExpr_Found covers the aggregate-found path in
// findAggregateInExpr when the expression is an aggregate function.
func TestMCDC18b_FindAggregateInExpr_Found(t *testing.T) {
	t.Parallel()
	// MC/DC for findAggregateInExpr:
	//   C1: expr is *FunctionExpr with aggregate name → returns expr
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	s := &Stmt{conn: c}

	aggExpr := &parser.FunctionExpr{Name: "COUNT", Star: true}
	result := s.findAggregateInExpr(aggExpr)
	if result == nil {
		t.Error("findAggregateInExpr with COUNT aggregate should return non-nil")
	}
	if result != aggExpr {
		t.Errorf("expected same *FunctionExpr pointer back, got different")
	}
}

// TestMCDC18b_FindAggregateInExpr_NotFound covers the nil return path in
// findAggregateInExpr for a plain column expression (not an aggregate).
func TestMCDC18b_FindAggregateInExpr_NotFound(t *testing.T) {
	t.Parallel()
	// MC/DC for findAggregateInExpr:
	//   C2: expr is *IdentExpr (no aggregate) → returns nil
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	s := &Stmt{conn: c}

	colExpr := &parser.IdentExpr{Name: "id"}
	result := s.findAggregateInExpr(colExpr)
	if result != nil {
		t.Errorf("findAggregateInExpr with plain IdentExpr should return nil, got %v", result)
	}
}

// TestMCDC18b_FindAggregateInExpr_Nil covers the early nil-return path in
// findAggregateInExpr when expr is nil.
func TestMCDC18b_FindAggregateInExpr_Nil(t *testing.T) {
	t.Parallel()
	// MC/DC for findAggregateInExpr:
	//   C3: expr == nil → returns nil immediately
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	s := &Stmt{conn: c}

	result := s.findAggregateInExpr(nil)
	if result != nil {
		t.Errorf("findAggregateInExpr(nil) should return nil, got %v", result)
	}
}
