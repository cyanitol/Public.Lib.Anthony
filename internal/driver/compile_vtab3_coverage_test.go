// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// compile_vtab3_coverage_test.go exercises uncovered branches in compile_vtab.go:
//
//   emitInterfaceValue (line 585)  — case int, case []byte, default (bool)
//   collectVTabRows (line 336)     — Rowid() error, Column() error, Next() error
//   extractVTabColumnName (l. 317) — IdentExpr path (ok=true)
//   extractVTabOrderByName (l. 436)— IdentExpr path (ok=true)
//
// These functions are unexported, so the test must be in package driver.

import (
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// ============================================================================
// emitInterfaceValue — unit tests for the uncovered Go type branches.
//
// emitInterfaceValue(vm, val, reg) switches on the concrete type of val.
// The already-covered branches are: nil, int64, float64, string.
// The uncovered branches are: int, []byte, default (any other type, e.g. bool).
// ============================================================================

// newCv3VM returns a minimal VDBE suitable for emitInterfaceValue tests.
func newCv3VM() *vdbe.VDBE {
	vm := vdbe.New()
	vm.AllocMemory(8)
	return vm
}

// TestCompileVtab3Coverage_EmitInterfaceValue_Int exercises the "case int"
// branch of emitInterfaceValue (compile_vtab.go line 591-592).
// emitInterfaceValue converts the Go int to int64 and calls emitIntValue.
func TestCompileVtab3Coverage_EmitInterfaceValue_Int(t *testing.T) {
	vm := newCv3VM()
	// Pass a Go int (not int64) — exercises the "case int" branch.
	emitInterfaceValue(vm, int(99), 0)
	// If no panic, the branch was exercised.
	if len(vm.Program) == 0 {
		t.Error("emitInterfaceValue int: expected at least one instruction")
	}
}

// TestCompileVtab3Coverage_EmitInterfaceValue_Int_Large exercises the "case int"
// branch with a value exceeding int32 range, ensuring emitIntValue takes the
// OpInt64 sub-branch.
func TestCompileVtab3Coverage_EmitInterfaceValue_Int_Large(t *testing.T) {
	vm := newCv3VM()
	emitInterfaceValue(vm, int(3_000_000_000), 0)
	if len(vm.Program) == 0 {
		t.Error("emitInterfaceValue large int: expected at least one instruction")
	}
}

// TestCompileVtab3Coverage_EmitInterfaceValue_Bytes exercises the "case []byte"
// branch of emitInterfaceValue (compile_vtab.go line 597-599).
// emitInterfaceValue converts the []byte to a string and calls AddOpWithP4Str.
func TestCompileVtab3Coverage_EmitInterfaceValue_Bytes(t *testing.T) {
	vm := newCv3VM()
	emitInterfaceValue(vm, []byte("hello blob"), 0)
	if len(vm.Program) == 0 {
		t.Error("emitInterfaceValue []byte: expected at least one instruction")
	}
}

// TestCompileVtab3Coverage_EmitInterfaceValue_Default exercises the "default"
// branch of emitInterfaceValue (compile_vtab.go line 599-601) using a bool
// value.  The default case calls fmt.Sprintf("%v", v) and uses AddOpWithP4Str.
func TestCompileVtab3Coverage_EmitInterfaceValue_Default(t *testing.T) {
	vm := newCv3VM()
	emitInterfaceValue(vm, true, 0)
	if len(vm.Program) == 0 {
		t.Error("emitInterfaceValue default (bool): expected at least one instruction")
	}
}

// TestCompileVtab3Coverage_EmitInterfaceValue_Default_Struct exercises the
// "default" branch with a struct type to confirm any non-handled type is covered.
func TestCompileVtab3Coverage_EmitInterfaceValue_Default_Struct(t *testing.T) {
	vm := newCv3VM()
	type customType struct{ X int }
	emitInterfaceValue(vm, customType{X: 7}, 1)
	if len(vm.Program) == 0 {
		t.Error("emitInterfaceValue default (struct): expected at least one instruction")
	}
}

// ============================================================================
// collectVTabRows — unit tests for the error-return branches.
//
// collectVTabRows iterates a VirtualCursor and builds rows.  Three error paths
// exist: Rowid() error (line 344), Column() error (line 350), Next() error (357).
// ============================================================================

// cv3RowidErrCursor is a VirtualCursor with one row whose Rowid() always errors.
type cv3RowidErrCursor struct {
	vtab.BaseCursor
	done bool
}

func (c *cv3RowidErrCursor) Filter(_ int, _ string, _ []interface{}) error {
	c.done = false
	return nil
}
func (c *cv3RowidErrCursor) EOF() bool                         { return c.done }
func (c *cv3RowidErrCursor) Next() error                       { c.done = true; return nil }
func (c *cv3RowidErrCursor) Column(_ int) (interface{}, error) { return "v", nil }
func (c *cv3RowidErrCursor) Rowid() (int64, error) {
	return 0, fmt.Errorf("cv3RowidErrCursor: Rowid fails")
}

// TestCompileVtab3Coverage_CollectVTabRows_RowidError exercises the
// cursor.Rowid() error return path in collectVTabRows (line 344).
// colIndices contains -1 which routes through the Rowid() code path.
func TestCompileVtab3Coverage_CollectVTabRows_RowidError(t *testing.T) {
	c := &cv3RowidErrCursor{}
	// colIndices = [-1] tells collectVTabRows to call cursor.Rowid() for col 0.
	_, err := collectVTabRows(c, []int{-1})
	if err == nil {
		t.Error("collectVTabRows RowidError: expected error, got nil")
	}
}

// cv3ColumnErrCursor is a VirtualCursor with one row whose Column() always errors.
type cv3ColumnErrCursor struct {
	vtab.BaseCursor
	done bool
}

func (c *cv3ColumnErrCursor) Filter(_ int, _ string, _ []interface{}) error {
	c.done = false
	return nil
}
func (c *cv3ColumnErrCursor) EOF() bool   { return c.done }
func (c *cv3ColumnErrCursor) Next() error { c.done = true; return nil }
func (c *cv3ColumnErrCursor) Column(_ int) (interface{}, error) {
	return nil, fmt.Errorf("cv3ColumnErrCursor: Column fails")
}
func (c *cv3ColumnErrCursor) Rowid() (int64, error) { return 1, nil }

// TestCompileVtab3Coverage_CollectVTabRows_ColumnError exercises the
// cursor.Column() error return path in collectVTabRows (line 350).
// colIndices contains 0 (>= 0) which routes through the Column() code path.
func TestCompileVtab3Coverage_CollectVTabRows_ColumnError(t *testing.T) {
	c := &cv3ColumnErrCursor{}
	_, err := collectVTabRows(c, []int{0})
	if err == nil {
		t.Error("collectVTabRows ColumnError: expected error, got nil")
	}
}

// cv3NextErrCursor is a VirtualCursor with one row; Next() always returns an error.
type cv3NextErrCursor struct {
	vtab.BaseCursor
	pos int
}

func (c *cv3NextErrCursor) Filter(_ int, _ string, _ []interface{}) error {
	c.pos = 0
	return nil
}
func (c *cv3NextErrCursor) EOF() bool { return c.pos > 0 }
func (c *cv3NextErrCursor) Next() error {
	c.pos++
	return fmt.Errorf("cv3NextErrCursor: Next fails")
}
func (c *cv3NextErrCursor) Column(_ int) (interface{}, error) { return "row", nil }
func (c *cv3NextErrCursor) Rowid() (int64, error)             { return int64(c.pos + 1), nil }

// TestCompileVtab3Coverage_CollectVTabRows_NextError exercises the
// cursor.Next() error return path in collectVTabRows (line 357).
// The cursor delivers one row then Next() errors.
func TestCompileVtab3Coverage_CollectVTabRows_NextError(t *testing.T) {
	c := &cv3NextErrCursor{}
	_, err := collectVTabRows(c, []int{0})
	if err == nil {
		t.Error("collectVTabRows NextError: expected error, got nil")
	}
}

// cv3EmptyCursor is a VirtualCursor that is always at EOF with no rows.
type cv3EmptyCursor struct{ vtab.BaseCursor }

func (c *cv3EmptyCursor) EOF() bool { return true }

// TestCompileVtab3Coverage_CollectVTabRows_Empty exercises the zero-iteration
// path of collectVTabRows (cursor.EOF() is true from the first call).
func TestCompileVtab3Coverage_CollectVTabRows_Empty(t *testing.T) {
	c := &cv3EmptyCursor{}
	rows, err := collectVTabRows(c, []int{0})
	if err != nil {
		t.Errorf("collectVTabRows empty: unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("collectVTabRows empty: expected 0 rows, got %d", len(rows))
	}
}

// ============================================================================
// extractVTabColumnName — unit tests for both branches.
//
// extractVTabColumnName(col ResultColumn) string returns col.Expr.(*parser.IdentExpr).Name
// when the cast succeeds (ok=true), or "" otherwise (ok=false).
// The ok=false path was covered by TestCompileVtab2Coverage_ExtractVTabColumnName_NonIdent.
// The ok=true path is exercised here via a direct unit call.
// ============================================================================

// TestCompileVtab3Coverage_ExtractVTabColumnName_IdentTrue exercises the
// IdentExpr (ok=true) branch of extractVTabColumnName directly.
func TestCompileVtab3Coverage_ExtractVTabColumnName_IdentTrue(t *testing.T) {
	col := parser.ResultColumn{
		Expr: &parser.IdentExpr{Name: "mycolumn"},
	}
	got := extractVTabColumnName(col)
	if got != "mycolumn" {
		t.Errorf("extractVTabColumnName IdentExpr: want %q, got %q", "mycolumn", got)
	}
}

// TestCompileVtab3Coverage_ExtractVTabColumnName_NonIdentFalse exercises the
// non-IdentExpr (ok=false) branch of extractVTabColumnName directly.
func TestCompileVtab3Coverage_ExtractVTabColumnName_NonIdentFalse(t *testing.T) {
	col := parser.ResultColumn{
		Expr: &parser.LiteralExpr{Value: "42"},
	}
	got := extractVTabColumnName(col)
	if got != "" {
		t.Errorf("extractVTabColumnName non-IdentExpr: want %q, got %q", "", got)
	}
}

// ============================================================================
// extractVTabOrderByName — unit tests for both branches.
//
// extractVTabOrderByName(ob OrderingTerm) string returns ob.Expr.(*parser.IdentExpr).Name
// when the cast succeeds (ok=true), or "" otherwise (ok=false).
// The ok=false path was covered by TestCompileVtab2Coverage_ExtractVTabOrderByName_Literal.
// The ok=true path is exercised here via a direct unit call.
// ============================================================================

// TestCompileVtab3Coverage_ExtractVTabOrderByName_IdentTrue exercises the
// IdentExpr (ok=true) branch of extractVTabOrderByName directly.
func TestCompileVtab3Coverage_ExtractVTabOrderByName_IdentTrue(t *testing.T) {
	ob := parser.OrderingTerm{
		Expr: &parser.IdentExpr{Name: "price"},
		Asc:  true,
	}
	got := extractVTabOrderByName(ob)
	if got != "price" {
		t.Errorf("extractVTabOrderByName IdentExpr: want %q, got %q", "price", got)
	}
}

// TestCompileVtab3Coverage_ExtractVTabOrderByName_NonIdentFalse exercises the
// non-IdentExpr (ok=false) branch of extractVTabOrderByName directly.
func TestCompileVtab3Coverage_ExtractVTabOrderByName_NonIdentFalse(t *testing.T) {
	ob := parser.OrderingTerm{
		Expr: &parser.LiteralExpr{Value: "1"},
	}
	got := extractVTabOrderByName(ob)
	if got != "" {
		t.Errorf("extractVTabOrderByName non-IdentExpr: want %q, got %q", "", got)
	}
}

// ============================================================================
// Combination test — emitVTabBytecode with int, []byte, bool rows.
//
// emitVTabBytecode calls emitInterfaceValue for every cell in every row.
// Building a [][]interface{} with the three uncovered types and calling
// emitVTabBytecode exercises all three branches in one shot.
// ============================================================================

// TestCompileVtab3Coverage_EmitVTabBytecode_MixedTypes exercises emitVTabBytecode
// with rows containing int, []byte, and bool values, hitting all uncovered
// branches of emitInterfaceValue through the higher-level function.
func TestCompileVtab3Coverage_EmitVTabBytecode_MixedTypes(t *testing.T) {
	vm := vdbe.New()
	rows := [][]interface{}{
		{int(1)},         // case int
		{[]byte("data")}, // case []byte
		{true},           // default (bool)
	}
	colNames := []string{"val"}
	_, err := emitVTabBytecode(vm, rows, colNames)
	if err != nil {
		t.Fatalf("emitVTabBytecode mixed types: unexpected error: %v", err)
	}
	// Verify instructions were emitted.
	if len(vm.Program) == 0 {
		t.Error("emitVTabBytecode mixed types: expected instructions, got none")
	}
}
