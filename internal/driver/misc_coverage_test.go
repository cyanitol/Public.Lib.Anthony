// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"errors"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// errStubPager is a pager stub whose VerifyFreeList always returns an error.
// ---------------------------------------------------------------------------

type errStubPager struct{}

func (p *errStubPager) Get(_ pager.Pgno) (*pager.DbPage, error) { return nil, nil }
func (p *errStubPager) Put(_ *pager.DbPage)                     {}
func (p *errStubPager) PageSize() int                           { return 4096 }
func (p *errStubPager) PageCount() pager.Pgno                   { return 0 }
func (p *errStubPager) IsReadOnly() bool                        { return false }
func (p *errStubPager) GetHeader() *pager.DatabaseHeader        { return nil }
func (p *errStubPager) GetFreePageCount() uint32                { return 0 }
func (p *errStubPager) Write(_ *pager.DbPage) error             { return nil }
func (p *errStubPager) AllocatePage() (pager.Pgno, error)       { return 0, nil }
func (p *errStubPager) FreePage(_ pager.Pgno) error             { return nil }
func (p *errStubPager) Vacuum(_ *pager.VacuumOptions) error     { return nil }
func (p *errStubPager) SetUserVersion(_ uint32) error           { return nil }
func (p *errStubPager) SetSchemaCookie(_ uint32) error          { return nil }
func (p *errStubPager) VerifyFreeList() error                   { return errors.New("free list error injected") }
func (p *errStubPager) BeginRead() error                        { return nil }
func (p *errStubPager) BeginWrite() error                       { return nil }
func (p *errStubPager) Rollback() error                         { return nil }
func (p *errStubPager) Savepoint(_ string) error                { return nil }
func (p *errStubPager) Release(_ string) error                  { return nil }
func (p *errStubPager) RollbackTo(_ string) error               { return nil }
func (p *errStubPager) Close() error                            { return nil }
func (p *errStubPager) InWriteTransaction() bool                { return false }
func (p *errStubPager) Commit() error                           { return nil }
func (p *errStubPager) EndRead() error                          { return nil }

// ---------------------------------------------------------------------------
// TestMiscCoverage_ToFloat64Value exercises all branches of toFloat64Value.
// ---------------------------------------------------------------------------

// TestMiscCoverage_ToFloat64Value covers the int branch and the (0, false)
// fallthrough that existing tests leave uncovered.
func TestMiscCoverage_ToFloat64Value(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   interface{}
		wantVal float64
		wantOK  bool
	}{
		{"float64", float64(3.14), 3.14, true},
		{"int64", int64(7), 7.0, true},
		{"int", int(5), 5.0, true},
		{"string fallthrough", "hello", 0, false},
		{"nil fallthrough", nil, 0, false},
		{"bool fallthrough", true, 0, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := toFloat64Value(tc.input)
			if ok != tc.wantOK {
				t.Errorf("toFloat64Value(%v): ok=%v, want %v", tc.input, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("toFloat64Value(%v): val=%v, want %v", tc.input, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestMiscCoverage_RunIntegrityCheck exercises both branches of runIntegrityCheck.
// ---------------------------------------------------------------------------

// TestMiscCoverage_RunIntegrityCheck_OK exercises the happy path ("ok" return).
func TestMiscCoverage_RunIntegrityCheck_OK(t *testing.T) {
	t.Parallel()
	c := openMemConn(t)
	s := stmtFor(c)
	result := s.runIntegrityCheck()
	if result != "ok" {
		t.Errorf("runIntegrityCheck on clean db: want %q, got %q", "ok", result)
	}
}

// TestMiscCoverage_RunIntegrityCheck_Error exercises the error path by replacing
// the connection's pager with a stub that returns an error from VerifyFreeList.
func TestMiscCoverage_RunIntegrityCheck_Error(t *testing.T) {
	t.Parallel()
	c := openMemConn(t)
	// Inject a pager that always returns an error from VerifyFreeList.
	c.pager = &errStubPager{}
	s := stmtFor(c)
	result := s.runIntegrityCheck()
	if !strings.HasPrefix(result, "*** free list corruption:") {
		t.Errorf("runIntegrityCheck with error pager: want prefix %q, got %q",
			"*** free list corruption:", result)
	}
}

// ---------------------------------------------------------------------------
// TestMiscCoverage_EmitGeneratedExpr exercises the same-register branch.
// ---------------------------------------------------------------------------

// TestMiscCoverage_EmitGeneratedExpr_SameRegNoOp exercises the implicit
// no-op branch (err==nil && reg==colIdx) of emitGeneratedExpr.
//
// NewCodeGenerator starts nextReg at 1, so the first AllocReg() call returns 1.
// Passing colIdx=1 makes reg==colIdx, hitting the branch where no OpCopy is emitted.
func TestMiscCoverage_EmitGeneratedExpr_SameRegNoOp(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	// A literal integer – GenerateExpr allocates register 1 (nextReg starts at 1).
	// Passing colIdx=1 means reg==colIdx, so no OpCopy should be emitted.
	e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	before := len(vm.Program)
	s.emitGeneratedExpr(vm, gen, e, 1)
	after := len(vm.Program)

	// At least the literal-load instruction should be present.
	if after <= before {
		t.Error("emitGeneratedExpr SameReg: expected at least one instruction emitted")
	}
	// No OpCopy targeting colIdx 1 should be emitted when reg==colIdx.
	for _, instr := range vm.Program[before:] {
		if instr.Opcode == vdbe.OpCopy && instr.P2 == 1 {
			t.Error("emitGeneratedExpr SameReg: unexpected OpCopy targeting colIdx 1")
		}
	}
}

// TestMiscCoverage_EmitGeneratedExpr_ErrorBranch exercises the err!=nil branch
// (vm.AddOp(OpNull, ...)) of emitGeneratedExpr. Using an IdentExpr with an
// explicit table qualifier that has no registered cursor causes generateColumn
// to return an error, which reaches the OpNull fallback.
func TestMiscCoverage_EmitGeneratedExpr_ErrorBranch(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)

	// IdentExpr with explicit unknown table qualifier → generateColumn returns error.
	e := &parser.IdentExpr{Name: "col", Table: "no_such_table_xyz"}
	colIdx := 2
	before := len(vm.Program)
	s.emitGeneratedExpr(vm, gen, e, colIdx)
	after := len(vm.Program)

	// An OpNull targeting colIdx should have been emitted.
	if after <= before {
		t.Fatal("emitGeneratedExpr ErrorBranch: expected at least one instruction")
	}
	found := false
	for _, instr := range vm.Program[before:] {
		if instr.Opcode == vdbe.OpNull && instr.P2 == colIdx {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("emitGeneratedExpr ErrorBranch: expected OpNull targeting colIdx %d", colIdx)
	}
}

// ---------------------------------------------------------------------------
// TestMiscCoverage_EmitJSONGroupObjectUpdate exercises uncovered branches.
// ---------------------------------------------------------------------------

// makeMinimalTable builds a *schema.Table with a single column named "k".
func makeMinimalTable() *schema.Table {
	return &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "k", Type: "TEXT"},
		},
	}
}

// TestMiscCoverage_EmitJSONGroupObjectUpdate_KeyRegNegative exercises the
// keyReg < 0 early-return branch (lines 320-321). The first argument is an
// IdentExpr with an explicit unknown table qualifier, so generateColumn returns
// an error and loadJSONExprValue returns -1.
func TestMiscCoverage_EmitJSONGroupObjectUpdate_KeyRegNegative(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)
	tbl := makeMinimalTable()

	fn := &parser.FunctionExpr{
		Name: "json_group_object",
		Args: []parser.Expression{
			// First arg with unknown table qualifier → loadJSONExprValue returns -1.
			&parser.IdentExpr{Name: "col", Table: "no_such_table_xyz"},
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		},
	}
	before := len(vm.Program)
	s.emitJSONGroupObjectUpdate(vm, fn, tbl, "t", 5, gen)
	// No opcodes should be emitted (early return after keyReg < 0 check).
	if len(vm.Program) != before {
		t.Errorf("emitJSONGroupObjectUpdate KeyRegNegative: expected no opcodes, got %d new",
			len(vm.Program)-before)
	}
}

// TestMiscCoverage_EmitJSONGroupObjectUpdate_TooFewArgs exercises the
// len(args) < 2 early-return branch (line 314-315).
func TestMiscCoverage_EmitJSONGroupObjectUpdate_TooFewArgs(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)
	tbl := makeMinimalTable()

	// One argument only – the function should return immediately without panic.
	fn := &parser.FunctionExpr{
		Name: "json_group_object",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralString, Value: "key"},
		},
	}
	before := len(vm.Program)
	s.emitJSONGroupObjectUpdate(vm, fn, tbl, "t", 5, gen)
	// No opcodes should be emitted when there are fewer than 2 args.
	if len(vm.Program) != before {
		t.Errorf("emitJSONGroupObjectUpdate TooFewArgs: expected no opcodes, got %d new",
			len(vm.Program)-before)
	}
}

// TestMiscCoverage_EmitJSONGroupObjectUpdate_ValRegNegative exercises the
// valReg < 0 early-return branch (lines 329-331). The first argument resolves
// successfully (literal integer) and the second argument references a table
// qualifier that has no registered cursor, causing generateColumn to fail.
func TestMiscCoverage_EmitJSONGroupObjectUpdate_ValRegNegative(t *testing.T) {
	t.Parallel()
	s := newMiscCovStmt(t)
	vm := vdbe.New()
	vm.AllocMemory(10)
	gen := expr.NewCodeGenerator(vm)
	tbl := makeMinimalTable()

	// First arg: a literal that generates successfully.
	// Second arg: an IdentExpr with an explicit table qualifier that has no
	// registered cursor → resolveTableForColumn returns an error → loadJSONExprValue returns -1.
	fn := &parser.FunctionExpr{
		Name: "json_group_object",
		Args: []parser.Expression{
			&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
			&parser.IdentExpr{Name: "col", Table: "no_such_table_xyz"},
		},
	}

	// Should complete without panic, patching the skipAddr back-reference.
	s.emitJSONGroupObjectUpdate(vm, fn, tbl, "t", 5, gen)
	// If we get here without a panic or index out-of-range, the branch is covered.
}
