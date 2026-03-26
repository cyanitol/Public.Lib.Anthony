// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// Direct unit tests for the five dead-code functions in stmt_window_helpers.go
// that are never called from production code.

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// newWindowHelperStmt opens an in-memory Conn and returns a *Stmt.
func newWindowHelperStmt(t *testing.T) *Stmt {
	t.Helper()
	conn := openMemConn(t)
	return stmtFor(conn)
}

// testTable returns a simple schema.Table with three columns.
func testTable() *schema.Table {
	return &schema.Table{
		Name: "employees",
		Columns: []*schema.Column{
			{Name: "id"},
			{Name: "salary"},
			{Name: "name"},
		},
	}
}

// testRankRegisters returns a rankRegisters suitable for testing.
func testRankRegisters() rankRegisters {
	return initWindowRankRegisters(5)
}

// ---------------------------------------------------------------------------
// extractWindowOrderByCols
// ---------------------------------------------------------------------------

// TestWindowHelperDeadCode_ExtractOrderByCols_IdentExpr verifies that an
// IdentExpr whose name matches a column in the table produces a non-empty slice.
func TestWindowHelperDeadCode_ExtractOrderByCols_IdentExpr(t *testing.T) {
	t.Parallel()
	s := newWindowHelperStmt(t)
	table := testTable()

	orderBy := []parser.OrderingTerm{
		{Expr: &parser.IdentExpr{Name: "salary"}},
		{Expr: &parser.IdentExpr{Name: "name"}},
	}

	cols := s.extractWindowOrderByCols(orderBy, table)
	if len(cols) != 2 {
		t.Fatalf("extractWindowOrderByCols: want 2 cols, got %d", len(cols))
	}
	// "salary" is column index 1, "name" is index 2
	if cols[0] != 1 {
		t.Errorf("extractWindowOrderByCols: cols[0] = %d, want 1 (salary)", cols[0])
	}
	if cols[1] != 2 {
		t.Errorf("extractWindowOrderByCols: cols[1] = %d, want 2 (name)", cols[1])
	}
}

// TestWindowHelperDeadCode_ExtractOrderByCols_NonIdent verifies that a
// non-IdentExpr ORDER BY term is silently skipped.
func TestWindowHelperDeadCode_ExtractOrderByCols_NonIdent(t *testing.T) {
	t.Parallel()
	s := newWindowHelperStmt(t)
	table := testTable()

	orderBy := []parser.OrderingTerm{
		{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
	}

	cols := s.extractWindowOrderByCols(orderBy, table)
	if len(cols) != 0 {
		t.Errorf("extractWindowOrderByCols with non-ident: want 0 cols, got %d", len(cols))
	}
}

// TestWindowHelperDeadCode_ExtractOrderByCols_UnknownName verifies that an
// IdentExpr with a name not present in the table is not added to the result.
func TestWindowHelperDeadCode_ExtractOrderByCols_UnknownName(t *testing.T) {
	t.Parallel()
	s := newWindowHelperStmt(t)
	table := testTable()

	orderBy := []parser.OrderingTerm{
		{Expr: &parser.IdentExpr{Name: "nonexistent_column"}},
	}

	cols := s.extractWindowOrderByCols(orderBy, table)
	if len(cols) != 0 {
		t.Errorf("extractWindowOrderByCols with unknown name: want 0 cols, got %d", len(cols))
	}
}

// ---------------------------------------------------------------------------
// emitWindowRankTrackingFromSorter
// ---------------------------------------------------------------------------

// TestWindowHelperDeadCode_RankTrackingFromSorter_WithOrderByCols verifies that
// when hasRank is true and orderByCols is non-empty, the function calls through
// to emitWindowRankComparison (fromSorter=true), producing OpCopy instructions.
func TestWindowHelperDeadCode_RankTrackingFromSorter_WithOrderByCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{1},
	}

	emitWindowRankTrackingFromSorter(vm, regs, info, 5)

	if len(vm.Program) == 0 {
		t.Error("emitWindowRankTrackingFromSorter (with orderByCols): expected instructions, got none")
	}

	// When fromSorter=true the first instruction for a column copy is OpCopy.
	foundCopy := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpCopy {
			foundCopy = true
			break
		}
	}
	if !foundCopy {
		t.Error("emitWindowRankTrackingFromSorter: expected OpCopy for sorter path, not found")
	}
}

// TestWindowHelperDeadCode_RankTrackingFromSorter_EmptyOrderByCols verifies
// that when orderByCols is empty, the fallback OpAddImm is emitted.
func TestWindowHelperDeadCode_RankTrackingFromSorter_EmptyOrderByCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{},
	}

	emitWindowRankTrackingFromSorter(vm, regs, info, 5)

	foundAddImm := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpAddImm {
			foundAddImm = true
			break
		}
	}
	if !foundAddImm {
		t.Error("emitWindowRankTrackingFromSorter (empty cols): expected OpAddImm, not found")
	}
}

// ---------------------------------------------------------------------------
// emitWindowRankComparison
// ---------------------------------------------------------------------------

// TestWindowHelperDeadCode_RankComparison_FromSorterTrue verifies that when
// fromSorter=true and orderByCols is non-empty, OpCopy (not OpColumn) is used
// to read values.
func TestWindowHelperDeadCode_RankComparison_FromSorterTrue(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{1},
	}

	emitWindowRankComparison(vm, regs, info, 5, true)

	foundCopy := false
	foundColumn := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpCopy {
			foundCopy = true
		}
		if instr.Opcode == vdbe.OpColumn {
			foundColumn = true
		}
	}
	if !foundCopy {
		t.Error("emitWindowRankComparison (fromSorter=true): expected OpCopy, not found")
	}
	if foundColumn {
		t.Error("emitWindowRankComparison (fromSorter=true): unexpected OpColumn")
	}
}

// TestWindowHelperDeadCode_RankComparison_FromSorterFalse verifies that when
// fromSorter=false and orderByCols is non-empty, OpColumn (not OpCopy for the
// initial read) is used.
func TestWindowHelperDeadCode_RankComparison_FromSorterFalse(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{1},
	}

	emitWindowRankComparison(vm, regs, info, 5, false)

	foundColumn := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpColumn {
			foundColumn = true
			break
		}
	}
	if !foundColumn {
		t.Error("emitWindowRankComparison (fromSorter=false): expected OpColumn, not found")
	}
}

// TestWindowHelperDeadCode_RankComparison_FallbackPath exercises the branch
// where orderByCols is empty but hasRank is true, which falls back to OpAddImm.
func TestWindowHelperDeadCode_RankComparison_FallbackPath(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{},
	}

	emitWindowRankComparison(vm, regs, info, 5, true)

	if len(vm.Program) != 1 {
		t.Errorf("emitWindowRankComparison (fallback): expected 1 instruction, got %d", len(vm.Program))
	}
	if vm.Program[0].Opcode != vdbe.OpAddImm {
		t.Errorf("emitWindowRankComparison (fallback): expected OpAddImm, got %v", vm.Program[0].Opcode)
	}
}

// ---------------------------------------------------------------------------
// emitOrderByValueComparison
// ---------------------------------------------------------------------------

// TestWindowHelperDeadCode_OrderByValueComparison_EmptyCols verifies the
// zero-length shortcut: a single OpInteger 1 → valuesChangedReg is emitted.
func TestWindowHelperDeadCode_OrderByValueComparison_EmptyCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	valuesChangedReg := 45

	emitOrderByValueComparison(vm, regs, []int{}, valuesChangedReg)

	if len(vm.Program) != 1 {
		t.Fatalf("emitOrderByValueComparison (empty cols): want 1 instr, got %d", len(vm.Program))
	}
	instr := vm.Program[0]
	if instr.Opcode != vdbe.OpInteger {
		t.Errorf("emitOrderByValueComparison (empty cols): want OpInteger, got %v", instr.Opcode)
	}
	if instr.P2 != valuesChangedReg {
		t.Errorf("emitOrderByValueComparison (empty cols): P2 = %d, want %d", instr.P2, valuesChangedReg)
	}
}

// TestWindowHelperDeadCode_OrderByValueComparison_WithCols verifies that
// comparison opcodes (OpNotNull, OpNe, OpGoto, OpInteger) are emitted when
// orderByCols is non-empty.
func TestWindowHelperDeadCode_OrderByValueComparison_WithCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	valuesChangedReg := 45

	emitOrderByValueComparison(vm, regs, []int{1}, valuesChangedReg)

	if len(vm.Program) == 0 {
		t.Fatal("emitOrderByValueComparison (with cols): expected instructions, got none")
	}

	// Should contain OpNotNull and OpNe
	opcodes := map[vdbe.Opcode]bool{}
	for _, instr := range vm.Program {
		opcodes[instr.Opcode] = true
	}
	if !opcodes[vdbe.OpNotNull] {
		t.Error("emitOrderByValueComparison (with cols): missing OpNotNull")
	}
	if !opcodes[vdbe.OpNe] {
		t.Error("emitOrderByValueComparison (with cols): missing OpNe")
	}
}

// ---------------------------------------------------------------------------
// emitWindowRankUpdate
// ---------------------------------------------------------------------------

// TestWindowHelperDeadCode_WindowRankUpdate_NoPanic verifies that
// emitWindowRankUpdate completes without panicking and emits at least one
// instruction.
func TestWindowHelperDeadCode_WindowRankUpdate_NoPanic(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		hasDenseRank: true,
		orderByCols: []int{1, 2},
	}
	valuesChangedReg := 45

	emitWindowRankUpdate(vm, regs, valuesChangedReg, info)

	if len(vm.Program) == 0 {
		t.Error("emitWindowRankUpdate: expected instructions, got none")
	}
}

// TestWindowHelperDeadCode_WindowRankUpdate_EmptyOrderByCols exercises the
// path where info.orderByCols is empty (no prevOrderBy copy loop).
func TestWindowHelperDeadCode_WindowRankUpdate_EmptyOrderByCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := testRankRegisters()
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{},
	}
	valuesChangedReg := 45

	emitWindowRankUpdate(vm, regs, valuesChangedReg, info)

	if len(vm.Program) == 0 {
		t.Error("emitWindowRankUpdate (empty orderByCols): expected instructions, got none")
	}
}
