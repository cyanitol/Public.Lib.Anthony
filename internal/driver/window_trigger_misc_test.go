// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// emitWindowFunctionColumn
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_EmitWindowFunctionColumn_RowNumber(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	fn := &parser.FunctionExpr{Name: "ROW_NUMBER"}
	colIdx := 3

	emitWindowFunctionColumn(vm, fn, regs, colIdx)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowFunctionColumn ROW_NUMBER: expected instructions")
	}
	if vm.Program[0].Opcode != vdbe.OpCopy {
		t.Errorf("ROW_NUMBER: expected OpCopy, got %v", vm.Program[0].Opcode)
	}
	if vm.Program[0].P1 != regs.rowCount {
		t.Errorf("ROW_NUMBER: P1 = %d, want rowCount %d", vm.Program[0].P1, regs.rowCount)
	}
	if vm.Program[0].P2 != colIdx {
		t.Errorf("ROW_NUMBER: P2 = %d, want colIdx %d", vm.Program[0].P2, colIdx)
	}
}

func TestWindowTriggerMisc_EmitWindowFunctionColumn_NTILE(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	fn := &parser.FunctionExpr{Name: "NTILE"}
	colIdx := 4

	emitWindowFunctionColumn(vm, fn, regs, colIdx)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowFunctionColumn NTILE: expected instructions")
	}
	if vm.Program[0].Opcode != vdbe.OpCopy {
		t.Errorf("NTILE: expected OpCopy, got %v", vm.Program[0].Opcode)
	}
	if vm.Program[0].P1 != regs.rowCount {
		t.Errorf("NTILE: P1 = %d, want rowCount %d", vm.Program[0].P1, regs.rowCount)
	}
}

func TestWindowTriggerMisc_EmitWindowFunctionColumn_Rank(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	fn := &parser.FunctionExpr{Name: "RANK"}
	colIdx := 2

	emitWindowFunctionColumn(vm, fn, regs, colIdx)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowFunctionColumn RANK: expected instructions")
	}
	if vm.Program[0].Opcode != vdbe.OpCopy {
		t.Errorf("RANK: expected OpCopy, got %v", vm.Program[0].Opcode)
	}
	if vm.Program[0].P1 != regs.rank {
		t.Errorf("RANK: P1 = %d, want rank %d", vm.Program[0].P1, regs.rank)
	}
}

func TestWindowTriggerMisc_EmitWindowFunctionColumn_DenseRank(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	fn := &parser.FunctionExpr{Name: "DENSE_RANK"}
	colIdx := 2

	emitWindowFunctionColumn(vm, fn, regs, colIdx)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowFunctionColumn DENSE_RANK: expected instructions")
	}
	if vm.Program[0].Opcode != vdbe.OpCopy {
		t.Errorf("DENSE_RANK: expected OpCopy, got %v", vm.Program[0].Opcode)
	}
	if vm.Program[0].P1 != regs.denseRank {
		t.Errorf("DENSE_RANK: P1 = %d, want denseRank %d", vm.Program[0].P1, regs.denseRank)
	}
}

func TestWindowTriggerMisc_EmitWindowFunctionColumn_Default(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	colIdx := 2

	// CUME_DIST is not handled by name so falls to default
	fn := &parser.FunctionExpr{Name: "CUME_DIST"}
	emitWindowFunctionColumn(vm, fn, regs, colIdx)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowFunctionColumn default: expected instructions")
	}
	if vm.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("CUME_DIST (default): expected OpNull, got %v", vm.Program[0].Opcode)
	}
}

// ---------------------------------------------------------------------------
// emitWindowRankTracking
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_EmitWindowRankTracking_WithOrderByCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	info := rankFunctionInfo{
		hasRank:     true,
		orderByCols: []int{1},
	}

	emitWindowRankTracking(vm, regs, info, 5)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowRankTracking (hasRank+orderBy): expected instructions")
	}
	// With hasRank+orderByCols the comparison path is used: expects OpColumn
	foundColumn := false
	for _, instr := range vm.Program {
		if instr.Opcode == vdbe.OpColumn {
			foundColumn = true
			break
		}
	}
	if !foundColumn {
		t.Error("emitWindowRankTracking (hasRank+orderBy): expected OpColumn, not found")
	}
}

func TestWindowTriggerMisc_EmitWindowRankTracking_EmptyOrderByCols(t *testing.T) {
	t.Parallel()
	vm := vdbe.New()
	vm.AllocMemory(60)
	regs := initWindowRankRegisters(5)
	info := rankFunctionInfo{
		hasRank:     false,
		hasDenseRank: false,
		orderByCols: []int{},
	}

	emitWindowRankTracking(vm, regs, info, 5)

	if len(vm.Program) == 0 {
		t.Fatal("emitWindowRankTracking (no rank): expected instructions")
	}
	// Without hasRank/hasDenseRank the else branch emits OpAddImm
	if vm.Program[0].Opcode != vdbe.OpAddImm {
		t.Errorf("emitWindowRankTracking (no rank): expected OpAddImm, got %v", vm.Program[0].Opcode)
	}
}

// ---------------------------------------------------------------------------
// extractRowMaps
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_ExtractRowMaps_Nil(t *testing.T) {
	t.Parallel()
	old, new_ := extractRowMaps(nil)
	if old != nil {
		t.Errorf("extractRowMaps(nil): old = %v, want nil", old)
	}
	if new_ != nil {
		t.Errorf("extractRowMaps(nil): new = %v, want nil", new_)
	}
}

func TestWindowTriggerMisc_ExtractRowMaps_WithData(t *testing.T) {
	t.Parallel()
	oldRow := map[string]interface{}{"id": int64(1), "name": "Alice"}
	newRow := map[string]interface{}{"id": int64(1), "name": "Bob"}

	data := &vdbe.TriggerRowData{
		OldRow: oldRow,
		NewRow: newRow,
	}

	old, new_ := extractRowMaps(data)
	if old == nil {
		t.Fatal("extractRowMaps: old should not be nil")
	}
	if new_ == nil {
		t.Fatal("extractRowMaps: new should not be nil")
	}
	if old["name"] != "Alice" {
		t.Errorf("extractRowMaps: old[name] = %v, want Alice", old["name"])
	}
	if new_["name"] != "Bob" {
		t.Errorf("extractRowMaps: new[name] = %v, want Bob", new_["name"])
	}
}

func TestWindowTriggerMisc_ExtractRowMaps_InsertRow(t *testing.T) {
	t.Parallel()
	// INSERT: OldRow is nil, NewRow has data
	newRow := map[string]interface{}{"id": int64(42)}
	data := &vdbe.TriggerRowData{
		OldRow: nil,
		NewRow: newRow,
	}

	old, new_ := extractRowMaps(data)
	if old != nil {
		t.Errorf("extractRowMaps (INSERT): old = %v, want nil", old)
	}
	if new_ == nil {
		t.Fatal("extractRowMaps (INSERT): new should not be nil")
	}
}

// ---------------------------------------------------------------------------
// pragmaRowMatchesWhere / evalPragmaWhere
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_PragmaRowMatchesWhere_NilWhere(t *testing.T) {
	t.Parallel()
	row := []interface{}{"foo", "bar"}
	cols := []string{"name", "value"}
	if !pragmaRowMatchesWhere(nil, row, cols) {
		t.Error("pragmaRowMatchesWhere(nil where): expected true")
	}
}

func TestWindowTriggerMisc_PragmaRowMatchesWhere_EqMatch(t *testing.T) {
	t.Parallel()
	// WHERE name = 'foo'
	where := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Name: "name"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "foo"},
	}
	row := []interface{}{"foo", "bar"}
	cols := []string{"name", "value"}

	if !pragmaRowMatchesWhere(where, row, cols) {
		t.Error("pragmaRowMatchesWhere EqMatch: expected true")
	}
}

func TestWindowTriggerMisc_PragmaRowMatchesWhere_EqNoMatch(t *testing.T) {
	t.Parallel()
	// WHERE name = 'baz' but row has 'foo'
	where := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Name: "name"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "baz"},
	}
	row := []interface{}{"foo", "bar"}
	cols := []string{"name", "value"}

	if pragmaRowMatchesWhere(where, row, cols) {
		t.Error("pragmaRowMatchesWhere EqNoMatch: expected false")
	}
}

func TestWindowTriggerMisc_EvalPragmaWhere_UnknownExprPassThrough(t *testing.T) {
	t.Parallel()
	// Non-BinaryExpr expression passes through as true
	expr := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}
	row := []interface{}{"x"}
	cols := []string{"col1"}

	if !evalPragmaWhere(expr, row, cols) {
		t.Error("evalPragmaWhere non-binary: expected true (pass-through)")
	}
}

func TestWindowTriggerMisc_EvalPragmaWhere_AndBothMatch(t *testing.T) {
	t.Parallel()
	// WHERE name = 'foo' AND value = 'bar'
	where := &parser.BinaryExpr{
		Op: parser.OpAnd,
		Left: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "foo"},
		},
		Right: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "value"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "bar"},
		},
	}
	row := []interface{}{"foo", "bar"}
	cols := []string{"name", "value"}

	if !pragmaRowMatchesWhere(where, row, cols) {
		t.Error("evalPragmaWhere AND both match: expected true")
	}
}

func TestWindowTriggerMisc_EvalPragmaWhere_AndOneFails(t *testing.T) {
	t.Parallel()
	// WHERE name = 'foo' AND value = 'wrong'
	where := &parser.BinaryExpr{
		Op: parser.OpAnd,
		Left: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "name"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "foo"},
		},
		Right: &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "value"},
			Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "wrong"},
		},
	}
	row := []interface{}{"foo", "bar"}
	cols := []string{"name", "value"}

	if pragmaRowMatchesWhere(where, row, cols) {
		t.Error("evalPragmaWhere AND one fails: expected false")
	}
}

// ---------------------------------------------------------------------------
// insertSelectNeedsMaterialise
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_InsertSelectNeedsMaterialise_Nil(t *testing.T) {
	t.Parallel()
	s := stmtFor(openMemConn(t))
	if s.insertSelectNeedsMaterialise(nil) {
		t.Error("insertSelectNeedsMaterialise(nil): expected false")
	}
}

func TestWindowTriggerMisc_InsertSelectNeedsMaterialise_SimpleSelect(t *testing.T) {
	t.Parallel()
	s := stmtFor(openMemConn(t))
	sel := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"}},
		},
	}
	if s.insertSelectNeedsMaterialise(sel) {
		t.Error("insertSelectNeedsMaterialise simple: expected false")
	}
}

func TestWindowTriggerMisc_InsertSelectNeedsMaterialise_WithOrderBy(t *testing.T) {
	t.Parallel()
	s := stmtFor(openMemConn(t))
	sel := &parser.SelectStmt{
		OrderBy: []parser.OrderingTerm{
			{Expr: &parser.IdentExpr{Name: "id"}},
		},
	}
	if !s.insertSelectNeedsMaterialise(sel) {
		t.Error("insertSelectNeedsMaterialise with ORDER BY: expected true")
	}
}

func TestWindowTriggerMisc_InsertSelectNeedsMaterialise_WithLimit(t *testing.T) {
	t.Parallel()
	s := stmtFor(openMemConn(t))
	sel := &parser.SelectStmt{
		Limit: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10"},
	}
	if !s.insertSelectNeedsMaterialise(sel) {
		t.Error("insertSelectNeedsMaterialise with LIMIT: expected true")
	}
}

func TestWindowTriggerMisc_InsertSelectNeedsMaterialise_WithDistinct(t *testing.T) {
	t.Parallel()
	s := stmtFor(openMemConn(t))
	sel := &parser.SelectStmt{
		Distinct: true,
	}
	if !s.insertSelectNeedsMaterialise(sel) {
		t.Error("insertSelectNeedsMaterialise with DISTINCT: expected true")
	}
}

// ---------------------------------------------------------------------------
// createMemoryConnection
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_CreateMemoryConnection_Success(t *testing.T) {
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("createMemoryConnection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	if c.securityConfig == nil {
		t.Error("createMemoryConnection: securityConfig should be non-nil")
	}
}

func TestWindowTriggerMisc_CreateMemoryConnection_Functional(t *testing.T) {
	d := &Driver{}
	conn, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("createMemoryConnection functional: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)
	stmt, err := c.Prepare("CREATE TABLE wt_test(id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("createMemoryConnection: Prepare failed: %v", err)
	}
	if _, err := stmt.(*Stmt).Exec(nil); err != nil {
		t.Fatalf("createMemoryConnection: Exec CREATE failed: %v", err)
	}
	stmt.Close()
}

// ---------------------------------------------------------------------------
// MarkDirty (memoryPagerProvider)
// ---------------------------------------------------------------------------

func TestWindowTriggerMisc_MarkDirty_ViaInsertUpdate(t *testing.T) {
	c := openMemConn(t)

	setup, err := c.Prepare("CREATE TABLE wt_items(id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("MarkDirty: Prepare CREATE: %v", err)
	}
	if _, err := setup.(*Stmt).Exec(nil); err != nil {
		t.Fatalf("MarkDirty: Exec CREATE: %v", err)
	}
	setup.Close()

	ins, err := c.Prepare("INSERT INTO wt_items VALUES (?, ?)")
	if err != nil {
		t.Fatalf("MarkDirty: Prepare INSERT: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := ins.(*Stmt).Exec([]driver.Value{int64(i), int64(i * 10)}); err != nil {
			t.Fatalf("MarkDirty: INSERT row %d: %v", i, err)
		}
	}
	ins.Close()

	upd, err := c.Prepare("UPDATE wt_items SET val = val + 1 WHERE id = 1")
	if err != nil {
		t.Fatalf("MarkDirty: Prepare UPDATE: %v", err)
	}
	if _, err := upd.(*Stmt).Exec(nil); err != nil {
		t.Fatalf("MarkDirty: Exec UPDATE: %v", err)
	}
	upd.Close()
}
