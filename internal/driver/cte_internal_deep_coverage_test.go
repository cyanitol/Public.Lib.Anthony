// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// White-box unit tests for CTE bytecode-manipulation scaffolding functions.
// These functions have no SQL-reachable call sites (they are marked SCAFFOLDING
// in the source), so they must be exercised directly from package driver.

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ------------------------------------------------------------------ helpers

// makeInternalConn opens a fresh in-memory Conn for internal CTE tests.
func makeInternalConn(t *testing.T) *Conn {
	t.Helper()
	d := &Driver{}
	raw, err := d.Open(":memory:")
	if err != nil {
		t.Fatalf("makeInternalConn: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	return raw.(*Conn)
}

// makeInternalStmt returns a minimal *Stmt backed by an in-memory Conn.
func makeInternalStmt(t *testing.T) *Stmt {
	t.Helper()
	return &Stmt{conn: makeInternalConn(t)}
}

// makeLiteralSelect returns a minimal SelectStmt that selects the integer literal n.
func makeLiteralSelect(n string) *parser.SelectStmt {
	return &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: n}},
		},
	}
}

// ------------------------------------------------------------------ isControlFlowOp

// TestCTEInternal_IsControlFlowOp_True verifies that OpInit, OpHalt, and
// OpNoop are all reported as control-flow operations.
func TestCTEInternal_IsControlFlowOp_True(t *testing.T) {
	for _, op := range []vdbe.Opcode{vdbe.OpInit, vdbe.OpHalt, vdbe.OpNoop} {
		if !isControlFlowOp(op) {
			t.Errorf("isControlFlowOp(%v) = false, want true", op)
		}
	}
}

// TestCTEInternal_IsControlFlowOp_False verifies that non-control-flow
// opcodes are not misidentified.
func TestCTEInternal_IsControlFlowOp_False(t *testing.T) {
	for _, op := range []vdbe.Opcode{vdbe.OpResultRow, vdbe.OpGoto, vdbe.OpInteger, vdbe.OpRewind} {
		if isControlFlowOp(op) {
			t.Errorf("isControlFlowOp(%v) = true, want false", op)
		}
	}
}

// ------------------------------------------------------------------ buildSimpleAddrMap (via inlineMainQueryBytecode)

// TestCTEInternal_BuildSimpleAddrMap exercises buildSimpleAddrMap by calling
// inlineMainQueryBytecode with a tiny compiled sub-VDBE (one literal SELECT).
// buildSimpleAddrMap is called on line 140 of stmt_cte.go.
func TestCTEInternal_BuildSimpleAddrMap(t *testing.T) {
	s := makeInternalStmt(t)

	// Compile the main sub-VM with a trivial SELECT 42.
	subVM := vdbe.New()
	_ = subVM.AllocMemory(1)
	subVM.AddOp(vdbe.OpInit, 0, 0, 0)
	subVM.AddOp(vdbe.OpInteger, 42, 0, 0)
	subVM.AddOp(vdbe.OpResultRow, 0, 1, 0)
	subVM.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Build main VM and call inlineMainQueryBytecode.
	mainVM := vdbe.New()
	_ = mainVM.AllocMemory(2)
	offsets := cteInlineOffsets{
		baseCursor:   0,
		baseRegister: 0,
		baseSorter:   0,
		recordReg:    1,
		startAddr:    mainVM.NumOps(),
	}
	cursorMap := map[int]int{}

	// Must not panic — buildSimpleAddrMap is exercised internally.
	s.inlineMainQueryBytecode(mainVM, subVM, offsets, cursorMap)

	if mainVM.NumOps() == 0 {
		t.Error("expected instructions to be added by inlineMainQueryBytecode")
	}
}

// ------------------------------------------------------------------ compileCTEPopulation

// TestCTEInternal_CompileCTEPopulation exercises compileCTEPopulation with a
// simple literal SELECT, which in turn calls compileCTESelect, allocateCTEResources,
// and inlineCTEBytecode.
func TestCTEInternal_CompileCTEPopulation(t *testing.T) {
	s := makeInternalStmt(t)

	vm := vdbe.New()
	_ = vm.AllocMemory(4)
	vm.AllocCursors(2)

	cteSelect := makeLiteralSelect("7")
	var args []driver.NamedValue

	err := s.compileCTEPopulation(vm, cteSelect, 0, 1, args)
	if err != nil {
		t.Fatalf("compileCTEPopulation returned error: %v", err)
	}
	if vm.NumOps() == 0 {
		t.Error("expected instructions emitted by compileCTEPopulation")
	}
}

// TestCTEInternal_CompileCTEPopulation_Compound exercises compileCTEPopulation
// with a UNION ALL to stress inlineCTEBytecode with multiple result rows and
// jump opcodes inside the compiled sub-VM.
func TestCTEInternal_CompileCTEPopulation_Compound(t *testing.T) {
	s := makeInternalStmt(t)

	vm := vdbe.New()
	_ = vm.AllocMemory(4)
	vm.AllocCursors(2)

	// Build SELECT 1 UNION ALL SELECT 2
	left := makeLiteralSelect("1")
	right := makeLiteralSelect("2")
	cteSelect := &parser.SelectStmt{
		Compound: &parser.CompoundSelect{
			Op:    parser.CompoundUnionAll,
			Left:  left,
			Right: right,
		},
	}
	var args []driver.NamedValue

	err := s.compileCTEPopulation(vm, cteSelect, 0, 1, args)
	if err != nil {
		t.Fatalf("compileCTEPopulation (compound) returned error: %v", err)
	}
	if vm.NumOps() == 0 {
		t.Error("expected instructions emitted by compileCTEPopulation (compound)")
	}
}

// ------------------------------------------------------------------ handleSpecialOpcode

// TestCTEInternal_HandleSpecialOpcode_ResultRow verifies that handleSpecialOpcode
// converts OpResultRow into MakeRecord + Insert and returns true.
func TestCTEInternal_HandleSpecialOpcode_ResultRow(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()
	_ = vm.AllocMemory(4)

	offsets := cteInlineOffsets{
		baseRegister: 0,
		recordReg:    3,
		startAddr:    0,
	}
	instr := &vdbe.Instruction{Opcode: vdbe.OpResultRow, P1: 0, P2: 2}
	newInstr := *instr

	handled := s.handleSpecialOpcode(vm, instr, &newInstr, 1, offsets)
	if !handled {
		t.Error("handleSpecialOpcode(OpResultRow) should return true")
	}
	if vm.NumOps() == 0 {
		t.Error("handleSpecialOpcode(OpResultRow) should emit instructions")
	}
}

// TestCTEInternal_HandleSpecialOpcode_Init verifies that OpInit is converted
// to OpNoop and returns false (instruction is added by the caller).
func TestCTEInternal_HandleSpecialOpcode_Init(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{}
	instr := &vdbe.Instruction{Opcode: vdbe.OpInit}
	newInstr := *instr

	handled := s.handleSpecialOpcode(vm, instr, &newInstr, 0, offsets)
	if handled {
		t.Error("handleSpecialOpcode(OpInit) should return false")
	}
	if newInstr.Opcode != vdbe.OpNoop {
		t.Errorf("expected newInstr.Opcode = OpNoop, got %v", newInstr.Opcode)
	}
}

// TestCTEInternal_HandleSpecialOpcode_Halt verifies that OpHalt is converted
// to OpNoop and returns false.
func TestCTEInternal_HandleSpecialOpcode_Halt(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{}
	instr := &vdbe.Instruction{Opcode: vdbe.OpHalt}
	newInstr := *instr

	handled := s.handleSpecialOpcode(vm, instr, &newInstr, 0, offsets)
	if handled {
		t.Error("handleSpecialOpcode(OpHalt) should return false")
	}
	if newInstr.Opcode != vdbe.OpNoop {
		t.Errorf("expected newInstr.Opcode = OpNoop, got %v", newInstr.Opcode)
	}
}

// TestCTEInternal_HandleSpecialOpcode_Other verifies that an ordinary opcode
// (OpInteger) is not handled — function returns false and newInstr is unchanged.
func TestCTEInternal_HandleSpecialOpcode_Other(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{}
	instr := &vdbe.Instruction{Opcode: vdbe.OpInteger, P1: 99}
	newInstr := *instr

	handled := s.handleSpecialOpcode(vm, instr, &newInstr, 0, offsets)
	if handled {
		t.Error("handleSpecialOpcode(OpInteger) should return false")
	}
	if newInstr.Opcode != vdbe.OpInteger {
		t.Errorf("expected newInstr.Opcode unchanged = OpInteger, got %v", newInstr.Opcode)
	}
}

// ------------------------------------------------------------------ adjustJumpTarget

// TestCTEInternal_AdjustJumpTarget_Goto exercises the OpGoto branch of
// adjustJumpTarget, confirming the instruction's P2 is shifted by startAddr.
func TestCTEInternal_AdjustJumpTarget_Goto(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 10}
	instr := &vdbe.Instruction{Opcode: vdbe.OpGoto, P2: 3}
	addr := vm.AddOp(vdbe.OpGoto, 0, 3, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 13 {
		t.Errorf("adjustJumpTarget(OpGoto, P2=3, start=10) = %d, want 13", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_Rewind exercises the OpRewind branch.
func TestCTEInternal_AdjustJumpTarget_Rewind(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 5}
	instr := &vdbe.Instruction{Opcode: vdbe.OpRewind, P2: 2}
	addr := vm.AddOp(vdbe.OpRewind, 0, 2, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 7 {
		t.Errorf("adjustJumpTarget(OpRewind, P2=2, start=5) = %d, want 7", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_Next exercises the OpNext branch.
func TestCTEInternal_AdjustJumpTarget_Next(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 7}
	instr := &vdbe.Instruction{Opcode: vdbe.OpNext, P2: 4}
	addr := vm.AddOp(vdbe.OpNext, 0, 4, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 11 {
		t.Errorf("adjustJumpTarget(OpNext, P2=4, start=7) = %d, want 11", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_If exercises the OpIf branch.
func TestCTEInternal_AdjustJumpTarget_If(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 3}
	instr := &vdbe.Instruction{Opcode: vdbe.OpIf, P2: 1}
	addr := vm.AddOp(vdbe.OpIf, 0, 1, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 4 {
		t.Errorf("adjustJumpTarget(OpIf, P2=1, start=3) = %d, want 4", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_SorterSort exercises the OpSorterSort branch.
func TestCTEInternal_AdjustJumpTarget_SorterSort(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 20}
	instr := &vdbe.Instruction{Opcode: vdbe.OpSorterSort, P2: 5}
	addr := vm.AddOp(vdbe.OpSorterSort, 0, 5, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 25 {
		t.Errorf("adjustJumpTarget(OpSorterSort, P2=5, start=20) = %d, want 25", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_ZeroP2 verifies that P2=0 is not adjusted
// (the function guards with "if instr.P2 > 0").
func TestCTEInternal_AdjustJumpTarget_ZeroP2(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 10}
	instr := &vdbe.Instruction{Opcode: vdbe.OpGoto, P2: 0}
	addr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 0 {
		t.Errorf("adjustJumpTarget with P2=0 should not modify P2, got %d", vm.Program[addr].P2)
	}
}

// TestCTEInternal_AdjustJumpTarget_NonJump verifies that a non-jump opcode
// (OpInteger) leaves P2 unchanged.
func TestCTEInternal_AdjustJumpTarget_NonJump(t *testing.T) {
	s := makeInternalStmt(t)
	vm := vdbe.New()

	offsets := cteInlineOffsets{startAddr: 10}
	instr := &vdbe.Instruction{Opcode: vdbe.OpInteger, P2: 5}
	addr := vm.AddOp(vdbe.OpInteger, 0, 5, 0)

	s.adjustJumpTarget(vm, instr, addr, offsets)
	if vm.Program[addr].P2 != 5 {
		t.Errorf("adjustJumpTarget(OpInteger) should not modify P2, got %d", vm.Program[addr].P2)
	}
}

// ------------------------------------------------------------------ inlineCTEBytecode

// TestCTEInternal_InlineCTEBytecode exercises inlineCTEBytecode directly with
// a hand-crafted sub-VM containing each special opcode branch.
func TestCTEInternal_InlineCTEBytecode(t *testing.T) {
	s := makeInternalStmt(t)

	// Build a minimal compiled CTE VM with Init, Integer, ResultRow, Halt.
	compiledCTE := vdbe.New()
	_ = compiledCTE.AllocMemory(2)
	compiledCTE.AddOp(vdbe.OpInit, 0, 0, 0)
	compiledCTE.AddOp(vdbe.OpInteger, 99, 0, 0)
	compiledCTE.AddOp(vdbe.OpResultRow, 0, 1, 0)
	compiledCTE.AddOp(vdbe.OpHalt, 0, 0, 0)

	mainVM := vdbe.New()
	_ = mainVM.AllocMemory(5)
	mainVM.AllocCursors(2)

	offsets := cteInlineOffsets{
		baseCursor:   0,
		baseRegister: 0,
		baseSorter:   0,
		recordReg:    4,
		startAddr:    mainVM.NumOps(),
	}

	// Must not panic and should emit instructions.
	s.inlineCTEBytecode(mainVM, compiledCTE, 1, offsets)

	if mainVM.NumOps() == 0 {
		t.Error("expected instructions emitted by inlineCTEBytecode")
	}
}

// TestCTEInternal_InlineCTEBytecode_WithJump exercises inlineCTEBytecode with
// a jump opcode (OpGoto) so adjustJumpTarget is triggered.
func TestCTEInternal_InlineCTEBytecode_WithJump(t *testing.T) {
	s := makeInternalStmt(t)

	compiledCTE := vdbe.New()
	_ = compiledCTE.AllocMemory(2)
	compiledCTE.AddOp(vdbe.OpInit, 0, 0, 0)
	compiledCTE.AddOp(vdbe.OpInteger, 42, 0, 0)
	compiledCTE.AddOp(vdbe.OpGoto, 0, 1, 0) // jump to addr 1 (within sub-VM)
	compiledCTE.AddOp(vdbe.OpResultRow, 0, 1, 0)
	compiledCTE.AddOp(vdbe.OpHalt, 0, 0, 0)

	mainVM := vdbe.New()
	_ = mainVM.AllocMemory(5)
	mainVM.AllocCursors(2)

	offsets := cteInlineOffsets{
		recordReg: 4,
		startAddr: mainVM.NumOps(),
	}

	s.inlineCTEBytecode(mainVM, compiledCTE, 0, offsets)

	if mainVM.NumOps() == 0 {
		t.Error("expected instructions emitted by inlineCTEBytecode (with jump)")
	}
}
