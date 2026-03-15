// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// recursiveCTEState holds state for recursive CTE bytecode generation.
type recursiveCTEState struct {
	resultCursor int
	queueCursor  int
	nextCursor   int
	numColumns   int
	resultTable  *schema.Table
	queueTable   *schema.Table
	nextTable    *schema.Table
}

// compileRecursiveCTEBytecode generates runtime bytecode for a recursive CTE.
func (s *Stmt) compileRecursiveCTEBytecode(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	compound, err := s.validateRecursiveCTE(def, cteName)
	if err != nil {
		return nil, err
	}

	state := s.setupRecursiveCTEState(vm, cteName, def)
	s.registerRecursiveCTETables(state)

	if err := s.emitAnchorBytecode(vm, compound.Left, cteTempTables, state, args); err != nil {
		return nil, err
	}

	if err := s.emitRecursiveLoop(vm, compound.Right, cteName, cteTempTables, state, args); err != nil {
		return nil, err
	}

	return state.resultTable, nil
}

// setupRecursiveCTEState creates ephemeral tables and allocates cursors.
func (s *Stmt) setupRecursiveCTEState(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition) *recursiveCTEState {
	state := &recursiveCTEState{}

	state.resultTable = s.createCTETempTable(fmt.Sprintf("_cte_%s", cteName), def)
	state.queueTable = s.createCTETempTable(fmt.Sprintf("_cte_%s_queue", cteName), def)
	state.nextTable = s.createCTETempTable(fmt.Sprintf("_cte_%s_next", cteName), def)
	state.numColumns = len(state.resultTable.Columns)

	state.resultCursor = len(vm.Cursors)
	vm.AllocCursors(state.resultCursor + 1)
	state.queueCursor = len(vm.Cursors)
	vm.AllocCursors(state.queueCursor + 1)
	state.nextCursor = len(vm.Cursors)
	vm.AllocCursors(state.nextCursor + 1)

	vm.AddOp(vdbe.OpOpenEphemeral, state.resultCursor, state.numColumns, 0)
	vm.AddOp(vdbe.OpOpenEphemeral, state.queueCursor, state.numColumns, 0)
	vm.AddOp(vdbe.OpOpenEphemeral, state.nextCursor, state.numColumns, 0)

	state.resultTable.RootPage = uint32(state.resultCursor)
	state.queueTable.RootPage = uint32(state.queueCursor)
	state.nextTable.RootPage = uint32(state.nextCursor)

	return state
}

// registerRecursiveCTETables registers all temp tables in the schema.
func (s *Stmt) registerRecursiveCTETables(state *recursiveCTEState) {
	s.conn.schema.AddTableDirect(state.resultTable)
	s.conn.schema.AddTableDirect(state.queueTable)
	s.conn.schema.AddTableDirect(state.nextTable)
}

// emitAnchorBytecode inlines the anchor SELECT, inserting into result and queue.
func (s *Stmt) emitAnchorBytecode(vm *vdbe.VDBE, anchorSelect *parser.SelectStmt,
	cteTempTables map[string]*schema.Table, state *recursiveCTEState, args []driver.NamedValue) error {

	rewrittenAnchor := s.rewriteSelectWithCTETables(anchorSelect, cteTempTables)
	compiledAnchor, err := s.compileCTESelect(vm, rewrittenAnchor, args)
	if err != nil {
		return fmt.Errorf("failed to compile anchor: %w", err)
	}

	offsets := s.allocateCTEResources(vm, compiledAnchor)
	cursors := [2]int{state.resultCursor, state.queueCursor}
	s.inlineCTEWithAddrMap(vm, compiledAnchor, cursors, offsets, nil)
	return nil
}

// emitRecursiveLoop generates the bytecode loop for recursive member execution.
func (s *Stmt) emitRecursiveLoop(vm *vdbe.VDBE, recursiveMember *parser.SelectStmt,
	cteName string, cteTempTables map[string]*schema.Table, state *recursiveCTEState,
	args []driver.NamedValue) error {

	iterReg := len(vm.Mem)
	vm.AllocMemory(iterReg + 3)
	limitReg := iterReg + 1
	cmpReg := iterReg + 2

	vm.AddOp(vdbe.OpInteger, 0, iterReg, 0)
	vm.AddOp(vdbe.OpInteger, 1000, limitReg, 0)

	loopStart := vm.NumOps()
	vm.AddOp(vdbe.OpAddImm, iterReg, 1, 0)
	vm.AddOp(vdbe.OpGt, iterReg, limitReg, cmpReg)
	limitCheckAddr := vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)

	if err := s.emitRecursiveMemberInlined(vm, recursiveMember, cteName, cteTempTables, state, args); err != nil {
		return err
	}

	checkAddr := vm.AddOp(vdbe.OpRewind, state.nextCursor, 0, 0)
	s.emitQueueSwap(vm, state)
	vm.AddOp(vdbe.OpGoto, 0, loopStart, 0)

	exitAddr := vm.NumOps()
	vm.Program[limitCheckAddr].P2 = exitAddr
	vm.Program[checkAddr].P2 = exitAddr

	return nil
}

// emitRecursiveMemberInlined compiles and inlines the recursive member bytecode.
func (s *Stmt) emitRecursiveMemberInlined(vm *vdbe.VDBE, recursiveMember *parser.SelectStmt,
	cteName string, cteTempTables map[string]*schema.Table, state *recursiveCTEState,
	args []driver.NamedValue) error {

	recTempTables := make(map[string]*schema.Table)
	for k, v := range cteTempTables {
		recTempTables[k] = v
	}

	origRootPage := state.queueTable.RootPage
	state.queueTable.RootPage = 0
	recTempTables[cteName] = state.queueTable

	rewritten := s.rewriteSelectWithCTETables(recursiveMember, recTempTables)
	compiledRec, err := s.compileCTESelect(vm, rewritten, args)
	state.queueTable.RootPage = origRootPage

	if err != nil {
		return fmt.Errorf("failed to compile recursive member: %w", err)
	}

	cursorMap := map[int]int{0: state.queueCursor}
	offsets := s.allocateRecursiveCTEResources(vm, compiledRec, cursorMap)
	cursors := [2]int{state.resultCursor, state.nextCursor}
	s.inlineCTEWithAddrMap(vm, compiledRec, cursors, offsets, cursorMap)
	return nil
}

// allocateRecursiveCTEResources allocates resources for recursive member inlining.
func (s *Stmt) allocateRecursiveCTEResources(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE, cursorMap map[int]int) cteInlineOffsets {
	offsets := cteInlineOffsets{}

	offsets.baseCursor = len(vm.Cursors)
	newCursors := len(compiledCTE.Cursors) - len(cursorMap)
	if newCursors > 0 {
		vm.AllocCursors(offsets.baseCursor + newCursors)
	}

	offsets.baseRegister = len(vm.Mem)
	if len(compiledCTE.Mem) > 0 {
		vm.AllocMemory(offsets.baseRegister + len(compiledCTE.Mem))
	}

	offsets.baseSorter = len(vm.Sorters)
	for i := 0; i < len(compiledCTE.Sorters); i++ {
		vm.Sorters = append(vm.Sorters, nil)
	}

	offsets.recordReg = len(vm.Mem)
	vm.AllocMemory(offsets.recordReg + 1)
	offsets.startAddr = vm.NumOps()

	return offsets
}

// emitQueueSwap generates bytecode to move nextQueue rows into queue and clear nextQueue.
func (s *Stmt) emitQueueSwap(vm *vdbe.VDBE, state *recursiveCTEState) {
	vm.AddOp(vdbe.OpClearEphemeral, state.queueCursor, 0, 0)

	copyBase := len(vm.Mem)
	vm.AllocMemory(copyBase + state.numColumns + 1)
	copyRecordReg := copyBase + state.numColumns

	copyStart := vm.NumOps()
	for i := 0; i < state.numColumns; i++ {
		vm.AddOp(vdbe.OpColumn, state.nextCursor, i, copyBase+i)
	}
	vm.AddOp(vdbe.OpMakeRecord, copyBase, state.numColumns, copyRecordReg)
	vm.AddOp(vdbe.OpInsert, state.queueCursor, copyRecordReg, 0)
	vm.AddOp(vdbe.OpNext, state.nextCursor, copyStart, 0)

	vm.AddOp(vdbe.OpClearEphemeral, state.nextCursor, 0, 0)
}

// buildAddrMap builds a mapping from CTE instruction index to main VM address,
// accounting for instruction expansion (ResultRow -> 3 instructions).
func (s *Stmt) buildAddrMap(compiledCTE *vdbe.VDBE, startAddr int) []int {
	addrMap := make([]int, len(compiledCTE.Program)+1)
	mainAddr := startAddr
	for i, instr := range compiledCTE.Program {
		addrMap[i] = mainAddr
		if instr.Opcode == vdbe.OpResultRow {
			mainAddr += 3 // MakeRecord + Insert + Insert
		} else {
			mainAddr++
		}
	}
	addrMap[len(compiledCTE.Program)] = mainAddr // address past the end
	return addrMap
}

// inlineCTEWithAddrMap inlines CTE bytecode using a proper address mapping
// that accounts for instruction expansion.
func (s *Stmt) inlineCTEWithAddrMap(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE,
	cursors [2]int, offsets cteInlineOffsets, cursorMap map[int]int) {

	addrMap := s.buildAddrMap(compiledCTE, offsets.startAddr)

	for i, instr := range compiledCTE.Program {
		_ = i // used in addrMap
		newInstr := s.adjustInstrWithMap(instr, offsets, cursorMap)

		if instr.Opcode == vdbe.OpResultRow {
			s.emitMultiInsert(vm, &newInstr, instr, cursors, offsets)
			continue
		}

		if instr.Opcode == vdbe.OpHalt {
			newInstr.Opcode = vdbe.OpNoop
		}

		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].Comment = instr.Comment
		s.fixJumpWithAddrMap(vm, instr, addr, addrMap)
	}
}

// emitMultiInsert emits MakeRecord + Insert into two cursors for a ResultRow replacement.
func (s *Stmt) emitMultiInsert(vm *vdbe.VDBE, newInstr *vdbe.Instruction,
	instr *vdbe.Instruction, cursors [2]int, offsets cteInlineOffsets) {

	newInstr.Opcode = vdbe.OpMakeRecord
	newInstr.P3 = offsets.recordReg
	addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
	vm.Program[addr].P4 = instr.P4
	vm.Program[addr].Comment = "CTE: make record"
	vm.AddOp(vdbe.OpInsert, cursors[0], offsets.recordReg, 0)
	vm.AddOp(vdbe.OpInsert, cursors[1], offsets.recordReg, 0)
}

// adjustInstrWithMap adjusts instruction parameters with optional cursor mapping.
func (s *Stmt) adjustInstrWithMap(instr *vdbe.Instruction,
	offsets cteInlineOffsets, cursorMap map[int]int) vdbe.Instruction {

	if cursorMap == nil {
		return s.adjustInstructionParameters(instr, offsets)
	}

	newInstr := *instr
	p1, p2, p3 := instr.P1, instr.P2, instr.P3
	p1, p2, p3 = adjustRegisterNumbers(instr.Opcode, p1, p2, p3, offsets.baseRegister)

	if needsCursorAdjustment(instr.Opcode) {
		if mapped, ok := cursorMap[instr.P1]; ok {
			p1 = mapped
		} else {
			p1 = instr.P1 + offsets.baseCursor
		}
	}

	if needsSorterAdjustment(instr.Opcode) {
		p1 = instr.P1 + offsets.baseSorter
	}

	newInstr.P1 = p1
	newInstr.P2 = p2
	newInstr.P3 = p3
	return newInstr
}

// fixJumpWithAddrMap fixes jump targets using the address map.
func (s *Stmt) fixJumpWithAddrMap(vm *vdbe.VDBE, instr *vdbe.Instruction, addr int, addrMap []int) {
	if !isJumpOpcode(instr.Opcode) {
		return
	}
	if instr.P2 >= 0 && instr.P2 < len(addrMap) {
		vm.Program[addr].P2 = addrMap[instr.P2]
	}
}

// isJumpOpcode returns true if the opcode uses P2 as a jump target.
func isJumpOpcode(op vdbe.Opcode) bool {
	switch op {
	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos,
		vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev,
		vdbe.OpSorterSort, vdbe.OpSorterNext:
		return true
	}
	return false
}
