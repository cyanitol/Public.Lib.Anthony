// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
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
	dedupSetID   int  // DistinctSets key for UNION dedup (-2 = none)
	isUnion      bool // true for UNION (dedup), false for UNION ALL
}

// compileRecursiveCTEBytecode generates runtime bytecode for a recursive CTE.
func (s *Stmt) compileRecursiveCTEBytecode(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	compound, err := s.validateRecursiveCTE(def, cteName)
	if err != nil {
		return nil, err
	}

	state := s.setupRecursiveCTEState(vm, cteName, def)
	state.isUnion = compound.Op == parser.CompoundUnion
	state.dedupSetID = -2 // sentinel: no dedup
	if state.isUnion {
		state.dedupSetID = -3 - len(cteTempTables) // unique per CTE
	}
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
	s.inlineCTEWithDedup(vm, compiledAnchor, cursors, offsets, nil, state)
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

// cteMarkerPage is a sentinel RootPage value used to identify CTE table
// cursors in sub-VM bytecode. It must be large enough to never collide
// with real b-tree page numbers.
const cteMarkerPage = 2000000

// emitRecursiveMemberInlined compiles and inlines the recursive member bytecode.
func (s *Stmt) emitRecursiveMemberInlined(vm *vdbe.VDBE, recursiveMember *parser.SelectStmt,
	cteName string, cteTempTables map[string]*schema.Table, state *recursiveCTEState,
	args []driver.NamedValue) error {

	recTempTables := make(map[string]*schema.Table)
	for k, v := range cteTempTables {
		recTempTables[k] = v
	}

	// Temporarily mark the queue table as a non-temp table with a sentinel
	// RootPage so the sub-VM compiler emits a normal OpenRead for it.
	// This avoids cursor-0 collisions with real tables in JOIN queries and
	// lets findCTECursorByMarker detect the CTE cursor reliably.
	origRootPage := state.queueTable.RootPage
	origTemp := state.queueTable.Temp
	state.queueTable.RootPage = cteMarkerPage
	state.queueTable.Temp = false
	recTempTables[cteName] = state.queueTable

	rewritten := s.rewriteSelectWithCTETables(recursiveMember, recTempTables)
	compiledRec, err := s.compileCTESelect(vm, rewritten, args)
	state.queueTable.RootPage = origRootPage
	state.queueTable.Temp = origTemp

	if err != nil {
		return fmt.Errorf("failed to compile recursive member: %w", err)
	}

	// Find which cursor in the sub-VM corresponds to the CTE self-reference.
	// The marker page value uniquely identifies the OpenRead for the CTE.
	cteCursor := findCTECursorByMarker(compiledRec, cteMarkerPage)
	cursorMap := map[int]int{cteCursor: state.queueCursor}

	// Fix unfixed inner Rewind jump targets in the sub-VM. The JOIN compiler
	// leaves inner Rewind P2=0 which must be patched to skip the loop body.
	fixInnerRewindAddresses(compiledRec)
	offsets := s.allocateRecursiveCTEResources(vm, compiledRec, cursorMap)
	cursors := [2]int{state.resultCursor, state.nextCursor}
	s.inlineCTEWithDedup(vm, compiledRec, cursors, offsets, cursorMap, state)
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
	return s.buildAddrMapWithExpansion(compiledCTE, startAddr, 3)
}

// buildSimpleAddrMap builds a 1:1 address map (no instruction expansion).
func (s *Stmt) buildSimpleAddrMap(compiled *vdbe.VDBE, startAddr int) []int {
	return s.buildAddrMapWithExpansion(compiled, startAddr, 1)
}

// inlineCTEWithAddrMap inlines CTE bytecode using a proper address mapping
// that accounts for instruction expansion.
func (s *Stmt) inlineCTEWithAddrMap(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE,
	cursors [2]int, offsets cteInlineOffsets, cursorMap map[int]int) {

	addrMap := s.buildAddrMap(compiledCTE, offsets.startAddr)
	onResultRow := func(v *vdbe.VDBE, newInstr *vdbe.Instruction, origInstr *vdbe.Instruction) bool {
		s.emitMultiInsert(v, newInstr, origInstr, cursors, offsets)
		return true
	}
	s.inlineBytecodeCore(vm, compiledCTE, addrMap, offsets, cursorMap, true, onResultRow)
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

// inlineCTEWithDedup inlines CTE bytecode, optionally adding UNION dedup checks.
// For UNION ALL (state.isUnion=false) delegates to inlineCTEWithAddrMap.
func (s *Stmt) inlineCTEWithDedup(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE,
	cursors [2]int, offsets cteInlineOffsets, cursorMap map[int]int, state *recursiveCTEState) {

	if !state.isUnion {
		s.inlineCTEWithAddrMap(vm, compiledCTE, cursors, offsets, cursorMap)
		return
	}
	s.inlineCTEWithDedupInner(vm, compiledCTE, cursors, offsets, cursorMap, state)
}

// inlineCTEWithDedupInner inlines CTE bytecode with UNION dedup via OpDistinctRow.
func (s *Stmt) inlineCTEWithDedupInner(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE,
	cursors [2]int, offsets cteInlineOffsets, cursorMap map[int]int, state *recursiveCTEState) {

	addrMap := s.buildDedupAddrMap(compiledCTE, offsets.startAddr)
	onResultRow := func(v *vdbe.VDBE, newInstr *vdbe.Instruction, origInstr *vdbe.Instruction) bool {
		s.emitDedupMultiInsert(v, newInstr, origInstr, cursors, offsets, state)
		return true
	}
	s.inlineBytecodeCore(vm, compiledCTE, addrMap, offsets, cursorMap, true, onResultRow)
}

// buildDedupAddrMap builds an address map where ResultRow expands to 4 instructions
// (MakeRecord + DistinctRow + Insert + Insert) instead of 3.
func (s *Stmt) buildDedupAddrMap(compiledCTE *vdbe.VDBE, startAddr int) []int {
	return s.buildAddrMapWithExpansion(compiledCTE, startAddr, 4)
}

// buildAddrMapWithExpansion builds an address map from CTE instruction indices to main VM
// addresses. Each OpResultRow expands to resultRowExpansion instructions; all others map 1:1.
func (s *Stmt) buildAddrMapWithExpansion(compiled *vdbe.VDBE, startAddr int, resultRowExpansion int) []int {
	addrMap := make([]int, len(compiled.Program)+1)
	mainAddr := startAddr
	for i, instr := range compiled.Program {
		addrMap[i] = mainAddr
		if instr.Opcode == vdbe.OpResultRow {
			mainAddr += resultRowExpansion
		} else {
			mainAddr++
		}
	}
	addrMap[len(compiled.Program)] = mainAddr
	return addrMap
}

// emitDedupMultiInsert emits MakeRecord + DistinctRow + Insert(x2) for UNION dedup.
func (s *Stmt) emitDedupMultiInsert(vm *vdbe.VDBE, newInstr *vdbe.Instruction,
	instr *vdbe.Instruction, cursors [2]int, offsets cteInlineOffsets, state *recursiveCTEState) {

	// 1. MakeRecord
	newInstr.Opcode = vdbe.OpMakeRecord
	newInstr.P3 = offsets.recordReg
	addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
	vm.Program[addr].P4 = instr.P4
	vm.Program[addr].Comment = "CTE: make record"

	// 2. DistinctRow - skip inserts if duplicate
	skipTarget := vm.NumOps() + 3 // past DistinctRow + 2 Inserts
	dedupAddr := vm.AddOp(vdbe.OpDistinctRow, newInstr.P1, skipTarget, newInstr.P2)
	vm.Program[dedupAddr].P5 = uint16(int16(state.dedupSetID))
	vm.Program[dedupAddr].Comment = "CTE UNION: dedup check"

	// 3. Insert into both cursors
	vm.AddOp(vdbe.OpInsert, cursors[0], offsets.recordReg, 0)
	vm.AddOp(vdbe.OpInsert, cursors[1], offsets.recordReg, 0)
}

// resultRowHandler is called when a ResultRow opcode is encountered during
// bytecode inlining. It receives the adjusted and original instructions and
// must emit replacement bytecode into vm. Return true to skip the default
// AddOp for this instruction (i.e. the handler already emitted ops).
type resultRowHandler func(vm *vdbe.VDBE, newInstr *vdbe.Instruction, origInstr *vdbe.Instruction) bool

// inlineBytecodeCore is the shared core loop for inlining sub-VM bytecode into
// the main VM. It adjusts cursors/registers via adjustInstrWithMap, converts
// Init (and optionally Halt) plus mapped-cursor ops to Noop, and delegates
// ResultRow handling to the provided callback.
func (s *Stmt) inlineBytecodeCore(vm *vdbe.VDBE, compiled *vdbe.VDBE,
	addrMap []int, offsets cteInlineOffsets, cursorMap map[int]int,
	convertHalt bool, onResultRow resultRowHandler) {

	for _, instr := range compiled.Program {
		newInstr := s.adjustInstrWithMap(instr, offsets, cursorMap)

		if instr.Opcode == vdbe.OpResultRow && onResultRow != nil {
			if onResultRow(vm, &newInstr, instr) {
				continue
			}
		}

		switch {
		case instr.Opcode == vdbe.OpInit:
			newInstr.Opcode = vdbe.OpNoop
		case convertHalt && instr.Opcode == vdbe.OpHalt:
			newInstr.Opcode = vdbe.OpNoop
		case isMappedCursorOp(instr, cursorMap):
			newInstr.Opcode = vdbe.OpNoop
		}

		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].P4Type = instr.P4Type
		vm.Program[addr].P5 = instr.P5
		vm.Program[addr].Comment = instr.Comment
		s.fixJumpWithAddrMap(vm, instr, addr, addrMap)
	}
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

// isMappedCursorOp returns true if this instruction opens or closes a cursor
// that is already mapped (i.e. an already-open ephemeral table).
func isMappedCursorOp(instr *vdbe.Instruction, cursorMap map[int]int) bool {
	if cursorMap == nil {
		return false
	}
	switch instr.Opcode {
	case vdbe.OpOpenRead, vdbe.OpOpenWrite, vdbe.OpClose:
		_, mapped := cursorMap[instr.P1]
		return mapped
	}
	return false
}

// fixInnerRewindAddresses patches Rewind instructions with P2=0 so they
// skip past the matching Next instruction for the same cursor. The JOIN
// compiler leaves inner-loop Rewind P2 unfixed at 0.
func fixInnerRewindAddresses(compiledCTE *vdbe.VDBE) {
	for i, instr := range compiledCTE.Program {
		if instr.Opcode != vdbe.OpRewind || instr.P2 != 0 {
			continue
		}
		cursor := instr.P1
		// Find the matching Next for this cursor after the Rewind.
		for j := i + 1; j < len(compiledCTE.Program); j++ {
			if compiledCTE.Program[j].Opcode == vdbe.OpNext && compiledCTE.Program[j].P1 == cursor {
				// Skip target is the instruction after Next.
				compiledCTE.Program[i].P2 = j + 1
				break
			}
		}
	}
}

// findCTECursorByMarker scans compiled bytecode for the cursor that
// references a CTE temp table identified by a marker RootPage value.
func findCTECursorByMarker(compiledCTE *vdbe.VDBE, marker int) int {
	for _, instr := range compiledCTE.Program {
		if instr.Opcode == vdbe.OpOpenRead && instr.P2 == marker {
			return instr.P1
		}
	}
	return 0
}

// isJumpOpcode returns true if the opcode uses P2 as a jump target.
func isJumpOpcode(op vdbe.Opcode) bool {
	switch op {
	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos,
		vdbe.OpIsNull, vdbe.OpNotNull,
		vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev,
		vdbe.OpSorterSort, vdbe.OpSorterNext:
		return true
	}
	return false
}
