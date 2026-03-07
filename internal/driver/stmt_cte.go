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

// compileSelectWithCTEs compiles a SELECT statement with a WITH clause (CTEs).
func (s *Stmt) compileSelectWithCTEs(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	cteCtx, err := s.createAndValidateCTEContext(stmt.With)
	if err != nil {
		return nil, err
	}

	cteTempTables, err := s.materializeAllCTEs(vm, cteCtx, args)
	if err != nil {
		return nil, err
	}

	return s.compileMainQueryWithCTEs(vm, stmt, cteTempTables, args)
}

// createAndValidateCTEContext creates and validates the CTE context
func (s *Stmt) createAndValidateCTEContext(with *parser.WithClause) (*planner.CTEContext, error) {
	cteCtx, err := planner.NewCTEContext(with)
	if err != nil {
		return nil, fmt.Errorf("failed to create CTE context: %w", err)
	}

	if err := cteCtx.ValidateCTEs(); err != nil {
		return nil, fmt.Errorf("CTE validation failed: %w", err)
	}

	return cteCtx, nil
}

// materializeAllCTEs materializes all CTEs in dependency order
func (s *Stmt) materializeAllCTEs(vm *vdbe.VDBE, cteCtx *planner.CTEContext, args []driver.NamedValue) (map[string]*schema.Table, error) {
	cteTempTables := make(map[string]*schema.Table)
	for _, cteName := range cteCtx.CTEOrder {
		tempTable, err := s.compileSingleCTE(vm, cteName, cteCtx, cteTempTables, args)
		if err != nil {
			return nil, err
		}
		cteTempTables[cteName] = tempTable
	}
	return cteTempTables, nil
}

// compileSingleCTE compiles a single CTE (recursive or non-recursive)
func (s *Stmt) compileSingleCTE(vm *vdbe.VDBE, cteName string, cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {
	def, exists := cteCtx.GetCTE(cteName)
	if !exists {
		return nil, fmt.Errorf("CTE not found in context: %s", cteName)
	}

	if def.IsRecursive {
		return s.compileRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
	}
	return s.compileNonRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
}

// compileMainQueryWithCTEs compiles the main query with CTE references rewritten
func (s *Stmt) compileMainQueryWithCTEs(vm *vdbe.VDBE, stmt *parser.SelectStmt, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	mainStmt := s.rewriteSelectWithCTETables(stmt, cteTempTables)
	mainStmt.With = nil
	return s.compileSelect(vm, mainStmt, args)
}

// compileNonRecursiveCTE compiles a non-recursive CTE into a temporary table using coroutines.
// This generates bytecode that materializes the CTE at runtime using OpInitCoroutine, OpYield, and OpEndCoroutine.
func (s *Stmt) compileNonRecursiveCTE(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	// Create a temporary table to hold CTE results
	tempTableName := fmt.Sprintf("_cte_%s", cteName)
	tempTable := s.createCTETempTable(tempTableName, def)

	// Allocate a cursor for the ephemeral table
	cursorNum := len(vm.Cursors)
	vm.AllocCursors(cursorNum + 1)

	// Allocate a coroutine ID
	coroutineID := len(vm.Coroutines)
	if vm.Coroutines == nil {
		vm.Coroutines = make(map[int]*vdbe.CoroutineInfo)
	}

	// Store cursor number in temp table metadata for later reference
	tempTable.RootPage = uint32(cursorNum) // Use RootPage to store cursor number for ephemeral tables

	// Register the temp table in the schema so it can be found during compilation
	s.conn.schema.AddTableDirect(tempTable)

	// Rewrite the CTE's SELECT to use already-materialized CTEs
	cteSelect := s.rewriteSelectWithCTETables(def.Select, cteTempTables)

	// Generate bytecode to populate the ephemeral table using a coroutine
	if err := s.compileCTEPopulationCoroutine(vm, cteSelect, cursorNum, coroutineID, len(tempTable.Columns), args); err != nil {
		return nil, fmt.Errorf("failed to compile CTE population: %w", err)
	}

	return tempTable, nil
}

// compileCTEPopulation generates bytecode to populate an ephemeral table with CTE results.
//
// SCAFFOLDING: Alternative CTE population using bytecode inlining.
// Currently unused - the active implementation uses coroutines (compileCTEPopulationCoroutine).
// This function will be used when implementing:
// 1. CTE materialization optimization for multiple references
// 2. Recursive CTE support (requires iterative bytecode generation)
func (s *Stmt) compileCTEPopulation(vm *vdbe.VDBE, cteSelect *parser.SelectStmt, cursorNum int, numColumns int, args []driver.NamedValue) error {
	// Compile the CTE SELECT to generate rows
	compiledCTE, err := s.compileCTESelect(vm, cteSelect, args)
	if err != nil {
		return err
	}

	// Allocate resources for inlining CTE bytecode
	offsets := s.allocateCTEResources(vm, compiledCTE)

	// Copy CTE bytecode into main VM with adjustments
	s.inlineCTEBytecode(vm, compiledCTE, cursorNum, offsets)

	return nil
}

// compileCTESelect compiles the CTE SELECT statement.
func (s *Stmt) compileCTESelect(vm *vdbe.VDBE, cteSelect *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	cteVM := vdbe.New()
	cteVM.Ctx = vm.Ctx
	compiledCTE, err := s.compileSelect(cteVM, cteSelect, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile CTE SELECT: %w", err)
	}
	return compiledCTE, nil
}

// cteInlineOffsets holds offset information for inlining CTE bytecode.
type cteInlineOffsets struct {
	baseCursor   int
	baseRegister int
	recordReg    int
	startAddr    int
}

// allocateCTEResources allocates cursors and registers for CTE inlining.
func (s *Stmt) allocateCTEResources(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE) cteInlineOffsets {
	offsets := cteInlineOffsets{}

	// Allocate cursors
	offsets.baseCursor = len(vm.Cursors)
	cteCursorCount := len(compiledCTE.Cursors)
	if cteCursorCount > 0 {
		vm.AllocCursors(offsets.baseCursor + cteCursorCount)
	}

	// Allocate registers
	offsets.baseRegister = len(vm.Mem)
	cteRegisterCount := len(compiledCTE.Mem)
	if cteRegisterCount > 0 {
		vm.AllocMemory(offsets.baseRegister + cteRegisterCount)
	}

	// Allocate record register
	offsets.recordReg = len(vm.Mem)
	vm.AllocMemory(offsets.recordReg + 1)

	// Mark where CTE bytecode starts
	offsets.startAddr = vm.NumOps()

	return offsets
}

// inlineCTEBytecode copies CTE bytecode into main VM with necessary adjustments.
//
// SCAFFOLDING: Used by compileCTEPopulation for bytecode inlining approach.
// See compileCTEPopulation comment for when this will be activated.
func (s *Stmt) inlineCTEBytecode(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE, cursorNum int, offsets cteInlineOffsets) {
	for _, instr := range compiledCTE.Program {
		newInstr := s.adjustInstructionParameters(instr, offsets)

		// Handle special opcodes
		if s.handleSpecialOpcode(vm, instr, &newInstr, cursorNum, offsets) {
			continue // Instruction already added or skipped
		}

		// Add the instruction
		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].Comment = instr.Comment

		// Adjust jump targets
		s.adjustJumpTarget(vm, instr, addr, offsets)
	}
}

// adjustInstructionParameters adjusts cursor and register numbers in an instruction.
func (s *Stmt) adjustInstructionParameters(instr *vdbe.Instruction, offsets cteInlineOffsets) vdbe.Instruction {
	newInstr := *instr
	adjustedP1, adjustedP2, adjustedP3 := instr.P1, instr.P2, instr.P3

	// First, adjust register numbers (this modifies all three parameters)
	adjustedP1, adjustedP2, adjustedP3 = adjustRegisterNumbers(
		instr.Opcode, adjustedP1, adjustedP2, adjustedP3, offsets.baseRegister,
	)

	// Then, adjust cursor numbers for cursor operations
	// This MUST come after register adjustment to override P1 for cursor ops
	if needsCursorAdjustment(instr.Opcode) {
		adjustedP1 = instr.P1 + offsets.baseCursor
	}

	newInstr.P1 = adjustedP1
	newInstr.P2 = adjustedP2
	newInstr.P3 = adjustedP3

	return newInstr
}

// handleSpecialOpcode handles ResultRow and Halt opcodes specially. Returns true if handled.
//
// SCAFFOLDING: Used by inlineCTEBytecode for opcode transformation.
// See compileCTEPopulation comment for when this will be activated.
func (s *Stmt) handleSpecialOpcode(vm *vdbe.VDBE, instr *vdbe.Instruction, newInstr *vdbe.Instruction, cursorNum int, offsets cteInlineOffsets) bool {
	switch instr.Opcode {
	case vdbe.OpResultRow:
		// Replace ResultRow with MakeRecord + Insert
		newInstr.Opcode = vdbe.OpMakeRecord
		newInstr.P3 = offsets.recordReg

		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].Comment = instr.Comment

		vm.AddOp(vdbe.OpInsert, cursorNum, offsets.recordReg, 0)
		return true

	case vdbe.OpHalt:
		// Replace Halt with Noop
		newInstr.Opcode = vdbe.OpNoop
		return false
	}
	return false
}

// adjustJumpTarget adjusts jump target addresses for jump opcodes.
func (s *Stmt) adjustJumpTarget(vm *vdbe.VDBE, instr *vdbe.Instruction, addr int, offsets cteInlineOffsets) {
	switch instr.Opcode {
	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos:
		// These opcodes use P2 as a jump target
		if instr.P2 > 0 {
			vm.Program[addr].P2 = instr.P2 + offsets.startAddr
		}
	case vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev:
		// These opcodes use P2 as a jump target
		if instr.P2 > 0 {
			vm.Program[addr].P2 = instr.P2 + offsets.startAddr
		}
		// Note: OpEq, OpNe, OpLt, OpLe, OpGt, OpGe use P2 as a register, not a jump target
		// They are handled by adjustRegisterNumbers instead
	}
}

// needsCursorAdjustment returns true if the opcode uses P1 as a cursor number.
func needsCursorAdjustment(op vdbe.Opcode) bool {
	switch op {
	case vdbe.OpOpenRead, vdbe.OpOpenWrite, vdbe.OpOpenEphemeral,
		vdbe.OpClose, vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev,
		vdbe.OpSeekGE, vdbe.OpSeekGT, vdbe.OpSeekLE, vdbe.OpSeekLT,
		vdbe.OpColumn, vdbe.OpInsert, vdbe.OpDelete:
		return true
	}
	return false
}

// adjustRegisterNumbers adjusts register numbers when inlining bytecode.
// Most opcodes use P1, P2, P3 for registers, but some use them for other purposes.
func adjustRegisterNumbers(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	// For opcodes that use cursors in P1, don't adjust P1
	if needsCursorAdjustment(op) {
		return adjustCursorOpRegisters(op, p1, p2, p3, baseReg)
	}

	// For most other opcodes, adjust register parameters
	return adjustNonCursorOpRegisters(op, p1, p2, p3, baseReg)
}

// adjustCursorOpRegisters handles register adjustment for cursor operations.
func adjustCursorOpRegisters(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	switch op {
	case vdbe.OpColumn:
		// P1=cursor, P2=column, P3=dest register
		return p1, p2, p3 + baseReg
	case vdbe.OpInsert, vdbe.OpDelete:
		// P1=cursor, P2=data register, P3=key register (or 0)
		newP2 := p2 + baseReg
		newP3 := p3
		if p3 > 0 {
			newP3 = p3 + baseReg
		}
		return p1, newP2, newP3
	case vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev:
		// P1=cursor, P2=jump target, P3=unused
		return p1, p2, p3
	default:
		// For other cursor ops, only P1 is cursor
		return p1, p2, p3
	}
}

// adjustNonCursorOpRegisters handles register adjustment for non-cursor operations.
func adjustNonCursorOpRegisters(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	switch {
	case isValueLoadOp(op):
		return p1, p2 + baseReg, p3
	case isRecordOp(op):
		return p1 + baseReg, p2, p3 + baseReg
	case isCopyOp(op) || isUnaryOp(op):
		return p1 + baseReg, p2 + baseReg, p3
	case isArithmeticOrComparisonOp(op):
		return adjustArithmeticAndComparisonOps(op, p1, p2, p3, baseReg)
	case isJumpOp(op):
		return adjustJumpOps(op, p1, p2, p3, baseReg)
	default:
		return p1, p2, p3
	}
}

// isValueLoadOp checks if op is a value loading operation
func isValueLoadOp(op vdbe.Opcode) bool {
	return op == vdbe.OpInteger || op == vdbe.OpReal || op == vdbe.OpString8 || op == vdbe.OpBlob || op == vdbe.OpNull
}

// isRecordOp checks if op is a record operation
func isRecordOp(op vdbe.Opcode) bool {
	return op == vdbe.OpResultRow || op == vdbe.OpMakeRecord
}

// isCopyOp checks if op is a copy operation
func isCopyOp(op vdbe.Opcode) bool {
	return op == vdbe.OpCopy || op == vdbe.OpSCopy
}

// isArithmeticOrComparisonOp checks if op is arithmetic or comparison
func isArithmeticOrComparisonOp(op vdbe.Opcode) bool {
	return isArithmeticOp(op) || isComparisonOp(op)
}

// isArithmeticOp checks if op is an arithmetic operation
func isArithmeticOp(op vdbe.Opcode) bool {
	return op == vdbe.OpAdd || op == vdbe.OpSubtract || op == vdbe.OpMultiply ||
		op == vdbe.OpDivide || op == vdbe.OpRemainder || op == vdbe.OpConcat
}

// isComparisonOp checks if op is a comparison operation
func isComparisonOp(op vdbe.Opcode) bool {
	return op == vdbe.OpEq || op == vdbe.OpNe || op == vdbe.OpLt ||
		op == vdbe.OpLe || op == vdbe.OpGt || op == vdbe.OpGe
}

// isUnaryOp checks if op is a unary operation
func isUnaryOp(op vdbe.Opcode) bool {
	return op == vdbe.OpNot || op == vdbe.OpBitNot
}

// isJumpOp checks if op is a jump operation
func isJumpOp(op vdbe.Opcode) bool {
	return op == vdbe.OpGoto || op == vdbe.OpIf || op == vdbe.OpIfNot || op == vdbe.OpIfPos
}

// isControlFlowOp checks if op is a control flow operation.
// SCAFFOLDING: Helper for bytecode analysis, will be used in recursive CTE implementation.
func isControlFlowOp(op vdbe.Opcode) bool {
	return op == vdbe.OpInit || op == vdbe.OpHalt || op == vdbe.OpNoop
}

// adjustArithmeticAndComparisonOps handles register adjustment for arithmetic and comparison operations.
func adjustArithmeticAndComparisonOps(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	// Both arithmetic and comparison ops use P1, P2, P3 as registers
	// P1 = left operand register
	// P2 = right operand register
	// P3 = result/destination register
	return p1 + baseReg, p2 + baseReg, p3 + baseReg
}

// adjustJumpOps handles register adjustment for jump operations.
func adjustJumpOps(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	// P1=register or unused, P2=jump target
	if op == vdbe.OpGoto {
		return p1, p2, p3
	}
	return p1 + baseReg, p2, p3
}

// compileRecursiveCTE compiles a recursive CTE using iterative execution.
func (s *Stmt) compileRecursiveCTE(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	compound, err := s.validateRecursiveCTE(def, cteName)
	if err != nil {
		return nil, err
	}

	// Create and initialize temp tables
	tempTable, currentTable, resultCursor, currentCursor, err := s.setupRecursiveTables(vm, cteName, def)
	if err != nil {
		return nil, err
	}

	// Register temp tables in schema
	s.registerRecursiveTempTables(tempTable, currentTable)

	// Step 1: Execute anchor member
	numColumns := len(tempTable.Columns)
	anchorRows, err := s.executeAnchorMember(vm, compound.Left, cteTempTables, numColumns, args)
	if err != nil {
		return nil, err
	}

	// Materialize anchor results
	baseReg := len(vm.Mem)
	vm.AllocMemory(baseReg + numColumns + 2)
	recordReg := baseReg + numColumns
	s.materializeRows(vm, anchorRows, numColumns, baseReg, recordReg, resultCursor, currentCursor)

	// Step 2: Iterate recursive member
	err = s.executeRecursiveIterations(vm, compound.Right, cteName, currentTable, cteTempTables,
		numColumns, baseReg, recordReg, resultCursor, currentCursor, args)
	if err != nil {
		return nil, err
	}

	return tempTable, nil
}

// validateRecursiveCTE validates that a CTE is properly structured for recursion.
func (s *Stmt) validateRecursiveCTE(def *planner.CTEDefinition, cteName string) (*parser.CompoundSelect, error) {
	if def.Select.Compound == nil {
		return nil, fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", cteName)
	}

	compound := def.Select.Compound
	if compound.Op != parser.CompoundUnion && compound.Op != parser.CompoundUnionAll {
		return nil, fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", cteName)
	}

	return compound, nil
}

// setupRecursiveTables creates and initializes the ephemeral tables for recursive CTE execution.
func (s *Stmt) setupRecursiveTables(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition) (
	*schema.Table, *schema.Table, int, int, error) {

	tempTableName := fmt.Sprintf("_cte_%s", cteName)
	currentTableName := fmt.Sprintf("_cte_%s_current", cteName)

	tempTable := s.createCTETempTable(tempTableName, def)
	currentTable := s.createCTETempTable(currentTableName, def)

	// Allocate cursors for both ephemeral tables
	resultCursor := len(vm.Cursors)
	vm.AllocCursors(resultCursor + 1)
	currentCursor := len(vm.Cursors)
	vm.AllocCursors(currentCursor + 1)

	// Open ephemeral tables
	numColumns := len(tempTable.Columns)
	vm.AddOp(vdbe.OpOpenEphemeral, resultCursor, numColumns, 0)
	vm.AddOp(vdbe.OpOpenEphemeral, currentCursor, numColumns, 0)

	// Store cursor numbers in temp tables
	tempTable.RootPage = uint32(resultCursor)
	currentTable.RootPage = uint32(currentCursor)

	return tempTable, currentTable, resultCursor, currentCursor, nil
}

// registerRecursiveTempTables registers temporary tables in the schema.
func (s *Stmt) registerRecursiveTempTables(tempTable, currentTable *schema.Table) {
	s.conn.schema.AddTableDirect(tempTable)
	s.conn.schema.AddTableDirect(currentTable)
}

// executeAnchorMember executes the anchor (non-recursive) part of a recursive CTE.
func (s *Stmt) executeAnchorMember(vm *vdbe.VDBE, anchorSelect *parser.SelectStmt,
	cteTempTables map[string]*schema.Table, numColumns int, args []driver.NamedValue) ([][]interface{}, error) {

	rewrittenAnchor := s.rewriteSelectWithCTETables(anchorSelect, cteTempTables)

	anchorVM := vdbe.New()
	anchorVM.Ctx = vm.Ctx
	compiledAnchor, err := s.compileSelect(anchorVM, rewrittenAnchor, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile recursive CTE anchor: %w", err)
	}

	return s.collectRows(compiledAnchor, numColumns, "anchor")
}

// collectRows collects all rows from a compiled VDBE execution.
func (s *Stmt) collectRows(vm *vdbe.VDBE, numColumns int, description string) ([][]interface{}, error) {
	var rows [][]interface{}
	for {
		hasRow, err := vm.Step()
		if err != nil {
			return nil, fmt.Errorf("%s execution failed: %w", description, err)
		}
		if !hasRow {
			break
		}
		// Copy the result row
		row := make([]interface{}, numColumns)
		for i := 0; i < numColumns && i < len(vm.ResultRow); i++ {
			row[i] = vm.ResultRow[i].Value()
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// materializeRows generates bytecode to materialize rows into ephemeral tables.
func (s *Stmt) materializeRows(vm *vdbe.VDBE, rows [][]interface{}, numColumns, baseReg, recordReg,
	resultCursor, currentCursor int) {

	for _, row := range rows {
		s.emitRowLoadBytecode(vm, row, numColumns, baseReg)
		vm.AddOp(vdbe.OpMakeRecord, baseReg, numColumns, recordReg)
		vm.AddOp(vdbe.OpInsert, resultCursor, recordReg, 0)
		vm.AddOp(vdbe.OpInsert, currentCursor, recordReg, 0)
	}
}

// emitRowLoadBytecode generates bytecode to load a row's values into registers.
func (s *Stmt) emitRowLoadBytecode(vm *vdbe.VDBE, row []interface{}, numColumns, baseReg int) {
	for i := 0; i < numColumns; i++ {
		switch v := row[i].(type) {
		case nil:
			vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
		case int64:
			vm.AddOp(vdbe.OpInteger, int(v), baseReg+i, 0)
		case float64:
			vm.AddOpWithP4Real(vdbe.OpReal, 0, baseReg+i, 0, v)
		case string:
			vm.AddOpWithP4Str(vdbe.OpString8, 0, baseReg+i, 0, v)
		case []byte:
			vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), baseReg+i, 0, v)
		default:
			vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
		}
	}
}

// executeRecursiveIterations executes the recursive member until no new rows are produced.
func (s *Stmt) executeRecursiveIterations(vm *vdbe.VDBE, recursiveMember *parser.SelectStmt,
	cteName string, currentTable *schema.Table, cteTempTables map[string]*schema.Table,
	numColumns, baseReg, recordReg, resultCursor, currentCursor int, args []driver.NamedValue) error {

	maxIterations := 1000

	for iteration := 0; iteration < maxIterations; iteration++ {
		newRows, err := s.executeRecursiveMember(vm, recursiveMember, cteName, currentTable,
			cteTempTables, numColumns, args)
		if err != nil {
			return err
		}

		// If no new rows, exit loop
		if len(newRows) == 0 {
			break
		}

		// Materialize new rows
		s.materializeRows(vm, newRows, numColumns, baseReg, recordReg, resultCursor, currentCursor)
	}

	return nil
}

// executeRecursiveMember executes one iteration of the recursive member.
func (s *Stmt) executeRecursiveMember(vm *vdbe.VDBE, recursiveMember *parser.SelectStmt,
	cteName string, currentTable *schema.Table, cteTempTables map[string]*schema.Table,
	numColumns int, args []driver.NamedValue) ([][]interface{}, error) {

	// Build temp tables map with CTE pointing to current table
	recursiveTempTables := make(map[string]*schema.Table)
	for k, v := range cteTempTables {
		recursiveTempTables[k] = v
	}
	recursiveTempTables[cteName] = currentTable

	rewrittenRecursive := s.rewriteSelectWithCTETables(recursiveMember, recursiveTempTables)
	recursiveVM := vdbe.New()
	recursiveVM.Ctx = vm.Ctx
	compiledRecursive, err := s.compileSelect(recursiveVM, rewrittenRecursive, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile recursive member: %w", err)
	}

	return s.collectRows(compiledRecursive, numColumns, "recursive member")
}

func (s *Stmt) createCTETempTable(tableName string, def *planner.CTEDefinition) *schema.Table {
	var columns []*schema.Column

	if len(def.Columns) > 0 {
		columns = s.createColumnsFromExplicitList(def.Columns)
	} else if def.Select != nil && len(def.Select.Columns) > 0 {
		// Expand SELECT * to actual columns before creating temp table
		expandedCols := s.expandStarColumns(def.Select)
		columns = s.createColumnsFromSelect(expandedCols)
	}

	return &schema.Table{
		Name:     tableName,
		Columns:  columns,
		RootPage: 0,
		Temp:     true,
	}
}

// createColumnsFromExplicitList creates columns from explicit column list
func (s *Stmt) createColumnsFromExplicitList(columnNames []string) []*schema.Column {
	columns := make([]*schema.Column, len(columnNames))
	for i, colName := range columnNames {
		columns[i] = &schema.Column{
			Name:    colName,
			Type:    "ANY",
			NotNull: false,
		}
	}
	return columns
}

// createColumnsFromSelect creates columns from SELECT columns
func (s *Stmt) createColumnsFromSelect(selectCols []parser.ResultColumn) []*schema.Column {
	var columns []*schema.Column

	for i, col := range selectCols {
		colName := s.inferColumnName(col, i)
		columns = append(columns, &schema.Column{
			Name:    colName,
			Type:    "ANY",
			NotNull: false,
		})
	}
	return columns
}

// inferColumnName infers column name from result column
func (s *Stmt) inferColumnName(col parser.ResultColumn, index int) string {
	if col.Alias != "" {
		return col.Alias
	}
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return fmt.Sprintf("column_%d", index+1)
}

// expandStarColumns expands SELECT * to individual columns from the source table(s)
func (s *Stmt) expandStarColumns(stmt *parser.SelectStmt) []parser.ResultColumn {
	var expandedCols []parser.ResultColumn

	for _, col := range stmt.Columns {
		if col.Star {
			// Get the table(s) from FROM clause and expand their columns
			if stmt.From != nil && len(stmt.From.Tables) > 0 {
				for _, tableOrSub := range stmt.From.Tables {
					if tableOrSub.TableName != "" {
						// Look up the table in schema
						if table, ok := s.conn.schema.GetTable(tableOrSub.TableName); ok {
							// Add each column from the table
							for _, schemaCol := range table.Columns {
								expandedCols = append(expandedCols, parser.ResultColumn{
									Expr: &parser.IdentExpr{Name: schemaCol.Name},
								})
							}
						}
					}
				}
			}
		} else {
			// Keep non-star columns as-is
			expandedCols = append(expandedCols, col)
		}
	}

	return expandedCols
}

// rewriteSelectWithCTETables rewrites a SELECT statement to replace CTE references
// with their corresponding temporary tables.
func (s *Stmt) rewriteSelectWithCTETables(stmt *parser.SelectStmt, cteTempTables map[string]*schema.Table) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}

	// Create a copy of the statement to avoid modifying the original
	rewritten := *stmt

	// Rewrite FROM clause
	if rewritten.From != nil {
		rewritten.From = s.rewriteFromClause(rewritten.From, cteTempTables)
	}

	// Rewrite WHERE clause subqueries
	if rewritten.Where != nil {
		rewritten.Where = s.rewriteExpressionSubqueries(rewritten.Where, cteTempTables)
	}

	// Rewrite HAVING clause subqueries
	if rewritten.Having != nil {
		rewritten.Having = s.rewriteExpressionSubqueries(rewritten.Having, cteTempTables)
	}

	// Rewrite SELECT column subqueries
	for i := range rewritten.Columns {
		if rewritten.Columns[i].Expr != nil {
			rewritten.Columns[i].Expr = s.rewriteExpressionSubqueries(rewritten.Columns[i].Expr, cteTempTables)
		}
	}

	// Recursively rewrite compound queries
	if rewritten.Compound != nil {
		compound := *rewritten.Compound
		compound.Left = s.rewriteSelectWithCTETables(compound.Left, cteTempTables)
		compound.Right = s.rewriteSelectWithCTETables(compound.Right, cteTempTables)
		rewritten.Compound = &compound
	}

	return &rewritten
}

// rewriteFromClause rewrites a FROM clause to replace CTE references.
func (s *Stmt) rewriteFromClause(from *parser.FromClause, cteTempTables map[string]*schema.Table) *parser.FromClause {
	if from == nil {
		return nil
	}

	rewritten := *from

	// Rewrite base tables
	rewrittenTables := make([]parser.TableOrSubquery, len(from.Tables))
	for i, table := range from.Tables {
		rewrittenTables[i] = s.rewriteTableOrSubquery(table, cteTempTables)
	}
	rewritten.Tables = rewrittenTables

	// Rewrite JOINs
	if len(from.Joins) > 0 {
		rewrittenJoins := make([]parser.JoinClause, len(from.Joins))
		for i, join := range from.Joins {
			rewrittenJoin := join
			rewrittenJoin.Table = s.rewriteTableOrSubquery(join.Table, cteTempTables)
			rewrittenJoins[i] = rewrittenJoin
		}
		rewritten.Joins = rewrittenJoins
	}

	return &rewritten
}

// rewriteTableOrSubquery rewrites a table reference, replacing CTE names with temp tables.
func (s *Stmt) rewriteTableOrSubquery(table parser.TableOrSubquery, cteTempTables map[string]*schema.Table) parser.TableOrSubquery {
	// Check if this table name references a CTE
	if tempTable, exists := cteTempTables[table.TableName]; exists {
		// Replace with temp table name
		rewritten := table
		rewritten.TableName = tempTable.Name
		return rewritten
	}

	// If it's a subquery, recursively rewrite it
	if table.Subquery != nil {
		rewritten := table
		rewritten.Subquery = s.rewriteSelectWithCTETables(table.Subquery, cteTempTables)
		return rewritten
	}

	return table
}

// rewriteExpressionSubqueries recursively rewrites subqueries in expressions.
func (s *Stmt) rewriteExpressionSubqueries(expr parser.Expression, cteTempTables map[string]*schema.Table) parser.Expression {
	if expr == nil {
		return nil
	}
	if rewritten := s.rewriteSubqueryTypes(expr, cteTempTables); rewritten != nil {
		return rewritten
	}
	if rewritten := s.rewriteCompoundTypes(expr, cteTempTables); rewritten != nil {
		return rewritten
	}
	return expr
}

// rewriteSubqueryTypes handles subquery-related expression types.
func (s *Stmt) rewriteSubqueryTypes(expr parser.Expression, cteTempTables map[string]*schema.Table) parser.Expression {
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		return s.rewriteSubqueryExpr(e, cteTempTables)
	case *parser.InExpr:
		return s.rewriteInExpr(e, cteTempTables)
	default:
		return nil
	}
}

// rewriteCompoundTypes handles compound and nested expression types.
func (s *Stmt) rewriteCompoundTypes(expr parser.Expression, cteTempTables map[string]*schema.Table) parser.Expression {
	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return s.rewriteBinaryExpr(e, cteTempTables)
	case *parser.UnaryExpr:
		return s.rewriteUnaryExpr(e, cteTempTables)
	case *parser.CaseExpr:
		return s.rewriteCaseExpr(e, cteTempTables)
	case *parser.BetweenExpr:
		return s.rewriteBetweenExpr(e, cteTempTables)
	case *parser.FunctionExpr:
		return s.rewriteFunctionExpr(e, cteTempTables)
	case *parser.ParenExpr:
		return s.rewriteParenExpr(e, cteTempTables)
	case *parser.CastExpr:
		return s.rewriteCastExpr(e, cteTempTables)
	case *parser.CollateExpr:
		return s.rewriteCollateExpr(e, cteTempTables)
	default:
		return nil
	}
}

func (s *Stmt) rewriteSubqueryExpr(e *parser.SubqueryExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	if e.Select != nil {
		rewritten.Select = s.rewriteSelectWithCTETables(e.Select, cteTempTables)
	}
	return &rewritten
}

func (s *Stmt) rewriteInExpr(e *parser.InExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	if e.Select != nil {
		rewritten.Select = s.rewriteSelectWithCTETables(e.Select, cteTempTables)
	}
	if e.Expr != nil {
		rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	}
	for i, val := range e.Values {
		rewritten.Values[i] = s.rewriteExpressionSubqueries(val, cteTempTables)
	}
	return &rewritten
}

func (s *Stmt) rewriteBinaryExpr(e *parser.BinaryExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Left = s.rewriteExpressionSubqueries(e.Left, cteTempTables)
	rewritten.Right = s.rewriteExpressionSubqueries(e.Right, cteTempTables)
	return &rewritten
}

func (s *Stmt) rewriteUnaryExpr(e *parser.UnaryExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	return &rewritten
}

func (s *Stmt) rewriteCaseExpr(e *parser.CaseExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	if e.Expr != nil {
		rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	}
	for i := range e.WhenClauses {
		rewritten.WhenClauses[i].Condition = s.rewriteExpressionSubqueries(e.WhenClauses[i].Condition, cteTempTables)
		rewritten.WhenClauses[i].Result = s.rewriteExpressionSubqueries(e.WhenClauses[i].Result, cteTempTables)
	}
	if e.ElseClause != nil {
		rewritten.ElseClause = s.rewriteExpressionSubqueries(e.ElseClause, cteTempTables)
	}
	return &rewritten
}

func (s *Stmt) rewriteBetweenExpr(e *parser.BetweenExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	rewritten.Lower = s.rewriteExpressionSubqueries(e.Lower, cteTempTables)
	rewritten.Upper = s.rewriteExpressionSubqueries(e.Upper, cteTempTables)
	return &rewritten
}

func (s *Stmt) rewriteFunctionExpr(e *parser.FunctionExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	for i, arg := range e.Args {
		rewritten.Args[i] = s.rewriteExpressionSubqueries(arg, cteTempTables)
	}
	if e.Filter != nil {
		rewritten.Filter = s.rewriteExpressionSubqueries(e.Filter, cteTempTables)
	}
	return &rewritten
}

func (s *Stmt) rewriteParenExpr(e *parser.ParenExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	return &rewritten
}

func (s *Stmt) rewriteCastExpr(e *parser.CastExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	return &rewritten
}

func (s *Stmt) rewriteCollateExpr(e *parser.CollateExpr, cteTempTables map[string]*schema.Table) parser.Expression {
	rewritten := *e
	rewritten.Expr = s.rewriteExpressionSubqueries(e.Expr, cteTempTables)
	return &rewritten
}
