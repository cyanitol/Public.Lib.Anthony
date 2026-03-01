// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileSelectWithCTEs compiles a SELECT statement with a WITH clause (CTEs).
func (s *Stmt) compileSelectWithCTEs(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Create CTE context from WITH clause
	cteCtx, err := planner.NewCTEContext(stmt.With)
	if err != nil {
		return nil, fmt.Errorf("failed to create CTE context: %w", err)
	}

	// Validate CTEs
	if err := cteCtx.ValidateCTEs(); err != nil {
		return nil, fmt.Errorf("CTE validation failed: %w", err)
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Materialize CTEs in dependency order
	cteTempTables := make(map[string]*schema.Table)
	for _, cteName := range cteCtx.CTEOrder {
		def, exists := cteCtx.GetCTE(cteName)
		if !exists {
			return nil, fmt.Errorf("CTE not found in context: %s", cteName)
		}

		if def.IsRecursive {
			// Handle recursive CTE
			tempTable, err := s.compileRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile recursive CTE %s: %w", cteName, err)
			}
			cteTempTables[cteName] = tempTable
		} else {
			// Handle non-recursive CTE
			tempTable, err := s.compileNonRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile CTE %s: %w", cteName, err)
			}
			cteTempTables[cteName] = tempTable
		}
	}

	// Now compile the main query, replacing CTE references with temp tables
	mainStmt := s.rewriteSelectWithCTETables(stmt, cteTempTables)

	// Compile the main query (without the WITH clause)
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

	// Adjust cursor numbers for cursor operations
	if needsCursorAdjustment(instr.Opcode) {
		adjustedP1 = instr.P1 + offsets.baseCursor
	}

	// Adjust register numbers
	adjustedP1, adjustedP2, adjustedP3 = adjustRegisterNumbers(
		instr.Opcode, adjustedP1, adjustedP2, adjustedP3, offsets.baseRegister,
	)

	newInstr.P1 = adjustedP1
	newInstr.P2 = adjustedP2
	newInstr.P3 = adjustedP3

	return newInstr
}

// handleSpecialOpcode handles ResultRow and Halt opcodes specially. Returns true if handled.
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
	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos,
		vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe,
		vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev:
		if instr.P2 > 0 {
			vm.Program[addr].P2 = instr.P2 + offsets.startAddr
		}
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
	switch op {
	case vdbe.OpInteger, vdbe.OpReal, vdbe.OpString8, vdbe.OpBlob, vdbe.OpNull:
		// P1=value/size, P2=dest register, P3=unused
		return p1, p2 + baseReg, p3

	case vdbe.OpResultRow, vdbe.OpMakeRecord:
		// P1=start register, P2=count, P3=dest register (for MakeRecord)
		return p1 + baseReg, p2, p3 + baseReg

	case vdbe.OpCopy, vdbe.OpSCopy:
		// P1=src register, P2=dest register
		return p1 + baseReg, p2 + baseReg, p3

	case vdbe.OpAdd, vdbe.OpSubtract, vdbe.OpMultiply, vdbe.OpDivide, vdbe.OpRemainder,
		vdbe.OpConcat, vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
		return adjustArithmeticAndComparisonOps(op, p1, p2, p3, baseReg)

	case vdbe.OpNot, vdbe.OpBitNot:
		// P1=src register, P2=dest register
		return p1 + baseReg, p2 + baseReg, p3

	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos:
		return adjustJumpOps(op, p1, p2, p3, baseReg)

	case vdbe.OpInit, vdbe.OpHalt, vdbe.OpNoop:
		// No register adjustments needed
		return p1, p2, p3

	default:
		// Conservative default: don't adjust
		return p1, p2, p3
	}
}

// adjustArithmeticAndComparisonOps handles register adjustment for arithmetic and comparison operations.
func adjustArithmeticAndComparisonOps(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	// P1=left register, P2=right register or jump, P3=dest register
	// Note: comparison ops use P2 for jump target, not register
	switch op {
	case vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
		// P1=register, P2=jump target, P3=register
		return p1 + baseReg, p2, p3 + baseReg
	default:
		// Arithmetic ops: P1, P2, P3 all registers
		return p1 + baseReg, p2 + baseReg, p3 + baseReg
	}
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
	// Infer columns from CTE definition
	var columns []*schema.Column

	if len(def.Columns) > 0 {
		// Use explicit column list
		columns = make([]*schema.Column, len(def.Columns))
		for i, colName := range def.Columns {
			columns[i] = &schema.Column{
				Name:    colName,
				Type:    "ANY",
				NotNull: false,
			}
		}
	} else if def.Select != nil && len(def.Select.Columns) > 0 {
		// Infer from SELECT columns
		columns = make([]*schema.Column, len(def.Select.Columns))
		for i, col := range def.Select.Columns {
			colName := col.Alias
			if colName == "" {
				if ident, ok := col.Expr.(*parser.IdentExpr); ok {
					colName = ident.Name
				} else {
					colName = fmt.Sprintf("column_%d", i+1)
				}
			}
			columns[i] = &schema.Column{
				Name:    colName,
				Type:    "ANY",
				NotNull: false,
			}
		}
	}

	return &schema.Table{
		Name:     tableName,
		Columns:  columns,
		RootPage: 0,    // Will be set to cursor number
		Temp:     true, // Mark as temporary/ephemeral table
	}
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
