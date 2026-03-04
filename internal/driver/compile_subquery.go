// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// FROM Subquery Compilation
// ============================================================================

// hasFromSubqueries checks if a SELECT statement has subqueries in FROM clause.
func (s *Stmt) hasFromSubqueries(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}

	// Check base tables
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			return true
		}
	}

	// Check JOIN clauses
	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			return true
		}
	}

	return false
}

// compileSelectWithFromSubqueries compiles a SELECT with FROM subqueries.
func (s *Stmt) compileSelectWithFromSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Special case: if we have a single FROM subquery and the outer query is simple
	// (just selecting columns, possibly with WHERE/ORDER BY), we can optimize by
	// compiling the subquery directly and handling column references
	if len(stmt.From.Tables) == 1 && stmt.From.Tables[0].Subquery != nil && len(stmt.From.Joins) == 0 {
		return s.compileSingleFromSubquery(vm, stmt, args)
	}

	// Strategy: compile each FROM subquery into a temp table, then compile main query
	// This is a more complex case with multiple subqueries or joins

	// Allocate cursors for all subqueries and main query
	numSubqueries := s.countFromSubqueries(stmt)
	vm.AllocCursors(numSubqueries + 1)
	vm.AllocMemory(50)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Compile each FROM subquery
	cursorIdx := 0
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			// Compile the subquery
			subVM, err := s.compileSelect(vdbe.New(), table.Subquery, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
			}

			// Create a temp table to hold results
			// In a full implementation, would:
			// 1. Execute subVM to get results
			// 2. Store results in temp table
			// 3. Use temp table in main query

			// For now, emit a comment
			commentOp := vm.AddOp(vdbe.OpNoop, 0, 0, 0)
			vm.Program[commentOp].Comment = fmt.Sprintf("FROM subquery compiled for cursor %d", cursorIdx)
			cursorIdx++

			// Merge the subquery program into main VM
			// This is simplified - real implementation would properly handle temp tables
			vm.Program = append(vm.Program, subVM.Program...)
		}
	}

	// Now compile the main query as normal, but referencing the temp tables
	// For this simplified implementation, we'll just compile it normally
	// A full implementation would track temp table schemas and use them

	// Simplified: compile as if no subquery (assumes flattening occurred)
	if len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Subquery == nil {
		return s.compileSelect(vm, stmt, args)
	}

	// If all tables are subqueries, emit a placeholder result
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compileSingleFromSubquery compiles a SELECT with a single FROM subquery.
// This handles cases like: SELECT columns FROM (subquery) [WHERE ...] [ORDER BY ...]
func (s *Stmt) compileSingleFromSubquery(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subquery := stmt.From.Tables[0].Subquery

	// Special optimization: SELECT * with no WHERE clause
	if s.isSimpleSelectStar(stmt) {
		return s.compileSimpleSubquery(subquery, args)
	}

	// Try to flatten the subquery if possible
	flattened, canFlatten := s.tryFlattenFromSubquery(stmt, subquery)
	if canFlatten {
		return s.compileSelect(vm, flattened, args)
	}

	// Complex case: specific columns or WHERE clause
	return s.compileComplexSubquery(stmt, subquery, args)
}

// isSimpleSelectStar checks if statement is SELECT * with no WHERE.
func (s *Stmt) isSimpleSelectStar(stmt *parser.SelectStmt) bool {
	return isSelectStar(stmt) && stmt.Where == nil
}

// tryFlattenFromSubquery attempts to flatten a FROM subquery into the outer query.
// This is possible when the subquery is simple (no GROUP BY, no DISTINCT, etc.)
// Returns the flattened statement and true if flattening succeeded, or nil and false if not.
func (s *Stmt) tryFlattenFromSubquery(outer *parser.SelectStmt, subquery *parser.SelectStmt) (*parser.SelectStmt, bool) {
	// Can't flatten if subquery has features that prevent it
	if !s.canFlattenSubquery(subquery) {
		return nil, false
	}

	// Create flattened statement
	flattened := &parser.SelectStmt{
		Columns:  outer.Columns,   // Keep outer columns (may include aggregates)
		From:     subquery.From,    // Use subquery's FROM clause
		Where:    subquery.Where,   // Use subquery's WHERE (or merge with outer WHERE if needed)
		GroupBy:  outer.GroupBy,    // Keep outer GROUP BY
		Having:   outer.Having,     // Keep outer HAVING
		OrderBy:  outer.OrderBy,    // Keep outer ORDER BY
		Limit:    outer.Limit,      // Keep outer LIMIT
	}

	// If outer has a WHERE clause, we'd need to merge it (complex)
	// For now, only flatten if outer has no WHERE
	if outer.Where != nil {
		return nil, false
	}

	return flattened, true
}

// canFlattenSubquery checks if a subquery can be safely flattened.
func (s *Stmt) canFlattenSubquery(subquery *parser.SelectStmt) bool {
	// Can't flatten if subquery has:
	// - GROUP BY
	// - HAVING
	// - DISTINCT
	// - LIMIT
	// - OFFSET
	// - Aggregate functions
	// - UNION/INTERSECT/EXCEPT

	if len(subquery.GroupBy) > 0 {
		return false
	}
	if subquery.Having != nil {
		return false
	}
	if subquery.Distinct {
		return false
	}
	if subquery.Limit != nil {
		return false
	}
	if subquery.Compound != nil {
		return false
	}

	// Check if subquery has aggregates
	if s.detectAggregates(subquery) {
		return false
	}

	return true
}

// compileSimpleSubquery compiles a simple SELECT * subquery.
func (s *Stmt) compileSimpleSubquery(subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}
	// TODO: Handle ORDER BY from outer query
	return subVM, nil
}

// compileComplexSubquery compiles a subquery with column selection or WHERE clause.
func (s *Stmt) compileComplexSubquery(stmt *parser.SelectStmt, subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Compile subquery to get its structure
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}

	// Map outer columns to subquery columns
	newColumns, err := s.mapSubqueryColumns(stmt, subquery, subVM.ResultCols)
	if err != nil {
		return nil, err
	}

	// Recompile with mapped columns
	modifiedSubquery := copySelectStmtShallow(subquery)
	modifiedSubquery.Columns = newColumns
	return s.compileSelect(s.newVDBE(), modifiedSubquery, args)
}

// mapSubqueryColumns maps outer query columns to subquery columns.
func (s *Stmt) mapSubqueryColumns(stmt *parser.SelectStmt, subquery *parser.SelectStmt, subqueryColumns []string) ([]parser.ResultColumn, error) {
	var newColumns []parser.ResultColumn

	for _, outerCol := range stmt.Columns {
		if outerCol.Star {
			// SELECT * - use all subquery columns
			return subquery.Columns, nil
		}

		if ident, ok := outerCol.Expr.(*parser.IdentExpr); ok {
			col, err := s.findSubqueryColumn(ident.Name, subquery, subqueryColumns)
			if err != nil {
				return nil, err
			}
			newColumns = append(newColumns, col)
		}
	}

	return newColumns, nil
}

// findSubqueryColumn finds a column in the subquery by name.
func (s *Stmt) findSubqueryColumn(name string, subquery *parser.SelectStmt, subqueryColumns []string) (parser.ResultColumn, error) {
	for i, subCol := range subqueryColumns {
		if subCol == name {
			return subquery.Columns[i], nil
		}
	}
	return parser.ResultColumn{}, fmt.Errorf("column not found: %s", name)
}

// copySelectStmtShallow makes a shallow copy of a SELECT statement.
func copySelectStmtShallow(stmt *parser.SelectStmt) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}
	copy := *stmt
	return &copy
}

// isSelectStar checks if SELECT is SELECT *.
func isSelectStar(stmt *parser.SelectStmt) bool {
	if len(stmt.Columns) == 1 {
		col := stmt.Columns[0]
		if col.Star && col.Table == "" {
			return true
		}
	}
	return false
}

// countFromSubqueries counts the number of subqueries in FROM clause.
func (s *Stmt) countFromSubqueries(stmt *parser.SelectStmt) int {
	count := 0
	if stmt.From == nil {
		return 0
	}

	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			count++
		}
	}

	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			count++
		}
	}

	return count
}

// ============================================================================
// Expression Subquery Compilation (scalar, EXISTS, IN)
// ============================================================================

// setupSubqueryCompiler configures the CodeGenerator to handle subqueries.
// It provides a callback that compiles subquery SELECT statements.
// The subquery is compiled into a temporary VDBE, then its bytecode is
// adjusted to use cursor indices starting from the parent's next cursor.
// Register allocations are also adjusted to avoid conflicts with parent registers.
// Control flow opcodes (OpInit, OpHalt) are replaced with appropriate jumps.
//
// This follows SQLite's approach where pParse->nMem is shared across parent
// and subquery compilation to ensure register allocations don't conflict.
func (s *Stmt) setupSubqueryCompiler(gen *expr.CodeGenerator) {
	gen.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		// Get the parent VDBE and current register allocation state
		parentVM := gen.GetVDBE()
		cursorOffset := len(parentVM.Cursors)

		// CRITICAL: Get the parent's current register count to avoid conflicts
		// This is analogous to SQLite's pParse->nMem being shared across
		// parent and subquery compilation contexts
		registerOffset := parentVM.NumMem

		// Create a temporary VDBE for the subquery
		subVM := vdbe.New()

		// Copy context from parent so btree is available
		subVM.Ctx = parentVM.Ctx

		// Pre-allocate memory in subquery to account for parent's registers
		// This ensures the subquery's CodeGenerator starts allocating after parent's registers
		subVM.AllocMemory(registerOffset + 50)

		// Compile the subquery SELECT statement
		compiledVM, err := s.compileSelect(subVM, selectStmt, nil)
		if err != nil {
			return nil, err
		}

		// Strip OpInit (convert to Noop) and track OpHalt locations
		// OpHalt will be patched by the caller to jump to the end of the subquery
		stripSubqueryControlFlow(compiledVM)

		// Adjust cursor references in the compiled bytecode
		// by adding the cursor offset to all cursor operations
		adjustSubqueryCursors(compiledVM, cursorOffset)

		// CRITICAL: Adjust register references in the compiled bytecode
		// by adding the register offset to all register operations
		// This prevents register conflicts between parent and subquery
		adjustSubqueryRegisters(compiledVM, registerOffset)

		// Ensure parent has enough cursors allocated
		// Find max cursor used in subquery (after adjustment)
		maxCursor := findMaxCursor(compiledVM)
		if maxCursor >= 0 {
			parentVM.AllocCursors(maxCursor + 1)
		}

		// Ensure parent has enough registers allocated
		// Find max register used in subquery (after adjustment)
		maxRegister := findMaxRegister(compiledVM)
		if maxRegister >= 0 {
			parentVM.AllocMemory(maxRegister + 1)
		}

		return compiledVM, nil
	})
}

// stripSubqueryControlFlow removes OpInit and converts OpHalt to OpNoop.
// OpHalt is converted to Noop so execution continues past the subquery.
// The caller (generateSubquery) will handle the control flow properly.
//
// For OpInit, we find its jump target (P2) which points past the initialization
// code to the actual program start. We adjust this to be relative to the start
// of the subquery by subtracting the OpInit's position.
func stripSubqueryControlFlow(vm *vdbe.VDBE) {
	// First pass: find OpInit's jump target
	startAddr := 0
	for i := range vm.Program {
		if vm.Program[i].Opcode == vdbe.OpInit {
			startAddr = vm.Program[i].P2
			break
		}
	}

	// If we found an OpInit, we'll mark instructions before the start address as Noop
	// and also convert OpInit and OpHalt to Noop
	for i := range vm.Program {
		switch vm.Program[i].Opcode {
		case vdbe.OpInit:
			vm.Program[i].Opcode = vdbe.OpNoop
			vm.Program[i].Comment = "subquery: stripped OpInit"
		case vdbe.OpHalt:
			vm.Program[i].Opcode = vdbe.OpNoop
			vm.Program[i].Comment = "subquery: stripped OpHalt"
		default:
			// If this instruction is before the start address (initialization code),
			// convert it to Noop as it shouldn't be executed when embedded
			if i < startAddr && startAddr > 0 {
				vm.Program[i].Opcode = vdbe.OpNoop
				vm.Program[i].Comment = "subquery: stripped init code"
			}
		}
	}
}

// adjustSubqueryJumpTargets adjusts all jump targets in the subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE at address baseAddr,
// all absolute jump targets (in P2) must be adjusted by adding baseAddr.
// This ensures jumps land at the correct locations in the combined program.
//
// Jump targets are used in opcodes like:
// - OpGoto, OpGosub: P2 = absolute jump target
// - OpIf, OpIfNot, OpIfPos, OpIfNeg: P2 = jump if condition met
// - OpOnce: P2 = jump if already executed
// - OpRewind, OpNext, OpPrev, OpLast, OpFirst: P2 = jump on end/no rows
// - OpSeekGE, OpSeekGT, OpSeekLE, OpSeekLT: P2 = jump if not found
// - OpSeekRowid, OpNotExists: P2 = jump if not found
// - OpInitCoroutine: P2 = jump past coroutine body, P3 = entry point
// - OpSorterSort, OpSorterNext: P2 = jump when done
func adjustSubqueryJumpTargets(vm *vdbe.VDBE, baseAddr int) {
	if baseAddr == 0 {
		return // No adjustment needed
	}

	// Opcodes that use P2 as a jump target
	jumpOpcodes := map[vdbe.Opcode]bool{
		// Conditional jumps
		vdbe.OpIf:        true,
		vdbe.OpIfNot:     true,
		vdbe.OpIfPos:     true,
		vdbe.OpIfNotZero: true,
		vdbe.OpIfNullRow: true,
		vdbe.OpIsNull:    true, // Jump if P1 is NULL
		vdbe.OpNotNull:   true, // Jump if P1 is not NULL

		// Unconditional jumps
		vdbe.OpGoto:  true,
		vdbe.OpGosub: true,

		// Loop control
		vdbe.OpRewind: true,
		vdbe.OpNext:   true,
		vdbe.OpPrev:   true,
		vdbe.OpLast:   true,
		vdbe.OpFirst:  true,

		// Seek operations
		vdbe.OpSeekGE:    true,
		vdbe.OpSeekGT:    true,
		vdbe.OpSeekLE:    true,
		vdbe.OpSeekLT:    true,
		vdbe.OpSeekRowid: true,
		vdbe.OpNotExists: true,

		// Sorter operations
		vdbe.OpSorterSort: true,
		vdbe.OpSorterNext: true,

		// Special control flow
		vdbe.OpOnce: true,
	}

	// Opcodes that use both P2 and P3 as jump targets
	dualJumpOpcodes := map[vdbe.Opcode]bool{
		vdbe.OpInitCoroutine: true, // P2 = skip address, P3 = entry point
	}

	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Adjust P2 for jump opcodes
		if jumpOpcodes[op] && vm.Program[i].P2 > 0 {
			vm.Program[i].P2 += baseAddr
		}

		// Adjust both P2 and P3 for dual-jump opcodes
		if dualJumpOpcodes[op] {
			if vm.Program[i].P2 > 0 {
				vm.Program[i].P2 += baseAddr
			}
			if vm.Program[i].P3 > 0 {
				vm.Program[i].P3 += baseAddr
			}
		}
	}
}

// adjustSubqueryCursors adds an offset to all cursor references in the bytecode.
// This allows subquery bytecode to use cursors that don't conflict with the parent.
func adjustSubqueryCursors(vm *vdbe.VDBE, offset int) {
	if offset == 0 {
		return
	}

	// Opcodes that use cursor in P1
	cursorP1Opcodes := map[vdbe.Opcode]bool{
		vdbe.OpOpenRead:      true,
		vdbe.OpOpenWrite:     true,
		vdbe.OpOpenEphemeral: true,
		vdbe.OpClose:         true,
		vdbe.OpRewind:        true,
		vdbe.OpNext:          true,
		vdbe.OpPrev:          true,
		vdbe.OpColumn:        true,
		vdbe.OpRowid:         true,
		vdbe.OpSeekGE:        true,
		vdbe.OpSeekGT:        true,
		vdbe.OpSeekLE:        true,
		vdbe.OpSeekLT:        true,
		vdbe.OpSeekRowid:     true,
		vdbe.OpNotExists:     true,
		vdbe.OpInsert:        true,
		vdbe.OpDelete:        true,
		vdbe.OpSorterOpen:    true,
		vdbe.OpSorterInsert:  true,
		vdbe.OpSorterSort:    true,
		vdbe.OpSorterNext:    true,
		vdbe.OpSorterData:    true,
		vdbe.OpSorterClose:   true,
	}

	for i := range vm.Program {
		if cursorP1Opcodes[vm.Program[i].Opcode] {
			vm.Program[i].P1 += offset
		}
	}
}

// findMaxCursor finds the maximum cursor index used in the bytecode.
func findMaxCursor(vm *vdbe.VDBE) int {
	maxCursor := -1

	cursorP1Opcodes := map[vdbe.Opcode]bool{
		vdbe.OpOpenRead:      true,
		vdbe.OpOpenWrite:     true,
		vdbe.OpOpenEphemeral: true,
		vdbe.OpClose:         true,
		vdbe.OpRewind:        true,
		vdbe.OpNext:          true,
		vdbe.OpPrev:          true,
		vdbe.OpColumn:        true,
		vdbe.OpRowid:         true,
		vdbe.OpSorterOpen:    true,
	}

	for i := range vm.Program {
		if cursorP1Opcodes[vm.Program[i].Opcode] {
			if vm.Program[i].P1 > maxCursor {
				maxCursor = vm.Program[i].P1
			}
		}
	}

	return maxCursor
}

// adjustSubqueryRegisters adds an offset to all register references in the bytecode.
// This prevents register conflicts between parent and subquery execution contexts.
func adjustSubqueryRegisters(vm *vdbe.VDBE, offset int) {
	if offset == 0 {
		return
	}

	// Opcodes that use registers - we need to adjust P1, P2, and/or P3
	// depending on the opcode's register usage pattern
	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Most opcodes use P1 and P2 as registers
		// We need to be selective about which ones to adjust
		switch op {
		// Register operations (P1 = target register, P2 = source register)
		case vdbe.OpMove, vdbe.OpCopy, vdbe.OpSCopy:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset

		// Arithmetic operations (P1 = left reg, P2 = right reg, P3 = result reg)
		case vdbe.OpAdd, vdbe.OpSubtract, vdbe.OpMultiply, vdbe.OpDivide, vdbe.OpRemainder,
			vdbe.OpConcat, vdbe.OpBitAnd, vdbe.OpBitOr, vdbe.OpShiftLeft, vdbe.OpShiftRight:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset
			vm.Program[i].P3 += offset

		// Comparison operations (P1 = left reg, P2 = right reg, P3 = result reg)
		case vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset
			vm.Program[i].P3 += offset

		// Column operations (P1 = cursor, P2 = column, P3 = target reg)
		case vdbe.OpColumn:
			vm.Program[i].P3 += offset

		// Integer/String/Null (P1 = ignored, P2 = target reg)
		case vdbe.OpInteger, vdbe.OpString8, vdbe.OpNull, vdbe.OpRowid:
			vm.Program[i].P2 += offset

		// ResultRow (P1 = start reg, P2 = num regs)
		case vdbe.OpResultRow:
			vm.Program[i].P1 += offset

		// Function calls (P2 = first arg reg, P3 = result reg, P5 = arg count)
		// Aggregate functions (P1 = not used, P2 = first arg, P3 = accum reg)
		case vdbe.OpFunction, vdbe.OpAggStep:
			vm.Program[i].P2 += offset
			vm.Program[i].P3 += offset

		// Aggregate final (P1 = dest reg)
		case vdbe.OpAggFinal:
			vm.Program[i].P1 += offset

		// If/IfNot (P1 = condition reg, P2 = jump target)
		case vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos, vdbe.OpIfNotZero:
			vm.Program[i].P1 += offset

		// Other register-using opcodes (P1 = source reg, P2 = dest reg or jump)
		case vdbe.OpNot, vdbe.OpBitNot:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset

		// Opcodes where P1 is register, P2 is jump target
		case vdbe.OpIsNull, vdbe.OpNotNull:
			vm.Program[i].P1 += offset
			// P2 is jump target, don't adjust

		// MakeRecord (P1 = first reg, P2 = count, P3 = dest reg)
		case vdbe.OpMakeRecord:
			vm.Program[i].P1 += offset
			vm.Program[i].P3 += offset

		// Gosub/Return/Coroutine opcodes (P1 = return addr reg)
		case vdbe.OpGosub, vdbe.OpReturn, vdbe.OpInitCoroutine, vdbe.OpEndCoroutine, vdbe.OpYield:
			vm.Program[i].P1 += offset

		// Cast (P1 = source reg, P2 = dest reg)
		case vdbe.OpCast:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset

		// Real, Int64, Blob (P2 = dest reg)
		case vdbe.OpReal, vdbe.OpInt64, vdbe.OpBlob:
			vm.Program[i].P2 += offset

		// Insert/SorterInsert (P2 = key reg, P3 = data reg)
		case vdbe.OpInsert, vdbe.OpSorterInsert:
			vm.Program[i].P2 += offset
			vm.Program[i].P3 += offset

		// SorterData (P2 = dest reg)
		case vdbe.OpSorterData:
			vm.Program[i].P2 += offset

		// And/Or (P1 = left reg, P2 = right reg, P3 = result reg)
		case vdbe.OpAnd, vdbe.OpOr:
			vm.Program[i].P1 += offset
			vm.Program[i].P2 += offset
			vm.Program[i].P3 += offset

		// AddImm (P1 = register to modify, P2 = immediate value)
		case vdbe.OpAddImm:
			vm.Program[i].P1 += offset
			// P2 is immediate value, don't adjust

		// AggDistinct (P1 = input reg, P2 = jump target, P3 = aggregate reg)
		case vdbe.OpAggDistinct:
			vm.Program[i].P1 += offset
			// P2 is jump target, don't adjust
			vm.Program[i].P3 += offset
		}
	}
}

// findMaxRegister finds the maximum register index used in the bytecode.
func findMaxRegister(vm *vdbe.VDBE) int {
	maxReg := -1

	for i := range vm.Program {
		op := vm.Program[i].Opcode

		// Check all parameter positions that might contain register indices
		switch op {
		case vdbe.OpMove, vdbe.OpCopy, vdbe.OpSCopy:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}

		case vdbe.OpAdd, vdbe.OpSubtract, vdbe.OpMultiply, vdbe.OpDivide, vdbe.OpRemainder,
			vdbe.OpConcat, vdbe.OpBitAnd, vdbe.OpBitOr, vdbe.OpShiftLeft, vdbe.OpShiftRight,
			vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}

		case vdbe.OpColumn:
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}

		case vdbe.OpInteger, vdbe.OpString8, vdbe.OpNull, vdbe.OpRowid,
			vdbe.OpReal, vdbe.OpInt64, vdbe.OpBlob, vdbe.OpSorterData:
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}

		case vdbe.OpResultRow:
			// P1 = start reg, P2 = count
			endReg := vm.Program[i].P1 + vm.Program[i].P2 - 1
			if endReg > maxReg {
				maxReg = endReg
			}

		case vdbe.OpMakeRecord, vdbe.OpInsert, vdbe.OpSorterInsert:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}

		case vdbe.OpNot, vdbe.OpBitNot, vdbe.OpCast:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}

		case vdbe.OpFunction, vdbe.OpAggStep:
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}
			// Account for function argument range
			if instr := vm.Program[i]; instr.P2 > 0 && instr.P5 > 0 {
				argEnd := instr.P2 + int(instr.P5) - 1
				if argEnd > maxReg {
					maxReg = argEnd
				}
			}

		case vdbe.OpAggDistinct:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}

		case vdbe.OpAnd, vdbe.OpOr:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
			if vm.Program[i].P2 > maxReg {
				maxReg = vm.Program[i].P2
			}
			if vm.Program[i].P3 > maxReg {
				maxReg = vm.Program[i].P3
			}

		case vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos, vdbe.OpIfNotZero,
			vdbe.OpIsNull, vdbe.OpNotNull, vdbe.OpGosub, vdbe.OpReturn,
			vdbe.OpInitCoroutine, vdbe.OpEndCoroutine, vdbe.OpYield, vdbe.OpAggFinal,
			vdbe.OpAddImm:
			if vm.Program[i].P1 > maxReg {
				maxReg = vm.Program[i].P1
			}
		}
	}

	return maxReg
}

// compileScalarSubquery compiles a scalar subquery (returns single value).
func (s *Stmt) compileScalarSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	// Emit code to execute subquery and store result in targetReg
	// In a full implementation, would:
	// 1. Open a pseudo-cursor for the subquery
	// 2. Execute the subquery
	// 3. Fetch the first (and only) row
	// 4. Store the value in targetReg
	// 5. Verify no more rows (scalar must return 1 row)

	// For now, merge the subquery program and add a comment
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("Scalar subquery -> reg %d", targetReg)

	return nil
}

// compileExistsSubquery compiles an EXISTS subquery.
func (s *Stmt) compileExistsSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	// Emit code to execute subquery and check if any rows exist
	// EXISTS returns 1 if subquery returns any rows, 0 otherwise

	// Strategy:
	// 1. Execute subquery
	// 2. Try to fetch first row
	// 3. If row exists, set targetReg = 1
	// 4. If no rows, set targetReg = 0

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("EXISTS subquery -> reg %d", targetReg)

	// Set result register (simplified - assumes subquery ran)
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}

// compileInSubquery compiles an IN subquery.
func (s *Stmt) compileInSubquery(vm *vdbe.VDBE, leftExpr parser.Expression, subquery *parser.SelectStmt, targetReg int, gen *expr.CodeGenerator, args []driver.NamedValue) error {
	// Compile the left expression
	leftReg, err := gen.GenerateExpr(leftExpr)
	if err != nil {
		return fmt.Errorf("failed to compile IN left expression: %w", err)
	}

	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile IN subquery: %w", err)
	}

	// Strategy for IN subquery:
	// 1. Materialize subquery results into a temp table or ephemeral table
	// 2. Use OpFound to check if leftReg value exists in the temp table
	// 3. Set targetReg to 1 if found, 0 otherwise

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("IN subquery for reg %d -> reg %d", leftReg, targetReg)

	// Simplified: assume value is found
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}
