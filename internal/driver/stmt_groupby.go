package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// groupByState holds registers and state for GROUP BY processing
type groupByState struct {
	groupByRegs     []int
	prevGroupByRegs []int
	accRegs         []int
	avgCountRegs    []int
	firstRowReg     int
}

// initGroupByState allocates and initializes registers for GROUP BY processing
func (s *Stmt) initGroupByState(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt, numGroupBy int) groupByState {
	state := groupByState{
		groupByRegs:     make([]int, numGroupBy),
		prevGroupByRegs: make([]int, numGroupBy),
		accRegs:         make([]int, len(stmt.Columns)),
		avgCountRegs:    make([]int, len(stmt.Columns)),
	}

	// Allocate GROUP BY registers
	for i := 0; i < numGroupBy; i++ {
		state.groupByRegs[i] = gen.AllocReg()
		state.prevGroupByRegs[i] = gen.AllocReg()
	}

	// Allocate accumulator registers
	for i, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			state.accRegs[i] = gen.AllocReg()
			fnExpr := col.Expr.(*parser.FunctionExpr)
			if fnExpr.Name == "AVG" {
				state.avgCountRegs[i] = gen.AllocReg()
			}
		}
	}

	// Allocate first row flag register
	state.firstRowReg = gen.AllocReg()

	// Initialize first row flag to 1 (true)
	vm.AddOp(vdbe.OpInteger, 1, state.firstRowReg, 0)

	// Initialize prev GROUP BY registers to NULL
	for i := 0; i < numGroupBy; i++ {
		vm.AddOp(vdbe.OpNull, 0, state.prevGroupByRegs[i], 0)
	}

	return state
}

// emitGroupComparison emits code to compare GROUP BY values and output previous group if changed
// This function compares all GROUP BY columns. If ANY differ from the previous group, it outputs
// the previous group's accumulated results.
func (s *Stmt) emitGroupComparison(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt, state groupByState, numGroupBy, numCols int) int {
	// Check if group has changed (skip on first row)
	skipCheckAddr := vm.AddOp(vdbe.OpIf, state.firstRowReg, 0, 0)

	// Compare all GROUP BY columns to detect if group changed
	// We need to check if ANY column differs
	groupChangedReg := gen.AllocReg()
	vm.AddOp(vdbe.OpInteger, 0, groupChangedReg, 0) // Initialize to false

	for i := 0; i < numGroupBy; i++ {
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpNe, state.groupByRegs[i], state.prevGroupByRegs[i], cmpReg)
		// If this column differs, set groupChangedReg to true
		vm.AddOp(vdbe.OpOr, groupChangedReg, cmpReg, groupChangedReg)
	}

	// If group changed, output the previous group
	skipOutputAddr := vm.AddOp(vdbe.OpIfNot, groupChangedReg, 0, 0)
	s.emitGroupOutput(vm, stmt, state.accRegs, state.avgCountRegs, state.prevGroupByRegs, numCols)

	// After outputting, we'll fall through to reset accumulators
	// Skip output jumps here
	vm.Program[skipOutputAddr].P2 = vm.NumOps()

	return skipCheckAddr
}

// updateGroupAccumulatorsFromSorter updates accumulators from data extracted from sorter
func (s *Stmt) updateGroupAccumulatorsFromSorter(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt, state groupByState, sorterBaseReg int, numGroupBy int) {
	// Clear first row flag
	vm.AddOp(vdbe.OpInteger, 0, state.firstRowReg, 0)

	// Initialize/reset accumulators
	for i, col := range stmt.Columns {
		if !s.isAggregateExpr(col.Expr) {
			continue
		}
		fnExpr := col.Expr.(*parser.FunctionExpr)
		state.avgCountRegs[i] = s.initializeAggregateRegister(vm, fnExpr.Name, state.accRegs[i], gen)
	}

	// Save current GROUP BY values to prev
	for i := 0; i < len(state.groupByRegs); i++ {
		vm.AddOp(vdbe.OpCopy, state.groupByRegs[i], state.prevGroupByRegs[i], 0)
	}

	// Update accumulators from sorter data
	regIdx := numGroupBy
	for i, col := range stmt.Columns {
		if !s.isAggregateExpr(col.Expr) {
			continue
		}
		fnExpr := col.Expr.(*parser.FunctionExpr)

		// COUNT(*) doesn't need column data
		if fnExpr.Star || len(fnExpr.Args) == 0 {
			if fnExpr.Name == "COUNT" {
				vm.AddOp(vdbe.OpAddImm, state.accRegs[i], 1, 0)
			}
			continue
		}

		// Get column value from sorter data
		valueReg := sorterBaseReg + regIdx
		regIdx++

		// Update accumulator based on function type
		s.updateSingleAccumulator(vm, fnExpr.Name, state.accRegs[i], state.avgCountRegs[i], valueReg, gen)
	}
}

// updateSingleAccumulator updates a single accumulator register with a value
func (s *Stmt) updateSingleAccumulator(vm *vdbe.VDBE, funcName string, accReg int, countReg int, valueReg int, gen *expr.CodeGenerator) {
	// Skip NULL values
	skipAddr := vm.AddOp(vdbe.OpIsNull, valueReg, 0, 0)

	switch funcName {
	case "COUNT":
		vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)

	case "SUM", "TOTAL":
		// If accumulator is NOT NULL, jump to add instruction
		addAddr := vm.AddOp(vdbe.OpNotNull, accReg, 0, 0)
		// Accumulator is NULL - copy the first value
		vm.AddOp(vdbe.OpCopy, valueReg, accReg, 0)
		skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
		// Accumulator is not NULL - add to it
		vm.Program[addAddr].P2 = vm.NumOps()
		vm.AddOp(vdbe.OpAdd, accReg, valueReg, accReg)
		endAddr := vm.NumOps()
		vm.Program[skipToEndAddr].P2 = endAddr

	case "AVG":
		// Increment count
		vm.AddOp(vdbe.OpAddImm, countReg, 1, 0)
		// If sum accumulator is NOT NULL, jump to add instruction
		addAddr := vm.AddOp(vdbe.OpNotNull, accReg, 0, 0)
		// Sum is NULL - copy the first value
		vm.AddOp(vdbe.OpCopy, valueReg, accReg, 0)
		skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
		// Sum is not NULL - add to it
		vm.Program[addAddr].P2 = vm.NumOps()
		vm.AddOp(vdbe.OpAdd, accReg, valueReg, accReg)
		endAddr := vm.NumOps()
		vm.Program[skipToEndAddr].P2 = endAddr

	case "MIN":
		// If accumulator is NULL, just copy the value (first value)
		copyAddr := vm.AddOp(vdbe.OpIsNull, accReg, 0, 0)
		// Accumulator is not NULL - compare
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpLt, valueReg, accReg, cmpReg)
		notLessAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)
		// Copy value (either first value or new min)
		vm.Program[copyAddr].P2 = vm.NumOps()
		vm.AddOp(vdbe.OpCopy, valueReg, accReg, 0)
		endAddr := vm.NumOps()
		vm.Program[notLessAddr].P2 = endAddr

	case "MAX":
		// If accumulator is NULL, just copy the value (first value)
		copyAddr := vm.AddOp(vdbe.OpIsNull, accReg, 0, 0)
		// Accumulator is not NULL - compare
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpGt, valueReg, accReg, cmpReg)
		notGreaterAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)
		// Copy value (either first value or new max)
		vm.Program[copyAddr].P2 = vm.NumOps()
		vm.AddOp(vdbe.OpCopy, valueReg, accReg, 0)
		endAddr := vm.NumOps()
		vm.Program[notGreaterAddr].P2 = endAddr
	}

	// Fix skip address for NULL values
	vm.Program[skipAddr].P2 = vm.NumOps()
}

// calculateSorterColumns determines how many columns the sorter needs to store
func (s *Stmt) calculateSorterColumns(stmt *parser.SelectStmt, numGroupBy int) int {
	cols := numGroupBy

	// Add columns needed for aggregate functions
	for _, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			fnExpr := col.Expr.(*parser.FunctionExpr)
			// COUNT(*) doesn't need a column
			if !fnExpr.Star && len(fnExpr.Args) > 0 {
				cols++
			}
		}
	}

	return cols
}

// createGroupBySorterKeyInfo creates sorter key information for GROUP BY
func (s *Stmt) createGroupBySorterKeyInfo(numGroupBy int) *vdbe.SorterKeyInfo {
	keyCols := make([]int, numGroupBy)
	desc := make([]bool, numGroupBy)
	collations := make([]string, numGroupBy)

	for i := 0; i < numGroupBy; i++ {
		keyCols[i] = i
		desc[i] = false
		collations[i] = ""
	}

	return &vdbe.SorterKeyInfo{
		KeyCols:    keyCols,
		Desc:       desc,
		Collations: collations,
	}
}

// compileSelectWithGroupBy compiles a SELECT with GROUP BY clause using sorted aggregate approach.
// This implementation:
// 1. Scans the table and populates a sorter with GROUP BY columns + aggregate inputs
// 2. Sorts by GROUP BY columns
// 3. Processes sorted data row-by-row, detecting group changes and computing aggregates
func (s *Stmt) compileSelectWithGroupBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	numCols := len(stmt.Columns)
	numGroupBy := len(stmt.GroupBy)

	// Setup VDBE and code generator
	vm.AllocMemory(numCols + numGroupBy*3 + 100)
	vm.AllocCursors(2) // Cursor 0 for table, cursor 1 for sorter

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, 0)

	// Build result column names
	vm.ResultCols = make([]string, numCols)
	for i, col := range stmt.Columns {
		vm.ResultCols[i] = selectColName(col, i)
	}

	// Register table info
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	// Setup args
	s.setupAggregateArgs(gen, args)

	// Initialize GROUP BY state
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	state := s.initGroupByState(vm, gen, stmt, numGroupBy)

	// Phase 1: Scan table and populate sorter
	// We need to store: GROUP BY columns + columns needed for aggregates
	sorterCols := s.calculateSorterColumns(stmt, numGroupBy)

	// Create sorter key info (sort by all GROUP BY columns)
	keyInfo := s.createGroupBySorterKeyInfo(numGroupBy)

	// Open table and sorter
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))
	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 1, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
	loopStart := vm.NumOps()

	// WHERE clause
	skipAddr := s.emitWhereClause(vm, gen, stmt)

	// Evaluate and store data for sorter
	// Allocate base register for sorter data
	sorterBaseReg := gen.AllocReg()

	// Evaluate GROUP BY expressions
	for i, groupExpr := range stmt.GroupBy {
		reg, err := gen.GenerateExpr(groupExpr)
		if err != nil {
			return nil, fmt.Errorf("error compiling GROUP BY expression: %w", err)
		}
		vm.AddOp(vdbe.OpCopy, reg, sorterBaseReg+i, 0)
	}

	// Store columns needed for aggregates
	regIdx := numGroupBy
	for _, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			fnExpr := col.Expr.(*parser.FunctionExpr)
			// For COUNT(*), we don't need to store anything extra
			if !fnExpr.Star && len(fnExpr.Args) > 0 {
				argReg, err := gen.GenerateExpr(fnExpr.Args[0])
				if err == nil {
					vm.AddOp(vdbe.OpCopy, argReg, sorterBaseReg+regIdx, 0)
					regIdx++
				}
			}
		}
	}

	// Insert into sorter
	vm.AddOp(vdbe.OpSorterInsert, 1, sorterBaseReg, sorterCols)

	// Fix WHERE skip
	s.fixWhereSkip(vm, skipAddr)

	// Next row
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)

	// Close table, sort the data
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	sorterSortAddr := vm.AddOp(vdbe.OpSorterSort, 1, 0, 0)

	// Phase 2: Process sorted data with group detection
	afterScanAddr := vm.NumOps()

	// Iterate over sorted data
	sorterNextAddr := vm.AddOp(vdbe.OpSorterNext, 1, 0, 0)
	sorterLoopStart := vm.NumOps()

	// Extract data from sorter
	vm.AddOp(vdbe.OpSorterData, 1, sorterBaseReg, sorterCols)

	// Load GROUP BY values from sorter data
	for i := 0; i < numGroupBy; i++ {
		vm.AddOp(vdbe.OpCopy, sorterBaseReg+i, state.groupByRegs[i], 0)
	}

	// Compare GROUP BY values and output previous group if changed
	skipCheckAddr := s.emitGroupComparison(vm, gen, stmt, state, numGroupBy, numCols)

	// Patch first row skip
	initAccumulatorsAddr := vm.NumOps()
	vm.Program[skipCheckAddr].P2 = initAccumulatorsAddr

	// Update accumulators with current row data from sorter
	s.updateGroupAccumulatorsFromSorter(vm, gen, stmt, state, sorterBaseReg, numGroupBy)

	// Next sorted row
	vm.AddOp(vdbe.OpSorterNext, 1, sorterLoopStart, 0)

	// After processing all sorted rows - output last group
	finalOutputAddr := vm.NumOps()
	s.emitFinalGroupOutput(vm, gen, stmt, state, numCols)

	// Close sorter and halt
	vm.AddOp(vdbe.OpSorterClose, 1, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix jumps
	vm.Program[rewindAddr].P2 = afterScanAddr
	vm.Program[sorterSortAddr].P2 = finalOutputAddr
	vm.Program[sorterNextAddr].P2 = sorterLoopStart

	return vm, nil
}

// emitWhereClause emits WHERE clause code and returns skip address
func (s *Stmt) emitWhereClause(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt) int {
	if stmt.Where == nil {
		return -1
	}
	whereReg, err := gen.GenerateExpr(stmt.Where)
	if err != nil {
		return -1
	}
	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
}

// evaluateGroupByExprs evaluates GROUP BY expressions into registers
func (s *Stmt) evaluateGroupByExprs(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt, groupByRegs []int) error {
	for i, groupExpr := range stmt.GroupBy {
		reg, err := gen.GenerateExpr(groupExpr)
		if err != nil {
			return fmt.Errorf("error compiling GROUP BY expression: %w", err)
		}
		vm.AddOp(vdbe.OpCopy, reg, groupByRegs[i], 0)
	}
	return nil
}

// fixWhereSkip patches the WHERE skip address
func (s *Stmt) fixWhereSkip(vm *vdbe.VDBE, skipAddr int) {
	if skipAddr >= 0 {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}
}

// emitFinalGroupOutput emits code to output the last group
func (s *Stmt) emitFinalGroupOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator, stmt *parser.SelectStmt, state groupByState, numCols int) {
	// Check if we processed any rows (firstRowReg == 0 means we did)
	checkHasRowsReg := gen.AllocReg()
	vm.AddOp(vdbe.OpNot, state.firstRowReg, checkHasRowsReg, 0)
	skipFinalOutputAddr := vm.AddOp(vdbe.OpIfNot, checkHasRowsReg, 0, 0)

	// Output final group
	s.emitGroupOutput(vm, stmt, state.accRegs, state.avgCountRegs, state.prevGroupByRegs, numCols)

	// Patch skip address
	vm.Program[skipFinalOutputAddr].P2 = vm.NumOps()
}

// emitGroupOutput outputs a single group's results.
func (s *Stmt) emitGroupOutput(vm *vdbe.VDBE, stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int, groupByRegs []int, numCols int) {
	// First, copy all aggregate and group values to result registers (0..numCols-1)
	for i, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			fnExpr := col.Expr.(*parser.FunctionExpr)
			if fnExpr.Name == "AVG" {
				vm.AddOp(vdbe.OpDivide, accRegs[i], avgCountRegs[i], i)
			} else {
				vm.AddOp(vdbe.OpCopy, accRegs[i], i, 0)
			}
		} else {
			// Non-aggregate column - use GROUP BY value if it matches
			found := false
			for j, groupExpr := range stmt.GroupBy {
				if exprsEqual(col.Expr, groupExpr) {
					vm.AddOp(vdbe.OpCopy, groupByRegs[j], i, 0)
					found = true
					break
				}
			}
			if !found {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
	}

	// Evaluate HAVING clause if present
	havingSkipAddr := s.emitGroupByHavingClause(vm, stmt, accRegs, avgCountRegs, groupByRegs, numCols)

	// Emit result row
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix HAVING skip address to jump past the result row
	if havingSkipAddr > 0 {
		vm.Program[havingSkipAddr].P2 = vm.NumOps()
	}
}

// exprsEqual checks if two expressions are equal (simplified comparison).
func exprsEqual(e1, e2 parser.Expression) bool {
	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	ident1, ok1 := e1.(*parser.IdentExpr)
	ident2, ok2 := e2.(*parser.IdentExpr)
	if ok1 && ok2 {
		return ident1.Name == ident2.Name
	}

	return false
}

// emitAggregateHavingClause emits HAVING clause check for aggregate output (without GROUP BY).
// Returns the address of the IfNot instruction to skip the row if HAVING fails, or 0 if no HAVING clause.
func (s *Stmt) emitAggregateHavingClause(vm *vdbe.VDBE, stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int, numCols int) int {
	if stmt.Having == nil {
		return 0
	}

	// Create a code generator
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)

	// Build a map from aggregate expressions to their result registers
	aggregateMap := s.buildAggregateMap(stmt, accRegs, avgCountRegs)

	// Generate code for the HAVING expression
	havingReg, err := s.generateHavingExpression(vm, gen, stmt.Having, aggregateMap, numCols)
	if err != nil {
		// If we can't generate the HAVING expression, skip it (conservative)
		return 0
	}

	// Emit IfNot to skip the result row if HAVING condition is false
	return vm.AddOp(vdbe.OpIfNot, havingReg, 0, 0)
}

// emitGroupByHavingClause emits HAVING clause check for GROUP BY output.
// Returns the address of the IfNot instruction to skip the row if HAVING fails, or 0 if no HAVING clause.
func (s *Stmt) emitGroupByHavingClause(vm *vdbe.VDBE, stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int, groupByRegs []int, numCols int) int {
	if stmt.Having == nil {
		return 0
	}

	// Create a code generator
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)

	// Build a map from aggregate expressions and GROUP BY columns to their result registers
	aggregateMap := s.buildAggregateMap(stmt, accRegs, avgCountRegs)

	// Add GROUP BY columns to the map
	for i, groupExpr := range stmt.GroupBy {
		if ident, ok := groupExpr.(*parser.IdentExpr); ok {
			aggregateMap[ident.Name] = groupByRegs[i]
		}
	}

	// Generate code for the HAVING expression
	havingReg, err := s.generateHavingExpression(vm, gen, stmt.Having, aggregateMap, numCols)
	if err != nil {
		// If we can't generate the HAVING expression, skip it (conservative)
		return 0
	}

	// Emit IfNot to skip the result row if HAVING condition is false
	return vm.AddOp(vdbe.OpIfNot, havingReg, 0, 0)
}

// buildAggregateMap creates a mapping from aggregate function calls to their result registers.
func (s *Stmt) buildAggregateMap(stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int) map[string]int {
	aggregateMap := make(map[string]int)

	for i, col := range stmt.Columns {
		if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && s.isAggregateExpr(col.Expr) {
			// Create a key for this aggregate (e.g., "COUNT(*)", "SUM(value)")
			key := s.aggregateKey(fnExpr)
			aggregateMap[key] = i

			// For aliases, also map the alias name
			if col.Alias != "" {
				aggregateMap[col.Alias] = i
			}
		}
	}

	return aggregateMap
}

// aggregateKey generates a unique key for an aggregate function expression.
func (s *Stmt) aggregateKey(fnExpr *parser.FunctionExpr) string {
	if fnExpr.Star {
		return fnExpr.Name + "(*)"
	}
	if len(fnExpr.Args) > 0 {
		if ident, ok := fnExpr.Args[0].(*parser.IdentExpr); ok {
			return fnExpr.Name + "(" + ident.Name + ")"
		}
	}
	return fnExpr.Name + "()"
}

// generateHavingExpression generates code for the HAVING expression, resolving aggregate references.
func (s *Stmt) generateHavingExpression(vm *vdbe.VDBE, gen *expr.CodeGenerator, havingExpr parser.Expression,
	aggregateMap map[string]int, baseReg int) (int, error) {

	// Handle different expression types
	switch expr := havingExpr.(type) {
	case *parser.BinaryExpr:
		return s.generateHavingBinaryExpr(vm, gen, expr, aggregateMap, baseReg)
	case *parser.FunctionExpr:
		// Check if this is an aggregate function
		if s.isAggregateExpr(expr) {
			key := s.aggregateKey(expr)
			if reg, ok := aggregateMap[key]; ok {
				// Return the register where this aggregate is already computed
				return reg, nil
			}
		}
		// Not an aggregate or not found, generate normally
		return gen.GenerateExpr(expr)
	case *parser.IdentExpr:
		// Check if this is an alias for an aggregate
		if reg, ok := aggregateMap[expr.Name]; ok {
			return reg, nil
		}
		// Generate normally (column reference)
		return gen.GenerateExpr(expr)
	case *parser.LiteralExpr:
		// Simple literal, generate normally
		return gen.GenerateExpr(expr)
	default:
		// For other expressions, try to generate them
		return gen.GenerateExpr(havingExpr)
	}
}

// binaryOpToVdbeOpcode maps parser binary operators to VDBE opcodes.
var binaryOpToVdbeOpcode = map[parser.BinaryOp]vdbe.Opcode{
	parser.OpEq:    vdbe.OpEq,
	parser.OpNe:    vdbe.OpNe,
	parser.OpLt:    vdbe.OpLt,
	parser.OpLe:    vdbe.OpLe,
	parser.OpGt:    vdbe.OpGt,
	parser.OpGe:    vdbe.OpGe,
	parser.OpAnd:   vdbe.OpAnd,
	parser.OpOr:    vdbe.OpOr,
	parser.OpPlus:  vdbe.OpAdd,
	parser.OpMinus: vdbe.OpSubtract,
	parser.OpMul:   vdbe.OpMultiply,
	parser.OpDiv:   vdbe.OpDivide,
}

// generateHavingBinaryExpr generates code for a binary expression in HAVING.
func (s *Stmt) generateHavingBinaryExpr(vm *vdbe.VDBE, gen *expr.CodeGenerator, expr *parser.BinaryExpr,
	aggregateMap map[string]int, baseReg int) (int, error) {

	// Recursively generate left and right operands
	leftReg, err := s.generateHavingExpression(vm, gen, expr.Left, aggregateMap, baseReg)
	if err != nil {
		return 0, err
	}

	rightReg, err := s.generateHavingExpression(vm, gen, expr.Right, aggregateMap, baseReg)
	if err != nil {
		return 0, err
	}

	// Allocate a result register
	resultReg := gen.AllocReg()

	// Look up the opcode in the table
	opcode, ok := binaryOpToVdbeOpcode[expr.Op]
	if !ok {
		// For unsupported operations, try normal code generation
		return gen.GenerateExpr(expr)
	}

	// Emit the operation
	vm.AddOp(opcode, leftReg, rightReg, resultReg)
	return resultReg, nil
}
