// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// loadAggregateColumnValue is a helper to load a column value for aggregate functions.
// Returns (tempReg, skipAddr, ok) where skipAddr is the address to patch for NULL skip.
func (s *Stmt) loadAggregateColumnValue(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, gen *expr.CodeGenerator) (int, int, bool) {
	if len(fnExpr.Args) == 0 {
		return 0, 0, false
	}

	argIdent, ok := fnExpr.Args[0].(*parser.IdentExpr)
	if !ok {
		return 0, 0, false
	}

	colIdx := table.GetColumnIndex(argIdent.Name)
	if colIdx < 0 {
		return 0, 0, false
	}

	// Get the cursor number from the code generator (handles both regular and ephemeral tables)
	tableCursor, _ := gen.GetCursor(tableName)

	// Load column value into a temp register
	tempReg := gen.AllocReg()
	recordIdx := schemaRecordIdx(table.Columns, colIdx)
	vm.AddOp(vdbe.OpColumn, tableCursor, recordIdx, tempReg)

	// Skip NULL values
	skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0)

	return tempReg, skipAddr, true
}

// emitCountUpdate emits VDBE opcodes to update COUNT accumulator
func (s *Stmt) emitCountUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	// COUNT(*) - count all rows
	if fnExpr.Star || len(fnExpr.Args) == 0 {
		vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)
		return
	}

	// COUNT(column) - count non-NULL values only
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		// Not a column reference, just increment
		vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)
		return
	}

	// Handle DISTINCT if specified
	var distinctSkipAddr int
	if fnExpr.Distinct {
		// Check if value is distinct, skip increment if already seen
		distinctSkipAddr = vm.AddOp(vdbe.OpAggDistinct, tempReg, 0, accReg)
	}

	// Only increment if value is not NULL (loadAggregateColumnValue already added OpIsNull)
	vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)

	// Fix the skip addresses to jump past the increment
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	if fnExpr.Distinct {
		vm.Program[distinctSkipAddr].P2 = endAddr
	}
}

// emitSumUpdate emits VDBE opcodes to update SUM/TOTAL accumulator
func (s *Stmt) emitSumUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		return
	}

	// Handle DISTINCT if specified
	var distinctSkipAddr int
	if fnExpr.Distinct {
		// Check if value is distinct, skip addition if already seen
		distinctSkipAddr = vm.AddOp(vdbe.OpAggDistinct, tempReg, 0, accReg)
	}

	// If accumulator is NOT NULL, jump to add instruction
	addAddr := vm.AddOp(vdbe.OpNotNull, accReg, 0, 0)

	// Accumulator is NULL - copy the first value
	vm.AddOp(vdbe.OpCopy, tempReg, accReg, 0)
	skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)

	// Accumulator is not NULL - add to it
	vm.Program[addAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpAdd, accReg, tempReg, accReg)

	// Patch jump addresses
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[skipToEndAddr].P2 = endAddr
	if fnExpr.Distinct {
		vm.Program[distinctSkipAddr].P2 = endAddr
	}
}

// emitAvgUpdate emits VDBE opcodes to update AVG accumulator (sum and count)
func (s *Stmt) emitAvgUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, sumReg int, countReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		return
	}

	// Handle DISTINCT if specified
	var distinctSkipAddr int
	if fnExpr.Distinct {
		// Check if value is distinct, skip averaging if already seen
		distinctSkipAddr = vm.AddOp(vdbe.OpAggDistinct, tempReg, 0, sumReg)
	}

	// Increment count (always for non-NULL distinct values)
	vm.AddOp(vdbe.OpAddImm, countReg, 1, 0)

	// If sum accumulator is NOT NULL, jump to add instruction
	addAddr := vm.AddOp(vdbe.OpNotNull, sumReg, 0, 0)

	// Sum is NULL - copy the first value
	vm.AddOp(vdbe.OpCopy, tempReg, sumReg, 0)
	skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)

	// Sum is not NULL - add to it
	vm.Program[addAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpAdd, sumReg, tempReg, sumReg)

	// Patch jump addresses
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[skipToEndAddr].P2 = endAddr
	if fnExpr.Distinct {
		vm.Program[distinctSkipAddr].P2 = endAddr
	}
}

// emitMinUpdate emits VDBE opcodes to update MIN accumulator
func (s *Stmt) emitMinUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		return
	}

	// If accumulator is NULL, just copy the value (first value)
	copyAddr := vm.AddOp(vdbe.OpIsNull, accReg, 0, 0)

	// Accumulator is not NULL - compare
	cmpReg := gen.AllocReg()
	vm.AddOp(vdbe.OpLt, tempReg, accReg, cmpReg)
	notLessAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)

	// Copy value (either first value or new min)
	vm.Program[copyAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpCopy, tempReg, accReg, 0)

	// Patch jump addresses
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[notLessAddr].P2 = endAddr
}

// emitMaxUpdate emits VDBE opcodes to update MAX accumulator
func (s *Stmt) emitMaxUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		return
	}

	// If accumulator is NULL, just copy the value (first value)
	copyAddr := vm.AddOp(vdbe.OpIsNull, accReg, 0, 0)

	// Accumulator is not NULL - compare
	cmpReg := gen.AllocReg()
	vm.AddOp(vdbe.OpGt, tempReg, accReg, cmpReg)
	notGreaterAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)

	// Copy value (either first value or new max)
	vm.Program[copyAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpCopy, tempReg, accReg, 0)

	// Patch jump addresses
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[notGreaterAddr].P2 = endAddr
}

// initializeAggregateRegister initializes a single aggregate accumulator register.
func (s *Stmt) initializeAggregateRegister(vm *vdbe.VDBE, funcName string, accReg int, gen *expr.CodeGenerator) (avgCountReg int) {
	switch funcName {
	case "COUNT":
		vm.AddOp(vdbe.OpInteger, 0, accReg, 0)
	case "AVG":
		vm.AddOp(vdbe.OpNull, 0, accReg, 0)
		avgCountReg = gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, 0, avgCountReg, 0)
	case "TOTAL":
		vm.AddOpWithP4Real(vdbe.OpReal, 0, accReg, 0, 0.0)
	case "SUM", "MIN", "MAX":
		vm.AddOp(vdbe.OpNull, 0, accReg, 0)
	}
	return avgCountReg
}

// findAggregateInExpr finds the first aggregate function in an expression tree
func (s *Stmt) findAggregateInExpr(expr parser.Expression) *parser.FunctionExpr {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *parser.FunctionExpr:
		if s.isAggregateExpr(e) {
			return e
		}
	case *parser.BinaryExpr:
		if agg := s.findAggregateInExpr(e.Left); agg != nil {
			return agg
		}
		return s.findAggregateInExpr(e.Right)
	case *parser.UnaryExpr:
		return s.findAggregateInExpr(e.Expr)
	case *parser.ParenExpr:
		return s.findAggregateInExpr(e.Expr)
	}
	return nil
}

// initializeAggregateAccumulators allocates and initializes accumulator registers for aggregate functions
func (s *Stmt) initializeAggregateAccumulators(vm *vdbe.VDBE, stmt *parser.SelectStmt, gen *expr.CodeGenerator) (accRegs []int, avgCountRegs []int) {
	numCols := len(stmt.Columns)
	accRegs = make([]int, numCols)
	avgCountRegs = make([]int, numCols)

	for i, col := range stmt.Columns {
		// For simple aggregate expressions, handle directly
		if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
			accRegs[i] = gen.AllocReg()
			avgCountRegs[i] = s.initializeAggregateRegister(vm, fnExpr.Name, accRegs[i], gen)
			continue
		}

		// For complex expressions containing aggregates (e.g., COUNT(*) + 1)
		if !s.containsAggregate(col.Expr) {
			continue
		}

		// Find the aggregate function in the expression
		fnExpr := s.findAggregateInExpr(col.Expr)
		if fnExpr == nil {
			continue
		}

		accRegs[i] = gen.AllocReg()
		avgCountRegs[i] = s.initializeAggregateRegister(vm, fnExpr.Name, accRegs[i], gen)
	}
	return accRegs, avgCountRegs
}

// compileSelectWithAggregates compiles a SELECT with aggregate functions
func (s *Stmt) compileSelectWithAggregates(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Check if we have GROUP BY
	if len(stmt.GroupBy) > 0 {
		return s.compileSelectWithGroupBy(vm, stmt, tableName, table, args)
	}

	numCols := len(stmt.Columns)

	// Setup VDBE and code generator
	gen := s.setupAggregateVDBE(vm, stmt, tableName, table, numCols)

	// Initialize accumulator registers
	accRegs, avgCountRegs := s.initializeAggregateAccumulators(vm, stmt, gen)

	// Setup args for WHERE clause
	s.setupAggregateArgs(gen, args)

	// Emit scan loop that updates accumulators
	rewindAddr := s.emitAggregateScanLoop(vm, stmt, table, accRegs, avgCountRegs, gen)

	// Emit aggregate output
	afterScanAddr := s.emitAggregateOutput(vm, stmt, accRegs, avgCountRegs, numCols)

	// Close and finalize
	s.finalizeAggregate(vm, rewindAddr, afterScanAddr)

	return vm, nil
}

// setupAggregateVDBE initializes VDBE and code generator for aggregate SELECT.
func (s *Stmt) setupAggregateVDBE(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, numCols int) *expr.CodeGenerator {

	vm.AllocMemory(numCols + 20)

	// Determine cursor number for source table (handles both regular and ephemeral tables)
	tableCursor := s.determineCursorNum(table, vm)

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, tableCursor)

	// Build result column names
	vm.ResultCols = make([]string, numCols)
	for i, col := range stmt.Columns {
		vm.ResultCols[i] = selectColName(col, i)
	}

	// Register table info
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	return gen
}

// setupAggregateArgs sets up args for parameter binding.
func (s *Stmt) setupAggregateArgs(gen *expr.CodeGenerator, args []driver.NamedValue) {
	if len(args) > 0 {
		argValues := make([]interface{}, len(args))
		for i, a := range args {
			argValues[i] = a.Value
		}
		gen.SetArgs(argValues)
	}
}

// emitAggregateScanLoop emits the scan loop that updates accumulators.
func (s *Stmt) emitAggregateScanLoop(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	table *schema.Table, accRegs []int, avgCountRegs []int,
	gen *expr.CodeGenerator) int {

	// Get the cursor number from the code generator (handles both regular and ephemeral tables)
	tableName, _ := selectFromTableName(stmt)
	tableCursor, _ := gen.GetCursor(tableName)

	// Emit scan preamble
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Only open the table cursor if it's not already open (e.g., for ephemeral CTE tables)
	if !table.Temp {
		vm.AddOp(vdbe.OpOpenRead, tableCursor, int(table.RootPage), len(table.Columns))
	}
	rewindAddr := vm.AddOp(vdbe.OpRewind, tableCursor, 0, 0)

	loopStart := vm.NumOps()

	// Handle WHERE clause
	skipAddr := s.emitAggregateWhereClause(vm, stmt, gen)

	// Update accumulators for each aggregate
	s.emitAggregateUpdates(vm, stmt, table, accRegs, avgCountRegs, gen)

	// Fix WHERE skip address
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Continue scan
	vm.AddOp(vdbe.OpNext, tableCursor, loopStart, 0)

	return rewindAddr
}

// emitAggregateWhereClause emits WHERE clause for aggregate SELECT.
func (s *Stmt) emitAggregateWhereClause(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	gen *expr.CodeGenerator) int {

	if stmt.Where == nil {
		return 0
	}

	whereReg, err := gen.GenerateExpr(stmt.Where)
	if err != nil {
		return 0
	}

	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
}

// emitAggregateUpdates emits accumulator updates for all aggregate functions.
func (s *Stmt) emitAggregateUpdates(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	table *schema.Table, accRegs []int, avgCountRegs []int,
	gen *expr.CodeGenerator) {

	// Get table name for cursor lookup
	tableName, _ := selectFromTableName(stmt)

	for i, col := range stmt.Columns {
		// Check if this column contains any aggregates
		if !s.containsAggregate(col.Expr) {
			continue
		}

		// Find the aggregate function (may be nested in an expression)
		fnExpr := s.findAggregateInExpr(col.Expr)
		if fnExpr == nil {
			continue
		}

		s.emitSingleAggregateUpdate(vm, fnExpr, table, tableName, accRegs[i], avgCountRegs[i], gen)
	}
}

// emitSingleAggregateUpdate emits update for a single aggregate function.
func (s *Stmt) emitSingleAggregateUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr,
	table *schema.Table, tableName string, accReg int, avgCountReg int, gen *expr.CodeGenerator) {

	switch fnExpr.Name {
	case "COUNT":
		s.emitCountUpdate(vm, fnExpr, table, tableName, accReg, gen)
	case "SUM", "TOTAL":
		s.emitSumUpdate(vm, fnExpr, table, tableName, accReg, gen)
	case "AVG":
		s.emitAvgUpdate(vm, fnExpr, table, tableName, accReg, avgCountReg, gen)
	case "MIN":
		s.emitMinUpdate(vm, fnExpr, table, tableName, accReg, gen)
	case "MAX":
		s.emitMaxUpdate(vm, fnExpr, table, tableName, accReg, gen)
	}
}

// emitAggregateArithmeticOutput generates code for arithmetic expressions with aggregates
// e.g., COUNT(*) * 2, SUM(value) + 10
func (s *Stmt) emitAggregateArithmeticOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	binExpr *parser.BinaryExpr, accReg int, avgCountReg int, targetReg int) error {

	// Get the aggregate value into a temp register
	tempReg := gen.AllocReg()

	// Check if left side is aggregate
	if fnExpr, ok := binExpr.Left.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
		if fnExpr.Name == "AVG" {
			vm.AddOp(vdbe.OpDivide, accReg, avgCountReg, tempReg)
		} else {
			vm.AddOp(vdbe.OpCopy, accReg, tempReg, 0)
		}

		// Generate code for right side (should be a constant or column)
		rightReg, err := gen.GenerateExpr(binExpr.Right)
		if err != nil {
			return err
		}

		// Apply the binary operation
		return s.emitBinaryOp(vm, binExpr.Op, tempReg, rightReg, targetReg)
	}

	// Check if right side is aggregate
	if fnExpr, ok := binExpr.Right.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
		if fnExpr.Name == "AVG" {
			vm.AddOp(vdbe.OpDivide, accReg, avgCountReg, tempReg)
		} else {
			vm.AddOp(vdbe.OpCopy, accReg, tempReg, 0)
		}

		// Generate code for left side (should be a constant or column)
		leftReg, err := gen.GenerateExpr(binExpr.Left)
		if err != nil {
			return err
		}

		// Apply the binary operation
		return s.emitBinaryOp(vm, binExpr.Op, leftReg, tempReg, targetReg)
	}

	return fmt.Errorf("no aggregate found in expression")
}

// emitBinaryOp emits the appropriate VDBE opcode for a binary operation
func (s *Stmt) emitBinaryOp(vm *vdbe.VDBE, op parser.BinaryOp, leftReg, rightReg, resultReg int) error {
	switch op {
	case parser.OpPlus:
		vm.AddOp(vdbe.OpAdd, leftReg, rightReg, resultReg)
	case parser.OpMinus:
		vm.AddOp(vdbe.OpSubtract, leftReg, rightReg, resultReg)
	case parser.OpMul:
		vm.AddOp(vdbe.OpMultiply, leftReg, rightReg, resultReg)
	case parser.OpDiv:
		vm.AddOp(vdbe.OpDivide, leftReg, rightReg, resultReg)
	case parser.OpRem:
		vm.AddOp(vdbe.OpRemainder, leftReg, rightReg, resultReg)
	default:
		return fmt.Errorf("unsupported binary operator in aggregate expression: %v", op)
	}
	return nil
}

// emitAggregateExpressionOutput generates code for an expression containing aggregates
// by substituting aggregate function calls with their accumulator register values
func (s *Stmt) emitAggregateExpressionOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	expr parser.Expression, accReg int, avgCountReg int, targetReg int) error {

	// Unwrap parentheses
	for {
		if parenExpr, ok := expr.(*parser.ParenExpr); ok {
			expr = parenExpr.Expr
		} else {
			break
		}
	}

	// Handle simple case: direct aggregate function
	if fnExpr, ok := expr.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
		if fnExpr.Name == "AVG" {
			vm.AddOp(vdbe.OpDivide, accReg, avgCountReg, targetReg)
		} else {
			vm.AddOp(vdbe.OpCopy, accReg, targetReg, 0)
		}
		return nil
	}

	// Handle expressions containing aggregates (e.g., COUNT(*) * 2, SUM(value) + 10)
	if s.containsAggregate(expr) {
		// Handle binary expressions with aggregates
		if binExpr, ok := expr.(*parser.BinaryExpr); ok {
			return s.emitAggregateArithmeticOutput(vm, gen, binExpr, accReg, avgCountReg, targetReg)
		}
	}

	// Non-aggregate expression - should be constant or error
	vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
	return nil
}

// emitAggregateOutput emits code to output aggregate results.
func (s *Stmt) emitAggregateOutput(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	accRegs []int, avgCountRegs []int, numCols int) int {

	afterScanAddr := vm.NumOps()

	// Create a code generator for non-aggregate expressions
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.SetNextReg(numCols + 10) // Start after result registers

	// Finalize and copy aggregates to result registers
	for i, col := range stmt.Columns {
		// Check if accumulator was allocated for this column
		if accRegs[i] == 0 && s.containsAggregate(col.Expr) {
			// Accumulator should have been allocated but wasn't - this is a bug
			// For safety, emit NULL
			vm.AddOp(vdbe.OpNull, 0, i, 0)
			continue
		}

		if err := s.emitAggregateExpressionOutput(vm, gen, col.Expr, accRegs[i], avgCountRegs[i], i); err != nil {
			// If error, just put NULL
			vm.AddOp(vdbe.OpNull, 0, i, 0)
		}
	}

	// Evaluate HAVING clause if present (for aggregates without GROUP BY)
	havingSkipAddr := s.emitAggregateHavingClause(vm, stmt, accRegs, avgCountRegs, numCols)

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix HAVING skip address to jump past the result row
	if havingSkipAddr > 0 {
		vm.Program[havingSkipAddr].P2 = vm.NumOps()
	}

	return afterScanAddr
}

// finalizeAggregate emits close and halt instructions.
func (s *Stmt) finalizeAggregate(vm *vdbe.VDBE, rewindAddr int, afterScanAddr int) {
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = afterScanAddr
}

// findColumnIndex finds the index of a column by name in a table
func (s *Stmt) findColumnIndex(table *schema.Table, colName string) int {
	for i, col := range table.Columns {
		if col.Name == colName {
			return i
		}
	}
	return -1
}

// compileSelectWithWindowFunctions compiles a SELECT with window functions using two-pass execution
func (s *Stmt) compileSelectWithWindowFunctions(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	expandedCols, colNames := expandStarColumns(stmt.Columns, table)
	numCols := len(expandedCols)

	vm.AllocMemory(numCols + 50)
	vm.AllocCursors(2)

	gen := s.setupWindowCodeGenerator(vm, tableName, table, args)
	vm.ResultCols = colNames

	s.initializeWindowStates(vm, expandedCols, table)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))

	rankRegs := initWindowRankRegisters(numCols)
	rankInfo := s.analyzeWindowRankFunctions(expandedCols, table)
	emitWindowRankSetup(vm, rankRegs, rankInfo)

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	skipAddr, err := s.compileWindowWhereClause(vm, gen, stmt.Where)
	if err != nil {
		return nil, err
	}

	emitWindowRankTracking(vm, rankRegs, rankInfo, numCols)

	for i := 0; i < numCols; i++ {
		s.emitWindowColumn(vm, gen, expandedCols[i], table, rankRegs, i)
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	s.finalizeWindowLoop(vm, skipAddr, rewindAddr)

	return vm, nil
}

// setupWindowCodeGenerator creates and configures the code generator for window functions.
func (s *Stmt) setupWindowCodeGenerator(vm *vdbe.VDBE, tableName string, table *schema.Table, args []driver.NamedValue) *expr.CodeGenerator {
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, 0)
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	return gen
}

// initializeWindowStates initializes window states for each window function.
func (s *Stmt) initializeWindowStates(vm *vdbe.VDBE, expandedCols []parser.ResultColumn, table *schema.Table) {
	windowStateIdx := 0

	for _, col := range expandedCols {
		fnExpr, ok := col.Expr.(*parser.FunctionExpr)
		if !ok || fnExpr.Over == nil {
			continue
		}

		orderByCols, orderByDesc := s.extractWindowOrderBy(fnExpr.Over, table)
		frame := vdbe.DefaultWindowFrame()
		windowState := vdbe.NewWindowState([]int{}, orderByCols, orderByDesc, frame)
		vm.WindowStates[windowStateIdx] = windowState
		windowStateIdx++
	}
}

// extractWindowOrderBy extracts ORDER BY columns from window specification.
func (s *Stmt) extractWindowOrderBy(over *parser.WindowSpec, table *schema.Table) ([]int, []bool) {
	var orderByCols []int
	var orderByDesc []bool

	if over.OrderBy == nil {
		return orderByCols, orderByDesc
	}

	for _, orderTerm := range over.OrderBy {
		identExpr, ok := orderTerm.Expr.(*parser.IdentExpr)
		if !ok {
			continue
		}
		colIdx := s.findColumnIndex(table, identExpr.Name)
		if colIdx >= 0 {
			orderByCols = append(orderByCols, colIdx)
			orderByDesc = append(orderByDesc, !orderTerm.Asc)
		}
	}

	return orderByCols, orderByDesc
}

// compileWindowWhereClause compiles the WHERE clause for window functions.
func (s *Stmt) compileWindowWhereClause(vm *vdbe.VDBE, gen *expr.CodeGenerator, where parser.Expression) (int, error) {
	if where == nil {
		return -1, nil
	}

	whereReg, err := gen.GenerateExpr(where)
	if err != nil {
		return -1, fmt.Errorf("error compiling WHERE clause: %w", err)
	}
	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0), nil
}

// finalizeWindowLoop finalizes the window function loop.
func (s *Stmt) finalizeWindowLoop(vm *vdbe.VDBE, skipAddr, rewindAddr int) {
	if skipAddr >= 0 {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.Program[rewindAddr].P2 = vm.NumOps()

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
}
