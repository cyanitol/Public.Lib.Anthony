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
func (s *Stmt) loadAggregateColumnValue(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, gen *expr.CodeGenerator) (int, int, bool) {
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

	// Load column value into a temp register
	tempReg := gen.AllocReg()
	recordIdx := schemaRecordIdx(table.Columns, colIdx)
	vm.AddOp(vdbe.OpColumn, 0, recordIdx, tempReg)

	// Skip NULL values
	skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0)

	return tempReg, skipAddr, true
}

// emitCountUpdate emits VDBE opcodes to update COUNT accumulator
func (s *Stmt) emitCountUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, accReg int) {
	// COUNT(*) or COUNT(expr) - for now both just increment
	vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)
}

// emitSumUpdate emits VDBE opcodes to update SUM/TOTAL accumulator
func (s *Stmt) emitSumUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, gen)
	if !ok {
		return
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
}

// emitAvgUpdate emits VDBE opcodes to update AVG accumulator (sum and count)
func (s *Stmt) emitAvgUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, sumReg int, countReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, gen)
	if !ok {
		return
	}

	// Increment count (always for non-NULL values)
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
}

// emitMinUpdate emits VDBE opcodes to update MIN accumulator
func (s *Stmt) emitMinUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, gen)
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
func (s *Stmt) emitMaxUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, accReg int, gen *expr.CodeGenerator) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, gen)
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

// initializeAggregateAccumulators allocates and initializes accumulator registers for aggregate functions
func (s *Stmt) initializeAggregateAccumulators(vm *vdbe.VDBE, stmt *parser.SelectStmt, gen *expr.CodeGenerator) (accRegs []int, avgCountRegs []int) {
	numCols := len(stmt.Columns)
	accRegs = make([]int, numCols)
	avgCountRegs = make([]int, numCols)

	for i, col := range stmt.Columns {
		fnExpr, isAgg := col.Expr.(*parser.FunctionExpr)
		if !isAgg || !s.isAggregateExpr(col.Expr) {
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
	vm.AllocCursors(1)

	gen := expr.NewCodeGenerator(vm)
	gen.RegisterCursor(tableName, 0)

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

	// Emit scan preamble
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

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
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)

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

	for i, col := range stmt.Columns {
		fnExpr, isAgg := col.Expr.(*parser.FunctionExpr)
		if !isAgg || !s.isAggregateExpr(col.Expr) {
			continue
		}

		s.emitSingleAggregateUpdate(vm, fnExpr, table, accRegs[i], avgCountRegs[i], gen)
	}
}

// emitSingleAggregateUpdate emits update for a single aggregate function.
func (s *Stmt) emitSingleAggregateUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr,
	table *schema.Table, accReg int, avgCountReg int, gen *expr.CodeGenerator) {

	switch fnExpr.Name {
	case "COUNT":
		s.emitCountUpdate(vm, fnExpr, accReg)
	case "SUM", "TOTAL":
		s.emitSumUpdate(vm, fnExpr, table, accReg, gen)
	case "AVG":
		s.emitAvgUpdate(vm, fnExpr, table, accReg, avgCountReg, gen)
	case "MIN":
		s.emitMinUpdate(vm, fnExpr, table, accReg, gen)
	case "MAX":
		s.emitMaxUpdate(vm, fnExpr, table, accReg, gen)
	}
}

// emitAggregateOutput emits code to output aggregate results.
func (s *Stmt) emitAggregateOutput(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	accRegs []int, avgCountRegs []int, numCols int) int {

	afterScanAddr := vm.NumOps()

	// Finalize and copy aggregates to result registers
	for i, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			fnExpr := col.Expr.(*parser.FunctionExpr)
			if fnExpr.Name == "AVG" {
				vm.AddOp(vdbe.OpDivide, accRegs[i], avgCountRegs[i], i)
			} else {
				vm.AddOp(vdbe.OpCopy, accRegs[i], i, 0)
			}
		} else {
			// Non-aggregate column (should be constant or error)
			vm.AddOp(vdbe.OpNull, 0, i, 0)
		}
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

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

	// Expand SELECT *
	expandedCols, colNames := expandStarColumns(stmt.Columns, table)
	numCols := len(expandedCols)

	vm.AllocMemory(numCols + 50) // Extra memory for window state
	vm.AllocCursors(2)            // Cursor 0 for table, cursor 1 for ephemeral table

	// Setup code generator
	gen := expr.NewCodeGenerator(vm)
	gen.RegisterCursor(tableName, 0)
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	// Setup args
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	vm.ResultCols = colNames

	// Initialize window states for each window function
	windowStateIdx := 0
	windowFuncMap := make(map[int]int) // Maps column index to window state index

	for i, col := range expandedCols {
		if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && fnExpr.Over != nil {
			// Create window state for this window function
			partitionCols := []int{}
			orderByCols := []int{}
			orderByDesc := []bool{}

			// Extract ORDER BY columns from window spec
			if fnExpr.Over.OrderBy != nil {
				for _, orderTerm := range fnExpr.Over.OrderBy {
					if identExpr, ok := orderTerm.Expr.(*parser.IdentExpr); ok {
						colIdx := s.findColumnIndex(table, identExpr.Name)
						if colIdx >= 0 {
							orderByCols = append(orderByCols, colIdx)
							orderByDesc = append(orderByDesc, !orderTerm.Asc)
						}
					}
				}
			}

			// Create default window frame if not specified
			frame := vdbe.DefaultWindowFrame()

			// Initialize window state in VDBE
			windowState := vdbe.NewWindowState(partitionCols, orderByCols, orderByDesc, frame)
			vm.WindowStates[windowStateIdx] = windowState
			windowFuncMap[i] = windowStateIdx
			windowStateIdx++
		}
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))

	// Initialize rank tracking registers and analyze rank functions
	rankRegs := initWindowRankRegisters(numCols)
	rankInfo := s.analyzeWindowRankFunctions(expandedCols, table)
	emitWindowRankSetup(vm, rankRegs, rankInfo)

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// WHERE clause
	skipAddr := -1
	if stmt.Where != nil {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("error compiling WHERE clause: %w", err)
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Emit rank tracking logic
	emitWindowRankTracking(vm, rankRegs, rankInfo, numCols)

	// Extract columns
	for i := 0; i < numCols; i++ {
		s.emitWindowColumn(vm, gen, expandedCols[i], table, rankRegs, i)
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	if skipAddr >= 0 {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.Program[rewindAddr].P2 = vm.NumOps()

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}
