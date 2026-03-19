// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
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
	recordIdx := schemaRecordIdxForTable(table, colIdx)
	vm.AddOp(vdbe.OpColumn, tableCursor, recordIdx, tempReg)

	// Skip NULL values
	skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0)

	return tempReg, skipAddr, true
}

// emitCountUpdate emits VDBE opcodes to update COUNT accumulator
func (s *Stmt) emitCountUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	if fnExpr.Star || len(fnExpr.Args) == 0 {
		vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)
		return
	}

	tempReg, skipAddr := s.loadCountValueReg(vm, fnExpr, table, tableName, gen)
	if tempReg == 0 {
		return
	}

	s.emitCountIncrement(vm, fnExpr, table, accReg, tempReg, skipAddr)
}

// loadCountValueReg loads the value register for COUNT expression
func (s *Stmt) loadCountValueReg(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, gen *expr.CodeGenerator) (int, int) {
	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if ok {
		return tempReg, skipAddr
	}

	if len(fnExpr.Args) == 0 {
		vm.AddOp(vdbe.OpAddImm, vm.Program[len(vm.Program)-1].P1, 1, 0)
		return 0, 0
	}

	exprReg, err := gen.GenerateExpr(fnExpr.Args[0])
	if err != nil {
		return 0, 0
	}
	return exprReg, vm.AddOp(vdbe.OpIsNull, exprReg, 0, 0)
}

// emitCountIncrement emits the increment and distinct check for COUNT
func (s *Stmt) emitCountIncrement(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, accReg, tempReg, skipAddr int) {
	var distinctSkipAddr int
	if fnExpr.Distinct {
		distinctSkipAddr = vm.AddOp(vdbe.OpAggDistinct, tempReg, 0, accReg)
		if len(fnExpr.Args) > 0 {
			if coll := resolveExprCollation(fnExpr.Args[0], table); coll != "" {
				vm.Program[distinctSkipAddr].P4.Z = coll
			}
		}
	}

	vm.AddOp(vdbe.OpAddImm, accReg, 1, 0)

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
		// Not a simple column reference - evaluate the expression
		if len(fnExpr.Args) > 0 {
			exprReg, err := gen.GenerateExpr(fnExpr.Args[0])
			if err != nil {
				return
			}
			tempReg = exprReg
			// Skip NULL values from the expression
			skipAddr = vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0)
		} else {
			return
		}
	}

	// Handle DISTINCT if specified
	var distinctSkipAddr int
	if fnExpr.Distinct {
		// Check if value is distinct, skip addition if already seen
		distinctSkipAddr = vm.AddOp(vdbe.OpAggDistinct, tempReg, 0, accReg)
		if len(fnExpr.Args) > 0 {
			if coll := resolveExprCollation(fnExpr.Args[0], table); coll != "" {
				vm.Program[distinctSkipAddr].P4.Z = coll
			}
		}
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
	cmpAddr := vm.AddOp(vdbe.OpLt, tempReg, accReg, cmpReg)
	if len(fnExpr.Args) > 0 {
		if coll := resolveExprCollation(fnExpr.Args[0], table); coll != "" {
			vm.Program[cmpAddr].P4.Z = coll
			vm.Program[cmpAddr].P4Type = vdbe.P4Static
		}
	}
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
	cmpAddr := vm.AddOp(vdbe.OpGt, tempReg, accReg, cmpReg)
	if len(fnExpr.Args) > 0 {
		if coll := resolveExprCollation(fnExpr.Args[0], table); coll != "" {
			vm.Program[cmpAddr].P4.Z = coll
			vm.Program[cmpAddr].P4Type = vdbe.P4Static
		}
	}
	notGreaterAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)

	// Copy value (either first value or new max)
	vm.Program[copyAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpCopy, tempReg, accReg, 0)

	// Patch jump addresses
	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[notGreaterAddr].P2 = endAddr
}

// emitGroupConcatUpdate emits VDBE opcodes to update GROUP_CONCAT accumulator.
func (s *Stmt) emitGroupConcatUpdate(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, table *schema.Table, tableName string, accReg int, gen *expr.CodeGenerator) {
	if len(fnExpr.Args) == 0 {
		return
	}

	tempReg, skipAddr, ok := s.loadAggregateColumnValue(vm, fnExpr, table, tableName, gen)
	if !ok {
		exprReg, err := gen.GenerateExpr(fnExpr.Args[0])
		if err != nil {
			return
		}
		tempReg = exprReg
		skipAddr = vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0)
	}

	sepReg := gen.AllocReg()
	if len(fnExpr.Args) > 1 {
		if lit, ok := fnExpr.Args[1].(*parser.LiteralExpr); ok && lit.Type == parser.LiteralString {
			vm.AddOpWithP4Str(vdbe.OpString8, 0, sepReg, 0, lit.Value)
		} else {
			vm.AddOpWithP4Str(vdbe.OpString8, 0, sepReg, 0, ",")
		}
	} else {
		vm.AddOpWithP4Str(vdbe.OpString8, 0, sepReg, 0, ",")
	}

	copyAddr := vm.AddOp(vdbe.OpIsNull, accReg, 0, 0)

	tmpReg := gen.AllocReg()
	vm.AddOp(vdbe.OpConcat, accReg, sepReg, tmpReg)
	vm.AddOp(vdbe.OpConcat, tmpReg, tempReg, accReg)

	skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	vm.Program[copyAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpCopy, tempReg, accReg, 0)

	endAddr := vm.NumOps()
	vm.Program[skipAddr].P2 = endAddr
	vm.Program[skipToEndAddr].P2 = endAddr
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
	case "SUM", "MIN", "MAX", "GROUP_CONCAT":
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
		return s.tryGetAggregateFn(e)
	case *parser.BinaryExpr:
		return s.findAggregateInBinary(e)
	case *parser.UnaryExpr:
		return s.findAggregateInExpr(e.Expr)
	case *parser.ParenExpr:
		return s.findAggregateInExpr(e.Expr)
	}
	return nil
}

// tryGetAggregateFn returns function if it's an aggregate
func (s *Stmt) tryGetAggregateFn(fnExpr *parser.FunctionExpr) *parser.FunctionExpr {
	if s.isAggregateExpr(fnExpr) {
		return fnExpr
	}
	return nil
}

// findAggregateInBinary finds aggregate in binary expression
func (s *Stmt) findAggregateInBinary(binExpr *parser.BinaryExpr) *parser.FunctionExpr {
	if agg := s.findAggregateInExpr(binExpr.Left); agg != nil {
		return agg
	}
	return s.findAggregateInExpr(binExpr.Right)
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

	tableCursor := s.determineCursorNum(table, vm)

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, tableCursor)

	alias := s.fromTableAlias(stmt)
	if alias != "" && alias != tableName {
		gen.RegisterCursor(alias, tableCursor)
	}

	vm.ResultCols = make([]string, numCols)
	for i, col := range stmt.Columns {
		vm.ResultCols[i] = selectColName(col, i)
	}

	s.registerAggTableInfo(gen, stmt, tableName, table)

	return gen
}

// fromTableAlias returns the alias of the first FROM table, or empty string.
func (s *Stmt) fromTableAlias(stmt *parser.SelectStmt) string {
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		return stmt.From.Tables[0].Alias
	}
	return ""
}

// registerAggTableInfo registers primary and alias table info in the code generator.
func (s *Stmt) registerAggTableInfo(gen *expr.CodeGenerator, stmt *parser.SelectStmt, tableName string, table *schema.Table) {
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	alias := s.fromTableAlias(stmt)
	if alias != "" && alias != tableName {
		aliasInfo := buildTableInfo(tableName, table)
		aliasInfo.Name = alias
		gen.RegisterTable(aliasInfo)
	}
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
	case "GROUP_CONCAT":
		s.emitGroupConcatUpdate(vm, fnExpr, table, tableName, accReg, gen)
	}
}

// emitAggregateArithmeticOutput generates code for arithmetic expressions with aggregates
// e.g., COUNT(*) * 2, SUM(value) + 10
func (s *Stmt) emitAggregateArithmeticOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	binExpr *parser.BinaryExpr, accReg int, avgCountReg int, targetReg int) error {

	tempReg := gen.AllocReg()

	// Check if left side is aggregate
	if fnExpr, ok := binExpr.Left.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
		return s.emitLeftAggregateOutput(vm, gen, binExpr, fnExpr, accReg, avgCountReg, tempReg, targetReg)
	}

	// Check if right side is aggregate
	if fnExpr, ok := binExpr.Right.(*parser.FunctionExpr); ok && s.isAggregateExpr(fnExpr) {
		return s.emitRightAggregateOutput(vm, gen, binExpr, fnExpr, accReg, avgCountReg, tempReg, targetReg)
	}

	return fmt.Errorf("no aggregate found in expression")
}

// emitLeftAggregateOutput handles binary expressions with aggregate on the left side.
func (s *Stmt) emitLeftAggregateOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	binExpr *parser.BinaryExpr, fnExpr *parser.FunctionExpr,
	accReg int, avgCountReg int, tempReg int, targetReg int) error {

	s.emitAggregateCopy(vm, fnExpr.Name, accReg, avgCountReg, tempReg)

	rightReg, err := gen.GenerateExpr(binExpr.Right)
	if err != nil {
		return err
	}

	return s.emitBinaryOp(vm, binExpr.Op, tempReg, rightReg, targetReg)
}

// emitRightAggregateOutput handles binary expressions with aggregate on the right side.
func (s *Stmt) emitRightAggregateOutput(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	binExpr *parser.BinaryExpr, fnExpr *parser.FunctionExpr,
	accReg int, avgCountReg int, tempReg int, targetReg int) error {

	s.emitAggregateCopy(vm, fnExpr.Name, accReg, avgCountReg, tempReg)

	leftReg, err := gen.GenerateExpr(binExpr.Left)
	if err != nil {
		return err
	}

	return s.emitBinaryOp(vm, binExpr.Op, leftReg, tempReg, targetReg)
}

// emitAggregateCopy copies aggregate value to target register (divides for AVG).
func (s *Stmt) emitAggregateCopy(vm *vdbe.VDBE, funcName string, accReg int, avgCountReg int, targetReg int) {
	if funcName == "AVG" {
		vm.AddOp(vdbe.OpDivide, accReg, avgCountReg, targetReg)
		vm.AddOp(vdbe.OpToReal, targetReg, 0, 0)
	} else {
		vm.AddOp(vdbe.OpCopy, accReg, targetReg, 0)
	}
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

	expr = unwrapParentheses(expr)

	if s.tryEmitDirectAggregate(vm, expr, accReg, avgCountReg, targetReg) {
		return nil
	}

	if s.containsAggregate(expr) {
		if binExpr, ok := expr.(*parser.BinaryExpr); ok {
			return s.emitAggregateArithmeticOutput(vm, gen, binExpr, accReg, avgCountReg, targetReg)
		}
	}

	vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
	return nil
}

// unwrapParentheses removes parentheses wrapping an expression
func unwrapParentheses(expr parser.Expression) parser.Expression {
	for {
		parenExpr, ok := expr.(*parser.ParenExpr)
		if !ok {
			return expr
		}
		expr = parenExpr.Expr
	}
}

// tryEmitDirectAggregate tries to emit a direct aggregate function
func (s *Stmt) tryEmitDirectAggregate(vm *vdbe.VDBE, expr parser.Expression, accReg, avgCountReg, targetReg int) bool {
	fnExpr, ok := expr.(*parser.FunctionExpr)
	if !ok || !s.isAggregateExpr(fnExpr) {
		return false
	}

	if fnExpr.Name == "AVG" {
		vm.AddOp(vdbe.OpDivide, accReg, avgCountReg, targetReg)
		vm.AddOp(vdbe.OpToReal, targetReg, 0, 0)
	} else {
		vm.AddOp(vdbe.OpCopy, accReg, targetReg, 0)
	}
	return true
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
	// Try exact match first
	for i, col := range table.Columns {
		if col.Name == colName {
			return i
		}
	}
	// Try case-insensitive match
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, colName) {
			return i
		}
	}
	// Try with uppercase column name
	upperColName := strings.ToUpper(colName)
	for i, col := range table.Columns {
		if col.Name == upperColName || strings.ToUpper(col.Name) == upperColName {
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

	// Check if we need to sort for window ORDER BY
	needsSorting, orderByCols, orderByDesc := s.detectWindowOrderBy(expandedCols, table)

	if needsSorting {
		return s.compileWindowWithSorting(vm, stmt, expandedCols, numCols, table, gen, orderByCols, orderByDesc)
	}

	// No sorting needed - use simple table scan
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
	seenOverClauses := make(map[string]int)
	windowFunctionCounts := make(map[int]int)
	windowStateIdx := 0

	for _, col := range expandedCols {
		s.collectWindowFuncs(col.Expr, table, vm, seenOverClauses, windowFunctionCounts, &windowStateIdx)
	}

	for idx, count := range windowFunctionCounts {
		if ws, ok := vm.WindowStates[idx]; ok {
			ws.WindowFunctionCount = count
		}
	}
}

// collectWindowFuncs recursively finds window functions in an expression tree
// and creates window states for them.
func (s *Stmt) collectWindowFuncs(e parser.Expression, table *schema.Table, vm *vdbe.VDBE,
	seen map[string]int, counts map[int]int, nextIdx *int) {

	if e == nil {
		return
	}
	fnExpr, ok := e.(*parser.FunctionExpr)
	if !ok {
		return
	}
	if fnExpr.Over != nil {
		partCols := s.extractPartitionByCols(fnExpr.Over, table)
		orderByCols, orderByDesc := s.extractWindowOrderBy(fnExpr.Over, table)
		overKey := s.makeOverClauseKey(orderByCols, orderByDesc)

		if existingIdx, exists := seen[overKey]; exists {
			counts[existingIdx]++
		} else {
			frame := s.extractWindowFrame(fnExpr.Over.Frame)
			windowState := vdbe.NewWindowState(partCols, orderByCols, orderByDesc, frame)
			vm.WindowStates[*nextIdx] = windowState
			seen[overKey] = *nextIdx
			counts[*nextIdx] = 1
			*nextIdx++
		}
		return
	}
	// Recurse into function args to find nested window functions
	for _, arg := range fnExpr.Args {
		s.collectWindowFuncs(arg, table, vm, seen, counts, nextIdx)
	}
}

// extractPartitionByCols extracts column indices from PARTITION BY expressions.
func (s *Stmt) extractPartitionByCols(over *parser.WindowSpec, table *schema.Table) []int {
	if over == nil || len(over.PartitionBy) == 0 {
		return nil
	}
	var cols []int
	for _, expr := range over.PartitionBy {
		if ident, ok := expr.(*parser.IdentExpr); ok {
			idx := s.findColumnIndex(table, ident.Name)
			if idx >= 0 {
				cols = append(cols, idx)
			}
		}
	}
	return cols
}

// makeOverClauseKey creates a unique key for an OVER clause based on its ORDER BY specification.
func (s *Stmt) makeOverClauseKey(orderByCols []int, orderByDesc []bool) string {
	if len(orderByCols) == 0 {
		return "no-order"
	}
	key := ""
	for i, col := range orderByCols {
		if i > 0 {
			key += ","
		}
		key += fmt.Sprintf("%d", col)
		if orderByDesc[i] {
			key += "D"
		} else {
			key += "A"
		}
	}
	return key
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

// detectWindowOrderBy checks if any window function has ORDER BY and extracts the columns
func (s *Stmt) detectWindowOrderBy(expandedCols []parser.ResultColumn, table *schema.Table) (bool, []int, []bool) {
	for _, col := range expandedCols {
		if found, orderByCols, orderByDesc := s.findWindowOrderBy(col.Expr, table); found {
			return true, orderByCols, orderByDesc
		}
	}
	return false, nil, nil
}

// findWindowOrderBy recursively searches an expression for a window function with ORDER BY.
// Returns partition+order columns combined (partition cols first, ASC) for sorter use.
func (s *Stmt) findWindowOrderBy(e parser.Expression, table *schema.Table) (bool, []int, []bool) {
	if e == nil {
		return false, nil, nil
	}
	fnExpr, ok := e.(*parser.FunctionExpr)
	if !ok {
		return false, nil, nil
	}
	if fnExpr.Over != nil {
		partCols := s.extractPartitionByCols(fnExpr.Over, table)
		orderByCols, orderByDesc := s.extractWindowOrderBy(fnExpr.Over, table)

		// Combine: partition cols (ASC) first, then order by cols
		allCols := make([]int, 0, len(partCols)+len(orderByCols))
		allDesc := make([]bool, 0, len(partCols)+len(orderByCols))
		for _, pc := range partCols {
			allCols = append(allCols, pc)
			allDesc = append(allDesc, false)
		}
		allCols = append(allCols, orderByCols...)
		allDesc = append(allDesc, orderByDesc...)

		if len(allCols) > 0 {
			return true, allCols, allDesc
		}
	}
	// Recurse into function arguments
	for _, arg := range fnExpr.Args {
		if found, cols, desc := s.findWindowOrderBy(arg, table); found {
			return true, cols, desc
		}
	}
	return false, nil, nil
}

// compileWindowWithSorting compiles window functions with sorting
func (s *Stmt) compileWindowWithSorting(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	expandedCols []parser.ResultColumn, numCols int, table *schema.Table,
	gen *expr.CodeGenerator, orderByCols []int, orderByDesc []bool) (*vdbe.VDBE, error) {

	// Calculate total columns needed for sorter (all table columns)
	numTableCols := len(table.Columns)
	sorterCols := numTableCols

	// Setup sorter with key info
	keyInfo := &vdbe.SorterKeyInfo{
		KeyCols:    orderByCols,
		Desc:       orderByDesc,
		Collations: make([]string, len(orderByCols)),
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))

	// Open sorter
	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 1, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	// First pass: populate sorter
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	var skipAddr int
	if stmt.Where != nil {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, err
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Read all columns from table into registers
	for i := 0; i < numTableCols; i++ {
		vm.AddOp(vdbe.OpColumn, 0, i, i)
	}

	// Insert into sorter
	vm.AddOp(vdbe.OpSorterInsert, 1, 0, sorterCols)

	// Fix skip address if WHERE exists
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Complete first pass
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.Program[rewindAddr].P2 = vm.NumOps()

	// Close the table
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Sort the data
	vm.AddOp(vdbe.OpSorterSort, 1, 0, 0)

	// Populate window states with all rows from the sorted data
	// This is needed for frame-dependent functions like NTH_VALUE, FIRST_VALUE, LAST_VALUE
	collectNextAddr := vm.AddOp(vdbe.OpSorterNext, 1, 0, 0)
	collectSkipAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	collectLoopAddr := vm.NumOps()

	vm.AddOp(vdbe.OpSorterData, 1, 0, numTableCols)

	// Add row to window state 0
	addr := vm.AddOp(vdbe.OpAggStepWindow, 0, 0, 0)
	vm.Program[addr].P4.Z = "_window_feed"
	vm.Program[addr].P4Type = vdbe.P4Static
	vm.Program[addr].P5 = uint16(numTableCols)

	vm.AddOp(vdbe.OpSorterNext, 1, collectLoopAddr, 0)
	vm.Program[collectNextAddr].P2 = collectLoopAddr
	vm.Program[collectSkipAddr].P2 = vm.NumOps()

	// Re-sort to rewind the sorter for the output pass
	vm.AddOp(vdbe.OpSorterSort, 1, 0, 0)

	// Output pass: read from sorter and compute window functions
	sorterNextAddr := vm.AddOp(vdbe.OpSorterNext, 1, 0, 0)
	haltJumpAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	sorterLoopAddr := vm.NumOps()

	// Read all data from sorter into registers 0..numTableCols-1
	vm.AddOp(vdbe.OpSorterData, 1, 0, numTableCols)

	// Emit columns
	for i := 0; i < numCols; i++ {
		s.emitWindowColumnFromSorter(vm, gen, expandedCols[i], table, i)
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Loop back to get next row from sorter
	vm.AddOp(vdbe.OpSorterNext, 1, sorterLoopAddr, 0)

	// Fix addresses
	haltAddr := vm.NumOps()
	vm.Program[sorterNextAddr].P2 = sorterLoopAddr
	vm.Program[haltJumpAddr].P2 = haltAddr

	// Close sorter and halt
	vm.AddOp(vdbe.OpSorterClose, 1, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// emitWindowColumnFromSorter emits code for a column when reading from sorter.
func (s *Stmt) emitWindowColumnFromSorter(vm *vdbe.VDBE, gen *expr.CodeGenerator, col parser.ResultColumn,
	table *schema.Table, colIdx int) {

	numTableCols := len(table.Columns)

	if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && fnExpr.Over != nil {
		s.emitWindowFunctionColumnWithOpcodes(vm, fnExpr, colIdx, numTableCols)
		return
	}

	if s.isWindowFunctionExpr(col.Expr) {
		s.precomputeNestedWindowFuncs(vm, gen, col.Expr, table)
		s.emitGeneratedExpr(vm, gen, col.Expr, colIdx)
		return
	}

	s.emitSorterColumnValue(vm, gen, col.Expr, table, colIdx)
}

// emitGeneratedExpr generates code for an expression and copies result to colIdx.
func (s *Stmt) emitGeneratedExpr(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	e parser.Expression, colIdx int) {

	reg, err := gen.GenerateExpr(e)
	if err == nil && reg != colIdx {
		vm.AddOp(vdbe.OpCopy, reg, colIdx, 0)
	} else if err != nil {
		vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
	}
}

// emitSorterColumnValue emits code for a regular column or expression from sorter data.
func (s *Stmt) emitSorterColumnValue(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	e parser.Expression, table *schema.Table, colIdx int) {

	if identExpr, ok := e.(*parser.IdentExpr); ok {
		tableColIdx := s.findColumnIndex(table, identExpr.Name)
		if tableColIdx >= 0 && tableColIdx != colIdx {
			vm.AddOp(vdbe.OpCopy, tableColIdx, colIdx, 0)
		} else if tableColIdx < 0 {
			vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
		}
		return
	}

	s.emitGeneratedExpr(vm, gen, e, colIdx)
}

// precomputeNestedWindowFuncs finds window function calls inside an expression,
// emits their opcodes into temporary registers, and registers them as precomputed
// so the code generator skips them.
func (s *Stmt) precomputeNestedWindowFuncs(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	e parser.Expression, table *schema.Table) {

	numTableCols := len(table.Columns)
	s.walkAndPrecompute(vm, gen, e, numTableCols)
}

// walkAndPrecompute recursively walks an expression tree to precompute window functions.
func (s *Stmt) walkAndPrecompute(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	e parser.Expression, numTableCols int) {

	if e == nil {
		return
	}

	fnExpr, ok := e.(*parser.FunctionExpr)
	if ok && fnExpr.Over != nil {
		reg := gen.AllocReg()
		s.emitWindowFunctionColumnWithOpcodes(vm, fnExpr, reg, numTableCols)
		gen.SetPrecomputed(e, reg)
		return
	}

	if ok {
		for _, arg := range fnExpr.Args {
			s.walkAndPrecompute(vm, gen, arg, numTableCols)
		}
		return
	}

	s.walkAndPrecomputeChildren(vm, gen, e, numTableCols)
}

// walkAndPrecomputeChildren walks non-function expression children.
func (s *Stmt) walkAndPrecomputeChildren(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	e parser.Expression, numTableCols int) {

	switch ex := e.(type) {
	case *parser.BinaryExpr:
		s.walkAndPrecompute(vm, gen, ex.Left, numTableCols)
		s.walkAndPrecompute(vm, gen, ex.Right, numTableCols)
	case *parser.UnaryExpr:
		s.walkAndPrecompute(vm, gen, ex.Expr, numTableCols)
	case *parser.ParenExpr:
		s.walkAndPrecompute(vm, gen, ex.Expr, numTableCols)
	case *parser.CastExpr:
		s.walkAndPrecompute(vm, gen, ex.Expr, numTableCols)
	case *parser.CaseExpr:
		s.walkAndPrecomputeCase(vm, gen, ex, numTableCols)
	}
}

// walkAndPrecomputeCase walks CASE expression children.
func (s *Stmt) walkAndPrecomputeCase(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	ex *parser.CaseExpr, numTableCols int) {

	for _, w := range ex.WhenClauses {
		s.walkAndPrecompute(vm, gen, w.Condition, numTableCols)
		s.walkAndPrecompute(vm, gen, w.Result, numTableCols)
	}
	s.walkAndPrecompute(vm, gen, ex.ElseClause, numTableCols)
}
