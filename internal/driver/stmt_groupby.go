package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileSelectWithGroupBy compiles a SELECT with GROUP BY clause using a simplified row-by-row approach.
func (s *Stmt) compileSelectWithGroupBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	numCols := len(stmt.Columns)
	numGroupBy := len(stmt.GroupBy)

	// Setup VDBE and code generator
	vm.AllocMemory(numCols + numGroupBy*3 + 50)
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

	// Setup args
	s.setupAggregateArgs(gen, args)

	// Allocate registers
	groupByRegs := make([]int, numGroupBy)
	prevGroupByRegs := make([]int, numGroupBy)
	for i := 0; i < numGroupBy; i++ {
		groupByRegs[i] = gen.AllocReg()
		prevGroupByRegs[i] = gen.AllocReg()
	}

	// Allocate accumulator registers
	accRegs := make([]int, numCols)
	avgCountRegs := make([]int, numCols)
	for i, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			accRegs[i] = gen.AllocReg()
			fnExpr := col.Expr.(*parser.FunctionExpr)
			if fnExpr.Name == "AVG" {
				avgCountRegs[i] = gen.AllocReg()
			}
		}
	}

	// Flag register to track if we've seen the first row
	firstRowReg := gen.AllocReg()

	// Init
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Initialize first row flag to 1 (true)
	vm.AddOp(vdbe.OpInteger, 1, firstRowReg, 0)

	// Initialize prev GROUP BY registers to NULL
	for i := 0; i < numGroupBy; i++ {
		vm.AddOp(vdbe.OpNull, 0, prevGroupByRegs[i], 0)
	}

	// Open cursor
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	loopStart := vm.NumOps()

	// WHERE clause
	skipAddr := -1
	if stmt.Where != nil {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("error compiling WHERE clause: %w", err)
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Evaluate GROUP BY expressions into groupByRegs
	for i, groupExpr := range stmt.GroupBy {
		reg, err := gen.GenerateExpr(groupExpr)
		if err != nil {
			return nil, fmt.Errorf("error compiling GROUP BY expression: %w", err)
		}
		vm.AddOp(vdbe.OpCopy, reg, groupByRegs[i], 0)
	}

	// Check if group has changed (skip on first row)
	skipCheckAddr := vm.AddOp(vdbe.OpIf, firstRowReg, 0, 0)

	// Compare GROUP BY values - if any differ, output previous group
	for i := 0; i < numGroupBy; i++ {
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpNe, groupByRegs[i], prevGroupByRegs[i], cmpReg)
		outputGroupAddr := vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)

		// Patch to output group
		outputAddr := vm.NumOps()
		s.emitGroupOutput(vm, stmt, accRegs, avgCountRegs, prevGroupByRegs, numCols)
		vm.Program[outputGroupAddr].P2 = outputAddr
	}

	// Patch first row skip
	initAccumulatorsAddr := vm.NumOps()
	vm.Program[skipCheckAddr].P2 = initAccumulatorsAddr

	// Clear first row flag
	vm.AddOp(vdbe.OpInteger, 0, firstRowReg, 0)

	// Initialize/reset accumulators
	for i, col := range stmt.Columns {
		if !s.isAggregateExpr(col.Expr) {
			continue
		}
		fnExpr := col.Expr.(*parser.FunctionExpr)
		avgCountRegs[i] = s.initializeAggregateRegister(vm, fnExpr.Name, accRegs[i], gen)
	}

	// Save current GROUP BY values to prev
	for i := 0; i < numGroupBy; i++ {
		vm.AddOp(vdbe.OpCopy, groupByRegs[i], prevGroupByRegs[i], 0)
	}

	// Update accumulators
	for i, col := range stmt.Columns {
		if !s.isAggregateExpr(col.Expr) {
			continue
		}
		fnExpr := col.Expr.(*parser.FunctionExpr)
		s.emitSingleAggregateUpdate(vm, fnExpr, table, accRegs[i], avgCountRegs[i], gen)
	}

	// Fix WHERE skip
	if skipAddr >= 0 {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Next row
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)

	// After scan - output last group
	afterScanAddr := vm.NumOps()

	// Check if we processed any rows (firstRowReg == 0 means we did)
	checkHasRowsReg := gen.AllocReg()
	vm.AddOp(vdbe.OpNot, firstRowReg, checkHasRowsReg, 0)
	skipFinalOutputAddr := vm.AddOp(vdbe.OpIfNot, checkHasRowsReg, 0, 0)

	// Output final group
	s.emitGroupOutput(vm, stmt, accRegs, avgCountRegs, prevGroupByRegs, numCols)

	// Close and halt
	vm.Program[skipFinalOutputAddr].P2 = vm.NumOps()
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix rewind jump
	vm.Program[rewindAddr].P2 = afterScanAddr

	return vm, nil
}

// emitGroupOutput outputs a single group's results.
func (s *Stmt) emitGroupOutput(vm *vdbe.VDBE, stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int, groupByRegs []int, numCols int) {
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
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
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

// emitAggregateHavingClause emits HAVING clause check for aggregate output.
// Returns the address of the IfNot instruction to skip the row if HAVING fails, or 0 if no HAVING clause.
func (s *Stmt) emitAggregateHavingClause(vm *vdbe.VDBE, stmt *parser.SelectStmt, accRegs []int, avgCountRegs []int, numCols int) int {
	if stmt.Having == nil {
		return 0
	}

	// Create a code generator
	gen := expr.NewCodeGenerator(vm)

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

	// Emit the appropriate comparison or logical operation
	switch expr.Op {
	case parser.OpEq:
		vm.AddOp(vdbe.OpEq, leftReg, rightReg, resultReg)
	case parser.OpNe:
		vm.AddOp(vdbe.OpNe, leftReg, rightReg, resultReg)
	case parser.OpLt:
		vm.AddOp(vdbe.OpLt, leftReg, rightReg, resultReg)
	case parser.OpLe:
		vm.AddOp(vdbe.OpLe, leftReg, rightReg, resultReg)
	case parser.OpGt:
		vm.AddOp(vdbe.OpGt, leftReg, rightReg, resultReg)
	case parser.OpGe:
		vm.AddOp(vdbe.OpGe, leftReg, rightReg, resultReg)
	case parser.OpAnd:
		vm.AddOp(vdbe.OpAnd, leftReg, rightReg, resultReg)
	case parser.OpOr:
		vm.AddOp(vdbe.OpOr, leftReg, rightReg, resultReg)
	case parser.OpPlus:
		vm.AddOp(vdbe.OpAdd, leftReg, rightReg, resultReg)
	case parser.OpMinus:
		vm.AddOp(vdbe.OpSubtract, leftReg, rightReg, resultReg)
	case parser.OpMul:
		vm.AddOp(vdbe.OpMultiply, leftReg, rightReg, resultReg)
	case parser.OpDiv:
		vm.AddOp(vdbe.OpDivide, leftReg, rightReg, resultReg)
	default:
		// For unsupported operations, try normal code generation
		return gen.GenerateExpr(expr)
	}

	return resultReg, nil
}
