// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileSelect compiles a SELECT statement into VDBE bytecode.
func (s *Stmt) compileSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	// Handle special SELECT types
	if specialVM, err, handled := s.handleSpecialSelectTypes(vm, stmt, args); handled {
		return specialVM, err
	}

	// Get table and check for special cases
	tableName, table, err := s.resolveSelectTable(stmt)
	if err != nil {
		return nil, err
	}

	// Route to specialized compilers
	if routedVM, err, handled := s.routeSpecializedSelect(vm, stmt, tableName, table, args); handled {
		return routedVM, err
	}

	// Compile simple single-table SELECT
	return s.compileSimpleSelect(vm, stmt, tableName, table, args)
}

// handleSpecialSelectTypes handles compounds, CTEs, views, subqueries, and no-FROM selects.
func (s *Stmt) handleSpecialSelectTypes(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	// Handle compound SELECT (UNION, UNION ALL, INTERSECT, EXCEPT)
	if stmt.Compound != nil {
		result, err := s.compileCompoundSelect(vm, stmt, args)
		return result, err, true
	}

	// Handle WITH clause (CTEs)
	if stmt.With != nil {
		result, err := s.compileSelectWithCTEs(vm, stmt, args)
		return result, err, true
	}

	// Expand views
	expandedStmt, err := planner.ExpandViewsInSelect(stmt, s.conn.schema)
	if err != nil {
		return nil, err, true
	}
	*stmt = *expandedStmt

	// Handle FROM subqueries
	if s.hasFromSubqueries(stmt) {
		result, err := s.compileSelectWithFromSubqueries(vm, stmt, args)
		return result, err, true
	}

	// Handle SELECT without FROM
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		result, err := s.compileSelectWithoutFrom(vm, stmt, args)
		return result, err, true
	}

	return nil, nil, false
}

// resolveSelectTable gets the table name and schema for the SELECT.
func (s *Stmt) resolveSelectTable(stmt *parser.SelectStmt) (string, *schema.Table, error) {
	tableName, err := selectFromTableName(stmt)
	if err != nil {
		return "", nil, err
	}

	table, ok := s.conn.schema.GetTable(tableName)
	if !ok {
		return "", nil, fmt.Errorf("table not found: %s", tableName)
	}

	return tableName, table, nil
}

// routeSpecializedSelect routes to JOIN, aggregate, or window function SELECT compilers.
func (s *Stmt) routeSpecializedSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {

	// Handle JOINs (explicit JOIN or implicit cross join via comma-separated tables)
	if stmt.From != nil && (len(stmt.From.Joins) > 0 || len(stmt.From.Tables) > 1) {
		result, err := s.compileSelectWithJoins(vm, stmt, tableName, table, args)
		return result, err, true
	}

	// Handle window functions (check before aggregates since window functions take precedence)
	if s.detectWindowFunctions(stmt) {
		result, err := s.compileSelectWithWindowFunctions(vm, stmt, tableName, table, args)
		return result, err, true
	}

	// Handle aggregates
	if s.detectAggregates(stmt) {
		result, err := s.compileSelectWithAggregates(vm, stmt, tableName, table, args)
		return result, err, true
	}

	return nil, nil, false
}

// compileSimpleSelect compiles a simple single-table SELECT.
func (s *Stmt) compileSimpleSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	// Expand SELECT *
	expandedCols, colNames := expandStarColumns(stmt.Columns, table)
	numCols := len(expandedCols)

	// Setup VDBE and code generator
	gen, cursorNum := s.setupSimpleSelectVDBE(vm, stmt, tableName, table, numCols, colNames, args)

	// Handle ORDER BY
	if len(stmt.OrderBy) > 0 {
		return s.compileSelectWithOrderBy(vm, stmt, table, gen, numCols)
	}

	// Compile simple scan
	return s.emitSimpleSelectScan(vm, stmt, table, expandedCols, numCols, cursorNum, gen)
}

// setupSimpleSelectVDBE initializes VDBE and code generator for simple SELECT.
func (s *Stmt) setupSimpleSelectVDBE(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, numCols int, colNames []string,
	args []driver.NamedValue) (*expr.CodeGenerator, int) {

	vm.AllocMemory(numCols + 30)

	// Determine cursor number
	cursorNum := s.determineCursorNum(table, vm)

	// Setup code generator
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, cursorNum)
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	// Setup args
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	vm.ResultCols = colNames

	return gen, cursorNum
}

// determineCursorNum determines which cursor to use for a table.
func (s *Stmt) determineCursorNum(table *schema.Table, vm *vdbe.VDBE) int {
	if table.Temp {
		// Ephemeral tables use cursor stored in RootPage
		cursorNum := int(table.RootPage)
		vm.AllocCursors(cursorNum + 1)
		return cursorNum
	}
	// Regular tables use cursor 0
	vm.AllocCursors(1)
	return 0
}

// emitSimpleSelectScan emits bytecode for a simple table scan.
func (s *Stmt) emitSimpleSelectScan(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	table *schema.Table, expandedCols []parser.ResultColumn, numCols int,
	cursorNum int, gen *expr.CodeGenerator) (*vdbe.VDBE, error) {

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open cursor for regular tables
	if !table.Temp {
		vm.AddOp(vdbe.OpOpenRead, cursorNum, int(table.RootPage), len(table.Columns))
	}

	rewindAddr := vm.AddOp(vdbe.OpRewind, cursorNum, 0, 0)

	// WHERE clause
	skipAddr := s.emitSimpleSelectWhere(vm, stmt, gen)

	// SELECT columns
	for i, col := range expandedCols {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return nil, err
		}
	}

	// Handle DISTINCT - check if row is unique before outputting
	var distinctSkipAddr int
	if stmt.Distinct {
		// OpDistinctRow: P1=first reg, P2=jump if not distinct, P3=num cols
		distinctSkipAddr = vm.AddOp(vdbe.OpDistinctRow, 0, 0, numCols)
	}

	// Output row
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix DISTINCT skip - jump to after ResultRow (to OpNext)
	if stmt.Distinct {
		vm.Program[distinctSkipAddr].P2 = vm.NumOps()
	}

	// Fix WHERE skip
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Loop
	vm.AddOp(vdbe.OpNext, cursorNum, rewindAddr+1, 0)

	// Close regular tables
	if !table.Temp {
		vm.AddOp(vdbe.OpClose, cursorNum, 0, 0)
	}

	// Halt
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// emitSimpleSelectWhere emits WHERE clause for simple SELECT.
func (s *Stmt) emitSimpleSelectWhere(vm *vdbe.VDBE, stmt *parser.SelectStmt,
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

// compileSelectWithoutFrom handles SELECT statements without a FROM clause.
// This is used for queries like SELECT 1, SELECT 1+1, or recursive CTE anchors.
func (s *Stmt) compileSelectWithoutFrom(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Allocate memory for result columns
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 10)

	// Create expression code generator (no table context needed)
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)

	// Set up args for parameter binding
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	// Build result column names
	colNames := make([]string, numCols)
	for i, col := range stmt.Columns {
		colNames[i] = selectColName(col, i)
	}
	vm.ResultCols = colNames

	// Initialize VM
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Handle WHERE clause if present
	// For SELECT without FROM, we evaluate the WHERE once and skip result if false
	addrHalt := -1
	if stmt.Where != nil {
		// Evaluate WHERE condition
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to generate WHERE condition: %w", err)
		}
		// If WHERE is false, jump to Halt (skip ResultRow)
		vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
		addrHalt = vm.NumOps() - 1
	}

	// Evaluate each column expression
	for i, col := range stmt.Columns {
		// Generate code for the expression
		reg, err := gen.GenerateExpr(col.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to generate expression for column %d: %w", i, err)
		}
		// Copy result to target register if needed
		if reg != i {
			vm.AddOp(vdbe.OpCopy, reg, i, 0)
		}
	}

	// Return single row with the computed values (only if WHERE passed)
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Halt instruction - patch WHERE jump to point here if condition was false
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	if addrHalt >= 0 {
		vm.Program[addrHalt].P2 = vm.NumOps() - 1
	}

	return vm, nil
}

// orderByColumnInfo holds information about ORDER BY columns
type orderByColumnInfo struct {
	keyCols      []int
	desc         []bool
	collations   []string
	extraCols    []string
	extraColRegs []int
	sorterCols   int
}

// limitOffsetInfo holds LIMIT/OFFSET state
type limitOffsetInfo struct {
	hasLimit  bool
	hasOffset bool
	limitVal  int
	offsetVal int
	limitReg  int
	offsetReg int
}

// setupLimitOffset parses LIMIT/OFFSET and initializes counter registers
func (s *Stmt) setupLimitOffset(vm *vdbe.VDBE, stmt *parser.SelectStmt, gen *expr.CodeGenerator) *limitOffsetInfo {
	info := &limitOffsetInfo{}

	if stmt.Limit != nil {
		if litExpr, ok := stmt.Limit.(*parser.LiteralExpr); ok {
			var parsedVal int64
			if _, err := fmt.Sscanf(litExpr.Value, "%d", &parsedVal); err == nil {
				info.hasLimit = true
				info.limitVal = int(parsedVal)
				info.limitReg = gen.AllocReg()
				vm.AddOp(vdbe.OpInteger, 0, info.limitReg, 0)
			}
		}
	}

	if stmt.Offset != nil {
		if litExpr, ok := stmt.Offset.(*parser.LiteralExpr); ok {
			var parsedVal int64
			if _, err := fmt.Sscanf(litExpr.Value, "%d", &parsedVal); err == nil {
				info.hasOffset = true
				info.offsetVal = int(parsedVal)
				info.offsetReg = gen.AllocReg()
				vm.AddOp(vdbe.OpInteger, 0, info.offsetReg, 0)
			}
		}
	}

	return info
}

// emitLimitOffsetChecks emits VDBE opcodes to check LIMIT/OFFSET conditions
func (s *Stmt) emitLimitOffsetChecks(vm *vdbe.VDBE, info *limitOffsetInfo, gen *expr.CodeGenerator) (offsetSkipAddr int, limitJumpAddr int) {
	if info.hasOffset {
		// Increment offset counter
		vm.AddOp(vdbe.OpAddImm, info.offsetReg, 1, 0)
		// Compare counter to offset value
		offsetCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, info.offsetVal, offsetCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpLe, info.offsetReg, offsetCheckReg, cmpReg)
		offsetSkipAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)
	}

	if info.hasLimit {
		// Increment counter
		vm.AddOp(vdbe.OpAddImm, info.limitReg, 1, 0)
		// Compare counter to limit
		limitCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, info.limitVal, limitCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpGt, info.limitReg, limitCheckReg, cmpReg)
		limitJumpAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)
	}

	return offsetSkipAddr, limitJumpAddr
}

// resolveOrderByColumns determines which columns to sort by and identifies extra columns needed
func (s *Stmt) resolveOrderByColumns(stmt *parser.SelectStmt, table *schema.Table, numCols int, gen *expr.CodeGenerator) *orderByColumnInfo {
	info := &orderByColumnInfo{
		keyCols:      make([]int, len(stmt.OrderBy)),
		desc:         make([]bool, len(stmt.OrderBy)),
		collations:   make([]string, len(stmt.OrderBy)),
		extraCols:    make([]string, 0),
		extraColRegs: make([]int, 0),
	}

	for i, orderTerm := range stmt.OrderBy {
		s.resolveOrderByTerm(orderTerm, i, stmt, table, numCols, gen, info)
	}

	info.sorterCols = numCols + len(info.extraCols)
	return info
}

// resolveOrderByTerm resolves a single ORDER BY term.
func (s *Stmt) resolveOrderByTerm(orderTerm parser.OrderingTerm, termIdx int,
	stmt *parser.SelectStmt, table *schema.Table, numCols int,
	gen *expr.CodeGenerator, info *orderByColumnInfo) {

	// Extract base expression and collation
	baseExpr, collation := s.extractOrderByExpression(orderTerm, termIdx, info)

	// Try to find column in SELECT list
	orderColName, colIdx := s.findOrderByColumnInSelect(baseExpr, stmt)

	// Look up collation from schema if not explicitly specified
	if collation == "" && orderColName != "" {
		collation = s.findCollationInSchema(orderColName, table)
	}
	info.collations[termIdx] = collation

	// Handle column not in SELECT list
	if colIdx < 0 && orderColName != "" {
		colIdx = s.addExtraOrderByColumn(orderColName, numCols, gen, info)
	}

	// Default to first column if not found
	if colIdx < 0 {
		colIdx = 0
	}

	info.keyCols[termIdx] = colIdx
	info.desc[termIdx] = !orderTerm.Asc
}

// extractOrderByExpression extracts base expression and collation from ORDER BY term.
func (s *Stmt) extractOrderByExpression(orderTerm parser.OrderingTerm, termIdx int, info *orderByColumnInfo) (parser.Expression, string) {
	baseExpr := orderTerm.Expr
	collation := orderTerm.Collation

	if collateExpr, ok := orderTerm.Expr.(*parser.CollateExpr); ok {
		baseExpr = collateExpr.Expr
		collation = collateExpr.Collation
	}

	return baseExpr, collation
}

// findOrderByColumnInSelect searches for ORDER BY column in SELECT columns.
func (s *Stmt) findOrderByColumnInSelect(baseExpr parser.Expression, stmt *parser.SelectStmt) (string, int) {
	ident, ok := baseExpr.(*parser.IdentExpr)
	if !ok {
		return "", -1
	}

	orderColName := ident.Name

	// Search by alias or column name
	for j, selCol := range stmt.Columns {
		if selCol.Alias == orderColName {
			return orderColName, j
		}
		if selColIdent, ok := selCol.Expr.(*parser.IdentExpr); ok {
			if selColIdent.Name == orderColName {
				return orderColName, j
			}
		}
	}

	return orderColName, -1
}

// findCollationInSchema looks up collation from table schema.
func (s *Stmt) findCollationInSchema(colName string, table *schema.Table) string {
	for _, col := range table.Columns {
		if col.Name == colName {
			return col.Collation
		}
	}
	return ""
}

// addExtraOrderByColumn adds an extra column for ORDER BY that's not in SELECT.
func (s *Stmt) addExtraOrderByColumn(orderColName string, numCols int, gen *expr.CodeGenerator, info *orderByColumnInfo) int {
	// Check if already added
	for j, extraCol := range info.extraCols {
		if extraCol == orderColName {
			return numCols + j
		}
	}

	// Add new extra column
	colIdx := numCols + len(info.extraCols)
	info.extraCols = append(info.extraCols, orderColName)
	info.extraColRegs = append(info.extraColRegs, gen.AllocReg())
	return colIdx
}

// compileSelectWithOrderBy handles SELECT with ORDER BY clause using a sorter.
func (s *Stmt) compileSelectWithOrderBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, gen *expr.CodeGenerator, numCols int) (*vdbe.VDBE, error) {
	// Resolve ORDER BY columns and setup sorter
	orderInfo := s.resolveOrderByColumns(stmt, table, numCols, gen)
	gen.SetNextReg(orderInfo.sorterCols)
	keyInfo := s.createSorterKeyInfo(orderInfo)

	// Emit table scan and sorter population
	rewindAddr, skipAddr := s.emitOrderByScanSetup(vm, stmt, table, keyInfo, orderInfo.sorterCols, gen)
	if err := s.emitOrderBySorterPopulation(vm, stmt, table, orderInfo, numCols, gen); err != nil {
		return nil, err
	}
	s.fixOrderByScanAddresses(vm, stmt, rewindAddr, skipAddr)

	// Emit sorter output loop
	sorterSortAddr, limitInfo := s.emitOrderBySorterSort(vm, stmt, gen)
	sorterNextAddr, haltJumpAddr, sorterLoopAddr := s.emitOrderByOutputSetup(vm)
	offsetSkipAddr, limitJumpAddr, nextRowAddr := s.emitOrderByOutputLoop(vm, stmt, numCols, limitInfo, gen, sorterLoopAddr)
	haltAddr := s.emitOrderByCleanup(vm)

	// Fix all addresses
	s.fixOrderByAddresses(vm, rewindAddr, sorterSortAddr, sorterNextAddr, haltJumpAddr,
		offsetSkipAddr, limitJumpAddr, nextRowAddr, haltAddr, limitInfo, sorterLoopAddr)

	return vm, nil
}

// createSorterKeyInfo creates sorter key information from ORDER BY info.
func (s *Stmt) createSorterKeyInfo(orderInfo *orderByColumnInfo) *vdbe.SorterKeyInfo {
	return &vdbe.SorterKeyInfo{
		KeyCols:    orderInfo.keyCols,
		Desc:       orderInfo.desc,
		Collations: orderInfo.collations,
	}
}

// emitOrderByScanSetup emits initialization, table open, and sorter open operations.
func (s *Stmt) emitOrderByScanSetup(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, keyInfo *vdbe.SorterKeyInfo, sorterCols int, gen *expr.CodeGenerator) (int, int) {
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))

	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 0, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	var skipAddr int
	if stmt.Where != nil {
		whereReg, _ := gen.GenerateExpr(stmt.Where)
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	return rewindAddr, skipAddr
}

// emitOrderBySorterPopulation reads columns and populates the sorter.
func (s *Stmt) emitOrderBySorterPopulation(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, orderInfo *orderByColumnInfo, numCols int, gen *expr.CodeGenerator) error {
	// Read SELECT columns
	for i, col := range stmt.Columns {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return err
		}
	}

	// Read extra ORDER BY columns
	for i, colName := range orderInfo.extraCols {
		s.emitExtraOrderByColumn(vm, table, colName, orderInfo.extraColRegs[i])
	}

	// Copy extra columns to contiguous registers and insert
	for i := range orderInfo.extraCols {
		vm.AddOp(vdbe.OpCopy, orderInfo.extraColRegs[i], numCols+i, 0)
	}
	vm.AddOp(vdbe.OpSorterInsert, 0, 0, orderInfo.sorterCols)

	return nil
}

// emitExtraOrderByColumn emits code to read an extra ORDER BY column.
func (s *Stmt) emitExtraOrderByColumn(vm *vdbe.VDBE, table *schema.Table, colName string, targetReg int) {
	tableColIdx := table.GetColumnIndex(colName)
	if tableColIdx >= 0 {
		// Check if this is a rowid column (INTEGER PRIMARY KEY)
		if schemaColIsRowid(table.Columns[tableColIdx]) {
			vm.AddOp(vdbe.OpRowid, 0, targetReg, 0)
		} else {
			recordIdx := schemaRecordIdx(table.Columns, tableColIdx)
			vm.AddOp(vdbe.OpColumn, 0, recordIdx, targetReg)
		}
	} else {
		vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
	}
}

// fixOrderByScanAddresses fixes addresses for the table scan loop.
func (s *Stmt) fixOrderByScanAddresses(vm *vdbe.VDBE, stmt *parser.SelectStmt, rewindAddr int, skipAddr int) {
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
}

// emitOrderBySorterSort emits sorter sort operation and sets up LIMIT/OFFSET.
func (s *Stmt) emitOrderBySorterSort(vm *vdbe.VDBE, stmt *parser.SelectStmt, gen *expr.CodeGenerator) (int, *limitOffsetInfo) {
	sorterSortAddr := vm.AddOp(vdbe.OpSorterSort, 0, 0, 0)
	limitInfo := s.setupLimitOffset(vm, stmt, gen)
	return sorterSortAddr, limitInfo
}

// emitOrderByOutputSetup sets up the output loop structure.
func (s *Stmt) emitOrderByOutputSetup(vm *vdbe.VDBE) (int, int, int) {
	sorterNextAddr := vm.AddOp(vdbe.OpSorterNext, 0, 0, 0)
	haltJumpAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	sorterLoopAddr := vm.NumOps()
	return sorterNextAddr, haltJumpAddr, sorterLoopAddr
}

// emitOrderByOutputLoop emits the sorter output loop with LIMIT/OFFSET checks.
func (s *Stmt) emitOrderByOutputLoop(vm *vdbe.VDBE, stmt *parser.SelectStmt, numCols int, limitInfo *limitOffsetInfo, gen *expr.CodeGenerator, sorterLoopAddr int) (int, int, int) {
	vm.AddOp(vdbe.OpSorterData, 0, 0, numCols)
	offsetSkipAddr, limitJumpAddr := s.emitLimitOffsetChecks(vm, limitInfo, gen)

	// Handle DISTINCT - check if row is unique before outputting
	var distinctSkipAddr int
	if stmt.Distinct {
		distinctSkipAddr = vm.AddOp(vdbe.OpDistinctRow, 0, 0, numCols)
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix DISTINCT skip - jump to SorterNext
	nextRowAddr := vm.AddOp(vdbe.OpSorterNext, 0, sorterLoopAddr, 0)
	if stmt.Distinct {
		vm.Program[distinctSkipAddr].P2 = nextRowAddr
	}

	return offsetSkipAddr, limitJumpAddr, nextRowAddr
}

// emitOrderByCleanup emits cleanup operations.
func (s *Stmt) emitOrderByCleanup(vm *vdbe.VDBE) int {
	haltAddr := vm.NumOps()
	vm.AddOp(vdbe.OpSorterClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return haltAddr
}

// fixOrderByAddresses fixes all forward references in the ORDER BY bytecode.
func (s *Stmt) fixOrderByAddresses(vm *vdbe.VDBE, rewindAddr, sorterSortAddr, sorterNextAddr, haltJumpAddr,
	offsetSkipAddr, limitJumpAddr, nextRowAddr, haltAddr int, limitInfo *limitOffsetInfo, sorterLoopAddr int) {
	vm.Program[rewindAddr].P2 = haltAddr
	vm.Program[sorterSortAddr].P2 = haltAddr
	vm.Program[sorterNextAddr].P2 = sorterLoopAddr
	vm.Program[haltJumpAddr].P2 = haltAddr
	if limitInfo.hasOffset {
		vm.Program[offsetSkipAddr].P2 = nextRowAddr
	}
	if limitInfo.hasLimit {
		vm.Program[limitJumpAddr].P2 = haltAddr
	}
}

// detectWindowFunctions checks if a SELECT statement contains window functions
func (s *Stmt) detectWindowFunctions(stmt *parser.SelectStmt) bool {
	for _, col := range stmt.Columns {
		if s.isWindowFunctionExpr(col.Expr) {
			return true
		}
	}
	return false
}

// isWindowFunctionExpr checks if an expression is a window function (has OVER clause)
func (s *Stmt) isWindowFunctionExpr(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	fnExpr, ok := expr.(*parser.FunctionExpr)
	if !ok {
		return false
	}

	// A window function is identified by the presence of an OVER clause
	return fnExpr.Over != nil
}

// detectAggregates checks if a SELECT statement contains aggregate functions
func (s *Stmt) detectAggregates(stmt *parser.SelectStmt) bool {
	// Check for GROUP BY clause
	if len(stmt.GroupBy) > 0 {
		return true
	}

	for _, col := range stmt.Columns {
		if s.containsAggregate(col.Expr) {
			return true
		}
	}
	return false
}

// isAggregateExpr checks if an expression is or contains an aggregate function
func (s *Stmt) isAggregateExpr(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	fnExpr, ok := expr.(*parser.FunctionExpr)
	if !ok {
		return false
	}

	// Check if this is a known aggregate function
	aggFuncs := map[string]bool{
		"COUNT": true, "SUM": true, "AVG": true,
		"MIN": true, "MAX": true, "TOTAL": true,
		"GROUP_CONCAT": true,
	}

	return aggFuncs[fnExpr.Name]
}

// containsAggregate recursively checks if an expression tree contains any aggregate functions
func (s *Stmt) containsAggregate(expr parser.Expression) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *parser.FunctionExpr:
		return s.checkFunctionAggregate(e)
	case *parser.BinaryExpr:
		return s.containsAggregate(e.Left) || s.containsAggregate(e.Right)
	case *parser.UnaryExpr:
		return s.containsAggregate(e.Expr)
	case *parser.ParenExpr:
		return s.containsAggregate(e.Expr)
	case *parser.CaseExpr:
		return s.checkCaseAggregate(e)
	case *parser.CastExpr:
		return s.containsAggregate(e.Expr)
	case *parser.CollateExpr:
		return s.containsAggregate(e.Expr)
	}
	return false
}

// checkFunctionAggregate checks if a function expression contains aggregates.
func (s *Stmt) checkFunctionAggregate(e *parser.FunctionExpr) bool {
	if s.isAggregateExpr(e) {
		return true
	}
	for _, arg := range e.Args {
		if s.containsAggregate(arg) {
			return true
		}
	}
	return false
}

// checkCaseAggregate checks if a CASE expression contains aggregates.
func (s *Stmt) checkCaseAggregate(e *parser.CaseExpr) bool {
	for _, when := range e.WhenClauses {
		if s.containsAggregate(when.Condition) || s.containsAggregate(when.Result) {
			return true
		}
	}
	return s.containsAggregate(e.ElseClause)
}
