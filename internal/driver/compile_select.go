// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileSelect compiles a SELECT statement into VDBE bytecode.
func (s *Stmt) compileSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	// Handle special SELECT types
	if specialVM, err, handled := s.handleSpecialSelectTypes(vm, stmt, args); handled {
		return specialVM, err
	}

	// Get table and check for special cases
	tableName, table, db, err := s.resolveSelectTable(stmt)
	if err != nil {
		return nil, err
	}

	// Ensure VDBE context uses the correct database (main or attached)
	s.setVdbeContextForDatabase(vm, db)

	// Route virtual tables through vtab interface
	if s.isVirtualTable(table) {
		return s.compileVTabSelect(vm, stmt, table, args)
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

	// Handle FROM subqueries (but NOT join-only subqueries — those are handled in compileSelectWithJoins)
	if s.hasFromTableSubqueries(stmt) {
		result, err := s.compileSelectWithFromSubqueries(vm, stmt, args)
		return result, err, true
	}

	// Handle pragma table-valued functions (e.g., pragma_table_info)
	if s.isPragmaTVF(stmt) {
		result, err := s.compileSelectWithPragmaTVF(vm, stmt, args)
		return result, err, true
	}

	// Handle table-valued functions (standalone or correlated cross-join)
	if result, err, handled := s.handleTVFSelect(vm, stmt, args); handled {
		return result, err, true
	}

	// Handle SELECT without FROM
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		result, err := s.compileSelectWithoutFrom(vm, stmt, args)
		return result, err, true
	}

	return nil, nil, false
}

// handleTVFSelect handles standalone TVFs and correlated TVF cross-joins.
func (s *Stmt) handleTVFSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	if s.hasTableValuedFunction(stmt) {
		if s.detectAggregates(stmt) {
			if err := s.materializeTVFAsEphemeral(vm, stmt, args); err != nil {
				return nil, err, true
			}
			return nil, nil, false
		}
		result, err := s.compileSelectWithTVF(vm, stmt, args)
		return result, err, true
	}
	if s.hasCorrelatedTVF(stmt) {
		result, err := s.compileCorrelatedTVFJoin(vm, stmt, args)
		return result, err, true
	}
	return nil, nil, false
}

// resolveSelectTable gets the table name and schema for the SELECT.
func (s *Stmt) resolveSelectTable(stmt *parser.SelectStmt) (string, *schema.Table, *schema.Database, error) {
	tableRef, err := selectFromTableRef(stmt)
	if err != nil {
		return "", nil, nil, err
	}

	table, db, _, ok := s.conn.dbRegistry.ResolveTable(tableRef.Schema, tableRef.TableName)
	if !ok {
		if tableRef.Schema != "" {
			return "", nil, nil, fmt.Errorf("table not found: %s.%s", tableRef.Schema, tableRef.TableName)
		}
		return "", nil, nil, fmt.Errorf("table not found: %s", tableRef.TableName)
	}

	return tableRef.TableName, table, db, nil
}

// routeSpecializedSelect routes to JOIN, aggregate, or window function SELECT compilers.
func (s *Stmt) routeSpecializedSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {

	// Resolve named WINDOW clause references before routing
	resolveNamedWindows(stmt)

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

	// Replace stmt.Columns with expanded columns so that ORDER BY and other
	// downstream consumers see individual column references instead of *.
	stmt.Columns = expandedCols

	// Setup VDBE and code generator
	gen, cursorNum := s.setupSimpleSelectVDBE(vm, stmt, tableName, table, numCols, colNames, args)

	// Handle ORDER BY
	if len(stmt.OrderBy) > 0 {
		return s.compileSelectWithOrderBy(vm, stmt, table, gen, numCols, cursorNum)
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

	// Register alias so correlated subqueries can reference outer columns
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		alias := stmt.From.Tables[0].Alias
		if alias != "" && alias != tableName {
			gen.RegisterCursor(alias, cursorNum)
			aliasInfo := buildTableInfo(tableName, table)
			aliasInfo.Name = alias
			gen.RegisterTable(aliasInfo)
		}
	}

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

	// Setup LIMIT/OFFSET counters before the scan loop
	limitInfo := s.setupLimitOffset(vm, stmt, gen)

	// Open cursor and start scan loop
	rewindAddr := s.emitScanLoopSetup(vm, table, cursorNum)

	// WHERE clause
	skipAddr := s.emitSimpleSelectWhere(vm, stmt, gen)

	// OFFSET check before output (skip early rows)
	offsetSkipAddr := s.emitOffsetCheck(vm, limitInfo, gen)

	// SELECT columns
	if err := s.emitScanColumns(vm, table, expandedCols, gen); err != nil {
		return nil, err
	}

	// Handle DISTINCT and output row
	distinctSkipAddr := s.emitDistinctAndOutput(vm, stmt, numCols, gen)

	// LIMIT check after output (stop after N rows)
	limitJumpAddr := s.emitLimitCheck(vm, limitInfo, gen)

	// Fix addresses and close loop
	s.fixScanAddressesWithLimit(vm, stmt, rewindAddr, skipAddr, distinctSkipAddr,
		offsetSkipAddr, limitJumpAddr, limitInfo, cursorNum, table)

	return vm, nil
}

// fixScanAddressesWithLimit fixes jump addresses for scan with LIMIT/OFFSET support.
func (s *Stmt) fixScanAddressesWithLimit(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	rewindAddr, skipAddr, distinctSkipAddr, offsetSkipAddr, limitJumpAddr int,
	limitInfo *limitOffsetInfo, cursorNum int, table *schema.Table) {

	// Fix DISTINCT skip
	if stmt.Distinct {
		vm.Program[distinctSkipAddr].P2 = vm.NumOps()
	}

	// Fix WHERE skip
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Fix OFFSET skip to jump to Next
	nextAddr := vm.NumOps()
	if limitInfo.hasOffset {
		vm.Program[offsetSkipAddr].P2 = nextAddr
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

	// Fix LIMIT jump to halt
	if limitInfo.hasLimit {
		vm.Program[limitJumpAddr].P2 = haltAddr
	}

	// Fix early halt jump for LIMIT 0
	if limitInfo.earlyHaltAddr > 0 {
		vm.Program[limitInfo.earlyHaltAddr].P2 = haltAddr
	}
}

// emitScanLoopSetup opens cursor and starts the scan loop.
func (s *Stmt) emitScanLoopSetup(vm *vdbe.VDBE, table *schema.Table, cursorNum int) int {
	if !table.Temp {
		vm.AddOp(vdbe.OpOpenRead, cursorNum, int(table.RootPage), len(table.Columns))
	}
	return vm.AddOp(vdbe.OpRewind, cursorNum, 0, 0)
}

// emitScanColumns emits code to read all SELECT columns.
func (s *Stmt) emitScanColumns(vm *vdbe.VDBE, table *schema.Table, expandedCols []parser.ResultColumn, gen *expr.CodeGenerator) error {
	for i, col := range expandedCols {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return err
		}
	}
	return nil
}

// emitDistinctAndOutput handles DISTINCT check and result row emission.
func (s *Stmt) emitDistinctAndOutput(vm *vdbe.VDBE, stmt *parser.SelectStmt, numCols int, gen *expr.CodeGenerator) int {
	var distinctSkipAddr int
	if stmt.Distinct {
		distinctSkipAddr = vm.AddOp(vdbe.OpDistinctRow, 0, 0, numCols)
		if gen != nil {
			collations := make([]string, numCols)
			for i := 0; i < numCols; i++ {
				if coll, ok := gen.CollationForReg(i); ok {
					collations[i] = coll
				}
			}
			vm.Program[distinctSkipAddr].P4.P = collations
		}
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	return distinctSkipAddr
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
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 10)

	gen := s.setupNoFromCodeGenerator(vm, stmt, args, numCols)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Handle WHERE clause and get halt address for patching
	addrHalt := s.emitNoFromWhereClause(vm, stmt, gen)

	// Evaluate column expressions
	if err := s.emitNoFromColumns(vm, stmt, numCols, gen); err != nil {
		return nil, err
	}

	// Emit result row and halt
	s.finalizeNoFromSelect(vm, numCols, addrHalt)

	return vm, nil
}

// setupNoFromCodeGenerator initializes code generator for SELECT without FROM.
func (s *Stmt) setupNoFromCodeGenerator(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue, numCols int) *expr.CodeGenerator {
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)

	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	colNames := make([]string, numCols)
	for i, col := range stmt.Columns {
		colNames[i] = selectColName(col, i)
	}
	vm.ResultCols = colNames

	return gen
}

// emitNoFromWhereClause emits WHERE clause for SELECT without FROM and returns halt address.
func (s *Stmt) emitNoFromWhereClause(vm *vdbe.VDBE, stmt *parser.SelectStmt, gen *expr.CodeGenerator) int {
	if stmt.Where == nil {
		return -1
	}

	whereReg, err := gen.GenerateExpr(stmt.Where)
	if err != nil {
		return -1
	}

	vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	return vm.NumOps() - 1
}

// emitNoFromColumns evaluates and emits column expressions for SELECT without FROM.
func (s *Stmt) emitNoFromColumns(vm *vdbe.VDBE, stmt *parser.SelectStmt, numCols int, gen *expr.CodeGenerator) error {
	for i, col := range stmt.Columns {
		reg, err := gen.GenerateExpr(col.Expr)
		if err != nil {
			return fmt.Errorf("failed to generate expression for column %d: %w", i, err)
		}
		if reg != i {
			vm.AddOp(vdbe.OpCopy, reg, i, 0)
		}
	}
	return nil
}

// finalizeNoFromSelect emits result row, halt, and patches WHERE jump.
func (s *Stmt) finalizeNoFromSelect(vm *vdbe.VDBE, numCols int, addrHalt int) {
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	if addrHalt >= 0 {
		vm.Program[addrHalt].P2 = vm.NumOps() - 1
	}
}

// orderByColumnInfo holds information about ORDER BY columns
type orderByColumnInfo struct {
	keyCols      []int
	desc         []bool
	nullsFirst   []*bool
	collations   []string
	extraExprs   []parser.Expression
	extraColRegs []int
	sorterCols   int
}

// limitOffsetInfo holds LIMIT/OFFSET state
type limitOffsetInfo struct {
	hasLimit      bool
	hasOffset     bool
	limitVal      int
	offsetVal     int
	limitReg      int
	offsetReg     int
	earlyHaltAddr int // address of early Goto for LIMIT 0 (needs patching to halt)
}

// setupLimitOffset parses LIMIT/OFFSET and initializes counter registers.
// If LIMIT is 0, emits a Goto to a placeholder address (stored in earlyHaltAddr)
// that the caller must patch to point to Halt.
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
				// LIMIT 0 or negative: emit early jump to halt (patched later)
				if parsedVal <= 0 {
					info.earlyHaltAddr = vm.AddOp(vdbe.OpGoto, 0, 0, 0)
				}
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

// emitOffsetCheck emits VDBE opcodes to check OFFSET condition (skip early rows).
// Returns the address of the conditional jump that needs patching to the Next opcode.
func (s *Stmt) emitOffsetCheck(vm *vdbe.VDBE, info *limitOffsetInfo, gen *expr.CodeGenerator) int {
	if !info.hasOffset {
		return 0
	}
	// Increment offset counter
	vm.AddOp(vdbe.OpAddImm, info.offsetReg, 1, 0)
	// Compare counter to offset value: skip this row if counter <= offset
	offsetCheckReg := gen.AllocReg()
	vm.AddOp(vdbe.OpInteger, info.offsetVal, offsetCheckReg, 0)
	cmpReg := gen.AllocReg()
	vm.AddOp(vdbe.OpLe, info.offsetReg, offsetCheckReg, cmpReg)
	return vm.AddOp(vdbe.OpIf, cmpReg, 0, 0) // jump to Next (patched later)
}

// emitLimitCheck emits VDBE opcodes to check LIMIT condition (stop after N rows).
// Returns the address of the conditional jump that needs patching to the Halt opcode.
func (s *Stmt) emitLimitCheck(vm *vdbe.VDBE, info *limitOffsetInfo, gen *expr.CodeGenerator) int {
	if !info.hasLimit {
		return 0
	}
	// Increment counter
	vm.AddOp(vdbe.OpAddImm, info.limitReg, 1, 0)
	// Compare counter to limit: stop if counter >= limit
	limitCheckReg := gen.AllocReg()
	vm.AddOp(vdbe.OpInteger, info.limitVal, limitCheckReg, 0)
	cmpReg := gen.AllocReg()
	vm.AddOp(vdbe.OpGe, info.limitReg, limitCheckReg, cmpReg)
	return vm.AddOp(vdbe.OpIf, cmpReg, 0, 0) // jump to Halt (patched later)
}

// resolveOrderByColumns determines which columns to sort by and identifies extra columns needed
func (s *Stmt) resolveOrderByColumns(stmt *parser.SelectStmt, table *schema.Table, numCols int, gen *expr.CodeGenerator) *orderByColumnInfo {
	info := &orderByColumnInfo{
		keyCols:      make([]int, len(stmt.OrderBy)),
		desc:         make([]bool, len(stmt.OrderBy)),
		nullsFirst:   make([]*bool, len(stmt.OrderBy)),
		collations:   make([]string, len(stmt.OrderBy)),
		extraExprs:   make([]parser.Expression, 0),
		extraColRegs: make([]int, 0),
	}

	for i, orderTerm := range stmt.OrderBy {
		s.resolveOrderByTerm(orderTerm, i, stmt, table, numCols, gen, info)
	}

	info.sorterCols = numCols + len(info.extraExprs)
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

	// Look up collation from schema or expression if not explicitly specified
	if collation == "" {
		if orderColName != "" {
			collation = s.findCollationInSchema(orderColName, table)
		} else {
			collation = resolveExprCollation(baseExpr, table)
		}
	}
	info.collations[termIdx] = collation

	// Handle column not in SELECT list
	if colIdx < 0 {
		colIdx = s.addExtraOrderByExpr(baseExpr, numCols, gen, info)
	}

	// Default to first column if not found
	if colIdx < 0 {
		colIdx = 0
	}

	info.keyCols[termIdx] = colIdx
	info.desc[termIdx] = !orderTerm.Asc
	info.nullsFirst[termIdx] = orderTerm.NullsFirst
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
	// Check if it's a column number (literal integer)
	if colIdx := s.tryParseColumnNumber(baseExpr, stmt); colIdx >= 0 {
		return "", colIdx
	}

	// Check if it's a column name
	ident, ok := baseExpr.(*parser.IdentExpr)
	if !ok {
		return "", -1
	}

	// Search by alias or column name
	orderColName := ident.Name
	colIdx := s.searchColumnByName(orderColName, stmt.Columns)
	return orderColName, colIdx
}

// tryParseColumnNumber attempts to parse a column number from a literal expression.
func (s *Stmt) tryParseColumnNumber(baseExpr parser.Expression, stmt *parser.SelectStmt) int {
	litExpr, ok := baseExpr.(*parser.LiteralExpr)
	if !ok {
		return -1
	}

	var colNum int
	if _, err := fmt.Sscanf(litExpr.Value, "%d", &colNum); err != nil {
		return -1
	}

	// Column numbers are 1-indexed in SQL
	if colNum >= 1 && colNum <= len(stmt.Columns) {
		return colNum - 1
	}
	return -1
}

// searchColumnByName searches for a column by alias or name in SELECT columns.
func (s *Stmt) searchColumnByName(orderColName string, columns []parser.ResultColumn) int {
	for j, selCol := range columns {
		if selCol.Alias == orderColName {
			return j
		}
		if selColIdent, ok := selCol.Expr.(*parser.IdentExpr); ok {
			if selColIdent.Name == orderColName {
				return j
			}
		}
	}
	return -1
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
func (s *Stmt) addExtraOrderByExpr(orderExpr parser.Expression, numCols int, gen *expr.CodeGenerator, info *orderByColumnInfo) int {
	if ident, ok := orderExpr.(*parser.IdentExpr); ok {
		for j, extraExpr := range info.extraExprs {
			if extraIdent, ok := extraExpr.(*parser.IdentExpr); ok {
				if strings.EqualFold(extraIdent.Name, ident.Name) {
					return numCols + j
				}
			}
		}
	}

	colIdx := numCols + len(info.extraExprs)
	info.extraExprs = append(info.extraExprs, orderExpr)
	info.extraColRegs = append(info.extraColRegs, 0)
	return colIdx
}

// compileSelectWithOrderBy handles SELECT with ORDER BY clause using a sorter.
func (s *Stmt) compileSelectWithOrderBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, gen *expr.CodeGenerator, numCols int, cursorNum int) (*vdbe.VDBE, error) {
	// Resolve ORDER BY columns and setup sorter
	orderInfo := s.resolveOrderByColumns(stmt, table, numCols, gen)
	gen.SetNextReg(orderInfo.sorterCols)
	keyInfo := s.createSorterKeyInfo(orderInfo)

	// Emit table scan and sorter population
	rewindAddr, skipAddr := s.emitOrderByScanSetup(vm, stmt, table, keyInfo, orderInfo.sorterCols, gen, cursorNum)
	if err := s.emitOrderBySorterPopulation(vm, stmt, table, orderInfo, numCols, gen, cursorNum); err != nil {
		return nil, err
	}
	s.fixOrderByScanAddresses(vm, stmt, table, rewindAddr, skipAddr, cursorNum)

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
		NullsFirst: orderInfo.nullsFirst,
		Collations: orderInfo.collations,
	}
}

// emitOrderByScanSetup emits initialization, table open, and sorter open operations.
func (s *Stmt) emitOrderByScanSetup(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, keyInfo *vdbe.SorterKeyInfo, sorterCols int, gen *expr.CodeGenerator, cursorNum int) (int, int) {
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	if !table.Temp {
		vm.AddOp(vdbe.OpOpenRead, cursorNum, int(table.RootPage), len(table.Columns))
	}

	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 0, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	rewindAddr := vm.AddOp(vdbe.OpRewind, cursorNum, 0, 0)

	var skipAddr int
	if stmt.Where != nil {
		whereReg, _ := gen.GenerateExpr(stmt.Where)
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	return rewindAddr, skipAddr
}

// emitOrderBySorterPopulation reads columns and populates the sorter.
func (s *Stmt) emitOrderBySorterPopulation(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, orderInfo *orderByColumnInfo, numCols int, gen *expr.CodeGenerator, cursorNum int) error {
	// Read SELECT columns
	for i, col := range stmt.Columns {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return err
		}
	}

	// Read extra ORDER BY columns
	for i, orderExpr := range orderInfo.extraExprs {
		reg, err := gen.GenerateExpr(orderExpr)
		if err != nil {
			return err
		}
		orderInfo.extraColRegs[i] = reg
	}

	// Copy extra columns to contiguous registers and insert
	for i := range orderInfo.extraExprs {
		vm.AddOp(vdbe.OpCopy, orderInfo.extraColRegs[i], numCols+i, 0)
	}
	vm.AddOp(vdbe.OpSorterInsert, 0, 0, orderInfo.sorterCols)

	return nil
}

// emitExtraOrderByColumn emits code to read an extra ORDER BY column.
func (s *Stmt) emitExtraOrderByColumn(vm *vdbe.VDBE, table *schema.Table, colName string, targetReg int, cursorNum int) {
	tableColIdx := table.GetColumnIndexWithRowidAliases(colName)
	if tableColIdx >= 0 {
		// Check if this is a rowid column (INTEGER PRIMARY KEY)
		if schemaColIsRowidForTable(table, table.Columns[tableColIdx]) {
			vm.AddOp(vdbe.OpRowid, cursorNum, targetReg, 0)
		} else {
			recordIdx := schemaRecordIdxForTable(table, tableColIdx)
			vm.AddOp(vdbe.OpColumn, cursorNum, recordIdx, targetReg)
		}
	} else if tableColIdx == -2 && !table.WithoutRowID {
		// This is a rowid alias but no INTEGER PRIMARY KEY exists
		// (not applicable for WITHOUT ROWID tables)
		vm.AddOp(vdbe.OpRowid, cursorNum, targetReg, 0)
	} else {
		vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
	}
}

// fixOrderByScanAddresses fixes addresses for the table scan loop.
func (s *Stmt) fixOrderByScanAddresses(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, rewindAddr int, skipAddr int, cursorNum int) {
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}
	vm.AddOp(vdbe.OpNext, cursorNum, rewindAddr+1, 0)
	if !table.Temp {
		vm.AddOp(vdbe.OpClose, cursorNum, 0, 0)
	}
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
// OFFSET is checked before ResultRow (to skip rows), LIMIT after (to stop after N rows).
func (s *Stmt) emitOrderByOutputLoop(vm *vdbe.VDBE, stmt *parser.SelectStmt, numCols int, limitInfo *limitOffsetInfo, gen *expr.CodeGenerator, sorterLoopAddr int) (int, int, int) {
	vm.AddOp(vdbe.OpSorterData, 0, 0, numCols)

	// OFFSET check before ResultRow (skip early rows)
	var offsetSkipAddr int
	if limitInfo.hasOffset {
		vm.AddOp(vdbe.OpAddImm, limitInfo.offsetReg, 1, 0)
		offsetCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, limitInfo.offsetVal, offsetCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpLe, limitInfo.offsetReg, offsetCheckReg, cmpReg)
		offsetSkipAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)
	}

	// Handle DISTINCT - check if row is unique before outputting
	var distinctSkipAddr int
	if stmt.Distinct {
		distinctSkipAddr = vm.AddOp(vdbe.OpDistinctRow, 0, 0, numCols)
		if gen != nil {
			collations := make([]string, numCols)
			for i := 0; i < numCols; i++ {
				if coll, ok := gen.CollationForReg(i); ok {
					collations[i] = coll
				}
			}
			vm.Program[distinctSkipAddr].P4.P = collations
		}
	}

	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// LIMIT check after ResultRow (stop after N rows emitted)
	var limitJumpAddr int
	if limitInfo.hasLimit {
		vm.AddOp(vdbe.OpAddImm, limitInfo.limitReg, 1, 0)
		limitCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, limitInfo.limitVal, limitCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpGe, limitInfo.limitReg, limitCheckReg, cmpReg)
		limitJumpAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)
	}

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
	if limitInfo.earlyHaltAddr > 0 {
		vm.Program[limitInfo.earlyHaltAddr].P2 = haltAddr
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

// isWindowFunctionExpr checks if an expression contains a window function (has OVER clause)
func (s *Stmt) isWindowFunctionExpr(e parser.Expression) bool {
	if e == nil {
		return false
	}

	fnExpr, ok := e.(*parser.FunctionExpr)
	if !ok {
		return s.exprContainsWindowFunc(e)
	}

	if fnExpr.Over != nil {
		return true
	}
	// Check arguments for nested window functions (e.g., COALESCE(NTH_VALUE(...) OVER ...))
	for _, arg := range fnExpr.Args {
		if s.isWindowFunctionExpr(arg) {
			return true
		}
	}
	return false
}

// exprContainsWindowFunc checks non-function expressions for nested window functions.
func (s *Stmt) exprContainsWindowFunc(e parser.Expression) bool {
	switch ex := e.(type) {
	case *parser.BinaryExpr:
		return s.isWindowFunctionExpr(ex.Left) || s.isWindowFunctionExpr(ex.Right)
	case *parser.UnaryExpr:
		return s.isWindowFunctionExpr(ex.Expr)
	case *parser.ParenExpr:
		return s.isWindowFunctionExpr(ex.Expr)
	case *parser.CastExpr:
		return s.isWindowFunctionExpr(ex.Expr)
	case *parser.CaseExpr:
		return s.caseExprContainsWindowFunc(ex)
	}
	return false
}

// caseExprContainsWindowFunc checks CASE expression clauses for window functions.
func (s *Stmt) caseExprContainsWindowFunc(ex *parser.CaseExpr) bool {
	for _, w := range ex.WhenClauses {
		if s.isWindowFunctionExpr(w.Condition) || s.isWindowFunctionExpr(w.Result) {
			return true
		}
	}
	return s.isWindowFunctionExpr(ex.ElseClause)
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
		"GROUP_CONCAT":     true,
		"JSON_GROUP_ARRAY": true, "JSON_GROUP_OBJECT": true,
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
	case *parser.CaseExpr:
		return s.checkCaseAggregate(e)
	default:
		// Handle simple wrapper expressions with single child
		return s.checkWrappedAggregate(e)
	}
}

// checkWrappedAggregate handles expression types that simply wrap a single child expression.
func (s *Stmt) checkWrappedAggregate(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.UnaryExpr:
		return s.containsAggregate(e.Expr)
	case *parser.ParenExpr:
		return s.containsAggregate(e.Expr)
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
