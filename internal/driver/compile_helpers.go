// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/engine"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// JOIN Compilation Helpers
// ============================================================================

// stmtTableInfo holds information about a table in a query.
type stmtTableInfo struct {
	name      string
	table     *schema.Table
	cursorIdx int
}

// compileSelectWithJoins compiles a SELECT statement with JOIN clauses
// and implicit cross joins (comma-separated tables in FROM).
func (s *Stmt) compileSelectWithJoins(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Collect all tables involved in the query
	tables, err := s.collectJoinTables(stmt, tableName, table)
	if err != nil {
		return nil, err
	}

	// Setup VDBE and code generator (with table info and args for WHERE)
	numCols, gen := s.setupJoinVDBE(vm, stmt, tables, args)

	// Handle ORDER BY - requires sorter
	if len(stmt.OrderBy) > 0 {
		return s.compileSelectWithJoinsAndOrderBy(vm, stmt, tables, numCols, gen)
	}

	// Emit scan preamble and open cursors
	rewindAddr := s.emitJoinScanSetup(vm, tables)
	loopStart := vm.NumOps()

	// Setup nested loops for joined tables
	innerLoopStarts := s.emitJoinNestedLoops(vm, tables)

	// Emit WHERE filter, column reads, and result
	if err := s.emitJoinColumnsWithWhere(vm, stmt, tables, numCols, gen); err != nil {
		return nil, err
	}

	// Emit loop cleanup
	s.emitJoinLoopCleanup(vm, tables, innerLoopStarts, loopStart, rewindAddr)

	return vm, nil
}

// compileSelectWithJoinsAndOrderBy compiles a SELECT with JOINs and ORDER BY using a sorter.
func (s *Stmt) compileSelectWithJoinsAndOrderBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) (*vdbe.VDBE, error) {
	// Resolve ORDER BY columns and setup sorter
	orderInfo := s.resolveOrderByColumnsMultiTable(stmt, tables, numCols, gen)
	gen.SetNextReg(orderInfo.sorterCols)
	keyInfo := s.createSorterKeyInfo(orderInfo)

	// Emit initialization, open all cursors, and sorter
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	for _, tbl := range tables {
		vm.AddOp(vdbe.OpOpenRead, tbl.cursorIdx, int(tbl.table.RootPage), len(tbl.table.Columns))
	}
	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 0, orderInfo.sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	// Setup nested loops for joined tables
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
	loopStart := vm.NumOps()
	innerLoopStarts, innerRewindAddrs := s.emitJoinNestedLoopsWithRewinds(vm, tables)

	// Emit WHERE filter and populate sorter
	if err := s.emitJoinSorterPopulation(vm, stmt, tables, orderInfo, numCols, gen); err != nil {
		return nil, err
	}

	// Close all cursors after populating sorter
	for i := len(tables) - 1; i > 0; i-- {
		vm.AddOp(vdbe.OpNext, i, innerLoopStarts[i-1], 0)
		vm.AddOp(vdbe.OpClose, i, 0, 0)
	}
	outerNextAddr := vm.AddOp(vdbe.OpNext, 0, loopStart, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Fix inner loop Rewind addresses to jump to outer Next if empty
	for _, addr := range innerRewindAddrs {
		vm.Program[addr].P2 = outerNextAddr
	}

	// Emit sorter output loop
	sorterSortAddr, limitInfo := s.emitOrderBySorterSort(vm, stmt, gen)
	sorterNextAddr, haltJumpAddr, sorterLoopAddr := s.emitOrderByOutputSetup(vm)
	offsetSkipAddr, limitJumpAddr, nextRowAddr := s.emitOrderByOutputLoop(vm, stmt, numCols, limitInfo, gen, sorterLoopAddr)
	haltAddr := s.emitOrderByCleanup(vm)

	// Fix addresses
	vm.Program[rewindAddr].P2 = sorterSortAddr + 1
	s.fixOrderByAddresses(vm, rewindAddr, sorterSortAddr, sorterNextAddr, haltJumpAddr,
		offsetSkipAddr, limitJumpAddr, nextRowAddr, haltAddr, limitInfo, sorterLoopAddr)

	return vm, nil
}

// resolveOrderByColumnsMultiTable resolves ORDER BY columns for multi-table queries.
func (s *Stmt) resolveOrderByColumnsMultiTable(stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) *orderByColumnInfo {
	info := &orderByColumnInfo{
		keyCols:      make([]int, len(stmt.OrderBy)),
		desc:         make([]bool, len(stmt.OrderBy)),
		collations:   make([]string, len(stmt.OrderBy)),
		extraCols:    make([]string, 0),
		extraColRegs: make([]int, 0),
	}

	for i, orderTerm := range stmt.OrderBy {
		s.resolveOrderByTermMultiTable(orderTerm, i, stmt, tables, numCols, gen, info)
	}

	info.sorterCols = numCols + len(info.extraCols)
	return info
}

// resolveOrderByTermMultiTable resolves a single ORDER BY term for multi-table queries.
func (s *Stmt) resolveOrderByTermMultiTable(orderTerm parser.OrderingTerm, termIdx int,
	stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int,
	gen *expr.CodeGenerator, info *orderByColumnInfo) {

	// Extract base expression and collation
	baseExpr, collation := s.extractOrderByExpression(orderTerm, termIdx, info)

	// Try to find column in SELECT list
	orderColName, colIdx := s.findOrderByColumnInSelect(baseExpr, stmt)

	// Look up collation from schema if not explicitly specified
	if collation == "" && orderColName != "" {
		collation = s.findCollationInSchemaMultiTable(orderColName, tables)
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

// findCollationInSchemaMultiTable looks up collation from multi-table schema.
func (s *Stmt) findCollationInSchemaMultiTable(colName string, tables []stmtTableInfo) string {
	// Check each table for the column
	for _, tbl := range tables {
		colIdx := tbl.table.GetColumnIndexWithRowidAliases(colName)
		if colIdx >= 0 && colIdx < len(tbl.table.Columns) {
			return tbl.table.Columns[colIdx].Collation
		}
	}
	return ""
}

// emitJoinSorterPopulation emits WHERE filter and inserts joined rows into sorter.
func (s *Stmt) emitJoinSorterPopulation(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, orderInfo *orderByColumnInfo, numCols int, gen *expr.CodeGenerator) error {
	// Emit WHERE clause filter
	var skipAddr int
	hasWhere := stmt.Where != nil
	if hasWhere {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return fmt.Errorf("failed to compile WHERE clause: %w", err)
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Read SELECT columns
	for i, col := range stmt.Columns {
		if err := s.emitSelectColumnOpMultiTable(vm, tables, col, i, gen); err != nil {
			return err
		}
	}

	// Read extra ORDER BY columns
	for i, colName := range orderInfo.extraCols {
		s.emitExtraOrderByColumnMultiTable(vm, tables, colName, orderInfo.extraColRegs[i])
	}

	// Copy extra columns to contiguous registers and insert
	for i := range orderInfo.extraCols {
		vm.AddOp(vdbe.OpCopy, orderInfo.extraColRegs[i], numCols+i, 0)
	}
	vm.AddOp(vdbe.OpSorterInsert, 0, 0, orderInfo.sorterCols)

	// Fix WHERE skip target to jump past the sorter insert
	if hasWhere {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	return nil
}

// emitExtraOrderByColumnMultiTable emits code to read an extra ORDER BY column from multi-table context.
func (s *Stmt) emitExtraOrderByColumnMultiTable(vm *vdbe.VDBE, tables []stmtTableInfo, colName string, targetReg int) {
	// Try to find the column in any of the tables
	for _, tbl := range tables {
		tableColIdx := tbl.table.GetColumnIndexWithRowidAliases(colName)
		if tableColIdx >= 0 && tableColIdx < len(tbl.table.Columns) {
			// Check if this is a rowid column (INTEGER PRIMARY KEY)
			if schemaColIsRowid(tbl.table.Columns[tableColIdx]) {
				vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, targetReg, 0)
			} else {
				recordIdx := schemaRecordIdxForTable(tbl.table, tableColIdx)
				vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, recordIdx, targetReg)
			}
			return
		} else if tableColIdx == -2 {
			// This is a rowid alias but no INTEGER PRIMARY KEY exists
			vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, targetReg, 0)
			return
		}
	}
	// Column not found in any table, emit NULL
	vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
}

// collectJoinTables collects all tables involved in a JOIN query.
// This handles both explicit JOINs and implicit cross joins (comma-separated tables).
func (s *Stmt) collectJoinTables(stmt *parser.SelectStmt, tableName string, table *schema.Table) ([]stmtTableInfo, error) {
	tables := []stmtTableInfo{s.createBaseTableInfo(stmt, tableName, table)}
	cursorIdx := 1

	// Add comma-separated tables (implicit cross joins)
	newTables, newCursorIdx, err := s.collectCrossJoinTables(stmt, cursorIdx)
	if err != nil {
		return nil, err
	}
	tables = append(tables, newTables...)
	cursorIdx = newCursorIdx

	// Add explicit JOIN tables
	joinTables, err := s.collectExplicitJoinTables(stmt, cursorIdx)
	if err != nil {
		return nil, err
	}
	tables = append(tables, joinTables...)

	return tables, nil
}

// createBaseTableInfo creates the base table info with alias resolution.
func (s *Stmt) createBaseTableInfo(stmt *parser.SelectStmt, tableName string, table *schema.Table) stmtTableInfo {
	baseTableAlias := tableName
	if len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Alias != "" {
		baseTableAlias = stmt.From.Tables[0].Alias
	}
	return stmtTableInfo{name: baseTableAlias, table: table, cursorIdx: 0}
}

// collectCrossJoinTables collects comma-separated tables (implicit cross joins).
func (s *Stmt) collectCrossJoinTables(stmt *parser.SelectStmt, startCursorIdx int) ([]stmtTableInfo, int, error) {
	var tables []stmtTableInfo
	cursorIdx := startCursorIdx

	for i := 1; i < len(stmt.From.Tables); i++ {
		tableInfo, err := s.createTableInfoFromRef(stmt.From.Tables[i], cursorIdx)
		if err != nil {
			return nil, cursorIdx, err
		}
		tables = append(tables, tableInfo)
		cursorIdx++
	}

	return tables, cursorIdx, nil
}

// collectExplicitJoinTables collects tables from explicit JOIN clauses.
func (s *Stmt) collectExplicitJoinTables(stmt *parser.SelectStmt, startCursorIdx int) ([]stmtTableInfo, error) {
	var tables []stmtTableInfo
	cursorIdx := startCursorIdx

	for _, join := range stmt.From.Joins {
		tableInfo, err := s.createTableInfoFromRef(join.Table, cursorIdx)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableInfo)
		cursorIdx++
	}

	return tables, nil
}

// createTableInfoFromRef creates a stmtTableInfo from a table reference.
func (s *Stmt) createTableInfoFromRef(tableRef parser.TableOrSubquery, cursorIdx int) (stmtTableInfo, error) {
	table, ok := s.conn.schema.GetTable(tableRef.TableName)
	if !ok {
		return stmtTableInfo{}, fmt.Errorf("table not found: %s", tableRef.TableName)
	}

	tableAlias := tableRef.TableName
	if tableRef.Alias != "" {
		tableAlias = tableRef.Alias
	}

	return stmtTableInfo{
		name:      tableAlias,
		table:     table,
		cursorIdx: cursorIdx,
	}, nil
}

// setupJoinVDBE initializes VDBE and code generator for JOIN query.
func (s *Stmt) setupJoinVDBE(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, args []driver.NamedValue) (int, *expr.CodeGenerator) {
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 30)
	vm.AllocCursors(len(tables))

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	for _, tbl := range tables {
		gen.RegisterCursor(tbl.name, tbl.cursorIdx)
		tableInfo := buildTableInfo(tbl.name, tbl.table)
		gen.RegisterTable(tableInfo)
	}

	// Setup args for parameter binding
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	vm.ResultCols = s.buildMultiTableColumnNames(stmt.Columns, tables)

	return numCols, gen
}

// emitJoinScanSetup emits initialization and cursor open operations for JOIN.
func (s *Stmt) emitJoinScanSetup(vm *vdbe.VDBE, tables []stmtTableInfo) int {
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, tbl := range tables {
		vm.AddOp(vdbe.OpOpenRead, tbl.cursorIdx, int(tbl.table.RootPage), len(tbl.table.Columns))
	}

	return vm.AddOp(vdbe.OpRewind, 0, 0, 0)
}

// emitJoinNestedLoops sets up nested loops for joined tables.
func (s *Stmt) emitJoinNestedLoops(vm *vdbe.VDBE, tables []stmtTableInfo) []int {
	var innerLoopStarts []int
	for i := 1; i < len(tables); i++ {
		vm.AddOp(vdbe.OpRewind, i, 0, 0)
		innerLoopStarts = append(innerLoopStarts, vm.NumOps())
	}
	return innerLoopStarts
}

// emitJoinNestedLoopsWithRewinds sets up nested loops and returns both loop starts and rewind addresses.
func (s *Stmt) emitJoinNestedLoopsWithRewinds(vm *vdbe.VDBE, tables []stmtTableInfo) ([]int, []int) {
	var innerLoopStarts []int
	var innerRewindAddrs []int
	for i := 1; i < len(tables); i++ {
		rewindAddr := vm.AddOp(vdbe.OpRewind, i, 0, 0)
		innerRewindAddrs = append(innerRewindAddrs, rewindAddr)
		innerLoopStarts = append(innerLoopStarts, vm.NumOps())
	}
	return innerLoopStarts, innerRewindAddrs
}

// emitJoinColumnsWithWhere emits WHERE filter, column read operations, and result row for JOIN.
func (s *Stmt) emitJoinColumnsWithWhere(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) error {
	// Emit WHERE clause filter
	var skipAddr int
	hasWhere := stmt.Where != nil
	if hasWhere {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return fmt.Errorf("failed to compile WHERE clause: %w", err)
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	for i, col := range stmt.Columns {
		if err := s.emitSelectColumnOpMultiTable(vm, tables, col, i, gen); err != nil {
			return err
		}
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix WHERE skip target to jump past the result row
	if hasWhere {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	return nil
}

// emitJoinLoopCleanup emits Next and Close operations for all tables and fixes addresses.
func (s *Stmt) emitJoinLoopCleanup(vm *vdbe.VDBE, tables []stmtTableInfo, innerLoopStarts []int, loopStart int, rewindAddr int) {
	for i := len(tables) - 1; i > 0; i-- {
		vm.AddOp(vdbe.OpNext, i, innerLoopStarts[i-1], 0)
		vm.AddOp(vdbe.OpClose, i, 0, 0)
	}

	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr
}

// emitSelectColumnOpMultiTable emits the VDBE opcode(s) for reading a column across multiple tables.
func (s *Stmt) emitSelectColumnOpMultiTable(vm *vdbe.VDBE, tables []stmtTableInfo, col parser.ResultColumn, i int, gen *expr.CodeGenerator) error {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		return s.emitNonIdentifierColumn(vm, col, i, gen)
	}

	if ident.Table != "" {
		return s.emitQualifiedColumn(vm, tables, ident, i)
	}

	return s.emitUnqualifiedColumn(vm, tables, ident, i)
}

// emitNonIdentifierColumn handles non-identifier expressions in multi-table SELECT.
func (s *Stmt) emitNonIdentifierColumn(vm *vdbe.VDBE, col parser.ResultColumn, targetReg int, gen *expr.CodeGenerator) error {
	if gen == nil {
		vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
		return nil
	}

	reg, err := gen.GenerateExpr(col.Expr)
	if err != nil {
		vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
		return nil
	}

	if reg != targetReg {
		vm.AddOp(vdbe.OpCopy, reg, targetReg, 0)
	}
	return nil
}

// emitQualifiedColumn handles qualified column references (table.column) in multi-table SELECT.
func (s *Stmt) emitQualifiedColumn(vm *vdbe.VDBE, tables []stmtTableInfo, ident *parser.IdentExpr, targetReg int) error {
	for _, tbl := range tables {
		if tbl.name == ident.Table || tbl.table.Name == ident.Table {
			return s.emitColumnFromTable(vm, tbl, ident.Name, targetReg)
		}
	}
	return fmt.Errorf("table not found: %s", ident.Table)
}

// emitUnqualifiedColumn handles unqualified column references in multi-table SELECT.
func (s *Stmt) emitUnqualifiedColumn(vm *vdbe.VDBE, tables []stmtTableInfo, ident *parser.IdentExpr, targetReg int) error {
	for _, tbl := range tables {
		colIdx := tbl.table.GetColumnIndex(ident.Name)
		if colIdx >= 0 {
			return s.emitColumnFromTable(vm, tbl, ident.Name, targetReg)
		}
	}
	return fmt.Errorf("column not found: %s", ident.Name)
}

// emitColumnFromTable emits opcodes to read a specific column from a table.
func (s *Stmt) emitColumnFromTable(vm *vdbe.VDBE, tbl stmtTableInfo, colName string, targetReg int) error {
	colIdx := tbl.table.GetColumnIndexWithRowidAliases(colName)
	if colIdx == -1 {
		return fmt.Errorf("column not found: %s.%s", tbl.name, colName)
	}

	if colIdx == -2 {
		// This is a rowid alias but no INTEGER PRIMARY KEY exists
		vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, targetReg, 0)
		return nil
	}

	if schemaColIsRowid(tbl.table.Columns[colIdx]) {
		vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, targetReg, 0)
		return nil
	}

	vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, schemaRecordIdxForTable(tbl.table, colIdx), targetReg)
	return nil
}

// buildMultiTableColumnNames builds result column names for a SELECT with multiple tables.
func (s *Stmt) buildMultiTableColumnNames(cols []parser.ResultColumn, tables []stmtTableInfo) []string {
	var names []string
	for _, col := range cols {
		if col.Alias != "" {
			names = append(names, col.Alias)
		} else if col.Star {
			// SELECT * - add all columns from all tables
			for _, tbl := range tables {
				for _, schemaCol := range tbl.table.Columns {
					names = append(names, schemaCol.Name)
				}
			}
		} else if ident, ok := col.Expr.(*parser.IdentExpr); ok {
			names = append(names, ident.Name)
		} else {
			names = append(names, fmt.Sprintf("column%d", len(names)+1))
		}
	}
	return names
}

// ============================================================================
// EXPLAIN Compilation
// ============================================================================

// compileExplain compiles an EXPLAIN or EXPLAIN QUERY PLAN statement.
func (s *Stmt) compileExplain(vm *vdbe.VDBE, stmt *parser.ExplainStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	if stmt.QueryPlan {
		// EXPLAIN QUERY PLAN - generate high-level query plan
		return s.compileExplainQueryPlan(vm, stmt, args)
	}

	// EXPLAIN - show VDBE opcodes
	return s.compileExplainOpcodes(vm, stmt, args)
}

// compileExplainQueryPlan compiles EXPLAIN QUERY PLAN.
func (s *Stmt) compileExplainQueryPlan(vm *vdbe.VDBE, stmt *parser.ExplainStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Generate the explain plan for the inner statement
	plan, err := planner.GenerateExplain(stmt.Statement)
	if err != nil {
		return nil, fmt.Errorf("failed to generate explain plan: %w", err)
	}

	// Format the plan as table rows
	rows := plan.FormatAsTable()

	// Set up result columns: selectid, order, from, detail (SQLite format)
	vm.ResultCols = []string{"selectid", "order", "from", "detail"}

	// Allocate memory for result columns (4 columns)
	vm.AllocMemory(10)

	// Emit Init opcode
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// For each row in the plan, emit opcodes to output it
	for _, row := range rows {
		// Load values into registers 0-3
		// Register 0: id (integer)
		id := row[0].(int)
		vm.AddOp(vdbe.OpInteger, id, 0, 0)

		// Register 1: parent (integer)
		parent := row[1].(int)
		vm.AddOp(vdbe.OpInteger, parent, 1, 0)

		// Register 2: notused (integer)
		notused := row[2].(int)
		vm.AddOp(vdbe.OpInteger, notused, 2, 0)

		// Register 3: detail (string)
		detail := row[3].(string)
		vm.AddOpWithP4Str(vdbe.OpString8, 0, 3, 0, detail)

		// Emit result row
		vm.AddOp(vdbe.OpResultRow, 0, 4, 0)
	}

	// Halt
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileExplainOpcodes compiles basic EXPLAIN (show VDBE opcodes).
func (s *Stmt) compileExplainOpcodes(vm *vdbe.VDBE, stmt *parser.ExplainStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	compiledVM, err := s.compileInnerStatementForExplain(stmt, args)
	if err != nil {
		return nil, err
	}

	s.setupExplainVM(vm)
	s.emitExplainInstructions(vm, compiledVM.Program)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileInnerStatementForExplain compiles the inner statement for EXPLAIN
func (s *Stmt) compileInnerStatementForExplain(stmt *parser.ExplainStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	innerVM := s.newVDBE()
	compiledVM, err := s.compileInnerStatement(innerVM, stmt.Statement, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile inner statement: %w", err)
	}
	return compiledVM, nil
}

// setupExplainVM sets up the VM for EXPLAIN output
func (s *Stmt) setupExplainVM(vm *vdbe.VDBE) {
	vm.ResultCols = []string{"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"}
	vm.AllocMemory(20)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
}

// emitExplainInstructions emits EXPLAIN rows for each instruction
func (s *Stmt) emitExplainInstructions(vm *vdbe.VDBE, program []*vdbe.Instruction) {
	for i, instr := range program {
		s.emitExplainRow(vm, i, instr)
	}
}

// emitExplainRow emits a single EXPLAIN result row
func (s *Stmt) emitExplainRow(vm *vdbe.VDBE, addr int, instr *vdbe.Instruction) {
	vm.AddOp(vdbe.OpInteger, addr, 0, 0)
	vm.AddOpWithP4Str(vdbe.OpString8, 0, 1, 0, instr.Opcode.String())
	vm.AddOp(vdbe.OpInteger, instr.P1, 2, 0)
	vm.AddOp(vdbe.OpInteger, instr.P2, 3, 0)
	vm.AddOp(vdbe.OpInteger, instr.P3, 4, 0)
	vm.AddOpWithP4Str(vdbe.OpString8, 0, 5, 0, formatP4(instr))
	vm.AddOp(vdbe.OpInteger, int(instr.P5), 6, 0)
	vm.AddOpWithP4Str(vdbe.OpString8, 0, 7, 0, getComment(instr))
	vm.AddOp(vdbe.OpResultRow, 0, 8, 0)
}

// formatP4 formats the P4 parameter for display
func formatP4(instr *vdbe.Instruction) string {
	switch instr.P4Type {
	case vdbe.P4Int32:
		return fmt.Sprintf("%d", instr.P4.I)
	case vdbe.P4Int64:
		return fmt.Sprintf("%d", instr.P4.I64)
	case vdbe.P4Real:
		return fmt.Sprintf("%g", instr.P4.R)
	case vdbe.P4Static, vdbe.P4Dynamic:
		return instr.P4.Z
	}
	return ""
}

// getComment returns the comment or empty string
func getComment(instr *vdbe.Instruction) string {
	if instr.Comment != "" {
		return instr.Comment
	}
	return ""
}

// statementCompiler is a function type for compiling specific statement types.
type statementCompiler func(*vdbe.VDBE, parser.Statement, []driver.NamedValue) (*vdbe.VDBE, error)

// getStatementCompilerMap returns the map of statement types to their compilers.
func (s *Stmt) getStatementCompilerMap() map[string]statementCompiler {
	return map[string]statementCompiler{
		"*parser.SelectStmt":      s.compileSelectWrapper,
		"*parser.InsertStmt":      s.compileInsertWrapper,
		"*parser.UpdateStmt":      s.compileUpdateWrapper,
		"*parser.DeleteStmt":      s.compileDeleteWrapper,
		"*parser.CreateTableStmt": s.compileCreateTableWrapper,
		"*parser.DropTableStmt":   s.compileDropTableWrapper,
		"*parser.CreateViewStmt":  s.compileCreateViewWrapper,
		"*parser.DropViewStmt":    s.compileDropViewWrapper,
	}
}

// Wrapper functions for type-specific compilation
func (s *Stmt) compileSelectWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileSelect(vm, stmt.(*parser.SelectStmt), args)
}

func (s *Stmt) compileInsertWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileInsert(vm, stmt.(*parser.InsertStmt), args)
}

func (s *Stmt) compileUpdateWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileUpdate(vm, stmt.(*parser.UpdateStmt), args)
}

func (s *Stmt) compileDeleteWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileDelete(vm, stmt.(*parser.DeleteStmt), args)
}

func (s *Stmt) compileCreateTableWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileCreateTable(vm, stmt.(*parser.CreateTableStmt), args)
}

func (s *Stmt) compileDropTableWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileDropTable(vm, stmt.(*parser.DropTableStmt), args)
}

func (s *Stmt) compileCreateViewWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileCreateView(vm, stmt.(*parser.CreateViewStmt), args)
}

func (s *Stmt) compileDropViewWrapper(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	return s.compileDropView(vm, stmt.(*parser.DropViewStmt), args)
}

// compileInnerStatement compiles the inner statement of an EXPLAIN using table-driven dispatch.
func (s *Stmt) compileInnerStatement(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	compilerMap := s.getStatementCompilerMap()
	stmtType := fmt.Sprintf("%T", stmt)

	if compiler, ok := compilerMap[stmtType]; ok {
		return compiler(vm, stmt, args)
	}

	return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
}

// ============================================================================
// Trigger Execution Helper Functions
// ============================================================================

// executeBeforeInsertTriggers executes all BEFORE INSERT triggers for the given table.
func (s *Stmt) executeBeforeInsertTriggers(stmt *parser.InsertStmt, table *schema.Table) error {
	// Note: This is called during compilation, not runtime. For proper trigger execution,
	// we need to prepare NEW row data from the INSERT VALUES clause.
	// In a production implementation, triggers would execute during VDBE runtime when
	// actual row data is available. This is a simplified version that executes at compile time.

	timing := parser.TriggerBefore
	event := parser.TriggerInsert
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil // No triggers to execute
	}

	// Prepare NEW row data from INSERT statement
	newRow := s.prepareNewRowForInsert(stmt, table)

	// Create trigger context
	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    nil, // INSERT has no OLD row
		NewRow:    newRow,
		TableName: stmt.Table,
	}

	// Execute triggers
	return engine.ExecuteTriggersForInsert(ctx)
}

// executeAfterInsertTriggers executes all AFTER INSERT triggers for the given table.
func (s *Stmt) executeAfterInsertTriggers(stmt *parser.InsertStmt, table *schema.Table) error {
	timing := parser.TriggerAfter
	event := parser.TriggerInsert
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil
	}

	// Prepare NEW row data
	newRow := s.prepareNewRowForInsert(stmt, table)

	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    nil,
		NewRow:    newRow,
		TableName: stmt.Table,
	}

	return engine.ExecuteAfterInsertTriggers(ctx)
}

// executeBeforeUpdateTriggers executes all BEFORE UPDATE triggers for the given table.
func (s *Stmt) executeBeforeUpdateTriggers(stmt *parser.UpdateStmt, table *schema.Table, updatedColumns []string) error {
	timing := parser.TriggerBefore
	event := parser.TriggerUpdate
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil
	}

	// Note: In a full implementation, we would need to iterate through each row
	// and execute triggers with the actual OLD and NEW values. This simplified
	// version executes once at compile time with placeholder data.

	// For UPDATE, we need both OLD and NEW rows
	// Since we're at compile time, we can't access actual row data
	// In a production implementation, this would be done in the VDBE loop
	oldRow := make(map[string]interface{})
	newRow := make(map[string]interface{})

	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    oldRow,
		NewRow:    newRow,
		TableName: stmt.Table,
	}

	return engine.ExecuteTriggersForUpdate(ctx, updatedColumns)
}

// executeAfterUpdateTriggers executes all AFTER UPDATE triggers for the given table.
func (s *Stmt) executeAfterUpdateTriggers(stmt *parser.UpdateStmt, table *schema.Table, updatedColumns []string) error {
	timing := parser.TriggerAfter
	event := parser.TriggerUpdate
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil
	}

	oldRow := make(map[string]interface{})
	newRow := make(map[string]interface{})

	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    oldRow,
		NewRow:    newRow,
		TableName: stmt.Table,
	}

	return engine.ExecuteAfterUpdateTriggers(ctx, updatedColumns)
}

// executeBeforeDeleteTriggers executes all BEFORE DELETE triggers for the given table.
func (s *Stmt) executeBeforeDeleteTriggers(stmt *parser.DeleteStmt, table *schema.Table) error {
	timing := parser.TriggerBefore
	event := parser.TriggerDelete
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil
	}

	// For DELETE, we need the OLD row (the row being deleted)
	// Since we're at compile time, we use placeholder data
	oldRow := make(map[string]interface{})

	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    oldRow,
		NewRow:    nil, // DELETE has no NEW row
		TableName: stmt.Table,
	}

	return engine.ExecuteTriggersForDelete(ctx)
}

// executeAfterDeleteTriggers executes all AFTER DELETE triggers for the given table.
func (s *Stmt) executeAfterDeleteTriggers(stmt *parser.DeleteStmt, table *schema.Table) error {
	timing := parser.TriggerAfter
	event := parser.TriggerDelete
	triggers := s.conn.schema.GetTableTriggers(stmt.Table, &timing, &event)

	if len(triggers) == 0 {
		return nil
	}

	oldRow := make(map[string]interface{})

	ctx := &engine.TriggerContext{
		Schema:    s.conn.schema,
		Pager:     s.conn.pager,
		Btree:     s.conn.btree,
		OldRow:    oldRow,
		NewRow:    nil,
		TableName: stmt.Table,
	}

	return engine.ExecuteAfterDeleteTriggers(ctx)
}

// prepareNewRowForInsert constructs a NEW row map from the INSERT statement.
// This extracts values from the first row of the VALUES clause.
func (s *Stmt) prepareNewRowForInsert(stmt *parser.InsertStmt, table *schema.Table) map[string]interface{} {
	newRow := make(map[string]interface{})

	if len(stmt.Values) == 0 {
		return newRow
	}

	// Get column names (use all table columns if not specified)
	colNames := stmt.Columns
	if len(colNames) == 0 {
		colNames = make([]string, len(table.Columns))
		for i, col := range table.Columns {
			colNames[i] = col.Name
		}
	}

	// Get first row values
	firstRow := stmt.Values[0]

	// Map column names to values
	for i, colName := range colNames {
		if i < len(firstRow) {
			val := s.extractValueFromExpression(firstRow[i])
			newRow[colName] = val
		}
	}

	return newRow
}

// extractValueFromExpression extracts the actual value from an expression.
// This handles literal values and returns placeholder for complex expressions.
func (s *Stmt) extractValueFromExpression(expr parser.Expression) interface{} {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return s.parseLiteralValue(e)
	case *parser.VariableExpr:
		// Bound parameter - return placeholder
		return nil
	default:
		// Complex expression - return nil placeholder
		return nil
	}
}

// parseLiteralValue converts a literal expression to its Go value.
func (s *Stmt) parseLiteralValue(expr *parser.LiteralExpr) interface{} {
	switch expr.Type {
	case parser.LiteralInteger:
		var val int64
		fmt.Sscanf(expr.Value, "%d", &val)
		return val
	case parser.LiteralFloat:
		var val float64
		fmt.Sscanf(expr.Value, "%f", &val)
		return val
	case parser.LiteralString:
		return expr.Value
	case parser.LiteralNull:
		return nil
	default:
		return expr.Value
	}
}
