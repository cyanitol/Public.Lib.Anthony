// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// extractArgValues converts driver.NamedValue args to a plain []interface{}.
func extractArgValues(args []driver.NamedValue) []interface{} {
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	return argValues
}

// emitMakeRecordAndInsert emits OpMakeRecord + OpInsert with the correct
// rowid register handling for both normal and WITHOUT ROWID tables.
// cursor is the btree cursor index, conflictMode is stored in P4.I,
// and tableName is stored in P4.Z. conflictMode of -1 means no P4.I is set.
func emitMakeRecordAndInsert(vm *vdbe.VDBE, cursor, recordStartReg, numRecordCols, rowidReg int,
	withoutRowID bool, conflictMode int32, tableName string) int {

	resultReg := recordStartReg + numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)

	rowidRegToUse := rowidReg
	if withoutRowID {
		rowidRegToUse = 0
	}
	insertOp := vm.AddOp(vdbe.OpInsert, cursor, resultReg, rowidRegToUse)
	if conflictMode >= 0 {
		vm.Program[insertOp].P4.I = conflictMode
	}
	vm.Program[insertOp].P4.Z = tableName
	return insertOp
}

// newTableCodeGenerator creates a CodeGenerator configured for a table cursor,
// with subquery support and optional args binding.
func (s *Stmt) newTableCodeGenerator(vm *vdbe.VDBE, tableName string, table *schema.Table,
	cursorIdx int, args []driver.NamedValue) *expr.CodeGenerator {

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(tableName, cursorIdx)
	gen.RegisterTable(buildTableInfo(tableName, table))
	if len(args) > 0 {
		gen.SetArgs(extractArgValues(args))
	}
	return gen
}

// ============================================================================
// INSERT Compilation
// ============================================================================

// insertFirstRow validates that stmt has a VALUES clause and returns the first
// value row. It returns an error when no values are present.
func insertFirstRow(stmt *parser.InsertStmt) ([]parser.Expression, error) {
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}
	return stmt.Values[0], nil
}

// compileInsert compiles an INSERT statement. CC=4
func (s *Stmt) compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	table, db, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}
	s.setVdbeContextForDatabase(vm, db)

	// Route virtual tables through vtab interface
	if s.isVirtualTable(table) {
		return s.compileVTabInsert(vm, stmt, table, args)
	}

	// Trigger execution is wired through OpTriggerBefore/OpTriggerAfter opcodes
	// emitted in compileInsertValues, compileUpdate, and compileDelete.
	// At VDBE runtime, these opcodes invoke TriggerCompilerInterface to compile
	// and execute trigger body statements with actual OLD/NEW row data.

	// Handle INSERT...SELECT
	if stmt.Select != nil {
		return s.compileInsertSelect(vm, stmt, args)
	}

	// Check for UPSERT clause (INSERT...ON CONFLICT)
	if stmt.Upsert != nil {
		return s.compileInsertUpsert(vm, stmt, args)
	}

	return s.compileInsertValues(vm, stmt, table, args)
}

// compileInsertValues compiles a regular INSERT...VALUES statement.
func (s *Stmt) compileInsertValues(vm *vdbe.VDBE, stmt *parser.InsertStmt, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	expandedValues, err := s.validateAndExpandInsertValues(stmt, table)
	if err != nil {
		return nil, err
	}

	allColNames := allTableColumnNames(table)
	// If a rowid alias was appended by expandInsertDefaults, include it.
	if len(expandedValues[0]) > len(table.Columns) {
		allColNames = append(allColNames, "rowid")
	}
	rowidColIdx := findInsertRowidCol(allColNames, table)
	numRecordCols := insertRecordColCount(expandedValues[0], table, rowidColIdx)

	const rowidReg = 1
	const recordStartReg = 2
	gen := s.initInsertVDBE(vm, numRecordCols, recordStartReg, args)

	// Register table info when RETURNING is present so column refs resolve
	if len(stmt.Returning) > 0 {
		gen.RegisterCursor(stmt.Table, 0)
		gen.RegisterTable(buildTableInfo(stmt.Table, table))
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	paramIdx := 0
	conflictMode := getConflictMode(stmt.OnConflict)
	hasTriggers := s.tableHasTriggers(stmt.Table)

	// Expand and set up RETURNING before the loop
	var retCols []parser.ResultColumn
	var retNumCols int
	if len(stmt.Returning) > 0 {
		var retNames []string
		retCols, retNames = expandReturningColumns(stmt.Returning, table)
		retNumCols = len(retCols)
		vm.ResultCols = retNames
	}

	for _, row := range expandedValues {
		s.emitInsertValueRow(vm, stmt, table, row, allColNames, rowidColIdx,
			rowidReg, recordStartReg, numRecordCols, conflictMode, hasTriggers, args, &paramIdx, gen)

		// Emit RETURNING row: seek to inserted rowid and evaluate expressions
		if retNumCols > 0 {
			vm.AddOp(vdbe.OpSeekRowid, 0, 0, rowidReg)
			if err := emitReturningRow(vm, retCols, retNumCols, gen); err != nil {
				return nil, err
			}
		}
	}

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// validateAndExpandInsertValues validates the INSERT statement and expands defaults.
func (s *Stmt) validateAndExpandInsertValues(stmt *parser.InsertStmt, table *schema.Table) ([][]parser.Expression, error) {
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}

	// Reject explicit writes to generated columns
	if err := validateNoGeneratedColumns(stmt.Columns, table); err != nil {
		return nil, err
	}

	if len(stmt.Columns) > 0 {
		for _, row := range stmt.Values {
			if len(row) != len(stmt.Columns) {
				return nil, fmt.Errorf("%d values for %d columns", len(row), len(stmt.Columns))
			}
		}
	}

	expandedValues := expandInsertDefaults(stmt, table)
	numCols := len(table.Columns)
	hasVirtualRowid := findRowidAliasInColumns(stmt.Columns, table) >= 0
	for _, row := range expandedValues {
		expected := numCols
		if hasVirtualRowid {
			expected++
		}
		if len(row) != expected {
			return nil, fmt.Errorf("table %s has %d columns but %d values were supplied", table.Name, numCols, len(row))
		}
	}
	return expandedValues, nil
}

// validateNoGeneratedColumns returns an error if any explicitly named column
// in the INSERT column list is a generated column.
func validateNoGeneratedColumns(columns []string, table *schema.Table) error {
	for _, name := range columns {
		col, ok := table.GetColumn(name)
		if ok && col.Generated {
			return fmt.Errorf("cannot INSERT into generated column \"%s\"", name)
		}
	}
	return nil
}

// insertRecordColCount returns the number of columns in the record portion of an INSERT.
func insertRecordColCount(firstRow []parser.Expression, table *schema.Table, rowidColIdx int) int {
	numRecordCols := len(firstRow)
	if !table.WithoutRowID && rowidColIdx >= 0 {
		numRecordCols--
	}
	return numRecordCols
}

// initInsertVDBE initializes the VDBE and code generator for INSERT compilation.
func (s *Stmt) initInsertVDBE(vm *vdbe.VDBE, numRecordCols, recordStartReg int, args []driver.NamedValue) *expr.CodeGenerator {
	vm.AllocMemory(numRecordCols + 10)
	vm.AllocCursors(1)

	gen := expr.NewCodeGenerator(vm)
	gen.SetNextReg(recordStartReg + numRecordCols + 1)
	if len(args) > 0 {
		gen.SetArgs(extractArgValues(args))
	}
	return gen
}

// emitInsertValueRow emits bytecode for a single INSERT VALUES row.
func (s *Stmt) emitInsertValueRow(vm *vdbe.VDBE, stmt *parser.InsertStmt, table *schema.Table,
	row []parser.Expression, allColNames []string, rowidColIdx, rowidReg, recordStartReg, numRecordCols int,
	conflictMode int32, hasTriggers bool, args []driver.NamedValue, paramIdx *int, gen *expr.CodeGenerator) {

	if !table.WithoutRowID {
		s.emitInsertRowid(vm, table, row, rowidColIdx, rowidReg, args, paramIdx, gen)
	}
	s.emitInsertRecordValues(vm, table, allColNames, row, rowidColIdx, recordStartReg, args, paramIdx, gen)

	var beforeAddr int
	if hasTriggers {
		beforeAddr = vm.AddOp(vdbe.OpTriggerBefore, 0, 0, recordStartReg)
		vm.Program[beforeAddr].P4.Z = stmt.Table
	}

	emitMakeRecordAndInsert(vm, 0, recordStartReg, numRecordCols, rowidReg,
		table.WithoutRowID, conflictMode, table.Name)

	if hasTriggers {
		afterAddr := vm.AddOp(vdbe.OpTriggerAfter, 0, 0, recordStartReg)
		vm.Program[afterAddr].P4.Z = stmt.Table
		vm.Program[beforeAddr].P2 = vm.NumOps()
	}
}

// emitInsertRow emits bytecode for inserting a single row.
func (s *Stmt) emitInsertRow(vm *vdbe.VDBE, table *schema.Table, colNames []string, row []parser.Expression,
	rowidColIdx, rowidReg, recordStartReg, numRecordCols int, conflictMode int32,
	args []driver.NamedValue, paramIdx *int, gen *expr.CodeGenerator) {

	// For WITHOUT ROWID tables, we don't generate a rowid
	if !table.WithoutRowID {
		s.emitInsertRowid(vm, table, row, rowidColIdx, rowidReg, args, paramIdx, gen)
	}
	s.emitInsertRecordValues(vm, table, colNames, row, rowidColIdx, recordStartReg, args, paramIdx, gen)

	emitMakeRecordAndInsert(vm, 0, recordStartReg, numRecordCols, rowidReg,
		table.WithoutRowID, conflictMode, table.Name)
}

// compileInsertSelect compiles an INSERT...SELECT statement.
func (s *Stmt) compileInsertSelect(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// When the SELECT contains aggregates, ORDER BY, LIMIT, or other
	// features that require full materialisation, execute the SELECT first
	// and then insert the result rows.
	if s.insertSelectNeedsMaterialise(stmt.Select) {
		return s.compileInsertSelectMaterialised(vm, stmt, args)
	}

	// Prepare tables and columns
	ctx, err := s.prepareInsertSelectContext(vm, stmt, args)
	if err != nil {
		return nil, err
	}

	// Initialize VDBE program
	s.initializeInsertSelect(vm, ctx)

	// Emit the main SELECT loop
	rewindAddr := s.emitInsertSelectLoop(vm, stmt.Select, ctx)

	// Finalize program
	s.finalizeInsertSelect(vm, ctx, rewindAddr)

	return vm, nil
}

// insertSelectNeedsMaterialise returns true when the SELECT part contains
// features that cannot be compiled as a simple row-by-row scan loop, such as
// aggregates, ORDER BY, LIMIT, or DISTINCT.
func (s *Stmt) insertSelectNeedsMaterialise(sel *parser.SelectStmt) bool {
	if sel == nil {
		return false
	}
	return s.detectAggregates(sel) || len(sel.OrderBy) > 0 || sel.Limit != nil || sel.Distinct
}

// compileInsertSelectMaterialised executes the SELECT to completion, then
// inserts each result row into the target table.
func (s *Stmt) compileInsertSelectMaterialised(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Execute the SELECT
	selVM := vdbe.New()
	selVM.Ctx = vm.Ctx
	compiled, err := s.compileSelect(selVM, stmt.Select, args)
	if err != nil {
		return nil, fmt.Errorf("INSERT..SELECT compile: %w", err)
	}
	numCols := len(compiled.ResultCols)
	rows, err := s.collectRows(compiled, numCols, "INSERT..SELECT")
	if err != nil {
		return nil, err
	}

	// Resolve target table
	targetTable, db, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}
	s.setVdbeContextForDatabase(vm, db)

	return s.emitInsertMaterialisedRows(vm, targetTable, stmt, rows, numCols)
}

// emitInsertMaterialisedRows emits bytecode to insert pre-materialised rows.
func (s *Stmt) emitInsertMaterialisedRows(vm *vdbe.VDBE, table *schema.Table, stmt *parser.InsertStmt, rows [][]interface{}, numCols int) (*vdbe.VDBE, error) {
	colNames := resolveInsertColumns(stmt, table)
	rowidColIdx := findInsertRowidCol(colNames, table)
	numRecordCols := numCols
	if !table.WithoutRowID && rowidColIdx >= 0 {
		numRecordCols--
	}

	const rowidReg = 1
	const recordStartReg = 2
	vm.AllocMemory(numRecordCols + 10)
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	for _, row := range rows {
		s.emitMaterialisedRow(vm, table, row, rowidColIdx, rowidReg, recordStartReg, numRecordCols)
	}

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitMaterialisedRow emits bytecode for a single pre-materialised row.
func (s *Stmt) emitMaterialisedRow(vm *vdbe.VDBE, table *schema.Table, row []interface{}, rowidColIdx, rowidReg, recordStartReg, numRecordCols int) {
	// Handle rowid
	if !table.WithoutRowID {
		if rowidColIdx >= 0 && rowidColIdx < len(row) {
			emitLoadValue(vm, row[rowidColIdx], rowidReg)
		} else {
			vm.AddOp(vdbe.OpNewRowid, 0, 0, rowidReg)
		}
	}

	// Load non-rowid columns
	reg := recordStartReg
	for i, val := range row {
		if !table.WithoutRowID && i == rowidColIdx {
			continue
		}
		emitLoadValue(vm, val, reg)
		reg++
	}

	emitMakeRecordAndInsert(vm, 0, recordStartReg, numRecordCols, rowidReg,
		table.WithoutRowID, -1, table.Name)
}

// insertSelectContext holds the context for INSERT...SELECT compilation.
type insertSelectContext struct {
	targetTable    *schema.Table
	sourceTable    *schema.Table
	targetColNames []string
	selectCols     []parser.ResultColumn
	rowidColIdx    int
	numRecordCols  int
	gen            *expr.CodeGenerator
	skipAddr       int
	hasWhereClause bool
}

// prepareInsertSelectContext sets up the context for INSERT...SELECT compilation.
func (s *Stmt) prepareInsertSelectContext(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*insertSelectContext, error) {
	// Get target table schema
	targetTable, _, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Resolve target columns
	targetColNames := resolveInsertColumns(stmt, targetTable)
	rowidColIdx := findInsertRowidCol(targetColNames, targetTable)

	// Count non-rowid columns for record
	// For WITHOUT ROWID tables, all columns go in the record
	// For normal tables, only non-rowid columns go in the record
	numRecordCols := len(targetColNames)
	if !targetTable.WithoutRowID && rowidColIdx >= 0 {
		numRecordCols--
	}

	// Get source table from SELECT
	sourceTableName, err := selectFromTableName(stmt.Select)
	if err != nil {
		return nil, fmt.Errorf("INSERT...SELECT requires FROM clause: %w", err)
	}

	sourceTable, _, _, ok := s.conn.dbRegistry.ResolveTable("", sourceTableName)
	if !ok {
		return nil, fmt.Errorf("source table not found: %s", sourceTableName)
	}

	// Setup code generator for expression evaluation
	gen := s.setupInsertSelectGenerator(vm, sourceTableName, sourceTable, args)

	// Expand SELECT * if needed and validate column count
	selectCols, err := resolveSelectColumns(stmt.Select, sourceTable, targetColNames)
	if err != nil {
		return nil, err
	}

	// Allocate memory and cursors
	vm.AllocMemory(numRecordCols + 30)
	vm.AllocCursors(2)

	return &insertSelectContext{
		targetTable:    targetTable,
		sourceTable:    sourceTable,
		targetColNames: targetColNames,
		selectCols:     selectCols,
		rowidColIdx:    rowidColIdx,
		numRecordCols:  numRecordCols,
		gen:            gen,
		hasWhereClause: stmt.Select.Where != nil,
	}, nil
}

// setupInsertSelectGenerator creates and configures the code generator.
func (s *Stmt) setupInsertSelectGenerator(vm *vdbe.VDBE, sourceTableName string, sourceTable *schema.Table, args []driver.NamedValue) *expr.CodeGenerator {
	return s.newTableCodeGenerator(vm, sourceTableName, sourceTable, 1, args)
}

// resolveSelectColumns expands SELECT * if needed and validates column count.
func resolveSelectColumns(selectStmt *parser.SelectStmt, sourceTable *schema.Table, targetColNames []string) ([]parser.ResultColumn, error) {
	selectCols := selectStmt.Columns
	if len(selectCols) > 0 && selectCols[0].Star {
		selectCols = expandStarToColumns(sourceTable)
	}

	// Validate column count matches
	if len(selectCols) != len(targetColNames) {
		return nil, fmt.Errorf("column count mismatch: SELECT returns %d columns, but INSERT expects %d", len(selectCols), len(targetColNames))
	}

	return selectCols, nil
}

// initializeInsertSelect emits initialization opcodes.
func (s *Stmt) initializeInsertSelect(vm *vdbe.VDBE, ctx *insertSelectContext) {
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open target table for writing (cursor 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(ctx.targetTable.RootPage), len(ctx.targetTable.Columns))

	// Open source table for reading (cursor 1)
	if !ctx.sourceTable.Temp {
		vm.AddOp(vdbe.OpOpenRead, 1, int(ctx.sourceTable.RootPage), len(ctx.sourceTable.Columns))
	}
}

// emitInsertSelectLoop emits the main loop that reads from source and inserts into target.
func (s *Stmt) emitInsertSelectLoop(vm *vdbe.VDBE, selectStmt *parser.SelectStmt, ctx *insertSelectContext) int {
	const rowidReg = 1
	const recordStartReg = 2

	// Rewind source table
	rewindAddr := vm.AddOp(vdbe.OpRewind, 1, 0, 0)

	// WHERE clause evaluation (if present)
	if ctx.hasWhereClause {
		ctx.skipAddr = s.emitInsertSelectWhere(vm, selectStmt, ctx.gen)
	}

	// Generate rowid and record columns
	s.emitInsertSelectRowid(vm, ctx, rowidReg)
	s.emitInsertSelectRecordColumns(vm, ctx, recordStartReg)

	// Make record and insert
	emitMakeRecordAndInsert(vm, 0, recordStartReg, ctx.numRecordCols, rowidReg,
		ctx.targetTable.WithoutRowID, -1, ctx.targetTable.Name)

	// Fix WHERE skip address
	if ctx.hasWhereClause {
		vm.Program[ctx.skipAddr].P2 = vm.NumOps()
	}

	// Next row in source table
	vm.AddOp(vdbe.OpNext, 1, rewindAddr+1, 0)

	return rewindAddr
}

// emitInsertSelectWhere emits WHERE clause evaluation.
func (s *Stmt) emitInsertSelectWhere(vm *vdbe.VDBE, selectStmt *parser.SelectStmt, gen *expr.CodeGenerator) int {
	whereReg, err := gen.GenerateExpr(selectStmt.Where)
	if err != nil {
		return 0
	}
	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
}

// emitInsertSelectRowid generates rowid for the target row.
// For WITHOUT ROWID tables, this is a no-op since they don't use rowids.
func (s *Stmt) emitInsertSelectRowid(vm *vdbe.VDBE, ctx *insertSelectContext, rowidReg int) {
	// WITHOUT ROWID tables don't use OpNewRowid
	if ctx.targetTable.WithoutRowID {
		// No rowid needed - PRIMARY KEY columns are in the record
		return
	}

	if ctx.rowidColIdx >= 0 {
		// One of the SELECT columns maps to the rowid column
		selectCol := ctx.selectCols[ctx.rowidColIdx]
		emitSelectColumnOp(vm, ctx.sourceTable, selectCol, rowidReg, ctx.gen)
	} else {
		// Generate new rowid
		vm.AddOp(vdbe.OpNewRowid, 0, 0, rowidReg)
	}
}

// emitInsertSelectRecordColumns reads SELECT columns into record registers.
// For WITHOUT ROWID tables, includes PRIMARY KEY columns in the record.
func (s *Stmt) emitInsertSelectRecordColumns(vm *vdbe.VDBE, ctx *insertSelectContext, recordStartReg int) {
	recordReg := recordStartReg
	for i, selectCol := range ctx.selectCols {
		// For normal tables, skip the rowid column (it's stored separately)
		// For WITHOUT ROWID tables, include all columns (even PRIMARY KEY)
		if !ctx.targetTable.WithoutRowID && i == ctx.rowidColIdx {
			continue // Skip rowid - already handled
		}
		emitSelectColumnOp(vm, ctx.sourceTable, selectCol, recordReg, ctx.gen)
		recordReg++
	}
}

// finalizeInsertSelect closes tables and adds halt instruction.
func (s *Stmt) finalizeInsertSelect(vm *vdbe.VDBE, ctx *insertSelectContext, rewindAddr int) {
	// Close tables
	if !ctx.sourceTable.Temp {
		vm.AddOp(vdbe.OpClose, 1, 0, 0)
	}
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Halt
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix rewind jump target
	vm.Program[rewindAddr].P2 = haltAddr
}

// expandStarToColumns expands SELECT * to explicit column references.
func expandStarToColumns(table *schema.Table) []parser.ResultColumn {
	cols := make([]parser.ResultColumn, len(table.Columns))
	for i, schemaCol := range table.Columns {
		cols[i] = parser.ResultColumn{
			Expr: &parser.IdentExpr{Name: schemaCol.Name},
		}
	}
	return cols
}

// resolveInsertColumns returns the column name list for an INSERT statement.
// When the statement omits columns, every table column is used in order.
func resolveInsertColumns(stmt *parser.InsertStmt, table *schema.Table) []string {
	if len(stmt.Columns) > 0 {
		return stmt.Columns
	}
	names := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		names[i] = col.Name
	}
	return names
}

// findInsertRowidCol returns the index within names of the INTEGER PRIMARY KEY
// column (or explicit rowid alias), or -1 when none exists.
// For WITHOUT ROWID tables, always returns -1.
func findInsertRowidCol(names []string, table *schema.Table) int {
	if table.WithoutRowID {
		return -1
	}
	for i, name := range names {
		idx := table.GetColumnIndex(name)
		if idx >= 0 && schemaColIsRowid(table.Columns[idx]) {
			return i
		}
	}
	// Check for explicit rowid aliases (rowid, _rowid_, oid) not in schema.
	for i, name := range names {
		lower := strings.ToLower(name)
		if lower == "rowid" || lower == "_rowid_" || lower == "oid" {
			if table.GetColumnIndex(name) < 0 {
				return i
			}
		}
	}
	return -1
}

// emitInsertRowid emits the opcode that places the rowid into rowidReg.
// When the INSERT specifies an explicit rowid value it is loaded from the
// VALUES clause; otherwise OpNewRowid generates a fresh rowid.
// For AUTOINCREMENT columns, special handling ensures rowids are never reused.
// For WITHOUT ROWID tables, this function should not be called as they don't use rowids.
func (s *Stmt) emitInsertRowid(vm *vdbe.VDBE, table *schema.Table, row []parser.Expression, rowidColIdx int, rowidReg int, args []driver.NamedValue, paramIdx *int, gen *expr.CodeGenerator) {
	if rowidColIdx >= 0 {
		s.compileValue(vm, row[rowidColIdx], rowidReg, args, paramIdx, gen)

		// If this is an AUTOINCREMENT column, we need to update the sequence
		// even when an explicit value is provided (unless it's NULL)
		if autoincrementCol, hasAutoincrement := table.HasAutoincrementColumn(); hasAutoincrement {
			colIdx := table.GetColumnIndex(autoincrementCol.Name)
			if colIdx >= 0 && colIdx == rowidColIdx {
				// Check if the value is NULL - if so, generate from sequence
				// This is handled by checking the register value at runtime
				// For now, we'll add a comment that this needs sequence handling
				// The actual sequence update will happen in the INSERT opcode handler
				vm.Program[len(vm.Program)-1].Comment = "AUTOINCREMENT column - sequence update needed"
			}
		}
		return
	}

	// WITHOUT ROWID tables don't use OpNewRowid - they use PRIMARY KEY columns
	if table.WithoutRowID {
		// PRIMARY KEY values should already be in the record
		// Set rowidReg to 0 as a sentinel - OpInsert will handle this specially
		vm.AddOp(vdbe.OpInteger, 0, rowidReg, 0)
		return
	}

	// OpNewRowid: P1=cursor, P3=destination register
	vm.AddOp(vdbe.OpNewRowid, 0, 0, rowidReg)
}

// emitInsertRecordValues emits OpXxx opcodes that load each non-rowid value
// from row into consecutive registers beginning at startReg, applying column
// affinity as needed.
// For WITHOUT ROWID tables, includes PRIMARY KEY columns in the record.
func (s *Stmt) emitInsertRecordValues(vm *vdbe.VDBE, table *schema.Table, colNames []string, row []parser.Expression, rowidColIdx int, startReg int, args []driver.NamedValue, paramIdx *int, gen *expr.CodeGenerator) {
	reg := startReg
	for i, val := range row {
		// For normal tables, skip the rowid column (it's stored separately)
		// For WITHOUT ROWID tables, include all columns (even PRIMARY KEY)
		if !table.WithoutRowID && i == rowidColIdx {
			continue // rowid already loaded separately
		}
		s.compileValue(vm, val, reg, args, paramIdx, gen)

		// Apply column affinity if column exists in schema.
		// Per SQLite rules, BLOB values are never coerced by column affinity.
		colName := colNames[i]
		if !isBlobLiteral(val) {
			if colIdx := table.GetColumnIndex(colName); colIdx >= 0 {
				col := table.Columns[colIdx]
				if col.Affinity != schema.AffinityNone && col.Affinity != schema.AffinityBlob {
					affinityCode := affinityToOpCastCode(col.Affinity)
					vm.AddOp(vdbe.OpCast, reg, affinityCode, 0)
				}
			}
		}
		reg++
	}
}

// isBlobLiteral returns true if the expression is a BLOB literal (X'...').
// SQLite never applies column affinity to BLOB values.
func isBlobLiteral(e parser.Expression) bool {
	lit, ok := e.(*parser.LiteralExpr)
	return ok && lit.Type == parser.LiteralBlob
}

// affinityToOpCastCode converts a schema.Affinity to the OpCast P2 encoding.
// OpCast P2 values: 0=NONE/BLOB, 1=BLOB, 2=TEXT, 3=INTEGER, 4=REAL, 5=NUMERIC
func affinityToOpCastCode(aff schema.Affinity) int {
	switch aff {
	case schema.AffinityBlob:
		return 1
	case schema.AffinityText:
		return 2
	case schema.AffinityInteger:
		return 3
	case schema.AffinityReal:
		return 4
	case schema.AffinityNumeric:
		return 5
	default:
		return 0
	}
}

// compileValue compiles a value expression into bytecode that stores the result in reg.
// CC=4
func (s *Stmt) compileValue(vm *vdbe.VDBE, val parser.Expression, reg int, args []driver.NamedValue, paramIdx *int, gen *expr.CodeGenerator) {
	switch v := val.(type) {
	case *parser.LiteralExpr:
		compileLiteralExpr(vm, v, reg)
	case *parser.VariableExpr:
		compileVariableExpr(vm, v, reg, args, paramIdx)
	case *parser.UnaryExpr:
		compileUnaryExpr(vm, v, reg)
	default:
		if gen == nil {
			vm.AddOp(vdbe.OpNull, 0, reg, 0)
			return
		}
		gen.SetParamIndex(*paramIdx)
		exprReg, err := gen.GenerateExpr(val)
		*paramIdx = gen.ParamIndex()
		if err != nil {
			vm.AddOp(vdbe.OpNull, 0, reg, 0)
			return
		}
		if exprReg != reg {
			vm.AddOp(vdbe.OpCopy, exprReg, reg, 0)
		}
	}
}

// compileVariableExpr compiles a variable (parameter placeholder) expression.
func compileVariableExpr(vm *vdbe.VDBE, v *parser.VariableExpr, reg int, args []driver.NamedValue, paramIdx *int) {
	if *paramIdx >= len(args) {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}
	arg := args[*paramIdx]
	*paramIdx++
	compileArgValue(vm, arg.Value, reg)
}

// compileUnaryExpr compiles a unary expression (currently only handles negation).
func compileUnaryExpr(vm *vdbe.VDBE, v *parser.UnaryExpr, reg int) {
	if v.Op != parser.OpNeg {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}

	if compileNegatedLiteral(vm, v, reg) {
		return
	}

	vm.AddOp(vdbe.OpNull, 0, reg, 0)
}

// compileNegatedLiteral attempts to compile a negated literal expression.
// Returns true if successful, false otherwise.
func compileNegatedLiteral(vm *vdbe.VDBE, v *parser.UnaryExpr, reg int) bool {
	lit, ok := v.Expr.(*parser.LiteralExpr)
	if !ok {
		return false
	}

	switch lit.Type {
	case parser.LiteralInteger:
		intVal, _ := strconv.ParseInt(lit.Value, 10, 64)
		vm.AddOp(vdbe.OpInteger, int(-intVal), reg, 0)
		return true
	case parser.LiteralFloat:
		floatVal, _ := strconv.ParseFloat(lit.Value, 64)
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, -floatVal)
		return true
	}

	return false
}

// compileLiteralExpr emits the VDBE opcode for a literal value into register reg.
// Float literals use OpReal, String and Blob literals use OpString8, Integer and
// Null have dedicated opcodes. CC=4
func compileLiteralExpr(vm *vdbe.VDBE, expr *parser.LiteralExpr, reg int) {
	switch expr.Type {
	case parser.LiteralInteger:
		var intVal int64
		fmt.Sscanf(expr.Value, "%d", &intVal)
		vm.AddOp(vdbe.OpInteger, int(intVal), reg, 0)
	case parser.LiteralNull:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case parser.LiteralFloat:
		floatVal, _ := strconv.ParseFloat(expr.Value, 64)
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, floatVal)
	case parser.LiteralString:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, expr.Value)
	case parser.LiteralBlob:
		compileBlobLiteral(vm, expr.Value, reg)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// compileBlobLiteral decodes a hex blob literal (e.g. X'CAFE') and emits
// an OpBlob instruction so the value is stored as []byte, not string. CC=2
func compileBlobLiteral(vm *vdbe.VDBE, raw string, reg int) {
	hexStr := raw
	if len(hexStr) >= 3 && (hexStr[0] == 'X' || hexStr[0] == 'x') &&
		hexStr[1] == '\'' && hexStr[len(hexStr)-1] == '\'' {
		hexStr = hexStr[2 : len(hexStr)-1]
	}
	blobData, err := hex.DecodeString(strings.ToUpper(hexStr))
	if err != nil {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}
	vm.AddOpWithP4Blob(vdbe.OpBlob, len(blobData), reg, 0, blobData)
}

// compileArgValue emits the VDBE opcode for a concrete bound-parameter value
// into register reg. CC=2
func compileArgValue(vm *vdbe.VDBE, val driver.Value, reg int) {
	if val == nil {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}

	handler := getArgValueHandler(val)
	handler(vm, val, reg)
}

// argValueHandler is a function type for handling specific argument types.
type argValueHandler func(vm *vdbe.VDBE, val driver.Value, reg int)

// getArgValueHandler returns the appropriate handler for the given value type.
func getArgValueHandler(val driver.Value) argValueHandler {
	switch val.(type) {
	case bool:
		return compileBoolArg
	case int:
		return compileIntArg
	case int64:
		return compileInt64Arg
	case float64:
		return compileFloat64Arg
	case string:
		return compileStringArg
	case []byte:
		return compileBlobArg
	default:
		return compileDefaultArg
	}
}

// compileBoolArg compiles a boolean argument value.
func compileBoolArg(vm *vdbe.VDBE, val driver.Value, reg int) {
	intVal := 0
	if val.(bool) {
		intVal = 1
	}
	vm.AddOp(vdbe.OpInteger, intVal, reg, 0)
}

// compileIntArg compiles an int argument value.
func compileIntArg(vm *vdbe.VDBE, val driver.Value, reg int) {
	vm.AddOp(vdbe.OpInteger, val.(int), reg, 0)
}

// compileInt64Arg compiles an int64 argument value.
func compileInt64Arg(vm *vdbe.VDBE, val driver.Value, reg int) {
	vm.AddOp(vdbe.OpInteger, int(val.(int64)), reg, 0)
}

// compileFloat64Arg compiles a float64 argument value.
func compileFloat64Arg(vm *vdbe.VDBE, val driver.Value, reg int) {
	vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, val.(float64))
}

// compileStringArg compiles a string argument value.
func compileStringArg(vm *vdbe.VDBE, val driver.Value, reg int) {
	vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, val.(string))
}

// compileBlobArg compiles a []byte argument value.
func compileBlobArg(vm *vdbe.VDBE, val driver.Value, reg int) {
	v := val.([]byte)
	vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), reg, 0, v)
}

// compileDefaultArg compiles an unknown argument value.
func compileDefaultArg(vm *vdbe.VDBE, val driver.Value, reg int) {
	vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, fmt.Sprintf("%v", val))
}

// ============================================================================
// UPDATE Compilation
// ============================================================================

// compileUpdate compiles an UPDATE statement.
func (s *Stmt) compileUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Look up table in schema (supports cross-database qualified names)
	table, db, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}
	s.setVdbeContextForDatabase(vm, db)

	// Route virtual tables through vtab interface
	if s.isVirtualTable(table) {
		return s.compileVTabUpdate(vm, stmt, table, args)
	}

	if err := s.validateUpdateColumns(table, stmt); err != nil {
		return nil, err
	}

	// Dispatch to UPDATE...FROM when a FROM clause is present
	if stmt.From != nil {
		return s.compileUpdateFrom(vm, stmt, table, args)
	}

	// Build update map and column list
	updateMap, updatedColumns := s.buildUpdateMap(stmt)

	// Setup VDBE and code generator
	gen, numRecordCols := s.setupUpdateVDBE(vm, table, stmt)

	// Set up RETURNING columns before the loop
	if len(stmt.Returning) > 0 {
		retCols, retNames := expandReturningColumns(stmt.Returning, table)
		stmt.Returning = retCols
		vm.ResultCols = retNames
	}

	// Emit update loop
	rewindAddr := s.emitUpdateLoop(vm, stmt, table, updateMap, updatedColumns, numRecordCols, gen, args)

	// Close and finalize
	s.finalizeUpdate(vm, rewindAddr)

	return vm, nil
}

// validateUpdateColumns checks that all SET target columns exist and are not generated.
func (s *Stmt) validateUpdateColumns(table *schema.Table, stmt *parser.UpdateStmt) error {
	for _, assign := range stmt.Sets {
		colIdx := table.GetColumnIndexWithRowidAliases(assign.Column)
		if colIdx == -1 {
			return fmt.Errorf("no such column: %s", assign.Column)
		}
		if colIdx >= 0 && colIdx < len(table.Columns) && table.Columns[colIdx].Generated {
			return fmt.Errorf("cannot UPDATE generated column \"%s\"", assign.Column)
		}
	}
	return nil
}

// compileUpdateFrom compiles an UPDATE...FROM statement by materialising the
// FROM query and applying updates to matching rows. CC=8
func (s *Stmt) compileUpdateFrom(vm *vdbe.VDBE, stmt *parser.UpdateStmt,
	table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {

	// Build a SELECT that retrieves the values needed from the FROM tables.
	// We construct: SELECT <set-exprs>, <target>.rowid FROM <from-tables> WHERE <where>
	selStmt := s.buildUpdateFromSelect(stmt)

	subVM := vdbe.New()
	subVM.Ctx = vm.Ctx
	compiled, err := s.compileSelect(subVM, selStmt, args)
	if err != nil {
		return nil, fmt.Errorf("UPDATE...FROM select: %w", err)
	}

	numResultCols := len(stmt.Sets) + 1 // +1 for rowid
	rows, err := s.collectRows(compiled, numResultCols, "UPDATE...FROM")
	if err != nil {
		return nil, fmt.Errorf("UPDATE...FROM materialise: %w", err)
	}

	return s.applyUpdateFromRows(vm, table, stmt, rows)
}

// buildUpdateFromSelect builds a SELECT statement from the UPDATE...FROM clause.
// The SELECT returns the SET expressions plus the target table's rowid.
func (s *Stmt) buildUpdateFromSelect(stmt *parser.UpdateStmt) *parser.SelectStmt {
	var cols []parser.ResultColumn
	for _, assign := range stmt.Sets {
		cols = append(cols, parser.ResultColumn{Expr: assign.Value, Alias: assign.Column})
	}
	// Add rowid of the target table
	cols = append(cols, parser.ResultColumn{
		Expr: &parser.IdentExpr{Table: stmt.Table, Name: "rowid"},
	})

	// Merge the target table into the FROM clause
	from := s.mergeTargetIntoFrom(stmt)

	return &parser.SelectStmt{
		Columns: cols,
		From:    from,
		Where:   stmt.Where,
	}
}

// mergeTargetIntoFrom ensures the UPDATE target table is present in the FROM clause.
func (s *Stmt) mergeTargetIntoFrom(stmt *parser.UpdateStmt) *parser.FromClause {
	from := stmt.From
	// Check if the target table is already listed in FROM
	for _, t := range from.Tables {
		if strings.EqualFold(t.TableName, stmt.Table) {
			return from
		}
	}
	// Prepend the target table
	targetRef := parser.TableOrSubquery{Schema: stmt.Schema, TableName: stmt.Table}
	merged := &parser.FromClause{
		Tables: append([]parser.TableOrSubquery{targetRef}, from.Tables...),
		Joins:  from.Joins,
	}
	return merged
}

// applyUpdateFromRows applies materialised rows to the target table. CC=7
func (s *Stmt) applyUpdateFromRows(vm *vdbe.VDBE, table *schema.Table,
	stmt *parser.UpdateStmt, rows [][]interface{}) (*vdbe.VDBE, error) {

	numRecordCols := countNonRowidCols(table)

	vm.AllocMemory(numRecordCols*2 + 30)
	vm.AllocCursors(1)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	gen := s.newTableCodeGenerator(vm, stmt.Table, table, 0, nil)

	for _, row := range rows {
		s.emitUpdateFromSingleRow(vm, table, stmt, row, numRecordCols, gen)
	}

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// emitUpdateFromSingleRow emits bytecode to update one row from materialised data. CC=9
func (s *Stmt) emitUpdateFromSingleRow(vm *vdbe.VDBE, table *schema.Table,
	stmt *parser.UpdateStmt, row []interface{}, numRecordCols int, gen *expr.CodeGenerator) {

	numSets := len(stmt.Sets)
	rowid, ok := dmlToInt64(row[numSets])
	if !ok {
		return
	}

	// Seek to the target row by rowid
	rowidReg := gen.AllocReg()
	vm.AddOp(vdbe.OpInteger, int(rowid), rowidReg, 0)
	seekAddr := vm.AddOp(vdbe.OpSeekRowid, 0, 0, rowidReg)

	// Build updated column set
	updateSet := make(map[string]interface{}, numSets)
	for i, assign := range stmt.Sets {
		updateSet[assign.Column] = row[i]
	}

	// Build the new record
	recordStartReg := gen.AllocRegs(numRecordCols)
	reg := recordStartReg
	for colIdx, col := range table.Columns {
		if schemaColIsRowidForTable(table, col) {
			continue
		}
		if val, updated := updateSet[col.Name]; updated {
			loadValueIntoReg(vm, val, reg)
		} else {
			recordIdx := schemaRecordIdxForTable(table, colIdx)
			vm.AddOp(vdbe.OpColumn, 0, recordIdx, reg)
		}
		reg++
	}

	// Replace row
	s.emitUpdateRowReplacement(vm, recordStartReg, numRecordCols, rowidReg, table.Name, gen)

	// Fix seek miss target
	vm.Program[seekAddr].P2 = vm.NumOps()
}

// loadValueIntoReg emits an opcode to load a Go value into a VDBE register.
func loadValueIntoReg(vm *vdbe.VDBE, val interface{}, reg int) {
	switch v := val.(type) {
	case nil:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case int64:
		vm.AddOp(vdbe.OpInteger, int(v), reg, 0)
	case float64:
		addr := vm.AddOp(vdbe.OpReal, 0, reg, 0)
		vm.Program[addr].P4.R = v
	case string:
		addr := vm.AddOp(vdbe.OpString8, 0, reg, 0)
		vm.Program[addr].P4.Z = v
	case []byte:
		addr := vm.AddOp(vdbe.OpBlob, len(v), reg, 0)
		vm.Program[addr].P4.Z = string(v)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// dmlToInt64 converts an interface value to int64 for rowid usage.
func dmlToInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case float64:
		return int64(val), true
	case int:
		return int64(val), true
	default:
		return 0, false
	}
}

// buildUpdateMap builds the update map and column list from UPDATE statement.
func (s *Stmt) buildUpdateMap(stmt *parser.UpdateStmt) (map[string]parser.Expression, []string) {
	updateMap := make(map[string]parser.Expression)
	updatedColumns := make([]string, 0, len(stmt.Sets))
	for _, assign := range stmt.Sets {
		updateMap[assign.Column] = assign.Value
		updatedColumns = append(updatedColumns, assign.Column)
	}
	return updateMap, updatedColumns
}

// setupUpdateVDBE initializes VDBE and code generator for UPDATE.
func (s *Stmt) setupUpdateVDBE(vm *vdbe.VDBE, table *schema.Table, stmt *parser.UpdateStmt) (*expr.CodeGenerator, int) {
	// Count non-rowid columns (for WITHOUT ROWID tables, all columns are in the record)
	numRecordCols := 0
	for _, col := range table.Columns {
		if !schemaColIsRowidForTable(table, col) {
			numRecordCols++
		}
	}

	// Allocate resources (extra space for OLD row snapshot when triggers exist)
	vm.AllocMemory(numRecordCols*2 + 30)
	vm.AllocCursors(1)

	// Initialize program
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	// Create and configure code generator
	gen := s.newTableCodeGenerator(vm, stmt.Table, table, 0, nil)

	return gen, numRecordCols
}

// emitUpdateLoop generates the main UPDATE loop bytecode.
func (s *Stmt) emitUpdateLoop(vm *vdbe.VDBE, stmt *parser.UpdateStmt, table *schema.Table,
	updateMap map[string]parser.Expression, updatedCols []string, numRecordCols int,
	gen *expr.CodeGenerator, args []driver.NamedValue) int {

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Prepare args
	setParamCount := countParams(stmt.Sets)
	argValues := extractArgValues(args)

	// Get rowid
	rowidReg := gen.AllocReg()
	vm.AddOp(vdbe.OpRowid, 0, rowidReg, 0)

	// Compile WHERE clause if present
	skipAddr := s.emitUpdateWhereClause(vm, stmt, gen, argValues, setParamCount)

	// Reset args for SET clause
	gen.SetArgs(argValues)

	// Snapshot OLD row into registers for triggers (before record build)
	hasTriggers := s.tableHasTriggers(stmt.Table)
	oldRowStartReg := 0
	oldRowidReg := 0
	if hasTriggers {
		oldRowidReg = gen.AllocReg()
		oldRowStartReg = gen.AllocRegs(numRecordCols)
		emitOldRowSnapshot(vm, table, 0, oldRowidReg, oldRowStartReg, numRecordCols)
	}

	// Emit BEFORE UPDATE trigger (P1=1 for UPDATE event)
	var beforeAddr int
	if hasTriggers {
		beforeAddr = vm.AddOp(vdbe.OpTriggerBefore, 1, 0, oldRowStartReg)
		vm.Program[beforeAddr].P4.Z = stmt.Table
		vm.Program[beforeAddr].P4.P = updatedCols
	}

	// Build updated record
	recordStartReg, newRowidReg := s.emitUpdateRecordBuild(vm, table, updateMap, numRecordCols, gen)

	// Use new rowid if IPK is being updated, otherwise use old rowid
	effectiveRowidReg := rowidReg
	if newRowidReg != 0 {
		effectiveRowidReg = newRowidReg
	}

	// Create record, delete old, insert new
	s.emitUpdateRowReplacement(vm, recordStartReg, numRecordCols, effectiveRowidReg, table.Name, gen)

	// Emit AFTER UPDATE trigger with OLD in P3, NEW in P5
	if hasTriggers {
		afterAddr := vm.AddOp(vdbe.OpTriggerAfter, 1, 0, oldRowStartReg)
		vm.Program[afterAddr].P4.Z = stmt.Table
		vm.Program[afterAddr].P4.P = updatedCols
		vm.Program[afterAddr].P5 = uint16(recordStartReg)
		// Also set P5 on BEFORE trigger to point to NEW record
		// (NEW may not be fully populated yet for BEFORE, but set for consistency)
		vm.Program[beforeAddr].P5 = uint16(recordStartReg)
		vm.Program[beforeAddr].P2 = vm.NumOps()
	}

	// Emit RETURNING row after the update
	if len(stmt.Returning) > 0 {
		vm.AddOp(vdbe.OpSeekRowid, 0, 0, effectiveRowidReg)
		emitReturningRow(vm, stmt.Returning, len(stmt.Returning), gen) //nolint:errcheck
	}

	// Fix WHERE skip target
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Loop to next row
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)

	return rewindAddr
}

// emitUpdateWhereClause compiles WHERE clause for UPDATE.
func (s *Stmt) emitUpdateWhereClause(vm *vdbe.VDBE, stmt *parser.UpdateStmt,
	gen *expr.CodeGenerator, argValues []interface{}, setParamCount int) int {

	if stmt.Where == nil {
		return 0
	}

	// Set args with offset for WHERE clause
	whereArgs := argValues[setParamCount:]
	gen.SetArgs(whereArgs)

	// Generate WHERE expression
	whereReg, err := gen.GenerateExpr(stmt.Where)
	if err != nil {
		return 0
	}

	// Skip update if WHERE is false
	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
}

// emitUpdateRecordBuild builds the updated record in registers.
// Returns (recordStartReg, newRowidReg) where newRowidReg is non-zero
// if INTEGER PRIMARY KEY is being updated.
func (s *Stmt) emitUpdateRecordBuild(vm *vdbe.VDBE, table *schema.Table,
	updateMap map[string]parser.Expression, numRecordCols int,
	gen *expr.CodeGenerator) (int, int) {

	recordStartReg := gen.AllocRegs(numRecordCols)
	reg := recordStartReg
	newRowidReg := 0

	for colIdx, col := range table.Columns {
		if schemaColIsRowidForTable(table, col) {
			// Check if IPK is being updated
			if updateExpr, isUpdated := updateMap[col.Name]; isUpdated {
				valReg, _ := gen.GenerateExpr(updateExpr)
				newRowidReg = gen.AllocReg()
				vm.AddOp(vdbe.OpCopy, valReg, newRowidReg, 0)
			}
			continue
		}

		s.emitUpdateColumnValue(vm, col, colIdx, table, updateMap, reg, gen)
		reg++
	}

	return recordStartReg, newRowidReg
}

// emitUpdateColumnValue emits bytecode for a single column during UPDATE record build.
// Generated columns re-evaluate their expression; normal columns use SET or existing value.
func (s *Stmt) emitUpdateColumnValue(vm *vdbe.VDBE, col *schema.Column, colIdx int,
	table *schema.Table, updateMap map[string]parser.Expression, reg int,
	gen *expr.CodeGenerator) {

	if col.Generated {
		genExpr := generatedExprForColumn(col)
		valReg, err := gen.GenerateExpr(genExpr)
		if err != nil {
			vm.AddOp(vdbe.OpNull, 0, reg, 0)
			return
		}
		vm.AddOp(vdbe.OpCopy, valReg, reg, 0)
		return
	}

	if updateExpr, isUpdated := updateMap[col.Name]; isUpdated {
		valReg, _ := gen.GenerateExpr(updateExpr)
		vm.AddOp(vdbe.OpCopy, valReg, reg, 0)
	} else {
		recordIdx := schemaRecordIdxForTable(table, colIdx)
		vm.AddOp(vdbe.OpColumn, 0, recordIdx, reg)
	}
}

// emitUpdateRowReplacement emits bytecode to replace the row.
func (s *Stmt) emitUpdateRowReplacement(vm *vdbe.VDBE, recordStartReg int,
	numRecordCols int, rowidReg int, tableName string, gen *expr.CodeGenerator) {

	resultReg := gen.AllocReg()
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)
	delAddr := vm.AddOp(vdbe.OpDelete, 0, 0, 0)
	vm.Program[delAddr].P4.Z = tableName
	vm.Program[delAddr].P5 = 1 // mark as update delete for FK handling

	insertAddr := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidReg)
	vm.Program[insertAddr].P4.I = 1 // Don't double-count in NumChanges
	vm.Program[insertAddr].P4.Z = tableName
}

// finalizeUpdate closes cursor and adds halt instruction.
func (s *Stmt) finalizeUpdate(vm *vdbe.VDBE, rewindAddr int) {
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr
}

// countParams counts the number of parameter placeholders in SET clauses.
func countParams(sets []parser.Assignment) int {
	count := 0
	for _, assign := range sets {
		count += countExprParams(assign.Value)
	}
	return count
}

// countExprParams counts parameter placeholders in an expression.
func countExprParams(e parser.Expression) int {
	if e == nil {
		return 0
	}
	switch expr := e.(type) {
	case *parser.VariableExpr:
		return 1
	case *parser.BinaryExpr:
		return countExprParams(expr.Left) + countExprParams(expr.Right)
	case *parser.UnaryExpr:
		return countExprParams(expr.Expr)
	case *parser.FunctionExpr:
		count := 0
		for _, arg := range expr.Args {
			count += countExprParams(arg)
		}
		return count
	default:
		return 0
	}
}

// ============================================================================
// DELETE Compilation
// ============================================================================

// compileDelete compiles a DELETE statement.
func (s *Stmt) compileDelete(vm *vdbe.VDBE, stmt *parser.DeleteStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Look up table in schema (supports cross-database qualified names)
	table, db, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}
	s.setVdbeContextForDatabase(vm, db)

	// Route virtual tables through vtab interface
	if s.isVirtualTable(table) {
		return s.compileVTabDelete(vm, stmt, table, args)
	}

	hasTriggers := s.tableHasTriggers(stmt.Table)

	// Allocate extra registers for OLD row snapshot when triggers exist
	numRecordCols := countNonRowidCols(table)
	memSize := 10
	oldRowStartReg := 0
	if hasTriggers || len(stmt.Returning) > 0 {
		// Layout: reg 1 = rowid, regs 2..N+1 = record columns
		oldRowStartReg = 2
		memSize = numRecordCols + 20
	}
	vm.AllocMemory(memSize)
	vm.AllocCursors(1)

	// Expand and set up RETURNING columns
	var retGen *expr.CodeGenerator
	if len(stmt.Returning) > 0 {
		retCols, retNames := expandReturningColumns(stmt.Returning, table)
		stmt.Returning = retCols
		vm.ResultCols = retNames
		retGen = s.newTableCodeGenerator(vm, stmt.Table, table, 0, nil)
	}

	return s.emitDeleteBody(vm, stmt, table, hasTriggers, numRecordCols, oldRowStartReg, retGen, args)
}

// emitDeleteBody emits the DELETE loop and finalization opcodes.
func (s *Stmt) emitDeleteBody(vm *vdbe.VDBE, stmt *parser.DeleteStmt, table *schema.Table,
	hasTriggers bool, numRecordCols, oldRowStartReg int, retGen *expr.CodeGenerator,
	args []driver.NamedValue) (*vdbe.VDBE, error) {

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	if stmt.Where != nil {
		stmt.Where = s.materializeSubqueries(vm, stmt.Where)
	}

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
	skipAddr := s.emitDeleteWhereClause(vm, stmt, table, args)

	if hasTriggers {
		emitOldRowSnapshot(vm, table, 0, 1, oldRowStartReg, numRecordCols)
	}

	// Emit RETURNING before delete (cursor still points at the row)
	if retGen != nil {
		emitReturningRow(vm, stmt.Returning, len(stmt.Returning), retGen) //nolint:errcheck
	}

	var beforeAddr int
	if hasTriggers {
		beforeAddr = vm.AddOp(vdbe.OpTriggerBefore, 2, 0, oldRowStartReg)
		vm.Program[beforeAddr].P4.Z = stmt.Table
	}

	delAddr := vm.AddOp(vdbe.OpDelete, 0, 0, 0)
	vm.Program[delAddr].P4.Z = table.Name

	if hasTriggers {
		afterAddr := vm.AddOp(vdbe.OpTriggerAfter, 2, 0, oldRowStartReg)
		vm.Program[afterAddr].P4.Z = stmt.Table
		vm.Program[beforeAddr].P2 = vm.NumOps()
	}

	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// emitDeleteWhereClause compiles and emits the WHERE clause for DELETE.
// Returns the skip address (to be fixed up later), or 0 if no WHERE clause.
func (s *Stmt) emitDeleteWhereClause(vm *vdbe.VDBE, stmt *parser.DeleteStmt,
	table *schema.Table, args []driver.NamedValue) int {

	if stmt.Where == nil {
		return 0
	}

	gen := s.newTableCodeGenerator(vm, stmt.Table, table, 0, args)

	whereReg, err := gen.GenerateExpr(stmt.Where)
	if err != nil {
		return 0
	}

	return vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
}

// emitOldRowSnapshot emits opcodes to save the current cursor row into
// registers so trigger bodies can access OLD column values even after
// the row has been deleted or replaced. rowidReg receives the rowid,
// and record columns are stored starting at recordStartReg.
func emitOldRowSnapshot(vm *vdbe.VDBE, table *schema.Table,
	cursorIdx int, rowidReg int, recordStartReg int, numRecordCols int) {

	vm.AddOp(vdbe.OpRowid, cursorIdx, rowidReg, 0)
	for i := 0; i < numRecordCols; i++ {
		vm.AddOp(vdbe.OpColumn, cursorIdx, i, recordStartReg+i)
	}
}

// countNonRowidCols returns the number of non-rowid columns (record columns)
// in the table.
func countNonRowidCols(table *schema.Table) int {
	if table.WithoutRowID {
		return len(table.Columns)
	}
	count := 0
	for _, col := range table.Columns {
		if !schemaColIsRowid(col) {
			count++
		}
	}
	return count
}

// ============================================================================
// Subquery Pre-materialisation for DML
// ============================================================================

// materializeSubqueries walks an expression tree and replaces SubqueryExpr
// nodes with LiteralExpr nodes by executing the subquery once.  This prevents
// scalar subqueries from being re-evaluated inside a DML loop.
func (s *Stmt) materializeSubqueries(vm *vdbe.VDBE, e parser.Expression) parser.Expression {
	switch v := e.(type) {
	case *parser.SubqueryExpr:
		return s.evalSubqueryToLiteral(vm, v)
	case *parser.BinaryExpr:
		return s.materializeBinaryExpr(vm, v)
	case *parser.UnaryExpr:
		return s.materializeUnaryExpr(vm, v)
	case *parser.ParenExpr:
		return s.materializeParenExpr(vm, v)
	default:
		return e
	}
}

// evalSubqueryToLiteral executes a scalar subquery and returns a LiteralExpr.
func (s *Stmt) evalSubqueryToLiteral(vm *vdbe.VDBE, sq *parser.SubqueryExpr) parser.Expression {
	if sq.Select == nil {
		return sq
	}
	subVM := vdbe.New()
	subVM.Ctx = vm.Ctx
	compiled, err := s.compileSelect(subVM, sq.Select, nil)
	if err != nil {
		return sq // fallback: keep original
	}
	numCols := len(compiled.ResultCols)
	if numCols == 0 {
		numCols = 1
	}
	rows, err := s.collectRows(compiled, numCols, "DML subquery materialise")
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	}
	return goValueToLiteral(rows[0][0])
}

// goValueToLiteral converts a Go value to a parser.LiteralExpr.
func goValueToLiteral(val interface{}) *parser.LiteralExpr {
	switch v := val.(type) {
	case nil:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	case int64:
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: strconv.FormatInt(v, 10)}
	case float64:
		return &parser.LiteralExpr{Type: parser.LiteralFloat, Value: strconv.FormatFloat(v, 'g', -1, 64)}
	case string:
		return &parser.LiteralExpr{Type: parser.LiteralString, Value: v}
	default:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	}
}

// materializeBinaryExpr recursively materialises subqueries in a BinaryExpr.
func (s *Stmt) materializeBinaryExpr(vm *vdbe.VDBE, v *parser.BinaryExpr) parser.Expression {
	newL := s.materializeSubqueries(vm, v.Left)
	newR := s.materializeSubqueries(vm, v.Right)
	if newL == v.Left && newR == v.Right {
		return v
	}
	cp := *v
	cp.Left = newL
	cp.Right = newR
	return &cp
}

// materializeUnaryExpr recursively materialises subqueries in a UnaryExpr.
func (s *Stmt) materializeUnaryExpr(vm *vdbe.VDBE, v *parser.UnaryExpr) parser.Expression {
	newE := s.materializeSubqueries(vm, v.Expr)
	if newE == v.Expr {
		return v
	}
	cp := *v
	cp.Expr = newE
	return &cp
}

// materializeParenExpr recursively materialises subqueries in a ParenExpr.
func (s *Stmt) materializeParenExpr(vm *vdbe.VDBE, v *parser.ParenExpr) parser.Expression {
	newE := s.materializeSubqueries(vm, v.Expr)
	if newE == v.Expr {
		return v
	}
	cp := *v
	cp.Expr = newE
	return &cp
}

// ============================================================================
// UPSERT (ON CONFLICT) Helpers
// ============================================================================

// getConflictMode returns the conflict resolution mode based on the OnConflict clause.
// Maps parser.OnConflictClause to VDBE P4.I values
func getConflictMode(onConflict parser.OnConflictClause) int32 {
	switch onConflict {
	case parser.OnConflictIgnore:
		return 3
	case parser.OnConflictReplace:
		return 4
	case parser.OnConflictRollback:
		return 1
	case parser.OnConflictFail:
		return 2
	default:
		return 0 // OnConflictAbort is the default
	}
}

// compileInsertUpsert compiles an INSERT with ON CONFLICT (UPSERT) clause.
// For DO NOTHING, it delegates to INSERT OR IGNORE.
// For DO UPDATE, it rewrites excluded.col references and runs an UPDATE
// on conflict followed by INSERT OR IGNORE for new rows. CC=10
func (s *Stmt) compileInsertUpsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	if stmt.Upsert.Action == parser.ConflictDoNothing {
		return s.compileUpsertDoNothing(vm, stmt, args)
	}
	return s.compileUpsertDoUpdate(vm, stmt, args)
}

// compileUpsertDoNothing compiles UPSERT with DO NOTHING as INSERT OR IGNORE.
func (s *Stmt) compileUpsertDoNothing(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	saved := stmt.OnConflict
	stmt.OnConflict = parser.OnConflictIgnore
	savedUpsert := stmt.Upsert
	stmt.Upsert = nil
	result, err := s.compileInsert(vm, stmt, args)
	stmt.OnConflict = saved
	stmt.Upsert = savedUpsert
	return result, err
}

// compileUpsertDoUpdate compiles UPSERT with DO UPDATE by building an UPDATE
// statement with excluded references replaced by the INSERT values, then
// running INSERT OR IGNORE for new rows. CC=10
func (s *Stmt) compileUpsertDoUpdate(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	table, _, _, ok := s.conn.dbRegistry.ResolveTable(stmt.Schema, stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Build column-to-value map from the INSERT for excluded references
	colValueMap := buildInsertColumnValueMap(stmt, table)

	// Build an UPDATE statement from the DO UPDATE clause
	updateStmt := buildUpsertUpdateStmt(stmt, table, colValueMap)

	// Compile and execute the UPDATE first
	updateVM := vdbe.New()
	updateVM.Ctx = vm.Ctx
	if _, err := s.compileUpdate(updateVM, updateStmt, args); err != nil {
		return nil, fmt.Errorf("upsert update: %w", err)
	}
	if err := updateVM.Run(); err != nil {
		return nil, fmt.Errorf("upsert update exec: %w", err)
	}

	// Now do INSERT OR IGNORE for new rows
	return s.compileUpsertDoNothing(vm, stmt, args)
}

// buildInsertColumnValueMap maps column names to their INSERT value expressions.
func buildInsertColumnValueMap(stmt *parser.InsertStmt, table *schema.Table) map[string]parser.Expression {
	m := make(map[string]parser.Expression)
	cols := stmt.Columns
	if len(cols) == 0 {
		cols = allTableColumnNames(table)
	}
	if len(stmt.Values) > 0 {
		for i, col := range cols {
			if i < len(stmt.Values[0]) {
				m[col] = stmt.Values[0][i]
			}
		}
	}
	return m
}

// buildUpsertUpdateStmt constructs an UPDATE statement from the UPSERT clause,
// replacing excluded.col references with the INSERT values.
func buildUpsertUpdateStmt(stmt *parser.InsertStmt, table *schema.Table, colValueMap map[string]parser.Expression) *parser.UpdateStmt {
	sets := make([]parser.Assignment, len(stmt.Upsert.Update.Sets))
	for i, a := range stmt.Upsert.Update.Sets {
		sets[i] = parser.Assignment{
			Column: a.Column,
			Value:  replaceExcludedRefs(a.Value, colValueMap),
		}
	}

	// Build WHERE clause from conflict target columns
	var where parser.Expression
	if stmt.Upsert.Target != nil {
		where = buildConflictWhere(stmt.Upsert.Target, colValueMap)
	}
	// Apply additional WHERE from DO UPDATE if present
	if stmt.Upsert.Update.Where != nil {
		extraWhere := replaceExcludedRefs(stmt.Upsert.Update.Where, colValueMap)
		if where != nil {
			where = &parser.BinaryExpr{Left: where, Op: parser.OpAnd, Right: extraWhere}
		} else {
			where = extraWhere
		}
	}

	return &parser.UpdateStmt{
		Schema: stmt.Schema,
		Table:  stmt.Table,
		Sets:   sets,
		Where:  where,
	}
}

// buildConflictWhere builds a WHERE clause matching the conflict target columns
// to their INSERT values.
func buildConflictWhere(target *parser.ConflictTarget, colValueMap map[string]parser.Expression) parser.Expression {
	var result parser.Expression
	for _, col := range target.Columns {
		val, ok := colValueMap[col.Column]
		if !ok {
			continue
		}
		eq := &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: col.Column},
			Op:    parser.OpEq,
			Right: val,
		}
		if result == nil {
			result = eq
		} else {
			result = &parser.BinaryExpr{Left: result, Op: parser.OpAnd, Right: eq}
		}
	}
	return result
}

// replaceExcludedRefs replaces excluded.col references with the actual INSERT values.
func replaceExcludedRefs(e parser.Expression, colValueMap map[string]parser.Expression) parser.Expression {
	if e == nil {
		return nil
	}
	switch expr := e.(type) {
	case *parser.IdentExpr:
		if strings.EqualFold(expr.Table, "excluded") {
			if val, ok := colValueMap[expr.Name]; ok {
				return val
			}
		}
		return expr
	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:  replaceExcludedRefs(expr.Left, colValueMap),
			Op:    expr.Op,
			Right: replaceExcludedRefs(expr.Right, colValueMap),
		}
	case *parser.UnaryExpr:
		return &parser.UnaryExpr{
			Op:   expr.Op,
			Expr: replaceExcludedRefs(expr.Expr, colValueMap),
		}
	case *parser.FunctionExpr:
		newArgs := make([]parser.Expression, len(expr.Args))
		for i, arg := range expr.Args {
			newArgs[i] = replaceExcludedRefs(arg, colValueMap)
		}
		return &parser.FunctionExpr{Name: expr.Name, Args: newArgs, Distinct: expr.Distinct}
	default:
		return e
	}
}

// allTableColumnNames returns all column names from the table schema.
func allTableColumnNames(table *schema.Table) []string {
	names := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		names[i] = col.Name
	}
	return names
}

// expandInsertDefaults expands a partial-column INSERT into a full-column
// INSERT by filling in DEFAULT values for omitted columns. When the INSERT
// already specifies all columns, the original values are returned unchanged.
// If a rowid alias (rowid/_rowid_/oid) is in stmt.Columns but not a real
// table column, it is appended as an extra trailing element in each row.
func expandInsertDefaults(stmt *parser.InsertStmt, table *schema.Table) [][]parser.Expression {
	if len(stmt.Columns) == 0 {
		return replaceGeneratedValues(stmt.Values, table)
	}
	specifiedSet := buildColumnSet(stmt.Columns)
	rowidStmtIdx := findRowidAliasInColumns(stmt.Columns, table)
	expanded := make([][]parser.Expression, len(stmt.Values))
	for i, row := range stmt.Values {
		full := expandSingleRow(row, stmt.Columns, specifiedSet, table)
		if rowidStmtIdx >= 0 && rowidStmtIdx < len(row) {
			full = append(full, row[rowidStmtIdx])
		}
		expanded[i] = full
	}
	return expanded
}

// findRowidAliasInColumns returns the index in cols of an explicit rowid
// alias that is NOT a real table column, or -1 if none.
func findRowidAliasInColumns(cols []string, table *schema.Table) int {
	for i, c := range cols {
		lower := strings.ToLower(c)
		if lower == "rowid" || lower == "_rowid_" || lower == "oid" {
			if table.GetColumnIndex(c) < 0 {
				return i
			}
		}
	}
	return -1
}

// buildColumnSet creates a set of lower-cased column names for fast lookup.
func buildColumnSet(cols []string) map[string]int {
	m := make(map[string]int, len(cols))
	for i, c := range cols {
		m[strings.ToLower(c)] = i
	}
	return m
}

// expandSingleRow expands one VALUES row to cover all table columns.
// Generated columns use their generation expression regardless of user input.
func expandSingleRow(row []parser.Expression, insertCols []string, specifiedSet map[string]int, table *schema.Table) []parser.Expression {
	full := make([]parser.Expression, len(table.Columns))
	for i, col := range table.Columns {
		if col.Generated {
			full[i] = generatedExprForColumn(col)
			continue
		}
		idx, ok := specifiedSet[strings.ToLower(col.Name)]
		if ok && idx < len(row) {
			full[i] = row[idx]
		} else {
			full[i] = defaultExprForColumn(col)
		}
	}
	return full
}

// generatedExprForColumn parses and returns the generation expression for a
// generated column, or NULL if the expression cannot be parsed.
func generatedExprForColumn(col *schema.Column) parser.Expression {
	if col.GeneratedExpr == "" {
		return &parser.LiteralExpr{Type: parser.LiteralNull}
	}
	p := parser.NewParser(col.GeneratedExpr)
	e, err := p.ParseExpression()
	if err != nil {
		return &parser.LiteralExpr{Type: parser.LiteralNull}
	}
	return e
}

// replaceGeneratedValues replaces user-supplied values with generation expressions
// for any generated columns, preserving the original values for normal columns.
func replaceGeneratedValues(rows [][]parser.Expression, table *schema.Table) [][]parser.Expression {
	if !tableHasGeneratedColumns(table) {
		return rows
	}
	result := make([][]parser.Expression, len(rows))
	for i, row := range rows {
		newRow := make([]parser.Expression, len(row))
		copy(newRow, row)
		for j, col := range table.Columns {
			if col.Generated && j < len(newRow) {
				newRow[j] = generatedExprForColumn(col)
			}
		}
		result[i] = newRow
	}
	return result
}

// tableHasGeneratedColumns returns true if the table has any generated columns.
func tableHasGeneratedColumns(table *schema.Table) bool {
	for _, col := range table.Columns {
		if col.Generated {
			return true
		}
	}
	return false
}

// ============================================================================
// RETURNING Clause Compilation
// ============================================================================

// expandReturningColumns expands RETURNING * into individual column references
// and returns the expanded columns along with their names.
func expandReturningColumns(returning []parser.ResultColumn, table *schema.Table) ([]parser.ResultColumn, []string) {
	return expandStarColumns(returning, table)
}

// emitReturningRow evaluates RETURNING expressions and emits an OpResultRow.
// The gen must already have the cursor registered for column lookups.
func emitReturningRow(vm *vdbe.VDBE, cols []parser.ResultColumn, numCols int, gen *expr.CodeGenerator) error {
	retBaseReg := gen.AllocRegs(numCols)
	for i, col := range cols {
		reg, err := gen.GenerateExpr(col.Expr)
		if err != nil {
			return fmt.Errorf("RETURNING expression: %w", err)
		}
		if reg != retBaseReg+i {
			vm.AddOp(vdbe.OpCopy, reg, retBaseReg+i, 0)
		}
	}
	vm.AddOp(vdbe.OpResultRow, retBaseReg, numCols, 0)
	return nil
}

// defaultExprForColumn returns a parser.Expression representing the column's
// DEFAULT value, or a NULL literal when no default is defined.
func defaultExprForColumn(col *schema.Column) parser.Expression {
	if col.Default == nil {
		return &parser.LiteralExpr{Type: parser.LiteralNull}
	}
	defaultStr, ok := col.Default.(string)
	if !ok {
		return &parser.LiteralExpr{Type: parser.LiteralNull}
	}
	p := parser.NewParser(defaultStr)
	expr, err := p.ParseExpression()
	if err != nil {
		return &parser.LiteralExpr{Type: parser.LiteralNull}
	}
	return expr
}
