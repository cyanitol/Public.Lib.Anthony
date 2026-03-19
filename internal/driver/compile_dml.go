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
	rowidColIdx := findInsertRowidCol(allColNames, table)
	numRecordCols := insertRecordColCount(expandedValues[0], table, rowidColIdx)

	const rowidReg = 1
	const recordStartReg = 2
	gen := s.initInsertVDBE(vm, numRecordCols, recordStartReg, args)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	paramIdx := 0
	conflictMode := getConflictMode(stmt.OnConflict)
	hasTriggers := s.tableHasTriggers(stmt.Table)

	for _, row := range expandedValues {
		s.emitInsertValueRow(vm, stmt, table, row, allColNames, rowidColIdx,
			rowidReg, recordStartReg, numRecordCols, conflictMode, hasTriggers, args, &paramIdx, gen)
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

	if len(stmt.Columns) > 0 {
		for _, row := range stmt.Values {
			if len(row) != len(stmt.Columns) {
				return nil, fmt.Errorf("%d values for %d columns", len(row), len(stmt.Columns))
			}
		}
	}

	expandedValues := expandInsertDefaults(stmt, table)
	numCols := len(table.Columns)
	for _, row := range expandedValues {
		if len(row) != numCols {
			return nil, fmt.Errorf("table %s has %d columns but %d values were supplied", table.Name, numCols, len(row))
		}
	}
	return expandedValues, nil
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
		argValues := make([]interface{}, len(args))
		for i, a := range args {
			argValues[i] = a.Value
		}
		gen.SetArgs(argValues)
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

	resultReg := recordStartReg + numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)
	rowidRegToUse := rowidReg
	if table.WithoutRowID {
		rowidRegToUse = 0
	}
	insertOp := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidRegToUse)
	vm.Program[insertOp].P4.I = conflictMode
	vm.Program[insertOp].P4.Z = table.Name

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

	resultReg := recordStartReg + numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)

	// OpInsert: P1=cursor, P2=record register, P3=rowid register
	// For WITHOUT ROWID tables, P3 is set to 0 (no rowid)
	// P4.I = conflict mode (0=abort, 1=ignore, 2=replace)
	// P4.Z = table name (for UNIQUE constraint checking and AUTOINCREMENT)
	rowidRegToUse := rowidReg
	if table.WithoutRowID {
		rowidRegToUse = 0
	}
	insertOp := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidRegToUse)
	vm.Program[insertOp].P4.I = conflictMode
	vm.Program[insertOp].P4.Z = table.Name
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

	resultReg := recordStartReg + numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)
	rowidRegToUse := rowidReg
	if table.WithoutRowID {
		rowidRegToUse = 0
	}
	insertOp := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidRegToUse)
	vm.Program[insertOp].P4.Z = table.Name
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
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(sourceTableName, 1)
	sourceTableInfo := buildTableInfo(sourceTableName, sourceTable)
	gen.RegisterTable(sourceTableInfo)

	// Set up args for parameter binding
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	return gen
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
	resultReg := recordStartReg + ctx.numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, ctx.numRecordCols, resultReg)

	// For WITHOUT ROWID tables, P3 is set to 0 (no rowid)
	rowidRegToUse := rowidReg
	if ctx.targetTable.WithoutRowID {
		rowidRegToUse = 0
	}
	insertOp := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidRegToUse)
	// Pass table name for UNIQUE constraint checking and AUTOINCREMENT
	vm.Program[insertOp].P4.Z = ctx.targetTable.Name

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
// column, or -1 when none exists. For WITHOUT ROWID tables, always returns -1
// since they don't have a rowid column.
func findInsertRowidCol(names []string, table *schema.Table) int {
	if table.WithoutRowID {
		return -1 // WITHOUT ROWID tables don't have a rowid column
	}
	for i, name := range names {
		idx := table.GetColumnIndex(name)
		if idx < 0 {
			continue
		}
		if schemaColIsRowid(table.Columns[idx]) {
			return i
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

	for _, assign := range stmt.Sets {
		colIdx := table.GetColumnIndexWithRowidAliases(assign.Column)
		if colIdx == -1 {
			return nil, fmt.Errorf("no such column: %s", assign.Column)
		}
	}

	// Build update map and column list
	updateMap, updatedColumns := s.buildUpdateMap(stmt)

	// Setup VDBE and code generator
	gen, numRecordCols := s.setupUpdateVDBE(vm, table, stmt)

	// Emit update loop
	rewindAddr := s.emitUpdateLoop(vm, stmt, table, updateMap, updatedColumns, numRecordCols, gen, args)

	// Close and finalize
	s.finalizeUpdate(vm, rewindAddr)

	return vm, nil
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
	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(stmt.Table, 0)
	tableInfo := buildTableInfo(stmt.Table, table)
	gen.RegisterTable(tableInfo)

	return gen, numRecordCols
}

// emitUpdateLoop generates the main UPDATE loop bytecode.
func (s *Stmt) emitUpdateLoop(vm *vdbe.VDBE, stmt *parser.UpdateStmt, table *schema.Table,
	updateMap map[string]parser.Expression, updatedCols []string, numRecordCols int,
	gen *expr.CodeGenerator, args []driver.NamedValue) int {

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Prepare args
	setParamCount := countParams(stmt.Sets)
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}

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

		if updateExpr, isUpdated := updateMap[col.Name]; isUpdated {
			// Column is being updated
			valReg, _ := gen.GenerateExpr(updateExpr)
			vm.AddOp(vdbe.OpCopy, valReg, reg, 0)
		} else {
			// Column is not updated - load existing value
			recordIdx := schemaRecordIdxForTable(table, colIdx)
			vm.AddOp(vdbe.OpColumn, 0, recordIdx, reg)
		}
		reg++
	}

	return recordStartReg, newRowidReg
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

	hasTriggers := s.tableHasTriggers(stmt.Table)

	// Allocate extra registers for OLD row snapshot when triggers exist
	numRecordCols := countNonRowidCols(table)
	memSize := 10
	oldRowStartReg := 0
	if hasTriggers {
		// Layout: reg 1 = rowid, regs 2..N+1 = record columns
		oldRowStartReg = 2
		memSize = numRecordCols + 20
	}
	vm.AllocMemory(memSize)
	vm.AllocCursors(1)

	// Initialize program
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open table for writing (cursor 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	// Pre-materialise subqueries in the WHERE clause so they are evaluated
	// once before the delete loop rather than re-evaluated per row.
	if stmt.Where != nil {
		stmt.Where = s.materializeSubqueries(vm, stmt.Where)
	}

	// Start iteration from beginning
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	skipAddr := s.emitDeleteWhereClause(vm, stmt, table, args)

	// Snapshot OLD row into registers before deletion (for trigger access)
	if hasTriggers {
		emitOldRowSnapshot(vm, table, 0, 1, oldRowStartReg, numRecordCols)
	}

	// Emit BEFORE DELETE trigger (P1=2 for DELETE event)
	var beforeAddr int
	if hasTriggers {
		beforeAddr = vm.AddOp(vdbe.OpTriggerBefore, 2, 0, oldRowStartReg)
		vm.Program[beforeAddr].P4.Z = stmt.Table
	}

	// Delete the current row
	delAddr := vm.AddOp(vdbe.OpDelete, 0, 0, 0)
	vm.Program[delAddr].P4.Z = table.Name

	// Emit AFTER DELETE trigger
	if hasTriggers {
		afterAddr := vm.AddOp(vdbe.OpTriggerAfter, 2, 0, oldRowStartReg)
		vm.Program[afterAddr].P4.Z = stmt.Table
		vm.Program[beforeAddr].P2 = vm.NumOps()
	}

	// Fix up the skip target to point past the Delete to the Next instruction
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Move to next row and loop back
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)

	// Close table cursor
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Halt execution
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix up the rewind instruction to jump to halt when done
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

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	gen.RegisterCursor(stmt.Table, 0)

	tableInfo := buildTableInfo(stmt.Table, table)
	gen.RegisterTable(tableInfo)

	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

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
// This is a stub implementation that currently falls back to regular INSERT.
func (s *Stmt) compileInsertUpsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// TODO: Implement full UPSERT support
	// For now, ignore the UPSERT clause and fall through to regular INSERT
	// by clearing the Upsert field temporarily
	savedUpsert := stmt.Upsert
	stmt.Upsert = nil
	result, err := s.compileInsert(vm, stmt, args)
	stmt.Upsert = savedUpsert
	return result, err
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
func expandInsertDefaults(stmt *parser.InsertStmt, table *schema.Table) [][]parser.Expression {
	if len(stmt.Columns) == 0 {
		return stmt.Values // All columns specified implicitly
	}
	specifiedSet := buildColumnSet(stmt.Columns)
	expanded := make([][]parser.Expression, len(stmt.Values))
	for i, row := range stmt.Values {
		expanded[i] = expandSingleRow(row, stmt.Columns, specifiedSet, table)
	}
	return expanded
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
func expandSingleRow(row []parser.Expression, insertCols []string, specifiedSet map[string]int, table *schema.Table) []parser.Expression {
	full := make([]parser.Expression, len(table.Columns))
	for i, col := range table.Columns {
		idx, ok := specifiedSet[strings.ToLower(col.Name)]
		if ok && idx < len(row) {
			full[i] = row[idx]
		} else {
			full[i] = defaultExprForColumn(col)
		}
	}
	return full
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
