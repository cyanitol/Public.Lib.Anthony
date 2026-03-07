// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strconv"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
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

	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Phase 3.5 Note: BEFORE INSERT trigger execution framework exists but is not yet
	// integrated into the VDBE execution path. Triggers are defined and stored in schema,
	// but actual execution requires VDBE-level changes to handle OLD/NEW pseudo-tables.
	//
	// The trigger infrastructure is complete:
	// - schema.CreateTrigger() stores trigger definitions
	// - schema.GetTableTriggers() retrieves triggers by table/event/timing
	// - engine.TriggerExecutor provides execution framework
	// - engine.SubstituteOldNewReferences() handles OLD/NEW substitution
	//
	// What's missing for full execution:
	// - VDBE opcodes need to call trigger executor at runtime
	// - OLD/NEW values must be extracted from current row data during DML operations
	// - Trigger bytecode needs to be inlined or executed via callback in VDBE loop
	//
	// This will be completed in a future phase focused on VDBE enhancements.

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
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}

	// Use first row to determine structure
	firstRow := stmt.Values[0]
	colNames := resolveInsertColumns(stmt, table)
	rowidColIdx := findInsertRowidCol(colNames, table)

	// For WITHOUT ROWID tables, all columns go in the record
	// For normal tables, only non-rowid columns go in the record
	numRecordCols := len(firstRow)
	if !table.WithoutRowID && rowidColIdx >= 0 {
		numRecordCols--
	}

	// Register layout:
	//   reg 1         - rowid  (P3=0 is special in OpInsert, so start at 1)
	//   reg 2..N+1    - record column values (non-rowid only for normal tables, all for WITHOUT ROWID)
	//   reg N+2       - assembled record
	const rowidReg = 1
	const recordStartReg = 2
	vm.AllocMemory(numRecordCols + 10)
	vm.AllocCursors(1)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	paramIdx := 0

	// Determine conflict resolution mode for INSERT OR IGNORE/REPLACE
	conflictMode := getConflictMode(stmt.OnConflict)

	// Loop over all rows in VALUES clause
	for _, row := range stmt.Values {
		s.emitInsertRow(vm, table, colNames, row, rowidColIdx, rowidReg, recordStartReg, numRecordCols, conflictMode, args, &paramIdx)
	}

	// TODO Phase 3.5: AFTER INSERT trigger execution
	// Same limitation as BEFORE triggers - requires VDBE runtime integration.
	// Keeping framework in place for future implementation.

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// emitInsertRow emits bytecode for inserting a single row.
func (s *Stmt) emitInsertRow(vm *vdbe.VDBE, table *schema.Table, colNames []string, row []parser.Expression,
	rowidColIdx, rowidReg, recordStartReg, numRecordCols int, conflictMode int32,
	args []driver.NamedValue, paramIdx *int) {

	// For WITHOUT ROWID tables, we don't generate a rowid
	if !table.WithoutRowID {
		s.emitInsertRowid(vm, table, row, rowidColIdx, rowidReg, args, paramIdx)
	}
	s.emitInsertRecordValues(vm, table, colNames, row, rowidColIdx, recordStartReg, args, paramIdx)

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
	targetTable, ok := s.conn.schema.GetTable(stmt.Table)
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

	sourceTable, ok := s.conn.schema.GetTable(sourceTableName)
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
// column, or -1 when none exists.
func findInsertRowidCol(names []string, table *schema.Table) int {
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
func (s *Stmt) emitInsertRowid(vm *vdbe.VDBE, table *schema.Table, row []parser.Expression, rowidColIdx int, rowidReg int, args []driver.NamedValue, paramIdx *int) {
	if rowidColIdx >= 0 {
		s.compileValue(vm, row[rowidColIdx], rowidReg, args, paramIdx)

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
func (s *Stmt) emitInsertRecordValues(vm *vdbe.VDBE, table *schema.Table, colNames []string, row []parser.Expression, rowidColIdx int, startReg int, args []driver.NamedValue, paramIdx *int) {
	reg := startReg
	for i, val := range row {
		// For normal tables, skip the rowid column (it's stored separately)
		// For WITHOUT ROWID tables, include all columns (even PRIMARY KEY)
		if !table.WithoutRowID && i == rowidColIdx {
			continue // rowid already loaded separately
		}
		s.compileValue(vm, val, reg, args, paramIdx)

		// Apply column affinity if column exists in schema
		colName := colNames[i]
		if colIdx := table.GetColumnIndex(colName); colIdx >= 0 {
			col := table.Columns[colIdx]
			if col.Affinity != schema.AffinityNone && col.Affinity != schema.AffinityBlob {
				// Convert affinity to OpCast P2 encoding
				affinityCode := affinityToOpCastCode(col.Affinity)
				vm.AddOp(vdbe.OpCast, reg, affinityCode, 0)
			}
		}
		reg++
	}
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
func (s *Stmt) compileValue(vm *vdbe.VDBE, val parser.Expression, reg int, args []driver.NamedValue, paramIdx *int) {
	switch v := val.(type) {
	case *parser.LiteralExpr:
		compileLiteralExpr(vm, v, reg)
	case *parser.VariableExpr:
		compileVariableExpr(vm, v, reg, args, paramIdx)
	case *parser.UnaryExpr:
		compileUnaryExpr(vm, v, reg)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
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
	case parser.LiteralString, parser.LiteralBlob:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, expr.Value)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
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

	// Look up table in schema
	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Build update map and column list
	updateMap, _ := s.buildUpdateMap(stmt) // updatedColumns used for trigger execution (not yet operational)

	// TODO Phase 3.5: BEFORE/AFTER UPDATE trigger execution
	// Requires VDBE runtime integration. Framework exists but not yet operational.
	// When implemented, use updatedColumns parameter to determine which triggers fire.

	// Setup VDBE and code generator
	gen, numRecordCols := s.setupUpdateVDBE(vm, table, stmt)

	// Emit update loop
	rewindAddr := s.emitUpdateLoop(vm, stmt, table, updateMap, numRecordCols, gen, args)

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
	// Count non-rowid columns
	numRecordCols := 0
	for _, col := range table.Columns {
		if !schemaColIsRowid(col) {
			numRecordCols++
		}
	}

	// Allocate resources
	vm.AllocMemory(numRecordCols + 20)
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
	updateMap map[string]parser.Expression, numRecordCols int, gen *expr.CodeGenerator,
	args []driver.NamedValue) int {

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

	// Build updated record
	recordStartReg := s.emitUpdateRecordBuild(vm, table, updateMap, numRecordCols, gen)

	// Create record, delete old, insert new
	s.emitUpdateRowReplacement(vm, recordStartReg, numRecordCols, rowidReg, gen)

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
func (s *Stmt) emitUpdateRecordBuild(vm *vdbe.VDBE, table *schema.Table,
	updateMap map[string]parser.Expression, numRecordCols int,
	gen *expr.CodeGenerator) int {

	recordStartReg := gen.AllocRegs(numRecordCols)
	reg := recordStartReg

	for colIdx, col := range table.Columns {
		if schemaColIsRowid(col) {
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

	return recordStartReg
}

// emitUpdateRowReplacement emits bytecode to replace the row.
func (s *Stmt) emitUpdateRowReplacement(vm *vdbe.VDBE, recordStartReg int,
	numRecordCols int, rowidReg int, gen *expr.CodeGenerator) {

	resultReg := gen.AllocReg()
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)
	vm.AddOp(vdbe.OpDelete, 0, 0, 0)

	insertAddr := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidReg)
	vm.Program[insertAddr].P4.I = 1 // Don't double-count in NumChanges
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

	// Look up table in schema
	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// TODO Phase 3.5: BEFORE DELETE trigger execution
	// Requires VDBE runtime integration. Framework exists but not yet operational.

	vm.AllocMemory(10)
	vm.AllocCursors(1)

	// Initialize program
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open table for writing (cursor 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	// Start iteration from beginning
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// If WHERE clause exists, compile and evaluate it
	if stmt.Where != nil {
		// Create code generator for expression compilation
		gen := expr.NewCodeGenerator(vm)
		s.setupSubqueryCompiler(gen)
		gen.RegisterCursor(stmt.Table, 0)

		// Register table info for column resolution
		tableInfo := buildTableInfo(stmt.Table, table)
		gen.RegisterTable(tableInfo)

		// Set up args for parameter binding
		argValues := make([]interface{}, len(args))
		for i, a := range args {
			argValues[i] = a.Value
		}
		gen.SetArgs(argValues)

		// Generate code for WHERE expression
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE clause: %w", err)
		}

		// Skip deletion if WHERE condition is false (OpIfNot jumps when register is false/0)
		skipAddr := vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)

		// Delete the current row (only if WHERE is true)
		vm.AddOp(vdbe.OpDelete, 0, 0, 0)

		// Fix up the skip target to point past the Delete to the Next instruction
		vm.Program[skipAddr].P2 = vm.NumOps()
	} else {
		// No WHERE clause: delete current row unconditionally
		vm.AddOp(vdbe.OpDelete, 0, 0, 0)
	}

	// Move to next row and loop back (common for both WHERE and non-WHERE cases)
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)

	// TODO Phase 3.5: AFTER DELETE trigger execution
	// Requires VDBE runtime integration. Framework exists but not yet operational.

	// Close table cursor
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Halt execution
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix up the rewind instruction to jump to halt when done
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
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
