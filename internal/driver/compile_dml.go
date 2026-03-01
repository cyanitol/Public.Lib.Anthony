package driver

import (
	"database/sql/driver"
	"fmt"

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

// compileInsert compiles an INSERT statement. CC=3
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

	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}

	// Use first row to determine structure
	firstRow := stmt.Values[0]
	colNames := resolveInsertColumns(stmt, table)
	rowidColIdx := findInsertRowidCol(colNames, table)

	numRecordCols := len(firstRow)
	if rowidColIdx >= 0 {
		numRecordCols--
	}

	// Register layout:
	//   reg 1         - rowid  (P3=0 is special in OpInsert, so start at 1)
	//   reg 2..N+1    - record column values (non-rowid only)
	//   reg N+2       - assembled record
	const rowidReg = 1
	const recordStartReg = 2
	vm.AllocMemory(numRecordCols + 10)
	vm.AllocCursors(1)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	paramIdx := 0

	// Loop over all rows in VALUES clause
	for _, row := range stmt.Values {
		s.emitInsertRowid(vm, table, row, rowidColIdx, rowidReg, args, &paramIdx)
		s.emitInsertRecordValues(vm, row, rowidColIdx, recordStartReg, args, &paramIdx)

		resultReg := recordStartReg + numRecordCols
		vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)

		// OpInsert: P1=cursor, P2=record register, P3=rowid register
		insertOp := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidReg)

		// For AUTOINCREMENT tables, we need to pass table metadata to the Insert handler
		// Store table name in P4 string for sequence management
		if _, hasAutoincrement := table.HasAutoincrementColumn(); hasAutoincrement {
			vm.Program[insertOp].P4.Z = table.Name
		}
	}

	// TODO Phase 3.5: AFTER INSERT trigger execution
	// Same limitation as BEFORE triggers - requires VDBE runtime integration.
	// Keeping framework in place for future implementation.

	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
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
	// OpNewRowid: P1=cursor, P3=destination register
	vm.AddOp(vdbe.OpNewRowid, 0, 0, rowidReg)
}

// emitInsertRecordValues emits OpXxx opcodes that load each non-rowid value
// from row into consecutive registers beginning at startReg.
func (s *Stmt) emitInsertRecordValues(vm *vdbe.VDBE, row []parser.Expression, rowidColIdx int, startReg int, args []driver.NamedValue, paramIdx *int) {
	reg := startReg
	for i, val := range row {
		if i == rowidColIdx {
			continue // rowid already loaded separately
		}
		s.compileValue(vm, val, reg, args, paramIdx)
		reg++
	}
}

// compileValue compiles a value expression into bytecode that stores the result in reg.
// CC=3
func (s *Stmt) compileValue(vm *vdbe.VDBE, val parser.Expression, reg int, args []driver.NamedValue, paramIdx *int) {
	switch val.(type) {
	case *parser.LiteralExpr:
		compileLiteralExpr(vm, val.(*parser.LiteralExpr), reg)
	case *parser.VariableExpr:
		if *paramIdx >= len(args) {
			vm.AddOp(vdbe.OpNull, 0, reg, 0)
			return
		}
		arg := args[*paramIdx]
		*paramIdx++
		compileArgValue(vm, arg.Value, reg)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// compileLiteralExpr emits the VDBE opcode for a literal value into register reg.
// Float, String, and Blob literals all map to OpString8; Integer and Null have
// dedicated opcodes. CC=4
func compileLiteralExpr(vm *vdbe.VDBE, expr *parser.LiteralExpr, reg int) {
	switch expr.Type {
	case parser.LiteralInteger:
		var intVal int64
		fmt.Sscanf(expr.Value, "%d", &intVal)
		vm.AddOp(vdbe.OpInteger, int(intVal), reg, 0)
	case parser.LiteralNull:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case parser.LiteralFloat, parser.LiteralString, parser.LiteralBlob:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, expr.Value)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// compileArgValue emits the VDBE opcode for a concrete bound-parameter value
// into register reg. CC=6
func compileArgValue(vm *vdbe.VDBE, val driver.Value, reg int) {
	switch v := val.(type) {
	case nil:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case int:
		vm.AddOp(vdbe.OpInteger, v, reg, 0)
	case int64:
		vm.AddOp(vdbe.OpInteger, int(v), reg, 0)
	case float64:
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, v)
	case string:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
	case []byte:
		vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), reg, 0, v)
	default:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, fmt.Sprintf("%v", v))
	}
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
			recordIdx := schemaRecordIdx(table.Columns, colIdx)
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
