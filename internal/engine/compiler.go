// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package engine

import (
	"fmt"
	"reflect"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

type stmtHandler func(parser.Statement) (*vdbe.VDBE, error)

// Compiler compiles SQL AST to VDBE bytecode.
type Compiler struct {
	engine   *Engine
	handlers map[reflect.Type]stmtHandler
}

// NewCompiler creates a new SQL to VDBE compiler.
func NewCompiler(engine *Engine) *Compiler {
	c := &Compiler{engine: engine}
	c.handlers = map[reflect.Type]stmtHandler{
		reflect.TypeOf((*parser.SelectStmt)(nil)):      func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileSelect(s.(*parser.SelectStmt)) },
		reflect.TypeOf((*parser.InsertStmt)(nil)):      func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileInsert(s.(*parser.InsertStmt)) },
		reflect.TypeOf((*parser.UpdateStmt)(nil)):      func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileUpdate(s.(*parser.UpdateStmt)) },
		reflect.TypeOf((*parser.DeleteStmt)(nil)):      func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileDelete(s.(*parser.DeleteStmt)) },
		reflect.TypeOf((*parser.CreateTableStmt)(nil)): func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileCreateTable(s.(*parser.CreateTableStmt)) },
		reflect.TypeOf((*parser.CreateIndexStmt)(nil)): func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileCreateIndex(s.(*parser.CreateIndexStmt)) },
		reflect.TypeOf((*parser.DropTableStmt)(nil)):   func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileDropTable(s.(*parser.DropTableStmt)) },
		reflect.TypeOf((*parser.DropIndexStmt)(nil)):   func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileDropIndex(s.(*parser.DropIndexStmt)) },
		reflect.TypeOf((*parser.BeginStmt)(nil)):       func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileBegin(s.(*parser.BeginStmt)) },
		reflect.TypeOf((*parser.CommitStmt)(nil)):      func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileCommit(s.(*parser.CommitStmt)) },
		reflect.TypeOf((*parser.RollbackStmt)(nil)):    func(s parser.Statement) (*vdbe.VDBE, error) { return c.CompileRollback(s.(*parser.RollbackStmt)) },
	}
	return c
}

// Compile compiles a SQL statement to VDBE bytecode.
func (c *Compiler) Compile(stmt parser.Statement) (*vdbe.VDBE, error) {
	handler, ok := c.handlers[reflect.TypeOf(stmt)]
	if !ok {
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
	return handler(stmt)
}

// CompileSelect compiles a SELECT statement.
func (c *Compiler) CompileSelect(stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(true)

	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 10) // Extra registers for temps

	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return c.compileSelectNoFrom(vm, stmt)
	}

	tableName := stmt.From.Tables[0].TableName
	table, ok := c.engine.schema.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	return c.compileSelectScan(vm, stmt, tableName, table)
}

// compileSelectNoFrom handles SELECT without a FROM clause (e.g. SELECT 1+1).
func (c *Compiler) compileSelectNoFrom(vm *vdbe.VDBE, stmt *parser.SelectStmt) (*vdbe.VDBE, error) {
	numCols := len(stmt.Columns)

	for i, col := range stmt.Columns {
		if col.Expr != nil {
			// Evaluate expression into register; simplified for now.
			vm.AddOp(vdbe.OpNull, 0, i, 0)
		}
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	vm.ResultCols = resolveNoFromColNames(stmt.Columns)
	return vm, nil
}

// resolveNoFromColNames builds result column names for a FROM-less SELECT.
func resolveNoFromColNames(cols []parser.ResultColumn) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		if col.Alias != "" {
			names[i] = col.Alias
		} else {
			names[i] = fmt.Sprintf("column_%d", i)
		}
	}
	return names
}

// compileSelectScan emits the full table-scan bytecode for a SELECT with a FROM clause.
func (c *Compiler) compileSelectScan(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table) (*vdbe.VDBE, error) {
	// Collect all tables involved in the query (base table + JOINs)
	tables, err := c.collectQueryTables(stmt, tableName, table)
	if err != nil {
		return nil, err
	}

	// Allocate and open cursors for all tables
	c.openTableCursors(vm, tables)

	// Set up nested loops
	loopStart, innerLoopStarts := c.setupNestedLoops(vm, tables)

	// Compile WHERE clause and emit column operations
	if err := c.compileWhereAndColumns(vm, stmt, tables, loopStart); err != nil {
		return nil, err
	}

	// Close nested loops and cursors
	c.closeNestedLoops(vm, tables, loopStart, innerLoopStarts)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.ResultCols = resolveMultiTableColNames(stmt.Columns, tables)
	return vm, nil
}

// compileWhereAndColumns compiles WHERE clause and emits column operations for SELECT.
func (c *Compiler) compileWhereAndColumns(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []tableInfo, loopStart int) error {
	// Evaluate WHERE clause if present
	hasWhere := c.compileWhereClause(vm, stmt, tables)

	// Emit column operations
	if err := emitColumnOpsMultiTable(vm, stmt.Columns, tables); err != nil {
		return err
	}

	vm.AddOp(vdbe.OpResultRow, 0, len(stmt.Columns), 0)

	// Patch the skip label if WHERE clause exists
	if hasWhere {
		patchWhereSkipLabelFromStart(vm, loopStart)
	}

	return nil
}

// compileWhereClause compiles WHERE clause and returns true if WHERE was present.
func (c *Compiler) compileWhereClause(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []tableInfo) bool {
	if stmt.Where == nil {
		return false
	}

	// Create code generator for WHERE expression
	codegen := c.createMultiTableCodeGenerator(vm, tables)

	// Generate WHERE condition
	whereReg, err := codegen.GenerateExpr(stmt.Where)
	if err != nil {
		// Note: In production code, this should return error, but keeping current behavior
		return false
	}

	// If WHERE is false, skip this row combination
	skipLabel := vm.NumOps() + 100 // Will be patched later
	vm.AddOp(vdbe.OpIfNot, whereReg, skipLabel, 0)
	vm.SetComment(vm.NumOps()-1, "Skip row if WHERE is false")
	return true
}

// patchWhereSkipLabelFromStart patches WHERE skip labels starting from loopStart.
func patchWhereSkipLabelFromStart(vm *vdbe.VDBE, loopStart int) {
	currentAddr := vm.NumOps()
	// Find and patch the OpIfNot instruction
	for i := loopStart; i < vm.NumOps(); i++ {
		if vm.Program[i].Opcode == vdbe.OpIfNot && vm.Program[i].P2 > vm.NumOps() {
			vm.Program[i].P2 = currentAddr
			break
		}
	}
}

func (c *Compiler) collectQueryTables(stmt *parser.SelectStmt, tableName string, table *schema.Table) ([]tableInfo, error) {
	tables := []tableInfo{{name: tableName, table: table, cursorIdx: 0}}

	if stmt.From != nil && len(stmt.From.Joins) > 0 {
		for i, join := range stmt.From.Joins {
			joinTable, ok := c.engine.schema.GetTable(join.Table.TableName)
			if !ok {
				return nil, fmt.Errorf("table not found: %s", join.Table.TableName)
			}
			tables = append(tables, tableInfo{
				name:      join.Table.TableName,
				table:     joinTable,
				cursorIdx: i + 1,
			})
		}
	}
	return tables, nil
}

func (c *Compiler) openTableCursors(vm *vdbe.VDBE, tables []tableInfo) {
	vm.AllocCursors(len(tables))
	for _, tbl := range tables {
		vm.AddOp(vdbe.OpOpenRead, tbl.cursorIdx, int(tbl.table.RootPage), 0)
		vm.SetComment(vm.NumOps()-1, fmt.Sprintf("Open cursor for %s", tbl.name))
	}
}

func (c *Compiler) setupNestedLoops(vm *vdbe.VDBE, tables []tableInfo) (loopStart int, innerLoopStarts []int) {
	vm.AddOp(vdbe.OpRewind, 0, vm.NumOps()+100, 0)
	loopStart = vm.NumOps()

	for i := 1; i < len(tables); i++ {
		vm.AddOp(vdbe.OpRewind, i, 0, 0)
		innerLoopStarts = append(innerLoopStarts, vm.NumOps())
	}
	return
}

func (c *Compiler) closeNestedLoops(vm *vdbe.VDBE, tables []tableInfo, loopStart int, innerLoopStarts []int) {
	for i := len(tables) - 1; i > 0; i-- {
		vm.AddOp(vdbe.OpNext, i, innerLoopStarts[i-1], 0)
		vm.AddOp(vdbe.OpClose, i, 0, 0)
	}
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
}

// tableInfo holds information about a table in a query.
type tableInfo struct {
	name      string
	table     *schema.Table
	cursorIdx int
}

// emitColumnOps emits OpColumn instructions for each result column in a table scan.
func emitColumnOps(vm *vdbe.VDBE, cursorIdx int, cols []parser.ResultColumn, table *schema.Table) error {
	for i, col := range cols {
		if col.Star {
			// SELECT * - emit one OpColumn per table column.
			for j := range table.Columns {
				vm.AddOp(vdbe.OpColumn, cursorIdx, j, i+j)
			}
			continue
		}

		colIdx, err := resolveColumnIndex(col, table)
		if err != nil {
			return err
		}
		vm.AddOp(vdbe.OpColumn, cursorIdx, colIdx, i)
	}
	return nil
}

// emitColumnOpsMultiTable emits OpColumn instructions for each result column across multiple tables.
func emitColumnOpsMultiTable(vm *vdbe.VDBE, cols []parser.ResultColumn, tables []tableInfo) error {
	for i, col := range cols {
		if col.Star {
			// SELECT * - emit one OpColumn per column from all tables
			regOffset := i
			for _, tbl := range tables {
				for j := range tbl.table.Columns {
					vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, j, regOffset)
					regOffset++
				}
			}
			continue
		}

		cursorIdx, colIdx, err := resolveColumnIndexMultiTable(col, tables)
		if err != nil {
			return err
		}
		vm.AddOp(vdbe.OpColumn, cursorIdx, colIdx, i)
	}
	return nil
}

// resolveColumnIndex resolves the physical column index for a non-star result column.
func resolveColumnIndex(col parser.ResultColumn, table *schema.Table) (int, error) {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		return 0, nil
	}
	idx := table.GetColumnIndex(ident.Name)
	if idx < 0 {
		return 0, fmt.Errorf("column not found: %s", ident.Name)
	}
	return idx, nil
}

// resolveColumnIndexMultiTable resolves the cursor index and column index for a column across multiple tables.
// It handles both qualified (table.column) and unqualified (column) names.
func resolveColumnIndexMultiTable(col parser.ResultColumn, tables []tableInfo) (cursorIdx int, colIdx int, err error) {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		return 0, 0, nil
	}

	// Dispatch based on whether column has table qualifier
	if ident.Table != "" {
		return resolveQualifiedColumn(ident, tables)
	}
	return resolveUnqualifiedColumn(ident, tables)
}

// resolveQualifiedColumn resolves a qualified column reference (table.column).
func resolveQualifiedColumn(ident *parser.IdentExpr, tables []tableInfo) (cursorIdx int, colIdx int, err error) {
	tbl, found := findTableByName(ident.Table, tables)
	if !found {
		return 0, 0, fmt.Errorf("table not found: %s", ident.Table)
	}

	idx := tbl.table.GetColumnIndex(ident.Name)
	if idx < 0 {
		return 0, 0, fmt.Errorf("column not found: %s.%s", ident.Table, ident.Name)
	}
	return tbl.cursorIdx, idx, nil
}

// resolveUnqualifiedColumn resolves an unqualified column reference by searching all tables.
func resolveUnqualifiedColumn(ident *parser.IdentExpr, tables []tableInfo) (cursorIdx int, colIdx int, err error) {
	for _, tbl := range tables {
		if idx := tbl.table.GetColumnIndex(ident.Name); idx >= 0 {
			return tbl.cursorIdx, idx, nil
		}
	}
	return 0, 0, fmt.Errorf("column not found: %s", ident.Name)
}

// findTableByName finds a table by name in the table list.
func findTableByName(tableName string, tables []tableInfo) (tableInfo, bool) {
	for _, tbl := range tables {
		if tbl.name == tableName || tbl.table.Name == tableName {
			return tbl, true
		}
	}
	return tableInfo{}, false
}

// resolveTableColNames builds result column names for a SELECT with a FROM clause.
func resolveTableColNames(cols []parser.ResultColumn, table *schema.Table) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = resolveOneTableColName(i, col, table)
	}
	return names
}

// resolveOneTableColName resolves the display name for a single result column.
func resolveOneTableColName(i int, col parser.ResultColumn, table *schema.Table) string {
	if col.Alias != "" {
		return col.Alias
	}
	if col.Star && i < len(table.Columns) {
		return table.Columns[i].Name
	}
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return fmt.Sprintf("column_%d", i)
}

// resolveMultiTableColNames builds result column names for a SELECT with multiple tables (JOINs).
func resolveMultiTableColNames(cols []parser.ResultColumn, tables []tableInfo) []string {
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
			names = append(names, fmt.Sprintf("column_%d", len(names)))
		}
	}
	return names
}

// CompileInsert compiles an INSERT statement.
func (c *Compiler) CompileInsert(stmt *parser.InsertStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Get table
	table, ok := c.engine.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Allocate registers for values
	numCols := len(table.Columns)
	vm.AllocMemory(numCols + 10)

	// Open cursor for table
	cursorIdx := 0
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpOpenWrite, cursorIdx, int(table.RootPage), 0)

	// For each row to insert
	for _, values := range stmt.Values {
		// Generate new rowid
		rowidReg := numCols
		vm.AddOp(vdbe.OpNewRowid, cursorIdx, rowidReg, 0)

		// Load values into registers
		for i, val := range values {
			reg := i
			if lit, ok := val.(*parser.LiteralExpr); ok {
				switch lit.Type {
				case parser.LiteralInteger:
					// Parse the integer from string
					intVal := int64(0)
					fmt.Sscanf(lit.Value, "%d", &intVal)
					vm.AddOp(vdbe.OpInteger, int(intVal), reg, 0)
				case parser.LiteralString:
					vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, lit.Value)
				case parser.LiteralNull:
					vm.AddOp(vdbe.OpNull, 0, reg, 0)
				}
			}
		}

		// Create record from registers
		recordReg := numCols + 1
		vm.AddOp(vdbe.OpMakeRecord, 0, len(values), recordReg)

		// Insert the record
		vm.AddOp(vdbe.OpInsert, cursorIdx, recordReg, rowidReg)
	}

	// Close cursor and halt
	vm.AddOp(vdbe.OpClose, cursorIdx, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// CompileUpdate compiles an UPDATE statement.
func (c *Compiler) CompileUpdate(stmt *parser.UpdateStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Validate and get table
	table, numCols, err := c.validateAndGetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	// Setup cursor and code generator
	cursorIdx := c.setupUpdateCursor(vm, stmt.Table, table, numCols)
	codegen := createCodeGenerator(vm, stmt.Table, table, cursorIdx)

	// Setup table scan loop
	loopStart := c.setupTableScanLoop(vm, cursorIdx)

	// Compile WHERE clause if present
	if err := c.compileUpdateWhere(vm, stmt, codegen, loopStart); err != nil {
		return nil, err
	}

	// Compile SET clause
	if err := c.compileUpdateSet(vm, stmt, table, codegen, cursorIdx, numCols); err != nil {
		return nil, err
	}

	// Close loop and cursor
	c.closeUpdateLoop(vm, cursorIdx, loopStart)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// validateAndGetTable validates table existence and returns table info.
func (c *Compiler) validateAndGetTable(tableName string) (*schema.Table, int, error) {
	table, ok := c.engine.schema.GetTable(tableName)
	if !ok {
		return nil, 0, fmt.Errorf("table not found: %s", tableName)
	}
	return table, len(table.Columns), nil
}

// setupUpdateCursor sets up the cursor for UPDATE operation.
func (c *Compiler) setupUpdateCursor(vm *vdbe.VDBE, tableName string, table *schema.Table, numCols int) int {
	vm.AllocMemory(numCols*2 + 20) // Extra registers for temps, SET expressions, etc.
	cursorIdx := 0
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpOpenWrite, cursorIdx, int(table.RootPage), 0)
	vm.SetComment(vm.NumOps()-1, fmt.Sprintf("Open cursor for UPDATE on %s", tableName))
	return cursorIdx
}

// setupTableScanLoop sets up the table scan loop and returns the loop start address.
func (c *Compiler) setupTableScanLoop(vm *vdbe.VDBE, cursorIdx int) int {
	endLabel := vm.NumOps() + 1000 // Will be patched
	vm.AddOp(vdbe.OpRewind, cursorIdx, endLabel, 0)
	return vm.NumOps()
}

// compileUpdateWhere compiles the WHERE clause for UPDATE.
func (c *Compiler) compileUpdateWhere(vm *vdbe.VDBE, stmt *parser.UpdateStmt, codegen *expr.CodeGenerator, loopStart int) error {
	if stmt.Where == nil {
		return nil
	}

	whereReg, err := codegen.GenerateExpr(stmt.Where)
	if err != nil {
		return fmt.Errorf("failed to compile WHERE clause: %w", err)
	}

	// If WHERE is false, skip this row
	skipLabel := vm.NumOps() + 100 // Will be calculated
	vm.AddOp(vdbe.OpIfNot, whereReg, skipLabel, 0)
	return nil
}

// compileUpdateSet compiles the SET clause for UPDATE.
func (c *Compiler) compileUpdateSet(vm *vdbe.VDBE, stmt *parser.UpdateStmt, table *schema.Table, codegen *expr.CodeGenerator, cursorIdx, numCols int) error {
	// Read current row into registers for OLD values
	oldValueBase := codegen.AllocRegs(numCols)
	for i := 0; i < numCols; i++ {
		vm.AddOp(vdbe.OpColumn, cursorIdx, i, oldValueBase+i)
	}

	// Get the current rowid
	rowidReg := codegen.AllocReg()
	vm.AddOp(vdbe.OpRowid, cursorIdx, rowidReg, 0)

	// Evaluate SET expressions into new registers
	newValueBase := codegen.AllocRegs(numCols)
	// Initialize with old values
	for i := 0; i < numCols; i++ {
		vm.AddOp(vdbe.OpCopy, oldValueBase+i, newValueBase+i, 0)
	}

	// Update columns based on SET assignments
	if err := c.applySetAssignments(vm, stmt, table, codegen, newValueBase); err != nil {
		return err
	}

	// Create new record from updated values
	recordReg := codegen.AllocReg()
	vm.AddOp(vdbe.OpMakeRecord, newValueBase, numCols, recordReg)

	// Update the row (using OpInsert with existing rowid overwrites)
	vm.AddOp(vdbe.OpInsert, cursorIdx, recordReg, rowidReg)
	vm.SetComment(vm.NumOps()-1, "Update row")

	return nil
}

// applySetAssignments applies the SET assignments to update columns.
func (c *Compiler) applySetAssignments(vm *vdbe.VDBE, stmt *parser.UpdateStmt, table *schema.Table, codegen *expr.CodeGenerator, newValueBase int) error {
	for _, assignment := range stmt.Sets {
		colIdx := table.GetColumnIndex(assignment.Column)
		if colIdx < 0 {
			return fmt.Errorf("column not found: %s", assignment.Column)
		}

		// Evaluate the SET expression
		valueReg, err := codegen.GenerateExpr(assignment.Value)
		if err != nil {
			return fmt.Errorf("failed to compile SET expression for %s: %w", assignment.Column, err)
		}

		// Copy the new value to the appropriate position
		vm.AddOp(vdbe.OpCopy, valueReg, newValueBase+colIdx, 0)
	}
	return nil
}

// closeUpdateLoop closes the UPDATE loop and patches labels.
func (c *Compiler) closeUpdateLoop(vm *vdbe.VDBE, cursorIdx, loopStart int) {
	// Patch the skip label if WHERE clause exists
	patchWhereSkipLabel(vm, loopStart)

	// Next row
	vm.AddOp(vdbe.OpNext, cursorIdx, loopStart, 0)

	// Close cursor - patch end label
	patchRewindEndLabel(vm)

	vm.AddOp(vdbe.OpClose, cursorIdx, 0, 0)
}

// patchWhereSkipLabel patches the skip label for WHERE clause.
func patchWhereSkipLabel(vm *vdbe.VDBE, loopStart int) {
	skipLabel := vm.NumOps()
	for i := loopStart; i < vm.NumOps(); i++ {
		if vm.Program[i].Opcode == vdbe.OpIfNot && vm.Program[i].P2 > vm.NumOps() {
			vm.Program[i].P2 = skipLabel
			break
		}
	}
}

// patchRewindEndLabel patches the end label for Rewind instruction.
func patchRewindEndLabel(vm *vdbe.VDBE) {
	endAddr := vm.NumOps()
	for i := 0; i < vm.NumOps(); i++ {
		if vm.Program[i].Opcode == vdbe.OpRewind && vm.Program[i].P2 > vm.NumOps() {
			vm.Program[i].P2 = endAddr
			break
		}
	}
}

// CompileDelete compiles a DELETE statement.
func (c *Compiler) CompileDelete(stmt *parser.DeleteStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Validate and get table
	table, numCols, err := c.validateAndGetTable(stmt.Table)
	if err != nil {
		return nil, err
	}

	// Setup cursor and code generator
	cursorIdx := c.setupDeleteCursor(vm, stmt.Table, table, numCols)
	codegen := createCodeGenerator(vm, stmt.Table, table, cursorIdx)

	// Setup table scan loop
	loopStart := c.setupTableScanLoop(vm, cursorIdx)

	// Compile WHERE clause if present
	if err := c.compileDeleteWhere(vm, stmt, codegen, cursorIdx, numCols, loopStart); err != nil {
		return nil, err
	}

	// Delete the current row
	vm.AddOp(vdbe.OpDelete, cursorIdx, 0, 0)
	vm.SetComment(vm.NumOps()-1, "Delete row")

	// Close loop and cursor
	c.closeDeleteLoop(vm, cursorIdx, loopStart)

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// setupDeleteCursor sets up the cursor for DELETE operation.
func (c *Compiler) setupDeleteCursor(vm *vdbe.VDBE, tableName string, table *schema.Table, numCols int) int {
	vm.AllocMemory(numCols + 10) // Extra registers for temps
	cursorIdx := 0
	vm.AllocCursors(1)
	vm.AddOp(vdbe.OpOpenWrite, cursorIdx, int(table.RootPage), 0)
	vm.SetComment(vm.NumOps()-1, fmt.Sprintf("Open cursor for DELETE on %s", tableName))
	return cursorIdx
}

// compileDeleteWhere compiles the WHERE clause for DELETE.
func (c *Compiler) compileDeleteWhere(vm *vdbe.VDBE, stmt *parser.DeleteStmt, codegen *expr.CodeGenerator, cursorIdx, numCols, loopStart int) error {
	if stmt.Where == nil {
		return nil
	}

	// Read all columns into registers (needed for WHERE evaluation)
	colBase := codegen.AllocRegs(numCols)
	for i := 0; i < numCols; i++ {
		vm.AddOp(vdbe.OpColumn, cursorIdx, i, colBase+i)
	}

	whereReg, err := codegen.GenerateExpr(stmt.Where)
	if err != nil {
		return fmt.Errorf("failed to compile WHERE clause: %w", err)
	}

	// If WHERE is false, skip deletion and go to next row
	skipLabel := vm.NumOps() + 10 // Will be calculated
	vm.AddOp(vdbe.OpIfNot, whereReg, skipLabel, 0)
	return nil
}

// closeDeleteLoop closes the DELETE loop and patches labels.
func (c *Compiler) closeDeleteLoop(vm *vdbe.VDBE, cursorIdx, loopStart int) {
	// Patch the skip label if WHERE clause exists
	patchWhereSkipLabel(vm, loopStart)

	// Next row
	vm.AddOp(vdbe.OpNext, cursorIdx, loopStart, 0)

	// Close cursor - patch end label
	patchRewindEndLabel(vm)

	vm.AddOp(vdbe.OpClose, cursorIdx, 0, 0)
}

// CompileCreateTable compiles a CREATE TABLE statement.
func (c *Compiler) CompileCreateTable(stmt *parser.CreateTableStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Create table in schema
	table, err := c.engine.schema.CreateTable(stmt)
	if err != nil {
		return nil, err
	}

	// Allocate a root page for the table
	rootPage, err := c.engine.btree.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("failed to create table btree: %w", err)
	}

	table.RootPage = rootPage

	// In a real implementation, we would also:
	// 1. Insert a row into sqlite_master
	// 2. Persist the schema

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileCreateIndex compiles a CREATE INDEX statement.
func (c *Compiler) CompileCreateIndex(stmt *parser.CreateIndexStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Create index in schema
	index, err := c.engine.schema.CreateIndex(stmt)
	if err != nil {
		return nil, err
	}

	// Allocate a root page for the index
	rootPage, err := c.engine.btree.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("failed to create index btree: %w", err)
	}

	index.RootPage = rootPage

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileDropTable compiles a DROP TABLE statement.
func (c *Compiler) CompileDropTable(stmt *parser.DropTableStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Get table
	table, ok := c.engine.schema.GetTable(stmt.Name)
	if !ok {
		if stmt.IfExists {
			// Not an error
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("table not found: %s", stmt.Name)
	}

	// Drop the B-tree
	if err := c.engine.btree.DropTable(table.RootPage); err != nil {
		return nil, fmt.Errorf("failed to drop table btree: %w", err)
	}

	// Remove from schema
	if err := c.engine.schema.DropTable(stmt.Name); err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileDropIndex compiles a DROP INDEX statement.
func (c *Compiler) CompileDropIndex(stmt *parser.DropIndexStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Get index
	index, ok := c.engine.schema.GetIndex(stmt.Name)
	if !ok {
		if stmt.IfExists {
			// Not an error
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("index not found: %s", stmt.Name)
	}

	// Drop the B-tree
	if err := c.engine.btree.DropTable(index.RootPage); err != nil {
		return nil, fmt.Errorf("failed to drop index btree: %w", err)
	}

	// Remove from schema
	if err := c.engine.schema.DropIndex(stmt.Name); err != nil {
		return nil, err
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileBegin compiles a BEGIN statement.
func (c *Compiler) CompileBegin(stmt *parser.BeginStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	// Transaction is started by the engine
	// VDBE just needs to track it
	vm.InTxn = true

	vm.AddOp(vdbe.OpTransaction, 1, 0, 0) // Read-write transaction
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileCommit compiles a COMMIT statement.
func (c *Compiler) CompileCommit(stmt *parser.CommitStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpCommit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// CompileRollback compiles a ROLLBACK statement.
func (c *Compiler) CompileRollback(stmt *parser.RollbackStmt) (*vdbe.VDBE, error) {
	vm := vdbe.New()
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpRollback, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// createCodeGenerator creates a code generator for expression compilation.
func createCodeGenerator(vm *vdbe.VDBE, tableName string, table *schema.Table, cursorIdx int) *expr.CodeGenerator {
	codegen := expr.NewCodeGenerator(vm)
	codegen.RegisterCursor(tableName, cursorIdx)

	// Register table columns
	columns := make([]expr.ColumnInfo, len(table.Columns))
	for i, col := range table.Columns {
		columns[i] = expr.ColumnInfo{
			Name:  col.Name,
			Index: i,
		}
	}

	codegen.RegisterTable(expr.TableInfo{
		Name:    tableName,
		Columns: columns,
	})

	// Set next register to avoid conflicts
	codegen.SetNextReg(len(table.Columns) + 1)

	return codegen
}

// createMultiTableCodeGenerator creates a code generator for multi-table queries (JOINs).
func (c *Compiler) createMultiTableCodeGenerator(vm *vdbe.VDBE, tables []tableInfo) *expr.CodeGenerator {
	codegen := expr.NewCodeGenerator(vm)

	// Calculate total number of columns across all tables
	totalCols := 0
	for _, tbl := range tables {
		totalCols += len(tbl.table.Columns)
	}

	// Register all tables and their cursors
	for _, tbl := range tables {
		codegen.RegisterCursor(tbl.name, tbl.cursorIdx)

		// Register table columns
		columns := make([]expr.ColumnInfo, len(tbl.table.Columns))
		for i, col := range tbl.table.Columns {
			columns[i] = expr.ColumnInfo{
				Name:  col.Name,
				Index: i,
			}
		}

		codegen.RegisterTable(expr.TableInfo{
			Name:    tbl.name,
			Columns: columns,
		})
	}

	// Set next register to avoid conflicts
	codegen.SetNextReg(totalCols + 1)

	return codegen
}
