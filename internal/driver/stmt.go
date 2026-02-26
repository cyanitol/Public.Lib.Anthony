package driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// Stmt implements database/sql/driver.Stmt for SQLite.
type Stmt struct {
	conn   *Conn
	query  string
	ast    parser.Statement
	vdbe   *vdbe.VDBE
	closed bool
}

// Close closes the statement.
func (s *Stmt) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	// Finalize VDBE if it exists
	if s.vdbe != nil {
		s.vdbe.Finalize()
		s.vdbe = nil
	}

	// Remove from connection's statement map
	s.conn.removeStmt(s)

	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	// Count the number of parameters in the AST
	// For now, return -1 to indicate unknown (the driver will check args at exec time)
	return -1
}

// Exec executes a statement that doesn't return rows.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a statement that doesn't return rows.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Compile the statement to VDBE bytecode
	vm, err := s.compile(args)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}
	defer vm.Finalize()

	// Execute the statement
	if err := vm.Run(); err != nil {
		// Rollback on error if in autocommit mode
		if !s.conn.inTx {
			s.conn.pager.Rollback()
		}
		return nil, fmt.Errorf("execution error: %w", err)
	}

	// Auto-commit if not in explicit transaction and pager has a write transaction
	if !s.conn.inTx && s.conn.pager.InWriteTransaction() {
		if err := s.conn.pager.Commit(); err != nil {
			return nil, fmt.Errorf("auto-commit error: %w", err)
		}
	}

	// Return result
	result := &Result{
		lastInsertID: vm.LastInsertID,
		rowsAffected: vm.NumChanges,
	}

	return result, nil
}

// Query executes a query that returns rows.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that returns rows.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, driver.ErrBadConn
	}

	// Compile the statement to VDBE bytecode
	vm, err := s.compile(args)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	// Create rows iterator
	rows := &Rows{
		stmt:    s,
		vdbe:    vm,
		columns: vm.ResultCols,
		ctx:     ctx,
	}

	return rows, nil
}

// compile compiles the SQL statement into VDBE bytecode.
func (s *Stmt) compile(args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm := s.newVDBE()
	return s.dispatchCompile(vm, args)
}

// newVDBE creates a new VDBE with the connection's context.
func (s *Stmt) newVDBE() *vdbe.VDBE {
	vm := vdbe.New()
	vm.Ctx = &vdbe.VDBEContext{
		Btree:  s.conn.btree,
		Pager:  s.conn.pager,
		Schema: s.conn.schema,
	}
	return vm
}

// dispatchCompile routes compilation to the appropriate handler.
func (s *Stmt) dispatchCompile(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error) {
	switch stmt := s.ast.(type) {
	case *parser.SelectStmt:
		return s.compileSelect(vm, stmt, args)
	case *parser.InsertStmt:
		return s.compileInsert(vm, stmt, args)
	case *parser.UpdateStmt:
		return s.compileUpdate(vm, stmt, args)
	case *parser.DeleteStmt:
		return s.compileDelete(vm, stmt, args)
	default:
		return s.dispatchDDLOrTxn(vm, args)
	}
}

// dispatchDDLOrTxn handles DDL and transaction statements.
func (s *Stmt) dispatchDDLOrTxn(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error) {
	switch stmt := s.ast.(type) {
	case *parser.CreateTableStmt:
		return s.compileCreateTable(vm, stmt, args)
	case *parser.DropTableStmt:
		return s.compileDropTable(vm, stmt, args)
	case *parser.BeginStmt:
		return s.compileBegin(vm, stmt, args)
	case *parser.CommitStmt:
		return s.compileCommit(vm, stmt, args)
	case *parser.RollbackStmt:
		return s.compileRollback(vm, stmt, args)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// schemaColIsRowid reports whether a *schema.Column is an INTEGER PRIMARY KEY
// (a rowid alias). Such columns are not stored in the B-tree record itself.
func schemaColIsRowid(col *schema.Column) bool {
	return col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")
}

// selectFromTableName returns the first table name from a SELECT FROM clause.
// It returns an error when no FROM clause or no tables are present.
func selectFromTableName(stmt *parser.SelectStmt) (string, error) {
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return "", fmt.Errorf("SELECT requires FROM clause")
	}
	return stmt.From.Tables[0].TableName, nil
}

// selectColName derives the output column name for a single SELECT column:
// alias > identifier name > positional fallback.
func selectColName(col parser.ResultColumn, pos int) string {
	if col.Alias != "" {
		return col.Alias
	}
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return fmt.Sprintf("column%d", pos+1)
}

// schemaRecordIdx computes the B-tree record index for column colIdx in table.
// It equals the number of non-rowid columns that precede position colIdx.
func schemaRecordIdx(columns []*schema.Column, colIdx int) int {
	recordIdx := 0
	for j := 0; j < colIdx; j++ {
		if !schemaColIsRowid(columns[j]) {
			recordIdx++
		}
	}
	return recordIdx
}

// emitSelectColumnOp emits the VDBE opcode(s) required to read the i-th SELECT
// column into register i. It returns an error when the named column is not
// found in the table.
func emitSelectColumnOp(vm *vdbe.VDBE, table *schema.Table, col parser.ResultColumn, i int, gen *expr.CodeGenerator) error {
	// Check if this is a simple column reference
	ident, ok := col.Expr.(*parser.IdentExpr)
	if ok {
		// Simple column reference - use optimized path
		colIdx := table.GetColumnIndex(ident.Name)
		if colIdx == -1 {
			return fmt.Errorf("column not found: %s", ident.Name)
		}

		if schemaColIsRowid(table.Columns[colIdx]) {
			vm.AddOp(vdbe.OpRowid, 0, i, 0)
			return nil
		}

		vm.AddOp(vdbe.OpColumn, 0, schemaRecordIdx(table.Columns, colIdx), i)
		return nil
	}

	// Check if this is a function expression (COUNT, SUM, etc.)
	fnExpr, isFn := col.Expr.(*parser.FunctionExpr)
	if isFn {
		// Handle aggregate function with proper accumulator
		return emitAggregateFunction(vm, fnExpr, i, gen)
	}

	// For other complex expressions, use the expression code generator
	if gen != nil {
		reg, err := gen.GenerateExpr(col.Expr)
		if err != nil {
			return fmt.Errorf("failed to generate expression: %w", err)
		}
		// Copy result to target register if needed
		if reg != i {
			vm.AddOp(vdbe.OpCopy, reg, i, 0)
		}
		return nil
	}

	// Fallback: emit NULL placeholder
	vm.AddOp(vdbe.OpNull, 0, i, 0)
	return nil
}

// emitAggregateFunction emits VDBE opcodes for aggregate functions like COUNT, SUM, etc.
// This handles the special case where aggregates need accumulators and are evaluated
// across all rows in a scan loop.
func emitAggregateFunction(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, targetReg int, gen *expr.CodeGenerator) error {
	funcName := fnExpr.Name

	// For COUNT(*), we need to count rows in the scan loop
	// The accumulator will be initialized before the loop and incremented in the loop
	if funcName == "COUNT" && fnExpr.Star {
		// COUNT(*) - the accumulator is managed by compileSelectWithAggregates
		// For now, just mark that this register will hold the count
		// The actual counting logic will be in the scan loop
		return nil
	}

	// For COUNT(expr), SUM, AVG, MIN, MAX, etc.
	// These also need accumulator handling in the scan loop
	if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" ||
	   funcName == "MIN" || funcName == "MAX" || funcName == "TOTAL" {
		// The accumulator will be managed by the scan loop
		return nil
	}

	// For non-aggregate functions, generate normal function call
	if gen != nil {
		reg, err := gen.GenerateExpr(fnExpr)
		if err != nil {
			return fmt.Errorf("failed to generate function: %w", err)
		}
		if reg != targetReg {
			vm.AddOp(vdbe.OpCopy, reg, targetReg, 0)
		}
		return nil
	}

	return fmt.Errorf("unsupported function: %s", funcName)
}

// compileSelect compiles a SELECT statement. CC=5
func (s *Stmt) compileSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	tableName, err := selectFromTableName(stmt)
	if err != nil {
		return nil, err
	}

	table, ok := s.conn.schema.GetTable(tableName)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	// Check if we have JOIN clauses
	hasJoins := stmt.From != nil && len(stmt.From.Joins) > 0

	if hasJoins {
		return s.compileSelectWithJoins(vm, stmt, tableName, table, args)
	}

	// Check if we have aggregate functions
	hasAggregates := s.detectAggregates(stmt)
	if hasAggregates {
		return s.compileSelectWithAggregates(vm, stmt, tableName, table, args)
	}

	// Check if we have ORDER BY
	hasOrderBy := len(stmt.OrderBy) > 0

	// Single table SELECT without aggregates
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 30) // Extra registers for sorting
	vm.AllocCursors(1)

	// Create expression code generator
	gen := expr.NewCodeGenerator(vm)
	gen.RegisterCursor(tableName, 0)

	// Register table info for column resolution
	tableInfo := buildTableInfo(tableName, table)
	gen.RegisterTable(tableInfo)

	// Set up args for parameter binding
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}
	gen.SetArgs(argValues)

	// Build result column name list.
	vm.ResultCols = make([]string, numCols)
	for i, col := range stmt.Columns {
		vm.ResultCols[i] = selectColName(col, i)
	}

	if hasOrderBy {
		return s.compileSelectWithOrderBy(vm, stmt, table, gen, numCols)
	}

	// No ORDER BY - original simple implementation
	// Emit scan preamble.
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Handle WHERE clause if present
	var skipAddr int
	if stmt.Where != nil {
		// Generate code for WHERE expression
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE clause: %w", err)
		}
		// Skip this row if WHERE condition is false
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Emit one column-read op per SELECT column.
	for i, col := range stmt.Columns {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return nil, err
		}
	}

	// Emit scan tail.
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Fix up WHERE skip address to point to Next
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// compileSelectWithOrderBy handles SELECT with ORDER BY clause using a sorter.
func (s *Stmt) compileSelectWithOrderBy(vm *vdbe.VDBE, stmt *parser.SelectStmt, table *schema.Table, gen *expr.CodeGenerator, numCols int) (*vdbe.VDBE, error) {
	// Build a list of columns to store in sorter:
	// - All SELECT columns first (at positions 0..numCols-1)
	// - Additional ORDER BY columns that aren't in SELECT (at positions numCols..)

	// Track extra columns needed for ORDER BY
	extraCols := make([]string, 0) // Column names not in SELECT
	extraColRegs := make([]int, 0) // Register numbers for extra columns

	// Determine which columns to sort by and their position in the sorter row
	keyCols := make([]int, len(stmt.OrderBy))
	desc := make([]bool, len(stmt.OrderBy))

	for i, orderTerm := range stmt.OrderBy {
		colIdx := -1
		var orderColName string

		if ident, ok := orderTerm.Expr.(*parser.IdentExpr); ok {
			orderColName = ident.Name

			// Search by column name in SELECT columns
			for j, selCol := range stmt.Columns {
				if selCol.Alias == ident.Name {
					colIdx = j
					break
				}
				if selColIdent, ok := selCol.Expr.(*parser.IdentExpr); ok {
					if selColIdent.Name == ident.Name {
						colIdx = j
						break
					}
				}
			}
		}

		if colIdx < 0 && orderColName != "" {
			// ORDER BY column is not in SELECT list
			// Check if we already added it to extra columns
			for j, extraCol := range extraCols {
				if extraCol == orderColName {
					colIdx = numCols + j
					break
				}
			}

			// If still not found, add as a new extra column
			if colIdx < 0 {
				colIdx = numCols + len(extraCols)
				extraCols = append(extraCols, orderColName)
				extraColRegs = append(extraColRegs, gen.AllocReg())
			}
		}

		if colIdx < 0 {
			// Default to first column if we can't resolve
			colIdx = 0
		}

		keyCols[i] = colIdx
		desc[i] = !orderTerm.Asc
	}

	// Total columns in sorter = SELECT columns + extra ORDER BY columns
	sorterCols := numCols + len(extraCols)

	// Reserve registers for result columns so AllocReg doesn't reuse them
	// Result columns use registers 0..numCols-1 for output
	// Extra ORDER BY columns use registers numCols..sorterCols-1 for sorting
	gen.SetNextReg(sorterCols)

	// Create sorter key info
	keyInfo := &vdbe.SorterKeyInfo{
		KeyCols: keyCols,
		Desc:    desc,
	}

	// Emit scan preamble
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))

	// Open sorter with the full number of columns
	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, 0, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Handle WHERE clause if present
	var skipAddr int
	if stmt.Where != nil {
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE clause: %w", err)
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Read SELECT columns into registers 0..numCols-1
	for i, col := range stmt.Columns {
		if err := emitSelectColumnOp(vm, table, col, i, gen); err != nil {
			return nil, err
		}
	}

	// Read extra ORDER BY columns into their registers
	for i, colName := range extraCols {
		tableColIdx := table.GetColumnIndex(colName)
		if tableColIdx >= 0 {
			recordIdx := schemaRecordIdx(table.Columns, tableColIdx)
			vm.AddOp(vdbe.OpColumn, 0, recordIdx, extraColRegs[i])
		} else {
			vm.AddOp(vdbe.OpNull, 0, extraColRegs[i], 0)
		}
	}

	// Insert row into sorter - need to use a contiguous range of registers
	// Copy extra column registers to positions after SELECT columns
	for i := range extraCols {
		vm.AddOp(vdbe.OpCopy, extraColRegs[i], numCols+i, 0)
	}
	vm.AddOp(vdbe.OpSorterInsert, 0, 0, sorterCols)

	// Fix up WHERE skip address to point to Next
	if stmt.Where != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}

	// Next row from table
	vm.AddOp(vdbe.OpNext, 0, rewindAddr+1, 0)

	// Close table cursor
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Sort the collected rows
	sorterSortAddr := vm.AddOp(vdbe.OpSorterSort, 0, 0, 0)

	// Handle LIMIT and OFFSET - parse and set up counters
	var limitReg, limitVal int
	var offsetReg, offsetVal int
	hasLimit := false
	hasOffset := false

	if stmt.Limit != nil {
		if litExpr, ok := stmt.Limit.(*parser.LiteralExpr); ok {
			var parsedVal int64
			if _, err := fmt.Sscanf(litExpr.Value, "%d", &parsedVal); err == nil {
				hasLimit = true
				limitVal = int(parsedVal)
				limitReg = gen.AllocReg()
				// Initialize limit counter to 0
				vm.AddOp(vdbe.OpInteger, 0, limitReg, 0)
			}
		}
	}

	if stmt.Offset != nil {
		if litExpr, ok := stmt.Offset.(*parser.LiteralExpr); ok {
			var parsedVal int64
			if _, err := fmt.Sscanf(litExpr.Value, "%d", &parsedVal); err == nil {
				hasOffset = true
				offsetVal = int(parsedVal)
				offsetReg = gen.AllocReg()
				// Initialize offset counter to 0
				vm.AddOp(vdbe.OpInteger, 0, offsetReg, 0)
			}
		}
	}

	// First SorterNext call to move to first row
	// This jumps to loop body if there are rows
	sorterNextAddr := vm.AddOp(vdbe.OpSorterNext, 0, 0, 0) // P2 will be patched to loop body

	// If no rows, jump to halt
	haltJumpAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0) // P2 will be patched to halt

	// Output loop body: read from sorter and emit result rows
	sorterLoopAddr := vm.NumOps()

	// Copy data from sorter to result registers
	vm.AddOp(vdbe.OpSorterData, 0, 0, numCols)

	// Check OFFSET if present - skip first offsetVal rows
	var offsetSkipAddr int
	if hasOffset {
		// Increment offset counter
		vm.AddOp(vdbe.OpAddImm, offsetReg, 1, 0)
		// Compare counter to offset value - if counter <= offset, skip this row
		offsetCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, offsetVal, offsetCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpLe, offsetReg, offsetCheckReg, cmpReg) // cmpReg = 1 if counter <= offset
		offsetSkipAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)     // Jump to next if still in offset range
	}

	// Check LIMIT if present
	var limitJumpAddr int
	if hasLimit {
		// Increment counter
		vm.AddOp(vdbe.OpAddImm, limitReg, 1, 0)
		// Compare counter to limit
		limitCheckReg := gen.AllocReg()
		vm.AddOp(vdbe.OpInteger, limitVal, limitCheckReg, 0)
		cmpReg := gen.AllocReg()
		vm.AddOp(vdbe.OpGt, limitReg, limitCheckReg, cmpReg) // cmpReg = 1 if counter > limit
		limitJumpAddr = vm.AddOp(vdbe.OpIf, cmpReg, 0, 0)    // Jump to halt if over limit
	}

	// Emit result row
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Get next sorted row - jump back to loop body if more rows
	nextRowAddr := vm.AddOp(vdbe.OpSorterNext, 0, sorterLoopAddr, 0)

	// Close sorter and halt
	haltAddr := vm.NumOps()
	vm.AddOp(vdbe.OpSorterClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix up addresses
	vm.Program[rewindAddr].P2 = haltAddr
	vm.Program[sorterSortAddr].P2 = haltAddr // Jump to close if empty
	vm.Program[sorterNextAddr].P2 = sorterLoopAddr // Jump to loop body if there are rows
	vm.Program[haltJumpAddr].P2 = haltAddr
	if hasOffset {
		vm.Program[offsetSkipAddr].P2 = nextRowAddr // Skip to next row if still in offset range
	}
	if hasLimit {
		vm.Program[limitJumpAddr].P2 = haltAddr
	}

	return vm, nil
}

// detectAggregates checks if a SELECT statement contains aggregate functions
func (s *Stmt) detectAggregates(stmt *parser.SelectStmt) bool {
	for _, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
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

// compileSelectWithAggregates compiles a SELECT with aggregate functions
func (s *Stmt) compileSelectWithAggregates(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	numCols := len(stmt.Columns)

	// Allocate extra registers for accumulators
	vm.AllocMemory(numCols + 20)
	vm.AllocCursors(1)

	// Create expression code generator
	gen := expr.NewCodeGenerator(vm)
	gen.RegisterCursor(tableName, 0)

	// Build result column name list
	vm.ResultCols = make([]string, numCols)
	for i, col := range stmt.Columns {
		vm.ResultCols[i] = selectColName(col, i)
	}

	// Allocate accumulator registers for each aggregate
	// For AVG, we need a second register for count
	accRegs := make([]int, numCols)
	avgCountRegs := make([]int, numCols) // Only used for AVG
	for i, col := range stmt.Columns {
		fnExpr, isAgg := col.Expr.(*parser.FunctionExpr)
		if isAgg && s.isAggregateExpr(col.Expr) {
			accRegs[i] = gen.AllocReg()

			// Initialize accumulator based on function type
			switch fnExpr.Name {
			case "COUNT":
				// COUNT starts at 0
				vm.AddOp(vdbe.OpInteger, 0, accRegs[i], 0)
			case "AVG":
				// AVG needs sum (NULL initially) and count (0 initially)
				vm.AddOp(vdbe.OpNull, 0, accRegs[i], 0) // sum starts as NULL
				avgCountRegs[i] = gen.AllocReg()
				vm.AddOp(vdbe.OpInteger, 0, avgCountRegs[i], 0) // count starts at 0
			case "SUM", "MIN", "MAX", "TOTAL":
				// Other aggregates start as NULL
				vm.AddOp(vdbe.OpNull, 0, accRegs[i], 0)
			}
		}
	}

	// Emit scan preamble
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpOpenRead, 0, int(table.RootPage), len(table.Columns))
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Scan loop: update accumulators
	loopStart := vm.NumOps()

	for i, col := range stmt.Columns {
		fnExpr, isAgg := col.Expr.(*parser.FunctionExpr)
		if !isAgg || !s.isAggregateExpr(col.Expr) {
			continue
		}

		// Update accumulator based on function type
		switch fnExpr.Name {
		case "COUNT":
			if fnExpr.Star {
				// COUNT(*) - always increment
				vm.AddOp(vdbe.OpAddImm, accRegs[i], 1, 0)
			} else {
				// COUNT(expr) - increment if not NULL
				// TODO: evaluate expression and check for NULL
				vm.AddOp(vdbe.OpAddImm, accRegs[i], 1, 0)
			}
		case "SUM", "TOTAL":
			// SUM(expr) - Add value to accumulator
			// Get the expression argument
			if len(fnExpr.Args) > 0 {
				// Get the column value
				argIdent, ok := fnExpr.Args[0].(*parser.IdentExpr)
				if ok {
					colIdx := table.GetColumnIndex(argIdent.Name)
					if colIdx >= 0 {
						// Load column value into a temp register
						tempReg := gen.AllocReg()
						recordIdx := schemaRecordIdx(table.Columns, colIdx)
						vm.AddOp(vdbe.OpColumn, 0, recordIdx, tempReg)

						// Skip NULL values - if tempReg is NULL, skip this row
						skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0) // P2 will be patched

						// If accumulator is NOT NULL, jump to add instruction
						addAddr := vm.AddOp(vdbe.OpNotNull, accRegs[i], 0, 0) // P2 will be patched

						// Accumulator is NULL - copy the first value, then skip add
						vm.AddOp(vdbe.OpCopy, tempReg, accRegs[i], 0)
						skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0) // Jump past the add

						// Accumulator is not NULL - add to it
						vm.Program[addAddr].P2 = vm.NumOps()
						vm.AddOp(vdbe.OpAdd, accRegs[i], tempReg, accRegs[i])

						// Patch the skip jumps to here (end of SUM logic for this row)
						endAddr := vm.NumOps()
						vm.Program[skipAddr].P2 = endAddr
						vm.Program[skipToEndAddr].P2 = endAddr
					}
				}
			}
		case "AVG":
			// AVG(expr) - Add value to sum accumulator and increment count
			if len(fnExpr.Args) > 0 {
				argIdent, ok := fnExpr.Args[0].(*parser.IdentExpr)
				if ok {
					colIdx := table.GetColumnIndex(argIdent.Name)
					if colIdx >= 0 {
						// Load column value into a temp register
						tempReg := gen.AllocReg()
						recordIdx := schemaRecordIdx(table.Columns, colIdx)
						vm.AddOp(vdbe.OpColumn, 0, recordIdx, tempReg)

						// Skip NULL values - if tempReg is NULL, skip this row
						skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0) // P2 will be patched

						// Increment count (always for non-NULL values)
						vm.AddOp(vdbe.OpAddImm, avgCountRegs[i], 1, 0)

						// If sum accumulator is NOT NULL, jump to add instruction
						addAddr := vm.AddOp(vdbe.OpNotNull, accRegs[i], 0, 0) // P2 will be patched

						// Sum is NULL - copy the first value, then skip add
						vm.AddOp(vdbe.OpCopy, tempReg, accRegs[i], 0)
						skipToEndAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0) // Jump past the add

						// Sum is not NULL - add to it
						vm.Program[addAddr].P2 = vm.NumOps()
						vm.AddOp(vdbe.OpAdd, accRegs[i], tempReg, accRegs[i])

						// Patch the skip jumps to here
						endAddr := vm.NumOps()
						vm.Program[skipAddr].P2 = endAddr
						vm.Program[skipToEndAddr].P2 = endAddr
					}
				}
			}
		case "MIN":
			// MIN(expr) - Keep smallest value
			if len(fnExpr.Args) > 0 {
				argIdent, ok := fnExpr.Args[0].(*parser.IdentExpr)
				if ok {
					colIdx := table.GetColumnIndex(argIdent.Name)
					if colIdx >= 0 {
						// Load column value into a temp register
						tempReg := gen.AllocReg()
						recordIdx := schemaRecordIdx(table.Columns, colIdx)
						vm.AddOp(vdbe.OpColumn, 0, recordIdx, tempReg)

						// Skip NULL values
						skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0) // P2 will be patched

						// If accumulator is NULL, just copy the value (first value)
						copyAddr := vm.AddOp(vdbe.OpIsNull, accRegs[i], 0, 0) // Jump to copy

						// Accumulator is not NULL - compare
						cmpReg := gen.AllocReg()
						vm.AddOp(vdbe.OpLt, tempReg, accRegs[i], cmpReg) // cmpReg = 1 if tempReg < accRegs[i]
						notLessAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0) // Jump if NOT less

						// Copy value (either first value or new min)
						vm.Program[copyAddr].P2 = vm.NumOps()
						vm.AddOp(vdbe.OpCopy, tempReg, accRegs[i], 0)

						// End of MIN logic
						endAddr := vm.NumOps()
						vm.Program[skipAddr].P2 = endAddr
						vm.Program[notLessAddr].P2 = endAddr
					}
				}
			}
		case "MAX":
			// MAX(expr) - Keep largest value
			if len(fnExpr.Args) > 0 {
				argIdent, ok := fnExpr.Args[0].(*parser.IdentExpr)
				if ok {
					colIdx := table.GetColumnIndex(argIdent.Name)
					if colIdx >= 0 {
						// Load column value into a temp register
						tempReg := gen.AllocReg()
						recordIdx := schemaRecordIdx(table.Columns, colIdx)
						vm.AddOp(vdbe.OpColumn, 0, recordIdx, tempReg)

						// Skip NULL values
						skipAddr := vm.AddOp(vdbe.OpIsNull, tempReg, 0, 0) // P2 will be patched

						// If accumulator is NULL, just copy the value (first value)
						copyAddr := vm.AddOp(vdbe.OpIsNull, accRegs[i], 0, 0) // Jump to copy

						// Accumulator is not NULL - compare
						cmpReg := gen.AllocReg()
						vm.AddOp(vdbe.OpGt, tempReg, accRegs[i], cmpReg) // cmpReg = 1 if tempReg > accRegs[i]
						notGreaterAddr := vm.AddOp(vdbe.OpIfNot, cmpReg, 0, 0) // Jump if NOT greater

						// Copy value (either first value or new max)
						vm.Program[copyAddr].P2 = vm.NumOps()
						vm.AddOp(vdbe.OpCopy, tempReg, accRegs[i], 0)

						// End of MAX logic
						endAddr := vm.NumOps()
						vm.Program[skipAddr].P2 = endAddr
						vm.Program[notGreaterAddr].P2 = endAddr
					}
				}
			}
		}
	}

	// Continue scan
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)

	// After scan completes (rewind jumps here when no more rows)
	// We need to output aggregate results even if there were no rows
	afterScanAddr := vm.NumOps()

	// Finalize aggregates and copy to result registers
	for i, col := range stmt.Columns {
		if s.isAggregateExpr(col.Expr) {
			fnExpr := col.Expr.(*parser.FunctionExpr)
			if fnExpr.Name == "AVG" {
				// AVG = sum / count (result goes in result register i)
				// If sum is NULL (no rows), result is NULL
				// Otherwise divide sum by count
				vm.AddOp(vdbe.OpDivide, accRegs[i], avgCountRegs[i], i)
			} else {
				vm.AddOp(vdbe.OpCopy, accRegs[i], i, 0)
			}
		} else {
			// Non-aggregate column in aggregate query (should be constant or error)
			vm.AddOp(vdbe.OpNull, 0, i, 0) // P2=destination register, P3=0 for single register
		}
	}

	// Output the single result row (always output for aggregates, even with 0 input rows)
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Close and halt
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix up the rewind jump to go to the aggregate output code (not halt)
	vm.Program[rewindAddr].P2 = afterScanAddr

	return vm, nil
}

// stmtTableInfo holds information about a table in a query.
type stmtTableInfo struct {
	name      string
	table     *schema.Table
	cursorIdx int
}

// compileSelectWithJoins compiles a SELECT statement with JOIN clauses.
func (s *Stmt) compileSelectWithJoins(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Collect all tables involved in the query (base table + JOINs)
	tables := []stmtTableInfo{{name: tableName, table: table, cursorIdx: 0}}

	// Process JOIN clauses
	for i, join := range stmt.From.Joins {
		joinTable, ok := s.conn.schema.GetTable(join.Table.TableName)
		if !ok {
			return nil, fmt.Errorf("table not found: %s", join.Table.TableName)
		}
		tables = append(tables, stmtTableInfo{
			name:      join.Table.TableName,
			table:     joinTable,
			cursorIdx: i + 1,
		})
	}

	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 10)
	vm.AllocCursors(len(tables))

	// Create expression code generator
	gen := expr.NewCodeGenerator(vm)
	for _, tbl := range tables {
		gen.RegisterCursor(tbl.name, tbl.cursorIdx)
	}

	// Build result column name list
	vm.ResultCols = s.buildMultiTableColumnNames(stmt.Columns, tables)

	// Emit scan preamble
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open cursors for all tables
	for _, tbl := range tables {
		vm.AddOp(vdbe.OpOpenRead, tbl.cursorIdx, int(tbl.table.RootPage), len(tbl.table.Columns))
	}

	// Set up nested loop for first table
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)
	loopStart := vm.NumOps()

	// Set up nested loops for joined tables
	var innerLoopStarts []int
	for i := 1; i < len(tables); i++ {
		vm.AddOp(vdbe.OpRewind, i, 0, 0) // Will fix P2 later if needed
		innerLoopStarts = append(innerLoopStarts, vm.NumOps())
	}

	// Emit column-read ops
	for i, col := range stmt.Columns {
		if err := s.emitSelectColumnOpMultiTable(vm, tables, col, i, gen); err != nil {
			return nil, err
		}
	}

	// Emit result row
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)

	// Next operations for joined tables (innermost to outermost)
	for i := len(tables) - 1; i > 0; i-- {
		vm.AddOp(vdbe.OpNext, i, innerLoopStarts[i-1], 0)
		vm.AddOp(vdbe.OpClose, i, 0, 0)
	}

	// Next operation for base table
	vm.AddOp(vdbe.OpNext, 0, loopStart, 0)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// emitSelectColumnOpMultiTable emits the VDBE opcode(s) for reading a column across multiple tables.
func (s *Stmt) emitSelectColumnOpMultiTable(vm *vdbe.VDBE, tables []stmtTableInfo, col parser.ResultColumn, i int, gen *expr.CodeGenerator) error {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		// Non-identifier expression: try using expression code generator
		if gen != nil {
			reg, err := gen.GenerateExpr(col.Expr)
			if err != nil {
				// If code generation fails, emit NULL placeholder
				vm.AddOp(vdbe.OpNull, 0, i, 0)
				return nil
			}
			// Copy result to target register if needed
			if reg != i {
				vm.AddOp(vdbe.OpCopy, reg, i, 0)
			}
			return nil
		}
		// No code generator: emit NULL placeholder
		vm.AddOp(vdbe.OpNull, 0, i, 0)
		return nil
	}

	// If qualified (table.column), search only that table
	if ident.Table != "" {
		for _, tbl := range tables {
			if tbl.name == ident.Table || tbl.table.Name == ident.Table {
				colIdx := tbl.table.GetColumnIndex(ident.Name)
				if colIdx == -1 {
					return fmt.Errorf("column not found: %s.%s", ident.Table, ident.Name)
				}

				if schemaColIsRowid(tbl.table.Columns[colIdx]) {
					vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, i, 0)
					return nil
				}

				vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, schemaRecordIdx(tbl.table.Columns, colIdx), i)
				return nil
			}
		}
		return fmt.Errorf("table not found: %s", ident.Table)
	}

	// For unqualified names, search all tables in join order
	for _, tbl := range tables {
		colIdx := tbl.table.GetColumnIndex(ident.Name)
		if colIdx >= 0 {
			if schemaColIsRowid(tbl.table.Columns[colIdx]) {
				vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, i, 0)
				return nil
			}

			vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, schemaRecordIdx(tbl.table.Columns, colIdx), i)
			return nil
		}
	}

	return fmt.Errorf("column not found: %s", ident.Name)
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

// insertFirstRow validates that stmt has a VALUES clause and returns the first
// value row. It returns an error when no values are present.
func insertFirstRow(stmt *parser.InsertStmt) ([]parser.Expression, error) {
	if len(stmt.Values) == 0 {
		return nil, fmt.Errorf("INSERT requires VALUES clause")
	}
	return stmt.Values[0], nil
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
func (s *Stmt) emitInsertRowid(vm *vdbe.VDBE, row []parser.Expression, rowidColIdx int, rowidReg int, args []driver.NamedValue, paramIdx *int) {
	if rowidColIdx >= 0 {
		s.compileValue(vm, row[rowidColIdx], rowidReg, args, paramIdx)
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

// compileInsert compiles an INSERT statement. CC=3
func (s *Stmt) compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	row, err := insertFirstRow(stmt)
	if err != nil {
		return nil, err
	}

	colNames := resolveInsertColumns(stmt, table)
	rowidColIdx := findInsertRowidCol(colNames, table)

	numRecordCols := len(row)
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
	s.emitInsertRowid(vm, row, rowidColIdx, rowidReg, args, &paramIdx)
	s.emitInsertRecordValues(vm, row, rowidColIdx, recordStartReg, args, &paramIdx)

	resultReg := recordStartReg + numRecordCols
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)
	vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidReg)
	vm.AddOp(vdbe.OpClose, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
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

// compileUpdate compiles an UPDATE statement.
func (s *Stmt) compileUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Look up table in schema
	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Build map of columns being updated
	updateMap := make(map[string]parser.Expression)
	for _, assign := range stmt.Sets {
		updateMap[assign.Column] = assign.Value
	}

	// Count non-rowid columns for record creation
	numRecordCols := 0
	for _, col := range table.Columns {
		if !schemaColIsRowid(col) {
			numRecordCols++
		}
	}

	// Allocate sufficient memory and cursors
	vm.AllocMemory(numRecordCols + 20)
	vm.AllocCursors(1)

	// Initialize program
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Open table for writing (cursor 0)
	vm.AddOp(vdbe.OpOpenWrite, 0, int(table.RootPage), len(table.Columns))

	// Start iteration from beginning
	rewindAddr := vm.AddOp(vdbe.OpRewind, 0, 0, 0)

	// Create code generator for expression compilation
	gen := expr.NewCodeGenerator(vm)
	gen.RegisterCursor(stmt.Table, 0)

	// Register table info for column resolution
	tableInfo := buildTableInfo(stmt.Table, table)
	gen.RegisterTable(tableInfo)

	// Set up args for parameter binding
	// Count params in SET expressions first to offset WHERE params correctly
	setParamCount := countParams(stmt.Sets)
	argValues := make([]interface{}, len(args))
	for i, a := range args {
		argValues[i] = a.Value
	}

	// Allocate register for rowid
	rowidReg := gen.AllocReg()
	vm.AddOp(vdbe.OpRowid, 0, rowidReg, 0)

	// If WHERE clause exists, compile and evaluate it
	// WHERE params come after SET params in the args list
	var skipAddr int
	if stmt.Where != nil {
		// Set args with offset for WHERE clause (params start after SET params)
		whereArgs := argValues[setParamCount:]
		gen.SetArgs(whereArgs)

		// Generate code for WHERE expression
		whereReg, err := gen.GenerateExpr(stmt.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE clause: %w", err)
		}

		// Skip update if WHERE condition is false (OpIfNot jumps when register is false/0)
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Reset args for SET clause (params start at index 0)
	gen.SetArgs(argValues)

	// Load column values into registers for new record
	// For each column in the table, either:
	//   - Use the SET value if this column is being updated
	//   - Load existing value from current row if not being updated
	recordStartReg := gen.AllocRegs(numRecordCols)
	reg := recordStartReg
	for colIdx, col := range table.Columns {
		if schemaColIsRowid(col) {
			continue // rowid is handled separately
		}

		if updateExpr, isUpdated := updateMap[col.Name]; isUpdated {
			// This column is being updated - compile the new value expression
			valReg, err := gen.GenerateExpr(updateExpr)
			if err != nil {
				return nil, fmt.Errorf("failed to compile SET expression for column %s: %w", col.Name, err)
			}
			// Copy the computed value to the record register
			vm.AddOp(vdbe.OpCopy, valReg, reg, 0)
		} else {
			// This column is NOT being updated - load existing value
			recordIdx := schemaRecordIdx(table.Columns, colIdx)
			vm.AddOp(vdbe.OpColumn, 0, recordIdx, reg)
		}
		reg++
	}

	// Create new record from the column values
	resultReg := gen.AllocReg()
	vm.AddOp(vdbe.OpMakeRecord, recordStartReg, numRecordCols, resultReg)

	// Delete old row
	vm.AddOp(vdbe.OpDelete, 0, 0, 0)

	// Insert new row with same rowid
	// Set P4.I = 1 to indicate this is part of UPDATE (don't double-count in NumChanges)
	insertAddr := vm.AddOp(vdbe.OpInsert, 0, resultReg, rowidReg)
	vm.Program[insertAddr].P4.I = 1

	// Fix up the skip target if WHERE clause exists
	if stmt.Where != nil {
		// Point skip address to the Next instruction
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

// compileDelete compiles a DELETE statement.
func (s *Stmt) compileDelete(vm *vdbe.VDBE, stmt *parser.DeleteStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Look up table in schema
	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

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

	// Close table cursor
	vm.AddOp(vdbe.OpClose, 0, 0, 0)

	// Halt execution
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix up the rewind instruction to jump to halt when done
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// compileCreateTable compiles a CREATE TABLE statement.
func (s *Stmt) compileCreateTable(vm *vdbe.VDBE, stmt *parser.CreateTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the table in the schema
	// This simplified implementation registers the table in memory
	// A full implementation would also persist to sqlite_master
	table, err := s.conn.schema.CreateTable(stmt)
	if err != nil {
		return nil, err
	}

	// Allocate a root page for the table btree
	if s.conn.btree != nil {
		rootPage, err := s.conn.btree.CreateTable()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate table root page: %w", err)
		}
		table.RootPage = rootPage
	} else {
		// For in-memory databases without btree, use a placeholder
		table.RootPage = 2
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileDropTable compiles a DROP TABLE statement.
func (s *Stmt) compileDropTable(vm *vdbe.VDBE, stmt *parser.DropTableStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// In a real implementation, this would:
	// 1. Remove entry from sqlite_master
	// 2. Free all pages used by the table
	// 3. Update the schema in memory

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// TODO: Generate bytecode to:
	// - Delete from sqlite_master table
	// - Free table pages
	// - Update schema cookie

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileBegin compiles a BEGIN statement.
func (s *Stmt) compileBegin(vm *vdbe.VDBE, stmt *parser.BeginStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.InTxn = true

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileCommit compiles a COMMIT statement.
func (s *Stmt) compileCommit(vm *vdbe.VDBE, stmt *parser.CommitStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	// TODO: Add commit opcode
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileRollback compiles a ROLLBACK statement.
func (s *Stmt) compileRollback(vm *vdbe.VDBE, stmt *parser.RollbackStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	vm.AddOp(vdbe.OpInit, 0, 3, 0)
	// TODO: Add rollback opcode
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// valuesToNamedValues converts []driver.Value to []driver.NamedValue
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	nv := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nv[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return nv
}

// buildTableInfo creates TableInfo for the code generator from schema.Table.
func buildTableInfo(tableName string, table *schema.Table) expr.TableInfo {
	var columns []expr.ColumnInfo
	recordIdx := 0
	for _, col := range table.Columns {
		isRowid := schemaColIsRowid(col)
		colInfo := expr.ColumnInfo{
			Name:    col.Name,
			Index:   recordIdx,
			IsRowid: isRowid,
		}
		columns = append(columns, colInfo)
		if !isRowid {
			recordIdx++
		}
	}
	return expr.TableInfo{
		Name:    tableName,
		Columns: columns,
	}
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
