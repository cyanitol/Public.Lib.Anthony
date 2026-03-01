package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/engine"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/security"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// Stmt implements database/sql/driver.Stmt for SQLite.
type Stmt struct {
	conn   *Conn
	query  string
	ast    parser.Statement
	vdbe   *vdbe.VDBE
	closed bool
	mu     sync.Mutex // Protects closed and vdbe fields
}

// Close closes the statement.
func (s *Stmt) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true

	// Finalize VDBE if it exists
	if s.vdbe != nil {
		s.vdbe.Finalize()
		s.vdbe = nil
	}

	// Save connection reference before unlocking
	conn := s.conn
	s.mu.Unlock()

	// Remove from connection's statement map
	// This is done after releasing stmt.mu to avoid deadlock
	// (conn.removeStmt acquires conn.mu)
	if conn != nil {
		conn.removeStmt(s)
	}

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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, driver.ErrBadConn
	}
	s.mu.Unlock()

	// Lock connection for entire execution to prevent concurrent access to pager
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	inTx := s.conn.inTx
	if s.conn.closed {
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
		if !inTx {
			s.conn.pager.Rollback()
		}
		return nil, fmt.Errorf("execution error: %w", err)
	}

	// Auto-commit if not in explicit transaction and pager has a write transaction
	if !inTx && s.conn.pager.InWriteTransaction() {
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, driver.ErrBadConn
	}
	s.mu.Unlock()

	// Check connection state under lock to avoid TOCTOU race
	s.conn.mu.Lock()
	connClosed := s.conn.closed
	s.conn.mu.Unlock()

	if connClosed {
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
	case *parser.ExplainStmt:
		return s.compileExplain(vm, stmt, args)
	default:
		return s.dispatchDDLOrTxn(vm, args)
	}
}

// dispatchDDLOrTxn handles DDL and transaction statements.
func (s *Stmt) dispatchDDLOrTxn(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Try schema DDL statements (CREATE/DROP/ALTER)
	if result, err, handled := s.dispatchSchemaDDL(vm, args); handled {
		return result, err
	}

	// Try transaction control statements
	if result, err, handled := s.dispatchTransactionControl(vm, args); handled {
		return result, err
	}

	// Try other statements (PRAGMA, ATTACH, DETACH, VACUUM)
	if result, err, handled := s.dispatchOtherStatements(vm, args); handled {
		return result, err
	}

	return nil, fmt.Errorf("unsupported statement type: %T", s.ast)
}

// dispatchSchemaDDL handles CREATE/DROP/ALTER statements.
func (s *Stmt) dispatchSchemaDDL(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	// Try table operations
	if result, err, handled := s.dispatchTableDDL(vm, args); handled {
		return result, err, true
	}

	// Try index operations
	if result, err, handled := s.dispatchIndexDDL(vm, args); handled {
		return result, err, true
	}

	// Try view operations
	if result, err, handled := s.dispatchViewDDL(vm, args); handled {
		return result, err, true
	}

	// Try trigger operations
	if result, err, handled := s.dispatchTriggerDDL(vm, args); handled {
		return result, err, true
	}

	return nil, nil, false
}

// dispatchTableDDL handles CREATE/DROP/ALTER TABLE statements.
func (s *Stmt) dispatchTableDDL(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.CreateTableStmt:
		result, err := s.compileCreateTable(vm, stmt, args)
		return result, err, true
	case *parser.DropTableStmt:
		result, err := s.compileDropTable(vm, stmt, args)
		return result, err, true
	case *parser.AlterTableStmt:
		result, err := s.compileAlterTable(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// dispatchIndexDDL handles CREATE/DROP INDEX statements.
func (s *Stmt) dispatchIndexDDL(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.CreateIndexStmt:
		result, err := s.compileCreateIndex(vm, stmt, args)
		return result, err, true
	case *parser.DropIndexStmt:
		result, err := s.compileDropIndex(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// dispatchViewDDL handles CREATE/DROP VIEW statements.
func (s *Stmt) dispatchViewDDL(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.CreateViewStmt:
		result, err := s.compileCreateView(vm, stmt, args)
		return result, err, true
	case *parser.DropViewStmt:
		result, err := s.compileDropView(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// dispatchTriggerDDL handles CREATE/DROP TRIGGER statements.
func (s *Stmt) dispatchTriggerDDL(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.CreateTriggerStmt:
		result, err := s.compileCreateTrigger(vm, stmt, args)
		return result, err, true
	case *parser.DropTriggerStmt:
		result, err := s.compileDropTrigger(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// dispatchTransactionControl handles BEGIN/COMMIT/ROLLBACK statements.
func (s *Stmt) dispatchTransactionControl(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.BeginStmt:
		result, err := s.compileBegin(vm, stmt, args)
		return result, err, true
	case *parser.CommitStmt:
		result, err := s.compileCommit(vm, stmt, args)
		return result, err, true
	case *parser.RollbackStmt:
		result, err := s.compileRollback(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// dispatchOtherStatements handles PRAGMA, ATTACH, DETACH, VACUUM statements.
func (s *Stmt) dispatchOtherStatements(vm *vdbe.VDBE, args []driver.NamedValue) (*vdbe.VDBE, error, bool) {
	switch stmt := s.ast.(type) {
	case *parser.PragmaStmt:
		result, err := s.compilePragma(vm, stmt, args)
		return result, err, true
	case *parser.AttachStmt:
		result, err := s.compileAttach(vm, stmt, args)
		return result, err, true
	case *parser.DetachStmt:
		result, err := s.compileDetach(vm, stmt, args)
		return result, err, true
	case *parser.VacuumStmt:
		result, err := s.compileVacuum(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
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

// expandStarColumns expands any SELECT * or table.* columns into explicit column references.
// Returns the expanded list of columns and their corresponding names.
func expandStarColumns(columns []parser.ResultColumn, table *schema.Table) ([]parser.ResultColumn, []string) {
	var expandedCols []parser.ResultColumn
	var colNames []string

	for _, col := range columns {
		if col.Star {
			// Expand * to all columns from the table
			for _, schemaCol := range table.Columns {
				expandedCols = append(expandedCols, parser.ResultColumn{
					Expr: &parser.IdentExpr{Name: schemaCol.Name},
				})
				colNames = append(colNames, schemaCol.Name)
			}
		} else {
			expandedCols = append(expandedCols, col)
			colNames = append(colNames, selectColName(col, len(colNames)))
		}
	}

	return expandedCols, colNames
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

	if isCountStar(fnExpr) {
		return handleCountStar()
	}

	if isKnownAggregateFunction(funcName) {
		return handleKnownAggregate()
	}

	return handleNonAggregateFunction(vm, fnExpr, targetReg, gen)
}

// isCountStar checks if the expression is COUNT(*).
func isCountStar(fnExpr *parser.FunctionExpr) bool {
	return fnExpr.Name == "COUNT" && fnExpr.Star
}

// handleCountStar handles COUNT(*) - accumulator managed by compileSelectWithAggregates.
func handleCountStar() error {
	// COUNT(*) - the accumulator is managed by compileSelectWithAggregates
	// For now, just mark that this register will hold the count
	// The actual counting logic will be in the scan loop
	return nil
}

// isKnownAggregateFunction checks if the function is a known aggregate function.
func isKnownAggregateFunction(funcName string) bool {
	switch funcName {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL":
		return true
	default:
		return false
	}
}

// handleKnownAggregate handles known aggregate functions - accumulator managed by scan loop.
func handleKnownAggregate() error {
	// The accumulator will be managed by the scan loop
	return nil
}

// handleNonAggregateFunction handles non-aggregate functions with normal code generation.
func handleNonAggregateFunction(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, targetReg int, gen *expr.CodeGenerator) error {
	if gen == nil {
		return fmt.Errorf("unsupported function: %s", fnExpr.Name)
	}

	reg, err := gen.GenerateExpr(fnExpr)
	if err != nil {
		return fmt.Errorf("failed to generate function: %w", err)
	}

	if reg != targetReg {
		vm.AddOp(vdbe.OpCopy, reg, targetReg, 0)
	}

	return nil
}

// stmtTableInfo holds information about a table in a query.
type stmtTableInfo struct {
	name      string
	table     *schema.Table
	cursorIdx int
}

// compileSelectWithJoins compiles a SELECT statement with JOIN clauses.
func (s *Stmt) compileSelectWithJoins(vm *vdbe.VDBE, stmt *parser.SelectStmt, tableName string, table *schema.Table, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Collect all tables involved in the query
	tables, err := s.collectJoinTables(stmt, tableName, table)
	if err != nil {
		return nil, err
	}

	// Setup VDBE and code generator
	numCols, gen := s.setupJoinVDBE(vm, stmt, tables)

	// Emit scan preamble and open cursors
	rewindAddr := s.emitJoinScanSetup(vm, tables)
	loopStart := vm.NumOps()

	// Setup nested loops for joined tables
	innerLoopStarts := s.emitJoinNestedLoops(vm, tables)

	// Emit column reads and result
	if err := s.emitJoinColumns(vm, stmt, tables, numCols, gen); err != nil {
		return nil, err
	}

	// Emit loop cleanup
	s.emitJoinLoopCleanup(vm, tables, innerLoopStarts, loopStart, rewindAddr)

	return vm, nil
}

// collectJoinTables collects all tables involved in a JOIN query.
func (s *Stmt) collectJoinTables(stmt *parser.SelectStmt, tableName string, table *schema.Table) ([]stmtTableInfo, error) {
	baseTableAlias := tableName
	if len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Alias != "" {
		baseTableAlias = stmt.From.Tables[0].Alias
	}
	tables := []stmtTableInfo{{name: baseTableAlias, table: table, cursorIdx: 0}}

	for i, join := range stmt.From.Joins {
		joinTable, ok := s.conn.schema.GetTable(join.Table.TableName)
		if !ok {
			return nil, fmt.Errorf("table not found: %s", join.Table.TableName)
		}
		joinTableAlias := join.Table.TableName
		if join.Table.Alias != "" {
			joinTableAlias = join.Table.Alias
		}
		tables = append(tables, stmtTableInfo{
			name:      joinTableAlias,
			table:     joinTable,
			cursorIdx: i + 1,
		})
	}

	return tables, nil
}

// setupJoinVDBE initializes VDBE and code generator for JOIN query.
func (s *Stmt) setupJoinVDBE(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo) (int, *expr.CodeGenerator) {
	numCols := len(stmt.Columns)
	vm.AllocMemory(numCols + 10)
	vm.AllocCursors(len(tables))

	gen := expr.NewCodeGenerator(vm)
	s.setupSubqueryCompiler(gen)
	for _, tbl := range tables {
		gen.RegisterCursor(tbl.name, tbl.cursorIdx)
	}

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

// emitJoinColumns emits column read operations and result row for JOIN.
func (s *Stmt) emitJoinColumns(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) error {
	for i, col := range stmt.Columns {
		if err := s.emitSelectColumnOpMultiTable(vm, tables, col, i, gen); err != nil {
			return err
		}
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
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
	colIdx := tbl.table.GetColumnIndex(colName)
	if colIdx == -1 {
		return fmt.Errorf("column not found: %s.%s", tbl.name, colName)
	}

	if schemaColIsRowid(tbl.table.Columns[colIdx]) {
		vm.AddOp(vdbe.OpRowid, tbl.cursorIdx, targetReg, 0)
		return nil
	}

	vm.AddOp(vdbe.OpColumn, tbl.cursorIdx, schemaRecordIdx(tbl.table.Columns, colIdx), targetReg)
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

// compileInsert compiles an INSERT statement. CC=3
func (s *Stmt) compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Execute BEFORE INSERT triggers
	if err := s.executeBeforeInsertTriggers(stmt, table); err != nil {
		return nil, fmt.Errorf("BEFORE INSERT trigger failed: %w", err)
	}

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

	// Execute AFTER INSERT triggers
	if err := s.executeAfterInsertTriggers(stmt, table); err != nil {
		return nil, fmt.Errorf("AFTER INSERT trigger failed: %w", err)
	}

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

	// Build update map and column list
	updateMap, updatedColumns := s.buildUpdateMap(stmt)

	// Execute BEFORE UPDATE triggers
	if err := s.executeBeforeUpdateTriggers(stmt, table, updatedColumns); err != nil {
		return nil, fmt.Errorf("BEFORE UPDATE trigger failed: %w", err)
	}

	// Setup VDBE and code generator
	gen, numRecordCols := s.setupUpdateVDBE(vm, table, stmt)

	// Emit update loop
	rewindAddr := s.emitUpdateLoop(vm, stmt, table, updateMap, numRecordCols, gen, args)

	// Execute AFTER UPDATE triggers
	if err := s.executeAfterUpdateTriggers(stmt, table, updatedColumns); err != nil {
		return nil, fmt.Errorf("AFTER UPDATE trigger failed: %w", err)
	}

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

// compileDelete compiles a DELETE statement.
func (s *Stmt) compileDelete(vm *vdbe.VDBE, stmt *parser.DeleteStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)

	// Look up table in schema
	table, ok := s.conn.schema.GetTable(stmt.Table)
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}

	// Execute BEFORE DELETE triggers
	if err := s.executeBeforeDeleteTriggers(stmt, table); err != nil {
		return nil, fmt.Errorf("BEFORE DELETE trigger failed: %w", err)
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

	// Execute AFTER DELETE triggers
	if err := s.executeAfterDeleteTriggers(stmt, table); err != nil {
		return nil, fmt.Errorf("AFTER DELETE trigger failed: %w", err)
	}

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

// compileCreateView compiles a CREATE VIEW statement.
func (s *Stmt) compileCreateView(vm *vdbe.VDBE, stmt *parser.CreateViewStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the view in the schema
	_, err := s.conn.schema.CreateView(stmt)
	if err != nil {
		return nil, err
	}

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileDropView compiles a DROP VIEW statement.
func (s *Stmt) compileDropView(vm *vdbe.VDBE, stmt *parser.DropViewStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if view exists
	_, exists := s.conn.schema.GetView(stmt.Name)
	if !exists {
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("view not found: %s", stmt.Name)
	}

	// Drop the view from the schema
	if err := s.conn.schema.DropView(stmt.Name); err != nil {
		return nil, err
	}

	// In a full implementation, this would:
	// 1. Delete entry from sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileCreateTrigger compiles a CREATE TRIGGER statement.
func (s *Stmt) compileCreateTrigger(vm *vdbe.VDBE, stmt *parser.CreateTriggerStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Create the trigger in the schema
	_, err := s.conn.schema.CreateTrigger(stmt)
	if err != nil {
		if stmt.IfNotExists && err.Error() == fmt.Sprintf("trigger already exists: %s", stmt.Name) {
			// IF NOT EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, err
	}

	// In a full implementation, this would also:
	// 1. Insert entry into sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileDropTrigger compiles a DROP TRIGGER statement.
func (s *Stmt) compileDropTrigger(vm *vdbe.VDBE, stmt *parser.DropTriggerStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(false)
	vm.AllocMemory(10)

	// Check if trigger exists
	_, exists := s.conn.schema.GetTrigger(stmt.Name)
	if !exists {
		if stmt.IfExists {
			// IF EXISTS was specified, silently succeed
			vm.AddOp(vdbe.OpInit, 0, 0, 0)
			vm.AddOp(vdbe.OpHalt, 0, 0, 0)
			return vm, nil
		}
		return nil, fmt.Errorf("trigger not found: %s", stmt.Name)
	}

	// Drop the trigger from the schema
	if err := s.conn.schema.DropTrigger(stmt.Name); err != nil {
		return nil, err
	}

	// In a full implementation, this would:
	// 1. Delete entry from sqlite_master table
	// 2. Update the schema cookie

	vm.AddOp(vdbe.OpInit, 0, 0, 0)
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

// setupSubqueryCompiler configures the CodeGenerator to handle subqueries.
// It provides a callback that compiles subquery SELECT statements.
func (s *Stmt) setupSubqueryCompiler(gen *expr.CodeGenerator) {
	gen.SetSubqueryCompiler(func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error) {
		// Create a new VDBE for the subquery
		subVM := vdbe.New()
		// Compile the subquery SELECT statement
		return s.compileSelect(subVM, selectStmt, nil)
	})
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

// hasFromSubqueries checks if a SELECT statement has subqueries in FROM clause.
func (s *Stmt) hasFromSubqueries(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}

	// Check base tables
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			return true
		}
	}

	// Check JOIN clauses
	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			return true
		}
	}

	return false
}

// compileSelectWithFromSubqueries compiles a SELECT with FROM subqueries.
func (s *Stmt) compileSelectWithFromSubqueries(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Special case: if we have a single FROM subquery and the outer query is simple
	// (just selecting columns, possibly with WHERE/ORDER BY), we can optimize by
	// compiling the subquery directly and handling column references
	if len(stmt.From.Tables) == 1 && stmt.From.Tables[0].Subquery != nil && len(stmt.From.Joins) == 0 {
		return s.compileSingleFromSubquery(vm, stmt, args)
	}

	// Strategy: compile each FROM subquery into a temp table, then compile main query
	// This is a more complex case with multiple subqueries or joins

	// Allocate cursors for all subqueries and main query
	numSubqueries := s.countFromSubqueries(stmt)
	vm.AllocCursors(numSubqueries + 1)
	vm.AllocMemory(50)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Compile each FROM subquery
	cursorIdx := 0
	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			// Compile the subquery
			subVM, err := s.compileSelect(vdbe.New(), table.Subquery, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
			}

			// Create a temp table to hold results
			// In a full implementation, would:
			// 1. Execute subVM to get results
			// 2. Store results in temp table
			// 3. Use temp table in main query

			// For now, emit a comment
			commentOp := vm.AddOp(vdbe.OpNoop, 0, 0, 0)
			vm.Program[commentOp].Comment = fmt.Sprintf("FROM subquery compiled for cursor %d", cursorIdx)
			cursorIdx++

			// Merge the subquery program into main VM
			// This is simplified - real implementation would properly handle temp tables
			vm.Program = append(vm.Program, subVM.Program...)
		}
	}

	// Now compile the main query as normal, but referencing the temp tables
	// For this simplified implementation, we'll just compile it normally
	// A full implementation would track temp table schemas and use them

	// Simplified: compile as if no subquery (assumes flattening occurred)
	if len(stmt.From.Tables) > 0 && stmt.From.Tables[0].Subquery == nil {
		return s.compileSelect(vm, stmt, args)
	}

	// If all tables are subqueries, emit a placeholder result
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// compileSingleFromSubquery compiles a SELECT with a single FROM subquery.
// This handles cases like: SELECT columns FROM (subquery) [WHERE ...] [ORDER BY ...]
func (s *Stmt) compileSingleFromSubquery(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subquery := stmt.From.Tables[0].Subquery

	// Special optimization: SELECT * with no WHERE clause
	if s.isSimpleSelectStar(stmt) {
		return s.compileSimpleSubquery(subquery, args)
	}

	// Complex case: specific columns or WHERE clause
	return s.compileComplexSubquery(stmt, subquery, args)
}

// isSimpleSelectStar checks if statement is SELECT * with no WHERE.
func (s *Stmt) isSimpleSelectStar(stmt *parser.SelectStmt) bool {
	return isSelectStar(stmt) && stmt.Where == nil
}

// compileSimpleSubquery compiles a simple SELECT * subquery.
func (s *Stmt) compileSimpleSubquery(subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}
	// TODO: Handle ORDER BY from outer query
	return subVM, nil
}

// compileComplexSubquery compiles a subquery with column selection or WHERE clause.
func (s *Stmt) compileComplexSubquery(stmt *parser.SelectStmt, subquery *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Compile subquery to get its structure
	subVM, err := s.compileSelect(s.newVDBE(), subquery, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile FROM subquery: %w", err)
	}

	// Map outer columns to subquery columns
	newColumns, err := s.mapSubqueryColumns(stmt, subquery, subVM.ResultCols)
	if err != nil {
		return nil, err
	}

	// Recompile with mapped columns
	modifiedSubquery := copySelectStmtShallow(subquery)
	modifiedSubquery.Columns = newColumns
	return s.compileSelect(s.newVDBE(), modifiedSubquery, args)
}

// mapSubqueryColumns maps outer query columns to subquery columns.
func (s *Stmt) mapSubqueryColumns(stmt *parser.SelectStmt, subquery *parser.SelectStmt, subqueryColumns []string) ([]parser.ResultColumn, error) {
	var newColumns []parser.ResultColumn

	for _, outerCol := range stmt.Columns {
		if outerCol.Star {
			// SELECT * - use all subquery columns
			return subquery.Columns, nil
		}

		if ident, ok := outerCol.Expr.(*parser.IdentExpr); ok {
			col, err := s.findSubqueryColumn(ident.Name, subquery, subqueryColumns)
			if err != nil {
				return nil, err
			}
			newColumns = append(newColumns, col)
		}
	}

	return newColumns, nil
}

// findSubqueryColumn finds a column in the subquery by name.
func (s *Stmt) findSubqueryColumn(name string, subquery *parser.SelectStmt, subqueryColumns []string) (parser.ResultColumn, error) {
	for i, subCol := range subqueryColumns {
		if subCol == name {
			return subquery.Columns[i], nil
		}
	}
	return parser.ResultColumn{}, fmt.Errorf("column not found: %s", name)
}

// copySelectStmtShallow makes a shallow copy of a SELECT statement.
func copySelectStmtShallow(stmt *parser.SelectStmt) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}
	copy := *stmt
	return &copy
}

// isSelectStar checks if SELECT is SELECT *.
func isSelectStar(stmt *parser.SelectStmt) bool {
	if len(stmt.Columns) == 1 {
		col := stmt.Columns[0]
		if col.Star && col.Table == "" {
			return true
		}
	}
	return false
}

// countFromSubqueries counts the number of subqueries in FROM clause.
func (s *Stmt) countFromSubqueries(stmt *parser.SelectStmt) int {
	count := 0
	if stmt.From == nil {
		return 0
	}

	for _, table := range stmt.From.Tables {
		if table.Subquery != nil {
			count++
		}
	}

	for _, join := range stmt.From.Joins {
		if join.Table.Subquery != nil {
			count++
		}
	}

	return count
}

// compileScalarSubquery compiles a scalar subquery (returns single value).
func (s *Stmt) compileScalarSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	// Emit code to execute subquery and store result in targetReg
	// In a full implementation, would:
	// 1. Open a pseudo-cursor for the subquery
	// 2. Execute the subquery
	// 3. Fetch the first (and only) row
	// 4. Store the value in targetReg
	// 5. Verify no more rows (scalar must return 1 row)

	// For now, merge the subquery program and add a comment
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("Scalar subquery -> reg %d", targetReg)

	return nil
}

// compileExistsSubquery compiles an EXISTS subquery.
func (s *Stmt) compileExistsSubquery(vm *vdbe.VDBE, subquery *parser.SelectStmt, targetReg int, args []driver.NamedValue) error {
	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	// Emit code to execute subquery and check if any rows exist
	// EXISTS returns 1 if subquery returns any rows, 0 otherwise

	// Strategy:
	// 1. Execute subquery
	// 2. Try to fetch first row
	// 3. If row exists, set targetReg = 1
	// 4. If no rows, set targetReg = 0

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("EXISTS subquery -> reg %d", targetReg)

	// Set result register (simplified - assumes subquery ran)
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}

// compileInSubquery compiles an IN subquery.
func (s *Stmt) compileInSubquery(vm *vdbe.VDBE, leftExpr parser.Expression, subquery *parser.SelectStmt, targetReg int, gen *expr.CodeGenerator, args []driver.NamedValue) error {
	// Compile the left expression
	leftReg, err := gen.GenerateExpr(leftExpr)
	if err != nil {
		return fmt.Errorf("failed to compile IN left expression: %w", err)
	}

	// Compile the subquery
	subVM, err := s.compileSelect(vdbe.New(), subquery, args)
	if err != nil {
		return fmt.Errorf("failed to compile IN subquery: %w", err)
	}

	// Strategy for IN subquery:
	// 1. Materialize subquery results into a temp table or ephemeral table
	// 2. Use OpFound to check if leftReg value exists in the temp table
	// 3. Set targetReg to 1 if found, 0 otherwise

	// For now, merge the subquery program
	startAddr := len(vm.Program)
	vm.Program = append(vm.Program, subVM.Program...)
	vm.Program[startAddr].Comment = fmt.Sprintf("IN subquery for reg %d -> reg %d", leftReg, targetReg)

	// Simplified: assume value is found
	vm.AddOp(vdbe.OpInteger, 1, targetReg, 0)

	return nil
}

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

	// Set up result columns: id, parent, notused, detail
	vm.ResultCols = []string{"id", "parent", "notused", "detail"}

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
	// Compile the inner statement to get its VDBE program
	innerVM := s.newVDBE()
	compiledVM, err := s.compileInnerStatement(innerVM, stmt.Statement, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile inner statement: %w", err)
	}

	// Set up result columns for EXPLAIN output
	// Format: addr, opcode, p1, p2, p3, p4, p5, comment
	vm.ResultCols = []string{"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"}

	// Allocate memory for result columns (8 columns)
	vm.AllocMemory(20)

	// Emit Init opcode
	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// For each instruction in the compiled program, emit it as a result row
	for i, instr := range compiledVM.Program {
		// Register 0: addr (instruction address)
		vm.AddOp(vdbe.OpInteger, i, 0, 0)

		// Register 1: opcode (as string)
		vm.AddOpWithP4Str(vdbe.OpString8, 0, 1, 0, instr.Opcode.String())

		// Register 2: p1
		vm.AddOp(vdbe.OpInteger, instr.P1, 2, 0)

		// Register 3: p2
		vm.AddOp(vdbe.OpInteger, instr.P2, 3, 0)

		// Register 4: p3
		vm.AddOp(vdbe.OpInteger, instr.P3, 4, 0)

		// Register 5: p4 (format based on type)
		p4str := ""
		switch instr.P4Type {
		case vdbe.P4Int32:
			p4str = fmt.Sprintf("%d", instr.P4.I)
		case vdbe.P4Int64:
			p4str = fmt.Sprintf("%d", instr.P4.I64)
		case vdbe.P4Real:
			p4str = fmt.Sprintf("%g", instr.P4.R)
		case vdbe.P4Static, vdbe.P4Dynamic:
			p4str = instr.P4.Z
		}
		vm.AddOpWithP4Str(vdbe.OpString8, 0, 5, 0, p4str)

		// Register 6: p5
		vm.AddOp(vdbe.OpInteger, int(instr.P5), 6, 0)

		// Register 7: comment
		comment := ""
		if instr.Comment != "" {
			comment = instr.Comment
		}
		vm.AddOpWithP4Str(vdbe.OpString8, 0, 7, 0, comment)

		// Emit result row
		vm.AddOp(vdbe.OpResultRow, 0, 8, 0)
	}

	// Halt
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	return vm, nil
}

// compileInnerStatement compiles the inner statement of an EXPLAIN.
func (s *Stmt) compileInnerStatement(vm *vdbe.VDBE, stmt parser.Statement, args []driver.NamedValue) (*vdbe.VDBE, error) {
	switch innerStmt := stmt.(type) {
	case *parser.SelectStmt:
		return s.compileSelect(vm, innerStmt, args)
	case *parser.InsertStmt:
		return s.compileInsert(vm, innerStmt, args)
	case *parser.UpdateStmt:
		return s.compileUpdate(vm, innerStmt, args)
	case *parser.DeleteStmt:
		return s.compileDelete(vm, innerStmt, args)
	case *parser.CreateTableStmt:
		return s.compileCreateTable(vm, innerStmt, args)
	case *parser.DropTableStmt:
		return s.compileDropTable(vm, innerStmt, args)
	case *parser.CreateViewStmt:
		return s.compileCreateView(vm, innerStmt, args)
	case *parser.DropViewStmt:
		return s.compileDropView(vm, innerStmt, args)
	default:
		return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
	}
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

// validateDatabasePath validates a database file path using the connection's security configuration.
func (s *Stmt) validateDatabasePath(path string) (string, error) {
	if s.conn.securityConfig == nil {
		// No security config, return path as-is (should not happen in normal operation)
		return path, nil
	}
	// Import the security package to use ValidateDatabasePath
	return security.ValidateDatabasePath(path, s.conn.securityConfig)
}
