// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Stmt implements database/sql/driver.Stmt for SQLite.
type Stmt struct {
	conn           *Conn
	query          string
	ast            parser.Statement
	vdbe           *vdbe.VDBE
	closed         bool
	mu             sync.Mutex     // Protects closed and vdbe fields
	windowStateMap map[string]int // Maps OVER clause key → window state index
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
	if err := s.checkStmtClosed(); err != nil {
		return nil, err
	}

	// Acquire the shared write mutex first to serialize write operations across
	// all connections sharing the same database, preventing data races on page
	// buffers between concurrent VM execution and commit/writePage.
	s.conn.writeMu.Lock()
	defer s.conn.writeMu.Unlock()

	// Lock connection for entire execution to prevent concurrent access to pager
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if err := s.checkConnClosed(); err != nil {
		return nil, err
	}

	inTx := s.conn.inTx
	return s.executeAndCommit(args, inTx)
}

// checkStmtClosed checks if the statement is closed
func (s *Stmt) checkStmtClosed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return driver.ErrBadConn
	}
	return nil
}

// checkConnClosed checks if the connection is closed
func (s *Stmt) checkConnClosed() error {
	if s.conn.closed {
		return driver.ErrBadConn
	}
	return nil
}

// executeAndCommit compiles, executes, and commits the statement
func (s *Stmt) executeAndCommit(args []driver.NamedValue, inTx bool) (driver.Result, error) {
	vm, err := s.compile(args)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}
	defer vm.Finalize()

	if err := s.runVMWithRollback(vm, inTx); err != nil {
		return nil, err
	}

	// Check if schema changed during execution (e.g., root page split)
	// If so, invalidate the statement cache
	if vm.SchemaChanged && s.conn.stmtCache != nil {
		s.conn.stmtCache.InvalidateAll()
	}

	// Transaction control statements manage transaction boundaries themselves.
	if err := s.applyTxnControl(); err != nil {
		return nil, err
	}
	if s.txnControlType() != txnNone {
		return s.buildResult(vm), nil
	}

	if err := s.autoCommitIfNeeded(inTx); err != nil {
		return nil, err
	}

	return s.buildResult(vm), nil
}

// applyTxnControl updates connection state after transaction control statements.
func (s *Stmt) applyTxnControl() error {
	switch s.txnControlType() {
	case txnBegin:
		s.conn.inTx = true
		s.conn.sqlTx = true
		s.conn.savepointOnly = false
	case txnCommit:
		s.conn.clearTxState()
	case txnRollback:
		s.conn.clearTxState()
		s.conn.reloadSchemaAfterRollback()
	case txnSavepoint:
		return s.applySavepointControl()
	}
	return nil
}

// applySavepointControl handles SAVEPOINT, RELEASE, and ROLLBACK TO state.
func (s *Stmt) applySavepointControl() error {
	if _, ok := s.ast.(*parser.ReleaseStmt); ok && s.conn.savepointOnly {
		if err := s.commitSavepointTransaction(); err != nil {
			return err
		}
	}
	if rb, ok := s.ast.(*parser.RollbackStmt); ok && rb.Savepoint != "" {
		s.conn.reloadSchemaAfterRollback()
	}
	return nil
}

// commitSavepointTransaction commits a savepoint-only transaction on RELEASE.
func (s *Stmt) commitSavepointTransaction() error {
	if s.conn.pager.InWriteTransaction() {
		if err := s.conn.pager.Commit(); err != nil {
			return fmt.Errorf("auto-commit after release: %w", err)
		}
	}
	s.conn.clearTxState()
	return nil
}

// runVMWithRollback runs the VM and rolls back on error if in autocommit mode
func (s *Stmt) runVMWithRollback(vm *vdbe.VDBE, inTx bool) error {
	if err := vm.Run(); err != nil {
		if !inTx {
			s.conn.pager.Rollback()
		}
		return fmt.Errorf("execution error: %w", err)
	}
	return nil
}

// txnControlType classifies the statement as a transaction control statement.
func (s *Stmt) txnControlType() txnControlKind {
	switch stmt := s.ast.(type) {
	case *parser.BeginStmt:
		return txnBegin
	case *parser.CommitStmt:
		return txnCommit
	case *parser.RollbackStmt:
		if stmt.Savepoint != "" {
			return txnSavepoint // ROLLBACK TO does not end the transaction
		}
		return txnRollback
	case *parser.SavepointStmt, *parser.ReleaseStmt:
		return txnSavepoint
	default:
		return txnNone
	}
}

type txnControlKind int

const (
	txnNone txnControlKind = iota
	txnBegin
	txnCommit
	txnRollback
	txnSavepoint
)

// autoCommitIfNeeded commits if not in a transaction and a write transaction exists
func (s *Stmt) autoCommitIfNeeded(inTx bool) error {
	if !inTx && s.conn.pager.InWriteTransaction() {
		if err := s.conn.pager.Commit(); err != nil {
			return fmt.Errorf("auto-commit error: %w", err)
		}
	}
	return nil
}

// buildResult creates a Result from the VDBE execution
func (s *Stmt) buildResult(vm *vdbe.VDBE) *Result {
	return &Result{
		lastInsertID: vm.LastInsertID,
		rowsAffected: vm.NumChanges,
	}
}

// Query executes a query that returns rows.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that returns rows.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.checkStmtClosed(); err != nil {
		return nil, err
	}

	// Acquire the shared write mutex to serialize with write operations across
	// all connections sharing the same database, preventing data races on page
	// buffers between concurrent VM execution and commit/writePage.
	s.conn.writeMu.Lock()
	defer s.conn.writeMu.Unlock()

	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()

	if err := s.checkConnClosed(); err != nil {
		return nil, err
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

// queryInternal executes a query without acquiring the connection mutex.
// This must only be called when the caller already holds c.mu.
func (s *Stmt) queryInternal(args []driver.NamedValue) (driver.Rows, error) {
	vm, err := s.compile(args)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	rows := &Rows{
		stmt:    s,
		vdbe:    vm,
		columns: vm.ResultCols,
		ctx:     context.Background(),
	}

	return rows, nil
}

// compile compiles the SQL statement into VDBE bytecode.
// It checks the statement cache first and returns a cached VDBE if available.
func (s *Stmt) compile(args []driver.NamedValue) (*vdbe.VDBE, error) {
	useCache := len(args) == 0 && s.conn.stmtCache != nil && !s.conn.hasAttachedDatabases()
	if useCache {
		if cachedVdbe := s.tryGetCachedVdbe(); cachedVdbe != nil {
			return cachedVdbe, nil
		}
	}

	vm := s.newVDBE()
	compiledVdbe, err := s.dispatchCompile(vm, args)
	if err != nil {
		return nil, err
	}

	if useCache {
		s.cacheVdbeIfAppropriate(compiledVdbe, args)
	}
	return compiledVdbe, nil
}

// tryGetCachedVdbe attempts to retrieve cached VDBE
func (s *Stmt) tryGetCachedVdbe() *vdbe.VDBE {
	cachedVdbe := s.conn.stmtCache.Get(s.query)
	if cachedVdbe == nil {
		return nil
	}
	// Reset the VDBE to initial state before re-execution
	cachedVdbe.Reset()
	s.setVdbeContextForDatabase(cachedVdbe, nil)
	return cachedVdbe
}

// setVdbeContext sets the VDBE context for this connection
func (s *Stmt) setVdbeContext(vm *vdbe.VDBE) {
	s.setVdbeContextForDatabase(vm, nil)
}

// setVdbeContextForDatabase sets the VDBE context for a specific database.
// When db is nil, the main database for the connection is used.
func (s *Stmt) setVdbeContextForDatabase(vm *vdbe.VDBE, db *schema.Database) {
	targetSchema := s.conn.schema
	targetPager := s.conn.pager
	targetBtree := s.conn.btree

	if db != nil {
		if db.Schema != nil {
			targetSchema = db.Schema
		}
		if db.Pager != nil {
			targetPager = db.Pager
		}
		if db.Btree != nil {
			targetBtree = db.Btree
		}
	}

	vm.Ctx = &vdbe.VDBEContext{
		Btree:              targetBtree,
		Pager:              interface{}(targetPager),
		Schema:             interface{}(targetSchema),
		CollationRegistry:  interface{}(s.conn.collRegistry),
		FKManager:          interface{}(s.conn.fkManager),
		ForeignKeysEnabled: s.conn.foreignKeysEnabled,
		TriggerCompiler:    NewTriggerRuntime(s.conn),
	}
}

// cacheVdbeIfAppropriate caches VDBE if conditions are met
func (s *Stmt) cacheVdbeIfAppropriate(vm *vdbe.VDBE, args []driver.NamedValue) {
	if len(args) == 0 && s.conn.stmtCache != nil && vm != nil && s.isCacheable() && !s.conn.hasAttachedDatabases() {
		s.conn.stmtCache.Put(s.query, vm)
	}
}

// isCacheable returns true if this statement can be safely cached.
// DDL statements, PRAGMAs, and transaction control statements cannot be cached
// because they embed schema state or connection state at compile time.
func (s *Stmt) isCacheable() bool {
	switch s.ast.(type) {
	case *parser.PragmaStmt, *parser.CreateTableStmt, *parser.DropTableStmt,
		*parser.CreateIndexStmt, *parser.DropIndexStmt, *parser.AlterTableStmt,
		*parser.CreateViewStmt, *parser.DropViewStmt, *parser.CreateVirtualTableStmt,
		*parser.BeginStmt, *parser.CommitStmt, *parser.RollbackStmt,
		*parser.SavepointStmt, *parser.ReleaseStmt:
		return false
	default:
		return true
	}
}

// invalidateStmtCache invalidates the statement cache when schema changes.
// This should be called after any DDL operation (CREATE/DROP/ALTER TABLE/INDEX/VIEW/TRIGGER).
func (s *Stmt) invalidateStmtCache() {
	if s.conn.stmtCache != nil {
		s.conn.stmtCache.InvalidateAll()
	}
}

// newVDBE creates a new VDBE with the connection's context.
func (s *Stmt) newVDBE() *vdbe.VDBE {
	vm := vdbe.New()
	s.setVdbeContextForDatabase(vm, nil)
	return vm
}

// ============================================================================
// Statement Dispatch
// ============================================================================

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
	case *parser.CreateVirtualTableStmt:
		result, err := s.compileCreateVirtualTable(vm, stmt, args)
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

// dispatchTransactionControl handles BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE statements.
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
	case *parser.SavepointStmt:
		result, err := s.compileSavepoint(vm, stmt, args)
		return result, err, true
	case *parser.ReleaseStmt:
		result, err := s.compileRelease(vm, stmt, args)
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
	case *parser.ReindexStmt:
		result, err := s.compileReindex(vm, stmt, args)
		return result, err, true
	default:
		return nil, nil, false
	}
}

// ============================================================================
// Utility Functions
// ============================================================================

// schemaColIsRowid reports whether a *schema.Column is an INTEGER PRIMARY KEY
// (a rowid alias). Such columns are not stored in the B-tree record itself.
// For WITHOUT ROWID tables, INTEGER PRIMARY KEY is NOT a rowid alias - it's
// stored in the record like any other column.
func schemaColIsRowid(col *schema.Column) bool {
	return col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")
}

// schemaColIsRowidForTable checks if a column is a rowid alias, considering
// the table's WITHOUT ROWID status. For WITHOUT ROWID tables, returns false
// since all columns are stored in the record.
func schemaColIsRowidForTable(table *schema.Table, col *schema.Column) bool {
	if table != nil && table.WithoutRowID {
		return false // WITHOUT ROWID tables store all columns in the record
	}
	return schemaColIsRowid(col)
}

// selectFromTableName returns the first table name from a SELECT FROM clause.
// It returns an error when no FROM clause or no tables are present.
func selectFromTableName(stmt *parser.SelectStmt) (string, error) {
	tableRef, err := selectFromTableRef(stmt)
	if err != nil {
		return "", err
	}
	return tableRef.TableName, nil
}

// selectFromTableRef returns the first table reference from a SELECT FROM clause.
// It returns an error when no FROM clause or no tables are present.
func selectFromTableRef(stmt *parser.SelectStmt) (*parser.TableOrSubquery, error) {
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return nil, fmt.Errorf("SELECT requires FROM clause")
	}
	return &stmt.From.Tables[0], nil
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
// For normal tables: equals the number of non-rowid columns that precede position colIdx.
// For WITHOUT ROWID tables: equals colIdx (all columns are in the record).
// This is a legacy function - prefer using schemaRecordIdxForTable.
func schemaRecordIdx(columns []*schema.Column, colIdx int) int {
	recordIdx := 0
	for j := 0; j < colIdx; j++ {
		if !schemaColIsRowid(columns[j]) {
			recordIdx++
		}
	}
	return recordIdx
}

// schemaRecordIdxForTable computes the B-tree record index for column colIdx in a table.
// For normal tables: equals the number of non-rowid columns that precede position colIdx.
// For WITHOUT ROWID tables: equals colIdx (all columns are in the record).
func schemaRecordIdxForTable(table *schema.Table, colIdx int) int {
	if table.WithoutRowID {
		// For WITHOUT ROWID tables, all columns are in the record
		return colIdx
	}
	// For normal tables, skip rowid columns
	return schemaRecordIdx(table.Columns, colIdx)
}

// emitSelectColumnOp emits the VDBE opcode(s) required to read the i-th SELECT
// column into register i. It returns an error when the named column is not
// found in the table. When a CodeGenerator with a non-default cursor mapping
// is available, all expressions are routed through it so the correct cursor is
// used (critical for INSERT..SELECT where the source cursor is not cursor 0).
func emitSelectColumnOp(vm *vdbe.VDBE, table *schema.Table, col parser.ResultColumn, i int, gen *expr.CodeGenerator) error {
	// When the code generator has a non-zero cursor for this table, route
	// through it so that the correct cursor is used for column reads.
	if gen != nil && gen.HasNonZeroCursor() {
		return emitComplexExpression(vm, col.Expr, i, gen)
	}

	ident, ok := col.Expr.(*parser.IdentExpr)
	if ok {
		if gen != nil {
			colIdx := table.GetColumnIndex(ident.Name)
			if colIdx >= 0 {
				gen.SetCollationForReg(i, table.Columns[colIdx].Collation)
			}
		}
		// Use cursor from table metadata: temp/CTE tables store cursor in RootPage
		cursorNum := 0
		if table.Temp {
			cursorNum = int(table.RootPage)
		}
		return emitSimpleColumnRef(vm, table, ident, i, cursorNum)
	}

	fnExpr, isFn := col.Expr.(*parser.FunctionExpr)
	if isFn {
		return emitAggregateFunction(vm, fnExpr, i, gen)
	}

	return emitComplexExpression(vm, col.Expr, i, gen)
}

// emitSimpleColumnRef emits opcodes for simple column reference using the given cursor.
func emitSimpleColumnRef(vm *vdbe.VDBE, table *schema.Table, ident *parser.IdentExpr, targetReg int, cursorNum int) error {
	colIdx := table.GetColumnIndexWithRowidAliases(ident.Name)
	if colIdx == -1 {
		return fmt.Errorf("column not found: %s", ident.Name)
	}

	// colIdx == -2 means it's a rowid alias (rowid, _rowid_, oid)
	if colIdx == -2 {
		if table.WithoutRowID {
			// WITHOUT ROWID tables don't have rowid - return error
			return fmt.Errorf("no such column: %s", ident.Name)
		}
		// Regular tables: emit OpRowid
		vm.AddOp(vdbe.OpRowid, cursorNum, targetReg, 0)
		return nil
	}

	if schemaColIsRowidForTable(table, table.Columns[colIdx]) {
		vm.AddOp(vdbe.OpRowid, cursorNum, targetReg, 0)
		return nil
	}

	vm.AddOp(vdbe.OpColumn, cursorNum, schemaRecordIdxForTable(table, colIdx), targetReg)
	return nil
}

// emitComplexExpression emits opcodes for complex expression
func emitComplexExpression(vm *vdbe.VDBE, expr parser.Expression, targetReg int, gen *expr.CodeGenerator) error {
	if gen == nil {
		vm.AddOp(vdbe.OpNull, 0, targetReg, 0)
		return nil
	}

	reg, err := gen.GenerateExpr(expr)
	if err != nil {
		return fmt.Errorf("failed to generate expression: %w", err)
	}

	if coll, ok := gen.CollationForReg(reg); ok {
		gen.SetCollationForReg(targetReg, coll)
	}

	if reg != targetReg {
		vm.AddOp(vdbe.OpCopy, reg, targetReg, 0)
	}
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
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT":
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
		isRowid := schemaColIsRowidForTable(table, col)
		colInfo := expr.ColumnInfo{
			Name:      col.Name,
			Index:     recordIdx,
			IsRowid:   isRowid,
			Collation: col.GetCollation(),
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

// validateDatabasePath validates a database file path using the connection's security configuration.
func (s *Stmt) validateDatabasePath(path string) (string, error) {
	if s.conn.securityConfig == nil {
		// No security config, return path as-is (should not happen in normal operation)
		return path, nil
	}
	// Import the security package to use ValidateDatabasePath
	return security.ValidateDatabasePath(path, s.conn.securityConfig)
}
