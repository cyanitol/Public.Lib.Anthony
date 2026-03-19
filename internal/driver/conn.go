// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab/fts5"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab/rtree"
)

// Conn implements database/sql/driver.Conn for SQLite.
type Conn struct {
	driver     *Driver
	filename   string
	pager      pager.PagerInterface
	btree      *btree.Btree
	schema     *schema.Schema
	funcReg    *functions.Registry
	dbRegistry *schema.DatabaseRegistry
	stmts      map[*Stmt]struct{}
	stmtCache  *StmtCache
	mu         sync.Mutex
	writeMu    *sync.Mutex // Shared across all connections to the same database
	closed     bool

	// Transaction state
	inTx          bool
	sqlTx         bool // true when transaction was started via SQL (BEGIN/SAVEPOINT)
	savepointOnly bool // true when transaction was started implicitly by SAVEPOINT

	// PRAGMA settings
	foreignKeysEnabled bool
	journalMode        string
	cacheSize          int64 // PRAGMA cache_size: positive=pages, negative=KiB

	// Security configuration
	securityConfig *security.SecurityConfig

	// Virtual table and collation registries
	vtabRegistry *vtab.ModuleRegistry
	collRegistry *collation.CollationRegistry

	// Foreign key constraint manager
	fkManager *constraint.ForeignKeyManager
}

// Prepare prepares a SQL statement.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext prepares a SQL statement with context.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Parse the SQL
	p := parser.NewParser(query)
	stmts, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	// Support multiple statements by creating a MultiStmt
	if len(stmts) > 1 {
		parts := splitStatements(query)
		multiStmt := &MultiStmt{
			conn:  c,
			query: query,
			stmts: make([]*Stmt, len(stmts)),
		}
		for i, ast := range stmts {
			subQuery := fmt.Sprintf("%s#%d", query, i)
			if len(parts) == len(stmts) && i < len(parts) {
				subQuery = parts[i]
			}
			child := &Stmt{
				conn:  c,
				query: subQuery,
				ast:   ast,
			}
			c.stmts[child] = struct{}{}
			multiStmt.stmts[i] = child
		}
		return multiStmt, nil
	}

	stmt := &Stmt{
		conn:  c,
		query: query,
		ast:   stmts[0],
	}

	c.stmts[stmt] = struct{}{}

	return stmt, nil
}

// Close closes the connection using a two-phase close pattern to avoid lock ordering violations.
// Phase 1: Mark closed and collect cleanup items under conn lock
// Phase 2: Close statements without holding conn lock
// Phase 3: Rollback any active transaction
// Phase 4: Release shared state via driver (only closes pager when last ref is released)
func (c *Conn) Close() error {
	// Phase 1: Mark closed and collect cleanup items under conn lock
	stmts, inTx, pgr := c.markClosedAndCollect()
	if stmts == nil {
		return nil // Already closed
	}

	// Phase 2: Close statements without holding conn lock
	c.closeStatements(stmts)

	// Phase 3: Rollback any active transaction and close attached databases
	if err := c.cleanupPager(pgr, inTx); err != nil {
		return err
	}

	// Phase 4: Release shared state via driver (reference-counted pager close)
	c.releaseFromDriver()

	return nil
}

// markClosedAndCollect marks the connection as closed and collects cleanup items.
// Returns nil stmts if already closed.
func (c *Conn) markClosedAndCollect() ([]*Stmt, bool, pager.PagerInterface) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, false, nil // Already closed
	}
	c.closed = true

	// Collect statements to close (make a slice copy)
	stmts := make([]*Stmt, 0, len(c.stmts))
	for stmt := range c.stmts {
		stmts = append(stmts, stmt)
	}
	c.stmts = nil

	return stmts, c.inTx, c.pager
}

// closeStatements closes all statements without holding conn lock.
// This avoids deadlock since stmt.Close() calls removeStmt() which needs conn.mu.
func (c *Conn) closeStatements(stmts []*Stmt) {
	for _, stmt := range stmts {
		stmt.mu.Lock()
		stmt.closed = true
		if stmt.vdbe != nil {
			stmt.vdbe.Finalize()
			stmt.vdbe = nil
		}
		stmt.mu.Unlock()
	}
}

// releaseFromDriver removes the connection from the driver's connection map
// and decrements the shared database state reference count. The pager is only
// closed when the last reference is released.
func (c *Conn) releaseFromDriver() {
	if c.driver == nil {
		return
	}
	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()

	delete(c.driver.conns, c.filename)

	if state, ok := c.driver.dbs[c.filename]; ok {
		c.driver.releaseState(c.filename, state)
	}
}

// cleanupPager closes attached databases and rolls back any active transaction.
// It does NOT close the pager itself; that is handled by releaseFromDriver.
func (c *Conn) cleanupPager(pgr pager.PagerInterface, inTx bool) error {
	// Close all attached databases (excludes main and temp)
	if c.dbRegistry != nil {
		if err := c.dbRegistry.CloseAttached(); err != nil {
			return fmt.Errorf("failed to close attached databases: %w", err)
		}
	}

	if pgr == nil {
		return nil
	}

	// Acquire the shared write mutex to serialize with any in-flight
	// write operations on other connections sharing this database.
	if c.writeMu != nil {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
	}

	// Rollback any active transaction
	if inTx {
		if err := pgr.Rollback(); err != nil {
			return err
		}
	}

	return nil
}

// Begin starts a transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with options.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driver.ErrBadConn
	}

	if c.inTx {
		return nil, fmt.Errorf("transaction already in progress")
	}

	// Create transaction based on options
	if opts.ReadOnly {
		// Start a read transaction in the pager
		if err := c.pager.BeginRead(); err != nil {
			return nil, fmt.Errorf("failed to begin read transaction: %w", err)
		}

		c.inTx = true
		c.setFKTransactionState(true)

		return &Tx{
			conn:     c,
			readOnly: true,
		}, nil
	}

	// Start a write transaction in the pager
	if err := c.pager.BeginWrite(); err != nil {
		return nil, fmt.Errorf("failed to begin write transaction: %w", err)
	}

	c.inTx = true
	c.setFKTransactionState(true)

	return &Tx{
		conn:     c,
		readOnly: false,
	}, nil
}

// setFKTransactionState sets the transaction state in the foreign key manager.
func (c *Conn) setFKTransactionState(inTx bool) {
	if c.fkManager != nil {
		c.fkManager.SetInTransaction(inTx)
	}
}

// checkDeferredFKConstraints validates all deferred foreign key constraints.
func (c *Conn) checkDeferredFKConstraints() error {
	if !c.foreignKeysEnabled || c.fkManager == nil {
		return nil
	}

	// We need a minimal row reader to check the deferred violations
	// Create a simple wrapper that uses the btree directly
	rowReader := &ConnRowReader{conn: c}
	return c.fkManager.CheckDeferredViolations(c.schema, rowReader)
}

// clearDeferredFKViolations clears all deferred foreign key violations.
func (c *Conn) clearDeferredFKViolations() {
	if c.fkManager != nil {
		c.fkManager.ClearDeferredViolations()
	}
}

// Ping verifies the connection is still alive.
func (c *Conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	return nil
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before.
func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// SQL-managed transactions (via BEGIN/SAVEPOINT SQL statements) are allowed
	// to persist across database/sql pool reuse. Go-API transactions (via db.Begin())
	// must be completed before the connection can be reused.
	if c.inTx && !c.sqlTx {
		return fmt.Errorf("cannot reset session with active transaction")
	}

	return nil
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Allow standard Go types
	// SQLite is dynamically typed so we accept most values
	return driver.ErrSkip
}

// removeStmt removes a statement from the connection's statement map.
// This is safe to call even if the connection is closed, as Close() already
// removed all statements from the map.
func (c *Conn) removeStmt(stmt *Stmt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stmts != nil {
		delete(c.stmts, stmt)
	}
}

// hasAttachedDatabases reports whether any auxiliary databases (beyond "main")
// are currently registered on this connection.
func (c *Conn) hasAttachedDatabases() bool {
	if c.dbRegistry == nil {
		return false
	}
	dbs := c.dbRegistry.ListDatabases()
	return len(dbs) > 1
}

// clearTxState resets all transaction-related state flags.
func (c *Conn) clearTxState() {
	c.inTx = false
	c.sqlTx = false
	c.savepointOnly = false
}

// reloadSchemaAfterRollback reloads the in-memory schema from the btree
// after a ROLLBACK so that DDL changes (CREATE/DROP TABLE) are undone.
func (c *Conn) reloadSchemaAfterRollback() {
	if c.btree == nil || c.schema == nil {
		return
	}
	fresh := schema.NewSchema()
	if err := fresh.InitializeMaster(); err != nil {
		return
	}
	if err := fresh.LoadFromMaster(c.btree); err != nil {
		return // empty DB after rollback is fine
	}
	c.schema = fresh
	// Update the registry so callers see the restored schema
	if c.dbRegistry != nil {
		if mainDB, ok := c.dbRegistry.GetDatabase("main"); ok {
			mainDB.Schema = c.schema
		}
	}
}

// splitStatements splits a raw SQL string into individual statements using ';' delimiters.
// It trims whitespace and drops empty segments. This is a simplified splitter suitable
// for test inputs that do not contain semicolons inside literals.
func splitStatements(query string) []string {
	raw := strings.Split(query, ";")
	parts := make([]string, 0, len(raw))
	for _, part := range raw {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

// openDatabase initializes the database connection by:
// 1. Loading the schema from sqlite_master (page 1) if first connection
// 2. Registering built-in functions
// The schemaLoaded parameter indicates if schema was already loaded by another connection.
func (c *Conn) openDatabase(schemaLoaded bool) error {
	if !schemaLoaded {
		if err := c.loadInitialSchema(); err != nil {
			return err
		}
	}

	if err := c.registerMainDatabase(); err != nil {
		return err
	}

	c.initRegistries()

	if err := c.registerBuiltinVirtualTables(); err != nil {
		return fmt.Errorf("failed to register virtual tables: %w", err)
	}

	c.fkManager = constraint.NewForeignKeyManager()

	if c.cacheSize == 0 {
		c.cacheSize = -2000
	}

	return nil
}

// loadInitialSchema initializes and loads the schema for the first connection.
func (c *Conn) loadInitialSchema() error {
	if err := c.schema.InitializeMaster(); err != nil {
		return fmt.Errorf("failed to initialize sqlite_master: %w", err)
	}

	if c.btree != nil && c.pager != nil && c.pager.PageCount() <= 1 {
		if _, err := c.btree.CreateTable(); err != nil {
			return fmt.Errorf("failed to create sqlite_master storage: %w", err)
		}
	}

	// Schema loading may fail for new empty databases, which is expected and safe to ignore.
	_ = c.schema.LoadFromMaster(c.btree)
	return nil
}

// registerMainDatabase registers the main database in the registry and syncs schema.
func (c *Conn) registerMainDatabase() error {
	if err := c.dbRegistry.AttachDatabase("main", c.filename, c.pager, c.btree); err != nil {
		return fmt.Errorf("failed to register main database: %w", err)
	}

	if mainDB, ok := c.dbRegistry.GetDatabase("main"); ok {
		mainDB.Schema = c.schema
		mainDB.Pager = c.pager
		mainDB.Btree = c.btree
	}
	return nil
}

// initRegistries initializes function, virtual table, and collation registries.
func (c *Conn) initRegistries() {
	c.funcReg = functions.DefaultRegistry()
	c.vtabRegistry = vtab.NewModuleRegistry()
	c.collRegistry = collation.NewCollationRegistry()
}

// registerBuiltinVirtualTables registers built-in virtual table modules like FTS5 and RTree.
func (c *Conn) registerBuiltinVirtualTables() error {
	// Register FTS5 full-text search module
	if err := c.vtabRegistry.RegisterModule("fts5", fts5.NewFTS5Module()); err != nil {
		return fmt.Errorf("failed to register fts5 module: %w", err)
	}

	// Register RTree spatial index module
	if err := c.vtabRegistry.RegisterModule("rtree", rtree.NewRTreeModule()); err != nil {
		return fmt.Errorf("failed to register rtree module: %w", err)
	}

	// Future: Register additional modules like JSON, etc. when implemented

	return nil
}

// ensureMasterPage makes sure page 1 exists in the btree for sqlite_master.
func (c *Conn) ensureMasterPage() error {
	if c.btree == nil {
		return nil
	}
	if _, err := c.btree.GetPage(1); err == nil {
		return nil
	}

	page := make([]byte, c.btree.PageSize)
	headerOffset := btree.FileHeaderSize
	page[headerOffset+btree.PageHeaderOffsetType] = btree.PageTypeLeafTable
	// NumCells, CellContentStart, Fragmented already zeroed
	return c.btree.SetPage(1, page)
}

// applyConfig applies the DSN configuration settings to the connection.
// This is called after the connection is opened to apply settings like
// journal_mode, cache_size, foreign_keys, etc.
func (c *Conn) applyConfig(config *DriverConfig) error {
	if config == nil {
		return nil
	}

	// Store configuration settings that affect connection behavior
	c.foreignKeysEnabled = config.EnableForeignKeys
	if c.fkManager != nil {
		c.fkManager.SetEnabled(c.foreignKeysEnabled)
	}

	// Apply PRAGMA settings by executing them as SQL statements
	// We need to do this through the statement execution path to ensure
	// all settings are properly applied
	pragmas := config.ApplyPragmas()
	for _, pragma := range pragmas {
		// Execute the PRAGMA statement
		// We use the Exec method which will prepare, execute, and close the statement
		stmt, err := c.PrepareContext(context.Background(), pragma)
		if err != nil {
			return fmt.Errorf("failed to prepare pragma %q: %w", pragma, err)
		}

		// Execute the statement
		if execer, ok := stmt.(driver.StmtExecContext); ok {
			_, err = execer.ExecContext(context.Background(), nil)
		} else {
			return fmt.Errorf("statement does not support ExecContext")
		}

		// Close the statement
		stmt.Close()

		if err != nil {
			return fmt.Errorf("failed to execute pragma %q: %w", pragma, err)
		}
	}

	return nil
}

// CreateScalarFunction registers a user-defined scalar function.
// The function is registered in the connection's local function registry
// and can be used in SQL queries executed on this connection.
//
// Parameters:
//   - name: The function name to use in SQL
//   - numArgs: Number of arguments (-1 for variadic)
//   - deterministic: Whether the function always returns the same result for the same inputs
//   - fn: The UserFunction implementation
//
// Example:
//
//	type DoubleFunc struct{}
//	func (f *DoubleFunc) Invoke(args []functions.Value) (functions.Value, error) {
//	    if args[0].IsNull() {
//	        return functions.NewNullValue(), nil
//	    }
//	    return functions.NewIntValue(args[0].AsInt64() * 2), nil
//	}
//	conn.CreateScalarFunction("double", 1, true, &DoubleFunc{})
func (c *Conn) CreateScalarFunction(name string, numArgs int, deterministic bool, fn functions.UserFunction) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	config := functions.FunctionConfig{
		Name:          name,
		NumArgs:       numArgs,
		Deterministic: deterministic,
	}

	return functions.RegisterScalarFunction(c.funcReg, config, fn)
}

// CreateAggregateFunction registers a user-defined aggregate function.
// The function is registered in the connection's local function registry
// and can be used in GROUP BY queries and aggregations on this connection.
//
// Parameters:
//   - name: The function name to use in SQL
//   - numArgs: Number of arguments (-1 for variadic)
//   - deterministic: Whether the function always returns the same result for the same inputs
//   - fn: The UserAggregateFunction implementation
//
// Example:
//
//	type ProductFunc struct { product int64; hasValue bool }
//	func (f *ProductFunc) Step(args []functions.Value) error {
//	    if !args[0].IsNull() {
//	        if !f.hasValue { f.product = 1; f.hasValue = true }
//	        f.product *= args[0].AsInt64()
//	    }
//	    return nil
//	}
//	func (f *ProductFunc) Final() (functions.Value, error) {
//	    if !f.hasValue { return functions.NewNullValue(), nil }
//	    return functions.NewIntValue(f.product), nil
//	}
//	func (f *ProductFunc) Reset() { f.product = 1; f.hasValue = false }
//	conn.CreateAggregateFunction("product", 1, true, &ProductFunc{})
func (c *Conn) CreateAggregateFunction(name string, numArgs int, deterministic bool, fn functions.UserAggregateFunction) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	config := functions.FunctionConfig{
		Name:          name,
		NumArgs:       numArgs,
		Deterministic: deterministic,
	}

	return functions.RegisterAggregateFunction(c.funcReg, config, fn)
}

// UnregisterFunction removes a user-defined function from the connection.
// This allows removing functions that were previously registered.
//
// Parameters:
//   - name: The function name to remove
//   - numArgs: The number of arguments (-1 for variadic)
//
// Returns true if a function was removed, false if not found.
func (c *Conn) UnregisterFunction(name string, numArgs int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return false
	}

	return functions.UnregisterFunction(c.funcReg, name, numArgs)
}

// RegisterVirtualTableModule registers a virtual table module with the connection.
// This allows custom virtual table implementations to be used in SQL queries.
//
// A virtual table module provides callbacks for creating and accessing virtual tables
// that don't have persistent storage in the database file. Common use cases include:
// - In-memory tables computed on-the-fly
// - Views over external data sources (CSV files, REST APIs, etc.)
// - Full-text search indexes (FTS)
// - R-tree spatial indexes
//
// Parameters:
//   - name: The module name to use in CREATE VIRTUAL TABLE statements
//   - module: The virtual table module implementation
//
// Example:
//
//	// Create a module
//	type MyModule struct{ vtab.BaseModule }
//	func (m *MyModule) Create(db interface{}, moduleName, dbName, tableName string, args []string) (vtab.VirtualTable, string, error) {
//	    return &MyTable{}, "CREATE TABLE x(id INTEGER, name TEXT)", nil
//	}
//
//	// Register it
//	conn.RegisterVirtualTableModule("my_module", &MyModule{})
//
//	// Use in SQL
//	db.Exec("CREATE VIRTUAL TABLE my_table USING my_module(arg1, arg2)")
func (c *Conn) RegisterVirtualTableModule(name string, module vtab.Module) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	// Initialize the registry if not already done
	if c.vtabRegistry == nil {
		c.vtabRegistry = vtab.NewModuleRegistry()
	}

	return c.vtabRegistry.RegisterModule(name, module)
}

// UnregisterVirtualTableModule removes a virtual table module from the connection.
// This prevents the module from being used in new CREATE VIRTUAL TABLE statements.
// Existing virtual tables created with this module are not affected.
//
// Parameters:
//   - name: The module name to unregister
//
// Returns an error if the module is not registered.
func (c *Conn) UnregisterVirtualTableModule(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	if c.vtabRegistry == nil {
		return fmt.Errorf("virtual table module %q not registered", name)
	}

	return c.vtabRegistry.UnregisterModule(name)
}

// CreateCollation registers a custom collation sequence for string comparisons.
// Collations define how strings are compared and sorted, which affects:
// - ORDER BY clauses
// - Comparison operators (=, <, >, etc.)
// - DISTINCT operations
// - GROUP BY clauses
// - Indexes on TEXT columns
//
// SQLite provides three built-in collations:
// - BINARY: Byte-by-byte comparison (case-sensitive, default)
// - NOCASE: Case-insensitive for ASCII A-Z
// - RTRIM: Ignores trailing spaces
//
// Custom collations allow you to implement locale-specific sorting,
// natural sort order, or any custom comparison logic.
//
// Parameters:
//   - name: The collation name to use in SQL (e.g., "UTF8_UNICODE_CI")
//   - fn: A comparison function that returns -1, 0, or 1
//
// Example:
//
//	// Create a reverse collation (sorts in reverse order)
//	reverseCollation := func(a, b string) int {
//	    if a > b { return -1 }
//	    if a < b { return 1 }
//	    return 0
//	}
//	conn.CreateCollation("REVERSE", reverseCollation)
//
//	// Use in SQL
//	db.Query("SELECT name FROM users ORDER BY name COLLATE REVERSE")
//
//	// Or in table definition
//	db.Exec("CREATE TABLE users (name TEXT COLLATE REVERSE)")
func (c *Conn) CreateCollation(name string, fn collation.CollationFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	if name == "" {
		return fmt.Errorf("collation name cannot be empty")
	}

	if fn == nil {
		return fmt.Errorf("collation function cannot be nil")
	}

	// Initialize the registry if not already done
	if c.collRegistry == nil {
		c.collRegistry = collation.NewCollationRegistry()
	}

	return c.collRegistry.Register(name, fn)
}

// RemoveCollation removes a custom collation sequence from the connection.
// Built-in collations (BINARY, NOCASE, RTRIM) cannot be removed.
//
// Parameters:
//   - name: The collation name to remove
//
// Returns an error if the collation is built-in or not registered.
func (c *Conn) RemoveCollation(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return driver.ErrBadConn
	}

	if c.collRegistry == nil {
		return fmt.Errorf("collation %q not registered", name)
	}

	return c.collRegistry.Unregister(name)
}

// ConnRowReader provides a minimal RowReader implementation for deferred FK constraint checking.
// It creates a temporary VDBE context to access the row reading functionality.
type ConnRowReader struct {
	conn *Conn
}

// newRowReader creates a VDBERowReader backed by a minimal VDBE context.
// This avoids repeating the same VDBE construction in every ConnRowReader method.
func (r *ConnRowReader) newRowReader() *vdbe.VDBERowReader {
	v := &vdbe.VDBE{
		Ctx: &vdbe.VDBEContext{
			Schema:             r.conn.schema,
			Btree:              r.conn.btree,
			Pager:              r.conn.pager,
			ForeignKeysEnabled: r.conn.foreignKeysEnabled,
			FKManager:          r.conn.fkManager,
		},
		Cursors: make([]*vdbe.Cursor, 10),
	}
	return vdbe.NewVDBERowReader(v)
}

// RowExists checks if a row exists with the given column values.
func (r *ConnRowReader) RowExists(table string, columns []string, values []interface{}) (bool, error) {
	return r.newRowReader().RowExists(table, columns, values)
}

// RowExistsWithCollation checks if a row exists using specified collations.
func (r *ConnRowReader) RowExistsWithCollation(table string, columns []string, values []interface{}, collations []string) (bool, error) {
	return r.newRowReader().RowExistsWithCollation(table, columns, values, collations)
}

// FindReferencingRows finds all rows that reference the given values.
func (r *ConnRowReader) FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error) {
	return r.newRowReader().FindReferencingRows(table, columns, values)
}

// FindReferencingRowsWithParentAffinity finds all rows that reference the given values,
// using the parent column's affinity and collation for comparison.
func (r *ConnRowReader) FindReferencingRowsWithParentAffinity(
	childTableName string,
	childColumns []string,
	parentValues []interface{},
	parentTableName string,
	parentColumns []string,
) ([]int64, error) {
	return r.newRowReader().FindReferencingRowsWithParentAffinity(childTableName, childColumns, parentValues, parentTableName, parentColumns)
}

// ReadRowByRowid reads a row's values by its rowid.
func (r *ConnRowReader) ReadRowByRowid(table string, rowid int64) (map[string]interface{}, error) {
	return r.newRowReader().ReadRowByRowid(table, rowid)
}

// DatabaseExecutor implementation for FTS5/R-Tree shadow table operations.
// These methods allow virtual table modules to create and query their shadow tables.
// They use prepareInternal to avoid deadlocking on the connection mutex,
// since they are called during CREATE VIRTUAL TABLE which already holds the lock.

// prepareInternal prepares a SQL statement without acquiring the connection mutex.
// This must only be called when the caller already holds c.mu.
func (c *Conn) prepareInternal(sqlStr string) (*Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	p := parser.NewParser(sqlStr)
	stmts, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	stmt := &Stmt{
		conn:  c,
		query: sqlStr,
		ast:   stmts[0],
	}
	return stmt, nil
}

// ExecDDL executes a DDL statement (CREATE TABLE, DROP TABLE, etc.).
// Must be called with c.mu already held.
func (c *Conn) ExecDDL(sqlStr string) error {
	stmt, err := c.prepareInternal(sqlStr)
	if err != nil {
		return err
	}

	namedArgs := valuesToNamedValues(nil)
	_, err = stmt.executeAndCommit(namedArgs, c.inTx)
	return err
}

// ExecDML executes a DML statement (INSERT, UPDATE, DELETE) and returns rows affected.
// Must be called with c.mu already held.
func (c *Conn) ExecDML(sqlStr string, args ...interface{}) (int64, error) {
	stmt, err := c.prepareInternal(sqlStr)
	if err != nil {
		return 0, err
	}

	namedArgs := valuesToNamedValues(toDriverValues(args))
	result, err := stmt.executeAndCommit(namedArgs, c.inTx)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// Query executes a SELECT statement and returns results as rows.
// Must be called with c.mu already held.
func (c *Conn) Query(sqlStr string, args ...interface{}) ([][]interface{}, error) {
	stmt, err := c.prepareInternal(sqlStr)
	if err != nil {
		return nil, err
	}

	namedArgs := valuesToNamedValues(toDriverValues(args))
	rows, err := stmt.queryInternal(namedArgs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return c.collectQueryResults(rows)
}

// toDriverValues converts interface args to driver.Value slice.
func toDriverValues(args []interface{}) []driver.Value {
	driverArgs := make([]driver.Value, len(args))
	for i, arg := range args {
		driverArgs[i] = arg
	}
	return driverArgs
}

// collectQueryResults reads all rows from a result set into a slice.
func (c *Conn) collectQueryResults(rows driver.Rows) ([][]interface{}, error) {
	var results [][]interface{}
	dest := make([]driver.Value, len(rows.Columns()))

	for {
		if err := rows.Next(dest); err != nil {
			break
		}
		row := make([]interface{}, len(dest))
		for i, v := range dest {
			row[i] = v
		}
		results = append(results, row)
	}

	return results, nil
}
