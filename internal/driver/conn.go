// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
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
	closed     bool

	// Transaction state
	inTx bool

	// PRAGMA settings
	foreignKeysEnabled bool
	journalMode        string

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
		multiStmt := &MultiStmt{
			conn:  c,
			query: query,
			stmts: make([]*Stmt, len(stmts)),
		}
		for i, ast := range stmts {
			// Use unique query string per sub-statement to avoid cache collisions
			subQuery := fmt.Sprintf("%s#%d", query, i)
			multiStmt.stmts[i] = &Stmt{
				conn:  c,
				query: subQuery,
				ast:   ast,
			}
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
// Phase 3: Remove from driver (only driver lock needed)
// Phase 4: Close pager
func (c *Conn) Close() error {
	// Phase 1: Mark closed and collect cleanup items under conn lock
	stmts, inTx, pager := c.markClosedAndCollect()
	if stmts == nil {
		return nil // Already closed
	}

	// Phase 2: Close statements without holding conn lock
	c.closeStatements(stmts)

	// Phase 3: Remove from driver (only driver lock needed)
	c.removeFromDriver()

	// Phase 4: Close pager (no locks needed)
	return c.closePager(pager, inTx)
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

// removeFromDriver removes the connection from the driver's connection map.
// This respects the lock hierarchy: Driver.mu should be acquired before Conn.mu.
func (c *Conn) removeFromDriver() {
	if c.driver != nil {
		c.driver.mu.Lock()
		delete(c.driver.conns, c.filename)
		c.driver.mu.Unlock()
	}
}

// closePager closes the pager, rolling back any active transaction first.
func (c *Conn) closePager(pager pager.PagerInterface, inTx bool) error {
	if pager == nil {
		return nil
	}

	// Rollback any active transaction
	if inTx {
		if err := pager.Rollback(); err != nil {
			return err
		}
	}

	// Close pager
	return pager.Close()
}

// Begin starts a transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with options.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
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

	return &Tx{
		conn:     c,
		readOnly: false,
	}, nil
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

	// Ensure no transaction is active
	if c.inTx {
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

// openDatabase initializes the database connection by:
// 1. Loading the schema from sqlite_master (page 1) if first connection
// 2. Registering built-in functions
// The schemaLoaded parameter indicates if schema was already loaded by another connection.
func (c *Conn) openDatabase(schemaLoaded bool) error {
	// Load schema from the database only if this is the first connection
	if !schemaLoaded {
		// First, ensure sqlite_master table exists in the schema
		// This is required for both new and existing databases
		if err := c.schema.InitializeMaster(); err != nil {
			return fmt.Errorf("failed to initialize sqlite_master: %w", err)
		}

		if err := c.schema.LoadFromMaster(c.btree); err != nil {
			// Schema loading may fail for new empty databases (no sqlite_master table yet),
			// which is expected and safe to ignore. The schema will be populated as tables
			// are created through DDL statements.
			// We explicitly ignore this error as it indicates a new database, not a failure.
		}
	}

	// Register the main database in the registry
	if err := c.dbRegistry.AttachDatabase("main", c.filename, c.pager, c.btree); err != nil {
		return fmt.Errorf("failed to register main database: %w", err)
	}

	// Register built-in SQL functions
	c.funcReg = functions.DefaultRegistry()

	// Initialize virtual table and collation registries
	c.vtabRegistry = vtab.NewModuleRegistry()
	c.collRegistry = collation.NewCollationRegistry()

	// Initialize foreign key constraint manager
	c.fkManager = constraint.NewForeignKeyManager()

	return nil
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
