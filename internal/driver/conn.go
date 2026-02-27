package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/functions"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// Conn implements database/sql/driver.Conn for SQLite.
type Conn struct {
	driver   *Driver
	filename string
	pager    *pager.Pager
	btree    *btree.Btree
	schema   *schema.Schema
	funcReg  *functions.Registry
	stmts    map[*Stmt]struct{}
	mu       sync.Mutex
	closed   bool

	// Transaction state
	inTx bool
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

	// For now, only support single statements
	if len(stmts) > 1 {
		return nil, fmt.Errorf("multiple statements not supported")
	}

	stmt := &Stmt{
		conn:  c,
		query: query,
		ast:   stmts[0],
	}

	c.stmts[stmt] = struct{}{}

	return stmt, nil
}

// Close closes the connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	// Close all statements - finalize VDBEs directly since we already hold the lock
	// (calling stmt.Close() would cause a deadlock as it tries to acquire c.mu)
	for stmt := range c.stmts {
		stmt.closed = true
		if stmt.vdbe != nil {
			stmt.vdbe.Finalize()
			stmt.vdbe = nil
		}
	}
	c.stmts = nil

	// Rollback any active transaction
	if c.inTx {
		if err := c.pager.Rollback(); err != nil {
			return err
		}
	}

	// Close pager
	if err := c.pager.Close(); err != nil {
		return err
	}

	c.closed = true

	// Remove from driver's connection map
	c.driver.mu.Lock()
	delete(c.driver.conns, c.filename)
	c.driver.mu.Unlock()

	return nil
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
func (c *Conn) removeStmt(stmt *Stmt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.stmts, stmt)
}

// openDatabase initializes the database connection by:
// 1. Loading the schema from sqlite_master (page 1) if first connection
// 2. Registering built-in functions
// The schemaLoaded parameter indicates if schema was already loaded by another connection.
func (c *Conn) openDatabase(schemaLoaded bool) error {
	// Load schema from the database only if this is the first connection
	if !schemaLoaded {
		if err := c.schema.LoadFromMaster(c.btree); err != nil {
			// Schema loading may fail for new empty databases (no sqlite_master table yet),
			// which is expected and safe to ignore. The schema will be populated as tables
			// are created through DDL statements.
			// We explicitly ignore this error as it indicates a new database, not a failure.
		}
	}

	// Register built-in SQL functions
	c.funcReg = functions.DefaultRegistry()

	return nil
}
