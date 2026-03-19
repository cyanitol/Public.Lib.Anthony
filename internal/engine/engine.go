// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package engine implements the top-level integration for the SQLite database engine.
// It ties together the pager, btree, schema, parser, VDBE, and function components.
package engine

import (
	"fmt"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Engine is the main SQLite database engine that coordinates all components.
type Engine struct {
	// Core components
	pager   *pager.Pager
	btree   *btree.Btree
	schema  *schema.Schema
	funcReg *functions.Registry

	// Database metadata
	filename string
	readOnly bool

	// Transaction state
	inTransaction bool
	mu            sync.RWMutex // Protects transaction state

	// Compiler for SQL to VDBE
	compiler *Compiler
}

// Open opens or creates a SQLite database at the specified path.
// If the database doesn't exist and readOnly is false, it will be created.
func Open(filename string) (*Engine, error) {
	return OpenWithOptions(filename, false)
}

// OpenWithOptions opens a database with specific options.
func OpenWithOptions(filename string, readOnly bool) (*Engine, error) {
	// Step 1: Open/create the database file with the pager
	pg, err := pager.Open(filename, readOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to open pager: %w", err)
	}

	// Step 2: Initialize the B-tree
	bt := btree.NewBtree(uint32(pg.PageSize()))

	// Step 3: Load or create the schema
	// For a new database, schema starts empty
	// For existing databases, we would load from sqlite_master table
	sch := schema.NewSchema()

	// Step 4: Register built-in functions
	funcReg := functions.DefaultRegistry()

	// Create the engine
	engine := &Engine{
		pager:    pg,
		btree:    bt,
		schema:   sch,
		funcReg:  funcReg,
		filename: filename,
		readOnly: readOnly,
	}

	// Initialize compiler
	engine.compiler = NewCompiler(engine)

	// Load existing schema if database is not empty
	if pg.PageCount() > 1 {
		if err := engine.loadSchema(); err != nil {
			pg.Close()
			return nil, fmt.Errorf("failed to load schema: %w", err)
		}
	}

	return engine, nil
}

// Close closes the database and releases all resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Rollback any active transaction
	if e.inTransaction {
		if err := e.pager.Rollback(); err != nil {
			return fmt.Errorf("failed to rollback transaction on close: %w", err)
		}
		e.inTransaction = false
	}

	// Close the pager (which closes the database file)
	if err := e.pager.Close(); err != nil {
		return fmt.Errorf("failed to close pager: %w", err)
	}

	return nil
}

// Execute executes a SQL statement and returns the result.
// This is the main entry point for SQL execution.
func (e *Engine) Execute(sql string) (*Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Step 1: Parse the SQL
	statements, err := parser.ParseString(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(statements) == 0 {
		return &Result{}, nil
	}

	// For simplicity, execute only the first statement
	// Real SQLite can execute multiple statements
	stmt := statements[0]

	// Step 2: Compile the statement to VDBE bytecode
	vm, err := e.compiler.Compile(stmt)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	// Step 3: Set up VDBE context
	vm.Ctx = &vdbe.VDBEContext{
		Btree:  e.btree,
		Schema: e.schema,
	}

	// Step 4: Execute the VDBE
	result, err := e.executeVDBE(vm)
	if err != nil {
		vm.Finalize()
		return nil, fmt.Errorf("execution error: %w", err)
	}

	// Step 5: Finalize the VDBE
	if err := vm.Finalize(); err != nil {
		return nil, fmt.Errorf("finalize error: %w", err)
	}

	return result, nil
}

// Query executes a query and returns rows.
// This is a convenience method for SELECT statements.
func (e *Engine) Query(sql string) (*Rows, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Parse the SQL
	statements, err := parser.ParseString(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statement to execute")
	}

	stmt := statements[0]

	// Verify it's a SELECT statement
	if _, ok := stmt.(*parser.SelectStmt); !ok {
		return nil, fmt.Errorf("not a SELECT statement")
	}

	// Compile to VDBE
	vm, err := e.compiler.Compile(stmt)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	// Set up context
	vm.Ctx = &vdbe.VDBEContext{
		Btree:  e.btree,
		Schema: e.schema,
	}

	// Create Rows object
	rows := &Rows{
		engine:  e,
		vdbe:    vm,
		columns: vm.ResultCols,
		done:    false,
	}

	return rows, nil
}

// Exec executes a statement and returns the number of affected rows.
// This is a convenience method for INSERT, UPDATE, DELETE statements.
func (e *Engine) Exec(sql string) (int64, error) {
	result, err := e.Execute(sql)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected, nil
}

// Begin starts a new transaction.
func (e *Engine) Begin() (*Tx, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.inTransaction {
		return nil, fmt.Errorf("transaction already in progress")
	}

	// Pager will start transaction on first write
	e.inTransaction = true

	return &Tx{
		engine: e,
		done:   false,
	}, nil
}

// Prepare prepares a SQL statement for later execution.
func (e *Engine) Prepare(sql string) (*PreparedStmt, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Parse the SQL
	statements, err := parser.ParseString(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statement to prepare")
	}

	stmt := statements[0]

	// Compile to VDBE
	vm, err := e.compiler.Compile(stmt)
	if err != nil {
		return nil, fmt.Errorf("compile error: %w", err)
	}

	return &PreparedStmt{
		engine: e,
		vdbe:   vm,
		sql:    sql,
	}, nil
}

// loadSchema loads the database schema from sqlite_master table.
// This is called when opening an existing database.
func (e *Engine) loadSchema() error {
	// In a real implementation, we would:
	// 1. Read the sqlite_master table (root page 1)
	// 2. Parse each CREATE TABLE/INDEX statement
	// 3. Populate the schema

	// For now, this is a placeholder
	// The schema will be populated as tables are created
	return nil
}

// executeVDBE executes a VDBE program and collects results.
func (e *Engine) executeVDBE(vm *vdbe.VDBE) (*Result, error) {
	result := &Result{
		Columns: vm.ResultCols,
		Rows:    make([][]interface{}, 0),
	}

	// Execute the VDBE
	for {
		hasRow, err := vm.Step()
		if err != nil {
			return nil, err
		}

		if !hasRow {
			break
		}

		// Collect result row
		if vm.ResultRow != nil {
			row := make([]interface{}, len(vm.ResultRow))
			for i, mem := range vm.ResultRow {
				row[i] = memToInterface(mem)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	// Set metadata
	result.RowsAffected = vm.NumChanges
	// LastInsertID would come from the last inserted rowid

	return result, nil
}

// memToInterface converts a VDBE memory cell to a Go interface{}.
func memToInterface(mem *vdbe.Mem) interface{} {
	if mem == nil {
		return nil
	}

	flags := mem.GetFlags()
	switch flags & vdbe.MemTypeMask {
	case vdbe.MemNull:
		return nil
	case vdbe.MemInt:
		return mem.IntValue()
	case vdbe.MemReal:
		return mem.RealValue()
	case vdbe.MemStr:
		return mem.StrValue()
	case vdbe.MemBlob:
		return mem.BlobValue()
	default:
		return nil
	}
}

// GetSchema returns the database schema.
func (e *Engine) GetSchema() *schema.Schema {
	return e.schema
}

// GetPager returns the pager (for testing/internal use).
func (e *Engine) GetPager() *pager.Pager {
	return e.pager
}

// GetBtree returns the B-tree (for testing/internal use).
func (e *Engine) GetBtree() *btree.Btree {
	return e.btree
}

// IsReadOnly returns true if the database is read-only.
func (e *Engine) IsReadOnly() bool {
	return e.readOnly
}
