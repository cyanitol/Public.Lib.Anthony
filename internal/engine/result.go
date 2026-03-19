// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"fmt"
	"io"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Result represents the result of executing a SQL statement.
type Result struct {
	// Columns contains the names of result columns (for SELECT)
	Columns []string

	// Rows contains all result rows (for SELECT)
	Rows [][]interface{}

	// RowsAffected is the number of rows affected (for INSERT/UPDATE/DELETE)
	RowsAffected int64

	// LastInsertID is the last inserted rowid (for INSERT)
	LastInsertID int64
}

// RowCount returns the number of rows in the result.
func (r *Result) RowCount() int {
	return len(r.Rows)
}

// ColumnCount returns the number of columns in the result.
func (r *Result) ColumnCount() int {
	return len(r.Columns)
}

// Rows represents an iterator over query results.
// This is similar to database/sql.Rows.
type Rows struct {
	engine  *Engine
	vdbe    *vdbe.VDBE
	columns []string
	done    bool

	// Current row data
	currentRow []*vdbe.Mem
	err        error
}

// Next advances to the next result row.
// Returns true if there is a row, false if no more rows or an error occurred.
func (r *Rows) Next() bool {
	if r.done {
		return false
	}

	// Execute one step of the VDBE
	hasRow, err := r.vdbe.Step()
	if err != nil {
		r.err = err
		r.done = true
		return false
	}

	if !hasRow {
		r.done = true
		return false
	}

	// Store the current row
	r.currentRow = r.vdbe.ResultRow
	return true
}

// Scan copies the columns from the current row into the values pointed at by dest.
// The number of values in dest must match the number of columns.
func (r *Rows) Scan(dest ...interface{}) error {
	if r.currentRow == nil {
		return fmt.Errorf("no current row")
	}

	if len(dest) != len(r.currentRow) {
		return fmt.Errorf("expected %d destinations, got %d", len(r.currentRow), len(dest))
	}

	for i, mem := range r.currentRow {
		if err := scanInto(mem, dest[i]); err != nil {
			return fmt.Errorf("error scanning column %d: %w", i, err)
		}
	}

	return nil
}

// Close closes the Rows, preventing further enumeration.
func (r *Rows) Close() error {
	if r.done {
		return nil
	}

	r.done = true

	// Finalize the VDBE
	if r.vdbe != nil {
		return r.vdbe.Finalize()
	}

	return nil
}

// Columns returns the column names.
func (r *Rows) Columns() []string {
	return r.columns
}

// Err returns the error, if any, that was encountered during iteration.
func (r *Rows) Err() error {
	return r.err
}

func scanTyped(mem *vdbe.Mem, dest interface{}) (bool, error) {
	switch d := dest.(type) {
	case *int:
		*d = int(mem.IntValue())
	case *int64:
		*d = mem.IntValue()
	case *float64:
		*d = mem.RealValue()
	case *string:
		*d = mem.StrValue()
	case *[]byte:
		*d = mem.BlobValue()
	case *bool:
		*d = mem.IntValue() != 0
	default:
		return false, nil
	}
	return true, nil
}

func scanInto(mem *vdbe.Mem, dest interface{}) error {
	if mem == nil {
		return fmt.Errorf("nil memory cell")
	}
	if d, ok := dest.(*interface{}); ok {
		*d = memToInterface(mem)
		return nil
	}
	if handled, err := scanTyped(mem, dest); err != nil || handled {
		return err
	}
	return fmt.Errorf("unsupported scan destination type: %T", dest)
}

// Tx represents a database transaction.
type Tx struct {
	engine *Engine
	done   bool
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if tx.done {
		return fmt.Errorf("transaction already finished")
	}

	tx.engine.mu.Lock()
	defer tx.engine.mu.Unlock()

	if !tx.engine.inTransaction {
		return fmt.Errorf("no transaction in progress")
	}

	// Commit via pager
	if err := tx.engine.pager.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	tx.engine.inTransaction = false
	tx.done = true
	return nil
}

// Rollback rolls back the transaction.
func (tx *Tx) Rollback() error {
	if tx.done {
		return fmt.Errorf("transaction already finished")
	}

	tx.engine.mu.Lock()
	defer tx.engine.mu.Unlock()

	if !tx.engine.inTransaction {
		return fmt.Errorf("no transaction in progress")
	}

	// Rollback via pager
	if err := tx.engine.pager.Rollback(); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	tx.engine.inTransaction = false
	tx.done = true
	return nil
}

// Execute executes a SQL statement within the transaction.
func (tx *Tx) Execute(sql string) (*Result, error) {
	if tx.done {
		return nil, fmt.Errorf("transaction already finished")
	}

	return tx.engine.Execute(sql)
}

// Query executes a query within the transaction.
func (tx *Tx) Query(sql string) (*Rows, error) {
	if tx.done {
		return nil, fmt.Errorf("transaction already finished")
	}

	return tx.engine.Query(sql)
}

// Exec executes a statement within the transaction.
func (tx *Tx) Exec(sql string) (int64, error) {
	if tx.done {
		return 0, fmt.Errorf("transaction already finished")
	}

	return tx.engine.Exec(sql)
}

// PreparedStmt represents a prepared SQL statement.
type PreparedStmt struct {
	engine *Engine
	vdbe   *vdbe.VDBE
	sql    string
	closed bool
}

// Execute executes the prepared statement with the given parameters.
func (ps *PreparedStmt) Execute(params ...interface{}) (*Result, error) {
	if ps.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Reset the VDBE for re-execution
	if err := ps.vdbe.Reset(); err != nil {
		return nil, fmt.Errorf("failed to reset statement: %w", err)
	}

	// Bind parameters
	if err := ps.bindParams(params); err != nil {
		return nil, err
	}

	// Set up context
	ps.vdbe.Ctx = &vdbe.VDBEContext{
		Btree:  ps.engine.btree,
		Schema: ps.engine.schema,
	}

	// Execute
	result, err := ps.engine.executeVDBE(ps.vdbe)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Query executes the prepared statement and returns rows.
func (ps *PreparedStmt) Query(params ...interface{}) (*Rows, error) {
	if ps.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Reset the VDBE
	if err := ps.vdbe.Reset(); err != nil {
		return nil, fmt.Errorf("failed to reset statement: %w", err)
	}

	// Bind parameters
	if err := ps.bindParams(params); err != nil {
		return nil, err
	}

	// Set up context
	ps.vdbe.Ctx = &vdbe.VDBEContext{
		Btree:  ps.engine.btree,
		Schema: ps.engine.schema,
	}

	// Create Rows object
	rows := &Rows{
		engine:  ps.engine,
		vdbe:    ps.vdbe,
		columns: ps.vdbe.ResultCols,
		done:    false,
	}

	return rows, nil
}

// Close closes the prepared statement and releases resources.
func (ps *PreparedStmt) Close() error {
	if ps.closed {
		return nil
	}

	ps.closed = true
	if ps.vdbe != nil {
		return ps.vdbe.Finalize()
	}
	return nil
}

// bindParams binds parameters to the prepared statement.
func (ps *PreparedStmt) bindParams(params []interface{}) error {
	// Parameter binding would work with variables in the VDBE
	// For now, this is a placeholder
	// Real implementation would:
	// 1. Find variable slots in VDBE
	// 2. Set their values from params array
	return nil
}

// SQL returns the original SQL text.
func (ps *PreparedStmt) SQL() string {
	return ps.sql
}

// QueryRow is a convenience method that executes a query expected to return at most one row.
type QueryRow struct {
	rows *Rows
	err  error
}

// QueryRow executes a query that is expected to return at most one row.
func (e *Engine) QueryRow(sql string) *QueryRow {
	rows, err := e.Query(sql)
	if err != nil {
		return &QueryRow{err: err}
	}
	return &QueryRow{rows: rows}
}

// Scan scans the result into dest.
func (qr *QueryRow) Scan(dest ...interface{}) error {
	if qr.err != nil {
		return qr.err
	}

	defer qr.rows.Close()

	if !qr.rows.Next() {
		if err := qr.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	return qr.rows.Scan(dest...)
}
