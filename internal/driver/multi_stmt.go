// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

// MultiStmt implements database/sql/driver.Stmt for multiple SQL statements.
// It executes all statements in sequence when Exec is called.
type MultiStmt struct {
	conn   *Conn
	query  string
	stmts  []*Stmt
	closed bool
	mu     sync.Mutex
}

// Close closes all statements.
func (m *MultiStmt) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	// Close all sub-statements
	for _, stmt := range m.stmts {
		stmt.Close()
	}
	return nil
}

// NumInput returns -1 to indicate unknown number of parameters.
func (m *MultiStmt) NumInput() int {
	return -1
}

// Exec executes all statements in sequence.
func (m *MultiStmt) Exec(args []driver.Value) (driver.Result, error) {
	return m.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes all statements in sequence with context.
func (m *MultiStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := m.checkClosed(); err != nil {
		return nil, err
	}

	m.conn.mu.Lock()
	defer m.conn.mu.Unlock()

	if m.conn.closed {
		return nil, driver.ErrBadConn
	}

	lastResult, totalRowsAffected, err := m.executeAllStmts(ctx, args)
	if err != nil {
		return nil, err
	}

	if err := m.commitIfNeeded(); err != nil {
		return nil, err
	}

	return m.buildResult(lastResult, totalRowsAffected), nil
}

// checkClosed checks if the multi-statement is closed.
func (m *MultiStmt) checkClosed() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return driver.ErrBadConn
	}
	return nil
}

// executeAllStmts executes all statements and aggregates results.
func (m *MultiStmt) executeAllStmts(ctx context.Context, args []driver.NamedValue) (driver.Result, int64, error) {
	var lastResult driver.Result
	var totalRowsAffected int64

	for _, stmt := range m.stmts {
		result, err := m.execSingleStmt(ctx, stmt, args)
		if err != nil {
			return nil, 0, err
		}
		lastResult = result
		if result != nil {
			if rows, err := result.RowsAffected(); err == nil {
				totalRowsAffected += rows
			}
		}
	}

	return lastResult, totalRowsAffected, nil
}

// commitIfNeeded commits the transaction if in autocommit mode.
func (m *MultiStmt) commitIfNeeded() error {
	if !m.conn.inTx && m.conn.pager.InWriteTransaction() {
		if err := m.conn.pager.Commit(); err != nil {
			return errors.New("auto-commit error: " + err.Error())
		}
	}
	return nil
}

// buildResult constructs the final result from the execution.
func (m *MultiStmt) buildResult(lastResult driver.Result, totalRowsAffected int64) driver.Result {
	if lastResult != nil {
		lastInsertId, _ := lastResult.LastInsertId()
		return &Result{
			lastInsertID: lastInsertId,
			rowsAffected: totalRowsAffected,
		}
	}
	return &Result{rowsAffected: totalRowsAffected}
}

// execSingleStmt executes a single statement without locking (caller must hold lock).
func (m *MultiStmt) execSingleStmt(ctx context.Context, stmt *Stmt, args []driver.NamedValue) (driver.Result, error) {
	// Compile the statement to VDBE bytecode
	vm, err := stmt.compile(args)
	if err != nil {
		return nil, err
	}
	defer vm.Finalize()

	// Execute the statement
	if err := vm.Run(); err != nil {
		// Rollback on error if in autocommit mode
		if !m.conn.inTx {
			m.conn.pager.Rollback()
		}
		return nil, err
	}

	// Don't auto-commit here - we'll commit after all statements

	// Return result
	return &Result{
		lastInsertID: vm.LastInsertID,
		rowsAffected: vm.NumChanges,
	}, nil
}

// Query is not supported for multi-statements (only the last would return rows).
func (m *MultiStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

// QueryContext is not supported for multi-statements.
func (m *MultiStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, driver.ErrSkip
}
