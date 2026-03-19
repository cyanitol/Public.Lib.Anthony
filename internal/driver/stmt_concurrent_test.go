// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"
)

// concurrentOpenAndCreate opens a DB file, creates a test table, and returns the conn.
func concurrentOpenAndCreate(t *testing.T, dbFile, createSQL string) *Conn {
	t.Helper()
	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	c := conn.(*Conn)
	concurrentExecSQL(t, c, createSQL)
	return c
}

// concurrentExecSQL prepares and executes a SQL statement on the connection.
func concurrentExecSQL(t *testing.T, c *Conn, sql string) {
	t.Helper()
	stmt, err := c.PrepareContext(context.Background(), sql)
	if err != nil {
		t.Fatalf("failed to prepare %q: %v", sql, err)
	}
	if _, err := stmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to exec %q: %v", sql, err)
	}
	stmt.Close()
}

// concurrentCountRows queries COUNT(*) and returns the count.
func concurrentCountRows(t *testing.T, c *Conn, table string) int64 {
	t.Helper()
	queryStmt, err := c.PrepareContext(context.Background(), "SELECT COUNT(*) FROM "+table)
	if err != nil {
		t.Fatalf("failed to prepare count query: %v", err)
	}
	defer queryStmt.Close()

	rows, err := queryStmt.(*Stmt).QueryContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatalf("failed to get count: %v", err)
	}
	return values[0].(int64)
}

// TestConcurrentExecContext tests concurrent ExecContext calls on the same statement.
func TestConcurrentExecContext(t *testing.T) {
	dbFile := t.TempDir() + "/test_concurrent_exec.db"

	c := concurrentOpenAndCreate(t, dbFile, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	defer c.Close()

	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			args := []driver.NamedValue{{Ordinal: 1, Value: "test"}}
			if _, err := s.ExecContext(context.Background(), args); err != nil {
				errors <- err
			}
		}(i)
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("ExecContext error: %v", err)
	}

	if count := concurrentCountRows(t, c, "test"); count != numGoroutines {
		t.Errorf("expected %d rows, got %d", numGoroutines, count)
	}
}

// concurrentInsertN inserts N rows using a prepared statement.
func concurrentInsertN(t *testing.T, c *Conn, sql string, n int) {
	t.Helper()
	stmt, err := c.PrepareContext(context.Background(), sql)
	if err != nil {
		t.Fatalf("failed to prepare insert: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := stmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	stmt.Close()
}

// TestConcurrentQueryContext tests concurrent QueryContext calls on the same statement.
func TestConcurrentQueryContext(t *testing.T) {
	dbFile := t.TempDir() + "/test_concurrent_query.db"

	c := concurrentOpenAndCreate(t, dbFile, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	defer c.Close()

	concurrentInsertN(t, c, "INSERT INTO test (value) VALUES ('test')", 5)

	stmt, err := c.PrepareContext(context.Background(), "SELECT * FROM test")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			rows, err := s.QueryContext(context.Background(), nil)
			if err != nil {
				errors <- err
				return
			}
			defer rows.Close()
			values := make([]driver.Value, 2)
			for rows.Next(values) == nil {
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("QueryContext error: %v", err)
	}
}

// concurrentCloseVerify checks that a statement is properly closed and unregistered.
func concurrentCloseVerify(t *testing.T, c *Conn, s *Stmt) {
	t.Helper()
	if !s.closed {
		t.Error("statement should be closed")
	}
	c.mu.Lock()
	_, exists := c.stmts[s]
	c.mu.Unlock()
	if exists {
		t.Error("statement should be removed from connection's map")
	}
}

// TestConcurrentCloseWhileExecuting tests closing a statement while it's being executed.
func TestConcurrentCloseWhileExecuting(t *testing.T) {
	dbFile := t.TempDir() + "/test_concurrent_close.db"

	c := concurrentOpenAndCreate(t, dbFile, "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	defer c.Close()

	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	s := stmt.(*Stmt)

	const numGoroutines = 5
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				args := []driver.NamedValue{{Ordinal: 1, Value: "test"}}
				s.ExecContext(context.Background(), args)
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	time.Sleep(5 * time.Millisecond)
	if closeErr := s.Close(); closeErr != nil {
		t.Errorf("Close() error = %v", closeErr)
	}
	wg.Wait()
	concurrentCloseVerify(t, c, s)
}

// TestExecContextWithClosedConnection tests ExecContext with a closed connection.
// This verifies the TOCTOU fix where connection state is checked under lock.
func TestExecContextWithClosedConnection(t *testing.T) {
	dbFile := t.TempDir() + "/test_exec_closed_conn.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Create a test table
	createStmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("failed to prepare create: %v", err)
	}
	if _, err := createStmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	createStmt.Close()

	// Prepare a statement
	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test DEFAULT VALUES")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close the connection
	conn.Close()

	// Try to execute on closed connection
	_, err = s.ExecContext(context.Background(), nil)
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got %v", err)
	}
}

// TestQueryContextWithClosedConnection tests QueryContext with a closed connection.
// This verifies the TOCTOU fix where connection state is checked under lock.
func TestQueryContextWithClosedConnection(t *testing.T) {
	dbFile := t.TempDir() + "/test_query_closed_conn.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Prepare a statement
	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close the connection
	conn.Close()

	// Try to query on closed connection
	_, err = s.QueryContext(context.Background(), nil)
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got %v", err)
	}
}

// TestConcurrentTransactionStateCheck tests that transaction state checks
// don't race with transaction begin/commit/rollback operations.
func TestConcurrentTransactionStateCheck(t *testing.T) {
	dbFile := t.TempDir() + "/test_concurrent_tx_state.db"

	c := concurrentOpenAndCreate(t, dbFile, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	defer c.Close()

	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test DEFAULT VALUES")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)
	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				s.ExecContext(context.Background(), nil)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			concurrentTxCycle(c)
		}()
	}

	wg.Wait()
}

// concurrentTxCycle runs begin/commit cycles.
func concurrentTxCycle(c *Conn) {
	for j := 0; j < 3; j++ {
		tx, err := c.BeginTx(context.Background(), driver.TxOptions{})
		if err != nil {
			continue
		}
		time.Sleep(2 * time.Millisecond)
		tx.Commit()
		time.Sleep(time.Millisecond)
	}
}

// TestMultipleStmtClose tests that multiple simultaneous Close calls are safe.
func TestMultipleStmtClose(t *testing.T) {
	dbFile := t.TempDir() + "/test_multi_close.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Prepare a statement
	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close from multiple goroutines simultaneously
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			err := s.Close()
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Close() error = %v", err)
	}

	// Verify statement is closed
	if !s.closed {
		t.Error("statement should be closed")
	}

	// Verify removed from connection
	c.mu.Lock()
	_, exists := c.stmts[s]
	c.mu.Unlock()

	if exists {
		t.Error("statement should be removed from connection's map")
	}
}
