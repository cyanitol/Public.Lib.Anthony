package driver

import (
	"context"
	"database/sql/driver"
	"os"
	"sync"
	"testing"
	"time"
)

// TestConcurrentExecContext tests concurrent ExecContext calls on the same statement.
// This verifies that the transaction state checks are properly protected by locks.
func TestConcurrentExecContext(t *testing.T) {
	dbFile := "test_concurrent_exec.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create a test table
	createStmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to prepare create: %v", err)
	}
	if _, err := createStmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	createStmt.Close()

	// Prepare an insert statement
	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Run concurrent ExecContext calls
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			args := []driver.NamedValue{
				{Ordinal: 1, Value: "test"},
			}
			_, err := s.ExecContext(context.Background(), args)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("ExecContext error: %v", err)
	}

	// Verify all rows were inserted
	queryStmt, err := c.PrepareContext(context.Background(), "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("failed to prepare query: %v", err)
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

	count := values[0].(int64)
	if count != numGoroutines {
		t.Errorf("expected %d rows, got %d", numGoroutines, count)
	}
}

// TestConcurrentQueryContext tests concurrent QueryContext calls on the same statement.
// This verifies that the connection state checks are properly protected by locks.
func TestConcurrentQueryContext(t *testing.T) {
	dbFile := "test_concurrent_query.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create and populate a test table
	createStmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to prepare create: %v", err)
	}
	if _, err := createStmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	createStmt.Close()

	// Insert test data
	insertStmt, err := c.PrepareContext(context.Background(), "INSERT INTO test (value) VALUES ('test')")
	if err != nil {
		t.Fatalf("failed to prepare insert: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := insertStmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	insertStmt.Close()

	// Prepare a query statement
	stmt, err := c.PrepareContext(context.Background(), "SELECT * FROM test")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Run concurrent QueryContext calls
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

			// Read all rows
			values := make([]driver.Value, 2)
			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("QueryContext error: %v", err)
	}
}

// TestConcurrentCloseWhileExecuting tests closing a statement while it's being executed.
// This verifies that the Close method properly handles the lock ordering to avoid deadlocks.
func TestConcurrentCloseWhileExecuting(t *testing.T) {
	dbFile := "test_concurrent_close.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create a test table
	createStmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to prepare create: %v", err)
	}
	if _, err := createStmt.(*Stmt).ExecContext(context.Background(), nil); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	createStmt.Close()

	// Prepare an insert statement
	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Start multiple goroutines executing the statement
	const numGoroutines = 5
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				args := []driver.NamedValue{
					{Ordinal: 1, Value: "test"},
				}
				s.ExecContext(context.Background(), args)
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Wait a bit then close the statement
	time.Sleep(5 * time.Millisecond)
	closeErr := s.Close()

	wg.Wait()

	if closeErr != nil {
		t.Errorf("Close() error = %v", closeErr)
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

// TestExecContextWithClosedConnection tests ExecContext with a closed connection.
// This verifies the TOCTOU fix where connection state is checked under lock.
func TestExecContextWithClosedConnection(t *testing.T) {
	dbFile := "test_exec_closed_conn.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_query_closed_conn.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_concurrent_tx_state.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

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

	// Prepare an insert statement
	stmt, err := c.PrepareContext(context.Background(), "INSERT INTO test DEFAULT VALUES")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Run concurrent operations: some executing statements, some starting/ending transactions
	var wg sync.WaitGroup
	wg.Add(20)

	// Goroutines executing statements
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				s.ExecContext(context.Background(), nil)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Goroutines starting and committing transactions
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				tx, err := c.BeginTx(context.Background(), driver.TxOptions{})
				if err != nil {
					continue
				}
				time.Sleep(2 * time.Millisecond)
				tx.Commit()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

// TestMultipleStmtClose tests that multiple simultaneous Close calls are safe.
func TestMultipleStmtClose(t *testing.T) {
	dbFile := "test_multi_close.db"
	defer os.Remove(dbFile)

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
