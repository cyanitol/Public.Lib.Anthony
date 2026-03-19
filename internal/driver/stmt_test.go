// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestStmtClose(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_close.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close statement
	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Should be closed
	if !s.closed {
		t.Error("statement should be closed")
	}

	// Should be removed from connection's statement map
	c.mu.Lock()
	_, exists := c.stmts[s]
	c.mu.Unlock()

	if exists {
		t.Error("statement should be removed from connection's map")
	}

	// Second close should be safe
	if err := s.Close(); err != nil {
		t.Errorf("second Close() should be safe: %v", err)
	}
}

func TestStmtNumInput(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_numinput.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// NumInput should return -1 (unknown)
	if n := s.NumInput(); n != -1 {
		t.Errorf("NumInput() = %d, want -1", n)
	}
}

func TestStmtExec(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_exec.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Create a table
	stmt, err := c.PrepareContext(context.Background(), "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to prepare CREATE TABLE: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Execute statement
	args := []driver.Value{}
	result, err := s.Exec(args)
	if err != nil {
		t.Errorf("Exec() error = %v", err)
	}

	if result == nil {
		t.Error("Exec() result should not be nil")
	}
}

func TestStmtExecContextOnClosed(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_exec_closed.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close statement
	s.Close()

	// Try to execute closed statement
	_, err = s.ExecContext(context.Background(), nil)
	if err != driver.ErrBadConn {
		t.Errorf("ExecContext() on closed statement error = %v, want ErrBadConn", err)
	}
}

func TestStmtQuery(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_query.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	s := stmt.(*Stmt)

	// Execute query
	args := []driver.Value{}
	rows, err := s.Query(args)
	if err != nil {
		t.Errorf("Query() error = %v", err)
	}

	if rows == nil {
		t.Error("Query() rows should not be nil")
	} else {
		rows.Close()
	}
}

func TestStmtQueryContextOnClosed(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_query_closed.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close statement
	s.Close()

	// Try to query closed statement
	_, err = s.QueryContext(context.Background(), nil)
	if err != driver.ErrBadConn {
		t.Errorf("QueryContext() on closed statement error = %v, want ErrBadConn", err)
	}
}

// Note: Testing unsupported statement types requires package-private access
// to parser.Statement interface methods, so that's covered by integration tests

func TestValueToNamedValues(t *testing.T) {
	values := []driver.Value{
		int64(42),
		"hello",
		float64(3.14),
	}

	named := valuesToNamedValues(values)

	if len(named) != len(values) {
		t.Errorf("valuesToNamedValues() length = %d, want %d", len(named), len(values))
	}

	for i, nv := range named {
		if nv.Ordinal != i+1 {
			t.Errorf("NamedValue[%d].Ordinal = %d, want %d", i, nv.Ordinal, i+1)
		}
		if nv.Value != values[i] {
			t.Errorf("NamedValue[%d].Value = %v, want %v", i, nv.Value, values[i])
		}
	}
}

func TestStmtNewVDBE(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_new_vdbe.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt := &Stmt{
		conn:  c,
		query: "SELECT 1",
	}

	vm := stmt.newVDBE()

	if vm == nil {
		t.Error("newVDBE() returned nil")
	}

	if vm.Ctx == nil {
		t.Error("VDBE context should not be nil")
	}

	if vm.Ctx.Btree != c.btree {
		t.Error("VDBE context should have connection's btree")
	}

	if vm.Ctx.Pager != c.pager {
		t.Error("VDBE context should have connection's pager")
	}

	if vm.Ctx.Schema != c.schema {
		t.Error("VDBE context should have connection's schema")
	}
}

func TestStmtCloseWithVDBE(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_close_vdbe.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Create a VDBE
	s.vdbe = s.newVDBE()

	// Close should finalize VDBE
	if err := s.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if s.vdbe != nil {
		t.Error("VDBE should be nil after close")
	}
}

func TestStmtConcurrentClose(t *testing.T) {
	dbFile := t.TempDir() + "/test_stmt_concurrent_close.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	stmt, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	s := stmt.(*Stmt)

	// Close from multiple goroutines concurrently
	done := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func() {
			s.Close()
			done <- true
		}()
	}

	// Wait for all closes to complete
	for i := 0; i < 3; i++ {
		<-done
	}

	// Statement should be closed
	if !s.closed {
		t.Error("statement should be closed")
	}
}
