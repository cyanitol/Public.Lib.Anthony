// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"os"
	"testing"
)

func TestPrepareContextErrors(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
		},
		{
			name:    "invalid SQL",
			query:   "INVALID SQL SYNTAX",
			wantErr: true,
		},
		{
			name:    "multiple statements",
			query:   "SELECT 1; SELECT 2",
			wantErr: false, // Multi-statement is now supported via MultiStmt
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			dbFile := "test_prepare_" + tt.name + ".db"
			defer os.Remove(dbFile)

			d := &Driver{}
			conn, err := d.Open(dbFile)
			if err != nil {
				t.Fatalf("failed to open connection: %v", err)
			}
			defer conn.Close()

			c := conn.(*Conn)

			_, err = c.PrepareContext(context.Background(), tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("PrepareContext() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrepareContextOnClosedConn(t *testing.T) {
	dbFile := "test_prepare_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close the connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Try to prepare on closed connection
	_, err = c.PrepareContext(context.Background(), "SELECT 1")
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestCloseWithStatements(t *testing.T) {
	dbFile := "test_close_with_stmts.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Prepare multiple statements
	stmt1, err := c.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("failed to prepare stmt1: %v", err)
	}

	stmt2, err := c.PrepareContext(context.Background(), "SELECT 2")
	if err != nil {
		t.Fatalf("failed to prepare stmt2: %v", err)
	}

	// Close connection should close all statements
	if err := c.Close(); err != nil {
		t.Errorf("failed to close connection: %v", err)
	}

	// Statements should be closed
	s1 := stmt1.(*Stmt)
	s2 := stmt2.(*Stmt)

	if !s1.closed {
		t.Error("stmt1 should be closed")
	}
	if !s2.closed {
		t.Error("stmt2 should be closed")
	}
}

func TestCloseWithActiveTransaction(t *testing.T) {
	dbFile := "test_close_with_tx.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Begin transaction
	_, err = c.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Close connection should rollback transaction
	if err := c.Close(); err != nil {
		t.Errorf("failed to close connection: %v", err)
	}

	// Connection should be closed
	if !c.closed {
		t.Error("connection should be closed")
	}

	// Note: We can't easily verify transaction state after close
	// because the pager state is cleaned up. The important part
	// is that Close() succeeds and doesn't leak resources.
}

func TestCloseIdempotent(t *testing.T) {
	dbFile := "test_close_idempotent.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close first time
	if err := c.Close(); err != nil {
		t.Errorf("first close failed: %v", err)
	}

	// Close second time should be safe
	if err := c.Close(); err != nil {
		t.Errorf("second close should be safe: %v", err)
	}
}

func TestPingOnClosedConnection(t *testing.T) {
	dbFile := "test_ping_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Ping should fail
	if err := c.Ping(context.Background()); err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestResetSession(t *testing.T) {
	dbFile := "test_reset_session.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Reset session should succeed when no transaction is active
	if err := c.ResetSession(context.Background()); err != nil {
		t.Errorf("ResetSession failed: %v", err)
	}
}

func TestResetSessionWithActiveTransaction(t *testing.T) {
	dbFile := "test_reset_session_tx.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	_, err = c.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Reset session should fail with active transaction
	err = c.ResetSession(context.Background())
	if err == nil || err.Error() != "cannot reset session with active transaction" {
		t.Errorf("expected error about active transaction, got: %v", err)
	}
}

func TestResetSessionOnClosedConnection(t *testing.T) {
	dbFile := "test_reset_session_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Reset session should fail
	if err := c.ResetSession(context.Background()); err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestCheckNamedValue(t *testing.T) {
	dbFile := "test_check_named_value.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// CheckNamedValue should return ErrSkip (allow driver to handle it)
	nv := &driver.NamedValue{
		Ordinal: 1,
		Value:   int64(42),
	}

	if err := c.CheckNamedValue(nv); err != driver.ErrSkip {
		t.Errorf("expected ErrSkip, got: %v", err)
	}
}

func TestCreateScalarFunctionOnClosedConn(t *testing.T) {
	dbFile := "test_scalar_func_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Try to create function on closed connection
	err = c.CreateScalarFunction("test", 1, true, nil)
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestCreateAggregateFunctionOnClosedConn(t *testing.T) {
	dbFile := "test_agg_func_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Try to create function on closed connection
	err = c.CreateAggregateFunction("test", 1, true, nil)
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestUnregisterFunctionOnClosedConn(t *testing.T) {
	dbFile := "test_unreg_func_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Try to unregister function on closed connection
	if result := c.UnregisterFunction("test", 1); result {
		t.Error("expected false for closed connection")
	}
}

func TestBeginTxOnClosedConnection(t *testing.T) {
	dbFile := "test_begintx_closed.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	c := conn.(*Conn)

	// Close connection
	if err := c.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	// Try to begin transaction on closed connection
	_, err = c.BeginTx(context.Background(), driver.TxOptions{})
	if err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}
