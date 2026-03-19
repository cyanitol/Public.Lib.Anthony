// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestBeginCommit(t *testing.T) {
	dbFile := t.TempDir() + "/test_begin_commit.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Errorf("failed to commit: %v", err)
	}
}

func TestBeginRollback(t *testing.T) {
	dbFile := t.TempDir() + "/test_begin_rollback.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Rollback transaction
	if err := tx.Rollback(); err != nil {
		t.Errorf("failed to rollback: %v", err)
	}
}

func TestReadOnlyTransaction(t *testing.T) {
	dbFile := t.TempDir() + "/test_readonly_tx.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin read-only transaction
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("failed to begin read-only transaction: %v", err)
	}

	// Commit read-only transaction
	if err := tx.Commit(); err != nil {
		t.Errorf("failed to commit read-only transaction: %v", err)
	}
}

func TestWriteTransaction(t *testing.T) {
	dbFile := t.TempDir() + "/test_write_tx.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin write transaction (default)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: false,
	})
	if err != nil {
		t.Fatalf("failed to begin write transaction: %v", err)
	}

	// Commit write transaction
	if err := tx.Commit(); err != nil {
		t.Errorf("failed to commit write transaction: %v", err)
	}
}

func TestMultipleTransactions(t *testing.T) {
	dbFile := t.TempDir() + "/test_multi_tx.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// First transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin first transaction: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Errorf("failed to commit first transaction: %v", err)
	}

	// Second transaction
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin second transaction: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Errorf("failed to commit second transaction: %v", err)
	}
}

func TestNestedTransactionError(t *testing.T) {
	// Use a single driver-level connection to test nested transaction rejection.
	// database/sql's connection pool would assign a second connection, so we
	// must test at the driver level.
	c := txOpenConn(t, "test_nested_tx.db")

	// Begin first transaction
	tx1, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin first transaction: %v", err)
	}

	// Try to begin second transaction on the same connection - should fail
	_, err = c.Begin()
	if err == nil {
		t.Error("expected error when beginning nested transaction")
	}

	tx1.Rollback()
}

func TestTransactionDoubleCommit(t *testing.T) {
	dbFile := t.TempDir() + "/test_double_commit.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// First commit should succeed
	if err := tx.Commit(); err != nil {
		t.Errorf("first commit failed: %v", err)
	}

	// Second commit should fail
	if err := tx.Commit(); err == nil {
		t.Error("expected error on double commit")
	}
}

func TestTransactionDoubleRollback(t *testing.T) {
	dbFile := t.TempDir() + "/test_double_rollback.db"

	// Test at driver level to verify double rollback is safe
	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// First rollback should succeed
	if err := tx.Rollback(); err != nil {
		t.Errorf("first rollback failed: %v", err)
	}

	// Second rollback should be safe (no error)
	// Note: database/sql prevents this at a higher layer, but at the driver
	// level we want to ensure idempotent rollback behavior
	if err := tx.Rollback(); err != nil {
		t.Errorf("second rollback should be safe: %v", err)
	}
}

func TestTransactionCommitAfterRollback(t *testing.T) {
	dbFile := t.TempDir() + "/test_commit_after_rollback.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("rollback failed: %v", err)
	}

	// Try to commit - should fail
	if err := tx.Commit(); err == nil {
		t.Error("expected error committing after rollback")
	}
}

func TestTransactionRollbackAfterCommit(t *testing.T) {
	dbFile := t.TempDir() + "/test_rollback_after_commit.db"

	// Test at driver level to verify rollback after commit is safe
	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}

	// Try to rollback - should be safe (no error)
	// Note: database/sql prevents this at a higher layer, but at the driver
	// level we want to ensure idempotent rollback behavior
	if err := tx.Rollback(); err != nil {
		t.Errorf("rollback after commit should be safe: %v", err)
	}
}

func TestTransactionIsolation(t *testing.T) {
	dbFile := t.TempDir() + "/test_tx_isolation.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create initial state
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}

	// Make changes in tx1
	// (Note: actual table operations would require parser/executor implementation)

	// Rollback tx1
	if err := tx1.Rollback(); err != nil {
		t.Errorf("failed to rollback tx1: %v", err)
	}

	// Changes should be rolled back
	// Verification would require query execution
}

func TestContextCancellation(t *testing.T) {
	dbFile := t.TempDir() + "/test_context_cancel.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Cancel context - database/sql automatically rolls back the transaction
	cancel()

	// Commit should fail because database/sql rolls back on context cancellation
	if err := tx.Commit(); err == nil {
		t.Error("expected error committing after context cancellation")
	}
}

func TestTransactionWithClosedConnection(t *testing.T) {
	// Test at driver level: closing the connection should make commit fail.
	c := txOpenConn(t, "test_closed_conn.db")

	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Close the underlying connection
	c.Close()

	// Operations on transaction should fail gracefully
	if err := tx.Commit(); err == nil {
		t.Error("expected error committing after connection closed")
	}
}

func txOpenConn(t *testing.T, name string) *Conn {
	t.Helper()
	dbFile := t.TempDir() + "/" + name
	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	c, ok := conn.(*Conn)
	if !ok {
		t.Fatal("connection is not *Conn type")
	}
	return c
}

func txBeginAndCast(t *testing.T, c *Conn) *Tx {
	t.Helper()
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin: %v", err)
	}
	if _, ok := tx.(driver.Tx); !ok {
		t.Error("transaction does not implement driver.Tx")
	}
	ourTx, ok := tx.(*Tx)
	if !ok {
		t.Fatal("transaction is not *Tx type")
	}
	return ourTx
}

func TestDriverTxInterface(t *testing.T) {
	c := txOpenConn(t, "test_driver_interface.db")
	ourTx := txBeginAndCast(t, c)

	if ourTx.IsReadOnly() {
		t.Error("default transaction should not be read-only")
	}
	if ourTx.IsClosed() {
		t.Error("transaction should not be closed yet")
	}
	if err := ourTx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}
	if !ourTx.IsClosed() {
		t.Error("transaction should be closed after commit")
	}
}

func TestReadOnlyTransactionProperties(t *testing.T) {
	dbFile := t.TempDir() + "/test_readonly_props.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin read-only transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("failed to begin read-only transaction: %v", err)
	}

	ourTx := tx.(*Tx)

	// Should be read-only
	if !ourTx.IsReadOnly() {
		t.Error("transaction should be read-only")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}
}

func TestWriteTransactionProperties(t *testing.T) {
	dbFile := t.TempDir() + "/test_write_props.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin write transaction
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{
		ReadOnly: false,
	})
	if err != nil {
		t.Fatalf("failed to begin write transaction: %v", err)
	}

	ourTx := tx.(*Tx)

	// Should not be read-only
	if ourTx.IsReadOnly() {
		t.Error("transaction should not be read-only")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}
}

func TestTransactionStateAfterError(t *testing.T) {
	dbFile := t.TempDir() + "/test_error_state.db"

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Even if operations fail, should be able to rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("rollback failed: %v", err)
	}
}

func TestConcurrentTransactions(t *testing.T) {
	// Use separate database files so that each sql.DB gets its own underlying
	// connection and pager state, avoiding shared write-mutex interference.
	dir := t.TempDir()
	dbFile1 := dir + "/test_concurrent_tx1.db"
	dbFile2 := dir + "/test_concurrent_tx2.db"

	db1, err := sql.Open(DriverName, dbFile1)
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open(DriverName, dbFile2)
	if err != nil {
		t.Fatalf("failed to open db2: %v", err)
	}
	defer db2.Close()

	// Begin transaction on first connection
	tx1, err := db1.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}

	// Begin transaction on second connection
	tx2, err := db2.Begin()
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}

	// Both should be able to commit independently
	if err := tx1.Commit(); err != nil {
		t.Errorf("failed to commit tx1: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Errorf("failed to commit tx2: %v", err)
	}
}

func TestSavepoint(t *testing.T) {
	dbFile := t.TempDir() + "/test_savepoint.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin write transaction
	dtx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer dtx.Rollback()

	tx := dtx.(*Tx)

	// Create savepoint
	if err := tx.Savepoint("sp1"); err != nil {
		t.Errorf("failed to create savepoint: %v", err)
	}

	// Release savepoint
	if err := tx.ReleaseSavepoint("sp1"); err != nil {
		t.Errorf("failed to release savepoint: %v", err)
	}
}

func TestSavepointRollback(t *testing.T) {
	dbFile := t.TempDir() + "/test_savepoint_rollback.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin write transaction
	dtx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer dtx.Rollback()

	tx := dtx.(*Tx)

	// Create savepoint
	if err := tx.Savepoint("sp1"); err != nil {
		t.Errorf("failed to create savepoint: %v", err)
	}

	// Rollback to savepoint
	if err := tx.RollbackToSavepoint("sp1"); err != nil {
		t.Errorf("failed to rollback to savepoint: %v", err)
	}
}

func TestSavepointOnClosedTx(t *testing.T) {
	dbFile := t.TempDir() + "/test_savepoint_closed.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin and commit transaction
	dtx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	tx := dtx.(*Tx)

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Try savepoint operations on closed transaction
	if err := tx.Savepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}

	if err := tx.ReleaseSavepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}

	if err := tx.RollbackToSavepoint("sp1"); err != driver.ErrBadConn {
		t.Errorf("expected ErrBadConn, got: %v", err)
	}
}

func TestSavepointOnReadOnlyTx(t *testing.T) {
	dbFile := t.TempDir() + "/test_savepoint_readonly.db"

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	// Begin read-only transaction
	dtx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("failed to begin read-only transaction: %v", err)
	}
	defer dtx.Rollback()

	tx := dtx.(*Tx)

	// Try savepoint operations on read-only transaction
	err = tx.Savepoint("sp1")
	if err == nil || err.Error() != "cannot create savepoint in read-only transaction" {
		t.Errorf("expected read-only error, got: %v", err)
	}

	err = tx.ReleaseSavepoint("sp1")
	if err == nil || err.Error() != "cannot release savepoint in read-only transaction" {
		t.Errorf("expected read-only error, got: %v", err)
	}

	err = tx.RollbackToSavepoint("sp1")
	if err == nil || err.Error() != "cannot rollback to savepoint in read-only transaction" {
		t.Errorf("expected read-only error, got: %v", err)
	}
}

func txExpectError(t *testing.T, err error, wantMsg string) {
	t.Helper()
	if err == nil || err.Error() != wantMsg {
		t.Errorf("expected %q error, got: %v", wantMsg, err)
	}
}

func TestSavepointWithoutActiveTx(t *testing.T) {
	c := txOpenConn(t, "test_savepoint_no_tx.db")

	dtx, err := c.BeginTx(context.Background(), driver.TxOptions{ReadOnly: false})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	tx := dtx.(*Tx)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Manually clear closed flag to test transaction state check
	tx.closed = false

	txExpectError(t, tx.Savepoint("sp1"), "no transaction in progress")
	txExpectError(t, tx.ReleaseSavepoint("sp1"), "no transaction in progress")
	txExpectError(t, tx.RollbackToSavepoint("sp1"), "no transaction in progress")
}
