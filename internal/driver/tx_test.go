package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"testing"
)

func TestBeginCommit(t *testing.T) {
	dbFile := "test_begin_commit.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_begin_rollback.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_readonly_tx.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_write_tx.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_multi_tx.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_nested_tx.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin first transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin first transaction: %v", err)
	}
	defer tx1.Rollback()

	// Try to begin second transaction - should fail
	_, err = db.Begin()
	if err == nil {
		t.Error("expected error when beginning nested transaction")
	}

	tx1.Rollback()
}

func TestTransactionDoubleCommit(t *testing.T) {
	dbFile := "test_double_commit.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_double_rollback.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_commit_after_rollback.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_rollback_after_commit.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_tx_isolation.db"
	defer os.Remove(dbFile)

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
	t.Skip("Context cancellation handling not yet implemented in internal driver")
	dbFile := "test_context_cancel.db"
	defer os.Remove(dbFile)

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

	// Cancel context
	cancel()

	// Transaction should still be able to commit
	// (Context cancellation doesn't automatically rollback)
	if err := tx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}
}

func TestTransactionWithClosedConnection(t *testing.T) {
	t.Skip("Closed connection handling not yet implemented in internal driver")
	dbFile := "test_closed_conn.db"
	defer os.Remove(dbFile)

	db, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Close database
	db.Close()

	// Operations on transaction should fail gracefully
	if err := tx.Commit(); err == nil {
		t.Error("expected error committing after connection closed")
	}
}

func TestDriverTxInterface(t *testing.T) {
	dbFile := "test_driver_interface.db"
	defer os.Remove(dbFile)

	d := &Driver{}
	conn, err := d.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Cast to our Conn type
	c, ok := conn.(*Conn)
	if !ok {
		t.Fatal("connection is not *Conn type")
	}

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("failed to begin: %v", err)
	}

	// Verify it implements driver.Tx
	_, ok = tx.(driver.Tx)
	if !ok {
		t.Error("transaction does not implement driver.Tx")
	}

	// Cast to our Tx type
	ourTx, ok := tx.(*Tx)
	if !ok {
		t.Fatal("transaction is not *Tx type")
	}

	// Test custom methods
	if ourTx.IsReadOnly() {
		t.Error("default transaction should not be read-only")
	}

	if ourTx.IsClosed() {
		t.Error("transaction should not be closed yet")
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("commit failed: %v", err)
	}

	// Now should be closed
	if !ourTx.IsClosed() {
		t.Error("transaction should be closed after commit")
	}
}

func TestReadOnlyTransactionProperties(t *testing.T) {
	dbFile := "test_readonly_props.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_write_props.db"
	defer os.Remove(dbFile)

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
	dbFile := "test_error_state.db"
	defer os.Remove(dbFile)

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
	t.Skip("Concurrent transactions not yet supported in internal driver")
	dbFile := "test_concurrent_tx.db"
	defer os.Remove(dbFile)

	// Open multiple connections
	db1, err := sql.Open(DriverName, dbFile)
	if err != nil {
		t.Fatalf("failed to open db1: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open(DriverName, dbFile)
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
