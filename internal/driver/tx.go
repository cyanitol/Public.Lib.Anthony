// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"
)

// Tx implements database/sql/driver.Tx for SQLite.
type Tx struct {
	conn     *Conn
	readOnly bool
	closed   bool
}

// Commit commits the transaction.
func (tx *Tx) Commit() error {
	if tx.closed {
		return driver.ErrBadConn
	}

	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return fmt.Errorf("no transaction in progress")
	}

	// For read-only transactions, just end the read transaction
	if tx.readOnly {
		if err := tx.conn.pager.EndRead(); err != nil {
			return fmt.Errorf("failed to end read transaction: %w", err)
		}
	} else {
		// For write transactions, check deferred FK constraints before committing
		if err := tx.conn.checkDeferredFKConstraints(); err != nil {
			return err
		}

		// Commit the pager transaction
		if err := tx.conn.pager.Commit(); err != nil {
			return fmt.Errorf("commit failed: %w", err)
		}

		// Clear deferred FK violations after successful commit
		tx.conn.clearDeferredFKViolations()
	}

	tx.conn.inTx = false
	tx.conn.setFKTransactionState(false)
	tx.closed = true

	return nil
}

// Rollback rolls back the transaction.
func (tx *Tx) Rollback() error {
	if tx.closed {
		return nil // Already rolled back or committed
	}

	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		// Transaction not active - mark as closed and return success
		// This handles the case where Rollback is called multiple times
		tx.closed = true
		return nil
	}

	// Clear deferred FK violations before rollback
	tx.conn.clearDeferredFKViolations()

	// For read-only transactions, just end the read transaction
	if tx.readOnly {
		if err := tx.conn.pager.EndRead(); err != nil {
			return fmt.Errorf("failed to end read transaction: %w", err)
		}
	} else {
		// For write transactions, rollback the pager transaction
		if err := tx.conn.pager.Rollback(); err != nil {
			return fmt.Errorf("rollback failed: %w", err)
		}
		// Clear btree cache so pages are re-read from disk with restored data
		tx.conn.btree.ClearCache()
	}

	tx.conn.inTx = false
	tx.conn.setFKTransactionState(false)
	tx.closed = true

	return nil
}

// IsReadOnly returns true if this is a read-only transaction.
func (tx *Tx) IsReadOnly() bool {
	return tx.readOnly
}

// IsClosed returns true if the transaction has been committed or rolled back.
func (tx *Tx) IsClosed() bool {
	return tx.closed
}

// Savepoint creates a named savepoint within the transaction.
// This is not part of the standard driver.Tx interface, but can be
// used through direct calls or SQL statements.
func (tx *Tx) Savepoint(name string) error {
	if tx.closed {
		return driver.ErrBadConn
	}

	if tx.readOnly {
		return fmt.Errorf("cannot create savepoint in read-only transaction")
	}

	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return fmt.Errorf("no transaction in progress")
	}

	return tx.conn.pager.Savepoint(name)
}

// ReleaseSavepoint releases a savepoint and all savepoints created after it.
func (tx *Tx) ReleaseSavepoint(name string) error {
	if tx.closed {
		return driver.ErrBadConn
	}

	if tx.readOnly {
		return fmt.Errorf("cannot release savepoint in read-only transaction")
	}

	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return fmt.Errorf("no transaction in progress")
	}

	return tx.conn.pager.Release(name)
}

// RollbackToSavepoint rolls back to a savepoint.
func (tx *Tx) RollbackToSavepoint(name string) error {
	if tx.closed {
		return driver.ErrBadConn
	}

	if tx.readOnly {
		return fmt.Errorf("cannot rollback to savepoint in read-only transaction")
	}

	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()

	if !tx.conn.inTx {
		return fmt.Errorf("no transaction in progress")
	}

	return tx.conn.pager.RollbackTo(name)
}
