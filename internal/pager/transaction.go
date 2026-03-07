// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"errors"
	"sync"
)

// TransactionState represents the current state of a transaction.
type TransactionState int

const (
	// TxNone indicates no transaction is active.
	TxNone TransactionState = iota

	// TxRead indicates a read transaction is active.
	TxRead

	// TxWrite indicates a write transaction is active.
	TxWrite
)

// TransactionManager manages transaction state and operations for the pager.
//
// SCAFFOLDING: Fields marked below are prepared for advanced transaction features:
// - journal: For rollback journal integration with pager
// - readRefs: For multi-reader concurrency tracking
// - writeOwner: For deadlock detection in concurrent writes
// - mu: For thread-safe transaction state management
//
// Currently using simplified transaction model; will be activated for:
// 1. WAL mode concurrent readers
// 2. Deadlock detection
// 3. Connection-level transaction isolation
type TransactionManager struct {
	// Current transaction state
	state TransactionState

	// Journal for write transactions
	// SCAFFOLDING: For direct journal integration
	journal *Journal

	// Read transaction reference count
	// SCAFFOLDING: For multi-reader tracking
	readRefs int

	// Write transaction owner (only one write transaction allowed)
	// SCAFFOLDING: For deadlock detection
	writeOwner interface{}

	// Mutex for thread-safe operations
	// SCAFFOLDING: For concurrent access protection
	mu sync.RWMutex
}

// NewTransactionManager creates a new transaction manager.
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		state: TxNone,
	}
}

// BeginRead starts a read transaction.
// Multiple read transactions can be active simultaneously.
func (p *Pager) BeginRead() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Acquire shared lock if not already held
	if p.state == PagerStateOpen {
		if err := p.acquireSharedLock(); err != nil {
			return err
		}
	}

	// If we're in an error state, can't start a transaction
	if p.state == PagerStateError {
		return p.errCode
	}

	// If already in a write transaction, that's fine - write includes read
	if p.state >= PagerStateWriterLocked {
		return nil
	}

	// Move to reader state
	p.state = PagerStateReader
	return nil
}

// BeginWrite starts a write transaction.
// Only one write transaction can be active at a time.
func (p *Pager) BeginWrite() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.readOnly {
		return ErrReadOnly
	}

	// If already in a write transaction, return error
	if p.state >= PagerStateWriterLocked && p.state < PagerStateError {
		return ErrTransactionOpen
	}

	// If in error state, can't start a transaction
	if p.state == PagerStateError {
		return p.errCode
	}

	// Start the write transaction
	return p.beginWriteTransaction()
}

// InTransaction returns true if any transaction is active.
func (p *Pager) InTransaction() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.state >= PagerStateReader && p.state < PagerStateError
}

// InWriteTransaction returns true if a write transaction is active.
func (p *Pager) InWriteTransaction() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.state >= PagerStateWriterLocked && p.state < PagerStateError
}

// GetTransactionState returns the current transaction state.
func (p *Pager) GetTransactionState() TransactionState {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.state == PagerStateError {
		return TxNone
	}

	if p.state >= PagerStateWriterLocked {
		return TxWrite
	}

	if p.state >= PagerStateReader {
		return TxRead
	}

	return TxNone
}

// EndRead ends a read transaction.
// This is automatically called when the connection is closed.
func (p *Pager) EndRead() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Only end read transaction if we're in reader state
	if p.state != PagerStateReader {
		return nil
	}

	// Release shared lock and return to open state
	p.state = PagerStateOpen
	p.lockState = LockNone

	return nil
}

// validateTransactionState checks if the pager state is valid for operations.
func (p *Pager) validateTransactionState() error {
	if p.state == PagerStateError {
		if p.errCode != nil {
			return p.errCode
		}
		return errors.New("pager is in error state")
	}
	return nil
}

// setErrorState sets the pager to error state with the given error.
func (p *Pager) setErrorState(err error) {
	p.state = PagerStateError
	p.errCode = err
}

// clearErrorState clears the error state.
func (p *Pager) clearErrorState() {
	if p.state == PagerStateError {
		p.state = PagerStateOpen
		p.errCode = nil
	}
}

// GetLockState returns the current lock state.
func (p *Pager) GetLockState() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lockState
}

// upgradeToWriteLock upgrades a read lock to a write lock.
func (p *Pager) upgradeToWriteLock() error {
	if p.readOnly {
		return ErrReadOnly
	}

	// Can't upgrade if we already have a write lock
	if p.lockState >= LockReserved {
		return nil
	}

	// Acquire reserved lock
	p.lockState = LockReserved

	return nil
}

// downgradeLock downgrades from write lock to read lock.
func (p *Pager) downgradeLock() error {
	// Can't downgrade if not in a write state
	if p.lockState < LockReserved {
		return nil
	}

	// Downgrade to shared lock
	p.lockState = LockShared

	return nil
}

// TryUpgradeToExclusive attempts to acquire an exclusive lock.
// Returns true if successful, false if the lock is held by another process.
// If a busy handler is set, it will retry on lock contention.
func (p *Pager) TryUpgradeToExclusive() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.lockState >= LockExclusive {
		return true, nil
	}

	// Try to acquire exclusive lock with busy handler if available
	var err error
	if p.busyHandler != nil {
		err = p.acquireExclusiveLockWithRetry()
	} else {
		err = p.tryAcquireExclusiveLock()
	}

	if err != nil {
		if err == ErrDatabaseLocked {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// WaitForReadersToFinish waits for all read transactions to complete.
// This is used before acquiring an exclusive lock for writing.
func (p *Pager) WaitForReadersToFinish() error {
	// In a real implementation, this would wait for file locks to be released
	// For now, we just check if we can upgrade to exclusive
	_, err := p.TryUpgradeToExclusive()
	return err
}

// Checkpoint performs a WAL checkpoint operation.
// In WAL mode, this copies all WAL frames back to the database.
// In other journal modes, this returns an error.
func (p *Pager) Checkpoint() error {
	return p.CheckpointMode(CheckpointPassive)
}

// CheckpointMode performs a WAL checkpoint with the specified mode.
func (p *Pager) CheckpointMode(mode CheckpointMode) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.journalMode != JournalModeWAL {
		return errors.New("checkpoint only supported in WAL mode")
	}

	if p.wal == nil {
		return errors.New("WAL not initialized")
	}

	// Perform checkpoint using WAL's checkpoint implementation
	_, _, err := p.wal.CheckpointWithMode(mode)
	return err
}

// SetJournalMode sets the journal mode for the pager.
func (p *Pager) SetJournalMode(mode int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != PagerStateOpen {
		return errors.New("cannot change journal mode during transaction")
	}

	if !isValidJournalMode(mode) {
		return errors.New("invalid journal mode")
	}

	if err := p.handleJournalModeTransition(mode); err != nil {
		return err
	}

	p.journalMode = mode
	return nil
}

// isValidJournalMode checks if a journal mode is valid.
func isValidJournalMode(mode int) bool {
	switch mode {
	case JournalModeDelete, JournalModePersist, JournalModeOff,
		JournalModeTruncate, JournalModeMemory, JournalModeWAL:
		return true
	default:
		return false
	}
}

// handleJournalModeTransition handles transitions between journal modes.
func (p *Pager) handleJournalModeTransition(newMode int) error {
	// Handle transition to WAL mode
	if newMode == JournalModeWAL && p.journalMode != JournalModeWAL {
		return p.enableWALMode()
	}

	// Handle transition from WAL mode
	if p.journalMode == JournalModeWAL && newMode != JournalModeWAL {
		return p.disableWALMode()
	}

	return nil
}

// enableWALMode enables WAL mode for the pager.
func (p *Pager) enableWALMode() error {
	if p.readOnly {
		return errors.New("cannot enable WAL mode on read-only database")
	}

	// Create WAL instance
	p.wal = NewWAL(p.filename, p.pageSize)
	if err := p.wal.Open(); err != nil {
		p.wal = nil
		return err
	}

	// Create WAL index
	walIndex, err := NewWALIndex(p.filename)
	if err != nil {
		p.wal.Close()
		p.wal = nil
		return err
	}
	p.walIndex = walIndex

	// Set WAL index page size
	if err := p.walIndex.SetPageCount(uint32(p.dbSize)); err != nil {
		p.walIndex.Close()
		p.wal.Close()
		p.walIndex = nil
		p.wal = nil
		return err
	}

	return nil
}

// disableWALMode disables WAL mode and checkpoints any remaining frames.
func (p *Pager) disableWALMode() error {
	if p.wal == nil {
		return nil
	}

	// Checkpoint all remaining frames
	if err := p.wal.Checkpoint(); err != nil {
		return err
	}

	// Close and delete WAL files
	if err := p.wal.Delete(); err != nil {
		return err
	}
	p.wal = nil

	if p.walIndex != nil {
		if err := p.walIndex.Delete(); err != nil {
			return err
		}
		p.walIndex = nil
	}

	return nil
}

// GetJournalMode returns the current journal mode.
func (p *Pager) GetJournalMode() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.journalMode
}

// IsAutoVacuum returns true if auto-vacuum is enabled.
func (p *Pager) IsAutoVacuum() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.header == nil {
		return false
	}

	// Check auto-vacuum flag in header (LargestRootPage > 0 indicates auto-vacuum)
	return p.header.LargestRootPage > 0
}

// GetPageCount returns the current page count.
// This is used during transaction processing.
func (p *Pager) GetPageCount() Pgno {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dbSize
}

// GetOriginalPageCount returns the page count at the start of the transaction.
func (p *Pager) GetOriginalPageCount() Pgno {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dbOrigSize
}
