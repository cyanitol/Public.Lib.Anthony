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
type TransactionManager struct {
	// Current transaction state
	state TransactionState

	// Journal for write transactions
	journal *Journal

	// Read transaction reference count
	readRefs int

	// Write transaction owner (only one write transaction allowed)
	writeOwner interface{}

	// Mutex for thread-safe operations
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
// This is a placeholder for future WAL mode support.
func (p *Pager) Checkpoint() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// WAL mode not yet implemented
	return errors.New("checkpoint not supported in delete journal mode")
}

// SetJournalMode sets the journal mode for the pager.
func (p *Pager) SetJournalMode(mode int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Can't change journal mode during a transaction
	if p.state != PagerStateOpen {
		return errors.New("cannot change journal mode during transaction")
	}

	// Validate journal mode
	switch mode {
	case JournalModeDelete, JournalModePersist, JournalModeOff,
		JournalModeTruncate, JournalModeMemory:
		p.journalMode = mode
		return nil
	default:
		return errors.New("invalid journal mode")
	}
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
