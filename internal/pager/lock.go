package pager

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// LockLevel represents the different lock levels in SQLite's locking protocol.
// SQLite uses a five-level locking hierarchy to enable concurrent readers with
// a single writer.
//
// Lock levels (defined in pager.go):
//   - LockNone:      No locks are held. The database is unlocked.
//   - LockShared:    A SHARED lock allows reading from the database.
//     Multiple connections can hold SHARED locks simultaneously.
//     A SHARED lock prevents any other connection from modifying the database.
//   - LockReserved:  A RESERVED lock means the process is planning to write to
//     the database at some point in the future but is currently just reading.
//     Only one connection can hold a RESERVED lock at a time.
//     Other connections can continue to read (hold SHARED locks).
//   - LockPending:   A PENDING lock means the process holding the lock wants to
//     write to the database as soon as possible and is waiting for all current
//     SHARED locks to clear. No new SHARED locks are allowed while a PENDING
//     lock is held, but existing SHARED locks are allowed to continue.
//   - LockExclusive: An EXCLUSIVE lock is required to write to the database.
//     Only one connection can hold an EXCLUSIVE lock, and no other locks of
//     any kind can coexist with an EXCLUSIVE lock.
type LockLevel int

// Lock level constants as LockLevel type for convenience
const (
	lockNone      LockLevel = LockNone
	lockShared    LockLevel = LockShared
	lockReserved  LockLevel = LockReserved
	lockPending   LockLevel = LockPending
	lockExclusive LockLevel = LockExclusive
)

// String returns a string representation of the lock level.
func (l LockLevel) String() string {
	switch l {
	case lockNone:
		return "NONE"
	case lockShared:
		return "SHARED"
	case lockReserved:
		return "RESERVED"
	case lockPending:
		return "PENDING"
	case lockExclusive:
		return "EXCLUSIVE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", l)
	}
}

// Lock errors
var (
	ErrLockBusy    = errors.New("database is locked by another process")
	ErrLockTimeout = errors.New("timeout acquiring lock")
	ErrInvalidLock = errors.New("invalid lock transition")
	ErrLockNotHeld = errors.New("lock not held")
	ErrFileNotOpen = errors.New("file not open")
)

// LockManager manages file locks for a database file.
// It implements the SQLite locking protocol using platform-specific file locking.
type LockManager struct {
	// Database file handle
	file *os.File

	// Current lock level held by this manager
	currentLevel LockLevel

	// Mutex for thread-safe lock operations
	mu sync.RWMutex

	// Platform-specific lock data (used by platform implementations)
	platformData interface{}
}

// NewLockManager creates a new lock manager for the given file.
// The file must be open and remain open for the lifetime of the lock manager.
func NewLockManager(file *os.File) (*LockManager, error) {
	if file == nil {
		return nil, ErrFileNotOpen
	}

	lm := &LockManager{
		file:         file,
		currentLevel: lockNone,
	}

	// Initialize platform-specific lock data if needed
	if err := lm.initPlatform(); err != nil {
		return nil, fmt.Errorf("failed to initialize platform locks: %w", err)
	}

	return lm, nil
}

// AcquireLock attempts to acquire the specified lock level.
// This follows SQLite's lock escalation rules:
//   - NONE -> SHARED: Acquire shared lock
//   - SHARED -> RESERVED: Acquire reserved lock (keeps shared)
//   - RESERVED -> PENDING: Acquire pending lock
//   - PENDING -> EXCLUSIVE: Acquire exclusive lock (releases all others)
//   - Can also downgrade: EXCLUSIVE -> SHARED, RESERVED -> SHARED, etc.
//
// Returns ErrLockBusy if the lock cannot be acquired due to conflicts.
func (lm *LockManager) AcquireLock(level LockLevel) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Already have the requested lock level
	if lm.currentLevel == level {
		return nil
	}

	// Validate lock transition
	if !lm.isValidTransition(lm.currentLevel, level) {
		return fmt.Errorf("%w: cannot transition from %s to %s",
			ErrInvalidLock, lm.currentLevel, level)
	}

	// Perform the lock operation
	if err := lm.acquireLockPlatform(level); err != nil {
		return err
	}

	lm.currentLevel = level
	return nil
}

// ReleaseLock releases the current lock back to the specified level.
// This is used to downgrade locks. To fully release all locks, use LockNone.
func (lm *LockManager) ReleaseLock(level LockLevel) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Already at the requested level or lower
	if lm.currentLevel <= level {
		return nil
	}

	// Perform the unlock operation
	if err := lm.releaseLockPlatform(level); err != nil {
		return err
	}

	lm.currentLevel = level
	return nil
}

// GetLockState returns the current lock level.
func (lm *LockManager) GetLockState() LockLevel {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.currentLevel
}

// Close releases all locks and cleans up resources.
func (lm *LockManager) Close() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.currentLevel == lockNone {
		return nil
	}

	// Release all locks
	if err := lm.releaseLockPlatform(lockNone); err != nil {
		return err
	}

	lm.currentLevel = lockNone

	// Clean up platform-specific resources
	if err := lm.cleanupPlatform(); err != nil {
		return err
	}

	return nil
}

// isValidTransition checks if transitioning from one lock level to another is valid.
func (lm *LockManager) isValidTransition(from, to LockLevel) bool {
	// Can always stay at the same level
	if from == to {
		return true
	}

	// Check valid transition based on source lock level
	switch from {
	case lockNone:
		return lm.isValidFromNone(to)
	case lockShared:
		return lm.isValidFromShared(to)
	case lockReserved:
		return lm.isValidFromReserved(to)
	case lockPending:
		return lm.isValidFromPending(to)
	case lockExclusive:
		return lm.isValidFromExclusive(to)
	default:
		return false
	}
}

// isValidFromNone checks valid transitions from NONE lock level.
func (lm *LockManager) isValidFromNone(to LockLevel) bool {
	// Can acquire any lock from NONE, but typically start with SHARED
	return to == lockShared || to == lockExclusive
}

// isValidFromShared checks valid transitions from SHARED lock level.
func (lm *LockManager) isValidFromShared(to LockLevel) bool {
	// From SHARED, can go to RESERVED or EXCLUSIVE, or back to NONE
	return to == lockReserved || to == lockExclusive || to == lockNone
}

// isValidFromReserved checks valid transitions from RESERVED lock level.
func (lm *LockManager) isValidFromReserved(to LockLevel) bool {
	// From RESERVED, can go to PENDING or EXCLUSIVE, or downgrade to SHARED or NONE
	return to == lockPending || to == lockExclusive || to == lockShared || to == lockNone
}

// isValidFromPending checks valid transitions from PENDING lock level.
func (lm *LockManager) isValidFromPending(to LockLevel) bool {
	// From PENDING, can go to EXCLUSIVE, or downgrade to SHARED or NONE
	return to == lockExclusive || to == lockShared || to == lockNone
}

// isValidFromExclusive checks valid transitions from EXCLUSIVE lock level.
func (lm *LockManager) isValidFromExclusive(to LockLevel) bool {
	// From EXCLUSIVE, can only downgrade to SHARED or NONE
	return to == lockShared || to == lockNone
}

// TryAcquireLock attempts to acquire a lock without blocking.
// Returns ErrLockBusy immediately if the lock cannot be acquired.
func (lm *LockManager) TryAcquireLock(level LockLevel) error {
	// For now, this is the same as AcquireLock since our platform
	// implementations use non-blocking locks by default.
	// In a future implementation with timeouts, this would be different.
	return lm.AcquireLock(level)
}

// IsLockHeld checks if a specific lock level is currently held.
// Returns true if the current lock level is >= the specified level.
func (lm *LockManager) IsLockHeld(level LockLevel) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.currentLevel >= level
}

// CanAcquire checks if acquiring a specific lock level would be valid
// without actually acquiring it.
func (lm *LockManager) CanAcquire(level LockLevel) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.isValidTransition(lm.currentLevel, level)
}
