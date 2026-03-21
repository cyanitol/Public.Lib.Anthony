// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build unix || linux || darwin || freebsd || openbsd || netbsd

package pager

import (
	"fmt"
	"syscall"
)

// Platform-specific lock implementation for Unix systems.
// This implementation uses OFD (Open File Description) locks when available,
// which are per-file-descriptor rather than per-process. This allows proper
// locking between different file descriptors in the same process.
//
// SQLite uses byte-range locks on specific regions of the database file:
//
//   Byte Range         Lock Type    Lock Name
//   -----------        ---------    ---------
//   PENDING_BYTE       Exclusive    PENDING lock
//   RESERVED_BYTE      Exclusive    RESERVED lock
//   SHARED_FIRST to    Shared       SHARED lock
//   SHARED_FIRST+510
//
// The SHARED lock uses multiple bytes to allow for process-specific locking.
// SQLite uses a randomized byte within the SHARED range for each connection.

const (
	// Lock byte offsets (matching SQLite's implementation)
	// These offsets are in the region beyond the maximum database size
	pendingByte  = 0x40000000      // 1GB mark - PENDING lock byte
	reservedByte = pendingByte + 1 // RESERVED lock byte
	sharedFirst  = pendingByte + 2 // Start of SHARED lock range
	sharedSize   = 510             // Number of bytes in SHARED lock range
)

// OFD lock command constants (Linux-specific, available since kernel 3.15)
// These provide per-file-descriptor locking instead of per-process locking
const (
	// F_OFD_GETLK gets the lock info for open file description locks
	F_OFD_GETLK = 36
	// F_OFD_SETLK sets/clears an open file description lock (non-blocking)
	F_OFD_SETLK = 37
	// F_OFD_SETLKW sets/clears an open file description lock (blocking)
	F_OFD_SETLKW = 38
)

// unixLockData holds Unix-specific locking information.
type unixLockData struct {
	// Which shared byte we're using (randomized within the range)
	sharedByte int64
	// Whether OFD locks are available (detected at init time)
	useOFD bool
}

// initPlatform initializes platform-specific lock data.
func (lm *LockManager) initPlatform() error {
	// Use a simple hash of the file descriptor to pick a shared byte
	// In a real implementation, this could be randomized or based on PID
	fd := lm.file.Fd()
	sharedOffset := (fd % sharedSize)

	data := &unixLockData{
		sharedByte: sharedFirst + int64(sharedOffset),
		useOFD:     true, // Try OFD locks by default
	}

	// Test if OFD locks are supported by attempting a test lock
	testLock := syscall.Flock_t{
		Type:   syscall.F_RDLCK,
		Whence: 0,
		Start:  0,
		Len:    0, // Lock entire file
	}
	err := syscall.FcntlFlock(fd, F_OFD_GETLK, &testLock)
	if err != nil {
		// OFD locks not supported, fall back to POSIX locks
		data.useOFD = false
	}

	lm.platformData = data
	return nil
}

// cleanupPlatform cleans up platform-specific resources.
func (lm *LockManager) cleanupPlatform() error {
	// Nothing special to clean up on Unix
	return nil
}

// fcntlCmd returns the appropriate fcntl command based on OFD availability
func (lm *LockManager) fcntlSetLk() int {
	data := lm.platformData.(*unixLockData)
	if data.useOFD {
		return F_OFD_SETLK
	}
	return syscall.F_SETLK
}

// fcntlGetLk returns the appropriate fcntl command for lock testing
func (lm *LockManager) fcntlGetLk() int {
	data := lm.platformData.(*unixLockData)
	if data.useOFD {
		return F_OFD_GETLK
	}
	return syscall.F_GETLK
}

// acquireLockPlatform performs the platform-specific lock acquisition.
func (lm *LockManager) acquireLockPlatform(level LockLevel) error {
	switch level {
	case lockNone:
		// No locks to acquire
		return nil

	case lockShared:
		return lm.acquireSharedLock()

	case lockReserved:
		return lm.acquireReservedLock()

	case lockPending:
		return lm.acquirePendingLock()

	case lockExclusive:
		return lm.acquireExclusiveLock()

	default:
		return fmt.Errorf("unknown lock level: %d", level)
	}
}

// shouldReleaseLock determines if a specific lock level should be released.
func (lm *LockManager) shouldReleaseLock(currentLevel, targetLevel, lockType LockLevel) bool {
	return lm.currentLevel >= currentLevel && targetLevel < lockType
}

// lockReleaseStep represents a lock release operation.
type lockReleaseStep struct {
	level       LockLevel
	releaseFunc func() error
}

// releaseLockPlatform performs the platform-specific lock release.
func (lm *LockManager) releaseLockPlatform(level LockLevel) error {
	steps := []lockReleaseStep{
		{lockExclusive, lm.releaseExclusiveLock},
		{lockPending, lm.releasePendingLock},
		{lockReserved, lm.releaseReservedLock},
		{lockShared, lm.releaseSharedLock},
	}

	for _, step := range steps {
		if lm.shouldReleaseLock(step.level, level, step.level) {
			if err := step.releaseFunc(); err != nil {
				return err
			}
		}
	}

	return nil
}

// acquireSharedLock acquires a SHARED lock.
func (lm *LockManager) acquireSharedLock() error {
	data := lm.platformData.(*unixLockData)

	// Try to acquire a shared (read) lock on one byte in the SHARED range
	lock := syscall.Flock_t{
		Type:   syscall.F_RDLCK, // Read lock
		Whence: 0,               // SEEK_SET
		Start:  data.sharedByte,
		Len:    1,
	}

	// Use F_OFD_SETLK or F_SETLK for non-blocking lock
	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		if err == syscall.EAGAIN || err == syscall.EACCES {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire shared lock: %w", err)
	}

	return nil
}

// releaseSharedLock releases the SHARED lock.
func (lm *LockManager) releaseSharedLock() error {
	data := lm.platformData.(*unixLockData)

	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  data.sharedByte,
		Len:    1,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		return fmt.Errorf("failed to release shared lock: %w", err)
	}

	return nil
}

// acquireReservedLock acquires a RESERVED lock.
func (lm *LockManager) acquireReservedLock() error {
	// RESERVED lock is an exclusive lock on the RESERVED byte
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK, // Write lock
		Whence: 0,
		Start:  reservedByte,
		Len:    1,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		if err == syscall.EAGAIN || err == syscall.EACCES {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire reserved lock: %w", err)
	}

	// Must maintain the SHARED lock when acquiring RESERVED
	if lm.currentLevel < lockShared {
		if err := lm.acquireSharedLock(); err != nil {
			// Rollback the reserved lock
			lm.releaseReservedLock()
			return err
		}
	}

	return nil
}

// releaseReservedLock releases the RESERVED lock.
func (lm *LockManager) releaseReservedLock() error {
	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  reservedByte,
		Len:    1,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		return fmt.Errorf("failed to release reserved lock: %w", err)
	}

	return nil
}

// acquirePendingLock acquires a PENDING lock.
func (lm *LockManager) acquirePendingLock() error {
	// PENDING lock is an exclusive lock on the PENDING byte
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  pendingByte,
		Len:    1,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		if err == syscall.EAGAIN || err == syscall.EACCES {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire pending lock: %w", err)
	}

	// Must maintain SHARED and RESERVED locks when acquiring PENDING
	if lm.currentLevel < lockReserved {
		if err := lm.acquireReservedLock(); err != nil {
			lm.releasePendingLock()
			return err
		}
	}

	return nil
}

// releasePendingLock releases the PENDING lock.
func (lm *LockManager) releasePendingLock() error {
	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  pendingByte,
		Len:    1,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		return fmt.Errorf("failed to release pending lock: %w", err)
	}

	return nil
}

// acquireExclusiveLock acquires an EXCLUSIVE lock.
func (lm *LockManager) acquireExclusiveLock() error {
	data := lm.platformData.(*unixLockData)

	// EXCLUSIVE lock requires:
	// 1. Exclusive lock on all bytes in the SHARED range (to block new readers)
	// 2. Waiting for existing SHARED locks to be released

	// First, acquire PENDING if not already held (to block new SHARED locks)
	if lm.currentLevel < lockPending {
		if err := lm.acquirePendingLock(); err != nil {
			return err
		}
	}

	// Now try to acquire exclusive lock on the entire SHARED range
	// This will block until all readers have released their locks
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK, // Write lock (exclusive)
		Whence: 0,
		Start:  sharedFirst,
		Len:    sharedSize,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		if lm.currentLevel < lockPending {
			lm.releasePendingLock()
		}
		if err == syscall.EAGAIN || err == syscall.EACCES {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire exclusive lock: %w", err)
	}

	// Release our individual SHARED lock since we now have exclusive access
	// We ignore errors here because we might not have had a SHARED lock
	releaseLock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  data.sharedByte,
		Len:    1,
	}
	syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &releaseLock)

	return nil
}

// releaseExclusiveLock releases the EXCLUSIVE lock.
func (lm *LockManager) releaseExclusiveLock() error {
	// Release the exclusive lock on the SHARED range
	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  sharedFirst,
		Len:    sharedSize,
	}

	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlSetLk(), &lock); err != nil {
		return fmt.Errorf("failed to release exclusive lock: %w", err)
	}

	return nil
}

// CheckReservedLock checks if any other process holds a RESERVED lock.
// This is used to detect lock conflicts.
func (lm *LockManager) CheckReservedLock() (bool, error) {
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  reservedByte,
		Len:    1,
	}

	// F_GETLK/F_OFD_GETLK checks if a lock would succeed without actually acquiring it
	if err := syscall.FcntlFlock(lm.file.Fd(), lm.fcntlGetLk(), &lock); err != nil {
		return false, fmt.Errorf("failed to check reserved lock: %w", err)
	}

	// If lock.Type is F_UNLCK, no conflicting lock exists
	return lock.Type != syscall.F_UNLCK, nil
}
