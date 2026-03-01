// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
//go:build windows

package pager

import (
	"fmt"
)

// Platform-specific lock implementation for Windows systems.
// This is a stub implementation that will be fully implemented later.
//
// Windows uses LockFileEx/UnlockFileEx for byte-range locking, similar to Unix
// but with different APIs. The same byte ranges and locking strategy apply:
//
//   Byte Range         Lock Type    Lock Name
//   -----------        ---------    ---------
//   PENDING_BYTE       Exclusive    PENDING lock
//   RESERVED_BYTE      Exclusive    RESERVED lock
//   SHARED_FIRST to    Shared       SHARED lock
//   SHARED_FIRST+510
//
// TODO: Implement full Windows locking using syscall.LockFileEx

const (
	// Lock byte offsets (matching SQLite's implementation)
	pendingByte  = 0x40000000      // 1GB mark - PENDING lock byte
	reservedByte = pendingByte + 1 // RESERVED lock byte
	sharedFirst  = pendingByte + 2 // Start of SHARED lock range
	sharedSize   = 510             // Number of bytes in SHARED lock range
)

// windowsLockData holds Windows-specific locking information.
type windowsLockData struct {
	// Which shared byte we're using
	sharedByte int64
}

// initPlatform initializes platform-specific lock data.
func (lm *LockManager) initPlatform() error {
	// Use a simple hash of the file handle to pick a shared byte
	// Windows file handles are pointers, so we use the address
	sharedOffset := (uintptr(lm.file.Fd()) % sharedSize)

	lm.platformData = &windowsLockData{
		sharedByte: sharedFirst + int64(sharedOffset),
	}

	return nil
}

// cleanupPlatform cleans up platform-specific resources.
func (lm *LockManager) cleanupPlatform() error {
	// Nothing special to clean up on Windows
	return nil
}

// acquireLockPlatform performs the platform-specific lock acquisition.
func (lm *LockManager) acquireLockPlatform(level LockLevel) error {
	switch level {
	case lockNone:
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

// releaseLockPlatform performs the platform-specific lock release.
func (lm *LockManager) releaseLockPlatform(level LockLevel) error {
	// Release locks in reverse order of acquisition
	if lm.shouldReleaseLock(lockExclusive, level, lockExclusive) {
		if err := lm.releaseExclusiveLock(); err != nil {
			return err
		}
	}

	if lm.shouldReleaseLock(lockPending, level, lockPending) {
		if err := lm.releasePendingLock(); err != nil {
			return err
		}
	}

	if lm.shouldReleaseLock(lockReserved, level, lockReserved) {
		if err := lm.releaseReservedLock(); err != nil {
			return err
		}
	}

	if lm.shouldReleaseLock(lockShared, level, lockShared) {
		if err := lm.releaseSharedLock(); err != nil {
			return err
		}
	}

	return nil
}

// Stub implementations for Windows
// TODO: Replace these with actual LockFileEx/UnlockFileEx calls

func (lm *LockManager) acquireSharedLock() error {
	// TODO: Implement using LockFileEx with LOCKFILE_FAIL_IMMEDIATELY
	// and no LOCKFILE_EXCLUSIVE_LOCK flag for shared access
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) releaseSharedLock() error {
	// TODO: Implement using UnlockFileEx
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) acquireReservedLock() error {
	// TODO: Implement using LockFileEx with LOCKFILE_EXCLUSIVE_LOCK flag
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) releaseReservedLock() error {
	// TODO: Implement using UnlockFileEx
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) acquirePendingLock() error {
	// TODO: Implement using LockFileEx with LOCKFILE_EXCLUSIVE_LOCK flag
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) releasePendingLock() error {
	// TODO: Implement using UnlockFileEx
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) acquireExclusiveLock() error {
	// TODO: Implement using LockFileEx with LOCKFILE_EXCLUSIVE_LOCK flag
	// Must first acquire PENDING, then lock entire SHARED range
	return fmt.Errorf("Windows file locking not yet implemented")
}

func (lm *LockManager) releaseExclusiveLock() error {
	// TODO: Implement using UnlockFileEx
	return fmt.Errorf("Windows file locking not yet implemented")
}

// CheckReservedLock checks if any other process holds a RESERVED lock.
func (lm *LockManager) CheckReservedLock() (bool, error) {
	// TODO: Implement by attempting to acquire a read lock on the reserved byte
	return false, fmt.Errorf("Windows file locking not yet implemented")
}
