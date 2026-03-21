// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build windows

package pager

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Platform-specific lock implementation for Windows systems.
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

const (
	// Lock byte offsets (matching SQLite's implementation)
	pendingByte  = 0x40000000      // 1GB mark - PENDING lock byte
	reservedByte = pendingByte + 1 // RESERVED lock byte
	sharedFirst  = pendingByte + 2 // Start of SHARED lock range
	sharedSize   = 510             // Number of bytes in SHARED lock range
)

// Windows API constants for file locking
const (
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002

	// ERROR_LOCK_VIOLATION is Windows error code 33 (0x21).
	// Go's stdlib syscall package does not define this constant.
	ERROR_LOCK_VIOLATION = syscall.Errno(0x21)
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
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

// lockFileEx wraps the Windows LockFileEx API.
// unsafe.Pointer is required to pass the OVERLAPPED struct to the Windows
// syscall interface; there is no safe alternative for raw Win32 API calls.
func lockFileEx(handle syscall.Handle, flags uint32, offsetLow uint32, offsetHigh uint32, nBytes uint32) error {
	var overlapped syscall.Overlapped
	overlapped.Offset = offsetLow
	overlapped.OffsetHigh = offsetHigh

	r1, _, err := procLockFileEx.Call(
		uintptr(handle),
		uintptr(flags),
		uintptr(0), // Reserved
		uintptr(nBytes),
		uintptr(0), // nNumberOfBytesToLockHigh
		uintptr(unsafe.Pointer(&overlapped)),
	)

	if r1 == 0 {
		return err
	}
	return nil
}

// unlockFileEx wraps the Windows UnlockFileEx API.
func unlockFileEx(handle syscall.Handle, offsetLow uint32, offsetHigh uint32, nBytes uint32) error {
	var overlapped syscall.Overlapped
	overlapped.Offset = offsetLow
	overlapped.OffsetHigh = offsetHigh

	r1, _, err := procUnlockFileEx.Call(
		uintptr(handle),
		uintptr(0), // Reserved
		uintptr(nBytes),
		uintptr(0), // nNumberOfBytesToUnlockHigh
		uintptr(unsafe.Pointer(&overlapped)),
	)

	if r1 == 0 {
		return err
	}
	return nil
}

func (lm *LockManager) acquireSharedLock() error {
	data := lm.platformData.(*windowsLockData)

	// Acquire a shared (read) lock on one byte in the SHARED range
	// Use LOCKFILE_FAIL_IMMEDIATELY for non-blocking
	flags := uint32(LOCKFILE_FAIL_IMMEDIATELY)

	err := lockFileEx(
		syscall.Handle(lm.file.Fd()),
		flags,
		uint32(data.sharedByte),
		0, // high 32 bits of offset
		1, // lock 1 byte
	)

	if err != nil {
		if err == ERROR_LOCK_VIOLATION {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire shared lock: %w", err)
	}

	return nil
}

func (lm *LockManager) releaseSharedLock() error {
	data := lm.platformData.(*windowsLockData)

	err := unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(data.sharedByte),
		0, // high 32 bits of offset
		1, // unlock 1 byte
	)

	if err != nil {
		return fmt.Errorf("failed to release shared lock: %w", err)
	}

	return nil
}

func (lm *LockManager) acquireReservedLock() error {
	// RESERVED lock is an exclusive lock on the RESERVED byte
	flags := uint32(LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK)

	err := lockFileEx(
		syscall.Handle(lm.file.Fd()),
		flags,
		uint32(reservedByte),
		0, // high 32 bits of offset
		1, // lock 1 byte
	)

	if err != nil {
		if err == ERROR_LOCK_VIOLATION {
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

func (lm *LockManager) releaseReservedLock() error {
	err := unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(reservedByte),
		0, // high 32 bits of offset
		1, // unlock 1 byte
	)

	if err != nil {
		return fmt.Errorf("failed to release reserved lock: %w", err)
	}

	return nil
}

func (lm *LockManager) acquirePendingLock() error {
	// PENDING lock is an exclusive lock on the PENDING byte
	flags := uint32(LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK)

	err := lockFileEx(
		syscall.Handle(lm.file.Fd()),
		flags,
		uint32(pendingByte),
		0, // high 32 bits of offset
		1, // lock 1 byte
	)

	if err != nil {
		if err == ERROR_LOCK_VIOLATION {
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

func (lm *LockManager) releasePendingLock() error {
	err := unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(pendingByte),
		0, // high 32 bits of offset
		1, // unlock 1 byte
	)

	if err != nil {
		return fmt.Errorf("failed to release pending lock: %w", err)
	}

	return nil
}

func (lm *LockManager) acquireExclusiveLock() error {
	data := lm.platformData.(*windowsLockData)

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
	flags := uint32(LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK)

	err := lockFileEx(
		syscall.Handle(lm.file.Fd()),
		flags,
		uint32(sharedFirst),
		0,          // high 32 bits of offset
		sharedSize, // lock entire shared range
	)

	if err != nil {
		if lm.currentLevel < lockPending {
			lm.releasePendingLock()
		}
		if err == ERROR_LOCK_VIOLATION {
			return ErrLockBusy
		}
		return fmt.Errorf("failed to acquire exclusive lock: %w", err)
	}

	// Release our individual SHARED lock since we now have exclusive access
	// We ignore errors here because we might not have had a SHARED lock
	unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(data.sharedByte),
		0, // high 32 bits of offset
		1, // unlock 1 byte
	)

	return nil
}

func (lm *LockManager) releaseExclusiveLock() error {
	// Release the exclusive lock on the SHARED range
	err := unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(sharedFirst),
		0,          // high 32 bits of offset
		sharedSize, // unlock entire shared range
	)

	if err != nil {
		return fmt.Errorf("failed to release exclusive lock: %w", err)
	}

	return nil
}

// CheckReservedLock checks if any other process holds a RESERVED lock.
// This is used to detect lock conflicts.
func (lm *LockManager) CheckReservedLock() (bool, error) {
	// Try to acquire a shared lock on the reserved byte
	// If it fails, someone else has an exclusive lock (RESERVED)
	flags := uint32(LOCKFILE_FAIL_IMMEDIATELY)

	err := lockFileEx(
		syscall.Handle(lm.file.Fd()),
		flags,
		uint32(reservedByte),
		0, // high 32 bits of offset
		1, // lock 1 byte
	)

	if err != nil {
		if err == ERROR_LOCK_VIOLATION {
			// Someone else holds the reserved lock
			return true, nil
		}
		return false, fmt.Errorf("failed to check reserved lock: %w", err)
	}

	// We got the lock, so release it immediately
	unlockErr := unlockFileEx(
		syscall.Handle(lm.file.Fd()),
		uint32(reservedByte),
		0, // high 32 bits of offset
		1, // unlock 1 byte
	)

	if unlockErr != nil {
		return false, fmt.Errorf("failed to release test lock: %w", unlockErr)
	}

	// No one holds the reserved lock
	return false, nil
}
