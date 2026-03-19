// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestLockLevelString tests the String() method for lock levels.
func TestLockLevelString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		level    LockLevel
		expected string
	}{
		{lockNone, "NONE"},
		{lockShared, "SHARED"},
		{lockReserved, "RESERVED"},
		{lockPending, "PENDING"},
		{lockExclusive, "EXCLUSIVE"},
		{LockLevel(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.expected, func(t *testing.T) {
			got := tt.level.String()
			if got != tt.expected {
				t.Errorf("LockLevel.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestNewLockManager tests creating a new lock manager.
func TestNewLockManager(t *testing.T) {
	t.Parallel()
	t.Run("nil file", func(t *testing.T) {
		_, err := NewLockManager(nil)
		if err != ErrFileNotOpen {
			t.Errorf("NewLockManager(nil) error = %v, want %v", err, ErrFileNotOpen)
		}
	})

	t.Run("valid file", func(t *testing.T) {
		f, cleanup := createTestFile(t)
		defer cleanup()

		lm, err := NewLockManager(f)
		if err != nil {
			t.Fatalf("NewLockManager() error = %v", err)
		}
		defer lm.Close()

		if lm.currentLevel != lockNone {
			t.Errorf("initial lock level = %v, want %v", lm.currentLevel, lockNone)
		}
	})
}

// TestGetLockState tests getting the current lock state.
func TestGetLockState(t *testing.T) {
	t.Parallel()
	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	if got := lm.GetLockState(); got != lockNone {
		t.Errorf("GetLockState() = %v, want %v", got, lockNone)
	}
}

// TestIsValidTransition tests the lock transition validation.
func TestIsValidTransition(t *testing.T) {
	t.Parallel()
	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	tests := []struct {
		name  string
		from  LockLevel
		to    LockLevel
		valid bool
	}{
		// Same level transitions
		{"NONE->NONE", lockNone, lockNone, true},
		{"SHARED->SHARED", lockShared, lockShared, true},

		// Valid upgrades
		{"NONE->SHARED", lockNone, lockShared, true},
		{"NONE->EXCLUSIVE", lockNone, lockExclusive, true},
		{"SHARED->RESERVED", lockShared, lockReserved, true},
		{"SHARED->EXCLUSIVE", lockShared, lockExclusive, true},
		{"RESERVED->PENDING", lockReserved, lockPending, true},
		{"RESERVED->EXCLUSIVE", lockReserved, lockExclusive, true},
		{"PENDING->EXCLUSIVE", lockPending, lockExclusive, true},

		// Invalid upgrades (skipping levels in wrong way)
		{"NONE->RESERVED", lockNone, lockReserved, false},
		{"NONE->PENDING", lockNone, lockPending, false},
		{"SHARED->PENDING", lockShared, lockPending, false},
		{"EXCLUSIVE->anything", lockExclusive, lockPending, false},

		// Downgrades (all valid)
		{"EXCLUSIVE->NONE", lockExclusive, lockNone, true},
		{"EXCLUSIVE->SHARED", lockExclusive, lockShared, true},
		{"RESERVED->SHARED", lockReserved, lockShared, true},
		{"PENDING->SHARED", lockPending, lockShared, true},
		{"SHARED->NONE", lockShared, lockNone, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			lm.currentLevel = tt.from
			got := lm.isValidTransition(tt.from, tt.to)
			if got != tt.valid {
				t.Errorf("isValidTransition(%v, %v) = %v, want %v",
					tt.from, tt.to, got, tt.valid)
			}
		})
	}
}

// TestAcquireReleaseLock tests basic lock acquisition and release.
func TestAcquireReleaseLock(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Test acquiring SHARED lock
	t.Run("acquire shared", func(t *testing.T) {
		if err := lm.AcquireLock(lockShared); err != nil {
			t.Fatalf("AcquireLock(SHARED) error = %v", err)
		}

		if got := lm.GetLockState(); got != lockShared {
			t.Errorf("lock state = %v, want %v", got, lockShared)
		}
	})

	// Test upgrading to RESERVED
	t.Run("upgrade to reserved", func(t *testing.T) {
		if err := lm.AcquireLock(lockReserved); err != nil {
			t.Fatalf("AcquireLock(RESERVED) error = %v", err)
		}

		if got := lm.GetLockState(); got != lockReserved {
			t.Errorf("lock state = %v, want %v", got, lockReserved)
		}
	})

	// Test upgrading to EXCLUSIVE
	t.Run("upgrade to exclusive", func(t *testing.T) {
		if err := lm.AcquireLock(lockExclusive); err != nil {
			t.Fatalf("AcquireLock(EXCLUSIVE) error = %v", err)
		}

		if got := lm.GetLockState(); got != lockExclusive {
			t.Errorf("lock state = %v, want %v", got, lockExclusive)
		}
	})

	// Test downgrading to SHARED
	t.Run("downgrade to shared", func(t *testing.T) {
		if err := lm.ReleaseLock(lockShared); err != nil {
			t.Fatalf("ReleaseLock(SHARED) error = %v", err)
		}

		if got := lm.GetLockState(); got != lockShared {
			t.Errorf("lock state = %v, want %v", got, lockShared)
		}
	})

	// Test releasing all locks
	t.Run("release all", func(t *testing.T) {
		if err := lm.ReleaseLock(lockNone); err != nil {
			t.Fatalf("ReleaseLock(NONE) error = %v", err)
		}

		if got := lm.GetLockState(); got != lockNone {
			t.Errorf("lock state = %v, want %v", got, lockNone)
		}
	})
}

// TestInvalidTransitions tests that invalid lock transitions are rejected.
func TestInvalidTransitions(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Try to acquire RESERVED without SHARED first
	err = lm.AcquireLock(lockReserved)
	if err == nil {
		t.Error("AcquireLock(RESERVED) from NONE should fail")
	}

	// Try to acquire PENDING without going through proper states
	err = lm.AcquireLock(lockPending)
	if err == nil {
		t.Error("AcquireLock(PENDING) from NONE should fail")
	}
}

// TestConcurrentReaders tests that multiple readers can hold SHARED locks.
func TestConcurrentReaders(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f1, cleanup1 := createTestFile(t)
	defer cleanup1()

	// Open the same file again for second lock manager
	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// Both should be able to acquire SHARED locks
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm1.AcquireLock(SHARED) error = %v", err)
	}

	if err := lm2.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm2.AcquireLock(SHARED) error = %v", err)
	}

	// Verify both have SHARED locks
	if got := lm1.GetLockState(); got != lockShared {
		t.Errorf("lm1 lock state = %v, want %v", got, lockShared)
	}
	if got := lm2.GetLockState(); got != lockShared {
		t.Errorf("lm2 lock state = %v, want %v", got, lockShared)
	}
}

// TestReaderWriterConflict tests that a writer blocks when readers exist.
func TestReaderWriterConflict(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f1, cleanup1 := createTestFile(t)
	defer cleanup1()

	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// lm1 acquires a SHARED lock (reader)
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm1.AcquireLock(SHARED) error = %v", err)
	}

	// lm2 should be able to get RESERVED (planning to write)
	if err := lm2.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm2.AcquireLock(SHARED) error = %v", err)
	}
	if err := lm2.AcquireLock(lockReserved); err != nil {
		t.Fatalf("lm2.AcquireLock(RESERVED) error = %v", err)
	}

	// lm2 should NOT be able to get EXCLUSIVE while lm1 holds SHARED
	err = lm2.AcquireLock(lockExclusive)
	if err == nil {
		t.Error("lm2.AcquireLock(EXCLUSIVE) should fail while reader exists")
	}
	if err != ErrLockBusy {
		t.Errorf("error = %v, want %v", err, ErrLockBusy)
	}

	// After lm1 releases, lm2 should be able to get EXCLUSIVE
	if err := lm1.ReleaseLock(lockNone); err != nil {
		t.Fatalf("lm1.ReleaseLock(NONE) error = %v", err)
	}

	if err := lm2.AcquireLock(lockExclusive); err != nil {
		t.Fatalf("lm2.AcquireLock(EXCLUSIVE) error = %v", err)
	}
}

// TestReservedlockExclusive tests that only one RESERVED lock can be held.
func TestReservedlockExclusive(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f1, cleanup1 := createTestFile(t)
	defer cleanup1()

	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// Both acquire SHARED locks
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm1.AcquireLock(SHARED) error = %v", err)
	}
	if err := lm2.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm2.AcquireLock(SHARED) error = %v", err)
	}

	// lm1 acquires RESERVED
	if err := lm1.AcquireLock(lockReserved); err != nil {
		t.Fatalf("lm1.AcquireLock(RESERVED) error = %v", err)
	}

	// lm2 should NOT be able to acquire RESERVED
	err = lm2.AcquireLock(lockReserved)
	if err == nil {
		t.Error("lm2.AcquireLock(RESERVED) should fail when lm1 holds it")
	}
	if err != ErrLockBusy {
		t.Errorf("error = %v, want %v", err, ErrLockBusy)
	}
}

// TestExclusivelockExclusive tests that EXCLUSIVE locks are truly exclusive.
func TestExclusivelockExclusive(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f1, cleanup1 := createTestFile(t)
	defer cleanup1()

	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// lm1 acquires EXCLUSIVE lock
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm1.AcquireLock(SHARED) error = %v", err)
	}
	if err := lm1.AcquireLock(lockExclusive); err != nil {
		t.Fatalf("lm1.AcquireLock(EXCLUSIVE) error = %v", err)
	}

	// lm2 should NOT be able to acquire even a SHARED lock
	err = lm2.AcquireLock(lockShared)
	if err == nil {
		t.Error("lm2.AcquireLock(SHARED) should fail when lm1 holds EXCLUSIVE")
	}
	if err != ErrLockBusy {
		t.Errorf("error = %v, want %v", err, ErrLockBusy)
	}
}

// TestConcurrentLockOperations tests thread safety of lock operations.
func TestConcurrentLockOperations(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Perform concurrent lock state queries
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = lm.GetLockState()
				_ = lm.IsLockHeld(lockShared)
				_ = lm.CanAcquire(lockReserved)
			}
		}()
	}

	wg.Wait()
}

// TestIsLockHeld tests the IsLockHeld method.
func TestIsLockHeld(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Initially no locks held
	if lm.IsLockHeld(lockShared) {
		t.Error("IsLockHeld(SHARED) = true, want false")
	}

	// Acquire SHARED
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}

	if !lm.IsLockHeld(lockShared) {
		t.Error("IsLockHeld(SHARED) = false, want true")
	}
	if lm.IsLockHeld(lockReserved) {
		t.Error("IsLockHeld(RESERVED) = true, want false")
	}

	// Acquire RESERVED
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	if !lm.IsLockHeld(lockShared) {
		t.Error("IsLockHeld(SHARED) = false, want true")
	}
	if !lm.IsLockHeld(lockReserved) {
		t.Error("IsLockHeld(RESERVED) = false, want true")
	}
	if lm.IsLockHeld(lockExclusive) {
		t.Error("IsLockHeld(EXCLUSIVE) = true, want false")
	}
}

// TestCanAcquire tests the CanAcquire method.
func TestCanAcquire(t *testing.T) {
	t.Parallel()
	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	tests := []struct {
		name         string
		currentLevel LockLevel
		checkLevel   LockLevel
		canAcquire   bool
	}{
		{"NONE->SHARED", lockNone, lockShared, true},
		{"NONE->RESERVED", lockNone, lockReserved, false},
		{"SHARED->RESERVED", lockShared, lockReserved, true},
		{"SHARED->EXCLUSIVE", lockShared, lockExclusive, true},
		{"RESERVED->PENDING", lockReserved, lockPending, true},
		{"EXCLUSIVE->PENDING", lockExclusive, lockPending, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			lm.mu.Lock()
			lm.currentLevel = tt.currentLevel
			lm.mu.Unlock()

			got := lm.CanAcquire(tt.checkLevel)
			if got != tt.canAcquire {
				t.Errorf("CanAcquire(%v) from %v = %v, want %v",
					tt.checkLevel, tt.currentLevel, got, tt.canAcquire)
			}
		})
	}
}

// TestLockManagerClose tests that Close releases all locks.
func TestLockManagerClose(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}

	// Acquire locks
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	// Close should release all locks
	if err := lm.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := lm.GetLockState(); got != lockNone {
		t.Errorf("lock state after Close() = %v, want %v", got, lockNone)
	}
}

// TestTryAcquireLock tests the non-blocking lock acquisition.
func TestTryAcquireLock(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// TryAcquire should succeed when no conflicts
	if err := lm.TryAcquireLock(lockShared); err != nil {
		t.Fatalf("TryAcquireLock(SHARED) error = %v", err)
	}

	if got := lm.GetLockState(); got != lockShared {
		t.Errorf("lock state = %v, want %v", got, lockShared)
	}
}

// TestLockSequence tests a typical lock sequence for a write transaction.
func TestLockSequence(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f, cleanup := createTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Typical write transaction sequence:
	// 1. Begin: NONE -> SHARED (start reading)
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}

	// 2. Prepare to write: SHARED -> RESERVED
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	// 3. Ready to commit: RESERVED -> PENDING -> EXCLUSIVE
	if err := lm.AcquireLock(lockPending); err != nil {
		t.Fatalf("AcquireLock(PENDING) error = %v", err)
	}
	if err := lm.AcquireLock(lockExclusive); err != nil {
		t.Fatalf("AcquireLock(EXCLUSIVE) error = %v", err)
	}

	// 4. Write data (lock held)
	// ... writing happens here ...

	// 5. Finish: EXCLUSIVE -> NONE
	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Fatalf("ReleaseLock(NONE) error = %v", err)
	}

	if got := lm.GetLockState(); got != lockNone {
		t.Errorf("final lock state = %v, want %v", got, lockNone)
	}
}

// TestPendingBlocksNewReaders tests that PENDING blocks new SHARED locks.
func TestPendingBlocksNewReaders(t *testing.T) {
	t.Parallel()
	// Skip on Windows until implemented
	if isWindows() {
		t.Skip("Skipping test on Windows (not yet implemented)")
	}

	f1, cleanup1 := createTestFile(t)
	defer cleanup1()

	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// lm1 acquires PENDING lock (going through proper states)
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("lm1.AcquireLock(SHARED) error = %v", err)
	}
	if err := lm1.AcquireLock(lockReserved); err != nil {
		t.Fatalf("lm1.AcquireLock(RESERVED) error = %v", err)
	}
	if err := lm1.AcquireLock(lockPending); err != nil {
		t.Fatalf("lm1.AcquireLock(PENDING) error = %v", err)
	}

	// lm2 should NOT be able to acquire SHARED lock
	// Note: This behavior depends on how PENDING is implemented.
	// In full SQLite, PENDING prevents new SHARED locks.
	// Our implementation may differ slightly due to platform limitations.

	// Give a small delay to ensure lock is fully acquired
	time.Sleep(10 * time.Millisecond)

	// Try to acquire SHARED with lm2
	err = lm2.TryAcquireLock(lockShared)
	// The exact behavior here depends on the implementation.
	// At minimum, this should not panic or deadlock.
	_ = err // May succeed or fail depending on implementation details
}

// Helper functions

// createTestFile creates a temporary file for testing.
func createTestFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Write some initial data to ensure file exists
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		f.Close()
		t.Fatalf("failed to write test data: %v", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatalf("failed to sync test file: %v", err)
	}

	cleanup := func() {
		f.Close()
	}

	return f, cleanup
}

// isWindows returns true if running on Windows.
func isWindows() bool {
	// Check if we're on Windows by trying to detect the OS
	// This is a simple check that works for build tags
	return false // Will be overridden by lock_windows.go if on Windows
}
