// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"errors"
	"testing"
)

// newBusyLockTestPager returns a minimal Pager suitable for busy lock retry tests.
// It has no open file; it is only valid for exercising the lock retry logic.
func newBusyLockTestPager(t *testing.T) *Pager {
	t.Helper()
	p := newPager("busylocktest.db", DefaultPageSize, false)
	t.Cleanup(func() {
		if p.file != nil {
			p.Close()
		}
	})
	return p
}

// TestBusyLockShared_SuccessFirstTry verifies the fast path: lock acquired immediately.
func TestBusyLockShared_SuccessFirstTry(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)

	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p.lockState < LockShared {
		t.Errorf("lockState = %v, want >= LockShared", p.lockState)
	}
}

// TestBusyLockShared_AlreadyHeld verifies that holding a shared lock returns immediately.
func TestBusyLockShared_AlreadyHeld(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockShared

	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Fatalf("expected success when lock already held, got %v", err)
	}
}

// TestBusyLockShared_NilHandlerFailsImmediately verifies that with no busy handler,
// a lock failure returns ErrDatabaseLocked without retrying.
func TestBusyLockShared_NilHandlerFailsImmediately(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockTestFailsRemaining = 1 // force one failure

	err := p.acquireSharedLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
	// lock should still be unset
	if p.lockState >= LockShared {
		t.Errorf("lockState = %v, want < LockShared", p.lockState)
	}
}

// TestBusyLockShared_HandlerReturnsFalse verifies that when the busy handler returns
// false on the first invocation, the call returns ErrDatabaseLocked immediately.
func TestBusyLockShared_HandlerReturnsFalse(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockTestFailsRemaining = 2

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		callCount++
		return false
	})

	err := p.acquireSharedLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("busy handler called %d times, want 1", callCount)
	}
}

// TestBusyLockShared_HandlerReturnsTrueThenSucceeds verifies the retry loop:
// the handler returns true N times, allowing retries, and eventually the lock succeeds.
func TestBusyLockShared_HandlerReturnsTrueThenSucceeds(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)

	const failures = 3
	p.lockTestFailsRemaining = failures

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		if count != callCount {
			// count is the retry number passed by the caller
		}
		callCount++
		return true // always retry
	})

	err := p.acquireSharedLockWithRetry()
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if callCount != failures {
		t.Errorf("busy handler called %d times, want %d", callCount, failures)
	}
	if p.lockState < LockShared {
		t.Errorf("lockState = %v after success, want >= LockShared", p.lockState)
	}
}

// TestBusyLockShared_HandlerReceivesMonotonicCount verifies the retry count
// passed to the busy handler is monotonically increasing.
func TestBusyLockShared_HandlerReceivesMonotonicCount(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockTestFailsRemaining = 4

	counts := []int{}
	p.busyHandler = BusyCallback(func(count int) bool {
		counts = append(counts, count)
		return true
	})

	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, c := range counts {
		if c != i {
			t.Errorf("counts[%d] = %d, want %d", i, c, i)
		}
	}
}

// TestBusyLockReserved_SuccessFirstTry verifies the fast path for reserved lock.
func TestBusyLockReserved_SuccessFirstTry(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockShared // prerequisite

	if err := p.acquireReservedLockWithRetry(); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p.lockState < LockReserved {
		t.Errorf("lockState = %v, want >= LockReserved", p.lockState)
	}
}

// TestBusyLockReserved_AlreadyHeld verifies immediate return when reserved lock is held.
func TestBusyLockReserved_AlreadyHeld(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved

	if err := p.acquireReservedLockWithRetry(); err != nil {
		t.Fatalf("expected success when lock already held, got %v", err)
	}
}

// TestBusyLockReserved_NonBusyErrorReturnsImmediately verifies that a non-BUSY error
// (ErrReadOnly) is returned immediately without invoking the busy handler.
func TestBusyLockReserved_NonBusyErrorReturnsImmediately(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.readOnly = true

	handlerCalled := false
	p.busyHandler = BusyCallback(func(count int) bool {
		handlerCalled = true
		return true
	})

	err := p.acquireReservedLockWithRetry()
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	if handlerCalled {
		t.Error("busy handler must not be called for non-BUSY errors")
	}
}

// TestBusyLockReserved_NilHandlerFailsImmediately verifies that with no busy handler,
// a lock failure returns ErrDatabaseLocked.
func TestBusyLockReserved_NilHandlerFailsImmediately(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockShared
	p.lockTestFailsRemaining = 1

	err := p.acquireReservedLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
}

// TestBusyLockReserved_HandlerReturnsFalse verifies abort on handler returning false.
func TestBusyLockReserved_HandlerReturnsFalse(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockShared
	p.lockTestFailsRemaining = 5

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		callCount++
		return false
	})

	err := p.acquireReservedLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("handler called %d times, want 1", callCount)
	}
}

// TestBusyLockReserved_HandlerReturnsTrueThenSucceeds exercises the retry loop
// for reserved lock acquisition.
func TestBusyLockReserved_HandlerReturnsTrueThenSucceeds(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockShared

	const failures = 2
	p.lockTestFailsRemaining = failures

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		callCount++
		return true
	})

	if err := p.acquireReservedLockWithRetry(); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if callCount != failures {
		t.Errorf("handler called %d times, want %d", callCount, failures)
	}
	if p.lockState < LockReserved {
		t.Errorf("lockState = %v after success, want >= LockReserved", p.lockState)
	}
}

// TestBusyLockExclusive_SuccessFirstTry verifies the fast path for exclusive lock.
func TestBusyLockExclusive_SuccessFirstTry(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved // prerequisite

	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p.lockState < LockExclusive {
		t.Errorf("lockState = %v, want >= LockExclusive", p.lockState)
	}
}

// TestBusyLockExclusive_AlreadyHeld verifies immediate return when exclusive lock is held.
func TestBusyLockExclusive_AlreadyHeld(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockExclusive

	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Fatalf("expected success when lock already held, got %v", err)
	}
}

// TestBusyLockExclusive_NilHandlerFailsImmediately verifies that with no busy handler,
// a lock failure returns ErrDatabaseLocked.
func TestBusyLockExclusive_NilHandlerFailsImmediately(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved
	p.lockTestFailsRemaining = 1

	err := p.acquireExclusiveLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
}

// TestBusyLockExclusive_HandlerReturnsFalse verifies abort on handler returning false.
func TestBusyLockExclusive_HandlerReturnsFalse(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved
	p.lockTestFailsRemaining = 5

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		callCount++
		return false
	})

	err := p.acquireExclusiveLockWithRetry()
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("handler called %d times, want 1", callCount)
	}
}

// TestBusyLockExclusive_HandlerReturnsTrueThenSucceeds exercises the retry loop
// for exclusive lock acquisition.
func TestBusyLockExclusive_HandlerReturnsTrueThenSucceeds(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved

	const failures = 3
	p.lockTestFailsRemaining = failures

	callCount := 0
	p.busyHandler = BusyCallback(func(count int) bool {
		callCount++
		return true
	})

	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if callCount != failures {
		t.Errorf("handler called %d times, want %d", callCount, failures)
	}
	if p.lockState < LockExclusive {
		t.Errorf("lockState = %v after success, want >= LockExclusive", p.lockState)
	}
}

// TestBusyLockExclusive_HandlerReceivesMonotonicCount verifies retry count
// increments correctly for exclusive lock retries.
func TestBusyLockExclusive_HandlerReceivesMonotonicCount(t *testing.T) {
	t.Parallel()
	p := newBusyLockTestPager(t)
	p.lockState = LockReserved
	p.lockTestFailsRemaining = 4

	counts := []int{}
	p.busyHandler = BusyCallback(func(count int) bool {
		counts = append(counts, count)
		return true
	})

	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, c := range counts {
		if c != i {
			t.Errorf("counts[%d] = %d, want %d", i, c, i)
		}
	}
}
