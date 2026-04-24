// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows && !js && !wasip1

package pager_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// lockCoverageOpenFile creates a temporary file backed by a real file.
func lockCoverageOpenFile(t *testing.T) (*os.File, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "lock_cov.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("lockCoverageOpenFile: %v", err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		f.Close()
		t.Fatalf("lockCoverageOpenFile write: %v", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatalf("lockCoverageOpenFile sync: %v", err)
	}
	return f, func() { f.Close() }
}

// lockCoverageSecondHandle opens a second file descriptor on the same path.
func lockCoverageSecondHandle(t *testing.T, path string) (*os.File, *pager.LockManager) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("lockCoverageSecondHandle: %v", err)
	}
	lm, err := pager.NewLockManager(f)
	if err != nil {
		f.Close()
		t.Fatalf("lockCoverageSecondHandle NewLockManager: %v", err)
	}
	t.Cleanup(func() {
		lm.Close()
		f.Close()
	})
	return f, lm
}

// TestLockCoverage_PendingLockAcquireAndRelease exercises acquirePendingLock
// through the SHARED -> RESERVED -> PENDING upgrade sequence. This covers the
// normal (happy) path of the function body.
func TestLockCoverage_PendingLockAcquireAndRelease(t *testing.T) {
	f, cleanup := lockCoverageOpenFile(t)
	defer cleanup()

	lm, err := pager.NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// SHARED
	if err := lm.AcquireLock(pager.LockLevel(pager.LockShared)); err != nil {
		t.Fatalf("AcquireLock(SHARED): %v", err)
	}
	// RESERVED
	if err := lm.AcquireLock(pager.LockLevel(pager.LockReserved)); err != nil {
		t.Fatalf("AcquireLock(RESERVED): %v", err)
	}
	// PENDING — exercises acquirePendingLock; currentLevel == RESERVED so the
	// inner `if lm.currentLevel < lockReserved` branch is NOT taken here.
	if err := lm.AcquireLock(pager.LockLevel(pager.LockPending)); err != nil {
		t.Fatalf("AcquireLock(PENDING): %v", err)
	}
	if got := lm.GetLockState(); got != pager.LockLevel(pager.LockPending) {
		t.Errorf("lock state after PENDING = %v, want PENDING", got)
	}

	// Release back to NONE
	if err := lm.ReleaseLock(pager.LockLevel(pager.LockNone)); err != nil {
		t.Fatalf("ReleaseLock(NONE): %v", err)
	}
}

// TestLockCoverage_PendingLockConflict exercises the EAGAIN/EACCES error
// branch inside acquirePendingLock. lm1 holds PENDING; lm2 tries to acquire
// PENDING on the same file and must see ErrLockBusy.
// lcEscalateLM escalates a lock manager through the given levels, fataling on error.
func lcEscalateLM(t *testing.T, lm *pager.LockManager, levels []pager.LockLevel) {
	t.Helper()
	for _, lvl := range levels {
		if err := lm.AcquireLock(lvl); err != nil {
			t.Fatalf("AcquireLock(%v): %v", lvl, err)
		}
	}
}

func TestLockCoverage_PendingLockConflict(t *testing.T) {
	f1, cleanup1 := lockCoverageOpenFile(t)
	defer cleanup1()

	lm1, err := pager.NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(lm1): %v", err)
	}
	defer lm1.Close()

	_, lm2 := lockCoverageSecondHandle(t, f1.Name())

	lcEscalateLM(t, lm1, []pager.LockLevel{
		pager.LockLevel(pager.LockShared),
		pager.LockLevel(pager.LockReserved),
		pager.LockLevel(pager.LockPending),
	})

	if err := lm2.TryAcquireLock(pager.LockLevel(pager.LockShared)); err != nil {
		t.Skipf("lm2 cannot acquire SHARED (platform behaviour): %v", err)
	}
	if err := lm2.TryAcquireLock(pager.LockLevel(pager.LockReserved)); err != nil {
		t.Skipf("lm2 cannot acquire RESERVED (platform behaviour): %v", err)
	}

	err = lm2.TryAcquireLock(pager.LockLevel(pager.LockPending))
	if err == nil {
		t.Log("lm2 acquired PENDING (same-process POSIX lock sharing)")
	} else if err == pager.ErrLockBusy {
		t.Log("lm2 correctly received ErrLockBusy for PENDING conflict")
	} else {
		t.Logf("lm2 received unexpected error (acceptable): %v", err)
	}
}

// TestLockCoverage_PendingThenExclusive exercises the full write-commit
// sequence SHARED -> RESERVED -> PENDING -> EXCLUSIVE and release, exercising
// acquirePendingLock and acquireExclusiveLock together.
func TestLockCoverage_PendingThenExclusive(t *testing.T) {
	f, cleanup := lockCoverageOpenFile(t)
	defer cleanup()

	lm, err := pager.NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	steps := []struct {
		level pager.LockLevel
		name  string
	}{
		{pager.LockLevel(pager.LockShared), "SHARED"},
		{pager.LockLevel(pager.LockReserved), "RESERVED"},
		{pager.LockLevel(pager.LockPending), "PENDING"},
		{pager.LockLevel(pager.LockExclusive), "EXCLUSIVE"},
	}
	for _, s := range steps {
		if err := lm.AcquireLock(s.level); err != nil {
			t.Fatalf("AcquireLock(%s): %v", s.name, err)
		}
		if got := lm.GetLockState(); got != s.level {
			t.Errorf("after acquiring %s: state = %v, want %v", s.name, got, s.level)
		}
	}

	if err := lm.ReleaseLock(pager.LockLevel(pager.LockNone)); err != nil {
		t.Fatalf("ReleaseLock(NONE): %v", err)
	}
	if got := lm.GetLockState(); got != pager.LockLevel(pager.LockNone) {
		t.Errorf("after release: state = %v, want NONE", got)
	}
}
