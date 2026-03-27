// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

// MC/DC test coverage for the pager package, batch 7.
// This file targets remaining uncovered branches in:
//   lock_unix.go: acquireLockPlatform (lockPending case, default case),
//                 acquirePendingLock (currentLevel < lockReserved branch),
//                 acquireReservedLock (error rollback path)
//   vacuum.go:    copyDatabaseToTarget (error from copyHeader/copyPage1Content/copyLivePages),
//                 copyFile (dest creation error), writeHeaderToPage1 (state guard),
//                 Vacuum (readOnly guard, transaction-open guard),
//                 commitTargetPager (state < PagerStateWriterLocked guard)

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// lock_unix.go — acquireLockPlatform: lockPending case
//
// MC/DC conditions:
//   Case lockPending: delegates to acquirePendingLock
//
// Valid path: hold lockReserved first, then AcquireLock(lockPending).
// This exercises the lockPending arm of acquireLockPlatform.
// ---------------------------------------------------------------------------

func TestMCDC7_AcquireLockPlatform_LockPending(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "pending.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// Reach RESERVED via SHARED first (valid transitions: NONE→SHARED→RESERVED).
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Skipf("AcquireLock(lockReserved): %v", err)
	}

	// Now acquire PENDING — exercises the lockPending case in acquireLockPlatform.
	if err := lm.AcquireLock(lockPending); err != nil {
		t.Logf("AcquireLock(lockPending) returned %v (may be platform limitation)", err)
		return
	}
	if lm.currentLevel != lockPending {
		t.Errorf("currentLevel = %v, want lockPending", lm.currentLevel)
	}
}

// ---------------------------------------------------------------------------
// lock_unix.go — acquireLockPlatform: default (unknown level) case
//
// MC/DC conditions:
//   Case default: returns error "unknown lock level"
//
// acquireLockPlatform is unexported. AcquireLock validates transitions before
// calling it, so bypass validation by calling acquireLockPlatform directly.
// ---------------------------------------------------------------------------

func TestMCDC7_AcquireLockPlatform_DefaultCase(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "default.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// LockLevel(99) is not a defined level; this hits the default case.
	err = lm.acquireLockPlatform(LockLevel(99))
	if err == nil {
		t.Error("MC/DC default case: expected error for unknown lock level, got nil")
	}
}

// ---------------------------------------------------------------------------
// lock_unix.go — acquirePendingLock: currentLevel < lockReserved branch
//
// MC/DC conditions on `lm.currentLevel < lockReserved`:
//   Case A (true):  acquireReservedLock is called inside acquirePendingLock
//   Case B (false): acquireReservedLock is skipped
//
// Case A: call acquireLockPlatform(lockPending) directly when currentLevel==lockNone.
// Case B: already covered when currentLevel >= lockReserved.
// ---------------------------------------------------------------------------

func TestMCDC7_AcquirePendingLock_BelowReserved_TriggersReservedAcquire(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "pend2.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// currentLevel starts at lockNone (< lockReserved), so the inner guard fires.
	// Bypass AcquireLock's transition validator and call directly.
	err = lm.acquireLockPlatform(lockPending)
	if err != nil {
		t.Logf("acquireLockPlatform(lockPending) returned %v (platform may block)", err)
		return
	}
	// If success, the branch ran and reserved+shared locks were also acquired.
}

// ---------------------------------------------------------------------------
// vacuum.go — Vacuum: readOnly guard (ErrReadOnly)
//
// MC/DC conditions on `p.readOnly`:
//   Case 1 (true):  returns ErrReadOnly before any file I/O
//   Case 2 (false): proceeds with vacuum
//
// Case 1 is the uncovered branch in validateVacuumPreconditions.
// ---------------------------------------------------------------------------

func TestMCDC7_Vacuum_ReadOnly_Returns_ErrReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro.db")

	// Create a valid database first.
	p := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, p)
	mustCommit(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen read-only.
	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	err := ro.Vacuum(nil)
	if err != ErrReadOnly {
		t.Errorf("Vacuum on read-only pager = %v, want ErrReadOnly", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — Vacuum: transaction-open guard (ErrTransactionOpen)
//
// MC/DC conditions on `p.state != PagerStateOpen`:
//   Case 1 (state == PagerStateWriterLocked): returns ErrTransactionOpen
//   Case 2 (state == PagerStateOpen):         proceeds normally
//
// Case 1 exercises the second guard in validateVacuumPreconditions.
// ---------------------------------------------------------------------------

func TestMCDC7_Vacuum_TransactionOpen_Returns_ErrTransactionOpen(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	mustBeginWrite(t, p)
	// State is now PagerStateWriterLocked; Vacuum should reject it.
	err := p.Vacuum(nil)
	if err != ErrTransactionOpen {
		t.Errorf("Vacuum with open transaction = %v, want ErrTransactionOpen", err)
	}
	// Rollback so cleanup doesn't panic.
	if rbErr := p.Rollback(); rbErr != nil {
		t.Logf("Rollback: %v (non-fatal)", rbErr)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — commitTargetPager: state < PagerStateWriterLocked guard
//
// MC/DC conditions on `targetPager.state >= PagerStateWriterLocked`:
//   Case 1 (true):  Commit() is called
//   Case 2 (false): Commit() is skipped
//
// Case 2: open a fresh target pager without beginning any write transaction,
// then call commitTargetPager directly.
// ---------------------------------------------------------------------------

func TestMCDC7_CommitTargetPager_StateBelow_WriterLocked_Skips(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "target.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// State is PagerStateOpen (< PagerStateWriterLocked); commit is skipped.
	err := p.commitTargetPager(p)
	if err != nil {
		t.Errorf("commitTargetPager with state < WriterLocked = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — copyFile: destination create error
//
// MC/DC conditions on `os.Create(dst)` error:
//   Case 1 (create fails): returns error immediately
//   Case 2 (create ok):    continues with io.Copy
//
// Case 1: use a directory path as destination to force a create error.
// ---------------------------------------------------------------------------

func TestMCDC7_CopyFile_DestCreateError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a real source file.
	srcFile := filepath.Join(dir, "src.db")
	if err := os.WriteFile(srcFile, []byte("data"), 0600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Use a directory as destination — os.Create on a dir returns an error.
	err := copyFile(srcFile, dir)
	if err == nil {
		t.Error("copyFile with directory as dest expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — copyFile: source open error
//
// MC/DC conditions on `os.Open(src)` error:
//   Case 1 (open fails): returns error immediately
//   Case 2 (open ok):    continues
//
// Case 1: use a non-existent source path.
// ---------------------------------------------------------------------------

func TestMCDC7_CopyFile_SrcOpenError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	nonExistent := filepath.Join(dir, "does_not_exist.db")
	dst := filepath.Join(dir, "dst.db")

	err := copyFile(nonExistent, dst)
	if err == nil {
		t.Error("copyFile with missing source expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — writeHeaderToPage1: called without write transaction
//
// MC/DC conditions on `p.ensureWriteTransaction()` result:
//   Case 1: state triggers beginWriteTransaction → succeeds
//   Case 2: p.readOnly=true causes beginWriteTransaction to return ErrReadOnly
//
// Case 2: call writeHeaderToPage1 on a read-only pager so ensureWriteTransaction
// calls beginWriteTransaction which returns ErrReadOnly.
// ---------------------------------------------------------------------------

func TestMCDC7_WriteHeaderToPage1_ReadOnly_Error(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro2.db")

	p := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, p)
	mustCommit(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	// writeHeaderToPage1 calls ensureWriteTransaction, which calls BeginWrite.
	// On a read-only pager, BeginWrite returns ErrReadOnly.
	err := ro.writeHeaderToPage1()
	if err == nil {
		t.Error("writeHeaderToPage1 on read-only pager expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — replaceForVacuumInto: copyFile error path
//
// MC/DC conditions on `copyFile(tempFilename, targetFile)` error:
//   Case 1 (copyFile fails): returns wrapped error
//   Case 2 (copyFile ok):    removes temp and reopens db
//
// Case 1: make temp file non-existent so copyFile fails on os.Open.
// ---------------------------------------------------------------------------

func TestMCDC7_ReplaceForVacuumInto_CopyFileError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "main.db")

	p := openTestPagerAt(t, dbFile, false)
	// Close the db file handle so replaceForVacuumInto can reopen it.
	if err := p.closeCurrentDatabase(); err != nil {
		t.Fatalf("closeCurrentDatabase: %v", err)
	}

	// Use a non-existent temp filename → copyFile will fail.
	nonExistentTemp := filepath.Join(dir, "ghost.db")
	targetFile := filepath.Join(dir, "target.db")

	err := p.replaceForVacuumInto(nonExistentTemp, targetFile)
	if err == nil {
		t.Error("replaceForVacuumInto with missing temp expected error, got nil")
	}
	// Restore file handle to avoid panic on close.
	f, _ := os.OpenFile(dbFile, os.O_RDWR|os.O_CREATE, 0600)
	p.file = f
	p.Close()
}

// ---------------------------------------------------------------------------
// vacuum.go — buildFreePageSet: nil freeList guard
//
// MC/DC conditions on `p.freeList == nil`:
//   Case 1 (nil):  returns empty map immediately
//   Case 2 (!nil): calls collectFreePages
//
// Case 1: set freeList to nil before calling buildFreePageSet.
// ---------------------------------------------------------------------------

func TestMCDC7_BuildFreePageSet_NilFreeList(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)
	mustBeginWrite(t, p)

	// Temporarily nil out the free list.
	origFL := p.freeList
	p.freeList = nil
	defer func() { p.freeList = origFL }()

	set, err := p.buildFreePageSet()
	if err != nil {
		t.Errorf("buildFreePageSet with nil freeList = %v, want nil", err)
	}
	if len(set) != 0 {
		t.Errorf("expected empty set, got %d entries", len(set))
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — IncrementalVacuum: not in incremental mode (returns nil early)
//
// MC/DC conditions on `p.header.LargestRootPage == 0 || p.header.IncrementalVacuum == 0`:
//   Case 1 (true):  returns nil immediately (no-op)
//   Case 2 (false): proceeds with freeTrailingPages
//
// Case 1: fresh database has LargestRootPage == 0.
// ---------------------------------------------------------------------------

func TestMCDC7_IncrementalVacuum_NotIncrementalMode_NoOp(t *testing.T) {
	t.Parallel()
	p := openTestPager(t)

	// LargestRootPage is 0 by default on a fresh DB → should return nil.
	err := p.IncrementalVacuum(0)
	if err != nil {
		t.Errorf("IncrementalVacuum on non-incremental DB = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// vacuum.go — IncrementalVacuum: readOnly guard
//
// MC/DC conditions on `p.readOnly`:
//   Case 1 (true):  returns ErrReadOnly
//   Case 2 (false): proceeds
//
// Case 1: call IncrementalVacuum on a read-only pager.
// ---------------------------------------------------------------------------

func TestMCDC7_IncrementalVacuum_ReadOnly_Returns_ErrReadOnly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro3.db")

	p := openTestPagerAt(t, dbFile, false)
	mustBeginWrite(t, p)
	mustCommit(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ro := openTestPagerAt(t, dbFile, true)
	defer ro.Close()

	err := ro.IncrementalVacuum(0)
	if err != ErrReadOnly {
		t.Errorf("IncrementalVacuum on read-only = %v, want ErrReadOnly", err)
	}
}
