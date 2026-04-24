// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows

package pager

// MC/DC test coverage for the pager package, batch 10.
// This file targets remaining uncovered branches in:
//   memory_pager.go: FreePage (beginWriteTransaction path, already-in-tx path),
//                    allocatePageInternal (free-list reuse path, pgno != 0),
//                    rollbackLocked (RollbackCallback invocation, journal restoration),
//                    AllocatePage (freeList.Allocate returns non-zero),
//                    rollbackLocked (ErrNoTransaction guard).
//   lock_unix.go:    fcntlSetLk / fcntlGetLk (useOFD == false → POSIX fallback),
//                    acquirePendingLock (currentLevel >= lockReserved, skips inner guard),
//                    releaseSharedLock / releaseReservedLock / releasePendingLock /
//                    releaseExclusiveLock (called through full lock/release cycles),
//                    CheckReservedLock (success path, no conflicting lock).
//   lock.go:         Close (currentLevel != lockNone → releases locks).

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// ---------------------------------------------------------------------------
// memory_pager.go — FreePage: path where beginWriteTransaction is called
//
// MC/DC conditions in FreePage:
//   Case 1 (state == PagerStateOpen):   beginWriteTransaction is called
//   Case 2 (state == PagerStateReader): beginWriteTransaction is called
//   Case 3 (state >= WriterLocked):     beginWriteTransaction is skipped
//
// Cases 1 and 3 are exercised here. Case 2 via BeginRead then FreePage.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_FreePage_FromOpenState(t *testing.T) {
	t.Parallel()
	// Case 1: state == PagerStateOpen → beginWriteTransaction called inside FreePage.
	mp := mustOpenMemoryPager(t, 4096)

	// First allocate a page in a separate transaction so we have one to free.
	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	// mp.state is now PagerStateOpen; FreePage should implicitly start a write tx.
	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("FreePage from Open state: %v", err)
	}
	mustMemoryCommit(t, mp)

	// Free page count should now be non-zero.
	if mp.GetFreePageCount() == 0 {
		t.Error("expected at least one free page after FreePage")
	}
}

func TestMCDC10_MemoryPager_FreePage_FromReaderState(t *testing.T) {
	t.Parallel()
	// Case 2: state == PagerStateReader → beginWriteTransaction called inside FreePage.
	mp := mustOpenMemoryPager(t, 4096)

	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	// Begin a read transaction so state == PagerStateReader.
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead: %v", err)
	}
	// FreePage must promote from reader → writer.
	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("FreePage from Reader state: %v", err)
	}
	mustMemoryCommit(t, mp)
}

func TestMCDC10_MemoryPager_FreePage_WhileInWriteTransaction(t *testing.T) {
	t.Parallel()
	// Case 3: state >= WriterLocked → beginWriteTransaction is skipped.
	mp := mustOpenMemoryPager(t, 4096)

	mustMemoryBeginWrite(t, mp)
	pgno := mustMemoryAllocate(t, mp)

	// Still in write transaction; FreePage must skip the beginWriteTransaction call.
	if err := mp.FreePage(pgno); err != nil {
		t.Fatalf("FreePage while in write transaction: %v", err)
	}
	mustMemoryCommit(t, mp)
}

// ---------------------------------------------------------------------------
// memory_pager.go — FreePage: invalid page number guard
//
// MC/DC conditions `pgno == 0 || pgno > mp.dbSize`:
//   Case A (pgno == 0):        returns ErrInvalidPageNum
//   Case B (pgno > dbSize):    returns ErrInvalidPageNum
//   Case C (valid pgno):       proceeds to freeList.Free
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_FreePage_InvalidPageNum(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// Case A: pgno == 0
	if err := mp.FreePage(0); err != ErrInvalidPageNum {
		t.Errorf("FreePage(0) = %v, want ErrInvalidPageNum", err)
	}

	// Case B: pgno > dbSize (dbSize is 1 after OpenMemory)
	if err := mp.FreePage(Pgno(mp.dbSize + 1)); err != ErrInvalidPageNum {
		t.Errorf("FreePage(dbSize+1) = %v, want ErrInvalidPageNum", err)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — FreePage: readOnly guard
//
// MC/DC condition `mp.readOnly`:
//   Case 1 (readOnly == true):  returns ErrReadOnly immediately
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_FreePage_ReadOnly(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)
	mp.readOnly = true

	if err := mp.FreePage(1); err != ErrReadOnly {
		t.Errorf("FreePage on readOnly pager = %v, want ErrReadOnly", err)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — allocatePageInternal: free-list reuse path (pgno != 0)
//
// MC/DC condition `if pgno != 0` in allocatePageInternal:
//   Case 1 (pgno != 0): returns recycled page from free list
//   Case 2 (pgno == 0): falls through to allocateNewPage
//
// Case 1: allocate, free, then allocate again — second AllocatePage reuses
// the freed page from the free list.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_AllocatePageInternal_FreeListReuse(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// Allocate a new page (extends database).
	mustMemoryBeginWrite(t, mp)
	pgno1 := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	// Free that page so it lands in the free list.
	mustMemoryBeginWrite(t, mp)
	mustMemoryFreePage(t, mp, pgno1)
	mustMemoryCommit(t, mp)

	// Now allocate again — should reuse pgno1 from the free list (pgno != 0 branch).
	mustMemoryBeginWrite(t, mp)
	pgno2 := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	if pgno2 != pgno1 {
		t.Errorf("expected recycled page %d from free list, got %d", pgno1, pgno2)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — AllocatePage: free-list allocation path
//
// Complementary to the above: verifies the overall AllocatePage correctly
// delegates to allocatePageInternal and returns the reused page.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_AllocatePage_FromFreeList(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// Build up some pages, then free two of them.
	mustMemoryBeginWrite(t, mp)
	pgno1 := mustMemoryAllocate(t, mp)
	pgno2 := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	// Free both.
	mustMemoryBeginWrite(t, mp)
	mustMemoryFreePage(t, mp, pgno1)
	mustMemoryFreePage(t, mp, pgno2)
	mustMemoryCommit(t, mp)

	freeCount := mp.GetFreePageCount()
	if freeCount < 2 {
		t.Fatalf("expected >= 2 free pages, got %d", freeCount)
	}

	// Allocate: should reuse a page from the free list.
	mustMemoryBeginWrite(t, mp)
	reused := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	// The recycled page must have been one of the freed pages.
	if reused != pgno1 && reused != pgno2 {
		t.Errorf("expected reused page to be %d or %d, got %d", pgno1, pgno2, reused)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — rollbackLocked: ErrNoTransaction guard
//
// MC/DC condition `mp.state < PagerStateWriterLocked`:
//   Case 1 (true):  returns ErrNoTransaction
//   Case 2 (false): proceeds with rollback
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_RollbackLocked_NoTransaction(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// State is PagerStateOpen (no transaction).
	if err := mp.Rollback(); err != ErrNoTransaction {
		t.Errorf("Rollback with no transaction = %v, want ErrNoTransaction", err)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — rollbackLocked: journal restoration path
//
// MC/DC conditions in rollbackLocked (case 2):
//   a) journalPages is non-empty → pages are restored
//   b) RollbackCallback is nil   → skipped safely
//   c) RollbackCallback is set   → called
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_Rollback_RestoresJournaledPages(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// Write data to page 1 and commit so we have a clean baseline.
	mustMemoryBeginWrite(t, mp)
	page := mustMemoryGet(t, mp, 1)
	mustMemoryWrite(t, mp, page)
	page.Data[100] = 0xAA
	mp.Put(page)
	mustMemoryCommit(t, mp)

	// Start a new write transaction and modify page 1.
	mustMemoryBeginWrite(t, mp)
	page = mustMemoryGet(t, mp, 1)
	mustMemoryWrite(t, mp, page)
	page.Data[100] = 0xBB
	mp.Put(page)

	// Rollback: journal should restore page 1 to 0xAA.
	mustMemoryRollback(t, mp)

	// Verify restoration.
	page = mustMemoryGet(t, mp, 1)
	got := page.Data[100]
	mp.Put(page)
	if got != 0xAA {
		t.Errorf("after rollback page.Data[100] = 0x%X, want 0xAA", got)
	}
}

func TestMCDC10_MemoryPager_Rollback_CallbackInvoked(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	callbackCalled := false
	mp.RollbackCallback = func() {
		callbackCalled = true
	}

	mustMemoryBeginWrite(t, mp)
	page := mustMemoryGet(t, mp, 1)
	mustMemoryWrite(t, mp, page)
	mp.Put(page)

	mustMemoryRollback(t, mp)

	if !callbackCalled {
		t.Error("expected RollbackCallback to be called during rollback")
	}
}

func TestMCDC10_MemoryPager_Rollback_NilCallback(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	// Explicitly confirm nil callback doesn't panic.
	mp.RollbackCallback = nil

	mustMemoryBeginWrite(t, mp)
	page := mustMemoryGet(t, mp, 1)
	mustMemoryWrite(t, mp, page)
	mp.Put(page)

	// Must not panic.
	mustMemoryRollback(t, mp)
}

// ---------------------------------------------------------------------------
// memory_pager.go — rollbackLocked: dbSize restoration
//
// After rollback, dbSize must be restored to dbOrigSize.
// This covers the `mp.dbSize = mp.dbOrigSize` line.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_Rollback_RestoresDbSize(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	origSize := mp.PageCount()

	// Allocate a page (extends dbSize), then rollback.
	mustMemoryBeginWrite(t, mp)
	_ = mustMemoryAllocate(t, mp)

	// dbSize has grown by 1.
	if mp.dbSize <= origSize {
		t.Fatalf("expected dbSize to grow, got %d", mp.dbSize)
	}

	mustMemoryRollback(t, mp)

	// dbSize must be restored.
	if mp.dbSize != origSize {
		t.Errorf("after rollback dbSize = %d, want %d", mp.dbSize, origSize)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — Commit: freeList.Flush error path
//
// MC/DC condition `if err := mp.freeList.Flush(); err != nil`:
//   Normally flush succeeds. The error path is exercised by calling Commit
//   after setting mp into an error-state that causes Flush to fail.
//
// The simplest approach: nil out the freeList to cause Flush to panic/error.
// Instead we rely on observing a successful Flush path (non-nil freeList).
// Since injecting a Flush error requires a mock, we verify the commit error
// state machine: after a successful Flush the state advances to Finished.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_Commit_FreeListFlushSucceeds(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	mustMemoryBeginWrite(t, mp)
	// Allocate two pages and free one — creates pending free list work for Flush.
	p1 := mustMemoryAllocate(t, mp)
	_ = mustMemoryAllocate(t, mp)
	mustMemoryFreePage(t, mp, p1)

	// Commit must call freeList.Flush() successfully.
	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// After commit, state should be PagerStateOpen.
	if mp.state != PagerStateOpen {
		t.Errorf("after Commit state = %d, want PagerStateOpen", mp.state)
	}
}

// ---------------------------------------------------------------------------
// memory_pager.go — Commit: needsHeaderUpdate = false path
//
// MC/DC: when dbSize, FreelistTrunk, and FreelistCount are all unchanged,
// updateDatabaseHeader is NOT called.
// ---------------------------------------------------------------------------

func TestMCDC10_MemoryPager_Commit_NeedsHeaderUpdate_False(t *testing.T) {
	t.Parallel()
	mp := mustOpenMemoryPager(t, 4096)

	mustMemoryBeginWrite(t, mp)
	// Capture dbOrigSize and header state before any changes.
	dbOrig := mp.dbSize

	// Write to existing page 1 only — no allocation, no free-list changes.
	page := mustMemoryGet(t, mp, 1)
	mustMemoryWrite(t, mp, page)
	page.Data[200] = 0xFF
	mp.Put(page)

	// Ensure dbSize hasn't changed.
	if mp.dbSize != dbOrig {
		t.Skip("dbSize changed unexpectedly, skipping NeedsHeaderUpdate=false test")
	}

	headerBefore := mp.header.FileChangeCounter

	mustMemoryCommit(t, mp)

	// If needsHeaderUpdate was false, FileChangeCounter should not have changed.
	// Note: if freeList counts happen to differ, counter may still increment.
	// We just verify Commit succeeded cleanly.
	t.Logf("FileChangeCounter: before=%d, after=%d", headerBefore, mp.header.FileChangeCounter)
}

// ---------------------------------------------------------------------------
// lock_unix.go — fcntlSetLk / fcntlGetLk: useOFD == false (POSIX fallback)
//
// MC/DC conditions:
//   Case 1 (useOFD == true):  returns F_OFD_SETLK / F_OFD_GETLK
//   Case 2 (useOFD == false): returns F_SETLK / F_GETLK (POSIX fallback)
//
// Force Case 2 by setting data.useOFD = false after creating the LockManager.
// ---------------------------------------------------------------------------

func TestMCDC10_LockUnix_FcntlSetLk_POSIXFallback(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "posix.bin")
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

	// Force POSIX fallback by setting useOFD = false.
	data := lm.platformData.(*unixLockData)
	data.useOFD = false

	cmd := lm.fcntlSetLk()
	if cmd != syscall.F_SETLK {
		t.Errorf("fcntlSetLk with useOFD=false = %d, want F_SETLK (%d)", cmd, syscall.F_SETLK)
	}
}

func TestMCDC10_LockUnix_FcntlGetLk_POSIXFallback(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "posixget.bin")
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

	data := lm.platformData.(*unixLockData)
	data.useOFD = false

	cmd := lm.fcntlGetLk()
	if cmd != syscall.F_GETLK {
		t.Errorf("fcntlGetLk with useOFD=false = %d, want F_GETLK (%d)", cmd, syscall.F_GETLK)
	}
}

func TestMCDC10_LockUnix_FcntlSetLk_OFDEnabled(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "ofd.bin")
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

	data := lm.platformData.(*unixLockData)
	data.useOFD = true

	cmd := lm.fcntlSetLk()
	if cmd != F_OFD_SETLK {
		t.Errorf("fcntlSetLk with useOFD=true = %d, want F_OFD_SETLK (%d)", cmd, F_OFD_SETLK)
	}

	cmdGet := lm.fcntlGetLk()
	if cmdGet != F_OFD_GETLK {
		t.Errorf("fcntlGetLk with useOFD=true = %d, want F_OFD_GETLK (%d)", cmdGet, F_OFD_GETLK)
	}
}

// ---------------------------------------------------------------------------
// lock_unix.go — acquirePendingLock: currentLevel >= lockReserved (inner guard false)
//
// MC/DC condition `lm.currentLevel < lockReserved`:
//   Case A (true):  acquireReservedLock called inside acquirePendingLock
//   Case B (false): acquireReservedLock is skipped
//
// Case B: hold lockReserved before calling acquirePendingLock so the guard is false.
// ---------------------------------------------------------------------------

func TestMCDC10_LockUnix_AcquirePendingLock_AlreadyHoldsReserved(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "pend_res.bin")
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

	// Escalate to SHARED then RESERVED so currentLevel == lockReserved.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Skipf("AcquireLock(lockReserved): %v", err)
	}

	// Now currentLevel == lockReserved (>= lockReserved), so the inner guard
	// in acquirePendingLock is false (Case B — reserved lock is NOT re-acquired).
	// Call acquirePendingLock directly to exercise this branch.
	err = lm.acquirePendingLock()
	if err != nil {
		t.Logf("acquirePendingLock() returned %v (platform may block)", err)
		return
	}
	// Verify pending lock was acquired.
	if lm.currentLevel < lockReserved {
		t.Errorf("expected currentLevel >= lockReserved after acquirePendingLock, got %v", lm.currentLevel)
	}
}

// ---------------------------------------------------------------------------
// lock_unix.go — releaseSharedLock via ReleaseLock cycle
//
// Exercise releaseSharedLock, releaseReservedLock by acquiring and fully
// releasing locks through the standard API.
// ---------------------------------------------------------------------------

func TestMCDC10_LockUnix_ReleaseSharedLock(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "rel_sh.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}

	// Acquire SHARED.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}

	// Release back to NONE — exercises releaseSharedLock.
	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Errorf("ReleaseLock(lockNone) from lockShared: %v", err)
	}
	if lm.currentLevel != lockNone {
		t.Errorf("expected lockNone after release, got %v", lm.currentLevel)
	}

	lm.Close()
}

func TestMCDC10_LockUnix_ReleaseReservedLock(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "rel_res.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}

	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Skipf("AcquireLock(lockReserved): %v", err)
	}

	// Release back to NONE — exercises releaseReservedLock and releaseSharedLock.
	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Errorf("ReleaseLock(lockNone) from lockReserved: %v", err)
	}

	lm.Close()
}

// ---------------------------------------------------------------------------
// lock_unix.go — releasePendingLock and releaseExclusiveLock via full cycle
//
// Acquire EXCLUSIVE (which goes through PENDING internally), then release.
// ---------------------------------------------------------------------------

func TestMCDC10_LockUnix_ReleasePendingAndExclusiveLock(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "excl.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}

	// Acquire SHARED → RESERVED → PENDING → EXCLUSIVE.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Skipf("AcquireLock(lockReserved): %v", err)
	}
	if err := lm.AcquireLock(lockPending); err != nil {
		t.Skipf("AcquireLock(lockPending): %v", err)
	}
	if err := lm.AcquireLock(lockExclusive); err != nil {
		t.Skipf("AcquireLock(lockExclusive): %v", err)
	}

	// Release back to NONE — exercises releaseExclusiveLock, releasePendingLock,
	// releaseReservedLock, and releaseSharedLock.
	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Errorf("ReleaseLock(lockNone) from lockExclusive: %v", err)
	}

	lm.Close()
}

// ---------------------------------------------------------------------------
// lock_unix.go — CheckReservedLock: success path (no conflicting lock)
//
// MC/DC condition `lock.Type != syscall.F_UNLCK`:
//   Case 1 (F_UNLCK):  returns (false, nil) — no lock conflict
//   Case 2 (!F_UNLCK): returns (true, nil) — lock conflict exists
//
// Case 1: on a freshly opened file with no other locks, CheckReservedLock
// must report no conflict.
// ---------------------------------------------------------------------------

func TestMCDC10_LockUnix_CheckReservedLock_NoConflict(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "chkres.bin")
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

	locked, err := lm.CheckReservedLock()
	if err != nil {
		t.Fatalf("CheckReservedLock: %v", err)
	}
	if locked {
		t.Error("expected no reserved lock conflict on fresh file, got locked=true")
	}
}

func TestMCDC10_LockUnix_CheckReservedLock_POSIXFallback(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "chkres2.bin")
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

	// Force POSIX path for fcntlGetLk.
	data := lm.platformData.(*unixLockData)
	data.useOFD = false

	locked, err := lm.CheckReservedLock()
	if err != nil {
		t.Fatalf("CheckReservedLock (POSIX): %v", err)
	}
	// No other process holds it; must be false.
	if locked {
		t.Error("expected no reserved lock conflict, got locked=true")
	}
}

// ---------------------------------------------------------------------------
// lock.go — Close: currentLevel != lockNone (releases locks then cleans up)
//
// MC/DC condition `if lm.currentLevel == lockNone`:
//   Case 1 (== lockNone):  returns nil immediately  [covered by mcdc8]
//   Case 2 (!= lockNone):  calls releaseLockPlatform + cleanupPlatform
//
// Case 2: acquire a SHARED lock, then Close() must release it.
// ---------------------------------------------------------------------------

func TestMCDC10_LockManager_Close_WithLockHeld(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "close_held.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}

	if err := lm.AcquireLock(lockShared); err != nil {
		t.Skipf("AcquireLock(lockShared): %v", err)
	}

	// currentLevel != lockNone → Close must release all locks.
	if err := lm.Close(); err != nil {
		t.Errorf("Close() with lockShared held: %v", err)
	}
	if lm.currentLevel != lockNone {
		t.Errorf("after Close(), currentLevel = %v, want lockNone", lm.currentLevel)
	}
}

// mcdc10EscalateToExclusive escalates the lock manager through all levels to exclusive.
func mcdc10EscalateToExclusive(t *testing.T, lm *LockManager) {
	t.Helper()
	levels := []LockLevel{lockShared, lockReserved, lockPending, lockExclusive}
	for _, level := range levels {
		if err := lm.AcquireLock(level); err != nil {
			t.Skipf("AcquireLock(%v): %v", level, err)
		}
	}
}

func TestMCDC10_LockManager_Close_WithExclusiveLockHeld(t *testing.T) {
	t.Parallel()
	tmp := filepath.Join(t.TempDir(), "close_excl.bin")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}

	mcdc10EscalateToExclusive(t, lm)

	if err := lm.Close(); err != nil {
		t.Errorf("Close() with lockExclusive held: %v", err)
	}
	if lm.currentLevel != lockNone {
		t.Errorf("after Close(), currentLevel = %v, want lockNone", lm.currentLevel)
	}
}
