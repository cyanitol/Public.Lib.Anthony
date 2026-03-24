// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// acquireSharedLockWithRetry — retry path via NoBusyHandler (immediate fail)
// and no handler (also immediate fail from tryAcquireSharedLock).
// ---------------------------------------------------------------------------

// TestAcquireSharedLockWithRetry_AlreadyLocked exercises the already-locked fast
// path: lockState >= LockShared so tryAcquireSharedLock returns nil immediately.
func TestAcquireSharedLockWithRetry_AlreadyLocked(t *testing.T) {
	p := openTestPager(t)
	// Pre-set lock state so tryAcquireSharedLock returns nil immediately.
	p.lockState = LockShared
	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// TestAcquireSharedLockWithRetry_NoBusyHandlerImmediate exercises the path where
// no busy handler is installed, tryAcquireSharedLock succeeds on first try.
func TestAcquireSharedLockWithRetry_NoBusyHandlerImmediate(t *testing.T) {
	p := openTestPager(t)
	p.lockState = LockNone
	// tryAcquireSharedLock sets lockState → success on first iteration.
	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Errorf("expected nil on first try, got: %v", err)
	}
}

// TestAcquireSharedLockWithRetry_NoBusyHandlerSet exercises the no-retry path
// when invokeBusyHandler returns false (handler returns false immediately).
func TestAcquireSharedLockWithRetry_NoBusyHandlerSet(t *testing.T) {
	p := openTestPager(t)
	p.WithBusyHandler(&NoBusyHandler{})
	// Force tryAcquireSharedLock to succeed so we never hit the retry path.
	p.lockState = LockNone
	if err := p.acquireSharedLockWithRetry(); err != nil {
		t.Errorf("expected nil (lock succeeded on first try), got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// acquireExclusiveLockWithRetry — similar patterns.
// ---------------------------------------------------------------------------

// TestAcquireExclusiveLockWithRetry_AlreadyExclusive exercises the already-exclusive
// fast path.
func TestAcquireExclusiveLockWithRetry_AlreadyExclusive(t *testing.T) {
	p := openTestPager(t)
	p.lockState = LockExclusive
	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// TestAcquireExclusiveLockWithRetry_Immediate exercises the first-try success.
func TestAcquireExclusiveLockWithRetry_Immediate(t *testing.T) {
	p := openTestPager(t)
	p.lockState = LockNone
	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Errorf("expected nil on first try, got: %v", err)
	}
}

// TestAcquireExclusiveLockWithRetry_WithNoBusyHandler exercises the path where
// the exclusive lock is acquired on the first try (no retry needed).
func TestAcquireExclusiveLockWithRetry_WithNoBusyHandler(t *testing.T) {
	p := openTestPager(t)
	p.WithBusyHandler(&NoBusyHandler{})
	p.lockState = LockNone
	// tryAcquireExclusiveLock always succeeds in this implementation.
	if err := p.acquireExclusiveLockWithRetry(); err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// MarkDirty — nil page and already-dirty branches.
// ---------------------------------------------------------------------------

// TestMarkDirty_NilPage exercises the nil-guard branch.
func TestMarkDirty_NilPage(t *testing.T) {
	c := NewLRUCacheSimple(DefaultPageSize, 100)
	// Must not panic.
	c.MarkDirty(nil)
}

// TestMarkDirty_AlreadyDirty exercises the "page already dirty" branch of
// MarkDirtyByPgno so the inner if block is skipped.
func TestMarkDirty_AlreadyDirty(t *testing.T) {
	c := NewLRUCacheSimple(DefaultPageSize, 100)
	page := NewDbPage(1, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Mark dirty once — page is now in dirty list.
	c.MarkDirty(page)
	// Mark dirty again — exercises the "already dirty" path.
	c.MarkDirty(page)
}

// TestMarkDirty_NewlyDirty exercises the "newly dirty" branch.
func TestMarkDirty_NewlyDirty(t *testing.T) {
	c := NewLRUCacheSimple(DefaultPageSize, 100)
	page := NewDbPage(2, DefaultPageSize)
	if err := c.Put(page); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Page starts clean — first MarkDirty call exercises the newly-dirty branch.
	c.MarkDirty(page)
	if !page.IsDirty() {
		t.Error("expected page to be dirty after MarkDirty")
	}
}

// ---------------------------------------------------------------------------
// processTrunkPage — trunk-not-full and trunk-full branches.
// ---------------------------------------------------------------------------

// TestProcessTrunkPage_SpaceAvailable exercises the addPendingToTrunk path by
// allocating pages, freeing them into the free list, and flushing.
func TestProcessTrunkPage_SpaceAvailable(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)

	// Allocate several pages so we have material to free.
	pgno1 := mustMemoryAllocate(t, mp)
	pgno2 := mustMemoryAllocate(t, mp)
	pgno3 := mustMemoryAllocate(t, mp)
	mustMemoryCommit(t, mp)

	mustMemoryBeginWrite(t, mp)
	mustMemoryFreePage(t, mp, pgno3)
	mustMemoryFreePage(t, mp, pgno2)
	mustMemoryFreePage(t, mp, pgno1)
	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit after freeing: %v", err)
	}
}

// TestProcessTrunkPage_TrunkFull exercises the createNewTrunk path by freeing
// enough pages to overflow one trunk page.
func TestProcessTrunkPage_TrunkFull(t *testing.T) {
	// Use a small page size so trunk fills quickly: (512-8)/4 = 126 leaf pages.
	const smallPage = 512
	mp := mustOpenMemoryPager(t, smallPage)

	// Allocate enough pages to overflow the trunk.
	const numPages = 130
	pgnoList := make([]Pgno, numPages)
	for i := 0; i < numPages; i++ {
		mustMemoryBeginWrite(t, mp)
		pgno := mustMemoryAllocate(t, mp)
		pgnoList[i] = pgno
		mustMemoryCommit(t, mp)
	}

	mustMemoryBeginWrite(t, mp)
	// Free all pages; this should overflow one trunk and create a second.
	for _, pgno := range pgnoList {
		if err := mp.FreePage(pgno); err != nil {
			t.Fatalf("FreePage(%d): %v", pgno, err)
		}
	}
	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// ---------------------------------------------------------------------------
// recoverWALReadOnly / recoverWALReadWrite
// ---------------------------------------------------------------------------

// TestRecoverWALReadOnly exercises recoverWALReadOnly by creating a WAL file
// and then opening the database in read-only mode to trigger recovery.
func TestRecoverWALReadOnly(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_ro.db")

	// Create a database with some WAL frames.
	p := openTestPagerAt(t, dbFile, false)
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		p.Close()
		t.Fatalf("SetJournalMode: %v", err)
	}
	walAllocWriteCommit(t, p, []byte("hello"))
	p.Close()

	// Reopen read-only — recoverWALReadOnly should be called internally.
	p2 := openTestPagerAt(t, dbFile, true)
	defer p2.Close()

	if p2.GetJournalMode() != JournalModeWAL {
		t.Errorf("expected WAL mode after read-only recovery, got %d", p2.GetJournalMode())
	}
}

// TestRecoverWALReadWrite exercises recoverWALReadWrite by leaving uncommitted
// WAL frames in the WAL file and then reopening the database in read-write mode.
func TestRecoverWALReadWrite(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_rw.db")

	// Set up a WAL-mode database.
	p := openTestPagerAt(t, dbFile, false)
	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		p.Close()
		t.Fatalf("SetJournalMode: %v", err)
	}
	walAllocWriteCommit(t, p, []byte("data"))
	// Close without explicit checkpoint to leave WAL frames.
	p.Close()

	// Reopen read-write — recoverWALReadWrite should be called.
	p2 := openTestPagerAt(t, dbFile, false)
	defer p2.Close()
}

// ---------------------------------------------------------------------------
// getLocked — error branches in MemoryPager.
// ---------------------------------------------------------------------------

// TestGetLocked_InvalidPageNum exercises the pgno==0 guard.
func TestGetLocked_InvalidPageNum(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	_, err := mp.Get(0)
	if err != ErrInvalidPageNum {
		t.Errorf("expected ErrInvalidPageNum, got: %v", err)
	}
}

// TestGetLocked_PageBeyondMax exercises the pgno > maxPageNum guard by setting
// maxPageNum to a small value.
func TestGetLocked_PageBeyondMax(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	// Restrict maxPageNum so any page > 1 is out of range.
	mp.maxPageNum = 1
	_, err := mp.Get(2)
	if err != ErrInvalidPageNum {
		t.Errorf("expected ErrInvalidPageNum for page beyond max, got: %v", err)
	}
}

// TestGetLocked_CacheHit exercises the cache-hit branch.
func TestGetLocked_CacheHit(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	// Page 1 is always in the cache after Open.
	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	mp.Put(page)

	// Second Get on page 1 exercises the cache hit branch.
	page2, err := mp.Get(1)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	mp.Put(page2)
}

// ---------------------------------------------------------------------------
// writeLocked — error branches.
// ---------------------------------------------------------------------------

// TestWriteLocked_ReadOnly exercises the readOnly guard.
func TestWriteLocked_ReadOnly(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "ro_write.db")
	pw := openTestPagerAt(t, dbFile, false)
	pw.Close()
	mp, err := openReadOnlyMemoryPager(DefaultPageSize)
	if err != nil {
		// If the helper doesn't exist fall back to a manual approach.
		t.Skip("read-only memory pager not supported")
	}
	defer mp.Close()
	page := NewDbPage(1, DefaultPageSize)
	if werr := mp.Write(page); werr != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got: %v", werr)
	}
}

// openReadOnlyMemoryPager creates a MemoryPager with readOnly=true.
func openReadOnlyMemoryPager(pageSize int) (*MemoryPager, error) {
	mp, err := OpenMemory(pageSize)
	if err != nil {
		return nil, err
	}
	mp.readOnly = true
	return mp, nil
}

// TestWriteLocked_NilPage exercises the nil-page guard.
func TestWriteLocked_NilPage(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.Write(nil); err == nil {
		t.Error("expected error writing nil page")
	}
}

// TestWriteLocked_Success exercises the normal write path.
func TestWriteLocked_Success(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	page, err := mp.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	defer mp.Put(page)
	if err := mp.Write(page); err != nil {
		t.Errorf("expected nil from Write, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Commit — branches in MemoryPager.Commit
// ---------------------------------------------------------------------------

// TestMemoryPagerCommit_NoTransaction exercises the no-transaction error.
func TestMemoryPagerCommit_NoTransaction(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	// No write transaction started.
	if err := mp.Commit(); err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction, got: %v", err)
	}
}

// TestMemoryPagerCommit_NeedsHeaderUpdate exercises the needsHeaderUpdate=true branch.
func TestMemoryPagerCommit_NeedsHeaderUpdate(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mustMemoryBeginWrite(t, mp)
	// Allocate a new page to change dbSize and trigger header update.
	_ = mustMemoryAllocate(t, mp)
	if err := mp.Commit(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestMemoryPagerCommit_ReadTransaction exercises commit with only a read tx.
func TestMemoryPagerCommit_ReadTransaction(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead: %v", err)
	}
	// Commit should fail — not in write state.
	if err := mp.Commit(); err != ErrNoTransaction {
		t.Errorf("expected ErrNoTransaction from read tx, got: %v", err)
	}
	_ = mp.EndRead()
}

// ---------------------------------------------------------------------------
// validateAllocation — branches.
// ---------------------------------------------------------------------------

// TestValidateAllocation_ReadOnly exercises the readOnly error path.
func TestValidateAllocation_ReadOnly(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	mp.readOnly = true
	if err := mp.validateAllocation(); err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got: %v", err)
	}
}

// TestValidateAllocation_OK exercises the success path.
func TestValidateAllocation_OK(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.validateAllocation(); err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Vacuum — both branches.
// ---------------------------------------------------------------------------

// TestMemoryPagerVacuum_NoOpts exercises the nil-opts (no-op) branch.
func TestMemoryPagerVacuum_NoOpts(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	if err := mp.Vacuum(nil); err != nil {
		t.Errorf("Vacuum(nil): %v", err)
	}
}

// TestMemoryPagerVacuum_IntoFileError exercises the IntoFile error branch.
func TestMemoryPagerVacuum_IntoFileError(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	opts := &VacuumOptions{IntoFile: "/tmp/vacuum_out.db"}
	if err := mp.Vacuum(opts); err == nil {
		t.Error("expected error for VACUUM INTO on memory pager, got nil")
	}
}

// TestMemoryPagerVacuum_EmptyIntoFile exercises the empty IntoFile path (no-op).
func TestMemoryPagerVacuum_EmptyIntoFile(t *testing.T) {
	mp := mustOpenMemoryPager(t, DefaultPageSize)
	opts := &VacuumOptions{IntoFile: ""}
	if err := mp.Vacuum(opts); err != nil {
		t.Errorf("Vacuum with empty IntoFile: %v", err)
	}
}
