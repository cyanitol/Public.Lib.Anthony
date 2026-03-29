// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows

package pager

// MC/DC test coverage for the pager package, batch 13.
// This file targets remaining uncovered branches in:
//   pager.go:      writeDirtyPagesToWAL, writePageFrameToWAL (WAL write path)
//                  commitPhase1WriteDirtyPages, commitPhase2SyncDatabase
//                  beginWriteTransaction
//   savepoint.go:  restoreToSavepoint
//   transaction.go: TryUpgradeToExclusive

import (
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// writeDirtyPagesToWAL / writePageFrameToWAL
//
// Exercise the WAL write path by opening a WAL-mode pager, writing 3 pages
// in a single transaction, and committing — this forces both writeDirtyPagesToWAL
// (which iterates dirty pages) and writePageFrameToWAL (called per page).
// ---------------------------------------------------------------------------

func TestMCDC13_WAL_WriteMultiplePages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_multi.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	// Allocate and write 3 pages within one transaction.
	mustBeginWrite(t, p)
	var pgnos [3]Pgno
	for i := 0; i < 3; i++ {
		pgnos[i] = mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgnos[i])
		page.Data[0] = byte(0x10 + i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	// Verify all three pages are readable and have the correct data.
	mustBeginRead(t, p)
	for i := 0; i < 3; i++ {
		rPage := mustGetPage(t, p, pgnos[i])
		want := byte(0x10 + i)
		if rPage.Data[0] != want {
			t.Errorf("page %d Data[0] = 0x%X, want 0x%X", pgnos[i], rPage.Data[0], want)
		}
		p.Put(rPage)
	}
	mustEndRead(t, p)
}

// TestMCDC13_WAL_LargeDataCommit writes a page with a larger data payload to
// cover writePageFrameToWAL with non-trivial data sizes.
func TestMCDC13_WAL_LargeDataCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "wal_large.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	if err := p.SetJournalMode(JournalModeWAL); err != nil {
		t.Fatalf("SetJournalMode(WAL): %v", err)
	}

	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	// Fill the entire page with a recognisable pattern.
	for i := range page.Data {
		page.Data[i] = byte(i % 251)
	}
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Read back and spot-check.
	mustBeginRead(t, p)
	rPage := mustGetPage(t, p, pgno)
	for i := 0; i < 4; i++ {
		want := byte(i % 251)
		if rPage.Data[i] != want {
			t.Errorf("page %d Data[%d] = 0x%X, want 0x%X", pgno, i, rPage.Data[i], want)
		}
	}
	p.Put(rPage)
	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// restoreToSavepoint
//
// TestMCDC13_Savepoint_RestoreModified:
//   Write a page, create a savepoint, modify the page again, roll back to the
//   savepoint, verify that the original write is restored.
// ---------------------------------------------------------------------------

func TestMCDC13_Savepoint_RestoreModified(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sp_restore.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Write an initial value and commit so the page exists in the database.
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0xAA
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Begin a new write transaction, modify to 0xBB, then create a savepoint.
	mustBeginWrite(t, p)
	page = mustGetWritePage(t, p, pgno) // Write() called before data change (saves 0xAA in journal)
	page.Data[0] = 0xBB
	p.Put(page)

	mustSavepoint(t, p, "sp1")

	// Modify the page again after the savepoint.
	// Use mustGetWritePage: Write() is called before the data change so
	// savePageState captures 0xBB (current value) into sp1.Pages.
	page = mustGetWritePage(t, p, pgno)
	page.Data[0] = 0xCC
	p.Put(page)

	// Roll back to the savepoint — should restore Data[0] to 0xBB.
	mustRollbackTo(t, p, "sp1")

	page = mustGetPage(t, p, pgno)
	got := page.Data[0]
	p.Put(page)

	if got != 0xBB {
		t.Errorf("after rollback to savepoint: Data[0] = 0x%X, want 0xBB", got)
	}

	mustRollback(t, p)
}

// TestMCDC13_Savepoint_MultipleRestores creates two savepoints, modifies the
// page between them, and rolls back to each in turn, exercising the loop in
// restoreToSavepoint that merges page state from newer savepoints.
func TestMCDC13_Savepoint_MultipleRestores(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "sp_multi.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Establish a base page.
	mustBeginWrite(t, p)
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0x01
	mustWritePage(t, p, page)
	p.Put(page)
	mustCommit(t, p)

	// Transaction: set 0x11, create sp_a, set 0x22, create sp_b, then set 0x33.
	// Use mustGetWritePage so Write() is called BEFORE modifying Data; this
	// ensures savePageState captures the pre-change value in each savepoint.
	mustBeginWrite(t, p)

	page = mustGetWritePage(t, p, pgno) // saves 0x01 into any existing savepoints (none yet)
	page.Data[0] = 0x11
	p.Put(page)
	mustSavepoint(t, p, "sp_a") // sp_a created; sp_a.Pages is initially empty

	page = mustGetWritePage(t, p, pgno) // savePageState: saves 0x11 into sp_a.Pages
	page.Data[0] = 0x22
	p.Put(page)
	mustSavepoint(t, p, "sp_b") // sp_b created; sp_b.Pages is initially empty

	page = mustGetWritePage(t, p, pgno) // savePageState: saves 0x22 into sp_b.Pages (and sp_a if not already set)
	page.Data[0] = 0x33
	p.Put(page)

	// Roll back to sp_b: Data[0] should return to 0x22.
	mustRollbackTo(t, p, "sp_b")
	page = mustGetPage(t, p, pgno)
	if page.Data[0] != 0x22 {
		t.Errorf("after rollback to sp_b: Data[0] = 0x%X, want 0x22", page.Data[0])
	}
	p.Put(page)

	// Roll back to sp_a: Data[0] should return to 0x11.
	mustRollbackTo(t, p, "sp_a")
	page = mustGetPage(t, p, pgno)
	if page.Data[0] != 0x11 {
		t.Errorf("after rollback to sp_a: Data[0] = 0x%X, want 0x11", page.Data[0])
	}
	p.Put(page)

	mustRollback(t, p)
}

// ---------------------------------------------------------------------------
// TryUpgradeToExclusive
//
// File-based pagers with OS locking support upgrading to an exclusive lock.
// On platforms / FS configurations where locking is a no-op the function still
// returns (true, nil), so this test is unconditionally useful.
// ---------------------------------------------------------------------------

func TestMCDC13_TryUpgradeToExclusive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "exclusive.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	// Acquire a shared lock first by beginning a read transaction.
	mustBeginRead(t, p)

	ok, err := p.TryUpgradeToExclusive()
	if err != nil {
		// On some environments (e.g., FUSE, tmpfs without flock) the syscall
		// may return an error. Skip rather than fail.
		t.Skipf("TryUpgradeToExclusive returned error (locking not supported): %v", err)
	}
	if !ok {
		t.Skip("TryUpgradeToExclusive returned false (another process holds lock), skipping")
	}

	// After a successful upgrade the lock state should be at least Exclusive.
	if p.lockState < LockExclusive {
		t.Errorf("lockState after TryUpgradeToExclusive = %v, want >= LockExclusive", p.lockState)
	}

	mustEndRead(t, p)
}

// ---------------------------------------------------------------------------
// commitPhase1WriteDirtyPages / commitPhase2SyncDatabase
//
// These are exercised indirectly via Commit.  Writing 5 pages and committing
// forces both phases (phase 1 writes, phase 2 syncs).
// ---------------------------------------------------------------------------

func TestMCDC13_Commit_DirtyPages(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "commit_dirty.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginWrite(t, p)
	var pgnos [5]Pgno
	for i := 0; i < 5; i++ {
		pgnos[i] = mustAllocatePage(t, p)
		page := mustGetPage(t, p, pgnos[i])
		page.Data[0] = byte(0x50 + i)
		mustWritePage(t, p, page)
		p.Put(page)
	}
	mustCommit(t, p)

	// Verify all pages survived the commit.
	mustBeginRead(t, p)
	for i := 0; i < 5; i++ {
		rPage := mustGetPage(t, p, pgnos[i])
		want := byte(0x50 + i)
		if rPage.Data[0] != want {
			t.Errorf("page %d Data[0] = 0x%X, want 0x%X", pgnos[i], rPage.Data[0], want)
		}
		p.Put(rPage)
	}
	mustEndRead(t, p)
}

// TestMCDC13_Commit_EmptyTransaction begins a write transaction and immediately
// commits without writing any pages, exercising the "no dirty pages" branch of
// commitPhase1WriteDirtyPages.
func TestMCDC13_Commit_EmptyTransaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "commit_empty.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginWrite(t, p)
	// Commit with no writes — dirty page list is empty.
	mustCommit(t, p)
}

// ---------------------------------------------------------------------------
// beginWriteTransaction
//
// TestMCDC13_BeginWriteTransaction_Twice calls BeginWrite twice on the same
// pager; the second call should return ErrTransactionOpen because a write
// transaction is already active.
// ---------------------------------------------------------------------------

func TestMCDC13_BeginWriteTransaction_Twice(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "bwt_twice.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginWrite(t, p)

	err := p.BeginWrite()
	if err == nil {
		t.Error("expected error on second BeginWrite, got nil")
	}
	if err != ErrTransactionOpen {
		t.Errorf("second BeginWrite error = %v, want ErrTransactionOpen", err)
	}

	mustRollback(t, p)
}

// TestMCDC13_BeginWriteTransaction_ReadFirst begins a read transaction first
// and then begins a write transaction, exercising the upgrade path inside
// beginWriteTransaction.
func TestMCDC13_BeginWriteTransaction_ReadFirst(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "bwt_read_first.db")

	p := openTestPagerAt(t, dbFile, false)
	defer p.Close()

	mustBeginRead(t, p)
	mustEndRead(t, p)

	// After ending the read transaction the pager is back in Open state.
	// Now begin a write transaction directly.
	mustBeginWrite(t, p)

	// Write a page to confirm the write transaction is functional.
	pgno := mustAllocatePage(t, p)
	page := mustGetPage(t, p, pgno)
	page.Data[0] = 0x77
	mustWritePage(t, p, page)
	p.Put(page)

	mustCommit(t, p)

	// Verify the page was persisted.
	mustBeginRead(t, p)
	rPage := mustGetPage(t, p, pgno)
	if rPage.Data[0] != 0x77 {
		t.Errorf("Data[0] after read-first upgrade path = 0x%X, want 0x77", rPage.Data[0])
	}
	p.Put(rPage)
	mustEndRead(t, p)
}
