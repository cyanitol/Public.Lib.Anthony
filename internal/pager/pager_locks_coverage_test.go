// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build !windows

package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// openLockTestFile creates a temporary file suitable for lock manager tests.
func openLockTestFile(t *testing.T) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lock.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("openLockTestFile: %v", err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		f.Close()
		t.Fatalf("openLockTestFile write: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// ---------------------------------------------------------------------------
// acquireReservedLock — branch: currentLevel < lockShared triggers acquireSharedLock
// ---------------------------------------------------------------------------

// TestAcquireReservedLock_FromBelowShared exercises the branch in acquireReservedLock
// where lm.currentLevel < lockShared, which causes the function to also acquire a
// shared lock before returning.  We call acquireReservedLock directly on a
// LockManager whose currentLevel is still lockNone.
func TestAcquireReservedLock_FromBelowShared(t *testing.T) {
	f := openLockTestFile(t)
	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// currentLevel starts at lockNone which is < lockShared.
	// acquireReservedLock must therefore call acquireSharedLock internally.
	if err := lm.acquireReservedLock(); err != nil {
		t.Errorf("acquireReservedLock() from lockNone: unexpected error: %v", err)
	}
}

// TestAcquireReservedLock_AlreadyShared exercises the other branch: when
// currentLevel is already >= lockShared the shared-lock sub-call is skipped.
func TestAcquireReservedLock_AlreadyShared(t *testing.T) {
	f := openLockTestFile(t)
	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// Bring level up to SHARED first via the public API.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED): %v", err)
	}

	// Now acquireReservedLock should not call acquireSharedLock again.
	if err := lm.acquireReservedLock(); err != nil {
		t.Errorf("acquireReservedLock() from lockShared: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// acquirePendingLock — branch: currentLevel < lockReserved triggers acquireReservedLock
// ---------------------------------------------------------------------------

// TestAcquirePendingLock_FromShared exercises the branch in acquirePendingLock
// where lm.currentLevel < lockReserved.  With currentLevel == lockShared the
// function must call acquireReservedLock internally to satisfy the invariant.
func TestAcquirePendingLock_FromShared(t *testing.T) {
	f := openLockTestFile(t)
	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// Bring to SHARED so currentLevel (lockShared) < lockReserved.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED): %v", err)
	}

	// acquirePendingLock must acquire reserved internally before returning.
	if err := lm.acquirePendingLock(); err != nil {
		t.Errorf("acquirePendingLock() from lockShared: unexpected error: %v", err)
	}
}

// TestAcquirePendingLock_AlreadyReserved exercises the skip-branch where
// currentLevel >= lockReserved, so acquireReservedLock is not called.
func TestAcquirePendingLock_AlreadyReserved(t *testing.T) {
	f := openLockTestFile(t)
	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager: %v", err)
	}
	defer lm.Close()

	// Bring to SHARED then RESERVED via the standard escalation path.
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED): %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED): %v", err)
	}

	// acquirePendingLock with currentLevel == lockReserved skips the sub-call.
	if err := lm.acquirePendingLock(); err != nil {
		t.Errorf("acquirePendingLock() from lockReserved: unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// commitPhase0FlushFreeList — error path: setErrorState branch
// ---------------------------------------------------------------------------

// TestCommitPhase0FlushFreeList_ErrorPath exercises the error branch of
// commitPhase0FlushFreeList.  We add pending pages to the freelist but close
// the underlying database file first so that Flush → getLocked fails, causing
// setErrorState to be called.
func TestCommitPhase0FlushFreeList_ErrorPath(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(filepath.Join(dir, "flush_err.db"), false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Do not defer p.Close() — we close the file manually below.

	// Allocate a page so dbSize > 1, giving us a valid page to free.
	if err := p.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	pgno, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("Commit (setup): %v", err)
	}

	// Begin another write transaction and free the page so there are
	// pending pages in the freelist.
	if err := p.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite (free): %v", err)
	}
	if err := p.FreePage(pgno); err != nil {
		t.Fatalf("FreePage: %v", err)
	}

	// Close the underlying file so that Flush's getLocked call will fail.
	p.mu.Lock()
	p.file.Close()
	p.mu.Unlock()

	// Now call commitPhase0FlushFreeList directly (lock already conceptually
	// held since we hold p.mu above; call without lock to match test pattern).
	p.mu.Lock()
	flushErr := p.commitPhase0FlushFreeList()
	p.mu.Unlock()

	if flushErr == nil {
		t.Error("expected error from commitPhase0FlushFreeList with closed file, got nil")
	}

	// Verify pager entered error state.
	if p.state != PagerStateError {
		t.Errorf("expected PagerStateError after flush failure, got state=%d", p.state)
	}
}

// ---------------------------------------------------------------------------
// commitPhase3FinalizeJournal — error path: setErrorState branch
// ---------------------------------------------------------------------------

// TestCommitPhase3FinalizeJournal_ErrorPath exercises the error branch of
// commitPhase3FinalizeJournal.  We inject a pre-closed *os.File as the
// journal file so that journalFile.Close() returns an error, causing
// setErrorState to be called.
func TestCommitPhase3FinalizeJournal_ErrorPath(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(filepath.Join(dir, "finalize_err.db"), false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Create a real file that we will close before injecting.
	jPath := filepath.Join(dir, "finalize_err.db-journal")
	jf, err := os.OpenFile(jPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("create journal file: %v", err)
	}
	// Close it immediately so that p.journalFile.Close() will fail.
	jf.Close()

	// Inject the already-closed file as the pager's journal file.
	p.mu.Lock()
	p.state = PagerStateWriterCachemod
	p.journalFile = jf
	p.journalFilename = jPath
	p.journalMode = JournalModeDelete
	finalizeErr := p.commitPhase3FinalizeJournal()
	p.mu.Unlock()

	if finalizeErr == nil {
		t.Error("expected error from commitPhase3FinalizeJournal with closed journal file, got nil")
	}

	// Verify pager entered error state.
	if p.state != PagerStateError {
		t.Errorf("expected PagerStateError after finalize failure, got state=%d", p.state)
	}
}
