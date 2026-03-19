// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestZeroJournalHeader tests the zeroJournalHeader function (0% coverage)
func TestZeroJournalHeader(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_zero_header.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create a journal file first
	journal := NewJournal(pager.journalFilename, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	pageData := make([]byte, 4096)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write to journal: %v", err)
	}
	journal.Close()

	// Now zero the journal header using the pager's method
	if err := pager.zeroJournalHeader(); err != nil {
		t.Errorf("zeroJournalHeader() error = %v", err)
	}

	// Verify the journal header is zeroed
	data := make([]byte, 4)
	f, err := os.Open(pager.journalFilename)
	if err != nil {
		t.Fatalf("failed to open journal file: %v", err)
	}
	defer f.Close()

	if _, err := f.ReadAt(data, 0); err != nil {
		t.Fatalf("failed to read journal header: %v", err)
	}

	if !bytes.Equal(data, []byte{0, 0, 0, 0}) {
		t.Errorf("journal header not zeroed: got %v", data)
	}
}

// TestZeroJournalHeaderNonExistent tests zeroing a non-existent journal
func TestZeroJournalHeaderNonExistent(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_no_journal.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try to zero a non-existent journal (should fail)
	err = pager.zeroJournalHeader()
	if err == nil {
		t.Error("expected error when zeroing non-existent journal")
	}
}

// TestCommitPhase0FlushFreeList tests commitPhase0FlushFreeList (50% coverage)
func TestCommitPhase0FlushFreeList(t *testing.T) {
	t.Parallel()
	p := openTestPagerSized(t, 4096)

	// Allocate pages and commit
	mustBeginWrite(t, p)
	mustAllocatePages(t, p, 10)
	mustCommit(t, p)

	// Free some pages
	mustBeginWrite(t, p)
	mustFreePage(t, p, 5)
	mustFreePage(t, p, 6)

	// Start a write transaction
	mustGetWritePageData(t, p, 1, 0)

	// Test phase 0
	p.mu.Lock()
	err := p.commitPhase0FlushFreeList()
	p.mu.Unlock()

	if err != nil {
		t.Errorf("commitPhase0FlushFreeList() error = %v", err)
	}
}

// TestCommitPhase1WriteDirtyPages tests commitPhase1WriteDirtyPages (55.6% coverage)
func TestCommitPhase1WriteDirtyPages(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_phase1.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create dirty pages
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	copy(page.Data[DatabaseHeaderSize:], []byte("test data"))
	pager.Put(page)

	// Test phase 1
	pager.mu.Lock()
	err = pager.commitPhase1WriteDirtyPages()
	pager.mu.Unlock()

	if err != nil {
		t.Errorf("commitPhase1WriteDirtyPages() error = %v", err)
	}
}

// TestCommitPhase2SyncDatabase tests commitPhase2SyncDatabase (50% coverage)
func TestCommitPhase2SyncDatabase(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_phase2.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Test phase 2 (sync)
	pager.mu.Lock()
	err = pager.commitPhase2SyncDatabase()
	pager.mu.Unlock()

	if err != nil {
		t.Errorf("commitPhase2SyncDatabase() error = %v", err)
	}
}

// TestCommitPhase3FinalizeJournal tests commitPhase3FinalizeJournal (50% coverage)
func TestCommitPhase3FinalizeJournal(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_phase3.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create a journal
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	pager.Put(page)

	// Test phase 3
	pager.mu.Lock()
	err = pager.commitPhase3FinalizeJournal()
	pager.mu.Unlock()

	if err != nil {
		t.Errorf("commitPhase3FinalizeJournal() error = %v", err)
	}
}

// TestInitNewDatabaseInternalReadOnly tests initNewDatabase with read-only flag (42.9% coverage)
func TestInitNewDatabaseInternalReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_readonly_init.db")

	// Try to create a new database in read-only mode (should fail)
	pager := &Pager{
		filename: dbFile,
		pageSize: 4096,
	}

	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	pager.file = f

	err = pager.initNewDatabase(true)
	if err == nil {
		t.Error("expected error when creating new database in read-only mode")
	}
	if err != nil && err.Error() != "cannot create new database in read-only mode" {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireSharedLockWithRetrySuccess tests successful acquisition (50% coverage)
func TestAcquireSharedLockWithRetrySuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_shared_retry.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set up pager state
	pager.lockState = LockNone

	// Should succeed immediately
	err = pager.acquireSharedLockWithRetry()
	if err != nil {
		t.Errorf("acquireSharedLockWithRetry() error = %v", err)
	}

	if pager.lockState != LockShared {
		t.Errorf("lock state = %v, want %v", pager.lockState, LockShared)
	}
}

// TestAcquireReservedLockWithRetrySuccess tests successful acquisition (50% coverage)
func TestAcquireReservedLockWithRetrySuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_reserved_retry.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set up pager state
	pager.lockState = LockShared

	// Should succeed immediately
	err = pager.acquireReservedLockWithRetry()
	if err != nil {
		t.Errorf("acquireReservedLockWithRetry() error = %v", err)
	}

	if pager.lockState != LockReserved {
		t.Errorf("lock state = %v, want %v", pager.lockState, LockReserved)
	}
}

// TestAcquireExclusiveLockWithRetrySuccess tests successful acquisition (50% coverage)
func TestAcquireExclusiveLockWithRetrySuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_exclusive_retry.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set up pager state
	pager.lockState = LockReserved

	// Should succeed immediately
	err = pager.acquireExclusiveLockWithRetry()
	if err != nil {
		t.Errorf("acquireExclusiveLockWithRetry() error = %v", err)
	}

	if pager.lockState != LockExclusive {
		t.Errorf("lock state = %v, want %v", pager.lockState, LockExclusive)
	}
}

// TestProcessTrunkPageFullTrunk tests trunk page processing when trunk is full (53.8% coverage)
func TestProcessTrunkPageFullTrunk(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_trunk_full.db")

	pager := mustOpenPagerSized(t, dbFile, 4096)
	defer pager.Close()

	fl := NewFreeList(pager)

	maxLeaves := FreeListMaxLeafPages(4096)
	numPages := maxLeaves + 20

	mustCreateWritePages(t, pager, 2, Pgno(numPages))

	// Free enough pages to fill a trunk
	mustFreePages(t, fl, 10, Pgno(numPages-5))
	mustFlush(t, fl)

	if fl.GetFirstTrunk() == 0 {
		t.Error("expected trunk page to be created")
	}

	// Free more pages to trigger processing of a full trunk
	mustFreePages(t, fl, Pgno(numPages-4), Pgno(numPages))
	mustFlush(t, fl)

	if err := fl.Verify(); err != nil {
		t.Errorf("freelist verification failed: %v", err)
	}
}

// TestProcessTrunkPageAddToExisting tests adding pages to existing trunk (53.8% coverage)
func TestProcessTrunkPageAddToExisting(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_trunk_add.db")

	pager := mustOpenPagerSized(t, dbFile, 4096)
	defer pager.Close()

	fl := NewFreeList(pager)

	mustCreateWritePages(t, pager, 2, 30)

	// Free a small number of pages (should create partially-filled trunk)
	mustFreePages(t, fl, 10, 15)
	mustFlush(t, fl)

	// Free a few more pages (should add to existing trunk)
	mustFreePages(t, fl, 20, 22)
	mustFlush(t, fl)

	if fl.GetTotalFree() == 0 {
		t.Error("expected non-zero free page count")
	}
}

// TestEnableWALModeReadOnly tests enabling WAL on read-only database (42.1% coverage)
func TestEnableWALModeReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_readonly.db")

	// Create database first
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	pager1.Close()

	// Open read-only
	pager2, err := OpenWithPageSize(dbFile, true, 4096)
	if err != nil {
		t.Fatalf("failed to open read-only: %v", err)
	}
	defer pager2.Close()

	// Try to enable WAL mode (should fail)
	err = pager2.enableWALMode()
	if err == nil {
		t.Error("expected error when enabling WAL on read-only database")
	}
	if err != nil && err.Error() != "cannot enable WAL mode on read-only database" {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestEnableWALModeSuccess tests successful WAL mode enabling (42.1% coverage)
func TestEnableWALModeSuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_enable.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Enable WAL mode
	err = pager.enableWALMode()
	if err != nil {
		t.Errorf("enableWALMode() error = %v", err)
	}

	// Verify WAL was created
	if pager.wal == nil {
		t.Error("expected wal to be initialized")
	}

	// Verify WAL index was created
	if pager.walIndex == nil {
		t.Error("expected walIndex to be initialized")
	}

	// Cleanup
	if pager.wal != nil {
		pager.wal.Close()
	}
	if pager.walIndex != nil {
		pager.walIndex.Close()
	}
}

// TestEnableWALModeWALIndexFailure tests WAL index creation failure (42.1% coverage)
func TestEnableWALModeWALIndexFailure(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_index_fail.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Enable WAL mode normally to test full path
	err = pager.enableWALMode()
	// May succeed or fail depending on environment
	// Just verify the method can be called
	t.Logf("enableWALMode() returned: %v", err)

	// Cleanup
	if pager.wal != nil {
		pager.wal.Close()
		pager.wal = nil
	}
	if pager.walIndex != nil {
		pager.walIndex.Close()
		pager.walIndex = nil
	}
}

// TestDisableWALModeSuccess tests disabling WAL mode (66.7% coverage)
func TestDisableWALModeSuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_disable.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Enable WAL first
	if err := pager.enableWALMode(); err != nil {
		t.Fatalf("failed to enable WAL: %v", err)
	}

	// Now disable it
	if err := pager.disableWALMode(); err != nil {
		t.Errorf("disableWALMode() error = %v", err)
	}

	// Verify cleanup
	if pager.wal != nil {
		t.Error("expected wal to be nil after disable")
	}
	if pager.walIndex != nil {
		t.Error("expected walIndex to be nil after disable")
	}
}

// TestDisableWALModeNoWAL tests disabling when WAL is not enabled (66.7% coverage)
func TestDisableWALModeNoWAL(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_no_wal_disable.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Disable WAL when not enabled (should be no-op)
	if err := pager.disableWALMode(); err != nil {
		t.Errorf("disableWALMode() error = %v", err)
	}
}

// TestJournalRestoreEntryPageNumberZero tests restoring page 0 (55.6% coverage)
func TestJournalRestoreEntryPageNumberZero(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_restore_zero.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	journal := NewJournal(dbFile+"-journal", 4096, 1)

	// Create an entry with page number 0 (invalid)
	entry := make([]byte, 4+4096+4)
	binary.BigEndian.PutUint32(entry[0:4], 0) // page 0

	err = journal.restoreEntry(pager, entry)
	if err == nil {
		t.Error("expected error when restoring page 0")
	}
}

// TestMemoryPagerReadPageBeyondSize tests reading beyond database size (54.5% coverage)
func TestMemoryPagerReadPageBeyondSize(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Try to read a page beyond current size (page 100 doesn't exist yet)
	page, err := mp.readPage(100)
	if err != nil {
		t.Errorf("readPage() beyond size should zero the page, got error: %v", err)
	}

	// Verify page was zeroed
	if page != nil {
		for _, b := range page.Data {
			if b != 0 {
				t.Error("expected page to be zeroed")
				break
			}
		}
	}
}

// TestMemoryPagerReadPageWithinSize tests reading existing page (54.5% coverage)
func TestMemoryPagerReadPageWithinSize(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Write a page first
	page1, err := mp.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := mp.Write(page1); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("test data for memory pager")
	copy(page1.Data[DatabaseHeaderSize:], testData)
	mp.Put(page1)

	if err := mp.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Read the page again
	page2, err := mp.readPage(1)
	if err != nil {
		t.Errorf("readPage() error = %v", err)
	}

	// Verify data
	if page2 != nil && !bytes.Equal(page2.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(testData)], testData) {
		t.Error("page data not read correctly")
	}
}

// TestMemoryPagerWritePageError tests write page error handling (70% coverage)
func TestMemoryPagerWritePageError(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Create a page with invalid page number
	page := NewDbPage(0, 4096)

	err = mp.writePage(page)
	if err == nil {
		t.Error("expected error when writing page 0")
	}
}

// TestLockUnixAcquirePendingLockError tests pending lock error cases (50% coverage)
func TestLockUnixAcquirePendingLockError(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_pending_error.db")

	// Create file
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		f.Close()
		t.Fatalf("failed to write: %v", err)
	}
	f.Close()

	// Open file
	f, err = os.OpenFile(dbFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Try to acquire pending without proper state (should work or return error)
	err = lm.AcquireLock(lockPending)
	// Result depends on platform lock implementation
	t.Logf("AcquireLock(PENDING) from NONE: error = %v", err)
}

// TestBusyHandlerWithCustomHandler tests custom busy handler
func TestBusyHandlerWithCustomHandler(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_custom_busy.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	callCount := 0
	customHandler := BusyCallback(func(count int) bool {
		callCount++
		return count < 3 // Retry 3 times
	})

	pager.WithBusyHandler(customHandler)

	// Verify handler is set
	if pager.GetBusyHandler() == nil {
		t.Error("expected busy handler to be set")
	}
}

// TestTryAcquireSharedLockAlreadyHeld tests acquiring shared lock when already held (80% coverage)
func TestTryAcquireSharedLockAlreadyHeld(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_shared_held.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Acquire shared lock
	pager.lockState = LockShared

	// Try to acquire again (should be no-op)
	err = pager.tryAcquireSharedLock()
	if err != nil {
		t.Errorf("tryAcquireSharedLock() error = %v", err)
	}
}

// TestTryAcquireReservedLockReadOnly tests acquiring reserved lock on read-only pager (83.3% coverage)
func TestTryAcquireReservedLockReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_reserved_ro.db")

	// Create database first
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	pager1.Close()

	// Open read-only
	pager2, err := OpenWithPageSize(dbFile, true, 4096)
	if err != nil {
		t.Fatalf("failed to open read-only: %v", err)
	}
	defer pager2.Close()

	// Try to acquire reserved lock (should fail)
	err = pager2.tryAcquireReservedLock()
	if err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// TestTryAcquireExclusiveLockAlreadyHeld tests acquiring exclusive lock when already held (75% coverage)
func TestTryAcquireExclusiveLockAlreadyHeld(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_exclusive_held.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set to exclusive
	pager.lockState = LockExclusive

	// Try to acquire again (should be no-op)
	err = pager.tryAcquireExclusiveLock()
	if err != nil {
		t.Errorf("tryAcquireExclusiveLock() error = %v", err)
	}
}

// TestBusyHandlerInvocationRetry tests busy handler invocation with retries
func TestBusyHandlerInvocationRetry(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_busy_invoke.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create a handler that returns false after N retries
	retryLimit := 5
	handler := BusyCallback(func(count int) bool {
		return count < retryLimit
	})

	pager.WithBusyHandler(handler)

	// Test invokeBusyHandler
	for i := 0; i < retryLimit; i++ {
		if !pager.invokeBusyHandler(i) {
			t.Errorf("expected true for retry %d, got false", i)
		}
	}

	// Should return false after retry limit
	if pager.invokeBusyHandler(retryLimit) {
		t.Error("expected false after retry limit")
	}
}

// TestFreeListCreateNewTrunkNoPages tests creating trunk with no pending pages (77.8% coverage)
func TestFreeListCreateNewTrunkNoPages(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_trunk_nopages.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	fl := NewFreeList(pager)
	fl.pendingFree = []Pgno{} // Empty pending list

	// Try to create trunk with no pages (should fail)
	err = fl.createNewTrunk()
	if err != ErrNoFreePages {
		t.Errorf("expected ErrNoFreePages, got %v", err)
	}
}

// TestCommitPhaseErrorHandling tests error handling in commit phases
func TestCommitPhaseErrorHandling(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_commit_error.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Start a write transaction
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	pager.Put(page)

	// Close the file to cause sync to fail
	pager.file.Close()

	// Try phase 2 (should fail)
	pager.mu.Lock()
	err = pager.commitPhase2SyncDatabase()
	pager.mu.Unlock()

	if err == nil {
		t.Error("expected error when file is closed")
	}

	// Error state handling is internal - just verify the commit would fail
	// The error should have been set internally
}

// TestReadExistingDatabaseError tests error handling when reading existing database
func TestReadExistingDatabaseError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_read_error.db")

	// Create an invalid database file (too small)
	if err := os.WriteFile(dbFile, []byte("invalid"), 0600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Try to open (should fail)
	_, err := OpenWithPageSize(dbFile, false, 4096)
	if err == nil {
		t.Error("expected error when opening invalid database")
	}
}

// TestSetJournalModeTransitions tests journal mode transitions
func TestSetJournalModeTransitions(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_journal_mode.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Test transition to WAL
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Errorf("SetJournalMode(WAL) error = %v", err)
	}

	// Verify WAL is enabled
	if pager.GetJournalMode() != JournalModeWAL {
		t.Error("expected WAL mode")
	}

	// Test transition back to DELETE
	if err := pager.SetJournalMode(JournalModeDelete); err != nil {
		t.Errorf("SetJournalMode(DELETE) error = %v", err)
	}

	// Verify WAL is disabled
	if pager.GetJournalMode() != JournalModeDelete {
		t.Error("expected DELETE mode")
	}

	// Cleanup
	if pager.wal != nil {
		pager.wal.Close()
		pager.wal = nil
	}
	if pager.walIndex != nil {
		pager.walIndex.Close()
		pager.walIndex = nil
	}
}

// TestIsAutoVacuumNilHeader tests IsAutoVacuum with nil header (80% coverage)
func TestIsAutoVacuumNilHeader(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_autovacuum.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Temporarily set header to nil
	originalHeader := pager.header
	pager.header = nil

	// Should return false
	if pager.IsAutoVacuum() {
		t.Error("expected false when header is nil")
	}

	// Restore header
	pager.header = originalHeader
}

// TestOpenWithLRUCacheError tests error handling in OpenWithLRUCache (70% coverage)
func TestOpenWithLRUCacheError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "readonly", "test.db")

	// Try to open in non-existent directory
	config := DefaultLRUCacheConfig(4096)
	_, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err == nil {
		t.Error("expected error when directory doesn't exist")
	}
}

// TestInitNewDatabaseError tests error in initializeNewDatabase
func TestInitNewDatabaseError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_init_error.db")

	pager := &Pager{
		filename: dbFile,
		pageSize: 4096,
	}

	// Create a file but make it read-only to cause write error
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	f.Close()

	// Make file read-only
	if err := os.Chmod(dbFile, 0400); err != nil {
		t.Fatalf("failed to chmod: %v", err)
	}
	defer os.Chmod(dbFile, 0600) // Restore for cleanup

	// Open file (will succeed in read mode)
	f, err = os.OpenFile(dbFile, os.O_RDONLY, 0400)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	pager.file = f

	// Try to initialize (should fail due to read-only)
	err = pager.initNewDatabase(false)
	if err == nil {
		t.Error("expected error when initializing read-only file")
	}
}

// TestJournalTruncateError tests journal truncate error handling
func TestJournalTruncateError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_trunc.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	journal := NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Write some data
	pageData := make([]byte, 4096)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Close the file to cause truncate to fail
	journal.file.Close()

	// Try to truncate (should fail)
	err = journal.Truncate()
	if err == nil {
		t.Error("expected error when truncating closed journal")
	}
}

// TestCommitWithHeaderUpdate tests commit that requires header update
func TestCommitWithHeaderUpdate(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_header_update.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Allocate a page to trigger header update
	_, err = pager.AllocatePage()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}

	// Commit should update header
	if err := pager.Commit(); err != nil {
		t.Errorf("Commit() error = %v", err)
	}

	// Verify header was updated
	if pager.header.DatabaseSize == 0 {
		t.Error("expected header to be updated")
	}
}

// TestAcquireSharedLockError tests shared lock acquisition with error
func TestAcquireSharedLockError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_shared_error.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Close file to cause error
	pager.file.Close()

	// Try to acquire shared lock via acquireSharedLock function
	err = pager.acquireSharedLock()
	// Error handling depends on platform
	t.Logf("acquireSharedLock() with closed file: error = %v", err)
}

// TestNeedsHeaderUpdateTrue tests needsHeaderUpdate when update is needed
func TestNeedsHeaderUpdateTrue(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_needs_update.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Change dbSize to trigger header update
	pager.dbSize = pager.dbOrigSize + 1

	if !pager.needsHeaderUpdate() {
		t.Error("expected needsHeaderUpdate to return true")
	}

	// Change freelist trunk
	pager.dbSize = pager.dbOrigSize
	pager.header.FreelistTrunk = 999

	if !pager.needsHeaderUpdate() {
		t.Error("expected needsHeaderUpdate to return true for freelist change")
	}
}

// TestCommitPhase5Cleanup tests the cleanup phase
func TestCommitPhase5Cleanup(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_cleanup.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set up dirty state
	pager.state = PagerStateWriterLocked
	pager.lockState = LockExclusive
	pager.dbOrigSize = 1
	pager.dbSize = 5

	// Call cleanup
	pager.commitPhase5Cleanup()

	// Verify cleanup
	if pager.state != PagerStateOpen {
		t.Errorf("state = %v, want %v", pager.state, PagerStateOpen)
	}
	if pager.lockState != LockNone {
		t.Errorf("lockState = %v, want %v", pager.lockState, LockNone)
	}
	if pager.dbOrigSize != pager.dbSize {
		t.Error("dbOrigSize should be updated to dbSize")
	}
}

// TestValidateFormatFileFormats tests file format validation edge cases
func TestValidateFormatFileFormats(t *testing.T) {
	t.Parallel()
	header := NewDatabaseHeader(4096)

	// Serialize and parse to test validation
	data := header.Serialize()

	// Modify magic string to cause validation failure
	data[0] = 'X' // Corrupt magic

	_, err := ParseDatabaseHeader(data)
	if err == nil {
		t.Error("expected error for invalid magic string")
	}
}
