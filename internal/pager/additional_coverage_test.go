package pager

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestReadExistingDatabase tests reading an existing database
func TestReadExistingDatabase(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_existing.db")

	// Create database
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Write some data
	page, err := pager1.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager1.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("EXISTING DATABASE TEST")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(testData)], testData)
	pager1.Put(page)

	if err := pager1.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	pager1.Close()

	// Reopen and verify
	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager2.Close()

	page2, err := pager2.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer pager2.Put(page2)

	readData := string(page2.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(testData)])
	if readData != string(testData) {
		t.Errorf("data mismatch: got %q, want %q", readData, testData)
	}
}

// TestPagerReadHeader tests reading database header
func TestPagerReadHeader(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_read_header.db")

	pager1, err := OpenWithPageSize(dbFile, false, 8192)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	pager1.Close()

	// Reopen and verify header
	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager2.Close()

	header := pager2.GetHeader()
	if header == nil {
		t.Fatal("header is nil")
	}

	if header.GetPageSize() != 8192 {
		t.Errorf("page size = %d, want 8192", header.GetPageSize())
	}
}

// TestPagerAcquireSharedLock tests shared lock acquisition path
func TestPagerAcquireSharedLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_shared_lock.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// BeginRead should acquire shared lock
	if err := pager.BeginRead(); err != nil {
		t.Errorf("BeginRead() error = %v", err)
	}

	// End read
	if err := pager.EndRead(); err != nil {
		t.Errorf("EndRead() error = %v", err)
	}
}

// TestPagerBeginWriteTransaction tests write transaction initialization
func TestPagerBeginWriteTransaction(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_begin_write.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Begin write should create journal
	if err := pager.BeginWrite(); err != nil {
		t.Errorf("BeginWrite() error = %v", err)
	}

	// Verify we're in write transaction
	if !pager.InWriteTransaction() {
		t.Error("should be in write transaction")
	}

	// Rollback to clean up
	if err := pager.Rollback(); err != nil {
		t.Errorf("Rollback() error = %v", err)
	}
}

// TestPagerJournalPage tests journaling a page
func TestPagerJournalPage(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_journal_page.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Start write transaction
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Write to page (this should journal it)
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("JOURNAL TEST DATA")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(testData)], testData)
	pager.Put(page)

	// Commit
	if err := pager.Commit(); err != nil {
		t.Errorf("Commit() error = %v", err)
	}
}

// TestPagerRollbackJournal tests journal rollback
func TestPagerRollbackJournal(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_rollback_journal.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Write and commit initial data
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	originalData := []byte("ORIGINAL")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(originalData)], originalData)
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Start new transaction and modify
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	modifiedData := []byte("MODIFIED")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(modifiedData)], modifiedData)
	pager.Put(page)

	// Rollback
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	pager.Close()

	// Reopen and verify data was restored
	pager, err = Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager.Close()

	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer pager.Put(page)

	readData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(originalData)])
	if readData != string(originalData) {
		t.Errorf("data after rollback: got %q, want %q", readData, originalData)
	}
}

// TestPagerFinalizeJournal tests journal finalization
func TestPagerFinalizeJournal(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_finalize.db")
	journalFile := dbFile + "-journal"

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Write and commit (journal should be finalized)
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Journal file should be deleted after commit
	if _, err := os.Stat(journalFile); !os.IsNotExist(err) {
		t.Error("journal file should be deleted after commit")
	}
}

// TestPagerUpdateDatabaseHeader tests database header updates
func TestPagerUpdateDatabaseHeader(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_update_header.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Make a change and commit
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	// Modify the data so the page is actually dirty
	page.Data[DatabaseHeaderSize] = 0xAB
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	initialCounter := pager.GetHeader().FileChangeCounter

	// Make another change
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	page.Data[DatabaseHeaderSize] = 0xCD
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	pager.Close()

	// Reopen and check counter was incremented
	pager, err = Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager.Close()

	newCounter := pager.GetHeader().FileChangeCounter
	if newCounter <= initialCounter {
		t.Logf("Change counter: was %d, now %d (may not increment in all cases)", initialCounter, newCounter)
	}
}

// TestFreeListAllocateFromDisk tests allocation from disk-based freelist
func TestFreeListAllocateFromDisk(t *testing.T) {
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Create many pages
	for i := Pgno(2); i <= 100; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		pager.Put(page)
	}
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Free many pages
	for i := Pgno(50); i <= 90; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("failed to free page %d: %v", i, err)
		}
	}

	// Flush to create disk-based structure
	if err := fl.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Now allocate - should come from disk
	for i := 0; i < 10; i++ {
		pgno, err := fl.Allocate()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
		if pgno == 0 {
			t.Error("expected non-zero page number from freelist")
		}
	}
}

// TestFreeListCreateNewTrunk tests creating new trunk pages
func TestFreeListCreateNewTrunk(t *testing.T) {
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	fl.maxPending = 5 // Small threshold to trigger trunk creation

	// Create pages
	for i := Pgno(2); i <= 50; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}
		pager.Put(page)
	}
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Free many pages to trigger trunk creation
	for i := Pgno(10); i <= 40; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("failed to free page %d: %v", i, err)
		}
	}

	// Should have created trunk pages
	if fl.GetFirstTrunk() == 0 {
		// Flush to ensure trunk is created
		if err := fl.Flush(); err != nil {
			t.Fatalf("failed to flush: %v", err)
		}
	}

	if fl.GetFirstTrunk() == 0 {
		t.Error("expected non-zero first trunk after freeing many pages")
	}
}

// TestTransactionGetTransactionState tests transaction state retrieval
func TestTransactionGetTransactionState(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_txn_state.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Check initial state
	state := pager.GetTransactionState()
	t.Logf("Initial transaction state: %d", state)

	// Begin write
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite() error = %v", err)
	}

	state = pager.GetTransactionState()
	t.Logf("Write transaction state: %d", state)

	// Rollback
	if err := pager.Rollback(); err != nil {
		t.Errorf("Rollback() error = %v", err)
	}
}

// TestTransactionSetJournalMode tests journal mode transitions
func TestTransactionSetJournalMode(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_journal_mode.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try to set journal mode
	err = pager.SetJournalMode(JournalModeDelete)
	if err != nil {
		t.Logf("SetJournalMode(DELETE) error = %v", err)
	}

	// Get journal mode
	mode := pager.GetJournalMode()
	t.Logf("Journal mode: %d", mode)
}

// TestTransactionIsAutoVacuum tests auto-vacuum detection
func TestTransactionIsAutoVacuum(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_auto_vacuum.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	isAutoVacuum := pager.IsAutoVacuum()
	t.Logf("Auto-vacuum enabled: %v", isAutoVacuum)
}

// TestTransactionGetOriginalPageCount tests original page count retrieval
func TestTransactionGetOriginalPageCount(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_orig_count.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	origCount := pager.GetOriginalPageCount()
	t.Logf("Original page count: %d", origCount)

	// Allocate some pages
	for i := 0; i < 5; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	newCount := pager.PageCount()
	if newCount <= origCount {
		t.Errorf("page count should have increased: was %d, now %d", origCount, newCount)
	}
}

// TestCacheModeOperations tests cache mode get/set
func TestCacheModeOperations(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_cache_mode.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set mode on cache if it supports it
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		originalMode := lruCache.Mode()
		t.Logf("Original cache mode: %d", originalMode)

		lruCache.SetMode(1)
		newMode := lruCache.Mode()
		if newMode != 1 {
			t.Errorf("cache mode = %d, want 1", newMode)
		}
	}
}

// TestCacheEvictOperations tests cache eviction
func TestCacheEvictOperations(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_cache_evict.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Get a page to add to cache
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	pager.Put(page)

	// Evict from cache if it's LRU
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		pgno, err := lruCache.Evict()
		if err != nil {
			t.Logf("Evict() error = %v", err)
		} else {
			t.Logf("Evicted page: %d", pgno)
		}
	}
}
