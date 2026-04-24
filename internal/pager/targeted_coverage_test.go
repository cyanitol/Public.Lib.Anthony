// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestAcquireSharedLockWithRetryFailure tests when busy handler returns false
func TestAcquireSharedLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler that immediately returns false
	callCount := 0
	handler := BusyCallback(func(count int) bool {
		callCount++
		return false
	})
	pager.WithBusyHandler(handler)

	// Should succeed since there's no actual contention
	err = pager.acquireSharedLockWithRetry()
	if err != nil && err != ErrDatabaseLocked {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireReservedLockWithRetryFailure tests reserved lock with failing busy handler
func TestAcquireReservedLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_reserved_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler
	pager.WithBusyHandler(BusyCallback(func(count int) bool {
		return false
	}))

	// Begin write to allow reserved lock
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire reserved lock
	err = pager.acquireReservedLockWithRetry()
	if err != nil && err != ErrDatabaseLocked && err != ErrReadOnly {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireExclusiveLockWithRetryFailure tests exclusive lock with failing busy handler
func TestAcquireExclusiveLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_exclusive_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler
	pager.WithBusyHandler(BusyCallback(func(count int) bool {
		return false
	}))

	// Begin write
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire exclusive lock
	err = pager.acquireExclusiveLockWithRetry()
	if err != nil && err != ErrDatabaseLocked {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCacheRemoveLockedDirtyPage tests removing a dirty page
func TestCacheRemoveLockedDirtyPage(t *testing.T) {
	t.Parallel()
	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Add dirty page
	page := &DbPage{
		Pgno:  1,
		Data:  make([]byte, 4096),
		Flags: PageFlagDirty,
	}
	cache.Put(page)

	// Remove it
	cache.Remove(1)

	// Should be gone
	retrieved := cache.Get(1)
	if retrieved != nil {
		t.Error("page should have been removed")
	}
}

// TestCacheMarkDirtyCleanPage tests marking a clean page dirty
func TestCacheMarkDirtyCleanPage(t *testing.T) {
	t.Parallel()
	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Add clean page
	page := &DbPage{
		Pgno:  1,
		Data:  make([]byte, 4096),
		Flags: PageFlagClean,
	}
	cache.Put(page)

	// Verify it's clean
	dirtyPages := cache.GetDirtyPages()
	if len(dirtyPages) != 0 {
		t.Fatalf("should have 0 dirty pages, got %d", len(dirtyPages))
	}

	// Mark it dirty
	cache.MarkDirty(page)

	// Should now be dirty
	dirtyPages = cache.GetDirtyPages()
	if len(dirtyPages) != 1 {
		t.Errorf("should have 1 dirty page, got %d", len(dirtyPages))
	}
}

// TestFreeListProcessTrunkPageWithSpace tests trunk page with available space
func TestFreeListProcessTrunkPageWithSpace(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_trunk_space_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate some pages
	for i := 0; i < 5; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
	}

	// Setup trunk page with space
	trunkPgno := Pgno(2)
	trunk, err := pager.Get(trunkPgno)
	if err != nil {
		t.Fatalf("failed to get trunk: %v", err)
	}
	pager.Write(trunk)

	// Initialize trunk header with space
	binary.BigEndian.PutUint32(trunk.Data[0:4], 0) // next trunk
	binary.BigEndian.PutUint32(trunk.Data[4:8], 1) // 1 leaf page

	pager.Put(trunk)

	// Set as trunk
	fl := pager.freeList
	fl.firstTrunk = trunkPgno
	fl.pendingFree = []Pgno{50}

	// Process - should add to trunk
	maxLeaves := (int(pager.PageSize()) - FreeListTrunkHeaderSize) / 4
	err = fl.processTrunkPage(maxLeaves)
	if err != nil {
		t.Errorf("processTrunkPage failed: %v", err)
	}

	pager.Rollback()
}

// TestFreeListCreateNewTrunkSuccess tests successful trunk creation
func TestFreeListCreateNewTrunkSuccess(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_newtrunk_ok_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	fl := pager.freeList

	// Add pending pages
	fl.pendingFree = []Pgno{10, 11, 12}
	fl.firstTrunk = 5

	// Create new trunk
	err = fl.createNewTrunk()
	if err != nil {
		t.Errorf("createNewTrunk failed: %v", err)
	}

	// Verify trunk was created
	if fl.firstTrunk == 5 {
		t.Error("first trunk should have changed")
	}

	pager.Rollback()
}

// TestFreeListFlushPendingWithPages tests flushing with pending pages
func TestFreeListFlushPendingWithPages(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_flush_pages_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate pages
	for i := 0; i < 10; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
	}

	fl := pager.freeList

	// Add pending pages
	fl.pendingFree = []Pgno{3, 4, 5}

	// Flush
	err = fl.flushPending()
	if err != nil {
		t.Errorf("flushPending failed: %v", err)
	}

	pager.Rollback()
}

// TestFreeListIterateWithPages tests iteration over freelist
// tcAllocAndFreePages allocates n pages, freeing those at index >= freeFrom.
func tcAllocAndFreePages(t *testing.T, p *Pager, n, freeFrom int) {
	t.Helper()
	for i := 0; i < n; i++ {
		pgno, err := p.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
		if i >= freeFrom {
			p.freeList.Free(pgno)
		}
	}
}

func TestFreeListIterateWithPages(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_iterate_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}
	tcAllocAndFreePages(t, pager, 10, 5)
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	count := 0
	err = pager.freeList.Iterate(func(pgno Pgno) bool {
		count++
		return true
	})
	if err != nil {
		t.Errorf("iterate failed: %v", err)
	}
	t.Logf("iterated over %d free pages", count)
}

// TestFreeListVerifyValid tests verification of valid freelist
func TestFreeListVerifyValid(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_verify_valid_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate and free pages
	for i := 0; i < 5; i++ {
		pgno, _ := pager.AllocatePage()
		if i > 2 {
			pager.freeList.Free(pgno)
		}
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify
	err = pager.freeList.Verify()
	if err != nil {
		t.Errorf("verify failed: %v", err)
	}
}

// TestJournalOpenCreateNew tests opening a new journal
func TestJournalOpenCreateNew(t *testing.T) {
	t.Parallel()
	journalFile := "test_open_new.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)

	// Open should create new file
	err := journal.Open()
	if err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Verify file exists
	if !journal.Exists() {
		t.Error("journal file should exist")
	}

	// Verify it's open
	if !journal.IsOpen() {
		t.Error("journal should be open")
	}
}

// TestJournalWriteOriginalValidSize tests writing with correct size
func TestJournalWriteOriginalValidSize(t *testing.T) {
	t.Parallel()
	journalFile := "test_write_valid.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer journal.Close()

	// Write with correct size
	pageData := make([]byte, 4096)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	err := journal.WriteOriginal(1, pageData)
	if err != nil {
		t.Errorf("write should succeed: %v", err)
	}

	// Verify page count
	count := journal.GetPageCount()
	if count != 1 {
		t.Errorf("expected page count 1, got %d", count)
	}
}

// TestJournalRollbackSuccess tests successful rollback
func TestJournalRollbackSuccess(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_rollback_success.db")

	p := mustOpenPagerSized(t, dbFile, 4096)
	originalData := []byte("ROLLBACK SUCCESS")
	mustWriteDataToPage(t, p, 1, DatabaseHeaderSize, originalData)
	mustCommit(t, p)

	// Modify and rollback
	mustModifyPage(t, p, 1, DatabaseHeaderSize, []byte("CHANGED DATA"))
	mustRollback(t, p)

	// Verify original data is restored
	page := mustGetPage(t, p, 1)
	defer p.Put(page)
	readData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(originalData)])
	if readData != string(originalData) {
		t.Errorf("data not restored: got %q, want %q", readData, originalData)
	}
	p.Close()
}

// TestJournalFinalizeSuccess tests successful finalize
func TestJournalFinalizeSuccess(t *testing.T) {
	t.Parallel()
	journalFile := "test_finalize_ok.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write data
	pageData := make([]byte, 4096)
	journal.WriteOriginal(1, pageData)

	// Finalize
	err := journal.Finalize()
	if err != nil {
		t.Errorf("finalize failed: %v", err)
	}

	// File should be deleted
	if journal.Exists() {
		t.Error("journal should not exist after finalize")
	}
}

// TestJournalDeleteSuccess tests successful delete
func TestJournalDeleteSuccess(t *testing.T) {
	t.Parallel()
	journalFile := "test_delete_ok.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Delete
	err := journal.Delete()
	if err != nil {
		t.Errorf("delete failed: %v", err)
	}

	// Should not exist
	if journal.Exists() {
		t.Error("journal should not exist after delete")
	}

	// Should not be open
	if journal.IsOpen() {
		t.Error("journal should not be open after delete")
	}
}

// TestJournalIsValidAfterWrite tests validation after writing
func TestJournalIsValidAfterWrite(t *testing.T) {
	t.Parallel()
	journalFile := "test_valid_after_write.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write data
	pageData := make([]byte, 4096)
	journal.WriteOriginal(1, pageData)
	journal.Sync()
	journal.Close()

	// Check validity
	valid, err := journal.IsValid()
	if err != nil {
		t.Logf("IsValid returned error: %v", err)
	}
	t.Logf("Journal validity: %v", valid)
}

// TestParseDBHeaderValid tests parsing valid header
func TestParseDBHeaderValid(t *testing.T) {
	t.Parallel()
	// Create valid header
	header := make([]byte, 100)
	copy(header[0:16], []byte("SQLite format 3\x00"))
	binary.BigEndian.PutUint16(header[16:18], 4096) // page size
	header[18] = 1                                  // file format write version
	header[19] = 1                                  // file format read version

	parsed, err := ParseDatabaseHeader(header)
	if err != nil {
		t.Errorf("failed to parse valid header: %v", err)
	}

	if parsed.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", parsed.PageSize)
	}
}

// TestBusyHandlerMultipleRetries tests handler with multiple retries
func TestBusyHandlerMultipleRetries(t *testing.T) {
	t.Parallel()
	retries := 0
	maxRetries := 3

	handler := BusyCallback(func(count int) bool {
		retries++
		time.Sleep(1 * time.Millisecond)
		return count < maxRetries
	})

	// Simulate retries
	count := 0
	for handler.Busy(count) {
		count++
		if count > 10 {
			break
		}
	}

	if retries != maxRetries+1 {
		t.Errorf("expected %d retries, got %d", maxRetries+1, retries)
	}
}
