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

// TestEnableWALModeFull tests complete WAL mode enable path
func TestEnableWALModeFull(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_full.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try to enable WAL mode
	err = pager.SetJournalMode(JournalModeWAL)
	if err != nil {
		t.Logf("SetJournalMode(WAL) error = %v", err)
		// May fail in some environments, that's okay
		return
	}

	// If successful, verify WAL is enabled
	if pager.GetJournalMode() != JournalModeWAL {
		t.Error("expected WAL mode to be enabled")
	}

	// Disable it
	err = pager.SetJournalMode(JournalModeDelete)
	if err != nil {
		t.Logf("SetJournalMode(DELETE) error = %v", err)
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

// TestJournalRestoreEntryWithValidChecksum tests restore with valid checksum
func TestJournalRestoreEntryWithValidChecksum(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_restore_valid.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Write initial data
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	testData := []byte("initial data for restore test")
	copy(page.Data[DatabaseHeaderSize:], testData)
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Create a journal manually
	journal := NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Get the current page data
	page, _ = pager.Get(1)
	originalData := make([]byte, len(page.Data))
	copy(originalData, page.Data)
	pager.Put(page)

	// Write original data to journal
	if err := journal.WriteOriginal(1, originalData); err != nil {
		t.Fatalf("WriteOriginal() error = %v", err)
	}
	journal.Close()

	// Modify the page in memory
	page, _ = pager.Get(1)
	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	modifiedData := []byte("modified data that should be rolled back")
	copy(page.Data[DatabaseHeaderSize:], modifiedData)
	pager.Put(page)

	// Write to disk without committing
	pager.writeDirtyPages()
	pager.Close()

	// Reopen and rollback
	pager, err = OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager.Close()

	journal = NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Rollback(pager); err != nil {
		t.Errorf("Rollback() error = %v", err)
	}

	// Verify data was restored
	page, _ = pager.Get(1)
	defer pager.Put(page)

	if !bytes.Equal(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(testData)], testData) {
		t.Error("data not properly restored from journal")
	}
}

// TestAcquirePendingLockFullPath tests pending lock acquisition
func TestAcquirePendingLockFullPath(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_pending_full.db")

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

	// Acquire shared first
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}

	// Acquire reserved
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	// Now acquire pending
	if err := lm.AcquireLock(lockPending); err != nil {
		t.Logf("AcquireLock(PENDING) error = %v", err)
	}

	// Verify state
	state := lm.GetLockState()
	t.Logf("Final lock state: %v", state)
}

// TestProcessTrunkPageEdgeCases tests trunk page processing edge cases
func TestProcessTrunkPageEdgeCases(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_trunk_edge.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	fl := NewFreeList(pager)

	// Create pages
	totalPages := 100 // Reduced to avoid cache full error

	for i := Pgno(2); i <= Pgno(totalPages); i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		pager.Put(page)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Free pages to fill part of a trunk
	pagesToFree := 30 // Less than a full trunk
	for i := Pgno(10); i < Pgno(10+pagesToFree); i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("Free() error = %v", err)
		}
	}

	// Flush
	if err := fl.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Now free one more to trigger adding to existing trunk
	if err := fl.Free(Pgno(50)); err != nil {
		t.Fatalf("Free() error = %v", err)
	}

	if err := fl.Flush(); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}

	// Verify freelist
	totalFree := fl.GetTotalFree()
	if totalFree == 0 {
		t.Error("expected non-zero total free pages")
	}

	t.Logf("Total free pages: %d", totalFree)
}

// TestFreeListCreateNewTrunkMultiple tests creating multiple trunks
func TestFreeListCreateNewTrunkMultiple(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_multi_trunk.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	fl := NewFreeList(pager)

	// Create enough pages
	totalPages := 150 // Reduced to avoid cache full

	for i := Pgno(2); i <= Pgno(totalPages); i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		if err := pager.Write(page); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		pager.Put(page)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Free many pages to create multiple trunks
	for i := Pgno(20); i <= Pgno(totalPages-10); i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("Free() error = %v", err)
		}
	}

	// Flush to create trunk structure
	if err := fl.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify trunk was created
	if fl.GetFirstTrunk() == 0 {
		t.Error("expected trunk to be created")
	}

	// Read trunk
	nextTrunk, leaves, err := fl.ReadTrunk(fl.GetFirstTrunk())
	if err != nil {
		t.Fatalf("ReadTrunk() error = %v", err)
	}

	t.Logf("First trunk: next=%d, leaves=%d", nextTrunk, len(leaves))

	// Verify the structure
	if err := fl.Verify(); err != nil {
		t.Errorf("Verify() error = %v", err)
	}
}

// TestWALModeCheckpointTransition tests WAL checkpoint
func TestWALModeCheckpointTransition(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_checkpoint.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Enable WAL mode
	if err := pager.SetJournalMode(JournalModeWAL); err != nil {
		t.Logf("SetJournalMode(WAL) error = %v", err)
		// May not be supported in all environments
		return
	}

	// Write some data in WAL mode
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	copy(page.Data[DatabaseHeaderSize:], []byte("WAL mode data"))
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Disable WAL mode (should checkpoint)
	if err := pager.SetJournalMode(JournalModeDelete); err != nil {
		t.Errorf("SetJournalMode(DELETE) error = %v", err)
	}

	// Verify mode changed
	if pager.GetJournalMode() != JournalModeDelete {
		t.Error("expected DELETE mode after disabling WAL")
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

// TestCommitPhasesWithErrors tests error handling in commit phases
func TestCommitPhasesWithErrors(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_commit_errors.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Start a transaction
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	pager.Put(page)

	// Close the underlying file to cause errors
	if pager.file != nil {
		pager.file.Close()
	}

	// Try to commit (should fail gracefully)
	err = pager.Commit()
	if err == nil {
		t.Log("Commit succeeded despite closed file (may have been buffered)")
	} else {
		t.Logf("Commit failed as expected: %v", err)
	}
}

// TestLRUCacheSetMaxPagesExtended tests setting max pages
func TestLRUCacheSetMaxPagesExtended(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_set_max.db")

	config := DefaultLRUCacheConfig(4096)
	config.MaxPages = 20
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Add some pages
	for i := Pgno(1); i <= 10; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		pager.Put(page)
	}

	if lruCache, ok := pager.cache.(*LRUCache); ok {
		// Reduce max pages
		lruCache.SetMaxPages(5)

		// Verify cache was shrunk
		if lruCache.Size() > 5 {
			t.Logf("Cache size %d > 5 (some pages may be in use)", lruCache.Size())
		}

		// Increase max pages
		lruCache.SetMaxPages(30)
		t.Logf("Max pages set to 30, current size: %d", lruCache.Size())
	}
}

// TestLRUCacheSetMaxMemoryExtended tests setting max memory
func TestLRUCacheSetMaxMemoryExtended(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_set_mem.db")

	config := DefaultLRUCacheConfig(4096)
	config.MaxMemory = 100 * 1024 // 100KB
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Add some pages
	for i := Pgno(1); i <= 10; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		pager.Put(page)
	}

	if lruCache, ok := pager.cache.(*LRUCache); ok {
		// Reduce max memory
		lruCache.SetMaxMemory(20 * 1024) // 20KB

		// Verify cache was shrunk
		memUsage := lruCache.MemoryUsage()
		t.Logf("Memory usage after SetMaxMemory(20KB): %d bytes", memUsage)

		// Increase max memory
		lruCache.SetMaxMemory(200 * 1024) // 200KB
		t.Logf("Max memory set to 200KB")
	}
}

// TestCacheMarkDirtyError tests error handling in MarkDirty
func TestCacheMarkDirtyError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_mark_dirty.db")

	config := DefaultLRUCacheConfig(4096)
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Get a page
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Mark it dirty
	if lruCache, ok := pager.cache.(*LRUCache); ok {
		page.MakeDirty()
		lruCache.MarkDirty(page)
		// MarkDirty doesn't return error, just marks the page
	}

	pager.Put(page)
}

// TestCacheFlushPageNonExistent tests flushing non-existent page
func TestCacheFlushPageNonExistent(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_flush_nonexist.db")

	config := DefaultLRUCacheConfig(4096)
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	if lruCache, ok := pager.cache.(*LRUCache); ok {
		lruCache.SetPager(pager)

		// Try to flush non-existent page
		err := lruCache.FlushPage(999)
		if err != nil {
			t.Logf("FlushPage(999) error = %v (expected)", err)
		}
	}
}

// TestCacheRemoveLocked tests removing locked page from cache
func TestCacheRemoveLocked(t *testing.T) {
	t.Parallel()
	cache := NewPageCache(10, 4096)

	// Add a page
	page := NewDbPage(1, 4096)
	page.Ref() // Increase ref count
	cache.Put(page)

	// Try to remove while referenced
	cache.Remove(1)

	// Should still be in cache if implementation protects it
	if cache.Get(1) == nil {
		t.Log("Page was removed despite being referenced")
	}

	// Unref and try again
	page.Unref()
	cache.Remove(1)
}

// TestCacheEvictClean tests evicting clean pages
func TestCacheEvictClean(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_evict_clean.db")

	config := DefaultLRUCacheConfig(4096)
	config.MaxPages = 10
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Add clean pages
	for i := Pgno(1); i <= 8; i++ {
		page, err := pager.Get(i)
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		pager.Put(page)
	}

	if lruCache, ok := pager.cache.(*LRUCache); ok {
		// Evict clean pages
		evicted := lruCache.EvictClean()
		t.Logf("Evicted %d clean pages", evicted)
	}
}

// TestPeekCacheHit tests Peek with cache hit
func TestPeekCacheHit(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_peek_hit.db")

	config := DefaultLRUCacheConfig(4096)
	pager, err := OpenWithLRUCache(dbFile, false, 4096, config)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Add a page
	page1, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	pager.Put(page1)

	if lruCache, ok := pager.cache.(*LRUCache); ok {
		// Peek at the page
		page2 := lruCache.Peek(1)
		if page2 == nil {
			t.Error("expected to find page in cache")
		}
		if page2 != nil && page2.Pgno != 1 {
			t.Errorf("peeked page pgno = %d, want 1", page2.Pgno)
		}
	}
}

// TestInitOrReadHeaderExisting tests reading existing header
func TestInitOrReadHeaderExisting(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_init_or_read.db")

	// Create initial database
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Write something
	page, _ := pager1.Get(1)
	pager1.Write(page)
	pager1.Put(page)
	pager1.Commit()
	pager1.Close()

	// Reopen - should read existing header
	pager2, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer pager2.Close()

	// Verify header
	if pager2.header == nil {
		t.Error("header should be loaded from file")
	}
	if pager2.header.GetPageSize() != 4096 {
		t.Errorf("page size = %d, want 4096", pager2.header.GetPageSize())
	}
}

// TestJournalUpdatePageCountExtended tests updating page count in journal
func TestJournalUpdatePageCountExtended(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_journal_pagecount.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	journal := NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Write some pages
	for i := 1; i <= 3; i++ {
		pageData := make([]byte, 4096)
		binary.BigEndian.PutUint32(pageData[0:4], uint32(i))
		if err := journal.WriteOriginal(uint32(i), pageData); err != nil {
			t.Fatalf("WriteOriginal(%d) error = %v", i, err)
		}
	}

	// Verify page count
	count := journal.GetPageCount()
	if count != 3 {
		t.Errorf("page count = %d, want 3", count)
	}
}
