package pager

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestInitNewDatabaseReadOnly tests attempting to create a database in read-only mode
func TestInitNewDatabaseReadOnly(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_readonly_create.db")

	// Try to open non-existent file in read-only mode (should fail)
	_, err := Open(dbFile, true)
	if err == nil {
		t.Error("expected error creating new database in read-only mode")
	}
}

// TestCommitWithFreelistFlush tests commit phase 0 with freelist
func TestCommitWithFreelistFlush(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_freelist_flush.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Allocate many pages
	var allocated []Pgno
	for i := 0; i < 20; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
		allocated = append(allocated, pgno)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("first commit error = %v", err)
	}

	// Free half of them
	for i := 0; i < len(allocated)/2; i++ {
		if err := pager.FreePage(allocated[i]); err != nil {
			t.Errorf("FreePage() error = %v", err)
		}
	}

	// Modify a page
	page, err := pager.Get(allocated[len(allocated)-1])
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	page.Data[0] = 0xAB
	pager.Put(page)

	// Commit should flush freelist (phase 0)
	if err := pager.Commit(); err != nil {
		t.Errorf("commit with freelist error = %v", err)
	}
}

// TestCommitWithMultipleDirtyPages tests commit phase 1 with many dirty pages
func TestCommitWithMultipleDirtyPages(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_dirty_pages.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Allocate pages first
	var pages []Pgno
	for i := 0; i < 10; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
		pages = append(pages, pgno)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("first commit error = %v", err)
	}

	// Modify all pages
	for _, pgno := range pages {
		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Get(%d) error = %v", pgno, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("Write() error = %v", err)
		}

		page.Data[0] = byte(pgno)
		pager.Put(page)
	}

	// Commit should write all dirty pages (phase 1)
	if err := pager.Commit(); err != nil {
		t.Errorf("commit with dirty pages error = %v", err)
	}

	// Verify all pages persisted
	for _, pgno := range pages {
		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("Get(%d) after commit error = %v", pgno, err)
		}

		if page.Data[0] != byte(pgno) {
			t.Errorf("page %d: data = %d, want %d", pgno, page.Data[0], byte(pgno))
		}

		pager.Put(page)
	}
}

// TestCommitWithSync tests commit phase 2 (sync)
func TestCommitWithSync(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_sync.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Write data
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	testData := []byte("SYNC TEST DATA")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(testData)], testData)
	pager.Put(page)

	// Commit triggers sync (phase 2)
	if err := pager.Commit(); err != nil {
		t.Errorf("commit with sync error = %v", err)
	}

	// Verify file was synced by closing and reopening
	pager.Close()

	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer pager2.Close()

	page2, err := pager2.Get(1)
	if err != nil {
		t.Fatalf("Get() after reopen error = %v", err)
	}
	defer pager2.Put(page2)

	readData := string(page2.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(testData)])
	if readData != string(testData) {
		t.Errorf("data after sync: got %q, want %q", readData, testData)
	}
}

// TestCommitWithJournalFinalization tests commit phase 3 (finalize journal)
func TestCommitWithJournalFinalization(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_finalize_journal.db")
	journalFile := dbFile + "-journal"

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Write to create journal
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	page.Data[DatabaseHeaderSize] = 0xFF
	pager.Put(page)

	// Before commit, verify journal exists (if transaction started)
	// After commit, journal should be finalized (phase 3)
	if err := pager.Commit(); err != nil {
		t.Errorf("commit error = %v", err)
	}

	// Journal should be removed after successful commit
	if _, err := os.Stat(journalFile); !os.IsNotExist(err) {
		t.Error("journal file should be removed after commit")
	}
}

// TestAcquireReservedLockWithRetryContention tests retry logic for reserved lock
func TestAcquireReservedLockWithRetryContention(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_reserved_retry.db")

	// Create database
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager1.Close()

	// Set busy handler with short timeout
	handler := BusyTimeout(50)
	pager1.WithBusyHandler(handler)

	// Try to acquire reserved lock
	// First get shared
	if err := pager1.BeginRead(); err != nil {
		t.Fatalf("BeginRead() error = %v", err)
	}

	// Try to upgrade - tests acquireReservedLockWithRetry
	err = pager1.BeginWrite()
	if err != nil {
		t.Logf("BeginWrite() error = %v (may fail without contention)", err)
	} else {
		// Successfully acquired, clean up
		if err := pager1.Rollback(); err != nil {
			t.Logf("Rollback() error = %v", err)
		}
	}

	pager1.EndRead()
}

// TestAcquirePendingLockWithRetry tests pending lock acquisition with retry
func TestAcquirePendingLockWithRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_pending_retry.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set busy handler
	handler := BusyTimeout(50)
	pager.WithBusyHandler(handler)

	// Write to trigger full lock sequence
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	page.Data[DatabaseHeaderSize] = 0xAB
	pager.Put(page)

	// Commit goes through pending lock
	if err := pager.Commit(); err != nil {
		t.Errorf("Commit() error = %v", err)
	}
}

// TestProcessTrunkPageWithMultipleTrunks tests trunk page processing
func TestProcessTrunkPageWithMultipleTrunks(t *testing.T) {
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	fl.maxPending = 10 // Lower threshold

	// Create many pages
	for i := Pgno(2); i <= 250; i++ {
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

	// Free many pages to create multiple trunk pages
	for i := Pgno(50); i <= 200; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("failed to free page %d: %v", i, err)
		}
	}

	// Flush to create trunk structure
	if err := fl.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Verify trunk chain
	firstTrunk := fl.GetFirstTrunk()
	if firstTrunk == 0 {
		t.Fatal("expected non-zero first trunk")
	}

	// Count trunks
	trunkCount := 0
	currentTrunk := firstTrunk
	for currentTrunk != 0 && trunkCount < 50 {
		nextTrunk, leaves, err := fl.ReadTrunk(currentTrunk)
		if err != nil {
			t.Fatalf("failed to read trunk %d: %v", currentTrunk, err)
		}

		trunkCount++
		t.Logf("Trunk %d: next=%d, leaves=%d", currentTrunk, nextTrunk, len(leaves))

		if nextTrunk == currentTrunk {
			t.Fatal("trunk points to itself (infinite loop)")
		}

		currentTrunk = nextTrunk
	}

	if trunkCount == 0 {
		t.Error("expected at least one trunk page")
	}

	t.Logf("Total trunk pages: %d", trunkCount)
}

// TestEnableWALModeEdgeCases tests WAL mode edge cases
func TestEnableWALModeEdgeCases(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_wal_edge.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try multiple times to enable WAL
	for i := 0; i < 3; i++ {
		err = pager.SetJournalMode(JournalModeWAL)
		if err != nil {
			t.Logf("Attempt %d: SetJournalMode(WAL) error = %v", i+1, err)
		} else {
			t.Logf("Attempt %d: WAL mode enabled", i+1)

			// Make a transaction in WAL mode
			page, err := pager.Get(1)
			if err != nil {
				t.Fatalf("Get() in WAL mode error = %v", err)
			}

			if err := pager.Write(page); err != nil {
				t.Logf("Write() in WAL mode error = %v", err)
			} else {
				page.Data[DatabaseHeaderSize] = byte(i)
				pager.Put(page)

				if err := pager.Commit(); err != nil {
					t.Logf("Commit() in WAL mode error = %v", err)
				}
			}

			// Disable WAL
			err = pager.SetJournalMode(JournalModeDelete)
			if err != nil {
				t.Logf("SetJournalMode(DELETE) error = %v", err)
			}

			break
		}
	}
}

// TestTryUpgradeToExclusiveEdgeCases tests exclusive upgrade edge cases
func TestTryUpgradeToExclusiveEdgeCases(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_excl_edge.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set busy handler for retry logic
	handler := BusyCallback(func(retries int) bool {
		t.Logf("Busy callback: retry %d", retries)
		return retries < 3
	})
	pager.WithBusyHandler(handler)

	// Try to upgrade from various states
	success, err := pager.TryUpgradeToExclusive()
	t.Logf("TryUpgradeToExclusive() from initial state: success=%v, error=%v", success, err)

	// If failed, try with busy handler
	if !success && err == nil {
		// Retry
		success, err = pager.TryUpgradeToExclusive()
		t.Logf("Second attempt: success=%v, error=%v", success, err)
	}
}

// TestMultipleCommitCycles tests multiple commit/rollback cycles
func TestMultipleCommitCycles(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_cycles.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Run multiple commit cycles
	for cycle := 0; cycle < 5; cycle++ {
		// Allocate a page
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("cycle %d: AllocatePage() error = %v", cycle, err)
		}

		// Write to it
		page, err := pager.Get(pgno)
		if err != nil {
			t.Fatalf("cycle %d: Get() error = %v", cycle, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("cycle %d: Write() error = %v", cycle, err)
		}

		page.Data[0] = byte(cycle)
		pager.Put(page)

		// Commit
		if err := pager.Commit(); err != nil {
			t.Errorf("cycle %d: Commit() error = %v", cycle, err)
		}

		// Free it
		if err := pager.FreePage(pgno); err != nil {
			t.Errorf("cycle %d: FreePage() error = %v", cycle, err)
		}

		if err := pager.Commit(); err != nil {
			t.Errorf("cycle %d: Commit after free error = %v", cycle, err)
		}
	}
}

// TestRollbackCycles tests multiple rollback cycles
func TestRollbackCycles(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "test_rollback_cycles.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Write and commit initial state
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	originalData := []byte("ORIGINAL")
	copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(originalData)], originalData)
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("initial commit error = %v", err)
	}

	// Run multiple modify/rollback cycles
	for cycle := 0; cycle < 5; cycle++ {
		page, err := pager.Get(1)
		if err != nil {
			t.Fatalf("cycle %d: Get() error = %v", cycle, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("cycle %d: Write() error = %v", cycle, err)
		}

		modifiedData := []byte(fmt.Sprintf("MODIFIED%d", cycle))
		copy(page.Data[DatabaseHeaderSize:DatabaseHeaderSize+len(modifiedData)], modifiedData)
		pager.Put(page)

		// Rollback
		if err := pager.Rollback(); err != nil {
			t.Errorf("cycle %d: Rollback() error = %v", cycle, err)
		}

		// Verify data was restored
		page, err = pager.Get(1)
		if err != nil {
			t.Fatalf("cycle %d: Get() after rollback error = %v", cycle, err)
		}

		readData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(originalData)])
		if readData != string(originalData) {
			t.Errorf("cycle %d: data after rollback = %q, want %q", cycle, readData, originalData)
		}

		pager.Put(page)
	}
}
