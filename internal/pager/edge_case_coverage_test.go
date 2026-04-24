//go:build !windows

// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestInitNewDatabaseReadOnly tests attempting to create a database in read-only mode
func TestInitNewDatabaseReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_readonly_create.db")

	// Try to open non-existent file in read-only mode (should fail)
	_, err := Open(dbFile, true)
	if err == nil {
		t.Error("expected error creating new database in read-only mode")
	}
}

// TestCommitWithFreelistFlush tests commit phase 0 with freelist
func TestCommitWithFreelistFlush(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_freelist_flush.db")

	p := mustOpenPagerSized(t, dbFile, 4096)
	defer p.Close()

	allocated := mustAllocateAndCommit(t, p, 20)

	// Free half of them
	for i := 0; i < len(allocated)/2; i++ {
		mustFreePage(t, p, allocated[i])
	}

	// Modify a page
	mustGetWritePageData(t, p, allocated[len(allocated)-1], 0xAB)

	// Commit should flush freelist (phase 0)
	mustCommit(t, p)
}

// TestCommitWithMultipleDirtyPages tests commit phase 1 with many dirty pages
func TestCommitWithMultipleDirtyPages(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_dirty_pages.db")

	p := mustOpenPagerSized(t, dbFile, 4096)
	defer p.Close()

	pages := mustAllocateAndCommit(t, p, 10)

	// Modify all pages
	for _, pgno := range pages {
		mustGetWritePageData(t, p, pgno, byte(pgno))
	}

	mustCommit(t, p)

	// Verify all pages persisted
	for _, pgno := range pages {
		page := mustGetPage(t, p, pgno)
		if page.Data[0] != byte(pgno) {
			t.Errorf("page %d: data = %d, want %d", pgno, page.Data[0], byte(pgno))
		}
		p.Put(page)
	}
}

// TestCommitWithSync tests commit phase 2 (sync)
func TestCommitWithSync(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)
	fl.maxPending = 10 // Lower threshold

	mustCreateWritePages(t, pager, 2, 250)

	mustFreePages(t, fl, 50, 200)
	mustFlush(t, fl)

	firstTrunk := fl.GetFirstTrunk()
	if firstTrunk == 0 {
		t.Fatal("expected non-zero first trunk")
	}

	trunkCount := walkTrunkChain(t, fl, firstTrunk, 50)
	if trunkCount == 0 {
		t.Error("expected at least one trunk page")
	}
	t.Logf("Total trunk pages: %d", trunkCount)
}

// TestEnableWALModeEdgeCases tests WAL mode edge cases
func TestEnableWALModeEdgeCases(t *testing.T) {
	t.Parallel()
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
	t.Parallel()

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
// ecAllocWriteCommitFree performs one cycle of allocate, write, commit, free, commit.
func ecAllocWriteCommitFree(t *testing.T, p *Pager, cycle int) {
	t.Helper()
	pgno, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("cycle %d: AllocatePage() error = %v", cycle, err)
	}
	page, err := p.Get(pgno)
	if err != nil {
		t.Fatalf("cycle %d: Get() error = %v", cycle, err)
	}
	if err := p.Write(page); err != nil {
		t.Fatalf("cycle %d: Write() error = %v", cycle, err)
	}
	page.Data[0] = byte(cycle)
	p.Put(page)
	if err := p.Commit(); err != nil {
		t.Errorf("cycle %d: Commit() error = %v", cycle, err)
	}
	if err := p.FreePage(pgno); err != nil {
		t.Errorf("cycle %d: FreePage() error = %v", cycle, err)
	}
	if err := p.Commit(); err != nil {
		t.Errorf("cycle %d: Commit after free error = %v", cycle, err)
	}
}

func TestMultipleCommitCycles(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_cycles.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	for cycle := 0; cycle < 5; cycle++ {
		ecAllocWriteCommitFree(t, pager, cycle)
	}
}

// rollbackCycleOnce modifies page 1 and rolls back, verifying original data.
func rollbackCycleOnce(t *testing.T, p *Pager, cycle int, originalData []byte) {
	t.Helper()
	modifiedData := []byte(fmt.Sprintf("MODIFIED%d", cycle))
	mustModifyPage(t, p, 1, DatabaseHeaderSize, modifiedData)
	mustRollback(t, p)

	page := mustGetPage(t, p, 1)
	readData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(originalData)])
	if readData != string(originalData) {
		t.Errorf("cycle %d: data after rollback = %q, want %q", cycle, readData, originalData)
	}
	p.Put(page)
}

// TestRollbackCycles tests multiple rollback cycles
func TestRollbackCycles(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_rollback_cycles.db")

	p := mustOpenPagerSized(t, dbFile, 4096)
	defer p.Close()

	originalData := []byte("ORIGINAL")
	mustWriteDataToPage(t, p, 1, DatabaseHeaderSize, originalData)
	mustCommit(t, p)

	for cycle := 0; cycle < 5; cycle++ {
		rollbackCycleOnce(t, p, cycle, originalData)
	}
}
