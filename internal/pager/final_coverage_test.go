// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

// TestPagerInitNewDatabaseEdgeCases tests edge cases in database initialization
func TestPagerInitNewDatabaseEdgeCases(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_init_edge.db")

	// Create with specific page size
	pager, err := OpenWithPageSize(dbFile, false, 8192)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Verify initialization
	if pager.PageSize() != 8192 {
		t.Errorf("page size = %d, want 8192", pager.PageSize())
	}

	if pager.PageCount() == 0 {
		t.Error("page count should be initialized")
	}

	// Allocate to trigger growth
	for i := 0; i < 3; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage() error = %v", err)
		}
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

// TestCommitPhaseErrors tests error handling in commit phases
func TestCommitPhaseErrors(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_commit_errors.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Test with read-only should fail
	pager2, err := Open(dbFile, true)
	if err != nil {
		// Create writable first
		pager.Close()
		pager, _ = OpenWithPageSize(dbFile, false, 4096)
		page, _ := pager.Get(1)
		pager.Write(page)
		pager.Put(page)
		pager.Commit()
		pager.Close()

		pager2, err = Open(dbFile, true)
		if err != nil {
			t.Fatalf("failed to open read-only: %v", err)
		}
		defer pager2.Close()

		// Try to write (should fail)
		page2, err := pager2.Get(1)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		err = pager2.Write(page2)
		if err != ErrReadOnly {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
		pager2.Put(page2)
	}
}

// TestFreeListProcessTrunkPageEdgeCases tests trunk page processing edge cases
func TestFreeListProcessTrunkPageEdgeCases(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	mustCreateWritePages(t, pager, 2, 200)
	mustFreePages(t, fl, 50, 180)
	mustFlush(t, fl)

	firstTrunk := fl.GetFirstTrunk()
	if firstTrunk == 0 {
		t.Fatal("expected non-zero first trunk")
	}

	trunkCount := walkTrunkChain(t, fl, firstTrunk, 20)
	if trunkCount == 0 {
		t.Error("expected at least one trunk page")
	}
}

// TestTryUpgradeToExclusiveWithContention tests exclusive lock upgrade with contention
func TestTryUpgradeToExclusiveWithContention(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_excl_contention.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set busy handler
	handler := BusyTimeout(100)
	pager.WithBusyHandler(handler)

	// Try to upgrade - may succeed or fail depending on locks
	success, err := pager.TryUpgradeToExclusive()
	t.Logf("TryUpgradeToExclusive() success=%v, error=%v", success, err)

	// Try again when already in appropriate state
	if success {
		success2, err2 := pager.TryUpgradeToExclusive()
		if err2 != nil {
			t.Errorf("second TryUpgradeToExclusive() error = %v", err2)
		}
		if !success2 {
			t.Error("should succeed when already exclusive")
		}
	}
}

// TestAcquireReservedLockEdgeCases tests reserved lock acquisition edge cases
func TestAcquireReservedLockEdgeCases(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_reserved_edge.db")

	// Create file first
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Commit initial data
	page, _ := pager1.Get(1)
	pager1.Write(page)
	pager1.Put(page)
	pager1.Commit()
	pager1.Close()

	// Open two connections
	pager2, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager2: %v", err)
	}
	defer pager2.Close()

	pager3, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager3: %v", err)
	}
	defer pager3.Close()

	// Both acquire shared locks
	if err := pager2.BeginRead(); err != nil {
		t.Fatalf("BeginRead() pager2 error = %v", err)
	}

	if err := pager3.BeginRead(); err != nil {
		t.Fatalf("BeginRead() pager3 error = %v", err)
	}

	// One tries to upgrade to reserved
	err = pager2.BeginWrite()
	t.Logf("BeginWrite() with contention: %v", err)

	// Clean up
	pager2.EndRead()
	pager3.EndRead()
}

// TestAcquirePendingLockEdgeCases tests pending lock acquisition edge cases
func TestAcquirePendingLockEdgeCases(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_pending_edge.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite() error = %v", err)
	}

	// Make a modification
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	page.Data[DatabaseHeaderSize] = 0xFF
	pager.Put(page)

	// Commit (goes through pending state)
	if err := pager.Commit(); err != nil {
		t.Errorf("Commit() error = %v", err)
	}
}

// TestEnableWALMode tests WAL mode enabling
func TestEnableWALMode(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try to enable WAL mode
	err = pager.SetJournalMode(JournalModeWAL)
	if err != nil {
		t.Logf("SetJournalMode(WAL) error = %v (expected on some systems)", err)
	} else {
		t.Log("WAL mode enabled successfully")

		// Verify mode
		mode := pager.GetJournalMode()
		t.Logf("Journal mode: %d", mode)

		// Try to disable WAL mode
		err = pager.SetJournalMode(JournalModeDelete)
		if err != nil {
			t.Logf("SetJournalMode(DELETE) error = %v", err)
		}
	}
}

// TestDisableWALMode tests WAL mode disabling
func TestDisableWALMode(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wal_disable.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Try to enable then disable
	_ = pager.SetJournalMode(JournalModeWAL)
	err = pager.SetJournalMode(JournalModeDelete)
	if err != nil {
		t.Logf("Disable WAL error = %v", err)
	}
}

// TestValidateTransactionStateError tests error state validation
func TestValidateTransactionStateError(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_validate_state.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set error state
	testErr := fmt.Errorf("test error condition")
	pager.setErrorState(testErr)

	// Validate should return the error
	if err := pager.validateTransactionState(); err == nil {
		t.Error("expected error from validateTransactionState")
	} else if err != testErr {
		t.Errorf("expected %v, got %v", testErr, err)
	}

	// Clear and verify
	pager.clearErrorState()
	if err := pager.validateTransactionState(); err != nil {
		t.Errorf("expected no error after clear, got %v", err)
	}
}

// TestUpgradeToWriteLockReadOnly tests upgrade on read-only database
func TestUpgradeToWriteLockReadOnly(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_upgrade_readonly.db")

	// Create database first
	pager1, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	pager1.Close()

	// Open read-only
	pager2, err := Open(dbFile, true)
	if err != nil {
		t.Fatalf("failed to open read-only: %v", err)
	}
	defer pager2.Close()

	// Try to upgrade (should fail)
	err = pager2.upgradeToWriteLock()
	if err != ErrReadOnly {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// TestJournalRestoreEntryFull tests full journal restore with valid entry
func TestJournalRestoreEntryFull(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_restore_full.db")

	p := mustOpenPagerSized(t, dbFile, 4096)

	originalData := []byte("RESTORE TEST DATA")
	mustWriteDataToPage(t, p, 1, DatabaseHeaderSize, originalData)
	mustCommit(t, p)

	// Modify and rollback
	mustModifyPage(t, p, 1, DatabaseHeaderSize, []byte("MODIFIED TEST DATA"))
	mustRollback(t, p)
	p.Close()

	// Verify restoration after reopen
	p = openTestPagerAt(t, dbFile, false)
	defer p.Close()

	page := mustGetPage(t, p, 1)
	defer p.Put(page)

	readData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(originalData)])
	if readData != string(originalData) {
		t.Errorf("data not restored: got %q, want %q", readData, originalData)
	}
}

// TestFcntlGetLkWithOFD tests fcntlGetLk with OFD locks
func TestFcntlGetLkWithOFD(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	f, cleanup := createCoverageTestFile(t)
	defer cleanup()

	lm, err := NewLockManager(f)
	if err != nil {
		t.Fatalf("NewLockManager() error = %v", err)
	}
	defer lm.Close()

	// Test fcntlGetLk returns correct command
	cmd := lm.fcntlGetLk()
	t.Logf("fcntlGetLk() returned: %d", cmd)

	// The command should be non-zero
	if cmd == 0 {
		t.Error("fcntlGetLk should return non-zero command")
	}
}

// TestCheckReservedLockDetection tests reserved lock detection
func TestCheckReservedLockDetection(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	f1, cleanup1 := createCoverageTestFile(t)
	defer cleanup1()

	lm1 := mustNewLockManager(t, f1)
	defer lm1.Close()

	reserved, err := lm1.CheckReservedLock()
	if err != nil {
		t.Errorf("CheckReservedLock() error = %v", err)
	}
	if reserved {
		t.Error("should not detect reserved lock initially")
	}

	mustAcquireLock(t, lm1, lockShared)
	mustAcquireLock(t, lm1, lockReserved)

	_, lm2 := mustOpenSecondLockManager(t, f1.Name())

	reserved, err = lm2.CheckReservedLock()
	if err != nil {
		t.Errorf("CheckReservedLock() error = %v", err)
	}
	t.Logf("Reserved lock detected: %v", reserved)
}

// TestCommitPhasesInSequence tests commit phases execute in proper sequence
func TestCommitPhasesInSequence(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_commit_seq.db")

	p := mustOpenPagerSized(t, dbFile, 4096)
	defer p.Close()

	// Allocate and free alternating pages
	for i := 0; i < 10; i++ {
		pgno := mustAllocatePage(t, p)
		if i%2 == 0 {
			mustFreePage(t, p, pgno)
		}
	}

	mustGetWritePageData(t, p, 2, 0xAB)
	mustCommit(t, p)

	page2 := mustGetPage(t, p, 2)
	defer p.Put(page2)
	if page2.Data[0] != 0xAB {
		t.Error("data not persisted after commit")
	}
}
