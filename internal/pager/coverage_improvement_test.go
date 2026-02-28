package pager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestJournalRestoreEntry tests the journal restoreEntry function
func TestJournalRestoreEntry(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_restore.db")

	// Create a pager
	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	// Write some initial data
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	// Set unique data
	testData := []byte("ORIGINAL DATA")
	copy(page.Data[DatabaseHeaderSize:], testData)
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Now create a journal entry manually
	journal := NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Save original page data
	page, _ = pager.Get(1)
	originalData := make([]byte, len(page.Data))
	copy(originalData, page.Data)
	pager.Put(page)

	// Write to journal
	if err := journal.WriteOriginal(1, originalData); err != nil {
		t.Fatalf("failed to write to journal: %v", err)
	}
	journal.Close()

	// Modify the database page
	page, _ = pager.Get(1)
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	modifiedData := []byte("MODIFIED DATA")
	copy(page.Data[DatabaseHeaderSize:], modifiedData)
	pager.Put(page)

	// Flush to disk without committing
	if err := pager.writePage(page); err != nil {
		t.Fatalf("failed to write page to disk: %v", err)
	}

	pager.Close()

	// Reopen pager and journal
	pager, err = OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to reopen pager: %v", err)
	}
	defer pager.Close()

	journal = NewJournal(dbFile+"-journal", 4096, 1)

	// Rollback should restore original data
	if err := journal.Rollback(pager); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify data was restored
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page after rollback: %v", err)
	}
	defer pager.Put(page)

	restoredData := string(page.Data[DatabaseHeaderSize : DatabaseHeaderSize+len(testData)])
	if restoredData != string(testData) {
		t.Errorf("data not restored correctly: got %q, want %q", restoredData, testData)
	}
}

// TestJournalRestoreEntryChecksumMismatch tests checksum validation in restoreEntry
func TestJournalRestoreEntryChecksumMismatch(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_checksum.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	journal := NewJournal(dbFile+"-journal", 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	// Create a corrupted journal entry
	pageData := make([]byte, 4096)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Manually create an entry with wrong checksum
	entry := make([]byte, 4+4096+4)
	binary.BigEndian.PutUint32(entry[0:4], 1) // page number
	copy(entry[4:4+4096], pageData)
	binary.BigEndian.PutUint32(entry[4+4096:], 0xDEADBEEF) // wrong checksum

	// Try to restore this corrupted entry
	err = journal.restoreEntry(pager, entry)
	if err == nil {
		t.Error("expected error for checksum mismatch, got nil")
	}
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("checksum mismatch")) {
		t.Errorf("expected checksum mismatch error, got: %v", err)
	}

	journal.Close()
}

// TestJournalValidateHeader tests journal header validation
func TestJournalValidateHeader(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_validate.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	pager.Close()

	journal := NewJournal(dbFile+"-journal", 4096, 1)

	t.Run("validate valid header", func(t *testing.T) {
			t.Parallel()
		if err := journal.Open(); err != nil {
			t.Fatalf("failed to open journal: %v", err)
		}

		// Write some data to create a valid header
		pageData := make([]byte, 4096)
		if err := journal.WriteOriginal(1, pageData); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		journal.Close()

		// Reopen to validate header
		if err := journal.Open(); err != nil {
			t.Fatalf("failed to reopen journal: %v", err)
		}

		// Validate header
		valid, err := journal.validateHeader()
		if err != nil {
			t.Fatalf("failed to validate header: %v", err)
		}
		if !valid {
			t.Error("valid header should pass validation")
		}

		journal.Close()
	})
}

// TestLockUnixFcntlGetLk tests the fcntlGetLk function
func TestLockUnixFcntlGetLk(t *testing.T) {
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

	// Just verify the function returns a valid fcntl constant
	cmd := lm.fcntlGetLk()
	if cmd == 0 {
		t.Error("fcntlGetLk returned 0, expected valid constant")
	}
}

// TestLockUnixCheckReservedLock tests the CheckReservedLock function
func TestLockUnixCheckReservedLock(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	f1, cleanup1 := createCoverageTestFile(t)
	defer cleanup1()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	// Check reserved lock when no lock is held
	reserved, err := lm1.CheckReservedLock()
	if err != nil {
		t.Errorf("CheckReservedLock() error = %v", err)
	}
	if reserved {
		t.Error("expected no reserved lock initially")
	}

	// Acquire shared lock then reserved
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}
	if err := lm1.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	// Now check from another lock manager
	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	reserved, err = lm2.CheckReservedLock()
	if err != nil {
		t.Errorf("CheckReservedLock() error = %v", err)
	}
	if !reserved {
		t.Error("expected reserved lock to be detected")
	}
}

// TestLockUnixAcquirePendingLock tests pending lock acquisition
func TestLockUnixAcquirePendingLock(t *testing.T) {
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

	// Acquire shared, then reserved, then pending
	if err := lm.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}
	if err := lm.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}
	if err := lm.AcquireLock(lockPending); err != nil {
		t.Fatalf("AcquireLock(PENDING) error = %v", err)
	}

	if lm.GetLockState() != lockPending {
		t.Errorf("lock state = %v, want %v", lm.GetLockState(), lockPending)
	}
}

// TestLockUnixAcquireReservedLock tests reserved lock acquisition edge cases
func TestLockUnixAcquireReservedLock(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	f1, cleanup1 := createCoverageTestFile(t)
	defer cleanup1()

	lm1, err := NewLockManager(f1)
	if err != nil {
		t.Fatalf("NewLockManager(1) error = %v", err)
	}
	defer lm1.Close()

	// First acquire shared lock
	if err := lm1.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}

	// Acquire reserved lock
	if err := lm1.AcquireLock(lockReserved); err != nil {
		t.Fatalf("AcquireLock(RESERVED) error = %v", err)
	}

	// Try to acquire reserved from another process (should fail)
	f2, err := os.OpenFile(f1.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer f2.Close()

	lm2, err := NewLockManager(f2)
	if err != nil {
		t.Fatalf("NewLockManager(2) error = %v", err)
	}
	defer lm2.Close()

	// Acquire shared first
	if err := lm2.AcquireLock(lockShared); err != nil {
		t.Fatalf("AcquireLock(SHARED) error = %v", err)
	}

	// Try to acquire reserved (should fail)
	err = lm2.AcquireLock(lockReserved)
	if err == nil {
		t.Error("expected error acquiring reserved when already held")
	}
}

// TestTransactionErrorState tests transaction error state management
func TestTransactionErrorState(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_error.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set error state
	testErr := fmt.Errorf("test error")
	pager.setErrorState(testErr)

	// Verify error state
	if err := pager.validateTransactionState(); err == nil {
		t.Error("expected error from validateTransactionState")
	} else if err != testErr {
		t.Errorf("expected %v, got %v", testErr, err)
	}

	// Clear error state
	pager.clearErrorState()

	// Verify cleared
	if err := pager.validateTransactionState(); err != nil {
		t.Errorf("expected no error after clear, got %v", err)
	}
}

// TestTransactionUpgradeDowngradeLock tests lock upgrade/downgrade
func TestTransactionUpgradeDowngradeLock(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_lock_updown.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Start with no lock
	pager.lockState = LockNone

	// Upgrade to write lock
	if err := pager.upgradeToWriteLock(); err != nil {
		t.Errorf("upgradeToWriteLock() error = %v", err)
	}
	if pager.lockState != LockReserved {
		t.Errorf("lock state = %v, want %v", pager.lockState, LockReserved)
	}

	// Try to upgrade again (should be no-op)
	if err := pager.upgradeToWriteLock(); err != nil {
		t.Errorf("second upgradeToWriteLock() error = %v", err)
	}

	// Downgrade lock
	if err := pager.downgradeLock(); err != nil {
		t.Errorf("downgradeLock() error = %v", err)
	}
	if pager.lockState != LockShared {
		t.Errorf("lock state = %v, want %v", pager.lockState, LockShared)
	}

	// Try to downgrade from shared (should be no-op)
	if err := pager.downgradeLock(); err != nil {
		t.Errorf("downgradeLock() from shared error = %v", err)
	}
}

// TestTransactionTryUpgradeToExclusive tests exclusive lock upgrade
func TestTransactionTryUpgradeToExclusive(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_exclusive.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Start from reserved state
	pager.lockState = LockReserved

	// Try to upgrade (should succeed if no other locks)
	success, err := pager.TryUpgradeToExclusive()
	if err != nil {
		t.Errorf("TryUpgradeToExclusive() error = %v", err)
	}

	// Result depends on whether we can get exclusive lock
	t.Logf("TryUpgradeToExclusive() success = %v", success)

	// If already exclusive, should return true
	pager.lockState = LockExclusive
	success, err = pager.TryUpgradeToExclusive()
	if err != nil {
		t.Errorf("TryUpgradeToExclusive() when already exclusive error = %v", err)
	}
	if !success {
		t.Error("expected success when already exclusive")
	}
}

// TestTransactionWaitForReadersToFinish tests waiting for readers
func TestTransactionWaitForReadersToFinish(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_wait.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set to reserved state
	pager.lockState = LockReserved

	// Call WaitForReadersToFinish
	err = pager.WaitForReadersToFinish()
	// May or may not succeed depending on lock availability
	t.Logf("WaitForReadersToFinish() error = %v", err)
}

// TestPagerJournalZeroHeader tests zeroing journal header via journal API
func TestPagerJournalZeroHeader(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_zero.db")
	journalFile := dbFile + "-journal"

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Start a write transaction to create journal
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	pager.Put(page)

	// Commit to ensure journal is created and synced
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Create a new journal for testing
	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}

	pageData := make([]byte, 4096)
	if err := journal.WriteOriginal(1, pageData); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	journal.Close()

	// Zero the header
	journal = NewJournal(journalFile, 4096, 1)
	if err := journal.ZeroHeader(); err != nil {
		t.Errorf("ZeroHeader() error = %v", err)
	}

	// Verify journal is no longer valid
	valid, _ := journal.IsValid()
	if valid {
		t.Error("journal should not be valid after zeroing header")
	}
}

// TestPagerFullCommitCycle tests complete commit cycle
func TestPagerFullCommitCycle(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_commit_cycle.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Allocate some pages first
	for i := 0; i < 5; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate page: %v", err)
		}
		t.Logf("Allocated page %d", pgno)
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit allocation: %v", err)
	}

	// Free some pages to test freelist flush
	if err := pager.FreePage(3); err != nil {
		t.Fatalf("failed to free page: %v", err)
	}

	// Start new transaction
	page, err := pager.Get(2)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	testData := []byte("COMMIT CYCLE TEST")
	copy(page.Data[:len(testData)], testData)
	pager.Put(page)

	// Full commit should go through all phases
	if err := pager.Commit(); err != nil {
		t.Errorf("Commit() error = %v", err)
	}

	// Verify data persisted
	page2, err := pager.Get(2)
	if err != nil {
		t.Fatalf("failed to get page after commit: %v", err)
	}
	defer pager.Put(page2)

	if string(page2.Data[:len(testData)]) != string(testData) {
		t.Error("data not persisted correctly after commit")
	}
}

// TestPagerInitNewDatabase tests new database initialization
func TestPagerInitNewDatabaseViaOpen(t *testing.T) {
	t.Parallel()
	dbFile := filepath.Join(t.TempDir(), "test_init.db")

	// Open will initialize a new database
	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Verify header was created
	if pager.GetHeader() == nil {
		t.Error("header should be initialized")
	}

	// Verify page count
	if pager.PageCount() == 0 {
		t.Error("page count should be > 0")
	}

	// Verify page size
	if pager.PageSize() != 4096 {
		t.Errorf("page size = %d, want 4096", pager.PageSize())
	}
}

// TestFreeListProcessTrunkPage tests trunk page processing
func TestFreeListProcessTrunkPage(t *testing.T) {
	t.Parallel()
	pager, cleanup := createTestPagerForFreeList(t)
	defer cleanup()

	fl := NewFreeList(pager)

	// Create pages to work with
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

	// Free many pages to create trunk structure
	for i := Pgno(10); i <= 40; i++ {
		if err := fl.Free(i); err != nil {
			t.Fatalf("failed to free page %d: %v", i, err)
		}
	}

	// Flush to create trunk pages
	if err := fl.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Verify trunk exists
	if fl.GetFirstTrunk() == 0 {
		t.Fatal("expected non-zero first trunk")
	}

	// Read and verify trunk page
	nextTrunk, leaves, err := fl.ReadTrunk(fl.GetFirstTrunk())
	if err != nil {
		t.Fatalf("failed to read trunk: %v", err)
	}

	t.Logf("Trunk page: next=%d, leaves=%d", nextTrunk, len(leaves))

	// Verify leaves
	if len(leaves) == 0 {
		t.Error("expected at least one leaf page")
	}
}

// Helper function to create test file for coverage tests
func createCoverageTestFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "lock_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	// Write some data to make it a valid file
	data := make([]byte, 4096)
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("failed to write to temp file: %v", err)
	}

	cleanup := func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}

	return tmpFile, cleanup
}

// TestBusyHandlerRetry tests busy handler with retry logic
func TestBusyHandlerRetry(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-specific test")
	}

	dbFile := filepath.Join(t.TempDir(), "test_busy.db")

	pager, err := OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Set a busy handler with timeout
	handler := BusyTimeout(100) // 100ms timeout
	pager.WithBusyHandler(handler)

	// Try operations that might trigger busy handler
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	pager.Put(page)

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

