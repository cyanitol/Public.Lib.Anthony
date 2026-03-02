// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"testing"
)

func TestBeginReadTransaction(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_begin_read.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin read transaction
	if err := pager.BeginRead(); err != nil {
		t.Fatalf("failed to begin read transaction: %v", err)
	}

	// Verify transaction state
	if !pager.InTransaction() {
		t.Error("expected to be in transaction")
	}

	state := pager.GetTransactionState()
	if state != TxRead {
		t.Errorf("expected TxRead state, got %v", state)
	}

	// End read transaction
	if err := pager.EndRead(); err != nil {
		t.Fatalf("failed to end read transaction: %v", err)
	}

	// Verify transaction ended
	if pager.InTransaction() {
		t.Error("expected to not be in transaction")
	}
}

func TestBeginWriteTransaction(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_begin_write.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write transaction: %v", err)
	}

	// Verify transaction state
	if !pager.InTransaction() {
		t.Error("expected to be in transaction")
	}

	if !pager.InWriteTransaction() {
		t.Error("expected to be in write transaction")
	}

	state := pager.GetTransactionState()
	if state != TxWrite {
		t.Errorf("expected TxWrite state, got %v", state)
	}

	// Commit transaction
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Verify transaction ended
	if pager.InTransaction() {
		t.Error("expected to not be in transaction")
	}
}

func TestWriteTransactionExclusive(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_write_exclusive.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin first write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin first write transaction: %v", err)
	}

	// Try to begin another write transaction - should fail
	if err := pager.BeginWrite(); err == nil {
		t.Error("expected error when starting second write transaction")
	}

	// Commit first transaction
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

func TestReadTransactionReadOnly(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_read_only.db"

	// Create database with some data
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	pager.Close()

	// Open read-only
	pager, err = Open(dbFile, true)
	if err != nil {
		t.Fatalf("failed to open pager read-only: %v", err)
	}
	defer pager.Close()

	// Should be able to start read transaction
	if err := pager.BeginRead(); err != nil {
		t.Fatalf("failed to begin read transaction: %v", err)
	}

	// Should not be able to start write transaction
	if err := pager.BeginWrite(); err == nil {
		t.Error("expected error when starting write transaction on read-only pager")
	}

	// End read transaction
	if err := pager.EndRead(); err != nil {
		t.Fatalf("failed to end read transaction: %v", err)
	}
}

func TestTransactionStateTransitions(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_state_transitions.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Initial state
	if pager.InTransaction() {
		t.Error("should not be in transaction initially")
	}

	// Open -> Read
	if err := pager.BeginRead(); err != nil {
		t.Fatalf("failed to begin read: %v", err)
	}
	if !pager.InTransaction() || pager.GetTransactionState() != TxRead {
		t.Error("should be in read transaction")
	}

	// Read -> Open
	if err := pager.EndRead(); err != nil {
		t.Fatalf("failed to end read: %v", err)
	}
	if pager.InTransaction() {
		t.Error("should not be in transaction")
	}

	// Open -> Write
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}
	if !pager.InWriteTransaction() || pager.GetTransactionState() != TxWrite {
		t.Error("should be in write transaction")
	}

	// Write -> Open (commit)
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if pager.InTransaction() {
		t.Error("should not be in transaction after commit")
	}

	// Open -> Write -> Open (rollback)
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}
	if pager.InTransaction() {
		t.Error("should not be in transaction after rollback")
	}
}

func TestTransactionIsolation(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_isolation.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Start write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Get a page and modify it
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to mark page writable: %v", err)
	}

	// Modify page data
	originalData := make([]byte, len(page.Data))
	copy(originalData, page.Data)
	page.Data[0] = 0xFF

	pager.Put(page)

	// Rollback transaction
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify data was restored
	page2, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page after rollback: %v", err)
	}
	defer pager.Put(page2)

	if page2.Data[0] == 0xFF {
		t.Error("page data was not restored after rollback")
	}
}

func TestLockStateManagement(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_lock_state.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Initial lock state
	if pager.GetLockState() != LockNone {
		t.Error("initial lock state should be LockNone")
	}

	// Begin read transaction
	if err := pager.BeginRead(); err != nil {
		t.Fatalf("failed to begin read: %v", err)
	}

	// Should have shared lock
	if pager.GetLockState() < LockShared {
		t.Error("should have at least shared lock in read transaction")
	}

	// End read transaction
	if err := pager.EndRead(); err != nil {
		t.Fatalf("failed to end read: %v", err)
	}

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Should have reserved lock
	if pager.GetLockState() < LockReserved {
		t.Error("should have at least reserved lock in write transaction")
	}

	// Commit
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Lock should be released
	if pager.GetLockState() != LockNone {
		t.Error("lock should be released after commit")
	}
}

func TestJournalModeSettings(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_journal_mode.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Default journal mode
	if pager.GetJournalMode() != JournalModeDelete {
		t.Error("default journal mode should be DELETE")
	}

	// Set journal mode
	modes := []int{
		JournalModePersist,
		JournalModeTruncate,
		JournalModeOff,
		JournalModeDelete,
	}

	for _, mode := range modes {
		if err := pager.SetJournalMode(mode); err != nil {
			t.Errorf("failed to set journal mode %d: %v", mode, err)
		}

		if pager.GetJournalMode() != mode {
			t.Errorf("journal mode not set correctly: expected %d, got %d", mode, pager.GetJournalMode())
		}
	}

	// Try to set invalid mode
	if err := pager.SetJournalMode(999); err == nil {
		t.Error("expected error when setting invalid journal mode")
	}

	// Cannot change journal mode during transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	if err := pager.SetJournalMode(JournalModePersist); err == nil {
		t.Error("expected error when changing journal mode during transaction")
	}

	pager.Rollback()
}

func TestPageCountTracking(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_page_count.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Initial page count
	initialCount := pager.GetPageCount()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Original count should be saved
	if pager.GetOriginalPageCount() != initialCount {
		t.Error("original page count not saved correctly")
	}

	// Commit
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}
