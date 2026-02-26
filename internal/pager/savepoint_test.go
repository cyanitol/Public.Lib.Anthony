package pager

import (
	"os"
	"testing"
)

func TestSavepointCreation(t *testing.T) {
	dbFile := "test_savepoint.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Cannot create savepoint without transaction
	if err := pager.Savepoint("sp1"); err == nil {
		t.Error("expected error creating savepoint without transaction")
	}

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Create savepoint
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Verify savepoint exists
	if !pager.HasSavepoint("sp1") {
		t.Error("savepoint sp1 should exist")
	}

	// Cannot create duplicate savepoint
	if err := pager.Savepoint("sp1"); err == nil {
		t.Error("expected error creating duplicate savepoint")
	}

	// Can create different savepoint
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create second savepoint: %v", err)
	}

	// Commit
	pager.Commit()
}

func TestSavepointRelease(t *testing.T) {
	dbFile := "test_savepoint_release.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Create savepoints
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create sp1: %v", err)
	}
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create sp2: %v", err)
	}
	if err := pager.Savepoint("sp3"); err != nil {
		t.Fatalf("failed to create sp3: %v", err)
	}

	// Release sp2 should also release sp3
	if err := pager.Release("sp2"); err != nil {
		t.Fatalf("failed to release sp2: %v", err)
	}

	// sp1 should still exist
	if !pager.HasSavepoint("sp1") {
		t.Error("sp1 should still exist")
	}

	// sp2 and sp3 should not exist
	if pager.HasSavepoint("sp2") {
		t.Error("sp2 should not exist after release")
	}
	if pager.HasSavepoint("sp3") {
		t.Error("sp3 should not exist after release")
	}

	// Release non-existent savepoint
	if err := pager.Release("sp2"); err == nil {
		t.Error("expected error releasing non-existent savepoint")
	}

	pager.Commit()
}

func TestSavepointRollback(t *testing.T) {
	dbFile := "test_savepoint_rollback.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Get page and set initial value
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	page.Data[0] = 0xAA
	pager.Put(page)

	// Create savepoint
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Modify page again
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	page.Data[0] = 0xBB
	pager.Put(page)

	// Rollback to savepoint
	if err := pager.RollbackTo("sp1"); err != nil {
		t.Fatalf("failed to rollback to sp1: %v", err)
	}

	// Verify data was restored to savepoint state
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer pager.Put(page)

	if page.Data[0] != 0xAA {
		t.Errorf("data not restored to savepoint: expected 0xAA, got 0x%02X", page.Data[0])
	}

	pager.Commit()
}

func TestNestedSavepoints(t *testing.T) {
	dbFile := "test_nested_savepoints.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Get page
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	// Initial state
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	page.Data[0] = 0x11
	pager.Put(page)

	// Savepoint 1
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create sp1: %v", err)
	}

	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	page.Data[0] = 0x22
	pager.Put(page)

	// Savepoint 2
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create sp2: %v", err)
	}

	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	page.Data[0] = 0x33
	pager.Put(page)

	// Savepoint 3
	if err := pager.Savepoint("sp3"); err != nil {
		t.Fatalf("failed to create sp3: %v", err)
	}

	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	page.Data[0] = 0x44
	pager.Put(page)

	// Rollback to sp2
	if err := pager.RollbackTo("sp2"); err != nil {
		t.Fatalf("failed to rollback to sp2: %v", err)
	}

	// Should be at 0x22 (sp1 state)
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if page.Data[0] != 0x22 {
		t.Errorf("wrong value after rollback to sp2: expected 0x22, got 0x%02X", page.Data[0])
	}
	pager.Put(page)

	// Rollback to sp1
	if err := pager.RollbackTo("sp1"); err != nil {
		t.Fatalf("failed to rollback to sp1: %v", err)
	}

	// Should be at 0x11 (initial state)
	page, err = pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer pager.Put(page)

	if page.Data[0] != 0x11 {
		t.Errorf("wrong value after rollback to sp1: expected 0x11, got 0x%02X", page.Data[0])
	}

	pager.Commit()
}

func TestSavepointMultiplePages(t *testing.T) {
	dbFile := "test_savepoint_multi.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Set initial values for multiple pages
	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}

		page.Data[0] = byte(i)
		pager.Put(page)
	}

	// Create savepoint
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}

	// Modify all pages
	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		if err := pager.Write(page); err != nil {
			t.Fatalf("failed to write page %d: %v", i, err)
		}

		page.Data[0] = 0xFF
		pager.Put(page)
	}

	// Rollback to savepoint
	if err := pager.RollbackTo("sp1"); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify all pages restored
	for i := 1; i <= 3; i++ {
		page, err := pager.Get(Pgno(i))
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}

		if page.Data[0] != byte(i) {
			t.Errorf("page %d not restored: expected %d, got 0x%02X", i, i, page.Data[0])
		}

		pager.Put(page)
	}

	pager.Commit()
}

func TestSavepointClearOnCommit(t *testing.T) {
	dbFile := "test_savepoint_clear.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Create savepoints
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create sp1: %v", err)
	}
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create sp2: %v", err)
	}

	// Commit
	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Begin new transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Old savepoints should not exist
	if pager.HasSavepoint("sp1") {
		t.Error("sp1 should not exist after commit")
	}
	if pager.HasSavepoint("sp2") {
		t.Error("sp2 should not exist after commit")
	}

	pager.Commit()
}

func TestSavepointClearOnRollback(t *testing.T) {
	dbFile := "test_savepoint_rollback_clear.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Create savepoints
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create sp1: %v", err)
	}
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create sp2: %v", err)
	}

	// Rollback entire transaction
	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Begin new transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Old savepoints should not exist
	if pager.HasSavepoint("sp1") {
		t.Error("sp1 should not exist after rollback")
	}
	if pager.HasSavepoint("sp2") {
		t.Error("sp2 should not exist after rollback")
	}

	pager.Commit()
}

func TestSavepointNames(t *testing.T) {
	dbFile := "test_savepoint_names.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Create savepoints
	names := []string{"sp1", "sp2", "sp3"}
	for _, name := range names {
		if err := pager.Savepoint(name); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
	}

	// Get savepoint names
	spNames := pager.GetSavepointNames()

	// Should have all savepoints (may be in reverse order due to stack)
	if len(spNames) != len(names) {
		t.Errorf("expected %d savepoint names, got %d", len(names), len(spNames))
	}

	// All names should be present
	nameMap := make(map[string]bool)
	for _, name := range spNames {
		nameMap[name] = true
	}

	for _, name := range names {
		if !nameMap[name] {
			t.Errorf("savepoint %s not in names list", name)
		}
	}

	pager.Commit()
}

func TestSavepointEmptyName(t *testing.T) {
	dbFile := "test_savepoint_empty.db"
	defer os.Remove(dbFile)

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Begin write transaction
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Cannot create savepoint with empty name
	if err := pager.Savepoint(""); err == nil {
		t.Error("expected error creating savepoint with empty name")
	}

	pager.Commit()
}

func TestSavepointReadOnlyTransaction(t *testing.T) {
	dbFile := "test_savepoint_readonly.db"
	defer os.Remove(dbFile)

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

	// Begin read transaction
	if err := pager.BeginRead(); err != nil {
		t.Fatalf("failed to begin read: %v", err)
	}

	// Cannot create savepoint in read transaction
	if err := pager.Savepoint("sp1"); err == nil {
		t.Error("expected error creating savepoint in read transaction")
	}

	pager.EndRead()
}
