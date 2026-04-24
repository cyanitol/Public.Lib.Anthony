// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"testing"
)

func TestSavepointCreation(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_savepoint.db"

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
	t.Parallel()
	pager := openTestPager(t)

	mustBeginWrite(t, pager)
	mustSavepoint(t, pager, "sp1")
	mustSavepoint(t, pager, "sp2")
	mustSavepoint(t, pager, "sp3")
	mustRelease(t, pager, "sp2")

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
	t.Parallel()
	pager := openTestPager(t)

	mustBeginWrite(t, pager)
	mustGetWritePageData(t, pager, 1, 0xAA)
	mustSavepoint(t, pager, "sp1")
	mustGetWritePageData(t, pager, 1, 0xBB)
	mustRollbackTo(t, pager, "sp1")

	page := mustGetPage(t, pager, 1)
	defer pager.Put(page)
	if page.Data[0] != 0xAA {
		t.Errorf("data not restored to savepoint: expected 0xAA, got 0x%02X", page.Data[0])
	}
	pager.Commit()
}

// spNestedAction represents a declarative savepoint action
type spNestedAction struct {
	actionType string // "write", "savepoint", "rollback", "verify"
	spName     string
	value      byte
}

// spWritePage writes a value to page 1
func spWritePage(t *testing.T, pager *Pager, value byte) {
	t.Helper()
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	if err := pager.Write(page); err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	page.Data[0] = value
	pager.Put(page)
}

// spVerifyPage verifies page 1 has expected value
func spVerifyPage(t *testing.T, pager *Pager, expected byte) {
	t.Helper()
	page, err := pager.Get(1)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}
	defer pager.Put(page)

	if page.Data[0] != expected {
		t.Errorf("expected 0x%02X, got 0x%02X", expected, page.Data[0])
	}
}

func TestNestedSavepoints(t *testing.T) {
	t.Parallel()
	pager := openTestPager(t)
	mustBeginWrite(t, pager)

	actions := []spNestedAction{
		{actionType: "write", value: 0x11},
		{actionType: "savepoint", spName: "sp1"},
		{actionType: "write", value: 0x22},
		{actionType: "savepoint", spName: "sp2"},
		{actionType: "write", value: 0x33},
		{actionType: "savepoint", spName: "sp3"},
		{actionType: "write", value: 0x44},
		{actionType: "rollback", spName: "sp2"},
		{actionType: "verify", value: 0x22},
		{actionType: "rollback", spName: "sp1"},
		{actionType: "verify", value: 0x11},
	}

	for _, action := range actions {
		switch action.actionType {
		case "write":
			spWritePage(t, pager, action.value)
		case "savepoint":
			if err := pager.Savepoint(action.spName); err != nil {
				t.Fatalf("failed to create %s: %v", action.spName, err)
			}
		case "rollback":
			if err := pager.RollbackTo(action.spName); err != nil {
				t.Fatalf("failed to rollback to %s: %v", action.spName, err)
			}
		case "verify":
			spVerifyPage(t, pager, action.value)
		}
	}

	pager.Commit()
}

func TestSavepointMultiplePages(t *testing.T) {
	t.Parallel()
	pager := openTestPager(t)

	mustBeginWrite(t, pager)
	for i := 1; i <= 3; i++ {
		mustGetWritePageData(t, pager, Pgno(i), byte(i))
	}
	mustSavepoint(t, pager, "sp1")
	for i := 1; i <= 3; i++ {
		mustGetWritePageData(t, pager, Pgno(i), 0xFF)
	}
	mustRollbackTo(t, pager, "sp1")

	for i := 1; i <= 3; i++ {
		page := mustGetPage(t, pager, Pgno(i))
		if page.Data[0] != byte(i) {
			t.Errorf("page %d not restored: expected %d, got 0x%02X", i, i, page.Data[0])
		}
		pager.Put(page)
	}
	pager.Commit()
}

// spOpenPagerWithSavepoints opens a pager, begins write, creates sp1 and sp2.
func spOpenPagerWithSavepoints(t *testing.T, dbFile string) *Pager {
	t.Helper()
	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}
	if err := pager.Savepoint("sp1"); err != nil {
		t.Fatalf("failed to create sp1: %v", err)
	}
	if err := pager.Savepoint("sp2"); err != nil {
		t.Fatalf("failed to create sp2: %v", err)
	}
	return pager
}

// spVerifyNoSavepoints begins a new write transaction and verifies sp1 and sp2 are gone.
func spVerifyNoSavepoints(t *testing.T, pager *Pager) {
	t.Helper()
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}
	if pager.HasSavepoint("sp1") {
		t.Error("sp1 should not exist")
	}
	if pager.HasSavepoint("sp2") {
		t.Error("sp2 should not exist")
	}
	pager.Commit()
}

func TestSavepointClearOnCommit(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/" + "test_savepoint_clear.db"

	pager := spOpenPagerWithSavepoints(t, dbFile)
	defer pager.Close()

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	spVerifyNoSavepoints(t, pager)
}

func TestSavepointClearOnRollback(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/" + "test_savepoint_rollback_clear.db"

	pager := spOpenPagerWithSavepoints(t, dbFile)
	defer pager.Close()

	if err := pager.Rollback(); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}
	spVerifyNoSavepoints(t, pager)
}

// spVerifyAllNamesPresent checks that all expected names appear in got.
func spVerifyAllNamesPresent(t *testing.T, got, expected []string) {
	t.Helper()
	nameMap := make(map[string]bool)
	for _, name := range got {
		nameMap[name] = true
	}
	for _, name := range expected {
		if !nameMap[name] {
			t.Errorf("savepoint %s not in names list", name)
		}
	}
}

func TestSavepointNames(t *testing.T) {
	t.Parallel()
	dbFile := t.TempDir() + "/" + "test_savepoint_names.db"

	pager, err := Open(dbFile, false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	names := []string{"sp1", "sp2", "sp3"}
	for _, name := range names {
		if err := pager.Savepoint(name); err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
	}

	spNames := pager.GetSavepointNames()
	if len(spNames) != len(names) {
		t.Errorf("expected %d savepoint names, got %d", len(names), len(spNames))
	}
	spVerifyAllNamesPresent(t, spNames, names)
	pager.Commit()
}

func TestSavepointEmptyName(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_savepoint_empty.db"

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
	t.Parallel()
	tmpDir := t.TempDir()
	dbFile := tmpDir + "/" + "test_savepoint_readonly.db"

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
