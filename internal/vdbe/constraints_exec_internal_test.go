// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// ---------------------------------------------------------------------------
// TestConstraintsExecInternal_CheckMultiColRow_SkipRowid
//
// Directly exercises the skipRowid early-exit in checkMultiColRow (line 550).
// We fabricate a BtCursor whose CurrentCell.Key matches skipRowid so that
// GetKey() == skipRowid and the function returns nil immediately.
// ---------------------------------------------------------------------------

// minExecProvider is a no-op schemaIndexProvider used in these unit tests.
// checkMultiColRow returns nil before GetRecordColumnIndex is called when
// the skip path is taken, so all methods can safely be no-ops.
type minExecProvider struct{}

func (m *minExecProvider) GetTableIndexes(_ string) []uniqueIndexInfo {
	return nil
}
func (m *minExecProvider) GetTablePrimaryKey(_ string) ([]string, bool) {
	return nil, false
}
func (m *minExecProvider) GetRecordColumnIndex(_, _ string) int {
	return -1
}
func (m *minExecProvider) GetColumnCollation(_, _ string) string {
	return ""
}

// TestConstraintsExecInternal_CheckMultiColRow_SkipRowid verifies that when
// the scan cursor's key equals skipRowid the function returns nil without
// reading any payload — covering the uncovered early-exit branch.
func TestConstraintsExecInternal_CheckMultiColRow_SkipRowid(t *testing.T) {
	const skipRowid = int64(42)

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Build a cursor in CursorValid state with CurrentCell.Key == skipRowid.
	scanCursor := btree.NewCursor(bt, rootPage)
	scanCursor.State = btree.CursorValid
	scanCursor.CurrentCell = &btree.CellInfo{Key: skipRowid}

	if got := scanCursor.GetKey(); got != skipRowid {
		t.Fatalf("precondition: GetKey()=%d, want %d", got, skipRowid)
	}

	v := New()
	provider := &minExecProvider{}
	newValues := []*Mem{NewMemInt(1), NewMemInt(2)}

	err = v.checkMultiColRow(scanCursor, []string{"a", "b"}, newValues, skipRowid, "t", provider)
	if err != nil {
		t.Errorf("checkMultiColRow with skipRowid match: expected nil, got %v", err)
	}
}

// TestConstraintsExecInternal_CheckMultiColRow_NoSkip verifies that when the
// scan cursor's key does NOT equal skipRowid the function proceeds past the
// skip guard.  With nil payload GetPayloadWithOverflow fails gracefully and
// the function returns nil, confirming the non-skip path is exercised.
func TestConstraintsExecInternal_CheckMultiColRow_NoSkip(t *testing.T) {
	const skipRowid = int64(42)
	const differentRowid = int64(99)

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	scanCursor := btree.NewCursor(bt, rootPage)
	scanCursor.State = btree.CursorValid
	scanCursor.CurrentCell = &btree.CellInfo{Key: differentRowid}

	v := New()
	provider := &minExecProvider{}
	newValues := []*Mem{NewMemInt(1)}

	err = v.checkMultiColRow(scanCursor, []string{"a"}, newValues, skipRowid, "t", provider)
	if err != nil {
		t.Errorf("checkMultiColRow with non-matching key and empty payload: expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsExecInternal_HandleExistingRowConflict_DefaultReturn
//
// Exercises the `return false, nil` at the end of handleExistingRowConflict:
// the path where a row with the given rowid exists but conflictMode is neither
// Replace (4) nor Ignore (3) — e.g. conflictModeFail (2) or
// conflictModeRollback (1).
// ---------------------------------------------------------------------------

// TestConstraintsExecInternal_HandleExistingRowConflict_FailMode calls
// handleExistingRowConflict with conflictModeFail when a row exists, hitting
// the default `return false, nil` branch.
func TestConstraintsExecInternal_HandleExistingRowConflict_FailMode(t *testing.T) {
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	btCursor := btree.NewCursor(bt, rootPage)
	payload := makeIntRecord(99)
	if err := btCursor.Insert(5, payload); err != nil {
		t.Fatalf("btree insert: %v", err)
	}

	v := New()
	cursor := &Cursor{IsTable: true, Writable: true, RootPage: rootPage}

	skip, err := v.handleExistingRowConflict(cursor, btCursor, 5, "t", conflictModeFail)
	if err != nil {
		t.Errorf("conflictModeFail with existing row: expected nil error, got %v", err)
	}
	if skip {
		t.Error("conflictModeFail with existing row: expected skip=false")
	}
}

// TestConstraintsExecInternal_HandleExistingRowConflict_RollbackMode exercises
// the same default branch using conflictModeRollback (1).
func TestConstraintsExecInternal_HandleExistingRowConflict_RollbackMode(t *testing.T) {
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	btCursor := btree.NewCursor(bt, rootPage)
	payload := makeIntRecord(7)
	if err := btCursor.Insert(10, payload); err != nil {
		t.Fatalf("btree insert: %v", err)
	}

	v := New()
	cursor := &Cursor{IsTable: true, Writable: true, RootPage: rootPage}

	skip, err := v.handleExistingRowConflict(cursor, btCursor, 10, "t", conflictModeRollback)
	if err != nil {
		t.Errorf("conflictModeRollback with existing row: expected nil error, got %v", err)
	}
	if skip {
		t.Error("conflictModeRollback with existing row: expected skip=false")
	}
}

// TestConstraintsExecInternal_HandleExistingRowConflict_RowNotExists verifies
// the early-exit when rowExists returns false (row not in btree).
func TestConstraintsExecInternal_HandleExistingRowConflict_RowNotExists(t *testing.T) {
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	btCursor := btree.NewCursor(bt, rootPage)

	v := New()
	cursor := &Cursor{IsTable: true, Writable: true, RootPage: rootPage}

	skip, err := v.handleExistingRowConflict(cursor, btCursor, 99, "t", conflictModeFail)
	if err != nil {
		t.Errorf("row not exists: unexpected error: %v", err)
	}
	if skip {
		t.Error("row not exists: expected skip=false")
	}
}
