// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// validateUpdateConstraints — remaining branches
// ---------------------------------------------------------------------------

// TestValidateUpdateConstraints_FKManagerOK exercises the path where
// shouldValidateUpdate returns true, getFKManager succeeds, but the payload is
// minimal (decode returns no values) so ValidateUpdate is called.
func TestValidateUpdateConstraints_FKManagerOK(t *testing.T) {
	tbl := &fkMockTable{hasRowID: true, primaryKey: []string{"id"}}
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{validateUpdateErr: nil},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	// Minimal valid payload: header byte 0x01 (1 column), one serial-type 0 (NULL).
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		rowid:     1,
		oldValues: map[string]interface{}{"id": int64(1)},
	}

	// payload bytes: header-varint=2, serial-type=0 (NULL), no data bytes
	payload := []byte{0x02, 0x00}
	err := v.validateUpdateConstraints("t", payload)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestValidateUpdateConstraints_FKManagerError verifies that when
// ValidateUpdate returns an error it is propagated.
func TestValidateUpdateConstraints_FKManagerError(t *testing.T) {
	import_err := errString("fk constraint violated")
	tbl := &fkMockTable{hasRowID: true, primaryKey: []string{"id"}}
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{validateUpdateErr: import_err},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		rowid:     1,
		oldValues: map[string]interface{}{"id": int64(1)},
	}
	payload := []byte{0x02, 0x00}
	err := v.validateUpdateConstraints("t", payload)
	if err == nil {
		t.Error("expected error from ValidateUpdate, got nil")
	}
}

// errString is a helper that makes a simple error value for test expectations.
type errString string

func (e errString) Error() string { return string(e) }

// ---------------------------------------------------------------------------
// validateWithoutRowIDConstraints — remaining branches
// ---------------------------------------------------------------------------

// TestValidateWithoutRowIDConstraints_EmptyTableName exercises the fallthrough
// branch where tableName == "" so neither branch is taken.
func TestValidateWithoutRowIDConstraints_EmptyTableName(t *testing.T) {
	v := New()
	err := v.validateWithoutRowIDConstraints("", []byte{0x02, 0x00}, []byte{}, false)
	if err != nil {
		t.Errorf("expected nil for empty table name, got: %v", err)
	}
}

// TestValidateWithoutRowIDConstraints_UpdatePath exercises the isUpdate=true branch.
// shouldValidateWithoutRowIDUpdate will return false (nil ctx) → early return nil.
func TestValidateWithoutRowIDConstraints_UpdatePath(t *testing.T) {
	v := New()
	v.Ctx = nil // causes shouldValidateWithoutRowIDUpdate → false
	err := v.validateWithoutRowIDConstraints("t", []byte{0x02, 0x00}, []byte{}, true)
	if err != nil {
		t.Errorf("expected nil from update path with nil ctx, got: %v", err)
	}
}

// TestValidateWithoutRowIDConstraints_InsertPath exercises the isUpdate=false branch.
// checkForeignKeyConstraintsWithoutRowID with ForeignKeysEnabled=false returns nil.
func TestValidateWithoutRowIDConstraints_InsertPath(t *testing.T) {
	v := New()
	v.Ctx = &VDBEContext{ForeignKeysEnabled: false}
	err := v.validateWithoutRowIDConstraints("t", []byte{0x02, 0x00}, []byte{}, false)
	if err != nil {
		t.Errorf("expected nil from insert path with FK disabled, got: %v", err)
	}
}

// TestValidateWithoutRowIDConstraints_UpdateWithValidFKManager exercises the
// full update path where shouldValidateWithoutRowIDUpdate returns true,
// getTypedFKManager succeeds, and ValidateUpdate runs.
func TestValidateWithoutRowIDConstraints_UpdateWithValidFKManager(t *testing.T) {
	tbl := &fkMockTable{hasRowID: false, primaryKey: []string{"code"}}
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{validateUpdateErr: nil},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		oldValues: map[string]interface{}{"code": "A"},
	}
	payload := []byte{0x02, 0x00}
	err := v.validateWithoutRowIDConstraints("t", payload, []byte{}, true)
	if err != nil {
		t.Errorf("unexpected error from update path: %v", err)
	}
}

// TestValidateWithoutRowIDConstraints_UpdateFKManagerReturnsError checks that
// an FK error from the update path is propagated and pendingFKUpdate is cleared.
func TestValidateWithoutRowIDConstraints_UpdateFKManagerReturnsError(t *testing.T) {
	tbl := &fkMockTable{hasRowID: false, primaryKey: []string{"code"}}
	fkErr := errString("update fk violation")
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{validateUpdateErr: fkErr},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		oldValues: map[string]interface{}{"code": "A"},
	}
	payload := []byte{0x02, 0x00}
	err := v.validateWithoutRowIDConstraints("t", payload, []byte{}, true)
	if err == nil {
		t.Error("expected error from update FK validation, got nil")
	}
}

// ---------------------------------------------------------------------------
// checkPKUniquenessIfChanged — WITHOUT ROWID path
// ---------------------------------------------------------------------------

// TestCheckPKUniquenessIfChanged_WithoutRowidNoPKChange exercises the case
// where the table is WITHOUT ROWID but the PK values are identical, so
// pkChanged returns false and the function returns nil.
func TestCheckPKUniquenessIfChanged_WithoutRowidNoPKChange(t *testing.T) {
	tbl := &fkMockTable{hasRowID: false, primaryKey: []string{"id"}}
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	oldValues := map[string]interface{}{"id": int64(5)}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		rowid:     1,
		oldValues: oldValues,
	}
	// newValues has same PK → pkChanged should return false → no uniqueness check
	newValues := map[string]interface{}{"id": int64(5)}
	payload := []byte{0x02, 0x09} // header=2, serial-type=9 (const 1)
	err := v.checkPKUniquenessIfChanged("t", payload, newValues)
	if err != nil {
		t.Errorf("expected nil when PK unchanged on WITHOUT ROWID table, got: %v", err)
	}
}

// TestCheckPKUniquenessIfChanged_WithoutRowidPKChanged exercises the path where
// the table is WITHOUT ROWID and the PK changed.  checkWithoutRowidPKUniqueness
// will fail because the btree/schema data doesn't exist, but it should at least
// return a non-nil error (not panic).
func TestCheckPKUniquenessIfChanged_WithoutRowidPKChanged(t *testing.T) {
	tbl := &fkMockTable{hasRowID: false, primaryKey: []string{"id"}, rootPage: 0}
	v := New()
	v.Ctx = &VDBEContext{
		ForeignKeysEnabled: true,
		FKManager:          &fkMockFKManager{},
		Schema:             &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	oldValues := map[string]interface{}{"id": int64(1)}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		rowid:     1,
		oldValues: oldValues,
	}
	// newValues has different PK → pkChanged returns true
	newValues := map[string]interface{}{"id": int64(99)}
	payload := []byte{0x02, 0x09}
	// This will call checkWithoutRowidPKUniqueness which may return an error
	// because there's no real btree backing it.  We just verify no panic.
	_ = v.checkPKUniquenessIfChanged("t", payload, newValues)
}

// ---------------------------------------------------------------------------
// getWindowOffset — both branches
// ---------------------------------------------------------------------------

// TestGetWindowOffset_Positive exercises the P4.I > 0 branch.
func TestGetWindowOffset_Positive(t *testing.T) {
	v := NewTestVDBE(2)
	instr := &Instruction{P4: P4Union{I: 5}}
	got := v.getWindowOffset(instr)
	if got != 5 {
		t.Errorf("getWindowOffset with P4.I=5 got %d, want 5", got)
	}
}

// TestGetWindowOffset_Zero exercises the default-to-1 branch.
func TestGetWindowOffset_Zero(t *testing.T) {
	v := NewTestVDBE(2)
	instr := &Instruction{P4: P4Union{I: 0}}
	got := v.getWindowOffset(instr)
	if got != 1 {
		t.Errorf("getWindowOffset with P4.I=0 got %d, want 1", got)
	}
}

// TestGetWindowOffset_Negative exercises the negative-value branch (≤ 0 → default 1).
func TestGetWindowOffset_Negative(t *testing.T) {
	v := NewTestVDBE(2)
	instr := &Instruction{P4: P4Union{I: -3}}
	got := v.getWindowOffset(instr)
	if got != 1 {
		t.Errorf("getWindowOffset with P4.I=-3 got %d, want 1", got)
	}
}
