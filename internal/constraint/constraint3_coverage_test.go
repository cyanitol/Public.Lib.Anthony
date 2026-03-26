// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// TestConstraint3Coverage_ValidateRow_AllPaths exercises both reachable paths
// through ValidateRow: the success path (defaults applied, ValidateInsert passes)
// and the error path (ValidateInsert returns an error).
func TestConstraint3Coverage_ValidateRow_AllPaths(t *testing.T) {
	table := &schema.Table{
		Name: "cov3_table",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "label", Type: "TEXT", NotNull: true, Default: "default_label"},
			{Name: "required", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)

	t.Run("success path with default applied", func(t *testing.T) {
		values := map[string]interface{}{
			"id":       1,
			"required": "present",
			// label is missing but has a default; ApplyDefaults will fill it
		}
		err := nnc.ValidateRow(values)
		if err != nil {
			t.Errorf("ValidateRow should succeed when defaults cover missing columns: %v", err)
		}
		if values["label"] != "default_label" {
			t.Errorf("expected default_label to be applied, got %v", values["label"])
		}
	})

	t.Run("error path from ValidateInsert when required column is nil", func(t *testing.T) {
		values := map[string]interface{}{
			"id":       2,
			"label":    "explicit",
			"required": nil,
		}
		err := nnc.ValidateRow(values)
		if err == nil {
			t.Error("ValidateRow should return error when required NOT NULL column is nil")
		} else if !strings.Contains(err.Error(), "NOT NULL constraint failed") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("error path from ValidateInsert when required column is missing", func(t *testing.T) {
		values := map[string]interface{}{
			"id": 3,
			// required is missing and has no default
		}
		err := nnc.ValidateRow(values)
		if err == nil {
			t.Error("ValidateRow should return error when required NOT NULL column has no default and is absent")
		}
	})

	t.Run("success path all columns provided", func(t *testing.T) {
		values := map[string]interface{}{
			"id":       4,
			"label":    "lbl",
			"required": "req",
		}
		err := nnc.ValidateRow(values)
		if err != nil {
			t.Errorf("ValidateRow should succeed with all columns present: %v", err)
		}
	})
}

// TestConstraint3Coverage_CheckForDuplicates_UniqueViolation exercises the exists==true
// branch of checkForDuplicates, which returns a UniqueViolationError.
// A properly encoded row is inserted using vdbe.EncodeSimpleRecord so that
// parseRecordValues can decode a matching email value.
func TestConstraint3Coverage_CheckForDuplicates_UniqueViolation(t *testing.T) {
	table := &schema.Table{
		Name: "cov3_unique",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	table.RootPage = rootPage

	// Encode a row: id=10, email="dup@example.com"
	payload := vdbe.EncodeSimpleRecord([]interface{}{int64(10), "dup@example.com"})

	cursor := btree.NewCursor(bt, table.RootPage)
	if insErr := cursor.Insert(10, payload); insErr != nil {
		t.Fatalf("Insert: %v", insErr)
	}

	uc := NewUniqueConstraint("uk_email", "cov3_unique", []string{"email"})

	// constraintValues matches the row already in the table
	constraintValues := map[string]interface{}{
		"email": "dup@example.com",
	}

	// rowid 999 is different from the stored row's rowid (10), so the self-check
	// skip won't apply and we should detect a duplicate.
	dupErr := uc.checkForDuplicates(bt, table, constraintValues, 999)
	if dupErr == nil {
		t.Fatal("checkForDuplicates should return UniqueViolationError for duplicate email")
	}

	uve, ok := dupErr.(*UniqueViolationError)
	if !ok {
		t.Fatalf("expected *UniqueViolationError, got %T: %v", dupErr, dupErr)
	}
	if uve.ConstraintName != "uk_email" {
		t.Errorf("ConstraintName = %q, want %q", uve.ConstraintName, "uk_email")
	}
	if uve.TableName != "cov3_unique" {
		t.Errorf("TableName = %q, want %q", uve.TableName, "cov3_unique")
	}
}

// TestConstraint3Coverage_CheckForDuplicates_NoConflict exercises the exists==false
// return path of checkForDuplicates (non-matching rows and self-row skip).
func TestConstraint3Coverage_CheckForDuplicates_NoConflict(t *testing.T) {
	table := &schema.Table{
		Name: "cov3_nodup",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	table.RootPage = rootPage

	// Insert a row with a different email value
	payload := vdbe.EncodeSimpleRecord([]interface{}{int64(1), "other@example.com"})
	cursor := btree.NewCursor(bt, table.RootPage)
	if insErr := cursor.Insert(1, payload); insErr != nil {
		t.Fatalf("Insert: %v", insErr)
	}

	uc := NewUniqueConstraint("", "cov3_nodup", []string{"email"})

	constraintValues := map[string]interface{}{
		"email": "unique@example.com",
	}

	err = uc.checkForDuplicates(bt, table, constraintValues, 999)
	if err != nil {
		t.Errorf("checkForDuplicates should return nil for non-duplicate: %v", err)
	}
}

// TestConstraint3Coverage_CheckForDuplicates_SelfRowSkipped ensures that the
// self-row skip in checkCurrentRow (skipRowid match) does not trigger a false
// duplicate, and checkForDuplicates returns nil.
func TestConstraint3Coverage_CheckForDuplicates_SelfRowSkipped(t *testing.T) {
	table := &schema.Table{
		Name: "cov3_selfskip",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	table.RootPage = rootPage

	// Insert a row with rowid=5 and email="self@example.com"
	payload := vdbe.EncodeSimpleRecord([]interface{}{int64(5), "self@example.com"})
	cursor := btree.NewCursor(bt, table.RootPage)
	if insErr := cursor.Insert(5, payload); insErr != nil {
		t.Fatalf("Insert: %v", insErr)
	}

	uc := NewUniqueConstraint("", "cov3_selfskip", []string{"email"})

	constraintValues := map[string]interface{}{
		"email": "self@example.com",
	}

	// rowid 5 matches the stored row — self-check skip should suppress the conflict
	err = uc.checkForDuplicates(bt, table, constraintValues, 5)
	if err != nil {
		t.Errorf("checkForDuplicates should return nil when only matching row is self: %v", err)
	}
}
