// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestExtractCheckConstraints_ColumnLevelParseError covers the uncovered branch
// at check.go line 60-62: column-level CHECK expression fails to parse.
//
// This exercises the `return nil, err` path in extractCheckConstraints when
// parseCheckConstraint returns an error for a column's Check string.
func TestExtractCheckConstraints_ColumnLevelParseError(t *testing.T) {
	t.Parallel()

	// Build a table directly in the schema model with a column whose Check
	// field contains an expression string that the parser cannot accept.
	table := &schema.Table{
		Name: "bad_check_col",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{
				Name:  "val",
				Type:  "INTEGER",
				Check: "!!!invalid_expression@@@",
			},
		},
	}

	_, err := extractCheckConstraints(table)
	if err == nil {
		t.Fatal("extractCheckConstraints: expected parse error for invalid column CHECK, got nil")
	}
}

// TestExtractCheckConstraints_TableLevelParseError covers the table-level
// parse-error branch in extractCheckConstraints (check.go line 72-74).
func TestExtractCheckConstraints_TableLevelParseError(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name: "bad_check_table",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:       schema.ConstraintCheck,
				Name:       "bad_expr",
				Expression: "!!!invalid@@@",
			},
		},
	}

	_, err := extractCheckConstraints(table)
	if err == nil {
		t.Fatal("extractCheckConstraints: expected parse error for invalid table-level CHECK, got nil")
	}
}

// TestValidateRow_DefaultFill covers the ValidateRow success path where
// defaults are applied before validation.  The existing not_null_test.go
// tests ValidateRow only via the internal package; this adds parallel-tagged
// coverage of the two reachable statements (ApplyDefaults + ValidateInsert).
func TestValidateRow_DefaultFill(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name: "defaults_table",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "label", Type: "TEXT", NotNull: true, Default: "n/a"},
		},
	}
	nnc := NewNotNullConstraint(table)

	// "label" is missing but has a default — ValidateRow must apply it and succeed.
	values := map[string]interface{}{"id": int64(1)}
	if err := nnc.ValidateRow(values); err != nil {
		t.Fatalf("ValidateRow with defaults: unexpected error: %v", err)
	}
	if values["label"] != "n/a" {
		t.Errorf("ValidateRow: expected default 'n/a' for label, got %v", values["label"])
	}
}

// TestValidateRow_MissingNoDefault covers the ValidateInsert-returns-error
// path inside ValidateRow (i.e. ApplyDefaults succeeds but ValidateInsert
// finds a NOT NULL column with no default and no value).
func TestValidateRow_MissingNoDefault(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name: "no_default_table",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "required", Type: "TEXT", NotNull: true}, // no Default
		},
	}
	nnc := NewNotNullConstraint(table)

	values := map[string]interface{}{"id": int64(2)}
	err := nnc.ValidateRow(values)
	if err == nil {
		t.Fatal("ValidateRow: expected NOT NULL error for missing required column, got nil")
	}
	if !strings.Contains(err.Error(), "NOT NULL") {
		t.Errorf("ValidateRow: error should mention NOT NULL, got: %v", err)
	}
}

// TestHasAutoIncrement_MissingPKColumn covers the `!ok` branch in
// HasAutoIncrement (primary_key.go line 311-313) where the column named in
// PrimaryKey does not exist in the Columns slice.
func TestHasAutoIncrement_MissingPKColumn(t *testing.T) {
	t.Parallel()

	// Table claims "ghost" as its PK, but "ghost" is not in Columns.
	table := &schema.Table{
		Name:       "ghost_pk",
		PrimaryKey: []string{"ghost"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}
	pk := NewPrimaryKeyConstraint(table, nil, nil)

	if pk.HasAutoIncrement() {
		t.Error("HasAutoIncrement: expected false when PK column not found, got true")
	}
}

// TestCreateBackingIndex_TableNotFound covers the "table not found" error
// path in CreateBackingIndex (unique.go line 404-406).
func TestCreateBackingIndex_TableNotFound(t *testing.T) {
	t.Parallel()

	sch := schema.NewSchema()
	// Do NOT add the table to the schema — the constraint references a
	// non-existent table.
	uc := NewUniqueConstraint("uq_email", "nonexistent_table", []string{"email"})

	err := uc.CreateBackingIndex(sch, nil)
	if err == nil {
		t.Fatal("CreateBackingIndex: expected error for missing table, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent_table") {
		t.Errorf("CreateBackingIndex: error should mention table name, got: %v", err)
	}
}

// TestUniqueConstraint_AllNullValues covers the early-return path in Validate
// (unique.go) where all constraint columns are NULL — no duplicate check needed.
func TestUniqueConstraint_AllNullValues(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name:     "nullable_unique",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}
	uc := NewUniqueConstraint("", table.Name, []string{"email"})

	// Both id and email are NULL — the SQL standard allows multiple NULLs in
	// a UNIQUE column, so this should return nil without scanning the B-tree.
	values := map[string]interface{}{
		"id":    nil,
		"email": nil,
	}
	if err := uc.Validate(table, nil, values, 1); err != nil {
		t.Fatalf("Validate all-NULL: unexpected error: %v", err)
	}
}

// TestUniqueViolationError_WithName covers the named-constraint branch of
// UniqueViolationError.Error() (unique.go line 49-50).
func TestUniqueViolationError_WithName(t *testing.T) {
	t.Parallel()

	e := &UniqueViolationError{
		ConstraintName: "uq_email",
		TableName:      "users",
		Columns:        []string{"email"},
	}
	msg := e.Error()
	if !strings.Contains(msg, "uq_email") {
		t.Errorf("UniqueViolationError with name: expected constraint name in message, got: %s", msg)
	}
}

// TestExtractUniqueConstraints_TableLevel covers the table-level UNIQUE
// constraint extraction branch in ExtractUniqueConstraints (unique.go).
func TestExtractUniqueConstraints_TableLevel(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name: "multi_col_unique",
		Columns: []*schema.Column{
			{Name: "a", Type: "INTEGER"},
			{Name: "b", Type: "TEXT"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintUnique,
				Name:    "uq_ab",
				Columns: []string{"a", "b"},
			},
		},
	}

	constraints := ExtractUniqueConstraints(table)
	if len(constraints) != 1 {
		t.Fatalf("ExtractUniqueConstraints: want 1 table-level constraint, got %d", len(constraints))
	}
	if constraints[0].Name != "uq_ab" {
		t.Errorf("ExtractUniqueConstraints: want name uq_ab, got %s", constraints[0].Name)
	}
}

// TestForeignKeyManager_ShouldDeferDeleteViolation_NotDeferred exercises the
// false branch in shouldDeferDeleteViolation when the constraint is not
// DeferrableInitiallyDeferred.
func TestForeignKeyManager_ShouldDeferDeleteViolation_NotDeferred(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "child",
		RefTable:   "parent",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
		OnDelete:   FKActionNone,
		Deferrable: DeferrableNone,
	}

	// Not in transaction and not deferred — must return false.
	result := m.shouldDeferDeleteViolation(fk, []interface{}{int64(1)})
	if result {
		t.Error("shouldDeferDeleteViolation: expected false for non-deferred constraint, got true")
	}
}

// TestForeignKeyManager_ShouldDeferDeleteViolation_DeferredNotNone exercises
// the branch where Deferrable == DeferrableInitiallyDeferred but OnDelete
// is not FKActionNone/FKActionNoAction, so it still returns false.
func TestForeignKeyManager_ShouldDeferDeleteViolation_DeferredNotNone(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	m.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		RefTable:   "parent",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade, // not None/NoAction
		Deferrable: DeferrableInitiallyDeferred,
	}

	result := m.shouldDeferDeleteViolation(fk, []interface{}{int64(1)})
	if result {
		t.Error("shouldDeferDeleteViolation: expected false for Cascade action even when deferred, got true")
	}
}

// TestForeignKeyManager_ShouldDeferDeleteViolation_TrueCase exercises the
// path where shouldDeferDeleteViolation returns true: constraint is
// DeferrableInitiallyDeferred, we are in a transaction, and OnDelete is
// FKActionNone.
func TestForeignKeyManager_ShouldDeferDeleteViolation_TrueCase(t *testing.T) {
	t.Parallel()

	m := NewForeignKeyManager()
	m.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		RefTable:   "parent",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
		OnDelete:   FKActionNone,
		Deferrable: DeferrableInitiallyDeferred,
	}
	m.AddConstraint(fk)

	result := m.shouldDeferDeleteViolation(fk, []interface{}{int64(99)})
	if !result {
		t.Error("shouldDeferDeleteViolation: expected true for deferred FK in transaction, got false")
	}
	if m.DeferredViolationCount() == 0 {
		t.Error("shouldDeferDeleteViolation: expected a deferred violation to be recorded")
	}
}

// TestValidateIntegerPKUpdate_NullNewValue covers primary_key.go line 171-173:
// validateIntegerPKUpdate returns an error when the new PK value is NULL.
func TestValidateIntegerPKUpdate_NullNewValue(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name:       "int_pk_table",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}
	pk := NewPrimaryKeyConstraint(table, nil, nil)

	// Setting the INTEGER PRIMARY KEY column to NULL must return an error.
	err := pk.validateIntegerPKUpdate(1, map[string]interface{}{"id": nil})
	if err == nil {
		t.Fatal("validateIntegerPKUpdate: expected error for NULL new PK value, got nil")
	}
	if !strings.Contains(err.Error(), "cannot be NULL") {
		t.Errorf("validateIntegerPKUpdate: error should mention NULL, got: %v", err)
	}
}

// TestValidateIntegerPKUpdate_NonIntValue covers primary_key.go line 175-178:
// validateIntegerPKUpdate returns an error when the new PK value has a type
// that cannot be converted to int64 (e.g. a plain string).
func TestValidateIntegerPKUpdate_NonIntValue(t *testing.T) {
	t.Parallel()

	table := &schema.Table{
		Name:       "int_pk_nonint",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}
	pk := NewPrimaryKeyConstraint(table, nil, nil)

	// A string value cannot be converted to int64 → convertToInt64 returns an error.
	err := pk.validateIntegerPKUpdate(1, map[string]interface{}{"id": "not-an-int"})
	if err == nil {
		t.Fatal("validateIntegerPKUpdate: expected conversion error for string PK value, got nil")
	}
	if !strings.Contains(err.Error(), "INTEGER") {
		t.Errorf("validateIntegerPKUpdate: error should mention INTEGER, got: %v", err)
	}
}
