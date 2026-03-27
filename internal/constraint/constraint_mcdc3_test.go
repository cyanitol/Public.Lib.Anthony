// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestMCDC3_ValidateInsert_DisabledManager covers the FK-disabled fast path.
//
// MC/DC for ValidateInsert:
//
//	C1: !m.IsEnabled() → return nil immediately
func TestMCDC3_ValidateInsert_DisabledManager(t *testing.T) {
	mgr := NewForeignKeyManager()
	// mgr.IsEnabled() is false by default

	err := mgr.ValidateInsert("t", map[string]interface{}{"id": 1}, nil, nil)
	if err != nil {
		t.Errorf("ValidateInsert with disabled manager should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_NoConstraints covers the no-constraints fast path.
//
// MC/DC for ValidateInsert:
//
//	C1: IsEnabled()=true
//	C2: len(constraints)==0 → return nil (covered here)
func TestMCDC3_ValidateInsert_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	err := mgr.ValidateInsert("no_such_table", map[string]interface{}{"id": 1}, nil, nil)
	if err != nil {
		t.Errorf("ValidateInsert with no constraints should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_InvalidSchema covers the bad schema type-assertion path.
//
// MC/DC for ValidateInsert:
//
//	C1: IsEnabled()=true, len>0
//	C2: schemaObj not *schema.Schema → return nil (type assertion fails)
func TestMCDC3_ValidateInsert_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:      "child",
		Columns:    []string{"parent_id"},
		RefTable:   "parent",
		RefColumns: []string{"id"},
		OnDelete:   FKActionNone,
	})

	// Pass a non-schema type so type assertion fails → should return nil (no crash)
	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": 1}, "not a schema", nil)
	if err != nil {
		t.Errorf("ValidateInsert with invalid schema should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_InvalidReader covers the bad reader type-assertion path.
//
// MC/DC for ValidateInsert:
//
//	C3: rowReader not RowReader → return nil
func TestMCDC3_ValidateInsert_InvalidReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": 1}, sch, "not a reader")
	if err != nil {
		t.Errorf("ValidateInsert with invalid reader should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_NullFK covers the NULL FK value fast path.
//
// MC/DC for validateInsertConstraint:
//
//	C1: hasNull=true → return nil (NULL FK values don't violate constraint)
func TestMCDC3_ValidateInsert_NullFK(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	// NULL FK value → constraint not enforced
	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": nil}, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert with NULL FK should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateDelete_TypeAssertionFailure covers each type-assertion failure
// path in validateDeleteTypeAssertions.
//
// MC/DC for validateDeleteTypeAssertions:
//
//	C1: schemaObj not *schema.Schema → return false
//	C2: rowReader not RowReader      → return false
//	C3: rowDeleter not RowDeleter    → return false
//	C4: rowUpdater not RowUpdater    → return false
func TestMCDC3_ValidateDelete_TypeAssertionFailure(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	tests := []struct {
		name      string
		schemaObj interface{}
		reader    interface{}
		deleter   interface{}
		updater   interface{}
	}{
		{"bad schema", "not schema", reader, deleter, updater},
		{"bad reader", sch, "not reader", deleter, updater},
		{"bad deleter", sch, reader, "not deleter", updater},
		{"bad updater", sch, reader, deleter, "not updater"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// With bad type assertion, ValidateDelete should return nil (not panic)
			err := mgr.ValidateDelete("parent", map[string]interface{}{"id": 1},
				tt.schemaObj, tt.reader, tt.deleter, tt.updater)
			if err != nil {
				t.Errorf("ValidateDelete with %s should return nil, got: %v", tt.name, err)
			}
		})
	}
}

// TestMCDC3_ValidateOutgoingReferences_ColumnsUnchanged covers the path where
// FK columns are not changed (no-op path).
//
// MC/DC for validateOutgoingReferences:
//
//	C1: !columnsChanged → continue (skip this constraint)
func TestMCDC3_ValidateOutgoingReferences_ColumnsUnchanged(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	sch.AddTableDirect(&schema.Table{
		Name:    "parent",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	})
	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{"parent_id": 10, "name": "old"}
	// FK column "parent_id" not in newValues → columns not changed
	newValues := map[string]interface{}{"name": "new"}

	err := mgr.ValidateUpdate("child", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate with unchanged FK should return nil, got: %v", err)
	}
}

// TestMCDC3_HasAutoIncrement_AllBranches covers all branches of HasAutoIncrement.
//
// MC/DC for HasAutoIncrement:
//
//	C1: !isIntegerPrimaryKey() → return false
//	C2: isIntegerPrimaryKey() AND col not found → return false
//	C3: isIntegerPrimaryKey() AND col.Autoincrement=false → return false
//	C4: isIntegerPrimaryKey() AND col.Autoincrement=true  → return true
func TestMCDC3_HasAutoIncrement_AllBranches(t *testing.T) {
	bt := btree.NewBtree(4096)

	tests := []struct {
		name   string
		col    *schema.Column
		pkCols []string
		want   bool
	}{
		{
			name:   "no PK (empty PrimaryKey)",
			col:    &schema.Column{Name: "id", Type: "INTEGER", PrimaryKey: false},
			pkCols: []string{},
			want:   false,
		},
		{
			name:   "non-integer PK",
			col:    &schema.Column{Name: "code", Type: "TEXT", PrimaryKey: true},
			pkCols: []string{"code"},
			want:   false,
		},
		{
			name:   "integer PK no autoincrement",
			col:    &schema.Column{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: false},
			pkCols: []string{"id"},
			want:   false,
		},
		{
			name:   "integer PK with autoincrement",
			col:    &schema.Column{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: true},
			pkCols: []string{"id"},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &schema.Table{
				Name:       "t_" + tt.name,
				PrimaryKey: tt.pkCols,
				Columns:    []*schema.Column{tt.col},
			}
			pk := NewPrimaryKeyConstraint(table, bt, nil)
			got := pk.HasAutoIncrement()
			if got != tt.want {
				t.Errorf("HasAutoIncrement()=%v, want %v", got, tt.want)
			}
		})
	}
}

// TestMCDC3_ExtractCheckConstraints_TableLevel covers the table-level
// CHECK constraint extraction path (non-column-level).
//
// MC/DC for extractCheckConstraints:
//
//	C1: col.Check != "" → extract column-level (covered by other tests)
//	C2: tc.Type == ConstraintCheck AND tc.Expression != "" → extract table-level (here)
func TestMCDC3_ExtractCheckConstraints_TableLevel(t *testing.T) {
	table := &schema.Table{
		Name: "tbl_check",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "age", Type: "INTEGER"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:       schema.ConstraintCheck,
				Expression: "age > 0",
				Name:       "age_positive",
			},
		},
	}

	v, err := NewCheckValidator(table)
	if err != nil {
		t.Fatalf("NewCheckValidator: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil CheckValidator")
	}
	if len(v.constraints) == 0 {
		t.Error("expected at least one check constraint from table level")
	}
}

// TestMCDC3_ExtractCheckConstraints_SkipsEmptyExpression covers the path where
// a table constraint has empty expression (skipped).
//
// MC/DC for extractCheckConstraints:
//
//	C2: tc.Type==ConstraintCheck AND tc.Expression=="" → skip (covered here)
func TestMCDC3_ExtractCheckConstraints_SkipsEmptyExpression(t *testing.T) {
	table := &schema.Table{
		Name: "tbl_no_check_expr",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:       schema.ConstraintCheck,
				Expression: "", // empty expression → should be skipped
			},
		},
	}

	v, err := NewCheckValidator(table)
	if err != nil {
		t.Fatalf("NewCheckValidator: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil CheckValidator")
	}
	// No constraints should have been extracted since expression was empty.
	if len(v.constraints) != 0 {
		t.Errorf("expected 0 constraints from empty expression, got %d", len(v.constraints))
	}
}

// TestMCDC3_CreateBackingIndex_AlreadyExists covers the "index already exists"
// early-return path in CreateBackingIndex.
//
// MC/DC for CreateBackingIndex:
//
//	C1: index already exists → return nil immediately (covered here)
//	C2: index doesn't exist  → create it (covered implicitly)
func TestMCDC3_CreateBackingIndex_AlreadyExists(t *testing.T) {
	bt := btree.NewBtree(4096)
	sch := schema.NewSchema()
	sch.AddTableDirect(&schema.Table{
		Name:    "u_tbl",
		Columns: []*schema.Column{{Name: "email", Type: "TEXT"}},
	})

	// Pre-create the backing index.
	sch.AddIndexDirect(&schema.Index{
		Name:    "uq_u_tbl_email",
		Table:   "u_tbl",
		Columns: []string{"email"},
		Unique:  true,
	})

	uc := &UniqueConstraint{
		Name:      "uq_email",
		TableName: "u_tbl",
		IndexName: "uq_u_tbl_email",
		Columns:   []string{"email"},
	}

	// Should return nil because the index already exists.
	err := uc.CreateBackingIndex(sch, bt)
	if err != nil {
		t.Errorf("CreateBackingIndex when index exists should return nil, got: %v", err)
	}
}

// TestMCDC3_CreateBackingIndex_MissingTable covers the missing-table error path.
//
// MC/DC for CreateBackingIndex:
//
//	C2: table not found → return error
func TestMCDC3_CreateBackingIndex_MissingTable(t *testing.T) {
	bt := btree.NewBtree(4096)
	sch := schema.NewSchema()

	uc := &UniqueConstraint{
		Name:      "uq_missing",
		TableName: "no_such_table",
		IndexName: "uq_missing_idx",
		Columns:   []string{"id"},
	}

	err := uc.CreateBackingIndex(sch, bt)
	if err == nil {
		t.Error("CreateBackingIndex on missing table should return error")
	}
}

// TestMCDC3_ValidateInsert_WithFKDisabled verifies that with FK disabled,
// even a constraint-violating insert is not checked.
func TestMCDC3_ValidateInsert_WithFKDisabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	// FK checking disabled
	mgr.SetEnabled(false)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	// No parent row exists, but FK is disabled → should succeed
	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": 999}, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert with FK disabled should return nil, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_SelfReference covers the self-reference path in
// validateInsertConstraint.
//
// MC/DC for validateInsertConstraint:
//
//	C1: fk.Table==fk.RefTable (self-ref) AND selfReferenceMatches → return nil
func TestMCDC3_ValidateInsert_SelfReference(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	// Self-referencing FK: employees.manager_id → employees.id
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"manager_id"},
		RefTable:   "employees",
		RefColumns: []string{"id"},
	})

	sch := schema.NewSchema()
	sch.AddTableDirect(&schema.Table{
		Name: "employees",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "manager_id", Type: "INTEGER"},
		},
	})
	reader := NewMockRowReader()

	// Row references itself (id==manager_id) → constraint satisfied
	values := map[string]interface{}{
		"id":         1,
		"manager_id": 1, // references own id
	}
	err := mgr.ValidateInsert("employees", values, sch, reader)
	if err != nil {
		t.Errorf("self-referencing insert should succeed, got: %v", err)
	}
}

// TestMCDC3_ValidateInsert_ReferencedRowExists covers the successful FK validation
// path when the referenced row exists.
func TestMCDC3_ValidateInsert_ReferencedRowExists(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:      "child",
		Columns:    []string{"parent_id"},
		RefTable:   "parent",
		RefColumns: []string{"id"},
	})

	sch := schema.NewSchema()
	sch.AddTableDirect(&schema.Table{
		Name:       "parent",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	})

	reader := NewMockRowReader()
	reader.AddRow("parent", []string{"id"}, []interface{}{int64(10)})

	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": int64(10)}, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert with existing parent should succeed, got: %v", err)
	}
}

// TestMCDC3_ValidateDelete_Disabled covers the disabled fast path.
//
// MC/DC for ValidateDelete:
//
//	C1: !m.IsEnabled() → return nil immediately
func TestMCDC3_ValidateDelete_Disabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(false)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	err := mgr.ValidateDelete("parent",
		map[string]interface{}{"id": 1},
		schema.NewSchema(),
		NewMockRowReader(),
		NewMockRowDeleter(),
		NewMockRowUpdater(),
	)
	if err != nil {
		t.Errorf("ValidateDelete disabled should return nil, got: %v", err)
	}
}

// TestMCDC3_FindViolationsAllTables_NoConstraints covers the path where
// findViolationsAllTables has no constraints registered.
func TestMCDC3_FindViolationsAllTables_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	violations, err := mgr.FindViolations("", sch, reader)
	if err != nil {
		t.Fatalf("FindViolations: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations with no constraints, got %d", len(violations))
	}
}

// TestMCDC3_CheckTableViolations_NoConstraintsForTable covers the path where
// no constraints exist for the specific table in checkTableViolations.
func TestMCDC3_CheckTableViolations_NoConstraintsForTable(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "other_table",
		Columns:  []string{"ref_id"},
		RefTable: "parent",
	})

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	// "my_table" has no constraints → should return empty
	violations, err := mgr.FindViolations("my_table", sch, reader)
	if err != nil {
		t.Fatalf("FindViolations: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations for unconstrained table, got %d", len(violations))
	}
}

// TestMCDC3_ValidateReference_RefTableNotFound covers the error path in
// validateReference when the referenced table doesn't exist in schema.
func TestMCDC3_ValidateReference_RefTableNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "nonexistent_parent",
	})

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	// Reference table doesn't exist → error
	err := mgr.ValidateInsert("child", map[string]interface{}{"parent_id": 1}, sch, reader)
	if err == nil {
		t.Error("ValidateInsert should fail when reference table doesn't exist")
	}
}

// TestMCDC3_NotNullValidateRow_ApplyDefaultsError covers the error path
// from ApplyDefaults in ValidateRow.
//
// This indirectly covers ValidateRow's "apply defaults first" path.
// MC/DC for ValidateRow:
//
//	C1: ApplyDefaults returns error → propagate error
//	C2: ApplyDefaults succeeds, ValidateInsert returns error → propagate
//	C3: both succeed → return nil
func TestMCDC3_NotNullValidateRow_ApplyDefaultsError(t *testing.T) {
	table := &schema.Table{
		Name: "vr_test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "name", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)

	tests := []struct {
		name    string
		values  map[string]interface{}
		wantErr bool
	}{
		{
			name:    "all columns provided",
			values:  map[string]interface{}{"id": 1, "name": "test"},
			wantErr: false,
		},
		{
			name:    "null required column",
			values:  map[string]interface{}{"id": 1, "name": nil},
			wantErr: true,
		},
		{
			name:    "missing required column",
			values:  map[string]interface{}{"id": 1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := nnc.ValidateRow(tt.values)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestMCDC3_ValidateUpdate_Disabled covers the disabled fast path for ValidateUpdate.
//
// MC/DC for ValidateUpdate:
//
//	C1: !m.IsEnabled() → return nil
func TestMCDC3_ValidateUpdate_Disabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(false)
	mgr.AddConstraint(&ForeignKeyConstraint{
		Table:    "child",
		Columns:  []string{"parent_id"},
		RefTable: "parent",
	})

	err := mgr.ValidateUpdate("child",
		map[string]interface{}{"parent_id": 1},
		map[string]interface{}{"parent_id": 999},
		schema.NewSchema(),
		NewMockRowReader(),
		NewMockRowUpdater(),
	)
	if err != nil {
		t.Errorf("ValidateUpdate disabled should return nil, got: %v", err)
	}
}
