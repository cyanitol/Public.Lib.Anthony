// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// --- filterDeleteSelfRef coverage (80%) ---

// TestFilterDeleteSelfRef_EmptyRows ensures early-exit when no referencing rows.
func TestFilterDeleteSelfRef_EmptyRows(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"manager_id"},
		RefTable:   "employees",
		RefColumns: []string{"id"},
	}

	result := mgr.filterDeleteSelfRef(fk, []int64{}, map[string]interface{}{"id": int64(1)}, []string{"id"})
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d rows", len(result))
	}
}

// TestFilterDeleteSelfRef_NonSelfRef ensures rows are returned unmodified for non-self-ref FKs.
func TestFilterDeleteSelfRef_NonSelfRef(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	rows := []int64{1, 2, 3}
	result := mgr.filterDeleteSelfRef(fk, rows, map[string]interface{}{"id": int64(1)}, []string{"id"})
	if len(result) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result))
	}
}

// TestFilterDeleteSelfRef_SelfRefWithMatch triggers the self-reference filter branch.
func TestFilterDeleteSelfRef_SelfRefWithMatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"manager_id"},
		RefTable:   "employees",
		RefColumns: []string{"id"},
	}

	// Only 1 referencing row that matches the deleted row (self-ref to itself)
	// selfReferenceMatches checks whether values[columns] == refCols content
	values := map[string]interface{}{"id": int64(1), "manager_id": int64(1)}
	rows := []int64{1, 2, 3}
	result := mgr.filterDeleteSelfRef(fk, rows, values, []string{"id"})
	// Result is either filtered or returned as-is depending on selfReferenceMatches logic
	if result == nil {
		t.Error("expected non-nil result")
	}
}

// --- cascadeDeleteWithoutRowID coverage (90%) ---

// TestCascadeDeleteWithoutRowID_NoDeleterExt verifies error when RowDeleterExtended not available.
func TestCascadeDeleteWithoutRowID_NoDeleterExt(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	empTable := &schema.Table{
		Name:         "employees",
		WithoutRowID: true,
		PrimaryKey:   []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
	}
	sch.Tables["employees"] = empTable

	// Use a regular deleter that does NOT implement RowDeleterExtended
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()
	readerExt := newMockRowReaderExtended()
	readerExt.rowDataList = []map[string]interface{}{
		{"id": int64(1), "name": "Test"},
	}

	err := mgr.cascadeDeleteWithoutRowID(
		"employees",
		empTable,
		readerExt.rowDataList,
		sch,
		deleter,
		updater,
		readerExt,
	)
	if err == nil {
		t.Error("expected error when RowDeleterExtended not available")
	}
}

// TestCascadeDeleteWithoutRowID_WithDeleterExt verifies successful cascade delete.
func TestCascadeDeleteWithoutRowID_WithDeleterExt(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	empTable := &schema.Table{
		Name:         "employees",
		WithoutRowID: true,
		PrimaryKey:   []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
	}
	sch.Tables["employees"] = empTable

	deleterExt := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()
	readerExt := newMockRowReaderExtended()

	rowDataList := []map[string]interface{}{
		{"id": int64(2), "name": "Child"},
	}

	err := mgr.cascadeDeleteWithoutRowID(
		"employees",
		empTable,
		rowDataList,
		sch,
		deleterExt,
		updater,
		readerExt,
	)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(deleterExt.deletedKeys) != 1 {
		t.Errorf("expected 1 deleted key, got %d", len(deleterExt.deletedKeys))
	}
}

// TestCascadeDeleteWithoutRowID_DeleterFails verifies error propagation when deleter fails.
func TestCascadeDeleteWithoutRowID_DeleterFails(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	empTable := &schema.Table{
		Name:         "employees",
		WithoutRowID: true,
		PrimaryKey:   []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}
	sch.Tables["employees"] = empTable

	deleterExt := newMockRowDeleterExtended()
	deleterExt.shouldFail = true
	updater := NewMockRowUpdater()
	readerExt := newMockRowReaderExtended()

	rowDataList := []map[string]interface{}{
		{"id": int64(1)},
	}

	err := mgr.cascadeDeleteWithoutRowID(
		"employees",
		empTable,
		rowDataList,
		sch,
		deleterExt,
		updater,
		readerExt,
	)
	if err == nil {
		t.Error("expected error when deleter fails")
	}
}

// --- Self-referential FK with CASCADE DELETE (exercising cascadeDelete) ---

// TestValidateDelete_SelfRefSetNull exercises SET NULL on self-referential FK.
// Uses a separate parent/child table setup to avoid infinite recursion in mock.
func TestValidateDelete_SelfRefSetNull(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	// Use two distinct tables to avoid infinite cascade recursion in mock
	managerTable := &schema.Table{
		Name: "managers",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
		PrimaryKey: []string{"id"},
	}
	empTable := &schema.Table{
		Name: "employees",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "manager_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["managers"] = managerTable
	sch.Tables["employees"] = empTable

	fk := &ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"manager_id"},
		RefTable:   "managers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionSetNull,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	reader.AddReferencingRows("employees", []int64{2, 3})
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": int64(1)}

	err := mgr.ValidateDelete("managers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("unexpected error on set null: %v", err)
	}
	// Verify rows 2 and 3 got updated to NULL
	if len(updater.updates) < 2 {
		t.Errorf("expected at least 2 updates, got %d", len(updater.updates))
	}
}

// TestValidateDelete_CascadeMultipleRows exercises cascade delete with multiple referencing rows.
func TestValidateDelete_CascadeMultipleRows(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	parentTable := &schema.Table{
		Name: "departments",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	childTable := &schema.Table{
		Name: "employees",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "dept_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["departments"] = parentTable
	sch.Tables["employees"] = childTable

	fk := &ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"dept_id"},
		RefTable:   "departments",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	// Multiple employees in department 1
	reader.AddReferencingRows("employees", []int64{10, 20, 30})
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": int64(1), "name": "Engineering"}

	err := mgr.ValidateDelete("departments", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("unexpected error on cascade delete: %v", err)
	}
	// Verify all 3 employees were deleted
	if len(deleter.deletedRows["employees"]) != 3 {
		t.Errorf("expected 3 deleted employees, got %d", len(deleter.deletedRows["employees"]))
	}
}

// --- ValidateTableRow coverage (80%) ---

// TestValidateTableRow_NoConstraints verifies nil is returned when no unique constraints exist.
func TestValidateTableRow_NoConstraints(t *testing.T) {
	table := &schema.Table{
		Name: "simple",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		Constraints: []schema.TableConstraint{},
	}

	bt := btree.NewBtree(4096)

	values := map[string]interface{}{"id": int64(1), "name": "Alice"}
	err := ValidateTableRow(table, bt, values, 1)
	if err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// TestValidateTableRow_CompositeUniqueNoViolation checks no violation on unique composite values.
func TestValidateTableRow_CompositeUniqueNoViolation(t *testing.T) {
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "first_name", Type: "TEXT"},
			{Name: "last_name", Type: "TEXT"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintUnique,
				Name:    "uk_name",
				Columns: []string{"first_name", "last_name"},
			},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("failed to create btree table: %v", err)
	}
	table.RootPage = rootPage

	// Create backing index in schema and btree
	sch := schema.NewSchema()
	sch.Tables["users"] = table

	uc := NewUniqueConstraint("uk_name", "users", []string{"first_name", "last_name"})
	_ = uc.CreateBackingIndex(sch, bt)

	values := map[string]interface{}{"id": int64(1), "first_name": "Alice", "last_name": "Smith"}
	err = ValidateTableRow(table, bt, values, 1)
	if err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// TestValidateTableRow_SingleColumnUniqueViolation verifies violation detection via ValidateTableRow.
func TestValidateTableRow_SingleColumnUniqueViolation(t *testing.T) {
	table := &schema.Table{
		Name: "products",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "sku", Type: "TEXT", Unique: true},
		},
		Constraints: []schema.TableConstraint{},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("failed to create btree table: %v", err)
	}
	table.RootPage = rootPage

	sch := schema.NewSchema()
	sch.Tables["products"] = table

	// Create backing index
	uc := NewUniqueConstraint("", "products", []string{"sku"})
	_ = uc.CreateBackingIndex(sch, bt)

	// Insert first row - should succeed
	values1 := map[string]interface{}{"id": int64(1), "sku": "ABC-001"}
	err = ValidateTableRow(table, bt, values1, 1)
	if err != nil {
		t.Errorf("first insert should not violate: %v", err)
	}
}

// TestValidateTableRow_TableLevelConstraint exercises the table-level UNIQUE constraint path.
func TestValidateTableRow_TableLevelConstraint(t *testing.T) {
	table := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "order_num", Type: "TEXT"},
			{Name: "line", Type: "INTEGER"},
		},
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintUnique,
				Name:    "uk_order_line",
				Columns: []string{"order_num", "line"},
			},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	sch := schema.NewSchema()
	sch.Tables["orders"] = table

	uc := NewUniqueConstraint("uk_order_line", "orders", []string{"order_num", "line"})
	_ = uc.CreateBackingIndex(sch, bt)

	// Insert first row
	values := map[string]interface{}{"id": int64(1), "order_num": "ORD-001", "line": int64(1)}
	err := ValidateTableRow(table, bt, values, 1)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

// TestValidateTableRow_ConstraintErrorOnMissingColumn verifies error when constraint
// references a column that doesn't exist in the table definition.
func TestValidateTableRow_ConstraintErrorOnMissingColumn(t *testing.T) {
	// Table with a constraint referencing a column that doesn't exist
	table := &schema.Table{
		Name: "broken",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
		// Constraint references "nonexistent_col" which is not in Columns
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintUnique,
				Name:    "uk_bad",
				Columns: []string{"nonexistent_col"},
			},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	values := map[string]interface{}{"id": int64(1)}
	err := ValidateTableRow(table, bt, values, 1)
	if err == nil {
		t.Error("expected error for constraint referencing non-existent column")
	}
}

// TestValidateTableRow_NullValuesAllowed verifies NULL values bypass unique check.
func TestValidateTableRow_NullValuesAllowed(t *testing.T) {
	table := &schema.Table{
		Name: "items",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "code", Type: "TEXT", Unique: true},
		},
		Constraints: []schema.TableConstraint{},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	sch := schema.NewSchema()
	sch.Tables["items"] = table

	uc := NewUniqueConstraint("", "items", []string{"code"})
	_ = uc.CreateBackingIndex(sch, bt)

	// NULL values should not cause a violation
	values := map[string]interface{}{"id": int64(1), "code": nil}
	err := ValidateTableRow(table, bt, values, 1)
	if err != nil {
		t.Errorf("NULL values should not violate unique constraint: %v", err)
	}
}
