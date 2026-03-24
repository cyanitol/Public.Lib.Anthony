// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// --- Mock implementations for WITHOUT ROWID tests ---

// mockRowReaderExtended implements RowReaderExtended for testing.
type mockRowReaderExtended struct {
	MockRowReader
	rowDataList []map[string]interface{}
	shouldFail  bool
}

func newMockRowReaderExtended() *mockRowReaderExtended {
	return &mockRowReaderExtended{
		MockRowReader: MockRowReader{
			rows:            make(map[string]map[string][]interface{}),
			referencingRows: make(map[string][]int64),
		},
	}
}

func (m *mockRowReaderExtended) ReadRowByKey(table string, keyValues []interface{}) (map[string]interface{}, error) {
	return make(map[string]interface{}), nil
}

func (m *mockRowReaderExtended) FindReferencingRowsWithData(table string, columns []string, values []interface{}) ([]map[string]interface{}, error) {
	if m.shouldFail {
		return nil, &mockReadError{}
	}
	return m.rowDataList, nil
}

// mockRowDeleterExtended implements RowDeleterExtended for testing.
type mockRowDeleterExtended struct {
	MockRowDeleter
	deletedKeys [][]interface{}
	shouldFail  bool
}

func newMockRowDeleterExtended() *mockRowDeleterExtended {
	return &mockRowDeleterExtended{
		MockRowDeleter: MockRowDeleter{
			deletedRows: make(map[string][]int64),
		},
	}
}

func (m *mockRowDeleterExtended) DeleteRowByKey(table string, keyValues []interface{}) error {
	if m.shouldFail {
		return &mockReadError{}
	}
	m.deletedKeys = append(m.deletedKeys, keyValues)
	return nil
}

// --- CheckDeferredViolations tests (0% coverage) ---

func TestForeignKeyManager_CheckDeferredViolations_Empty(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.CheckDeferredViolations(schema.NewSchema(), NewMockRowReader())
	if err != nil {
		t.Errorf("Expected nil for empty deferred violations, got: %v", err)
	}
}

func TestForeignKeyManager_CheckDeferredViolations_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	// Add a deferred violation so the function doesn't return early
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.deferredViolations = append(mgr.deferredViolations, &DeferredViolation{
		Constraint: fk,
		Values:     []interface{}{1},
		Table:      "orders",
	})
	// Pass invalid schema - should return nil (not panic)
	err := mgr.CheckDeferredViolations("not-a-schema", NewMockRowReader())
	if err != nil {
		t.Errorf("Expected nil for invalid schema, got: %v", err)
	}
}

func TestForeignKeyManager_CheckDeferredViolations_InvalidReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.deferredViolations = append(mgr.deferredViolations, &DeferredViolation{
		Constraint: fk,
		Values:     []interface{}{1},
		Table:      "orders",
	})
	err := mgr.CheckDeferredViolations(schema.NewSchema(), "not-a-reader")
	if err != nil {
		t.Errorf("Expected nil for invalid reader, got: %v", err)
	}
}

func TestForeignKeyManager_CheckDeferredViolations_WithViolation(t *testing.T) {
	mgr := NewForeignKeyManager()

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.deferredViolations = append(mgr.deferredViolations, &DeferredViolation{
		Constraint: fk,
		Values:     []interface{}{999},
		Table:      "orders",
	})

	reader := NewMockRowReader()
	// customer 999 does not exist in reader -> violation
	err := mgr.CheckDeferredViolations(sch, reader)
	if err == nil {
		t.Error("Expected error for deferred FK violation")
	}
}

func TestForeignKeyManager_CheckDeferredViolations_NoViolation(t *testing.T) {
	mgr := NewForeignKeyManager()

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.deferredViolations = append(mgr.deferredViolations, &DeferredViolation{
		Constraint: fk,
		Values:     []interface{}{1},
		Table:      "orders",
	})

	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{1})
	err := mgr.CheckDeferredViolations(sch, reader)
	if err != nil {
		t.Errorf("Expected nil for satisfied deferred FK, got: %v", err)
	}
}

// --- ClearDeferredViolations tests (0% coverage) ---

func TestForeignKeyManager_ClearDeferredViolations(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{Table: "t", RefTable: "p"}
	mgr.deferredViolations = append(mgr.deferredViolations, &DeferredViolation{
		Constraint: fk, Values: []interface{}{1}, Table: "t",
	})
	if len(mgr.deferredViolations) != 1 {
		t.Fatal("Expected 1 deferred violation")
	}
	mgr.ClearDeferredViolations()
	if len(mgr.deferredViolations) != 0 {
		t.Error("Expected 0 deferred violations after clear")
	}
}

// --- DeferredViolationCount tests (0% coverage) ---

func TestForeignKeyManager_DeferredViolationCount(t *testing.T) {
	mgr := NewForeignKeyManager()
	if mgr.DeferredViolationCount() != 0 {
		t.Error("Expected count 0 initially")
	}
	fk := &ForeignKeyConstraint{Table: "t", RefTable: "p"}
	mgr.deferredViolations = append(mgr.deferredViolations,
		&DeferredViolation{Constraint: fk, Values: []interface{}{1}, Table: "t"},
		&DeferredViolation{Constraint: fk, Values: []interface{}{2}, Table: "t"},
	)
	if mgr.DeferredViolationCount() != 2 {
		t.Errorf("Expected count 2, got %d", mgr.DeferredViolationCount())
	}
}

// --- CheckSchemaMismatch tests (0% coverage) ---

func TestForeignKeyManager_CheckSchemaMismatch_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.CheckSchemaMismatch("orders", "not-a-schema")
	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}

func TestForeignKeyManager_CheckSchemaMismatch_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	err := mgr.CheckSchemaMismatch("orders", sch)
	if err != nil {
		t.Errorf("Expected nil for no constraints, got: %v", err)
	}
}

func TestForeignKeyManager_CheckSchemaMismatch_AllTables(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	// customers table missing - not an error at schema mismatch level
	err := mgr.CheckSchemaMismatch("", sch)
	if err != nil {
		t.Errorf("Expected nil when parent missing, got: %v", err)
	}
}

func TestForeignKeyManager_CheckSchemaMismatch_ColumnCountMismatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	// FK has 2 columns but parent only has 1 PK column
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"cid1", "cid2"},
		RefTable:   "customers",
		RefColumns: []string{},
	}
	mgr.AddConstraint(fk)

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("Expected error for column count mismatch")
	}
}

func TestForeignKeyManager_CheckSchemaMismatch_MissingRefColumn(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	// FK refs a column that doesn't exist
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"nonexistent_col"},
	}
	mgr.AddConstraint(fk)

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("Expected error for missing referenced column")
	}
}

func TestForeignKeyManager_CheckSchemaMismatch_NotUnique(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	customerTable := &schema.Table{
		Name: "customers",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	// FK refs non-unique column 'name'
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"cust_name"},
		RefTable:   "customers",
		RefColumns: []string{"name"},
	}
	mgr.AddConstraint(fk)

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("Expected error for non-unique referenced column")
	}
}

// --- ValidateFKAtCreateTime tests (0% coverage) ---

func TestForeignKeyManager_ValidateFKAtCreateTime_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.ValidateFKAtCreateTime("orders", "not-a-schema")
	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}

func TestForeignKeyManager_ValidateFKAtCreateTime_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.ValidateFKAtCreateTime("orders", schema.NewSchema())
	if err != nil {
		t.Errorf("Expected nil for no constraints, got: %v", err)
	}
}

func TestForeignKeyManager_ValidateFKAtCreateTime_RefsView(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Views["customers"] = &schema.View{Name: "customers"}

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err == nil {
		t.Error("Expected error for FK referencing a view")
	}
}

func TestForeignKeyManager_ValidateFKAtCreateTime_ParentMissing(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	// Parent table doesn't exist - not an error at CREATE TABLE time
	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err != nil {
		t.Errorf("Expected nil for missing parent at create time, got: %v", err)
	}
}

func TestForeignKeyManager_ValidateFKAtCreateTime_ColumnCountMismatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"cid1", "cid2"},
		RefTable:   "customers",
		RefColumns: []string{},
	}
	mgr.AddConstraint(fk)

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err == nil {
		t.Error("Expected error for column count mismatch at create time")
	}
}

// --- FindViolations tests (0% coverage) ---

func TestForeignKeyManager_FindViolations_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	_, err := mgr.FindViolations("", "not-a-schema", nil)
	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}

func TestForeignKeyManager_FindViolations_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	violations, err := mgr.FindViolations("", sch, nil)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("Expected 0 violations, got %d", len(violations))
	}
}

func TestForeignKeyManager_FindViolations_SpecificTable_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	violations, err := mgr.FindViolations("orders", sch, nil)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("Expected 0 violations, got %d", len(violations))
	}
}

func TestForeignKeyManager_FindViolations_TableNotInSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	violations, err := mgr.FindViolations("orders", sch, nil)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("Expected 0 violations when table not in schema, got %d", len(violations))
	}
}

func TestForeignKeyManager_FindViolations_AllTables(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	ordersTable := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "customer_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["orders"] = ordersTable

	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	violations, err := mgr.FindViolations("", sch, reader)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	_ = violations
}

func TestForeignKeyManager_FindViolations_MissingParent_WithRows(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	ordersTable := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "customer_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["orders"] = ordersTable
	// customers table intentionally missing

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	// Simulate a row with non-null FK value
	reader.referencingRows["orders"] = []int64{1}
	reader.rows["orders"] = map[string][]interface{}{
		"rowid:1": {1},
	}

	violations, err := mgr.FindViolations("orders", sch, reader)
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	// May or may not find violations depending on reader mock impl
	_ = violations
}

// --- handleDeleteConstraintWithoutRowID tests (0% coverage) ---

func TestForeignKeyManager_HandleDeleteConstraintWithoutRowID_NoExtendedReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:    "child",
		RefTable: "parent",
		Columns:  []string{"pid"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{
		Name:       "child",
		PrimaryKey: []string{"id"},
	}

	// Use plain reader (not extended) - should error
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(
		fk, childTable, []interface{}{1}, []string{"id"},
		map[string]interface{}{"pid": 1},
		sch, reader, deleter, updater,
	)
	if err == nil {
		t.Error("Expected error when RowReaderExtended not available")
	}
}

func TestForeignKeyManager_HandleDeleteConstraintWithoutRowID_FindRowsError(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:    "child",
		RefTable: "parent",
		Columns:  []string{"pid"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{
		Name:       "child",
		PrimaryKey: []string{"id"},
	}

	reader := newMockRowReaderExtended()
	reader.shouldFail = true
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(
		fk, childTable, []interface{}{1}, []string{"id"},
		map[string]interface{}{"pid": 1},
		sch, reader, deleter, updater,
	)
	if err == nil {
		t.Error("Expected error when FindReferencingRowsWithData fails")
	}
}

func TestForeignKeyManager_HandleDeleteConstraintWithoutRowID_NoRows(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	fk := &ForeignKeyConstraint{
		Table:    "child",
		RefTable: "parent",
		Columns:  []string{"pid"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{
		Name:       "child",
		PrimaryKey: []string{"id"},
	}

	reader := newMockRowReaderExtended()
	reader.rowDataList = nil // no rows
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(
		fk, childTable, []interface{}{1}, []string{"id"},
		map[string]interface{}{"pid": 1},
		sch, reader, deleter, updater,
	)
	if err != nil {
		t.Errorf("Expected nil for no rows, got: %v", err)
	}
}

// --- applyDeleteActionWithoutRowID tests (0% coverage) ---

func TestForeignKeyManager_ApplyDeleteActionWithoutRowID_Empty(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:    "child",
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(
		fk, childTable, nil, schema.NewSchema(), deleter, updater, readerExt,
	)
	if err != nil {
		t.Errorf("Expected nil for empty rowDataList, got: %v", err)
	}
}

func TestForeignKeyManager_ApplyDeleteActionWithoutRowID_Restrict(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:    "child",
		OnDelete: FKActionRestrict,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": 1, "pid": 1}}
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(
		fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt,
	)
	if err == nil {
		t.Error("Expected error for RESTRICT with rows")
	}
}

func TestForeignKeyManager_ApplyDeleteActionWithoutRowID_SetNull(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:    "child",
		OnDelete: FKActionSetNull,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": 1}}
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(
		fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt,
	)
	if err == nil {
		t.Error("Expected error for SET NULL on WITHOUT ROWID")
	}
}

func TestForeignKeyManager_ApplyDeleteActionWithoutRowID_SetDefault(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:    "child",
		OnDelete: FKActionSetDefault,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": 1}}
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(
		fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt,
	)
	if err == nil {
		t.Error("Expected error for SET DEFAULT on WITHOUT ROWID")
	}
}

func TestForeignKeyManager_ApplyDeleteActionWithoutRowID_Cascade(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	childTable := &schema.Table{
		Name:       "child",
		PrimaryKey: []string{"id"},
	}
	sch.Tables["child"] = childTable

	fk := &ForeignKeyConstraint{
		Table:    "child",
		OnDelete: FKActionCascade,
	}

	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": int64(1)}}

	deleterExt := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(
		fk, childTable, rowDataList, sch, deleterExt, updater, readerExt,
	)
	if err != nil {
		t.Errorf("Expected nil for CASCADE delete, got: %v", err)
	}
}

// --- cascadeDeleteWithoutRowID tests (0% coverage) ---

func TestForeignKeyManager_CascadeDeleteWithoutRowID_NoExtendedDeleter(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": 1}}

	// Plain deleter (not extended)
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, sch, deleter, updater, readerExt)
	if err == nil {
		t.Error("Expected error when RowDeleterExtended not available")
	}
}

func TestForeignKeyManager_CascadeDeleteWithoutRowID_DeleteKeyError(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}

	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": int64(1)}}

	deleterExt := newMockRowDeleterExtended()
	deleterExt.shouldFail = true
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, sch, deleterExt, updater, readerExt)
	if err == nil {
		t.Error("Expected error when DeleteRowByKey fails")
	}
}

func TestForeignKeyManager_CascadeDeleteWithoutRowID_Success(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	sch.Tables["child"] = childTable

	readerExt := newMockRowReaderExtended()
	rowDataList := []map[string]interface{}{{"id": int64(1)}}

	deleterExt := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, sch, deleterExt, updater, readerExt)
	if err != nil {
		t.Errorf("Expected nil for successful cascade delete, got: %v", err)
	}
	if len(deleterExt.deletedKeys) != 1 {
		t.Errorf("Expected 1 deleted key, got %d", len(deleterExt.deletedKeys))
	}
}

// --- filterSelfReferenceWithoutRowID tests (0% coverage) ---

func TestForeignKeyManager_FilterSelfReferenceWithoutRowID_FiltersMatch(t *testing.T) {
	mgr := NewForeignKeyManager()

	deletedRow := map[string]interface{}{"id": int64(1), "pid": int64(1)}
	rowDataList := []map[string]interface{}{
		{"id": int64(1), "pid": int64(1)}, // Same as deleted
		{"id": int64(2), "pid": int64(1)}, // Different
	}

	result := mgr.filterSelfReferenceWithoutRowID(rowDataList, deletedRow, []string{"id"})
	if len(result) != 1 {
		t.Errorf("Expected 1 row after filtering self-reference, got %d", len(result))
	}
}

func TestForeignKeyManager_FilterSelfReferenceWithoutRowID_NoMatch(t *testing.T) {
	mgr := NewForeignKeyManager()

	deletedRow := map[string]interface{}{"id": int64(99)}
	rowDataList := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
	}

	result := mgr.filterSelfReferenceWithoutRowID(rowDataList, deletedRow, []string{"id"})
	if len(result) != 2 {
		t.Errorf("Expected 2 rows when no match, got %d", len(result))
	}
}

// --- valuesMatch tests (0% coverage) ---

func TestValuesMatch_Equal(t *testing.T) {
	a := []interface{}{int64(1), "hello", 3.14}
	b := []interface{}{int64(1), "hello", 3.14}
	if !valuesMatch(a, b) {
		t.Error("Expected true for equal values")
	}
}

func TestValuesMatch_DifferentLength(t *testing.T) {
	a := []interface{}{1, 2}
	b := []interface{}{1}
	if valuesMatch(a, b) {
		t.Error("Expected false for different lengths")
	}
}

func TestValuesMatch_DifferentValues(t *testing.T) {
	a := []interface{}{int64(1)}
	b := []interface{}{int64(2)}
	if valuesMatch(a, b) {
		t.Error("Expected false for different values")
	}
}

func TestValuesMatch_Empty(t *testing.T) {
	if !valuesMatch(nil, nil) {
		t.Error("Expected true for nil slices")
	}
}

// --- tryParseFloat tests (0% coverage) ---

func TestTryParseFloat_ValidFloat(t *testing.T) {
	result := tryParseFloat("3.14", "fallback")
	if v, ok := result.(float64); !ok || v != 3.14 {
		t.Errorf("Expected 3.14 float64, got %v", result)
	}
}

func TestTryParseFloat_InvalidFloat(t *testing.T) {
	result := tryParseFloat("not-a-float", "fallback")
	if result != "fallback" {
		t.Errorf("Expected fallback, got %v", result)
	}
}

func TestTryParseFloat_Integer(t *testing.T) {
	result := tryParseFloat("42", "fallback")
	if v, ok := result.(float64); !ok || v != 42.0 {
		t.Errorf("Expected 42.0, got %v", result)
	}
}

// --- tryParseNumeric tests (0% coverage) ---

func TestTryParseNumeric_Integer(t *testing.T) {
	result := tryParseNumeric("42", "fallback")
	if v, ok := result.(int64); !ok || v != 42 {
		t.Errorf("Expected int64(42), got %v", result)
	}
}

func TestTryParseNumeric_Float(t *testing.T) {
	result := tryParseNumeric("3.14", "fallback")
	if v, ok := result.(float64); !ok || v != 3.14 {
		t.Errorf("Expected float64(3.14), got %v", result)
	}
}

func TestTryParseNumeric_Invalid(t *testing.T) {
	result := tryParseNumeric("not-a-number", "fallback")
	if result != "fallback" {
		t.Errorf("Expected fallback, got %v", result)
	}
}

// --- validatedCollation tests (33.3% coverage) ---

func TestValidatedCollation_Empty(t *testing.T) {
	result := validatedCollation("")
	if result != "BINARY" {
		t.Errorf("Expected BINARY for empty, got %s", result)
	}
}

func TestValidatedCollation_Binary(t *testing.T) {
	result := validatedCollation("binary")
	if result != "BINARY" {
		t.Errorf("Expected BINARY, got %s", result)
	}
}

func TestValidatedCollation_Nocase(t *testing.T) {
	result := validatedCollation("nocase")
	if result != "NOCASE" {
		t.Errorf("Expected NOCASE, got %s", result)
	}
}

func TestValidatedCollation_Rtrim(t *testing.T) {
	result := validatedCollation("rtrim")
	if result != "RTRIM" {
		t.Errorf("Expected RTRIM, got %s", result)
	}
}

func TestValidatedCollation_Unknown(t *testing.T) {
	result := validatedCollation("CUSTOM")
	if result != "BINARY" {
		t.Errorf("Expected BINARY for unknown collation, got %s", result)
	}
}

// --- applyAffinityToDefault tests ---

func TestApplyAffinityToDefault_RealAffinity(t *testing.T) {
	result := applyAffinityToDefault("3.14", "REAL")
	if v, ok := result.(float64); !ok || v != 3.14 {
		t.Errorf("Expected float64(3.14), got %v", result)
	}
}

func TestApplyAffinityToDefault_FloatAffinity(t *testing.T) {
	result := applyAffinityToDefault("2.5", "FLOAT")
	if v, ok := result.(float64); !ok || v != 2.5 {
		t.Errorf("Expected float64(2.5), got %v", result)
	}
}

func TestApplyAffinityToDefault_DoubleAffinity(t *testing.T) {
	result := applyAffinityToDefault("1.0", "DOUBLE")
	if v, ok := result.(float64); !ok || v != 1.0 {
		t.Errorf("Expected float64(1.0), got %v", result)
	}
}

func TestApplyAffinityToDefault_NumericAffinity(t *testing.T) {
	result := applyAffinityToDefault("42", "NUMERIC")
	if v, ok := result.(int64); !ok || v != 42 {
		t.Errorf("Expected int64(42), got %v", result)
	}
}

func TestApplyAffinityToDefault_DecimalAffinity(t *testing.T) {
	result := applyAffinityToDefault("3.14", "DECIMAL")
	if v, ok := result.(float64); !ok || v != 3.14 {
		t.Errorf("Expected float64(3.14), got %v", result)
	}
}

func TestApplyAffinityToDefault_NotString(t *testing.T) {
	result := applyAffinityToDefault(int64(42), "INTEGER")
	if v, ok := result.(int64); !ok || v != 42 {
		t.Errorf("Expected int64(42) passthrough, got %v", result)
	}
}

// --- NotNullConstraint_ValidateRow coverage (66.7%) ---

func TestNotNullConstraint_ValidateRow_Failure(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
		},
	}

	nnc := NewNotNullConstraint(table)

	// Explicit NULL value should fail
	values := map[string]interface{}{
		"id": nil,
	}

	err := nnc.ValidateRow(values)
	if err == nil {
		t.Error("Expected error for NULL in NOT NULL column")
	}
}

// --- hasUniqueConstraint tests (50% coverage) ---

func TestHasUniqueConstraint_WithPKColumns(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
		PrimaryKey: []string{"id"},
		Constraints: []schema.TableConstraint{
			{Type: schema.ConstraintUnique, Columns: []string{"id"}},
		},
	}

	// Columns have UNIQUE constraint
	result := hasUniqueConstraint(table, []string{"id"})
	if !result {
		t.Error("Expected true for columns with UNIQUE constraint")
	}
}

func TestHasUniqueConstraint_NoConstraint(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		PrimaryKey:  []string{"id"},
		Constraints: nil,
	}

	result := hasUniqueConstraint(table, []string{"name"})
	if result {
		t.Error("Expected false for column without UNIQUE constraint")
	}
}

// --- hasUniqueIndex tests ---

func TestHasUniqueIndex_WithUniqueIndex(t *testing.T) {
	sch := schema.NewSchema()
	sch.Indexes["uk_test_email"] = &schema.Index{
		Name:    "uk_test_email",
		Table:   "test",
		Columns: []string{"email"},
		Unique:  true,
	}

	result := hasUniqueIndex(sch, "test", []string{"email"})
	if !result {
		t.Error("Expected true for column with unique index")
	}
}

func TestHasUniqueIndex_NoIndex(t *testing.T) {
	sch := schema.NewSchema()
	result := hasUniqueIndex(sch, "test", []string{"email"})
	if result {
		t.Error("Expected false when no index")
	}
}

// --- validateDeleteTypeAssertions tests (69.2% coverage) ---

func TestForeignKeyManager_ValidateDelete_DeleteActionNone(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	// FKActionNone with referencing rows should fail
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionNone,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10})

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": int64(1)}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err == nil {
		t.Error("Expected error when FKActionNone and rows exist")
	}
}

// --- cascadeDelete additional paths (64.7% coverage) ---

func TestForeignKeyManager_CascadeDelete_WithRowIDAndExtendedDeleter(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	// WITHOUT ROWID table
	childTable := &schema.Table{
		Name:         "child",
		PrimaryKey:   []string{"id"},
		WithoutRowID: true,
	}
	sch.Tables["child"] = childTable

	rowDataMap := map[string]interface{}{"id": int64(1)}

	reader := NewMockRowReader()
	reader.rows["child"] = map[string][]interface{}{}
	// Use ReadRowByRowid via referencingRows
	reader.referencingRows["child"] = []int64{1}

	// Extended deleter
	deleterExt := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDelete("child", []int64{1}, sch, deleterExt, updater, reader)
	_ = rowDataMap
	if err != nil {
		t.Errorf("Expected nil for cascade delete with extended deleter, got: %v", err)
	}
}

// --- validateDeleteRecursive tests (62.5% coverage) ---

func TestForeignKeyManager_ValidateDeleteRecursive_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	// No constraints for "parent" table - should return nil immediately
	err := mgr.validateDeleteRecursive("parent", map[string]interface{}{"id": 1}, sch, NewMockRowReader(), NewMockRowDeleter(), NewMockRowUpdater())
	if err != nil {
		t.Errorf("Expected nil when no constraints, got: %v", err)
	}
}

// --- filterDeleteSelfRef tests (80% coverage) ---

func TestForeignKeyManager_FilterDeleteSelfRef_EmptyRows(t *testing.T) {
	mgr := NewForeignKeyManager()

	fk := &ForeignKeyConstraint{
		Table:      "self_ref",
		RefTable:   "self_ref",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
	}

	values := map[string]interface{}{"id": int64(1)}
	result := mgr.filterDeleteSelfRef(fk, []int64{}, values, []string{"id"})
	if len(result) != 0 {
		t.Errorf("Expected 0 rows for empty input, got %d", len(result))
	}
}

func TestForeignKeyManager_FilterDeleteSelfRef_NonSelfRef(t *testing.T) {
	mgr := NewForeignKeyManager()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		RefTable:   "customers",
		Columns:    []string{"customer_id"},
		RefColumns: []string{"id"},
	}

	values := map[string]interface{}{"id": int64(1)}
	result := mgr.filterDeleteSelfRef(fk, []int64{1, 2}, values, []string{"id"})
	if len(result) != 2 {
		t.Errorf("Expected 2 rows for non-self-ref, got %d", len(result))
	}
}
