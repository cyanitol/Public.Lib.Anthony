// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestNotNullConstraint_ValidateRow_WithApplyDefaultsSuccess tests successful default application
func TestNotNullConstraint_ValidateRow_WithApplyDefaultsSuccess(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "status", Type: "TEXT", NotNull: true, Default: "active"},
		},
	}

	nnc := NewNotNullConstraint(table)

	values := map[string]interface{}{
		"id": 1,
		// status missing but has default
	}

	err := nnc.ValidateRow(values)
	if err != nil {
		t.Errorf("ValidateRow should succeed with default: %v", err)
	}

	// Verify default was applied
	if values["status"] != "active" {
		t.Errorf("Expected default 'active', got %v", values["status"])
	}
}

// TestPrimaryKeyConstraint_HandleIntegerPrimaryKey_ConvertError tests conversion error
func TestPrimaryKeyConstraint_HandleIntegerPrimaryKey_ConvertError(t *testing.T) {
	columns := []*schema.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{"id"},
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// Provide invalid type for INTEGER PRIMARY KEY
	values := map[string]interface{}{
		"id": "not_a_number",
	}

	_, err := pk.handleIntegerPrimaryKey(values, false, 0)
	if err == nil {
		t.Error("Expected error for invalid INTEGER type")
	}
}

// TestPrimaryKeyConstraint_HandleCompositePrimaryKey_MissingColumn tests missing column
func TestPrimaryKeyConstraint_HandleCompositePrimaryKey_MissingColumn(t *testing.T) {
	columns := []*schema.Column{
		{Name: "dept", Type: "INTEGER", PrimaryKey: true},
		{Name: "emp", Type: "INTEGER", PrimaryKey: true},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{"dept", "emp"},
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// Missing emp column
	values := map[string]interface{}{
		"dept": int64(1),
	}

	_, err := pk.handleCompositePrimaryKey(values, false, 0)
	if err == nil {
		t.Error("Expected error for missing PRIMARY KEY column")
	}
}

// TestCheckConstraint_ExtractCheckConstraints_ColumnCheck tests column-level CHECK
func TestCheckConstraint_ExtractCheckConstraints_ColumnCheck(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{
				Name:  "age",
				Type:  "INTEGER",
				Check: "age >= 0",
			},
		},
	}

	constraints, err := extractCheckConstraints(table)
	if err != nil {
		t.Fatalf("extractCheckConstraints failed: %v", err)
	}
	if len(constraints) == 0 {
		t.Error("Expected at least one CHECK constraint from column")
	}
}

// TestPrimaryKeyConstraint_CheckRowidUniqueness_SeekError tests seek operation
func TestPrimaryKeyConstraint_CheckRowidUniqueness_SeekError(t *testing.T) {
	columns := []*schema.Column{{Name: "data", Type: "TEXT"}}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{},
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// Insert a row then check for different rowid (should be unique)
	cursor := btree.NewCursor(bt, table.RootPage)
	cursor.Insert(100, []byte("data"))

	// Check for different rowid - should succeed
	err := pk.checkRowidUniqueness(200)
	if err != nil {
		t.Errorf("Expected nil for unique rowid, got: %v", err)
	}
}

// TestForeignKeyManager_ValidateDelete_MissingRefColumns tests missing ref columns
func TestForeignKeyManager_ValidateDelete_MissingRefColumns(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

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
		RefColumns: []string{}, // Empty - should use PK
		OnDelete:   FKActionRestrict,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err == nil {
		t.Error("Expected error with RESTRICT when referencing rows exist")
	}
}

// TestForeignKeyManager_HandleUpdateConstraint_MissingRefColumns tests missing ref columns
func TestForeignKeyManager_HandleUpdateConstraint_MissingRefColumns(t *testing.T) {
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
		RefColumns: []string{}, // Empty - should use PK
		OnUpdate:   FKActionRestrict,
	}

	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10})

	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 2}

	err := mgr.handleUpdateConstraint(fk, customerTable, oldValues, newValues, sch, reader, updater)
	if err == nil {
		t.Error("Expected error with RESTRICT when referencing rows exist")
	}
}

// TestUniqueConstraint_Validate_AllNull tests all NULL values (should be allowed)
func TestUniqueConstraint_Validate_AllNull(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "email", Type: "TEXT"},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	uc := NewUniqueConstraint("", "test", []string{"email"})

	// All NULL values should be allowed (NULL != NULL in SQL)
	values := map[string]interface{}{
		"id":    1,
		"email": nil,
	}

	err := uc.Validate(table, bt, values, 1)
	if err != nil {
		t.Errorf("Expected nil for NULL unique value, got: %v", err)
	}
}

// TestUniqueConstraint_CheckDuplicateViaIndex_MovementError tests cursor movement
func TestUniqueConstraint_CheckDuplicateViaIndex_MovementError(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "email", Type: "TEXT"}},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	// Insert some rows
	cursor := btree.NewCursor(bt, table.RootPage)
	cursor.Insert(1, []byte{0x02, 0x01, 0x01})
	cursor.Insert(2, []byte{0x02, 0x01, 0x02})

	uc := NewUniqueConstraint("", "test", []string{"email"})

	values := map[string]interface{}{"email": "test@example.com"}

	// Should scan all rows without error
	_, _, err := uc.checkDuplicateViaIndex(bt, table, values, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestPrimaryKeyConstraint_ValidateIntegerPKUpdate_DifferentValue tests updating to different value
func TestPrimaryKeyConstraint_ValidateIntegerPKUpdate_DifferentValue(t *testing.T) {
	columns := []*schema.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
		{Name: "name", Type: "TEXT"},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{"id"},
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// Insert initial row with id=10
	cursor := btree.NewCursor(bt, table.RootPage)
	cursor.Insert(10, []byte("data"))

	// Update to different id
	newValues := map[string]interface{}{
		"id": int64(20),
	}

	err := pk.validateIntegerPKUpdate(10, newValues)
	if err != nil {
		t.Errorf("Expected nil for valid unique update, got: %v", err)
	}
}

// TestForeignKeyManager_SetDefaultOnRows_GetDefaultsError tests error in getDefaultValues
func TestForeignKeyManager_SetDefaultOnRows_GetDefaultsError(t *testing.T) {
	mgr := NewForeignKeyManager()

	// Empty schema - no tables
	sch := schema.NewSchema()
	updater := NewMockRowUpdater()

	err := mgr.setDefaultOnRows("nonexistent", []string{"col"}, []int64{1}, sch, updater)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

// TestForeignKeyManager_ValidateReference_EmptyRefColumns tests empty ref columns
func TestForeignKeyManager_ValidateReference_EmptyRefColumns(t *testing.T) {
	mgr := NewForeignKeyManager()

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{1})

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{}, // Empty - should use PK
	}

	err := mgr.validateReference(fk, []interface{}{1}, sch, reader)
	if err != nil {
		t.Errorf("Expected nil when using default PK, got: %v", err)
	}
}

// TestUniqueConstraint_CreateBackingIndex_IndexCreation tests successful index creation
func TestUniqueConstraint_CreateBackingIndex_IndexCreation(t *testing.T) {
	sch := schema.NewSchema()
	bt := btree.NewBtree(4096)

	table := &schema.Table{
		Name:     "users",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "email", Type: "TEXT", Unique: true}},
	}
	tableRootPage, _ := bt.CreateTable()
	table.RootPage = tableRootPage

	sch.Tables["users"] = table

	uc := NewUniqueConstraint("uk_users_email", "users", []string{"email"})

	err := uc.CreateBackingIndex(sch, bt)
	if err != nil {
		t.Errorf("Unexpected error creating index: %v", err)
	}

	// Verify index was created
	index, exists := sch.GetIndex(uc.IndexName)
	if !exists {
		t.Error("Expected index to exist")
	}
	if index.Name != uc.IndexName {
		t.Errorf("Expected index name %s, got %s", uc.IndexName, index.Name)
	}
}

// TestCheckConstraint_ExtractCheckConstraints_EmptyColumnCheck tests empty column check
func TestCheckConstraint_ExtractCheckConstraints_EmptyColumnCheck(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{
				Name:  "age",
				Type:  "INTEGER",
				Check: "", // Empty check
			},
		},
	}

	constraints, err := extractCheckConstraints(table)
	if err != nil {
		t.Fatalf("extractCheckConstraints failed: %v", err)
	}
	// Empty check should be skipped
	if len(constraints) != 0 {
		t.Errorf("Expected 0 constraints for empty check, got %d", len(constraints))
	}
}

// TestUniqueConstraint_ValidateTableRow_MultipleConstraints tests multiple unique constraints
func TestUniqueConstraint_ValidateTableRow_MultipleConstraints(t *testing.T) {
	table := &schema.Table{
		Name:     "users",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT", Unique: true},
			{Name: "username", Type: "TEXT", Unique: true},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	values := map[string]interface{}{
		"id":       1,
		"email":    "user@example.com",
		"username": "user1",
	}

	err := ValidateTableRow(table, bt, values, 1)
	if err != nil {
		t.Errorf("Expected nil for valid unique values, got: %v", err)
	}
}

// TestForeignKeyManager_ValidateUpdate_TableNotInSchema tests missing table
func TestForeignKeyManager_ValidateUpdate_TableNotInSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 2}

	// Table not in schema, should return nil (no constraints to check)
	err := mgr.ValidateUpdate("nonexistent", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("Expected nil for nonexistent table, got: %v", err)
	}
}

// TestForeignKeyManager_ValidateInsert_DeferredCheck tests deferred constraint
func TestForeignKeyManager_ValidateInsert_DeferredCheck(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	ordersTable := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "customer_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable
	sch.Tables["orders"] = ordersTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()

	// Must be in a transaction for deferred constraints to work
	mgr.SetInTransaction(true)

	// Insert with invalid reference but deferred
	values := map[string]interface{}{
		"id":          1,
		"customer_id": 999, // Doesn't exist but deferred
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("Deferred constraint should skip validation: %v", err)
	}

	// Clean up
	mgr.SetInTransaction(false)
}
