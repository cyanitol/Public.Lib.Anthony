package constraint

import (
	"fmt"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// MockRowReader implements RowReader for testing
type MockRowReader struct {
	rows            map[string]map[string][]interface{} // table -> columns -> values
	referencingRows map[string][]int64                  // key -> rowids
}

func NewMockRowReader() *MockRowReader {
	return &MockRowReader{
		rows:            make(map[string]map[string][]interface{}),
		referencingRows: make(map[string][]int64),
	}
}

func (m *MockRowReader) RowExists(table string, columns []string, values []interface{}) (bool, error) {
	tableRows, ok := m.rows[table]
	if !ok {
		return false, nil
	}

	key := fmt.Sprintf("%v:%v", columns, values)
	if _, exists := tableRows[key]; exists {
		return true, nil
	}

	return false, nil
}

func (m *MockRowReader) FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error) {
	// First try the full key format
	key := fmt.Sprintf("%s:%v:%v", table, columns, values)
	if rowids, ok := m.referencingRows[key]; ok {
		return rowids, nil
	}

	// Also check for simple table-only key (used by AddReferencingRows)
	if rowids, ok := m.referencingRows[table]; ok {
		return rowids, nil
	}

	return []int64{}, nil
}

func (m *MockRowReader) AddRow(table string, columns []string, values []interface{}) {
	if m.rows[table] == nil {
		m.rows[table] = make(map[string][]interface{})
	}
	key := fmt.Sprintf("%v:%v", columns, values)
	m.rows[table][key] = values
}

func (m *MockRowReader) AddReferencingRow(table string, columns []string, values []interface{}, rowid int64) {
	key := fmt.Sprintf("%s:%v:%v", table, columns, values)
	m.referencingRows[key] = append(m.referencingRows[key], rowid)
}

// AddReferencingRows adds multiple referencing rowids for a table (simplified for CASCADE tests)
func (m *MockRowReader) AddReferencingRows(table string, rowids []int64) {
	// Store under a simple table key for CASCADE operations
	key := table
	m.referencingRows[key] = rowids
}

// MockRowDeleter implements RowDeleter for testing
type MockRowDeleter struct {
	deletedRows map[string][]int64 // table -> rowids
}

func NewMockRowDeleter() *MockRowDeleter {
	return &MockRowDeleter{
		deletedRows: make(map[string][]int64),
	}
}

func (m *MockRowDeleter) DeleteRow(table string, rowid int64) error {
	m.deletedRows[table] = append(m.deletedRows[table], rowid)
	return nil
}

// updateRecord holds a single update record for test verification
type updateRecord struct {
	table  string
	rowid  int64
	values map[string]interface{}
}

// MockRowUpdater implements RowUpdater for testing
type MockRowUpdater struct {
	updatedRows map[string]map[int64]map[string]interface{} // table -> rowid -> column values
	updates     []updateRecord                              // ordered list of updates for verification
}

func NewMockRowUpdater() *MockRowUpdater {
	return &MockRowUpdater{
		updatedRows: make(map[string]map[int64]map[string]interface{}),
		updates:     make([]updateRecord, 0),
	}
}

func (m *MockRowUpdater) UpdateRow(table string, rowid int64, values map[string]interface{}) error {
	if m.updatedRows[table] == nil {
		m.updatedRows[table] = make(map[int64]map[string]interface{})
	}
	m.updatedRows[table][rowid] = values
	m.updates = append(m.updates, updateRecord{table: table, rowid: rowid, values: values})
	return nil
}

// TestForeignKeyManager_SetEnabled tests the PRAGMA foreign_keys setting
func TestForeignKeyManager_SetEnabled(t *testing.T) {
	mgr := NewForeignKeyManager()

	if mgr.IsEnabled() {
		t.Error("foreign_keys should be disabled by default")
	}

	mgr.SetEnabled(true)
	if !mgr.IsEnabled() {
		t.Error("foreign_keys should be enabled after SetEnabled(true)")
	}

	mgr.SetEnabled(false)
	if mgr.IsEnabled() {
		t.Error("foreign_keys should be disabled after SetEnabled(false)")
	}
}

// TestForeignKeyManager_AddConstraint tests adding constraints
func TestForeignKeyManager_AddConstraint(t *testing.T) {
	mgr := NewForeignKeyManager()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
		OnUpdate:   FKActionRestrict,
	}

	mgr.AddConstraint(fk)

	constraints := mgr.GetConstraints("orders")
	if len(constraints) != 1 {
		t.Fatalf("expected 1 constraint, got %d", len(constraints))
	}

	if constraints[0].RefTable != "customers" {
		t.Errorf("expected RefTable 'customers', got '%s'", constraints[0].RefTable)
	}
}

// TestForeignKeyManager_ValidateInsert_Success tests successful INSERT validation
func TestForeignKeyManager_ValidateInsert_Success(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	// Mock row reader with existing customer
	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{1})

	// Insert order with valid customer_id
	values := map[string]interface{}{
		"id":          1,
		"customer_id": 1,
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert should succeed: %v", err)
	}
}

// TestForeignKeyManager_ValidateInsert_Failure tests failed INSERT validation
func TestForeignKeyManager_ValidateInsert_Failure(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	// Mock row reader with NO customer id=999
	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{1})

	// Try to insert order with invalid customer_id
	values := map[string]interface{}{
		"id":          1,
		"customer_id": 999, // Does not exist
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err == nil {
		t.Error("ValidateInsert should fail with non-existent customer_id")
	}
}

// TestForeignKeyManager_ValidateInsert_NullAllowed tests that NULL foreign keys are allowed
func TestForeignKeyManager_ValidateInsert_NullAllowed(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()

	// Insert order with NULL customer_id (should be allowed)
	values := map[string]interface{}{
		"id":          1,
		"customer_id": nil,
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert should allow NULL foreign key: %v", err)
	}
}

// TestForeignKeyManager_ValidateDelete_Restrict tests ON DELETE RESTRICT
func TestForeignKeyManager_ValidateDelete_Restrict(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint with RESTRICT
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionRestrict,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing row
	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	// Try to delete customer that has orders
	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err == nil {
		t.Error("ValidateDelete should fail with RESTRICT when referencing rows exist")
	}
}

// TestForeignKeyManager_ValidateDelete_Cascade tests ON DELETE CASCADE
func TestForeignKeyManager_ValidateDelete_Cascade(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint with CASCADE
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing rows
	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 101)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	// Delete customer - should cascade to orders
	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("ValidateDelete should succeed with CASCADE: %v", err)
	}

	// Verify cascade deletes occurred
	if len(deleter.deletedRows["orders"]) != 2 {
		t.Errorf("expected 2 cascade deletes, got %d", len(deleter.deletedRows["orders"]))
	}
}

// TestForeignKeyManager_ValidateDelete_SetNull tests ON DELETE SET NULL
func TestForeignKeyManager_ValidateDelete_SetNull(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint with SET NULL
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionSetNull,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing row
	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	// Delete customer - should set customer_id to NULL in orders
	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("ValidateDelete should succeed with SET NULL: %v", err)
	}

	// Verify SET NULL update occurred
	if len(updater.updatedRows["orders"]) != 1 {
		t.Errorf("expected 1 SET NULL update, got %d", len(updater.updatedRows["orders"]))
	}

	updatedValues := updater.updatedRows["orders"][100]
	if updatedValues["customer_id"] != nil {
		t.Errorf("expected customer_id to be NULL, got %v", updatedValues["customer_id"])
	}
}

// TestForeignKeyManager_ValidateDelete_SetDefault tests ON DELETE SET DEFAULT
func TestForeignKeyManager_ValidateDelete_SetDefault(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema with DEFAULT value
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
			{Name: "customer_id", Type: "INTEGER", Default: 0},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable
	sch.Tables["orders"] = ordersTable

	// Add foreign key constraint with SET DEFAULT
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionSetDefault,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing row
	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	// Delete customer - should set customer_id to DEFAULT in orders
	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("ValidateDelete should succeed with SET DEFAULT: %v", err)
	}

	// Verify SET DEFAULT update occurred
	if len(updater.updatedRows["orders"]) != 1 {
		t.Errorf("expected 1 SET DEFAULT update, got %d", len(updater.updatedRows["orders"]))
	}

	updatedValues := updater.updatedRows["orders"][100]
	if updatedValues["customer_id"] != 0 {
		t.Errorf("expected customer_id to be 0, got %v", updatedValues["customer_id"])
	}
}

// TestForeignKeyManager_ValidateUpdate tests UPDATE validation
func TestForeignKeyManager_ValidateUpdate(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema
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

	// Add foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnUpdate:   FKActionRestrict,
	}
	mgr.AddConstraint(fk)

	// Mock row reader with customer 2
	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{2})

	// Mock row updater
	updater := NewMockRowUpdater()

	// Update order to reference customer 2 (valid)
	oldValues := map[string]interface{}{
		"id":          1,
		"customer_id": 1,
	}
	newValues := map[string]interface{}{
		"id":          1,
		"customer_id": 2,
	}

	err := mgr.ValidateUpdate("orders", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed with valid reference: %v", err)
	}

	// Update to non-existent customer (invalid)
	newValues["customer_id"] = 999

	err = mgr.ValidateUpdate("orders", oldValues, newValues, sch, reader, updater)
	if err == nil {
		t.Error("ValidateUpdate should fail with non-existent reference")
	}
}

// TestForeignKeyManager_Disabled tests that validation is skipped when disabled
func TestForeignKeyManager_Disabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(false) // Disabled

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	// Try to insert with invalid FK (should succeed because FKs disabled)
	values := map[string]interface{}{
		"id":          1,
		"customer_id": 999,
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert should skip checks when disabled: %v", err)
	}
}

// TestCreateForeignKeyFromParser tests converting parser AST to constraint
func TestCreateForeignKeyFromParser(t *testing.T) {
	parserFK := &parser.ForeignKeyConstraint{
		Table:      "customers",
		Columns:    []string{"id"},
		OnDelete:   parser.FKActionCascade,
		OnUpdate:   parser.FKActionRestrict,
		Deferrable: parser.DeferrableInitiallyDeferred,
	}

	fk := CreateForeignKeyFromParser("orders", []string{"customer_id"}, parserFK, "fk_orders_customers")

	if fk.Table != "orders" {
		t.Errorf("expected Table 'orders', got '%s'", fk.Table)
	}

	if fk.RefTable != "customers" {
		t.Errorf("expected RefTable 'customers', got '%s'", fk.RefTable)
	}

	if fk.OnDelete != FKActionCascade {
		t.Errorf("expected OnDelete CASCADE, got %v", fk.OnDelete)
	}

	if fk.OnUpdate != FKActionRestrict {
		t.Errorf("expected OnUpdate RESTRICT, got %v", fk.OnUpdate)
	}

	if fk.Deferrable != DeferrableInitiallyDeferred {
		t.Errorf("expected Deferrable INITIALLY DEFERRED, got %v", fk.Deferrable)
	}

	if fk.Name != "fk_orders_customers" {
		t.Errorf("expected Name 'fk_orders_customers', got '%s'", fk.Name)
	}
}

// TestForeignKeyManager_DeferredConstraints tests deferred constraint checking
func TestForeignKeyManager_DeferredConstraints(t *testing.T) {
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

	// Add foreign key constraint with DEFERRABLE INITIALLY DEFERRED
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnDelete:   FKActionRestrict,
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()

	// Insert with invalid FK should succeed (deferred checking)
	values := map[string]interface{}{
		"id":          1,
		"customer_id": 999,
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("Deferred constraint should skip immediate validation: %v", err)
	}
}

// TestForeignKeyManager_MultiColumnFK tests composite foreign keys
func TestForeignKeyManager_MultiColumnFK(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	// Create schema with composite key
	sch := schema.NewSchema()
	productTable := &schema.Table{
		Name: "products",
		Columns: []*schema.Column{
			{Name: "category", Type: "TEXT"},
			{Name: "sku", Type: "TEXT"},
		},
		PrimaryKey: []string{"category", "sku"},
	}
	orderItemsTable := &schema.Table{
		Name: "order_items",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "product_category", Type: "TEXT"},
			{Name: "product_sku", Type: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["products"] = productTable
	sch.Tables["order_items"] = orderItemsTable

	// Add composite foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:      "order_items",
		Columns:    []string{"product_category", "product_sku"},
		RefTable:   "products",
		RefColumns: []string{"category", "sku"},
		OnDelete:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	// Mock reader with product
	reader := NewMockRowReader()
	reader.AddRow("products", []string{"category", "sku"}, []interface{}{"electronics", "ABC123"})

	// Insert order_item with valid composite FK
	values := map[string]interface{}{
		"id":               1,
		"product_category": "electronics",
		"product_sku":      "ABC123",
	}

	err := mgr.ValidateInsert("order_items", values, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert should succeed with valid composite FK: %v", err)
	}

	// Try invalid composite FK
	values["product_sku"] = "INVALID"

	err = mgr.ValidateInsert("order_items", values, sch, reader)
	if err == nil {
		t.Error("ValidateInsert should fail with invalid composite FK")
	}
}

// TestForeignKeyManager_OnUpdateCascade tests ON UPDATE CASCADE action
func TestForeignKeyManager_OnUpdateCascade(t *testing.T) {
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

	// Add foreign key with CASCADE UPDATE
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnUpdate:   FKActionCascade,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing rows
	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10, 20}) // Two orders reference customer 1

	// Mock updater
	updater := NewMockRowUpdater()

	// Update customer ID from 1 to 100
	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 100}

	err := mgr.ValidateUpdate("customers", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed with CASCADE: %v", err)
	}

	// Verify CASCADE updated referencing rows
	if len(updater.updates) != 2 {
		t.Errorf("Expected 2 CASCADE updates, got %d", len(updater.updates))
	}

	for _, update := range updater.updates {
		if update.table != "orders" {
			t.Errorf("Expected CASCADE on 'orders' table, got '%s'", update.table)
		}
		if update.values["customer_id"] != 100 {
			t.Errorf("Expected CASCADE to set customer_id=100, got %v", update.values["customer_id"])
		}
	}
}

// TestForeignKeyManager_OnUpdateSetNull tests ON UPDATE SET NULL action
func TestForeignKeyManager_OnUpdateSetNull(t *testing.T) {
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

	// Add foreign key with SET NULL UPDATE
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnUpdate:   FKActionSetNull,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing rows
	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10, 20})

	// Mock updater
	updater := NewMockRowUpdater()

	// Update customer ID from 1 to 100
	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 100}

	err := mgr.ValidateUpdate("customers", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed with SET NULL: %v", err)
	}

	// Verify SET NULL updated referencing rows
	if len(updater.updates) != 2 {
		t.Errorf("Expected 2 SET NULL updates, got %d", len(updater.updates))
	}

	for _, update := range updater.updates {
		if update.table != "orders" {
			t.Errorf("Expected SET NULL on 'orders' table, got '%s'", update.table)
		}
		if update.values["customer_id"] != nil {
			t.Errorf("Expected SET NULL to set customer_id=NULL, got %v", update.values["customer_id"])
		}
	}
}

// TestForeignKeyManager_OnUpdateSetDefault tests ON UPDATE SET DEFAULT action
func TestForeignKeyManager_OnUpdateSetDefault(t *testing.T) {
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
			{Name: "customer_id", Type: "INTEGER", Default: 0},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable
	sch.Tables["orders"] = ordersTable

	// Add foreign key with SET DEFAULT UPDATE
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnUpdate:   FKActionSetDefault,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing rows
	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10, 20})

	// Mock updater
	updater := NewMockRowUpdater()

	// Update customer ID from 1 to 100
	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 100}

	err := mgr.ValidateUpdate("customers", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed with SET DEFAULT: %v", err)
	}

	// Verify SET DEFAULT updated referencing rows
	if len(updater.updates) != 2 {
		t.Errorf("Expected 2 SET DEFAULT updates, got %d", len(updater.updates))
	}

	for _, update := range updater.updates {
		if update.table != "orders" {
			t.Errorf("Expected SET DEFAULT on 'orders' table, got '%s'", update.table)
		}
		if update.values["customer_id"] != 0 {
			t.Errorf("Expected SET DEFAULT to set customer_id=0, got %v", update.values["customer_id"])
		}
	}
}

// TestForeignKeyManager_OnUpdateRestrict tests ON UPDATE RESTRICT action
func TestForeignKeyManager_OnUpdateRestrict(t *testing.T) {
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

	// Add foreign key with RESTRICT UPDATE
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		OnUpdate:   FKActionRestrict,
	}
	mgr.AddConstraint(fk)

	// Mock reader with referencing rows
	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{10, 20})

	// Mock updater
	updater := NewMockRowUpdater()

	// Update customer ID from 1 to 100
	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 100}

	err := mgr.ValidateUpdate("customers", oldValues, newValues, sch, reader, updater)
	if err == nil {
		t.Error("ValidateUpdate should fail with RESTRICT when referencing rows exist")
	}
}
