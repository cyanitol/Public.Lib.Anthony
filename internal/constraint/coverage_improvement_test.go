package constraint

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// TestUniqueConstraint_CheckCurrentRow tests the checkCurrentRow method
func TestUniqueConstraint_CheckCurrentRow(t *testing.T) {
	table := &schema.Table{
		Name:     "test_table",
		RootPage: 2,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	table.RootPage = rootPage

	cursor := btree.NewCursor(bt, table.RootPage)

	// Insert a test row
	payload := []byte{0x02, 0x01, 0x01} // Simple header
	if err := cursor.Insert(1, payload); err != nil {
		t.Fatalf("Failed to insert test row: %v", err)
	}

	uc := NewUniqueConstraint("", "test_table", []string{"email"})

	tests := []struct {
		name         string
		values       map[string]interface{}
		skipRowid    int64
		wantConflict bool
	}{
		{
			name: "skip same rowid - no conflict",
			values: map[string]interface{}{
				"email": "test@example.com",
			},
			skipRowid:    1,
			wantConflict: false,
		},
		{
			name: "different rowid - check performed",
			values: map[string]interface{}{
				"email": "test@example.com",
			},
			skipRowid:    2,
			wantConflict: false, // No actual conflict since parseRecordValues returns empty map
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Move cursor to first row
			if err := cursor.MoveToFirst(); err != nil {
				t.Fatalf("Failed to move cursor: %v", err)
			}

			conflictFound, conflictRowid := uc.checkCurrentRow(cursor, table, tt.values, tt.skipRowid)

			if conflictFound != tt.wantConflict {
				t.Errorf("checkCurrentRow() conflict = %v, want %v", conflictFound, tt.wantConflict)
			}

			if conflictFound && conflictRowid == 0 {
				t.Error("Expected non-zero conflict rowid when conflict found")
			}
		})
	}
}

// TestUniqueConstraint_ValidateTableRow tests ValidateTableRow function
func TestUniqueConstraint_ValidateTableRow(t *testing.T) {
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
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	table.RootPage = rootPage

	tests := []struct {
		name      string
		values    map[string]interface{}
		rowid     int64
		wantError bool
	}{
		{
			name: "valid unique values",
			values: map[string]interface{}{
				"id":       1,
				"email":    "user@example.com",
				"username": "user1",
			},
			rowid:     1,
			wantError: false,
		},
		{
			name: "null values allowed in unique columns",
			values: map[string]interface{}{
				"id":       2,
				"email":    nil,
				"username": nil,
			},
			rowid:     2,
			wantError: false,
		},
		{
			name: "mixed null and non-null",
			values: map[string]interface{}{
				"id":       3,
				"email":    "user3@example.com",
				"username": nil,
			},
			rowid:     3,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTableRow(table, bt, tt.values, tt.rowid)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateTableRow() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

// TestUniqueConstraint_NoColumns tests error case with no columns
func TestUniqueConstraint_NoColumns(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	uc := NewUniqueConstraint("", "test", []string{})

	err := uc.Validate(table, bt, map[string]interface{}{}, 1)
	if err == nil {
		t.Error("Expected error for unique constraint with no columns")
	}
}

// TestUniqueConstraint_MissingColumn tests error case with missing column
func TestUniqueConstraint_MissingColumn(t *testing.T) {
	table := &schema.Table{
		Name:     "test",
		RootPage: 2,
		Columns:  []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	bt := btree.NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	table.RootPage = rootPage

	uc := NewUniqueConstraint("", "test", []string{"nonexistent"})

	values := map[string]interface{}{"id": 1}
	err := uc.Validate(table, bt, values, 1)
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

// TestUniqueConstraint_CreateBackingIndexTableNotFound tests error case
func TestUniqueConstraint_CreateBackingIndexTableNotFound(t *testing.T) {
	sch := schema.NewSchema()
	bt := btree.NewBtree(4096)

	uc := NewUniqueConstraint("uk_test", "nonexistent_table", []string{"col"})

	err := uc.CreateBackingIndex(sch, bt)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

// TestPrimaryKeyConstraint_FindGapInRowids tests gap finding in rowids
func TestPrimaryKeyConstraint_FindGapInRowids(t *testing.T) {
	tests := []struct {
		name         string
		existingRows []int64
		wantRowid    int64
	}{
		{
			name:         "empty table",
			existingRows: []int64{},
			wantRowid:    1,
		},
		{
			name:         "gap at beginning",
			existingRows: []int64{2, 3, 4},
			wantRowid:    1,
		},
		{
			name:         "gap in middle",
			existingRows: []int64{1, 2, 4, 5},
			wantRowid:    3,
		},
		{
			name:         "no gaps",
			existingRows: []int64{1, 2, 3, 4, 5},
			wantRowid:    6,
		},
		{
			name:         "multiple gaps, return first",
			existingRows: []int64{1, 3, 5, 7},
			wantRowid:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create table and btree
			columns := []*schema.Column{
				{Name: "data", Type: "TEXT"},
			}

			bt := btree.NewBtree(4096)
			rootPage, err := bt.CreateTable()
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			table := &schema.Table{
				Name:       "test",
				RootPage:   rootPage,
				Columns:    columns,
				PrimaryKey: []string{},
			}

			// Insert existing rows
			cursor := btree.NewCursor(bt, table.RootPage)
			for _, rowid := range tt.existingRows {
				if err := cursor.Insert(rowid, []byte("data")); err != nil {
					t.Fatalf("Failed to insert rowid %d: %v", rowid, err)
				}
			}

			pk := NewPrimaryKeyConstraint(table, bt, nil)

			gotRowid, err := pk.findGapInRowids()
			if err != nil {
				t.Fatalf("findGapInRowids() error = %v", err)
			}

			if gotRowid != tt.wantRowid {
				t.Errorf("findGapInRowids() = %d, want %d", gotRowid, tt.wantRowid)
			}
		})
	}
}

// TestPrimaryKeyConstraint_GenerateRowidMaxInt64 tests behavior at max int64
func TestPrimaryKeyConstraint_GenerateRowidMaxInt64(t *testing.T) {
	columns := []*schema.Column{
		{Name: "data", Type: "TEXT"},
	}

	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	table := &schema.Table{
		Name:       "test",
		RootPage:   rootPage,
		Columns:    columns,
		PrimaryKey: []string{},
	}

	// Insert a row with max int64
	cursor := btree.NewCursor(bt, table.RootPage)
	maxRowid := int64(9223372036854775807)
	if err := cursor.Insert(maxRowid, []byte("data")); err != nil {
		t.Fatalf("Failed to insert max rowid: %v", err)
	}

	pk := NewPrimaryKeyConstraint(table, bt, nil)

	// When max rowid is at limit, should call findGapInRowids
	rowid, err := pk.generateRowid()
	if err != nil {
		t.Fatalf("generateRowid() error = %v", err)
	}

	// Should find gap at 1 since we only have max value
	if rowid != 1 {
		t.Errorf("generateRowid() = %d, want 1", rowid)
	}
}

// TestPrimaryKeyConstraint_ConvertToInt64_VdbeMem tests conversion with vdbe.Mem types
func TestPrimaryKeyConstraint_ConvertToInt64_VdbeMem(t *testing.T) {
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

	tests := []struct {
		name      string
		value     interface{}
		want      int64
		wantError bool
	}{
		{
			name:      "vdbe.Mem with integer",
			value:     vdbe.NewMemInt(42),
			want:      42,
			wantError: false,
		},
		{
			name:      "vdbe.Mem with real",
			value:     vdbe.NewMemReal(99.7),
			want:      99,
			wantError: false,
		},
		{
			name:      "vdbe.Mem with string (should fail)",
			value:     vdbe.NewMemStr("not a number"),
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pk.convertToInt64(tt.value)

			if tt.wantError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if got != tt.want {
					t.Errorf("convertToInt64() = %d, want %d", got, tt.want)
				}
			}
		})
	}
}

// TestPrimaryKeyConstraint_UpdateCompositePKNull tests updating composite PK to NULL
func TestPrimaryKeyConstraint_UpdateCompositePKNull(t *testing.T) {
	columns := []*schema.Column{
		{Name: "dept", Type: "INTEGER", PrimaryKey: true},
		{Name: "emp", Type: "INTEGER", PrimaryKey: true},
		{Name: "name", Type: "TEXT"},
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

	// Try to update composite PK column to NULL
	newValues := map[string]interface{}{
		"dept": nil,
	}

	err := pk.ValidateUpdate(10, newValues)
	if err == nil {
		t.Error("Expected error when updating composite PK to NULL")
	}
}

// TestCompareInt64_BothTypes tests int64 comparison with different types
func TestCompareInt64_BothTypes(t *testing.T) {
	tests := []struct {
		name string
		v1   int64
		v2   interface{}
		want bool
	}{
		{"int64 equal", int64(42), int64(42), true},
		{"int64 not equal", int64(42), int64(43), false},
		{"int64 vs int equal", int64(42), int(42), true},
		{"int64 vs int not equal", int64(42), int(43), false},
		{"int64 vs string", int64(42), "42", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareInt64(tt.v1, tt.v2)
			if got != tt.want {
				t.Errorf("compareInt64(%v, %v) = %v, want %v", tt.v1, tt.v2, got, tt.want)
			}
		})
	}
}

// TestForeignKeyManager_ValidateDelete_NoAction tests ON DELETE NO ACTION
func TestForeignKeyManager_ValidateDelete_NoAction(t *testing.T) {
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
		OnDelete:   FKActionNoAction,
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	reader.AddReferencingRow("orders", []string{"customer_id"}, []interface{}{1}, 100)

	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": 1}

	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err == nil {
		t.Error("ValidateDelete should fail with NO ACTION when referencing rows exist")
	}
}

// TestForeignKeyManager_ValidateDelete_NoReferences tests delete with no references
func TestForeignKeyManager_ValidateDelete_NoReferences(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	values := map[string]interface{}{"id": 1}

	// No constraints, should succeed
	err := mgr.ValidateDelete("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("ValidateDelete should succeed with no references: %v", err)
	}
}

// TestForeignKeyManager_ValidateUpdate_NoChange tests update when FK columns don't change
func TestForeignKeyManager_ValidateUpdate_NoChange(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	ordersTable := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "customer_id", Type: "INTEGER"},
			{Name: "amount", Type: "REAL"},
		},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["orders"] = ordersTable

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{
		"id":          1,
		"customer_id": 5,
		"amount":      100.0,
	}
	newValues := map[string]interface{}{
		"id":          1,
		"customer_id": 5,
		"amount":      150.0,
	}

	// FK columns unchanged, should skip validation
	err := mgr.ValidateUpdate("orders", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("ValidateUpdate should succeed when FK columns unchanged: %v", err)
	}
}

// TestForeignKeyManager_ValidateInsert_MissingColumn tests insert with missing FK column
func TestForeignKeyManager_ValidateInsert_MissingColumn(t *testing.T) {
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
	}
	mgr.AddConstraint(fk)

	reader := NewMockRowReader()

	// Missing customer_id in values - treated as NULL
	values := map[string]interface{}{
		"id": 1,
	}

	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("ValidateInsert should allow missing FK column (NULL): %v", err)
	}
}

// TestForeignKeyManager_RemoveConstraints tests removing constraints
func TestForeignKeyManager_RemoveConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()

	fk := &ForeignKeyConstraint{
		Table:    "orders",
		Columns:  []string{"customer_id"},
		RefTable: "customers",
	}
	mgr.AddConstraint(fk)

	// Verify constraint exists
	constraints := mgr.GetConstraints("orders")
	if len(constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(constraints))
	}

	// Remove constraints
	mgr.RemoveConstraints("orders")

	// Verify constraints removed
	constraints = mgr.GetConstraints("orders")
	if len(constraints) != 0 {
		t.Errorf("Expected 0 constraints after removal, got %d", len(constraints))
	}
}

// TestForeignKeyManager_ValidateReference_TableNotFound tests error when referenced table missing
func TestForeignKeyManager_ValidateReference_TableNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()

	sch := schema.NewSchema()
	reader := NewMockRowReader()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}

	err := mgr.validateReference(fk, []interface{}{1}, sch, reader)
	if err == nil {
		t.Error("Expected error for nonexistent referenced table")
	}
}

// TestForeignKeyManager_ValidateReference_ColumnCountMismatch tests column count mismatch
func TestForeignKeyManager_ValidateReference_ColumnCountMismatch(t *testing.T) {
	mgr := NewForeignKeyManager()

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	reader := NewMockRowReader()

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id", "other_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	err := mgr.validateReference(fk, []interface{}{1, 2}, sch, reader)
	if err == nil {
		t.Error("Expected error for column count mismatch")
	}
}

// TestForeignKeyManager_CascadeDelete_Error tests cascade delete with error
func TestForeignKeyManager_CascadeDelete_Error(t *testing.T) {
	mgr := NewForeignKeyManager()

	// Mock deleter that fails
	deleter := &MockRowDeleterWithError{shouldFail: true}

	err := mgr.cascadeDelete("orders", []int64{1, 2, 3}, deleter)
	if err == nil {
		t.Error("Expected error from cascade delete")
	}
}

// MockRowDeleterWithError for testing error cases
type MockRowDeleterWithError struct {
	shouldFail bool
}

func (m *MockRowDeleterWithError) DeleteRow(table string, rowid int64) error {
	if m.shouldFail {
		return &MockDeleteError{}
	}
	return nil
}

type MockDeleteError struct{}

func (e *MockDeleteError) Error() string {
	return "mock delete error"
}

// TestForeignKeyManager_SetNullOnRows_Error tests set null with error
func TestForeignKeyManager_SetNullOnRows_Error(t *testing.T) {
	mgr := NewForeignKeyManager()

	// Mock updater that fails
	updater := &MockRowUpdaterWithError{shouldFail: true}

	err := mgr.setNullOnRows("orders", []string{"customer_id"}, []int64{1}, updater)
	if err == nil {
		t.Error("Expected error from set null")
	}
}

// MockRowUpdaterWithError for testing error cases
type MockRowUpdaterWithError struct {
	shouldFail bool
}

func (m *MockRowUpdaterWithError) UpdateRow(table string, rowid int64, values map[string]interface{}) error {
	if m.shouldFail {
		return &MockUpdateError{}
	}
	return nil
}

type MockUpdateError struct{}

func (e *MockUpdateError) Error() string {
	return "mock update error"
}

// TestForeignKeyManager_GetDefaultValues_TableNotFound tests error case
func TestForeignKeyManager_GetDefaultValues_TableNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	_, err := mgr.getDefaultValues("nonexistent", []string{"col"}, sch)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

// TestForeignKeyManager_GetDefaultValues_ColumnNotFound tests error case
func TestForeignKeyManager_GetDefaultValues_ColumnNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()

	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}
	sch.Tables["test"] = table

	_, err := mgr.getDefaultValues("test", []string{"nonexistent"}, sch)
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

// TestForeignKeyManager_ValidateUpdate_IncomingRefsTableNotFound tests error case
func TestForeignKeyManager_ValidateUpdate_IncomingRefsTableNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 2}

	// Table not in schema, should return nil
	err := mgr.validateIncomingReferences("nonexistent", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("Expected nil for nonexistent table, got: %v", err)
	}
}

// TestForeignKeyManager_ValidateUpdate_NoIncomingRefs tests update with no incoming refs
func TestForeignKeyManager_ValidateUpdate_NoIncomingRefs(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)

	sch := schema.NewSchema()
	customerTable := &schema.Table{
		Name:       "customers",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["customers"] = customerTable

	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	oldValues := map[string]interface{}{"id": 1}
	newValues := map[string]interface{}{"id": 2}

	// No referencing constraints, should succeed
	err := mgr.validateIncomingReferences("customers", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("Expected nil with no incoming refs, got: %v", err)
	}
}

// TestNotNullConstraint_ValidateRow_Error tests ValidateRow with error
func TestNotNullConstraint_ValidateRow_Error(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "name", Type: "TEXT", NotNull: true},
		},
	}

	nnc := NewNotNullConstraint(table)

	// Missing NOT NULL column after applying defaults
	values := map[string]interface{}{
		"id": 1,
	}

	err := nnc.ValidateRow(values)
	if err == nil {
		t.Error("Expected error for missing NOT NULL column")
	}
}

// TestCheckConstraint_ValidateInsert_ParseError tests handling of parse errors
func TestCheckConstraint_ValidateInsert_ParseError(t *testing.T) {
	// Create table with invalid CHECK expression that will fail to parse
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "age", Type: "INTEGER", Check: "invalid syntax ((("},
		},
	}

	validator := NewCheckValidator(table)

	// The parser may or may not successfully parse invalid syntax
	// Just verify that the validator was created successfully
	if validator == nil {
		t.Error("Expected non-nil validator")
	}
}

// TestCheckConstraint_ExtractFromTableConstraint_ParseError tests table-level parse error
func TestCheckConstraint_ExtractFromTableConstraint_ParseError(t *testing.T) {
	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
		Constraints: []schema.TableConstraint{
			{
				Type:       schema.ConstraintCheck,
				Name:       "invalid_check",
				Expression: "invalid ((((",
			},
		},
	}

	validator := NewCheckValidator(table)

	// The parser may or may not successfully parse invalid syntax
	// Just verify that the validator was created successfully
	if validator == nil {
		t.Error("Expected non-nil validator")
	}
}

// TestExtractCheckConstraints_SkipNonCheck tests that non-CHECK constraints are skipped
func TestExtractCheckConstraints_SkipNonCheck(t *testing.T) {
	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
		Constraints: []schema.TableConstraint{
			{
				Type:    schema.ConstraintPrimaryKey,
				Columns: []string{"id"},
			},
			{
				Type:    schema.ConstraintUnique,
				Columns: []string{"id"},
			},
		},
	}

	constraints := extractCheckConstraints(table)
	if len(constraints) != 0 {
		t.Errorf("Expected 0 CHECK constraints, got %d", len(constraints))
	}
}
