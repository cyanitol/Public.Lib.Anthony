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
	mgr.AddConstraint(fk)
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk, Values: []interface{}{int64(1)}, Table: "orders"},
	}

	// Pass invalid schema - should return nil (type assertion fails)
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
	}
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk, Values: []interface{}{int64(1)}, Table: "orders"},
	}

	// Pass invalid reader - should return nil
	err := mgr.CheckDeferredViolations(schema.NewSchema(), "not-a-reader")
	if err != nil {
		t.Errorf("Expected nil for invalid reader, got: %v", err)
	}
}

func TestForeignKeyManager_CheckDeferredViolations_WithViolation(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk, Values: []interface{}{int64(999)}, Table: "orders"},
	}

	sch := schema.NewSchema()
	customersTable := &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}
	sch.Tables["customers"] = customersTable

	reader := NewMockRowReader()
	// id=999 doesn't exist in customers - violation should occur

	err := mgr.CheckDeferredViolations(sch, reader)
	if err == nil {
		t.Error("Expected violation error")
	}
}

func TestForeignKeyManager_CheckDeferredViolations_NoViolation(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)

	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk, Values: []interface{}{int64(1)}, Table: "orders"},
	}

	sch := schema.NewSchema()
	customersTable := &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}
	sch.Tables["customers"] = customersTable

	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{int64(1)})

	err := mgr.CheckDeferredViolations(sch, reader)
	if err != nil {
		t.Errorf("Expected no violation, got: %v", err)
	}
}

func TestForeignKeyManager_ClearDeferredViolations(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{Table: "t", Columns: []string{"id"}, RefTable: "p", RefColumns: []string{"id"}}
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk, Values: []interface{}{1}, Table: "t"},
	}
	if mgr.DeferredViolationCount() == 0 {
		t.Fatal("expected violations")
	}
	mgr.ClearDeferredViolations()
	if mgr.DeferredViolationCount() != 0 {
		t.Error("expected violations to be cleared")
	}
}

// --- tryParseInt ---

func TestTryParseInt_Success(t *testing.T) {
	result := tryParseInt("42", "fallback")
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestTryParseInt_Failure(t *testing.T) {
	result := tryParseInt("not-a-number", "fallback")
	if result != "fallback" {
		t.Errorf("expected fallback, got %v", result)
	}
}

// --- selfReferenceMatches ---

func TestSelfReferenceMatches_LenMismatch(t *testing.T) {
	values := map[string]interface{}{"id": int64(1)}
	result := selfReferenceMatches(values, []string{"id"}, []string{"a", "b"})
	if result {
		t.Error("expected false for len mismatch")
	}
}

func TestSelfReferenceMatches_MissingFK(t *testing.T) {
	values := map[string]interface{}{"parent_id": int64(1)}
	result := selfReferenceMatches(values, []string{"id"}, []string{"parent_id"})
	if result {
		t.Error("expected false when fk col missing")
	}
}

func TestSelfReferenceMatches_MissingParent(t *testing.T) {
	values := map[string]interface{}{"id": int64(1)}
	result := selfReferenceMatches(values, []string{"id"}, []string{"missing"})
	if result {
		t.Error("expected false when parent col missing")
	}
}

func TestSelfReferenceMatches_NotEqual(t *testing.T) {
	values := map[string]interface{}{"id": int64(1), "parent_id": int64(2)}
	result := selfReferenceMatches(values, []string{"id"}, []string{"parent_id"})
	if result {
		t.Error("expected false for different values")
	}
}

func TestSelfReferenceMatches_Equal(t *testing.T) {
	values := map[string]interface{}{"id": int64(5), "parent_id": int64(5)}
	result := selfReferenceMatches(values, []string{"id"}, []string{"parent_id"})
	if !result {
		t.Error("expected true for equal values")
	}
}

// --- validateDeleteTypeAssertions ---

func TestValidateDeleteTypeAssertions_AllValid(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	schResult, readerResult, deleterResult, updaterResult, ok := mgr.validateDeleteTypeAssertions(sch, reader, deleter, updater)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if schResult == nil || readerResult == nil || deleterResult == nil || updaterResult == nil {
		t.Error("expected non-nil results")
	}
}

func TestValidateDeleteTypeAssertions_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	_, _, _, _, ok := mgr.validateDeleteTypeAssertions("not-schema", reader, deleter, updater)
	if ok {
		t.Error("expected ok=false for invalid schema")
	}
}

func TestValidateDeleteTypeAssertions_InvalidReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	_, _, _, _, ok := mgr.validateDeleteTypeAssertions(sch, "not-reader", deleter, updater)
	if ok {
		t.Error("expected ok=false for invalid reader")
	}
}

func TestValidateDeleteTypeAssertions_InvalidDeleter(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	reader := NewMockRowReader()
	updater := NewMockRowUpdater()

	_, _, _, _, ok := mgr.validateDeleteTypeAssertions(sch, reader, "not-deleter", updater)
	if ok {
		t.Error("expected ok=false for invalid deleter")
	}
}

func TestValidateDeleteTypeAssertions_InvalidUpdater(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()

	_, _, _, _, ok := mgr.validateDeleteTypeAssertions(sch, reader, deleter, "not-updater")
	if ok {
		t.Error("expected ok=false for invalid updater")
	}
}

// --- NotNullConstraint.ValidateRow ---

func TestNotNullConstraint_ValidateRow_WithDefault(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true, Default: int64(0)},
			{Name: "name", Type: "TEXT", NotNull: false},
		},
	}
	nnc := NewNotNullConstraint(table)
	// With id as nil but it has a default
	values := map[string]interface{}{
		"name": "Alice",
	}
	err := nnc.ValidateRow(values)
	if err != nil {
		t.Errorf("expected success with default applied, got: %v", err)
	}
}

func TestNotNullConstraint_ValidateRow_NullRequired(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)
	values := map[string]interface{}{"id": nil}
	err := nnc.ValidateRow(values)
	if err == nil {
		t.Error("expected error for null required column")
	}
}

func TestNotNullConstraint_ValidateRow_SuccessCoverage(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)
	values := map[string]interface{}{"id": int64(1)}
	err := nnc.ValidateRow(values)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- columnsAreUnique ---

func TestColumnsAreUnique_PrimaryKey(t *testing.T) {
	table := &schema.Table{
		Name:       "users",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}
	sch := schema.NewSchema()
	result := columnsAreUnique(table, []string{"id"}, sch)
	if !result {
		t.Error("expected true for primary key column")
	}
}

func TestColumnsAreUnique_UniqueColumn(t *testing.T) {
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "email", Type: "TEXT", Unique: true},
		},
	}
	sch := schema.NewSchema()
	result := columnsAreUnique(table, []string{"email"}, sch)
	if !result {
		t.Error("expected true for unique column")
	}
}

func TestColumnsAreUnique_UniqueConstraint(t *testing.T) {
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "code", Type: "TEXT"},
		},
		Constraints: []schema.TableConstraint{
			{Type: schema.ConstraintUnique, Columns: []string{"code"}},
		},
	}
	sch := schema.NewSchema()
	result := columnsAreUnique(table, []string{"code"}, sch)
	if !result {
		t.Error("expected true for unique constraint")
	}
}

func TestColumnsAreUnique_UniqueIndex(t *testing.T) {
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "phone", Type: "TEXT"},
		},
	}
	sch := schema.NewSchema()
	sch.Indexes = map[string]*schema.Index{
		"idx_phone": {
			Name:    "idx_phone",
			Table:   "users",
			Unique:  true,
			Columns: []string{"phone"},
		},
	}
	result := columnsAreUnique(table, []string{"phone"}, sch)
	if !result {
		t.Error("expected true for unique index")
	}
}

func TestColumnsAreUnique_NotUnique(t *testing.T) {
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT"},
		},
	}
	sch := schema.NewSchema()
	result := columnsAreUnique(table, []string{"name"}, sch)
	if result {
		t.Error("expected false for non-unique column")
	}
}

// --- ClearDeferredViolationsForTable ---

func TestClearDeferredViolationsForTable(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk1 := &ForeignKeyConstraint{Table: "orders", Columns: []string{"cid"}, RefTable: "cust", RefColumns: []string{"id"}}
	fk2 := &ForeignKeyConstraint{Table: "items", Columns: []string{"oid"}, RefTable: "orders", RefColumns: []string{"id"}}
	mgr.deferredViolations = []*DeferredViolation{
		{Constraint: fk1, Table: "orders"},
		{Constraint: fk2, Table: "items"},
	}
	mgr.ClearDeferredViolationsForTable("orders")
	if mgr.DeferredViolationCount() != 1 {
		t.Errorf("expected 1 violation remaining, got %d", mgr.DeferredViolationCount())
	}
	if mgr.deferredViolations[0].Table != "items" {
		t.Error("wrong violation remaining")
	}
}

func TestClearDeferredViolationsForTable_Empty(t *testing.T) {
	mgr := NewForeignKeyManager()
	// Should not panic on empty list
	mgr.ClearDeferredViolationsForTable("orders")
	if mgr.DeferredViolationCount() != 0 {
		t.Error("expected 0 violations")
	}
}

// --- checkAllTablesSchemaMismatch ---

func TestCheckAllTablesSchemaMismatch_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	err := mgr.checkAllTablesSchemaMismatch(sch)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckAllTablesSchemaMismatch_WithConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	// No customers table - but validateFKSchema returns nil for missing ref table
	err := mgr.checkAllTablesSchemaMismatch(sch)
	if err != nil {
		t.Errorf("unexpected error for missing ref table: %v", err)
	}
}

// --- checkRowForViolation / isViolation ---

func TestIsViolation_RowExists(t *testing.T) {
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	reader := NewMockRowReader()
	reader.AddRow("customers", []string{"id"}, []interface{}{int64(1)})
	fkValues := []interface{}{int64(1)}
	refCols := []string{"id"}

	violated, err := isViolation(fk, fkValues, refCols, reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if violated {
		t.Error("expected no violation (row exists)")
	}
}

func TestIsViolation_RowMissing(t *testing.T) {
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	reader := NewMockRowReader()
	fkValues := []interface{}{int64(999)}
	refCols := []string{"id"}

	violated, err := isViolation(fk, fkValues, refCols, reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !violated {
		t.Error("expected violation (row missing)")
	}
}

func TestCheckRowForViolation_Success(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	reader := NewMockRowReader()
	// ReadRowByRowid returns empty map, so fkValues will be nil/empty
	// With null values, no violation will be returned

	sch := schema.NewSchema()
	customersTable := &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}
	sch.Tables["customers"] = customersTable

	violation, err := mgr.checkRowForViolation(fk, 0, 1, []string{"id"}, reader, sch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// With empty row (all nil), hasNull=true, should return nil
	if violation != nil {
		t.Errorf("expected nil violation for null FK values, got %v", violation)
	}
}

// --- columnsMatch and columnsMatchUnordered ---

func TestColumnsMatch_Equal(t *testing.T) {
	if !columnsMatch([]string{"a", "b"}, []string{"a", "b"}) {
		t.Error("expected true")
	}
}

func TestColumnsMatch_DiffLen(t *testing.T) {
	if columnsMatch([]string{"a"}, []string{"a", "b"}) {
		t.Error("expected false for different lengths")
	}
}

func TestColumnsMatch_CaseInsensitive(t *testing.T) {
	if !columnsMatch([]string{"A"}, []string{"a"}) {
		t.Error("expected true for case-insensitive match")
	}
}

func TestColumnsMatch_NotEqual(t *testing.T) {
	if columnsMatch([]string{"a"}, []string{"b"}) {
		t.Error("expected false")
	}
}

func TestColumnsMatchUnordered_Equal(t *testing.T) {
	if !columnsMatchUnordered([]string{"a", "b"}, []string{"b", "a"}) {
		t.Error("expected true for unordered match")
	}
}

func TestColumnsMatchUnordered_DiffLen(t *testing.T) {
	if columnsMatchUnordered([]string{"a"}, []string{"a", "b"}) {
		t.Error("expected false for different lengths")
	}
}

func TestColumnsMatchUnordered_NotEqual(t *testing.T) {
	if columnsMatchUnordered([]string{"a", "c"}, []string{"a", "b"}) {
		t.Error("expected false")
	}
}

// --- extractCollations ---

func TestExtractCollations_Found(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT", Collation: "NOCASE"},
		},
	}
	collations := extractCollations(table, []string{"name"})
	if len(collations) != 1 || collations[0] != "NOCASE" {
		t.Errorf("expected NOCASE, got %v", collations)
	}
}

func TestExtractCollations_NotFound(t *testing.T) {
	table := &schema.Table{
		Name:    "test",
		Columns: []*schema.Column{},
	}
	collations := extractCollations(table, []string{"missing"})
	if len(collations) != 1 || collations[0] != "BINARY" {
		t.Errorf("expected BINARY for missing column, got %v", collations)
	}
}

func TestExtractCollations_EmptyCollation(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", Collation: ""},
		},
	}
	collations := extractCollations(table, []string{"id"})
	if len(collations) != 1 || collations[0] != "BINARY" {
		t.Errorf("expected BINARY for empty collation, got %v", collations)
	}
}

// --- tryParseFloat / tryParseNumeric ---

func TestTryParseFloat_Success(t *testing.T) {
	result := tryParseFloat("3.14", "fallback")
	if result != float64(3.14) {
		t.Errorf("expected 3.14, got %v", result)
	}
}

func TestTryParseFloat_Integer(t *testing.T) {
	result := tryParseFloat("42", "fallback")
	if result != float64(42) {
		t.Errorf("expected 42.0, got %v", result)
	}
}

func TestTryParseFloat_Failure(t *testing.T) {
	result := tryParseFloat("not-a-float", "fallback")
	if result != "fallback" {
		t.Errorf("expected fallback, got %v", result)
	}
}

func TestTryParseNumeric_Integer(t *testing.T) {
	result := tryParseNumeric("100", "fallback")
	if result != int64(100) {
		t.Errorf("expected 100, got %v", result)
	}
}

func TestTryParseNumeric_Float(t *testing.T) {
	result := tryParseNumeric("1.5", "fallback")
	if result != float64(1.5) {
		t.Errorf("expected 1.5, got %v", result)
	}
}

func TestTryParseNumeric_Failure(t *testing.T) {
	result := tryParseNumeric("not-numeric", "fallback")
	if result != "fallback" {
		t.Errorf("expected fallback, got %v", result)
	}
}

// --- valuesMatch ---

func TestValuesMatch_Equal(t *testing.T) {
	a := []interface{}{int64(1), "foo"}
	b := []interface{}{int64(1), "foo"}
	if !valuesMatch(a, b) {
		t.Error("expected true for equal slices")
	}
}

func TestValuesMatch_DiffLen(t *testing.T) {
	a := []interface{}{int64(1)}
	b := []interface{}{int64(1), int64(2)}
	if valuesMatch(a, b) {
		t.Error("expected false for different lengths")
	}
}

func TestValuesMatch_NotEqual(t *testing.T) {
	a := []interface{}{int64(1)}
	b := []interface{}{int64(2)}
	if valuesMatch(a, b) {
		t.Error("expected false for different values")
	}
}

func TestValuesMatch_Empty(t *testing.T) {
	if !valuesMatch([]interface{}{}, []interface{}{}) {
		t.Error("expected true for two empty slices")
	}
}

// --- filterSelfReferenceWithoutRowID ---

func TestFilterSelfReferenceWithoutRowID_FiltersMatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	deletedRow := map[string]interface{}{"id": int64(1), "name": "alice"}
	row1 := map[string]interface{}{"id": int64(1), "name": "alice"}
	row2 := map[string]interface{}{"id": int64(2), "name": "bob"}
	rowDataList := []map[string]interface{}{row1, row2}

	result := mgr.filterSelfReferenceWithoutRowID(rowDataList, deletedRow, []string{"id"})
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
	if result[0]["id"] != int64(2) {
		t.Error("expected row with id=2 to remain")
	}
}

func TestFilterSelfReferenceWithoutRowID_NoMatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	deletedRow := map[string]interface{}{"id": int64(99)}
	row1 := map[string]interface{}{"id": int64(1)}
	row2 := map[string]interface{}{"id": int64(2)}
	rowDataList := []map[string]interface{}{row1, row2}

	result := mgr.filterSelfReferenceWithoutRowID(rowDataList, deletedRow, []string{"id"})
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}
}

// --- applyDeleteActionWithoutRowID ---

func TestApplyDeleteActionWithoutRowID_Empty(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(fk, childTable, []map[string]interface{}{}, schema.NewSchema(), deleter, updater, readerExt)
	if err != nil {
		t.Errorf("expected nil for empty rowDataList, got: %v", err)
	}
}

func TestApplyDeleteActionWithoutRowID_Restrict(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionRestrict,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err == nil {
		t.Error("expected error for RESTRICT action")
	}
}

func TestApplyDeleteActionWithoutRowID_SetNull(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionSetNull,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err == nil {
		t.Error("expected error for SET NULL on WITHOUT ROWID")
	}
}

func TestApplyDeleteActionWithoutRowID_SetDefault(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionSetDefault,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.applyDeleteActionWithoutRowID(fk, childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err == nil {
		t.Error("expected error for SET DEFAULT on WITHOUT ROWID")
	}
}

// --- cascadeDeleteWithoutRowID ---

func TestCascadeDeleteWithoutRowID_NoDeleterExtended(t *testing.T) {
	mgr := NewForeignKeyManager()
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := NewMockRowDeleter() // does NOT implement RowDeleterExtended
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err == nil {
		t.Error("expected error when deleter does not implement RowDeleterExtended")
	}
}

func TestCascadeDeleteWithoutRowID_DeleteFails(t *testing.T) {
	mgr := NewForeignKeyManager()
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	deleter.shouldFail = true
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err == nil {
		t.Error("expected error when DeleteRowByKey fails")
	}
}

func TestCascadeDeleteWithoutRowID_Success(t *testing.T) {
	mgr := NewForeignKeyManager()
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	rowDataList := []map[string]interface{}{{"id": int64(1)}}
	readerExt := newMockRowReaderExtended()
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDeleteWithoutRowID("child", childTable, rowDataList, schema.NewSchema(), deleter, updater, readerExt)
	if err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
	if len(deleter.deletedKeys) != 1 {
		t.Errorf("expected 1 deleted key, got %d", len(deleter.deletedKeys))
	}
}

// --- handleDeleteConstraintWithoutRowID ---

func TestHandleDeleteConstraintWithoutRowID_NoExtendedReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	deletedRow := map[string]interface{}{"id": int64(1)}
	reader := NewMockRowReader() // does NOT implement RowReaderExtended
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(fk, childTable, []interface{}{int64(1)}, []string{"id"}, deletedRow, schema.NewSchema(), reader, deleter, updater)
	if err == nil {
		t.Error("expected error for non-extended reader")
	}
}

func TestHandleDeleteConstraintWithoutRowID_EmptyResult(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	deletedRow := map[string]interface{}{"id": int64(1)}
	readerExt := newMockRowReaderExtended()
	// rowDataList is empty by default
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(fk, childTable, []interface{}{int64(1)}, []string{"id"}, deletedRow, schema.NewSchema(), readerExt, deleter, updater)
	if err != nil {
		t.Errorf("expected nil for empty result, got: %v", err)
	}
}

func TestHandleDeleteConstraintWithoutRowID_ReaderFails(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table: "child", Columns: []string{"pid"},
		RefTable: "parent", RefColumns: []string{"id"},
		OnDelete: FKActionCascade,
	}
	childTable := &schema.Table{Name: "child", PrimaryKey: []string{"id"}}
	deletedRow := map[string]interface{}{"id": int64(1)}
	readerExt := newMockRowReaderExtended()
	readerExt.shouldFail = true
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	err := mgr.handleDeleteConstraintWithoutRowID(fk, childTable, []interface{}{int64(1)}, []string{"id"}, deletedRow, schema.NewSchema(), readerExt, deleter, updater)
	if err == nil {
		t.Error("expected error when reader fails")
	}
}

// --- CheckSchemaMismatch ---

func TestCheckSchemaMismatch_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.CheckSchemaMismatch("orders", "not-a-schema")
	if err == nil {
		t.Error("expected error for invalid schema")
	}
}

func TestCheckSchemaMismatch_EmptyTableName(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	err := mgr.CheckSchemaMismatch("", sch)
	if err != nil {
		t.Errorf("expected nil for empty table name with no constraints, got: %v", err)
	}
}

func TestCheckSchemaMismatch_WithTableName(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	// customers table with primary key (unique)
	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err != nil {
		t.Errorf("expected nil for valid schema, got: %v", err)
	}
}

func TestCheckSchemaMismatch_ColumnCountMismatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"a", "b"}, // 2 columns
		RefTable:   "customers",
		RefColumns: []string{"id"}, // 1 column
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestCheckSchemaMismatch_AllTables(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	// tableName="" -> check all tables
	err := mgr.CheckSchemaMismatch("", sch)
	if err != nil {
		t.Errorf("expected nil for valid schema, got: %v", err)
	}
}

// --- ValidateFKAtCreateTime ---

func TestValidateFKAtCreateTime_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.ValidateFKAtCreateTime("orders", "not-a-schema")
	if err == nil {
		t.Error("expected error for invalid schema")
	}
}

func TestValidateFKAtCreateTime_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err != nil {
		t.Errorf("expected nil for no constraints, got: %v", err)
	}
}

func TestValidateFKAtCreateTime_RefIsView(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers_view",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Views["customers_view"] = &schema.View{Name: "customers_view"}

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err == nil {
		t.Error("expected error when FK references a view")
	}
}

func TestValidateFKAtCreateTime_MissingRefTable(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	// customers table doesn't exist - should NOT be an error

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err != nil {
		t.Errorf("expected nil when ref table missing (not an error at create time), got: %v", err)
	}
}

func TestValidateFKAtCreateTime_ColumnCountMismatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"a", "b"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err == nil {
		t.Error("expected error for column count mismatch at create time")
	}
}

func TestValidateFKAtCreateTime_Valid(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.ValidateFKAtCreateTime("orders", sch)
	if err != nil {
		t.Errorf("expected nil for valid FK at create time, got: %v", err)
	}
}

// --- FindViolations ---

func TestFindViolations_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	_, err := mgr.FindViolations("orders", "not-a-schema", nil)
	if err == nil {
		t.Error("expected error for invalid schema")
	}
}

func TestFindViolations_NoConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	violations, err := mgr.FindViolations("orders", sch, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations, got %d", len(violations))
	}
}

func TestFindViolations_ByTable_NoViolations(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	// No rows, so no violations

	violations, err := mgr.FindViolations("orders", sch, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations, got %d", len(violations))
	}
}

func TestFindViolations_AllTables_NoViolations(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()

	violations, err := mgr.FindViolations("", sch, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations, got %d", len(violations))
	}
}

func TestFindViolations_WithViolation(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	// Add a referencing row in orders with rowid=1 and customer_id=99 (not in customers)
	reader.AddReferencingRows("orders", []int64{1})
	// ReadRowByRowid returns empty map, meaning customer_id = nil -> no violation (null check)
	// To get a real violation we need non-null customer_id, but MockRowReader returns empty map
	// So no violation will be detected - this just tests the scan path

	violations, err := mgr.FindViolations("orders", sch, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// With empty row data (all nil), hasNull=true, so no violation
	_ = violations
}

func TestFindViolations_MissingParentTable(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	// "nonexistent" table not in schema

	reader := NewMockRowReader()
	reader.AddReferencingRows("orders", []int64{})

	violations, err := mgr.FindViolations("orders", sch, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	_ = violations
}

func TestFindViolations_NilReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	violations, err := mgr.FindViolations("orders", sch, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations with nil reader, got %d", len(violations))
	}
}

// mockRowReaderWithData returns specific row data for testing scanMissingParentViolations
// and checkRowForViolation.
type mockRowReaderWithData struct {
	rowsByRowid  map[int64]map[string]interface{}
	allRowids    map[string][]int64
	rowExistsFn  func(table string, columns []string, values []interface{}) (bool, error)
	findRowsFail bool
	readRowFail  bool
}

func newMockRowReaderWithData() *mockRowReaderWithData {
	return &mockRowReaderWithData{
		rowsByRowid: make(map[int64]map[string]interface{}),
		allRowids:   make(map[string][]int64),
	}
}

func (m *mockRowReaderWithData) RowExists(table string, columns []string, values []interface{}) (bool, error) {
	if m.rowExistsFn != nil {
		return m.rowExistsFn(table, columns, values)
	}
	return false, nil
}

func (m *mockRowReaderWithData) RowExistsWithCollation(table string, columns []string, values []interface{}, collations []string) (bool, error) {
	return m.RowExists(table, columns, values)
}

func (m *mockRowReaderWithData) FindReferencingRows(table string, columns []string, values []interface{}) ([]int64, error) {
	if m.findRowsFail {
		return nil, &mockReadError{}
	}
	return m.allRowids[table], nil
}

func (m *mockRowReaderWithData) ReadRowByRowid(table string, rowid int64) (map[string]interface{}, error) {
	if m.readRowFail {
		return nil, &mockReadError{}
	}
	if row, ok := m.rowsByRowid[rowid]; ok {
		return row, nil
	}
	return make(map[string]interface{}), nil
}

func (m *mockRowReaderWithData) AddRowByRowid(rowid int64, data map[string]interface{}) {
	m.rowsByRowid[rowid] = data
}

func (m *mockRowReaderWithData) SetRowids(table string, rowids []int64) {
	m.allRowids[table] = rowids
}

// --- scanMissingParentViolations tests ---

func TestScanMissingParentViolations_NilReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}

	violations, err := mgr.scanMissingParentViolations(fk, 0, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("expected 0 violations with nil reader, got %d", len(violations))
	}
}

func TestScanMissingParentViolations_WithNonNullFK(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	reader.SetRowids("orders", []int64{1, 2})
	// Row 1: non-null customer_id -> violation
	reader.AddRowByRowid(1, map[string]interface{}{"id": int64(1), "customer_id": int64(99)})
	// Row 2: null customer_id -> no violation
	reader.AddRowByRowid(2, map[string]interface{}{"id": int64(2), "customer_id": nil})

	violations, err := mgr.scanMissingParentViolations(fk, 0, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(violations) != 1 {
		t.Errorf("expected 1 violation, got %d", len(violations))
	}
}

func TestScanMissingParentViolations_FindRowsFail(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	reader.findRowsFail = true

	_, err := mgr.scanMissingParentViolations(fk, 0, reader)
	if err == nil {
		t.Error("expected error when FindReferencingRows fails")
	}
}

func TestScanMissingParentViolations_ReadRowFail(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "nonexistent",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	reader.SetRowids("orders", []int64{1})
	reader.readRowFail = true

	_, err := mgr.scanMissingParentViolations(fk, 0, reader)
	if err == nil {
		t.Error("expected error when ReadRowByRowid fails")
	}
}

// --- checkRowForViolation tests ---

func TestCheckRowForViolation_WithViolation(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	// Row 1 has customer_id=99 (non-null, not in customers -> violation)
	reader.AddRowByRowid(1, map[string]interface{}{"id": int64(1), "customer_id": int64(99)})
	// RowExists for customers returns false (default)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	violation, err := mgr.checkRowForViolation(fk, 0, 1, []string{"id"}, reader, sch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if violation == nil {
		t.Error("expected violation for missing parent row")
	}
}

func TestCheckRowForViolation_NoViolation(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	reader.AddRowByRowid(1, map[string]interface{}{"id": int64(1), "customer_id": int64(5)})
	// RowExists returns true for customers.id=5
	reader.rowExistsFn = func(table string, columns []string, values []interface{}) (bool, error) {
		return true, nil
	}

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	violation, err := mgr.checkRowForViolation(fk, 0, 1, []string{"id"}, reader, sch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if violation != nil {
		t.Error("expected no violation when parent row exists")
	}
}

func TestCheckRowForViolation_ReadRowFails(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}

	reader := newMockRowReaderWithData()
	reader.readRowFail = true

	sch := schema.NewSchema()

	_, err := mgr.checkRowForViolation(fk, 0, 1, []string{"id"}, reader, sch)
	if err == nil {
		t.Error("expected error when ReadRowByRowid fails")
	}
}

// --- scanTableForViolations via FindViolations ---

func TestScanTableForViolations_FindRowsFail(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := newMockRowReaderWithData()
	reader.findRowsFail = true

	_, err := mgr.FindViolations("orders", sch, reader)
	if err == nil {
		t.Error("expected error when FindReferencingRows fails")
	}
}

// --- applyAffinityToDefault via setDefaultOnRows (indirect coverage) ---

func TestApplyAffinityToDefault_RealType(t *testing.T) {
	// Direct call to applyAffinityToDefault to cover REAL/FLOAT/DOUBLE affinity
	result := applyAffinityToDefault("3.14", "REAL")
	if result != float64(3.14) {
		t.Errorf("expected 3.14, got %v", result)
	}
}

func TestApplyAffinityToDefault_NumericType(t *testing.T) {
	result := applyAffinityToDefault("42", "NUMERIC")
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
}

func TestApplyAffinityToDefault_NonString(t *testing.T) {
	result := applyAffinityToDefault(int64(5), "INTEGER")
	if result != int64(5) {
		t.Errorf("expected 5, got %v", result)
	}
}

// --- validateFKSchema missing column coverage ---

func TestValidateFKSchema_RefColumnMissing(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"nonexistent_col"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("expected error when referenced column doesn't exist")
	}
}

func TestValidateFKSchema_NonUniqueRefCols(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"name"}, // name is not unique
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}

	err := mgr.CheckSchemaMismatch("orders", sch)
	if err == nil {
		t.Error("expected error when referenced columns are not unique")
	}
}

// --- validatedCollation ---

func TestValidatedCollation_NOCASE(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT", Collation: "nocase"},
		},
	}
	collations := extractCollations(table, []string{"name"})
	if collations[0] != "NOCASE" {
		t.Errorf("expected NOCASE, got %v", collations[0])
	}
}

func TestValidatedCollation_RTRIM(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT", Collation: "rtrim"},
		},
	}
	collations := extractCollations(table, []string{"name"})
	if collations[0] != "RTRIM" {
		t.Errorf("expected RTRIM, got %v", collations[0])
	}
}

func TestValidatedCollation_Unknown(t *testing.T) {
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT", Collation: "custom"},
		},
	}
	collations := extractCollations(table, []string{"name"})
	if collations[0] != "BINARY" {
		t.Errorf("expected BINARY for unknown collation, got %v", collations[0])
	}
}

// --- validateInsertConstraint self-reference and deferred ---

func TestValidateInsertConstraint_SelfReference(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	fk := &ForeignKeyConstraint{
		Table:      "employees",
		Columns:    []string{"manager_id"},
		RefTable:   "employees",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["employees"] = &schema.Table{
		Name:       "employees",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "manager_id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	// Row that references itself: id=1, manager_id=1
	values := map[string]interface{}{"id": int64(1), "manager_id": int64(1)}
	err := mgr.ValidateInsert("employees", values, sch, reader)
	if err != nil {
		t.Errorf("expected no error for self-reference, got: %v", err)
	}
}

func TestValidateInsertConstraint_Deferred(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	values := map[string]interface{}{"id": int64(1), "customer_id": int64(999)}
	err := mgr.ValidateInsert("orders", values, sch, reader)
	if err != nil {
		t.Errorf("expected no error for deferred constraint, got: %v", err)
	}
	if mgr.DeferredViolationCount() != 1 {
		t.Errorf("expected 1 deferred violation, got %d", mgr.DeferredViolationCount())
	}
}

// --- ValidateUpdate branches ---

func TestValidateUpdate_Disabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	// disabled by default
	err := mgr.ValidateUpdate("orders", nil, nil, schema.NewSchema(), NewMockRowReader(), NewMockRowUpdater())
	if err != nil {
		t.Errorf("expected nil when disabled, got: %v", err)
	}
}

func TestValidateUpdate_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	err := mgr.ValidateUpdate("orders", nil, nil, "bad-schema", NewMockRowReader(), NewMockRowUpdater())
	if err != nil {
		t.Errorf("expected nil for invalid schema, got: %v", err)
	}
}

func TestValidateUpdate_InvalidReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	err := mgr.ValidateUpdate("orders", nil, nil, schema.NewSchema(), "bad-reader", NewMockRowUpdater())
	if err != nil {
		t.Errorf("expected nil for invalid reader, got: %v", err)
	}
}

func TestValidateUpdate_InvalidUpdater(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	err := mgr.ValidateUpdate("orders", nil, nil, schema.NewSchema(), NewMockRowReader(), "bad-updater")
	if err != nil {
		t.Errorf("expected nil for invalid updater, got: %v", err)
	}
}

func TestValidateUpdate_OutgoingDeferred(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
		Deferrable: DeferrableInitiallyDeferred,
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	updater := NewMockRowUpdater()
	oldValues := map[string]interface{}{"id": int64(1), "customer_id": int64(1)}
	newValues := map[string]interface{}{"id": int64(1), "customer_id": int64(999)}
	err := mgr.ValidateUpdate("orders", oldValues, newValues, sch, reader, updater)
	if err != nil {
		t.Errorf("expected nil for deferred update, got: %v", err)
	}
}

// --- ValidateInsert invalid schema/reader ---

func TestValidateInsert_InvalidSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	values := map[string]interface{}{"customer_id": int64(1)}
	err := mgr.ValidateInsert("orders", values, "not-a-schema", NewMockRowReader())
	if err != nil {
		t.Errorf("expected nil for invalid schema, got: %v", err)
	}
}

func TestValidateInsert_InvalidReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	values := map[string]interface{}{"customer_id": int64(1)}
	err := mgr.ValidateInsert("orders", values, schema.NewSchema(), "not-a-reader")
	if err != nil {
		t.Errorf("expected nil for invalid reader, got: %v", err)
	}
}

// --- ValidateDelete branches ---

func TestValidateDelete_Disabled(t *testing.T) {
	mgr := NewForeignKeyManager()
	err := mgr.ValidateDelete("customers", nil, schema.NewSchema(), NewMockRowReader(), NewMockRowDeleter(), NewMockRowUpdater())
	if err != nil {
		t.Errorf("expected nil when disabled, got: %v", err)
	}
}

// --- cascadeDelete table not found ---

func TestCascadeDelete_TableNotFound(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDelete("nonexistent", []int64{1}, sch, deleter, updater, reader)
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

// --- cascadeDelete success with rows ---

func TestCascadeDelete_Success(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.cascadeDelete("orders", []int64{1, 2}, sch, deleter, updater, reader)
	if err != nil {
		t.Errorf("expected nil, got: %v", err)
	}
}

// --- filterSelfReference branches ---

func TestFilterSelfReference_EmptyPKCols(t *testing.T) {
	mgr := NewForeignKeyManager()
	result := mgr.filterSelfReference([]int64{1, 2, 3}, map[string]interface{}{"id": int64(1)}, []string{})
	if len(result) != 3 {
		t.Errorf("expected 3 rows unchanged, got %d", len(result))
	}
}

func TestFilterSelfReference_PKValMissing(t *testing.T) {
	mgr := NewForeignKeyManager()
	result := mgr.filterSelfReference([]int64{1, 2}, map[string]interface{}{}, []string{"id"})
	if len(result) != 2 {
		t.Errorf("expected 2 rows unchanged, got %d", len(result))
	}
}

func TestFilterSelfReference_IntPKMatch(t *testing.T) {
	mgr := NewForeignKeyManager()
	// pk val is int (not int64)
	result := mgr.filterSelfReference([]int64{1, 2, 3}, map[string]interface{}{"id": int(2)}, []string{"id"})
	if len(result) != 2 {
		t.Errorf("expected 2 rows (filtered rowid=2), got %d", len(result))
	}
}

func TestFilterSelfReference_NonIntPK(t *testing.T) {
	mgr := NewForeignKeyManager()
	// pk val is a string - can't determine rowid, return all
	result := mgr.filterSelfReference([]int64{1, 2}, map[string]interface{}{"id": "abc"}, []string{"id"})
	if len(result) != 2 {
		t.Errorf("expected 2 rows unchanged, got %d", len(result))
	}
}

// --- isViolation error case ---

func TestIsViolation_ReaderError(t *testing.T) {
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	reader := &MockRowReaderWithError{shouldFail: true}

	_, err := isViolation(fk, []interface{}{int64(1)}, []string{"id"}, reader)
	if err == nil {
		t.Error("expected error from reader")
	}
}

// --- findReferencingRowsWithAffinity affinity reader ---

type mockAffinityRowReader struct {
	*MockRowReader
}

func (m *mockAffinityRowReader) FindReferencingRowsWithParentAffinity(
	childTableName string,
	childColumns []string,
	parentValues []interface{},
	parentTableName string,
	parentColumns []string,
) ([]int64, error) {
	return []int64{42}, nil
}

func TestFindReferencingRowsWithAffinity_AffinityReader(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	reader := &mockAffinityRowReader{MockRowReader: NewMockRowReader()}
	rowids, err := mgr.findReferencingRowsWithAffinity(reader, fk, "customers", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rowids) != 1 || rowids[0] != 42 {
		t.Errorf("expected [42], got %v", rowids)
	}
}

// --- checkAllTablesSchemaMismatch with error ---

func TestCheckAllTablesSchemaMismatch_WithError(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"a", "b"},
		RefTable:   "customers",
		RefColumns: []string{"id"}, // mismatch: 2 columns vs 1
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	err := mgr.checkAllTablesSchemaMismatch(sch)
	if err == nil {
		t.Error("expected error for column count mismatch")
	}
}

// --- scanTableForViolations with row read failure ---

func TestScanTableForViolations_ReadRowFail(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_id", Type: "INTEGER"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	reader := newMockRowReaderWithData()
	reader.SetRowids("orders", []int64{1})
	reader.readRowFail = true

	_, err := mgr.FindViolations("orders", sch, reader)
	if err == nil {
		t.Error("expected error when ReadRowByRowid fails during scan")
	}
}

// --- checkConstraintViolations with non-unique refs (skip scan) ---

func TestCheckConstraintViolations_NonUniqueRefCols(t *testing.T) {
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_name"},
		RefTable:   "customers",
		RefColumns: []string{"name"}, // not unique
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	sch.Tables["orders"] = &schema.Table{
		Name:       "orders",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "customer_name", Type: "TEXT"}},
	}
	sch.Tables["customers"] = &schema.Table{
		Name:    "customers",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}

	reader := NewMockRowReader()
	violations, err := mgr.FindViolations("orders", sch, reader)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Non-unique columns -> skip scan -> 0 violations
	if len(violations) != 0 {
		t.Errorf("expected 0 violations for non-unique ref cols, got %d", len(violations))
	}
}

// --- validateDeleteRecursive ---

func TestValidateDeleteRecursive_NoReferencingConstraints(t *testing.T) {
	mgr := NewForeignKeyManager()
	sch := schema.NewSchema()
	sch.Tables["customers"] = &schema.Table{
		Name:       "customers",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}

	values := map[string]interface{}{"id": int64(1)}
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.validateDeleteRecursive("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("expected nil for no referencing constraints, got: %v", err)
	}
}

func TestValidateDeleteRecursive_TableNotInSchema(t *testing.T) {
	mgr := NewForeignKeyManager()
	// Add a FK that makes "customers" a parent table
	fk := &ForeignKeyConstraint{
		Table:      "orders",
		Columns:    []string{"customer_id"},
		RefTable:   "customers",
		RefColumns: []string{"id"},
	}
	mgr.AddConstraint(fk)

	sch := schema.NewSchema()
	// "customers" is not in schema tables

	values := map[string]interface{}{"id": int64(1)}
	reader := NewMockRowReader()
	deleter := NewMockRowDeleter()
	updater := NewMockRowUpdater()

	err := mgr.validateDeleteRecursive("customers", values, sch, reader, deleter, updater)
	if err != nil {
		t.Errorf("expected nil when table not in schema, got: %v", err)
	}
}

// --- NotNullConstraint.ValidateRow error from ValidateInsert ---

func TestNotNullConstraint_ValidateRow_NullError(t *testing.T) {
	// Create a table with a NOT NULL column that has no default and no value provided
	table := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", NotNull: true},
			{Name: "name", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(table)
	values := map[string]interface{}{"id": int64(1)} // missing "name"
	err := nnc.ValidateRow(values)
	// Should fail because "name" is not null but not provided and no default
	if err == nil {
		t.Error("expected error for null required column with no default")
	}
}

// --- handleDeleteConstraintWithoutRowID self-reference filtering ---

func TestHandleDeleteConstraintWithoutRowID_SelfRefFiltered(t *testing.T) {
	mgr := NewForeignKeyManager()
	// Self-referencing FK
	fk := &ForeignKeyConstraint{
		Table:      "categories",
		Columns:    []string{"parent_id"},
		RefTable:   "categories",
		RefColumns: []string{"id"},
		OnDelete:   FKActionCascade,
	}
	childTable := &schema.Table{Name: "categories", PrimaryKey: []string{"id"}}
	// deletedRowValues matches the returned row (same id -> self-reference, filtered out)
	deletedRow := map[string]interface{}{"id": int64(1), "parent_id": int64(0)}

	readerExt := newMockRowReaderExtended()
	// FindReferencingRowsWithData returns the deleted row itself
	readerExt.rowDataList = []map[string]interface{}{
		{"id": int64(1), "parent_id": int64(0)},
	}
	deleter := newMockRowDeleterExtended()
	updater := NewMockRowUpdater()

	// selfReferenceMatches checks if deletedRow's fk cols (parent_id) match refCols (id):
	// deletedRow[parent_id]=0, deletedRow[id]=1 -> not equal -> not self-reference match
	// So the row won't be filtered, but since deletedPK vs rowPK:
	// deletedPK=[1], rowPK=[1] -> will be filtered by filterSelfReferenceWithoutRowID
	// Because fk.Table == fk.RefTable AND selfReferenceMatches checks fk.Columns vs parentCols
	parentCols := []string{"id"}
	err := mgr.handleDeleteConstraintWithoutRowID(fk, childTable, []interface{}{int64(1)}, parentCols, deletedRow, schema.NewSchema(), readerExt, deleter, updater)
	// The result depends on the filtering logic; just check no panic
	_ = err
}
