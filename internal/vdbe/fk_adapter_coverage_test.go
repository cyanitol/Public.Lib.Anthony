// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Mock schema types for fk_adapter tests
// ---------------------------------------------------------------------------

// fkaCovMockColumn implements the column interfaces used by fk_adapter.go.
type fkaCovMockColumn struct {
	name      string
	colType   string
	collation string
	isPK      bool
}

func (c *fkaCovMockColumn) GetName() string          { return c.name }
func (c *fkaCovMockColumn) GetType() string          { return c.colType }
func (c *fkaCovMockColumn) GetCollation() string     { return c.collation }
func (c *fkaCovMockColumn) IsPrimaryKeyColumn() bool { return c.isPK }

// fkaCovMockTable implements GetColumns and exposes RootPage/WithoutRowID as exported
// fields so extractTableMetadata can read them via reflection.
type fkaCovMockTable struct {
	RootPage     uint32
	WithoutRowID bool
	columns      []interface{}
}

func (t *fkaCovMockTable) GetColumns() []interface{} { return t.columns }

// fkaCovSchema implements the schemaWithGetTableByName interface.
type fkaCovSchema struct {
	tables map[string]interface{}
}

func (s *fkaCovSchema) GetTableByName(name string) (interface{}, bool) {
	if t, ok := s.tables[name]; ok {
		return t, true
	}
	return nil, false
}

// fkaCovNoMethodSchema holds no table-lookup method at all.
type fkaCovNoMethodSchema struct{}

// fkaCovGetTableSchema exposes a GetTable(name) (interface{}, bool) method so
// the reflection fallback path in getTable is exercised.
type fkaCovGetTableSchema struct {
	tables map[string]interface{}
}

func (s *fkaCovGetTableSchema) GetTable(name string) (interface{}, bool) {
	if t, ok := s.tables[name]; ok {
		return t, true
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newReaderWithSchema returns a VDBERowReader backed by the provided schema.
func newReaderWithSchema(schema interface{}) *VDBERowReader {
	v := New()
	v.Ctx = &VDBEContext{Schema: schema}
	return &VDBERowReader{vdbe: v}
}

// makeSimpleTable builds a fkaCovMockTable with a single TEXT column.
func makeSimpleTable(rootPage uint32) *fkaCovMockTable {
	return &fkaCovMockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		columns: []interface{}{
			&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
			&fkaCovMockColumn{name: "name", colType: "TEXT", collation: "NOCASE"},
		},
	}
}

// makeTableInfo builds a tableInfo directly, without going through schema.
func makeTableInfo(cols []columnInfo) *tableInfo {
	return &tableInfo{
		RootPage:        1,
		Columns:         cols,
		WithoutRowID:    false,
		PKColumnIndices: []int{},
	}
}

// ---------------------------------------------------------------------------
// validateContext
// ---------------------------------------------------------------------------

func TestFKAdapterValidateContext(t *testing.T) {
	t.Parallel()

	t.Run("NilVdbe", func(t *testing.T) {
		t.Parallel()
		r := &VDBERowReader{vdbe: nil}
		if err := r.validateContext(); err == nil {
			t.Error("expected error for nil vdbe")
		}
	})

	t.Run("NilCtx", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = nil
		r := &VDBERowReader{vdbe: v}
		if err := r.validateContext(); err == nil {
			t.Error("expected error for nil ctx")
		}
	})

	t.Run("ValidCtx", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{}
		r := &VDBERowReader{vdbe: v}
		if err := r.validateContext(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// getTable
// ---------------------------------------------------------------------------

func testFKAdapterGetTableFound(t *testing.T) {
	t.Parallel()
	tbl := makeSimpleTable(5)
	schema := &fkaCovSchema{tables: map[string]interface{}{"users": tbl}}
	r := newReaderWithSchema(schema)
	info, err := r.getTable("users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil tableInfo")
	}
	if len(info.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(info.Columns))
	}
}

func testFKAdapterGetTableNotFound(t *testing.T) {
	t.Parallel()
	schema := &fkaCovSchema{tables: map[string]interface{}{}}
	r := newReaderWithSchema(schema)
	_, err := r.getTable("missing")
	if err == nil {
		t.Error("expected error for missing table")
	}
}

func testFKAdapterGetTableFallbackFound(t *testing.T) {
	t.Parallel()
	tbl := makeSimpleTable(3)
	schema := &fkaCovGetTableSchema{tables: map[string]interface{}{"orders": tbl}}
	r := newReaderWithSchema(schema)
	info, err := r.getTable("orders")
	if err != nil {
		t.Fatalf("unexpected error via reflection fallback: %v", err)
	}
	if info == nil {
		t.Fatal("expected non-nil tableInfo via reflection fallback")
	}
}

func testFKAdapterGetTableFallbackNotFound(t *testing.T) {
	t.Parallel()
	schema := &fkaCovGetTableSchema{tables: map[string]interface{}{}}
	r := newReaderWithSchema(schema)
	_, err := r.getTable("missing")
	if err == nil {
		t.Error("expected error for missing table via reflection fallback")
	}
}

func testFKAdapterGetTableNoSchemaMethod(t *testing.T) {
	t.Parallel()
	r := newReaderWithSchema(&fkaCovNoMethodSchema{})
	_, err := r.getTable("any")
	if err == nil {
		t.Error("expected error when schema has no table method")
	}
}

func TestFKAdapterGetTable(t *testing.T) {
	t.Parallel()
	t.Run("TableFound_GetTableByName", testFKAdapterGetTableFound)
	t.Run("TableNotFound_GetTableByName", testFKAdapterGetTableNotFound)
	t.Run("FallbackGetTable_Found", testFKAdapterGetTableFallbackFound)
	t.Run("FallbackGetTable_NotFound", testFKAdapterGetTableFallbackNotFound)
	t.Run("NoSchemaMethod_ReturnsError", testFKAdapterGetTableNoSchemaMethod)
}

// ---------------------------------------------------------------------------
// getCollationForColumn
// ---------------------------------------------------------------------------

func TestGetCollationForColumn(t *testing.T) {
	t.Parallel()

	t.Run("InBoundsWithValue", func(t *testing.T) {
		t.Parallel()
		got := getCollationForColumn([]string{"NOCASE", "BINARY"}, 0)
		if got != "NOCASE" {
			t.Errorf("expected NOCASE, got %s", got)
		}
	})

	t.Run("InBoundsEmptyString_DefaultBinary", func(t *testing.T) {
		t.Parallel()
		got := getCollationForColumn([]string{""}, 0)
		if got != "BINARY" {
			t.Errorf("expected BINARY for empty string, got %s", got)
		}
	})

	t.Run("OutOfBounds_DefaultBinary", func(t *testing.T) {
		t.Parallel()
		got := getCollationForColumn([]string{"NOCASE"}, 5)
		if got != "BINARY" {
			t.Errorf("expected BINARY for out-of-bounds index, got %s", got)
		}
	})

	t.Run("NilSlice_DefaultBinary", func(t *testing.T) {
		t.Parallel()
		got := getCollationForColumn(nil, 0)
		if got != "BINARY" {
			t.Errorf("expected BINARY for nil slice, got %s", got)
		}
	})
}

// ---------------------------------------------------------------------------
// getParentColumnTypeAndCollation
// ---------------------------------------------------------------------------

func fkaParentTable() *tableInfo {
	return makeTableInfo([]columnInfo{
		{Name: "pid", Type: "INTEGER", Collation: ""},
		{Name: "label", Type: "TEXT", Collation: "NOCASE"},
	})
}

func testGetParentColTypeValid(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	colType, coll := r.getParentColumnTypeAndCollation(fkaParentTable(), []string{"pid", "label"}, 1)
	if colType != "TEXT" {
		t.Errorf("expected TEXT, got %s", colType)
	}
	if coll != "NOCASE" {
		t.Errorf("expected NOCASE, got %s", coll)
	}
}

func testGetParentColTypeBeyondBounds(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	colType, coll := r.getParentColumnTypeAndCollation(fkaParentTable(), []string{"pid"}, 5)
	if colType != "" || coll != "" {
		t.Errorf("expected empty strings for out-of-bounds idx, got %q %q", colType, coll)
	}
}

func testGetParentColTypeNilTable(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	colType, coll := r.getParentColumnTypeAndCollation(nil, []string{"pid"}, 0)
	if colType != "" || coll != "" {
		t.Errorf("expected empty strings for nil parent table, got %q %q", colType, coll)
	}
}

func testGetParentColTypeNotFound(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	colType, coll := r.getParentColumnTypeAndCollation(fkaParentTable(), []string{"nonexistent"}, 0)
	if colType != "" || coll != "" {
		t.Errorf("expected empty strings for missing column, got %q %q", colType, coll)
	}
}

func TestGetParentColumnTypeAndCollation(t *testing.T) {
	t.Parallel()
	t.Run("ValidIdx", testGetParentColTypeValid)
	t.Run("IdxBeyondParentColumns", testGetParentColTypeBeyondBounds)
	t.Run("NilParentTable", testGetParentColTypeNilTable)
	t.Run("ColumnNotFoundInParent", testGetParentColTypeNotFound)
}

// ---------------------------------------------------------------------------
// valuesEqualDirect
// ---------------------------------------------------------------------------

func TestValuesEqualDirect_NilAndString(t *testing.T) {
	t.Parallel()
	if !valuesEqualDirect(nil, nil) {
		t.Error("nil == nil should be true")
	}
	if !valuesEqualDirect("hello", "hello") {
		t.Error("same string should be equal")
	}
	if valuesEqualDirect("hello", "world") {
		t.Error("different strings should not be equal")
	}
	if valuesEqualDirect("abc", "def") {
		t.Error("different strings should not be equal (non-numeric fallback)")
	}
}

func TestValuesEqualDirect_Numeric(t *testing.T) {
	t.Parallel()
	if !valuesEqualDirect(int(42), int64(42)) {
		t.Error("int(42) and int64(42) should be equal")
	}
	if !valuesEqualDirect(int64(7), int64(7)) {
		t.Error("int64 equal values should match")
	}
	if valuesEqualDirect(int64(1), int64(2)) {
		t.Error("different int64 values should not be equal")
	}
	if !valuesEqualDirect(float64(10), int64(10)) {
		t.Error("float64(10) and int64(10) should be equal via toInt64")
	}
	if valuesEqualDirect(int64(5), "5") {
		t.Error("int64 and string should not be equal via this path")
	}
}

// ---------------------------------------------------------------------------
// valuesEqualWithAffinityAndCollation
// ---------------------------------------------------------------------------

func TestValuesEqualWithAffinityAndCollation(t *testing.T) {
	t.Parallel()
	t.Run("NullCases", testValuesEqualAffinityNull)
	t.Run("IntegerAffinity", testValuesEqualAffinityInteger)
	t.Run("TextAffinity", testValuesEqualAffinityText)
	t.Run("NoAffinity_FallbackDirect", testValuesEqualAffinityFallback)
}

func testValuesEqualAffinityNull(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqualWithAffinityAndCollation(NewMemNull(), nil, "TEXT", "BINARY") {
		t.Error("NULL mem vs nil should be equal")
	}
	if r.valuesEqualWithAffinityAndCollation(NewMemNull(), "hello", "TEXT", "BINARY") {
		t.Error("NULL mem vs non-nil should not be equal")
	}
}

func testValuesEqualAffinityInteger(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqualWithAffinityAndCollation(NewMemInt(42), int64(42), "INTEGER", "BINARY") {
		t.Error("integer affinity: same value should be equal")
	}
	if r.valuesEqualWithAffinityAndCollation(NewMemInt(1), int64(2), "INTEGER", "BINARY") {
		t.Error("integer affinity: different values should not be equal")
	}
}

func testValuesEqualAffinityText(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqualWithAffinityAndCollation(NewMemStr("Hello"), "hello", "TEXT", "NOCASE") {
		t.Error("TEXT+NOCASE: Hello vs hello should be equal")
	}
	if r.valuesEqualWithAffinityAndCollation(NewMemStr("Hello"), "hello", "TEXT", "BINARY") {
		t.Error("TEXT+BINARY: Hello vs hello should not be equal")
	}
	if !r.valuesEqualWithAffinityAndCollation(NewMemStr("exact"), "exact", "TEXT", "BINARY") {
		t.Error("TEXT+BINARY: same string should be equal")
	}
}

func testValuesEqualAffinityFallback(t *testing.T) {
	t.Parallel()
	r := &VDBERowReader{}
	if !r.valuesEqualWithAffinityAndCollation(NewMemInt(99), int64(99), "", "BINARY") {
		t.Error("empty type, equal int values should match")
	}
}

// ---------------------------------------------------------------------------
// findMatchingRowWithCollation (error path: bad cursor type)
// ---------------------------------------------------------------------------

func TestFindMatchingRowWithCollationBadCursor(t *testing.T) {
	t.Parallel()

	v := New()
	v.Ctx = &VDBEContext{}
	if err := v.AllocCursors(10); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}

	// Install a cursor with a non-btree BtreeCursor so getBTreeCursor fails.
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		BtreeCursor: "not-a-btcursor", // wrong type
	}

	r := &VDBERowReader{vdbe: v}
	table := makeTableInfo([]columnInfo{
		{Name: "id", Type: "INTEGER"},
	})

	_, err := r.findMatchingRowWithCollation(0, table, []string{"id"}, []interface{}{int64(1)}, []string{"BINARY"})
	if err == nil {
		t.Error("expected error for invalid cursor type")
	}
}

// ---------------------------------------------------------------------------
// checkRowMatchWithParentAffinityAndCollation (error path: bad cursor type)
// ---------------------------------------------------------------------------

func TestCheckRowMatchWithParentAffinityAndCollationBadCursor(t *testing.T) {
	t.Parallel()

	v := New()
	v.Ctx = &VDBEContext{}
	if err := v.AllocCursors(5); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}

	// Wrong cursor type causes getBTreeCursor check to fail.
	cursor := &Cursor{
		CurType:     CursorBTree,
		BtreeCursor: struct{}{}, // wrong type
	}

	r := &VDBERowReader{vdbe: v}

	childTable := makeTableInfo([]columnInfo{
		{Name: "fk_id", Type: "INTEGER"},
	})
	parentTable := makeTableInfo([]columnInfo{
		{Name: "id", Type: "INTEGER"},
	})

	_, err := r.checkRowMatchWithParentAffinityAndCollation(
		cursor,
		childTable,
		[]string{"fk_id"},
		[]interface{}{int64(1)},
		parentTable,
		[]string{"id"},
	)
	if err == nil {
		t.Error("expected error for invalid cursor type")
	}
}

// ---------------------------------------------------------------------------
// FindReferencingRowsWithParentAffinity – validateContext and getTable errors
// ---------------------------------------------------------------------------

func TestFindReferencingRowsWithParentAffinity_Errors(t *testing.T) {
	t.Parallel()

	t.Run("NilCtx_ReturnsError", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = nil
		r := &VDBERowReader{vdbe: v}
		_, err := r.FindReferencingRowsWithParentAffinity("child", []string{"fk"}, []interface{}{int64(1)}, "parent", []string{"id"})
		if err == nil {
			t.Error("expected validateContext error")
		}
	})

	t.Run("ChildTableNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		schema := &fkaCovSchema{tables: map[string]interface{}{}}
		r := newReaderWithSchema(schema)
		_, err := r.FindReferencingRowsWithParentAffinity("child", []string{"fk"}, []interface{}{int64(1)}, "parent", []string{"id"})
		if err == nil {
			t.Error("expected error for missing child table")
		}
	})

	t.Run("ParentTableNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		tbl := makeSimpleTable(2)
		schema := &fkaCovSchema{tables: map[string]interface{}{"child": tbl}}
		r := newReaderWithSchema(schema)
		_, err := r.FindReferencingRowsWithParentAffinity("child", []string{"fk"}, []interface{}{int64(1)}, "parent", []string{"id"})
		if err == nil {
			t.Error("expected error for missing parent table")
		}
	})
}

// ---------------------------------------------------------------------------
// getTable via reflection fallback: table not found (results[1].Bool() == false)
// ---------------------------------------------------------------------------

// fkaCovGetTableSchemaNotFound's GetTable always returns not-found.
type fkaCovGetTableSchemaNotFound struct{}

func (s *fkaCovGetTableSchemaNotFound) GetTable(_ string) (interface{}, bool) {
	return nil, false
}

func TestGetTable_ReflectionFallback_NotFound(t *testing.T) {
	t.Parallel()
	r := newReaderWithSchema(&fkaCovGetTableSchemaNotFound{})
	_, err := r.getTable("missing")
	if err == nil {
		t.Error("expected error when GetTable returns not-found")
	}
}

// ---------------------------------------------------------------------------
// valuesEqualWithAffinityAndCollation: real affinity path
// ---------------------------------------------------------------------------

func TestValuesEqualWithAffinityAndCollation_RealAffinity(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{}

	t.Run("RealEqual", func(t *testing.T) {
		t.Parallel()
		// Use whole number to ensure clean int64 conversion.
		mem := NewMemReal(5.0)
		if !r.valuesEqualWithAffinityAndCollation(mem, float64(5.0), "REAL", "BINARY") {
			t.Error("REAL affinity: 5.0 == 5.0 should be true")
		}
	})

	t.Run("RealNotEqual", func(t *testing.T) {
		t.Parallel()
		mem := NewMemReal(1.5)
		// 1.5 and 2.5 both convert to int64 1 and 2 – not equal
		if r.valuesEqualWithAffinityAndCollation(mem, float64(2.5), "REAL", "BINARY") {
			t.Error("REAL affinity: 1.5 vs 2.5 should not be equal")
		}
	})
}

// ---------------------------------------------------------------------------
// valuesEqualDirect: both non-numeric, non-identical (covers the false return)
// ---------------------------------------------------------------------------

func TestValuesEqualDirect_NonNumeric(t *testing.T) {
	t.Parallel()

	t.Run("StringVsString_NotEqual", func(t *testing.T) {
		t.Parallel()
		if valuesEqualDirect("abc", "xyz") {
			t.Error("different strings should not be equal")
		}
	})

	t.Run("NilVsString", func(t *testing.T) {
		t.Parallel()
		// nil != "hello"; toInt64(nil) fails, toInt64("hello") fails → false
		if valuesEqualDirect(nil, "hello") {
			t.Error("nil vs string should not be equal")
		}
	})

	t.Run("IntVsFloat_EqualValue", func(t *testing.T) {
		t.Parallel()
		// int(3) → int64(3), float64(3.0) → int64(3): equal
		if !valuesEqualDirect(int(3), float64(3.0)) {
			t.Error("int(3) and float64(3.0) should be equal via toInt64")
		}
	})
}

// ---------------------------------------------------------------------------
// getParentColumnTypeAndCollation: first column, standard path
// ---------------------------------------------------------------------------

func TestGetParentColumnTypeAndCollation_FirstColumn(t *testing.T) {
	t.Parallel()

	r := &VDBERowReader{}
	parent := makeTableInfo([]columnInfo{
		{Name: "id", Type: "INTEGER", Collation: "BINARY"},
	})

	colType, coll := r.getParentColumnTypeAndCollation(parent, []string{"id"}, 0)
	if colType != "INTEGER" {
		t.Errorf("expected INTEGER, got %q", colType)
	}
	if coll != "BINARY" {
		t.Errorf("expected BINARY, got %q", coll)
	}
}

// ---------------------------------------------------------------------------
// applyIntegerAffinity
// ---------------------------------------------------------------------------

func TestApplyIntegerAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{"Int64PassThrough", int64(42), int64(42)},
		{"IntConverted", int(7), int64(7)},
		{"Float64Truncated", float64(3.9), int64(3)},
		{"StringParsedAsInt", "123", int64(123)},
		{"StringParsedAsFloat", "4.7", int64(4)},
		{"StringUnparseable", "hello", "hello"},
		{"NilPassThrough", nil, nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := applyIntegerAffinity(tc.input)
			if got != tc.want {
				t.Errorf("applyIntegerAffinity(%v) = %v (%T), want %v (%T)",
					tc.input, got, got, tc.want, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// applyRealAffinity
// ---------------------------------------------------------------------------

func TestApplyRealAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{"Float64PassThrough", float64(1.5), float64(1.5)},
		{"Int64Converted", int64(10), float64(10)},
		{"IntConverted", int(3), float64(3)},
		{"StringParsed", "2.5", float64(2.5)},
		{"StringUnparseable", "abc", "abc"},
		{"NilPassThrough", nil, nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := applyRealAffinity(tc.input)
			if got != tc.want {
				t.Errorf("applyRealAffinity(%v) = %v (%T), want %v (%T)",
					tc.input, got, got, tc.want, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// applyTextAffinity
// ---------------------------------------------------------------------------

func TestApplyTextAffinity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{"StringPassThrough", "hello", "hello"},
		{"IntConverted", int(42), "42"},
		{"Int64Converted", int64(99), "99"},
		{"Float64Converted", float64(3.14), "3.14"},
		{"NilPassThrough", nil, nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := applyTextAffinity(tc.input)
			if got != tc.want {
				t.Errorf("applyTextAffinity(%v) = %v (%T), want %v (%T)",
					tc.input, got, got, tc.want, tc.want)
			}
		})
	}
}
