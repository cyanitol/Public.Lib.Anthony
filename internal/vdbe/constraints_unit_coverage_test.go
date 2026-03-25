// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ---------------------------------------------------------------------------
// Mock types used only in this file (avoid collision with mockSchema2 etc.)
// ---------------------------------------------------------------------------

// unitMockSchema implements schemaWithGetTableByName and
// sequenceManagerAccessor.
type unitMockSchema struct {
	table     interface{}
	sequences interface{}
}

func (m *unitMockSchema) GetTableByName(name string) (interface{}, bool) {
	if m.table != nil {
		return m.table, true
	}
	return nil, false
}

func (m *unitMockSchema) GetSequences() interface{} {
	return m.sequences
}

// unitMockSchemaNoSeq implements GetTableByName but not GetSequences.
type unitMockSchemaNoSeq struct {
	table interface{}
}

func (m *unitMockSchemaNoSeq) GetTableByName(name string) (interface{}, bool) {
	if m.table != nil {
		return m.table, true
	}
	return nil, false
}

// unitMockSequenceUpdater implements sequenceUpdater.
type unitMockSequenceUpdater struct {
	sequences map[string]int64
}

func (s *unitMockSequenceUpdater) HasSequence(name string) bool {
	_, ok := s.sequences[name]
	return ok
}

func (s *unitMockSequenceUpdater) NextSequence(name string, currentMax int64) (int64, error) {
	v := s.sequences[name]
	if v <= currentMax {
		v = currentMax + 1
	}
	s.sequences[name] = v
	return v, nil
}

func (s *unitMockSequenceUpdater) UpdateSequence(name string, rowid int64) {
	s.sequences[name] = rowid
}

// unitTableWithPK implements tableWithColumns and pkGetter.
type unitTableWithPK struct {
	columns []interface{}
	pk      []string
}

func (t *unitTableWithPK) GetColumns() []interface{} { return t.columns }
func (t *unitTableWithPK) GetPrimaryKey() []string   { return t.pk }

// unitTableNoPK implements tableWithColumns but not pkGetter.
type unitTableNoPK struct {
	columns []interface{}
}

func (t *unitTableNoPK) GetColumns() []interface{} { return t.columns }

// unitTableWithCollation implements tableWithColumns and collationGetter.
type unitTableWithCollation struct {
	columns    []interface{}
	collations map[string]string
}

func (t *unitTableWithCollation) GetColumns() []interface{} { return t.columns }
func (t *unitTableWithCollation) GetColumnCollationByName(col string) string {
	return t.collations[col]
}

// unitTableNoCollation implements tableWithColumns but not collationGetter.
type unitTableNoCollation struct {
	columns []interface{}
}

func (t *unitTableNoCollation) GetColumns() []interface{} { return t.columns }

// unitTableWithRecordCols implements recordColGetter.
type unitTableWithRecordCols struct {
	cols []string
}

func (t *unitTableWithRecordCols) GetRecordColumnNames() []string { return t.cols }
func (t *unitTableWithRecordCols) GetColumns() []interface{}      { return nil }

// ---------------------------------------------------------------------------
// TestConstraintsUnit_GetSequenceManager
// ---------------------------------------------------------------------------

// TestConstraintsUnit_GetSequenceManagerNilCtx covers the nil-ctx path.
func TestConstraintsUnit_GetSequenceManagerNilCtx(t *testing.T) {
	v := New()
	v.Ctx = nil
	if v.getSequenceManager() != nil {
		t.Error("expected nil for nil ctx")
	}
}

// TestConstraintsUnit_GetSequenceManagerNilSchema covers the nil-schema path.
func TestConstraintsUnit_GetSequenceManagerNilSchema(t *testing.T) {
	v := New()
	v.Ctx = &VDBEContext{Schema: nil}
	if v.getSequenceManager() != nil {
		t.Error("expected nil for nil schema")
	}
}

// TestConstraintsUnit_GetSequenceManagerNoAccessor covers the schema-not-accessor path.
func TestConstraintsUnit_GetSequenceManagerNoAccessor(t *testing.T) {
	v := New()
	// unitMockSchemaNoSeq does not implement sequenceManagerAccessor
	v.Ctx = &VDBEContext{Schema: &unitMockSchemaNoSeq{}}
	if v.getSequenceManager() != nil {
		t.Error("expected nil when schema does not implement GetSequences")
	}
}

// TestConstraintsUnit_GetSequenceManagerNotUpdater covers the case where
// GetSequences returns something that is not a sequenceUpdater.
func TestConstraintsUnit_GetSequenceManagerNotUpdater(t *testing.T) {
	v := New()
	v.Ctx = &VDBEContext{Schema: &unitMockSchema{sequences: "not-an-updater"}}
	if v.getSequenceManager() != nil {
		t.Error("expected nil when GetSequences does not return sequenceUpdater")
	}
}

// TestConstraintsUnit_GetSequenceManagerSuccess covers the happy path.
func TestConstraintsUnit_GetSequenceManagerSuccess(t *testing.T) {
	v := New()
	sm := &unitMockSequenceUpdater{sequences: map[string]int64{}}
	v.Ctx = &VDBEContext{Schema: &unitMockSchema{sequences: sm}}
	got := v.getSequenceManager()
	if got == nil {
		t.Error("expected non-nil sequenceUpdater")
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_MemToNumber
// ---------------------------------------------------------------------------

// TestConstraintsUnit_MemToNumber_Null covers the nil-null path.
func TestConstraintsUnit_MemToNumber_Null(t *testing.T) {
	m := NewMemNull()
	if memToNumber(m) != nil {
		t.Error("expected nil for NULL mem")
	}
}

// TestConstraintsUnit_MemToNumber_Int covers the integer path.
func TestConstraintsUnit_MemToNumber_Int(t *testing.T) {
	m := NewMemInt(42)
	v := memToNumber(m)
	if v == nil {
		t.Fatal("expected non-nil for int mem")
	}
	if v.(int64) != 42 {
		t.Errorf("expected 42, got %v", v)
	}
}

// TestConstraintsUnit_MemToNumber_Real covers the real path.
func TestConstraintsUnit_MemToNumber_Real(t *testing.T) {
	m := NewMemReal(3.14)
	v := memToNumber(m)
	if v == nil {
		t.Fatal("expected non-nil for real mem")
	}
	if v.(float64) != 3.14 {
		t.Errorf("expected 3.14, got %v", v)
	}
}

// TestConstraintsUnit_MemToNumber_String covers the string path (returns nil).
func TestConstraintsUnit_MemToNumber_String(t *testing.T) {
	m := NewMemStr("hello")
	if memToNumber(m) != nil {
		t.Error("expected nil for string mem")
	}
}

// TestConstraintsUnit_MemToNumber_Blob covers the blob path (returns nil).
func TestConstraintsUnit_MemToNumber_Blob(t *testing.T) {
	m := NewMemBlob([]byte{1, 2, 3})
	if memToNumber(m) != nil {
		t.Error("expected nil for blob mem")
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_ToFloat
// ---------------------------------------------------------------------------

// TestConstraintsUnit_ToFloat_Int64 covers the int64 branch.
func TestConstraintsUnit_ToFloat_Int64(t *testing.T) {
	got := toFloat(int64(7))
	if got != 7.0 {
		t.Errorf("expected 7.0, got %v", got)
	}
}

// TestConstraintsUnit_ToFloat_Float64 covers the float64 branch.
func TestConstraintsUnit_ToFloat_Float64(t *testing.T) {
	got := toFloat(float64(2.5))
	if got != 2.5 {
		t.Errorf("expected 2.5, got %v", got)
	}
}

// TestConstraintsUnit_ToFloat_Unknown covers the default branch (returns 0).
func TestConstraintsUnit_ToFloat_Unknown(t *testing.T) {
	got := toFloat("not-a-number")
	if got != 0.0 {
		t.Errorf("expected 0.0 for unknown type, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_LiteralToNumber
// ---------------------------------------------------------------------------

// TestConstraintsUnit_LiteralToNumber_InvalidInt covers the ParseInt error path.
func TestConstraintsUnit_LiteralToNumber_InvalidInt(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "not-an-int"}
	if literalToNumber(lit) != nil {
		t.Error("expected nil for unparseable integer literal")
	}
}

// TestConstraintsUnit_LiteralToNumber_InvalidFloat covers the ParseFloat error path.
func TestConstraintsUnit_LiteralToNumber_InvalidFloat(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "not-a-float"}
	if literalToNumber(lit) != nil {
		t.Error("expected nil for unparseable float literal")
	}
}

// TestConstraintsUnit_LiteralToNumber_String covers the default (string) path.
func TestConstraintsUnit_LiteralToNumber_String(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralString, Value: "hello"}
	if literalToNumber(lit) != nil {
		t.Error("expected nil for string literal type")
	}
}

// TestConstraintsUnit_LiteralToNumber_ValidInt covers the valid integer path.
func TestConstraintsUnit_LiteralToNumber_ValidInt(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "99"}
	v := literalToNumber(lit)
	if v == nil {
		t.Fatal("expected non-nil for valid integer literal")
	}
	if v.(int64) != 99 {
		t.Errorf("expected 99, got %v", v)
	}
}

// TestConstraintsUnit_LiteralToNumber_ValidFloat covers the valid float path.
func TestConstraintsUnit_LiteralToNumber_ValidFloat(t *testing.T) {
	lit := &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "1.5"}
	v := literalToNumber(lit)
	if v == nil {
		t.Fatal("expected non-nil for valid float literal")
	}
	if v.(float64) != 1.5 {
		t.Errorf("expected 1.5, got %v", v)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_CompareCheckValues
// ---------------------------------------------------------------------------

// TestConstraintsUnit_CompareCheckValues_Default covers the default operator path.
func TestConstraintsUnit_CompareCheckValues_Default(t *testing.T) {
	// Use an operator value that is not one of the handled ones.
	got := compareCheckValues(int64(1), int64(1), parser.BinaryOp(255))
	if !got {
		t.Error("expected true for unhandled operator (default branch)")
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_EvalBinaryCheck_NilOperand
// ---------------------------------------------------------------------------

// TestConstraintsUnit_EvalBinaryCheck_NilLeft exercises the nil-operand guard
// in evalBinaryCheck when the left literal cannot be resolved to a number.
func TestConstraintsUnit_EvalBinaryCheck_NilLeft(t *testing.T) {
	// String literal on left side → literalToNumber returns nil.
	e := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.LiteralExpr{Type: parser.LiteralString, Value: "foo"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
	}
	m := NewMemInt(5)
	// When left resolves to nil, evalBinaryCheck should return true (pass).
	if !evalBinaryCheck(e, m) {
		t.Error("expected true when left operand is nil (NULL in comparison)")
	}
}

// TestConstraintsUnit_EvalBinaryCheck_NilRight exercises nil on the right side.
func TestConstraintsUnit_EvalBinaryCheck_NilRight(t *testing.T) {
	e := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "bar"},
	}
	m := NewMemInt(5)
	if !evalBinaryCheck(e, m) {
		t.Error("expected true when right operand is nil (NULL in comparison)")
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_ResolveCheckOperand_Default
// ---------------------------------------------------------------------------

// TestConstraintsUnit_ResolveCheckOperand_Default covers the default (unknown
// expression type) branch in resolveCheckOperand.
func TestConstraintsUnit_ResolveCheckOperand_Default(t *testing.T) {
	// Use a BinaryExpr as the operand — it is not LiteralExpr, IdentExpr, or
	// UnaryExpr, so resolveCheckOperand should return nil.
	inner := &parser.BinaryExpr{
		Op:    parser.OpGt,
		Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
		Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
	}
	m := NewMemInt(5)
	got := resolveCheckOperand(inner, m)
	if got != nil {
		t.Errorf("expected nil for unknown expression type, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_EvaluateCheckConstraint_ParseError
// ---------------------------------------------------------------------------

// TestConstraintsUnit_EvaluateCheckConstraint_ParseError covers the branch
// where the CHECK expression cannot be parsed (returns true to skip).
// A bare closing parenthesis is not a valid expression start and triggers
// "expected expression, got TK_RP" from parseSpecialExpression.
func TestConstraintsUnit_EvaluateCheckConstraint_ParseError(t *testing.T) {
	m := NewMemInt(1)
	got := evaluateCheckConstraint(")", m)
	if !got {
		t.Error("expected true (pass) when CHECK expression cannot be parsed")
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_SchemaIndexAdapter
// ---------------------------------------------------------------------------

// TestConstraintsUnit_GetTablePrimaryKey_NoPKGetter covers the path where
// the table does not implement pkGetter.
func TestConstraintsUnit_GetTablePrimaryKey_NoPKGetter(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableNoPK{},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	cols, hasPK := adapter.GetTablePrimaryKey("t")
	if hasPK || cols != nil {
		t.Error("expected (nil, false) when table does not implement pkGetter")
	}
}

// TestConstraintsUnit_GetTablePrimaryKey_EmptyPK covers the path where
// GetPrimaryKey returns an empty slice.
func TestConstraintsUnit_GetTablePrimaryKey_EmptyPK(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableWithPK{pk: []string{}},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	cols, hasPK := adapter.GetTablePrimaryKey("t")
	if hasPK {
		t.Error("expected hasPK=false for empty pk slice")
	}
	if len(cols) != 0 {
		t.Errorf("expected empty cols, got %v", cols)
	}
}

// TestConstraintsUnit_GetTablePrimaryKey_TableNotFound covers the path
// where the table is not found.
func TestConstraintsUnit_GetTablePrimaryKey_TableNotFound(t *testing.T) {
	schema := &mockSchema2{tables: map[string]interface{}{}}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	cols, hasPK := adapter.GetTablePrimaryKey("missing")
	if hasPK || cols != nil {
		t.Error("expected (nil, false) when table not found")
	}
}

// TestConstraintsUnit_GetColumnCollation_NoGetter covers the path where
// the table does not implement collationGetter.
func TestConstraintsUnit_GetColumnCollation_NoGetter(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableNoCollation{},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	collation := adapter.GetColumnCollation("t", "a")
	if collation != "" {
		t.Errorf("expected empty collation, got %q", collation)
	}
}

// TestConstraintsUnit_GetColumnCollation_TableNotFound covers the path
// where the table is not found.
func TestConstraintsUnit_GetColumnCollation_TableNotFound(t *testing.T) {
	schema := &mockSchema2{tables: map[string]interface{}{}}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	collation := adapter.GetColumnCollation("missing", "a")
	if collation != "" {
		t.Errorf("expected empty collation for missing table, got %q", collation)
	}
}

// TestConstraintsUnit_GetColumnCollation_WithGetter covers the path where
// the collationGetter is implemented.
func TestConstraintsUnit_GetColumnCollation_WithGetter(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableWithCollation{
				collations: map[string]string{"name": "NOCASE"},
			},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	collation := adapter.GetColumnCollation("t", "name")
	if collation != "NOCASE" {
		t.Errorf("expected NOCASE, got %q", collation)
	}
}

// TestConstraintsUnit_GetRecordColumnIndex_NotFound covers the path where
// the column name is not in the record.
func TestConstraintsUnit_GetRecordColumnIndex_NotFound(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableWithRecordCols{cols: []string{"a", "b"}},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	idx := adapter.GetRecordColumnIndex("t", "c")
	if idx != -1 {
		t.Errorf("expected -1 for missing column, got %d", idx)
	}
}

// TestConstraintsUnit_GetRecordColumnIndex_Found covers the happy path.
func TestConstraintsUnit_GetRecordColumnIndex_Found(t *testing.T) {
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableWithRecordCols{cols: []string{"a", "b", "c"}},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	idx := adapter.GetRecordColumnIndex("t", "B") // case-insensitive
	if idx != 1 {
		t.Errorf("expected 1 for column 'b', got %d", idx)
	}
}

// TestConstraintsUnit_GetRecordColumnIndex_TableNotFound covers the table-
// not-found path.
func TestConstraintsUnit_GetRecordColumnIndex_TableNotFound(t *testing.T) {
	schema := &mockSchema2{tables: map[string]interface{}{}}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	idx := adapter.GetRecordColumnIndex("missing", "a")
	if idx != -1 {
		t.Errorf("expected -1 for missing table, got %d", idx)
	}
}

// TestConstraintsUnit_GetTableIndexes_NoGetter covers the path where the
// schema does not implement indexListGetter (returns nil).
func TestConstraintsUnit_GetTableIndexes_NoGetter(t *testing.T) {
	// mockSchema2 does not implement ListIndexesForTable
	schema := &mockSchema2{tables: map[string]interface{}{}}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	indexes := adapter.GetTableIndexes("t")
	if indexes != nil {
		t.Errorf("expected nil when schema has no ListIndexesForTable, got %v", indexes)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_ValidateNotNullColumns_NonConformingCol
// ---------------------------------------------------------------------------

// TestConstraintsUnit_ValidateNotNullColumns_NonConformingCol covers the
// branch where a column does not implement constraintColumnInfo.
func TestConstraintsUnit_ValidateNotNullColumns_NonConformingCol(t *testing.T) {
	v := New()
	// Column that does not implement constraintColumnInfo — should be skipped.
	columns := []interface{}{"not-a-column-info"}
	payload := makeIntRecord(5) // reuse helper from correlated file
	err := v.validateNotNullColumns(columns, payload)
	if err != nil {
		t.Errorf("expected nil error for non-conforming column, got %v", err)
	}
}

// TestConstraintsUnit_ValidateCheckColumns_NonConformingCol covers the same
// branch in validateCheckColumns.
func TestConstraintsUnit_ValidateCheckColumns_NonConformingCol(t *testing.T) {
	v := New()
	columns := []interface{}{"not-a-column-info"}
	payload := makeIntRecord(5)
	err := v.validateCheckColumns(columns, payload)
	if err != nil {
		t.Errorf("expected nil error for non-conforming column, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_CheckNotNull_NoSchema
// ---------------------------------------------------------------------------

// TestConstraintsUnit_CheckNotNullConstraints_NoSchema covers the !ok branch
// in checkNotNullConstraints when getTableColumns returns false.
func TestConstraintsUnit_CheckNotNullConstraints_NoSchema(t *testing.T) {
	v := New()
	v.Ctx = nil // causes getTableColumns to return false
	err := v.checkNotNullConstraints("t", makeIntRecord(1))
	if err != nil {
		t.Errorf("expected nil error when schema unavailable, got %v", err)
	}
}

// TestConstraintsUnit_CheckCheckConstraints_NoSchema covers the !ok branch
// in checkCheckConstraints when getTableColumns returns false.
func TestConstraintsUnit_CheckCheckConstraints_NoSchema(t *testing.T) {
	v := New()
	v.Ctx = nil
	err := v.checkCheckConstraints("t", makeIntRecord(1), 0)
	if err != nil {
		t.Errorf("expected nil error when schema unavailable, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_CheckColumnNotNull_ParseError
// ---------------------------------------------------------------------------

// invalidVarintPayload is a byte sequence whose varint header cannot be decoded
// (all continuation bits set with no terminator), causing parseRecordColumn to
// return an error for any column index.
var invalidVarintPayload = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

// TestConstraintsUnit_CheckColumnNotNull_ParseError covers the path where
// parseRecordColumn fails with a corrupt payload for a NOT NULL column.
func TestConstraintsUnit_CheckColumnNotNull_ParseError(t *testing.T) {
	v := New()
	col := &mockColumn2{name: "x", notNull: true}
	// A payload with an unterminated varint causes parseRecordColumn to return error.
	err := v.checkColumnNotNull(col, invalidVarintPayload, 0)
	// Expect a NOT NULL constraint error since parse fails.
	if err == nil {
		t.Error("expected NOT NULL constraint error when payload is corrupt")
	}
}

// TestConstraintsUnit_CheckColumnCheck_ParseError covers the skip path where
// parseRecordColumn fails in checkColumnCheck (returns nil, not an error).
func TestConstraintsUnit_CheckColumnCheck_ParseError(t *testing.T) {
	v := New()
	col := &mockColumn2{name: "x", check: "x > 0"}
	// A payload with an unterminated varint causes parseRecordColumn to fail;
	// the constraint is skipped and nil is returned.
	err := v.checkColumnCheck(col, invalidVarintPayload, 0)
	if err != nil {
		t.Errorf("expected nil when parseRecordColumn fails in checkColumnCheck, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_GetTable_SchemaNotTableGetter
// ---------------------------------------------------------------------------

// TestConstraintsUnit_GetTable_SchemaNotTableGetter covers the path where
// the schema stored in adapter.schema does not implement tableGetter.
func TestConstraintsUnit_GetTable_SchemaNotTableGetter(t *testing.T) {
	// Use a bare struct that does not implement GetTableByName.
	adapter := &schemaIndexAdapterImpl{schema: struct{}{}}
	got := adapter.getTable("t")
	if got != nil {
		t.Errorf("expected nil when schema does not implement tableGetter, got %v", got)
	}
}

// TestConstraintsUnit_GetRecordColumnIndex_NoRecordColGetter covers the path
// where the table does not implement recordColGetter.
func TestConstraintsUnit_GetRecordColumnIndex_NoRecordColGetter(t *testing.T) {
	// unitTableNoPK does not implement GetRecordColumnNames.
	schema := &mockSchema2{
		tables: map[string]interface{}{
			"t": &unitTableNoPK{},
		},
	}
	adapter := &schemaIndexAdapterImpl{schema: schema}
	idx := adapter.GetRecordColumnIndex("t", "a")
	if idx != -1 {
		t.Errorf("expected -1 when table has no GetRecordColumnNames, got %d", idx)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_ValidateNotNullColumns_PKColumnSkipped
// ---------------------------------------------------------------------------

// TestConstraintsUnit_ValidateNotNullColumns_PKColumnSkipped exercises the
// IsPrimaryKeyColumn() = true branch, ensuring PK columns are skipped.
func TestConstraintsUnit_ValidateNotNullColumns_PKColumnSkipped(t *testing.T) {
	v := New()
	// A PK column with NOT NULL set; if it were checked it would fail on NULL payload.
	pkCol := &mockColumn2{name: "id", pk: true, notNull: true}
	columns := []interface{}{pkCol}
	err := v.validateNotNullColumns(columns, makeNullRecord())
	if err != nil {
		t.Errorf("expected nil: PK columns should be skipped, got %v", err)
	}
}

// TestConstraintsUnit_ValidateCheckColumns_PKColumnSkipped exercises the
// IsPrimaryKeyColumn() = true branch in validateCheckColumns.
func TestConstraintsUnit_ValidateCheckColumns_PKColumnSkipped(t *testing.T) {
	v := New()
	pkCol := &mockColumn2{name: "id", pk: true, check: "id > 0"}
	columns := []interface{}{pkCol}
	err := v.validateCheckColumns(columns, makeNullRecord())
	if err != nil {
		t.Errorf("expected nil: PK columns skipped in check constraints, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConstraintsUnit_CheckSchemaUniqueConstraints_NilProvider
// ---------------------------------------------------------------------------

// TestConstraintsUnit_CheckSchemaUniqueConstraints_NilProvider covers the
// provider==nil path in checkSchemaUniqueConstraints by using a nil ctx.
func TestConstraintsUnit_CheckSchemaUniqueConstraints_NilProvider(t *testing.T) {
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	cursor := btree.NewCursor(bt, rootPage)

	v := New()
	v.Ctx = nil // causes getSchemaIndexProvider to return nil

	// checkSchemaUniqueConstraints must return nil when provider is nil.
	if err := v.checkSchemaUniqueConstraints("t", makeIntRecord(1), cursor, 1); err != nil {
		t.Errorf("expected nil when provider is nil, got %v", err)
	}
}
