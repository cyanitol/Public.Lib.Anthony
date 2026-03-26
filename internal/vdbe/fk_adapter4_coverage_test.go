// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// Minimal SQLite record encoder (avoids dependency on unexported encodeSimpleRecord).
// ---------------------------------------------------------------------------

// fk4EncodeVarint encodes a uint64 as a SQLite varint.
func fk4EncodeVarint(v uint64) []byte {
	if v <= 0x7f {
		return []byte{byte(v)}
	}
	if v <= 0x3fff {
		return []byte{byte(v>>7) | 0x80, byte(v & 0x7f)}
	}
	// For values up to 2^21-1 (sufficient for record headers in tests)
	return []byte{byte(v>>14) | 0x80, byte((v>>7)&0x7f) | 0x80, byte(v & 0x7f)}
}

// fk4EncodeRecord encodes a slice of interface{} values into a SQLite record.
// Supported types: nil (NULL), int64, float64, string.
func fk4EncodeRecord(vals []interface{}) []byte {
	types := make([][]byte, len(vals))
	data := make([][]byte, len(vals))

	for i, v := range vals {
		switch vv := v.(type) {
		case nil:
			types[i] = fk4EncodeVarint(0)
			data[i] = nil
		case int64:
			types[i] = fk4EncodeVarint(6)
			b := make([]byte, 8)
			u := uint64(vv)
			b[0], b[1], b[2], b[3] = byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32)
			b[4], b[5], b[6], b[7] = byte(u>>24), byte(u>>16), byte(u>>8), byte(u)
			data[i] = b
		case float64:
			// SQLite type code 7 = 8-byte IEEE 754 float
			types[i] = fk4EncodeVarint(7)
			b := make([]byte, 8)
			bits := math.Float64bits(vv)
			b[0], b[1], b[2], b[3] = byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32)
			b[4], b[5], b[6], b[7] = byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits)
			data[i] = b
		case string:
			rawBytes := []byte(vv)
			typeCode := uint64(len(rawBytes))*2 + 13
			types[i] = fk4EncodeVarint(typeCode)
			data[i] = rawBytes
		}
	}

	// Compute header length: 1 byte for the header-size varint + sum of type varint lengths.
	typeLen := 0
	for _, t := range types {
		typeLen += len(t)
	}
	// Header size varint (header-size includes itself).
	headerSizeVal := uint64(1 + typeLen)
	headerSizeBytes := fk4EncodeVarint(headerSizeVal)
	// Recompute if header size varint is longer than 1 byte.
	headerSizeVal = uint64(len(headerSizeBytes) + typeLen)
	headerSizeBytes = fk4EncodeVarint(headerSizeVal)

	var out []byte
	out = append(out, headerSizeBytes...)
	for _, t := range types {
		out = append(out, t...)
	}
	for _, d := range data {
		out = append(out, d...)
	}
	return out
}

// ---------------------------------------------------------------------------
// Mock schema types (in vdbe_test; mirror the shape expected by getTable).
// ---------------------------------------------------------------------------

// fk4MockColumn has GetName, GetType, GetCollation, IsPrimaryKeyColumn.
type fk4MockColumn struct {
	Name      string
	ColType   string
	Collation string
	IsPK      bool
}

func (c *fk4MockColumn) GetName() string          { return c.Name }
func (c *fk4MockColumn) GetType() string          { return c.ColType }
func (c *fk4MockColumn) GetCollation() string     { return c.Collation }
func (c *fk4MockColumn) IsPrimaryKeyColumn() bool { return c.IsPK }

// fk4MockColumnNameOnly has only GetName — exercises buildMinimalColumnInfo path.
type fk4MockColumnNameOnly struct {
	Name string
}

func (c *fk4MockColumnNameOnly) GetName() string { return c.Name }

// fk4MockTable holds RootPage and WithoutRowID as exported fields (read via reflection).
type fk4MockTable struct {
	RootPage     uint32
	WithoutRowID bool
	cols         []interface{}
}

func (t *fk4MockTable) GetColumns() []interface{} { return t.cols }

// fk4MockSchema implements GetTableByName.
type fk4MockSchema struct {
	tables map[string]*fk4MockTable
}

func (s *fk4MockSchema) GetTableByName(name string) (interface{}, bool) {
	t, ok := s.tables[name]
	return t, ok
}

// ---------------------------------------------------------------------------
// Helper: build a VDBE backed by a real in-memory btree + mock schema.
// ---------------------------------------------------------------------------

func fk4NewVDBE(t *testing.T, schema *fk4MockSchema) (*vdbe.VDBE, *btree.Btree) {
	t.Helper()
	bt := btree.NewBtree(4096)
	v := &vdbe.VDBE{
		Ctx: &vdbe.VDBEContext{
			Btree:  bt,
			Schema: schema,
		},
		Cursors: make([]*vdbe.Cursor, 10),
	}
	return v, bt
}

// fk4InsertRow inserts a row into the btree at the given rootPage using the provided rowid and payload.
func fk4InsertRow(t *testing.T, bt *btree.Btree, rootPage uint32, rowid int64, payload []byte) {
	t.Helper()
	cur := btree.NewCursorWithOptions(bt, rootPage, false)
	if err := cur.Insert(rowid, payload); err != nil {
		t.Fatalf("fk4InsertRow rowid=%d: %v", rowid, err)
	}
}

// ---------------------------------------------------------------------------
// TestFK4RowExists_HappyPath
// Exercises RowExists → findMatchingRow → scanForMatch (match found).
// Also exercises valuesEqual (non-null path) via checkRowMatch.
// ---------------------------------------------------------------------------

func TestFK4RowExists_HappyPath(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Table: id INTEGER PRIMARY KEY, name TEXT
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "name", ColType: "TEXT"},
		},
	}
	schema.tables["users"] = tbl

	// Insert rowid=1 with payload for name="alice"
	payload := fk4EncodeRecord([]interface{}{"alice"})
	fk4InsertRow(t, bt, rootPage, 1, payload)

	payload2 := fk4EncodeRecord([]interface{}{"bob"})
	fk4InsertRow(t, bt, rootPage, 2, payload2)

	rr := vdbe.NewVDBERowReader(v)

	// RowExists: id=1 should exist.
	exists, err := rr.RowExists("users", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("RowExists(id=1): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(id=1) = true")
	}

	// RowExists: id=99 should not exist.
	exists, err = rr.RowExists("users", []string{"id"}, []interface{}{int64(99)})
	if err != nil {
		t.Fatalf("RowExists(id=99): %v", err)
	}
	if exists {
		t.Error("expected RowExists(id=99) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4RowExists_TextColumn
// Exercises RowExists → scanForMatch → valuesEqual for TEXT column (non-PK).
// Also covers the compareMemToString path inside checkRowMatch.
// ---------------------------------------------------------------------------

func TestFK4RowExists_TextColumn(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Table: id INTEGER PRIMARY KEY, code TEXT (text is the FK target)
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "code", ColType: "TEXT"},
		},
	}
	schema.tables["codes"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"X"}))
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{"Y"}))

	rr := vdbe.NewVDBERowReader(v)

	// Search by text column value.
	exists, err := rr.RowExists("codes", []string{"code"}, []interface{}{"X"})
	if err != nil {
		t.Fatalf("RowExists(code=X): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(code=X) = true")
	}

	// Non-existent text value.
	exists, err = rr.RowExists("codes", []string{"code"}, []interface{}{"Z"})
	if err != nil {
		t.Fatalf("RowExists(code=Z): %v", err)
	}
	if exists {
		t.Error("expected RowExists(code=Z) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4RowExists_NullValue
// Exercises valuesEqual when Mem is NULL (null comparison path).
// A NULL in the searched column should not match a non-null query value.
// ---------------------------------------------------------------------------

func TestFK4RowExists_NullValue(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "tag", ColType: "TEXT"},
		},
	}
	schema.tables["tags"] = tbl

	// Insert row with NULL tag.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{nil}))

	rr := vdbe.NewVDBERowReader(v)

	// Searching for tag="hello" should not match the NULL row.
	exists, err := rr.RowExists("tags", []string{"tag"}, []interface{}{"hello"})
	if err != nil {
		t.Fatalf("RowExists: %v", err)
	}
	if exists {
		t.Error("expected RowExists(tag=hello) = false when row has NULL")
	}
}

// ---------------------------------------------------------------------------
// TestFK4FindReferencingRows_Basic
// Exercises FindReferencingRows → collectMatchingRowids → collectAllMatchingRowids.
// ---------------------------------------------------------------------------

func TestFK4FindReferencingRows_Basic(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Child table: id INTEGER PRIMARY KEY, pid INTEGER (FK column, stored in payload)
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "pid", ColType: "INTEGER"},
		},
	}
	schema.tables["child"] = tbl

	// Insert rows: (rowid=1, pid=10), (rowid=2, pid=10), (rowid=3, pid=20)
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{int64(10)}))
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{int64(10)}))
	fk4InsertRow(t, bt, rootPage, 3, fk4EncodeRecord([]interface{}{int64(20)}))

	rr := vdbe.NewVDBERowReader(v)

	// Find rows where pid=10 — should return rowids [1, 2].
	rowids, err := rr.FindReferencingRows("child", []string{"pid"}, []interface{}{int64(10)})
	if err != nil {
		t.Fatalf("FindReferencingRows(pid=10): %v", err)
	}
	if len(rowids) != 2 {
		t.Errorf("expected 2 rowids for pid=10, got %d: %v", len(rowids), rowids)
	}

	// Find rows where pid=20 — should return rowid [3].
	rowids, err = rr.FindReferencingRows("child", []string{"pid"}, []interface{}{int64(20)})
	if err != nil {
		t.Fatalf("FindReferencingRows(pid=20): %v", err)
	}
	if len(rowids) != 1 {
		t.Errorf("expected 1 rowid for pid=20, got %d", len(rowids))
	}

	// Find rows where pid=99 — should return empty slice.
	rowids, err = rr.FindReferencingRows("child", []string{"pid"}, []interface{}{int64(99)})
	if err != nil {
		t.Fatalf("FindReferencingRows(pid=99): %v", err)
	}
	if len(rowids) != 0 {
		t.Errorf("expected 0 rowids for pid=99, got %d", len(rowids))
	}
}

// ---------------------------------------------------------------------------
// TestFK4FindReferencingRows_EmptyTable
// Exercises collectAllMatchingRowids when the table is empty.
// ---------------------------------------------------------------------------

func TestFK4FindReferencingRows_EmptyTable(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "pid", ColType: "INTEGER"},
		},
	}
	schema.tables["empty_child"] = tbl

	rr := vdbe.NewVDBERowReader(v)

	rowids, err := rr.FindReferencingRows("empty_child", []string{"pid"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("FindReferencingRows on empty table: %v", err)
	}
	if len(rowids) != 0 {
		t.Errorf("expected 0 rowids for empty table, got %d", len(rowids))
	}
}

// ---------------------------------------------------------------------------
// TestFK4ReadRowByKey_Regular
// Exercises ReadRowByKey → seekByKeyValues (regular table, single rowid) →
// readRowValuesFromCursor.
// ---------------------------------------------------------------------------

func TestFK4ReadRowByKey_Regular(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "val", ColType: "TEXT"},
		},
	}
	schema.tables["items"] = tbl

	fk4InsertRow(t, bt, rootPage, 5, fk4EncodeRecord([]interface{}{"hello"}))
	fk4InsertRow(t, bt, rootPage, 7, fk4EncodeRecord([]interface{}{"world"}))

	rr := vdbe.NewVDBERowReader(v)

	row, err := rr.ReadRowByKey("items", []interface{}{int64(5)})
	if err != nil {
		t.Fatalf("ReadRowByKey(5): %v", err)
	}
	if row["id"] != int64(5) {
		t.Errorf("expected id=5, got %v", row["id"])
	}
	if row["val"] != "hello" {
		t.Errorf("expected val=hello, got %v", row["val"])
	}

	row, err = rr.ReadRowByKey("items", []interface{}{int64(7)})
	if err != nil {
		t.Fatalf("ReadRowByKey(7): %v", err)
	}
	if row["val"] != "world" {
		t.Errorf("expected val=world, got %v", row["val"])
	}
}

// ---------------------------------------------------------------------------
// TestFK4ReadRowByKey_NotFound
// Exercises seekByKeyValues error path when the row doesn't exist.
// ---------------------------------------------------------------------------

func TestFK4ReadRowByKey_NotFound(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["sparse"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{}))

	rr := vdbe.NewVDBERowReader(v)

	_, err = rr.ReadRowByKey("sparse", []interface{}{int64(999)})
	if err == nil {
		t.Error("expected error for missing rowid 999")
	}
}

// ---------------------------------------------------------------------------
// TestFK4ReadRowByKey_BadKeyType
// Exercises seekByKeyValues error path when keyValues[0] is not int64 for
// a regular (rowid) table.
// ---------------------------------------------------------------------------

func TestFK4ReadRowByKey_BadKeyType(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["typed"] = tbl

	rr := vdbe.NewVDBERowReader(v)

	// Pass a string where int64 is expected.
	_, err = rr.ReadRowByKey("typed", []interface{}{"not-an-int64"})
	if err == nil {
		t.Error("expected error when rowid is not int64")
	}
}

// ---------------------------------------------------------------------------
// TestFK4ReadRowByKey_TooManyKeys
// Exercises seekByKeyValues when len(keyValues) != 1 for regular table.
// ---------------------------------------------------------------------------

func TestFK4ReadRowByKey_TooManyKeys(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["single"] = tbl

	rr := vdbe.NewVDBERowReader(v)

	// Two keys for a regular table should fail.
	_, err = rr.ReadRowByKey("single", []interface{}{int64(1), int64(2)})
	if err == nil {
		t.Error("expected error for too many key values on regular table")
	}
}

// ---------------------------------------------------------------------------
// TestFK4BuildMinimalColumnInfo
// Exercises buildMinimalColumnInfo by using a column object that only
// implements GetName() (no GetType or IsPrimaryKeyColumn).
// RowExists is called to trigger the code path.
// ---------------------------------------------------------------------------

func TestFK4BuildMinimalColumnInfo(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Use fk4MockColumnNameOnly — only GetName(), no GetType/IsPK.
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumnNameOnly{Name: "myval"},
		},
	}
	schema.tables["minimal"] = tbl

	// Insert a row: single TEXT column value "foo" (no INTEGER PK alias).
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"foo"}))

	rr := vdbe.NewVDBERowReader(v)

	// RowExists with the minimal-column table — forces buildMinimalColumnInfo path.
	exists, err := rr.RowExists("minimal", []string{"myval"}, []interface{}{"foo"})
	if err != nil {
		t.Fatalf("RowExists on minimal-column table: %v", err)
	}
	if !exists {
		t.Error("expected RowExists(myval=foo) = true")
	}

	// Non-matching value.
	exists, err = rr.RowExists("minimal", []string{"myval"}, []interface{}{"bar"})
	if err != nil {
		t.Fatalf("RowExists(myval=bar): %v", err)
	}
	if exists {
		t.Error("expected RowExists(myval=bar) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4UpdateRowByKey_Regular
// Exercises UpdateRowByKey → seekByKey (rowid path) → readAndMergeRowByKey →
// replaceRow (regular table path). Also exercises extractPrimaryKeyValues fallback.
// ---------------------------------------------------------------------------

func TestFK4UpdateRowByKey_Regular(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "val", ColType: "TEXT"},
		},
	}
	schema.tables["mutable"] = tbl

	fk4InsertRow(t, bt, rootPage, 3, fk4EncodeRecord([]interface{}{"original"}))

	mod := vdbe.NewVDBERowModifier(v)

	// Update val for rowid=3.
	err = mod.UpdateRowByKey("mutable", []interface{}{int64(3)}, map[string]interface{}{"val": "updated"})
	if err != nil {
		t.Fatalf("UpdateRowByKey: %v", err)
	}

	// Verify via ReadRowByKey.
	rr := vdbe.NewVDBERowReader(v)
	row, err := rr.ReadRowByKey("mutable", []interface{}{int64(3)})
	if err != nil {
		t.Fatalf("ReadRowByKey after update: %v", err)
	}
	if row["val"] != "updated" {
		t.Errorf("expected val=updated after UpdateRowByKey, got %v", row["val"])
	}
}

// ---------------------------------------------------------------------------
// TestFK4UpdateRowByKey_NotFound
// Exercises UpdateRowByKey when the key does not exist — seekByKey returns false.
// ---------------------------------------------------------------------------

func TestFK4UpdateRowByKey_NotFound(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "val", ColType: "TEXT"},
		},
	}
	schema.tables["ghost"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"x"}))

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.UpdateRowByKey("ghost", []interface{}{int64(999)}, map[string]interface{}{"val": "new"})
	if err == nil {
		t.Error("expected error for UpdateRowByKey on non-existent row")
	}
}

// ---------------------------------------------------------------------------
// TestFK4DeleteRowByKey_Regular
// Exercises DeleteRowByKey (rowid/regular-table path).
// ---------------------------------------------------------------------------

func TestFK4DeleteRowByKey_Regular(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "name", ColType: "TEXT"},
		},
	}
	schema.tables["delme"] = tbl

	fk4InsertRow(t, bt, rootPage, 10, fk4EncodeRecord([]interface{}{"to_delete"}))
	fk4InsertRow(t, bt, rootPage, 20, fk4EncodeRecord([]interface{}{"keep"}))

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.DeleteRowByKey("delme", []interface{}{int64(10)})
	if err != nil {
		t.Fatalf("DeleteRowByKey(10): %v", err)
	}

	// Row 10 should be gone; row 20 should remain.
	rr := vdbe.NewVDBERowReader(v)
	_, err = rr.ReadRowByKey("delme", []interface{}{int64(10)})
	if err == nil {
		t.Error("expected error reading deleted row 10")
	}
	row20, err := rr.ReadRowByKey("delme", []interface{}{int64(20)})
	if err != nil {
		t.Fatalf("ReadRowByKey(20) after delete of 10: %v", err)
	}
	if row20["name"] != "keep" {
		t.Errorf("expected name=keep for row 20, got %v", row20["name"])
	}
}

// ---------------------------------------------------------------------------
// TestFK4DeleteRowByKey_NotFound
// Exercises DeleteRowByKey when key doesn't exist (not-found error path).
// ---------------------------------------------------------------------------

func TestFK4DeleteRowByKey_NotFound(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["nodels"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{}))

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.DeleteRowByKey("nodels", []interface{}{int64(999)})
	if err == nil {
		t.Error("expected error for DeleteRowByKey on non-existent row")
	}
}

// ---------------------------------------------------------------------------
// TestFK4DeleteRowByKey_BadKeyType
// Exercises DeleteRowByKey error when rowid is not int64 for regular table.
// ---------------------------------------------------------------------------

func TestFK4DeleteRowByKey_BadKeyType(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["badkey"] = tbl

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.DeleteRowByKey("badkey", []interface{}{"not-int64"})
	if err == nil {
		t.Error("expected error for non-int64 rowid in DeleteRowByKey")
	}
}

// ---------------------------------------------------------------------------
// TestFK4DeleteRowByKey_TooManyKeys
// Exercises DeleteRowByKey error when len(keyValues) != 1 for regular table.
// ---------------------------------------------------------------------------

func TestFK4DeleteRowByKey_TooManyKeys(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["multikey"] = tbl

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.DeleteRowByKey("multikey", []interface{}{int64(1), int64(2)})
	if err == nil {
		t.Error("expected error for too many key values in DeleteRowByKey")
	}
}

// ---------------------------------------------------------------------------
// TestFK4ApplyNumericAffinity
// Exercises applyNumericAffinity via RowExists using a NUMERIC-affinity column.
// The column type "NUMERIC" routes through applyNumericAffinity (not INT/TEXT/REAL).
// ---------------------------------------------------------------------------

func TestFK4ApplyNumericAffinity(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// NUMERIC affinity column (not INT, not CHAR/CLOB/TEXT, not REAL/FLOA/DOUB)
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "score", ColType: "NUMERIC"},
		},
	}
	schema.tables["scores"] = tbl

	// Insert rowid=1 with score stored as int64(42).
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{int64(42)}))

	// Insert rowid=2 with score stored as TEXT "3.14".
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{"3.14"}))

	rr := vdbe.NewVDBERowReader(v)

	// Test 1: search for score=42 (int64). applyNumericAffinity(int64(42)) = int64(42).
	exists, err := rr.RowExists("scores", []string{"score"}, []interface{}{int64(42)})
	if err != nil {
		t.Fatalf("RowExists(score=42): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(score=42) = true")
	}

	// Test 2: search for score="99" (string). applyNumericAffinity("99") → float64(99) or int64(99).
	exists, err = rr.RowExists("scores", []string{"score"}, []interface{}{"99"})
	if err != nil {
		t.Fatalf("RowExists(score=99string): %v", err)
	}
	if exists {
		t.Error("expected RowExists(score='99') = false (no row has score 99)")
	}

	// Test 3: search for score="notanumber" (non-parseable string).
	// applyNumericAffinity("notanumber") returns "notanumber" (passthrough).
	exists, err = rr.RowExists("scores", []string{"score"}, []interface{}{"notanumber"})
	if err != nil {
		t.Fatalf("RowExists(score=notanumber): %v", err)
	}
	if exists {
		t.Error("expected RowExists(score=notanumber) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4CompareMemToFloat64_IntPath
// Exercises compareMemToFloat64 mem.IsInt() branch.
// Column type is empty so compareMemToInterface is used (no affinity conversion).
// Stored value is int64 (SQLite type 6); search value is float64.
// compareMemToFloat64Handler sees float64 value, compareMemToFloat64 then checks
// mem.IsReal()=false (stored as int) → mem.IsInt()=true → float64(mem.IntValue()) == v.
// ---------------------------------------------------------------------------

func TestFK4CompareMemToFloat64_IntPath(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Empty-type column so compareMemToInterface path is used (not affinity path).
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "measure", ColType: ""},
		},
	}
	schema.tables["measures"] = tbl

	// Store int64(5) — mem will have IsInt()=true, IsReal()=false.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{int64(5)}))

	rr := vdbe.NewVDBERowReader(v)

	// Search for float64(5.0) — compareMemToFloat64: IsReal()=false, IsInt()=true → match.
	exists, err := rr.RowExists("measures", []string{"measure"}, []interface{}{float64(5.0)})
	if err != nil {
		t.Fatalf("RowExists(measure=5.0): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(measure=5.0) = true when stored as int64(5)")
	}

	// Non-matching float against int-stored value.
	exists, err = rr.RowExists("measures", []string{"measure"}, []interface{}{float64(6.0)})
	if err != nil {
		t.Fatalf("RowExists(measure=6.0): %v", err)
	}
	if exists {
		t.Error("expected RowExists(measure=6.0) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4ValuesEqual_NullVsNil
// Exercises valuesEqual (line 938) — null Mem vs nil interface (should be equal).
// Triggered via RowExists with an empty-string column type (no affinity),
// which calls compareMemToInterface → the NULL path of valuesEqual.
// ---------------------------------------------------------------------------

func TestFK4ValuesEqual_NullVsNil(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// No-type column forces valuesEqual path (compareMemToInterface).
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "opt", ColType: ""},
		},
	}
	schema.tables["opts"] = tbl

	// Row with NULL opt.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{nil}))

	rr := vdbe.NewVDBERowReader(v)

	// RowExists with nil value should match the NULL row.
	exists, err := rr.RowExists("opts", []string{"opt"}, []interface{}{nil})
	if err != nil {
		t.Fatalf("RowExists(opt=nil): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(opt=nil) = true for NULL row")
	}
}

// ---------------------------------------------------------------------------
// TestFK4CompareMemToBlob
// Exercises compareMemToBlob — not directly reachable via typical paths since
// SQLite stores BLOBs distinctly. We exercise it by inserting a row with a
// text value and searching with a string, verifying the blob handler falls back.
// The indirect route: compareMemToInterface tries compareMemToBlob which
// returns (false, false) when the value is not []byte — increasing branch coverage.
// We also exercise it via FindReferencingRows with an int64 search on an
// empty-type column (all handlers cycle through including compareMemToBlob).
// ---------------------------------------------------------------------------

func TestFK4CompareMemToBlob_FallbackPath(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// No-type column: compareMemToInterface cycles through all handlers.
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "data", ColType: ""},
		},
	}
	schema.tables["blobs"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{int64(77)}))

	rr := vdbe.NewVDBERowReader(v)

	// Search via FindReferencingRows with int64 value — compareMemToInt64 matches,
	// but we cycle through compareMemToBlob with a non-[]byte value (int64).
	rowids, err := rr.FindReferencingRows("blobs", []string{"data"}, []interface{}{int64(77)})
	if err != nil {
		t.Fatalf("FindReferencingRows: %v", err)
	}
	if len(rowids) != 1 {
		t.Errorf("expected 1 rowid, got %d", len(rowids))
	}
}

// ---------------------------------------------------------------------------
// TestFK4UpdateRowByKey_SeekByKey_RegularBadKeyCount
// Exercises seekByKey error when regular table receives != 1 key.
// ---------------------------------------------------------------------------

func TestFK4UpdateRowByKey_SeekByKey_RegularBadKeyCount(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "v", ColType: "TEXT"},
		},
	}
	schema.tables["sk_bad"] = tbl

	mod := vdbe.NewVDBERowModifier(v)

	// Two keys → seekByKey returns error "regular tables expect single rowid value"
	err = mod.UpdateRowByKey("sk_bad", []interface{}{int64(1), int64(2)}, map[string]interface{}{})
	if err == nil {
		t.Error("expected error for two key values on regular table in UpdateRowByKey")
	}
}

// ---------------------------------------------------------------------------
// TestFK4UpdateRowByKey_SeekByKey_BadKeyType
// Exercises seekByKey error when key is not int64 for a regular table.
// ---------------------------------------------------------------------------

func TestFK4UpdateRowByKey_SeekByKey_BadKeyType(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
		},
	}
	schema.tables["sk_type"] = tbl

	mod := vdbe.NewVDBERowModifier(v)

	err = mod.UpdateRowByKey("sk_type", []interface{}{"not-int64"}, map[string]interface{}{})
	if err == nil {
		t.Error("expected error for non-int64 key in UpdateRowByKey")
	}
}

// ---------------------------------------------------------------------------
// TestFK4RowExists_MultipleColumns
// Exercises scanForMatch with multiple columns — covers multi-column iteration
// in checkRowMatch. Also exercises findMatchingRow / scanForMatch.
// ---------------------------------------------------------------------------

func TestFK4RowExists_MultipleColumns(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Table with two non-PK columns to check.
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "a", ColType: "TEXT"},
			&fk4MockColumn{Name: "b", ColType: "TEXT"},
		},
	}
	schema.tables["multi"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"x", "y"}))
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{"x", "z"}))

	rr := vdbe.NewVDBERowReader(v)

	// Both columns match row 1 exactly.
	exists, err := rr.RowExists("multi", []string{"a", "b"}, []interface{}{"x", "y"})
	if err != nil {
		t.Fatalf("RowExists(a=x,b=y): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(a=x,b=y) = true")
	}

	// Only 'a' matches but 'b' doesn't — no row should match.
	exists, err = rr.RowExists("multi", []string{"a", "b"}, []interface{}{"x", "w"})
	if err != nil {
		t.Fatalf("RowExists(a=x,b=w): %v", err)
	}
	if exists {
		t.Error("expected RowExists(a=x,b=w) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4FindReferencingRows_TextColumn
// Exercises collectAllMatchingRowids with TEXT FK column matching.
// Covers compareMemToString via the checkRowMatch path.
// ---------------------------------------------------------------------------

func TestFK4FindReferencingRows_TextColumn(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "pcode", ColType: "TEXT"},
		},
	}
	schema.tables["textchild"] = tbl

	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"A"}))
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{"A"}))
	fk4InsertRow(t, bt, rootPage, 3, fk4EncodeRecord([]interface{}{"B"}))

	rr := vdbe.NewVDBERowReader(v)

	rowids, err := rr.FindReferencingRows("textchild", []string{"pcode"}, []interface{}{"A"})
	if err != nil {
		t.Fatalf("FindReferencingRows(pcode=A): %v", err)
	}
	if len(rowids) != 2 {
		t.Errorf("expected 2 rowids for pcode=A, got %d", len(rowids))
	}
}

// ---------------------------------------------------------------------------
// fk4EncodeCompositeKey encodes values as a composite key matching fk_adapter.go's
// encodeCompositeKey (same byte encoding, replicated here for test use).
// ---------------------------------------------------------------------------

func fk4EncodeCompositeKey(vals []interface{}) []byte {
	var buf []byte
	for _, v := range vals {
		switch val := v.(type) {
		case nil:
			buf = append(buf, 0x00)
		case int64:
			buf = append(buf, 0x10)
			u := uint64(val) ^ (1 << 63)
			buf = append(buf,
				byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
				byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
		case string:
			buf = append(buf, 0x30)
			buf = append(buf, []byte(val)...)
			buf = append(buf, 0x00)
		case []byte:
			buf = append(buf, 0x40)
			buf = append(buf, val...)
			buf = append(buf, 0x00)
		default:
			buf = append(buf, 0x50)
			buf = append(buf, []byte(fmt.Sprintf("%v", val))...)
			buf = append(buf, 0x00)
		}
	}
	return buf
}

// fk4InsertWithoutRowIDRow inserts a WITHOUT ROWID row using composite key + full payload.
func fk4InsertWithoutRowIDRow(t *testing.T, bt *btree.Btree, rootPage uint32, keyVals []interface{}, allCols []interface{}) {
	t.Helper()
	cur := btree.NewCursorWithOptions(bt, rootPage, true)
	keyBytes := fk4EncodeCompositeKey(keyVals)
	payload := fk4EncodeRecord(allCols)
	if err := cur.InsertWithComposite(0, keyBytes, payload); err != nil {
		t.Fatalf("fk4InsertWithoutRowIDRow: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestFK4WithoutRowID_UpdateRowByKey
// Exercises UpdateRowByKey → seekByKey (WITHOUT ROWID path, SeekComposite) →
// readAndMergeRowByKey → replaceRowWithoutRowID → extractPrimaryKeyValues.
// ---------------------------------------------------------------------------

func TestFK4WithoutRowID_UpdateRowByKey(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	// WITHOUT ROWID table: code TEXT PK, name TEXT
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: true,
		cols: []interface{}{
			&fk4MockColumn{Name: "code", ColType: "TEXT", IsPK: true},
			&fk4MockColumn{Name: "name", ColType: "TEXT"},
		},
	}
	schema.tables["writable_wr"] = tbl

	// Insert WITHOUT ROWID row: key=("X"), allCols=("X", "original")
	fk4InsertWithoutRowIDRow(t, bt, rootPage, []interface{}{"X"}, []interface{}{"X", "original"})

	mod := vdbe.NewVDBERowModifier(v)

	// Update the row: change name but keep code.
	err = mod.UpdateRowByKey("writable_wr", []interface{}{"X"}, map[string]interface{}{"name": "updated"})
	if err != nil {
		t.Fatalf("UpdateRowByKey (WITHOUT ROWID): %v", err)
	}

	// Verify the row was updated via ReadRowByKey.
	rr := vdbe.NewVDBERowReader(v)
	row, err := rr.ReadRowByKey("writable_wr", []interface{}{"X"})
	if err != nil {
		t.Fatalf("ReadRowByKey after WITHOUT ROWID update: %v", err)
	}
	if row["name"] != "updated" {
		t.Errorf("expected name=updated, got %v", row["name"])
	}
}

// ---------------------------------------------------------------------------
// TestFK4WithoutRowID_ReadRowByKey
// Exercises ReadRowByKey → seekByKeyValues (WITHOUT ROWID path, SeekComposite).
// ---------------------------------------------------------------------------

func TestFK4WithoutRowID_ReadRowByKey(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	// WITHOUT ROWID table: id INTEGER PK, val TEXT
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: true,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "val", ColType: "TEXT"},
		},
	}
	schema.tables["without_rowid"] = tbl

	// Insert row with composite key (int64(7)) and payload (7, "hello").
	fk4InsertWithoutRowIDRow(t, bt, rootPage, []interface{}{int64(7)}, []interface{}{int64(7), "hello"})

	rr := vdbe.NewVDBERowReader(v)

	row, err := rr.ReadRowByKey("without_rowid", []interface{}{int64(7)})
	if err != nil {
		t.Fatalf("ReadRowByKey (WITHOUT ROWID, key=7): %v", err)
	}
	if row["val"] != "hello" {
		t.Errorf("expected val=hello, got %v", row["val"])
	}
}

// ---------------------------------------------------------------------------
// TestFK4WithoutRowID_ReadRowByKey_NotFound
// Exercises seekByKeyValues NOT FOUND path for WITHOUT ROWID table.
// ---------------------------------------------------------------------------

func TestFK4WithoutRowID_ReadRowByKey_NotFound(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: true,
		cols: []interface{}{
			&fk4MockColumn{Name: "code", ColType: "TEXT", IsPK: true},
			&fk4MockColumn{Name: "desc", ColType: "TEXT"},
		},
	}
	schema.tables["wr_miss"] = tbl

	fk4InsertWithoutRowIDRow(t, bt, rootPage, []interface{}{"A"}, []interface{}{"A", "desc_a"})

	rr := vdbe.NewVDBERowReader(v)

	_, err = rr.ReadRowByKey("wr_miss", []interface{}{"ZZZ"})
	if err == nil {
		t.Error("expected error for missing composite key ZZZ")
	}
}

// ---------------------------------------------------------------------------
// TestFK4CompareMemToBlob_Handled
// Exercises compareMemToBlob when value IS a []byte (handled=true path).
// We store a row with a TEXT column value and search using a []byte value —
// this triggers compareMemToBlob, which returns (false, true) because the
// stored mem is a string (not blob), so mem.IsBlob() is false.
// This covers the function body beyond the type assertion.
// ---------------------------------------------------------------------------

func TestFK4CompareMemToBlob_Handled(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Empty column type → compareMemToInterface path, cycles through all handlers.
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "data", ColType: ""},
		},
	}
	schema.tables["blob_test"] = tbl

	// Store a string value in the column.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"sometext"}))

	rr := vdbe.NewVDBERowReader(v)

	// Search with a []byte value — triggers compareMemToBlob with handled=true.
	// mem.IsBlob() = false (it's a string), so match = false.
	exists, err := rr.RowExists("blob_test", []string{"data"}, []interface{}{[]byte("sometext")})
	if err != nil {
		t.Fatalf("RowExists with []byte value: %v", err)
	}
	// The stored value is TEXT, not BLOB, so even same bytes won't match via compareMemToBlob.
	if exists {
		t.Error("expected RowExists with []byte = false (TEXT != BLOB)")
	}
}

// ---------------------------------------------------------------------------
// TestFK4CompareMemToFloat64_RealPath
// Exercises compareMemToFloat64 mem.IsReal() branch.
// Stores an actual float64 value (SQLite type 7) in the btree row.
// compareMemToInterface → compareMemToFloat64Handler → compareMemToFloat64 with mem.IsReal().
// ---------------------------------------------------------------------------

func TestFK4CompareMemToFloat64_RealPath(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Empty-type column so compareMemToInterface is used (not valuesEqualWithAffinity).
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "measure", ColType: ""},
		},
	}
	schema.tables["real_tbl"] = tbl

	// Store float64(2.5) using SQLite type code 7 in the payload.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{float64(2.5)}))
	fk4InsertRow(t, bt, rootPage, 2, fk4EncodeRecord([]interface{}{float64(9.9)}))

	rr := vdbe.NewVDBERowReader(v)

	// Matching: mem.IsReal() = true, mem.RealValue() == 2.5 → true.
	exists, err := rr.RowExists("real_tbl", []string{"measure"}, []interface{}{float64(2.5)})
	if err != nil {
		t.Fatalf("RowExists(measure=2.5): %v", err)
	}
	if !exists {
		t.Error("expected RowExists(measure=2.5) = true")
	}

	// Non-matching: mem.IsReal() = true but values differ → false.
	exists, err = rr.RowExists("real_tbl", []string{"measure"}, []interface{}{float64(3.14)})
	if err != nil {
		t.Fatalf("RowExists(measure=3.14): %v", err)
	}
	if exists {
		t.Error("expected RowExists(measure=3.14) = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4CompareMemToFloat64_ReturnFalse
// Exercises compareMemToFloat64 return false branch.
// Stores a string in the column; searching with float64 causes compareMemToFloat64
// to be called with mem.IsReal()=false and mem.IsInt()=false, returning false.
// ---------------------------------------------------------------------------

func TestFK4CompareMemToFloat64_ReturnFalse(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Empty-type column so compareMemToInterface dispatches to handlers.
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: false,
		cols: []interface{}{
			&fk4MockColumn{Name: "id", ColType: "INTEGER", IsPK: true},
			&fk4MockColumn{Name: "txt", ColType: ""},
		},
	}
	schema.tables["str_float"] = tbl

	// Store a string value — mem will be a string, not real/int.
	fk4InsertRow(t, bt, rootPage, 1, fk4EncodeRecord([]interface{}{"hello"}))

	rr := vdbe.NewVDBERowReader(v)

	// Search with float64 — compareMemToFloat64Handler sees value=float64(1.0),
	// but mem is a string so IsReal()=false, IsInt()=false → return false.
	exists, err := rr.RowExists("str_float", []string{"txt"}, []interface{}{float64(1.0)})
	if err != nil {
		t.Fatalf("RowExists(txt=1.0 float): %v", err)
	}
	if exists {
		t.Error("expected RowExists with float64 against string column = false")
	}
}

// ---------------------------------------------------------------------------
// TestFK4ExtractPrimaryKeyValues_NoPKIndices
// Exercises the extractPrimaryKeyValues fallback path when PKColumnIndices is empty.
// This occurs when a table has no columns with IsPK=true.
// UpdateRowByKey on a table with no explicitly marked PK columns will trigger this.
// ---------------------------------------------------------------------------

func TestFK4ExtractPrimaryKeyValues_NoPKIndices(t *testing.T) {
	t.Parallel()

	schema := &fk4MockSchema{tables: map[string]*fk4MockTable{}}
	v, bt := fk4NewVDBE(t, schema)

	rootPage, err := bt.CreateWithoutRowidTable()
	if err != nil {
		t.Fatalf("CreateWithoutRowidTable: %v", err)
	}

	// WITHOUT ROWID table with no PK columns — PKColumnIndices will be empty,
	// triggering the fallback in extractPrimaryKeyValues (returns all values).
	tbl := &fk4MockTable{
		RootPage:     rootPage,
		WithoutRowID: true,
		cols: []interface{}{
			&fk4MockColumn{Name: "a", ColType: "TEXT", IsPK: false},
			&fk4MockColumn{Name: "b", ColType: "TEXT", IsPK: false},
		},
	}
	schema.tables["nopk_wr"] = tbl

	// Insert with composite key ("alpha") and full payload.
	fk4InsertWithoutRowIDRow(t, bt, rootPage, []interface{}{"alpha"}, []interface{}{"alpha", "beta"})

	mod := vdbe.NewVDBERowModifier(v)

	// UpdateRowByKey — this will exercise extractPrimaryKeyValues fallback (empty PKColumnIndices).
	err = mod.UpdateRowByKey("nopk_wr", []interface{}{"alpha"}, map[string]interface{}{"b": "gamma"})
	if err != nil {
		t.Fatalf("UpdateRowByKey on nopk WITHOUT ROWID: %v", err)
	}
}
