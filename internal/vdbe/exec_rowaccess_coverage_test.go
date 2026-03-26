// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newTableBtree creates a fresh in-memory btree table and returns it together
// with its root page.
func newTableBtree(t *testing.T) (*btree.Btree, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return bt, root
}

// insertRow inserts a single row (payload) with the given rowid into a btree.
func insertRow(t *testing.T, bt *btree.Btree, root uint32, rowid int64, payload []byte) {
	t.Helper()
	c := btree.NewCursor(bt, root)
	if err := c.Insert(rowid, payload); err != nil {
		t.Fatalf("Insert rowid %d: %v", rowid, err)
	}
}

// nullRecord returns a minimal SQLite record with one NULL column.
// Header: [2, 0]  (header-size=2, serial-type=0)  Body: (empty)
func nullRecord() []byte { return []byte{0x02, 0x00} }

// int8Record returns a minimal SQLite record containing one 1-byte integer.
// Serial type 1 => 1-byte signed int.
func int8Record(v int8) []byte { return []byte{0x02, 0x01, byte(v)} }

// textRecord returns a minimal record with one TEXT column.
func textRecord(s string) []byte {
	n := len(s)
	st := uint64(n*2 + 13) // text serial type
	hdr := []byte{0x00, 0x00}
	hdr[1] = byte(st)
	hdr[0] = 2 // header size = 2
	return append(hdr, []byte(s)...)
}

// blobRecord returns a minimal record with one BLOB column.
func blobRecord(b []byte) []byte {
	n := len(b)
	st := uint64(n*2 + 12) // blob serial type
	hdr := []byte{0x02, byte(st)}
	return append(hdr, b...)
}

// newVdbeWithTable returns a VDBE and a writable cursor 0 over a btree table.
func newVdbeWithTable(t *testing.T) (*VDBE, *btree.Btree, uint32) {
	t.Helper()
	bt, root := newTableBtree(t)
	v := New()
	v.Ctx = &VDBEContext{Btree: bt}
	v.AllocMemory(10)
	v.AllocCursors(2)
	btc := btree.NewCursor(bt, root)
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		Writable:    true,
		RootPage:    root,
		BtreeCursor: btc,
	}
	return v, bt, root
}

// ---------------------------------------------------------------------------
// repositionToRowid
// ---------------------------------------------------------------------------

func TestRepositionToRowid(t *testing.T) {
	t.Parallel()

	t.Run("FoundRowid", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		insertRow(t, bt, root, 1, nullRecord())
		insertRow(t, bt, root, 2, nullRecord())
		c := btree.NewCursor(bt, root)
		if err := repositionToRowid(c, 2); err != nil {
			t.Fatalf("expected success, got %v", err)
		}
	})

	t.Run("MissingRowid", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		insertRow(t, bt, root, 1, nullRecord())
		c := btree.NewCursor(bt, root)
		err := repositionToRowid(c, 99)
		if err == nil {
			t.Fatal("expected error for missing rowid")
		}
	})

	t.Run("EmptyTree", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		c := btree.NewCursor(bt, root)
		err := repositionToRowid(c, 1)
		if err == nil {
			t.Fatal("expected error for empty tree")
		}
	})
}

// ---------------------------------------------------------------------------
// getDeferredSeekRowid  (exercises the P3==0 branch → index cursor path)
// ---------------------------------------------------------------------------

func TestGetDeferredSeekRowid(t *testing.T) {
	t.Parallel()

	t.Run("FromRegister", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[1].SetInt(42)
		tc := &Cursor{CurType: CursorBTree, IsTable: true}
		instr := &Instruction{P1: 0, P2: 0, P3: 1}
		rowid, err := v.getDeferredSeekRowid(instr, tc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rowid != 42 {
			t.Fatalf("want 42, got %d", rowid)
		}
	})

	t.Run("FromIndexCursorNilBtreeCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		// cursor 0 has a non-IndexCursor (plain BtCursor) → falls through to EOF branch
		bt, root := newTableBtree(t)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			IsTable:     true,
			BtreeCursor: btree.NewCursor(bt, root),
		}
		tc := &Cursor{CurType: CursorBTree, IsTable: true}
		instr := &Instruction{P1: 0, P2: 0, P3: 0}
		rowid, err := v.getDeferredSeekRowid(instr, tc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rowid != 0 {
			t.Fatalf("want 0, got %d", rowid)
		}
		if !tc.EOF {
			t.Fatal("want tc.EOF=true when cursor type mismatch")
		}
	})
}

// ---------------------------------------------------------------------------
// performDeferredSeek
// ---------------------------------------------------------------------------

func TestPerformDeferredSeek(t *testing.T) {
	t.Parallel()

	t.Run("NilBtreeCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		tc := &Cursor{CurType: CursorBTree, IsTable: true, BtreeCursor: nil}
		if err := v.performDeferredSeek(tc, 1); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !tc.EOF {
			t.Fatal("want EOF=true when no btree cursor")
		}
	})

	t.Run("RowidFound", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		insertRow(t, bt, root, 5, nullRecord())
		v := New()
		v.Ctx = &VDBEContext{Btree: bt}
		tc := &Cursor{
			CurType:     CursorBTree,
			IsTable:     true,
			BtreeCursor: btree.NewCursor(bt, root),
		}
		if err := v.performDeferredSeek(tc, 5); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tc.EOF {
			t.Fatal("want EOF=false when rowid found")
		}
	})

	t.Run("RowidNotFound", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		insertRow(t, bt, root, 1, nullRecord())
		v := New()
		v.Ctx = &VDBEContext{Btree: bt}
		tc := &Cursor{
			CurType:     CursorBTree,
			IsTable:     true,
			BtreeCursor: btree.NewCursor(bt, root),
		}
		if err := v.performDeferredSeek(tc, 99); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !tc.EOF {
			t.Fatal("want EOF=true when rowid not found")
		}
	})
}

// ---------------------------------------------------------------------------
// getBtreeCursorPayload
// ---------------------------------------------------------------------------

func TestGetBtreeCursorPayloadRowAccess(t *testing.T) {
	t.Parallel()

	t.Run("IndexCursorWithKey", func(t *testing.T) {
		t.Parallel()
		bt, idxC, _ := setupIndexCursor(t)
		_ = bt
		if err := idxC.InsertIndex([]byte("hello"), 7); err != nil {
			t.Fatalf("InsertIndex: %v", err)
		}
		if _, err := idxC.SeekIndex([]byte("hello")); err != nil {
			t.Fatalf("SeekIndex: %v", err)
		}
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: false, BtreeCursor: idxC}
		dst := NewMem()
		payload := v.getBtreeCursorPayload(cursor, dst)
		if payload == nil {
			t.Fatal("expected non-nil payload for index cursor")
		}
	})

	t.Run("NilBtreeCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: true, BtreeCursor: nil}
		dst := NewMem()
		payload := v.getBtreeCursorPayload(cursor, dst)
		if payload != nil {
			t.Fatal("expected nil payload for nil cursor")
		}
		if !dst.IsNull() {
			t.Fatal("expected dst to be NULL")
		}
	})

	t.Run("BtCursorWithPayload", func(t *testing.T) {
		t.Parallel()
		bt, root := newTableBtree(t)
		rec := nullRecord()
		insertRow(t, bt, root, 3, rec)
		c := btree.NewCursor(bt, root)
		if err := c.MoveToFirst(); err != nil {
			t.Fatalf("MoveToFirst: %v", err)
		}
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: true, BtreeCursor: c}
		dst := NewMem()
		payload := v.getBtreeCursorPayload(cursor, dst)
		if payload == nil {
			t.Fatal("expected non-nil payload from table cursor")
		}
	})
}

// ---------------------------------------------------------------------------
// parseColumnIntoMem
// ---------------------------------------------------------------------------

func TestParseColumnIntoMemRowAccess(t *testing.T) {
	t.Parallel()

	type tc struct {
		name     string
		payload  []byte
		col      int
		wantNull bool
		wantErr  bool
	}
	cases := []tc{
		{"NullColumn", nullRecord(), 0, true, false},
		{"IntColumn", int8Record(77), 0, false, false},
		{"TextColumn", textRecord("hi"), 0, false, false},
		{"BlobColumn", blobRecord([]byte{0x01, 0x02}), 0, false, false},
		{"OutOfRange", nullRecord(), 5, true, false},
		{"InvalidPayload", []byte{0xFF}, 0, true, true},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			dst := NewMem()
			err := v.parseColumnIntoMem(c.payload, c.col, dst, nil)
			if c.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !c.wantErr && c.wantNull && !dst.IsNull() {
				t.Fatal("expected NULL mem")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseRecordColumnHeader
// ---------------------------------------------------------------------------

func TestParseRecordColumnHeader(t *testing.T) {
	t.Parallel()

	t.Run("EmptyData", func(t *testing.T) {
		t.Parallel()
		dst := NewMem()
		_, _, err := parseRecordColumnHeader([]byte{}, dst)
		if err == nil {
			t.Fatal("expected error for empty data")
		}
	})

	t.Run("ValidNullRecord", func(t *testing.T) {
		t.Parallel()
		dst := NewMem()
		sts, bodyOff, err := parseRecordColumnHeader(nullRecord(), dst)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(sts) != 1 || sts[0] != 0 {
			t.Fatalf("expected [0] serial types, got %v", sts)
		}
		if bodyOff < 1 {
			t.Fatalf("body offset should be >= 1, got %d", bodyOff)
		}
	})

	t.Run("ValidIntRecord", func(t *testing.T) {
		t.Parallel()
		dst := NewMem()
		sts, _, err := parseRecordColumnHeader(int8Record(5), dst)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(sts) != 1 || sts[0] != 1 {
			t.Fatalf("expected serial type 1, got %v", sts)
		}
	})
}

// ---------------------------------------------------------------------------
// wrapInsertError
// ---------------------------------------------------------------------------

func TestWrapInsertError(t *testing.T) {
	t.Parallel()

	t.Run("DuplicateKey", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		err := v.wrapInsertError(errors.New("btree: duplicate key violation"))
		if err == nil {
			t.Fatal("expected non-nil error")
		}
		if err.Error() != "UNIQUE constraint failed: PRIMARY KEY must be unique" {
			t.Fatalf("unexpected message: %s", err.Error())
		}
	})

	t.Run("OtherError", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		err := v.wrapInsertError(errors.New("disk full"))
		if err == nil {
			t.Fatal("expected non-nil error")
		}
		if err.Error() == "UNIQUE constraint failed: PRIMARY KEY must be unique" {
			t.Fatal("should not return unique constraint message for generic error")
		}
	})
}

// ---------------------------------------------------------------------------
// extractValuesFromPayload / extractValuesFromPayloadWithRowid
// ---------------------------------------------------------------------------

// mockSchemaForExtract implements the minimum interfaces needed by
// extractValuesFromPayload without pulling in the real schema package.
type mockSchemaForExtract struct {
	tables map[string]*mockTableForExtract
}

func (m *mockSchemaForExtract) GetTableByName(name string) (interface{}, bool) {
	tbl, ok := m.tables[name]
	return tbl, ok
}

type mockTableForExtract struct {
	cols []interface{}
}

func (m *mockTableForExtract) GetColumns() []interface{} { return m.cols }

type mockColForExtract struct {
	name  string
	isPK  bool
	ctype string
}

func (c *mockColForExtract) GetName() string          { return c.name }
func (c *mockColForExtract) IsPrimaryKeyColumn() bool { return c.isPK }
func (c *mockColForExtract) GetType() string          { return c.ctype }

func TestExtractValuesFromPayload(t *testing.T) {
	t.Parallel()

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		_, err := v.extractValuesFromPayload("t", nullRecord())
		if err == nil {
			t.Fatal("expected error when no schema")
		}
	})

	t.Run("SchemaTableNotFound", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Ctx = &VDBEContext{
			Schema: &mockSchemaForExtract{tables: map[string]*mockTableForExtract{}},
		}
		_, err := v.extractValuesFromPayload("missing", nullRecord())
		if err == nil {
			t.Fatal("expected error for missing table")
		}
	})

	t.Run("ValidPayloadWithTextCol", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Ctx = &VDBEContext{
			Schema: &mockSchemaForExtract{
				tables: map[string]*mockTableForExtract{
					"t": {cols: []interface{}{
						&mockColForExtract{name: "name", isPK: false, ctype: "TEXT"},
					}},
				},
			},
		}
		payload := textRecord("world")
		vals, err := v.extractValuesFromPayload("t", payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vals == nil {
			t.Fatal("expected non-nil map")
		}
	})
}

func TestExtractValuesFromPayloadWithRowid(t *testing.T) {
	t.Parallel()

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		_, err := v.extractValuesFromPayloadWithRowid("t", nullRecord(), 1)
		if err == nil {
			t.Fatal("expected error when no schema")
		}
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Ctx = &VDBEContext{
			Schema: &mockSchemaForExtract{
				tables: map[string]*mockTableForExtract{
					"t": {cols: []interface{}{
						&mockColForExtract{name: "id", isPK: true, ctype: "INTEGER"},
						&mockColForExtract{name: "val", isPK: false, ctype: "TEXT"},
					}},
				},
			},
		}
		// payload has only one non-PK column (text)
		payload := textRecord("hello")
		vals, err := v.extractValuesFromPayloadWithRowid("t", payload, 42)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if vals == nil {
			t.Fatal("expected non-nil map")
		}
	})
}

// ---------------------------------------------------------------------------
// seekAndDeleteIndexEntry
// ---------------------------------------------------------------------------

func newIndexBtree(t *testing.T) (*btree.Btree, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	pageNum, err := createIndexPage(bt)
	if err != nil {
		t.Fatalf("createIndexPage: %v", err)
	}
	return bt, pageNum
}

func TestSeekAndDeleteIndexEntry(t *testing.T) {
	t.Parallel()

	t.Run("KeyNotFound", func(t *testing.T) {
		t.Parallel()
		bt, root := newIndexBtree(t)
		idxC := btree.NewIndexCursor(bt, root)
		// Insert a different key so the tree is non-empty.
		if err := idxC.InsertIndex([]byte("other"), 1); err != nil {
			t.Fatalf("InsertIndex: %v", err)
		}
		// Seek for a key that doesn't exist — should return nil (not an error).
		err := seekAndDeleteIndexEntry(idxC, []byte("missing"))
		if err != nil {
			t.Fatalf("expected nil for missing key, got %v", err)
		}
	})

	t.Run("KeyFound_Delete", func(t *testing.T) {
		t.Parallel()
		bt, root := newIndexBtree(t)
		idxC := btree.NewIndexCursor(bt, root)
		if err := idxC.InsertIndex([]byte("alpha"), 10); err != nil {
			t.Fatalf("InsertIndex: %v", err)
		}
		if err := seekAndDeleteIndexEntry(idxC, []byte("alpha")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// After deletion the key should no longer be found.
		found, err := idxC.SeekIndex([]byte("alpha"))
		if err != nil {
			t.Fatalf("SeekIndex after delete: %v", err)
		}
		if found {
			t.Fatal("expected key to be gone after deletion")
		}
	})
}

// ---------------------------------------------------------------------------
// extractIndexRowid
// ---------------------------------------------------------------------------

func TestExtractIndexRowid(t *testing.T) {
	t.Parallel()

	t.Run("EOFCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: false, EOF: true}
		dst := NewMem()
		v.extractIndexRowid(cursor, dst)
		if !dst.IsNull() {
			t.Fatal("expected NULL for EOF cursor")
		}
	})

	t.Run("NullRowCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: false, NullRow: true}
		dst := NewMem()
		v.extractIndexRowid(cursor, dst)
		if !dst.IsNull() {
			t.Fatal("expected NULL for NullRow cursor")
		}
	})

	t.Run("NonIndexCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		bt, root := newTableBtree(t)
		// store a plain BtCursor so the type assertion to *IndexCursor fails
		cursor := &Cursor{
			CurType:     CursorBTree,
			IsTable:     true,
			BtreeCursor: btree.NewCursor(bt, root),
		}
		dst := NewMem()
		v.extractIndexRowid(cursor, dst)
		if !dst.IsNull() {
			t.Fatal("expected NULL when BtreeCursor is not an IndexCursor")
		}
	})

	t.Run("ValidIndexCursor", func(t *testing.T) {
		t.Parallel()
		bt, root := newIndexBtree(t)
		idxC := btree.NewIndexCursor(bt, root)
		if err := idxC.InsertIndex([]byte("key"), 99); err != nil {
			t.Fatalf("InsertIndex: %v", err)
		}
		if _, err := idxC.SeekIndex([]byte("key")); err != nil {
			t.Fatalf("SeekIndex: %v", err)
		}
		v := NewTestVDBE(5)
		cursor := &Cursor{CurType: CursorBTree, IsTable: false, BtreeCursor: idxC}
		dst := NewMem()
		v.extractIndexRowid(cursor, dst)
		if dst.IsNull() {
			t.Fatal("expected non-NULL rowid")
		}
		if dst.IntValue() != 99 {
			t.Fatalf("expected rowid 99, got %d", dst.IntValue())
		}
	})
}

// ---------------------------------------------------------------------------
// deleteRowWithDuplicateValue  (covers scan loop and no-match exit)
// ---------------------------------------------------------------------------

func TestDeleteRowWithDuplicateValue(t *testing.T) {
	t.Parallel()

	t.Run("NoDuplicate", func(t *testing.T) {
		t.Parallel()
		v, bt, root := newVdbeWithTable(t)
		insertRow(t, bt, root, 1, int8Record(10))
		insertRow(t, bt, root, 2, int8Record(20))
		btc := btree.NewCursor(bt, root)
		cursor := v.Cursors[0]
		newVal := NewMem()
		newVal.SetInt(99) // value not in tree
		err := v.deleteRowWithDuplicateValue(cursor, btc, 0, newVal, 3, "t", "BINARY")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("EmptyTree", func(t *testing.T) {
		t.Parallel()
		v, bt, root := newVdbeWithTable(t)
		btc := btree.NewCursor(bt, root)
		cursor := v.Cursors[0]
		newVal := NewMem()
		newVal.SetInt(5)
		err := v.deleteRowWithDuplicateValue(cursor, btc, 0, newVal, 1, "t", "BINARY")
		if err != nil {
			t.Fatalf("unexpected error for empty tree: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// findMultiColConflictRowid  (covers nil-newValues path)
// ---------------------------------------------------------------------------

// mockSchemaIndexProvider implements schemaIndexProvider with no real indexes.
type mockSchemaIndexProvider struct{}

func (m *mockSchemaIndexProvider) GetTableIndexes(string) []uniqueIndexInfo   { return nil }
func (m *mockSchemaIndexProvider) GetTablePrimaryKey(string) ([]string, bool) { return nil, false }
func (m *mockSchemaIndexProvider) GetRecordColumnIndex(string, string) int    { return -1 }
func (m *mockSchemaIndexProvider) GetColumnCollation(string, string) string   { return "BINARY" }

func TestFindMultiColConflictRowid(t *testing.T) {
	t.Parallel()

	t.Run("NilNewValues", func(t *testing.T) {
		// When GetRecordColumnIndex returns -1, parseNewIndexValues returns nil,
		// so findMultiColConflictRowid returns (0, false) immediately.
		t.Parallel()
		v, bt, root := newVdbeWithTable(t)
		insertRow(t, bt, root, 1, nullRecord())
		btc := btree.NewCursor(bt, root)
		provider := &mockSchemaIndexProvider{}
		rowid, found := v.findMultiColConflictRowid(provider, "t", []string{"col"}, nullRecord(), btc, 2)
		if found {
			t.Fatalf("expected not found, got rowid=%d", rowid)
		}
	})
}

// ---------------------------------------------------------------------------
// Verify the mock satisfies the interface (compile-time check)
// ---------------------------------------------------------------------------

var _ fmt.Stringer = (*mockColForExtract)(nil) // keep import used

func (c *mockColForExtract) String() string { return c.name }
