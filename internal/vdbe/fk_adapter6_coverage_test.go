// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// fk6MakeVDBEWithTable creates a VDBE backed by a real btree, creates a table,
// inserts a row, and returns the VDBE, btree, rootPage, and the tableInfo.
func fk6MakeVDBEWithTable(t *testing.T, cols []columnInfo, rowid int64, payload []byte) (*VDBE, uint32) {
	t.Helper()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("fk6MakeVDBEWithTable CreateTable: %v", err)
	}
	if payload != nil {
		cur := btree.NewCursorWithOptions(bt, root, false)
		if err := cur.Insert(rowid, payload); err != nil {
			t.Fatalf("fk6MakeVDBEWithTable Insert rowid=%d: %v", rowid, err)
		}
	}

	tbl := &fkaCovMockTable{
		RootPage:     root,
		WithoutRowID: false,
		columns:      make([]interface{}, len(cols)),
	}
	for i, c := range cols {
		tbl.columns[i] = &fkaCovMockColumn{
			name:    c.Name,
			colType: c.Type,
			isPK:    c.Name == "id",
		}
	}
	schema := &fkaCovSchema{
		tables: map[string]interface{}{"items": tbl},
	}
	v := New()
	v.Ctx = &VDBEContext{
		Btree:  bt,
		Schema: schema,
	}
	if err := v.AllocCursors(10); err != nil {
		t.Fatalf("fk6MakeVDBEWithTable AllocCursors: %v", err)
	}
	return v, root
}

// fk6TextPayload encodes a single text value as a minimal SQLite record.
func fk6TextPayload(s string) []byte {
	b := []byte(s)
	serialType := uint64(len(b)*2 + 13)
	headerSize := byte(2)
	payload := append([]byte{headerSize, byte(serialType)}, b...)
	return payload
}

// ---------------------------------------------------------------------------
// TestFKAdapter6FindMatchingRow
// Exercises findMatchingRow (line 541).
// ---------------------------------------------------------------------------

func TestFKAdapter6FindMatchingRow(t *testing.T) {
	t.Parallel()

	t.Run("BadCursor_ReturnsError", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{}
		if err := v.AllocCursors(5); err != nil {
			t.Fatalf("AllocCursors: %v", err)
		}
		// Install a cursor with a wrong BtreeCursor type so getBTreeCursor fails.
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: "not-a-btcursor",
		}
		r := &VDBERowReader{vdbe: v}
		tbl := makeTableInfo([]columnInfo{{Name: "id", Type: "INTEGER"}})
		_, err := r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{int64(1)})
		if err == nil {
			t.Error("expected error for invalid cursor type")
		}
	})

	t.Run("EmptyTable_ReturnsFalse", func(t *testing.T) {
		t.Parallel()
		// Empty table: moveToFirstRow returns isEmpty=true → false, nil
		v, _ := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			0, nil, // no rows inserted
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		// Open a real cursor on the empty table.
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, tbl.RootPage, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		found, err := r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{int64(99)})
		if err != nil {
			t.Fatalf("findMatchingRow on empty table: %v", err)
		}
		if found {
			t.Error("expected false for empty table")
		}
	})

	t.Run("MatchFound_ReturnsTrue", func(t *testing.T) {
		t.Parallel()
		// Insert one row with val="hello"
		payload := fk6TextPayload("hello")
		v, root := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			1, payload,
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, root, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		// Match by rowid (id column is integer PK — rowid value is stored in Mem.i)
		found, err := r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{int64(1)})
		if err != nil {
			t.Fatalf("findMatchingRow: %v", err)
		}
		if !found {
			t.Error("expected true when row with id=1 exists")
		}
	})

	t.Run("NoMatch_ReturnsFalse", func(t *testing.T) {
		t.Parallel()
		payload := fk6TextPayload("world")
		v, root := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			2, payload,
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, root, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		// Search for id=99 which does not exist.
		found, err := r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{int64(99)})
		if err != nil {
			t.Fatalf("findMatchingRow: %v", err)
		}
		if found {
			t.Error("expected false when id=99 does not exist")
		}
	})
}

// ---------------------------------------------------------------------------
// TestFKAdapter6CollectMatchingRowData
// Exercises collectMatchingRowData (line 645).
// ---------------------------------------------------------------------------

func TestFKAdapter6CollectMatchingRowData(t *testing.T) {
	t.Parallel()

	t.Run("BadCursor_ReturnsError", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{}
		if err := v.AllocCursors(5); err != nil {
			t.Fatalf("AllocCursors: %v", err)
		}
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: struct{}{}, // wrong type
		}
		r := &VDBERowReader{vdbe: v}
		tbl := makeTableInfo([]columnInfo{{Name: "id", Type: "INTEGER"}})
		_, err := r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{int64(1)})
		if err == nil {
			t.Error("expected error for invalid cursor type")
		}
	})

	t.Run("EmptyTable_ReturnsEmptySlice", func(t *testing.T) {
		t.Parallel()
		v, _ := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			0, nil,
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, tbl.RootPage, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		rows, err := r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{int64(1)})
		if err != nil {
			t.Fatalf("collectMatchingRowData on empty table: %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("expected empty slice for empty table, got %d rows", len(rows))
		}
	})

	t.Run("MatchFound_ReturnsRows", func(t *testing.T) {
		t.Parallel()
		payload := fk6TextPayload("alice")
		v, root := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			5, payload,
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, root, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		rows, err := r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{int64(5)})
		if err != nil {
			t.Fatalf("collectMatchingRowData: %v", err)
		}
		if len(rows) != 1 {
			t.Errorf("expected 1 matching row, got %d", len(rows))
		}
		if rows[0]["id"] != int64(5) {
			t.Errorf("expected id=5, got %v", rows[0]["id"])
		}
	})

	t.Run("NoMatch_ReturnsEmptySlice", func(t *testing.T) {
		t.Parallel()
		payload := fk6TextPayload("bob")
		v, root := fk6MakeVDBEWithTable(t,
			[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
			3, payload,
		)
		r := &VDBERowReader{vdbe: v}
		tbl, err := r.getTable("items")
		if err != nil {
			t.Fatalf("getTable: %v", err)
		}
		bt := v.Ctx.Btree.(*btree.Btree)
		cur := btree.NewCursorWithOptions(bt, root, false)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: cur,
		}
		rows, err := r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{int64(999)})
		if err != nil {
			t.Fatalf("collectMatchingRowData no match: %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("expected 0 rows for no match, got %d", len(rows))
		}
	})
}

// ---------------------------------------------------------------------------
// TestFKAdapter6DeleteRow
// Exercises VDBERowModifier.DeleteRow (line 1238).
// ---------------------------------------------------------------------------

func TestFKAdapter6DeleteRow(t *testing.T) {
	t.Parallel()

	t.Run("TableNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		schema := &fkaCovSchema{tables: map[string]interface{}{}}
		v := New()
		v.Ctx = &VDBEContext{Schema: schema}
		m := NewVDBERowModifier(v)
		err := m.DeleteRow("missing", 1)
		if err == nil {
			t.Error("expected error for missing table")
		}
	})

	t.Run("WithoutRowID_ReturnsError", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		root, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		schema := &fkaCovSchema{
			tables: map[string]interface{}{
				"wr": &fkaCovMockTable{
					RootPage:     root,
					WithoutRowID: true,
					columns: []interface{}{
						&fkaCovMockColumn{name: "pk", colType: "TEXT", isPK: true},
					},
				},
			},
		}
		v := New()
		v.Ctx = &VDBEContext{Btree: bt, Schema: schema}
		if err := v.AllocCursors(10); err != nil {
			t.Fatalf("AllocCursors: %v", err)
		}
		m := NewVDBERowModifier(v)
		err = m.DeleteRow("wr", 1)
		if err == nil {
			t.Error("expected error for WITHOUT ROWID table")
		}
	})

	t.Run("RowNotFound_ReturnsError", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		root, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		// Empty table; rowid 42 does not exist.
		schema := &fkaCovSchema{
			tables: map[string]interface{}{
				"items": &fkaCovMockTable{
					RootPage:     root,
					WithoutRowID: false,
					columns: []interface{}{
						&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
					},
				},
			},
		}
		v := New()
		v.Ctx = &VDBEContext{Btree: bt, Schema: schema}
		if err := v.AllocCursors(10); err != nil {
			t.Fatalf("AllocCursors: %v", err)
		}
		m := NewVDBERowModifier(v)
		err = m.DeleteRow("items", 42)
		if err == nil {
			t.Error("expected error for rowid not found in empty table")
		}
	})

	t.Run("HappyPath_DeletesRow", func(t *testing.T) {
		t.Parallel()
		bt := btree.NewBtree(4096)
		root, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		// Insert rowid=7 with a small payload.
		payload := fk6TextPayload("to-delete")
		cur := btree.NewCursorWithOptions(bt, root, false)
		if err := cur.Insert(7, payload); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		schema := &fkaCovSchema{
			tables: map[string]interface{}{
				"items": &fkaCovMockTable{
					RootPage:     root,
					WithoutRowID: false,
					columns: []interface{}{
						&fkaCovMockColumn{name: "id", colType: "INTEGER", isPK: true},
						&fkaCovMockColumn{name: "val", colType: "TEXT"},
					},
				},
			},
		}
		v := New()
		v.Ctx = &VDBEContext{Btree: bt, Schema: schema}
		if err := v.AllocCursors(10); err != nil {
			t.Fatalf("AllocCursors: %v", err)
		}
		m := NewVDBERowModifier(v)
		if err := m.DeleteRow("items", 7); err != nil {
			t.Fatalf("DeleteRow: %v", err)
		}
		// Verify the row is gone by checking the rowid cannot be sought.
		verifyCur := btree.NewCursorWithOptions(bt, root, false)
		found, err := verifyCur.SeekRowid(7)
		if err != nil {
			t.Fatalf("SeekRowid after delete: %v", err)
		}
		if found {
			t.Error("expected rowid 7 to be deleted")
		}
	})
}

// ---------------------------------------------------------------------------
// TestFKAdapter6GetTriggerCompiler
// Exercises getTriggerCompiler (line 102 in exec_trigger.go).
// ---------------------------------------------------------------------------

// fk6MockTriggerCompiler is a minimal TriggerCompilerInterface implementation.
type fk6MockTriggerCompiler struct{}

func (c *fk6MockTriggerCompiler) ExecuteTriggers(_ string, _ int, _ int, _ *TriggerRowData, _ []string) error {
	return nil
}

// fk6BadTriggerCompiler does NOT implement TriggerCompilerInterface.
type fk6BadTriggerCompiler struct{}

func TestFKAdapter6GetTriggerCompiler(t *testing.T) {
	t.Parallel()

	t.Run("NilCtx_ReturnsNil", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = nil
		result := v.getTriggerCompiler()
		if result != nil {
			t.Errorf("expected nil for nil Ctx, got %v", result)
		}
	})

	t.Run("NilTriggerCompiler_ReturnsNil", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{TriggerCompiler: nil}
		result := v.getTriggerCompiler()
		if result != nil {
			t.Errorf("expected nil when TriggerCompiler is nil, got %v", result)
		}
	})

	t.Run("ValidCompiler_ReturnsCompiler", func(t *testing.T) {
		t.Parallel()
		compiler := &fk6MockTriggerCompiler{}
		v := New()
		v.Ctx = &VDBEContext{TriggerCompiler: compiler}
		result := v.getTriggerCompiler()
		if result == nil {
			t.Error("expected non-nil TriggerCompilerInterface")
		}
	})

	t.Run("TypeAssertionFailure_ReturnsNil", func(t *testing.T) {
		t.Parallel()
		// fk6BadTriggerCompiler does not implement TriggerCompilerInterface.
		v := New()
		v.Ctx = &VDBEContext{TriggerCompiler: &fk6BadTriggerCompiler{}}
		result := v.getTriggerCompiler()
		if result != nil {
			t.Errorf("expected nil for non-implementing type, got %v", result)
		}
	})
}

// ---------------------------------------------------------------------------
// TestFKAdapter6WriteAndRecordSpill
// Exercises writeAndRecordSpill (line 159 in sorter_spill.go).
// ---------------------------------------------------------------------------

func TestFKAdapter6WriteAndRecordSpill(t *testing.T) {
	t.Parallel()

	t.Run("InvalidPath_ReturnsError", func(t *testing.T) {
		t.Parallel()
		s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 1, nil)
		// Use an invalid path (directory that doesn't exist).
		err := s.writeAndRecordSpill("/nonexistent_dir_abc123/spill.tmp", 0)
		if err == nil {
			t.Error("expected error for invalid spill file path")
		}
	})

	t.Run("EmptyRows_WritesSpilledRun", func(t *testing.T) {
		t.Parallel()
		s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 1, nil)
		// s.Rows is empty; writeAndRecordSpill writes 0 rows.
		dir := t.TempDir()
		filePath := filepath.Join(dir, "spill_empty.tmp")
		err := s.writeAndRecordSpill(filePath, 0)
		if err != nil {
			t.Fatalf("writeAndRecordSpill (empty rows): %v", err)
		}
		if len(s.spilledRuns) != 1 {
			t.Errorf("expected 1 spilled run recorded, got %d", len(s.spilledRuns))
		}
		if s.spilledRuns[0].FilePath != filePath {
			t.Errorf("expected FilePath=%q, got %q", filePath, s.spilledRuns[0].FilePath)
		}
		if s.spilledRuns[0].NumRows != 0 {
			t.Errorf("expected NumRows=0, got %d", s.spilledRuns[0].NumRows)
		}
		// Verify the file was created and then closed (no open handle).
		if s.spilledRuns[0].File != nil {
			t.Error("expected File to be nil (file is closed) after writeAndRecordSpill")
		}
		// Verify the spill file exists on disk.
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Error("expected spill file to exist on disk")
		}
	})

	t.Run("WithRows_WritesSpilledRun", func(t *testing.T) {
		t.Parallel()
		s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 2, nil)
		// Add rows to the in-memory buffer.
		row1 := []*Mem{NewMemInt(1), NewMemStr("alpha")}
		row2 := []*Mem{NewMemInt(2), NewMemStr("beta")}
		s.Sorter.Insert(row1)
		s.Sorter.Insert(row2)

		dir := t.TempDir()
		filePath := filepath.Join(dir, "spill_rows.tmp")
		numRows := len(s.Rows)
		err := s.writeAndRecordSpill(filePath, numRows)
		if err != nil {
			t.Fatalf("writeAndRecordSpill (with rows): %v", err)
		}
		if len(s.spilledRuns) != 1 {
			t.Errorf("expected 1 spilled run, got %d", len(s.spilledRuns))
		}
		if s.spilledRuns[0].NumRows != numRows {
			t.Errorf("expected NumRows=%d, got %d", numRows, s.spilledRuns[0].NumRows)
		}
		// Verify file is non-empty.
		info, err := os.Stat(filePath)
		if err != nil {
			t.Fatalf("Stat spill file: %v", err)
		}
		if info.Size() == 0 {
			t.Error("expected non-empty spill file")
		}
	})
}
