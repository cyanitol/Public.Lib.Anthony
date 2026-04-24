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

func TestFKAdapter6FindMatchingRow_BadCursor(t *testing.T) {
	t.Parallel()
	v := New()
	v.Ctx = &VDBEContext{}
	if err := v.AllocCursors(5); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
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
}

func TestFKAdapter6FindMatchingRow_EmptyTable(t *testing.T) {
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
	found, err := r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{int64(99)})
	if err != nil {
		t.Fatalf("findMatchingRow on empty table: %v", err)
	}
	if found {
		t.Error("expected false for empty table")
	}
}

// fk6FindMatchingRowWithPayload is a helper that creates a VDBE with a table
// containing one row, opens a cursor, and calls findMatchingRow.
func fk6FindMatchingRowWithPayload(t *testing.T, payload []byte, rowid int64, searchID int64) (bool, error) {
	t.Helper()
	v, root := fk6MakeVDBEWithTable(t,
		[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
		rowid, payload,
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
	return r.findMatchingRow(0, tbl, []string{"id"}, []interface{}{searchID})
}

func TestFKAdapter6FindMatchingRow_MatchFound(t *testing.T) {
	t.Parallel()
	found, err := fk6FindMatchingRowWithPayload(t, fk6TextPayload("hello"), 1, int64(1))
	if err != nil {
		t.Fatalf("findMatchingRow: %v", err)
	}
	if !found {
		t.Error("expected true when row with id=1 exists")
	}
}

func TestFKAdapter6FindMatchingRow_NoMatch(t *testing.T) {
	t.Parallel()
	found, err := fk6FindMatchingRowWithPayload(t, fk6TextPayload("world"), 2, int64(99))
	if err != nil {
		t.Fatalf("findMatchingRow: %v", err)
	}
	if found {
		t.Error("expected false when id=99 does not exist")
	}
}

// ---------------------------------------------------------------------------
// TestFKAdapter6CollectMatchingRowData
// Exercises collectMatchingRowData (line 645).
// ---------------------------------------------------------------------------

func TestFKAdapter6CollectMatchingRowData_BadCursor(t *testing.T) {
	t.Parallel()
	v := New()
	v.Ctx = &VDBEContext{}
	if err := v.AllocCursors(5); err != nil {
		t.Fatalf("AllocCursors: %v", err)
	}
	v.Cursors[0] = &Cursor{
		CurType:     CursorBTree,
		BtreeCursor: struct{}{},
	}
	r := &VDBERowReader{vdbe: v}
	tbl := makeTableInfo([]columnInfo{{Name: "id", Type: "INTEGER"}})
	_, err := r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{int64(1)})
	if err == nil {
		t.Error("expected error for invalid cursor type")
	}
}

func TestFKAdapter6CollectMatchingRowData_EmptyTable(t *testing.T) {
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
}

// fk6CollectMatchingWithPayload is a helper for collectMatchingRowData tests.
func fk6CollectMatchingWithPayload(t *testing.T, payload []byte, rowid, searchID int64) ([]map[string]interface{}, error) {
	t.Helper()
	v, root := fk6MakeVDBEWithTable(t,
		[]columnInfo{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
		rowid, payload,
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
	return r.collectMatchingRowData(0, tbl, []string{"id"}, []interface{}{searchID})
}

func TestFKAdapter6CollectMatchingRowData_MatchFound(t *testing.T) {
	t.Parallel()
	rows, err := fk6CollectMatchingWithPayload(t, fk6TextPayload("alice"), 5, int64(5))
	if err != nil {
		t.Fatalf("collectMatchingRowData: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 matching row, got %d", len(rows))
	}
	if rows[0]["id"] != int64(5) {
		t.Errorf("expected id=5, got %v", rows[0]["id"])
	}
}

func TestFKAdapter6CollectMatchingRowData_NoMatch(t *testing.T) {
	t.Parallel()
	rows, err := fk6CollectMatchingWithPayload(t, fk6TextPayload("bob"), 3, int64(999))
	if err != nil {
		t.Fatalf("collectMatchingRowData no match: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for no match, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// TestFKAdapter6DeleteRow
// Exercises VDBERowModifier.DeleteRow (line 1238).
// ---------------------------------------------------------------------------

func TestFKAdapter6DeleteRow_TableNotFound(t *testing.T) {
	t.Parallel()
	schema := &fkaCovSchema{tables: map[string]interface{}{}}
	v := New()
	v.Ctx = &VDBEContext{Schema: schema}
	m := NewVDBERowModifier(v)
	if err := m.DeleteRow("missing", 1); err == nil {
		t.Error("expected error for missing table")
	}
}

func TestFKAdapter6DeleteRow_WithoutRowID(t *testing.T) {
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
	if err := m.DeleteRow("wr", 1); err == nil {
		t.Error("expected error for WITHOUT ROWID table")
	}
}

func TestFKAdapter6DeleteRow_RowNotFound(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
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
	if err := m.DeleteRow("items", 42); err == nil {
		t.Error("expected error for rowid not found in empty table")
	}
}

func TestFKAdapter6DeleteRow_HappyPath(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	root, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
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
	verifyCur := btree.NewCursorWithOptions(bt, root, false)
	found, err := verifyCur.SeekRowid(7)
	if err != nil {
		t.Fatalf("SeekRowid after delete: %v", err)
	}
	if found {
		t.Error("expected rowid 7 to be deleted")
	}
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

func TestFKAdapter6WriteAndRecordSpill_InvalidPath(t *testing.T) {
	t.Parallel()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 1, nil)
	err := s.writeAndRecordSpill("/nonexistent_dir_abc123/spill.tmp", 0)
	if err == nil {
		t.Error("expected error for invalid spill file path")
	}
}

func TestFKAdapter6WriteAndRecordSpill_EmptyRows(t *testing.T) {
	t.Parallel()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 1, nil)
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
	if s.spilledRuns[0].File != nil {
		t.Error("expected File to be nil (file is closed) after writeAndRecordSpill")
	}
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected spill file to exist on disk")
	}
}

func TestFKAdapter6WriteAndRecordSpill_WithRows(t *testing.T) {
	t.Parallel()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{"BINARY"}, 2, nil)
	s.Sorter.Insert([]*Mem{NewMemInt(1), NewMemStr("alpha")})
	s.Sorter.Insert([]*Mem{NewMemInt(2), NewMemStr("beta")})

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
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat spill file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("expected non-empty spill file")
	}
}
