// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Local mock types for schema / table used only in this file.
// ---------------------------------------------------------------------------

// tr4MockTable implements GetColumnNames so it satisfies the tableWithColumns
// interface checked inside getTableColumnNames.
type tr4MockTable struct {
	colNames []string
}

func (t *tr4MockTable) GetColumnNames() []string { return t.colNames }

// tr4MockTableNoIface does NOT implement GetColumnNames, exercising the
// !ok branch of the tableWithColumns type assertion.
type tr4MockTableNoIface struct{}

// tr4MockSchema implements GetTableByName.
type tr4MockSchema struct {
	tables map[string]interface{}
}

func (s *tr4MockSchema) GetTableByName(name string) (interface{}, bool) {
	tbl, ok := s.tables[name]
	return tbl, ok
}

// tr4MockSchemaNoIface does NOT implement GetTableByName, exercising the
// !ok branch of the schemaWithTable type assertion.
type tr4MockSchemaNoIface struct{}

// ---------------------------------------------------------------------------
// newTR4VDBE builds a VDBE with the given schema and numMem register slots.
// ---------------------------------------------------------------------------

func newTR4VDBE(schema interface{}, numMem int) *VDBE {
	v := New()
	v.Ctx = &VDBEContext{Schema: schema}
	_ = v.AllocMemory(numMem)
	return v
}

// ---------------------------------------------------------------------------
// getTableColumnNames
// ---------------------------------------------------------------------------

// TestExecTrigger4_GetTableColumnNames_NilCtx covers the v.Ctx == nil branch.
func TestExecTrigger4_GetTableColumnNames_NilCtx(t *testing.T) {
	t.Parallel()
	v := New()
	v.Ctx = nil
	got := v.getTableColumnNames("foo")
	if got != nil {
		t.Errorf("expected nil for nil ctx, got %v", got)
	}
}

// TestExecTrigger4_GetTableColumnNames_NilSchema covers the v.Ctx.Schema == nil branch.
func TestExecTrigger4_GetTableColumnNames_NilSchema(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(nil, 0)
	got := v.getTableColumnNames("foo")
	if got != nil {
		t.Errorf("expected nil for nil schema, got %v", got)
	}
}

// TestExecTrigger4_GetTableColumnNames_SchemaNoIface covers the !ok branch
// where Schema does not implement GetTableByName.
func TestExecTrigger4_GetTableColumnNames_SchemaNoIface(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(&tr4MockSchemaNoIface{}, 0)
	got := v.getTableColumnNames("foo")
	if got != nil {
		t.Errorf("expected nil when schema lacks GetTableByName, got %v", got)
	}
}

// TestExecTrigger4_GetTableColumnNames_TableNotFound covers the !found branch.
func TestExecTrigger4_GetTableColumnNames_TableNotFound(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{}}
	v := newTR4VDBE(schema, 0)
	got := v.getTableColumnNames("missing")
	if got != nil {
		t.Errorf("expected nil for missing table, got %v", got)
	}
}

// TestExecTrigger4_GetTableColumnNames_TableNoIface covers the !ok branch where
// the table object does not implement GetColumnNames.
func TestExecTrigger4_GetTableColumnNames_TableNoIface(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTableNoIface{},
	}}
	v := newTR4VDBE(schema, 0)
	got := v.getTableColumnNames("t")
	if got != nil {
		t.Errorf("expected nil when table lacks GetColumnNames, got %v", got)
	}
}

// TestExecTrigger4_GetTableColumnNames_HappyPath covers the successful path
// returning column names from a properly shaped schema.
func TestExecTrigger4_GetTableColumnNames_HappyPath(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"id", "name", "val"}},
	}}
	v := newTR4VDBE(schema, 0)
	got := v.getTableColumnNames("t")
	if len(got) != 3 {
		t.Errorf("expected 3 column names, got %d: %v", len(got), got)
	}
	if got[0] != "id" || got[1] != "name" || got[2] != "val" {
		t.Errorf("unexpected column names: %v", got)
	}
}

// ---------------------------------------------------------------------------
// extractRowFromRegisters
// ---------------------------------------------------------------------------

// TestExecTrigger4_ExtractRowFromRegisters_NilSchema covers the nil-schema
// path where getTableColumnNames returns nil.
func TestExecTrigger4_ExtractRowFromRegisters_NilSchema(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(nil, 4)
	got := v.extractRowFromRegisters("t", 0)
	if got != nil {
		t.Errorf("expected nil for nil schema, got %v", got)
	}
}

// TestExecTrigger4_ExtractRowFromRegisters_WithSchema covers the happy path
// where registers are read for the named columns.
func TestExecTrigger4_ExtractRowFromRegisters_WithSchema(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"a", "b"}},
	}}
	v := newTR4VDBE(schema, 4)
	// Set register values: reg 1 = int 42, reg 2 = string "hello".
	v.Mem[1].SetInt(42)
	_ = v.Mem[2].SetStr("hello")
	got := v.extractRowFromRegisters("t", 1)
	if got == nil {
		t.Fatal("expected non-nil row map")
	}
	if len(got) != 2 {
		t.Errorf("expected 2 columns, got %d", len(got))
	}
}

// TestExecTrigger4_ExtractRowFromRegisters_OutOfRangeReg covers the
// err != nil branch inside the loop (register index out of range).
func TestExecTrigger4_ExtractRowFromRegisters_OutOfRangeReg(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"x", "y", "z"}},
	}}
	// Only allocate 2 registers; start at reg 1, so reg 3 is out of range.
	v := newTR4VDBE(schema, 2)
	// Should not panic; out-of-range registers map to nil values.
	got := v.extractRowFromRegisters("t", 1)
	if got == nil {
		t.Fatal("expected non-nil row map even with out-of-range registers")
	}
}

// ---------------------------------------------------------------------------
// extractRowFromCursor
// ---------------------------------------------------------------------------

// TestExecTrigger4_ExtractRowFromCursor_NilSchema covers the nil-schema path.
func TestExecTrigger4_ExtractRowFromCursor_NilSchema(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(nil, 2)
	_ = v.AllocCursors(2)
	got := v.extractRowFromCursor("t", 0)
	if got != nil {
		t.Errorf("expected nil for nil schema, got %v", got)
	}
}

// TestExecTrigger4_ExtractRowFromCursor_CursorNotOpen covers the cursor-error
// path when a cursor slot is nil (cursor not open).
func TestExecTrigger4_ExtractRowFromCursor_CursorNotOpen(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"a"}},
	}}
	v := newTR4VDBE(schema, 2)
	// Allocate cursor slots but leave slot 0 nil (not open).
	_ = v.AllocCursors(2)
	got := v.extractRowFromCursor("t", 0)
	if got != nil {
		t.Errorf("expected nil when cursor not open, got %v", got)
	}
}

// TestExecTrigger4_ExtractRowFromCursor_CursorOutOfRange covers the
// out-of-range cursor index error path.
func TestExecTrigger4_ExtractRowFromCursor_CursorOutOfRange(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"a"}},
	}}
	v := newTR4VDBE(schema, 2)
	// No cursors allocated, so index 0 is out of range.
	got := v.extractRowFromCursor("t", 0)
	if got != nil {
		t.Errorf("expected nil for out-of-range cursor, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// readCursorColumns
// ---------------------------------------------------------------------------

// TestExecTrigger4_ReadCursorColumns_NoRegisters covers the tempReg < 0
// branch when NumMem is 0.
func TestExecTrigger4_ReadCursorColumns_NoRegisters(t *testing.T) {
	t.Parallel()
	v := New()
	// NumMem = 0 so tempReg = v.NumMem - 1 = -1, triggering the tempReg < 0 branch.
	v.NumMem = 0
	_ = v.AllocCursors(1)
	v.Cursors[0] = &Cursor{CurType: CursorPseudo, NullRow: true}
	colNames := []string{"a", "b"}
	got := v.readCursorColumns(0, colNames)
	if got == nil {
		t.Fatal("expected non-nil map")
	}
	// All columns should be nil because tempReg < 0.
	for _, name := range colNames {
		if got[name] != nil {
			t.Errorf("expected nil for %q when no registers, got %v", name, got[name])
		}
	}
}

// TestExecTrigger4_ReadCursorColumns_GetMemError covers the GetMem error branch
// when tempReg is valid (>= 0) but the register slot is missing.
func TestExecTrigger4_ReadCursorColumns_GetMemError(t *testing.T) {
	t.Parallel()
	v := New()
	// Set NumMem = 5 but Mem slice is empty, so GetMem(4) will fail.
	v.NumMem = 5
	// v.Mem stays empty (length 0), so tempReg = 4 is out of range.
	_ = v.AllocCursors(1)
	v.Cursors[0] = &Cursor{CurType: CursorPseudo, NullRow: true}
	colNames := []string{"x"}
	got := v.readCursorColumns(0, colNames)
	if got == nil {
		t.Fatal("expected non-nil map")
	}
	if got["x"] != nil {
		t.Errorf("expected nil for 'x' on GetMem error, got %v", got["x"])
	}
}

// TestExecTrigger4_ReadCursorColumns_NullRowCursor covers the path where
// execColumnDirect is called on a NullRow cursor (sets NULL in dest register).
func TestExecTrigger4_ReadCursorColumns_NullRowCursor(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(3)
	v.NumMem = 3
	_ = v.AllocCursors(1)
	// NullRow cursor: execColumn will set dest to NULL and return nil.
	v.Cursors[0] = &Cursor{CurType: CursorPseudo, NullRow: true}
	colNames := []string{"col"}
	got := v.readCursorColumns(0, colNames)
	if got == nil {
		t.Fatal("expected non-nil map")
	}
	// NullRow means execColumn sets NULL, memToGoValue returns nil.
	_ = got["col"] // just verify no panic
}

// TestExecTrigger4_ReadCursorColumns_EmptyColNames covers the empty-list path.
func TestExecTrigger4_ReadCursorColumns_EmptyColNames(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(3)
	v.NumMem = 3
	_ = v.AllocCursors(1)
	v.Cursors[0] = &Cursor{CurType: CursorPseudo, NullRow: true}
	got := v.readCursorColumns(0, nil)
	if got == nil {
		t.Fatal("expected non-nil (empty) map")
	}
	if len(got) != 0 {
		t.Errorf("expected empty map for nil colNames, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// execColumnDirect
// ---------------------------------------------------------------------------

// TestExecTrigger4_ExecColumnDirect_CursorError covers the error path in
// execColumn when the cursor index is invalid.
func TestExecTrigger4_ExecColumnDirect_CursorError(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(2)
	// No cursors allocated, so P1=0 will trigger a cursor error.
	instr := &Instruction{Opcode: OpColumn, P1: 0, P2: 0, P3: 1}
	err := v.execColumnDirect(instr)
	if err == nil {
		t.Error("expected error from execColumnDirect with invalid cursor index")
	}
}

// TestExecTrigger4_ExecColumnDirect_NullRow covers the nil-payload fast path
// in execColumn (cursor NullRow = true returns nil error after setting NULL).
func TestExecTrigger4_ExecColumnDirect_NullRow(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(3)
	_ = v.AllocCursors(1)
	v.Cursors[0] = &Cursor{CurType: CursorPseudo, NullRow: true}
	instr := &Instruction{Opcode: OpColumn, P1: 0, P2: 0, P3: 2}
	err := v.execColumnDirect(instr)
	if err != nil {
		t.Errorf("expected nil error for NullRow cursor, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// buildTriggerRowFromDelete
// ---------------------------------------------------------------------------

// TestExecTrigger4_BuildTriggerRowFromDelete_NilSchema verifies that
// buildTriggerRowFromDelete returns a non-nil TriggerRowData with nil OldRow
// when the schema is nil (no column names available).
func TestExecTrigger4_BuildTriggerRowFromDelete_NilSchema(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(nil, 2)
	_ = v.AllocCursors(1)
	result := v.buildTriggerRowFromDelete("t", 0)
	if result == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	if result.OldRow != nil {
		t.Errorf("expected nil OldRow for nil schema, got %v", result.OldRow)
	}
	if result.NewRow != nil {
		t.Errorf("expected nil NewRow for DELETE, got %v", result.NewRow)
	}
}

// TestExecTrigger4_BuildTriggerRowFromDelete_WithSchema verifies that
// buildTriggerRowFromDelete returns a TriggerRowData when schema is valid
// but cursor is not open (extractRowFromCursor returns nil).
func TestExecTrigger4_BuildTriggerRowFromDelete_WithSchema(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"id", "val"}},
	}}
	v := newTR4VDBE(schema, 4)
	// No cursor open → extractRowFromCursor returns nil.
	_ = v.AllocCursors(2)
	result := v.buildTriggerRowFromDelete("t", 0)
	if result == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	// OldRow is nil because cursor slot 0 is nil.
	if result.OldRow != nil {
		t.Errorf("expected nil OldRow (cursor not open), got %v", result.OldRow)
	}
}

// ---------------------------------------------------------------------------
// buildTriggerRowFromUpdate
// ---------------------------------------------------------------------------

// TestExecTrigger4_BuildTriggerRowFromUpdate_NilSchema verifies that
// buildTriggerRowFromUpdate returns a non-nil TriggerRowData with nil rows
// when schema is nil.
func TestExecTrigger4_BuildTriggerRowFromUpdate_NilSchema(t *testing.T) {
	t.Parallel()
	v := newTR4VDBE(nil, 4)
	_ = v.AllocCursors(1)
	result := v.buildTriggerRowFromUpdate("t", 0, 1)
	if result == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	if result.OldRow != nil {
		t.Errorf("expected nil OldRow for nil schema, got %v", result.OldRow)
	}
	if result.NewRow != nil {
		t.Errorf("expected nil NewRow for nil schema, got %v", result.NewRow)
	}
}

// TestExecTrigger4_BuildTriggerRowFromUpdate_WithSchema verifies that
// buildTriggerRowFromUpdate populates NewRow from registers when schema is set.
func TestExecTrigger4_BuildTriggerRowFromUpdate_WithSchema(t *testing.T) {
	t.Parallel()
	schema := &tr4MockSchema{tables: map[string]interface{}{
		"t": &tr4MockTable{colNames: []string{"id", "val"}},
	}}
	v := newTR4VDBE(schema, 6)
	_ = v.AllocCursors(2)
	// Set register values for the NEW row (starting at reg 2).
	v.Mem[2].SetInt(1)
	_ = v.Mem[3].SetStr("updated")
	result := v.buildTriggerRowFromUpdate("t", 0, 2)
	if result == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	// OldRow: cursor 0 not open → nil.
	if result.OldRow != nil {
		t.Errorf("expected nil OldRow (cursor not open), got %v", result.OldRow)
	}
	// NewRow: extracted from registers 2..3.
	if result.NewRow == nil {
		t.Error("expected non-nil NewRow from registers")
	}
	if len(result.NewRow) != 2 {
		t.Errorf("expected 2 NewRow entries, got %d", len(result.NewRow))
	}
}
