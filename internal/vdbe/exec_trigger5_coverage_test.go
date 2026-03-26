// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"os"
	"testing"
)

// ---------------------------------------------------------------------------
// buildTriggerRowForOp – direct unit tests for uncovered branches
// ---------------------------------------------------------------------------

// newTR5VDBE creates a VDBE with the given schema and allocates numMem
// register slots, mirroring the helper pattern from exec_trigger4.
func newTR5VDBE(schema interface{}, numMem int) *VDBE {
	v := New()
	v.Ctx = &VDBEContext{Schema: schema}
	_ = v.AllocMemory(numMem)
	return v
}

// tr5MockTable implements GetColumnNames for the tableWithColumns interface.
type tr5MockTable struct {
	colNames []string
}

func (t *tr5MockTable) GetColumnNames() []string { return t.colNames }

// tr5MockSchema implements GetTableByName for the schemaWithTable interface.
type tr5MockSchema struct {
	tables map[string]interface{}
}

func (s *tr5MockSchema) GetTableByName(name string) (interface{}, bool) {
	tbl, ok := s.tables[name]
	return tbl, ok
}

// TestExecTrigger5_BuildTriggerRowForOp_DefaultP1 covers the default branch
// of the switch in buildTriggerRowForOp when P1 is not 0, 1, or 2.
// The function should return an empty (non-nil) TriggerRowData.
func TestExecTrigger5_BuildTriggerRowForOp_DefaultP1(t *testing.T) {
	t.Parallel()
	v := newTR5VDBE(nil, 4)
	instr := &Instruction{P1: 99, P3: 1, P4: P4Union{Z: "mytable"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData for default P1")
	}
	if got.OldRow != nil || got.NewRow != nil {
		t.Errorf("expected empty TriggerRowData for default P1, got OldRow=%v NewRow=%v",
			got.OldRow, got.NewRow)
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_StartRegZeroWithTriggerRow covers the
// startReg <= 0 branch when v.TriggerRow is non-nil. The function should
// return the existing TriggerRow pointer unchanged.
func TestExecTrigger5_BuildTriggerRowForOp_StartRegZeroWithTriggerRow(t *testing.T) {
	t.Parallel()
	v := newTR5VDBE(nil, 4)
	existing := &TriggerRowData{
		OldRow: map[string]interface{}{"id": int64(7)},
	}
	v.TriggerRow = existing

	instr := &Instruction{P1: 0, P3: 0, P4: P4Union{Z: "mytable"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	if got != existing {
		t.Errorf("expected the existing TriggerRow to be returned, got a different pointer")
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_StartRegNegWithTriggerRow covers the
// startReg < 0 branch when v.TriggerRow is non-nil. Negative P3 is also <= 0.
func TestExecTrigger5_BuildTriggerRowForOp_StartRegNegWithTriggerRow(t *testing.T) {
	t.Parallel()
	v := newTR5VDBE(nil, 4)
	existing := &TriggerRowData{
		NewRow: map[string]interface{}{"val": "hello"},
	}
	v.TriggerRow = existing

	instr := &Instruction{P1: 1, P3: -5, P4: P4Union{Z: "mytable"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData")
	}
	if got != existing {
		t.Errorf("expected existing TriggerRow returned for negative startReg, got different pointer")
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_StartRegZeroNoTriggerRow covers the
// startReg <= 0 branch when v.TriggerRow is nil. The function should return
// a new empty TriggerRowData.
func TestExecTrigger5_BuildTriggerRowForOp_StartRegZeroNoTriggerRow(t *testing.T) {
	t.Parallel()
	v := newTR5VDBE(nil, 4)
	// TriggerRow is nil by default after New().

	instr := &Instruction{P1: 0, P3: 0, P4: P4Union{Z: "mytable"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData for startReg=0, nil TriggerRow")
	}
	if got.OldRow != nil || got.NewRow != nil {
		t.Errorf("expected empty TriggerRowData, got OldRow=%v NewRow=%v", got.OldRow, got.NewRow)
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_P1Insert covers the P1=0 (INSERT)
// branch with a positive startReg so the switch case is exercised.
func TestExecTrigger5_BuildTriggerRowForOp_P1Insert(t *testing.T) {
	t.Parallel()
	schema := &tr5MockSchema{tables: map[string]interface{}{
		"t": &tr5MockTable{colNames: []string{"name", "score"}},
	}}
	v := newTR5VDBE(schema, 6)
	v.Mem[1].SetInt(42)
	_ = v.Mem[2].SetStr("alice")

	instr := &Instruction{P1: 0, P3: 1, P4: P4Union{Z: "t"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData for P1=0 INSERT")
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_P1Update covers the P1=1 (UPDATE)
// branch with positive startReg and a valid P5 new-record register.
func TestExecTrigger5_BuildTriggerRowForOp_P1Update(t *testing.T) {
	t.Parallel()
	schema := &tr5MockSchema{tables: map[string]interface{}{
		"t": &tr5MockTable{colNames: []string{"val"}},
	}}
	v := newTR5VDBE(schema, 8)
	v.Mem[2].SetInt(100)
	v.Mem[5].SetInt(200)

	instr := &Instruction{P1: 1, P3: 2, P5: 5, P4: P4Union{Z: "t"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData for P1=1 UPDATE")
	}
}

// TestExecTrigger5_BuildTriggerRowForOp_P1Delete covers the P1=2 (DELETE)
// branch with a positive startReg.
func TestExecTrigger5_BuildTriggerRowForOp_P1Delete(t *testing.T) {
	t.Parallel()
	schema := &tr5MockSchema{tables: map[string]interface{}{
		"t": &tr5MockTable{colNames: []string{"label"}},
	}}
	v := newTR5VDBE(schema, 6)
	_ = v.Mem[2].SetStr("deleted-item")

	instr := &Instruction{P1: 2, P3: 2, P4: P4Union{Z: "t"}}
	got := v.buildTriggerRowForOp(instr)
	if got == nil {
		t.Fatal("expected non-nil TriggerRowData for P1=2 DELETE")
	}
}

// ---------------------------------------------------------------------------
// writeAndRecordSpill – additional coverage paths
// ---------------------------------------------------------------------------

// TestExecTrigger5_WriteAndRecordSpill_SuccessPath exercises the happy path of
// writeAndRecordSpill: rows are written, file is closed, and the run is appended
// to spilledRuns.
func TestExecTrigger5_WriteAndRecordSpill_SuccessPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, &SorterConfig{
		MaxMemoryBytes: 1 << 20,
		TempDir:        tempDir,
		EnableSpill:    true,
	})
	defer s.Close()

	s.Sorter.Insert([]*Mem{NewMemInt(10)})
	s.Sorter.Insert([]*Mem{NewMemInt(20)})

	// Copy rows into the SorterWithSpill.Rows so writeAndRecordSpill sees them.
	s.Sorter.Rows = append(s.Sorter.Rows, []*Mem{NewMemInt(10)})
	s.Sorter.Rows = append(s.Sorter.Rows, []*Mem{NewMemInt(20)})

	filePath := tempDir + "/test_spill_success.tmp"
	numBefore := len(s.spilledRuns)

	if err := s.writeAndRecordSpill(filePath, 2); err != nil {
		t.Fatalf("writeAndRecordSpill happy path: %v", err)
	}

	if len(s.spilledRuns) != numBefore+1 {
		t.Errorf("expected spilledRuns to grow by 1, before=%d after=%d",
			numBefore, len(s.spilledRuns))
	}

	run := s.spilledRuns[len(s.spilledRuns)-1]
	if run.FilePath != filePath {
		t.Errorf("spilledRun.FilePath: want %q, got %q", filePath, run.FilePath)
	}
	if run.NumRows != 2 {
		t.Errorf("spilledRun.NumRows: want 2, got %d", run.NumRows)
	}
	if run.File != nil {
		t.Error("spilledRun.File should be nil after writeAndRecordSpill")
	}

	// Verify the file actually exists on disk.
	if _, err := os.Stat(filePath); err != nil {
		t.Errorf("spill file should exist on disk: %v", err)
	}
}

// TestExecTrigger5_WriteAndRecordSpill_BadPathError covers the os.Create
// failure branch where the parent directory does not exist.
func TestExecTrigger5_WriteAndRecordSpill_BadPathError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	s.Sorter.Rows = append(s.Sorter.Rows, []*Mem{NewMemInt(1)})

	badPath := "/no/such/directory/spill5_test.tmp"
	err := s.writeAndRecordSpill(badPath, 1)
	if err == nil {
		t.Fatal("expected error for bad file path, got nil")
	}
}

// TestExecTrigger5_WriteAndRecordSpill_EmptyRows exercises writeAndRecordSpill
// with no in-memory rows (empty Rows slice). This exercises the successful
// write path with zero rows encoded in the file.
func TestExecTrigger5_WriteAndRecordSpill_EmptyRows(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, &SorterConfig{
		MaxMemoryBytes: 1 << 20,
		TempDir:        tempDir,
		EnableSpill:    true,
	})
	defer s.Close()

	// Rows is empty; writeAndRecordSpill should still succeed.
	filePath := tempDir + "/empty_rows_spill.tmp"
	// Clear Sorter.Rows explicitly to ensure it is empty.
	s.Sorter.Rows = s.Sorter.Rows[:0]
	if err := s.writeAndRecordSpill(filePath, 0); err != nil {
		t.Fatalf("writeAndRecordSpill with empty rows: %v", err)
	}

	if len(s.spilledRuns) != 1 {
		t.Errorf("expected 1 spilledRun after empty write, got %d", len(s.spilledRuns))
	}
}

// TestExecTrigger5_WriteAndRecordSpill_MultipleCallsAccumulate verifies that
// successive calls to writeAndRecordSpill correctly accumulate multiple
// SpilledRun entries in s.spilledRuns.
func TestExecTrigger5_WriteAndRecordSpill_MultipleCallsAccumulate(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, &SorterConfig{
		MaxMemoryBytes: 1 << 20,
		TempDir:        tempDir,
		EnableSpill:    true,
	})
	defer s.Close()

	for i := 1; i <= 3; i++ {
		s.Sorter.Rows = [][]*Mem{{NewMemInt(int64(i))}}
		filePath := tempDir + "/" + "spill_multi_" + string(rune('0'+i)) + ".tmp"
		if err := s.writeAndRecordSpill(filePath, 1); err != nil {
			t.Fatalf("writeAndRecordSpill call %d: %v", i, err)
		}
	}

	if len(s.spilledRuns) != 3 {
		t.Errorf("expected 3 spilledRuns, got %d", len(s.spilledRuns))
	}
}
