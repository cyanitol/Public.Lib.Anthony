// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"bytes"
	"math"
	"os"
	"testing"
)

// mockConnStateFKSorter implements ConnStateProvider for testing.
type mockConnStateFKSorter struct {
	lastInsertRowID int64
	changes         int64
	totalChanges    int64
}

func (m *mockConnStateFKSorter) LastInsertRowID() int64 { return m.lastInsertRowID }
func (m *mockConnStateFKSorter) Changes() int64         { return m.changes }
func (m *mockConnStateFKSorter) TotalChanges() int64    { return m.totalChanges }

// newVDBEWithInstr creates a minimal VDBE with one instruction at Program[0] and PC=1.
func newVDBEWithInstr(instr *Instruction) *VDBE {
	v := New()
	v.AllocMemory(10)
	v.Program = append(v.Program, instr)
	v.PC = 1
	return v
}

// --------------------------------------------------------------------------
// connStateFuncTotalChanges
// --------------------------------------------------------------------------

func TestConnStateFuncTotalChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connState ConnStateProvider
		wantTotal int64
	}{
		{
			name:      "with ConnState returns provider value",
			connState: &mockConnStateFKSorter{totalChanges: 42},
			wantTotal: 42,
		},
		{
			name:      "nil ConnState returns zero",
			connState: nil,
			wantTotal: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			if tt.connState != nil {
				v.Ctx = &VDBEContext{ConnState: tt.connState}
			}
			result := v.connStateFuncTotalChanges()
			if !result.IsInt() || result.IntValue() != tt.wantTotal {
				t.Errorf("connStateFuncTotalChanges() = %v, want %d", result.IntValue(), tt.wantTotal)
			}
		})
	}
}

// --------------------------------------------------------------------------
// handleConnStateFunc
// --------------------------------------------------------------------------

func TestHandleConnStateFunc(t *testing.T) {
	t.Parallel()

	state := &mockConnStateFKSorter{lastInsertRowID: 10, changes: 5, totalChanges: 99}

	tests := []struct {
		name        string
		funcName    string
		wantHandled bool
		wantVal     int64
	}{
		{"last_insert_rowid lowercase", "last_insert_rowid", true, 10},
		{"last_insert_rowid upper", "LAST_INSERT_ROWID", true, 10},
		{"changes", "changes", true, 5},
		{"total_changes", "total_changes", true, 99},
		{"unknown function not handled", "abs", false, 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			v.Ctx = &VDBEContext{ConnState: state}

			result, ok := v.handleConnStateFunc(tt.funcName)
			if ok != tt.wantHandled {
				t.Fatalf("handleConnStateFunc(%q) handled=%v, want %v", tt.funcName, ok, tt.wantHandled)
			}
			if tt.wantHandled && result.IntValue() != tt.wantVal {
				t.Errorf("handleConnStateFunc(%q) val=%d, want %d", tt.funcName, result.IntValue(), tt.wantVal)
			}
		})
	}
}

// --------------------------------------------------------------------------
// opFunction edge cases
// --------------------------------------------------------------------------

func TestOpFunctionEdgeCases_Errors(t *testing.T) {
	t.Parallel()

	// invalid P4Type
	instr := &Instruction{Opcode: OpFunction, P4Type: P4Type(0)}
	v := newVDBEWithInstr(instr)
	if err := v.opFunction(0, 1, 5, 0, 0); err == nil {
		t.Error("expected error for missing P4 function name")
	}

	// unknown function
	instr = &Instruction{Opcode: OpFunction, P4: P4Union{Z: "no_such_func_xyz"}, P4Type: P4Static}
	v = newVDBEWithInstr(instr)
	if err := v.opFunction(0, 1, 5, 0, 0); err == nil {
		t.Error("expected error for unknown function")
	}

	// aggregate as scalar
	instr = &Instruction{Opcode: OpFunction, P2: 1, P3: 5, P4: P4Union{Z: "sum"}, P4Type: P4Static, P5: 1}
	v = newVDBEWithInstr(instr)
	v.Mem[1].SetInt(1)
	if err := v.opFunction(0, 1, 5, 0, 1); err == nil {
		t.Error("expected error calling aggregate sum() as scalar")
	}
}

func TestOpFunctionEdgeCases_ConnState(t *testing.T) {
	t.Parallel()

	instr := &Instruction{Opcode: OpFunction, P2: 1, P3: 5, P4: P4Union{Z: "last_insert_rowid"}, P4Type: P4Static}
	v := newVDBEWithInstr(instr)
	v.Ctx = &VDBEContext{ConnState: &mockConnStateFKSorter{lastInsertRowID: 77}}
	if err := v.opFunction(0, 1, 5, 0, 0); err != nil {
		t.Fatalf("opFunction(last_insert_rowid) error = %v", err)
	}
	if v.Mem[5].IntValue() != 77 {
		t.Errorf("got %d, want 77", v.Mem[5].IntValue())
	}

	instr = &Instruction{Opcode: OpFunction, P2: 1, P3: 5, P4: P4Union{Z: "total_changes"}, P4Type: P4Static}
	v = newVDBEWithInstr(instr)
	v.Ctx = &VDBEContext{ConnState: &mockConnStateFKSorter{totalChanges: 123}}
	if err := v.opFunction(0, 1, 5, 0, 0); err != nil {
		t.Fatalf("opFunction(total_changes) error = %v", err)
	}
	if v.Mem[5].IntValue() != 123 {
		t.Errorf("got %d, want 123", v.Mem[5].IntValue())
	}
}

func TestOpFunctionEdgeCases_AbsNull(t *testing.T) {
	t.Parallel()
	instr := &Instruction{Opcode: OpFunction, P2: 1, P3: 5, P4: P4Union{Z: "abs"}, P4Type: P4Static, P5: 1}
	v := newVDBEWithInstr(instr)
	v.Mem[1].SetNull()
	if err := v.opFunction(0, 1, 5, 0, 1); err != nil {
		t.Fatalf("opFunction(abs, NULL) error = %v", err)
	}
	if !v.Mem[5].IsNull() {
		t.Errorf("abs(NULL) expected NULL")
	}
}

// --------------------------------------------------------------------------
// opAggStep edge cases
// --------------------------------------------------------------------------

func TestOpAggStepEdgeCases_Errors(t *testing.T) {
	t.Parallel()

	instr := &Instruction{Opcode: OpAggStep, P4Type: P4Type(0)}
	v := newVDBEWithInstr(instr)
	if err := v.opAggStep(0, 0, 0, 0, 0); err == nil {
		t.Error("expected error for missing P4 name")
	}

	instr = &Instruction{Opcode: OpAggStep, P4: P4Union{Z: "no_such_agg_xyz"}, P4Type: P4Static}
	v = newVDBEWithInstr(instr)
	v.AllocCursors(1)
	if err := v.opAggStep(0, 1, 0, 0, 0); err == nil {
		t.Error("expected error for unknown aggregate")
	}
}

// opAggStepHelper runs an aggregate function over values and returns the finalized result.
func opAggStepHelper(t *testing.T, funcName string, values []int64) int64 {
	t.Helper()
	v := New()
	v.AllocMemory(10)
	v.AllocCursors(1)
	for i, val := range values {
		v.Mem[i].SetInt(val)
		instr := &Instruction{Opcode: OpAggStep, P1: 0, P2: i, P3: 0, P4: P4Union{Z: funcName}, P4Type: P4Static, P5: 1}
		v.Program = append(v.Program, instr)
		v.PC = len(v.Program)
		if err := v.opAggStep(0, i, 0, 0, 1); err != nil {
			t.Fatalf("opAggStep(%s) error = %v", funcName, err)
		}
	}
	if err := v.opAggFinal(0, 5, 0); err != nil {
		t.Fatalf("opAggFinal(%s) error = %v", funcName, err)
	}
	return v.Mem[5].IntValue()
}

func TestOpAggStepEdgeCases_Sum(t *testing.T) {
	t.Parallel()
	if got := opAggStepHelper(t, "sum", []int64{10, 20, 30}); got != 60 {
		t.Errorf("sum result = %d, want 60", got)
	}
}

func TestOpAggStepEdgeCases_Max(t *testing.T) {
	t.Parallel()
	if got := opAggStepHelper(t, "max", []int64{3, 1, 2}); got != 3 {
		t.Errorf("max result = %d, want 3", got)
	}
}

// --------------------------------------------------------------------------
// encodeInt64ForKey
// --------------------------------------------------------------------------

func TestEncodeInt64ForKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b int64
		less bool // a < b in sort order
	}{
		{"zero < positive", 0, 1, true},
		{"negative < zero", -1, 0, true},
		{"negative < positive", -100, 100, true},
		{"min int64 < max int64", math.MinInt64, math.MaxInt64, true},
		{"equal values equal", 42, 42, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encA := encodeInt64ForKey(tt.a)
			encB := encodeInt64ForKey(tt.b)
			if len(encA) != 8 || len(encB) != 8 {
				t.Fatalf("expected 8 bytes, got %d and %d", len(encA), len(encB))
			}
			cmp := bytes.Compare(encA, encB)
			if tt.less && cmp >= 0 {
				t.Errorf("encode(%d) should sort < encode(%d)", tt.a, tt.b)
			}
			if !tt.less && cmp != 0 {
				t.Errorf("encode(%d) should equal encode(%d)", tt.a, tt.b)
			}
		})
	}
}

// --------------------------------------------------------------------------
// encodeFloat64ForKey
// --------------------------------------------------------------------------

func TestEncodeFloat64ForKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b float64
		less bool
	}{
		{"neg < zero", -1.0, 0.0, true},
		{"zero < pos", 0.0, 1.0, true},
		{"neg inf < pos inf", math.Inf(-1), math.Inf(1), true},
		{"equal floats equal", 3.14, 3.14, false},
		{"-0 and 0 sort same", math.Copysign(0, -1), 0.0, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encA := encodeFloat64ForKey(tt.a)
			encB := encodeFloat64ForKey(tt.b)
			if len(encA) != 8 || len(encB) != 8 {
				t.Fatalf("expected 8 bytes, got %d and %d", len(encA), len(encB))
			}
			cmp := bytes.Compare(encA, encB)
			if tt.less && cmp >= 0 {
				t.Errorf("encode(%v) should sort < encode(%v)", tt.a, tt.b)
			}
			if !tt.less && cmp != 0 {
				t.Errorf("encode(%v) should equal encode(%v)", tt.a, tt.b)
			}
		})
	}
}

// --------------------------------------------------------------------------
// encodeCompositeKey
// --------------------------------------------------------------------------

func TestEncodeCompositeKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []interface{}
	}{
		{"nil value", []interface{}{nil}},
		{"int value", []interface{}{int(42)}},
		{"int64 value", []interface{}{int64(100)}},
		{"float64 value", []interface{}{float64(3.14)}},
		{"string value", []interface{}{"hello"}},
		{"blob value", []interface{}{[]byte{1, 2, 3}}},
		{"unknown type uses fmt", []interface{}{struct{ X int }{X: 7}}},
		{"mixed values", []interface{}{int64(1), "abc", nil}},
		{"empty slice", []interface{}{}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := encodeCompositeKey(tt.values)
			// Basic sanity: non-empty input produces non-empty output (except empty slice)
			if len(tt.values) > 0 && len(result) == 0 {
				t.Error("encodeCompositeKey returned empty bytes for non-empty input")
			}
		})
	}

	// Ordering: int key 1 must sort before int key 2
	t.Run("int ordering preserved", func(t *testing.T) {
		t.Parallel()
		k1 := encodeCompositeKey([]interface{}{int64(1)})
		k2 := encodeCompositeKey([]interface{}{int64(2)})
		if bytes.Compare(k1, k2) >= 0 {
			t.Error("key(1) should sort before key(2)")
		}
	})
}

// --------------------------------------------------------------------------
// SorterWithSpill – target the specific functions
// --------------------------------------------------------------------------

// TestSorterSpillNewSorterWithSpill covers NewSorterWithSpill with nil config.
func TestSorterSpillNewSorterWithSpill(t *testing.T) {
	t.Parallel()

	t.Run("nil config uses default", func(t *testing.T) {
		t.Parallel()
		s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
		defer s.Close()
		if s.Config == nil {
			t.Fatal("Config should not be nil when nil passed")
		}
		if s.Config.MaxMemoryBytes != 10*1024*1024 {
			t.Errorf("expected default 10MB, got %d", s.Config.MaxMemoryBytes)
		}
	})

	t.Run("explicit config is used", func(t *testing.T) {
		t.Parallel()
		cfg := &SorterConfig{MaxMemoryBytes: 256, EnableSpill: true}
		s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
		defer s.Close()
		if s.Config.MaxMemoryBytes != 256 {
			t.Errorf("expected 256, got %d", s.Config.MaxMemoryBytes)
		}
	})
}

// TestSorterSpillWriteAndRecord covers writeAndRecordSpill directly.
func TestSorterSpillWriteAndRecord(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()

	cfg := &SorterConfig{MaxMemoryBytes: 10 * 1024 * 1024, TempDir: tempDir, EnableSpill: true}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 2, cfg)
	defer s.Close()

	// Put rows into sorter in-memory buffer manually
	s.Sorter.Insert([]*Mem{NewMemInt(1), NewMemStr("a")})
	s.Sorter.Insert([]*Mem{NewMemInt(2), NewMemStr("b")})

	filePath := tempDir + "/test_spill.tmp"
	if err := s.writeAndRecordSpill(filePath, 2); err != nil {
		t.Fatalf("writeAndRecordSpill error: %v", err)
	}

	if len(s.spilledRuns) != 1 {
		t.Errorf("expected 1 spilled run, got %d", len(s.spilledRuns))
	}
	if s.spilledRuns[0].NumRows != 2 {
		t.Errorf("expected NumRows=2, got %d", s.spilledRuns[0].NumRows)
	}

	// File should exist on disk
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("spill file should exist after writeAndRecordSpill")
	}
}

// TestSorterSpillWriteRunToFile covers writeRunToFile with varied Mem types.
func TestSorterSpillWriteRunToFile(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()

	cfg := &SorterConfig{MaxMemoryBytes: 10 * 1024 * 1024, TempDir: tempDir, EnableSpill: true}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 4, cfg)
	defer s.Close()

	rows := [][]*Mem{
		{NewMemInt(1), NewMemReal(2.5), NewMemStr("hello"), NewMemNull()},
		{NewMemInt(2), NewMemBlob([]byte{0xDE, 0xAD}), NewMemNull(), NewMemStr("world")},
	}

	filePath := tempDir + "/write_run_test.tmp"
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := s.writeRunToFile(f, rows); err != nil {
		f.Close()
		t.Fatalf("writeRunToFile error: %v", err)
	}
	f.Close()

	// Read back and verify row count
	f2, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("open spill file: %v", err)
	}
	defer f2.Close()
	readRows, err := s.readRunFromFile(f2)
	if err != nil {
		t.Fatalf("readRunFromFile error: %v", err)
	}
	if len(readRows) != 2 {
		t.Errorf("expected 2 rows back, got %d", len(readRows))
	}
}

func verifySortedSequence(t *testing.T, s *SorterWithSpill, n int64) {
	t.Helper()
	for i := int64(1); i <= n; i++ {
		if !s.Next() {
			t.Fatalf("missing row %d", i)
		}
		if got := s.CurrentRow()[0].IntValue(); got != i {
			t.Errorf("row %d: got %d, want %d", i, got, i)
		}
	}
	if s.Next() {
		t.Error("unexpected extra row after sorted output")
	}
}

// TestSorterSpillMergeAndPeek triggers mergeSpilledRuns and peek via a full spill cycle.
func TestSorterSpillMergeAndPeek(t *testing.T) {
	t.Parallel()
	cfg := &SorterConfig{
		MaxMemoryBytes: 300,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	defer s.Close()

	const numRows = 20
	for i := int64(numRows); i > 0; i-- {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			t.Fatalf("Insert(%d) error: %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() < 2 {
		t.Errorf("expected >=2 spilled runs for merge coverage, got %d", s.GetNumSpilledRuns())
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort error: %v", err)
	}

	verifySortedSequence(t, s, numRows)
}

// TestRunReaderPeek exercises the peek() method on runReader directly.
func TestRunReaderPeek(t *testing.T) {
	t.Parallel()

	row1 := []*Mem{NewMemInt(1)}
	row2 := []*Mem{NewMemInt(2)}

	r := &runReader{
		rows:    [][]*Mem{row1, row2},
		current: 0,
	}

	// peek at first row
	if got := r.peek(); got == nil || got[0].IntValue() != 1 {
		t.Errorf("peek() = %v, want row with value 1", got)
	}

	// advance and peek second row
	r.next()
	if got := r.peek(); got == nil || got[0].IntValue() != 2 {
		t.Errorf("peek() after next() = %v, want row with value 2", got)
	}

	// advance past end — peek should return nil
	r.next()
	if got := r.peek(); got != nil {
		t.Errorf("peek() at EOF = %v, want nil", got)
	}
}
