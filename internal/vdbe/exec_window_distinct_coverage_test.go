// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// makeRows builds a [][]*Mem from int64 values (col 0) for window aggregate tests.
func makeIntRows(vals ...int64) [][]*Mem {
	rows := make([][]*Mem, len(vals))
	for i, v := range vals {
		rows[i] = []*Mem{NewMemInt(v)}
	}
	return rows
}

// makeNullRows builds rows where col 0 is NULL.
func makeNullRows(n int) [][]*Mem {
	rows := make([][]*Mem, n)
	for i := range rows {
		rows[i] = []*Mem{NewMemNull()}
	}
	return rows
}

// makeStrRows builds rows where col 0 is a string.
func makeStrRows(vals ...string) [][]*Mem {
	rows := make([][]*Mem, len(vals))
	for i, v := range vals {
		rows[i] = []*Mem{NewMemStr(v)}
	}
	return rows
}

// TestWindowAggCount covers windowAggCount.
func TestWindowAggCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rows [][]*Mem
		want int64
	}{
		{"empty", nil, 0},
		{"all null", makeNullRows(3), 0},
		{"all values", makeIntRows(1, 2, 3), 3},
		{"mixed null and value", func() [][]*Mem {
			return [][]*Mem{
				{NewMemNull()},
				{NewMemInt(5)},
				{NewMemNull()},
				{NewMemInt(7)},
			}
		}(), 2},
		{"out of bounds col", func() [][]*Mem {
			// col 1 doesn't exist in single-col rows
			return [][]*Mem{{NewMemInt(1)}, {NewMemInt(2)}}
		}(), 0}, // colIdx=1, rows only have col 0
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			colIdx := 0
			if tc.name == "out of bounds col" {
				colIdx = 1
			}
			windowAggCount(tc.rows, colIdx, out)
			if out.IntValue() != tc.want {
				t.Errorf("windowAggCount: got %d, want %d", out.IntValue(), tc.want)
			}
		})
	}
}

// TestWindowAggAvg covers windowAggAvg.
func TestWindowAggAvg(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		rows     [][]*Mem
		wantNull bool
		wantReal float64
	}{
		{"empty rows", nil, true, 0},
		{"all null", makeNullRows(3), true, 0},
		{"integers", makeIntRows(2, 4, 6), false, 4.0},
		{"single value", makeIntRows(10), false, 10.0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			windowAggAvg(tc.rows, 0, out)
			if tc.wantNull {
				if !out.IsNull() {
					t.Errorf("windowAggAvg: expected NULL, got %v", out)
				}
				return
			}
			if out.RealValue() != tc.wantReal {
				t.Errorf("windowAggAvg: got %v, want %v", out.RealValue(), tc.wantReal)
			}
		})
	}
}

// TestWindowAggMinMax covers windowAggMinMax and windowShouldReplace.
func TestWindowAggMinMax(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		rows     [][]*Mem
		isMin    bool
		wantNull bool
		wantInt  int64
	}{
		{"min empty", nil, true, true, 0},
		{"max empty", nil, false, true, 0},
		{"min all null", makeNullRows(2), true, true, 0},
		{"min values", makeIntRows(5, 3, 8, 1), true, false, 1},
		{"max values", makeIntRows(5, 3, 8, 1), false, false, 8},
		{"single value min", makeIntRows(42), true, false, 42},
		{"single value max", makeIntRows(42), false, false, 42},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			windowAggMinMax(tc.rows, 0, out, tc.isMin)
			if tc.wantNull {
				if !out.IsNull() {
					t.Errorf("windowAggMinMax %s: expected NULL", tc.name)
				}
				return
			}
			if out.IntValue() != tc.wantInt {
				t.Errorf("windowAggMinMax %s: got %d, want %d", tc.name, out.IntValue(), tc.wantInt)
			}
		})
	}
}

// TestWindowShouldReplace covers windowShouldReplace directly.
func TestWindowShouldReplace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		val   *Mem
		best  *Mem
		isMin bool
		want  bool
	}{
		{"min smaller replaces", NewMemInt(1), NewMemInt(5), true, true},
		{"min larger no replace", NewMemInt(9), NewMemInt(5), true, false},
		{"min equal no replace", NewMemInt(5), NewMemInt(5), true, false},
		{"max larger replaces", NewMemInt(9), NewMemInt(5), false, true},
		{"max smaller no replace", NewMemInt(1), NewMemInt(5), false, false},
		{"max equal no replace", NewMemInt(5), NewMemInt(5), false, false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := windowShouldReplace(tc.val, tc.best, tc.isMin)
			if got != tc.want {
				t.Errorf("windowShouldReplace %s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestWindowAggTotal covers windowAggTotal.
func TestWindowAggTotal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rows [][]*Mem
		want float64
	}{
		{"empty rows", nil, 0.0},
		{"all null returns 0", makeNullRows(3), 0.0},
		{"sum values", makeIntRows(1, 2, 3, 4), 10.0},
		{"mixed null", func() [][]*Mem {
			return [][]*Mem{{NewMemNull()}, {NewMemInt(5)}, {NewMemInt(3)}}
		}(), 8.0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			windowAggTotal(tc.rows, 0, out)
			if out.RealValue() != tc.want {
				t.Errorf("windowAggTotal %s: got %v, want %v", tc.name, out.RealValue(), tc.want)
			}
		})
	}
}

// TestWindowAggGroupConcat covers windowAggGroupConcat.
func TestWindowAggGroupConcat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		rows     [][]*Mem
		sep      string
		wantNull bool
		wantStr  string
	}{
		{"empty rows", nil, ",", true, ""},
		{"all null", makeNullRows(2), ",", true, ""},
		{"single value", makeStrRows("hello"), ",", false, "hello"},
		{"multiple values", makeStrRows("a", "b", "c"), ",", false, "a,b,c"},
		{"custom sep", makeStrRows("x", "y"), "|", false, "x|y"},
		{"mixed null and str", func() [][]*Mem {
			return [][]*Mem{{NewMemNull()}, {NewMemStr("foo")}, {NewMemNull()}, {NewMemStr("bar")}}
		}(), "-", false, "foo-bar"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			windowAggGroupConcat(tc.rows, 0, out, tc.sep)
			if tc.wantNull {
				if !out.IsNull() {
					t.Errorf("windowAggGroupConcat %s: expected NULL", tc.name)
				}
				return
			}
			if out.StringValue() != tc.wantStr {
				t.Errorf("windowAggGroupConcat %s: got %q, want %q", tc.name, out.StringValue(), tc.wantStr)
			}
		})
	}
}

// TestWindowAggDispatch covers windowAggDispatch including the default branch.
func TestWindowAggDispatch(t *testing.T) {
	t.Parallel()
	rows := makeIntRows(2, 4, 6)
	tests := []struct {
		funcName string
		wantNull bool
		check    func(*Mem) bool
	}{
		{"COUNT", false, func(m *Mem) bool { return m.IntValue() == 3 }},
		{"AVG", false, func(m *Mem) bool { return m.RealValue() == 4.0 }},
		{"MIN", false, func(m *Mem) bool { return m.IntValue() == 2 }},
		{"MAX", false, func(m *Mem) bool { return m.IntValue() == 6 }},
		{"TOTAL", false, func(m *Mem) bool { return m.RealValue() == 12.0 }},
		{"GROUP_CONCAT", false, func(m *Mem) bool { return !m.IsNull() }},
		{"UNKNOWN_FUNC", true, nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.funcName, func(t *testing.T) {
			t.Parallel()
			out := NewMem()
			err := windowAggDispatch(tc.funcName, rows, 0, out)
			if err != nil {
				t.Fatalf("windowAggDispatch %s: unexpected error: %v", tc.funcName, err)
			}
			if tc.wantNull {
				if !out.IsNull() {
					t.Errorf("windowAggDispatch %s: expected NULL output", tc.funcName)
				}
				return
			}
			if !tc.check(out) {
				t.Errorf("windowAggDispatch %s: check failed, got %v", tc.funcName, out)
			}
		})
	}
}

// makeVDBEWithWindow creates a VDBE with a populated window state at index wsIdx.
func makeVDBEWithWindow(wsIdx int, rows [][]*Mem) *VDBE {
	v := New()
	_ = v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)
	ws := NewWindowState(nil, nil, nil, EntirePartitionFrame())
	for _, row := range rows {
		ws.AddRow(row)
	}
	v.WindowStates[wsIdx] = ws
	// Set up program to make jumpIfValid range checks work
	v.Program = make([]*Instruction, 5)
	return v
}

// TestSetWindowDefault covers setWindowDefault.
func TestSetWindowDefault(t *testing.T) {
	t.Parallel()

	t.Run("defaultReg zero sets null", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(5)
		mem := NewMem()
		mem.SetInt(99)
		err := v.setWindowDefault(mem, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !mem.IsNull() {
			t.Errorf("expected NULL when defaultReg=0, got %v", mem)
		}
	})

	t.Run("defaultReg nonzero copies from register", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(5)
		// Put a value into register 2
		src, _ := v.GetMem(2)
		src.SetInt(777)
		// setWindowDefault with defaultReg=2 should copy register 2 into mem
		dst := NewMem()
		err := v.setWindowDefault(dst, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dst.IntValue() != 777 {
			t.Errorf("expected 777, got %d", dst.IntValue())
		}
	})

	t.Run("defaultReg out of range returns error", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(3)
		mem := NewMem()
		err := v.setWindowDefault(mem, 99)
		if err == nil {
			t.Error("expected error for out-of-range defaultReg, got nil")
		}
	})
}

// TestEnsureDistinctSet covers ensureDistinctSet.
func TestEnsureDistinctSet(t *testing.T) {
	t.Parallel()

	t.Run("nil DistinctSets initialized", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.DistinctSets = nil
		v.ensureDistinctSet(42)
		if v.DistinctSets == nil {
			t.Error("DistinctSets should not be nil after ensureDistinctSet")
		}
		if v.DistinctSets[42] == nil {
			t.Error("set for key 42 should not be nil")
		}
	})

	t.Run("existing sets not overwritten", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.DistinctSets = make(map[int]map[string]bool)
		v.DistinctSets[1] = map[string]bool{"x": true}
		v.ensureDistinctSet(1)
		if !v.DistinctSets[1]["x"] {
			t.Error("existing entry in set should be preserved")
		}
	})

	t.Run("new set created for missing key", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.DistinctSets = make(map[int]map[string]bool)
		v.ensureDistinctSet(7)
		if v.DistinctSets[7] == nil {
			t.Error("set for key 7 should be created")
		}
	})
}

// TestJumpIfValid covers jumpIfValid.
func TestJumpIfValid(t *testing.T) {
	t.Parallel()
	v := New()
	v.Program = make([]*Instruction, 5)

	tests := []struct {
		name   string
		addr   int
		initPC int
		wantPC int
	}{
		{"valid jump", 3, 0, 3},
		{"negative addr no jump", -1, 0, 0},
		{"addr equals len no jump", 5, 0, 0},
		{"addr past len no jump", 10, 0, 0},
		{"addr zero valid", 0, 2, 0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v.PC = tc.initPC
			v.jumpIfValid(tc.addr)
			if v.PC != tc.wantPC {
				t.Errorf("jumpIfValid(%d): PC=%d, want %d", tc.addr, v.PC, tc.wantPC)
			}
		})
	}
}

// TestBuildDistinctKey covers buildDistinctKey.
func TestBuildDistinctKey(t *testing.T) {
	t.Parallel()

	t.Run("single column", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(5)
		m, _ := v.GetMem(0)
		m.SetInt(42)
		key, err := v.buildDistinctKey(0, 1, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key == "" {
			t.Error("expected non-empty key")
		}
	})

	t.Run("multi column produces separator", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(5)
		m0, _ := v.GetMem(0)
		m0.SetInt(1)
		m1, _ := v.GetMem(1)
		m1.SetInt(2)
		key, err := v.buildDistinctKey(0, 2, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Key must contain the null-byte separator
		found := false
		for _, b := range key {
			if b == '\x00' {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("multi-column key should contain separator, got %q", key)
		}
	})

	t.Run("out of range register returns error", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(2)
		_, err := v.buildDistinctKey(100, 1, nil)
		if err == nil {
			t.Error("expected error for out-of-range register")
		}
	})
}

// TestExecAggDistinct covers execAggDistinct: new value proceeds, duplicate jumps.
func TestExecAggDistinct(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(5)
	v.Program = make([]*Instruction, 10)

	// Put a value into register 1
	m, _ := v.GetMem(1)
	m.SetInt(99)

	instr := &Instruction{
		P1: 1, // input reg
		P2: 5, // jump addr (duplicate seen → jump to 5)
		P3: 0, // agg reg (key for set)
	}

	// First call: value is new, PC should NOT jump to 5
	v.PC = 0
	err := v.execAggDistinct(instr)
	if err != nil {
		t.Fatalf("first execAggDistinct: %v", err)
	}
	if v.PC == 5 {
		t.Error("first call: PC should not have jumped for a new value")
	}

	// Second call: same value, already seen → PC should jump to 5
	v.PC = 0
	err = v.execAggDistinct(instr)
	if err != nil {
		t.Fatalf("second execAggDistinct: %v", err)
	}
	if v.PC != 5 {
		t.Errorf("second call: PC should be 5 (duplicate), got %d", v.PC)
	}
}

// TestExecDistinctRow covers execDistinctRow: new row proceeds, duplicate jumps.
func TestExecDistinctRow(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(5)
	v.Program = make([]*Instruction, 10)

	// Set up registers 0 and 1 with values
	m0, _ := v.GetMem(0)
	m0.SetInt(1)
	m1, _ := v.GetMem(1)
	m1.SetInt(2)

	instr := &Instruction{
		P1: 0, // first reg
		P2: 7, // jump addr for duplicate
		P3: 2, // num cols
	}

	// First call: row is new
	v.PC = 0
	err := v.execDistinctRow(instr)
	if err != nil {
		t.Fatalf("first execDistinctRow: %v", err)
	}
	if v.PC == 7 {
		t.Error("first call: should not jump for new row")
	}

	// Second call: same row → duplicate → jump
	v.PC = 0
	err = v.execDistinctRow(instr)
	if err != nil {
		t.Fatalf("second execDistinctRow: %v", err)
	}
	if v.PC != 7 {
		t.Errorf("second call: PC should be 7, got %d", v.PC)
	}
}

// TestExecDistinctRowWithCollation covers execDistinctRow with P4 collations.
func TestExecDistinctRowWithCollation(t *testing.T) {
	t.Parallel()
	v := New()
	_ = v.AllocMemory(5)
	v.Program = make([]*Instruction, 10)

	m0, _ := v.GetMem(0)
	m0.SetStr("hello")

	instr := &Instruction{
		P1: 0,
		P2: 3,
		P3: 1,
		P4: P4Union{P: []string{"NOCASE"}},
		P5: 0,
	}

	v.PC = 0
	if err := v.execDistinctRow(instr); err != nil {
		t.Fatalf("execDistinctRow with collation: %v", err)
	}
}

// TestExecWindowPercentRank covers execWindowPercentRank via the helper,
// exercising partSize <= 1 and partSize > 1 branches.
func TestExecWindowPercentRank(t *testing.T) {
	t.Parallel()

	t.Run("single row partition returns 0", func(t *testing.T) {
		t.Parallel()
		v := makeVDBEWithWindow(0, makeIntRows(10))
		ws := v.WindowStates[0]
		ws.NextRow()

		instr := &Instruction{P1: 0}
		err := v.execWindowPercentRank(instr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out, _ := v.GetMem(0)
		if out.RealValue() != 0.0 {
			t.Errorf("single row: expected 0.0, got %v", out.RealValue())
		}
	})

	t.Run("multi row partition", func(t *testing.T) {
		t.Parallel()
		v := makeVDBEWithWindow(0, makeIntRows(10, 20, 30))
		ws := v.WindowStates[0]
		ws.CurrentPartIdx = 0
		ws.CurrentPartRow = 1 // second row (0-based)

		instr := &Instruction{P1: 1}
		err := v.execWindowPercentRank(instr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out, _ := v.GetMem(1)
		// rank starts at 1; after IncrementPartRowOnFirstCall it becomes 2
		// partSize=3, result = (rank-1)/(partSize-1)
		if out.IsNull() {
			t.Error("expected non-null result for multi-row partition")
		}
	})
}

// TestExecWindowCumeDist covers execWindowCumeDist,
// exercising partSize == 0 and > 0 branches.
func TestExecWindowCumeDist(t *testing.T) {
	t.Parallel()

	t.Run("empty partition returns 0", func(t *testing.T) {
		t.Parallel()
		v := New()
		_ = v.AllocMemory(5)
		v.Program = make([]*Instruction, 5)
		v.WindowStates = make(map[int]*WindowState)
		ws := NewWindowState(nil, nil, nil, EntirePartitionFrame())
		// No rows → CurrentPartIdx stays -1 → GetPartitionSize() returns 0
		v.WindowStates[0] = ws

		instr := &Instruction{P1: 0}
		err := v.execWindowCumeDist(instr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out, _ := v.GetMem(0)
		if out.RealValue() != 0.0 {
			t.Errorf("empty partition: expected 0.0, got %v", out.RealValue())
		}
	})

	t.Run("non-empty partition", func(t *testing.T) {
		t.Parallel()
		v := makeVDBEWithWindow(0, makeIntRows(1, 2, 3))
		ws := v.WindowStates[0]
		ws.CurrentPartIdx = 0
		ws.CurrentPartRow = 0
		ws.CurrentRank = 0
		ws.RowsAtCurrentRank = 2

		instr := &Instruction{P1: 2}
		err := v.execWindowCumeDist(instr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out, _ := v.GetMem(2)
		if out.IsNull() {
			t.Error("expected non-null for non-empty partition")
		}
	})
}
