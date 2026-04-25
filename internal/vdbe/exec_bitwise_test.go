// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestBitAnd tests the OpBitAnd instruction
func TestBitAnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     int64
		right    int64
		expected int64
	}{
		{"basic and", 12, 10, 8},           // 1100 & 1010 = 1000
		{"all bits set", 15, 15, 15},       // 1111 & 1111 = 1111
		{"no common bits", 8, 4, 0},        // 1000 & 0100 = 0000
		{"zero operand", 255, 0, 0},        // any & 0 = 0
		{"negative numbers", -1, 127, 127}, // all bits & 0111...1111
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			// Set up operands
			v.Mem[1].SetInt(tt.left)
			v.Mem[2].SetInt(tt.right)

			// Execute BitAnd: r3 = r1 & r2
			instr := &Instruction{
				Opcode: OpBitAnd,
				P1:     1,
				P2:     2,
				P3:     3,
			}

			err := v.execBitAnd(instr)
			if err != nil {
				t.Fatalf("execBitAnd failed: %v", err)
			}

			if !v.Mem[3].IsInt() {
				t.Errorf("result is not an integer")
			}

			result := v.Mem[3].IntValue()
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestBitAndNull tests NULL propagation for OpBitAnd
func TestBitAndNull(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	// Test NULL & value = NULL
	v.Mem[1].SetNull()
	v.Mem[2].SetInt(42)

	instr := &Instruction{
		Opcode: OpBitAnd,
		P1:     1,
		P2:     2,
		P3:     3,
	}

	err := v.execBitAnd(instr)
	if err != nil {
		t.Fatalf("execBitAnd failed: %v", err)
	}

	if !v.Mem[3].IsNull() {
		t.Errorf("expected NULL result, got %v", v.Mem[3].Value())
	}

	// Test value & NULL = NULL
	v.Mem[1].SetInt(42)
	v.Mem[2].SetNull()

	err = v.execBitAnd(instr)
	if err != nil {
		t.Fatalf("execBitAnd failed: %v", err)
	}

	if !v.Mem[3].IsNull() {
		t.Errorf("expected NULL result, got %v", v.Mem[3].Value())
	}
}

// TestBitOr tests the OpBitOr instruction
func TestBitOr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     int64
		right    int64
		expected int64
	}{
		{"basic or", 12, 10, 14},        // 1100 | 1010 = 1110
		{"no bits set", 0, 0, 0},        // 0000 | 0000 = 0000
		{"one zero", 15, 0, 15},         // 1111 | 0000 = 1111
		{"combine bits", 8, 4, 12},      // 1000 | 0100 = 1100
		{"negative numbers", -1, 0, -1}, // all bits set
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			v.Mem[1].SetInt(tt.left)
			v.Mem[2].SetInt(tt.right)

			instr := &Instruction{
				Opcode: OpBitOr,
				P1:     1,
				P2:     2,
				P3:     3,
			}

			err := v.execBitOr(instr)
			if err != nil {
				t.Fatalf("execBitOr failed: %v", err)
			}

			if !v.Mem[3].IsInt() {
				t.Errorf("result is not an integer")
			}

			result := v.Mem[3].IntValue()
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestBitNot tests the OpBitNot instruction
func TestBitNot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    int64
		expected int64
	}{
		{"zero", 0, -1},
		{"all ones", -1, 0},
		{"positive", 15, -16}, // ~0000...1111 = 1111...0000
		{"negative", -16, 15}, // ~1111...0000 = 0000...1111
		{"one", 1, -2},        // ~0000...0001 = 1111...1110
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			v.Mem[1].SetInt(tt.value)

			instr := &Instruction{
				Opcode: OpBitNot,
				P1:     1,
				P2:     2,
			}

			err := v.execBitNot(instr)
			if err != nil {
				t.Fatalf("execBitNot failed: %v", err)
			}

			if !v.Mem[2].IsInt() {
				t.Errorf("result is not an integer")
			}

			result := v.Mem[2].IntValue()
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestShiftLeft tests the OpShiftLeft instruction
func TestShiftLeft(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    int64
		shift    int64
		expected int64
	}{
		{"basic shift", 1, 3, 8},          // 1 << 3 = 8
		{"shift by zero", 42, 0, 42},      // 42 << 0 = 42
		{"large shift", 1, 10, 1024},      // 1 << 10 = 1024
		{"negative shift", 8, -1, 0},      // negative shift = 0
		{"shift >= 64", 1, 64, 0},         // shift too large = 0
		{"shift multiple bits", 5, 2, 20}, // 0101 << 2 = 10100
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			v.Mem[1].SetInt(tt.shift)
			v.Mem[2].SetInt(tt.value)

			instr := &Instruction{
				Opcode: OpShiftLeft,
				P1:     1, // shift amount
				P2:     2, // value to shift
				P3:     3, // result
			}

			err := v.execShiftLeft(instr)
			if err != nil {
				t.Fatalf("execShiftLeft failed: %v", err)
			}

			if !v.Mem[3].IsInt() {
				t.Errorf("result is not an integer")
			}

			result := v.Mem[3].IntValue()
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestShiftRight tests the OpShiftRight instruction
func TestShiftRight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    int64
		shift    int64
		expected int64
	}{
		{"basic shift", 8, 3, 1},               // 8 >> 3 = 1
		{"shift by zero", 42, 0, 42},           // 42 >> 0 = 42
		{"shift to zero", 7, 3, 0},             // 0111 >> 3 = 0000
		{"negative shift", 8, -1, 0},           // negative shift = 0
		{"shift >= 64 positive", 100, 64, 0},   // large shift of positive = 0
		{"shift >= 64 negative", -100, 64, -1}, // large shift of negative = -1
		{"negative value", -8, 2, -2},          // arithmetic shift preserves sign
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			v.Mem[1].SetInt(tt.shift)
			v.Mem[2].SetInt(tt.value)

			instr := &Instruction{
				Opcode: OpShiftRight,
				P1:     1, // shift amount
				P2:     2, // value to shift
				P3:     3, // result
			}

			err := v.execShiftRight(instr)
			if err != nil {
				t.Fatalf("execShiftRight failed: %v", err)
			}

			if !v.Mem[3].IsInt() {
				t.Errorf("result is not an integer")
			}

			result := v.Mem[3].IntValue()
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func setLogicalOperand(m *Mem, val interface{}) {
	if val == nil {
		m.SetNull()
	} else {
		m.SetInt(int64(val.(int)))
	}
}

func checkLogicalResult(t *testing.T, m *Mem, expectedNull bool, expectedInt int64) {
	t.Helper()
	if expectedNull {
		if !m.IsNull() {
			t.Errorf("expected NULL result, got %v", m.Value())
		}
		return
	}
	if !m.IsInt() {
		t.Errorf("result is not an integer")
	}
	if m.IntValue() != expectedInt {
		t.Errorf("expected %d, got %d", expectedInt, m.IntValue())
	}
}

// TestLogicalAnd tests the OpAnd instruction
func TestLogicalAnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		left         interface{}
		right        interface{}
		expectedInt  int64
		expectedNull bool
	}{
		{"true and true", 1, 1, 1, false},
		{"true and false", 1, 0, 0, false},
		{"false and true", 0, 1, 0, false},
		{"false and false", 0, 0, 0, false},
		{"non-zero and non-zero", 5, 7, 1, false},
		{"null and true", nil, 1, 0, true},
		{"true and null", 1, nil, 0, true},
		{"null and false", nil, 0, 0, false},
		{"false and null", 0, nil, 0, false},
		{"null and null", nil, nil, 0, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)
			setLogicalOperand(v.Mem[1], tt.left)
			setLogicalOperand(v.Mem[2], tt.right)

			err := v.execAnd(&Instruction{Opcode: OpAnd, P1: 1, P2: 2, P3: 3})
			if err != nil {
				t.Fatalf("execAnd failed: %v", err)
			}
			checkLogicalResult(t, v.Mem[3], tt.expectedNull, tt.expectedInt)
		})
	}
}

// TestLogicalOr tests the OpOr instruction
func TestLogicalOr(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		left         interface{}
		right        interface{}
		expectedInt  int64
		expectedNull bool
	}{
		{"true or true", 1, 1, 1, false},
		{"true or false", 1, 0, 1, false},
		{"false or true", 0, 1, 1, false},
		{"false or false", 0, 0, 0, false},
		{"non-zero or zero", 5, 0, 1, false},
		{"null or true", nil, 1, 1, false},
		{"true or null", 1, nil, 1, false},
		{"null or false", nil, 0, 0, true},
		{"false or null", 0, nil, 0, true},
		{"null or null", nil, nil, 0, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)
			setLogicalOperand(v.Mem[1], tt.left)
			setLogicalOperand(v.Mem[2], tt.right)

			err := v.execOr(&Instruction{Opcode: OpOr, P1: 1, P2: 2, P3: 3})
			if err != nil {
				t.Fatalf("execOr failed: %v", err)
			}
			checkLogicalResult(t, v.Mem[3], tt.expectedNull, tt.expectedInt)
		})
	}
}

// TestLogicalNot tests the OpNot instruction
func TestLogicalNot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		value        interface{}
		expectedInt  int64
		expectedNull bool
	}{
		{"not true", 1, 0, false},
		{"not false", 0, 1, false},
		{"not non-zero", 42, 0, false},
		{"not null", nil, 0, true},
		{"not negative", -5, 0, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(10)

			if tt.value == nil {
				v.Mem[1].SetNull()
			} else {
				v.Mem[1].SetInt(int64(tt.value.(int)))
			}

			instr := &Instruction{
				Opcode: OpNot,
				P1:     1,
				P2:     2,
			}

			err := v.execNot(instr)
			if err != nil {
				t.Fatalf("execNot failed: %v", err)
			}

			if tt.expectedNull {
				if !v.Mem[2].IsNull() {
					t.Errorf("expected NULL result, got %v", v.Mem[2].Value())
				}
			} else {
				if !v.Mem[2].IsInt() {
					t.Errorf("result is not an integer")
				}

				result := v.Mem[2].IntValue()
				if result != tt.expectedInt {
					t.Errorf("expected %d, got %d", tt.expectedInt, result)
				}
			}
		})
	}
}

// NewTestVDBE creates a VDBE instance for testing with the specified number of memory cells
func NewTestVDBE(numMem int) *VDBE {
	v := &VDBE{
		State:   StateReady,
		Mem:     make([]*Mem, numMem),
		Program: make([]*Instruction, 0),
	}

	// Initialize memory cells
	for i := 0; i < numMem; i++ {
		v.Mem[i] = NewMem()
	}

	return v
}
