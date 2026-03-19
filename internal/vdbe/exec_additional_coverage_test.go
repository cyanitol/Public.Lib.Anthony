// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestMemRealify tests Realify function (66.7% coverage)
func TestMemRealify(t *testing.T) {
	t.Parallel()
	t.Run("IntToReal", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(42)
		err := mem.Realify()
		if err != nil {
			t.Fatalf("Realify failed: %v", err)
		}
		if !mem.IsReal() {
			t.Error("Expected mem to be real")
		}
		if mem.RealValue() != 42.0 {
			t.Errorf("Expected 42.0, got %f", mem.RealValue())
		}
	})

	t.Run("StringToReal", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("3.14")
		err := mem.Realify()
		if err != nil {
			t.Fatalf("Realify failed: %v", err)
		}
		if !mem.IsReal() {
			t.Error("Expected mem to be real")
		}
	})

	t.Run("RealNoOp", func(t *testing.T) {
		t.Parallel()
		mem := NewMemReal(2.5)
		err := mem.Realify()
		if err != nil {
			t.Fatalf("Realify failed: %v", err)
		}
		if mem.RealValue() != 2.5 {
			t.Errorf("Expected 2.5, got %f", mem.RealValue())
		}
	})
}

// TestMemIntegerify tests Integerify function (72.7% coverage)
func TestMemIntegerify(t *testing.T) {
	t.Parallel()
	t.Run("RealToInt", func(t *testing.T) {
		t.Parallel()
		mem := NewMemReal(42.7)
		err := mem.Integerify()
		if err != nil {
			t.Fatalf("Integerify failed: %v", err)
		}
		if !mem.IsInt() {
			t.Error("Expected mem to be int")
		}
		if mem.IntValue() != 42 {
			t.Errorf("Expected 42, got %d", mem.IntValue())
		}
	})

	t.Run("StringToInt", func(t *testing.T) {
		t.Parallel()
		mem := NewMemStr("123")
		err := mem.Integerify()
		if err != nil {
			t.Fatalf("Integerify failed: %v", err)
		}
		if !mem.IsInt() {
			t.Error("Expected mem to be int")
		}
		if mem.IntValue() != 123 {
			t.Errorf("Expected 123, got %d", mem.IntValue())
		}
	})

	t.Run("IntNoOp", func(t *testing.T) {
		t.Parallel()
		mem := NewMemInt(99)
		err := mem.Integerify()
		if err != nil {
			t.Fatalf("Integerify failed: %v", err)
		}
		if mem.IntValue() != 99 {
			t.Errorf("Expected 99, got %d", mem.IntValue())
		}
	})
}

// TestExecBitNot tests execBitNot function (66.7% coverage)
func TestExecBitNot(t *testing.T) {
	t.Parallel()
	t.Run("BitNotInt", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(5) // Binary: 0101

		instr := &Instruction{Opcode: OpBitNot, P1: 0, P2: 1}
		err := v.execBitNot(instr)
		if err != nil {
			t.Fatalf("execBitNot failed: %v", err)
		}

		// ~5 = -6 (two's complement)
		if v.Mem[1].IntValue() != ^int64(5) {
			t.Errorf("Expected %d, got %d", ^int64(5), v.Mem[1].IntValue())
		}
	})

	t.Run("BitNotZero", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(0)

		instr := &Instruction{Opcode: OpBitNot, P1: 0, P2: 1}
		err := v.execBitNot(instr)
		if err != nil {
			t.Fatalf("execBitNot failed: %v", err)
		}

		if v.Mem[1].IntValue() != -1 {
			t.Errorf("Expected -1, got %d", v.Mem[1].IntValue())
		}
	})

	t.Run("BitNotNull", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetNull()

		instr := &Instruction{Opcode: OpBitNot, P1: 0, P2: 1}
		err := v.execBitNot(instr)
		if err != nil {
			t.Fatalf("execBitNot failed: %v", err)
		}

		if !v.Mem[1].IsNull() {
			t.Error("Expected NULL result for NULL input")
		}
	})
}

// TestExecBitOr tests execBitOr function (68.8% coverage)
func TestExecBitOr(t *testing.T) {
	t.Parallel()
	t.Run("BitOrInts", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(12) // Binary: 1100
		v.Mem[1].SetInt(10) // Binary: 1010

		// P3 = P1 | P2
		instr := &Instruction{Opcode: OpBitOr, P1: 0, P2: 1, P3: 2}
		err := v.execBitOr(instr)
		if err != nil {
			t.Fatalf("execBitOr failed: %v", err)
		}

		// 12 | 10 = 14 (1110)
		if v.Mem[2].IntValue() != 14 {
			t.Errorf("Expected 14, got %d", v.Mem[2].IntValue())
		}
	})

	t.Run("BitOrWithNull", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetInt(5)
		v.Mem[1].SetNull()

		instr := &Instruction{Opcode: OpBitOr, P1: 0, P2: 1, P3: 2}
		err := v.execBitOr(instr)
		if err != nil {
			t.Fatalf("execBitOr failed: %v", err)
		}

		if !v.Mem[2].IsNull() {
			t.Error("Expected NULL result when one operand is NULL")
		}
	})
}

// Note: getLogicalOperands is tested through execAnd and execOr tests

// Note: castToBlob is tested through OpCast tests

// TestMemToValue tests memToValue function (72.7% coverage)
func TestMemToValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mem      *Mem
		wantType string
	}{
		{
			name:     "IntMem",
			mem:      NewMemInt(42),
			wantType: "int64",
		},
		{
			name:     "RealMem",
			mem:      NewMemReal(3.14),
			wantType: "float64",
		},
		{
			name:     "StringMem",
			mem:      NewMemStr("test"),
			wantType: "string",
		},
		{
			name:     "BlobMem",
			mem:      NewMemBlob([]byte{1, 2, 3}),
			wantType: "[]byte",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val := memToValue(tt.mem)
			if tt.wantType == "nil" {
				if val != nil {
					t.Errorf("Expected nil, got %v", val)
				}
			} else {
				if val == nil {
					t.Error("Expected non-nil value")
				}
			}
		})
	}
}

// TestGetVarintGeneral tests getVarintGeneral function (70% coverage)
func TestGetVarintGeneral(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		data    []byte
		offset  int
		want    uint64
		wantN   int
		wantErr bool
	}{
		{
			name:   "SingleByte",
			data:   []byte{0x7F},
			offset: 0,
			want:   0x7F,
			wantN:  1,
		},
		{
			name:   "TwoBytes",
			data:   []byte{0x81, 0x00},
			offset: 0,
			want:   0x80,
			wantN:  2,
		},
		{
			name:   "ThreeBytes",
			data:   []byte{0x81, 0x81, 0x00},
			offset: 0,
			want:   0x4080,
			wantN:  3,
		},
		{
			name:    "EmptyData",
			data:    []byte{},
			offset:  0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, n := getVarintGeneral(tt.data, tt.offset)
			if tt.wantErr {
				if n > 0 {
					t.Error("Expected error (n=0)")
				}
			} else {
				if n != tt.wantN {
					t.Errorf("Expected n=%d, got n=%d", tt.wantN, n)
				}
				if val != tt.want {
					t.Errorf("Expected val=%d, got val=%d", tt.want, val)
				}
			}
		})
	}
}

// Note: varintLen is already tested in exec_helpers_test.go

// TestDecodeIntValue tests decodeIntValue function (71.4% coverage)
func TestDecodeIntValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		data   []byte
		offset int
		st     uint64
		want   int64
	}{
		{
			name:   "1Byte",
			data:   []byte{0x42},
			offset: 0,
			st:     1,
			want:   0x42,
		},
		{
			name:   "2Bytes",
			data:   []byte{0x12, 0x34},
			offset: 0,
			st:     2,
			want:   0x1234,
		},
		{
			name:   "4Bytes",
			data:   []byte{0x12, 0x34, 0x56, 0x78},
			offset: 0,
			st:     4,
			want:   0x12345678,
		},
		{
			name:   "8Bytes",
			data:   []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},
			offset: 0,
			st:     6,
			want:   0x123456789ABCDEF0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeIntValue(tt.data, tt.offset, tt.st)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("decodeIntValue() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestExecGoto tests execGoto function (75% coverage)
func TestExecGoto(t *testing.T) {
	t.Parallel()
	t.Run("UnconditionalJump", func(t *testing.T) {
		t.Parallel()
		v := New()
		// Add enough instructions to make jump valid
		for i := 0; i < 25; i++ {
			v.AddOp(OpNoop, 0, 0, 0)
		}
		v.PC = 5

		instr := &Instruction{Opcode: OpGoto, P2: 20}
		err := v.execGoto(instr)
		if err != nil {
			t.Fatalf("execGoto failed: %v", err)
		}

		if v.PC != 20 {
			t.Errorf("Expected PC=20, got PC=%d", v.PC)
		}
	})
}

// TestExecBlob tests execBlob function (75% coverage)
func TestExecBlob(t *testing.T) {
	t.Parallel()
	t.Run("SetBlobFromP4", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		data := []byte{1, 2, 3, 4, 5}

		instr := &Instruction{
			Opcode: OpBlob,
			P1:     len(data),
			P2:     0,
			P4:     P4Union{P: data},
			P4Type: P4Dynamic,
		}

		err := v.execBlob(instr)
		if err != nil {
			t.Fatalf("execBlob failed: %v", err)
		}

		if !v.Mem[0].IsBlob() {
			t.Error("Expected blob value")
		}

		blob := v.Mem[0].BlobValue()
		if len(blob) != len(data) {
			t.Errorf("Expected length %d, got %d", len(data), len(blob))
		}
	})
}

// TestExecMove tests execMove function (75% coverage)
func TestExecMove(t *testing.T) {
	t.Parallel()
	t.Run("MoveMultipleRegisters", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Mem[0].SetInt(1)
		v.Mem[1].SetInt(2)
		v.Mem[2].SetInt(3)

		// Move r0-r2 to r5-r7
		instr := &Instruction{Opcode: OpMove, P1: 0, P2: 5, P3: 3}
		err := v.execMove(instr)
		if err != nil {
			t.Fatalf("execMove failed: %v", err)
		}

		if v.Mem[5].IntValue() != 1 {
			t.Errorf("Expected r5=1, got %d", v.Mem[5].IntValue())
		}
		if v.Mem[6].IntValue() != 2 {
			t.Errorf("Expected r6=2, got %d", v.Mem[6].IntValue())
		}
		if v.Mem[7].IntValue() != 3 {
			t.Errorf("Expected r7=3, got %d", v.Mem[7].IntValue())
		}
	})
}

// TestExecSCopy tests execSCopy function (75% coverage)
func TestExecSCopy(t *testing.T) {
	t.Parallel()
	t.Run("ShallowCopy", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.Mem[0].SetStr("test string")

		instr := &Instruction{Opcode: OpSCopy, P1: 0, P2: 1}
		err := v.execSCopy(instr)
		if err != nil {
			t.Fatalf("execSCopy failed: %v", err)
		}

		if v.Mem[1].StrValue() != "test string" {
			t.Errorf("Expected 'test string', got '%s'", v.Mem[1].StrValue())
		}
	})
}

// TestExecVerifyCookie tests execVerifyCookie function (75% coverage)
func TestExecVerifyCookie(t *testing.T) {
	t.Parallel()
	t.Run("NoPager", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		// No context set

		instr := &Instruction{Opcode: OpVerifyCookie, P1: 0, P2: 1}
		err := v.execVerifyCookie(instr)
		if err == nil {
			t.Error("Expected error when no pager")
		}
	})
}

// Note: execCommit is already tested in exec_low_coverage_test.go
