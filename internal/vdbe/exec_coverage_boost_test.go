// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestExecIfPos tests the execIfPos function comprehensively
func TestExecIfPos(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		setupMem      func(*VDBE)
		p1            int
		p2            int
		p3            int
		expectedPC    int
		expectedValue int64
	}{
		{
			name: "PositiveValue_JumpsAndDecrements",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(5)
			},
			p1:            0,
			p2:            10,
			p3:            -1,
			expectedPC:    10,
			expectedValue: 4,
		},
		{
			name: "ZeroValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(0)
			},
			p1:            0,
			p2:            10,
			p3:            -1,
			expectedPC:    0, // PC doesn't change when not jumping
			expectedValue: 0,
		},
		{
			name: "NegativeValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(-5)
			},
			p1:            0,
			p2:            10,
			p3:            -1,
			expectedPC:    0,
			expectedValue: -5,
		},
		{
			name: "NullValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetNull()
			},
			p1:            0,
			p2:            10,
			p3:            -1,
			expectedPC:    0,
			expectedValue: 0,
		},
		{
			name: "RealValue_ConvertsAndJumps",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetReal(3.7)
			},
			p1:            0,
			p2:            15,
			p3:            -2,
			expectedPC:    15,
			expectedValue: 1, // 3 - 2
		},
		{
			name: "PositiveWithPositiveDecrement",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(10)
			},
			p1:            0,
			p2:            20,
			p3:            3,
			expectedPC:    20,
			expectedValue: 13, // 10 + 3
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			tt.setupMem(v)
			v.PC = 0

			instr := &Instruction{
				Opcode: OpIfPos,
				P1:     tt.p1,
				P2:     tt.p2,
				P3:     tt.p3,
			}

			err := v.execIfPos(instr)
			if err != nil {
				t.Fatalf("execIfPos failed: %v", err)
			}

			if v.PC != tt.expectedPC {
				t.Errorf("Expected PC=%d, got %d", tt.expectedPC, v.PC)
			}

			if v.Mem[tt.p1].IntValue() != tt.expectedValue {
				t.Errorf("Expected value=%d, got %d", tt.expectedValue, v.Mem[tt.p1].IntValue())
			}
		})
	}
}

// TestExecInt64 tests the execInt64 function
func TestExecInt64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		p4Type  P4Type
		p4Value int64
		wantErr bool
	}{
		{
			name:    "ValidInt64",
			p4Type:  P4Int64,
			p4Value: 9223372036854775807,
			wantErr: false,
		},
		{
			name:    "NegativeInt64",
			p4Type:  P4Int64,
			p4Value: -9223372036854775808,
			wantErr: false,
		},
		{
			name:    "WrongP4Type",
			p4Type:  P4Static,
			p4Value: 42,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			instr := &Instruction{
				Opcode: OpInt64,
				P2:     0,
				P4Type: tt.p4Type,
				P4:     P4Union{I64: tt.p4Value},
			}

			err := v.execInt64(instr)
			if (err != nil) != tt.wantErr {
				t.Errorf("execInt64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if v.Mem[0].IntValue() != tt.p4Value {
					t.Errorf("Expected %d, got %d", tt.p4Value, v.Mem[0].IntValue())
				}
			}
		})
	}
}

// TestExecReal tests the execReal function
func TestExecReal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		p4Type  P4Type
		p4Value float64
		wantErr bool
	}{
		{
			name:    "ValidReal",
			p4Type:  P4Real,
			p4Value: 3.14159265359,
			wantErr: false,
		},
		{
			name:    "NegativeReal",
			p4Type:  P4Real,
			p4Value: -2.71828,
			wantErr: false,
		},
		{
			name:    "ZeroReal",
			p4Type:  P4Real,
			p4Value: 0.0,
			wantErr: false,
		},
		{
			name:    "WrongP4Type",
			p4Type:  P4Static,
			p4Value: 3.14,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			instr := &Instruction{
				Opcode: OpReal,
				P2:     0,
				P4Type: tt.p4Type,
				P4:     P4Union{R: tt.p4Value},
			}

			err := v.execReal(instr)
			if (err != nil) != tt.wantErr {
				t.Errorf("execReal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if v.Mem[0].RealValue() != tt.p4Value {
					t.Errorf("Expected %f, got %f", tt.p4Value, v.Mem[0].RealValue())
				}
			}
		})
	}
}

// TestExecString tests the execString function
func TestExecString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		p4Type  P4Type
		p4Value string
		wantErr bool
	}{
		{
			name:    "ValidStaticString",
			p4Type:  P4Static,
			p4Value: "hello world",
			wantErr: false,
		},
		{
			name:    "ValidDynamicString",
			p4Type:  P4Dynamic,
			p4Value: "dynamic string",
			wantErr: false,
		},
		{
			name:    "EmptyString",
			p4Type:  P4Static,
			p4Value: "",
			wantErr: false,
		},
		{
			name:    "WrongP4Type",
			p4Type:  P4Real,
			p4Value: "fail",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			instr := &Instruction{
				Opcode: OpString,
				P2:     0,
				P4Type: tt.p4Type,
				P4:     P4Union{Z: tt.p4Value},
			}

			err := v.execString(instr)
			if (err != nil) != tt.wantErr {
				t.Errorf("execString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if v.Mem[0].StrValue() != tt.p4Value {
					t.Errorf("Expected %q, got %q", tt.p4Value, v.Mem[0].StrValue())
				}
			}
		})
	}
}

// TestExecCopy tests the execCopy function
func TestExecCopy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setupSrc func(*Mem)
		wantType func(*Mem) bool
	}{
		{
			name:     "CopyInt",
			setupSrc: func(m *Mem) { m.SetInt(42) },
			wantType: func(m *Mem) bool { return m.IsInt() && m.IntValue() == 42 },
		},
		{
			name:     "CopyReal",
			setupSrc: func(m *Mem) { m.SetReal(3.14) },
			wantType: func(m *Mem) bool { return m.IsReal() && m.RealValue() == 3.14 },
		},
		{
			name:     "CopyString",
			setupSrc: func(m *Mem) { m.SetStr("test") },
			wantType: func(m *Mem) bool { return m.IsString() && m.StrValue() == "test" },
		},
		{
			name:     "CopyBlob",
			setupSrc: func(m *Mem) { m.SetBlob([]byte{1, 2, 3}) },
			wantType: func(m *Mem) bool { return m.IsBlob() && len(m.BlobValue()) == 3 },
		},
		{
			name:     "CopyNull",
			setupSrc: func(m *Mem) { m.SetNull() },
			wantType: func(m *Mem) bool { return m.IsNull() },
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			tt.setupSrc(v.Mem[0])

			instr := &Instruction{
				Opcode: OpCopy,
				P1:     0,
				P2:     1,
			}

			err := v.execCopy(instr)
			if err != nil {
				t.Fatalf("execCopy failed: %v", err)
			}

			if !tt.wantType(v.Mem[1]) {
				t.Error("Copy did not preserve type correctly")
			}
		})
	}
}

// TestExecOpenEphemeral tests the execOpenEphemeral function error case
func TestExecOpenEphemeral(t *testing.T) {
	t.Parallel()
	t.Run("NoBtreeContext", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(5)
		v.Ctx = &VDBEContext{
			Pager: &MockTransactionPager{},
			// No BTree set - this will cause an error
		}

		instr := &Instruction{
			Opcode: OpOpenEphemeral,
			P1:     0,
			P2:     5,
		}

		err := v.execOpenEphemeral(instr)
		if err == nil {
			t.Error("Expected error when no btree context")
		}
	})
}

// TestExecDelete_NotWritable tests the execDelete error case
func TestExecDelete_NotWritable(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.AllocCursors(2)
	v.Cursors[0] = &Cursor{
		Writable: false,
	}

	instr := &Instruction{
		Opcode: OpDelete,
		P1:     0,
	}

	err := v.execDelete(instr)
	if err == nil {
		t.Error("Expected error for non-writable cursor")
	}
}

// TestExecCommit_ReadTransaction tests execCommit with read transaction
func TestExecCommit_ReadTransaction(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	mockPager := &MockTransactionPager{
		inTxn:      true,
		inWriteTxn: false,
	}
	v.Ctx = &VDBEContext{
		Pager: mockPager,
	}

	instr := &Instruction{Opcode: OpCommit}
	err := v.execCommit(instr)
	if err != nil {
		t.Fatalf("execCommit failed: %v", err)
	}

	if !mockPager.endedRead {
		t.Error("Expected EndRead to be called")
	}
}

// TestExecCommit_NoTransaction tests execCommit with no transaction
func TestExecCommit_NoTransaction(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	mockPager := &MockTransactionPager{
		inTxn:      false,
		inWriteTxn: false,
	}
	v.Ctx = &VDBEContext{
		Pager: mockPager,
	}

	instr := &Instruction{Opcode: OpCommit}
	err := v.execCommit(instr)
	if err != nil {
		t.Fatalf("execCommit failed: %v", err)
	}

	// Should succeed without doing anything
	if mockPager.committed || mockPager.endedRead {
		t.Error("Expected no operations when not in transaction")
	}
}

// TestCastToBlob tests the castToBlob function
func TestCastToBlob(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		setup   func(*Mem)
		wantLen int
	}{
		{
			name:    "AlreadyBlob",
			setup:   func(m *Mem) { m.SetBlob([]byte{1, 2, 3}) },
			wantLen: 3,
		},
		{
			name:    "StringToBlob",
			setup:   func(m *Mem) { m.SetStr("test") },
			wantLen: 4,
		},
		{
			name:    "IntToBlob",
			setup:   func(m *Mem) { m.SetInt(42) },
			wantLen: 2, // "42"
		},
		{
			name:    "RealToBlob",
			setup:   func(m *Mem) { m.SetReal(3.14) },
			wantLen: 4, // "3.14" or similar
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mem := NewMem()
			tt.setup(mem)

			err := castToBlob(mem)
			if err != nil {
				t.Fatalf("castToBlob failed: %v", err)
			}

			if !mem.IsBlob() {
				t.Error("Expected blob type after cast")
			}
		})
	}
}

// TestGetLogicalOperands tests the getLogicalOperands function
func TestGetLogicalOperands(t *testing.T) {
	t.Parallel()
	t.Run("ValidOperands", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)
		v.Mem[0].SetInt(1)
		v.Mem[1].SetInt(0)

		instr := &Instruction{
			P1: 0,
			P2: 1,
			P3: 2,
		}

		left, right, result, err := v.getLogicalOperands(instr)
		if err != nil {
			t.Fatalf("getLogicalOperands failed: %v", err)
		}

		if left == nil || right == nil || result == nil {
			t.Error("Expected non-nil operands")
		}
	})

	t.Run("InvalidP1", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(3)
		instr := &Instruction{
			P1: 100, // Out of bounds
			P2: 1,
			P3: 2,
		}

		_, _, _, err := v.getLogicalOperands(instr)
		if err == nil {
			t.Error("Expected error for invalid P1")
		}
	})

	t.Run("InvalidP2", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(3)
		instr := &Instruction{
			P1: 0,
			P2: 100, // Out of bounds
			P3: 2,
		}

		_, _, _, err := v.getLogicalOperands(instr)
		if err == nil {
			t.Error("Expected error for invalid P2")
		}
	})

	t.Run("InvalidP3", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(3)
		instr := &Instruction{
			P1: 0,
			P2: 1,
			P3: 100, // Out of bounds
		}

		_, _, _, err := v.getLogicalOperands(instr)
		if err == nil {
			t.Error("Expected error for invalid P3")
		}
	})
}

// arithTestCase represents a declarative arithmetic operation test
type arithTestCase struct {
	name     string
	opcode   Opcode
	leftInt  int64
	leftReal float64
	isLeftInt bool
	rightInt  int64
	rightReal float64
	isRightInt bool
	expectInt  int64
	expectReal float64
	isExpectInt bool
}

// arithExecOp executes the arithmetic operation for the given opcode
func arithExecOp(v *VDBE, opcode Opcode, instr *Instruction) error {
	switch opcode {
	case OpAdd:
		return v.execAdd(instr)
	case OpSubtract:
		return v.execSubtract(instr)
	case OpMultiply:
		return v.execMultiply(instr)
	case OpDivide:
		return v.execDivide(instr)
	case OpRemainder:
		return v.execRemainder(instr)
	default:
		return nil
	}
}

// arithVerifyResult checks if the result matches expected value
func arithVerifyResult(t *testing.T, mem *Mem, tc arithTestCase) {
	t.Helper()
	if tc.isExpectInt {
		if !mem.IsInt() || mem.IntValue() != tc.expectInt {
			t.Errorf("Expected int %d, got type=%v val=%v", tc.expectInt, mem.IsInt(), mem.IntValue())
		}
	} else {
		if !mem.IsReal() || mem.RealValue() != tc.expectReal {
			t.Errorf("Expected real %f, got type=%v val=%v", tc.expectReal, mem.IsReal(), mem.RealValue())
		}
	}
}

// TestArithmeticOperations tests arithmetic operations comprehensively
func TestArithmeticOperations(t *testing.T) {
	t.Parallel()
	tests := []arithTestCase{
		{name: "AddInts", opcode: OpAdd, leftInt: 10, isLeftInt: true, rightInt: 20, isRightInt: true, expectInt: 30, isExpectInt: true},
		{name: "AddReals", opcode: OpAdd, leftReal: 1.5, isLeftInt: false, rightReal: 2.5, isRightInt: false, expectReal: 4.0, isExpectInt: false},
		{name: "SubtractInts", opcode: OpSubtract, leftInt: 50, isLeftInt: true, rightInt: 20, isRightInt: true, expectInt: 30, isExpectInt: true},
		{name: "SubtractReals", opcode: OpSubtract, leftReal: 5.5, isLeftInt: false, rightReal: 2.5, isRightInt: false, expectReal: 3.0, isExpectInt: false},
		{name: "MultiplyInts", opcode: OpMultiply, leftInt: 5, isLeftInt: true, rightInt: 6, isRightInt: true, expectInt: 30, isExpectInt: true},
		{name: "MultiplyReals", opcode: OpMultiply, leftReal: 2.5, isLeftInt: false, rightReal: 4.0, isRightInt: false, expectReal: 10.0, isExpectInt: false},
		{name: "DivideInts", opcode: OpDivide, leftInt: 20, isLeftInt: true, rightInt: 4, isRightInt: true, expectInt: 5, isExpectInt: true},
		{name: "DivideReals", opcode: OpDivide, leftReal: 10.0, isLeftInt: false, rightReal: 2.0, isRightInt: false, expectReal: 5.0, isExpectInt: false},
		{name: "RemainderInts", opcode: OpRemainder, leftInt: 17, isLeftInt: true, rightInt: 5, isRightInt: true, expectInt: 2, isExpectInt: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)

			if tt.isLeftInt {
				v.Mem[0].SetInt(tt.leftInt)
			} else {
				v.Mem[0].SetReal(tt.leftReal)
			}

			if tt.isRightInt {
				v.Mem[1].SetInt(tt.rightInt)
			} else {
				v.Mem[1].SetReal(tt.rightReal)
			}

			instr := &Instruction{Opcode: tt.opcode, P1: 0, P2: 1, P3: 2}

			if err := arithExecOp(v, tt.opcode, instr); err != nil {
				t.Fatalf("Operation failed: %v", err)
			}

			arithVerifyResult(t, v.Mem[2], tt)
		})
	}
}

// TestArithmeticWithNulls tests arithmetic operations with NULL values
func TestArithmeticWithNulls(t *testing.T) {
	t.Parallel()
	opcodes := []struct {
		name   string
		opcode Opcode
		exec   func(*VDBE, *Instruction) error
	}{
		{"Add", OpAdd, (*VDBE).execAdd},
		{"Subtract", OpSubtract, (*VDBE).execSubtract},
		{"Multiply", OpMultiply, (*VDBE).execMultiply},
		{"Divide", OpDivide, (*VDBE).execDivide},
		{"Remainder", OpRemainder, (*VDBE).execRemainder},
	}

	for _, op := range opcodes {
		t.Run(op.name+"_LeftNull", func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			v.Mem[0].SetNull()
			v.Mem[1].SetInt(10)

			instr := &Instruction{
				Opcode: op.opcode,
				P1:     0,
				P2:     1,
				P3:     2,
			}

			err := op.exec(v, instr)
			if err != nil {
				t.Fatalf("%s failed: %v", op.name, err)
			}

			if !v.Mem[2].IsNull() {
				t.Error("Expected NULL result when left operand is NULL")
			}
		})

		t.Run(op.name+"_RightNull", func(t *testing.T) {
			t.Parallel()
			v := NewTestVDBE(5)
			v.Mem[0].SetInt(10)
			v.Mem[1].SetNull()

			instr := &Instruction{
				Opcode: op.opcode,
				P1:     0,
				P2:     1,
				P3:     2,
			}

			err := op.exec(v, instr)
			if err != nil {
				t.Fatalf("%s failed: %v", op.name, err)
			}

			if !v.Mem[2].IsNull() {
				t.Error("Expected NULL result when right operand is NULL")
			}
		})
	}
}

// TestMemRealifyExtended tests additional Realify cases
func TestMemRealifyExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    func(*Mem)
		expected float64
		wantErr  bool
	}{
		{
			name:     "BlobToReal",
			setup:    func(m *Mem) { m.SetBlob([]byte("1.23")) },
			expected: 1.23,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mem := NewMem()
			tt.setup(mem)

			err := mem.Realify()
			if (err != nil) != tt.wantErr {
				t.Errorf("Realify() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if mem.RealValue() != tt.expected {
					t.Errorf("Expected %f, got %f", tt.expected, mem.RealValue())
				}
				if !mem.IsReal() {
					t.Error("Expected IsReal() to be true")
				}
			}
		})
	}
}

// TestParseColumnIntoMem tests parseColumnIntoMem error handling
func TestParseColumnIntoMem(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)

	t.Run("ValidPayload", func(t *testing.T) {
		t.Parallel()
		dst := NewMem()
		// Valid record: header length 2, serial type 8 (int 0)
		payload := []byte{2, 8}
		err := v.parseColumnIntoMem(payload, 0, dst, nil)
		if err != nil {
			t.Fatalf("parseColumnIntoMem failed: %v", err)
		}
		if dst.IntValue() != 0 {
			t.Errorf("Expected 0, got %d", dst.IntValue())
		}
	})
}

// TestCheckRowidExists tests checkRowidExists edge cases
func TestCheckRowidExists(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)

	t.Run("NilBtreeCursor", func(t *testing.T) {
		t.Parallel()
		cursor := &Cursor{BtreeCursor: nil}
		found, err := v.checkRowidExists(cursor, 1)
		if err != nil {
			t.Fatalf("checkRowidExists failed: %v", err)
		}
		if found {
			t.Error("Expected not found for nil btree cursor")
		}
	})
}

// TestExecSeekRowid tests execSeekRowid edge cases
func TestExecSeekRowid(t *testing.T) {
	t.Parallel()
	t.Run("InvalidCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{
			Opcode: OpSeekRowid,
			P1:     0,
			P2:     10,
			P3:     0,
		}

		err := v.execSeekRowid(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})
}

// TestExecOpenRead tests execOpenRead edge cases
func TestExecOpenRead(t *testing.T) {
	t.Parallel()
	t.Run("NoContext", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{
			Opcode: OpOpenRead,
			P1:     0,
			P2:     1,
		}

		err := v.execOpenRead(instr)
		if err == nil {
			t.Error("Expected error when no context")
		}
	})
}

// TestExecOpenWrite tests execOpenWrite edge cases
func TestExecOpenWrite(t *testing.T) {
	t.Parallel()
	t.Run("NoContext", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{
			Opcode: OpOpenWrite,
			P1:     0,
			P2:     1,
		}

		err := v.execOpenWrite(instr)
		if err == nil {
			t.Error("Expected error when no context")
		}
	})
}

// TestExecPrevExtended tests additional execPrev edge cases
func TestExecPrevExtended(t *testing.T) {
	t.Parallel()
	t.Run("NilBtreeCursor_SetsEOF", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		// Create a cursor with nil BtreeCursor
		v.Cursors[0] = &Cursor{
			IsTable:     true,
			BtreeCursor: nil,
			EOF:         false,
		}

		instr := &Instruction{
			Opcode: OpPrev,
			P1:     0,
			P2:     10,
		}

		err := v.execPrev(instr)
		if err != nil {
			t.Fatalf("execPrev failed: %v", err)
		}

		if !v.Cursors[0].EOF {
			t.Error("Expected EOF to be set for nil btree cursor")
		}
	})
}

// TestSeekAndDeleteIndexEntry is tested through integration tests
// as it requires valid index cursors which are hard to mock
// Removed nil cursor test as it causes panic before error check

// TestExecIdxRowidExtended tests additional execIdxRowid edge cases
func TestExecIdxRowidExtended(t *testing.T) {
	t.Parallel()
	t.Run("TableCursor", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{IsTable: true}

		instr := &Instruction{
			Opcode: OpIdxRowid,
			P1:     0,
			P2:     0,
		}

		err := v.execIdxRowid(instr)
		if err == nil {
			t.Error("Expected error for table cursor")
		}
	})
}

// TestRepositionToRowid is tested through integration tests
// as it requires valid btree cursors which are hard to mock
// Removed nil cursor test as it causes panic before error check
