package vdbe

import (
	"testing"
)

// TestExecGotoExtended tests additional execGoto edge cases
func TestExecGotoExtended(t *testing.T) {
	v := NewTestVDBE(5)
	// Add some instructions to the program
	v.AddOp(OpNoop, 0, 0, 0)   // 0
	v.AddOp(OpNoop, 0, 0, 0)   // 1
	v.AddOp(OpNoop, 0, 0, 0)   // 2
	v.AddOp(OpGoto, 0, 1, 0)   // 3 - goto instruction
	v.AddOp(OpHalt, 0, 0, 0)   // 4

	v.PC = 3

	instr := v.Program[3]
	err := v.execGoto(instr)
	if err != nil {
		t.Fatalf("execGoto failed: %v", err)
	}

	if v.PC != 1 {
		t.Errorf("Expected PC=1, got %d", v.PC)
	}
}

// TestExecGosub tests execGosub edge cases
func TestExecGosub(t *testing.T) {
	v := NewTestVDBE(10)
	v.PC = 3

	instr := &Instruction{
		Opcode: OpGosub,
		P1:     2,
		P2:     15,
	}

	err := v.execGosub(instr)
	if err != nil {
		t.Fatalf("execGosub failed: %v", err)
	}

	if v.PC != 15 {
		t.Errorf("Expected PC=15, got %d", v.PC)
	}

	if v.Mem[2].IntValue() != 3 {
		t.Errorf("Expected return address 3, got %d", v.Mem[2].IntValue())
	}
}

// TestExecReturn tests execReturn edge cases
func TestExecReturn(t *testing.T) {
	v := NewTestVDBE(10)
	v.Mem[2].SetInt(7)
	v.PC = 15

	instr := &Instruction{
		Opcode: OpReturn,
		P1:     2,
	}

	err := v.execReturn(instr)
	if err != nil {
		t.Fatalf("execReturn failed: %v", err)
	}

	if v.PC != 7 {
		t.Errorf("Expected PC=7, got %d", v.PC)
	}
}

// TestExecHalt tests execHalt
func TestExecHalt(t *testing.T) {
	v := NewTestVDBE(5)

	instr := &Instruction{
		Opcode: OpHalt,
		P1:     0,
	}

	err := v.execHalt(instr)
	if err != nil {
		t.Fatalf("execHalt failed: %v", err)
	}

	if v.State != StateHalt {
		t.Errorf("Expected state=StateHalt, got %v", v.State)
	}
}

// TestExecIf tests execIf edge cases
func TestExecIf(t *testing.T) {
	tests := []struct {
		name       string
		setupMem   func(*VDBE)
		p1         int
		p2         int
		expectedPC int
	}{
		{
			name: "TrueValue_Jumps",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(1)
			},
			p1:         0,
			p2:         10,
			expectedPC: 10,
		},
		{
			name: "FalseValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(0)
			},
			p1:         0,
			p2:         10,
			expectedPC: 0,
		},
		{
			name: "NullValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetNull()
			},
			p1:         0,
			p2:         10,
			expectedPC: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewTestVDBE(5)
			tt.setupMem(v)
			v.PC = 0

			instr := &Instruction{
				Opcode: OpIf,
				P1:     tt.p1,
				P2:     tt.p2,
			}

			err := v.execIf(instr)
			if err != nil {
				t.Fatalf("execIf failed: %v", err)
			}

			if v.PC != tt.expectedPC {
				t.Errorf("Expected PC=%d, got %d", tt.expectedPC, v.PC)
			}
		})
	}
}

// TestExecIfNot tests execIfNot edge cases
func TestExecIfNot(t *testing.T) {
	tests := []struct {
		name       string
		setupMem   func(*VDBE)
		p1         int
		p2         int
		expectedPC int
	}{
		{
			name: "FalseValue_Jumps",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(0)
			},
			p1:         0,
			p2:         10,
			expectedPC: 10,
		},
		{
			name: "TrueValue_NoJump",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetInt(1)
			},
			p1:         0,
			p2:         10,
			expectedPC: 0,
		},
		{
			name: "NullValue_Jumps",
			setupMem: func(v *VDBE) {
				v.Mem[0].SetNull()
			},
			p1:         0,
			p2:         10,
			expectedPC: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewTestVDBE(5)
			tt.setupMem(v)
			v.PC = 0

			instr := &Instruction{
				Opcode: OpIfNot,
				P1:     tt.p1,
				P2:     tt.p2,
			}

			err := v.execIfNot(instr)
			if err != nil {
				t.Fatalf("execIfNot failed: %v", err)
			}

			if v.PC != tt.expectedPC {
				t.Errorf("Expected PC=%d, got %d", tt.expectedPC, v.PC)
			}
		})
	}
}

// TestExecInteger tests execInteger edge cases
func TestExecInteger(t *testing.T) {
	v := NewTestVDBE(5)

	instr := &Instruction{
		Opcode: OpInteger,
		P1:     42,
		P2:     0,
	}

	err := v.execInteger(instr)
	if err != nil {
		t.Fatalf("execInteger failed: %v", err)
	}

	if v.Mem[0].IntValue() != 42 {
		t.Errorf("Expected 42, got %d", v.Mem[0].IntValue())
	}
}

// TestExecNull tests execNull
func TestExecNull(t *testing.T) {
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(42)
	v.Mem[1].SetInt(43)
	v.Mem[2].SetInt(44)

	instr := &Instruction{
		Opcode: OpNull,
		P2:     0,
		P3:     3, // Set 3 registers to NULL
	}

	err := v.execNull(instr)
	if err != nil {
		t.Fatalf("execNull failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if !v.Mem[i].IsNull() {
			t.Errorf("Expected register %d to be NULL", i)
		}
	}
}

// TestExecMoveExtended tests execMove with zero count
func TestExecMoveExtended(t *testing.T) {
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(10)

	instr := &Instruction{
		Opcode: OpMove,
		P1:     0,
		P2:     5,
		P3:     0, // Zero count - should default to 1
	}

	err := v.execMove(instr)
	if err != nil {
		t.Fatalf("execMove failed: %v", err)
	}

	// Check destination register (should move 1 register)
	if v.Mem[5].IntValue() != 10 {
		t.Errorf("Expected Mem[5]=10, got %d", v.Mem[5].IntValue())
	}
}

// TestExecSCopyExtended tests execSCopy with different types
func TestExecSCopyExtended(t *testing.T) {
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(42)

	instr := &Instruction{
		Opcode: OpSCopy,
		P1:     0,
		P2:     3,
	}

	err := v.execSCopy(instr)
	if err != nil {
		t.Fatalf("execSCopy failed: %v", err)
	}

	if v.Mem[3].IntValue() != 42 {
		t.Errorf("Expected 42, got %d", v.Mem[3].IntValue())
	}
}

// TestExecBlobExtended tests execBlob with nil P4
func TestExecBlobExtended(t *testing.T) {
	v := NewTestVDBE(5)
	v.Mem[0].SetInt(42) // Set initial value

	instr := &Instruction{
		Opcode: OpBlob,
		P1:     0,
		P2:     0,
		P4Type: P4Static,
		P4:     P4Union{}, // nil P4
	}

	err := v.execBlob(instr)
	if err != nil {
		t.Fatalf("execBlob failed: %v", err)
	}

	// When P4.P is nil, the register is not modified
}

// TestExecAddImm tests execAddImm
func TestExecAddImm(t *testing.T) {
	tests := []struct {
		name     string
		initial  int64
		p2       int
		expected int64
	}{
		{
			name:     "AddPositive",
			initial:  10,
			p2:       5,
			expected: 15,
		},
		{
			name:     "AddNegative",
			initial:  10,
			p2:       -5,
			expected: 5,
		},
		{
			name:     "AddZero",
			initial:  10,
			p2:       0,
			expected: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewTestVDBE(5)
			v.Mem[0].SetInt(tt.initial)

			instr := &Instruction{
				Opcode: OpAddImm,
				P1:     0,
				P2:     tt.p2,
			}

			err := v.execAddImm(instr)
			if err != nil {
				t.Fatalf("execAddImm failed: %v", err)
			}

			if v.Mem[0].IntValue() != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, v.Mem[0].IntValue())
			}
		})
	}
}

// TestExecIsNull tests execIsNull
func TestExecIsNull(t *testing.T) {
	tests := []struct {
		name     string
		setupMem func(*Mem)
		expected int64
	}{
		{
			name:     "NullValue",
			setupMem: func(m *Mem) { m.SetNull() },
			expected: 1,
		},
		{
			name:     "IntValue",
			setupMem: func(m *Mem) { m.SetInt(42) },
			expected: 0,
		},
		{
			name:     "StringValue",
			setupMem: func(m *Mem) { m.SetStr("test") },
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewTestVDBE(5)
			tt.setupMem(v.Mem[0])

			instr := &Instruction{
				Opcode: OpIsNull,
				P1:     0,
				P2:     10,
			}

			err := v.execIsNull(instr)
			if err != nil {
				t.Fatalf("execIsNull failed: %v", err)
			}

			if tt.expected == 1 && v.PC != 10 {
				t.Error("Expected jump for NULL value")
			}
			if tt.expected == 0 && v.PC != 0 {
				t.Error("Expected no jump for non-NULL value")
			}
		})
	}
}

// TestExecNotNull tests execNotNull
func TestExecNotNull(t *testing.T) {
	tests := []struct {
		name     string
		setupMem func(*Mem)
		expected int64
	}{
		{
			name:     "NullValue",
			setupMem: func(m *Mem) { m.SetNull() },
			expected: 0,
		},
		{
			name:     "IntValue",
			setupMem: func(m *Mem) { m.SetInt(42) },
			expected: 1,
		},
		{
			name:     "StringValue",
			setupMem: func(m *Mem) { m.SetStr("test") },
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewTestVDBE(5)
			tt.setupMem(v.Mem[0])

			instr := &Instruction{
				Opcode: OpNotNull,
				P1:     0,
				P2:     10,
			}

			err := v.execNotNull(instr)
			if err != nil {
				t.Fatalf("execNotNull failed: %v", err)
			}

			if tt.expected == 1 && v.PC != 10 {
				t.Error("Expected jump for non-NULL value")
			}
			if tt.expected == 0 && v.PC != 0 {
				t.Error("Expected no jump for NULL value")
			}
		})
	}
}

// TestExecRollbackExtended tests execRollback extended cases
func TestExecRollbackExtended(t *testing.T) {
	t.Run("RollbackWithState", func(t *testing.T) {
		v := NewTestVDBE(5)
		mockPager := &MockTransactionPager{
			inWriteTxn: true,
			inTxn:      true,
		}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}
		v.NumChanges = 10

		instr := &Instruction{Opcode: OpRollback}
		err := v.execRollback(instr)
		if err != nil {
			t.Fatalf("execRollback failed: %v", err)
		}

		if !mockPager.rolledBack {
			t.Error("Expected Rollback to be called")
		}
	})
}

// TestExecTransaction tests execTransaction
func TestExecTransaction(t *testing.T) {
	t.Run("BeginWrite", func(t *testing.T) {
		v := NewTestVDBE(5)
		mockPager := &MockTransactionPager{}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}

		instr := &Instruction{
			Opcode: OpTransaction,
			P1:     0, // Database index
			P2:     1, // Write transaction
		}

		err := v.execTransaction(instr)
		if err != nil {
			t.Fatalf("execTransaction failed: %v", err)
		}

		if !mockPager.inWriteTxn {
			t.Error("Expected write transaction to begin")
		}
	})

	t.Run("BeginRead", func(t *testing.T) {
		v := NewTestVDBE(5)
		mockPager := &MockTransactionPager{}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}

		instr := &Instruction{
			Opcode: OpTransaction,
			P1:     0, // Database index
			P2:     0, // Read transaction
		}

		err := v.execTransaction(instr)
		if err != nil {
			t.Fatalf("execTransaction failed: %v", err)
		}

		if !mockPager.inTxn {
			t.Error("Expected read transaction to begin")
		}
	})
}

// TestExecClose tests execClose
func TestExecClose(t *testing.T) {
	v := NewTestVDBE(5)
	v.AllocCursors(3)
	v.Cursors[0] = &Cursor{
		IsTable: true,
	}

	instr := &Instruction{
		Opcode: OpClose,
		P1:     0,
	}

	err := v.execClose(instr)
	if err != nil {
		t.Fatalf("execClose failed: %v", err)
	}

	if v.Cursors[0] != nil {
		t.Error("Expected cursor to be closed (nil)")
	}
}

// TestEvalMemAsBool tests evalMemAsBool
func TestEvalMemAsBool(t *testing.T) {
	tests := []struct {
		name     string
		setupMem func(*Mem)
		expected bool
		isNull   bool
	}{
		{
			name:     "IntZero",
			setupMem: func(m *Mem) { m.SetInt(0) },
			expected: false,
			isNull:   false,
		},
		{
			name:     "IntNonZero",
			setupMem: func(m *Mem) { m.SetInt(42) },
			expected: true,
			isNull:   false,
		},
		{
			name:     "RealZero",
			setupMem: func(m *Mem) { m.SetReal(0.0) },
			expected: false,
			isNull:   false,
		},
		{
			name:     "RealNonZero",
			setupMem: func(m *Mem) { m.SetReal(1.5) },
			expected: true,
			isNull:   false,
		},
		{
			name:     "NullValue",
			setupMem: func(m *Mem) { m.SetNull() },
			expected: false,
			isNull:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := NewMem()
			tt.setupMem(mem)

			isNull, val := evalMemAsBool(mem)

			if isNull != tt.isNull {
				t.Errorf("Expected isNull=%v, got %v", tt.isNull, isNull)
			}
			if !tt.isNull && val != tt.expected {
				t.Errorf("Expected value=%v, got %v", tt.expected, val)
			}
		})
	}
}
