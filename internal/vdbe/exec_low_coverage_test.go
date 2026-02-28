package vdbe

import (
	"bytes"
	"testing"
)

// TestParseSerialValue tests parseSerialValue function (30% coverage)
func TestParseSerialValue(t *testing.T) {
	tests := []struct {
		name       string
		serialType uint64
		data       []byte
		offset     int
		wantErr    bool
		checkMem   func(*Mem) bool
	}{
		{
			name:       "SerialType0_Null",
			serialType: 0,
			data:       []byte{},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsNull() },
		},
		{
			name:       "SerialType8_IntZero",
			serialType: 8,
			data:       []byte{},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 0 },
		},
		{
			name:       "SerialType9_IntOne",
			serialType: 9,
			data:       []byte{},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 1 },
		},
		{
			name:       "SerialType1_Int8",
			serialType: 1,
			data:       []byte{0x7F}, // 127
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 127 },
		},
		{
			name:       "SerialType2_Int16",
			serialType: 2,
			data:       []byte{0x01, 0x00}, // 256
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 256 },
		},
		{
			name:       "SerialType3_Int24",
			serialType: 3,
			data:       []byte{0x00, 0x01, 0x00}, // 256
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 256 },
		},
		{
			name:       "SerialType4_Int32",
			serialType: 4,
			data:       []byte{0x00, 0x00, 0x01, 0x00}, // 256
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 256 },
		},
		{
			name:       "SerialType5_Int48",
			serialType: 5,
			data:       []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, // 256
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 256 },
		},
		{
			name:       "SerialType6_Int64",
			serialType: 6,
			data:       []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, // 256
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IntValue() == 256 },
		},
		{
			name:       "SerialType7_Float64",
			serialType: 7,
			data:       []byte{0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}, // 3.14159...
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsReal() },
		},
		{
			name:       "SerialType12_Blob0",
			serialType: 12,
			data:       []byte{},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsBlob() && len(m.BlobValue()) == 0 },
		},
		{
			name:       "SerialType13_Text0",
			serialType: 13,
			data:       []byte{},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsString() && m.StrValue() == "" },
		},
		{
			name:       "SerialType14_Blob1",
			serialType: 14,
			data:       []byte{0x42},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsBlob() && len(m.BlobValue()) == 1 && m.BlobValue()[0] == 0x42 },
		},
		{
			name:       "SerialType15_Text1",
			serialType: 15,
			data:       []byte{0x41}, // 'A'
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsString() && m.StrValue() == "A" },
		},
		{
			name:       "SerialType20_Blob4",
			serialType: 20,
			data:       []byte{0x01, 0x02, 0x03, 0x04},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsBlob() && len(m.BlobValue()) == 4 },
		},
		{
			name:       "SerialType21_Text4",
			serialType: 21,
			data:       []byte{'t', 'e', 's', 't'},
			offset:     0,
			wantErr:    false,
			checkMem:   func(m *Mem) bool { return m.IsString() && m.StrValue() == "test" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := NewMem()
			err := parseSerialValue(tt.data, tt.offset, tt.serialType, mem)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSerialValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.checkMem(mem) {
				t.Errorf("parseSerialValue() mem check failed for %s", tt.name)
			}
		})
	}
}

// TestParseSerialInt tests parseSerialInt function (50% coverage)
func TestParseSerialInt(t *testing.T) {
	tests := []struct {
		name       string
		serialType uint64
		data       []byte
		offset     int
		want       int64
		wantErr    bool
	}{
		{
			name:       "Int8_Positive",
			serialType: 1,
			data:       []byte{0x42},
			offset:     0,
			want:       66,
			wantErr:    false,
		},
		{
			name:       "Int8_Negative",
			serialType: 1,
			data:       []byte{0xFF}, // -1
			offset:     0,
			want:       -1,
			wantErr:    false,
		},
		{
			name:       "Int16_Value",
			serialType: 2,
			data:       []byte{0x12, 0x34},
			offset:     0,
			want:       0x1234,
			wantErr:    false,
		},
		{
			name:       "Int24_Value",
			serialType: 3,
			data:       []byte{0x12, 0x34, 0x56},
			offset:     0,
			want:       0x123456,
			wantErr:    false,
		},
		{
			name:       "Int32_Value",
			serialType: 4,
			data:       []byte{0x12, 0x34, 0x56, 0x78},
			offset:     0,
			want:       0x12345678,
			wantErr:    false,
		},
		{
			name:       "Int48_Value",
			serialType: 5,
			data:       []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			offset:     0,
			want:       0x123456789ABC,
			wantErr:    false,
		},
		{
			name:       "Int64_Value",
			serialType: 6,
			data:       []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},
			offset:     0,
			want:       0x123456789ABCDEF0,
			wantErr:    false,
		},
		{
			name:       "Truncated_Int16",
			serialType: 2,
			data:       []byte{0x12}, // Only 1 byte, need 2
			offset:     0,
			wantErr:    true,
		},
		{
			name:       "Truncated_Int64",
			serialType: 6,
			data:       []byte{0x12, 0x34}, // Only 2 bytes, need 8
			offset:     0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := NewMem()
			err := parseSerialInt(tt.data, tt.offset, tt.serialType, mem)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSerialInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && mem.IntValue() != tt.want {
				t.Errorf("parseSerialInt() got = %d, want %d", mem.IntValue(), tt.want)
			}
		})
	}
}

// TestGetColumnPayload tests getColumnPayload function (50% coverage)
func TestGetColumnPayload(t *testing.T) {
	t.Run("NullRow", func(t *testing.T) {
		v := NewTestVDBE(5)
		cursor := &Cursor{NullRow: true}
		dst := NewMem()

		payload := v.getColumnPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for null row")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL for null row")
		}
	})

	t.Run("EOF", func(t *testing.T) {
		v := NewTestVDBE(5)
		cursor := &Cursor{EOF: true}
		dst := NewMem()

		payload := v.getColumnPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for EOF")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL for EOF")
		}
	})

	t.Run("PseudoCursor_WithBlob", func(t *testing.T) {
		v := NewTestVDBE(10)
		v.Mem[5].SetBlob([]byte{1, 2, 3, 4})

		cursor := &Cursor{
			CurType:   CursorPseudo,
			PseudoReg: 5,
		}
		dst := NewMem()

		payload := v.getColumnPayload(cursor, dst)
		if payload == nil {
			t.Error("Expected non-nil payload for pseudo cursor with blob")
		}
		if len(payload) != 4 {
			t.Errorf("Expected payload length 4, got %d", len(payload))
		}
	})

	t.Run("PseudoCursor_WithNull", func(t *testing.T) {
		v := NewTestVDBE(10)
		v.Mem[5].SetNull()

		cursor := &Cursor{
			CurType:   CursorPseudo,
			PseudoReg: 5,
		}
		dst := NewMem()

		payload := v.getColumnPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for pseudo cursor with null")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL")
		}
	})

	t.Run("PseudoCursor_WithNonBlob", func(t *testing.T) {
		v := NewTestVDBE(10)
		v.Mem[5].SetInt(42)

		cursor := &Cursor{
			CurType:   CursorPseudo,
			PseudoReg: 5,
		}
		dst := NewMem()

		payload := v.getColumnPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for pseudo cursor with non-blob")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL for non-blob")
		}
	})
}

// TestGetBtreeCursorPayload tests getBtreeCursorPayload function (62.5% coverage)
func TestGetBtreeCursorPayload(t *testing.T) {
	t.Run("NilBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		cursor := &Cursor{
			CurType:      CursorBTree,
			BtreeCursor:  nil,
		}
		dst := NewMem()

		payload := v.getBtreeCursorPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for nil btree cursor")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL")
		}
	})

	t.Run("InvalidBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		cursor := &Cursor{
			CurType:      CursorBTree,
			BtreeCursor:  "not a cursor", // Invalid type
		}
		dst := NewMem()

		payload := v.getBtreeCursorPayload(cursor, dst)
		if payload != nil {
			t.Error("Expected nil payload for invalid btree cursor")
		}
		if !dst.IsNull() {
			t.Error("Expected dst to be NULL")
		}
	})
}

// TestExecRowid tests execRowid function (57.9% coverage)
func TestExecRowid(t *testing.T) {
	t.Run("NullRow", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{NullRow: true}

		instr := &Instruction{Opcode: OpRowid, P1: 0, P2: 0}
		err := v.execRowid(instr)
		if err != nil {
			t.Fatalf("execRowid failed: %v", err)
		}

		if !v.Mem[0].IsNull() {
			t.Error("Expected NULL for null row")
		}
	})

	t.Run("EOF", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{EOF: true}

		instr := &Instruction{Opcode: OpRowid, P1: 0, P2: 0}
		err := v.execRowid(instr)
		if err != nil {
			t.Fatalf("execRowid failed: %v", err)
		}

		if !v.Mem[0].IsNull() {
			t.Error("Expected NULL for EOF")
		}
	})

	t.Run("PseudoCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{CurType: CursorPseudo}

		instr := &Instruction{Opcode: OpRowid, P1: 0, P2: 0}
		err := v.execRowid(instr)
		if err != nil {
			t.Fatalf("execRowid failed: %v", err)
		}

		if !v.Mem[0].IsNull() {
			t.Error("Expected NULL for pseudo cursor")
		}
	})

	t.Run("NilBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: nil,
		}

		instr := &Instruction{Opcode: OpRowid, P1: 0, P2: 0}
		err := v.execRowid(instr)
		if err != nil {
			t.Fatalf("execRowid failed: %v", err)
		}

		if !v.Mem[0].IsNull() {
			t.Error("Expected NULL for nil btree cursor")
		}
	})

	t.Run("InvalidBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{
			CurType:     CursorBTree,
			BtreeCursor: "invalid",
		}

		instr := &Instruction{Opcode: OpRowid, P1: 0, P2: 0}
		err := v.execRowid(instr)
		if err != nil {
			t.Fatalf("execRowid failed: %v", err)
		}

		if !v.Mem[0].IsNull() {
			t.Error("Expected NULL for invalid btree cursor")
		}
	})
}

// TestParseRecordColumn tests parseRecordColumn function (65.2% coverage)
func TestParseRecordColumn(t *testing.T) {
	t.Run("TruncatedHeader", func(t *testing.T) {
		mem := NewMem()
		// Header length says 5, but only 2 bytes provided
		err := parseRecordColumn([]byte{5, 1}, 0, mem)
		if err == nil {
			t.Error("Expected error for truncated header")
		}
	})

	t.Run("ValidColumn0_Null", func(t *testing.T) {
		mem := NewMem()
		// Valid record: header length 2, serial type 0 (NULL)
		payload := []byte{2, 0}
		err := parseRecordColumn(payload, 0, mem)
		if err != nil {
			t.Fatalf("parseRecordColumn failed: %v", err)
		}
		if !mem.IsNull() {
			t.Error("Expected NULL value")
		}
	})

	t.Run("ValidColumn0_Int", func(t *testing.T) {
		mem := NewMem()
		// Record: header length 2, serial type 1 (int8), value 42
		payload := []byte{2, 1, 42}
		err := parseRecordColumn(payload, 0, mem)
		if err != nil {
			t.Fatalf("parseRecordColumn failed: %v", err)
		}
		if mem.IntValue() != 42 {
			t.Errorf("Expected 42, got %d", mem.IntValue())
		}
	})

	t.Run("MultiColumn_GetSecond", func(t *testing.T) {
		mem := NewMem()
		// Record: header length 3, col0: serial type 8 (int 0), col1: serial type 1 (int8), value 99
		payload := []byte{3, 8, 1, 99}
		err := parseRecordColumn(payload, 1, mem)
		if err != nil {
			t.Fatalf("parseRecordColumn failed: %v", err)
		}
		if mem.IntValue() != 99 {
			t.Errorf("Expected 99, got %d", mem.IntValue())
		}
	})
}

// Note: checkRowidExists and repositionToRowid are tested through integration tests
// as they require valid btree cursors which are hard to mock

// TestExecDelete tests execDelete function (57.1% coverage)
func TestExecDelete(t *testing.T) {
	t.Run("InvalidCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{Opcode: OpDelete, P1: 0}
		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

	t.Run("NilBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{BtreeCursor: nil}

		instr := &Instruction{Opcode: OpDelete, P1: 0}
		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error for nil btree cursor")
		}
	})

	t.Run("InvalidBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{BtreeCursor: "invalid"}

		instr := &Instruction{Opcode: OpDelete, P1: 0}
		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error for invalid btree cursor")
		}
	})
}

// TestExecCommit tests execCommit function (50% coverage)
func TestExecCommit(t *testing.T) {
	t.Run("NoPager", func(t *testing.T) {
		v := NewTestVDBE(5)
		// No context, no pager

		instr := &Instruction{Opcode: OpCommit}
		err := v.execCommit(instr)
		if err == nil {
			t.Error("Expected error when no pager")
		}
	})

	t.Run("WithPager", func(t *testing.T) {
		v := NewTestVDBE(5)
		mockPager := &MockTransactionPager{
			inWriteTxn: true,
			inTxn:      true,
		}
		v.Ctx = &VDBEContext{
			Pager: mockPager,
		}

		instr := &Instruction{Opcode: OpCommit}
		err := v.execCommit(instr)
		if err != nil {
			t.Fatalf("execCommit failed: %v", err)
		}
		if !mockPager.committed {
			t.Error("Expected Commit to be called")
		}
	})
}

// TestExtractKeyAsBlob tests extractKeyAsBlob function (33.3% coverage)
func TestExtractKeyAsBlob(t *testing.T) {
	t.Run("BlobMem", func(t *testing.T) {
		mem := NewMemBlob([]byte{1, 2, 3, 4})
		result := extractKeyAsBlob(mem)
		if !bytes.Equal(result, []byte{1, 2, 3, 4}) {
			t.Errorf("Expected [1,2,3,4], got %v", result)
		}
	})

	t.Run("StringMem", func(t *testing.T) {
		mem := NewMemStr("test")
		result := extractKeyAsBlob(mem)
		if !bytes.Equal(result, []byte("test")) {
			t.Errorf("Expected 'test', got %v", string(result))
		}
	})

	t.Run("IntMem", func(t *testing.T) {
		mem := NewMemInt(42)
		result := extractKeyAsBlob(mem)
		// Should stringify the int
		if result == nil {
			t.Error("Expected non-nil result")
		}
	})
}

// Note: seekAndDeleteIndexEntry is tested through integration tests
// as it requires valid index cursors which are hard to mock

// TestExecIdxRowid tests execIdxRowid function (61.1% coverage)
func TestExecIdxRowid(t *testing.T) {
	t.Run("InvalidCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{Opcode: OpIdxRowid, P1: 0, P2: 0}
		err := v.execIdxRowid(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})


}

// TestExecDeferredSeek tests execDeferredSeek function (68.2% coverage)
func TestExecDeferredSeek(t *testing.T) {
	t.Run("InvalidCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{Opcode: OpDeferredSeek, P1: 0, P2: 0}
		err := v.execDeferredSeek(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

}

// TestExecPrev tests execPrev function (66.7% coverage)
func TestExecPrev(t *testing.T) {
	t.Run("InvalidCursor", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.AllocCursors(2)

		instr := &Instruction{Opcode: OpPrev, P1: 0, P2: 10}
		err := v.execPrev(instr)
		if err == nil {
			t.Error("Expected error for invalid cursor")
		}
	})

}

// TestExecStep tests Step function edge cases (66.7% coverage)
func TestExecStep(t *testing.T) {
	t.Run("HaltState", func(t *testing.T) {
		v := New()
		v.State = StateHalt

		hasRow, err := v.Step()
		if err == nil {
			t.Error("Expected error when stepping halted VDBE")
		}
		if hasRow {
			t.Error("Expected no row for halted VDBE")
		}
	})

	t.Run("InitToReady", func(t *testing.T) {
		v := New()
		v.AddOp(OpHalt, 0, 0, 0)
		v.State = StateInit

		hasRow, err := v.Step()
		if err != nil {
			t.Fatalf("Step failed: %v", err)
		}
		if hasRow {
			t.Error("Expected no row for halt instruction")
		}
	})
}

// TestExecInstruction tests execInstruction edge cases (66.7% coverage)
func TestExecInstruction(t *testing.T) {
	t.Run("UnknownOpcode", func(t *testing.T) {
		v := NewTestVDBE(5)
		instr := &Instruction{Opcode: Opcode(255)} // Invalid opcode

		err := v.execInstruction(instr)
		if err == nil {
			t.Error("Expected error for unknown opcode")
		}
	})
}

// TestExecInit tests execInit edge cases (66.7% coverage)
func TestExecInit(t *testing.T) {
	t.Run("JumpToP2", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.PC = 0

		instr := &Instruction{Opcode: OpInit, P2: 10}
		err := v.execInit(instr)
		if err != nil {
			t.Fatalf("execInit failed: %v", err)
		}

		if v.PC != 10 {
			t.Errorf("Expected PC=10, got %d", v.PC)
		}
	})
}

// TestGetInsertPayload tests getInsertPayload function (66.7% coverage)
func TestGetInsertPayload(t *testing.T) {
	t.Run("InvalidRegister", func(t *testing.T) {
		v := NewTestVDBE(5)

		_, err := v.getInsertPayload(100) // Out of bounds
		if err == nil {
			t.Error("Expected error for invalid register")
		}
	})

	t.Run("BlobRegister", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Mem[0].SetBlob([]byte{1, 2, 3, 4})

		payload, err := v.getInsertPayload(0)
		if err != nil {
			t.Fatalf("getInsertPayload failed: %v", err)
		}
		if len(payload) != 4 {
			t.Errorf("Expected payload length 4, got %d", len(payload))
		}
	})
}

// TestGetSavepointPager tests getSavepointPager function (66.7% coverage)
func TestGetSavepointPager(t *testing.T) {
	t.Run("NoContext", func(t *testing.T) {
		v := NewTestVDBE(5)

		_, err := v.getSavepointPager()
		if err == nil {
			t.Error("Expected error when no context")
		}
	})

	t.Run("NoPager", func(t *testing.T) {
		v := NewTestVDBE(5)
		v.Ctx = &VDBEContext{}

		_, err := v.getSavepointPager()
		if err == nil {
			t.Error("Expected error when no pager")
		}
	})
}


// TestMemRealValue tests RealValue function (50% coverage)
func TestMemRealValue(t *testing.T) {
	t.Run("IntToReal", func(t *testing.T) {
		mem := NewMemInt(42)
		val := mem.RealValue()
		if val != 42.0 {
			t.Errorf("Expected 42.0, got %f", val)
		}
	})

	t.Run("StringToReal", func(t *testing.T) {
		mem := NewMemStr("3.14")
		val := mem.RealValue()
		if val < 3.13 || val > 3.15 {
			t.Errorf("Expected ~3.14, got %f", val)
		}
	})

	t.Run("NullToReal", func(t *testing.T) {
		mem := NewMemNull()
		val := mem.RealValue()
		if val != 0.0 {
			t.Errorf("Expected 0.0 for null, got %f", val)
		}
	})
}

// TestMemAdd tests Add function (50% coverage)
func TestMemAdd(t *testing.T) {
	t.Run("IntAdd", func(t *testing.T) {
		a := NewMemInt(10)
		b := NewMemInt(20)

		err := a.Add(b)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if a.IntValue() != 30 {
			t.Errorf("Expected 30, got %d", a.IntValue())
		}
	})

	t.Run("RealAdd", func(t *testing.T) {
		a := NewMemReal(1.5)
		b := NewMemReal(2.5)

		err := a.Add(b)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if a.RealValue() != 4.0 {
			t.Errorf("Expected 4.0, got %f", a.RealValue())
		}
	})
}

// TestWindowCurrentRow tests CurrentRow function (66.7% coverage)
func TestWindowCurrentRow(t *testing.T) {
	frame := DefaultWindowFrame()
	ws := NewWindowState(nil, nil, nil, frame)

	row := ws.CurrentRow()
	if row != nil {
		t.Error("Expected nil for empty window state")
	}
}

// TestWindowGetPartitionSize tests GetPartitionSize function (66.7% coverage)
func TestWindowGetPartitionSize(t *testing.T) {
	frame := DefaultWindowFrame()
	ws := NewWindowState(nil, nil, nil, frame)

	size := ws.GetPartitionSize()
	if size != 0 {
		t.Errorf("Expected 0 for empty window state, got %d", size)
	}
}

// TestCalculateFrameStart tests calculateFrameStart function (50% coverage)
func TestCalculateFrameStart(t *testing.T) {
	frame := DefaultWindowFrame()
	ws := NewWindowState(nil, nil, nil, frame)
	// Add some rows
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(3)})

	start := ws.calculateFrameStart(0, 3)
	if start < 0 {
		t.Errorf("Expected non-negative start, got %d", start)
	}
}

// TestCalculateFrameEnd tests calculateFrameEnd function (50% coverage)
func TestCalculateFrameEnd(t *testing.T) {
	frame := DefaultWindowFrame()
	ws := NewWindowState(nil, nil, nil, frame)
	// Add some rows
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(3)})

	end := ws.calculateFrameEnd(0, 3)
	if end < 0 {
		t.Errorf("Expected non-negative end, got %d", end)
	}
}

// TestOpcodeString tests Opcode.String function (66.7% coverage)
func TestOpcodeString(t *testing.T) {
	tests := []struct {
		opcode Opcode
		want   string
	}{
		{OpNoop, "Noop"},
		{OpInit, "Init"},
		{OpGoto, "Goto"},
		{OpHalt, "Halt"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.opcode.String()
			if got != tt.want {
				t.Errorf("Opcode.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
