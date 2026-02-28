package vdbe

import (
	"testing"
)

// TestExecFunctionCoverage tests the execFunction opcode (0% coverage)
func TestExecFunctionCoverage(t *testing.T) {
	v := NewTestVDBE(20)

	// Set up function context (uses default registry with built-in functions)
	v.funcCtx = NewFunctionContext()

	// Set up arguments in registers - test upper() function
	v.Mem[1].SetStr("hello")

	// Test function call with 1 argument - upper("hello") -> "HELLO"
	instr := &Instruction{
		Opcode: OpFunction,
		P1:     0,  // constant mask
		P2:     1,  // first arg register
		P3:     5,  // output register
		P4:     P4Union{Z: "upper"},
		P4Type: P4Static,
		P5:     1,  // number of arguments
	}

	// Add instruction to program and set PC so it can access it
	v.Program = append(v.Program, instr)
	v.PC = 1  // Set to 1 so PC-1 = 0 (the instruction we just added)

	err := v.execFunction(instr)
	if err != nil {
		t.Fatalf("execFunction failed: %v", err)
	}

	// Verify result is in register P3
	if v.Mem[5].StrValue() != "HELLO" {
		t.Errorf("Expected r5='HELLO', got %s", v.Mem[5].StrValue())
	}
}

// TestExecAggStepWindowCoverage tests the execAggStepWindow opcode (0% coverage)
func TestExecAggStepWindowCoverage(t *testing.T) {
	v := NewTestVDBE(20)
	v.funcCtx = NewFunctionContext()
	v.WindowStates = make(map[int]*WindowState)

	// Set up arguments
	v.Mem[1].SetInt(10)
	v.Mem[2].SetStr("test")

	// Test window aggregate step
	instr := &Instruction{
		Opcode: OpAggStepWindow,
		P1:     0,  // window index
		P2:     1,  // first arg register
		P4:     P4Union{Z: "count"},
		P4Type: P4Static,
		P5:     2,  // number of arguments
	}

	err := v.execAggStepWindow(instr)
	if err != nil {
		t.Fatalf("execAggStepWindow failed: %v", err)
	}

	// Verify window state was created
	if _, ok := v.WindowStates[0]; !ok {
		t.Error("Expected window state to be created")
	}

	// Test error case - no function name
	instrNoFunc := &Instruction{
		Opcode: OpAggStepWindow,
		P1:     1,
		P2:     1,
		P4:     P4Union{Z: ""},
		P4Type: P4Static,
		P5:     2,
	}

	err = v.execAggStepWindow(instrNoFunc)
	if err == nil {
		t.Error("Expected error when function name is empty")
	}
}

// TestDecodeInt48ValueCoverage tests decodeInt48Value (0% coverage)
func TestDecodeInt48ValueCoverage(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		offset   int
		expected int64
	}{
		{
			name:     "PositiveValue",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			offset:   0,
			expected: 256,
		},
		{
			name:     "NegativeValue",
			data:     []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: -1,
		},
		{
			name:     "MaxPositive",
			data:     []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			offset:   0,
			expected: 140737488355327,
		},
		{
			name:     "WithOffset",
			data:     []byte{0xAA, 0xBB, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00},
			offset:   2,
			expected: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decodeInt48Value(tt.data, tt.offset)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestSorterRewindCoverage tests Sorter.Rewind (0% coverage)
func TestSorterRewindCoverage(t *testing.T) {
	t.Run("EmptySorter", func(t *testing.T) {
		sorter := NewSorter(nil, nil, nil, 0)
		hasRows := sorter.Rewind()
		if hasRows {
			t.Error("Expected false for empty sorter")
		}
		if sorter.Current != 0 {
			t.Errorf("Expected Current=0, got %d", sorter.Current)
		}
	})

	t.Run("SorterWithRows", func(t *testing.T) {
		sorter := NewSorter(nil, nil, nil, 0)
		row1 := []*Mem{NewMemInt(1)}
		row2 := []*Mem{NewMemInt(2)}
		sorter.Insert(row1)
		sorter.Insert(row2)

		// Advance current
		sorter.Current = 1

		// Rewind should reset to 0 and return true
		hasRows := sorter.Rewind()
		if !hasRows {
			t.Error("Expected true for sorter with rows")
		}
		if sorter.Current != 0 {
			t.Errorf("Expected Current=0 after rewind, got %d", sorter.Current)
		}
	})
}

// TestExecDeleteComprehensive tests execDelete with more coverage (28.6% -> higher)
func TestExecDeleteComprehensive(t *testing.T) {
	t.Run("DeleteNoCursor", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		instr := &Instruction{
			Opcode: OpDelete,
			P1:     0,
		}

		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error when cursor is nil")
		}
	})

	t.Run("DeleteNotWritableCursor", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		cursor := &Cursor{
			CurType:  CursorBTree,
			IsTable:  true,
			Writable: false,  // Not writable
		}
		v.Cursors[0] = cursor

		instr := &Instruction{
			Opcode: OpDelete,
			P1:     0,
		}

		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error for non-writable cursor")
		}
	})

	t.Run("DeleteInvalidBtreeCursor", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		cursor := &Cursor{
			CurType:     CursorBTree,
			IsTable:     true,
			Writable:    true,
			BtreeCursor: &MockBTreeCursor{},  // Wrong type - not *btree.BtCursor
		}
		v.Cursors[0] = cursor

		instr := &Instruction{
			Opcode: OpDelete,
			P1:     0,
		}

		err := v.execDelete(instr)
		if err == nil {
			t.Error("Expected error for invalid btree cursor type")
		}
	})
}

// TestSeekGEAndLEComprehensive tests SeekGE and SeekLE (33.3% -> higher)
func TestSeekGEAndLEComprehensive(t *testing.T) {
	t.Run("SeekGE_Basic", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		cursor := &Cursor{
			CurType: CursorBTree,
			IsTable: true,
			BtreeCursor: &MockBTreeCursor{
				valid:       true,
				currentKey:  []byte{0, 0, 0, 0, 0, 0, 0, 5},
				currentRowid: 5,
			},
		}
		v.Cursors[0] = cursor
		v.Mem[1].SetInt(3)

		v.PC = 0
		instr := &Instruction{
			Opcode: OpSeekGE,
			P1:     0,   // cursor
			P2:     10,  // jump if not found
			P3:     1,   // register with value
		}

		err := v.execSeekGE(instr)
		if err != nil {
			t.Fatalf("execSeekGE failed: %v", err)
		}

		// Cursor should not be at EOF
		if cursor.EOF {
			t.Error("Cursor should not be at EOF after seek")
		}
	})

	t.Run("SeekGE_ErrorNoCursor", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		v.Mem[1].SetInt(100)

		v.PC = 0
		instr := &Instruction{
			Opcode: OpSeekGE,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		// Should error since cursor is nil
		err := v.execSeekGE(instr)
		if err == nil {
			t.Error("Expected error when cursor is nil")
		}
	})

	t.Run("SeekLE_Basic", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		cursor := &Cursor{
			CurType: CursorBTree,
			IsTable: true,
			BtreeCursor: &MockBTreeCursor{
				valid:       true,
				currentKey:  []byte{0, 0, 0, 0, 0, 0, 0, 3},
				currentRowid: 3,
			},
		}
		v.Cursors[0] = cursor
		v.Mem[1].SetInt(5)

		v.PC = 0
		instr := &Instruction{
			Opcode: OpSeekLE,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		err := v.execSeekLE(instr)
		if err != nil {
			t.Fatalf("execSeekLE failed: %v", err)
		}

		// Cursor should not be at EOF
		if cursor.EOF {
			t.Error("Cursor should not be at EOF after seek")
		}
	})

	t.Run("SeekLE_ErrorNoCursor", func(t *testing.T) {
		v := NewTestVDBE(20)
		v.AllocCursors(5)

		v.Mem[1].SetInt(1)

		v.PC = 0
		instr := &Instruction{
			Opcode: OpSeekLE,
			P1:     0,
			P2:     10,
			P3:     1,
		}

		// Should error since cursor is nil
		err := v.execSeekLE(instr)
		if err == nil {
			t.Error("Expected error when cursor is nil")
		}
	})
}

// MockBTreeCursor for testing
type MockBTreeCursor struct {
	valid        bool
	currentKey   []byte
	currentRowid int64
	deleted      bool
	inserted     bool
}

func (c *MockBTreeCursor) Valid() bool {
	return c.valid
}

func (c *MockBTreeCursor) Key() []byte {
	return c.currentKey
}

func (c *MockBTreeCursor) Rowid() int64 {
	return c.currentRowid
}

func (c *MockBTreeCursor) Delete() error {
	c.deleted = true
	c.valid = false
	return nil
}

func (c *MockBTreeCursor) Insert(key []byte, value []byte) error {
	c.inserted = true
	c.currentKey = key
	return nil
}

func (c *MockBTreeCursor) First() error {
	c.valid = len(c.currentKey) > 0
	return nil
}

func (c *MockBTreeCursor) Last() error {
	c.valid = len(c.currentKey) > 0
	return nil
}

func (c *MockBTreeCursor) Next() error {
	return nil
}

func (c *MockBTreeCursor) Prev() error {
	return nil
}

func (c *MockBTreeCursor) Seek(key []byte) (bool, error) {
	return c.valid, nil
}

func (c *MockBTreeCursor) SeekGE(key []byte) (bool, error) {
	return c.valid, nil
}

func (c *MockBTreeCursor) SeekLE(key []byte) (bool, error) {
	return c.valid, nil
}

func (c *MockBTreeCursor) Close() error {
	c.valid = false
	return nil
}
