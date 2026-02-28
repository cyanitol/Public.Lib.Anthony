package vdbe

import (
	"testing"
)

// TestWindowRowNumber tests the ROW_NUMBER() window function
func TestWindowRowNumber(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create window state with sample data
	windowState := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add rows to window state
	rows := [][]*Mem{
		{NewMemInt(1), NewMemStr("A")},
		{NewMemInt(2), NewMemStr("B")},
		{NewMemInt(3), NewMemStr("C")},
		{NewMemInt(4), NewMemStr("D")},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// Test ROW_NUMBER for each row
	expectedRowNums := []int64{1, 2, 3, 4}

	for i, expected := range expectedRowNums {
		windowState.NextRow()

		// Create and execute OpWindowRowNum instruction
		instr := &Instruction{
			Opcode: OpWindowRowNum,
			P1:     0, // window state index
			P2:     0, // output register
		}

		err := v.execWindowRowNum(instr)
		if err != nil {
			t.Fatalf("execWindowRowNum failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected row_number=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowRank tests the RANK() window function
func TestWindowRank(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create window state with ORDER BY column
	orderByCols := []int{0}
	windowState := NewWindowState(nil, orderByCols, nil, DefaultWindowFrame())

	// Add rows with duplicate values for testing rank gaps
	rows := [][]*Mem{
		{NewMemInt(10)},
		{NewMemInt(10)}, // Same as previous - same rank
		{NewMemInt(20)},
		{NewMemInt(20)}, // Same as previous - same rank
		{NewMemInt(30)},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// Expected ranks: 1, 1, 3, 3, 5 (note the gaps)
	expectedRanks := []int64{1, 1, 3, 3, 5}

	for i, expected := range expectedRanks {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowRank,
			P1:     0,
			P2:     0,
		}

		err := v.execWindowRank(instr)
		if err != nil {
			t.Fatalf("execWindowRank failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected rank=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowDenseRank tests the DENSE_RANK() window function
func TestWindowDenseRank(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	orderByCols := []int{0}
	windowState := NewWindowState(nil, orderByCols, nil, DefaultWindowFrame())

	// Add rows with duplicate values
	rows := [][]*Mem{
		{NewMemInt(10)},
		{NewMemInt(10)}, // Same as previous
		{NewMemInt(20)},
		{NewMemInt(20)}, // Same as previous
		{NewMemInt(30)},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// Expected dense ranks: 1, 1, 2, 2, 3 (no gaps)
	expectedRanks := []int64{1, 1, 2, 2, 3}

	for i, expected := range expectedRanks {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowDenseRank,
			P1:     0,
			P2:     0,
		}

		err := v.execWindowDenseRank(instr)
		if err != nil {
			t.Fatalf("execWindowDenseRank failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected dense_rank=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowNtile tests the NTILE() window function
func TestWindowNtile(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	windowState := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add 10 rows
	for i := 1; i <= 10; i++ {
		windowState.AddRow([]*Mem{NewMemInt(int64(i))})
	}

	v.WindowStates[0] = windowState

	// Test NTILE(4) - divide into 4 buckets
	// Expected: 1,1,1, 2,2,2, 3,3, 4,4
	expectedBuckets := []int64{1, 1, 1, 2, 2, 2, 3, 3, 4, 4}

	for i, expected := range expectedBuckets {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowNtile,
			P1:     0,
			P2:     0,
			P3:     4, // 4 buckets
		}

		err := v.execWindowNtile(instr)
		if err != nil {
			t.Fatalf("execWindowNtile failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected ntile=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowLag tests the LAG() window function
func TestWindowLag(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	windowState := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add rows
	rows := [][]*Mem{
		{NewMemInt(10), NewMemStr("A")},
		{NewMemInt(20), NewMemStr("B")},
		{NewMemInt(30), NewMemStr("C")},
		{NewMemInt(40), NewMemStr("D")},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	tests := []struct {
		rowIdx      int
		offset      int
		colIdx      int
		expectNull  bool
		expectValue int64
	}{
		{0, 1, 0, true, 0},   // First row, lag 1 - should be NULL
		{1, 1, 0, false, 10}, // Second row, lag 1 - should be 10
		{2, 1, 0, false, 20}, // Third row, lag 1 - should be 20
		{2, 2, 0, false, 10}, // Third row, lag 2 - should be 10
		{3, 1, 0, false, 30}, // Fourth row, lag 1 - should be 30
	}

	for _, tt := range tests {
		// Reset and advance to target row
		windowState.CurrentPartIdx = -1
		windowState.CurrentPartRow = -1
		for i := 0; i <= tt.rowIdx; i++ {
			windowState.NextRow()
		}

		instr := &Instruction{
			Opcode: OpWindowLag,
			P1:     0,
			P2:     0,
			P3:     tt.colIdx,
			P4:     P4Union{I: int32(tt.offset)},
		}

		err := v.execWindowLag(instr)
		if err != nil {
			t.Fatalf("execWindowLag failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if tt.expectNull {
			if !result.IsNull() {
				t.Errorf("Row %d, offset %d: expected NULL, got %v", tt.rowIdx, tt.offset, result.IntValue())
			}
		} else {
			if result.IntValue() != tt.expectValue {
				t.Errorf("Row %d, offset %d: expected %d, got %d", tt.rowIdx, tt.offset, tt.expectValue, result.IntValue())
			}
		}
	}
}

// TestWindowLead tests the LEAD() window function
func TestWindowLead(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	windowState := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add rows
	rows := [][]*Mem{
		{NewMemInt(10), NewMemStr("A")},
		{NewMemInt(20), NewMemStr("B")},
		{NewMemInt(30), NewMemStr("C")},
		{NewMemInt(40), NewMemStr("D")},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	tests := []struct {
		rowIdx      int
		offset      int
		colIdx      int
		expectNull  bool
		expectValue int64
	}{
		{0, 1, 0, false, 20}, // First row, lead 1 - should be 20
		{0, 2, 0, false, 30}, // First row, lead 2 - should be 30
		{1, 1, 0, false, 30}, // Second row, lead 1 - should be 30
		{2, 1, 0, false, 40}, // Third row, lead 1 - should be 40
		{3, 1, 0, true, 0},   // Fourth row, lead 1 - should be NULL
	}

	for _, tt := range tests {
		// Reset and advance to target row
		windowState.CurrentPartIdx = -1
		windowState.CurrentPartRow = -1
		for i := 0; i <= tt.rowIdx; i++ {
			windowState.NextRow()
		}

		instr := &Instruction{
			Opcode: OpWindowLead,
			P1:     0,
			P2:     0,
			P3:     tt.colIdx,
			P4:     P4Union{I: int32(tt.offset)},
		}

		err := v.execWindowLead(instr)
		if err != nil {
			t.Fatalf("execWindowLead failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if tt.expectNull {
			if !result.IsNull() {
				t.Errorf("Row %d, offset %d: expected NULL, got %v", tt.rowIdx, tt.offset, result.IntValue())
			}
		} else {
			if result.IntValue() != tt.expectValue {
				t.Errorf("Row %d, offset %d: expected %d, got %d", tt.rowIdx, tt.offset, tt.expectValue, result.IntValue())
			}
		}
	}
}

// TestWindowFirstValue tests the FIRST_VALUE() window function
func TestWindowFirstValue(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create window with custom frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	frame := WindowFrame{
		Type: FrameRows,
		Start: WindowFrameBound{
			Type: BoundUnboundedPreceding,
		},
		End: WindowFrameBound{
			Type: BoundCurrentRow,
		},
	}
	windowState := NewWindowState(nil, nil, nil, frame)

	// Add rows
	rows := [][]*Mem{
		{NewMemInt(10)},
		{NewMemInt(20)},
		{NewMemInt(30)},
		{NewMemInt(40)},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// FIRST_VALUE should always be 10 (first row in frame)
	for i := 0; i < len(rows); i++ {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowFirstValue,
			P1:     0,
			P2:     0,
			P3:     0, // column index
		}

		err := v.execWindowFirstValue(instr)
		if err != nil {
			t.Fatalf("execWindowFirstValue failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != 10 {
			t.Errorf("Row %d: expected first_value=10, got %d", i+1, result.IntValue())
		}
	}
}

// TestWindowLastValue tests the LAST_VALUE() window function
func TestWindowLastValue(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create window with frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	frame := WindowFrame{
		Type: FrameRows,
		Start: WindowFrameBound{
			Type: BoundUnboundedPreceding,
		},
		End: WindowFrameBound{
			Type: BoundCurrentRow,
		},
	}
	windowState := NewWindowState(nil, nil, nil, frame)

	// Add rows
	rows := [][]*Mem{
		{NewMemInt(10)},
		{NewMemInt(20)},
		{NewMemInt(30)},
		{NewMemInt(40)},
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// LAST_VALUE should be the current row value (since frame ends at current row)
	expectedValues := []int64{10, 20, 30, 40}

	for i, expected := range expectedValues {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowLastValue,
			P1:     0,
			P2:     0,
			P3:     0, // column index
		}

		err := v.execWindowLastValue(instr)
		if err != nil {
			t.Fatalf("execWindowLastValue failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected last_value=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowWithPartitioning tests window functions with PARTITION BY
func TestWindowWithPartitioning(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create window with PARTITION BY column 1
	partitionCols := []int{1}
	windowState := NewWindowState(partitionCols, nil, nil, DefaultWindowFrame())

	// Add rows with different partition values
	rows := [][]*Mem{
		{NewMemInt(1), NewMemStr("A")},
		{NewMemInt(2), NewMemStr("A")}, // Same partition as row 1
		{NewMemInt(3), NewMemStr("B")}, // New partition
		{NewMemInt(4), NewMemStr("B")}, // Same partition as row 3
	}

	for _, row := range rows {
		windowState.AddRow(row)
	}

	v.WindowStates[0] = windowState

	// Expected row numbers should reset for each partition
	expectedRowNums := []int64{1, 2, 1, 2}

	for i, expected := range expectedRowNums {
		windowState.NextRow()

		instr := &Instruction{
			Opcode: OpWindowRowNum,
			P1:     0,
			P2:     0,
		}

		err := v.execWindowRowNum(instr)
		if err != nil {
			t.Fatalf("execWindowRowNum failed: %v", err)
		}

		result, _ := v.GetMem(0)
		if result.IntValue() != expected {
			t.Errorf("Row %d: expected row_number=%d, got %d", i+1, expected, result.IntValue())
		}
	}
}

// TestWindowFrameBounds tests different frame bound types
func TestWindowFrameBounds(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Test ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	frame := WindowFrame{
		Type: FrameRows,
		Start: WindowFrameBound{
			Type:   BoundPreceding,
			Offset: 1,
		},
		End: WindowFrameBound{
			Type:   BoundFollowing,
			Offset: 1,
		},
	}
	windowState := NewWindowState(nil, nil, nil, frame)

	// Add rows
	for i := 1; i <= 5; i++ {
		windowState.AddRow([]*Mem{NewMemInt(int64(i * 10))})
	}

	v.WindowStates[0] = windowState

	// Move to row 3 (value 30)
	windowState.NextRow() // Row 1
	windowState.NextRow() // Row 2
	windowState.NextRow() // Row 3

	// Frame should contain rows 2, 3, 4 (values 20, 30, 40)
	frameRows := windowState.GetFrameRows()
	if len(frameRows) != 3 {
		t.Errorf("Expected frame size 3, got %d", len(frameRows))
	}

	// Check frame values
	expectedValues := []int64{20, 30, 40}
	for i, expected := range expectedValues {
		if frameRows[i][0].IntValue() != expected {
			t.Errorf("Frame row %d: expected %d, got %d", i, expected, frameRows[i][0].IntValue())
		}
	}
}

// TestWindowStateReset tests that window states are properly managed
func TestWindowStateReset(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.WindowStates = make(map[int]*WindowState)

	// Create and populate window state
	windowState := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	windowState.AddRow([]*Mem{NewMemInt(1)})
	windowState.AddRow([]*Mem{NewMemInt(2)})

	v.WindowStates[0] = windowState

	// Verify initial state
	if len(windowState.Partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(windowState.Partitions))
	}

	if len(windowState.Partitions[0].Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(windowState.Partitions[0].Rows))
	}
}
