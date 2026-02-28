package vdbe

import (
	"testing"
)

// TestWindowStateCreation tests basic window state creation
func TestWindowStateCreation(t *testing.T) {
	t.Parallel()
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	if ws == nil {
		t.Fatal("NewWindowState returned nil")
	}

	if len(ws.Partitions) != 0 {
		t.Errorf("Expected 0 partitions initially, got %d", len(ws.Partitions))
	}
}

// TestWindowStateAddRow tests adding rows to window state
func TestWindowStateAddRow(t *testing.T) {
	t.Parallel()
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add first row
	row1 := []*Mem{NewMemInt(10), NewMemStr("A")}
	ws.AddRow(row1)

	if len(ws.Partitions) != 1 {
		t.Fatalf("Expected 1 partition after adding row, got %d", len(ws.Partitions))
	}

	if len(ws.Partitions[0].Rows) != 1 {
		t.Errorf("Expected 1 row in partition, got %d", len(ws.Partitions[0].Rows))
	}

	// Add second row
	row2 := []*Mem{NewMemInt(20), NewMemStr("B")}
	ws.AddRow(row2)

	if len(ws.Partitions[0].Rows) != 2 {
		t.Errorf("Expected 2 rows in partition, got %d", len(ws.Partitions[0].Rows))
	}
}

// TestWindowStatePartitioning tests partitioning behavior
func TestWindowStatePartitioning(t *testing.T) {
	t.Parallel()
	// Create window state with partition by column 1
	partitionCols := []int{1}
	ws := NewWindowState(partitionCols, nil, nil, DefaultWindowFrame())

	// Add rows with different partition values
	ws.AddRow([]*Mem{NewMemInt(1), NewMemStr("A")})
	ws.AddRow([]*Mem{NewMemInt(2), NewMemStr("A")}) // Same partition
	ws.AddRow([]*Mem{NewMemInt(3), NewMemStr("B")}) // Different partition

	if len(ws.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(ws.Partitions))
	}

	if len(ws.Partitions[0].Rows) != 2 {
		t.Errorf("Expected 2 rows in first partition, got %d", len(ws.Partitions[0].Rows))
	}

	if len(ws.Partitions[1].Rows) != 1 {
		t.Errorf("Expected 1 row in second partition, got %d", len(ws.Partitions[1].Rows))
	}
}

// TestWindowFrameDefault tests default frame behavior
func TestWindowFrameDefault(t *testing.T) {
	t.Parallel()
	frame := DefaultWindowFrame()

	if frame.Type != FrameRange {
		t.Errorf("Expected default frame type RANGE, got %v", frame.Type)
	}

	if frame.Start.Type != BoundUnboundedPreceding {
		t.Errorf("Expected start bound UNBOUNDED PRECEDING, got %v", frame.Start.Type)
	}

	if frame.End.Type != BoundCurrentRow {
		t.Errorf("Expected end bound CURRENT ROW, got %v", frame.End.Type)
	}
}

// TestWindowStateNextRow tests row iteration
func TestWindowStateNextRow(t *testing.T) {
	t.Parallel()
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	// Add test rows
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.AddRow([]*Mem{NewMemInt(20)})
	ws.AddRow([]*Mem{NewMemInt(30)})

	// Iterate through rows
	count := 0
	for row := ws.NextRow(); row != nil; row = ws.NextRow() {
		count++
		expectedValue := int64(count * 10)
		if row[0].IntValue() != expectedValue {
			t.Errorf("Row %d: expected value %d, got %d", count, expectedValue, row[0].IntValue())
		}
	}

	if count != 3 {
		t.Errorf("Expected to iterate 3 rows, got %d", count)
	}
}

// TestWindowStateRowNumber tests row numbering
func TestWindowStateRowNumber(t *testing.T) {
	t.Parallel()
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())

	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(3)})

	// Test row numbers
	expectedRowNums := []int64{1, 2, 3}
	for i, expected := range expectedRowNums {
		ws.NextRow()
		rowNum := ws.GetCurrentRowNumber()
		if rowNum != expected {
			t.Errorf("Row %d: expected row_number=%d, got %d", i, expected, rowNum)
		}
	}
}
