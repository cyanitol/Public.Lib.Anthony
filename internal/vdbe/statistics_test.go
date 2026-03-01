package vdbe

import (
	"testing"
)

// TestQueryStatistics verifies that query statistics tracking works.
func TestQueryStatistics(t *testing.T) {
	vdbe := New()

	// Verify statistics are enabled by default
	if vdbe.Stats == nil {
		t.Fatal("Statistics should be enabled by default")
	}

	// Test basic statistics initialization
	stats := vdbe.GetStatistics()
	if stats == nil {
		t.Fatal("GetStatistics should not return nil")
	}

	if stats.NumInstructions != 0 {
		t.Errorf("Expected 0 instructions initially, got %d", stats.NumInstructions)
	}
}

// TestStatisticsRecordInstruction tests instruction recording.
func TestStatisticsRecordInstruction(t *testing.T) {
	stats := NewQueryStatistics()

	// Record some instructions
	stats.RecordInstruction(OpInteger)
	stats.RecordInstruction(OpGoto)
	stats.RecordInstruction(OpEq)

	if stats.NumInstructions != 3 {
		t.Errorf("Expected 3 instructions, got %d", stats.NumInstructions)
	}

	if stats.NumJumps != 1 {
		t.Errorf("Expected 1 jump (OpGoto), got %d", stats.NumJumps)
	}

	if stats.NumComparisons != 1 {
		t.Errorf("Expected 1 comparison (OpEq), got %d", stats.NumComparisons)
	}
}

// TestStatisticsRowTracking tests row read/write tracking.
func TestStatisticsRowTracking(t *testing.T) {
	stats := NewQueryStatistics()

	// Record row operations
	stats.RecordRowScanned()
	stats.RecordRowScanned()
	stats.RecordRowScanned()
	stats.RowsRead = 2
	stats.RowsWritten = 1

	if stats.RowsScanned != 3 {
		t.Errorf("Expected 3 rows scanned, got %d", stats.RowsScanned)
	}

	if stats.RowsRead != 2 {
		t.Errorf("Expected 2 rows read, got %d", stats.RowsRead)
	}

	if stats.RowsWritten != 1 {
		t.Errorf("Expected 1 row written, got %d", stats.RowsWritten)
	}
}

// TestStatisticsCursorTracking tests cursor operation tracking.
func TestStatisticsCursorTracking(t *testing.T) {
	stats := NewQueryStatistics()

	// Record cursor operations
	stats.RecordInstruction(OpRewind)
	stats.RecordInstruction(OpSeekGE)
	stats.RecordInstruction(OpNext)
	stats.RecordInstruction(OpNext)

	if stats.CursorSeeks != 2 {
		t.Errorf("Expected 2 cursor seeks (OpRewind, OpSeekGE), got %d", stats.CursorSeeks)
	}

	if stats.CursorSteps != 2 {
		t.Errorf("Expected 2 cursor steps (2x OpNext), got %d", stats.CursorSteps)
	}
}

// TestStatisticsMemoryTracking tests memory usage tracking.
func TestStatisticsMemoryTracking(t *testing.T) {
	stats := NewQueryStatistics()

	// Update memory usage
	stats.UpdateMemoryUsage(1024)
	if stats.MemoryUsed != 1024 {
		t.Errorf("Expected memory usage of 1024, got %d", stats.MemoryUsed)
	}

	// Update with smaller value - should not change
	stats.UpdateMemoryUsage(512)
	if stats.MemoryUsed != 1024 {
		t.Errorf("Expected memory usage to remain 1024 (peak), got %d", stats.MemoryUsed)
	}

	// Update with larger value - should update
	stats.UpdateMemoryUsage(2048)
	if stats.MemoryUsed != 2048 {
		t.Errorf("Expected memory usage to update to 2048, got %d", stats.MemoryUsed)
	}

	// Test sorter memory
	stats.UpdateSorterMemory(512)
	if stats.SorterMemory != 512 {
		t.Errorf("Expected sorter memory of 512, got %d", stats.SorterMemory)
	}
}

// TestStatisticsTiming tests timing functionality.
func TestStatisticsTiming(t *testing.T) {
	stats := NewQueryStatistics()

	// Start and end timing
	stats.Start()
	if stats.StartTime == 0 {
		t.Error("StartTime should be set after Start()")
	}

	stats.End()
	if stats.EndTime == 0 {
		t.Error("EndTime should be set after End()")
	}

	if stats.ExecutionNS < 0 {
		t.Errorf("ExecutionNS should be non-negative, got %d", stats.ExecutionNS)
	}

	// ExecutionMS should be derived from ExecutionNS
	expectedMS := stats.ExecutionNS / 1000000
	if stats.ExecutionMS != expectedMS {
		t.Errorf("Expected ExecutionMS to be %d, got %d", expectedMS, stats.ExecutionMS)
	}
}

// TestStatisticsString tests the String() method.
func TestStatisticsString(t *testing.T) {
	stats := NewQueryStatistics()
	stats.NumInstructions = 100
	stats.RowsRead = 10
	stats.RowsWritten = 5
	stats.QueryType = "SELECT"
	stats.IsReadOnly = true

	str := stats.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}

	// Check that output contains key information
	if !containsSubstr(str, "Query Statistics") {
		t.Error("String() should contain 'Query Statistics'")
	}
	if !containsSubstr(str, "Instructions: 100") {
		t.Error("String() should contain instruction count")
	}
	if !containsSubstr(str, "Rows Read: 10") {
		t.Error("String() should contain rows read")
	}
	if !containsSubstr(str, "Query Type: SELECT") {
		t.Error("String() should contain query type")
	}
}

// TestVDBEStatisticsIntegration tests statistics integration with VDBE execution.
func TestVDBEStatisticsIntegration(t *testing.T) {
	vdbe := New()

	// Add a simple program
	vdbe.AllocMemory(3)
	vdbe.AddOp(OpInteger, 0, 1, 42)    // Load 42 into register 1
	vdbe.AddOp(OpInteger, 0, 2, 10)    // Load 10 into register 2
	vdbe.AddOp(OpResultRow, 1, 2, 0)   // Return registers 1 and 2
	vdbe.AddOp(OpHalt, 0, 0, 0)        // Halt

	// Run the program
	err := vdbe.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Check statistics
	stats := vdbe.GetStatistics()
	if stats == nil {
		t.Fatal("GetStatistics should not return nil")
	}

	// Should have executed at least 4 instructions
	if stats.NumInstructions < 4 {
		t.Errorf("Expected at least 4 instructions, got %d", stats.NumInstructions)
	}

	// Should have returned 1 row
	if stats.RowsRead != 1 {
		t.Errorf("Expected 1 row read, got %d", stats.RowsRead)
	}

	// Timing should be set
	if stats.StartTime == 0 {
		t.Error("StartTime should be set after execution")
	}

	if stats.EndTime == 0 {
		t.Error("EndTime should be set after execution")
	}
}

// TestVDBEReset tests that Reset() clears statistics.
func TestVDBEReset(t *testing.T) {
	vdbe := New()

	// Add and run a simple program
	vdbe.AllocMemory(2)
	vdbe.AddOp(OpInteger, 0, 1, 100)
	vdbe.AddOp(OpResultRow, 1, 1, 0)
	vdbe.AddOp(OpHalt, 0, 0, 0)

	err := vdbe.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify statistics were recorded
	stats := vdbe.GetStatistics()
	if stats.NumInstructions == 0 {
		t.Error("Statistics should have been recorded")
	}

	// Reset
	err = vdbe.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify statistics were reset
	stats = vdbe.GetStatistics()
	if stats.NumInstructions != 0 {
		t.Errorf("Statistics should be reset, but NumInstructions = %d", stats.NumInstructions)
	}
}

// TestStatisticsEnableDisable tests enabling/disabling statistics.
func TestStatisticsEnableDisable(t *testing.T) {
	vdbe := New()

	// Statistics should be enabled by default
	if vdbe.Stats == nil {
		t.Error("Statistics should be enabled by default")
	}

	// Disable statistics
	vdbe.DisableStatistics()
	if vdbe.Stats != nil {
		t.Error("Statistics should be disabled")
	}

	// Re-enable statistics
	vdbe.EnableStatistics()
	if vdbe.Stats == nil {
		t.Error("Statistics should be re-enabled")
	}
}

// Helper function to check if a string contains a substring.
func containsSubstr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[0:len(substr)] == substr || containsSubstr(s[1:], substr))))
}
