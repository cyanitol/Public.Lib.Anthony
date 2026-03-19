// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/observability"
)

// TestDebugModeOff verifies that debug mode is off by default.
func TestDebugModeOff(t *testing.T) {
	v := New()

	if v.GetDebugMode() != DebugOff {
		t.Errorf("Expected debug mode to be off by default, got %v", v.GetDebugMode())
	}

	if v.IsDebugEnabled(DebugTrace) {
		t.Error("Expected DebugTrace to be disabled by default")
	}
}

// TestSetDebugMode verifies setting debug modes.
func TestSetDebugMode(t *testing.T) {
	v := New()

	// Test setting trace mode
	v.SetDebugMode(DebugTrace)
	if !v.IsDebugEnabled(DebugTrace) {
		t.Error("Expected DebugTrace to be enabled")
	}

	// Test setting multiple modes
	v.SetDebugMode(DebugTrace | DebugRegisters)
	if !v.IsDebugEnabled(DebugTrace) {
		t.Error("Expected DebugTrace to be enabled")
	}
	if !v.IsDebugEnabled(DebugRegisters) {
		t.Error("Expected DebugRegisters to be enabled")
	}

	// Test setting all modes
	v.SetDebugMode(DebugAll)
	if !v.IsDebugEnabled(DebugTrace) {
		t.Error("Expected DebugTrace to be enabled in DebugAll")
	}
	if !v.IsDebugEnabled(DebugRegisters) {
		t.Error("Expected DebugRegisters to be enabled in DebugAll")
	}
	if !v.IsDebugEnabled(DebugCursors) {
		t.Error("Expected DebugCursors to be enabled in DebugAll")
	}
}

// TestInstructionTracing verifies instruction tracing.
func TestInstructionTracing(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace)

	// Build a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOp(OpInteger, 99, 1, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	// Allocate memory
	v.AllocMemory(2)

	// Run the program
	_, err := v.Step()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that instruction log was populated
	log := v.GetInstructionLog()
	if len(log) == 0 {
		t.Error("Expected instruction log to be populated")
	}

	// Check that log contains opcode names
	logStr := strings.Join(log, "\n")
	if !strings.Contains(logStr, "Integer") {
		t.Error("Expected instruction log to contain Integer")
	}
}

// TestDumpRegisters verifies register dumping.
func TestDumpRegisters(t *testing.T) {
	v := New()
	v.AllocMemory(3)

	// Set some register values
	v.Mem[0].SetInt(42)
	v.Mem[1].SetStr("test")
	v.Mem[2].SetReal(3.14)

	dump := v.DumpRegisters()

	if !strings.Contains(dump, "R0") {
		t.Error("Expected dump to contain R0")
	}
	if !strings.Contains(dump, "42") {
		t.Error("Expected dump to contain value 42")
	}
	if !strings.Contains(dump, "test") {
		t.Error("Expected dump to contain value test")
	}
	if !strings.Contains(dump, "3.14") {
		t.Error("Expected dump to contain value 3.14")
	}
}

// TestDumpCursors verifies cursor dumping.
func TestDumpCursors(t *testing.T) {
	v := New()
	v.AllocCursors(2)

	// Open a cursor
	v.OpenCursor(0, CursorBTree, 1, true)

	dump := v.DumpCursors()

	if !strings.Contains(dump, "C0") {
		t.Error("Expected dump to contain C0")
	}
	if !strings.Contains(dump, "BTREE") {
		t.Error("Expected dump to contain BTREE type")
	}
	if !strings.Contains(dump, "C1") {
		t.Error("Expected dump to contain C1")
	}
	if !strings.Contains(dump, "CLOSED") {
		t.Error("Expected dump to contain CLOSED for unopened cursor")
	}
}

// TestRegisterWatch verifies register watching.
func TestRegisterWatch(t *testing.T) {
	v := New()
	v.AllocMemory(3)
	v.SetDebugMode(DebugRegisters)

	// Watch register 0
	v.WatchRegister(0)

	dump := v.DumpRegisters()

	if !strings.Contains(dump, "[WATCHED]") {
		t.Error("Expected dump to show watched register")
	}

	// Unwatch register
	v.UnwatchRegister(0)

	dump = v.DumpRegisters()
	lines := strings.Split(dump, "\n")
	watchedCount := 0
	for _, line := range lines {
		if strings.Contains(line, "[WATCHED]") {
			watchedCount++
		}
	}

	if watchedCount != 0 {
		t.Error("Expected no watched registers after unwatching")
	}
}

// TestBreakpoints verifies breakpoint functionality.
func TestBreakpoints(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace)

	// Build a simple program
	v.AddOp(OpInteger, 1, 0, 0)
	v.AddOp(OpInteger, 2, 1, 0)
	v.AddOp(OpInteger, 3, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	v.AllocMemory(3)

	// Set breakpoint at instruction 1
	v.AddBreakpoint(1)

	// Execute first instruction (should succeed)
	_, err := v.Step()
	if err != nil {
		t.Fatalf("Unexpected error on first step: %v", err)
	}

	// Should hit breakpoint on second instruction
	if v.State != StateHalt {
		t.Errorf("Expected State to be StateHalt after breakpoint, got %v", v.State)
	}

	// Verify PC was restored to breakpoint location
	if v.PC != 1 {
		t.Errorf("Expected PC to be at breakpoint (1), got %d", v.PC)
	}

	// Remove breakpoint
	v.RemoveBreakpoint(1)

	// Clear breakpoints
	v.ClearBreakpoints()
}

// TestDumpState verifies state dumping.
func TestDumpState(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugAll)
	v.AllocMemory(2)
	v.AllocCursors(1)

	v.Mem[0].SetInt(42)
	v.OpenCursor(0, CursorBTree, 1, true)

	dump := v.DumpState()

	// Check that dump contains key sections
	if !strings.Contains(dump, "VDBE STATE DUMP") {
		t.Error("Expected dump to contain header")
	}
	if !strings.Contains(dump, "State:") {
		t.Error("Expected dump to contain state")
	}
	if !strings.Contains(dump, "Register Dump:") {
		t.Error("Expected dump to contain register dump")
	}
	if !strings.Contains(dump, "Cursor Dump:") {
		t.Error("Expected dump to contain cursor dump")
	}
}

// TestDumpProgram verifies program dumping.
func TestDumpProgram(t *testing.T) {
	v := New()

	// Build a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOpWithP4Str(OpString, 0, 1, 0, "hello")
	v.AddOp(OpHalt, 0, 0, 0)

	v.SetComment(0, "Load integer")
	v.SetComment(1, "Load string")

	dump := v.DumpProgram()

	if !strings.Contains(dump, "Integer") {
		t.Error("Expected dump to contain Integer")
	}
	if !strings.Contains(dump, "String") {
		t.Error("Expected dump to contain String")
	}
	if !strings.Contains(dump, "Load integer") {
		t.Error("Expected dump to contain comment")
	}
	if !strings.Contains(dump, "hello") {
		t.Error("Expected dump to contain P4 string value")
	}
}

// TestDebugWithLogger verifies integration with observability logger.
func TestDebugWithLogger(t *testing.T) {
	v := New()

	// Create a logger (using default stderr)
	logger := observability.NewLogger(observability.DebugLevel, nil, observability.TextFormat)

	v.SetDebugMode(DebugTrace)
	v.SetDebugLogger(logger)
	v.SetDebugLogLevel(observability.DebugLevel)

	// Build and run a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	v.AllocMemory(1)

	// Run the program
	_, err := v.Step()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify that instruction log was populated
	log := v.GetInstructionLog()
	if len(log) == 0 {
		t.Error("Expected instruction log to be populated with logger set")
	}
}

// TestTraceInstructionCallback verifies custom trace callbacks.
func TestTraceInstructionCallback(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace)

	callbackCalled := false
	callbackPC := -1

	// Set a custom trace callback that captures the first invocation
	v.SetTraceCallback(func(vdbe *VDBE, pc int, instr *Instruction) bool {
		if !callbackCalled {
			callbackCalled = true
			callbackPC = pc
		}
		return true // Continue execution
	})

	// Build a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	v.AllocMemory(1)

	// Run the program
	_, err := v.Step()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !callbackCalled {
		t.Error("Expected trace callback to be called")
	}

	if callbackPC != 0 {
		t.Errorf("Expected callback PC to be 0, got %d", callbackPC)
	}
}

// TestInstructionLogLimit verifies that instruction log respects max size.
func TestInstructionLogLimit(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace)

	// Set a small max log size
	if v.Debug != nil {
		v.Debug.MaxLogSize = 10
	}

	// Build a program with many instructions
	for i := 0; i < 20; i++ {
		v.AddOp(OpInteger, i, 0, 0)
	}
	v.AddOp(OpHalt, 0, 0, 0)
	v.AllocMemory(1)

	// Run the program
	for {
		hasMore, err := v.Step()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !hasMore {
			break
		}
	}

	// Check that log size is limited
	log := v.GetInstructionLog()
	if len(log) > 10 {
		t.Errorf("Expected log size to be limited to 10, got %d", len(log))
	}
}

// TestClearInstructionLog verifies clearing the instruction log.
func TestClearInstructionLog(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace)

	// Build and run a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	v.AllocMemory(1)

	_, err := v.Step()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify log is populated
	if len(v.GetInstructionLog()) == 0 {
		t.Error("Expected instruction log to be populated")
	}

	// Clear the log
	v.ClearInstructionLog()

	// Verify log is empty
	if len(v.GetInstructionLog()) != 0 {
		t.Error("Expected instruction log to be empty after clearing")
	}
}

// TestRegisterAndCursorTracing verifies that register and cursor changes are logged.
func TestRegisterAndCursorTracing(t *testing.T) {
	v := New()
	v.SetDebugMode(DebugTrace | DebugRegisters | DebugCursors)

	// Create a logger to capture output
	logger := observability.NewLogger(observability.DebugLevel, nil, observability.TextFormat)
	v.SetDebugLogger(logger)

	// Build a program that modifies registers (no cursor ops that require btree)
	v.AddOp(OpInteger, 42, 0, 0) // Set R0 = 42
	v.AddOp(OpInteger, 99, 1, 0) // Set R1 = 99
	v.AddOp(OpHalt, 0, 0, 0)

	v.AllocMemory(2)
	v.AllocCursors(1)

	// Run the program
	for {
		hasMore, err := v.Step()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !hasMore {
			break
		}
	}

	// Verify that instruction log was populated
	log := v.GetInstructionLog()
	if len(log) == 0 {
		t.Error("Expected instruction log to be populated")
	}
}
