// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"strings"
	"testing"
)

// TestDebug2TraceInstructionAfterNilDebug covers the v.Debug == nil early
// return path in TraceInstructionAfter.
func TestDebug2TraceInstructionAfterNilDebug(t *testing.T) {
	t.Parallel()
	v := New()
	// Ensure Debug is nil so the nil guard is exercised.
	v.Debug = nil
	instr := &Instruction{Opcode: OpHalt}
	// Must not panic.
	v.TraceInstructionAfter(0, instr)
}

// TestDebug2LogInstructionUnlimitedLog covers the MaxLogSize == 0 branch in
// logInstruction (unlimited log — no trimming occurs).
func TestDebug2LogInstructionUnlimitedLog(t *testing.T) {
	t.Parallel()
	v := New()
	v.SetDebugMode(DebugTrace)
	// MaxLogSize == 0 means unlimited; the trim branch is never entered.
	v.Debug.MaxLogSize = 0

	instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
	for i := 0; i < 20; i++ {
		v.logInstruction(i, instr, "")
	}

	log := v.GetInstructionLog()
	if len(log) != 20 {
		t.Errorf("expected 20 log entries with unlimited log, got %d", len(log))
	}
}

// TestDebug2LogInstructionNilDebug covers the v.Debug == nil early return in
// logInstruction, called directly (not via TraceInstruction which also guards).
func TestDebug2LogInstructionNilDebug(t *testing.T) {
	t.Parallel()
	v := New()
	v.Debug = nil
	instr := &Instruction{Opcode: OpHalt}
	// Must not panic.
	v.logInstruction(0, instr, "TAG")
}

// TestDebug2DumpProgramEmptyProgram covers the early return "Empty program"
// path in DumpProgram when the program has no instructions.
func TestDebug2DumpProgramEmptyProgram(t *testing.T) {
	t.Parallel()
	v := New()
	// Program is empty — no instructions added.
	out := v.DumpProgram()
	if out != "Empty program" {
		t.Errorf("DumpProgram() with empty program = %q, want %q", out, "Empty program")
	}
}

// TestDebug2DumpProgramCurrentPCMarker covers the ">>> " marker branch in
// DumpProgram when an instruction is the current program counter.
func TestDebug2DumpProgramCurrentPCMarker(t *testing.T) {
	t.Parallel()
	v := New()
	v.AddOp(OpInteger, 1, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	// Set PC to 1 so the second instruction gets the ">>> " marker.
	v.PC = 1

	out := v.DumpProgram()
	if !strings.Contains(out, ">>>") {
		t.Errorf("expected '>>>' current-PC marker in DumpProgram output:\n%s", out)
	}
}

// TestDebug2DumpProgramBreakpointMarker covers the "BP> " marker branch in
// DumpProgram when a breakpoint is set at an instruction address.
func TestDebug2DumpProgramBreakpointMarker(t *testing.T) {
	t.Parallel()
	v := New()
	v.AddOp(OpInteger, 1, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)
	// Set PC to something other than 0 so the breakpoint branch is taken
	// rather than being shadowed by the PC marker.
	v.PC = 1
	v.SetDebugMode(DebugTrace)
	v.AddBreakpoint(0)

	out := v.DumpProgram()
	if !strings.Contains(out, "BP>") {
		t.Errorf("expected 'BP>' breakpoint marker in DumpProgram output:\n%s", out)
	}
}

// TestDebug2DumpBasicStateWithError covers the v.ErrorMsg != "" branch in
// dumpBasicState.
func TestDebug2DumpBasicStateWithError(t *testing.T) {
	t.Parallel()
	v := New()
	v.ErrorMsg = "something went wrong"
	v.AddOp(OpHalt, 0, 0, 0)

	var sb strings.Builder
	v.dumpBasicState(&sb)

	out := sb.String()
	if !strings.Contains(out, "something went wrong") {
		t.Errorf("expected error message in dumpBasicState output:\n%s", out)
	}
}

// TestDebug2DumpStatisticsNilStats covers the v.Stats == nil early-return path
// in dumpStatistics.
func TestDebug2DumpStatisticsNilStats(t *testing.T) {
	t.Parallel()
	v := New()
	v.Stats = nil

	var sb strings.Builder
	v.dumpStatistics(&sb)

	// Nothing should be written when Stats is nil.
	if sb.Len() != 0 {
		t.Errorf("expected empty output for nil Stats, got: %q", sb.String())
	}
}

// TestDebug2CaptureRegisterSnapshotNilDebug covers the v.Debug == nil early
// return inside captureRegisterSnapshot, called directly.
func TestDebug2CaptureRegisterSnapshotNilDebug(t *testing.T) {
	t.Parallel()
	v := New()
	v.Debug = nil
	// Must not panic.
	v.captureRegisterSnapshot(0)
}

// TestDebug2LogAffectedRegistersNilDebug covers the v.Debug == nil early return
// inside logAffectedRegisters, called directly.
func TestDebug2LogAffectedRegistersNilDebug(t *testing.T) {
	t.Parallel()
	v := New()
	v.AllocMemory(3) //nolint:errcheck
	v.Debug = nil
	instr := &Instruction{Opcode: OpAdd, P1: 0, P2: 1, P3: 2}
	// Must not panic.
	v.logAffectedRegisters(0, instr)
}

// TestDebug2GetCursorTypeNameDefault covers the default "UNKNOWN" branch in
// getCursorTypeName for an unrecognised cursor type value.
func TestDebug2GetCursorTypeNameDefault(t *testing.T) {
	t.Parallel()
	// Use a value outside the defined constants.
	got := getCursorTypeName(CursorType(255))
	if got != "UNKNOWN" {
		t.Errorf("getCursorTypeName(255) = %q, want %q", got, "UNKNOWN")
	}
}

// TestDebug2LogInstructionToObservabilityNilDebug covers the v.Debug == nil
// early return in logInstructionToObservability, called directly.
func TestDebug2LogInstructionToObservabilityNilDebug(t *testing.T) {
	t.Parallel()
	v := New()
	v.Debug = nil
	instr := &Instruction{Opcode: OpHalt}
	// Must not panic.
	v.logInstructionToObservability(0, instr)
}

// TestDebug2LogInstructionToObservabilityNilLogger covers the path where
// Debug is non-nil but Logger is nil (the if-Logger branch is skipped).
func TestDebug2LogInstructionToObservabilityNilLogger(t *testing.T) {
	t.Parallel()
	v := New()
	v.SetDebugMode(DebugTrace)
	// Logger remains nil — the inner if block is not entered.
	v.Debug.Logger = nil
	instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
	// Must not panic.
	v.logInstructionToObservability(0, instr)
}
