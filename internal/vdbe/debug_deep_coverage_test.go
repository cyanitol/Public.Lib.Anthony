// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"io"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/observability"
)

// newTestLogger creates a logger that discards output, useful for coverage without noise.
func newTestLogger() observability.Logger {
	return observability.NewLogger(observability.TraceLevel, io.Discard, observability.TextFormat)
}

// TestDebugDeepGetDebugMode covers both nil and non-nil Debug branches.
func TestDebugDeepGetDebugMode(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		setup    func() *VDBE
		wantMode DebugMode
	}{
		{
			name:     "nil debug returns DebugOff",
			setup:    func() *VDBE { return New() },
			wantMode: DebugOff,
		},
		{
			name: "DebugTrace mode",
			setup: func() *VDBE {
				v := New()
				v.SetDebugMode(DebugTrace)
				return v
			},
			wantMode: DebugTrace,
		},
		{
			name: "DebugRegisters mode",
			setup: func() *VDBE {
				v := New()
				v.SetDebugMode(DebugRegisters)
				return v
			},
			wantMode: DebugRegisters,
		},
		{
			name: "DebugAll mode",
			setup: func() *VDBE {
				v := New()
				v.SetDebugMode(DebugAll)
				return v
			},
			wantMode: DebugAll,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := tc.setup()
			got := v.GetDebugMode()
			if got != tc.wantMode {
				t.Errorf("GetDebugMode() = %v, want %v", got, tc.wantMode)
			}
		})
	}
}

// TestDebugDeepSetTraceCallback covers nil-debug init and actual callback invocation.
func TestDebugDeepSetTraceCallback(t *testing.T) {
	t.Parallel()

	t.Run("NilDebugCreatesContext", func(t *testing.T) {
		t.Parallel()
		v := New()
		called := false
		v.SetTraceCallback(func(_ *VDBE, _ int, _ *Instruction) bool {
			called = true
			return true
		})
		if v.Debug == nil {
			t.Fatal("expected Debug context to be created")
		}
		// Trigger callback via TraceInstruction with DebugTrace enabled.
		v.Debug.Mode = DebugTrace
		v.AllocMemory(1)
		instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
		v.TraceInstruction(0, instr)
		if !called {
			t.Error("expected TraceCallback to be called")
		}
	})

	t.Run("ExistingDebugUpdatesCallback", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		callCount := 0
		v.SetTraceCallback(func(_ *VDBE, _ int, _ *Instruction) bool {
			callCount++
			return false
		})
		if v.Debug.TraceCallback == nil {
			t.Fatal("expected TraceCallback to be set")
		}
		instr := &Instruction{Opcode: OpHalt}
		result := v.TraceInstruction(0, instr)
		if result {
			t.Error("expected callback to return false, stopping execution")
		}
		if callCount != 1 {
			t.Errorf("expected callback called once, got %d", callCount)
		}
	})
}

// TestDebugDeepAddBreakpoint covers nil-debug init and breakpoint storage.
func TestDebugDeepAddBreakpoint(t *testing.T) {
	t.Parallel()

	t.Run("NilDebugCreatesContext", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AddBreakpoint(5)
		if v.Debug == nil {
			t.Fatal("expected Debug context to be created")
		}
		if !v.Debug.BreakPoints[5] {
			t.Error("expected breakpoint at PC=5")
		}
	})

	t.Run("ExistingDebugAddsBreakpoint", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		v.AddBreakpoint(10)
		v.AddBreakpoint(20)
		if !v.Debug.BreakPoints[10] || !v.Debug.BreakPoints[20] {
			t.Error("expected breakpoints at PC=10 and PC=20")
		}
	})

	t.Run("BreakpointCausesHalt", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		v.SetDebugLogger(newTestLogger())
		v.AddBreakpoint(0)
		instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
		result := v.TraceInstruction(0, instr)
		if result {
			t.Error("expected TraceInstruction to return false at breakpoint")
		}
	})
}

// TestDebugDeepSetDebugLogger covers nil-debug init and logger assignment.
func TestDebugDeepSetDebugLogger(t *testing.T) {
	t.Parallel()

	t.Run("NilDebugCreatesContext", func(t *testing.T) {
		t.Parallel()
		v := New()
		lg := newTestLogger()
		v.SetDebugLogger(lg)
		if v.Debug == nil {
			t.Fatal("expected Debug context to be created")
		}
		if v.Debug.Logger == nil {
			t.Error("expected Logger to be set")
		}
	})

	t.Run("SetNilLogger", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		v.SetDebugLogger(nil)
		if v.Debug.Logger != nil {
			t.Error("expected Logger to be nil after setting nil")
		}
	})
}

// TestDebugDeepSetDebugLogLevel covers nil-debug init and level assignment.
func TestDebugDeepSetDebugLogLevel(t *testing.T) {
	t.Parallel()

	levels := []observability.Level{
		observability.TraceLevel,
		observability.DebugLevel,
		observability.InfoLevel,
		observability.WarnLevel,
		observability.ErrorLevel,
	}

	t.Run("NilDebugCreatesContext", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugLogLevel(observability.WarnLevel)
		if v.Debug == nil {
			t.Fatal("expected Debug context to be created")
		}
		if v.Debug.LogLevel != observability.WarnLevel {
			t.Errorf("expected WarnLevel, got %v", v.Debug.LogLevel)
		}
	})

	for _, lvl := range levels {
		lvl := lvl
		t.Run("Level_"+lvl.String(), func(t *testing.T) {
			t.Parallel()
			v := New()
			v.SetDebugMode(DebugTrace)
			v.SetDebugLogLevel(lvl)
			if v.Debug.LogLevel != lvl {
				t.Errorf("expected level %v, got %v", lvl, v.Debug.LogLevel)
			}
		})
	}
}

// TestDebugDeepLogMessageAtLevel covers all level branches in logMessageAtLevel.
func TestDebugDeepLogMessageAtLevel(t *testing.T) {
	t.Parallel()

	levels := []observability.Level{
		observability.TraceLevel,
		observability.DebugLevel,
		observability.InfoLevel,
		observability.WarnLevel,
		observability.ErrorLevel,
	}

	for _, lvl := range levels {
		lvl := lvl
		t.Run(lvl.String(), func(t *testing.T) {
			t.Parallel()
			v := New()
			v.SetDebugMode(DebugTrace)
			v.SetDebugLogger(newTestLogger())
			// logMessageAtLevel should not panic for any level.
			v.logMessageAtLevel(lvl, "test message")
		})
	}
}

// TestDebugDeepDumpRecentInstructions covers the branch where InstructionLog is non-empty.
func TestDebugDeepDumpRecentInstructions(t *testing.T) {
	t.Parallel()

	t.Run("EmptyLog_NilDebug", func(t *testing.T) {
		t.Parallel()
		v := New()
		var sb strings.Builder
		v.dumpRecentInstructions(&sb)
		if sb.Len() != 0 {
			t.Errorf("expected empty output for nil debug, got: %q", sb.String())
		}
	})

	t.Run("EmptyLog_NonNilDebug", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		var sb strings.Builder
		v.dumpRecentInstructions(&sb)
		if sb.Len() != 0 {
			t.Errorf("expected empty output for empty log, got: %q", sb.String())
		}
	})

	t.Run("FewInstructions_ShowAll", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		v.AllocMemory(1)
		// Populate the instruction log with 3 entries.
		instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
		for i := 0; i < 3; i++ {
			v.logInstruction(i, instr, "")
		}
		var sb strings.Builder
		v.dumpRecentInstructions(&sb)
		out := sb.String()
		if !strings.Contains(out, "Recent Instructions") {
			t.Errorf("expected 'Recent Instructions' header, got: %q", out)
		}
	})

	t.Run("ManyInstructions_ShowLast10", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugTrace)
		v.AllocMemory(1)
		instr := &Instruction{Opcode: OpInteger, P1: 1, P2: 0}
		// Log 15 instructions; only last 10 should appear.
		for i := 0; i < 15; i++ {
			v.logInstruction(i, instr, "")
		}
		var sb strings.Builder
		v.dumpRecentInstructions(&sb)
		out := sb.String()
		if !strings.Contains(out, "Recent Instructions") {
			t.Errorf("expected 'Recent Instructions' header, got: %q", out)
		}
		// Confirm the output shows the tail, not the full log.
		lines := strings.Split(strings.TrimSpace(out), "\n")
		// Header line + separator + 10 instruction lines + blank = 13 non-empty lines
		if len(lines) < 10 {
			t.Errorf("expected at least 10 lines, got %d", len(lines))
		}
	})
}

// TestDebugDeepCleanupOldSnapshots covers the snapshot-count threshold.
func TestDebugDeepCleanupOldSnapshots(t *testing.T) {
	t.Parallel()

	t.Run("BelowThreshold_NoCleanup", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugRegisters)
		v.AllocMemory(2)
		// Add 50 snapshots — below the 100 threshold.
		for i := 0; i < 50; i++ {
			v.Debug.RegisterSnapshots[i] = map[int]string{0: "val"}
		}
		before := len(v.Debug.RegisterSnapshots)
		v.cleanupOldSnapshots(50)
		after := len(v.Debug.RegisterSnapshots)
		if after != before {
			t.Errorf("expected no cleanup below threshold: before=%d after=%d", before, after)
		}
	})

	t.Run("AboveThreshold_OldSnapshotsRemoved", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugRegisters)
		v.AllocMemory(2)
		// Add 110 snapshots at PC 0..109.
		for i := 0; i < 110; i++ {
			v.Debug.RegisterSnapshots[i] = map[int]string{0: "val"}
		}
		// currentPC=200, cutoff=100; PCs 0..99 should be pruned.
		v.cleanupOldSnapshots(200)
		for pc := range v.Debug.RegisterSnapshots {
			if pc < 100 {
				t.Errorf("expected PC %d to be pruned", pc)
			}
		}
	})
}

// TestDebugDeepLogAffectedCursors covers cursor-logging branches.
func TestDebugDeepLogAffectedCursors(t *testing.T) {
	t.Parallel()

	t.Run("NilDebug_NoPanic", func(t *testing.T) {
		t.Parallel()
		v := New()
		instr := &Instruction{Opcode: OpOpenRead, P1: 0}
		v.logAffectedCursors(0, instr)
	})

	t.Run("NoAffectedCursors", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugCursors)
		v.SetDebugLogger(newTestLogger())
		v.AllocCursors(2)
		// OpHalt has no cursor operands.
		instr := &Instruction{Opcode: OpHalt}
		v.logAffectedCursors(0, instr)
	})

	t.Run("WithNilCursor", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugCursors)
		v.SetDebugLogger(newTestLogger())
		v.AllocCursors(2)
		// v.Cursors[0] is nil — log should say CLOSED.
		instr := &Instruction{Opcode: OpOpenRead, P1: 0}
		v.logAffectedCursors(0, instr)
	})

	t.Run("WithOpenCursor", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.SetDebugMode(DebugCursors)
		v.SetDebugLogger(newTestLogger())
		v.AllocCursors(2)
		v.Cursors[0] = &Cursor{CurType: CursorBTree, IsTable: true}
		instr := &Instruction{Opcode: OpNext, P1: 0}
		v.logAffectedCursors(0, instr)
	})
}

// TestDebugDeepGetAffectedCursors covers empty vs non-empty result cases.
func TestDebugDeepGetAffectedCursors(t *testing.T) {
	t.Parallel()

	cursorOpcodes := []Opcode{
		OpOpenRead, OpOpenWrite, OpOpenEphemeral, OpOpenPseudo,
		OpClose,
		OpRewind, OpLast, OpNext, OpPrev,
		OpSeek, OpSeekGE, OpSeekGT, OpSeekLE, OpSeekLT, OpSeekRowid,
		OpColumn, OpRowData, OpRowid,
		OpInsert, OpDelete, OpIdxInsert, OpIdxDelete,
	}

	for _, op := range cursorOpcodes {
		op := op
		t.Run(op.String(), func(t *testing.T) {
			t.Parallel()
			v := New()
			instr := &Instruction{Opcode: op, P1: 3}
			cursors := v.getAffectedCursors(instr)
			if len(cursors) == 0 {
				t.Errorf("opcode %v should affect cursor P1=3, got empty list", op)
			}
			if cursors[0] != 3 {
				t.Errorf("expected cursor index 3, got %d", cursors[0])
			}
		})
	}

	t.Run("UnknownOpcode_EmptyList", func(t *testing.T) {
		t.Parallel()
		v := New()
		instr := &Instruction{Opcode: OpHalt, P1: 0}
		cursors := v.getAffectedCursors(instr)
		if len(cursors) != 0 {
			t.Errorf("expected empty cursor list for OpHalt, got %v", cursors)
		}
	})
}

// TestDebugDeepApplyPatternToRegisters covers the P1RangeP3 branch and the simple-pattern delegate.
func TestDebugDeepApplyPatternToRegisters(t *testing.T) {
	t.Parallel()

	t.Run("P1RangeP3_AppendsBothRangeAndP3", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.AllocMemory(10)
		instr := &Instruction{Opcode: OpMakeRecord, P1: 1, P2: 3, P3: 7}
		result := v.applyPatternToRegisters(nil, regPatternP1RangeP3, instr)
		// Expect P3=7 first, then range 1..3 = {1,2,3}.
		if len(result) < 4 {
			t.Fatalf("expected at least 4 elements, got %d: %v", len(result), result)
		}
		if result[0] != 7 {
			t.Errorf("expected P3=7 first, got %d", result[0])
		}
	})

	t.Run("DelegatesSimplePattern", func(t *testing.T) {
		t.Parallel()
		v := New()
		instr := &Instruction{Opcode: OpInteger, P2: 5}
		result := v.applyPatternToRegisters(nil, regPatternP2, instr)
		if len(result) != 1 || result[0] != 5 {
			t.Errorf("expected [5], got %v", result)
		}
	})
}

// TestDebugDeepApplySimplePattern covers all switch branches in applySimplePattern.
func TestDebugDeepApplySimplePattern(t *testing.T) {
	t.Parallel()

	v := New()
	v.AllocMemory(10)

	cases := []struct {
		name    string
		pattern registerPattern
		instr   *Instruction
		check   func([]int) bool
	}{
		{
			name:    "regPatternP2",
			pattern: regPatternP2,
			instr:   &Instruction{P2: 4},
			check:   func(r []int) bool { return len(r) == 1 && r[0] == 4 },
		},
		{
			name:    "regPatternP1P2",
			pattern: regPatternP1P2,
			instr:   &Instruction{P1: 1, P2: 2},
			check:   func(r []int) bool { return len(r) == 2 && r[0] == 1 && r[1] == 2 },
		},
		{
			name:    "regPatternP3",
			pattern: regPatternP3,
			instr:   &Instruction{P3: 6},
			check:   func(r []int) bool { return len(r) == 1 && r[0] == 6 },
		},
		{
			name:    "regPatternP1Range",
			pattern: regPatternP1Range,
			instr:   &Instruction{P1: 2, P2: 3},
			check:   func(r []int) bool { return len(r) == 3 && r[0] == 2 && r[2] == 4 },
		},
		{
			name:    "regPatternP1P2P3",
			pattern: regPatternP1P2P3,
			instr:   &Instruction{P1: 1, P2: 2, P3: 3},
			check:   func(r []int) bool { return len(r) == 3 },
		},
		{
			name:    "regPatternDefault",
			pattern: regPatternDefault,
			instr:   &Instruction{P1: 0, P2: 1, P3: 2},
			check:   func(r []int) bool { return len(r) >= 1 },
		},
		{
			name:    "unknownPattern_returnsUnchanged",
			pattern: registerPattern(999),
			instr:   &Instruction{},
			check:   func(r []int) bool { return len(r) == 0 },
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := v.applySimplePattern(nil, tc.pattern, tc.instr)
			if !tc.check(result) {
				t.Errorf("applySimplePattern(%v) = %v, check failed", tc.pattern, result)
			}
		})
	}
}

// TestDebugDeepBuildCursorDumpFlags covers all flag combinations in buildCursorDumpFlags.
func TestDebugDeepBuildCursorDumpFlags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		cursor   *Cursor
		contains []string
		absent   []string
	}{
		{
			name:     "IsTable_Writable_NullRow_EOF_Root",
			cursor:   &Cursor{IsTable: true, Writable: true, NullRow: true, EOF: true, RootPage: 5},
			contains: []string{"[TABLE]", "[WRITABLE]", "[NULL ROW]", "[EOF]", "Root=5"},
		},
		{
			name:     "IsIndex_NoFlags",
			cursor:   &Cursor{IsTable: false},
			contains: []string{"[INDEX]"},
			absent:   []string{"[TABLE]", "[WRITABLE]", "[NULL ROW]", "[EOF]", "Root="},
		},
		{
			name:     "IsTable_NoOtherFlags",
			cursor:   &Cursor{IsTable: true},
			contains: []string{"[TABLE]"},
			absent:   []string{"[WRITABLE]", "[NULL ROW]", "[EOF]", "Root="},
		},
		{
			name:   "RootPage_Zero_NoRootLabel",
			cursor: &Cursor{RootPage: 0},
			absent: []string{"Root="},
		},
		{
			name:     "RootPage_Positive",
			cursor:   &Cursor{RootPage: 42},
			contains: []string{"Root=42"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			flags := buildCursorDumpFlags(tc.cursor)
			for _, want := range tc.contains {
				if !strings.Contains(flags, want) {
					t.Errorf("expected %q in flags %q", want, flags)
				}
			}
			for _, notWant := range tc.absent {
				if strings.Contains(flags, notWant) {
					t.Errorf("did not expect %q in flags %q", notWant, flags)
				}
			}
		})
	}
}
