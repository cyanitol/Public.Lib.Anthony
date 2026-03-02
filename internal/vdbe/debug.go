// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/observability"
)

// DebugMode controls VDBE debugging features.
type DebugMode int

const (
	// DebugOff disables all debugging.
	DebugOff DebugMode = 0
	// DebugTrace enables instruction tracing.
	DebugTrace DebugMode = 1 << 0
	// DebugRegisters enables register inspection after each instruction.
	DebugRegisters DebugMode = 1 << 1
	// DebugCursors enables cursor state inspection.
	DebugCursors DebugMode = 1 << 2
	// DebugStack enables stack trace on errors.
	DebugStack DebugMode = 1 << 3
	// DebugAll enables all debugging features.
	DebugAll DebugMode = DebugTrace | DebugRegisters | DebugCursors | DebugStack
)

// DebugContext holds the debugging state for a VDBE instance.
type DebugContext struct {
	Mode              DebugMode              // Debug mode flags
	TraceCallback     TraceCallback          // Custom trace callback
	BreakPoints       map[int]bool           // Map of PC addresses to break on
	StepMode          bool                   // Single-step mode
	InstructionLog    []string               // Log of executed instructions
	MaxLogSize        int                    // Maximum log size (0 = unlimited)
	RegisterWatches   map[int]bool           // Map of register indices to watch
	Logger            observability.Logger   // Observability logger for debug output
	LogLevel          observability.Level    // Log level for debug messages
	RegisterSnapshots map[int]map[int]string // PC -> Register -> Value snapshots (before execution)
}

// TraceCallback is called for each instruction execution when tracing is enabled.
// It receives the VDBE instance, program counter, and instruction.
// Return true to continue execution, false to break.
type TraceCallback func(v *VDBE, pc int, instr *Instruction) bool

// NewDebugContext creates a new debug context with the specified mode.
func NewDebugContext(mode DebugMode) *DebugContext {
	return &DebugContext{
		Mode:              mode,
		BreakPoints:       make(map[int]bool),
		InstructionLog:    make([]string, 0),
		MaxLogSize:        1000, // Default max log size
		RegisterWatches:   make(map[int]bool),
		Logger:            nil, // Will use default logger if nil
		LogLevel:          observability.DebugLevel,
		RegisterSnapshots: make(map[int]map[int]string),
	}
}

// SetDebugMode sets the debug mode for a VDBE instance.
func (v *VDBE) SetDebugMode(mode DebugMode) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(mode)
	} else {
		v.Debug.Mode = mode
	}
}

// GetDebugMode returns the current debug mode.
func (v *VDBE) GetDebugMode() DebugMode {
	if v.Debug == nil {
		return DebugOff
	}
	return v.Debug.Mode
}

// IsDebugEnabled checks if a specific debug flag is enabled.
func (v *VDBE) IsDebugEnabled(flag DebugMode) bool {
	if v.Debug == nil {
		return false
	}
	return (v.Debug.Mode & flag) != 0
}

// SetTraceCallback sets a custom trace callback.
func (v *VDBE) SetTraceCallback(callback TraceCallback) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.TraceCallback = callback
}

// AddBreakpoint adds a breakpoint at the specified program counter.
func (v *VDBE) AddBreakpoint(pc int) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.BreakPoints[pc] = true
}

// RemoveBreakpoint removes a breakpoint at the specified program counter.
func (v *VDBE) RemoveBreakpoint(pc int) {
	if v.Debug != nil {
		delete(v.Debug.BreakPoints, pc)
	}
}

// ClearBreakpoints removes all breakpoints.
func (v *VDBE) ClearBreakpoints() {
	if v.Debug != nil {
		v.Debug.BreakPoints = make(map[int]bool)
	}
}

// SetStepMode enables or disables single-step mode.
func (v *VDBE) SetStepMode(enabled bool) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.StepMode = enabled
}

// SetDebugLogger sets the logger for debug output.
// If logger is nil, the default observability logger will be used.
func (v *VDBE) SetDebugLogger(logger observability.Logger) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.Logger = logger
}

// SetDebugLogLevel sets the log level for debug messages.
func (v *VDBE) SetDebugLogLevel(level observability.Level) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.LogLevel = level
}

// WatchRegister adds a register to the watch list.
// Changes to watched registers will be logged in debug mode.
func (v *VDBE) WatchRegister(regIndex int) {
	if v.Debug == nil {
		v.Debug = NewDebugContext(DebugOff)
	}
	v.Debug.RegisterWatches[regIndex] = true
}

// UnwatchRegister removes a register from the watch list.
func (v *VDBE) UnwatchRegister(regIndex int) {
	if v.Debug != nil {
		delete(v.Debug.RegisterWatches, regIndex)
	}
}

// TraceInstruction logs the execution of an instruction.
// This is called internally during VDBE execution when tracing is enabled.
// It captures register/cursor state before execution and logs the instruction.
func (v *VDBE) TraceInstruction(pc int, instr *Instruction) bool {
	if v.Debug == nil {
		return true
	}

	// Capture register state before execution if register debugging is enabled
	if v.IsDebugEnabled(DebugRegisters) {
		v.captureRegisterSnapshot(pc)
	}

	// Check for breakpoint
	if v.Debug.BreakPoints[pc] {
		v.logInstruction(pc, instr, "BREAKPOINT")
		v.logToObservability(observability.WarnLevel, "BREAKPOINT at PC=%d: %s", pc, formatInstruction(pc, instr))
		return false // Break execution
	}

	// Check step mode
	if v.Debug.StepMode {
		v.logInstruction(pc, instr, "STEP")
		v.logToObservability(observability.DebugLevel, "STEP at PC=%d: %s", pc, formatInstruction(pc, instr))
		return false // Break after each instruction
	}

	// Log instruction if tracing enabled
	if v.IsDebugEnabled(DebugTrace) {
		v.logInstruction(pc, instr, "")
		v.logInstructionToObservability(pc, instr)
	}

	// Call custom trace callback if set
	if v.Debug.TraceCallback != nil {
		return v.Debug.TraceCallback(v, pc, instr)
	}

	return true // Continue execution
}

// TraceInstructionAfter logs register/cursor state changes after instruction execution.
// This should be called after an instruction has been executed.
func (v *VDBE) TraceInstructionAfter(pc int, instr *Instruction) {
	if v.Debug == nil {
		return
	}

	// Log affected registers if debugging enabled
	if v.IsDebugEnabled(DebugRegisters) {
		v.logAffectedRegisters(pc, instr)
	}

	// Log cursor state if debugging enabled
	if v.IsDebugEnabled(DebugCursors) {
		v.logAffectedCursors(pc, instr)
	}
}

// logInstruction adds an instruction to the debug log.
func (v *VDBE) logInstruction(pc int, instr *Instruction, tag string) {
	if v.Debug == nil {
		return
	}

	logEntry := formatInstruction(pc, instr)
	if tag != "" {
		logEntry = fmt.Sprintf("[%s] %s", tag, logEntry)
	}

	v.Debug.InstructionLog = append(v.Debug.InstructionLog, logEntry)

	// Trim log if it exceeds max size
	if v.Debug.MaxLogSize > 0 && len(v.Debug.InstructionLog) > v.Debug.MaxLogSize {
		// Keep the most recent entries
		v.Debug.InstructionLog = v.Debug.InstructionLog[len(v.Debug.InstructionLog)-v.Debug.MaxLogSize:]
	}
}

// formatInstruction formats an instruction for logging.
func formatInstruction(pc int, instr *Instruction) string {
	p4str := ""
	switch instr.P4Type {
	case P4Int32:
		p4str = fmt.Sprintf("%d", instr.P4.I)
	case P4Int64:
		p4str = fmt.Sprintf("%d", instr.P4.I64)
	case P4Real:
		p4str = fmt.Sprintf("%g", instr.P4.R)
	case P4Static, P4Dynamic:
		p4str = fmt.Sprintf("%q", instr.P4.Z)
	}

	comment := ""
	if instr.Comment != "" {
		comment = " ; " + instr.Comment
	}

	return fmt.Sprintf("%04d: %-12s P1=%-4d P2=%-4d P3=%-4d P4=%-12s P5=%-2d%s",
		pc, instr.Opcode.String(), instr.P1, instr.P2, instr.P3, p4str, instr.P5, comment)
}

// GetInstructionLog returns the instruction execution log.
func (v *VDBE) GetInstructionLog() []string {
	if v.Debug == nil {
		return []string{}
	}
	// Return a copy
	log := make([]string, len(v.Debug.InstructionLog))
	copy(log, v.Debug.InstructionLog)
	return log
}

// ClearInstructionLog clears the instruction execution log.
func (v *VDBE) ClearInstructionLog() {
	if v.Debug != nil {
		v.Debug.InstructionLog = make([]string, 0)
	}
}

// DumpRegisters returns a formatted dump of all memory registers.
func (v *VDBE) DumpRegisters() string {
	var sb strings.Builder
	sb.WriteString("Register Dump:\n")
	sb.WriteString("─────────────────────────────────────────\n")

	for i, mem := range v.Mem {
		watched := ""
		if v.Debug != nil && v.Debug.RegisterWatches[i] {
			watched = " [WATCHED]"
		}
		sb.WriteString(fmt.Sprintf("R%-4d: %s%s\n", i, mem.String(), watched))
	}

	return sb.String()
}

// DumpRegister returns a formatted dump of a specific register.
func (v *VDBE) DumpRegister(index int) string {
	if index < 0 || index >= len(v.Mem) {
		return fmt.Sprintf("Register R%d: OUT OF RANGE", index)
	}

	mem := v.Mem[index]
	watched := ""
	if v.Debug != nil && v.Debug.RegisterWatches[index] {
		watched = " [WATCHED]"
	}

	return fmt.Sprintf("Register R%d: %s%s", index, mem.String(), watched)
}

// DumpCursors returns a formatted dump of all cursors.
func (v *VDBE) DumpCursors() string {
	var sb strings.Builder
	sb.WriteString("Cursor Dump:\n")
	sb.WriteString("─────────────────────────────────────────\n")

	for i, cursor := range v.Cursors {
		if cursor == nil {
			sb.WriteString(fmt.Sprintf("C%-4d: <CLOSED>\n", i))
			continue
		}

		curType := "UNKNOWN"
		switch cursor.CurType {
		case CursorBTree:
			curType = "BTREE"
		case CursorSorter:
			curType = "SORTER"
		case CursorVTab:
			curType = "VTAB"
		case CursorPseudo:
			curType = "PSEUDO"
		}

		sb.WriteString(fmt.Sprintf("C%-4d: Type=%s", i, curType))

		if cursor.IsTable {
			sb.WriteString(" [TABLE]")
		} else {
			sb.WriteString(" [INDEX]")
		}

		if cursor.Writable {
			sb.WriteString(" [WRITABLE]")
		}

		if cursor.NullRow {
			sb.WriteString(" [NULL ROW]")
		}

		if cursor.EOF {
			sb.WriteString(" [EOF]")
		}

		if cursor.RootPage > 0 {
			sb.WriteString(fmt.Sprintf(" Root=%d", cursor.RootPage))
		}

		sb.WriteString("\n")
	}

	return sb.String()
}

// DumpProgram returns a formatted dump of the entire program.
func (v *VDBE) DumpProgram() string {
	if len(v.Program) == 0 {
		return "Empty program"
	}

	var sb strings.Builder
	sb.WriteString("Program Dump:\n")
	sb.WriteString("═════════════════════════════════════════════════════════════════════════════\n")
	sb.WriteString("Addr  Opcode         P1    P2    P3    P4             P5  Comment\n")
	sb.WriteString("────  ─────────────  ────  ────  ────  ─────────────  ──  ───────────────────\n")

	for i, instr := range v.Program {
		// Mark current PC
		marker := "    "
		if i == v.PC {
			marker = ">>> "
		}

		// Mark breakpoints
		if v.Debug != nil && v.Debug.BreakPoints[i] {
			marker = "BP> "
		}

		sb.WriteString(marker)
		sb.WriteString(formatInstruction(i, instr))
		sb.WriteString("\n")
	}

	sb.WriteString("═════════════════════════════════════════════════════════════════════════════\n")
	return sb.String()
}

// DumpState returns a complete dump of VDBE state for debugging.
func (v *VDBE) DumpState() string {
	var sb strings.Builder

	sb.WriteString("═════════════════════════════════════════════════════════════════════════════\n")
	sb.WriteString("VDBE STATE DUMP\n")
	sb.WriteString("═════════════════════════════════════════════════════════════════════════════\n\n")

	// Basic state
	sb.WriteString(fmt.Sprintf("State: %v\n", v.State))
	sb.WriteString(fmt.Sprintf("PC: %d / %d\n", v.PC, len(v.Program)))
	sb.WriteString(fmt.Sprintf("Num Steps: %d\n", v.NumSteps))
	sb.WriteString(fmt.Sprintf("In Transaction: %v\n", v.InTxn))
	sb.WriteString(fmt.Sprintf("Read Only: %v\n", v.ReadOnly))
	sb.WriteString(fmt.Sprintf("Explain Mode: %v\n", v.Explain))

	if v.ErrorMsg != "" {
		sb.WriteString(fmt.Sprintf("Error: %s\n", v.ErrorMsg))
	}

	sb.WriteString("\n")

	// Statistics
	if v.Stats != nil {
		sb.WriteString("Statistics:\n")
		sb.WriteString(fmt.Sprintf("  Instructions: %d\n", v.Stats.NumInstructions))
		sb.WriteString(fmt.Sprintf("  Rows Read: %d\n", v.Stats.RowsRead))
		sb.WriteString(fmt.Sprintf("  Rows Written: %d\n", v.Stats.RowsWritten))
		sb.WriteString(fmt.Sprintf("  Page Reads: %d\n", v.Stats.PageReads))
		sb.WriteString(fmt.Sprintf("  Page Writes: %d\n", v.Stats.PageWrites))
		sb.WriteString("\n")
	}

	// Registers
	if v.IsDebugEnabled(DebugRegisters) {
		sb.WriteString(v.DumpRegisters())
		sb.WriteString("\n")
	}

	// Cursors
	if v.IsDebugEnabled(DebugCursors) {
		sb.WriteString(v.DumpCursors())
		sb.WriteString("\n")
	}

	// Recent instruction log
	if v.Debug != nil && len(v.Debug.InstructionLog) > 0 {
		sb.WriteString("Recent Instructions:\n")
		sb.WriteString("─────────────────────────────────────────\n")
		// Show last 10 instructions
		start := 0
		if len(v.Debug.InstructionLog) > 10 {
			start = len(v.Debug.InstructionLog) - 10
		}
		for i := start; i < len(v.Debug.InstructionLog); i++ {
			sb.WriteString(v.Debug.InstructionLog[i])
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	sb.WriteString("═════════════════════════════════════════════════════════════════════════════\n")

	return sb.String()
}

// captureRegisterSnapshot captures the current state of all registers before execution.
func (v *VDBE) captureRegisterSnapshot(pc int) {
	if v.Debug == nil {
		return
	}

	snapshot := make(map[int]string)
	for i, mem := range v.Mem {
		snapshot[i] = mem.String()
	}
	v.Debug.RegisterSnapshots[pc] = snapshot
}

// logAffectedRegisters logs changes to registers caused by instruction execution.
func (v *VDBE) logAffectedRegisters(pc int, instr *Instruction) {
	if v.Debug == nil {
		return
	}

	// Get the before snapshot
	snapshot, hasSnapshot := v.Debug.RegisterSnapshots[pc]
	if !hasSnapshot {
		return
	}

	// Determine which registers might be affected based on opcode
	affectedRegs := v.getAffectedRegisters(instr)

	// Log changes
	for _, regIdx := range affectedRegs {
		if regIdx < 0 || regIdx >= len(v.Mem) {
			continue
		}

		oldVal := snapshot[regIdx]
		newVal := v.Mem[regIdx].String()

		if oldVal != newVal {
			msg := fmt.Sprintf("R%d: %s -> %s", regIdx, oldVal, newVal)
			v.logToObservability(v.Debug.LogLevel, "  [REG CHANGE] %s", msg)
		}
	}

	// Clean up old snapshots to prevent memory leaks
	if len(v.Debug.RegisterSnapshots) > 100 {
		// Keep only the last 100 snapshots
		for oldPC := range v.Debug.RegisterSnapshots {
			if oldPC < pc-100 {
				delete(v.Debug.RegisterSnapshots, oldPC)
			}
		}
	}
}

// logAffectedCursors logs cursor state changes.
func (v *VDBE) logAffectedCursors(pc int, instr *Instruction) {
	if v.Debug == nil {
		return
	}

	// Determine which cursors might be affected based on opcode
	affectedCursors := v.getAffectedCursors(instr)

	for _, curIdx := range affectedCursors {
		if curIdx < 0 || curIdx >= len(v.Cursors) {
			continue
		}

		cursor := v.Cursors[curIdx]
		if cursor == nil {
			v.logToObservability(v.Debug.LogLevel, "  [CURSOR] C%d: <CLOSED>", curIdx)
			continue
		}

		curType := "UNKNOWN"
		switch cursor.CurType {
		case CursorBTree:
			curType = "BTREE"
		case CursorSorter:
			curType = "SORTER"
		case CursorVTab:
			curType = "VTAB"
		case CursorPseudo:
			curType = "PSEUDO"
		}

		flags := ""
		if cursor.EOF {
			flags += " EOF"
		}
		if cursor.NullRow {
			flags += " NULL"
		}

		v.logToObservability(v.Debug.LogLevel, "  [CURSOR] C%d: Type=%s%s", curIdx, curType, flags)
	}
}

// getAffectedRegisters returns the list of register indices potentially affected by an instruction.
func (v *VDBE) getAffectedRegisters(instr *Instruction) []int {
	affected := make([]int, 0, 3)

	// Most instructions affect P1, P2, or P3 as register destinations
	switch instr.Opcode {
	case OpInteger, OpInt64, OpReal, OpString, OpString8, OpBlob, OpNull:
		// These write to P2
		affected = append(affected, instr.P2)
	case OpCopy, OpSCopy, OpMove:
		// These write to P2 and read from P1
		affected = append(affected, instr.P1, instr.P2)
	case OpColumn, OpRowData:
		// These write to P3
		affected = append(affected, instr.P3)
	case OpMakeRecord:
		// Writes to P3, reads from P1..P1+P2
		affected = append(affected, instr.P3)
		for i := instr.P1; i < instr.P1+instr.P2; i++ {
			affected = append(affected, i)
		}
	case OpResultRow:
		// Reads from P1..P1+P2
		for i := instr.P1; i < instr.P1+instr.P2; i++ {
			affected = append(affected, i)
		}
	case OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder:
		// Binary operations: P1 op P2 -> P3
		affected = append(affected, instr.P1, instr.P2, instr.P3)
	case OpConcat:
		// P1..P1+P2 -> P3
		affected = append(affected, instr.P3)
		for i := instr.P1; i < instr.P1+instr.P2; i++ {
			affected = append(affected, i)
		}
	default:
		// For unknown opcodes, check if any operand looks like a register index
		if instr.P1 >= 0 && instr.P1 < len(v.Mem) {
			affected = append(affected, instr.P1)
		}
		if instr.P2 >= 0 && instr.P2 < len(v.Mem) {
			affected = append(affected, instr.P2)
		}
		if instr.P3 >= 0 && instr.P3 < len(v.Mem) {
			affected = append(affected, instr.P3)
		}
	}

	return affected
}

// getAffectedCursors returns the list of cursor indices potentially affected by an instruction.
func (v *VDBE) getAffectedCursors(instr *Instruction) []int {
	affected := make([]int, 0, 1)

	switch instr.Opcode {
	case OpOpenRead, OpOpenWrite, OpOpenEphemeral, OpOpenPseudo:
		// Opens cursor at P1
		affected = append(affected, instr.P1)
	case OpClose:
		// Closes cursor at P1
		affected = append(affected, instr.P1)
	case OpRewind, OpLast, OpNext, OpPrev, OpSeek, OpSeekGE, OpSeekGT, OpSeekLE, OpSeekLT, OpSeekRowid:
		// Cursor operations on P1
		affected = append(affected, instr.P1)
	case OpColumn, OpRowData, OpRowid:
		// Read from cursor P1
		affected = append(affected, instr.P1)
	case OpInsert, OpDelete, OpIdxInsert, OpIdxDelete:
		// Write operations on cursor P1
		affected = append(affected, instr.P1)
	}

	return affected
}

// logInstructionToObservability logs instruction execution to the observability logger.
func (v *VDBE) logInstructionToObservability(pc int, instr *Instruction) {
	if v.Debug == nil {
		return
	}

	instrStr := formatInstruction(pc, instr)

	// Use custom logger if set, otherwise skip (default global logger not exposed)
	if v.Debug.Logger != nil {
		v.Debug.Logger.Debug(instrStr, observability.Fields{"pc": pc, "opcode": instr.Opcode.String()})
	}
}

// logToObservability logs a message to the observability logger.
func (v *VDBE) logToObservability(level observability.Level, format string, args ...interface{}) {
	if v.Debug == nil {
		return
	}

	msg := fmt.Sprintf(format, args...)

	// Use custom logger if set
	if v.Debug.Logger != nil {
		switch level {
		case observability.TraceLevel:
			v.Debug.Logger.Trace(msg, observability.Fields{})
		case observability.DebugLevel:
			v.Debug.Logger.Debug(msg, observability.Fields{})
		case observability.InfoLevel:
			v.Debug.Logger.Info(msg, observability.Fields{})
		case observability.WarnLevel:
			v.Debug.Logger.Warn(msg, observability.Fields{})
		case observability.ErrorLevel:
			v.Debug.Logger.Error(msg, observability.Fields{})
		}
	}
}
