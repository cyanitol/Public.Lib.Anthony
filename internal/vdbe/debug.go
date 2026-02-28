package vdbe

import (
	"fmt"
	"strings"
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
	Mode            DebugMode     // Debug mode flags
	TraceCallback   TraceCallback // Custom trace callback
	BreakPoints     map[int]bool  // Map of PC addresses to break on
	StepMode        bool          // Single-step mode
	InstructionLog  []string      // Log of executed instructions
	MaxLogSize      int           // Maximum log size (0 = unlimited)
	RegisterWatches map[int]bool  // Map of register indices to watch
}

// TraceCallback is called for each instruction execution when tracing is enabled.
// It receives the VDBE instance, program counter, and instruction.
// Return true to continue execution, false to break.
type TraceCallback func(v *VDBE, pc int, instr *Instruction) bool

// NewDebugContext creates a new debug context with the specified mode.
func NewDebugContext(mode DebugMode) *DebugContext {
	return &DebugContext{
		Mode:            mode,
		BreakPoints:     make(map[int]bool),
		InstructionLog:  make([]string, 0),
		MaxLogSize:      1000, // Default max log size
		RegisterWatches: make(map[int]bool),
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
func (v *VDBE) TraceInstruction(pc int, instr *Instruction) bool {
	if v.Debug == nil {
		return true
	}

	// Check for breakpoint
	if v.Debug.BreakPoints[pc] {
		v.logInstruction(pc, instr, "BREAKPOINT")
		return false // Break execution
	}

	// Check step mode
	if v.Debug.StepMode {
		v.logInstruction(pc, instr, "STEP")
		return false // Break after each instruction
	}

	// Log instruction if tracing enabled
	if v.IsDebugEnabled(DebugTrace) {
		v.logInstruction(pc, instr, "")
	}

	// Call custom trace callback if set
	if v.Debug.TraceCallback != nil {
		return v.Debug.TraceCallback(v, pc, instr)
	}

	return true // Continue execution
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

// Debug field to VDBE struct (this should be added to vdbe.go VDBE struct)
// Debug *DebugContext // Debug context for tracing and inspection
