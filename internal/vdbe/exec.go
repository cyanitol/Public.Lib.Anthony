// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/observability"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/types"
)

// Step executes the VDBE program until a result row is ready or the program halts.
// Returns true if a row is ready, false if halted.
// This mirrors SQLite's sqlite3_step() behavior.
func (v *VDBE) Step() (bool, error) {
	if err := v.prepareForStep(); err != nil {
		return false, err
	}
	return v.runUntilRowOrHalt()
}

// prepareForStep transitions the VDBE to running state.
func (v *VDBE) prepareForStep() error {
	switch v.State {
	case StateHalt:
		return fmt.Errorf("VDBE is halted")
	case StateInit:
		v.State = StateReady
		fallthrough
	case StateReady:
		v.PC = 0
		v.State = StateRun
		// Start statistics tracking if enabled
		if v.Stats != nil && v.Stats.StartTime == 0 {
			v.Stats.Start()
		}
	case StateRowReady:
		v.ResultRow = nil
		v.State = StateRun
	}
	return nil
}

// runUntilRowOrHalt executes instructions until a row is ready or the program halts.
func (v *VDBE) runUntilRowOrHalt() (bool, error) {
	for {
		// Check if program has ended
		if hasEnded, result := v.handleProgramEnd(); hasEnded {
			return result, nil
		}

		// Fetch next instruction and update PC
		instr, currentPC := v.fetchNextInstruction()

		// Record instruction stats
		v.updateExecutionStats(instr)

		// Handle debug breakpoint before execution
		if shouldPause := v.handleDebugBreakpoint(currentPC, instr); shouldPause {
			return false, nil
		}

		// Execute instruction and handle error
		if hasError, err := v.handleExecutionError(instr, currentPC); hasError {
			return false, err
		}

		// Trace after execution
		v.traceAfterExecution(currentPC, instr)

		// Check state transitions
		if isReady := v.checkStateTransition(); isReady != nil {
			return *isReady, nil
		}
	}
}

// handleProgramEnd checks if PC exceeds program length and halts if needed.
func (v *VDBE) handleProgramEnd() (bool, bool) {
	if v.PC >= len(v.Program) {
		v.State = StateHalt
		v.endStatsIfNeeded()
		return true, false
	}
	return false, false
}

// fetchNextInstruction gets the next instruction and increments PC.
func (v *VDBE) fetchNextInstruction() (*Instruction, int) {
	instr := v.Program[v.PC]
	currentPC := v.PC
	v.PC++
	v.NumSteps++
	return instr, currentPC
}

// updateExecutionStats records instruction execution if stats are enabled.
func (v *VDBE) updateExecutionStats(instr *Instruction) {
	if v.Stats != nil {
		v.Stats.RecordInstruction(instr.Opcode)
	}
}

// handleDebugBreakpoint checks for debug breakpoints and pauses if needed.
func (v *VDBE) handleDebugBreakpoint(currentPC int, instr *Instruction) bool {
	if v.Debug == nil {
		return false
	}
	shouldContinue := v.TraceInstruction(currentPC, instr)
	if !shouldContinue {
		// Breakpoint or step mode - pause execution
		v.PC = currentPC // Restore PC so next step re-executes this instruction
		v.State = StateHalt
		return true
	}
	return false
}

// handleExecutionError executes instruction and handles any errors.
func (v *VDBE) handleExecutionError(instr *Instruction, currentPC int) (bool, error) {
	err := v.execInstruction(instr)
	if err == nil {
		return false, nil
	}

	v.SetError(err.Error())
	v.State = StateHalt
	v.endStatsIfNeeded()

	// Log error in debug mode
	if v.Debug != nil && v.IsDebugEnabled(DebugStack) {
		v.logToObservability(observability.ErrorLevel, "Error at PC=%d: %s", currentPC, err.Error())
	}

	return true, err
}

// traceAfterExecution traces instruction after execution if debug enabled.
func (v *VDBE) traceAfterExecution(currentPC int, instr *Instruction) {
	if v.Debug != nil {
		v.TraceInstructionAfter(currentPC, instr)
	}
}

// checkStateTransition checks if state indicates halt or row ready.
func (v *VDBE) checkStateTransition() *bool {
	if v.State == StateHalt {
		v.endStatsIfNeeded()
		result := false
		return &result
	}
	if v.State == StateRowReady {
		result := true
		return &result
	}
	return nil
}

// endStatsIfNeeded ends statistics tracking if enabled and not already ended.
func (v *VDBE) endStatsIfNeeded() {
	if v.Stats != nil && v.Stats.EndTime == 0 {
		v.Stats.End()
	}
}

// Run executes the entire VDBE program until completion.
func (v *VDBE) Run() error {
	for {
		hasMore, err := v.Step()
		if err != nil {
			return err
		}
		if !hasMore {
			break
		}
	}
	return nil
}

// opcodeHandler is a function that executes a specific opcode.
type opcodeHandler func(v *VDBE, instr *Instruction) error

// opcodeDispatch maps opcodes to their handler functions.
// Using a dispatch table reduces cyclomatic complexity vs switch statement.
// Initialized in init() to avoid initialization cycle.
var opcodeDispatch map[Opcode]opcodeHandler

func init() {
	opcodeDispatch = map[Opcode]opcodeHandler{
		// Control flow opcodes
		OpInit:   (*VDBE).execInit,
		OpGoto:   (*VDBE).execGoto,
		OpGosub:  (*VDBE).execGosub,
		OpReturn: (*VDBE).execReturn,
		OpHalt:   (*VDBE).execHalt,
		OpIf:     (*VDBE).execIf,
		OpIfNot:  (*VDBE).execIfNot,
		OpIfPos:  (*VDBE).execIfPos,

		// Register operations
		OpInteger: (*VDBE).execInteger,
		OpInt64:   (*VDBE).execInt64,
		OpReal:    (*VDBE).execReal,
		OpString:  (*VDBE).execString,
		OpString8: (*VDBE).execString,
		OpBlob:    (*VDBE).execBlob,
		OpNull:    (*VDBE).execNull,
		OpCopy:    (*VDBE).execCopy,
		OpMove:    (*VDBE).execMove,
		OpSCopy:   (*VDBE).execSCopy,

		// Cursor operations
		OpOpenRead:      (*VDBE).execOpenRead,
		OpOpenWrite:     (*VDBE).execOpenWrite,
		OpOpenEphemeral: (*VDBE).execOpenEphemeral,
		OpClose:         (*VDBE).execClose,
		OpRewind:        (*VDBE).execRewind,
		OpNext:          (*VDBE).execNext,
		OpPrev:          (*VDBE).execPrev,
		OpSeekGE:        (*VDBE).execSeekGE,
		OpSeekGT:        (*VDBE).execSeekGT,
		OpSeekLE:        (*VDBE).execSeekLE,
		OpSeekLT:        (*VDBE).execSeekLT,
		OpSeekRowid:     (*VDBE).execSeekRowid,
		OpNotExists:     (*VDBE).execNotExists,
		OpDeferredSeek:  (*VDBE).execDeferredSeek,

		// Data retrieval
		OpColumn:    (*VDBE).execColumn,
		OpRowid:     (*VDBE).execRowid,
		OpResultRow: (*VDBE).execResultRow,

		// Data modification
		OpNewRowid:   (*VDBE).execNewRowid,
		OpMakeRecord: (*VDBE).execMakeRecord,
		OpInsert:     (*VDBE).execInsert,
		OpDelete:     (*VDBE).execDelete,

		// Comparison
		OpEq:      (*VDBE).execEq,
		OpNe:      (*VDBE).execNe,
		OpLt:      (*VDBE).execLt,
		OpLe:      (*VDBE).execLe,
		OpGt:      (*VDBE).execGt,
		OpGe:      (*VDBE).execGe,
		OpIsNull:  (*VDBE).execIsNull,
		OpNotNull: (*VDBE).execNotNull,

		// Arithmetic
		OpAdd:       (*VDBE).execAdd,
		OpSubtract:  (*VDBE).execSubtract,
		OpMultiply:  (*VDBE).execMultiply,
		OpDivide:    (*VDBE).execDivide,
		OpRemainder: (*VDBE).execRemainder,
		OpAddImm:    (*VDBE).execAddImm,

		// String operations
		OpConcat: (*VDBE).execConcat,

		// Bitwise operations
		OpBitAnd:     (*VDBE).execBitAnd,
		OpBitOr:      (*VDBE).execBitOr,
		OpBitNot:     (*VDBE).execBitNot,
		OpShiftLeft:  (*VDBE).execShiftLeft,
		OpShiftRight: (*VDBE).execShiftRight,

		// Logical operations
		OpAnd: (*VDBE).execAnd,
		OpOr:  (*VDBE).execOr,
		OpNot: (*VDBE).execNot,

		// Aggregate functions
		OpAggStep:  (*VDBE).execAggStep,
		OpAggFinal: (*VDBE).execAggFinal,

		// Function calls
		OpFunction: (*VDBE).execFunction,

		// Transaction operations
		OpTransaction:  (*VDBE).execTransaction,
		OpCommit:       (*VDBE).execCommit,
		OpRollback:     (*VDBE).execRollback,
		OpAutocommit:   (*VDBE).execAutoCommit,
		OpSavepoint:    (*VDBE).execSavepoint,
		OpVerifyCookie: (*VDBE).execVerifyCookie,
		OpSetCookie:    (*VDBE).execSetCookie,

		// Sorter operations
		OpSorterOpen:   (*VDBE).execSorterOpen,
		OpSorterInsert: (*VDBE).execSorterInsert,
		OpSorterSort:   (*VDBE).execSorterSort,
		OpSorterNext:   (*VDBE).execSorterNext,
		OpSorterData:   (*VDBE).execSorterData,
		OpSorterClose:  (*VDBE).execSorterClose,

		// No operation
		OpNoop: (*VDBE).execNoop,

		// Type conversion operations
		OpCast:      (*VDBE).execCast,
		OpToText:    (*VDBE).execToText,
		OpToBlob:    (*VDBE).execToBlob,
		OpToNumeric: (*VDBE).execToNumeric,
		OpToInt:     (*VDBE).execToInt,
		OpToReal:    (*VDBE).execToReal,

		// Index operations
		OpIdxInsert: (*VDBE).execIdxInsert,
		OpIdxDelete: (*VDBE).execIdxDelete,
		OpIdxRowid:  (*VDBE).execIdxRowid,
		OpIdxLT:     (*VDBE).execIdxLT,
		OpIdxGT:     (*VDBE).execIdxGT,
		OpIdxLE:     (*VDBE).execIdxLE,
		OpIdxGE:     (*VDBE).execIdxGE,

		// Trigger and sub-program operations
		OpProgram:       (*VDBE).execProgram,
		OpParam:         (*VDBE).execParam,
		OpInitCoroutine: (*VDBE).execInitCoroutine,
		OpEndCoroutine:  (*VDBE).execEndCoroutine,
		OpYield:         (*VDBE).execYield,
		OpOnce:          (*VDBE).execOnce,
		OpOpenPseudo:    (*VDBE).execOpenPseudo,

		// Window function operations
		OpAggStepWindow:    (*VDBE).execAggStepWindow,
		OpWindowRowNum:     (*VDBE).execWindowRowNum,
		OpWindowRank:       (*VDBE).execWindowRank,
		OpWindowDenseRank:  (*VDBE).execWindowDenseRank,
		OpWindowNtile:      (*VDBE).execWindowNtile,
		OpWindowLag:        (*VDBE).execWindowLag,
		OpWindowLead:       (*VDBE).execWindowLead,
		OpWindowFirstValue: (*VDBE).execWindowFirstValue,
		OpWindowLastValue:  (*VDBE).execWindowLastValue,
		OpAggDistinct:      (*VDBE).execAggDistinct,
		OpDistinctRow:      (*VDBE).execDistinctRow,
	}
}

// execNoop handles the OpNoop instruction (no operation).
func (v *VDBE) execNoop(_ *Instruction) error {
	return nil
}

// execInstruction executes a single instruction using the dispatch table.
func (v *VDBE) execInstruction(instr *Instruction) error {
	if handler, ok := opcodeDispatch[instr.Opcode]; ok {
		return handler(v, instr)
	}
	return fmt.Errorf("unimplemented opcode: %s", instr.Opcode.String())
}

// Control flow instruction implementations

func (v *VDBE) execInit(instr *Instruction) error {
	// Initialize the program - typically jumps to P2
	if instr.P2 > 0 {
		v.PC = instr.P2
	}
	return nil
}

func (v *VDBE) execGoto(instr *Instruction) error {
	// Unconditional jump to P2
	if instr.P2 < 0 || instr.P2 >= len(v.Program) {
		return fmt.Errorf("invalid jump address: %d", instr.P2)
	}
	v.PC = instr.P2
	return nil
}

func (v *VDBE) execGosub(instr *Instruction) error {
	// Save return address in P1, jump to P2
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}
	mem.SetInt(int64(v.PC))
	v.PC = instr.P2
	return nil
}

func (v *VDBE) execReturn(instr *Instruction) error {
	// Jump to address stored in P1
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}
	if mem.IsInt() {
		v.PC = int(mem.IntValue())
	}
	return nil
}

func (v *VDBE) execHalt(instr *Instruction) error {
	// Halt execution
	v.State = StateHalt
	v.RC = instr.P1
	if instr.P4Type == P4Static || instr.P4Type == P4Dynamic {
		v.SetError(instr.P4.Z)
	}
	return nil
}

func (v *VDBE) execIf(instr *Instruction) error {
	// Jump to P2 if P1 is true
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	isTrue := false
	if !mem.IsNull() {
		if mem.IsInt() {
			isTrue = mem.IntValue() != 0
		} else {
			isTrue = mem.RealValue() != 0.0
		}
	}

	if isTrue {
		v.PC = instr.P2
	}
	return nil
}

func (v *VDBE) execIfNot(instr *Instruction) error {
	// Jump to P2 if P1 is false
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	isFalse := mem.IsNull()
	if !isFalse {
		if mem.IsInt() {
			isFalse = mem.IntValue() == 0
		} else {
			isFalse = mem.RealValue() == 0.0
		}
	}

	if isFalse {
		v.PC = instr.P2
	}
	return nil
}

func (v *VDBE) execIfPos(instr *Instruction) error {
	// Jump to P2 if register P1 is positive, and decrement P1 by P3
	// P1 = register to check and decrement
	// P2 = jump address if positive
	// P3 = amount to decrement (typically -1)
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// Get current value as integer
	var val int64
	if mem.IsInt() {
		val = mem.IntValue()
	} else if !mem.IsNull() {
		mem.Integerify()
		val = mem.IntValue()
	}

	// Check if positive
	if val > 0 {
		// Decrement by P3 (usually -1, so this adds)
		newVal := val + int64(instr.P3)
		mem.SetInt(newVal)
		// Jump to P2
		v.PC = instr.P2
	}

	return nil
}

// Register operation implementations

func (v *VDBE) execInteger(instr *Instruction) error {
	// Store integer P1 in register P2
	mem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	mem.SetInt(int64(instr.P1))
	return nil
}

func (v *VDBE) execInt64(instr *Instruction) error {
	// Store 64-bit integer P4 in register P2
	mem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	if instr.P4Type != P4Int64 {
		return fmt.Errorf("expected P4_INT64 for Int64 opcode")
	}
	mem.SetInt(instr.P4.I64)
	return nil
}

func (v *VDBE) execReal(instr *Instruction) error {
	// Store real P4 in register P2
	mem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	if instr.P4Type != P4Real {
		return fmt.Errorf("expected P4_REAL for Real opcode")
	}
	mem.SetReal(instr.P4.R)
	return nil
}

func (v *VDBE) execString(instr *Instruction) error {
	// Store string P4 in register P2
	mem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	if instr.P4Type != P4Static && instr.P4Type != P4Dynamic {
		return fmt.Errorf("expected P4_STATIC or P4_DYNAMIC for String opcode")
	}
	mem.SetStr(instr.P4.Z)
	return nil
}

func (v *VDBE) execBlob(instr *Instruction) error {
	// Store blob P4 in register P2
	mem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	// P4 contains the blob data
	if instr.P4.P != nil {
		if blob, ok := instr.P4.P.([]byte); ok {
			mem.SetBlob(blob)
		} else {
			return fmt.Errorf("P4 is not a byte slice for Blob opcode")
		}
	}
	return nil
}

func (v *VDBE) execNull(instr *Instruction) error {
	// Store NULL in registers P2 through P2+P3
	for i := instr.P2; i <= instr.P2+instr.P3; i++ {
		mem, err := v.GetMem(i)
		if err != nil {
			return err
		}
		mem.SetNull()
	}
	return nil
}

func (v *VDBE) execCopy(instr *Instruction) error {
	// Copy P3+1 registers starting at P1 to starting at P2
	// SQLite OP_Copy: Make a copy of registers P1..P1+P3 into registers P2..P2+P3
	// This makes a deep copy of the value (string/blob data is duplicated)
	count := instr.P3 + 1

	for i := 0; i < count; i++ {
		src, err := v.GetMem(instr.P1 + i)
		if err != nil {
			return err
		}
		dst, err := v.GetMem(instr.P2 + i)
		if err != nil {
			return err
		}
		if err := dst.Copy(src); err != nil {
			return err
		}
	}
	return nil
}

func (v *VDBE) execMove(instr *Instruction) error {
	// Move P3 registers starting at P1 to starting at P2
	count := instr.P3
	if count <= 0 {
		count = 1
	}

	for i := 0; i < count; i++ {
		src, err := v.GetMem(instr.P1 + i)
		if err != nil {
			return err
		}
		dst, err := v.GetMem(instr.P2 + i)
		if err != nil {
			return err
		}
		dst.Move(src)
	}
	return nil
}

func (v *VDBE) execSCopy(instr *Instruction) error {
	// Shallow copy register P1 to register P2
	src, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}
	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	dst.ShallowCopy(src)
	return nil
}

// Cursor operation implementations

// findTableByRootPage finds a table by its root page number from the schema.
// Returns nil if schema is not available or table not found.
func (v *VDBE) findTableByRootPage(rootPage uint32) interface{} {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}

	// Use interface to avoid import cycle with schema package
	type schemaWithRootPageLookup interface {
		GetTableByRootPage(uint32) (interface{}, bool)
	}

	schemaObj, ok := v.Ctx.Schema.(schemaWithRootPageLookup)
	if !ok {
		return nil
	}

	if table, found := schemaObj.GetTableByRootPage(rootPage); found {
		return table
	}

	return nil
}

func (v *VDBE) execOpenRead(instr *Instruction) error {
	// Open cursor P1 for reading on root page P2
	// P1 = cursor number, P2 = root page, P3 = num columns
	if v.Ctx == nil || v.Ctx.Btree == nil {
		return fmt.Errorf("no btree context available")
	}

	bt, ok := v.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree context type")
	}

	// Allocate cursors if needed
	if err := v.AllocCursors(instr.P1 + 1); err != nil {
		return err
	}

	// Create btree cursor
	btCursor := btree.NewCursor(bt, uint32(instr.P2))

	// Create VDBE cursor
	cursor := &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		RootPage:    uint32(instr.P2),
		BtreeCursor: btCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	// Store table metadata if available (for handling ALTER TABLE ADD COLUMN defaults)
	cursor.Table = v.findTableByRootPage(uint32(instr.P2))

	v.Cursors[instr.P1] = cursor
	return nil
}

func (v *VDBE) execOpenWrite(instr *Instruction) error {
	// Open cursor P1 for writing on root page P2
	// P1 = cursor number, P2 = root page, P3 = num columns
	if v.Ctx == nil || v.Ctx.Btree == nil {
		return fmt.Errorf("no btree context available")
	}

	bt, ok := v.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree context type")
	}

	// Allocate cursors if needed
	if err := v.AllocCursors(instr.P1 + 1); err != nil {
		return err
	}

	// Create btree cursor (same as read cursor - btree cursors support both read and write)
	btCursor := btree.NewCursor(bt, uint32(instr.P2))

	// Create VDBE cursor with writable flag set
	cursor := &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		Writable:    true, // Mark as writable for write operations
		RootPage:    uint32(instr.P2),
		BtreeCursor: btCursor,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	// Store table metadata if available (for handling ALTER TABLE ADD COLUMN defaults)
	cursor.Table = v.findTableByRootPage(uint32(instr.P2))

	v.Cursors[instr.P1] = cursor
	return nil
}

func (v *VDBE) execClose(instr *Instruction) error {
	// Close cursor P1
	if instr.P1 < 0 || instr.P1 >= len(v.Cursors) {
		return fmt.Errorf("cursor index %d out of range", instr.P1)
	}

	cursor := v.Cursors[instr.P1]
	if cursor != nil {
		// Clear any btree cursor
		cursor.BtreeCursor = nil
		cursor.CurrentKey = nil
		cursor.CurrentVal = nil
		cursor.CachedCols = nil
	}

	v.Cursors[instr.P1] = nil
	return nil
}

func (v *VDBE) execRewind(instr *Instruction) error {
	// Rewind cursor P1 to the beginning, jump to P2 if empty
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Reset state
	cursor.EOF = false
	cursor.NullRow = false
	cursor.CurrentKey = nil
	cursor.CurrentVal = nil
	v.IncrCacheCtr() // Invalidate column cache

	// Get btree cursor
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		// Empty table or invalid cursor - jump to P2
		if instr.P2 > 0 {
			v.PC = instr.P2
		}
		cursor.EOF = true
		return nil
	}

	// Move to first entry
	err = btCursor.MoveToFirst()
	if err != nil {
		// Empty table or error - jump to P2
		if instr.P2 > 0 {
			v.PC = instr.P2
		}
		cursor.EOF = true
		return nil
	}

	// Successfully positioned at first entry
	cursor.EOF = false
	return nil
}

func (v *VDBE) execNext(instr *Instruction) error {
	// Move cursor P1 to next entry, jump to P2 if more rows exist
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get btree cursor
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		cursor.EOF = true
		return nil
	}

	// Invalidate column cache
	v.IncrCacheCtr()

	// Move to next entry
	err = btCursor.Next()
	if err != nil {
		// Reached end of btree or error
		cursor.EOF = true
		return nil
	}

	// Successfully moved to next entry - jump to P2
	cursor.EOF = false
	v.PC = instr.P2

	// Track row scanned
	if v.Stats != nil {
		v.Stats.RecordRowScanned()
	}

	return nil
}

func (v *VDBE) execPrev(instr *Instruction) error {
	// Move cursor P1 to previous entry, jump to P2 if successful
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get btree cursor
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		cursor.EOF = true
		return nil
	}

	// Invalidate column cache
	v.IncrCacheCtr()

	// Move to previous entry
	err = btCursor.Previous()
	if err != nil {
		// Reached beginning of btree or error
		cursor.EOF = true
		return nil
	}

	// Successfully moved to previous entry - jump to P2
	cursor.EOF = false
	v.PC = instr.P2

	return nil
}

// handleIndexSeekGE performs SeekGE on an index cursor.
func (v *VDBE) handleIndexSeekGE(cursor *Cursor, idxCursor *btree.IndexCursor, keyReg *Mem, jumpAddr int) error {
	// Encode the search key as a SQLite record
	searchKey := encodeValueAsRecord(keyReg)

	// Seek to the first entry >= searchKey
	found, err := idxCursor.SeekIndex(searchKey)
	if err != nil {
		return err
	}

	// Check if cursor is still valid after seek
	if !found && !idxCursor.IsValid() {
		return v.seekNotFound(cursor, jumpAddr)
	}

	cursor.EOF = false
	return nil
}

func (v *VDBE) execSeekGE(instr *Instruction) error {
	// Seek cursor P1 to entry >= key in register P3
	// P1 = cursor number
	// P2 = jump target if not found
	// P3 = register containing search key
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	keyReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Handle index cursors
	if idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor); ok && idxCursor != nil {
		return v.handleIndexSeekGE(cursor, idxCursor, keyReg, instr.P2)
	}

	// Handle table cursors - simplified implementation
	cursor.EOF = false
	return nil
}

func (v *VDBE) execSeekLE(instr *Instruction) error {
	// Seek cursor P1 to entry <= key in register P3
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	keyReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	_ = keyReg
	cursor.EOF = false

	return nil
}

// seekNotFound marks the cursor as EOF and, if addr is positive, redirects
// the program counter to addr. It always returns nil so callers can
// "return seekNotFound(...)".
func (v *VDBE) seekNotFound(cursor *Cursor, addr int) error {
	cursor.EOF = true
	if addr > 0 {
		v.PC = addr
	}
	return nil
}

// seekGetBtCursor extracts the *btree.BtCursor from cursor, returning nil when
// the underlying cursor is absent or has the wrong type.
func seekGetBtCursor(cursor *Cursor) *btree.BtCursor {
	btc, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok {
		return nil
	}
	return btc
}

// seekLinearScan walks the btree from its current position looking for
// targetRowid. It returns true (found) together with a nil error when the row
// exists, false (not found) with nil when the scan is exhausted, and false
// with a non-nil error only on unexpected failures.
//
// In a real implementation this would use the btree's own key-search path
// instead of a sequential walk.
func seekLinearScan(btCursor *btree.BtCursor, targetRowid int64) (found bool, err error) {
	for {
		currentRowid := btCursor.GetKey()
		if currentRowid == targetRowid {
			return true, nil
		}
		if currentRowid > targetRowid {
			return false, nil
		}
		if err = btCursor.Next(); err != nil {
			return false, nil //nolint:nilerr // end-of-tree is not an error here
		}
	}
}

func (v *VDBE) execSeekRowid(instr *Instruction) error {
	// Seek cursor P1 to rowid in register P3, jump to P2 if not found.
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	rowidReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	btCursor := seekGetBtCursor(cursor)
	if btCursor == nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	if err = btCursor.MoveToFirst(); err != nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	found, err := seekLinearScan(btCursor, rowidReg.IntValue())
	if err != nil {
		return err
	}
	if !found {
		return v.seekNotFound(cursor, instr.P2)
	}

	cursor.EOF = false
	v.IncrCacheCtr()
	return nil
}

func (v *VDBE) execOpenEphemeral(instr *Instruction) error {
	// Open ephemeral/temporary table
	// P1 = cursor number
	// P2 = number of columns
	// P4 = key info (optional, for index tables)

	// Allocate cursors if needed
	if err := v.AllocCursors(instr.P1 + 1); err != nil {
		return err
	}

	// Create an in-memory btree for the ephemeral table
	// We need to import btree package types, but we store as interface{} to avoid cycles
	if v.Ctx == nil || v.Ctx.Btree == nil {
		return fmt.Errorf("cannot create ephemeral table: no btree context")
	}

	// Type assert to get the actual Btree
	bt, ok := v.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree type")
	}

	// Create a new table in the btree for this ephemeral cursor
	rootPage, err := bt.CreateTable()
	if err != nil {
		return fmt.Errorf("failed to create ephemeral table: %w", err)
	}

	// Open a cursor on this table
	btCursor := btree.NewCursor(bt, rootPage)

	// Create the cursor
	cursor := &Cursor{
		CurType:     CursorBTree,
		IsTable:     true,
		Writable:    true,
		BtreeCursor: btCursor,
		RootPage:    rootPage,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	v.Cursors[instr.P1] = cursor
	return nil
}

// seekLinearScanGT scans btree to find first row > targetRowid.
func seekLinearScanGT(btCursor *btree.BtCursor, targetRowid int64) (bool, error) {
	for {
		currentRowid := btCursor.GetKey()
		if currentRowid > targetRowid {
			return true, nil
		}
		if err := btCursor.Next(); err != nil {
			// Reached end without finding
			return false, nil
		}
	}
}

func (v *VDBE) execSeekGT(instr *Instruction) error {
	// Position cursor to first row > key
	// P1 = cursor number
	// P2 = jump address if not found
	// P3 = key register
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	keyReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	btCursor := seekGetBtCursor(cursor)
	if btCursor == nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	// Move to first entry
	if err = btCursor.MoveToFirst(); err != nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	// Linear scan to find first entry > key
	found, err := seekLinearScanGT(btCursor, keyReg.IntValue())
	if err != nil || !found {
		return v.seekNotFound(cursor, instr.P2)
	}

	cursor.EOF = false
	v.IncrCacheCtr()
	return nil
}

func (v *VDBE) execSeekLT(instr *Instruction) error {
	// Position cursor to last row < key
	// P1 = cursor number
	// P2 = jump address if not found
	// P3 = key register
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	keyReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	btCursor := seekGetBtCursor(cursor)
	if btCursor == nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	targetRowid := keyReg.IntValue()

	// Find and position to the last valid entry
	lastValidRowid, found, err := findLastRowidLessThan(btCursor, targetRowid)
	if err != nil || !found {
		return v.seekNotFound(cursor, instr.P2)
	}

	// Reposition to the last valid entry
	if err := repositionToRowid(btCursor, lastValidRowid); err != nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	cursor.EOF = false
	v.IncrCacheCtr()
	return nil
}

// findLastRowidLessThan scans the cursor to find the last rowid less than target.
func findLastRowidLessThan(btCursor *btree.BtCursor, targetRowid int64) (int64, bool, error) {
	// Move to first entry
	if err := btCursor.MoveToFirst(); err != nil {
		return 0, false, err
	}

	// Linear scan to find last entry < key
	found := false
	lastValidRowid := int64(0)

	for {
		currentRowid := btCursor.GetKey()
		if currentRowid < targetRowid {
			lastValidRowid = currentRowid
			found = true
		} else {
			// Gone too far
			break
		}
		if err := btCursor.Next(); err != nil {
			// Reached end
			break
		}
	}

	return lastValidRowid, found, nil
}

// repositionToRowid moves the cursor to first, then scans to the target rowid.
func repositionToRowid(btCursor *btree.BtCursor, targetRowid int64) error {
	if err := btCursor.MoveToFirst(); err != nil {
		return err
	}

	found, err := seekLinearScan(btCursor, targetRowid)
	if err != nil || !found {
		return fmt.Errorf("failed to reposition to rowid %d", targetRowid)
	}

	return nil
}

func (v *VDBE) execNotExists(instr *Instruction) error {
	// Jump if rowid does not exist
	// P1 = cursor number
	// P2 = jump address if not found
	// P3 = rowid register
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	rowidReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	found, err := v.checkRowidExists(cursor, rowidReg.IntValue())
	if err != nil {
		return err
	}

	if !found {
		v.jumpToAddress(instr.P2)
	}

	return nil
}

// checkRowidExists checks if a rowid exists in the cursor's table
func (v *VDBE) checkRowidExists(cursor *Cursor, rowid int64) (bool, error) {
	btCursor := seekGetBtCursor(cursor)
	if btCursor == nil {
		return false, nil
	}

	if err := btCursor.MoveToFirst(); err != nil {
		return false, nil
	}

	return seekLinearScan(btCursor, rowid)
}

// jumpToAddress performs a jump to the given address if it's valid
func (v *VDBE) jumpToAddress(address int) {
	if address > 0 {
		v.PC = address
	}
}

func (v *VDBE) execDeferredSeek(instr *Instruction) error {
	// Seek cursor but defer until data needed
	// P1 = index cursor number
	// P2 = table cursor number
	// P3 = rowid register (or 0 to use rowid from index cursor P1)
	//
	// This is an optimization opcode that defers seeking the table cursor
	// until data is actually needed from it. For this simplified implementation,
	// we'll just perform the seek immediately.

	tableCursor, err := v.GetCursor(instr.P2)
	if err != nil {
		return err
	}

	rowid, err := v.getDeferredSeekRowid(instr, tableCursor)
	if err != nil {
		return err
	}

	return v.performDeferredSeek(tableCursor, rowid)
}

// getDeferredSeekRowid extracts the rowid to seek to from either index cursor or register
func (v *VDBE) getDeferredSeekRowid(instr *Instruction, tableCursor *Cursor) (int64, error) {
	if instr.P3 != 0 {
		return v.getRowidFromRegister(instr.P3)
	}
	return v.getRowidFromIndexCursor(instr.P1, tableCursor)
}

// getRowidFromRegister retrieves rowid from a register
func (v *VDBE) getRowidFromRegister(regNum int) (int64, error) {
	rowidReg, err := v.GetMem(regNum)
	if err != nil {
		return 0, err
	}
	return rowidReg.IntValue(), nil
}

// getRowidFromIndexCursor retrieves rowid from an index cursor
func (v *VDBE) getRowidFromIndexCursor(cursorNum int, tableCursor *Cursor) (int64, error) {
	indexCursor, err := v.GetCursor(cursorNum)
	if err != nil {
		return 0, err
	}

	idxCursor, ok := indexCursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		tableCursor.EOF = true
		return 0, nil
	}

	return idxCursor.GetRowid(), nil
}

// performDeferredSeek executes the actual seek operation on the table cursor
func (v *VDBE) performDeferredSeek(tableCursor *Cursor, rowid int64) error {
	btCursor := seekGetBtCursor(tableCursor)
	if btCursor == nil {
		tableCursor.EOF = true
		return nil
	}

	if err := btCursor.MoveToFirst(); err != nil {
		tableCursor.EOF = true
		return nil
	}

	found, err := seekLinearScan(btCursor, rowid)
	if err != nil {
		return err
	}

	tableCursor.EOF = !found
	if found {
		v.IncrCacheCtr()
	}
	return nil
}

// Data retrieval implementations

func (v *VDBE) execColumn(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}
	dst, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}
	payload := v.getColumnPayload(cursor, dst)
	if payload == nil {
		return nil
	}
	return v.parseColumnIntoMem(payload, instr.P2, dst, cursor)
}

// getColumnPayload returns the payload from cursor, or nil if unavailable.
func (v *VDBE) getColumnPayload(cursor *Cursor, dst *Mem) []byte {
	if cursor.NullRow || cursor.EOF {
		dst.SetNull()
		return nil
	}

	if cursor.CurType == CursorPseudo {
		return v.getPseudoCursorPayload(cursor, dst)
	}

	return v.getBtreeCursorPayload(cursor, dst)
}

// getPseudoCursorPayload returns payload from a pseudo-table cursor
func (v *VDBE) getPseudoCursorPayload(cursor *Cursor, dst *Mem) []byte {
	pseudoMem, err := v.GetMem(cursor.PseudoReg)
	if err != nil || pseudoMem.IsNull() {
		dst.SetNull()
		return nil
	}

	if pseudoMem.IsBlob() {
		return pseudoMem.BlobValue()
	}

	dst.SetNull()
	return nil
}

// getBtreeCursorPayload returns payload from a btree cursor
func (v *VDBE) getBtreeCursorPayload(cursor *Cursor, dst *Mem) []byte {
	// Handle index cursors
	if idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor); ok && idxCursor != nil {
		// For index cursors, the "payload" is the index key (which contains the indexed column values)
		key := idxCursor.GetKey()
		if key == nil {
			dst.SetNull()
		}
		return key
	}

	// Handle table cursors
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		dst.SetNull()
		return nil
	}

	payload := btCursor.GetPayload()
	if payload == nil {
		dst.SetNull()
	}
	return payload
}

// parseColumnIntoMem parses a record column into dst.
// If the column doesn't exist in the record (e.g., added via ALTER TABLE ADD COLUMN),
// it returns the column's DEFAULT value from the schema.
func (v *VDBE) parseColumnIntoMem(payload []byte, colIndex int, dst *Mem, cursor *Cursor) error {
	if err := parseRecordColumn(payload, colIndex, dst); err != nil {
		return fmt.Errorf("failed to parse record column: %w", err)
	}

	// If the column was set to NULL and we have a cursor with table metadata,
	// check if this column has a DEFAULT value
	if dst.IsNull() && cursor != nil && cursor.Table != nil {
		// Try to get default value from schema
		// This handles the case where a column was added via ALTER TABLE ADD COLUMN
		// and existing rows don't have data for that column
		v.applyDefaultValueIfAvailable(colIndex, dst, cursor.Table)
	}

	return nil
}

// applyDefaultValueIfAvailable applies a default value from schema if available.
// This is used when reading columns that were added via ALTER TABLE ADD COLUMN.
func (v *VDBE) applyDefaultValueIfAvailable(colIndex int, dst *Mem, tableInterface interface{}) {
	// Define interfaces that schema.Table and schema.Column implement
	// to avoid import cycle
	type colDefault interface {
		GetDefault() interface{}
	}

	type tblColumns interface {
		GetColumns() []interface{}
	}

	tbl, ok := tableInterface.(tblColumns)
	if !ok {
		return
	}

	cols := tbl.GetColumns()
	if colIndex < 0 || colIndex >= len(cols) {
		return
	}

	col, ok := cols[colIndex].(colDefault)
	if !ok {
		return
	}

	defaultVal := col.GetDefault()
	if defaultVal == nil {
		return
	}

	// Parse the default value string and set it in dst
	if defaultStr, ok := defaultVal.(string); ok {
		v.parseDefaultValue(defaultStr, dst)
	}
}

// parseDefaultValue parses a DEFAULT value string and stores it in dst.
// This handles integer, real, string, and NULL literals.
func (v *VDBE) parseDefaultValue(defaultStr string, dst *Mem) {
	// Try to parse as integer first
	if v.tryParseAsInteger(defaultStr, dst) {
		return
	}

	// Try to parse as float
	if v.tryParseAsFloat(defaultStr, dst) {
		return
	}

	// Check for NULL
	if defaultStr == "NULL" {
		dst.SetNull()
		return
	}

	// Check for string literal (remove quotes)
	if v.tryParseAsQuotedString(defaultStr, dst) {
		return
	}

	// Default: treat as string
	dst.SetStr(defaultStr)
}

// tryParseAsInteger attempts to parse defaultStr as an integer.
// Returns true if successful, false otherwise.
func (v *VDBE) tryParseAsInteger(defaultStr string, dst *Mem) bool {
	if i, err := strconv.ParseInt(defaultStr, 10, 64); err == nil {
		dst.SetInt(i)
		return true
	}
	return false
}

// tryParseAsFloat attempts to parse defaultStr as a float.
// Returns true if successful, false otherwise.
func (v *VDBE) tryParseAsFloat(defaultStr string, dst *Mem) bool {
	if f, err := strconv.ParseFloat(defaultStr, 64); err == nil {
		dst.SetReal(f)
		return true
	}
	return false
}

// tryParseAsQuotedString attempts to parse defaultStr as a quoted string.
// Returns true if successful (string was quoted), false otherwise.
func (v *VDBE) tryParseAsQuotedString(defaultStr string, dst *Mem) bool {
	if len(defaultStr) < 2 {
		return false
	}

	if isQuotedWith(defaultStr, '\'') || isQuotedWith(defaultStr, '"') {
		dst.SetStr(defaultStr[1 : len(defaultStr)-1])
		return true
	}

	return false
}

// isQuotedWith checks if a string is quoted with the given quote character.
func isQuotedWith(s string, quote byte) bool {
	return s[0] == quote && s[len(s)-1] == quote
}

func (v *VDBE) execRowid(instr *Instruction) error {
	// Get rowid from cursor P1 into register P2
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	v.extractRowidFromCursor(cursor, dst)
	return nil
}

// extractRowidFromCursor extracts the rowid from a cursor and sets it in the destination memory.
func (v *VDBE) extractRowidFromCursor(cursor *Cursor, dst *Mem) {
	if shouldSetRowidToNull(cursor) {
		dst.SetNull()
		return
	}

	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		dst.SetNull()
		return
	}

	rowid := btCursor.GetKey()
	dst.SetInt(rowid)
}

// shouldSetRowidToNull checks if the rowid should be NULL based on cursor state.
func shouldSetRowidToNull(cursor *Cursor) bool {
	return cursor.NullRow || cursor.EOF || cursor.CurType == CursorPseudo
}

// parseRecordColumn parses a specific column from a SQLite record
// This is a simplified version to avoid import cycles with the sql package
func parseRecordColumn(data []byte, colIndex int, dst *Mem) error {
	if len(data) == 0 {
		dst.SetNull()
		return nil
	}

	// Read serial types from the record header
	serialTypes, bodyOffset, err := parseRecordColumnHeader(data, dst)
	if err != nil {
		return err
	}

	// Check if column index is valid
	if colIndex < 0 || colIndex >= len(serialTypes) {
		dst.SetNull()
		return nil
	}

	// Skip to the target column in the body
	offset := skipToColumn(serialTypes, bodyOffset, colIndex)

	// Parse the target column value
	st := serialTypes[colIndex]
	return parseSerialValue(data, offset, st, dst)
}

// parseRecordColumnHeader reads the header of a SQLite record and returns the serial types
// and the offset where the body begins.
func parseRecordColumnHeader(data []byte, dst *Mem) ([]uint64, int, error) {
	// Read header size (varint)
	headerSize, n := getVarint(data, 0)
	if n == 0 {
		dst.SetNull()
		return nil, 0, fmt.Errorf("invalid header size")
	}

	if headerSize > math.MaxInt {
		dst.SetNull()
		return nil, 0, fmt.Errorf("header size too large")
	}

	// Read serial types from header
	serialTypes, offset, err := readSerialTypes(data, n, int(headerSize), dst)
	if err != nil {
		return nil, 0, err
	}

	return serialTypes, offset, nil
}

// readSerialTypes reads all serial types from the record header.
func readSerialTypes(data []byte, startOffset, headerSize int, dst *Mem) ([]uint64, int, error) {
	serialTypes := make([]uint64, 0)
	offset := startOffset
	for offset < headerSize {
		st, n := getVarint(data, offset)
		if n == 0 {
			dst.SetNull()
			return nil, 0, fmt.Errorf("invalid serial type")
		}
		serialTypes = append(serialTypes, st)
		offset += n
	}
	return serialTypes, offset, nil
}

// skipToColumn calculates the offset to a specific column by skipping over previous columns.
func skipToColumn(serialTypes []uint64, bodyOffset int, colIndex int) int {
	offset := bodyOffset
	for i := 0; i < colIndex; i++ {
		offset += serialTypeLen(serialTypes[i])
	}
	return offset
}

// getVarint reads a SQLite varint from buf starting at offset
// SQLite uses 7-bit continuation encoding (MSB set = more bytes follow)
func getVarint(buf []byte, offset int) (uint64, int) {
	if offset >= len(buf) {
		return 0, 0
	}
	if buf[offset] < 0x80 {
		return uint64(buf[offset]), 1
	}
	if offset+1 < len(buf) && buf[offset+1] < 0x80 {
		return (uint64(buf[offset]&0x7f) << 7) | uint64(buf[offset+1]), 2
	}
	return getVarintGeneral(buf, offset)
}

// getVarintGeneral handles the general case for varints > 2 bytes.
func getVarintGeneral(buf []byte, offset int) (uint64, int) {
	var v uint64
	for i := 0; i < 9 && offset+i < len(buf); i++ {
		b := buf[offset+i]
		if i < 8 {
			v = (v << 7) | uint64(b&0x7f)
			if b&0x80 == 0 {
				return v, i + 1
			}
		} else {
			v = (v << 8) | uint64(b)
			return v, 9
		}
	}
	return v, 0
}

// serialTypeLenTable maps serial types 0-11 to their byte lengths.
var serialTypeLenTable = [12]int{0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0}

// serialTypeLen returns the number of bytes required for a value with the given serial type
func serialTypeLen(serialType uint64) int {
	if serialType < 12 {
		return serialTypeLenTable[serialType]
	}
	// Calculate the result as uint64 first to check bounds
	result := (serialType - 12) / 2
	// Check if it fits in int
	if result > uint64(math.MaxInt) {
		return 0 // Invalid, return 0 to trigger error
	}
	return int(result)
}

// parseSerialValue parses a SQLite record value based on its serial type.
func parseSerialValue(data []byte, offset int, st uint64, mem *Mem) error {
	switch st {
	case 0:
		mem.SetNull()
		return nil
	case 8:
		mem.SetInt(0)
		return nil
	case 9:
		mem.SetInt(1)
		return nil
	case 1, 2, 3, 4, 5, 6:
		return parseSerialInt(data, offset, st, mem)
	case 7:
		return parseSerialFloat(data, offset, mem)
	default:
		return parseSerialBlobOrText(data, offset, st, mem)
	}
}

// parseSerialInt parses signed integers of various sizes (serial types 1-6).
func parseSerialInt(data []byte, offset int, st uint64, mem *Mem) error {
	size := serialTypeLen(st)
	if offset+size > len(data) {
		mem.SetNull()
		return fmt.Errorf("truncated int%d", size*8)
	}

	v := decodeSerialIntValue(data, offset, st)
	mem.SetInt(v)
	return nil
}

// decodeSerialIntValue decodes an integer value based on serial type.
func decodeSerialIntValue(data []byte, offset int, st uint64) int64 {
	switch st {
	case 1:
		return int64(int8(data[offset]))
	case 2:
		return int64(int16(binary.BigEndian.Uint16(data[offset:])))
	case 3:
		return parseSignedInt24(data[offset:])
	case 4:
		return int64(int32(binary.BigEndian.Uint32(data[offset:])))
	case 5:
		return parseSignedInt48(data[offset:])
	case 6:
		return int64(binary.BigEndian.Uint64(data[offset:]))
	default:
		return 0
	}
}

// parseSignedInt24 decodes a 3-byte big-endian signed integer.
func parseSignedInt24(data []byte) int64 {
	v := int32(data[0])<<16 | int32(data[1])<<8 | int32(data[2])
	if v&0x800000 != 0 {
		v |= ^0xffffff // Sign extend
	}
	return int64(v)
}

// parseSignedInt48 decodes a 6-byte big-endian signed integer.
func parseSignedInt48(data []byte) int64 {
	v := int64(data[0])<<40 | int64(data[1])<<32 |
		int64(data[2])<<24 | int64(data[3])<<16 |
		int64(data[4])<<8 | int64(data[5])
	if v&0x800000000000 != 0 {
		v |= ^0xffffffffffff // Sign extend
	}
	return v
}

// parseSerialFloat parses a float64 from record data (serial type 7).
func parseSerialFloat(data []byte, offset int, mem *Mem) error {
	if offset+8 > len(data) {
		mem.SetNull()
		return fmt.Errorf("truncated float64")
	}
	bits := binary.BigEndian.Uint64(data[offset:])
	mem.SetReal(math.Float64frombits(bits))
	return nil
}

// parseSerialBlobOrText parses blob or text from record data (serial types >= 12).
func parseSerialBlobOrText(data []byte, offset int, st uint64, mem *Mem) error {
	length := serialTypeLen(st)
	if offset+length > len(data) {
		mem.SetNull()
		return fmt.Errorf("truncated blob/text")
	}

	b := make([]byte, length)
	copy(b, data[offset:offset+length])

	if st%2 == 0 {
		mem.SetBlob(b)
	} else {
		mem.SetStr(string(b))
	}
	return nil
}

func (v *VDBE) execResultRow(instr *Instruction) error {
	// Copy values from registers P1..P1+P2-1 to ResultRow
	// This makes a row of data available to the driver
	v.ResultRow = make([]*Mem, instr.P2)
	for i := 0; i < instr.P2; i++ {
		mem, err := v.GetMem(instr.P1 + i)
		if err != nil {
			return err
		}
		// Make a copy for the result using pooled Mem
		v.ResultRow[i] = GetMem()
		v.ResultRow[i].Copy(mem)
	}

	// Track row returned
	if v.Stats != nil {
		v.Stats.RowsRead++
	}

	// Set state to StateRowReady to pause execution and allow the driver to read this row
	// The Run() loop will pause here, and the driver's Next() method can read ResultRow
	// When Step() is called again, it will clear ResultRow and continue execution
	v.State = StateRowReady
	return nil
}

// Data modification implementations

func (v *VDBE) execNewRowid(instr *Instruction) error {
	// Generate a new rowid for cursor P1, store in register P3
	// P1 = cursor number, P3 = destination register
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get the btree to find max rowid
	bt, ok := v.Ctx.Btree.(*btree.Btree)
	if !ok {
		return fmt.Errorf("invalid btree context type")
	}

	// Generate new rowid by finding the max rowid in the table
	newRowid, err := bt.NewRowid(cursor.RootPage)
	if err != nil {
		// If table is empty, NewRowid returns an error, so start with 1
		newRowid = 1
	}

	// Store the new rowid
	cursor.LastRowid = newRowid

	// Store the new rowid in register P3
	mem, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}
	mem.SetInt(newRowid)

	return nil
}

func (v *VDBE) execMakeRecord(instr *Instruction) error {
	// Create a record from registers P1..P1+P2-1 and store in register P3
	// P1 = first register, P2 = number of registers, P3 = destination
	startReg := instr.P1
	numFields := instr.P2
	destReg := instr.P3

	// Collect values from registers
	values := make([]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		mem, err := v.GetMem(startReg + i)
		if err != nil {
			values[i] = nil
			continue
		}
		values[i] = memToInterface(mem)
	}

	// Create a simple record representation
	// In a full implementation, this would encode according to SQLite record format
	mem, err := v.GetMem(destReg)
	if err != nil {
		return err
	}
	mem.SetBlob(encodeSimpleRecord(values))

	return nil
}

// memToInterface converts a Mem value to a Go interface{}
func memToInterface(m *Mem) interface{} {
	if m == nil || m.IsNull() {
		return nil
	}
	if m.IsInt() {
		return m.IntValue()
	}
	if m.IsReal() {
		return m.RealValue()
	}
	if m.IsString() {
		return m.StrValue()
	}
	if m.IsBlob() {
		return m.BlobValue()
	}
	return nil
}

// encodeSimpleRecord creates a SQLite record encoding
// SQLite record format:
// - Header size (varint)
// - Serial type for each column (varint)
// - Data for each column
func encodeSimpleRecord(values []interface{}) []byte {
	if len(values) == 0 {
		return []byte{0}
	}

	var header, data []byte
	header = append(header, 0) // placeholder for header size

	for _, val := range values {
		serialType, valueData := encodeValue(val)
		header = appendVarint(header, serialType)
		data = append(data, valueData...)
	}

	return buildRecord(header, data)
}

// encodeValue encodes a value and returns its serial type and data bytes.
func encodeValue(val interface{}) (uint64, []byte) {
	switch v := val.(type) {
	case nil:
		return 0, nil
	case int64:
		return encodeInt64(v)
	case float64:
		return encodeFloat64(v)
	case string:
		return uint64(2*len(v) + 13), []byte(v)
	case []byte:
		return uint64(2*len(v) + 12), v
	default:
		return 0, nil
	}
}

var int64Ranges = []struct {
	lo     int64
	hi     int64
	serial uint64
}{
	{-128, 127, 1},
	{-32768, 32767, 2},
	{-8388608, 8388607, 3},
	{-2147483648, 2147483647, 4},
}

func int64SerialType(v int64) uint64 {
	for _, r := range int64Ranges {
		if v >= r.lo && v <= r.hi {
			return r.serial
		}
	}
	return 6
}

func int64Bytes(v int64, serial uint64) []byte {
	switch serial {
	case 1:
		return []byte{byte(v)}
	case 2:
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(v))
		return buf
	case 3:
		return []byte{byte(v >> 16), byte(v >> 8), byte(v)}
	case 4:
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		return buf
	default:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	}
}

func encodeInt64(v int64) (uint64, []byte) {
	if v == 0 {
		return 8, nil
	}
	if v == 1 {
		return 9, nil
	}
	serial := int64SerialType(v)
	return serial, int64Bytes(v, serial)
}

// encodeFloat64 encodes a float64 to SQLite serial type 7 format.
func encodeFloat64(v float64) (uint64, []byte) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return 7, buf
}

// buildRecord assembles the final record from header and data.
func buildRecord(header, data []byte) []byte {
	headerSizeVarint := encodeVarint(uint64(len(header)))
	record := make([]byte, 0, len(headerSizeVarint)+len(header)-1+len(data))
	record = append(record, headerSizeVarint...)
	record = append(record, header[1:]...)
	record = append(record, data...)
	return record
}

// encodeVarint encodes a uint64 as a SQLite varint and returns the bytes.
// SQLite uses 7-bit continuation encoding (MSB set = more bytes follow).
func encodeVarint(v uint64) []byte {
	n := varintLen(v)
	return encodeVarintN(v, n)
}

// varintThresholds defines the upper bounds for each varint size.
var varintThresholds = [8]uint64{
	0x7f, 0x3fff, 0x1fffff, 0xfffffff,
	0x7ffffffff, 0x3ffffffffff, 0x1ffffffffffff, 0xffffffffffffff,
}

// varintLen returns the number of bytes needed to encode v as a SQLite varint.
func varintLen(v uint64) int {
	for i, thresh := range varintThresholds {
		if v <= thresh {
			return i + 1
		}
	}
	return 9
}

// encodeVarintN encodes v as an n-byte SQLite varint.
func encodeVarintN(v uint64, n int) []byte {
	if n == 9 {
		// Special case: 9-byte varint has 8 continuation bytes + 8-bit final byte
		return []byte{
			byte((v>>57)&0x7f | 0x80), byte((v>>50)&0x7f | 0x80),
			byte((v>>43)&0x7f | 0x80), byte((v>>36)&0x7f | 0x80),
			byte((v>>29)&0x7f | 0x80), byte((v>>22)&0x7f | 0x80),
			byte((v>>15)&0x7f | 0x80), byte((v>>8)&0x7f | 0x80),
			byte(v),
		}
	}

	buf := make([]byte, n)
	for i := n - 1; i > 0; i-- {
		buf[i] = byte(v & 0x7f)
		v >>= 7
	}
	buf[0] = byte(v & 0x7f)

	// Set continuation bits on all but the last byte
	for i := 0; i < n-1; i++ {
		buf[i] |= 0x80
	}
	return buf
}

// appendVarint appends a varint to a byte slice
func appendVarint(buf []byte, v uint64) []byte {
	return append(buf, encodeVarint(v)...)
}

func (v *VDBE) execInsert(instr *Instruction) error {
	cursor, btCursor, err := v.getWritableBtreeCursor(instr.P1)
	if err != nil {
		return err
	}
	payload, err := v.getInsertPayload(instr.P2)
	if err != nil {
		return err
	}
	rowid, err := v.getInsertRowid(instr.P3, cursor)
	if err != nil {
		return err
	}

	// Check constraints before inserting - P4.Z contains the table name
	if instr.P4.Z != "" {
		// Check NOT NULL constraints
		if err := v.checkNotNullConstraints(instr.P4.Z, payload); err != nil {
			return err
		}
		// Check CHECK constraints
		if err := v.checkCheckConstraints(instr.P4.Z, payload, rowid); err != nil {
			return err
		}
		// Check UNIQUE column constraints
		if err := v.checkUniqueConstraints(instr.P4.Z, payload, btCursor, rowid); err != nil {
			return err
		}
	}

	if err = btCursor.Insert(rowid, payload); err != nil {
		// Check if this is a duplicate key error and convert to SQL constraint error
		if strings.Contains(err.Error(), "duplicate key") {
			return fmt.Errorf("UNIQUE constraint failed: PRIMARY KEY must be unique")
		}
		return fmt.Errorf("btree insert failed: %w", err)
	}
	cursor.LastRowid = rowid
	v.LastInsertID = rowid // Track last insert ID for database/sql driver
	// P4.I == 1 means this is part of UPDATE (paired with Delete), don't double-count
	if instr.P4.I != 1 {
		v.NumChanges++
	}

	// Track row written
	if v.Stats != nil {
		v.Stats.RowsWritten++
	}

	return nil
}

// getWritableBtreeCursor returns a writable btree cursor for the given cursor number.
func (v *VDBE) getWritableBtreeCursor(cursorNum int) (*Cursor, *btree.BtCursor, error) {
	cursor, err := v.GetCursor(cursorNum)
	if err != nil {
		return nil, nil, err
	}
	if !cursor.Writable {
		return nil, nil, fmt.Errorf("cursor %d is not writable (opened with OpenRead instead of OpenWrite)", cursorNum)
	}
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		return nil, nil, fmt.Errorf("invalid btree cursor for insert")
	}
	return cursor, btCursor, nil
}

// getInsertPayload retrieves the blob payload from the given register.
func (v *VDBE) getInsertPayload(reg int) ([]byte, error) {
	data, err := v.GetMem(reg)
	if err != nil {
		return nil, err
	}
	if !data.IsBlob() {
		return nil, fmt.Errorf("insert data must be a blob")
	}
	return data.BlobValue(), nil
}

// getInsertRowid determines the rowid for the insert operation.
func (v *VDBE) getInsertRowid(p3 int, cursor *Cursor) (int64, error) {
	if p3 == 0 {
		// Auto-generate a new rowid
		bt, ok := v.Ctx.Btree.(*btree.Btree)
		if !ok {
			return 0, fmt.Errorf("invalid btree type for rowid generation")
		}
		newRowid, err := bt.NewRowid(cursor.RootPage)
		if err != nil {
			// If table is empty, start with 1
			newRowid = 1
		}
		cursor.LastRowid = newRowid
		return newRowid, nil
	}
	rowidMem, err := v.GetMem(p3)
	if err != nil {
		return 0, err
	}
	return rowidMem.IntValue(), nil
}

// checkUniqueConstraints verifies that inserting the given payload doesn't violate
// any UNIQUE column constraints in the table.
func (v *VDBE) checkUniqueConstraints(tableName string, payload []byte, btCursor *btree.BtCursor, newRowid int64) error {
	// Get the table schema from the context
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil // No schema available, skip check
	}

	// Type assert to get the schema with GetTableByName method
	// Returns interface{} to avoid import cycle with schema package
	type schemaWithGetTableByName interface {
		GetTableByName(name string) (interface{}, bool)
	}

	schema, ok := v.Ctx.Schema.(schemaWithGetTableByName)
	if !ok {
		return nil // Schema doesn't support GetTableByName, skip check
	}

	// Get the table definition
	tableIface, exists := schema.GetTableByName(tableName)
	if !exists {
		return nil // Table not found in schema, skip check
	}

	// Type assert to access table columns via GetColumns() method
	type tableWithColumns interface {
		GetColumns() []interface{}
	}

	table, ok := tableIface.(tableWithColumns)
	if !ok {
		return nil // Can't access columns, skip check
	}

	columns := table.GetColumns()

	// Define interface for column info (matches schema.Column methods)
	type columnInfo interface {
		GetName() string
		IsUniqueColumn() bool
		IsPrimaryKeyColumn() bool
	}

	// Check each column for UNIQUE constraint
	// Track recordIdx separately (skips INTEGER PRIMARY KEY columns which are stored as rowid)
	recordIdx := 0
	for _, colIface := range columns {
		// Type assert to access column info
		col, ok := colIface.(columnInfo)
		if !ok {
			recordIdx++
			continue
		}

		// INTEGER PRIMARY KEY columns are stored as rowid, not in the record
		isPK := col.IsPrimaryKeyColumn()

		// Skip if not a UNIQUE column (PRIMARY KEY is handled by btree)
		if !col.IsUniqueColumn() || isPK {
			if !isPK {
				recordIdx++
			}
			continue
		}

		// Extract the value from the new record for this column
		// Use recordIdx (not colIdx) because INTEGER PRIMARY KEY columns don't appear in record
		newValueMem := NewMem()
		if err := parseRecordColumn(payload, recordIdx, newValueMem); err != nil {
			recordIdx++
			continue // Skip if can't parse
		}

		// Scan the table to check for duplicates
		if err := v.scanTableForDuplicate(btCursor, recordIdx, newValueMem, newRowid, col.GetName()); err != nil {
			return err
		}

		recordIdx++
	}

	return nil
}

// scanTableForDuplicate scans the entire table to check if the given column value
// already exists in any row (excluding the row with newRowid for UPDATE operations).
// IMPORTANT: Creates a separate read cursor to avoid disturbing the insert cursor position.
func (v *VDBE) scanTableForDuplicate(insertCursor *btree.BtCursor, colIdx int, newValue *Mem, newRowid int64, colName string) error {
	// Create a separate read cursor for scanning to avoid disturbing the insert cursor
	scanCursor := btree.NewCursor(insertCursor.Btree, insertCursor.RootPage)

	// Move to the first record
	if err := scanCursor.MoveToFirst(); err != nil {
		// Table is empty, no duplicates possible
		return nil
	}

	// Iterate through all records
	for scanCursor.IsValid() {
		// Get current rowid
		currentRowid := scanCursor.GetKey()

		// Skip if this is the row we're inserting/updating (for UPDATE operations)
		// For INSERT operations, newRowid won't match any existing row
		if currentRowid != newRowid {
			// Get the record data
			recordData, err := scanCursor.GetPayloadWithOverflow()
			if err != nil {
				// Skip if can't get payload
				if err := scanCursor.Next(); err != nil {
					break
				}
				continue
			}

			// Extract the column value from this record
			existingValueMem := NewMem()
			if err := parseRecordColumn(recordData, colIdx, existingValueMem); err != nil {
				// Skip records that can't be parsed
				if err := scanCursor.Next(); err != nil {
					break
				}
				continue
			}

			// Compare the values
			if v.compareMemValues(existingValueMem, newValue) == 0 {
				// Duplicate found!
				return fmt.Errorf("UNIQUE constraint failed: %s", colName)
			}
		}

		// Move to next record
		if err := scanCursor.Next(); err != nil {
			break
		}
	}

	return nil
}

// compareMemValues compares two Mem values and returns:
// -1 if a < b, 0 if a == b, 1 if a > b
func (v *VDBE) compareMemValues(a, b *Mem) int {
	// Handle NULL values
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Compare based on type
	if a.IsInt() && b.IsInt() {
		aVal := a.IntValue()
		bVal := b.IntValue()
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}

	if a.IsReal() && b.IsReal() {
		aVal := a.RealValue()
		bVal := b.RealValue()
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}

	if a.IsString() && b.IsString() {
		aVal := a.StringValue()
		bVal := b.StringValue()
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}

	if a.IsBlob() && b.IsBlob() {
		aVal := a.BlobValue()
		bVal := b.BlobValue()
		return compareBytes(aVal, bVal)
	}

	// Mixed types - try string comparison
	aStr := fmt.Sprintf("%v", a.Value())
	bStr := fmt.Sprintf("%v", b.Value())
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (v *VDBE) execDelete(instr *Instruction) error {
	// Delete current record from cursor P1
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Verify cursor is writable
	if !cursor.Writable {
		return fmt.Errorf("cursor %d is not writable (opened with OpenRead instead of OpenWrite)", instr.P1)
	}

	// Get the btree cursor
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		return fmt.Errorf("invalid btree cursor for delete operation")
	}

	// Delete the current row from the btree
	err = btCursor.Delete()
	if err != nil {
		return fmt.Errorf("failed to delete from btree: %w", err)
	}

	// Invalidate column cache since cursor state changed
	v.IncrCacheCtr()

	// Update change counter
	v.NumChanges++

	// Track row written (deleted)
	if v.Stats != nil {
		v.Stats.RowsWritten++
	}

	return nil
}

// Comparison implementations

func (v *VDBE) execEq(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp == 0 })
}

func (v *VDBE) execNe(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp != 0 })
}

func (v *VDBE) execLt(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp < 0 })
}

func (v *VDBE) execLe(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp <= 0 })
}

func (v *VDBE) execGt(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp > 0 })
}

func (v *VDBE) execGe(instr *Instruction) error {
	return v.execCompare(instr, func(cmp int) bool { return cmp >= 0 })
}

func (v *VDBE) execCompare(instr *Instruction, test func(int) bool) error {
	// Compare r[P1] with r[P2] and store the boolean result (0 or 1) in r[P3].
	// P1 = left operand register
	// P2 = right operand register
	// P3 = result register (1 if condition is true, 0 otherwise, NULL if either operand is NULL)
	// P4 = optional collation name (for string comparisons)
	left, right, err := v.getCompareOperands(instr)
	if err != nil {
		return err
	}

	// NULL handling: NULL compared to anything (including NULL) results in NULL (UNKNOWN)
	// This implements SQL three-valued logic
	if left.IsNull() || right.IsNull() {
		v.Mem[instr.P3].SetNull()
		return nil
	}

	cmp := v.compareWithOptionalCollation(left, right, instr)

	result := int64(0)
	if test(cmp) {
		result = 1
	}

	v.Mem[instr.P3].SetInt(result)
	return nil
}

// getCompareOperands retrieves and validates the left and right operands for comparison operations.
func (v *VDBE) getCompareOperands(instr *Instruction) (left, right *Mem, err error) {
	left, err = v.GetMem(instr.P1)
	if err != nil {
		return nil, nil, err
	}

	right, err = v.GetMem(instr.P2)
	if err != nil {
		return nil, nil, err
	}

	// Validate result register
	if instr.P3 >= len(v.Mem) {
		return nil, nil, fmt.Errorf("register %d out of range", instr.P3)
	}

	return left, right, nil
}

// compareWithOptionalCollation performs comparison using P4 collation if present, otherwise default comparison.
func (v *VDBE) compareWithOptionalCollation(left, right *Mem, instr *Instruction) int {
	if instr.P4Type == P4Static && instr.P4.Z != "" {
		return left.CompareWithCollation(right, instr.P4.Z)
	}
	return left.Compare(right)
}

// execIsNull - Jump to P2 if the value in register P1 is NULL.
func (v *VDBE) execIsNull(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	if mem.IsNull() {
		v.PC = instr.P2
	}
	return nil
}

// execNotNull - Jump to P2 if the value in register P1 is NOT NULL.
func (v *VDBE) execNotNull(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	if !mem.IsNull() {
		v.PC = instr.P2
	}
	return nil
}

// Arithmetic implementations

func (v *VDBE) execAdd(instr *Instruction) error {
	// P3 = P1 + P2
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	result.Copy(left)
	return result.Add(right)
}

func (v *VDBE) execConcat(instr *Instruction) error {
	// P3 = P1 || P2 (string concatenation)
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Handle NULL: NULL || anything = NULL
	if left.IsNull() || right.IsNull() {
		result.SetNull()
		return nil
	}

	// Concatenate as strings
	s1 := left.StrValue()
	s2 := right.StrValue()
	return result.SetStr(s1 + s2)
}

func (v *VDBE) execSubtract(instr *Instruction) error {
	// P3 = P1 - P2
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	result.Copy(left)
	return result.Subtract(right)
}

func (v *VDBE) execMultiply(instr *Instruction) error {
	// P3 = P1 * P2
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	result.Copy(left)
	return result.Multiply(right)
}

func (v *VDBE) execDivide(instr *Instruction) error {
	// P3 = P1 / P2
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	result.Copy(left)
	return result.Divide(right)
}

func (v *VDBE) execRemainder(instr *Instruction) error {
	// P3 = P1 % P2
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	result.Copy(left)
	return result.Remainder(right)
}

func (v *VDBE) execAddImm(instr *Instruction) error {
	// Add immediate value P2 to register P1
	// P1 = register to modify
	// P2 = immediate value to add
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// Get current value as integer
	if !mem.IsInt() && !mem.IsNull() {
		// Try to convert to integer
		mem.Integerify()
	}

	currentVal := mem.IntValue()
	newVal := currentVal + int64(instr.P2)
	mem.SetInt(newVal)

	return nil
}

// Aggregate function implementations

func (v *VDBE) execAggStep(instr *Instruction) error {
	// Execute aggregate step function
	// P1 = cursor (for grouping context)
	// P2 = first argument register
	// P3 = aggregate function index
	// P4 = function name (string)
	// P5 = number of arguments
	return v.opAggStep(instr.P1, instr.P2, instr.P3, instr.P1, int(instr.P5))
}

func (v *VDBE) execAggFinal(instr *Instruction) error {
	// Execute aggregate finalization function
	// P1 = cursor (for grouping context)
	// P2 = output register
	// P3 = aggregate function index
	return v.opAggFinal(instr.P1, instr.P2, instr.P3)
}

// Function call implementation

func (v *VDBE) execFunction(instr *Instruction) error {
	// Call function with arguments in registers P2...P2+P5-1, store result in P3
	// P1 = constant mask (bit flags for which args are constant)
	// P2 = first argument register
	// P3 = output register
	// P4 = function name (string)
	// P5 = number of arguments
	return v.opFunction(instr.P1, instr.P2, instr.P3, instr.P1, int(instr.P5))
}

// Transaction operation implementations

func (v *VDBE) execTransaction(instr *Instruction) error {
	// Begin a transaction
	// P1 = database index (0 for main)
	// P2 = write flag (0 = read-only, 1 = read-write)
	// P3 = schema version (for verification)
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.PagerWriter)
	if !ok {
		return fmt.Errorf("pager does not implement types.PagerWriter")
	}

	// P2 != 0 means write transaction
	if instr.P2 != 0 {
		return pager.BeginWrite()
	}
	return pager.BeginRead()
}

func (v *VDBE) execCommit(instr *Instruction) error {
	// Commit the current transaction
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.PagerWriter)
	if !ok {
		return fmt.Errorf("pager does not implement types.PagerWriter")
	}

	// Check if we're in a write transaction
	if pager.InWriteTransaction() {
		return pager.Commit()
	}

	// If we're in a read transaction, end it
	if pager.InTransaction() {
		return pager.EndRead()
	}

	return nil
}

func (v *VDBE) execRollback(instr *Instruction) error {
	// Rollback the current transaction
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.PagerWriter)
	if !ok {
		return fmt.Errorf("pager does not implement types.PagerWriter")
	}

	// Check if we're in a write transaction
	if pager.InWriteTransaction() {
		return pager.Rollback()
	}

	// If we're in a read transaction, end it
	if pager.InTransaction() {
		return pager.EndRead()
	}

	return nil
}

func (v *VDBE) execAutoCommit(instr *Instruction) error {
	// Set autocommit mode or begin/commit transaction
	// P1: 1 for commit/enable autocommit, 0 for begin/disable autocommit
	// P2: rollback flag - if non-zero and P1=1, do rollback instead of commit
	pager, err := v.getAutoCommitPager()
	if err != nil {
		return err
	}

	if instr.P1 == 0 {
		return v.execBeginTransaction(pager)
	}
	return v.execCommitOrRollback(pager, instr.P2)
}

// getAutoCommitPager validates and returns the pager for autocommit operations
func (v *VDBE) getAutoCommitPager() (types.PagerWriter, error) {
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return nil, fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.PagerWriter)
	if !ok {
		return nil, fmt.Errorf("pager does not implement types.PagerWriter")
	}

	return pager, nil
}

// execBeginTransaction begins a write transaction if not already in one
func (v *VDBE) execBeginTransaction(pager types.PagerWriter) error {
	// P1=0: Begin transaction (disable autocommit)
	// Only start a transaction if not already in one
	if !pager.InTransaction() {
		return pager.BeginWrite()
	}
	return nil
}

// execCommitOrRollback commits or rolls back the current transaction
func (v *VDBE) execCommitOrRollback(pager types.PagerWriter, rollbackFlag int) error {
	// P1=1: Commit or rollback transaction (enable autocommit)
	if pager.InWriteTransaction() {
		if rollbackFlag != 0 {
			// P2 non-zero: rollback
			return pager.Rollback()
		}
		// P2 zero: commit
		return pager.Commit()
	}
	if pager.InTransaction() {
		return pager.EndRead()
	}
	return nil
}

func (v *VDBE) execSavepoint(instr *Instruction) error {
	// Create, release, or rollback to a savepoint
	// P1: operation (0=begin, 1=release, 2=rollback)
	// P4: savepoint name (string)
	pager, err := v.getSavepointPager()
	if err != nil {
		return err
	}

	name, err := v.getSavepointName(instr)
	if err != nil {
		return err
	}

	return v.executeSavepointOperation(pager, instr.P1, name)
}

// getSavepointPager gets the pager that supports savepoints
func (v *VDBE) getSavepointPager() (types.SavepointPager, error) {
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return nil, fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.SavepointPager)
	if !ok {
		return nil, fmt.Errorf("pager does not implement types.SavepointPager")
	}

	return pager, nil
}

// getSavepointName extracts and validates the savepoint name from instruction
func (v *VDBE) getSavepointName(instr *Instruction) (string, error) {
	if instr.P4Type != P4Static && instr.P4Type != P4Dynamic {
		return "", fmt.Errorf("savepoint name must be in P4 as string")
	}

	name := instr.P4.Z
	if name == "" {
		return "", fmt.Errorf("savepoint name cannot be empty")
	}

	return name, nil
}

// executeSavepointOperation executes the savepoint operation
func (v *VDBE) executeSavepointOperation(pager types.SavepointPager, operation int, name string) error {
	switch operation {
	case 0:
		return pager.Savepoint(name)
	case 1:
		return pager.Release(name)
	case 2:
		return pager.RollbackTo(name)
	default:
		return fmt.Errorf("invalid savepoint operation: %d", operation)
	}
}

func (v *VDBE) execVerifyCookie(instr *Instruction) error {
	// Verify that the schema cookie matches the expected value
	// P1: database index (0 for main)
	// P2: cookie type (typically 0 for schema cookie)
	// P3: expected cookie value
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.CookiePager)
	if !ok {
		return fmt.Errorf("pager does not implement types.CookiePager")
	}

	// Get the current cookie value
	currentValue, err := pager.GetCookie(instr.P1, instr.P2)
	if err != nil {
		return err
	}

	// Verify it matches the expected value
	expectedValue := uint32(instr.P3)
	if currentValue != expectedValue {
		return fmt.Errorf("schema changed: expected cookie %d, got %d", expectedValue, currentValue)
	}

	return nil
}

func (v *VDBE) execSetCookie(instr *Instruction) error {
	// Set a database cookie value
	// P1: database index (0 for main)
	// P2: cookie type
	// P3: new cookie value
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(types.CookiePager)
	if !ok {
		return fmt.Errorf("pager does not implement types.CookiePager")
	}

	// Set the cookie value
	return pager.SetCookie(instr.P1, instr.P2, uint32(instr.P3))
}

// Sorter opcode implementations

// execSorterOpen opens a new sorter.
// P1 = sorter cursor number
// P2 = number of columns in the rows
// P4.P = pointer to key info (SorterKeyInfo)
func (v *VDBE) execSorterOpen(instr *Instruction) error {
	sorterNum := instr.P1
	numCols := instr.P2

	// Ensure we have enough sorter slots
	for len(v.Sorters) <= sorterNum {
		v.Sorters = append(v.Sorters, nil)
	}

	// Get key columns, desc, and collations info from P4.P
	var keyCols []int
	var desc []bool
	var collations []string
	if instr.P4.P != nil {
		if keyInfo, ok := instr.P4.P.(*SorterKeyInfo); ok {
			keyCols = keyInfo.KeyCols
			desc = keyInfo.Desc
			collations = keyInfo.Collations
		}
	}

	// Get collation registry from VDBE context
	var collRegistry interface{}
	if v.Ctx != nil {
		collRegistry = v.Ctx.CollationRegistry
	}

	v.Sorters[sorterNum] = NewSorterWithRegistry(keyCols, desc, collations, numCols, collRegistry)
	return nil
}

// SorterKeyInfo holds key column information for sorting.
type SorterKeyInfo struct {
	KeyCols    []int    // Column indices for sorting
	Desc       []bool   // True for descending order
	Collations []string // Collation name for each key column (empty string for default)
}

// execSorterInsert inserts a row into the sorter.
// P1 = sorter cursor number
// P2 = start register containing the row data
// P3 = number of columns
func (v *VDBE) execSorterInsert(instr *Instruction) error {
	sorterNum := instr.P1
	startReg := instr.P2
	numCols := instr.P3

	if sorterNum >= len(v.Sorters) || v.Sorters[sorterNum] == nil {
		return fmt.Errorf("sorter %d not open", sorterNum)
	}

	// Collect the row data from registers
	row := make([]*Mem, numCols)
	for i := 0; i < numCols; i++ {
		mem, err := v.GetMem(startReg + i)
		if err != nil {
			return err
		}
		row[i] = mem
	}

	v.Sorters[sorterNum].Insert(row)

	// Track sorter memory usage (rough estimate)
	if v.Stats != nil {
		sorter := v.Sorters[sorterNum]
		// Estimate: each row is roughly numCols * 64 bytes
		memBytes := int64(len(sorter.Rows) * numCols * 64)
		v.Stats.UpdateSorterMemory(memBytes)
	}

	return nil
}

// execSorterSort sorts the collected rows and rewinds to the start.
// P1 = sorter cursor number
// P2 = jump address if sorter is empty (no rows)
func (v *VDBE) execSorterSort(instr *Instruction) error {
	sorterNum := instr.P1

	if sorterNum >= len(v.Sorters) || v.Sorters[sorterNum] == nil {
		return fmt.Errorf("sorter %d not open", sorterNum)
	}

	sorter := v.Sorters[sorterNum]
	sorter.Sort()

	// Rewind to position before first row (Next will move to first)
	sorter.Current = -1

	// Jump if empty
	if len(sorter.Rows) == 0 && instr.P2 > 0 {
		v.PC = instr.P2
	}
	return nil
}

// execSorterNext advances to the next row in the sorted results.
// P1 = sorter cursor number
// P2 = jump address if more rows available
func (v *VDBE) execSorterNext(instr *Instruction) error {
	sorterNum := instr.P1

	if sorterNum >= len(v.Sorters) || v.Sorters[sorterNum] == nil {
		return fmt.Errorf("sorter %d not open", sorterNum)
	}

	if v.Sorters[sorterNum].Next() {
		v.PC = instr.P2
	}
	return nil
}

// execSorterData copies the current sorted row to registers.
// P1 = sorter cursor number
// P2 = destination start register
// P3 = number of columns to copy
func (v *VDBE) execSorterData(instr *Instruction) error {
	sorterNum := instr.P1
	destReg := instr.P2
	numCols := instr.P3

	if sorterNum >= len(v.Sorters) || v.Sorters[sorterNum] == nil {
		return fmt.Errorf("sorter %d not open", sorterNum)
	}

	row := v.Sorters[sorterNum].CurrentRow()
	if row == nil {
		return fmt.Errorf("sorter %d has no current row", sorterNum)
	}

	// Copy row data to destination registers
	for i := 0; i < numCols && i < len(row); i++ {
		mem, err := v.GetMem(destReg + i)
		if err != nil {
			return err
		}
		mem.Copy(row[i])
	}

	return nil
}

// execSorterClose closes a sorter and frees its resources.
// P1 = sorter cursor number
func (v *VDBE) execSorterClose(instr *Instruction) error {
	sorterNum := instr.P1

	if sorterNum < len(v.Sorters) && v.Sorters[sorterNum] != nil {
		// Return pooled memory to the pool
		v.Sorters[sorterNum].Close()
		v.Sorters[sorterNum] = nil
	}
	return nil
}

// Type conversion opcode implementations

// affinityFunc is a function that applies an affinity conversion to a memory value.
type affinityFunc func(*Mem) error

// affinityHandlers maps affinity codes to their conversion functions.
var affinityHandlers = map[int]affinityFunc{
	0: func(mem *Mem) error { return nil }, // NONE/BLOB affinity - keep as-is
	1: castToBlob,                          // BLOB affinity - convert to blob
	2: (*Mem).Stringify,                    // TEXT affinity - convert to text
	3: castToInteger,                       // INTEGER affinity - convert to integer
	4: (*Mem).Realify,                      // REAL affinity - convert to real
	5: castToNumeric,                       // NUMERIC affinity - try int then float
}

// execCast converts the value in register P1 to the type specified by P2 affinity.
// P1 = register containing value to convert
// P2 = affinity code (NONE=0, BLOB=1, TEXT=2, INTEGER=3, REAL=4, NUMERIC=5)
//
// SQLite affinity codes:
// - 0 (NONE/BLOB): Keep as-is
// - 1 (BLOB): Convert to blob
// - 2 (TEXT): Convert to text
// - 3 (INTEGER): Convert to integer, NULL if not numeric
// - 4 (REAL): Convert to real
// - 5 (NUMERIC): Try int, then float, keep text if neither works
func (v *VDBE) execCast(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// NULL values remain NULL regardless of affinity
	if mem.IsNull() {
		return nil
	}

	// Look up the affinity handler
	handler, ok := affinityHandlers[instr.P2]
	if !ok {
		return fmt.Errorf("unknown affinity code: %d", instr.P2)
	}

	return handler(mem)
}

// castToBlob converts a memory value to blob affinity.
func castToBlob(mem *Mem) error {
	if mem.IsBlob() {
		return nil
	}
	// Convert to blob by getting string representation and treating as bytes
	if mem.IsString() {
		return mem.SetBlob(mem.BlobValue())
	}
	// For numeric types, stringify first then convert to blob
	if err := mem.Stringify(); err != nil {
		return err
	}
	return mem.SetBlob(mem.BlobValue())
}

// castToInteger converts a memory value to integer affinity, NULL if not numeric.
// castIntegerFromReal converts real to integer if no fractional part.
func castIntegerFromReal(mem *Mem) {
	if mem.r == float64(int64(mem.r)) {
		mem.SetInt(int64(mem.r))
	}
	// Otherwise keep as real (SQLite behavior for INTEGER affinity)
}

// castIntegerFromStringOrBlob implements SQLite INTEGER affinity for strings/blobs.
// SQLite behavior:
// - Strings that parse as integers → integer
// - Strings that parse as reals with fractional part → real (not truncated)
// - Non-numeric strings → keep as text (not NULL)
func castIntegerFromStringOrBlob(mem *Mem) {
	str := string(mem.z)
	// Try to parse as integer first
	if val, err := strconv.ParseInt(str, 10, 64); err == nil {
		mem.SetInt(val)
		return
	}
	// Try to parse as real - if it has a fractional part, keep as real
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		if val == float64(int64(val)) {
			// Whole number like "5.0" - store as integer
			mem.SetInt(int64(val))
		} else {
			// Fractional like "5.1" - store as real
			mem.SetReal(val)
		}
		return
	}
	// Not numeric - keep as text (SQLite INTEGER affinity doesn't nullify text)
}

func castToInteger(mem *Mem) error {
	if mem.IsInt() {
		return nil
	}
	if mem.IsReal() {
		castIntegerFromReal(mem)
		return nil
	}
	if mem.IsString() || mem.IsBlob() {
		castIntegerFromStringOrBlob(mem)
		return nil
	}
	// Can't convert to integer - set to NULL
	mem.SetNull()
	return nil
}

// setFloatOrInt sets a float value as int if it has no fractional part, otherwise as real.
func setFloatOrInt(mem *Mem, val float64) {
	if val == float64(int64(val)) {
		mem.SetInt(int64(val))
	} else {
		mem.SetReal(val)
	}
}

// parseNumericFromString tries to parse string/blob as int then float.
// Returns true if successful parsing occurred.
func parseNumericFromString(mem *Mem, str string) bool {
	// Try integer first
	if val, err := strconv.ParseInt(str, 10, 64); err == nil {
		mem.SetInt(val)
		return true
	}
	// Try real
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		setFloatOrInt(mem, val)
		return true
	}
	return false
}

// castToNumeric tries int, then float, keeps text if neither works.
func castToNumeric(mem *Mem) error {
	// If already numeric, check if real can be converted to integer
	if mem.IsReal() {
		// Convert real to integer if it has no fractional part
		setFloatOrInt(mem, mem.r)
		return nil
	}
	if mem.IsInt() {
		return nil
	}
	if mem.IsString() || mem.IsBlob() {
		parseNumericFromString(mem, mem.StrValue())
		// Keep as text if parsing failed
		return nil
	}
	return nil
}

// execToText forces the value in register P1 to text type.
// P1 = register to convert
// This always converts to string representation.
func (v *VDBE) execToText(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// NULL remains NULL
	if mem.IsNull() {
		return nil
	}

	// Convert to string
	return mem.Stringify()
}

// execToBlob forces the value in register P1 to blob type.
// P1 = register to convert
// Converts the value to a blob (byte array).
func (v *VDBE) execToBlob(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// NULL remains NULL
	if mem.IsNull() {
		return nil
	}

	// If already a blob, nothing to do
	if mem.IsBlob() {
		return nil
	}

	// If string, convert to blob
	if mem.IsString() {
		return mem.SetBlob(mem.BlobValue())
	}

	// For numeric types, stringify first then convert to blob
	if err := mem.Stringify(); err != nil {
		return err
	}
	return mem.SetBlob(mem.BlobValue())
}

// execToNumeric applies numeric affinity to the value in register P1.
// P1 = register to convert
// Tries to convert to integer first, then real, keeps as text if neither works.
func (v *VDBE) execToNumeric(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	if mem.IsNull() || mem.IsNumeric() {
		return nil
	}

	v.convertMemToNumeric(mem)
	return nil
}

// convertMemToNumeric attempts to convert a memory value to numeric.
func (v *VDBE) convertMemToNumeric(mem *Mem) {
	if !mem.IsString() && !mem.IsBlob() {
		return
	}

	str := mem.StrValue()
	if tryConvertToInt(mem, str) {
		return
	}
	tryConvertToReal(mem, str)
}

// tryConvertToInt attempts to convert a string to integer, returns true if successful.
func tryConvertToInt(mem *Mem, str string) bool {
	if val, err := strconv.ParseInt(str, 10, 64); err == nil {
		mem.SetInt(val)
		return true
	}
	return false
}

// tryConvertToReal attempts to convert a string to real.
func tryConvertToReal(mem *Mem, str string) {
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		mem.SetReal(val)
	}
}

// convertStringToInt tries to parse string as integer, returns 0 if it fails.
func convertStringToInt(str string) int64 {
	// Try parsing as integer directly
	if val, err := strconv.ParseInt(str, 10, 64); err == nil {
		return val
	}
	// Try parsing as float and truncating
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		return int64(val)
	}
	// Can't convert - return 0
	return 0
}

// execToInt forces the value in register P1 to integer type.
// P1 = register to convert
// Truncates real numbers to integers.
func (v *VDBE) execToInt(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// NULL remains NULL
	if mem.IsNull() {
		return nil
	}

	// Already an integer, nothing to do
	if mem.IsInt() {
		return nil
	}

	// Convert to integer, truncating if real
	if mem.IsReal() {
		mem.SetInt(int64(mem.RealValue()))
		return nil
	}

	// Try to parse string/blob as integer, default to 0
	if mem.IsString() || mem.IsBlob() {
		mem.SetInt(convertStringToInt(mem.StrValue()))
		return nil
	}

	// Default to 0 for unknown types
	mem.SetInt(0)
	return nil
}

// execToReal forces the value in register P1 to real (floating-point) type.
// P1 = register to convert
// Converts integers and strings to real numbers.
func (v *VDBE) execToReal(instr *Instruction) error {
	mem, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	// NULL remains NULL
	if mem.IsNull() {
		return nil
	}

	// Already a real, nothing to do
	if mem.IsReal() {
		return nil
	}

	// Convert to real
	return mem.Realify()
}

// Bitwise operation implementations

func (v *VDBE) execBitAnd(instr *Instruction) error {
	// P3 = P1 & P2 (bitwise AND)
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// NULL propagation: if either operand is NULL, result is NULL
	if left.IsNull() || right.IsNull() {
		result.SetNull()
		return nil
	}

	// Convert both operands to integers
	leftVal := left.IntValue()
	rightVal := right.IntValue()

	// Perform bitwise AND
	result.SetInt(leftVal & rightVal)
	return nil
}

func (v *VDBE) execBitOr(instr *Instruction) error {
	// P3 = P1 | P2 (bitwise OR)
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// NULL propagation: if either operand is NULL, result is NULL
	if left.IsNull() || right.IsNull() {
		result.SetNull()
		return nil
	}

	// Convert both operands to integers
	leftVal := left.IntValue()
	rightVal := right.IntValue()

	// Perform bitwise OR
	result.SetInt(leftVal | rightVal)
	return nil
}

func (v *VDBE) execBitNot(instr *Instruction) error {
	// P2 = ~P1 (bitwise NOT)
	src, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// NULL propagation: if operand is NULL, result is NULL
	if src.IsNull() {
		dst.SetNull()
		return nil
	}

	// Convert operand to integer
	srcVal := src.IntValue()

	// Perform bitwise NOT
	dst.SetInt(^srcVal)
	return nil
}

func (v *VDBE) execShiftLeft(instr *Instruction) error {
	shiftAmount, value, result, err := v.getShiftOperands(instr)
	if err != nil {
		return err
	}

	if shiftAmount.IsNull() || value.IsNull() {
		result.SetNull()
		return nil
	}

	shift := shiftAmount.IntValue()
	val := value.IntValue()
	result.SetInt(computeLeftShift(shift, val))
	return nil
}

// getShiftOperands retrieves the three memory operands for shift operations.
func (v *VDBE) getShiftOperands(instr *Instruction) (*Mem, *Mem, *Mem, error) {
	shiftAmount, err := v.GetMem(instr.P1)
	if err != nil {
		return nil, nil, nil, err
	}

	value, err := v.GetMem(instr.P2)
	if err != nil {
		return nil, nil, nil, err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return nil, nil, nil, err
	}

	return shiftAmount, value, result, nil
}

// computeLeftShift computes left shift with SQLite semantics.
func computeLeftShift(shift, val int64) int64 {
	if shift < 0 || shift >= 64 {
		return 0
	}
	return val << uint(shift)
}

// computeShiftRight computes right shift with SQLite semantics.
func computeShiftRight(shift, val int64) int64 {
	// SQLite behavior: negative shift amounts result in 0
	if shift < 0 {
		return 0
	}
	// Shifts >= 64: sign-extend for negative values, 0 otherwise
	if shift >= 64 {
		if val < 0 {
			return -1
		}
		return 0
	}
	// Perform right shift (arithmetic shift in Go for signed integers)
	return val >> uint(shift)
}

func (v *VDBE) execShiftRight(instr *Instruction) error {
	// P3 = P2 >> P1
	// Note: P1 is shift amount, P2 is value to shift
	shiftAmount, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	value, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// NULL propagation: if either operand is NULL, result is NULL
	if shiftAmount.IsNull() || value.IsNull() {
		result.SetNull()
		return nil
	}

	// Convert operands to integers and perform shift
	shift := shiftAmount.IntValue()
	val := value.IntValue()
	result.SetInt(computeShiftRight(shift, val))
	return nil
}

// Logical operation implementations

func (v *VDBE) execAnd(instr *Instruction) error {
	// P3 = P1 AND P2 (logical AND, returns 0/1/NULL)
	// SQLite semantics:
	// - FALSE AND anything = FALSE (0)
	// - TRUE AND TRUE = TRUE (1)
	// - TRUE AND FALSE = FALSE (0)
	// - NULL AND FALSE = FALSE (0)
	// - NULL AND TRUE = NULL
	// - NULL AND NULL = NULL
	left, right, result, err := v.getLogicalOperands(instr)
	if err != nil {
		return err
	}

	leftIsNull, leftBool := evalMemAsBool(left)
	rightIsNull, rightBool := evalMemAsBool(right)

	v.setLogicalAndResult(result, leftIsNull, leftBool, rightIsNull, rightBool)
	return nil
}

// getLogicalOperands retrieves the operands and result for logical operations
func (v *VDBE) getLogicalOperands(instr *Instruction) (*Mem, *Mem, *Mem, error) {
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return nil, nil, nil, err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return nil, nil, nil, err
	}

	result, err := v.GetMem(instr.P3)
	if err != nil {
		return nil, nil, nil, err
	}

	return left, right, result, nil
}

// setLogicalAndResult sets the result of a logical AND operation
func (v *VDBE) setLogicalAndResult(result *Mem, leftIsNull, leftBool, rightIsNull, rightBool bool) {
	if !leftIsNull && !leftBool {
		result.SetInt(0)
		return
	}

	if !rightIsNull && !rightBool {
		result.SetInt(0)
		return
	}

	if leftIsNull || rightIsNull {
		result.SetNull()
		return
	}

	result.SetInt(1)
}

func (v *VDBE) execOr(instr *Instruction) error {
	// P3 = P1 OR P2 (logical OR, returns 0/1/NULL)
	// SQLite semantics:
	// - TRUE OR anything = TRUE (1)
	// - FALSE OR FALSE = FALSE (0)
	// - FALSE OR TRUE = TRUE (1)
	// - NULL OR TRUE = TRUE (1)
	// - NULL OR FALSE = NULL
	// - NULL OR NULL = NULL
	left, right, result, err := v.getLogicalOperands(instr)
	if err != nil {
		return err
	}

	leftIsNull, leftBool := evalMemAsBool(left)
	rightIsNull, rightBool := evalMemAsBool(right)

	v.setLogicalOrResult(result, leftIsNull, leftBool, rightIsNull, rightBool)
	return nil
}

// setLogicalOrResult sets the result of a logical OR operation
func (v *VDBE) setLogicalOrResult(result *Mem, leftIsNull, leftBool, rightIsNull, rightBool bool) {
	if !leftIsNull && leftBool {
		result.SetInt(1)
		return
	}

	if !rightIsNull && rightBool {
		result.SetInt(1)
		return
	}

	if leftIsNull || rightIsNull {
		result.SetNull()
		return
	}

	result.SetInt(0)
}

// evalMemAsBool evaluates a memory value as a boolean.
// Returns (isNull, boolValue).
func evalMemAsBool(mem *Mem) (bool, bool) {
	if mem.IsNull() {
		return true, false
	}
	if mem.IsInt() {
		return false, mem.IntValue() != 0
	}
	return false, mem.RealValue() != 0.0
}

func (v *VDBE) execNot(instr *Instruction) error {
	// P2 = NOT P1 (logical NOT)
	// SQLite semantics:
	// - NOT TRUE = FALSE (0)
	// - NOT FALSE = TRUE (1)
	// - NOT NULL = NULL
	src, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// NULL propagation
	if src.IsNull() {
		dst.SetNull()
		return nil
	}

	// Evaluate as boolean
	var srcBool bool
	if src.IsInt() {
		srcBool = src.IntValue() != 0
	} else {
		srcBool = src.RealValue() != 0.0
	}

	// Apply logical NOT
	if srcBool {
		dst.SetInt(0) // NOT TRUE = FALSE
	} else {
		dst.SetInt(1) // NOT FALSE = TRUE
	}

	return nil
}

// Index operation implementations

// execIdxInsert inserts a key into an index.
// P1 = cursor number (index cursor)
// P2 = register containing the key
// P3 = register containing the rowid (data)
func (v *VDBE) execIdxInsert(instr *Instruction) error {
	// Get and verify the writable index cursor
	_, idxCursor, err := v.getWritableIndexCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get the key from register P2
	keyMem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// Get the rowid from register P3
	rowidMem, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Extract key as blob (index keys are stored as binary data)
	key := extractKeyAsBlob(keyMem)

	// Extract rowid as integer
	rowid := rowidMem.IntValue()

	// Insert the key-rowid pair into the index
	if err := idxCursor.InsertIndex(key, rowid); err != nil {
		return fmt.Errorf("index insert failed: %w", err)
	}

	// Invalidate cursor cache
	v.IncrCacheCtr()

	return nil
}

// execIdxDelete deletes a key from an index.
// P1 = cursor number (index cursor)
// P2 = register containing the key to delete
func (v *VDBE) execIdxDelete(instr *Instruction) error {
	// Get and verify the index cursor
	_, idxCursor, err := v.getWritableIndexCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get the key from register P2
	keyMem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// Extract key as blob
	key := extractKeyAsBlob(keyMem)

	// Seek and delete the index entry
	if err := seekAndDeleteIndexEntry(idxCursor, key); err != nil {
		return err
	}

	// Invalidate cursor cache
	v.IncrCacheCtr()

	return nil
}

// getWritableIndexCursor retrieves and validates a writable index cursor.
func (v *VDBE) getWritableIndexCursor(cursorNum int) (*Cursor, *btree.IndexCursor, error) {
	cursor, idxCursor, err := v.getIndexCursor(cursorNum)
	if err != nil {
		return nil, nil, err
	}

	// Verify cursor is writable
	if !cursor.Writable {
		return nil, nil, fmt.Errorf("cursor %d is not writable (opened with OpenRead instead of OpenWrite)", cursorNum)
	}

	return cursor, idxCursor, nil
}

// seekAndDeleteIndexEntry seeks to a key in an index and deletes it.
func seekAndDeleteIndexEntry(idxCursor *btree.IndexCursor, key []byte) error {
	// For index deletion, we need both key and rowid to uniquely identify the entry
	// In real SQLite, the rowid would be encoded as part of the key or provided separately
	// For now, we'll seek to the key and delete the current entry
	found, err := idxCursor.SeekIndex(key)
	if err != nil {
		return fmt.Errorf("index seek failed: %w", err)
	}

	if !found {
		// Key not found - this is not necessarily an error in SQLite
		// It might have already been deleted
		return nil
	}

	// Get the rowid from the current position
	rowid := idxCursor.GetRowid()

	// Delete the key-rowid pair
	if err := idxCursor.DeleteIndex(key, rowid); err != nil {
		return fmt.Errorf("index delete failed: %w", err)
	}

	return nil
}

// execIdxRowid gets the rowid from the current index position.
// P1 = cursor number (index cursor)
// P2 = destination register for rowid
func (v *VDBE) execIdxRowid(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	if cursor.IsTable {
		return fmt.Errorf("cursor %d is not an index cursor", instr.P1)
	}

	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	v.extractIndexRowid(cursor, dst)
	return nil
}

// extractIndexRowid extracts the rowid from an index cursor.
func (v *VDBE) extractIndexRowid(cursor *Cursor, dst *Mem) {
	if cursor.EOF || cursor.NullRow {
		dst.SetNull()
		return
	}

	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		dst.SetNull()
		return
	}

	rowid := idxCursor.GetRowid()
	dst.SetInt(rowid)
}

// execIdxLT jumps if the index key at cursor < key in register.
// P1 = cursor number (index cursor)
// P2 = jump address if condition is true
// P3 = register containing comparison key
func (v *VDBE) execIdxLT(instr *Instruction) error {
	return v.execIdxCompare(instr, func(cmp int) bool { return cmp < 0 })
}

// execIdxGT jumps if the index key at cursor > key in register.
// P1 = cursor number (index cursor)
// P2 = jump address if condition is true
// P3 = register containing comparison key
func (v *VDBE) execIdxGT(instr *Instruction) error {
	return v.execIdxCompare(instr, func(cmp int) bool { return cmp > 0 })
}

// execIdxLE jumps if the index key at cursor <= key in register.
// P1 = cursor number (index cursor)
// P2 = jump address if condition is true
// P3 = register containing comparison key
func (v *VDBE) execIdxLE(instr *Instruction) error {
	return v.execIdxCompare(instr, func(cmp int) bool { return cmp <= 0 })
}

// execIdxGE jumps if the index key at cursor >= key in register.
// P1 = cursor number (index cursor)
// P2 = jump address if condition is true
// P3 = register containing comparison key
func (v *VDBE) execIdxGE(instr *Instruction) error {
	return v.execIdxCompare(instr, func(cmp int) bool { return cmp >= 0 })
}

// execIdxCompare is a helper function for index comparison operations.
// It compares the key at the current cursor position with a key in a register.
func (v *VDBE) execIdxCompare(instr *Instruction, test func(int) bool) error {
	// Get and verify the index cursor
	cursor, idxCursor, err := v.getIndexCursor(instr.P1)
	if err != nil {
		return err
	}

	// Get the comparison key from register P3
	keyMem, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Extract comparison key as blob
	compKey := extractKeyAsBlob(keyMem)

	// Check if cursor is at a valid position
	if !idxCursor.IsValid() || cursor.EOF {
		// Cursor is not at a valid position - don't jump
		return nil
	}

	// Get the current key from the index cursor
	currentKey := idxCursor.GetKey()
	if currentKey == nil {
		// No current key - don't jump
		return nil
	}

	// Compare the keys (byte-wise comparison)
	cmp := compareBytes(currentKey, compKey)

	// Jump if the test condition is satisfied
	if test(cmp) {
		v.PC = instr.P2
	}

	return nil
}

// getIndexCursor retrieves and validates an index cursor.
func (v *VDBE) getIndexCursor(cursorNum int) (*Cursor, *btree.IndexCursor, error) {
	cursor, err := v.GetCursor(cursorNum)
	if err != nil {
		return nil, nil, err
	}

	// Verify this is an index cursor
	if cursor.IsTable {
		return nil, nil, fmt.Errorf("cursor %d is not an index cursor", cursorNum)
	}

	// Get the index cursor (btree.IndexCursor)
	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		return nil, nil, fmt.Errorf("invalid index cursor for index operation")
	}

	return cursor, idxCursor, nil
}

// extractKeyAsBlob extracts a memory value as a blob suitable for index operations.
func extractKeyAsBlob(keyMem *Mem) []byte {
	if keyMem.IsBlob() {
		return keyMem.BlobValue()
	}
	if keyMem.IsString() {
		return []byte(keyMem.StrValue())
	}
	keyMem.Stringify()
	return []byte(keyMem.StrValue())
}

// encodeValueAsRecord encodes a single value as a SQLite record
// This is used for index seeks where the search key needs to be in record format
func encodeValueAsRecord(mem *Mem) []byte {
	// Convert Mem to interface{} and encode as a single-column record
	val := memToInterface(mem)
	return encodeSimpleRecord([]interface{}{val})
}

// compareBytes compares two byte slices lexicographically.
// Returns negative if a < b, positive if a > b, 0 if equal.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	// If all compared bytes are equal, compare lengths
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// Trigger and sub-program implementations

// execProgram executes a trigger sub-program.
// P1 = sub-program ID
// P2 = jump address on completion
// P4 = sub-VDBE program (P4SubProgram)
func (v *VDBE) execProgram(instr *Instruction) error {
	// Get or create the sub-program VDBE
	subID := instr.P1
	subVdbe, ok := v.SubPrograms[subID]
	if !ok {
		// Extract sub-program from P4
		if instr.P4Type != P4SubProgram {
			return fmt.Errorf("OpProgram requires P4_SUBPROGRAM type")
		}
		if instr.P4.P == nil {
			return fmt.Errorf("OpProgram: sub-program is nil")
		}

		// The sub-program should be a *VDBE
		var subProg *VDBE
		subProg, ok = instr.P4.P.(*VDBE)
		if !ok {
			return fmt.Errorf("OpProgram: P4 is not a *VDBE")
		}

		// Set up parent relationship
		subProg.Parent = v
		subProg.Ctx = v.Ctx // Share execution context

		v.SubPrograms[subID] = subProg
		subVdbe = subProg
	}

	// Execute the sub-program until completion
	err := subVdbe.Run()
	if err != nil {
		return fmt.Errorf("sub-program execution failed: %w", err)
	}

	// Reset the sub-program for next execution
	subVdbe.Reset()

	return nil
}

// execParam gets a parameter from the parent VDBE.
// P1 = parameter index in parent's register space
// P2 = destination register in this VDBE
func (v *VDBE) execParam(instr *Instruction) error {
	// Check if we have a parent VDBE
	if v.Parent == nil {
		return fmt.Errorf("OpParam: no parent VDBE available")
	}

	// Get the parameter from parent's register
	parentReg, err := v.Parent.GetMem(instr.P1)
	if err != nil {
		return fmt.Errorf("OpParam: failed to get parent register %d: %w", instr.P1, err)
	}

	// Copy to our destination register
	destReg, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	return destReg.Copy(parentReg)
}

// execInitCoroutine initializes a coroutine.
// P1 = coroutine ID
// P2 = jump address to skip coroutine body initially
// P3 = entry point address for the coroutine
func (v *VDBE) execInitCoroutine(instr *Instruction) error {
	coroutineID := instr.P1
	jumpAddr := instr.P2
	entryPoint := instr.P3

	// Create coroutine info
	v.Coroutines[coroutineID] = &CoroutineInfo{
		EntryPoint: entryPoint,
		YieldAddr:  0,
		Active:     false,
	}

	// Jump past the coroutine body (don't execute it during init)
	if jumpAddr > 0 {
		v.PC = jumpAddr
	}

	return nil
}

// execEndCoroutine ends coroutine execution and returns to the caller.
// P1 = coroutine ID
func (v *VDBE) execEndCoroutine(instr *Instruction) error {
	coroutineID := instr.P1

	// Get coroutine info
	coInfo, ok := v.Coroutines[coroutineID]
	if !ok {
		return fmt.Errorf("OpEndCoroutine: coroutine %d not initialized", coroutineID)
	}

	if !coInfo.Active {
		return fmt.Errorf("OpEndCoroutine: coroutine %d is not active", coroutineID)
	}

	// Return to the yield address
	if coInfo.YieldAddr > 0 {
		v.PC = coInfo.YieldAddr
	}

	// Mark as inactive
	coInfo.Active = false

	return nil
}

// execYield yields from a coroutine, saving the return address.
// P1 = coroutine ID
// P2 = register containing return address (or 0 to use PC)
func (v *VDBE) execYield(instr *Instruction) error {
	coroutineID := instr.P1

	// Get coroutine info
	coInfo, ok := v.Coroutines[coroutineID]
	if !ok {
		return fmt.Errorf("OpYield: coroutine %d not initialized", coroutineID)
	}

	// If P2 is specified, get return address from register
	// Otherwise, use current PC as return address
	returnAddr := v.PC
	if instr.P2 > 0 {
		regMem, err := v.GetMem(instr.P2)
		if err != nil {
			return err
		}
		if regMem.IsInt() {
			returnAddr = int(regMem.IntValue())
		}
	}

	// Save return address and jump to entry point
	coInfo.YieldAddr = returnAddr
	coInfo.Active = true
	v.PC = coInfo.EntryPoint

	return nil
}

// execOnce executes a block of code only once.
//
// OP_Once is used to ensure a block of code runs only once per program invocation.
// It uses self-modifying code for top-level programs and a bitmask for subprograms.
//
// P1 = Initial value that is set on OP_Init
// P2 = Address to jump to on subsequent executions
//
// On first execution: fall through to next instruction and set P1 = OP_Init.P1
// On subsequent executions: jump to P2
func (v *VDBE) execOnce(instr *Instruction) error {
	jumpAddr := instr.P2

	// Get the OP_Init instruction (should always be at position 0)
	if len(v.Program) == 0 || v.Program[0].Opcode != OpInit {
		return fmt.Errorf("OpOnce requires OP_Init at position 0")
	}
	initInstr := v.Program[0]

	// For now, we implement the self-modifying code approach for top-level programs.
	// Check if this instruction's P1 matches the Init instruction's P1.
	// If they match, we've already executed this once - jump.
	// If they differ, this is the first execution - fall through and update P1.

	if instr.P1 == initInstr.P1 {
		// Already executed in this invocation - jump to P2
		v.PC = jumpAddr
		return nil
	}

	// First execution - set P1 to match OP_Init.P1 (self-modify)
	instr.P1 = initInstr.P1

	// Fall through to next instruction (execute the once block)
	return nil
}

// execOpenPseudo opens a pseudo-table cursor for accessing OLD/NEW row data in triggers.
// P1 = cursor number
// P2 = register containing the pseudo-table data (record blob)
// P3 = number of columns
func (v *VDBE) execOpenPseudo(instr *Instruction) error {
	cursorNum := instr.P1
	dataReg := instr.P2

	// Allocate cursors if needed
	if err := v.AllocCursors(cursorNum + 1); err != nil {
		return err
	}

	// Create pseudo-table cursor
	cursor := &Cursor{
		CurType:     CursorPseudo,
		IsTable:     true,
		PseudoReg:   dataReg,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
		NullRow:     false,
		EOF:         false,
	}

	v.Cursors[cursorNum] = cursor
	return nil
}

// Virtual table opcode implementations

// execVOpen opens a cursor on a virtual table.
// P1: cursor number
// P2: module name (unused, virtual table is passed via P4)
// P3: unused
// P4: VirtualTable instance (interface{})
func (v *VDBE) execVOpen(instr *Instruction) error {
	cursorIdx := instr.P1

	// Ensure cursor array is large enough
	if err := v.AllocCursors(cursorIdx + 1); err != nil {
		return err
	}

	// Get the virtual table from P4
	if instr.P4Type != P4VTab {
		return fmt.Errorf("OpVOpen: P4 must contain VirtualTable")
	}

	vtable := instr.P4.P
	if vtable == nil {
		return fmt.Errorf("OpVOpen: virtual table is nil")
	}

	// Create a cursor for the virtual table
	// We'll need to import vtab package to use VirtualTable interface
	// For now, store it as interface{} and cast when needed
	cursor := &Cursor{
		CurType: CursorVTab,
		VTable:  vtable,
	}

	v.Cursors[cursorIdx] = cursor
	return nil
}

// execVFilter initializes a virtual table cursor for iteration.
// P1: cursor number
// P2: number of arguments in P4
// P3: idxNum (query plan index)
// P4: idxStr (query plan string)
// P5+: constraint values from registers
func (v *VDBE) execVFilter(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	if cursor.CurType != CursorVTab {
		return fmt.Errorf("OpVFilter: cursor %d is not a virtual table cursor", instr.P1)
	}

	v.ensureVTabCursorOpen(cursor)
	argv := v.buildVFilterArguments(instr)
	cursor.EOF = false
	_ = argv // Suppress unused warning until we implement actual Filter call

	return nil
}

// ensureVTabCursorOpen ensures the virtual table cursor is open.
func (v *VDBE) ensureVTabCursorOpen(cursor *Cursor) {
	if cursor.VTabCursor == nil {
		cursor.VTabCursor = nil // Will be set by actual implementation
	}
}

// buildVFilterArguments builds the argument array for the VFilter operation.
func (v *VDBE) buildVFilterArguments(instr *Instruction) []interface{} {
	argc := instr.P2
	argv := make([]interface{}, argc)

	for i := 0; i < argc; i++ {
		regIdx := int(instr.P5) + i
		if regIdx < len(v.Mem) {
			argv[i] = v.Mem[regIdx].Value()
		}
	}

	return argv
}

// execVColumn reads a column from a virtual table cursor.
// P1: cursor number
// P2: column number
// P3: destination register
func (v *VDBE) execVColumn(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	if cursor.CurType != CursorVTab {
		return fmt.Errorf("OpVColumn: cursor %d is not a virtual table cursor", instr.P1)
	}

	if cursor.VTabCursor == nil {
		return fmt.Errorf("OpVColumn: virtual cursor is not initialized")
	}

	// Get the destination register
	destReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Call Column on the virtual cursor
	// This would be: value, err := cursor.VTabCursor.Column(instr.P2)
	// For now, set a null value
	destReg.SetNull()

	return nil
}

// execVNext advances a virtual table cursor to the next row.
// P1: cursor number
// P2: jump address if not EOF
func (v *VDBE) execVNext(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	if cursor.CurType != CursorVTab {
		return fmt.Errorf("OpVNext: cursor %d is not a virtual table cursor", instr.P1)
	}

	if cursor.VTabCursor == nil {
		return fmt.Errorf("OpVNext: virtual cursor is not initialized")
	}

	// Call Next on the virtual cursor
	// This would be: err := cursor.VTabCursor.Next()
	// Then check EOF: if !cursor.VTabCursor.EOF() { v.PC = instr.P2 }
	// For now, set EOF to true
	cursor.EOF = true

	// If not at EOF, jump to P2
	if !cursor.EOF && instr.P2 > 0 {
		v.PC = instr.P2
	}

	return nil
}

// execVRowid gets the rowid of the current row from a virtual table cursor.
// P1: cursor number
// P2: destination register
func (v *VDBE) execVRowid(instr *Instruction) error {
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	if cursor.CurType != CursorVTab {
		return fmt.Errorf("OpVRowid: cursor %d is not a virtual table cursor", instr.P1)
	}

	if cursor.VTabCursor == nil {
		return fmt.Errorf("OpVRowid: virtual cursor is not initialized")
	}

	// Get the destination register
	destReg, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// Call Rowid on the virtual cursor
	// This would be: rowid, err := cursor.VTabCursor.Rowid()
	// For now, set a default value
	destReg.SetInt(0)

	return nil
}

// Note: OpClose handles closing virtual table cursors as well as regular cursors.
// The existing execClose implementation should check cursor.CurType and call
// cursor.VTabCursor.Close() for virtual table cursors.

// Window function implementations

// execAggStepWindow implements OpAggStepWindow - Step aggregate in window context
// P1 = window state index
// P2 = first argument register
// P3 = aggregate function index (reserved for future use)
// P4 = function name (string)
// P5 = number of arguments
func (v *VDBE) execAggStepWindow(instr *Instruction) error {
	windowIdx := instr.P1
	argReg := instr.P2
	numArgs := int(instr.P5)

	// Get or create window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		// Initialize with default window frame
		windowState = NewWindowState(nil, nil, nil, DefaultWindowFrame())
		v.WindowStates[windowIdx] = windowState
	}

	// Get function name from P4
	funcName := instr.P4.Z
	if funcName == "" {
		return fmt.Errorf("OpAggStepWindow requires function name in P4")
	}

	// Collect arguments
	args, err := v.collectFunctionArgs(argReg, numArgs)
	if err != nil {
		return err
	}

	// Add current row to window state
	windowState.AddRow(args)

	// Execute aggregate step within window context
	// This would integrate with aggregate function state
	return nil
}

// execWindowRowNum implements OpWindowRowNum - ROW_NUMBER() window function
// P1 = window state index
// P2 = output register
func (v *VDBE) execWindowRowNum(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get row number (1-based)
	rowNum := windowState.GetCurrentRowNumber()

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}
	mem.SetInt(rowNum)

	return nil
}

// execWindowRank implements OpWindowRank - RANK() window function
// P1 = window state index
// P2 = output register
func (v *VDBE) execWindowRank(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Update ranking based on current row
	windowState.UpdateRanking()

	// Get rank value
	rank := windowState.GetRank()

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}
	mem.SetInt(rank)

	return nil
}

// execWindowDenseRank implements OpWindowDenseRank - DENSE_RANK() window function
// P1 = window state index
// P2 = output register
func (v *VDBE) execWindowDenseRank(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Update ranking based on current row
	windowState.UpdateRanking()

	// Get dense rank value
	denseRank := windowState.GetDenseRank()

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}
	mem.SetInt(denseRank)

	return nil
}

// execWindowNtile implements OpWindowNtile - NTILE() window function
// P1 = window state index
// P2 = output register
// P3 = number of buckets (from register or immediate)
func (v *VDBE) execWindowNtile(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2
	numBuckets := instr.P3

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get partition size and current row number
	partitionSize := windowState.GetPartitionSize()
	currentRowNum := windowState.GetCurrentRowNumber()

	// Calculate bucket number
	// NTILE divides rows into N buckets as evenly as possible
	if numBuckets <= 0 {
		numBuckets = 1
	}

	bucketSize := int64(partitionSize) / int64(numBuckets)
	remainder := int64(partitionSize) % int64(numBuckets)

	// Rows are distributed: first 'remainder' buckets get (bucketSize+1) rows,
	// remaining buckets get bucketSize rows
	var bucket int64
	if currentRowNum <= remainder*(bucketSize+1) {
		bucket = (currentRowNum-1)/(bucketSize+1) + 1
	} else {
		bucket = (currentRowNum-remainder*(bucketSize+1)-1)/bucketSize + remainder + 1
	}

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}
	mem.SetInt(bucket)

	return nil
}

// execWindowLag implements OpWindowLag - LAG() window function
// P1 = window state index
// P2 = output register
// P3 = column index to retrieve
// P4 = offset (default 1)
// P5 = default value register (if row doesn't exist)
func (v *VDBE) execWindowLag(instr *Instruction) error {
	windowState, err := v.getWindowState(instr.P1)
	if err != nil {
		return err
	}

	offset := v.getWindowOffset(instr)
	lagRow := windowState.GetLagRow(offset)

	return v.setWindowResult(instr.P2, instr.P3, lagRow, instr.P5)
}

// execWindowLead implements OpWindowLead - LEAD() window function
// P1 = window state index
// P2 = output register
// P3 = column index to retrieve
// P4 = offset (default 1)
// P5 = default value register (if row doesn't exist)
func (v *VDBE) execWindowLead(instr *Instruction) error {
	windowState, err := v.getWindowState(instr.P1)
	if err != nil {
		return err
	}

	offset := v.getWindowOffset(instr)
	leadRow := windowState.GetLeadRow(offset)

	return v.setWindowResult(instr.P2, instr.P3, leadRow, instr.P5)
}

// execWindowFirstValue implements OpWindowFirstValue - FIRST_VALUE() window function
// P1 = window state index
// P2 = output register
// P3 = column index to retrieve
func (v *VDBE) execWindowFirstValue(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2
	colIdx := instr.P3

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get first value in frame
	firstValue := windowState.GetFirstValue(colIdx)

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}

	return mem.Copy(firstValue)
}

// execWindowLastValue implements OpWindowLastValue - LAST_VALUE() window function
// P1 = window state index
// P2 = output register
// P3 = column index to retrieve
func (v *VDBE) execWindowLastValue(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2
	colIdx := instr.P3

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get last value in frame
	lastValue := windowState.GetLastValue(colIdx)

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}

	return mem.Copy(lastValue)
}

// getWindowState retrieves a window state by index.
func (v *VDBE) getWindowState(windowIdx int) (*WindowState, error) {
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return nil, fmt.Errorf("window state %d not found", windowIdx)
	}
	return windowState, nil
}

// getWindowOffset extracts the offset from an instruction (default 1).
func (v *VDBE) getWindowOffset(instr *Instruction) int {
	if instr.P4.I > 0 {
		return int(instr.P4.I)
	}
	return 1
}

// setWindowResult stores a window function result, handling NULL/default values.
func (v *VDBE) setWindowResult(outputReg, colIdx int, row []*Mem, defaultReg uint16) error {
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}

	if row == nil || colIdx >= len(row) {
		return v.setWindowDefault(mem, defaultReg)
	}
	return mem.Copy(row[colIdx])
}

// setWindowDefault sets a default value or NULL for window functions.
func (v *VDBE) setWindowDefault(mem *Mem, defaultReg uint16) error {
	if defaultReg > 0 {
		defaultMem, err := v.GetMem(int(defaultReg))
		if err != nil {
			return err
		}
		return mem.Copy(defaultMem)
	}
	mem.SetNull()
	return nil
}

// execAggDistinct implements OpAggDistinct - Check if value is distinct for aggregate
// P1 = input register (value to check)
// P2 = jump address if NOT distinct (already seen)
// P3 = aggregate register (used as key for distinct set)
func (v *VDBE) execAggDistinct(instr *Instruction) error {
	inputReg := instr.P1
	jumpAddr := instr.P2
	aggReg := instr.P3

	// Get the value to check
	mem, err := v.GetMem(inputReg)
	if err != nil {
		return err
	}

	// Initialize DistinctSets if needed
	if v.DistinctSets == nil {
		v.DistinctSets = make(map[int]map[string]bool)
	}

	// Initialize the set for this aggregate if needed
	if v.DistinctSets[aggReg] == nil {
		v.DistinctSets[aggReg] = make(map[string]bool)
	}

	// Convert value to string key for deduplication
	key := mem.ToDistinctKey()

	// Check if we've seen this value before
	if v.DistinctSets[aggReg][key] {
		// Already seen - jump to skip address
		if jumpAddr >= 0 && jumpAddr < len(v.Program) {
			v.PC = jumpAddr
		}
		return nil
	}

	// New value - mark as seen
	v.DistinctSets[aggReg][key] = true

	// Continue to next instruction (don't jump)
	return nil
}

// execDistinctRow implements OpDistinctRow - Check if row is distinct for SELECT DISTINCT
// P1 = first register of the row
// P2 = jump address if NOT distinct (already seen)
// P3 = number of columns in the row
func (v *VDBE) execDistinctRow(instr *Instruction) error {
	firstReg := instr.P1
	jumpAddr := instr.P2
	numCols := instr.P3

	// Build a composite key from all columns
	key, err := v.buildDistinctKey(firstReg, numCols)
	if err != nil {
		return err
	}

	// Use a special set ID (-1) for SELECT DISTINCT rows
	const distinctRowSetID = -1

	// Ensure the distinct set is initialized
	v.ensureDistinctSet(distinctRowSetID)

	// Check if we've seen this row before
	if v.DistinctSets[distinctRowSetID][key] {
		// Already seen - jump to skip address
		v.jumpIfValid(jumpAddr)
		return nil
	}

	// New row - mark as seen
	v.DistinctSets[distinctRowSetID][key] = true

	// Continue to next instruction (don't jump)
	return nil
}

// buildDistinctKey creates a composite key from multiple register values
func (v *VDBE) buildDistinctKey(firstReg, numCols int) (string, error) {
	var key string
	for i := 0; i < numCols; i++ {
		mem, err := v.GetMem(firstReg + i)
		if err != nil {
			return "", err
		}
		if i > 0 {
			key += "\x00" // Separator
		}
		key += mem.ToDistinctKey()
	}
	return key, nil
}

// ensureDistinctSet initializes the distinct set map for a given set ID if needed
func (v *VDBE) ensureDistinctSet(setID int) {
	if v.DistinctSets == nil {
		v.DistinctSets = make(map[int]map[string]bool)
	}
	if v.DistinctSets[setID] == nil {
		v.DistinctSets[setID] = make(map[string]bool)
	}
}

// jumpIfValid performs a jump to the given address if it is valid
func (v *VDBE) jumpIfValid(jumpAddr int) {
	if jumpAddr >= 0 && jumpAddr < len(v.Program) {
		v.PC = jumpAddr
	}
}
