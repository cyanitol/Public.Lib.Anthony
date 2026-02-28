package vdbe

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
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
	case StateRowReady:
		v.ResultRow = nil
		v.State = StateRun
	}
	return nil
}

// runUntilRowOrHalt executes instructions until a row is ready or the program halts.
func (v *VDBE) runUntilRowOrHalt() (bool, error) {
	for {
		if v.PC >= len(v.Program) {
			v.State = StateHalt
			return false, nil
		}
		instr := v.Program[v.PC]
		v.PC++
		v.NumSteps++
		if err := v.execInstruction(instr); err != nil {
			v.SetError(err.Error())
			v.State = StateHalt
			return false, err
		}
		if v.State == StateHalt {
			return false, nil
		}
		if v.State == StateRowReady {
			return true, nil
		}
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
	// Copy register P1 to register P2
	src, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}
	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}
	return dst.Copy(src)
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

func (v *VDBE) execSeekGE(instr *Instruction) error {
	// Seek cursor P1 to entry >= key in register P3
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	keyReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// In a real implementation, this would perform a B-tree seek
	_ = keyReg
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

	// For now, we create an in-memory btree for ephemeral tables
	// In a full implementation, this would create a temporary btree structure

	// Allocate cursors if needed
	if err := v.AllocCursors(instr.P1 + 1); err != nil {
		return err
	}

	// Create an ephemeral cursor (in-memory temporary table)
	// For simplicity, we'll use a pseudo-cursor type to represent ephemeral tables
	cursor := &Cursor{
		CurType:     CursorPseudo,
		IsTable:     true,
		Writable:    true,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	v.Cursors[instr.P1] = cursor
	return nil
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
	targetRowid := keyReg.IntValue()
	found := false
	for {
		currentRowid := btCursor.GetKey()
		if currentRowid > targetRowid {
			found = true
			break
		}
		if err = btCursor.Next(); err != nil {
			// Reached end without finding
			break
		}
	}

	if !found {
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

	// Move to first entry
	if err = btCursor.MoveToFirst(); err != nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	// Linear scan to find last entry < key
	targetRowid := keyReg.IntValue()
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
		if err = btCursor.Next(); err != nil {
			// Reached end
			break
		}
	}

	if !found {
		return v.seekNotFound(cursor, instr.P2)
	}

	// Reposition to the last valid entry
	if err = btCursor.MoveToFirst(); err != nil {
		return v.seekNotFound(cursor, instr.P2)
	}

	foundAgain, err := seekLinearScan(btCursor, lastValidRowid)
	if err != nil || !foundAgain {
		return v.seekNotFound(cursor, instr.P2)
	}

	cursor.EOF = false
	v.IncrCacheCtr()
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

	btCursor := seekGetBtCursor(cursor)
	if btCursor == nil {
		// No cursor, rowid doesn't exist - jump
		if instr.P2 > 0 {
			v.PC = instr.P2
		}
		return nil
	}

	if err = btCursor.MoveToFirst(); err != nil {
		// Empty table, rowid doesn't exist - jump
		if instr.P2 > 0 {
			v.PC = instr.P2
		}
		return nil
	}

	found, err := seekLinearScan(btCursor, rowidReg.IntValue())
	if err != nil {
		return err
	}

	if !found {
		// Rowid doesn't exist - jump to P2
		if instr.P2 > 0 {
			v.PC = instr.P2
		}
	}
	// If found, don't jump (continue to next instruction)

	return nil
}

func (v *VDBE) execDeferredSeek(instr *Instruction) error {
	// Seek cursor but defer until data needed
	// P1 = index cursor number
	// P2 = table cursor number
	// P3 = rowid register
	//
	// This is an optimization opcode that defers seeking the table cursor
	// until data is actually needed from it. For this simplified implementation,
	// we'll just perform the seek immediately.

	tableCursor, err := v.GetCursor(instr.P2)
	if err != nil {
		return err
	}

	rowidReg, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	btCursor := seekGetBtCursor(tableCursor)
	if btCursor == nil {
		tableCursor.EOF = true
		return nil
	}

	if err = btCursor.MoveToFirst(); err != nil {
		tableCursor.EOF = true
		return nil
	}

	found, err := seekLinearScan(btCursor, rowidReg.IntValue())
	if err != nil {
		return err
	}

	if !found {
		tableCursor.EOF = true
		return nil
	}

	tableCursor.EOF = false
	v.IncrCacheCtr()
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
	return v.parseColumnIntoMem(payload, instr.P2, dst)
}

// getColumnPayload returns the payload from cursor, or nil if unavailable.
func (v *VDBE) getColumnPayload(cursor *Cursor, dst *Mem) []byte {
	if cursor.NullRow || cursor.EOF {
		dst.SetNull()
		return nil
	}

	// Handle pseudo-table cursors (for OLD/NEW in triggers)
	if cursor.CurType == CursorPseudo {
		// Get the record data from the pseudo register
		pseudoMem, err := v.GetMem(cursor.PseudoReg)
		if err != nil || pseudoMem.IsNull() {
			dst.SetNull()
			return nil
		}
		// The pseudo register should contain a blob (record)
		if pseudoMem.IsBlob() {
			return pseudoMem.BlobValue()
		}
		dst.SetNull()
		return nil
	}

	// Handle regular btree cursors
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
func (v *VDBE) parseColumnIntoMem(payload []byte, colIndex int, dst *Mem) error {
	if err := parseRecordColumn(payload, colIndex, dst); err != nil {
		return fmt.Errorf("failed to parse record column: %w", err)
	}
	return nil
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

	// Check for null row or EOF
	if cursor.NullRow || cursor.EOF {
		dst.SetNull()
		return nil
	}

	// Handle pseudo-table cursors (they don't have rowids, set to NULL)
	if cursor.CurType == CursorPseudo {
		// Pseudo-tables for OLD/NEW don't have meaningful rowids
		// Set to NULL or could use a stored rowid if needed
		dst.SetNull()
		return nil
	}

	// Get btree cursor
	btCursor, ok := cursor.BtreeCursor.(*btree.BtCursor)
	if !ok || btCursor == nil {
		dst.SetNull()
		return nil
	}

	// Get rowid (key) from current cell
	rowid := btCursor.GetKey()
	dst.SetInt(rowid)

	return nil
}

// parseRecordColumn parses a specific column from a SQLite record
// This is a simplified version to avoid import cycles with the sql package
func parseRecordColumn(data []byte, colIndex int, dst *Mem) error {
	if len(data) == 0 {
		dst.SetNull()
		return nil
	}

	// Read header size (varint)
	headerSize, n := getVarint(data, 0)
	if n == 0 {
		dst.SetNull()
		return fmt.Errorf("invalid header size")
	}

	offset := n

	// Read serial types from header
	serialTypes := make([]uint64, 0)
	for offset < int(headerSize) {
		st, n := getVarint(data, offset)
		if n == 0 {
			dst.SetNull()
			return fmt.Errorf("invalid serial type")
		}
		serialTypes = append(serialTypes, st)
		offset += n
	}

	// Check if column index is valid
	if colIndex < 0 || colIndex >= len(serialTypes) {
		dst.SetNull()
		return nil
	}

	// Skip to the target column in the body
	for i := 0; i < colIndex; i++ {
		offset += serialTypeLen(serialTypes[i])
	}

	// Parse the target column value
	st := serialTypes[colIndex]
	return parseSerialValue(data, offset, st, dst)
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
	return int(serialType-12) / 2
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

	var v int64
	switch st {
	case 1:
		v = int64(int8(data[offset]))
	case 2:
		v = int64(int16(binary.BigEndian.Uint16(data[offset:])))
	case 3:
		v = parseSignedInt24(data[offset:])
	case 4:
		v = int64(int32(binary.BigEndian.Uint32(data[offset:])))
	case 5:
		v = parseSignedInt48(data[offset:])
	case 6:
		v = int64(binary.BigEndian.Uint64(data[offset:]))
	}
	mem.SetInt(v)
	return nil
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
		// Make a copy for the result
		v.ResultRow[i] = NewMem()
		v.ResultRow[i].Copy(mem)
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
	lo      int64
	hi      int64
	serial  uint64
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
	if err = btCursor.Insert(rowid, payload); err != nil {
		return fmt.Errorf("btree insert failed: %w", err)
	}
	cursor.LastRowid = rowid
	v.LastInsertID = rowid // Track last insert ID for database/sql driver
	// P4.I == 1 means this is part of UPDATE (paired with Delete), don't double-count
	if instr.P4.I != 1 {
		v.NumChanges++
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
		return cursor.LastRowid, nil
	}
	rowidMem, err := v.GetMem(p3)
	if err != nil {
		return 0, err
	}
	return rowidMem.IntValue(), nil
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
	// P3 = result register (1 if condition is true, 0 otherwise)
	left, err := v.GetMem(instr.P1)
	if err != nil {
		return err
	}

	right, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	cmp := left.Compare(right)
	result := int64(0)
	if test(cmp) {
		result = 1
	}

	// Store result in P3
	if instr.P3 >= len(v.Mem) {
		return fmt.Errorf("register %d out of range", instr.P3)
	}
	v.Mem[instr.P3].SetInt(result)
	return nil
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

	pager, ok := v.Ctx.Pager.(PagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement PagerInterface")
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

	pager, ok := v.Ctx.Pager.(PagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement PagerInterface")
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

	pager, ok := v.Ctx.Pager.(PagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement PagerInterface")
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
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(PagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement PagerInterface")
	}

	if instr.P1 == 0 {
		// P1=0: Begin transaction (disable autocommit)
		// Only start a transaction if not already in one
		if !pager.InTransaction() {
			return pager.BeginWrite()
		}
	} else {
		// P1=1: Commit or rollback transaction (enable autocommit)
		if pager.InWriteTransaction() {
			if instr.P2 != 0 {
				// P2 non-zero: rollback
				return pager.Rollback()
			}
			// P2 zero: commit
			return pager.Commit()
		}
		if pager.InTransaction() {
			return pager.EndRead()
		}
	}

	return nil
}

func (v *VDBE) execSavepoint(instr *Instruction) error {
	// Create, release, or rollback to a savepoint
	// P1: operation (0=begin, 1=release, 2=rollback)
	// P4: savepoint name (string)
	if v.Ctx == nil || v.Ctx.Pager == nil {
		return fmt.Errorf("no pager context available")
	}

	pager, ok := v.Ctx.Pager.(SavepointPagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement SavepointPagerInterface")
	}

	// Get savepoint name from P4
	if instr.P4Type != P4Static && instr.P4Type != P4Dynamic {
		return fmt.Errorf("savepoint name must be in P4 as string")
	}
	name := instr.P4.Z

	if name == "" {
		return fmt.Errorf("savepoint name cannot be empty")
	}

	switch instr.P1 {
	case 0:
		// Begin savepoint
		return pager.Savepoint(name)
	case 1:
		// Release savepoint
		return pager.Release(name)
	case 2:
		// Rollback to savepoint
		return pager.RollbackTo(name)
	default:
		return fmt.Errorf("invalid savepoint operation: %d", instr.P1)
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

	pager, ok := v.Ctx.Pager.(CookiePagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement CookiePagerInterface")
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

	pager, ok := v.Ctx.Pager.(CookiePagerInterface)
	if !ok {
		return fmt.Errorf("pager does not implement CookiePagerInterface")
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

	// Get key columns and desc info from P4.P
	var keyCols []int
	var desc []bool
	if instr.P4.P != nil {
		if keyInfo, ok := instr.P4.P.(*SorterKeyInfo); ok {
			keyCols = keyInfo.KeyCols
			desc = keyInfo.Desc
		}
	}

	v.Sorters[sorterNum] = NewSorter(keyCols, desc, numCols)
	return nil
}

// SorterKeyInfo holds key column information for sorting.
type SorterKeyInfo struct {
	KeyCols []int  // Column indices for sorting
	Desc    []bool // True for descending order
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

	if sorterNum < len(v.Sorters) {
		v.Sorters[sorterNum] = nil
	}
	return nil
}

// Type conversion opcode implementations

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

	affinity := instr.P2
	switch affinity {
	case 0: // NONE/BLOB affinity - keep as-is
		return nil

	case 1: // BLOB affinity - convert to blob
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

	case 2: // TEXT affinity - convert to text
		return mem.Stringify()

	case 3: // INTEGER affinity - convert to integer, NULL if not numeric
		if mem.IsInt() {
			return nil
		}
		// Try to convert to integer
		if mem.IsReal() {
			mem.SetInt(int64(mem.RealValue()))
			return nil
		}
		if mem.IsString() || mem.IsBlob() {
			// Try to parse as integer
			err := mem.Integerify()
			if err != nil {
				// Not a valid integer - set to NULL
				mem.SetNull()
			}
			return nil
		}
		// Can't convert to integer - set to NULL
		mem.SetNull()
		return nil

	case 4: // REAL affinity - convert to real
		return mem.Realify()

	case 5: // NUMERIC affinity - try int, then float, keep text if neither works
		if mem.IsNumeric() {
			return nil
		}
		if mem.IsString() || mem.IsBlob() {
			// Try integer first
			str := mem.StrValue()
			if val, err := strconv.ParseInt(str, 10, 64); err == nil {
				mem.SetInt(val)
				return nil
			}
			// Try real
			if val, err := strconv.ParseFloat(str, 64); err == nil {
				mem.SetReal(val)
				return nil
			}
			// Keep as text if neither works
			return nil
		}
		return nil

	default:
		return fmt.Errorf("unknown affinity code: %d", affinity)
	}
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

	// NULL remains NULL
	if mem.IsNull() {
		return nil
	}

	// Already numeric, nothing to do
	if mem.IsNumeric() {
		return nil
	}

	// Try to convert to numeric
	if mem.IsString() || mem.IsBlob() {
		str := mem.StrValue()

		// Try integer first
		if val, err := strconv.ParseInt(str, 10, 64); err == nil {
			mem.SetInt(val)
			return nil
		}

		// Try real
		if val, err := strconv.ParseFloat(str, 64); err == nil {
			mem.SetReal(val)
			return nil
		}

		// Keep as text if neither conversion works
		return nil
	}

	return nil
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

	// Try to parse string/blob as integer
	if mem.IsString() || mem.IsBlob() {
		str := mem.StrValue()
		// Try parsing as integer directly
		if val, err := strconv.ParseInt(str, 10, 64); err == nil {
			mem.SetInt(val)
			return nil
		}
		// Try parsing as float and truncating
		if val, err := strconv.ParseFloat(str, 64); err == nil {
			mem.SetInt(int64(val))
			return nil
		}
		// Can't convert - set to 0
		mem.SetInt(0)
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
	// P3 = P2 << P1
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

	// Convert operands to integers
	shift := shiftAmount.IntValue()
	val := value.IntValue()

	// SQLite behavior: negative shift amounts or shifts >= 64 result in 0
	if shift < 0 || shift >= 64 {
		result.SetInt(0)
		return nil
	}

	// Perform left shift
	result.SetInt(val << uint(shift))
	return nil
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

	// Convert operands to integers
	shift := shiftAmount.IntValue()
	val := value.IntValue()

	// SQLite behavior: negative shift amounts result in 0
	// Shifts >= 64 also result in 0 (or -1 for negative values with arithmetic shift)
	if shift < 0 {
		result.SetInt(0)
		return nil
	}

	if shift >= 64 {
		// Arithmetic right shift: sign-extend
		if val < 0 {
			result.SetInt(-1)
		} else {
			result.SetInt(0)
		}
		return nil
	}

	// Perform right shift (arithmetic shift in Go for signed integers)
	result.SetInt(val >> uint(shift))
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

	// Evaluate left operand as boolean
	leftIsNull := left.IsNull()
	var leftBool bool
	if !leftIsNull {
		if left.IsInt() {
			leftBool = left.IntValue() != 0
		} else {
			leftBool = left.RealValue() != 0.0
		}
	}

	// Evaluate right operand as boolean
	rightIsNull := right.IsNull()
	var rightBool bool
	if !rightIsNull {
		if right.IsInt() {
			rightBool = right.IntValue() != 0
		} else {
			rightBool = right.RealValue() != 0.0
		}
	}

	// Apply SQLite logical AND semantics
	if !leftIsNull && !leftBool {
		// FALSE AND anything = FALSE
		result.SetInt(0)
		return nil
	}

	if !rightIsNull && !rightBool {
		// anything AND FALSE = FALSE
		result.SetInt(0)
		return nil
	}

	if leftIsNull || rightIsNull {
		// NULL involved (and no FALSE found) = NULL
		result.SetNull()
		return nil
	}

	// Both are TRUE
	result.SetInt(1)
	return nil
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

	// Evaluate left operand as boolean
	leftIsNull := left.IsNull()
	var leftBool bool
	if !leftIsNull {
		if left.IsInt() {
			leftBool = left.IntValue() != 0
		} else {
			leftBool = left.RealValue() != 0.0
		}
	}

	// Evaluate right operand as boolean
	rightIsNull := right.IsNull()
	var rightBool bool
	if !rightIsNull {
		if right.IsInt() {
			rightBool = right.IntValue() != 0
		} else {
			rightBool = right.RealValue() != 0.0
		}
	}

	// Apply SQLite logical OR semantics
	if !leftIsNull && leftBool {
		// TRUE OR anything = TRUE
		result.SetInt(1)
		return nil
	}

	if !rightIsNull && rightBool {
		// anything OR TRUE = TRUE
		result.SetInt(1)
		return nil
	}

	if leftIsNull || rightIsNull {
		// NULL involved (and no TRUE found) = NULL
		result.SetNull()
		return nil
	}

	// Both are FALSE
	result.SetInt(0)
	return nil
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
	// Get the index cursor
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Verify this is an index cursor
	if cursor.IsTable {
		return fmt.Errorf("cursor %d is not an index cursor", instr.P1)
	}

	// Verify cursor is writable
	if !cursor.Writable {
		return fmt.Errorf("cursor %d is not writable (opened with OpenRead instead of OpenWrite)", instr.P1)
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
	var key []byte
	if keyMem.IsBlob() {
		key = keyMem.BlobValue()
	} else if keyMem.IsString() {
		key = []byte(keyMem.StrValue())
	} else {
		// Convert other types to blob representation
		keyMem.Stringify()
		key = []byte(keyMem.StrValue())
	}

	// Extract rowid as integer
	rowid := rowidMem.IntValue()

	// Get the index cursor (btree.IndexCursor)
	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		return fmt.Errorf("invalid index cursor for IdxInsert")
	}

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
	// Get the index cursor
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Verify this is an index cursor
	if cursor.IsTable {
		return fmt.Errorf("cursor %d is not an index cursor", instr.P1)
	}

	// Verify cursor is writable
	if !cursor.Writable {
		return fmt.Errorf("cursor %d is not writable (opened with OpenRead instead of OpenWrite)", instr.P1)
	}

	// Get the key from register P2
	keyMem, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// Extract key as blob
	var key []byte
	if keyMem.IsBlob() {
		key = keyMem.BlobValue()
	} else if keyMem.IsString() {
		key = []byte(keyMem.StrValue())
	} else {
		keyMem.Stringify()
		key = []byte(keyMem.StrValue())
	}

	// Get the index cursor (btree.IndexCursor)
	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		return fmt.Errorf("invalid index cursor for IdxDelete")
	}

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

	// Invalidate cursor cache
	v.IncrCacheCtr()

	return nil
}

// execIdxRowid gets the rowid from the current index position.
// P1 = cursor number (index cursor)
// P2 = destination register for rowid
func (v *VDBE) execIdxRowid(instr *Instruction) error {
	// Get the index cursor
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Verify this is an index cursor
	if cursor.IsTable {
		return fmt.Errorf("cursor %d is not an index cursor", instr.P1)
	}

	// Get destination register
	dst, err := v.GetMem(instr.P2)
	if err != nil {
		return err
	}

	// Check for EOF or null row
	if cursor.EOF || cursor.NullRow {
		dst.SetNull()
		return nil
	}

	// Get the index cursor (btree.IndexCursor)
	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		dst.SetNull()
		return nil
	}

	// Get the rowid from the current index entry
	rowid := idxCursor.GetRowid()
	dst.SetInt(rowid)

	return nil
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
	// Get the index cursor
	cursor, err := v.GetCursor(instr.P1)
	if err != nil {
		return err
	}

	// Verify this is an index cursor
	if cursor.IsTable {
		return fmt.Errorf("cursor %d is not an index cursor", instr.P1)
	}

	// Get the comparison key from register P3
	keyMem, err := v.GetMem(instr.P3)
	if err != nil {
		return err
	}

	// Extract comparison key as blob
	var compKey []byte
	if keyMem.IsBlob() {
		compKey = keyMem.BlobValue()
	} else if keyMem.IsString() {
		compKey = []byte(keyMem.StrValue())
	} else {
		keyMem.Stringify()
		compKey = []byte(keyMem.StrValue())
	}

	// Get the index cursor (btree.IndexCursor)
	idxCursor, ok := cursor.BtreeCursor.(*btree.IndexCursor)
	if !ok || idxCursor == nil {
		return fmt.Errorf("invalid index cursor for index comparison")
	}

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

	// Get the virtual cursor from the virtual table
	// This requires casting the interface{} to vtab.VirtualTable
	// For now, we'll assume it has an Open() method
	if cursor.VTabCursor == nil {
		// Open the cursor if not already open
		// This would call vtable.Open() in a real implementation
		// For now, just set a placeholder
		cursor.VTabCursor = nil // Will be set by actual implementation
	}

	// Get idxNum and idxStr
	_ = instr.P3 // idxNum - will be used when implementing actual Filter call
	_ = ""       // idxStr - will be used when implementing actual Filter call
	if instr.P4Type == P4Static || instr.P4Type == P4Dynamic {
		_ = instr.P4.Z // Will be used for idxStr
	}

	// Get constraint values from registers
	argc := instr.P2
	argv := make([]interface{}, argc)
	for i := 0; i < argc; i++ {
		regIdx := int(instr.P5) + i
		if regIdx < len(v.Mem) {
			argv[i] = v.Mem[regIdx].Value()
		}
	}

	// Call Filter on the virtual cursor
	// This would be: cursor.VTabCursor.Filter(idxNum, idxStr, argv)
	// For now, just mark the cursor as not at EOF
	cursor.EOF = false
	_ = argv // Suppress unused warning until we implement actual Filter call

	return nil
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
	windowIdx := instr.P1
	outputReg := instr.P2
	colIdx := instr.P3
	offset := 1
	if instr.P4.I > 0 {
		offset = int(instr.P4.I)
	}

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get the lag row
	lagRow := windowState.GetLagRow(offset)

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}

	if lagRow == nil || colIdx >= len(lagRow) {
		// Use default value if provided in P5, otherwise NULL
		if instr.P5 > 0 {
			defaultMem, err := v.GetMem(int(instr.P5))
			if err != nil {
				return err
			}
			return mem.Copy(defaultMem)
		}
		mem.SetNull()
	} else {
		return mem.Copy(lagRow[colIdx])
	}

	return nil
}

// execWindowLead implements OpWindowLead - LEAD() window function
// P1 = window state index
// P2 = output register
// P3 = column index to retrieve
// P4 = offset (default 1)
// P5 = default value register (if row doesn't exist)
func (v *VDBE) execWindowLead(instr *Instruction) error {
	windowIdx := instr.P1
	outputReg := instr.P2
	colIdx := instr.P3
	offset := 1
	if instr.P4.I > 0 {
		offset = int(instr.P4.I)
	}

	// Get window state
	windowState, ok := v.WindowStates[windowIdx]
	if !ok {
		return fmt.Errorf("window state %d not found", windowIdx)
	}

	// Get the lead row
	leadRow := windowState.GetLeadRow(offset)

	// Store result
	mem, err := v.GetMem(outputReg)
	if err != nil {
		return err
	}

	if leadRow == nil || colIdx >= len(leadRow) {
		// Use default value if provided in P5, otherwise NULL
		if instr.P5 > 0 {
			defaultMem, err := v.GetMem(int(instr.P5))
			if err != nil {
				return err
			}
			return mem.Copy(defaultMem)
		}
		mem.SetNull()
	} else {
		return mem.Copy(leadRow[colIdx])
	}

	return nil
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
