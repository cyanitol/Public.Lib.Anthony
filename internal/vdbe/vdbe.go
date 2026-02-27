package vdbe

import (
	"fmt"
)

// VdbeState represents the execution state of the VDBE.
type VdbeState uint8

const (
	StateInit     VdbeState = 0 // Prepared statement under construction
	StateReady    VdbeState = 1 // Ready to run but not yet started
	StateRun      VdbeState = 2 // Run in progress
	StateRowReady VdbeState = 3 // A result row is ready to be read
	StateHalt     VdbeState = 4 // Finished, need reset or finalize
)

// Instruction represents a single VDBE instruction.
type Instruction struct {
	Opcode  Opcode  // The opcode to execute
	P1      int     // First operand
	P2      int     // Second operand (often jump destination)
	P3      int     // Third operand
	P4      P4Union // Fourth operand (various types)
	P4Type  P4Type  // Type of P4 operand
	P5      uint16  // Fifth operand (16-bit unsigned)
	Comment string  // Debug comment (if enabled)
}

// P4Union represents the fourth operand which can be various types.
type P4Union struct {
	I   int32       // Integer value (P4_INT32)
	I64 int64       // 64-bit integer (P4_INT64)
	R   float64     // Real value (P4_REAL)
	Z   string      // String value (P4_STATIC, P4_DYNAMIC)
	P   interface{} // Generic pointer for other types
}

// CursorType represents the type of a VDBE cursor.
type CursorType uint8

const (
	CursorBTree  CursorType = 0 // B-tree cursor
	CursorSorter CursorType = 1 // Sorter cursor
	CursorVTab   CursorType = 2 // Virtual table cursor
	CursorPseudo CursorType = 3 // Pseudo-table cursor (single row)
)

// Cursor represents a database cursor in the VDBE.
type Cursor struct {
	CurType    CursorType // Type of cursor
	IsTable    bool       // True for rowid tables, false for indexes
	Writable   bool       // True if cursor supports write operations
	NullRow    bool       // True if pointing to a row with no data
	SeekResult int        // Result of previous seek operation

	// B-tree cursor data (for CursorBTree)
	RootPage    uint32      // Root page of the B-tree
	BtreeCursor interface{} // Actual btree.BtCursor (stored as interface to avoid import cycle)
	CurrentKey  []byte      // Current key being pointed at
	CurrentVal  []byte      // Current value/record being pointed at

	// Pseudo-table cursor data (for CursorPseudo)
	PseudoReg int // Register containing pseudo-table data

	// Column cache for OP_Column optimization
	CacheStatus uint32   // Valid if matches VDBE cacheCtr
	CachedCols  [][]byte // Cached column values

	// For iterating
	EOF bool // True if cursor is at end of file

	// For rowid generation
	LastRowid int64 // Last rowid used by this cursor

	// Sorter data (for CursorSorter)
	// SortOrders[i] = 0 for ASC, 1 for DESC
	SortOrders []int // Sort direction for each ORDER BY column
}

// VDBEContext holds runtime context for VDBE execution
type VDBEContext struct {
	Btree  interface{} // *btree.Btree (stored as interface to avoid import cycle)
	Pager  interface{} // *pager.Pager (stored as interface to avoid import cycle)
	Schema interface{} // Schema metadata (for future use)
}

// VDBE represents the Virtual Database Engine - a bytecode virtual machine.
type VDBE struct {
	// Program and execution state
	Program []*Instruction // The bytecode program
	PC      int            // Program counter
	State   VdbeState      // Execution state
	RC      int            // Return code

	// Memory and registers
	Mem    []*Mem // Array of memory cells (registers)
	NumMem int    // Number of memory cells allocated

	// Cursors
	Cursors   []*Cursor // Array of open cursors
	NumCursor int       // Number of cursors allocated

	// Result handling
	ResultCols []string // Names of result columns
	ResultRow  []*Mem   // Current result row

	// Error handling
	ErrorMsg    string // Error message (if any)
	ErrorAction uint8  // Recovery action on error

	// Statistics and counters
	CacheCtr uint32 // Cursor cache generation counter
	NumSteps int64  // Number of VM steps executed

	// Transaction and change tracking
	InTxn        bool  // True if in a transaction
	NumChanges   int64 // Number of database changes
	LastInsertID int64 // Last inserted rowid (for database/sql driver)

	// Function execution context
	funcCtx *FunctionContext // Function registry and aggregate state

	// Flags
	Explain  bool // True if EXPLAIN mode
	ReadOnly bool // True for read-only statements

	// Runtime context
	Ctx *VDBEContext // Execution context (btree, schema, etc.)

	// Sorters for ORDER BY
	Sorters []*Sorter // Array of open sorters

	// Trigger and sub-program support
	Parent      *VDBE                  // Parent VDBE (for sub-programs/triggers)
	SubPrograms map[int]*VDBE          // Sub-programs (triggers) keyed by ID
	Coroutines  map[int]*CoroutineInfo // Coroutine state keyed by coroutine ID
}

// CoroutineInfo holds the state of a coroutine
type CoroutineInfo struct {
	EntryPoint int  // Address to jump to when yielding to this coroutine
	YieldAddr  int  // Address to return to after yield
	Active     bool // True if coroutine is currently active
}

// Sorter is an in-memory sorting structure for ORDER BY.
type Sorter struct {
	Rows     [][]*Mem // Collected rows
	KeyCols  []int    // Indices of columns to sort by (relative to row start)
	Desc     []bool   // True for descending sort for each key column
	Current  int      // Current position during iteration
	Sorted   bool     // True after Sort() has been called
	NumCols  int      // Number of data columns per row
}

// NewSorter creates a new Sorter with the given key columns and sort directions.
func NewSorter(keyCols []int, desc []bool, numCols int) *Sorter {
	return &Sorter{
		Rows:    make([][]*Mem, 0),
		KeyCols: keyCols,
		Desc:    desc,
		Current: -1,
		Sorted:  false,
		NumCols: numCols,
	}
}

// Insert adds a row to the sorter. The row is copied.
func (s *Sorter) Insert(row []*Mem) {
	// Make a deep copy of the row
	rowCopy := make([]*Mem, len(row))
	for i, m := range row {
		newMem := &Mem{}
		newMem.Copy(m)
		rowCopy[i] = newMem
	}
	s.Rows = append(s.Rows, rowCopy)
	s.Sorted = false
}

// Sort sorts the collected rows by the key columns.
func (s *Sorter) Sort() {
	if s.Sorted || len(s.Rows) <= 1 {
		s.Sorted = true
		return
	}

	// Simple insertion sort (adequate for most ORDER BY cases)
	for i := 1; i < len(s.Rows); i++ {
		key := s.Rows[i]
		j := i - 1
		for j >= 0 && s.compareRows(s.Rows[j], key) > 0 {
			s.Rows[j+1] = s.Rows[j]
			j--
		}
		s.Rows[j+1] = key
	}
	s.Sorted = true
}

// compareRows compares two rows based on key columns.
// Returns negative if a < b, positive if a > b, 0 if equal.
func (s *Sorter) compareRows(a, b []*Mem) int {
	for i, colIdx := range s.KeyCols {
		if colIdx >= len(a) || colIdx >= len(b) {
			continue
		}
		cmp := a[colIdx].Compare(b[colIdx])
		if cmp != 0 {
			if len(s.Desc) > i && s.Desc[i] {
				return -cmp // Reverse for DESC
			}
			return cmp
		}
	}
	return 0
}

// Rewind resets the iteration to the beginning.
func (s *Sorter) Rewind() bool {
	s.Current = 0
	return len(s.Rows) > 0
}

// Next advances to the next row. Returns false if no more rows.
func (s *Sorter) Next() bool {
	s.Current++
	return s.Current < len(s.Rows)
}

// Current row returns the current row, or nil if invalid.
func (s *Sorter) CurrentRow() []*Mem {
	if s.Current >= 0 && s.Current < len(s.Rows) {
		return s.Rows[s.Current]
	}
	return nil
}

// New creates a new VDBE instance.
func New() *VDBE {
	return &VDBE{
		Program:     make([]*Instruction, 0, 16),
		Mem:         make([]*Mem, 0, 16),
		Cursors:     make([]*Cursor, 0, 4),
		State:       StateInit,
		CacheCtr:    1,
		funcCtx:     NewFunctionContext(),
		SubPrograms: make(map[int]*VDBE),
		Coroutines:  make(map[int]*CoroutineInfo),
	}
}

// AddOp adds an instruction to the program.
func (v *VDBE) AddOp(opcode Opcode, p1, p2, p3 int) int {
	addr := len(v.Program)
	instr := &Instruction{
		Opcode: opcode,
		P1:     p1,
		P2:     p2,
		P3:     p3,
		P4Type: P4NotUsed,
	}
	v.Program = append(v.Program, instr)
	return addr
}

// AddOpWithP4Int adds an instruction with a P4 integer operand.
func (v *VDBE) AddOpWithP4Int(opcode Opcode, p1, p2, p3 int, p4 int32) int {
	addr := v.AddOp(opcode, p1, p2, p3)
	v.Program[addr].P4.I = p4
	v.Program[addr].P4Type = P4Int32
	return addr
}

// AddOpWithP4Str adds an instruction with a P4 string operand.
func (v *VDBE) AddOpWithP4Str(opcode Opcode, p1, p2, p3 int, p4 string) int {
	addr := v.AddOp(opcode, p1, p2, p3)
	v.Program[addr].P4.Z = p4
	v.Program[addr].P4Type = P4Static
	return addr
}

// AddOpWithP4Real adds an instruction with a P4 real (float64) operand.
func (v *VDBE) AddOpWithP4Real(opcode Opcode, p1, p2, p3 int, p4 float64) int {
	addr := v.AddOp(opcode, p1, p2, p3)
	v.Program[addr].P4.R = p4
	v.Program[addr].P4Type = P4Real
	return addr
}

// AddOpWithP4Blob adds an instruction with a P4 blob operand.
func (v *VDBE) AddOpWithP4Blob(opcode Opcode, p1, p2, p3 int, p4 []byte) int {
	addr := v.AddOp(opcode, p1, p2, p3)
	v.Program[addr].P4.P = p4
	v.Program[addr].P4Type = P4Dynamic
	return addr
}

// SetComment sets a comment on an instruction for debugging.
func (v *VDBE) SetComment(addr int, comment string) {
	if addr >= 0 && addr < len(v.Program) {
		v.Program[addr].Comment = comment
	}
}

// AllocMemory allocates the specified number of memory cells.
func (v *VDBE) AllocMemory(n int) error {
	if n <= len(v.Mem) {
		return nil // Already allocated
	}

	// Expand the memory array
	for i := len(v.Mem); i < n; i++ {
		v.Mem = append(v.Mem, NewMem())
	}
	v.NumMem = n
	return nil
}

// GetMem returns a memory cell by index.
func (v *VDBE) GetMem(index int) (*Mem, error) {
	if index < 0 || index >= len(v.Mem) {
		return nil, fmt.Errorf("register index %d out of range [0, %d)", index, len(v.Mem))
	}
	return v.Mem[index], nil
}

// AllocCursors allocates the specified number of cursors.
func (v *VDBE) AllocCursors(n int) error {
	if n <= len(v.Cursors) {
		return nil // Already allocated
	}

	// Expand the cursor array
	for i := len(v.Cursors); i < n; i++ {
		v.Cursors = append(v.Cursors, nil)
	}
	v.NumCursor = n
	return nil
}

// GetCursor returns a cursor by index.
func (v *VDBE) GetCursor(index int) (*Cursor, error) {
	if index < 0 || index >= len(v.Cursors) {
		return nil, fmt.Errorf("cursor index %d out of range [0, %d)", index, len(v.Cursors))
	}
	if v.Cursors[index] == nil {
		return nil, fmt.Errorf("cursor %d is not open", index)
	}
	return v.Cursors[index], nil
}

// OpenCursor opens a cursor at the specified index.
func (v *VDBE) OpenCursor(index int, curType CursorType, rootPage uint32, isTable bool) error {
	if index < 0 || index >= len(v.Cursors) {
		return fmt.Errorf("cursor index %d out of range", index)
	}

	cursor := &Cursor{
		CurType:     curType,
		IsTable:     isTable,
		RootPage:    rootPage,
		CachedCols:  make([][]byte, 0),
		CacheStatus: 0,
	}

	v.Cursors[index] = cursor
	return nil
}

// CloseCursor closes a cursor at the specified index.
func (v *VDBE) CloseCursor(index int) error {
	if index < 0 || index >= len(v.Cursors) {
		return fmt.Errorf("cursor index %d out of range", index)
	}
	v.Cursors[index] = nil
	return nil
}

// Reset resets the VDBE to its initial state, ready to execute again.
func (v *VDBE) Reset() error {
	// Reset all memory cells
	for _, mem := range v.Mem {
		mem.Release()
	}

	// Close all cursors
	for i := range v.Cursors {
		v.Cursors[i] = nil
	}

	// Reset execution state
	v.PC = 0
	v.State = StateReady
	v.RC = 0
	v.ResultRow = nil
	v.ErrorMsg = ""
	v.NumSteps = 0

	return nil
}

// Finalize finalizes the VDBE and releases all resources.
func (v *VDBE) Finalize() error {
	// Release all memory cells
	for _, mem := range v.Mem {
		if mem != nil {
			mem.Release()
		}
	}
	v.Mem = nil

	// Close all cursors
	v.Cursors = nil

	// Clear the program
	v.Program = nil

	v.State = StateHalt
	return nil
}

// SetError sets an error message on the VDBE.
func (v *VDBE) SetError(msg string) {
	v.ErrorMsg = msg
}

// GetError returns the current error message.
func (v *VDBE) GetError() string {
	return v.ErrorMsg
}

// ExplainProgram returns a string representation of the program for debugging.
func (v *VDBE) ExplainProgram() string {
	if len(v.Program) == 0 {
		return "Empty program"
	}

	result := "addr  opcode         p1    p2    p3    p4             p5  comment\n"
	result += "----  -------------  ----  ----  ----  -------------  --  -------\n"

	for i, instr := range v.Program {
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
			comment = instr.Comment
		}

		result += fmt.Sprintf("%-4d  %-13s  %-4d  %-4d  %-4d  %-13s  %-2d  %s\n",
			i, instr.Opcode.String(), instr.P1, instr.P2, instr.P3, p4str, instr.P5, comment)
	}

	return result
}

// IsReadOnly returns true if this is a read-only statement.
func (v *VDBE) IsReadOnly() bool {
	return v.ReadOnly
}

// SetReadOnly sets the read-only flag.
func (v *VDBE) SetReadOnly(ro bool) {
	v.ReadOnly = ro
}

// NumOps returns the number of instructions in the program.
func (v *VDBE) NumOps() int {
	return len(v.Program)
}

// GetInstruction returns an instruction by address.
func (v *VDBE) GetInstruction(addr int) (*Instruction, error) {
	if addr < 0 || addr >= len(v.Program) {
		return nil, fmt.Errorf("instruction address %d out of range", addr)
	}
	return v.Program[addr], nil
}

// IncrCacheCtr increments the cache counter, invalidating all cursor caches.
func (v *VDBE) IncrCacheCtr() {
	v.CacheCtr++
}
