// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
	"time"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/types"
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

	// Virtual table cursor data (for CursorVTab)
	VTabCursor interface{} // Actual vtab.VirtualCursor (stored as interface to avoid import cycle)
	VTable     interface{} // Actual vtab.VirtualTable (stored as interface to avoid import cycle)

	// Schema metadata (for handling ALTER TABLE ADD COLUMN default values)
	Table interface{} // *schema.Table (stored as interface to avoid import cycle)
}

// VDBEContext holds runtime context for VDBE execution.
// Uses typed interfaces from internal/types where possible to improve type safety.
// Some fields remain as interface{} due to Go's type system limitations with covariant return types.
type VDBEContext struct {
	Btree             types.BtreeAccess // B-tree storage layer access
	Pager             interface{}       // *pager.Pager (kept as interface{} - InTransaction not in PagerWriter interface)
	Schema            interface{}       // *schema.Schema (kept as interface{} due to covariant return type issues)
	CollationRegistry interface{}       // *collation.CollationRegistry (kept as interface{} due to covariant return types)

	// Foreign key constraint support
	FKManager          interface{} // *constraint.ForeignKeyManager
	ForeignKeysEnabled bool        // PRAGMA foreign_keys setting
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
	CacheCtr uint32           // Cursor cache generation counter
	NumSteps int64            // Number of VM steps executed
	Stats    *QueryStatistics // Query execution statistics (Phase 9.2)

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

	// Window function support
	WindowStates map[int]*WindowState // Window states keyed by cursor/index

	// DISTINCT aggregate tracking (maps aggregate register to seen values)
	DistinctSets map[int]map[string]bool // Tracks unique values for DISTINCT aggregates

	// Debug support (Phase 9.4)
	Debug *DebugContext // Debug context for instruction tracing and inspection
}

// CoroutineInfo holds the state of a coroutine
type CoroutineInfo struct {
	EntryPoint int  // Address to jump to when yielding to this coroutine
	YieldAddr  int  // Address to return to after yield
	Active     bool // True if coroutine is currently active
}

// Sorter is an in-memory sorting structure for ORDER BY.
type Sorter struct {
	Rows              [][]*Mem    // Collected rows
	KeyCols           []int       // Indices of columns to sort by (relative to row start)
	Desc              []bool      // True for descending sort for each key column
	Collations        []string    // Collation name for each key column (empty string for default)
	Current           int         // Current position during iteration
	Sorted            bool        // True after Sort() has been called
	NumCols           int         // Number of data columns per row
	CollationRegistry interface{} // *collation.CollationRegistry (for connection-specific collations)
}

// NewSorter creates a new Sorter with the given key columns and sort directions.
func NewSorter(keyCols []int, desc []bool, collations []string, numCols int) *Sorter {
	return &Sorter{
		Rows:       make([][]*Mem, 0),
		KeyCols:    keyCols,
		Desc:       desc,
		Collations: collations,
		Current:    -1,
		Sorted:     false,
		NumCols:    numCols,
	}
}

// NewSorterWithRegistry creates a new Sorter with a custom collation registry.
func NewSorterWithRegistry(keyCols []int, desc []bool, collations []string, numCols int, registry interface{}) *Sorter {
	return &Sorter{
		Rows:              make([][]*Mem, 0),
		KeyCols:           keyCols,
		Desc:              desc,
		Collations:        collations,
		Current:           -1,
		Sorted:            false,
		NumCols:           numCols,
		CollationRegistry: registry,
	}
}

// Insert adds a row to the sorter. The row is copied.
func (s *Sorter) Insert(row []*Mem) {
	// Make a deep copy of the row using pooled Mem cells
	rowCopy := make([]*Mem, len(row))
	for i, m := range row {
		newMem := GetMem()
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

// isColumnInBounds checks if the column index is valid for both rows.
func (s *Sorter) isColumnInBounds(colIdx int, a, b []*Mem) bool {
	return colIdx < len(a) && colIdx < len(b)
}

// compareColumn compares a single column from two rows using the appropriate collation.
func (s *Sorter) compareColumn(a, b *Mem, keyIdx int) int {
	if len(s.Collations) > keyIdx && s.Collations[keyIdx] != "" {
		return a.CompareWithCollationRegistry(b, s.Collations[keyIdx], s.CollationRegistry)
	}
	return a.Compare(b)
}

// applySortDirection applies the sort direction (ASC/DESC) to a comparison result.
func (s *Sorter) applySortDirection(cmp int, keyIdx int) int {
	if len(s.Desc) > keyIdx && s.Desc[keyIdx] {
		return -cmp // Reverse for DESC
	}
	return cmp
}

// compareRows compares two rows based on key columns.
// Returns negative if a < b, positive if a > b, 0 if equal.
func (s *Sorter) compareRows(a, b []*Mem) int {
	for i, colIdx := range s.KeyCols {
		if !s.isColumnInBounds(colIdx, a, b) {
			continue
		}

		cmp := s.compareColumn(a[colIdx], b[colIdx], i)
		if cmp != 0 {
			return s.applySortDirection(cmp, i)
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

// Close releases all pooled Mem cells used by the sorter.
func (s *Sorter) Close() {
	for _, row := range s.Rows {
		for _, mem := range row {
			if mem != nil {
				PutMem(mem)
			}
		}
	}
	s.Rows = nil
}

// New creates a new VDBE instance.
func New() *VDBE {
	return &VDBE{
		Program:      make([]*Instruction, 0, 16),
		Mem:          make([]*Mem, 0, 16),
		Cursors:      make([]*Cursor, 0, 4),
		State:        StateInit,
		CacheCtr:     1,
		funcCtx:      NewFunctionContext(),
		SubPrograms:  make(map[int]*VDBE),
		Coroutines:   make(map[int]*CoroutineInfo),
		WindowStates: make(map[int]*WindowState),
		Stats:        NewQueryStatistics(), // Enable statistics by default
	}
}

// AddOp adds an instruction to the program.
func (v *VDBE) AddOp(opcode Opcode, p1, p2, p3 int) int {
	addr := len(v.Program)
	// Use pooled instruction
	instr := GetInstruction()
	instr.Opcode = opcode
	instr.P1 = p1
	instr.P2 = p2
	instr.P3 = p3
	instr.P4Type = P4NotUsed
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

// AddOpWithP4Int64 adds an instruction with a P4 int64 operand.
func (v *VDBE) AddOpWithP4Int64(opcode Opcode, p1, p2, p3 int, p4 int64) int {
	addr := v.AddOp(opcode, p1, p2, p3)
	v.Program[addr].P4.I64 = p4
	v.Program[addr].P4Type = P4Int64
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

	// Expand the memory array using pooled Mem cells
	for i := len(v.Mem); i < n; i++ {
		v.Mem = append(v.Mem, GetMem())
	}
	v.NumMem = n

	// Update statistics if enabled
	if v.Stats != nil {
		v.Stats.AllocatedCells = n
		// Estimate memory usage (each Mem cell is roughly 64 bytes)
		v.Stats.UpdateMemoryUsage(int64(n * 64))
	}

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
	v.resetMemoryCells()
	v.resetResultRow()
	v.resetSorters()
	v.resetCursors()
	v.resetExecutionState()
	v.resetStatistics()
	return nil
}

// resetMemoryCells resets all memory cells but keeps them allocated.
func (v *VDBE) resetMemoryCells() {
	for _, mem := range v.Mem {
		mem.Release()
	}
}

// resetResultRow returns ResultRow cells to pool.
func (v *VDBE) resetResultRow() {
	for _, mem := range v.ResultRow {
		if mem != nil {
			PutMem(mem)
		}
	}
	v.ResultRow = nil
}

// resetSorters closes all sorters and returns pooled memory.
func (v *VDBE) resetSorters() {
	for _, sorter := range v.Sorters {
		if sorter != nil {
			sorter.Close()
		}
	}
}

// resetCursors closes all cursors.
func (v *VDBE) resetCursors() {
	for i := range v.Cursors {
		v.Cursors[i] = nil
	}
}

// resetExecutionState resets the execution state variables.
func (v *VDBE) resetExecutionState() {
	v.PC = 0
	v.State = StateReady
	v.RC = 0
	v.ErrorMsg = ""
	v.NumSteps = 0
}

// resetStatistics resets statistics if enabled.
func (v *VDBE) resetStatistics() {
	if v.Stats != nil {
		v.Stats = NewQueryStatistics()
	}
}

// Finalize finalizes the VDBE and releases all resources.
func (v *VDBE) Finalize() error {
	v.releaseMemoryCells()
	v.releaseResultRow()
	v.releaseSorters()
	v.releaseInstructions()

	// Close all cursors
	v.Cursors = nil

	// Clear the program
	v.Program = nil

	v.State = StateHalt
	return nil
}

// releaseMemoryCells returns all memory cells to the pool.
func (v *VDBE) releaseMemoryCells() {
	for _, mem := range v.Mem {
		if mem != nil {
			PutMem(mem)
		}
	}
	v.Mem = nil
}

// releaseResultRow returns ResultRow cells to the pool.
func (v *VDBE) releaseResultRow() {
	for _, mem := range v.ResultRow {
		if mem != nil {
			PutMem(mem)
		}
	}
	v.ResultRow = nil
}

// releaseSorters closes all sorters and returns pooled memory.
func (v *VDBE) releaseSorters() {
	for _, sorter := range v.Sorters {
		if sorter != nil {
			sorter.Close()
		}
	}
	v.Sorters = nil
}

// releaseInstructions returns all instructions to the pool.
func (v *VDBE) releaseInstructions() {
	for _, instr := range v.Program {
		if instr != nil {
			PutInstruction(instr)
		}
	}
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

	result := v.explainHeader()
	for i, instr := range v.Program {
		result += v.explainInstruction(i, instr)
	}

	return result
}

// explainHeader returns the header for the explain output.
func (v *VDBE) explainHeader() string {
	return "addr  opcode         p1    p2    p3    p4             p5  comment\n" +
		"----  -------------  ----  ----  ----  -------------  --  -------\n"
}

// explainInstruction formats a single instruction for explain output.
func (v *VDBE) explainInstruction(addr int, instr *Instruction) string {
	p4str := v.formatP4(instr)
	comment := instr.Comment
	return fmt.Sprintf("%-4d  %-13s  %-4d  %-4d  %-4d  %-13s  %-2d  %s\n",
		addr, instr.Opcode.String(), instr.P1, instr.P2, instr.P3, p4str, instr.P5, comment)
}

// formatP4 formats the P4 operand based on its type.
func (v *VDBE) formatP4(instr *Instruction) string {
	switch instr.P4Type {
	case P4Int32:
		return fmt.Sprintf("%d", instr.P4.I)
	case P4Int64:
		return fmt.Sprintf("%d", instr.P4.I64)
	case P4Real:
		return fmt.Sprintf("%g", instr.P4.R)
	case P4Static, P4Dynamic:
		return fmt.Sprintf("%q", instr.P4.Z)
	default:
		return ""
	}
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

// QueryStatistics tracks execution statistics for a VDBE program.
// This provides observability into query performance and resource usage.
type QueryStatistics struct {
	// Execution timing
	StartTime   int64 // Start time in nanoseconds (time.Now().UnixNano())
	EndTime     int64 // End time in nanoseconds
	ExecutionNS int64 // Total execution time in nanoseconds
	ExecutionMS int64 // Total execution time in milliseconds (derived)

	// Instruction counts
	NumInstructions int64 // Total number of instructions executed
	NumJumps        int64 // Number of jump instructions
	NumComparisons  int64 // Number of comparison operations

	// Data operations
	RowsRead    int64 // Number of rows read from tables/indexes
	RowsWritten int64 // Number of rows inserted/updated/deleted
	RowsScanned int64 // Number of rows scanned (including non-matching)

	// I/O operations
	PageReads   int64 // Number of pages read from disk
	PageWrites  int64 // Number of pages written to disk
	CacheHits   int64 // Number of page cache hits
	CacheMisses int64 // Number of page cache misses

	// Memory operations
	MemoryUsed     int64 // Peak memory used by registers (bytes)
	SorterMemory   int64 // Memory used by sorters (bytes)
	TempSpaceUsed  int64 // Temporary disk space used (bytes)
	AllocatedCells int   // Number of memory cells allocated

	// Cursor operations
	CursorSeeks  int64 // Number of cursor seek operations
	CursorSteps  int64 // Number of cursor next/previous operations
	IndexLookups int64 // Number of index lookup operations

	// Transaction tracking
	TransactionLevel int    // Nesting level of transactions (0=none)
	IsolationLevel   string // Transaction isolation level

	// Query metadata
	IsReadOnly bool   // True if query is read-only
	IsExplain  bool   // True if this is an EXPLAIN query
	QueryType  string // "SELECT", "INSERT", "UPDATE", "DELETE", etc.
}

// NewQueryStatistics creates a new statistics tracker.
func NewQueryStatistics() *QueryStatistics {
	return &QueryStatistics{
		StartTime: 0,
		EndTime:   0,
	}
}

// Start marks the beginning of query execution.
func (s *QueryStatistics) Start() {
	s.StartTime = getCurrentTimeNanos()
}

// End marks the end of query execution and calculates duration.
func (s *QueryStatistics) End() {
	s.EndTime = getCurrentTimeNanos()
	if s.StartTime > 0 {
		s.ExecutionNS = s.EndTime - s.StartTime
		s.ExecutionMS = s.ExecutionNS / 1000000
	}
}

// getCurrentTimeNanos returns current time in nanoseconds.
func getCurrentTimeNanos() int64 {
	return time.Now().UnixNano()
}

// RecordInstruction records execution of an instruction.
func (s *QueryStatistics) RecordInstruction(opcode Opcode) {
	s.NumInstructions++
	s.trackOperationType(opcode)
}

// opcodeStatCategory maps opcodes to their statistic categories
var opcodeStatCategory = map[Opcode]func(*QueryStatistics){
	OpGoto:      func(s *QueryStatistics) { s.NumJumps++ },
	OpIf:        func(s *QueryStatistics) { s.NumJumps++ },
	OpIfNot:     func(s *QueryStatistics) { s.NumJumps++ },
	OpIfPos:     func(s *QueryStatistics) { s.NumJumps++ },
	OpIfNotZero: func(s *QueryStatistics) { s.NumJumps++ },
	OpEq:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpNe:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpLt:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpLe:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpGt:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpGe:        func(s *QueryStatistics) { s.NumComparisons++ },
	OpRewind:    func(s *QueryStatistics) { s.CursorSeeks++ },
	OpSeekGE:    func(s *QueryStatistics) { s.CursorSeeks++ },
	OpSeekGT:    func(s *QueryStatistics) { s.CursorSeeks++ },
	OpSeekLE:    func(s *QueryStatistics) { s.CursorSeeks++ },
	OpSeekLT:    func(s *QueryStatistics) { s.CursorSeeks++ },
	OpSeekRowid: func(s *QueryStatistics) { s.CursorSeeks++ },
	OpNext:      func(s *QueryStatistics) { s.CursorSteps++ },
	OpPrev:      func(s *QueryStatistics) { s.CursorSteps++ },
	OpRowData:   func(s *QueryStatistics) { s.RowsRead++ },
	OpColumn:    func(s *QueryStatistics) { s.RowsRead++ },
	OpInsert:    func(s *QueryStatistics) { s.RowsWritten++ },
	OpDelete:    func(s *QueryStatistics) { s.RowsWritten++ },
	OpIdxInsert: func(s *QueryStatistics) { s.IndexLookups++ },
	OpIdxDelete: func(s *QueryStatistics) { s.IndexLookups++ },
}

// trackOperationType tracks specific operation types for statistics.
func (s *QueryStatistics) trackOperationType(opcode Opcode) {
	if fn, ok := opcodeStatCategory[opcode]; ok {
		fn(s)
	}
}

// RecordPageRead records a page read operation.
func (s *QueryStatistics) RecordPageRead() {
	s.PageReads++
}

// RecordPageWrite records a page write operation.
func (s *QueryStatistics) RecordPageWrite() {
	s.PageWrites++
}

// RecordCacheHit records a page cache hit.
func (s *QueryStatistics) RecordCacheHit() {
	s.CacheHits++
}

// RecordCacheMiss records a page cache miss.
func (s *QueryStatistics) RecordCacheMiss() {
	s.CacheMisses++
}

// RecordRowScanned records scanning a row (even if not returned).
func (s *QueryStatistics) RecordRowScanned() {
	s.RowsScanned++
}

// UpdateMemoryUsage updates the peak memory usage.
func (s *QueryStatistics) UpdateMemoryUsage(bytes int64) {
	if bytes > s.MemoryUsed {
		s.MemoryUsed = bytes
	}
}

// UpdateSorterMemory updates the sorter memory usage.
func (s *QueryStatistics) UpdateSorterMemory(bytes int64) {
	if bytes > s.SorterMemory {
		s.SorterMemory = bytes
	}
}

// GetStatistics returns the current query statistics.
// This creates a copy to avoid race conditions.
func (v *VDBE) GetStatistics() *QueryStatistics {
	if v.Stats == nil {
		return NewQueryStatistics()
	}
	// Return a copy to avoid concurrent modification
	statsCopy := *v.Stats
	return &statsCopy
}

// ResetStatistics resets all statistics counters.
func (v *VDBE) ResetStatistics() {
	v.Stats = NewQueryStatistics()
}

// EnableStatistics enables statistics tracking for this VDBE.
func (v *VDBE) EnableStatistics() {
	if v.Stats == nil {
		v.Stats = NewQueryStatistics()
	}
}

// DisableStatistics disables statistics tracking.
func (v *VDBE) DisableStatistics() {
	v.Stats = nil
}

// String returns a formatted string representation of the statistics.
func (s *QueryStatistics) String() string {
	if s == nil {
		return "Statistics: disabled"
	}

	var result string
	result += fmt.Sprintf("Query Statistics:\n")
	result += fmt.Sprintf("  Execution Time: %d ms (%d ns)\n", s.ExecutionMS, s.ExecutionNS)
	result += fmt.Sprintf("  Instructions: %d\n", s.NumInstructions)
	result += fmt.Sprintf("    - Jumps: %d\n", s.NumJumps)
	result += fmt.Sprintf("    - Comparisons: %d\n", s.NumComparisons)
	result += fmt.Sprintf("  Data Operations:\n")
	result += fmt.Sprintf("    - Rows Read: %d\n", s.RowsRead)
	result += fmt.Sprintf("    - Rows Written: %d\n", s.RowsWritten)
	result += fmt.Sprintf("    - Rows Scanned: %d\n", s.RowsScanned)
	result += fmt.Sprintf("  Cursor Operations:\n")
	result += fmt.Sprintf("    - Seeks: %d\n", s.CursorSeeks)
	result += fmt.Sprintf("    - Steps: %d\n", s.CursorSteps)
	result += fmt.Sprintf("    - Index Lookups: %d\n", s.IndexLookups)
	result += fmt.Sprintf("  I/O Operations:\n")
	result += fmt.Sprintf("    - Page Reads: %d\n", s.PageReads)
	result += fmt.Sprintf("    - Page Writes: %d\n", s.PageWrites)
	result += fmt.Sprintf("    - Cache Hits: %d\n", s.CacheHits)
	result += fmt.Sprintf("    - Cache Misses: %d\n", s.CacheMisses)
	result += fmt.Sprintf("  Memory:\n")
	result += fmt.Sprintf("    - Peak Memory: %d bytes\n", s.MemoryUsed)
	result += fmt.Sprintf("    - Sorter Memory: %d bytes\n", s.SorterMemory)
	result += fmt.Sprintf("    - Allocated Cells: %d\n", s.AllocatedCells)
	result += fmt.Sprintf("  Query Type: %s\n", s.QueryType)
	result += fmt.Sprintf("  Read-Only: %v\n", s.IsReadOnly)

	return result
}
