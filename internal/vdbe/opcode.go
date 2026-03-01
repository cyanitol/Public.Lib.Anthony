package vdbe

// Opcode represents a VDBE instruction opcode.
type Opcode uint8

// VDBE Opcodes - these values are used as instruction opcodes in the virtual machine.
// The order and numbering must match SQLite's internal implementation for compatibility.
const (
	// Control flow opcodes
	OpInit       Opcode = 0  // Initialize the VDBE program
	OpGoto       Opcode = 1  // Unconditional jump
	OpGosub      Opcode = 2  // Jump to subroutine
	OpReturn     Opcode = 3  // Return from subroutine
	OpYield      Opcode = 4  // Yield to coroutine
	OpHalt       Opcode = 5  // Halt execution
	OpHaltIfNull Opcode = 6  // Halt if register is NULL
	OpIfPos      Opcode = 7  // Jump if positive
	OpIfNotZero  Opcode = 8  // Jump if not zero
	OpIfNullRow  Opcode = 9  // Jump if cursor points to null row
	OpIfNot      Opcode = 10 // Jump if false
	OpIf         Opcode = 11 // Jump if true

	// Integer operations
	OpInteger   Opcode = 12 // Load integer into register
	OpInt64     Opcode = 13 // Load 64-bit integer into register
	OpAddImm    Opcode = 14 // Add immediate value to register
	OpMustBeInt Opcode = 15 // Assert register contains integer
	OpIntCopy   Opcode = 16 // Copy integer between registers

	// Real (floating point) operations
	OpReal         Opcode = 17 // Load real value into register
	OpRealAffinity Opcode = 18 // Apply real affinity

	// String operations
	OpString8 Opcode = 19 // Load UTF-8 string into register
	OpString  Opcode = 20 // Load string into register (optimized)
	OpConcat  Opcode = 21 // Concatenate two strings

	// Blob operations
	OpBlob Opcode = 22 // Load blob into register

	// NULL operations
	OpNull     Opcode = 23 // Load NULL into register
	OpSoftNull Opcode = 24 // Load soft NULL (for aggregates)

	// Variable operations
	OpVariable Opcode = 25 // Load bound parameter into register
	OpParam    Opcode = 26 // Get parameter from parent VDBE

	// Register operations
	OpMove  Opcode = 27 // Move value between registers
	OpCopy  Opcode = 28 // Copy value between registers
	OpSCopy Opcode = 29 // Shallow copy between registers
	OpCast  Opcode = 30 // Cast value to type

	// Cursor operations
	OpOpenRead      Opcode = 31 // Open cursor for reading
	OpOpenWrite     Opcode = 32 // Open cursor for writing
	OpOpenAutoindex Opcode = 33 // Open auto-index cursor
	OpOpenEphemeral Opcode = 34 // Open ephemeral table
	OpClose         Opcode = 35 // Close cursor
	OpRewind        Opcode = 36 // Move cursor to first entry
	OpNext          Opcode = 37 // Move cursor to next entry
	OpPrev          Opcode = 38 // Move cursor to previous entry
	OpSeek          Opcode = 39 // Seek to specific key
	OpSeekGE        Opcode = 40 // Seek greater than or equal
	OpSeekGT        Opcode = 41 // Seek greater than
	OpSeekLE        Opcode = 42 // Seek less than or equal
	OpSeekLT        Opcode = 43 // Seek less than
	OpSeekRowid     Opcode = 44 // Seek to rowid
	OpNotExists     Opcode = 45 // Check if rowid exists
	OpSequence      Opcode = 46 // Get cursor sequence number
	OpDeferredSeek  Opcode = 47 // Deferred seek operation

	// Data retrieval opcodes
	OpColumn     Opcode = 48 // Read column from cursor
	OpRowid      Opcode = 49 // Get rowid from cursor
	OpRowData    Opcode = 50 // Get entire row data
	OpResultRow  Opcode = 51 // Output result row
	OpMakeRecord Opcode = 52 // Create a record from registers
	OpCount      Opcode = 53 // Count entries in cursor

	// Data modification opcodes
	OpInsert     Opcode = 54 // Insert record into table
	OpInsertInt  Opcode = 55 // Insert with integer key
	OpDelete     Opcode = 56 // Delete current record
	OpUpdate     Opcode = 57 // Update current record
	OpNewRowid   Opcode = 58 // Generate new rowid
	OpRowSetAdd  Opcode = 59 // Add rowid to rowset
	OpRowSetRead Opcode = 60 // Read rowid from rowset
	OpRowSetTest Opcode = 61 // Test if rowid in rowset

	// Comparison opcodes
	OpEq      Opcode = 62 // Equal comparison
	OpNe      Opcode = 63 // Not equal comparison
	OpLt      Opcode = 64 // Less than comparison
	OpLe      Opcode = 65 // Less than or equal comparison
	OpGt      Opcode = 66 // Greater than comparison
	OpGe      Opcode = 67 // Greater than or equal comparison
	OpCompare Opcode = 68 // Compare two register ranges
	OpJump    Opcode = 69 // Three-way comparison jump
	OpElseEq  Opcode = 70 // Else-equal for optimization

	// Arithmetic opcodes
	OpAdd       Opcode = 71 // Addition
	OpSubtract  Opcode = 72 // Subtraction
	OpMultiply  Opcode = 73 // Multiplication
	OpDivide    Opcode = 74 // Division
	OpRemainder Opcode = 75 // Modulo/remainder

	// Bitwise opcodes
	OpBitAnd     Opcode = 76 // Bitwise AND
	OpBitOr      Opcode = 77 // Bitwise OR
	OpBitNot     Opcode = 78 // Bitwise NOT
	OpShiftLeft  Opcode = 79 // Left shift
	OpShiftRight Opcode = 80 // Right shift

	// Logical opcodes
	OpAnd Opcode = 81 // Logical AND
	OpOr  Opcode = 82 // Logical OR
	OpNot Opcode = 83 // Logical NOT

	// Aggregate function opcodes
	OpAggStep  Opcode = 84 // Execute aggregate step function
	OpAggFinal Opcode = 85 // Execute aggregate finalization
	OpAggValue Opcode = 86 // Get aggregate intermediate value

	// Scalar function opcodes
	OpFunction Opcode = 87 // Call scalar function
	OpPureFunc Opcode = 88 // Call pure function (cacheable)

	// Transaction opcodes
	OpTransaction Opcode = 89 // Begin transaction
	OpCommit      Opcode = 90 // Commit transaction
	OpRollback    Opcode = 91 // Rollback transaction
	OpSavepoint   Opcode = 92 // Create savepoint
	OpRelease     Opcode = 93 // Release savepoint

	// Schema opcodes
	OpVerifyCookie Opcode = 94 // Verify schema cookie
	OpSetCookie    Opcode = 95 // Set schema cookie
	OpOpenPseudo   Opcode = 96 // Open pseudo-cursor

	// Sorting opcodes
	OpSorterOpen   Opcode = 97  // Open sorter
	OpSorterInsert Opcode = 98  // Insert into sorter
	OpSorterNext   Opcode = 99  // Get next from sorter
	OpSorterSort   Opcode = 100 // Sort the sorter
	OpSorterData   Opcode = 101 // Get data from sorter
	OpSorterClose  Opcode = 102 // Close sorter

	// Program flow opcodes
	OpProgram       Opcode = 150 // Execute sub-program
	OpInitCoroutine Opcode = 151 // Initialize coroutine
	OpEndCoroutine  Opcode = 152 // End coroutine
	OpOnce          Opcode = 153 // Execute once guard
	OpBeginSubrtn   Opcode = 154 // Begin subroutine

	// Index opcodes
	OpIdxInsert Opcode = 155 // Insert into index
	OpIdxDelete Opcode = 156 // Delete from index
	OpIdxRowid  Opcode = 157 // Get rowid from index
	OpIdxLT     Opcode = 158 // Index less than
	OpIdxGE     Opcode = 159 // Index greater or equal
	OpIdxGT     Opcode = 160 // Index greater than
	OpIdxLE     Opcode = 161 // Index less or equal

	// Virtual table opcodes
	OpVOpen    Opcode = 162 // Open virtual table cursor
	OpVFilter  Opcode = 163 // Virtual table filter
	OpVColumn  Opcode = 164 // Read virtual table column
	OpVNext    Opcode = 165 // Next row in virtual table
	OpVCreate  Opcode = 166 // Create virtual table
	OpVDestroy Opcode = 167 // Destroy virtual table
	OpVUpdate  Opcode = 168 // Update virtual table
	OpVRename  Opcode = 169 // Rename virtual table

	// Miscellaneous opcodes
	OpNoop        Opcode = 170 // No operation
	OpExplain     Opcode = 171 // EXPLAIN opcode marker
	OpPermutation Opcode = 172 // Set permutation for comparison
	OpCollSeq     Opcode = 173 // Set collation sequence
	OpTableLock   Opcode = 174 // Lock table
	OpFkCheck     Opcode = 175 // Foreign key check
	OpFkCounter   Opcode = 176 // Foreign key counter
	OpIsNull      Opcode = 177 // Check if NULL
	OpNotNull     Opcode = 178 // Check if NOT NULL
	OpMaxPgcnt    Opcode = 179 // Set max page count
	OpMemMax      Opcode = 180 // Track maximum value
	OpAutocommit  Opcode = 181 // Set autocommit mode

	// Type checking opcodes
	OpIsType      Opcode = 182 // Check value type
	OpIsNumeric   Opcode = 183 // Check if numeric
	OpMustBeInt64 Opcode = 184 // Assert 64-bit integer

	// Additional cursor opcodes
	OpLast          Opcode = 185 // Move cursor to last entry
	OpFirst         Opcode = 186 // Move cursor to first entry (alias)
	OpSorterCompare Opcode = 187 // Compare sorter keys

	// String operations
	OpToText    Opcode = 188 // Convert to text
	OpToBlob    Opcode = 189 // Convert to blob
	OpToNumeric Opcode = 190 // Convert to numeric
	OpToInt     Opcode = 191 // Convert to integer
	OpToReal    Opcode = 192 // Convert to real

	// Debugging/profiling opcodes
	OpTrace      Opcode = 193 // Debug trace point
	OpScanStatus Opcode = 194 // Scan status tracking

	// Join operation opcodes
	OpHashJoin   Opcode = 195 // Hash join operation
	OpMergeJoin  Opcode = 196 // Merge join operation
	OpBuildHash  Opcode = 197 // Build hash table for join
	OpProbeHash  Opcode = 198 // Probe hash table for join

	// Window function opcodes
	OpAggStepWindow     Opcode = 199 // Step aggregate in window context
	OpWindowRowNum      Opcode = 200 // ROW_NUMBER() window function
	OpWindowRank        Opcode = 201 // RANK() window function
	OpWindowDenseRank   Opcode = 202 // DENSE_RANK() window function
	OpWindowNtile       Opcode = 203 // NTILE() window function
	OpWindowLag         Opcode = 204 // LAG() window function
	OpWindowLead        Opcode = 205 // LEAD() window function
	OpWindowFirstValue  Opcode = 206 // FIRST_VALUE() window function
	OpWindowLastValue   Opcode = 207 // LAST_VALUE() window function
	OpAggDistinct       Opcode = 208 // Check if value is distinct for aggregate
)

// OpcodeNames maps opcodes to their string names for debugging.
var OpcodeNames = map[Opcode]string{
	OpInit:          "Init",
	OpGoto:          "Goto",
	OpGosub:         "Gosub",
	OpReturn:        "Return",
	OpYield:         "Yield",
	OpHalt:          "Halt",
	OpHaltIfNull:    "HaltIfNull",
	OpIfPos:         "IfPos",
	OpIfNotZero:     "IfNotZero",
	OpIfNullRow:     "IfNullRow",
	OpIfNot:         "IfNot",
	OpIf:            "If",
	OpInteger:       "Integer",
	OpInt64:         "Int64",
	OpAddImm:        "AddImm",
	OpMustBeInt:     "MustBeInt",
	OpIntCopy:       "IntCopy",
	OpReal:          "Real",
	OpRealAffinity:  "RealAffinity",
	OpString8:       "String8",
	OpString:        "String",
	OpConcat:        "Concat",
	OpBlob:          "Blob",
	OpNull:          "Null",
	OpSoftNull:      "SoftNull",
	OpVariable:      "Variable",
	OpParam:         "Param",
	OpMove:          "Move",
	OpCopy:          "Copy",
	OpSCopy:         "SCopy",
	OpCast:          "Cast",
	OpOpenRead:      "OpenRead",
	OpOpenWrite:     "OpenWrite",
	OpOpenAutoindex: "OpenAutoindex",
	OpOpenEphemeral: "OpenEphemeral",
	OpClose:         "Close",
	OpRewind:        "Rewind",
	OpNext:          "Next",
	OpPrev:          "Prev",
	OpSeek:          "Seek",
	OpSeekGE:        "SeekGE",
	OpSeekGT:        "SeekGT",
	OpSeekLE:        "SeekLE",
	OpSeekLT:        "SeekLT",
	OpSeekRowid:     "SeekRowid",
	OpNotExists:     "NotExists",
	OpSequence:      "Sequence",
	OpColumn:        "Column",
	OpRowid:         "Rowid",
	OpRowData:       "RowData",
	OpResultRow:     "ResultRow",
	OpMakeRecord:    "MakeRecord",
	OpCount:         "Count",
	OpInsert:        "Insert",
	OpInsertInt:     "InsertInt",
	OpDelete:        "Delete",
	OpUpdate:        "Update",
	OpNewRowid:      "NewRowid",
	OpRowSetAdd:     "RowSetAdd",
	OpRowSetRead:    "RowSetRead",
	OpRowSetTest:    "RowSetTest",
	OpEq:            "Eq",
	OpNe:            "Ne",
	OpLt:            "Lt",
	OpLe:            "Le",
	OpGt:            "Gt",
	OpGe:            "Ge",
	OpCompare:       "Compare",
	OpJump:          "Jump",
	OpElseEq:        "ElseEq",
	OpAdd:           "Add",
	OpSubtract:      "Subtract",
	OpMultiply:      "Multiply",
	OpDivide:        "Divide",
	OpRemainder:     "Remainder",
	OpBitAnd:        "BitAnd",
	OpBitOr:         "BitOr",
	OpBitNot:        "BitNot",
	OpShiftLeft:     "ShiftLeft",
	OpShiftRight:    "ShiftRight",
	OpAnd:           "And",
	OpOr:            "Or",
	OpNot:           "Not",
	OpAggStep:       "AggStep",
	OpAggFinal:      "AggFinal",
	OpAggValue:      "AggValue",
	OpFunction:      "Function",
	OpPureFunc:      "PureFunc",
	OpTransaction:   "Transaction",
	OpCommit:        "Commit",
	OpRollback:      "Rollback",
	OpSavepoint:     "Savepoint",
	OpRelease:       "Release",
	OpVerifyCookie:  "VerifyCookie",
	OpSetCookie:     "SetCookie",
	OpOpenPseudo:    "OpenPseudo",
	OpSorterOpen:    "SorterOpen",
	OpSorterInsert:  "SorterInsert",
	OpSorterNext:    "SorterNext",
	OpSorterSort:    "SorterSort",
	OpSorterData:    "SorterData",
	OpSorterClose:   "SorterClose",
	OpProgram:       "Program",
	OpInitCoroutine: "InitCoroutine",
	OpEndCoroutine:  "EndCoroutine",
	OpOnce:          "Once",
	OpBeginSubrtn:   "BeginSubrtn",
	OpIdxInsert:     "IdxInsert",
	OpIdxDelete:     "IdxDelete",
	OpIdxRowid:      "IdxRowid",
	OpIdxLT:         "IdxLT",
	OpIdxGE:         "IdxGE",
	OpIdxGT:         "IdxGT",
	OpIdxLE:         "IdxLE",
	OpVOpen:         "VOpen",
	OpVFilter:       "VFilter",
	OpVColumn:       "VColumn",
	OpVNext:         "VNext",
	OpVCreate:       "VCreate",
	OpVDestroy:      "VDestroy",
	OpVUpdate:       "VUpdate",
	OpVRename:       "VRename",
	OpNoop:          "Noop",
	OpExplain:       "Explain",
	OpPermutation:   "Permutation",
	OpCollSeq:       "CollSeq",
	OpTableLock:     "TableLock",
	OpFkCheck:       "FkCheck",
	OpFkCounter:     "FkCounter",
	OpIsNull:        "IsNull",
	OpNotNull:       "NotNull",
	OpMaxPgcnt:      "MaxPgcnt",
	OpMemMax:        "MemMax",
	OpAutocommit:    "Autocommit",
	OpIsType:        "IsType",
	OpIsNumeric:     "IsNumeric",
	OpMustBeInt64:   "MustBeInt64",
	OpLast:          "Last",
	OpFirst:         "First",
	OpSorterCompare: "SorterCompare",
	OpToText:        "ToText",
	OpToBlob:        "ToBlob",
	OpToNumeric:     "ToNumeric",
	OpToInt:         "ToInt",
	OpToReal:            "ToReal",
	OpTrace:             "Trace",
	OpScanStatus:        "ScanStatus",
	OpHashJoin:          "HashJoin",
	OpMergeJoin:         "MergeJoin",
	OpBuildHash:         "BuildHash",
	OpProbeHash:         "ProbeHash",
	OpAggStepWindow:     "AggStepWindow",
	OpWindowRowNum:      "WindowRowNum",
	OpWindowRank:        "WindowRank",
	OpWindowDenseRank:   "WindowDenseRank",
	OpWindowNtile:       "WindowNtile",
	OpWindowLag:         "WindowLag",
	OpWindowLead:        "WindowLead",
	OpWindowFirstValue:  "WindowFirstValue",
	OpWindowLastValue:   "WindowLastValue",
	OpAggDistinct:       "AggDistinct",
}

// String returns the string representation of an opcode.
func (op Opcode) String() string {
	if name, ok := OpcodeNames[op]; ok {
		return name
	}
	return "Unknown"
}

// P4Type represents the type of the P4 operand.
type P4Type int8

const (
	P4NotUsed    P4Type = 0   // P4 parameter not used
	P4Transient  P4Type = 0   // P4 is a transient string (same as NotUsed)
	P4Static     P4Type = -1  // P4 is a static string
	P4CollSeq    P4Type = -2  // P4 is a collation sequence
	P4Int32      P4Type = -3  // P4 is a 32-bit signed integer
	P4SubProgram P4Type = -4  // P4 is a sub-program
	P4Table      P4Type = -5  // P4 is a table reference
	P4Dynamic    P4Type = -6  // P4 is dynamically allocated
	P4FuncDef    P4Type = -7  // P4 is a function definition
	P4KeyInfo    P4Type = -8  // P4 is key information for indexes
	P4Expr       P4Type = -9  // P4 is an expression tree
	P4Mem        P4Type = -10 // P4 is a memory cell
	P4VTab       P4Type = -11 // P4 is a virtual table
	P4Real       P4Type = -12 // P4 is a 64-bit float
	P4Int64      P4Type = -13 // P4 is a 64-bit signed integer
	P4IntArray   P4Type = -14 // P4 is an array of integers
	P4FuncCtx    P4Type = -15 // P4 is a function context
	P4TableRef   P4Type = -16 // P4 is a reference-counted table
)
