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

	// Register operations
	OpMove  Opcode = 26 // Move value between registers
	OpCopy  Opcode = 27 // Copy value between registers
	OpSCopy Opcode = 28 // Shallow copy between registers
	OpCast  Opcode = 29 // Cast value to type

	// Cursor operations
	OpOpenRead      Opcode = 30 // Open cursor for reading
	OpOpenWrite     Opcode = 31 // Open cursor for writing
	OpOpenAutoindex Opcode = 32 // Open auto-index cursor
	OpOpenEphemeral Opcode = 33 // Open ephemeral table
	OpClose         Opcode = 34 // Close cursor
	OpRewind        Opcode = 35 // Move cursor to first entry
	OpNext          Opcode = 36 // Move cursor to next entry
	OpPrev          Opcode = 37 // Move cursor to previous entry
	OpSeek          Opcode = 38 // Seek to specific key
	OpSeekGE        Opcode = 39 // Seek greater than or equal
	OpSeekGT        Opcode = 40 // Seek greater than
	OpSeekLE        Opcode = 41 // Seek less than or equal
	OpSeekLT        Opcode = 42 // Seek less than
	OpSeekRowid     Opcode = 43 // Seek to rowid
	OpNotExists     Opcode = 44 // Check if rowid exists
	OpSequence      Opcode = 45 // Get cursor sequence number

	// Data retrieval opcodes
	OpColumn     Opcode = 46 // Read column from cursor
	OpRowid      Opcode = 47 // Get rowid from cursor
	OpRowData    Opcode = 48 // Get entire row data
	OpResultRow  Opcode = 49 // Output result row
	OpMakeRecord Opcode = 50 // Create a record from registers
	OpCount      Opcode = 51 // Count entries in cursor

	// Data modification opcodes
	OpInsert     Opcode = 52 // Insert record into table
	OpInsertInt  Opcode = 53 // Insert with integer key
	OpDelete     Opcode = 54 // Delete current record
	OpUpdate     Opcode = 55 // Update current record
	OpNewRowid   Opcode = 56 // Generate new rowid
	OpRowSetAdd  Opcode = 57 // Add rowid to rowset
	OpRowSetRead Opcode = 58 // Read rowid from rowset
	OpRowSetTest Opcode = 59 // Test if rowid in rowset

	// Comparison opcodes
	OpEq      Opcode = 60 // Equal comparison
	OpNe      Opcode = 61 // Not equal comparison
	OpLt      Opcode = 62 // Less than comparison
	OpLe      Opcode = 63 // Less than or equal comparison
	OpGt      Opcode = 64 // Greater than comparison
	OpGe      Opcode = 65 // Greater than or equal comparison
	OpCompare Opcode = 66 // Compare two register ranges
	OpJump    Opcode = 67 // Three-way comparison jump
	OpElseEq  Opcode = 68 // Else-equal for optimization

	// Arithmetic opcodes
	OpAdd       Opcode = 69 // Addition
	OpSubtract  Opcode = 70 // Subtraction
	OpMultiply  Opcode = 71 // Multiplication
	OpDivide    Opcode = 72 // Division
	OpRemainder Opcode = 73 // Modulo/remainder

	// Bitwise opcodes
	OpBitAnd     Opcode = 74 // Bitwise AND
	OpBitOr      Opcode = 75 // Bitwise OR
	OpBitNot     Opcode = 76 // Bitwise NOT
	OpShiftLeft  Opcode = 77 // Left shift
	OpShiftRight Opcode = 78 // Right shift

	// Logical opcodes
	OpAnd Opcode = 79 // Logical AND
	OpOr  Opcode = 80 // Logical OR
	OpNot Opcode = 81 // Logical NOT

	// Aggregate function opcodes
	OpAggStep  Opcode = 82 // Execute aggregate step function
	OpAggFinal Opcode = 83 // Execute aggregate finalization
	OpAggValue Opcode = 84 // Get aggregate intermediate value

	// Scalar function opcodes
	OpFunction Opcode = 85 // Call scalar function
	OpPureFunc Opcode = 86 // Call pure function (cacheable)

	// Transaction opcodes
	OpTransaction Opcode = 87 // Begin transaction
	OpCommit      Opcode = 88 // Commit transaction
	OpRollback    Opcode = 89 // Rollback transaction
	OpSavepoint   Opcode = 90 // Create savepoint
	OpRelease     Opcode = 91 // Release savepoint

	// Schema opcodes
	OpVerifyCookie Opcode = 92 // Verify schema cookie
	OpSetCookie    Opcode = 93 // Set schema cookie
	OpOpenPseudo   Opcode = 94 // Open pseudo-cursor

	// Sorting opcodes
	OpSorterOpen   Opcode = 95  // Open sorter
	OpSorterInsert Opcode = 96  // Insert into sorter
	OpSorterNext   Opcode = 97  // Get next from sorter
	OpSorterSort   Opcode = 98  // Sort the sorter
	OpSorterData   Opcode = 99  // Get data from sorter
	OpSorterClose  Opcode = 100 // Close sorter

	// Program flow opcodes
	OpProgram       Opcode = 101 // Execute sub-program
	OpInitCoroutine Opcode = 102 // Initialize coroutine
	OpEndCoroutine  Opcode = 103 // End coroutine
	OpOnce          Opcode = 104 // Execute once guard
	OpBeginSubrtn   Opcode = 105 // Begin subroutine

	// Index opcodes
	OpIdxInsert Opcode = 106 // Insert into index
	OpIdxDelete Opcode = 107 // Delete from index
	OpIdxRowid  Opcode = 108 // Get rowid from index
	OpIdxLT     Opcode = 109 // Index less than
	OpIdxGE     Opcode = 110 // Index greater or equal
	OpIdxGT     Opcode = 111 // Index greater than
	OpIdxLE     Opcode = 112 // Index less or equal

	// Virtual table opcodes
	OpVOpen    Opcode = 113 // Open virtual table cursor
	OpVFilter  Opcode = 114 // Virtual table filter
	OpVColumn  Opcode = 115 // Read virtual table column
	OpVNext    Opcode = 116 // Next row in virtual table
	OpVCreate  Opcode = 117 // Create virtual table
	OpVDestroy Opcode = 118 // Destroy virtual table
	OpVUpdate  Opcode = 119 // Update virtual table
	OpVRename  Opcode = 120 // Rename virtual table

	// Miscellaneous opcodes
	OpNoop        Opcode = 121 // No operation
	OpExplain     Opcode = 122 // EXPLAIN opcode marker
	OpPermutation Opcode = 123 // Set permutation for comparison
	OpCollSeq     Opcode = 124 // Set collation sequence
	OpTableLock   Opcode = 125 // Lock table
	OpFkCheck     Opcode = 126 // Foreign key check
	OpFkCounter   Opcode = 127 // Foreign key counter
	OpIsNull      Opcode = 128 // Check if NULL
	OpNotNull     Opcode = 129 // Check if NOT NULL
	OpMaxPgcnt    Opcode = 130 // Set max page count
	OpMemMax      Opcode = 131 // Track maximum value
	OpAutocommit  Opcode = 132 // Set autocommit mode

	// Type checking opcodes
	OpIsType      Opcode = 133 // Check value type
	OpIsNumeric   Opcode = 134 // Check if numeric
	OpMustBeInt64 Opcode = 135 // Assert 64-bit integer

	// Additional cursor opcodes
	OpLast          Opcode = 136 // Move cursor to last entry
	OpFirst         Opcode = 137 // Move cursor to first entry (alias)
	OpSorterCompare Opcode = 138 // Compare sorter keys

	// String operations
	OpToText    Opcode = 139 // Convert to text
	OpToBlob    Opcode = 140 // Convert to blob
	OpToNumeric Opcode = 141 // Convert to numeric
	OpToInt     Opcode = 142 // Convert to integer
	OpToReal    Opcode = 143 // Convert to real

	// Debugging/profiling opcodes
	OpTrace      Opcode = 144 // Debug trace point
	OpScanStatus Opcode = 145 // Scan status tracking
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
	OpToReal:        "ToReal",
	OpTrace:         "Trace",
	OpScanStatus:    "ScanStatus",
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
