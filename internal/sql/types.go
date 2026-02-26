package sql

// This file defines the core types and structures used in SQL compilation.

// Parse represents the parser and code generator context.
type Parse struct {
	DB          *Database // Database connection
	Vdbe        *Vdbe     // The VDBE being generated
	Mem         int       // Next available register
	Tabs        int       // Next available cursor
	Errs        int       // Number of errors
	ColNamesSet bool      // Column names already set
	Explain     int       // Explain mode
	NSelect     int       // SELECT statement counter
}

// GetVdbe returns the VDBE for this parse context.
func (p *Parse) GetVdbe() *Vdbe {
	if p.Vdbe == nil {
		p.Vdbe = NewVdbe(p.DB)
	}
	return p.Vdbe
}

// AllocReg allocates a single register.
func (p *Parse) AllocReg() int {
	p.Mem++
	return p.Mem
}

// AllocRegs allocates multiple consecutive registers.
func (p *Parse) AllocRegs(n int) int {
	base := p.Mem + 1
	p.Mem += n
	return base
}

// ReleaseReg releases a register (no-op in simple implementation).
func (p *Parse) ReleaseReg(reg int) {
	// In a more sophisticated implementation, this would track register usage
}

// ReleaseRegs releases multiple registers.
func (p *Parse) ReleaseRegs(base int, n int) {
	// In a more sophisticated implementation, this would track register usage
}

// AllocCursor allocates a cursor number.
func (p *Parse) AllocCursor() int {
	p.Tabs++
	return p.Tabs
}

// Select represents a SELECT statement.
type Select struct {
	Op       int       // Operator (TK_SELECT, TK_UNION, etc)
	SelFlags uint32    // SF_* flags
	EList    *ExprList // Result columns
	Src      *SrcList  // FROM clause
	Where    *Expr     // WHERE clause
	GroupBy  *ExprList // GROUP BY clause
	Having   *Expr     // HAVING clause
	OrderBy  *ExprList // ORDER BY clause
	Prior    *Select   // Prior SELECT for compound queries
	Next     *Select   // Next SELECT in list
	Limit    int       // LIMIT value (0 = none)
	Offset   int       // OFFSET value (0 = none)
	SelectID int       // Unique identifier
}

// SELECT flags
const (
	SF_Distinct      = 0x0001 // Output should be DISTINCT
	SF_Resolved      = 0x0002 // Identifiers have been resolved
	SF_Aggregate     = 0x0004 // Contains aggregate functions
	SF_UsesEphemeral = 0x0008 // Uses ephemeral table
	SF_Expanded      = 0x0010 // sqlite3SelectExpand() called
	SF_HasTypeInfo   = 0x0020 // Has column type info
	SF_Compound      = 0x0040 // Part of compound query
)

// Token operators
const (
	TK_SELECT = iota + 100
	TK_UNION
	TK_UNION_ALL
	TK_INTERSECT
	TK_EXCEPT
	TK_COLUMN
	TK_INTEGER
	TK_FLOAT
	TK_STRING
	TK_BLOB
	TK_NULL
	TK_ID
	TK_ASTERISK
	TK_DOT
	TK_FUNCTION
	TK_AGG_FUNCTION
	TK_CAST
	TK_EQ
	TK_NE
	TK_LT
	TK_LE
	TK_GT
	TK_GE
	TK_AND
	TK_OR
	TK_NOT
	TK_PLUS
	TK_MINUS
	TK_STAR
	TK_SLASH
	TK_REM
)

// Expr represents an expression node.
type Expr struct {
	Op          int       // Operation (TK_*)
	Left        *Expr     // Left operand
	Right       *Expr     // Right operand
	List        *ExprList // Function arguments
	Table       int       // Table cursor for TK_COLUMN
	Column      int       // Column index for TK_COLUMN
	IntValue    int       // Integer value for TK_INTEGER
	FloatValue  float64   // Float value for TK_FLOAT
	StringValue string    // String value for TK_STRING, TK_ID
	ColumnRef   *Column   // Reference to column definition
	FuncDef     *FuncDef  // Function definition for TK_FUNCTION
}

// SrcList represents a list of tables (FROM clause).
type SrcList struct {
	Items []SrcListItem
}

// NewSrcList creates a new source list.
func NewSrcList() *SrcList {
	return &SrcList{
		Items: make([]SrcListItem, 0),
	}
}

// Len returns the number of items.
func (sl *SrcList) Len() int {
	if sl == nil {
		return 0
	}
	return len(sl.Items)
}

// Get returns the item at the given index.
func (sl *SrcList) Get(idx int) *SrcListItem {
	if sl == nil || idx < 0 || idx >= len(sl.Items) {
		return nil
	}
	return &sl.Items[idx]
}

// Append adds an item to the list.
func (sl *SrcList) Append(item SrcListItem) {
	if sl != nil {
		sl.Items = append(sl.Items, item)
	}
}

// SrcListItem represents one table in the FROM clause.
type SrcListItem struct {
	Database      string  // Database name (if qualified)
	Name          string  // Table name
	Alias         string  // Alias name
	Table         *Table  // Resolved table definition
	Select        *Select // SELECT for subquery
	Cursor        int     // Cursor number for this table
	AddrFillIndex int     // Address of OP_Rewind (for patching)
	RegReturn     int     // Register for return address (coroutine)
	RegResult     int     // Register for result data (coroutine)
}

// Table represents a database table.
type Table struct {
	Name        string   // Table name
	NumColumns  int      // Number of columns
	Columns     []Column // Column definitions
	RootPage    int      // Root page in database file
	PrimaryKey  int      // Index of PRIMARY KEY column (-1 if none)
	RowidColumn int      // Column that is the rowid (-1 if none)
}

// GetColumn returns the column at the given index.
func (t *Table) GetColumn(idx int) *Column {
	if t == nil || idx < 0 || idx >= len(t.Columns) {
		return nil
	}
	return &t.Columns[idx]
}

// Column represents a table column.
type Column struct {
	Name         string   // Column name
	DeclType     string   // Declared type (TEXT, INTEGER, etc)
	Affinity     Affinity // Type affinity
	NotNull      bool     // NOT NULL constraint
	PrimaryKey   bool     // PRIMARY KEY constraint
	DefaultValue *Expr    // DEFAULT value
}

// Affinity represents type affinity.
type Affinity int

const (
	SQLITE_AFF_NONE    Affinity = 0 // No affinity
	SQLITE_AFF_BLOB    Affinity = 1 // BLOB affinity
	SQLITE_AFF_TEXT    Affinity = 2 // TEXT affinity
	SQLITE_AFF_NUMERIC Affinity = 3 // NUMERIC affinity
	SQLITE_AFF_INTEGER Affinity = 4 // INTEGER affinity
	SQLITE_AFF_REAL    Affinity = 5 // REAL affinity
	SQLITE_AFF_FLEXNUM Affinity = 6 // Flexible numeric
)

// FuncDef represents a function definition.
type FuncDef struct {
	Name      string                                     // Function name
	NumArgs   int                                        // Number of arguments (-1 = variable)
	FuncFlags int                                        // Function flags
	UserData  interface{}                                // User data
	Next      *FuncDef                                   // Next overload
	Func      func(ctx *FuncContext, args []interface{}) // Implementation
}

// FuncContext represents function execution context.
type FuncContext struct {
	Result interface{} // Result value
}

// Database represents a database connection.
type Database struct {
	Name string
	// Other fields...
}

// Vdbe represents the virtual database engine.
type Vdbe struct {
	DB       *Database // Database connection
	Ops      []VdbeOp  // Program instructions
	NumCols  int       // Number of result columns
	ColNames []string  // Column names
	ColTypes []string  // Column declared types
}

// NewVdbe creates a new VDBE.
func NewVdbe(db *Database) *Vdbe {
	return &Vdbe{
		DB:  db,
		Ops: make([]VdbeOp, 0, 256),
	}
}

// VdbeOp represents a single VDBE instruction.
type VdbeOp struct {
	Opcode  Opcode      // Operation code
	P1      int         // First parameter
	P2      int         // Second parameter
	P3      int         // Third parameter
	P4      interface{} // Fourth parameter (various types)
	P5      uint8       // Fifth parameter (flags)
	Comment string      // Comment for debugging
}

// Opcode represents a VDBE opcode.
type Opcode int

// VDBE opcodes used in SELECT compilation
const (
	OP_Init Opcode = iota + 1
	OP_Halt
	OP_OpenRead
	OP_OpenWrite
	OP_OpenEphemeral
	OP_SorterOpen
	OP_Close
	OP_Rewind
	OP_Next
	OP_Column
	OP_ResultRow
	OP_MakeRecord
	OP_Insert
	OP_NewRowid
	OP_IdxInsert
	OP_IdxDelete
	OP_Sequence
	OP_Integer
	OP_String8
	OP_Null
	OP_Copy
	OP_Move
	OP_Add
	OP_Subtract
	OP_Multiply
	OP_Divide
	OP_Remainder
	OP_Concat
	OP_Ne
	OP_Eq
	OP_Lt
	OP_Le
	OP_Gt
	OP_Ge
	OP_And
	OP_Or
	OP_Not
	OP_IsNull
	OP_NotNull
	OP_IfNot
	OP_IfPos
	OP_If
	OP_Goto
	OP_Gosub
	OP_Return
	OP_Yield
	OP_Once
	OP_AddImm
	OP_MustBeInt
	OP_Compare
	OP_Jump
	OP_Found
	OP_NotFound
	OP_SeekRowid
	OP_SeekGE
	OP_SeekGT
	OP_SeekLE
	OP_SeekLT
	OP_IdxLE
	OP_Sort
	OP_SorterSort
	OP_SorterData
	OP_SorterNext
	OP_SorterInsert
	OP_OpenPseudo
	OP_NullRow
	OP_FilterAdd
)

// VDBE methods for adding instructions

// AddOp2 adds a 2-parameter instruction.
func (v *Vdbe) AddOp2(opcode Opcode, p1 int, p2 int) int {
	addr := len(v.Ops)
	v.Ops = append(v.Ops, VdbeOp{
		Opcode: opcode,
		P1:     p1,
		P2:     p2,
	})
	return addr
}

// AddOp3 adds a 3-parameter instruction.
func (v *Vdbe) AddOp3(opcode Opcode, p1 int, p2 int, p3 int) int {
	addr := len(v.Ops)
	v.Ops = append(v.Ops, VdbeOp{
		Opcode: opcode,
		P1:     p1,
		P2:     p2,
		P3:     p3,
	})
	return addr
}

// AddOp4 adds a 4-parameter instruction.
func (v *Vdbe) AddOp4(opcode Opcode, p1 int, p2 int, p3 int, p4 interface{}) int {
	addr := len(v.Ops)
	v.Ops = append(v.Ops, VdbeOp{
		Opcode: opcode,
		P1:     p1,
		P2:     p2,
		P3:     p3,
		P4:     p4,
	})
	return addr
}

// AddOp4Int adds an instruction with integer P4.
func (v *Vdbe) AddOp4Int(opcode Opcode, p1 int, p2 int, p3 int, p4 int) int {
	return v.AddOp4(opcode, p1, p2, p3, p4)
}

// AddOp1 adds a 1-parameter instruction.
func (v *Vdbe) AddOp1(opcode Opcode, p1 int) int {
	return v.AddOp2(opcode, p1, 0)
}

// MakeLabel creates a new label.
func (v *Vdbe) MakeLabel() int {
	return -1 - len(v.Ops)
}

// ResolveLabel resolves a label to the current address.
func (v *Vdbe) ResolveLabel(label int) {
	if label < 0 {
		_ = -1 - label // addr - reserved for future use
		// Patch all instructions that jump to this label
		currentAddr := len(v.Ops)
		for i := 0; i < len(v.Ops); i++ {
			op := &v.Ops[i]
			if op.P2 == label {
				op.P2 = currentAddr
			}
		}
	}
}

// CurrentAddr returns the address of the next instruction.
func (v *Vdbe) CurrentAddr() int {
	return len(v.Ops)
}

// SetNumCols sets the number of result columns.
func (v *Vdbe) SetNumCols(n int) {
	v.NumCols = n
	v.ColNames = make([]string, n)
	v.ColTypes = make([]string, n)
}

// SetColName sets a column name.
func (v *Vdbe) SetColName(idx int, name string) {
	if idx >= 0 && idx < len(v.ColNames) {
		v.ColNames[idx] = name
	}
}

// SetColDeclType sets a column's declared type.
func (v *Vdbe) SetColDeclType(idx int, declType string) {
	if idx >= 0 && idx < len(v.ColTypes) {
		v.ColTypes[idx] = declType
	}
}

// ChangeP2 changes the P2 parameter of an instruction.
func (v *Vdbe) ChangeP2(addr int, p2 int) {
	if addr >= 0 && addr < len(v.Ops) {
		v.Ops[addr].P2 = p2
	}
}

// ChangeP5 changes the P5 parameter of the last instruction.
func (v *Vdbe) ChangeP5(p5 uint8) {
	if len(v.Ops) > 0 {
		v.Ops[len(v.Ops)-1].P5 = p5
	}
}

// GetOp returns an instruction at the given address.
func (v *Vdbe) GetOp(addr int) *VdbeOp {
	if addr >= 0 && addr < len(v.Ops) {
		return &v.Ops[addr]
	}
	return nil
}

// Comment adds a comment to the last instruction.
func (v *Vdbe) Comment(comment string) {
	if len(v.Ops) > 0 {
		v.Ops[len(v.Ops)-1].Comment = comment
	}
}
