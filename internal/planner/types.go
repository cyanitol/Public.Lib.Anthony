// Package planner implements the SQLite query planner and optimizer.
// This is a pure Go implementation based on SQLite's query planning algorithms.
//
// The query planner analyzes WHERE clauses, identifies usable indexes,
// estimates costs for different access paths, and chooses the optimal
// execution plan.
package planner

import (
	"fmt"
)

// LogEst represents a logarithmic estimate of a quantity.
// It is stored as 10 times the base-2 logarithm of the actual value.
// For example, LogEst(100) means approximately 2^10 = 1024 items.
type LogEst int16

// NewLogEst creates a LogEst from an actual count.
func NewLogEst(n int64) LogEst {
	if n <= 0 {
		return 0
	}
	// Compute log2(n) * 10
	x := 0
	for i := n; i > 1; i >>= 1 {
		x += 10
	}
	return LogEst(x)
}

// ToInt converts a LogEst back to an approximate integer value.
func (e LogEst) ToInt() int64 {
	if e <= 0 {
		return 1
	}
	// 2^(e/10)
	return 1 << (int(e) / 10)
}

// Add adds two LogEst values (multiplication in linear space).
func (e LogEst) Add(other LogEst) LogEst {
	return e + other
}

// Subtract subtracts two LogEst values (division in linear space).
func (e LogEst) Subtract(other LogEst) LogEst {
	return e - other
}

// Bitmask represents a set of tables/cursors as a bit vector.
// Each bit position corresponds to a table in the FROM clause.
type Bitmask uint64

// Set sets the bit for the given cursor/table index.
func (b *Bitmask) Set(index int) {
	*b |= Bitmask(1 << uint(index))
}

// Has checks if the bit for the given cursor/table index is set.
func (b Bitmask) Has(index int) bool {
	return (b & (Bitmask(1 << uint(index)))) != 0
}

// HasAll checks if all bits in the mask are set.
func (b Bitmask) HasAll(mask Bitmask) bool {
	return (b & mask) == mask
}

// Overlaps checks if any bits overlap with the given mask.
func (b Bitmask) Overlaps(mask Bitmask) bool {
	return (b & mask) != 0
}

// WhereTerm represents a single term in a WHERE clause.
// Each term is typically of the form: column OP value
type WhereTerm struct {
	// Expr is the expression for this term
	Expr Expr

	// Operator is the comparison operator (WO_EQ, WO_LT, etc.)
	Operator WhereOperator

	// LeftCursor is the cursor number of the table on the left side
	LeftCursor int

	// LeftColumn is the column number on the left side (-1 for rowid)
	LeftColumn int

	// RightValue is the value or expression on the right side
	RightValue interface{}

	// PrereqRight is bitmask of tables used by RHS
	PrereqRight Bitmask

	// PrereqAll is bitmask of all tables referenced
	PrereqAll Bitmask

	// TruthProb is the estimated probability this term is true (LogEst)
	TruthProb LogEst

	// Flags contains various TERM_* flags
	Flags TermFlags

	// Parent is the index of parent term (for disabling)
	Parent int
}

// WhereOperator defines the type of comparison operator.
type WhereOperator uint16

const (
	WO_IN     WhereOperator = 0x0001 // IN operator
	WO_EQ     WhereOperator = 0x0002 // = operator
	WO_LT     WhereOperator = 0x0004 // < operator
	WO_LE     WhereOperator = 0x0008 // <= operator
	WO_GT     WhereOperator = 0x0010 // > operator
	WO_GE     WhereOperator = 0x0020 // >= operator
	WO_IS     WhereOperator = 0x0080 // IS operator
	WO_ISNULL WhereOperator = 0x0100 // IS NULL operator
	WO_OR     WhereOperator = 0x0200 // OR-connected terms
	WO_AND    WhereOperator = 0x0400 // AND-connected terms
	WO_EQUIV  WhereOperator = 0x0800 // Column equivalence (A=B)
	WO_NOOP   WhereOperator = 0x1000 // Term doesn't restrict search
)

// TermFlags defines flags for WhereTerm objects.
type TermFlags uint16

const (
	TERM_DYNAMIC   TermFlags = 0x0001 // Expression needs cleanup
	TERM_VIRTUAL   TermFlags = 0x0002 // Added by optimizer, don't code
	TERM_CODED     TermFlags = 0x0004 // This term already coded
	TERM_COPIED    TermFlags = 0x0008 // Has a child term
	TERM_OK        TermFlags = 0x0040 // Used during OR-clause processing
	TERM_VNULL     TermFlags = 0x0080 // Manufactured x>NULL term
	TERM_LIKEOPT   TermFlags = 0x0100 // Virtual term from LIKE optimization
	TERM_LIKECOND  TermFlags = 0x0200 // Conditional LIKE operator
	TERM_LIKE      TermFlags = 0x0400 // Original LIKE operator
	TERM_IS        TermFlags = 0x0800 // Term.Expr is an IS operator
	TERM_HEURTRUTH TermFlags = 0x2000 // Heuristic truth probability used
)

// WhereClause represents a parsed WHERE clause with multiple terms.
type WhereClause struct {
	// Terms is the list of all WHERE terms (connected by AND)
	Terms []*WhereTerm

	// HasOr indicates if any term uses OR operator
	HasOr bool
}

// WhereLoop represents one possible algorithm for evaluating a join term.
// For each table in the FROM clause, there may be multiple WhereLoop
// objects representing different strategies (full scan, different indexes, etc.)
type WhereLoop struct {
	// Prereq is bitmask of other loops that must run first
	Prereq Bitmask

	// MaskSelf is bitmask identifying this table
	MaskSelf Bitmask

	// TabIndex is position in FROM clause of table for this loop
	TabIndex int

	// Setup is one-time setup cost (e.g., create temp index)
	Setup LogEst

	// Run is cost of running each loop iteration
	Run LogEst

	// NOut is estimated number of output rows
	NOut LogEst

	// Flags describes the access method (WHERE_* flags)
	Flags WhereFlags

	// Index info for btree access
	Index *IndexInfo

	// Terms is the list of WHERE terms used by this loop
	Terms []*WhereTerm

	// NextLoop points to next WhereLoop in the list
	NextLoop *WhereLoop
}

// WhereFlags defines flags describing the access path.
type WhereFlags uint32

const (
	WHERE_COLUMN_EQ    WhereFlags = 0x00000001 // x=EXPR
	WHERE_COLUMN_RANGE WhereFlags = 0x00000002 // x<EXPR and/or x>EXPR
	WHERE_COLUMN_IN    WhereFlags = 0x00000004 // x IN (...)
	WHERE_COLUMN_NULL  WhereFlags = 0x00000008 // x IS NULL
	WHERE_CONSTRAINT   WhereFlags = 0x0000000f // Any WHERE_COLUMN_xxx
	WHERE_TOP_LIMIT    WhereFlags = 0x00000010 // x<EXPR or x<=EXPR
	WHERE_BTM_LIMIT    WhereFlags = 0x00000020 // x>EXPR or x>=EXPR
	WHERE_BOTH_LIMIT   WhereFlags = 0x00000030 // Both x>EXPR and x<EXPR
	WHERE_IDX_ONLY     WhereFlags = 0x00000040 // Use index only (covering)
	WHERE_IPK          WhereFlags = 0x00000100 // x is INTEGER PRIMARY KEY
	WHERE_INDEXED      WhereFlags = 0x00000200 // Uses an index
	WHERE_VIRTUALTABLE WhereFlags = 0x00000400 // Virtual table
	WHERE_IN_ABLE      WhereFlags = 0x00000800 // Can support IN operator
	WHERE_ONEROW       WhereFlags = 0x00001000 // Selects at most one row
	WHERE_MULTI_OR     WhereFlags = 0x00002000 // OR using multiple indices
	WHERE_AUTO_INDEX   WhereFlags = 0x00004000 // Uses ephemeral index
	WHERE_SKIPSCAN     WhereFlags = 0x00008000 // Skip-scan algorithm
	WHERE_BLOOMFILTER  WhereFlags = 0x00400000 // Consider Bloom filter
)

// IndexInfo describes an index that can be used for a query.
type IndexInfo struct {
	// Name is the index name
	Name string

	// Table is the name of the table
	Table string

	// Columns are the indexed columns in order
	Columns []IndexColumn

	// Unique indicates if this is a unique index
	Unique bool

	// Primary indicates if this is the primary key
	Primary bool

	// RowCount is estimated total rows in the table
	RowCount int64

	// RowLogEst is LogEst of RowCount
	RowLogEst LogEst

	// ColumnStats contains per-column cardinality estimates
	// ColumnStats[i] is the estimated distinct values for the
	// first i+1 columns of the index
	ColumnStats []LogEst
}

// IndexColumn describes a single column in an index.
type IndexColumn struct {
	// Name is the column name
	Name string

	// Index is the column index in the table (-1 for rowid)
	Index int

	// Ascending is true for ASC order, false for DESC
	Ascending bool

	// Collation is the collation sequence name (e.g., "BINARY", "NOCASE")
	Collation string
}

// WherePath represents a complete or partial query plan.
// It is a sequence of WhereLoop objects, one for each table.
type WherePath struct {
	// MaskLoop is bitmask of all WhereLoop objects in this path
	MaskLoop Bitmask

	// NRow is estimated number of rows generated
	NRow LogEst

	// Cost is total cost of this path
	Cost LogEst

	// Loops is array of WhereLoop objects implementing this path
	Loops []*WhereLoop
}

// WhereInfo holds the complete state of the query planner.
type WhereInfo struct {
	// Clause is the parsed WHERE clause
	Clause *WhereClause

	// Tables is the list of tables in the FROM clause
	Tables []*TableInfo

	// AllLoops is list of all generated WhereLoop objects
	AllLoops []*WhereLoop

	// BestPath is the chosen optimal path
	BestPath *WherePath

	// NOut is estimated number of output rows
	NOut LogEst
}

// TableInfo describes a table in the FROM clause.
type TableInfo struct {
	// Name is the table name
	Name string

	// Alias is the table alias (if any)
	Alias string

	// Cursor is the VDBE cursor number for this table
	Cursor int

	// RowCount is estimated number of rows
	RowCount int64

	// RowLogEst is LogEst of RowCount
	RowLogEst LogEst

	// Columns describes the table columns
	Columns []ColumnInfo

	// Indexes are all available indexes on this table
	Indexes []*IndexInfo

	// PrimaryKey is the primary key index (may be nil)
	PrimaryKey *IndexInfo
}

// ColumnInfo describes a single column in a table.
type ColumnInfo struct {
	// Name is the column name
	Name string

	// Index is the column position in the table
	Index int

	// Type is the column type (INTEGER, TEXT, etc.)
	Type string

	// NotNull indicates if the column has a NOT NULL constraint
	NotNull bool

	// DefaultValue is the default value (if any)
	DefaultValue interface{}
}

// Expr represents a SQL expression in the WHERE clause.
type Expr interface {
	// String returns a string representation
	String() string

	// UsedTables returns bitmask of tables referenced
	UsedTables() Bitmask
}

// BinaryExpr represents a binary operation (e.g., column = value).
type BinaryExpr struct {
	Op    string // Operator: "=", "<", ">", "<=", ">=", "IN", "IS", "IS NULL"
	Left  Expr   // Left operand (usually a column reference)
	Right Expr   // Right operand (usually a value or another column)
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Op, e.Right.String())
}

func (e *BinaryExpr) UsedTables() Bitmask {
	return e.Left.UsedTables() | e.Right.UsedTables()
}

// ColumnExpr represents a column reference.
type ColumnExpr struct {
	Table  string // Table name or alias
	Column string // Column name
	Cursor int    // Cursor number for this table
}

func (e *ColumnExpr) String() string {
	if e.Table != "" {
		return fmt.Sprintf("%s.%s", e.Table, e.Column)
	}
	return e.Column
}

func (e *ColumnExpr) UsedTables() Bitmask {
	var mask Bitmask
	if e.Cursor >= 0 {
		mask.Set(e.Cursor)
	}
	return mask
}

// ValueExpr represents a constant value.
type ValueExpr struct {
	Value interface{} // The constant value
}

func (e *ValueExpr) String() string {
	return fmt.Sprintf("%v", e.Value)
}

func (e *ValueExpr) UsedTables() Bitmask {
	return 0 // Constants don't reference tables
}

// AndExpr represents an AND of multiple expressions.
type AndExpr struct {
	Terms []Expr
}

func (e *AndExpr) String() string {
	s := "("
	for i, term := range e.Terms {
		if i > 0 {
			s += " AND "
		}
		s += term.String()
	}
	s += ")"
	return s
}

func (e *AndExpr) UsedTables() Bitmask {
	var mask Bitmask
	for _, term := range e.Terms {
		mask |= term.UsedTables()
	}
	return mask
}

// OrExpr represents an OR of multiple expressions.
type OrExpr struct {
	Terms []Expr
}

func (e *OrExpr) String() string {
	s := "("
	for i, term := range e.Terms {
		if i > 0 {
			s += " OR "
		}
		s += term.String()
	}
	s += ")"
	return s
}

func (e *OrExpr) UsedTables() Bitmask {
	var mask Bitmask
	for _, term := range e.Terms {
		mask |= term.UsedTables()
	}
	return mask
}
