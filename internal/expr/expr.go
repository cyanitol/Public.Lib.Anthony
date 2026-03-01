// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package expr implements SQLite expression evaluation and code generation.
// This is a pure Go implementation based on SQLite's expr.c.
//
// Expressions form an Abstract Syntax Tree (AST) representing SQL operations.
// The code generator transforms these expressions into VDBE bytecode instructions.
package expr

import (
	"fmt"
	"strconv"
	"strings"
)

// OpCode represents an expression operation type.
// These correspond to SQLite's TK_* token types.
type OpCode uint8

const (
	// Literals
	OpNull     OpCode = iota // NULL literal
	OpInteger                // Integer literal
	OpFloat                  // Float literal
	OpString                 // String literal
	OpBlob                   // Blob literal
	OpVariable               // Bound parameter (?NNN or :AAA)

	// Column references
	OpColumn    // Column reference (table.column)
	OpAggColumn // Aggregate column reference

	// Binary operators
	OpPlus      // a + b
	OpMinus     // a - b
	OpMultiply  // a * b
	OpDivide    // a / b
	OpRemainder // a % b
	OpConcat    // a || b (string concatenation)
	OpBitAnd    // a & b
	OpBitOr     // a | b
	OpBitXor    // a ^ b
	OpLShift    // a << b
	OpRShift    // a >> b

	// Comparison operators
	OpEq      // a = b
	OpNe      // a != b or a <> b
	OpLt      // a < b
	OpLe      // a <= b
	OpGt      // a > b
	OpGe      // a >= b
	OpIs      // a IS b
	OpIsNot   // a IS NOT b
	OpIsNull  // a IS NULL
	OpNotNull // a IS NOT NULL

	// Logical operators
	OpAnd // a AND b
	OpOr  // a OR b
	OpNot // NOT a

	// Unary operators
	OpNegate    // -a
	OpBitNot    // ~a
	OpUnaryPlus // +a

	// Pattern matching
	OpLike   // a LIKE b [ESCAPE c]
	OpGlob   // a GLOB b
	OpRegexp // a REGEXP b

	// Range operators
	OpIn         // a IN (...)
	OpNotIn      // a NOT IN (...)
	OpBetween    // a BETWEEN b AND c
	OpNotBetween // a NOT BETWEEN b AND c

	// Special operators
	OpCase     // CASE WHEN ... THEN ... ELSE ... END
	OpCast     // CAST(expr AS type)
	OpCollate  // expr COLLATE collation
	OpFunction // Function call func(args...)
	OpAggFunc  // Aggregate function SUM(args...)
	OpExists   // EXISTS (subquery)
	OpSelect   // Scalar subquery (SELECT ...)
	OpVector   // Row value (a, b, c)

	// Other
	OpRegister     // Result stored in register
	OpIfNullRow    // Special handling for outer join
	OpSelectColumn // Column from subquery result
	OpError        // Parse error marker
)

// String returns the string representation of an OpCode.
func (op OpCode) String() string {
	names := map[OpCode]string{
		OpNull: "NULL", OpInteger: "INTEGER", OpFloat: "FLOAT",
		OpString: "STRING", OpBlob: "BLOB", OpVariable: "VARIABLE",
		OpColumn: "COLUMN", OpAggColumn: "AGG_COLUMN",
		OpPlus: "PLUS", OpMinus: "MINUS", OpMultiply: "MULTIPLY",
		OpDivide: "DIVIDE", OpRemainder: "REMAINDER", OpConcat: "CONCAT",
		OpBitAnd: "BITAND", OpBitOr: "BITOR", OpBitXor: "BITXOR",
		OpLShift: "LSHIFT", OpRShift: "RSHIFT",
		OpEq: "EQ", OpNe: "NE", OpLt: "LT", OpLe: "LE", OpGt: "GT", OpGe: "GE",
		OpIs: "IS", OpIsNot: "ISNOT", OpIsNull: "ISNULL", OpNotNull: "NOTNULL",
		OpAnd: "AND", OpOr: "OR", OpNot: "NOT",
		OpNegate: "NEGATE", OpBitNot: "BITNOT", OpUnaryPlus: "UPLUS",
		OpLike: "LIKE", OpGlob: "GLOB", OpRegexp: "REGEXP",
		OpIn: "IN", OpNotIn: "NOTIN", OpBetween: "BETWEEN", OpNotBetween: "NOTBETWEEN",
		OpCase: "CASE", OpCast: "CAST", OpCollate: "COLLATE",
		OpFunction: "FUNCTION", OpAggFunc: "AGG_FUNCTION",
		OpExists: "EXISTS", OpSelect: "SELECT", OpVector: "VECTOR",
		OpRegister: "REGISTER", OpIfNullRow: "IF_NULL_ROW",
		OpSelectColumn: "SELECT_COLUMN", OpError: "ERROR",
	}
	if name, ok := names[op]; ok {
		return name
	}
	return fmt.Sprintf("OpCode(%d)", op)
}

// ExprFlags defines flags for expression properties.
type ExprFlags uint32

const (
	// EP_OuterON - Originates in ON/USING clause of outer join
	EP_OuterON ExprFlags = 0x000001
	// EP_InnerON - Originates in ON/USING of an inner join
	EP_InnerON ExprFlags = 0x000002
	// EP_Distinct - Aggregate function with DISTINCT keyword
	EP_Distinct ExprFlags = 0x000004
	// EP_HasFunc - Contains one or more functions of any kind
	EP_HasFunc ExprFlags = 0x000008
	// EP_Agg - Contains one or more aggregate functions
	EP_Agg ExprFlags = 0x000010
	// EP_FixedCol - TK_Column with a known fixed value
	EP_FixedCol ExprFlags = 0x000020
	// EP_VarSelect - pSelect is correlated, not constant
	EP_VarSelect ExprFlags = 0x000040
	// EP_DblQuoted - token.z was originally in "..."
	EP_DblQuoted ExprFlags = 0x000080
	// EP_InfixFunc - True for an infix function: LIKE, GLOB, etc
	EP_InfixFunc ExprFlags = 0x000100
	// EP_Collate - Tree contains a TK_COLLATE operator
	EP_Collate ExprFlags = 0x000200
	// EP_Commuted - Comparison operator has been commuted
	EP_Commuted ExprFlags = 0x000400
	// EP_IntValue - Integer value contained in u.iValue
	EP_IntValue ExprFlags = 0x000800
	// EP_xIsSelect - x.pSelect is valid (otherwise x.pList is)
	EP_xIsSelect ExprFlags = 0x001000
	// EP_Skip - Operator does not contribute to affinity
	EP_Skip ExprFlags = 0x002000
	// EP_Reduced - Expr struct EXPR_REDUCEDSIZE bytes only
	EP_Reduced ExprFlags = 0x004000
	// EP_TokenOnly - Expr struct EXPR_TOKENONLYSIZE bytes only
	EP_TokenOnly ExprFlags = 0x010000
	// EP_FullSize - Expr structure must remain full sized
	EP_FullSize ExprFlags = 0x020000
	// EP_IfNullRow - The TK_IF_NULL_ROW opcode
	EP_IfNullRow ExprFlags = 0x040000
	// EP_Unlikely - unlikely() or likelihood() function
	EP_Unlikely ExprFlags = 0x080000
	// EP_ConstFunc - A SQLITE_FUNC_CONSTANT or _SLOCHNG function
	EP_ConstFunc ExprFlags = 0x100000
	// EP_CanBeNull - Can be null despite NOT NULL constraint
	EP_CanBeNull ExprFlags = 0x200000
	// EP_Subquery - Tree contains a TK_SELECT operator
	EP_Subquery ExprFlags = 0x400000
	// EP_Leaf - Expr.pLeft, .pRight, .u.pSelect all NULL
	EP_Leaf ExprFlags = 0x800000
	// EP_Subrtn - Uses Expr.y.sub. TK_IN, _SELECT, or _EXISTS
	EP_Subrtn ExprFlags = 0x2000000
	// EP_Quoted - TK_ID was originally quoted
	EP_Quoted ExprFlags = 0x4000000
	// EP_Static - Held in memory not obtained from malloc()
	EP_Static ExprFlags = 0x8000000
	// EP_IsTrue - Always has boolean value of TRUE
	EP_IsTrue ExprFlags = 0x10000000
	// EP_IsFalse - Always has boolean value of FALSE
	EP_IsFalse ExprFlags = 0x20000000
)

// Affinity represents SQLite type affinity.
type Affinity byte

const (
	AFF_NONE    Affinity = 0x00 // No affinity
	AFF_BLOB    Affinity = 0x41 // 'A' - BLOB affinity
	AFF_TEXT    Affinity = 0x42 // 'B' - TEXT affinity
	AFF_NUMERIC Affinity = 0x43 // 'C' - NUMERIC affinity
	AFF_INTEGER Affinity = 0x44 // 'D' - INTEGER affinity
	AFF_REAL    Affinity = 0x45 // 'E' - REAL affinity
)

// String returns the string representation of affinity.
func (a Affinity) String() string {
	switch a {
	case AFF_NONE:
		return "NONE"
	case AFF_BLOB:
		return "BLOB"
	case AFF_TEXT:
		return "TEXT"
	case AFF_NUMERIC:
		return "NUMERIC"
	case AFF_INTEGER:
		return "INTEGER"
	case AFF_REAL:
		return "REAL"
	default:
		return fmt.Sprintf("Affinity(%d)", a)
	}
}

// IsNumericAffinity checks if affinity is numeric.
func IsNumericAffinity(aff Affinity) bool {
	return aff >= AFF_NUMERIC
}

// Expr represents a node in the expression AST.
// This is the Go equivalent of SQLite's Expr struct.
type Expr struct {
	// Op is the operation performed by this node
	Op OpCode

	// Affinity is the type affinity for this expression
	Affinity Affinity

	// Flags contains various EP_* flags
	Flags ExprFlags

	// Token is the token value for literals, column names, etc.
	// For integers with EP_IntValue, this is empty.
	Token string

	// IntValue is used when EP_IntValue flag is set
	IntValue int64

	// FloatValue is used for floating point literals
	FloatValue float64

	// Left and Right are child expressions for binary operators
	Left  *Expr
	Right *Expr

	// List is used for function arguments, CASE expressions, IN lists, etc.
	// Valid when !(Flags & EP_xIsSelect)
	List *ExprList

	// Select is used for subquery expressions
	// Valid when (Flags & EP_xIsSelect)
	Select *SelectStmt

	// Table information for TK_COLUMN nodes
	Table      *TableRef // Table containing the column
	TableName  string    // Table name (for reference)
	ColumnName string    // Column name
	IColumn    int       // Column index (-1 for rowid)
	ITable     int       // Cursor number / table index

	// Collation sequence name for COLLATE expressions
	CollSeq string

	// Function name for TK_FUNCTION
	FuncName string

	// Height of expression tree (for depth checking)
	Height int

	// IAgg is the index into aggregation info
	IAgg int

	// IOp2 stores original op for TK_REGISTER nodes
	IOp2 OpCode

	// Sub contains subroutine info for IN/EXISTS/SELECT
	Sub struct {
		IAddr     int // Subroutine entry address
		RegReturn int // Register used to hold return address
	}
}

// ExprList represents a list of expressions.
// Used for function arguments, SELECT columns, ORDER BY, etc.
type ExprList struct {
	Items []*ExprListItem
}

// ExprListItem is a single item in an expression list.
type ExprListItem struct {
	Expr  *Expr
	Name  string // AS name or column name
	Alias string // Alias for this expression

	// Flags
	SortFlags  uint8  // KEYINFO_ORDER_* flags for ORDER BY
	Done       bool   // Processing finished
	Reusable   bool   // Constant expression is reusable
	SorterRef  bool   // Defer evaluation until after sorting
	Nulls      bool   // Explicit NULLS FIRST/LAST
	OrderByCol uint16 // For ORDER BY, column number in result
}

// SelectStmt represents a SELECT statement (simplified).
// This is referenced by subquery expressions.
type SelectStmt struct {
	SelectID int         // Unique ID for this SELECT
	Columns  *ExprList   // SELECT columns
	From     *TableRef   // FROM clause
	Where    *Expr       // WHERE clause
	GroupBy  *ExprList   // GROUP BY clause
	Having   *Expr       // HAVING clause
	OrderBy  *ExprList   // ORDER BY clause
	Limit    *Expr       // LIMIT expression
	Offset   *Expr       // OFFSET expression
	Flags    SelectFlags // SF_* flags
}

// SelectFlags defines flags for SELECT statements.
type SelectFlags uint32

const (
	SF_Distinct      SelectFlags = 0x0001 // DISTINCT keyword
	SF_All           SelectFlags = 0x0002 // ALL keyword
	SF_Resolved      SelectFlags = 0x0004 // Names have been resolved
	SF_Aggregate     SelectFlags = 0x0008 // Contains aggregate functions
	SF_HasAgg        SelectFlags = 0x0010 // Contains agg funcs or GROUP BY
	SF_UsesEphemeral SelectFlags = 0x0020 // Uses ephemeral table
)

// TableRef represents a table reference in a query.
type TableRef struct {
	Name   string // Table name
	Alias  string // Table alias
	Cursor int    // VDBE cursor number
}

// NewIntExpr creates an integer literal expression.
func NewIntExpr(val int64) *Expr {
	return &Expr{
		Op:       OpInteger,
		Flags:    EP_IntValue | EP_Leaf,
		IntValue: val,
		Height:   1,
		IAgg:     -1,
	}
}

// NewFloatExpr creates a float literal expression.
func NewFloatExpr(val float64) *Expr {
	return &Expr{
		Op:         OpFloat,
		Flags:      EP_Leaf,
		FloatValue: val,
		Height:     1,
		IAgg:       -1,
	}
}

// NewStringExpr creates a string literal expression.
func NewStringExpr(val string) *Expr {
	return &Expr{
		Op:     OpString,
		Flags:  EP_Leaf,
		Token:  val,
		Height: 1,
		IAgg:   -1,
	}
}

// NewNullExpr creates a NULL literal expression.
func NewNullExpr() *Expr {
	return &Expr{
		Op:     OpNull,
		Flags:  EP_Leaf,
		Height: 1,
		IAgg:   -1,
	}
}

// NewColumnExpr creates a column reference expression.
func NewColumnExpr(table, column string, cursor, colIndex int) *Expr {
	return &Expr{
		Op:         OpColumn,
		Flags:      EP_Leaf,
		TableName:  table,
		ColumnName: column,
		ITable:     cursor,
		IColumn:    colIndex,
		Height:     1,
		IAgg:       -1,
	}
}

// NewBinaryExpr creates a binary operation expression.
func NewBinaryExpr(op OpCode, left, right *Expr) *Expr {
	expr := &Expr{
		Op:     op,
		Left:   left,
		Right:  right,
		Height: 1,
		IAgg:   -1,
	}
	expr.updateHeight()
	return expr
}

// NewUnaryExpr creates a unary operation expression.
func NewUnaryExpr(op OpCode, operand *Expr) *Expr {
	expr := &Expr{
		Op:     op,
		Left:   operand,
		Height: 1,
		IAgg:   -1,
	}
	expr.updateHeight()
	return expr
}

// NewFunctionExpr creates a function call expression.
func NewFunctionExpr(name string, args *ExprList) *Expr {
	expr := &Expr{
		Op:       OpFunction,
		FuncName: name,
		List:     args,
		Height:   1,
		IAgg:     -1,
	}
	expr.updateHeight()
	return expr
}

// updateHeight updates the height of the expression tree.
func (e *Expr) updateHeight() {
	if e == nil {
		return
	}
	height := e.maxChildHeight()
	e.Height = height + 1
}

// maxChildHeight returns the maximum height among child expressions.
func (e *Expr) maxChildHeight() int {
	height := 0
	if e.Left != nil && e.Left.Height > height {
		height = e.Left.Height
	}
	if e.Right != nil && e.Right.Height > height {
		height = e.Right.Height
	}
	height = maxListHeight(e.List, height)
	return height
}

// maxListHeight returns the maximum height in an expression list.
func maxListHeight(list *ExprList, current int) int {
	if list == nil {
		return current
	}
	for _, item := range list.Items {
		if item.Expr != nil && item.Expr.Height > current {
			current = item.Expr.Height
		}
	}
	return current
}

// HasProperty checks if the expression has the given flags.
func (e *Expr) HasProperty(flags ExprFlags) bool {
	return e != nil && (e.Flags&flags) != 0
}

// SetProperty sets the given flags on the expression.
func (e *Expr) SetProperty(flags ExprFlags) {
	if e != nil {
		e.Flags |= flags
	}
}

// ClearProperty clears the given flags on the expression.
func (e *Expr) ClearProperty(flags ExprFlags) {
	if e != nil {
		e.Flags &^= flags
	}
}

// isConstantLiteralOp returns true for literal value operators.
var isConstantLiteralOp = map[OpCode]bool{
	OpNull: true, OpInteger: true, OpFloat: true, OpString: true, OpBlob: true,
}

// isNonConstantOp returns true for operators that are never constant.
var isNonConstantOp = map[OpCode]bool{
	OpColumn: true, OpAggColumn: true, OpVariable: true,
}

// isUnaryOp returns true for unary operators.
var isUnaryOp = map[OpCode]bool{
	OpNegate: true, OpNot: true, OpBitNot: true, OpUnaryPlus: true,
}

// isBinaryOp returns true for binary operators.
var isBinaryOp = map[OpCode]bool{
	OpPlus: true, OpMinus: true, OpMultiply: true, OpDivide: true, OpRemainder: true,
	OpConcat: true, OpBitAnd: true, OpBitOr: true, OpBitXor: true, OpLShift: true, OpRShift: true,
	OpEq: true, OpNe: true, OpLt: true, OpLe: true, OpGt: true, OpGe: true, OpAnd: true, OpOr: true,
}

// IsConstant checks if the expression is a constant (does not reference tables).
func (e *Expr) IsConstant() bool {
	if e == nil {
		return true
	}
	if isConstantLiteralOp[e.Op] {
		return true
	}
	if isNonConstantOp[e.Op] {
		return false
	}
	if isUnaryOp[e.Op] {
		return e.Left.IsConstant()
	}
	if isBinaryOp[e.Op] {
		return e.Left.IsConstant() && e.Right.IsConstant()
	}
	if e.Op == OpFunction {
		return e.isFunctionConstant()
	}
	return false
}

func (e *Expr) isFunctionConstant() bool {
	if e.List != nil {
		for _, item := range e.List.Items {
			if !item.Expr.IsConstant() {
				return false
			}
		}
	}
	return !e.HasProperty(EP_HasFunc | EP_VarSelect)
}

// exprUnaryPrefix maps unary OpCodes to their prefix symbols.
// Ops in this map produce "(symbol operand)".
var exprUnaryPrefix = map[OpCode]string{
	OpNegate:    "-",
	OpBitNot:    "~",
	OpUnaryPlus: "+",
}

// exprUnaryKeyword maps unary OpCodes to keyword-style prefix strings.
// Ops in this map produce "(KEYWORD operand)".
var exprUnaryKeyword = map[OpCode]string{
	OpNot: "NOT",
}

// exprUnaryPostfix maps unary OpCodes to their postfix SQL phrases.
// Ops in this map produce "(operand PHRASE)".
var exprUnaryPostfix = map[OpCode]string{
	OpIsNull:  "IS NULL",
	OpNotNull: "IS NOT NULL",
}

// exprBinaryInfix maps binary OpCodes to their infix operator strings.
var exprBinaryInfix = map[OpCode]string{
	OpPlus:      "+",
	OpMinus:     "-",
	OpMultiply:  "*",
	OpDivide:    "/",
	OpRemainder: "%",
	OpConcat:    "||",
	OpBitAnd:    "&",
	OpBitOr:     "|",
	OpBitXor:    "^",
	OpLShift:    "<<",
	OpRShift:    ">>",
	OpEq:        "=",
	OpNe:        "!=",
	OpLt:        "<",
	OpLe:        "<=",
	OpGt:        ">",
	OpGe:        ">=",
	OpIs:        "IS",
	OpIsNot:     "IS NOT",
	OpAnd:       "AND",
	OpOr:        "OR",
	OpLike:      "LIKE",
	OpGlob:      "GLOB",
	OpRegexp:    "REGEXP",
}

// stringLiteral handles literal OpCodes: NULL, INTEGER, FLOAT, STRING, BLOB, VARIABLE.
func (e *Expr) stringLiteral() string {
	switch e.Op {
	case OpNull:
		return "NULL"
	case OpInteger:
		if e.HasProperty(EP_IntValue) {
			return strconv.FormatInt(e.IntValue, 10)
		}
		return e.Token
	case OpFloat:
		return fmt.Sprintf("%g", e.FloatValue)
	case OpString:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(e.Token, "'", "''"))
	case OpBlob:
		return fmt.Sprintf("x'%s'", e.Token)
	default: // OpVariable
		return e.Token
	}
}

// stringColumn handles OpColumn: returns "table.column" or just "column".
func (e *Expr) stringColumn() string {
	if e.TableName != "" {
		return fmt.Sprintf("%s.%s", e.TableName, e.ColumnName)
	}
	return e.ColumnName
}

// stringUnary handles all unary operator expressions using the lookup tables.
func (e *Expr) stringUnary() string {
	if sym, ok := exprUnaryPrefix[e.Op]; ok {
		return fmt.Sprintf("(%s%s)", sym, e.Left.String())
	}
	if kw, ok := exprUnaryKeyword[e.Op]; ok {
		return fmt.Sprintf("(%s %s)", kw, e.Left.String())
	}
	// postfix unary (IS NULL, IS NOT NULL)
	phrase := exprUnaryPostfix[e.Op]
	return fmt.Sprintf("(%s %s)", e.Left.String(), phrase)
}

// stringBinary handles all binary infix operator expressions using the lookup table.
func (e *Expr) stringBinary() string {
	sym := exprBinaryInfix[e.Op]
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), sym, e.Right.String())
}

// stringFunction handles OpFunction: "name(arg1, arg2, ...)".
func (e *Expr) stringFunction() string {
	var args []string
	if e.List != nil {
		for _, item := range e.List.Items {
			args = append(args, item.Expr.String())
		}
	}
	return fmt.Sprintf("%s(%s)", e.FuncName, strings.Join(args, ", "))
}

// exprStringHandlers is a dispatch table mapping each OpCode to the method
// that produces its string representation.
var exprStringHandlers map[OpCode]func(*Expr) string

func init() {
	// Build the dispatch table once at program start.
	literalHandler := (*Expr).stringLiteral
	unaryHandler := (*Expr).stringUnary
	binaryHandler := (*Expr).stringBinary

	exprStringHandlers = map[OpCode]func(*Expr) string{
		// Literals
		OpNull:     literalHandler,
		OpInteger:  literalHandler,
		OpFloat:    literalHandler,
		OpString:   literalHandler,
		OpBlob:     literalHandler,
		OpVariable: literalHandler,

		// Column reference
		OpColumn: (*Expr).stringColumn,

		// Unary prefix symbol ops
		OpNegate:    unaryHandler,
		OpBitNot:    unaryHandler,
		OpUnaryPlus: unaryHandler,

		// Unary keyword ops
		OpNot: unaryHandler,

		// Unary postfix ops
		OpIsNull:  unaryHandler,
		OpNotNull: unaryHandler,

		// Binary infix ops
		OpPlus:      binaryHandler,
		OpMinus:     binaryHandler,
		OpMultiply:  binaryHandler,
		OpDivide:    binaryHandler,
		OpRemainder: binaryHandler,
		OpConcat:    binaryHandler,
		OpBitAnd:    binaryHandler,
		OpBitOr:     binaryHandler,
		OpBitXor:    binaryHandler,
		OpLShift:    binaryHandler,
		OpRShift:    binaryHandler,
		OpEq:        binaryHandler,
		OpNe:        binaryHandler,
		OpLt:        binaryHandler,
		OpLe:        binaryHandler,
		OpGt:        binaryHandler,
		OpGe:        binaryHandler,
		OpIs:        binaryHandler,
		OpIsNot:     binaryHandler,
		OpAnd:       binaryHandler,
		OpOr:        binaryHandler,
		OpLike:      binaryHandler,
		OpGlob:      binaryHandler,
		OpRegexp:    binaryHandler,

		// Special forms
		OpFunction: (*Expr).stringFunction,
		OpCast: func(e *Expr) string {
			return fmt.Sprintf("CAST(%s AS %s)", e.Left.String(), e.Token)
		},
		OpCollate: func(e *Expr) string {
			return fmt.Sprintf("(%s COLLATE %s)", e.Left.String(), e.CollSeq)
		},
	}
}

// String returns a string representation of the expression.
func (e *Expr) String() string {
	if e == nil {
		return "NULL"
	}
	if handler, ok := exprStringHandlers[e.Op]; ok {
		return handler(e)
	}
	return fmt.Sprintf("Expr<%s>", e.Op.String())
}

// VectorSize returns the number of columns in a vector expression.
// Returns 1 for scalar expressions.
func (e *Expr) VectorSize() int {
	if e == nil {
		return 0
	}
	if e.Op == OpVector && e.List != nil {
		return len(e.List.Items)
	}
	if e.Op == OpSelect && e.Select != nil && e.Select.Columns != nil {
		return len(e.Select.Columns.Items)
	}
	return 1
}

// IsVector checks if the expression is a vector (multiple columns).
func (e *Expr) IsVector() bool {
	return e.VectorSize() > 1
}

// Clone creates a deep copy of the expression.
func (e *Expr) Clone() *Expr {
	if e == nil {
		return nil
	}

	clone := &Expr{
		Op:         e.Op,
		Affinity:   e.Affinity,
		Flags:      e.Flags,
		Token:      e.Token,
		IntValue:   e.IntValue,
		FloatValue: e.FloatValue,
		TableName:  e.TableName,
		ColumnName: e.ColumnName,
		IColumn:    e.IColumn,
		ITable:     e.ITable,
		CollSeq:    e.CollSeq,
		FuncName:   e.FuncName,
		Height:     e.Height,
		IAgg:       e.IAgg,
		IOp2:       e.IOp2,
		Sub:        e.Sub,
	}

	if e.Left != nil {
		clone.Left = e.Left.Clone()
	}
	if e.Right != nil {
		clone.Right = e.Right.Clone()
	}
	if e.List != nil {
		clone.List = e.List.Clone()
	}
	// Note: SelectStmt and TableRef are not cloned (would need deep copy)

	return clone
}

// Clone creates a deep copy of the expression list.
func (el *ExprList) Clone() *ExprList {
	if el == nil {
		return nil
	}

	clone := &ExprList{
		Items: make([]*ExprListItem, len(el.Items)),
	}

	for i, item := range el.Items {
		clone.Items[i] = &ExprListItem{
			Expr:       item.Expr.Clone(),
			Name:       item.Name,
			Alias:      item.Alias,
			SortFlags:  item.SortFlags,
			Done:       item.Done,
			Reusable:   item.Reusable,
			SorterRef:  item.SorterRef,
			Nulls:      item.Nulls,
			OrderByCol: item.OrderByCol,
		}
	}

	return clone
}
