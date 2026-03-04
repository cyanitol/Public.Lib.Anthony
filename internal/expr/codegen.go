// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// binaryOpEntry holds the VDBE opcode and comment string for a binary operator.
type binaryOpEntry struct {
	op      vdbe.Opcode
	comment string
}

// binaryOpTable maps parser binary operators to their VDBE opcodes and
// comment strings. Operators not present here require special handling.
var binaryOpTable = map[parser.BinaryOp]binaryOpEntry{
	// Arithmetic operators
	parser.OpPlus:  {vdbe.OpAdd, "ADD"},
	parser.OpMinus: {vdbe.OpSubtract, "SUB"},
	parser.OpMul:   {vdbe.OpMultiply, "MUL"},
	parser.OpDiv:   {vdbe.OpDivide, "DIV"},
	parser.OpRem:   {vdbe.OpRemainder, "MOD"},
	// Comparison operators
	parser.OpEq: {vdbe.OpEq, "EQ"},
	parser.OpNe: {vdbe.OpNe, "NE"},
	parser.OpLt: {vdbe.OpLt, "LT"},
	parser.OpLe: {vdbe.OpLe, "LE"},
	parser.OpGt: {vdbe.OpGt, "GT"},
	parser.OpGe: {vdbe.OpGe, "GE"},
	// Bitwise operators
	parser.OpBitAnd: {vdbe.OpBitAnd, "BITAND"},
	parser.OpBitOr:  {vdbe.OpBitOr, "BITOR"},
	parser.OpLShift: {vdbe.OpShiftLeft, "LSHIFT"},
	parser.OpRShift: {vdbe.OpShiftRight, "RSHIFT"},
	// String concatenation
	parser.OpConcat: {vdbe.OpConcat, "CONCAT"},
}

// binarySpecialHandler is the signature for operators that need custom
// register-level handling (e.g. LIKE/GLOB) and return early.
type binarySpecialHandler func(g *CodeGenerator, leftReg, rightReg int) (int, error)

// binarySpecialHandlers maps operators that require special code paths to
// their handler functions.
var binarySpecialHandlers = map[parser.BinaryOp]binarySpecialHandler{
	parser.OpLike: (*CodeGenerator).generateLikeExpr,
	parser.OpGlob: (*CodeGenerator).generateGlobExpr,
}

// literalHandler is the signature for literal-type dispatch handlers.
type literalHandler func(g *CodeGenerator, e *parser.LiteralExpr, reg int) error

// literalHandlers maps literal types to their code-generation handlers.
var literalHandlers = map[parser.LiteralType]literalHandler{
	parser.LiteralNull:    (*CodeGenerator).generateNullLiteralValue,
	parser.LiteralInteger: (*CodeGenerator).generateIntegerLiteral,
	parser.LiteralFloat:   (*CodeGenerator).generateFloatLiteral,
	parser.LiteralString:  (*CodeGenerator).generateStringLiteral,
	parser.LiteralBlob:    (*CodeGenerator).generateBlobLiteral,
}

// registerAdjustmentRule defines which parameters need register adjustment for an opcode.
type registerAdjustmentRule struct {
	adjustP1 bool // Whether P1 is a register that needs adjustment
	adjustP3 bool // Whether P3 is a register that needs adjustment
}

// registerAdjustmentRules maps opcodes to their register adjustment rules.
var registerAdjustmentRules = map[vdbe.Opcode]registerAdjustmentRule{
	// Arithmetic operations
	vdbe.OpAdd:        {adjustP1: true, adjustP3: true},
	vdbe.OpSubtract:   {adjustP1: true, adjustP3: true},
	vdbe.OpMultiply:   {adjustP1: true, adjustP3: true},
	vdbe.OpDivide:     {adjustP1: true, adjustP3: true},
	vdbe.OpRemainder:  {adjustP1: true, adjustP3: true},
	vdbe.OpBitNot:     {adjustP1: true, adjustP3: true},
	vdbe.OpBitAnd:     {adjustP1: true, adjustP3: true},
	vdbe.OpBitOr:      {adjustP1: true, adjustP3: true},
	vdbe.OpShiftLeft:  {adjustP1: true, adjustP3: true},
	vdbe.OpShiftRight: {adjustP1: true, adjustP3: true},
	// Comparison operations
	vdbe.OpLt:      {adjustP1: true, adjustP3: true},
	vdbe.OpLe:      {adjustP1: true, adjustP3: true},
	vdbe.OpGt:      {adjustP1: true, adjustP3: true},
	vdbe.OpGe:      {adjustP1: true, adjustP3: true},
	vdbe.OpEq:      {adjustP1: true, adjustP3: true},
	vdbe.OpNe:      {adjustP1: true, adjustP3: true},
	vdbe.OpAnd:     {adjustP1: true, adjustP3: true},
	vdbe.OpOr:      {adjustP1: true, adjustP3: true},
	vdbe.OpNot:     {adjustP1: true, adjustP3: true},
	vdbe.OpIsNull:  {adjustP1: true, adjustP3: false},
	vdbe.OpNotNull: {adjustP1: true, adjustP3: false},
	// String operations
	vdbe.OpConcat: {adjustP1: true, adjustP3: true},
	// Type conversion
	vdbe.OpCast: {adjustP1: true, adjustP3: true},
	// Data movement
	vdbe.OpCopy:    {adjustP1: true, adjustP3: false},
	vdbe.OpMove:    {adjustP1: true, adjustP3: false},
	vdbe.OpInteger: {adjustP1: false, adjustP3: false},
	vdbe.OpReal:    {adjustP1: false, adjustP3: false},
	vdbe.OpString8: {adjustP1: false, adjustP3: false},
	vdbe.OpNull:    {adjustP1: false, adjustP3: false},
	// Column operations
	vdbe.OpColumn: {adjustP1: false, adjustP3: true},
	// Result operations
	vdbe.OpResultRow: {adjustP1: true, adjustP3: false},
	// Aggregate operations
	vdbe.OpAggDistinct: {adjustP1: true, adjustP3: false},
	vdbe.OpAddImm:      {adjustP1: true, adjustP3: false},
	// Function calls
	vdbe.OpFunction: {adjustP1: true, adjustP3: true},
	// Control flow with register operands
	vdbe.OpIf:        {adjustP1: true, adjustP3: false},
	vdbe.OpIfNot:     {adjustP1: true, adjustP3: false},
	vdbe.OpIfPos:     {adjustP1: true, adjustP3: false},
	vdbe.OpIfNotZero: {adjustP1: true, adjustP3: false},
	// Subroutine control
	vdbe.OpGosub:  {adjustP1: true, adjustP3: false},
	vdbe.OpReturn: {adjustP1: true, adjustP3: false},
	// Comparison operations
	vdbe.OpCompare: {adjustP1: true, adjustP3: true},
}

// exprHandler is the signature for expression-type dispatch handlers.
type exprHandler func(g *CodeGenerator, e parser.Expression) (int, error)

// exprDispatch maps concrete parser.Expression types to their code-generation
// handlers. Populated by init() to avoid forward-reference issues.
var exprDispatch map[reflect.Type]exprHandler

func init() {
	exprDispatch = map[reflect.Type]exprHandler{
		reflect.TypeOf((*parser.LiteralExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateLiteral(e.(*parser.LiteralExpr))
		},
		reflect.TypeOf((*parser.IdentExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateColumn(e.(*parser.IdentExpr))
		},
		reflect.TypeOf((*parser.BinaryExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateBinary(e.(*parser.BinaryExpr))
		},
		reflect.TypeOf((*parser.UnaryExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateUnary(e.(*parser.UnaryExpr))
		},
		reflect.TypeOf((*parser.FunctionExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateFunction(e.(*parser.FunctionExpr))
		},
		reflect.TypeOf((*parser.CaseExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateCase(e.(*parser.CaseExpr))
		},
		reflect.TypeOf((*parser.InExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateIn(e.(*parser.InExpr))
		},
		reflect.TypeOf((*parser.BetweenExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateBetween(e.(*parser.BetweenExpr))
		},
		reflect.TypeOf((*parser.CastExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateCast(e.(*parser.CastExpr))
		},
		reflect.TypeOf((*parser.SubqueryExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateSubquery(e.(*parser.SubqueryExpr))
		},
		reflect.TypeOf((*parser.ExistsExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateExists(e.(*parser.ExistsExpr))
		},
		reflect.TypeOf((*parser.VariableExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateVariable(e.(*parser.VariableExpr))
		},
		reflect.TypeOf((*parser.CollateExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateCollate(e.(*parser.CollateExpr))
		},
		reflect.TypeOf((*parser.ParenExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.GenerateExpr(e.(*parser.ParenExpr).Expr)
		},
	}
}

// ColumnInfo contains column metadata for code generation.
type ColumnInfo struct {
	Name     string
	Index    int  // Column index in the record
	IsRowid  bool // True if this is the INTEGER PRIMARY KEY (alias for rowid)
}

// TableInfo contains table metadata for code generation.
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

// SubqueryCompiler is a callback function to compile subquery SELECT statements.
// It takes a SELECT AST node and returns compiled VDBE bytecode.
type SubqueryCompiler func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error)

// CodeGenerator generates VDBE bytecode for expressions.
// It converts parser AST nodes into executable VDBE instructions.
type CodeGenerator struct {
	vdbe             *vdbe.VDBE
	nextReg          int
	nextCursor       int                  // next cursor index to allocate for subqueries
	cursorMap        map[string]int       // table name -> cursor number
	tableInfo        map[string]TableInfo // table name -> table info
	args             []interface{}        // bound parameter values
	paramIdx         int                  // next parameter index to use
	collations       map[int]string       // register -> collation name (for collate expressions)
	subqueryCompiler SubqueryCompiler     // callback to compile subqueries
}

// NewCodeGenerator creates a new code generator.
func NewCodeGenerator(v *vdbe.VDBE) *CodeGenerator {
	// Start cursor allocation after any cursors already allocated
	startCursor := len(v.Cursors)
	return &CodeGenerator{
		vdbe:             v,
		nextReg:          1,
		nextCursor:       startCursor,
		cursorMap:        make(map[string]int),
		tableInfo:        make(map[string]TableInfo),
		args:             nil,
		paramIdx:         0,
		collations:       make(map[int]string),
		subqueryCompiler: nil,
	}
}

// SetSubqueryCompiler sets the callback for compiling subqueries.
func (g *CodeGenerator) SetSubqueryCompiler(compiler SubqueryCompiler) {
	g.subqueryCompiler = compiler
}

// GetVDBE returns the underlying VDBE for access to context.
func (g *CodeGenerator) GetVDBE() *vdbe.VDBE {
	return g.vdbe
}

// SetArgs sets the bound parameter values for the code generator.
func (g *CodeGenerator) SetArgs(args []interface{}) {
	g.args = args
	g.paramIdx = 0
}

// RegisterTable registers table information for column resolution.
func (g *CodeGenerator) RegisterTable(info TableInfo) {
	g.tableInfo[info.Name] = info
}

// SetNextReg sets the next register number to allocate.
// Use this to reserve a range of registers (e.g., for result columns).
func (g *CodeGenerator) SetNextReg(next int) {
	if next > g.nextReg {
		g.nextReg = next
	}
}

// AllocReg allocates a new register and returns its index.
func (g *CodeGenerator) AllocReg() int {
	reg := g.nextReg
	g.nextReg++
	// Ensure VDBE has enough memory allocated
	if reg >= g.vdbe.NumMem {
		g.vdbe.AllocMemory(g.nextReg)
	}
	return reg
}

// AllocRegs allocates N consecutive registers and returns the first index.
func (g *CodeGenerator) AllocRegs(n int) int {
	reg := g.nextReg
	g.nextReg += n
	if g.nextReg > g.vdbe.NumMem {
		g.vdbe.AllocMemory(g.nextReg)
	}
	return reg
}

// AllocCursor allocates a new cursor index and returns it.
// This is separate from register allocation - cursors are used for
// OpOpenRead, OpOpenWrite, OpOpenEphemeral, OpRewind, OpNext, etc.
func (g *CodeGenerator) AllocCursor() int {
	cursor := g.nextCursor
	g.nextCursor++
	// Ensure VDBE has enough cursors allocated
	g.vdbe.AllocCursors(g.nextCursor)
	return cursor
}

// SetNextCursor sets the next cursor index to allocate.
// Use this to reserve cursor indices for tables in the query.
func (g *CodeGenerator) SetNextCursor(next int) {
	if next > g.nextCursor {
		g.nextCursor = next
	}
}

// RegisterCursor associates a table name with a cursor number.
func (g *CodeGenerator) RegisterCursor(tableName string, cursor int) {
	g.cursorMap[tableName] = cursor
}

// GetCursor returns the cursor number for a table name.
func (g *CodeGenerator) GetCursor(tableName string) (int, bool) {
	cursor, ok := g.cursorMap[tableName]
	return cursor, ok
}

// GenerateExpr generates code for any expression and returns the result register.
func (g *CodeGenerator) GenerateExpr(expr parser.Expression) (int, error) {
	if expr == nil {
		return g.generateNullLiteral()
	}
	handler, ok := exprDispatch[reflect.TypeOf(expr)]
	if !ok {
		return 0, fmt.Errorf("unsupported expression type: %T", expr)
	}
	return handler(g, expr)
}

// generateNullLiteral emits a NULL opcode and returns the allocated register.
func (g *CodeGenerator) generateNullLiteral() (int, error) {
	reg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "NULL literal")
	return reg, nil
}

// generateLiteral generates code for literal values using a table-driven dispatch.
func (g *CodeGenerator) generateLiteral(e *parser.LiteralExpr) (int, error) {
	reg := g.AllocReg()

	handler, ok := literalHandlers[e.Type]
	if !ok {
		return 0, fmt.Errorf("unsupported literal type: %v", e.Type)
	}

	if err := handler(g, e, reg); err != nil {
		return 0, err
	}

	return reg, nil
}

// generateNullLiteralValue generates code for NULL literals.
func (g *CodeGenerator) generateNullLiteralValue(e *parser.LiteralExpr, reg int) error {
	g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "NULL")
	return nil
}

// generateIntegerLiteral generates code for integer literals.
func (g *CodeGenerator) generateIntegerLiteral(e *parser.LiteralExpr, reg int) error {
	var val int64
	fmt.Sscanf(e.Value, "%d", &val)
	if val >= -2147483648 && val <= 2147483647 {
		g.vdbe.AddOp(vdbe.OpInteger, int(val), reg, 0)
	} else {
		g.vdbe.AddOpWithP4Int64(vdbe.OpInt64, 0, reg, 0, val)
	}
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("INT %s", e.Value))
	return nil
}

// generateFloatLiteral generates code for float literals.
func (g *CodeGenerator) generateFloatLiteral(e *parser.LiteralExpr, reg int) error {
	var val float64
	fmt.Sscanf(e.Value, "%f", &val)
	addr := g.vdbe.AddOp(vdbe.OpReal, 0, reg, 0)
	g.vdbe.Program[addr].P4.R = val
	g.vdbe.Program[addr].P4Type = vdbe.P4Real
	g.vdbe.SetComment(addr, fmt.Sprintf("REAL %s", e.Value))
	return nil
}

// generateStringLiteral generates code for string literals.
func (g *CodeGenerator) generateStringLiteral(e *parser.LiteralExpr, reg int) error {
	g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, e.Value)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("STRING '%s'", e.Value))
	return nil
}

// generateBlobLiteral generates code for blob literals.
func (g *CodeGenerator) generateBlobLiteral(e *parser.LiteralExpr, reg int) error {
	// e.Value is the raw lexeme like X'01020304' or x'ABCDEF'.
	// Strip the X'...' wrapper to get the hex digits, then decode to []byte.
	hexStr := e.Value
	if len(hexStr) >= 3 && (hexStr[0] == 'X' || hexStr[0] == 'x') && hexStr[1] == '\'' && hexStr[len(hexStr)-1] == '\'' {
		hexStr = hexStr[2 : len(hexStr)-1]
	}
	blobData, err := hex.DecodeString(strings.ToUpper(hexStr))
	if err != nil {
		return fmt.Errorf("invalid blob literal %q: %w", e.Value, err)
	}
	g.vdbe.AddOpWithP4Blob(vdbe.OpBlob, len(blobData), reg, 0, blobData)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "BLOB")
	return nil
}

// generateColumn generates code for column references.
func (g *CodeGenerator) generateColumn(e *parser.IdentExpr) (int, error) {
	reg := g.AllocReg()

	// Resolve table and cursor
	tableName, cursor, err := g.resolveTableForColumn(e)
	if err != nil {
		return 0, err
	}

	// Look up column info
	colIndex, isRowid, err := g.lookupColumnInfo(tableName, e.Name)
	if err != nil {
		return 0, err
	}

	// Emit opcode
	g.emitColumnOpcode(cursor, colIndex, isRowid, reg)
	g.addColumnComment(reg, tableName, e.Name)

	return reg, nil
}

// resolveTableForColumn finds the table name and cursor for a column reference.
func (g *CodeGenerator) resolveTableForColumn(e *parser.IdentExpr) (string, int, error) {
	tableName := e.Table

	if tableName != "" {
		cursor, ok := g.cursorMap[tableName]
		if !ok {
			return "", 0, fmt.Errorf("unknown table: %s", tableName)
		}
		return tableName, cursor, nil
	}

	// No table qualifier - find the table that has this column
	tableName, cursor := g.findTableWithColumn(e.Name)
	return tableName, cursor, nil
}

// findTableWithColumn searches for a table that contains the given column name.
func (g *CodeGenerator) findTableWithColumn(colName string) (string, int) {
	for name, info := range g.tableInfo {
		for _, col := range info.Columns {
			if col.Name == colName {
				return name, g.cursorMap[name]
			}
		}
	}
	return "", 0
}

// lookupColumnInfo finds the column index and rowid flag for a column in a table.
func (g *CodeGenerator) lookupColumnInfo(tableName, colName string) (int, bool, error) {
	info, ok := g.tableInfo[tableName]
	if !ok {
		return 0, false, nil
	}

	for _, col := range info.Columns {
		if col.Name == colName {
			return col.Index, col.IsRowid, nil
		}
	}

	return 0, false, fmt.Errorf("column not found: %s", colName)
}

// emitColumnOpcode emits the appropriate opcode for reading a column value.
func (g *CodeGenerator) emitColumnOpcode(cursor, colIndex int, isRowid bool, reg int) {
	if isRowid {
		g.vdbe.AddOp(vdbe.OpRowid, cursor, reg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpColumn, cursor, colIndex, reg)
	}
}

// addColumnComment adds a descriptive comment to the last opcode.
func (g *CodeGenerator) addColumnComment(reg int, tableName, colName string) {
	if tableName != "" {
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("r[%d]=%s.%s", reg, tableName, colName))
	} else {
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("r[%d]=%s", reg, colName))
	}
}

// generateBinary generates code for binary operations.
func (g *CodeGenerator) generateBinary(e *parser.BinaryExpr) (int, error) {
	// Special handling for AND and OR (short-circuit evaluation)
	if e.Op == parser.OpAnd || e.Op == parser.OpOr {
		return g.generateLogical(e)
	}

	leftReg, rightReg, err := g.generateBinaryOperands(e)
	if err != nil {
		return 0, err
	}

	// Operators that require custom register-level dispatch (e.g. LIKE, GLOB).
	if handler, ok := binarySpecialHandlers[e.Op]; ok {
		return handler(g, leftReg, rightReg)
	}

	return g.generateStandardBinaryOp(e.Op, leftReg, rightReg)
}

// generateBinaryOperands generates code for left and right operands
func (g *CodeGenerator) generateBinaryOperands(e *parser.BinaryExpr) (int, int, error) {
	leftReg, err := g.GenerateExpr(e.Left)
	if err != nil {
		return 0, 0, err
	}
	rightReg, err := g.GenerateExpr(e.Right)
	if err != nil {
		return 0, 0, err
	}
	return leftReg, rightReg, nil
}

// generateStandardBinaryOp generates code for standard binary operators from the lookup table
func (g *CodeGenerator) generateStandardBinaryOp(op parser.BinaryOp, leftReg, rightReg int) (int, error) {
	entry, ok := binaryOpTable[op]
	if !ok {
		return 0, fmt.Errorf("unsupported binary operator: %v", op)
	}

	resultReg := g.AllocReg()
	collation := g.getCollationForOperands(leftReg, rightReg)

	g.emitBinaryOpcode(entry, op, leftReg, rightReg, resultReg, collation)
	return resultReg, nil
}

// getCollationForOperands retrieves the collation from either operand register
func (g *CodeGenerator) getCollationForOperands(leftReg, rightReg int) string {
	if coll, ok := g.collations[leftReg]; ok {
		return coll
	}
	if coll, ok := g.collations[rightReg]; ok {
		return coll
	}
	return ""
}

// emitBinaryOpcode emits the appropriate opcode with optional collation
func (g *CodeGenerator) emitBinaryOpcode(entry binaryOpEntry, op parser.BinaryOp, leftReg, rightReg, resultReg int, collation string) {
	isComparison := (op >= parser.OpEq && op <= parser.OpGe)

	if isComparison && collation != "" {
		g.emitCollatedComparison(entry, leftReg, rightReg, resultReg, collation)
	} else {
		g.vdbe.AddOp(entry.op, leftReg, rightReg, resultReg)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, entry.comment)
	}
}

// emitCollatedComparison emits a comparison operator with collation
func (g *CodeGenerator) emitCollatedComparison(entry binaryOpEntry, leftReg, rightReg, resultReg int, collation string) {
	addr := g.vdbe.AddOp(entry.op, leftReg, rightReg, resultReg)
	g.vdbe.Program[addr].P4.Z = collation
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.SetComment(addr, fmt.Sprintf("%s (COLLATE %s)", entry.comment, collation))
}

// generateLogical generates code for AND/OR with short-circuit evaluation.
func (g *CodeGenerator) generateLogical(e *parser.BinaryExpr) (int, error) {
	resultReg := g.AllocReg()

	if e.Op == parser.OpAnd {
		// AND: if left is false, result is false (skip right)
		leftReg, err := g.GenerateExpr(e.Left)
		if err != nil {
			return 0, err
		}

		// Copy left to result
		g.vdbe.AddOp(vdbe.OpCopy, leftReg, resultReg, 0)

		// If false, jump to end
		endLabel := g.vdbe.NumOps() + 100 // Placeholder - will be patched
		g.vdbe.AddOp(vdbe.OpIfNot, resultReg, endLabel, 0)
		ifNotAddr := g.vdbe.NumOps() - 1

		// Evaluate right side
		rightReg, err := g.GenerateExpr(e.Right)
		if err != nil {
			return 0, err
		}

		// Copy right to result
		g.vdbe.AddOp(vdbe.OpCopy, rightReg, resultReg, 0)

		// Patch the jump
		g.vdbe.Program[ifNotAddr].P2 = g.vdbe.NumOps()

	} else { // OpOr
		// OR: if left is true, result is true (skip right)
		leftReg, err := g.GenerateExpr(e.Left)
		if err != nil {
			return 0, err
		}

		// Copy left to result
		g.vdbe.AddOp(vdbe.OpCopy, leftReg, resultReg, 0)

		// If true, jump to end
		endLabel := g.vdbe.NumOps() + 100
		g.vdbe.AddOp(vdbe.OpIf, resultReg, endLabel, 0)
		ifAddr := g.vdbe.NumOps() - 1

		// Evaluate right side
		rightReg, err := g.GenerateExpr(e.Right)
		if err != nil {
			return 0, err
		}

		// Copy right to result
		g.vdbe.AddOp(vdbe.OpCopy, rightReg, resultReg, 0)

		// Patch the jump
		g.vdbe.Program[ifAddr].P2 = g.vdbe.NumOps()
	}

	return resultReg, nil
}

// generateUnary generates code for unary operations.
func (g *CodeGenerator) generateUnary(e *parser.UnaryExpr) (int, error) {
	// Generate operand
	operandReg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	resultReg := g.AllocReg()

	switch e.Op {
	case parser.OpNot:
		g.vdbe.AddOp(vdbe.OpNot, operandReg, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NOT")

	case parser.OpNeg:
		// Negate: load zero, then subtract (zero - operand)
		zeroReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpInteger, 0, zeroReg, 0)
		g.vdbe.AddOp(vdbe.OpSubtract, zeroReg, operandReg, resultReg)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NEG")

	case parser.OpBitNot:
		g.vdbe.AddOp(vdbe.OpBitNot, operandReg, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "BITNOT")

	case parser.OpIsNull:
		// OpIsNull is a jump instruction: jumps to P2 if P1 is NULL.
		// Emit a boolean-producing sequence instead of using it directly.
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
		isNullAddr := g.vdbe.AddOp(vdbe.OpIsNull, operandReg, 0, 0)
		g.vdbe.SetComment(isNullAddr, "IS NULL")
		g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
		g.vdbe.Program[isNullAddr].P2 = g.vdbe.NumOps()

	case parser.OpNotNull:
		// OpNotNull is a jump instruction: jumps to P2 if P1 is NOT NULL.
		// Emit a boolean-producing sequence instead of using it directly.
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
		notNullAddr := g.vdbe.AddOp(vdbe.OpNotNull, operandReg, 0, 0)
		g.vdbe.SetComment(notNullAddr, "NOT NULL")
		g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
		g.vdbe.Program[notNullAddr].P2 = g.vdbe.NumOps()

	default:
		return 0, fmt.Errorf("unsupported unary operator: %v", e.Op)
	}

	return resultReg, nil
}

// generateFunction generates code for function calls.
func (g *CodeGenerator) generateFunction(e *parser.FunctionExpr) (int, error) {
	resultReg := g.AllocReg()

	// Handle special COUNT(*) case
	if e.Star {
		// COUNT(*) - use OpCount
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("%s(*)", e.Name))
		return resultReg, nil
	}

	// Allocate consecutive registers for arguments
	argCount := len(e.Args)
	var firstArg int

	if argCount > 0 {
		// Allocate N consecutive registers for arguments
		firstArg = g.AllocRegs(argCount)

		// Evaluate each argument and move to consecutive register
		for i, arg := range e.Args {
			argReg, err := g.GenerateExpr(arg)
			if err != nil {
				return 0, err
			}

			// If argument is not already in the correct position, move it
			targetReg := firstArg + i
			if argReg != targetReg {
				g.vdbe.AddOp(vdbe.OpMove, argReg, targetReg, 0)
			}
		}
	}

	// Emit function call
	// P1 = constant mask (unused), P2 = first arg register, P3 = result register
	// P4 = function name, P5 = number of arguments
	addr := g.vdbe.AddOp(vdbe.OpFunction, 0, firstArg, resultReg)
	g.vdbe.Program[addr].P4.Z = e.Name
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.Program[addr].P5 = uint16(argCount)
	g.vdbe.SetComment(addr, fmt.Sprintf("%s(%d args)", e.Name, argCount))

	return resultReg, nil
}

// generateCase generates code for CASE expressions.
func (g *CodeGenerator) generateCase(e *parser.CaseExpr) (int, error) {
	resultReg := g.AllocReg()

	// Track jump addresses to patch
	var endJumps []int

	// For simple CASE (CASE x WHEN v1 THEN r1 ...), evaluate x once
	var caseExprReg int
	if e.Expr != nil {
		var err error
		caseExprReg, err = g.GenerateExpr(e.Expr)
		if err != nil {
			return 0, err
		}
	}

	// Generate code for each WHEN clause
	for _, when := range e.WhenClauses {
		var condReg int
		var err error

		if e.Expr != nil {
			// Simple CASE: compare CASE expression with WHEN value
			// CASE x WHEN v1 THEN r1 ... => compare x = v1
			whenValueReg, err := g.GenerateExpr(when.Condition)
			if err != nil {
				return 0, err
			}
			// Generate comparison: caseExprReg == whenValueReg
			condReg = g.AllocReg()
			g.vdbe.AddOp(vdbe.OpEq, caseExprReg, whenValueReg, condReg)
		} else {
			// Searched CASE: evaluate WHEN condition directly
			condReg, err = g.GenerateExpr(when.Condition)
			if err != nil {
				return 0, err
			}
		}

		// If condition is false, jump to next WHEN
		nextWhenAddr := g.vdbe.NumOps() + 100 // Placeholder
		g.vdbe.AddOp(vdbe.OpIfNot, condReg, nextWhenAddr, 0)
		jumpToNext := g.vdbe.NumOps() - 1

		// Evaluate THEN result
		thenReg, err := g.GenerateExpr(when.Result)
		if err != nil {
			return 0, err
		}

		// Copy result
		g.vdbe.AddOp(vdbe.OpCopy, thenReg, resultReg, 0)

		// Jump to end
		g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
		endJumps = append(endJumps, g.vdbe.NumOps()-1)

		// Patch jump to next WHEN
		g.vdbe.Program[jumpToNext].P2 = g.vdbe.NumOps()
	}

	// Generate ELSE clause (or NULL if not present)
	if e.ElseClause != nil {
		elseReg, err := g.GenerateExpr(e.ElseClause)
		if err != nil {
			return 0, err
		}
		g.vdbe.AddOp(vdbe.OpCopy, elseReg, resultReg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	}

	// Patch all end jumps
	endAddr := g.vdbe.NumOps()
	for _, jumpAddr := range endJumps {
		g.vdbe.Program[jumpAddr].P2 = endAddr
	}

	return resultReg, nil
}

// generateIn generates code for IN expressions.
func (g *CodeGenerator) generateIn(e *parser.InExpr) (int, error) {
	exprReg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	var resultReg int
	if e.Select != nil {
		// IN (subquery) - compile subquery and check if value exists in results
		resultReg, err = g.generateInSubquery(e, exprReg, g.AllocReg())
	} else {
		// IN (value_list) - generate comparisons
		resultReg, err = g.generateInValueList(e, exprReg)
	}

	if err != nil {
		return 0, err
	}

	// Handle NOT IN
	if e.Not {
		return g.negateResult(resultReg), nil
	}

	return resultReg, nil
}

// generateInValueList generates code for IN (value_list) expressions.
func (g *CodeGenerator) generateInValueList(e *parser.InExpr, exprReg int) (int, error) {
	resultReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)

	var endJumps []int

	for _, val := range e.Values {
		jumpAddr, err := g.generateInValueComparison(exprReg, val, resultReg)
		if err != nil {
			return 0, err
		}
		endJumps = append(endJumps, jumpAddr)
	}

	g.patchJumpsToEnd(endJumps)
	return resultReg, nil
}

// generateInValueComparison generates code to compare exprReg with a single IN value.
// Returns the address of the jump to the end on match.
func (g *CodeGenerator) generateInValueComparison(exprReg int, val parser.Expression, resultReg int) (int, error) {
	valReg, err := g.GenerateExpr(val)
	if err != nil {
		return 0, err
	}

	// Compare
	cmpReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpEq, exprReg, valReg, cmpReg)

	// If true, set result to true and jump to end
	g.vdbe.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)
	ifAddr := g.vdbe.NumOps() - 1

	g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
	gotoAddr := g.vdbe.NumOps() - 1

	// Patch the If jump to next iteration
	g.vdbe.Program[ifAddr].P2 = g.vdbe.NumOps()

	return gotoAddr, nil
}

// patchJumpsToEnd patches a list of jump addresses to the current position.
func (g *CodeGenerator) patchJumpsToEnd(jumps []int) {
	endAddr := g.vdbe.NumOps()
	for _, jumpAddr := range jumps {
		g.vdbe.Program[jumpAddr].P2 = endAddr
	}
}

// negateResult creates a new register with the negated boolean value.
func (g *CodeGenerator) negateResult(reg int) int {
	notReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpNot, reg, notReg, 0)
	return notReg
}

// generateInSubquery generates code for IN (SELECT ...) expressions.
// Strategy: Similar to SQLite's approach - create an ephemeral table,
// populate it with subquery results, then use OP_Found to check membership.
func (g *CodeGenerator) generateInSubquery(e *parser.InExpr, exprReg int, resultReg int) (int, error) {
	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}

	// Initialize result to false (0)
	g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: init result to false")

	// Allocate a cursor for the ephemeral table that will hold subquery results
	subqueryCursor := g.AllocCursor()

	// Open ephemeral table with 1 column
	g.vdbe.AddOp(vdbe.OpOpenEphemeral, subqueryCursor, 1, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: open ephemeral table")

	// Compile the SELECT statement
	// The subquery compiler should generate code that uses ResultRow
	subVM, err := g.subqueryCompiler(e.Select)
	if err != nil {
		return 0, fmt.Errorf("failed to compile IN subquery: %w", err)
	}

	// Record where we'll insert the subquery bytecode
	addrSubqueryStart := g.vdbe.NumOps()

	// NOTE: setupSubqueryCompiler already handles register and cursor adjustments.
	// We only need to adjust jump targets for the embedding position.
	// Do NOT adjust registers or cursors again - that would cause double adjustment!

	// Adjust jump targets in subquery bytecode for embedding position
	g.adjustSubqueryJumpTargets(subVM, addrSubqueryStart)

	// Copy the subquery bytecode into the current VDBE
	// Replace ResultRow instructions with code to insert into ephemeral table
	// Skip Halt instructions (they would terminate execution)
	for i := 0; i < subVM.NumOps(); i++ {
		instr := *subVM.Program[i] // Copy instruction
		if instr.Opcode == vdbe.OpHalt {
			// Skip Halt - we want execution to continue after subquery
			g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: skip halt (replaced with noop)")
			continue
		} else if instr.Opcode == vdbe.OpResultRow {
			// Replace ResultRow with: MakeRecord + Insert into ephemeral table
			// P1 = source register (already adjusted), P2 = number of columns (should be 1)
			// For IN subqueries, we only care about the first column

			// Allocate register for the record
			recReg := g.AllocReg()

			// MakeRecord: create a record from the result column(s)
			// P1 = first source reg, P2 = num regs, P3 = dest reg
			g.vdbe.AddOp(vdbe.OpMakeRecord, instr.P1, 1, recReg)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: make record")

			// Insert: insert record into ephemeral table
			// P1 = cursor, P2 = record reg, P3 = unused (0)
			g.vdbe.AddOp(vdbe.OpInsert, subqueryCursor, recReg, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: insert into ephemeral")
		} else {
			g.vdbe.Program = append(g.vdbe.Program, &instr)
		}
	}

	// NOTE: Do NOT update g.nextReg here - setupSubqueryCompiler already
	// ensured the parent VDBE has enough registers allocated via findMaxRegister().

	// Now check if the LHS value exists in the ephemeral table
	// First, rewind the ephemeral table cursor
	addrRewind := g.vdbe.AddOp(vdbe.OpRewind, subqueryCursor, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: rewind")

	// Loop through ephemeral table
	addrLoop := g.vdbe.NumOps()

	// Read the value from the current row
	valueReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpColumn, subqueryCursor, 0, valueReg)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: read column 0")

	// Compare with LHS value
	cmpReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpEq, exprReg, valueReg, cmpReg)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: compare")

	// If not equal, skip to next row
	g.vdbe.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)
	addrIfNot := g.vdbe.NumOps() - 1

	// Found a match - set result to true (1)
	g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: match found")

	// Jump to end
	g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
	addrGotoEnd := g.vdbe.NumOps() - 1

	// Patch the IfNot to continue to next iteration
	g.vdbe.Program[addrIfNot].P2 = g.vdbe.NumOps()

	// Next: advance to next row and loop
	g.vdbe.AddOp(vdbe.OpNext, subqueryCursor, addrLoop, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: next row")

	// End of loop - no match found (result stays 0)
	addrEnd := g.vdbe.NumOps()

	// Patch the Rewind jump to end if no rows
	g.vdbe.Program[addrRewind].P2 = addrEnd

	// Patch the Goto to end
	g.vdbe.Program[addrGotoEnd].P2 = addrEnd

	// Close ephemeral table
	g.vdbe.AddOp(vdbe.OpClose, subqueryCursor, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "IN subquery: close ephemeral")

	return resultReg, nil
}

// generateBetween generates code for BETWEEN expressions.
func (g *CodeGenerator) generateBetween(e *parser.BetweenExpr) (int, error) {
	// expr BETWEEN lower AND upper
	// Implemented as: (expr >= lower) AND (expr <= upper)

	exprReg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	lowerReg, err := g.GenerateExpr(e.Lower)
	if err != nil {
		return 0, err
	}

	upperReg, err := g.GenerateExpr(e.Upper)
	if err != nil {
		return 0, err
	}

	// expr >= lower
	cmp1Reg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpGe, exprReg, lowerReg, cmp1Reg)

	// expr <= upper
	cmp2Reg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpLe, exprReg, upperReg, cmp2Reg)

	// AND them together
	resultReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpAnd, cmp1Reg, cmp2Reg, resultReg)

	// Handle NOT BETWEEN
	if e.Not {
		notReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpNot, resultReg, notReg, 0)
		return notReg, nil
	}

	return resultReg, nil
}

// generateCast generates code for CAST expressions.
func (g *CodeGenerator) generateCast(e *parser.CastExpr) (int, error) {
	// Evaluate the expression
	exprReg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	resultReg := g.AllocReg()

	// Emit cast operation
	addr := g.vdbe.AddOp(vdbe.OpCast, exprReg, resultReg, 0)
	g.vdbe.Program[addr].P4.Z = e.Type
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.SetComment(addr, fmt.Sprintf("CAST AS %s", e.Type))

	return resultReg, nil
}

// generateSubquery generates code for scalar subquery expressions.
// A scalar subquery is a SELECT that returns a single value.
// Strategy: Inline the subquery code directly. The subquery's ResultRow is
// replaced with a Goto to skip further processing, capturing the result.
// If the subquery returns zero rows, the result is NULL.
func (g *CodeGenerator) generateSubquery(e *parser.SubqueryExpr) (int, error) {
	if e.Select == nil {
		return 0, fmt.Errorf("subquery expression has no SELECT statement")
	}

	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}

	resultReg := g.AllocReg()

	// Initialize result to NULL (default if subquery returns no rows)
	g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: init result to NULL")

	// Compile the SELECT statement
	subVM, err := g.subqueryCompiler(e.Select)
	if err != nil {
		return 0, fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	// Record subquery start address
	addrSubqueryStart := g.vdbe.NumOps()

	// NOTE: setupSubqueryCompiler already handles:
	// - Register adjustment (adjustSubqueryRegisters)
	// - Cursor adjustment (adjustSubqueryCursors)
	// - Memory/cursor allocation in parent VDBE
	// We adjust jump targets here while copying instructions to account for
	// instruction replacements (e.g., OpResultRow becomes 2 instructions)

	// Build address mapping from subquery addresses to parent addresses
	// This maps each instruction's original position to its final position
	addrMap := make(map[int]int)
	currentAddr := addrSubqueryStart
	for i := 0; i < subVM.NumOps(); i++ {
		addrMap[i] = currentAddr
		switch subVM.Program[i].Opcode {
		case vdbe.OpResultRow:
			currentAddr += 2 // Becomes OpCopy + OpGoto
		default:
			currentAddr += 1 // Copied as-is or replaced with 1 instruction
		}
	}
	// Add mapping for address just past the last instruction (common jump target)
	addrMap[subVM.NumOps()] = currentAddr

	// Track addresses where ResultRow is replaced with Goto (need to patch later)
	var resultRowGotoAddrs []int

	// Copy the subquery bytecode, adjusting jumps and replacing special opcodes
	for i := 0; i < subVM.NumOps(); i++ {
		instr := subVM.Program[i]
		switch instr.Opcode {
		case vdbe.OpResultRow:
			// Copy first result column to resultReg
			g.vdbe.AddOp(vdbe.OpCopy, instr.P1, resultReg, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: copy result")
			// Goto past subquery (will patch address later)
			resultRowGotoAddrs = append(resultRowGotoAddrs, g.vdbe.NumOps())
			g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: done, skip to end")
		case vdbe.OpHalt:
			// Convert Halt to Noop - subquery shouldn't halt main program
			g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: stripped Halt")
		default:
			// Copy instruction and adjust jump targets using address map
			instrCopy := *instr
			g.adjustInstructionJumps(&instrCopy, addrMap)
			g.vdbe.Program = append(g.vdbe.Program, &instrCopy)
		}
	}

	// NOTE: Do NOT update g.nextReg here - setupSubqueryCompiler already
	// ensured the parent VDBE has enough registers allocated via findMaxRegister().

	// End of subquery - patch all ResultRow Goto addresses to point here
	addrAfterSubquery := g.vdbe.NumOps()
	for _, addr := range resultRowGotoAddrs {
		g.vdbe.Program[addr].P2 = addrAfterSubquery
	}

	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: end")

	return resultReg, nil
}

// generateLikeExpr generates code for LIKE expressions.
func (g *CodeGenerator) generateLikeExpr(leftReg, rightReg int) (int, error) {
	resultReg := g.AllocReg()
	// For now, use function call approach
	// In real implementation, would use optimized LIKE opcode

	// Allocate consecutive registers for the two arguments (value, pattern)
	firstArg := g.AllocRegs(2)

	// Move arguments to consecutive registers
	if leftReg != firstArg {
		g.vdbe.AddOp(vdbe.OpMove, leftReg, firstArg, 0)
	}
	if rightReg != firstArg+1 {
		g.vdbe.AddOp(vdbe.OpMove, rightReg, firstArg+1, 0)
	}

	// Emit function call
	addr := g.vdbe.AddOp(vdbe.OpFunction, 0, firstArg, resultReg)
	g.vdbe.Program[addr].P4.Z = "like"
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.Program[addr].P5 = 2
	g.vdbe.SetComment(addr, "LIKE")
	return resultReg, nil
}

// generateGlobExpr generates code for GLOB expressions.
func (g *CodeGenerator) generateGlobExpr(leftReg, rightReg int) (int, error) {
	resultReg := g.AllocReg()

	// Allocate consecutive registers for the two arguments (value, pattern)
	firstArg := g.AllocRegs(2)

	// Move arguments to consecutive registers
	if leftReg != firstArg {
		g.vdbe.AddOp(vdbe.OpMove, leftReg, firstArg, 0)
	}
	if rightReg != firstArg+1 {
		g.vdbe.AddOp(vdbe.OpMove, rightReg, firstArg+1, 0)
	}

	// Emit function call
	addr := g.vdbe.AddOp(vdbe.OpFunction, 0, firstArg, resultReg)
	g.vdbe.Program[addr].P4.Z = "glob"
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.Program[addr].P5 = 2
	g.vdbe.SetComment(addr, "GLOB")
	return resultReg, nil
}

// GenerateCondition generates code for a WHERE condition with conditional jump.
// jumpIfFalse is the address to jump to if the condition is false.
// Returns the address of the jump instruction for later patching.
func (g *CodeGenerator) GenerateCondition(expr parser.Expression, jumpIfFalse int) (int, error) {
	// Evaluate the condition
	condReg, err := g.GenerateExpr(expr)
	if err != nil {
		return 0, err
	}

	// Emit conditional jump
	g.vdbe.AddOp(vdbe.OpIfNot, condReg, jumpIfFalse, 0)
	jumpAddr := g.vdbe.NumOps() - 1

	return jumpAddr, nil
}

// GenerateWhereClause generates code for a WHERE clause.
// skipLabel is the address to jump to if the condition fails.
func (g *CodeGenerator) GenerateWhereClause(where parser.Expression, skipLabel int) error {
	if where == nil {
		return nil
	}

	// For complex WHERE with AND/OR, we need special handling
	// For now, simple evaluation and jump
	_, err := g.GenerateCondition(where, skipLabel)
	return err
}

// PatchJump patches a jump instruction to jump to the current position.
func (g *CodeGenerator) PatchJump(addr int) {
	if addr >= 0 && addr < g.vdbe.NumOps() {
		g.vdbe.Program[addr].P2 = g.vdbe.NumOps()
	}
}

// CurrentAddr returns the address of the next instruction to be generated.
func (g *CodeGenerator) CurrentAddr() int {
	return g.vdbe.NumOps()
}

// generateExists generates code for EXISTS (SELECT ...) expressions.
// Strategy: Similar to generateSubquery, but for EXISTS we only need to check
// if any row is returned. The result is initialized to 0 (false), and if the
// subquery produces any row, the ResultRow opcode sets it to 1 (true).
// EXISTS is optimized with LIMIT 1 since we only need to know if at least one row exists.
func (g *CodeGenerator) generateExists(e *parser.ExistsExpr) (int, error) {
	if e.Select == nil {
		return 0, fmt.Errorf("EXISTS expression has no SELECT statement")
	}

	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}

	resultReg := g.AllocReg()

	// Initialize result to false (0) - will be set to 1 if any row is produced
	g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: init result to false")

	// Add LIMIT 1 to the SELECT if it doesn't have one
	// EXISTS only needs to check if at least one row exists
	selectWithLimit := *e.Select
	if selectWithLimit.Limit == nil {
		selectWithLimit.Limit = &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: "1",
		}
	}

	// Compile the SELECT statement
	subVM, err := g.subqueryCompiler(&selectWithLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	// Record subquery start address for jump target adjustment
	addrSubqueryStart := g.vdbe.NumOps()

	// NOTE: setupSubqueryCompiler already handles:
	// - Register adjustment (adjustSubqueryRegisters)
	// - Cursor adjustment (adjustSubqueryCursors)
	// - Memory/cursor allocation in parent VDBE
	// We only need to adjust jump targets here for the embedding position

	// Adjust jump targets in subquery bytecode for embedding position
	g.adjustSubqueryJumpTargets(subVM, addrSubqueryStart)

	// Track addresses where ResultRow is replaced with assignment (need to patch later)
	var resultRowGotoAddrs []int

	// Copy the subquery bytecode into the current VDBE
	// Replace ResultRow with: Set result to 1 (true), then Goto past subquery
	// Replace Halt with Noop (don't halt the main program)
	for i := 0; i < subVM.NumOps(); i++ {
		instr := subVM.Program[i]
		switch instr.Opcode {
		case vdbe.OpResultRow:
			// For EXISTS, any row means true - set result to 1
			g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: row found, set true")
			// Goto past subquery (will patch address later)
			resultRowGotoAddrs = append(resultRowGotoAddrs, g.vdbe.NumOps())
			g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: done, skip to end")
		case vdbe.OpHalt:
			// Convert Halt to Noop - subquery shouldn't halt main program
			g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
			g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: stripped Halt")
		default:
			// Create a copy of the instruction to avoid aliasing
			instrCopy := *instr
			g.vdbe.Program = append(g.vdbe.Program, &instrCopy)
		}
	}

	// Update nextReg to account for registers used by subquery
	g.nextReg += subVM.NumMem

	// End of subquery - patch all ResultRow Goto addresses to point here
	addrAfterSubquery := g.vdbe.NumOps()
	for _, addr := range resultRowGotoAddrs {
		g.vdbe.Program[addr].P2 = addrAfterSubquery
	}

	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS subquery: end")

	// Handle NOT EXISTS
	if e.Not {
		notReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpNot, resultReg, notReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NOT EXISTS: negate result")
		return notReg, nil
	}

	return resultReg, nil
}

// generateVariable generates code for parameter placeholders (?, ?1, :name, etc.).
func (g *CodeGenerator) generateVariable(e *parser.VariableExpr) (int, error) {
	reg := g.AllocReg()

	// Check if we have args and haven't exceeded them
	if g.args == nil || g.paramIdx >= len(g.args) {
		g.emitNullParameter(reg, e.Name)
		return reg, nil
	}

	// Get the next parameter value and emit code for it
	arg := g.args[g.paramIdx]
	g.paramIdx++
	g.emitParameterValue(reg, arg)

	return reg, nil
}

// emitNullParameter emits a NULL opcode for a missing parameter.
func (g *CodeGenerator) emitNullParameter(reg int, name string) {
	g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param %s (no value)", name))
}

// emitParameterValue emits the appropriate opcode for a parameter value.
func (g *CodeGenerator) emitParameterValue(reg int, arg interface{}) {
	switch v := arg.(type) {
	case nil:
		g.emitNullValue(reg)
	case int:
		g.emitIntValue(reg, v)
	case int64:
		g.emitInt64Value(reg, v)
	case float64:
		g.emitFloatValue(reg, v)
	case string:
		g.emitStringValue(reg, v)
	case []byte:
		g.emitBlobValue(reg, v)
	case bool:
		g.emitBoolValue(reg, v)
	default:
		g.emitDefaultValue(reg, v)
	}
}

// emitNullValue emits a NULL parameter value.
func (g *CodeGenerator) emitNullValue(reg int) {
	g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "param NULL")
}

// emitIntValue emits an integer parameter value.
func (g *CodeGenerator) emitIntValue(reg int, v int) {
	g.vdbe.AddOp(vdbe.OpInteger, v, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param INT %d", v))
}

// emitInt64Value emits an int64 parameter value.
func (g *CodeGenerator) emitInt64Value(reg int, v int64) {
	if v >= -2147483648 && v <= 2147483647 {
		g.vdbe.AddOp(vdbe.OpInteger, int(v), reg, 0)
	} else {
		g.vdbe.AddOpWithP4Int64(vdbe.OpInt64, 0, reg, 0, v)
	}
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param INT64 %d", v))
}

// emitFloatValue emits a float64 parameter value.
func (g *CodeGenerator) emitFloatValue(reg int, v float64) {
	addr := g.vdbe.AddOp(vdbe.OpReal, 0, reg, 0)
	g.vdbe.Program[addr].P4.R = v
	g.vdbe.Program[addr].P4Type = vdbe.P4Real
	g.vdbe.SetComment(addr, fmt.Sprintf("param REAL %v", v))
}

// emitStringValue emits a string parameter value.
func (g *CodeGenerator) emitStringValue(reg int, v string) {
	g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param STRING '%s'", v))
}

// emitBlobValue emits a blob parameter value.
func (g *CodeGenerator) emitBlobValue(reg int, v []byte) {
	g.vdbe.AddOpWithP4Str(vdbe.OpBlob, len(v), reg, 0, string(v))
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "param BLOB")
}

// emitBoolValue emits a boolean parameter value.
func (g *CodeGenerator) emitBoolValue(reg int, v bool) {
	val := 0
	if v {
		val = 1
	}
	g.vdbe.AddOp(vdbe.OpInteger, val, reg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param BOOL %v", v))
}

// emitDefaultValue emits a default parameter value by converting to string.
func (g *CodeGenerator) emitDefaultValue(reg int, v interface{}) {
	str := fmt.Sprintf("%v", v)
	g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, str)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param %T as STRING", v))
}

// generateCollate generates code for COLLATE expressions.
// The COLLATE expression wraps another expression and applies a collation for comparison.
// We evaluate the inner expression and track the collation for that register.
func (g *CodeGenerator) generateCollate(e *parser.CollateExpr) (int, error) {
	// Evaluate the wrapped expression
	reg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	// Track the collation for this register
	// This will be used by comparison operators
	g.collations[reg] = e.Collation

	return reg, nil
}

// adjustInstructionJumps adjusts jump targets in a single instruction using the address map.
func (g *CodeGenerator) adjustInstructionJumps(instr *vdbe.Instruction, addrMap map[int]int) {
	// Opcodes that use P2 as a jump target
	jumpOpcodes := map[vdbe.Opcode]bool{
		vdbe.OpIf:        true,
		vdbe.OpIfNot:     true,
		vdbe.OpIfPos:     true,
		vdbe.OpIfNotZero: true,
		vdbe.OpIfNullRow: true,
		vdbe.OpIsNull:    true,
		vdbe.OpNotNull:   true,
		vdbe.OpGoto:      true,
		vdbe.OpGosub:     true,
		vdbe.OpRewind:    true,
		vdbe.OpNext:      true,
		vdbe.OpPrev:      true,
		vdbe.OpLast:      true,
		vdbe.OpFirst:     true,
		vdbe.OpSeekGE:    true,
		vdbe.OpSeekGT:    true,
		vdbe.OpSeekLE:    true,
		vdbe.OpSeekLT:    true,
		vdbe.OpSeekRowid: true,
		vdbe.OpNotExists: true,
		vdbe.OpSorterSort: true,
		vdbe.OpSorterNext: true,
		vdbe.OpOnce:       true,
	}

	// Adjust P2 for jump opcodes
	if jumpOpcodes[instr.Opcode] && instr.P2 > 0 {
		if mapped, ok := addrMap[instr.P2]; ok {
			instr.P2 = mapped
		}
	}

	// Adjust both P2 and P3 for InitCoroutine
	if instr.Opcode == vdbe.OpInitCoroutine {
		if instr.P2 > 0 {
			if mapped, ok := addrMap[instr.P2]; ok {
				instr.P2 = mapped
			}
		}
		if instr.P3 > 0 {
			if mapped, ok := addrMap[instr.P3]; ok {
				instr.P3 = mapped
			}
		}
	}
}

// adjustSubqueryJumpTargets adjusts all jump targets in the subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE at address baseAddr,
// all absolute jump targets (in P2) must be adjusted by adding baseAddr.
// This ensures jumps land at the correct locations in the combined program.
//
// Jump targets are used in opcodes like:
// - OpGoto, OpGosub: P2 = absolute jump target
// - OpIf, OpIfNot, OpIfPos, OpIfNeg: P2 = jump if condition met
// - OpOnce: P2 = jump if already executed
// - OpRewind, OpNext, OpPrev, OpLast, OpFirst: P2 = jump on end/no rows
// - OpSeekGE, OpSeekGT, OpSeekLE, OpSeekLT: P2 = jump if not found
// - OpSeekRowid, OpNotExists: P2 = jump if not found
// - OpInitCoroutine: P2 = jump past coroutine body, P3 = entry point
// - OpSorterSort, OpSorterNext: P2 = jump when done
func (g *CodeGenerator) adjustSubqueryJumpTargets(subVM *vdbe.VDBE, baseAddr int) {
	if baseAddr == 0 {
		return // No adjustment needed
	}

	// Opcodes that use P2 as a jump target
	jumpOpcodes := map[vdbe.Opcode]bool{
		// Conditional jumps
		vdbe.OpIf:        true,
		vdbe.OpIfNot:     true,
		vdbe.OpIfPos:     true,
		vdbe.OpIfNotZero: true,
		vdbe.OpIfNullRow: true,
		vdbe.OpIsNull:    true, // Jump if P1 is NULL
		vdbe.OpNotNull:   true, // Jump if P1 is not NULL

		// Unconditional jumps
		vdbe.OpGoto:  true,
		vdbe.OpGosub: true,

		// Loop control
		vdbe.OpRewind: true,
		vdbe.OpNext:   true,
		vdbe.OpPrev:   true,
		vdbe.OpLast:   true,
		vdbe.OpFirst:  true,

		// Seek operations
		vdbe.OpSeekGE:    true,
		vdbe.OpSeekGT:    true,
		vdbe.OpSeekLE:    true,
		vdbe.OpSeekLT:    true,
		vdbe.OpSeekRowid: true,
		vdbe.OpNotExists: true,

		// Sorter operations
		vdbe.OpSorterSort: true,
		vdbe.OpSorterNext: true,

		// Special control flow
		vdbe.OpOnce: true,
	}

	// Opcodes that use both P2 and P3 as jump targets
	dualJumpOpcodes := map[vdbe.Opcode]bool{
		vdbe.OpInitCoroutine: true, // P2 = skip address, P3 = entry point
	}

	for i := range subVM.Program {
		op := subVM.Program[i].Opcode

		// Adjust P2 for jump opcodes
		if jumpOpcodes[op] && subVM.Program[i].P2 > 0 {
			subVM.Program[i].P2 += baseAddr
		}

		// Adjust both P2 and P3 for dual-jump opcodes
		if dualJumpOpcodes[op] {
			if subVM.Program[i].P2 > 0 {
				subVM.Program[i].P2 += baseAddr
			}
			if subVM.Program[i].P3 > 0 {
				subVM.Program[i].P3 += baseAddr
			}
		}
	}
}

// adjustSubqueryCursors adjusts all cursor references in subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE, cursor numbers from the
// subquery must be offset to avoid colliding with cursors already allocated in the
// parent VDBE. This function adds cursorOffset to all P1, P2, P3 fields that reference cursors.
func (g *CodeGenerator) adjustSubqueryCursors(subVM *vdbe.VDBE, cursorOffset int) {
	if cursorOffset == 0 {
		return // No adjustment needed
	}

	// Opcodes where P1 is a cursor number
	cursorP1Opcodes := map[vdbe.Opcode]bool{
		vdbe.OpOpenRead:      true,
		vdbe.OpOpenWrite:     true,
		vdbe.OpOpenEphemeral: true,
		vdbe.OpRewind:        true,
		vdbe.OpNext:          true,
		vdbe.OpPrev:          true,
		vdbe.OpLast:          true,
		vdbe.OpFirst:         true,
		vdbe.OpColumn:        true,
		vdbe.OpClose:         true,
		vdbe.OpSeekRowid:     true,
		vdbe.OpSeekGE:        true,
		vdbe.OpSeekGT:        true,
		vdbe.OpSeekLE:        true,
		vdbe.OpSeekLT:        true,
		vdbe.OpInsert:        true,
		vdbe.OpDelete:        true,
		vdbe.OpUpdate:        true,
		vdbe.OpMakeRecord:    true,
		vdbe.OpSorterOpen:    true,
		vdbe.OpSorterInsert:  true,
		vdbe.OpSorterNext:    true,
		vdbe.OpSorterSort:    true,
		vdbe.OpSorterData:    true,
		vdbe.OpSorterClose:   true,
	}

	for i := range subVM.Program {
		op := subVM.Program[i].Opcode

		// Adjust P1 if it's a cursor number
		if cursorP1Opcodes[op] {
			subVM.Program[i].P1 += cursorOffset
		}
	}
}

// adjustSubqueryRegisters adjusts all register references in subquery bytecode
// using a table-driven approach. When subquery bytecode is embedded into a parent
// VDBE, register numbers from the subquery (starting from 1) must be offset to
// avoid colliding with registers already allocated in the parent VDBE.
func (g *CodeGenerator) adjustSubqueryRegisters(subVM *vdbe.VDBE, regOffset int) {
	if regOffset == 0 {
		return // No adjustment needed
	}

	for i := range subVM.Program {
		g.adjustInstructionRegisters(subVM.Program[i], regOffset)
	}
}

// adjustInstructionRegisters adjusts register references in a single instruction.
func (g *CodeGenerator) adjustInstructionRegisters(instr *vdbe.Instruction, regOffset int) {
	rule, ok := registerAdjustmentRules[instr.Opcode]
	if !ok {
		return // No register adjustment needed for this opcode
	}

	if rule.adjustP1 {
		instr.P1 += regOffset
	}

	if rule.adjustP3 {
		instr.P3 += regOffset
	}
}
