// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package expr

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
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
	parser.OpLike:              (*CodeGenerator).generateLikeExpr,
	parser.OpGlob:              (*CodeGenerator).generateGlobExpr,
	parser.OpIs:                (*CodeGenerator).generateIsExpr,
	parser.OpIsNot:             (*CodeGenerator).generateIsNotExpr,
	parser.OpIsDistinctFrom:    (*CodeGenerator).generateIsNotExpr,
	parser.OpIsNotDistinctFrom: (*CodeGenerator).generateIsExpr,
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

// ============================================================================
// Register Adjustment Rules for Subquery Compilation
// ============================================================================
//
// SCAFFOLDING: These types and mappings support subquery bytecode embedding.
// Currently unused because subqueries use coroutine-based execution.
// Will be activated when implementing:
// 1. Subquery flattening optimization
// 2. Inline scalar subquery evaluation
// ============================================================================

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
		reflect.TypeOf((*parser.RaiseExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateRaise(e.(*parser.RaiseExpr))
		},
	}
}

// ColumnInfo contains column metadata for code generation.
type ColumnInfo struct {
	Name      string
	Index     int    // Column index in the record
	IsRowid   bool   // True if this is the INTEGER PRIMARY KEY (alias for rowid)
	Collation string // Column collation (e.g., NOCASE, BINARY, RTRIM)
}

// TableInfo contains table metadata for code generation.
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

// SubqueryCompiler is a callback function to compile subquery SELECT statements.
// It takes a SELECT AST node and returns compiled VDBE bytecode.
type SubqueryCompiler func(selectStmt *parser.SelectStmt) (*vdbe.VDBE, error)

// SubqueryExecutor compiles and executes a subquery, returning the result rows.
// Each row is a slice of interface{} values (one per result column).
type SubqueryExecutor func(selectStmt *parser.SelectStmt) ([][]interface{}, error)

// CodeGenerator generates VDBE bytecode for expressions.
// It converts parser AST nodes into executable VDBE instructions.
type CodeGenerator struct {
	vdbe             *vdbe.VDBE
	nextReg          int
	nextCursor       int                       // next cursor index to allocate for subqueries
	cursorMap        map[string]int            // table name -> cursor number
	tableInfo        map[string]TableInfo      // table name -> table info
	args             []interface{}             // bound parameter values
	paramIdx         int                       // next parameter index to use
	collations       map[int]string            // register -> collation name (for collate expressions)
	subqueryCompiler SubqueryCompiler          // callback to compile subqueries
	subqueryExecutor SubqueryExecutor          // callback to materialise subqueries
	precomputed      map[parser.Expression]int // expressions pre-computed into registers
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

// SetSubqueryExecutor sets the callback for materialising subqueries.
func (g *CodeGenerator) SetSubqueryExecutor(executor SubqueryExecutor) {
	g.subqueryExecutor = executor
}

// SetPrecomputed registers an expression as already computed in a given register.
// When GenerateExpr encounters this expression, it returns the register directly.
func (g *CodeGenerator) SetPrecomputed(e parser.Expression, reg int) {
	if g.precomputed == nil {
		g.precomputed = make(map[parser.Expression]int)
	}
	g.precomputed[e] = reg
}

// GetVDBE returns the underlying VDBE for access to context.
func (g *CodeGenerator) GetVDBE() *vdbe.VDBE {
	return g.vdbe
}

// HasNonZeroCursor returns true if any registered cursor is non-zero.
// This indicates that column references should be routed through the code
// generator to use the correct cursor (e.g. INSERT..SELECT source cursor).
func (g *CodeGenerator) HasNonZeroCursor() bool {
	for _, cursor := range g.cursorMap {
		if cursor != 0 {
			return true
		}
	}
	return false
}

// SetArgs sets the bound parameter values for the code generator.
func (g *CodeGenerator) SetArgs(args []interface{}) {
	g.args = args
	g.paramIdx = 0
}

// SetParamIndex sets the next parameter index to consume.
func (g *CodeGenerator) SetParamIndex(idx int) {
	g.paramIdx = idx
}

// ParamIndex returns the next parameter index to consume.
func (g *CodeGenerator) ParamIndex() int {
	return g.paramIdx
}

// CollationForReg returns the collation name for a register, if any.
func (g *CodeGenerator) CollationForReg(reg int) (string, bool) {
	coll, ok := g.collations[reg]
	return coll, ok
}

// SetCollationForReg assigns a collation name to a register.
func (g *CodeGenerator) SetCollationForReg(reg int, coll string) {
	if coll == "" {
		return
	}
	g.collations[reg] = coll
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
	// Check if this expression was pre-computed into a register
	if reg, ok := g.precomputed[expr]; ok {
		return reg, nil
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
	clean := strings.ReplaceAll(e.Value, "_", "")
	base := 10
	if strings.HasPrefix(clean, "0x") || strings.HasPrefix(clean, "0X") {
		base = 16
		clean = clean[2:]
	}
	val, err := strconv.ParseInt(clean, base, 64)
	if err != nil {
		floatVal, floatErr := strconv.ParseFloat(strings.ReplaceAll(e.Value, "_", ""), 64)
		if floatErr != nil {
			return fmt.Errorf("invalid integer literal %q", e.Value)
		}
		addr := g.vdbe.AddOp(vdbe.OpReal, 0, reg, 0)
		g.vdbe.Program[addr].P4.R = floatVal
		g.vdbe.Program[addr].P4Type = vdbe.P4Real
		g.vdbe.SetComment(addr, fmt.Sprintf("REAL %s", e.Value))
		return nil
	}
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

	// Look up and store column collation for comparison operations
	if collation := g.lookupColumnCollation(tableName, e.Name); collation != "" {
		g.collations[reg] = collation
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

// isRowidAlias checks if a column name is one of the special rowid aliases.
// SQLite recognizes "rowid", "_rowid_", and "oid" as aliases for the rowid.
func isRowidAlias(colName string) bool {
	lower := strings.ToLower(colName)
	return lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

// findTableWithColumn searches for a table that contains the given column name.
// It also handles special rowid aliases (rowid, _rowid_, oid) by checking if
// the table has any rowid column (INTEGER PRIMARY KEY).
func (g *CodeGenerator) findTableWithColumn(colName string) (string, int) {
	// First try exact column name match
	for name, info := range g.tableInfo {
		for _, col := range info.Columns {
			if col.Name == colName {
				return name, g.cursorMap[name]
			}
		}
	}

	// If no exact match and this is a rowid alias, find a table with a rowid column
	if isRowidAlias(colName) {
		// First, prefer a table that has an explicit INTEGER PRIMARY KEY
		for name, info := range g.tableInfo {
			for _, col := range info.Columns {
				if col.IsRowid {
					return name, g.cursorMap[name]
				}
			}
		}
		// If no explicit INTEGER PRIMARY KEY, return the first available table
		// since SQLite always has an implicit rowid for regular tables
		for name := range g.tableInfo {
			return name, g.cursorMap[name]
		}
	}

	return "", 0
}

// lookupColumnInfo finds the column index and rowid flag for a column in a table.
// It handles both exact column name matches and special rowid aliases.
func (g *CodeGenerator) lookupColumnInfo(tableName, colName string) (int, bool, error) {
	info, ok := g.tableInfo[tableName]
	if !ok {
		return 0, false, nil
	}

	// First try exact column name match
	for _, col := range info.Columns {
		if col.Name == colName {
			return col.Index, col.IsRowid, nil
		}
	}

	// If no exact match but this is a rowid alias, find the rowid column
	if isRowidAlias(colName) {
		for _, col := range info.Columns {
			if col.IsRowid {
				return col.Index, true, nil
			}
		}
		// If no INTEGER PRIMARY KEY column exists, still return rowid
		// (SQLite always has an implicit rowid for regular tables)
		return 0, true, nil
	}

	return 0, false, fmt.Errorf("column not found: %s", colName)
}

// lookupColumnCollation returns the collation for a column in a table.
// Returns empty string if table not found or column has no explicit collation.
func (g *CodeGenerator) lookupColumnCollation(tableName, colName string) string {
	info, ok := g.tableInfo[tableName]
	if !ok {
		return ""
	}
	for _, col := range info.Columns {
		if col.Name == colName {
			return col.Collation
		}
	}
	return ""
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

	// LIKE with ESCAPE clause needs 3 arguments
	if e.Op == parser.OpLike && e.Escape != nil {
		return g.generateLikeEscapeExpr(e)
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
		p1, p2 := leftReg, rightReg
		// SQLite shift opcodes use P1=shift amount (right operand),
		// P2=value to shift (left operand), so swap the registers.
		if entry.op == vdbe.OpShiftLeft || entry.op == vdbe.OpShiftRight {
			p1, p2 = rightReg, leftReg
		}
		g.vdbe.AddOp(entry.op, p1, p2, resultReg)
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
	// Generate left operand
	leftReg, err := g.GenerateExpr(e.Left)
	if err != nil {
		return 0, err
	}

	resultReg := g.AllocReg()
	// Copy left result to result register
	g.vdbe.AddOp(vdbe.OpCopy, leftReg, resultReg, 0)

	var shortCircuitAddr int
	if e.Op == parser.OpAnd {
		// AND: if left is false, skip right (short-circuit).
		// P3=1: do not jump when left is NULL (need to evaluate right side).
		shortCircuitAddr = g.vdbe.AddOp(vdbe.OpIfNot, leftReg, 0, 1)
		g.vdbe.SetComment(shortCircuitAddr, "AND short-circuit")
	} else {
		// OR: if left is true, skip right (short-circuit)
		shortCircuitAddr = g.vdbe.AddOp(vdbe.OpIf, leftReg, 0, 0)
		g.vdbe.SetComment(shortCircuitAddr, "OR short-circuit")
	}

	// Generate right operand
	rightReg, err := g.GenerateExpr(e.Right)
	if err != nil {
		return 0, err
	}

	// Combine results
	if e.Op == parser.OpAnd {
		g.vdbe.AddOp(vdbe.OpAnd, leftReg, rightReg, resultReg)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "AND")
	} else {
		g.vdbe.AddOp(vdbe.OpOr, leftReg, rightReg, resultReg)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "OR")
	}

	// Fix short-circuit jump target to after the combine
	endAddr := g.vdbe.NumOps()
	g.vdbe.Program[shortCircuitAddr].P2 = endAddr

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

	// Evaluate the CASE expression once if present (for simple CASE)
	caseExprReg, err := g.evaluateCaseExpr(e)
	if err != nil {
		return 0, err
	}

	// Generate code for each WHEN clause
	endJumps, err := g.generateWhenClauses(e, caseExprReg, resultReg)
	if err != nil {
		return 0, err
	}

	// Generate ELSE clause (or NULL if not present)
	if err := g.generateElseClause(e, resultReg); err != nil {
		return 0, err
	}

	// Patch all end jumps
	g.patchJumpsToEnd(endJumps)

	return resultReg, nil
}

// evaluateCaseExpr evaluates the CASE expression for simple CASE statements.
// Returns the register containing the evaluated expression, or 0 if not present.
func (g *CodeGenerator) evaluateCaseExpr(e *parser.CaseExpr) (int, error) {
	if e.Expr == nil {
		return 0, nil
	}
	return g.GenerateExpr(e.Expr)
}

// generateWhenClauses generates code for all WHEN clauses.
// Returns a slice of jump addresses that need to be patched to point to the end.
func (g *CodeGenerator) generateWhenClauses(e *parser.CaseExpr, caseExprReg, resultReg int) ([]int, error) {
	var endJumps []int

	for _, when := range e.WhenClauses {
		// Generate condition evaluation
		condReg, err := g.generateWhenCondition(e, &when, caseExprReg)
		if err != nil {
			return nil, err
		}

		// Generate THEN clause and collect end jump
		endJump, err := g.generateThenClause(&when, condReg, resultReg)
		if err != nil {
			return nil, err
		}
		endJumps = append(endJumps, endJump)
	}

	return endJumps, nil
}

// generateWhenCondition generates code for a single WHEN condition.
// For simple CASE, compares the case expression with the when value.
// For searched CASE, evaluates the condition directly.
func (g *CodeGenerator) generateWhenCondition(e *parser.CaseExpr, when *parser.WhenClause, caseExprReg int) (int, error) {
	if e.Expr != nil {
		// Simple CASE: compare CASE expression with WHEN value
		return g.generateSimpleCaseCondition(when, caseExprReg)
	}
	// Searched CASE: evaluate WHEN condition directly
	return g.GenerateExpr(when.Condition)
}

// generateSimpleCaseCondition generates comparison code for simple CASE.
func (g *CodeGenerator) generateSimpleCaseCondition(when *parser.WhenClause, caseExprReg int) (int, error) {
	whenValueReg, err := g.GenerateExpr(when.Condition)
	if err != nil {
		return 0, err
	}
	condReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpEq, caseExprReg, whenValueReg, condReg)
	return condReg, nil
}

// generateThenClause generates code for a THEN clause.
// Returns the address of the jump to end that needs to be patched.
func (g *CodeGenerator) generateThenClause(when *parser.WhenClause, condReg, resultReg int) (int, error) {
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
	endJump := g.vdbe.NumOps() - 1

	// Patch jump to next WHEN
	g.vdbe.Program[jumpToNext].P2 = g.vdbe.NumOps()

	return endJump, nil
}

// generateElseClause generates code for the ELSE clause or NULL if not present.
func (g *CodeGenerator) generateElseClause(e *parser.CaseExpr, resultReg int) error {
	if e.ElseClause != nil {
		elseReg, err := g.GenerateExpr(e.ElseClause)
		if err != nil {
			return err
		}
		g.vdbe.AddOp(vdbe.OpCopy, elseReg, resultReg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	}
	return nil
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
	nullSeenReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpInteger, 0, nullSeenReg, 0)

	var endJumps []int

	for _, val := range e.Values {
		jumpAddr, err := g.generateInValueComparison(exprReg, val, resultReg, nullSeenReg)
		if err != nil {
			return 0, err
		}
		endJumps = append(endJumps, jumpAddr)
	}

	// If any comparison was NULL and no match found, result is NULL.
	setNullAddr := g.vdbe.AddOp(vdbe.OpIfNot, nullSeenReg, 0, 0)
	g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	endAddr := g.vdbe.NumOps()
	g.vdbe.Program[setNullAddr].P2 = endAddr

	g.patchJumpsToEnd(endJumps)
	return resultReg, nil
}

// generateInValueComparison generates code to compare exprReg with a single IN value.
// Returns the address of the jump to the end on match.
func (g *CodeGenerator) generateInValueComparison(exprReg int, val parser.Expression, resultReg int, nullSeenReg int) (int, error) {
	valReg, err := g.GenerateExpr(val)
	if err != nil {
		return 0, err
	}

	// Compare
	cmpReg := g.AllocReg()
	collation := g.getCollationForOperands(exprReg, valReg)
	addr := g.vdbe.AddOp(vdbe.OpEq, exprReg, valReg, cmpReg)
	if collation != "" {
		g.vdbe.Program[addr].P4.Z = collation
		g.vdbe.Program[addr].P4Type = vdbe.P4Static
	}

	// If true, set result to true and jump to end
	isNullAddr := g.vdbe.AddOp(vdbe.OpIsNull, cmpReg, 0, 0)
	ifAddr := g.vdbe.AddOp(vdbe.OpIfNot, cmpReg, 0, 0)

	g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
	gotoAddr := g.vdbe.NumOps() - 1

	// Patch the If jump to next iteration
	setNullAddr := g.vdbe.NumOps()
	g.vdbe.AddOp(vdbe.OpInteger, 1, nullSeenReg, 0)
	nextIterAddr := g.vdbe.NumOps()
	g.vdbe.Program[ifAddr].P2 = nextIterAddr
	g.vdbe.Program[isNullAddr].P2 = setNullAddr

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
// When a SubqueryExecutor is available, the subquery is materialised at compile
// time and converted to a value-list comparison. Otherwise falls back to
// bytecode embedding via ephemeral tables.
func (g *CodeGenerator) generateInSubquery(e *parser.InExpr, exprReg int, resultReg int) (int, error) {
	// Prefer materialisation: execute the subquery, then emit value-list checks.
	if g.subqueryExecutor != nil {
		res, err := g.generateInSubqueryMaterialised(e, exprReg, resultReg)
		if err == nil {
			return res, nil
		}
		// Fall through to bytecode embedding
	}

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

// generateInSubqueryMaterialised executes the subquery at compile time and
// emits value-list comparisons for the collected results. Falls back to
// bytecode embedding on failure.
func (g *CodeGenerator) generateInSubqueryMaterialised(e *parser.InExpr, exprReg int, resultReg int) (int, error) {
	rows, err := g.subqueryExecutor(e.Select)
	if err != nil {
		// Fall back to bytecode embedding for correlated subqueries
		return g.generateInSubquery(e, exprReg, resultReg)
	}

	// Convert rows to a value-list InExpr and generate code for it.
	var values []parser.Expression
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		values = append(values, valueToLiteral(row[0]))
	}

	synth := &parser.InExpr{Expr: e.Expr, Values: values, Not: false}
	return g.generateInValueList(synth, exprReg)
}

// valueToLiteral converts an interface{} value to a parser literal expression.
func valueToLiteral(v interface{}) parser.Expression {
	switch val := v.(type) {
	case int64:
		return &parser.LiteralExpr{Type: parser.LiteralInteger, Value: fmt.Sprintf("%d", val)}
	case float64:
		return &parser.LiteralExpr{Type: parser.LiteralFloat, Value: fmt.Sprintf("%g", val)}
	case string:
		return &parser.LiteralExpr{Type: parser.LiteralString, Value: val}
	case nil:
		return &parser.LiteralExpr{Type: parser.LiteralNull, Value: "NULL"}
	default:
		return &parser.LiteralExpr{Type: parser.LiteralString, Value: fmt.Sprintf("%v", val)}
	}
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
// When a SubqueryExecutor is available, the subquery is materialised at compile
// time. Otherwise falls back to bytecode embedding.
func (g *CodeGenerator) generateSubquery(e *parser.SubqueryExpr) (int, error) {
	if e.Select == nil {
		return 0, fmt.Errorf("subquery expression has no SELECT statement")
	}

	// Prefer materialisation.
	if g.subqueryExecutor != nil {
		return g.generateSubqueryMaterialised(e)
	}

	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}

	resultReg := g.AllocReg()
	g.initSubqueryResult(resultReg)

	subVM, err := g.subqueryCompiler(e.Select)
	if err != nil {
		return 0, fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	addrSubqueryStart := g.vdbe.NumOps()
	addrMap := g.buildSubqueryAddressMap(subVM, addrSubqueryStart)
	resultRowGotoAddrs := g.copySubqueryInstructions(subVM, addrMap, resultReg)
	g.patchResultRowGotos(resultRowGotoAddrs)

	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: end")

	return resultReg, nil
}

// generateSubqueryMaterialised executes a scalar subquery at compile time and
// emits a constant value. Falls back to bytecode embedding on failure.
func (g *CodeGenerator) generateSubqueryMaterialised(e *parser.SubqueryExpr) (int, error) {
	// Check for correlated subquery FIRST - outer refs mean we must evaluate per-row.
	refs := g.findOuterRefs(e.Select)
	if len(refs) > 0 {
		return g.emitCorrelatedScalar(e, refs)
	}

	rows, err := g.subqueryExecutor(e.Select)
	if err != nil {
		return g.generateSubqueryBytecodeEmbedding(e)
	}

	resultReg := g.AllocReg()
	if len(rows) == 0 || len(rows[0]) == 0 {
		g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
		return resultReg, nil
	}

	g.emitLiteralValue(resultReg, rows[0][0])
	return resultReg, nil
}

// generateSubqueryBytecodeEmbedding falls back to bytecode embedding for
// scalar subqueries (typically correlated ones).
func (g *CodeGenerator) generateSubqueryBytecodeEmbedding(e *parser.SubqueryExpr) (int, error) {
	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}
	resultReg := g.AllocReg()
	g.initSubqueryResult(resultReg)

	subVM, err := g.subqueryCompiler(e.Select)
	if err != nil {
		return 0, fmt.Errorf("failed to compile scalar subquery: %w", err)
	}

	addrSubqueryStart := g.vdbe.NumOps()
	addrMap := g.buildSubqueryAddressMap(subVM, addrSubqueryStart)
	resultRowGotoAddrs := g.copySubqueryInstructions(subVM, addrMap, resultReg)
	g.patchResultRowGotos(resultRowGotoAddrs)

	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: end")

	return resultReg, nil
}

// emitLiteralValue emits an opcode to load a concrete value into a register.
func (g *CodeGenerator) emitLiteralValue(reg int, v interface{}) {
	switch val := v.(type) {
	case int64:
		g.vdbe.AddOp(vdbe.OpInteger, int(val), reg, 0)
	case float64:
		g.vdbe.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, val)
	case string:
		g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, val)
	case nil:
		g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
	default:
		g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, fmt.Sprintf("%v", val))
	}
}

// initSubqueryResult initializes the result register to NULL.
func (g *CodeGenerator) initSubqueryResult(resultReg int) {
	g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: init result to NULL")
}

// buildSubqueryAddressMap builds a mapping from subquery addresses to parent addresses.
// This accounts for instruction replacements (e.g., OpResultRow becomes 2 instructions).
func (g *CodeGenerator) buildSubqueryAddressMap(subVM *vdbe.VDBE, startAddr int) map[int]int {
	addrMap := make(map[int]int)
	currentAddr := startAddr
	for i := 0; i < subVM.NumOps(); i++ {
		addrMap[i] = currentAddr
		if subVM.Program[i].Opcode == vdbe.OpResultRow {
			currentAddr += 2 // Becomes OpCopy + OpGoto
		} else {
			currentAddr += 1 // Copied as-is or replaced with 1 instruction
		}
	}
	addrMap[subVM.NumOps()] = currentAddr
	return addrMap
}

// copySubqueryInstructions copies and adjusts subquery bytecode into parent VDBE.
// Returns the addresses of ResultRow Goto instructions that need patching.
func (g *CodeGenerator) copySubqueryInstructions(subVM *vdbe.VDBE, addrMap map[int]int, resultReg int) []int {
	var resultRowGotoAddrs []int
	for i := 0; i < subVM.NumOps(); i++ {
		instr := subVM.Program[i]
		switch instr.Opcode {
		case vdbe.OpResultRow:
			resultRowGotoAddrs = g.emitResultRowReplacement(instr, resultReg, resultRowGotoAddrs)
		case vdbe.OpHalt:
			g.emitHaltReplacement()
		default:
			g.copyAndAdjustInstruction(instr, addrMap)
		}
	}
	return resultRowGotoAddrs
}

// emitResultRowReplacement replaces OpResultRow with OpCopy + OpGoto.
func (g *CodeGenerator) emitResultRowReplacement(instr *vdbe.Instruction, resultReg int, gotoAddrs []int) []int {
	g.vdbe.AddOp(vdbe.OpCopy, instr.P1, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: copy result")
	gotoAddrs = append(gotoAddrs, g.vdbe.NumOps())
	g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: done, skip to end")
	return gotoAddrs
}

// emitHaltReplacement replaces OpHalt with OpNoop.
func (g *CodeGenerator) emitHaltReplacement() {
	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "Scalar subquery: stripped Halt")
}

// copyAndAdjustInstruction copies an instruction and adjusts its jump targets.
func (g *CodeGenerator) copyAndAdjustInstruction(instr *vdbe.Instruction, addrMap map[int]int) {
	instrCopy := *instr
	g.adjustInstructionJumps(&instrCopy, addrMap)
	g.vdbe.Program = append(g.vdbe.Program, &instrCopy)
}

// patchResultRowGotos patches all ResultRow Goto addresses to point to the end.
func (g *CodeGenerator) patchResultRowGotos(gotoAddrs []int) {
	addrAfterSubquery := g.vdbe.NumOps()
	for _, addr := range gotoAddrs {
		g.vdbe.Program[addr].P2 = addrAfterSubquery
	}
}

// generateLikeEscapeExpr generates code for LIKE with ESCAPE clause.
func (g *CodeGenerator) generateLikeEscapeExpr(e *parser.BinaryExpr) (int, error) {
	leftReg, err := g.GenerateExpr(e.Left)
	if err != nil {
		return 0, err
	}
	rightReg, err := g.GenerateExpr(e.Right)
	if err != nil {
		return 0, err
	}
	escapeReg, err := g.GenerateExpr(e.Escape)
	if err != nil {
		return 0, err
	}

	resultReg := g.AllocReg()
	firstArg := g.AllocRegs(3)

	if leftReg != firstArg {
		g.vdbe.AddOp(vdbe.OpMove, leftReg, firstArg, 0)
	}
	if rightReg != firstArg+1 {
		g.vdbe.AddOp(vdbe.OpMove, rightReg, firstArg+1, 0)
	}
	if escapeReg != firstArg+2 {
		g.vdbe.AddOp(vdbe.OpMove, escapeReg, firstArg+2, 0)
	}

	addr := g.vdbe.AddOp(vdbe.OpFunction, 0, firstArg, resultReg)
	g.vdbe.Program[addr].P4.Z = "like"
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.Program[addr].P5 = 3
	g.vdbe.SetComment(addr, "LIKE ESCAPE")
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

// generateIsExpr generates null-safe IS comparison.
// NULL IS NULL → 1, NULL IS x → 0, x IS NULL → 0, else normal equality.
func (g *CodeGenerator) generateIsExpr(leftReg, rightReg int) (int, error) {
	return g.generateNullSafeCompare(leftReg, rightReg, true)
}

// generateIsNotExpr generates null-safe IS NOT comparison.
// NULL IS NOT NULL → 0, NULL IS NOT x → 1, x IS NOT NULL → 1, else normal inequality.
func (g *CodeGenerator) generateIsNotExpr(leftReg, rightReg int) (int, error) {
	return g.generateNullSafeCompare(leftReg, rightReg, false)
}

// generateNullSafeCompare emits bytecode for IS (eq=true) or IS NOT (eq=false).
// Sequence: check left null, check right null, compare non-null values.
func (g *CodeGenerator) generateNullSafeCompare(leftReg, rightReg int, eq bool) (int, error) {
	resultReg := g.AllocReg()

	// Check if left is NULL → jump to leftIsNull
	leftNullAddr := g.vdbe.AddOp(vdbe.OpIsNull, leftReg, 0, 0)

	// Left is NOT NULL: check if right is NULL
	rightNullAddr := g.vdbe.AddOp(vdbe.OpIsNull, rightReg, 0, 0)

	// Both non-NULL: do normal comparison
	if eq {
		g.vdbe.AddOp(vdbe.OpEq, leftReg, rightReg, resultReg)
	} else {
		g.vdbe.AddOp(vdbe.OpNe, leftReg, rightReg, resultReg)
	}
	doneAddr := g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)

	// rightIsNull: left NOT NULL, right NULL → IS=0, IS NOT=1
	rightNullResult := g.vdbe.NumOps()
	g.vdbe.Program[rightNullAddr].P2 = rightNullResult
	if eq {
		g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	}
	doneAddr2 := g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)

	// leftIsNull: check if right is also NULL
	leftNullBlock := g.vdbe.NumOps()
	g.vdbe.Program[leftNullAddr].P2 = leftNullBlock
	rightAlsoNullAddr := g.vdbe.AddOp(vdbe.OpIsNull, rightReg, 0, 0)

	// left NULL, right NOT NULL → IS=0, IS NOT=1
	if eq {
		g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	}
	doneAddr3 := g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)

	// Both NULL → IS=1, IS NOT=0
	bothNullBlock := g.vdbe.NumOps()
	g.vdbe.Program[rightAlsoNullAddr].P2 = bothNullBlock
	if eq {
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	}

	// Patch all done gotos
	endAddr := g.vdbe.NumOps()
	g.vdbe.Program[doneAddr].P2 = endAddr
	g.vdbe.Program[doneAddr2].P2 = endAddr
	g.vdbe.Program[doneAddr3].P2 = endAddr

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
// When a SubqueryExecutor is available, the subquery is materialised at compile
// time to determine existence. Otherwise falls back to bytecode embedding.
func (g *CodeGenerator) generateExists(e *parser.ExistsExpr) (int, error) {
	if e.Select == nil {
		return 0, fmt.Errorf("EXISTS expression has no SELECT statement")
	}

	// Prefer materialisation.
	if g.subqueryExecutor != nil {
		return g.generateExistsMaterialised(e)
	}

	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}

	resultReg := g.AllocReg()
	g.initExistsResult(resultReg)

	selectWithLimit := g.applyExistsLimit(e.Select)
	subVM, err := g.subqueryCompiler(selectWithLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	addrSubqueryStart := g.vdbe.NumOps()
	g.adjustSubqueryJumpTargets(subVM, addrSubqueryStart)
	resultRowGotoAddrs := g.embedExistsSubquery(subVM, resultReg)
	g.finalizeExistsSubquery(resultReg, resultRowGotoAddrs)

	return g.applyExistsNegation(e.Not, resultReg), nil
}

// generateExistsMaterialised executes the subquery at compile time and emits a
// constant boolean result. Falls back to bytecode embedding on failure
// (e.g. correlated subqueries).
func (g *CodeGenerator) generateExistsMaterialised(e *parser.ExistsExpr) (int, error) {
	// Check for correlated subquery FIRST - outer refs mean we must evaluate per-row.
	refs := g.findOuterRefs(e.Select)
	if len(refs) > 0 {
		return g.emitCorrelatedExists(e, refs)
	}

	selectWithLimit := g.applyExistsLimit(e.Select)
	rows, err := g.subqueryExecutor(selectWithLimit)
	if err != nil {
		return g.generateExistsBytecodeEmbedding(e)
	}

	exists := len(rows) > 0
	resultReg := g.AllocReg()
	val := 0
	if exists {
		val = 1
	}

	if e.Not {
		if val == 1 {
			val = 0
		} else {
			val = 1
		}
	}

	g.vdbe.AddOp(vdbe.OpInteger, val, resultReg, 0)
	return resultReg, nil
}

// generateExistsBytecodeEmbedding falls back to bytecode embedding for EXISTS.
func (g *CodeGenerator) generateExistsBytecodeEmbedding(e *parser.ExistsExpr) (int, error) {
	if g.subqueryCompiler == nil {
		return 0, fmt.Errorf("subquery compiler not set")
	}
	resultReg := g.AllocReg()
	g.initExistsResult(resultReg)

	selectWithLimit := g.applyExistsLimit(e.Select)
	subVM, err := g.subqueryCompiler(selectWithLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to compile EXISTS subquery: %w", err)
	}

	addrSubqueryStart := g.vdbe.NumOps()
	g.adjustSubqueryJumpTargets(subVM, addrSubqueryStart)
	resultRowGotoAddrs := g.embedExistsSubquery(subVM, resultReg)
	g.finalizeExistsSubquery(resultReg, resultRowGotoAddrs)

	return g.applyExistsNegation(e.Not, resultReg), nil
}

// initExistsResult initializes the EXISTS result register to false.
func (g *CodeGenerator) initExistsResult(resultReg int) {
	g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: init result to false")
}

// applyExistsLimit adds LIMIT 1 to the SELECT if it doesn't have one.
// EXISTS only needs to check if at least one row exists.
func (g *CodeGenerator) applyExistsLimit(selectStmt *parser.SelectStmt) *parser.SelectStmt {
	selectWithLimit := *selectStmt
	if selectWithLimit.Limit == nil {
		selectWithLimit.Limit = &parser.LiteralExpr{
			Type:  parser.LiteralInteger,
			Value: "1",
		}
	}
	return &selectWithLimit
}

// embedExistsSubquery embeds subquery bytecode, replacing ResultRow and Halt opcodes.
// Returns the addresses of ResultRow Goto instructions that need patching.
func (g *CodeGenerator) embedExistsSubquery(subVM *vdbe.VDBE, resultReg int) []int {
	var resultRowGotoAddrs []int
	for i := 0; i < subVM.NumOps(); i++ {
		instr := subVM.Program[i]
		switch instr.Opcode {
		case vdbe.OpResultRow:
			resultRowGotoAddrs = g.emitExistsResultRow(resultReg, resultRowGotoAddrs)
		case vdbe.OpHalt:
			g.emitExistsHalt()
		default:
			instrCopy := *instr
			g.vdbe.Program = append(g.vdbe.Program, &instrCopy)
		}
	}
	g.nextReg += subVM.NumMem
	return resultRowGotoAddrs
}

// emitExistsResultRow emits code to set result to true and jump to end.
func (g *CodeGenerator) emitExistsResultRow(resultReg int, gotoAddrs []int) []int {
	g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: row found, set true")
	gotoAddrs = append(gotoAddrs, g.vdbe.NumOps())
	g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: done, skip to end")
	return gotoAddrs
}

// emitExistsHalt replaces Halt with Noop in EXISTS subquery.
func (g *CodeGenerator) emitExistsHalt() {
	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS: stripped Halt")
}

// finalizeExistsSubquery patches jump addresses and adds end marker.
func (g *CodeGenerator) finalizeExistsSubquery(resultReg int, resultRowGotoAddrs []int) {
	addrAfterSubquery := g.vdbe.NumOps()
	for _, addr := range resultRowGotoAddrs {
		g.vdbe.Program[addr].P2 = addrAfterSubquery
	}
	g.vdbe.AddOp(vdbe.OpNoop, 0, 0, 0)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "EXISTS subquery: end")
}

// applyExistsNegation handles NOT EXISTS by negating the result.
func (g *CodeGenerator) applyExistsNegation(not bool, resultReg int) int {
	if not {
		notReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpNot, resultReg, notReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NOT EXISTS: negate result")
		return notReg
	}
	return resultReg
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

// emitScalarParameterValue emits opcodes for int, int64, float64.
func (g *CodeGenerator) emitScalarParameterValue(reg int, arg interface{}) bool {
	switch v := arg.(type) {
	case int:
		g.emitIntValue(reg, v)
		return true
	case int64:
		g.emitInt64Value(reg, v)
		return true
	case float64:
		g.emitFloatValue(reg, v)
		return true
	}
	return false
}

// emitComplexParameterValue emits opcodes for string, []byte, bool.
func (g *CodeGenerator) emitComplexParameterValue(reg int, arg interface{}) bool {
	switch v := arg.(type) {
	case string:
		g.emitStringValue(reg, v)
		return true
	case []byte:
		g.emitBlobValue(reg, v)
		return true
	case bool:
		g.emitBoolValue(reg, v)
		return true
	}
	return false
}

// emitParameterValue emits the appropriate opcode for a parameter value.
func (g *CodeGenerator) emitParameterValue(reg int, arg interface{}) {
	if arg == nil {
		g.emitNullValue(reg)
		return
	}
	if g.emitScalarParameterValue(reg, arg) {
		return
	}
	if g.emitComplexParameterValue(reg, arg) {
		return
	}
	g.emitDefaultValue(reg, arg)
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

// jumpAdjustmentRule defines which parameters need jump target adjustment for an opcode.
type jumpAdjustmentRule struct {
	adjustP2 bool // Whether P2 is a jump target that needs adjustment
	adjustP3 bool // Whether P3 is a jump target that needs adjustment
}

// jumpAdjustmentRules maps opcodes to their jump target adjustment rules.
var jumpAdjustmentRules = map[vdbe.Opcode]jumpAdjustmentRule{
	// Conditional jumps
	vdbe.OpIf:        {adjustP2: true, adjustP3: false},
	vdbe.OpIfNot:     {adjustP2: true, adjustP3: false},
	vdbe.OpIfPos:     {adjustP2: true, adjustP3: false},
	vdbe.OpIfNotZero: {adjustP2: true, adjustP3: false},
	vdbe.OpIfNullRow: {adjustP2: true, adjustP3: false},
	vdbe.OpIsNull:    {adjustP2: true, adjustP3: false},
	vdbe.OpNotNull:   {adjustP2: true, adjustP3: false},
	// Unconditional jumps
	vdbe.OpGoto:  {adjustP2: true, adjustP3: false},
	vdbe.OpGosub: {adjustP2: true, adjustP3: false},
	// Loop control
	vdbe.OpRewind: {adjustP2: true, adjustP3: false},
	vdbe.OpNext:   {adjustP2: true, adjustP3: false},
	vdbe.OpPrev:   {adjustP2: true, adjustP3: false},
	vdbe.OpLast:   {adjustP2: true, adjustP3: false},
	vdbe.OpFirst:  {adjustP2: true, adjustP3: false},
	// Seek operations
	vdbe.OpSeekGE:    {adjustP2: true, adjustP3: false},
	vdbe.OpSeekGT:    {adjustP2: true, adjustP3: false},
	vdbe.OpSeekLE:    {adjustP2: true, adjustP3: false},
	vdbe.OpSeekLT:    {adjustP2: true, adjustP3: false},
	vdbe.OpSeekRowid: {adjustP2: true, adjustP3: false},
	vdbe.OpNotExists: {adjustP2: true, adjustP3: false},
	// Sorter operations
	vdbe.OpSorterSort: {adjustP2: true, adjustP3: false},
	vdbe.OpSorterNext: {adjustP2: true, adjustP3: false},
	// Special control flow
	vdbe.OpOnce:          {adjustP2: true, adjustP3: false},
	vdbe.OpInitCoroutine: {adjustP2: true, adjustP3: true},
}

// adjustInstructionJumps adjusts jump targets in a single instruction using the address map.
func (g *CodeGenerator) adjustInstructionJumps(instr *vdbe.Instruction, addrMap map[int]int) {
	rule, ok := jumpAdjustmentRules[instr.Opcode]
	if !ok {
		return // No jump adjustment needed for this opcode
	}

	if rule.adjustP2 {
		g.adjustJumpTarget(&instr.P2, addrMap)
	}

	if rule.adjustP3 {
		g.adjustJumpTarget(&instr.P3, addrMap)
	}
}

// adjustJumpTarget adjusts a single jump target parameter using the address map.
func (g *CodeGenerator) adjustJumpTarget(param *int, addrMap map[int]int) {
	if *param > 0 {
		if mapped, ok := addrMap[*param]; ok {
			*param = mapped
		}
	}
}

// adjustJumpP2 adjusts P2 jump target for an instruction.
func adjustJumpP2(instr *vdbe.Instruction, baseAddr int) {
	if instr.P2 > 0 {
		instr.P2 += baseAddr
	}
}

// adjustDualJump adjusts both P2 and P3 jump targets for an instruction.
func adjustDualJump(instr *vdbe.Instruction, baseAddr int) {
	if instr.P2 > 0 {
		instr.P2 += baseAddr
	}
	if instr.P3 > 0 {
		instr.P3 += baseAddr
	}
}

// adjustSubqueryJumpTargets adjusts all jump targets in the subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE at address baseAddr,
// all absolute jump targets (in P2) must be adjusted by adding baseAddr.
// This ensures jumps land at the correct locations in the combined program.
func (g *CodeGenerator) adjustSubqueryJumpTargets(subVM *vdbe.VDBE, baseAddr int) {
	if baseAddr == 0 {
		return // No adjustment needed
	}

	for i := range subVM.Program {
		instr := subVM.Program[i]
		rule, ok := jumpAdjustmentRules[instr.Opcode]
		if !ok {
			continue
		}

		if rule.adjustP2 && rule.adjustP3 {
			adjustDualJump(instr, baseAddr)
		} else if rule.adjustP2 {
			adjustJumpP2(instr, baseAddr)
		}
	}
}

// adjustSubqueryCursors adjusts all cursor references in subquery bytecode.
// When subquery bytecode is embedded into a parent VDBE, cursor numbers from the
// subquery must be offset to avoid colliding with cursors already allocated in the
// parent VDBE. This function adds cursorOffset to all P1, P2, P3 fields that reference cursors.
//
// SCAFFOLDING: See registerAdjustmentRules comment for when this will be activated.
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
//
// SCAFFOLDING: See registerAdjustmentRules comment for when this will be activated.
func (g *CodeGenerator) adjustSubqueryRegisters(subVM *vdbe.VDBE, regOffset int) {
	if regOffset == 0 {
		return // No adjustment needed
	}

	for i := range subVM.Program {
		g.adjustInstructionRegisters(subVM.Program[i], regOffset)
	}
}

// adjustInstructionRegisters adjusts register references in a single instruction.
//
// SCAFFOLDING: See registerAdjustmentRules comment for when this will be activated.
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

// generateRaise generates VDBE code for a RAISE expression in a trigger body.
// RAISE(IGNORE) -> OpRaise P1=0
// RAISE(ROLLBACK, msg) -> OpRaise P1=1, P4.Z=msg
// RAISE(ABORT, msg) -> OpRaise P1=2, P4.Z=msg
// RAISE(FAIL, msg) -> OpRaise P1=3, P4.Z=msg
func (g *CodeGenerator) generateRaise(e *parser.RaiseExpr) (int, error) {
	raiseType := int(e.Type)
	addr := g.vdbe.AddOp(vdbe.OpRaise, raiseType, 0, 0)
	if e.Message != "" {
		g.vdbe.Program[addr].P4.Z = e.Message
		g.vdbe.Program[addr].P4Type = vdbe.P4Static
	}
	// RAISE doesn't produce a value, but we need to return a register
	resultReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpNull, 0, resultReg, 0)
	return resultReg, nil
}
