package expr

import (
	"fmt"
	"reflect"

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
		reflect.TypeOf((*parser.VariableExpr)(nil)): func(g *CodeGenerator, e parser.Expression) (int, error) {
			return g.generateVariable(e.(*parser.VariableExpr))
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

// CodeGenerator generates VDBE bytecode for expressions.
// It converts parser AST nodes into executable VDBE instructions.
type CodeGenerator struct {
	vdbe      *vdbe.VDBE
	nextReg   int
	cursorMap map[string]int       // table name -> cursor number
	tableInfo map[string]TableInfo // table name -> table info
	args      []interface{}        // bound parameter values
	paramIdx  int                  // next parameter index to use
}

// NewCodeGenerator creates a new code generator.
func NewCodeGenerator(v *vdbe.VDBE) *CodeGenerator {
	return &CodeGenerator{
		vdbe:      v,
		nextReg:   1,
		cursorMap: make(map[string]int),
		tableInfo: make(map[string]TableInfo),
		args:      nil,
		paramIdx:  0,
	}
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

// generateLiteral generates code for literal values.
func (g *CodeGenerator) generateLiteral(e *parser.LiteralExpr) (int, error) {
	reg := g.AllocReg()

	switch e.Type {
	case parser.LiteralNull:
		g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NULL")

	case parser.LiteralInteger:
		// Parse integer value
		var val int64
		fmt.Sscanf(e.Value, "%d", &val)
		if val >= -2147483648 && val <= 2147483647 {
			g.vdbe.AddOp(vdbe.OpInteger, int(val), reg, 0)
		} else {
			g.vdbe.AddOpWithP4Int(vdbe.OpInt64, 0, reg, 0, int32(val))
		}
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("INT %s", e.Value))

	case parser.LiteralFloat:
		// Parse float value
		var val float64
		fmt.Sscanf(e.Value, "%f", &val)
		addr := g.vdbe.AddOp(vdbe.OpReal, 0, reg, 0)
		g.vdbe.Program[addr].P4.R = val
		g.vdbe.Program[addr].P4Type = vdbe.P4Real
		g.vdbe.SetComment(addr, fmt.Sprintf("REAL %s", e.Value))

	case parser.LiteralString:
		g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, e.Value)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("STRING '%s'", e.Value))

	case parser.LiteralBlob:
		g.vdbe.AddOpWithP4Str(vdbe.OpBlob, len(e.Value), reg, 0, e.Value)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "BLOB")

	default:
		return 0, fmt.Errorf("unsupported literal type: %v", e.Type)
	}

	return reg, nil
}

// generateColumn generates code for column references.
func (g *CodeGenerator) generateColumn(e *parser.IdentExpr) (int, error) {
	reg := g.AllocReg()

	// Look up cursor for table
	cursor := 0
	tableName := e.Table
	if tableName != "" {
		var ok bool
		cursor, ok = g.cursorMap[tableName]
		if !ok {
			return 0, fmt.Errorf("unknown table: %s", tableName)
		}
	} else {
		// No table qualifier - find the table that has this column
		for name, info := range g.tableInfo {
			for _, col := range info.Columns {
				if col.Name == e.Name {
					tableName = name
					cursor = g.cursorMap[name]
					break
				}
			}
			if tableName != "" {
				break
			}
		}
	}

	// Look up column index from table info
	colIndex := 0
	isRowid := false
	if info, ok := g.tableInfo[tableName]; ok {
		found := false
		for _, col := range info.Columns {
			if col.Name == e.Name {
				colIndex = col.Index
				isRowid = col.IsRowid
				found = true
				break
			}
		}
		if !found {
			return 0, fmt.Errorf("column not found: %s", e.Name)
		}
	}

	// For rowid columns, use OpRowid instead of OpColumn
	if isRowid {
		g.vdbe.AddOp(vdbe.OpRowid, cursor, reg, 0)
	} else {
		g.vdbe.AddOp(vdbe.OpColumn, cursor, colIndex, reg)
	}

	if tableName != "" {
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("r[%d]=%s.%s", reg, tableName, e.Name))
	} else {
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("r[%d]=%s", reg, e.Name))
	}

	return reg, nil
}

// generateBinary generates code for binary operations.
func (g *CodeGenerator) generateBinary(e *parser.BinaryExpr) (int, error) {
	// Special handling for AND and OR (short-circuit evaluation)
	if e.Op == parser.OpAnd || e.Op == parser.OpOr {
		return g.generateLogical(e)
	}

	leftReg, err := g.GenerateExpr(e.Left)
	if err != nil {
		return 0, err
	}
	rightReg, err := g.GenerateExpr(e.Right)
	if err != nil {
		return 0, err
	}

	// Operators that require custom register-level dispatch (e.g. LIKE, GLOB).
	if handler, ok := binarySpecialHandlers[e.Op]; ok {
		return handler(g, leftReg, rightReg)
	}

	// Standard operators resolved via lookup table.
	entry, ok := binaryOpTable[e.Op]
	if !ok {
		return 0, fmt.Errorf("unsupported binary operator: %v", e.Op)
	}

	resultReg := g.AllocReg()
	g.vdbe.AddOp(entry.op, leftReg, rightReg, resultReg)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, entry.comment)
	return resultReg, nil
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
		// Negate: load zero, then subtract
		zeroReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpInteger, 0, zeroReg, 0)
		g.vdbe.AddOp(vdbe.OpSubtract, operandReg, zeroReg, resultReg)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NEG")

	case parser.OpBitNot:
		g.vdbe.AddOp(vdbe.OpBitNot, operandReg, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "BITNOT")

	case parser.OpIsNull:
		g.vdbe.AddOp(vdbe.OpIsNull, operandReg, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "IS NULL")

	case parser.OpNotNull:
		g.vdbe.AddOp(vdbe.OpNotNull, operandReg, resultReg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "NOT NULL")

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

	// Evaluate arguments into consecutive registers
	var argRegs []int
	for _, arg := range e.Args {
		reg, err := g.GenerateExpr(arg)
		if err != nil {
			return 0, err
		}
		argRegs = append(argRegs, reg)
	}

	// Determine first arg register and count
	firstArg := 0
	argCount := len(argRegs)
	if argCount > 0 {
		firstArg = argRegs[0]
	}

	// Emit function call
	// P1 = first arg register, P2 = arg count, P3 = result register
	addr := g.vdbe.AddOp(vdbe.OpFunction, firstArg, argCount, resultReg)
	g.vdbe.Program[addr].P4.Z = e.Name
	g.vdbe.Program[addr].P4Type = vdbe.P4Static
	g.vdbe.SetComment(addr, fmt.Sprintf("%s(%d args)", e.Name, argCount))

	return resultReg, nil
}

// generateCase generates code for CASE expressions.
func (g *CodeGenerator) generateCase(e *parser.CaseExpr) (int, error) {
	resultReg := g.AllocReg()

	// Track jump addresses to patch
	var endJumps []int

	// Generate code for each WHEN clause
	for _, when := range e.WhenClauses {
		// Evaluate WHEN condition
		condReg, err := g.GenerateExpr(when.Condition)
		if err != nil {
			return 0, err
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
	resultReg := g.AllocReg()

	// Evaluate the LHS expression
	exprReg, err := g.GenerateExpr(e.Expr)
	if err != nil {
		return 0, err
	}

	if e.Select != nil {
		// IN (subquery) - not implemented yet
		return 0, fmt.Errorf("IN with subquery not yet implemented")
	}

	// IN (value_list) - generate comparisons
	// result = (expr == val1) OR (expr == val2) OR ...

	// Start with false
	g.vdbe.AddOp(vdbe.OpInteger, 0, resultReg, 0)

	var endJumps []int

	for _, val := range e.Values {
		// Evaluate value
		valReg, err := g.GenerateExpr(val)
		if err != nil {
			return 0, err
		}

		// Compare
		cmpReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpEq, exprReg, valReg, cmpReg)

		// If true, set result to true and jump to end
		g.vdbe.AddOp(vdbe.OpIf, cmpReg, 0, 0)
		ifAddr := g.vdbe.NumOps() - 1

		// Set result to true
		g.vdbe.AddOp(vdbe.OpInteger, 1, resultReg, 0)

		// Jump to end
		g.vdbe.AddOp(vdbe.OpGoto, 0, 0, 0)
		endJumps = append(endJumps, g.vdbe.NumOps()-1)

		// Patch the If jump to next iteration
		g.vdbe.Program[ifAddr].P2 = g.vdbe.NumOps()
	}

	// Patch end jumps
	endAddr := g.vdbe.NumOps()
	for _, jumpAddr := range endJumps {
		g.vdbe.Program[jumpAddr].P2 = endAddr
	}

	// Handle NOT IN
	if e.Not {
		notReg := g.AllocReg()
		g.vdbe.AddOp(vdbe.OpNot, resultReg, notReg, 0)
		return notReg, nil
	}

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

// generateSubquery generates code for subquery expressions.
func (g *CodeGenerator) generateSubquery(e *parser.SubqueryExpr) (int, error) {
	// Subquery code generation requires full SELECT implementation
	return 0, fmt.Errorf("subquery expressions not yet implemented")
}

// generateLikeExpr generates code for LIKE expressions.
func (g *CodeGenerator) generateLikeExpr(patternReg, valueReg int) (int, error) {
	resultReg := g.AllocReg()
	// For now, use function call approach
	// In real implementation, would use optimized LIKE opcode
	g.vdbe.AddOp(vdbe.OpFunction, patternReg, 2, resultReg)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "LIKE")
	return resultReg, nil
}

// generateGlobExpr generates code for GLOB expressions.
func (g *CodeGenerator) generateGlobExpr(patternReg, valueReg int) (int, error) {
	resultReg := g.AllocReg()
	g.vdbe.AddOp(vdbe.OpFunction, patternReg, 2, resultReg)
	g.vdbe.SetComment(g.vdbe.NumOps()-1, "GLOB")
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

// generateVariable generates code for parameter placeholders (?, ?1, :name, etc.).
func (g *CodeGenerator) generateVariable(e *parser.VariableExpr) (int, error) {
	reg := g.AllocReg()

	// Check if we have args and haven't exceeded them
	if g.args == nil || g.paramIdx >= len(g.args) {
		// No args or out of bounds - emit NULL
		g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param %s (no value)", e.Name))
		return reg, nil
	}

	// Get the next parameter value
	arg := g.args[g.paramIdx]
	g.paramIdx++

	// Emit the appropriate opcode based on the arg type
	switch v := arg.(type) {
	case nil:
		g.vdbe.AddOp(vdbe.OpNull, 0, reg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "param NULL")

	case int:
		g.vdbe.AddOp(vdbe.OpInteger, v, reg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param INT %d", v))

	case int64:
		if v >= -2147483648 && v <= 2147483647 {
			g.vdbe.AddOp(vdbe.OpInteger, int(v), reg, 0)
		} else {
			g.vdbe.AddOpWithP4Int(vdbe.OpInt64, 0, reg, 0, int32(v))
		}
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param INT64 %d", v))

	case float64:
		addr := g.vdbe.AddOp(vdbe.OpReal, 0, reg, 0)
		g.vdbe.Program[addr].P4.R = v
		g.vdbe.Program[addr].P4Type = vdbe.P4Real
		g.vdbe.SetComment(addr, fmt.Sprintf("param REAL %v", v))

	case string:
		g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param STRING '%s'", v))

	case []byte:
		g.vdbe.AddOpWithP4Str(vdbe.OpBlob, len(v), reg, 0, string(v))
		g.vdbe.SetComment(g.vdbe.NumOps()-1, "param BLOB")

	case bool:
		val := 0
		if v {
			val = 1
		}
		g.vdbe.AddOp(vdbe.OpInteger, val, reg, 0)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param BOOL %v", v))

	default:
		// Try to convert to string as fallback
		str := fmt.Sprintf("%v", v)
		g.vdbe.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, str)
		g.vdbe.SetComment(g.vdbe.NumOps()-1, fmt.Sprintf("param %T as STRING", v))
	}

	return reg, nil
}
