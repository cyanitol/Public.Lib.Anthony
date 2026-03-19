// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"errors"
	"fmt"
)

// UpdateStmt represents a compiled UPDATE statement
type UpdateStmt struct {
	Table        string
	Columns      []string // Columns to update
	Values       []Value  // New values for columns
	Where        *WhereClause
	OrderBy      []OrderByColumn
	Limit        *int
	IsOrReplace  bool
	IsOrIgnore   bool
	IsOrAbort    bool
	IsOrFail     bool
	IsOrRollback bool
}

// WhereClause represents a WHERE clause
type WhereClause struct {
	Expr *Expression
}

// Expression represents a SQL expression
type Expression struct {
	Type     ExprType
	Column   string
	Operator string
	Value    Value
	Left     *Expression
	Right    *Expression
}

// ExprType represents the type of expression
type ExprType int

const (
	ExprColumn ExprType = iota
	ExprLiteral
	ExprBinary
	ExprUnary
	ExprFunction
)

// OrderByColumn represents an ORDER BY column
type OrderByColumn struct {
	Column     string
	Descending bool
}

// CompileUpdate compiles an UPDATE statement into VDBE bytecode
//
// Generated code structure (simplified one-pass update):
//
//	OP_Init         0, end
//	OP_OpenWrite    0, table_root
//	OP_Rewind       0, end
//
// loop:
//
//	OP_Rowid        0, reg_rowid
//	[WHERE evaluation if present]
//	[OP_IfNot       reg_where, next]
//	OP_Column       0, col_idx, reg_old_val
//	[Compute new values]
//	OP_MakeRecord   reg_cols, num_cols, reg_record
//	OP_Insert       0, reg_record, reg_rowid
//	OP_Delete       0, reg_old_rowid
//
// next:
//
//	OP_Next         0, loop
//
// end:
//
//	OP_Close        0
//	OP_Halt
func CompileUpdate(stmt *UpdateStmt, tableRoot int, numColumns int) (*Program, error) {
	if err := validateUpdateStmt(stmt); err != nil {
		return nil, err
	}
	ctx := newUpdateCompiler(stmt, tableRoot, numColumns)
	return ctx.compile()
}

// validateUpdateStmt checks the update statement for basic validity.
func validateUpdateStmt(stmt *UpdateStmt) error {
	if stmt == nil {
		return errors.New("nil update statement")
	}
	if len(stmt.Columns) == 0 {
		return errors.New("no columns to update")
	}
	if len(stmt.Columns) != len(stmt.Values) {
		return fmt.Errorf("column count (%d) does not match value count (%d)", len(stmt.Columns), len(stmt.Values))
	}
	return nil
}

// updateCompiler holds state for compiling an UPDATE statement.
type updateCompiler struct {
	stmt        *UpdateStmt
	tableRoot   int
	numColumns  int
	prog        *Program
	cursorNum   int
	regRowid    int
	regNewRowid int
	regOldCols  int
	regNewCols  int
	regRecord   int
	regWhere    int
}

func newUpdateCompiler(stmt *UpdateStmt, tableRoot, numColumns int) *updateCompiler {
	prog := &Program{Instructions: make([]Instruction, 0), NumRegisters: 0, NumCursors: 1}
	ctx := &updateCompiler{stmt: stmt, tableRoot: tableRoot, numColumns: numColumns, prog: prog}
	ctx.regRowid = prog.allocReg()
	_ = prog.allocReg() // reserved
	ctx.regNewRowid = prog.allocReg()
	ctx.regOldCols = prog.allocRegs(numColumns)
	ctx.regNewCols = prog.allocRegs(numColumns)
	ctx.regRecord = prog.allocReg()
	ctx.regWhere = prog.allocReg()
	return ctx
}

func (c *updateCompiler) compile() (*Program, error) {
	c.prog.add(OpInit, 0, 0, 0, nil, 0, "Initialize program")
	c.prog.add(OpOpenWrite, c.cursorNum, c.tableRoot, 0, nil, 0, fmt.Sprintf("Open table %s for update", c.stmt.Table))
	c.prog.add(OpRewind, c.cursorNum, 0, 0, nil, 0, "Rewind to start")
	rewindInst := len(c.prog.Instructions) - 1
	loopLabel := len(c.prog.Instructions)
	c.prog.add(OpRowid, c.cursorNum, c.regRowid, 0, nil, 0, "Get current rowid")
	whereJumpInst, err := c.compileWhere()
	if err != nil {
		return nil, err
	}
	c.loadOldColumns()
	if err := c.buildNewRow(); err != nil {
		return nil, err
	}
	c.prog.add(OpDelete, c.cursorNum, 0, 0, nil, 0, "Delete old record")
	c.prog.add(OpInsert, c.cursorNum, c.regRecord, c.regNewRowid, nil, 0, "Insert updated record")
	nextLabel := len(c.prog.Instructions)
	if whereJumpInst >= 0 {
		c.prog.Instructions[whereJumpInst].P2 = nextLabel
	}
	c.prog.add(OpNext, c.cursorNum, loopLabel, 0, nil, 0, "Next row")
	endLabel := len(c.prog.Instructions)
	c.prog.Instructions[0].P2 = endLabel
	c.prog.Instructions[rewindInst].P2 = endLabel
	c.prog.add(OpClose, c.cursorNum, 0, 0, nil, 0, fmt.Sprintf("Close table %s", c.stmt.Table))
	c.prog.add(OpHalt, 0, 0, 0, nil, 0, "End program")
	return c.prog, nil
}

func (c *updateCompiler) compileWhere() (int, error) {
	if c.stmt.Where == nil {
		return -1, nil
	}
	if err := c.prog.compileExpression(c.stmt.Where.Expr, c.cursorNum, c.regOldCols, c.regWhere); err != nil {
		return -1, fmt.Errorf("WHERE clause: %v", err)
	}
	c.prog.add(OpIfNot, c.regWhere, 0, 0, nil, 0, "Skip if WHERE is false")
	return len(c.prog.Instructions) - 1, nil
}

func (c *updateCompiler) loadOldColumns() {
	for i := 0; i < c.numColumns; i++ {
		c.prog.add(OpColumn, c.cursorNum, i, c.regOldCols+i, nil, 0, fmt.Sprintf("Load old column %d", i))
	}
}

func (c *updateCompiler) buildNewRow() error {
	c.prog.add(OpCopy, c.regOldCols, c.regNewCols, c.numColumns, nil, 0, "Copy old values to new")
	for i, colName := range c.stmt.Columns {
		reg := c.regNewCols + i
		if err := c.prog.addValueLoad(c.stmt.Values[i], reg); err != nil {
			return fmt.Errorf("column %s: %v", colName, err)
		}
	}
	c.prog.add(OpCopy, c.regRowid, c.regNewRowid, 0, nil, 0, "Copy rowid")
	c.prog.add(OpMakeRecord, c.regNewCols, c.numColumns, c.regRecord, nil, 0, "Make updated record")
	return nil
}

// binaryOpTable maps SQL operator strings to their corresponding opcodes.
var binaryOpTable = map[string]OpCode{
	"=":  OpEq,
	"!=": OpNe,
	"<":  OpLt,
	"<=": OpLe,
	">":  OpGt,
	">=": OpGe,
	"+":  OpAdd,
	"-":  OpSubtract,
	"*":  OpMultiply,
	"/":  OpDivide,
}

// compileExpression compiles an expression to VDBE bytecode
func (p *Program) compileExpression(expr *Expression, cursorNum int, regBase int, regDest int) error {
	if expr == nil {
		return errors.New("nil expression")
	}

	switch expr.Type {
	case ExprLiteral:
		return p.compileExprLiteral(expr, regDest)
	case ExprColumn:
		return p.compileExprColumn(expr, cursorNum, regDest)
	case ExprBinary:
		return p.compileExprBinary(expr, cursorNum, regBase, regDest)
	default:
		return fmt.Errorf("unsupported expression type: %v", expr.Type)
	}
}

// compileExprLiteral emits bytecode to load a literal value into regDest.
func (p *Program) compileExprLiteral(expr *Expression, regDest int) error {
	return p.addValueLoad(expr.Value, regDest)
}

// compileExprColumn emits bytecode to load a column value into regDest.
func (p *Program) compileExprColumn(expr *Expression, cursorNum int, regDest int) error {
	// In real implementation, would look up column index from table metadata
	colIdx := 0 // Placeholder
	p.add(OpColumn, cursorNum, colIdx, regDest, nil, 0,
		fmt.Sprintf("Load column %s", expr.Column))
	return nil
}

// compileExprBinary emits bytecode for a binary expression into regDest.
func (p *Program) compileExprBinary(expr *Expression, cursorNum int, regBase int, regDest int) error {
	regLeft := p.allocReg()
	regRight := p.allocReg()

	if err := p.compileExpression(expr.Left, cursorNum, regBase, regLeft); err != nil {
		return err
	}
	if err := p.compileExpression(expr.Right, cursorNum, regBase, regRight); err != nil {
		return err
	}

	opCode, ok := binaryOpTable[expr.Operator]
	if !ok {
		return fmt.Errorf("unsupported operator: %s", expr.Operator)
	}
	p.add(opCode, regLeft, regRight, regDest, nil, 0, expr.Operator)
	return nil
}

// CompileUpdateWithIndex compiles an UPDATE that affects indexes
func CompileUpdateWithIndex(stmt *UpdateStmt, tableRoot int, numColumns int, indexes []int) (*Program, error) {
	// Start with basic update
	prog, err := CompileUpdate(stmt, tableRoot, numColumns)
	if err != nil {
		return nil, err
	}

	// In a full implementation, we would:
	// 1. Delete old index entries
	// 2. Insert new index entries
	// This requires additional cursors and complexity

	// For now, return the basic program
	return prog, nil
}

// ValidateUpdate performs validation on an UPDATE statement
func ValidateUpdate(stmt *UpdateStmt) error {
	if stmt == nil {
		return errors.New("nil update statement")
	}

	if stmt.Table == "" {
		return errors.New("table name is required")
	}

	if len(stmt.Columns) == 0 {
		return errors.New("no columns to update")
	}

	if len(stmt.Columns) != len(stmt.Values) {
		return fmt.Errorf("column count (%d) does not match value count (%d)",
			len(stmt.Columns), len(stmt.Values))
	}

	return nil
}

// NewUpdateStmt creates a new UPDATE statement
func NewUpdateStmt(table string, columns []string, values []Value, where *WhereClause) *UpdateStmt {
	return &UpdateStmt{
		Table:   table,
		Columns: columns,
		Values:  values,
		Where:   where,
	}
}

// NewWhereClause creates a new WHERE clause
func NewWhereClause(expr *Expression) *WhereClause {
	return &WhereClause{Expr: expr}
}

// NewBinaryExpression creates a new binary expression
func NewBinaryExpression(left *Expression, operator string, right *Expression) *Expression {
	return &Expression{
		Type:     ExprBinary,
		Operator: operator,
		Left:     left,
		Right:    right,
	}
}

// NewColumnExpression creates a column reference expression
func NewColumnExpression(column string) *Expression {
	return &Expression{
		Type:   ExprColumn,
		Column: column,
	}
}

// NewLiteralExpression creates a literal value expression
func NewLiteralExpression(value Value) *Expression {
	return &Expression{
		Type:  ExprLiteral,
		Value: value,
	}
}
