// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// NATURAL / USING resolution
// ---------------------------------------------------------------------------

// resolveJoinConditions converts NATURAL and USING joins into ON conditions
// by synthesizing equality expressions from common or named columns.
func (s *Stmt) resolveJoinConditions(stmt *parser.SelectStmt, tables []stmtTableInfo) {
	if stmt.From == nil {
		return
	}
	crossCount := len(stmt.From.Tables)
	for i := range stmt.From.Joins {
		join := &stmt.From.Joins[i]
		tblIdx := crossCount + i
		if tblIdx >= len(tables) {
			continue
		}
		if join.Natural {
			resolveNaturalJoin(join, tables, tblIdx)
		} else if len(join.Condition.Using) > 0 {
			resolveUsingJoin(join, tables, tblIdx)
		}
	}
}

// resolveNaturalJoin synthesizes an ON condition from columns common to
// the left side and the right table.
func resolveNaturalJoin(join *parser.JoinClause, tables []stmtTableInfo, rightIdx int) {
	rightTable := tables[rightIdx].table
	var commonCols []string
	for _, rc := range rightTable.Columns {
		if findColumnInTables(rc.Name, tables[:rightIdx]) {
			commonCols = append(commonCols, rc.Name)
		}
	}
	if len(commonCols) == 0 {
		return // no common columns → cross product (SQLite behaviour)
	}
	join.Condition.On = buildEqualityChain(commonCols, tables, rightIdx)
}

// resolveUsingJoin synthesises an ON condition from the USING column list.
func resolveUsingJoin(join *parser.JoinClause, tables []stmtTableInfo, rightIdx int) {
	if len(join.Condition.Using) == 0 {
		return
	}
	join.Condition.On = buildEqualityChain(join.Condition.Using, tables, rightIdx)
}

// findColumnInTables returns true if colName exists in any of the given tables.
func findColumnInTables(colName string, tables []stmtTableInfo) bool {
	lower := strings.ToLower(colName)
	for _, t := range tables {
		for _, c := range t.table.Columns {
			if strings.ToLower(c.Name) == lower {
				return true
			}
		}
	}
	return false
}

// buildEqualityChain builds an AND-chain of left.col = right.col.
func buildEqualityChain(cols []string, tables []stmtTableInfo, rightIdx int) parser.Expression {
	rightAlias := tables[rightIdx].name
	var combined parser.Expression
	for _, col := range cols {
		leftAlias := findOwnerAlias(col, tables[:rightIdx])
		eq := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Table: leftAlias, Name: col},
			Right: &parser.IdentExpr{Table: rightAlias, Name: col},
		}
		if combined == nil {
			combined = eq
		} else {
			combined = &parser.BinaryExpr{Op: parser.OpAnd, Left: combined, Right: eq}
		}
	}
	return combined
}

// findOwnerAlias returns the alias of the first table containing colName.
func findOwnerAlias(colName string, tables []stmtTableInfo) string {
	lower := strings.ToLower(colName)
	for _, t := range tables {
		for _, c := range t.table.Columns {
			if strings.ToLower(c.Name) == lower {
				return t.name
			}
		}
	}
	return ""
}

// hasOuterJoin returns true if any explicit join is a LEFT, RIGHT, or FULL join.
func hasOuterJoin(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, j := range stmt.From.Joins {
		switch j.Type {
		case parser.JoinLeft, parser.JoinRight, parser.JoinFull:
			return true
		}
	}
	return false
}

// hasRightJoin returns true if any explicit join is a RIGHT join.
func hasRightJoin(stmt *parser.SelectStmt) bool {
	if stmt.From == nil {
		return false
	}
	for _, j := range stmt.From.Joins {
		if j.Type == parser.JoinRight {
			return true
		}
	}
	return false
}

// rewriteRightJoinsWithTables converts RIGHT JOINs into LEFT JOINs by swapping
// the table operands in both the AST and the tables slice. Returns the updated
// tables slice.  Must be called after star expansion so column ordering is preserved.
func rewriteRightJoinsWithTables(stmt *parser.SelectStmt, tables []stmtTableInfo) []stmtTableInfo {
	if stmt.From == nil || len(stmt.From.Joins) == 0 {
		return tables
	}
	crossCount := len(stmt.From.Tables)
	for i := range stmt.From.Joins {
		if stmt.From.Joins[i].Type != parser.JoinRight {
			continue
		}
		// Swap: FROM A RIGHT JOIN B -> FROM B LEFT JOIN A
		// Handle the common single-cross-table case (index 0 of Tables).
		if i == 0 && crossCount == 1 {
			stmt.From.Tables[0], stmt.From.Joins[0].Table = stmt.From.Joins[0].Table, stmt.From.Tables[0]
			stmt.From.Joins[0].Type = parser.JoinLeft
			// Swap corresponding entries in the tables slice
			tables[0], tables[crossCount] = tables[crossCount], tables[0]
		}
	}
	return tables
}

// expandStarForJoinTables expands SELECT * into explicit column references
// using the provided table metadata, preserving original table order.
func expandStarForJoinTables(stmt *parser.SelectStmt, tables []stmtTableInfo) {
	var expanded []parser.ResultColumn
	needExpansion := false
	for _, col := range stmt.Columns {
		if col.Star {
			needExpansion = true
			break
		}
	}
	if !needExpansion {
		return
	}
	for _, col := range stmt.Columns {
		expanded = expandOneResultColumn(col, tables, expanded)
	}
	stmt.Columns = expanded
}

// expandOneResultColumn appends the expansion of a single ResultColumn to dst.
// Non-star columns are passed through; star columns are expanded using tables.
func expandOneResultColumn(col parser.ResultColumn, tables []stmtTableInfo, dst []parser.ResultColumn) []parser.ResultColumn {
	if !col.Star {
		return append(dst, col)
	}
	if col.Table != "" {
		// table.* — expand only that table's columns
		for _, tbl := range tables {
			if tbl.name == col.Table || tbl.table.Name == col.Table {
				return appendTableColumns(tbl, dst)
			}
		}
		return dst
	}
	// bare * — expand all tables in order
	for _, tbl := range tables {
		dst = appendTableColumns(tbl, dst)
	}
	return dst
}

// appendTableColumns appends ResultColumns for every column in tbl to dst.
func appendTableColumns(tbl stmtTableInfo, dst []parser.ResultColumn) []parser.ResultColumn {
	for _, sc := range tbl.table.Columns {
		dst = append(dst, parser.ResultColumn{
			Expr: &parser.IdentExpr{Table: tbl.name, Name: sc.Name},
		})
	}
	return dst
}

// ---------------------------------------------------------------------------
// LEFT JOIN compilation (non-ORDER-BY path)
// ---------------------------------------------------------------------------

// leftJoinCtx holds state for LEFT JOIN code generation.
type leftJoinCtx struct {
	vm       *vdbe.VDBE
	stmt     *parser.SelectStmt
	tables   []stmtTableInfo
	numCols  int
	gen      *expr.CodeGenerator
	flagRegs []int // one per explicit join
}

// compileJoinsWithLeftSupport compiles the non-ORDER BY path with LEFT JOIN support.
func (s *Stmt) compileJoinsWithLeftSupport(vm *vdbe.VDBE, stmt *parser.SelectStmt, tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) (*vdbe.VDBE, error) {
	gen.SetNextReg(numCols)

	joinCount := len(stmt.From.Joins)
	flagRegs := make([]int, joinCount)
	for i := range flagRegs {
		flagRegs[i] = gen.AllocReg()
	}

	ctx := &leftJoinCtx{
		vm: vm, stmt: stmt, tables: tables,
		numCols: numCols, gen: gen, flagRegs: flagRegs,
	}

	// Init + open cursors
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	openTableCursors(vm, tables)

	outerCursor := tables[0].cursorIdx
	rewindAddr := vm.AddOp(vdbe.OpRewind, outerCursor, 0, 0)
	loopStart := vm.NumOps()

	// Emit the recursive join body starting at join index 0
	s.emitJoinLevel(ctx, 0)

	// Outer Next loops back; close ALL cursors after loop exits
	vm.AddOp(vdbe.OpNext, outerCursor, loopStart, 0)
	closeTableCursors(vm, tables)
	haltAddr := vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	vm.Program[rewindAddr].P2 = haltAddr

	return vm, nil
}

// emitJoinLevel emits the loop body for join index joinIdx.
// When joinIdx == joinCount, we are at the leaf: emit WHERE + columns + ResultRow.
func (s *Stmt) emitJoinLevel(ctx *leftJoinCtx, joinIdx int) {
	joinCount := len(ctx.stmt.From.Joins)
	if joinIdx >= joinCount {
		s.emitLeafRow(ctx)
		return
	}

	join := ctx.stmt.From.Joins[joinIdx]
	crossCount := len(ctx.stmt.From.Tables)
	cursorIdx := ctx.tables[crossCount+joinIdx].cursorIdx

	// matchFlag = 0
	ctx.vm.AddOp(vdbe.OpInteger, 0, ctx.flagRegs[joinIdx], 0)

	rewindAddr := ctx.vm.AddOp(vdbe.OpRewind, cursorIdx, 0, 0)
	innerStart := ctx.vm.NumOps()

	// Evaluate ON condition
	var onSkipAddr int
	if join.Condition.On != nil {
		onReg, err := ctx.gen.GenerateExpr(join.Condition.On)
		if err == nil {
			onSkipAddr = ctx.vm.AddOp(vdbe.OpIfNot, onReg, 0, 0)
		}
	}

	// ON matched → set matchFlag = 1
	if join.Type == parser.JoinLeft {
		ctx.vm.AddOp(vdbe.OpInteger, 1, ctx.flagRegs[joinIdx], 0)
	}

	// Recurse to next join level
	s.emitJoinLevel(ctx, joinIdx+1)

	// Fix ON skip to jump to Next
	nextAddr := ctx.vm.AddOp(vdbe.OpNext, cursorIdx, innerStart, 0)
	if join.Condition.On != nil && onSkipAddr != 0 {
		ctx.vm.Program[onSkipAddr].P2 = nextAddr
	}

	// After inner loop: handle LEFT JOIN null emission
	afterLoop := ctx.vm.NumOps()
	if join.Type == parser.JoinLeft {
		s.emitNullEmission(ctx, joinIdx)
		ctx.vm.Program[rewindAddr].P2 = afterLoop
	} else {
		ctx.vm.Program[rewindAddr].P2 = ctx.vm.NumOps()
	}
}

// emitLeafRow emits WHERE filter, column reads, and ResultRow at the leaf.
func (s *Stmt) emitLeafRow(ctx *leftJoinCtx) {
	var whereSkip int
	if ctx.stmt.Where != nil {
		whereReg, err := ctx.gen.GenerateExpr(ctx.stmt.Where)
		if err == nil {
			whereSkip = ctx.vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
		}
	}

	for i, col := range ctx.stmt.Columns {
		_ = s.emitSelectColumnOpMultiTable(ctx.vm, ctx.tables, col, i, ctx.gen)
	}
	ctx.vm.AddOp(vdbe.OpResultRow, 0, ctx.numCols, 0)

	if ctx.stmt.Where != nil && whereSkip != 0 {
		ctx.vm.Program[whereSkip].P2 = ctx.vm.NumOps()
	}
}

// emitNullEmission emits the null-row for a LEFT JOIN when matchFlag == 0.
func (s *Stmt) emitNullEmission(ctx *leftJoinCtx, joinIdx int) {
	// Skip if matched
	skipAddr := ctx.vm.AddOp(vdbe.OpIf, ctx.flagRegs[joinIdx], 0, 0)

	crossCount := len(ctx.stmt.From.Tables)
	// Build set of table indices that should produce NULLs
	nullTables := make(map[int]bool)
	for j := joinIdx; j < len(ctx.stmt.From.Joins); j++ {
		nullTables[crossCount+j] = true
	}

	// Read columns, NULLing out those from unmatched tables
	for i, col := range ctx.stmt.Columns {
		tblIdx := s.findColumnTableIndex(col, ctx.tables)
		if nullTables[tblIdx] {
			ctx.vm.AddOp(vdbe.OpNull, 0, i, 0)
		} else {
			_ = s.emitSelectColumnOpMultiTable(ctx.vm, ctx.tables, col, i, ctx.gen)
		}
	}

	// Apply WHERE filter
	if ctx.stmt.Where != nil {
		whereReg, err := ctx.gen.GenerateExpr(ctx.stmt.Where)
		if err == nil {
			nullWhereSkip := ctx.vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
			ctx.vm.AddOp(vdbe.OpResultRow, 0, ctx.numCols, 0)
			ctx.vm.Program[nullWhereSkip].P2 = ctx.vm.NumOps()
		}
	} else {
		ctx.vm.AddOp(vdbe.OpResultRow, 0, ctx.numCols, 0)
	}

	ctx.vm.Program[skipAddr].P2 = ctx.vm.NumOps()
}

// findColumnTableIndex returns the table index for a column expression.
func (s *Stmt) findColumnTableIndex(col parser.ResultColumn, tables []stmtTableInfo) int {
	ident, ok := col.Expr.(*parser.IdentExpr)
	if !ok {
		return 0
	}
	if ident.Table != "" {
		for i, tbl := range tables {
			if tbl.name == ident.Table || tbl.table.Name == ident.Table {
				return i
			}
		}
	}
	for i, tbl := range tables {
		if tbl.table.GetColumnIndex(ident.Name) >= 0 {
			return i
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// LEFT JOIN compilation (ORDER-BY path)
// ---------------------------------------------------------------------------

// leftSorterCtx extends leftJoinCtx with sorter info for the ORDER BY path.
type leftSorterCtx struct {
	leftJoinCtx
	orderInfo *orderByColumnInfo
}

// emitJoinLevelSorter emits the loop body for join index joinIdx with sorter.
func (s *Stmt) emitJoinLevelSorter(ctx *leftSorterCtx, joinIdx int) {
	joinCount := len(ctx.stmt.From.Joins)
	if joinIdx >= joinCount {
		s.emitLeafRowSorter(ctx)
		return
	}

	join := ctx.stmt.From.Joins[joinIdx]
	crossCount := len(ctx.stmt.From.Tables)
	cursorIdx := ctx.tables[crossCount+joinIdx].cursorIdx

	ctx.vm.AddOp(vdbe.OpInteger, 0, ctx.flagRegs[joinIdx], 0)
	rewindAddr := ctx.vm.AddOp(vdbe.OpRewind, cursorIdx, 0, 0)
	innerStart := ctx.vm.NumOps()

	var onSkipAddr int
	if join.Condition.On != nil {
		onReg, err := ctx.gen.GenerateExpr(join.Condition.On)
		if err == nil {
			onSkipAddr = ctx.vm.AddOp(vdbe.OpIfNot, onReg, 0, 0)
		}
	}

	if join.Type == parser.JoinLeft {
		ctx.vm.AddOp(vdbe.OpInteger, 1, ctx.flagRegs[joinIdx], 0)
	}

	s.emitJoinLevelSorter(ctx, joinIdx+1)

	nextAddr := ctx.vm.AddOp(vdbe.OpNext, cursorIdx, innerStart, 0)
	if join.Condition.On != nil && onSkipAddr != 0 {
		ctx.vm.Program[onSkipAddr].P2 = nextAddr
	}

	afterLoop := ctx.vm.NumOps()
	if join.Type == parser.JoinLeft {
		s.emitNullEmissionSorter(ctx, joinIdx)
		ctx.vm.Program[rewindAddr].P2 = afterLoop
	} else {
		ctx.vm.Program[rewindAddr].P2 = ctx.vm.NumOps()
	}
}

// emitLeafRowSorter emits WHERE + columns + SorterInsert at the leaf.
func (s *Stmt) emitLeafRowSorter(ctx *leftSorterCtx) {
	var whereSkip int
	if ctx.stmt.Where != nil {
		whereReg, err := ctx.gen.GenerateExpr(ctx.stmt.Where)
		if err == nil {
			whereSkip = ctx.vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
		}
	}

	for i, col := range ctx.stmt.Columns {
		_ = s.emitSelectColumnOpMultiTable(ctx.vm, ctx.tables, col, i, ctx.gen)
	}
	s.emitSorterExtrasAndInsert(ctx)

	if ctx.stmt.Where != nil && whereSkip != 0 {
		ctx.vm.Program[whereSkip].P2 = ctx.vm.NumOps()
	}
}

// emitNullEmissionSorter emits null-row into sorter for LEFT JOIN.
func (s *Stmt) emitNullEmissionSorter(ctx *leftSorterCtx, joinIdx int) {
	skipAddr := ctx.vm.AddOp(vdbe.OpIf, ctx.flagRegs[joinIdx], 0, 0)

	crossCount := len(ctx.stmt.From.Tables)
	nullTables := make(map[int]bool)
	for j := joinIdx; j < len(ctx.stmt.From.Joins); j++ {
		nullTables[crossCount+j] = true
	}

	for i, col := range ctx.stmt.Columns {
		tblIdx := s.findColumnTableIndex(col, ctx.tables)
		if nullTables[tblIdx] {
			ctx.vm.AddOp(vdbe.OpNull, 0, i, 0)
		} else {
			_ = s.emitSelectColumnOpMultiTable(ctx.vm, ctx.tables, col, i, ctx.gen)
		}
	}

	if ctx.stmt.Where != nil {
		whereReg, err := ctx.gen.GenerateExpr(ctx.stmt.Where)
		if err == nil {
			nullWhereSkip := ctx.vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
			s.emitSorterExtrasAndInsert(ctx)
			ctx.vm.Program[nullWhereSkip].P2 = ctx.vm.NumOps()
		}
	} else {
		s.emitSorterExtrasAndInsert(ctx)
	}

	ctx.vm.Program[skipAddr].P2 = ctx.vm.NumOps()
}

// emitSorterExtrasAndInsert reads extra ORDER BY columns and inserts into the sorter.
func (s *Stmt) emitSorterExtrasAndInsert(ctx *leftSorterCtx) {
	for i, orderExpr := range ctx.orderInfo.extraExprs {
		reg, err := ctx.gen.GenerateExpr(orderExpr)
		if err != nil {
			reg = ctx.gen.AllocReg()
			ctx.vm.AddOp(vdbe.OpNull, 0, reg, 0)
		}
		ctx.orderInfo.extraColRegs[i] = reg
	}
	for i := range ctx.orderInfo.extraExprs {
		ctx.vm.AddOp(vdbe.OpCopy, ctx.orderInfo.extraColRegs[i], ctx.numCols+i, 0)
	}
	ctx.vm.AddOp(vdbe.OpSorterInsert, 0, 0, ctx.orderInfo.sorterCols)
}
