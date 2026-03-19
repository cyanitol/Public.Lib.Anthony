// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileSelectWithJoinsAndAggregates compiles a SELECT that has both JOINs and
// aggregates (GROUP BY + COUNT/SUM/etc.). It materialises the join result into a
// sorter keyed by GROUP BY columns, then iterates the sorted rows using the
// standard GROUP BY aggregation machinery.
func (s *Stmt) compileSelectWithJoinsAndAggregates(vm *vdbe.VDBE, stmt *parser.SelectStmt,
	tables []stmtTableInfo, numCols int, gen *expr.CodeGenerator) (*vdbe.VDBE, error) {

	numGroupBy := len(stmt.GroupBy)
	sorterCols := s.calculateSorterColumns(stmt, numGroupBy)
	collations := s.groupByCollationsMultiTable(stmt.GroupBy, tables)

	// Allocate extra memory for GROUP BY + aggregation registers
	vm.AllocMemory(numCols + numGroupBy*3 + 100)

	// Reserve result column registers in the code generator
	for i := 0; i < numCols; i++ {
		gen.AllocReg()
	}

	// Phase 1: join loop -> sorter
	sorterCursor, state, rewindAddr, sorterSortAddr, sorterBaseReg, err :=
		s.joinAggPhase1(vm, gen, stmt, tables, numGroupBy, sorterCols, collations)
	if err != nil {
		return nil, err
	}

	// Phase 2: iterate sorted rows, aggregate, emit groups
	sorterNextAddr, sorterLoopStart := s.processSortedDataWithGrouping(
		vm, gen, stmt, tables[0].table, state,
		sorterCursor, sorterBaseReg, numGroupBy, numCols, sorterCols, collations)

	finalOutputAddr := vm.NumOps()
	s.emitFinalGroupOutput(vm, gen, stmt, state, numCols)

	// When there is no GROUP BY but there ARE aggregates (e.g. SELECT COUNT(*)
	// FROM t1 JOIN t2 ...), an empty sorter must still produce one row with
	// default aggregate values (COUNT→0, SUM→NULL, etc.).  emitFinalGroupOutput
	// correctly skips output when no rows were processed (needed for GROUP BY),
	// so we emit a separate default-row path that the empty-sorter jump targets.
	emptyAggAddr := -1
	if numGroupBy == 0 {
		// Skip past the default-row block when the normal path runs
		skipDefaultAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
		emptyAggAddr = vm.NumOps()
		s.emitDefaultAggregateRow(vm, stmt, numCols)
		vm.Program[skipDefaultAddr].P2 = vm.NumOps()
	}

	vm.AddOp(vdbe.OpSorterClose, sorterCursor, 0, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Fix forward references.
	// rewindAddr jumps to OpSorterSort (not past it) so that an empty outer
	// table still triggers the empty-sorter logic in OpSorterSort.
	vm.Program[rewindAddr].P2 = sorterSortAddr
	if emptyAggAddr >= 0 {
		vm.Program[sorterSortAddr].P2 = emptyAggAddr
	} else {
		vm.Program[sorterSortAddr].P2 = finalOutputAddr
	}
	vm.Program[sorterNextAddr].P2 = sorterLoopStart

	return vm, nil
}

// emitDefaultAggregateRow emits a single result row with default aggregate
// values (COUNT→0, SUM/MIN/MAX→NULL, TOTAL→0.0, etc.).  This is used when
// an aggregate query without GROUP BY has zero input rows.
func (s *Stmt) emitDefaultAggregateRow(vm *vdbe.VDBE, stmt *parser.SelectStmt, numCols int) {
	for i, col := range stmt.Columns {
		if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && s.isAggregateExpr(col.Expr) {
			s.emitDefaultAggregateValue(vm, fnExpr, i)
		} else {
			vm.AddOp(vdbe.OpNull, 0, i, 0)
		}
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
}

// emitDefaultAggregateValue writes the default empty-input value for one
// aggregate function into the given result register.
func (s *Stmt) emitDefaultAggregateValue(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, reg int) {
	switch fnExpr.Name {
	case "COUNT":
		vm.AddOp(vdbe.OpInteger, 0, reg, 0)
	case "TOTAL":
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, 0.0)
	default: // SUM, MIN, MAX, AVG, GROUP_CONCAT, etc.
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// joinAggPhase1 opens cursors, creates the sorter, runs the nested-loop join,
// and inserts matching rows into the sorter. It returns sorter cursor, GROUP BY
// state, and key addresses that the caller must patch.
func (s *Stmt) joinAggPhase1(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	stmt *parser.SelectStmt, tables []stmtTableInfo,
	numGroupBy, sorterCols int, collations []string,
) (sorterCursor int, state groupByState, rewindAddr, sorterSortAddr, sorterBaseReg int, err error) {

	// Allocate sorter cursor beyond all table cursors
	sorterCursor = s.nextCursorAfterTables(tables, vm)

	// Initialise GROUP BY state (accumulators, prev-group registers, first-row flag)
	vm.AddOp(vdbe.OpInit, 0, 0, 0)
	state = s.initGroupByState(vm, gen, stmt, numGroupBy)

	// Open table cursors and sorter
	keyInfo := s.createGroupBySorterKeyInfo(numGroupBy, collations)
	for _, tbl := range tables {
		if !tbl.table.Temp {
			vm.AddOp(vdbe.OpOpenRead, tbl.cursorIdx, int(tbl.table.RootPage), len(tbl.table.Columns))
		}
	}
	sorterOpenAddr := vm.AddOp(vdbe.OpSorterOpen, sorterCursor, sorterCols, 0)
	vm.Program[sorterOpenAddr].P4.P = keyInfo

	// Allocate contiguous sorter registers
	sorterBaseReg = gen.AllocReg()
	for i := 1; i < sorterCols; i++ {
		gen.AllocReg()
	}

	// Outer loop on first table
	rewindAddr = vm.AddOp(vdbe.OpRewind, tables[0].cursorIdx, 0, 0)
	loopStart := vm.NumOps()

	if hasOuterJoin(stmt) {
		s.emitLeftJoinAggBody(vm, gen, stmt, tables, sorterCursor, sorterBaseReg, numGroupBy, sorterCols, loopStart)
	} else {
		s.emitInnerJoinAggBody(vm, gen, stmt, tables, sorterCursor, sorterBaseReg, numGroupBy, sorterCols, loopStart)
	}

	sorterSortAddr = vm.AddOp(vdbe.OpSorterSort, sorterCursor, 0, 0)
	return
}

// emitInnerJoinAggBody emits the nested loop + sorter population for INNER JOIN aggregates.
func (s *Stmt) emitInnerJoinAggBody(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	stmt *parser.SelectStmt, tables []stmtTableInfo,
	sorterCursor, sorterBaseReg, numGroupBy, sorterCols, loopStart int) {

	innerLoopStarts, innerRewindAddrs := s.emitJoinNestedLoops(vm, tables)
	_ = s.emitJoinAggSorterInsert(vm, gen, stmt, tables, sorterCursor, sorterBaseReg, numGroupBy, sorterCols)

	for i := len(tables) - 1; i > 0; i-- {
		vm.AddOp(vdbe.OpNext, tables[i].cursorIdx, innerLoopStarts[i-1], 0)
	}
	outerNextAddr := vm.AddOp(vdbe.OpNext, tables[0].cursorIdx, loopStart, 0)

	for i := len(tables) - 1; i >= 0; i-- {
		if !tables[i].table.Temp {
			vm.AddOp(vdbe.OpClose, tables[i].cursorIdx, 0, 0)
		}
	}
	for _, addr := range innerRewindAddrs {
		vm.Program[addr].P2 = outerNextAddr
	}
}

// leftAggCtx holds state for LEFT JOIN aggregate sorter code generation.
type leftAggCtx struct {
	vm           *vdbe.VDBE
	gen          *expr.CodeGenerator
	stmt         *parser.SelectStmt
	tables       []stmtTableInfo
	flagRegs     []int
	sorterCursor int
	sorterBase   int
	numGroupBy   int
	sorterCols   int
}

// emitLeftJoinAggBody emits the nested loop + sorter population with LEFT JOIN support for aggregates.
func (s *Stmt) emitLeftJoinAggBody(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	stmt *parser.SelectStmt, tables []stmtTableInfo,
	sorterCursor, sorterBaseReg, numGroupBy, sorterCols, loopStart int) {

	joinCount := len(stmt.From.Joins)
	flagRegs := make([]int, joinCount)
	for i := range flagRegs {
		flagRegs[i] = gen.AllocReg()
	}

	ctx := &leftAggCtx{
		vm: vm, gen: gen, stmt: stmt, tables: tables,
		flagRegs: flagRegs, sorterCursor: sorterCursor,
		sorterBase: sorterBaseReg, numGroupBy: numGroupBy,
		sorterCols: sorterCols,
	}

	s.emitJoinLevelAgg(ctx, 0)

	vm.AddOp(vdbe.OpNext, tables[0].cursorIdx, loopStart, 0)
	for i := len(tables) - 1; i >= 0; i-- {
		if !tables[i].table.Temp {
			vm.AddOp(vdbe.OpClose, tables[i].cursorIdx, 0, 0)
		}
	}
}

// emitJoinLevelAgg emits the loop body for join index joinIdx, inserting into the aggregate sorter.
func (s *Stmt) emitJoinLevelAgg(ctx *leftAggCtx, joinIdx int) {
	joinCount := len(ctx.stmt.From.Joins)
	if joinIdx >= joinCount {
		s.emitAggLeafRow(ctx)
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

	s.emitJoinLevelAgg(ctx, joinIdx+1)

	nextAddr := ctx.vm.AddOp(vdbe.OpNext, cursorIdx, innerStart, 0)
	if join.Condition.On != nil && onSkipAddr != 0 {
		ctx.vm.Program[onSkipAddr].P2 = nextAddr
	}

	afterLoop := ctx.vm.NumOps()
	if join.Type == parser.JoinLeft {
		s.emitNullEmissionAgg(ctx, joinIdx)
	}

	if join.Type == parser.JoinLeft {
		ctx.vm.Program[rewindAddr].P2 = afterLoop
	} else {
		ctx.vm.Program[rewindAddr].P2 = ctx.vm.NumOps()
	}
}

// emitAggLeafRow emits WHERE filter + sorter insert at the leaf of the LEFT JOIN aggregate loop.
func (s *Stmt) emitAggLeafRow(ctx *leftAggCtx) {
	var whereSkip int
	if ctx.stmt.Where != nil {
		whereReg, err := ctx.gen.GenerateExpr(ctx.stmt.Where)
		if err == nil {
			whereSkip = ctx.vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
		}
	}

	_ = s.populateGroupByExprsMultiTable(ctx.vm, ctx.gen, ctx.stmt, ctx.sorterBase)
	s.populateAggregateArgs(ctx.vm, ctx.gen, ctx.stmt.Columns, ctx.sorterBase, ctx.numGroupBy)
	ctx.vm.AddOp(vdbe.OpSorterInsert, ctx.sorterCursor, ctx.sorterBase, ctx.sorterCols)

	if ctx.stmt.Where != nil && whereSkip != 0 {
		ctx.vm.Program[whereSkip].P2 = ctx.vm.NumOps()
	}
}

// emitNullEmissionAgg emits a null-row into the aggregate sorter for LEFT JOIN when no match was found.
func (s *Stmt) emitNullEmissionAgg(ctx *leftAggCtx, joinIdx int) {
	skipAddr := ctx.vm.AddOp(vdbe.OpIf, ctx.flagRegs[joinIdx], 0, 0)

	// Populate GROUP BY keys (some may reference right-side tables and become NULL naturally)
	_ = s.populateGroupByExprsMultiTable(ctx.vm, ctx.gen, ctx.stmt, ctx.sorterBase)

	// Null out aggregate arg slots (right-side columns are not accessible)
	argIdx := ctx.numGroupBy
	for range ctx.sorterCols - ctx.numGroupBy {
		ctx.vm.AddOp(vdbe.OpNull, 0, ctx.sorterBase+argIdx, 0)
		argIdx++
	}

	ctx.vm.AddOp(vdbe.OpSorterInsert, ctx.sorterCursor, ctx.sorterBase, ctx.sorterCols)
	ctx.vm.Program[skipAddr].P2 = ctx.vm.NumOps()
}

// emitJoinAggSorterInsert evaluates the combined WHERE/ON filter, then copies
// GROUP BY keys and aggregate argument values into sorter registers and inserts.
func (s *Stmt) emitJoinAggSorterInsert(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	stmt *parser.SelectStmt, tables []stmtTableInfo,
	sorterCursor, sorterBaseReg, numGroupBy, sorterCols int) error {

	// Combined WHERE + ON filter
	var skipAddr int
	combinedWhere := s.buildCombinedWhereExpression(stmt)
	if combinedWhere != nil {
		whereReg, err := gen.GenerateExpr(combinedWhere)
		if err != nil {
			return err
		}
		skipAddr = vm.AddOp(vdbe.OpIfNot, whereReg, 0, 0)
	}

	// Populate GROUP BY keys
	if err := s.populateGroupByExprsMultiTable(vm, gen, stmt, sorterBaseReg); err != nil {
		return err
	}

	// Populate aggregate argument values
	s.populateAggregateArgs(vm, gen, stmt.Columns, sorterBaseReg, numGroupBy)

	vm.AddOp(vdbe.OpSorterInsert, sorterCursor, sorterBaseReg, sorterCols)

	if combinedWhere != nil {
		vm.Program[skipAddr].P2 = vm.NumOps()
	}
	return nil
}

// populateGroupByExprsMultiTable evaluates GROUP BY expressions in a multi-table
// context, resolving aliases to column expressions before code generation.
func (s *Stmt) populateGroupByExprsMultiTable(vm *vdbe.VDBE, gen *expr.CodeGenerator,
	stmt *parser.SelectStmt, baseReg int) error {

	for i, groupExpr := range stmt.GroupBy {
		resolvedExpr := s.resolveGroupByExpr(stmt, groupExpr)
		reg, err := gen.GenerateExpr(resolvedExpr)
		if err != nil {
			return err
		}
		vm.AddOp(vdbe.OpCopy, reg, baseReg+i, 0)
	}
	return nil
}

// nextCursorAfterTables returns a cursor number one past the highest table cursor,
// allocating space in the VM.
func (s *Stmt) nextCursorAfterTables(tables []stmtTableInfo, vm *vdbe.VDBE) int {
	maxCursor := 0
	for _, tbl := range tables {
		if tbl.cursorIdx+1 > maxCursor {
			maxCursor = tbl.cursorIdx + 1
		}
	}
	vm.AllocCursors(maxCursor + 1)
	return maxCursor
}

// groupByCollationsMultiTable resolves collations for GROUP BY expressions
// across multiple tables.
func (s *Stmt) groupByCollationsMultiTable(groupBy []parser.Expression, tables []stmtTableInfo) []string {
	collations := make([]string, len(groupBy))
	for i, expr := range groupBy {
		collations[i] = resolveExprCollationMultiTable(expr, tables)
	}
	return collations
}

// resolveExprCollationMultiTable resolves collation for an expression across multiple tables.
func resolveExprCollationMultiTable(e parser.Expression, tables []stmtTableInfo) string {
	switch ex := e.(type) {
	case *parser.CollateExpr:
		return strings.ToUpper(ex.Collation)
	case *parser.ParenExpr:
		return resolveExprCollationMultiTable(ex.Expr, tables)
	case *parser.IdentExpr:
		return findColumnCollation(ex, tables)
	default:
		return ""
	}
}

// findColumnCollation looks up the collation for an identifier in the table list.
func findColumnCollation(ident *parser.IdentExpr, tables []stmtTableInfo) string {
	// If table-qualified, search only that table
	if ident.Table != "" {
		for _, tbl := range tables {
			if tbl.name == ident.Table || tbl.table.Name == ident.Table {
				return columnCollation(tbl.table, ident.Name)
			}
		}
		return ""
	}
	// Unqualified: first match wins
	for _, tbl := range tables {
		if c := columnCollation(tbl.table, ident.Name); c != "" {
			return c
		}
	}
	return ""
}

// columnCollation returns the declared collation for a column in a table, or "".
func columnCollation(table *schema.Table, colName string) string {
	idx := table.GetColumnIndex(colName)
	if idx < 0 || idx >= len(table.Columns) {
		return ""
	}
	return table.Columns[idx].Collation
}
