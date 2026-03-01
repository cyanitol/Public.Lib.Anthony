// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"github.com/JuniperBible/Public.Lib.Anthony/internal/expr"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// rankRegisters holds register indices for rank tracking in window functions
type rankRegisters struct {
	rowCount    int
	rank        int
	denseRank   int
	prevOrderBy int
	currOrderBy int
}

// rankFunctionInfo holds information about rank functions in query
type rankFunctionInfo struct {
	hasRank      bool
	hasDenseRank bool
	orderByCols  []int
}

// initWindowRankRegisters allocates register indices for rank tracking
func initWindowRankRegisters(numCols int) rankRegisters {
	return rankRegisters{
		rowCount:    numCols + 10,
		rank:        numCols + 11,
		denseRank:   numCols + 12,
		prevOrderBy: numCols + 20,
		currOrderBy: numCols + 30,
	}
}

// analyzeWindowRankFunctions analyzes columns to find rank functions and their ORDER BY
func (s *Stmt) analyzeWindowRankFunctions(expandedCols []parser.ResultColumn, table *schema.Table) rankFunctionInfo {
	info := rankFunctionInfo{}
	for _, col := range expandedCols {
		fnExpr, ok := col.Expr.(*parser.FunctionExpr)
		if !ok || fnExpr.Over == nil {
			continue
		}

		if fnExpr.Name == "rank" || fnExpr.Name == "dense_rank" {
			if fnExpr.Name == "rank" {
				info.hasRank = true
			} else {
				info.hasDenseRank = true
			}

			if fnExpr.Over.OrderBy != nil && len(info.orderByCols) == 0 {
				info.orderByCols = s.extractWindowOrderByCols(fnExpr.Over.OrderBy, table)
			}
		}
	}
	return info
}

// extractWindowOrderByCols extracts column indices from ORDER BY terms
func (s *Stmt) extractWindowOrderByCols(orderBy []parser.OrderingTerm, table *schema.Table) []int {
	var cols []int
	for _, orderTerm := range orderBy {
		identExpr, ok := orderTerm.Expr.(*parser.IdentExpr)
		if !ok {
			continue
		}
		colIdx := s.findColumnIndex(table, identExpr.Name)
		if colIdx >= 0 {
			cols = append(cols, colIdx)
		}
	}
	return cols
}

// emitWindowRankSetup emits initialization opcodes for window rank tracking
func emitWindowRankSetup(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo) {
	vm.AddOp(vdbe.OpInteger, 0, regs.rowCount, 0)
	vm.AddOp(vdbe.OpInteger, 1, regs.rank, 0)
	vm.AddOp(vdbe.OpInteger, 1, regs.denseRank, 0)

	for idx := range info.orderByCols {
		vm.AddOp(vdbe.OpNull, 0, regs.prevOrderBy+idx, 0)
	}
}

// emitWindowRankTracking emits rank comparison and update logic
func emitWindowRankTracking(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo, numCols int) {
	if (info.hasRank || info.hasDenseRank) && len(info.orderByCols) > 0 {
		emitWindowRankComparison(vm, regs, info, numCols)
	} else {
		vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)
	}
}

// emitWindowRankComparison emits the comparison logic for rank functions
func emitWindowRankComparison(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo, numCols int) {
	// Read current ORDER BY values
	for idx, colIdx := range info.orderByCols {
		vm.AddOp(vdbe.OpColumn, 0, colIdx, regs.currOrderBy+idx)
	}

	valuesChangedReg := numCols + 40
	vm.AddOp(vdbe.OpInteger, 0, valuesChangedReg, 0)

	emitOrderByValueComparison(vm, regs, info.orderByCols, valuesChangedReg)
	vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)
	emitWindowRankUpdate(vm, regs, valuesChangedReg, info)
}

// emitOrderByValueComparison compares current and previous ORDER BY values
func emitOrderByValueComparison(vm *vdbe.VDBE, regs rankRegisters, orderByCols []int, valuesChangedReg int) {
	for idx := range orderByCols {
		curr := regs.currOrderBy + idx
		prev := regs.prevOrderBy + idx

		isNullAddr := vm.AddOp(vdbe.OpNotNull, prev, 0, 0)
		vm.AddOp(vdbe.OpInteger, 1, valuesChangedReg, 0)
		skipNullAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)

		vm.Program[isNullAddr].P2 = vm.NumOps()
		neAddr := vm.AddOp(vdbe.OpNe, curr, 0, prev)
		skipNeAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)

		vm.Program[neAddr].P2 = vm.NumOps()
		vm.AddOp(vdbe.OpInteger, 1, valuesChangedReg, 0)

		vm.Program[skipNeAddr].P2 = vm.NumOps()
		vm.Program[skipNullAddr].P2 = vm.NumOps()
	}
}

// emitWindowRankUpdate updates rank values when ORDER BY changes
func emitWindowRankUpdate(vm *vdbe.VDBE, regs rankRegisters, valuesChangedReg int, info rankFunctionInfo) {
	updateRankAddr := vm.AddOp(vdbe.OpIfNot, valuesChangedReg, 0, 0)

	vm.AddOp(vdbe.OpCopy, regs.rowCount, regs.rank, 0)

	firstRowAddr := vm.AddOp(vdbe.OpIsNull, regs.prevOrderBy, 0, 0)
	vm.AddOp(vdbe.OpAddImm, regs.denseRank, 1, 0)
	skipDenseIncAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	vm.Program[firstRowAddr].P2 = vm.NumOps()
	vm.Program[skipDenseIncAddr].P2 = vm.NumOps()

	for idx := range info.orderByCols {
		vm.AddOp(vdbe.OpCopy, regs.currOrderBy+idx, regs.prevOrderBy+idx, 0)
	}

	skipUpdateRankAddr := vm.AddOp(vdbe.OpGoto, 0, 0, 0)
	vm.Program[updateRankAddr].P2 = vm.NumOps()
	vm.Program[skipUpdateRankAddr].P2 = vm.NumOps()
}

// emitWindowFunctionColumn emits code for a window function result column
func emitWindowFunctionColumn(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, regs rankRegisters, colIdx int) {
	switch fnExpr.Name {
	case "row_number", "ntile":
		vm.AddOp(vdbe.OpCopy, regs.rowCount, colIdx, 0)
	case "rank":
		vm.AddOp(vdbe.OpCopy, regs.rank, colIdx, 0)
	case "dense_rank":
		vm.AddOp(vdbe.OpCopy, regs.denseRank, colIdx, 0)
	default:
		vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
	}
}

// emitWindowColumn emits code for a single column (window function or regular)
func (s *Stmt) emitWindowColumn(vm *vdbe.VDBE, gen *expr.CodeGenerator, col parser.ResultColumn,
	table *schema.Table, regs rankRegisters, colIdx int) {

	if fnExpr, ok := col.Expr.(*parser.FunctionExpr); ok && fnExpr.Over != nil {
		emitWindowFunctionColumn(vm, fnExpr, regs, colIdx)
		return
	}

	// Regular column
	if identExpr, ok := col.Expr.(*parser.IdentExpr); ok {
		tableColIdx := s.findColumnIndex(table, identExpr.Name)
		if tableColIdx >= 0 {
			vm.AddOp(vdbe.OpColumn, 0, tableColIdx, colIdx)
		} else {
			vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
		}
		return
	}

	// Other expressions
	reg, err := gen.GenerateExpr(col.Expr)
	if err == nil && reg != colIdx {
		vm.AddOp(vdbe.OpCopy, reg, colIdx, 0)
	} else {
		vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
	}
}
