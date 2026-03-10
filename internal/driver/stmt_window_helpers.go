// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"strconv"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
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
		if !isWindowFunction(col) {
			continue
		}
		s.processWindowRankFunction(col.Expr.(*parser.FunctionExpr), &info, table)
	}
	return info
}

// isWindowFunction checks if a result column is a window function
func isWindowFunction(col parser.ResultColumn) bool {
	fnExpr, ok := col.Expr.(*parser.FunctionExpr)
	return ok && fnExpr.Over != nil
}

// processWindowRankFunction processes a single window rank function
func (s *Stmt) processWindowRankFunction(fnExpr *parser.FunctionExpr, info *rankFunctionInfo, table *schema.Table) {
	if !isRankFunction(fnExpr.Name) {
		return
	}

	updateRankInfo(fnExpr.Name, info)

	if shouldExtractOrderBy(fnExpr, info) {
		info.orderByCols = s.extractWindowOrderByCols(fnExpr.Over.OrderBy, table)
	}
}

// isRankFunction checks if a function name is a rank function
func isRankFunction(name string) bool {
	return name == "RANK" || name == "DENSE_RANK"
}

// updateRankInfo updates the rank info flags based on function name
func updateRankInfo(name string, info *rankFunctionInfo) {
	if name == "RANK" {
		info.hasRank = true
	} else if name == "DENSE_RANK" {
		info.hasDenseRank = true
	}
}

// shouldExtractOrderBy checks if ORDER BY columns should be extracted
func shouldExtractOrderBy(fnExpr *parser.FunctionExpr, info *rankFunctionInfo) bool {
	return fnExpr.Over.OrderBy != nil && len(info.orderByCols) == 0
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
	vm.AddOp(vdbe.OpInteger, 0, regs.rank, 0)        // Start at 0, will be set to 1 on first row
	vm.AddOp(vdbe.OpInteger, 0, regs.denseRank, 0)  // Start at 0, will be set to 1 on first row

	// Initialize prevOrderBy registers to NULL for comparison
	// We need at least 10 registers to handle multiple ORDER BY columns
	maxOrderByCols := 10
	if len(info.orderByCols) > 0 {
		maxOrderByCols = len(info.orderByCols)
	}
	for idx := 0; idx < maxOrderByCols; idx++ {
		vm.AddOp(vdbe.OpNull, 0, regs.prevOrderBy+idx, 0)
	}
}

// emitWindowRankTracking emits rank comparison and update logic
func emitWindowRankTracking(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo, numCols int) {
	if (info.hasRank || info.hasDenseRank) && len(info.orderByCols) > 0 {
		emitWindowRankComparison(vm, regs, info, numCols, false)
	} else {
		vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)
	}
}

// emitWindowRankTrackingFromSorter emits rank comparison and update logic when reading from sorter
func emitWindowRankTrackingFromSorter(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo, numCols int) {
	if (info.hasRank || info.hasDenseRank) && len(info.orderByCols) > 0 {
		emitWindowRankComparison(vm, regs, info, numCols, true)
	} else {
		vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)
	}
}

// emitWindowRankComparison emits the comparison logic for rank functions
// If fromSorter is true, data is already in registers 0..N-1 from OpSorterData
// If fromSorter is false, data is read from cursor 0 using OpColumn
func emitWindowRankComparison(vm *vdbe.VDBE, regs rankRegisters, info rankFunctionInfo, numCols int, fromSorter bool) {
	// Determine which columns to compare
	orderByCols := info.orderByCols
	if len(orderByCols) == 0 && (info.hasRank || info.hasDenseRank) {
		// Fallback: if no ORDER BY columns specified but we have ranking,
		// this might indicate a bug in orderByCols extraction
		// For now, do nothing - all ranks will be the same
		vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)
		return
	}

	// Read current ORDER BY values
	for idx, colIdx := range orderByCols {
		if fromSorter {
			// Data is already in registers from OpSorterData, just copy it
			vm.AddOp(vdbe.OpCopy, colIdx, regs.currOrderBy+idx, 0)
		} else {
			// Read from cursor 0
			vm.AddOp(vdbe.OpColumn, 0, colIdx, regs.currOrderBy+idx)
		}
	}

	valuesChangedReg := numCols + 40
	vm.AddOp(vdbe.OpInteger, 0, valuesChangedReg, 0)

	// Increment rowCount BEFORE checking for changes (rowCount is 1-based)
	vm.AddOp(vdbe.OpAddImm, regs.rowCount, 1, 0)

	// Check if values changed and update rank accordingly
	emitOrderByValueComparison(vm, regs, orderByCols, valuesChangedReg)
	emitWindowRankUpdate(vm, regs, valuesChangedReg, info)
}

// emitOrderByValueComparison compares current and previous ORDER BY values
func emitOrderByValueComparison(vm *vdbe.VDBE, regs rankRegisters, orderByCols []int, valuesChangedReg int) {
	// If no ORDER BY columns (shouldn't happen for RANK/DENSE_RANK, but just in case),
	// always mark as changed so rank increments like ROW_NUMBER
	if len(orderByCols) == 0 {
		vm.AddOp(vdbe.OpInteger, 1, valuesChangedReg, 0)
		return
	}

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

	// When values change: update rank to current rowCount
	vm.AddOp(vdbe.OpCopy, regs.rowCount, regs.rank, 0)
	// Always increment dense_rank when values change
	vm.AddOp(vdbe.OpAddImm, regs.denseRank, 1, 0)

	// Copy current ORDER BY values to prev for next comparison
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
	case "ROW_NUMBER":
		vm.AddOp(vdbe.OpCopy, regs.rowCount, colIdx, 0)
	case "NTILE":
		// NTILE uses rowCount for now - proper implementation uses window state
		vm.AddOp(vdbe.OpCopy, regs.rowCount, colIdx, 0)
	case "RANK":
		vm.AddOp(vdbe.OpCopy, regs.rank, colIdx, 0)
	case "DENSE_RANK":
		vm.AddOp(vdbe.OpCopy, regs.denseRank, colIdx, 0)
	case "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE":
		// These require window state access - emit placeholder for now
		// Real implementation uses OpWindowLag/Lead/FirstValue/LastValue opcodes
		vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
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

// emitWindowFunctionColumnWithOpcodes emits code for a window function using proper opcodes
// numTableCols is used for streaming mode to read from registers
func (s *Stmt) emitWindowFunctionColumnWithOpcodes(vm *vdbe.VDBE, fnExpr *parser.FunctionExpr, colIdx int, numTableCols int) {
	windowStateIdx := 0 // For now, we use window state 0 for all rank functions

	switch fnExpr.Name {
	case "RANK":
		// P3 = number of columns for streaming mode
		vm.AddOp(vdbe.OpWindowRank, windowStateIdx, colIdx, numTableCols)
	case "DENSE_RANK":
		// P3 = number of columns for streaming mode
		vm.AddOp(vdbe.OpWindowDenseRank, windowStateIdx, colIdx, numTableCols)
	case "ROW_NUMBER":
		// P3 = number of columns for streaming mode
		vm.AddOp(vdbe.OpWindowRowNum, windowStateIdx, colIdx, numTableCols)
	case "NTILE":
		// P1 = window state, P2 = output register, P3 = number of buckets
		numBuckets := s.extractNtileArg(fnExpr)
		vm.AddOp(vdbe.OpWindowNtile, windowStateIdx, colIdx, numBuckets)
	case "LAG":
		// P1 = window state, P2 = output register, P3 = column index
		// P4.I = offset (default 1), P5 = default value register (0 = NULL)
		colIndex, offset := s.extractLagLeadArgs(fnExpr, numTableCols)
		addr := vm.AddOp(vdbe.OpWindowLag, windowStateIdx, colIdx, colIndex)
		vm.Program[addr].P4.I = int32(offset)
	case "LEAD":
		// P1 = window state, P2 = output register, P3 = column index
		// P4.I = offset (default 1), P5 = default value register (0 = NULL)
		colIndex, offset := s.extractLagLeadArgs(fnExpr, numTableCols)
		addr := vm.AddOp(vdbe.OpWindowLead, windowStateIdx, colIdx, colIndex)
		vm.Program[addr].P4.I = int32(offset)
	case "FIRST_VALUE":
		// P1 = window state, P2 = output register, P3 = column index
		colIndex := s.extractValueFunctionArg(fnExpr, numTableCols)
		vm.AddOp(vdbe.OpWindowFirstValue, windowStateIdx, colIdx, colIndex)
	case "LAST_VALUE":
		// P1 = window state, P2 = output register, P3 = column index
		colIndex := s.extractValueFunctionArg(fnExpr, numTableCols)
		vm.AddOp(vdbe.OpWindowLastValue, windowStateIdx, colIdx, colIndex)
	default:
		vm.AddOp(vdbe.OpNull, 0, colIdx, 0)
	}
}

// extractNtileArg extracts the number of buckets from NTILE(n) function
func (s *Stmt) extractNtileArg(fnExpr *parser.FunctionExpr) int {
	if len(fnExpr.Args) > 0 {
		if lit, ok := fnExpr.Args[0].(*parser.LiteralExpr); ok {
			// LiteralExpr.Value is a string, parse it
			if n, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
				return int(n)
			}
		}
	}
	return 4 // Default to 4 buckets
}

// extractLagLeadArgs extracts column index and offset from LAG/LEAD functions
func (s *Stmt) extractLagLeadArgs(fnExpr *parser.FunctionExpr, numTableCols int) (colIndex int, offset int) {
	offset = 1 // Default offset
	colIndex = 0

	if len(fnExpr.Args) > 0 {
		// First arg is the column expression
		if ident, ok := fnExpr.Args[0].(*parser.IdentExpr); ok {
			// Try to find column index - for now use a simple approach
			colIndex = s.findColumnIndexByName(ident.Name, numTableCols)
		}
	}

	if len(fnExpr.Args) > 1 {
		// Second arg is the offset
		if lit, ok := fnExpr.Args[1].(*parser.LiteralExpr); ok {
			// LiteralExpr.Value is a string, parse it
			if n, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
				offset = int(n)
			}
		}
	}

	return colIndex, offset
}

// extractValueFunctionArg extracts column index from FIRST_VALUE/LAST_VALUE
func (s *Stmt) extractValueFunctionArg(fnExpr *parser.FunctionExpr, numTableCols int) int {
	if len(fnExpr.Args) > 0 {
		if ident, ok := fnExpr.Args[0].(*parser.IdentExpr); ok {
			return s.findColumnIndexByName(ident.Name, numTableCols)
		}
	}
	return 0
}

// findColumnIndexByName finds column index by name - simple implementation
func (s *Stmt) findColumnIndexByName(name string, numTableCols int) int {
	// Try to find in current table context
	if s.conn != nil && s.conn.schema != nil {
		for _, t := range s.conn.schema.Tables {
			for i, col := range t.Columns {
				if col.Name == name {
					return i
				}
			}
		}
	}
	return 0 // Default to first column
}

// extractWindowFrame converts parser.FrameSpec to vdbe.WindowFrame
func (s *Stmt) extractWindowFrame(frameSpec *parser.FrameSpec) vdbe.WindowFrame {
	if frameSpec == nil {
		return vdbe.DefaultWindowFrame()
	}

	frame := vdbe.WindowFrame{
		Type:  s.convertFrameMode(frameSpec.Mode),
		Start: s.convertFrameBound(frameSpec.Start),
		End:   s.convertFrameBound(frameSpec.End),
	}

	return frame
}

// convertFrameMode converts parser.FrameMode to vdbe.WindowFrameType
func (s *Stmt) convertFrameMode(mode parser.FrameMode) vdbe.WindowFrameType {
	switch mode {
	case parser.FrameRows:
		return vdbe.FrameRows
	case parser.FrameRange:
		return vdbe.FrameRange
	case parser.FrameGroups:
		return vdbe.FrameGroups
	default:
		return vdbe.FrameRange // Default to RANGE
	}
}

// convertFrameBound converts parser.FrameBound to vdbe.WindowFrameBound
func (s *Stmt) convertFrameBound(bound parser.FrameBound) vdbe.WindowFrameBound {
	vdbeBound := vdbe.WindowFrameBound{
		Type:   s.convertFrameBoundType(bound.Type),
		Offset: 0,
	}

	// Extract offset value if present
	if bound.Offset != nil {
		if lit, ok := bound.Offset.(*parser.LiteralExpr); ok {
			if n, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
				vdbeBound.Offset = int(n)
			}
		}
	}

	return vdbeBound
}

// convertFrameBoundType converts parser.FrameBoundType to vdbe.FrameBoundType
func (s *Stmt) convertFrameBoundType(boundType parser.FrameBoundType) vdbe.FrameBoundType {
	switch boundType {
	case parser.BoundUnboundedPreceding:
		return vdbe.BoundUnboundedPreceding
	case parser.BoundPreceding:
		return vdbe.BoundPreceding
	case parser.BoundCurrentRow:
		return vdbe.BoundCurrentRow
	case parser.BoundFollowing:
		return vdbe.BoundFollowing
	case parser.BoundUnboundedFollowing:
		return vdbe.BoundUnboundedFollowing
	default:
		return vdbe.BoundCurrentRow
	}
}
