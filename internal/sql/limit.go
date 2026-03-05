// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"fmt"
)

// LimitCompiler handles LIMIT and OFFSET clause compilation.
type LimitCompiler struct {
	parse *Parse
}

// NewLimitCompiler creates a new LIMIT/OFFSET compiler.
func NewLimitCompiler(parse *Parse) *LimitCompiler {
	return &LimitCompiler{parse: parse}
}

// LimitInfo holds runtime information for LIMIT/OFFSET processing.
type LimitInfo struct {
	Limit      int // LIMIT value (0 = no limit)
	Offset     int // OFFSET value (0 = no offset)
	LimitReg   int // Register holding current limit counter
	OffsetReg  int // Register holding current offset counter
	AddrLimit  int // Address of limit check instruction
	AddrOffset int // Address of offset check instruction
}

// applyLimit applies LIMIT and OFFSET to a SELECT.
func (sc *SelectCompiler) applyLimit(sel *Select, dest *SelectDest, breakAddr int) error {
	lc := NewLimitCompiler(sc.parse)
	return lc.applyLimit(sel, dest, breakAddr)
}

// applyLimit generates code to enforce LIMIT and OFFSET.
func (lc *LimitCompiler) applyLimit(sel *Select, dest *SelectDest, breakAddr int) error {
	vdbe := lc.parse.GetVdbe()

	// Handle LIMIT
	if sel.Limit > 0 {
		// Allocate register for limit counter
		regLimit := lc.parse.AllocReg()

		// Initialize limit counter
		vdbe.AddOp2(OP_Integer, sel.Limit, regLimit)

		// Each row decrements counter and checks if done
		vdbe.AddOp3(OP_IfNot, regLimit, breakAddr, -1)

		lc.parse.ReleaseReg(regLimit)
	}

	// OFFSET is typically handled during row scanning, not here
	// This is just for completeness

	return nil
}

// CompileLimitOffset compiles LIMIT and OFFSET expressions.
// In SQLite, these can be expressions, not just constants.
func (lc *LimitCompiler) CompileLimitOffset(sel *Select) (*LimitInfo, error) {
	info := &LimitInfo{}

	// Compile LIMIT expression
	if sel.Limit > 0 {
		info.Limit = sel.Limit
		info.LimitReg = lc.parse.AllocReg()

		vdbe := lc.parse.GetVdbe()
		vdbe.AddOp2(OP_Integer, sel.Limit, info.LimitReg)
	}

	// Compile OFFSET expression
	if sel.Offset > 0 {
		info.Offset = sel.Offset
		info.OffsetReg = lc.parse.AllocReg()

		vdbe := lc.parse.GetVdbe()
		vdbe.AddOp2(OP_Integer, sel.Offset, info.OffsetReg)
	}

	return info, nil
}

// GenerateLimitCode generates code to check and enforce LIMIT.
// This is called for each row in the result set.
func (lc *LimitCompiler) GenerateLimitCode(info *LimitInfo, jumpIfDone int) {
	if info.Limit == 0 {
		return // No limit
	}

	vdbe := lc.parse.GetVdbe()

	// Decrement limit counter
	vdbe.AddOp2(OP_AddImm, info.LimitReg, -1)

	// Jump if limit reached (counter becomes negative)
	info.AddrLimit = vdbe.AddOp3(OP_IfNot, info.LimitReg, jumpIfDone, 1)
}

// GenerateOffsetCode generates code to skip OFFSET rows.
// This is called before including each row in the result.
func (lc *LimitCompiler) GenerateOffsetCode(info *LimitInfo, jumpToNext int) {
	if info.Offset == 0 {
		return // No offset
	}

	vdbe := lc.parse.GetVdbe()

	// Check if still skipping rows
	// If offset counter > 0, decrement and skip this row
	info.AddrOffset = vdbe.AddOp3(OP_IfPos, info.OffsetReg, jumpToNext, -1)
}

// OptimizeLimitWithIndex checks if LIMIT can be optimized using an index.
// Returns true if optimization applied.
func (lc *LimitCompiler) OptimizeLimitWithIndex(sel *Select, info *LimitInfo) bool {
	// LIMIT optimization is possible when:
	// 1. There's an ORDER BY that matches an index
	// 2. We can stop scanning after LIMIT rows
	// 3. No aggregates or grouping

	if info.Limit == 0 {
		return false
	}

	// Check for aggregates
	if sel.GroupBy != nil && sel.GroupBy.Len() > 0 {
		return false
	}

	// Check for ORDER BY
	if sel.OrderBy == nil || sel.OrderBy.Len() == 0 {
		return false
	}

	// Would need index matching logic here
	// For now, return false (no optimization)
	return false
}

// ComputeLimitOffset computes static LIMIT/OFFSET values at compile time.
func (lc *LimitCompiler) ComputeLimitOffset(limitExpr *Expr, offsetExpr *Expr) (limit int, offset int, err error) {
	// Evaluate LIMIT expression
	if limitExpr != nil {
		if limitExpr.Op == TK_INTEGER {
			limit = limitExpr.IntValue
			if limit < 0 {
				return 0, 0, fmt.Errorf("LIMIT must be non-negative")
			}
		} else {
			// Complex expression - would need to evaluate at runtime
			return 0, 0, fmt.Errorf("LIMIT must be a constant expression")
		}
	}

	// Evaluate OFFSET expression
	if offsetExpr != nil {
		if offsetExpr.Op == TK_INTEGER {
			offset = offsetExpr.IntValue
			if offset < 0 {
				return 0, 0, fmt.Errorf("OFFSET must be non-negative")
			}
		} else {
			// Complex expression - would need to evaluate at runtime
			return 0, 0, fmt.Errorf("OFFSET must be a constant expression")
		}
	}

	return limit, offset, nil
}

// GenerateLimitOffsetPlan creates an execution plan for LIMIT/OFFSET.
func (lc *LimitCompiler) GenerateLimitOffsetPlan(sel *Select) (*LimitPlan, error) {
	plan := &LimitPlan{
		HasLimit:  sel.Limit > 0,
		HasOffset: sel.Offset > 0,
		Limit:     sel.Limit,
		Offset:    sel.Offset,
	}

	// Determine where to apply LIMIT/OFFSET
	if sel.OrderBy != nil && sel.OrderBy.Len() > 0 {
		// With ORDER BY: apply after sorting
		plan.ApplyAfterSort = true
	} else if sel.GroupBy != nil && sel.GroupBy.Len() > 0 {
		// With GROUP BY: apply after grouping
		plan.ApplyAfterGroup = true
	} else {
		// Simple query: apply during scan
		plan.ApplyDuringScan = true
	}

	// Check if we can optimize with index
	if plan.ApplyDuringScan {
		plan.CanUseIndex = lc.canOptimizeWithIndex(sel)
	}

	return plan, nil
}

// LimitPlan describes how to execute LIMIT/OFFSET.
type LimitPlan struct {
	HasLimit        bool // True if LIMIT clause present
	HasOffset       bool // True if OFFSET clause present
	Limit           int  // LIMIT value
	Offset          int  // OFFSET value
	ApplyDuringScan bool // Apply during table scan
	ApplyAfterSort  bool // Apply after ORDER BY sort
	ApplyAfterGroup bool // Apply after GROUP BY
	CanUseIndex     bool // Can use index to optimize
}

// canOptimizeWithIndex checks if LIMIT can use index optimization.
func (lc *LimitCompiler) canOptimizeWithIndex(sel *Select) bool {
	// Simple heuristic: can optimize if:
	// 1. Single table in FROM
	// 2. No complex WHERE clause
	// 3. Have an index available (would need to check catalog)

	if sel.Src == nil || sel.Src.Len() != 1 {
		return false
	}

	// Would check for suitable index here
	return false
}

// GenerateLimitedScan generates code for a table scan with LIMIT.
// This can stop scanning early when LIMIT is reached.
func (lc *LimitCompiler) GenerateLimitedScan(
	cursor int,
	rootPage int,
	limit int,
	offset int,
	destReg int,
) error {
	vdbe := lc.parse.GetVdbe()
	vdbe.AddOp2(OP_OpenRead, cursor, rootPage)

	addrEnd := vdbe.MakeLabel()
	vdbe.AddOp2(OP_Rewind, cursor, addrEnd)

	regOffset, regLimit := lc.initializeCounters(offset, limit)
	addrLoop := vdbe.CurrentAddr()
	lc.generateScanLoop(offset, limit, regOffset, regLimit, addrEnd)
	vdbe.AddOp2(OP_Next, cursor, addrLoop)

	vdbe.ResolveLabel(addrEnd)
	vdbe.AddOp1(OP_Close, cursor)
	lc.cleanupCounters(regOffset, regLimit)

	return nil
}

// initializeCounters initializes offset and limit counters.
func (lc *LimitCompiler) initializeCounters(offset, limit int) (regOffset, regLimit int) {
	vdbe := lc.parse.GetVdbe()
	if offset > 0 {
		regOffset = lc.parse.AllocReg()
		vdbe.AddOp2(OP_Integer, offset, regOffset)
	}
	if limit > 0 {
		regLimit = lc.parse.AllocReg()
		vdbe.AddOp2(OP_Integer, limit, regLimit)
	}
	return
}

// generateScanLoop generates the main scan loop logic.
func (lc *LimitCompiler) generateScanLoop(offset, limit, regOffset, regLimit, addrEnd int) {
	if offset > 0 {
		lc.generateScanWithOffset(limit, regOffset, regLimit, addrEnd)
	} else {
		lc.generateScanNoOffset(limit, regLimit, addrEnd)
	}
}

// generateScanWithOffset generates scan logic with offset handling.
func (lc *LimitCompiler) generateScanWithOffset(limit, regOffset, regLimit, addrEnd int) {
	vdbe := lc.parse.GetVdbe()
	addrSkip := vdbe.MakeLabel()
	vdbe.AddOp3(OP_IfPos, regOffset, addrSkip, -1)
	lc.applyLimitInLoop(limit, regLimit, addrEnd)
	vdbe.ResolveLabel(addrSkip)
}

// generateScanNoOffset generates scan logic without offset.
func (lc *LimitCompiler) generateScanNoOffset(limit, regLimit, addrEnd int) {
	lc.applyLimitInLoop(limit, regLimit, addrEnd)
}

// applyLimitInLoop applies limit check in the scan loop.
func (lc *LimitCompiler) applyLimitInLoop(limit, regLimit, addrEnd int) {
	if limit > 0 {
		vdbe := lc.parse.GetVdbe()
		vdbe.AddOp2(OP_AddImm, regLimit, -1)
		vdbe.AddOp3(OP_IfNot, regLimit, addrEnd, 1)
	}
}

// cleanupCounters releases allocated counter registers.
func (lc *LimitCompiler) cleanupCounters(regOffset, regLimit int) {
	if regOffset != 0 {
		lc.parse.ReleaseReg(regOffset)
	}
	if regLimit != 0 {
		lc.parse.ReleaseReg(regLimit)
	}
}

// CombineLimitOffset combines LIMIT and OFFSET into effective limit.
// For example, "LIMIT 10 OFFSET 5" effectively needs to scan 15 rows.
func (lc *LimitCompiler) CombineLimitOffset(limit int, offset int) int {
	if limit == 0 {
		return 0 // No limit
	}

	// Effective limit is LIMIT + OFFSET
	// We need to read (offset + limit) rows total
	return limit + offset
}

// SplitLimitOffset splits effective limit back to LIMIT and OFFSET.
func (lc *LimitCompiler) SplitLimitOffset(effective int, offset int) int {
	if effective == 0 || offset == 0 {
		return effective
	}

	if effective > offset {
		return effective - offset
	}

	return 0
}

// ValidateLimitOffset validates LIMIT and OFFSET values.
func (lc *LimitCompiler) ValidateLimitOffset(limit int, offset int) error {
	if limit < 0 {
		return fmt.Errorf("LIMIT must be non-negative, got %d", limit)
	}

	if offset < 0 {
		return fmt.Errorf("OFFSET must be non-negative, got %d", offset)
	}

	// Check for potential overflow
	const maxInt = int(^uint(0) >> 1)
	if limit > 0 && offset > 0 {
		if limit > maxInt-offset {
			return fmt.Errorf("LIMIT + OFFSET overflow")
		}
	}

	return nil
}
