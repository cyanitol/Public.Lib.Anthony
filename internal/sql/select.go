// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package sql implements SQL statement compilation to VDBE bytecode.
package sql

import (
	"fmt"
)

// SelectCompiler compiles SELECT statements to VDBE bytecode.
type SelectCompiler struct {
	parse *Parse
}

// SelectDest describes where SELECT results should go.
type SelectDest struct {
	Dest    SelectDestType // Destination type
	SDParm  int            // First parameter (meaning depends on Dest)
	SDParm2 int            // Second parameter
	AffSdst string         // Affinity string for SRT_Set destination
	Sdst    int            // Base register to hold results
	NSdst   int            // Number of result columns
}

// SelectDestType defines how to dispose of query results.
type SelectDestType int

const (
	SRT_Output    SelectDestType = iota + 1 // Output to callback/result row
	SRT_Mem                                 // Store result in a memory cell
	SRT_Set                                 // Store results as keys in an index
	SRT_Union                               // Store results as keys of union
	SRT_Except                              // Remove result from union
	SRT_Exists                              // Store 1 if result is not empty
	SRT_Table                               // Store results in a table
	SRT_EphemTab                            // Store results in ephemeral table
	SRT_Coroutine                           // Generate a single row of result
	SRT_Fifo                                // Store results in FIFO queue
	SRT_DistFifo                            // Like SRT_Fifo but distinct
	SRT_Queue                               // Store results in priority queue
	SRT_DistQueue                           // Like SRT_Queue but distinct
	SRT_Upfrom                              // Store results for UPDATE FROM
)

// DistinctCtx records information about how to process DISTINCT keyword.
type DistinctCtx struct {
	IsTnct    uint8 // 0: Not distinct. 1: DISTINCT  2: DISTINCT and ORDER BY
	ETnctType uint8 // One of WHERE_DISTINCT_* operators
	TabTnct   int   // Ephemeral table used for DISTINCT processing
	AddrTnct  int   // Address of OP_OpenEphemeral opcode for TabTnct
}

// WHERE_DISTINCT_* values for DISTINCT processing
const (
	WHERE_DISTINCT_NOOP      = 0 // No DISTINCT keyword
	WHERE_DISTINCT_UNIQUE    = 1 // DISTINCT can be optimized away
	WHERE_DISTINCT_ORDERED   = 2 // DISTINCT with ORDER BY optimization
	WHERE_DISTINCT_UNORDERED = 3 // DISTINCT requires ephemeral table
)

// SortCtx contains information about ORDER BY or GROUP BY clause.
type SortCtx struct {
	OrderBy       *ExprList // The ORDER BY (or GROUP BY clause)
	NOBSat        int       // Number of ORDER BY terms satisfied by indices
	ECursor       int       // Cursor number for the sorter
	RegReturn     int       // Register holding block-output return address
	LabelBkOut    int       // Start label for the block-output subroutine
	AddrSortIndex int       // Address of the OP_SorterOpen or OP_OpenEphemeral
	LabelDone     int       // Jump here when done, ex: LIMIT reached
	LabelOBLopt   int       // Jump here when sorter is full
	SortFlags     uint8     // Zero or more SORTFLAG_* bits
	DeferredRowLd *RowLoadInfo
}

// SORTFLAG_* bit values
const (
	SORTFLAG_UseSorter = 0x01 // Use SorterOpen instead of OpenEphemeral
)

// RowLoadInfo contains information for loading a result row.
type RowLoadInfo struct {
	RegResult int   // Start of memory holding result
	EcelFlags uint8 // ExprCodeExprList flags
}

// NewSelectCompiler creates a new SELECT compiler.
func NewSelectCompiler(parse *Parse) *SelectCompiler {
	return &SelectCompiler{
		parse: parse,
	}
}

// InitSelectDest initializes a SelectDest structure.
func InitSelectDest(dest *SelectDest, destType SelectDestType, parm int) {
	dest.Dest = destType
	dest.SDParm = parm
	dest.SDParm2 = 0
	dest.AffSdst = ""
	dest.Sdst = 0
	dest.NSdst = 0
}

// CompileSelect compiles a SELECT statement to VDBE bytecode.
// This is the main entry point for SELECT compilation.
func (c *SelectCompiler) CompileSelect(sel *Select, dest *SelectDest) error {
	if sel == nil {
		return fmt.Errorf("nil SELECT statement")
	}

	// Handle compound SELECT (UNION, INTERSECT, EXCEPT)
	if sel.Prior != nil {
		return c.compileCompoundSelect(sel, dest)
	}

	// Simple SELECT
	return c.compileSimpleSelect(sel, dest)
}

// compileSimpleSelect compiles a single (non-compound) SELECT statement.
func (c *SelectCompiler) compileSimpleSelect(sel *Select, dest *SelectDest) error {
	vdbe := c.parse.GetVdbe()
	if vdbe == nil {
		return fmt.Errorf("no VDBE available")
	}

	c.ensureOutputDest(sel, dest)
	addrEnd := vdbe.MakeLabel()

	if err := c.processFromClause(sel); err != nil {
		return err
	}

	distinct, sort, hasOrderBy, err := c.setupSelectContexts(sel)
	if err != nil {
		return err
	}

	addrBreak := vdbe.MakeLabel()
	addrContinue := vdbe.MakeLabel()

	if err := c.compileSelectLoop(sel, dest, &sort, &distinct, hasOrderBy, addrBreak, addrContinue); err != nil {
		return err
	}

	vdbe.ResolveLabel(addrBreak)
	vdbe.ResolveLabel(addrEnd)

	return nil
}

// ensureOutputDest allocates output registers in dest if not already allocated.
func (c *SelectCompiler) ensureOutputDest(sel *Select, dest *SelectDest) {
	if dest.Sdst == 0 {
		dest.Sdst = c.parse.AllocRegs(sel.EList.Len())
		dest.NSdst = sel.EList.Len()
	}
}

// setupSelectContexts initialises DISTINCT and ORDER BY contexts for sel.
func (c *SelectCompiler) setupSelectContexts(sel *Select) (DistinctCtx, SortCtx, bool, error) {
	var distinct DistinctCtx
	if sel.SelFlags&SF_Distinct != 0 {
		c.setupDistinct(sel, &distinct)
	}

	var sort SortCtx
	hasOrderBy := sel.OrderBy != nil && sel.OrderBy.Len() > 0
	if hasOrderBy {
		if err := c.setupOrderBy(sel, &sort); err != nil {
			return distinct, sort, false, err
		}
	}

	return distinct, sort, hasOrderBy, nil
}

// compileSelectLoop emits the WHERE filter, GROUP BY (if any), inner loop,
// ORDER BY sort tail, and LIMIT check for a simple SELECT.
func (c *SelectCompiler) compileSelectLoop(
	sel *Select,
	dest *SelectDest,
	sort *SortCtx,
	distinct *DistinctCtx,
	hasOrderBy bool,
	addrBreak int,
	addrContinue int,
) error {
	if sel.Where != nil {
		c.compileWhereClause(sel.Where, addrContinue)
	}

	if sel.GroupBy != nil && sel.GroupBy.Len() > 0 {
		return c.compileGroupBy(sel, dest)
	}

	c.selectInnerLoop(sel, -1, sort, distinct, dest, addrContinue, addrBreak)

	if hasOrderBy {
		// When ORDER BY is present, pass LIMIT info to sort tail so it can apply
		// LIMIT during the sorted output loop
		c.generateSortTailWithLimit(sel, sort, sel.EList.Len(), dest)
	} else if sel.Limit != 0 {
		// When no ORDER BY, apply LIMIT after the scan loop
		c.applyLimit(sel, dest, addrBreak)
	}

	return nil
}

// processFromClause processes the FROM clause and opens necessary table cursors.
func (c *SelectCompiler) processFromClause(sel *Select) error {
	if sel.Src == nil || sel.Src.Len() == 0 {
		return nil // No tables (e.g., SELECT 1+1)
	}

	vdbe := c.parse.GetVdbe()

	for i := 0; i < sel.Src.Len(); i++ {
		srcItem := sel.Src.Get(i)
		if srcItem.Table == nil {
			continue
		}

		// Get cursor number
		cursor := srcItem.Cursor
		if cursor < 0 {
			cursor = c.parse.AllocCursor()
			srcItem.Cursor = cursor
		}

		// Open table cursor
		// OP_OpenRead cursor_num, root_page
		rootPage := srcItem.Table.RootPage
		vdbe.AddOp2(OP_OpenRead, cursor, rootPage)

		// Rewind cursor to start
		// OP_Rewind cursor_num, done_label
		addrRewind := vdbe.AddOp2(OP_Rewind, cursor, 0)
		srcItem.AddrFillIndex = addrRewind // Store for patching later
	}

	return nil
}

// compileWhereClause generates code for the WHERE clause.
func (c *SelectCompiler) compileWhereClause(where *Expr, jumpIfFalse int) error {
	// Generate code to evaluate WHERE expression
	reg := c.parse.AllocReg()
	c.compileExpr(where, reg)

	// Jump if false
	vdbe := c.parse.GetVdbe()
	vdbe.AddOp3(OP_IfNot, reg, jumpIfFalse, 1)

	c.parse.ReleaseReg(reg)
	return nil
}

// selectInnerLoop generates the code for the inner loop of a SELECT.
// This extracts the result columns and processes each row.
func (c *SelectCompiler) selectInnerLoop(
	sel *Select,
	srcTab int,
	sort *SortCtx,
	distinct *DistinctCtx,
	dest *SelectDest,
	iContinue int,
	iBreak int,
) error {
	nResultCol := sel.EList.Len()

	// Allocate registers for result if needed
	regResult := dest.Sdst
	if regResult == 0 {
		regResult = c.parse.AllocRegs(nResultCol)
		dest.Sdst = regResult
		dest.NSdst = nResultCol
	}

	c.extractResultColumns(sel, srcTab, nResultCol, regResult)
	c.applyDistinctFilter(distinct, nResultCol, regResult, iContinue)
	c.applyOffsetFilter(sort, sel, iContinue)

	if err := c.disposeResult(dest, nResultCol, regResult); err != nil {
		return err
	}

	c.applyLimitFilter(sort, sel, iBreak)
	return nil
}

// extractResultColumns loads result column values into registers.
// If srcTab >= 0 the values are read from an intermediate table cursor;
// otherwise each expression in the SELECT list is evaluated directly.
func (c *SelectCompiler) extractResultColumns(sel *Select, srcTab int, nResultCol int, regResult int) {
	vdbe := c.parse.GetVdbe()
	if srcTab >= 0 {
		// Pull data from intermediate table
		for i := 0; i < nResultCol; i++ {
			vdbe.AddOp3(OP_Column, srcTab, i, regResult+i)
		}
		return
	}
	// Evaluate result expressions directly
	for i := 0; i < nResultCol; i++ {
		expr := sel.EList.Get(i).Expr
		c.compileExpr(expr, regResult+i)
	}
}

// applyDistinctFilter emits code to skip duplicate rows when DISTINCT is active.
func (c *SelectCompiler) applyDistinctFilter(distinct *DistinctCtx, nResultCol int, regResult int, iContinue int) {
	if distinct == nil || distinct.IsTnct == 0 {
		return
	}
	c.codeDistinct(distinct, nResultCol, regResult, iContinue)
}

// applyOffsetFilter emits code to skip the first OFFSET rows when there is no
// ORDER BY clause (OFFSET is handled by the sorter when ORDER BY is present).
func (c *SelectCompiler) applyOffsetFilter(sort *SortCtx, sel *Select, iContinue int) {
	if sort != nil || sel.Offset <= 0 {
		return
	}
	c.codeOffset(sel.Offset, iContinue)
}

// disposeResult emits code to deliver the current result row to dest.
func (c *SelectCompiler) disposeResult(dest *SelectDest, nResultCol int, regResult int) error {
	handler, ok := resultDisposers[dest.Dest]
	if !ok {
		return fmt.Errorf("unsupported destination type: %d", dest.Dest)
	}
	handler(c, dest, nResultCol, regResult)
	return nil
}

// resultDisposer is a function that emits VDBE code for a specific result disposition.
type resultDisposer func(*SelectCompiler, *SelectDest, int, int)

// resultDisposers maps each SelectDestType to its disposal handler.
var resultDisposers = map[SelectDestType]resultDisposer{
	SRT_Output:    disposeAsOutput,
	SRT_Mem:       disposeAsMem,
	SRT_Set:       disposeAsSet,
	SRT_Union:     disposeAsUnion,
	SRT_Except:    disposeAsExcept,
	SRT_Table:     disposeAsTable,
	SRT_EphemTab:  disposeAsEphemTab,
	SRT_Exists:    disposeAsExists,
	SRT_Coroutine: disposeAsCoroutine,
}

func disposeAsOutput(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.parse.GetVdbe().AddOp2(OP_ResultRow, regResult, nResultCol)
}

func disposeAsMem(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	if regResult != dest.SDParm {
		c.parse.GetVdbe().AddOp3(OP_Copy, regResult, dest.SDParm, nResultCol-1)
	}
}

func disposeAsSet(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.disposeResultWithRecord(dest, nResultCol, regResult, OP_IdxInsert)
}

func disposeAsUnion(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.disposeResultWithRecord(dest, nResultCol, regResult, OP_IdxInsert)
}

func disposeAsExcept(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.disposeResultWithRecord(dest, nResultCol, regResult, OP_IdxDelete)
}

func disposeAsTable(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.disposeResultAsTableInsert(dest, nResultCol, regResult)
}

func disposeAsEphemTab(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.disposeResultAsTableInsert(dest, nResultCol, regResult)
}

func disposeAsExists(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.parse.GetVdbe().AddOp2(OP_Integer, 1, dest.SDParm)
}

func disposeAsCoroutine(c *SelectCompiler, dest *SelectDest, nResultCol int, regResult int) {
	c.parse.GetVdbe().AddOp1(OP_Yield, dest.SDParm)
}

func (c *SelectCompiler) disposeResultWithRecord(dest *SelectDest, nResultCol int, regResult int, op Opcode) {
	vdbe := c.parse.GetVdbe()
	r1 := c.parse.AllocReg()
	vdbe.AddOp3(OP_MakeRecord, regResult, nResultCol, r1)
	vdbe.AddOp4Int(op, dest.SDParm, r1, regResult, nResultCol)
	c.parse.ReleaseReg(r1)
}

func (c *SelectCompiler) disposeResultAsTableInsert(dest *SelectDest, nResultCol int, regResult int) {
	vdbe := c.parse.GetVdbe()
	r1 := c.parse.AllocReg()
	r2 := c.parse.AllocReg()
	vdbe.AddOp3(OP_MakeRecord, regResult, nResultCol, r1)
	vdbe.AddOp2(OP_NewRowid, dest.SDParm, r2)
	vdbe.AddOp3(OP_Insert, dest.SDParm, r1, r2)
	c.parse.ReleaseReg(r1)
	c.parse.ReleaseReg(r2)
}

// applyLimitFilter emits code to stop iteration once the LIMIT is reached when
// there is no ORDER BY clause (LIMIT is handled by the sorter otherwise).
func (c *SelectCompiler) applyLimitFilter(sort *SortCtx, sel *Select, iBreak int) {
	if sort != nil || sel.Limit <= 0 {
		return
	}
	c.applyLimitCheck(sel.Limit, iBreak)
}

// codeDistinct generates code to enforce DISTINCT.
func (c *SelectCompiler) codeDistinct(distinct *DistinctCtx, nCol int, regResult int, jumpIfDup int) {
	vdbe := c.parse.GetVdbe()

	if distinct.ETnctType == WHERE_DISTINCT_ORDERED {
		// Use comparison with previous row
		regPrev := c.parse.AllocRegs(nCol)
		addrJump := vdbe.AddOp4Int(OP_Compare, regPrev, regResult, nCol, 0)
		vdbe.AddOp3(OP_Jump, addrJump+2, jumpIfDup, addrJump+2)
		vdbe.AddOp3(OP_Copy, regResult, regPrev, nCol-1)
	} else {
		// Use ephemeral table
		r1 := c.parse.AllocReg()
		vdbe.AddOp3(OP_MakeRecord, regResult, nCol, r1)
		vdbe.AddOp4Int(OP_Found, distinct.TabTnct, jumpIfDup, r1, 0)
		vdbe.AddOp4Int(OP_IdxInsert, distinct.TabTnct, r1, regResult, nCol)
		c.parse.ReleaseReg(r1)
	}
}

// setupDistinct initializes DISTINCT processing.
func (c *SelectCompiler) setupDistinct(sel *Select, distinct *DistinctCtx) {
	distinct.IsTnct = 1
	distinct.TabTnct = c.parse.AllocCursor()

	// Determine if we can optimize DISTINCT
	if sel.OrderBy != nil && c.canUseOrderedDistinct(sel) {
		distinct.ETnctType = WHERE_DISTINCT_ORDERED
	} else {
		distinct.ETnctType = WHERE_DISTINCT_UNORDERED

		// Open ephemeral table for DISTINCT
		vdbe := c.parse.GetVdbe()
		nCol := sel.EList.Len()
		distinct.AddrTnct = vdbe.AddOp2(OP_OpenEphemeral, distinct.TabTnct, nCol)
	}
}

// canUseOrderedDistinct checks if DISTINCT can use ORDER BY optimization.
func (c *SelectCompiler) canUseOrderedDistinct(sel *Select) bool {
	// Simple heuristic: if ORDER BY covers all result columns, can use optimization
	if sel.OrderBy == nil || sel.EList.Len() != sel.OrderBy.Len() {
		return false
	}
	return true
}

// compileExpr generates code to evaluate an expression.
// This is a simplified version - full implementation would be in expr.go
func (c *SelectCompiler) compileExpr(expr *Expr, target int) {
	vdbe := c.parse.GetVdbe()

	switch expr.Op {
	case TK_COLUMN:
		// OP_Column cursor, column_idx, reg
		vdbe.AddOp3(OP_Column, expr.Table, expr.Column, target)

	case TK_INTEGER:
		// OP_Integer value, reg
		vdbe.AddOp2(OP_Integer, expr.IntValue, target)

	case TK_STRING:
		// OP_String8 0, reg, value
		vdbe.AddOp4(OP_String8, 0, target, 0, expr.StringValue)

	case TK_NULL:
		// OP_Null 0, reg
		vdbe.AddOp2(OP_Null, 0, target)

	case TK_ASTERISK:
		// SELECT * - handled elsewhere

	default:
		// For complex expressions, would call expression compiler
		vdbe.AddOp2(OP_Null, 0, target)
	}
}

// compileCompoundSelect handles UNION, INTERSECT, EXCEPT.
func (c *SelectCompiler) compileCompoundSelect(sel *Select, dest *SelectDest) error {
	// Determine compound type
	op := sel.Op

	switch op {
	case TK_UNION, TK_UNION_ALL:
		return c.compileUnion(sel, dest)
	case TK_INTERSECT:
		return c.compileIntersect(sel, dest)
	case TK_EXCEPT:
		return c.compileExcept(sel, dest)
	default:
		return fmt.Errorf("unsupported compound operator: %d", op)
	}
}

// compileUnion compiles UNION/UNION ALL.
func (c *SelectCompiler) compileUnion(sel *Select, dest *SelectDest) error {
	vdbe := c.parse.GetVdbe()

	// Create temporary table for union
	unionTab := c.parse.AllocCursor()
	nCol := sel.EList.Len()
	vdbe.AddOp2(OP_OpenEphemeral, unionTab, nCol)

	// Compile left side into temp table
	leftDest := &SelectDest{
		Dest:   SRT_Union,
		SDParm: unionTab,
	}
	if err := c.CompileSelect(sel.Prior, leftDest); err != nil {
		return err
	}

	// Compile right side into temp table
	if err := c.compileSimpleSelect(sel, leftDest); err != nil {
		return err
	}

	// Read from temp table to destination
	addrEnd := vdbe.MakeLabel()
	vdbe.AddOp2(OP_Rewind, unionTab, addrEnd)

	addrLoop := vdbe.CurrentAddr()
	regResult := c.parse.AllocRegs(nCol)
	for i := 0; i < nCol; i++ {
		vdbe.AddOp3(OP_Column, unionTab, i, regResult+i)
	}
	vdbe.AddOp2(OP_ResultRow, regResult, nCol)
	vdbe.AddOp2(OP_Next, unionTab, addrLoop)

	vdbe.ResolveLabel(addrEnd)
	vdbe.AddOp1(OP_Close, unionTab)

	return nil
}

// compileIntersect compiles INTERSECT.
// INTERSECT returns only rows that appear in BOTH the left and right queries.
// Algorithm:
// 1. Execute left query and store results in ephemeral table A
// 2. Execute right query and for each row:
//    - Check if it exists in table A
//    - If yes, insert into result table B
// 3. Read from result table B and output
func (c *SelectCompiler) compileIntersect(sel *Select, dest *SelectDest) error {
	vdbe := c.parse.GetVdbe()
	nCol := sel.EList.Len()

	// Create ephemeral table for left query results
	leftTab := c.parse.AllocCursor()
	vdbe.AddOp2(OP_OpenEphemeral, leftTab, nCol)

	// Create ephemeral table for final results
	resultTab := c.parse.AllocCursor()
	vdbe.AddOp2(OP_OpenEphemeral, resultTab, nCol)

	// Compile left side into leftTab
	leftDest := &SelectDest{
		Dest:   SRT_Union,
		SDParm: leftTab,
	}
	if err := c.CompileSelect(sel.Prior, leftDest); err != nil {
		return err
	}

	// For right side, we need to check each row against leftTab
	// and only insert into resultTab if it exists in leftTab
	// We compile the right query and for each row:
	// 1. Check if row exists in leftTab (using OP_Found)
	// 2. If found, insert into resultTab

	// We need to handle this differently - compile right side with custom logic
	// For now, use a temporary approach: compile to a temp table, then intersect

	rightTab := c.parse.AllocCursor()
	vdbe.AddOp2(OP_OpenEphemeral, rightTab, nCol)

	rightDest := &SelectDest{
		Dest:   SRT_Union,
		SDParm: rightTab,
	}
	if err := c.compileSimpleSelect(sel, rightDest); err != nil {
		return err
	}

	// Now iterate through rightTab and check each row against leftTab
	addrEnd := vdbe.MakeLabel()
	vdbe.AddOp2(OP_Rewind, rightTab, addrEnd)

	addrLoop := vdbe.CurrentAddr()
	regResult := c.parse.AllocRegs(nCol)

	// Extract row from rightTab
	for i := 0; i < nCol; i++ {
		vdbe.AddOp3(OP_Column, rightTab, i, regResult+i)
	}

	// Make record for lookup
	regRecord := c.parse.AllocReg()
	vdbe.AddOp3(OP_MakeRecord, regResult, nCol, regRecord)

	// Check if this row exists in leftTab
	addrNotFound := vdbe.MakeLabel()
	vdbe.AddOp4Int(OP_NotFound, leftTab, addrNotFound, regRecord, 0)

	// Row exists in both - insert into resultTab
	vdbe.AddOp4Int(OP_IdxInsert, resultTab, regRecord, regResult, nCol)

	// Continue to next row
	vdbe.ResolveLabel(addrNotFound)
	vdbe.AddOp2(OP_Next, rightTab, addrLoop)

	vdbe.ResolveLabel(addrEnd)

	// Close temporary tables
	vdbe.AddOp1(OP_Close, leftTab)
	vdbe.AddOp1(OP_Close, rightTab)

	// Now output results from resultTab
	addrOutputEnd := vdbe.MakeLabel()
	vdbe.AddOp2(OP_Rewind, resultTab, addrOutputEnd)

	addrOutputLoop := vdbe.CurrentAddr()
	regOutput := c.parse.AllocRegs(nCol)
	for i := 0; i < nCol; i++ {
		vdbe.AddOp3(OP_Column, resultTab, i, regOutput+i)
	}

	// Deliver result based on destination
	if dest.Dest == SRT_Output {
		vdbe.AddOp2(OP_ResultRow, regOutput, nCol)
	} else {
		// For other destinations, use disposeResult
		tempDest := &SelectDest{
			Dest:  dest.Dest,
			SDParm: dest.SDParm,
			SDParm2: dest.SDParm2,
			Sdst:  regOutput,
			NSdst: nCol,
		}
		if err := c.disposeResult(tempDest, nCol, regOutput); err != nil {
			return err
		}
	}

	vdbe.AddOp2(OP_Next, resultTab, addrOutputLoop)

	vdbe.ResolveLabel(addrOutputEnd)
	vdbe.AddOp1(OP_Close, resultTab)

	return nil
}

// compileExcept compiles EXCEPT.
// EXCEPT returns rows from the left query that do NOT appear in the right query.
// Algorithm:
// 1. Execute left query and store results in ephemeral table A
// 2. Execute right query and for each row:
//    - Delete matching row from table A (using SRT_Except)
// 3. Read remaining rows from table A and output
func (c *SelectCompiler) compileExcept(sel *Select, dest *SelectDest) error {
	vdbe := c.parse.GetVdbe()
	nCol := sel.EList.Len()

	// Create ephemeral table for left query results
	exceptTab := c.parse.AllocCursor()
	vdbe.AddOp2(OP_OpenEphemeral, exceptTab, nCol)

	// Compile left side into exceptTab
	leftDest := &SelectDest{
		Dest:   SRT_Union,
		SDParm: exceptTab,
	}
	if err := c.CompileSelect(sel.Prior, leftDest); err != nil {
		return err
	}

	// Compile right side with SRT_Except to remove matching rows
	rightDest := &SelectDest{
		Dest:   SRT_Except,
		SDParm: exceptTab,
	}
	if err := c.compileSimpleSelect(sel, rightDest); err != nil {
		return err
	}

	// Now output remaining rows from exceptTab
	addrEnd := vdbe.MakeLabel()
	vdbe.AddOp2(OP_Rewind, exceptTab, addrEnd)

	addrLoop := vdbe.CurrentAddr()
	regResult := c.parse.AllocRegs(nCol)

	// Extract row from exceptTab
	for i := 0; i < nCol; i++ {
		vdbe.AddOp3(OP_Column, exceptTab, i, regResult+i)
	}

	// Deliver result based on destination
	if dest.Dest == SRT_Output {
		vdbe.AddOp2(OP_ResultRow, regResult, nCol)
	} else {
		// For other destinations, use disposeResult
		tempDest := &SelectDest{
			Dest:    dest.Dest,
			SDParm:  dest.SDParm,
			SDParm2: dest.SDParm2,
			Sdst:    regResult,
			NSdst:   nCol,
		}
		if err := c.disposeResult(tempDest, nCol, regResult); err != nil {
			return err
		}
	}

	vdbe.AddOp2(OP_Next, exceptTab, addrLoop)

	vdbe.ResolveLabel(addrEnd)
	vdbe.AddOp1(OP_Close, exceptTab)

	return nil
}

// generateSortTailWithLimit generates code to extract sorted results with LIMIT applied.
// This delegates to the OrderByCompiler with LIMIT information.
func (c *SelectCompiler) generateSortTailWithLimit(sel *Select, sort *SortCtx, nColumn int, dest *SelectDest) error {
	obc := NewOrderByCompiler(c.parse)
	return obc.generateSortTailWithLimit(sel, sort, nColumn, dest)
}

// codeOffset generates code to skip OFFSET rows.
func (c *SelectCompiler) codeOffset(offset int, jumpTo int) {
	vdbe := c.parse.GetVdbe()

	// Check if we've skipped enough rows yet
	regOffset := c.parse.AllocReg()
	vdbe.AddOp2(OP_Integer, offset, regOffset)
	vdbe.AddOp3(OP_IfPos, regOffset, jumpTo, -1)
	c.parse.ReleaseReg(regOffset)
}

// applyLimitCheck generates code to check LIMIT.
func (c *SelectCompiler) applyLimitCheck(limit int, jumpTo int) {
	vdbe := c.parse.GetVdbe()

	regLimit := c.parse.AllocReg()
	vdbe.AddOp2(OP_Integer, limit, regLimit)
	vdbe.AddOp3(OP_IfNot, regLimit, jumpTo, -1)
	c.parse.ReleaseReg(regLimit)
}
