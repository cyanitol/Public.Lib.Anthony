// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"fmt"
)

// WhereLoopBuilder is responsible for generating WhereLoop objects
// for all possible access paths for a given table.
type WhereLoopBuilder struct {
	// Table being analyzed
	Table *TableInfo

	// WHERE clause terms that apply to this table
	Terms []*WhereTerm

	// Cost model for estimation
	CostModel *CostModel

	// Generated loops
	Loops []*WhereLoop

	// Cursor is the cursor number for this table
	Cursor int

	// NotReady is bitmask of tables not yet processed
	NotReady Bitmask
}

// NewWhereLoopBuilder creates a new builder for a table.
func NewWhereLoopBuilder(table *TableInfo, cursor int, terms []*WhereTerm, costModel *CostModel) *WhereLoopBuilder {
	return &WhereLoopBuilder{
		Table:     table,
		Cursor:    cursor,
		Terms:     terms,
		CostModel: costModel,
		Loops:     make([]*WhereLoop, 0),
	}
}

// Build generates all possible WhereLoop objects for this table.
func (b *WhereLoopBuilder) Build() []*WhereLoop {
	// Always generate a full table scan option
	b.addFullScan()

	// Generate index scan options for each index
	for _, index := range b.Table.Indexes {
		b.addIndexScans(index)
	}

	// Generate primary key lookup if applicable
	if b.Table.PrimaryKey != nil {
		b.addPrimaryKeyLookup()
	}

	return b.Loops
}

// addFullScan adds a full table scan WhereLoop.
func (b *WhereLoopBuilder) addFullScan() {
	cost, nOut := b.CostModel.EstimateFullScan(b.Table)

	loop := &WhereLoop{
		TabIndex: b.Cursor,
		Setup:    0,
		Run:      cost,
		NOut:     nOut,
		Flags:    0, // No special flags for full scan
		Index:    nil,
		Terms:    make([]*WhereTerm, 0),
	}

	// Set bitmask for this table
	loop.MaskSelf.Set(b.Cursor)

	// Apply all WHERE terms that reference only this table
	b.applyTermsToLoop(loop)

	b.Loops = append(b.Loops, loop)
}

// addIndexScans adds WhereLoop objects for all possible ways to use an index.
func (b *WhereLoopBuilder) addIndexScans(index *IndexInfo) {
	// Skip partial indexes whose WHERE clause is not implied by the query
	if index.Partial && !b.partialIndexUsable(index) {
		return
	}

	// Try using increasing numbers of index columns
	for nCol := 1; nCol <= len(index.Columns); nCol++ {
		b.addIndexScanWithColumns(index, nCol)
	}
}

// partialIndexUsable checks whether a partial index can be used with the
// current query. A partial index is usable when the query's WHERE clause
// implies the index's WHERE clause. As a practical heuristic, we check
// whether any query WHERE term references a column that appears in the
// index's WHERE clause text. If no query terms reference this table at all,
// the partial index cannot be relied upon.
func (b *WhereLoopBuilder) partialIndexUsable(index *IndexInfo) bool {
	if index.WhereClause == "" {
		return false
	}
	for _, term := range b.Terms {
		if term.LeftCursor == b.Cursor {
			return true
		}
	}
	return false
}

type termConstraints struct {
	nEq      int
	hasRange bool
}

func analyzeTermConstraints(terms []*WhereTerm) termConstraints {
	c := termConstraints{}
	for _, term := range terms {
		if term.Operator == WO_EQ {
			c.nEq++
		} else if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
			c.hasRange = true
		}
	}
	return c
}

func computeIndexFlags(c termConstraints, terms []*WhereTerm, index *IndexInfo, covering bool) WhereFlags {
	flags := WHERE_INDEXED
	flags = addEqualityFlags(c, flags)
	flags = addRangeFlags(c, terms, flags)
	flags = addCoveringFlag(covering, flags)
	flags = addUniqueFlag(index, c, flags)
	return flags
}

// addEqualityFlags adds flags for equality constraints.
func addEqualityFlags(c termConstraints, flags WhereFlags) WhereFlags {
	if c.nEq > 0 {
		return flags | WHERE_COLUMN_EQ
	}
	return flags
}

// addRangeFlags adds flags for range constraints.
func addRangeFlags(c termConstraints, terms []*WhereTerm, flags WhereFlags) WhereFlags {
	if !c.hasRange {
		return flags
	}
	flags |= WHERE_COLUMN_RANGE
	if hasLowerBound(terms) {
		flags |= WHERE_BTM_LIMIT
	}
	if hasUpperBound(terms) {
		flags |= WHERE_TOP_LIMIT
	}
	return flags
}

// addCoveringFlag adds flag for covering index.
func addCoveringFlag(covering bool, flags WhereFlags) WhereFlags {
	if covering {
		return flags | WHERE_IDX_ONLY
	}
	return flags
}

// addUniqueFlag adds flag for unique constraint satisfaction.
func addUniqueFlag(index *IndexInfo, c termConstraints, flags WhereFlags) WhereFlags {
	if index.Unique && c.nEq >= len(index.Columns) {
		return flags | WHERE_ONEROW
	}
	return flags
}

// addIndexScanWithColumns adds loops for using nCol columns of an index.
func (b *WhereLoopBuilder) addIndexScanWithColumns(index *IndexInfo, nCol int) {
	usableTerms := b.findUsableTerms(index, nCol)
	if len(usableTerms) == 0 {
		return
	}

	c := analyzeTermConstraints(usableTerms)
	covering := false
	cost, nOut := b.CostModel.EstimateIndexScan(b.Table, index, usableTerms, c.nEq, c.hasRange, covering)
	flags := computeIndexFlags(c, usableTerms, index, covering)

	loop := &WhereLoop{
		TabIndex: b.Cursor,
		Setup:    0,
		Run:      cost,
		NOut:     nOut,
		Flags:    flags,
		Index:    index,
		Terms:    usableTerms,
	}

	loop.MaskSelf.Set(b.Cursor)
	b.setPrerequisites(loop)
	b.Loops = append(b.Loops, loop)

	b.tryInOperator(index, nCol, usableTerms)
}

// addPrimaryKeyLookup adds a WhereLoop for direct rowid/primary key lookup.
func (b *WhereLoopBuilder) addPrimaryKeyLookup() {
	// Find term that constrains the primary key
	var pkTerm *WhereTerm
	for _, term := range b.Terms {
		if term.LeftCursor == b.Cursor && term.LeftColumn == -1 { // -1 = rowid
			if term.Operator == WO_EQ {
				pkTerm = term
				break
			}
		}
	}

	if pkTerm == nil {
		return // No primary key constraint
	}

	cost, nOut := b.CostModel.EstimateRowidLookup()

	loop := &WhereLoop{
		TabIndex: b.Cursor,
		Setup:    0,
		Run:      cost,
		NOut:     nOut,
		Flags:    WHERE_IPK | WHERE_COLUMN_EQ | WHERE_ONEROW,
		Index:    b.Table.PrimaryKey,
		Terms:    []*WhereTerm{pkTerm},
	}

	loop.MaskSelf.Set(b.Cursor)
	b.setPrerequisites(loop)

	b.Loops = append(b.Loops, loop)
}

// findUsableTerms finds WHERE terms that can use the first nCol columns of an index.
func (b *WhereLoopBuilder) findUsableTerms(index *IndexInfo, nCol int) []*WhereTerm {
	usable := make([]*WhereTerm, 0)

	for i := 0; i < nCol && i < len(index.Columns); i++ {
		colIdx := index.Columns[i].Index
		termForCol := b.findTermForIndexColumn(colIdx)
		if termForCol == nil {
			break
		}
		usable = append(usable, termForCol)
		if termForCol.Operator != WO_EQ {
			break
		}
	}

	return usable
}

// findTermForIndexColumn finds a usable term for a specific column index.
func (b *WhereLoopBuilder) findTermForIndexColumn(colIdx int) *WhereTerm {
	for _, term := range b.Terms {
		if term.LeftCursor == b.Cursor && term.LeftColumn == colIdx && isUsableOperator(term.Operator) {
			return term
		}
	}
	return nil
}

// isUsableOperator checks if an operator can be used with an index.
func isUsableOperator(op WhereOperator) bool {
	return op&(WO_EQ|WO_LT|WO_LE|WO_GT|WO_GE|WO_IN|WO_ISNULL) != 0
}

// hasLowerBound checks if terms include a lower bound (> or >=).
func hasLowerBound(terms []*WhereTerm) bool {
	for _, term := range terms {
		if term.Operator&(WO_GT|WO_GE) != 0 {
			return true
		}
	}
	return false
}

// hasUpperBound checks if terms include an upper bound (< or <=).
func hasUpperBound(terms []*WhereTerm) bool {
	for _, term := range terms {
		if term.Operator&(WO_LT|WO_LE) != 0 {
			return true
		}
	}
	return false
}

func (b *WhereLoopBuilder) findInTermForColumn(colIdx int) *WhereTerm {
	for _, term := range b.Terms {
		if term.LeftCursor == b.Cursor && term.LeftColumn == colIdx && term.Operator == WO_IN {
			return term
		}
	}
	return nil
}

func computeInOperatorFlags(nEq int, covering bool) WhereFlags {
	flags := WHERE_INDEXED | WHERE_COLUMN_IN | WHERE_IN_ABLE
	if nEq > 0 {
		flags |= WHERE_COLUMN_EQ
	}
	if covering {
		flags |= WHERE_IDX_ONLY
	}
	return flags
}

func (b *WhereLoopBuilder) buildInOperatorLoop(index *IndexInfo, baseTerms []*WhereTerm, inTerm *WhereTerm, nEq int, covering bool) {
	const inListSize = 5
	cost, nOut := b.CostModel.EstimateInOperator(b.Table, index, nEq, inListSize, covering)

	terms := make([]*WhereTerm, len(baseTerms)+1)
	copy(terms, baseTerms)
	terms[len(baseTerms)] = inTerm

	loop := &WhereLoop{
		TabIndex: b.Cursor,
		Setup:    0,
		Run:      cost,
		NOut:     nOut,
		Flags:    computeInOperatorFlags(nEq, covering),
		Index:    index,
		Terms:    terms,
	}
	loop.MaskSelf.Set(b.Cursor)
	b.setPrerequisites(loop)
	b.Loops = append(b.Loops, loop)
}

// tryInOperator attempts to optimize using IN operator with an index.
func (b *WhereLoopBuilder) tryInOperator(index *IndexInfo, nCol int, baseTerms []*WhereTerm) {
	if nCol >= len(index.Columns) {
		return
	}

	inTerm := b.findInTermForColumn(index.Columns[nCol].Index)
	if inTerm == nil {
		return
	}

	nEq := countEqualityTerms(baseTerms)
	covering := false
	b.buildInOperatorLoop(index, baseTerms, inTerm, nEq, covering)
}

// applyTermsToLoop applies WHERE terms to refine cost of a full table scan.
func (b *WhereLoopBuilder) applyTermsToLoop(loop *WhereLoop) {
	selectivity := LogEst(0)

	for _, term := range b.Terms {
		// Check if term applies to this table only
		if term.LeftCursor == b.Cursor && term.PrereqRight == 0 {
			// Apply truth probability to reduce estimated rows
			prob := b.CostModel.EstimateTruthProbability(term)
			selectivity += prob

			// Add term to loop
			loop.Terms = append(loop.Terms, term)
		}
	}

	// Adjust output rows based on selectivity
	loop.NOut += selectivity
	if loop.NOut < 0 {
		loop.NOut = 0 // At least 1 row
	}
}

// setPrerequisites determines which tables must be evaluated before this loop.
func (b *WhereLoopBuilder) setPrerequisites(loop *WhereLoop) {
	prereq := Bitmask(0)

	for _, term := range loop.Terms {
		// If term's right side references other tables, they're prerequisites
		prereq |= term.PrereqRight
	}

	// Remove this table itself from prerequisites
	prereq &= ^loop.MaskSelf

	loop.Prereq = prereq
}

func (b *WhereLoopBuilder) columnConstrained(colIdx int) bool {
	for _, term := range b.Terms {
		if term.LeftCursor == b.Cursor && term.LeftColumn == colIdx {
			return true
		}
	}
	return false
}

func (b *WhereLoopBuilder) laterColumnTerms(index *IndexInfo) []*WhereTerm {
	terms := make([]*WhereTerm, 0)
	for i := 1; i < len(index.Columns); i++ {
		colIdx := index.Columns[i].Index
		for _, term := range b.Terms {
			if term.LeftCursor == b.Cursor && term.LeftColumn == colIdx {
				terms = append(terms, term)
				break
			}
		}
	}
	return terms
}

func countEqualityTerms(terms []*WhereTerm) int {
	n := 0
	for _, term := range terms {
		if term.Operator == WO_EQ {
			n++
		}
	}
	return n
}

func (b *WhereLoopBuilder) skipScanCostViable(index *IndexInfo, laterTerms []*WhereTerm) (cost, nOut LogEst, ok bool) {
	distinctFirst := LogEst(40)
	nEq := countEqualityTerms(laterTerms)
	baseCost, baseNOut := b.CostModel.EstimateIndexScan(b.Table, index, laterTerms, nEq, false, false)
	cost = distinctFirst.Add(baseCost)
	nOut = distinctFirst.Add(baseNOut)
	fullScanCost, _ := b.CostModel.EstimateFullScan(b.Table)
	return cost, nOut, cost < fullScanCost
}

func (b *WhereLoopBuilder) OptimizeForSkipScan(index *IndexInfo) *WhereLoop {
	if len(index.Columns) < 2 {
		return nil
	}
	if b.columnConstrained(index.Columns[0].Index) {
		return nil
	}
	laterTerms := b.laterColumnTerms(index)
	if len(laterTerms) == 0 {
		return nil
	}
	cost, nOut, ok := b.skipScanCostViable(index, laterTerms)
	if !ok {
		return nil
	}
	loop := &WhereLoop{
		TabIndex: b.Cursor,
		Setup:    0,
		Run:      cost,
		NOut:     nOut,
		Flags:    WHERE_INDEXED | WHERE_SKIPSCAN,
		Index:    index,
		Terms:    laterTerms,
	}
	loop.MaskSelf.Set(b.Cursor)
	b.setPrerequisites(loop)
	return loop
}

// String returns a string representation of a WhereLoop for debugging.
func (loop *WhereLoop) String() string {
	s := fmt.Sprintf("WhereLoop[tab=%d", loop.TabIndex)

	if loop.Index != nil {
		s += fmt.Sprintf(" index=%s", loop.Index.Name)
	} else {
		s += " scan=FULL"
	}

	s += fmt.Sprintf(" cost=%d nOut=%d", loop.Run, loop.NOut)
	s += formatWhereLoopFlags(loop.Flags)
	s += "]"
	return s
}

// formatWhereLoopFlags formats WhereFlags for display.
func formatWhereLoopFlags(flags WhereFlags) string {
	var s string
	if flags&WHERE_ONEROW != 0 {
		s += " ONEROW"
	}
	if flags&WHERE_COLUMN_EQ != 0 {
		s += " EQ"
	}
	if flags&WHERE_COLUMN_RANGE != 0 {
		s += " RANGE"
	}
	if flags&WHERE_COLUMN_IN != 0 {
		s += " IN"
	}
	if flags&WHERE_IDX_ONLY != 0 {
		s += " COVERING"
	}
	if flags&WHERE_SKIPSCAN != 0 {
		s += " SKIPSCAN"
	}
	return s
}

// Clone creates a deep copy of a WhereLoop.
func (loop *WhereLoop) Clone() *WhereLoop {
	clone := &WhereLoop{
		Prereq:   loop.Prereq,
		MaskSelf: loop.MaskSelf,
		TabIndex: loop.TabIndex,
		Setup:    loop.Setup,
		Run:      loop.Run,
		NOut:     loop.NOut,
		Flags:    loop.Flags,
		Index:    loop.Index,
		NextLoop: nil, // Don't copy the linked list pointer
	}

	// Copy terms
	clone.Terms = make([]*WhereTerm, len(loop.Terms))
	copy(clone.Terms, loop.Terms)

	return clone
}
