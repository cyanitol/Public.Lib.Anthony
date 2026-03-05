// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"fmt"
	"strings"
)

// IndexSelector is responsible for selecting the best index for a query.
type IndexSelector struct {
	Table     *TableInfo
	Terms     []*WhereTerm
	CostModel *CostModel
}

// NewIndexSelector creates a new index selector.
func NewIndexSelector(table *TableInfo, terms []*WhereTerm, costModel *CostModel) *IndexSelector {
	return &IndexSelector{
		Table:     table,
		Terms:     terms,
		CostModel: costModel,
	}
}

// SelectBestIndex chooses the best index for the given WHERE terms.
// Returns nil if no index is beneficial (should use full table scan).
func (s *IndexSelector) SelectBestIndex() *IndexInfo {
	if len(s.Table.Indexes) == 0 {
		return nil
	}

	var bestIndex *IndexInfo
	var bestScore float64 = -1

	for _, index := range s.Table.Indexes {
		score := s.scoreIndex(index)
		if score > bestScore {
			bestScore = score
			bestIndex = index
		}
	}

	// Only return index if it's actually beneficial
	if bestScore > 0 {
		return bestIndex
	}

	return nil
}

// scoreIndex calculates a score for how well an index matches the WHERE terms.
// Higher scores are better.
func (s *IndexSelector) scoreIndex(index *IndexInfo) float64 {
	score := 0.0

	// Find which WHERE terms can use this index
	usableTerms := s.findUsableTermsForIndex(index)

	// Score based on number of usable terms
	score += float64(len(usableTerms)) * 10

	// Bonus for equality constraints (more selective)
	for _, term := range usableTerms {
		if term.Operator == WO_EQ {
			score += 5
		} else if term.Operator == WO_IN {
			score += 3
		} else if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
			score += 1
		}
	}

	// Bonus for unique index
	if index.Unique {
		score += 20
	}

	// Bonus for primary key
	if index.Primary {
		score += 15
	}

	// Penalty for wide indexes (more I/O)
	score -= float64(len(index.Columns)) * 0.5

	return score
}

// findUsableTermsForIndex finds all WHERE terms that can benefit from an index.
func (s *IndexSelector) findUsableTermsForIndex(index *IndexInfo) []*WhereTerm {
	usable := make([]*WhereTerm, 0)

	// Check each index column in order
	for i, col := range index.Columns {
		found := false

		for _, term := range s.Terms {
			if s.termMatchesColumn(term, col) {
				usable = append(usable, term)
				found = true
				break
			}
		}

		// If no term for this column and it's not the first, we can't use later columns
		if !found && i > 0 {
			break
		}
	}

	return usable
}

// termMatchesColumn checks if a WHERE term can use a specific index column.
func (s *IndexSelector) termMatchesColumn(term *WhereTerm, col IndexColumn) bool {
	// Term must reference this column
	if term.LeftColumn != col.Index {
		return false
	}

	// Must be a usable operator
	return isUsableOperator(term.Operator)
}

// AnalyzeIndexUsage analyzes how an index would be used for given terms.
type IndexUsage struct {
	Index      *IndexInfo
	EqTerms    []*WhereTerm  // Equality constraints
	RangeTerms []*WhereTerm  // Range constraints (< > <= >=)
	InTerms    []*WhereTerm  // IN constraints
	StartKey   []interface{} // Start key for index seek
	EndKey     []interface{} // End key for index seek
	Covering   bool          // Whether index covers all needed columns
}

// AnalyzeIndexUsage determines how an index would be used.
func (s *IndexSelector) AnalyzeIndexUsage(index *IndexInfo, neededColumns []string) *IndexUsage {
	usage := s.createEmptyIndexUsage(index)

	if !s.processIndexColumns(usage, index) {
		return usage
	}

	usage.Covering = s.checkCovering(index, neededColumns)
	return usage
}

// createEmptyIndexUsage creates an empty IndexUsage structure.
func (s *IndexSelector) createEmptyIndexUsage(index *IndexInfo) *IndexUsage {
	return &IndexUsage{
		Index:      index,
		EqTerms:    make([]*WhereTerm, 0),
		RangeTerms: make([]*WhereTerm, 0),
		InTerms:    make([]*WhereTerm, 0),
		StartKey:   make([]interface{}, 0),
		EndKey:     make([]interface{}, 0),
	}
}

// processIndexColumns processes index columns and applies terms to usage.
// Returns false if first column has no constraint, true otherwise.
func (s *IndexSelector) processIndexColumns(usage *IndexUsage, index *IndexInfo) bool {
	for i, col := range index.Columns {
		term := s.findTermForColumn(col.Index)
		if term == nil {
			return i > 0 // First column must have constraint
		}
		if done := s.applyTermToUsage(term, usage); done {
			break
		}
	}
	return true
}

// applyTermToUsage applies a term to the usage and returns true if no more columns should be processed.
func (s *IndexSelector) applyTermToUsage(term *WhereTerm, usage *IndexUsage) bool {
	switch {
	case term.Operator == WO_EQ:
		usage.EqTerms = append(usage.EqTerms, term)
		usage.StartKey = append(usage.StartKey, term.RightValue)
		usage.EndKey = append(usage.EndKey, term.RightValue)
		return false
	case term.Operator == WO_IN:
		usage.InTerms = append(usage.InTerms, term)
		return true
	case term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0:
		s.applyRangeTerm(term, usage)
		return true
	}
	return false
}

// applyRangeTerm applies a range term to the usage.
func (s *IndexSelector) applyRangeTerm(term *WhereTerm, usage *IndexUsage) {
	usage.RangeTerms = append(usage.RangeTerms, term)
	if term.Operator&(WO_GT|WO_GE) != 0 {
		usage.StartKey = append(usage.StartKey, term.RightValue)
	}
	if term.Operator&(WO_LT|WO_LE) != 0 {
		usage.EndKey = append(usage.EndKey, term.RightValue)
	}
}

// findTermForColumn finds a WHERE term that constrains a specific column.
func (s *IndexSelector) findTermForColumn(colIdx int) *WhereTerm {
	for _, term := range s.Terms {
		if term.LeftColumn == colIdx && isUsableOperator(term.Operator) {
			return term
		}
	}
	return nil
}

// checkCovering checks if an index covers all needed columns.
func (s *IndexSelector) checkCovering(index *IndexInfo, neededColumns []string) bool {
	indexCols := make(map[string]bool)
	for _, col := range index.Columns {
		indexCols[col.Name] = true
	}

	for _, col := range neededColumns {
		if !indexCols[col] {
			return false
		}
	}

	return true
}

// ExplainIndexUsage returns a human-readable explanation of index usage.
func (usage *IndexUsage) Explain() string {
	if usage.Index == nil {
		return "FULL TABLE SCAN"
	}

	parts := make([]string, 0)
	parts = append(parts, fmt.Sprintf("INDEX %s", usage.Index.Name))

	// Explain constraints
	constraints := make([]string, 0)

	for _, term := range usage.EqTerms {
		col := usage.Index.Columns[term.LeftColumn].Name
		constraints = append(constraints, fmt.Sprintf("%s=?", col))
	}

	for _, term := range usage.InTerms {
		col := usage.Index.Columns[term.LeftColumn].Name
		constraints = append(constraints, fmt.Sprintf("%s IN (?)", col))
	}

	for _, term := range usage.RangeTerms {
		col := usage.Index.Columns[term.LeftColumn].Name
		op := operatorString(term.Operator)
		constraints = append(constraints, fmt.Sprintf("%s%s?", col, op))
	}

	if len(constraints) > 0 {
		parts = append(parts, "("+strings.Join(constraints, " AND ")+")")
	}

	if usage.Covering {
		parts = append(parts, "COVERING")
	}

	return strings.Join(parts, " ")
}

// operatorStringMap maps operators to their string representations.
var operatorStringMap = map[WhereOperator]string{
	WO_EQ:     "=",
	WO_LT:     "<",
	WO_LE:     "<=",
	WO_GT:     ">",
	WO_GE:     ">=",
	WO_IN:     " IN ",
	WO_IS:     " IS ",
	WO_ISNULL: " IS NULL",
}

// operatorString converts an operator to its string representation.
func operatorString(op WhereOperator) string {
	if s, ok := operatorStringMap[op]; ok {
		return s
	}
	return "?"
}

// BuildIndex creates statistics for a new index.
// This would typically be called when analyzing CREATE INDEX statements.
func BuildIndexStats(table *TableInfo, columns []string, unique bool) *IndexInfo {
	index := &IndexInfo{
		Name:        fmt.Sprintf("idx_%s_%s", table.Name, strings.Join(columns, "_")),
		Table:       table.Name,
		Columns:     make([]IndexColumn, len(columns)),
		Unique:      unique,
		Primary:     false,
		RowCount:    table.RowCount,
		RowLogEst:   table.RowLogEst,
		ColumnStats: make([]LogEst, len(columns)),
	}

	// Build column info
	for i, colName := range columns {
		// Find column in table
		colIdx := -1
		for j, tableCol := range table.Columns {
			if tableCol.Name == colName {
				colIdx = j
				break
			}
		}

		index.Columns[i] = IndexColumn{
			Name:      colName,
			Index:     colIdx,
			Ascending: true,
			Collation: "BINARY",
		}

		// Estimate statistics for this column prefix
		// Each additional column reduces selectivity
		// Simple heuristic: divide by 10 for each column
		index.ColumnStats[i] = table.RowLogEst - LogEst((i+1)*33) // 33 ~= 10*log2(10)
		if index.ColumnStats[i] < 0 {
			index.ColumnStats[i] = 0
		}
	}

	// If unique index, last stat should be 0 (1 row)
	if unique && len(index.ColumnStats) > 0 {
		index.ColumnStats[len(index.ColumnStats)-1] = 0
	}

	return index
}

// CompareIndexes compares two indexes for a given set of WHERE terms.
// Returns -1 if idx1 is better, 1 if idx2 is better, 0 if equal.
func CompareIndexes(idx1, idx2 *IndexInfo, terms []*WhereTerm, costModel *CostModel) int {
	selector := &IndexSelector{
		Terms:     terms,
		CostModel: costModel,
	}

	score1 := selector.scoreIndex(idx1)
	score2 := selector.scoreIndex(idx2)

	if score1 > score2 {
		return -1
	} else if score1 < score2 {
		return 1
	}
	return 0
}

// OptimizeIndexSelection performs advanced index selection considering multiple factors.
type OptimizeOptions struct {
	// PreferCovering gives bonus to covering indexes
	PreferCovering bool

	// PreferUnique gives bonus to unique indexes
	PreferUnique bool

	// ConsiderOrderBy includes ORDER BY optimization
	ConsiderOrderBy bool

	// OrderBy columns if ConsiderOrderBy is true
	OrderBy []OrderByColumn
}

// indexScore holds a scored candidate index along with its estimated cost.
type indexScore struct {
	index *IndexInfo
	score float64
	cost  LogEst
	nOut  LogEst
}

// applyOptionsBonus adds score bonuses dictated by OptimizeOptions.
func (s *IndexSelector) applyOptionsBonus(index *IndexInfo, opts OptimizeOptions, score float64) float64 {
	score = applyCoveringBonus(index, opts, score)
	score = applyUniqueBonus(index, opts, score)
	score = applyOrderByBonus(s, index, opts, score)
	return score
}

// applyCoveringBonus adds bonus for covering indexes.
func applyCoveringBonus(index *IndexInfo, opts OptimizeOptions, score float64) float64 {
	if opts.PreferCovering && len(index.Columns) > 3 {
		return score + 10
	}
	return score
}

// applyUniqueBonus adds bonus for unique indexes.
func applyUniqueBonus(index *IndexInfo, opts OptimizeOptions, score float64) float64 {
	if opts.PreferUnique && index.Unique {
		return score + 15
	}
	return score
}

// applyOrderByBonus adds bonus for order-by matching indexes.
func applyOrderByBonus(s *IndexSelector, index *IndexInfo, opts OptimizeOptions, score float64) float64 {
	if opts.ConsiderOrderBy && len(opts.OrderBy) > 0 && s.indexMatchesOrderBy(index, opts.OrderBy) {
		return score + 25
	}
	return score
}

// analyzeTermCounts counts equality constraints and detects range constraints
// among the usable terms for an index.
func analyzeTermCounts(usableTerms []*WhereTerm) (nEq int, hasRange bool) {
	for _, term := range usableTerms {
		if term.Operator == WO_EQ {
			nEq++
		} else if term.Operator&(WO_LT|WO_LE|WO_GT|WO_GE) != 0 {
			hasRange = true
		}
	}
	return nEq, hasRange
}

// scoreIndexEntry builds a complete indexScore for one index, incorporating
// option bonuses and the cost model estimate.
func (s *IndexSelector) scoreIndexEntry(index *IndexInfo, opts OptimizeOptions) indexScore {
	score := s.applyOptionsBonus(index, opts, s.scoreIndex(index))

	usableTerms := s.findUsableTermsForIndex(index)
	nEq, hasRange := analyzeTermCounts(usableTerms)
	cost, nOut := s.CostModel.EstimateIndexScan(s.Table, index, usableTerms, nEq, hasRange, false)

	return indexScore{index: index, score: score, cost: cost, nOut: nOut}
}

// pickBestScore returns the highest-scoring entry from scores, preferring lower
// cost when scores are equal.
func pickBestScore(scores []indexScore) indexScore {
	best := scores[0]
	for _, candidate := range scores[1:] {
		if candidate.score > best.score || (candidate.score == best.score && candidate.cost < best.cost) {
			best = candidate
		}
	}
	return best
}

// SelectBestIndexWithOptions selects the best index with advanced options.
func (s *IndexSelector) SelectBestIndexWithOptions(opts OptimizeOptions) *IndexInfo {
	if len(s.Table.Indexes) == 0 {
		return nil
	}

	best := s.findBestIndexScore(opts)
	if best.score > 0 {
		return best.index
	}
	return nil
}

// findBestIndexScore finds the best index score from all available indexes.
func (s *IndexSelector) findBestIndexScore(opts OptimizeOptions) indexScore {
	scores := make([]indexScore, 0, len(s.Table.Indexes))
	for _, index := range s.Table.Indexes {
		scores = append(scores, s.scoreIndexEntry(index, opts))
	}
	return pickBestScore(scores)
}

// indexMatchesOrderBy checks if an index can satisfy ORDER BY without sorting.
func (s *IndexSelector) indexMatchesOrderBy(index *IndexInfo, orderBy []OrderByColumn) bool {
	// Simplified: check if index columns match order by columns
	if len(orderBy) > len(index.Columns) {
		return false
	}

	for i, ob := range orderBy {
		if index.Columns[i].Name != ob.Column {
			return false
		}
		if index.Columns[i].Ascending != ob.Ascending {
			return false
		}
	}

	return true
}

// EstimateIndexBuildCost estimates the cost of building a new index.
// This is used to decide if an automatic index should be created.
func EstimateIndexBuildCost(table *TableInfo, columns []string) LogEst {
	// Cost to build index:
	// 1. Scan all table rows
	// 2. Sort them
	// 3. Build B-tree structure

	nRows := table.RowLogEst

	// Scan cost
	scanCost := nRows + costFullScan

	// Sort cost: O(n log n)
	sortCost := nRows
	if nRows > 0 {
		logN := LogEst(float64(nRows) * 0.3) // log2(n) approximation
		sortCost += logN
	}

	// Build cost: roughly same as sorted scan
	buildCost := nRows + LogEst(10)

	return scanCost + sortCost + buildCost
}
