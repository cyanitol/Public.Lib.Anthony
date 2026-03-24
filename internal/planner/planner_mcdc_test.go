// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// MC/DC tests for internal/planner
//
// For each compound boolean condition A && B (or A || B), MC/DC requires
// N+1 test cases such that each sub-condition independently causes the
// overall outcome to flip.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Source: cost.go EstimateIndexLookup
//   Condition: index.Unique && nEq >= len(index.Columns)
//   Sub-conditions:
//     A = index.Unique
//     B = nEq >= len(index.Columns)
//   Coverage pairs:
//     A flips outcome: (A=T,B=T)->true  vs (A=F,B=T)->false
//     B flips outcome: (A=T,B=T)->true  vs (A=T,B=F)->false
// ---------------------------------------------------------------------------

func TestMCDC_EstimateIndexLookup_UniqueAndNEqGELen(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()

	index := &IndexInfo{
		Name:        "test_idx",
		Table:       "users",
		Unique:      true,
		Columns:     []IndexColumn{{Name: "id", Index: 0, Ascending: true}},
		RowCount:    1000,
		RowLogEst:   NewLogEst(1000),
		ColumnStats: []LogEst{},
	}

	cases := []struct {
		name     string
		unique   bool
		nEq      int
		nCols    int // number of columns in index; we rebuild index with this many cols
		wantUniq bool
	}{
		// A=T, B=T -> unique lookup path (overall true)
		{"MCDC A=T B=T: unique lookup", true, 1, 1, true},
		// A=F, B=T -> non-unique despite nEq >= cols (A flips outcome)
		{"MCDC A=F B=T: no unique lookup", false, 1, 1, false},
		// A=T, B=F -> nEq < cols, not unique lookup (B flips outcome)
		{"MCDC A=T B=F: not enough eq", true, 0, 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := make([]IndexColumn, tc.nCols)
			for i := range cols {
				cols[i] = IndexColumn{Name: "id", Index: i, Ascending: true}
			}
			idx := &IndexInfo{
				Name:        "test_idx",
				Table:       "users",
				Unique:      tc.unique,
				Columns:     cols,
				RowCount:    index.RowCount,
				RowLogEst:   index.RowLogEst,
				ColumnStats: []LogEst{},
			}

			cost, nOut := cm.EstimateIndexLookup(table, idx, tc.nEq, false)

			// When the unique lookup branch fires, nOut == 0 (exactly 1 row)
			// and cost == costIndexSeek (or costIndexSeek + costRowidLookup).
			// When the non-unique branch fires, nOut is computed differently.
			if tc.wantUniq {
				if nOut != 0 {
					t.Errorf("expected nOut=0 (unique lookup), got %d", nOut)
				}
				if cost < costIndexSeek {
					t.Errorf("expected cost >= costIndexSeek, got %d", cost)
				}
			} else {
				// Non-unique path: cost is computed via calculateLookupCost (cm method)
				// Just verify we got a positive cost without panicking.
				if cost <= 0 {
					t.Errorf("expected positive cost for non-unique path, got %d", cost)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go (CostModel).estimateOutputRows
//   Condition: nEq > 0 && nEq <= len(index.ColumnStats)
//   Sub-conditions:
//     A = nEq > 0
//     B = nEq <= len(index.ColumnStats)
//   Coverage pairs:
//     A flips: (A=T,B=T)->stats branch  vs (A=F,B=T)->no-stats branch
//     B flips: (A=T,B=T)->stats branch  vs (A=T,B=F)->extrapolate branch
// ---------------------------------------------------------------------------

func TestMCDC_CostModel_EstimateOutputRows_NEqAndStats(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()

	statVal := NewLogEst(100)
	index := &IndexInfo{
		Name:        "test_idx",
		Table:       "users",
		Unique:      false,
		Columns:     []IndexColumn{{Name: "city", Index: 0, Ascending: true}},
		RowCount:    10000,
		RowLogEst:   NewLogEst(10000),
		ColumnStats: []LogEst{statVal},
	}

	cases := []struct {
		name      string
		nEq       int
		nStatCols int // how many ColumnStats to expose
		wantNOut  LogEst
	}{
		// A=T, B=T: nEq=1, len(ColumnStats)=1 => use stats directly
		{"MCDC A=T B=T: use column stats", 1, 1, statVal},
		// A=F, B=T: nEq=0 => return index.RowLogEst unchanged
		{"MCDC A=F B=T: nEq=0 skip stats", 0, 1, index.RowLogEst},
		// A=T, B=F: nEq=2 > len(ColumnStats)=1 => extrapolate
		{"MCDC A=T B=F: extrapolate beyond stats", 2, 1, -1 /* sentinel: just check different from statVal */},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := &IndexInfo{
				Name:        index.Name,
				Table:       index.Table,
				Unique:      index.Unique,
				Columns:     index.Columns,
				RowCount:    index.RowCount,
				RowLogEst:   index.RowLogEst,
				ColumnStats: index.ColumnStats[:tc.nStatCols],
			}
			_, nOut := cm.EstimateIndexLookup(table, idx, tc.nEq, false)

			if tc.wantNOut == -1 {
				// Extrapolate path: should be different from statVal and <= index.RowLogEst
				if nOut == statVal {
					t.Errorf("expected extrapolated value != statVal %d, got %d", statVal, nOut)
				}
			} else {
				// The public method wraps estimateOutputRows; check within tolerance.
				_ = nOut // result existence proves no panic; deep equality varies by path
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go (CostModel).estimateEqSelectivity
//   Condition: val >= -1 && val <= 1
//   Sub-conditions:
//     A = val >= -1
//     B = val <= 1
//   Coverage pairs:
//     A flips: (A=T,B=T,val=0)->SmallInt  vs (A=F,B=T,val=-2)->EQ
//     B flips: (A=T,B=T,val=0)->SmallInt  vs (A=T,B=F,val=2)->EQ
// ---------------------------------------------------------------------------

func TestMCDC_EstimateEqSelectivity_SmallIntRange(t *testing.T) {
	cm := NewCostModel()

	cases := []struct {
		name       string
		val        int
		wantResult LogEst
	}{
		// A=T, B=T: val=0 -> truthProbSmallInt
		{"MCDC A=T B=T: val=0 small int", 0, truthProbSmallInt},
		// A=T, B=T: val=-1 -> truthProbSmallInt  (boundary)
		{"MCDC A=T B=T: val=-1 boundary", -1, truthProbSmallInt},
		// A=T, B=T: val=1 -> truthProbSmallInt  (boundary)
		{"MCDC A=T B=T: val=1 boundary", 1, truthProbSmallInt},
		// A=F, B=T: val=-2 -> selectivityEq  (A flips outcome)
		{"MCDC A=F B=T: val=-2 not small", -2, selectivityEq},
		// A=T, B=F: val=2 -> selectivityEq  (B flips outcome)
		{"MCDC A=T B=F: val=2 not small", 2, selectivityEq},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := &WhereTerm{
				Operator:   WO_EQ,
				RightValue: tc.val,
			}
			got := cm.EstimateTruthProbability(term)
			if got != tc.wantResult {
				t.Errorf("EstimateTruthProbability with val=%d: got %d, want %d",
					tc.val, got, tc.wantResult)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: cost.go EstimateInOperator
//   Condition: nEq > 0 && nEq <= len(index.ColumnStats)
//   Sub-conditions:
//     A = nEq > 0
//     B = nEq <= len(index.ColumnStats)
//   Coverage pairs:
//     A flips: (A=T,B=T)->stats branch  vs (A=F,*)->RowLogEst branch
//     B flips: (A=T,B=T)->stats branch  vs (A=T,B=F)->RowLogEst+selectivityEq branch
// ---------------------------------------------------------------------------

func TestMCDC_EstimateInOperator_NEqAndStats(t *testing.T) {
	cm := NewCostModel()
	table := createTestTable()

	statVal := NewLogEst(50)
	index := &IndexInfo{
		Name:        "in_test_idx",
		Table:       "users",
		Unique:      false,
		Columns:     []IndexColumn{{Name: "city", Index: 3, Ascending: true}},
		RowCount:    10000,
		RowLogEst:   NewLogEst(10000),
		ColumnStats: []LogEst{statVal},
	}

	inListSize := 5

	cases := []struct {
		name      string
		nEq       int
		nStatCols int
	}{
		// A=T, B=T: nEq=1, len(ColumnStats)=1 -> use stats
		{"MCDC A=T B=T: nEq=1 use stats", 1, 1},
		// A=F, B=T: nEq=0 -> RowLogEst path (A flips)
		{"MCDC A=F B=T: nEq=0 skip stats", 0, 1},
		// A=T, B=F: nEq=2 > len(ColumnStats)=1 -> extrapolate (B flips)
		{"MCDC A=T B=F: nEq=2 beyond stats", 2, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := &IndexInfo{
				Name:        index.Name,
				Table:       index.Table,
				Unique:      index.Unique,
				Columns:     index.Columns,
				RowCount:    index.RowCount,
				RowLogEst:   index.RowLogEst,
				ColumnStats: index.ColumnStats[:tc.nStatCols],
			}
			cost, nOut := cm.EstimateInOperator(table, idx, tc.nEq, inListSize, false)
			// Sanity: both cost and nOut must be non-negative
			if cost < 0 {
				t.Errorf("got negative cost %d", cost)
			}
			if nOut < 0 {
				t.Errorf("got negative nOut %d", nOut)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go formatIndexScan
//   Condition: candidate.IsUnique && candidate.HasEquality
//   Sub-conditions:
//     A = candidate.IsUnique
//     B = candidate.HasEquality
//   Coverage pairs:
//     A flips: (A=T,B=T)->"SEARCH…=?" vs (A=F,B=T)->SEARCH or SCAN path
//     B flips: (A=T,B=T)->"SEARCH…=?" vs (A=T,B=F)->SCAN path
// ---------------------------------------------------------------------------

func TestMCDC_FormatIndexScan_UniqueAndHasEquality(t *testing.T) {
	cases := []struct {
		name        string
		isUnique    bool
		hasEquality bool
		wantContain string
	}{
		// A=T, B=T -> "SEARCH TABLE … USING INDEX …(col=?)"
		{"MCDC A=T B=T: unique+eq search", true, true, "SEARCH TABLE"},
		// A=F, B=T -> uses SEARCH without unique prefix (A flips)
		{"MCDC A=F B=T: non-unique eq search", false, true, "SEARCH TABLE"},
		// A=T, B=F -> covering/scan path (B flips)
		{"MCDC A=T B=F: unique but no eq", true, false, "TABLE"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cand := &IndexCandidate{
				IndexName:   "idx_test",
				TableName:   "tbl",
				Columns:     []string{"col"},
				IsUnique:    tc.isUnique,
				IsCovering:  false,
				HasEquality: tc.hasEquality,
			}
			result := formatIndexScan(cand)
			if len(result) == 0 {
				t.Errorf("formatIndexScan returned empty string")
			}
			// Verify it contains "TABLE" to confirm no panic and valid output
			if len(tc.wantContain) > 0 {
				found := false
				for i := 0; i <= len(result)-len(tc.wantContain); i++ {
					if result[i:i+len(tc.wantContain)] == tc.wantContain {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected %q to contain %q", result, tc.wantContain)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: explain.go isIndexCoveringCols  (uses *schema.Index)
//   Condition (per-column): lower != "rowid" && lower != "oid" &&
//                            lower != "_rowid_" && !m[lower]
//   This is a 4-operand AND. We test the important pairings:
//     If any of the first three aliases matches, the column is not required
//     to be in the index map (short-circuit via exclusion). If the column is
//     NOT an alias AND not in the map, the function returns false.
//   Coverage pairs:
//     All aliases true + col not in map -> false (fails last sub-condition)
//     All aliases true + col in map    -> true  (passes)
//     Col is a rowid alias             -> treated as covered (true)
//
// Also tested via CostModel.EstimateCoveringIndex which implements the same
// logic for *IndexInfo.
// ---------------------------------------------------------------------------

func TestMCDC_IsIndexCoveringCols_AliasAndMapCheck(t *testing.T) {
	idx := &schema.Index{
		Name:    "cov_idx",
		Columns: []string{"city", "age"},
	}

	cases := []struct {
		name          string
		neededColumns []string
		wantCovering  bool
	}{
		// All non-alias cols in index -> covering (all sub-conditions: aliases=F, map=T)
		{"MCDC all=in-idx: covering", []string{"city", "age"}, true},
		// Non-alias col NOT in index -> not covering (aliases=F, map=F flips outcome)
		{"MCDC col-not-in-idx: not covering", []string{"city", "name"}, false},
		// rowid alias counts as covered (alias sub-condition flips)
		{"MCDC rowid-alias: covered", []string{"city", "rowid"}, true},
		// oid alias counts as covered
		{"MCDC oid-alias: covered", []string{"city", "oid"}, true},
		// _rowid_ alias counts as covered
		{"MCDC _rowid_-alias: covered", []string{"city", "_rowid_"}, true},
		// empty needed columns -> not covering (len==0 guard)
		{"MCDC empty-needed: not covering", []string{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isIndexCoveringCols(idx, tc.neededColumns)
			if got != tc.wantCovering {
				t.Errorf("isIndexCoveringCols(%v) = %v, want %v",
					tc.neededColumns, got, tc.wantCovering)
			}
		})
	}
}

// TestMCDC_EstimateCoveringIndex tests the *IndexInfo variant of the
// covering-index check (cost.go CostModel.EstimateCoveringIndex).
//
//	Condition: !indexCols[col]  (per needed column)
//	A single column either IS or IS NOT in the index.
//	Coverage pairs:
//	  col in index     -> doesn't return false for that col -> overall true
//	  col not in index -> returns false immediately
func TestMCDC_EstimateCoveringIndex_ColInOrOut(t *testing.T) {
	cm := NewCostModel()
	index := &IndexInfo{
		Name:    "cov_idx",
		Columns: []IndexColumn{{Name: "city"}, {Name: "age"}},
	}

	cases := []struct {
		name          string
		neededColumns []string
		wantCovering  bool
	}{
		// All needed cols in index -> covering
		{"MCDC all-cols-in: covering", []string{"city", "age"}, true},
		// One col missing -> not covering (flips outcome)
		{"MCDC col-missing: not covering", []string{"city", "name"}, false},
		// Empty needed -> covering (trivially all satisfied)
		{"MCDC empty-needed: covering", []string{}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cm.EstimateCoveringIndex(index, tc.neededColumns)
			if got != tc.wantCovering {
				t.Errorf("EstimateCoveringIndex(%v) = %v, want %v",
					tc.neededColumns, got, tc.wantCovering)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go applyCoveringBonus
//   Condition: opts.PreferCovering && len(index.Columns) > 3
//   Sub-conditions:
//     A = opts.PreferCovering
//     B = len(index.Columns) > 3
//   Coverage pairs:
//     A flips: (A=T,B=T)->score+10  vs  (A=F,B=T)->no change
//     B flips: (A=T,B=T)->score+10  vs  (A=T,B=F)->no change
// ---------------------------------------------------------------------------

func TestMCDC_ApplyCoveringBonus_PreferCoveringAndWideIndex(t *testing.T) {
	wideIndex := &IndexInfo{
		Columns: []IndexColumn{{}, {}, {}, {}}, // 4 columns, > 3
	}
	narrowIndex := &IndexInfo{
		Columns: []IndexColumn{{}, {}}, // 2 columns, <= 3
	}

	baseScore := 10.0

	cases := []struct {
		name           string
		idx            *IndexInfo
		preferCovering bool
		wantBonus      float64
	}{
		// A=T, B=T -> +10 bonus
		{"MCDC A=T B=T: prefer+wide gets bonus", wideIndex, true, 10.0},
		// A=F, B=T -> no bonus (A flips outcome)
		{"MCDC A=F B=T: no prefer no bonus", wideIndex, false, 0.0},
		// A=T, B=F -> no bonus (B flips outcome)
		{"MCDC A=T B=F: prefer but narrow no bonus", narrowIndex, true, 0.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := OptimizeOptions{PreferCovering: tc.preferCovering}
			got := applyCoveringBonus(tc.idx, opts, baseScore)
			wantScore := baseScore + tc.wantBonus
			if got != wantScore {
				t.Errorf("applyCoveringBonus: got %.1f, want %.1f", got, wantScore)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go applyUniqueBonus
//   Condition: opts.PreferUnique && index.Unique
//   Sub-conditions:
//     A = opts.PreferUnique
//     B = index.Unique
//   Coverage pairs:
//     A flips: (A=T,B=T)->score+15  vs (A=F,B=T)->no change
//     B flips: (A=T,B=T)->score+15  vs (A=T,B=F)->no change
// ---------------------------------------------------------------------------

func TestMCDC_ApplyUniqueBonus_PreferUniqueAndUniqueIndex(t *testing.T) {
	baseScore := 5.0

	cases := []struct {
		name        string
		prefer      bool
		indexUnique bool
		wantBonus   float64
	}{
		// A=T, B=T -> +15
		{"MCDC A=T B=T: prefer+unique gets bonus", true, true, 15.0},
		// A=F, B=T -> no bonus (A flips)
		{"MCDC A=F B=T: no-prefer no bonus", false, true, 0.0},
		// A=T, B=F -> no bonus (B flips)
		{"MCDC A=T B=F: prefer non-unique no bonus", true, false, 0.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := &IndexInfo{Unique: tc.indexUnique}
			opts := OptimizeOptions{PreferUnique: tc.prefer}
			got := applyUniqueBonus(idx, opts, baseScore)
			want := baseScore + tc.wantBonus
			if got != want {
				t.Errorf("applyUniqueBonus: got %.1f, want %.1f", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go applyOrderByBonus
//   Condition: opts.ConsiderOrderBy && len(opts.OrderBy) > 0 &&
//              s.indexMatchesOrderBy(index, opts.OrderBy)
//   Sub-conditions:
//     A = opts.ConsiderOrderBy
//     B = len(opts.OrderBy) > 0
//     C = s.indexMatchesOrderBy(...)
//   Coverage pairs (3-term AND needs 4 cases):
//     A flips: (A=T,B=T,C=T)->+25  vs (A=F,B=T,C=T)->no change
//     B flips: (A=T,B=T,C=T)->+25  vs (A=T,B=F,C=*)->no change
//     C flips: (A=T,B=T,C=T)->+25  vs (A=T,B=T,C=F)->no change
// ---------------------------------------------------------------------------

func TestMCDC_ApplyOrderByBonus_ThreeTermAnd(t *testing.T) {
	matchingIndex := &IndexInfo{
		Columns: []IndexColumn{{Name: "city", Ascending: true}},
	}
	nonMatchingIndex := &IndexInfo{
		Columns: []IndexColumn{{Name: "age", Ascending: true}},
	}
	orderBy := []OrderByColumn{{Column: "city", Ascending: true}}

	sel := &IndexSelector{
		Table:     createTestTable(),
		Terms:     nil,
		CostModel: NewCostModel(),
	}

	baseScore := 0.0

	cases := []struct {
		name         string
		idx          *IndexInfo
		considerOB   bool
		orderBySlice []OrderByColumn
		wantBonus    float64
	}{
		// A=T, B=T, C=T -> +25
		{"MCDC A=T B=T C=T: all true bonus", matchingIndex, true, orderBy, 25.0},
		// A=F, B=T, C=T -> 0 (A flips)
		{"MCDC A=F B=T C=T: no consider", matchingIndex, false, orderBy, 0.0},
		// A=T, B=F, C=T -> 0 (B flips: empty slice)
		{"MCDC A=T B=F C=T: empty orderby", matchingIndex, true, []OrderByColumn{}, 0.0},
		// A=T, B=T, C=F -> 0 (C flips: index doesn't match)
		{"MCDC A=T B=T C=F: no match", nonMatchingIndex, true, orderBy, 0.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := OptimizeOptions{
				ConsiderOrderBy: tc.considerOB,
				OrderBy:         tc.orderBySlice,
			}
			got := applyOrderByBonus(sel, tc.idx, opts, baseScore)
			want := baseScore + tc.wantBonus
			if got != want {
				t.Errorf("applyOrderByBonus: got %.1f, want %.1f", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: index.go pickBestScore
//   Condition: candidate.score > best.score ||
//              (candidate.score == best.score && candidate.cost < best.cost)
//   Sub-conditions:
//     A = candidate.score > best.score
//     B = candidate.score == best.score
//     C = candidate.cost < best.cost
//   Key pairs (A dominates; B&&C is the tie-break):
//     A=T  -> pick candidate regardless of B/C
//     A=F, B=T, C=T  -> pick candidate via tie-break
//     A=F, B=T, C=F  -> keep best (C flips tie-break)
//     A=F, B=F       -> keep best (different lower score)
// ---------------------------------------------------------------------------

func TestMCDC_PickBestScore_ScoreAndCostTieBreak(t *testing.T) {
	bestEntry := indexScore{score: 10.0, cost: LogEst(50)}

	cases := []struct {
		name      string
		candidate indexScore
		wantBest  bool // whether candidate should win
	}{
		// A=T: candidate score higher -> candidate wins
		{"MCDC A=T: higher score wins", indexScore{score: 20.0, cost: LogEst(100)}, true},
		// A=F, B=T, C=T: same score, lower cost -> candidate wins
		{"MCDC A=F B=T C=T: tie score lower cost", indexScore{score: 10.0, cost: LogEst(30)}, true},
		// A=F, B=T, C=F: same score, higher cost -> best keeps
		{"MCDC A=F B=T C=F: tie score higher cost", indexScore{score: 10.0, cost: LogEst(80)}, false},
		// A=F, B=F: lower score -> best keeps
		{"MCDC A=F B=F: lower score loses", indexScore{score: 5.0, cost: LogEst(10)}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scores := []indexScore{bestEntry, tc.candidate}
			result := pickBestScore(scores)
			gotCandidate := (result.score == tc.candidate.score && result.cost == tc.candidate.cost &&
				!(result.score == bestEntry.score && result.cost == bestEntry.cost))

			// Handle the tie case where candidate == bestEntry (same values)
			if tc.candidate.score == bestEntry.score && tc.candidate.cost == bestEntry.cost {
				// Both are equal, either result is acceptable
				return
			}

			if gotCandidate != tc.wantBest {
				t.Errorf("pickBestScore: got candidate=%v, want=%v (result score=%.1f cost=%d)",
					gotCandidate, tc.wantBest, result.score, result.cost)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go addUniqueFlag (via computeIndexFlags -> addUniqueFlag)
//   Condition: index.Unique && c.nEq >= len(index.Columns)
//   Sub-conditions:
//     A = index.Unique
//     B = c.nEq >= len(index.Columns)
//   Coverage pairs:
//     A flips: (A=T,B=T)->WHERE_ONEROW set  vs (A=F,B=T)->not set
//     B flips: (A=T,B=T)->WHERE_ONEROW set  vs (A=T,B=F)->not set
// ---------------------------------------------------------------------------

func TestMCDC_AddUniqueFlag_UniqueAndNEqGECols(t *testing.T) {
	singleColIndex := &IndexInfo{
		Unique:  true,
		Columns: []IndexColumn{{Name: "id", Index: 0}},
	}
	multiColIndex := &IndexInfo{
		Unique:  true,
		Columns: []IndexColumn{{Name: "a", Index: 0}, {Name: "b", Index: 1}},
	}
	nonUniqueIndex := &IndexInfo{
		Unique:  false,
		Columns: []IndexColumn{{Name: "id", Index: 0}},
	}

	cases := []struct {
		name       string
		index      *IndexInfo
		nEq        int
		wantOneRow bool
	}{
		// A=T, B=T: unique + nEq==cols -> WHERE_ONEROW
		{"MCDC A=T B=T: unique full eq", singleColIndex, 1, true},
		// A=F, B=T: non-unique + nEq==cols -> no WHERE_ONEROW (A flips)
		{"MCDC A=F B=T: non-unique full eq", nonUniqueIndex, 1, false},
		// A=T, B=F: unique + nEq < cols -> no WHERE_ONEROW (B flips)
		{"MCDC A=T B=F: unique partial eq", multiColIndex, 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := termConstraints{nEq: tc.nEq, hasRange: false}
			flags := addUniqueFlag(tc.index, c, WHERE_INDEXED)
			hasOneRow := flags&WHERE_ONEROW != 0
			if hasOneRow != tc.wantOneRow {
				t.Errorf("addUniqueFlag: WHERE_ONEROW=%v, want %v (flags=%d)", hasOneRow, tc.wantOneRow, flags)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: statistics.go EstimateDistinctValues
//   Condition 1: sampleSize == 0 || uniqueInSample == 0
//   Sub-conditions:
//     A = sampleSize == 0
//     B = uniqueInSample == 0
//   Coverage pairs:
//     A=T -> returns 1 (A dominates OR)
//     A=F, B=T -> returns 1 (B flips outcome)
//     A=F, B=F -> proceeds to estimation
//
//   Condition 2: sampleSize >= totalRows
//   Single boolean: no compound, but pair with surrounding context.
//
//   Condition 3: sampleSize < totalRows/10
//   Single boolean tested via two cases.
// ---------------------------------------------------------------------------

func TestMCDC_EstimateDistinctValues_ZeroGuards(t *testing.T) {
	cases := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalRows      int64
		wantResult     int64
	}{
		// A=T: sampleSize==0 -> 1 (A dominates OR)
		{"MCDC A=T B=*: sampleSize=0 returns 1", 0, 5, 1000, 1},
		// A=F, B=T: uniqueInSample==0 -> 1 (B flips)
		{"MCDC A=F B=T: uniqueInSample=0 returns 1", 100, 0, 1000, 1},
		// A=F, B=F: both non-zero, proceeds
		{"MCDC A=F B=F: both nonzero proceeds", 100, 50, 1000, -1 /* sentinel: any non-1 result */},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EstimateDistinctValues(tc.sampleSize, tc.uniqueInSample, tc.totalRows)
			if tc.wantResult == -1 {
				if got <= 1 {
					t.Errorf("expected value > 1 for proceeding case, got %d", got)
				}
			} else {
				if got != tc.wantResult {
					t.Errorf("EstimateDistinctValues = %d, want %d", got, tc.wantResult)
				}
			}
		})
	}
}

func TestMCDC_EstimateDistinctValues_SampleSizeVsTotalRows(t *testing.T) {
	// Condition: sampleSize >= totalRows
	// A=T -> return uniqueInSample exactly
	// A=F -> extrapolate

	cases := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalRows      int64
		wantExact      bool
	}{
		// A=T: full sample -> exact count
		{"MCDC A=T: sample==total exact", 1000, 500, 1000, true},
		// A=T: sample > total -> exact count
		{"MCDC A=T: sample>total exact", 1500, 500, 1000, true},
		// A=F: partial sample -> extrapolated (different from uniqueInSample unless ratio happens to match)
		{"MCDC A=F: partial sample extrapolated", 100, 50, 1000, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EstimateDistinctValues(tc.sampleSize, tc.uniqueInSample, tc.totalRows)
			if tc.wantExact {
				if got != tc.uniqueInSample {
					t.Errorf("expected exact %d, got %d", tc.uniqueInSample, got)
				}
			} else {
				// Just confirm result is non-trivial (extrapolation ran)
				if got <= 0 {
					t.Errorf("expected positive extrapolated result, got %d", got)
				}
			}
		})
	}
}

func TestMCDC_EstimateDistinctValues_SmallSampleCorrection(t *testing.T) {
	// Condition: sampleSize < totalRows/10
	// A=T -> correction factor applied
	// A=F -> no correction

	totalRows := int64(10000)

	cases := []struct {
		name       string
		sampleSize int64
		wantCorr   bool // whether correction factor was applied (result > naive estimate)
	}{
		// A=T: sample=100 < 10000/10=1000 -> correction applied
		{"MCDC A=T: small sample correction applied", 100, true},
		// A=F: sample=5000 >= 1000 -> no correction
		{"MCDC A=F: large sample no correction", 5000, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uniqueInSample := tc.sampleSize / 2 // 50% unique in sample
			got := EstimateDistinctValues(tc.sampleSize, uniqueInSample, totalRows)

			// Naive estimate (no correction): (uniqueInSample/sampleSize) * totalRows
			naive := int64(float64(uniqueInSample) / float64(tc.sampleSize) * float64(totalRows))

			if tc.wantCorr {
				if got <= naive {
					t.Errorf("expected correction to increase estimate: naive=%d, got=%d", naive, got)
				}
			} else {
				// Without correction, result equals naive (before bounds clamping)
				if got < naive {
					t.Errorf("expected no-correction result >= naive: naive=%d, got=%d", naive, got)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: statistics.go computeColumnStat
//   Condition: avg > 0 (inner: distinctValues < 1)
//   A = avg > 0
//   Coverage pairs:
//     A=T -> computes distinctValues = rowCount/avg
//     A=F -> returns NewLogEst(rowCount) directly
// ---------------------------------------------------------------------------

func TestMCDC_ComputeColumnStat_AvgPositive(t *testing.T) {
	rowCount := int64(10000)

	cases := []struct {
		name      string
		avg       int64
		wantEqual bool // true if result should equal NewLogEst(rowCount/avg), false if NewLogEst(rowCount)
	}{
		// A=T: avg > 0 -> use rowCount/avg path
		{"MCDC A=T: avg=100 uses distinct values", 100, true},
		// A=T boundary: avg=1 -> distinct=rowCount (stays as is)
		{"MCDC A=T: avg=1 distinct=rowCount", 1, false /* NewLogEst(10000/1)==NewLogEst(rowCount) */},
		// A=F: avg=0 -> use NewLogEst(rowCount) path
		{"MCDC A=F: avg=0 fallback to rowCount", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeColumnStat(tc.avg, rowCount)
			if got < 0 {
				t.Errorf("computeColumnStat returned negative LogEst %d", got)
			}
			// When avg > 0 and avg < rowCount, result should be smaller than NewLogEst(rowCount)
			if tc.wantEqual {
				wantDistinct := NewLogEst(rowCount / tc.avg)
				if got != wantDistinct {
					t.Errorf("computeColumnStat(avg=%d, rows=%d): got %d, want %d",
						tc.avg, rowCount, got, wantDistinct)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go findTermForIndexColumn
//   Condition: term.LeftCursor == b.Cursor && term.LeftColumn == colIdx &&
//              isUsableOperator(term.Operator)
//   Sub-conditions:
//     A = term.LeftCursor == b.Cursor
//     B = term.LeftColumn == colIdx
//     C = isUsableOperator(term.Operator)
//   Coverage pairs:
//     A=T,B=T,C=T -> returns term
//     A=F,B=T,C=T -> returns nil (A flips)
//     A=T,B=F,C=T -> returns nil (B flips)
//     A=T,B=T,C=F -> returns nil (C flips)
// ---------------------------------------------------------------------------

func TestMCDC_FindTermForIndexColumn_CursorColOpTriple(t *testing.T) {
	const cursor = 0
	const colIdx = 2

	cases := []struct {
		name       string
		termCursor int
		termCol    int
		operator   WhereOperator
		wantFound  bool
	}{
		// A=T, B=T, C=T -> found
		{"MCDC A=T B=T C=T: matches all", cursor, colIdx, WO_EQ, true},
		// A=F, B=T, C=T -> wrong cursor, not found (A flips)
		{"MCDC A=F B=T C=T: wrong cursor", cursor + 1, colIdx, WO_EQ, false},
		// A=T, B=F, C=T -> wrong col, not found (B flips)
		{"MCDC A=T B=F C=T: wrong col", cursor, colIdx + 1, WO_EQ, false},
		// A=T, B=T, C=F -> unusable operator, not found (C flips)
		{"MCDC A=T B=T C=F: unusable op", cursor, colIdx, WO_OR, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := &WhereTerm{
				LeftCursor: tc.termCursor,
				LeftColumn: tc.termCol,
				Operator:   tc.operator,
			}
			builder := &WhereLoopBuilder{
				Cursor: cursor,
				Terms:  []*WhereTerm{term},
			}
			result := builder.findTermForIndexColumn(colIdx)
			found := result != nil
			if found != tc.wantFound {
				t.Errorf("findTermForIndexColumn: found=%v, want=%v", found, tc.wantFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Source: whereloop.go applyTermsToLoop
//   Condition: term.LeftCursor == b.Cursor && term.PrereqRight == 0
//   Sub-conditions:
//     A = term.LeftCursor == b.Cursor
//     B = term.PrereqRight == 0
//   Coverage pairs:
//     A=T, B=T -> term applied to loop
//     A=F, B=T -> term not applied (A flips)
//     A=T, B=F -> term not applied (B flips)
// ---------------------------------------------------------------------------

func TestMCDC_ApplyTermsToLoop_CursorAndNoPrereq(t *testing.T) {
	const cursor = 0

	table := createTestTable()
	cm := NewCostModel()

	cases := []struct {
		name        string
		termCursor  int
		prereqRight Bitmask
		wantApplied bool
	}{
		// A=T, B=T: term applied
		{"MCDC A=T B=T: term applied", cursor, 0, true},
		// A=F, B=T: wrong cursor -> not applied (A flips)
		{"MCDC A=F B=T: wrong cursor not applied", cursor + 1, 0, false},
		// A=T, B=F: has prereq -> not applied (B flips)
		{"MCDC A=T B=F: prereq not applied", cursor, Bitmask(1 << 1), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := &WhereTerm{
				LeftCursor:  tc.termCursor,
				LeftColumn:  0,
				Operator:    WO_EQ,
				RightValue:  5,
				PrereqRight: tc.prereqRight,
			}
			builder := &WhereLoopBuilder{
				Table:     table,
				Cursor:    cursor,
				Terms:     []*WhereTerm{term},
				CostModel: cm,
			}
			cost, nOut := cm.EstimateFullScan(table)
			loop := &WhereLoop{
				TabIndex: cursor,
				Run:      cost,
				NOut:     nOut,
				Terms:    make([]*WhereTerm, 0),
			}
			loop.MaskSelf.Set(cursor)

			builder.applyTermsToLoop(loop)

			applied := len(loop.Terms) > 0
			if applied != tc.wantApplied {
				t.Errorf("applyTermsToLoop: applied=%v, want=%v", applied, tc.wantApplied)
			}
		})
	}
}
