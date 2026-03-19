// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"
)

// TestNewIndexSelector tests creating a new index selector.
func TestNewIndexSelector(t *testing.T) {
	table := &TableInfo{
		Name: "users",
		Indexes: []*IndexInfo{
			{Name: "idx_name", Columns: []IndexColumn{{Name: "name", Index: 0}}},
		},
	}
	terms := []*WhereTerm{
		{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
	}
	costModel := NewCostModel()

	selector := NewIndexSelector(table, terms, costModel)
	if selector == nil {
		t.Fatal("NewIndexSelector returned nil")
	}
	if selector.Table != table {
		t.Error("Table not set correctly")
	}
	if len(selector.Terms) != 1 {
		t.Errorf("Expected 1 term, got %d", len(selector.Terms))
	}
	if selector.CostModel == nil {
		t.Error("CostModel not set")
	}
}

// TestSelectBestIndex tests index selection.
func TestSelectBestIndex(t *testing.T) {
	tests := []struct {
		name      string
		table     *TableInfo
		terms     []*WhereTerm
		wantIndex string
		wantNil   bool
	}{
		{
			name: "no indexes",
			table: &TableInfo{
				Name:    "users",
				Indexes: []*IndexInfo{},
			},
			terms:   []*WhereTerm{},
			wantNil: true,
		},
		{
			name: "single index matching term",
			table: &TableInfo{
				Name: "users",
				Indexes: []*IndexInfo{
					{
						Name: "idx_name",
						Columns: []IndexColumn{
							{Name: "name", Index: 0},
						},
					},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
			},
			wantIndex: "idx_name",
		},
		{
			name: "multiple indexes choose best",
			table: &TableInfo{
				Name: "users",
				Indexes: []*IndexInfo{
					{
						Name: "idx_name",
						Columns: []IndexColumn{
							{Name: "name", Index: 0},
						},
					},
					{
						Name:    "idx_pk",
						Primary: true,
						Unique:  true,
						Columns: []IndexColumn{
							{Name: "id", Index: 1},
						},
					},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_EQ, RightValue: 1},
			},
			wantIndex: "idx_pk",
		},
		{
			name: "unique index preferred",
			table: &TableInfo{
				Name: "users",
				Indexes: []*IndexInfo{
					{
						Name:   "idx_email",
						Unique: true,
						Columns: []IndexColumn{
							{Name: "email", Index: 0},
						},
					},
					{
						Name: "idx_name",
						Columns: []IndexColumn{
							{Name: "name", Index: 1},
						},
					},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test@test.com"},
				{LeftColumn: 1, Operator: WO_EQ, RightValue: "test"},
			},
			wantIndex: "idx_email",
		},
		{
			name: "no usable index returns nil",
			table: &TableInfo{
				Name: "users",
				Indexes: []*IndexInfo{
					{
						Name: "idx_name",
						Columns: []IndexColumn{
							{Name: "name", Index: 0},
						},
					},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_EQ, RightValue: "test"},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(tt.table, tt.terms, NewCostModel())
			result := selector.SelectBestIndex()

			if tt.wantNil {
				if result != nil {
					t.Errorf("Expected nil, got index %s", result.Name)
				}
			} else {
				if result == nil {
					t.Fatalf("Expected index %s, got nil", tt.wantIndex)
				}
				if result.Name != tt.wantIndex {
					t.Errorf("Expected index %s, got %s", tt.wantIndex, result.Name)
				}
			}
		})
	}
}

// TestScoreIndex tests index scoring algorithm.
func TestScoreIndex(t *testing.T) {
	tests := []struct {
		name     string
		index    *IndexInfo
		terms    []*WhereTerm
		minScore float64
		maxScore float64
	}{
		{
			name: "equality constraint",
			index: &IndexInfo{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
			},
			minScore: 10.0,
		},
		{
			name: "unique index bonus",
			index: &IndexInfo{
				Name:   "idx_email",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "email", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test@test.com"},
			},
			minScore: 30.0, // 10 (term) + 5 (eq) + 20 (unique)
		},
		{
			name: "primary key bonus",
			index: &IndexInfo{
				Name:    "idx_pk",
				Primary: true,
				Columns: []IndexColumn{
					{Name: "id", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
			},
			minScore: 25.0, // 10 (term) + 5 (eq) + 15 (primary)
		},
		{
			name: "range operator lower score",
			index: &IndexInfo{
				Name: "idx_age",
				Columns: []IndexColumn{
					{Name: "age", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_GT, RightValue: 18},
			},
			minScore: 10.0, // 10 (term) + 1 (range)
		},
		{
			name: "IN operator",
			index: &IndexInfo{
				Name: "idx_status",
				Columns: []IndexColumn{
					{Name: "status", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_IN},
			},
			minScore: 10.0, // 10 (term) + 3 (IN)
		},
		{
			name: "multi-column index penalty",
			index: &IndexInfo{
				Name: "idx_compound",
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
					{Name: "col2", Index: 1},
					{Name: "col3", Index: 2},
					{Name: "col4", Index: 3},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
			},
			maxScore: 20.0, // Gets penalty for wide index
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, tt.terms, NewCostModel())
			score := selector.scoreIndex(tt.index)

			if tt.minScore > 0 && score < tt.minScore {
				t.Errorf("Score %f is less than minimum %f", score, tt.minScore)
			}
			if tt.maxScore > 0 && score > tt.maxScore {
				t.Errorf("Score %f is greater than maximum %f", score, tt.maxScore)
			}
		})
	}
}

// TestFindUsableTermsForIndex tests finding usable WHERE terms for an index.
func TestFindUsableTermsForIndex(t *testing.T) {
	tests := []struct {
		name      string
		index     *IndexInfo
		terms     []*WhereTerm
		wantCount int
	}{
		{
			name: "single column match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
			},
			wantCount: 1,
		},
		{
			name: "multi-column all match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
					{Name: "col2", Index: 1},
					{Name: "col3", Index: 2},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
				{LeftColumn: 1, Operator: WO_EQ, RightValue: 2},
				{LeftColumn: 2, Operator: WO_EQ, RightValue: 3},
			},
			wantCount: 3,
		},
		{
			name: "multi-column partial match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
					{Name: "col2", Index: 1},
					{Name: "col3", Index: 2},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
				{LeftColumn: 2, Operator: WO_EQ, RightValue: 3}, // Skip col2
			},
			wantCount: 1, // Stops at col1 because col2 not found
		},
		{
			name: "no match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_EQ, RightValue: 1},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, tt.terms, NewCostModel())
			usable := selector.findUsableTermsForIndex(tt.index)

			if len(usable) != tt.wantCount {
				t.Errorf("Expected %d usable terms, got %d", tt.wantCount, len(usable))
			}
		})
	}
}

// TestTermMatchesColumn tests term matching logic.
func TestTermMatchesColumn(t *testing.T) {
	tests := []struct {
		name    string
		term    *WhereTerm
		column  IndexColumn
		matches bool
	}{
		{
			name:    "exact match",
			term:    &WhereTerm{LeftColumn: 0, Operator: WO_EQ},
			column:  IndexColumn{Index: 0},
			matches: true,
		},
		{
			name:    "different column",
			term:    &WhereTerm{LeftColumn: 1, Operator: WO_EQ},
			column:  IndexColumn{Index: 0},
			matches: false,
		},
		{
			name:    "unusable operator",
			term:    &WhereTerm{LeftColumn: 0, Operator: WO_NOOP},
			column:  IndexColumn{Index: 0},
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, nil, NewCostModel())
			matches := selector.termMatchesColumn(tt.term, tt.column)

			if matches != tt.matches {
				t.Errorf("Expected matches = %v, got %v", tt.matches, matches)
			}
		})
	}
}

// TestAnalyzeIndexUsage tests analyzing how an index would be used.
func TestAnalyzeIndexUsage(t *testing.T) {
	tests := []struct {
		name           string
		index          *IndexInfo
		terms          []*WhereTerm
		neededColumns  []string
		wantEqTerms    int
		wantRangeTerms int
		wantInTerms    int
		wantCovering   bool
	}{
		{
			name: "equality constraint",
			index: &IndexInfo{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
			},
			neededColumns:  []string{"name"},
			wantEqTerms:    1,
			wantRangeTerms: 0,
			wantInTerms:    0,
			wantCovering:   true,
		},
		{
			name: "range constraint",
			index: &IndexInfo{
				Name: "idx_age",
				Columns: []IndexColumn{
					{Name: "age", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_GT, RightValue: 18},
			},
			neededColumns:  []string{"age"},
			wantEqTerms:    0,
			wantRangeTerms: 1,
			wantInTerms:    0,
			wantCovering:   true,
		},
		{
			name: "IN constraint",
			index: &IndexInfo{
				Name: "idx_status",
				Columns: []IndexColumn{
					{Name: "status", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_IN},
			},
			neededColumns:  []string{"status"},
			wantEqTerms:    0,
			wantRangeTerms: 0,
			wantInTerms:    1,
			wantCovering:   true,
		},
		{
			name: "multiple equality constraints",
			index: &IndexInfo{
				Name: "idx_compound",
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
					{Name: "col2", Index: 1},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
				{LeftColumn: 1, Operator: WO_EQ, RightValue: 2},
			},
			neededColumns:  []string{"col1", "col2"},
			wantEqTerms:    2,
			wantRangeTerms: 0,
			wantInTerms:    0,
			wantCovering:   true,
		},
		{
			name: "not covering",
			index: &IndexInfo{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
			},
			neededColumns:  []string{"name", "email", "age"},
			wantEqTerms:    1,
			wantRangeTerms: 0,
			wantInTerms:    0,
			wantCovering:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, tt.terms, NewCostModel())
			usage := selector.AnalyzeIndexUsage(tt.index, tt.neededColumns)

			if usage == nil {
				t.Fatal("AnalyzeIndexUsage returned nil")
			}

			if len(usage.EqTerms) != tt.wantEqTerms {
				t.Errorf("Expected %d eq terms, got %d", tt.wantEqTerms, len(usage.EqTerms))
			}
			if len(usage.RangeTerms) != tt.wantRangeTerms {
				t.Errorf("Expected %d range terms, got %d", tt.wantRangeTerms, len(usage.RangeTerms))
			}
			if len(usage.InTerms) != tt.wantInTerms {
				t.Errorf("Expected %d IN terms, got %d", tt.wantInTerms, len(usage.InTerms))
			}
			if usage.Covering != tt.wantCovering {
				t.Errorf("Expected covering = %v, got %v", tt.wantCovering, usage.Covering)
			}
		})
	}
}

// TestExplainIndexUsage tests explaining index usage.
func TestExplainIndexUsage(t *testing.T) {
	tests := []struct {
		name      string
		usage     *IndexUsage
		wantEmpty bool
	}{
		{
			name: "nil index",
			usage: &IndexUsage{
				Index: nil,
			},
			wantEmpty: false, // Returns "FULL TABLE SCAN"
		},
		{
			name: "simple index",
			usage: &IndexUsage{
				Index: &IndexInfo{
					Name: "idx_name",
					Columns: []IndexColumn{
						{Name: "name", Index: 0},
					},
				},
				EqTerms: []*WhereTerm{
					{LeftColumn: 0, Operator: WO_EQ},
				},
			},
			wantEmpty: false,
		},
		{
			name: "covering index",
			usage: &IndexUsage{
				Index: &IndexInfo{
					Name: "idx_name",
					Columns: []IndexColumn{
						{Name: "name", Index: 0},
					},
				},
				EqTerms: []*WhereTerm{
					{LeftColumn: 0, Operator: WO_EQ},
				},
				Covering: true,
			},
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.usage.Explain()

			if tt.wantEmpty && result != "" {
				t.Errorf("Expected empty string, got %s", result)
			}
			if !tt.wantEmpty && result == "" {
				t.Error("Expected non-empty string, got empty")
			}
		})
	}
}

// TestOperatorStringIndex tests operator string conversion in index context.
func TestOperatorStringIndex(t *testing.T) {
	tests := []struct {
		operator WhereOperator
		want     string
	}{
		{WO_EQ, "="},
		{WO_LT, "<"},
		{WO_LE, "<="},
		{WO_GT, ">"},
		{WO_GE, ">="},
		{WO_IN, " IN "},
		{WO_IS, " IS "},
		{WO_ISNULL, " IS NULL"},
		{WO_NOOP, "?"}, // Unknown operator
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := operatorString(tt.operator)
			if result != tt.want {
				t.Errorf("Expected %s, got %s", tt.want, result)
			}
		})
	}
}

// TestBuildIndexStats tests building index statistics.
func TestBuildIndexStats(t *testing.T) {
	table := &TableInfo{
		Name:      "users",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
		Columns: []ColumnInfo{
			{Name: "id", Index: 0},
			{Name: "name", Index: 1},
			{Name: "email", Index: 2},
		},
	}

	tests := []struct {
		name    string
		columns []string
		unique  bool
	}{
		{
			name:    "single column",
			columns: []string{"name"},
			unique:  false,
		},
		{
			name:    "multi-column",
			columns: []string{"name", "email"},
			unique:  false,
		},
		{
			name:    "unique index",
			columns: []string{"email"},
			unique:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := BuildIndexStats(table, tt.columns, tt.unique)

			if index == nil {
				t.Fatal("BuildIndexStats returned nil")
			}
			if index.Table != table.Name {
				t.Errorf("Expected table %s, got %s", table.Name, index.Table)
			}
			if len(index.Columns) != len(tt.columns) {
				t.Errorf("Expected %d columns, got %d", len(tt.columns), len(index.Columns))
			}
			if index.Unique != tt.unique {
				t.Errorf("Expected unique = %v, got %v", tt.unique, index.Unique)
			}
			if len(index.ColumnStats) != len(tt.columns) {
				t.Errorf("Expected %d column stats, got %d", len(tt.columns), len(index.ColumnStats))
			}
		})
	}
}

// TestCompareIndexes tests comparing two indexes.
func TestCompareIndexes(t *testing.T) {
	tests := []struct {
		name     string
		idx1     *IndexInfo
		idx2     *IndexInfo
		terms    []*WhereTerm
		expected int
	}{
		{
			name: "first index better",
			idx1: &IndexInfo{
				Name:   "idx_pk",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "id", Index: 0},
				},
			},
			idx2: &IndexInfo{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 1},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
			},
			expected: -1,
		},
		{
			name: "second index better",
			idx1: &IndexInfo{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 1},
				},
			},
			idx2: &IndexInfo{
				Name:   "idx_pk",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "id", Index: 0},
				},
			},
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareIndexes(tt.idx1, tt.idx2, tt.terms, NewCostModel())

			if (tt.expected < 0 && result >= 0) ||
				(tt.expected > 0 && result <= 0) ||
				(tt.expected == 0 && result != 0) {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestSelectBestIndexWithOptions tests advanced index selection.
func TestSelectBestIndexWithOptions(t *testing.T) {
	table := &TableInfo{
		Name: "users",
		Indexes: []*IndexInfo{
			{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
			},
			{
				Name:   "idx_email",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "email", Index: 1},
					{Name: "name", Index: 0},
					{Name: "age", Index: 2},
					{Name: "city", Index: 3},
				},
			},
		},
	}

	tests := []struct {
		name      string
		terms     []*WhereTerm
		options   OptimizeOptions
		wantIndex string
	}{
		{
			name: "prefer unique",
			terms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
				{LeftColumn: 1, Operator: WO_EQ, RightValue: "test@test.com"},
			},
			options: OptimizeOptions{
				PreferUnique: true,
			},
			wantIndex: "idx_email",
		},
		{
			name: "prefer covering",
			terms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_EQ, RightValue: "test@test.com"},
			},
			options: OptimizeOptions{
				PreferCovering: true,
			},
			wantIndex: "idx_email",
		},
		{
			name: "order by optimization",
			terms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_EQ, RightValue: "test@test.com"},
			},
			options: OptimizeOptions{
				ConsiderOrderBy: true,
				OrderBy: []OrderByColumn{
					{Column: "email", Ascending: true},
					{Column: "name", Ascending: true},
				},
			},
			wantIndex: "idx_email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(table, tt.terms, NewCostModel())
			result := selector.SelectBestIndexWithOptions(tt.options)

			if result == nil {
				t.Fatal("Expected index, got nil")
			}
			if result.Name != tt.wantIndex {
				t.Errorf("Expected index %s, got %s", tt.wantIndex, result.Name)
			}
		})
	}
}

// TestIndexMatchesOrderBy tests ORDER BY matching.
func TestIndexMatchesOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		index   *IndexInfo
		orderBy []OrderByColumn
		matches bool
	}{
		{
			name: "exact match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
					{Name: "age", Ascending: true},
				},
			},
			orderBy: []OrderByColumn{
				{Column: "name", Ascending: true},
				{Column: "age", Ascending: true},
			},
			matches: true,
		},
		{
			name: "partial match",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
					{Name: "age", Ascending: true},
					{Name: "city", Ascending: true},
				},
			},
			orderBy: []OrderByColumn{
				{Column: "name", Ascending: true},
			},
			matches: true,
		},
		{
			name: "direction mismatch",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
				},
			},
			orderBy: []OrderByColumn{
				{Column: "name", Ascending: false},
			},
			matches: false,
		},
		{
			name: "column mismatch",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
				},
			},
			orderBy: []OrderByColumn{
				{Column: "age", Ascending: true},
			},
			matches: false,
		},
		{
			name: "order by longer than index",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
				},
			},
			orderBy: []OrderByColumn{
				{Column: "name", Ascending: true},
				{Column: "age", Ascending: true},
			},
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, nil, NewCostModel())
			matches := selector.indexMatchesOrderBy(tt.index, tt.orderBy)

			if matches != tt.matches {
				t.Errorf("Expected matches = %v, got %v", tt.matches, matches)
			}
		})
	}
}

// TestEstimateIndexBuildCost tests estimating index build cost.
func TestEstimateIndexBuildCost(t *testing.T) {
	tests := []struct {
		name      string
		table     *TableInfo
		columns   []string
		expectMin LogEst
	}{
		{
			name: "single column index",
			table: &TableInfo{
				Name:      "users",
				RowCount:  1000,
				RowLogEst: NewLogEst(1000),
			},
			columns:   []string{"name"},
			expectMin: 0,
		},
		{
			name: "multi-column index",
			table: &TableInfo{
				Name:      "users",
				RowCount:  10000,
				RowLogEst: NewLogEst(10000),
			},
			columns:   []string{"name", "age", "city"},
			expectMin: 0,
		},
		{
			name: "large table",
			table: &TableInfo{
				Name:      "events",
				RowCount:  1000000,
				RowLogEst: NewLogEst(1000000),
			},
			columns:   []string{"event_type"},
			expectMin: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost := EstimateIndexBuildCost(tt.table, tt.columns)

			if cost < tt.expectMin {
				t.Errorf("Cost %d is less than expected minimum %d", cost, tt.expectMin)
			}

			// Cost should be positive for non-empty tables
			if tt.table.RowCount > 0 && cost <= 0 {
				t.Error("Expected positive cost for non-empty table")
			}
		})
	}
}

// TestAnalyzeTermCounts tests analyzing WHERE term counts.
func TestAnalyzeTermCounts(t *testing.T) {
	tests := []struct {
		name         string
		terms        []*WhereTerm
		wantEq       int
		wantHasRange bool
	}{
		{
			name: "only equality",
			terms: []*WhereTerm{
				{Operator: WO_EQ},
				{Operator: WO_EQ},
			},
			wantEq:       2,
			wantHasRange: false,
		},
		{
			name: "equality and range",
			terms: []*WhereTerm{
				{Operator: WO_EQ},
				{Operator: WO_GT},
			},
			wantEq:       1,
			wantHasRange: true,
		},
		{
			name: "only range",
			terms: []*WhereTerm{
				{Operator: WO_LT},
				{Operator: WO_GE},
			},
			wantEq:       0,
			wantHasRange: true,
		},
		{
			name:         "no terms",
			terms:        []*WhereTerm{},
			wantEq:       0,
			wantHasRange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nEq, hasRange := analyzeTermCounts(tt.terms)

			if nEq != tt.wantEq {
				t.Errorf("Expected %d equality terms, got %d", tt.wantEq, nEq)
			}
			if hasRange != tt.wantHasRange {
				t.Errorf("Expected hasRange = %v, got %v", tt.wantHasRange, hasRange)
			}
		})
	}
}

// TestScoreIndexEntry tests scoring an index entry.
func TestScoreIndexEntry(t *testing.T) {
	table := &TableInfo{
		Name:      "users",
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
	}

	index := &IndexInfo{
		Name: "idx_name",
		Columns: []IndexColumn{
			{Name: "name", Index: 0},
		},
		RowCount:    1000,
		RowLogEst:   NewLogEst(1000),
		ColumnStats: []LogEst{NewLogEst(100)},
	}

	terms := []*WhereTerm{
		{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
	}

	selector := NewIndexSelector(table, terms, NewCostModel())
	score := selector.scoreIndexEntry(index, OptimizeOptions{})

	if score.index != index {
		t.Error("Index not set correctly in score")
	}
	if score.score <= 0 {
		t.Error("Expected positive score")
	}
}

// TestPickBestScore tests picking the best score from candidates.
func TestPickBestScore(t *testing.T) {
	scores := []indexScore{
		{score: 10.0, cost: 100},
		{score: 20.0, cost: 50},
		{score: 15.0, cost: 75},
	}

	best := pickBestScore(scores)

	if best.score != 20.0 {
		t.Errorf("Expected score 20.0, got %f", best.score)
	}
}

// TestApplyOptionsBonus tests applying option bonuses.
func TestApplyOptionsBonus(t *testing.T) {
	tests := []struct {
		name      string
		index     *IndexInfo
		options   OptimizeOptions
		baseScore float64
		wantMore  bool
	}{
		{
			name: "covering bonus",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"},
				},
			},
			options: OptimizeOptions{
				PreferCovering: true,
			},
			baseScore: 10.0,
			wantMore:  true,
		},
		{
			name: "unique bonus",
			index: &IndexInfo{
				Unique: true,
			},
			options: OptimizeOptions{
				PreferUnique: true,
			},
			baseScore: 10.0,
			wantMore:  true,
		},
		{
			name: "order by bonus",
			index: &IndexInfo{
				Columns: []IndexColumn{
					{Name: "name", Ascending: true},
				},
			},
			options: OptimizeOptions{
				ConsiderOrderBy: true,
				OrderBy: []OrderByColumn{
					{Column: "name", Ascending: true},
				},
			},
			baseScore: 10.0,
			wantMore:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selector := NewIndexSelector(&TableInfo{}, nil, NewCostModel())
			result := selector.applyOptionsBonus(tt.index, tt.options, tt.baseScore)

			if tt.wantMore && result <= tt.baseScore {
				t.Errorf("Expected score > %f, got %f", tt.baseScore, result)
			}
			if !tt.wantMore && result != tt.baseScore {
				t.Errorf("Expected score = %f, got %f", tt.baseScore, result)
			}
		})
	}
}

// TestFindBestIndexScore tests finding the best index score.
func TestFindBestIndexScore(t *testing.T) {
	table := &TableInfo{
		Name:      "users",
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
		Indexes: []*IndexInfo{
			{
				Name: "idx_name",
				Columns: []IndexColumn{
					{Name: "name", Index: 0},
				},
				RowCount:    1000,
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			{
				Name:   "idx_email",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "email", Index: 1},
				},
				RowCount:    1000,
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(1000)},
			},
		},
	}

	terms := []*WhereTerm{
		{LeftColumn: 1, Operator: WO_EQ, RightValue: "test@test.com"},
	}

	selector := NewIndexSelector(table, terms, NewCostModel())
	best := selector.findBestIndexScore(OptimizeOptions{})

	if best.index == nil {
		t.Fatal("Expected index, got nil")
	}
	if best.score <= 0 {
		t.Error("Expected positive score")
	}
}
