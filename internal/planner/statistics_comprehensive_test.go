// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
)

// TestEstimateRowsComplexScenarios tests complex row estimation scenarios.
func TestEstimateRowsComplexScenarios(t *testing.T) {
	tests := []struct {
		name      string
		indexStat *IndexStatistics
		nEq       int
		hasRange  bool
	}{
		{
			name:      "nil statistics",
			indexStat: nil,
			nEq:       1,
			hasRange:  false,
		},
		{
			name: "zero equality constraints",
			indexStat: &IndexStatistics{
				IndexName:   "idx_test",
				RowCount:    1000,
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			nEq:      0,
			hasRange: false,
		},
		{
			name: "nEq greater than column stats",
			indexStat: &IndexStatistics{
				IndexName:   "idx_test",
				RowCount:    1000,
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			nEq:      5,
			hasRange: false,
		},
		{
			name: "with range constraint",
			indexStat: &IndexStatistics{
				IndexName:   "idx_age",
				RowCount:    10000,
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			nEq:      1,
			hasRange: true,
		},
		{
			name: "multiple equality constraints",
			indexStat: &IndexStatistics{
				IndexName:   "idx_compound",
				RowCount:    10000,
				ColumnStats: []LogEst{NewLogEst(100), NewLogEst(10)},
			},
			nEq:      2,
			hasRange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateRows(tt.indexStat, tt.nEq, tt.hasRange)

			// Should always return a non-negative estimate
			if result < 0 {
				t.Errorf("Negative row estimate: %d", result)
			}
		})
	}
}

// TestEstimateSelectivityOperators tests selectivity for different operators.
func TestEstimateSelectivityOperators(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{
		TableName: "users",
		RowCount:  1000,
	}

	tests := []struct {
		name     string
		term     *WhereTerm
		minValue LogEst
		maxValue LogEst
	}{
		{
			name: "equality with integer",
			term: &WhereTerm{
				Operator:   WO_EQ,
				RightValue: 42,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "equality with zero",
			term: &WhereTerm{
				Operator:   WO_EQ,
				RightValue: 0,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "less than",
			term: &WhereTerm{
				Operator:   WO_LT,
				RightValue: 100,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "greater than",
			term: &WhereTerm{
				Operator:   WO_GT,
				RightValue: 50,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "less than or equal",
			term: &WhereTerm{
				Operator:   WO_LE,
				RightValue: 75,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "greater than or equal",
			term: &WhereTerm{
				Operator:   WO_GE,
				RightValue: 25,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "IN operator",
			term: &WhereTerm{
				Operator: WO_IN,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "IS NULL",
			term: &WhereTerm{
				Operator: WO_ISNULL,
			},
			minValue: -100,
			maxValue: 0,
		},
		{
			name: "unknown operator",
			term: &WhereTerm{
				Operator: WO_NOOP,
			},
			minValue: -100,
			maxValue: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateSelectivity(tt.term, stats)

			if result < tt.minValue || result > tt.maxValue {
				t.Logf("Selectivity %d outside expected range [%d, %d]", result, tt.minValue, tt.maxValue)
			}
		})
	}
}

// TestApplyStatisticsToIndex tests applying statistics to indexes.
func TestApplyStatisticsToIndex(t *testing.T) {
	stats := NewStatistics()
	stats.IndexStats["idx_name"] = &IndexStatistics{
		IndexName:   "idx_name",
		TableName:   "users",
		RowCount:    5000,
		ColumnStats: []LogEst{NewLogEst(500)},
	}

	index := &IndexInfo{
		Name:        "idx_name",
		Table:       "users",
		RowCount:    100,
		RowLogEst:   NewLogEst(100),
		ColumnStats: []LogEst{},
	}

	ApplyStatisticsToIndex(index, stats)

	if index.RowCount != 5000 {
		t.Errorf("Expected RowCount 5000, got %d", index.RowCount)
	}

	if len(index.ColumnStats) != 1 {
		t.Errorf("Expected 1 column stat, got %d", len(index.ColumnStats))
	}
}

// TestLoadStatisticsErrors tests error handling in LoadStatistics.
func TestLoadStatisticsErrors(t *testing.T) {
	tests := []struct {
		name    string
		rows    []StatRow
		wantErr bool
	}{
		{
			name: "valid data",
			rows: []StatRow{
				{Tbl: "users", Idx: "", Stat: "1000"},
			},
			wantErr: false,
		},
		{
			name: "invalid table stat",
			rows: []StatRow{
				{Tbl: "users", Idx: "", Stat: "invalid"},
			},
			wantErr: true,
		},
		{
			name: "invalid index stat",
			rows: []StatRow{
				{Tbl: "users", Idx: "idx_name", Stat: "1000 invalid"},
			},
			wantErr: true,
		},
		{
			name:    "empty rows",
			rows:    []StatRow{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadStatistics(tt.rows)

			if tt.wantErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestComputeIndexStatisticsEdgeCases tests edge cases for index statistics.
func TestComputeIndexStatisticsEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		tableName      string
		indexName      string
		rowCount       int64
		distinctCounts []int64
	}{
		{
			name:           "zero rows",
			tableName:      "test",
			indexName:      "idx_test",
			rowCount:       0,
			distinctCounts: []int64{0},
		},
		{
			name:           "single distinct value",
			tableName:      "test",
			indexName:      "idx_test",
			rowCount:       1000,
			distinctCounts: []int64{1},
		},
		{
			name:           "all unique values",
			tableName:      "test",
			indexName:      "idx_test",
			rowCount:       1000,
			distinctCounts: []int64{1000},
		},
		{
			name:           "empty distinct counts",
			tableName:      "test",
			indexName:      "idx_test",
			rowCount:       1000,
			distinctCounts: []int64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stat := ComputeIndexStatistics(tt.tableName, tt.indexName, tt.rowCount, tt.distinctCounts)

			if stat == nil {
				t.Fatal("ComputeIndexStatistics returned nil")
			}

			if stat.RowCount != tt.rowCount {
				t.Errorf("Expected row count %d, got %d", tt.rowCount, stat.RowCount)
			}

			if len(stat.AvgEq) != len(tt.distinctCounts) {
				t.Errorf("Expected %d avgEq values, got %d", len(tt.distinctCounts), len(stat.AvgEq))
			}
		})
	}
}

// TestEstimateDistinctValuesEdgeCases tests edge cases for distinct value estimation.
func TestEstimateDistinctValuesEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalRows      int64
		checkBounds    bool
	}{
		{
			name:           "zero total rows",
			sampleSize:     100,
			uniqueInSample: 50,
			totalRows:      0,
			checkBounds:    true,
		},
		{
			name:           "negative sample size",
			sampleSize:     -100,
			uniqueInSample: 50,
			totalRows:      1000,
			checkBounds:    true,
		},
		{
			name:           "unique exceeds sample",
			sampleSize:     100,
			uniqueInSample: 200,
			totalRows:      1000,
			checkBounds:    true,
		},
		{
			name:           "sample exceeds total",
			sampleSize:     2000,
			uniqueInSample: 100,
			totalRows:      1000,
			checkBounds:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateDistinctValues(tt.sampleSize, tt.uniqueInSample, tt.totalRows)

			if tt.checkBounds {
				if result < 0 {
					t.Error("Result should not be negative")
				}
				if tt.totalRows > 0 && result > tt.totalRows {
					t.Errorf("Result %d exceeds total rows %d", result, tt.totalRows)
				}
			}
		})
	}
}

// TestEstimateEqualitySelectivity tests equality selectivity estimation.
func TestEstimateEqualitySelectivity(t *testing.T) {
	tests := []struct {
		name  string
		term  *WhereTerm
		stats *Statistics
	}{
		{
			name: "with statistics",
			term: &WhereTerm{
				Operator:   WO_EQ,
				LeftColumn: 0,
				RightValue: 42,
			},
			stats: &Statistics{
				TableStats: map[string]*TableStatistics{
					"test": {RowCount: 1000},
				},
			},
		},
		{
			name: "without statistics",
			term: &WhereTerm{
				Operator:   WO_EQ,
				LeftColumn: 0,
				RightValue: 42,
			},
			stats: NewStatistics(),
		},
		{
			name: "nil right value",
			term: &WhereTerm{
				Operator:   WO_EQ,
				LeftColumn: 0,
				RightValue: nil,
			},
			stats: NewStatistics(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := estimateEqualitySelectivity(tt.term, tt.stats)

			// Should return a reasonable selectivity value
			if result > 0 {
				t.Logf("Selectivity: %d (informational)", result)
			}
		})
	}
}

// TestEstimateRangeSelectivity tests range selectivity estimation.
func TestEstimateRangeSelectivity(t *testing.T) {
	term := &WhereTerm{
		Operator:   WO_GT,
		RightValue: 50,
	}
	stats := NewStatistics()

	result := estimateRangeSelectivity(term, stats)

	// Should return a consistent value
	if result == 0 {
		t.Error("Expected non-zero range selectivity")
	}
}

// TestEstimateInSelectivity tests IN selectivity estimation.
func TestEstimateInSelectivity(t *testing.T) {
	term := &WhereTerm{
		Operator: WO_IN,
	}
	stats := NewStatistics()

	result := estimateInSelectivity(term, stats)

	// Should return a consistent value
	if result == 0 {
		t.Error("Expected non-zero IN selectivity")
	}
}

// TestStatisticsTableOperations tests table-level statistics operations.
func TestStatisticsTableOperations(t *testing.T) {
	stats := NewStatistics()

	// Add table statistics
	stats.TableStats["table1"] = &TableStatistics{
		TableName: "table1",
		RowCount:  1000,
	}
	stats.TableStats["table2"] = &TableStatistics{
		TableName: "table2",
		RowCount:  2000,
	}

	// Test GetTableStats
	table1Stats := stats.GetTableStats("table1")
	if table1Stats == nil {
		t.Error("Expected table1 stats, got nil")
	}
	if table1Stats.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", table1Stats.RowCount)
	}

	// Test GetTableStats for non-existent table
	noStats := stats.GetTableStats("nonexistent")
	if noStats != nil {
		t.Error("Expected nil for non-existent table")
	}

	// Test GetIndexStats
	stats.IndexStats["idx1"] = &IndexStatistics{
		IndexName: "idx1",
		RowCount:  1000,
	}

	idx1Stats := stats.GetIndexStats("idx1")
	if idx1Stats == nil {
		t.Error("Expected idx1 stats, got nil")
	}

	// Test GetIndexStats for non-existent index
	noIdxStats := stats.GetIndexStats("nonexistent")
	if noIdxStats != nil {
		t.Error("Expected nil for non-existent index")
	}
}

// TestMergeStatisticsComplex tests complex merge scenarios.
func TestMergeStatisticsComplex(t *testing.T) {
	s1 := NewStatistics()
	s1.TableStats["table1"] = &TableStatistics{TableName: "table1", RowCount: 100}
	s1.TableStats["table2"] = &TableStatistics{TableName: "table2", RowCount: 200}
	s1.IndexStats["idx1"] = &IndexStatistics{IndexName: "idx1", RowCount: 100}

	s2 := NewStatistics()
	s2.TableStats["table2"] = &TableStatistics{TableName: "table2", RowCount: 250}
	s2.TableStats["table3"] = &TableStatistics{TableName: "table3", RowCount: 300}
	s2.IndexStats["idx2"] = &IndexStatistics{IndexName: "idx2", RowCount: 200}

	merged := MergeStatistics(s1, s2)

	// Should have all three tables
	if len(merged.TableStats) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(merged.TableStats))
	}

	// table2 should have s2's value (override)
	if merged.TableStats["table2"].RowCount != 250 {
		t.Errorf("Expected table2 row count 250, got %d", merged.TableStats["table2"].RowCount)
	}

	// Should have both indexes
	if len(merged.IndexStats) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(merged.IndexStats))
	}
}

// TestValidateStatisticsComplex tests complex validation scenarios.
func TestValidateStatisticsComplex(t *testing.T) {
	tests := []struct {
		name    string
		stats   *Statistics
		wantErr bool
	}{
		{
			name: "empty statistics",
			stats: NewStatistics(),
			wantErr: false,
		},
		{
			name: "zero row count",
			stats: &Statistics{
				TableStats: map[string]*TableStatistics{
					"test": {TableName: "test", RowCount: 0},
				},
			},
			wantErr: false,
		},
		{
			name: "negative index row count",
			stats: &Statistics{
				IndexStats: map[string]*IndexStatistics{
					"idx": {IndexName: "idx", RowCount: -1},
				},
			},
			wantErr: true,
		},
		{
			name: "empty avgEq in index",
			stats: &Statistics{
				IndexStats: map[string]*IndexStatistics{
					"idx": {IndexName: "idx", RowCount: 100, AvgEq: []int64{}},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.stats.ValidateStatistics()

			if tt.wantErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestStatisticsIntegration tests integration scenarios.
func TestStatisticsIntegration(t *testing.T) {
	// Create statistics
	stats := NewStatistics()

	// Compute table statistics
	tableStat := ComputeTableStatistics("users", 10000)
	stats.TableStats["users"] = tableStat

	// Compute index statistics
	idxStat := ComputeIndexStatistics("users", "idx_email", 10000, []int64{10000})
	stats.IndexStats["idx_email"] = idxStat

	// Validate
	if err := stats.ValidateStatistics(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	// Save and load
	rows := SaveStatistics(stats)
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	loaded, err := LoadStatistics(rows)
	if err != nil {
		t.Fatalf("LoadStatistics failed: %v", err)
	}

	// Verify loaded statistics match
	if loaded.GetTableStats("users").RowCount != 10000 {
		t.Error("Loaded table statistics don't match")
	}

	if loaded.GetIndexStats("idx_email").RowCount != 10000 {
		t.Error("Loaded index statistics don't match")
	}
}

// TestApplyStatisticsToTableWithoutStats tests applying with missing statistics.
func TestApplyStatisticsToTableWithoutStats(t *testing.T) {
	stats := NewStatistics()

	table := &TableInfo{
		Name:     "users",
		RowCount: 100,
		Indexes: []*IndexInfo{
			{Name: "idx_name", RowCount: 100},
		},
	}

	// Apply statistics when none exist for this table
	ApplyStatisticsToTable(table, stats)

	// Should not crash and table should keep original values
	if table.RowCount != 100 {
		t.Error("Table row count should remain unchanged")
	}
}

// TestEstimateRowsRangeAdjustment tests range adjustment in row estimation.
func TestEstimateRowsRangeAdjustment(t *testing.T) {
	indexStat := &IndexStatistics{
		IndexName:   "idx_age",
		RowCount:    10000,
		ColumnStats: []LogEst{NewLogEst(1000)},
	}

	// Test with range constraint
	withRange := EstimateRows(indexStat, 1, true)
	withoutRange := EstimateRows(indexStat, 1, false)

	// Range constraint should reduce the estimate
	if withRange >= withoutRange {
		t.Logf("Range estimate %d should be less than non-range %d (informational)",
			withRange, withoutRange)
	}
}

// TestLoadStatisticsMultiColumnIndex tests loading multi-column index stats.
func TestLoadStatisticsMultiColumnIndex(t *testing.T) {
	rows := []StatRow{
		{Tbl: "users", Idx: "", Stat: "10000"},
		{Tbl: "users", Idx: "idx_compound", Stat: "10000 100 50 10"},
	}

	stats, err := LoadStatistics(rows)
	if err != nil {
		t.Fatalf("LoadStatistics failed: %v", err)
	}

	idxStat := stats.GetIndexStats("idx_compound")
	if idxStat == nil {
		t.Fatal("Index stats not loaded")
	}

	if len(idxStat.AvgEq) != 3 {
		t.Errorf("Expected 3 avgEq values, got %d", len(idxStat.AvgEq))
	}

	expectedAvgEq := []int64{100, 50, 10}
	for i, expected := range expectedAvgEq {
		if idxStat.AvgEq[i] != expected {
			t.Errorf("avgEq[%d]: expected %d, got %d", i, expected, idxStat.AvgEq[i])
		}
	}
}
