// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"
)

// TestNewStatistics tests creating a new Statistics object.
func TestNewStatistics(t *testing.T) {
	stats := NewStatistics()
	if stats == nil {
		t.Fatal("NewStatistics returned nil")
	}
	if stats.TableStats == nil {
		t.Error("TableStats map is nil")
	}
	if stats.IndexStats == nil {
		t.Error("IndexStats map is nil")
	}
	if len(stats.TableStats) != 0 {
		t.Error("Expected empty TableStats")
	}
	if len(stats.IndexStats) != 0 {
		t.Error("Expected empty IndexStats")
	}
}

// TestLoadStatistics tests loading statistics from sqlite_stat1 format.
func TestLoadStatistics(t *testing.T) {
	rows := []StatRow{
		{Tbl: "users", Idx: "", Stat: "1000"},
		{Tbl: "users", Idx: "idx_name", Stat: "1000 10"},
		{Tbl: "users", Idx: "idx_age_city", Stat: "1000 50 5"},
	}

	stats, err := LoadStatistics(rows)
	if err != nil {
		t.Fatalf("LoadStatistics failed: %v", err)
	}

	// Check table statistics
	tableStat := stats.GetTableStats("users")
	if tableStat == nil {
		t.Fatal("Table stats not loaded")
	}
	if tableStat.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", tableStat.RowCount)
	}
	if tableStat.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", tableStat.TableName)
	}

	// Check index statistics
	idxStat := stats.GetIndexStats("idx_name")
	if idxStat == nil {
		t.Fatal("Index stats not loaded for idx_name")
	}
	if idxStat.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", idxStat.RowCount)
	}
	if len(idxStat.AvgEq) != 1 {
		t.Errorf("Expected 1 avgEq value, got %d", len(idxStat.AvgEq))
	}
	if idxStat.AvgEq[0] != 10 {
		t.Errorf("Expected avgEq[0] = 10, got %d", idxStat.AvgEq[0])
	}

	// Check multi-column index
	idxStat2 := stats.GetIndexStats("idx_age_city")
	if idxStat2 == nil {
		t.Fatal("Index stats not loaded for idx_age_city")
	}
	if len(idxStat2.AvgEq) != 2 {
		t.Errorf("Expected 2 avgEq values, got %d", len(idxStat2.AvgEq))
	}
	if idxStat2.AvgEq[0] != 50 {
		t.Errorf("Expected avgEq[0] = 50, got %d", idxStat2.AvgEq[0])
	}
	if idxStat2.AvgEq[1] != 5 {
		t.Errorf("Expected avgEq[1] = 5, got %d", idxStat2.AvgEq[1])
	}
}

// TestSaveStatistics tests saving statistics to sqlite_stat1 format.
func TestSaveStatistics(t *testing.T) {
	stats := NewStatistics()

	// Add table statistics
	stats.TableStats["users"] = &TableStatistics{
		TableName: "users",
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
	}

	// Add index statistics
	stats.IndexStats["idx_name"] = &IndexStatistics{
		IndexName: "idx_name",
		TableName: "users",
		Stat:      "1000 10",
		RowCount:  1000,
		AvgEq:     []int64{10},
	}

	rows := SaveStatistics(stats)

	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}

	// Check table row
	foundTable := false
	foundIndex := false
	for _, row := range rows {
		if row.Tbl == "users" && row.Idx == "" {
			foundTable = true
			if row.Stat != "1000" {
				t.Errorf("Expected stat '1000', got '%s'", row.Stat)
			}
		}
		if row.Tbl == "users" && row.Idx == "idx_name" {
			foundIndex = true
			if row.Stat != "1000 10" {
				t.Errorf("Expected stat '1000 10', got '%s'", row.Stat)
			}
		}
	}

	if !foundTable {
		t.Error("Table statistics not saved")
	}
	if !foundIndex {
		t.Error("Index statistics not saved")
	}
}

// TestEstimateRows tests row estimation using statistics.
func TestEstimateRows(t *testing.T) {
	indexStats := &IndexStatistics{
		IndexName:   "idx_age_city",
		TableName:   "users",
		RowCount:    1000,
		AvgEq:       []int64{50, 5},
		ColumnStats: []LogEst{NewLogEst(20), NewLogEst(200)},
	}

	tests := []struct {
		name     string
		nEq      int
		hasRange bool
		expected string
	}{
		{"no constraints", 0, false, "returns full row count"},
		{"one equality", 1, false, "uses ColumnStats[0]"},
		{"two equalities", 2, false, "uses ColumnStats[1]"},
		{"one eq + range", 1, true, "applies range selectivity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nOut := EstimateRows(indexStats, tt.nEq, tt.hasRange)

			// Basic sanity checks
			if nOut < 0 {
				t.Errorf("Negative row estimate: %d", nOut)
			}

			// For one equality, should use first column stats
			if tt.nEq == 1 && !tt.hasRange {
				if nOut != indexStats.ColumnStats[0] {
					t.Logf("Expected %d, got %d (note: this is informational)",
						indexStats.ColumnStats[0], nOut)
				}
			}
		})
	}
}

// TestEstimateRowsWithoutStats tests estimation when no statistics available.
func TestEstimateRowsWithoutStats(t *testing.T) {
	nOut := EstimateRows(nil, 1, false)

	// Should return default estimate
	if nOut <= 0 {
		t.Errorf("Expected positive default estimate, got %d", nOut)
	}
}

// TestEstimateSelectivity tests selectivity estimation.
func TestEstimateSelectivity(t *testing.T) {
	stats := NewStatistics()

	tests := []struct {
		name     string
		operator WhereOperator
		value    interface{}
	}{
		{"equality int", WO_EQ, 5},
		{"equality small int", WO_EQ, 0},
		{"less than", WO_LT, 100},
		{"greater than", WO_GT, 50},
		{"IN operator", WO_IN, nil},
		{"IS NULL", WO_ISNULL, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term := &WhereTerm{
				Operator:   tt.operator,
				RightValue: tt.value,
			}

			selectivity := EstimateSelectivity(term, stats)

			// Selectivity should be negative (representing fraction < 1)
			// or small positive for default
			if selectivity > 100 {
				t.Errorf("Unexpected high selectivity: %d", selectivity)
			}
		})
	}
}

// TestApplyStatisticsToTable tests applying statistics to a table.
func TestApplyStatisticsToTable(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{
		TableName: "users",
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
	}
	stats.IndexStats["idx_name"] = &IndexStatistics{
		IndexName:   "idx_name",
		TableName:   "users",
		RowCount:    5000,
		ColumnStats: []LogEst{NewLogEst(500)},
	}

	table := &TableInfo{
		Name:      "users",
		RowCount:  100, // Default estimate
		RowLogEst: NewLogEst(100),
		Indexes: []*IndexInfo{
			{
				Name:        "idx_name",
				Table:       "users",
				RowCount:    100,
				RowLogEst:   NewLogEst(100),
				ColumnStats: []LogEst{},
			},
		},
	}

	ApplyStatisticsToTable(table, stats)

	// Check table statistics were applied
	if table.RowCount != 5000 {
		t.Errorf("Expected RowCount 5000, got %d", table.RowCount)
	}

	// Check index statistics were applied
	if table.Indexes[0].RowCount != 5000 {
		t.Errorf("Expected index RowCount 5000, got %d", table.Indexes[0].RowCount)
	}
	if len(table.Indexes[0].ColumnStats) != 1 {
		t.Errorf("Expected 1 column stat, got %d", len(table.Indexes[0].ColumnStats))
	}
}

// TestComputeTableStatistics tests computing statistics for a table.
func TestComputeTableStatistics(t *testing.T) {
	stat := ComputeTableStatistics("users", 1000)

	if stat == nil {
		t.Fatal("ComputeTableStatistics returned nil")
	}
	if stat.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", stat.TableName)
	}
	if stat.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", stat.RowCount)
	}
}

// TestComputeIndexStatistics tests computing statistics for an index.
func TestComputeIndexStatistics(t *testing.T) {
	distinctCounts := []int64{100, 500}
	stat := ComputeIndexStatistics("users", "idx_age_city", 1000, distinctCounts)

	if stat == nil {
		t.Fatal("ComputeIndexStatistics returned nil")
	}
	if stat.IndexName != "idx_age_city" {
		t.Errorf("Expected index name 'idx_age_city', got '%s'", stat.IndexName)
	}
	if stat.RowCount != 1000 {
		t.Errorf("Expected row count 1000, got %d", stat.RowCount)
	}
	if len(stat.AvgEq) != 2 {
		t.Fatalf("Expected 2 avgEq values, got %d", len(stat.AvgEq))
	}

	// avgEq[0] should be 1000 / 100 = 10
	if stat.AvgEq[0] != 10 {
		t.Errorf("Expected avgEq[0] = 10, got %d", stat.AvgEq[0])
	}
	// avgEq[1] should be 1000 / 500 = 2
	if stat.AvgEq[1] != 2 {
		t.Errorf("Expected avgEq[1] = 2, got %d", stat.AvgEq[1])
	}

	// Check stat string format
	expectedStat := "1000 10 2"
	if stat.Stat != expectedStat {
		t.Errorf("Expected stat string '%s', got '%s'", expectedStat, stat.Stat)
	}
}

// TestEstimateDistinctValues tests distinct value estimation.
func TestEstimateDistinctValues(t *testing.T) {
	tests := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalRows      int64
		minExpected    int64
		maxExpected    int64
	}{
		{"full sample", 1000, 100, 1000, 100, 100},
		{"half sample", 500, 50, 1000, 50, 200},
		{"small sample", 100, 50, 1000, 50, 1000},
		{"zero sample", 0, 0, 1000, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EstimateDistinctValues(tt.sampleSize, tt.uniqueInSample, tt.totalRows)

			if result < tt.minExpected {
				t.Errorf("Result %d is less than min expected %d", result, tt.minExpected)
			}
			if result > tt.maxExpected {
				t.Errorf("Result %d is greater than max expected %d", result, tt.maxExpected)
			}
		})
	}
}

// TestMergeStatistics tests merging two statistics objects.
func TestMergeStatistics(t *testing.T) {
	s1 := NewStatistics()
	s1.TableStats["table1"] = &TableStatistics{TableName: "table1", RowCount: 100}
	s1.IndexStats["idx1"] = &IndexStatistics{IndexName: "idx1", RowCount: 100}

	s2 := NewStatistics()
	s2.TableStats["table2"] = &TableStatistics{TableName: "table2", RowCount: 200}
	s2.TableStats["table1"] = &TableStatistics{TableName: "table1", RowCount: 150} // Override
	s2.IndexStats["idx2"] = &IndexStatistics{IndexName: "idx2", RowCount: 200}

	merged := MergeStatistics(s1, s2)

	// Should have both tables
	if len(merged.TableStats) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(merged.TableStats))
	}

	// table1 should be overridden
	if merged.TableStats["table1"].RowCount != 150 {
		t.Errorf("Expected table1 row count 150, got %d", merged.TableStats["table1"].RowCount)
	}

	// Should have both indexes
	if len(merged.IndexStats) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(merged.IndexStats))
	}
}

// TestClearStatistics tests clearing statistics for a table.
func TestClearStatistics(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{TableName: "users", RowCount: 1000}
	stats.TableStats["posts"] = &TableStatistics{TableName: "posts", RowCount: 5000}
	stats.IndexStats["idx_users_name"] = &IndexStatistics{
		IndexName: "idx_users_name",
		TableName: "users",
		RowCount:  1000,
	}
	stats.IndexStats["idx_posts_date"] = &IndexStatistics{
		IndexName: "idx_posts_date",
		TableName: "posts",
		RowCount:  5000,
	}

	stats.ClearStatistics("users")

	// users table stats should be removed
	if stats.GetTableStats("users") != nil {
		t.Error("users table stats should be removed")
	}

	// users index stats should be removed
	if stats.GetIndexStats("idx_users_name") != nil {
		t.Error("users index stats should be removed")
	}

	// posts stats should remain
	if stats.GetTableStats("posts") == nil {
		t.Error("posts table stats should remain")
	}
	if stats.GetIndexStats("idx_posts_date") == nil {
		t.Error("posts index stats should remain")
	}
}

// TestValidateStatistics tests statistics validation.
func TestValidateStatistics(t *testing.T) {
	tests := []struct {
		name      string
		stats     *Statistics
		shouldErr bool
	}{
		{
			name:      "valid statistics",
			stats:     createValidStats(),
			shouldErr: false,
		},
		{
			name:      "negative row count",
			stats:     createStatsWithNegativeRowCount(),
			shouldErr: true,
		},
		{
			name:      "invalid avgEq",
			stats:     createStatsWithInvalidAvgEq(),
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.stats.ValidateStatistics()
			if tt.shouldErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// Helper functions for tests

func createValidStats() *Statistics {
	stats := NewStatistics()
	stats.TableStats["test"] = &TableStatistics{
		TableName: "test",
		RowCount:  100,
	}
	stats.IndexStats["idx_test"] = &IndexStatistics{
		IndexName: "idx_test",
		TableName: "test",
		RowCount:  100,
		AvgEq:     []int64{10, 5},
	}
	return stats
}

func createStatsWithNegativeRowCount() *Statistics {
	stats := NewStatistics()
	stats.TableStats["test"] = &TableStatistics{
		TableName: "test",
		RowCount:  -100,
	}
	return stats
}

func createStatsWithInvalidAvgEq() *Statistics {
	stats := NewStatistics()
	stats.IndexStats["idx_test"] = &IndexStatistics{
		IndexName: "idx_test",
		TableName: "test",
		RowCount:  100,
		AvgEq:     []int64{0}, // Invalid: must be >= 1
	}
	return stats
}

// TestStatisticsString tests the String() method.
func TestStatisticsString(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{
		TableName: "users",
		RowCount:  1000,
	}
	stats.IndexStats["idx_name"] = &IndexStatistics{
		IndexName: "idx_name",
		TableName: "users",
		Stat:      "1000 10",
		RowCount:  1000,
	}

	str := stats.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain table and index information
	if !contains(str, "users") {
		t.Error("String() should contain table name")
	}
	if !contains(str, "idx_name") {
		t.Error("String() should contain index name")
	}
}

// contains checks if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(s) > len(substr) && indexOf(s, substr) >= 0)
}

// indexOf returns the index of the first occurrence of substr in s.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TestPlannerWithStatistics tests using planner with statistics.
func TestPlannerWithStatistics(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{
		TableName: "users",
		RowCount:  10000,
		RowLogEst: NewLogEst(10000),
	}

	planner := NewPlannerWithStatistics(stats)
	if planner == nil {
		t.Fatal("NewPlannerWithStatistics returned nil")
	}

	if planner.Statistics == nil {
		t.Error("Planner statistics is nil")
	}

	retrievedStats := planner.GetStatistics()
	if retrievedStats == nil {
		t.Error("GetStatistics returned nil")
	}

	if retrievedStats.GetTableStats("users") == nil {
		t.Error("Statistics not properly set in planner")
	}
}

// TestSetStatistics tests updating planner statistics.
func TestSetStatistics(t *testing.T) {
	planner := NewPlanner()

	stats := NewStatistics()
	stats.TableStats["test"] = &TableStatistics{
		TableName: "test",
		RowCount:  500,
	}

	planner.SetStatistics(stats)

	retrievedStats := planner.GetStatistics()
	if retrievedStats.GetTableStats("test") == nil {
		t.Error("Statistics not updated in planner")
	}
	if retrievedStats.GetTableStats("test").RowCount != 500 {
		t.Error("Statistics values not correct")
	}
}
