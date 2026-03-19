// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"strings"
	"testing"
)

func TestAnalyzeTable(t *testing.T) {
	schema := NewSchema()
	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
		RootPage: 1,
	}
	schema.AddTable(table)

	index := &Index{
		Name:    "idx_users_id",
		Table:   "users",
		Columns: []string{"id"},
	}
	schema.AddIndex(index)

	result, err := AnalyzeTable("users", schema)
	if err != nil {
		t.Fatalf("AnalyzeTable failed: %v", err)
	}

	if result.TableName != "users" {
		t.Errorf("TableName = %s, want users", result.TableName)
	}

	if result.TableStats == nil {
		t.Error("TableStats should not be nil")
	}

	if result.RowsScanned <= 0 {
		t.Error("RowsScanned should be > 0")
	}
}

func TestAnalyzeTableNotFound(t *testing.T) {
	schema := NewSchema()

	_, err := AnalyzeTable("nonexistent", schema)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestAnalyzeDatabase(t *testing.T) {
	schema := NewSchema()
	table := &Table{
		Name:       "test",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	}
	schema.AddTable(table)

	results, err := AnalyzeDatabase(schema)
	if err != nil {
		t.Fatalf("AnalyzeDatabase failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected results for at least one table")
	}
}

func TestCreateStat1Table(t *testing.T) {
	schema := NewSchema()

	err := CreateStat1Table(schema)
	if err != nil {
		t.Fatalf("CreateStat1Table failed: %v", err)
	}
}

func TestSaveAnalysisResults(t *testing.T) {
	result := &AnalyzeResult{
		TableName:  "users",
		TableStats: &TableAnalysis{TableName: "users", RowCount: 100},
		IndexStats: []*IndexAnalysis{
			{
				IndexName:      "idx_id",
				TableName:      "users",
				RowCount:       100,
				DistinctCounts: []int64{100},
				AvgEq:          []int64{1},
			},
		},
	}

	rows := SaveAnalysisResults([]*AnalyzeResult{result})
	if len(rows) == 0 {
		t.Error("Expected at least one row")
	}
}

func TestClearStatistics(t *testing.T) {
	schema := NewSchema()

	err := ClearStatistics("users", schema)
	if err != nil {
		t.Fatalf("ClearStatistics failed: %v", err)
	}
}

func TestExecuteAnalyze(t *testing.T) {
	schema := NewSchema()
	table := &Table{
		Name:       "test",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	}
	schema.AddTable(table)

	_, err := ExecuteAnalyze(schema, AnalyzeOptions{})
	if err != nil {
		t.Fatalf("ExecuteAnalyze failed: %v", err)
	}
}

func TestFormatStatRow(t *testing.T) {
	idxName := "idx_test"
	row := Stat1Row{
		Tbl:  "test",
		Idx:  &idxName,
		Stat: "100 50",
	}

	formatted := FormatStatRow(row)
	if formatted == "" {
		t.Error("Formatted stat row should not be empty")
	}
}

func TestEstimateDistinctValuesFromSample(t *testing.T) {
	tests := []struct {
		name        string
		sample      *IndexSample
		columnIndex int
		wantMin     int64
		wantMax     int64
	}{
		{
			name:        "nil sample",
			sample:      nil,
			columnIndex: 0,
			wantMin:     1,
			wantMax:     1,
		},
		{
			name: "empty sample",
			sample: &IndexSample{
				Values: [][]interface{}{},
				Count:  0,
			},
			columnIndex: 0,
			wantMin:     1,
			wantMax:     1,
		},
		{
			name: "sample with distinct values",
			sample: &IndexSample{
				Values: [][]interface{}{
					{1, "a"},
					{2, "b"},
					{3, "c"},
					{4, "d"},
				},
				Count: 100,
			},
			columnIndex: 0,
			wantMin:     4,
			wantMax:     200,
		},
		{
			name: "sample with duplicate values",
			sample: &IndexSample{
				Values: [][]interface{}{
					{1, "a"},
					{1, "b"},
					{2, "c"},
					{2, "d"},
				},
				Count: 50,
			},
			columnIndex: 0,
			wantMin:     2,
			wantMax:     100,
		},
		{
			name: "multi-column sample",
			sample: &IndexSample{
				Values: [][]interface{}{
					{1, "a", "x"},
					{1, "a", "y"},
					{1, "b", "z"},
					{2, "a", "w"},
				},
				Count: 80,
			},
			columnIndex: 1,
			wantMin:     2,
			wantMax:     100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EstimateDistinctValuesFromSample(tt.sample, tt.columnIndex)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("EstimateDistinctValuesFromSample() = %d, want between %d and %d", got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestBuildKeyFromValues(t *testing.T) {
	tests := []struct {
		name      string
		values    []interface{}
		upToIndex int
		want      string
	}{
		{
			name:      "single value",
			values:    []interface{}{1},
			upToIndex: 0,
			want:      "1",
		},
		{
			name:      "multiple values",
			values:    []interface{}{1, "hello", 3.14},
			upToIndex: 1,
			want:      "1|hello",
		},
		{
			name:      "all values",
			values:    []interface{}{1, "hello", 3.14},
			upToIndex: 2,
			want:      "1|hello|3.14",
		},
		{
			name:      "index beyond length",
			values:    []interface{}{1, 2},
			upToIndex: 10,
			want:      "1|2",
		},
		{
			name:      "empty values",
			values:    []interface{}{},
			upToIndex: 0,
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildKeyFromValues(tt.values, tt.upToIndex)
			if got != tt.want {
				t.Errorf("buildKeyFromValues() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEstimateDistinct(t *testing.T) {
	tests := []struct {
		name           string
		sampleSize     int64
		uniqueInSample int64
		totalCount     int64
		wantMin        int64
		wantMax        int64
	}{
		{
			name:           "zero sample size",
			sampleSize:     0,
			uniqueInSample: 0,
			totalCount:     100,
			wantMin:        1,
			wantMax:        1,
		},
		{
			name:           "zero unique",
			sampleSize:     10,
			uniqueInSample: 0,
			totalCount:     100,
			wantMin:        1,
			wantMax:        1,
		},
		{
			name:           "sample equals total",
			sampleSize:     100,
			uniqueInSample: 50,
			totalCount:     100,
			wantMin:        50,
			wantMax:        50,
		},
		{
			name:           "sample larger than total",
			sampleSize:     150,
			uniqueInSample: 50,
			totalCount:     100,
			wantMin:        50,
			wantMax:        50,
		},
		{
			name:           "small sample with correction",
			sampleSize:     5,
			uniqueInSample: 4,
			totalCount:     100,
			wantMin:        4,
			wantMax:        100,
		},
		{
			name:           "large sample no correction",
			sampleSize:     50,
			uniqueInSample: 40,
			totalCount:     100,
			wantMin:        40,
			wantMax:        100,
		},
		{
			name:           "result bounded by total",
			sampleSize:     10,
			uniqueInSample: 10,
			totalCount:     50,
			wantMin:        10,
			wantMax:        50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateDistinct(tt.sampleSize, tt.uniqueInSample, tt.totalCount)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("estimateDistinct(%d, %d, %d) = %d, want between %d and %d",
					tt.sampleSize, tt.uniqueInSample, tt.totalCount, got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestAnalyzeReportString(t *testing.T) {
	report := &AnalyzeReport{
		TablesAnalyzed:  3,
		IndexesAnalyzed: 5,
		RowsScanned:     1000,
	}

	str := report.String()
	if str == "" {
		t.Error("AnalyzeReport.String() should not return empty string")
	}
}

// Test analyzeTableIndexes with no indexes
func TestAnalyzeTableIndexesEmpty(t *testing.T) {
	schema := NewSchema()
	table := &Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	}

	results := analyzeTableIndexes(table, 1000, schema)
	if len(results) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(results))
	}
}

// Test analyzeIndex with non-unique index
func TestAnalyzeIndexNonUnique(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 2,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "name", DeclType: "TEXT"},
		},
		RootPage: 1,
	}

	index := &Index{
		Name:    "idx_name",
		Table:   "users",
		Columns: []string{"name"},
		Unique:  false,
	}

	result := analyzeIndex(table, index, 1000)

	if result == nil {
		t.Fatal("analyzeIndex returned nil")
	}

	if result.IndexName != "idx_name" {
		t.Errorf("IndexName = %s, want idx_name", result.IndexName)
	}

	if result.RowCount != 1000 {
		t.Errorf("RowCount = %d, want 1000", result.RowCount)
	}

	// Check that it's not using rowCount for non-unique
	if len(result.DistinctCounts) > 0 && result.DistinctCounts[0] == 1000 {
		t.Error("Non-unique index should not have distinctCount == rowCount")
	}
}

// Test analyzeIndex with unique multi-column index
func TestAnalyzeIndexUniqueMultiColumn(t *testing.T) {
	table := &Table{
		Name:       "users",
		NumColumns: 3,
		Columns: []Column{
			{Name: "id", DeclType: "INTEGER"},
			{Name: "email", DeclType: "TEXT"},
			{Name: "phone", DeclType: "TEXT"},
		},
		RootPage: 1,
	}

	index := &Index{
		Name:    "idx_unique",
		Table:   "users",
		Columns: []string{"email", "phone"},
		Unique:  true,
	}

	result := analyzeIndex(table, index, 1000)

	if result == nil {
		t.Fatal("analyzeIndex returned nil")
	}

	// For unique index, last column should have rowCount distinct values
	if len(result.DistinctCounts) != 2 {
		t.Errorf("Expected 2 distinct counts, got %d", len(result.DistinctCounts))
	}

	lastIdx := len(result.DistinctCounts) - 1
	if result.DistinctCounts[lastIdx] != 1000 {
		t.Errorf("Last column distinctCount = %d, want 1000 for unique index", result.DistinctCounts[lastIdx])
	}
}

// Test analyzeIndex with zero divisor edge case
func TestAnalyzeIndexZeroDivisor(t *testing.T) {
	table := &Table{
		Name:       "small",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	}

	index := &Index{
		Name:    "idx_id",
		Table:   "small",
		Columns: []string{"id"},
		Unique:  false,
	}

	// Use a very small rowCount
	result := analyzeIndex(table, index, 5)

	if result == nil {
		t.Fatal("analyzeIndex returned nil")
	}

	// DistinctCounts should be at least 1
	if len(result.DistinctCounts) > 0 && result.DistinctCounts[0] < 1 {
		t.Error("DistinctCount should be at least 1")
	}

	// AvgEq should be at least 1
	if len(result.AvgEq) > 0 && result.AvgEq[0] < 1 {
		t.Error("AvgEq should be at least 1")
	}
}

// Test buildStatString with empty avgEq
func TestBuildStatStringEmpty(t *testing.T) {
	result := buildStatString(1000, []int64{})
	if result != "1000" {
		t.Errorf("buildStatString with empty avgEq = %q, want \"1000\"", result)
	}
}

// Test buildStatString with multiple values
func TestBuildStatStringMultiple(t *testing.T) {
	result := buildStatString(1000, []int64{100, 50, 25})
	expected := "1000 100 50 25"
	if result != expected {
		t.Errorf("buildStatString = %q, want %q", result, expected)
	}
}

// Test joinStrings with empty slice
func TestJoinStringsEmpty(t *testing.T) {
	result := joinStrings([]string{}, " ")
	if result != "" {
		t.Errorf("joinStrings with empty slice = %q, want \"\"", result)
	}
}

// Test joinStrings with single element
func TestJoinStringsSingle(t *testing.T) {
	result := joinStrings([]string{"hello"}, " ")
	if result != "hello" {
		t.Errorf("joinStrings with single element = %q, want \"hello\"", result)
	}
}

// Test joinStrings with multiple elements
func TestJoinStringsMultiple(t *testing.T) {
	result := joinStrings([]string{"a", "b", "c"}, "|")
	if result != "a|b|c" {
		t.Errorf("joinStrings = %q, want \"a|b|c\"", result)
	}
}

// Test AnalyzeDatabase with system tables
func TestAnalyzeDatabaseSkipsSystemTables(t *testing.T) {
	schema := NewSchema()

	// Add regular table
	schema.AddTable(&Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   2,
	})

	// Add system tables (should be skipped)
	schema.AddTable(&Table{
		Name:       "sqlite_master",
		NumColumns: 5,
		Columns:    []Column{{Name: "type", DeclType: "TEXT"}},
		RootPage:   1,
	})

	schema.AddTable(&Table{
		Name:       "sqlite_stat1",
		NumColumns: 3,
		Columns:    []Column{{Name: "tbl", DeclType: "TEXT"}},
		RootPage:   3,
	})

	results, err := AnalyzeDatabase(schema)
	if err != nil {
		t.Fatalf("AnalyzeDatabase failed: %v", err)
	}

	// Should only analyze "users", not system tables
	if len(results) != 1 {
		t.Errorf("Expected 1 result (users only), got %d", len(results))
	}

	if len(results) > 0 && results[0].TableName != "users" {
		t.Errorf("Expected to analyze 'users', got '%s'", results[0].TableName)
	}
}

// Test AnalyzeDatabase with multiple tables
func TestAnalyzeDatabaseMultipleTables(t *testing.T) {
	schema := NewSchema()

	tables := []string{"aaa", "zzz", "mmm"} // Not alphabetical
	for _, name := range tables {
		schema.AddTable(&Table{
			Name:       name,
			NumColumns: 1,
			Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
			RootPage:   1,
		})
	}

	results, err := AnalyzeDatabase(schema)
	if err != nil {
		t.Fatalf("AnalyzeDatabase failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	// Results should be in alphabetical order
	if len(results) == 3 {
		if results[0].TableName != "aaa" {
			t.Errorf("First table = %s, want aaa", results[0].TableName)
		}
		if results[1].TableName != "mmm" {
			t.Errorf("Second table = %s, want mmm", results[1].TableName)
		}
		if results[2].TableName != "zzz" {
			t.Errorf("Third table = %s, want zzz", results[2].TableName)
		}
	}
}

// Test SaveAnalysisResults with nil TableStats
func TestSaveAnalysisResultsNilTableStats(t *testing.T) {
	result := &AnalyzeResult{
		TableName:  "users",
		TableStats: nil, // nil TableStats
		IndexStats: []*IndexAnalysis{
			{
				IndexName:  "idx_id",
				TableName:  "users",
				StatString: "100 50",
			},
		},
	}

	rows := SaveAnalysisResults([]*AnalyzeResult{result})

	// Should only have index stats, not table stats
	if len(rows) != 1 {
		t.Errorf("Expected 1 row (index only), got %d", len(rows))
	}

	if len(rows) > 0 && rows[0].Idx == nil {
		t.Error("Expected index row, not table row")
	}
}

// Test SaveAnalysisResults with empty IndexStats
func TestSaveAnalysisResultsEmptyIndexStats(t *testing.T) {
	result := &AnalyzeResult{
		TableName: "users",
		TableStats: &TableAnalysis{
			TableName: "users",
			RowCount:  100,
		},
		IndexStats: []*IndexAnalysis{}, // Empty
	}

	rows := SaveAnalysisResults([]*AnalyzeResult{result})

	// Should only have table stats
	if len(rows) != 1 {
		t.Errorf("Expected 1 row (table only), got %d", len(rows))
	}

	if len(rows) > 0 && rows[0].Idx != nil {
		t.Error("Expected table row with nil Idx")
	}
}

// Test CreateStat1Table when already exists
func TestCreateStat1TableAlreadyExists(t *testing.T) {
	schema := NewSchema()

	// Add sqlite_stat1 first
	schema.AddTable(&Table{
		Name:       "sqlite_stat1",
		NumColumns: 3,
		Columns:    []Column{{Name: "tbl", DeclType: "TEXT"}},
		RootPage:   1,
	})

	err := CreateStat1Table(schema)
	if err != nil {
		t.Errorf("CreateStat1Table should not error when table exists: %v", err)
	}
}

// Test ExecuteAnalyze with specific table
func TestExecuteAnalyzeSpecificTable(t *testing.T) {
	schema := NewSchema()

	schema.AddTable(&Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	})

	schema.AddTable(&Table{
		Name:       "posts",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   2,
	})

	opts := AnalyzeOptions{
		TableName: "users",
	}

	report, err := ExecuteAnalyze(schema, opts)
	if err != nil {
		t.Fatalf("ExecuteAnalyze failed: %v", err)
	}

	// Should only analyze "users"
	if report.TablesAnalyzed != 1 {
		t.Errorf("TablesAnalyzed = %d, want 1", report.TablesAnalyzed)
	}
}

// Test ExecuteAnalyze with error on specific table
func TestExecuteAnalyzeSpecificTableError(t *testing.T) {
	schema := NewSchema()

	opts := AnalyzeOptions{
		TableName: "nonexistent",
	}

	_, err := ExecuteAnalyze(schema, opts)
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

// Test ExecuteAnalyze all tables
func TestExecuteAnalyzeAllTables(t *testing.T) {
	schema := NewSchema()

	schema.AddTable(&Table{
		Name:       "users",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	})

	schema.AddTable(&Table{
		Name:       "posts",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   2,
	})

	opts := AnalyzeOptions{
		TableName: "", // Empty means all tables
	}

	report, err := ExecuteAnalyze(schema, opts)
	if err != nil {
		t.Fatalf("ExecuteAnalyze failed: %v", err)
	}

	if report.TablesAnalyzed != 2 {
		t.Errorf("TablesAnalyzed = %d, want 2", report.TablesAnalyzed)
	}

	if report.RowsScanned <= 0 {
		t.Error("RowsScanned should be > 0")
	}

	if len(report.Stat1Rows) == 0 {
		t.Error("Should have stat1 rows")
	}
}

// Test FormatStatRow with nil index
func TestFormatStatRowNilIndex(t *testing.T) {
	row := Stat1Row{
		Tbl:  "users",
		Idx:  nil,
		Stat: "100",
	}

	formatted := FormatStatRow(row)
	if !strings.Contains(formatted, "NULL") {
		t.Errorf("FormatStatRow should contain NULL for nil Idx, got: %s", formatted)
	}
	if !strings.Contains(formatted, "users") {
		t.Errorf("FormatStatRow should contain table name, got: %s", formatted)
	}
}

// Test FormatStatRow with index name
func TestFormatStatRowWithIndex(t *testing.T) {
	idxName := "idx_users_id"
	row := Stat1Row{
		Tbl:  "users",
		Idx:  &idxName,
		Stat: "100 50",
	}

	formatted := FormatStatRow(row)
	if !strings.Contains(formatted, "idx_users_id") {
		t.Errorf("FormatStatRow should contain index name, got: %s", formatted)
	}
	if !strings.Contains(formatted, "users") {
		t.Errorf("FormatStatRow should contain table name, got: %s", formatted)
	}
}

// Test buildKeyFromValues with negative index
func TestBuildKeyFromValuesNegativeIndex(t *testing.T) {
	values := []interface{}{1, "hello", 3.14}
	key := buildKeyFromValues(values, -1)
	// Should handle gracefully - result depends on implementation
	_ = key
}

// Test analyzeIndex with zero distinctCount edge case
func TestAnalyzeIndexZeroDistinct(t *testing.T) {
	table := &Table{
		Name:       "test",
		NumColumns: 1,
		Columns:    []Column{{Name: "id", DeclType: "INTEGER"}},
		RootPage:   1,
	}

	index := &Index{
		Name:    "idx_test",
		Table:   "test",
		Columns: []string{"id"},
		Unique:  false,
	}

	// Use rowCount = 0 to test edge case
	result := analyzeIndex(table, index, 0)

	if result == nil {
		t.Fatal("analyzeIndex returned nil")
	}

	// Check that we handle zero rowCount gracefully
	if len(result.AvgEq) > 0 {
		for _, avg := range result.AvgEq {
			if avg < 0 {
				t.Error("AvgEq should not be negative")
			}
		}
	}
}
