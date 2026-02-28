package sql

import (
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
		name             string
		sampleSize       int64
		uniqueInSample   int64
		totalCount       int64
		wantMin          int64
		wantMax          int64
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
