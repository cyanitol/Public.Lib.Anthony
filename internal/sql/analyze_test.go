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
