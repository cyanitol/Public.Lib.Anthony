// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"
)

// Comprehensive tests for planner.go functions

func TestNewPlanner(t *testing.T) {
	p := NewPlanner()
	if p == nil {
		t.Fatal("NewPlanner() returned nil")
	}
	if p.CostModel == nil {
		t.Error("CostModel is nil")
	}
	if p.SubqueryOptimizer == nil {
		t.Error("SubqueryOptimizer is nil")
	}
	if p.Statistics == nil {
		t.Error("Statistics is nil")
	}
}

func TestNewPlannerWithStatistics(t *testing.T) {
	stats := NewStatistics()
	stats.TableStats["users"] = &TableStatistics{RowCount: 1000}

	p := NewPlannerWithStatistics(stats)
	if p == nil {
		t.Fatal("NewPlannerWithStatistics() returned nil")
	}
	if p.Statistics == nil {
		t.Error("Statistics is nil")
	}
	if p.Statistics.TableStats["users"] == nil {
		t.Error("Statistics were not set correctly")
	}
}

func TestPlannerGetSetStatistics(t *testing.T) {
	p := NewPlanner()
	stats := NewStatistics()
	stats.TableStats["test"] = &TableStatistics{RowCount: 500}

	p.SetStatistics(stats)
	retrieved := p.GetStatistics()

	if retrieved == nil {
		t.Fatal("GetStatistics() returned nil")
	}
	if retrieved.TableStats["test"] == nil {
		t.Error("Statistics were not set correctly")
	}
	if retrieved.TableStats["test"].RowCount != 500 {
		t.Errorf("Expected row count 500, got %d", retrieved.TableStats["test"].RowCount)
	}
}

func TestPlannerGetSetCTEContext(t *testing.T) {
	p := NewPlanner()

	// Initially nil
	if p.GetCTEContext() != nil {
		t.Error("Initial CTE context should be nil")
	}

	ctx := &CTEContext{CTEs: make(map[string]*CTEDefinition)}
	ctx.CTEs["test"] = &CTEDefinition{Name: "test"}

	p.SetCTEContext(ctx)
	retrieved := p.GetCTEContext()

	if retrieved == nil {
		t.Fatal("GetCTEContext() returned nil")
	}
	if retrieved.CTEs["test"] == nil {
		t.Error("CTE context was not set correctly")
	}
}

func TestPlanQueryNoTables(t *testing.T) {
	p := NewPlanner()
	_, err := p.PlanQuery([]*TableInfo{}, nil)

	if err == nil {
		t.Error("Expected error for query with no tables")
	}
	if err.Error() != "no tables in query" {
		t.Errorf("Expected 'no tables in query', got %v", err)
	}
}

func TestPlanQuerySingleTableFullScan(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	info, err := p.PlanQuery([]*TableInfo{table}, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info == nil {
		t.Fatal("Plan info is nil")
	}
	if info.BestPath == nil {
		t.Fatal("BestPath is nil")
	}
	if len(info.BestPath.Loops) != 1 {
		t.Errorf("Expected 1 loop, got %d", len(info.BestPath.Loops))
	}
}

func TestPlanQueryWithWhereClause(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	// Create simple WHERE clause
	whereClause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Expr:        &BinaryExpr{Op: "=", Left: &ColumnExpr{Cursor: 0, Column: "id"}, Right: &ValueExpr{Value: 10}},
				Operator:    WO_EQ,
				LeftCursor:  0,
				LeftColumn:  0,
				RightValue:  10,
				PrereqRight: 0,
				PrereqAll:   Bitmask(1),
				TruthProb:   selectivityEq,
			},
		},
	}

	info, err := p.PlanQuery([]*TableInfo{table}, whereClause)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info == nil {
		t.Fatal("Plan info is nil")
	}
	if info.Clause != whereClause {
		t.Error("WHERE clause not set in info")
	}
}

func TestSplitAnd(t *testing.T) {
	p := NewPlanner()

	tests := []struct {
		name     string
		expr     Expr
		expected int
	}{
		{
			name:     "single term",
			expr:     &BinaryExpr{Op: "="},
			expected: 1,
		},
		{
			name: "two AND terms",
			expr: &AndExpr{Terms: []Expr{
				&BinaryExpr{Op: "="},
				&BinaryExpr{Op: ">"},
			}},
			expected: 2,
		},
		{
			name: "nested AND",
			expr: &AndExpr{Terms: []Expr{
				&BinaryExpr{Op: "="},
				&AndExpr{Terms: []Expr{
					&BinaryExpr{Op: ">"},
					&BinaryExpr{Op: "<"},
				}},
			}},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terms := p.splitAnd(tt.expr)
			if len(terms) != tt.expected {
				t.Errorf("Expected %d terms, got %d", tt.expected, len(terms))
			}
		})
	}
}

func assertAnalyzeExprError(t *testing.T, err error, wantErr bool) {
	t.Helper()
	if wantErr && err == nil {
		t.Error("Expected error but got none")
	}
	if !wantErr && err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func assertAnalyzeExprResult(t *testing.T, term *WhereTerm, err error, wantErr, wantNil bool) {
	t.Helper()
	assertAnalyzeExprError(t, err, wantErr)
	if wantNil && term != nil {
		t.Error("Expected nil term but got non-nil")
	}
	if !wantNil && !wantErr && term == nil {
		t.Error("Expected non-nil term but got nil")
	}
}

func TestAnalyzeExpr(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()
	tables := []*TableInfo{table}

	tests := []struct {
		name    string
		expr    Expr
		wantErr bool
		wantNil bool
	}{
		{"equality expression", &BinaryExpr{Op: "=", Left: &ColumnExpr{Cursor: 0, Column: "id"}, Right: &ValueExpr{Value: 5}}, false, false},
		{"range expression", &BinaryExpr{Op: ">", Left: &ColumnExpr{Cursor: 0, Column: "age"}, Right: &ValueExpr{Value: 18}}, false, false},
		{"OR expression", &OrExpr{Terms: []Expr{
			&BinaryExpr{Op: "=", Left: &ColumnExpr{}, Right: &ValueExpr{}},
			&BinaryExpr{Op: "=", Left: &ColumnExpr{}, Right: &ValueExpr{}},
		}}, false, false},
		{"non-binary expression", &ValueExpr{Value: 42}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term, err := p.analyzeExpr(tt.expr, tables)
			assertAnalyzeExprResult(t, term, err, tt.wantErr, tt.wantNil)
		})
	}
}

func TestParseOperator(t *testing.T) {
	p := NewPlanner()

	tests := []struct {
		op       string
		expected WhereOperator
	}{
		{"=", WO_EQ},
		{"<", WO_LT},
		{"<=", WO_LE},
		{">", WO_GT},
		{">=", WO_GE},
		{"IN", WO_IN},
		{"IS", WO_IS},
		{"IS NULL", WO_ISNULL},
		{"UNKNOWN", 0},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			result := p.parseOperator(tt.op)
			if result != tt.expected {
				t.Errorf("parseOperator(%s) = %v, want %v", tt.op, result, tt.expected)
			}
		})
	}
}

func TestExplainPlanComprehensive(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	info, err := p.PlanQuery([]*TableInfo{table}, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	explanation := p.ExplainPlan(info)
	if explanation == "" {
		t.Error("Explanation is empty")
	}
	if explanation == "No plan available" {
		t.Error("Expected valid plan explanation")
	}
}

func TestExplainPlanNoBestPath(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{}

	explanation := p.ExplainPlan(info)
	if explanation != "No plan available" {
		t.Errorf("Expected 'No plan available', got %s", explanation)
	}
}

func TestValidatePlanComprehensive(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	info, err := p.PlanQuery([]*TableInfo{table}, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	err = p.ValidatePlan(info)
	if err != nil {
		t.Errorf("ValidatePlan failed: %v", err)
	}
}

func TestValidatePlanNoBestPath(t *testing.T) {
	p := NewPlanner()
	info := &WhereInfo{}

	err := p.ValidatePlan(info)
	if err == nil {
		t.Error("Expected error for info with no BestPath")
	}
}

func TestValidatePlanWrongLoopCount(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	info := &WhereInfo{
		Tables: []*TableInfo{table, table}, // 2 tables
		BestPath: &WherePath{
			Loops: []*WhereLoop{
				{TabIndex: 0, MaskSelf: 1, NOut: 100, Run: 100},
			}, // Only 1 loop
		},
	}

	err := p.ValidatePlan(info)
	if err == nil {
		t.Error("Expected error for mismatched loop count")
	}
}

func TestValidatePlanDuplicateTable(t *testing.T) {
	p := NewPlanner()
	table := createTestTable()

	info := &WhereInfo{
		Tables: []*TableInfo{table},
		BestPath: &WherePath{
			Loops: []*WhereLoop{
				{TabIndex: 0, MaskSelf: 1, NOut: 100, Run: 100},
				{TabIndex: 0, MaskSelf: 1, NOut: 100, Run: 100}, // Duplicate
			},
		},
	}

	err := p.ValidatePlan(info)
	if err == nil {
		t.Error("Expected error for duplicate table")
	}
}

func TestFindBestMultiTable(t *testing.T) {
	p := NewPlanner()
	table1 := createTestTable()
	table1.Cursor = 0
	table2 := &TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
		Columns: []ColumnInfo{
			{Name: "order_id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
		},
		Indexes: []*IndexInfo{},
	}

	info := &WhereInfo{
		Tables: []*TableInfo{table1, table2},
		AllLoops: []*WhereLoop{
			{TabIndex: 0, MaskSelf: 1, NOut: 100, Run: 100},
			{TabIndex: 1, MaskSelf: 2, NOut: 50, Run: 50},
		},
	}

	path, err := p.findBestMultiTable(info)
	if err != nil {
		t.Fatalf("findBestMultiTable failed: %v", err)
	}
	if path == nil {
		t.Fatal("Expected non-nil path")
	}
	if len(path.Loops) != 2 {
		t.Errorf("Expected 2 loops, got %d", len(path.Loops))
	}
}

func TestSelectBestPaths(t *testing.T) {
	p := NewPlanner()

	paths := []*WherePath{
		{Cost: 1000, NRow: 100},
		{Cost: 500, NRow: 50},
		{Cost: 2000, NRow: 200},
		{Cost: 300, NRow: 30},
		{Cost: 1500, NRow: 150},
		{Cost: 800, NRow: 80},
	}

	// Select top 3
	best := p.selectBestPaths(paths, 3)
	if len(best) != 3 {
		t.Errorf("Expected 3 paths, got %d", len(best))
	}

	// Verify they're sorted by cost
	for i := 1; i < len(best); i++ {
		if best[i].Cost < best[i-1].Cost {
			t.Error("Paths not sorted by cost")
		}
	}

	// Verify the lowest cost path is first
	if best[0].Cost != 300 {
		t.Errorf("Expected lowest cost 300, got %d", best[0].Cost)
	}
}

func TestDetectSubquery(t *testing.T) {
	p := NewPlanner()

	tests := []struct {
		name     string
		expr     Expr
		expected bool
	}{
		{
			name:     "nil expression",
			expr:     nil,
			expected: false,
		},
		{
			name:     "subquery expression",
			expr:     &SubqueryExpr{Type: SubqueryIn, Query: &ValueExpr{}},
			expected: true,
		},
		{
			name: "IN operator",
			expr: &BinaryExpr{
				Op:    "IN",
				Left:  &ColumnExpr{},
				Right: &ValueExpr{},
			},
			expected: true,
		},
		{
			name: "regular binary expression",
			expr: &BinaryExpr{
				Op:    "=",
				Left:  &ColumnExpr{},
				Right: &ValueExpr{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := p.detectSubquery(tt.expr)
			if (result != nil) != tt.expected {
				t.Errorf("detectSubquery() = %v, want %v", result != nil, tt.expected)
			}
		})
	}
}

func TestMakeIndent(t *testing.T) {
	tests := []struct {
		level    int
		expected string
	}{
		{0, ""},
		{1, "  "},
		{2, "    "},
		{3, "      "},
	}

	for _, tt := range tests {
		result := makeIndent(tt.level)
		if result != tt.expected {
			t.Errorf("makeIndent(%d) = %q, want %q", tt.level, result, tt.expected)
		}
	}
}

func TestJoinStrings(t *testing.T) {
	tests := []struct {
		name     string
		strs     []string
		sep      string
		expected string
	}{
		{
			name:     "empty slice",
			strs:     []string{},
			sep:      ",",
			expected: "",
		},
		{
			name:     "single string",
			strs:     []string{"hello"},
			sep:      ",",
			expected: "hello",
		},
		{
			name:     "multiple strings",
			strs:     []string{"a", "b", "c"},
			sep:      ",",
			expected: "a,b,c",
		},
		{
			name:     "different separator",
			strs:     []string{"a", "b", "c"},
			sep:      " AND ",
			expected: "a AND b AND c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := joinStrings(tt.strs, tt.sep)
			if result != tt.expected {
				t.Errorf("joinStrings() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestOperatorString(t *testing.T) {
	tests := []struct {
		op       WhereOperator
		expected string
	}{
		{WO_EQ, "="},
		{WO_LT, "<"},
		{WO_LE, "<="},
		{WO_GT, ">"},
		{WO_GE, ">="},
		{WO_IN, " IN "},
	}

	for _, tt := range tests {
		result := operatorString(tt.op)
		if result != tt.expected {
			t.Errorf("operatorString(%v) = %q, want %q", tt.op, result, tt.expected)
		}
	}
}

func TestPlanQueryMultipleTables(t *testing.T) {
	p := NewPlanner()

	table1 := createTestTable()
	table1.Cursor = 0

	table2 := &TableInfo{
		Name:      "orders",
		Cursor:    1,
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
		Columns: []ColumnInfo{
			{Name: "order_id", Index: 0, Type: "INTEGER"},
			{Name: "user_id", Index: 1, Type: "INTEGER"},
		},
		Indexes: []*IndexInfo{},
	}

	info, err := p.PlanQuery([]*TableInfo{table1, table2}, nil)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}

	if info == nil {
		t.Fatal("Plan info is nil")
	}
	if info.BestPath == nil {
		t.Fatal("BestPath is nil")
	}
	if len(info.BestPath.Loops) != 2 {
		t.Errorf("Expected 2 loops for 2 tables, got %d", len(info.BestPath.Loops))
	}
}

func TestAnalyzeOrExpr(t *testing.T) {
	p := NewPlanner()
	tables := []*TableInfo{createTestTable()}

	orExpr := &OrExpr{
		Terms: []Expr{
			&BinaryExpr{Op: "=", Left: &ColumnExpr{}, Right: &ValueExpr{}},
			&BinaryExpr{Op: ">", Left: &ColumnExpr{}, Right: &ValueExpr{}},
		},
	}

	term, err := p.analyzeOrExpr(orExpr, tables)
	if err != nil {
		t.Fatalf("analyzeOrExpr failed: %v", err)
	}
	if term == nil {
		t.Fatal("Expected non-nil term")
	}
	if term.Operator != WO_OR {
		t.Errorf("Expected WO_OR operator, got %v", term.Operator)
	}
}

func TestBuildEquivalenceClasses(t *testing.T) {
	p := NewPlanner()

	clause := &WhereClause{
		Terms: []*WhereTerm{
			{
				Expr: &BinaryExpr{
					Op:    "=",
					Left:  &ColumnExpr{Cursor: 0, Column: "a"},
					Right: &ColumnExpr{Cursor: 0, Column: "b"},
				},
				Operator:   WO_EQ,
				LeftCursor: 0,
				LeftColumn: 0,
			},
		},
	}

	equiv := p.buildEquivalenceClasses(clause)
	if len(equiv) == 0 {
		t.Error("Expected equivalence classes to be built")
	}
}
