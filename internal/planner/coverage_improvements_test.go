// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// Tests for CTE expression handling functions

func TestHandleSubqueryExpr(t *testing.T) {
	ctx := &CTEContext{
		CTEs:        make(map[string]*CTEDefinition),
		IsRecursive: false,
	}

	// Add a CTE definition
	ctx.CTEs["test_cte"] = &CTEDefinition{
		Name: "test_cte",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}

	// Create a subquery expression that references the CTE
	subquery := &parser.SubqueryExpr{
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "test_cte"},
				},
			},
		},
	}

	deps := make(map[string]bool)
	ctx.handleSubqueryExpr(subquery, deps)

	if !deps["test_cte"] {
		t.Error("handleSubqueryExpr should have detected test_cte dependency")
	}
}

func TestHandleCaseExpr(t *testing.T) {
	ctx := &CTEContext{
		CTEs:        make(map[string]*CTEDefinition),
		IsRecursive: false,
	}

	ctx.CTEs["cte1"] = &CTEDefinition{Name: "cte1"}
	ctx.CTEs["cte2"] = &CTEDefinition{Name: "cte2"}

	// Create a CASE expression that references CTEs in subqueries
	caseExpr := &parser.CaseExpr{
		Expr: &parser.SubqueryExpr{
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "cte1"},
					},
				},
			},
		},
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.BinaryExpr{
					Left:  &parser.IdentExpr{Name: "id"},
					Op:    parser.OpEq,
					Right: &parser.LiteralExpr{Value: "1"},
				},
				Result: &parser.LiteralExpr{Value: "active"},
			},
		},
		ElseClause: &parser.SubqueryExpr{
			Select: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "cte2"},
					},
				},
			},
		},
	}

	deps := make(map[string]bool)
	ctx.handleCaseExpr(caseExpr, deps)

	if !deps["cte1"] {
		t.Error("handleCaseExpr should have detected cte1 dependency")
	}
	if !deps["cte2"] {
		t.Error("handleCaseExpr should have detected cte2 dependency")
	}
}

func TestHandleCaseExprNoElse(t *testing.T) {
	ctx := &CTEContext{
		CTEs:        make(map[string]*CTEDefinition),
		IsRecursive: false,
	}

	caseExpr := &parser.CaseExpr{
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.BinaryExpr{
					Left:  &parser.IdentExpr{Name: "id"},
					Op:    parser.OpGt,
					Right: &parser.LiteralExpr{Value: "100"},
				},
				Result: &parser.LiteralExpr{Value: "high"},
			},
		},
		ElseClause: nil,
	}

	deps := make(map[string]bool)
	ctx.handleCaseExpr(caseExpr, deps)

	// Should not panic with nil ElseClause
	if len(deps) != 0 {
		t.Error("no dependencies should be found")
	}
}

func TestCheckLevelCircularity(t *testing.T) {
	ctx := &CTEContext{
		CTEs: make(map[string]*CTEDefinition),
	}

	// Create a circular dependency: A -> B -> A
	defA := &CTEDefinition{
		Name:      "A",
		DependsOn: []string{"B"},
		Level:     0,
	}
	ctx.CTEs["A"] = defA
	ctx.CTEs["B"] = &CTEDefinition{
		Name:      "B",
		DependsOn: []string{"A"},
		Level:     0,
	}

	visiting := map[string]bool{"A": true}
	err := ctx.checkLevelCircularity("A", defA, visiting)
	if err == nil {
		t.Error("checkLevelCircularity should detect circular dependency")
	}
}

func TestCheckLevelCircularityNoCycle(t *testing.T) {
	ctx := &CTEContext{
		CTEs: make(map[string]*CTEDefinition),
	}

	// Create a non-circular dependency: A -> B -> C
	defA := &CTEDefinition{
		Name:      "A",
		DependsOn: []string{"B"},
		Level:     0,
	}
	ctx.CTEs["A"] = defA
	ctx.CTEs["B"] = &CTEDefinition{
		Name:      "B",
		DependsOn: []string{"C"},
		Level:     0,
	}
	ctx.CTEs["C"] = &CTEDefinition{
		Name:      "C",
		DependsOn: []string{},
		Level:     1,
	}

	visiting := map[string]bool{}
	err := ctx.checkLevelCircularity("A", defA, visiting)
	if err != nil {
		t.Errorf("checkLevelCircularity should not detect circular dependency, got: %v", err)
	}
}

func TestCalculateMaxDependencyLevel(t *testing.T) {
	ctx := &CTEContext{
		CTEs: make(map[string]*CTEDefinition),
	}

	// Create dependency chain: A -> B -> C
	ctx.CTEs["C"] = &CTEDefinition{
		Name:      "C",
		DependsOn: []string{},
		Level:     1,
	}
	ctx.CTEs["B"] = &CTEDefinition{
		Name:      "B",
		DependsOn: []string{"C"},
		Level:     2,
	}
	defA := &CTEDefinition{
		Name:      "A",
		DependsOn: []string{"B"},
		Level:     0,
	}
	ctx.CTEs["A"] = defA

	visiting := map[string]bool{}
	maxLevel, err := ctx.calculateMaxDependencyLevel("A", defA, visiting)
	if err != nil {
		t.Fatalf("calculateMaxDependencyLevel() error = %v", err)
	}
	if maxLevel != 2 {
		t.Errorf("expected max level 2, got %d", maxLevel)
	}
}

func TestCalculateMaxDependencyLevelNoDeps(t *testing.T) {
	ctx := &CTEContext{
		CTEs: make(map[string]*CTEDefinition),
	}

	defA := &CTEDefinition{
		Name:      "A",
		DependsOn: []string{},
		Level:     0,
	}
	ctx.CTEs["A"] = defA

	visiting := map[string]bool{}
	maxLevel, err := ctx.calculateMaxDependencyLevel("A", defA, visiting)
	if err != nil {
		t.Fatalf("calculateMaxDependencyLevel() error = %v", err)
	}
	if maxLevel != 0 {
		t.Errorf("expected max level 0, got %d", maxLevel)
	}
}

// Tests for join functions

func TestValidateTableCount(t *testing.T) {
	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    []*TableInfo{},
		WhereInfo: &WhereInfo{},
	}

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"zero tables", 0, true},
		{"one table", 1, false},
		{"many tables", 10, false},
		{"max tables", 64, false},
		{"too many tables", 65, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jo.validateTableCount(tt.count)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTableCount(%d) error = %v, wantErr %v", tt.count, err, tt.wantErr)
			}
		})
	}
}

func TestEnumerateSubsets(t *testing.T) {
	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    createJoinTestTables()[:3],
		WhereInfo: &WhereInfo{
			Clause: &WhereClause{Terms: []*WhereTerm{}},
		},
	}

	callback := func(subset uint64) {
		// Just count callbacks
	}

	jo.enumerateSubsets(3, 2, callback)
	// If this doesn't panic, the test passes
}

func TestEnumerateSubsetsAllSizes(t *testing.T) {
	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    createJoinTestTables()[:3],
		WhereInfo: &WhereInfo{
			Clause: &WhereClause{Terms: []*WhereTerm{}},
		},
	}

	callbackCount := 0
	callback := func(subset uint64) {
		callbackCount++
	}

	// Test with size 1
	jo.enumerateSubsets(3, 1, callback)
	if callbackCount != 3 {
		t.Errorf("expected 3 subsets of size 1, got %d", callbackCount)
	}

	// Test with size 2
	callbackCount = 0
	jo.enumerateSubsets(3, 2, callback)
	if callbackCount != 3 {
		t.Errorf("expected 3 subsets of size 2, got %d", callbackCount)
	}

	// Test with size 3
	callbackCount = 0
	jo.enumerateSubsets(3, 3, callback)
	if callbackCount != 1 {
		t.Errorf("expected 1 subset of size 3, got %d", callbackCount)
	}
}

func TestEstimateSingleTableCost(t *testing.T) {
	tables := createJoinTestTables()
	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    tables,
		WhereInfo: &WhereInfo{
			Clause: &WhereClause{Terms: []*WhereTerm{}},
			Tables: tables,
		},
	}

	cost := jo.estimateSingleTableCost(0)
	if cost <= 0 {
		t.Errorf("expected positive cost, got %d", cost)
	}
}

func TestEstimateSingleTableCostWithNilWhereInfo(t *testing.T) {
	tables := createJoinTestTables()
	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    tables,
		WhereInfo: nil,
	}

	cost := jo.estimateSingleTableCost(0)
	if cost <= 0 {
		t.Errorf("expected positive cost, got %d", cost)
	}
}

func TestFindJoinConditions(t *testing.T) {
	tables := createJoinTestTables()

	// Create join condition: users.dept_id = departments.id
	term := &WhereTerm{
		Operator:    WO_EQ,
		LeftCursor:  0, // users
		LeftColumn:  2, // dept_id
		PrereqAll:   (Bitmask(1) << 0) | (Bitmask(1) << 1),
		PrereqRight: Bitmask(1) << 1, // departments
	}

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{term},
		},
		Tables: tables,
	}

	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    tables,
		WhereInfo: whereInfo,
	}

	// Test finding join conditions between users (0) and departments (1)
	left := &JoinOrder{
		Tables:   []int{0},
		Cost:     0,
		RowCount: NewLogEst(10000),
	}
	right := &JoinOrder{
		Tables:   []int{1},
		Cost:     0,
		RowCount: NewLogEst(100),
	}

	conditions := jo.findJoinConditions(left, right)
	if len(conditions) != 1 {
		t.Errorf("expected 1 join condition, got %d", len(conditions))
	}
}

func TestFindJoinConditionsNoMatch(t *testing.T) {
	tables := createJoinTestTables()

	whereInfo := &WhereInfo{
		Clause: &WhereClause{
			Terms: []*WhereTerm{},
		},
		Tables: tables,
	}

	jo := &JoinOptimizer{
		CostModel: NewCostModel(),
		Tables:    tables,
		WhereInfo: whereInfo,
	}

	// Test with no join conditions
	left := &JoinOrder{
		Tables:   []int{0},
		Cost:     0,
		RowCount: NewLogEst(10000),
	}
	right := &JoinOrder{
		Tables:   []int{1},
		Cost:     0,
		RowCount: NewLogEst(100),
	}

	conditions := jo.findJoinConditions(left, right)
	if len(conditions) != 0 {
		t.Errorf("expected 0 join conditions, got %d", len(conditions))
	}
}

// Tests for explain functions

func TestFormatJoinType(t *testing.T) {
	tests := []struct {
		joinType parser.JoinType
		expected string
	}{
		{parser.JoinInner, "INNER JOIN"},
		{parser.JoinLeft, "LEFT JOIN"},
		{parser.JoinCross, "CROSS JOIN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatJoinType(tt.joinType)
			if result != tt.expected {
				t.Errorf("formatJoinType(%v) = %q, want %q", tt.joinType, result, tt.expected)
			}
		})
	}
}

func TestGenerateExplainForInsertWithValues(t *testing.T) {
	insert := &parser.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]parser.Expression{
			{
				&parser.LiteralExpr{Value: "1"},
				&parser.LiteralExpr{Value: "Alice"},
			},
		},
	}

	plan, err := GenerateExplain(insert)
	if err != nil {
		t.Fatalf("GenerateExplain() error = %v", err)
	}

	if plan == nil {
		t.Error("GenerateExplain should not return nil plan")
	}
}

func TestGenerateExplainForInsertWithSelect(t *testing.T) {
	insert := &parser.InsertStmt{
		Table: "users",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "temp_users"},
				},
			},
		},
	}

	plan, err := GenerateExplain(insert)
	if err != nil {
		t.Fatalf("GenerateExplain() error = %v", err)
	}

	if plan == nil {
		t.Error("GenerateExplain should not return nil plan")
	}
}

func TestGenerateExplainWithSubqueries(t *testing.T) {
	sel := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{
					Subquery: &parser.SelectStmt{
						Columns: []parser.ResultColumn{{Star: true}},
						From: &parser.FromClause{
							Tables: []parser.TableOrSubquery{
								{TableName: "users"},
							},
						},
					},
					Alias: "u",
				},
			},
		},
	}

	plan, err := GenerateExplain(sel)
	if err != nil {
		t.Fatalf("GenerateExplain() error = %v", err)
	}

	if plan == nil {
		t.Error("GenerateExplain should not return nil")
	}

	// Check that the plan includes a subquery node
	if len(plan.Roots) == 0 {
		t.Error("expected at least one root node")
	}
}

// Tests for types.go String() methods - already tested via integration tests

// Tests for subquery optimizer - additional coverage

func TestTryTypeSpecificOptimization(t *testing.T) {
	opt := NewSubqueryOptimizer(NewCostModel())

	// Test that scalar subqueries return nil
	info := &SubqueryInfo{
		Type:           SubqueryScalar,
		IsCorrelated:   false,
		EstimatedRows:  NewLogEst(100),
		ExecutionCount: NewLogEst(1),
	}

	whereInfo := &WhereInfo{
		Tables: []*TableInfo{
			{Name: "test", RowCount: 1000, RowLogEst: NewLogEst(1000)},
		},
		NOut: NewLogEst(1000),
	}

	result, _ := opt.tryTypeSpecificOptimization(info, whereInfo)
	if result != nil {
		t.Error("expected nil result for scalar subquery")
	}

	// Test IN subquery (may or may not optimize, but shouldn't crash)
	info.Type = SubqueryIn
	_, _ = opt.tryTypeSpecificOptimization(info, whereInfo)

	// Test EXISTS subquery (may or may not optimize, but shouldn't crash)
	info.Type = SubqueryExists
	_, _ = opt.tryTypeSpecificOptimization(info, whereInfo)
}

func TestSubqueryTypeString(t *testing.T) {
	tests := []struct {
		typ      SubqueryType
		expected string
	}{
		{SubqueryScalar, "SCALAR"},
		{SubqueryExists, "EXISTS"},
		{SubqueryIn, "IN"},
		{SubqueryFrom, "FROM"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.typ.String()
			if result != tt.expected {
				t.Errorf("SubqueryType.String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestSubqueryTypeStringUnknown(t *testing.T) {
	var st SubqueryType = 999
	result := st.String()
	if result != "UNKNOWN" {
		t.Errorf("expected 'UNKNOWN', got %q", result)
	}
}

// TestRowSourceString removed - RowSource is not exported

func TestNewLogEst(t *testing.T) {
	tests := []struct {
		n int64
	}{
		{0},
		{1},
		{10},
		{100},
		{1000},
		{10000},
	}

	for _, tt := range tests {
		result := NewLogEst(tt.n)
		// Just verify it doesn't panic
		_ = result
	}
}

// Tests for additional join algorithm coverage - moved to join_test.go
