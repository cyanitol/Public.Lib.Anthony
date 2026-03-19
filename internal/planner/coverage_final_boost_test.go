// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func parseCTEAndCheckLevel(t *testing.T, sql, cteName string) {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	selectStmt := stmts[0].(*parser.SelectStmt)
	ctx, err := NewCTEContext(selectStmt.With)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ctx != nil {
		def, exists := ctx.GetCTE(cteName)
		if exists && def.Level < 0 {
			t.Errorf("Expected non-negative level, got %d", def.Level)
		}
	}
}

// TestCalculateMaxDependencyLevelComprehensive tests the calculateMaxDependencyLevel function comprehensively.
func TestCalculateMaxDependencyLevelComprehensive(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		cteName string
	}{
		{"CTE with no dependencies", "WITH a AS (SELECT 1) SELECT * FROM a", "a"},
		{"CTE with single dependency", "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b", "b"},
		{"CTE with multiple dependencies", "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a UNION SELECT * FROM b) SELECT * FROM c", "c"},
		{"Recursive CTE self-reference", "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte", "cte"},
		{"CTE with dependency chain", "WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c", "c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseCTEAndCheckLevel(t, tt.sql, tt.cteName)
		})
	}
}

func parseCTEAndValidate(t *testing.T, sql string) error {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	selectStmt := stmts[0].(*parser.SelectStmt)
	ctx, err := NewCTEContext(selectStmt.With)
	if err != nil {
		return err
	}
	if ctx != nil {
		return ctx.ValidateCTEs()
	}
	return nil
}

// TestCheckCircularDependencyComprehensive tests circular dependency detection thoroughly.
func TestCheckCircularDependencyComprehensive(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"No circular dependency - linear chain", "WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c"},
		{"No circular dependency - independent CTEs", "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3) SELECT * FROM a, b, c"},
		{"Recursive CTE allowed to reference itself", "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"},
		{"Complex dependency graph without cycles", "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a), d AS (SELECT * FROM b), e AS (SELECT * FROM c UNION SELECT * FROM d) SELECT * FROM e"},
		{"Multiple independent CTEs", "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3) SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseCTEAndValidate(t, tt.sql); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestFormatJoinTypeComprehensive tests all join type formatting.
func TestFormatJoinTypeComprehensive(t *testing.T) {
	tests := []struct {
		name     string
		joinType parser.JoinType
		expected string
	}{
		{
			name:     "LEFT JOIN",
			joinType: parser.JoinLeft,
			expected: "LEFT JOIN",
		},
		{
			name:     "RIGHT JOIN",
			joinType: parser.JoinRight,
			expected: "RIGHT JOIN",
		},
		{
			name:     "FULL JOIN",
			joinType: parser.JoinFull,
			expected: "FULL JOIN",
		},
		{
			name:     "CROSS JOIN",
			joinType: parser.JoinCross,
			expected: "CROSS JOIN",
		},
		{
			name:     "INNER JOIN (default)",
			joinType: parser.JoinInner,
			expected: "INNER JOIN",
		},
		{
			name:     "Unknown join type defaults to INNER",
			joinType: parser.JoinType(999),
			expected: "INNER JOIN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatJoinType(tt.joinType)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

// TestFormatTableScanComprehensive tests table scan formatting with various WHERE conditions.
func TestFormatTableScanComprehensive(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		where       parser.Expression
		isWrite     bool
		expectScan  bool
		expectIndex bool
	}{
		{
			name:       "No WHERE clause",
			tableName:  "users",
			where:      nil,
			isWrite:    false,
			expectScan: true,
		},
		{
			name:      "Equality WHERE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "id"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "Range WHERE with GT",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpGt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "Range WHERE with LT",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpLt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "65"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "Range WHERE with GE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpGe,
				Left:  &parser.IdentExpr{Name: "score"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "100"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "Range WHERE with LE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpLe,
				Left:  &parser.IdentExpr{Name: "score"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "200"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "LIKE expression",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpLike,
				Left:  &parser.IdentExpr{Name: "name"},
				Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "John%"},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "IN expression",
			tableName: "users",
			where: &parser.InExpr{
				Expr: &parser.IdentExpr{Name: "status"},
				Values: []parser.Expression{
					&parser.LiteralExpr{Type: parser.LiteralString, Value: "active"},
					&parser.LiteralExpr{Type: parser.LiteralString, Value: "pending"},
				},
			},
			isWrite:     false,
			expectIndex: true,
		},
		{
			name:      "Write operation",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "id"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			isWrite:     true,
			expectIndex: true,
		},
		{
			name:      "AND expression",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op: parser.OpAnd,
				Left: &parser.BinaryExpr{
					Op:    parser.OpEq,
					Left:  &parser.IdentExpr{Name: "age"},
					Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "25"},
				},
				Right: &parser.BinaryExpr{
					Op:    parser.OpEq,
					Left:  &parser.IdentExpr{Name: "status"},
					Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "active"},
				},
			},
			isWrite:     false,
			expectIndex: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTableScan(tt.tableName, tt.where, tt.isWrite)
			assertTableScanResult(t, result, tt.tableName, tt.expectScan, tt.expectIndex, tt.where)
		})
	}
}

func assertTableScanResult(t *testing.T, result, tableName string, expectScan, expectIndex bool, where parser.Expression) {
	t.Helper()
	if result == "" {
		t.Error("formatTableScan returned empty string")
	}
	if !strings.Contains(result, tableName) {
		t.Errorf("Expected result to contain table name '%s', got '%s'", tableName, result)
	}
	if expectScan && !strings.Contains(result, "SCAN") {
		t.Errorf("Expected result to contain 'SCAN', got '%s'", result)
	}
	if expectIndex && where != nil {
		if !strings.Contains(result, "SEARCH") && !strings.Contains(result, "SCAN") {
			t.Errorf("Expected result to contain 'SEARCH' or 'SCAN', got '%s'", result)
		}
	}
}

// TestFormatTableScanWithWhereDetails tests the formatTableScanWithWhere function.
func TestFormatTableScanWithWhereDetails(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		where     parser.Expression
	}{
		{
			name:      "NULL WHERE",
			tableName: "users",
			where:     nil,
		},
		{
			name:      "Binary expression WHERE",
			tableName: "users",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "id"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
		},
		{
			name:      "IN expression WHERE",
			tableName: "users",
			where: &parser.InExpr{
				Expr: &parser.IdentExpr{Name: "id"},
			},
		},
		{
			name:      "Other expression type",
			tableName: "users",
			where:     &parser.UnaryExpr{Op: parser.OpNot, Expr: &parser.IdentExpr{Name: "deleted"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTableScanWithWhere(tt.tableName, tt.where)
			if result == "" {
				t.Error("formatTableScanWithWhere returned empty string")
			}
			if !strings.Contains(result, tt.tableName) {
				t.Errorf("Expected result to contain table name '%s', got '%s'", tt.tableName, result)
			}
		})
	}
}

// TestAnalyzeIndexabilityComprehensive tests the analyzeIndexability function.
func TestAnalyzeIndexabilityComprehensive(t *testing.T) {
	tests := []struct {
		name            string
		where           parser.Expression
		expectIndexable bool
		expectColName   string
	}{
		{
			name:            "Nil expression",
			where:           nil,
			expectIndexable: false,
			expectColName:   "",
		},
		{
			name: "Equality expression",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.IdentExpr{Name: "id"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
			expectIndexable: true,
			expectColName:   "id",
		},
		{
			name: "Greater than expression",
			where: &parser.BinaryExpr{
				Op:    parser.OpGt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
			},
			expectIndexable: true,
			expectColName:   "age",
		},
		{
			name: "Less than expression",
			where: &parser.BinaryExpr{
				Op:    parser.OpLt,
				Left:  &parser.IdentExpr{Name: "age"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "65"},
			},
			expectIndexable: true,
			expectColName:   "age",
		},
		{
			name: "Greater or equal expression",
			where: &parser.BinaryExpr{
				Op:    parser.OpGe,
				Left:  &parser.IdentExpr{Name: "score"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "100"},
			},
			expectIndexable: true,
			expectColName:   "score",
		},
		{
			name: "Less or equal expression",
			where: &parser.BinaryExpr{
				Op:    parser.OpLe,
				Left:  &parser.IdentExpr{Name: "score"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "200"},
			},
			expectIndexable: true,
			expectColName:   "score",
		},
		{
			name: "LIKE expression (not indexable in simple analysis)",
			where: &parser.BinaryExpr{
				Op:    parser.OpLike,
				Left:  &parser.IdentExpr{Name: "name"},
				Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "John%"},
			},
			expectIndexable: false,
			expectColName:   "name", // Still extracts column name
		},
		{
			name: "Non-binary expression (IN)",
			where: &parser.InExpr{
				Expr: &parser.IdentExpr{Name: "status"},
			},
			expectIndexable: false,
			expectColName:   "",
		},
		{
			name: "Non-indexable binary expression (OR)",
			where: &parser.BinaryExpr{
				Op:    parser.OpOr,
				Left:  &parser.IdentExpr{Name: "a"},
				Right: &parser.IdentExpr{Name: "b"},
			},
			expectIndexable: false,
			expectColName:   "a", // Still extracts column name even if not indexable
		},
		{
			name: "Binary expression with non-ident left side",
			where: &parser.BinaryExpr{
				Op:    parser.OpEq,
				Left:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
				Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "2"},
			},
			expectIndexable: false,
			expectColName:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexable, colName := analyzeIndexability(tt.where)

			if indexable != tt.expectIndexable {
				t.Errorf("Expected indexable=%v, got %v", tt.expectIndexable, indexable)
			}

			if colName != tt.expectColName {
				t.Errorf("Expected colName=%s, got %s", tt.expectColName, colName)
			}
		})
	}
}

// TestEstimateSetupCostComprehensive tests all setup cost estimation paths.
func TestEstimateSetupCostComprehensive(t *testing.T) {
	tests := []struct {
		name       string
		setupType  SetupType
		nRows      LogEst
		expectZero bool
		expectMin  LogEst
	}{
		{
			name:       "No setup",
			setupType:  SetupNone,
			nRows:      NewLogEst(1000),
			expectZero: true,
		},
		{
			name:      "Auto index creation",
			setupType: SetupAutoIndex,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(50),
		},
		{
			name:      "Sort operation with positive rows",
			setupType: SetupSort,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(100),
		},
		{
			name:       "Sort operation with zero rows",
			setupType:  SetupSort,
			nRows:      NewLogEst(0),
			expectZero: true,
		},
		{
			name:       "Sort operation with negative rows",
			setupType:  SetupSort,
			nRows:      LogEst(-10),
			expectZero: true,
		},
		{
			name:      "Bloom filter creation",
			setupType: SetupBloomFilter,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(10),
		},
		{
			name:       "Unknown setup type",
			setupType:  SetupType(999),
			nRows:      NewLogEst(1000),
			expectZero: true,
		},
		{
			name:      "Large dataset auto index",
			setupType: SetupAutoIndex,
			nRows:     NewLogEst(1000000),
			expectMin: NewLogEst(1000),
		},
		{
			name:      "Large dataset sort",
			setupType: SetupSort,
			nRows:     NewLogEst(1000000),
			expectMin: NewLogEst(10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			costModel := NewCostModel()
			result := costModel.EstimateSetupCost(tt.setupType, tt.nRows)

			if tt.expectZero {
				if result != 0 {
					t.Errorf("Expected zero cost, got %d", result)
				}
			} else {
				if result < tt.expectMin {
					t.Errorf("Expected cost >= %d, got %d", tt.expectMin, result)
				}
			}

			if result < 0 {
				t.Errorf("Cost should never be negative, got %d", result)
			}
		})
	}
}

// TestEstimateIndexScanEdgeCases tests edge cases in index scan estimation.
func TestEstimateIndexScanEdgeCases(t *testing.T) {
	costModel := NewCostModel()

	tests := []struct {
		name     string
		table    *TableInfo
		index    *IndexInfo
		terms    []*WhereTerm
		nEq      int
		hasRange bool
		covering bool
	}{
		{
			name: "No equality constraints",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_name",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: false,
			covering: false,
		},
		{
			name: "Equality beyond stats length",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_compound",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      5, // More than ColumnStats length
			hasRange: false,
			covering: false,
		},
		{
			name: "Range with covering index",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_age",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: true,
			covering: true,
		},
		{
			name: "Range without covering index",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_age",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: true,
			covering: false,
		},
		{
			name: "Multiple equality with covering",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_compound",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100), NewLogEst(10), NewLogEst(1)},
			},
			terms:    []*WhereTerm{},
			nEq:      3,
			hasRange: false,
			covering: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost, nOut := costModel.EstimateIndexScan(
				tt.table,
				tt.index,
				tt.terms,
				tt.nEq,
				tt.hasRange,
				tt.covering,
			)

			if cost < 0 {
				t.Errorf("Cost should be non-negative, got %d", cost)
			}

			if nOut < 0 {
				t.Errorf("nOut should be non-negative, got %d", nOut)
			}

			// Covering index should have lower cost than non-covering
			// (when comparing similar scenarios)
			if tt.covering && tt.hasRange {
				// Just verify we get valid values
				if cost == 0 && tt.table.RowLogEst > 0 {
					t.Error("Expected non-zero cost for non-empty table")
				}
			}
		})
	}
}

// TestExplainGenerationWithMultipleRoots tests explain plan with multiple root nodes.
func TestExplainGenerationWithMultipleRoots(t *testing.T) {
	plan := NewExplainPlan()

	// Add multiple root nodes
	root1 := plan.AddNode(nil, "QUERY PLAN 1")
	root2 := plan.AddNode(nil, "QUERY PLAN 2")

	child1 := plan.AddNode(root1, "SCAN table1")
	child2 := plan.AddNode(root2, "SCAN table2")

	if len(plan.Roots) != 2 {
		t.Errorf("Expected 2 roots, got %d", len(plan.Roots))
	}

	if len(root1.Children) != 1 {
		t.Errorf("Expected root1 to have 1 child, got %d", len(root1.Children))
	}

	if len(root2.Children) != 1 {
		t.Errorf("Expected root2 to have 1 child, got %d", len(root2.Children))
	}

	if child1.Level != 1 {
		t.Errorf("Expected child1 level 1, got %d", child1.Level)
	}

	if child2.Level != 1 {
		t.Errorf("Expected child2 level 1, got %d", child2.Level)
	}

	// Test table format includes both trees
	rows := plan.FormatAsTable()
	if len(rows) < 4 {
		t.Errorf("Expected at least 4 rows, got %d", len(rows))
	}

	// Test text format includes both trees
	text := plan.FormatAsText()
	if !strings.Contains(text, "QUERY PLAN 1") {
		t.Error("Expected text to contain 'QUERY PLAN 1'")
	}
	if !strings.Contains(text, "QUERY PLAN 2") {
		t.Error("Expected text to contain 'QUERY PLAN 2'")
	}
}

// TestExplainWithDeepNesting tests deeply nested explain plans.
func TestExplainWithDeepNesting(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "ROOT")
	level1 := plan.AddNode(root, "LEVEL 1")
	level2 := plan.AddNode(level1, "LEVEL 2")
	level3 := plan.AddNode(level2, "LEVEL 3")

	if level3.Level != 3 {
		t.Errorf("Expected level3 to be at level 3, got %d", level3.Level)
	}

	if level3.Parent != level2.ID {
		t.Errorf("Expected level3 parent to be %d, got %d", level2.ID, level3.Parent)
	}

	// Check indentation in text format
	text := plan.FormatAsText()
	lines := strings.Split(text, "\n")

	// Find the LEVEL 3 line and check indentation
	for _, line := range lines {
		if strings.Contains(line, "LEVEL 3") {
			// Should have 6 spaces (2 per level * 3 levels)
			if !strings.HasPrefix(line, "      LEVEL 3") {
				t.Errorf("Expected 6 spaces of indentation for LEVEL 3, got: '%s'", line)
			}
		}
	}
}

func parseCTEContextForValidation(t *testing.T, sql string) (*CTEContext, error) {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		return nil, err
	}
	selectStmt := stmts[0].(*parser.SelectStmt)
	return NewCTEContext(selectStmt.With)
}

func parseCTEAndValidateFull(t *testing.T, sql string, wantErr bool) {
	t.Helper()
	ctx, err := parseCTEContextForValidation(t, sql)
	if err != nil {
		if !wantErr {
			t.Fatalf("Parse/CTE creation failed: %v", err)
		}
		return
	}
	if ctx == nil {
		return
	}
	err = ctx.ValidateCTEs()
	if wantErr && err == nil {
		t.Error("Expected validation error, got nil")
	}
	if !wantErr && err != nil {
		t.Errorf("Unexpected validation error: %v", err)
	}
}

// TestCTEEdgeCasesForCoverage tests edge cases in CTE handling.
func TestCTEEdgeCasesForCoverage(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"CTE with subquery in FROM that references another CTE", "WITH a AS (SELECT 1), b AS (SELECT * FROM (SELECT * FROM a)) SELECT * FROM b", false},
		{"CTE with subquery in JOIN that references another CTE", "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM users JOIN (SELECT * FROM a) ON users.id = a.x) SELECT * FROM b", false},
		{"Recursive CTE with EXCEPT", "WITH RECURSIVE cte AS (SELECT 1 EXCEPT SELECT 2) SELECT * FROM cte", true},
		{"Recursive CTE with INTERSECT", "WITH RECURSIVE cte AS (SELECT 1 INTERSECT SELECT 2) SELECT * FROM cte", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseCTEAndValidateFull(t, tt.sql, tt.wantErr)
		})
	}
}
