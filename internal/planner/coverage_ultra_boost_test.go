// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func checkCircDepHelper(t *testing.T, sql, cteName string) *CTEContext {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	selectStmt := stmts[0].(*parser.SelectStmt)
	ctx, err := NewCTEContext(selectStmt.With)
	if err != nil {
		t.Fatalf("NewCTEContext failed: %v", err)
	}
	return ctx
}

// TestCheckCircularDependencyAllPaths tests all code paths in checkCircularDependency.
func TestCheckCircularDependencyAllPaths(t *testing.T) {
	t.Run("Recursive CTE visiting itself", func(t *testing.T) {
		ctx := checkCircDepHelper(t, "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte", "cte")
		visiting := make(map[string]bool)
		if err := ctx.checkCircularDependency("cte", visiting); err != nil {
			t.Errorf("should succeed for recursive CTE, got: %v", err)
		}
	})

	t.Run("Non-existent CTE", func(t *testing.T) {
		ctx := &CTEContext{CTEs: make(map[string]*CTEDefinition), IsRecursive: false}
		if err := ctx.checkCircularDependency("non_existent", make(map[string]bool)); err != nil {
			t.Errorf("should succeed for non-existent CTE, got: %v", err)
		}
	})

	t.Run("Valid dependency chain", func(t *testing.T) {
		ctx := checkCircDepHelper(t, "WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c", "c")
		if err := ctx.checkCircularDependency("c", make(map[string]bool)); err != nil {
			t.Errorf("should succeed for valid chain, got: %v", err)
		}
	})

	t.Run("Cleanup visiting map", func(t *testing.T) {
		ctx := checkCircDepHelper(t, "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b", "b")
		visiting := make(map[string]bool)
		if err := ctx.checkCircularDependency("b", visiting); err != nil {
			t.Errorf("failed: %v", err)
		}
		if visiting["b"] {
			t.Error("Expected 'b' to be removed from visiting map")
		}
	})
}

// TestCalculateMaxDependencyLevelAllPaths tests all paths in calculateMaxDependencyLevel.
func TestCalculateMaxDependencyLevelAllPaths(t *testing.T) {
	t.Run("Self-reference in recursive CTE", func(t *testing.T) {
		ctx := checkCircDepHelper(t, "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte", "cte")
		def, exists := ctx.GetCTE("cte")
		if !exists {
			t.Fatal("CTE 'cte' not found")
		}
		if def.Level <= 0 {
			t.Errorf("Expected positive level for recursive CTE, got %d", def.Level)
		}
	})

	t.Run("Multiple dependencies with max level calculation", func(t *testing.T) {
		ctx := checkCircDepHelper(t, "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT * FROM a), d AS (SELECT * FROM c UNION SELECT * FROM b) SELECT * FROM d", "d")
		defA, _ := ctx.GetCTE("a")
		defB, _ := ctx.GetCTE("b")
		defC, _ := ctx.GetCTE("c")
		defD, _ := ctx.GetCTE("d")

		assertLevelGreater(t, "d", defD.Level, "c", defC.Level)
		assertLevelGreater(t, "c", defC.Level, "a", defA.Level)
		assertLevelGreater(t, "d", defD.Level, "b", defB.Level)
	})
}

func assertLevelGreater(t *testing.T, name1 string, level1 int, name2 string, level2 int) {
	t.Helper()
	if level1 <= level2 {
		t.Errorf("Expected level of '%s' (%d) > level of '%s' (%d)", name1, level1, name2, level2)
	}
}

// TestSelectBestIndexWithOptionsAllPaths tests all paths in SelectBestIndexWithOptions.
func TestSelectBestIndexWithOptionsAllPaths(t *testing.T) {
	// Test case 1: No indexes available
	t.Run("No indexes", func(t *testing.T) {
		table := &TableInfo{
			Name:    "users",
			Indexes: []*IndexInfo{},
		}

		terms := []*WhereTerm{
			{LeftColumn: 0, Operator: WO_EQ, RightValue: "test"},
		}

		selector := NewIndexSelector(table, terms, NewCostModel())
		result := selector.SelectBestIndexWithOptions(OptimizeOptions{})

		if result != nil {
			t.Errorf("Expected nil when no indexes, got %v", result)
		}
	})

	// Test case 2: Index with all option bonuses
	t.Run("All option bonuses", func(t *testing.T) {
		table := &TableInfo{
			Name: "users",
			Indexes: []*IndexInfo{
				{
					Name:   "idx_compound",
					Unique: true,
					Columns: []IndexColumn{
						{Name: "email", Index: 0, Ascending: true},
						{Name: "name", Index: 1, Ascending: true},
						{Name: "age", Index: 2, Ascending: true},
						{Name: "city", Index: 3, Ascending: true},
					},
					RowLogEst:   NewLogEst(1000),
					ColumnStats: []LogEst{NewLogEst(1000), NewLogEst(100), NewLogEst(50), NewLogEst(10)},
				},
			},
		}

		terms := []*WhereTerm{
			{LeftColumn: 0, Operator: WO_EQ, RightValue: "test@test.com"},
		}

		options := OptimizeOptions{
			PreferUnique:    true,
			PreferCovering:  true,
			ConsiderOrderBy: true,
			OrderBy: []OrderByColumn{
				{Column: "email", Ascending: true},
			},
		}

		selector := NewIndexSelector(table, terms, NewCostModel())
		result := selector.SelectBestIndexWithOptions(options)

		if result == nil {
			t.Fatal("Expected index to be selected")
		}

		if result.Name != "idx_compound" {
			t.Errorf("Expected 'idx_compound', got %s", result.Name)
		}
	})

	// Test case 3: Multiple indexes to compare
	t.Run("Multiple indexes comparison", func(t *testing.T) {
		table := &TableInfo{
			Name: "users",
			Indexes: []*IndexInfo{
				{
					Name: "idx_name",
					Columns: []IndexColumn{
						{Name: "name", Index: 0},
					},
					RowLogEst:   NewLogEst(1000),
					ColumnStats: []LogEst{NewLogEst(100)},
				},
				{
					Name:   "idx_email",
					Unique: true,
					Columns: []IndexColumn{
						{Name: "email", Index: 1},
					},
					RowLogEst:   NewLogEst(1000),
					ColumnStats: []LogEst{NewLogEst(1000)},
				},
			},
		}

		terms := []*WhereTerm{
			{LeftColumn: 1, Operator: WO_EQ, RightValue: "test@test.com"},
		}

		options := OptimizeOptions{
			PreferUnique: true,
		}

		selector := NewIndexSelector(table, terms, NewCostModel())
		result := selector.SelectBestIndexWithOptions(options)

		if result == nil {
			t.Fatal("Expected index to be selected")
		}

		// Should prefer the unique index
		if result.Name != "idx_email" {
			t.Errorf("Expected 'idx_email' (unique), got %s", result.Name)
		}
	})
}

// TestIndexUsageExplainEdgeCases tests edge cases in IndexUsage.Explain().
func TestIndexUsageExplainEdgeCases(t *testing.T) {
	// Test case 1: Nil index (full table scan)
	t.Run("Nil index", func(t *testing.T) {
		usage := &IndexUsage{
			Index: nil,
		}

		result := usage.Explain()
		if result != "FULL TABLE SCAN" {
			t.Errorf("Expected 'FULL TABLE SCAN', got '%s'", result)
		}
	})

	// Test case 2: Index with no terms
	t.Run("Index with no terms", func(t *testing.T) {
		usage := &IndexUsage{
			Index: &IndexInfo{
				Name: "idx_test",
			},
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			InTerms:    []*WhereTerm{},
		}

		result := usage.Explain()
		if result == "" {
			t.Error("Expected non-empty explanation")
		}
	})

	// Test case 3: Index with all term types
	t.Run("Index with all term types", func(t *testing.T) {
		usage := &IndexUsage{
			Index: &IndexInfo{
				Name: "idx_compound",
				Columns: []IndexColumn{
					{Name: "col1", Index: 0},
					{Name: "col2", Index: 1},
					{Name: "col3", Index: 2},
				},
			},
			EqTerms: []*WhereTerm{
				{LeftColumn: 0, Operator: WO_EQ},
			},
			RangeTerms: []*WhereTerm{
				{LeftColumn: 1, Operator: WO_GT},
			},
			InTerms: []*WhereTerm{
				{LeftColumn: 2, Operator: WO_IN},
			},
			Covering: true,
		}

		result := usage.Explain()
		if result == "" {
			t.Error("Expected non-empty explanation")
		}
		// Should mention covering
		// Note: The actual Explain() implementation may vary
	})
}

// TestFindTermForColumnEdgeCases tests edge cases in findTermForColumn.
func TestFindTermForColumnEdgeCases(t *testing.T) {
	// Test case 1: No terms
	t.Run("No terms", func(t *testing.T) {
		selector := NewIndexSelector(&TableInfo{}, []*WhereTerm{}, NewCostModel())
		result := selector.findTermForColumn(0)

		if result != nil {
			t.Errorf("Expected nil when no terms, got %v", result)
		}
	})

	// Test case 2: Multiple terms for same column
	t.Run("Multiple terms same column", func(t *testing.T) {
		terms := []*WhereTerm{
			{LeftColumn: 0, Operator: WO_GT, RightValue: 10},
			{LeftColumn: 0, Operator: WO_LT, RightValue: 100},
			{LeftColumn: 1, Operator: WO_EQ, RightValue: "test"},
		}

		selector := NewIndexSelector(&TableInfo{}, terms, NewCostModel())
		result := selector.findTermForColumn(0)

		if result == nil {
			t.Error("Expected to find a term for column 0")
		}
	})

	// Test case 3: Term for different column
	t.Run("Term for different column", func(t *testing.T) {
		terms := []*WhereTerm{
			{LeftColumn: 1, Operator: WO_EQ, RightValue: "test"},
		}

		selector := NewIndexSelector(&TableInfo{}, terms, NewCostModel())
		result := selector.findTermForColumn(0)

		if result != nil {
			t.Error("Expected nil when searching for different column")
		}
	})
}

// TestApplyRangeTermEdgeCases tests edge cases in applyRangeTerm.
func TestApplyRangeTermEdgeCases(t *testing.T) {
	// Test case 1: GT operator
	t.Run("Greater than operator", func(t *testing.T) {
		index := &IndexInfo{
			Columns: []IndexColumn{
				{Name: "age", Index: 0},
			},
		}

		term := &WhereTerm{
			LeftColumn: 0,
			Operator:   WO_GT,
			RightValue: 18,
		}

		selector := NewIndexSelector(&TableInfo{}, []*WhereTerm{term}, NewCostModel())
		usage := &IndexUsage{
			Index:      index,
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		selector.applyRangeTerm(term, usage)

		if len(usage.RangeTerms) != 1 {
			t.Errorf("Expected 1 range term, got %d", len(usage.RangeTerms))
		}
		if len(usage.StartKey) != 1 {
			t.Errorf("Expected 1 start key, got %d", len(usage.StartKey))
		}
	})

	// Test case 2: LT operator
	t.Run("Less than operator", func(t *testing.T) {
		index := &IndexInfo{
			Columns: []IndexColumn{
				{Name: "age", Index: 0},
			},
		}

		term := &WhereTerm{
			LeftColumn: 0,
			Operator:   WO_LT,
			RightValue: 65,
		}

		selector := NewIndexSelector(&TableInfo{}, []*WhereTerm{term}, NewCostModel())
		usage := &IndexUsage{
			Index:      index,
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		selector.applyRangeTerm(term, usage)

		if len(usage.RangeTerms) != 1 {
			t.Errorf("Expected 1 range term, got %d", len(usage.RangeTerms))
		}
		if len(usage.EndKey) != 1 {
			t.Errorf("Expected 1 end key, got %d", len(usage.EndKey))
		}
	})

	// Test case 3: GE operator
	t.Run("Greater or equal operator", func(t *testing.T) {
		term := &WhereTerm{
			LeftColumn: 0,
			Operator:   WO_GE,
			RightValue: 18,
		}

		selector := NewIndexSelector(&TableInfo{}, []*WhereTerm{term}, NewCostModel())
		usage := &IndexUsage{
			Index:      &IndexInfo{},
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		selector.applyRangeTerm(term, usage)

		if len(usage.StartKey) != 1 {
			t.Errorf("Expected 1 start key, got %d", len(usage.StartKey))
		}
	})

	// Test case 4: LE operator
	t.Run("Less or equal operator", func(t *testing.T) {
		term := &WhereTerm{
			LeftColumn: 0,
			Operator:   WO_LE,
			RightValue: 65,
		}

		selector := NewIndexSelector(&TableInfo{}, []*WhereTerm{term}, NewCostModel())
		usage := &IndexUsage{
			Index:      &IndexInfo{},
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		selector.applyRangeTerm(term, usage)

		if len(usage.EndKey) != 1 {
			t.Errorf("Expected 1 end key, got %d", len(usage.EndKey))
		}
	})
}

// TestProcessIndexColumnsEdgeCases tests edge cases in processIndexColumns.
func TestProcessIndexColumnsEdgeCases(t *testing.T) {
	// Test case 1: Equality term found
	t.Run("Equality term found", func(t *testing.T) {
		index := &IndexInfo{
			Columns: []IndexColumn{
				{Name: "id", Index: 0},
			},
		}

		terms := []*WhereTerm{
			{LeftColumn: 0, Operator: WO_EQ, RightValue: 1},
		}

		selector := NewIndexSelector(&TableInfo{}, terms, NewCostModel())
		usage := &IndexUsage{
			Index:      index,
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		result := selector.processIndexColumns(usage, index)

		if !result {
			t.Error("Expected true when first column has constraint")
		}
		if len(usage.EqTerms) != 1 {
			t.Errorf("Expected 1 eq term, got %d", len(usage.EqTerms))
		}
	})

	// Test case 2: Range term found
	t.Run("Range term found", func(t *testing.T) {
		index := &IndexInfo{
			Columns: []IndexColumn{
				{Name: "age", Index: 0},
			},
		}

		terms := []*WhereTerm{
			{LeftColumn: 0, Operator: WO_GT, RightValue: 18},
		}

		selector := NewIndexSelector(&TableInfo{}, terms, NewCostModel())
		usage := &IndexUsage{
			Index:      index,
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		result := selector.processIndexColumns(usage, index)

		if !result {
			t.Error("Expected true when first column has constraint")
		}
		if len(usage.RangeTerms) != 1 {
			t.Errorf("Expected 1 range term, got %d", len(usage.RangeTerms))
		}
	})

	// Test case 3: No matching term (stops processing)
	t.Run("No matching term", func(t *testing.T) {
		index := &IndexInfo{
			Columns: []IndexColumn{
				{Name: "col1", Index: 0},
				{Name: "col2", Index: 1},
			},
		}

		terms := []*WhereTerm{
			{LeftColumn: 1, Operator: WO_EQ, RightValue: "test"},
		}

		selector := NewIndexSelector(&TableInfo{}, terms, NewCostModel())
		usage := &IndexUsage{
			Index:      index,
			EqTerms:    []*WhereTerm{},
			RangeTerms: []*WhereTerm{},
			StartKey:   []interface{}{},
			EndKey:     []interface{}{},
		}

		result := selector.processIndexColumns(usage, index)

		if result {
			t.Error("Expected false when first column has no constraint")
		}
		// Should stop at first column since no term found
		if len(usage.EqTerms) != 0 {
			t.Errorf("Expected 0 eq terms, got %d", len(usage.EqTerms))
		}
	})
}

// TestExtractMainTableNameEdgeCases tests edge cases in extractMainTableName.
func TestExtractMainTableNameEdgeCases(t *testing.T) {
	// Test case 1: Nil FROM clause
	t.Run("Nil FROM clause", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: nil,
		}

		result := extractMainTableName(stmt)
		if result != "" {
			t.Errorf("Expected empty string for nil FROM, got '%s'", result)
		}
	})

	// Test case 2: Empty tables slice
	t.Run("Empty tables", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{},
			},
		}

		result := extractMainTableName(stmt)
		if result != "" {
			t.Errorf("Expected empty string for empty tables, got '%s'", result)
		}
	})

	// Test case 3: Subquery instead of table name
	t.Run("Subquery in FROM", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{
						Subquery: &parser.SelectStmt{},
					},
				},
			},
		}

		result := extractMainTableName(stmt)
		if result != "subquery" {
			t.Errorf("Expected 'subquery', got '%s'", result)
		}
	})

	// Test case 4: Valid table name
	t.Run("Valid table name", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		}

		result := extractMainTableName(stmt)
		if result != "users" {
			t.Errorf("Expected 'users', got '%s'", result)
		}
	})
}

// TestJoinAlgorithmStringMethod tests the String() method of JoinAlgorithm.
func TestJoinAlgorithmStringMethod(t *testing.T) {
	tests := []struct {
		name      string
		algorithm JoinAlgorithm
		want      string
	}{
		{
			name:      "Nested loop join",
			algorithm: JoinNestedLoop,
			want:      "NestedLoop",
		},
		{
			name:      "Hash join",
			algorithm: JoinHash,
			want:      "Hash",
		},
		{
			name:      "Merge join",
			algorithm: JoinMerge,
			want:      "Merge",
		},
		{
			name:      "Unknown join algorithm",
			algorithm: JoinAlgorithm(999),
			want:      "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.algorithm.String()
			if result != tt.want {
				t.Errorf("Expected '%s', got '%s'", tt.want, result)
			}
		})
	}
}
