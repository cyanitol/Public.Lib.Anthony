// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// TestCoveringIndexDetection tests the covering index detection implementation.
func TestCoveringIndexDetection(t *testing.T) {
	// Create a test schema with a table and indexes
	s := schema.NewSchema()

	// Add a test table
	table := &schema.Table{
		Name:     "users",
		RootPage: 1,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
			{Name: "age", Type: "INTEGER"},
		},
	}
	s.AddTableDirect(table)

	// Add an index on (name, email) - this should be covering for queries needing just name and email
	idx1 := &schema.Index{
		Name:     "idx_name_email",
		Table:    "users",
		RootPage: 2,
		Columns:  []string{"name", "email"},
		Unique:   false,
	}
	s.AddIndexDirect(idx1)

	// Add an index on just (name) - not covering for queries needing name and email
	idx2 := &schema.Index{
		Name:     "idx_name",
		Table:    "users",
		RootPage: 3,
		Columns:  []string{"name"},
		Unique:   false,
	}
	s.AddIndexDirect(idx2)

	tests := []struct {
		name           string
		neededColumns  []string
		expectedResult map[string]bool // index name -> is covering
	}{
		{
			name:          "Covering index - both columns",
			neededColumns: []string{"name", "email"},
			expectedResult: map[string]bool{
				"idx_name_email": true,
				"idx_name":       false,
			},
		},
		{
			name:          "Covering index - single column",
			neededColumns: []string{"name"},
			expectedResult: map[string]bool{
				"idx_name_email": true,
				"idx_name":       true,
			},
		},
		{
			name:          "Not covering - needs age column",
			neededColumns: []string{"name", "age"},
			expectedResult: map[string]bool{
				"idx_name_email": false,
				"idx_name":       false,
			},
		},
		{
			name:          "Empty needed columns",
			neededColumns: []string{},
			expectedResult: map[string]bool{
				"idx_name_email": false,
				"idx_name":       false,
			},
		},
		{
			name:          "Covering with rowid",
			neededColumns: []string{"name", "rowid"},
			expectedResult: map[string]bool{
				"idx_name_email": true, // rowid is always available
				"idx_name":       true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test idx_name_email
			isCovering := isIndexCovering(idx1, tt.neededColumns)
			expected := tt.expectedResult["idx_name_email"]
			if isCovering != expected {
				t.Errorf("isIndexCovering(idx_name_email, %v) = %v, want %v",
					tt.neededColumns, isCovering, expected)
			}

			// Test idx_name
			isCovering = isIndexCovering(idx2, tt.neededColumns)
			expected = tt.expectedResult["idx_name"]
			if isCovering != expected {
				t.Errorf("isIndexCovering(idx_name, %v) = %v, want %v",
					tt.neededColumns, isCovering, expected)
			}
		})
	}
}

// TestSortOrderTracking tests the sort order tracking for merge join.
func TestSortOrderTracking(t *testing.T) {
	// Create test tables
	table1 := &TableInfo{
		Name:      "users",
		RowCount:  1000,
		RowLogEst: NewLogEst(1000),
	}
	table2 := &TableInfo{
		Name:      "orders",
		RowCount:  5000,
		RowLogEst: NewLogEst(5000),
	}

	tables := []*TableInfo{table1, table2}
	costModel := NewCostModel()

	// Create a join optimizer with WhereInfo that has index information
	whereInfo := &WhereInfo{
		AllLoops: []*WhereLoop{
			{
				TabIndex: 0,
				Index: &IndexInfo{
					Name:  "idx_user_id",
					Table: "users",
					Columns: []IndexColumn{
						{Name: "id", Ascending: true},
					},
					Unique: true,
				},
			},
			{
				TabIndex: 1,
				Index: &IndexInfo{
					Name:  "idx_order_user",
					Table: "orders",
					Columns: []IndexColumn{
						{Name: "user_id", Ascending: true},
					},
					Unique: false,
				},
			},
		},
	}

	optimizer := NewJoinOptimizer(tables, whereInfo, costModel)

	t.Run("Single table sort order from index", func(t *testing.T) {
		sortOrder := optimizer.getSingleTableSortOrder(0)
		if len(sortOrder) != 1 {
			t.Fatalf("Expected 1 sort column, got %d", len(sortOrder))
		}
		if sortOrder[0].Column != "id" {
			t.Errorf("Expected column 'id', got '%s'", sortOrder[0].Column)
		}
		if !sortOrder[0].Ascending {
			t.Error("Expected ascending sort order")
		}
	})

	t.Run("Merge join detection", func(t *testing.T) {
		// Create two join orders with compatible sort orders
		outer := &JoinOrder{
			Tables: []int{0},
			SortOrder: []SortColumn{
				{TableIdx: 0, Column: "id", Ascending: true},
			},
		}
		inner := &JoinOrder{
			Tables: []int{1},
			SortOrder: []SortColumn{
				{TableIdx: 1, Column: "user_id", Ascending: true},
			},
		}

		// Create a dummy equi-join term
		joinTerms := []*WhereTerm{
			{Operator: WO_EQ},
		}

		canUseMerge := canUseMergeJoin(outer, inner, joinTerms)
		if !canUseMerge {
			t.Error("Expected to be able to use merge join with compatible sort orders")
		}
	})

	t.Run("Merge join not possible - different directions", func(t *testing.T) {
		outer := &JoinOrder{
			Tables: []int{0},
			SortOrder: []SortColumn{
				{TableIdx: 0, Column: "id", Ascending: true},
			},
		}
		inner := &JoinOrder{
			Tables: []int{1},
			SortOrder: []SortColumn{
				{TableIdx: 1, Column: "user_id", Ascending: false},
			},
		}

		joinTerms := []*WhereTerm{
			{Operator: WO_EQ},
		}

		canUseMerge := canUseMergeJoin(outer, inner, joinTerms)
		if canUseMerge {
			t.Error("Expected NOT to be able to use merge join with incompatible sort directions")
		}
	})

	t.Run("Merge join not possible - no equi-join", func(t *testing.T) {
		outer := &JoinOrder{
			Tables: []int{0},
			SortOrder: []SortColumn{
				{TableIdx: 0, Column: "id", Ascending: true},
			},
		}
		inner := &JoinOrder{
			Tables: []int{1},
			SortOrder: []SortColumn{
				{TableIdx: 1, Column: "user_id", Ascending: true},
			},
		}

		// No equi-join terms
		joinTerms := []*WhereTerm{
			{Operator: WO_GT},
		}

		canUseMerge := canUseMergeJoin(outer, inner, joinTerms)
		if canUseMerge {
			t.Error("Expected NOT to be able to use merge join without equi-join condition")
		}
	})

	t.Run("Sort order preservation - nested loop", func(t *testing.T) {
		outer := &JoinOrder{
			Tables: []int{0},
			SortOrder: []SortColumn{
				{TableIdx: 0, Column: "id", Ascending: true},
			},
		}
		inner := &JoinOrder{
			Tables:    []int{1},
			SortOrder: []SortColumn{},
		}

		sortOrder := optimizer.determineCombinedSortOrder(outer, inner, JoinNestedLoop)
		if len(sortOrder) != 1 || sortOrder[0].Column != "id" {
			t.Error("Nested loop join should preserve outer sort order")
		}
	})

	t.Run("Sort order preservation - hash join", func(t *testing.T) {
		outer := &JoinOrder{
			Tables: []int{0},
			SortOrder: []SortColumn{
				{TableIdx: 0, Column: "id", Ascending: true},
			},
		}
		inner := &JoinOrder{
			Tables:    []int{1},
			SortOrder: []SortColumn{},
		}

		sortOrder := optimizer.determineCombinedSortOrder(outer, inner, JoinHash)
		if len(sortOrder) != 0 {
			t.Error("Hash join should not preserve sort order")
		}
	})
}

// TestFindBestIndexWithColumns tests the covering index detection in context.
func TestFindBestIndexWithColumns(t *testing.T) {
	// Create a schema
	s := schema.NewSchema()

	table := &schema.Table{
		Name:     "products",
		RootPage: 1,
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "price", Type: "REAL"},
			{Name: "category", Type: "TEXT"},
		},
	}
	s.AddTableDirect(table)

	// Add indexes
	idx1 := &schema.Index{
		Name:     "idx_name_price",
		Table:    "products",
		RootPage: 2,
		Columns:  []string{"name", "price"},
		Unique:   false,
	}
	s.AddIndexDirect(idx1)

	// Create a WHERE clause: name = 'test'
	whereClause := &parser.BinaryExpr{
		Op:    parser.OpEq,
		Left:  &parser.IdentExpr{Name: "name"},
		Right: &parser.LiteralExpr{Value: "test"},
	}

	t.Run("Without needed columns", func(t *testing.T) {
		candidate := findBestIndex("products", whereClause, s)
		if candidate == nil {
			t.Fatal("Expected to find an index")
		}
		// Should not be marked as covering since we don't know what columns are needed
		if candidate.IsCovering {
			t.Error("Expected index not to be marked as covering without needed columns")
		}
	})

	t.Run("With covering columns", func(t *testing.T) {
		neededColumns := []string{"name", "price"}
		candidate := findBestIndexWithColumns("products", whereClause, s, neededColumns)
		if candidate == nil {
			t.Fatal("Expected to find an index")
		}
		// Should be marked as covering since index has both name and price
		if !candidate.IsCovering {
			t.Error("Expected index to be marked as covering")
		}
	})

	t.Run("With non-covering columns", func(t *testing.T) {
		neededColumns := []string{"name", "category"}
		candidate := findBestIndexWithColumns("products", whereClause, s, neededColumns)
		if candidate == nil {
			t.Fatal("Expected to find an index")
		}
		// Should not be marked as covering since index doesn't have category
		if candidate.IsCovering {
			t.Error("Expected index not to be marked as covering")
		}
	})
}
