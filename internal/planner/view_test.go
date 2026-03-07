// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

func TestExpandView(t *testing.T) {
	view := &schema.View{
		Name: "active_users",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
				{Expr: &parser.IdentExpr{Name: "name"}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		},
	}

	expanded, err := ExpandView(view, "")
	if err != nil {
		t.Fatalf("ExpandView() error = %v", err)
	}

	if expanded == nil {
		t.Fatal("ExpandView() returned nil")
	}

	if len(expanded.Columns) != 2 {
		t.Errorf("expanded view has %d columns, want 2", len(expanded.Columns))
	}
}

func TestExpandViewWithColumns(t *testing.T) {
	view := &schema.View{
		Name:    "user_view",
		Columns: []string{"user_id", "user_name"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
				{Expr: &parser.IdentExpr{Name: "name"}},
			},
		},
	}

	expanded, err := ExpandView(view, "")
	if err != nil {
		t.Fatalf("ExpandView() error = %v", err)
	}

	// Check that columns were renamed
	if expanded.Columns[0].Alias != "user_id" {
		t.Errorf("column 0 alias = %q, want %q", expanded.Columns[0].Alias, "user_id")
	}
	if expanded.Columns[1].Alias != "user_name" {
		t.Errorf("column 1 alias = %q, want %q", expanded.Columns[1].Alias, "user_name")
	}
}

func TestExpandViewNil(t *testing.T) {
	_, err := ExpandView(nil, "")
	if err == nil {
		t.Error("ExpandView(nil) should return error")
	}
}

func TestExpandViewNoSelect(t *testing.T) {
	view := &schema.View{
		Name:   "bad_view",
		Select: nil,
	}

	_, err := ExpandView(view, "")
	if err == nil {
		t.Error("ExpandView() should return error for view without SELECT")
	}
}

func TestCopySelectStmt(t *testing.T) {
	original := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.IdentExpr{Name: "id"}},
			{Expr: &parser.IdentExpr{Name: "name"}},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
		GroupBy: []parser.Expression{
			&parser.IdentExpr{Name: "category"},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: &parser.IdentExpr{Name: "name"}, Asc: true},
		},
	}

	copied := copySelectStmt(original)

	// Verify it's a different instance
	if copied == original {
		t.Error("copySelectStmt() returned same instance")
	}

	// Verify slices are different instances
	if len(copied.Columns) != len(original.Columns) {
		t.Errorf("copied has %d columns, original has %d", len(copied.Columns), len(original.Columns))
	}

	// Modify copy to ensure it doesn't affect original
	copied.Columns[0].Alias = "test"
	if original.Columns[0].Alias == "test" {
		t.Error("modifying copy affected original")
	}
}

func TestCopySelectStmtNil(t *testing.T) {
	copied := copySelectStmt(nil)
	if copied != nil {
		t.Error("copySelectStmt(nil) should return nil")
	}
}

func TestIsViewReference(t *testing.T) {
	s := schema.NewSchema()

	// Add a view
	view := &schema.View{
		Name: "test_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	s.Views["test_view"] = view

	// Test view reference
	if !IsViewReference("test_view", s) {
		t.Error("IsViewReference() should return true for existing view")
	}

	// Test non-view reference
	if IsViewReference("nonexistent", s) {
		t.Error("IsViewReference() should return false for non-existent view")
	}
}

func TestExpandViewsInSelect(t *testing.T) {
	s := schema.NewSchema()

	// Create a simple view that can be flattened
	view := &schema.View{
		Name: "user_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
				{Expr: &parser.IdentExpr{Name: "name"}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		},
	}
	s.Views["user_view"] = view

	// Create a SELECT statement that references the view
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "user_view"},
			},
		},
	}

	// Expand the view
	expanded, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	// For simple views, flattening replaces the view reference with the underlying table
	// and inherits the view's columns
	if expanded.From.Tables[0].TableName != "users" {
		t.Errorf("simple view should be flattened to underlying table 'users', got %q", expanded.From.Tables[0].TableName)
	}

	// SELECT * from a flattened view should use the view's columns
	if len(expanded.Columns) != 2 {
		t.Errorf("expected 2 columns from view, got %d", len(expanded.Columns))
	}
}

func TestExpandViewsInSelectWithJoin(t *testing.T) {
	t.Skip("View expansion in JOINs is not yet implemented - views in JOINs remain as table references")

	s := schema.NewSchema()

	// Create a view
	view := &schema.View{
		Name: "active_users",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		},
	}
	s.Views["active_users"] = view

	// Create a SELECT with JOIN
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "orders"},
			},
			Joins: []parser.JoinClause{
				{
					Type: parser.JoinLeft,
					Table: parser.TableOrSubquery{
						TableName: "active_users",
					},
				},
			},
		},
	}

	// Expand views
	expanded, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	// Check that the view in the JOIN was expanded
	if expanded.From.Joins[0].Table.Subquery == nil {
		t.Error("view in JOIN was not expanded to subquery")
	}
}

func TestExpandViewsInSelectNoViews(t *testing.T) {
	s := schema.NewSchema()

	// Create a SELECT with regular tables
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "users"},
			},
		},
	}

	// Expand (should be no-op)
	expanded, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	// Should return the same statement structure
	if expanded.From.Tables[0].TableName != "users" {
		t.Error("table name should not change when no views are present")
	}

	if expanded.From.Tables[0].Subquery != nil {
		t.Error("regular table should not be converted to subquery")
	}
}

func TestExpandViewsInSelectNilFrom(t *testing.T) {
	s := schema.NewSchema()

	// SELECT without FROM clause
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: &parser.LiteralExpr{Value: "1"}},
		},
		From: nil,
	}

	expanded, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if expanded != stmt {
		t.Error("should return same statement when FROM is nil")
	}
}

func TestExpandViewsInSelectWithExistingSubquery(t *testing.T) {
	s := schema.NewSchema()

	// Create a view
	view := &schema.View{
		Name: "test_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}
	s.Views["test_view"] = view

	// Create a SELECT with an existing subquery
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{
					Subquery: &parser.SelectStmt{
						Columns: []parser.ResultColumn{{Star: true}},
					},
					Alias: "sub",
				},
			},
		},
	}

	// Expand (should not affect existing subquery)
	expanded, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	// Subquery should remain unchanged
	if expanded.From.Tables[0].Subquery == nil {
		t.Error("existing subquery should remain")
	}
	if expanded.From.Tables[0].Alias != "sub" {
		t.Error("alias should remain unchanged")
	}
}

func TestCopyFromJoins(t *testing.T) {
	original := &parser.FromClause{
		Tables: []parser.TableOrSubquery{
			{TableName: "users"},
		},
		Joins: []parser.JoinClause{
			{
				Type: parser.JoinLeft,
				Table: parser.TableOrSubquery{
					TableName: "orders",
				},
			},
			{
				Type: parser.JoinInner,
				Table: parser.TableOrSubquery{
					TableName: "products",
				},
			},
		},
	}

	copied := &parser.FromClause{
		Tables: []parser.TableOrSubquery{
			{TableName: "users"},
		},
	}

	copyFromJoins(copied, original)

	if len(copied.Joins) != 2 {
		t.Errorf("expected 2 joins, got %d", len(copied.Joins))
	}

	if copied.Joins[0].Type != parser.JoinLeft {
		t.Errorf("expected JoinLeft, got %v", copied.Joins[0].Type)
	}

	if copied.Joins[1].Type != parser.JoinInner {
		t.Errorf("expected JoinInner, got %v", copied.Joins[1].Type)
	}
}

func TestExpandViewAsSubquery(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "complex_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
				Joins: []parser.JoinClause{
					{
						Type: parser.JoinLeft,
						Table: parser.TableOrSubquery{
							TableName: "orders",
						},
					},
				},
			},
		},
	}

	table := &parser.TableOrSubquery{
		TableName: "complex_view",
		Alias:     "cv",
	}

	err := expandViewAsSubquery(table, view, s, 0)
	if err != nil {
		t.Fatalf("expandViewAsSubquery() error = %v", err)
	}

	if table.Subquery == nil {
		t.Error("expected subquery to be set")
	}

	if table.Alias != "cv" {
		t.Errorf("expected alias to be 'cv', got %q", table.Alias)
	}

	if table.TableName != "" {
		t.Error("expected table name to be cleared")
	}
}

func TestExpandViewAsSubqueryNoAlias(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "test_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}

	table := &parser.TableOrSubquery{
		TableName: "test_view",
	}

	err := expandViewAsSubquery(table, view, s, 0)
	if err != nil {
		t.Fatalf("expandViewAsSubquery() error = %v", err)
	}

	if table.Alias != "test_view" {
		t.Errorf("expected alias to default to view name, got %q", table.Alias)
	}
}

func TestExpandViewsInSelectWithDepth(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "user_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		},
	}
	s.Views["user_view"] = view

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "user_view"},
			},
		},
	}

	expanded, err := expandViewsInSelectWithDepth(stmt, s, 0)
	if err != nil {
		t.Fatalf("expandViewsInSelectWithDepth() error = %v", err)
	}

	if expanded.From.Tables[0].Subquery == nil {
		t.Error("expected view to be expanded to subquery")
	}
}

func TestExpandViewsInSelectWithDepthLimit(t *testing.T) {
	s := schema.NewSchema()

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "test"},
			},
		},
	}

	_, err := expandViewsInSelectWithDepth(stmt, s, 101)
	if err == nil {
		t.Error("expected error for depth > 100")
	}
}

func TestExpandViewsInFromTables(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "user_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
		},
	}
	s.Views["user_view"] = view

	tables := []parser.TableOrSubquery{
		{TableName: "user_view"},
		{TableName: "orders"},
	}

	err := expandViewsInFromTables(tables, s, 0)
	if err != nil {
		t.Fatalf("expandViewsInFromTables() error = %v", err)
	}

	if tables[0].Subquery == nil {
		t.Error("expected view to be expanded")
	}

	if tables[1].Subquery != nil {
		t.Error("expected non-view to remain unchanged")
	}
}

func TestExpandViewsInFromTablesWithExistingSubquery(t *testing.T) {
	s := schema.NewSchema()

	nestedView := &schema.View{
		Name: "nested_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "products"},
				},
			},
		},
	}
	s.Views["nested_view"] = nestedView

	tables := []parser.TableOrSubquery{
		{
			Subquery: &parser.SelectStmt{
				Columns: []parser.ResultColumn{{Star: true}},
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "nested_view"},
					},
				},
			},
		},
	}

	err := expandViewsInFromTables(tables, s, 0)
	if err != nil {
		t.Fatalf("expandViewsInFromTables() error = %v", err)
	}

	if tables[0].Subquery.From.Tables[0].Subquery == nil {
		t.Error("expected nested view in subquery to be expanded")
	}
}

func TestExpandViewsInJoins(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "order_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "orders"},
				},
			},
		},
	}
	s.Views["order_view"] = view

	joins := []parser.JoinClause{
		{
			Type: parser.JoinLeft,
			Table: parser.TableOrSubquery{
				TableName: "order_view",
			},
		},
	}

	err := expandViewsInJoins(joins, s, 0)
	if err != nil {
		t.Fatalf("expandViewsInJoins() error = %v", err)
	}

	if joins[0].Table.Subquery == nil {
		t.Error("expected view in join to be expanded")
	}
}

func TestExpandViewsInJoinsWithSubquery(t *testing.T) {
	s := schema.NewSchema()

	nestedView := &schema.View{
		Name: "nested_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "products"},
				},
			},
		},
	}
	s.Views["nested_view"] = nestedView

	joins := []parser.JoinClause{
		{
			Type: parser.JoinLeft,
			Table: parser.TableOrSubquery{
				Subquery: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "nested_view"},
						},
					},
				},
			},
		},
	}

	err := expandViewsInJoins(joins, s, 0)
	if err != nil {
		t.Fatalf("expandViewsInJoins() error = %v", err)
	}

	if joins[0].Table.Subquery.From.Tables[0].Subquery == nil {
		t.Error("expected nested view in join subquery to be expanded")
	}
}

func TestExpandViewReference(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "test_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "base_table"},
				},
			},
		},
	}

	table := &parser.TableOrSubquery{
		TableName: "test_view",
		Alias:     "tv",
	}

	err := expandViewReference(table, view, s, 0)
	if err != nil {
		t.Fatalf("expandViewReference() error = %v", err)
	}

	if table.Subquery == nil {
		t.Error("expected subquery to be set")
	}

	if table.TableName != "" {
		t.Error("expected table name to be cleared")
	}

	if table.Alias != "tv" {
		t.Errorf("expected alias 'tv', got %q", table.Alias)
	}
}

func TestExpandViewReferenceNoAlias(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "my_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
		},
	}

	table := &parser.TableOrSubquery{
		TableName: "my_view",
	}

	err := expandViewReference(table, view, s, 0)
	if err != nil {
		t.Fatalf("expandViewReference() error = %v", err)
	}

	if table.Alias != "my_view" {
		t.Errorf("expected alias to default to 'my_view', got %q", table.Alias)
	}
}

func TestExpandViewReferenceRecursive(t *testing.T) {
	s := schema.NewSchema()

	nestedView := &schema.View{
		Name: "nested",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "base"},
				},
			},
		},
	}
	s.Views["nested"] = nestedView

	outerView := &schema.View{
		Name: "outer",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{{Star: true}},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "nested"},
				},
			},
		},
	}

	table := &parser.TableOrSubquery{
		TableName: "outer",
	}

	err := expandViewReference(table, outerView, s, 0)
	if err != nil {
		t.Fatalf("expandViewReference() error = %v", err)
	}

	if table.Subquery == nil {
		t.Error("expected subquery to be set")
	}

	if table.Subquery.From.Tables[0].Subquery == nil {
		t.Error("expected nested view to be expanded recursively")
	}
}

func TestIsSelectStar(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.SelectStmt
		expected bool
	}{
		{
			name: "SELECT *",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Star: true, Table: ""},
				},
			},
			expected: true,
		},
		{
			name: "SELECT table.*",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Star: true, Table: "users"},
				},
			},
			expected: false,
		},
		{
			name: "SELECT id",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Expr: &parser.IdentExpr{Name: "id"}},
				},
			},
			expected: false,
		},
		{
			name: "SELECT *, name",
			stmt: &parser.SelectStmt{
				Columns: []parser.ResultColumn{
					{Star: true},
					{Expr: &parser.IdentExpr{Name: "name"}},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSelectStar(tt.stmt)
			if result != tt.expected {
				t.Errorf("isSelectStar() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCanFlattenView(t *testing.T) {
	tests := []struct {
		name     string
		view     *schema.View
		expected bool
	}{
		{
			name: "simple view",
			view: &schema.View{
				Name: "simple",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "users"},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "view with explicit columns",
			view: &schema.View{
				Name:    "explicit_cols",
				Columns: []string{"id", "name"},
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "users"},
						},
					},
				},
			},
			// Views with explicit columns can now be flattened with proper column mapping
			expected: true,
		},
		{
			name: "view with join",
			view: &schema.View{
				Name: "with_join",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "users"},
						},
						Joins: []parser.JoinClause{
							{Table: parser.TableOrSubquery{TableName: "orders"}},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "view with GROUP BY",
			view: &schema.View{
				Name: "with_groupby",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "users"},
						},
					},
					GroupBy: []parser.Expression{
						&parser.IdentExpr{Name: "dept"},
					},
				},
			},
			expected: false,
		},
		{
			name: "view with LIMIT",
			view: &schema.View{
				Name: "with_limit",
				Select: &parser.SelectStmt{
					Columns: []parser.ResultColumn{{Star: true}},
					From: &parser.FromClause{
						Tables: []parser.TableOrSubquery{
							{TableName: "users"},
						},
					},
					Limit: &parser.LiteralExpr{Value: "10"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := canFlattenView(tt.view)
			if result != tt.expected {
				t.Errorf("canFlattenView() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestHasValidFromClauseForFlattening(t *testing.T) {
	tests := []struct {
		name     string
		sel      *parser.SelectStmt
		expected bool
	}{
		{
			name: "single table",
			sel: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
				},
			},
			expected: true,
		},
		{
			name: "multiple tables",
			sel: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
						{TableName: "orders"},
					},
				},
			},
			expected: false,
		},
		{
			name: "subquery",
			sel: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{Subquery: &parser.SelectStmt{}},
					},
				},
			},
			expected: false,
		},
		{
			name: "with join",
			sel: &parser.SelectStmt{
				From: &parser.FromClause{
					Tables: []parser.TableOrSubquery{
						{TableName: "users"},
					},
					Joins: []parser.JoinClause{
						{Table: parser.TableOrSubquery{TableName: "orders"}},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasValidFromClauseForFlattening(tt.sel)
			if result != tt.expected {
				t.Errorf("hasValidFromClauseForFlattening() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestHasNoComplexFeatures(t *testing.T) {
	tests := []struct {
		name     string
		sel      *parser.SelectStmt
		expected bool
	}{
		{
			name:     "simple SELECT",
			sel:      &parser.SelectStmt{},
			expected: true,
		},
		{
			name: "with GROUP BY",
			sel: &parser.SelectStmt{
				GroupBy: []parser.Expression{
					&parser.IdentExpr{Name: "dept"},
				},
			},
			expected: false,
		},
		{
			name: "with HAVING",
			sel: &parser.SelectStmt{
				Having: &parser.BinaryExpr{},
			},
			expected: false,
		},
		{
			name: "with LIMIT",
			sel: &parser.SelectStmt{
				Limit: &parser.LiteralExpr{Value: "10"},
			},
			expected: false,
		},
		{
			name: "with OFFSET",
			sel: &parser.SelectStmt{
				Offset: &parser.LiteralExpr{Value: "5"},
			},
			expected: false,
		},
		{
			name: "with UNION",
			sel: &parser.SelectStmt{
				Compound: &parser.CompoundSelect{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasNoComplexFeatures(tt.sel)
			if result != tt.expected {
				t.Errorf("hasNoComplexFeatures() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFlattenSimpleView(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name: "simple_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "id"}},
				{Expr: &parser.IdentExpr{Name: "name"}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "users"},
				},
			},
			Where: &parser.BinaryExpr{
				Left:  &parser.IdentExpr{Name: "active"},
				Op:    parser.OpEq,
				Right: &parser.LiteralExpr{Value: "1"},
			},
		},
	}

	outer := &parser.SelectStmt{
		Columns: []parser.ResultColumn{{Star: true}},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{
				{TableName: "simple_view"},
			},
		},
		Where: &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: "id"},
			Op:    parser.OpGt,
			Right: &parser.LiteralExpr{Value: "100"},
		},
	}

	result, err := flattenSimpleView(outer, 0, view, s, 0)
	if err != nil {
		t.Fatalf("flattenSimpleView() error = %v", err)
	}

	if result.From.Tables[0].TableName != "users" {
		t.Errorf("expected table name 'users', got %q", result.From.Tables[0].TableName)
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns from view, got %d", len(result.Columns))
	}

	binaryWhere, ok := result.Where.(*parser.BinaryExpr)
	if !ok || binaryWhere.Op != parser.OpAnd {
		t.Error("expected WHERE clauses to be combined with AND")
	}
}
