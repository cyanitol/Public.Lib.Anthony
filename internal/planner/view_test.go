package planner

import (
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
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
