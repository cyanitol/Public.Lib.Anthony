// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// makeIdentExpr creates an IdentExpr for testing.
func makeIdentExpr(name string) *parser.IdentExpr {
	return &parser.IdentExpr{Name: name}
}

// makeColumnMap creates a simple column map for testing.
func makeColumnMap(from, to string) map[string]parser.Expression {
	return map[string]parser.Expression{
		from: makeIdentExpr(to),
	}
}

// TestRewriteColumnReferencesNil ensures nil returns nil.
func TestRewriteColumnReferencesNil(t *testing.T) {
	result := rewriteColumnReferences(nil, makeColumnMap("a", "b"))
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

// TestRewriteIdentExprMapped verifies mapped ident is rewritten.
func TestRewriteIdentExprMapped(t *testing.T) {
	colMap := makeColumnMap("x", "a")
	expr := makeIdentExpr("x")
	result := rewriteColumnReferences(expr, colMap)
	ident, ok := result.(*parser.IdentExpr)
	if !ok {
		t.Fatalf("expected *parser.IdentExpr, got %T", result)
	}
	if ident.Name != "a" {
		t.Errorf("expected name 'a', got %q", ident.Name)
	}
}

// TestRewriteIdentExprUnmapped verifies unmapped ident is unchanged.
func TestRewriteIdentExprUnmapped(t *testing.T) {
	colMap := makeColumnMap("x", "a")
	expr := makeIdentExpr("y")
	result := rewriteColumnReferences(expr, colMap)
	ident, ok := result.(*parser.IdentExpr)
	if !ok {
		t.Fatalf("expected *parser.IdentExpr, got %T", result)
	}
	if ident.Name != "y" {
		t.Errorf("expected name 'y', got %q", ident.Name)
	}
}

// TestRewriteBinaryExpr verifies both sides of binary expression are rewritten.
func TestRewriteBinaryExpr(t *testing.T) {
	colMap := map[string]parser.Expression{
		"a": makeIdentExpr("col_a"),
		"b": makeIdentExpr("col_b"),
	}
	expr := &parser.BinaryExpr{
		Left:  makeIdentExpr("a"),
		Op:    parser.OpPlus,
		Right: makeIdentExpr("b"),
	}
	result := rewriteColumnReferences(expr, colMap)
	bin, ok := result.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected *parser.BinaryExpr, got %T", result)
	}
	leftIdent, ok := bin.Left.(*parser.IdentExpr)
	if !ok || leftIdent.Name != "col_a" {
		t.Errorf("left should be 'col_a', got %v", bin.Left)
	}
	rightIdent, ok := bin.Right.(*parser.IdentExpr)
	if !ok || rightIdent.Name != "col_b" {
		t.Errorf("right should be 'col_b', got %v", bin.Right)
	}
}

// TestRewriteUnaryExpr verifies the inner expression of a unary expr is rewritten.
func TestRewriteUnaryExpr(t *testing.T) {
	colMap := makeColumnMap("a", "col_a")
	expr := &parser.UnaryExpr{
		Op:   parser.OpNeg,
		Expr: makeIdentExpr("a"),
	}
	result := rewriteColumnReferences(expr, colMap)
	unary, ok := result.(*parser.UnaryExpr)
	if !ok {
		t.Fatalf("expected *parser.UnaryExpr, got %T", result)
	}
	inner, ok := unary.Expr.(*parser.IdentExpr)
	if !ok || inner.Name != "col_a" {
		t.Errorf("inner should be 'col_a', got %v", unary.Expr)
	}
}

// TestRewriteFunctionExpr verifies function arguments are rewritten.
func TestRewriteFunctionExpr(t *testing.T) {
	colMap := makeColumnMap("b", "col_b")
	expr := &parser.FunctionExpr{
		Name: "upper",
		Args: []parser.Expression{makeIdentExpr("b")},
	}
	result := rewriteColumnReferences(expr, colMap)
	fn, ok := result.(*parser.FunctionExpr)
	if !ok {
		t.Fatalf("expected *parser.FunctionExpr, got %T", result)
	}
	arg, ok := fn.Args[0].(*parser.IdentExpr)
	if !ok || arg.Name != "col_b" {
		t.Errorf("arg should be 'col_b', got %v", fn.Args[0])
	}
}

// TestRewriteCaseExpr verifies CASE expression WHEN/THEN/ELSE are rewritten.
func TestRewriteCaseExpr(t *testing.T) {
	colMap := map[string]parser.Expression{
		"a":   makeIdentExpr("col_a"),
		"pos": makeIdentExpr("col_pos"),
		"neg": makeIdentExpr("col_neg"),
	}
	expr := &parser.CaseExpr{
		WhenClauses: []parser.WhenClause{
			{
				Condition: &parser.BinaryExpr{
					Left:  makeIdentExpr("a"),
					Op:    parser.OpGt,
					Right: &parser.LiteralExpr{Value: "0"},
				},
				Result: makeIdentExpr("pos"),
			},
		},
		ElseClause: makeIdentExpr("neg"),
	}
	result := rewriteColumnReferences(expr, colMap)
	caseExpr, ok := result.(*parser.CaseExpr)
	if !ok {
		t.Fatalf("expected *parser.CaseExpr, got %T", result)
	}
	elseIdent, ok := caseExpr.ElseClause.(*parser.IdentExpr)
	if !ok || elseIdent.Name != "col_neg" {
		t.Errorf("else should be 'col_neg', got %v", caseExpr.ElseClause)
	}
	condBin, ok := caseExpr.WhenClauses[0].Condition.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("condition should be *parser.BinaryExpr, got %T", caseExpr.WhenClauses[0].Condition)
	}
	condLeft, ok := condBin.Left.(*parser.IdentExpr)
	if !ok || condLeft.Name != "col_a" {
		t.Errorf("condition left should be 'col_a', got %v", condBin.Left)
	}
}

// TestRewriteCastExpr verifies CAST expression inner is rewritten.
func TestRewriteCastExpr(t *testing.T) {
	colMap := makeColumnMap("c", "col_c")
	expr := &parser.CastExpr{
		Expr: makeIdentExpr("c"),
		Type: "INT",
	}
	result := rewriteColumnReferences(expr, colMap)
	castExpr, ok := result.(*parser.CastExpr)
	if !ok {
		t.Fatalf("expected *parser.CastExpr, got %T", result)
	}
	inner, ok := castExpr.Expr.(*parser.IdentExpr)
	if !ok || inner.Name != "col_c" {
		t.Errorf("cast inner should be 'col_c', got %v", castExpr.Expr)
	}
}

// TestRewriteCollateExpr verifies COLLATE expression inner is rewritten.
func TestRewriteCollateExpr(t *testing.T) {
	colMap := makeColumnMap("b", "col_b")
	expr := &parser.CollateExpr{
		Expr:      makeIdentExpr("b"),
		Collation: "NOCASE",
	}
	result := rewriteColumnReferences(expr, colMap)
	collateExpr, ok := result.(*parser.CollateExpr)
	if !ok {
		t.Fatalf("expected *parser.CollateExpr, got %T", result)
	}
	inner, ok := collateExpr.Expr.(*parser.IdentExpr)
	if !ok || inner.Name != "col_b" {
		t.Errorf("collate inner should be 'col_b', got %v", collateExpr.Expr)
	}
}

// TestRewriteParenExpr verifies parenthesized expression inner is rewritten.
func TestRewriteParenExpr(t *testing.T) {
	colMap := makeColumnMap("a", "col_a")
	expr := &parser.ParenExpr{
		Expr: makeIdentExpr("a"),
	}
	result := rewriteColumnReferences(expr, colMap)
	parenExpr, ok := result.(*parser.ParenExpr)
	if !ok {
		t.Fatalf("expected *parser.ParenExpr, got %T", result)
	}
	inner, ok := parenExpr.Expr.(*parser.IdentExpr)
	if !ok || inner.Name != "col_a" {
		t.Errorf("paren inner should be 'col_a', got %v", parenExpr.Expr)
	}
}

// TestRewriteLiteralExprPassthrough verifies literal expressions pass through unchanged.
func TestRewriteLiteralExprPassthrough(t *testing.T) {
	colMap := makeColumnMap("x", "y")
	expr := &parser.LiteralExpr{Value: "42"}
	result := rewriteColumnReferences(expr, colMap)
	lit, ok := result.(*parser.LiteralExpr)
	if !ok {
		t.Fatalf("expected *parser.LiteralExpr, got %T", result)
	}
	if lit.Value != "42" {
		t.Errorf("literal value should be '42', got %q", lit.Value)
	}
}

// TestApplyViewColumnMappingWithExplicitColumns exercises applyViewColumnMapping
// and the rewriteSelectColumns/rewriteWhereClause/rewriteOrderByClause/rewriteGroupByClause
// code paths by flattening a view with explicit column names.
func TestApplyViewColumnMappingWithExplicitColumns(t *testing.T) {
	s := schema.NewSchema()

	// View: CREATE VIEW v(id, label) AS SELECT pk, name FROM base
	view := &schema.View{
		Name:    "v",
		Columns: []string{"id", "label"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("pk")},
				{Expr: makeIdentExpr("name")},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "base"}},
			},
		},
	}
	s.Views["v"] = view

	// Outer: SELECT id, label FROM v WHERE id > 5 ORDER BY label GROUP BY id
	outer := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("id")},
			{Expr: makeIdentExpr("label")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "v"}},
		},
		Where: &parser.BinaryExpr{
			Left:  makeIdentExpr("id"),
			Op:    parser.OpGt,
			Right: &parser.LiteralExpr{Value: "5"},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: makeIdentExpr("label"), Asc: true},
		},
		GroupBy: []parser.Expression{makeIdentExpr("id")},
	}

	result, err := ExpandViewsInSelect(outer, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	// After flattening, the table should be "base"
	if result.From.Tables[0].TableName != "base" {
		t.Errorf("expected table 'base', got %q", result.From.Tables[0].TableName)
	}
}

// TestApplyViewColumnMappingHavingClause exercises rewriteHavingClause path.
func TestApplyViewColumnMappingHavingClause(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name:    "agg_view",
		Columns: []string{"dept", "cnt"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("department")},
				{Expr: &parser.FunctionExpr{Name: "count", Args: []parser.Expression{&parser.LiteralExpr{Value: "*"}}}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "employees"}},
			},
		},
	}
	s.Views["agg_view"] = view

	outer := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("dept")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "agg_view"}},
		},
		Having: &parser.BinaryExpr{
			Left:  makeIdentExpr("cnt"),
			Op:    parser.OpGt,
			Right: &parser.LiteralExpr{Value: "3"},
		},
	}

	result, err := ExpandViewsInSelect(outer, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result.From.Tables[0].TableName != "employees" {
		t.Errorf("expected 'employees', got %q", result.From.Tables[0].TableName)
	}
}

// TestRewriteWithNestedViewExplicitColumns exercises dispatchRewrite when an outer
// query references a view with explicit columns that itself references another view.
func TestRewriteWithNestedViewExplicitColumns(t *testing.T) {
	s := schema.NewSchema()

	// Inner view: CREATE VIEW base_view AS SELECT a, b FROM t
	innerView := &schema.View{
		Name: "base_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("a")},
				{Expr: makeIdentExpr("b")},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "t"}},
			},
		},
	}
	s.Views["base_view"] = innerView

	// Outer view: CREATE VIEW outer_view(x, y) AS SELECT a, b FROM base_view
	outerView := &schema.View{
		Name:    "outer_view",
		Columns: []string{"x", "y"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("a")},
				{Expr: makeIdentExpr("b")},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "base_view"}},
			},
		},
	}
	s.Views["outer_view"] = outerView

	// Query: SELECT x FROM outer_view WHERE y > 0
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("x")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "outer_view"}},
		},
		Where: &parser.BinaryExpr{
			Left:  makeIdentExpr("y"),
			Op:    parser.OpGt,
			Right: &parser.LiteralExpr{Value: "0"},
		},
	}

	result, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// TestRewriteSelectColumnsAliasPreservation exercises extractOriginalName and
// preserveColumnAlias through the applyViewColumnMapping path.
func TestRewriteSelectColumnsAliasPreservation(t *testing.T) {
	s := schema.NewSchema()

	// View with explicit column names that map ident to ident
	view := &schema.View{
		Name:    "alias_view",
		Columns: []string{"item_id", "item_name"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("product_id")},
				{Expr: makeIdentExpr("product_name")},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "products"}},
			},
		},
	}
	s.Views["alias_view"] = view

	// Query referencing the view columns
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("item_id")},
			{Expr: makeIdentExpr("item_name")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "alias_view"}},
		},
	}

	result, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result.From.Tables[0].TableName != "products" {
		t.Errorf("expected 'products', got %q", result.From.Tables[0].TableName)
	}
}

// TestApplySelectAliasMappingWithAliasedColumns exercises the applySelectAliasMapping
// and buildSelectAliasMap paths for views with aliased SELECT columns.
func TestApplySelectAliasMappingWithAliasedColumns(t *testing.T) {
	s := schema.NewSchema()

	// View: CREATE VIEW v AS SELECT a AS id, b AS label FROM t
	view := &schema.View{
		Name: "alias_map_view",
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("a"), Alias: "id"},
				{Expr: makeIdentExpr("b"), Alias: "label"},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "t"}},
			},
		},
	}
	s.Views["alias_map_view"] = view

	// Outer query referencing view alias names
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("id")},
			{Expr: makeIdentExpr("label")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "alias_map_view"}},
		},
		Where: &parser.BinaryExpr{
			Left:  makeIdentExpr("id"),
			Op:    parser.OpEq,
			Right: &parser.LiteralExpr{Value: "1"},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: makeIdentExpr("label"), Asc: true},
		},
		GroupBy: []parser.Expression{makeIdentExpr("id")},
	}

	result, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result.From.Tables[0].TableName != "t" {
		t.Errorf("expected 't', got %q", result.From.Tables[0].TableName)
	}
}

// TestRewriteComplexExpressionsViaColumnMap exercises all complex expression
// types (CastExpr, CollateExpr, ParenExpr, CaseExpr, FunctionExpr, UnaryExpr)
// through the column mapping rewrite path.
func TestRewriteComplexExpressionsViaColumnMap(t *testing.T) {
	s := schema.NewSchema()

	// Build a view with explicit column names mapping "expr_col" to a complex expression
	// The outer query uses "expr_col" which will be rewritten to the complex expression.
	view := &schema.View{
		Name:    "complex_view",
		Columns: []string{"cast_col", "collate_col", "paren_col", "case_col", "fn_col", "neg_col"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.CastExpr{Expr: makeIdentExpr("c"), Type: "INT"}},
				{Expr: &parser.CollateExpr{Expr: makeIdentExpr("b"), Collation: "NOCASE"}},
				{Expr: &parser.ParenExpr{Expr: makeIdentExpr("a")}},
				{Expr: &parser.CaseExpr{
					WhenClauses: []parser.WhenClause{
						{Condition: makeIdentExpr("a"), Result: &parser.LiteralExpr{Value: "'yes'"}},
					},
					ElseClause: &parser.LiteralExpr{Value: "'no'"},
				}},
				{Expr: &parser.FunctionExpr{Name: "upper", Args: []parser.Expression{makeIdentExpr("b")}}},
				{Expr: &parser.UnaryExpr{Op: parser.OpNeg, Expr: makeIdentExpr("a")}},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "data"}},
			},
		},
	}
	s.Views["complex_view"] = view

	// Outer query selecting each of the complex view columns
	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("cast_col")},
			{Expr: makeIdentExpr("collate_col")},
			{Expr: makeIdentExpr("paren_col")},
			{Expr: makeIdentExpr("case_col")},
			{Expr: makeIdentExpr("fn_col")},
			{Expr: makeIdentExpr("neg_col")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "complex_view"}},
		},
	}

	result, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result.From.Tables[0].TableName != "data" {
		t.Errorf("expected 'data', got %q", result.From.Tables[0].TableName)
	}

	if len(result.Columns) != 6 {
		t.Errorf("expected 6 columns, got %d", len(result.Columns))
	}
}

// TestDispatchWrapperRewriteUnknownType exercises the default branch of
// dispatchWrapperRewrite by passing an expression type not handled by any case.
func TestDispatchWrapperRewriteUnknownType(t *testing.T) {
	colMap := makeColumnMap("x", "y")
	// InExpr is not handled by dispatchWrapperRewrite, so it returns as-is
	expr := &parser.InExpr{
		Expr:   makeIdentExpr("x"),
		Values: []parser.Expression{&parser.LiteralExpr{Value: "1"}},
	}
	result := dispatchWrapperRewrite(expr, colMap)
	if result != expr {
		t.Error("expected same expression returned for unhandled type")
	}
}

// TestCopyExpressionTypes exercises copyExpression for each supported type.
func TestCopyExpressionTypes_SimpleTypes(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if copyExpression(nil) != nil {
			t.Error("expected nil")
		}
	})

	t.Run("IdentExpr", func(t *testing.T) {
		orig := makeIdentExpr("col")
		cp := copyExpression(orig)
		ident, ok := cp.(*parser.IdentExpr)
		if !ok || ident == orig {
			t.Error("expected deep copy of IdentExpr")
		}
	})

	t.Run("LiteralExpr", func(t *testing.T) {
		orig := &parser.LiteralExpr{Value: "42"}
		cp := copyExpression(orig)
		lit, ok := cp.(*parser.LiteralExpr)
		if !ok || lit == orig {
			t.Error("expected deep copy of LiteralExpr")
		}
	})

	t.Run("UnknownType", func(t *testing.T) {
		orig := &parser.InExpr{Expr: makeIdentExpr("x")}
		cp := copyExpression(orig)
		if cp != orig {
			t.Error("expected same instance for unknown type")
		}
	})
}

func TestCopyExpressionTypes_BinaryExpr(t *testing.T) {
	orig := &parser.BinaryExpr{
		Left:  makeIdentExpr("a"),
		Op:    parser.OpPlus,
		Right: makeIdentExpr("b"),
	}
	cp := copyExpression(orig)
	bin, ok := cp.(*parser.BinaryExpr)
	if !ok || bin == orig {
		t.Error("expected deep copy of BinaryExpr")
	}
	if bin.Left == orig.Left {
		t.Error("expected deep copy of Left")
	}
}

func TestCopyExpressionTypes_UnaryAndFunctionExpr(t *testing.T) {
	t.Run("UnaryExpr", func(t *testing.T) {
		orig := &parser.UnaryExpr{Op: parser.OpNeg, Expr: makeIdentExpr("a")}
		cp := copyExpression(orig)
		unary, ok := cp.(*parser.UnaryExpr)
		if !ok || unary == orig {
			t.Error("expected deep copy of UnaryExpr")
		}
	})

	t.Run("FunctionExpr", func(t *testing.T) {
		orig := &parser.FunctionExpr{Name: "lower", Args: []parser.Expression{makeIdentExpr("x")}}
		cp := copyExpression(orig)
		fn, ok := cp.(*parser.FunctionExpr)
		if !ok || fn == orig {
			t.Error("expected deep copy of FunctionExpr")
		}
		if len(fn.Args) != 1 {
			t.Error("expected args to be copied")
		}
	})
}

// TestRewriteSelectColumnsWithOrderAndGroup exercises rewriteOrderByClause and
// rewriteGroupByClause through applyViewColumnMapping.
func TestRewriteSelectColumnsWithOrderAndGroup(t *testing.T) {
	s := schema.NewSchema()

	view := &schema.View{
		Name:    "order_group_view",
		Columns: []string{"amount"},
		Select: &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: makeIdentExpr("total")},
			},
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "sales"}},
			},
		},
	}
	s.Views["order_group_view"] = view

	stmt := &parser.SelectStmt{
		Columns: []parser.ResultColumn{
			{Expr: makeIdentExpr("amount")},
		},
		From: &parser.FromClause{
			Tables: []parser.TableOrSubquery{{TableName: "order_group_view"}},
		},
		OrderBy: []parser.OrderingTerm{
			{Expr: makeIdentExpr("amount"), Asc: false},
		},
		GroupBy: []parser.Expression{makeIdentExpr("amount")},
	}

	result, err := ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("ExpandViewsInSelect() error = %v", err)
	}

	if result.From.Tables[0].TableName != "sales" {
		t.Errorf("expected 'sales', got %q", result.From.Tables[0].TableName)
	}
}
