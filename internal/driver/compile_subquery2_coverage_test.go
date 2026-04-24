// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// newSubquery2Stmt opens an in-memory Conn and returns a *Stmt for calling
// unexported methods directly.
func newSubquery2Stmt(t *testing.T) *Stmt {
	t.Helper()
	conn := openMemConn(t)
	return stmtFor(conn)
}

// TestCompileSubquery2Coverage_HasFromSubqueries covers hasFromSubqueries branches.
func TestCompileSubquery2Coverage_HasFromSubqueries(t *testing.T) {
	t.Run("NilFrom", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		stmt := &parser.SelectStmt{From: nil}
		if s.hasFromSubqueries(stmt) {
			t.Error("expected false for nil From, got true")
		}
	})

	t.Run("TableOnly", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "t"},
				},
			},
		}
		if s.hasFromSubqueries(stmt) {
			t.Error("expected false for plain table, got true")
		}
	})

	t.Run("FromSubquery", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		innerStmt := &parser.SelectStmt{}
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{Subquery: innerStmt},
				},
			},
		}
		if !s.hasFromSubqueries(stmt) {
			t.Error("expected true for FROM subquery, got false")
		}
	})

	t.Run("JoinSubquery", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		innerStmt := &parser.SelectStmt{}
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "t"},
				},
				Joins: []parser.JoinClause{
					{Table: parser.TableOrSubquery{Subquery: innerStmt}},
				},
			},
		}
		if !s.hasFromSubqueries(stmt) {
			t.Error("expected true for JOIN subquery, got false")
		}
	})

	t.Run("JoinNoSubquery", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{
					{TableName: "t"},
				},
				Joins: []parser.JoinClause{
					{Table: parser.TableOrSubquery{TableName: "u"}},
				},
			},
		}
		if s.hasFromSubqueries(stmt) {
			t.Error("expected false for JOIN with plain table, got true")
		}
	})
}

// TestCompileSubquery2Coverage_ResolveAggColumnIndex covers resolveAggColumnIndex branches.
func TestCompileSubquery2Coverage_ResolveAggColumnIndex(t *testing.T) {
	t.Run("NoArgs", func(t *testing.T) {
		t.Parallel()
		fn := &parser.FunctionExpr{Name: "SUM", Args: nil}
		_, err := resolveAggColumnIndex(fn, []string{"a", "b"})
		if err == nil {
			t.Error("expected error for function with no args, got nil")
		}
	})

	t.Run("NonIdentArg", func(t *testing.T) {
		t.Parallel()
		fn := &parser.FunctionExpr{
			Name: "SUM",
			Args: []parser.Expression{
				&parser.LiteralExpr{Type: parser.LiteralInteger, Value: "1"},
			},
		}
		_, err := resolveAggColumnIndex(fn, []string{"a", "b"})
		if err == nil {
			t.Error("expected error for non-ident arg, got nil")
		}
	})

	t.Run("ColumnNotFound", func(t *testing.T) {
		t.Parallel()
		fn := &parser.FunctionExpr{
			Name: "SUM",
			Args: []parser.Expression{
				&parser.IdentExpr{Name: "missing"},
			},
		}
		_, err := resolveAggColumnIndex(fn, []string{"a", "b"})
		if err == nil {
			t.Error("expected error for unknown column, got nil")
		}
	})

	t.Run("Found", func(t *testing.T) {
		t.Parallel()
		fn := &parser.FunctionExpr{
			Name: "SUM",
			Args: []parser.Expression{
				&parser.IdentExpr{Name: "b"},
			},
		}
		idx, err := resolveAggColumnIndex(fn, []string{"a", "b", "c"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx != 1 {
			t.Errorf("expected index 1, got %d", idx)
		}
	})
}

// TestCompileSubquery2Coverage_EvalWhereOnRow covers evalWhereOnRow branches.
func TestCompileSubquery2Coverage_EvalWhereOnRow(t *testing.T) {
	t.Run("UnhandledExpr", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}
		row := []interface{}{int64(42)}
		colNames := []string{"x"}
		if !s.evalWhereOnRow(e, row, colNames) {
			t.Error("expected true (conservative) for unhandled expression type")
		}
	})

	t.Run("BinaryEqTrue", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		e := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "x"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "42"},
		}
		row := []interface{}{int64(42)}
		colNames := []string{"x"}
		if !s.evalWhereOnRow(e, row, colNames) {
			t.Error("expected true for 42 = 42")
		}
	})

	t.Run("BinaryEqFalse", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		e := &parser.BinaryExpr{
			Op:    parser.OpEq,
			Left:  &parser.IdentExpr{Name: "x"},
			Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "99"},
		}
		row := []interface{}{int64(42)}
		colNames := []string{"x"}
		if s.evalWhereOnRow(e, row, colNames) {
			t.Error("expected false for 42 = 99")
		}
	})
}

// TestCompileSubquery2Coverage_FindSubqueryColumn_Basic covers found/not-found branches.
func TestCompileSubquery2Coverage_FindSubqueryColumn_Basic(t *testing.T) {
	t.Run("Found", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		subquery := &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "a"}},
				{Expr: &parser.IdentExpr{Name: "b"}},
			},
		}
		subqueryColumns := []string{"a", "b"}
		col, err := s.findSubqueryColumn("b", subquery, subqueryColumns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ident, ok := col.Expr.(*parser.IdentExpr)
		if !ok || ident.Name != "b" {
			t.Errorf("expected column b, got %v", col.Expr)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		subquery := &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "a"}},
			},
		}
		subqueryColumns := []string{"a"}
		_, err := s.findSubqueryColumn("missing", subquery, subqueryColumns)
		if err == nil {
			t.Error("expected error for missing column, got nil")
		}
	})
}

// TestCompileSubquery2Coverage_FindSubqueryColumn_Synthesize covers synthesize and compound branches.
func TestCompileSubquery2Coverage_FindSubqueryColumn_Synthesize(t *testing.T) {
	t.Run("SynthesizesMissingASTNode", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		subquery := &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "a"}},
			},
		}
		subqueryColumns := []string{"a", "b"}
		col, err := s.findSubqueryColumn("b", subquery, subqueryColumns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ident, ok := col.Expr.(*parser.IdentExpr)
		if !ok || ident.Name != "b" {
			t.Errorf("expected synthesized IdentExpr{b}, got %v", col.Expr)
		}
	})

	t.Run("CompoundFallback", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		leaf := &parser.SelectStmt{
			Columns: []parser.ResultColumn{
				{Expr: &parser.IdentExpr{Name: "v"}},
			},
		}
		subquery := &parser.SelectStmt{
			Columns: nil,
			Compound: &parser.CompoundSelect{
				Op:    parser.CompoundUnionAll,
				Left:  leaf,
				Right: &parser.SelectStmt{},
			},
		}
		subqueryColumns := []string{"v"}
		col, err := s.findSubqueryColumn("v", subquery, subqueryColumns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ident, ok := col.Expr.(*parser.IdentExpr)
		if !ok || ident.Name != "v" {
			t.Errorf("expected column v from compound leaf, got %v", col.Expr)
		}
	})
}
