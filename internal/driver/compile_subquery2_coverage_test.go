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

// TestCompileSubquery2Coverage covers dead-code and low-coverage functions in
// compile_subquery.go that are not exercised by the normal integration tests.
func TestCompileSubquery2Coverage(t *testing.T) {
	// -------------------------------------------------------------------------
	// hasFromSubqueries
	// -------------------------------------------------------------------------

	t.Run("hasFromSubqueries/NilFrom", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		stmt := &parser.SelectStmt{From: nil}
		if s.hasFromSubqueries(stmt) {
			t.Error("expected false for nil From, got true")
		}
	})

	t.Run("hasFromSubqueries/TableOnly", func(t *testing.T) {
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

	t.Run("hasFromSubqueries/FromSubquery", func(t *testing.T) {
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

	t.Run("hasFromSubqueries/JoinSubquery", func(t *testing.T) {
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

	t.Run("hasFromSubqueries/JoinNoSubquery", func(t *testing.T) {
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

	// -------------------------------------------------------------------------
	// resolveAggColumnIndex
	// -------------------------------------------------------------------------

	t.Run("resolveAggColumnIndex/NoArgs", func(t *testing.T) {
		t.Parallel()
		fn := &parser.FunctionExpr{Name: "SUM", Args: nil}
		_, err := resolveAggColumnIndex(fn, []string{"a", "b"})
		if err == nil {
			t.Error("expected error for function with no args, got nil")
		}
	})

	t.Run("resolveAggColumnIndex/NonIdentArg", func(t *testing.T) {
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

	t.Run("resolveAggColumnIndex/ColumnNotFound", func(t *testing.T) {
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

	t.Run("resolveAggColumnIndex/Found", func(t *testing.T) {
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

	// -------------------------------------------------------------------------
	// evalWhereOnRow
	// -------------------------------------------------------------------------

	t.Run("evalWhereOnRow/UnhandledExpr", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		// LiteralExpr is not *BinaryExpr — hits the default (conservative true) branch.
		e := &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}
		row := []interface{}{int64(42)}
		colNames := []string{"x"}
		if !s.evalWhereOnRow(e, row, colNames) {
			t.Error("expected true (conservative) for unhandled expression type")
		}
	})

	t.Run("evalWhereOnRow/BinaryEqTrue", func(t *testing.T) {
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

	t.Run("evalWhereOnRow/BinaryEqFalse", func(t *testing.T) {
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

	// -------------------------------------------------------------------------
	// findSubqueryColumn
	// -------------------------------------------------------------------------

	t.Run("findSubqueryColumn/Found", func(t *testing.T) {
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

	t.Run("findSubqueryColumn/NotFound", func(t *testing.T) {
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

	t.Run("findSubqueryColumn/SynthesizesMissingASTNode", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		// subqueryColumns has two entries but subquery.Columns has only one,
		// so index 1 ("b") triggers the synthesize branch.
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

	t.Run("findSubqueryColumn/CompoundFallback", func(t *testing.T) {
		t.Parallel()
		s := newSubquery2Stmt(t)
		// subquery.Columns is empty but subquery.Compound is set — triggers
		// compoundLeafColumns fallback.
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
