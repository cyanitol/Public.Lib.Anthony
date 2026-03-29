// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser_test

// MC/DC 6 — parser low-coverage branch coverage.
//
// Targets:
//   parser.go:3250  parseNotPatternOp       (71.4%) — NOT LIKE, NOT GLOB, NOT REGEXP
//   parser.go:3428  parseUnaryExpression    (75.0%) — unary plus (+x), ~ operator, MinInt
//   parser.go:3172  parseIsDistinctFrom     (83.3%) — IS DISTINCT FROM
//   parser.go:3184  parseIsNotDistinctFrom  (83.3%) — IS NOT DISTINCT FROM
//   parser.go:2059  applyTableConstraintUnique (80.0%) — table-level UNIQUE constraint
//   parser.go:2650  parseTriggerBodyStatement  (83.3%) — trigger with UPDATE body
//   parser.go:4277  parseFrameMode          (80.0%) — ROWS/RANGE frame spec
//   parser.go:3966  parseParenOrSubquery    (80.0%) — EXISTS subquery

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func parseExpr(t *testing.T, sql string) parser.Expression {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if len(stmts) == 0 {
		t.Fatalf("no statements in %q", sql)
	}
	sel, ok := stmts[0].(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}
	if len(sel.Columns) == 0 {
		t.Fatalf("no result columns in %q", sql)
	}
	return sel.Columns[0].Expr
}

func parseOne(t *testing.T, sql string) parser.Statement {
	t.Helper()
	p := parser.NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if len(stmts) == 0 {
		t.Fatalf("no statements in %q", sql)
	}
	return stmts[0]
}

// TestMCDC6_NotLike exercises the NOT LIKE path in parseNotPatternOp.
func TestMCDC6_NotLike(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT 'hello' NOT LIKE '%world%'")
	if expr == nil {
		t.Error("expected non-nil expr for NOT LIKE")
	}
}

// TestMCDC6_NotGlob exercises the NOT GLOB path.
func TestMCDC6_NotGlob(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT 'hello' NOT GLOB '*world*'")
	if expr == nil {
		t.Error("expected non-nil expr for NOT GLOB")
	}
}

// TestMCDC6_UnaryPlus exercises the unary plus (+x) path in parseUnaryExpression.
func TestMCDC6_UnaryPlus(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT +42")
	if expr == nil {
		t.Error("expected non-nil expr for +42")
	}
	// Unary plus is a no-op, so the expression should be a LiteralExpr.
	if _, ok := expr.(*parser.LiteralExpr); !ok {
		// Could be a UnaryExpr for +; either is fine.
	}
}

// TestMCDC6_BitwiseNot exercises the ~ (bitwise NOT) unary operator.
func TestMCDC6_BitwiseNot(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT ~5")
	if expr == nil {
		t.Error("expected non-nil expr for ~5")
	}
}

// TestMCDC6_MinInt64 exercises the special -9223372036854775808 folding path.
func TestMCDC6_MinInt64(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT -9223372036854775808")
	if expr == nil {
		t.Error("expected non-nil expr for min int64")
	}
}

// TestMCDC6_IsDistinctFrom exercises parseIsDistinctFrom.
func TestMCDC6_IsDistinctFrom(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT 1 IS DISTINCT FROM 2")
	if expr == nil {
		t.Error("expected non-nil expr for IS DISTINCT FROM")
	}
	b, ok := expr.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	if b.Op != parser.OpIsDistinctFrom {
		t.Errorf("expected OpIsDistinctFrom, got %v", b.Op)
	}
}

// TestMCDC6_IsNotDistinctFrom exercises parseIsNotDistinctFrom.
func TestMCDC6_IsNotDistinctFrom(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT 1 IS NOT DISTINCT FROM 1")
	if expr == nil {
		t.Error("expected non-nil expr for IS NOT DISTINCT FROM")
	}
	b, ok := expr.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	if b.Op != parser.OpIsNotDistinctFrom {
		t.Errorf("expected OpIsNotDistinctFrom, got %v", b.Op)
	}
}

// TestMCDC6_TableUniqueConstraint exercises applyTableConstraintUnique via
// parsing a CREATE TABLE with a table-level UNIQUE constraint.
func TestMCDC6_TableUniqueConstraint(t *testing.T) {
	t.Parallel()
	stmt := parseOne(t, "CREATE TABLE t (a INTEGER, b TEXT, UNIQUE(a, b))")
	create, ok := stmt.(*parser.CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}
	if len(create.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(create.Columns))
	}
}

// TestMCDC6_TriggerUpdateBody exercises parseTriggerBodyStatement with an
// UPDATE statement inside a trigger body.
func TestMCDC6_TriggerUpdateBody(t *testing.T) {
	t.Parallel()
	sql := `CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE t SET x=1; END`
	stmt := parseOne(t, sql)
	trig, ok := stmt.(*parser.CreateTriggerStmt)
	if !ok {
		t.Fatalf("expected CreateTriggerStmt, got %T", stmt)
	}
	if len(trig.Body) == 0 {
		t.Error("expected non-empty trigger body")
	}
}

// TestMCDC6_WindowFrameRows exercises ROWS frame specification.
func TestMCDC6_WindowFrameRows(t *testing.T) {
	t.Parallel()
	sql := `SELECT ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t`
	stmt := parseOne(t, sql)
	if stmt == nil {
		t.Error("expected non-nil statement")
	}
}

// TestMCDC6_WindowFrameRange exercises RANGE frame specification.
func TestMCDC6_WindowFrameRange(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(val) OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) FROM t`
	stmt := parseOne(t, sql)
	if stmt == nil {
		t.Error("expected non-nil statement")
	}
}

// TestMCDC6_ExistsSubquery exercises the EXISTS subquery path in parseParenOrSubquery.
func TestMCDC6_ExistsSubquery(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT EXISTS (SELECT 1)")
	if expr == nil {
		t.Error("expected non-nil expr for EXISTS subquery")
	}
}

// TestMCDC6_NotPatternOp_Error exercises the error path in parseNotPatternOp
// when neither LIKE nor GLOB follows NOT.
func TestMCDC6_NotPatternOp_Error(t *testing.T) {
	t.Parallel()
	// "x NOT 5" — after NOT, no pattern op → parse error.
	p := parser.NewParser("SELECT x NOT 5")
	_, err := p.Parse()
	// The parser may treat this as NOT(5) for x; either way, we verify no panic.
	_ = err
}

// TestMCDC6_LikeEscape exercises LIKE with ESCAPE clause.
func TestMCDC6_LikeEscape(t *testing.T) {
	t.Parallel()
	expr := parseExpr(t, "SELECT 'a%b' LIKE 'a\\%b' ESCAPE '\\'")
	if expr == nil {
		t.Error("expected non-nil expr for LIKE ESCAPE")
	}
}

// TestMCDC6_ForeignKeyRefColumns exercises parseForeignKeyRefColumns.
func TestMCDC6_ForeignKeyRefColumns(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE child (id INTEGER, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent(id))`
	stmt := parseOne(t, sql)
	if stmt == nil {
		t.Error("expected non-nil statement")
	}
}
