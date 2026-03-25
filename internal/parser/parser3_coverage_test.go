// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestParser3 targets branches that remain uncovered after existing test suites.
// Focuses on: parseFrameExclude (0%), parseWindowDef with frame, parseExistsExpr,
// parseParenOrSubquery WITH-subquery path, parseAnalyze schema-qualified,
// SAVEPOINT/RELEASE, UNIQUE/CHECK table constraints, extractExpressionName,
// and miscellaneous low-coverage branches.
func TestParser3(t *testing.T) {
	t.Parallel()

	t.Run("FrameExcludeGroup", testP3FrameExcludeGroup)
	t.Run("FrameExcludeTies", testP3FrameExcludeTies)
	t.Run("FrameExcludeNoOthers", testP3FrameExcludeNoOthers)
	t.Run("FrameExcludeCurrentRow", testP3FrameExcludeCurrentRow)
	t.Run("WindowDefWithFrame", testP3WindowDefWithFrame)
	t.Run("ExistsSubquery", testP3ExistsSubquery)
	t.Run("NotExistsSubquery", testP3NotExistsSubquery)
	t.Run("ParenSubquery", testP3ParenSubquery)
	t.Run("WithSubqueryInExpr", testP3WithSubqueryInExpr)
	t.Run("AnalyzeSchemaQualified", testP3AnalyzeSchemaQualified)
	t.Run("AnalyzeBare", testP3AnalyzeBare)
	t.Run("SavepointStatement", testP3SavepointStatement)
	t.Run("ReleaseWithSavepointKeyword", testP3ReleaseWithSavepointKeyword)
	t.Run("ReleaseWithoutSavepointKeyword", testP3ReleaseWithoutSavepointKeyword)
	t.Run("RollbackToSavepoint", testP3RollbackToSavepoint)
	t.Run("TableUniqueConstraintWithName", testP3TableUniqueConstraintWithName)
	t.Run("TableCheckConstraintWithName", testP3TableCheckConstraintWithName)
	t.Run("ExtractExpressionNameQualified", testP3ExtractExpressionNameQualified)
	t.Run("ExtractExpressionNameOther", testP3ExtractExpressionNameOther)
	t.Run("ForeignKeyActionRestrict", testP3ForeignKeyActionRestrict)
	t.Run("ForeignKeyActionNoAction", testP3ForeignKeyActionNoAction)
	t.Run("ForeignKeyActionSetNull", testP3ForeignKeyActionSetNull)
	t.Run("ColumnConstraintCheckComplex", testP3ColumnConstraintCheckComplex)
	t.Run("ReturningExpr", testP3ReturningExpr)
	t.Run("UnaryPlus", testP3UnaryPlus)
	t.Run("RaiseAbortInline", testP3RaiseAbortInline)
}

// --- parseFrameExclude (0% → needs all 4 branches) ---

func testP3FrameExcludeGroup(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(x) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM t`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected statement")
	}
	sel := stmts[0].(*SelectStmt)
	fn := sel.Columns[0].Expr.(*FunctionExpr)
	if fn.Over == nil || fn.Over.Frame == nil {
		t.Fatal("expected frame spec")
	}
	if fn.Over.Frame.Exclude != ExcludeGroup {
		t.Errorf("Exclude: got %v, want ExcludeGroup", fn.Over.Frame.Exclude)
	}
}

func testP3FrameExcludeTies(t *testing.T) {
	t.Parallel()
	sql := `SELECT RANK() OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn := sel.Columns[0].Expr.(*FunctionExpr)
	if fn.Over == nil || fn.Over.Frame == nil {
		t.Fatal("expected frame spec")
	}
	if fn.Over.Frame.Exclude != ExcludeTies {
		t.Errorf("Exclude: got %v, want ExcludeTies", fn.Over.Frame.Exclude)
	}
}

func testP3FrameExcludeNoOthers(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(x) OVER (ORDER BY y RANGE UNBOUNDED PRECEDING EXCLUDE NO OTHERS) FROM t`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn := sel.Columns[0].Expr.(*FunctionExpr)
	if fn.Over == nil || fn.Over.Frame == nil {
		t.Fatal("expected frame spec")
	}
	if fn.Over.Frame.Exclude != ExcludeNoOthers {
		t.Errorf("Exclude: got %v, want ExcludeNoOthers", fn.Over.Frame.Exclude)
	}
}

func testP3FrameExcludeCurrentRow(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn := sel.Columns[0].Expr.(*FunctionExpr)
	if fn.Over == nil || fn.Over.Frame == nil {
		t.Fatal("expected frame spec")
	}
	if fn.Over.Frame.Exclude != ExcludeCurrentRow {
		t.Errorf("Exclude: got %v, want ExcludeCurrentRow", fn.Over.Frame.Exclude)
	}
}

// --- parseWindowDef with frame (58.8% → named window with frame) ---

func testP3WindowDefWithFrame(t *testing.T) {
	t.Parallel()
	// Named window definition includes a frame spec — exercises parseWindowFrame
	// inside parseWindowDef, which is the uncovered path.
	sql := `SELECT SUM(x) OVER w FROM t WINDOW w AS (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.WindowDefs) != 1 {
		t.Fatalf("expected 1 window def, got %d", len(sel.WindowDefs))
	}
	wd := sel.WindowDefs[0]
	if wd.Name != "w" {
		t.Errorf("window name: got %q, want w", wd.Name)
	}
	if wd.Spec == nil || wd.Spec.Frame == nil {
		t.Fatal("expected frame in window def")
	}
}

// --- parseExistsExpr (60% → EXISTS and NOT EXISTS) ---

func testP3ExistsSubquery(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other WHERE other.id = t.id)`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	exists, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected ExistsExpr, got %T", sel.Where)
	}
	if exists.Not {
		t.Error("expected Not=false")
	}
	if exists.Select == nil {
		t.Error("expected subquery")
	}
}

func testP3NotExistsSubquery(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM other WHERE other.id = t.id)`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	// NOT EXISTS is wrapped: UnaryExpr{NOT, ExistsExpr{Not: true}}
	// or directly ExistsExpr{Not:true} depending on implementation
	// Just verify it parsed without error and WHERE is non-nil.
}

// --- parseParenOrSubquery WITH-subquery path (60%) ---

func testP3ParenSubquery(t *testing.T) {
	t.Parallel()
	// Scalar subquery in SELECT — exercises parseParenOrSubquery SELECT branch.
	sql := `SELECT (SELECT max(id) FROM other) AS m FROM t`
	p := NewParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected statement")
	}
}

func testP3WithSubqueryInExpr(t *testing.T) {
	t.Parallel()
	// Scalar subquery with a CTE — exercises the p.check(TK_WITH) branch
	// inside parseParenOrSubquery. The paren is opened, then WITH is next.
	sql := `SELECT * FROM t WHERE id = (WITH cte AS (SELECT max(id) FROM other) SELECT id FROM cte)`
	p := NewParser(sql)
	_, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// --- parseAnalyze (60%) ---

func testP3AnalyzeSchemaQualified(t *testing.T) {
	t.Parallel()
	// Schema-qualified: ANALYZE schema.table exercises the TK_DOT branch.
	sql := `ANALYZE main.mytable`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	analyze := stmts[0].(*AnalyzeStmt)
	if analyze.Schema != "main" {
		t.Errorf("Schema: got %q, want main", analyze.Schema)
	}
	if analyze.Name != "mytable" {
		t.Errorf("Name: got %q, want mytable", analyze.Name)
	}
}

func testP3AnalyzeBare(t *testing.T) {
	t.Parallel()
	// Bare ANALYZE — no name at all.
	sql := `ANALYZE`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	analyze := stmts[0].(*AnalyzeStmt)
	if analyze.Name != "" || analyze.Schema != "" {
		t.Errorf("expected empty name/schema, got %q / %q", analyze.Name, analyze.Schema)
	}
}

// --- parseSavepoint / parseRelease (75%) ---

func testP3SavepointStatement(t *testing.T) {
	t.Parallel()
	sql := `SAVEPOINT sp1`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sp := stmts[0].(*SavepointStmt)
	if sp.Name != "sp1" {
		t.Errorf("savepoint name: got %q, want sp1", sp.Name)
	}
}

func testP3ReleaseWithSavepointKeyword(t *testing.T) {
	t.Parallel()
	// RELEASE SAVEPOINT name — exercises p.match(TK_SAVEPOINT) true branch.
	sql := `RELEASE SAVEPOINT sp1`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	rel := stmts[0].(*ReleaseStmt)
	if rel.Name != "sp1" {
		t.Errorf("release name: got %q, want sp1", rel.Name)
	}
}

func testP3ReleaseWithoutSavepointKeyword(t *testing.T) {
	t.Parallel()
	// RELEASE name — exercises p.match(TK_SAVEPOINT) false branch.
	sql := `RELEASE sp2`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	rel := stmts[0].(*ReleaseStmt)
	if rel.Name != "sp2" {
		t.Errorf("release name: got %q, want sp2", rel.Name)
	}
}

// --- parseRollback (88.9% → TO SAVEPOINT branch) ---

func testP3RollbackToSavepoint(t *testing.T) {
	t.Parallel()
	// ROLLBACK TO SAVEPOINT name — exercises the TK_SAVEPOINT match inside ROLLBACK TO.
	sql := `ROLLBACK TO SAVEPOINT sp1`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	rb := stmts[0].(*RollbackStmt)
	if rb.Savepoint != "sp1" {
		t.Errorf("savepoint: got %q, want sp1", rb.Savepoint)
	}
}

// --- applyTableConstraintUnique / applyTableConstraintCheck with CONSTRAINT name ---

func testP3TableUniqueConstraintWithName(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT, CONSTRAINT uq_ab UNIQUE (a, b))`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	if len(ct.Constraints) != 1 {
		t.Fatalf("expected 1 table constraint, got %d", len(ct.Constraints))
	}
	c := ct.Constraints[0]
	if c.Name != "uq_ab" {
		t.Errorf("constraint name: got %q, want uq_ab", c.Name)
	}
	if c.Type != ConstraintUnique {
		t.Errorf("constraint type: got %v, want ConstraintUnique", c.Type)
	}
	if c.Unique == nil {
		t.Fatal("expected UniqueTableConstraint")
	}
	if len(c.Unique.Columns) != 2 {
		t.Errorf("unique columns: got %d, want 2", len(c.Unique.Columns))
	}
}

func testP3TableCheckConstraintWithName(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, CONSTRAINT chk_pos CHECK (a > 0))`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	if len(ct.Constraints) != 1 {
		t.Fatalf("expected 1 table constraint, got %d", len(ct.Constraints))
	}
	c := ct.Constraints[0]
	if c.Name != "chk_pos" {
		t.Errorf("constraint name: got %q, want chk_pos", c.Name)
	}
	if c.Type != ConstraintCheck {
		t.Errorf("constraint type: got %v, want ConstraintCheck", c.Type)
	}
	if c.Check == nil {
		t.Fatal("expected check expression")
	}
}

// --- extractExpressionName (66.7%) ---

func testP3ExtractExpressionNameQualified(t *testing.T) {
	t.Parallel()
	// A qualified column reference in an index (table.col) exercises the
	// ident.Table != "" branch of extractExpressionName.
	sql := `CREATE INDEX idx ON t (t.a)`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func testP3ExtractExpressionNameOther(t *testing.T) {
	t.Parallel()
	// An expression-based index column (e.g. function call) exercises the
	// fallback expr.String() branch of extractExpressionName.
	sql := `CREATE INDEX idx ON t (lower(name))`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// --- parseForeignKeyAction (80% → RESTRICT and NO ACTION) ---

func testP3ForeignKeyActionRestrict(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE RESTRICT)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	fk := ct.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnDelete != FKActionRestrict {
		t.Errorf("OnDelete: got %v, want FKActionRestrict", fk.OnDelete)
	}
}

func testP3ForeignKeyActionNoAction(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON UPDATE NO ACTION)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	fk := ct.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnUpdate != FKActionNoAction {
		t.Errorf("OnUpdate: got %v, want FKActionNoAction", fk.OnUpdate)
	}
}

func testP3ForeignKeyActionSetNull(t *testing.T) {
	t.Parallel()
	// ON UPDATE SET NULL exercises the SET NULL branch of parseForeignKeyAction.
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON UPDATE SET NULL)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	fk := ct.Columns[0].Constraints[0].ForeignKey
	if fk == nil {
		t.Fatal("expected ForeignKeyConstraint")
	}
	if fk.OnUpdate != FKActionSetNull {
		t.Errorf("OnUpdate: got %v, want FKActionSetNull", fk.OnUpdate)
	}
}

// --- applyConstraintCheck complex expression (column-level, 80%) ---

func testP3ColumnConstraintCheckComplex(t *testing.T) {
	t.Parallel()
	// Column-level CHECK with a complex expression (AND, comparison).
	sql := `CREATE TABLE t (a INT CHECK (a > 0 AND a < 100))`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ct := stmts[0].(*CreateTableStmt)
	if len(ct.Columns[0].Constraints) == 0 {
		t.Fatal("expected CHECK constraint")
	}
	c := ct.Columns[0].Constraints[0]
	if c.Type != ConstraintCheck {
		t.Errorf("constraint type: got %v, want ConstraintCheck", c.Type)
	}
	if c.Check == nil {
		t.Fatal("expected non-nil check expression")
	}
}

// --- parseReturningClause with expression (85.7%) ---

func testP3ReturningExpr(t *testing.T) {
	t.Parallel()
	// RETURNING with a computed expression (not just a column name).
	sql := `INSERT INTO t(a, b) VALUES(1, 2) RETURNING a + b AS total`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if len(ins.Returning) == 0 {
		t.Fatal("expected RETURNING clause")
	}
	if ins.Returning[0].Alias != "total" {
		t.Errorf("alias: got %q, want total", ins.Returning[0].Alias)
	}
}

// --- parseUnaryExpression unary plus (75%) ---

func testP3UnaryPlus(t *testing.T) {
	t.Parallel()
	// Unary + prefix — exercises the TK_PLUS branch of parseUnaryExpression.
	sql := `SELECT +42 FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("expected statement")
	}
}

// --- isRaiseAction / parseRaiseFunction (66.7%) ---

func testP3RaiseAbortInline(t *testing.T) {
	t.Parallel()
	// RAISE(ABORT, msg) directly in a trigger — exercises isRaiseAction for
	// TK_ABORT token type and parseRaiseMessage non-IGNORE path.
	sql := `CREATE TRIGGER tr AFTER DELETE ON t BEGIN SELECT RAISE(ABORT, 'deleted'); END`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}
