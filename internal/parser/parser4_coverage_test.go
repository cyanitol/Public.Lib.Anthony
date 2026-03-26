// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestParser4 targets branches that remain uncovered after prior test suites.
// Focuses on: GROUPS window frame, IS DISTINCT FROM, JSON arrow operators,
// EXISTS subquery, multi-statement parsing, table-valued functions, REPLACE INTO,
// NOT LIKE/GLOB, RAISE(FAIL/ROLLBACK), parseNotPatternOp, parseReplaceInto,
// parseTableFuncArgs, and various error recovery paths.
func TestParser4(t *testing.T) {
	t.Parallel()

	t.Run("GroupsWindowFrameNamedWindow", testP4GroupsWindowNamedWindow)
	t.Run("GroupsWindowFrameInlineWithPartition", testP4GroupsWindowInlinePartition)
	t.Run("IsDistinctFrom", testP4IsDistinctFrom)
	t.Run("IsNotDistinctFrom", testP4IsNotDistinctFrom)
	t.Run("JSONArrowOperator", testP4JSONArrow)
	t.Run("JSONDoubleArrowOperator", testP4JSONDoubleArrow)
	t.Run("ExistsSubquery", testP4ExistsSubquery)
	t.Run("MultiStatementSemicolon", testP4MultiStatement)
	t.Run("LeadingSemicolon", testP4LeadingSemicolon)
	t.Run("TableValuedFunction", testP4TableValuedFunction)
	t.Run("ReplaceInto", testP4ReplaceInto)
	t.Run("NotLike", testP4NotLike)
	t.Run("NotGlob", testP4NotGlob)
	t.Run("RaiseFailInTrigger", testP4RaiseFailInTrigger)
	t.Run("RaiseRollbackInTrigger", testP4RaiseRollbackInTrigger)
	t.Run("RaiseIgnoreInTrigger", testP4RaiseIgnoreInTrigger)
	t.Run("ErrorUnknownStatement", testP4ErrorUnknownStatement)
	t.Run("ErrorMalformedSelect", testP4ErrorMalformedSelect)
	t.Run("ErrorMissingFromAfterDistinct", testP4ErrorMissingFromAfterDistinct)
	t.Run("JSONArrowChained", testP4JSONArrowChained)
	t.Run("GroupsFrameExcludeCurrentRow", testP4GroupsFrameExcludeCurrentRow)
	t.Run("IsDistinctFromErrorMissingFrom", testP4IsDistinctFromErrorMissingFrom)
	t.Run("IsNotDistinctFromErrorMissingFrom", testP4IsNotDistinctFromErrorMissingFrom)
	t.Run("ParseSelectHaving", testP4ParseSelectHaving)
	t.Run("OrderByClauseError", testP4OrderByClauseError)
	t.Run("GroupByClauseError", testP4GroupByClauseError)
}

// --- GROUPS window frame in a named WINDOW clause ---

func testP4GroupsWindowNamedWindow(t *testing.T) {
	t.Parallel()
	// Named window definition with GROUPS mode exercises parseWindowDef+frame path.
	sql := `SELECT SUM(a) OVER w FROM t WINDOW w AS (PARTITION BY b GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.WindowDefs) != 1 {
		t.Fatalf("expected 1 window def, got %d", len(sel.WindowDefs))
	}
	wd := sel.WindowDefs[0]
	if wd.Spec == nil || wd.Spec.Frame == nil {
		t.Fatal("expected frame in named window def")
	}
	if wd.Spec.Frame.Mode != FrameGroups {
		t.Errorf("frame mode: got %v, want FrameGroups", wd.Spec.Frame.Mode)
	}
	if wd.Spec.Frame.Start.Type != BoundPreceding {
		t.Errorf("start bound: got %v, want BoundPreceding", wd.Spec.Frame.Start.Type)
	}
	if wd.Spec.Frame.End.Type != BoundFollowing {
		t.Errorf("end bound: got %v, want BoundFollowing", wd.Spec.Frame.End.Type)
	}
}

func testP4GroupsWindowInlinePartition(t *testing.T) {
	t.Parallel()
	// Inline window spec using GROUPS with PARTITION BY.
	sql := `SELECT SUM(a) OVER (PARTITION BY b GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn := sel.Columns[0].Expr.(*FunctionExpr)
	if fn.Over == nil || fn.Over.Frame == nil {
		t.Fatal("expected frame in inline window")
	}
	if fn.Over.Frame.Mode != FrameGroups {
		t.Errorf("frame mode: got %v, want FrameGroups", fn.Over.Frame.Mode)
	}
}

// --- IS DISTINCT FROM / IS NOT DISTINCT FROM ---

func testP4IsDistinctFrom(t *testing.T) {
	t.Parallel()
	sql := `SELECT a IS DISTINCT FROM b FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	bin, ok := sel.Columns[0].Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Columns[0].Expr)
	}
	if bin.Op != OpIsDistinctFrom {
		t.Errorf("op: got %v, want OpIsDistinctFrom", bin.Op)
	}
}

func testP4IsNotDistinctFrom(t *testing.T) {
	t.Parallel()
	sql := `SELECT a IS NOT DISTINCT FROM b FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	bin, ok := sel.Columns[0].Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Columns[0].Expr)
	}
	if bin.Op != OpIsNotDistinctFrom {
		t.Errorf("op: got %v, want OpIsNotDistinctFrom", bin.Op)
	}
}

// --- JSON arrow operators ---

func testP4JSONArrow(t *testing.T) {
	t.Parallel()
	sql := `SELECT t.data->'$.key' FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn, ok := sel.Columns[0].Expr.(*FunctionExpr)
	if !ok {
		t.Fatalf("expected FunctionExpr (JSON_EXTRACT), got %T", sel.Columns[0].Expr)
	}
	if fn.Name != "JSON_EXTRACT" {
		t.Errorf("function name: got %q, want JSON_EXTRACT", fn.Name)
	}
	if len(fn.Args) != 2 {
		t.Errorf("arg count: got %d, want 2", len(fn.Args))
	}
}

func testP4JSONDoubleArrow(t *testing.T) {
	t.Parallel()
	sql := `SELECT t.data->>'$.key' FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	fn, ok := sel.Columns[0].Expr.(*FunctionExpr)
	if !ok {
		t.Fatalf("expected FunctionExpr (JSON_EXTRACT_TEXT), got %T", sel.Columns[0].Expr)
	}
	if fn.Name != "JSON_EXTRACT_TEXT" {
		t.Errorf("function name: got %q, want JSON_EXTRACT_TEXT", fn.Name)
	}
}

func testP4JSONArrowChained(t *testing.T) {
	t.Parallel()
	// Chain multiple -> operators to exercise the loop in parseJSONArrowOps.
	sql := `SELECT data->'$.a'->'$.b' FROM t`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	outer, ok := sel.Columns[0].Expr.(*FunctionExpr)
	if !ok {
		t.Fatalf("expected outer FunctionExpr, got %T", sel.Columns[0].Expr)
	}
	if outer.Name != "JSON_EXTRACT" {
		t.Errorf("outer function: got %q, want JSON_EXTRACT", outer.Name)
	}
	// Inner arg should also be JSON_EXTRACT from the first arrow.
	if _, ok := outer.Args[0].(*FunctionExpr); !ok {
		t.Errorf("inner arg: expected FunctionExpr, got %T", outer.Args[0])
	}
}

// --- EXISTS subquery ---

func testP4ExistsSubquery(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE EXISTS (SELECT 1)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	exists, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected ExistsExpr, got %T", sel.Where)
	}
	if exists.Not {
		t.Error("expected Not=false for plain EXISTS")
	}
	if exists.Select == nil {
		t.Error("expected non-nil subquery")
	}
}

// --- Multi-statement with semicolons ---

func testP4MultiStatement(t *testing.T) {
	t.Parallel()
	sql := `SELECT 1; SELECT 2; SELECT 3`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) != 3 {
		t.Errorf("expected 3 statements, got %d", len(stmts))
	}
}

func testP4LeadingSemicolon(t *testing.T) {
	t.Parallel()
	// Leading semicolons should be skipped by parseStatements.
	sql := `; ; SELECT 42`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}
}

// --- Table-valued function in FROM clause ---

func testP4TableValuedFunction(t *testing.T) {
	t.Parallel()
	// json_each(data) exercises parseTableFuncArgs (71.4% coverage target).
	sql := `SELECT key, value FROM json_each(data)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.From == nil || len(sel.From.Tables) == 0 {
		t.Fatal("expected FROM clause with tables")
	}
	tbl := &sel.From.Tables[0]
	if tbl.TableName != "json_each" {
		t.Errorf("table name: got %q, want json_each", tbl.TableName)
	}
	if len(tbl.FuncArgs) != 1 {
		t.Errorf("func args: got %d, want 1", len(tbl.FuncArgs))
	}
}

// --- REPLACE INTO ---

func testP4ReplaceInto(t *testing.T) {
	t.Parallel()
	sql := `REPLACE INTO t(a, b) VALUES(1, 2)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ins := stmts[0].(*InsertStmt)
	if ins.OnConflict != OnConflictReplace {
		t.Errorf("OnConflict: got %v, want OnConflictReplace", ins.OnConflict)
	}
	if ins.Table != "t" {
		t.Errorf("table: got %q, want t", ins.Table)
	}
}

// --- NOT LIKE / NOT GLOB (parseNotPatternOp 71.4%) ---

func testP4NotLike(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE name NOT LIKE '%foo%'`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	// NOT LIKE wraps a UnaryExpr{OpNot, BinaryExpr{OpLike}}
	unary, ok := sel.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", sel.Where)
	}
	if unary.Op != OpNot {
		t.Errorf("unary op: got %v, want OpNot", unary.Op)
	}
	bin, ok := unary.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("inner expr: expected BinaryExpr, got %T", unary.Expr)
	}
	if bin.Op != OpLike {
		t.Errorf("binary op: got %v, want OpLike", bin.Op)
	}
}

func testP4NotGlob(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE name NOT GLOB 'foo*'`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	unary, ok := sel.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", sel.Where)
	}
	if unary.Op != OpNot {
		t.Errorf("op: got %v, want OpNot", unary.Op)
	}
	bin, ok := unary.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("inner: expected BinaryExpr, got %T", unary.Expr)
	}
	if bin.Op != OpGlob {
		t.Errorf("binary op: got %v, want OpGlob", bin.Op)
	}
}

// --- RAISE function variants ---

func testP4RaiseFailInTrigger(t *testing.T) {
	t.Parallel()
	// RAISE(FAIL, msg) exercises the FAIL branch of isRaiseAction.
	sql := `CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT RAISE(FAIL, 'error'); END`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func testP4RaiseRollbackInTrigger(t *testing.T) {
	t.Parallel()
	// RAISE(ROLLBACK, msg) exercises the ROLLBACK branch of isRaiseAction.
	sql := `CREATE TRIGGER tr BEFORE DELETE ON t BEGIN SELECT RAISE(ROLLBACK, 'rollback msg'); END`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func testP4RaiseIgnoreInTrigger(t *testing.T) {
	t.Parallel()
	// RAISE(IGNORE) exercises the IGNORE path (no message expected).
	sql := `CREATE TRIGGER tr AFTER UPDATE ON t BEGIN SELECT RAISE(IGNORE); END`
	_, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// --- Error recovery paths ---

func testP4ErrorUnknownStatement(t *testing.T) {
	t.Parallel()
	// An entirely unknown keyword triggers the "expected statement" error path.
	_, err := NewParser(`FROBNICATE foo`).Parse()
	if err == nil {
		t.Error("expected error for unknown statement keyword")
	}
}

func testP4ErrorMalformedSelect(t *testing.T) {
	t.Parallel()
	// SELECT with no columns triggers the column-list error path.
	_, err := NewParser(`SELECT FROM t`).Parse()
	if err == nil {
		t.Error("expected error for SELECT with no columns")
	}
}

func testP4ErrorMissingFromAfterDistinct(t *testing.T) {
	t.Parallel()
	// IS DISTINCT missing FROM keyword triggers parseIsDistinctFrom error.
	_, err := NewParser(`SELECT a IS DISTINCT b FROM t`).Parse()
	if err == nil {
		t.Error("expected error for IS DISTINCT without FROM")
	}
}

// --- IS DISTINCT FROM error path: missing FROM after IS NOT DISTINCT ---

func testP4IsDistinctFromErrorMissingFrom(t *testing.T) {
	t.Parallel()
	_, err := NewParser(`SELECT a IS DISTINCT 1 FROM t`).Parse()
	if err == nil {
		t.Error("expected error for IS DISTINCT without FROM keyword")
	}
}

func testP4IsNotDistinctFromErrorMissingFrom(t *testing.T) {
	t.Parallel()
	_, err := NewParser(`SELECT a IS NOT DISTINCT 1 FROM t`).Parse()
	if err == nil {
		t.Error("expected error for IS NOT DISTINCT without FROM keyword")
	}
}

// --- GROUPS frame with EXCLUDE CURRENT ROW in named window ---

func testP4GroupsFrameExcludeCurrentRow(t *testing.T) {
	t.Parallel()
	sql := `SELECT SUM(a) OVER w FROM t WINDOW w AS (ORDER BY b GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW)`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.WindowDefs) != 1 {
		t.Fatalf("expected 1 window def, got %d", len(sel.WindowDefs))
	}
	frame := sel.WindowDefs[0].Spec.Frame
	if frame == nil {
		t.Fatal("expected frame")
	}
	if frame.Mode != FrameGroups {
		t.Errorf("mode: got %v, want FrameGroups", frame.Mode)
	}
	if frame.Exclude != ExcludeCurrentRow {
		t.Errorf("exclude: got %v, want ExcludeCurrentRow", frame.Exclude)
	}
}

// --- SELECT with HAVING but no GROUP BY (parseGroupByClauseInto HAVING branch) ---

func testP4ParseSelectHaving(t *testing.T) {
	t.Parallel()
	// HAVING without GROUP BY is syntactically valid in SQLite.
	sql := `SELECT count(*) FROM t HAVING count(*) > 0`
	stmts, err := NewParser(sql).Parse()
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if sel.Having == nil {
		t.Error("expected non-nil Having clause")
	}
}

// --- ORDER BY without BY keyword triggers error in parseOrderByClauseInto ---

func testP4OrderByClauseError(t *testing.T) {
	t.Parallel()
	_, err := NewParser(`SELECT 1 FROM t ORDER a`).Parse()
	if err == nil {
		t.Error("expected error for ORDER without BY")
	}
}

// --- GROUP BY without BY keyword triggers error in parseGroupByClauseInto ---

func testP4GroupByClauseError(t *testing.T) {
	t.Parallel()
	_, err := NewParser(`SELECT a FROM t GROUP a`).Parse()
	if err == nil {
		t.Error("expected error for GROUP without BY")
	}
}
