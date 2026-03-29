// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// =============================================================================
// MC/DC Coverage Tests — parser_mcdc5_test.go
//
// Covers the following functions identified as below-target:
//   - parseNotPatternOp (71.4%)
//   - parseTableFuncArgs (71.4%)
//   - parseUnaryExpression (75%)
//   - applyConstraintGenerated (76.5%)
//   - parseForeignKeyAction (80%)
//   - applyTableConstraintUnique (80%)
//   - applyTableConstraintCheck (80%)
//   - parseRelease (80%)
//   - parseParenOrSubquery (80%)
//   - parseFrameExclude (80%)
//   - parseFrameMode (80%)
// =============================================================================

// ---------------------------------------------------------------------------
// parseNotPatternOp — 71.4%
//
// The function consumes NOT then delegates to tryParsePatternOp.
// Uncovered paths: each of the supported NOT variants and the error branch
// when no pattern operator follows NOT.
//
// MC/DC condition: expr == nil (no recognisable pattern op after NOT)
//   true  → error returned
//   false → UnaryExpr{Not} wrapping the BinaryExpr returned
// ---------------------------------------------------------------------------

func TestMCDC5Parser_NotLike(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE x NOT LIKE 'a%'`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("NOT LIKE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_NotGlob(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE x NOT GLOB '*a'`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("NOT GLOB: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// NOT REGEXP and NOT MATCH do not reach parseNotPatternOp via the WHERE parse
// path — the gate at parseComparisonExpression only checks NOT LIKE / NOT GLOB.
// Instead exercise the LIKE with ESCAPE clause (another branch in tryParsePatternOp).
func TestMCDC5Parser_LikeWithEscape(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE x LIKE 'a\%' ESCAPE '\'`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("LIKE with ESCAPE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_NotIn(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE x NOT IN (1, 2, 3)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("NOT IN: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_NotBetween(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE x NOT BETWEEN 1 AND 10`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("NOT BETWEEN: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error branch: NOT followed by a token that is not a pattern operator.
// parseNotPatternOp returns an error when tryParsePatternOp returns nil.
func TestMCDC5Parser_NotPatternOp_Error_NoBranchToken(t *testing.T) {
	t.Parallel()
	// NOT followed by a plain integer literal — no pattern op.
	sql := `SELECT x FROM t WHERE x NOT 42`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when NOT is followed by a non-pattern token")
	}
}

// ---------------------------------------------------------------------------
// parseTableFuncArgs — 71.4%
//
// Table-valued function calls inside FROM clauses.
// Uncovered path: the error branch when ')' is missing after the argument list.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_TableFuncArgs_OneArg(t *testing.T) {
	t.Parallel()
	sql := `SELECT value FROM json_each(data)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("json_each one arg: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_TableFuncArgs_TwoArgs(t *testing.T) {
	t.Parallel()
	sql := `SELECT key, value FROM json_tree(data, '$')`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("json_tree two args: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_TableFuncArgs_SeriesThreeArgs(t *testing.T) {
	t.Parallel()
	sql := `SELECT value FROM generate_series(1, 10, 2)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("generate_series three args: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error branch: missing ')' after table function arguments.
func TestMCDC5Parser_TableFuncArgs_Error_MissingParen(t *testing.T) {
	t.Parallel()
	// The closing paren is replaced by WHERE; parser should error.
	sql := `SELECT value FROM json_each(data WHERE 1=1`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when table func args missing closing paren")
	}
}

// ---------------------------------------------------------------------------
// parseUnaryExpression — 75%
//
// Branches:
//   1. TK_PLUS  → recursive call (unary plus, no-op)
//   2. NOT EXISTS → parseExistsExpr(true)
//   3. NOT <expr> → parseUnaryExprWithOp(OpNot)
//   4. - or ~   → parseUnaryExprWithOp(...)
//   5. plain    → parsePostfixExpression
//
// Uncovered: unary plus pass-through and NOT EXISTS path.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_UnaryPlus(t *testing.T) {
	t.Parallel()
	sql := `SELECT +42`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("unary plus: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_NotExists(t *testing.T) {
	t.Parallel()
	sql := `SELECT x FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("NOT EXISTS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_UnaryBitNot(t *testing.T) {
	t.Parallel()
	sql := `SELECT ~0`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("unary bitnot: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyConstraintGenerated — 76.5%
//
// Branches:
//   A. GENERATED ALWAYS AS (expr) STORED
//   B. GENERATED ALWAYS AS (expr) VIRTUAL
//   C. AS (expr)               (no ALWAYS keyword)
//   Error paths: missing AS, missing LP, missing RP
//
// Uncovered: VIRTUAL keyword present explicitly; missing-AS error.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_GeneratedAlways_Stored(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a*2) STORED)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("GENERATED ALWAYS AS STORED: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_GeneratedAlways_Virtual(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a+1) VIRTUAL)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("GENERATED ALWAYS AS VIRTUAL: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_GeneratedAlways_DefaultVirtual(t *testing.T) {
	t.Parallel()
	// No STORED or VIRTUAL keyword — defaults to virtual.
	sql := `CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a+1))`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("GENERATED ALWAYS AS (default virtual): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_GeneratedAs_NoAlways(t *testing.T) {
	t.Parallel()
	// GENERATED AS (...) — GENERATED keyword present but no ALWAYS keyword.
	// The TK_GENERATED handler fires; then p.match(TK_ALWAYS) fails (not present);
	// then p.match(TK_AS) succeeds.
	sql := `CREATE TABLE t (a INT, b INT GENERATED AS (a*3) STORED)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("GENERATED AS (expr) STORED without ALWAYS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error: GENERATED without AS keyword following.
func TestMCDC5Parser_GeneratedAlways_Error_NoAS(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT GENERATED ALWAYS (a*2))`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when GENERATED ALWAYS is not followed by AS")
	}
}

// Error: GENERATED ALWAYS AS without opening paren.
func TestMCDC5Parser_GeneratedAlways_Error_NoOpenParen(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS a*2)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when GENERATED ALWAYS AS has no opening paren")
	}
}

// ---------------------------------------------------------------------------
// parseForeignKeyAction — 80%
//
// Branches: CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION, + error.
// Uncovered: SET DEFAULT and the error path (unknown action).
// ---------------------------------------------------------------------------

func TestMCDC5Parser_FKAction_SetDefault(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE SET DEFAULT)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ON DELETE SET DEFAULT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FKAction_Restrict(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE RESTRICT)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ON DELETE RESTRICT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FKAction_SetNull(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON UPDATE SET NULL)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ON UPDATE SET NULL: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error: ON DELETE followed by an unrecognised action keyword.
func TestMCDC5Parser_FKAction_Error_Unknown(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE NOTHING)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error for unrecognised FK action")
	}
}

// Error: SET followed by neither NULL nor DEFAULT.
func TestMCDC5Parser_FKAction_Error_SetWithoutNullOrDefault(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE SET SOMETHING)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when SET is not followed by NULL or DEFAULT")
	}
}

// Error: NO followed by something other than ACTION.
func TestMCDC5Parser_FKAction_Error_NoWithoutAction(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT REFERENCES p(id) ON DELETE NO OPERATION)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when NO is not followed by ACTION")
	}
}

// ---------------------------------------------------------------------------
// applyTableConstraintUnique — 80%
//
// Uncovered: the error branches (missing '(' and missing ')').
// ---------------------------------------------------------------------------

func TestMCDC5Parser_TableConstraintUnique_Simple(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, b INT, UNIQUE(a, b))`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("TABLE UNIQUE(a,b): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_TableConstraintUnique_Error_NoOpenParen(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, UNIQUE a)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when UNIQUE has no opening paren")
	}
}

func TestMCDC5Parser_TableConstraintUnique_Error_NoClosingParen(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (a INT, UNIQUE(a)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when UNIQUE column list has no closing paren")
	}
}

// ---------------------------------------------------------------------------
// applyTableConstraintCheck — 80%
//
// The column-level CHECK tests (mcdc4) cover LP/RP errors; here we exercise
// the table-level CHECK constraint paths not yet hit.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_TableConstraintCheck_Simple(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (x INT, CHECK(x > 0))`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("TABLE CHECK(x>0): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_TableConstraintCheck_Error_NoOpenParen(t *testing.T) {
	t.Parallel()
	sql := `CREATE TABLE t (x INT, CHECK x > 0)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when table CHECK has no opening paren")
	}
}

func TestMCDC5Parser_TableConstraintCheck_Error_NoClosingParen(t *testing.T) {
	t.Parallel()
	// Expression parses (x), next token 'b' is not ')'.
	sql := `CREATE TABLE t (x INT, CHECK(x b c))`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when table CHECK expression has no closing paren")
	}
}

// ---------------------------------------------------------------------------
// parseRelease — 80%
//
// Branches:
//   A. RELEASE SAVEPOINT <name>   (SAVEPOINT keyword consumed)
//   B. RELEASE <name>             (no SAVEPOINT keyword)
//   Error: no identifier after RELEASE [SAVEPOINT]
// ---------------------------------------------------------------------------

func TestMCDC5Parser_Release_WithSavepoint(t *testing.T) {
	t.Parallel()
	sql := `RELEASE SAVEPOINT sp1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("RELEASE SAVEPOINT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_Release_WithoutSavepoint(t *testing.T) {
	t.Parallel()
	sql := `RELEASE sp1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("RELEASE sp1: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error: RELEASE with no name following.
func TestMCDC5Parser_Release_Error_NoName(t *testing.T) {
	t.Parallel()
	sql := `RELEASE`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when RELEASE has no savepoint name")
	}
}

// ---------------------------------------------------------------------------
// parseParenOrSubquery — 80%
//
// Branches:
//   A. TK_SELECT → SubqueryExpr
//   B. TK_WITH   → SubqueryExpr (CTE subquery)
//   C. plain expression → ParenExpr
//   Error paths: missing ')' after subquery or expression
// ---------------------------------------------------------------------------

func TestMCDC5Parser_ParenSubquery_Select(t *testing.T) {
	t.Parallel()
	sql := `SELECT * FROM t WHERE id IN (SELECT id FROM u)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("paren subquery SELECT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_ParenSubquery_WithCTE(t *testing.T) {
	t.Parallel()
	// CTE subquery in scalar expression context (not inside IN(...)).
	// parseParenOrSubquery is entered after '(' is consumed; the WITH branch
	// fires when TK_WITH is the next token.
	sql := `SELECT (WITH cte AS (SELECT 1) SELECT * FROM cte) AS val`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("paren subquery WITH CTE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_ParenExpr_Simple(t *testing.T) {
	t.Parallel()
	sql := `SELECT (1 + 2) * 3`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("paren expression: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFrameExclude — 80%
//
// Branches: NO OTHERS, CURRENT ROW, GROUP, TIES, + error.
// Uncovered: TIES path and the error branch.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_FrameExclude_CurrentRow(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE CURRENT ROW
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("EXCLUDE CURRENT ROW: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FrameExclude_NoOthers(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (
		ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		EXCLUDE NO OTHERS
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("EXCLUDE NO OTHERS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FrameExclude_Group(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE GROUP
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("EXCLUDE GROUP: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FrameExclude_Ties(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE TIES
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("EXCLUDE TIES: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// Error: EXCLUDE followed by an unrecognised token.
func TestMCDC5Parser_FrameExclude_Error_Unknown(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE SOMETHING
	) FROM t`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error for unknown EXCLUDE clause")
	}
}

// ---------------------------------------------------------------------------
// parseFrameMode — 80%
//
// Branches: ROWS, RANGE, GROUPS, + error.
// Uncovered: GROUPS mode and the error path.
// ---------------------------------------------------------------------------

func TestMCDC5Parser_FrameMode_Rows(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (ROWS UNBOUNDED PRECEDING) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("ROWS frame: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FrameMode_Range(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (RANGE UNBOUNDED PRECEDING) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("RANGE frame: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC5Parser_FrameMode_Groups(t *testing.T) {
	t.Parallel()
	sql := `SELECT x, SUM(x) OVER (GROUPS UNBOUNDED PRECEDING) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("GROUPS frame: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}
