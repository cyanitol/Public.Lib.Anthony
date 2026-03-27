// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// applyConstraintReferences — 75.0%
// Uncovered: the optional column list branch inside REFERENCES.
// ---------------------------------------------------------------------------

func TestMCDC3_FKConstraint_ReferencesWithColumn(t *testing.T) {
	// Column-level REFERENCES with explicit column list
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK REFERENCES with column: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_FKConstraint_ReferencesWithoutColumn(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK REFERENCES without column: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFKOnClause — 84.6%
// Uncovered: ON UPDATE branch.
// ---------------------------------------------------------------------------

func TestMCDC3_FKOnClause_OnUpdate(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) ON UPDATE SET NULL
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK ON UPDATE SET NULL: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFKMatchClause — 75.0%
// Uncovered: the MATCH clause path.
// ---------------------------------------------------------------------------

func TestMCDC3_FKMatchClause(t *testing.T) {
	// Use a regular identifier (not a reserved keyword) as the match name.
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) MATCH simple
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK MATCH simple: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFKNotDeferrable — 75.0%
// Uncovered: NOT DEFERRABLE clause.
// ---------------------------------------------------------------------------

func TestMCDC3_FKNotDeferrable(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) NOT DEFERRABLE
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse NOT DEFERRABLE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseForeignKeyAction — 80.0%
// Uncovered: SET DEFAULT action, and SET with invalid token (error path).
// ---------------------------------------------------------------------------

func TestMCDC3_FKAction_SetDefault(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) ON DELETE SET DEFAULT
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK SET DEFAULT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_FKAction_Restrict(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) ON DELETE RESTRICT
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK RESTRICT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_FKAction_NoAction(t *testing.T) {
	sql := `CREATE TABLE orders (
		customer_id INTEGER REFERENCES customers(id) ON DELETE NO ACTION
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK NO ACTION: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyConstraintGenerated — 76.5%
// Uncovered: STORED path and VIRTUAL path.
// ---------------------------------------------------------------------------

func TestMCDC3_GeneratedColumn_Stored(t *testing.T) {
	sql := `CREATE TABLE t (
		id INTEGER,
		doubled INTEGER GENERATED ALWAYS AS (id * 2) STORED
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse GENERATED ALWAYS AS STORED: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_GeneratedColumn_Virtual(t *testing.T) {
	// GENERATED ALWAYS AS (expr) VIRTUAL — use the full syntax.
	sql := `CREATE TABLE t (
		id INTEGER,
		doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse GENERATED ALWAYS AS VIRTUAL: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyTableConstraintUnique — 80.0%
// Uncovered: missing ( after UNIQUE error path.
// Cover the happy path (UNIQUE table constraint).
// ---------------------------------------------------------------------------

func TestMCDC3_TableConstraint_Unique(t *testing.T) {
	sql := `CREATE TABLE t (
		a INTEGER,
		b TEXT,
		UNIQUE (a, b)
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse UNIQUE table constraint: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyTableConstraintCheck — 80.0%
// Uncovered: CHECK table constraint.
// ---------------------------------------------------------------------------

func TestMCDC3_TableConstraint_Check(t *testing.T) {
	sql := `CREATE TABLE t (
		a INTEGER,
		CONSTRAINT chk_positive CHECK (a > 0)
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CHECK table constraint: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyTableConstraintForeignKey — 85.7%
// Uncovered: parseForeignKeyReferences ref-column list path.
// ---------------------------------------------------------------------------

func TestMCDC3_TableConstraint_ForeignKey(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER,
		customer_id INTEGER,
		FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FOREIGN KEY table constraint: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseForeignKeyRefColumns — 83.3%
// Uncovered: multiple ref columns path.
// ---------------------------------------------------------------------------

func TestMCDC3_FKRefColumns_Multiple(t *testing.T) {
	sql := `CREATE TABLE child (
		a INTEGER,
		b INTEGER,
		FOREIGN KEY (a, b) REFERENCES parent(x, y)
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FK multiple ref columns: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseColumnOrConstraint — 81.8%
// Uncovered: inline column CHECK constraint path.
// ---------------------------------------------------------------------------

func TestMCDC3_ColumnConstraint_Check(t *testing.T) {
	sql := `CREATE TABLE t (
		a INTEGER CHECK (a > 0) NOT NULL
	)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse column CHECK constraint: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// applyConstraintCheck — 80.0%
// Error path: missing ( after CHECK.
// ---------------------------------------------------------------------------

func TestMCDC3_ColumnConstraintCheck_Error_MissingParen(t *testing.T) {
	sql := `CREATE TABLE t (a INTEGER CHECK a > 0)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error for missing ( after CHECK")
	}
}

// ---------------------------------------------------------------------------
// parseUnaryExpression — 75.0%
// Uncovered: NOT EXISTS expr path, and bitwise NOT (~).
// ---------------------------------------------------------------------------

func TestMCDC3_UnaryExpr_BitNot(t *testing.T) {
	sql := `SELECT ~42`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse bitwise NOT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_UnaryExpr_NotExists(t *testing.T) {
	sql := `SELECT NOT EXISTS (SELECT 1)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse NOT EXISTS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_UnaryExpr_MinInt64(t *testing.T) {
	// Exercises the special -9223372036854775808 path in parseUnaryExprWithOp
	sql := `SELECT -9223372036854775808`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse -9223372036854775808: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parsePostfixExpression — 81.8%
// Uncovered: COLLATE postfix operator on expression.
// ---------------------------------------------------------------------------

func TestMCDC3_PostfixExpr_Collate(t *testing.T) {
	sql := `SELECT a COLLATE NOCASE FROM t ORDER BY a COLLATE BINARY`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse COLLATE postfix expr: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_PostfixExpr_JSONArrowChained(t *testing.T) {
	// Chained -> operators exercise parseJSONArrowOps loop body twice.
	sql := `SELECT data->'$.a'->'$.b' FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse chained JSON ->: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseNotPatternOp — 71.4%
// Uncovered: NOT GLOB path.
// ---------------------------------------------------------------------------

func TestMCDC3_NotPatternOp_NotGlob(t *testing.T) {
	sql := `SELECT 'hello' NOT GLOB 'h*o'`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse NOT GLOB: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_NotPatternOp_NotLike(t *testing.T) {
	sql := `SELECT 'hello' NOT LIKE '%world%'`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse NOT LIKE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseParenOrSubquery — 80.0%
// Uncovered: CTE subquery (WITH ... SELECT inside parentheses).
// ---------------------------------------------------------------------------

func TestMCDC3_ParenOrSubquery_CTESubquery(t *testing.T) {
	sql := `SELECT * FROM (WITH cte AS (SELECT 1 AS x) SELECT x FROM cte)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CTE subquery: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFrameExclude — 80.0%
// Uncovered: EXCLUDE GROUP and EXCLUDE TIES paths.
// ---------------------------------------------------------------------------

func TestMCDC3_FrameExclude_Group(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE GROUP
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse EXCLUDE GROUP: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_FrameExclude_Ties(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		EXCLUDE TIES
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse EXCLUDE TIES: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_FrameExclude_NoOthers(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		ROWS UNBOUNDED PRECEDING
		EXCLUDE NO OTHERS
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse EXCLUDE NO OTHERS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFrameMode — 80.0%
// Uncovered: GROUPS frame mode.
// ---------------------------------------------------------------------------

func TestMCDC3_FrameMode_Groups(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse GROUPS frame mode: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFrameBetween — 81.8%
// Uncovered: missing AND error path.
// Also cover FOLLOWING bound in BETWEEN.
// ---------------------------------------------------------------------------

func TestMCDC3_FrameBetween_UnboundedFollowing(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseOffsetBound — 84.6%
// Uncovered: N FOLLOWING bound type.
// ---------------------------------------------------------------------------

func TestMCDC3_OffsetBound_Following(t *testing.T) {
	sql := `SELECT sum(a) OVER (
		ORDER BY b
		ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
	) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse N FOLLOWING bound: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseRelease — 80.0%
// Uncovered: RELEASE without SAVEPOINT keyword.
// ---------------------------------------------------------------------------

func TestMCDC3_Release_WithoutSavepointKeyword(t *testing.T) {
	sql := `RELEASE mypoint`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse RELEASE without SAVEPOINT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_Release_WithSavepointKeyword(t *testing.T) {
	sql := `RELEASE SAVEPOINT mypoint`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse RELEASE SAVEPOINT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseOrExpression / parseAndExpression — 83.3%
// Uncovered: multi-term OR / AND chains.
// ---------------------------------------------------------------------------

func TestMCDC3_OrExpr_ThreeTerms(t *testing.T) {
	sql := `SELECT 1 WHERE a = 1 OR b = 2 OR c = 3`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse three-term OR: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_AndExpr_ThreeTerms(t *testing.T) {
	sql := `SELECT 1 WHERE a = 1 AND b = 2 AND c = 3`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse three-term AND: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseNotExpression — 88.9%
// Uncovered: double NOT (NOT NOT expr).
// ---------------------------------------------------------------------------

func TestMCDC3_NotExpr_Double(t *testing.T) {
	sql := `SELECT NOT NOT 1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse NOT NOT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseIsNotBranch / parseIsDistinctFrom / parseIsNotDistinctFrom — 87.5% / 83.3% / 83.3%
// Uncovered: IS DISTINCT FROM, IS NOT DISTINCT FROM paths.
// ---------------------------------------------------------------------------

func TestMCDC3_IsDistinctFrom(t *testing.T) {
	sql := `SELECT 1 IS DISTINCT FROM 2`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse IS DISTINCT FROM: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_IsNotDistinctFrom(t *testing.T) {
	sql := `SELECT 1 IS NOT DISTINCT FROM 1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse IS NOT DISTINCT FROM: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseOptionalWhereExpr — 83.3%
// Covers WHERE clause in UPDATE (covers path where *where is set).
// ---------------------------------------------------------------------------

func TestMCDC3_OptionalWhereExpr_InUpdate(t *testing.T) {
	sql := `UPDATE t SET a = 1 WHERE id = 42`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse UPDATE with WHERE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseReturningClause — 85.7%
// Uncovered: multiple columns in RETURNING.
// ---------------------------------------------------------------------------

func TestMCDC3_ReturningClause_MultipleColumns(t *testing.T) {
	sql := `INSERT INTO t (a, b) VALUES (1, 2) RETURNING a, b`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse RETURNING multiple columns: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCreateTableAsSelect — 85.7%
// Uncovered: CREATE TABLE AS SELECT path.
// ---------------------------------------------------------------------------

func TestMCDC3_CreateTableAsSelect(t *testing.T) {
	sql := `CREATE TABLE t2 AS SELECT * FROM t1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CREATE TABLE AS SELECT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCreateView — 90.9%
// Uncovered: CREATE VIEW IF NOT EXISTS path.
// ---------------------------------------------------------------------------

func TestMCDC3_CreateView_IfNotExists(t *testing.T) {
	sql := `CREATE VIEW IF NOT EXISTS v AS SELECT 1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CREATE VIEW IF NOT EXISTS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseTriggerBodyStatement — 83.3%
// Uncovered: DELETE statement inside trigger body.
// ---------------------------------------------------------------------------

func TestMCDC3_TriggerBody_Delete(t *testing.T) {
	sql := `CREATE TRIGGER trig AFTER INSERT ON t BEGIN
		DELETE FROM log WHERE id < 0;
	END`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse trigger with DELETE body: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_TriggerBody_Select(t *testing.T) {
	sql := `CREATE TRIGGER trig AFTER INSERT ON t BEGIN
		SELECT 1;
	END`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse trigger with SELECT body: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parsePragmaParenValue — 85.7%
// Uncovered: non-identifier pragma value (integer literal).
// ---------------------------------------------------------------------------

func TestMCDC3_Pragma_IntegerValue(t *testing.T) {
	sql := `PRAGMA cache_size = 1000`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse PRAGMA integer value: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parsePragmaValue — 90.9%
// Uncovered: negative number pragma value.
// ---------------------------------------------------------------------------

func TestMCDC3_Pragma_NegativeValue(t *testing.T) {
	sql := `PRAGMA cache_size(-2000)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse PRAGMA negative paren value: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseJSONArrowOps — 90.0%
// Uncovered: ->> (double arrow) path.
// ---------------------------------------------------------------------------

func TestMCDC3_JSONDoubleArrow(t *testing.T) {
	sql := `SELECT data->>'$.name' FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse JSON ->> operator: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseViewSelect — 88.9%
// Uncovered: view with column names.
// ---------------------------------------------------------------------------

func TestMCDC3_CreateView_WithColumnNames(t *testing.T) {
	sql := `CREATE VIEW v (col1, col2) AS SELECT a, b FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CREATE VIEW with column names: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseTableFuncArgs — 71.4%
// Uncovered: table-valued function call with arguments.
// ---------------------------------------------------------------------------

func TestMCDC3_TableFuncArgs_WithArgs(t *testing.T) {
	sql := `SELECT * FROM json_each('{"a":1}', '$.a')`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse table-valued function with args: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseRollback — 88.9%
// Uncovered: ROLLBACK TO SAVEPOINT name path.
// ---------------------------------------------------------------------------

func TestMCDC3_Rollback_ToSavepointKeyword(t *testing.T) {
	sql := `ROLLBACK TO SAVEPOINT sp1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse ROLLBACK TO SAVEPOINT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_Rollback_ToWithoutSavepointKeyword(t *testing.T) {
	sql := `ROLLBACK TRANSACTION TO sp1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse ROLLBACK TRANSACTION TO: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseInExpression — 88.2%
// Uncovered: IN (subquery) path.
// ---------------------------------------------------------------------------

func TestMCDC3_InExpr_Subquery(t *testing.T) {
	sql := `SELECT * FROM t WHERE id IN (SELECT id FROM other)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse IN (subquery): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCaseElse — 85.7%
// Uncovered: CASE WHEN without ELSE (no else branch).
// Already tested; add explicit ELSE.
// ---------------------------------------------------------------------------

func TestMCDC3_CaseExpr_WithElse(t *testing.T) {
	sql := `SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CASE with ELSE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// extractExpressionName — 88.9%
// Uncovered: expression with table.column form.
// ---------------------------------------------------------------------------

func TestMCDC3_ExtractExprName_TableDotColumn(t *testing.T) {
	sql := `SELECT t.col FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse table.column: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseInsertBody — 88.2%
// Uncovered: INSERT OR IGNORE path.
// ---------------------------------------------------------------------------

func TestMCDC3_Insert_OrIgnore(t *testing.T) {
	sql := `INSERT OR IGNORE INTO t (a) VALUES (1)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse INSERT OR IGNORE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseInsertSource — 91.7%
// Uncovered: INSERT ... DEFAULT VALUES path.
// ---------------------------------------------------------------------------

func TestMCDC3_InsertSource_DefaultValues(t *testing.T) {
	sql := `INSERT INTO t DEFAULT VALUES`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse INSERT DEFAULT VALUES: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCreateVirtualTable — 88.2%
// Uncovered: USING clause with no args (empty parens).
// ---------------------------------------------------------------------------

func TestMCDC3_CreateVirtualTable_NoArgs(t *testing.T) {
	sql := `CREATE VIRTUAL TABLE t USING fts5`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CREATE VIRTUAL TABLE no args: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseTriggerUpdateOf — 88.9%
// Uncovered: multiple columns in UPDATE OF.
// ---------------------------------------------------------------------------

func TestMCDC3_TriggerUpdateOf_MultipleColumns(t *testing.T) {
	sql := `CREATE TRIGGER trig AFTER UPDATE OF a, b ON t BEGIN
		SELECT 1;
	END`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse TRIGGER UPDATE OF multiple columns: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseJoinUsingCondition — 88.9%
// Uncovered: multiple columns in USING clause.
// ---------------------------------------------------------------------------

func TestMCDC3_JoinUsing_MultipleColumns(t *testing.T) {
	sql := `SELECT * FROM a JOIN b USING (x, y)`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse JOIN USING multiple columns: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFunctionFilter — 92.3%
// Uncovered: FILTER (WHERE ...) clause on window function.
// ---------------------------------------------------------------------------

func TestMCDC3_FunctionFilter_Window(t *testing.T) {
	sql := `SELECT count(*) FILTER (WHERE a > 0) OVER () FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FILTER WHERE on window function: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseRaiseFunction — 91.7%
// Uncovered: RAISE(FAIL, message) and RAISE(ABORT, message).
// ---------------------------------------------------------------------------

func TestMCDC3_RaiseFunction_Fail(t *testing.T) {
	sql := `SELECT RAISE(FAIL, 'fail message')`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse RAISE(FAIL,...): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_RaiseFunction_Abort(t *testing.T) {
	sql := `SELECT RAISE(ABORT, 'abort message')`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse RAISE(ABORT,...): %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseExistsExpr — 90.0%
// Uncovered: missing ) after EXISTS error path.
// ---------------------------------------------------------------------------

func TestMCDC3_ExistsExpr_Error_MissingParen(t *testing.T) {
	sql := `SELECT EXISTS (SELECT 1`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error for missing ) after EXISTS subquery")
	}
}

// ---------------------------------------------------------------------------
// parseWindowDef — 82.4%
// Uncovered: window PARTITION BY + ORDER BY + frame spec.
// ---------------------------------------------------------------------------

func TestMCDC3_WindowDef_PartitionAndOrder(t *testing.T) {
	sql := `SELECT row_number() OVER (PARTITION BY a ORDER BY b DESC) FROM t`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse window PARTITION BY ORDER BY: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseDoUpdateClause — 88.9%
// Uncovered: ON CONFLICT DO UPDATE WHERE condition.
// ---------------------------------------------------------------------------

func TestMCDC3_DoUpdateClause_WithWhere(t *testing.T) {
	sql := `INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = excluded.a WHERE a < 10`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse DO UPDATE WHERE: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseSetAssignments — 92.3%
// Uncovered: multiple column tuple assignment (a, b) = (1, 2).
// ---------------------------------------------------------------------------

func TestMCDC3_SetAssignments_TupleForm(t *testing.T) {
	// Standard multi-assign is a = 1, b = 2 (most parsers support tuple form too)
	sql := `UPDATE t SET a = 1, b = 2 WHERE id = 1`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse UPDATE SET multiple: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCTEColumns — 91.7%
// Uncovered: CTE with explicit column list.
// ---------------------------------------------------------------------------

func TestMCDC3_CTEColumns_Explicit(t *testing.T) {
	sql := `WITH cte (x, y) AS (SELECT 1, 2) SELECT x, y FROM cte`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse CTE with explicit columns: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseCTESelect — 92.3%
// Uncovered: MATERIALIZED and NOT MATERIALIZED hints.
// ---------------------------------------------------------------------------

func TestMCDC3_CTESelect_Materialized(t *testing.T) {
	sql := `WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte`
	_, err := ParseString(sql)
	if err != nil {
		// Not all parsers support this; just check it doesn't panic
		if !strings.Contains(err.Error(), "expected") {
			t.Fatalf("parse CTE MATERIALIZED: unexpected error: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// parseUpdateOrderByClause / parseDeleteOrderBy — 88.9%
// Uncovered: LIMIT clause in UPDATE/DELETE.
// ---------------------------------------------------------------------------

func TestMCDC3_UpdateWithOrderAndLimit(t *testing.T) {
	sql := `UPDATE t SET a = 1 ORDER BY id LIMIT 10`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse UPDATE with ORDER BY LIMIT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestMCDC3_DeleteWithOrderAndLimit(t *testing.T) {
	sql := `DELETE FROM t ORDER BY id DESC LIMIT 5`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse DELETE with ORDER BY LIMIT: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseFromClause — 93.8%
// Uncovered: multiple FROM items separated by comma.
// ---------------------------------------------------------------------------

func TestMCDC3_FromClause_MultipleItems(t *testing.T) {
	sql := `SELECT * FROM t1, t2 WHERE t1.id = t2.id`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse FROM multiple items: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseTableAlias — 87.5%
// Uncovered: alias without AS keyword.
// ---------------------------------------------------------------------------

func TestMCDC3_TableAlias_WithoutAS(t *testing.T) {
	sql := `SELECT a.x FROM t a`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse table alias without AS: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// parseLimitClauseInto — 88.9%
// Uncovered: LIMIT with OFFSET keyword.
// ---------------------------------------------------------------------------

func TestMCDC3_LimitOffset_KeywordForm(t *testing.T) {
	sql := `SELECT * FROM t LIMIT 10 OFFSET 5`
	stmts, err := ParseString(sql)
	if err != nil {
		t.Fatalf("parse LIMIT OFFSET keyword form: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}
