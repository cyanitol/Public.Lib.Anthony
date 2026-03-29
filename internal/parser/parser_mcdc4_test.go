// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// ---------------------------------------------------------------------------
// applyConstraintReferences — 75.0%
// Uncovered error paths:
//   - "expected table name after REFERENCES"  (no TK_ID follows REFERENCES)
//   - "expected column name"                  (LP found, but no TK_ID inside)
//   - "expected ')'"                          (column parsed, no RP)
// ---------------------------------------------------------------------------

// TestMCDC4Parser_ReferencesError_NoTableName exercises the error branch
// where no identifier follows REFERENCES.
//
// MC/DC for applyConstraintReferences:
//
//	C1: !p.check(TK_ID) after REFERENCES → error (covered here)
func TestMCDC4Parser_ReferencesError_NoTableName(t *testing.T) {
	// REFERENCES is followed by a keyword (ON), not an identifier.
	sql := `CREATE TABLE t (a INTEGER REFERENCES ON DELETE CASCADE)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when REFERENCES has no table name")
	}
}

// TestMCDC4Parser_ReferencesError_NoColumnName exercises the error branch
// where '(' appears after the referenced table name but no identifier follows.
//
// MC/DC for applyConstraintReferences:
//
//	C2: p.match(TK_LP) && !p.check(TK_ID) → error "expected column name"
func TestMCDC4Parser_ReferencesError_NoColumnName(t *testing.T) {
	// REFERENCES parent( ) — left paren but no column name.
	sql := `CREATE TABLE t (a INTEGER REFERENCES parent())`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when REFERENCES column list has no column name")
	}
}

// TestMCDC4Parser_ReferencesError_NoClosingParen exercises the error branch
// where the column name is found but the closing ')' is missing.
//
// MC/DC for applyConstraintReferences:
//
//	C3: column name parsed, !p.match(TK_RP) → error "expected ')'"
func TestMCDC4Parser_ReferencesError_NoClosingParen(t *testing.T) {
	// REFERENCES parent(id ON DELETE CASCADE — no closing paren.
	sql := `CREATE TABLE t (a INTEGER REFERENCES parent(id ON DELETE CASCADE)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when REFERENCES column list has no closing paren")
	}
}

// ---------------------------------------------------------------------------
// parseFKMatchClause — 75.0%
// Uncovered error path:
//   - "expected match name" when no TK_ID follows MATCH
// ---------------------------------------------------------------------------

// TestMCDC4Parser_FKMatchClause_Error_NoMatchName exercises the error branch
// where MATCH is followed by a non-identifier token.
//
// MC/DC for parseFKMatchClause:
//
//	C1: !p.check(TK_ID) after MATCH → error (covered here)
func TestMCDC4Parser_FKMatchClause_Error_NoMatchName(t *testing.T) {
	// MATCH followed by a keyword that is not a plain identifier.
	sql := `CREATE TABLE t (a INTEGER REFERENCES p(id) MATCH 42)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when MATCH has no identifier following it")
	}
}

// ---------------------------------------------------------------------------
// parseFKNotDeferrable — 75.0%
// Uncovered error path:
//   - "expected DEFERRABLE after NOT" when NOT is not followed by DEFERRABLE
// ---------------------------------------------------------------------------

// TestMCDC4Parser_FKNotDeferrable_Error_NoDeferrable exercises the error
// branch where NOT is not followed by DEFERRABLE.
//
// MC/DC for parseFKNotDeferrable:
//
//	C1: !p.match(TK_DEFERRABLE) after NOT → error (covered here)
func TestMCDC4Parser_FKNotDeferrable_Error_NoDeferrable(t *testing.T) {
	// NOT INITIALLY — missing DEFERRABLE keyword.
	sql := `CREATE TABLE t (a INTEGER REFERENCES p(id) NOT INITIALLY DEFERRED)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when NOT is not followed by DEFERRABLE")
	}
}

// ---------------------------------------------------------------------------
// applyConstraintCheck — 80.0%
// Uncovered error paths:
//   - "expected ( after CHECK"        (no LP after CHECK)
//   - "expected ) after CHECK expression" (expression parsed, no RP)
// ---------------------------------------------------------------------------

// TestMCDC4Parser_ConstraintCheck_Error_NoOpenParen exercises the error branch
// where CHECK is not followed by '('.
//
// MC/DC for applyConstraintCheck:
//
//	C1: !p.match(TK_LP) after CHECK → error (covered here)
func TestMCDC4Parser_ConstraintCheck_Error_NoOpenParen(t *testing.T) {
	// CHECK without parentheses.
	sql := `CREATE TABLE t (a INTEGER CHECK a > 0)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when CHECK has no opening paren")
	}
}

// TestMCDC4Parser_ConstraintCheck_Error_NoClosingParen exercises the error
// branch where the CHECK expression is parsed but no ')' follows — a stray
// identifier after the expression prevents the closing paren from matching.
//
// MC/DC for applyConstraintCheck:
//
//	C2: expression parsed, !p.match(TK_RP) → error "expected ) after CHECK expression"
func TestMCDC4Parser_ConstraintCheck_Error_NoClosingParen(t *testing.T) {
	// After parsing expression "a", the next token is "b" (TK_ID), not ")".
	// p.match(TK_RP) fails → "expected ) after CHECK expression".
	sql := `CREATE TABLE t (a INTEGER CHECK(a b c))`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when CHECK expression is not followed by closing paren")
	}
}

// ---------------------------------------------------------------------------
// parseColumnOrConstraint — 81.8%
// Uncovered path:
//   - return colErr  (both column-parse and table-constraint-parse fail)
// ---------------------------------------------------------------------------

// TestMCDC4Parser_ColumnOrConstraint_Error_BothFail exercises the fallback
// path in parseColumnOrConstraint where neither a column definition nor a
// table constraint can be parsed from the current token.
//
// MC/DC for parseColumnOrConstraint:
//
//	C1: isTableConstraintKeyword() == false AND parseColumnDef() fails
//	    AND parseTableConstraint() fails → return colErr (covered here)
func TestMCDC4Parser_ColumnOrConstraint_Error_BothFail(t *testing.T) {
	// A number literal is not a valid column name, nor a constraint keyword.
	sql := `CREATE TABLE t (42 INTEGER)`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when column definition starts with a number")
	}
}

// ---------------------------------------------------------------------------
// parseOptionalWhereExpr — 83.3%
// Uncovered path:
//   - error return from parseExpression when WHERE has no valid expression
// ---------------------------------------------------------------------------

// TestMCDC4Parser_OptionalWhereExpr_Error_MissingExpr exercises the error
// path where WHERE is present but has no expression following it.
//
// MC/DC for parseOptionalWhereExpr:
//
//	C1: p.match(TK_WHERE) == true AND parseExpression() fails → error (covered here)
func TestMCDC4Parser_OptionalWhereExpr_Error_MissingExpr(t *testing.T) {
	// WHERE without any expression — statement ends immediately.
	sql := `UPDATE t SET x = 1 WHERE`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when WHERE clause has no expression")
	}
}

// TestMCDC4Parser_OptionalWhereExpr_Error_DeleteMissingExpr also covers the
// parseOptionalWhereExpr error path via a DELETE statement.
//
// MC/DC for parseOptionalWhereExpr (DELETE context):
//
//	C1: p.match(TK_WHERE) == true AND parseExpression() fails → error (covered here)
func TestMCDC4Parser_OptionalWhereExpr_Error_DeleteMissingExpr(t *testing.T) {
	sql := `DELETE FROM t WHERE`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when DELETE WHERE clause has no expression")
	}
}

// ---------------------------------------------------------------------------
// parseReturningClause — 85.7%
// Uncovered path:
//   - error return from parseResultColumns when the column list is malformed
// ---------------------------------------------------------------------------

// TestMCDC4Parser_ReturningClause_Error_MissingColumns exercises the error
// path where RETURNING is present but no valid result columns follow.
//
// MC/DC for parseReturningClause:
//
//	C1: p.match(TK_RETURNING) == true AND parseResultColumns() fails → error
func TestMCDC4Parser_ReturningClause_Error_MissingColumns(t *testing.T) {
	// RETURNING followed by a comma (no first column).
	sql := `INSERT INTO t VALUES(1) RETURNING ,id`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when RETURNING clause has no valid result columns")
	}
}

// TestMCDC4Parser_ReturningClause_Error_TrailingComma exercises the error
// path where RETURNING has a trailing comma with no column following.
//
// MC/DC for parseReturningClause (trailing comma):
//
//	C1: parseResultColumns() encounters comma with no following column → error
func TestMCDC4Parser_ReturningClause_Error_TrailingComma(t *testing.T) {
	// RETURNING id, — trailing comma with nothing after.
	sql := `UPDATE t SET x = 1 RETURNING id,`
	_, err := ParseString(sql)
	if err == nil {
		t.Error("expected error when RETURNING clause has a trailing comma")
	}
}
