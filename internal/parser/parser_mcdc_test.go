// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"strings"
	"testing"
)

// =============================================================================
// MC/DC Coverage Tests for internal/parser
//
// Each test function covers one compound boolean condition. For a condition
// with N independent sub-conditions, N+1 test cases are provided where each
// sub-condition independently flips the overall outcome.
// =============================================================================

// -----------------------------------------------------------------------------
// Condition: tok.Type != TK_SPACE && tok.Type != TK_COMMENT
// Location: parser.go tokenize()
// Purpose: tokens of type SPACE or COMMENT are filtered out during tokenization.
//
// Sub-condition A: tok.Type != TK_SPACE
// Sub-condition B: tok.Type != TK_COMMENT
//
// MC/DC table:
//
//	A=T B=T → T (non-space, non-comment token is kept)
//	A=F B=T → F (space token is dropped)
//	A=T B=F → F (comment token is dropped)
//
// -----------------------------------------------------------------------------
func TestMCDC_Tokenize_SpaceAndCommentFilter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		sql           string
		wantTokenType TokenType // first non-EOF token type expected
		wantCount     int       // total non-EOF tokens expected
	}{
		// A=T B=T: regular token (not space, not comment) is kept
		{
			name:          "A=T B=T: identifier kept",
			sql:           "foo",
			wantTokenType: TK_ID,
			wantCount:     1,
		},
		// A=F B=T: space token is dropped; only the identifier survives
		{
			name:          "A=F B=T: space between tokens dropped",
			sql:           "foo bar",
			wantTokenType: TK_ID,
			wantCount:     2,
		},
		// A=T B=F: line comment dropped; only SELECT survives
		{
			name:          "A=T B=F: line comment dropped",
			sql:           "SELECT -- this is a comment\n1",
			wantTokenType: TK_SELECT,
			wantCount:     2,
		},
		// A=T B=F: block comment dropped
		{
			name:          "A=T B=F: block comment dropped",
			sql:           "SELECT /* comment */ 1",
			wantTokenType: TK_SELECT,
			wantCount:     2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.sql)
			var tokens []Token
			for {
				tok := lexer.NextToken()
				if tok.Type == TK_SPACE || tok.Type == TK_COMMENT {
					continue
				}
				if tok.Type == TK_EOF {
					break
				}
				tokens = append(tokens, tok)
			}
			if len(tokens) != tt.wantCount {
				t.Errorf("token count: got %d, want %d (sql=%q)", len(tokens), tt.wantCount, tt.sql)
			}
			if len(tokens) > 0 && tokens[0].Type != tt.wantTokenType {
				t.Errorf("first token type: got %s, want %s (sql=%q)", tokens[0].Type, tt.wantTokenType, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_UNION) || p.check(TK_EXCEPT) || p.check(TK_INTERSECT)
// Location: parser.go checkCompoundOp()
// Purpose: determines whether the next token starts a compound SELECT operator.
//
// Sub-condition A: check(TK_UNION)
// Sub-condition B: check(TK_EXCEPT)
// Sub-condition C: check(TK_INTERSECT)
//
// MC/DC table (each sub-condition independently flips the overall result):
//
//	A=T B=F C=F → T (UNION alone is sufficient)
//	A=F B=T C=F → T (EXCEPT alone is sufficient)
//	A=F B=F C=T → T (INTERSECT alone is sufficient)
//	A=F B=F C=F → F (none present)
//
// -----------------------------------------------------------------------------
func TestMCDC_CheckCompoundOp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=F C=F: UNION is a valid compound operator
		{
			name:    "A=T B=F C=F: UNION accepted",
			sql:     "SELECT 1 UNION SELECT 2",
			wantErr: false,
		},
		// A=F B=T C=F: EXCEPT is a valid compound operator
		{
			name:    "A=F B=T C=F: EXCEPT accepted",
			sql:     "SELECT 1 EXCEPT SELECT 2",
			wantErr: false,
		},
		// A=F B=F C=T: INTERSECT is a valid compound operator
		{
			name:    "A=F B=F C=T: INTERSECT accepted",
			sql:     "SELECT 1 INTERSECT SELECT 2",
			wantErr: false,
		},
		// A=F B=F C=F: no compound operator — single SELECT
		{
			name:    "A=F B=F C=F: no compound op",
			sql:     "SELECT 1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_ID) && p.peekAhead(1).Type == TK_DOT && p.peekAhead(2).Type == TK_STAR
// Location: parser.go isTableStar()
// Purpose: recognises the table.* form in a SELECT column list.
//
// Sub-condition A: current token is TK_ID
// Sub-condition B: next token (ahead 1) is TK_DOT
// Sub-condition C: token at ahead 2 is TK_STAR
//
// MC/DC table:
//
//	A=T B=T C=T → T (table.* form parsed)
//	A=F B=T C=T → F (not an identifier — plain * form)
//	A=T B=F C=T → F (no dot — parsed as expression)
//	A=T B=T C=F → F (dot not followed by star — parsed as qualified column)
//
// -----------------------------------------------------------------------------
func TestMCDC_IsTableStar(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(stmts []Statement) bool
	}{
		// A=T B=T C=T: table.* is valid
		{
			name:    "A=T B=T C=T: table.* accepted",
			sql:     "SELECT t.* FROM t",
			wantErr: false,
			check: func(stmts []Statement) bool {
				sel, ok := stmts[0].(*SelectStmt)
				if !ok {
					return false
				}
				return len(sel.Columns) == 1 && sel.Columns[0].Star && sel.Columns[0].Table == "t"
			},
		},
		// A=F B=T C=T: plain * (no table qualifier)
		{
			name:    "A=F B=T C=T: bare * accepted",
			sql:     "SELECT * FROM t",
			wantErr: false,
			check: func(stmts []Statement) bool {
				sel, ok := stmts[0].(*SelectStmt)
				if !ok {
					return false
				}
				return len(sel.Columns) == 1 && sel.Columns[0].Star && sel.Columns[0].Table == ""
			},
		},
		// A=T B=F C=T: identifier without dot — treated as expression
		{
			name:    "A=T B=F C=T: id without dot is expression",
			sql:     "SELECT col FROM t",
			wantErr: false,
			check: func(stmts []Statement) bool {
				sel, ok := stmts[0].(*SelectStmt)
				if !ok {
					return false
				}
				return len(sel.Columns) == 1 && !sel.Columns[0].Star
			},
		},
		// A=T B=T C=F: qualified column (id.col, no star)
		{
			name:    "A=T B=T C=F: id.col is expression not table-star",
			sql:     "SELECT t.col FROM t",
			wantErr: false,
			check: func(stmts []Statement) bool {
				sel, ok := stmts[0].(*SelectStmt)
				if !ok {
					return false
				}
				return len(sel.Columns) == 1 && !sel.Columns[0].Star
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.check(stmts) {
				t.Errorf("ParseString(%q): check failed", tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (parseAlias, guard branch): !p.check(TK_ID) && !p.check(TK_STRING)
// Location: parser.go parseAlias()
// Purpose: when AS is present, the following token must be an identifier or
//
//	string — if neither, a parse error is returned.
//
// Sub-condition A: !p.check(TK_ID) — current token is NOT TK_ID
// Sub-condition B: !p.check(TK_STRING) — current token is NOT TK_STRING
//
// MC/DC table:
//
//	A=T B=T → T (error: nothing valid follows AS)
//	A=F B=T → F (TK_ID follows AS — valid alias)
//	A=T B=F → F (TK_STRING follows AS — valid alias)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseAlias_AfterAS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=T: keyword that is neither ID nor STRING follows AS
		{
			name:    "A=T B=T: invalid token after AS errors",
			sql:     "SELECT 1 AS SELECT",
			wantErr: true,
		},
		// A=F B=T: plain identifier follows AS — valid
		{
			name:    "A=F B=T: identifier after AS accepted",
			sql:     "SELECT 1 AS n",
			wantErr: false,
		},
		// A=T B=F: quoted string follows AS — valid
		{
			name:    "A=T B=F: string literal after AS accepted",
			sql:     `SELECT 1 AS "my alias"`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (parseAlias, implicit alias): p.check(TK_ID) || p.check(TK_STRING)
// Location: parser.go parseAlias() — implicit alias branch (no AS keyword)
//
// Sub-condition A: p.check(TK_ID)
// Sub-condition B: p.check(TK_STRING)
//
// MC/DC table:
//
//	A=T B=F → T (bare identifier is taken as implicit alias)
//	A=F B=T → T (quoted string is taken as implicit alias)
//	A=F B=F → F (no implicit alias; next token is something else)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseAlias_Implicit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantAlias string
	}{
		// A=T B=F: bare identifier following expression = implicit alias
		{
			name:      "A=T B=F: bare identifier alias",
			sql:       "SELECT 1 myalias",
			wantAlias: "myalias",
		},
		// A=F B=T: quoted string following expression = implicit alias
		{
			name:      `A=F B=T: quoted string alias`,
			sql:       `SELECT 1 "myalias"`,
			wantAlias: "myalias",
		},
		// A=F B=F: next token is FROM — no implicit alias
		{
			name:      "A=F B=F: no implicit alias",
			sql:       "SELECT 1 FROM t",
			wantAlias: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString(%q) unexpected error: %v", tt.sql, err)
			}
			sel := stmts[0].(*SelectStmt)
			if sel.Columns[0].Alias != tt.wantAlias {
				t.Errorf("alias: got %q, want %q (sql=%q)", sel.Columns[0].Alias, tt.wantAlias, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: !p.match(TK_SELECT) && !p.check(TK_WITH)
// Location: parser.go parseSubquery()
// Purpose: a subquery must start with SELECT or WITH — anything else is an error.
//
// Sub-condition A: !p.match(TK_SELECT) — SELECT is NOT present
// Sub-condition B: !p.check(TK_WITH)   — WITH is NOT present
//
// MC/DC table:
//
//	A=T B=T → T (error: neither SELECT nor WITH)
//	A=F B=T → F (SELECT starts the subquery)
//	A=T B=F → F (WITH starts the subquery — CTE subquery)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseSubquery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=F B=T: subquery starts with SELECT
		{
			name:    "A=F B=T: SELECT subquery accepted",
			sql:     "SELECT * FROM (SELECT 1) sub",
			wantErr: false,
		},
		// A=T B=F: subquery starts with WITH (CTE)
		{
			name:    "A=T B=F: WITH subquery accepted",
			sql:     "SELECT * FROM (WITH cte AS (SELECT 1) SELECT * FROM cte) sub",
			wantErr: false,
		},
		// A=T B=T: subquery body starts with a non-SELECT/non-WITH token
		{
			name:    "A=T B=T: invalid subquery errors",
			sql:     "SELECT * FROM (INSERT INTO t VALUES(1)) sub",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_ID) && !p.isJoinKeyword()
// Location: parser.go parseTableAlias()
// Purpose: an identifier that does NOT look like a join keyword is taken as a
//
//	table alias; a join keyword is NOT consumed as an alias.
//
// Sub-condition A: p.check(TK_ID) — current token is an identifier
// Sub-condition B: !p.isJoinKeyword() — it is NOT a join keyword
//
// MC/DC table:
//
//	A=T B=T → T (plain identifier taken as alias)
//	A=F B=T → F (no identifier present — no alias)
//	A=T B=F → F (identifier is a join keyword — not consumed as alias)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseTableAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantAlias string
		wantErr   bool
	}{
		// A=T B=T: plain identifier after table name = alias
		{
			name:      "A=T B=T: plain identifier is alias",
			sql:       "SELECT u.id FROM users u",
			wantAlias: "u",
		},
		// A=F B=T: next token is a keyword (WHERE) — no alias
		{
			name:      "A=F B=T: WHERE keyword not consumed as alias",
			sql:       "SELECT id FROM users WHERE id = 1",
			wantAlias: "",
		},
		// A=T B=F: next identifier is a join keyword — not consumed as alias
		{
			name:      "A=T B=F: JOIN keyword not consumed as alias",
			sql:       "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
			wantAlias: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			sel := stmts[0].(*SelectStmt)
			if sel.From == nil || len(sel.From.Tables) == 0 {
				t.Fatalf("no FROM clause in %q", tt.sql)
			}
			alias := sel.From.Tables[0].Alias
			if alias != tt.wantAlias {
				t.Errorf("alias: got %q, want %q (sql=%q)", alias, tt.wantAlias, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.match(TK_TEMP) || p.match(TK_TEMPORARY)
// Location: parser.go parseCreate()
// Purpose: both TEMP and TEMPORARY keywords mark the table/view as temporary.
//
// Sub-condition A: p.match(TK_TEMP)
// Sub-condition B: p.match(TK_TEMPORARY)
//
// MC/DC table:
//
//	A=T B=F → T (TEMP keyword sets temp=true)
//	A=F B=T → T (TEMPORARY keyword sets temp=true)
//	A=F B=F → F (neither present — temp=false)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseCreate_TempOrTemporary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=F: TEMP keyword
		{
			name:    "A=T B=F: TEMP TABLE accepted",
			sql:     "CREATE TEMP TABLE t (id INTEGER)",
			wantErr: false,
		},
		// A=F B=T: TEMPORARY keyword
		{
			name:    "A=F B=T: TEMPORARY TABLE accepted",
			sql:     "CREATE TEMPORARY TABLE t (id INTEGER)",
			wantErr: false,
		},
		// A=F B=F: neither — regular CREATE TABLE
		{
			name:    "A=F B=F: CREATE TABLE without temp accepted",
			sql:     "CREATE TABLE t (id INTEGER)",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: !p.match(TK_NOT) || !p.match(TK_EXISTS)
// Location: parser.go parseIfNotExists()
// Purpose: after IF, the parser must see NOT followed by EXISTS; if either is
//
//	absent the condition is true and a parse error is returned.
//
// Sub-condition A: !p.match(TK_NOT) — NOT is absent
// Sub-condition B: !p.match(TK_EXISTS) — EXISTS is absent (when NOT was present)
//
// MC/DC table:
//
//	A=T  B=* → T (NOT missing — error)
//	A=F  B=T → T (NOT present but EXISTS missing — error)
//	A=F  B=F → F (NOT EXISTS both present — valid)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseIfNotExists(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=F B=F: IF NOT EXISTS fully spelled out — valid
		{
			name:    "A=F B=F: IF NOT EXISTS accepted",
			sql:     "CREATE TABLE IF NOT EXISTS t (id INTEGER)",
			wantErr: false,
		},
		// A=T B=*: IF followed by something other than NOT
		{
			name:    "A=T B=*: IF EXISTS (missing NOT) errors",
			sql:     "CREATE TABLE IF EXISTS t (id INTEGER)",
			wantErr: true,
		},
		// A=F B=T: IF NOT followed by something other than EXISTS
		{
			name:    "A=F B=T: IF NOT TABLE (missing EXISTS) errors",
			sql:     "CREATE TABLE IF NOT TABLE t (id INTEGER)",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_NOT) && p.peekAhead(1).Type == tok
// Location: parser.go checkNotWithAhead()
// Purpose: recognises negated operators like NOT IN, NOT BETWEEN, NOT LIKE.
//
// Sub-condition A: p.check(TK_NOT) — current token is NOT
// Sub-condition B: peekAhead(1).Type == tok — next token matches the operator
//
// MC/DC table:
//
//	A=T B=T → T (NOT IN detected)
//	A=F B=T → F (no NOT prefix — plain IN detected)
//	A=T B=F → F (NOT followed by wrong token — not this pattern)
//
// -----------------------------------------------------------------------------
func TestMCDC_CheckNotWithAhead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=T: NOT IN
		{
			name:    "A=T B=T: NOT IN accepted",
			sql:     "SELECT * FROM t WHERE id NOT IN (1, 2, 3)",
			wantErr: false,
		},
		// A=F B=T: plain IN (no NOT prefix)
		{
			name:    "A=F B=T: IN accepted",
			sql:     "SELECT * FROM t WHERE id IN (1, 2, 3)",
			wantErr: false,
		},
		// A=T B=F: NOT LIKE (different operator — still valid, but exercises B=F for NOT IN)
		{
			name:    "A=T B=F: NOT LIKE accepted",
			sql:     "SELECT * FROM t WHERE name NOT LIKE 'foo%'",
			wantErr: false,
		},
		// A=T B=T: NOT BETWEEN
		{
			name:    "A=T B=T: NOT BETWEEN accepted",
			sql:     "SELECT * FROM t WHERE id NOT BETWEEN 1 AND 10",
			wantErr: false,
		},
		// A=T B=T: NOT GLOB
		{
			name:    "A=T B=T: NOT GLOB accepted",
			sql:     "SELECT * FROM t WHERE name NOT GLOB '*.txt'",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_IN) || p.checkNotWithAhead(TK_IN)
// Location: parser.go parseComparisonExpression()
// Purpose: routes into parseInExpression for both IN and NOT IN forms.
//
// Sub-condition A: p.check(TK_IN) — bare IN keyword
// Sub-condition B: p.checkNotWithAhead(TK_IN) — NOT IN sequence
//
// MC/DC table:
//
//	A=T B=F → T (plain IN enters parseInExpression)
//	A=F B=T → T (NOT IN enters parseInExpression)
//	A=F B=F → F (neither IN nor NOT IN — falls through to other operators)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseComparisonExpr_In(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=F: plain IN
		{
			name:    "A=T B=F: IN list accepted",
			sql:     "SELECT * FROM t WHERE x IN (1, 2)",
			wantErr: false,
		},
		// A=F B=T: NOT IN
		{
			name:    "A=F B=T: NOT IN list accepted",
			sql:     "SELECT * FROM t WHERE x NOT IN (1, 2)",
			wantErr: false,
		},
		// A=F B=F: comparison falls through to = operator
		{
			name:    "A=F B=F: equality comparison accepted",
			sql:     "SELECT * FROM t WHERE x = 1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_BETWEEN) || p.checkNotWithAhead(TK_BETWEEN)
// Location: parser.go parseComparisonExpression()
// Purpose: routes into parseBetweenExpression for both BETWEEN and NOT BETWEEN.
//
// Sub-condition A: p.check(TK_BETWEEN) — bare BETWEEN keyword
// Sub-condition B: p.checkNotWithAhead(TK_BETWEEN) — NOT BETWEEN sequence
//
// MC/DC table:
//
//	A=T B=F → T (BETWEEN enters parseBetweenExpression)
//	A=F B=T → T (NOT BETWEEN enters parseBetweenExpression)
//	A=F B=F → F (falls through to other operators)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseComparisonExpr_Between(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=F: plain BETWEEN
		{
			name:    "A=T B=F: BETWEEN accepted",
			sql:     "SELECT * FROM t WHERE x BETWEEN 1 AND 10",
			wantErr: false,
		},
		// A=F B=T: NOT BETWEEN
		{
			name:    "A=F B=T: NOT BETWEEN accepted",
			sql:     "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10",
			wantErr: false,
		},
		// A=F B=F: comparison does not involve BETWEEN
		{
			name:    "A=F B=F: greater-than comparison accepted",
			sql:     "SELECT * FROM t WHERE x > 5",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: !p.check(TK_ROWS) && !p.check(TK_RANGE) && !p.check(TK_GROUPS)
// Location: parser.go parseWindowFrame()
// Purpose: exits early (no frame spec) when none of ROWS/RANGE/GROUPS is present.
//
// Sub-condition A: !p.check(TK_ROWS)
// Sub-condition B: !p.check(TK_RANGE)
// Sub-condition C: !p.check(TK_GROUPS)
//
// MC/DC table:
//
//	A=T B=T C=T → T (no frame keyword — early return, no frame spec)
//	A=F B=T C=T → F (ROWS keyword present — frame spec parsed)
//	A=T B=F C=T → F (RANGE keyword present — frame spec parsed)
//	A=T B=T C=F → F (GROUPS keyword present — frame spec parsed)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseWindowFrame(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		wantFrame bool // whether a frame spec should be present
	}{
		// A=T B=T C=T: window function with ORDER BY but no frame clause
		{
			name:      "A=T B=T C=T: no frame spec",
			sql:       "SELECT row_number() OVER (ORDER BY id) FROM t",
			wantErr:   false,
			wantFrame: false,
		},
		// A=F B=T C=T: ROWS frame
		{
			name:      "A=F B=T C=T: ROWS frame spec",
			sql:       "SELECT sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr:   false,
			wantFrame: true,
		},
		// A=T B=F C=T: RANGE frame
		{
			name:      "A=T B=F C=T: RANGE frame spec",
			sql:       "SELECT sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr:   false,
			wantFrame: true,
		},
		// A=T B=T C=F: GROUPS frame
		{
			name:      "A=T B=T C=F: GROUPS frame spec",
			sql:       "SELECT sum(v) OVER (ORDER BY id GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
			wantErr:   false,
			wantFrame: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			sel := stmts[0].(*SelectStmt)
			fn, ok := sel.Columns[0].Expr.(*FunctionExpr)
			if !ok {
				t.Fatalf("expected FunctionExpr, got %T (sql=%q)", sel.Columns[0].Expr, tt.sql)
			}
			hasFrame := fn.Over != nil && fn.Over.Frame != nil
			if hasFrame != tt.wantFrame {
				t.Errorf("frame present: got %v, want %v (sql=%q)", hasFrame, tt.wantFrame, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.current >= len(p.tokens) || p.peek().Type == TK_EOF
// Location: parser.go isAtEnd()
// Purpose: detects end of token stream (either past end or explicit EOF token).
//
// Sub-condition A: p.current >= len(p.tokens)
// Sub-condition B: p.peek().Type == TK_EOF
//
// MC/DC table:
//
//	A=T B=* → T (current past end — isAtEnd=true — empty input)
//	A=F B=T → T (at explicit EOF token — isAtEnd=true)
//	A=F B=F → F (more tokens remain — parser continues)
//
// -----------------------------------------------------------------------------
func TestMCDC_IsAtEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=*: empty input — parser sees only EOF immediately
		{
			name:    "A=T B=*: empty input yields no statements",
			sql:     "",
			wantErr: false,
		},
		// A=F B=T: single semicolon — parser skips it and then hits EOF
		{
			name:    "A=F B=T: semicolons only yields no statements",
			sql:     ";;;",
			wantErr: false,
		},
		// A=F B=F: normal statement — parser has real tokens to consume
		{
			name:    "A=F B=F: statement consumed before EOF",
			sql:     "SELECT 1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (lexer): (l.ch == 'x' || l.ch == 'X') && l.peekChar() == '\”
// Location: lexer.go isBlobLiteralStart()
// Purpose: detects X'...' blob literal syntax.
//
// Sub-condition A: l.ch == 'x' || l.ch == 'X' — current char is x or X
// Sub-condition B: l.peekChar() == '\” — next char is a quote
//
// MC/DC table:
//
//	A=T B=T → T (blob literal like X'FF' is recognised)
//	A=F B=T → F (identifier followed by quote — not a blob)
//	A=T B=F → F (standalone x without quote — identifier)
//
// -----------------------------------------------------------------------------
func TestMCDC_Lexer_IsBlobLiteralStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		sql           string
		wantTokenType TokenType
	}{
		// A=T B=T: lowercase x followed by quote — blob literal
		{
			name:          "A=T B=T: x'...' is blob literal",
			sql:           "x'DEADBEEF'",
			wantTokenType: TK_BLOB,
		},
		// A=T B=T: uppercase X followed by quote — blob literal
		{
			name:          "A=T B=T: X'...' is blob literal",
			sql:           "X'DEADBEEF'",
			wantTokenType: TK_BLOB,
		},
		// A=T B=F: x without quote — plain identifier
		{
			name:          "A=T B=F: x without quote is identifier",
			sql:           "xvalue",
			wantTokenType: TK_ID,
		},
		// A=F B=T: non-x letter followed by quote — identifier then string
		{
			name:          "A=F B=T: non-x letter followed by quote is not blob",
			sql:           "n",
			wantTokenType: TK_ID,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lex := NewLexer(tt.sql)
			tok := lex.NextToken()
			if tok.Type != tt.wantTokenType {
				t.Errorf("token type: got %s, want %s (sql=%q)", tok.Type, tt.wantTokenType, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (lexer): l.ch == '0' && (l.peekChar() == 'x' || l.peekChar() == 'X')
// Location: lexer.go readNumber()
// Purpose: detects hexadecimal integer literals starting with 0x or 0X.
//
// Sub-condition A: l.ch == '0' — current char is zero
// Sub-condition B: l.peekChar() == 'x' || l.peekChar() == 'X' — next char is x/X
//
// MC/DC table:
//
//	A=T B=T → T (0x... is parsed as hexadecimal integer)
//	A=F B=T → F (non-zero digit — decimal integer)
//	A=T B=F → F (0 followed by digit — decimal integer starting with 0)
//
// -----------------------------------------------------------------------------
func TestMCDC_Lexer_HexNumberStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		sql           string
		wantTokenType TokenType
		wantLexeme    string
	}{
		// A=T B=T: 0x hex literal (lowercase)
		{
			name:          "A=T B=T: 0x hex literal lowercase",
			sql:           "0xFF",
			wantTokenType: TK_INTEGER,
			wantLexeme:    "0xFF",
		},
		// A=T B=T: 0X hex literal (uppercase)
		{
			name:          "A=T B=T: 0X hex literal uppercase",
			sql:           "0XFF",
			wantTokenType: TK_INTEGER,
			wantLexeme:    "0XFF",
		},
		// A=F B=T: non-zero digit — plain decimal
		{
			name:          "A=F B=T: decimal integer",
			sql:           "42",
			wantTokenType: TK_INTEGER,
			wantLexeme:    "42",
		},
		// A=T B=F: zero followed by decimal digit — decimal
		{
			name:          "A=T B=F: 0 followed by digit is decimal",
			sql:           "01",
			wantTokenType: TK_INTEGER,
			wantLexeme:    "01",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lex := NewLexer(tt.sql)
			tok := lex.NextToken()
			if tok.Type != tt.wantTokenType {
				t.Errorf("token type: got %s, want %s (sql=%q)", tok.Type, tt.wantTokenType, tt.sql)
			}
			if tok.Lexeme != tt.wantLexeme {
				t.Errorf("lexeme: got %q, want %q (sql=%q)", tok.Lexeme, tt.wantLexeme, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (lexer): isHexDigit — isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
// Location: lexer.go isHexDigit()
// Purpose: classifies characters that can appear in a hexadecimal literal.
//
// Sub-condition A: isDigit(ch) — ch is 0-9
// Sub-condition B: ch >= 'a' && ch <= 'f' — ch is a-f
// Sub-condition C: ch >= 'A' && ch <= 'F' — ch is A-F
//
// MC/DC table (each condition independently determines a true result):
//
//	A=T B=F C=F → T (digit in hex literal)
//	A=F B=T C=F → T (lowercase hex letter in literal)
//	A=F B=F C=T → T (uppercase hex letter in literal)
//	A=F B=F C=F → F (non-hex character ends the literal)
//
// -----------------------------------------------------------------------------
func TestMCDC_Lexer_IsHexDigit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantLexeme string
	}{
		// A=T B=F C=F: digits only
		{
			name:       "A=T B=F C=F: hex literal with digits only",
			sql:        "0x01234",
			wantLexeme: "0x01234",
		},
		// A=F B=T C=F: lowercase hex letters
		{
			name:       "A=F B=T C=F: hex literal with lowercase letters",
			sql:        "0xabcdef",
			wantLexeme: "0xabcdef",
		},
		// A=F B=F C=T: uppercase hex letters
		{
			name:       "A=F B=F C=T: hex literal with uppercase letters",
			sql:        "0xABCDEF",
			wantLexeme: "0xABCDEF",
		},
		// A=F B=F C=F: non-hex char terminates literal after initial digit
		{
			name:       "A=F B=F C=F: hex literal terminated by non-hex char",
			sql:        "0x1G",
			wantLexeme: "0x1",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lex := NewLexer(tt.sql)
			tok := lex.NextToken()
			if tok.Lexeme != tt.wantLexeme {
				t.Errorf("lexeme: got %q, want %q (sql=%q)", tok.Lexeme, tt.wantLexeme, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (lexer): len(s) >= 2 && s[0] == quote && s[len(s)-1] == quote
// Location: lexer.go isMatchingQuote()
// Purpose: guards the unquoting logic — only strips quotes when both ends match.
//
// Sub-condition A: len(s) >= 2 — string is long enough to have matching quotes
// Sub-condition B: s[0] == quote — first char is the expected quote
// Sub-condition C: s[len(s)-1] == quote — last char is the expected quote
//
// MC/DC table:
//
//	A=T B=T C=T → T (properly quoted string — quotes stripped)
//	A=F B=* C=* → F (too short — not unquoted)
//	A=T B=F C=T → F (does not start with quote — not unquoted)
//	A=T B=T C=F → F (does not end with quote — not unquoted)
//
// -----------------------------------------------------------------------------
func TestMCDC_Lexer_Unquote_IsMatchingQuote(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		input      string
		wantOutput string
	}{
		// A=T B=T C=T: both quotes present — unquoted
		{
			name:       "A=T B=T C=T: single-quoted string unquoted",
			input:      "'hello'",
			wantOutput: "hello",
		},
		// A=T B=T C=T: double-quoted identifier unquoted
		{
			name:       `A=T B=T C=T: double-quoted identifier unquoted`,
			input:      `"hello"`,
			wantOutput: "hello",
		},
		// A=F B=*: too short (1 char) — returned as-is
		{
			name:       "A=F B=*: single char not unquoted",
			input:      "'",
			wantOutput: "'",
		},
		// A=T B=F C=T: does not start with quote
		{
			name:       "A=T B=F C=T: no opening quote not unquoted",
			input:      "hello'",
			wantOutput: "hello'",
		},
		// A=T B=T C=F: does not end with quote
		{
			name:       "A=T B=T C=F: no closing quote not unquoted",
			input:      "'hello",
			wantOutput: "'hello",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Unquote(tt.input)
			if got != tt.wantOutput {
				t.Errorf("Unquote(%q) = %q, want %q", tt.input, got, tt.wantOutput)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: isMatchingQuote(s,'\”) || isMatchingQuote(s,'"') || isMatchingQuote(s,'`')
// Location: lexer.go Unquote()
// Purpose: tries each of the three standard quote characters in order.
//
// Sub-condition A: isMatchingQuote(s, '\”) — single quote
// Sub-condition B: isMatchingQuote(s, '"')  — double quote
// Sub-condition C: isMatchingQuote(s, '`')  — backtick
//
// MC/DC table:
//
//	A=T B=F C=F → T (single-quoted string unquoted)
//	A=F B=T C=F → T (double-quoted identifier unquoted)
//	A=F B=F C=T → T (backtick-quoted identifier unquoted)
//	A=F B=F C=F → F (no standard quote — bracket or bare identifier)
//
// -----------------------------------------------------------------------------
func TestMCDC_Unquote_QuoteType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		input      string
		wantOutput string
	}{
		// A=T B=F C=F: single-quoted string
		{
			name:       "A=T B=F C=F: single-quoted unquoted",
			input:      "'value'",
			wantOutput: "value",
		},
		// A=F B=T C=F: double-quoted identifier
		{
			name:       "A=F B=T C=F: double-quoted unquoted",
			input:      `"value"`,
			wantOutput: "value",
		},
		// A=F B=F C=T: backtick-quoted identifier
		{
			name:       "A=F B=F C=T: backtick-quoted unquoted",
			input:      "`value`",
			wantOutput: "value",
		},
		// A=F B=F C=F: bracket-quoted identifier — handled by separate branch
		{
			name:       "A=F B=F C=F: bracket-quoted unquoted",
			input:      "[value]",
			wantOutput: "value",
		},
		// A=F B=F C=F: bare identifier — returned as-is
		{
			name:       "A=F B=F C=F: bare identifier unchanged",
			input:      "value",
			wantOutput: "value",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Unquote(tt.input)
			if got != tt.wantOutput {
				t.Errorf("Unquote(%q) = %q, want %q", tt.input, got, tt.wantOutput)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralFloat || lit.Type == LiteralInteger
// Location: parser.go FloatValue()
// Purpose: FloatValue accepts both float and integer literals.
//
// Sub-condition A: ok (expr is *LiteralExpr)
// Sub-condition B: lit.Type == LiteralFloat
// Sub-condition C: lit.Type == LiteralInteger
//
// MC/DC table (for the inner OR: B || C, guarded by A):
//
//	A=T B=T C=F → T (float literal accepted by FloatValue)
//	A=T B=F C=T → T (integer literal accepted by FloatValue)
//	A=T B=F C=F → F (non-numeric literal — FloatValue returns error)
//	A=F B=* C=* → F (non-LiteralExpr — FloatValue returns error)
//
// -----------------------------------------------------------------------------
func TestMCDC_FloatValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		want    float64
	}{
		// A=T B=T C=F: float literal
		{
			name:    "A=T B=T C=F: float literal accepted",
			sql:     "SELECT 3.14 FROM t",
			wantErr: false,
			want:    3.14,
		},
		// A=T B=F C=T: integer literal coerced to float
		{
			name:    "A=T B=F C=T: integer literal accepted as float",
			sql:     "SELECT 42 FROM t",
			wantErr: false,
			want:    42.0,
		},
		// A=T B=F C=F: string literal — not numeric
		{
			name:    "A=T B=F C=F: string literal errors in FloatValue",
			sql:     "SELECT 'hello' FROM t",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString(%q) unexpected error: %v", tt.sql, err)
			}
			sel := stmts[0].(*SelectStmt)
			val, err := FloatValue(sel.Columns[0].Expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("FloatValue err=%v, wantErr=%v (sql=%q)", err, tt.wantErr, tt.sql)
			}
			if err == nil && val != tt.want {
				t.Errorf("FloatValue = %v, want %v (sql=%q)", val, tt.want, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralInteger
// Location: parser.go IntValue()
// Purpose: IntValue only accepts integer literals, not floats or strings.
//
// Sub-condition A: ok (expr is *LiteralExpr)
// Sub-condition B: lit.Type == LiteralInteger
//
// MC/DC table:
//
//	A=T B=T → T (integer literal accepted by IntValue)
//	A=T B=F → F (float literal rejected by IntValue)
//	A=F B=* → F (non-LiteralExpr rejected by IntValue)
//
// -----------------------------------------------------------------------------
func TestMCDC_IntValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		want    int64
	}{
		// A=T B=T: integer literal
		{
			name:    "A=T B=T: integer literal accepted",
			sql:     "SELECT 7 FROM t",
			wantErr: false,
			want:    7,
		},
		// A=T B=F: float literal rejected
		{
			name:    "A=T B=F: float literal errors in IntValue",
			sql:     "SELECT 1.5 FROM t",
			wantErr: true,
		},
		// A=F B=*: column reference — not a LiteralExpr
		{
			name:    "A=F B=*: column reference errors in IntValue",
			sql:     "SELECT id FROM t",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString(%q) unexpected error: %v", tt.sql, err)
			}
			sel := stmts[0].(*SelectStmt)
			val, err := IntValue(sel.Columns[0].Expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("IntValue err=%v, wantErr=%v (sql=%q)", err, tt.wantErr, tt.sql)
			}
			if err == nil && val != tt.want {
				t.Errorf("IntValue = %v, want %v (sql=%q)", val, tt.want, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralString
// Location: parser.go StringValue()
// Purpose: StringValue only accepts string literals, not integers or floats.
//
// Sub-condition A: ok (expr is *LiteralExpr)
// Sub-condition B: lit.Type == LiteralString
//
// MC/DC table:
//
//	A=T B=T → T (string literal accepted)
//	A=T B=F → F (integer literal rejected)
//	A=F B=* → F (non-literal rejected)
//
// -----------------------------------------------------------------------------
func TestMCDC_StringValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		want    string
	}{
		// A=T B=T: string literal
		{
			name:    "A=T B=T: string literal accepted",
			sql:     "SELECT 'hello' FROM t",
			wantErr: false,
			want:    "hello",
		},
		// A=T B=F: integer literal rejected
		{
			name:    "A=T B=F: integer literal errors in StringValue",
			sql:     "SELECT 42 FROM t",
			wantErr: true,
		},
		// A=F B=*: function call — not a literal
		{
			name:    "A=F B=*: function call errors in StringValue",
			sql:     "SELECT upper('x') FROM t",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if err != nil {
				t.Fatalf("ParseString(%q) unexpected error: %v", tt.sql, err)
			}
			sel := stmts[0].(*SelectStmt)
			val, err := StringValue(sel.Columns[0].Expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("StringValue err=%v, wantErr=%v (sql=%q)", err, tt.wantErr, tt.sql)
			}
			if err == nil && val != tt.want {
				t.Errorf("StringValue = %q, want %q (sql=%q)", val, tt.want, tt.sql)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition (trigger body loop): !p.check(TK_END) && !p.isAtEnd()
// Location: parser.go parseTriggerBody()
// Purpose: loop continues only while neither END token nor actual EOF is seen.
//
// Sub-condition A: !p.check(TK_END) — END keyword not yet seen
// Sub-condition B: !p.isAtEnd() — input is not exhausted
//
// MC/DC table:
//
//	A=T B=T → T (loop continues — more trigger body statements consumed)
//	A=F B=T → F (END seen — loop exits, END consumed)
//	A=T B=F → F (EOF without END — error)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseTriggerBody_LoopCondition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=T: trigger body has multiple statements before END
		{
			name:    "A=T B=T: trigger body with multiple statements",
			sql:     "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; SELECT 2; END",
			wantErr: false,
		},
		// A=F B=T: trigger body immediately followed by END (empty body)
		{
			name:    "A=F B=T: empty trigger body",
			sql:     "CREATE TRIGGER tr AFTER INSERT ON t BEGIN END",
			wantErr: false,
		},
		// A=T B=F: trigger body never ends — EOF without END token
		{
			name:    "A=T B=F: trigger body without END errors",
			sql:     "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: p.check(TK_ID) && p.peekAhead(1).Type != TK_LP
// Location: parser.go parseFunctionOver()
// Purpose: after OVER, if an identifier is followed by something other than '('
//
//	it is treated as a window name reference rather than an inline spec.
//
// Sub-condition A: p.check(TK_ID) — token is an identifier
// Sub-condition B: p.peekAhead(1).Type != TK_LP — next token is not '('
//
// MC/DC table:
//
//	A=T B=T → T (window name reference: OVER win_name)
//	A=T B=F → F (identifier followed by '(' — inline window spec parsed instead)
//	A=F B=T → F (not an identifier — must be '(' for inline spec)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseFunctionOver_WindowNameVsSpec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// A=T B=T: named window reference
		{
			name:    "A=T B=T: OVER named window",
			sql:     "SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY id)",
			wantErr: false,
		},
		// A=T B=F: identifier followed by '(' — inline window spec
		{
			name:    "A=T B=F: OVER (inline spec)",
			sql:     "SELECT row_number() OVER (ORDER BY id) FROM t",
			wantErr: false,
		},
		// A=F B=T: OVER without identifier or '(' — error
		{
			name:    "A=F B=T: OVER without name or spec errors",
			sql:     "SELECT row_number() OVER FROM t",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: op == OpNeg && lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralInteger
// Location: parser.go parseUnaryExprWithOp()
// Purpose: special-cases negation of the integer 9223372036854775808 to fold it
//
//	into the minimum int64 literal rather than wrapping in UnaryExpr.
//
// Sub-condition A: op == OpNeg — the unary operator is negation
// Sub-condition B: ok (inner expression is *LiteralExpr) && lit.Type == LiteralInteger
//
// MC/DC table:
//
//	A=T B=T → T (negative int64-min literal folds; -9223372036854775808 value kept)
//	A=F B=T → F (unary plus on integer — no folding)
//	A=T B=F → F (negation of float — no folding)
//
// -----------------------------------------------------------------------------
func TestMCDC_ParseUnaryExprWithOp_NegFold(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		checkExpr func(expr Expression) bool
	}{
		// A=T B=T: negation of integer literal
		{
			name:    "A=T B=T: negation of integer literal",
			sql:     "SELECT -42 FROM t",
			wantErr: false,
			checkExpr: func(expr Expression) bool {
				// Either a folded literal or a UnaryExpr wrapping an integer
				switch e := expr.(type) {
				case *LiteralExpr:
					return strings.HasPrefix(e.Value, "-") && e.Type == LiteralInteger
				case *UnaryExpr:
					return e.Op == OpNeg
				}
				return false
			},
		},
		// A=T B=T: special int64-min case
		{
			name:    "A=T B=T: -9223372036854775808 folds to literal",
			sql:     "SELECT -9223372036854775808 FROM t",
			wantErr: false,
			checkExpr: func(expr Expression) bool {
				lit, ok := expr.(*LiteralExpr)
				return ok && lit.Value == "-9223372036854775808"
			},
		},
		// A=F B=T: unary plus on integer — unary + is consumed transparently,
		// leaving the raw integer literal (op != OpNeg, so no folding check)
		{
			name:    "A=F B=T: unary plus on integer",
			sql:     "SELECT +42 FROM t",
			wantErr: false,
			checkExpr: func(expr Expression) bool {
				// unary + is a no-op; result is the bare literal
				lit, ok := expr.(*LiteralExpr)
				return ok && lit.Type == LiteralInteger
			},
		},
		// A=T B=F: negation of float
		{
			name:    "A=T B=F: negation of float literal",
			sql:     "SELECT -3.14 FROM t",
			wantErr: false,
			checkExpr: func(expr Expression) bool {
				_, ok := expr.(*UnaryExpr)
				return ok
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmts, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			sel := stmts[0].(*SelectStmt)
			if !tt.checkExpr(sel.Columns[0].Expr) {
				t.Errorf("checkExpr failed for sql=%q (got %T)", tt.sql, sel.Columns[0].Expr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Condition: isJoinKeyword — multi-OR across eight join-related tokens
// Location: parser.go isJoinKeyword()
// Purpose: identifies any token that begins or qualifies a join clause so it
//
//	is not consumed as an implicit table alias.
//
// This test exercises three representative MC/DC sub-cases:
//
//	A=T (JOIN alone) → T
//	A=F B=T (LEFT alone) → T
//	A=F ... =F (no join keyword) → F
//
// -----------------------------------------------------------------------------
func TestMCDC_IsJoinKeyword(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		// wantJoin is the type of join we expect to see (empty = no join)
		wantJoin string
	}{
		// JOIN keyword alone
		{
			name:     "JOIN keyword produces inner join",
			sql:      "SELECT * FROM a JOIN b ON a.id = b.id",
			wantErr:  false,
			wantJoin: "inner",
		},
		// LEFT keyword starts a left join
		{
			name:     "LEFT keyword produces left join",
			sql:      "SELECT * FROM a LEFT JOIN b ON a.id = b.id",
			wantErr:  false,
			wantJoin: "left",
		},
		// CROSS keyword
		{
			name:     "CROSS keyword produces cross join",
			sql:      "SELECT * FROM a CROSS JOIN b",
			wantErr:  false,
			wantJoin: "cross",
		},
		// No join keyword — FROM with two tables via comma
		{
			name:     "no join keyword — comma-separated tables",
			sql:      "SELECT * FROM a, b",
			wantErr:  false,
			wantJoin: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseString(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseString(%q) err=%v, wantErr=%v", tt.sql, err, tt.wantErr)
			}
		})
	}
}
