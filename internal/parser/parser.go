// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
)

// keywordsAsIdentifiers lists keywords that can also be used as identifiers (column/table names)
// in SQLite without quoting. This includes most non-reserved keywords.
var keywordsAsIdentifiers = map[TokenType]bool{
	TK_ID:           true,
	TK_ACTION:       true,
	TK_ABORT:        true,
	TK_CASCADE:      true,
	TK_DEFERRED:     true,
	TK_IGNORE:       true,
	TK_IMMEDIATE:    true,
	TK_INITIALLY:    true,
	TK_KEY:          true,
	TK_NO:           true,
	TK_OFFSET:       true,
	TK_RESTRICT:     true,
	TK_ROLLBACK:     true,
	TK_TEMP:         true,
	TK_TEMPORARY:    true,
	TK_TRANSACTION:  true,
	TK_TEXT:         true,
	TK_INTEGER_TYPE: true,
	TK_REAL:         true,
	TK_BLOB_TYPE:    true,
	TK_NUMERIC:      true,
	TK_AFTER:        true,
	TK_BEFORE:       true,
	TK_BEGIN:        true,
	TK_END:          true,
	TK_CONFLICT:     true,
	TK_REPLACE:      true,
	TK_ASC:          true,
	TK_DESC:         true,
	TK_COLLATE:      true,
	TK_WITHOUT:      true,
	TK_ROWID:        true,
	TK_SAVEPOINT:    true,
	TK_RELEASE:      true,
	TK_OUTER:        true,
	TK_INNER:        true,
	TK_LEFT:         true,
	TK_RIGHT:        true,
	TK_CROSS:        true,
}

// columnConstraintKeywords lists keywords that indicate the start of a column constraint.
var columnConstraintKeywords = map[TokenType]bool{
	TK_CONSTRAINT: true,
	TK_PRIMARY:    true,
	TK_NOT:        true,
	TK_UNIQUE:     true,
	TK_CHECK:      true,
	TK_DEFAULT:    true,
	TK_COLLATE:    true,
	TK_REFERENCES: true,
	TK_GENERATED:  true,
	TK_AS:         true,
}

// Parser implements a recursive descent parser for SQL.
type Parser struct {
	lexer     *Lexer
	tokens    []Token
	current   int
	errors    []string
	exprDepth int // Current expression depth to prevent stack overflow
}

// NewParser creates a new parser for the given SQL input.
func NewParser(input string) *Parser {
	return &Parser{
		lexer:     NewLexer(input),
		tokens:    make([]Token, 0),
		errors:    make([]string, 0),
		exprDepth: 0,
	}
}

// Parse parses the SQL input and returns a list of statements.
func (p *Parser) Parse() ([]Statement, error) {
	// Check SQL length limit to prevent DoS attacks
	if len(p.lexer.input) > security.MaxSQLLength {
		return nil, fmt.Errorf("SQL query too long: %d bytes exceeds maximum of %d", len(p.lexer.input), security.MaxSQLLength)
	}

	p.tokenize()

	// Check token count limit
	if len(p.tokens) > security.MaxTokens {
		return nil, fmt.Errorf("SQL query has too many tokens: %d exceeds maximum of %d", len(p.tokens), security.MaxTokens)
	}

	statements, err := p.parseStatements()
	if err != nil {
		return statements, err
	}
	if len(p.errors) > 0 {
		return statements, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
	}
	return statements, nil
}

// tokenize reads all tokens from the lexer.
func (p *Parser) tokenize() {
	for {
		tok := p.lexer.NextToken()
		if tok.Type != TK_SPACE && tok.Type != TK_COMMENT {
			p.tokens = append(p.tokens, tok)
		}
		if tok.Type == TK_EOF {
			break
		}
	}
}

// parseStatements parses all statements from the token stream.
func (p *Parser) parseStatements() ([]Statement, error) {
	statements := make([]Statement, 0)
	for !p.isAtEnd() {
		if p.match(TK_SEMI) {
			continue
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return statements, err
		}
		statements = append(statements, stmt)
		p.match(TK_SEMI)
	}
	return statements, nil
}

type statementParser func(p *Parser) (Statement, error)

var statementParsers = map[TokenType]statementParser{
	TK_SELECT:   func(p *Parser) (Statement, error) { return p.parseSelect() },
	TK_INSERT:   func(p *Parser) (Statement, error) { return p.parseInsert() },
	TK_UPDATE:   func(p *Parser) (Statement, error) { return p.parseUpdate() },
	TK_DELETE:   func(p *Parser) (Statement, error) { return p.parseDelete() },
	TK_CREATE:   (*Parser).parseCreate,
	TK_DROP:     (*Parser).parseDrop,
	TK_ALTER:    (*Parser).parseAlter,
	TK_BEGIN:    func(p *Parser) (Statement, error) { return p.parseBegin() },
	TK_COMMIT:   func(p *Parser) (Statement, error) { return &CommitStmt{}, nil },
	TK_ROLLBACK: func(p *Parser) (Statement, error) { return p.parseRollback() },
	TK_ATTACH:   func(p *Parser) (Statement, error) { return p.parseAttach() },
	TK_DETACH:   func(p *Parser) (Statement, error) { return p.parseDetach() },
	TK_PRAGMA:   func(p *Parser) (Statement, error) { return p.parsePragma() },
	TK_VACUUM:   func(p *Parser) (Statement, error) { return p.parseVacuum() },
	TK_REINDEX:   func(p *Parser) (Statement, error) { return p.parseReindex() },
	TK_SAVEPOINT: func(p *Parser) (Statement, error) { return p.parseSavepoint() },
	TK_RELEASE:   func(p *Parser) (Statement, error) { return p.parseRelease() },
	TK_END:       func(p *Parser) (Statement, error) { return &CommitStmt{}, nil },
	TK_WITH:      func(p *Parser) (Statement, error) { return p.parseSelect() }, // WITH starts a CTE, parsed as part of SELECT
}

var statementParserOrder = []TokenType{
	TK_WITH, TK_SELECT, TK_INSERT, TK_UPDATE, TK_DELETE,
	TK_CREATE, TK_DROP, TK_ALTER, TK_BEGIN, TK_COMMIT, TK_ROLLBACK,
	TK_ATTACH, TK_DETACH, TK_PRAGMA, TK_VACUUM, TK_REINDEX,
	TK_SAVEPOINT, TK_RELEASE, TK_END,
}

func (p *Parser) parseStatement() (Statement, error) {
	if p.match(TK_EXPLAIN) {
		return p.parseExplain()
	}
	for _, tok := range statementParserOrder {
		if tok == TK_WITH {
			// Don't consume WITH - parseSelect() will handle it
			if p.check(TK_WITH) {
				return statementParsers[tok](p)
			}
		} else if p.match(tok) {
			return statementParsers[tok](p)
		}
	}
	return nil, p.error("expected statement, got %s", p.peek().Type)
}

// =============================================================================
// SELECT
// =============================================================================

func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	// Parse optional WITH clause
	if p.check(TK_WITH) {
		with, err := p.parseWithClause()
		if err != nil {
			return nil, err
		}
		stmt.With = with

		// After WITH clause, the main SELECT keyword should follow
		// (it wasn't consumed by the statement dispatcher because WITH was first)
		if !p.match(TK_SELECT) {
			return nil, p.error("expected SELECT after WITH clause")
		}
	}

	return p.parseSelectBody(stmt)
}

// parseSelectBody parses the body of a SELECT statement (after WITH clause and SELECT keyword).
// This is used both for top-level SELECT and for CTE SELECT bodies.
func (p *Parser) parseSelectBody(stmt *SelectStmt) (*SelectStmt, error) {
	// DISTINCT or ALL
	if p.match(TK_DISTINCT) {
		stmt.Distinct = true
	} else {
		p.match(TK_ALL)
	}

	// Result columns
	cols, err := p.parseResultColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// Parse optional clauses
	if err := p.parseSelectClauses(stmt); err != nil {
		return nil, err
	}

	// Compound SELECT (UNION, EXCEPT, INTERSECT)
	if p.checkCompoundOp() {
		return p.parseCompoundSelect(stmt)
	}

	return stmt, nil
}

// parseSelectClauses parses FROM, WHERE, GROUP BY, WINDOW, ORDER BY, and LIMIT clauses.
func (p *Parser) parseSelectClauses(stmt *SelectStmt) error {
	if err := p.parseFromClauseInto(stmt); err != nil {
		return err
	}
	if err := p.parseWhereClauseInto(stmt); err != nil {
		return err
	}
	if err := p.parseGroupByClauseInto(stmt); err != nil {
		return err
	}
	if err := p.parseWindowClauseInto(stmt); err != nil {
		return err
	}
	if err := p.parseOrderByClauseInto(stmt); err != nil {
		return err
	}
	return p.parseLimitClauseInto(stmt)
}

// parseWindowClauseInto parses the optional WINDOW clause for named window definitions.
// Syntax: WINDOW name AS (window-spec) [, name AS (window-spec)] ...
func (p *Parser) parseWindowClauseInto(stmt *SelectStmt) error {
	if !p.match(TK_WINDOW) {
		return nil
	}
	defs, err := p.parseWindowDefList()
	if err != nil {
		return err
	}
	stmt.WindowDefs = defs
	return nil
}

// parseWindowDefList parses a comma-separated list of named window definitions.
func (p *Parser) parseWindowDefList() ([]WindowDef, error) {
	var defs []WindowDef
	for {
		def, err := p.parseWindowDef()
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
		if !p.match(TK_COMMA) {
			break
		}
	}
	return defs, nil
}

// parseWindowDef parses a single named window definition: name AS (window-spec).
func (p *Parser) parseWindowDef() (WindowDef, error) {
	if !p.check(TK_ID) {
		return WindowDef{}, p.error("expected window name")
	}
	name := Unquote(p.advance().Lexeme)
	if !p.match(TK_AS) {
		return WindowDef{}, p.error("expected AS after window name")
	}
	if !p.match(TK_LP) {
		return WindowDef{}, p.error("expected ( after AS")
	}

	spec := &WindowSpec{}
	if err := p.parsePartitionBy(spec); err != nil {
		return WindowDef{}, err
	}
	if err := p.parseWindowOrderBy(spec); err != nil {
		return WindowDef{}, err
	}
	if err := p.parseWindowFrame(spec); err != nil {
		return WindowDef{}, err
	}

	if !p.match(TK_RP) {
		return WindowDef{}, p.error("expected ) after window specification")
	}

	return WindowDef{Name: name, Spec: spec}, nil
}

// parseFromClauseInto parses the FROM clause into the statement.
func (p *Parser) parseFromClauseInto(stmt *SelectStmt) error {
	if !p.match(TK_FROM) {
		return nil
	}
	from, err := p.parseFromClause()
	if err != nil {
		return err
	}
	stmt.From = from
	return nil
}

// parseWhereClauseInto parses the WHERE clause into the statement.
func (p *Parser) parseWhereClauseInto(stmt *SelectStmt) error {
	if !p.match(TK_WHERE) {
		return nil
	}
	where, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Where = where
	return nil
}

// parseGroupByClauseInto parses the GROUP BY and HAVING clauses.
func (p *Parser) parseGroupByClauseInto(stmt *SelectStmt) error {
	if p.match(TK_GROUP) {
		if !p.match(TK_BY) {
			return p.error("expected BY after GROUP")
		}
		groupBy, err := p.parseExpressionList()
		if err != nil {
			return err
		}
		stmt.GroupBy = groupBy
	}

	// HAVING clause (can appear with or without GROUP BY)
	if p.match(TK_HAVING) {
		having, err := p.parseExpression()
		if err != nil {
			return err
		}
		stmt.Having = having
	}
	return nil
}

// parseOrderByClauseInto parses the ORDER BY clause.
func (p *Parser) parseOrderByClauseInto(stmt *SelectStmt) error {
	if !p.match(TK_ORDER) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after ORDER")
	}
	orderBy, err := p.parseOrderByList()
	if err != nil {
		return err
	}
	stmt.OrderBy = orderBy
	return nil
}

// parseLimitClauseInto parses the LIMIT and OFFSET clauses.
func (p *Parser) parseLimitClauseInto(stmt *SelectStmt) error {
	if !p.match(TK_LIMIT) {
		return nil
	}
	limit, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Limit = limit

	// OFFSET clause
	if p.match(TK_OFFSET) || p.match(TK_COMMA) {
		offset, err := p.parseExpression()
		if err != nil {
			return err
		}
		stmt.Offset = offset
	}
	return nil
}

// checkCompoundOp checks if the next token is a compound operator.
func (p *Parser) checkCompoundOp() bool {
	return p.check(TK_UNION) || p.check(TK_EXCEPT) || p.check(TK_INTERSECT)
}

func (p *Parser) parseCompoundSelect(left *SelectStmt) (*SelectStmt, error) {
	var op CompoundOp
	if p.match(TK_UNION) {
		if p.match(TK_ALL) {
			op = CompoundUnionAll
		} else {
			op = CompoundUnion
		}
	} else if p.match(TK_EXCEPT) {
		op = CompoundExcept
	} else if p.match(TK_INTERSECT) {
		op = CompoundIntersect
	}

	// Consume SELECT keyword for the right side of the compound
	if !p.match(TK_SELECT) {
		return nil, p.error("expected SELECT after %s", op.String())
	}

	right, err := p.parseSelectBody(&SelectStmt{})
	if err != nil {
		return nil, err
	}

	result := &SelectStmt{
		Compound: &CompoundSelect{
			Op:    op,
			Left:  left,
			Right: right,
		},
	}

	return result, nil
}

// parseWithClause parses a WITH clause containing Common Table Expressions.
// Syntax: WITH [RECURSIVE] cte_name [(col1, col2, ...)] AS (SELECT ...) [, ...]
func (p *Parser) parseWithClause() (*WithClause, error) {
	if !p.match(TK_WITH) {
		return nil, p.error("expected WITH")
	}

	with := &WithClause{}

	// Check for RECURSIVE
	if p.match(TK_RECURSIVE) {
		with.Recursive = true
	}

	// Parse one or more CTEs
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return nil, err
		}
		with.CTEs = append(with.CTEs, *cte)

		// Check for more CTEs
		if !p.match(TK_COMMA) {
			break
		}
	}

	return with, nil
}

// parseCTE parses a single Common Table Expression.
// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...)
func (p *Parser) parseCTE() (*CTE, error) {
	cte := &CTE{}

	// Parse CTE name
	if !p.check(TK_ID) {
		return nil, p.error("expected CTE name")
	}
	cte.Name = Unquote(p.advance().Lexeme)

	// Parse optional column list
	if err := p.parseCTEColumns(cte); err != nil {
		return nil, err
	}

	// Parse AS (SELECT ...)
	if err := p.parseCTESelect(cte); err != nil {
		return nil, err
	}

	return cte, nil
}

// parseCTEColumns parses the optional column list in a CTE.
func (p *Parser) parseCTEColumns(cte *CTE) error {
	if p.match(TK_LP) {
		// Check if this is the AS clause or a column list
		// If we see SELECT, it's the AS clause
		if p.check(TK_SELECT) {
			// No column list, this is the start of the SELECT
			// Put back the LP by decrementing current
			p.current--
		} else {
			// Parse column list
			for {
				if !p.check(TK_ID) {
					return p.error("expected column name")
				}
				cte.Columns = append(cte.Columns, Unquote(p.advance().Lexeme))

				if !p.match(TK_COMMA) {
					break
				}
			}

			if !p.match(TK_RP) {
				return p.error("expected ) after column list")
			}
		}
	}
	return nil
}

// parseCTESelect parses the AS (SELECT ...) clause of a CTE.
func (p *Parser) parseCTESelect(cte *CTE) error {
	if !p.match(TK_AS) {
		return p.error("expected AS after CTE name")
	}

	if !p.match(TK_LP) {
		return p.error("expected ( after AS")
	}

	if !p.match(TK_SELECT) {
		return p.error("expected SELECT in CTE")
	}

	// Parse the SELECT statement body (SELECT keyword already consumed)
	sel, err := p.parseSelectBody(&SelectStmt{})
	if err != nil {
		return err
	}
	cte.Select = sel

	if !p.match(TK_RP) {
		return p.error("expected ) after CTE SELECT")
	}

	return nil
}

func (p *Parser) isTableStar() bool {
	return p.check(TK_ID) && p.peekAhead(1).Type == TK_DOT && p.peekAhead(2).Type == TK_STAR
}

func (p *Parser) parseAlias() (string, error) {
	if p.match(TK_AS) {
		if !p.check(TK_ID) && !p.check(TK_STRING) {
			return "", p.error("expected alias after AS")
		}
		return Unquote(p.advance().Lexeme), nil
	}
	if p.check(TK_ID) || p.check(TK_STRING) {
		return Unquote(p.advance().Lexeme), nil
	}
	return "", nil
}

func (p *Parser) parseExprColumn() (ResultColumn, error) {
	expr, err := p.parseExpression()
	if err != nil {
		return ResultColumn{}, err
	}
	col := ResultColumn{Expr: expr}
	col.Alias, err = p.parseAlias()
	if err != nil {
		return ResultColumn{}, err
	}
	return col, nil
}

func (p *Parser) parseOneResultColumn() (ResultColumn, error) {
	if p.match(TK_STAR) {
		return ResultColumn{Star: true}, nil
	}
	if p.isTableStar() {
		table := p.advance().Lexeme
		p.advance()
		p.advance()
		return ResultColumn{Table: table, Star: true}, nil
	}
	return p.parseExprColumn()
}

func (p *Parser) parseResultColumns() ([]ResultColumn, error) {
	columns := make([]ResultColumn, 0)
	for {
		col, err := p.parseOneResultColumn()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
		if !p.match(TK_COMMA) {
			break
		}
	}
	return columns, nil
}

func (p *Parser) parseFromClause() (*FromClause, error) {
	clause := &FromClause{
		Tables: make([]TableOrSubquery, 0),
		Joins:  make([]JoinClause, 0),
	}

	// Parse first table or subquery
	table, err := p.parseTableOrSubquery()
	if err != nil {
		return nil, err
	}
	clause.Tables = append(clause.Tables, *table)

	// Parse joins
	for p.isJoinKeyword() {
		join, err := p.parseJoinClause()
		if err != nil {
			return nil, err
		}
		clause.Joins = append(clause.Joins, *join)
	}

	// Parse comma-separated tables (implicit cross join)
	for p.match(TK_COMMA) {
		table, err := p.parseTableOrSubquery()
		if err != nil {
			return nil, err
		}
		clause.Tables = append(clause.Tables, *table)
	}

	return clause, nil
}

func (p *Parser) parseTableOrSubquery() (*TableOrSubquery, error) {
	table := &TableOrSubquery{}
	var err error
	if p.match(TK_LP) {
		err = p.parseSubquery(table)
	} else {
		err = p.parseTableRef(table)
	}
	if err != nil {
		return nil, err
	}
	if err = p.parseTableAlias(table); err != nil {
		return nil, err
	}
	return table, nil
}

func (p *Parser) parseSubquery(table *TableOrSubquery) error {
	if !p.match(TK_SELECT) {
		return p.error("expected SELECT in subquery")
	}
	subquery, err := p.parseSelect()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after subquery")
	}
	table.Subquery = subquery
	return nil
}

func (p *Parser) parseTableRef(table *TableOrSubquery) error {
	if !p.isTableIdentifier() {
		return p.error("expected table name")
	}
	firstIdent := p.consumeTableIdentifier()

	// Check for schema.table syntax
	if p.match(TK_DOT) {
		if !p.isTableIdentifier() {
			return p.error("expected table name after schema")
		}
		table.Schema = firstIdent
		table.TableName = p.consumeTableIdentifier()
	} else {
		table.TableName = firstIdent
	}

	// Check for table-valued function syntax: name(args...)
	if p.match(TK_LP) {
		return p.parseTableFuncArgs(table)
	}

	return p.parseIndexedBy(table)
}

// parseTableFuncArgs parses table-valued function arguments after '(' has been consumed.
func (p *Parser) parseTableFuncArgs(table *TableOrSubquery) error {
	args, err := p.parseExpressionList()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after function arguments")
	}
	table.FuncArgs = args
	return nil
}

// parseIndexedBy parses an optional INDEXED BY clause.
func (p *Parser) parseIndexedBy(table *TableOrSubquery) error {
	if !p.match(TK_INDEXED) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after INDEXED")
	}
	if !p.isTableIdentifier() {
		return p.error("expected index name")
	}
	table.Indexed = p.consumeTableIdentifier()
	return nil
}

// isTableIdentifier checks if the current token can be used as a table/index name.
// This includes regular identifiers and certain keywords that can be used as names.
func (p *Parser) isTableIdentifier() bool {
	switch p.peek().Type {
	case TK_ID, TK_TEMP:
		return true
	default:
		return false
	}
}

// consumeTableIdentifier consumes and returns a table/index identifier token.
func (p *Parser) consumeTableIdentifier() string {
	tok := p.advance()
	return Unquote(tok.Lexeme)
}

func (p *Parser) parseTableAlias(table *TableOrSubquery) error {
	if p.match(TK_AS) {
		if !p.check(TK_ID) {
			return p.error("expected alias after AS")
		}
		table.Alias = Unquote(p.advance().Lexeme)
		return nil
	}
	if p.check(TK_ID) && !p.isJoinKeyword() {
		table.Alias = Unquote(p.advance().Lexeme)
	}
	return nil
}

// joinTypeMap maps a keyword token to its JoinType. Tokens that accept an
// optional OUTER suffix are listed in joinOuterTokens.
var joinTypeMap = map[TokenType]JoinType{
	TK_LEFT:  JoinLeft,
	TK_RIGHT: JoinRight,
	TK_INNER: JoinInner,
	TK_CROSS: JoinCross,
}

// joinOuterTokens is the set of join-type tokens that may be followed by OUTER.
var joinOuterTokens = map[TokenType]bool{
	TK_LEFT:  true,
	TK_RIGHT: true,
}

func (p *Parser) parseJoinClause() (*JoinClause, error) {
	join := &JoinClause{}

	join.Natural = p.match(TK_NATURAL) // optional NATURAL prefix

	p.parseJoinType(join)

	if !p.match(TK_JOIN) {
		return nil, p.error("expected JOIN")
	}

	table, err := p.parseTableOrSubquery()
	if err != nil {
		return nil, err
	}
	join.Table = *table

	return join, p.parseJoinCondition(join)
}

// parseJoinType consumes the optional directional keyword (LEFT, RIGHT, INNER,
// CROSS) and the optional OUTER suffix, then sets join.Type.
func (p *Parser) parseJoinType(join *JoinClause) {
	for tok, jt := range joinTypeMap {
		if !p.match(tok) {
			continue
		}
		join.Type = jt
		if joinOuterTokens[tok] {
			p.match(TK_OUTER) // optional OUTER, discard
		}
		return
	}
}

// parseJoinCondition parses the optional ON or USING clause.
func (p *Parser) parseJoinCondition(join *JoinClause) error {
	if p.match(TK_ON) {
		return p.parseJoinOnCondition(join)
	}
	if p.match(TK_USING) {
		return p.parseJoinUsingCondition(join)
	}
	return nil
}

// parseJoinOnCondition parses an ON <expr> join condition.
func (p *Parser) parseJoinOnCondition(join *JoinClause) error {
	condition, err := p.parseExpression()
	if err != nil {
		return err
	}
	join.Condition.On = condition
	return nil
}

// parseJoinUsingCondition parses a USING (col, ...) join condition.
func (p *Parser) parseJoinUsingCondition(join *JoinClause) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after USING")
	}
	columns, err := p.parseUsingColumnList()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after USING columns")
	}
	join.Condition.Using = columns
	return nil
}

// parseUsingColumnList parses the comma-separated identifier list inside USING().
func (p *Parser) parseUsingColumnList() ([]string, error) {
	columns := make([]string, 0)
	for {
		if !p.check(TK_ID) {
			return nil, p.error("expected column name")
		}
		columns = append(columns, Unquote(p.advance().Lexeme))
		if !p.match(TK_COMMA) {
			break
		}
	}
	return columns, nil
}

// =============================================================================
// INSERT
// =============================================================================

func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}

	if p.match(TK_OR) {
		stmt.OnConflict = p.parseOnConflict()
	}

	if !p.match(TK_INTO) {
		return nil, p.error("expected INTO after INSERT")
	}
	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)

	// Handle schema-qualified table name: schema.table
	if p.match(TK_DOT) {
		if !p.check(TK_ID) {
			return nil, p.error("expected table name after '.'")
		}
		stmt.Schema = stmt.Table
		stmt.Table = Unquote(p.advance().Lexeme)
	}

	if err := p.parseInsertColumnList(stmt); err != nil {
		return nil, err
	}
	if err := p.parseInsertSource(stmt); err != nil {
		return nil, err
	}
	if err := p.parseInsertUpsertClause(stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseInsertColumnList parses the optional (col1, col2, ...) column list.
func (p *Parser) parseInsertColumnList(stmt *InsertStmt) error {
	if !p.match(TK_LP) {
		return nil
	}
	for {
		if !p.checkIdentifier() {
			return p.error("expected column name")
		}
		stmt.Columns = append(stmt.Columns, Unquote(p.advance().Lexeme))
		if !p.match(TK_COMMA) {
			break
		}
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after column list")
	}
	return nil
}

// parseInsertValues parses the VALUES (...), (...) clause.
func (p *Parser) parseInsertValues(stmt *InsertStmt) error {
	for {
		if !p.match(TK_LP) {
			return p.error("expected ( before values")
		}
		values, err := p.parseExpressionList()
		if err != nil {
			return err
		}
		stmt.Values = append(stmt.Values, values)
		if !p.match(TK_RP) {
			return p.error("expected ) after values")
		}
		if !p.match(TK_COMMA) {
			break
		}
	}
	return nil
}

// parseInsertSource parses the VALUES, SELECT, or DEFAULT VALUES source.
func (p *Parser) parseInsertSource(stmt *InsertStmt) error {
	switch {
	case p.match(TK_VALUES):
		return p.parseInsertValues(stmt)
	case p.match(TK_SELECT):
		// SELECT keyword already consumed, go directly to body
		sel, err := p.parseSelectBody(&SelectStmt{})
		if err != nil {
			return err
		}
		stmt.Select = sel
		return nil
	case p.match(TK_DEFAULT):
		if !p.match(TK_VALUES) {
			return p.error("expected VALUES after DEFAULT")
		}
		stmt.DefaultVals = true
		return nil
	default:
		return p.error("expected VALUES, SELECT, or DEFAULT")
	}
}

// parseInsertUpsertClause parses the optional ON CONFLICT clause.
func (p *Parser) parseInsertUpsertClause(stmt *InsertStmt) error {
	if !p.match(TK_ON) {
		return nil
	}
	if !p.match(TK_CONFLICT) {
		return p.error("expected CONFLICT after ON")
	}
	upsert, err := p.parseUpsertClause()
	if err != nil {
		return err
	}
	stmt.Upsert = upsert
	return nil
}

// parseUpsertClause parses the complete ON CONFLICT clause.
// ON CONFLICT [(columns) [WHERE expr]] [ON CONSTRAINT name] DO NOTHING | DO UPDATE SET ...
func (p *Parser) parseUpsertClause() (*UpsertClause, error) {
	upsert := &UpsertClause{}

	// Parse conflict target (optional)
	if err := p.parseConflictTarget(upsert); err != nil {
		return nil, err
	}

	// Parse DO NOTHING or DO UPDATE
	if !p.match(TK_DO) {
		return nil, p.error("expected DO after ON CONFLICT")
	}

	if p.match(TK_NOTHING) {
		upsert.Action = ConflictDoNothing
		return upsert, nil
	}

	if p.match(TK_UPDATE) {
		upsert.Action = ConflictDoUpdate
		return p.parseDoUpdateClause(upsert)
	}

	return nil, p.error("expected NOTHING or UPDATE after DO")
}

// parseConflictTarget parses the optional conflict target.
// (columns) [WHERE expr] or ON CONSTRAINT name
func (p *Parser) parseConflictTarget(upsert *UpsertClause) error {
	// ON CONSTRAINT name
	if p.match(TK_ON) {
		return p.parseConstraintTarget(upsert)
	}

	// (columns) [WHERE expr]
	if p.match(TK_LP) {
		return p.parseColumnsTarget(upsert)
	}

	return nil
}

// parseConstraintTarget handles ON CONSTRAINT name syntax.
func (p *Parser) parseConstraintTarget(upsert *UpsertClause) error {
	if !p.match(TK_CONSTRAINT) {
		return p.error("expected CONSTRAINT after ON")
	}
	if !p.check(TK_ID) {
		return p.error("expected constraint name")
	}
	upsert.Target = &ConflictTarget{
		ConstraintName: Unquote(p.advance().Lexeme),
	}
	return nil
}

// parseColumnsTarget handles (columns) [WHERE expr] syntax.
func (p *Parser) parseColumnsTarget(upsert *UpsertClause) error {
	cols, err := p.parseIndexedColumns()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after conflict columns")
	}
	target := &ConflictTarget{Columns: cols}

	// Optional WHERE clause
	if p.match(TK_WHERE) {
		where, err := p.parseExpression()
		if err != nil {
			return err
		}
		target.Where = where
	}

	upsert.Target = target
	return nil
}

// parseDoUpdateClause parses the DO UPDATE SET clause.
func (p *Parser) parseDoUpdateClause(upsert *UpsertClause) (*UpsertClause, error) {
	if !p.match(TK_SET) {
		return nil, p.error("expected SET after DO UPDATE")
	}

	doUpdate := &DoUpdateClause{}

	// Parse SET assignments
	if err := p.parseSetAssignments(doUpdate); err != nil {
		return nil, err
	}

	// Optional WHERE clause
	if err := p.parseOptionalWhereExpr(&doUpdate.Where); err != nil {
		return nil, err
	}

	upsert.Update = doUpdate
	return upsert, nil
}

// parseSetAssignments parses a comma-separated list of column assignments.
func (p *Parser) parseSetAssignments(doUpdate *DoUpdateClause) error {
	for {
		if !p.check(TK_ID) {
			return p.error("expected column name")
		}
		column := Unquote(p.advance().Lexeme)

		if !p.match(TK_EQ) {
			return p.error("expected = after column name")
		}

		value, err := p.parseExpression()
		if err != nil {
			return err
		}

		doUpdate.Sets = append(doUpdate.Sets, Assignment{Column: column, Value: value})

		if !p.match(TK_COMMA) {
			break
		}
	}
	return nil
}

// parseOptionalWhereExpr parses an optional WHERE clause.
func (p *Parser) parseOptionalWhereExpr(where *Expression) error {
	if p.match(TK_WHERE) {
		expr, err := p.parseExpression()
		if err != nil {
			return err
		}
		*where = expr
	}
	return nil
}

// =============================================================================
// UPDATE
// =============================================================================

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}

	if p.match(TK_OR) {
		stmt.OnConflict = p.parseOnConflict()
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)

	if !p.match(TK_SET) {
		return nil, p.error("expected SET")
	}

	if err := p.parseUpdateClauses(stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseUpdateClauses parses all post-SET clauses: assignments, WHERE, ORDER BY,
// and LIMIT. Grouping them here keeps parseUpdate at CC <= 6.
func (p *Parser) parseUpdateClauses(stmt *UpdateStmt) error {
	if err := p.parseUpdateAssignments(stmt); err != nil {
		return err
	}
	if err := p.parseUpdateWhereClause(stmt); err != nil {
		return err
	}
	if err := p.parseUpdateOrderByClause(stmt); err != nil {
		return err
	}
	return p.parseUpdateLimitClause(stmt)
}

// parseUpdateAssignments parses the comma-separated col = expr assignment list.
func (p *Parser) parseUpdateAssignments(stmt *UpdateStmt) error {
	for {
		if !p.check(TK_ID) {
			return p.error("expected column name")
		}
		column := Unquote(p.advance().Lexeme)

		if !p.match(TK_EQ) {
			return p.error("expected = after column name")
		}
		value, err := p.parseExpression()
		if err != nil {
			return err
		}
		stmt.Sets = append(stmt.Sets, Assignment{Column: column, Value: value})

		if !p.match(TK_COMMA) {
			break
		}
	}
	return nil
}

// parseUpdateWhereClause parses the optional WHERE clause.
func (p *Parser) parseUpdateWhereClause(stmt *UpdateStmt) error {
	if !p.match(TK_WHERE) {
		return nil
	}
	where, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Where = where
	return nil
}

// parseUpdateOrderByClause parses the optional ORDER BY clause.
func (p *Parser) parseUpdateOrderByClause(stmt *UpdateStmt) error {
	if !p.match(TK_ORDER) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after ORDER")
	}
	orderBy, err := p.parseOrderByList()
	if err != nil {
		return err
	}
	stmt.OrderBy = orderBy
	return nil
}

// parseUpdateLimitClause parses the optional LIMIT clause.
func (p *Parser) parseUpdateLimitClause(stmt *UpdateStmt) error {
	if !p.match(TK_LIMIT) {
		return nil
	}
	limit, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Limit = limit
	return nil
}

// =============================================================================
// DELETE
// =============================================================================

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}
	if !p.match(TK_FROM) {
		return nil, p.error("expected FROM after DELETE")
	}
	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)
	if err := p.parseDeleteClauses(stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseDeleteClauses parses optional WHERE, ORDER BY, and LIMIT clauses.
func (p *Parser) parseDeleteClauses(stmt *DeleteStmt) error {
	if err := p.parseDeleteWhere(stmt); err != nil {
		return err
	}
	if err := p.parseDeleteOrderBy(stmt); err != nil {
		return err
	}
	return p.parseDeleteLimit(stmt)
}

func (p *Parser) parseDeleteWhere(stmt *DeleteStmt) error {
	if !p.match(TK_WHERE) {
		return nil
	}
	where, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Where = where
	return nil
}

func (p *Parser) parseDeleteOrderBy(stmt *DeleteStmt) error {
	if !p.match(TK_ORDER) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after ORDER")
	}
	orderBy, err := p.parseOrderByList()
	if err != nil {
		return err
	}
	stmt.OrderBy = orderBy
	return nil
}

func (p *Parser) parseDeleteLimit(stmt *DeleteStmt) error {
	if !p.match(TK_LIMIT) {
		return nil
	}
	limit, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Limit = limit
	return nil
}

// =============================================================================
// CREATE
// =============================================================================

func (p *Parser) parseCreate() (Statement, error) {
	// TEMP/TEMPORARY
	temp := p.match(TK_TEMP) || p.match(TK_TEMPORARY)

	// Check for VIRTUAL TABLE
	if p.match(TK_VIRTUAL) {
		if !p.match(TK_TABLE) {
			return nil, p.error("expected TABLE after VIRTUAL")
		}
		return p.parseCreateVirtualTable()
	}

	// Check for UNIQUE before INDEX
	unique := p.match(TK_UNIQUE)

	if p.match(TK_TABLE) {
		return p.parseCreateTable(temp)
	} else if p.match(TK_INDEX) {
		return p.parseCreateIndex(unique)
	} else if p.match(TK_VIEW) {
		return p.parseCreateView(temp)
	} else if p.match(TK_TRIGGER) {
		return p.parseCreateTrigger(temp)
	} else if unique {
		return nil, p.error("expected INDEX after UNIQUE")
	} else {
		return nil, p.error("expected TABLE, INDEX, VIEW, or TRIGGER after CREATE")
	}
}

func (p *Parser) parseCreateTable(temp bool) (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{Temp: temp}

	if err := p.parseIfNotExists(&stmt.IfNotExists); err != nil {
		return nil, err
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	// Handle schema-qualified table name: schema.table
	if p.match(TK_DOT) {
		if !p.check(TK_ID) {
			return nil, p.error("expected table name after '.'")
		}
		stmt.Schema = stmt.Name
		stmt.Name = Unquote(p.advance().Lexeme)
	}

	if p.match(TK_AS) {
		return p.parseCreateTableAsSelect(stmt)
	}

	if err := p.parseCreateTableBody(stmt); err != nil {
		return nil, err
	}
	if err := p.parseTableOptions(stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseCreateVirtualTable parses a CREATE VIRTUAL TABLE statement.
// Syntax: CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module[(args)]
func (p *Parser) parseCreateVirtualTable() (*CreateVirtualTableStmt, error) {
	stmt := &CreateVirtualTableStmt{}

	if err := p.parseIfNotExists(&stmt.IfNotExists); err != nil {
		return nil, err
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	if !p.match(TK_USING) {
		return nil, p.error("expected USING after virtual table name")
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected module name after USING")
	}
	stmt.Module = Unquote(p.advance().Lexeme)

	if p.match(TK_LP) {
		args, err := p.parseModuleArgs()
		if err != nil {
			return nil, err
		}
		stmt.Args = args
	}

	return stmt, nil
}

// parseModuleArgs parses comma-separated module arguments within parentheses.
// The opening paren has already been consumed.
func (p *Parser) parseModuleArgs() ([]string, error) {
	var args []string
	for {
		if p.check(TK_RP) {
			break
		}
		tok := p.peek()
		if tok.Type == TK_ID || tok.Type == TK_STRING {
			args = append(args, Unquote(p.advance().Lexeme))
		} else {
			return nil, p.error("expected module argument")
		}
		if !p.match(TK_COMMA) {
			break
		}
	}
	if !p.match(TK_RP) {
		return nil, p.error("expected ) after module arguments")
	}
	return args, nil
}

// parseIfNotExists parses the optional IF NOT EXISTS clause and sets the flag.
func (p *Parser) parseIfNotExists(flag *bool) error {
	if !p.match(TK_IF) {
		return nil
	}
	if !p.match(TK_NOT) || !p.match(TK_EXISTS) {
		return p.error("expected NOT EXISTS after IF")
	}
	*flag = true
	return nil
}

// parseCreateTableAsSelect handles CREATE TABLE ... AS SELECT ....
func (p *Parser) parseCreateTableAsSelect(stmt *CreateTableStmt) (*CreateTableStmt, error) {
	if !p.match(TK_SELECT) {
		return nil, p.error("expected SELECT after AS")
	}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.Select = sel
	return stmt, nil
}

// parseCreateTableBody parses the parenthesised column-definition and
// table-constraint list.
func (p *Parser) parseCreateTableBody(stmt *CreateTableStmt) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after table name")
	}
	for {
		if err := p.parseColumnOrConstraint(stmt); err != nil {
			return err
		}
		if !p.match(TK_COMMA) {
			break
		}
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after column definitions")
	}
	return nil
}

// tableConstraintKeywords contains tokens that start a table constraint.
var tableConstraintKeywords = map[TokenType]bool{
	TK_CONSTRAINT: true,
	TK_PRIMARY:    true,
	TK_UNIQUE:     true,
	TK_CHECK:      true,
	TK_FOREIGN:    true,
}

// parseColumnOrConstraint attempts to parse one column definition; on failure
// it falls back to a table constraint.
func (p *Parser) parseColumnOrConstraint(stmt *CreateTableStmt) error {
	// Check if current token is a table constraint keyword
	// If so, parse it as a constraint directly to avoid misleading error messages
	if p.isTableConstraintKeyword() {
		return p.parseAndAppendTableConstraint(stmt)
	}

	// Try to parse as a column definition
	col, colErr := p.parseColumnDef()
	if colErr == nil {
		stmt.Columns = append(stmt.Columns, *col)
		return nil
	}

	// Fall back to table constraint
	constraint, err := p.parseTableConstraint()
	if err != nil {
		return colErr // return the original column-parse error
	}
	stmt.Constraints = append(stmt.Constraints, *constraint)
	return nil
}

// isTableConstraintKeyword checks if current token starts a table constraint.
func (p *Parser) isTableConstraintKeyword() bool {
	return tableConstraintKeywords[p.peek().Type]
}

// parseAndAppendTableConstraint parses a table constraint and appends it to stmt.
func (p *Parser) parseAndAppendTableConstraint(stmt *CreateTableStmt) error {
	constraint, err := p.parseTableConstraint()
	if err != nil {
		return err
	}
	stmt.Constraints = append(stmt.Constraints, *constraint)
	return nil
}

// parseTableOptions parses the trailing WITHOUT ROWID / STRICT options.
func (p *Parser) parseTableOptions(stmt *CreateTableStmt) error {
	for {
		switch {
		case p.match(TK_WITHOUT):
			if !p.match(TK_ROWID) {
				return p.error("expected ROWID after WITHOUT")
			}
			stmt.WithoutRowID = true
		case p.match(TK_STRICT):
			stmt.Strict = true
		default:
			return nil
		}
		p.match(TK_COMMA)
	}
}

func (p *Parser) parseColumnDef() (*ColumnDef, error) {
	if !p.checkIdentifier() {
		return nil, p.error("expected column name")
	}
	col := &ColumnDef{Name: Unquote(p.advance().Lexeme)}
	col.Type = p.parseOptionalTypeName()
	return p.parseColumnConstraints(col)
}

// typeNameTokens are the tokens that can start a type name.
var typeNameTokens = map[TokenType]bool{
	TK_ID: true, TK_INTEGER_TYPE: true, TK_TEXT: true,
	TK_REAL: true, TK_BLOB_TYPE: true, TK_NUMERIC: true,
}

// parseOptionalTypeName parses an optional type name.
func (p *Parser) parseOptionalTypeName() string {
	if typeNameTokens[p.peek().Type] {
		return p.parseTypeName()
	}
	return ""
}

// parseColumnConstraints parses all constraints on a column.
func (p *Parser) parseColumnConstraints(col *ColumnDef) (*ColumnDef, error) {
	for p.isColumnConstraint() {
		constraint, err := p.parseColumnConstraint()
		if err != nil {
			return nil, err
		}
		col.Constraints = append(col.Constraints, *constraint)
	}
	return col, nil
}

func (p *Parser) parseTypeName() string {
	parts := make([]string, 0)
	parts = append(parts, p.advance().Lexeme)

	// Handle type modifiers like INTEGER(10) or NUMERIC(10, 2)
	if p.match(TK_LP) {
		parts = append(parts, "(")
		parts = append(parts, p.advance().Lexeme)
		if p.match(TK_COMMA) {
			parts = append(parts, ",")
			parts = append(parts, p.advance().Lexeme)
		}
		if p.match(TK_RP) {
			parts = append(parts, ")")
		}
	}

	return strings.Join(parts, "")
}

// columnConstraintHandler is a function that fills in the constraint details
// for one specific constraint keyword. The keyword has already been consumed.
type columnConstraintHandler func(p *Parser, c *ColumnConstraint) error

// columnConstraintHandlers maps each leading keyword to its handler.
// Order matters when iterating, but map lookup is used for dispatch, so the
// handlers are individually keyed. The one two-token prefix (PRIMARY KEY) is
// handled by the PRIMARY handler itself.
var columnConstraintHandlers = map[TokenType]columnConstraintHandler{
	TK_PRIMARY:    (*Parser).applyConstraintPrimaryKey,
	TK_NOT:        (*Parser).applyConstraintNotNull,
	TK_UNIQUE:     (*Parser).applyConstraintUnique,
	TK_CHECK:      (*Parser).applyConstraintCheck,
	TK_DEFAULT:    (*Parser).applyConstraintDefault,
	TK_COLLATE:    (*Parser).applyConstraintCollate,
	TK_REFERENCES: (*Parser).applyConstraintReferences,
	TK_GENERATED:  (*Parser).applyConstraintGenerated,
	TK_AS:         (*Parser).applyConstraintGenerated,
}

// columnConstraintOrder defines the token-check order so the dispatch is
// deterministic (maps have no guaranteed iteration order in Go).
var columnConstraintOrder = []TokenType{
	TK_PRIMARY, TK_NOT, TK_UNIQUE, TK_CHECK, TK_DEFAULT, TK_COLLATE,
	TK_REFERENCES, TK_GENERATED, TK_AS,
}

func (p *Parser) parseColumnConstraint() (*ColumnConstraint, error) {
	constraint := &ColumnConstraint{}

	if p.match(TK_CONSTRAINT) {
		if !p.check(TK_ID) {
			return nil, p.error("expected constraint name")
		}
		constraint.Name = Unquote(p.advance().Lexeme)
	}

	for _, tok := range columnConstraintOrder {
		if !p.match(tok) {
			continue
		}
		handler := columnConstraintHandlers[tok]
		if err := handler(p, constraint); err != nil {
			return nil, err
		}
		return constraint, nil
	}

	return nil, p.error("expected column constraint")
}

// applyConstraintPrimaryKey handles PRIMARY KEY [ASC|DESC] [AUTOINCREMENT].
func (p *Parser) applyConstraintPrimaryKey(c *ColumnConstraint) error {
	if !p.match(TK_KEY) {
		return p.error("expected KEY after PRIMARY")
	}
	c.Type = ConstraintPrimaryKey
	c.PrimaryKey = &PrimaryKeyConstraint{}
	if p.match(TK_ASC) {
		c.PrimaryKey.Order = SortAsc
	} else if p.match(TK_DESC) {
		c.PrimaryKey.Order = SortDesc
	}
	if p.match(TK_AUTOINCREMENT) {
		c.PrimaryKey.Autoincrement = true
	}
	return nil
}

// applyConstraintNotNull handles NOT NULL.
func (p *Parser) applyConstraintNotNull(c *ColumnConstraint) error {
	if !p.match(TK_NULL) {
		return p.error("expected NULL after NOT")
	}
	c.Type = ConstraintNotNull
	c.NotNull = true
	return nil
}

// applyConstraintUnique handles UNIQUE.
func (p *Parser) applyConstraintUnique(c *ColumnConstraint) error {
	c.Type = ConstraintUnique
	c.Unique = true
	return nil
}

// applyConstraintCheck handles CHECK (expr).
func (p *Parser) applyConstraintCheck(c *ColumnConstraint) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after CHECK")
	}
	expr, err := p.parseExpression()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after CHECK expression")
	}
	c.Type = ConstraintCheck
	c.Check = expr
	return nil
}

// applyConstraintDefault handles DEFAULT <expr>.
// This supports literals, negative numbers, and other expressions.
func (p *Parser) applyConstraintDefault(c *ColumnConstraint) error {
	expr, err := p.parseUnaryExpression()
	if err != nil {
		return err
	}
	c.Type = ConstraintDefault
	c.Default = expr
	return nil
}

// applyConstraintCollate handles COLLATE <name>.
func (p *Parser) applyConstraintCollate(c *ColumnConstraint) error {
	if !p.check(TK_ID) {
		return p.error("expected collation name")
	}
	c.Type = ConstraintCollate
	c.Collate = Unquote(p.advance().Lexeme)
	return nil
}

// applyConstraintReferences handles REFERENCES table [(column)] [ON DELETE/UPDATE action] [MATCH name] [DEFERRABLE].
func (p *Parser) applyConstraintReferences(c *ColumnConstraint) error {
	c.Type = ConstraintForeignKey
	if !p.check(TK_ID) {
		return p.error("expected table name after REFERENCES")
	}
	tableName := Unquote(p.advance().Lexeme)

	// Initialize ForeignKey struct
	c.ForeignKey = &ForeignKeyConstraint{
		Table: tableName,
	}

	// Optional column list
	if p.match(TK_LP) {
		if !p.check(TK_ID) {
			return p.error("expected column name")
		}
		c.ForeignKey.Columns = []string{Unquote(p.advance().Lexeme)}
		if !p.match(TK_RP) {
			return p.error("expected ')'")
		}
	}

	// Parse optional ON DELETE/UPDATE, MATCH, DEFERRABLE clauses
	return p.parseForeignKeyActions(c.ForeignKey)
}

// parseFKOnClause parses ON DELETE/UPDATE action
func (p *Parser) parseFKOnClause(fk *ForeignKeyConstraint) error {
	if p.match(TK_DELETE) {
		action, err := p.parseForeignKeyAction()
		if err != nil {
			return err
		}
		fk.OnDelete = action
		return nil
	}
	if p.match(TK_UPDATE) {
		action, err := p.parseForeignKeyAction()
		if err != nil {
			return err
		}
		fk.OnUpdate = action
		return nil
	}
	return p.error("expected DELETE or UPDATE after ON")
}

// parseFKDeferrable parses DEFERRABLE INITIALLY DEFERRED/IMMEDIATE
func (p *Parser) parseFKDeferrable(fk *ForeignKeyConstraint) error {
	if !p.match(TK_INITIALLY) {
		fk.Deferrable = DeferrableInitiallyImmediate
		return nil
	}
	if p.match(TK_DEFERRED) {
		fk.Deferrable = DeferrableInitiallyDeferred
		return nil
	}
	if p.match(TK_IMMEDIATE) {
		fk.Deferrable = DeferrableInitiallyImmediate
		return nil
	}
	return p.error("expected DEFERRED or IMMEDIATE after INITIALLY")
}

// parseFKMatchClause parses the MATCH clause of a foreign key.
func (p *Parser) parseFKMatchClause(fk *ForeignKeyConstraint) error {
	if !p.check(TK_ID) {
		return p.error("expected match name")
	}
	fk.Match = Unquote(p.advance().Lexeme)
	return nil
}

// parseFKNotDeferrable parses the NOT DEFERRABLE clause.
func (p *Parser) parseFKNotDeferrable(fk *ForeignKeyConstraint) error {
	if !p.match(TK_DEFERRABLE) {
		return p.error("expected DEFERRABLE after NOT")
	}
	fk.Deferrable = DeferrableNone
	return nil
}

// parseFKActionStep tries to match and parse one FK action clause. Returns false if no match.
func (p *Parser) parseFKActionStep(fk *ForeignKeyConstraint) (bool, error) {
	switch {
	case p.match(TK_ON):
		return true, p.parseFKOnClause(fk)
	case p.match(TK_MATCH):
		return true, p.parseFKMatchClause(fk)
	case p.match(TK_NOT):
		return true, p.parseFKNotDeferrable(fk)
	case p.match(TK_DEFERRABLE):
		return true, p.parseFKDeferrable(fk)
	default:
		return false, nil
	}
}

// parseForeignKeyActions parses ON DELETE/UPDATE, MATCH, and DEFERRABLE clauses.
func (p *Parser) parseForeignKeyActions(fk *ForeignKeyConstraint) error {
	for {
		matched, err := p.parseFKActionStep(fk)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
	}
}

// parseForeignKeyAction parses an FK action (CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION).
func (p *Parser) parseForeignKeyAction() (ForeignKeyAction, error) {
	if p.match(TK_CASCADE) {
		return FKActionCascade, nil
	}
	if p.match(TK_RESTRICT) {
		return FKActionRestrict, nil
	}
	if p.match(TK_SET) {
		if p.match(TK_NULL) {
			return FKActionSetNull, nil
		}
		if p.match(TK_DEFAULT) {
			return FKActionSetDefault, nil
		}
		return FKActionNone, p.error("expected NULL or DEFAULT after SET")
	}
	if p.match(TK_NO) {
		if !p.match(TK_ACTION) {
			return FKActionNone, p.error("expected ACTION after NO")
		}
		return FKActionNoAction, nil
	}
	return FKActionNone, p.error("expected foreign key action (CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION)")
}

// applyConstraintGenerated handles GENERATED ALWAYS AS (expr) or AS (expr).
func (p *Parser) applyConstraintGenerated(c *ColumnConstraint) error {
	c.Type = ConstraintGenerated

	// Skip ALWAYS if present
	p.match(TK_ALWAYS)

	// Expect AS
	if !p.match(TK_AS) {
		return p.error("expected AS in generated column")
	}

	// Expect (expr)
	if !p.match(TK_LP) {
		return p.error("expected '(' after AS")
	}

	expr, err := p.parseExpression()
	if err != nil {
		return err
	}

	// Initialize Generated struct
	c.Generated = &GeneratedConstraint{
		Expr: expr,
	}

	if !p.match(TK_RP) {
		return p.error("expected ')'")
	}

	// Optional STORED or VIRTUAL
	if p.match(TK_STORED) {
		c.Generated.Stored = true
	} else {
		p.match(TK_VIRTUAL) // VIRTUAL is default, just consume if present
		c.Generated.Virtual = true
	}

	return nil
}

// tableConstraintHandler fills in a TableConstraint once its leading keyword
// has been consumed. Mirrors the columnConstraintHandler pattern.
type tableConstraintHandler func(p *Parser, c *TableConstraint) error

// tableConstraintHandlers maps each leading keyword to its handler.
var tableConstraintHandlers = map[TokenType]tableConstraintHandler{
	TK_PRIMARY: (*Parser).applyTableConstraintPrimaryKey,
	TK_UNIQUE:  (*Parser).applyTableConstraintUnique,
	TK_CHECK:   (*Parser).applyTableConstraintCheck,
	TK_FOREIGN: (*Parser).applyTableConstraintForeignKey,
}

// tableConstraintOrder determines the order in which keywords are tried.
var tableConstraintOrder = []TokenType{TK_PRIMARY, TK_UNIQUE, TK_CHECK, TK_FOREIGN}

func (p *Parser) parseTableConstraint() (*TableConstraint, error) {
	constraint := &TableConstraint{}

	if err := p.parseTableConstraintName(constraint); err != nil {
		return nil, err
	}

	for _, tok := range tableConstraintOrder {
		if !p.match(tok) {
			continue
		}
		if err := tableConstraintHandlers[tok](p, constraint); err != nil {
			return nil, err
		}
		return constraint, nil
	}

	return nil, p.error("expected table constraint")
}

// parseTableConstraintName parses the optional CONSTRAINT <name> prefix.
func (p *Parser) parseTableConstraintName(c *TableConstraint) error {
	if !p.match(TK_CONSTRAINT) {
		return nil
	}
	if !p.check(TK_ID) {
		return p.error("expected constraint name")
	}
	c.Name = Unquote(p.advance().Lexeme)
	return nil
}

// applyTableConstraintPrimaryKey handles PRIMARY KEY (columns...).
func (p *Parser) applyTableConstraintPrimaryKey(c *TableConstraint) error {
	if !p.match(TK_KEY) {
		return p.error("expected KEY after PRIMARY")
	}
	if !p.match(TK_LP) {
		return p.error("expected ( after PRIMARY KEY")
	}
	cols, err := p.parseIndexedColumns()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after PRIMARY KEY columns")
	}
	c.Type = ConstraintPrimaryKey
	c.PrimaryKey = &PrimaryKeyTableConstraint{Columns: cols}
	return nil
}

// applyTableConstraintUnique handles UNIQUE (columns...).
func (p *Parser) applyTableConstraintUnique(c *TableConstraint) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after UNIQUE")
	}
	cols, err := p.parseIndexedColumns()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after UNIQUE columns")
	}
	c.Type = ConstraintUnique
	c.Unique = &UniqueTableConstraint{Columns: cols}
	return nil
}

// applyTableConstraintCheck handles CHECK (expr).
func (p *Parser) applyTableConstraintCheck(c *TableConstraint) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after CHECK")
	}
	expr, err := p.parseExpression()
	if err != nil {
		return err
	}
	if !p.match(TK_RP) {
		return p.error("expected ) after CHECK expression")
	}
	c.Type = ConstraintCheck
	c.Check = expr
	return nil
}

// applyTableConstraintForeignKey handles FOREIGN KEY (columns...) REFERENCES table(columns...).
func (p *Parser) applyTableConstraintForeignKey(c *TableConstraint) error {
	// Expect KEY after FOREIGN
	if !p.match(TK_KEY) {
		return p.error("expected KEY after FOREIGN")
	}

	// Parse column list: (col1, col2, ...)
	columns, err := p.parseForeignKeyColumns()
	if err != nil {
		return err
	}

	// Parse REFERENCES clause
	refTable, refColumns, err := p.parseForeignKeyReferences()
	if err != nil {
		return err
	}

	// Create the foreign key constraint
	fk := &ForeignKeyConstraint{
		Table:   refTable,
		Columns: refColumns,
	}

	// Parse ON DELETE/UPDATE actions, MATCH, DEFERRABLE
	if err := p.parseForeignKeyActions(fk); err != nil {
		return err
	}

	c.Type = ConstraintForeignKey
	c.ForeignKey = &ForeignKeyTableConstraint{
		Columns:    columns,
		ForeignKey: *fk,
	}

	return nil
}

// parseForeignKeyColumns parses the column list in FOREIGN KEY (col1, col2, ...).
func (p *Parser) parseForeignKeyColumns() ([]string, error) {
	if !p.match(TK_LP) {
		return nil, p.error("expected ( after FOREIGN KEY")
	}

	var columns []string
	for {
		if !p.check(TK_ID) {
			return nil, p.error("expected column name")
		}
		columns = append(columns, Unquote(p.advance().Lexeme))

		if !p.match(TK_COMMA) {
			break
		}
	}

	if !p.match(TK_RP) {
		return nil, p.error("expected ) after FOREIGN KEY columns")
	}

	return columns, nil
}

// parseForeignKeyReferences parses the REFERENCES table (columns) clause.
func (p *Parser) parseForeignKeyReferences() (string, []string, error) {
	if !p.match(TK_REFERENCES) {
		return "", nil, p.error("expected REFERENCES after FOREIGN KEY columns")
	}

	if !p.check(TK_ID) {
		return "", nil, p.error("expected table name after REFERENCES")
	}
	refTable := Unquote(p.advance().Lexeme)

	refColumns, err := p.parseForeignKeyRefColumns()
	if err != nil {
		return "", nil, err
	}

	return refTable, refColumns, nil
}

func (p *Parser) parseForeignKeyRefColumns() ([]string, error) {
	if !p.match(TK_LP) {
		return nil, nil
	}

	var refColumns []string
	for {
		if !p.check(TK_ID) {
			return nil, p.error("expected column name")
		}
		refColumns = append(refColumns, Unquote(p.advance().Lexeme))

		if !p.match(TK_COMMA) {
			break
		}
	}

	if !p.match(TK_RP) {
		return nil, p.error("expected ) after referenced columns")
	}

	return refColumns, nil
}

func (p *Parser) parseCreateIndex(unique bool) (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{Unique: unique}

	// Also check for UNIQUE here for backwards compatibility (if UNIQUE comes after CREATE INDEX)
	if p.match(TK_UNIQUE) {
		stmt.Unique = true
	}

	if err := p.parseIndexIfNotExists(stmt); err != nil {
		return nil, err
	}
	if err := p.parseIndexNameAndTable(stmt); err != nil {
		return nil, err
	}
	if err := p.parseIndexColumns(stmt); err != nil {
		return nil, err
	}
	if err := p.parseIndexWhereClause(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseIndexIfNotExists(stmt *CreateIndexStmt) error {
	if !p.match(TK_IF) {
		return nil
	}
	if !p.match(TK_NOT) || !p.match(TK_EXISTS) {
		return p.error("expected NOT EXISTS after IF")
	}
	stmt.IfNotExists = true
	return nil
}

func (p *Parser) parseIndexNameAndTable(stmt *CreateIndexStmt) error {
	if !p.check(TK_ID) {
		return p.error("expected index name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)
	if !p.match(TK_ON) {
		return p.error("expected ON after index name")
	}
	if !p.check(TK_ID) {
		return p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)
	return nil
}

func (p *Parser) parseIndexColumns(stmt *CreateIndexStmt) error {
	if !p.match(TK_LP) {
		return p.error("expected ( after table name")
	}
	cols, err := p.parseIndexedColumns()
	if err != nil {
		return err
	}
	stmt.Columns = cols
	if !p.match(TK_RP) {
		return p.error("expected ) after columns")
	}
	return nil
}

func (p *Parser) parseIndexWhereClause(stmt *CreateIndexStmt) error {
	if !p.match(TK_WHERE) {
		return nil
	}
	where, err := p.parseExpression()
	if err != nil {
		return err
	}
	stmt.Where = where
	return nil
}

func (p *Parser) parseIndexedColumns() ([]IndexedColumn, error) {
	columns := make([]IndexedColumn, 0)

	for {
		// Parse the expression (which could be a simple column name or complex expression)
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		col := IndexedColumn{
			Expr: expr,
		}

		// Extract column name from expression for backwards compatibility
		col.Column = extractExpressionName(expr)

		// Parse optional ASC/DESC
		if p.match(TK_ASC) {
			col.Order = SortAsc
		} else if p.match(TK_DESC) {
			col.Order = SortDesc
		}

		columns = append(columns, col)

		if !p.match(TK_COMMA) {
			break
		}
	}

	return columns, nil
}

// extractExpressionName extracts a string representation from an expression.
// For simple identifiers, it returns the column name.
// For complex expressions, it returns the expression's string representation.
func extractExpressionName(expr Expression) string {
	if expr == nil {
		return ""
	}

	// For simple column references, return just the column name
	if ident, ok := expr.(*IdentExpr); ok {
		if ident.Table != "" {
			return ident.Table + "." + ident.Name
		}
		return ident.Name
	}

	// For any other expression, return its string representation
	return expr.String()
}

// =============================================================================
// CREATE VIEW
// =============================================================================

// parseCreateView parses a CREATE VIEW statement.
// Syntax:
//
//	CREATE [TEMP|TEMPORARY] VIEW [IF NOT EXISTS] view_name [(column_list)] AS select_stmt
func (p *Parser) parseCreateView(temp bool) (*CreateViewStmt, error) {
	stmt := &CreateViewStmt{Temporary: temp}

	// Parse IF NOT EXISTS
	if err := p.parseIfNotExists(&stmt.IfNotExists); err != nil {
		return nil, err
	}

	// Parse view name
	if !p.check(TK_ID) {
		return nil, p.error("expected view name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	// Parse optional column list: (col1, col2, ...)
	if err := p.parseViewColumns(stmt); err != nil {
		return nil, err
	}

	// Parse AS SELECT ...
	if err := p.parseViewSelect(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseViewColumns parses the optional column list in a CREATE VIEW statement.
func (p *Parser) parseViewColumns(stmt *CreateViewStmt) error {
	if p.match(TK_LP) {
		for {
			if !p.check(TK_ID) {
				return p.error("expected column name")
			}
			stmt.Columns = append(stmt.Columns, Unquote(p.advance().Lexeme))
			if !p.match(TK_COMMA) {
				break
			}
		}
		if !p.match(TK_RP) {
			return p.error("expected ) after column list")
		}
	}
	return nil
}

// parseViewSelect parses the AS SELECT clause of a CREATE VIEW statement.
func (p *Parser) parseViewSelect(stmt *CreateViewStmt) error {
	if !p.match(TK_AS) {
		return p.error("expected AS after view name")
	}

	if !p.match(TK_SELECT) {
		return p.error("expected SELECT after AS")
	}

	sel, err := p.parseSelectBody(&SelectStmt{})
	if err != nil {
		return err
	}
	stmt.Select = sel

	return nil
}

// =============================================================================
// DROP
// =============================================================================

func (p *Parser) parseDrop() (Statement, error) {
	if p.match(TK_TABLE) {
		return p.parseDropTable()
	} else if p.match(TK_INDEX) {
		return p.parseDropIndex()
	} else if p.match(TK_VIEW) {
		return p.parseDropView()
	} else if p.match(TK_TRIGGER) {
		return p.parseDropTrigger()
	} else {
		return nil, p.error("expected TABLE, INDEX, VIEW, or TRIGGER after DROP")
	}
}

func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}

	if p.match(TK_IF) {
		if !p.match(TK_EXISTS) {
			return nil, p.error("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected table name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	return stmt, nil
}

func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	stmt := &DropIndexStmt{}

	if p.match(TK_IF) {
		if !p.match(TK_EXISTS) {
			return nil, p.error("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected index name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	return stmt, nil
}

// parseDropView parses a DROP VIEW statement.
// Syntax: DROP VIEW [IF EXISTS] view_name
func (p *Parser) parseDropView() (*DropViewStmt, error) {
	stmt := &DropViewStmt{}

	if p.match(TK_IF) {
		if !p.match(TK_EXISTS) {
			return nil, p.error("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	if !p.check(TK_ID) {
		return nil, p.error("expected view name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	return stmt, nil
}

// =============================================================================
// TRIGGER
// =============================================================================

// parseCreateTrigger parses a CREATE TRIGGER statement.
// Syntax: CREATE [TEMP] TRIGGER [IF NOT EXISTS] name
//
//	{BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE|DELETE} ON table
//	[FOR EACH ROW] [WHEN expr]
//	BEGIN statements END
func (p *Parser) parseCreateTrigger(temp bool) (*CreateTriggerStmt, error) {
	stmt := &CreateTriggerStmt{Temp: temp}

	// Parse trigger header: IF NOT EXISTS and name
	if err := p.parseTriggerHeader(stmt); err != nil {
		return nil, err
	}

	// Parse trigger specification: timing, event, table
	if err := p.parseTriggerSpec(stmt); err != nil {
		return nil, err
	}

	// Parse optional clauses and body
	if err := p.parseTriggerOptionalAndBody(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseTriggerHeader parses IF NOT EXISTS and the trigger name.
func (p *Parser) parseTriggerHeader(stmt *CreateTriggerStmt) error {
	if err := p.parseIfNotExists(&stmt.IfNotExists); err != nil {
		return err
	}
	if !p.check(TK_ID) {
		return p.error("expected trigger name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)
	return nil
}

// parseTriggerSpec parses timing, event, and table clauses.
func (p *Parser) parseTriggerSpec(stmt *CreateTriggerStmt) error {
	if err := p.parseTriggerTiming(stmt); err != nil {
		return err
	}
	if err := p.parseTriggerEvent(stmt); err != nil {
		return err
	}
	return p.parseTriggerTable(stmt)
}

// parseTriggerOptionalAndBody parses FOR EACH ROW, WHEN, and BEGIN...END.
func (p *Parser) parseTriggerOptionalAndBody(stmt *CreateTriggerStmt) error {
	if err := p.parseTriggerForEachRow(stmt); err != nil {
		return err
	}
	if err := p.parseTriggerWhen(stmt); err != nil {
		return err
	}
	return p.parseTriggerBody(stmt)
}

// parseTriggerTiming parses the trigger timing (BEFORE, AFTER, INSTEAD OF).
func (p *Parser) parseTriggerTiming(stmt *CreateTriggerStmt) error {
	if p.match(TK_BEFORE) {
		stmt.Timing = TriggerBefore
	} else if p.match(TK_AFTER) {
		stmt.Timing = TriggerAfter
	} else if p.match(TK_INSTEAD) {
		if !p.match(TK_OF) {
			return p.error("expected OF after INSTEAD")
		}
		stmt.Timing = TriggerInsteadOf
	} else {
		return p.error("expected BEFORE, AFTER, or INSTEAD OF")
	}
	return nil
}

// parseTriggerEvent parses the trigger event (INSERT, UPDATE, DELETE).
func (p *Parser) parseTriggerEvent(stmt *CreateTriggerStmt) error {
	if p.match(TK_INSERT) {
		stmt.Event = TriggerInsert
		return nil
	}
	if p.match(TK_DELETE) {
		stmt.Event = TriggerDelete
		return nil
	}
	if p.match(TK_UPDATE) {
		stmt.Event = TriggerUpdate
		return p.parseTriggerUpdateOf(stmt)
	}
	return p.error("expected INSERT, UPDATE, or DELETE")
}

// parseTriggerUpdateOf parses the optional UPDATE OF column list.
func (p *Parser) parseTriggerUpdateOf(stmt *CreateTriggerStmt) error {
	if !p.match(TK_OF) {
		return nil
	}
	for {
		if !p.check(TK_ID) {
			return p.error("expected column name after UPDATE OF")
		}
		stmt.UpdateOf = append(stmt.UpdateOf, Unquote(p.advance().Lexeme))
		if !p.match(TK_COMMA) {
			break
		}
	}
	return nil
}

// parseTriggerTable parses the ON table clause.
func (p *Parser) parseTriggerTable(stmt *CreateTriggerStmt) error {
	if !p.match(TK_ON) {
		return p.error("expected ON after trigger event")
	}
	if !p.check(TK_ID) {
		return p.error("expected table name")
	}
	stmt.Table = Unquote(p.advance().Lexeme)
	return nil
}

// parseTriggerForEachRow parses the optional FOR EACH ROW clause.
func (p *Parser) parseTriggerForEachRow(stmt *CreateTriggerStmt) error {
	if p.match(TK_FOR) {
		if !p.match(TK_EACH) {
			return p.error("expected EACH after FOR")
		}
		if !p.match(TK_ROW) {
			return p.error("expected ROW after EACH")
		}
		stmt.ForEachRow = true
	}
	return nil
}

// parseTriggerWhen parses the optional WHEN clause.
func (p *Parser) parseTriggerWhen(stmt *CreateTriggerStmt) error {
	if p.match(TK_WHEN) {
		when, err := p.parseExpression()
		if err != nil {
			return err
		}
		stmt.When = when
	}
	return nil
}

// parseTriggerBody parses the trigger body: BEGIN statements END.
func (p *Parser) parseTriggerBody(stmt *CreateTriggerStmt) error {
	if !p.match(TK_BEGIN) {
		return p.error("expected BEGIN")
	}

	// Parse trigger body statements
	// Note: Trigger bodies can only contain INSERT, UPDATE, DELETE, and SELECT statements
	for !p.check(TK_END) && !p.isAtEnd() {
		if p.match(TK_SEMI) {
			continue
		}
		triggerStmt, err := p.parseTriggerBodyStatement()
		if err != nil {
			return err
		}
		stmt.Body = append(stmt.Body, triggerStmt)
		p.match(TK_SEMI)
	}

	if !p.match(TK_END) {
		return p.error("expected END")
	}
	return nil
}

// parseTriggerBodyStatement parses a single statement in a trigger body.
func (p *Parser) parseTriggerBodyStatement() (Statement, error) {
	switch {
	case p.match(TK_SELECT):
		return p.parseSelect()
	case p.match(TK_INSERT):
		return p.parseInsert()
	case p.match(TK_UPDATE):
		return p.parseUpdate()
	case p.match(TK_DELETE):
		return p.parseDelete()
	default:
		return nil, p.error("trigger body can only contain SELECT, INSERT, UPDATE, or DELETE statements")
	}
}

// parseDropTrigger parses a DROP TRIGGER statement.
// Syntax: DROP TRIGGER [IF EXISTS] name
func (p *Parser) parseDropTrigger() (*DropTriggerStmt, error) {
	stmt := &DropTriggerStmt{}

	// IF EXISTS
	if p.match(TK_IF) {
		if !p.match(TK_EXISTS) {
			return nil, p.error("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Trigger name
	if !p.check(TK_ID) {
		return nil, p.error("expected trigger name")
	}
	stmt.Name = Unquote(p.advance().Lexeme)

	return stmt, nil
}

// =============================================================================
// EXPLAIN
// =============================================================================

// parseExplain parses an EXPLAIN or EXPLAIN QUERY PLAN statement.
// Syntax:
//
//	EXPLAIN statement
//	EXPLAIN QUERY PLAN statement
func (p *Parser) parseExplain() (*ExplainStmt, error) {
	stmt := &ExplainStmt{}

	// Check for QUERY PLAN
	if p.match(TK_QUERY) {
		if !p.match(TK_PLAN) {
			return nil, p.error("expected PLAN after QUERY")
		}
		stmt.QueryPlan = true
	}

	// Parse the statement being explained
	innerStmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	stmt.Statement = innerStmt

	return stmt, nil
}

// =============================================================================
// Transactions
// =============================================================================

func (p *Parser) parseBegin() (*BeginStmt, error) {
	stmt := &BeginStmt{Mode: TransactionDeferred}

	p.match(TK_TRANSACTION)

	if p.match(TK_DEFERRED) {
		stmt.Mode = TransactionDeferred
	} else if p.match(TK_IMMEDIATE) {
		stmt.Mode = TransactionImmediate
	} else if p.match(TK_EXCLUSIVE) {
		stmt.Mode = TransactionExclusive
	}

	return stmt, nil
}

func (p *Parser) parseRollback() (*RollbackStmt, error) {
	stmt := &RollbackStmt{}

	// ROLLBACK [TRANSACTION] [TO [SAVEPOINT] name]
	p.match(TK_TRANSACTION)

	if p.match(TK_TO) {
		p.match(TK_SAVEPOINT) // optional SAVEPOINT keyword
		if !p.checkIdentifier() {
			return nil, p.error("expected savepoint name after ROLLBACK TO")
		}
		tok := p.advance()
		stmt.Savepoint = Unquote(tok.Lexeme)
	}

	return stmt, nil
}

func (p *Parser) parseSavepoint() (*SavepointStmt, error) {
	if !p.checkIdentifier() {
		return nil, p.error("expected savepoint name after SAVEPOINT")
	}
	tok := p.advance()
	return &SavepointStmt{Name: Unquote(tok.Lexeme)}, nil
}

func (p *Parser) parseRelease() (*ReleaseStmt, error) {
	p.match(TK_SAVEPOINT) // optional SAVEPOINT keyword
	if !p.checkIdentifier() {
		return nil, p.error("expected savepoint name after RELEASE")
	}
	tok := p.advance()
	return &ReleaseStmt{Name: Unquote(tok.Lexeme)}, nil
}

// =============================================================================
// ATTACH / DETACH DATABASE
// =============================================================================

// parseAttach parses an ATTACH DATABASE statement.
// Syntax: ATTACH [DATABASE] filename AS schema_name
func (p *Parser) parseAttach() (*AttachStmt, error) {
	stmt := &AttachStmt{}

	// DATABASE keyword is optional
	p.match(TK_DATABASE)

	// Parse filename expression (usually a string literal)
	filename, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	stmt.Filename = filename

	// Expect AS keyword
	if !p.match(TK_AS) {
		return nil, p.error("expected AS after database filename")
	}

	// Parse schema name (can be identifier or keyword like "temp")
	if !p.isSchemaIdentifier() {
		return nil, p.error("expected schema name")
	}
	stmt.SchemaName = p.consumeSchemaIdentifier()

	return stmt, nil
}

// parseDetach parses a DETACH DATABASE statement.
// Syntax: DETACH [DATABASE] schema_name
func (p *Parser) parseDetach() (*DetachStmt, error) {
	stmt := &DetachStmt{}

	// DATABASE keyword is optional
	p.match(TK_DATABASE)

	// Parse schema name (can be identifier or keyword like "temp")
	if !p.isSchemaIdentifier() {
		return nil, p.error("expected schema name")
	}
	stmt.SchemaName = p.consumeSchemaIdentifier()

	return stmt, nil
}

// isSchemaIdentifier checks if the current token can be used as a schema name.
// This includes regular identifiers and certain keywords that are valid as schema names.
func (p *Parser) isSchemaIdentifier() bool {
	switch p.peek().Type {
	case TK_ID, TK_TEMP:
		return true
	default:
		return false
	}
}

// consumeSchemaIdentifier consumes and returns a schema identifier token.
func (p *Parser) consumeSchemaIdentifier() string {
	tok := p.advance()
	return Unquote(tok.Lexeme)
}

// =============================================================================
// PRAGMA
// =============================================================================

// parsePragma parses a PRAGMA statement.
// Syntax:
//
//	PRAGMA name
//	PRAGMA name = value
//	PRAGMA name(value)
//	PRAGMA schema.name
//	PRAGMA schema.name = value
//	PRAGMA schema.name(value)
func (p *Parser) parsePragma() (*PragmaStmt, error) {
	stmt := &PragmaStmt{}

	// Parse schema.name or just name
	if err := p.parsePragmaName(stmt); err != nil {
		return nil, err
	}

	// Security check
	if !security.IsSafePragma(stmt.Name) {
		return nil, p.error("PRAGMA '%s' is not allowed for security reasons", stmt.Name)
	}

	// Parse optional value assignment
	if err := p.parsePragmaAssignment(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parsePragmaName parses the pragma name, which can be "name" or "schema.name".
func (p *Parser) parsePragmaName(stmt *PragmaStmt) error {
	if !p.isPragmaIdentifier() {
		return p.error("expected pragma name")
	}

	firstID := p.consumePragmaIdentifier()

	// Check if this is schema.name syntax
	if p.match(TK_DOT) {
		stmt.Schema = firstID
		if !p.isPragmaIdentifier() {
			return p.error("expected pragma name after schema")
		}
		stmt.Name = p.consumePragmaIdentifier()
	} else {
		stmt.Name = firstID
	}

	return nil
}

// parsePragmaAssignment parses the optional PRAGMA value assignment (= or function call syntax).
func (p *Parser) parsePragmaAssignment(stmt *PragmaStmt) error {
	if p.match(TK_EQ) {
		return p.parsePragmaEqValue(stmt)
	}
	if p.match(TK_LP) {
		return p.parsePragmaParenValue(stmt)
	}
	return nil
}

// parsePragmaEqValue parses "PRAGMA name = value" syntax.
func (p *Parser) parsePragmaEqValue(stmt *PragmaStmt) error {
	value, err := p.parsePragmaValue()
	if err != nil {
		return err
	}
	stmt.Value = value
	return nil
}

// parsePragmaParenValue parses "PRAGMA name(value)" syntax.
func (p *Parser) parsePragmaParenValue(stmt *PragmaStmt) error {
	value, err := p.parsePragmaValue()
	if err != nil {
		return err
	}
	stmt.Value = value
	if !p.match(TK_RP) {
		return p.error("expected ) after pragma value")
	}
	return nil
}

// isPragmaIdentifier checks if the current token can be used as a pragma/schema name.
// This includes regular identifiers and certain keywords that are valid as schema names.
func (p *Parser) isPragmaIdentifier() bool {
	switch p.peek().Type {
	case TK_ID, TK_TEMP:
		return true
	default:
		return false
	}
}

// consumePragmaIdentifier consumes and returns a pragma identifier token.
func (p *Parser) consumePragmaIdentifier() string {
	tok := p.advance()
	return Unquote(tok.Lexeme)
}

// parsePragmaValue parses a PRAGMA value, which can be a literal, number, or keyword.
// This is more permissive than parseUnaryExpression because it allows keywords like ON, OFF, WAL, etc.
func (p *Parser) parsePragmaValue() (Expression, error) {
	// Try numeric literals first (including negative numbers)
	if p.match(TK_MINUS) {
		expr, err := p.parsePragmaValue()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: OpNeg, Expr: expr}, nil
	}

	// Try literal values
	if expr := p.tryParseLiteral(); expr != nil {
		return expr, nil
	}

	// Try identifier or keyword - for PRAGMA values, we accept most keywords as identifiers
	// This handles cases like: PRAGMA foreign_keys = ON, PRAGMA journal_mode = WAL
	if p.isPragmaValueIdentifier() {
		tok := p.advance()
		return &IdentExpr{Name: Unquote(tok.Lexeme)}, nil
	}

	return nil, p.error("expected pragma value")
}

// isPragmaValueIdentifier checks if the current token can be used as a PRAGMA value.
// This is more permissive than isExpressionIdentifier because PRAGMA accepts keywords.
func (p *Parser) isPragmaValueIdentifier() bool {
	switch p.peek().Type {
	// Regular identifiers
	case TK_ID:
		return true
	// Common keywords that can be PRAGMA values
	case TK_ON, TK_DELETE, TK_TEMP, TK_TEMPORARY, TK_DEFAULT:
		return true
	default:
		return false
	}
}

// =============================================================================
// Expressions
// =============================================================================

// enterExpr increments expression depth and checks for overflow.
func (p *Parser) enterExpr() error {
	p.exprDepth++
	if p.exprDepth > security.MaxExprDepth {
		return fmt.Errorf("expression depth exceeds maximum of %d (possible stack overflow attack)", security.MaxExprDepth)
	}
	return nil
}

// exitExpr decrements expression depth.
func (p *Parser) exitExpr() {
	p.exprDepth--
}

func (p *Parser) parseExpression() (Expression, error) {
	if err := p.enterExpr(); err != nil {
		return nil, err
	}
	defer p.exitExpr()
	return p.parseOrExpression()
}

// ParseExpression parses a single SQL expression.
// This is an exported version for external packages that need to parse expressions.
func (p *Parser) ParseExpression() (Expression, error) {
	p.tokenize()
	return p.parseExpression()
}

func (p *Parser) parseOrExpression() (Expression, error) {
	if err := p.enterExpr(); err != nil {
		return nil, err
	}
	defer p.exitExpr()

	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.match(TK_OR) {
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpOr,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) parseAndExpression() (Expression, error) {
	if err := p.enterExpr(); err != nil {
		return nil, err
	}
	defer p.exitExpr()

	left, err := p.parseNotExpression()
	if err != nil {
		return nil, err
	}

	for p.match(TK_AND) {
		right, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    OpAnd,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) parseNotExpression() (Expression, error) {
	if err := p.enterExpr(); err != nil {
		return nil, err
	}
	defer p.exitExpr()

	if p.match(TK_NOT) {
		expr, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{
			Op:   OpNot,
			Expr: expr,
		}, nil
	}

	return p.parseComparisonExpression()
}

// comparisonOpMap maps token types to their binary operators.
var comparisonOpMap = map[TokenType]BinaryOp{
	TK_EQ: OpEq,
	TK_NE: OpNe,
	TK_LT: OpLt,
	TK_LE: OpLe,
	TK_GT: OpGt,
	TK_GE: OpGe,
}

// patternOpMap maps pattern matching token types to their binary operators.
var patternOpMap = map[TokenType]BinaryOp{
	TK_LIKE:   OpLike,
	TK_GLOB:   OpGlob,
	TK_REGEXP: OpRegexp,
	TK_MATCH:  OpMatch,
}

// checkNotWithAhead checks if we have NOT followed by the given token.
func (p *Parser) checkNotWithAhead(tok TokenType) bool {
	return p.check(TK_NOT) && p.peekAhead(1).Type == tok
}

func (p *Parser) parseComparisonExpression() (Expression, error) {
	left, err := p.parseBitwiseExpression()
	if err != nil {
		return nil, err
	}

	// IS NULL, IS NOT NULL
	if p.match(TK_IS) {
		return p.parseIsExpression(left)
	}

	// IN / NOT IN
	if p.check(TK_IN) || p.checkNotWithAhead(TK_IN) {
		return p.parseInExpression(left)
	}

	// BETWEEN / NOT BETWEEN
	if p.check(TK_BETWEEN) || p.checkNotWithAhead(TK_BETWEEN) {
		return p.parseBetweenExpression(left)
	}

	// NOT LIKE / NOT GLOB
	if p.checkNotWithAhead(TK_LIKE) || p.checkNotWithAhead(TK_GLOB) {
		return p.parseNotPatternOp(left)
	}

	// Pattern operators and comparison operators
	return p.tryParseOperators(left)
}

// tryParseOperators attempts to parse pattern or comparison operators.
func (p *Parser) tryParseOperators(left Expression) (Expression, error) {
	// LIKE, GLOB, REGEXP, MATCH
	if expr, err := p.tryParsePatternOp(left); expr != nil || err != nil {
		return expr, err
	}

	// Comparison operators
	if expr, err := p.tryParseComparisonOp(left); expr != nil || err != nil {
		return expr, err
	}

	return left, nil
}

// parseIsExpression parses IS NULL, IS NOT NULL, or IS comparison.
func (p *Parser) parseIsExpression(left Expression) (Expression, error) {
	if p.match(TK_NOT) {
		if p.match(TK_NULL) {
			return &UnaryExpr{Op: OpNotNull, Expr: left}, nil
		}
		right, err := p.parseBitwiseExpression()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: OpIsNot, Right: right}, nil
	}
	if p.match(TK_NULL) {
		return &UnaryExpr{Op: OpIsNull, Expr: left}, nil
	}
	// IS comparison (null-safe equality)
	right, err := p.parseBitwiseExpression()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: OpIs, Right: right}, nil
}

// parseInExpression parses IN or NOT IN expressions.
func (p *Parser) parseInExpression(left Expression) (Expression, error) {
	not := p.match(TK_NOT)
	p.match(TK_IN)

	if !p.match(TK_LP) {
		return nil, p.error("expected ( after IN")
	}

	inExpr := &InExpr{Expr: left, Not: not}

	if p.match(TK_SELECT) {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		inExpr.Select = sel
	} else {
		values, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		inExpr.Values = values
	}

	if !p.match(TK_RP) {
		return nil, p.error("expected ) after IN values")
	}

	return inExpr, nil
}

// parseBetweenExpression parses BETWEEN or NOT BETWEEN expressions.
func (p *Parser) parseBetweenExpression(left Expression) (Expression, error) {
	not := p.match(TK_NOT)
	p.match(TK_BETWEEN)

	lower, err := p.parseBitwiseExpression()
	if err != nil {
		return nil, err
	}

	if !p.match(TK_AND) {
		return nil, p.error("expected AND in BETWEEN")
	}

	upper, err := p.parseBitwiseExpression()
	if err != nil {
		return nil, err
	}

	return &BetweenExpr{Expr: left, Lower: lower, Upper: upper, Not: not}, nil
}

// parseNotPatternOp parses NOT LIKE or NOT GLOB expressions.
func (p *Parser) parseNotPatternOp(left Expression) (Expression, error) {
	p.match(TK_NOT) // consume NOT
	expr, err := p.tryParsePatternOp(left)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, p.error("expected LIKE or GLOB after NOT")
	}
	return &UnaryExpr{Op: OpNot, Expr: expr}, nil
}

// tryParsePatternOp tries to parse LIKE, GLOB, REGEXP, or MATCH.
func (p *Parser) tryParsePatternOp(left Expression) (Expression, error) {
	for tokType, op := range patternOpMap {
		if p.match(tokType) {
			right, err := p.parseBitwiseExpression()
			if err != nil {
				return nil, err
			}
			result := &BinaryExpr{Left: left, Op: op, Right: right}
			// Handle optional ESCAPE clause for LIKE
			if op == OpLike && p.match(TK_ESCAPE) {
				escExpr, escErr := p.parseBitwiseExpression()
				if escErr != nil {
					return nil, escErr
				}
				result.Escape = escExpr
			}
			return result, nil
		}
	}
	return nil, nil
}

// tryParseComparisonOp tries to parse comparison operators (=, <>, <, <=, >, >=).
func (p *Parser) tryParseComparisonOp(left Expression) (Expression, error) {
	for tokType, op := range comparisonOpMap {
		if p.match(tokType) {
			right, err := p.parseBitwiseExpression()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: op, Right: right}, nil
		}
	}
	return nil, nil
}

// bitwiseTokenOps maps token types to bitwise operations.
var bitwiseTokenOps = map[TokenType]BinaryOp{
	TK_BITAND: OpBitAnd,
	TK_BITOR:  OpBitOr,
	TK_LSHIFT: OpLShift,
	TK_RSHIFT: OpRShift,
}

func (p *Parser) parseBitwiseExpression() (Expression, error) {
	left, err := p.parseAdditiveExpression()
	if err != nil {
		return nil, err
	}

	for {
		op, matched := p.matchBitwiseOp()
		if !matched {
			break
		}
		right, err := p.parseAdditiveExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

// matchBitwiseOp tries to match a bitwise operator.
func (p *Parser) matchBitwiseOp() (BinaryOp, bool) {
	for tk, op := range bitwiseTokenOps {
		if p.match(tk) {
			return op, true
		}
	}
	return 0, false
}

// additiveTokenOps maps tokens to additive operators.
var additiveTokenOps = map[TokenType]BinaryOp{
	TK_PLUS:   OpPlus,
	TK_MINUS:  OpMinus,
	TK_CONCAT: OpConcat,
}

func (p *Parser) parseAdditiveExpression() (Expression, error) {
	left, err := p.parseMultiplicativeExpression()
	if err != nil {
		return nil, err
	}

	for {
		op, matched := p.matchAdditiveOp()
		if !matched {
			break
		}

		right, err := p.parseMultiplicativeExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) matchAdditiveOp() (BinaryOp, bool) {
	for tk, op := range additiveTokenOps {
		if p.match(tk) {
			return op, true
		}
	}
	return 0, false
}

// multiplicativeTokenOps maps tokens to multiplicative operators.
var multiplicativeTokenOps = map[TokenType]BinaryOp{
	TK_STAR:  OpMul,
	TK_SLASH: OpDiv,
	TK_REM:   OpRem,
}

func (p *Parser) parseMultiplicativeExpression() (Expression, error) {
	left, err := p.parseUnaryExpression()
	if err != nil {
		return nil, err
	}

	for {
		op, matched := p.matchMultiplicativeOp()
		if !matched {
			break
		}

		right, err := p.parseUnaryExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}

	return left, nil
}

func (p *Parser) matchMultiplicativeOp() (BinaryOp, bool) {
	for tk, op := range multiplicativeTokenOps {
		if p.match(tk) {
			return op, true
		}
	}
	return 0, false
}

// unaryOperatorMap maps tokens to their corresponding unary operators.
var unaryOperatorMap = map[TokenType]UnaryOp{
	TK_MINUS:  OpNeg,
	TK_BITNOT: OpBitNot,
	TK_NOT:    OpNot,
}

func (p *Parser) parseUnaryExpression() (Expression, error) {
	// Handle unary plus (no-op)
	if p.match(TK_PLUS) {
		return p.parseUnaryExpression()
	}

	// Handle NOT EXISTS
	if p.match(TK_NOT) && p.match(TK_EXISTS) {
		return p.parseExistsExpr(true)
	}

	// Handle other unary operators using map lookup
	for tok, op := range unaryOperatorMap {
		if p.match(tok) {
			return p.parseUnaryExprWithOp(op)
		}
	}

	return p.parsePostfixExpression()
}

// parseUnaryExprWithOp parses a unary expression with the given operator.
func (p *Parser) parseUnaryExprWithOp(op UnaryOp) (Expression, error) {
	expr, err := p.parseUnaryExpression()
	if err != nil {
		return nil, err
	}
	if op == OpNeg {
		if lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralInteger {
			clean := strings.ReplaceAll(lit.Value, "_", "")
			if clean == "9223372036854775808" {
				lit.Value = "-9223372036854775808"
				return lit, nil
			}
		}
	}
	return &UnaryExpr{Op: op, Expr: expr}, nil
}

func (p *Parser) parsePostfixExpression() (Expression, error) {
	expr, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	// COLLATE
	if p.match(TK_COLLATE) {
		if !p.check(TK_ID) {
			return nil, p.error("expected collation name")
		}
		return &CollateExpr{
			Expr:      expr,
			Collation: Unquote(p.advance().Lexeme),
		}, nil
	}

	return expr, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	// Try literal parsing
	if expr := p.tryParseLiteral(); expr != nil {
		return expr, nil
	}

	// Variable
	if p.check(TK_VARIABLE) {
		tok := p.advance()
		return &VariableExpr{Name: tok.Lexeme}, nil
	}

	// Identifier or function call (including keywords that can be used as identifiers)
	if p.isExpressionIdentifier() {
		return p.parseIdentOrFunction()
	}

	// Try parsing special expression forms
	return p.parseSpecialExpression()
}

// parseSpecialExpression parses CASE, CAST, EXISTS, or parenthesized expressions.
func (p *Parser) parseSpecialExpression() (Expression, error) {
	if p.match(TK_CASE) {
		return p.parseCaseExpr()
	}
	if p.match(TK_CAST) {
		return p.parseCastExpr()
	}
	if p.match(TK_EXISTS) {
		return p.parseExistsExpr(false)
	}
	if p.match(TK_LP) {
		return p.parseParenOrSubquery()
	}
	return nil, p.error("expected expression, got %s", p.peek().Type)
}

// literalTokenTypes maps token types to their literal expression type.
var literalTokenTypes = map[TokenType]LiteralType{
	TK_INTEGER: LiteralInteger,
	TK_FLOAT:   LiteralFloat,
	TK_STRING:  LiteralString,
	TK_BLOB:    LiteralBlob,
}

// tryParseLiteral attempts to parse a literal expression, returns nil if not a literal.
func (p *Parser) tryParseLiteral() Expression {
	if litType, ok := literalTokenTypes[p.peek().Type]; ok {
		tok := p.advance()
		val := tok.Lexeme
		if litType == LiteralString {
			val = Unquote(val)
		}
		return &LiteralExpr{Type: litType, Value: val}
	}
	if p.match(TK_NULL) {
		return &LiteralExpr{Type: LiteralNull, Value: "NULL"}
	}
	return p.tryParseBoolLiteral()
}

// tryParseBoolLiteral parses TRUE/FALSE as integer literals, returns nil if not a boolean.
func (p *Parser) tryParseBoolLiteral() Expression {
	if !p.check(TK_ID) {
		return nil
	}
	switch {
	case strings.EqualFold(p.peek().Lexeme, "TRUE"):
		p.advance()
		return &LiteralExpr{Type: LiteralInteger, Value: "1"}
	case strings.EqualFold(p.peek().Lexeme, "FALSE"):
		p.advance()
		return &LiteralExpr{Type: LiteralInteger, Value: "0"}
	default:
		return nil
	}
}

// parseIdentOrFunction parses an identifier or function call.
func (p *Parser) parseIdentOrFunction() (Expression, error) {
	name := Unquote(p.advance().Lexeme)

	// Function call
	if p.match(TK_LP) {
		// Check for RAISE function (trigger-specific)
		if strings.ToUpper(name) == "RAISE" {
			return p.parseRaiseFunction()
		}
		return p.parseFunctionCall(name)
	}

	// Column reference with optional table qualifier
	if p.match(TK_DOT) {
		if !p.isExpressionIdentifier() {
			return nil, p.error("expected column name after .")
		}
		column := Unquote(p.advance().Lexeme)
		return &IdentExpr{Table: name, Name: column}, nil
	}

	return &IdentExpr{Name: name}, nil
}

// isExpressionIdentifier checks if the current token can be used as an identifier in an expression.
// This includes regular identifiers and certain keywords that can be used as column/table names.
// In SQLite, type names and many other keywords can be used as identifiers.
func (p *Parser) isExpressionIdentifier() bool {
	switch p.peek().Type {
	case TK_ID, TK_TEMP,
		// Type keywords that can be column names
		TK_TEXT, TK_INTEGER_TYPE, TK_REAL, TK_BLOB_TYPE, TK_NUMERIC,
		// Other common keywords that SQLite allows as identifiers
		TK_KEY, TK_ABORT, TK_ACTION, TK_AFTER, TK_ANALYZE,
		TK_ASC, TK_BEFORE, TK_CASCADE, TK_CONFLICT, TK_DATABASE,
		TK_DEFERRED, TK_DESC, TK_EACH, TK_EXCLUSIVE, TK_FAIL,
		TK_FOR, TK_IGNORE, TK_IMMEDIATE, TK_INITIALLY, TK_NO,
		TK_OF, TK_PLAN, TK_PRAGMA, TK_QUERY, TK_RECURSIVE,
		TK_REINDEX, TK_RELEASE, TK_RENAME, TK_REPLACE, TK_RESTRICT,
		TK_ROW, TK_ROWID, TK_SAVEPOINT, TK_STRICT, TK_VACUUM,
		TK_VIEW, TK_VIRTUAL, TK_WITHOUT:
		return true
	default:
		return false
	}
}

// isRaiseAction checks if the current token is a valid RAISE action keyword.
// RAISE actions may be tokenized as keywords (TK_ABORT, TK_FAIL, etc.) or TK_ID.
func (p *Parser) isRaiseAction() bool {
	switch p.peek().Type {
	case TK_ID, TK_ABORT, TK_FAIL, TK_IGNORE:
		return true
	default:
		// ROLLBACK may also be tokenized as a keyword
		return p.check(TK_ID) || strings.EqualFold(p.peek().Lexeme, "ROLLBACK")
	}
}

// raiseActionMap maps RAISE action strings to their RaiseType.
var raiseActionMap = map[string]RaiseType{
	"IGNORE":   RaiseIgnore,
	"ROLLBACK": RaiseRollback,
	"ABORT":    RaiseAbort,
	"FAIL":     RaiseFail,
}

// parseRaiseMessage parses the optional comma and message for non-IGNORE RAISE types.
func (p *Parser) parseRaiseMessage(raise *RaiseExpr) error {
	if raise.Type == RaiseIgnore {
		return nil
	}
	if !p.match(TK_COMMA) {
		return p.error("expected ',' after RAISE action")
	}
	if !p.check(TK_STRING) {
		return p.error("expected error message string in RAISE")
	}
	raise.Message = Unquote(p.advance().Lexeme)
	return nil
}

// parseRaiseFunction parses RAISE(IGNORE) or RAISE(ROLLBACK|ABORT|FAIL, msg).
// The opening paren has already been consumed.
func (p *Parser) parseRaiseFunction() (Expression, error) {
	if !p.isRaiseAction() {
		return nil, p.error("expected IGNORE, ROLLBACK, ABORT, or FAIL in RAISE")
	}

	action := strings.ToUpper(p.advance().Lexeme)
	raiseType, ok := raiseActionMap[action]
	if !ok {
		return nil, p.error("expected IGNORE, ROLLBACK, ABORT, or FAIL in RAISE")
	}
	raise := &RaiseExpr{Type: raiseType}

	if err := p.parseRaiseMessage(raise); err != nil {
		return nil, err
	}

	if !p.match(TK_RP) {
		return nil, p.error("expected ')' after RAISE arguments")
	}

	return raise, nil
}

// parseFunctionCall parses a function call after the opening paren.
func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	fn := &FunctionExpr{Name: strings.ToUpper(name)}

	if err := p.parseFunctionArgs(fn); err != nil {
		return nil, err
	}

	if err := p.parseFunctionClauses(fn); err != nil {
		return nil, err
	}

	return fn, nil
}

func (p *Parser) parseFunctionArgs(fn *FunctionExpr) error {
	if p.match(TK_DISTINCT) {
		fn.Distinct = true
	}

	if p.match(TK_STAR) {
		fn.Star = true
	} else if !p.check(TK_RP) {
		args, err := p.parseExpressionList()
		if err != nil {
			return err
		}
		fn.Args = args
	}

	if !p.match(TK_RP) {
		return p.error("expected ) after function arguments")
	}
	return nil
}

func (p *Parser) parseFunctionClauses(fn *FunctionExpr) error {
	if err := p.parseFunctionFilter(fn); err != nil {
		return err
	}
	return p.parseFunctionOver(fn)
}

// parseFunctionFilter parses the optional FILTER clause for a function.
func (p *Parser) parseFunctionFilter(fn *FunctionExpr) error {
	if !p.match(TK_FILTER) {
		return nil
	}
	if !p.match(TK_LP) {
		return p.error("expected ( after FILTER")
	}
	if !p.match(TK_WHERE) {
		return p.error("expected WHERE in FILTER")
	}
	filter, err := p.parseExpression()
	if err != nil {
		return err
	}
	fn.Filter = filter
	if !p.match(TK_RP) {
		return p.error("expected ) after FILTER")
	}
	return nil
}

// parseFunctionOver parses the optional OVER clause for window functions.
// Supports both OVER (window-spec) and OVER window-name.
func (p *Parser) parseFunctionOver(fn *FunctionExpr) error {
	if !p.match(TK_OVER) {
		return nil
	}

	// Check for named window reference: OVER window_name
	if p.check(TK_ID) && p.peekAhead(1).Type != TK_LP {
		fn.Over = &WindowSpec{BaseName: Unquote(p.advance().Lexeme)}
		return nil
	}

	if !p.match(TK_LP) {
		return p.error("expected ( or window name after OVER")
	}

	windowSpec := &WindowSpec{}

	if err := p.parsePartitionBy(windowSpec); err != nil {
		return err
	}
	if err := p.parseWindowOrderBy(windowSpec); err != nil {
		return err
	}
	if err := p.parseWindowFrame(windowSpec); err != nil {
		return err
	}

	if !p.match(TK_RP) {
		return p.error("expected ) after window specification")
	}

	fn.Over = windowSpec
	return nil
}

// parsePartitionBy parses the optional PARTITION BY clause.
func (p *Parser) parsePartitionBy(windowSpec *WindowSpec) error {
	if !p.match(TK_PARTITION) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after PARTITION")
	}
	partitionExprs, err := p.parseExpressionList()
	if err != nil {
		return err
	}
	windowSpec.PartitionBy = partitionExprs
	return nil
}

// parseWindowOrderBy parses the optional ORDER BY clause.
func (p *Parser) parseWindowOrderBy(windowSpec *WindowSpec) error {
	if !p.match(TK_ORDER) {
		return nil
	}
	if !p.match(TK_BY) {
		return p.error("expected BY after ORDER")
	}
	orderTerms, err := p.parseOrderByList()
	if err != nil {
		return err
	}
	windowSpec.OrderBy = orderTerms
	return nil
}

// parseWindowFrame parses the optional frame specification (ROWS, RANGE, GROUPS).
func (p *Parser) parseWindowFrame(windowSpec *WindowSpec) error {
	if !p.check(TK_ROWS) && !p.check(TK_RANGE) && !p.check(TK_GROUPS) {
		return nil
	}
	frameSpec, err := p.parseFrameSpec()
	if err != nil {
		return err
	}
	windowSpec.Frame = frameSpec
	return nil
}

// parseCaseExpr parses a CASE expression after the CASE keyword.
func (p *Parser) parseCaseExpr() (Expression, error) {
	caseExpr := &CaseExpr{}
	if err := p.parseCaseBaseExpr(caseExpr); err != nil {
		return nil, err
	}
	if err := p.parseWhenClauses(caseExpr); err != nil {
		return nil, err
	}
	if err := p.parseCaseElse(caseExpr); err != nil {
		return nil, err
	}
	if !p.match(TK_END) {
		return nil, p.error("expected END after CASE")
	}
	return caseExpr, nil
}

// parseCaseBaseExpr parses the optional base expression in CASE.
func (p *Parser) parseCaseBaseExpr(caseExpr *CaseExpr) error {
	if p.check(TK_WHEN) {
		return nil
	}
	expr, err := p.parseExpression()
	if err != nil {
		return err
	}
	caseExpr.Expr = expr
	return nil
}

// parseWhenClauses parses all WHEN clauses.
func (p *Parser) parseWhenClauses(caseExpr *CaseExpr) error {
	for p.match(TK_WHEN) {
		clause, err := p.parseWhenClause()
		if err != nil {
			return err
		}
		caseExpr.WhenClauses = append(caseExpr.WhenClauses, clause)
	}
	return nil
}

// parseWhenClause parses a single WHEN ... THEN ... clause.
func (p *Parser) parseWhenClause() (WhenClause, error) {
	condition, err := p.parseExpression()
	if err != nil {
		return WhenClause{}, err
	}
	if !p.match(TK_THEN) {
		return WhenClause{}, p.error("expected THEN after WHEN condition")
	}
	result, err := p.parseExpression()
	if err != nil {
		return WhenClause{}, err
	}
	return WhenClause{Condition: condition, Result: result}, nil
}

// parseCaseElse parses the optional ELSE clause.
func (p *Parser) parseCaseElse(caseExpr *CaseExpr) error {
	if !p.match(TK_ELSE) {
		return nil
	}
	elseExpr, err := p.parseExpression()
	if err != nil {
		return err
	}
	caseExpr.ElseClause = elseExpr
	return nil
}

// parseCastExpr parses a CAST expression after the CAST keyword.
func (p *Parser) parseCastExpr() (Expression, error) {
	if !p.match(TK_LP) {
		return nil, p.error("expected ( after CAST")
	}
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(TK_AS) {
		return nil, p.error("expected AS in CAST")
	}
	if !typeNameTokens[p.peek().Type] {
		return nil, p.error("expected type name")
	}
	typeName := p.parseTypeName()
	if !p.match(TK_RP) {
		return nil, p.error("expected ) after CAST")
	}
	return &CastExpr{Expr: expr, Type: typeName}, nil
}

// parseExistsExpr parses an EXISTS or NOT EXISTS expression after the EXISTS keyword.
func (p *Parser) parseExistsExpr(not bool) (Expression, error) {
	if !p.match(TK_LP) {
		return nil, p.error("expected ( after EXISTS")
	}
	if !p.match(TK_SELECT) {
		return nil, p.error("expected SELECT in EXISTS subquery")
	}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if !p.match(TK_RP) {
		return nil, p.error("expected ) after EXISTS subquery")
	}
	return &ExistsExpr{Select: sel, Not: not}, nil
}

// parseParenOrSubquery parses a parenthesized expression or subquery.
func (p *Parser) parseParenOrSubquery() (Expression, error) {
	if p.match(TK_SELECT) {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if !p.match(TK_RP) {
			return nil, p.error("expected ) after subquery")
		}
		return &SubqueryExpr{Select: sel}, nil
	}

	// Handle WITH ... SELECT subquery (CTE in subquery)
	if p.check(TK_WITH) {
		sel, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if !p.match(TK_RP) {
			return nil, p.error("expected ) after subquery")
		}
		return &SubqueryExpr{Select: sel}, nil
	}

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if !p.match(TK_RP) {
		return nil, p.error("expected ) after expression")
	}
	return &ParenExpr{Expr: expr}, nil
}

// =============================================================================
// Helper methods
// =============================================================================

func (p *Parser) parseExpressionList() ([]Expression, error) {
	exprs := make([]Expression, 0)

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if !p.match(TK_COMMA) {
			break
		}
	}

	return exprs, nil
}

func (p *Parser) parseOrderByList() ([]OrderingTerm, error) {
	terms := make([]OrderingTerm, 0)

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		term := OrderingTerm{
			Expr: expr,
			Asc:  true,
		}

		// If the expression is a CollateExpr, extract the collation
		if collateExpr, ok := expr.(*CollateExpr); ok {
			term.Collation = collateExpr.Collation
			term.Expr = collateExpr.Expr
		}

		if p.match(TK_DESC) {
			term.Asc = false
		} else {
			p.match(TK_ASC)
		}

		terms = append(terms, term)

		if !p.match(TK_COMMA) {
			break
		}
	}

	return terms, nil
}

func (p *Parser) parseOnConflict() OnConflictClause {
	if p.match(TK_ROLLBACK) {
		return OnConflictRollback
	} else if p.match(TK_ABORT) {
		return OnConflictAbort
	} else if p.match(TK_FAIL) {
		return OnConflictFail
	} else if p.match(TK_IGNORE) {
		return OnConflictIgnore
	} else if p.match(TK_REPLACE) {
		return OnConflictReplace
	}
	return OnConflictNone
}

func (p *Parser) isJoinKeyword() bool {
	return p.check(TK_JOIN) || p.check(TK_LEFT) || p.check(TK_RIGHT) ||
		p.check(TK_INNER) || p.check(TK_OUTER) || p.check(TK_CROSS) ||
		p.check(TK_NATURAL)
}

func (p *Parser) isColumnConstraint() bool {
	if p.isAtEnd() {
		return false
	}
	return columnConstraintKeywords[p.peek().Type]
}

func (p *Parser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TK_EOF}
	}
	return p.tokens[p.current]
}

func (p *Parser) peekAhead(n int) Token {
	pos := p.current + n
	if pos >= len(p.tokens) {
		return Token{Type: TK_EOF}
	}
	return p.tokens[pos]
}

func (p *Parser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

func (p *Parser) check(t TokenType) bool {
	if p.isAtEnd() {
		return false
	}
	return p.peek().Type == t
}

// checkIdentifier returns true if the current token can be used as an identifier
// (column name, table name, etc.). This includes TK_ID and many keywords that
// SQLite allows as identifiers.
func (p *Parser) checkIdentifier() bool {
	if p.isAtEnd() {
		return false
	}
	return keywordsAsIdentifiers[p.peek().Type]
}

func (p *Parser) match(types ...TokenType) bool {
	for _, t := range types {
		if p.check(t) {
			p.advance()
			return true
		}
	}
	return false
}

func (p *Parser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.peek().Type == TK_EOF
}

func (p *Parser) error(format string, args ...interface{}) error {
	tok := p.peek()
	msg := fmt.Sprintf(format, args...)
	fullMsg := fmt.Sprintf("parse error at line %d, col %d: %s", tok.Line, tok.Col, msg)
	p.errors = append(p.errors, fullMsg)
	return fmt.Errorf("%s", fullMsg)
}

// ParseString is a convenience function to parse a SQL string.
func ParseString(sql string) ([]Statement, error) {
	parser := NewParser(sql)
	return parser.Parse()
}

// IntValue returns the integer value of a literal expression.
func IntValue(expr Expression) (int64, error) {
	if lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralInteger {
		return strconv.ParseInt(lit.Value, 10, 64)
	}
	return 0, fmt.Errorf("not an integer literal")
}

// FloatValue returns the float value of a literal expression.
func FloatValue(expr Expression) (float64, error) {
	if lit, ok := expr.(*LiteralExpr); ok && (lit.Type == LiteralFloat || lit.Type == LiteralInteger) {
		return strconv.ParseFloat(lit.Value, 64)
	}
	return 0, fmt.Errorf("not a numeric literal")
}

// StringValue returns the string value of a literal expression.
func StringValue(expr Expression) (string, error) {
	if lit, ok := expr.(*LiteralExpr); ok && lit.Type == LiteralString {
		return lit.Value, nil
	}
	return "", fmt.Errorf("not a string literal")
}

// parseFrameSpec parses a window frame specification (ROWS/RANGE/GROUPS BETWEEN...).
// frameModeMap maps tokens to frame modes.
var frameModeMap = map[TokenType]FrameMode{
	TK_ROWS:   FrameRows,
	TK_RANGE:  FrameRange,
	TK_GROUPS: FrameGroups,
}

func (p *Parser) parseFrameSpec() (*FrameSpec, error) {
	frameSpec := &FrameSpec{}

	// Parse frame mode using map lookup
	if err := p.parseFrameMode(frameSpec); err != nil {
		return nil, err
	}

	// Parse BETWEEN clause or simple frame bound
	if p.match(TK_BETWEEN) {
		return p.parseFrameBetween(frameSpec)
	}
	return p.parseFrameSingleBound(frameSpec)
}

// parseFrameMode parses the frame mode (ROWS, RANGE, or GROUPS).
func (p *Parser) parseFrameMode(frameSpec *FrameSpec) error {
	for tok, mode := range frameModeMap {
		if p.match(tok) {
			frameSpec.Mode = mode
			return nil
		}
	}
	return p.error("expected ROWS, RANGE, or GROUPS")
}

// parseFrameBetween parses a BETWEEN start AND end clause.
func (p *Parser) parseFrameBetween(frameSpec *FrameSpec) (*FrameSpec, error) {
	start, err := p.parseFrameBound()
	if err != nil {
		return nil, err
	}
	frameSpec.Start = start

	if !p.match(TK_AND) {
		return nil, p.error("expected AND in frame specification")
	}

	end, err := p.parseFrameBound()
	if err != nil {
		return nil, err
	}
	frameSpec.End = end
	return frameSpec, nil
}

// parseFrameSingleBound parses a single bound (implicitly UNBOUNDED PRECEDING to specified bound).
func (p *Parser) parseFrameSingleBound(frameSpec *FrameSpec) (*FrameSpec, error) {
	bound, err := p.parseFrameBound()
	if err != nil {
		return nil, err
	}
	frameSpec.Start = FrameBound{Type: BoundUnboundedPreceding}
	frameSpec.End = bound
	return frameSpec, nil
}

// parseFrameBound parses a single frame boundary.
func (p *Parser) parseFrameBound() (FrameBound, error) {
	// Try UNBOUNDED PRECEDING/FOLLOWING
	if p.match(TK_UNBOUNDED) {
		return p.parseUnboundedBound()
	}

	// Try CURRENT ROW
	if p.match(TK_CURRENT) {
		return p.parseCurrentRowBound()
	}

	// Parse numeric offset with PRECEDING/FOLLOWING
	return p.parseOffsetBound()
}

// parseUnboundedBound parses UNBOUNDED PRECEDING or UNBOUNDED FOLLOWING.
func (p *Parser) parseUnboundedBound() (FrameBound, error) {
	bound := FrameBound{}
	if p.match(TK_PRECEDING) {
		bound.Type = BoundUnboundedPreceding
		return bound, nil
	}
	if p.match(TK_FOLLOWING) {
		bound.Type = BoundUnboundedFollowing
		return bound, nil
	}
	return bound, p.error("expected PRECEDING or FOLLOWING after UNBOUNDED")
}

// parseCurrentRowBound parses CURRENT ROW.
func (p *Parser) parseCurrentRowBound() (FrameBound, error) {
	bound := FrameBound{}
	if !p.match(TK_ROW) {
		return bound, p.error("expected ROW after CURRENT")
	}
	bound.Type = BoundCurrentRow
	return bound, nil
}

// parseOffsetBound parses N PRECEDING or N FOLLOWING.
func (p *Parser) parseOffsetBound() (FrameBound, error) {
	bound := FrameBound{}
	expr, err := p.parsePrimaryExpression()
	if err != nil {
		return bound, err
	}

	if p.match(TK_PRECEDING) {
		bound.Type = BoundPreceding
		bound.Offset = expr
		return bound, nil
	}
	if p.match(TK_FOLLOWING) {
		bound.Type = BoundFollowing
		bound.Offset = expr
		return bound, nil
	}
	return bound, p.error("expected PRECEDING or FOLLOWING after offset")
}
