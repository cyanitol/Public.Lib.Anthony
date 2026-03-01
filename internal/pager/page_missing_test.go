// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"testing"
)

// TestDbPageGetPgno tests GetPgno method
func TestDbPageGetPgno(t *testing.T) {
	t.Parallel()
	page := NewDbPage(42, 4096)

	if page.GetPgno() != 42 {
		t.Errorf("expected pgno 42, got %d", page.GetPgno())
	}
}

// TestDbPageGetData tests GetData method
func TestDbPageGetData(t *testing.T) {
	t.Parallel()
	page := NewDbPage(1, 4096)
	page.Data[0] = 0xAA
	page.Data[100] = 0xBB

	data := page.GetData()
	if data[0] != 0xAA {
		t.Errorf("expected data[0] = 0xAA, got 0x%02X", data[0])
	}
	if data[100] != 0xBB {
		t.Errorf("expected data[100] = 0xBB, got 0x%02X", data[100])
	}
}

// TestDbPageSize tests Size method
func TestDbPageSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pageSize int
	}{
		{"1KB", 1024},
		{"4KB", 4096},
		{"8KB", 8192},
		{"16KB", 16384},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			page := NewDbPage(1, tt.pageSize)
			if page.Size() != tt.pageSize {
				t.Errorf("expected size %d, got %d", tt.pageSize, page.Size())
			}
		})
	}
}

// TestDbPageSetDontWrite tests SetDontWrite method
func TestDbPageSetDontWrite(t *testing.T) {
	t.Parallel()
	page := NewDbPage(1, 4096)

	// Should write by default
	if !page.ShouldWrite() {
		t.Error("page should write by default")
	}

	// Set dont write
	page.SetDontWrite()

	if page.ShouldWrite() {
		t.Error("page should not write after SetDontWrite")
	}
}
OINT
	TK_RELEASE
	TK_DEFERRED
	TK_IMMEDIATE
	TK_EXCLUSIVE

	// Keywords - CTE
	TK_WITH
	TK_RECURSIVE

	// Keywords - Other
	TK_EXPLAIN
	TK_QUERY
	TK_PLAN
	TK_PRAGMA
	TK_ANALYZE
	TK_ATTACH
	TK_DETACH
	TK_DATABASE
	TK_VACUUM
	TK_REINDEX

	// Operators - Comparison
	TK_EQ // =, ==
	TK_NE // <>, !=
	TK_LT // <
	TK_LE // <=
	TK_GT // >
	TK_GE // >=
	TK_ISNULL
	TK_NOTNULL

	// Operators - Arithmetic
	TK_PLUS  // +
	TK_MINUS // -
	TK_STAR  // *
	TK_SLASH // /
	TK_REM   // %

	// Operators - Bitwise
	TK_BITAND // &
	TK_BITOR  // |
	TK_BITNOT // ~
	TK_LSHIFT // <<
	TK_RSHIFT // >>

	// Operators - String
	TK_CONCAT // ||

	// Punctuation
	TK_LP    // (
	TK_RP    // )
	TK_COMMA // ,
	TK_SEMI  // ;
	TK_DOT   // .

	// Keywords - Window functions
	TK_OVER
	TK_PARTITION
	TK_ROWS
	TK_RANGE
	TK_UNBOUNDED
	TK_CURRENT
	TK_FOLLOWING
	TK_PRECEDING
	TK_FILTER
	TK_WINDOW
	TK_GROUPS
	TK_EXCLUDE
	TK_TIES
	TK_OTHERS

	// Keywords - Set operations
	TK_UNION
	TK_EXCEPT
	TK_INTERSECT

	// Additional keywords
	TK_CAST
	TK_ESCAPE
	TK_MATCH
	TK_REGEXP
	TK_ABORT
	TK_ACTION
	TK_AFTER
	TK_BEFORE
	TK_CASCADE
	TK_CONFLICT
	TK_FAIL
	TK_IGNORE
	TK_REPLACE
	TK_RESTRICT
	TK_NO
	TK_EACH
	TK_FOR
	TK_ROW
	TK_INITIALLY
	TK_DEFERRABLE
	TK_INDEXED
	TK_WITHOUT
	TK_ROWID
	TK_STRICT
	TK_GENERATED
	TK_ALWAYS
	TK_STORED
	TK_INSTEAD
	TK_OF
	TK_DO
	TK_NOTHING

	// Special operator types
	TK_PTR     // ->
	TK_QNUMBER // Quoted number (with separators)
)

// Token represents a SQL token with its type, text, and position.
type Token struct {
	Type   TokenType // Token type
	Lexeme string    // Raw text of the token
	Pos    int       // Starting position in source
	Line   int       // Line number (1-based)
	Col    int       // Column number (1-based)
}

// tokenTypeNames maps TokenType values to their string representations.
var tokenTypeNames = [...]string{
	TK_EOF:           "EOF",
	TK_ILLEGAL:       "ILLEGAL",
	TK_SPACE:         "SPACE",
	TK_COMMENT:       "COMMENT",
	TK_INTEGER:       "INTEGER",
	TK_FLOAT:         "FLOAT",
	TK_STRING:        "STRING",
	TK_BLOB:          "BLOB",
	TK_NULL:          "NULL",
	TK_ID:            "ID",
	TK_VARIABLE:      "VARIABLE",
	TK_CREATE:        "CREATE",
	TK_TABLE:         "TABLE",
	TK_INDEX:         "INDEX",
	TK_VIEW:          "VIEW",
	TK_TRIGGER:       "TRIGGER",
	TK_DROP:          "DROP",
	TK_ALTER:         "ALTER",
	TK_RENAME:        "RENAME",
	TK_ADD:           "ADD",
	TK_COLUMN:        "COLUMN",
	TK_TO:            "TO",
	TK_SELECT:        "SELECT",
	TK_FROM:          "FROM",
	TK_WHERE:         "WHERE",
	TK_INSERT:        "INSERT",
	TK_INTO:          "INTO",
	TK_VALUES:        "VALUES",
	TK_UPDATE:        "UPDATE",
	TK_SET:           "SET",
	TK_DELETE:        "DELETE",
	TK_ORDER:         "ORDER",
	TK_BY:            "BY",
	TK_GROUP:         "GROUP",
	TK_HAVING:        "HAVING",
	TK_LIMIT:         "LIMIT",
	TK_OFFSET:        "OFFSET",
	TK_DISTINCT:      "DISTINCT",
	TK_ALL:           "ALL",
	TK_ASC:           "ASC",
	TK_DESC:          "DESC",
	TK_JOIN:          "JOIN",
	TK_LEFT:          "LEFT",
	TK_RIGHT:         "RIGHT",
	TK_INNER:         "INNER",
	TK_OUTER:         "OUTER",
	TK_CROSS:         "CROSS",
	TK_NATURAL:       "NATURAL",
	TK_ON:            "ON",
	TK_USING:         "USING",
	TK_AND:           "AND",
	TK_OR:            "OR",
	TK_NOT:           "NOT",
	TK_IS:            "IS",
	TK_IN:            "IN",
	TK_LIKE:          "LIKE",
	TK_GLOB:          "GLOB",
	TK_BETWEEN:       "BETWEEN",
	TK_CASE:          "CASE",
	TK_WHEN:          "WHEN",
	TK_THEN:          "THEN",
	TK_ELSE:          "ELSE",
	TK_END:           "END",
	TK_INTEGER_TYPE:  "INTEGER_TYPE",
	TK_REAL:          "REAL",
	TK_TEXT:          "TEXT",
	TK_BLOB_TYPE:     "BLOB_TYPE",
	TK_NUMERIC:       "NUMERIC",
	TK_PRIMARY:       "PRIMARY",
	TK_KEY:           "KEY",
	TK_UNIQUE:        "UNIQUE",
	TK_CHECK:         "CHECK",
	TK_DEFAULT:       "DEFAULT",
	TK_CONSTRAINT:    "CONSTRAINT",
	TK_FOREIGN:       "FOREIGN",
	TK_REFERENCES:    "REFERENCES",
	TK_AUTOINCREMENT: "AUTOINCREMENT",
	TK_COLLATE:       "COLLATE",
	TK_AS:            "AS",
	TK_IF:            "IF",
	TK_EXISTS:        "EXISTS",
	TK_TEMPORARY:     "TEMPORARY",
	TK_TEMP:          "TEMP",
	TK_VIRTUAL:       "VIRTUAL",
	TK_BEGIN:         "BEGIN",
	TK_COMMIT:        "COMMIT",
	TK_ROLLBACK:      "ROLLBACK",
	TK_TRANSACTION:   "TRANSACTION",
	TK_SAVEPOINT:     "SAVEPOINT",
	TK_RELEASE:       "RELEASE",
	TK_DEFERRED:      "DEFERRED",
	TK_IMMEDIATE:     "IMMEDIATE",
	TK_EXCLUSIVE:     "EXCLUSIVE",
	TK_WITH:          "WITH",
	TK_RECURSIVE:     "RECURSIVE",
	TK_EXPLAIN:       "EXPLAIN",
	TK_QUERY:         "QUERY",
	TK_PLAN:          "PLAN",
	TK_PRAGMA:        "PRAGMA",
	TK_ANALYZE:       "ANALYZE",
	TK_ATTACH:        "ATTACH",
	TK_DETACH:        "DETACH",
	TK_DATABASE:      "DATABASE",
	TK_VACUUM:        "VACUUM",
	TK_REINDEX:       "REINDEX",
	TK_EQ:            "EQ",
	TK_NE:            "NE",
	TK_LT:            "LT",
	TK_LE:            "LE",
	TK_GT:            "GT",
	TK_GE:            "GE",
	TK_ISNULL:        "ISNULL",
	TK_NOTNULL:       "NOTNULL",
	TK_PLUS:          "PLUS",
	TK_MINUS:         "MINUS",
	TK_STAR:          "STAR",
	TK_SLASH:         "SLASH",
	TK_REM:           "REM",
	TK_BITAND:        "BITAND",
	TK_BITOR:         "BITOR",
	TK_BITNOT:        "BITNOT",
	TK_LSHIFT:        "LSHIFT",
	TK_RSHIFT:        "RSHIFT",
	TK_CONCAT:        "CONCAT",
	TK_LP:            "LP",
	TK_RP:            "RP",
	TK_COMMA:         "COMMA",
	TK_SEMI:          "SEMI",
	TK_DOT:           "DOT",
	TK_OVER:          "OVER",
	TK_PARTITION:     "PARTITION",
	TK_ROWS:          "ROWS",
	TK_RANGE:         "RANGE",
	TK_UNBOUNDED:     "UNBOUNDED",
	TK_CURRENT:       "CURRENT",
	TK_FOLLOWING:     "FOLLOWING",
	TK_PRECEDING:     "PRECEDING",
	TK_FILTER:        "FILTER",
	TK_WINDOW:        "WINDOW",
	TK_GROUPS:        "GROUPS",
	TK_EXCLUDE:       "EXCLUDE",
	TK_TIES:          "TIES",
	TK_OTHERS:        "OTHERS",
	TK_UNION:         "UNION",
	TK_EXCEPT:        "EXCEPT",
	TK_INTERSECT:     "INTERSECT",
	TK_CAST:          "CAST",
	TK_ESCAPE:        "ESCAPE",
	TK_MATCH:         "MATCH",
	TK_REGEXP:        "REGEXP",
	TK_ABORT:         "ABORT",
	TK_ACTION:        "ACTION",
	TK_AFTER:         "AFTER",
	TK_BEFORE:        "BEFORE",
	TK_CASCADE:       "CASCADE",
	TK_CONFLICT:      "CONFLICT",
	TK_FAIL:          "FAIL",
	TK_IGNORE:        "IGNORE",
	TK_REPLACE:       "REPLACE",
	TK_RESTRICT:      "RESTRICT",
	TK_NO:            "NO",
	TK_EACH:          "EACH",
	TK_FOR:           "FOR",
	TK_ROW:           "ROW",
	TK_INITIALLY:     "INITIALLY",
	TK_DEFERRABLE:    "DEFERRABLE",
	TK_INDEXED:       "INDEXED",
	TK_WITHOUT:       "WITHOUT",
	TK_ROWID:         "ROWID",
	TK_STRICT:        "STRICT",
	TK_GENERATED:     "GENERATED",
	TK_ALWAYS:        "ALWAYS",
	TK_STORED:        "STORED",
	TK_INSTEAD:       "INSTEAD",
	TK_OF:            "OF",
	TK_DO:            "DO",
	TK_NOTHING:       "NOTHING",
	TK_PTR:           "PTR",
	TK_QNUMBER:       "QNUMBER",
}

// String returns a string representation of the token type.
func (t TokenType) String() string {
	if int(t) >= 0 && int(t) < len(tokenTypeNames) && tokenTypeNames[t] != "" {
		return tokenTypeNames[t]
	}
	return "UNKNOWN"
}

// IsKeyword returns true if the token is a SQL keyword.
func (t TokenType) IsKeyword() bool {
	return t >= TK_CREATE && t <= TK_STORED
}

// IsOperator returns true if the token is an operator.
func (t TokenType) IsOperator() bool {
	return (t >= TK_EQ && t <= TK_NOTNULL) ||
		(t >= TK_PLUS && t <= TK_REM) ||
		(t >= TK_BITAND && t <= TK_RSHIFT) ||
		t == TK_CONCAT
}

// IsLiteral returns true if the token is a literal value.
func (t TokenType) IsLiteral() bool {
	return t >= TK_INTEGER && t <= TK_NULL
}

// IsPunctuation returns true if the token is punctuation.
func (t TokenType) IsPunctuation() bool {
	return t >= TK_LP && t <= TK_DOT
}
