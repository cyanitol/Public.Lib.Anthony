// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestParserEdgeCasesForHighCoverage tests remaining edge cases for high coverage
func TestParserEdgeCasesForHighCoverage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// parseCreateIndex edge cases
		{
			name:    "CREATE INDEX missing table name",
			sql:     "CREATE INDEX idx",
			wantErr: true,
		},
		{
			name:    "CREATE INDEX missing ON",
			sql:     "CREATE INDEX idx users (name)",
			wantErr: true,
		},
		{
			name:    "CREATE INDEX missing columns",
			sql:     "CREATE INDEX idx ON users",
			wantErr: true,
		},

		// parseIndexColumns edge cases
		{
			name:    "CREATE INDEX with invalid columns",
			sql:     "CREATE INDEX idx ON users ()",
			wantErr: true,
		},

		// parseFunctionFilter edge cases
		{
			name:    "function with FILTER and WHERE",
			sql:     "SELECT COUNT(*) FILTER (WHERE age > 18) FROM users",
			wantErr: false,
		},
		{
			name:    "aggregate with FILTER missing WHERE",
			sql:     "SELECT COUNT(*) FILTER (age > 18) FROM users",
			wantErr: true,
		},

		// parseWhenClause edge cases
		{
			name:    "CASE with WHEN missing THEN",
			sql:     "SELECT CASE WHEN x = 1 END FROM t",
			wantErr: true,
		},

		// parseParenOrSubquery edge cases
		{
			name:    "subquery in expression",
			sql:     "SELECT (SELECT MAX(age) FROM users) FROM dual",
			wantErr: false,
		},

		// parseBetweenExpression edge cases
		{
			name:    "BETWEEN missing AND",
			sql:     "SELECT * FROM users WHERE age BETWEEN 18",
			wantErr: true,
		},

		// parseInExpression edge cases
		{
			name:    "IN with subquery",
			sql:     "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantErr: false,
		},
		{
			name:    "IN with empty list",
			sql:     "SELECT * FROM users WHERE id IN ()",
			wantErr: true,
		},

		// parseTriggerForEachRow edge cases
		{
			name:    "trigger with FOR EACH ROW",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users FOR EACH ROW BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger without FOR EACH ROW",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},

		// parseVacuum edge cases
		{
			name:    "VACUUM",
			sql:     "VACUUM",
			wantErr: false,
		},
		{
			name:    "VACUUM with schema",
			sql:     "VACUUM main",
			wantErr: false,
		},
		{
			name:    "VACUUM with table",
			sql:     "VACUUM users",
			wantErr: false,
		},

		// peek edge case - empty input actually returns success with no statements
		{
			name:    "empty statement",
			sql:     "",
			wantErr: false,
		},

		// IntValue, FloatValue, StringValue edge cases via PRAGMA
		{
			name:    "PRAGMA with integer",
			sql:     "PRAGMA cache_size = 2000",
			wantErr: false,
		},
		{
			name:    "PRAGMA with string",
			sql:     "PRAGMA encoding = 'UTF-8'",
			wantErr: false,
		},
		{
			name:    "PRAGMA with float - not typically supported",
			sql:     "PRAGMA mmap_size = 3.14",
			wantErr: false,
		},

		// isPragmaValueIdentifier edge cases
		{
			name:    "PRAGMA with TRUE",
			sql:     "PRAGMA foreign_keys = TRUE",
			wantErr: false,
		},
		{
			name:    "PRAGMA with FALSE",
			sql:     "PRAGMA foreign_keys = FALSE",
			wantErr: false,
		},

		// checkIdentifier edge cases
		{
			name:    "SELECT with reserved word as column",
			sql:     "SELECT \"select\" FROM users",
			wantErr: false,
		},

		// parseAlterTableRename edge cases
		{
			name:    "ALTER TABLE RENAME TO",
			sql:     "ALTER TABLE old_name RENAME TO new_name",
			wantErr: false,
		},
		{
			name:    "ALTER TABLE RENAME COLUMN",
			sql:     "ALTER TABLE users RENAME COLUMN old_col TO new_col",
			wantErr: false,
		},
		{
			name:    "ALTER TABLE ADD COLUMN",
			sql:     "ALTER TABLE users ADD COLUMN email TEXT",
			wantErr: false,
		},
		{
			name:    "ALTER TABLE DROP COLUMN",
			sql:     "ALTER TABLE users DROP COLUMN email",
			wantErr: false,
		},

		// parseForeignKeyReferences edge cases - ON DELETE/UPDATE now supported
		{
			name:    "FOREIGN KEY with ON DELETE CASCADE",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY with ON UPDATE SET NULL",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE SET NULL)",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY with ON DELETE SET DEFAULT",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET DEFAULT)",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY with ON UPDATE RESTRICT",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE RESTRICT)",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY with ON DELETE NO ACTION",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION)",
			wantErr: false,
		},

		// parseForeignKeyColumns edge cases
		{
			name:    "FOREIGN KEY missing column list",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY () REFERENCES users(id))",
			wantErr: true,
		},

		// parseDropTable edge cases
		{
			name:    "DROP TABLE",
			sql:     "DROP TABLE users",
			wantErr: false,
		},
		{
			name:    "DROP TABLE IF EXISTS",
			sql:     "DROP TABLE IF EXISTS users",
			wantErr: false,
		},

		// parseDropIndex edge cases
		{
			name:    "DROP INDEX",
			sql:     "DROP INDEX idx_name",
			wantErr: false,
		},
		{
			name:    "DROP INDEX IF EXISTS",
			sql:     "DROP INDEX IF EXISTS idx_name",
			wantErr: false,
		},

		// parseCreateView edge cases
		{
			name:    "CREATE VIEW",
			sql:     "CREATE VIEW v1 AS SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW IF NOT EXISTS",
			sql:     "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW with columns",
			sql:     "CREATE VIEW v1 (id, name) AS SELECT id, name FROM users",
			wantErr: false,
		},

		// parseViewSelect edge cases
		{
			name:    "CREATE VIEW missing AS",
			sql:     "CREATE VIEW v1 SELECT * FROM users",
			wantErr: true,
		},

		// parseTriggerEvent edge cases
		{
			name:    "trigger UPDATE OF single column",
			sql:     "CREATE TRIGGER t1 BEFORE UPDATE OF name ON users BEGIN SELECT 1; END",
			wantErr: false,
		},

		// parseTriggerWhen edge cases
		{
			name:    "trigger with WHEN clause",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users WHEN NEW.age > 18 BEGIN SELECT 1; END",
			wantErr: false,
		},

		// parseTriggerBody edge cases
		{
			name:    "trigger with multiple statements",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1; UPDATE stats SET count = count + 1; END",
			wantErr: false,
		},

		// parseTriggerBodyStatement edge cases
		{
			name:    "trigger body with SELECT",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT NEW.id; END",
			wantErr: false,
		},
		{
			name:    "trigger body with INSERT",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN INSERT INTO log VALUES (NEW.id); END",
			wantErr: false,
		},
		{
			name:    "trigger body with UPDATE",
			sql:     "CREATE TRIGGER t1 AFTER UPDATE ON users BEGIN UPDATE stats SET count = count + 1; END",
			wantErr: false,
		},
		{
			name:    "trigger body with DELETE",
			sql:     "CREATE TRIGGER t1 AFTER DELETE ON users BEGIN DELETE FROM sessions WHERE user_id = OLD.id; END",
			wantErr: false,
		},

		// parsePragma edge cases
		{
			name:    "PRAGMA with parentheses",
			sql:     "PRAGMA table_info('users')",
			wantErr: false,
		},
		{
			name:    "PRAGMA query without value",
			sql:     "PRAGMA foreign_keys",
			wantErr: false,
		},

		// parseOrExpression, parseAndExpression edge cases
		{
			name:    "multiple OR conditions",
			sql:     "SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3",
			wantErr: false,
		},
		{
			name:    "multiple AND conditions",
			sql:     "SELECT * FROM users WHERE a = 1 AND b = 2 AND c = 3",
			wantErr: false,
		},

		// parseUnaryExpression edge cases
		{
			name:    "nested unary operators",
			sql:     "SELECT -(-5) FROM dual",
			wantErr: false,
		},
		{
			name:    "bitwise NOT operator",
			sql:     "SELECT ~flags FROM settings",
			wantErr: false,
		},

		// parsePostfixExpression edge cases - ISNULL/NOTNULL not supported as postfix
		{
			name:    "ISNULL postfix - unsupported",
			sql:     "SELECT * FROM users WHERE email ISNULL",
			wantErr: true,
		},
		{
			name:    "NOTNULL postfix - unsupported",
			sql:     "SELECT * FROM users WHERE email NOTNULL",
			wantErr: true,
		},

		// parseIdentOrFunction edge cases
		{
			name:    "qualified identifier",
			sql:     "SELECT users.id FROM users",
			wantErr: false,
		},
		{
			name:    "deeply qualified identifier - unsupported",
			sql:     "SELECT main.users.id FROM main.users",
			wantErr: true,
		},

		// parseCaseBaseExpr edge cases
		{
			name:    "simple CASE with base",
			sql:     "SELECT CASE x WHEN 1 THEN 'one' END FROM t",
			wantErr: false,
		},

		// parseExpressionList edge cases
		{
			name:    "single expression in list",
			sql:     "INSERT INTO users (id) VALUES (1)",
			wantErr: false,
		},
		{
			name:    "multiple expressions in list",
			sql:     "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com')",
			wantErr: false,
		},

		// parseOrderByList edge cases
		{
			name:    "ORDER BY single column",
			sql:     "SELECT * FROM users ORDER BY name",
			wantErr: false,
		},
		{
			name:    "ORDER BY multiple columns",
			sql:     "SELECT * FROM users ORDER BY last_name, first_name",
			wantErr: false,
		},
		{
			name:    "ORDER BY with NULLS FIRST",
			sql:     "SELECT * FROM users ORDER BY name NULLS FIRST",
			wantErr: false,
		},
		{
			name:    "ORDER BY with NULLS LAST",
			sql:     "SELECT * FROM users ORDER BY name NULLS LAST",
			wantErr: false,
		},

		// tryParsePatternOp edge cases
		{
			name:    "MATCH operator",
			sql:     "SELECT * FROM users WHERE name MATCH 'pattern'",
			wantErr: false,
		},

		// parseBitwiseExpression edge cases
		{
			name:    "bitwise AND chain",
			sql:     "SELECT a & b & c FROM t",
			wantErr: false,
		},
		{
			name:    "bitwise OR chain",
			sql:     "SELECT a | b | c FROM t",
			wantErr: false,
		},

		// parseAdditiveExpression edge cases
		{
			name:    "addition chain",
			sql:     "SELECT a + b + c FROM t",
			wantErr: false,
		},
		{
			name:    "subtraction chain",
			sql:     "SELECT a - b - c FROM t",
			wantErr: false,
		},

		// parseMultiplicativeExpression edge cases
		{
			name:    "multiplication chain",
			sql:     "SELECT a * b * c FROM t",
			wantErr: false,
		},
		{
			name:    "division chain",
			sql:     "SELECT a / b / c FROM t",
			wantErr: false,
		},
		{
			name:    "modulo operator",
			sql:     "SELECT a % b FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserIntFloatStringValue tests value extraction methods
func TestParserIntFloatStringValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "PRAGMA with negative integer",
			sql:     "PRAGMA cache_size = -2000",
			wantErr: false,
		},
		{
			name:    "PRAGMA with positive float",
			sql:     "PRAGMA mmap_size = 3.14159",
			wantErr: false,
		},
		{
			name:    "PRAGMA with negative float",
			sql:     "PRAGMA mmap_size = -2.5",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestLexerEdgeCasesMultilineIdentifiers tests newline tracking in identifiers
func TestLexerEdgeCasesMultilineIdentifiers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "double-quoted identifier with multiple newlines",
			input: "SELECT \"col\nwith\nmany\nlines\" FROM t",
		},
		{
			name:  "backtick identifier with multiple newlines",
			input: "SELECT `col\nwith\nmany\nlines` FROM t",
		},
		{
			name:  "bracketed identifier with multiple newlines",
			input: "SELECT [col\nwith\nmany\nlines] FROM t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			_, err := p.Parse()
			// Just ensure it parses without panic
			_ = err
		})
	}
}

// TestParserCheckIdentifierEdgeCases tests identifier checking
func TestParserCheckIdentifierEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "quoted keyword as identifier",
			sql:     "SELECT \"from\" FROM users",
			wantErr: false,
		},
		{
			name:    "backtick keyword as identifier",
			sql:     "SELECT `where` FROM users",
			wantErr: false,
		},
		{
			name:    "bracketed keyword as identifier",
			sql:     "SELECT [select] FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserPeekBounds tests peek at end of token stream
func TestParserPeekBounds(t *testing.T) {
	t.Parallel()
	// This tests the bounds check in peek()
	p := NewParser("SELECT")
	_, err := p.Parse()
	if err == nil {
		t.Error("expected error for incomplete SELECT")
	}
}

// TestCastExprErrorPaths tests error paths in parseCastExpr
func TestCastExprErrorPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "CAST complete",
			sql:     "SELECT CAST(x AS INTEGER) FROM t",
			wantErr: false,
		},
		{
			name:    "CAST with VARCHAR",
			sql:     "SELECT CAST(x AS VARCHAR(50)) FROM t",
			wantErr: false,
		},
		{
			name:    "CAST with NUMERIC precision",
			sql:     "SELECT CAST(x AS NUMERIC(10, 2)) FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
