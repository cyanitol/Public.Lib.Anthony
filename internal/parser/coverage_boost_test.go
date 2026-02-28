package parser

import (
	"testing"
)

// TestCastExpressionParsing tests CAST expressions to achieve 100% coverage of parseCastExpr
func TestCastExpressionParsing(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "CAST to INTEGER",
			sql:     "SELECT CAST(age AS INTEGER) FROM users",
			wantErr: false,
		},
		{
			name:    "CAST to TEXT",
			sql:     "SELECT CAST(id AS TEXT) FROM users",
			wantErr: false,
		},
		{
			name:    "CAST to REAL",
			sql:     "SELECT CAST(value AS REAL) FROM data",
			wantErr: false,
		},
		{
			name:    "CAST with complex expression",
			sql:     "SELECT CAST(x + y AS INTEGER) FROM calc",
			wantErr: false,
		},
		{
			name:    "CAST in WHERE clause",
			sql:     "SELECT * FROM users WHERE CAST(age AS TEXT) = '25'",
			wantErr: false,
		},
		{
			name:    "CAST missing opening paren",
			sql:     "SELECT CAST age AS INTEGER) FROM users",
			wantErr: true,
		},
		{
			name:    "CAST missing AS keyword",
			sql:     "SELECT CAST(age INTEGER) FROM users",
			wantErr: true,
		},
		{
			name:    "CAST missing type name",
			sql:     "SELECT CAST(age AS) FROM users",
			wantErr: true,
		},
		{
			name:    "CAST missing closing paren",
			sql:     "SELECT CAST(age AS INTEGER FROM users",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestLexerIdentifiersWithNewlines tests identifiers containing newlines
func TestLexerIdentifiersWithNewlines(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantID string
	}{
		{
			name:   "double quoted identifier with newline",
			input:  "\"table\nname\"",
			wantID: "\"table\nname\"",
		},
		{
			name:   "backtick identifier with newline",
			input:  "`column\nname`",
			wantID: "`column\nname`",
		},
		{
			name:   "bracketed identifier with newline",
			input:  "[field\nname]",
			wantID: "[field\nname]",
		},
		{
			name:   "double quoted with multiple newlines",
			input:  "\"multi\nline\nidentifier\"",
			wantID: "\"multi\nline\nidentifier\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_ID {
				t.Errorf("got type %s, want TK_ID", tok.Type)
			}
			if tok.Lexeme != tt.wantID {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.wantID)
			}
		})
	}
}

// TestTokenizeAllWithIllegalToken tests the error path in TokenizeAll
func TestTokenizeAllWithIllegalToken(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "illegal character",
			input:   "SELECT ^ FROM users",
			wantErr: true,
		},
		{
			name:    "unicode special character",
			input:   "SELECT ™ FROM users",
			wantErr: true,
		},
		{
			name:    "valid SQL no error",
			input:   "SELECT * FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := TokenizeAll(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("TokenizeAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserLowCoverageFunctions tests various parser functions with low coverage
func TestParserLowCoverageFunctions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// parseUpdateClauses edge cases
		{
			name:    "UPDATE without WHERE",
			sql:     "UPDATE users SET name = 'John'",
			wantErr: false,
		},
		{
			name:    "UPDATE with ORDER BY and LIMIT",
			sql:     "UPDATE users SET age = age + 1 ORDER BY name LIMIT 10",
			wantErr: false,
		},

		// parseDeleteClauses edge cases
		{
			name:    "DELETE without WHERE",
			sql:     "DELETE FROM users",
			wantErr: false,
		},
		{
			name:    "DELETE with ORDER BY and LIMIT",
			sql:     "DELETE FROM users ORDER BY created_at LIMIT 5",
			wantErr: false,
		},

		// parseCreateTable edge cases
		{
			name:    "CREATE TABLE without columns (should fail)",
			sql:     "CREATE TABLE users",
			wantErr: true,
		},
		{
			name:    "CREATE TABLE AS SELECT",
			sql:     "CREATE TABLE new_users AS SELECT * FROM users",
			wantErr: false,
		},

		// parseColumnOrConstraint edge cases
		{
			name:    "CREATE TABLE with PRIMARY KEY constraint",
			sql:     "CREATE TABLE users (id INTEGER, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE with UNIQUE constraint",
			sql:     "CREATE TABLE users (id INTEGER, UNIQUE (id))",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE with CHECK constraint",
			sql:     "CREATE TABLE users (age INTEGER, CHECK (age > 0))",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE with FOREIGN KEY",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))",
			wantErr: false,
		},

		// parseColumnConstraint edge cases
		{
			name:    "column with DEFAULT value",
			sql:     "CREATE TABLE users (status TEXT DEFAULT 'active')",
			wantErr: false,
		},
		{
			name:    "column with COLLATE",
			sql:     "CREATE TABLE users (name TEXT COLLATE NOCASE)",
			wantErr: false,
		},
		{
			name:    "column with CHECK constraint",
			sql:     "CREATE TABLE users (age INTEGER CHECK (age > 0))",
			wantErr: false,
		},

		// parseCreateIndex edge cases
		{
			name:    "CREATE INDEX without IF NOT EXISTS",
			sql:     "CREATE INDEX idx_name ON users (name)",
			wantErr: false,
		},
		{
			name:    "CREATE UNIQUE INDEX",
			sql:     "CREATE UNIQUE INDEX idx_email ON users (email)",
			wantErr: false,
		},
		{
			name:    "CREATE INDEX with WHERE clause",
			sql:     "CREATE INDEX idx_active ON users (name) WHERE active = 1",
			wantErr: false,
		},

		// parseCTE edge cases
		{
			name:    "CTE with column list",
			sql:     "WITH cte (id, name) AS (SELECT id, name FROM users) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "CTE without column list",
			sql:     "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
			wantErr: false,
		},

		// parseSubquery edge cases
		{
			name:    "subquery in FROM",
			sql:     "SELECT * FROM (SELECT * FROM users) AS u",
			wantErr: false,
		},
		{
			name:    "subquery in WHERE",
			sql:     "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantErr: false,
		},

		// parseJoinClause edge cases
		{
			name:    "INNER JOIN",
			sql:     "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "LEFT OUTER JOIN",
			sql:     "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id",
			wantErr: false,
		},
		{
			name:    "CROSS JOIN",
			sql:     "SELECT * FROM users CROSS JOIN settings",
			wantErr: false,
		},
		{
			name:    "JOIN with USING",
			sql:     "SELECT * FROM users JOIN orders USING (user_id)",
			wantErr: false,
		},

		// parseInsert edge cases
		{
			name:    "INSERT with column list",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John')",
			wantErr: false,
		},
		{
			name:    "INSERT without column list",
			sql:     "INSERT INTO users VALUES (1, 'John')",
			wantErr: false,
		},
		{
			name:    "INSERT with SELECT",
			sql:     "INSERT INTO new_users SELECT * FROM old_users",
			wantErr: false,
		},

		// parseCompoundSelect edge cases
		{
			name:    "UNION",
			sql:     "SELECT * FROM users UNION SELECT * FROM admins",
			wantErr: false,
		},
		{
			name:    "UNION ALL",
			sql:     "SELECT * FROM users UNION ALL SELECT * FROM admins",
			wantErr: false,
		},
		{
			name:    "EXCEPT",
			sql:     "SELECT * FROM users EXCEPT SELECT * FROM banned",
			wantErr: false,
		},
		{
			name:    "INTERSECT",
			sql:     "SELECT * FROM users INTERSECT SELECT * FROM active",
			wantErr: false,
		},

		// parseExpression edge cases
		{
			name:    "NOT expression",
			sql:     "SELECT * FROM users WHERE NOT active",
			wantErr: false,
		},
		{
			name:    "BETWEEN expression",
			sql:     "SELECT * FROM users WHERE age BETWEEN 18 AND 65",
			wantErr: false,
		},
		{
			name:    "NOT BETWEEN",
			sql:     "SELECT * FROM users WHERE age NOT BETWEEN 0 AND 17",
			wantErr: false,
		},
		{
			name:    "IN expression with list",
			sql:     "SELECT * FROM users WHERE status IN ('active', 'pending')",
			wantErr: false,
		},
		{
			name:    "NOT IN",
			sql:     "SELECT * FROM users WHERE status NOT IN ('banned', 'deleted')",
			wantErr: false,
		},
		{
			name:    "IS NULL",
			sql:     "SELECT * FROM users WHERE deleted_at IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT NULL",
			sql:     "SELECT * FROM users WHERE email IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "LIKE pattern",
			sql:     "SELECT * FROM users WHERE name LIKE 'John%'",
			wantErr: false,
		},
		{
			name:    "NOT LIKE - unsupported, expecting error",
			sql:     "SELECT * FROM users WHERE name NOT LIKE 'Admin%'",
			wantErr: true,
		},
		{
			name:    "GLOB pattern",
			sql:     "SELECT * FROM users WHERE name GLOB 'J*'",
			wantErr: false,
		},
		{
			name:    "REGEXP pattern",
			sql:     "SELECT * FROM users WHERE email REGEXP '^[a-z]+@'",
			wantErr: false,
		},

		// parseFunctionCall edge cases
		{
			name:    "function with DISTINCT",
			sql:     "SELECT COUNT(DISTINCT email) FROM users",
			wantErr: false,
		},
		{
			name:    "function with *",
			sql:     "SELECT COUNT(*) FROM users",
			wantErr: false,
		},
		{
			name:    "function with FILTER",
			sql:     "SELECT COUNT(*) FILTER (WHERE active = 1) FROM users",
			wantErr: false,
		},

		// parseCaseExpr edge cases
		{
			name:    "CASE with base expression",
			sql:     "SELECT CASE status WHEN 'active' THEN 1 ELSE 0 END FROM users",
			wantErr: false,
		},
		{
			name:    "CASE without base expression",
			sql:     "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
			wantErr: false,
		},
		{
			name:    "CASE without ELSE",
			sql:     "SELECT CASE WHEN active THEN 1 END FROM users",
			wantErr: false,
		},

		// parsePragma edge cases
		{
			name:    "PRAGMA query",
			sql:     "PRAGMA table_info(users)",
			wantErr: false,
		},
		{
			name:    "PRAGMA set value",
			sql:     "PRAGMA foreign_keys = ON",
			wantErr: false,
		},
		{
			name:    "PRAGMA with schema",
			sql:     "PRAGMA main.table_list",
			wantErr: false,
		},

		// parseCreateTrigger edge cases
		{
			name:    "CREATE TRIGGER BEFORE INSERT",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "CREATE TRIGGER AFTER UPDATE",
			sql:     "CREATE TRIGGER t2 AFTER UPDATE ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "CREATE TRIGGER INSTEAD OF DELETE",
			sql:     "CREATE TRIGGER t3 INSTEAD OF DELETE ON view1 BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "CREATE TRIGGER with FOR EACH ROW",
			sql:     "CREATE TRIGGER t4 BEFORE INSERT ON users FOR EACH ROW BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "CREATE TRIGGER with WHEN",
			sql:     "CREATE TRIGGER t5 BEFORE INSERT ON users WHEN NEW.age > 18 BEGIN SELECT 1; END",
			wantErr: false,
		},

		// parseTableAlias edge cases
		{
			name:    "table with AS alias",
			sql:     "SELECT * FROM users AS u",
			wantErr: false,
		},
		{
			name:    "table with implicit alias",
			sql:     "SELECT * FROM users u",
			wantErr: false,
		},

		// parseOptionalTypeName edge cases
		{
			name:    "column without type",
			sql:     "CREATE TABLE users (id)",
			wantErr: false,
		},

		// parseWhenClause edge cases
		{
			name:    "CASE with multiple WHEN",
			sql:     "SELECT CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END FROM t",
			wantErr: false,
		},

		// parseConflictTarget edge cases
		{
			name:    "UPSERT with conflict target",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
			wantErr: false,
		},
		{
			name:    "UPSERT with DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING",
			wantErr: false,
		},

		// parseForeignKeyReferences edge cases
		{
			name:    "FOREIGN KEY with column list",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users (id))",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY without column list",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserExpressionEdgeCases tests expression parsing edge cases
func TestParserExpressionEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "unary minus",
			sql:     "SELECT -5 FROM users",
			wantErr: false,
		},
		{
			name:    "unary plus",
			sql:     "SELECT +5 FROM users",
			wantErr: false,
		},
		{
			name:    "unary NOT",
			sql:     "SELECT * FROM users WHERE NOT active",
			wantErr: false,
		},
		{
			name:    "bitwise NOT",
			sql:     "SELECT ~flags FROM users",
			wantErr: false,
		},
		{
			name:    "bitwise AND",
			sql:     "SELECT flags & 0x0F FROM users",
			wantErr: false,
		},
		{
			name:    "bitwise OR",
			sql:     "SELECT flags | 0x0F FROM users",
			wantErr: false,
		},
		{
			name:    "left shift",
			sql:     "SELECT value << 2 FROM data",
			wantErr: false,
		},
		{
			name:    "right shift",
			sql:     "SELECT value >> 2 FROM data",
			wantErr: false,
		},
		{
			name:    "string concatenation",
			sql:     "SELECT first_name || ' ' || last_name FROM users",
			wantErr: false,
		},
		{
			name:    "COLLATE in expression",
			sql:     "SELECT * FROM users WHERE name = 'John' COLLATE NOCASE",
			wantErr: false,
		},
		{
			name:    "EXISTS subquery - unsupported",
			sql:     "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			wantErr: true,
		},
		{
			name:    "NOT EXISTS subquery - unsupported",
			sql:     "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			wantErr: true,
		},
		{
			name:    "parenthesized expression",
			sql:     "SELECT (age + 1) * 2 FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserGroupByHaving tests GROUP BY with HAVING clause
func TestParserGroupByHaving(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "GROUP BY with HAVING",
			sql:     "SELECT department, COUNT(*) FROM users GROUP BY department HAVING COUNT(*) > 5",
			wantErr: false,
		},
		{
			name:    "GROUP BY without HAVING",
			sql:     "SELECT department, COUNT(*) FROM users GROUP BY department",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserLimitOffset tests LIMIT with OFFSET clause
func TestParserLimitOffset(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "LIMIT with OFFSET",
			sql:     "SELECT * FROM users LIMIT 10 OFFSET 20",
			wantErr: false,
		},
		{
			name:    "LIMIT with comma syntax",
			sql:     "SELECT * FROM users LIMIT 20, 10",
			wantErr: false,
		},
		{
			name:    "LIMIT without OFFSET",
			sql:     "SELECT * FROM users LIMIT 10",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserAlias tests alias parsing edge cases
func TestParserAlias(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "column alias with AS",
			sql:     "SELECT name AS n FROM users",
			wantErr: false,
		},
		{
			name:    "column alias without AS",
			sql:     "SELECT name n FROM users",
			wantErr: false,
		},
		{
			name:    "column alias with keyword",
			sql:     "SELECT name user FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserConstraintNames tests named constraints
func TestParserConstraintNames(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "named PRIMARY KEY constraint",
			sql:     "CREATE TABLE users (id INTEGER, CONSTRAINT pk_users PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "named UNIQUE constraint",
			sql:     "CREATE TABLE users (email TEXT, CONSTRAINT uk_email UNIQUE (email))",
			wantErr: false,
		},
		{
			name:    "named CHECK constraint",
			sql:     "CREATE TABLE users (age INTEGER, CONSTRAINT chk_age CHECK (age > 0))",
			wantErr: false,
		},
		{
			name:    "named FOREIGN KEY constraint",
			sql:     "CREATE TABLE orders (user_id INTEGER, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserIndexedColumns tests indexed column variations
func TestParserIndexedColumns(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "index with ASC",
			sql:     "CREATE INDEX idx ON users (name ASC)",
			wantErr: false,
		},
		{
			name:    "index with DESC",
			sql:     "CREATE INDEX idx ON users (name DESC)",
			wantErr: false,
		},
		{
			name:    "index with multiple columns",
			sql:     "CREATE INDEX idx ON users (last_name, first_name)",
			wantErr: false,
		},
		{
			name:    "index with expression - unsupported",
			sql:     "CREATE INDEX idx ON users (LOWER(email))",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserPragmaValue tests PRAGMA value variations
func TestParserPragmaValue(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "PRAGMA with integer value",
			sql:     "PRAGMA cache_size = 2000",
			wantErr: false,
		},
		{
			name:    "PRAGMA with string value",
			sql:     "PRAGMA encoding = 'UTF-8'",
			wantErr: false,
		},
		{
			name:    "PRAGMA with identifier value",
			sql:     "PRAGMA synchronous = FULL",
			wantErr: false,
		},
		{
			name:    "PRAGMA with ON value",
			sql:     "PRAGMA foreign_keys = ON",
			wantErr: false,
		},
		{
			name:    "PRAGMA with OFF value",
			sql:     "PRAGMA case_sensitive_like = OFF",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParserTriggerEvent tests trigger event variations
func TestParserTriggerEvent(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "trigger on INSERT",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger on UPDATE",
			sql:     "CREATE TRIGGER t2 BEFORE UPDATE ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger on UPDATE OF columns",
			sql:     "CREATE TRIGGER t3 BEFORE UPDATE OF name, email ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger on DELETE",
			sql:     "CREATE TRIGGER t4 BEFORE DELETE ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
