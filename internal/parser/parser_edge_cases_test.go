package parser

import (
	"testing"
)

// Test parser edge cases and error paths for better coverage

func TestParserEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			"simple OR",
			"SELECT * FROM t WHERE a OR b",
			false,
		},
		{
			"multiple OR",
			"SELECT * FROM t WHERE a OR b OR c",
			false,
		},
		{
			"IS expression variations",
			"SELECT * FROM t WHERE x IS NULL",
			false,
		},
		{
			"IS NOT NULL",
			"SELECT * FROM t WHERE x IS NOT NULL",
			false,
		},
		{
			"IN with list",
			"SELECT * FROM t WHERE id IN (1, 2, 3)",
			false,
		},
		{
			"NOT IN",
			"SELECT * FROM t WHERE id NOT IN (1, 2, 3)",
			false,
		},
		{
			"BETWEEN",
			"SELECT * FROM t WHERE x BETWEEN 1 AND 10",
			false,
		},
		{
			"NOT BETWEEN",
			"SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10",
			false,
		},
		{
			"LIKE pattern",
			"SELECT * FROM t WHERE name LIKE 'test%'",
			false,
		},
		{
			"GLOB pattern",
			"SELECT * FROM t WHERE name GLOB 'test*'",
			false,
		},
		{
			"REGEXP pattern",
			"SELECT * FROM t WHERE name REGEXP '[0-9]+'",
			false,
		},
		{
			"MATCH pattern",
			"SELECT * FROM t WHERE content MATCH 'search'",
			false,
		},
		{
			"bitwise AND",
			"SELECT * FROM t WHERE flags & 1",
			false,
		},
		{
			"bitwise OR",
			"SELECT * FROM t WHERE flags | 2",
			false,
		},
		{
			"left shift",
			"SELECT * FROM t WHERE val << 2",
			false,
		},
		{
			"right shift",
			"SELECT * FROM t WHERE val >> 3",
			false,
		},
		{
			"additive expression",
			"SELECT * FROM t WHERE a + b - c",
			false,
		},
		{
			"multiplicative expression",
			"SELECT * FROM t WHERE a * b / c % d",
			false,
		},
		{
			"unary minus",
			"SELECT * FROM t WHERE -x",
			false,
		},
		{
			"unary NOT",
			"SELECT * FROM t WHERE NOT active",
			false,
		},
		{
			"COLLATE expression",
			"SELECT * FROM t WHERE name COLLATE NOCASE = 'test'",
			false,
		},
		{
			"subquery in FROM",
			"SELECT * FROM (SELECT id FROM users) AS t",
			false,
		},
		{
			"IN with subquery",
			"SELECT * FROM t WHERE id IN (SELECT user_id FROM orders)",
			false,
		},
		{
			"qualified column",
			"SELECT users.id, users.name FROM users",
			false,
		},
		{
			"SELECT DISTINCT",
			"SELECT DISTINCT name FROM users",
			false,
		},
		{
			"LIMIT and OFFSET",
			"SELECT * FROM t LIMIT 10 OFFSET 5",
			false,
		},
		{
			"ORDER BY multiple",
			"SELECT * FROM t ORDER BY name ASC, id DESC",
			false,
		},
		{
			"GROUP BY with HAVING",
			"SELECT COUNT(*) FROM t GROUP BY category HAVING COUNT(*) > 5",
			false,
		},
		{
			"LEFT JOIN",
			"SELECT * FROM a LEFT JOIN b ON a.id = b.a_id",
			false,
		},
		{
			"INNER JOIN",
			"SELECT * FROM a INNER JOIN b ON a.id = b.a_id",
			false,
		},
		{
			"CROSS JOIN",
			"SELECT * FROM a CROSS JOIN b",
			false,
		},
		{
			"JOIN with USING",
			"SELECT * FROM a JOIN b USING (id)",
			false,
		},
		{
			"UNION",
			"SELECT * FROM a UNION SELECT * FROM b",
			false,
		},
		{
			"UNION ALL",
			"SELECT * FROM a UNION ALL SELECT * FROM b",
			false,
		},
		{
			"EXCEPT",
			"SELECT * FROM a EXCEPT SELECT * FROM b",
			false,
		},
		{
			"INTERSECT",
			"SELECT * FROM a INTERSECT SELECT * FROM b",
			false,
		},
		{
			"CASE simple",
			"SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM t",
			false,
		},
		{
			"CASE with base",
			"SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t",
			false,
		},
		{
			"function with DISTINCT",
			"SELECT COUNT(DISTINCT id) FROM t",
			false,
		},
		{
			"function with FILTER",
			"SELECT COUNT(*) FILTER (WHERE active = 1) FROM t",
			false,
		},
		{
			"concatenation",
			"SELECT 'hello' || ' ' || 'world'",
			false,
		},
		{
			"parameter placeholder",
			"SELECT * FROM t WHERE id = ?",
			false,
		},
		{
			"named parameter",
			"SELECT * FROM t WHERE id = :id",
			false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			_, err := p.Parse()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestParserDMLStatements(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			"INSERT with VALUES",
			"INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
		},
		{
			"INSERT with SELECT",
			"INSERT INTO archive SELECT * FROM users WHERE active = 0",
		},
		{
			"INSERT with DEFAULT VALUES",
			"INSERT INTO users DEFAULT VALUES",
		},
		{
			"UPDATE simple",
			"UPDATE users SET name = 'Jane' WHERE id = 1",
		},
		{
			"UPDATE multiple columns",
			"UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 1",
		},
		{
			"UPDATE with ORDER BY LIMIT",
			"UPDATE users SET active = 0 ORDER BY created_at LIMIT 10",
		},
		{
			"DELETE simple",
			"DELETE FROM users WHERE id = 1",
		},
		{
			"DELETE with ORDER BY LIMIT",
			"DELETE FROM users ORDER BY created_at LIMIT 10",
		},
		{
			"DELETE all",
			"DELETE FROM users",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("Parse failed: %v", err)
			}
		})
	}
}

func TestParserDDLStatements(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			"CREATE TABLE simple",
			"CREATE TABLE users (id INTEGER, name TEXT)",
		},
		{
			"CREATE TABLE IF NOT EXISTS",
			"CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)",
		},
		{
			"CREATE TABLE with constraints",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)",
		},
		{
			"CREATE INDEX",
			"CREATE INDEX idx_name ON users (name)",
		},
		{
			"CREATE UNIQUE INDEX",
			"CREATE UNIQUE INDEX idx_email ON users (email)",
		},
		{
			"DROP TABLE",
			"DROP TABLE users",
		},
		{
			"DROP TABLE IF EXISTS",
			"DROP TABLE IF EXISTS users",
		},
		{
			"DROP INDEX",
			"DROP INDEX idx_name",
		},
		{
			"DROP INDEX IF EXISTS",
			"DROP INDEX IF EXISTS idx_name",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("Parse failed: %v", err)
			}
		})
	}
}

func TestParserUtilityStatements(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			"BEGIN",
			"BEGIN",
		},
		{
			"BEGIN TRANSACTION",
			"BEGIN TRANSACTION",
		},
		{
			"BEGIN DEFERRED",
			"BEGIN DEFERRED",
		},
		{
			"COMMIT",
			"COMMIT",
		},
		{
			"ROLLBACK",
			"ROLLBACK",
		},
		{
			"VACUUM",
			"VACUUM",
		},
		{
			"VACUUM schema",
			"VACUUM main",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("Parse failed: %v", err)
			}
		})
	}
}
