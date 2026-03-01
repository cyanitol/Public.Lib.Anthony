// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"strings"
	"testing"
)

// FuzzParse tests the parser with random input to ensure it doesn't panic
func FuzzParse(f *testing.F) {
	// Add seed corpus with various SQL statements
	seeds := []string{
		// Basic SELECT statements
		"SELECT * FROM t",
		"SELECT * FROM t WHERE id = 1",
		"SELECT id, name FROM t",
		"SELECT COUNT(*) FROM t",
		"SELECT DISTINCT name FROM t",

		// INSERT statements
		"INSERT INTO t VALUES (1, 'test')",
		"INSERT INTO t (id, name) VALUES (1, 'test')",
		"INSERT INTO t DEFAULT VALUES",
		"INSERT OR REPLACE INTO t VALUES (1)",

		// UPDATE statements
		"UPDATE t SET name = 'test' WHERE id = 1",
		"UPDATE t SET id = 1, name = 'test'",
		"UPDATE OR IGNORE t SET name = 'x'",

		// DELETE statements
		"DELETE FROM t WHERE id = 1",
		"DELETE FROM t",

		// CREATE TABLE statements
		"CREATE TABLE t (id INT, name TEXT)",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS t (id INT)",
		"CREATE TEMPORARY TABLE t (id INT)",
		"CREATE TABLE t (id INT, FOREIGN KEY(id) REFERENCES other(id))",
		"CREATE TABLE t (id INT CHECK(id > 0))",
		"CREATE TABLE t (id INT UNIQUE, name TEXT)",
		"CREATE TABLE t AS SELECT 1",

		// DROP statements
		"DROP TABLE t",
		"DROP TABLE IF EXISTS t",
		"DROP INDEX idx",

		// CREATE INDEX statements
		"CREATE INDEX idx ON t(id)",
		"CREATE UNIQUE INDEX idx ON t(id)",
		"CREATE INDEX IF NOT EXISTS idx ON t(id)",
		"CREATE INDEX idx ON t(id, name)",

		// ALTER TABLE statements
		"ALTER TABLE t ADD COLUMN age INT",
		"ALTER TABLE t RENAME TO new_t",
		"ALTER TABLE t RENAME COLUMN old_name TO new_name",
		"ALTER TABLE t DROP COLUMN age",

		// Queries with JOINs
		"SELECT * FROM a JOIN b ON a.id = b.id",
		"SELECT * FROM a LEFT JOIN b ON a.id = b.id",
		"SELECT * FROM a RIGHT JOIN b USING (id)",
		"SELECT * FROM a CROSS JOIN b",
		"SELECT * FROM a INNER JOIN b ON a.id = b.id",
		"SELECT * FROM a, b WHERE a.id = b.id",

		// Subqueries
		"SELECT * FROM (SELECT * FROM t)",
		"SELECT * FROM (SELECT * FROM t) AS sub",
		"SELECT * FROM t WHERE id IN (SELECT id FROM other)",
		"SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other)",

		// Aggregate functions
		"SELECT COUNT(*), SUM(id), AVG(id), MIN(id), MAX(id) FROM t",
		"SELECT name, COUNT(*) FROM t GROUP BY name",
		"SELECT name, COUNT(*) FROM t GROUP BY name HAVING COUNT(*) > 1",

		// ORDER BY and LIMIT
		"SELECT * FROM t ORDER BY id",
		"SELECT * FROM t ORDER BY id DESC",
		"SELECT * FROM t ORDER BY id, name",
		"SELECT * FROM t ORDER BY id ASC, name DESC",
		"SELECT * FROM t LIMIT 10",
		"SELECT * FROM t LIMIT 10 OFFSET 5",
		"SELECT * FROM t ORDER BY id LIMIT 10",

		// UNION, INTERSECT, EXCEPT
		"SELECT 1 UNION SELECT 2",
		"SELECT 1 UNION ALL SELECT 2",
		"SELECT 1 INTERSECT SELECT 2",
		"SELECT 1 EXCEPT SELECT 2",

		// CTEs (Common Table Expressions)
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2",
		"WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",

		// Expressions
		"SELECT 1 + 2",
		"SELECT 1 + 2 * 3",
		"SELECT (1 + 2) * 3",
		"SELECT 1 AND 2 OR 3",
		"SELECT NOT (1 = 2)",
		"SELECT 1 BETWEEN 2 AND 3",
		"SELECT 1 IN (1, 2, 3)",
		"SELECT 1 IS NULL",
		"SELECT 1 IS NOT NULL",
		"SELECT CASE WHEN 1 THEN 2 END",
		"SELECT CASE WHEN 1 THEN 2 ELSE 3 END",
		"SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END",
		"SELECT CAST(1 AS TEXT)",
		"SELECT func()",
		"SELECT func(1, 2, 3)",
		"SELECT table.col",
		"SELECT schema.table.col",

		// String literals
		"SELECT 'test'",
		"SELECT 'test''s'", // escaped quote
		"SELECT \"test\"",
		"SELECT 'line1\nline2'",
		"SELECT '\x00'",

		// Comments
		"SELECT * FROM t -- comment",
		"SELECT * FROM t /* comment */",
		"/* multi\nline\ncomment */ SELECT * FROM t",

		// PRAGMA statements
		"PRAGMA foreign_keys",
		"PRAGMA foreign_keys = ON",
		"PRAGMA table_info(t)",
		"PRAGMA schema_version",

		// Transaction statements
		"BEGIN",
		"BEGIN TRANSACTION",
		"BEGIN IMMEDIATE",
		"BEGIN EXCLUSIVE",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT sp",
		"RELEASE sp",
		"ROLLBACK TO sp",

		// VIEW statements
		"CREATE VIEW v AS SELECT * FROM t",
		"CREATE VIEW IF NOT EXISTS v AS SELECT 1",
		"DROP VIEW v",
		"DROP VIEW IF EXISTS v",

		// TRIGGER statements
		"CREATE TRIGGER trg AFTER INSERT ON t BEGIN SELECT 1; END",
		"CREATE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW BEGIN DELETE FROM other; END",
		"DROP TRIGGER trg",

		// EXPLAIN
		"EXPLAIN SELECT * FROM t",
		"EXPLAIN QUERY PLAN SELECT * FROM t",

		// UPSERT
		"INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
		"INSERT INTO t VALUES (1) ON CONFLICT(id) DO UPDATE SET name = 'x'",

		// ATTACH/DETACH
		"ATTACH DATABASE 'file.db' AS db",
		"DETACH DATABASE db",

		// VACUUM
		"VACUUM",

		// ANALYZE
		"ANALYZE",
		"ANALYZE t",

		// REINDEX
		"REINDEX",
		"REINDEX idx",

		// Empty and whitespace
		"",
		" ",
		"\n",
		"\t",

		// Nested expressions
		"SELECT (1 + (2 * (3 - 4)))",
		"SELECT * FROM (SELECT * FROM (SELECT 1))",

		// Long inputs to test boundaries
		string(make([]byte, 1000)),
		string(make([]byte, 10000)),
		strings.Repeat("SELECT * FROM t UNION ", 100) + "SELECT * FROM t",

		// Special characters
		"SELECT '\x00'",
		"SELECT '\n\r\t'",

		// Malformed SQL (should error gracefully)
		"SELECT",
		"FROM",
		"WHERE",
		"SELECT FROM",
		"SELECT * FROM",
		"INSERT INTO",
		"CREATE TABLE",
		"DROP",
		"UPDATE",
		"DELETE",
		"(",
		")",
		"SELECT * FROM (",
		"SELECT * FROM (SELECT * FROM",

		// SQL injection attempts
		"SELECT * FROM t WHERE id = 1; DROP TABLE t",
		"SELECT * FROM t WHERE name = '' OR '1'='1",
		"SELECT * FROM t WHERE name = '\\' OR 1=1--",

		// Unicode
		"SELECT '你好'",
		"SELECT 'Привет'",
		"SELECT '🔥'",

		// Deeply nested parentheses
		strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100),

		// Multiple statements
		"SELECT 1; SELECT 2; SELECT 3",

		// Window functions
		"SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t",
		"SELECT RANK() OVER (PARTITION BY name ORDER BY id) FROM t",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	// Fuzz function - should never panic
	f.Fuzz(func(t *testing.T, sql string) {
		// Skip extremely long inputs to prevent timeout
		if len(sql) > 100000 {
			t.Skip("input too long")
		}

		// Create parser
		p := NewParser(sql)

		// Parse should not panic, even with malformed input
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Parser panicked on input: %q\nPanic: %v", sql, r)
			}
		}()

		// We don't care if it returns an error, just that it doesn't panic
		_, _ = p.Parse()
	})
}

// FuzzLexer tests the lexer independently with random input
func FuzzLexer(f *testing.F) {
	seeds := []string{
		// Keywords
		"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "TABLE", "INDEX", "VIEW", "TRIGGER",

		// Literals
		"123",
		"123.456",
		"0x1234",
		"'string'",
		"\"identifier\"",
		"`identifier`",
		"[identifier]",

		// Operators
		"+", "-", "*", "/", "%",
		"=", "!=", "<>", "<", ">", "<=", ">=",
		"AND", "OR", "NOT",
		"||", "<<", ">>", "&", "|",

		// Punctuation
		";", ",", "(", ")", ".",

		// Comments
		"-- comment",
		"/* comment */",
		"/* multi\nline */",

		// Whitespace
		"", " ", "\n", "\r\n", "\t",

		// Special characters
		"\x00",
		"\x01\x02\x03",

		// Long strings
		strings.Repeat("a", 1000),
		strings.Repeat("a", 10000),

		// Mixed content
		"SELECT * FROM t WHERE id = 123",
		"'string with spaces and special chars: !@#$%^&*()'",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// Skip extremely long inputs
		if len(input) > 100000 {
			t.Skip("input too long")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Lexer panicked on input: %q\nPanic: %v", input, r)
			}
		}()

		lexer := NewLexer(input)

		// Tokenize entire input
		maxTokens := 100000 // prevent infinite loops
		count := 0
		for {
			tok := lexer.NextToken()
			count++

			if tok.Type == TK_EOF || tok.Type == TK_ILLEGAL || count > maxTokens {
				break
			}
		}
	})
}

// FuzzParseExpression tests expression parsing specifically
func FuzzParseExpression(f *testing.F) {
	seeds := []string{
		// Simple literals
		"1",
		"123.456",
		"'string'",
		"NULL",
		"TRUE",
		"FALSE",

		// Binary operations
		"1 + 2",
		"1 - 2",
		"1 * 2",
		"1 / 2",
		"1 % 2",
		"1 + 2 * 3",
		"(1 + 2) * 3",
		"1 << 2",
		"1 >> 2",
		"1 & 2",
		"1 | 2",

		// Comparisons
		"1 = 2",
		"1 != 2",
		"1 <> 2",
		"1 < 2",
		"1 > 2",
		"1 <= 2",
		"1 >= 2",

		// Logical operations
		"1 AND 2",
		"1 OR 2",
		"NOT 1",
		"1 AND 2 OR 3",

		// Special operators
		"1 BETWEEN 2 AND 3",
		"1 IN (1, 2, 3)",
		"1 NOT IN (1, 2, 3)",
		"1 IS NULL",
		"1 IS NOT NULL",
		"col LIKE 'pattern'",
		"col GLOB 'pattern'",
		"col MATCH 'pattern'",
		"col REGEXP 'pattern'",

		// Function calls
		"func()",
		"func(1)",
		"func(1, 2, 3)",
		"COUNT(*)",
		"COUNT(DISTINCT id)",
		"SUM(id)",
		"MAX(id)",

		// Column references
		"col",
		"table.col",
		"schema.table.col",

		// CASE expressions
		"CASE WHEN 1 THEN 2 END",
		"CASE WHEN 1 THEN 2 ELSE 3 END",
		"CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",

		// CAST expressions
		"CAST(1 AS TEXT)",
		"CAST('123' AS INTEGER)",

		// EXISTS subqueries
		"EXISTS (SELECT 1)",
		"NOT EXISTS (SELECT 1 FROM t)",

		// Subquery expressions
		"(SELECT 1)",
		"(SELECT id FROM t)",

		// Nested expressions
		"(1 + (2 * (3 - 4)))",
		strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100),

		// Complex expressions
		"(a + b) * (c - d) / (e + f)",
		"(x > 0 AND y < 10) OR (z = 5)",

		// String concatenation
		"'hello' || ' ' || 'world'",

		// Unary operations
		"-1",
		"+1",
		"~1",
		"NOT TRUE",

		// Malformed expressions
		"",
		"(",
		")",
		"1 +",
		"+ 1",
		"1 + + 2",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, expr string) {
		// Skip extremely long inputs
		if len(expr) > 10000 {
			t.Skip("input too long")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Expression parser panicked on: %q\nPanic: %v", expr, r)
			}
		}()

		// Try parsing as a SELECT expression
		sql := "SELECT " + expr
		p := NewParser(sql)
		_, _ = p.Parse()
	})
}

// FuzzParseTableName tests table name parsing
func FuzzParseTableName(f *testing.F) {
	seeds := []string{
		// Simple table names
		"table",
		"t",

		// Quoted identifiers
		"\"table\"",
		"`table`",
		"[table]",

		// Schema-qualified names
		"schema.table",
		"main.table",
		"\"schema\".\"table\"",

		// Aliased names
		"table AS alias",
		"table alias",
		"schema.table AS alias",

		// Special characters in names
		"table_name",
		"table123",
		"_table",

		// Malformed names
		"",
		".",
		".table",
		"schema.",
		"schema..table",

		// Long names
		strings.Repeat("a", 100),
		strings.Repeat("a", 1000),
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, table string) {
		// Skip extremely long inputs
		if len(table) > 10000 {
			t.Skip("input too long")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Table name parser panicked on: %q\nPanic: %v", table, r)
			}
		}()

		sql := "SELECT * FROM " + table
		p := NewParser(sql)
		_, _ = p.Parse()
	})
}

// FuzzParseCreateTable tests CREATE TABLE parsing
func FuzzParseCreateTable(f *testing.F) {
	seeds := []string{
		// Basic CREATE TABLE
		"CREATE TABLE t (id INT)",
		"CREATE TABLE t (id INTEGER, name TEXT)",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",

		// Table constraints
		"CREATE TABLE t (id INT, name TEXT, UNIQUE(id))",
		"CREATE TABLE t (id INT, FOREIGN KEY(id) REFERENCES other(id))",
		"CREATE TABLE t (id INT CHECK(id > 0))",
		"CREATE TABLE t (id INT, PRIMARY KEY(id))",

		// Column constraints
		"CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
		"CREATE TABLE t (id INT UNIQUE NOT NULL DEFAULT 0)",
		"CREATE TABLE t (id INT REFERENCES other(id))",
		"CREATE TABLE t (id INT CHECK(id > 0))",

		// Options
		"CREATE TEMPORARY TABLE t (id INT)",
		"CREATE TEMP TABLE t (id INT)",
		"CREATE TABLE IF NOT EXISTS t (id INT)",

		// CREATE TABLE AS
		"CREATE TABLE t AS SELECT 1",
		"CREATE TABLE t AS SELECT * FROM other",

		// Malformed CREATE TABLE
		"CREATE TABLE",
		"CREATE TABLE t",
		"CREATE TABLE t ()",
		"CREATE TABLE t (",
		"CREATE TABLE t (id",
		"CREATE TABLE t (id INT,",

		// Special data types
		"CREATE TABLE t (id INT, name TEXT, value REAL, data BLOB)",
		"CREATE TABLE t (id INTEGER(10), name VARCHAR(255))",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, ddl string) {
		// Skip extremely long inputs
		if len(ddl) > 10000 {
			t.Skip("input too long")
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("CREATE TABLE parser panicked on: %q\nPanic: %v", ddl, r)
			}
		}()

		p := NewParser(ddl)
		_, _ = p.Parse()
	})
}

// TestFuzzCorpusRegression tests against any previously found crashes
func TestFuzzCorpusRegression(t *testing.T) {
	t.Parallel()
	// This test ensures known problematic inputs don't cause panics
	regressionInputs := []string{
		// Empty and null bytes
		"",
		"\x00",
		strings.Repeat("\x00", 1000),

		// Unbalanced parentheses
		strings.Repeat("(", 1000),
		strings.Repeat(")", 1000),
		"(" + strings.Repeat("(", 100),
		strings.Repeat(")", 100) + ")",

		// Long UNION chains
		strings.Repeat("SELECT * FROM t UNION ", 1000) + "SELECT * FROM t",

		// Incomplete statements
		"SELECT * FROM (",
		"SELECT * FROM (SELECT * FROM",
		"CREATE TABLE t (",
		"INSERT INTO t VALUES (",

		// Deeply nested subqueries
		strings.Repeat("SELECT * FROM (", 50) + "SELECT 1" + strings.Repeat(")", 50),

		// Very long identifiers
		"SELECT " + strings.Repeat("a", 10000),

		// Special character sequences
		strings.Repeat("'", 1000),
		strings.Repeat("\"", 1000),
		strings.Repeat("-", 1000),
		strings.Repeat("/", 1000),
	}

	for i, input := range regressionInputs {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic on regression input %d: %v\nInput: %q", i, r, input)
				}
			}()

			p := NewParser(input)
			_, _ = p.Parse()
		})
	}
}
