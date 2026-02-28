package parser

import (
	"testing"
)

// Comprehensive tests to maximize coverage of all parser paths

func TestParserAllSQLKeywordVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		// Comprehensive SELECT variations
		{"SELECT with *", "SELECT * FROM t"},
		{"SELECT with table.*", "SELECT users.* FROM users"},
		{"SELECT multiple columns", "SELECT id, name, email FROM users"},
		{"SELECT with aliases", "SELECT name AS user_name, id AS user_id FROM users"},
		{"SELECT with AS keyword", "SELECT name AS username FROM users"},
		{"SELECT without AS", "SELECT name username FROM users"},

		// WHERE clause variations
		{"WHERE with equality", "SELECT * FROM t WHERE id = 1"},
		{"WHERE with inequality", "SELECT * FROM t WHERE id != 1"},
		{"WHERE with <", "SELECT * FROM t WHERE id < 10"},
		{"WHERE with <=", "SELECT * FROM t WHERE id <= 10"},
		{"WHERE with >", "SELECT * FROM t WHERE id > 10"},
		{"WHERE with >=", "SELECT * FROM t WHERE id >= 10"},
		{"WHERE with AND", "SELECT * FROM t WHERE a = 1 AND b = 2"},
		{"WHERE with OR", "SELECT * FROM t WHERE a = 1 OR b = 2"},
		{"WHERE with NOT", "SELECT * FROM t WHERE NOT active"},

		// JOIN variations
		{"LEFT OUTER JOIN", "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id"},
		{"RIGHT JOIN", "SELECT * FROM a RIGHT JOIN b ON a.id = b.id"},
		{"RIGHT OUTER JOIN", "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id"},
		{"FULL JOIN", "SELECT * FROM a FULL JOIN b ON a.id = b.id"},
		{"NATURAL JOIN", "SELECT * FROM a NATURAL JOIN b"},
		{"NATURAL LEFT JOIN", "SELECT * FROM a NATURAL LEFT JOIN b"},
		{"NATURAL LEFT OUTER JOIN", "SELECT * FROM a NATURAL LEFT OUTER JOIN b"},

		// Expression variations
		{"Expression with parentheses", "SELECT (a + b) * c FROM t"},
		{"Expression with multiple ops", "SELECT a + b - c * d / e % f FROM t"},
		{"Expression with bitwise NOT", "SELECT ~flags FROM t"},

		// Function variations
		{"Function no args", "SELECT RANDOM()"},
		{"Function one arg", "SELECT ABS(x) FROM t"},
		{"Function multiple args", "SELECT SUBSTR(name, 1, 5) FROM t"},
		{"Function COUNT(*)", "SELECT COUNT(*) FROM t"},
		{"Function COUNT with column", "SELECT COUNT(id) FROM t"},
		{"Function with DISTINCT", "SELECT COUNT(DISTINCT id) FROM t"},

		// ORDER BY variations
		{"ORDER BY ASC", "SELECT * FROM t ORDER BY name ASC"},
		{"ORDER BY DESC", "SELECT * FROM t ORDER BY name DESC"},
		{"ORDER BY default", "SELECT * FROM t ORDER BY name"},
		{"ORDER BY multiple", "SELECT * FROM t ORDER BY name ASC, id DESC"},
		{"ORDER BY with COLLATE", "SELECT * FROM t ORDER BY name COLLATE NOCASE"},

		// GROUP BY variations
		{"GROUP BY simple", "SELECT category, COUNT(*) FROM t GROUP BY category"},
		{"GROUP BY multiple", "SELECT category, type, COUNT(*) FROM t GROUP BY category, type"},
		{"GROUP BY with HAVING", "SELECT category FROM t GROUP BY category HAVING COUNT(*) > 5"},

		// LIMIT and OFFSET
		{"LIMIT only", "SELECT * FROM t LIMIT 10"},
		{"LIMIT with OFFSET", "SELECT * FROM t LIMIT 10 OFFSET 5"},

		// Set operations
		{"UNION DISTINCT", "SELECT * FROM a UNION SELECT * FROM b"},
		{"UNION ALL explicit", "SELECT * FROM a UNION ALL SELECT * FROM b"},

		// CREATE TABLE variations
		{"CREATE TABLE with PRIMARY KEY", "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
		{"CREATE TABLE with AUTOINCREMENT", "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)"},
		{"CREATE TABLE with NOT NULL", "CREATE TABLE t (name TEXT NOT NULL)"},
		{"CREATE TABLE with UNIQUE", "CREATE TABLE t (email TEXT UNIQUE)"},
		{"CREATE TABLE with DEFAULT", "CREATE TABLE t (active INTEGER DEFAULT 1)"},
		{"CREATE TABLE with CHECK", "CREATE TABLE t (age INTEGER CHECK (age >= 0))"},
		{"CREATE TABLE with COLLATE", "CREATE TABLE t (name TEXT COLLATE NOCASE)"},
		{"CREATE TABLE with table constraint", "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))"},
		{"CREATE TABLE with UNIQUE constraint", "CREATE TABLE t (id INT, email TEXT, UNIQUE (email))"},
		{"CREATE TABLE with CHECK constraint", "CREATE TABLE t (age INT, CHECK (age >= 0))"},
		{"CREATE TABLE WITHOUT ROWID", "CREATE TABLE t (id TEXT PRIMARY KEY) WITHOUT ROWID"},
		{"CREATE TABLE STRICT", "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"},

		// CREATE INDEX variations
		{"CREATE INDEX IF NOT EXISTS", "CREATE INDEX IF NOT EXISTS idx ON t (col)"},
		{"CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX idx ON t (col)"},
		{"CREATE INDEX multiple columns", "CREATE INDEX idx ON t (col1, col2)"},
		{"CREATE INDEX with ASC", "CREATE INDEX idx ON t (col ASC)"},
		{"CREATE INDEX with DESC", "CREATE INDEX idx ON t (col DESC)"},
		{"CREATE INDEX with WHERE", "CREATE INDEX idx ON t (col) WHERE active = 1"},

		// INSERT variations
		{"INSERT with column list", "INSERT INTO t (a, b) VALUES (1, 2)"},
		{"INSERT multiple values", "INSERT INTO t VALUES (1, 2), (3, 4)"},
		{"INSERT OR REPLACE", "INSERT OR REPLACE INTO t VALUES (1)"},
		{"INSERT OR IGNORE", "INSERT OR IGNORE INTO t VALUES (1)"},
		{"INSERT OR ABORT", "INSERT OR ABORT INTO t VALUES (1)"},
		{"INSERT OR FAIL", "INSERT OR FAIL INTO t VALUES (1)"},
		{"INSERT OR ROLLBACK", "INSERT OR ROLLBACK INTO t VALUES (1)"},

		// UPDATE variations
		{"UPDATE with WHERE", "UPDATE t SET a = 1 WHERE id = 1"},
		{"UPDATE multiple columns", "UPDATE t SET a = 1, b = 2 WHERE id = 1"},
		{"UPDATE without WHERE", "UPDATE t SET active = 0"},
		{"UPDATE OR IGNORE", "UPDATE OR IGNORE t SET a = 1"},
		{"UPDATE OR REPLACE", "UPDATE OR REPLACE t SET a = 1"},

		// DELETE variations
		{"DELETE with WHERE", "DELETE FROM t WHERE id = 1"},
		{"DELETE without WHERE", "DELETE FROM t"},
		{"DELETE with ORDER BY", "DELETE FROM t WHERE active = 0 ORDER BY created_at"},
		{"DELETE with LIMIT", "DELETE FROM t WHERE active = 0 LIMIT 10"},

		// Transaction variations
		{"BEGIN IMMEDIATE", "BEGIN IMMEDIATE"},
		{"BEGIN EXCLUSIVE", "BEGIN EXCLUSIVE"},

		// Literal variations
		{"NULL literal", "SELECT NULL"},
		{"Integer literal", "SELECT 42"},
		{"Negative integer", "SELECT -42"},
		{"Float literal", "SELECT 3.14"},
		{"String literal", "SELECT 'hello'"},
		{"String with escaped quote", "SELECT 'it''s'"},
		{"Blob literal uppercase", "SELECT X'DEADBEEF'"},
		{"Blob literal lowercase", "SELECT x'deadbeef'"},
		{"Hex integer", "SELECT 0xABCD"},

		// Parameter placeholders
		{"? placeholder", "SELECT * FROM t WHERE id = ?"},
		{"?N placeholder", "SELECT * FROM t WHERE id = ?1"},
		{":name placeholder", "SELECT * FROM t WHERE id = :id"},
		{"@name placeholder", "SELECT * FROM t WHERE id = @id"},
		{"$name placeholder", "SELECT * FROM t WHERE id = $id"},
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

func TestParserSubqueryVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"Subquery in FROM with AS", "SELECT * FROM (SELECT id FROM users) AS t"},
		{"Subquery in FROM without AS", "SELECT * FROM (SELECT id FROM users) t"},
		{"IN with value list", "SELECT * FROM t WHERE id IN (1, 2, 3)"},
		{"IN with subquery", "SELECT * FROM t WHERE id IN (SELECT user_id FROM orders)"},
		{"NOT IN with list", "SELECT * FROM t WHERE id NOT IN (1, 2, 3)"},
		{"NOT IN with subquery", "SELECT * FROM t WHERE id NOT IN (SELECT user_id FROM orders)"},
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

func TestParserAllOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"==", "SELECT * FROM t WHERE a == b"},
		{"<>", "SELECT * FROM t WHERE a <> b"},
		{"Concatenation ||", "SELECT 'a' || 'b'"},
		{"Bitwise &", "SELECT a & b FROM t"},
		{"Bitwise |", "SELECT a | b FROM t"},
		{"Bitwise ~", "SELECT ~a FROM t"},
		{"Left shift <<", "SELECT a << 2 FROM t"},
		{"Right shift >>", "SELECT a >> 2 FROM t"},
		{"Modulo %", "SELECT a % b FROM t"},
		{"LIKE", "SELECT * FROM t WHERE name LIKE 'test%'"},
		{"GLOB", "SELECT * FROM t WHERE name GLOB 'test*'"},
		{"REGEXP", "SELECT * FROM t WHERE name REGEXP '[0-9]+'"},
		{"MATCH", "SELECT * FROM t WHERE content MATCH 'search'"},
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

func TestParserCaseExpressionVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"CASE without base", "SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM t"},
		{"CASE with base", "SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM t"},
		{"CASE without ELSE", "SELECT CASE WHEN x > 0 THEN 1 END FROM t"},
		{"CASE with multiple WHEN", "SELECT CASE WHEN x < 0 THEN -1 WHEN x > 0 THEN 1 ELSE 0 END FROM t"},
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
