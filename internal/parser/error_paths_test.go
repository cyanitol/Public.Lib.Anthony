// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Test error paths and edge cases to improve coverage

// assertParseSucceeds checks that the given SQL parses without error and produces at least one statement
// (unless the input is empty or only semicolons).
func assertParseSucceeds(t *testing.T, input string) {
	t.Helper()
	p := NewParser(input)
	stmts, err := p.Parse()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if len(stmts) == 0 && input != "" && input != ";" && input != ";;;" {
		t.Error("expected at least one statement")
	}
}

func TestParserErrorPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		// These should parse successfully
		{"empty input", ""},
		{"just semicolon", ";"},
		{"multiple semicolons", ";;;"},
		{"SELECT with error recovery", "SELECT * FROM users; SELECT * FROM orders"},

		// Edge cases that should parse
		{"SELECT without FROM", "SELECT 1"},
		{"SELECT with complex WHERE", "SELECT * FROM t WHERE a AND b OR c"},
		{"INSERT with multiple rows", "INSERT INTO t VALUES (1), (2), (3), (4), (5)"},
		{"UPDATE with complex SET", "UPDATE t SET a = b + c, d = e * f WHERE id = 1"},
		{"CREATE TABLE AS SELECT", "CREATE TABLE new_table AS SELECT * FROM old_table"},
		{"CREATE TEMP TABLE", "CREATE TEMP TABLE t (id INT)"},
		{"CREATE TEMPORARY TABLE", "CREATE TEMPORARY TABLE t (id INT)"},
		{"DROP VIEW", "DROP VIEW v"},
		{"DROP VIEW IF EXISTS", "DROP VIEW IF EXISTS v"},
		{"CREATE VIEW", "CREATE VIEW v AS SELECT * FROM t"},
		{"CREATE VIEW IF NOT EXISTS", "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t"},
		{"CREATE TEMPORARY VIEW", "CREATE TEMP VIEW v AS SELECT * FROM t"},
		{"CREATE VIEW with columns", "CREATE VIEW v (a, b, c) AS SELECT x, y, z FROM t"},

		// INDEXED BY
		{"SELECT with INDEXED BY", "SELECT * FROM t INDEXED BY idx WHERE id = 1"},

		// Complex JOIN
		{"Multiple joins", "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"},
		{"JOIN with complex ON", "SELECT * FROM a JOIN b ON a.x = b.x AND a.y = b.y"},
		{"JOIN with USING multiple columns", "SELECT * FROM a JOIN b USING (id, type)"},

		// Subqueries
		{"Scalar subquery", "SELECT (SELECT COUNT(*) FROM orders) AS cnt FROM users"},
		{"Correlated subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = users.id)"},

		// UPSERT / ON CONFLICT
		{"INSERT with ON CONFLICT DO NOTHING", "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING"},
		{"INSERT with ON CONFLICT DO UPDATE", "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = excluded.name"},
		{"INSERT with ON CONFLICT WHERE", "INSERT INTO t VALUES (1) ON CONFLICT (id) WHERE active = 1 DO NOTHING"},

		// CTE
		{"WITH clause", "WITH cte AS (SELECT * FROM t) SELECT * FROM cte"},
		{"WITH RECURSIVE", "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"},
		{"Multiple CTEs", "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a JOIN b"},
		{"CTE with column list", "WITH cte (x, y, z) AS (SELECT a, b, c FROM t) SELECT * FROM cte"},

		// EXPLAIN
		{"EXPLAIN", "EXPLAIN SELECT * FROM t"},
		{"EXPLAIN QUERY PLAN", "EXPLAIN QUERY PLAN SELECT * FROM t"},

		// PRAGMA
		{"PRAGMA simple", "PRAGMA cache_size"},
		{"PRAGMA with value", "PRAGMA cache_size = 2000"},
		{"PRAGMA with function call syntax", "PRAGMA cache_size(2000)"},
		{"PRAGMA with schema", "PRAGMA main.cache_size"},

		// ATTACH/DETACH
		{"ATTACH DATABASE", "ATTACH DATABASE 'file.db' AS db2"},
		{"ATTACH with expression", "ATTACH 'file.db' AS db2"},
		{"DETACH DATABASE", "DETACH DATABASE db2"},
		{"DETACH simple", "DETACH db2"},

		// VACUUM
		{"VACUUM INTO", "VACUUM INTO 'backup.db'"},
		{"VACUUM schema INTO", "VACUUM main INTO 'backup.db'"},

		// ALTER TABLE
		{"ALTER TABLE RENAME TO", "ALTER TABLE old_name RENAME TO new_name"},
		{"ALTER TABLE RENAME COLUMN", "ALTER TABLE t RENAME COLUMN old_col TO new_col"},
		{"ALTER TABLE ADD COLUMN", "ALTER TABLE t ADD COLUMN new_col TEXT"},
		{"ALTER TABLE ADD with keyword", "ALTER TABLE t ADD new_col TEXT"},
		{"ALTER TABLE DROP COLUMN", "ALTER TABLE t DROP COLUMN old_col"},

		// CREATE TRIGGER
		{"CREATE TRIGGER BEFORE INSERT", "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END"},
		{"CREATE TRIGGER AFTER UPDATE", "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN SELECT 1; END"},
		{"CREATE TRIGGER AFTER DELETE", "CREATE TRIGGER tr AFTER DELETE ON t BEGIN SELECT 1; END"},
		{"CREATE TRIGGER INSTEAD OF", "CREATE TRIGGER tr INSTEAD OF INSERT ON v BEGIN SELECT 1; END"},
		{"CREATE TRIGGER UPDATE OF", "CREATE TRIGGER tr AFTER UPDATE OF col1, col2 ON t BEGIN SELECT 1; END"},
		{"CREATE TRIGGER FOR EACH ROW", "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END"},
		{"CREATE TRIGGER WHEN", "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.id > 0 BEGIN SELECT 1; END"},
		{"CREATE TRIGGER IF NOT EXISTS", "CREATE TRIGGER IF NOT EXISTS tr AFTER INSERT ON t BEGIN SELECT 1; END"},
		{"CREATE TEMP TRIGGER", "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END"},
		{"DROP TRIGGER", "DROP TRIGGER tr"},
		{"DROP TRIGGER IF EXISTS", "DROP TRIGGER IF EXISTS tr"},

		// Foreign Key
		{"CREATE TABLE with FOREIGN KEY", "CREATE TABLE t (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertParseSucceeds(t, tt.input)
		})
	}
}
