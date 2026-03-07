// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// Test error paths and edge cases to improve coverage

func TestParserErrorPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// These should parse successfully
		{"empty input", "", false},
		{"just semicolon", ";", false},
		{"multiple semicolons", ";;;", false},
		{"SELECT with error recovery", "SELECT * FROM users; SELECT * FROM orders", false},

		// Edge cases that should parse
		{"SELECT without FROM", "SELECT 1", false},
		{"SELECT with complex WHERE", "SELECT * FROM t WHERE a AND b OR c", false},
		{"INSERT with multiple rows", "INSERT INTO t VALUES (1), (2), (3), (4), (5)", false},
		{"UPDATE with complex SET", "UPDATE t SET a = b + c, d = e * f WHERE id = 1", false},
		{"CREATE TABLE AS SELECT", "CREATE TABLE new_table AS SELECT * FROM old_table", false},
		{"CREATE TEMP TABLE", "CREATE TEMP TABLE t (id INT)", false},
		{"CREATE TEMPORARY TABLE", "CREATE TEMPORARY TABLE t (id INT)", false},
		{"DROP VIEW", "DROP VIEW v", false},
		{"DROP VIEW IF EXISTS", "DROP VIEW IF EXISTS v", false},
		{"CREATE VIEW", "CREATE VIEW v AS SELECT * FROM t", false},
		{"CREATE VIEW IF NOT EXISTS", "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t", false},
		{"CREATE TEMPORARY VIEW", "CREATE TEMP VIEW v AS SELECT * FROM t", false},
		{"CREATE VIEW with columns", "CREATE VIEW v (a, b, c) AS SELECT x, y, z FROM t", false},

		// INDEXED BY
		{"SELECT with INDEXED BY", "SELECT * FROM t INDEXED BY idx WHERE id = 1", false},

		// Complex JOIN
		{"Multiple joins", "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id", false},
		{"JOIN with complex ON", "SELECT * FROM a JOIN b ON a.x = b.x AND a.y = b.y", false},
		{"JOIN with USING multiple columns", "SELECT * FROM a JOIN b USING (id, type)", false},

		// Subqueries
		{"Scalar subquery", "SELECT (SELECT COUNT(*) FROM orders) AS cnt FROM users", false},
		{"Correlated subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = users.id)", false},

		// UPSERT / ON CONFLICT
		{"INSERT with ON CONFLICT DO NOTHING", "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING", false},
		{"INSERT with ON CONFLICT DO UPDATE", "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = excluded.name", false},
		{"INSERT with ON CONFLICT WHERE", "INSERT INTO t VALUES (1) ON CONFLICT (id) WHERE active = 1 DO NOTHING", false},

		// CTE
		{"WITH clause", "WITH cte AS (SELECT * FROM t) SELECT * FROM cte", false},
		{"WITH RECURSIVE", "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte", false},
		{"Multiple CTEs", "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) SELECT * FROM a JOIN b", false},
		{"CTE with column list", "WITH cte (x, y, z) AS (SELECT a, b, c FROM t) SELECT * FROM cte", false},

		// EXPLAIN
		{"EXPLAIN", "EXPLAIN SELECT * FROM t", false},
		{"EXPLAIN QUERY PLAN", "EXPLAIN QUERY PLAN SELECT * FROM t", false},

		// PRAGMA
		{"PRAGMA simple", "PRAGMA cache_size", false},
		{"PRAGMA with value", "PRAGMA cache_size = 2000", false},
		{"PRAGMA with function call syntax", "PRAGMA cache_size(2000)", false},
		{"PRAGMA with schema", "PRAGMA main.cache_size", false},

		// ATTACH/DETACH
		{"ATTACH DATABASE", "ATTACH DATABASE 'file.db' AS db2", false},
		{"ATTACH with expression", "ATTACH 'file.db' AS db2", false},
		{"DETACH DATABASE", "DETACH DATABASE db2", false},
		{"DETACH simple", "DETACH db2", false},

		// VACUUM
		{"VACUUM INTO", "VACUUM INTO 'backup.db'", false},
		{"VACUUM schema INTO", "VACUUM main INTO 'backup.db'", false},

		// ALTER TABLE
		{"ALTER TABLE RENAME TO", "ALTER TABLE old_name RENAME TO new_name", false},
		{"ALTER TABLE RENAME COLUMN", "ALTER TABLE t RENAME COLUMN old_col TO new_col", false},
		{"ALTER TABLE ADD COLUMN", "ALTER TABLE t ADD COLUMN new_col TEXT", false},
		{"ALTER TABLE ADD with keyword", "ALTER TABLE t ADD new_col TEXT", false},
		{"ALTER TABLE DROP COLUMN", "ALTER TABLE t DROP COLUMN old_col", false},

		// CREATE TRIGGER
		{"CREATE TRIGGER BEFORE INSERT", "CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER AFTER UPDATE", "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER AFTER DELETE", "CREATE TRIGGER tr AFTER DELETE ON t BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER INSTEAD OF", "CREATE TRIGGER tr INSTEAD OF INSERT ON v BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER UPDATE OF", "CREATE TRIGGER tr AFTER UPDATE OF col1, col2 ON t BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER FOR EACH ROW", "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER WHEN", "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.id > 0 BEGIN SELECT 1; END", false},
		{"CREATE TRIGGER IF NOT EXISTS", "CREATE TRIGGER IF NOT EXISTS tr AFTER INSERT ON t BEGIN SELECT 1; END", false},
		{"CREATE TEMP TRIGGER", "CREATE TEMP TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END", false},
		{"DROP TRIGGER", "DROP TRIGGER tr", false},
		{"DROP TRIGGER IF EXISTS", "DROP TRIGGER IF EXISTS tr", false},

		// Foreign Key
		{"CREATE TABLE with FOREIGN KEY", "CREATE TABLE t (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.input)
			stmts, err := p.Parse()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if len(stmts) == 0 && tt.input != "" && tt.input != ";" && tt.input != ";;;" {
					t.Error("expected at least one statement")
				}
			}
		})
	}
}
