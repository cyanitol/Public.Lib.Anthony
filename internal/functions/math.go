// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// TestComprehensiveParserCoverage tests edge cases for remaining low-coverage functions
func TestComprehensiveParserCoverage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// parseUpdateClauses - 57.1%
		{
			name:    "UPDATE with all clauses",
			sql:     "UPDATE users SET name = 'John' WHERE id = 1 ORDER BY name LIMIT 1",
			wantErr: false,
		},
		{
			name:    "UPDATE minimal",
			sql:     "UPDATE users SET name = 'John'",
			wantErr: false,
		},

		// parseDeleteClauses - 60.0%
		{
			name:    "DELETE with all clauses",
			sql:     "DELETE FROM users WHERE id = 1 ORDER BY name LIMIT 1",
			wantErr: false,
		},
		{
			name:    "DELETE minimal",
			sql:     "DELETE FROM users",
			wantErr: false,
		},

		// parseColumnConstraint - 61.5%
		{
			name:    "column with AUTOINCREMENT",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
			wantErr: false,
		},
		{
			name:    "column with multiple constraints",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL UNIQUE)",
			wantErr: false,
		},
		{
			name:    "column with DEFAULT expression",
			sql:     "CREATE TABLE t (created INTEGER DEFAULT (strftime('%s', 'now')))",
			wantErr: false,
		},
		{
			name:    "column with DEFAULT number",
			sql:     "CREATE TABLE t (count INTEGER DEFAULT 0)",
			wantErr: false,
		},

		// parseSubquery - 66.7%
		{
			name:    "subquery missing closing paren",
			sql:     "SELECT * FROM (SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "subquery with alias",
			sql:     "SELECT * FROM (SELECT * FROM users) AS u",
			wantErr: false,
		},

		// parseJoinUsingCondition - 66.7%
		{
			name:    "JOIN USING missing column list",
			sql:     "SELECT * FROM a JOIN b USING",
			wantErr: true,
		},
		{
			name:    "JOIN USING with columns",
			sql:     "SELECT * FROM a JOIN b USING (id)",
			wantErr: false,
		},

		// parseTableConstraintPrimaryKey - 66.7%
		{
			name:    "table PRIMARY KEY with autoincrement",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY (id) AUTOINCREMENT)",
			wantErr: true, // AUTOINCREMENT only valid on column constraint
		},
		{
			name:    "table PRIMARY KEY simple",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "table PRIMARY KEY composite",
			sql:     "CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))",
			wantErr: false,
		},

		// parseColumnOrConstraint - 66.7%
		{
			name:    "mixed columns and constraints",
			sql:     "CREATE TABLE t (id INTEGER, name TEXT, PRIMARY KEY (id), UNIQUE (name))",
			wantErr: false,
		},
		{
			name:    "constraint with unexpected token - treated as column name",
			sql:     "CREATE TABLE t (id INTEGER, INVALID)",
			wantErr: false, // INVALID is treated as a column name without type
		},

		// parseCreateIndex - 66.7%
		{
			name:    "CREATE INDEX missing name",
			sql:     "CREATE INDEX ON users (name)",
			wantErr: true,
		},
		{
			name:    "CREATE UNIQUE INDEX IF NOT EXISTS",
			sql:     "CREATE UNIQUE INDEX IF NOT EXISTS idx ON users (name)",
			wantErr: false,
		},

		// parseIndexNameAndTable - 66.7%
		{
			name:    "CREATE INDEX with schema - unsupported",
			sql:     "CREATE INDEX main.idx ON users (name)",
			wantErr: true,
		},

		// parseWhenClause - 66.7%
		{
			name:    "CASE WHEN with complex expression",
			sql:     "SELECT CASE WHEN x > 0 AND x < 10 THEN 'small' WHEN x >= 10 THEN 'large' END FROM t",
			wantErr: false,
		},

		// parseVacuum - 66.7%
		{
			name:    "VACUUM with INTO",
			sql:     "VACUUM INTO 'backup.db'",
			wantErr: false,
		},

		// parseFunctionFilter - 69.2%
		{
			name:    "aggregate with complex filter",
			sql:     "SELECT COUNT(*) FILTER (WHERE age > 18 AND active = 1) FROM users",
			wantErr: false,
		},

		// parseCreateTableAsSelect - 71.4%
		{
			name:    "CREATE TABLE AS with complex SELECT",
			sql:     "CREATE TABLE new_users AS SELECT * FROM users WHERE active = 1",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE AS missing SELECT",
			sql:     "CREATE TABLE new_users AS",
			wantErr: true,
		},

		// parseBetweenExpression - 72.7%
		{
			name:    "BETWEEN with expressions",
			sql:     "SELECT * FROM t WHERE x + 1 BETWEEN y * 2 AND z / 3",
			wantErr: false,
		},

		// parseInsertValues - 75.0%
		{
			name:    "INSERT VALUES multiple rows",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
			wantErr: false,
		},
		{
			name:    "INSERT VALUES missing comma",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'a') (2, 'b')",
			wantErr: true,
		},

		// parseInsertSource - 75.0%
		{
			name:    "INSERT DEFAULT VALUES",
			sql:     "INSERT INTO users DEFAULT VALUES",
			wantErr: false,
		},
		{
			name:    "INSERT with SELECT",
			sql:     "INSERT INTO new_users SELECT * FROM old_users",
			wantErr: false,
		},

		// parseForeignKeyReferences - 75.0%
		{
			name:    "FOREIGN KEY without referenced columns",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users)",
			wantErr: false,
		},
		{
			name:    "FOREIGN KEY with referenced columns",
			sql:     "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users (id))",
			wantErr: false,
		},

		// isPragmaValueIdentifier - 75.0%
		{
			name:    "PRAGMA with YES",
			sql:     "PRAGMA secure_delete = YES",
			wantErr: false,
		},
		{
			name:    "PRAGMA with NO - unsupported",
			sql:     "PRAGMA secure_delete = NO",
			wantErr: true,
		},

		// parseConflictTarget - 76.2%
		{
			name:    "INSERT ON CONFLICT without target",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING",
			wantErr: false,
		},
		{
			name:    "INSERT ON CONFLICT with WHERE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) WHERE id > 0 DO NOTHING",
			wantErr: false,
		},

		// parseCreate - 76.9%
		{
			name:    "CREATE with invalid object",
			sql:     "CREATE INVALID",
			wantErr: true,
		},
		{
			name:    "CREATE TEMP TABLE",
			sql:     "CREATE TEMP TABLE t (id INTEGER)",
			wantErr: false,
		},
		{
			name:    "CREATE TEMPORARY TABLE",
			sql:     "CREATE TEMPORARY TABLE t (id INTEGER)",
			wantErr: false,
		},

		// parseCreateTable - 76.9%
		{
			name:    "CREATE TABLE with schema - unsupported",
			sql:     "CREATE TABLE main.users (id INTEGER)",
			wantErr: true,
		},

		// parseUpdateAssignments - 76.9%
		{
			name:    "UPDATE multiple assignments",
			sql:     "UPDATE users SET name = 'John', age = 30, email = 'john@example.com' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "UPDATE missing assignment",
			sql:     "UPDATE users SET WHERE id = 1",
			wantErr: true,
		},

		// parseDelete - 77.8%
		{
			name:    "DELETE FROM with schema - unsupported",
			sql:     "DELETE FROM main.users WHERE id = 1",
			wantErr: true,
		},

		// parseDropTable - 77.8%
		{
			name:    "DROP TABLE with schema - unsupported",
			sql:     "DROP TABLE main.users",
			wantErr: true,
		},

		// parseDropIndex - 77.8%
		{
			name:    "DROP INDEX with schema - unsupported",
			sql:     "DROP INDEX main.idx_name",
			wantErr: true,
		},

		// parseDeleteOrderBy - 77.8%
		{
			name:    "DELETE ORDER BY multiple",
			sql:     "DELETE FROM users ORDER BY created_at DESC, id ASC LIMIT 10",
			wantErr: false,
		},

		// parseUpdateOrderByClause - 77.8%
		{
			name:    "UPDATE ORDER BY multiple",
			sql:     "UPDATE users SET status = 'inactive' ORDER BY last_login ASC, id DESC LIMIT 100",
			wantErr: false,
		},

		// parseSelectClauses - 77.8%
		{
			name:    "SELECT with HAVING without GROUP BY - unsupported",
			sql:     "SELECT COUNT(*) FROM users HAVING COUNT(*) > 5",
			wantErr: true,
		},

		// parseGroupByClauseInto - 78.6%
		{
			name:    "GROUP BY with HAVING complex",
			sql:     "SELECT dept, COUNT(*) FROM users GROUP BY dept HAVING COUNT(*) > 5 AND AVG(age) > 30",
			wantErr: false,
		},
		{
			name:    "GROUP BY multiple columns",
			sql:     "SELECT dept, role, COUNT(*) FROM users GROUP BY dept, role",
			wantErr: false,
		},

		// parseUpdate - 81.8%
		{
			name:    "UPDATE with schema - unsupported",
			sql:     "UPDATE main.users SET name = 'John' WHERE id = 1",
			wantErr: true,
		},

		// parseDoUpdateClause - 81.8%
		{
			name:    "UPSERT with DO UPDATE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
			wantErr: false,
		},
		{
			name:    "UPSERT DO UPDATE with WHERE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE users.id > 0",
			wantErr: false,
		},

		// parseTableRef - 81.8%
		{
			name:    "FROM with indexed hint",
			sql:     "SELECT * FROM users INDEXED BY idx_name WHERE name = 'John'",
			wantErr: false,
		},
		{
			name:    "FROM with NOT INDEXED - unsupported",
			sql:     "SELECT * FROM users NOT INDEXED WHERE name = 'John'",
			wantErr: true,
		},

		// parseLimitClauseInto - 83.3%
		{
			name:    "LIMIT with expression",
			sql:     "SELECT * FROM users LIMIT 10 + 5",
			wantErr: false,
		},

		// parseIfNotExists - 83.3%
		{
			name:    "CREATE TABLE IF NOT EXISTS",
			sql:     "CREATE TABLE IF NOT EXISTS users (id INTEGER)",
			wantErr: false,
		},

		// parseCTEColumns - 83.3%
		{
			name:    "CTE with many columns",
			sql:     "WITH cte (a, b, c, d) AS (SELECT 1, 2, 3, 4) SELECT * FROM cte",
			wantErr: false,
		},

		// parseAlias - 85.7%
		{
			name:    "column alias quoted",
			sql:     "SELECT name AS \"User Name\" FROM users",
			wantErr: false,
		},

		// parseExprColumn - 87.5%
		{
			name:    "SELECT expr with AS",
			sql:     "SELECT id * 2 AS double_id FROM users",
			wantErr: false,
		},

		// parseFromClause - 87.5%
		{
			name:    "FROM with multiple joins",
			sql:     "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
			wantErr: false,
		},

		// parseTableAlias - 87.5%
		{
			name:    "table alias with AS keyword",
			sql:     "SELECT * FROM users AS u WHERE u.id = 1",
			wantErr: false,
		},

		// parseUsingColumnList - 87.5%
		{
			name:    "USING with multiple columns",
			sql:     "SELECT * FROM a JOIN b USING (id, name)",
			wantErr: false,
		},

		// parseTableOptions - 87.5%
		{
			name:    "CREATE TABLE with WITHOUT ROWID",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY) WITHOUT ROWID",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE with STRICT",
			sql:     "CREATE TABLE t (id INTEGER) STRICT",
			wantErr: false,
		},

		// parseCompoundSelect - 87.5%
		{
			name:    "UNION with ORDER BY",
			sql:     "SELECT * FROM a UNION SELECT * FROM b ORDER BY id",
			wantErr: false,
		},

		// parseSelect - 88.9%
		{
			name:    "SELECT with WITH RECURSIVE",
			sql:     "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT * FROM cnt",
			wantErr: false,
		},

		// parseCTE - 88.9%
		{
			name:    "CTE with RECURSIVE",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
			wantErr: false,
		},

		// parseViewSelect - 88.9%
		{
			name:    "CREATE VIEW with complex SELECT",
			sql:     "CREATE VIEW v AS SELECT u.*, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id",
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

// TestParserAdditionalErrorPaths tests error conditions to increase coverage
func TestParserAdditionalErrorPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		// Parse error - tokenization failure
		{"illegal character", "SELECT ™ FROM users"},

		// parseSelect errors
		{"SELECT missing columns", "SELECT FROM users"},

		// parseInsert errors
		{"INSERT missing INTO", "INSERT users VALUES (1)"},
		{"INSERT missing table", "INSERT INTO VALUES (1)"},

		// parseUpdate errors
		{"UPDATE missing table", "UPDATE SET x = 1"},
		{"UPDATE missing SET", "UPDATE users x = 1"},

		// parseDelete errors
		{"DELETE missing FROM", "DELETE users"},
		{"DELETE missing table", "DELETE FROM"},

		// parseCreate errors
		{"CREATE missing object", "CREATE"},

		// parseDrop errors
		{"DROP missing object", "DROP"},

		// Expression errors
		{"incomplete expression", "SELECT (1 + FROM users"},

		// Subquery errors
		{"unclosed subquery", "SELECT * FROM (SELECT * FROM users"},

		// Function errors
		{"unclosed function", "SELECT COUNT( FROM users"},

		// CASE errors
		{"CASE missing END", "SELECT CASE WHEN 1 THEN 2 FROM t"},

		// JOIN errors
		{"JOIN missing table", "SELECT * FROM users JOIN"},
		// Note: JOIN without ON is valid (CROSS JOIN), so removed this test

		// Constraint errors
		{"CHECK missing expression", "CREATE TABLE t (id INTEGER CHECK)"},
		{"FOREIGN KEY missing reference", "CREATE TABLE t (id INTEGER, FOREIGN KEY (id))"},

		// Index errors
		{"CREATE INDEX missing columns", "CREATE INDEX idx ON users"},

		// Trigger errors
		{"CREATE TRIGGER missing BEGIN", "CREATE TRIGGER t1 BEFORE INSERT ON users"},
		{"CREATE TRIGGER missing END", "CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1"},

		// PRAGMA errors
		{"PRAGMA invalid syntax", "PRAGMA = value"},

		// CTE errors
		{"WITH missing AS", "WITH cte SELECT * FROM cte"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if err == nil {
				t.Errorf("expected error for %q, got nil", tt.sql)
			}
		})
	}
}
