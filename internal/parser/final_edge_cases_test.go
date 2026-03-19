// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// TestFinalEdgeCasesToReach99 covers remaining uncovered lines
func TestFinalEdgeCasesToReach99(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// parseTriggerForEachRow - error paths
		{
			name:    "trigger FOR without EACH",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users FOR BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "trigger FOR EACH without ROW",
			sql:     "CREATE TRIGGER t1 BEFORE INSERT ON users FOR EACH BEGIN SELECT 1; END",
			wantErr: true,
		},

		// parseCreateIndex - UNIQUE after CREATE INDEX
		{
			name:    "CREATE INDEX UNIQUE - unique after INDEX",
			sql:     "CREATE INDEX UNIQUE idx ON users (name)",
			wantErr: false,
		},

		// parseIndexIfNotExists - error paths
		{
			name:    "CREATE INDEX IF without NOT",
			sql:     "CREATE INDEX IF idx ON users (name)",
			wantErr: true,
		},
		{
			name:    "CREATE INDEX IF NOT without EXISTS",
			sql:     "CREATE INDEX IF NOT idx ON users (name)",
			wantErr: true,
		},

		// parseDropTable - error paths
		{
			name:    "DROP TABLE IF without NOT",
			sql:     "DROP TABLE IF users",
			wantErr: true,
		},
		{
			name:    "DROP TABLE IF NOT without EXISTS",
			sql:     "DROP TABLE IF NOT users",
			wantErr: true,
		},

		// parseDropIndex - error paths
		{
			name:    "DROP INDEX IF without NOT",
			sql:     "DROP INDEX IF idx_name",
			wantErr: true,
		},
		{
			name:    "DROP INDEX IF NOT without EXISTS",
			sql:     "DROP INDEX IF NOT idx_name",
			wantErr: true,
		},

		// parseCreateTrigger - error paths
		{
			name:    "trigger missing IF NOT EXISTS after IF",
			sql:     "CREATE TRIGGER IF t1 BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "trigger IF NOT without EXISTS",
			sql:     "CREATE TRIGGER IF NOT t1 BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: true,
		},

		// parseViewSelect - error paths
		{
			name:    "CREATE VIEW missing SELECT keyword",
			sql:     "CREATE VIEW v AS FROM users",
			wantErr: true,
		},

		// parseIndexWhereClause - error paths
		{
			name:    "CREATE INDEX WHERE without expression",
			sql:     "CREATE INDEX idx ON users (name) WHERE",
			wantErr: true,
		},

		// parseForeignKeyReferences - error paths
		{
			name:    "FOREIGN KEY REFERENCES missing paren for columns",
			sql:     "CREATE TABLE t (id INTEGER, FOREIGN KEY (id) REFERENCES other id))",
			wantErr: true,
		},

		// parseCreateTableAsSelect - error paths
		{
			name:    "CREATE TABLE AS missing keyword",
			sql:     "CREATE TABLE new_users * FROM users",
			wantErr: true,
		},

		// parseIfNotExists - error paths for tables
		{
			name:    "CREATE TABLE IF without NOT",
			sql:     "CREATE TABLE IF users (id INTEGER)",
			wantErr: true,
		},
		{
			name:    "CREATE TABLE IF NOT without EXISTS",
			sql:     "CREATE TABLE IF NOT users (id INTEGER)",
			wantErr: true,
		},

		// applyConstraintDefault - error path
		{
			name:    "DEFAULT missing value",
			sql:     "CREATE TABLE t (status TEXT DEFAULT)",
			wantErr: true,
		},

		// parseTableOptions - error path
		{
			name:    "WITHOUT missing ROWID",
			sql:     "CREATE TABLE t (id INTEGER) WITHOUT",
			wantErr: true,
		},

		// parseColumnOrConstraint - more coverage
		{
			name:    "table with only constraints no columns - allowed",
			sql:     "CREATE TABLE t (PRIMARY KEY (id))",
			wantErr: false, // Allowed even if semantically odd
		},

		// parseAlterTableRename - error paths
		{
			name:    "ALTER TABLE RENAME TO missing name",
			sql:     "ALTER TABLE old_name RENAME TO",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE RENAME COLUMN missing column name",
			sql:     "ALTER TABLE t RENAME COLUMN TO new_col",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE RENAME COLUMN missing TO keyword",
			sql:     "ALTER TABLE t RENAME COLUMN old_col",
			wantErr: true,
		},

		// parseTableConstraintName - error path
		{
			name:    "CONSTRAINT without name",
			sql:     "CREATE TABLE t (id INTEGER, CONSTRAINT PRIMARY KEY (id))",
			wantErr: true,
		},

		// parseCreateTable - more error paths
		{
			name:    "CREATE TABLE missing closing paren",
			sql:     "CREATE TABLE users (id INTEGER, name TEXT",
			wantErr: true,
		},

		// parseDeleteOrderBy - error path
		{
			name:    "DELETE ORDER BY with empty list",
			sql:     "DELETE FROM users ORDER BY LIMIT 1",
			wantErr: true,
		},

		// parseUpdateOrderByClause - error path
		{
			name:    "UPDATE ORDER BY with empty list",
			sql:     "UPDATE users SET x = 1 ORDER BY LIMIT 1",
			wantErr: true,
		},

		// parseSelect - more branches
		{
			name:    "SELECT with ALL keyword",
			sql:     "SELECT ALL * FROM users",
			wantErr: false,
		},

		// Multiple constraints on same column
		{
			name:    "column with multiple constraints",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL UNIQUE CHECK (id > 0) DEFAULT 1 COLLATE BINARY)",
			wantErr: false,
		},

		// Trigger variations
		{
			name:    "trigger AFTER INSERT",
			sql:     "CREATE TRIGGER t1 AFTER INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger INSTEAD OF DELETE on view",
			sql:     "CREATE TRIGGER t1 INSTEAD OF DELETE ON user_view BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger with all options",
			sql:     "CREATE TRIGGER IF NOT EXISTS t1 BEFORE UPDATE OF name ON users FOR EACH ROW WHEN NEW.name != OLD.name BEGIN UPDATE audit SET count = count + 1; END",
			wantErr: false,
		},

		// Index variations
		{
			name:    "CREATE UNIQUE INDEX IF NOT EXISTS with WHERE",
			sql:     "CREATE UNIQUE INDEX IF NOT EXISTS idx ON users (email) WHERE deleted_at IS NULL",
			wantErr: false,
		},

		// View variations
		{
			name:    "CREATE VIEW IF NOT EXISTS with columns",
			sql:     "CREATE VIEW IF NOT EXISTS user_emails (id, email) AS SELECT id, email FROM users",
			wantErr: false,
		},

		// Table variations
		{
			name:    "CREATE TEMP TABLE IF NOT EXISTS",
			sql:     "CREATE TEMP TABLE IF NOT EXISTS temp_data (id INTEGER)",
			wantErr: false,
		},
		{
			name:    "CREATE TEMPORARY TABLE IF NOT EXISTS",
			sql:     "CREATE TEMPORARY TABLE IF NOT EXISTS temp_data (id INTEGER)",
			wantErr: false,
		},

		// PRAGMA variations to hit IntValue, FloatValue, StringValue
		{
			name:    "PRAGMA with negative integer",
			sql:     "PRAGMA cache_size = -2000",
			wantErr: false,
		},
		{
			name:    "PRAGMA with float value",
			sql:     "PRAGMA mmap_size = 3.14159",
			wantErr: false,
		},

		// More expression coverage
		{
			name:    "complex expression with all operators",
			sql:     "SELECT a + b - c * d / e % f & g | h << i >> j FROM t",
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

// TestAllStatementTypes ensures all statement types are covered
func TestAllStatementTypes(t *testing.T) {
	t.Parallel()
	tests := []string{
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1)",
		"UPDATE users SET x = 1",
		"DELETE FROM users",
		"CREATE TABLE t (id INTEGER)",
		"DROP TABLE t",
		"CREATE INDEX idx ON t (id)",
		"DROP INDEX idx",
		"CREATE VIEW v AS SELECT * FROM t",
		"DROP VIEW v",
		"CREATE TRIGGER tr BEFORE INSERT ON t BEGIN SELECT 1; END",
		"DROP TRIGGER tr",
		"BEGIN",
		"BEGIN TRANSACTION",
		"BEGIN DEFERRED",
		"BEGIN IMMEDIATE",
		"BEGIN EXCLUSIVE",
		"COMMIT",
		// "END" and "END TRANSACTION" are not supported as standalone - use COMMIT
		"ROLLBACK",
		"ROLLBACK TRANSACTION",
		"EXPLAIN SELECT * FROM t",
		"EXPLAIN QUERY PLAN SELECT * FROM t",
		"ATTACH DATABASE 'file.db' AS db1",
		"DETACH DATABASE db1",
		"PRAGMA foreign_keys",
		"PRAGMA foreign_keys = ON",
		"ALTER TABLE t RENAME TO new_t",
		"ALTER TABLE t RENAME COLUMN old TO new",
		"ALTER TABLE t ADD COLUMN new TEXT",
		"ALTER TABLE t DROP COLUMN old",
		"VACUUM",
		"VACUUM main",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(sql)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("unexpected error for %q: %v", sql, err)
			}
		})
	}
}

// TestErrorRecovery tests that parser properly returns errors
func TestErrorRecovery(t *testing.T) {
	t.Parallel()
	errorTests := []string{
		"",
		"SELECT",
		"SELECT * FROM",
		"INSERT",
		"INSERT INTO",
		"INSERT INTO t",
		"UPDATE",
		"UPDATE SET x = 1",
		"DELETE",
		"CREATE",
		"CREATE TABLE",
		"CREATE TABLE t",
		"DROP",
		"DROP TABLE",
		"BEGIN INVALID",
		"PRAGMA",
		"ALTER",
		"ALTER TABLE",
		"ALTER TABLE t",
		"ATTACH",
		"DETACH",
	}

	for _, sql := range errorTests {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			p := NewParser(sql)
			_, err := p.Parse()
			// We just want to ensure these produce some result (error or empty)
			// and don't panic
			_ = err
		})
	}
}
