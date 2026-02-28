package parser

import (
	"testing"
)

// TestEdgeCasesForMaxCoverage focuses on specific uncovered lines
func TestEdgeCasesForMaxCoverage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// applyTableConstraintPrimaryKey - error paths
		{
			name:    "table PRIMARY KEY missing KEY",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY)",
			wantErr: true,
		},
		{
			name:    "table PRIMARY KEY missing opening paren",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY id)",
			wantErr: true,
		},
		{
			name:    "table PRIMARY KEY missing closing paren",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY (id",
			wantErr: true,
		},

		// parseColumnConstraint - error paths
		{
			name:    "column constraint with name but no constraint",
			sql:     "CREATE TABLE t (id INTEGER CONSTRAINT pk_id)",
			wantErr: true,
		},
		{
			name:    "column constraint named PRIMARY KEY",
			sql:     "CREATE TABLE t (id INTEGER CONSTRAINT pk_id PRIMARY KEY)",
			wantErr: false,
		},
		{
			name:    "column constraint named NOT NULL",
			sql:     "CREATE TABLE t (id INTEGER CONSTRAINT nn_id NOT NULL)",
			wantErr: false,
		},
		{
			name:    "column constraint named UNIQUE",
			sql:     "CREATE TABLE t (id INTEGER CONSTRAINT uk_id UNIQUE)",
			wantErr: false,
		},
		{
			name:    "column constraint named CHECK",
			sql:     "CREATE TABLE t (age INTEGER CONSTRAINT chk_age CHECK (age > 0))",
			wantErr: false,
		},
		{
			name:    "column constraint named DEFAULT",
			sql:     "CREATE TABLE t (status TEXT CONSTRAINT def_status DEFAULT 'active')",
			wantErr: false,
		},
		{
			name:    "column constraint named COLLATE",
			sql:     "CREATE TABLE t (name TEXT CONSTRAINT col_name COLLATE NOCASE)",
			wantErr: false,
		},

		// applyConstraintPrimaryKey - ASC/DESC/AUTOINCREMENT paths
		{
			name:    "column PRIMARY KEY ASC",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY ASC)",
			wantErr: false,
		},
		{
			name:    "column PRIMARY KEY DESC",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY DESC)",
			wantErr: false,
		},
		{
			name:    "column PRIMARY KEY missing KEY",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY)",
			wantErr: true,
		},

		// parseCreateIndex - error paths
		{
			name:    "CREATE INDEX missing columns paren",
			sql:     "CREATE INDEX idx ON users",
			wantErr: true,
		},
		{
			name:    "CREATE INDEX missing table name",
			sql:     "CREATE INDEX idx ON",
			wantErr: true,
		},

		// parseCreateTable - error paths
		{
			name:    "CREATE TABLE missing paren",
			sql:     "CREATE TABLE users",
			wantErr: true,
		},
		{
			name:    "CREATE TABLE empty columns",
			sql:     "CREATE TABLE users ()",
			wantErr: true,
		},

		// parseUpdateAssignments - error paths
		{
			name:    "UPDATE missing equal sign",
			sql:     "UPDATE users SET name 'John'",
			wantErr: true,
		},

		// parseColumnOrConstraint - additional coverage
		{
			name:    "CREATE TABLE with CONSTRAINT CHECK",
			sql:     "CREATE TABLE t (age INTEGER, CONSTRAINT chk_age CHECK (age > 0))",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE with CONSTRAINT FOREIGN KEY",
			sql:     "CREATE TABLE orders (user_id INTEGER, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))",
			wantErr: false,
		},

		// parseIndexWhereClause - more coverage
		{
			name:    "CREATE INDEX with complex WHERE",
			sql:     "CREATE INDEX idx ON users (name) WHERE active = 1 AND deleted_at IS NULL",
			wantErr: false,
		},

		// parseForeignKeyReferences - error paths
		{
			name:    "FOREIGN KEY missing table",
			sql:     "CREATE TABLE t (id INTEGER, FOREIGN KEY (id) REFERENCES)",
			wantErr: true,
		},
		{
			name:    "FOREIGN KEY references with multiple columns",
			sql:     "CREATE TABLE t (a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES other (x, y))",
			wantErr: false,
		},

		// parseForeignKeyColumns - error paths
		{
			name:    "FOREIGN KEY missing opening paren",
			sql:     "CREATE TABLE t (id INTEGER, FOREIGN KEY id REFERENCES other(id))",
			wantErr: true,
		},
		{
			name:    "FOREIGN KEY empty column list",
			sql:     "CREATE TABLE t (id INTEGER, FOREIGN KEY () REFERENCES other(id))",
			wantErr: true,
		},

		// parseIfNotExists - both paths
		{
			name:    "CREATE TABLE without IF NOT EXISTS",
			sql:     "CREATE TABLE users (id INTEGER)",
			wantErr: false,
		},

		// parseCreateTableAsSelect - error paths
		{
			name:    "CREATE TABLE AS without SELECT",
			sql:     "CREATE TABLE new_users AS WHERE id = 1",
			wantErr: true,
		},

		// parseTableOptions - all variations
		{
			name:    "CREATE TABLE WITHOUT ROWID",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY) WITHOUT ROWID",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE STRICT",
			sql:     "CREATE TABLE t (id INTEGER) STRICT",
			wantErr: false,
		},
		{
			name:    "CREATE TABLE WITHOUT ROWID STRICT",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY) WITHOUT ROWID, STRICT",
			wantErr: false,
		},

		// applyConstraintDefault - expression vs literal
		{
			name:    "DEFAULT with parenthesized expression",
			sql:     "CREATE TABLE t (created INTEGER DEFAULT (strftime('%s', 'now')))",
			wantErr: false,
		},
		{
			name:    "DEFAULT with literal string",
			sql:     "CREATE TABLE t (status TEXT DEFAULT 'active')",
			wantErr: false,
		},
		{
			name:    "DEFAULT with literal number",
			sql:     "CREATE TABLE t (count INTEGER DEFAULT 42)",
			wantErr: false,
		},

		// parseTableConstraintName - with and without name
		{
			name:    "table constraint without name",
			sql:     "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "table constraint with name",
			sql:     "CREATE TABLE t (id INTEGER, CONSTRAINT pk_t PRIMARY KEY (id))",
			wantErr: false,
		},

		// parseIndexIfNotExists - both paths
		{
			name:    "CREATE INDEX without IF NOT EXISTS",
			sql:     "CREATE INDEX idx ON users (name)",
			wantErr: false,
		},
		{
			name:    "CREATE INDEX IF NOT EXISTS",
			sql:     "CREATE INDEX IF NOT EXISTS idx ON users (name)",
			wantErr: false,
		},

		// parseIndexNameAndTable - error paths
		{
			name:    "CREATE INDEX missing ON keyword",
			sql:     "CREATE INDEX idx users (name)",
			wantErr: true,
		},

		// parseViewSelect - error paths
		{
			name:    "CREATE VIEW without AS keyword",
			sql:     "CREATE VIEW v SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "CREATE VIEW AS without SELECT",
			sql:     "CREATE VIEW v AS UPDATE users SET x = 1",
			wantErr: true,
		},

		// parseUpdateWhereClause, parseUpdateOrderByClause, parseUpdateLimitClause
		{
			name:    "UPDATE with WHERE missing expression",
			sql:     "UPDATE users SET x = 1 WHERE",
			wantErr: true,
		},
		{
			name:    "UPDATE with ORDER BY missing expression",
			sql:     "UPDATE users SET x = 1 ORDER BY",
			wantErr: true,
		},
		{
			name:    "UPDATE with LIMIT missing expression",
			sql:     "UPDATE users SET x = 1 LIMIT",
			wantErr: true,
		},

		// parseDeleteWhere, parseDeleteOrderBy, parseDeleteLimit
		{
			name:    "DELETE with WHERE missing expression",
			sql:     "DELETE FROM users WHERE",
			wantErr: true,
		},
		{
			name:    "DELETE with ORDER BY missing expression",
			sql:     "DELETE FROM users ORDER BY",
			wantErr: true,
		},
		{
			name:    "DELETE with LIMIT missing expression",
			sql:     "DELETE FROM users LIMIT",
			wantErr: true,
		},

		// parseDelete - all branches
		{
			name:    "DELETE missing FROM keyword",
			sql:     "DELETE users WHERE id = 1",
			wantErr: true,
		},
		{
			name:    "DELETE missing table name",
			sql:     "DELETE FROM WHERE id = 1",
			wantErr: true,
		},

		// parseCreateTableAsSelect - full coverage
		{
			name:    "CREATE TABLE AS SELECT with WHERE",
			sql:     "CREATE TABLE archived AS SELECT * FROM users WHERE deleted = 1",
			wantErr: false,
		},

		// parseSelect - additional edge cases
		{
			name:    "SELECT DISTINCT ALL - contradiction not supported",
			sql:     "SELECT DISTINCT ALL * FROM users",
			wantErr: true,
		},

		// parseCreate - all object types
		{
			name:    "CREATE after temp variations",
			sql:     "CREATE TABLE t (x INTEGER)",
			wantErr: false,
		},

		// parseAlterTableRename - more paths
		{
			name:    "ALTER TABLE RENAME missing TO",
			sql:     "ALTER TABLE old_name RENAME new_name",
			wantErr: true,
		},
		{
			name:    "ALTER TABLE RENAME COLUMN missing TO",
			sql:     "ALTER TABLE t RENAME COLUMN old_col new_col",
			wantErr: true,
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

// TestParserHelperFunctions tests low-level helper functions
func TestParserHelperFunctions(t *testing.T) {
	t.Parallel()
	// Test checkIdentifier with all keyword types
	tests := []struct {
		name string
		sql  string
	}{
		{"quoted SELECT keyword", "SELECT \"select\" FROM t"},
		{"quoted FROM keyword", "SELECT \"from\" FROM t"},
		{"quoted WHERE keyword", "SELECT \"where\" FROM t"},
		{"quoted AS keyword", "SELECT x AS \"as\" FROM t"},
		{"backtick keyword", "SELECT `table` FROM t"},
		{"bracket keyword", "SELECT [index] FROM t"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestAllConstraintTypes ensures all constraint handling paths are covered
func TestAllConstraintTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
	}{
		// Column constraints
		{"PRIMARY KEY", "CREATE TABLE t (id INTEGER PRIMARY KEY)"},
		{"PRIMARY KEY ASC", "CREATE TABLE t (id INTEGER PRIMARY KEY ASC)"},
		{"PRIMARY KEY DESC", "CREATE TABLE t (id INTEGER PRIMARY KEY DESC)"},
		{"PRIMARY KEY AUTOINCREMENT", "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)"},
		{"NOT NULL", "CREATE TABLE t (id INTEGER NOT NULL)"},
		{"UNIQUE", "CREATE TABLE t (id INTEGER UNIQUE)"},
		{"CHECK simple", "CREATE TABLE t (age INTEGER CHECK (age > 0))"},
		{"CHECK complex", "CREATE TABLE t (age INTEGER CHECK (age BETWEEN 0 AND 120))"},
		{"DEFAULT string", "CREATE TABLE t (status TEXT DEFAULT 'active')"},
		{"DEFAULT number", "CREATE TABLE t (count INTEGER DEFAULT 0)"},
		{"DEFAULT expr", "CREATE TABLE t (created INTEGER DEFAULT (strftime('%s', 'now')))"},
		{"COLLATE", "CREATE TABLE t (name TEXT COLLATE NOCASE)"},

		// Table constraints
		{"table PRIMARY KEY", "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))"},
		{"table PRIMARY KEY composite", "CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))"},
		{"table UNIQUE", "CREATE TABLE t (email TEXT, UNIQUE (email))"},
		{"table CHECK", "CREATE TABLE t (age INTEGER, CHECK (age > 0))"},
		{"table FOREIGN KEY", "CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))"},

		// Named constraints
		{"named column PRIMARY KEY", "CREATE TABLE t (id INTEGER CONSTRAINT pk_id PRIMARY KEY)"},
		{"named column NOT NULL", "CREATE TABLE t (id INTEGER CONSTRAINT nn_id NOT NULL)"},
		{"named column UNIQUE", "CREATE TABLE t (email TEXT CONSTRAINT uk_email UNIQUE)"},
		{"named column CHECK", "CREATE TABLE t (age INTEGER CONSTRAINT chk_age CHECK (age > 0))"},
		{"named column DEFAULT", "CREATE TABLE t (status TEXT CONSTRAINT def_status DEFAULT 'active')"},
		{"named column COLLATE", "CREATE TABLE t (name TEXT CONSTRAINT col_name COLLATE NOCASE)"},
		{"named table PRIMARY KEY", "CREATE TABLE t (id INTEGER, CONSTRAINT pk_id PRIMARY KEY (id))"},
		{"named table UNIQUE", "CREATE TABLE t (email TEXT, CONSTRAINT uk_email UNIQUE (email))"},
		{"named table CHECK", "CREATE TABLE t (age INTEGER, CONSTRAINT chk_age CHECK (age > 0))"},
		{"named table FOREIGN KEY", "CREATE TABLE orders (user_id INTEGER, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
