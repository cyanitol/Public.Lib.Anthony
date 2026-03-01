// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

// TestTableConstraints tests table-level constraints
func TestTableConstraints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "primary key constraint",
			sql:     "CREATE TABLE t (id INT, name TEXT, PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "named primary key",
			sql:     "CREATE TABLE t (id INT, CONSTRAINT pk PRIMARY KEY (id))",
			wantErr: false,
		},
		{
			name:    "unique constraint",
			sql:     "CREATE TABLE t (email TEXT, UNIQUE (email))",
			wantErr: false,
		},
		{
			name:    "check constraint",
			sql:     "CREATE TABLE t (age INT, CHECK (age >= 0))",
			wantErr: false,
		},
		{
			name:    "foreign key constraint",
			sql:     "CREATE TABLE orders (user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestCreateIndexEdgeCases tests CREATE INDEX edge cases
func TestCreateIndexEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create index if not exists",
			sql:     "CREATE INDEX IF NOT EXISTS idx_name ON users(name)",
			wantErr: false,
		},
		{
			name:    "create unique index",
			sql:     "CREATE UNIQUE INDEX idx_email ON users(email)",
			wantErr: false,
		},
		{
			name:    "create index with where clause",
			sql:     "CREATE INDEX idx_active ON users(status) WHERE status = 'active'",
			wantErr: false,
		},
		{
			name:    "create index with multiple columns",
			sql:     "CREATE INDEX idx_name_age ON users(name ASC, age DESC)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseCompoundSelectExtended tests UNION, EXCEPT, INTERSECT
func TestParseCompoundSelectExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "UNION",
			sql:     "SELECT 1 UNION SELECT 2",
			wantErr: false,
		},
		{
			name:    "UNION ALL",
			sql:     "SELECT 1 UNION ALL SELECT 2",
			wantErr: false,
		},
		{
			name:    "EXCEPT",
			sql:     "SELECT 1 EXCEPT SELECT 2",
			wantErr: false,
		},
		{
			name:    "INTERSECT",
			sql:     "SELECT 1 INTERSECT SELECT 2",
			wantErr: false,
		},
		{
			name:    "multiple compounds",
			sql:     "SELECT 1 UNION SELECT 2 UNION SELECT 3",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseExpressionEdgeCases tests edge cases in expression parsing
func TestParseExpressionEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "NOT with BETWEEN",
			sql:     "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10",
			wantErr: false,
		},
		{
			name:    "NOT with IN",
			sql:     "SELECT * FROM t WHERE x NOT IN (1, 2, 3)",
			wantErr: false,
		},
		{
			name:    "IS NULL",
			sql:     "SELECT * FROM t WHERE value IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT NULL",
			sql:     "SELECT * FROM t WHERE value IS NOT NULL",
			wantErr: false,
		},
		{
			name:    "CASE with base expression",
			sql:     "SELECT CASE status WHEN 1 THEN 'active' ELSE 'inactive' END FROM t",
			wantErr: false,
		},
		{
			name:    "CASE without ELSE",
			sql:     "SELECT CASE WHEN age > 18 THEN 'adult' END FROM t",
			wantErr: false,
		},
		{
			name:    "unary minus",
			sql:     "SELECT -5 FROM t",
			wantErr: false,
		},
		{
			name:    "unary plus",
			sql:     "SELECT +5 FROM t",
			wantErr: false,
		},
		{
			name:    "unary NOT",
			sql:     "SELECT * FROM t WHERE NOT active",
			wantErr: false,
		},
		{
			name:    "bitwise not",
			sql:     "SELECT ~flags FROM t",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseFunctionEdgeCases tests function call edge cases
func TestParseFunctionEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "COUNT(*)",
			sql:     "SELECT COUNT(*) FROM t",
			wantErr: false,
		},
		{
			name:    "COUNT(DISTINCT col)",
			sql:     "SELECT COUNT(DISTINCT category) FROM products",
			wantErr: false,
		},
		{
			name:    "function with FILTER",
			sql:     "SELECT COUNT(*) FILTER (WHERE status = 'active') FROM users",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseSubquery tests subquery parsing
func TestParseSubquery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "subquery in FROM",
			sql:     "SELECT * FROM (SELECT id FROM users) AS u",
			wantErr: false,
		},
		{
			name:    "subquery in WHERE",
			sql:     "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 1)",
			wantErr: false,
		},
		{
			name:    "scalar subquery",
			sql:     "SELECT (SELECT COUNT(*) FROM orders) AS order_count",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseJoinUsingCondition tests JOIN USING syntax
func TestParseJoinUsingCondition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "JOIN USING single column",
			sql:     "SELECT * FROM users JOIN orders USING (user_id)",
			wantErr: false,
		},
		{
			name:    "JOIN USING multiple columns",
			sql:     "SELECT * FROM t1 JOIN t2 USING (col1, col2)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseTableRef tests table reference edge cases
func TestParseTableRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "table with alias",
			sql:     "SELECT * FROM users u",
			wantErr: false,
		},
		{
			name:    "table with AS alias",
			sql:     "SELECT * FROM users AS u",
			wantErr: false,
		},
		{
			name:    "table INDEXED BY",
			sql:     "SELECT * FROM users INDEXED BY idx_name",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseOneResultColumn tests single result column parsing edge cases
func TestParseOneResultColumn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "star",
			sql:     "SELECT *",
			wantErr: false,
		},
		{
			name:    "table star",
			sql:     "SELECT users.*",
			wantErr: false,
		},
		{
			name:    "expression without alias",
			sql:     "SELECT id + 1",
			wantErr: false,
		},
		{
			name:    "expression with AS alias",
			sql:     "SELECT id + 1 AS next_id",
			wantErr: false,
		},
		{
			name:    "expression with implicit alias",
			sql:     "SELECT id + 1 next_id",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseUpdateClauses tests UPDATE statement clauses
func TestParseUpdateClauses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "UPDATE with WHERE",
			sql:     "UPDATE users SET name = 'John' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "UPDATE with ORDER BY",
			sql:     "UPDATE users SET active = 0 ORDER BY created_at",
			wantErr: false,
		},
		{
			name:    "UPDATE with LIMIT",
			sql:     "UPDATE users SET active = 0 LIMIT 10",
			wantErr: false,
		},
		{
			name:    "UPDATE with all clauses",
			sql:     "UPDATE users SET active = 0 WHERE age < 18 ORDER BY id LIMIT 100",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseDeleteClauses tests DELETE statement clauses
func TestParseDeleteClauses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "DELETE with WHERE",
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "DELETE with ORDER BY",
			sql:     "DELETE FROM users ORDER BY created_at",
			wantErr: false,
		},
		{
			name:    "DELETE with LIMIT",
			sql:     "DELETE FROM users LIMIT 10",
			wantErr: false,
		},
		{
			name:    "DELETE with all clauses",
			sql:     "DELETE FROM users WHERE active = 0 ORDER BY id LIMIT 100",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseGroupByHaving tests GROUP BY and HAVING edge cases
func TestParseGroupByHaving(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "GROUP BY single column",
			sql:     "SELECT category, COUNT(*) FROM products GROUP BY category",
			wantErr: false,
		},
		{
			name:    "GROUP BY multiple columns",
			sql:     "SELECT category, brand, COUNT(*) FROM products GROUP BY category, brand",
			wantErr: false,
		},
		{
			name:    "GROUP BY with HAVING",
			sql:     "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseColumnConstraintTypes tests various column constraint types
func TestParseColumnConstraintTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "PRIMARY KEY",
			sql:     "CREATE TABLE t (id INT PRIMARY KEY)",
			wantErr: false,
		},
		{
			name:    "PRIMARY KEY ASC",
			sql:     "CREATE TABLE t (id INT PRIMARY KEY ASC)",
			wantErr: false,
		},
		{
			name:    "PRIMARY KEY DESC",
			sql:     "CREATE TABLE t (id INT PRIMARY KEY DESC)",
			wantErr: false,
		},
		{
			name:    "PRIMARY KEY AUTOINCREMENT",
			sql:     "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)",
			wantErr: false,
		},
		{
			name:    "NOT NULL",
			sql:     "CREATE TABLE t (name TEXT NOT NULL)",
			wantErr: false,
		},
		{
			name:    "UNIQUE",
			sql:     "CREATE TABLE t (email TEXT UNIQUE)",
			wantErr: false,
		},
		{
			name:    "CHECK constraint",
			sql:     "CREATE TABLE t (age INT CHECK (age >= 0))",
			wantErr: false,
		},
		{
			name:    "DEFAULT literal",
			sql:     "CREATE TABLE t (status TEXT DEFAULT 'active')",
			wantErr: false,
		},
		{
			name:    "DEFAULT expression",
			sql:     "CREATE TABLE t (created_at INT DEFAULT (strftime('%s', 'now')))",
			wantErr: false,
		},
		{
			name:    "COLLATE",
			sql:     "CREATE TABLE t (name TEXT COLLATE NOCASE)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseCreateTableOptions tests CREATE TABLE options
func TestParseCreateTableOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "WITHOUT ROWID",
			sql:     "CREATE TABLE t (id INT PRIMARY KEY) WITHOUT ROWID",
			wantErr: false,
		},
		{
			name:    "STRICT",
			sql:     "CREATE TABLE t (id INT) STRICT",
			wantErr: false,
		},
		{
			name:    "WITHOUT ROWID STRICT",
			sql:     "CREATE TABLE t (id INT PRIMARY KEY) WITHOUT ROWID, STRICT",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestBeginTransaction tests BEGIN with transaction modes
func TestBeginTransaction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "BEGIN",
			sql:     "BEGIN",
			wantErr: false,
		},
		{
			name:    "BEGIN DEFERRED",
			sql:     "BEGIN DEFERRED",
			wantErr: false,
		},
		{
			name:    "BEGIN IMMEDIATE",
			sql:     "BEGIN IMMEDIATE",
			wantErr: false,
		},
		{
			name:    "BEGIN EXCLUSIVE",
			sql:     "BEGIN EXCLUSIVE",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParsePragmaValue tests PRAGMA value parsing
func TestParsePragmaValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "PRAGMA with equals",
			sql:     "PRAGMA foreign_keys = ON",
			wantErr: false,
		},
		{
			name:    "PRAGMA with parens",
			sql:     "PRAGMA cache_size(2000)",
			wantErr: false,
		},
		{
			name:    "PRAGMA query",
			sql:     "PRAGMA table_info(users)",
			wantErr: false,
		},
		{
			name:    "PRAGMA with schema",
			sql:     "PRAGMA main.foreign_keys = 1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseTriggerForEachRow tests trigger FOR EACH ROW clause
func TestParseTriggerForEachRow(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "trigger with FOR EACH ROW",
			sql:     "CREATE TRIGGER t AFTER INSERT ON users FOR EACH ROW BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger without FOR EACH ROW",
			sql:     "CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}

// TestParseTriggerEvent tests trigger event types
func TestParseTriggerEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "INSERT trigger",
			sql:     "CREATE TRIGGER t AFTER INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "UPDATE trigger",
			sql:     "CREATE TRIGGER t AFTER UPDATE ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "UPDATE OF trigger",
			sql:     "CREATE TRIGGER t AFTER UPDATE OF name, email ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "DELETE trigger",
			sql:     "CREATE TRIGGER t BEFORE DELETE ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}
