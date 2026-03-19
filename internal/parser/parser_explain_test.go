// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func TestParseExplain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantQP      bool // want QueryPlan flag
		wantStmtTyp string
	}{
		{
			name:        "explain select",
			sql:         "EXPLAIN SELECT * FROM users",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "SELECT",
		},
		{
			name:        "explain query plan select",
			sql:         "EXPLAIN QUERY PLAN SELECT * FROM users",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "SELECT",
		},
		{
			name:        "explain insert",
			sql:         "EXPLAIN INSERT INTO users (name) VALUES ('Alice')",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "INSERT",
		},
		{
			name:        "explain query plan insert",
			sql:         "EXPLAIN QUERY PLAN INSERT INTO users VALUES (1, 'Bob')",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "INSERT",
		},
		{
			name:        "explain update",
			sql:         "EXPLAIN UPDATE users SET name = 'Charlie' WHERE id = 1",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "UPDATE",
		},
		{
			name:        "explain query plan update",
			sql:         "EXPLAIN QUERY PLAN UPDATE users SET age = 30",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "UPDATE",
		},
		{
			name:        "explain delete",
			sql:         "EXPLAIN DELETE FROM users WHERE age < 18",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "DELETE",
		},
		{
			name:        "explain query plan delete",
			sql:         "EXPLAIN QUERY PLAN DELETE FROM users",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "DELETE",
		},
		{
			name:        "explain create table",
			sql:         "EXPLAIN CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "CREATE TABLE",
		},
		{
			name:        "explain query plan create index",
			sql:         "EXPLAIN QUERY PLAN CREATE INDEX idx_name ON users(name)",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "CREATE INDEX",
		},
		{
			name:        "explain select with join",
			sql:         "EXPLAIN SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "SELECT",
		},
		{
			name:        "explain query plan select with complex query",
			sql:         "EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 10",
			wantErr:     false,
			wantQP:      true,
			wantStmtTyp: "SELECT",
		},
		{
			name:        "explain drop table",
			sql:         "EXPLAIN DROP TABLE users",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "DROP TABLE",
		},
		{
			name:        "explain begin transaction",
			sql:         "EXPLAIN BEGIN TRANSACTION",
			wantErr:     false,
			wantQP:      false,
			wantStmtTyp: "BEGIN",
		},
		{
			name:    "explain query without plan - error",
			sql:     "EXPLAIN QUERY SELECT * FROM users",
			wantErr: true,
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

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			explainStmt, ok := stmts[0].(*ExplainStmt)
			if !ok {
				t.Errorf("expected ExplainStmt, got %T", stmts[0])
				return
			}

			if explainStmt.QueryPlan != tt.wantQP {
				t.Errorf("QueryPlan = %v, want %v", explainStmt.QueryPlan, tt.wantQP)
			}

			if explainStmt.Statement == nil {
				t.Errorf("Statement is nil")
				return
			}

			if explainStmt.Statement.String() != tt.wantStmtTyp {
				t.Errorf("Statement type = %v, want %v", explainStmt.Statement.String(), tt.wantStmtTyp)
			}
		})
	}
}

func TestParseExplainNested(t *testing.T) {
	t.Parallel()
	// Test that nested EXPLAIN statements work correctly
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "nested explain - should work",
			sql:     "EXPLAIN EXPLAIN SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "nested explain query plan",
			sql:     "EXPLAIN QUERY PLAN EXPLAIN SELECT * FROM users",
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

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			// First level should be ExplainStmt
			explainStmt1, ok := stmts[0].(*ExplainStmt)
			if !ok {
				t.Errorf("expected ExplainStmt at top level, got %T", stmts[0])
				return
			}

			// Second level should also be ExplainStmt
			explainStmt2, ok := explainStmt1.Statement.(*ExplainStmt)
			if !ok {
				t.Errorf("expected ExplainStmt at second level, got %T", explainStmt1.Statement)
				return
			}

			// Third level should be SelectStmt
			_, ok = explainStmt2.Statement.(*SelectStmt)
			if !ok {
				t.Errorf("expected SelectStmt at third level, got %T", explainStmt2.Statement)
			}
		})
	}
}

func TestExplainStmtString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		stmt    *ExplainStmt
		wantStr string
	}{
		{
			name: "explain without query plan",
			stmt: &ExplainStmt{
				QueryPlan: false,
				Statement: &SelectStmt{},
			},
			wantStr: "EXPLAIN",
		},
		{
			name: "explain with query plan",
			stmt: &ExplainStmt{
				QueryPlan: true,
				Statement: &SelectStmt{},
			},
			wantStr: "EXPLAIN QUERY PLAN",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.stmt.String()
			if got != tt.wantStr {
				t.Errorf("String() = %v, want %v", got, tt.wantStr)
			}
		})
	}
}
