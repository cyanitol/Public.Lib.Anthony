// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// assertExplainParse parses sql as an EXPLAIN statement, checking error expectation and fields.
func assertExplainParse(t *testing.T, sql string, wantErr bool, wantQP bool, wantStmtTyp string) {
	t.Helper()
	p := NewParser(sql)
	stmts, err := p.Parse()

	if (err != nil) != wantErr {
		t.Errorf("Parse() error = %v, wantErr %v", err, wantErr)
		return
	}
	if wantErr {
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
	if explainStmt.QueryPlan != wantQP {
		t.Errorf("QueryPlan = %v, want %v", explainStmt.QueryPlan, wantQP)
	}
	if explainStmt.Statement == nil {
		t.Errorf("Statement is nil")
		return
	}
	if explainStmt.Statement.String() != wantStmtTyp {
		t.Errorf("Statement type = %v, want %v", explainStmt.Statement.String(), wantStmtTyp)
	}
}

func TestParseExplain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantQP      bool
		wantStmtTyp string
	}{
		{"explain select", "EXPLAIN SELECT * FROM users", false, false, "SELECT"},
		{"explain query plan select", "EXPLAIN QUERY PLAN SELECT * FROM users", false, true, "SELECT"},
		{"explain insert", "EXPLAIN INSERT INTO users (name) VALUES ('Alice')", false, false, "INSERT"},
		{"explain query plan insert", "EXPLAIN QUERY PLAN INSERT INTO users VALUES (1, 'Bob')", false, true, "INSERT"},
		{"explain update", "EXPLAIN UPDATE users SET name = 'Charlie' WHERE id = 1", false, false, "UPDATE"},
		{"explain query plan update", "EXPLAIN QUERY PLAN UPDATE users SET age = 30", false, true, "UPDATE"},
		{"explain delete", "EXPLAIN DELETE FROM users WHERE age < 18", false, false, "DELETE"},
		{"explain query plan delete", "EXPLAIN QUERY PLAN DELETE FROM users", false, true, "DELETE"},
		{"explain create table", "EXPLAIN CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", false, false, "CREATE TABLE"},
		{"explain query plan create index", "EXPLAIN QUERY PLAN CREATE INDEX idx_name ON users(name)", false, true, "CREATE INDEX"},
		{"explain select with join", "EXPLAIN SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", false, false, "SELECT"},
		{"explain query plan select with complex query", "EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 10", false, true, "SELECT"},
		{"explain drop table", "EXPLAIN DROP TABLE users", false, false, "DROP TABLE"},
		{"explain begin transaction", "EXPLAIN BEGIN TRANSACTION", false, false, "BEGIN"},
		{"explain query without plan - error", "EXPLAIN QUERY SELECT * FROM users", true, false, ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertExplainParse(t, tt.sql, tt.wantErr, tt.wantQP, tt.wantStmtTyp)
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
