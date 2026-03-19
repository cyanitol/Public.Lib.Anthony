// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Helper functions to reduce cyclomatic complexity

func parseCreateViewStmt(t *testing.T, sql string) *CreateViewStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected CreateViewStmt, got %T", stmts[0])
	}
	return stmt
}

func runCreateViewSubtest(t *testing.T, name, sql string, check func(*testing.T, *CreateViewStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseCreateViewStmt(t, sql)
		check(t, stmt)
	})
}

func runCreateViewErrorSubtest(t *testing.T, name, sql string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err == nil {
			t.Error("Expected error but got none")
		}
	})
}

// TestParseCreateView tests CREATE VIEW statement parsing.
func TestParseCreateView(t *testing.T) {
	t.Parallel()
	testParseCreateViewSuccess(t)
	testParseCreateViewErrors(t)
}

func testParseCreateViewSuccess(t *testing.T) {
	t.Helper()
	testParseCreateViewBasic(t)
	testParseCreateViewOptions(t)
	testParseCreateViewQuoted(t)
}

func assertViewName(t *testing.T, stmt *CreateViewStmt, want string) {
	t.Helper()
	if stmt.Name != want {
		t.Errorf("expected view name %q, got %q", want, stmt.Name)
	}
}

func assertViewHasSelect(t *testing.T, stmt *CreateViewStmt) {
	t.Helper()
	if stmt.Select == nil {
		t.Error("expected SELECT statement")
	}
}

func testParseCreateViewBasic(t *testing.T) {
	t.Helper()

	runCreateViewSubtest(t, "simple view",
		"CREATE VIEW v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			assertViewName(t, stmt, "v")
			if stmt.IfNotExists {
				t.Error("expected IfNotExists to be false")
			}
			if stmt.Temporary {
				t.Error("expected Temporary to be false")
			}
			if len(stmt.Columns) != 0 {
				t.Errorf("expected no columns, got %d", len(stmt.Columns))
			}
			assertViewHasSelect(t, stmt)
		})

	runCreateViewSubtest(t, "view with column list",
		"CREATE VIEW v(id, name, email) AS SELECT id, name, email FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			assertViewName(t, stmt, "v")
			expectedCols := []string{"id", "name", "email"}
			if len(stmt.Columns) != len(expectedCols) {
				t.Fatalf("expected %d columns, got %d", len(expectedCols), len(stmt.Columns))
			}
			for i, col := range stmt.Columns {
				if col != expectedCols[i] {
					t.Errorf("column %d: expected %q, got %q", i, expectedCols[i], col)
				}
			}
		})

	basicViewTests := []struct {
		name     string
		sql      string
		wantName string
	}{
		{"view with complex SELECT", "CREATE VIEW user_orders AS SELECT u.id, u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name", "user_orders"},
		{"view with WHERE clause", "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1", "active_users"},
		{"view with ORDER BY", "CREATE VIEW sorted_users AS SELECT * FROM users ORDER BY name", "sorted_users"},
		{"view with LIMIT", "CREATE VIEW top_users AS SELECT * FROM users LIMIT 10", "top_users"},
	}
	for _, tt := range basicViewTests {
		runCreateViewSubtest(t, tt.name, tt.sql, func(t *testing.T, stmt *CreateViewStmt) {
			assertViewName(t, stmt, tt.wantName)
			assertViewHasSelect(t, stmt)
		})
	}
}

func testParseCreateViewOptions(t *testing.T) {
	t.Helper()
	tests := []struct {
		name          string
		sql           string
		wantName      string
		wantTemp      bool
		wantIfNotExist bool
	}{
		{"view with IF NOT EXISTS", "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM users", "v", false, true},
		{"temporary view", "CREATE TEMP VIEW v AS SELECT * FROM users", "v", true, false},
		{"temporary view with TEMPORARY keyword", "CREATE TEMPORARY VIEW v AS SELECT * FROM users", "v", true, false},
		{"temporary view with IF NOT EXISTS", "CREATE TEMP VIEW IF NOT EXISTS v AS SELECT * FROM users", "v", true, true},
	}
	for _, tt := range tests {
		runCreateViewSubtest(t, tt.name, tt.sql, func(t *testing.T, stmt *CreateViewStmt) {
			assertViewName(t, stmt, tt.wantName)
			if stmt.Temporary != tt.wantTemp {
				t.Errorf("expected Temporary=%v, got %v", tt.wantTemp, stmt.Temporary)
			}
			if stmt.IfNotExists != tt.wantIfNotExist {
				t.Errorf("expected IfNotExists=%v, got %v", tt.wantIfNotExist, stmt.IfNotExists)
			}
		})
	}
}

func testParseCreateViewQuoted(t *testing.T) {
	t.Helper()

	runCreateViewSubtest(t, "quoted view name",
		`CREATE VIEW "my view" AS SELECT * FROM users`,
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "my view" {
				t.Errorf("expected view name 'my view', got %q", stmt.Name)
			}
		})

	runCreateViewSubtest(t, "backtick quoted view name",
		"CREATE VIEW `my_view` AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "my_view" {
				t.Errorf("expected view name 'my_view', got %q", stmt.Name)
			}
		})
}

func testParseCreateViewErrors(t *testing.T) {
	t.Helper()
	runCreateViewErrorSubtest(t, "missing AS keyword", "CREATE VIEW v SELECT * FROM users")
	runCreateViewErrorSubtest(t, "missing SELECT statement", "CREATE VIEW v AS")
	runCreateViewErrorSubtest(t, "missing view name", "CREATE VIEW AS SELECT * FROM users")
	runCreateViewErrorSubtest(t, "incomplete column list", "CREATE VIEW v(id, name AS SELECT * FROM users")
	runCreateViewErrorSubtest(t, "missing closing paren in column list", "CREATE VIEW v(id, name AS SELECT * FROM users")
}

func parseDropViewStmt(t *testing.T, sql string) *DropViewStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*DropViewStmt)
	if !ok {
		t.Fatalf("expected DropViewStmt, got %T", stmts[0])
	}
	return stmt
}

func runDropViewSubtest(t *testing.T, name, sql string, check func(*testing.T, *DropViewStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseDropViewStmt(t, sql)
		check(t, stmt)
	})
}

func runDropViewErrorSubtest(t *testing.T, name, sql string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err == nil {
			t.Error("Expected error but got none")
		}
	})
}

// TestParseDropView tests DROP VIEW statement parsing.
func TestParseDropView(t *testing.T) {
	t.Parallel()
	testParseDropViewSuccess(t)
	testParseDropViewErrors(t)
}

func testParseDropViewSuccess(t *testing.T) {
	t.Helper()
	tests := []struct {
		name  string
		sql   string
		check func(*testing.T, *DropViewStmt)
	}{
		{
			name: "simple drop view",
			sql:  "DROP VIEW v",
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if stmt.IfExists {
					t.Error("expected IfExists to be false")
				}
			},
		},
		{
			name: "drop view with IF EXISTS",
			sql:  "DROP VIEW IF EXISTS v",
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if !stmt.IfExists {
					t.Error("expected IfExists to be true")
				}
			},
		},
		{
			name: "drop view with complex name",
			sql:  "DROP VIEW user_orders_view",
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "user_orders_view" {
					t.Errorf("expected view name 'user_orders_view', got %q", stmt.Name)
				}
			},
		},
		{
			name: "quoted view name",
			sql:  `DROP VIEW "my view"`,
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "my view" {
					t.Errorf("expected view name 'my view', got %q", stmt.Name)
				}
			},
		},
		{
			name: "backtick quoted view name",
			sql:  "DROP VIEW `my_view`",
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "my_view" {
					t.Errorf("expected view name 'my_view', got %q", stmt.Name)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		runDropViewSubtest(t, tt.name, tt.sql, tt.check)
	}
}

func testParseDropViewErrors(t *testing.T) {
	t.Helper()
	runDropViewErrorSubtest(t, "missing view name", "DROP VIEW")
	runDropViewErrorSubtest(t, "incomplete IF EXISTS", "DROP VIEW IF v")
}

// TestParseViewWithUnion tests CREATE VIEW with UNION queries.
func TestParseViewWithUnion(t *testing.T) {
	t.Parallel()
	sql := "CREATE VIEW all_items AS SELECT id, name FROM products UNION SELECT id, name FROM services"
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected CreateViewStmt, got %T", stmts[0])
	}
	if stmt.Name != "all_items" {
		t.Errorf("expected view name 'all_items', got %q", stmt.Name)
	}
	if stmt.Select == nil {
		t.Error("expected SELECT statement")
	}
	if stmt.Select.Compound == nil {
		t.Error("expected compound SELECT (UNION)")
	}
}

// TestParseViewWithSubquery tests CREATE VIEW with subqueries.
func TestParseViewWithSubquery(t *testing.T) {
	t.Parallel()
	sql := "CREATE VIEW high_value_users AS SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 1000)"
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateViewStmt)
	if !ok {
		t.Fatalf("expected CreateViewStmt, got %T", stmts[0])
	}
	if stmt.Name != "high_value_users" {
		t.Errorf("expected view name 'high_value_users', got %q", stmt.Name)
	}
}

func assertCreateViewAt(t *testing.T, stmts []Statement, idx int, wantName string) {
	t.Helper()
	stmt, ok := stmts[idx].(*CreateViewStmt)
	if !ok {
		t.Errorf("statement %d: expected CreateViewStmt, got %T", idx, stmts[idx])
		return
	}
	if stmt.Name != wantName {
		t.Errorf("statement %d: expected view name %q, got %q", idx, wantName, stmt.Name)
	}
}

func assertDropViewAt(t *testing.T, stmts []Statement, idx int, wantName string, wantIfExists bool) {
	t.Helper()
	stmt, ok := stmts[idx].(*DropViewStmt)
	if !ok {
		t.Errorf("statement %d: expected DropViewStmt, got %T", idx, stmts[idx])
		return
	}
	if stmt.Name != wantName {
		t.Errorf("statement %d: expected view name %q, got %q", idx, wantName, stmt.Name)
	}
	if stmt.IfExists != wantIfExists {
		t.Errorf("statement %d: expected IfExists=%v, got %v", idx, wantIfExists, stmt.IfExists)
	}
}

// TestMultipleViewStatements tests parsing multiple VIEW statements.
func TestMultipleViewStatements(t *testing.T) {
	t.Parallel()
	sql := `
		CREATE VIEW v1 AS SELECT * FROM t1;
		CREATE VIEW v2 AS SELECT * FROM t2;
		DROP VIEW IF EXISTS v1;
	`
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}

	assertCreateViewAt(t, stmts, 0, "v1")
	assertCreateViewAt(t, stmts, 1, "v2")
	assertDropViewAt(t, stmts, 2, "v1", true)
}
