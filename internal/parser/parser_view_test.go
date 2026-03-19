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

func testParseCreateViewBasic(t *testing.T) {
	t.Helper()

	runCreateViewSubtest(t, "simple view",
		"CREATE VIEW v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if stmt.IfNotExists {
				t.Error("expected IfNotExists to be false")
			}
			if stmt.Temporary {
				t.Error("expected Temporary to be false")
			}
			if len(stmt.Columns) != 0 {
				t.Errorf("expected no columns, got %d", len(stmt.Columns))
			}
			if stmt.Select == nil {
				t.Error("expected SELECT statement")
			}
		})

	runCreateViewSubtest(t, "view with column list",
		"CREATE VIEW v(id, name, email) AS SELECT id, name, email FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if len(stmt.Columns) != 3 {
				t.Errorf("expected 3 columns, got %d", len(stmt.Columns))
			}
			expectedCols := []string{"id", "name", "email"}
			for i, col := range stmt.Columns {
				if col != expectedCols[i] {
					t.Errorf("column %d: expected %q, got %q", i, expectedCols[i], col)
				}
			}
		})

	runCreateViewSubtest(t, "view with complex SELECT",
		"CREATE VIEW user_orders AS SELECT u.id, u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "user_orders" {
				t.Errorf("expected view name 'user_orders', got %q", stmt.Name)
			}
			if stmt.Select == nil {
				t.Error("expected SELECT statement")
			}
		})

	runCreateViewSubtest(t, "view with WHERE clause",
		"CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "active_users" {
				t.Errorf("expected view name 'active_users', got %q", stmt.Name)
			}
			if stmt.Select == nil {
				t.Error("expected SELECT statement")
			}
		})

	runCreateViewSubtest(t, "view with ORDER BY",
		"CREATE VIEW sorted_users AS SELECT * FROM users ORDER BY name",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "sorted_users" {
				t.Errorf("expected view name 'sorted_users', got %q", stmt.Name)
			}
		})

	runCreateViewSubtest(t, "view with LIMIT",
		"CREATE VIEW top_users AS SELECT * FROM users LIMIT 10",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "top_users" {
				t.Errorf("expected view name 'top_users', got %q", stmt.Name)
			}
		})
}

func testParseCreateViewOptions(t *testing.T) {
	t.Helper()

	runCreateViewSubtest(t, "view with IF NOT EXISTS",
		"CREATE VIEW IF NOT EXISTS v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if !stmt.IfNotExists {
				t.Error("expected IfNotExists to be true")
			}
		})

	runCreateViewSubtest(t, "temporary view",
		"CREATE TEMP VIEW v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if !stmt.Temporary {
				t.Error("expected Temporary to be true")
			}
		})

	runCreateViewSubtest(t, "temporary view with TEMPORARY keyword",
		"CREATE TEMPORARY VIEW v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if !stmt.Temporary {
				t.Error("expected Temporary to be true")
			}
		})

	runCreateViewSubtest(t, "temporary view with IF NOT EXISTS",
		"CREATE TEMP VIEW IF NOT EXISTS v AS SELECT * FROM users",
		func(t *testing.T, stmt *CreateViewStmt) {
			if stmt.Name != "v" {
				t.Errorf("expected view name 'v', got %q", stmt.Name)
			}
			if !stmt.Temporary {
				t.Error("expected Temporary to be true")
			}
			if !stmt.IfNotExists {
				t.Error("expected IfNotExists to be true")
			}
		})
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

	// Check first CREATE VIEW
	if stmt1, ok := stmts[0].(*CreateViewStmt); !ok {
		t.Errorf("statement 0: expected CreateViewStmt, got %T", stmts[0])
	} else if stmt1.Name != "v1" {
		t.Errorf("statement 0: expected view name 'v1', got %q", stmt1.Name)
	}

	// Check second CREATE VIEW
	if stmt2, ok := stmts[1].(*CreateViewStmt); !ok {
		t.Errorf("statement 1: expected CreateViewStmt, got %T", stmts[1])
	} else if stmt2.Name != "v2" {
		t.Errorf("statement 1: expected view name 'v2', got %q", stmt2.Name)
	}

	// Check DROP VIEW
	if stmt3, ok := stmts[2].(*DropViewStmt); !ok {
		t.Errorf("statement 2: expected DropViewStmt, got %T", stmts[2])
	} else {
		if stmt3.Name != "v1" {
			t.Errorf("statement 2: expected view name 'v1', got %q", stmt3.Name)
		}
		if !stmt3.IfExists {
			t.Error("statement 2: expected IfExists to be true")
		}
	}
}
