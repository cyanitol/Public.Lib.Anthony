package parser

import (
	"testing"
)

// TestParseCreateView tests CREATE VIEW statement parsing.
func TestParseCreateView(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, *CreateViewStmt)
	}{
		{
			name:    "simple view",
			sql:     "CREATE VIEW v AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
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
			},
		},
		{
			name:    "view with column list",
			sql:     "CREATE VIEW v(id, name, email) AS SELECT id, name, email FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
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
			},
		},
		{
			name:    "view with IF NOT EXISTS",
			sql:     "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if !stmt.IfNotExists {
					t.Error("expected IfNotExists to be true")
				}
			},
		},
		{
			name:    "temporary view",
			sql:     "CREATE TEMP VIEW v AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if !stmt.Temporary {
					t.Error("expected Temporary to be true")
				}
			},
		},
		{
			name:    "temporary view with TEMPORARY keyword",
			sql:     "CREATE TEMPORARY VIEW v AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if !stmt.Temporary {
					t.Error("expected Temporary to be true")
				}
			},
		},
		{
			name:    "temporary view with IF NOT EXISTS",
			sql:     "CREATE TEMP VIEW IF NOT EXISTS v AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "v" {
					t.Errorf("expected view name 'v', got %q", stmt.Name)
				}
				if !stmt.Temporary {
					t.Error("expected Temporary to be true")
				}
				if !stmt.IfNotExists {
					t.Error("expected IfNotExists to be true")
				}
			},
		},
		{
			name:    "view with complex SELECT",
			sql:     "CREATE VIEW user_orders AS SELECT u.id, u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "user_orders" {
					t.Errorf("expected view name 'user_orders', got %q", stmt.Name)
				}
				if stmt.Select == nil {
					t.Error("expected SELECT statement")
				}
			},
		},
		{
			name:    "view with WHERE clause",
			sql:     "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "active_users" {
					t.Errorf("expected view name 'active_users', got %q", stmt.Name)
				}
				if stmt.Select == nil {
					t.Error("expected SELECT statement")
				}
			},
		},
		{
			name:    "view with ORDER BY",
			sql:     "CREATE VIEW sorted_users AS SELECT * FROM users ORDER BY name",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "sorted_users" {
					t.Errorf("expected view name 'sorted_users', got %q", stmt.Name)
				}
			},
		},
		{
			name:    "view with LIMIT",
			sql:     "CREATE VIEW top_users AS SELECT * FROM users LIMIT 10",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "top_users" {
					t.Errorf("expected view name 'top_users', got %q", stmt.Name)
				}
			},
		},
		{
			name:    "quoted view name",
			sql:     `CREATE VIEW "my view" AS SELECT * FROM users`,
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "my view" {
					t.Errorf("expected view name 'my view', got %q", stmt.Name)
				}
			},
		},
		{
			name:    "backtick quoted view name",
			sql:     "CREATE VIEW `my_view` AS SELECT * FROM users",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateViewStmt) {
				if stmt.Name != "my_view" {
					t.Errorf("expected view name 'my_view', got %q", stmt.Name)
				}
			},
		},
		// Error cases
		{
			name:    "missing AS keyword",
			sql:     "CREATE VIEW v SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "missing SELECT statement",
			sql:     "CREATE VIEW v AS",
			wantErr: true,
		},
		{
			name:    "missing view name",
			sql:     "CREATE VIEW AS SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "incomplete column list",
			sql:     "CREATE VIEW v(id, name AS SELECT * FROM users",
			wantErr: true,
		},
		{
			name:    "missing closing paren in column list",
			sql:     "CREATE VIEW v(id, name AS SELECT * FROM users",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			stmt, ok := stmts[0].(*CreateViewStmt)
			if !ok {
				t.Errorf("expected CreateViewStmt, got %T", stmts[0])
				return
			}
			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}

// TestParseDropView tests DROP VIEW statement parsing.
func TestParseDropView(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, *DropViewStmt)
	}{
		{
			name:    "simple drop view",
			sql:     "DROP VIEW v",
			wantErr: false,
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
			name:    "drop view with IF EXISTS",
			sql:     "DROP VIEW IF EXISTS v",
			wantErr: false,
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
			name:    "drop view with complex name",
			sql:     "DROP VIEW user_orders_view",
			wantErr: false,
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "user_orders_view" {
					t.Errorf("expected view name 'user_orders_view', got %q", stmt.Name)
				}
			},
		},
		{
			name:    "quoted view name",
			sql:     `DROP VIEW "my view"`,
			wantErr: false,
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "my view" {
					t.Errorf("expected view name 'my view', got %q", stmt.Name)
				}
			},
		},
		{
			name:    "backtick quoted view name",
			sql:     "DROP VIEW `my_view`",
			wantErr: false,
			check: func(t *testing.T, stmt *DropViewStmt) {
				if stmt.Name != "my_view" {
					t.Errorf("expected view name 'my_view', got %q", stmt.Name)
				}
			},
		},
		// Error cases
		{
			name:    "missing view name",
			sql:     "DROP VIEW",
			wantErr: true,
		},
		{
			name:    "incomplete IF EXISTS",
			sql:     "DROP VIEW IF v",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			stmt, ok := stmts[0].(*DropViewStmt)
			if !ok {
				t.Errorf("expected DropViewStmt, got %T", stmts[0])
				return
			}
			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}

// TestParseViewWithUnion tests CREATE VIEW with UNION queries.
func TestParseViewWithUnion(t *testing.T) {
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
