package parser

import (
	"testing"
)

func TestParseCTE_Simple(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple CTE",
			sql:     "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "CTE with column list",
			sql:     "WITH cte(id, name) AS (SELECT id, name FROM users) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "CTE with where clause",
			sql:     "WITH cte AS (SELECT * FROM users WHERE age > 18) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "multiple CTEs",
			sql:     "WITH cte1 AS (SELECT * FROM users), cte2 AS (SELECT * FROM orders) SELECT * FROM cte1 JOIN cte2",
			wantErr: false,
		},
		{
			name:    "CTE with multiple columns in column list",
			sql:     "WITH cte(id, name, email, age) AS (SELECT id, name, email, age FROM users) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "nested CTE usage",
			sql:     "WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id IN (SELECT id FROM cte)",
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Errorf("expected 1 statement, got %d", len(stmts))
					return
				}
				stmt, ok := stmts[0].(*SelectStmt)
				if !ok {
					t.Errorf("expected SelectStmt, got %T", stmts[0])
					return
				}
				if stmt.With == nil {
					t.Errorf("expected WITH clause, got nil")
					return
				}
			}
		})
	}
}

func TestParseCTE_Recursive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple recursive CTE",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name:    "recursive CTE with column list",
			sql:     "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
			wantErr: false,
		},
		{
			name: "recursive hierarchy traversal",
			sql: `WITH RECURSIVE org_chart(id, name, manager_id, level) AS (
				SELECT id, name, manager_id, 0 FROM employees WHERE manager_id IS NULL
				UNION ALL
				SELECT e.id, e.name, e.manager_id, oc.level + 1
				FROM employees e
				JOIN org_chart oc ON e.manager_id = oc.id
			) SELECT * FROM org_chart`,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Errorf("expected 1 statement, got %d", len(stmts))
					return
				}
				stmt, ok := stmts[0].(*SelectStmt)
				if !ok {
					t.Errorf("expected SelectStmt, got %T", stmts[0])
					return
				}
				if stmt.With == nil {
					t.Errorf("expected WITH clause, got nil")
					return
				}
				if !stmt.With.Recursive {
					t.Errorf("expected RECURSIVE CTE, got non-recursive")
				}
			}
		})
	}
}

func TestParseCTE_Multiple(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		numCTEs  int
	}{
		{
			name:     "two CTEs",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
			wantErr:  false,
			numCTEs:  2,
		},
		{
			name:     "three CTEs",
			sql:      "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3) SELECT * FROM a, b, c",
			wantErr:  false,
			numCTEs:  3,
		},
		{
			name:     "CTEs with column lists",
			sql:      "WITH a(x) AS (SELECT 1), b(y) AS (SELECT 2) SELECT * FROM a, b",
			wantErr:  false,
			numCTEs:  2,
		},
		{
			name:     "mixed CTEs with and without column lists",
			sql:      "WITH a AS (SELECT 1), b(y, z) AS (SELECT 2, 3), c AS (SELECT 4) SELECT * FROM a, b, c",
			wantErr:  false,
			numCTEs:  3,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Errorf("expected 1 statement, got %d", len(stmts))
					return
				}
				stmt, ok := stmts[0].(*SelectStmt)
				if !ok {
					t.Errorf("expected SelectStmt, got %T", stmts[0])
					return
				}
				if stmt.With == nil {
					t.Errorf("expected WITH clause, got nil")
					return
				}
				if len(stmt.With.CTEs) != tt.numCTEs {
					t.Errorf("expected %d CTEs, got %d", tt.numCTEs, len(stmt.With.CTEs))
				}
			}
		})
	}
}

func TestParseCTE_WithColumnList(t *testing.T) {
	t.Parallel()
	sql := "WITH users_info(user_id, user_name, user_email) AS (SELECT id, name, email FROM users) SELECT * FROM users_info"

	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}

	stmt, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmts[0])
	}

	if stmt.With == nil {
		t.Fatal("expected WITH clause, got nil")
	}

	if len(stmt.With.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.With.CTEs))
	}

	cte := stmt.With.CTEs[0]

	if cte.Name != "users_info" {
		t.Errorf("expected CTE name 'users_info', got '%s'", cte.Name)
	}

	expectedColumns := []string{"user_id", "user_name", "user_email"}
	if len(cte.Columns) != len(expectedColumns) {
		t.Errorf("expected %d columns, got %d", len(expectedColumns), len(cte.Columns))
	}

	for i, col := range expectedColumns {
		if i >= len(cte.Columns) {
			break
		}
		if cte.Columns[i] != col {
			t.Errorf("expected column[%d] = '%s', got '%s'", i, col, cte.Columns[i])
		}
	}
}

func TestParseCTE_Complex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "CTE with JOIN",
			sql: `WITH user_orders AS (
				SELECT u.id, u.name, o.order_id
				FROM users u
				JOIN orders o ON u.id = o.user_id
			) SELECT * FROM user_orders`,
			wantErr: false,
		},
		{
			name: "CTE with GROUP BY and HAVING",
			sql: `WITH sales_summary AS (
				SELECT product_id, SUM(amount) as total
				FROM sales
				GROUP BY product_id
				HAVING SUM(amount) > 1000
			) SELECT * FROM sales_summary`,
			wantErr: false,
		},
		{
			name: "CTE with ORDER BY and LIMIT",
			sql: `WITH top_users AS (
				SELECT * FROM users
				ORDER BY score DESC
				LIMIT 10
			) SELECT * FROM top_users`,
			wantErr: false,
		},
		{
			name: "CTE with subquery",
			sql: `WITH filtered_users AS (
				SELECT * FROM users
				WHERE id IN (SELECT user_id FROM active_sessions)
			) SELECT * FROM filtered_users`,
			wantErr: false,
		},
		{
			name: "Multiple CTEs with dependencies",
			sql: `WITH
				active_users AS (SELECT * FROM users WHERE active = 1),
				user_orders AS (SELECT * FROM orders WHERE user_id IN (SELECT id FROM active_users))
			SELECT * FROM user_orders`,
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
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Errorf("expected 1 statement, got %d", len(stmts))
					return
				}
				stmt, ok := stmts[0].(*SelectStmt)
				if !ok {
					t.Errorf("expected SelectStmt, got %T", stmts[0])
					return
				}
				if stmt.With == nil {
					t.Errorf("expected WITH clause, got nil")
				}
			}
		})
	}
}

func TestParseCTE_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "missing AS keyword",
			sql:     "WITH cte (SELECT * FROM users) SELECT * FROM cte",
			wantErr: true,
		},
		{
			name:    "missing SELECT in CTE",
			sql:     "WITH cte AS (DELETE FROM users) SELECT * FROM cte",
			wantErr: true,
		},
		{
			name:    "missing closing paren",
			sql:     "WITH cte AS (SELECT * FROM users SELECT * FROM cte",
			wantErr: true,
		},
		{
			name:    "missing CTE name",
			sql:     "WITH AS (SELECT * FROM users) SELECT * FROM cte",
			wantErr: true,
		},
		{
			name:    "missing parentheses around SELECT",
			sql:     "WITH cte AS SELECT * FROM users SELECT * FROM cte",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseCTE_RecursiveFlag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		recursive bool
	}{
		{
			name:      "non-recursive CTE",
			sql:       "WITH cte AS (SELECT 1) SELECT * FROM cte",
			recursive: false,
		},
		{
			name:      "recursive CTE",
			sql:       "WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte",
			recursive: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			stmt, ok := stmts[0].(*SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmts[0])
			}

			if stmt.With == nil {
				t.Fatal("expected WITH clause, got nil")
			}

			if stmt.With.Recursive != tt.recursive {
				t.Errorf("expected Recursive = %v, got %v", tt.recursive, stmt.With.Recursive)
			}
		})
	}
}
