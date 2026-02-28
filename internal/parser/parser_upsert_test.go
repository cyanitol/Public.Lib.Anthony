package parser

import (
	"testing"
)

func TestParseUpsert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, *InsertStmt)
	}{
		{
			name:    "ON CONFLICT DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Action != ConflictDoNothing {
					t.Errorf("expected ConflictDoNothing, got %v", stmt.Upsert.Action)
				}
				if stmt.Upsert.Target != nil {
					t.Errorf("expected no target for basic DO NOTHING")
				}
			},
		},
		{
			name:    "ON CONFLICT (column) DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Action != ConflictDoNothing {
					t.Errorf("expected ConflictDoNothing, got %v", stmt.Upsert.Action)
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if len(stmt.Upsert.Target.Columns) != 1 {
					t.Errorf("expected 1 column, got %d", len(stmt.Upsert.Target.Columns))
				}
				if stmt.Upsert.Target.Columns[0].Column != "id" {
					t.Errorf("expected column 'id', got %s", stmt.Upsert.Target.Columns[0].Column)
				}
			},
		},
		{
			name:    "ON CONFLICT (col1, col2) DO NOTHING",
			sql:     "INSERT INTO users (id, email, name) VALUES (1, 'john@example.com', 'John') ON CONFLICT (id, email) DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if len(stmt.Upsert.Target.Columns) != 2 {
					t.Errorf("expected 2 columns, got %d", len(stmt.Upsert.Target.Columns))
				}
			},
		},
		{
			name:    "ON CONFLICT (col) WHERE condition DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) WHERE id > 0 DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if stmt.Upsert.Target.Where == nil {
					t.Fatal("expected WHERE clause in conflict target")
				}
			},
		},
		{
			name:    "ON CONFLICT DO UPDATE SET single column",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE SET name = 'Jane'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Action != ConflictDoUpdate {
					t.Errorf("expected ConflictDoUpdate, got %v", stmt.Upsert.Action)
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
				if len(stmt.Upsert.Update.Sets) != 1 {
					t.Errorf("expected 1 SET clause, got %d", len(stmt.Upsert.Update.Sets))
				}
				if stmt.Upsert.Update.Sets[0].Column != "name" {
					t.Errorf("expected column 'name', got %s", stmt.Upsert.Update.Sets[0].Column)
				}
			},
		},
		{
			name:    "ON CONFLICT DO UPDATE SET multiple columns",
			sql:     "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com') ON CONFLICT DO UPDATE SET name = 'Jane', email = 'jane@example.com'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
				if len(stmt.Upsert.Update.Sets) != 2 {
					t.Errorf("expected 2 SET clauses, got %d", len(stmt.Upsert.Update.Sets))
				}
			},
		},
		{
			name:    "ON CONFLICT (id) DO UPDATE SET with excluded",
			sql:     "INSERT INTO users (id, name, count) VALUES (1, 'John', 5) ON CONFLICT (id) DO UPDATE SET count = count + 1",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
			},
		},
		{
			name:    "ON CONFLICT (id) DO UPDATE SET ... WHERE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = 'Jane' WHERE id > 0",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
				if stmt.Upsert.Update.Where == nil {
					t.Fatal("expected WHERE clause in DO UPDATE")
				}
			},
		},
		{
			name:    "ON CONFLICT ON CONSTRAINT name DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if stmt.Upsert.Target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", stmt.Upsert.Target.ConstraintName)
				}
			},
		},
		{
			name:    "ON CONFLICT ON CONSTRAINT name DO UPDATE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = 'Jane'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Target == nil {
					t.Fatal("expected conflict target")
				}
				if stmt.Upsert.Target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", stmt.Upsert.Target.ConstraintName)
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
			},
		},
		{
			name:    "Complex UPSERT with expressions",
			sql:     "INSERT INTO stats (id, count, updated) VALUES (1, 1, NOW()) ON CONFLICT (id) DO UPDATE SET count = count + 1, updated = NOW()",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
				if stmt.Upsert.Update == nil {
					t.Fatal("expected DoUpdateClause")
				}
				if len(stmt.Upsert.Update.Sets) != 2 {
					t.Errorf("expected 2 SET clauses, got %d", len(stmt.Upsert.Update.Sets))
				}
			},
		},
		{
			name:    "INSERT with SELECT and UPSERT",
			sql:     "INSERT INTO users (id, name) SELECT id, name FROM temp ON CONFLICT (id) DO UPDATE SET name = temp.name",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if stmt.Select == nil {
					t.Fatal("expected SELECT source")
				}
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
			},
		},
		{
			name:    "Multiple rows with UPSERT",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane') ON CONFLICT (id) DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				if len(stmt.Values) != 2 {
					t.Errorf("expected 2 value rows, got %d", len(stmt.Values))
				}
				if stmt.Upsert == nil {
					t.Fatal("expected Upsert clause")
				}
			},
		},
		{
			name:    "Error: missing DO",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT NOTHING",
			wantErr: true,
		},
		{
			name:    "Error: missing action after DO",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO",
			wantErr: true,
		},
		{
			name:    "Error: missing SET after DO UPDATE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE",
			wantErr: true,
		},
		{
			name:    "Error: ON without CONFLICT",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON DO NOTHING",
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

			stmt, ok := stmts[0].(*InsertStmt)
			if !ok {
				t.Errorf("expected InsertStmt, got %T", stmts[0])
				return
			}

			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}

func TestParseUpsertColumnOrder(t *testing.T) {
	t.Parallel()
	sql := "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id ASC, email DESC) DO NOTHING"
	parser := NewParser(sql)
	stmts, err := parser.Parse()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := stmts[0].(*InsertStmt)
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
	if stmt.Upsert.Target == nil {
		t.Fatal("expected conflict target")
	}
	if len(stmt.Upsert.Target.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(stmt.Upsert.Target.Columns))
	}
	if stmt.Upsert.Target.Columns[0].Column != "id" {
		t.Errorf("expected first column 'id', got %s", stmt.Upsert.Target.Columns[0].Column)
	}
	if stmt.Upsert.Target.Columns[0].Order != SortAsc {
		t.Errorf("expected first column ASC order")
	}
	if stmt.Upsert.Target.Columns[1].Column != "email" {
		t.Errorf("expected second column 'email', got %s", stmt.Upsert.Target.Columns[1].Column)
	}
	if stmt.Upsert.Target.Columns[1].Order != SortDesc {
		t.Errorf("expected second column DESC order")
	}
}

func TestParseInsertWithoutUpsert(t *testing.T) {
	t.Parallel()
	// Make sure regular INSERTs still work
	tests := []string{
		"INSERT INTO users (id, name) VALUES (1, 'John')",
		"INSERT INTO users VALUES (1, 'John')",
		"INSERT INTO users (id, name) SELECT id, name FROM temp",
		"INSERT INTO users DEFAULT VALUES",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(sql)
			stmts, err := parser.Parse()

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*InsertStmt)
			if !ok {
				t.Errorf("expected InsertStmt, got %T", stmts[0])
				return
			}

			if stmt.Upsert != nil {
				t.Error("expected no Upsert clause for regular INSERT")
			}
		})
	}
}
