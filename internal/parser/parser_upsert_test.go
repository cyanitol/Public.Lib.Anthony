// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Helper functions to reduce cyclomatic complexity

func parseInsertStmt(t *testing.T, sql string) *InsertStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*InsertStmt)
	if !ok {
		t.Fatalf("expected InsertStmt, got %T", stmts[0])
	}
	return stmt
}

func validateUpsertExists(t *testing.T, stmt *InsertStmt) {
	t.Helper()
	if stmt.Upsert == nil {
		t.Fatal("expected Upsert clause")
	}
}

func validateUpsertAction(t *testing.T, upsert *UpsertClause, action ConflictAction) {
	t.Helper()
	if upsert.Action != action {
		t.Errorf("expected %v, got %v", action, upsert.Action)
	}
}

func validateNoConflictTarget(t *testing.T, upsert *UpsertClause) {
	t.Helper()
	if upsert.Target != nil {
		t.Errorf("expected no target for basic DO NOTHING")
	}
}

func validateConflictTarget(t *testing.T, upsert *UpsertClause) *ConflictTarget {
	t.Helper()
	if upsert.Target == nil {
		t.Fatal("expected conflict target")
	}
	return upsert.Target
}

func validateTargetColumnCount(t *testing.T, target *ConflictTarget, count int) {
	t.Helper()
	if len(target.Columns) != count {
		t.Errorf("expected %d column(s), got %d", count, len(target.Columns))
	}
}

func validateTargetColumn(t *testing.T, target *ConflictTarget, idx int, name string) {
	t.Helper()
	if target.Columns[idx].Column != name {
		t.Errorf("expected column '%s', got %s", name, target.Columns[idx].Column)
	}
}

func validateDoUpdateClause(t *testing.T, upsert *UpsertClause) *DoUpdateClause {
	t.Helper()
	if upsert.Update == nil {
		t.Fatal("expected DoUpdateClause")
	}
	return upsert.Update
}

func validateSetCount(t *testing.T, update *DoUpdateClause, count int) {
	t.Helper()
	if len(update.Sets) != count {
		t.Errorf("expected %d SET clause(s), got %d", count, len(update.Sets))
	}
}

func validateSetColumn(t *testing.T, update *DoUpdateClause, idx int, name string) {
	t.Helper()
	if update.Sets[idx].Column != name {
		t.Errorf("expected column '%s', got %s", name, update.Sets[idx].Column)
	}
}

func runUpsertSubtest(t *testing.T, name, sql string, check func(*testing.T, *InsertStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseInsertStmt(t, sql)
		check(t, stmt)
	})
}

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
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoNothing)
				validateNoConflictTarget(t, stmt.Upsert)
			},
		},
		{
			name:    "ON CONFLICT (column) DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoNothing)
				target := validateConflictTarget(t, stmt.Upsert)
				validateTargetColumnCount(t, target, 1)
				validateTargetColumn(t, target, 0, "id")
			},
		},
		{
			name:    "ON CONFLICT (col1, col2) DO NOTHING",
			sql:     "INSERT INTO users (id, email, name) VALUES (1, 'john@example.com', 'John') ON CONFLICT (id, email) DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				validateTargetColumnCount(t, target, 2)
			},
		},
		{
			name:    "ON CONFLICT (col) WHERE condition DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) WHERE id > 0 DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.Where == nil {
					t.Fatal("expected WHERE clause in conflict target")
				}
			},
		},
		{
			name:    "ON CONFLICT DO UPDATE SET single column",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE SET name = 'Jane'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoUpdate)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 1)
				validateSetColumn(t, update, 0, "name")
			},
		},
		{
			name:    "ON CONFLICT DO UPDATE SET multiple columns",
			sql:     "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com') ON CONFLICT DO UPDATE SET name = 'Jane', email = 'jane@example.com'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 2)
			},
		},
		{
			name:    "ON CONFLICT (id) DO UPDATE SET with excluded",
			sql:     "INSERT INTO users (id, name, count) VALUES (1, 'John', 5) ON CONFLICT (id) DO UPDATE SET count = count + 1",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateConflictTarget(t, stmt.Upsert)
				validateDoUpdateClause(t, stmt.Upsert)
			},
		},
		{
			name:    "ON CONFLICT (id) DO UPDATE SET ... WHERE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = 'Jane' WHERE id > 0",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				if update.Where == nil {
					t.Fatal("expected WHERE clause in DO UPDATE")
				}
			},
		},
		{
			name:    "ON CONFLICT ON CONSTRAINT name DO NOTHING",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", target.ConstraintName)
				}
			},
		},
		{
			name:    "ON CONFLICT ON CONSTRAINT name DO UPDATE",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = 'Jane'",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", target.ConstraintName)
				}
				validateDoUpdateClause(t, stmt.Upsert)
			},
		},
		{
			name:    "Complex UPSERT with expressions",
			sql:     "INSERT INTO stats (id, count, updated) VALUES (1, 1, NOW()) ON CONFLICT (id) DO UPDATE SET count = count + 1, updated = NOW()",
			wantErr: false,
			check: func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 2)
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
				validateUpsertExists(t, stmt)
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
				validateUpsertExists(t, stmt)
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
			if tt.wantErr {
				parser := NewParser(tt.sql)
				_, err := parser.Parse()
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			stmt := parseInsertStmt(t, tt.sql)
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
