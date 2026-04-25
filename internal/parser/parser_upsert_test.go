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

func testParseUpsertSuccess(t *testing.T) {
	t.Helper()
	tests := []struct {
		name  string
		sql   string
		check func(*testing.T, *InsertStmt)
	}{
		{"ON CONFLICT DO NOTHING", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoNothing)
				validateNoConflictTarget(t, stmt.Upsert)
			}},
		{"ON CONFLICT (column) DO NOTHING", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoNothing)
				target := validateConflictTarget(t, stmt.Upsert)
				validateTargetColumnCount(t, target, 1)
				validateTargetColumn(t, target, 0, "id")
			}},
		{"ON CONFLICT (col1, col2) DO NOTHING", "INSERT INTO users (id, email, name) VALUES (1, 'john@example.com', 'John') ON CONFLICT (id, email) DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				validateTargetColumnCount(t, target, 2)
			}},
		{"ON CONFLICT (col) WHERE condition DO NOTHING", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) WHERE id > 0 DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.Where == nil {
					t.Fatal("expected WHERE clause in conflict target")
				}
			}},
		{"ON CONFLICT DO UPDATE SET single column", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE SET name = 'Jane'",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateUpsertAction(t, stmt.Upsert, ConflictDoUpdate)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 1)
				validateSetColumn(t, update, 0, "name")
			}},
		{"ON CONFLICT DO UPDATE SET multiple columns", "INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com') ON CONFLICT DO UPDATE SET name = 'Jane', email = 'jane@example.com'",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 2)
			}},
		{"ON CONFLICT (id) DO UPDATE SET with excluded", "INSERT INTO users (id, name, count) VALUES (1, 'John', 5) ON CONFLICT (id) DO UPDATE SET count = count + 1",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				validateConflictTarget(t, stmt.Upsert)
				validateDoUpdateClause(t, stmt.Upsert)
			}},
		{"ON CONFLICT (id) DO UPDATE SET ... WHERE", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO UPDATE SET name = 'Jane' WHERE id > 0",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				if update.Where == nil {
					t.Fatal("expected WHERE clause in DO UPDATE")
				}
			}},
		{"ON CONFLICT ON CONSTRAINT name DO NOTHING", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", target.ConstraintName)
				}
			}},
		{"ON CONFLICT ON CONSTRAINT name DO UPDATE", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = 'Jane'",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				target := validateConflictTarget(t, stmt.Upsert)
				if target.ConstraintName != "users_pkey" {
					t.Errorf("expected constraint 'users_pkey', got %s", target.ConstraintName)
				}
				validateDoUpdateClause(t, stmt.Upsert)
			}},
		{"Complex UPSERT with expressions", "INSERT INTO stats (id, count, updated) VALUES (1, 1, NOW()) ON CONFLICT (id) DO UPDATE SET count = count + 1, updated = NOW()",
			func(t *testing.T, stmt *InsertStmt) {
				validateUpsertExists(t, stmt)
				update := validateDoUpdateClause(t, stmt.Upsert)
				validateSetCount(t, update, 2)
			}},
		{"INSERT with SELECT and UPSERT", "INSERT INTO users (id, name) SELECT id, name FROM temp ON CONFLICT (id) DO UPDATE SET name = temp.name",
			func(t *testing.T, stmt *InsertStmt) {
				if stmt.Select == nil {
					t.Fatal("expected SELECT source")
				}
				validateUpsertExists(t, stmt)
			}},
		{"Multiple rows with UPSERT", "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane') ON CONFLICT (id) DO NOTHING",
			func(t *testing.T, stmt *InsertStmt) {
				if len(stmt.Values) != 2 {
					t.Errorf("expected 2 value rows, got %d", len(stmt.Values))
				}
				validateUpsertExists(t, stmt)
			}},
	}

	for _, tt := range tests {
		runUpsertSubtest(t, tt.name, tt.sql, tt.check)
	}
}

func testParseUpsertErrors(t *testing.T) {
	t.Helper()
	errorTests := []struct {
		name string
		sql  string
	}{
		{"Error: missing DO", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT NOTHING"},
		{"Error: missing action after DO", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO"},
		{"Error: missing SET after DO UPDATE", "INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO UPDATE"},
		{"Error: ON without CONFLICT", "INSERT INTO users (id, name) VALUES (1, 'John') ON DO NOTHING"},
	}

	for _, tt := range errorTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Error("Expected error but got none")
			}
		})
	}
}

func TestParseUpsert(t *testing.T) {
	t.Parallel()
	testParseUpsertSuccess(t)
	testParseUpsertErrors(t)
}

// assertConflictColumn checks a single conflict target column's name and order.
func assertConflictColumn(t *testing.T, col IndexedColumn, wantName string, wantOrder SortOrder) {
	t.Helper()
	if col.Column != wantName {
		t.Errorf("expected column %q, got %q", wantName, col.Column)
	}
	if col.Order != wantOrder {
		t.Errorf("expected order %v for column %q, got %v", wantOrder, wantName, col.Order)
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
		t.Fatalf("expected 2 columns, got %d", len(stmt.Upsert.Target.Columns))
	}
	assertConflictColumn(t, stmt.Upsert.Target.Columns[0], "id", SortAsc)
	assertConflictColumn(t, stmt.Upsert.Target.Columns[1], "email", SortDesc)
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
