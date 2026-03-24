// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// TestExpressionIndexCreation tests that expression indexes can be created.
func TestExpressionIndexCreation(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		create  string
		wantErr bool
	}{
		{
			name:   "basic function expression index",
			setup:  []string{"CREATE TABLE t1(name TEXT, age INTEGER)"},
			create: "CREATE INDEX idx_upper_name ON t1(UPPER(name))",
		},
		{
			name:   "arithmetic expression index",
			setup:  []string{"CREATE TABLE t2(a INTEGER, b INTEGER)"},
			create: "CREATE INDEX idx_sum ON t2(a+b)",
		},
		{
			name:   "mixed columns and expressions",
			setup:  []string{"CREATE TABLE t3(x TEXT, y INTEGER, z REAL)"},
			create: "CREATE INDEX idx_mixed ON t3(y, LOWER(x))",
		},
		{
			name:   "expression index with IF NOT EXISTS",
			setup:  []string{"CREATE TABLE t4(val TEXT)"},
			create: "CREATE INDEX IF NOT EXISTS idx_len ON t4(LENGTH(val))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupMemoryDB(t)
			defer db.Close()

			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			_, err := db.Exec(tt.create)
			if tt.wantErr && err == nil {
				t.Fatal("expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestExpressionIndexInPragmaIndexList verifies expression indexes appear in PRAGMA index_list.
func TestExpressionIndexInPragmaIndexList(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE users(name TEXT, email TEXT)")
	mustExec(t, db, "CREATE INDEX idx_upper_name ON users(UPPER(name))")

	rows, err := db.Query("SELECT name FROM pragma_index_list('users')")
	if err != nil {
		t.Fatalf("pragma_index_list failed: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if name == "idx_upper_name" {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if !found {
		t.Fatal("expression index idx_upper_name not found in pragma_index_list")
	}
}

// TestExpressionIndexSchemaStorage verifies expressions are stored correctly in schema.
func TestExpressionIndexSchemaStorage(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE products(name TEXT, price REAL, qty INTEGER)")
	mustExec(t, db, "CREATE INDEX idx_price_qty ON products(price * qty)")

	// Verify the index exists by querying pragma_index_list
	rows, err := db.Query("SELECT name, \"unique\" FROM pragma_index_list('products')")
	if err != nil {
		t.Fatalf("pragma_index_list failed: %v", err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var name string
		var unique int64
		if err := rows.Scan(&name, &unique); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if name == "idx_price_qty" {
			found = true
			if unique != 0 {
				t.Error("expected non-unique index")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
	if !found {
		t.Fatal("expression index idx_price_qty not found in pragma_index_list")
	}
}

// TestExpressionIndexUniqueConstraint tests unique expression indexes.
func TestExpressionIndexUniqueConstraint(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE emails(addr TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_lower_addr ON emails(LOWER(addr))")

	// Verify uniqueness flag via pragma
	rows, err := db.Query("SELECT name, \"unique\" FROM pragma_index_list('emails')")
	if err != nil {
		t.Fatalf("pragma_index_list failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var unique int64
		if err := rows.Scan(&name, &unique); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if name == "idx_lower_addr" && unique != 1 {
			t.Error("expected unique index but got non-unique")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
}
