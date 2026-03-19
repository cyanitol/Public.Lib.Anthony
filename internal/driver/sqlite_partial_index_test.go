// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestPartialIndex tests CREATE INDEX ... WHERE (partial indexes).
func TestPartialIndex(t *testing.T) {
	tests := []struct {
		name  string
		setup []string
		check func(*testing.T, *sql.DB)
	}{
		{
			name: "basic_partial_index_creation",
			setup: []string{
				"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)",
				"CREATE INDEX idx_t1_positive ON t1(a) WHERE a > 0",
			},
			check: func(t *testing.T, db *sql.DB) {
				// Verify the index exists via pragma index_list
				rows, err := db.Query("PRAGMA index_list(t1)")
				if err != nil {
					t.Fatalf("PRAGMA index_list failed: %v", err)
				}
				defer rows.Close()

				found := false
				for rows.Next() {
					var seq, unique, partial int
					var name, origin string
					if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					if name == "idx_t1_positive" {
						found = true
						if partial != 1 {
							t.Errorf("expected partial=1, got %d", partial)
						}
					}
				}
				if !found {
					t.Error("partial index idx_t1_positive not found in index_list")
				}
			},
		},
		{
			name: "partial_index_insert_and_query",
			setup: []string{
				"CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)",
				"CREATE INDEX idx_t2_pos ON t2(val) WHERE val > 0",
				"INSERT INTO t2 VALUES (1, -5)",
				"INSERT INTO t2 VALUES (2, 0)",
				"INSERT INTO t2 VALUES (3, 10)",
				"INSERT INTO t2 VALUES (4, 20)",
			},
			check: func(t *testing.T, db *sql.DB) {
				// Query that should match the partial index condition
				rows, err := db.Query("SELECT id, val FROM t2 WHERE val > 0 ORDER BY val")
				if err != nil {
					t.Fatalf("query failed: %v", err)
				}
				defer rows.Close()

				expected := []struct {
					id, val int
				}{{3, 10}, {4, 20}}

				i := 0
				for rows.Next() {
					var id, val int
					if err := rows.Scan(&id, &val); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					if i >= len(expected) {
						t.Fatalf("too many rows returned")
					}
					if id != expected[i].id || val != expected[i].val {
						t.Errorf("row %d: got (%d, %d), want (%d, %d)",
							i, id, val, expected[i].id, expected[i].val)
					}
					i++
				}
				if i != len(expected) {
					t.Errorf("got %d rows, want %d", i, len(expected))
				}
			},
		},
		{
			name: "multiple_partial_indexes_same_table",
			setup: []string{
				"CREATE TABLE t3 (a INTEGER, b TEXT, c REAL)",
				"CREATE INDEX idx_t3_a_pos ON t3(a) WHERE a > 0",
				"CREATE INDEX idx_t3_b_notnull ON t3(b) WHERE b IS NOT NULL",
			},
			check: func(t *testing.T, db *sql.DB) {
				rows, err := db.Query("PRAGMA index_list(t3)")
				if err != nil {
					t.Fatalf("PRAGMA index_list failed: %v", err)
				}
				defer rows.Close()

				partialCount := 0
				for rows.Next() {
					var seq, unique, partial int
					var name, origin string
					if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					if partial == 1 {
						partialCount++
					}
				}
				if partialCount != 2 {
					t.Errorf("expected 2 partial indexes, got %d", partialCount)
				}
			},
		},
		{
			name: "partial_index_compound_where",
			setup: []string{
				"CREATE TABLE t4 (x INTEGER, y INTEGER, z TEXT)",
				"CREATE INDEX idx_t4_compound ON t4(x) WHERE x > 0 AND y < 100",
			},
			check: func(t *testing.T, db *sql.DB) {
				rows, err := db.Query("PRAGMA index_list(t4)")
				if err != nil {
					t.Fatalf("PRAGMA index_list failed: %v", err)
				}
				defer rows.Close()

				found := false
				for rows.Next() {
					var seq, unique, partial int
					var name, origin string
					if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					if name == "idx_t4_compound" {
						found = true
						if partial != 1 {
							t.Errorf("expected partial=1 for compound WHERE, got %d", partial)
						}
					}
				}
				if !found {
					t.Error("compound partial index idx_t4_compound not found")
				}
			},
		},
		{
			name: "partial_index_unique",
			setup: []string{
				"CREATE TABLE t5 (a INTEGER, b TEXT)",
				"CREATE UNIQUE INDEX idx_t5_unique_partial ON t5(a) WHERE b IS NOT NULL",
			},
			check: func(t *testing.T, db *sql.DB) {
				rows, err := db.Query("PRAGMA index_list(t5)")
				if err != nil {
					t.Fatalf("PRAGMA index_list failed: %v", err)
				}
				defer rows.Close()

				found := false
				for rows.Next() {
					var seq, unique, partial int
					var name, origin string
					if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					if name == "idx_t5_unique_partial" {
						found = true
						if unique != 1 {
							t.Errorf("expected unique=1, got %d", unique)
						}
						if partial != 1 {
							t.Errorf("expected partial=1, got %d", partial)
						}
					}
				}
				if !found {
					t.Error("unique partial index idx_t5_unique_partial not found")
				}
			},
		},
		{
			name: "partial_vs_non_partial_index_list",
			setup: []string{
				"CREATE TABLE t6 (a INTEGER, b TEXT)",
				"CREATE INDEX idx_t6_full ON t6(a)",
				"CREATE INDEX idx_t6_partial ON t6(b) WHERE b IS NOT NULL",
			},
			check: func(t *testing.T, db *sql.DB) {
				rows, err := db.Query("PRAGMA index_list(t6)")
				if err != nil {
					t.Fatalf("PRAGMA index_list failed: %v", err)
				}
				defer rows.Close()

				results := make(map[string]int) // name -> partial
				for rows.Next() {
					var seq, unique, partial int
					var name, origin string
					if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
						t.Fatalf("scan failed: %v", err)
					}
					results[name] = partial
				}

				if p, ok := results["idx_t6_full"]; !ok {
					t.Error("idx_t6_full not found")
				} else if p != 0 {
					t.Errorf("idx_t6_full: expected partial=0, got %d", p)
				}

				if p, ok := results["idx_t6_partial"]; !ok {
					t.Error("idx_t6_partial not found")
				} else if p != 1 {
					t.Errorf("idx_t6_partial: expected partial=1, got %d", p)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupMemoryDB(t)
			defer db.Close()

			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed on %q: %v", stmt, err)
				}
			}

			if tt.check != nil {
				tt.check(t, db)
			}
		})
	}
}
