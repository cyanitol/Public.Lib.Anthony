// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// partialIndexSetup opens a memory DB, executes setup stmts, and returns it.
func partialIndexSetup(t *testing.T, stmts ...string) *sql.DB {
	t.Helper()
	db := setupMemoryDB(t)
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup failed on %q: %v", s, err)
		}
	}
	return db
}

// partialIndexFind checks PRAGMA index_list for the given table and returns
// a map of index name -> partial flag.
func partialIndexFind(t *testing.T, db *sql.DB, table string) map[string]int {
	t.Helper()
	rows, err := db.Query("PRAGMA index_list(" + table + ")")
	if err != nil {
		t.Fatalf("PRAGMA index_list failed: %v", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		result[name] = partial
	}
	return result
}

// TestPartialIndexCreation tests basic partial index creation.
func TestPartialIndexCreation(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)",
		"CREATE INDEX idx_t1_positive ON t1(a) WHERE a > 0",
	)
	defer db.Close()

	idxMap := partialIndexFind(t, db, "t1")
	p, ok := idxMap["idx_t1_positive"]
	if !ok {
		t.Fatal("partial index idx_t1_positive not found in index_list")
	}
	if p != 1 {
		t.Errorf("expected partial=1, got %d", p)
	}
}

// TestPartialIndexInsertAndQuery tests insert and query with partial index.
func TestPartialIndexInsertAndQuery(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)",
		"CREATE INDEX idx_t2_pos ON t2(val) WHERE val > 0",
		"INSERT INTO t2 VALUES (1, -5)",
		"INSERT INTO t2 VALUES (2, 0)",
		"INSERT INTO t2 VALUES (3, 10)",
		"INSERT INTO t2 VALUES (4, 20)",
	)
	defer db.Close()

	rows, err := db.Query("SELECT id, val FROM t2 WHERE val > 0 ORDER BY val")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	expected := []struct{ id, val int }{{3, 10}, {4, 20}}
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
}

// TestPartialIndexMultipleSameTable tests multiple partial indexes on one table.
func TestPartialIndexMultipleSameTable(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t3 (a INTEGER, b TEXT, c REAL)",
		"CREATE INDEX idx_t3_a_pos ON t3(a) WHERE a > 0",
		"CREATE INDEX idx_t3_b_notnull ON t3(b) WHERE b IS NOT NULL",
	)
	defer db.Close()

	idxMap := partialIndexFind(t, db, "t3")
	partialCount := 0
	for _, p := range idxMap {
		if p == 1 {
			partialCount++
		}
	}
	if partialCount != 2 {
		t.Errorf("expected 2 partial indexes, got %d", partialCount)
	}
}

// TestPartialIndexCompoundWhere tests a partial index with compound WHERE.
func TestPartialIndexCompoundWhere(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t4 (x INTEGER, y INTEGER, z TEXT)",
		"CREATE INDEX idx_t4_compound ON t4(x) WHERE x > 0 AND y < 100",
	)
	defer db.Close()

	idxMap := partialIndexFind(t, db, "t4")
	p, ok := idxMap["idx_t4_compound"]
	if !ok {
		t.Fatal("compound partial index idx_t4_compound not found")
	}
	if p != 1 {
		t.Errorf("expected partial=1 for compound WHERE, got %d", p)
	}
}

// TestPartialIndexUnique tests a unique partial index.
func TestPartialIndexUnique(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t5 (a INTEGER, b TEXT)",
		"CREATE UNIQUE INDEX idx_t5_unique_partial ON t5(a) WHERE b IS NOT NULL",
	)
	defer db.Close()

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
}

// TestPartialIndexVsNonPartialList tests mix of partial and non-partial indexes.
func TestPartialIndexVsNonPartialList(t *testing.T) {
	db := partialIndexSetup(t,
		"CREATE TABLE t6 (a INTEGER, b TEXT)",
		"CREATE INDEX idx_t6_full ON t6(a)",
		"CREATE INDEX idx_t6_partial ON t6(b) WHERE b IS NOT NULL",
	)
	defer db.Close()

	idxMap := partialIndexFind(t, db, "t6")

	if p, ok := idxMap["idx_t6_full"]; !ok {
		t.Error("idx_t6_full not found")
	} else if p != 0 {
		t.Errorf("idx_t6_full: expected partial=0, got %d", p)
	}

	if p, ok := idxMap["idx_t6_partial"]; !ok {
		t.Error("idx_t6_partial not found")
	} else if p != 1 {
		t.Errorf("idx_t6_partial: expected partial=1, got %d", p)
	}
}
