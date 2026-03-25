// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// TestCompileAnalyzeTableNoIndexes verifies ANALYZE on a table with no
// user-defined indexes still produces a table-level stat row and no index rows.
// This exercises analyzeTableIndexes with an empty index list (loop never entered).
func TestCompileAnalyzeTableNoIndexes(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE no_idx(a INTEGER, b TEXT, c REAL)",
		"INSERT INTO no_idx VALUES(1, 'x', 1.1)",
		"INSERT INTO no_idx VALUES(2, 'y', 2.2)",
		"INSERT INTO no_idx VALUES(3, 'z', 3.3)",
		"ANALYZE no_idx",
	)

	// Should have exactly one stat row: the table-level row (no index rows).
	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='no_idx'")
	if cnt != 1 {
		t.Errorf("expected 1 stat row (table-level only) for no-index table, got %d", cnt)
	}

	// The single stat row must have NULL idx.
	stat := queryString(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='no_idx' AND idx IS NULL")
	if stat != "3" {
		t.Errorf("expected table stat '3', got %q", stat)
	}
}

// TestCompileAnalyzePreExistingStat1Table exercises the ensureSqliteStat1Table
// early-exit path (table already exists check). By pre-creating sqlite_stat1
// before ANALYZE runs, the GetTable lookup finds it and returns nil immediately
// instead of calling ExecDDL.
func TestCompileAnalyzePreExistingStat1Table(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	// Manually create sqlite_stat1 BEFORE ANALYZE. This means when ANALYZE
	// calls ensureSqliteStat1Table, GetTable("sqlite_stat1") returns true
	// and the function takes the early return nil path.
	analyzeExec(t, db,
		"CREATE TABLE sqlite_stat1(tbl TEXT, idx TEXT, stat TEXT)",
		"CREATE TABLE pre_stat(k INTEGER, v INTEGER)",
		"CREATE INDEX pre_stat_k ON pre_stat(k)",
		"INSERT INTO pre_stat VALUES(1, 10)",
		"INSERT INTO pre_stat VALUES(2, 20)",
		"ANALYZE pre_stat",
	)

	// sqlite_stat1 must still exist and have the stats.
	cntTable := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'")
	if cntTable != 1 {
		t.Errorf("sqlite_stat1 should appear exactly once in sqlite_master, got %d", cntTable)
	}

	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='pre_stat'")
	if cnt == 0 {
		t.Error("expected stat rows for pre_stat after ANALYZE")
	}
}

// TestCompileAnalyzeGlobalMixedIndexed verifies bare ANALYZE with a mix of
// indexed and non-indexed tables. Tables with no indexes only produce a
// table-level stat; indexed tables produce table + index stats.
func TestCompileAnalyzeGlobalMixedIndexed(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE mix_plain(id INTEGER, v TEXT)",
		"CREATE TABLE mix_idx(id INTEGER, v TEXT)",
		"CREATE INDEX mix_idx_v ON mix_idx(v)",
		"INSERT INTO mix_plain VALUES(1,'a'),(2,'b'),(3,'c')",
		"INSERT INTO mix_idx VALUES(4,'d'),(5,'e')",
		"ANALYZE",
	)

	// mix_plain: 1 stat row (table-level, no index).
	cntPlain := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='mix_plain'")
	if cntPlain != 1 {
		t.Errorf("expected 1 stat row for mix_plain (no indexes), got %d", cntPlain)
	}

	// mix_idx: 2 stat rows (table + 1 index).
	cntIdx := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='mix_idx'")
	if cntIdx != 2 {
		t.Errorf("expected 2 stat rows for mix_idx (table + index), got %d", cntIdx)
	}

	// mix_plain stat should be '3'.
	stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='mix_plain' AND idx IS NULL")
	if stat != "3" {
		t.Errorf("expected mix_plain stat '3', got %q", stat)
	}
}

// TestCompileAnalyzeLargeUniqueIndex verifies ANALYZE on a table where every
// row has a distinct indexed value, so avg rows per key = 1 for every prefix.
func TestCompileAnalyzeLargeUniqueIndex(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE uniq_tbl(id INTEGER, code TEXT)",
		"CREATE UNIQUE INDEX uniq_tbl_code ON uniq_tbl(code)",
	)

	for i := 0; i < 20; i++ {
		if _, err := db.Exec("INSERT INTO uniq_tbl VALUES(?, ?)", i, string(rune('A'+i))); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	analyzeExec(t, db, "ANALYZE uniq_tbl")

	stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='uniq_tbl' AND idx='uniq_tbl_code'")
	if stat == "" {
		t.Fatal("expected non-empty stat for uniq_tbl_code")
	}
	// Format: "20 1" — 20 rows, each key distinct so avg=1.
	parts := splitStatParts(stat)
	if len(parts) != 2 {
		t.Fatalf("expected 2 stat parts, got %d: %q", len(parts), stat)
	}
	if parts[0] != "20" {
		t.Errorf("expected total rows '20', got %q", parts[0])
	}
	if parts[1] != "1" {
		t.Errorf("expected avg rows per key '1' for unique index, got %q", parts[1])
	}
}

// TestCompileAnalyzeRerunAfterDataChange verifies that running ANALYZE
// multiple times on the same table does not accumulate stat rows.
// Each ANALYZE pass clears the old stats and writes fresh ones.
func TestCompileAnalyzeRerunAfterDataChange(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE grow_tbl(n INTEGER)",
		"CREATE INDEX grow_idx ON grow_tbl(n)",
		"INSERT INTO grow_tbl VALUES(1),(2),(3)",
		"ANALYZE grow_tbl",
	)

	cnt1 := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='grow_tbl'")
	if cnt1 == 0 {
		t.Fatal("expected stat rows after first ANALYZE")
	}

	// Re-ANALYZE without changes: stat row count must stay the same (no accumulation).
	analyzeExec(t, db, "ANALYZE grow_tbl")

	cnt2 := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='grow_tbl'")
	if cnt2 != cnt1 {
		t.Errorf("stat row count changed across ANALYZE re-runs: first=%d second=%d", cnt1, cnt2)
	}

	// The table-level stat must be present and numeric.
	stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='grow_tbl' AND idx IS NULL")
	if stat == "" {
		t.Error("expected non-empty table-level stat after re-ANALYZE")
	}
}

// TestCompileAnalyzeSpecificIndexName verifies ANALYZE with an index name
// correctly resolves to the parent table and computes stats for all its indexes.
func TestCompileAnalyzeSpecificIndexName(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE idxname_t(a INTEGER, b TEXT)",
		"CREATE INDEX idxname_a ON idxname_t(a)",
		"CREATE INDEX idxname_b ON idxname_t(b)",
		"INSERT INTO idxname_t VALUES(1,'x'),(2,'y'),(3,'z')",
		// ANALYZE by index name — should analyze parent table idxname_t.
		"ANALYZE idxname_a",
	)

	// Both indexes and the table-level row should have stats.
	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='idxname_t'")
	if cnt != 3 {
		t.Errorf("expected 3 stat rows (table + 2 indexes) for idxname_t, got %d", cnt)
	}
}

// TestCompileAnalyzeEmptyTableNoIndexes verifies that ANALYZE on a completely
// empty table with no indexes produces a single table-level stat of "0".
func TestCompileAnalyzeEmptyTableNoIndexes(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE bare_empty(x INTEGER)",
		"ANALYZE bare_empty",
	)

	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='bare_empty'")
	if cnt != 1 {
		t.Errorf("expected 1 stat row for bare_empty, got %d", cnt)
	}

	stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='bare_empty' AND idx IS NULL")
	if stat != "0" {
		t.Errorf("expected stat '0' for empty no-index table, got %q", stat)
	}
}

// splitStatParts splits a stat string into its space-delimited parts.
func splitStatParts(stat string) []string {
	var parts []string
	start := -1
	for i, ch := range stat {
		if ch != ' ' {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				parts = append(parts, stat[start:i])
				start = -1
			}
		}
	}
	if start != -1 {
		parts = append(parts, stat[start:])
	}
	return parts
}
