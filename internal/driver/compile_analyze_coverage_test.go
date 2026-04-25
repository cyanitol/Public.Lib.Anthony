// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openAnalyzeDB opens an in-memory database for compile_analyze tests.
func openAnalyzeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// analyzeExec executes each statement and fatals on error.
func analyzeExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("analyzeExec %q: %v", s, err)
		}
	}
}

// analyzeExecLax executes statements, logging but not failing on error.
func analyzeExecLax(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Logf("analyzeExecLax (ignored) %q: %v", s, err)
		}
	}
}

// queryInt64 scans a single int64 from query or fatals.
func queryInt64(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("queryInt64 %q: %v", query, err)
	}
	return n
}

// queryString scans a single string from query, returns "" on ErrNoRows.
func queryString(t *testing.T, db *sql.DB, query string, args ...interface{}) string {
	t.Helper()
	var s string
	err := db.QueryRow(query, args...).Scan(&s)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		t.Fatalf("queryString %q: %v", query, err)
	}
	return s
}

// TestCompileAnalyzeEmptyTable verifies ANALYZE on an empty table creates
// a table-level sqlite_stat1 row with stat "0".
func TestCompileAnalyzeEmptyTable(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE empty_tbl(a INTEGER, b TEXT)",
		"ANALYZE empty_tbl",
	)

	// sqlite_stat1 must exist after ANALYZE.
	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='empty_tbl'")
	if cnt == 0 {
		t.Error("expected at least one sqlite_stat1 row for empty_tbl")
	}

	// The table-level stat should report 0 rows.
	stat := queryString(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='empty_tbl' AND idx IS NULL")
	if stat != "0" {
		t.Errorf("expected table-level stat '0' for empty table, got %q", stat)
	}
}

// TestCompileAnalyzeManyRowsMultipleIndexes verifies ANALYZE on a table with
// many rows and multiple indexes populates sqlite_stat1 for each index.
// analyzeInsertRows inserts n rows into the given table using the provided value function.
func analyzeInsertRows(t *testing.T, db *sql.DB, n int, sql string, argsFn func(i int) []interface{}) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := db.Exec(sql, argsFn(i)...); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
}

// analyzeCountIndexStats queries index stats and returns the count, verifying each stat starts with the given prefix.
func analyzeCountIndexStats(t *testing.T, db *sql.DB, tbl, prefix string) int {
	t.Helper()
	rows, err := db.Query("SELECT idx, stat FROM sqlite_stat1 WHERE tbl=? AND idx IS NOT NULL", tbl)
	if err != nil {
		t.Fatalf("query index stats: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var idx, stat string
		if err := rows.Scan(&idx, &stat); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !strings.HasPrefix(stat, prefix) {
			t.Errorf("index %q stat %q should start with %q", idx, stat, prefix)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return count
}

func TestCompileAnalyzeManyRowsMultipleIndexes(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE multi_idx(id INTEGER, cat INTEGER, val TEXT)",
		"CREATE INDEX mi_cat ON multi_idx(cat)",
		"CREATE INDEX mi_val ON multi_idx(val)",
		"CREATE INDEX mi_cat_val ON multi_idx(cat, val)",
	)

	analyzeInsertRows(t, db, 30, "INSERT INTO multi_idx VALUES(?, ?, ?)", func(i int) []interface{} {
		return []interface{}{i, i % 3, "v"}
	})

	analyzeExec(t, db, "ANALYZE multi_idx")

	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='multi_idx'")
	if cnt != 4 {
		t.Errorf("expected 4 stat rows (1 table + 3 indexes), got %d", cnt)
	}

	idxCount := analyzeCountIndexStats(t, db, "multi_idx", "30")
	if idxCount != 3 {
		t.Errorf("expected 3 index stat rows, got %d", idxCount)
	}
}

// TestCompileAnalyzeSpecificTableOrIndex verifies ANALYZE on a specific table
// name only populates stats for that table, not others.
func TestCompileAnalyzeSpecificTableOrIndex(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE tgt_a(id INTEGER, v INTEGER)",
		"CREATE TABLE tgt_b(id INTEGER, v INTEGER)",
		"CREATE INDEX tgt_a_v ON tgt_a(v)",
		"CREATE INDEX tgt_b_v ON tgt_b(v)",
		"INSERT INTO tgt_a VALUES(1,10),(2,20),(3,30)",
		"INSERT INTO tgt_b VALUES(4,40),(5,50)",
		"ANALYZE tgt_a",
	)

	// tgt_a should have stats (table row + index row = 2).
	cntA := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tgt_a'")
	if cntA != 2 {
		t.Errorf("expected 2 stat rows for tgt_a, got %d", cntA)
	}

	// tgt_b should have no stats since we only analyzed tgt_a.
	cntB := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tgt_b'")
	if cntB != 0 {
		t.Errorf("expected 0 stat rows for tgt_b, got %d", cntB)
	}
}

// TestCompileAnalyzeSpecificIndex verifies ANALYZE with an index name analyzes
// the parent table (exercises resolveNamedTarget index branch).
func TestCompileAnalyzeSpecificIndex(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE idx_tgt(k INTEGER, w INTEGER)",
		"CREATE INDEX idx_tgt_w ON idx_tgt(w)",
		"INSERT INTO idx_tgt VALUES(1,5),(2,10),(3,15)",
		// ANALYZE with the index name — should analyze the parent table.
		"ANALYZE idx_tgt_w",
	)

	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='idx_tgt'")
	if cnt == 0 {
		t.Error("expected sqlite_stat1 entries for idx_tgt after ANALYZE idx_tgt_w")
	}
}

// TestCompileAnalyzeCompositeIndex verifies that ANALYZE on a table with a
// composite index produces a stat string with the correct number of parts.
// For an index on (a, b, c) the stat format is "total avg_a avg_ab avg_abc".
func TestCompileAnalyzeCompositeIndex(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE comp_idx(a INTEGER, b INTEGER, c INTEGER)",
		"CREATE INDEX ci_abc ON comp_idx(a, b, c)",
		"INSERT INTO comp_idx VALUES(1,1,1)",
		"INSERT INTO comp_idx VALUES(1,1,2)",
		"INSERT INTO comp_idx VALUES(1,2,1)",
		"INSERT INTO comp_idx VALUES(2,1,1)",
		"INSERT INTO comp_idx VALUES(2,2,2)",
		"ANALYZE comp_idx",
	)

	stat := queryString(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='comp_idx' AND idx='ci_abc'")
	if stat == "" {
		t.Fatal("expected non-empty stat for composite index ci_abc")
	}
	// Stat should have 4 parts: total avg_a avg_ab avg_abc.
	parts := strings.Fields(stat)
	if len(parts) != 4 {
		t.Errorf("expected 4 stat parts for 3-column index, got %d: %q", len(parts), stat)
	}
	// First part is row count = 5.
	if parts[0] != "5" {
		t.Errorf("expected first stat part '5', got %q", parts[0])
	}
}

// TestCompileAnalyzeWithoutRowidTable verifies ANALYZE runs without error on a
// WITHOUT ROWID table and populates sqlite_stat1.
func TestCompileAnalyzeWithoutRowidTable(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE norowid(k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID",
		"INSERT INTO norowid VALUES('alpha', 1)",
		"INSERT INTO norowid VALUES('beta', 2)",
		"INSERT INTO norowid VALUES('gamma', 3)",
		"ANALYZE norowid",
	)

	cnt := queryInt64(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='norowid'")
	if cnt == 0 {
		t.Error("expected sqlite_stat1 entries for WITHOUT ROWID table after ANALYZE")
	}
}

// TestCompileAnalyzeSqliteStat1CreatedAndPopulated verifies that after ANALYZE:
// 1. sqlite_stat1 is created in sqlite_master.
// 2. It contains tbl, idx (NULL for table-level), and a numeric stat column.
func TestCompileAnalyzeSqliteStat1CreatedAndPopulated(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE stat_check(x INTEGER, y INTEGER)",
		"CREATE INDEX sc_x ON stat_check(x)",
		"INSERT INTO stat_check VALUES(1,10)",
		"INSERT INTO stat_check VALUES(2,20)",
		"INSERT INTO stat_check VALUES(3,30)",
		"ANALYZE stat_check",
	)

	// sqlite_stat1 must appear in sqlite_master.
	cnt := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'")
	if cnt != 1 {
		t.Error("sqlite_stat1 should appear in sqlite_master after ANALYZE")
	}

	// Table-level row: tbl='stat_check', idx IS NULL, stat='3'.
	tblStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='stat_check' AND idx IS NULL")
	if tblStat != "3" {
		t.Errorf("expected table-level stat '3', got %q", tblStat)
	}

	// Index row: tbl='stat_check', idx='sc_x', stat starts with '3'.
	idxStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='stat_check' AND idx='sc_x'")
	if !strings.HasPrefix(idxStat, "3") {
		t.Errorf("expected index stat to start with '3', got %q", idxStat)
	}
}

// TestCompileAnalyzeAfterInserts verifies that running ANALYZE after data inserts
// stores statistics and sqlite_stat1 is populated for both the initial empty
// state and after data is added.
func TestCompileAnalyzeAfterInserts(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE ins_tbl(a INTEGER, b INTEGER)",
		"CREATE INDEX ins_tbl_a ON ins_tbl(a)",
	)

	// First ANALYZE on empty table: sqlite_stat1 rows should be created.
	analyzeExec(t, db, "ANALYZE ins_tbl")
	cnt0 := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='ins_tbl'")
	// Engine creates 2 rows: table-level + index row.
	if cnt0 == 0 {
		t.Error("expected sqlite_stat1 rows after first ANALYZE (empty table)")
	}

	// Insert 10 rows and re-ANALYZE.
	for i := 1; i <= 10; i++ {
		if _, err := db.Exec("INSERT INTO ins_tbl VALUES(?, ?)", i, i*2); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	analyzeExec(t, db, "ANALYZE ins_tbl")

	// After re-ANALYZE the stat rows should still exist (not accumulate).
	cnt10 := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='ins_tbl'")
	if cnt10 == 0 {
		t.Error("expected sqlite_stat1 rows after second ANALYZE (10 rows)")
	}
	if cnt10 != cnt0 {
		t.Errorf("stat row count changed across ANALYZE runs: first=%d second=%d", cnt0, cnt10)
	}

	// Index stat must be present with at least 2 parts.
	idxStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='ins_tbl' AND idx='ins_tbl_a'")
	if idxStat == "" {
		t.Error("expected non-empty index stat after ANALYZE with 10 rows")
	}
	parts := strings.Fields(idxStat)
	if len(parts) < 2 {
		t.Errorf("expected at least 2 stat parts for index, got %q", idxStat)
	}
}

// TestCompileAnalyzeNonExistentTable verifies ANALYZE on a non-existent table
// does not panic and either succeeds silently or returns an error.
func TestCompileAnalyzeNonExistentTable(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	// The engine resolves unknown names to nil tables and silently skips them.
	// Either outcome (success or error) is acceptable — we just must not panic.
	_, err := db.Exec("ANALYZE no_such_table_xyz")
	if err != nil {
		t.Logf("ANALYZE on non-existent table returned error (acceptable): %v", err)
	}
	// No panic = pass.
}

// TestCompileAnalyzeGlobalAllTables verifies that a bare ANALYZE statement
// (no table name) analyzes all user tables and populates sqlite_stat1 for each.
func TestCompileAnalyzeGlobalAllTables(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE ga1(id INTEGER, v TEXT)",
		"CREATE TABLE ga2(id INTEGER, v TEXT)",
		"CREATE INDEX ga1_v ON ga1(v)",
		"CREATE INDEX ga2_v ON ga2(v)",
		"INSERT INTO ga1 VALUES(1,'a'),(2,'b')",
		"INSERT INTO ga2 VALUES(3,'c'),(4,'d'),(5,'e')",
		"ANALYZE",
	)

	// Both tables must appear in sqlite_stat1.
	cntTables := queryInt64(t, db,
		"SELECT COUNT(DISTINCT tbl) FROM sqlite_stat1 WHERE tbl IN ('ga1','ga2')")
	if cntTables != 2 {
		t.Errorf("expected stats for 2 tables, got %d distinct tbl values", cntTables)
	}

	// Row counts should be correct.
	ga1Stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='ga1' AND idx IS NULL")
	if ga1Stat != "2" {
		t.Errorf("expected ga1 stat '2', got %q", ga1Stat)
	}
	ga2Stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='ga2' AND idx IS NULL")
	if ga2Stat != "3" {
		t.Errorf("expected ga2 stat '3', got %q", ga2Stat)
	}
}

// TestCompileAnalyzeRerunClearsOldStats verifies that running ANALYZE a second
// time replaces (not accumulates) statistics.
func TestCompileAnalyzeRerunClearsOldStats(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE rerun_t(n INTEGER)",
		"CREATE INDEX rerun_t_n ON rerun_t(n)",
		"INSERT INTO rerun_t VALUES(1),(2),(3)",
		"ANALYZE rerun_t",
	)

	// Verify initial stats.
	cnt1 := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='rerun_t'")
	if cnt1 != 2 {
		t.Errorf("expected 2 stat rows after first ANALYZE, got %d", cnt1)
	}

	// Run ANALYZE again — should not double-insert.
	analyzeExec(t, db, "ANALYZE rerun_t")

	cnt2 := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='rerun_t'")
	if cnt2 != 2 {
		t.Errorf("expected 2 stat rows after second ANALYZE (not %d); stats accumulating", cnt2)
	}
}

// TestCompileAnalyzeSkewedDataAvgRowsPerKey verifies that highly skewed data
// produces a stat where avg rows per key > 1 for the skewed column.
// This exercises computeAvgRowsPerKey returning rowCount/distinct > 1.
func TestCompileAnalyzeSkewedDataAvgRowsPerKey(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE skewed(cat INTEGER, val INTEGER)",
		"CREATE INDEX skewed_cat ON skewed(cat)",
	)

	// 10 rows all with cat=1 (very skewed).
	for i := 0; i < 10; i++ {
		if _, err := db.Exec("INSERT INTO skewed VALUES(1, ?)", i); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	analyzeExec(t, db, "ANALYZE skewed")

	idxStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='skewed' AND idx='skewed_cat'")
	if idxStat == "" {
		t.Fatal("expected non-empty stat for skewed_cat index")
	}
	// Format: "10 10" — 10 rows, 10 avg rows per 1 distinct value.
	parts := strings.Fields(idxStat)
	if len(parts) != 2 {
		t.Errorf("expected 2 stat parts, got %d: %q", len(parts), idxStat)
	}
	if parts[0] != "10" {
		t.Errorf("expected total rows '10', got %q", parts[0])
	}
	// Average rows per key should be 10 (10 rows / 1 distinct value).
	if parts[1] != "10" {
		t.Errorf("expected avg rows per key '10', got %q", parts[1])
	}
}

// TestCompileAnalyzeSingleRowTable verifies that a table with exactly one row
// produces avg rows per key = 1 (not 0), exercising the avg<=0 clamp in
// computeAvgRowsPerKey.
func TestCompileAnalyzeSingleRowTable(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE single_row(x INTEGER, y INTEGER)",
		"CREATE INDEX sr_x ON single_row(x)",
		"INSERT INTO single_row VALUES(42, 99)",
		"ANALYZE single_row",
	)

	idxStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='single_row' AND idx='sr_x'")
	if idxStat == "" {
		t.Fatal("expected non-empty stat for sr_x index")
	}
	// 1 row, 1 distinct value: "1 1".
	parts := strings.Fields(idxStat)
	if len(parts) < 2 {
		t.Fatalf("expected at least 2 stat parts, got %d: %q", len(parts), idxStat)
	}
	if parts[1] == "0" {
		t.Errorf("avg rows per key should never be 0, got stat %q", idxStat)
	}
}

// TestCompileAnalyzeCountDistinctPrefixValLEZeroClamp verifies that when all
// rows have NULL values in an indexed column, countDistinctPrefix returns at
// least 1 (not 0), so the stat remains valid.
func TestCompileAnalyzeCountDistinctPrefixValLEZeroClamp(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE null_idx(a INTEGER, b INTEGER)",
		"CREATE INDEX null_idx_b ON null_idx(b)",
		"INSERT INTO null_idx VALUES(1, NULL)",
		"INSERT INTO null_idx VALUES(2, NULL)",
		"INSERT INTO null_idx VALUES(3, NULL)",
		"ANALYZE null_idx",
	)

	idxStat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='null_idx' AND idx='null_idx_b'")
	if idxStat == "" {
		t.Fatal("expected non-empty stat for null_idx_b index")
	}
	// No zero parts allowed in stat.
	parts := strings.Fields(idxStat)
	for i, p := range parts {
		if p == "0" && i > 0 {
			t.Errorf("stat part %d should not be 0 in %q", i, idxStat)
		}
	}
}

// TestCompileAnalyzeEnsureStat1TableIdempotent verifies ensureSqliteStat1Table
// is idempotent: multiple ANALYZE calls do not duplicate the sqlite_stat1 table
// in sqlite_master.
func TestCompileAnalyzeEnsureStat1TableIdempotent(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE idem_t(v INTEGER)",
		"INSERT INTO idem_t VALUES(1),(2),(3)",
		"ANALYZE idem_t",
		"ANALYZE idem_t",
		"ANALYZE idem_t",
	)

	// sqlite_stat1 should appear exactly once in sqlite_master.
	cnt := queryInt64(t, db,
		"SELECT COUNT(*) FROM sqlite_master WHERE name='sqlite_stat1'")
	if cnt != 1 {
		t.Errorf("expected sqlite_stat1 to appear exactly once in sqlite_master, got %d", cnt)
	}
}

// TestCompileAnalyzeMultiColumnDistinct verifies countDistinctPrefix for
// multi-column index prefixes, exercising the len(columns)>1 subquery path.
func TestCompileAnalyzeMultiColumnDistinct(t *testing.T) {
	db := openAnalyzeDB(t)
	defer db.Close()

	analyzeExec(t, db,
		"CREATE TABLE mcd(a INTEGER, b INTEGER, c INTEGER)",
		"CREATE INDEX mcd_abc ON mcd(a, b, c)",
		// 8 rows: 2 distinct a, 2 distinct b per a, 2 distinct c per (a,b).
		"INSERT INTO mcd VALUES(1,1,1)",
		"INSERT INTO mcd VALUES(1,1,2)",
		"INSERT INTO mcd VALUES(1,2,1)",
		"INSERT INTO mcd VALUES(1,2,2)",
		"INSERT INTO mcd VALUES(2,1,1)",
		"INSERT INTO mcd VALUES(2,1,2)",
		"INSERT INTO mcd VALUES(2,2,1)",
		"INSERT INTO mcd VALUES(2,2,2)",
		"ANALYZE mcd",
	)

	stat := queryString(t, db,
		"SELECT stat FROM sqlite_stat1 WHERE tbl='mcd' AND idx='mcd_abc'")
	if stat == "" {
		t.Fatal("expected non-empty stat for mcd_abc")
	}

	// "8 4 2 1": 8 rows, 4 avg per a-prefix, 2 avg per (a,b)-prefix, 1 avg per (a,b,c)-prefix.
	parts := strings.Fields(stat)
	if len(parts) != 4 {
		t.Errorf("expected 4 stat parts for 3-column index, got %d: %q", len(parts), stat)
	}
	if parts[0] != "8" {
		t.Errorf("expected row count '8', got %q", parts[0])
	}
}
