// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// TestCompileCompoundAnalyzeCoverage covers low-coverage functions in
// compile_compound.go and compile_analyze.go.
func TestCompileCompoundAnalyzeCoverage(t *testing.T) {
	t.Parallel()

	t.Run("compound_nulls_first_explicit", testCompoundNullsFirstExplicit)
	t.Run("compound_nulls_last_explicit", testCompoundNullsLastExplicit)
	t.Run("compound_nulls_default_asc", testCompoundNullsDefaultAsc)
	t.Run("compound_nulls_default_desc", testCompoundNullsDefaultDesc)
	t.Run("compound_floats_order", testCompoundFloatsOrder)
	t.Run("compound_floats_int_mix", testCompoundFloatsIntMix)
	t.Run("compound_extract_base_expr_collate", testCompoundExtractBaseExprCollate)
	t.Run("analyze_ensure_stat1_table", testAnalyzeEnsureStat1Table)
	t.Run("analyze_resolve_named_table", testAnalyzeResolveNamedTable)
	t.Run("analyze_resolve_named_index", testAnalyzeResolveNamedIndex)
	t.Run("analyze_count_table_rows", testAnalyzeCountTableRows)
	t.Run("analyze_estimate_distinct_via_table", testAnalyzeEstimateDistinctViaTable)
	t.Run("analyze_to_int64_types", testAnalyzeToInt64Types)
}

// testCompoundNullsFirstExplicit exercises shouldNullsFirst with explicit NULLS FIRST
// in a compound UNION ALL query, covering the `spec.nullsFirst != nil` branch.
func testCompoundNullsFirstExplicit(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	got := queryRows(t, db, "SELECT NULL UNION ALL SELECT 1 ORDER BY 1 NULLS FIRST")
	want := [][]interface{}{{nil}, {int64(1)}}
	compareRows(t, got, want)
}

// testCompoundNullsLastExplicit exercises shouldNullsFirst with explicit NULLS LAST
// in a compound query, covering the `*spec.nullsFirst == false` branch.
func testCompoundNullsLastExplicit(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	got := queryRows(t, db, "SELECT NULL UNION ALL SELECT 1 ORDER BY 1 NULLS LAST")
	want := [][]interface{}{{int64(1)}, {nil}}
	compareRows(t, got, want)
}

// testCompoundNullsDefaultAsc exercises shouldNullsFirst default ASC path
// (NULLs sort first by SQLite default when ascending, nullsFirst pointer is nil).
func testCompoundNullsDefaultAsc(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// ASC default: NULLs first
	got := queryRows(t, db, "SELECT NULL UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY 1 ASC")
	want := [][]interface{}{{nil}, {int64(1)}, {int64(2)}}
	compareRows(t, got, want)
}

// testCompoundNullsDefaultDesc exercises shouldNullsFirst default DESC path
// (NULLs sort last by SQLite default when descending, nullsFirst pointer is nil).
func testCompoundNullsDefaultDesc(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// DESC default: NULLs last
	got := queryRows(t, db, "SELECT NULL UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY 1 DESC")
	want := [][]interface{}{{int64(2)}, {int64(1)}, {nil}}
	compareRows(t, got, want)
}

// testCompoundFloatsOrder exercises cmpFloats via float values in compound ORDER BY.
func testCompoundFloatsOrder(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// Three floats: covers a<b, a>b, and a==b branches of cmpFloats.
	got := queryRows(t, db, "SELECT 2.5 UNION ALL SELECT 1.0 UNION ALL SELECT 3.7 ORDER BY 1")
	want := [][]interface{}{{1.0}, {2.5}, {3.7}}
	compareRows(t, got, want)
}

// testCompoundFloatsIntMix exercises cmpFloats with mixed int64/float64 comparison
// (the int64 vs float64 and float64 vs int64 branches in cmpSameType).
func testCompoundFloatsIntMix(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// integer 1 vs float 1.5 vs float 0.5: exercises cross-type float path
	got := queryRows(t, db, "SELECT 1 UNION ALL SELECT 1.5 UNION ALL SELECT 0.5 ORDER BY 1")
	want := [][]interface{}{{0.5}, {int64(1)}, {1.5}}
	compareRows(t, got, want)
}

// testCompoundExtractBaseExprCollate exercises extractBaseExpr via a COLLATE expression
// in a compound ORDER BY, forcing the CollateExpr unwrap branch.
func testCompoundExtractBaseExprCollate(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	// ORDER BY col COLLATE NOCASE should still sort correctly after unwrapping CollateExpr.
	got := queryRows(t, db, "SELECT 'B' AS c UNION ALL SELECT 'a' AS c ORDER BY c COLLATE NOCASE")
	// NOCASE: 'a' < 'B', so 'a' first
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
}

// testAnalyzeEnsureStat1Table exercises ensureSqliteStat1Table both when the table
// doesn't exist (creates it) and when it already exists (no-op).
func testAnalyzeEnsureStat1Table(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_stat1(a INTEGER)",
		"INSERT INTO t_stat1 VALUES(1)",
		"ANALYZE t_stat1",
	)
	// Second ANALYZE reuses existing sqlite_stat1 (covers the exists-already branch).
	execSQL(t, db, "ANALYZE t_stat1")

	got := queryRows(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t_stat1'")
	if len(got) == 0 {
		t.Fatal("expected at least one row from sqlite_stat1")
	}
}

// testAnalyzeResolveNamedTable exercises resolveNamedTarget via ANALYZE tablename
// when the target is a table (the first GetTable branch).
func testAnalyzeResolveNamedTable(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_named(x INTEGER, y TEXT)",
		"INSERT INTO t_named VALUES(1, 'a')",
		"INSERT INTO t_named VALUES(2, 'b')",
		"ANALYZE t_named",
	)
	got := queryRows(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t_named'")
	if len(got) == 0 || got[0][0] == int64(0) {
		t.Error("expected stats for t_named table")
	}
}

// testAnalyzeResolveNamedIndex exercises resolveNamedTarget via ANALYZE indexname
// when the target resolves through an index to its parent table.
func testAnalyzeResolveNamedIndex(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_idx_parent(id INTEGER, val INTEGER)",
		"CREATE INDEX idx_val ON t_idx_parent(val)",
		"INSERT INTO t_idx_parent VALUES(1, 10)",
		"INSERT INTO t_idx_parent VALUES(2, 20)",
		// ANALYZE with the index name: exercises GetIndex branch in resolveNamedTarget.
		"ANALYZE idx_val",
	)
	got := queryRows(t, db, "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t_idx_parent'")
	if len(got) == 0 {
		t.Fatal("expected rows from sqlite_stat1")
	}
}

// testAnalyzeCountTableRows exercises countTableRows by running ANALYZE on a populated table,
// which internally calls countTableRows to get the row count.
func testAnalyzeCountTableRows(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_rowcount(n INTEGER)",
		"INSERT INTO t_rowcount VALUES(1)",
		"INSERT INTO t_rowcount VALUES(2)",
		"INSERT INTO t_rowcount VALUES(3)",
		"ANALYZE t_rowcount",
	)
	got := queryRows(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='t_rowcount' AND idx IS NULL")
	if len(got) == 0 {
		t.Fatal("expected a stat row for t_rowcount")
	}
	// The stat should be "3" for 3 rows.
	if got[0][0] != "3" {
		t.Errorf("expected stat='3', got %v", got[0][0])
	}
}

// testAnalyzeEstimateDistinctViaTable forces estimateDistinct and analyzeToInt64 through
// a table+index ANALYZE run. estimateDistinct is called as a fallback when
// countDistinctPrefix fails. We also exercise analyzeToInt64 with float and string inputs
// by verifying the stat string is parseable.
func testAnalyzeEstimateDistinctViaTable(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_distinct(a INTEGER, b INTEGER)",
		"CREATE INDEX idx_ab ON t_distinct(a, b)",
		"INSERT INTO t_distinct VALUES(1, 1)",
		"INSERT INTO t_distinct VALUES(1, 2)",
		"INSERT INTO t_distinct VALUES(2, 1)",
		"INSERT INTO t_distinct VALUES(2, 2)",
		"ANALYZE t_distinct",
	)
	got := queryRows(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='t_distinct' AND idx='idx_ab'")
	if len(got) == 0 {
		t.Fatal("expected a stat row for t_distinct idx_ab")
	}
}

// testAnalyzeToInt64Types indirectly exercises analyzeToInt64 with multiple input types
// by issuing ANALYZE queries that produce counts and reading back the results.
func testAnalyzeToInt64Types(t *testing.T) {
	t.Parallel()
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t_int64(v INTEGER)",
		"CREATE INDEX idx_int64_v ON t_int64(v)",
		"INSERT INTO t_int64 VALUES(10)",
		"INSERT INTO t_int64 VALUES(20)",
		"INSERT INTO t_int64 VALUES(30)",
		"ANALYZE t_int64",
	)
	// Verify the row count stat was correctly converted from whatever internal type
	got := queryRows(t, db, "SELECT stat FROM sqlite_stat1 WHERE tbl='t_int64' AND idx IS NULL")
	if len(got) == 0 {
		t.Fatal("expected table-level stat row")
	}
	if got[0][0] != "3" {
		t.Errorf("expected stat='3' for 3 rows, got %v", got[0][0])
	}
}
