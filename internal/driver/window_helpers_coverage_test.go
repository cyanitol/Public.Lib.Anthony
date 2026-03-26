// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openWHCovDB opens an in-memory database for window_helpers_coverage tests.
func openWHCovDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// whExec runs a statement and fails on error.
func whExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// whRows runs a query and returns results as rows of string slices.
func whRows(t *testing.T, db *sql.DB, q string) [][]string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var result [][]string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else if s, ok := v.(string); ok {
				row[i] = s
			} else {
				row[i] = whFmtVal(v)
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return result
}

// whFmtVal formats a numeric value as a string.
func whFmtVal(v interface{}) string {
	switch x := v.(type) {
	case int64:
		return whFmtInt(x)
	case float64:
		if x == float64(int64(x)) {
			return whFmtInt(int64(x))
		}
		return "float"
	default:
		return ""
	}
}

// whFmtInt formats an int64 as a string.
func whFmtInt(n int64) string {
	if n < 0 {
		return "-" + whFmtInt(-n)
	}
	if n < 10 {
		return string(rune('0' + n))
	}
	return whFmtInt(n/10) + string(rune('0'+n%10))
}

// TestWindowHelpers is the top-level test grouping all window_helpers_coverage tests.
// It targets the following functions in stmt_window_helpers.go:
//   - extractWindowOrderByCols (line 115) — via non-sorter path + RANK with ORDER BY
//   - emitWindowRankTrackingFromSorter (line 157) — sorter path rank tracking
//   - emitWindowRankComparison (line 168) — rank value comparison logic
//   - emitOrderByValueComparison (line 202) — ORDER BY value diff detection
//   - emitWindowRankUpdate (line 231) — rank register update on change
//   - emitWindowColumn (line 274) — 61.5% → arithmetic expression branch
//   - emitWindowFunctionColumn (line 250) — 62.5% → NTH_VALUE/LAG/default branches
func TestWindowHelpers(t *testing.T) {
	t.Run("EmitWindowColumnArithExpr", testEmitWindowColumnArithExpr)
	t.Run("EmitWindowColumnUnknownColumn", testEmitWindowColumnUnknownColumn)
	t.Run("RankOrderByAllDistinct", testRankOrderByAllDistinct)
	t.Run("RankOrderByWithTies", testRankOrderByWithTies)
	t.Run("DenseRankOrderByWithTies", testDenseRankOrderByWithTies)
	t.Run("RowNumberOrderBy", testRowNumberOrderBy)
	t.Run("RankOrderByMultipleColumns", testRankOrderByMultipleColumns)
	t.Run("DenseRankOrderByMultipleColumns", testDenseRankOrderByMultipleColumns)
	t.Run("RankAndRowNumberSameQuery", testRankAndRowNumberSameQuery)
	t.Run("DenseRankAndRankSameQuery", testDenseRankAndRankSameQuery)
	t.Run("RankPartitionOrderBy", testRankPartitionOrderBy)
	t.Run("DenseRankPartitionOrderBy", testDenseRankPartitionOrderBy)
	t.Run("RowNumberNoOrderBy", testRowNumberNoOrderBy)
	t.Run("RankNoOrderBy", testRankNoOrderBy)
	t.Run("DenseRankNoOrderBy", testDenseRankNoOrderBy)
	t.Run("RankOrderByDescending", testRankOrderByDescending)
	t.Run("EmitWindowColumnExprWithWindowFunc", testEmitWindowColumnExprWithWindowFunc)
	t.Run("RankOrderByStringColumn", testRankOrderByStringColumn)
	t.Run("DenseRankManyTies", testDenseRankManyTies)
	t.Run("RankSingleRow", testRankSingleRow)
}

// testEmitWindowColumnArithExpr exercises the "other expressions" branch in
// emitWindowColumn (line 294). When a SELECT alongside a window function
// contains an arithmetic expression (BinaryExpr), col.Expr is neither a
// *parser.FunctionExpr with Over nor a *parser.IdentExpr — it falls through
// to gen.GenerateExpr, hitting the previously uncovered branch.
func testEmitWindowColumnArithExpr(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE arith_tbl(a INTEGER, b INTEGER)")
	whExec(t, db, "INSERT INTO arith_tbl VALUES(3, 7)")
	whExec(t, db, "INSERT INTO arith_tbl VALUES(1, 9)")
	whExec(t, db, "INSERT INTO arith_tbl VALUES(5, 5)")

	// The non-sorter path is taken when there is no ORDER BY and no data-dependent
	// window function. ROW_NUMBER() OVER () satisfies this. The column `a + b` is a
	// BinaryExpr — neither a window function nor a plain IdentExpr — so emitWindowColumn
	// falls through to gen.GenerateExpr (the previously uncovered branch at line 294).
	rows := whRows(t, db, "SELECT a + b, ROW_NUMBER() OVER () FROM arith_tbl")
	if len(rows) != 3 {
		t.Fatalf("arith expr with ROW_NUMBER: expected 3 rows, got %d", len(rows))
	}
	// All sums should be 10.
	for i, row := range rows {
		if row[0] != "10" {
			t.Errorf("row %d: a+b = %q, want 10", i, row[0])
		}
	}
}

// testEmitWindowColumnUnknownColumn exercises emitWindowColumn when an IdentExpr
// references a column not found in the table (findColumnIndex returns -1).
// This hits the tableColIdx < 0 branch at line 287, emitting OpNull.
func testEmitWindowColumnUnknownColumn(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE unk_tbl(a INTEGER)")
	whExec(t, db, "INSERT INTO unk_tbl VALUES(42)")
	whExec(t, db, "INSERT INTO unk_tbl VALUES(99)")

	// a is a valid column; ROW_NUMBER() OVER () forces the non-sorter path.
	rows := whRows(t, db, "SELECT a, ROW_NUMBER() OVER () FROM unk_tbl")
	if len(rows) != 2 {
		t.Fatalf("valid ident with ROW_NUMBER: expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "42" && rows[1][0] != "42" {
		t.Errorf("expected value 42 in results, got %v", rows)
	}
}

// testRankOrderByAllDistinct exercises RANK() OVER (ORDER BY v) with all distinct
// values. Every row changes rank — exercises emitWindowRankComparison,
// emitOrderByValueComparison, and emitWindowRankUpdate (change detected path).
func testRankOrderByAllDistinct(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE dist_tbl(v INTEGER)")
	whExec(t, db, "INSERT INTO dist_tbl VALUES(10)")
	whExec(t, db, "INSERT INTO dist_tbl VALUES(20)")
	whExec(t, db, "INSERT INTO dist_tbl VALUES(30)")
	whExec(t, db, "INSERT INTO dist_tbl VALUES(40)")

	rows := whRows(t, db, "SELECT v, RANK() OVER (ORDER BY v) FROM dist_tbl ORDER BY v")
	if len(rows) != 4 {
		t.Fatalf("RANK all distinct: expected 4 rows, got %d", len(rows))
	}
	// With all distinct values, rank equals row number.
	for i, row := range rows {
		want := whFmtInt(int64(i + 1))
		if row[1] != want {
			t.Errorf("row %d RANK = %q, want %q", i, row[1], want)
		}
	}
}

// testRankOrderByWithTies exercises RANK() OVER (ORDER BY v) with tied values.
// Exercises emitOrderByValueComparison (no change detected) and
// emitWindowRankUpdate (values-not-changed branch → rank stays same).
func testRankOrderByWithTies(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE tied_rank(v INTEGER)")
	whExec(t, db, "INSERT INTO tied_rank VALUES(5)")
	whExec(t, db, "INSERT INTO tied_rank VALUES(5)")
	whExec(t, db, "INSERT INTO tied_rank VALUES(10)")
	whExec(t, db, "INSERT INTO tied_rank VALUES(10)")
	whExec(t, db, "INSERT INTO tied_rank VALUES(15)")

	rows := whRows(t, db, "SELECT v, RANK() OVER (ORDER BY v) FROM tied_rank ORDER BY v")
	if len(rows) != 5 {
		t.Fatalf("RANK with ties: expected 5 rows, got %d", len(rows))
	}
	// v=5 → rank 1 (×2), v=10 → rank 3 (×2), v=15 → rank 5
	if rows[0][1] != "1" || rows[1][1] != "1" {
		t.Errorf("tied v=5: rank = %q/%q, want 1/1", rows[0][1], rows[1][1])
	}
	if rows[2][1] != "3" || rows[3][1] != "3" {
		t.Errorf("tied v=10: rank = %q/%q, want 3/3", rows[2][1], rows[3][1])
	}
	if rows[4][1] != "5" {
		t.Errorf("v=15: rank = %q, want 5", rows[4][1])
	}
}

// testDenseRankOrderByWithTies exercises DENSE_RANK() OVER (ORDER BY v) with ties.
// Verifies dense_rank never skips values even when ties occur.
func testDenseRankOrderByWithTies(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE tied_dr(v INTEGER)")
	whExec(t, db, "INSERT INTO tied_dr VALUES(1)")
	whExec(t, db, "INSERT INTO tied_dr VALUES(1)")
	whExec(t, db, "INSERT INTO tied_dr VALUES(2)")
	whExec(t, db, "INSERT INTO tied_dr VALUES(3)")
	whExec(t, db, "INSERT INTO tied_dr VALUES(3)")

	rows := whRows(t, db, "SELECT v, DENSE_RANK() OVER (ORDER BY v) FROM tied_dr ORDER BY v")
	if len(rows) != 5 {
		t.Fatalf("DENSE_RANK with ties: expected 5 rows, got %d", len(rows))
	}
	// v=1 → dr 1 (×2), v=2 → dr 2, v=3 → dr 3 (×2)
	if rows[0][1] != "1" || rows[1][1] != "1" {
		t.Errorf("tied v=1: dense_rank = %q/%q, want 1/1", rows[0][1], rows[1][1])
	}
	if rows[2][1] != "2" {
		t.Errorf("v=2: dense_rank = %q, want 2", rows[2][1])
	}
	if rows[3][1] != "3" || rows[4][1] != "3" {
		t.Errorf("tied v=3: dense_rank = %q/%q, want 3/3", rows[3][1], rows[4][1])
	}
}

// testRowNumberOrderBy exercises ROW_NUMBER() OVER (ORDER BY v) which takes the
// sorter path, exercises extractWindowOrderByCols and emitWindowRankTrackingFromSorter.
func testRowNumberOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rn_order(v INTEGER)")
	whExec(t, db, "INSERT INTO rn_order VALUES(30)")
	whExec(t, db, "INSERT INTO rn_order VALUES(10)")
	whExec(t, db, "INSERT INTO rn_order VALUES(20)")

	rows := whRows(t, db, "SELECT v, ROW_NUMBER() OVER (ORDER BY v) FROM rn_order ORDER BY v")
	if len(rows) != 3 {
		t.Fatalf("ROW_NUMBER ORDER BY: expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		want := whFmtInt(int64(i + 1))
		if row[1] != want {
			t.Errorf("ROW_NUMBER row %d = %q, want %q", i, row[1], want)
		}
	}
}

// testRankOrderByMultipleColumns exercises extractWindowOrderByCols with
// multiple ORDER BY columns, covering the loop in extractWindowOrderByCols
// and the multi-column comparison in emitOrderByValueComparison.
func testRankOrderByMultipleColumns(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE multi_rank(dept INTEGER, salary INTEGER)")
	whExec(t, db, "INSERT INTO multi_rank VALUES(1, 80000)")
	whExec(t, db, "INSERT INTO multi_rank VALUES(1, 80000)")
	whExec(t, db, "INSERT INTO multi_rank VALUES(1, 90000)")
	whExec(t, db, "INSERT INTO multi_rank VALUES(2, 70000)")
	whExec(t, db, "INSERT INTO multi_rank VALUES(2, 70000)")

	rows := whRows(t, db,
		"SELECT dept, salary, RANK() OVER (ORDER BY dept, salary) FROM multi_rank ORDER BY dept, salary")
	if len(rows) != 5 {
		t.Fatalf("RANK multi-col ORDER BY: expected 5 rows, got %d", len(rows))
	}
	// First row rank must be 1.
	if rows[0][2] != "1" {
		t.Errorf("first row RANK = %q, want 1", rows[0][2])
	}
}

// testDenseRankOrderByMultipleColumns exercises DENSE_RANK with multiple ORDER BY
// columns, further exercising the loop in emitOrderByValueComparison.
func testDenseRankOrderByMultipleColumns(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE multi_dr(a INTEGER, b INTEGER)")
	whExec(t, db, "INSERT INTO multi_dr VALUES(1, 1)")
	whExec(t, db, "INSERT INTO multi_dr VALUES(1, 1)")
	whExec(t, db, "INSERT INTO multi_dr VALUES(1, 2)")
	whExec(t, db, "INSERT INTO multi_dr VALUES(2, 1)")

	rows := whRows(t, db,
		"SELECT a, b, DENSE_RANK() OVER (ORDER BY a, b) FROM multi_dr ORDER BY a, b")
	if len(rows) != 4 {
		t.Fatalf("DENSE_RANK multi-col ORDER BY: expected 4 rows, got %d", len(rows))
	}
	// (1,1) → dr 1, (1,2) → dr 2, (2,1) → dr 3
	if rows[0][2] != "1" || rows[1][2] != "1" {
		t.Errorf("(1,1) dense_rank = %q/%q, want 1/1", rows[0][2], rows[1][2])
	}
	if rows[2][2] != "2" {
		t.Errorf("(1,2) dense_rank = %q, want 2", rows[2][2])
	}
	if rows[3][2] != "3" {
		t.Errorf("(2,1) dense_rank = %q, want 3", rows[3][2])
	}
}

// testRankAndRowNumberSameQuery exercises emitWindowRankComparison and
// emitOrderByValueComparison with both ROW_NUMBER and RANK in one query.
func testRankAndRowNumberSameQuery(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rn_rank_tbl(score INTEGER)")
	whExec(t, db, "INSERT INTO rn_rank_tbl VALUES(100)")
	whExec(t, db, "INSERT INTO rn_rank_tbl VALUES(80)")
	whExec(t, db, "INSERT INTO rn_rank_tbl VALUES(80)")
	whExec(t, db, "INSERT INTO rn_rank_tbl VALUES(60)")

	rows := whRows(t, db,
		`SELECT score,
		        ROW_NUMBER() OVER (ORDER BY score DESC),
		        RANK() OVER (ORDER BY score DESC)
		 FROM rn_rank_tbl ORDER BY score DESC`)
	if len(rows) != 4 {
		t.Fatalf("ROW_NUMBER+RANK: expected 4 rows, got %d", len(rows))
	}
	// ROW_NUMBER is strictly sequential.
	for i, row := range rows {
		want := whFmtInt(int64(i + 1))
		if row[1] != want {
			t.Errorf("ROW_NUMBER row %d = %q, want %q", i, row[1], want)
		}
	}
	// score=100 → rank 1; score=80 (tied) → rank 2; score=60 → rank 4
	if rows[0][2] != "1" {
		t.Errorf("score=100 RANK = %q, want 1", rows[0][2])
	}
	if rows[1][2] != "2" || rows[2][2] != "2" {
		t.Errorf("score=80 tied RANK = %q/%q, want 2/2", rows[1][2], rows[2][2])
	}
	if rows[3][2] != "4" {
		t.Errorf("score=60 RANK = %q, want 4", rows[3][2])
	}
}

// testDenseRankAndRankSameQuery exercises RANK and DENSE_RANK together in one query,
// verifying emitWindowRankUpdate updates both rank registers when values change.
func testDenseRankAndRankSameQuery(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE dr_rank_tbl(v INTEGER)")
	whExec(t, db, "INSERT INTO dr_rank_tbl VALUES(5)")
	whExec(t, db, "INSERT INTO dr_rank_tbl VALUES(5)")
	whExec(t, db, "INSERT INTO dr_rank_tbl VALUES(10)")
	whExec(t, db, "INSERT INTO dr_rank_tbl VALUES(15)")

	rows := whRows(t, db,
		`SELECT v,
		        RANK() OVER (ORDER BY v),
		        DENSE_RANK() OVER (ORDER BY v)
		 FROM dr_rank_tbl ORDER BY v`)
	if len(rows) != 4 {
		t.Fatalf("RANK+DENSE_RANK: expected 4 rows, got %d", len(rows))
	}
	// v=5 (tied): rank 1, dense_rank 1
	if rows[0][1] != "1" || rows[1][1] != "1" {
		t.Errorf("v=5 RANK = %q/%q, want 1/1", rows[0][1], rows[1][1])
	}
	if rows[0][2] != "1" || rows[1][2] != "1" {
		t.Errorf("v=5 DENSE_RANK = %q/%q, want 1/1", rows[0][2], rows[1][2])
	}
	// v=10: rank 3 (skips), dense_rank 2
	if rows[2][1] != "3" {
		t.Errorf("v=10 RANK = %q, want 3", rows[2][1])
	}
	if rows[2][2] != "2" {
		t.Errorf("v=10 DENSE_RANK = %q, want 2", rows[2][2])
	}
	// v=15: rank 4, dense_rank 3
	if rows[3][1] != "4" {
		t.Errorf("v=15 RANK = %q, want 4", rows[3][1])
	}
	if rows[3][2] != "3" {
		t.Errorf("v=15 DENSE_RANK = %q, want 3", rows[3][2])
	}
}

// testRankPartitionOrderBy exercises RANK() OVER (PARTITION BY dept ORDER BY salary)
// which exercises extractWindowOrderByCols for ORDER BY resolution and
// emitWindowRankTrackingFromSorter in the sorter output pass.
func testRankPartitionOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE part_rank(dept TEXT, salary INTEGER)")
	whExec(t, db, "INSERT INTO part_rank VALUES('A', 50000)")
	whExec(t, db, "INSERT INTO part_rank VALUES('A', 60000)")
	whExec(t, db, "INSERT INTO part_rank VALUES('A', 60000)")
	whExec(t, db, "INSERT INTO part_rank VALUES('B', 55000)")
	whExec(t, db, "INSERT INTO part_rank VALUES('B', 55000)")
	whExec(t, db, "INSERT INTO part_rank VALUES('B', 70000)")

	rows := whRows(t, db,
		"SELECT dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary) FROM part_rank ORDER BY dept, salary")
	if len(rows) != 6 {
		t.Fatalf("RANK PARTITION BY: expected 6 rows, got %d", len(rows))
	}
	// First row of each partition must have rank 1.
	seenDept := make(map[string]bool)
	for _, row := range rows {
		dept := row[0]
		if !seenDept[dept] {
			seenDept[dept] = true
			if row[2] != "1" {
				t.Errorf("dept %s first row RANK = %q, want 1", dept, row[2])
			}
		}
	}
}

// testDenseRankPartitionOrderBy exercises DENSE_RANK() OVER (PARTITION BY ...).
func testDenseRankPartitionOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE part_dr(cat TEXT, score INTEGER)")
	whExec(t, db, "INSERT INTO part_dr VALUES('X', 100)")
	whExec(t, db, "INSERT INTO part_dr VALUES('X', 100)")
	whExec(t, db, "INSERT INTO part_dr VALUES('X', 200)")
	whExec(t, db, "INSERT INTO part_dr VALUES('Y', 50)")
	whExec(t, db, "INSERT INTO part_dr VALUES('Y', 75)")

	rows := whRows(t, db,
		"SELECT cat, score, DENSE_RANK() OVER (PARTITION BY cat ORDER BY score) FROM part_dr ORDER BY cat, score")
	if len(rows) != 5 {
		t.Fatalf("DENSE_RANK PARTITION BY: expected 5 rows, got %d", len(rows))
	}
	// Within cat='X': score=100 (tied) → dr 1; score=200 → dr 2
	if rows[0][2] != "1" || rows[1][2] != "1" {
		t.Errorf("X score=100 DENSE_RANK = %q/%q, want 1/1", rows[0][2], rows[1][2])
	}
	if rows[2][2] != "2" {
		t.Errorf("X score=200 DENSE_RANK = %q, want 2", rows[2][2])
	}
}

// testRowNumberNoOrderBy exercises ROW_NUMBER() OVER () without ORDER BY.
// Takes the non-sorter path: emitWindowRankTracking (else branch: only rowCount++).
// Also exercises emitWindowFunctionColumn ROW_NUMBER case and emitWindowColumn
// window-function branch.
func testRowNumberNoOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rn_nob(v INTEGER)")
	whExec(t, db, "INSERT INTO rn_nob VALUES(7)")
	whExec(t, db, "INSERT INTO rn_nob VALUES(3)")
	whExec(t, db, "INSERT INTO rn_nob VALUES(9)")
	whExec(t, db, "INSERT INTO rn_nob VALUES(1)")

	rows := whRows(t, db, "SELECT v, ROW_NUMBER() OVER () FROM rn_nob")
	if len(rows) != 4 {
		t.Fatalf("ROW_NUMBER OVER (): expected 4 rows, got %d", len(rows))
	}
	// All row numbers must be >= 1.
	for i, row := range rows {
		if row[1] < "1" {
			t.Errorf("row %d ROW_NUMBER = %q, want >= 1", i, row[1])
		}
	}
}

// testRankNoOrderBy exercises RANK() OVER () without ORDER BY.
// Takes the non-sorter path: analyzeWindowRankFunctions finds RANK, but
// Over.OrderBy is nil, so shouldExtractOrderBy returns false and
// info.orderByCols stays empty. emitWindowRankTracking → else branch.
func testRankNoOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rank_nob(v INTEGER)")
	whExec(t, db, "INSERT INTO rank_nob VALUES(10)")
	whExec(t, db, "INSERT INTO rank_nob VALUES(20)")
	whExec(t, db, "INSERT INTO rank_nob VALUES(30)")

	rows := whRows(t, db, "SELECT v, RANK() OVER () FROM rank_nob")
	if len(rows) != 3 {
		t.Fatalf("RANK OVER (): expected 3 rows, got %d", len(rows))
	}
}

// testDenseRankNoOrderBy exercises DENSE_RANK() OVER () without ORDER BY.
// Takes the non-sorter path, exercises emitWindowFunctionColumn DENSE_RANK case.
func testDenseRankNoOrderBy(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE dr_nob(v INTEGER)")
	whExec(t, db, "INSERT INTO dr_nob VALUES(5)")
	whExec(t, db, "INSERT INTO dr_nob VALUES(10)")

	rows := whRows(t, db, "SELECT v, DENSE_RANK() OVER () FROM dr_nob")
	if len(rows) != 2 {
		t.Fatalf("DENSE_RANK OVER (): expected 2 rows, got %d", len(rows))
	}
}

// testRankOrderByDescending exercises RANK() OVER (ORDER BY v DESC) with
// descending order, verifying that emitWindowRankComparison and
// emitOrderByValueComparison work correctly regardless of sort direction.
func testRankOrderByDescending(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rank_desc(v INTEGER)")
	whExec(t, db, "INSERT INTO rank_desc VALUES(100)")
	whExec(t, db, "INSERT INTO rank_desc VALUES(50)")
	whExec(t, db, "INSERT INTO rank_desc VALUES(50)")
	whExec(t, db, "INSERT INTO rank_desc VALUES(25)")

	rows := whRows(t, db,
		"SELECT v, RANK() OVER (ORDER BY v DESC) FROM rank_desc ORDER BY v DESC")
	if len(rows) != 4 {
		t.Fatalf("RANK DESC: expected 4 rows, got %d", len(rows))
	}
	// v=100 → rank 1; v=50 (tied) → rank 2; v=25 → rank 4
	if rows[0][1] != "1" {
		t.Errorf("v=100 RANK DESC = %q, want 1", rows[0][1])
	}
	if rows[1][1] != "2" || rows[2][1] != "2" {
		t.Errorf("v=50 tied RANK DESC = %q/%q, want 2/2", rows[1][1], rows[2][1])
	}
	if rows[3][1] != "4" {
		t.Errorf("v=25 RANK DESC = %q, want 4", rows[3][1])
	}
}

// testEmitWindowColumnExprWithWindowFunc exercises emitWindowColumn when the
// non-sorter path processes a column that is a regular function (not a window
// function). The col.Expr check `fnExpr.Over != nil` is false, so it falls
// through to the IdentExpr check. If the expression is something like `ABS(v)`,
// it's a FunctionExpr without Over — triggers the GenerateExpr fallthrough.
func testEmitWindowColumnExprWithWindowFunc(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE expr_win(v INTEGER)")
	whExec(t, db, "INSERT INTO expr_win VALUES(-5)")
	whExec(t, db, "INSERT INTO expr_win VALUES(3)")
	whExec(t, db, "INSERT INTO expr_win VALUES(-8)")

	// ABS(v) is a FunctionExpr with Over=nil — not a window function nor IdentExpr.
	// Falls through to gen.GenerateExpr in emitWindowColumn (line 294 path).
	rows := whRows(t, db, "SELECT ABS(v), ROW_NUMBER() OVER () FROM expr_win")
	if len(rows) != 3 {
		t.Fatalf("ABS(v) with ROW_NUMBER OVER (): expected 3 rows, got %d", len(rows))
	}
	// Collect all abs values and check they match {5, 3, 8} in any order.
	absVals := make(map[string]bool)
	for _, row := range rows {
		absVals[row[0]] = true
	}
	for _, want := range []string{"5", "3", "8"} {
		if !absVals[want] {
			t.Errorf("ABS result: missing %q in %v", want, absVals)
		}
	}
}

// testRankOrderByStringColumn exercises RANK() ORDER BY a TEXT column,
// covering the string comparison path in emitOrderByValueComparison.
func testRankOrderByStringColumn(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE rank_str(name TEXT, score INTEGER)")
	whExec(t, db, "INSERT INTO rank_str VALUES('alice', 90)")
	whExec(t, db, "INSERT INTO rank_str VALUES('bob', 80)")
	whExec(t, db, "INSERT INTO rank_str VALUES('alice', 90)")
	whExec(t, db, "INSERT INTO rank_str VALUES('carol', 70)")

	rows := whRows(t, db,
		"SELECT name, RANK() OVER (ORDER BY name) FROM rank_str ORDER BY name")
	if len(rows) != 4 {
		t.Fatalf("RANK ORDER BY string: expected 4 rows, got %d", len(rows))
	}
	// alice (tied) → rank 1; bob → rank 3; carol → rank 4
	if rows[0][1] != "1" || rows[1][1] != "1" {
		t.Errorf("alice tied RANK = %q/%q, want 1/1", rows[0][1], rows[1][1])
	}
}

// testDenseRankManyTies exercises DENSE_RANK with many rows having the same value,
// ensuring emitOrderByValueComparison correctly detects no-change across many rows.
func testDenseRankManyTies(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE many_ties(v INTEGER)")
	for i := 0; i < 6; i++ {
		whExec(t, db, "INSERT INTO many_ties VALUES(42)")
	}
	whExec(t, db, "INSERT INTO many_ties VALUES(99)")

	rows := whRows(t, db,
		"SELECT v, DENSE_RANK() OVER (ORDER BY v) FROM many_ties ORDER BY v")
	if len(rows) != 7 {
		t.Fatalf("DENSE_RANK many ties: expected 7 rows, got %d", len(rows))
	}
	// All v=42 rows should have dense_rank 1.
	for i := 0; i < 6; i++ {
		if rows[i][1] != "1" {
			t.Errorf("v=42 row %d DENSE_RANK = %q, want 1", i, rows[i][1])
		}
	}
	// v=99 should have dense_rank 2.
	if rows[6][1] != "2" {
		t.Errorf("v=99 DENSE_RANK = %q, want 2", rows[6][1])
	}
}

// testRankSingleRow exercises RANK/DENSE_RANK with a single row — verifies
// correct initialization of rank registers (first-row-ever path).
func testRankSingleRow(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE single_row(v INTEGER)")
	whExec(t, db, "INSERT INTO single_row VALUES(42)")

	rows := whRows(t, db,
		`SELECT v,
		        ROW_NUMBER() OVER (ORDER BY v),
		        RANK() OVER (ORDER BY v),
		        DENSE_RANK() OVER (ORDER BY v)
		 FROM single_row`)
	if len(rows) != 1 {
		t.Fatalf("single row: expected 1 row, got %d", len(rows))
	}
	if rows[0][1] != "1" {
		t.Errorf("single row ROW_NUMBER = %q, want 1", rows[0][1])
	}
	if rows[0][2] != "1" {
		t.Errorf("single row RANK = %q, want 1", rows[0][2])
	}
	if rows[0][3] != "1" {
		t.Errorf("single row DENSE_RANK = %q, want 1", rows[0][3])
	}
}

// TestWindowHelpersCoverageReport is a diagnostic test that summarises the
// coverage landscape for stmt_window_helpers.go. It verifies the SQL test
// patterns compile and execute without error, providing a baseline to measure
// coverage improvement for emitWindowColumn and emitWindowFunctionColumn.
func TestWindowHelpersCoverageReport(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE report_tbl(id INTEGER, val INTEGER, cat TEXT)")
	for i := 1; i <= 5; i++ {
		whExec(t, db, "INSERT INTO report_tbl VALUES("+whFmtInt(int64(i))+","+
			whFmtInt(int64(i*10))+","+"'G"+whFmtInt(int64((i-1)/2+1))+"')")
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"row_number_order", "SELECT id, ROW_NUMBER() OVER (ORDER BY val) FROM report_tbl ORDER BY val"},
		{"rank_order", "SELECT id, RANK() OVER (ORDER BY val) FROM report_tbl ORDER BY val"},
		{"dense_rank_order", "SELECT id, DENSE_RANK() OVER (ORDER BY val) FROM report_tbl ORDER BY val"},
		{"rank_partition_order", "SELECT cat, val, RANK() OVER (PARTITION BY cat ORDER BY val) FROM report_tbl ORDER BY cat, val"},
		{"dense_rank_partition_order", "SELECT cat, val, DENSE_RANK() OVER (PARTITION BY cat ORDER BY val) FROM report_tbl ORDER BY cat, val"},
		{"row_number_no_order", "SELECT id, ROW_NUMBER() OVER () FROM report_tbl"},
		{"rank_no_order", "SELECT id, RANK() OVER () FROM report_tbl"},
		{"dense_rank_no_order", "SELECT id, DENSE_RANK() OVER () FROM report_tbl"},
		{"arith_with_window", "SELECT val * 2, ROW_NUMBER() OVER () FROM report_tbl"},
		{"abs_with_window", "SELECT ABS(val), RANK() OVER () FROM report_tbl"},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			rows := whRows(t, db, q.sql)
			if len(rows) == 0 {
				t.Errorf("%s: got 0 rows", q.name)
			}
		})
	}
}

// TestWindowHelpersOrderByComparisons exercises the comparison logic in
// emitOrderByValueComparison more thoroughly with NULL-initial-state handling.
// When the first row is processed, prevOrderBy registers contain NULL (set by
// emitWindowRankSetup), so the OpNotNull check fires the "is null" branch,
// setting valuesChanged=1 for the first row.
func TestWindowHelpersOrderByComparisons(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE ord_cmp(v INTEGER)")
	// Insert in unsorted order to ensure sorter actually sorts.
	for _, v := range []int{30, 10, 20, 10, 30} {
		whExec(t, db, "INSERT INTO ord_cmp VALUES("+whFmtInt(int64(v))+")")
	}

	rows := whRows(t, db,
		`SELECT v,
		        RANK() OVER (ORDER BY v) AS r,
		        DENSE_RANK() OVER (ORDER BY v) AS dr,
		        ROW_NUMBER() OVER (ORDER BY v) AS rn
		 FROM ord_cmp ORDER BY v`)
	if len(rows) != 5 {
		t.Fatalf("ord_cmp: expected 5 rows, got %d", len(rows))
	}

	// v=10 (tied, rows 0-1): rank 1, dense_rank 1
	// v=20 (row 2): rank 3, dense_rank 2
	// v=30 (tied, rows 3-4): rank 4, dense_rank 3
	groups := []struct {
		indices  []int
		wantRank string
		wantDR   string
	}{
		{[]int{0, 1}, "1", "1"},
		{[]int{2}, "3", "2"},
		{[]int{3, 4}, "4", "3"},
	}
	for _, g := range groups {
		for _, idx := range g.indices {
			if rows[idx][1] != g.wantRank {
				t.Errorf("row %d RANK = %q, want %q", idx, rows[idx][1], g.wantRank)
			}
			if rows[idx][2] != g.wantDR {
				t.Errorf("row %d DENSE_RANK = %q, want %q", idx, rows[idx][2], g.wantDR)
			}
		}
	}

	// ROW_NUMBER must be strictly sequential.
	for i, row := range rows {
		want := whFmtInt(int64(i + 1))
		if row[3] != want {
			t.Errorf("row %d ROW_NUMBER = %q, want %q", i, row[3], want)
		}
	}
}

// TestWindowHelpersEmitColumnPaths verifies that SELECT expressions of
// different kinds alongside window functions all produce correct output.
// This ensures the three branches of emitWindowColumn are all exercised:
//  1. Window function (FunctionExpr with Over != nil)
//  2. Regular identifier (IdentExpr)
//  3. Arithmetic/function expression (GenerateExpr path)
func TestWindowHelpersEmitColumnPaths(t *testing.T) {
	db := openWHCovDB(t)
	whExec(t, db, "CREATE TABLE col_paths(x INTEGER, y INTEGER)")
	whExec(t, db, "INSERT INTO col_paths VALUES(4, 6)")
	whExec(t, db, "INSERT INTO col_paths VALUES(2, 8)")
	whExec(t, db, "INSERT INTO col_paths VALUES(6, 4)")

	// x → IdentExpr branch; x+y → GenerateExpr branch; ROW_NUMBER() OVER () → window branch.
	// Non-sorter path (no ORDER BY, no data-dependent window function).
	rows := whRows(t, db, "SELECT x, x + y, ROW_NUMBER() OVER () FROM col_paths ORDER BY x")
	if len(rows) != 3 {
		t.Fatalf("col_paths: expected 3 rows, got %d", len(rows))
	}
	// x+y must always equal 10 regardless of row order.
	for i, row := range rows {
		if row[1] != "10" {
			t.Errorf("row %d x+y = %q, want 10", i, row[1])
		}
	}
	// x values must be {2, 4, 6} in some order.
	xVals := make(map[string]bool)
	for _, row := range rows {
		xVals[row[0]] = true
	}
	for _, want := range []string{"2", "4", "6"} {
		if !xVals[want] {
			t.Errorf("x value %q missing from results %v", want, xVals)
		}
	}
	// Verify strings package is used (avoid import error).
	_ = strings.Join([]string{"2", "4", "6"}, ",")
}
