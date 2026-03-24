// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// setupWinAggTable creates table t with rows (x=1,2,3) in an in-memory DB.
func setupWinAggTable(t *testing.T) func() {
	t.Helper()
	db, done := openMemDB(t)
	t.Cleanup(done)
	execAll(t, db, []string{
		"CREATE TABLE t (x INTEGER)",
		"INSERT INTO t VALUES (1),(2),(3)",
	})
	_ = db
	return done
}

// winAggQuery runs a single-column query and drains the rows, failing on error.
func winAggQuery(t *testing.T, query string) {
	t.Helper()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE t (x INTEGER)",
		"INSERT INTO t VALUES (1),(2),(3)",
	})
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err %q: %v", query, err)
	}
}

// TestExprHasDataDependent_SUM exercises FunctionExpr+Over for SUM (data-dependent → true).
func TestExprHasDataDependent_SUM(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT SUM(x) OVER () FROM t")
}

// TestExprHasDataDependent_COUNT exercises FunctionExpr+Over for COUNT.
func TestExprHasDataDependent_COUNT(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT COUNT(x) OVER () FROM t")
}

// TestExprHasDataDependent_AVG exercises FunctionExpr+Over for AVG.
func TestExprHasDataDependent_AVG(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT AVG(x) OVER () FROM t")
}

// TestExprHasDataDependent_MIN exercises FunctionExpr+Over for MIN.
func TestExprHasDataDependent_MIN(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT MIN(x) OVER () FROM t")
}

// TestExprHasDataDependent_MAX exercises FunctionExpr+Over for MAX.
func TestExprHasDataDependent_MAX(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT MAX(x) OVER () FROM t")
}

// TestExprHasDataDependent_TOTAL exercises FunctionExpr+Over for TOTAL.
func TestExprHasDataDependent_TOTAL(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT TOTAL(x) OVER () FROM t")
}

// TestExprHasDataDependent_GROUP_CONCAT exercises FunctionExpr+Over for GROUP_CONCAT.
func TestExprHasDataDependent_GROUP_CONCAT(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT GROUP_CONCAT(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY x LIMIT 1")
}

// TestExprHasDataDependent_FIRST_VALUE exercises FunctionExpr+Over for FIRST_VALUE.
func TestExprHasDataDependent_FIRST_VALUE(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT FIRST_VALUE(x) OVER (ORDER BY x) FROM t")
}

// TestExprHasDataDependent_LAST_VALUE exercises FunctionExpr+Over for LAST_VALUE.
func TestExprHasDataDependent_LAST_VALUE(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t")
}

// TestExprHasDataDependent_NTH_VALUE exercises FunctionExpr+Over for NTH_VALUE.
func TestExprHasDataDependent_NTH_VALUE(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT NTH_VALUE(x, 2) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t")
}

// TestExprHasDataDependent_LAG exercises FunctionExpr+Over for LAG.
func TestExprHasDataDependent_LAG(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT LAG(x) OVER (ORDER BY x) FROM t")
}

// TestExprHasDataDependent_LEAD exercises FunctionExpr+Over for LEAD.
func TestExprHasDataDependent_LEAD(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT LEAD(x) OVER (ORDER BY x) FROM t")
}

// TestExprHasDataDependent_RANK_NotDataDependent exercises FunctionExpr+Over for RANK
// (non-data-dependent → false path in isDataDependentWindowFunc).
func TestExprHasDataDependent_RANK_NotDataDependent(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT RANK() OVER (ORDER BY x) FROM t")
}

// TestExprHasDataDependent_ROW_NUMBER_NotDataDependent exercises ROW_NUMBER.
func TestExprHasDataDependent_ROW_NUMBER_NotDataDependent(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM t")
}

// TestExprHasDataDependent_BinaryExprLeft exercises BinaryExpr where the
// data-dependent window func is on the left operand.
func TestExprHasDataDependent_BinaryExprLeft(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT SUM(x) OVER () + 1 FROM t")
}

// TestExprHasDataDependent_BinaryExprRight exercises BinaryExpr where the
// data-dependent window func is on the right operand.
func TestExprHasDataDependent_BinaryExprRight(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT 1 + SUM(x) OVER () FROM t")
}

// TestExprHasDataDependent_UnaryExpr exercises UnaryExpr wrapping a
// data-dependent window func (unary minus).
func TestExprHasDataDependent_UnaryExpr(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT -SUM(x) OVER () FROM t")
}

// TestExprHasDataDependent_ParenExpr exercises ParenExpr containing a
// data-dependent window func.
func TestExprHasDataDependent_ParenExpr(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT (SUM(x) OVER ()) FROM t")
}

// TestExprHasDataDependent_CastExpr exercises CastExpr wrapping a
// data-dependent window func.
func TestExprHasDataDependent_CastExpr(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT CAST(SUM(x) OVER () AS REAL) FROM t")
}

// TestExprHasDataDependent_CaseExprWhenCondition exercises CaseExpr where a
// data-dependent window func appears in a WHEN condition.
func TestExprHasDataDependent_CaseExprWhenCondition(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT CASE WHEN SUM(x) OVER () > 0 THEN 1 ELSE 0 END FROM t")
}

// TestExprHasDataDependent_CaseExprWhenResult exercises CaseExpr where a
// data-dependent window func appears in a THEN result.
func TestExprHasDataDependent_CaseExprWhenResult(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT CASE WHEN 1=1 THEN SUM(x) OVER () ELSE 0 END FROM t")
}

// TestExprHasDataDependent_CaseExprElse exercises CaseExpr where a
// data-dependent window func appears in the ELSE clause.
func TestExprHasDataDependent_CaseExprElse(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT CASE WHEN 1=0 THEN 0 ELSE SUM(x) OVER () END FROM t")
}

// TestExprHasDataDependent_NonWindowFuncArgs exercises FunctionExpr without Over,
// where args are checked recursively (plain scalar function, no window func inside).
func TestExprHasDataDependent_NonWindowFuncArgs(t *testing.T) {
	t.Parallel()
	winAggQuery(t, "SELECT ABS(x) FROM t")
}
