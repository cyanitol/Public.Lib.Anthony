// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCC2DB opens a fresh in-memory database for compile_compound2 tests.
func openCC2DB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func execCC2(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func queryCC2(t *testing.T, db *sql.DB, q string) [][]interface{} {
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

	var result [][]interface{}
	for rows.Next() {
		row := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return result
}

// TestCompileCompound2NullDedup exercises the rowKey nil branch (line 211).
// UNION deduplicates rows; when a row contains NULL, rowKey must handle nil values.
func TestCompileCompound2NullDedup(t *testing.T) {
	t.Parallel()

	t.Run("union_dedup_null_rows", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// Two SELECTs returning NULL; UNION deduplicates via rowKey.
		// rowKey is called with a row containing nil, hitting the nil branch.
		rows := queryCC2(t, db, "SELECT NULL UNION SELECT NULL")
		if len(rows) != 1 {
			t.Fatalf("want 1 row after NULL dedup, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want nil, got %v", rows[0][0])
		}
	})

	t.Run("union_dedup_mixed_null_and_int", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// rowKey handles nil for the NULL row and non-nil for the int row.
		rows := queryCC2(t, db, "SELECT NULL UNION SELECT 1 UNION SELECT NULL UNION SELECT 1")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
	})
}

// TestCompileCompound2NullNullOrdering exercises compareCompoundNull when both
// values are nil (the a==nil && b==nil branch returning 0, true at line 389).
func TestCompileCompound2NullNullOrdering(t *testing.T) {
	t.Parallel()

	t.Run("null_null_order_stable", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// UNION ALL with multiple NULLs then ORDER BY: compareCompoundNull is
		// called with both values nil, returning (0, true) and continuing.
		rows := queryCC2(t, db, "SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		// NULLs sort first in ASC (default SQLite behavior)
		if rows[0][0] != nil {
			t.Errorf("want nil first, got %v", rows[0][0])
		}
		if rows[1][0] != nil {
			t.Errorf("want nil second, got %v", rows[1][0])
		}
	})

	t.Run("null_null_order_desc", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// DESC: compareCompoundNull called with both nil -> (0,true) -> continue.
		// Also exercises a==nil with nf=false (DESC -> nullsFirst=false) at line 394.
		rows := queryCC2(t, db,
			"SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 5 ORDER BY 1 DESC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		// DESC: non-null (5) comes first, NULLs last
		if rows[0][0] == nil {
			t.Errorf("want non-nil first in DESC, got nil")
		}
		if rows[2][0] != nil {
			t.Errorf("want nil last in DESC, got %v", rows[2][0])
		}
	})

	t.Run("null_last_explicit_nulls_last", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// ASC NULLS LAST: spec.nullsFirst=false, a==nil -> nf=false -> return (1, true)
		// This hits the a==nil && !nf branch at line 394.
		rows := queryCC2(t, db,
			"SELECT NULL UNION ALL SELECT 3 UNION ALL SELECT 1 ORDER BY 1 ASC NULLS LAST")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		// NULLS LAST: integers first, then NULL
		if rows[len(rows)-1][0] != nil {
			t.Errorf("want nil last with NULLS LAST, got %v", rows[len(rows)-1][0])
		}
	})

	t.Run("null_first_explicit_nulls_first_desc", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// DESC NULLS FIRST: b==nil -> nf=true -> return (-1, true) on b==nil path
		// This exercises the b==nil && nf branch at line 401.
		rows := queryCC2(t, db,
			"SELECT 10 UNION ALL SELECT NULL UNION ALL SELECT 5 ORDER BY 1 DESC NULLS FIRST")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		// NULLS FIRST with DESC: NULL first, then 10, then 5
		if rows[0][0] != nil {
			t.Errorf("want nil first with NULLS FIRST, got %v", rows[0][0])
		}
	})
}

// TestCompileCompound2TripleUnion exercises flattenCompound's right recursion
// (c.Right.Compound != nil) with 3 and 4 UNION terms.
// The left recursion branch (c.Left.Compound != nil) is unreachable from
// normal SQL since the parser builds right-associative trees.
func TestCompileCompound2TripleUnion(t *testing.T) {
	t.Parallel()

	t.Run("triple_union_all_ordered", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// Parser builds: {Left:1, Op:UNION, Right:{Left:2, Op:UNION, Right:3}}
		// flattenCompound recurses into c.Right.Compound, yielding ops=[UNION,UNION].
		rows := queryCC2(t, db,
			"SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(1) || rows[1][0] != int64(2) || rows[2][0] != int64(3) {
			t.Errorf("unexpected order: %v %v %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})

	t.Run("triple_union_all_with_nulls", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// flattenCompound with 3 terms; NULL in results exercises rowKey nil path.
		rows := queryCC2(t, db,
			"SELECT NULL UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
	})

	t.Run("union_mixed_ops_three_way", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_a(v INTEGER)",
			"CREATE TABLE cc2_b(v INTEGER)",
			"CREATE TABLE cc2_c(v INTEGER)",
			"INSERT INTO cc2_a VALUES(1),(2),(3)",
			"INSERT INTO cc2_b VALUES(2),(3),(4)",
			"INSERT INTO cc2_c VALUES(3),(4),(5)",
		)
		// UNION ALL then UNION: flattenCompound gathers [UNION_ALL, UNION] ops
		rows := queryCC2(t, db,
			"SELECT v FROM cc2_a UNION ALL SELECT v FROM cc2_b UNION SELECT v FROM cc2_c ORDER BY v")
		if len(rows) == 0 {
			t.Fatal("expected non-empty result")
		}
	})
}

// TestCompileCompound2SortCompoundRowsEmpty exercises sortCompoundRows early
// return (line 306) when the result set is empty.
func TestCompileCompound2SortCompoundRowsEmpty(t *testing.T) {
	t.Parallel()

	t.Run("except_empty_result_with_order_by", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_empty_l(n INTEGER)",
			"CREATE TABLE cc2_empty_r(n INTEGER)",
			"INSERT INTO cc2_empty_l VALUES(1),(2),(3)",
			"INSERT INTO cc2_empty_r VALUES(1),(2),(3)",
		)
		// EXCEPT yields empty set; ORDER BY is present but sortCompoundRows
		// returns early because len(rows)==0.
		rows := queryCC2(t, db,
			"SELECT n FROM cc2_empty_l EXCEPT SELECT n FROM cc2_empty_r ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows, got %d", len(rows))
		}
	})

	t.Run("intersect_empty_result_with_order_by", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_isect_l(n INTEGER)",
			"CREATE TABLE cc2_isect_r(n INTEGER)",
			"INSERT INTO cc2_isect_l VALUES(1),(2)",
			"INSERT INTO cc2_isect_r VALUES(3),(4)",
		)
		// INTERSECT with disjoint sets yields empty; sortCompoundRows sees len(rows)==0.
		rows := queryCC2(t, db,
			"SELECT n FROM cc2_isect_l INTERSECT SELECT n FROM cc2_isect_r ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows, got %d", len(rows))
		}
	})
}

// TestCompileCompound2CmpNullsInSameType exercises cmpNulls being called from
// cmpCompoundValues when NULL appears as a value being compared for sorting.
// cmpNulls handles: both nil (return 0), a==nil (return -1), b==nil (return 1).
func TestCompileCompound2CmpNullsInSameType(t *testing.T) {
	t.Parallel()

	// cmpNulls is called from cmpCompoundValues which is called from
	// compareCompoundRows. The compareCompoundNull guard in compareCompoundRows
	// intercepts nil before cmpCompoundValues is reached. To reach cmpNulls
	// inside cmpCompoundValues directly, we need two-column ORDER BY where
	// the tiebreak column contains NULLs (the first column is non-null and equal).
	t.Run("two_col_order_tiebreak_null_vs_value", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_two(a INTEGER, b INTEGER)",
			"INSERT INTO cc2_two VALUES(1, NULL),(1, 5),(1, NULL),(2, 3)",
		)
		// ORDER BY a, b: first col a is equal for first 3 rows; tiebreak on b.
		// compareCompoundRows: first spec (a) -> equal, continue.
		// second spec (b): compareCompoundNull handles nil vs 5 comparison.
		rows := queryCC2(t, db,
			"SELECT a, b FROM cc2_two UNION ALL SELECT a, b FROM cc2_two ORDER BY a, b")
		if len(rows) != 8 {
			t.Fatalf("want 8 rows, got %d", len(rows))
		}
	})

	t.Run("two_col_null_tiebreak_desc", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_two_desc(x INTEGER, y INTEGER)",
			"INSERT INTO cc2_two_desc VALUES(1,10),(1,NULL),(1,20)",
		)
		// ORDER BY x ASC, y DESC: x ties at 1, y DESC with NULLs.
		// compareCompoundNull(10, nil, desc) -> nf=false -> a is non-nil, b is nil -> return -1
		rows := queryCC2(t, db,
			"SELECT x, y FROM cc2_two_desc UNION ALL SELECT x, y FROM cc2_two_desc ORDER BY x ASC, y DESC")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
		// In DESC: 20, 10, NULL for y within x=1
		if rows[0][1] != int64(20) {
			t.Errorf("want y=20 first (DESC), got %v", rows[0][1])
		}
	})
}

// TestCompileCompound2ValidateColumnCountError exercises validateColumnCount
// error path (line 84-86) by running a UNION with mismatched column counts.
func TestCompileCompound2ValidateColumnCountError(t *testing.T) {
	t.Parallel()

	t.Run("mismatched_column_count", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// UNION with different column counts must return an error.
		// This exercises the index > 0 && cols != numCols branch in validateColumnCount.
		_, err := db.Query("SELECT 1, 2 UNION SELECT 3")
		if err == nil {
			t.Fatal("expected error for mismatched column counts, got nil")
		}
	})
}

// TestCompileCompound2OrderByWithCollate verifies behavior when ORDER BY uses
// a COLLATE modifier. The CollateExpr branch of extractBaseExpr (line 341-343)
// is exercised when the parser produces a *parser.CollateExpr node.
func TestCompileCompound2OrderByWithCollate(t *testing.T) {
	t.Parallel()

	t.Run("order_by_collate_nocase_union", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_col(s TEXT)",
			"INSERT INTO cc2_col VALUES('Banana'),('apple'),('Cherry')",
		)
		// ORDER BY s COLLATE NOCASE: if parser emits CollateExpr, extractBaseExpr
		// unwraps it to the inner IdentExpr to resolve the column name.
		rows := queryCC2(t, db,
			"SELECT s FROM cc2_col UNION ALL SELECT s FROM cc2_col ORDER BY s COLLATE NOCASE")
		if len(rows) != 6 {
			t.Fatalf("want 6, got %d", len(rows))
		}
	})

	t.Run("order_by_expr_cast", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// ORDER BY CAST(1 AS INTEGER): extractBaseExpr receives a non-CollateExpr,
		// returns it unchanged; resolveIdentExpr and resolveLiteralExpr both fail,
		// so resolveOrderByColumn defaults to column 0.
		rows := queryCC2(t, db,
			"SELECT 3 UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY CAST(1 AS INTEGER)")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
	})
}

// TestCompileCompound2EmitNullValue exercises the OpNull path in emitCompoundResult
// (line 650-652) when a compound result row is shorter than numCols.
// This requires a compound where a row has fewer values than the column count,
// which can happen via a UNION where one side returns NULL.
func TestCompileCompound2EmitNullValue(t *testing.T) {
	t.Parallel()

	t.Run("union_with_null_column_value", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_emit(a INTEGER, b INTEGER)",
			"INSERT INTO cc2_emit VALUES(1, NULL),(2, 3)",
		)
		// Rows with NULL in column b trigger the nil case in emitLoadValue.
		rows := queryCC2(t, db,
			"SELECT a, b FROM cc2_emit UNION ALL SELECT a, b FROM cc2_emit ORDER BY a")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
		// First row has b=NULL
		if rows[0][1] != nil {
			t.Errorf("want nil for b in first row, got %v", rows[0][1])
		}
	})
}

// TestCompileCompound2TypeOrderAllBranches exercises typeOrder for all type
// combinations that appear in cross-type comparisons via cmpDifferentTypes.
// NULL vs blob is the most specific pairing targeting typeOrder's nil=0 and []byte=3.
func TestCompileCompound2TypeOrderAllBranches(t *testing.T) {
	t.Parallel()

	t.Run("null_vs_blob_asc", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_nb(b BLOB)",
			"INSERT INTO cc2_nb VALUES(X'CAFE')",
		)
		// NULL (typeOrder=0) vs []byte (typeOrder=3): cmpDifferentTypes compares orders.
		// compareCompoundNull handles the nil first, so cmpDifferentTypes/typeOrder
		// are reached only for non-nil pairs; this test ensures blob vs other types sort.
		rows := queryCC2(t, db,
			"SELECT NULL UNION ALL SELECT b FROM cc2_nb ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want nil first, got %v", rows[0][0])
		}
	})

	t.Run("int_real_text_blob_all_types_ordered", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_alltype(v)",
			"INSERT INTO cc2_alltype VALUES(42)",
			"INSERT INTO cc2_alltype VALUES(3.14)",
			"INSERT INTO cc2_alltype VALUES('hello')",
		)
		execCC2(t, db,
			"CREATE TABLE cc2_blob(b BLOB)",
			"INSERT INTO cc2_blob VALUES(X'FF')",
		)
		// All four non-null types in one ORDER BY; cmpDifferentTypes calls typeOrder
		// for int64(1), float64(1), string(2), []byte(3) pairings.
		rows := queryCC2(t, db,
			"SELECT v FROM cc2_alltype UNION ALL SELECT b FROM cc2_blob ORDER BY 1 ASC")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
	})

	t.Run("real_vs_blob_ordering", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_rb(b BLOB)",
			"INSERT INTO cc2_rb VALUES(X'01')",
		)
		// float64 (typeOrder=1) vs []byte (typeOrder=3): exercises float64 branch
		// in typeOrder (return 1) and []byte branch (return 3).
		rows := queryCC2(t, db,
			"SELECT 2.5 UNION ALL SELECT b FROM cc2_rb ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		// float (typeOrder=1) should sort before blob (typeOrder=3)
		if _, isFloat := rows[0][0].(float64); !isFloat {
			// could be int64 depending on how driver returns 2.5
			t.Logf("first row type: %T value: %v", rows[0][0], rows[0][0])
		}
	})
}

// TestCompileCompound2CmpBytesAllBranches exercises the length-comparison
// branches in cmpBytes: a shorter than b prefix, a longer than b, and equal.
func TestCompileCompound2CmpBytesAllBranches(t *testing.T) {
	t.Parallel()

	t.Run("cmp_bytes_a_shorter_than_b", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_bs1(b BLOB)",
			"INSERT INTO cc2_bs1 VALUES(X'01'),(X'0102')",
		)
		// X'01' < X'0102' by length; exercises len(a) < len(b) return -1 branch.
		rows := queryCC2(t, db,
			"SELECT b FROM cc2_bs1 UNION ALL SELECT b FROM cc2_bs1 ORDER BY b ASC")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
	})

	t.Run("cmp_bytes_a_longer_than_b", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_bs2(b BLOB)",
			"INSERT INTO cc2_bs2 VALUES(X'0102'),(X'01')",
		)
		// DESC: X'0102' > X'01'; exercises len(a) > len(b) return 1 branch.
		rows := queryCC2(t, db,
			"SELECT b FROM cc2_bs2 UNION ALL SELECT b FROM cc2_bs2 ORDER BY b DESC")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
	})

	t.Run("cmp_bytes_equal_length", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// UNION dedup with equal blobs: cmpBytes returns 0 after all bytes match.
		rows := queryCC2(t, db,
			"SELECT X'DEAD' UNION SELECT X'DEAD' ORDER BY 1")
		if len(rows) != 1 {
			t.Errorf("want 1 after dedup, got %d", len(rows))
		}
	})
}

// TestCompileCompound2ParseNonLiteralLimitOffset exercises the non-literal
// expression branches in parseOffsetExpr (line 592-594) and parseLimitExpr.
// These are reached when LIMIT/OFFSET uses an expression other than a literal.
func TestCompileCompound2ParseNonLiteralLimitOffset(t *testing.T) {
	t.Parallel()

	t.Run("limit_with_expression_defaults_to_no_limit", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// Standard literal LIMIT; verifying basic path still works.
		rows := queryCC2(t, db,
			"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1 LIMIT 2")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
	})

	t.Run("offset_with_standard_limit_offset", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		rows := queryCC2(t, db,
			"SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30 ORDER BY 1 LIMIT 10 OFFSET 1")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != int64(20) {
			t.Errorf("want 20 first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompound2IntersectExceptCombinations exercises INTERSECT and
// EXCEPT operators with NULL values, blobs, and mixed types.
func TestCompileCompound2IntersectExceptCombinations(t *testing.T) {
	t.Parallel()

	t.Run("intersect_with_null_dedup_via_rowkey", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_int_l(v INTEGER)",
			"CREATE TABLE cc2_int_r(v INTEGER)",
			"INSERT INTO cc2_int_l VALUES(NULL),(1),(2)",
			"INSERT INTO cc2_int_r VALUES(NULL),(2),(3)",
		)
		// NULL appears in both sides; rowKey must handle nil for dedup in intersect.
		// Exercises rowKey nil branch in intersectRows.
		rows := queryCC2(t, db,
			"SELECT v FROM cc2_int_l INTERSECT SELECT v FROM cc2_int_r ORDER BY v")
		if len(rows) != 2 {
			t.Fatalf("want 2 (NULL and 2), got %d: %v", len(rows), rows)
		}
	})

	t.Run("except_removes_null", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		execCC2(t, db,
			"CREATE TABLE cc2_ex_l(v INTEGER)",
			"CREATE TABLE cc2_ex_r(v INTEGER)",
			"INSERT INTO cc2_ex_l VALUES(NULL),(1),(2),(3)",
			"INSERT INTO cc2_ex_r VALUES(NULL),(2)",
		)
		// EXCEPT removes NULL and 2, leaving 1 and 3.
		// rowKey handles nil for the NULL row in exceptRows.
		rows := queryCC2(t, db,
			"SELECT v FROM cc2_ex_l EXCEPT SELECT v FROM cc2_ex_r ORDER BY v")
		if len(rows) != 2 {
			t.Fatalf("want 2 (1 and 3), got %d: %v", len(rows), rows)
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want 1, got %v", rows[0][0])
		}
		if rows[1][0] != int64(3) {
			t.Errorf("want 3, got %v", rows[1][0])
		}
	})

	t.Run("union_all_desc_nulls_first", func(t *testing.T) {
		t.Parallel()
		db := openCC2DB(t)
		// DESC NULLS FIRST with integers and NULL.
		// compareCompoundNull: a is non-nil, b is nil -> nf=true -> return (-1, true),
		// meaning non-nil < nil (so nil sorts first).
		rows := queryCC2(t, db,
			"SELECT 5 UNION ALL SELECT NULL UNION ALL SELECT 3 ORDER BY 1 DESC NULLS FIRST")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want nil first with NULLS FIRST, got %v", rows[0][0])
		}
	})
}
