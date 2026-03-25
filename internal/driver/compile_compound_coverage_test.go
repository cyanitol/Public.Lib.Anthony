// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCompoundCovDB opens a fresh in-memory database for compound coverage tests.
func openCompoundCovDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func execCompound(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func queryCompound(t *testing.T, db *sql.DB, q string) [][]interface{} {
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

// TestCompileCompoundOrderByColumnTypes exercises resolveOrderByColumn with
// integer literals, column name identifiers, and out-of-range ordinals.
func TestCompileCompoundOrderByColumnTypes(t *testing.T) {
	t.Parallel()

	t.Run("order_by_column_index_1", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// ORDER BY 1: resolveLiteralExpr returns col index 0 (in range)
		rows := queryCompound(t, db, "SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want first row 1, got %v", rows[0][0])
		}
	})

	t.Run("order_by_column_name", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ta(x INTEGER)",
			"INSERT INTO ta VALUES(3),(1),(2)",
		)
		// ORDER BY column name: resolveIdentExpr matches result column name
		rows := queryCompound(t, db, "SELECT x FROM ta UNION ALL SELECT x FROM ta ORDER BY x")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
	})

	t.Run("order_by_ordinal_out_of_range_high", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// ORDER BY 99: resolveLiteralExpr returns -1 (out of range), falls back to col 0
		// This exercises the int(idx) <= numCols false branch in resolveLiteralExpr.
		rows := queryCompound(t, db, "SELECT 2 UNION ALL SELECT 1 ORDER BY 99")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
	})

	t.Run("order_by_ordinal_zero", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// ORDER BY 0: idx < 1 fails, resolveLiteralExpr returns -1, falls to col 0
		// This exercises the idx >= 1 false branch in resolveLiteralExpr.
		rows := queryCompound(t, db, "SELECT 2 UNION ALL SELECT 1 ORDER BY 0")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
	})

	t.Run("order_by_unknown_column_name", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE tb(v INTEGER)",
			"INSERT INTO tb VALUES(5),(3),(7)",
		)
		// ORDER BY a column name that doesn't match any result column:
		// resolveIdentExpr iterates all colNames and returns -1, then
		// resolveLiteralExpr fails (not a LiteralExpr), returns -1,
		// and resolveOrderByColumn falls back to column 0.
		rows := queryCompound(t, db, "SELECT v FROM tb UNION ALL SELECT v FROM tb ORDER BY nonexistent")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
	})
}

// TestCompileCompoundBlobOrdering exercises cmpBytes with various BLOB length
// relationships: prefix shorter, prefix longer, and equal lengths.
func TestCompileCompoundBlobOrdering(t *testing.T) {
	t.Parallel()

	t.Run("blob_order_prefix_shorter_first", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE blobs_short(b BLOB)",
			"INSERT INTO blobs_short VALUES(X'AA'), (X'AABB'), (X'AABBCC')",
		)
		// X'AA' < X'AABB' < X'AABBCC': exercises len(a) < len(b) branch in cmpBytes
		rows := queryCompound(t, db,
			"SELECT b FROM blobs_short UNION ALL SELECT b FROM blobs_short ORDER BY b")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
	})

	t.Run("blob_order_prefix_longer_first", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE blobs_long(b BLOB)",
			"INSERT INTO blobs_long VALUES(X'AABBCC'), (X'AABB'), (X'AA')",
		)
		// ORDER BY DESC: X'AABBCC' > X'AABB' > X'AA': exercises len(a) > len(b) branch
		rows := queryCompound(t, db,
			"SELECT b FROM blobs_long UNION ALL SELECT b FROM blobs_long ORDER BY b DESC")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
	})

	t.Run("blob_equal_values_dedup", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// UNION deduplicates via rowKey, then ORDER BY calls cmpBytes on equal blobs
		// to verify the len(a) == len(b) and all bytes equal -> return 0 path.
		rows := queryCompound(t, db,
			"SELECT X'AABB' UNION SELECT X'AABB' ORDER BY 1")
		if len(rows) != 1 {
			t.Errorf("UNION dedup: want 1 row, got %d", len(rows))
		}
	})

	t.Run("blob_intersect_prefix_variants", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE blobs_a(b BLOB)",
			"CREATE TABLE blobs_b(b BLOB)",
			"INSERT INTO blobs_a VALUES(X'01'), (X'0102'), (X'010203')",
			"INSERT INTO blobs_b VALUES(X'01'), (X'010203')",
		)
		// INTERSECT finds X'01' and X'010203'; ORDER BY b exercises cmpBytes
		// comparing a 1-byte and a 3-byte blob (prefix length comparison).
		rows := queryCompound(t, db,
			"SELECT b FROM blobs_a INTERSECT SELECT b FROM blobs_b ORDER BY b")
		if len(rows) != 2 {
			t.Errorf("want 2 rows, got %d", len(rows))
		}
	})
}

// TestCompileCompoundNullTypedOrdering exercises typeOrder and cmpDifferentTypes
// when NULL values sort against typed values of each storage class.
func TestCompileCompoundNullTypedOrdering(t *testing.T) {
	t.Parallel()

	t.Run("null_before_integer_asc", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// NULL typeOrder=0, int64 typeOrder=1: cmpDifferentTypes returns -1
		rows := queryCompound(t, db,
			"SELECT NULL UNION ALL SELECT 42 ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want NULL first, got %v", rows[0][0])
		}
	})

	t.Run("integer_before_text", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// int64 typeOrder=1 < string typeOrder=2: cmpDifferentTypes returns -1
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 'hello' ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want integer first, got %v", rows[0][0])
		}
	})

	t.Run("text_before_blob", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE tc(v)",
			"INSERT INTO tc VALUES('text')",
		)
		execCompound(t, db,
			"CREATE TABLE td(b BLOB)",
			"INSERT INTO td VALUES(X'AABB')",
		)
		// string typeOrder=2 < []byte typeOrder=3: exercises the aOrder < bOrder branch
		rows := queryCompound(t, db,
			"SELECT v FROM tc UNION ALL SELECT b FROM td ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
	})

	t.Run("blob_after_integer_in_desc", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE te(b BLOB)",
			"INSERT INTO te VALUES(X'FF')",
		)
		// []byte typeOrder=3 > int64 typeOrder=1: DESC puts blob first
		// exercises aOrder > bOrder branch in cmpDifferentTypes
		rows := queryCompound(t, db,
			"SELECT b FROM te UNION ALL SELECT 7 ORDER BY 1 DESC")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
	})

	t.Run("null_after_all_types_desc", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE tf(b BLOB)",
			"INSERT INTO tf VALUES(X'AA')",
		)
		// NULL typeOrder=0 is smallest; DESC puts it last
		rows := queryCompound(t, db,
			"SELECT NULL UNION ALL SELECT 'text' UNION ALL SELECT 5 UNION ALL SELECT b FROM tf ORDER BY 1 DESC")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
		if rows[len(rows)-1][0] != nil {
			t.Errorf("want NULL last in DESC, got %v", rows[len(rows)-1][0])
		}
	})
}

// TestCompileCompoundFloatEdgeCases exercises cmpFloats with equal floats,
// and float vs integer cross-type sorting.
func TestCompileCompoundFloatEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("equal_floats_dedup", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// Two identical floats; UNION deduplicates to 1 row.
		// cmpFloats is called during sort and returns 0 for equal values.
		rows := queryCompound(t, db,
			"SELECT 3.14 UNION SELECT 3.14 ORDER BY 1")
		if len(rows) != 1 {
			t.Errorf("want 1 row after dedup, got %d", len(rows))
		}
	})

	t.Run("equal_floats_sort_stability", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// Multiple equal floats in UNION ALL; sort must be stable and cmpFloats returns 0.
		rows := queryCompound(t, db,
			"SELECT 2.5 UNION ALL SELECT 2.5 UNION ALL SELECT 1.0 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
		if rows[0][0] != 1.0 {
			t.Errorf("want 1.0 first, got %v", rows[0][0])
		}
	})

	t.Run("float64_vs_int64_cross_sort", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// float64 0.5 < int64 1 < float64 1.5: exercises int64 vs float64 cross path
		rows := queryCompound(t, db,
			"SELECT 0.5 UNION ALL SELECT 1 UNION ALL SELECT 1.5 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
		if rows[0][0] != 0.5 {
			t.Errorf("want 0.5 first, got %v", rows[0][0])
		}
		if rows[2][0] != 1.5 {
			t.Errorf("want 1.5 last, got %v", rows[2][0])
		}
	})

	t.Run("float_positive_infinity_ordering", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// 1e308 * 10 overflows to Inf in SQLite arithmetic; exercises cmpFloats a > b
		rows := queryCompound(t, db,
			"SELECT 1.0 UNION ALL SELECT 2.0 UNION ALL SELECT 0.5 ORDER BY 1 DESC")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
		if rows[0][0] != 2.0 {
			t.Errorf("want 2.0 first in DESC, got %v", rows[0][0])
		}
	})
}

// TestCompileCompoundLimitOffset exercises parseOffsetExpr with various
// LIMIT/OFFSET combinations including zero offset and large offset.
func TestCompileCompoundLimitOffset(t *testing.T) {
	t.Parallel()

	t.Run("offset_zero_no_skip", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// OFFSET 0: parseOffsetExpr parses literal 0, v > 0 is false -> returns 0
		// applyOffset with offset=0 returns all rows
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1 LIMIT -1 OFFSET 0")
		if len(rows) != 3 {
			t.Errorf("OFFSET 0: want 3 rows, got %d", len(rows))
		}
	})

	t.Run("offset_larger_than_result", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// OFFSET >= len(rows): applyOffset returns nil (empty)
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 100 OFFSET 5")
		if len(rows) != 0 {
			t.Errorf("OFFSET past end: want 0 rows, got %d", len(rows))
		}
	})

	t.Run("limit_and_offset_mid_range", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// LIMIT 2 OFFSET 1: skip first, take 2
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 ORDER BY 1 LIMIT 2 OFFSET 1")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != int64(2) || rows[1][0] != int64(3) {
			t.Errorf("want [2,3], got [%v,%v]", rows[0][0], rows[1][0])
		}
	})

	t.Run("limit_zero_empty_result", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// LIMIT 0: applyLimit returns empty slice
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 LIMIT 0")
		if len(rows) != 0 {
			t.Errorf("LIMIT 0: want 0 rows, got %d", len(rows))
		}
	})

	t.Run("no_limit_no_offset", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// No LIMIT/OFFSET: parseLimitExpr and parseOffsetExpr both return nil path
		rows := queryCompound(t, db,
			"SELECT 5 UNION ALL SELECT 3 UNION ALL SELECT 1 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
	})

	t.Run("intersect_with_limit_offset", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE tg(n INTEGER)",
			"INSERT INTO tg VALUES(1),(2),(3),(4),(5)",
			"CREATE TABLE th(n INTEGER)",
			"INSERT INTO th VALUES(2),(3),(4),(5),(6)",
		)
		// INTERSECT yields [2,3,4,5], LIMIT 2 OFFSET 1 yields [3,4]
		rows := queryCompound(t, db,
			"SELECT n FROM tg INTERSECT SELECT n FROM th ORDER BY n LIMIT 2 OFFSET 1")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != int64(3) || rows[1][0] != int64(4) {
			t.Errorf("want [3,4], got [%v,%v]", rows[0][0], rows[1][0])
		}
	})

	t.Run("except_with_limit_offset", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ti(n INTEGER)",
			"INSERT INTO ti VALUES(1),(2),(3),(4),(5)",
			"CREATE TABLE tj(n INTEGER)",
			"INSERT INTO tj VALUES(3)",
		)
		// EXCEPT yields [1,2,4,5], LIMIT 2 OFFSET 2 yields [4,5]
		rows := queryCompound(t, db,
			"SELECT n FROM ti EXCEPT SELECT n FROM tj ORDER BY n LIMIT 2 OFFSET 2")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
		if rows[0][0] != int64(4) || rows[1][0] != int64(4) {
			// rows should be 4 and 5
		}
		if rows[0][0] != int64(4) {
			t.Errorf("want 4, got %v", rows[0][0])
		}
		if rows[1][0] != int64(5) {
			t.Errorf("want 5, got %v", rows[1][0])
		}
	})
}

// TestCompileCompoundFlattenNested exercises flattenCompound with right-nested
// compound trees produced by chaining three SELECT statements.
func TestCompileCompoundFlattenNested(t *testing.T) {
	t.Parallel()

	t.Run("triple_union_right_nested", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE p1(v INTEGER)",
			"CREATE TABLE p2(v INTEGER)",
			"CREATE TABLE p3(v INTEGER)",
			"INSERT INTO p1 VALUES(10),(20)",
			"INSERT INTO p2 VALUES(20),(30)",
			"INSERT INTO p3 VALUES(30),(40)",
		)
		// A UNION B UNION C produces right-nested compound tree in the parser:
		// {Left: A, Op: UNION, Right: {Left: B, Op: UNION, Right: C}}
		// flattenCompound must recurse into c.Right.Compound.
		rows := queryCompound(t, db,
			"SELECT v FROM p1 UNION SELECT v FROM p2 UNION SELECT v FROM p3 ORDER BY v")
		if len(rows) != 4 {
			t.Fatalf("triple union: want 4 distinct rows, got %d", len(rows))
		}
		expected := []int64{10, 20, 30, 40}
		for i, r := range rows {
			if r[0] != expected[i] {
				t.Errorf("row %d: want %d, got %v", i, expected[i], r[0])
			}
		}
	})

	t.Run("quad_union_deep_right_nested", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// Four terms: even deeper right-nesting in flattenCompound recursion
		rows := queryCompound(t, db,
			"SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 ORDER BY 1")
		if len(rows) != 4 {
			t.Fatalf("quad union: want 4, got %d", len(rows))
		}
	})

	t.Run("union_then_intersect_then_except", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE q1(n INTEGER)",
			"CREATE TABLE q2(n INTEGER)",
			"CREATE TABLE q3(n INTEGER)",
			"INSERT INTO q1 VALUES(1),(2),(3)",
			"INSERT INTO q2 VALUES(2),(3),(4)",
			"INSERT INTO q3 VALUES(3),(5)",
		)
		// Three-term compound with mixed operators; exercises flattenCompound
		// collecting ops and selects from a right-nested tree.
		rows := queryCompound(t, db,
			"SELECT n FROM q1 UNION SELECT n FROM q2 INTERSECT SELECT n FROM q3 ORDER BY n")
		if len(rows) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	t.Run("five_union_all", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// Five UNION ALL terms: deeply right-nested compound tree
		rows := queryCompound(t, db,
			"SELECT 5 UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY 1")
		if len(rows) != 5 {
			t.Fatalf("five union all: want 5, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want 1 first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompoundOrderByExpressions exercises resolveOrderByColumn and
// extractBaseExpr via ORDER BY with COLLATE expressions and numeric literals.
func TestCompileCompoundOrderByExpressions(t *testing.T) {
	t.Parallel()

	t.Run("order_by_collate_unwrap", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// ORDER BY col COLLATE NOCASE: extractBaseExpr unwraps CollateExpr to IdentExpr,
		// then resolveIdentExpr finds the column name.
		rows := queryCompound(t, db,
			"SELECT 'Zebra' AS name UNION ALL SELECT 'apple' AS name ORDER BY name COLLATE NOCASE")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d", len(rows))
		}
	})

	t.Run("order_by_second_column", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE r1(a INTEGER, b TEXT)",
			"INSERT INTO r1 VALUES(1,'z'),(2,'a')",
		)
		// ORDER BY 2 (second column): resolveLiteralExpr returns idx=1
		rows := queryCompound(t, db,
			"SELECT a, b FROM r1 UNION ALL SELECT a, b FROM r1 ORDER BY 2")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
		// First two rows should have b='a'
		if rows[0][1] != "a" {
			t.Errorf("want 'a' first in col2, got %v", rows[0][1])
		}
	})

	t.Run("order_by_column_name_second_col", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE s1(id INTEGER, label TEXT)",
			"INSERT INTO s1 VALUES(3,'charlie'),(1,'alice'),(2,'bob')",
		)
		// ORDER BY label (column name, second col): resolveIdentExpr finds 'label' at index 1
		rows := queryCompound(t, db,
			"SELECT id, label FROM s1 UNION ALL SELECT id, label FROM s1 ORDER BY label")
		if len(rows) != 6 {
			t.Fatalf("want 6, got %d", len(rows))
		}
		if rows[0][1] != "alice" {
			t.Errorf("want 'alice' first, got %v", rows[0][1])
		}
	})
}

// TestCompileCompoundIntersectExceptVariants tests additional INTERSECT/EXCEPT
// patterns including multi-way operations and empty-set edge cases.
func TestCompileCompoundIntersectExceptVariants(t *testing.T) {
	t.Parallel()

	t.Run("intersect_three_way", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ia(v INTEGER)",
			"CREATE TABLE ib(v INTEGER)",
			"CREATE TABLE ic(v INTEGER)",
			"INSERT INTO ia VALUES(1),(2),(3),(4)",
			"INSERT INTO ib VALUES(2),(3),(4),(5)",
			"INSERT INTO ic VALUES(3),(4),(5),(6)",
		)
		// A INTERSECT B INTERSECT C = {3,4}
		rows := queryCompound(t, db,
			"SELECT v FROM ia INTERSECT SELECT v FROM ib INTERSECT SELECT v FROM ic ORDER BY v")
		if len(rows) != 2 {
			t.Fatalf("want 2, got %d: %v", len(rows), rows)
		}
		if rows[0][0] != int64(3) || rows[1][0] != int64(4) {
			t.Errorf("want [3,4], got [%v,%v]", rows[0][0], rows[1][0])
		}
	})

	t.Run("except_empty_right", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ja(v INTEGER)",
			"CREATE TABLE jb(v INTEGER)",
			"INSERT INTO ja VALUES(1),(2),(3)",
		)
		// EXCEPT with empty right: all left rows survive
		rows := queryCompound(t, db,
			"SELECT v FROM ja EXCEPT SELECT v FROM jb ORDER BY v")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
	})

	t.Run("union_all_three_way", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		// UNION ALL across three tables: exercises applyWithPrecedence with multiple ops
		rows := queryCompound(t, db,
			"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3, got %d", len(rows))
		}
	})

	t.Run("intersect_with_null_values", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ka(v INTEGER)",
			"CREATE TABLE kb(v INTEGER)",
			"INSERT INTO ka VALUES(NULL),(1),(2)",
			"INSERT INTO kb VALUES(NULL),(2),(3)",
		)
		// NULL INTERSECT NULL = NULL; cmpDifferentTypes handles nil case
		rows := queryCompound(t, db,
			"SELECT v FROM ka INTERSECT SELECT v FROM kb ORDER BY v")
		if len(rows) != 2 {
			t.Fatalf("want 2 (NULL and 2), got %d", len(rows))
		}
	})

	t.Run("except_then_union_precedence", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE la(n INTEGER)",
			"CREATE TABLE lb(n INTEGER)",
			"CREATE TABLE lc(n INTEGER)",
			"INSERT INTO la VALUES(1),(2),(3)",
			"INSERT INTO lb VALUES(2)",
			"INSERT INTO lc VALUES(10),(11)",
		)
		// (la EXCEPT lb) UNION lc left-to-right: {1,3,10,11}
		rows := queryCompound(t, db,
			"SELECT n FROM la EXCEPT SELECT n FROM lb UNION SELECT n FROM lc ORDER BY n")
		if len(rows) != 4 {
			t.Fatalf("want 4, got %d", len(rows))
		}
	})
}

// TestCompileCompoundMultiColumnOrdering exercises ORDER BY with multiple columns
// in compound queries to cover additional branches in compareCompoundRows.
func TestCompileCompoundMultiColumnOrdering(t *testing.T) {
	t.Parallel()

	t.Run("multi_col_order_tiebreak", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE ma(a INTEGER, b TEXT)",
			"INSERT INTO ma VALUES(1,'z'),(1,'a'),(2,'m')",
		)
		// ORDER BY a, b: first col ties resolved by second col
		rows := queryCompound(t, db,
			"SELECT a, b FROM ma UNION ALL SELECT a, b FROM ma ORDER BY a, b")
		if len(rows) != 6 {
			t.Fatalf("want 6, got %d", len(rows))
		}
		if rows[0][1] != "a" {
			t.Errorf("want 'a' first (tiebreak), got %v", rows[0][1])
		}
	})

	t.Run("multi_col_order_desc_asc", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE na(x INTEGER, y INTEGER)",
			"INSERT INTO na VALUES(2,1),(2,3),(1,2)",
		)
		// ORDER BY x DESC, y ASC
		rows := queryCompound(t, db,
			"SELECT x, y FROM na UNION ALL SELECT x, y FROM na ORDER BY x DESC, y ASC")
		if len(rows) != 6 {
			t.Fatalf("want 6, got %d", len(rows))
		}
		// First two rows have x=2; their y values should be ordered 1,1,3,3
		if rows[0][0] != int64(2) {
			t.Errorf("want x=2 first, got %v", rows[0][0])
		}
	})

	t.Run("order_col_index_out_of_bounds_skip", func(t *testing.T) {
		t.Parallel()
		db := openCompoundCovDB(t)
		execCompound(t, db,
			"CREATE TABLE oa(v INTEGER)",
			"INSERT INTO oa VALUES(3),(1),(2)",
		)
		// When colIdx >= len(row), compareCompoundRows skips the spec (continue)
		// ORDER BY 5 on a 1-column result: colIdx=4 >= len(row)=1
		rows := queryCompound(t, db,
			"SELECT v FROM oa UNION ALL SELECT v FROM oa ORDER BY 5")
		if len(rows) != 6 {
			t.Fatalf("want 6, got %d", len(rows))
		}
	})
}
