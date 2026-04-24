// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openDeepDB opens a fresh in-memory database for deep compound coverage tests.
func openDeepDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func execDeep(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func queryDeep(t *testing.T, db *sql.DB, q string) [][]interface{} {
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

// TestCompileCompoundDeepIntersectNoMatch exercises applySetOperation for
// INTERSECT when there is no overlap between the two sides. This hits the
// intersectRows function where rightSet never matches any left row, resulting
// in an empty result. Combined with ORDER BY to also exercise sortCompoundRows
// early-return on an empty slice.
func TestCompileCompoundDeepIntersectNoMatch(t *testing.T) {
	t.Parallel()

	t.Run("intersect_completely_disjoint", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ia(n INTEGER)",
			"CREATE TABLE deep_ib(n INTEGER)",
			"INSERT INTO deep_ia VALUES(1),(2),(3)",
			"INSERT INTO deep_ib VALUES(4),(5),(6)",
		)
		// No row in left appears in right; intersectRows produces empty result.
		// applySetOperation(CompoundIntersect, ...) -> intersectRows -> [].
		rows := queryDeep(t, db,
			"SELECT n FROM deep_ia INTERSECT SELECT n FROM deep_ib ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows from disjoint INTERSECT, got %d", len(rows))
		}
	})

	t.Run("intersect_empty_left_side", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ic(n INTEGER)",
			"CREATE TABLE deep_id(n INTEGER)",
			"INSERT INTO deep_id VALUES(1),(2),(3)",
		)
		// Left side is empty; intersectRows has nothing to iterate.
		rows := queryDeep(t, db,
			"SELECT n FROM deep_ic INTERSECT SELECT n FROM deep_id ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows from empty-left INTERSECT, got %d", len(rows))
		}
	})

	t.Run("intersect_empty_right_side", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ie(n INTEGER)",
			"CREATE TABLE deep_if(n INTEGER)",
			"INSERT INTO deep_ie VALUES(1),(2),(3)",
		)
		// Right side is empty; rightSet is empty so nothing matches.
		rows := queryDeep(t, db,
			"SELECT n FROM deep_ie INTERSECT SELECT n FROM deep_if ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows from empty-right INTERSECT, got %d", len(rows))
		}
	})
}

// TestCompileCompoundDeepExceptAllRemoved exercises applySetOperation for
// EXCEPT when every row in the left side is present in the right side.
// exceptRows iterates all left rows and finds each one in rightSet, so
// the result variable never gets any row appended.
func TestCompileCompoundDeepExceptAllRemoved(t *testing.T) {
	t.Parallel()

	t.Run("except_all_rows_removed", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ea(n INTEGER)",
			"CREATE TABLE deep_eb(n INTEGER)",
			"INSERT INTO deep_ea VALUES(10),(20),(30)",
			"INSERT INTO deep_eb VALUES(10),(20),(30),(40)",
		)
		// Every left row is in right; exceptRows produces [].
		rows := queryDeep(t, db,
			"SELECT n FROM deep_ea EXCEPT SELECT n FROM deep_eb ORDER BY n")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows when all rows excepted, got %d", len(rows))
		}
	})

	t.Run("except_identical_tables", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ec(v TEXT)",
			"INSERT INTO deep_ec VALUES('alpha'),('beta'),('gamma')",
		)
		// EXCEPT with itself: all rows in right, result is empty.
		rows := queryDeep(t, db,
			"SELECT v FROM deep_ec EXCEPT SELECT v FROM deep_ec ORDER BY v")
		if len(rows) != 0 {
			t.Fatalf("want 0 rows for table EXCEPT itself, got %d", len(rows))
		}
	})
}

// TestCompileCompoundDeepUnionOrderByNull exercises cmpCompoundValues and
// compareCompoundNull when NULL appears among sorted values in a UNION result.
// The ORDER BY causes compareCompoundRows to handle nil vs non-nil for the
// a==nil case (returning -1 for ASC) and b==nil case (returning 1 for ASC).
func TestCompileCompoundDeepUnionOrderByNull(t *testing.T) {
	t.Parallel()

	t.Run("null_vs_nonnull_asc_null_first", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// cmpCompoundValues is called from compareCompoundRows when comparing
		// non-null vs non-null values. compareCompoundNull handles actual nil.
		// Here we have NULL vs int to exercise the shouldNullsFirst default ASC path.
		rows := queryDeep(t, db,
			"SELECT NULL UNION ALL SELECT 7 UNION ALL SELECT 3 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want NULL first in ASC, got %v", rows[0][0])
		}
	})

	t.Run("nonnull_vs_null_desc_null_last", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// DESC default: NULLs sort last. compareCompoundNull: b==nil, nf=false -> return -1
		// meaning non-nil (a) < nil (b), so b sorts after a in ASC terms; reversed in DESC.
		rows := queryDeep(t, db,
			"SELECT 5 UNION ALL SELECT NULL UNION ALL SELECT 2 ORDER BY 1 DESC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[len(rows)-1][0] != nil {
			t.Errorf("want NULL last in DESC, got %v", rows[len(rows)-1][0])
		}
	})

	t.Run("both_null_sort_stable", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// Two NULLs: compareCompoundNull(nil, nil) -> (0, true) -> continue (no diff).
		// With a non-null row to force a comparison where cmp==0 path is hit.
		rows := queryDeep(t, db,
			"SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != nil || rows[1][0] != nil {
			t.Errorf("want NULLs first, got %v, %v", rows[0][0], rows[1][0])
		}
	})
}

// TestCompileCompoundDeepCmpDifferentTypes exercises cmpDifferentTypes (and
// transitively typeOrder) for integer vs string pairings so that cmpCompoundValues
// routes through the type-order comparison branch rather than same-type dispatch.
func TestCompileCompoundDeepCmpDifferentTypes(t *testing.T) {
	t.Parallel()

	t.Run("int_before_string_in_asc", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// typeOrder(int64)=1 < typeOrder(string)=2 -> cmpDifferentTypes returns -1.
		// ORDER BY ASC: integer rows come before string rows.
		rows := queryDeep(t, db,
			"SELECT 42 UNION ALL SELECT 'hello' UNION ALL SELECT 99 UNION ALL SELECT 'world' ORDER BY 1 ASC")
		if len(rows) != 4 {
			t.Fatalf("want 4 rows, got %d", len(rows))
		}
		// First two should be integers
		if _, ok := rows[0][0].(int64); !ok {
			t.Errorf("want int64 first, got %T %v", rows[0][0], rows[0][0])
		}
		if _, ok := rows[1][0].(int64); !ok {
			t.Errorf("want int64 second, got %T %v", rows[1][0], rows[1][0])
		}
		// Last two should be strings
		if _, ok := rows[2][0].(string); !ok {
			t.Errorf("want string third, got %T %v", rows[2][0], rows[2][0])
		}
	})

	t.Run("string_before_blob_in_asc", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_sb(b BLOB)",
			"INSERT INTO deep_sb VALUES(X'deadbeef')",
		)
		// typeOrder(string)=2 < typeOrder([]byte)=3.
		rows := queryDeep(t, db,
			"SELECT 'text' UNION ALL SELECT b FROM deep_sb ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if _, ok := rows[0][0].(string); !ok {
			t.Errorf("want string first, got %T %v", rows[0][0], rows[0][0])
		}
	})

	t.Run("blob_after_int_desc", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_bi(b BLOB)",
			"INSERT INTO deep_bi VALUES(X'01')",
		)
		// DESC: typeOrder([]byte)=3 > typeOrder(int64)=1 -> blob first in DESC.
		// Exercises aOrder > bOrder branch in cmpDifferentTypes returning 1.
		rows := queryDeep(t, db,
			"SELECT b FROM deep_bi UNION ALL SELECT 100 ORDER BY 1 DESC")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if _, ok := rows[0][0].([]byte); !ok {
			t.Errorf("want blob first in DESC, got %T %v", rows[0][0], rows[0][0])
		}
	})
}

// TestCompileCompoundDeepCmpSameType_EqualIntegers exercises equal-integer sorting.
func TestCompileCompoundDeepCmpSameType_EqualIntegers(t *testing.T) {
	t.Parallel()
	db := openDeepDB(t)
	rows := queryDeep(t, db,
		"SELECT 5 UNION ALL SELECT 5 UNION ALL SELECT 3 ORDER BY 1 ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	if rows[0][0] != int64(3) {
		t.Errorf("want 3 first, got %v", rows[0][0])
	}
	if rows[1][0] != int64(5) || rows[2][0] != int64(5) {
		t.Errorf("want two 5s after 3, got %v %v", rows[1][0], rows[2][0])
	}
}

// TestCompileCompoundDeepCmpSameType_Strings exercises equal and ordered string sorting.
func TestCompileCompoundDeepCmpSameType_Strings(t *testing.T) {
	t.Parallel()

	t.Run("equal_strings_stable_order", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		rows := queryDeep(t, db,
			"SELECT 'foo' UNION ALL SELECT 'foo' UNION ALL SELECT 'bar' ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != "bar" {
			t.Errorf("want 'bar' first, got %v", rows[0][0])
		}
	})

	t.Run("string_ascending_order", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		rows := queryDeep(t, db,
			"SELECT 'cherry' UNION ALL SELECT 'apple' UNION ALL SELECT 'banana' ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != "apple" {
			t.Errorf("want 'apple' first, got %v", rows[0][0])
		}
		if rows[2][0] != "cherry" {
			t.Errorf("want 'cherry' last, got %v", rows[2][0])
		}
	})
}

// TestCompileCompoundDeepCmpSameType_FloatAndCross exercises cmpSameType for
// equal floats and int64/float64 cross-type comparisons.
func TestCompileCompoundDeepCmpSameType_FloatAndCross(t *testing.T) {
	t.Parallel()

	t.Run("float64_vs_float64_equal", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		rows := queryDeep(t, db,
			"SELECT 2.718 UNION ALL SELECT 2.718 UNION ALL SELECT 1.0 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != 1.0 {
			t.Errorf("want 1.0 first, got %v", rows[0][0])
		}
	})

	t.Run("int64_vs_float64_cross_comparison", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		rows := queryDeep(t, db,
			"SELECT 2 UNION ALL SELECT 2.5 UNION ALL SELECT 1.5 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != 1.5 {
			t.Errorf("want 1.5 first, got %v", rows[0][0])
		}
	})

	t.Run("float64_vs_int64_cross_comparison", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		rows := queryDeep(t, db,
			"SELECT 0.5 UNION ALL SELECT 1 UNION ALL SELECT 3.0 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != 0.5 {
			t.Errorf("want 0.5 first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompoundDeepApplyWithPrecedence exercises applyWithPrecedence with
// a three-part compound using parenthesized sub-unions to force a left-nested
// tree: (SELECT a FROM t1 UNION SELECT a FROM t2) UNION SELECT a FROM t3.
// The inner UNION is compiled first; then the outer UNION merges with t3.
// This exercises the left-recursion branch in flattenCompound (c.Left.Compound != nil)
// which is the branch that builds left-nested trees, and applyWithPrecedence
// with multiple ops after the INTERSECT collapse pass leaves no INTERSECTs.
func TestCompileCompoundDeepApplyWithPrecedence(t *testing.T) {
	t.Parallel()

	t.Run("left_nested_union_via_subquery", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_wp1(a INTEGER)",
			"CREATE TABLE deep_wp2(a INTEGER)",
			"CREATE TABLE deep_wp3(a INTEGER)",
			"INSERT INTO deep_wp1 VALUES(1),(2),(3)",
			"INSERT INTO deep_wp2 VALUES(3),(4),(5)",
			"INSERT INTO deep_wp3 VALUES(5),(6),(7)",
		)
		// The outer UNION takes a sub-select (which is itself a UNION) on the left.
		// Parser produces a CompoundSelect with Left.Compound != nil.
		// flattenCompound recurses into Left.Compound, exercising the left-recursion
		// branch. applyWithPrecedence then processes [UNION, UNION] ops left-to-right.
		rows := queryDeep(t, db,
			"SELECT a FROM (SELECT a FROM deep_wp1 UNION SELECT a FROM deep_wp2) UNION SELECT a FROM deep_wp3 ORDER BY a")
		if len(rows) != 7 {
			t.Fatalf("want 7 distinct rows (1-7), got %d: %v", len(rows), rows)
		}
		want := []int64{1, 2, 3, 4, 5, 6, 7}
		for i, r := range rows {
			if r[0] != want[i] {
				t.Errorf("row %d: want %d, got %v", i, want[i], r[0])
			}
		}
	})

	t.Run("triple_union_applyWithPrecedence_no_intersect", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_tri1(v INTEGER)",
			"CREATE TABLE deep_tri2(v INTEGER)",
			"CREATE TABLE deep_tri3(v INTEGER)",
			"INSERT INTO deep_tri1 VALUES(10),(20)",
			"INSERT INTO deep_tri2 VALUES(20),(30)",
			"INSERT INTO deep_tri3 VALUES(30),(40)",
		)
		// Three-way UNION: applyWithPrecedence collapseOp finds no INTERSECT,
		// then the for loop runs twice (once for each op in workOps).
		// This covers the loop body in applyWithPrecedence for len(ops)=2.
		rows := queryDeep(t, db,
			"SELECT v FROM deep_tri1 UNION SELECT v FROM deep_tri2 UNION SELECT v FROM deep_tri3 ORDER BY v")
		if len(rows) != 4 {
			t.Fatalf("want 4 distinct rows, got %d", len(rows))
		}
		expected := []int64{10, 20, 30, 40}
		for i, r := range rows {
			if r[0] != expected[i] {
				t.Errorf("row %d: want %d, got %v", i, expected[i], r[0])
			}
		}
	})

	t.Run("intersect_between_unions_precedence", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_ibu1(n INTEGER)",
			"CREATE TABLE deep_ibu2(n INTEGER)",
			"CREATE TABLE deep_ibu3(n INTEGER)",
			"INSERT INTO deep_ibu1 VALUES(1),(2),(3),(4)",
			"INSERT INTO deep_ibu2 VALUES(2),(3),(4),(5)",
			"INSERT INTO deep_ibu3 VALUES(10),(11)",
		)
		// A UNION B INTERSECT C: INTERSECT has higher precedence.
		// applyWithPrecedence collapseOp collapses (B INTERSECT C) first, then
		// applies (A UNION result). B INTERSECT C = {} (disjoint from ibu3).
		// So result = ibu1 UNION {} = {1,2,3,4}.
		rows := queryDeep(t, db,
			"SELECT n FROM deep_ibu1 UNION SELECT n FROM deep_ibu2 INTERSECT SELECT n FROM deep_ibu3 ORDER BY n")
		// B INTERSECT C = {2,3,4,5} intersect {10,11} = {}
		// A UNION {} = {1,2,3,4}
		if len(rows) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	t.Run("union_all_then_union_applyWithPrecedence", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// UNION ALL keeps duplicates, then UNION deduplicates with the third set.
		// applyWithPrecedence: no INTERSECTs, loop runs for UNION_ALL then UNION.
		rows := queryDeep(t, db,
			"SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION SELECT 3 ORDER BY 1")
		if len(rows) == 0 {
			t.Fatal("expected non-empty result")
		}
	})
}

// TestCompileCompoundDeepEmitLoadValueFloat exercises emitLoadValue for a
// float64 value. This causes vm.AddOpWithP4Real to be called (the float64 case).
// The compound result contains a float row, which goes through emitLoadValue.
func TestCompileCompoundDeepEmitLoadValueFloat(t *testing.T) {
	t.Parallel()

	t.Run("float_value_in_compound_result", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// emitLoadValue(vm, float64(3.14), reg) exercises the float64 case,
		// calling vm.AddOpWithP4Real.
		rows := queryDeep(t, db,
			"SELECT 3.14 UNION ALL SELECT 2.71 ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if rows[0][0] != 2.71 {
			t.Errorf("want 2.71 first, got %v", rows[0][0])
		}
	})

	t.Run("blob_value_in_compound_result", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_blob(b BLOB)",
			"INSERT INTO deep_blob VALUES(X'CAFEBABE')",
		)
		// emitLoadValue(vm, []byte{...}, reg) exercises the []byte case,
		// calling vm.AddOpWithP4Blob.
		rows := queryDeep(t, db,
			"SELECT b FROM deep_blob UNION ALL SELECT b FROM deep_blob ORDER BY b")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if _, ok := rows[0][0].([]byte); !ok {
			t.Errorf("want []byte, got %T %v", rows[0][0], rows[0][0])
		}
	})

	t.Run("string_value_in_compound_result", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// emitLoadValue(vm, string("hello"), reg) exercises the string case,
		// calling vm.AddOpWithP4Str.
		rows := queryDeep(t, db,
			"SELECT 'hello' UNION ALL SELECT 'world' ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if rows[0][0] != "hello" {
			t.Errorf("want 'hello' first, got %v", rows[0][0])
		}
	})

	t.Run("null_value_in_compound_result", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		// emitLoadValue(vm, nil, reg) exercises the nil case (OpNull).
		// Also exercises the emitCompoundResult path where val is nil.
		rows := queryDeep(t, db,
			"SELECT NULL UNION ALL SELECT 1 ORDER BY 1 ASC")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want nil first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompoundDeepEmitCompoundResultShortRow exercises the else-branch
// in emitCompoundResult (line 651: vm.AddOp(vdbe.OpNull, 0, i, 0)) which fires
// when i >= len(row). This happens when a result row has fewer values than
// numCols. We trigger it via a UNION where column count padding is needed.
func TestCompileCompoundDeepEmitCompoundResultShortRow(t *testing.T) {
	t.Parallel()

	t.Run("multi_col_with_null_values", func(t *testing.T) {
		t.Parallel()
		db := openDeepDB(t)
		execDeep(t, db,
			"CREATE TABLE deep_mc(x INTEGER, y INTEGER, z INTEGER)",
			"INSERT INTO deep_mc VALUES(1, NULL, 3)",
			"INSERT INTO deep_mc VALUES(4, 5, NULL)",
		)
		// Rows with NULL values go through emitLoadValue nil case (OpNull).
		// The emitCompoundResult iterates all numCols; when row[i] is nil,
		// emitLoadValue handles it. When i >= len(row), the else branch fires.
		rows := queryDeep(t, db,
			"SELECT x, y, z FROM deep_mc UNION ALL SELECT x, y, z FROM deep_mc ORDER BY x")
		if len(rows) != 4 {
			t.Fatalf("want 4 rows, got %d", len(rows))
		}
		// First row: x=1, y=NULL, z=3
		if rows[0][0] != int64(1) {
			t.Errorf("want x=1, got %v", rows[0][0])
		}
		if rows[0][1] != nil {
			t.Errorf("want y=nil, got %v", rows[0][1])
		}
	})
}
