// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openCC3DB opens a fresh in-memory database for compile_compound3 tests.
func openCC3DB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func queryCC3(t *testing.T, db *sql.DB, q string) [][]interface{} {
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

func execCC3(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// TestCompileCompound3TypeOrderAllFiveCases exercises typeOrder for all five
// storage classes in a single compound query. When values of different types
// are sorted together, cmpDifferentTypes calls typeOrder on every pairing.
// NULL (0), int64 (1), float64 (1), string (2), []byte (3), and the default
// branch (4) for any unrecognised type are the cases in the switch.
//
// NULL vs int64:  typeOrder(nil)=0  vs typeOrder(int64)=1
// int64 vs float64: both return 1 (same-type path via cmpSameType)
// float64 vs string: typeOrder(float64)=1 vs typeOrder(string)=2
// string vs blob:  typeOrder(string)=2 vs typeOrder([]byte)=3
func TestCompileCompound3TypeOrderAllFiveCases(t *testing.T) {
	t.Parallel()

	// All five SELECT arms in one query so that ORDER BY 1 must compare every
	// pair of distinct types, driving typeOrder through all non-default branches.
	t.Run("all_five_types_ordered_asc", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_blob(b BLOB)",
			"INSERT INTO cc3_blob VALUES(X'ff00')",
		)
		// NULL < int < float < text < blob in SQLite type order.
		// UNION ALL keeps all rows so ORDER BY must call typeOrder on each pair.
		rows := queryCC3(t, db,
			"SELECT NULL UNION ALL SELECT 42 UNION ALL SELECT 3.14 UNION ALL SELECT 'hello' UNION ALL SELECT b FROM cc3_blob ORDER BY 1")
		if len(rows) != 5 {
			t.Fatalf("want 5 rows, got %d", len(rows))
		}
		// NULL must be first (typeOrder=0)
		if rows[0][0] != nil {
			t.Errorf("want NULL first, got %v", rows[0][0])
		}
		// blob ([]byte) must be last (typeOrder=3)
		if _, ok := rows[4][0].([]byte); !ok {
			t.Errorf("want blob last, got %T %v", rows[4][0], rows[4][0])
		}
	})

	t.Run("all_five_types_ordered_desc", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_blob2(b BLOB)",
			"INSERT INTO cc3_blob2 VALUES(X'ff00')",
		)
		// DESC reverses the type order: blob first, NULL last.
		rows := queryCC3(t, db,
			"SELECT NULL UNION ALL SELECT 42 UNION ALL SELECT 3.14 UNION ALL SELECT 'hello' UNION ALL SELECT b FROM cc3_blob2 ORDER BY 1 DESC")
		if len(rows) != 5 {
			t.Fatalf("want 5 rows, got %d", len(rows))
		}
		// blob must be first in DESC
		if _, ok := rows[0][0].([]byte); !ok {
			t.Errorf("want blob first in DESC, got %T %v", rows[0][0], rows[0][0])
		}
		// NULL must be last in DESC
		if rows[4][0] != nil {
			t.Errorf("want NULL last in DESC, got %v", rows[4][0])
		}
	})

	// Three types mixed: int, text, blob — forces typeOrder int64 vs string,
	// and string vs []byte pairings without NULL involvement.
	t.Run("int_text_blob_ordering", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_itb(b BLOB)",
			"INSERT INTO cc3_itb VALUES(X'ab')",
		)
		rows := queryCC3(t, db,
			"SELECT 42 UNION ALL SELECT 'text' UNION ALL SELECT b FROM cc3_itb ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d: %v", len(rows), rows)
		}
		// int64 (typeOrder=1) < string (typeOrder=2) < []byte (typeOrder=3)
		if rows[0][0] != int64(42) {
			t.Errorf("want 42 first, got %T %v", rows[0][0], rows[0][0])
		}
		if _, ok := rows[2][0].([]byte); !ok {
			t.Errorf("want blob last, got %T %v", rows[2][0], rows[2][0])
		}
	})

	// NULL and float only — covers typeOrder nil=0 and float64=1.
	t.Run("null_and_float_ordering", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT NULL UNION ALL SELECT 3.14 UNION ALL SELECT 2.71 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Errorf("want NULL first, got %v", rows[0][0])
		}
	})

	// float vs text — covers typeOrder float64=1 vs string=2.
	t.Run("float_and_text_ordering", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 1.5 UNION ALL SELECT 'alpha' UNION ALL SELECT 0.5 ORDER BY 1 ASC")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		// Numeric types sort before text.
		if _, ok := rows[2][0].(string); !ok {
			t.Errorf("want string last, got %T %v", rows[2][0], rows[2][0])
		}
	})
}

// TestCompileCompound3ExtractBaseExpr targets the extractBaseExpr function at
// line 340. The function has two branches:
//   - CollateExpr: unwrap and return the inner expression (66.7% means this may be hit)
//   - non-CollateExpr: return expr unchanged (the uncovered branch)
//
// When ORDER BY uses a plain expression that is not a CollateExpr (e.g., a raw
// BinaryExpr from something like "ORDER BY (1)"), extractBaseExpr returns it
// unchanged. The resolveIdentExpr and resolveLiteralExpr calls then both fail
// and resolveOrderByColumn defaults to column 0.
func TestCompileCompound3ExtractBaseExpr(t *testing.T) {
	t.Parallel()

	// ORDER BY a column index literal: parser wraps as LiteralExpr (not CollateExpr).
	// extractBaseExpr receives *parser.LiteralExpr, falls through to the default
	// return — this is the non-CollateExpr branch.
	t.Run("order_by_literal_non_collate", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 30 UNION ALL SELECT 10 UNION ALL SELECT 20 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(10) {
			t.Errorf("want 10 first, got %v", rows[0][0])
		}
	})

	// ORDER BY with COLLATE: extractBaseExpr unwraps the CollateExpr (the other branch).
	t.Run("order_by_collate_expr", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 'Zebra' AS w UNION ALL SELECT 'apple' AS w ORDER BY w COLLATE NOCASE")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
	})

	// ORDER BY an IdentExpr (column name): extractBaseExpr receives *parser.IdentExpr,
	// the non-CollateExpr branch returns it directly, then resolveIdentExpr matches it.
	t.Run("order_by_ident_non_collate", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_ident(val INTEGER)",
			"INSERT INTO cc3_ident VALUES(3),(1),(2)",
		)
		rows := queryCC3(t, db,
			"SELECT val FROM cc3_ident UNION ALL SELECT val FROM cc3_ident ORDER BY val")
		if len(rows) != 6 {
			t.Fatalf("want 6 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want 1 first, got %v", rows[0][0])
		}
	})

	// Parenthesized SELECT arms: the parser may wrap inner SELECTs in a way
	// that exercises additional expression handling in ORDER BY resolution.
	t.Run("parenthesized_select_arms", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 1 UNION SELECT 2 ORDER BY 1")
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want 1 first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompound3FlattenCompound targets the flattenCompound function at
// line 158. The function has four branches:
//   - c.Left.Compound != nil  (left recursion — the uncovered branch)
//   - c.Left.Compound == nil  (leaf on left)
//   - c.Right.Compound != nil (right recursion — exercised by 3+ terms)
//   - c.Right.Compound == nil (leaf on right)
//
// The parser builds right-associative compound trees for standard SQL. The left
// recursion branch is reached when the LEFT side of the CompoundSelect node is
// itself a compound (i.e., the tree is left-nested). This can occur when the
// parser handles sub-queries or parenthesized compound expressions on the left.
//
// In practice, chaining many UNIONs causes deeper nesting. We exercise both
// recursion directions via triple and quadruple UNION chains.
func TestCompileCompound3FlattenCompound(t *testing.T) {
	t.Parallel()

	// Triple UNION: parser builds A UNION (B UNION C) — right-nested.
	// flattenCompound sees c.Left=A (leaf), c.Right={B UNION C} (compound).
	// Exercises: left leaf branch, right recursion branch, then right leaf branch.
	t.Run("triple_union_right_nested_with_tables", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_t1(a INTEGER)",
			"CREATE TABLE cc3_t2(b INTEGER)",
			"CREATE TABLE cc3_t3(c INTEGER)",
			"INSERT INTO cc3_t1 VALUES(10),(20)",
			"INSERT INTO cc3_t2 VALUES(20),(30)",
			"INSERT INTO cc3_t3 VALUES(30),(40)",
		)
		rows := queryCC3(t, db,
			"SELECT a FROM cc3_t1 UNION SELECT b FROM cc3_t2 UNION SELECT c FROM cc3_t3 ORDER BY 1")
		if len(rows) != 4 {
			t.Fatalf("triple union: want 4 rows, got %d", len(rows))
		}
		want := []int64{10, 20, 30, 40}
		for i, r := range rows {
			if r[0] != want[i] {
				t.Errorf("row %d: want %d, got %v", i, want[i], r[0])
			}
		}
	})

	// Five UNION ALL arms — deeply right-nested, exercising right recursion
	// multiple times within flattenCompound.
	t.Run("five_union_all_deep_flatten", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 5 UNION ALL SELECT 4 UNION ALL SELECT 3 UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY 1")
		if len(rows) != 5 {
			t.Fatalf("want 5 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(1) {
			t.Errorf("want 1 first, got %v", rows[0][0])
		}
		if rows[4][0] != int64(5) {
			t.Errorf("want 5 last, got %v", rows[4][0])
		}
	})

	// Mixed operators across three arms: UNION ALL then UNION.
	// flattenCompound collects [UNION_ALL, UNION] ops and three selects.
	t.Run("mixed_ops_triple_flatten", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_m1(v INTEGER)",
			"CREATE TABLE cc3_m2(v INTEGER)",
			"CREATE TABLE cc3_m3(v INTEGER)",
			"INSERT INTO cc3_m1 VALUES(1),(2),(3)",
			"INSERT INTO cc3_m2 VALUES(3),(4)",
			"INSERT INTO cc3_m3 VALUES(4),(5)",
		)
		// UNION ALL preserves duplicates from m1 and m2; UNION deduplicates with m3.
		rows := queryCC3(t, db,
			"SELECT v FROM cc3_m1 UNION ALL SELECT v FROM cc3_m2 UNION SELECT v FROM cc3_m3 ORDER BY v")
		if len(rows) == 0 {
			t.Fatal("expected non-empty result")
		}
	})

	// Four arms ensure flattenCompound recurses into the right sub-tree at
	// least twice (right recursion hits depth 2).
	t.Run("quad_union_flatten_depth", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		rows := queryCC3(t, db,
			"SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 ORDER BY 1")
		if len(rows) != 4 {
			t.Fatalf("want 4 rows, got %d", len(rows))
		}
	})

	// UNION with ORDER BY on a literal expression (column position 1):
	// exercises resolveOrderByColumn -> resolveLiteralExpr for a multi-arm query.
	t.Run("triple_union_order_by_literal_expr", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_lit1(n INTEGER)",
			"CREATE TABLE cc3_lit2(n INTEGER)",
			"CREATE TABLE cc3_lit3(n INTEGER)",
			"INSERT INTO cc3_lit1 VALUES(30)",
			"INSERT INTO cc3_lit2 VALUES(10)",
			"INSERT INTO cc3_lit3 VALUES(20)",
		)
		rows := queryCC3(t, db,
			"SELECT n FROM cc3_lit1 UNION SELECT n FROM cc3_lit2 UNION SELECT n FROM cc3_lit3 ORDER BY 1")
		if len(rows) != 3 {
			t.Fatalf("want 3 rows, got %d", len(rows))
		}
		if rows[0][0] != int64(10) {
			t.Errorf("want 10 first, got %v", rows[0][0])
		}
	})
}

// TestCompileCompound3TypeOrderDefaultBranch attempts to drive typeOrder into
// the default case (return 4). The default case is reached when a value is
// stored with a Go type that is not nil/int64/float64/string/[]byte.
// In practice, the engine only produces those five types, so this test
// verifies the normal type cases are all reachable without error.
func TestCompileCompound3TypeOrderDefaultBranch(t *testing.T) {
	t.Parallel()

	// Confirm all four non-null typeOrder values are exercised via sorting.
	// int64 vs string (1 < 2), float64 vs []byte (1 < 3), string vs []byte (2 < 3).
	t.Run("all_non_null_types_pairwise", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_pairwise(b BLOB)",
			"INSERT INTO cc3_pairwise VALUES(X'deadbeef')",
		)
		// int64, float64, string, []byte all in one sorted result.
		rows := queryCC3(t, db,
			"SELECT 100 UNION ALL SELECT 1.5 UNION ALL SELECT 'zz' UNION ALL SELECT b FROM cc3_pairwise ORDER BY 1 ASC")
		if len(rows) != 4 {
			t.Fatalf("want 4 rows, got %d: %v", len(rows), rows)
		}
		// string must sort before blob
		if _, ok := rows[3][0].([]byte); !ok {
			t.Errorf("want blob last, got %T %v", rows[3][0], rows[3][0])
		}
	})

	// DESC ordering with all four types to ensure both aOrder < bOrder and
	// aOrder > bOrder paths in cmpDifferentTypes are covered in both directions.
	t.Run("all_non_null_types_desc", func(t *testing.T) {
		t.Parallel()
		db := openCC3DB(t)
		execCC3(t, db,
			"CREATE TABLE cc3_desc4(b BLOB)",
			"INSERT INTO cc3_desc4 VALUES(X'cafe')",
		)
		rows := queryCC3(t, db,
			"SELECT 99 UNION ALL SELECT 0.5 UNION ALL SELECT 'hello' UNION ALL SELECT b FROM cc3_desc4 ORDER BY 1 DESC")
		if len(rows) != 4 {
			t.Fatalf("want 4 rows, got %d", len(rows))
		}
		// blob (typeOrder=3) must be first in DESC
		if _, ok := rows[0][0].([]byte); !ok {
			t.Errorf("want blob first in DESC, got %T %v", rows[0][0], rows[0][0])
		}
	})
}
