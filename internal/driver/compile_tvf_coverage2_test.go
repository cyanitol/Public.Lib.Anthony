// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// tvf2OpenDB opens an in-memory database for TVF coverage2 tests.
func tvf2OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("tvf2OpenDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	return db
}

// tvf2Exec executes SQL statements, fataling on error.
func tvf2Exec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("tvf2Exec %q: %v", s, err)
		}
	}
}

// tvf2QueryInts returns all int64 values from the first column of a query.
func tvf2QueryInts(t *testing.T, db *sql.DB, query string, args ...interface{}) []int64 {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("tvf2QueryInts %q: %v", query, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

// tvf2QueryOneInt queries a single int64 value.
func tvf2QueryOneInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("tvf2QueryOneInt %q: %v", query, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// emitTVFBytecode / emitTVFRow — direct unit tests (package driver)
// ---------------------------------------------------------------------------

// TestCompileTVF2_EmitTVFBytecode covers emitTVFBytecode and emitTVFRow directly.
func TestCompileTVF2_EmitTVFBytecode(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	colNames := []string{"a", "b"}
	colIndices := []int{0, 1}
	rows := [][]functions.Value{
		{functions.NewIntValue(1), functions.NewTextValue("hello")},
		{functions.NewIntValue(2), functions.NewTextValue("world")},
	}

	result, err := emitTVFBytecode(vm, rows, colNames, colIndices)
	if err != nil {
		t.Fatalf("emitTVFBytecode: %v", err)
	}
	if result == nil {
		t.Fatal("emitTVFBytecode returned nil vm")
	}
	if result.NumOps() == 0 {
		t.Fatal("emitTVFBytecode emitted no ops")
	}
}

// TestCompileTVF2_EmitTVFBytecode_NullAndOutOfRange covers emitTVFRow null and
// out-of-range source index branches.
func TestCompileTVF2_EmitTVFBytecode_NullAndOutOfRange(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	colNames := []string{"x", "y", "z"}
	// colIndices: first maps to valid src, second maps out-of-range (-1), third to valid
	colIndices := []int{0, -1, 0}
	rows := [][]functions.Value{
		{functions.NewIntValue(42)},
	}

	result, err := emitTVFBytecode(vm, rows, colNames, colIndices)
	if err != nil {
		t.Fatalf("emitTVFBytecode with out-of-range: %v", err)
	}
	if result == nil {
		t.Fatal("nil vm returned")
	}
}

// TestCompileTVF2_EmitTVFBytecode_Empty covers emitTVFBytecode with no rows.
func TestCompileTVF2_EmitTVFBytecode_Empty(t *testing.T) {
	t.Parallel()

	vm := vdbe.New()
	result, err := emitTVFBytecode(vm, nil, []string{"col"}, []int{0})
	if err != nil {
		t.Fatalf("emitTVFBytecode empty: %v", err)
	}
	if result == nil {
		t.Fatal("nil vm")
	}
}

// ---------------------------------------------------------------------------
// driverValueToFuncValue — all type branches
// ---------------------------------------------------------------------------

// TestCompileTVF2_DriverValueToFuncValue covers every type branch of
// driverValueToFuncValue, exercised via generate_series with bind params.
func TestCompileTVF2_DriverValueToFuncValue(t *testing.T) {
	t.Parallel()

	// int64 branch
	if v := driverValueToFuncValue(int64(7)); v.AsInt64() != 7 {
		t.Errorf("int64 branch: want 7, got %v", v.AsInt64())
	}
	// float64 branch
	if v := driverValueToFuncValue(float64(3.14)); v.AsFloat64() != 3.14 {
		t.Errorf("float64 branch: want 3.14, got %v", v.AsFloat64())
	}
	// string branch
	if v := driverValueToFuncValue("hi"); v.AsString() != "hi" {
		t.Errorf("string branch: want 'hi', got %v", v.AsString())
	}
	// []byte branch
	if v := driverValueToFuncValue([]byte("blob")); v.Type() != functions.TypeBlob {
		t.Errorf("[]byte branch: want TypeBlob, got %v", v.Type())
	}
	// nil branch
	if v := driverValueToFuncValue(nil); v.IsNull() != true {
		t.Errorf("nil branch: want null")
	}
	// default (unknown type) branch — uses fmt.Sprintf fallback
	if v := driverValueToFuncValue(struct{ X int }{X: 99}); v.AsString() == "" {
		t.Errorf("default branch: want non-empty string")
	}
}

// TestCompileTVF2_DriverValueToFuncValue_ViaSQL exercises driverValueToFuncValue
// through actual SQL bind parameters to generate_series.
func TestCompileTVF2_DriverValueToFuncValue_ViaSQL(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	// int64 bind param
	vals := tvf2QueryInts(t, db, "SELECT value FROM generate_series(1, ?)", int64(3))
	if len(vals) != 3 {
		t.Errorf("int64 bind: want 3 rows, got %d", len(vals))
	}

	// float64 bind param (generate_series converts via AsInt64)
	vals = tvf2QueryInts(t, db, "SELECT value FROM generate_series(1, ?)", float64(2.0))
	if len(vals) < 1 {
		t.Errorf("float64 bind: want rows, got 0")
	}

	// string bind param
	vals = tvf2QueryInts(t, db, "SELECT value FROM generate_series(1, ?)", "2")
	if len(vals) < 1 {
		t.Errorf("string bind: want rows, got 0")
	}
}

// ---------------------------------------------------------------------------
// evalTVFWhere + evalTVFUnary: IS NULL / IS NOT NULL
// ---------------------------------------------------------------------------

// TestCompileTVF2_EvalTVFUnary_IsNull covers evalTVFUnary OpIsNull via WHERE IS NULL.
func TestCompileTVF2_EvalTVFUnary_IsNull(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	// generate_series values are never null, so WHERE value IS NULL returns 0 rows.
	vals := tvf2QueryInts(t, db, "SELECT value FROM generate_series(1, 5) WHERE value IS NULL")
	if len(vals) != 0 {
		t.Errorf("IS NULL: want 0 rows, got %d: %v", len(vals), vals)
	}
}

// TestCompileTVF2_EvalTVFUnary_IsNotNull covers evalTVFUnary OpNotNull via IS NOT NULL.
func TestCompileTVF2_EvalTVFUnary_IsNotNull(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	// All values are non-null, so WHERE value IS NOT NULL returns all 4 rows.
	vals := tvf2QueryInts(t, db, "SELECT value FROM generate_series(1, 4) WHERE value IS NOT NULL")
	if len(vals) != 4 {
		t.Errorf("IS NOT NULL: want 4 rows, got %d: %v", len(vals), vals)
	}
}

// ---------------------------------------------------------------------------
// evalTVFWhere with unhandled expression type (default branch → true)
// ---------------------------------------------------------------------------

// TestCompileTVF2_EvalTVFWhere_Default covers the default/conservative branch
// of evalTVFWhere by combining an AND with an IS NOT NULL (uses unary path).
func TestCompileTVF2_EvalTVFWhere_Default(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	// Combine unary (IS NOT NULL) and comparison (> 2) in the same WHERE.
	vals := tvf2QueryInts(t, db,
		"SELECT value FROM generate_series(1, 5) WHERE value IS NOT NULL AND value > 2")
	if len(vals) != 3 {
		t.Errorf("combined where: want 3, got %d: %v", len(vals), vals)
	}
}

// ---------------------------------------------------------------------------
// tvfNullsFirst / tvfCompareNull / tvfRowLess — ORDER BY with NULLs
// ---------------------------------------------------------------------------

// TestCompileTVF2_TVFNullsFirst_ExplicitTrue covers tvfNullsFirst when NullsFirst
// is explicitly set to true.
func TestCompileTVF2_TVFNullsFirst_ExplicitTrue(t *testing.T) {
	t.Parallel()
	yes := true
	k := tvfSortKey{nullsFirst: &yes, desc: false}
	if !tvfNullsFirst(k) {
		t.Error("tvfNullsFirst: want true when NullsFirst=&true")
	}
}

// TestCompileTVF2_TVFNullsFirst_ExplicitFalse covers tvfNullsFirst NullsFirst=false.
func TestCompileTVF2_TVFNullsFirst_ExplicitFalse(t *testing.T) {
	t.Parallel()
	no := false
	k := tvfSortKey{nullsFirst: &no, desc: false}
	if tvfNullsFirst(k) {
		t.Error("tvfNullsFirst: want false when NullsFirst=&false")
	}
}

// TestCompileTVF2_TVFNullsFirst_DefaultAsc covers tvfNullsFirst nil pointer, ASC
// (NULLS LAST is default for ASC in SQLite, so !desc=true → nullsFirst=true).
func TestCompileTVF2_TVFNullsFirst_DefaultAsc(t *testing.T) {
	t.Parallel()
	k := tvfSortKey{nullsFirst: nil, desc: false}
	// !desc = true, so nulls come first for ASC
	if !tvfNullsFirst(k) {
		t.Error("tvfNullsFirst: want true for ASC with nil NullsFirst")
	}
}

// TestCompileTVF2_TVFNullsFirst_DefaultDesc covers tvfNullsFirst nil pointer, DESC.
func TestCompileTVF2_TVFNullsFirst_DefaultDesc(t *testing.T) {
	t.Parallel()
	k := tvfSortKey{nullsFirst: nil, desc: true}
	// !desc = false, so nulls come last for DESC
	if tvfNullsFirst(k) {
		t.Error("tvfNullsFirst: want false for DESC with nil NullsFirst")
	}
}

// TestCompileTVF2_TVFCompareNull_BothNull covers tvfCompareNull both-null branch.
func TestCompileTVF2_TVFCompareNull_BothNull(t *testing.T) {
	t.Parallel()
	k := tvfSortKey{nullsFirst: nil, desc: false}
	cmp, isNull := tvfCompareNull(nil, nil, k)
	if !isNull {
		t.Error("tvfCompareNull both-nil: want isNull=true")
	}
	if cmp != 0 {
		t.Errorf("tvfCompareNull both-nil: want cmp=0, got %d", cmp)
	}
}

// TestCompileTVF2_TVFCompareNull_NeitherNull covers tvfCompareNull neither-null branch.
func TestCompileTVF2_TVFCompareNull_NeitherNull(t *testing.T) {
	t.Parallel()
	a := functions.NewIntValue(1)
	b := functions.NewIntValue(2)
	k := tvfSortKey{}
	cmp, isNull := tvfCompareNull(a, b, k)
	if isNull {
		t.Error("tvfCompareNull neither-null: want isNull=false")
	}
	if cmp != 0 {
		t.Errorf("tvfCompareNull neither-null: want cmp=0 (pass-through), got %d", cmp)
	}
}

// TestCompileTVF2_TVFCompareNull_ANull_NullsFirst covers tvfCompareNull A-null, nulls-first.
func TestCompileTVF2_TVFCompareNull_ANull_NullsFirst(t *testing.T) {
	t.Parallel()
	yes := true
	k := tvfSortKey{nullsFirst: &yes}
	cmp, isNull := tvfCompareNull(nil, functions.NewIntValue(5), k)
	if !isNull {
		t.Error("want isNull=true")
	}
	if cmp >= 0 {
		t.Errorf("A=null, nullsFirst: want cmp<0, got %d", cmp)
	}
}

// TestCompileTVF2_TVFCompareNull_ANull_NullsLast covers tvfCompareNull A-null, nulls-last.
func TestCompileTVF2_TVFCompareNull_ANull_NullsLast(t *testing.T) {
	t.Parallel()
	no := false
	k := tvfSortKey{nullsFirst: &no}
	cmp, isNull := tvfCompareNull(nil, functions.NewIntValue(5), k)
	if !isNull {
		t.Error("want isNull=true")
	}
	if cmp <= 0 {
		t.Errorf("A=null, nullsLast: want cmp>0, got %d", cmp)
	}
}

// TestCompileTVF2_TVFCompareNull_BNull_NullsFirst covers B-null, nulls-first.
func TestCompileTVF2_TVFCompareNull_BNull_NullsFirst(t *testing.T) {
	t.Parallel()
	yes := true
	k := tvfSortKey{nullsFirst: &yes}
	cmp, isNull := tvfCompareNull(functions.NewIntValue(5), nil, k)
	if !isNull {
		t.Error("want isNull=true")
	}
	if cmp <= 0 {
		t.Errorf("B=null, nullsFirst: want cmp>0, got %d", cmp)
	}
}

// TestCompileTVF2_TVFCompareNull_BNull_NullsLast covers B-null, nulls-last.
func TestCompileTVF2_TVFCompareNull_BNull_NullsLast(t *testing.T) {
	t.Parallel()
	no := false
	k := tvfSortKey{nullsFirst: &no}
	cmp, isNull := tvfCompareNull(functions.NewIntValue(5), nil, k)
	if !isNull {
		t.Error("want isNull=true")
	}
	if cmp >= 0 {
		t.Errorf("B=null, nullsLast: want cmp<0, got %d", cmp)
	}
}

// TestCompileTVF2_TVFRowLess_Equal covers tvfRowLess when values are equal (returns false).
func TestCompileTVF2_TVFRowLess_Equal(t *testing.T) {
	t.Parallel()
	a := []functions.Value{functions.NewIntValue(5)}
	b := []functions.Value{functions.NewIntValue(5)}
	k := []tvfSortKey{{colIdx: 0}}
	if tvfRowLess(a, b, k) {
		t.Error("tvfRowLess equal values: want false")
	}
}

// TestCompileTVF2_TVFRowLess_Desc covers tvfRowLess DESC comparison.
func TestCompileTVF2_TVFRowLess_Desc(t *testing.T) {
	t.Parallel()
	a := []functions.Value{functions.NewIntValue(10)}
	b := []functions.Value{functions.NewIntValue(3)}
	k := []tvfSortKey{{colIdx: 0, desc: true}}
	// DESC: 10 > 3, so a sorts before b (cmp>0 && desc → true)
	if !tvfRowLess(a, b, k) {
		t.Error("tvfRowLess DESC: a(10) should sort before b(3)")
	}
}

// TestCompileTVF2_TVFRowLess_NullEqual covers tvfRowLess when both values are null
// (equal nulls → continue to next key, fall through → false).
func TestCompileTVF2_TVFRowLess_NullEqual(t *testing.T) {
	t.Parallel()
	a := []functions.Value{nil, functions.NewIntValue(1)}
	b := []functions.Value{nil, functions.NewIntValue(2)}
	k := []tvfSortKey{{colIdx: 0}, {colIdx: 1}}
	// First key both null (equal), second key a=1 < b=2
	if !tvfRowLess(a, b, k) {
		t.Error("tvfRowLess null-then-int: want true (a<b on second key)")
	}
}

// ---------------------------------------------------------------------------
// compareByType — float and text branches
// ---------------------------------------------------------------------------

// TestCompileTVF2_CompareByType_Float covers compareByType for float values.
func TestCompileTVF2_CompareByType_Float(t *testing.T) {
	t.Parallel()
	a := functions.NewFloatValue(1.5)
	b := functions.NewFloatValue(2.5)
	if compareByType(a, b, functions.TypeFloat) >= 0 {
		t.Error("compareByType float: a(1.5) should be < b(2.5)")
	}
	if compareByType(b, a, functions.TypeFloat) <= 0 {
		t.Error("compareByType float: b(2.5) should be > a(1.5)")
	}
	c := functions.NewFloatValue(1.5)
	if compareByType(a, c, functions.TypeFloat) != 0 {
		t.Error("compareByType float: equal values should return 0")
	}
}

// TestCompileTVF2_CompareByType_Text covers compareByType for text (default) values.
func TestCompileTVF2_CompareByType_Text(t *testing.T) {
	t.Parallel()
	a := functions.NewTextValue("apple")
	b := functions.NewTextValue("zebra")
	if compareByType(a, b, functions.TypeText) >= 0 {
		t.Error("compareByType text: 'apple' should be < 'zebra'")
	}
	if compareByType(b, a, functions.TypeText) <= 0 {
		t.Error("compareByType text: 'zebra' should be > 'apple'")
	}
	c := functions.NewTextValue("apple")
	if compareByType(a, c, functions.TypeText) != 0 {
		t.Error("compareByType text: equal should return 0")
	}
}

// ---------------------------------------------------------------------------
// applyTVFLimit — via correlated TVF join with LIMIT
// ---------------------------------------------------------------------------

// TestCompileTVF2_ApplyTVFLimit covers applyTVFLimit via a correlated TVF join.
func TestCompileTVF2_ApplyTVFLimit(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE lim_nums (n INTEGER)",
		"INSERT INTO lim_nums VALUES (10)",
		"INSERT INTO lim_nums VALUES (20)",
	)

	// Correlated join: lim_nums cross generate_series(1,n). LIMIT applies via
	// applyTVFLimit in compileCorrelatedTVFJoin.
	rows, err := db.Query("SELECT n, value FROM lim_nums, generate_series(1, n) LIMIT 3")
	if err != nil {
		t.Fatalf("LIMIT on correlated TVF: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("LIMIT 3: want 3 rows, got %d", count)
	}
}

// TestCompileTVF2_ApplyTVFLimit_LargerThanRows covers applyTVFLimit when limit
// exceeds available rows (returns all rows unchanged).
func TestCompileTVF2_ApplyTVFLimit_LargerThanRows(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE lim2 (n INTEGER)",
		"INSERT INTO lim2 VALUES (2)",
	)

	// 2 rows generated, LIMIT 100 should return all 2.
	rows, err := db.Query("SELECT value FROM lim2, generate_series(1, n) LIMIT 100")
	if err != nil {
		t.Fatalf("LIMIT 100 on small set: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("LIMIT 100 (2 rows total): want 2, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// emitCorrelatedAggregate / resolveAggColName / computeAggregate /
// computeCount / computeCountDistinct / findTVFColIdx
// ---------------------------------------------------------------------------

// TestCompileTVF2_EmitCorrelatedAggregate_CountStar covers emitCorrelatedAggregate
// with COUNT(*) over a correlated TVF join.
func TestCompileTVF2_EmitCorrelatedAggregate_CountStar(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE agg_nums (n INTEGER)",
		"INSERT INTO agg_nums VALUES (5)",
	)

	got := tvf2QueryOneInt(t, db,
		"SELECT COUNT(*) FROM agg_nums, generate_series(1, n)")
	if got != 5 {
		t.Errorf("COUNT(*) correlated: want 5, got %d", got)
	}
}

// TestCompileTVF2_EmitCorrelatedAggregate_CountCol covers computeCount (non-star).
func TestCompileTVF2_EmitCorrelatedAggregate_CountCol(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE cnt_col (n INTEGER)",
		"INSERT INTO cnt_col VALUES (4)",
	)

	// COUNT(value) counts non-null values of the TVF column.
	got := tvf2QueryOneInt(t, db,
		"SELECT COUNT(value) FROM cnt_col, generate_series(1, n)")
	if got != 4 {
		t.Errorf("COUNT(value): want 4, got %d", got)
	}
}

// TestCompileTVF2_EmitCorrelatedAggregate_CountDistinct covers computeCountDistinct.
func TestCompileTVF2_EmitCorrelatedAggregate_CountDistinct(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE cnt_dist (n INTEGER)",
		"INSERT INTO cnt_dist VALUES (5)",
	)

	// COUNT(DISTINCT value) should count 5 distinct values 1..5.
	got := tvf2QueryOneInt(t, db,
		"SELECT COUNT(DISTINCT value) FROM cnt_dist, generate_series(1, n)")
	if got != 5 {
		t.Errorf("COUNT(DISTINCT value): want 5, got %d", got)
	}
}

// TestCompileTVF2_ResolveAggColName_Alias covers resolveAggColName with alias.
func TestCompileTVF2_ResolveAggColName_Alias(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE agg_alias (n INTEGER)",
		"INSERT INTO agg_alias VALUES (3)",
	)

	rows, err := db.Query(
		"SELECT COUNT(*) AS total FROM agg_alias, generate_series(1, n)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) == 0 || cols[0] != "total" {
		t.Errorf("alias: want col 'total', got %v", cols)
	}
	for rows.Next() {
	}
}

// TestCompileTVF2_ResolveAggColName_NoAlias covers resolveAggColName without alias.
func TestCompileTVF2_ResolveAggColName_NoAlias(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE agg_noalias (n INTEGER)",
		"INSERT INTO agg_noalias VALUES (2)",
	)

	rows, err := db.Query(
		"SELECT COUNT(*) FROM agg_noalias, generate_series(1, n)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) == 0 {
		t.Errorf("want at least one column name, got none")
	}
	for rows.Next() {
	}
}

// ---------------------------------------------------------------------------
// evalCorrelatedArg — literal, ident (column ref), variable branches
// ---------------------------------------------------------------------------

// TestCompileTVF2_EvalCorrelatedArg_Literal covers the LiteralExpr branch of
// evalCorrelatedArg via a correlated call with a mix of literal and column ref args.
func TestCompileTVF2_EvalCorrelatedArg_Literal(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE lit_tbl (n INTEGER)",
		"INSERT INTO lit_tbl VALUES (4)",
	)

	// generate_series(1, n) — start=1 is a literal (LiteralExpr branch),
	// stop=n is an IdentExpr (column ref branch). Both branches of evalCorrelatedArg
	// are exercised. The query produces values 1..4.
	vals := tvf2QueryInts(t, db, "SELECT value FROM lit_tbl, generate_series(1, n)")
	if len(vals) != 4 {
		t.Errorf("literal correlated arg: want 4, got %d: %v", len(vals), vals)
	}
}

// TestCompileTVF2_EvalCorrelatedArg_ColumnRef covers the IdentExpr (column ref) branch.
func TestCompileTVF2_EvalCorrelatedArg_ColumnRef(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE ref_tbl (n INTEGER)",
		"INSERT INTO ref_tbl VALUES (3)",
		"INSERT INTO ref_tbl VALUES (2)",
	)

	// generate_series(1, n) — n is a column reference, exercises IdentExpr branch.
	vals := tvf2QueryInts(t, db, "SELECT value FROM ref_tbl, generate_series(1, n)")
	// 3 + 2 = 5 total rows
	if len(vals) != 5 {
		t.Errorf("column ref correlated arg: want 5, got %d: %v", len(vals), vals)
	}
}

// ---------------------------------------------------------------------------
// goToFuncValue — all type branches
// ---------------------------------------------------------------------------

// TestCompileTVF2_GoToFuncValue covers every type branch of goToFuncValue.
func TestCompileTVF2_GoToFuncValue(t *testing.T) {
	t.Parallel()

	// nil
	if v := goToFuncValue(nil); !v.IsNull() {
		t.Error("nil: want null")
	}
	// int64
	if v := goToFuncValue(int64(42)); v.AsInt64() != 42 {
		t.Errorf("int64: want 42, got %v", v.AsInt64())
	}
	// float64
	if v := goToFuncValue(float64(2.71)); v.AsFloat64() != 2.71 {
		t.Errorf("float64: want 2.71, got %v", v.AsFloat64())
	}
	// string
	if v := goToFuncValue("test"); v.AsString() != "test" {
		t.Errorf("string: want 'test', got %v", v.AsString())
	}
	// []byte
	if v := goToFuncValue([]byte("bin")); v.Type() != functions.TypeBlob {
		t.Errorf("[]byte: want TypeBlob, got %v", v.Type())
	}
	// unknown type — falls back to fmt.Sprintf
	if v := goToFuncValue(true); v.AsString() == "" {
		t.Error("unknown type: want non-empty string")
	}
}

// TestCompileTVF2_GoToFuncValue_ViaSQL exercises goToFuncValue through the
// flattenCorrelatedRows path (outer table rows converted via goToFuncValue).
func TestCompileTVF2_GoToFuncValue_ViaSQL(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE gofunc_tbl (id INTEGER, label TEXT, score REAL)",
		"INSERT INTO gofunc_tbl VALUES (1, 'alpha', 1.5)",
		"INSERT INTO gofunc_tbl VALUES (2, 'beta', 2.5)",
	)

	// The join reads outer rows (id INTEGER, label TEXT, score REAL) and converts
	// each field via goToFuncValue in flattenCorrelatedRows.
	rows, err := db.Query("SELECT id, value FROM gofunc_tbl, generate_series(1, id)")
	if err != nil {
		t.Fatalf("goToFuncValue via SQL: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	// id=1 → 1 row, id=2 → 2 rows = 3 total
	if count != 3 {
		t.Errorf("want 3 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// resolveCorrelatedColName — all branches
// ---------------------------------------------------------------------------

// TestCompileTVF2_ResolveCorrelatedColName_Ident covers the IdentExpr branch.
func TestCompileTVF2_ResolveCorrelatedColName_Ident(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE rcol_tbl (n INTEGER)",
		"INSERT INTO rcol_tbl VALUES (2)",
	)

	// SELECT n, value — both are IdentExpr columns; resolveCorrelatedColName ident branch.
	rows, err := db.Query("SELECT n, value FROM rcol_tbl, generate_series(1, n)")
	if err != nil {
		t.Fatalf("resolveCorrelatedColName ident: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) != 2 {
		t.Errorf("want 2 cols, got %v", cols)
	}
	for rows.Next() {
	}
}

// TestCompileTVF2_ResolveCorrelatedColName_FunctionExpr covers the FunctionExpr branch.
func TestCompileTVF2_ResolveCorrelatedColName_FunctionExpr(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE rcolfn (n INTEGER)",
		"INSERT INTO rcolfn VALUES (3)",
	)

	// COUNT(*) is a FunctionExpr; resolveCorrelatedColName picks fn.Name.
	rows, err := db.Query("SELECT COUNT(*) FROM rcolfn, generate_series(1, n)")
	if err != nil {
		t.Fatalf("resolveCorrelatedColName function: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) == 0 {
		t.Error("want at least one column")
	}
	for rows.Next() {
	}
}

// TestCompileTVF2_ResolveCorrelatedColName_Alias covers the alias branch.
func TestCompileTVF2_ResolveCorrelatedColName_Alias(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE rcoalias (n INTEGER)",
		"INSERT INTO rcoalias VALUES (2)",
	)

	rows, err := db.Query(
		"SELECT COUNT(*) AS cnt FROM rcoalias, generate_series(1, n)")
	if err != nil {
		t.Fatalf("resolveCorrelatedColName alias: %v", err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if len(cols) == 0 || cols[0] != "cnt" {
		t.Errorf("want alias 'cnt', got %v", cols)
	}
	for rows.Next() {
	}
}

// ---------------------------------------------------------------------------
// emitCorrelatedGroupByAggregate / resolveGroupByColIdxs / groupCorrelatedRows /
// makeGroupKey / sortGroupKeys / evalGroupByCol
// ---------------------------------------------------------------------------

// TestCompileTVF2_GroupByAggregate_CountStar covers emitCorrelatedGroupByAggregate,
// resolveGroupByColIdxs, groupCorrelatedRows, makeGroupKey, sortGroupKeys,
// evalGroupByCol (aggregate branch).
func TestCompileTVF2_GroupByAggregate_CountStar(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE grp_tbl (n INTEGER)",
		"INSERT INTO grp_tbl VALUES (3)",
		"INSERT INTO grp_tbl VALUES (3)",
		"INSERT INTO grp_tbl VALUES (5)",
	)

	// GROUP BY n forces emitCorrelatedGroupByAggregate path.
	// n=3 appears twice → two outer rows → generate_series(1,3) each → 6 joined rows for n=3
	// n=5 appears once → generate_series(1,5) → 5 joined rows for n=5
	rows, err := db.Query(
		"SELECT n, COUNT(*) FROM grp_tbl, generate_series(1, n) GROUP BY n")
	if err != nil {
		t.Fatalf("GROUP BY correlated TVF: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
		var n, cnt int64
		if err := rows.Scan(&n, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	// Should have 2 groups: n=3 and n=5
	if count != 2 {
		t.Errorf("GROUP BY: want 2 groups, got %d", count)
	}
}

// TestCompileTVF2_GroupByAggregate_NonAggCol covers evalGroupByCol non-aggregate
// branch (IdentExpr returning first-row value).
func TestCompileTVF2_GroupByAggregate_NonAggCol(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE grp2 (n INTEGER)",
		"INSERT INTO grp2 VALUES (2)",
		"INSERT INTO grp2 VALUES (4)",
	)

	// SELECT n, COUNT(*) — n is a non-aggregate IdentExpr, exercises evalGroupByCol
	// IdentExpr branch that reads from first row in the group.
	rows, err := db.Query(
		"SELECT n, COUNT(*) FROM grp2, generate_series(1, n) GROUP BY n")
	if err != nil {
		t.Fatalf("GROUP BY non-agg col: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n, cnt int64
		if err := rows.Scan(&n, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
}

// TestCompileTVF2_GroupByAggregate_EmptyGroup covers evalGroupByCol with empty groupRows.
func TestCompileTVF2_GroupByAggregate_EmptyGroup(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE grp3 (n INTEGER)",
	)

	// No rows → no groups → query returns nothing but must not error.
	rows, err := db.Query(
		"SELECT n, COUNT(*) FROM grp3, generate_series(1, n) GROUP BY n")
	if err != nil {
		t.Fatalf("GROUP BY empty: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// ---------------------------------------------------------------------------
// ORDER BY on correlated TVF join (exercises tvfRowLess / compareByType int path)
// ---------------------------------------------------------------------------

// TestCompileTVF2_CorrelatedJoin_OrderByDesc covers sortTVFRows DESC on a
// correlated join result (tvfRowLess DESC path, compareByType integer path).
func TestCompileTVF2_CorrelatedJoin_OrderByDesc(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE ord_tbl (n INTEGER)",
		"INSERT INTO ord_tbl VALUES (3)",
	)

	vals := tvf2QueryInts(t, db,
		"SELECT value FROM ord_tbl, generate_series(1, n) ORDER BY value DESC")
	if len(vals) != 3 {
		t.Fatalf("ORDER BY DESC: want 3, got %d", len(vals))
	}
	if vals[0] != 3 || vals[1] != 2 || vals[2] != 1 {
		t.Errorf("ORDER BY DESC: want [3,2,1], got %v", vals)
	}
}

// TestCompileTVF2_CorrelatedJoin_Distinct covers DISTINCT on a correlated join.
func TestCompileTVF2_CorrelatedJoin_Distinct(t *testing.T) {
	t.Parallel()
	db := tvf2OpenDB(t)
	defer db.Close()

	tvf2Exec(t, db,
		"CREATE TABLE dist_tbl (n INTEGER)",
		"INSERT INTO dist_tbl VALUES (3)",
		"INSERT INTO dist_tbl VALUES (3)",
	)

	// Both rows produce generate_series(1,3); DISTINCT removes duplicates.
	vals := tvf2QueryInts(t, db,
		"SELECT DISTINCT value FROM dist_tbl, generate_series(1, n)")
	if len(vals) != 3 {
		t.Errorf("DISTINCT correlated: want 3, got %d: %v", len(vals), vals)
	}
}
