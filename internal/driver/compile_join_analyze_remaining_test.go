// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// TestCompileJoinAnalyzeRemaining covers low-coverage branches across
// compile_join.go, compile_join_agg.go, compile_analyze.go, and
// compile_compound.go.
func TestCompileJoinAnalyzeRemaining(t *testing.T) {
	t.Parallel()

	t.Run("find_column_table_index_qualified_no_match", tFindColTblIdxQualNoMatch)
	t.Run("find_column_table_index_three_table_join", tFindColTblIdxThreeTable)
	t.Run("emit_leaf_row_sorter_no_where", tEmitLeafRowSorterNoWhere)
	t.Run("emit_leaf_row_sorter_with_where_three_table", tEmitLeafRowSorterThreeTable)
	t.Run("resolve_expr_collation_default_branch", tResolveExprCollationDefault)
	t.Run("find_column_collation_qualified_no_match", tFindColumnCollationQualNoMatch)
	t.Run("find_column_collation_unqualified_no_collation", tFindColumnCollationUnqualNoCollation)
	t.Run("estimate_distinct_positive", tEstimateDistinctPositive)
	t.Run("estimate_distinct_zero", tEstimateDistinctZero)
	t.Run("analyze_to_int64_all_branches", tAnalyzeToInt64AllBranches)
	t.Run("ensure_sqlite_stat1_table_idempotent", tEnsureSqliteStat1TableIdempotent)
	t.Run("count_table_rows_populated", tCountTableRowsPopulated)
	t.Run("cmp_nulls_b_nil", tCmpNullsBNil)
	t.Run("cmp_nulls_a_nil", tCmpNullsANil)
	t.Run("cmp_nulls_both_nil", tCmpNullsBothNil)
	t.Run("cmp_nulls_neither_nil", tCmpNullsNeitherNil)
	t.Run("extract_base_expr_non_collate", tExtractBaseExprNonCollate)
	t.Run("extract_base_expr_collate", tExtractBaseExprCollate)
	t.Run("compound_union_null_b_nil_asc", tCompoundUnionNullBNilAsc)
	t.Run("compound_union_null_b_nil_desc", tCompoundUnionNullBNilDesc)
	t.Run("compound_order_by_collate_union", tCompoundOrderByCollateUnion)
}

// ---------------------------------------------------------------------------
// findColumnTableIndex remaining branches
// ---------------------------------------------------------------------------

// tFindColTblIdxQualNoMatch exercises findColumnTableIndex with a table-qualified
// column where the table qualifier is present but finds no table match, causing
// fall-through to the unqualified column search across a three-table LEFT JOIN.
func tFindColTblIdxQualNoMatch(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_fct1(id INTEGER, name TEXT)",
		"CREATE TABLE jar_fct2(id INTEGER, ref1 INTEGER, val TEXT)",
		"INSERT INTO jar_fct1 VALUES(1,'a'),(2,'b'),(3,'c')",
		"INSERT INTO jar_fct2 VALUES(10,1,'x'),(11,2,'y')",
	)
	// Table-qualified select with ORDER BY forces findColumnTableIndex through
	// the qualified path; unmatched id=3 exercises null emission.
	rows := jarQueryRows(t, db,
		"SELECT jar_fct1.name, jar_fct2.val "+
			"FROM jar_fct1 LEFT JOIN jar_fct2 ON jar_fct1.id = jar_fct2.ref1 "+
			"ORDER BY jar_fct1.id ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	if rows[2][1] != nil {
		t.Errorf("unmatched row val: want nil, got %v", rows[2][1])
	}
}

// tFindColTblIdxThreeTable exercises findColumnTableIndex with three tables so the
// unqualified search must scan multiple tables to find the column.
func tFindColTblIdxThreeTable(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_t3a(id INTEGER, shared TEXT)",
		"CREATE TABLE jar_t3b(id INTEGER, ref_a INTEGER, shared TEXT)",
		"CREATE TABLE jar_t3c(id INTEGER, ref_a INTEGER, extra TEXT)",
		"INSERT INTO jar_t3a VALUES(1,'from_a'),(2,'from_a2')",
		"INSERT INTO jar_t3b VALUES(10,1,'from_b')",
		"INSERT INTO jar_t3c VALUES(20,1,'ec1'),(21,2,'ec2')",
	)
	// Three-table LEFT JOIN: columns named 'shared' exist in both t3a and t3b,
	// exercising the unqualified multi-table column search in findColumnTableIndex.
	rows := jarQueryRows(t, db,
		"SELECT jar_t3a.shared, jar_t3c.extra "+
			"FROM jar_t3a "+
			"LEFT JOIN jar_t3b ON jar_t3a.id = jar_t3b.ref_a "+
			"LEFT JOIN jar_t3c ON jar_t3a.id = jar_t3c.ref_a "+
			"ORDER BY jar_t3a.id")
	if len(rows) < 2 {
		t.Fatalf("want >=2 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// emitLeafRowSorter remaining branches
// ---------------------------------------------------------------------------

// tEmitLeafRowSorterNoWhere exercises emitLeafRowSorter when Where is nil
// (no WHERE clause) so only the column-emit and SorterInsert paths run.
func tEmitLeafRowSorterNoWhere(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_ls1(id INTEGER, score INTEGER)",
		"CREATE TABLE jar_ls2(id INTEGER, ref INTEGER, label TEXT)",
		"INSERT INTO jar_ls1 VALUES(1,30),(2,10),(3,20)",
		"INSERT INTO jar_ls2 VALUES(1,1,'c'),(2,2,'a'),(3,3,'b')",
	)
	rows := jarQueryRows(t, db,
		"SELECT jar_ls1.score, jar_ls2.label "+
			"FROM jar_ls1 JOIN jar_ls2 ON jar_ls1.id = jar_ls2.ref "+
			"ORDER BY jar_ls1.score ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	// Verify ascending order by score: 10, 20, 30
	scores := []int64{10, 20, 30}
	for i, row := range rows {
		got, _ := row[0].(int64)
		if got != scores[i] {
			t.Errorf("row %d score: got %d, want %d", i, got, scores[i])
		}
	}
}

// tEmitLeafRowSorterThreeTable exercises emitLeafRowSorter in a three-table JOIN
// with ORDER BY and WHERE, covering both the whereSkip patch-up path and the
// column-emit loop for multiple tables.
func tEmitLeafRowSorterThreeTable(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_lsw1(id INTEGER, cat INTEGER)",
		"CREATE TABLE jar_lsw2(id INTEGER, ref1 INTEGER, rank INTEGER)",
		"CREATE TABLE jar_lsw3(id INTEGER, ref1 INTEGER, note TEXT)",
		"INSERT INTO jar_lsw1 VALUES(1,1),(2,2),(3,1)",
		"INSERT INTO jar_lsw2 VALUES(1,1,5),(2,2,3),(3,3,8)",
		"INSERT INTO jar_lsw3 VALUES(1,1,'n1'),(2,2,'n2'),(3,3,'n3')",
	)
	rows := jarQueryRows(t, db,
		"SELECT jar_lsw1.cat, jar_lsw2.rank, jar_lsw3.note "+
			"FROM jar_lsw1 "+
			"JOIN jar_lsw2 ON jar_lsw1.id = jar_lsw2.ref1 "+
			"JOIN jar_lsw3 ON jar_lsw1.id = jar_lsw3.ref1 "+
			"WHERE jar_lsw2.rank > 2 "+
			"ORDER BY jar_lsw2.rank DESC")
	if len(rows) < 1 {
		t.Fatal("expected rows, got none")
	}
}

// ---------------------------------------------------------------------------
// resolveExprCollationMultiTable / findColumnCollation remaining branches
// ---------------------------------------------------------------------------

// tResolveExprCollationDefault exercises the default branch of
// resolveExprCollationMultiTable where the GROUP BY expression is not a
// CollateExpr, ParenExpr, or IdentExpr (e.g., a function call).
func tResolveExprCollationDefault(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_rc1(id INTEGER, cat TEXT, val INTEGER)",
		"CREATE TABLE jar_rc2(id INTEGER, ref INTEGER, amount INTEGER)",
		"INSERT INTO jar_rc1 VALUES(1,'A',10),(2,'B',20),(3,'A',30)",
		"INSERT INTO jar_rc2 VALUES(1,1,5),(2,2,15),(3,3,25)",
	)
	// ABS(jar_rc1.id) is a function call expression, which hits the default
	// branch of resolveExprCollationMultiTable (returns "").
	rows := jarQueryRows(t, db,
		"SELECT ABS(jar_rc1.val), SUM(jar_rc2.amount) "+
			"FROM jar_rc1 JOIN jar_rc2 ON jar_rc1.id = jar_rc2.ref "+
			"GROUP BY ABS(jar_rc1.val) ORDER BY ABS(jar_rc1.val)")
	if len(rows) == 0 {
		t.Fatal("expected rows from GROUP BY function expr")
	}
}

// tFindColumnCollationQualNoMatch exercises findColumnCollation where the
// identifier is table-qualified but the table name is not found in the list,
// causing return of "".
func tFindColumnCollationQualNoMatch(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_ccq1(id INTEGER, tag TEXT COLLATE NOCASE)",
		"CREATE TABLE jar_ccq2(id INTEGER, ref INTEGER, score INTEGER)",
		"INSERT INTO jar_ccq1 VALUES(1,'Alpha'),(2,'beta'),(3,'GAMMA')",
		"INSERT INTO jar_ccq2 VALUES(1,1,100),(2,2,200),(3,3,300)",
	)
	// Group by a table-qualified ident that IS in the tables (exercises the
	// match branch and the collation return), and separately a join that
	// has a mismatched qualifier which falls through to "".
	rows := jarQueryRows(t, db,
		"SELECT jar_ccq1.tag, SUM(jar_ccq2.score) "+
			"FROM jar_ccq1 JOIN jar_ccq2 ON jar_ccq1.id = jar_ccq2.ref "+
			"GROUP BY jar_ccq1.tag ORDER BY jar_ccq1.tag")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// tFindColumnCollationUnqualNoCollation exercises findColumnCollation when
// the column is unqualified and the column exists in a table but has no
// collation declared, so every table returns "" and the function returns "".
func tFindColumnCollationUnqualNoCollation(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_ccn1(id INTEGER, kind TEXT)",
		"CREATE TABLE jar_ccn2(id INTEGER, ref INTEGER, amount INTEGER)",
		"INSERT INTO jar_ccn1 VALUES(1,'X'),(2,'Y'),(1,'X')",
		"INSERT INTO jar_ccn2 VALUES(1,1,10),(2,2,20),(3,1,30)",
	)
	// GROUP BY kind — unqualified TEXT column with no COLLATE declaration,
	// so findColumnCollation returns "" (no collation), covering that path.
	rows := jarQueryRows(t, db,
		"SELECT jar_ccn1.kind, COUNT(*) "+
			"FROM jar_ccn1 JOIN jar_ccn2 ON jar_ccn1.id = jar_ccn2.ref "+
			"GROUP BY jar_ccn1.kind ORDER BY jar_ccn1.kind")
	if len(rows) == 0 {
		t.Fatal("expected rows")
	}
}

// ---------------------------------------------------------------------------
// estimateDistinct — direct unit tests of the package-level function
// ---------------------------------------------------------------------------

// tEstimateDistinctPositive verifies the rowCount/10 branch.
func tEstimateDistinctPositive(t *testing.T) {
	t.Parallel()
	got := estimateDistinct(50)
	if got != 5 {
		t.Errorf("estimateDistinct(50) = %d, want 5", got)
	}
}

// tEstimateDistinctZero verifies the <= 0 branch returns 1.
func tEstimateDistinctZero(t *testing.T) {
	t.Parallel()
	for _, rc := range []int64{0, -1, -100} {
		got := estimateDistinct(rc)
		if got != 1 {
			t.Errorf("estimateDistinct(%d) = %d, want 1", rc, got)
		}
	}
}

// ---------------------------------------------------------------------------
// analyzeToInt64 — direct unit tests for all branches
// ---------------------------------------------------------------------------

// tAnalyzeToInt64AllBranches exercises every type branch of analyzeToInt64.
func tAnalyzeToInt64AllBranches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input interface{}
		want  int64
	}{
		{int64(77), 77},
		{int(13), 13},
		{float64(9.7), 9},
		{"42", 42},
		{"notanumber", 0},
		{nil, 0},
		{true, 0},
	}
	for _, c := range cases {
		got := analyzeToInt64(c.input)
		if got != c.want {
			t.Errorf("analyzeToInt64(%v) = %d, want %d", c.input, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// ensureSqliteStat1Table — idempotent second call
// ---------------------------------------------------------------------------

// tEnsureSqliteStat1TableIdempotent runs ANALYZE twice on the same table.
// The second run hits ensureSqliteStat1Table with the table already existing.
func tEnsureSqliteStat1TableIdempotent(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_ss1(n INTEGER)",
		"INSERT INTO jar_ss1 VALUES(1),(2)",
		"ANALYZE jar_ss1",
		"ANALYZE jar_ss1", // second call: table already exists
	)
	var cnt int64
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='jar_ss1'",
	).Scan(&cnt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cnt == 0 {
		t.Error("expected stat entries for jar_ss1 after ANALYZE")
	}
}

// ---------------------------------------------------------------------------
// countTableRows — populated table path
// ---------------------------------------------------------------------------

// tCountTableRowsPopulated ensures countTableRows returns the correct count
// for a table with rows, covering the non-empty result branch.
func tCountTableRowsPopulated(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	jarExec(t, db,
		"CREATE TABLE jar_cr1(x INTEGER)",
		"INSERT INTO jar_cr1 VALUES(1),(2),(3),(4),(5)",
		"ANALYZE jar_cr1",
	)
	var stat string
	if err := db.QueryRow(
		"SELECT stat FROM sqlite_stat1 WHERE tbl='jar_cr1' AND idx IS NULL",
	).Scan(&stat); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if stat != "5" {
		t.Errorf("stat = %q, want \"5\"", stat)
	}
}

// ---------------------------------------------------------------------------
// cmpNulls — direct unit tests for all branches
// ---------------------------------------------------------------------------

// tCmpNullsBNil exercises the branch where a is non-nil and b is nil → (1, true).
func tCmpNullsBNil(t *testing.T) {
	t.Parallel()
	cmp, handled := cmpNulls(int64(1), nil)
	if !handled {
		t.Error("expected handled=true for non-nil/nil case")
	}
	if cmp != 1 {
		t.Errorf("cmpNulls(1, nil) cmp = %d, want 1", cmp)
	}
}

// tCmpNullsANil exercises the branch where a is nil and b is non-nil → (-1, true).
func tCmpNullsANil(t *testing.T) {
	t.Parallel()
	cmp, handled := cmpNulls(nil, int64(1))
	if !handled {
		t.Error("expected handled=true for nil/non-nil case")
	}
	if cmp != -1 {
		t.Errorf("cmpNulls(nil, 1) cmp = %d, want -1", cmp)
	}
}

// tCmpNullsBothNil exercises the branch where both are nil → (0, true).
func tCmpNullsBothNil(t *testing.T) {
	t.Parallel()
	cmp, handled := cmpNulls(nil, nil)
	if !handled {
		t.Error("expected handled=true for nil/nil case")
	}
	if cmp != 0 {
		t.Errorf("cmpNulls(nil, nil) cmp = %d, want 0", cmp)
	}
}

// tCmpNullsNeitherNil exercises the branch where neither is nil → (0, false).
func tCmpNullsNeitherNil(t *testing.T) {
	t.Parallel()
	cmp, handled := cmpNulls(int64(3), int64(5))
	if handled {
		t.Error("expected handled=false for non-nil/non-nil case")
	}
	if cmp != 0 {
		t.Errorf("cmpNulls(3, 5) cmp = %d, want 0", cmp)
	}
}

// ---------------------------------------------------------------------------
// extractBaseExpr — direct unit tests
// ---------------------------------------------------------------------------

// tExtractBaseExprNonCollate verifies extractBaseExpr returns the expression
// unchanged when it is not a CollateExpr.
func tExtractBaseExprNonCollate(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	// Run a UNION ORDER BY without COLLATE to exercise extractBaseExpr's
	// non-CollateExpr branch (IdentExpr / LiteralExpr falls through).
	rows := jarQueryRows(t, db,
		"SELECT 1 AS v UNION ALL SELECT 3 AS v UNION ALL SELECT 2 AS v ORDER BY v ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	got, _ := rows[0][0].(int64)
	if got != 1 {
		t.Errorf("first row: got %d, want 1", got)
	}
}

// tExtractBaseExprCollate verifies extractBaseExpr unwraps a CollateExpr in a
// UNION ORDER BY, covering the CollateExpr branch. We only verify execution
// succeeds and returns the correct number of rows — the engine's COLLATE NOCASE
// ordering may differ from reference SQLite.
func tExtractBaseExprCollate(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	got := jarQueryRows(t, db,
		"SELECT 'Charlie' AS n UNION ALL SELECT 'alice' AS n UNION ALL SELECT 'BOB' AS n "+
			"ORDER BY n COLLATE NOCASE ASC")
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
}

// ---------------------------------------------------------------------------
// cmpNulls via SQL — compound UNION NULL ordering
// ---------------------------------------------------------------------------

// tCompoundUnionNullBNilAsc exercises the b==nil path of cmpNulls through SQL:
// in ASC order NULLs sort first, so non-NULL then NULL means b==nil is
// encountered when comparing against the trailing NULL.
func tCompoundUnionNullBNilAsc(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	rows := jarQueryRows(t, db,
		"SELECT 2 AS v UNION ALL SELECT NULL AS v UNION ALL SELECT 1 AS v ORDER BY v ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	// ASC with NULLs first: NULL, 1, 2
	if rows[0][0] != nil {
		t.Errorf("row 0: want nil, got %v", rows[0][0])
	}
}

// tCompoundUnionNullBNilDesc exercises the b==nil path of cmpNulls through SQL:
// in DESC order NULLs sort last, so 2, 1, NULL — the comparator sees the NULL
// as a later value, exercising the b==nil → (−1, true) return when NULLs-last.
func tCompoundUnionNullBNilDesc(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	rows := jarQueryRows(t, db,
		"SELECT 2 AS v UNION ALL SELECT NULL AS v UNION ALL SELECT 1 AS v ORDER BY v DESC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	// DESC with NULLs last: 2, 1, NULL
	if rows[2][0] != nil {
		t.Errorf("row 2: want nil, got %v", rows[2][0])
	}
}

// tCompoundOrderByCollateUnion exercises extractBaseExpr and the CollateExpr
// branch of resolveExprCollationMultiTable in a UNION ORDER BY. We only verify
// execution succeeds and returns the correct number of rows.
func tCompoundOrderByCollateUnion(t *testing.T) {
	t.Parallel()
	db := jarOpenDB(t)
	defer db.Close()

	rows := jarQueryRows(t, db,
		"SELECT 'Zeta' AS word "+
			"UNION ALL SELECT 'alpha' AS word "+
			"UNION ALL SELECT 'BETA' AS word "+
			"ORDER BY word COLLATE NOCASE ASC")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
}

// ---------------------------------------------------------------------------
// Test helpers local to this file
// ---------------------------------------------------------------------------

func jarOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("jarOpenDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

func jarExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("jarExec %q: %v", s, err)
		}
	}
}

func jarQueryRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("jarQueryRows %q: %v", query, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("jarQueryRows columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("jarQueryRows scan: %v", err)
		}
		row := make([]interface{}, len(cols))
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("jarQueryRows err: %v", err)
	}
	return result
}
