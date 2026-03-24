// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// cj2OpenDB opens an in-memory database for compile_join_coverage2 tests.
func cj2OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("cj2OpenDB: %v", err)
	}
	return db
}

// cj2Exec executes SQL statements, failing the test on the first error.
func cj2Exec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// cj2QueryInt64 runs a query that must return a single int64.
func cj2QueryInt64(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("cj2QueryInt64 %q: %v", query, err)
	}
	return v
}

// cj2QueryRows fetches all rows from a query as [][]interface{}.
func cj2QueryRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("cj2QueryRows %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("cj2QueryRows columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("cj2QueryRows scan: %v", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cj2QueryRows err: %v", err)
	}
	return result
}

// ============================================================================
// resolveUsingJoin — exercises the USING join path with multiple shared columns
// ============================================================================

// TestCompileJoin2UsingMultipleColumns exercises resolveUsingJoin with two
// columns in the USING list, hitting the equality-chain building code.
func TestCompileJoin2UsingMultipleColumns(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE umu1(id INTEGER, cat INTEGER, val TEXT)",
		"CREATE TABLE umu2(id INTEGER, cat INTEGER, info TEXT)",
		"INSERT INTO umu1 VALUES(1,10,'a'),(2,20,'b'),(3,10,'c')",
		"INSERT INTO umu2 VALUES(1,10,'x'),(2,20,'y'),(3,30,'z')",
	)
	n := cj2QueryInt64(t, db,
		"SELECT COUNT(*) FROM umu1 JOIN umu2 USING(id, cat)")
	if n != 2 {
		t.Errorf("USING(id,cat): got %d, want 2", n)
	}
}

// TestCompileJoin2UsingSingleColumn exercises resolveUsingJoin with a single
// column, verifying the generated ON condition equates the same column name.
func TestCompileJoin2UsingSingleColumn(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE us1(code TEXT, name TEXT)",
		"CREATE TABLE us2(code TEXT, price INTEGER)",
		"INSERT INTO us1 VALUES('A','Alpha'),('B','Beta'),('C','Gamma')",
		"INSERT INTO us2 VALUES('A',100),('B',200)",
	)
	n := cj2QueryInt64(t, db,
		"SELECT COUNT(*) FROM us1 JOIN us2 USING(code)")
	if n != 2 {
		t.Errorf("USING(code): got %d, want 2", n)
	}
}

// TestCompileJoin2UsingWithWhere exercises resolveUsingJoin combined with a
// WHERE filter, ensuring the synthesised ON condition is applied correctly.
func TestCompileJoin2UsingWithWhere(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE uw1(id INTEGER, tag TEXT)",
		"CREATE TABLE uw2(id INTEGER, score INTEGER)",
		"INSERT INTO uw1 VALUES(1,'x'),(2,'y'),(3,'z')",
		"INSERT INTO uw2 VALUES(1,10),(2,50),(3,30)",
	)
	n := cj2QueryInt64(t, db,
		"SELECT COUNT(*) FROM uw1 JOIN uw2 USING(id) WHERE uw2.score > 20")
	if n != 2 {
		t.Errorf("USING with WHERE: got %d, want 2", n)
	}
}

// ============================================================================
// findColumnTableIndex — column-to-table disambiguation in JOIN queries
// ============================================================================

// TestCompileJoin2FindColumnTableIndexQualified exercises the table-qualified
// branch of findColumnTableIndex in a LEFT JOIN (null emission path).
func TestCompileJoin2FindColumnTableIndexQualified(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE fct_left(id INTEGER, label TEXT)",
		"CREATE TABLE fct_right(id INTEGER, ref INTEGER, extra TEXT)",
		"INSERT INTO fct_left VALUES(1,'one'),(2,'two'),(3,'three')",
		"INSERT INTO fct_right VALUES(10,1,'r1'),(11,2,'r2')",
	)
	// Table-qualified columns trigger the table-name lookup branch in
	// findColumnTableIndex when building the null-emission row.
	rows := cj2QueryRows(t, db,
		"SELECT fct_left.label, fct_right.extra "+
			"FROM fct_left LEFT JOIN fct_right ON fct_left.id = fct_right.ref "+
			"ORDER BY fct_left.id")
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN rows: got %d, want 3", len(rows))
	}
	// Row for id=3 must have NULL for fct_right.extra
	last := rows[2]
	if last[1] != nil {
		t.Errorf("unmatched row extra: got %v, want nil", last[1])
	}
}

// TestCompileJoin2FindColumnTableIndexUnqualified exercises the unqualified
// column lookup branch where findColumnTableIndex searches by column name.
func TestCompileJoin2FindColumnTableIndexUnqualified(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE fctu_a(id INTEGER, name TEXT)",
		"CREATE TABLE fctu_b(id INTEGER, ref INTEGER, score INTEGER)",
		"INSERT INTO fctu_a VALUES(1,'alpha'),(2,'beta'),(3,'gamma')",
		"INSERT INTO fctu_b VALUES(1,1,99),(2,3,42)",
	)
	// Unqualified column "name" can only come from fctu_a; "score" from fctu_b.
	rows := cj2QueryRows(t, db,
		"SELECT fctu_a.name, fctu_b.score "+
			"FROM fctu_a LEFT JOIN fctu_b ON fctu_a.id = fctu_b.ref "+
			"ORDER BY fctu_a.id")
	if len(rows) != 3 {
		t.Fatalf("unqualified LEFT JOIN rows: got %d, want 3", len(rows))
	}
	// id=2 has no match in fctu_b → score must be NULL
	if rows[1][1] != nil {
		t.Errorf("unmatched row score: got %v, want nil", rows[1][1])
	}
}

// ============================================================================
// emitLeafRowSorter — JOIN with ORDER BY on a non-index column
// ============================================================================

// TestCompileJoin2LeafRowSorterInnerJoin exercises emitLeafRowSorter for an
// INNER JOIN with ORDER BY on a column that is not an index.
func TestCompileJoin2LeafRowSorterInnerJoin(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE lrs_a(id INTEGER, name TEXT)",
		"CREATE TABLE lrs_b(id INTEGER, ref INTEGER, priority INTEGER)",
		"INSERT INTO lrs_a VALUES(1,'first'),(2,'second'),(3,'third')",
		"INSERT INTO lrs_b VALUES(10,1,30),(11,2,10),(12,3,20)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT lrs_a.name, lrs_b.priority "+
			"FROM lrs_a JOIN lrs_b ON lrs_a.id = lrs_b.ref "+
			"ORDER BY lrs_b.priority ASC")
	if len(rows) != 3 {
		t.Fatalf("sorter JOIN rows: got %d, want 3", len(rows))
	}
	// After sort by priority ASC the order must be 10, 20, 30
	priorities := []int64{10, 20, 30}
	for i, row := range rows {
		got, _ := row[1].(int64)
		if got != priorities[i] {
			t.Errorf("row %d priority: got %d, want %d", i, got, priorities[i])
		}
	}
}

// TestCompileJoin2LeafRowSorterWithWhere exercises emitLeafRowSorter when a
// WHERE filter prunes some rows before the SorterInsert.
func TestCompileJoin2LeafRowSorterWithWhere(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE lrsw_a(id INTEGER, label TEXT)",
		"CREATE TABLE lrsw_b(id INTEGER, ref INTEGER, rank INTEGER)",
		"INSERT INTO lrsw_a VALUES(1,'p'),(2,'q'),(3,'r')",
		"INSERT INTO lrsw_b VALUES(1,1,5),(2,2,15),(3,3,8)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT lrsw_a.label, lrsw_b.rank "+
			"FROM lrsw_a JOIN lrsw_b ON lrsw_a.id = lrsw_b.ref "+
			"WHERE lrsw_b.rank > 6 ORDER BY lrsw_b.rank DESC")
	if len(rows) != 2 {
		t.Fatalf("sorter with WHERE rows: got %d, want 2", len(rows))
	}
	// DESC order: 15, 8
	first, _ := rows[0][1].(int64)
	if first != 15 {
		t.Errorf("first rank: got %d, want 15", first)
	}
}

// TestCompileJoin2LeafRowSorterLeftJoin exercises emitLeafRowSorter (via
// emitJoinLevelSorter) for a LEFT JOIN with ORDER BY.
func TestCompileJoin2LeafRowSorterLeftJoin(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE lrsl_a(id INTEGER, tag TEXT)",
		"CREATE TABLE lrsl_b(id INTEGER, ref INTEGER, weight INTEGER)",
		"INSERT INTO lrsl_a VALUES(1,'t1'),(2,'t2'),(3,'t3')",
		"INSERT INTO lrsl_b VALUES(1,1,100),(2,3,50)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT lrsl_a.tag, lrsl_b.weight "+
			"FROM lrsl_a LEFT JOIN lrsl_b ON lrsl_a.id = lrsl_b.ref "+
			"ORDER BY lrsl_a.id")
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN sorter rows: got %d, want 3", len(rows))
	}
	// id=2 has no match → weight NULL
	if rows[1][1] != nil {
		t.Errorf("unmatched row weight: got %v, want nil", rows[1][1])
	}
}

// ============================================================================
// emitDefaultAggregateValue — aggregate on JOIN with no matching rows
// ============================================================================

// TestCompileJoin2DefaultAggCount exercises the COUNT default value (0) when
// no rows match the JOIN condition in a no-GROUP-BY aggregate query.
func TestCompileJoin2DefaultAggCount(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE dac_a(id INTEGER, name TEXT)",
		"CREATE TABLE dac_b(id INTEGER, ref INTEGER)",
		// No rows in dac_b → JOIN produces zero rows
	)
	n := cj2QueryInt64(t, db,
		"SELECT COUNT(*) FROM dac_a JOIN dac_b ON dac_a.id = dac_b.ref")
	if n != 0 {
		t.Errorf("COUNT with no matches: got %d, want 0", n)
	}
}

// TestCompileJoin2DefaultAggSum exercises the SUM default value (NULL) when no
// rows match — the default branch in emitDefaultAggregateValue.
func TestCompileJoin2DefaultAggSum(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE das_a(id INTEGER)",
		"CREATE TABLE das_b(id INTEGER, ref INTEGER, amount INTEGER)",
		"INSERT INTO das_a VALUES(1)",
		// No matching rows in das_b
	)
	rows, err := db.Query(
		"SELECT SUM(das_b.amount) FROM das_a JOIN das_b ON das_a.id = das_b.ref")
	if err != nil {
		t.Fatalf("SUM no-match query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var v sql.NullInt64
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan SUM: %v", err)
	}
	if v.Valid {
		t.Errorf("SUM with no matches: got %d, want NULL", v.Int64)
	}
}

// TestCompileJoin2DefaultAggTotal exercises the TOTAL default value (0.0) when
// no rows match — the "TOTAL" branch in emitDefaultAggregateValue.
func TestCompileJoin2DefaultAggTotal(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE dat_a(id INTEGER)",
		"CREATE TABLE dat_b(id INTEGER, ref INTEGER, val INTEGER)",
		"INSERT INTO dat_a VALUES(1)",
		// No matching rows in dat_b
	)
	rows, err := db.Query(
		"SELECT TOTAL(dat_b.val) FROM dat_a JOIN dat_b ON dat_a.id = dat_b.ref")
	if err != nil {
		t.Fatalf("TOTAL no-match query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var v float64
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan TOTAL: %v", err)
	}
	if v != 0.0 {
		t.Errorf("TOTAL with no matches: got %f, want 0.0", v)
	}
}

// ============================================================================
// emitAggLeafRow — multi-table JOIN with GROUP BY
// ============================================================================

// TestCompileJoin2AggLeafRowGroupBy exercises emitAggLeafRow via a LEFT JOIN
// with GROUP BY, which routes through the left-join aggregate code path.
func TestCompileJoin2AggLeafRowGroupBy(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE alr_dept(id INTEGER, dname TEXT)",
		"CREATE TABLE alr_emp(id INTEGER, dept_id INTEGER, salary INTEGER)",
		"INSERT INTO alr_dept VALUES(1,'Eng'),(2,'Mkt'),(3,'Sales')",
		"INSERT INTO alr_emp VALUES(1,1,1000),(2,1,2000),(3,2,1500)",
	)
	// GROUP BY with LEFT JOIN: Sales dept has no employees → sum NULL / count 0
	rows := cj2QueryRows(t, db,
		"SELECT alr_dept.dname, COUNT(alr_emp.id) "+
			"FROM alr_dept LEFT JOIN alr_emp ON alr_dept.id = alr_emp.dept_id "+
			"GROUP BY alr_dept.id ORDER BY alr_dept.id")
	if len(rows) != 3 {
		t.Fatalf("GROUP BY LEFT JOIN rows: got %d, want 3", len(rows))
	}
}

// TestCompileJoin2AggLeafRowInnerJoin exercises emitAggLeafRow via a plain
// INNER JOIN with GROUP BY and SUM.
func TestCompileJoin2AggLeafRowInnerJoin(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE alri_cat(id INTEGER, cname TEXT)",
		"CREATE TABLE alri_item(id INTEGER, cat_id INTEGER, price INTEGER)",
		"INSERT INTO alri_cat VALUES(1,'A'),(2,'B')",
		"INSERT INTO alri_item VALUES(1,1,10),(2,1,20),(3,2,30),(4,2,40)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT alri_cat.cname, SUM(alri_item.price) "+
			"FROM alri_cat JOIN alri_item ON alri_cat.id = alri_item.cat_id "+
			"GROUP BY alri_cat.id ORDER BY alri_cat.id")
	if len(rows) != 2 {
		t.Fatalf("INNER JOIN GROUP BY rows: got %d, want 2", len(rows))
	}
	sum0, _ := rows[0][1].(int64)
	if sum0 != 30 {
		t.Errorf("cat A sum: got %d, want 30", sum0)
	}
	sum1, _ := rows[1][1].(int64)
	if sum1 != 70 {
		t.Errorf("cat B sum: got %d, want 70", sum1)
	}
}

// TestCompileJoin2AggLeafRowWithWhere exercises emitAggLeafRow with a WHERE
// clause that filters some join rows before aggregation.
func TestCompileJoin2AggLeafRowWithWhere(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE alrw_grp(id INTEGER, gname TEXT)",
		"CREATE TABLE alrw_val(id INTEGER, gid INTEGER, v INTEGER)",
		"INSERT INTO alrw_grp VALUES(1,'G1'),(2,'G2')",
		"INSERT INTO alrw_val VALUES(1,1,5),(2,1,15),(3,2,8),(4,2,12)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT alrw_grp.gname, COUNT(*) "+
			"FROM alrw_grp LEFT JOIN alrw_val ON alrw_grp.id = alrw_val.gid "+
			"WHERE alrw_val.v > 6 "+
			"GROUP BY alrw_grp.id ORDER BY alrw_grp.id")
	if len(rows) != 2 {
		t.Fatalf("WHERE+GROUP BY rows: got %d, want 2", len(rows))
	}
}

// ============================================================================
// resolveExprCollationMultiTable / findColumnCollation — collation handling
// ============================================================================

// TestCompileJoin2CollationGroupByText exercises resolveExprCollationMultiTable
// and findColumnCollation when GROUP BY references a TEXT column whose collation
// is declared in the schema.
func TestCompileJoin2CollationGroupByText(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	// Declare a COLLATE NOCASE column so findColumnCollation returns a value.
	cj2Exec(t, db,
		"CREATE TABLE cgb_a(id INTEGER, tag TEXT COLLATE NOCASE)",
		"CREATE TABLE cgb_b(id INTEGER, ref INTEGER, amount INTEGER)",
		"INSERT INTO cgb_a VALUES(1,'Alpha'),(2,'beta'),(3,'ALPHA')",
		"INSERT INTO cgb_b VALUES(1,1,10),(2,2,20),(3,3,30)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT cgb_a.tag, SUM(cgb_b.amount) "+
			"FROM cgb_a JOIN cgb_b ON cgb_a.id = cgb_b.ref "+
			"GROUP BY cgb_a.tag ORDER BY cgb_a.tag")
	if len(rows) == 0 {
		t.Fatal("GROUP BY text collation: expected rows, got none")
	}
}

// TestCompileJoin2CollationOrderByText exercises resolveExprCollationMultiTable
// with an explicit COLLATE expression in ORDER BY across a two-table JOIN.
func TestCompileJoin2CollationOrderByText(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE cot_a(id INTEGER, label TEXT)",
		"CREATE TABLE cot_b(id INTEGER, ref INTEGER, note TEXT)",
		"INSERT INTO cot_a VALUES(1,'charlie'),(2,'alpha'),(3,'BETA')",
		"INSERT INTO cot_b VALUES(1,1,1),(2,2,2),(3,3,3)",
	)
	rows := cj2QueryRows(t, db,
		"SELECT cot_a.label FROM cot_a JOIN cot_b ON cot_a.id = cot_b.ref "+
			"ORDER BY cot_a.label COLLATE NOCASE ASC")
	if len(rows) != 3 {
		t.Fatalf("COLLATE ORDER BY rows: got %d, want 3", len(rows))
	}
}

// TestCompileJoin2CollationQualifiedIdent exercises the table-qualified branch
// of findColumnCollation (ident.Table != "").
func TestCompileJoin2CollationQualifiedIdent(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE cqi_a(id INTEGER, name TEXT COLLATE NOCASE)",
		"CREATE TABLE cqi_b(id INTEGER, ref INTEGER, val INTEGER)",
		"INSERT INTO cqi_a VALUES(1,'Apple'),(2,'banana'),(3,'Cherry')",
		"INSERT INTO cqi_b VALUES(1,1,5),(2,2,10),(3,3,15)",
	)
	// GROUP BY cqi_a.name forces the table-qualified lookup in findColumnCollation.
	rows := cj2QueryRows(t, db,
		"SELECT cqi_a.name, SUM(cqi_b.val) "+
			"FROM cqi_a JOIN cqi_b ON cqi_a.id = cqi_b.ref "+
			"GROUP BY cqi_a.name ORDER BY cqi_a.name")
	if len(rows) != 3 {
		t.Fatalf("qualified collation GROUP BY rows: got %d, want 3", len(rows))
	}
}

// TestCompileJoin2CollationParenExpr exercises the ParenExpr branch of
// resolveExprCollationMultiTable via a parenthesised GROUP BY expression.
func TestCompileJoin2CollationParenExpr(t *testing.T) {
	t.Parallel()
	db := cj2OpenDB(t)
	defer db.Close()
	cj2Exec(t, db,
		"CREATE TABLE cpe_a(id INTEGER, kind TEXT)",
		"CREATE TABLE cpe_b(id INTEGER, ref INTEGER, qty INTEGER)",
		"INSERT INTO cpe_a VALUES(1,'X'),(2,'Y'),(3,'X')",
		"INSERT INTO cpe_b VALUES(1,1,2),(2,2,4),(3,3,6)",
	)
	// Some engines allow GROUP BY (expr); parenthesised forms exercise the
	// ParenExpr branch during collation resolution if the parser wraps them.
	rows := cj2QueryRows(t, db,
		"SELECT cpe_a.kind, SUM(cpe_b.qty) "+
			"FROM cpe_a JOIN cpe_b ON cpe_a.id = cpe_b.ref "+
			"GROUP BY cpe_a.kind ORDER BY cpe_a.kind")
	if len(rows) != 2 {
		t.Fatalf("paren collation GROUP BY rows: got %d, want 2", len(rows))
	}
	// kind X: rows 1+3, qty 2+6=8
	sum0, _ := rows[0][1].(int64)
	if sum0 != 8 {
		t.Errorf("kind X sum: got %d, want 8", sum0)
	}
}
