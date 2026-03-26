// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"testing"
)

// ============================================================================
// resolveUsingJoin — USING clause join handling
// ============================================================================

// TestCSJ2_UsingJoinBasic exercises resolveUsingJoin with a single shared column.
func TestCompileSelectJoin2Coverage_UsingJoinBasic(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_u1(id INTEGER, val TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_u2(id INTEGER, info TEXT)")
	cscExec(t, db, "INSERT INTO csj2_u1 VALUES(1,'a'),(2,'b'),(3,'c')")
	cscExec(t, db, "INSERT INTO csj2_u2 VALUES(1,'x'),(3,'z')")
	n := csInt(t, db, "SELECT COUNT(*) FROM csj2_u1 JOIN csj2_u2 USING(id)")
	if n != 2 {
		t.Errorf("USING join: want 2 rows, got %d", n)
	}
}

// TestCSJ2_UsingJoinMultiCol exercises resolveUsingJoin with two shared columns,
// hitting the equality-chain concatenation path.
func TestCompileSelectJoin2Coverage_UsingJoinMultiCol(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_um1(id INTEGER, cat INTEGER, v TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_um2(id INTEGER, cat INTEGER, w TEXT)")
	cscExec(t, db, "INSERT INTO csj2_um1 VALUES(1,10,'a'),(2,20,'b'),(3,10,'c')")
	cscExec(t, db, "INSERT INTO csj2_um2 VALUES(1,10,'x'),(2,99,'y'),(3,10,'z')")
	n := csInt(t, db, "SELECT COUNT(*) FROM csj2_um1 JOIN csj2_um2 USING(id,cat)")
	if n != 2 {
		t.Errorf("USING(id,cat): want 2, got %d", n)
	}
}

// TestCSJ2_UsingJoinWithOrderBy exercises resolveUsingJoin together with an
// ORDER BY clause, triggering emitLeafRowSorter via the USING ON-condition.
func TestCompileSelectJoin2Coverage_UsingJoinWithOrderBy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_uo1(id INTEGER, name TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_uo2(id INTEGER, score INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_uo1 VALUES(1,'a'),(2,'b'),(3,'c')")
	cscExec(t, db, "INSERT INTO csj2_uo2 VALUES(1,30),(2,10),(3,20)")
	rows := queryCSRows(t, db,
		"SELECT csj2_uo1.name, csj2_uo2.score FROM csj2_uo1 JOIN csj2_uo2 USING(id) ORDER BY csj2_uo2.score ASC")
	if len(rows) != 3 {
		t.Fatalf("USING+ORDER BY: want 3 rows, got %d", len(rows))
	}
	first, _ := rows[0][1].(int64)
	if first != 10 {
		t.Errorf("first score want 10, got %d", first)
	}
}

// ============================================================================
// findColumnTableIndex — column-table disambiguation in multi-table JOIN
// ============================================================================

// TestCSJ2_FindColumnTableIndex_LeftJoin exercises the null-emission path in a
// LEFT JOIN with ORDER BY, which calls findColumnTableIndex to decide whether a
// column comes from the null-extended right table.
func TestCompileSelectJoin2Coverage_FindColumnTableIndex_LeftJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fl(id INTEGER, label TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_fr(id INTEGER, ref INTEGER, extra TEXT)")
	cscExec(t, db, "INSERT INTO csj2_fl VALUES(1,'one'),(2,'two'),(3,'three')")
	cscExec(t, db, "INSERT INTO csj2_fr VALUES(10,1,'r1'),(11,3,'r3')")
	rows := queryCSRows(t, db,
		"SELECT csj2_fl.label, csj2_fr.extra FROM csj2_fl LEFT JOIN csj2_fr ON csj2_fl.id = csj2_fr.ref ORDER BY csj2_fl.id")
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN findColumnTableIndex: want 3 rows, got %d", len(rows))
	}
	// id=2 has no match; extra must be NULL
	if rows[1][1] != nil {
		t.Errorf("unmatched row extra: want nil, got %v", rows[1][1])
	}
}

// TestCSJ2_FindColumnTableIndex_QualifiedExpr exercises the table-qualified
// branch of findColumnTableIndex when SELECT lists reference t.col syntax.
func TestCompileSelectJoin2Coverage_FindColumnTableIndex_QualifiedExpr(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_qa(id INTEGER, name TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_qb(id INTEGER, ref INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_qa VALUES(1,'p'),(2,'q'),(3,'r')")
	cscExec(t, db, "INSERT INTO csj2_qb VALUES(1,1,9),(2,3,4)")
	rows := queryCSRows(t, db,
		"SELECT csj2_qa.name, csj2_qb.val FROM csj2_qa LEFT JOIN csj2_qb ON csj2_qa.id = csj2_qb.ref ORDER BY csj2_qa.id")
	if len(rows) != 3 {
		t.Fatalf("qualified LEFT JOIN: want 3 rows, got %d", len(rows))
	}
	if rows[1][1] != nil {
		t.Errorf("row 2 val: want nil, got %v", rows[1][1])
	}
}

// ============================================================================
// emitLeafRowSorter — JOIN result sorting
// ============================================================================

// TestCSJ2_LeafRowSorter_InnerJoin exercises emitLeafRowSorter for an INNER
// JOIN with ORDER BY on a non-index column.
func TestCompileSelectJoin2Coverage_LeafRowSorter_InnerJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_sa(id INTEGER, tag TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_sb(id INTEGER, ref INTEGER, prio INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_sa VALUES(1,'x'),(2,'y'),(3,'z')")
	cscExec(t, db, "INSERT INTO csj2_sb VALUES(10,1,30),(11,2,10),(12,3,20)")
	rows := queryCSRows(t, db,
		"SELECT csj2_sa.tag, csj2_sb.prio FROM csj2_sa JOIN csj2_sb ON csj2_sa.id = csj2_sb.ref ORDER BY csj2_sb.prio")
	if len(rows) != 3 {
		t.Fatalf("sorter inner join: want 3, got %d", len(rows))
	}
	got, _ := rows[0][1].(int64)
	if got != 10 {
		t.Errorf("first prio want 10, got %d", got)
	}
}

// TestCSJ2_LeafRowSorter_WithWhere exercises emitLeafRowSorter when a WHERE
// clause prunes some rows before SorterInsert.
func TestCompileSelectJoin2Coverage_LeafRowSorter_WithWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_swa(id INTEGER, label TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_swb(id INTEGER, ref INTEGER, rank INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_swa VALUES(1,'a'),(2,'b'),(3,'c')")
	cscExec(t, db, "INSERT INTO csj2_swb VALUES(1,1,5),(2,2,15),(3,3,8)")
	rows := queryCSRows(t, db,
		"SELECT csj2_swa.label, csj2_swb.rank FROM csj2_swa JOIN csj2_swb ON csj2_swa.id = csj2_swb.ref WHERE csj2_swb.rank > 6 ORDER BY csj2_swb.rank DESC")
	if len(rows) != 2 {
		t.Fatalf("sorter with WHERE: want 2, got %d", len(rows))
	}
	first, _ := rows[0][1].(int64)
	if first != 15 {
		t.Errorf("first rank want 15, got %d", first)
	}
}

// TestCSJ2_LeafRowSorter_LeftJoin exercises emitLeafRowSorter for a LEFT JOIN
// with ORDER BY — both the matched and null-emission sub-paths are taken.
func TestCompileSelectJoin2Coverage_LeafRowSorter_LeftJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_sla(id INTEGER, tag TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_slb(id INTEGER, ref INTEGER, weight INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_sla VALUES(1,'t1'),(2,'t2'),(3,'t3')")
	cscExec(t, db, "INSERT INTO csj2_slb VALUES(1,1,100),(2,3,50)")
	rows := queryCSRows(t, db,
		"SELECT csj2_sla.tag, csj2_slb.weight FROM csj2_sla LEFT JOIN csj2_slb ON csj2_sla.id = csj2_slb.ref ORDER BY csj2_sla.id")
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN sorter: want 3, got %d", len(rows))
	}
	if rows[1][1] != nil {
		t.Errorf("unmatched row weight: want nil, got %v", rows[1][1])
	}
}

// ============================================================================
// emitExtraOrderByColumnMultiTable — multi-table ORDER BY extra column emission
// ============================================================================

// TestCSJ2_ExtraOrderByColumnMultiTable exercises the multi-table ORDER BY path
// where an ORDER BY column is not in the SELECT list (extra sort key).
func TestCompileSelectJoin2Coverage_ExtraOrderByColumnMultiTable(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_oa(id INTEGER, name TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_ob(id INTEGER, ref INTEGER, score INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_oa VALUES(1,'alpha'),(2,'beta'),(3,'gamma')")
	cscExec(t, db, "INSERT INTO csj2_ob VALUES(1,1,50),(2,2,10),(3,3,30)")
	// ORDER BY score which is not in the SELECT list forces emitExtraOrderByColumnMultiTable
	rows := queryCSRows(t, db,
		"SELECT csj2_oa.name FROM csj2_oa JOIN csj2_ob ON csj2_oa.id = csj2_ob.ref ORDER BY csj2_ob.score")
	if len(rows) != 3 {
		t.Fatalf("extra ORDER BY multi-table: want 3, got %d", len(rows))
	}
	first, _ := rows[0][0].(string)
	if first != "beta" {
		t.Errorf("first name want 'beta', got %q", first)
	}
}

// TestCSJ2_ExtraOrderByColumnNotFound exercises the "column not found" fallback
// that emits OpNull when the ORDER BY column cannot be resolved.
func TestCompileSelectJoin2Coverage_ExtraOrderByColumnNotFound(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_onf_a(id INTEGER, val TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_onf_b(id INTEGER, ref INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_onf_a VALUES(1,'x'),(2,'y')")
	cscExec(t, db, "INSERT INTO csj2_onf_b VALUES(1,1),(2,2)")
	// ORDER BY id which exists in both tables — engine resolves to one of them
	rows := queryCSRows(t, db,
		"SELECT csj2_onf_a.val FROM csj2_onf_a JOIN csj2_onf_b ON csj2_onf_a.id = csj2_onf_b.ref ORDER BY csj2_onf_a.id DESC")
	if len(rows) != 2 {
		t.Fatalf("ORDER BY existing col: want 2, got %d", len(rows))
	}
}

// ============================================================================
// extractOrderByExpression — ORDER BY expression extraction
// ============================================================================

// TestCSJ2_ExtractOrderByExpression_Collate exercises the COLLATE branch of
// extractOrderByExpression (CollateExpr unwrapping).
func TestCompileSelectJoin2Coverage_ExtractOrderByExpression_Collate(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_obe_a(id INTEGER, tag TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_obe_b(id INTEGER, ref INTEGER, note TEXT)")
	cscExec(t, db, "INSERT INTO csj2_obe_a VALUES(1,'Charlie'),(2,'alpha'),(3,'BETA')")
	cscExec(t, db, "INSERT INTO csj2_obe_b VALUES(1,1,1),(2,2,2),(3,3,3)")
	// ORDER BY with COLLATE exercises extractOrderByExpression's CollateExpr branch
	rows := queryCSRows(t, db,
		"SELECT csj2_obe_a.tag FROM csj2_obe_a JOIN csj2_obe_b ON csj2_obe_a.id = csj2_obe_b.ref ORDER BY csj2_obe_a.tag COLLATE NOCASE")
	if len(rows) != 3 {
		t.Fatalf("COLLATE ORDER BY: want 3, got %d", len(rows))
	}
}

// TestCSJ2_ExtractOrderByExpression_Plain exercises extractOrderByExpression
// without a COLLATE wrapper (plain expression path).
func TestCompileSelectJoin2Coverage_ExtractOrderByExpression_Plain(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_obp_a(id INTEGER, name TEXT)")
	cscExec(t, db, "CREATE TABLE csj2_obp_b(id INTEGER, ref INTEGER, rank INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_obp_a VALUES(1,'a'),(2,'b'),(3,'c')")
	cscExec(t, db, "INSERT INTO csj2_obp_b VALUES(1,1,3),(2,2,1),(3,3,2)")
	rows := queryCSRows(t, db,
		"SELECT csj2_obp_a.name FROM csj2_obp_a JOIN csj2_obp_b ON csj2_obp_a.id = csj2_obp_b.ref ORDER BY csj2_obp_b.rank ASC")
	if len(rows) != 3 {
		t.Fatalf("plain ORDER BY: want 3, got %d", len(rows))
	}
	first, _ := rows[0][0].(string)
	if first != "b" {
		t.Errorf("first name want 'b', got %q", first)
	}
}

// ============================================================================
// findColumnIndex — column index resolution (compile_select_agg.go)
// ============================================================================

// TestCSJ2_FindColumnIndex_CaseInsensitive exercises the case-insensitive
// fallback branch in findColumnIndex via a window function query where the
// column lookup may be needed for sorter path.
func TestCompileSelectJoin2Coverage_FindColumnIndex_CaseInsensitive(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fci(id INTEGER, Name TEXT, Score INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fci VALUES(1,'alice',80),(2,'bob',90),(3,'carol',70)")
	// window function with ORDER BY exercises compileWindowWithSorting which
	// calls emitSorterColumnValue -> findColumnIndex for column-name lookup.
	rows := queryCSRows(t, db,
		"SELECT Name, Score, ROW_NUMBER() OVER (ORDER BY Score DESC) AS rn FROM csj2_fci ORDER BY Score DESC")
	if len(rows) != 3 {
		t.Fatalf("window findColumnIndex: want 3 rows, got %d", len(rows))
	}
}

// TestCSJ2_FindColumnIndex_NotFound exercises the -1 return path in findColumnIndex
// by querying an aggregate on a table with aliased columns.
func TestCompileSelectJoin2Coverage_FindColumnIndex_NotFound(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fnf(x INTEGER, y INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fnf VALUES(1,10),(2,20),(3,30)")
	// window rank on a non-existent column name forces the not-found path
	rows := queryCSRows(t, db,
		"SELECT x, y, RANK() OVER (ORDER BY y) AS rk FROM csj2_fnf ORDER BY y")
	if len(rows) != 3 {
		t.Fatalf("findColumnIndex not-found: want 3 rows, got %d", len(rows))
	}
}

// ============================================================================
// fromTableAlias — table alias resolution
// ============================================================================

// TestCSJ2_FromTableAlias_WithAlias exercises the alias branch of fromTableAlias
// by using an aliased table in an aggregate query.
func TestCompileSelectJoin2Coverage_FromTableAlias_WithAlias(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fta(id INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fta VALUES(1,10),(2,20),(3,30)")
	// Use a table alias — fromTableAlias returns the alias which is then
	// registered as an additional alias in registerAggTableInfo.
	n := csInt(t, db, "SELECT SUM(t.val) FROM csj2_fta AS t")
	if n != 60 {
		t.Errorf("SUM with alias: want 60, got %d", n)
	}
}

// TestCSJ2_FromTableAlias_NoAlias exercises the no-alias branch (alias == "").
func TestCompileSelectJoin2Coverage_FromTableAlias_NoAlias(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fna(id INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fna VALUES(1,5),(2,15)")
	n := csInt(t, db, "SELECT COUNT(*) FROM csj2_fna")
	if n != 2 {
		t.Errorf("COUNT no alias: want 2, got %d", n)
	}
}

// TestCSJ2_FromTableAlias_SameAsTable exercises the branch where alias equals
// the table name (alias != "" but alias == tableName so no duplicate is added).
func TestCompileSelectJoin2Coverage_FromTableAlias_SameAsTable(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fsat(id INTEGER, n INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fsat VALUES(1,7),(2,3)")
	n := csInt(t, db, "SELECT MAX(csj2_fsat.n) FROM csj2_fsat AS csj2_fsat")
	if n != 7 {
		t.Errorf("MAX alias==tableName: want 7, got %d", n)
	}
}

// ============================================================================
// emitGeneratedExpr — generated expression emission
// ============================================================================

// TestCSJ2_EmitGeneratedExpr_BinaryExpr exercises emitGeneratedExpr via a
// window function query where a binary arithmetic expression appears in SELECT.
func TestCompileSelectJoin2Coverage_EmitGeneratedExpr_BinaryExpr(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_ege(id INTEGER, a INTEGER, b INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_ege VALUES(1,3,4),(2,1,2),(3,5,1)")
	// a+b is a generated expression emitted via emitGeneratedExpr in the
	// compileWindowWithSorting / emitWindowColumn path.
	rows := queryCSRows(t, db,
		"SELECT a+b, ROW_NUMBER() OVER (ORDER BY a+b) AS rn FROM csj2_ege ORDER BY a+b")
	if len(rows) != 3 {
		t.Fatalf("emitGeneratedExpr binary: want 3, got %d", len(rows))
	}
	// Verify all a+b values are non-zero (3+4=7, 1+2=3, 5+1=6)
	for i, row := range rows {
		v, _ := row[0].(int64)
		if v <= 0 {
			t.Errorf("row %d: a+b want positive, got %d", i, v)
		}
	}
}

// TestCSJ2_EmitGeneratedExpr_FunctionExpr exercises emitGeneratedExpr with a
// scalar function call expression in the SELECT list of a window query.
func TestCompileSelectJoin2Coverage_EmitGeneratedExpr_FunctionExpr(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_egf(id INTEGER, s TEXT)")
	cscExec(t, db, "INSERT INTO csj2_egf VALUES(1,'hello'),(2,'world'),(3,'foo')")
	rows := queryCSRows(t, db,
		"SELECT LENGTH(s), RANK() OVER (ORDER BY LENGTH(s) DESC) AS rk FROM csj2_egf ORDER BY LENGTH(s) DESC")
	if len(rows) != 3 {
		t.Fatalf("emitGeneratedExpr func: want 3, got %d", len(rows))
	}
}

// ============================================================================
// adjustCursorOpRegisters — CTE cursor register adjustment
// ============================================================================

// TestCSJ2_AdjustCursorOpRegisters_BasicCTE exercises adjustCursorOpRegisters
// via a non-recursive CTE that requires cursor-register offset adjustment when
// its bytecode is inlined into the main query.
func TestCompileSelectJoin2Coverage_AdjustCursorOpRegisters_BasicCTE(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_acr(id INTEGER, val INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_acr VALUES(1,100),(2,200),(3,300)")
	n := csInt(t, db,
		`WITH summary AS (SELECT SUM(val) AS total FROM csj2_acr)
		 SELECT total FROM summary`)
	if n != 600 {
		t.Errorf("CTE cursor adjust: want 600, got %d", n)
	}
}

// TestCSJ2_AdjustCursorOpRegisters_MultipleCTEs exercises adjustCursorOpRegisters
// with two chained CTEs, each requiring its own register-offset adjustment.
func TestCompileSelectJoin2Coverage_AdjustCursorOpRegisters_MultipleCTEs(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_amc(id INTEGER, n INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_amc VALUES(1,5),(2,10),(3,15)")
	n := csInt(t, db,
		`WITH a AS (SELECT SUM(n) AS s FROM csj2_amc),
		      b AS (SELECT s * 2 AS d FROM a)
		 SELECT d FROM b`)
	if n != 60 {
		t.Errorf("chained CTEs cursor adjust: want 60, got %d", n)
	}
}

// ============================================================================
// fixInnerRewindAddresses — recursive CTE inner loop fix
// ============================================================================

// TestCSJ2_FixInnerRewindAddresses_Counter exercises fixInnerRewindAddresses via
// a recursive CTE counter, which produces Rewind instructions with P2=0 in the
// inner loop that must be patched by fixInnerRewindAddresses.
func TestCompileSelectJoin2Coverage_FixInnerRewindAddresses_Counter(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	rows := queryCSRows(t, db,
		`WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 5
		) SELECT x FROM cnt`)
	if len(rows) != 5 {
		t.Fatalf("recursive CTE inner rewind: want 5 rows, got %d", len(rows))
	}
	last, _ := rows[4][0].(int64)
	if last != 5 {
		t.Errorf("last value want 5, got %d", last)
	}
}

// TestCSJ2_FixInnerRewindAddresses_JoinedRecursive exercises fixInnerRewindAddresses
// when the recursive CTE references a real table in the recursive term, producing
// an inner loop Rewind that requires patching.
func TestCompileSelectJoin2Coverage_FixInnerRewindAddresses_JoinedRecursive(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE csj2_fir(id INTEGER, parent INTEGER)")
	cscExec(t, db, "INSERT INTO csj2_fir VALUES(1,0),(2,1),(3,1),(4,2)")
	rows := queryCSRows(t, db,
		`WITH RECURSIVE tree(id, depth) AS (
			SELECT id, 0 FROM csj2_fir WHERE parent = 0
			UNION ALL
			SELECT c.id, t.depth + 1 FROM csj2_fir c JOIN tree t ON c.parent = t.id
		) SELECT id, depth FROM tree ORDER BY id`)
	if len(rows) == 0 {
		t.Fatal("recursive CTE with real table join: expected rows")
	}
}

// TestCSJ2_FixInnerRewindAddresses_Fibonacci exercises fixInnerRewindAddresses
// with a multi-column recursive CTE (Fibonacci), stressing the Rewind-patching
// logic across a wider program.
func TestCompileSelectJoin2Coverage_FixInnerRewindAddresses_Fibonacci(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	n := csInt(t, db,
		`WITH RECURSIVE fib(a, b) AS (
			SELECT 0, 1
			UNION ALL
			SELECT b, a+b FROM fib WHERE a < 50
		) SELECT MAX(a) FROM fib`)
	if n != 55 {
		t.Errorf("Fibonacci max: want 55, got %d", n)
	}
}
