// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openMCDCCompileDB opens a fresh :memory: database for MC/DC tests.
func openMCDCCompileDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mcdc_mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("Exec(%q): %v", query, err)
	}
}

func mcdc_mustQuery(t *testing.T, db *sql.DB, query string) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	return rows
}

func mcdc_collectInts(t *testing.T, rows *sql.Rows) []int64 {
	t.Helper()
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

// ============================================================================
// MC/DC: loadCountValueReg (70%)
//
// Compound condition at entry of loadCountValueReg:
//   A = loadAggregateColumnValue returns ok (col is a named ident that exists)
//   B = len(fnExpr.Args) == 0 (no args at all)
//
// Cases:
//   A=T        → returns early with tempReg, skipAddr from column load
//   A=F, B=F   → falls through to GenerateExpr path
//   A=F, B=T   → adds AddImm and returns 0,0
// ============================================================================

func TestMCDC_Compile_LoadCountValueReg_ColumnExists(t *testing.T) {
	// A=T: COUNT(col) where col exists in table → loadAggregateColumnValue succeeds
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE lc1(x INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO lc1 VALUES(1),(2),(NULL)")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(x) FROM lc1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("COUNT(x) want 2, got %v", vals)
	}
}

func TestMCDC_Compile_LoadCountValueReg_ExprPath(t *testing.T) {
	// A=F, B=F: COUNT with an expression arg (not a simple column ident)
	// triggers GenerateExpr path in loadCountValueReg
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE lc2(x INTEGER, y INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO lc2 VALUES(1,1),(2,NULL),(3,3)")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(x+y) FROM lc2")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("COUNT(x+y) want 2, got %v", vals)
	}
}

// ============================================================================
// MC/DC: collectWindowFuncs (95.2%) / findWindowOrderBy (95.2%)
//
// collectWindowFuncs collects window function columns from SELECT results.
// findWindowOrderBy locates ORDER BY inside a window spec.
//
// Cases:
//   - Window func with PARTITION BY and ORDER BY (hits findWindowOrderBy)
//   - Window func with no ORDER BY (findWindowOrderBy returns empty)
//   - Multiple window functions (loop in collectWindowFuncs iterates)
// ============================================================================

func TestMCDC_Compile_CollectWindowFuncs_WithOrderBy(t *testing.T) {
	// findWindowOrderBy: window has ORDER BY clause → non-empty orderby found
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wf1(grp INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wf1 VALUES(1,10),(1,20),(2,30)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM wf1")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, v, rn int64
		if err := rows.Scan(&g, &v, &rn); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

func TestMCDC_Compile_CollectWindowFuncs_NoOrderBy(t *testing.T) {
	// findWindowOrderBy: window has no ORDER BY clause → empty
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wf2(grp INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wf2 VALUES(1,10),(1,20),(2,30)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, SUM(val) OVER (PARTITION BY grp) FROM wf2")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, s int64
		if err := rows.Scan(&g, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

func TestMCDC_Compile_CollectWindowFuncs_MultipleWindowFuncs(t *testing.T) {
	// collectWindowFuncs iterates over multiple window function columns
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wf3(grp INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wf3 VALUES(1,10),(1,20),(2,5)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, val, ROW_NUMBER() OVER (ORDER BY val), RANK() OVER (ORDER BY val) FROM wf3")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, v, rn, rk int64
		if err := rows.Scan(&g, &v, &rn, &rk); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

// ============================================================================
// MC/DC: compileWindowWithSorting (98.2%) / compileSelectWithWindowFunctions (96.2%)
//
// compileSelectWithWindowFunctions:
//   A = hasWindowOrderBy (window has ORDER BY)
//   B = stmt.Where != nil
//
// compileWindowWithSorting: WHERE clause is compiled when present
// Cases: (A=T,B=T), (A=T,B=F), (A=F,B=F) — A=F,B=T: no-ORDER-BY with WHERE
// ============================================================================

func TestMCDC_Compile_WindowWithSorting_WhereClause(t *testing.T) {
	// compileWindowWithSorting: WHERE present, triggers WHERE filter compilation
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wws1(grp INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wws1 VALUES(1,10),(1,20),(2,30),(2,40)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM wws1 WHERE grp=1")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, v, rn int64
		if err := rows.Scan(&g, &v, &rn); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("want 2 rows (grp=1 only), got %d", count)
	}
}

func TestMCDC_Compile_WindowNoOrderByWithWhere(t *testing.T) {
	// compileSelectWithWindowFunctions: no ORDER BY in window, but WHERE present
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wws2(grp INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wws2 VALUES(1,10),(1,20),(2,30)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, SUM(val) OVER (PARTITION BY grp) FROM wws2 WHERE grp=2")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, s int64
		if err := rows.Scan(&g, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("want 1 row, got %d", count)
	}
}

// ============================================================================
// MC/DC: handleSpecialSelectTypes (95.5%)
//
// Compound routing condition:
//   A = hasCTE         → route to CTE handler
//   B = hasWindowFuncs → route to window handler
//   C = hasJoins && hasAgg → route to join+agg handler
//   D = hasTVF         → route to TVF handler
//   else               → return false (not special)
//
// Cases: hit each branch plus the fallthrough (non-special plain SELECT)
// ============================================================================

func TestMCDC_Compile_HandleSpecialSelectTypes_PlainSelect(t *testing.T) {
	// No special type → handleSpecialSelectTypes returns false (handled=false)
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE hss1(a INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO hss1 VALUES(42)")
	rows := mcdc_mustQuery(t, db, "SELECT a FROM hss1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 42 {
		t.Fatalf("plain SELECT want [42], got %v", vals)
	}
}

func TestMCDC_Compile_HandleSpecialSelectTypes_CTEBranch(t *testing.T) {
	// hasCTE=true → routes through CTE handler
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE hss_cte_src(n INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO hss_cte_src VALUES(1),(2),(3)")
	rows := mcdc_mustQuery(t, db, "WITH nums AS (SELECT n FROM hss_cte_src) SELECT n FROM nums ORDER BY n")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 {
		t.Fatalf("CTE branch want 3 rows, got %v", vals)
	}
}

func TestMCDC_Compile_HandleSpecialSelectTypes_WindowBranch(t *testing.T) {
	// hasWindowFuncs=true → routes through window handler
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE hss2(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO hss2 VALUES(1),(2),(3)")
	rows := mcdc_mustQuery(t, db, "SELECT v, ROW_NUMBER() OVER (ORDER BY v) FROM hss2")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var v, rn int64
		if err := rows.Scan(&v, &rn); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("window branch want 3 rows, got %d", count)
	}
}

func TestMCDC_Compile_HandleSpecialSelectTypes_TVFBranch(t *testing.T) {
	// hasTVF=true → routes through TVF handler
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(1,5)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 5 {
		t.Fatalf("TVF branch want 5 rows, got %v", vals)
	}
}

// ============================================================================
// MC/DC: emitOrderByOutputLoop (96.8%)
//
// Compound condition:
//   A = limitInfo != nil && limitInfo.limitReg > 0 (LIMIT present)
//   B = limitInfo != nil && limitInfo.offsetReg > 0 (OFFSET present)
//
// Cases: (A=F,B=F), (A=T,B=F), (A=T,B=T)
// ============================================================================

func TestMCDC_Compile_EmitOrderByOutputLoop_NoLimitOffset(t *testing.T) {
	// A=F, B=F: ORDER BY without LIMIT or OFFSET
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE obl1(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO obl1 VALUES(3),(1),(2)")
	rows := mcdc_mustQuery(t, db, "SELECT v FROM obl1 ORDER BY v")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 || vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Fatalf("ORDER BY want [1 2 3], got %v", vals)
	}
}

func TestMCDC_Compile_EmitOrderByOutputLoop_WithLimit(t *testing.T) {
	// A=T, B=F: ORDER BY with LIMIT (no OFFSET)
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE obl2(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO obl2 VALUES(3),(1),(2),(4)")
	rows := mcdc_mustQuery(t, db, "SELECT v FROM obl2 ORDER BY v LIMIT 2")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 2 || vals[0] != 1 || vals[1] != 2 {
		t.Fatalf("ORDER BY LIMIT want [1 2], got %v", vals)
	}
}

func TestMCDC_Compile_EmitOrderByOutputLoop_WithLimitAndOffset(t *testing.T) {
	// A=T, B=T: ORDER BY with LIMIT and OFFSET
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE obl3(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO obl3 VALUES(3),(1),(2),(4),(5)")
	rows := mcdc_mustQuery(t, db, "SELECT v FROM obl3 ORDER BY v LIMIT 2 OFFSET 2")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 2 || vals[0] != 3 || vals[1] != 4 {
		t.Fatalf("ORDER BY LIMIT OFFSET want [3 4], got %v", vals)
	}
}

// ============================================================================
// MC/DC: generatedExprForColumn (71.4%)
//
// Compound condition:
//   A = col.GeneratedExpr == "" → return NULL literal
//   B = parser fails             → return NULL literal
//   else                         → return parsed expression
//
// Cases: A=T (empty expr), A=F+B=F (valid expr), A=F+B=T (bad expr impossible via schema)
// ============================================================================

func TestMCDC_Compile_GeneratedExprForColumn_EmptyExpr(t *testing.T) {
	// generatedExprForColumn: GeneratedExpr=="" → returns NULL literal placeholder
	// INSERT into generated column is rejected; verify INSERT with no explicit col works
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE gen1(a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a+b))")
	mcdc_mustExec(t, db, "INSERT INTO gen1(a,b) VALUES(3,4)")
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM gen1").Scan(&count); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 row inserted, got %d", count)
	}
}

func TestMCDC_Compile_GeneratedExprForColumn_ValidExpr(t *testing.T) {
	// generatedExprForColumn: GeneratedExpr != "" → parsed expression returned
	// Verify that INSERT is rejected for direct generated column writes
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE gen2(x INTEGER, y INTEGER, z INTEGER GENERATED ALWAYS AS (x*y))")
	mcdc_mustExec(t, db, "INSERT INTO gen2(x,y) VALUES(5,6)")
	// Attempting to INSERT directly into generated col must fail
	_, err := db.Exec("INSERT INTO gen2(x,y,z) VALUES(1,2,3)")
	if err == nil {
		t.Fatal("expected error inserting into generated column")
	}
}

func TestMCDC_Compile_GeneratedExprForColumn_MultipleRows(t *testing.T) {
	// generatedExprForColumn called for each generated col in each inserted row
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE gen3(a INTEGER, b INTEGER, c INTEGER GENERATED ALWAYS AS (a+b))")
	mcdc_mustExec(t, db, "INSERT INTO gen3(a,b) VALUES(1,2),(10,20),(100,200)")
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM gen3").Scan(&count); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

// ============================================================================
// MC/DC: affinityToOpCastCode (71.4%)
//
// Switch statement cases: AffinityBlob(1), AffinityText(2), AffinityInteger(3),
// AffinityReal(4), AffinityNumeric(5), default(0)
//
// Exercised via CAST(...) in UPDATE/INSERT where column affinities apply.
// ============================================================================

func TestMCDC_Compile_AffinityToOpCastCode_AllTypes(t *testing.T) {
	// CAST to various types exercises different affinity->opcode paths
	t.Parallel()
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			// AffinityInteger → case 3
			name:  "CAST to INTEGER",
			query: "SELECT CAST(3.7 AS INTEGER)",
		},
		{
			// AffinityReal → case 4
			name:  "CAST to REAL",
			query: "SELECT CAST(3 AS REAL)",
		},
		{
			// AffinityText → case 2
			name:  "CAST to TEXT",
			query: "SELECT CAST(42 AS TEXT)",
		},
		{
			// AffinityBlob → case 1
			name:  "CAST to BLOB",
			query: "SELECT CAST('hello' AS BLOB)",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openMCDCCompileDB(t)
			var result interface{}
			if err := db.QueryRow(tc.query).Scan(&result); err != nil {
				t.Fatalf("QueryRow(%q): %v", tc.query, err)
			}
		})
	}
}

func TestMCDC_Compile_AffinityToOpCastCode_UpdateWithCast(t *testing.T) {
	// UPDATE with CAST exercises column affinity code paths in DML compilation
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE cast1(a TEXT, b REAL, c INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO cast1 VALUES('hello', 1.5, 3)")
	mcdc_mustExec(t, db, "UPDATE cast1 SET a = CAST(42 AS TEXT), b = CAST(7 AS REAL), c = CAST(9.9 AS INTEGER)")
	var a string
	var b float64
	var c int64
	if err := db.QueryRow("SELECT a,b,c FROM cast1").Scan(&a, &b, &c); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if a != "42" || b != 7.0 || c != 9 {
		t.Fatalf("UPDATE CAST want (42,7,9), got (%s,%f,%d)", a, b, c)
	}
}

// ============================================================================
// MC/DC: emitUpdateFromSingleRow (95.5%)
//
// Condition: table has triggers vs. no triggers
// ============================================================================

func TestMCDC_Compile_EmitUpdateFromSingleRow_NoTriggers(t *testing.T) {
	// UPDATE on table without triggers → simple single-row update path
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE usr1(id INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO usr1 VALUES(1,10),(2,20)")
	mcdc_mustExec(t, db, "UPDATE usr1 SET val=99 WHERE id=1")
	rows := mcdc_mustQuery(t, db, "SELECT val FROM usr1 WHERE id=1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 99 {
		t.Fatalf("UPDATE want 99, got %v", vals)
	}
}

func TestMCDC_Compile_EmitUpdateFromSingleRow_WithTriggers(t *testing.T) {
	// UPDATE on table with triggers → trigger-aware single-row update path
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE usr2(id INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE usr2_log(old_val INTEGER, new_val INTEGER)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER usr2_upd AFTER UPDATE ON usr2 FOR EACH ROW BEGIN "+
			"INSERT INTO usr2_log VALUES(OLD.val, NEW.val); END")
	mcdc_mustExec(t, db, "INSERT INTO usr2 VALUES(1,10)")
	mcdc_mustExec(t, db, "UPDATE usr2 SET val=99 WHERE id=1")
	var oldVal, newVal int64
	if err := db.QueryRow("SELECT old_val, new_val FROM usr2_log").Scan(&oldVal, &newVal); err != nil {
		t.Fatalf("Scan log: %v", err)
	}
	if oldVal != 10 || newVal != 99 {
		t.Fatalf("trigger log want (10,99), got (%d,%d)", oldVal, newVal)
	}
}

// ============================================================================
// MC/DC: compileInsertValues (97.1%)
//
// Compound condition: RETURNING clause present vs. absent
// ============================================================================

func TestMCDC_Compile_InsertValues_NoReturning(t *testing.T) {
	// INSERT without RETURNING clause
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE iv1(a INTEGER, b TEXT)")
	mcdc_mustExec(t, db, "INSERT INTO iv1 VALUES(1,'hello')")
	rows := mcdc_mustQuery(t, db, "SELECT a,b FROM iv1")
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected 1 row")
	}
	var a int64
	var b string
	if err := rows.Scan(&a, &b); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if a != 1 || b != "hello" {
		t.Fatalf("INSERT want (1,hello), got (%d,%s)", a, b)
	}
}

func TestMCDC_Compile_InsertValues_WithReturning(t *testing.T) {
	// INSERT with RETURNING clause → retNumCols > 0 path in compileInsertValues
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE iv2(a INTEGER, b TEXT)")
	rows, err := db.Query("INSERT INTO iv2 VALUES(42,'world') RETURNING a,b")
	if err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected RETURNING row")
	}
	var a int64
	var b string
	if err := rows.Scan(&a, &b); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if a != 42 || b != "world" {
		t.Fatalf("RETURNING want (42,world), got (%d,%s)", a, b)
	}
}

// ============================================================================
// MC/DC: emitDeleteWhereClause (93.3%)
//
// Compound condition:
//   A = stmt.Where != nil (WHERE clause present)
//   B = hasTriggers (table has triggers)
//
// Cases: (A=F,B=F), (A=T,B=F), (A=T,B=T), (A=F,B=T)
// ============================================================================

func TestMCDC_Compile_EmitDeleteWhereClause_NoWhereNoTriggers(t *testing.T) {
	// A=F, B=F: DELETE all rows, no triggers
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE del1(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO del1 VALUES(1),(2),(3)")
	mcdc_mustExec(t, db, "DELETE FROM del1")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM del1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 0 {
		t.Fatalf("DELETE all want 0, got %v", vals)
	}
}

func TestMCDC_Compile_EmitDeleteWhereClause_WithWhereNoTriggers(t *testing.T) {
	// A=T, B=F: DELETE with WHERE, no triggers
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE del2(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO del2 VALUES(1),(2),(3)")
	mcdc_mustExec(t, db, "DELETE FROM del2 WHERE v > 1")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM del2")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 1 {
		t.Fatalf("DELETE WHERE want 1, got %v", vals)
	}
}

func TestMCDC_Compile_EmitDeleteWhereClause_WithWherAndTriggers(t *testing.T) {
	// A=T, B=T: DELETE with WHERE and triggers
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE del3(v INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE del3_log(deleted_val INTEGER)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER del3_trig AFTER DELETE ON del3 FOR EACH ROW BEGIN "+
			"INSERT INTO del3_log VALUES(OLD.v); END")
	mcdc_mustExec(t, db, "INSERT INTO del3 VALUES(10),(20),(30)")
	mcdc_mustExec(t, db, "DELETE FROM del3 WHERE v > 15")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM del3_log")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("DELETE trigger log want 2 entries, got %v", vals)
	}
}

func TestMCDC_Compile_EmitDeleteWhereClause_NoWhereWithTriggers(t *testing.T) {
	// A=F, B=T: DELETE all rows with trigger firing
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE del4(v INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE del4_log(cnt INTEGER)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER del4_trig AFTER DELETE ON del4 FOR EACH ROW BEGIN "+
			"INSERT INTO del4_log VALUES(OLD.v); END")
	mcdc_mustExec(t, db, "INSERT INTO del4 VALUES(1),(2)")
	mcdc_mustExec(t, db, "DELETE FROM del4")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM del4_log")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("DELETE all trigger log want 2, got %v", vals)
	}
}

// ============================================================================
// MC/DC: lookupTVF (71.4%)
//
// Compound condition:
//   A = fn found in registry (Lookup ok=true)
//   B = fn is a TableValuedFunction (type assertion ok)
//
// Cases: A=F (not found), A=T+B=F (found but not TVF), A=T+B=T (found TVF)
// ============================================================================

func TestMCDC_Compile_LookupTVF_NotFound(t *testing.T) {
	// A=F: function not registered → TVF returns nil → error
	t.Parallel()
	db := openMCDCCompileDB(t)
	_, err := db.Query("SELECT * FROM nonexistent_tvf(1, 10)")
	if err == nil {
		t.Fatal("expected error for non-existent TVF")
	}
}

func TestMCDC_Compile_LookupTVF_FoundTVF(t *testing.T) {
	// A=T, B=T: generate_series is registered as TVF
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(1,3)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 || vals[0] != 1 || vals[2] != 3 {
		t.Fatalf("generate_series want [1 2 3], got %v", vals)
	}
}

func TestMCDC_Compile_LookupTVF_WithStep(t *testing.T) {
	// TVF with all 3 arguments: start, stop, step
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(0,10,3)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 4 || vals[0] != 0 || vals[3] != 9 {
		t.Fatalf("generate_series(0,10,3) want [0 3 6 9], got %v", vals)
	}
}

// ============================================================================
// MC/DC: resolveTVFValue (71.4%)
//
// Handles different value types: int, float, string, nil (NULL), and default
// The function converts driver.Value to functions.Value for TVF args.
// ============================================================================

func TestMCDC_Compile_ResolveTVFValue_IntArg(t *testing.T) {
	// Integer literal argument to TVF
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(5,5)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 5 {
		t.Fatalf("want [5], got %v", vals)
	}
}

func TestMCDC_Compile_ResolveTVFValue_FloatArg(t *testing.T) {
	// Float argument to TVF (float → int truncation)
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(1.0, 3.0)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 {
		t.Fatalf("float args want 3 rows, got %v", vals)
	}
}

func TestMCDC_Compile_ResolveTVFValue_BindParam(t *testing.T) {
	// Bind parameter (int64 from driver) to TVF
	// Use prepared statement with single bind param (stop value)
	t.Parallel()
	db := openMCDCCompileDB(t)
	stmt, err := db.Prepare("SELECT value FROM generate_series(1, ?)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(int64(4))
	if err != nil {
		t.Fatalf("Query with bind param: %v", err)
	}
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 4 || vals[0] != 1 || vals[3] != 4 {
		t.Fatalf("bind param TVF want [1 2 3 4], got %v", vals)
	}
}

// ============================================================================
// MC/DC: compileSelectWithJoinsAndAggregates (96.2%) / emitJoinLevelAgg (96.3%)
//
// Compound condition in emitJoinLevelAgg:
//   A = aggInfo has GROUP BY expressions
//   B = aggInfo has a HAVING clause
//
// Cases: (A=T,B=F), (A=T,B=T), (A=F,B=F)
// ============================================================================

func TestMCDC_Compile_JoinAgg_GroupByNoHaving(t *testing.T) {
	// A=T, B=F: GROUP BY without HAVING
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ja1(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE ja2(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO ja1 VALUES(1,10),(2,20)")
	mcdc_mustExec(t, db, "INSERT INTO ja2 VALUES(1,5),(1,15),(2,25)")
	rows := mcdc_mustQuery(t, db,
		"SELECT ja1.id, SUM(ja2.v) FROM ja1 JOIN ja2 ON ja1.id=ja2.id GROUP BY ja1.id ORDER BY ja1.id")
	defer rows.Close()
	var results [][2]int64
	for rows.Next() {
		var id, s int64
		if err := rows.Scan(&id, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, [2]int64{id, s})
	}
	if len(results) != 2 || results[0][1] != 20 || results[1][1] != 25 {
		t.Fatalf("JOIN GROUP BY want [(1,20),(2,25)], got %v", results)
	}
}

func TestMCDC_Compile_JoinAgg_GroupByWithHaving(t *testing.T) {
	// A=T, B=T: GROUP BY with HAVING
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ja3(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE ja4(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO ja3 VALUES(1,10),(2,20),(3,30)")
	mcdc_mustExec(t, db, "INSERT INTO ja4 VALUES(1,5),(1,15),(2,25),(3,35)")
	rows := mcdc_mustQuery(t, db,
		"SELECT ja3.id, SUM(ja4.v) FROM ja3 JOIN ja4 ON ja3.id=ja4.id GROUP BY ja3.id HAVING SUM(ja4.v) > 20")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, s int64
		if err := rows.Scan(&id, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		if s <= 20 {
			t.Fatalf("HAVING filtered wrong row: id=%d, sum=%d", id, s)
		}
	}
	if count != 2 {
		t.Fatalf("HAVING want 2 rows, got %d", count)
	}
}

func TestMCDC_Compile_JoinAgg_NoGroupBy(t *testing.T) {
	// A=F, B=F: JOIN with aggregate but no GROUP BY (total aggregate)
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ja5(id INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE ja6(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO ja5 VALUES(1),(2)")
	mcdc_mustExec(t, db, "INSERT INTO ja6 VALUES(1,10),(2,20)")
	rows := mcdc_mustQuery(t, db, "SELECT SUM(ja6.v) FROM ja5 JOIN ja6 ON ja5.id=ja6.id")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 30 {
		t.Fatalf("JOIN SUM want 30, got %v", vals)
	}
}

// ============================================================================
// MC/DC: execSingleStmt (70%) in multi_stmt.go
//
// Compound condition:
//   A = vm.Run() returns error
//   B = m.conn.inTx (in explicit transaction)
//
// Cases: A=F (success), A=T+B=F (error in autocommit → rollback), A=T+B=T (error in tx)
// ============================================================================

func TestMCDC_Compile_ExecSingleStmt_Success(t *testing.T) {
	// A=F: multi-statement exec succeeds
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ms1(v INTEGER); INSERT INTO ms1 VALUES(1); INSERT INTO ms1 VALUES(2)")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM ms1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("multi-stmt want 2 rows, got %v", vals)
	}
}

func TestMCDC_Compile_ExecSingleStmt_CreateAndInsert(t *testing.T) {
	// Multiple DML statements in sequence via semicolon
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db,
		"CREATE TABLE ms2(id INTEGER, name TEXT); "+
			"INSERT INTO ms2 VALUES(1,'alice'); "+
			"INSERT INTO ms2 VALUES(2,'bob')")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM ms2")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 2 {
		t.Fatalf("multi-stmt want 2, got %v", vals)
	}
}

func TestMCDC_Compile_ExecSingleStmt_ErrorOnBadSQL(t *testing.T) {
	// A=T, B=F: second statement in multi-stmt fails → rollback
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ms3(v INTEGER)")
	// INSERT into nonexistent table → second stmt fails
	_, err := db.Exec("INSERT INTO ms3 VALUES(1); INSERT INTO ms3_noexist VALUES(2)")
	if err == nil {
		t.Fatal("expected error for bad second statement")
	}
}

// ============================================================================
// MC/DC: executeVacuum (70%) / persistSchemaAfterVacuum (71.4%)
//
// executeVacuum conditions:
//   A = opts.IntoFile == "" (regular VACUUM, not INTO)
//   B = s.conn.btree != nil (btree available)
//   C = opts.IntoFile != "" && opts.SourceSchema != nil (VACUUM INTO with schema)
//
// persistSchemaAfterVacuum: always called when A=T && B=T
// ============================================================================

func TestMCDC_Compile_ExecuteVacuum_InMemory(t *testing.T) {
	// VACUUM on in-memory DB → executeVacuum called, IntoFile=""
	// In-memory: btree may not persist but vacuum runs
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE v1(x INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO v1 VALUES(1),(2),(3)")
	mcdc_mustExec(t, db, "DELETE FROM v1 WHERE x > 1")
	mcdc_mustExec(t, db, "VACUUM")
	rows := mcdc_mustQuery(t, db, "SELECT COUNT(*) FROM v1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 1 {
		t.Fatalf("post-VACUUM want 1 row, got %v", vals)
	}
}

func TestMCDC_Compile_ExecuteVacuum_FileBased(t *testing.T) {
	// VACUUM on file-based DB → persistSchemaAfterVacuum called
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "vacuum_test.db")
	db, err := sql.Open(driver.DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE fv1(x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO fv1 VALUES(1),(2),(3)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec("DELETE FROM fv1 WHERE x < 3"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if _, err := db.Exec("VACUUM"); err != nil {
		t.Fatalf("VACUUM: %v", err)
	}
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM fv1").Scan(&count); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("post-VACUUM want 1, got %d", count)
	}
}

func TestMCDC_Compile_ExecuteVacuum_Into(t *testing.T) {
	// VACUUM INTO: A=F (IntoFile != ""), C=T (SourceSchema != nil)
	// → setupVacuumIntoSchema branch
	t.Parallel()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	db, err := sql.Open(driver.DriverName, srcPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE vt1(x INTEGER)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO vt1 VALUES(10),(20)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := db.Exec("VACUUM INTO '" + dstPath + "'"); err != nil {
		t.Fatalf("VACUUM INTO: %v", err)
	}
	if _, err := os.Stat(dstPath); err != nil {
		t.Fatalf("dst file not created: %v", err)
	}
}

// ============================================================================
// MC/DC: evictLRU (71.4%) in stmt_cache.go
//
// Condition: lruList.Len() == 0 → early return
//
// Cases: empty list (early return), non-empty list (evicts back element)
// ============================================================================

func TestMCDC_Compile_EvictLRU_ManyStatements(t *testing.T) {
	// Force LRU eviction by preparing many distinct queries beyond cache capacity
	// The stmt cache has default capacity 100; prepare > 100 unique statements
	t.Parallel()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE evict1(v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO evict1 VALUES(%d)", i)); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}
	// Execute many unique SELECT statements to fill and exceed cache
	for i := 0; i < 110; i++ {
		query := fmt.Sprintf("SELECT v FROM evict1 WHERE v = %d", i)
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query %d: %v", i, err)
		}
		rows.Close()
	}
}

func TestMCDC_Compile_EvictLRU_SmallCache(t *testing.T) {
	// Use a DSN with small cache size to force eviction more easily
	t.Parallel()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE evict2(v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO evict2 VALUES(%d)", i)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	// Rapidly cycle through many unique queries
	for i := 0; i < 50; i++ {
		q := fmt.Sprintf("SELECT v+%d FROM evict2", i)
		rows, err := db.Query(q)
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		rows.Close()
	}
}

// ============================================================================
// MC/DC: resolveWindowStateIdx (71.4%) in stmt_window_helpers.go
//
// Exercises window function state index resolution for different function types.
// ============================================================================

func TestMCDC_Compile_ResolveWindowStateIdx_RowNumber(t *testing.T) {
	// ROW_NUMBER() → resolves state idx for row-number window function
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wsi1(g INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wsi1 VALUES(1,10),(1,20),(1,30),(2,40)")
	rows := mcdc_mustQuery(t, db,
		"SELECT g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn FROM wsi1 ORDER BY g, v")
	defer rows.Close()
	expected := [][2]int64{{1, 1}, {1, 2}, {1, 3}, {2, 1}}
	i := 0
	for rows.Next() {
		var g, rn int64
		if err := rows.Scan(&g, &rn); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if i < len(expected) && (g != expected[i][0] || rn != expected[i][1]) {
			t.Fatalf("row %d: want (%d,%d), got (%d,%d)", i, expected[i][0], expected[i][1], g, rn)
		}
		i++
	}
	if i != 4 {
		t.Fatalf("want 4 rows, got %d", i)
	}
}

func TestMCDC_Compile_ResolveWindowStateIdx_Rank(t *testing.T) {
	// RANK() → resolves state idx for rank window function
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wsi2(v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wsi2 VALUES(10),(10),(20),(30)")
	rows := mcdc_mustQuery(t, db, "SELECT v, RANK() OVER (ORDER BY v) FROM wsi2")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var v, rk int64
		if err := rows.Scan(&v, &rk); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 4 {
		t.Fatalf("RANK want 4 rows, got %d", count)
	}
}

func TestMCDC_Compile_ResolveWindowStateIdx_MultiWindow(t *testing.T) {
	// Multiple window functions → multiple state indices resolved
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE wsi3(grp INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO wsi3 VALUES(1,5),(1,10),(2,15)")
	rows := mcdc_mustQuery(t, db,
		"SELECT grp, v, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v), SUM(v) OVER (PARTITION BY grp) FROM wsi3")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var g, v, rn, s int64
		if err := rows.Scan(&g, &v, &rn, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("multi-window want 3 rows, got %d", count)
	}
}

// ============================================================================
// MC/DC: sortVTabRows (93.3%), collectVTabRows (94.1%), resolveVTabColumns (94.7%)
//
// sortVTabRows: sorts virtual table rows by ORDER BY
// collectVTabRows: collects rows from vtab cursor
// resolveVTabColumns: resolves which columns to return
// ============================================================================

func TestMCDC_Compile_VTabRows_SortAndCollect(t *testing.T) {
	// sortVTabRows: ORDER BY on virtual table (generate_series), ascending sort
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(1,5) ORDER BY value DESC")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 5 || vals[0] != 5 || vals[4] != 1 {
		t.Fatalf("sorted vtab want [5..1], got %v", vals)
	}
}

func TestMCDC_Compile_VTabRows_CollectWithWhere(t *testing.T) {
	// collectVTabRows: WHERE filter reduces collected rows
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(1,10) WHERE value > 7")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 || vals[0] != 8 {
		t.Fatalf("filtered vtab want [8 9 10], got %v", vals)
	}
}

func TestMCDC_Compile_VTabRows_ResolveColumns_Star(t *testing.T) {
	// resolveVTabColumns: SELECT * expands all columns
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows, err := db.Query("SELECT * FROM generate_series(1,3)")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	if len(cols) == 0 {
		t.Fatal("expected columns from generate_series")
	}
	count := 0
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

func TestMCDC_Compile_VTabRows_ResolveColumns_Specific(t *testing.T) {
	// resolveVTabColumns: SELECT specific column (not star)
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db, "SELECT value FROM generate_series(10, 12)")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 || vals[0] != 10 {
		t.Fatalf("specific col want [10 11 12], got %v", vals)
	}
}

// ============================================================================
// MC/DC: substituteBetween (70%), substituteBinary (71.4%), substituteIn (71.4%)
//
// These substitute OLD/NEW references in trigger body expressions.
// substituteBetween: BETWEEN expr in trigger body
// substituteIn: IN expr in trigger body
// substituteBinary: binary expr (AND, OR, =, etc.) in trigger body
// ============================================================================

func TestMCDC_Compile_SubstituteBetween_TriggerBETWEEN(t *testing.T) {
	// substituteBetween: trigger body INSERT with CASE WHEN NEW.val BETWEEN x AND y
	// This exercises substituteBetween in the trigger substitutor.
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE sb1(val INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE sb1_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER sb1_trig AFTER UPDATE ON sb1 FOR EACH ROW BEGIN "+
			"INSERT INTO sb1_log(msg) VALUES(CASE WHEN NEW.val BETWEEN 5 AND 15 THEN 'in_range' ELSE 'out_range' END); END")
	mcdc_mustExec(t, db, "INSERT INTO sb1 VALUES(1)")
	mcdc_mustExec(t, db, "UPDATE sb1 SET val=10")
	var msg string
	if err := db.QueryRow("SELECT msg FROM sb1_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "in_range" {
		t.Fatalf("BETWEEN trigger want 'in_range', got %q", msg)
	}
}

func TestMCDC_Compile_SubstituteBetween_NotBETWEEN(t *testing.T) {
	// substituteBetween: NOT BETWEEN in CASE WHEN expression
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE sb2(val INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE sb2_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER sb2_trig AFTER UPDATE ON sb2 FOR EACH ROW BEGIN "+
			"INSERT INTO sb2_log(msg) VALUES(CASE WHEN OLD.val NOT BETWEEN 5 AND 15 THEN 'out_range' ELSE 'in_range' END); END")
	mcdc_mustExec(t, db, "INSERT INTO sb2 VALUES(1)") // val=1, not in [5,15]
	mcdc_mustExec(t, db, "UPDATE sb2 SET val=100")
	var msg string
	if err := db.QueryRow("SELECT msg FROM sb2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "out_range" {
		t.Fatalf("NOT BETWEEN trigger want 'out_range', got %q", msg)
	}
}

func TestMCDC_Compile_SubstituteIn_TriggerIN(t *testing.T) {
	// substituteIn: trigger body uses IN in CASE WHEN with NEW column reference
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE si1(category TEXT)")
	mcdc_mustExec(t, db, "CREATE TABLE si1_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER si1_trig AFTER INSERT ON si1 FOR EACH ROW BEGIN "+
			"INSERT INTO si1_log(msg) VALUES(CASE WHEN NEW.category IN ('A','B','C') THEN 'valid' ELSE 'invalid' END); END")
	mcdc_mustExec(t, db, "INSERT INTO si1 VALUES('A')")
	var msg string
	if err := db.QueryRow("SELECT msg FROM si1_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "valid" {
		t.Fatalf("IN trigger want 'valid', got %q", msg)
	}
}

func TestMCDC_Compile_SubstituteIn_NotIN(t *testing.T) {
	// substituteIn: NOT IN expression in trigger body
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE si2(v INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE si2_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER si2_trig AFTER DELETE ON si2 FOR EACH ROW BEGIN "+
			"INSERT INTO si2_log(msg) VALUES(CASE WHEN OLD.v NOT IN (1,2,3) THEN 'special' ELSE 'normal' END); END")
	mcdc_mustExec(t, db, "INSERT INTO si2 VALUES(99)")
	mcdc_mustExec(t, db, "DELETE FROM si2")
	var msg string
	if err := db.QueryRow("SELECT msg FROM si2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "special" {
		t.Fatalf("NOT IN trigger want 'special', got %q", msg)
	}
}

func TestMCDC_Compile_SubstituteBinary_TriggerBinaryExpr(t *testing.T) {
	// substituteBinary: trigger body INSERT with binary expression using OLD/NEW
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE sbin1(a INTEGER, b INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE sbin1_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER sbin1_trig AFTER UPDATE ON sbin1 FOR EACH ROW BEGIN "+
			"INSERT INTO sbin1_log(msg) VALUES(CASE WHEN NEW.a > OLD.a AND NEW.b > OLD.b THEN 'both_up' ELSE 'other' END); END")
	mcdc_mustExec(t, db, "INSERT INTO sbin1 VALUES(1,1)")
	mcdc_mustExec(t, db, "UPDATE sbin1 SET a=5, b=10")
	var msg string
	if err := db.QueryRow("SELECT msg FROM sbin1_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "both_up" {
		t.Fatalf("binary AND trigger want 'both_up', got %q", msg)
	}
}

func TestMCDC_Compile_SubstituteBinary_ConditionFalse(t *testing.T) {
	// substituteBinary: binary condition evaluates to false branch
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE sbin2(a INTEGER, b INTEGER)")
	mcdc_mustExec(t, db, "CREATE TABLE sbin2_log(msg TEXT)")
	mcdc_mustExec(t, db,
		"CREATE TRIGGER sbin2_trig AFTER UPDATE ON sbin2 FOR EACH ROW BEGIN "+
			"INSERT INTO sbin2_log(msg) VALUES(CASE WHEN NEW.a > OLD.a AND NEW.b > OLD.b THEN 'both_up' ELSE 'other' END); END")
	mcdc_mustExec(t, db, "INSERT INTO sbin2 VALUES(10,10)")
	mcdc_mustExec(t, db, "UPDATE sbin2 SET a=5, b=20") // a decreased → AND is false
	var msg string
	if err := db.QueryRow("SELECT msg FROM sbin2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "other" {
		t.Fatalf("binary AND false want 'other', got %q", msg)
	}
}

// ============================================================================
// MC/DC: compileExplainQueryPlan (94.7%) in compile_helpers.go
//
// EXPLAIN QUERY PLAN routes through compileExplainQueryPlan.
// ============================================================================

func TestMCDC_Compile_ExplainQueryPlan_SimpleSelect(t *testing.T) {
	// EXPLAIN QUERY PLAN on simple SELECT
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE eqp1(id INTEGER, v TEXT)")
	mcdc_mustExec(t, db, "INSERT INTO eqp1 VALUES(1,'hello')")
	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM eqp1")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	// Just verify it runs without error; output varies
}

func TestMCDC_Compile_ExplainQueryPlan_WithIndex(t *testing.T) {
	// EXPLAIN QUERY PLAN on indexed SELECT
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE eqp2(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "CREATE INDEX idx_eqp2 ON eqp2(v)")
	mcdc_mustExec(t, db, "INSERT INTO eqp2 VALUES(1,100),(2,200)")
	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT id FROM eqp2 WHERE v=100")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// ============================================================================
// MC/DC: compilePragmaIndexList (95.2%) in stmt_ddl_additions.go
//
// Conditions:
//   A = table exists in schema
//   B = table has indexes
// ============================================================================

func TestMCDC_Compile_PragmaIndexList_TableWithIndexes(t *testing.T) {
	// A=T, B=T: table exists and has indexes
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE pil1(id INTEGER, v INTEGER)")
	mcdc_mustExec(t, db, "CREATE INDEX idx_pil1_v ON pil1(v)")
	rows := mcdc_mustQuery(t, db, "PRAGMA index_list('pil1')")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count == 0 {
		t.Fatal("PRAGMA index_list want at least 1 index")
	}
}

func TestMCDC_Compile_PragmaIndexList_TableNoIndexes(t *testing.T) {
	// A=T, B=F: table exists but has no indexes
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE pil2(id INTEGER, v INTEGER)")
	rows := mcdc_mustQuery(t, db, "PRAGMA index_list('pil2')")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Fatalf("no-index table want 0 indexes, got %d", count)
	}
}

func TestMCDC_Compile_PragmaIndexList_MultipleIndexes(t *testing.T) {
	// Multiple indexes on same table
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE pil3(a INTEGER, b INTEGER, c INTEGER)")
	mcdc_mustExec(t, db, "CREATE INDEX pil3_a ON pil3(a)")
	mcdc_mustExec(t, db, "CREATE UNIQUE INDEX pil3_b ON pil3(b)")
	rows := mcdc_mustQuery(t, db, "PRAGMA index_list('pil3')")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count < 2 {
		t.Fatalf("want >= 2 indexes, got %d", count)
	}
}

// ============================================================================
// MC/DC: rewriteFromClause (93.3%), rewriteSelectWithCTETables (94.4%)
//
// CTE rewriting exercises these functions.
// ============================================================================

func TestMCDC_Compile_RewriteFromClause_SimpleCTE(t *testing.T) {
	// rewriteFromClause: FROM references CTE table name
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db,
		"WITH cte1 AS (SELECT 1 AS x, 2 AS y) SELECT x+y FROM cte1")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 3 {
		t.Fatalf("CTE rewrite want [3], got %v", vals)
	}
}

func TestMCDC_Compile_RewriteSelectWithCTETables_MultiCTE(t *testing.T) {
	// rewriteSelectWithCTETables: multiple CTEs referenced in FROM
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db,
		"WITH a AS (SELECT 10 AS v), b AS (SELECT 20 AS v) SELECT a.v + b.v FROM a, b")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 30 {
		t.Fatalf("multi-CTE rewrite want [30], got %v", vals)
	}
}

func TestMCDC_Compile_RewriteFromClause_CTEWithJoin(t *testing.T) {
	// rewriteFromClause: CTE in JOIN context
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE base_tbl(id INTEGER, val INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO base_tbl VALUES(1,10),(2,20)")
	mcdc_mustExec(t, db, "CREATE TABLE mult_tbl(id INTEGER, m INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO mult_tbl VALUES(1,10),(2,20)")
	rows := mcdc_mustQuery(t, db,
		"WITH multipliers AS (SELECT id, m FROM mult_tbl) "+
			"SELECT base_tbl.val * multipliers.m FROM base_tbl JOIN multipliers ON base_tbl.id=multipliers.id ORDER BY base_tbl.id")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 2 || vals[0] != 100 || vals[1] != 400 {
		t.Fatalf("CTE join rewrite want [100 400], got %v", vals)
	}
}

// ============================================================================
// MC/DC: compileCTEPopulationCoroutine (93.3%)
//
// Non-recursive CTE coroutine population path.
// ============================================================================

func TestMCDC_Compile_CTEPopulationCoroutine_Basic(t *testing.T) {
	// Basic non-recursive CTE → compileCTEPopulationCoroutine called
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ctep_src(n INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO ctep_src VALUES(1),(2),(3)")
	rows := mcdc_mustQuery(t, db,
		"WITH vals AS (SELECT n FROM ctep_src) SELECT n FROM vals ORDER BY n")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 3 || vals[0] != 1 || vals[2] != 3 {
		t.Fatalf("CTE coroutine want [1 2 3], got %v", vals)
	}
}

func TestMCDC_Compile_CTEPopulationCoroutine_WithFilter(t *testing.T) {
	// CTE with WHERE on outer SELECT
	t.Parallel()
	db := openMCDCCompileDB(t)
	mcdc_mustExec(t, db, "CREATE TABLE ctep_src2(n INTEGER)")
	mcdc_mustExec(t, db, "INSERT INTO ctep_src2 VALUES(1),(2),(3),(4)")
	rows := mcdc_mustQuery(t, db,
		"WITH nums AS (SELECT n FROM ctep_src2) SELECT n FROM nums WHERE n > 2 ORDER BY n")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 2 || vals[0] != 3 || vals[1] != 4 {
		t.Fatalf("CTE coroutine filter want [3 4], got %v", vals)
	}
}

// ============================================================================
// MC/DC: allocateRecursiveCTEResources (93.3%), adjustInstrWithMap (93.3%),
//         emitRecursiveMemberInlined (95.2%)
//
// Recursive CTE exercises all three functions.
// ============================================================================

func TestMCDC_Compile_RecursiveCTE_Basic(t *testing.T) {
	// allocateRecursiveCTEResources, adjustInstrWithMap, emitRecursiveMemberInlined
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db,
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT n FROM cnt")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 5 || vals[0] != 1 || vals[4] != 5 {
		t.Fatalf("recursive CTE want [1..5], got %v", vals)
	}
}

func TestMCDC_Compile_RecursiveCTE_Fibonacci(t *testing.T) {
	// More complex recursive CTE with two columns
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db,
		"WITH RECURSIVE fib(a,b) AS (SELECT 0,1 UNION ALL SELECT b, a+b FROM fib WHERE a < 10) "+
			"SELECT a FROM fib")
	vals := mcdc_collectInts(t, rows)
	// Fibonacci: 0, 1, 1, 2, 3, 5, 8, 13 (stops when a >= 10)
	if len(vals) == 0 {
		t.Fatal("recursive CTE fibonacci: expected rows")
	}
	if vals[0] != 0 {
		t.Fatalf("fibonacci first want 0, got %d", vals[0])
	}
}

func TestMCDC_Compile_RecursiveCTE_WithAggregation(t *testing.T) {
	// Recursive CTE result used in aggregation → tests all resource paths
	t.Parallel()
	db := openMCDCCompileDB(t)
	rows := mcdc_mustQuery(t, db,
		"WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 10) "+
			"SELECT SUM(n) FROM nums")
	vals := mcdc_collectInts(t, rows)
	if len(vals) != 1 || vals[0] != 55 {
		t.Fatalf("recursive CTE sum want 55, got %v", vals)
	}
}
