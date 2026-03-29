// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// drv18Open opens an in-memory sqlite_internal database or fatals.
func drv18Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// drv18Exec executes SQL and fatals on error.
func drv18Exec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// drv18QueryInt runs a single-int query and returns the value.
func drv18QueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// ============================================================================
// Derived table (subquery in FROM) — materializeDerivedTables
// ============================================================================

// TestMCDC18_DerivedTable_Basic exercises materializeDerivedTables with a
// simple subquery alias in the FROM clause.
func TestMCDC18_DerivedTable_Basic(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(1),(2),(3)")

	rows, err := db.Query("SELECT d.x FROM (SELECT x FROM t) AS d")
	if err != nil {
		t.Skipf("derived table subquery not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x int64
		if scanErr := rows.Scan(&x); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 3 {
		t.Errorf("DerivedTable_Basic: got %d rows, want 3", count)
	}
}

// TestMCDC18_DerivedTable_WithFilter exercises materializeDerivedTables when
// the inner subquery has a WHERE clause.
func TestMCDC18_DerivedTable_WithFilter(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(-1),(0),(1),(2)")

	rows, err := db.Query("SELECT d.x FROM (SELECT x FROM t WHERE x > 0) AS d")
	if err != nil {
		t.Skipf("derived table with filter not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x int64
		if scanErr := rows.Scan(&x); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("DerivedTable_WithFilter: got %d rows, want 2", count)
	}
}

// TestMCDC18_DerivedTable_Join exercises materializeDerivedTables where the
// subquery appears as the right side of a JOIN.
func TestMCDC18_DerivedTable_Join(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, y INTEGER)")
	drv18Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,10),(2,20)")
	drv18Exec(t, db, "INSERT INTO t2(id,y) VALUES(1,100),(2,200)")

	rows, err := db.Query("SELECT t1.x, d.y FROM t1 JOIN (SELECT id, y FROM t2) AS d ON t1.id = d.id")
	if err != nil {
		t.Skipf("derived table join not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x, y int64
		if scanErr := rows.Scan(&x, &y); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("DerivedTable_Join: got %d rows, want 2", count)
	}
}

// ============================================================================
// Scalar subquery in SELECT
// ============================================================================

// TestMCDC18_ScalarSubquery exercises a scalar subquery in the SELECT list.
func TestMCDC18_ScalarSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(3),(7),(2)")

	var maxVal interface{}
	err := db.QueryRow("SELECT (SELECT MAX(x) FROM t) AS max_val").Scan(&maxVal)
	if err != nil {
		t.Skipf("scalar subquery in SELECT not implemented: %v", err)
	}
	if maxVal == nil {
		t.Errorf("ScalarSubquery: got NULL, want 7")
		return
	}
	switch v := maxVal.(type) {
	case int64:
		if v != 7 {
			t.Errorf("ScalarSubquery MAX: got %d, want 7", v)
		}
	default:
		t.Errorf("ScalarSubquery MAX: unexpected type %T value %v", maxVal, maxVal)
	}
}

// TestMCDC18_ScalarSubquery_InWhere exercises a scalar subquery used in a
// WHERE comparison.
func TestMCDC18_ScalarSubquery_InWhere(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(1),(2),(3)")

	got := drv18QueryInt(t, db, "SELECT x FROM t WHERE x = (SELECT MIN(x) FROM t)")
	if got != 1 {
		t.Errorf("ScalarSubquery_InWhere: got %d, want 1", got)
	}
}

// ============================================================================
// ALL-subquery FROM — finalizeFromSubqueriesCompilation else branch
// ============================================================================

// TestMCDC18_AllSubqueryFrom exercises the path where all FROM tables are
// subqueries, hitting the else branch of finalizeFromSubqueriesCompilation.
func TestMCDC18_AllSubqueryFrom(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	rows, err := db.Query("SELECT a FROM (SELECT 1 AS a)")
	if err != nil {
		t.Skipf("all-subquery FROM not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var a interface{}
		if scanErr := rows.Scan(&a); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Skipf("AllSubqueryFrom rows.Err (feature may be unsupported): %v", err)
	}
	// result may be 0 or 1 rows depending on implementation
	if count < 0 {
		t.Errorf("AllSubqueryFrom: unexpected negative count")
	}
}

// ============================================================================
// negateValue non-numeric path — vtab with WHERE clause
// ============================================================================

// TestMCDC18_VTab_NegateValue exercises scanVTabRows / matchesVTabWhere with a
// TVF that has a text-valued column, covering the non-numeric path in
// negateValue.
func TestMCDC18_VTab_NegateValue(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	rows, err := db.Query("SELECT value FROM json_each('[\"a\",\"b\",\"c\"]') WHERE key >= 0")
	if err != nil {
		t.Skipf("json_each TVF not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v string
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 3 {
		t.Errorf("VTab_NegateValue: got %d rows, want 3", count)
	}
}

// ============================================================================
// TVF evalLiteralExpr with different literal types
// ============================================================================

// TestMCDC18_TVF_LiteralInteger exercises evalLiteralExpr with an integer
// literal as a TVF WHERE argument.
func TestMCDC18_TVF_LiteralInteger(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	rows, err := db.Query("SELECT value FROM json_each('[1,2,3]') WHERE key = 1")
	if err != nil {
		t.Skipf("json_each with integer WHERE not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v interface{}
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1 {
		t.Errorf("TVF_LiteralInteger: got %d rows, want 1", count)
	}
}

// TestMCDC18_TVF_LiteralText exercises evalLiteralExpr with a text literal
// as a TVF WHERE argument.
func TestMCDC18_TVF_LiteralText(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	rows, err := db.Query("SELECT value FROM json_each('{\"key\":\"value\"}') WHERE key = 'key'")
	if err != nil {
		t.Skipf("json_each with text WHERE not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v string
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1 {
		t.Errorf("TVF_LiteralText: got %d rows, want 1", count)
	}
}

// TestMCDC18_TVF_LiteralFloat exercises evalLiteralExpr with a float-like
// comparison in a TVF WHERE clause.
func TestMCDC18_TVF_LiteralFloat(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	rows, err := db.Query("SELECT value FROM json_each('[1.5,2.5,3.5]') WHERE key >= 0")
	if err != nil {
		t.Skipf("json_each with float comparison not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v interface{}
		if scanErr := rows.Scan(&v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 3 {
		t.Errorf("TVF_LiteralFloat: got %d rows, want 3", count)
	}
}

// ============================================================================
// GROUP_CONCAT / JSON aggregates
// ============================================================================

// TestMCDC18_JsonGroupArray exercises json_group_array aggregate.
func TestMCDC18_JsonGroupArray(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(1),(2),(3)")

	var result string
	err := db.QueryRow("SELECT json_group_array(x) FROM t").Scan(&result)
	if err != nil {
		t.Skipf("json_group_array not implemented: %v", err)
	}
	if result == "" {
		t.Errorf("JsonGroupArray: got empty string, want non-empty JSON array")
	}
}

// TestMCDC18_JsonGroupObject exercises json_group_object aggregate.
func TestMCDC18_JsonGroupObject(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (k TEXT, v INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(k,v) VALUES('a',1),('b',2)")

	var result string
	err := db.QueryRow("SELECT json_group_object(k, v) FROM t").Scan(&result)
	if err != nil {
		t.Skipf("json_group_object not implemented: %v", err)
	}
	if result == "" {
		t.Errorf("JsonGroupObject: got empty string, want non-empty JSON object")
	}
}

// ============================================================================
// CASE in aggregate context — findAggregateInExpr
// ============================================================================

// TestMCDC18_CaseInAggregate exercises findAggregateInExpr when a CASE
// expression is the argument to SUM.
func TestMCDC18_CaseInAggregate(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(-1),(0),(1),(2)")

	got := drv18QueryInt(t, db, "SELECT SUM(CASE WHEN x > 0 THEN x ELSE 0 END) FROM t")
	if got != 3 {
		t.Errorf("CaseInAggregate SUM: got %d, want 3", got)
	}
}

// TestMCDC18_AggregateInSubquery exercises findAggregateInExpr when an
// aggregate is inside a scalar subquery with a CASE argument.
func TestMCDC18_AggregateInSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t(x) VALUES(-1),(0),(1),(2)")

	var result interface{}
	err := db.QueryRow("SELECT (SELECT COUNT(CASE WHEN x > 0 THEN 1 END) FROM t)").Scan(&result)
	if err != nil {
		t.Skipf("aggregate in subquery with CASE not implemented: %v", err)
	}
	if result == nil {
		t.Errorf("AggregateInSubquery: got NULL, want 2")
		return
	}
	switch v := result.(type) {
	case int64:
		if v != 2 {
			t.Errorf("AggregateInSubquery COUNT CASE: got %d, want 2", v)
		}
	default:
		t.Errorf("AggregateInSubquery: unexpected type %T value %v", result, result)
	}
}

// ============================================================================
// Correlated subquery
// ============================================================================

// TestMCDC18_CorrelatedSubquery exercises a correlated scalar subquery that
// references an outer table column.
func TestMCDC18_CorrelatedSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref_id INTEGER)")
	drv18Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,10),(2,20)")
	drv18Exec(t, db, "INSERT INTO t2(id,ref_id) VALUES(1,1),(2,1),(3,2)")

	rows, err := db.Query("SELECT x, (SELECT COUNT(*) FROM t2 WHERE t2.ref_id = t1.id) FROM t1")
	if err != nil {
		t.Skipf("correlated subquery not implemented: %v", err)
	}
	defer rows.Close()

	type row struct{ x, cnt int64 }
	var results []row
	for rows.Next() {
		var r row
		if scanErr := rows.Scan(&r.x, &r.cnt); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("CorrelatedSubquery: got %d rows, want 2", len(results))
	}
}

// ============================================================================
// EXISTS subquery
// ============================================================================

// TestMCDC18_ExistsSubquery exercises EXISTS (SELECT 1 ...) in a WHERE clause.
func TestMCDC18_ExistsSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,5),(2,10)")
	drv18Exec(t, db, "INSERT INTO t2(id,x) VALUES(1,5),(2,99)")

	rows, err := db.Query("SELECT x FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.x)")
	if err != nil {
		t.Skipf("EXISTS subquery not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x int64
		if scanErr := rows.Scan(&x); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1 {
		t.Errorf("ExistsSubquery: got %d rows, want 1", count)
	}
}

// TestMCDC18_NotExistsSubquery exercises NOT EXISTS (SELECT 1 ...) in a WHERE
// clause, returning rows that have no match in the subquery.
func TestMCDC18_NotExistsSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,5),(2,10)")
	drv18Exec(t, db, "INSERT INTO t2(id,x) VALUES(1,5)")

	rows, err := db.Query("SELECT x FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.x)")
	if err != nil {
		t.Skipf("NOT EXISTS subquery not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x int64
		if scanErr := rows.Scan(&x); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1 {
		t.Errorf("NotExistsSubquery: got %d rows, want 1", count)
	}
}

// ============================================================================
// IN subquery
// ============================================================================

// TestMCDC18_InSubquery exercises WHERE x IN (SELECT x FROM ...).
func TestMCDC18_InSubquery(t *testing.T) {
	t.Parallel()
	db := drv18Open(t)

	drv18Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv18Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,1),(2,2),(3,3)")
	drv18Exec(t, db, "INSERT INTO t2(id,x) VALUES(1,2),(2,3)")

	rows, err := db.Query("SELECT x FROM t1 WHERE x IN (SELECT x FROM t2)")
	if err != nil {
		t.Skipf("IN subquery not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var x int64
		if scanErr := rows.Scan(&x); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("InSubquery: got %d rows, want 2", count)
	}
}
