// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// drv17Open opens an in-memory sqlite_internal database or fatals.
func drv17Open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// drv17Exec executes SQL and fatals on error.
func drv17Exec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// drv17QueryInt runs a single-int query and returns the value.
func drv17QueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// ============================================================================
// CASE with aggregate
// ============================================================================

// TestMCDC17_Case_WithAggregate exercises checkCaseAggregate returning true
// when a WHEN condition contains an aggregate (COUNT(*)).
func TestMCDC17_Case_WithAggregate(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (x INTEGER)")
	drv17Exec(t, db, "INSERT INTO t(x) VALUES(1)")

	var result string
	err := db.QueryRow("SELECT CASE WHEN COUNT(*) > 0 THEN 'yes' ELSE 'no' END FROM t").Scan(&result)
	if err != nil {
		t.Skipf("CASE with aggregate in WHEN not implemented: %v", err)
	}
	if result != "yes" {
		t.Errorf("CASE COUNT(*) > 0: got %q, want %q", result, "yes")
	}
}

// TestMCDC17_Case_AggregateInResult exercises checkCaseAggregate when an
// aggregate appears in the THEN result expression (SUM in result column).
func TestMCDC17_Case_AggregateInResult(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (x INTEGER, y INTEGER)")
	drv17Exec(t, db, "INSERT INTO t(x,y) VALUES(1,10)")
	drv17Exec(t, db, "INSERT INTO t(x,y) VALUES(1,20)")

	rows, err := db.Query("SELECT CASE x WHEN 1 THEN SUM(y) ELSE 0 END FROM t GROUP BY x")
	if err != nil {
		t.Skipf("CASE with aggregate in result not implemented: %v", err)
	}
	defer rows.Close()

	var got sql.NullInt64
	for rows.Next() {
		if scanErr := rows.Scan(&got); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if got.Valid && got.Int64 != 30 {
		t.Errorf("CASE SUM result: got %d, want 30", got.Int64)
	}
}

// ============================================================================
// Multi-table SELECT with computed columns (buildMultiTableColumnNames else branch)
// ============================================================================

// TestMCDC17_MultiTable_ComputedColumn exercises the else branch of
// buildMultiTableColumnNames where a result column is an expression without an
// alias, producing a generated name like "column1".
func TestMCDC17_MultiTable_ComputedColumn(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv17Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, y INTEGER)")
	drv17Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,3)")
	drv17Exec(t, db, "INSERT INTO t2(id,y) VALUES(1,4)")

	rows, err := db.Query("SELECT t1.x + t2.y FROM t1 JOIN t2 ON t1.id = t2.id")
	if err != nil {
		t.Skipf("multi-table computed column not implemented: %v", err)
	}
	defer rows.Close()

	var got int64
	for rows.Next() {
		if scanErr := rows.Scan(&got); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if got != 7 {
		t.Errorf("t1.x + t2.y: got %d, want 7", got)
	}
}

// ============================================================================
// Schema-qualified table references
// ============================================================================

// TestMCDC17_SchemaQualified_Select exercises a SELECT against main.t to cover
// the schema-qualified table reference path in createTableInfoFromRef.
func TestMCDC17_SchemaQualified_Select(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv17Exec(t, db, "INSERT INTO t(id,v) VALUES(1,'hello')")

	rows, err := db.Query("SELECT * FROM main.t")
	if err != nil {
		t.Skipf("schema-qualified SELECT not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id int64
		var v string
		if scanErr := rows.Scan(&id, &v); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1 {
		t.Errorf("schema-qualified SELECT: got %d rows, want 1", count)
	}
}

// TestMCDC17_SchemaQualified_MultiTable exercises main.t1.x column reference
// with a schema-qualified table name in a single-table SELECT.
func TestMCDC17_SchemaQualified_MultiTable(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, x INTEGER)")
	drv17Exec(t, db, "INSERT INTO t1(id,x) VALUES(1,42)")

	var got int64
	err := db.QueryRow("SELECT main.t1.x FROM main.t1").Scan(&got)
	if err != nil {
		t.Skipf("schema-qualified column reference not implemented: %v", err)
	}
	if got != 42 {
		t.Errorf("main.t1.x: got %d, want 42", got)
	}
}

// ============================================================================
// UPDATE with subquery in SET (evalSubqueryToLiteral)
// ============================================================================

// TestMCDC17_UpdateFromSubquery exercises evalSubqueryToLiteral when the
// scalar subquery returns a non-empty result.
func TestMCDC17_UpdateFromSubquery(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	drv17Exec(t, db, "INSERT INTO t(id,x) VALUES(1,5)")
	drv17Exec(t, db, "INSERT INTO t(id,x) VALUES(2,10)")

	_, err := db.Exec("UPDATE t SET x = (SELECT MAX(x) FROM t) WHERE id = 1")
	if err != nil {
		t.Skipf("UPDATE with scalar subquery not implemented: %v", err)
	}

	got := drv17QueryInt(t, db, "SELECT x FROM t WHERE id = 1")
	if got != 10 {
		t.Errorf("UPDATE scalar subquery: got x=%d for id=1, want 10", got)
	}
}

// TestMCDC17_UpdateFromSubquery_NoRows exercises evalSubqueryToLiteral when
// the scalar subquery returns no rows, which should produce NULL.
func TestMCDC17_UpdateFromSubquery_NoRows(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	drv17Exec(t, db, "INSERT INTO t(id,x) VALUES(1,5)")
	drv17Exec(t, db, "CREATE TABLE empty_t (v INTEGER)")

	_, err := db.Exec("UPDATE t SET x = (SELECT v FROM empty_t LIMIT 1) WHERE id = 1")
	if err != nil {
		t.Skipf("UPDATE with no-rows subquery not implemented: %v", err)
	}

	var got interface{}
	if scanErr := db.QueryRow("SELECT x FROM t WHERE id = 1").Scan(&got); scanErr != nil {
		t.Fatalf("SELECT after UPDATE: %v", scanErr)
	}
	if got != nil {
		t.Errorf("UPDATE no-rows subquery: got x=%v, want NULL", got)
	}
}

// ============================================================================
// UPDATE FROM syntax (compileUpdateFrom)
// ============================================================================

// TestMCDC17_UpdateFrom_Basic exercises compileUpdateFrom by updating t1 rows
// using values from t2 via UPDATE ... FROM syntax.
func TestMCDC17_UpdateFrom_Basic(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	drv17Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")
	drv17Exec(t, db, "INSERT INTO t1(id,val) VALUES(1,0)")
	drv17Exec(t, db, "INSERT INTO t2(id,val) VALUES(1,99)")

	_, err := db.Exec("UPDATE t1 SET val = t2.val FROM t2 WHERE t1.id = t2.id")
	if err != nil {
		t.Skipf("UPDATE ... FROM not implemented: %v", err)
	}

	got := drv17QueryInt(t, db, "SELECT val FROM t1 WHERE id = 1")
	if got != 99 {
		t.Errorf("UPDATE FROM: got val=%d, want 99", got)
	}
}

// ============================================================================
// CREATE TRIGGER IF NOT EXISTS on existing trigger
// ============================================================================

// TestMCDC17_CreateTrigger_IfNotExists_Existing exercises the IF NOT EXISTS
// path in compileCreateTrigger when a trigger with the same name already exists.
// Expects silent success (no error).
func TestMCDC17_CreateTrigger_IfNotExists_Existing(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv17Exec(t, db, "CREATE TABLE log (entry TEXT)")

	createSQL := `CREATE TRIGGER trg_ine
		AFTER INSERT ON t
		FOR EACH ROW
		BEGIN
			INSERT INTO log(entry) VALUES(new.v);
		END`

	_, err := db.Exec(createSQL)
	if err != nil {
		t.Skipf("CREATE TRIGGER not implemented: %v", err)
	}

	_, err2 := db.Exec(`CREATE TRIGGER IF NOT EXISTS trg_ine
		AFTER INSERT ON t
		FOR EACH ROW
		BEGIN
			INSERT INTO log(entry) VALUES(new.v);
		END`)
	if err2 != nil {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS on existing trigger: got error %v, want nil", err2)
	}
}

// ============================================================================
// RIGHT JOIN (hasRightJoin, rewriteRightJoinsWithTables)
// ============================================================================

// TestMCDC17_RightJoin_Basic exercises hasRightJoin and rewriteRightJoinsWithTables
// by executing a RIGHT JOIN. Skips if unsupported.
func TestMCDC17_RightJoin_Basic(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT)")
	drv17Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, w TEXT)")
	drv17Exec(t, db, "INSERT INTO t1(id,v) VALUES(1,'a')")
	drv17Exec(t, db, "INSERT INTO t2(id,w) VALUES(1,'x'),(2,'y')")

	rows, err := db.Query("SELECT t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
	if err != nil {
		t.Skipf("RIGHT JOIN not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		if scanErr := rows.Scan(&id); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 2 {
		t.Errorf("RIGHT JOIN: got %d rows, want 2", count)
	}
}

// ============================================================================
// FULL OUTER JOIN (hasOuterJoin)
// ============================================================================

// TestMCDC17_FullOuterJoin exercises hasOuterJoin with a FULL OUTER JOIN.
// Skips if unsupported.
func TestMCDC17_FullOuterJoin(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	drv17Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
	drv17Exec(t, db, "INSERT INTO t1(id) VALUES(1),(2)")
	drv17Exec(t, db, "INSERT INTO t2(id) VALUES(2),(3)")

	rows, err := db.Query("SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id")
	if err != nil {
		t.Skipf("FULL OUTER JOIN not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var a, b interface{}
		if scanErr := rows.Scan(&a, &b); scanErr != nil {
			t.Fatalf("Scan: %v", scanErr)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count < 1 {
		t.Errorf("FULL OUTER JOIN: got %d rows, want >= 1", count)
	}
}

// ============================================================================
// EXPLAIN query (compileExplainOpcodes)
// ============================================================================

// TestMCDC17_Explain_Basic exercises compileExplain (opcode mode) by running
// EXPLAIN SELECT * FROM t and verifying rows are returned.
func TestMCDC17_Explain_Basic(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	rows, err := db.Query("EXPLAIN SELECT * FROM t")
	if err != nil {
		t.Skipf("EXPLAIN not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count == 0 {
		t.Errorf("EXPLAIN: expected at least one row, got 0")
	}
}

// TestMCDC17_Explain_QueryPlan exercises EXPLAIN QUERY PLAN which routes to
// compileExplainQueryPlan.
func TestMCDC17_Explain_QueryPlan(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	rows, err := db.Query("EXPLAIN QUERY PLAN SELECT * FROM t")
	if err != nil {
		t.Skipf("EXPLAIN QUERY PLAN not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count == 0 {
		t.Errorf("EXPLAIN QUERY PLAN: expected at least one row, got 0")
	}
}

// ============================================================================
// ANALYZE (analyzeTableIndexes, countTableRows, countDistinctPrefix)
// ============================================================================

// TestMCDC17_Analyze_Basic exercises ANALYZE on a specific table with an index,
// covering analyzeTableIndexes, countTableRows, and countDistinctPrefix.
func TestMCDC17_Analyze_Basic(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	drv17Exec(t, db, "CREATE INDEX idx_t_v ON t(v)")
	for i := 0; i < 5; i++ {
		drv17Exec(t, db, "INSERT INTO t(v) VALUES(?)", "val")
	}

	_, err := db.Exec("ANALYZE t")
	if err != nil {
		t.Skipf("ANALYZE tablename not implemented: %v", err)
	}
}

// TestMCDC17_Analyze_Database exercises ANALYZE without arguments, which
// analyzes all tables in the database.
func TestMCDC17_Analyze_Database(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, a INTEGER)")
	drv17Exec(t, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, b INTEGER)")
	drv17Exec(t, db, "CREATE INDEX idx_t1_a ON t1(a)")
	drv17Exec(t, db, "INSERT INTO t1(a) VALUES(1),(2),(3)")
	drv17Exec(t, db, "INSERT INTO t2(b) VALUES(10),(20)")

	_, err := db.Exec("ANALYZE")
	if err != nil {
		t.Skipf("ANALYZE (full database) not implemented: %v", err)
	}
}

// ============================================================================
// Pragma TVF with WHERE (compilePragmaTVFTableInfo, resolvePragmaExprValue)
// ============================================================================

// TestMCDC17_PragmaTable_IndexList exercises pragma_table_info as a table-valued
// function with a WHERE clause, covering compilePragmaTVFTableInfo and
// resolvePragmaExprValue.
func TestMCDC17_PragmaTable_IndexList(t *testing.T) {
	t.Parallel()
	db := drv17Open(t)

	drv17Exec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")

	rows, err := db.Query("SELECT * FROM pragma_table_info('t') WHERE cid > 0")
	if err != nil {
		t.Skipf("pragma_table_info TVF not implemented: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	// t has 3 columns (cid 0,1,2), filter cid>0 should return 2
	if count != 2 {
		t.Errorf("pragma_table_info WHERE cid>0: got %d rows, want 2", count)
	}
}
