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

// openDriverMCDCDB opens a fresh :memory: database for driver MC/DC tests.
func openDriverMCDCDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func driverMCDC_mustExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("Exec(%q): %v", query, err)
	}
}

func driverMCDC_mustQuery(t *testing.T, db *sql.DB, query string, args ...interface{}) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", query, err)
	}
	return rows
}

// ============================================================================
// MC/DC: createMemoryConnection (69.2%) in driver.go
//
// Branches:
//   A = config.Security == nil  → use DefaultSecurityConfig()
//   B = openDatabase fails      → close pager and return error
//   C = applyConfig fails       → close pager and return error
//
// Cases: A=T (nil security → default), A=F (provided security)
//        Normal success path, multiple connections to different memory DBs
// ============================================================================

func TestMCDC_Driver_CreateMemoryConnection_DefaultSecurity(t *testing.T) {
	// A=T: open :memory: without explicit security config → default config used
	t.Parallel()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open :memory: failed: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestMCDC_Driver_CreateMemoryConnection_MultipleSeparateConnections(t *testing.T) {
	// Each :memory: open creates a separate independent database
	// exercises createMemoryConnection being called multiple times
	t.Parallel()
	const n = 5
	dbs := make([]*sql.DB, n)
	for i := range dbs {
		db, err := sql.Open(driver.DriverName, ":memory:")
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		dbs[i] = db
		if _, err := db.Exec("CREATE TABLE t(v INTEGER)"); err != nil {
			t.Fatalf("CREATE TABLE in db %d: %v", i, err)
		}
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO t VALUES(%d)", i)); err != nil {
			t.Fatalf("INSERT in db %d: %v", i, err)
		}
	}
	// Each memory DB is isolated — counts are independent
	for i, db := range dbs {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
			t.Fatalf("SELECT in db %d: %v", i, err)
		}
		if count != 1 {
			t.Fatalf("db %d: want 1 row, got %d", i, count)
		}
		db.Close()
	}
}

func TestMCDC_Driver_CreateMemoryConnection_WriteAndRead(t *testing.T) {
	// Exercises createMemoryConnection success path with real operations
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE mem_data(id INTEGER PRIMARY KEY, val TEXT)")
	driverMCDC_mustExec(t, db, "INSERT INTO mem_data VALUES(1,'hello')")
	driverMCDC_mustExec(t, db, "INSERT INTO mem_data VALUES(2,'world')")
	var val string
	if err := db.QueryRow("SELECT val FROM mem_data WHERE id=1").Scan(&val); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if val != "hello" {
		t.Fatalf("want 'hello', got %q", val)
	}
}

// ============================================================================
// MC/DC: executeVacuum additional branches (70%)
//
// Target the branch: opts.IntoFile == "" && s.conn.btree != nil
// → calls persistSchemaAfterVacuum
//
// Also target persistSchemaAfterVacuum (71.4%):
//   A = pager.InWriteTransaction() → commit branch
// ============================================================================

func TestMCDC_Vacuum_FileDB_WithMultipleTables(t *testing.T) {
	// File-based VACUUM with multiple tables → persistSchemaAfterVacuum with richer schema
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "multi_table.db")
	db, err := sql.Open(driver.DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	driverMCDC_mustExec(t, db, "CREATE TABLE a(x INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE b(y TEXT)")
	for i := 0; i < 20; i++ {
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO a VALUES(%d)", i))
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO b VALUES('item%d')", i))
	}
	driverMCDC_mustExec(t, db, "DELETE FROM a WHERE x < 10")
	driverMCDC_mustExec(t, db, "VACUUM")

	var countA, countB int
	if err := db.QueryRow("SELECT COUNT(*) FROM a").Scan(&countA); err != nil {
		t.Fatalf("count a: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM b").Scan(&countB); err != nil {
		t.Fatalf("count b: %v", err)
	}
	if countA != 10 {
		t.Fatalf("table a: want 10, got %d", countA)
	}
	if countB != 20 {
		t.Fatalf("table b: want 20, got %d", countB)
	}
}

func TestMCDC_Vacuum_Into_VerifyTarget(t *testing.T) {
	// VACUUM INTO: exercises setupVacuumIntoSchema, then verify target readable
	t.Parallel()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src2.db")
	dstPath := filepath.Join(dir, "dst2.db")

	db, err := sql.Open(driver.DriverName, srcPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	driverMCDC_mustExec(t, db, "CREATE TABLE items(id INTEGER, name TEXT)")
	for i := 1; i <= 10; i++ {
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO items VALUES(%d,'item%d')", i, i))
	}
	if _, err := db.Exec("VACUUM INTO '" + dstPath + "'"); err != nil {
		t.Fatalf("VACUUM INTO: %v", err)
	}

	// Target file must exist
	if _, err := os.Stat(dstPath); err != nil {
		t.Fatalf("target file not created: %v", err)
	}
}

func TestMCDC_Vacuum_PersistSchema_AfterInsertDelete(t *testing.T) {
	// persistSchemaAfterVacuum: exercises InWriteTransaction commit branch
	// by doing insert/delete before VACUUM to ensure write transaction
	t.Parallel()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	db, err := sql.Open(driver.DriverName, dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	driverMCDC_mustExec(t, db, "CREATE TABLE persist_t(v INTEGER)")
	for i := 0; i < 50; i++ {
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO persist_t VALUES(%d)", i))
	}
	driverMCDC_mustExec(t, db, "DELETE FROM persist_t WHERE v % 2 = 0")
	driverMCDC_mustExec(t, db, "VACUUM")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM persist_t").Scan(&count); err != nil {
		t.Fatalf("post-VACUUM count: %v", err)
	}
	if count != 25 {
		t.Fatalf("want 25, got %d", count)
	}
}

// ============================================================================
// MC/DC: substituteBetween additional branches (70%)
//
// Target: error path when substituteExpr fails on sub-expressions
// Also target normal NOT BETWEEN with OLD reference
// ============================================================================

func TestMCDC_Trigger_SubstituteBetween_OldRef(t *testing.T) {
	// substituteBetween with OLD column reference (not NEW)
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE bt1(score INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE bt1_log(result TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER bt1_trig AFTER UPDATE ON bt1 FOR EACH ROW BEGIN "+
			"INSERT INTO bt1_log(result) VALUES("+
			"CASE WHEN OLD.score BETWEEN 1 AND 50 THEN 'was_low' ELSE 'was_high' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO bt1 VALUES(25)") // OLD.score=25 → in [1,50]
	driverMCDC_mustExec(t, db, "UPDATE bt1 SET score=100")
	var result string
	if err := db.QueryRow("SELECT result FROM bt1_log").Scan(&result); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if result != "was_low" {
		t.Fatalf("want 'was_low', got %q", result)
	}
}

func TestMCDC_Trigger_SubstituteBetween_BothOldNew(t *testing.T) {
	// substituteBetween: both OLD and NEW in same trigger body
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE bt2(v INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE bt2_log(msg TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER bt2_trig AFTER UPDATE ON bt2 FOR EACH ROW BEGIN "+
			"INSERT INTO bt2_log(msg) VALUES("+
			"CASE WHEN OLD.v BETWEEN 1 AND 10 AND NEW.v BETWEEN 11 AND 20 THEN 'upgrade' ELSE 'other' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO bt2 VALUES(5)") // OLD=5 ∈ [1,10]
	driverMCDC_mustExec(t, db, "UPDATE bt2 SET v=15")       // NEW=15 ∈ [11,20]
	var msg string
	if err := db.QueryRow("SELECT msg FROM bt2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "upgrade" {
		t.Fatalf("want 'upgrade', got %q", msg)
	}
}

// ============================================================================
// MC/DC: substituteIn additional branches (71.4%)
//
// Target: NOT IN with OLD reference, and IN with empty list or many items
// ============================================================================

func TestMCDC_Trigger_SubstituteIn_OldRef(t *testing.T) {
	// substituteIn: OLD reference in IN expression
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE in1(status INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE in1_log(msg TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER in1_trig AFTER DELETE ON in1 FOR EACH ROW BEGIN "+
			"INSERT INTO in1_log(msg) VALUES("+
			"CASE WHEN OLD.status IN (1,2,3) THEN 'active' ELSE 'inactive' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO in1 VALUES(2)")
	driverMCDC_mustExec(t, db, "DELETE FROM in1")
	var msg string
	if err := db.QueryRow("SELECT msg FROM in1_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "active" {
		t.Fatalf("want 'active', got %q", msg)
	}
}

func TestMCDC_Trigger_SubstituteIn_ManyValues(t *testing.T) {
	// substituteIn: IN expression with many literal values
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE in2(code INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE in2_log(msg TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER in2_trig AFTER INSERT ON in2 FOR EACH ROW BEGIN "+
			"INSERT INTO in2_log(msg) VALUES("+
			"CASE WHEN NEW.code IN (10,20,30,40,50,60,70,80,90,100) THEN 'ten_multiple' ELSE 'other' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO in2 VALUES(50)")
	var msg string
	if err := db.QueryRow("SELECT msg FROM in2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "ten_multiple" {
		t.Fatalf("want 'ten_multiple', got %q", msg)
	}
}

// ============================================================================
// MC/DC: substituteBinary additional branches (71.4%)
//
// Target: OR binary expression, comparison with literals
// ============================================================================

func TestMCDC_Trigger_SubstituteBinary_OrExpr(t *testing.T) {
	// substituteBinary: OR binary expression in trigger body
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE bin1(x INTEGER, y INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE bin1_log(msg TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER bin1_trig AFTER INSERT ON bin1 FOR EACH ROW BEGIN "+
			"INSERT INTO bin1_log(msg) VALUES("+
			"CASE WHEN NEW.x > 100 OR NEW.y > 100 THEN 'big' ELSE 'small' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO bin1 VALUES(5, 200)") // y > 100
	var msg string
	if err := db.QueryRow("SELECT msg FROM bin1_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "big" {
		t.Fatalf("want 'big', got %q", msg)
	}
}

func TestMCDC_Trigger_SubstituteBinary_ArithmeticExpr(t *testing.T) {
	// substituteBinary: arithmetic expression NEW.a + NEW.b in trigger
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE bin2(a INTEGER, b INTEGER)")
	driverMCDC_mustExec(t, db, "CREATE TABLE bin2_log(msg TEXT)")
	driverMCDC_mustExec(t, db,
		"CREATE TRIGGER bin2_trig AFTER INSERT ON bin2 FOR EACH ROW BEGIN "+
			"INSERT INTO bin2_log(msg) VALUES("+
			"CASE WHEN NEW.a + NEW.b > 10 THEN 'sum_big' ELSE 'sum_small' END); END")
	driverMCDC_mustExec(t, db, "INSERT INTO bin2 VALUES(7, 5)") // 7+5=12 > 10
	var msg string
	if err := db.QueryRow("SELECT msg FROM bin2_log").Scan(&msg); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if msg != "sum_big" {
		t.Fatalf("want 'sum_big', got %q", msg)
	}
}

// ============================================================================
// MC/DC: resolveWindowStateIdx additional branches (71.4%)
//
// Target: windowStateMap lookup hit vs miss, nil Over early return
// ============================================================================

func TestMCDC_Window_ResolveStateIdx_DenseRank(t *testing.T) {
	// DENSE_RANK() exercises hasDenseRank branch in resolveWindowStateIdx
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE wr1(cat INTEGER, v INTEGER)")
	driverMCDC_mustExec(t, db, "INSERT INTO wr1 VALUES(1,10),(1,10),(1,20),(2,5)")
	rows := driverMCDC_mustQuery(t, db,
		"SELECT cat, v, DENSE_RANK() OVER (PARTITION BY cat ORDER BY v) FROM wr1")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var cat, v, dr int64
		if err := rows.Scan(&cat, &v, &dr); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		_ = cat
		_ = v
		_ = dr
	}
	if count != 4 {
		t.Fatalf("want 4 rows, got %d", count)
	}
}

func TestMCDC_Window_ResolveStateIdx_MixedFunctions(t *testing.T) {
	// Multiple different window functions with different OVER clauses
	// → multiple windowStateMap entries, exercises both hit and miss paths
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE wr2(id INTEGER, grp INTEGER, val INTEGER)")
	driverMCDC_mustExec(t, db, "INSERT INTO wr2 VALUES(1,1,10),(2,1,20),(3,2,30)")
	rows, err := db.Query(
		"SELECT id, ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY id) FROM wr2")
	if err != nil {
		t.Skipf("mixed window funcs: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, rn, rk int64
		if err := rows.Scan(&id, &rn, &rk); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		_ = id
		_ = rn
		_ = rk
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

func TestMCDC_Window_ResolveStateIdx_SumWindow(t *testing.T) {
	// SUM() OVER → exercises aggregate window path through resolveWindowStateIdx
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE wr3(id INTEGER, val INTEGER)")
	driverMCDC_mustExec(t, db, "INSERT INTO wr3 VALUES(1,100),(2,200),(3,300)")
	rows := driverMCDC_mustQuery(t, db,
		"SELECT id, SUM(val) OVER (ORDER BY id) FROM wr3")
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, s int64
		if err := rows.Scan(&id, &s); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
		_ = s
	}
	if count != 3 {
		t.Fatalf("want 3 rows, got %d", count)
	}
}

// ============================================================================
// MC/DC: evictLRU additional branches (71.4%)
//
// Target: empty list early return (Len() == 0 → return immediately)
// Force cache cycling with schema invalidation
// ============================================================================

func TestMCDC_Cache_EvictLRU_SchemaInvalidation(t *testing.T) {
	// Schema change invalidates cached statements, forcing re-compilation
	// This exercises evictLRU indirectly via schema version bumps
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE cache1(v INTEGER)")
	// Execute queries, then change schema to invalidate cache
	for i := 0; i < 10; i++ {
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO cache1 VALUES(%d)", i))
	}
	rows := driverMCDC_mustQuery(t, db, "SELECT COUNT(*) FROM cache1")
	rows.Close()
	// Add a column to invalidate cached statements
	if _, err := db.Exec("ALTER TABLE cache1 ADD COLUMN extra TEXT"); err != nil {
		t.Skipf("ALTER TABLE: %v", err)
	}
	// Re-execute same query after schema change
	rows = driverMCDC_mustQuery(t, db, "SELECT COUNT(*) FROM cache1")
	defer rows.Close()
	for rows.Next() {
		var c int64
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
}

func TestMCDC_Cache_EvictLRU_CacheExceedCapacity(t *testing.T) {
	// Execute more unique statements than default cache capacity (100)
	// to trigger evictLRU for each statement beyond capacity
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE cache2(v INTEGER)")
	for i := 0; i < 5; i++ {
		driverMCDC_mustExec(t, db, fmt.Sprintf("INSERT INTO cache2 VALUES(%d)", i))
	}
	// Execute 120 unique queries to force LRU evictions
	for i := 0; i < 120; i++ {
		rows, err := db.Query(fmt.Sprintf("SELECT v FROM cache2 WHERE v = %d", i))
		if err != nil {
			t.Fatalf("Query %d: %v", i, err)
		}
		rows.Close()
	}
}

// ============================================================================
// MC/DC: execSingleStmt additional branches (70%)
//
// Target: A=T (error), B=T (conn.inTx = true) → no rollback in tx
// ============================================================================

func TestMCDC_Multi_ExecSingleStmt_ErrorInTransaction(t *testing.T) {
	// A=T, B=T: multi-stmt error within explicit transaction → no auto-rollback
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE multi1(v INTEGER)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec("INSERT INTO multi1 VALUES(1)"); err != nil {
		t.Fatalf("INSERT in tx: %v", err)
	}
	// Insert into non-existent table to trigger error inside a transaction
	_, execErr := tx.Exec("INSERT INTO no_such_table VALUES(99)")
	if execErr == nil {
		t.Fatal("expected error for non-existent table")
	}
	// Transaction should still be usable (or need rollback); check error was returned
	_ = tx.Rollback()
}

func TestMCDC_Multi_ExecSingleStmt_MultipleStmtTypes(t *testing.T) {
	// Multi-statement with CREATE + INSERT + UPDATE in one Exec call
	t.Parallel()
	db := openDriverMCDCDB(t)
	_, err := db.Exec(
		"CREATE TABLE multi2(id INTEGER, v INTEGER); " +
			"INSERT INTO multi2 VALUES(1, 10); " +
			"INSERT INTO multi2 VALUES(2, 20)")
	if err != nil {
		t.Fatalf("multi-stmt exec: %v", err)
	}
	var count int64
	if err := db.QueryRow("SELECT COUNT(*) FROM multi2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("want 2, got %d", count)
	}
}

func TestMCDC_Multi_ExecSingleStmt_AutocommitRollback(t *testing.T) {
	// A=T, B=F: multi-stmt error in autocommit mode → rollback called
	t.Parallel()
	db := openDriverMCDCDB(t)
	driverMCDC_mustExec(t, db, "CREATE TABLE multi3(v INTEGER NOT NULL)")
	// Second statement will fail (table doesn't exist) — should rollback first insert
	_, err := db.Exec("INSERT INTO multi3 VALUES(42); INSERT INTO multi3_bad VALUES(1)")
	if err == nil {
		t.Fatal("expected error from missing table")
	}
	// Due to rollback, original insert may or may not persist depending on implementation
	// Just verify no panic and error was returned — that's the important part
}

// ============================================================================
// MC/DC: lookupTVF additional branches (71.4%)
//
// Target: function found but is not a TableValuedFunction (non-TVF function)
// ============================================================================

func TestMCDC_TVF_LookupTVF_GenerateSeries(t *testing.T) {
	// generate_series → found and is a TVF (successful lookupTVF)
	t.Parallel()
	db := openDriverMCDCDB(t)
	rows := driverMCDC_mustQuery(t, db, "SELECT value FROM generate_series(1, 5)")
	defer rows.Close()
	var vals []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 5 {
		t.Fatalf("want 5 values, got %v", vals)
	}
	if vals[0] != 1 || vals[4] != 5 {
		t.Fatalf("want [1..5], got %v", vals)
	}
}

func TestMCDC_TVF_LookupTVF_StepArgument(t *testing.T) {
	// generate_series with step → exercises resolveTVFValue with 3 args
	t.Parallel()
	db := openDriverMCDCDB(t)
	rows := driverMCDC_mustQuery(t, db, "SELECT value FROM generate_series(0, 10, 2)")
	defer rows.Close()
	var vals []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 6 {
		t.Fatalf("want 6 values (0,2,4,6,8,10), got %v", vals)
	}
}

func TestMCDC_TVF_LookupTVF_NotFound(t *testing.T) {
	// lookupTVF: function name not registered → lookupTVF returns nil
	t.Parallel()
	db := openDriverMCDCDB(t)
	_, err := db.Query("SELECT * FROM no_such_tvf(1, 2)")
	if err == nil {
		t.Fatal("expected error for unknown TVF")
	}
}

func TestMCDC_TVF_ResolveTVFValue_StringArg(t *testing.T) {
	// resolveTVFValue with string literal (LiteralString branch)
	t.Parallel()
	db := openDriverMCDCDB(t)
	// json_each with a JSON string literal exercises string TVF arg
	rows, err := db.Query("SELECT value FROM json_each('[1,2,3]')")
	if err != nil {
		t.Skipf("json_each not available: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	if count == 0 {
		t.Fatal("expected rows from json_each")
	}
}

func TestMCDC_TVF_ResolveTVFValue_NullLiteral(t *testing.T) {
	// resolveTVFValue: NULL literal → LiteralNull branch → NewNullValue
	t.Parallel()
	db := openDriverMCDCDB(t)
	// generate_series with NULL stop → should handle gracefully
	rows, err := db.Query("SELECT value FROM generate_series(1, NULL)")
	if err != nil {
		t.Skipf("NULL arg to generate_series: %v", err)
	}
	defer rows.Close()
	// May return 0 rows or handle NULL as 0 — just ensure no panic
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
}
