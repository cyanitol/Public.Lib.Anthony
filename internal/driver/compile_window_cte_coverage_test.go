// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// ============================================================================
// emitWindowFunctionColumn coverage tests (stmt_window_helpers.go:250)
// These exercise the non-sorter window column emit path by using window
// functions WITHOUT ORDER BY, so emitWindowFunctionColumn is called directly.
// ============================================================================

// TestWindowNoOrderByRowNumber exercises ROW_NUMBER() OVER () (no ORDER BY).
func TestWindowNoOrderByRowNumber(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wt(val INTEGER)",
		"INSERT INTO wt VALUES(10),(20),(30)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT val, row_number() OVER () FROM wt)")
	if n != 3 {
		t.Errorf("ROW_NUMBER OVER (): got %d rows, want 3", n)
	}
}

// TestWindowNoOrderByNtile exercises NTILE(2) OVER () (no ORDER BY).
func TestWindowNoOrderByNtile(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wnt(val INTEGER)",
		"INSERT INTO wnt VALUES(1),(2),(3),(4)",
	})
	rows, err := db.Query("SELECT val, ntile(2) OVER () FROM wnt")
	if err != nil {
		t.Fatalf("NTILE OVER (): %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var val, tile int64
		if err := rows.Scan(&val, &tile); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if count != 4 {
		t.Errorf("NTILE OVER (): got %d rows, want 4", count)
	}
}

// TestWindowNoOrderByRank exercises RANK() OVER () (no ORDER BY).
func TestWindowNoOrderByRank(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wrk(val INTEGER)",
		"INSERT INTO wrk VALUES(5),(10),(15)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT val, rank() OVER () FROM wrk)")
	if n != 3 {
		t.Errorf("RANK OVER (): got %d rows, want 3", n)
	}
}

// TestWindowNoOrderByDenseRank exercises DENSE_RANK() OVER () (no ORDER BY).
func TestWindowNoOrderByDenseRank(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wdr(val INTEGER)",
		"INSERT INTO wdr VALUES(1),(2),(3)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT val, dense_rank() OVER () FROM wdr)")
	if n != 3 {
		t.Errorf("DENSE_RANK OVER (): got %d rows, want 3", n)
	}
}

// TestWindowNoOrderByNthValue exercises NTH_VALUE() OVER () (no ORDER BY).
func TestWindowNoOrderByNthValue(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wnv(val INTEGER)",
		"INSERT INTO wnv VALUES(10),(20),(30)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT val, nth_value(val, 1) OVER () FROM wnv)")
	if n != 3 {
		t.Errorf("NTH_VALUE OVER (): got %d rows, want 3", n)
	}
}

// TestWindowNoOrderByLag exercises LAG() OVER () (no ORDER BY, placeholder path).
func TestWindowNoOrderByLag(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE wlg(val INTEGER)",
		"INSERT INTO wlg VALUES(1),(2),(3)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT val, lag(val) OVER () FROM wlg)")
	if n != 3 {
		t.Errorf("LAG OVER (): got %d rows, want 3", n)
	}
}

// ============================================================================
// fixInnerRewindAddresses coverage tests (stmt_cte_recursive.go:443)
// Recursive CTEs exercise fixInnerRewindAddresses, which patches Rewind
// instructions with P2=0 in the compiled recursive body.
// ============================================================================

// TestRecursiveCTECounter exercises a basic recursive CTE counting from 1 to 5.
func TestRecursiveCTECounter(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	n := queryInt64(t, db,
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT COUNT(*) FROM cnt")
	if n != 5 {
		t.Errorf("recursive CTE counter: got %d, want 5", n)
	}
}

// TestRecursiveCTESum exercises a recursive CTE that accumulates a sum.
func TestRecursiveCTESum(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	n := queryInt64(t, db,
		"WITH RECURSIVE s(n, total) AS (SELECT 1, 1 UNION ALL SELECT n+1, total+n+1 FROM s WHERE n < 4) SELECT total FROM s ORDER BY n DESC LIMIT 1")
	if n != 10 {
		t.Errorf("recursive CTE sum: got %d, want 10", n)
	}
}

// TestRecursiveCTEJoinBase exercises a recursive CTE joined against a base table.
func TestRecursiveCTEJoinBase(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE items(id INTEGER)",
		"INSERT INTO items VALUES(1),(2),(3)",
	})
	n := queryInt64(t, db,
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 3) SELECT COUNT(*) FROM cnt JOIN items ON cnt.n = items.id")
	if n != 3 {
		t.Errorf("recursive CTE join: got %d, want 3", n)
	}
}

// ============================================================================
// generateHavingIdentExpr coverage tests (stmt_groupby.go:917)
// HAVING clauses with plain identifiers (non-aggregate columns in GROUP BY)
// exercise generateHavingIdentExpr.
// ============================================================================

// TestHavingIdentExprBasic exercises HAVING with a bare column identifier.
func TestHavingIdentExprBasic(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE grp(a INTEGER, b INTEGER)",
		"INSERT INTO grp VALUES(1,10),(2,20),(3,30),(1,40)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT a, COUNT(*) FROM grp GROUP BY a HAVING a > 1)")
	if n != 2 {
		t.Errorf("HAVING ident >1: got %d, want 2", n)
	}
}

// TestHavingIdentExprEquality exercises HAVING with an equality check on the grouped column.
func TestHavingIdentExprEquality(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE cats(cat TEXT, score INTEGER)",
		"INSERT INTO cats VALUES('A',1),('B',2),('A',3),('C',4)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT cat, SUM(score) FROM cats GROUP BY cat HAVING cat = 'A')")
	if n != 1 {
		t.Errorf("HAVING ident equality: got %d, want 1", n)
	}
}

// TestHavingCountStar exercises HAVING COUNT(*) > N alongside an identifier HAVING.
func TestHavingCountStar(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE hcs(grp INTEGER, val INTEGER)",
		"INSERT INTO hcs VALUES(1,1),(1,2),(2,3),(3,4),(3,5),(3,6)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM (SELECT grp, COUNT(*) FROM hcs GROUP BY grp HAVING COUNT(*) > 1)")
	if n != 2 {
		t.Errorf("HAVING COUNT(*) > 1: got %d, want 2", n)
	}
}

// ============================================================================
// substituteSelect / valueToLiteral coverage tests (trigger_runtime.go)
// A trigger with a SELECT in its body exercises substituteSelect.
// Using OLD/NEW values of varying types in the trigger body exercises
// valueToLiteral (int64, float64, string, bool, nil paths).
// ============================================================================

// TestSubstituteSelectInTrigger exercises substituteSelect by placing a
// SELECT statement with a WHERE referencing NEW inside a trigger body.
func TestSubstituteSelectInTrigger(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE src(id INTEGER PRIMARY KEY, val INTEGER)",
		"CREATE TABLE audit(id INTEGER PRIMARY KEY, cnt INTEGER)",
		`CREATE TRIGGER trg_select AFTER INSERT ON src
			BEGIN
				INSERT INTO audit(cnt)
					SELECT COUNT(*) FROM src WHERE val < NEW.val;
			END`,
		"INSERT INTO src(val) VALUES(10)",
		"INSERT INTO src(val) VALUES(20)",
		"INSERT INTO src(val) VALUES(30)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM audit")
	if n != 3 {
		t.Errorf("substituteSelect trigger: got %d audit rows, want 3", n)
	}
}

// TestSubstituteSelectWhereNewCol exercises substituteSelect where the WHERE
// clause directly compares NEW.col against a literal.
func TestSubstituteSelectWhereNewCol(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE ev(id INTEGER PRIMARY KEY, kind TEXT)",
		"CREATE TABLE log(id INTEGER PRIMARY KEY, found INTEGER)",
		`CREATE TRIGGER trg_sel2 AFTER INSERT ON ev
			BEGIN
				INSERT INTO log(found)
					SELECT COUNT(*) FROM ev WHERE kind = NEW.kind;
			END`,
		"INSERT INTO ev(kind) VALUES('click')",
		"INSERT INTO ev(kind) VALUES('view')",
	})
	var cnt int64
	if err := db.QueryRow("SELECT COUNT(*) FROM log").Scan(&cnt); err != nil {
		t.Fatalf("query: %v", err)
	}
	if cnt != 2 {
		t.Errorf("substituteSelect WHERE NEW.col: got %d log rows, want 2", cnt)
	}
}

// TestValueToLiteralIntPath exercises valueToLiteral via an AFTER INSERT trigger
// that reads back NEW.val (integer) inside a SELECT in the trigger body.
func TestValueToLiteralIntPath(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)",
		"CREATE TABLE echo(id INTEGER PRIMARY KEY, v INTEGER)",
		`CREATE TRIGGER trg_int AFTER INSERT ON nums
			BEGIN
				INSERT INTO echo(v) VALUES(NEW.v);
			END`,
		"INSERT INTO nums(v) VALUES(42)",
	})
	n := queryInt64(t, db, "SELECT v FROM echo")
	if n != 42 {
		t.Errorf("valueToLiteral int: got %d, want 42", n)
	}
}

// TestValueToLiteralStringPath exercises valueToLiteral with a string value
// via an AFTER INSERT trigger inserting NEW.txt into a log table.
func TestValueToLiteralStringPath(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE msgs(id INTEGER PRIMARY KEY, txt TEXT)",
		"CREATE TABLE copy(id INTEGER PRIMARY KEY, txt TEXT)",
		`CREATE TRIGGER trg_str AFTER INSERT ON msgs
			BEGIN
				INSERT INTO copy(txt) VALUES(NEW.txt);
			END`,
		"INSERT INTO msgs(txt) VALUES('hello')",
	})
	var got string
	if err := db.QueryRow("SELECT txt FROM copy").Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got != "hello" {
		t.Errorf("valueToLiteral string: got %q, want %q", got, "hello")
	}
}

// TestValueToLiteralNullPath exercises valueToLiteral with a NULL value by
// inserting a NULL column via an AFTER INSERT trigger.
func TestValueToLiteralNullPath(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE nullable(id INTEGER PRIMARY KEY, v INTEGER)",
		"CREATE TABLE nullcopy(id INTEGER PRIMARY KEY, v INTEGER)",
		`CREATE TRIGGER trg_null AFTER INSERT ON nullable
			BEGIN
				INSERT INTO nullcopy(v) VALUES(NEW.v);
			END`,
		"INSERT INTO nullable(v) VALUES(NULL)",
	})
	var got sql.NullInt64
	if err := db.QueryRow("SELECT v FROM nullcopy").Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got.Valid {
		t.Errorf("valueToLiteral null: expected NULL, got %d", got.Int64)
	}
}

// TestValueToLiteralFloatPath exercises valueToLiteral with a float64 value
// by inserting a REAL column via an AFTER INSERT trigger.
func TestValueToLiteralFloatPath(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE floats(id INTEGER PRIMARY KEY, v REAL)",
		"CREATE TABLE floatcopy(id INTEGER PRIMARY KEY, v REAL)",
		`CREATE TRIGGER trg_float AFTER INSERT ON floats
			BEGIN
				INSERT INTO floatcopy(v) VALUES(NEW.v);
			END`,
		"INSERT INTO floats(v) VALUES(3.14)",
	})
	var got float64
	if err := db.QueryRow("SELECT v FROM floatcopy").Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got < 3.0 || got > 4.0 {
		t.Errorf("valueToLiteral float: got %f, want ~3.14", got)
	}
}
