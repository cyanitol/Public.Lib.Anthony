// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openTriggerRuntimeDB opens an isolated in-memory database for trigger tests.
func openTriggerRuntimeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver.DriverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// mustExecTRC executes SQL and fails the test on error.
func mustExecTRC(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// querySingleInt scans a single integer from a query result.
func querySingleInt(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// querySingleString scans a single string from a query result.
func querySingleString(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// TestTriggerRuntime_AfterInsertNewColInSelect exercises substituteSelect via
// a trigger body that uses INSERT...SELECT with a WHERE clause referencing NEW.col.
func TestTriggerRuntime_AfterInsertNewColInSelect(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE src(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, cnt INTEGER)")
	// The INSERT...SELECT body statement exercises substituteSelect when the
	// SELECT has a WHERE referencing NEW.
	mustExecTRC(t, db, `CREATE TRIGGER trg_sel AFTER INSERT ON src
		BEGIN
			INSERT INTO log(cnt) SELECT COUNT(*) FROM src WHERE v < NEW.v;
		END`)
	mustExecTRC(t, db, "INSERT INTO src(v) VALUES(10)")
	mustExecTRC(t, db, "INSERT INTO src(v) VALUES(20)")

	n := querySingleInt(t, db, "SELECT COUNT(*) FROM log")
	if n != 2 {
		t.Errorf("substituteSelect: want 2 log rows, got %d", n)
	}
}

// TestTriggerRuntime_AfterUpdateNewOldInWhere exercises substituteUpdate and
// substituteIdent: the trigger UPDATE body references both NEW and OLD.
func TestTriggerRuntime_AfterUpdateNewOldInWhere(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE shadow(id INTEGER PRIMARY KEY, old_v INTEGER, new_v INTEGER)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_upd AFTER UPDATE ON items
		BEGIN
			INSERT INTO shadow(old_v, new_v) VALUES(OLD.v, NEW.v);
		END`)
	mustExecTRC(t, db, "INSERT INTO items(v) VALUES(10)")
	mustExecTRC(t, db, "UPDATE items SET v = 20 WHERE id = 1")

	var oldV, newV int64
	if err := db.QueryRow("SELECT old_v, new_v FROM shadow").Scan(&oldV, &newV); err != nil {
		t.Fatalf("query shadow: %v", err)
	}
	if oldV != 10 || newV != 20 {
		t.Errorf("substituteUpdate/substituteIdent: want old=10 new=20, got old=%d new=%d", oldV, newV)
	}
}

// TestTriggerRuntime_AfterUpdateCaseExpression exercises substituteCase via a
// CASE expression in the trigger body that references NEW and OLD columns.
func TestTriggerRuntime_AfterUpdateCaseExpression(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE scores(id INTEGER PRIMARY KEY, s INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, grade TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_case AFTER UPDATE ON scores
		BEGIN
			INSERT INTO log(grade) VALUES(
				CASE
					WHEN NEW.s >= 90 THEN 'A'
					WHEN NEW.s >= 80 THEN 'B'
					ELSE 'C'
				END
			);
		END`)
	mustExecTRC(t, db, "INSERT INTO scores(s) VALUES(50)")
	mustExecTRC(t, db, "UPDATE scores SET s = 95 WHERE id = 1")

	grade := querySingleString(t, db, "SELECT grade FROM log")
	if grade != "A" {
		t.Errorf("substituteCase: want grade='A', got %q", grade)
	}
}

// TestTriggerRuntime_AfterDeleteSubstituteDelete exercises substituteDelete:
// the trigger body contains a DELETE with a WHERE clause referencing OLD.col.
func TestTriggerRuntime_AfterDeleteSubstituteDelete(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE primary_t(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE backup_t(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "INSERT INTO primary_t(id, v) VALUES(1, 42)")
	mustExecTRC(t, db, "INSERT INTO backup_t(id, v) VALUES(1, 42)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_del AFTER DELETE ON primary_t
		BEGIN
			DELETE FROM backup_t WHERE v = OLD.v;
		END`)
	mustExecTRC(t, db, "DELETE FROM primary_t WHERE id = 1")

	n := querySingleInt(t, db, "SELECT COUNT(*) FROM backup_t")
	if n != 0 {
		t.Errorf("substituteDelete: want 0 backup rows, got %d", n)
	}
}

// TestTriggerRuntime_CastNewCol exercises substituteCast via CAST(NEW.col AS TEXT).
func TestTriggerRuntime_CastNewCol(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, txt TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_cast AFTER INSERT ON nums
		BEGIN
			INSERT INTO log(txt) VALUES(CAST(NEW.v AS TEXT));
		END`)
	mustExecTRC(t, db, "INSERT INTO nums(v) VALUES(77)")

	txt := querySingleString(t, db, "SELECT txt FROM log")
	if txt != "77" {
		t.Errorf("substituteCast: want '77', got %q", txt)
	}
}

// TestTriggerRuntime_BinaryExprNewANewB exercises substituteBinary via
// arithmetic on two NEW columns inside the trigger body.
func TestTriggerRuntime_BinaryExprNewANewB(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE pairs(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE sums(id INTEGER PRIMARY KEY, total INTEGER)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_bin AFTER INSERT ON pairs
		BEGIN
			INSERT INTO sums(total) VALUES(NEW.a + NEW.b);
		END`)
	mustExecTRC(t, db, "INSERT INTO pairs(a, b) VALUES(3, 4)")

	total := querySingleInt(t, db, "SELECT total FROM sums")
	if total != 7 {
		t.Errorf("substituteBinary: want 7, got %d", total)
	}
}

// TestTriggerRuntime_UnaryNotNewCol exercises substituteUnary via NOT NEW.col
// inside the trigger WHEN clause (which uses a boolean unary).
func TestTriggerRuntime_UnaryNotNewCol(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE flags(id INTEGER PRIMARY KEY, active INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_unary AFTER INSERT ON flags
		BEGIN
			INSERT INTO log(note) VALUES(CASE WHEN NOT NEW.active THEN 'inactive' ELSE 'active' END);
		END`)
	mustExecTRC(t, db, "INSERT INTO flags(active) VALUES(0)")

	note := querySingleString(t, db, "SELECT note FROM log")
	if note != "inactive" {
		t.Errorf("substituteUnary: want 'inactive', got %q", note)
	}
}

// TestTriggerRuntime_FunctionCallNewCol exercises substituteFunction via
// abs(NEW.col) in the trigger body.
func TestTriggerRuntime_FunctionCallNewCol(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, absval INTEGER)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_func AFTER INSERT ON nums
		BEGIN
			INSERT INTO log(absval) VALUES(abs(NEW.v));
		END`)
	mustExecTRC(t, db, "INSERT INTO nums(v) VALUES(-9)")

	absval := querySingleInt(t, db, "SELECT absval FROM log")
	if absval != 9 {
		t.Errorf("substituteFunction: want 9, got %d", absval)
	}
}

// TestTriggerRuntime_NewColInExpr exercises substituteIn via NEW.col IN (list).
func TestTriggerRuntime_NewColInExpr(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE colors(id INTEGER PRIMARY KEY, c TEXT)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_in AFTER INSERT ON colors
		BEGIN
			INSERT INTO log(note) VALUES(
				CASE WHEN NEW.c IN ('red', 'green', 'blue') THEN 'primary' ELSE 'other' END
			);
		END`)
	mustExecTRC(t, db, "INSERT INTO colors(c) VALUES('green')")

	note := querySingleString(t, db, "SELECT note FROM log")
	if note != "primary" {
		t.Errorf("substituteIn: want 'primary', got %q", note)
	}
}

// TestTriggerRuntime_NewColBetween exercises substituteBetween via
// NEW.col BETWEEN x AND y inside the trigger body.
func TestTriggerRuntime_NewColBetween(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_between AFTER INSERT ON nums
		BEGIN
			INSERT INTO log(note) VALUES(
				CASE WHEN NEW.v BETWEEN 1 AND 10 THEN 'in_range' ELSE 'out_range' END
			);
		END`)
	mustExecTRC(t, db, "INSERT INTO nums(v) VALUES(5)")

	note := querySingleString(t, db, "SELECT note FROM log")
	if note != "in_range" {
		t.Errorf("substituteBetween: want 'in_range', got %q", note)
	}
}

// TestTriggerRuntime_ParenthesizedNewCol exercises substituteParen via
// (NEW.col) in the trigger body.
func TestTriggerRuntime_ParenthesizedNewCol(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, val INTEGER)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_paren AFTER INSERT ON nums
		BEGIN
			INSERT INTO log(val) VALUES((NEW.v));
		END`)
	mustExecTRC(t, db, "INSERT INTO nums(v) VALUES(13)")

	val := querySingleInt(t, db, "SELECT val FROM log")
	if val != 13 {
		t.Errorf("substituteParen: want 13, got %d", val)
	}
}

// TestTriggerRuntime_CollateNOCASE exercises substituteCollate via
// OLD.col COLLATE NOCASE inside the trigger body.
func TestTriggerRuntime_CollateNOCASE(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE words(id INTEGER PRIMARY KEY, w TEXT)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_collate AFTER UPDATE ON words
		BEGIN
			INSERT INTO log(note) VALUES(
				CASE WHEN OLD.w COLLATE NOCASE = 'hello' THEN 'matched' ELSE 'no' END
			);
		END`)
	mustExecTRC(t, db, "INSERT INTO words(w) VALUES('HELLO')")
	mustExecTRC(t, db, "UPDATE words SET w = 'world'")

	note := querySingleString(t, db, "SELECT note FROM log")
	if note != "matched" {
		t.Errorf("substituteCollate: want 'matched', got %q", note)
	}
}

// TestTriggerRuntime_SelectStatementInBody exercises the substituteSelect path
// by using a trigger body with INSERT...SELECT WHERE referencing NEW.col.
func TestTriggerRuntime_SelectStatementInBody(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE events(id INTEGER PRIMARY KEY, kind TEXT)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, found INTEGER)")
	// The SELECT in the INSERT body has a WHERE referencing NEW.kind,
	// exercising the substituteSelect WHERE substitution path.
	mustExecTRC(t, db, `CREATE TRIGGER trg_sub AFTER INSERT ON events
		BEGIN
			INSERT INTO log(found) SELECT COUNT(*) FROM events WHERE kind = NEW.kind;
		END`)
	mustExecTRC(t, db, "INSERT INTO events(kind) VALUES('click')")
	mustExecTRC(t, db, "INSERT INTO events(kind) VALUES('view')")

	n := querySingleInt(t, db, "SELECT COUNT(*) FROM log")
	if n != 2 {
		t.Errorf("substituteSelect body: want 2 log rows, got %d", n)
	}
}

// TestTriggerRuntime_CaseInsensitiveLookup exercises caseInsensitiveLookup by
// referencing NEW columns with mixed-case table qualifiers and column names.
// The trigger uses "new.V" (lowercase qualifier, uppercase column name) to
// trigger case-insensitive map lookup.
func TestTriggerRuntime_CaseInsensitiveLookup(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, V INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, val INTEGER)")
	// Column name in map will be "V", but trigger references "new.v" (lowercase)
	// which should find it via caseInsensitiveLookup.
	mustExecTRC(t, db, `CREATE TRIGGER trg_ci AFTER INSERT ON nums
		BEGIN
			INSERT INTO log(val) VALUES(new.v);
		END`)
	mustExecTRC(t, db, "INSERT INTO nums(V) VALUES(99)")

	val := querySingleInt(t, db, "SELECT val FROM log")
	if val != 99 {
		t.Errorf("caseInsensitiveLookup: want 99, got %d", val)
	}
}

// TestTriggerRuntime_ValueToLiteralTypes exercises valueToLiteral for multiple
// column types by inserting various types and capturing them via trigger.
func TestTriggerRuntime_ValueToLiteralTypes(t *testing.T) {
	cases := []struct {
		name     string
		colType  string
		insertV  string
		wantText string
	}{
		{"integer", "INTEGER", "42", "42"},
		{"real", "REAL", "3.14", "3.14"},
		{"text", "TEXT", "'hello'", "hello"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := openTriggerRuntimeDB(t)

			mustExecTRC(t, db, fmt.Sprintf("CREATE TABLE src(id INTEGER PRIMARY KEY, v %s)", tc.colType))
			mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, val TEXT)")
			mustExecTRC(t, db, `CREATE TRIGGER trg_vtl AFTER INSERT ON src
				BEGIN
					INSERT INTO log(val) VALUES(CAST(NEW.v AS TEXT));
				END`)
			mustExecTRC(t, db, fmt.Sprintf("INSERT INTO src(v) VALUES(%s)", tc.insertV))

			got := querySingleString(t, db, "SELECT val FROM log")
			if !strings.HasPrefix(got, tc.wantText[:1]) {
				t.Errorf("valueToLiteral %s: want prefix %q, got %q", tc.name, tc.wantText, got)
			}
		})
	}
}

// TestTriggerRuntime_NullValueToLiteral exercises the nil branch of
// valueToLiteral by inserting a NULL value and capturing it via trigger.
func TestTriggerRuntime_NullValueToLiteral(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE src(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, was_null INTEGER)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_null AFTER INSERT ON src
		BEGIN
			INSERT INTO log(was_null) VALUES(CASE WHEN NEW.v IS NULL THEN 1 ELSE 0 END);
		END`)
	mustExecTRC(t, db, "INSERT INTO src(v) VALUES(NULL)")

	wasnull := querySingleInt(t, db, "SELECT was_null FROM log")
	if wasnull != 1 {
		t.Errorf("valueToLiteral nil: want 1 (is null), got %d", wasnull)
	}
}

// TestTriggerRuntime_ExtractRowMapsNilTriggerRow exercises the nil-guard in
// extractRowMaps by ensuring a trigger with no OLD/NEW references still fires.
func TestTriggerRuntime_ExtractRowMapsNilTriggerRow(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_noref AFTER INSERT ON t
		BEGIN
			INSERT INTO log(note) VALUES('fired');
		END`)
	mustExecTRC(t, db, "INSERT INTO t(v) VALUES(1)")

	n := querySingleInt(t, db, "SELECT COUNT(*) FROM log")
	if n != 1 {
		t.Errorf("extractRowMaps nil-guard: want 1 log row, got %d", n)
	}
}

// TestTriggerRuntime_UpdateStatementWithWhereSubstitution exercises substituteUpdate
// with a WHERE clause that references OLD and NEW columns.
func TestTriggerRuntime_UpdateStatementWithWhereSubstitution(t *testing.T) {
	db := openTriggerRuntimeDB(t)

	mustExecTRC(t, db, "CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER)")
	mustExecTRC(t, db, "CREATE TABLE mirror(id INTEGER PRIMARY KEY, balance INTEGER)")
	mustExecTRC(t, db, "INSERT INTO accounts(id, balance) VALUES(1, 100)")
	mustExecTRC(t, db, "INSERT INTO mirror(id, balance) VALUES(1, 100)")
	mustExecTRC(t, db, `CREATE TRIGGER trg_upd_where AFTER UPDATE ON accounts
		BEGIN
			UPDATE mirror SET balance = NEW.balance WHERE id = OLD.id;
		END`)
	mustExecTRC(t, db, "UPDATE accounts SET balance = 200 WHERE id = 1")

	bal := querySingleInt(t, db, "SELECT balance FROM mirror WHERE id = 1")
	if bal != 200 {
		t.Errorf("substituteUpdate WHERE: want 200, got %d", bal)
	}
}
