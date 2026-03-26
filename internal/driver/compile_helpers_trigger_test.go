// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// trigOpenMem opens an in-memory database for trigger coverage tests.
func trigOpenMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("trigOpenMem: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// trigExec executes a SQL statement and fatals on error.
func trigExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// trigQueryInt runs a scalar integer query and returns the result.
func trigQueryInt(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// TestTriggerBeforeInsertFires exercises executeBeforeInsertTriggers and
// prepareNewRowForInsert through a BEFORE INSERT trigger.
func TestTriggerBeforeInsertFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER ti BEFORE INSERT ON t BEGIN INSERT INTO log VALUES('before_insert'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='before_insert'")
	if n != 1 {
		t.Errorf("expected 1 log entry from BEFORE INSERT trigger, got %d", n)
	}
}

// TestTriggerAfterInsertFires exercises executeAfterInsertTriggers through an
// AFTER INSERT trigger, including the prepareNewRowForInsert path.
func TestTriggerAfterInsertFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES('after_insert'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "INSERT INTO t VALUES(2, 'b')")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='after_insert'")
	if n != 2 {
		t.Errorf("expected 2 log entries from AFTER INSERT trigger, got %d", n)
	}
}

// TestTriggerBeforeUpdateFires exercises executeBeforeUpdateTriggers.
func TestTriggerBeforeUpdateFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER tu BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES('before_update'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "UPDATE t SET val='b' WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='before_update'")
	if n != 1 {
		t.Errorf("expected 1 log entry from BEFORE UPDATE trigger, got %d", n)
	}
}

// TestTriggerAfterUpdateFires exercises executeAfterUpdateTriggers.
func TestTriggerAfterUpdateFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER tu AFTER UPDATE ON t BEGIN INSERT INTO log VALUES('after_update'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "UPDATE t SET val='b' WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='after_update'")
	if n != 1 {
		t.Errorf("expected 1 log entry from AFTER UPDATE trigger, got %d", n)
	}
}

// TestTriggerBeforeDeleteFires exercises executeBeforeDeleteTriggers.
func TestTriggerBeforeDeleteFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER td BEFORE DELETE ON t BEGIN INSERT INTO log VALUES('before_delete'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "DELETE FROM t WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='before_delete'")
	if n != 1 {
		t.Errorf("expected 1 log entry from BEFORE DELETE trigger, got %d", n)
	}
}

// TestTriggerAfterDeleteFires exercises executeAfterDeleteTriggers.
func TestTriggerAfterDeleteFires(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER td AFTER DELETE ON t BEGIN INSERT INTO log VALUES('after_delete'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "DELETE FROM t WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='after_delete'")
	if n != 1 {
		t.Errorf("expected 1 log entry from AFTER DELETE trigger, got %d", n)
	}
}

// TestTriggerNoTriggersNoOp verifies that tables without triggers do not error
// (the early-return nil path in all execute*Triggers functions).
func TestTriggerNoTriggersNoOp(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	// These should succeed with no triggers defined.
	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "UPDATE t SET val='b' WHERE id=1")
	trigExec(t, db, "DELETE FROM t WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM t")
	if n != 0 {
		t.Errorf("expected 0 rows after delete, got %d", n)
	}
}

// TestTriggerInsertWithExplicitColumns exercises prepareNewRowForInsert with
// explicit column lists in the INSERT statement.
func TestTriggerInsertWithExplicitColumns(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER ti AFTER INSERT ON t BEGIN INSERT INTO log VALUES('inserted'); END`)

	// INSERT with explicit column list — exercises the colNames branch in prepareNewRowForInsert.
	trigExec(t, db, "INSERT INTO t(name, score) VALUES('alice', 9.5)")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log WHERE evt='inserted'")
	if n != 1 {
		t.Errorf("expected 1 log entry, got %d", n)
	}
}

// TestTriggerAllTimingsOnSameTable exercises all six execute* functions on
// the same table to maximise path coverage in a single test.
func TestTriggerAllTimingsOnSameTable(t *testing.T) {
	db := trigOpenMem(t)

	trigExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	trigExec(t, db, "CREATE TABLE log (evt TEXT)")
	trigExec(t, db, `CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN INSERT INTO log VALUES('bi'); END`)
	trigExec(t, db, `CREATE TRIGGER t_ai AFTER  INSERT ON t BEGIN INSERT INTO log VALUES('ai'); END`)
	trigExec(t, db, `CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES('bu'); END`)
	trigExec(t, db, `CREATE TRIGGER t_au AFTER  UPDATE ON t BEGIN INSERT INTO log VALUES('au'); END`)
	trigExec(t, db, `CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN INSERT INTO log VALUES('bd'); END`)
	trigExec(t, db, `CREATE TRIGGER t_ad AFTER  DELETE ON t BEGIN INSERT INTO log VALUES('ad'); END`)

	trigExec(t, db, "INSERT INTO t VALUES(1, 'a')")
	trigExec(t, db, "UPDATE t SET val='b' WHERE id=1")
	trigExec(t, db, "DELETE FROM t WHERE id=1")

	n := trigQueryInt(t, db, "SELECT COUNT(*) FROM log")
	if n != 6 {
		t.Errorf("expected 6 trigger log entries, got %d", n)
	}
}
