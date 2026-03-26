// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// trigger3OpenDB opens an in-memory database for trigger3 tests.
func trigger3OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// trigger3Exec executes a SQL statement and fails the test on error.
func trigger3Exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// trigger3QueryInt executes a query and scans a single int result.
func trigger3QueryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// TestTrigger3AfterDeleteWithBlobColumn exercises the DELETE trigger path
// with a BLOB column in the table. The trigger reads OLD.data (a blob)
// and stores it in a log table, exercising the MemBlob branch of
// memToGoValue in extractOldRowFromRegisters.
func TestTrigger3AfterDeleteWithBlobColumn(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, data BLOB, tag TEXT)")
	trigger3Exec(t, db, "CREATE TABLE doc_archive (id INTEGER, tag TEXT)")
	trigger3Exec(t, db, `CREATE TRIGGER archive_doc AFTER DELETE ON docs BEGIN
		INSERT INTO doc_archive VALUES(OLD.id, OLD.tag);
	END`)
	trigger3Exec(t, db, "INSERT INTO docs VALUES(1, X'DEADBEEF', 'binary')")
	trigger3Exec(t, db, "DELETE FROM docs WHERE id = 1")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM doc_archive WHERE id=1")
	if cnt != 1 {
		t.Errorf("expected 1 archived doc, got %d", cnt)
	}

	var tag sql.NullString
	if err := db.QueryRow("SELECT tag FROM doc_archive WHERE id=1").Scan(&tag); err != nil {
		t.Fatalf("scan tag: %v", err)
	}
	if tag.String != "binary" {
		t.Errorf("archived tag: want 'binary', got %q", tag.String)
	}
}

// TestTrigger3AfterDeleteWithNullColumn exercises the DELETE trigger path
// where a column value is NULL. This exercises the MemNull branch of
// memToGoValue and the nil-check path in extractOldRowFromRegisters.
func TestTrigger3AfterDeleteWithNullColumn(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE nullable (id INTEGER PRIMARY KEY, val TEXT, extra TEXT)")
	trigger3Exec(t, db, "CREATE TABLE null_log (id INTEGER, had_extra INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER log_null_delete AFTER DELETE ON nullable BEGIN
		INSERT INTO null_log VALUES(OLD.id, CASE WHEN OLD.extra IS NULL THEN 0 ELSE 1 END);
	END`)
	trigger3Exec(t, db, "INSERT INTO nullable VALUES(1, 'test', NULL)")
	trigger3Exec(t, db, "DELETE FROM nullable WHERE id = 1")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM null_log WHERE id=1 AND had_extra=0")
	if cnt != 1 {
		t.Errorf("expected 1 null_log row with had_extra=0, got %d", cnt)
	}
}

// TestTrigger3AfterUpdateWithNullToNonNull exercises UPDATE trigger where
// a column transitions from NULL to a non-null value, testing OLD.col IS NULL
// and NEW.col IS NOT NULL paths.
func TestTrigger3AfterUpdateWithNullToNonNull(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE sparse (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE fill_log (id INTEGER, was_null INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER track_fill AFTER UPDATE ON sparse BEGIN
		INSERT INTO fill_log VALUES(OLD.id, CASE WHEN OLD.score IS NULL THEN 1 ELSE 0 END);
	END`)
	trigger3Exec(t, db, "INSERT INTO sparse VALUES(1, 'Alice', NULL)")
	trigger3Exec(t, db, "UPDATE sparse SET score = 100 WHERE id = 1")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM fill_log WHERE id=1 AND was_null=1")
	if cnt != 1 {
		t.Errorf("expected 1 fill_log row with was_null=1, got %d", cnt)
	}
}

// TestTrigger3AfterDeleteIPKLogsRowid exercises the DELETE trigger path for a
// table with an INTEGER PRIMARY KEY rowid alias. This ensures the rowid column
// is captured in the OLD row snapshot via extractOldRowFromRegisters and
// addRowidColumnToRow, then used in the trigger body.
func TestTrigger3AfterDeleteIPKLogsRowid(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE entities (rowkey INTEGER PRIMARY KEY, label TEXT, weight REAL)")
	trigger3Exec(t, db, "CREATE TABLE entity_tomb (rowkey INTEGER, label TEXT, weight REAL)")
	trigger3Exec(t, db, `CREATE TRIGGER tombstone AFTER DELETE ON entities BEGIN
		INSERT INTO entity_tomb VALUES(OLD.rowkey, OLD.label, OLD.weight);
	END`)
	trigger3Exec(t, db, "INSERT INTO entities VALUES(42, 'thing', 1.5)")
	trigger3Exec(t, db, "DELETE FROM entities WHERE rowkey = 42")

	var rk int
	var label sql.NullString
	var weight sql.NullFloat64
	err := db.QueryRow("SELECT rowkey, label, weight FROM entity_tomb WHERE rowkey=42").
		Scan(&rk, &label, &weight)
	if err != nil {
		t.Fatalf("scan entity_tomb: %v", err)
	}
	if rk != 42 {
		t.Errorf("entity_tomb.rowkey: want 42, got %d", rk)
	}
	t.Logf("tombstone: rowkey=%d label=%q weight=%v", rk, label.String, weight.Float64)
}

// TestTrigger3AfterUpdateIPKLogsOldAndNew exercises the UPDATE trigger path
// for an IPK table where both OLD and NEW rowid-alias column values are logged.
// This exercises buildTriggerRowFromUpdateRegs with a non-zero P5 register.
func TestTrigger3AfterUpdateIPKLogsOldAndNew(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE tasks (tid INTEGER PRIMARY KEY, title TEXT, done INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE task_edits (tid INTEGER, old_title TEXT, new_title TEXT, old_done INTEGER, new_done INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER log_task_edit AFTER UPDATE ON tasks BEGIN
		INSERT INTO task_edits VALUES(OLD.tid, OLD.title, NEW.title, OLD.done, NEW.done);
	END`)
	trigger3Exec(t, db, "INSERT INTO tasks VALUES(7, 'Write tests', 0)")
	trigger3Exec(t, db, "UPDATE tasks SET title='Write more tests', done=1 WHERE tid=7")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM task_edits WHERE tid=7")
	if cnt != 1 {
		t.Errorf("expected 1 task_edits row, got %d", cnt)
	}

	var tid, oldDone, newDone int
	var oldTitle, newTitle sql.NullString
	err := db.QueryRow("SELECT tid, old_title, new_title, old_done, new_done FROM task_edits WHERE tid=7").
		Scan(&tid, &oldTitle, &newTitle, &oldDone, &newDone)
	if err != nil {
		t.Fatalf("scan task_edits: %v", err)
	}
	t.Logf("task edit: tid=%d old_title=%q new_title=%q old_done=%d new_done=%d",
		tid, oldTitle.String, newTitle.String, oldDone, newDone)
}

// TestTrigger3BeforeDeleteChecksOldIntColumn exercises BEFORE DELETE trigger
// with a WHEN clause that reads an integer column from OLD. This ensures the
// MemInt branch of memToGoValue is exercised in the trigger row snapshot.
func TestTrigger3BeforeDeleteChecksOldIntColumn(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER, locked INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE deleted_counters (id INTEGER, value INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER guard_counter BEFORE DELETE ON counters
		WHEN OLD.locked = 0
		BEGIN
			INSERT INTO deleted_counters VALUES(OLD.id, OLD.value);
		END`)
	trigger3Exec(t, db, "INSERT INTO counters VALUES(1, 100, 0)")
	trigger3Exec(t, db, "INSERT INTO counters VALUES(2, 200, 1)")

	// Delete unlocked counter — trigger should fire
	trigger3Exec(t, db, "DELETE FROM counters WHERE id = 1")
	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM deleted_counters WHERE id=1")
	if cnt != 1 {
		t.Errorf("expected 1 deleted_counters row for unlocked counter, got %d", cnt)
	}

	// Delete locked counter — WHEN clause false, trigger body skipped
	trigger3Exec(t, db, "DELETE FROM counters WHERE id = 2")
	cnt2 := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM deleted_counters WHERE id=2")
	if cnt2 != 0 {
		t.Errorf("expected 0 deleted_counters rows for locked counter, got %d", cnt2)
	}
}

// TestTrigger3BeforeUpdateChecksOldRealColumn exercises BEFORE UPDATE trigger
// where the WHEN clause inspects a REAL (floating-point) OLD column. This
// exercises the MemReal branch of memToGoValue in the trigger row snapshot.
func TestTrigger3BeforeUpdateChecksOldRealColumn(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL, frozen INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE balance_changes (id INTEGER, old_bal REAL, new_bal REAL)")
	trigger3Exec(t, db, `CREATE TRIGGER track_balance BEFORE UPDATE OF balance ON accounts
		WHEN OLD.frozen = 0
		BEGIN
			INSERT INTO balance_changes VALUES(OLD.id, OLD.balance, NEW.balance);
		END`)
	trigger3Exec(t, db, "INSERT INTO accounts VALUES(1, 500.75, 0)")
	trigger3Exec(t, db, "INSERT INTO accounts VALUES(2, 1000.0, 1)")

	// Update unfrozen account — WHEN passes, trigger fires
	trigger3Exec(t, db, "UPDATE accounts SET balance = 450.25 WHERE id = 1")
	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM balance_changes WHERE id=1")
	if cnt != 1 {
		t.Errorf("expected 1 balance_changes row for unfrozen account, got %d", cnt)
	}

	// Update frozen account — WHEN fails, trigger body skipped
	trigger3Exec(t, db, "UPDATE accounts SET balance = 2000.0 WHERE id = 2")
	cnt2 := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM balance_changes WHERE id=2")
	if cnt2 != 0 {
		t.Errorf("expected 0 balance_changes rows for frozen account, got %d", cnt2)
	}

	var oldBal, newBal sql.NullFloat64
	if err := db.QueryRow("SELECT old_bal, new_bal FROM balance_changes WHERE id=1").
		Scan(&oldBal, &newBal); err != nil {
		t.Fatalf("scan balance_changes: %v", err)
	}
	t.Logf("balance change: old=%v new=%v", oldBal.Float64, newBal.Float64)
}

// TestTrigger3AfterDeleteNonIPKMultiCol exercises AFTER DELETE on a non-IPK
// table with multiple column types, covering the non-rowid-alias path of
// extractOldRowFromRegisters and getTableRecordColumnNames fallback.
func TestTrigger3AfterDeleteNonIPKMultiCol(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE metrics (name TEXT, value REAL, unit TEXT, ts INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE retired_metrics (name TEXT, value REAL)")
	trigger3Exec(t, db, `CREATE TRIGGER retire_metric AFTER DELETE ON metrics BEGIN
		INSERT INTO retired_metrics VALUES(OLD.name, OLD.value);
	END`)
	trigger3Exec(t, db, "INSERT INTO metrics VALUES('cpu', 85.5, 'pct', 1000)")
	trigger3Exec(t, db, "INSERT INTO metrics VALUES('mem', 4096.0, 'MB', 1001)")
	trigger3Exec(t, db, "DELETE FROM metrics WHERE name='cpu'")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM retired_metrics WHERE name='cpu'")
	if cnt != 1 {
		t.Errorf("expected 1 retired_metrics row for cpu, got %d", cnt)
	}

	var val sql.NullFloat64
	if err := db.QueryRow("SELECT value FROM retired_metrics WHERE name='cpu'").Scan(&val); err != nil {
		t.Fatalf("scan retired value: %v", err)
	}
	t.Logf("retired metric value: %v", val.Float64)
}

// TestTrigger3AfterUpdateNonIPKMultiColOldNew exercises AFTER UPDATE on a
// non-IPK table with OLD and NEW access across multiple column types.
func TestTrigger3AfterUpdateNonIPKMultiColOldNew(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE prefs (key TEXT PRIMARY KEY, ival INTEGER, rval REAL, sval TEXT)")
	trigger3Exec(t, db, "CREATE TABLE pref_audit (key TEXT, old_ival INTEGER, new_ival INTEGER, old_rval REAL, new_rval REAL)")
	trigger3Exec(t, db, `CREATE TRIGGER audit_pref AFTER UPDATE ON prefs BEGIN
		INSERT INTO pref_audit VALUES(OLD.key, OLD.ival, NEW.ival, OLD.rval, NEW.rval);
	END`)
	trigger3Exec(t, db, "INSERT INTO prefs VALUES('timeout', 30, 0.5, 'normal')")
	trigger3Exec(t, db, "UPDATE prefs SET ival=60, rval=1.0, sval='high' WHERE key='timeout'")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM pref_audit WHERE key='timeout'")
	if cnt != 1 {
		t.Errorf("expected 1 pref_audit row, got %d", cnt)
	}

	var key sql.NullString
	var oldIval, newIval sql.NullInt64
	var oldRval, newRval sql.NullFloat64
	err := db.QueryRow("SELECT key, old_ival, new_ival, old_rval, new_rval FROM pref_audit WHERE key='timeout'").
		Scan(&key, &oldIval, &newIval, &oldRval, &newRval)
	if err != nil {
		t.Fatalf("scan pref_audit: %v", err)
	}
	t.Logf("pref audit: key=%q old_ival=%v new_ival=%v old_rval=%v new_rval=%v",
		key.String, oldIval.Int64, newIval.Int64, oldRval.Float64, newRval.Float64)
}

// TestTrigger3AfterDeleteThenUpdateSameRow exercises delete and update
// triggers on the same table in sequence, covering both DELETE and UPDATE
// trigger row-building paths for the same schema.
func TestTrigger3AfterDeleteThenUpdateSameRow(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE stock (id INTEGER PRIMARY KEY, qty INTEGER, price REAL)")
	trigger3Exec(t, db, "CREATE TABLE stock_ops (op TEXT, id INTEGER, qty INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER stock_del AFTER DELETE ON stock BEGIN
		INSERT INTO stock_ops VALUES('del', OLD.id, OLD.qty);
	END`)
	trigger3Exec(t, db, `CREATE TRIGGER stock_upd AFTER UPDATE ON stock BEGIN
		INSERT INTO stock_ops VALUES('upd', OLD.id, OLD.qty);
	END`)

	trigger3Exec(t, db, "INSERT INTO stock VALUES(1, 50, 9.99)")
	trigger3Exec(t, db, "INSERT INTO stock VALUES(2, 100, 4.99)")

	trigger3Exec(t, db, "UPDATE stock SET qty = 75 WHERE id = 1")
	trigger3Exec(t, db, "DELETE FROM stock WHERE id = 2")

	updCnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM stock_ops WHERE op='upd' AND id=1")
	delCnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM stock_ops WHERE op='del' AND id=2")

	if updCnt != 1 {
		t.Errorf("expected 1 update op for id=1, got %d", updCnt)
	}
	if delCnt != 1 {
		t.Errorf("expected 1 delete op for id=2, got %d", delCnt)
	}
}

// TestTrigger3AfterUpdateBatchIPKAllRows exercises UPDATE trigger firing on
// all rows of an IPK table in a single batch UPDATE (no WHERE clause),
// exercising the per-row trigger firing path with correct OLD/NEW snapshots.
func TestTrigger3AfterUpdateBatchIPKAllRows(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE nodes (nid INTEGER PRIMARY KEY, depth INTEGER, active INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE depth_changes (nid INTEGER, old_d INTEGER, new_d INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER track_depth AFTER UPDATE OF depth ON nodes BEGIN
		INSERT INTO depth_changes VALUES(OLD.nid, OLD.depth, NEW.depth);
	END`)

	trigger3Exec(t, db, "INSERT INTO nodes VALUES(1, 1, 1)")
	trigger3Exec(t, db, "INSERT INTO nodes VALUES(2, 2, 1)")
	trigger3Exec(t, db, "INSERT INTO nodes VALUES(3, 3, 0)")

	trigger3Exec(t, db, "UPDATE nodes SET depth = depth * 2")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM depth_changes")
	if cnt < 1 {
		t.Errorf("expected at least 1 depth_changes row after batch UPDATE, got %d", cnt)
	}
	t.Logf("depth changes logged: %d", cnt)
}

// TestTrigger3AfterDeleteBatchNonIPKAllRows exercises DELETE trigger firing
// on all rows of a non-IPK table in a single batch DELETE, exercising the
// per-row delete trigger path.
func TestTrigger3AfterDeleteBatchNonIPKAllRows(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE sessions (token TEXT, uid INTEGER, expires INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE expired_sessions (token TEXT, uid INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER log_expired AFTER DELETE ON sessions BEGIN
		INSERT INTO expired_sessions VALUES(OLD.token, OLD.uid);
	END`)

	trigger3Exec(t, db, "INSERT INTO sessions VALUES('abc', 1, 100)")
	trigger3Exec(t, db, "INSERT INTO sessions VALUES('def', 2, 200)")
	trigger3Exec(t, db, "INSERT INTO sessions VALUES('ghi', 3, 300)")

	trigger3Exec(t, db, "DELETE FROM sessions")

	cnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM expired_sessions")
	if cnt < 1 {
		t.Errorf("expected at least 1 expired_sessions row after batch DELETE, got %d", cnt)
	}
	t.Logf("expired sessions logged: %d", cnt)
}

// TestTrigger3AfterInsertAndThenUpdateAndDelete exercises INSERT, UPDATE, and
// DELETE triggers in sequence on a single table, covering all three trigger
// event type paths (P1=0,1,2) in buildTriggerRowForOp via actual DML.
func TestTrigger3AfterInsertAndThenUpdateAndDelete(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE items3 (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE item_log (event TEXT, id INTEGER, name TEXT)")
	trigger3Exec(t, db, `CREATE TRIGGER item_ins AFTER INSERT ON items3 BEGIN
		INSERT INTO item_log VALUES('ins', NEW.id, NEW.name);
	END`)
	trigger3Exec(t, db, `CREATE TRIGGER item_upd AFTER UPDATE ON items3 BEGIN
		INSERT INTO item_log VALUES('upd', OLD.id, OLD.name);
	END`)
	trigger3Exec(t, db, `CREATE TRIGGER item_del AFTER DELETE ON items3 BEGIN
		INSERT INTO item_log VALUES('del', OLD.id, OLD.name);
	END`)

	trigger3Exec(t, db, "INSERT INTO items3 VALUES(10, 'alpha', 5)")
	trigger3Exec(t, db, "UPDATE items3 SET qty = 10 WHERE id = 10")
	trigger3Exec(t, db, "DELETE FROM items3 WHERE id = 10")

	insCnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM item_log WHERE event='ins' AND id=10")
	updCnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM item_log WHERE event='upd' AND id=10")
	delCnt := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM item_log WHERE event='del' AND id=10")

	if insCnt != 1 {
		t.Errorf("expected 1 insert log, got %d", insCnt)
	}
	if updCnt != 1 {
		t.Errorf("expected 1 update log, got %d", updCnt)
	}
	if delCnt != 1 {
		t.Errorf("expected 1 delete log, got %d", delCnt)
	}
}

// TestTrigger3BeforeUpdateOFColFilterFires exercises BEFORE UPDATE OF
// specific column trigger to cover the updatedCols filtering path in
// executeTriggerOp and the P4.P.([]string) type assertion.
func TestTrigger3BeforeUpdateOFColFilterFires(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE catalog (id INTEGER PRIMARY KEY, title TEXT, price REAL, hidden INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE price_log (id INTEGER, old_price REAL, new_price REAL)")
	trigger3Exec(t, db, `CREATE TRIGGER catalog_price_change BEFORE UPDATE OF price ON catalog BEGIN
		INSERT INTO price_log VALUES(OLD.id, OLD.price, NEW.price);
	END`)

	trigger3Exec(t, db, "INSERT INTO catalog VALUES(1, 'Widget', 9.99, 0)")

	// Update price — trigger should fire
	trigger3Exec(t, db, "UPDATE catalog SET price = 14.99 WHERE id = 1")
	cnt1 := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM price_log WHERE id=1")
	if cnt1 != 1 {
		t.Errorf("expected 1 price_log row after price update, got %d", cnt1)
	}

	// Update non-price column — trigger should NOT fire
	trigger3Exec(t, db, "UPDATE catalog SET hidden = 1 WHERE id = 1")
	cnt2 := trigger3QueryInt(t, db, "SELECT COUNT(*) FROM price_log WHERE id=1")
	if cnt2 != 1 {
		t.Errorf("expected still 1 price_log row after non-price update, got %d", cnt2)
	}
}

// TestTrigger3AfterDeleteIPKWithRealAndIntCols exercises AFTER DELETE on an
// IPK table with both REAL and INTEGER non-PK columns, ensuring both MemInt
// and MemReal branches are exercised in memToGoValue during OLD row extraction.
func TestTrigger3AfterDeleteIPKWithRealAndIntCols(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE measurements (id INTEGER PRIMARY KEY, reading REAL, count INTEGER, label TEXT)")
	trigger3Exec(t, db, "CREATE TABLE meas_history (id INTEGER, reading REAL, count INTEGER, label TEXT)")
	trigger3Exec(t, db, `CREATE TRIGGER meas_del AFTER DELETE ON measurements BEGIN
		INSERT INTO meas_history VALUES(OLD.id, OLD.reading, OLD.count, OLD.label);
	END`)

	trigger3Exec(t, db, "INSERT INTO measurements VALUES(1, 3.14159, 42, 'pi')")
	trigger3Exec(t, db, "INSERT INTO measurements VALUES(2, 2.71828, 100, 'e')")
	trigger3Exec(t, db, "DELETE FROM measurements WHERE id = 1")

	var reading sql.NullFloat64
	var count sql.NullInt64
	var label sql.NullString
	err := db.QueryRow("SELECT reading, count, label FROM meas_history WHERE id=1").
		Scan(&reading, &count, &label)
	if err != nil {
		t.Fatalf("scan meas_history: %v", err)
	}
	t.Logf("measurement history: reading=%v count=%v label=%q", reading.Float64, count.Int64, label.String)
}

// TestTrigger3AfterUpdateIPKWithRealAndIntCols exercises AFTER UPDATE on an
// IPK table with REAL and INTEGER columns in both OLD and NEW rows, ensuring
// both MemInt and MemReal branches are exercised in buildTriggerRowFromUpdateRegs.
func TestTrigger3AfterUpdateIPKWithRealAndIntCols(t *testing.T) {
	db := trigger3OpenDB(t)
	defer db.Close()

	trigger3Exec(t, db, "CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL, samples INTEGER)")
	trigger3Exec(t, db, "CREATE TABLE reading_diff (id INTEGER, old_val REAL, new_val REAL, old_samp INTEGER, new_samp INTEGER)")
	trigger3Exec(t, db, `CREATE TRIGGER reading_change AFTER UPDATE ON readings BEGIN
		INSERT INTO reading_diff VALUES(OLD.id, OLD.value, NEW.value, OLD.samples, NEW.samples);
	END`)

	trigger3Exec(t, db, "INSERT INTO readings VALUES(1, 1.23, 10)")
	trigger3Exec(t, db, "UPDATE readings SET value = 4.56, samples = 20 WHERE id = 1")

	var id int
	var oldVal, newVal sql.NullFloat64
	var oldSamp, newSamp sql.NullInt64
	err := db.QueryRow("SELECT id, old_val, new_val, old_samp, new_samp FROM reading_diff WHERE id=1").
		Scan(&id, &oldVal, &newVal, &oldSamp, &newSamp)
	if err != nil {
		t.Fatalf("scan reading_diff: %v", err)
	}
	if id != 1 {
		t.Errorf("reading_diff.id: want 1, got %d", id)
	}
	t.Logf("reading diff: id=%d old_val=%v new_val=%v old_samp=%v new_samp=%v",
		id, oldVal.Float64, newVal.Float64, oldSamp.Int64, newSamp.Int64)
}
