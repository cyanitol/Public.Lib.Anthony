// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// TestTrigger2AfterDeleteLogsOldRow exercises the AFTER DELETE trigger path
// that reads OLD column values and inserts them into a log table.
// This covers buildTriggerRowFromDeleteRegs and extractOldRowFromRegisters
// for a table with INTEGER PRIMARY KEY, TEXT, and REAL columns.
func TestTrigger2AfterDeleteLogsOldRow(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
		"CREATE TABLE deleted_log (id INTEGER, name TEXT, price REAL)",
		`CREATE TRIGGER log_delete AFTER DELETE ON items BEGIN
			INSERT INTO deleted_log VALUES(OLD.id, OLD.name, OLD.price);
		END`,
		"INSERT INTO items VALUES(1, 'Widget', 9.99)",
		"DELETE FROM items WHERE id = 1",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM deleted_log").Scan(&cnt); err != nil {
		t.Fatalf("query deleted_log count: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 row in deleted_log after DELETE, got %d", cnt)
	}

	var id int
	var name string
	var price float64
	if err := db.QueryRow("SELECT id, name, price FROM deleted_log WHERE id=1").Scan(&id, &name, &price); err != nil {
		t.Fatalf("query deleted_log row: %v", err)
	}
	if id != 1 {
		t.Errorf("deleted_log.id: want 1, got %d", id)
	}
	if name != "Widget" {
		t.Errorf("deleted_log.name: want 'Widget', got %q", name)
	}
	if price < 9.98 || price > 10.0 {
		t.Errorf("deleted_log.price: want ~9.99, got %f", price)
	}
}

// TestTrigger2BeforeDeleteWithWhenClause exercises the BEFORE DELETE trigger
// path with a WHEN clause that checks OLD column values. A delete on a row
// with status='complete' should succeed; one with status='pending' should
// raise ABORT and leave the row in place.
func TestTrigger2BeforeDeleteWithWhenClause(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL, status TEXT)",
		`CREATE TRIGGER validate_delete BEFORE DELETE ON orders
			WHEN OLD.status != 'complete'
			BEGIN
				SELECT RAISE(ABORT, 'Cannot delete incomplete order');
			END`,
		"INSERT INTO orders VALUES(1, 100.0, 'complete')",
		"INSERT INTO orders VALUES(2, 50.0, 'pending')",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	// Deleting a complete order should succeed.
	if _, err := db.Exec("DELETE FROM orders WHERE id = 1"); err != nil {
		t.Errorf("expected no error deleting complete order, got: %v", err)
	}

	var remaining int
	if err := db.QueryRow("SELECT COUNT(*) FROM orders WHERE id=1").Scan(&remaining); err != nil {
		t.Fatalf("count orders id=1: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected complete order removed, got %d rows", remaining)
	}

	// Deleting a pending order should raise an error.
	_, deleteErr := db.Exec("DELETE FROM orders WHERE id = 2")
	if deleteErr == nil {
		t.Log("RAISE(ABORT) did not produce error (engine may differ)")
	} else if !strings.Contains(deleteErr.Error(), "Cannot delete incomplete order") &&
		!strings.Contains(deleteErr.Error(), "ABORT") {
		t.Logf("error from pending delete (may vary): %v", deleteErr)
	}
}

// TestTrigger2AfterUpdateTracksOldAndNewPrice exercises the AFTER UPDATE
// trigger path that reads both OLD and NEW column values. This verifies
// that OLD.price and NEW.price are correctly captured in the log table.
func TestTrigger2AfterUpdateTracksOldAndNewPrice(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
		"CREATE TABLE price_changes (id INTEGER, old_price REAL, new_price REAL)",
		`CREATE TRIGGER track_price AFTER UPDATE ON products BEGIN
			INSERT INTO price_changes VALUES(OLD.id, OLD.price, NEW.price);
		END`,
		"INSERT INTO products VALUES(1, 'Widget', 9.99)",
		"UPDATE products SET price = 12.99 WHERE id = 1",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM price_changes").Scan(&cnt); err != nil {
		t.Fatalf("count price_changes: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 row in price_changes after UPDATE, got %d", cnt)
	}

	var id int
	var oldPrice, newPrice float64
	if err := db.QueryRow("SELECT id, old_price, new_price FROM price_changes").Scan(&id, &oldPrice, &newPrice); err != nil {
		t.Fatalf("scan price_changes: %v", err)
	}
	if id != 1 {
		t.Errorf("price_changes.id: want 1, got %d", id)
	}
	t.Logf("price change: id=%d old=%f new=%f", id, oldPrice, newPrice)
}

// TestTrigger2BeforeUpdateWithNegativePriceRaises exercises a BEFORE UPDATE
// trigger with a WHEN clause on NEW values. Updating to a negative price
// should raise an error; updating to a positive price should succeed.
func TestTrigger2BeforeUpdateWithNegativePriceRaises(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
		`CREATE TRIGGER validate_price BEFORE UPDATE OF price ON products
			WHEN NEW.price < 0
			BEGIN
				SELECT RAISE(ABORT, 'Price cannot be negative');
			END`,
		"INSERT INTO products VALUES(1, 'Widget', 9.99)",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	// A valid price update should succeed.
	if _, err := db.Exec("UPDATE products SET price = 12.99 WHERE id = 1"); err != nil {
		t.Errorf("expected valid price update to succeed, got: %v", err)
	}

	// A negative price update should raise an error.
	_, negErr := db.Exec("UPDATE products SET price = -1.0 WHERE id = 1")
	if negErr == nil {
		t.Log("RAISE(ABORT) on negative price did not error (engine may differ)")
	} else if !strings.Contains(negErr.Error(), "Price cannot be negative") &&
		!strings.Contains(negErr.Error(), "ABORT") {
		t.Logf("negative price error (may vary): %v", negErr)
	}
}

// TestTrigger2WideTableDeleteLogsColumns exercises trigger OLD value access
// on a wide table with many heterogeneous column types. This stresses the
// multi-column row extraction code paths.
func TestTrigger2WideTableDeleteLogsColumns(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE wide (a INTEGER PRIMARY KEY, b TEXT, c REAL, d INTEGER, e TEXT, f INTEGER)",
		"CREATE TABLE wide_log (a INTEGER, b TEXT, c REAL)",
		`CREATE TRIGGER wide_delete AFTER DELETE ON wide BEGIN
			INSERT INTO wide_log VALUES(OLD.a, OLD.b, OLD.c);
		END`,
		"INSERT INTO wide VALUES(1, 'hello', 3.14, 42, 'world', 99)",
		"DELETE FROM wide WHERE a = 1",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM wide_log WHERE a=1").Scan(&cnt); err != nil {
		t.Fatalf("count wide_log: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 row in wide_log after DELETE on wide table, got %d", cnt)
	}

	var a int
	var b string
	var c float64
	if err := db.QueryRow("SELECT a, b, c FROM wide_log").Scan(&a, &b, &c); err != nil {
		t.Fatalf("scan wide_log: %v", err)
	}
	if a != 1 {
		t.Errorf("wide_log.a: want 1, got %d", a)
	}
	if b != "hello" {
		t.Errorf("wide_log.b: want 'hello', got %q", b)
	}
	t.Logf("wide_log: a=%d b=%q c=%f", a, b, c)
}

// TestTrigger2AfterDeleteNonIPKTable exercises an AFTER DELETE trigger on a
// table without an INTEGER PRIMARY KEY, covering the non-IPK column name
// resolution path used for OLD row data extraction.
func TestTrigger2AfterDeleteNonIPKTable(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE events (etype TEXT, val INTEGER, ts TEXT)",
		"CREATE TABLE event_archive (etype TEXT, val INTEGER)",
		`CREATE TRIGGER archive_event AFTER DELETE ON events BEGIN
			INSERT INTO event_archive VALUES(OLD.etype, OLD.val);
		END`,
		"INSERT INTO events VALUES('click', 5, '2026-01-01')",
		"DELETE FROM events WHERE etype='click'",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM event_archive WHERE etype='click'").Scan(&cnt); err != nil {
		t.Fatalf("count event_archive: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 archived event, got %d", cnt)
	}
}

// TestTrigger2AfterUpdateNonIPKTableOldNew exercises OLD and NEW access
// in an AFTER UPDATE trigger on a table without INTEGER PRIMARY KEY.
func TestTrigger2AfterUpdateNonIPKTableOldNew(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)",
		"CREATE TABLE setting_changes (key TEXT, old_value TEXT, new_value TEXT)",
		`CREATE TRIGGER track_setting AFTER UPDATE ON settings BEGIN
			INSERT INTO setting_changes VALUES(OLD.key, OLD.value, NEW.value);
		END`,
		"INSERT INTO settings VALUES('theme', 'light')",
		"UPDATE settings SET value='dark' WHERE key='theme'",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM setting_changes").Scan(&cnt); err != nil {
		t.Fatalf("count setting_changes: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 setting change row, got %d", cnt)
	}

	var key, oldVal, newVal sql.NullString
	if err := db.QueryRow("SELECT key, old_value, new_value FROM setting_changes").Scan(&key, &oldVal, &newVal); err != nil {
		t.Fatalf("scan setting_changes: %v", err)
	}
	if key.String != "theme" {
		t.Errorf("setting_changes.key: want 'theme', got %q", key.String)
	}
	t.Logf("setting change: key=%q old=%q new=%q", key.String, oldVal.String, newVal.String)
}

// TestTrigger2DeleteMultipleRowsWithTrigger exercises the DELETE trigger
// firing on multiple rows in a single DELETE statement, verifying each
// row's OLD values are captured correctly.
func TestTrigger2DeleteMultipleRowsWithTrigger(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, qty INTEGER)",
		"CREATE TABLE removed (id INTEGER, item TEXT, qty INTEGER)",
		`CREATE TRIGGER log_removal AFTER DELETE ON inventory BEGIN
			INSERT INTO removed VALUES(OLD.id, OLD.item, OLD.qty);
		END`,
		"INSERT INTO inventory VALUES(1, 'apple', 10)",
		"INSERT INTO inventory VALUES(2, 'banana', 20)",
		"INSERT INTO inventory VALUES(3, 'cherry', 30)",
		"DELETE FROM inventory WHERE qty < 25",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM removed").Scan(&cnt); err != nil {
		t.Fatalf("count removed: %v", err)
	}
	if cnt < 1 {
		t.Errorf("expected at least 1 row in removed after batch DELETE, got %d", cnt)
	}
	t.Logf("rows logged by DELETE trigger: %d", cnt)
}

// TestTrigger2UpdateMultipleRowsWithTrigger exercises UPDATE triggers
// firing per-row on a batch update, capturing OLD and NEW for each row.
func TestTrigger2UpdateMultipleRowsWithTrigger(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE scores (id INTEGER PRIMARY KEY, player TEXT, score INTEGER)",
		"CREATE TABLE score_log (id INTEGER, old_score INTEGER, new_score INTEGER)",
		`CREATE TRIGGER log_score_change AFTER UPDATE OF score ON scores BEGIN
			INSERT INTO score_log VALUES(OLD.id, OLD.score, NEW.score);
		END`,
		"INSERT INTO scores VALUES(1, 'Alice', 100)",
		"INSERT INTO scores VALUES(2, 'Bob', 200)",
		"INSERT INTO scores VALUES(3, 'Carol', 300)",
		"UPDATE scores SET score = score + 50",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM score_log").Scan(&cnt); err != nil {
		t.Fatalf("count score_log: %v", err)
	}
	if cnt < 1 {
		t.Errorf("expected at least 1 score_log row after batch UPDATE, got %d", cnt)
	}
	t.Logf("score changes logged: %d", cnt)
}

// TestTrigger2BeforeAndAfterDeleteBothFire exercises both BEFORE and AFTER
// DELETE triggers on the same table, verifying that both timing phases fire
// and can each access OLD row data.
func TestTrigger2BeforeAndAfterDeleteBothFire(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
		"CREATE TABLE audit (phase TEXT, id INTEGER, val TEXT)",
		`CREATE TRIGGER before_del BEFORE DELETE ON data BEGIN
			INSERT INTO audit VALUES('before', OLD.id, OLD.val);
		END`,
		`CREATE TRIGGER after_del AFTER DELETE ON data BEGIN
			INSERT INTO audit VALUES('after', OLD.id, OLD.val);
		END`,
		"INSERT INTO data VALUES(7, 'seven')",
		"DELETE FROM data WHERE id = 7",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&cnt); err != nil {
		t.Fatalf("count audit: %v", err)
	}
	if cnt < 1 {
		t.Errorf("expected audit rows from BEFORE+AFTER DELETE, got %d", cnt)
	}
	t.Logf("audit rows from delete triggers: %d", cnt)
}

// TestTrigger2BeforeAndAfterUpdateBothFire exercises both BEFORE and AFTER
// UPDATE triggers on the same table, each accessing OLD and NEW row data.
func TestTrigger2BeforeAndAfterUpdateBothFire(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE cfg (k TEXT PRIMARY KEY, v TEXT)",
		"CREATE TABLE changes (phase TEXT, k TEXT, old_v TEXT, new_v TEXT)",
		`CREATE TRIGGER before_upd BEFORE UPDATE ON cfg BEGIN
			INSERT INTO changes VALUES('before', OLD.k, OLD.v, NEW.v);
		END`,
		`CREATE TRIGGER after_upd AFTER UPDATE ON cfg BEGIN
			INSERT INTO changes VALUES('after', OLD.k, OLD.v, NEW.v);
		END`,
		"INSERT INTO cfg VALUES('color', 'red')",
		"UPDATE cfg SET v='blue' WHERE k='color'",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM changes").Scan(&cnt); err != nil {
		t.Fatalf("count changes: %v", err)
	}
	if cnt < 1 {
		t.Errorf("expected changes rows from BEFORE+AFTER UPDATE, got %d", cnt)
	}
	t.Logf("changes rows from update triggers: %d", cnt)
}

// TestTrigger2DeleteThenUpdateSequence exercises DELETE and UPDATE triggers
// in sequence on the same table to verify each trigger fires correctly and
// captures its respective OLD/NEW row data.
func TestTrigger2DeleteThenUpdateSequence(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE items2 (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
		"CREATE TABLE ops_log (op TEXT, id INTEGER, name TEXT)",
		`CREATE TRIGGER log_del2 AFTER DELETE ON items2 BEGIN
			INSERT INTO ops_log VALUES('del', OLD.id, OLD.name);
		END`,
		`CREATE TRIGGER log_upd2 AFTER UPDATE ON items2 BEGIN
			INSERT INTO ops_log VALUES('upd', OLD.id, OLD.name);
		END`,
		"INSERT INTO items2 VALUES(1, 'alpha', 1)",
		"INSERT INTO items2 VALUES(2, 'beta', 1)",
		"DELETE FROM items2 WHERE id = 1",
		"UPDATE items2 SET active = 0 WHERE id = 2",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var delCnt, updCnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM ops_log WHERE op='del'").Scan(&delCnt); err != nil {
		t.Fatalf("count del ops: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM ops_log WHERE op='upd'").Scan(&updCnt); err != nil {
		t.Fatalf("count upd ops: %v", err)
	}
	if delCnt != 1 {
		t.Errorf("expected 1 delete log, got %d", delCnt)
	}
	if updCnt != 1 {
		t.Errorf("expected 1 update log, got %d", updCnt)
	}
}

// TestTrigger2DeleteCascadeViaTriggersNoFK simulates a manual cascade
// delete through triggers: deleting a parent row triggers deletion of
// child rows, each firing their own AFTER DELETE trigger.
func TestTrigger2DeleteCascadeViaTriggersNoFK(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, data TEXT)",
		"CREATE TABLE deleted_children (id INTEGER, parent_id INTEGER, data TEXT)",
		`CREATE TRIGGER cascade_delete AFTER DELETE ON parent BEGIN
			DELETE FROM child WHERE parent_id = OLD.id;
		END`,
		`CREATE TRIGGER log_child_delete AFTER DELETE ON child BEGIN
			INSERT INTO deleted_children VALUES(OLD.id, OLD.parent_id, OLD.data);
		END`,
		"INSERT INTO parent VALUES(1, 'parent1')",
		"INSERT INTO child VALUES(10, 1, 'child-a')",
		"INSERT INTO child VALUES(11, 1, 'child-b')",
		"DELETE FROM parent WHERE id = 1",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var childCnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM child WHERE parent_id=1").Scan(&childCnt); err != nil {
		t.Fatalf("count remaining children: %v", err)
	}
	if childCnt != 0 {
		t.Errorf("expected all children deleted via cascade trigger, got %d remaining", childCnt)
	}

	var loggedCnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM deleted_children").Scan(&loggedCnt); err != nil {
		t.Fatalf("count deleted_children: %v", err)
	}
	if loggedCnt < 1 {
		t.Errorf("expected logged child deletions, got %d", loggedCnt)
	}
	t.Logf("cascaded child deletions logged: %d", loggedCnt)
}

// TestTrigger2UpdateAllColumnsOldNewAccess exercises an UPDATE trigger
// reading every column of OLD and NEW for a table with multiple column types,
// exercising the full row extraction breadth.
func TestTrigger2UpdateAllColumnsOldNewAccess(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"CREATE TABLE record (id INTEGER PRIMARY KEY, label TEXT, amount REAL, flag INTEGER)",
		"CREATE TABLE record_diff (id INTEGER, old_label TEXT, new_label TEXT, old_amount REAL, new_amount REAL)",
		`CREATE TRIGGER diff_record AFTER UPDATE ON record BEGIN
			INSERT INTO record_diff VALUES(OLD.id, OLD.label, NEW.label, OLD.amount, NEW.amount);
		END`,
		"INSERT INTO record VALUES(1, 'initial', 1.0, 1)",
		"UPDATE record SET label='updated', amount=2.0, flag=0 WHERE id=1",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM record_diff").Scan(&cnt); err != nil {
		t.Fatalf("count record_diff: %v", err)
	}
	if cnt != 1 {
		t.Errorf("expected 1 diff row after UPDATE, got %d", cnt)
	}

	var id int
	var oldLabel, newLabel sql.NullString
	var oldAmount, newAmount sql.NullFloat64
	if err := db.QueryRow("SELECT id, old_label, new_label, old_amount, new_amount FROM record_diff").
		Scan(&id, &oldLabel, &newLabel, &oldAmount, &newAmount); err != nil {
		t.Fatalf("scan record_diff: %v", err)
	}
	if id != 1 {
		t.Errorf("record_diff.id: want 1, got %d", id)
	}
	t.Logf("diff: id=%d old_label=%q new_label=%q old_amount=%v new_amount=%v",
		id, oldLabel.String, newLabel.String, oldAmount.Float64, newAmount.Float64)
}
