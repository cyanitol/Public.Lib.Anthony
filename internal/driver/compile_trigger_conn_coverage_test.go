// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
	"time"
)

// trigMemDB opens an in-memory database with MaxOpenConns=1 for isolation.
// Uses the package-level openMemDB helper which returns (*sql.DB, closer).
func trigMemDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	db, closer := openMemDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, closer
}

// execOrFatal executes SQL on db or calls t.Fatal.
func execOrFatal(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// ============================================================================
// Trigger compilation path tests
// These exercise executeBeforeInsertTriggers, executeAfterInsertTriggers,
// executeBeforeUpdateTriggers, executeAfterUpdateTriggers,
// executeBeforeDeleteTriggers, executeAfterDeleteTriggers, and
// prepareNewRowForInsert by issuing DML on tables that have triggers.
// ============================================================================

func TestCompileTriggerConn_BeforeInsertTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN INSERT INTO log VALUES(NULL, 'before_insert'); END")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='before_insert'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("before insert trigger: got count %d, want 1", count)
	}
}

func TestCompileTriggerConn_AfterInsertTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES(NULL, 'after_insert'); END")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='after_insert'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("after insert trigger: got count %d, want 1", count)
	}
}

func TestCompileTriggerConn_BeforeUpdateTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")
	execOrFatal(t, db, "CREATE TRIGGER t_bu BEFORE UPDATE ON t BEGIN INSERT INTO log VALUES(NULL, 'before_update'); END")
	execOrFatal(t, db, "UPDATE t SET val=20 WHERE id=1")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='before_update'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("before update trigger: got count %d, want 1", count)
	}
}

func TestCompileTriggerConn_AfterUpdateTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")
	execOrFatal(t, db, "CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO log VALUES(NULL, 'after_update'); END")
	execOrFatal(t, db, "UPDATE t SET val=20 WHERE id=1")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='after_update'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("after update trigger: got count %d, want 1", count)
	}
}

func TestCompileTriggerConn_BeforeDeleteTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")
	execOrFatal(t, db, "CREATE TRIGGER t_bd BEFORE DELETE ON t BEGIN INSERT INTO log VALUES(NULL, 'before_delete'); END")
	execOrFatal(t, db, "DELETE FROM t WHERE id=1")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='before_delete'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("before delete trigger: got count %d, want 1", count)
	}
}

func TestCompileTriggerConn_AfterDeleteTrigger(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")
	execOrFatal(t, db, "CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO log VALUES(NULL, 'after_delete'); END")
	execOrFatal(t, db, "DELETE FROM t WHERE id=1")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log WHERE msg='after_delete'").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("after delete trigger: got count %d, want 1", count)
	}
}

// TestCompileTriggerConn_NoTriggers exercises the early-return paths when
// there are no triggers, covering the len(triggers)==0 branches in each
// execute*Triggers function.
func TestCompileTriggerConn_NoTriggers(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFatal(t, db, "INSERT INTO t VALUES(1, 10)")
	execOrFatal(t, db, "UPDATE t SET val=20 WHERE id=1")
	execOrFatal(t, db, "DELETE FROM t WHERE id=1")
}

// TestCompileTriggerConn_PrepareNewRowForInsert_ExplicitColumns tests
// prepareNewRowForInsert with explicit column list in the INSERT.
func TestCompileTriggerConn_PrepareNewRowForInsert_ExplicitColumns(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER, name TEXT, score REAL)")
	execOrFatal(t, db, "CREATE TRIGGER t_bi BEFORE INSERT ON t BEGIN INSERT INTO log VALUES('triggered'); END")
	// INSERT with explicit column list: exercises the colNames != nil path
	execOrFatal(t, db, "INSERT INTO t(id, name, score) VALUES(1, 'alice', 9.5)")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("prepareNewRowForInsert explicit columns: want 1 log row, got %d", count)
	}
}

// TestCompileTriggerConn_PrepareNewRowForInsert_ImplicitColumns tests
// prepareNewRowForInsert where no column list is provided (all-column insert).
func TestCompileTriggerConn_PrepareNewRowForInsert_ImplicitColumns(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE log(msg TEXT)")
	execOrFatal(t, db, "CREATE TABLE t(id INTEGER, name TEXT, score REAL)")
	execOrFatal(t, db, "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO log VALUES('triggered'); END")
	// INSERT without explicit column list: exercises the colNames == nil path
	execOrFatal(t, db, "INSERT INTO t VALUES(2, 'bob', 7.3)")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM log").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("prepareNewRowForInsert implicit columns: want 1 log row, got %d", count)
	}
}

// ============================================================================
// ensureMasterPage — conn.go ~542
// Opening any new in-memory database exercises this path because a fresh
// connection has no pages and ensureMasterPage is called to bootstrap it.
// ============================================================================

func TestCompileTriggerConn_EnsureMasterPage(t *testing.T) {
	t.Parallel()
	// Open fresh in-memory DB — this calls ensureMasterPage internally.
	db, closer := trigMemDB(t)
	defer closer()

	// Verify the connection is functional (master page is in place).
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master").Scan(&n); err != nil {
		t.Fatalf("sqlite_master query: %v", err)
	}
}

// TestCompileTriggerConn_EnsureMasterPageAfterTable exercises the GetPage
// success branch of ensureMasterPage (page 1 already exists after a CREATE).
func TestCompileTriggerConn_EnsureMasterPageAfterTable(t *testing.T) {
	t.Parallel()
	db, closer := trigMemDB(t)
	defer closer()

	execOrFatal(t, db, "CREATE TABLE t(id INTEGER)")
	// A second open on a fresh in-memory DB won't reuse the same btree,
	// but querying sqlite_master exercises the existing-page path.
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='t'").Scan(&n); err != nil {
		t.Fatalf("sqlite_master query: %v", err)
	}
	if n != 1 {
		t.Errorf("expected table t in sqlite_master, got count %d", n)
	}
}

// ============================================================================
// RowExists — conn.go ~871
// ConnRowReader.RowExists delegates to VDBERowReader; exercise via FK checks
// or direct ConnRowReader construction from a live Conn.
// ============================================================================

// trigConnExecStmt prepares and executes a SQL statement on a Conn.
func trigConnExecStmt(t *testing.T, c *Conn, sql string) {
	t.Helper()
	stmt, err := c.Prepare(sql)
	if err != nil {
		t.Fatalf("prepare %q: %v", sql, err)
	}
	if _, err := stmt.Exec(nil); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	stmt.Close()
}

func TestCompileTriggerConn_RowExists(t *testing.T) {
	t.Parallel()
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	trigConnExecStmt(t, c, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)")
	trigConnExecStmt(t, c, "INSERT INTO t VALUES(1, 'hello')")

	rr := &ConnRowReader{conn: c}

	// Row should exist.
	exists, err := rr.RowExists("t", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("RowExists: %v", err)
	}
	if !exists {
		t.Error("RowExists: expected true for row id=1")
	}

	// Row should not exist.
	missing, err := rr.RowExists("t", []string{"id"}, []interface{}{int64(99)})
	if err != nil {
		t.Fatalf("RowExists missing: %v", err)
	}
	if missing {
		t.Error("RowExists: expected false for row id=99")
	}
}

// ============================================================================
// FindReferencingRows — conn.go ~881
// ============================================================================

func TestCompileTriggerConn_FindReferencingRows(t *testing.T) {
	t.Parallel()
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	for _, q := range []string{
		"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
		"CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))",
		"INSERT INTO parent VALUES(1)",
		"INSERT INTO child VALUES(10, 1)",
		"INSERT INTO child VALUES(11, 1)",
	} {
		stmt, err := c.Prepare(q)
		if err != nil {
			t.Fatalf("prepare %q: %v", q, err)
		}
		if _, err := stmt.Exec(nil); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
		stmt.Close()
	}

	rr := &ConnRowReader{conn: c}

	rows, err := rr.FindReferencingRows("child", []string{"pid"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("FindReferencingRows: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("FindReferencingRows: want 2 rows, got %d", len(rows))
	}
}

// TestCompileTriggerConn_FindReferencingRows_NoMatch exercises the path where
// no child rows reference the given parent value.
func TestCompileTriggerConn_FindReferencingRows_NoMatch(t *testing.T) {
	t.Parallel()
	drv := &Driver{}
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	c := conn.(*Conn)

	for _, q := range []string{
		"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
		"CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER)",
		"INSERT INTO parent VALUES(1)",
	} {
		stmt, err := c.Prepare(q)
		if err != nil {
			t.Fatalf("prepare %q: %v", q, err)
		}
		if _, err := stmt.Exec(nil); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
		stmt.Close()
	}

	rr := &ConnRowReader{conn: c}
	rows, err := rr.FindReferencingRows("child", []string{"pid"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("FindReferencingRows no-match: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("FindReferencingRows no-match: want 0 rows, got %d", len(rows))
	}
}

// ============================================================================
// config.go Clone and Validate
// ============================================================================

func TestCompileTriggerConn_ConfigClone(t *testing.T) {
	t.Parallel()
	orig := DefaultDriverConfig()
	orig.MaxConnections = 5
	orig.MaxIdleConnections = 3
	orig.EnableForeignKeys = true
	orig.EnableTriggers = false
	orig.EnableQueryLog = true
	orig.CaseSensitiveLike = true
	orig.RecursiveTriggers = true
	orig.AutoVacuum = "full"
	orig.SharedCache = true
	orig.QueryTimeout = 10 * time.Second
	orig.Extensions = []string{"ext1", "ext2"}

	clone := orig.Clone()

	if clone == orig {
		t.Fatal("Clone returned same pointer")
	}
	if clone.MaxConnections != orig.MaxConnections {
		t.Errorf("MaxConnections: got %d, want %d", clone.MaxConnections, orig.MaxConnections)
	}
	if clone.EnableForeignKeys != orig.EnableForeignKeys {
		t.Errorf("EnableForeignKeys mismatch")
	}
	if clone.AutoVacuum != orig.AutoVacuum {
		t.Errorf("AutoVacuum: got %q, want %q", clone.AutoVacuum, orig.AutoVacuum)
	}
	if len(clone.Extensions) != len(orig.Extensions) {
		t.Errorf("Extensions len: got %d, want %d", len(clone.Extensions), len(orig.Extensions))
	}
	// Verify deep copy: mutating clone should not affect orig.
	clone.Extensions[0] = "changed"
	if orig.Extensions[0] == "changed" {
		t.Error("Clone did not deep-copy Extensions slice")
	}
}

func TestCompileTriggerConn_ConfigValidate_Defaults(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate on default config: %v", err)
	}
}

func TestCompileTriggerConn_ConfigValidate_NilPager(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	cfg.Pager = nil // Validate should set it to DefaultPagerConfig.
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with nil Pager: %v", err)
	}
	if cfg.Pager == nil {
		t.Error("expected Pager to be set after Validate")
	}
}

func TestCompileTriggerConn_ConfigValidate_NilSecurity(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	cfg.Security = nil
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with nil Security: %v", err)
	}
	if cfg.Security == nil {
		t.Error("expected Security to be set after Validate")
	}
}

func TestCompileTriggerConn_ConfigValidate_NegativeMaxConnections(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	cfg.MaxConnections = -5
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with negative MaxConnections: %v", err)
	}
	if cfg.MaxConnections != 0 {
		t.Errorf("MaxConnections after Validate: got %d, want 0", cfg.MaxConnections)
	}
}

func TestCompileTriggerConn_ConfigValidate_NegativeMaxIdleConnections(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	cfg.MaxIdleConnections = -1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with negative MaxIdleConnections: %v", err)
	}
	if cfg.MaxIdleConnections != 2 {
		t.Errorf("MaxIdleConnections after Validate: got %d, want 2", cfg.MaxIdleConnections)
	}
}

func TestCompileTriggerConn_ConfigValidate_InvalidAutoVacuum(t *testing.T) {
	t.Parallel()
	cfg := DefaultDriverConfig()
	cfg.AutoVacuum = "bogus"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate with invalid AutoVacuum: %v", err)
	}
	if cfg.AutoVacuum != "none" {
		t.Errorf("AutoVacuum after Validate: got %q, want %q", cfg.AutoVacuum, "none")
	}
}

func TestCompileTriggerConn_ConfigValidate_AllValidAutoVacuumModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"none", "full", "incremental"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultDriverConfig()
			cfg.AutoVacuum = mode
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate with AutoVacuum=%q: %v", mode, err)
			}
			if cfg.AutoVacuum != mode {
				t.Errorf("AutoVacuum changed unexpectedly: got %q, want %q", cfg.AutoVacuum, mode)
			}
		})
	}
}
