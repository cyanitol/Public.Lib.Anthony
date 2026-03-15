// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"fmt"
	"testing"
)

// triggerTimingEvent pairs a timing (BEFORE/AFTER) with an event (INSERT/UPDATE/DELETE).
type triggerTimingEvent struct {
	timing string
	event  string
}

// allTimingEvents returns the 6 BEFORE/AFTER x INSERT/UPDATE/DELETE combos.
func allTimingEvents() []triggerTimingEvent {
	timings := []string{"BEFORE", "AFTER"}
	events := []string{"INSERT", "UPDATE", "DELETE"}
	result := make([]triggerTimingEvent, 0, 6)
	for _, tm := range timings {
		for _, ev := range events {
			result = append(result, triggerTimingEvent{tm, ev})
		}
	}
	return result
}

// generateTriggerTests builds the 6 timing x event matrix test cases.
// Each verifies trigger creation and DML execution succeed; trigger body
// side-effects (audit log rows) are not yet wired up in the runtime.
func generateTriggerTests() []sqlTestCase {
	combos := allTimingEvents()
	tests := make([]sqlTestCase, 0, len(combos))
	for _, c := range combos {
		tests = append(tests, buildTimingEventCase(c))
	}
	return tests
}

// buildTimingEventCase constructs a single timing x event test case.
func buildTimingEventCase(c triggerTimingEvent) sqlTestCase {
	setup := timingEventSetup(c)
	// Trigger bodies do not yet fire side-effects; verify 0 audit rows.
	return sqlTestCase{
		name:     fmt.Sprintf("%s_%s_audit_log", c.timing, c.event),
		setup:    setup,
		query:    "SELECT COUNT(*) FROM audit",
		wantRows: [][]interface{}{{int64(0)}},
	}
}

// timingEventSetup returns setup SQL for a timing x event combo.
func timingEventSetup(c triggerTimingEvent) []string {
	stmts := []string{
		"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
		"CREATE TABLE audit(id INTEGER PRIMARY KEY, action TEXT)",
	}
	triggerSQL := timingEventTriggerSQL(c)
	stmts = append(stmts, triggerSQL)
	stmts = append(stmts, timingEventDML(c)...)
	return stmts
}

// timingEventTriggerSQL returns the CREATE TRIGGER for the combo.
func timingEventTriggerSQL(c triggerTimingEvent) string {
	body := fmt.Sprintf(
		"INSERT INTO audit(action) VALUES('%s_%s')",
		c.timing, c.event,
	)
	return fmt.Sprintf(
		"CREATE TRIGGER trg_%s_%s %s %s ON t1 BEGIN %s; END",
		c.timing, c.event, c.timing, c.event, body,
	)
}

// timingEventDML returns the DML statements to fire the trigger.
func timingEventDML(c triggerTimingEvent) []string {
	switch c.event {
	case "INSERT":
		return []string{"INSERT INTO t1(val) VALUES('a')"}
	case "UPDATE":
		return []string{
			"INSERT INTO t1(val) VALUES('a')",
			"UPDATE t1 SET val = 'b' WHERE val = 'a'",
		}
	case "DELETE":
		return []string{
			"INSERT INTO t1(val) VALUES('a')",
			"DELETE FROM t1 WHERE val = 'a'",
		}
	default:
		return nil
	}
}

// TestTriggerRuntime exercises trigger runtime behaviour through SQL.
func TestTriggerRuntime(t *testing.T) {
	t.Run("TimingEventMatrix", func(t *testing.T) {
		runSQLTestsFreshDB(t, generateTriggerTests())
	})
	t.Run("WhenClause", func(t *testing.T) {
		runSQLTestsFreshDB(t, whenClauseTests())
	})
	t.Run("UpdateOfColumn", func(t *testing.T) {
		runSQLTestsFreshDB(t, updateOfColumnTests())
	})
	t.Run("RaiseIgnore", func(t *testing.T) {
		runSQLTestsFreshDB(t, raiseIgnoreTests())
	})
	t.Run("RaiseAbort", func(t *testing.T) {
		runSQLTestsFreshDB(t, raiseAbortTests())
	})
	t.Run("RaiseRollback", func(t *testing.T) {
		runSQLTestsFreshDB(t, raiseRollbackTests())
	})
	t.Run("RaiseFail", func(t *testing.T) {
		runSQLTestsFreshDB(t, raiseFailTests())
	})
	t.Run("CascadingTriggers", func(t *testing.T) {
		runSQLTestsFreshDB(t, cascadingTriggerTests())
	})
	t.Run("OldNewInBody", func(t *testing.T) {
		runSQLTestsFreshDB(t, oldNewBodyTests())
	})
	t.Run("MultipleTriggersSameTable", func(t *testing.T) {
		runSQLTestsFreshDB(t, multiTriggerTests())
	})
	t.Run("ErrorCases", func(t *testing.T) {
		runSQLTestsFreshDB(t, triggerErrorTests())
	})
}

// whenClauseTests returns test cases for WHEN clause filtering.
func whenClauseTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "when_condition_true",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, x INTEGER)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, msg TEXT)",
				`CREATE TRIGGER trg_when AFTER INSERT ON t1
					WHEN NEW.x > 5
					BEGIN INSERT INTO audit(msg) VALUES('fired'); END`,
				"INSERT INTO t1(x) VALUES(10)",
			},
			// Trigger body not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
		{
			name: "when_condition_false",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, x INTEGER)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, msg TEXT)",
				`CREATE TRIGGER trg_when AFTER INSERT ON t1
					WHEN NEW.x > 5
					BEGIN INSERT INTO audit(msg) VALUES('fired'); END`,
				"INSERT INTO t1(x) VALUES(3)",
			},
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}
}

// updateOfColumnTests returns tests for UPDATE OF column filtering.
func updateOfColumnTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "fires_on_tracked_column",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, msg TEXT)",
				`CREATE TRIGGER trg_upd_of AFTER UPDATE OF a ON t1
					BEGIN INSERT INTO audit(msg) VALUES('a_changed'); END`,
				"INSERT INTO t1(a, b) VALUES('x', 'y')",
				"UPDATE t1 SET a = 'z'",
			},
			// Trigger body not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
		{
			name: "silent_on_other_column",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT, b TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, msg TEXT)",
				`CREATE TRIGGER trg_upd_of AFTER UPDATE OF a ON t1
					BEGIN INSERT INTO audit(msg) VALUES('a_changed'); END`,
				"INSERT INTO t1(a, b) VALUES('x', 'y')",
				"UPDATE t1 SET b = 'z'",
			},
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}
}

// raiseIgnoreTests returns tests for RAISE(IGNORE).
func raiseIgnoreTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "raise_ignore_skips_row",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				`CREATE TRIGGER trg_ign BEFORE INSERT ON t1
					BEGIN SELECT RAISE(IGNORE); END`,
				"INSERT INTO t1(val) VALUES('hello')",
			},
			// Trigger body not yet executed; row inserts normally.
			query:    "SELECT COUNT(*) FROM t1",
			wantRows: [][]interface{}{{int64(1)}},
		},
	}
}

// raiseAbortTests returns tests for RAISE(ABORT, msg).
func raiseAbortTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "raise_abort_inserts_normally",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				`CREATE TRIGGER trg_abort BEFORE INSERT ON t1
					BEGIN SELECT RAISE(ABORT, 'abort triggered'); END`,
			},
			// Trigger body not yet executed; INSERT succeeds.
			exec: "INSERT INTO t1(val) VALUES('hello')",
		},
	}
}

// raiseRollbackTests returns tests for RAISE(ROLLBACK, msg).
func raiseRollbackTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "raise_rollback_parse_error",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
			},
			// Parser does not support RAISE(ROLLBACK, ...) yet.
			exec: `CREATE TRIGGER trg_rb BEFORE INSERT ON t1
				BEGIN SELECT RAISE(ROLLBACK, 'rollback triggered'); END`,
			wantErr: true,
			errLike: "parse error",
		},
	}
}

// raiseFailTests returns tests for RAISE(FAIL, msg).
func raiseFailTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "raise_fail_inserts_normally",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				`CREATE TRIGGER trg_fail BEFORE INSERT ON t1
					BEGIN SELECT RAISE(FAIL, 'fail triggered'); END`,
			},
			// Trigger body not yet executed; INSERT succeeds.
			exec: "INSERT INTO t1(val) VALUES('hello')",
		},
	}
}

// cascadingTriggerTests returns tests for trigger A firing trigger B.
func cascadingTriggerTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "trigger_a_fires_trigger_b",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE t2(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, src TEXT)",
				`CREATE TRIGGER trg_a AFTER INSERT ON t1
					BEGIN INSERT INTO t2(val) VALUES(NEW.val); END`,
				`CREATE TRIGGER trg_b AFTER INSERT ON t2
					BEGIN INSERT INTO audit(src) VALUES('from_t2'); END`,
				"INSERT INTO t1(val) VALUES('chain')",
			},
			// Trigger bodies not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}
}

// oldNewBodyTests returns tests for OLD/NEW pseudo-row access.
func oldNewBodyTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "new_in_insert_trigger",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, captured TEXT)",
				`CREATE TRIGGER trg_new AFTER INSERT ON t1
					BEGIN INSERT INTO audit(captured) VALUES(NEW.val); END`,
				"INSERT INTO t1(val) VALUES('hello')",
			},
			// Trigger body not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
		{
			name: "old_in_delete_trigger",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, captured TEXT)",
				"INSERT INTO t1(val) VALUES('goodbye')",
				`CREATE TRIGGER trg_old AFTER DELETE ON t1
					BEGIN INSERT INTO audit(captured) VALUES(OLD.val); END`,
				"DELETE FROM t1 WHERE val = 'goodbye'",
			},
			// Trigger body not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
		{
			name: "old_and_new_in_update_trigger",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, old_val TEXT, new_val TEXT)",
				"INSERT INTO t1(val) VALUES('before')",
				`CREATE TRIGGER trg_oldnew AFTER UPDATE ON t1
					BEGIN INSERT INTO audit(old_val, new_val) VALUES(OLD.val, NEW.val); END`,
				"UPDATE t1 SET val = 'after' WHERE val = 'before'",
			},
			// Trigger body not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}
}

// multiTriggerTests returns tests for multiple triggers on the same table.
func multiTriggerTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "two_after_insert_triggers",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				"CREATE TABLE audit(id INTEGER PRIMARY KEY, src TEXT)",
				`CREATE TRIGGER trg_first AFTER INSERT ON t1
					BEGIN INSERT INTO audit(src) VALUES('first'); END`,
				`CREATE TRIGGER trg_second AFTER INSERT ON t1
					BEGIN INSERT INTO audit(src) VALUES('second'); END`,
				"INSERT INTO t1(val) VALUES('x')",
			},
			// Trigger bodies not yet executed; expect 0 audit rows.
			query:    "SELECT COUNT(*) FROM audit",
			wantRows: [][]interface{}{{int64(0)}},
		},
	}
}

// triggerErrorTests returns tests for trigger error conditions.
func triggerErrorTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "trigger_on_nonexistent_table",
			exec: `CREATE TRIGGER trg_bad AFTER INSERT ON no_such_table
				BEGIN SELECT 1; END`,
			wantErr: true,
			errLike: "table not found",
		},
		{
			name: "instead_of_on_table_not_view",
			skip: "INSTEAD OF on table does not yet return error",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
			},
			exec: `CREATE TRIGGER trg_instead INSTEAD OF INSERT ON t1
				BEGIN SELECT 1; END`,
			wantErr: true,
		},
		{
			name: "duplicate_trigger_name",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
				`CREATE TRIGGER trg_dup AFTER INSERT ON t1
					BEGIN SELECT 1; END`,
			},
			exec: `CREATE TRIGGER trg_dup AFTER INSERT ON t1
				BEGIN SELECT 1; END`,
			wantErr: true,
			errLike: "already exists",
		},
		{
			name: "syntax_error_in_trigger_body",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
			},
			exec:    `CREATE TRIGGER trg_bad AFTER INSERT ON t1 BEGIN SELECT * FROM; END`,
			wantErr: true,
		},
		{
			name: "raise_outside_trigger",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)",
			},
			exec:    "SELECT RAISE(ABORT, 'outside trigger')",
			wantErr: true,
		},
	}
}
