// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Helper functions to reduce cyclomatic complexity

func parseCreateTriggerStmt(t *testing.T, sql string) *CreateTriggerStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*CreateTriggerStmt)
	if !ok {
		t.Fatalf("expected CreateTriggerStmt, got %T", stmts[0])
	}
	return stmt
}

func runCreateTriggerSubtest(t *testing.T, name, sql string, check func(*testing.T, *CreateTriggerStmt)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		stmt := parseCreateTriggerStmt(t, sql)
		check(t, stmt)
	})
}

func runCreateTriggerErrorSubtest(t *testing.T, name, sql string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err == nil {
			t.Error("Expected error but got none")
		}
	})
}

func TestParseCreateTrigger(t *testing.T) {
	t.Parallel()
	testParseCreateTriggerSuccess(t)
	testParseCreateTriggerErrors(t)
}

// Prefix: trig_
type triggerTestCase struct {
	name            string
	sql             string
	wantName        string
	wantTable       string
	wantTiming      TriggerTiming
	wantEvent       TriggerEvent
	wantTemp        bool
	wantIfNotExists bool
	wantForEachRow  bool
	wantHasWhen     bool
	wantUpdateOf    []string
	wantBodyLen     int
}

func trig_checkBasicFields(t *testing.T, stmt *CreateTriggerStmt, tc triggerTestCase) {
	t.Helper()
	if tc.wantName != "" && stmt.Name != tc.wantName {
		t.Errorf("expected name %q, got %q", tc.wantName, stmt.Name)
	}
	if tc.wantTable != "" && stmt.Table != tc.wantTable {
		t.Errorf("expected table %q, got %q", tc.wantTable, stmt.Table)
	}
	if tc.wantTiming != 0 && stmt.Timing != tc.wantTiming {
		t.Errorf("expected timing %v, got %v", tc.wantTiming, stmt.Timing)
	}
	if tc.wantEvent != 0 && stmt.Event != tc.wantEvent {
		t.Errorf("expected event %v, got %v", tc.wantEvent, stmt.Event)
	}
}

func trig_checkFlags(t *testing.T, stmt *CreateTriggerStmt, tc triggerTestCase) {
	t.Helper()
	if tc.wantTemp && !stmt.Temp {
		t.Error("expected Temp to be true")
	}
	if tc.wantIfNotExists && !stmt.IfNotExists {
		t.Error("expected IfNotExists to be true")
	}
	if tc.wantForEachRow && !stmt.ForEachRow {
		t.Error("expected ForEachRow to be true")
	}
	if tc.wantHasWhen && stmt.When == nil {
		t.Error("expected When clause to be present")
	}
}

func trig_checkUpdateOfAndBody(t *testing.T, stmt *CreateTriggerStmt, tc triggerTestCase) {
	t.Helper()
	if tc.wantUpdateOf != nil && !trig_compareSlices(stmt.UpdateOf, tc.wantUpdateOf) {
		t.Errorf("expected UpdateOf %v, got %v", tc.wantUpdateOf, stmt.UpdateOf)
	}
	if tc.wantBodyLen > 0 && len(stmt.Body) != tc.wantBodyLen {
		t.Errorf("expected %d statements in body, got %d", tc.wantBodyLen, len(stmt.Body))
	}
}

func trig_checkStmt(t *testing.T, stmt *CreateTriggerStmt, tc triggerTestCase) {
	t.Helper()
	trig_checkBasicFields(t, stmt, tc)
	trig_checkFlags(t, stmt, tc)
	trig_checkUpdateOfAndBody(t, stmt, tc)
}

func trig_compareSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testParseCreateTriggerSuccess(t *testing.T) {
	t.Helper()
	tests := []triggerTestCase{
		{
			name:     "simple trigger - before insert",
			sql:      "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantName: "my_trigger", wantTable: "users", wantTiming: TriggerBefore, wantEvent: TriggerInsert, wantBodyLen: 1,
		},
		{
			name:       "trigger - after update",
			sql:        "CREATE TRIGGER audit_trigger AFTER UPDATE ON employees BEGIN INSERT INTO audit_log VALUES (NEW.id, NEW.name); END",
			wantTiming: TriggerAfter, wantEvent: TriggerUpdate,
		},
		{
			name:      "trigger - after delete",
			sql:       "CREATE TRIGGER delete_trigger AFTER DELETE ON products BEGIN DELETE FROM inventory WHERE product_id = OLD.id; END",
			wantEvent: TriggerDelete,
		},
		{
			name:       "trigger - instead of",
			sql:        "CREATE TRIGGER view_trigger INSTEAD OF INSERT ON my_view BEGIN SELECT 1; END",
			wantTiming: TriggerInsteadOf,
		},
		{
			name:            "trigger - with if not exists",
			sql:             "CREATE TRIGGER IF NOT EXISTS my_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantIfNotExists: true,
		},
		{
			name:     "temp trigger",
			sql:      "CREATE TEMP TRIGGER temp_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantTemp: true,
		},
		{
			name:           "trigger - for each row",
			sql:            "CREATE TRIGGER my_trigger BEFORE INSERT ON users FOR EACH ROW BEGIN SELECT 1; END",
			wantForEachRow: true,
		},
		{
			name:        "trigger - with when clause",
			sql:         "CREATE TRIGGER my_trigger BEFORE INSERT ON users WHEN NEW.age > 18 BEGIN SELECT 1; END",
			wantHasWhen: true,
		},
		{
			name:         "trigger - update of specific columns",
			sql:          "CREATE TRIGGER my_trigger BEFORE UPDATE OF name, email ON users BEGIN SELECT 1; END",
			wantUpdateOf: []string{"name", "email"},
		},
		{
			name:        "trigger - multiple statements in body",
			sql:         "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN UPDATE counter SET count = count + 1; INSERT INTO log VALUES (1); SELECT 1; END",
			wantBodyLen: 3,
		},
		{
			name:     "trigger - complete example",
			sql:      "CREATE TEMP TRIGGER IF NOT EXISTS audit_trigger AFTER UPDATE OF salary ON employees FOR EACH ROW WHEN NEW.salary > OLD.salary BEGIN INSERT INTO audit_log (emp_id, old_salary, new_salary, timestamp) VALUES (NEW.id, OLD.salary, NEW.salary, datetime('now')); END",
			wantTemp: true, wantIfNotExists: true, wantTiming: TriggerAfter, wantEvent: TriggerUpdate,
			wantUpdateOf: []string{"salary"}, wantForEachRow: true, wantHasWhen: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stmt := parseCreateTriggerStmt(t, tc.sql)
			trig_checkStmt(t, stmt, tc)
		})
	}
}

func testParseCreateTriggerErrors(t *testing.T) {
	t.Helper()
	runCreateTriggerErrorSubtest(t, "error - missing trigger name", "CREATE TRIGGER BEFORE INSERT ON users BEGIN SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing timing", "CREATE TRIGGER my_trigger INSERT ON users BEGIN SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing event", "CREATE TRIGGER my_trigger BEFORE ON users BEGIN SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing ON keyword", "CREATE TRIGGER my_trigger BEFORE INSERT users BEGIN SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing table name", "CREATE TRIGGER my_trigger BEFORE INSERT ON BEGIN SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing BEGIN", "CREATE TRIGGER my_trigger BEFORE INSERT ON users SELECT 1; END")
	runCreateTriggerErrorSubtest(t, "error - missing END", "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN SELECT 1;")
	runCreateTriggerErrorSubtest(t, "error - instead without OF", "CREATE TRIGGER my_trigger INSTEAD INSERT ON users BEGIN SELECT 1; END")
}

func parseDropTriggerStmt(t *testing.T, sql string) *DropTriggerStmt {
	t.Helper()
	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt, ok := stmts[0].(*DropTriggerStmt)
	if !ok {
		t.Fatalf("expected DropTriggerStmt, got %T", stmts[0])
	}
	return stmt
}

func TestParseDropTrigger(t *testing.T) {
	t.Parallel()

	t.Run("simple drop trigger", func(t *testing.T) {
		t.Parallel()
		stmt := parseDropTriggerStmt(t, "DROP TRIGGER my_trigger")
		if stmt.Name != "my_trigger" {
			t.Errorf("expected trigger name 'my_trigger', got %s", stmt.Name)
		}
		if stmt.IfExists {
			t.Error("expected IfExists to be false")
		}
	})

	t.Run("drop trigger if exists", func(t *testing.T) {
		t.Parallel()
		stmt := parseDropTriggerStmt(t, "DROP TRIGGER IF EXISTS my_trigger")
		if stmt.Name != "my_trigger" {
			t.Errorf("expected trigger name 'my_trigger', got %s", stmt.Name)
		}
		if !stmt.IfExists {
			t.Error("expected IfExists to be true")
		}
	})

	errorTests := []struct {
		name string
		sql  string
	}{
		{"error - missing trigger name", "DROP TRIGGER"},
		{"error - if without exists", "DROP TRIGGER IF my_trigger"},
	}
	for _, tt := range errorTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

func TestTriggerEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "trigger with semicolons in body",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN UPDATE counter SET count = count + 1; INSERT INTO log VALUES (1); END",
			wantErr: false,
		},
		{
			name:    "trigger with empty body statements",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN ; ; SELECT 1; ; END",
			wantErr: false,
		},
		{
			name:    "trigger with quoted identifiers",
			sql:     `CREATE TRIGGER "my trigger" BEFORE INSERT ON "my table" BEGIN SELECT 1; END`,
			wantErr: false,
		},
		{
			name:    "trigger with complex when expression",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users WHEN NEW.age > 18 AND NEW.status = 'active' BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger on update with single column",
			sql:     "CREATE TRIGGER my_trigger BEFORE UPDATE OF status ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
		{
			name:    "trigger on update with many columns",
			sql:     "CREATE TRIGGER my_trigger BEFORE UPDATE OF col1, col2, col3, col4, col5 ON users BEGIN SELECT 1; END",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
			}
		})
	}
}
