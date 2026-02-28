package parser

import (
	"testing"
)

func TestParseCreateTrigger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, *CreateTriggerStmt)
	}{
		{
			name:    "simple trigger - before insert",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if stmt.Name != "my_trigger" {
					t.Errorf("expected trigger name 'my_trigger', got %s", stmt.Name)
				}
				if stmt.Timing != TriggerBefore {
					t.Errorf("expected BEFORE timing, got %v", stmt.Timing)
				}
				if stmt.Event != TriggerInsert {
					t.Errorf("expected INSERT event, got %v", stmt.Event)
				}
				if stmt.Table != "users" {
					t.Errorf("expected table 'users', got %s", stmt.Table)
				}
				if len(stmt.Body) != 1 {
					t.Errorf("expected 1 statement in body, got %d", len(stmt.Body))
				}
			},
		},
		{
			name:    "trigger - after update",
			sql:     "CREATE TRIGGER audit_trigger AFTER UPDATE ON employees BEGIN INSERT INTO audit_log VALUES (NEW.id, NEW.name); END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if stmt.Timing != TriggerAfter {
					t.Errorf("expected AFTER timing, got %v", stmt.Timing)
				}
				if stmt.Event != TriggerUpdate {
					t.Errorf("expected UPDATE event, got %v", stmt.Event)
				}
			},
		},
		{
			name:    "trigger - after delete",
			sql:     "CREATE TRIGGER delete_trigger AFTER DELETE ON products BEGIN DELETE FROM inventory WHERE product_id = OLD.id; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if stmt.Event != TriggerDelete {
					t.Errorf("expected DELETE event, got %v", stmt.Event)
				}
			},
		},
		{
			name:    "trigger - instead of",
			sql:     "CREATE TRIGGER view_trigger INSTEAD OF INSERT ON my_view BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if stmt.Timing != TriggerInsteadOf {
					t.Errorf("expected INSTEAD OF timing, got %v", stmt.Timing)
				}
			},
		},
		{
			name:    "trigger - with if not exists",
			sql:     "CREATE TRIGGER IF NOT EXISTS my_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if !stmt.IfNotExists {
					t.Errorf("expected IfNotExists to be true")
				}
			},
		},
		{
			name:    "temp trigger",
			sql:     "CREATE TEMP TRIGGER temp_trigger BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if !stmt.Temp {
					t.Errorf("expected Temp to be true")
				}
			},
		},
		{
			name:    "trigger - for each row",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users FOR EACH ROW BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if !stmt.ForEachRow {
					t.Errorf("expected ForEachRow to be true")
				}
			},
		},
		{
			name:    "trigger - with when clause",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users WHEN NEW.age > 18 BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if stmt.When == nil {
					t.Errorf("expected When clause to be present")
				}
			},
		},
		{
			name:    "trigger - update of specific columns",
			sql:     "CREATE TRIGGER my_trigger BEFORE UPDATE OF name, email ON users BEGIN SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if len(stmt.UpdateOf) != 2 {
					t.Errorf("expected 2 columns in UpdateOf, got %d", len(stmt.UpdateOf))
				}
				if stmt.UpdateOf[0] != "name" || stmt.UpdateOf[1] != "email" {
					t.Errorf("expected columns [name, email], got %v", stmt.UpdateOf)
				}
			},
		},
		{
			name:    "trigger - multiple statements in body",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN UPDATE counter SET count = count + 1; INSERT INTO log VALUES (1); SELECT 1; END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if len(stmt.Body) != 3 {
					t.Errorf("expected 3 statements in body, got %d", len(stmt.Body))
				}
			},
		},
		{
			name:    "trigger - complete example",
			sql:     "CREATE TEMP TRIGGER IF NOT EXISTS audit_trigger AFTER UPDATE OF salary ON employees FOR EACH ROW WHEN NEW.salary > OLD.salary BEGIN INSERT INTO audit_log (emp_id, old_salary, new_salary, timestamp) VALUES (NEW.id, OLD.salary, NEW.salary, datetime('now')); END",
			wantErr: false,
			check: func(t *testing.T, stmt *CreateTriggerStmt) {
				if !stmt.Temp {
					t.Errorf("expected Temp to be true")
				}
				if !stmt.IfNotExists {
					t.Errorf("expected IfNotExists to be true")
				}
				if stmt.Timing != TriggerAfter {
					t.Errorf("expected AFTER timing")
				}
				if stmt.Event != TriggerUpdate {
					t.Errorf("expected UPDATE event")
				}
				if len(stmt.UpdateOf) != 1 || stmt.UpdateOf[0] != "salary" {
					t.Errorf("expected UpdateOf [salary], got %v", stmt.UpdateOf)
				}
				if !stmt.ForEachRow {
					t.Errorf("expected ForEachRow to be true")
				}
				if stmt.When == nil {
					t.Errorf("expected When clause to be present")
				}
			},
		},
		{
			name:    "error - missing trigger name",
			sql:     "CREATE TRIGGER BEFORE INSERT ON users BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing timing",
			sql:     "CREATE TRIGGER my_trigger INSERT ON users BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing event",
			sql:     "CREATE TRIGGER my_trigger BEFORE ON users BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing ON keyword",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT users BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing table name",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON BEGIN SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing BEGIN",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users SELECT 1; END",
			wantErr: true,
		},
		{
			name:    "error - missing END",
			sql:     "CREATE TRIGGER my_trigger BEFORE INSERT ON users BEGIN SELECT 1;",
			wantErr: true,
		},
		{
			name:    "error - instead without OF",
			sql:     "CREATE TRIGGER my_trigger INSTEAD INSERT ON users BEGIN SELECT 1; END",
			wantErr: true,
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

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*CreateTriggerStmt)
			if !ok {
				t.Errorf("expected CreateTriggerStmt, got %T", stmts[0])
				return
			}

			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}

func TestParseDropTrigger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*testing.T, *DropTriggerStmt)
	}{
		{
			name:    "simple drop trigger",
			sql:     "DROP TRIGGER my_trigger",
			wantErr: false,
			check: func(t *testing.T, stmt *DropTriggerStmt) {
				if stmt.Name != "my_trigger" {
					t.Errorf("expected trigger name 'my_trigger', got %s", stmt.Name)
				}
				if stmt.IfExists {
					t.Errorf("expected IfExists to be false")
				}
			},
		},
		{
			name:    "drop trigger if exists",
			sql:     "DROP TRIGGER IF EXISTS my_trigger",
			wantErr: false,
			check: func(t *testing.T, stmt *DropTriggerStmt) {
				if stmt.Name != "my_trigger" {
					t.Errorf("expected trigger name 'my_trigger', got %s", stmt.Name)
				}
				if !stmt.IfExists {
					t.Errorf("expected IfExists to be true")
				}
			},
		},
		{
			name:    "error - missing trigger name",
			sql:     "DROP TRIGGER",
			wantErr: true,
		},
		{
			name:    "error - if without exists",
			sql:     "DROP TRIGGER IF my_trigger",
			wantErr: true,
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

			if tt.wantErr {
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			stmt, ok := stmts[0].(*DropTriggerStmt)
			if !ok {
				t.Errorf("expected DropTriggerStmt, got %T", stmts[0])
				return
			}

			if tt.check != nil {
				tt.check(t, stmt)
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
