package parser

import (
	"testing"
)

func TestParseVacuum_Basic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple vacuum",
			sql:     "VACUUM",
			wantErr: false,
		},
		{
			name:    "vacuum with schema",
			sql:     "VACUUM main",
			wantErr: false,
		},
		{
			name:    "vacuum into",
			sql:     "VACUUM INTO 'backup.db'",
			wantErr: false,
		},
		{
			name:    "vacuum schema into",
			sql:     "VACUUM main INTO 'backup.db'",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}
			if !tt.wantErr {
				if _, ok := stmts[0].(*VacuumStmt); !ok {
					t.Errorf("expected VacuumStmt, got %T", stmts[0])
				}
			}
		})
	}
}

func TestParseVacuum_Structure(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantSchema string
		wantInto   string
	}{
		{
			name:       "simple vacuum",
			sql:        "VACUUM",
			wantSchema: "",
			wantInto:   "",
		},
		{
			name:       "vacuum with schema",
			sql:        "VACUUM mydb",
			wantSchema: "mydb",
			wantInto:   "",
		},
		{
			name:       "vacuum into",
			sql:        "VACUUM INTO 'output.db'",
			wantSchema: "",
			wantInto:   "output.db",
		},
		{
			name:       "vacuum schema into",
			sql:        "VACUUM main INTO 'backup.db'",
			wantSchema: "main",
			wantInto:   "backup.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			stmt, ok := stmts[0].(*VacuumStmt)
			if !ok {
				t.Fatalf("expected VacuumStmt, got %T", stmts[0])
			}

			if stmt.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", stmt.Schema, tt.wantSchema)
			}

			if stmt.Into != tt.wantInto {
				t.Errorf("Into = %q, want %q", stmt.Into, tt.wantInto)
			}
		})
	}
}

func TestParseVacuum_String(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantString string
	}{
		{
			name:       "simple vacuum",
			sql:        "VACUUM",
			wantString: "VACUUM",
		},
		{
			name:       "vacuum into",
			sql:        "VACUUM INTO 'file.db'",
			wantString: "VACUUM INTO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			stmt, ok := stmts[0].(*VacuumStmt)
			if !ok {
				t.Fatalf("expected VacuumStmt, got %T", stmts[0])
			}

			if stmt.String() != tt.wantString {
				t.Errorf("String() = %q, want %q", stmt.String(), tt.wantString)
			}
		})
	}
}

func TestParseVacuum_MultipleStatements(t *testing.T) {
	sql := `
		CREATE TABLE test (id INTEGER);
		INSERT INTO test VALUES (1);
		VACUUM;
		SELECT * FROM test;
	`

	parser := NewParser(sql)
	stmts, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if len(stmts) != 4 {
		t.Errorf("expected 4 statements, got %d", len(stmts))
	}

	// Verify third statement is VACUUM
	if _, ok := stmts[2].(*VacuumStmt); !ok {
		t.Errorf("statement 2: expected VacuumStmt, got %T", stmts[2])
	}
}

func TestParseVacuum_CaseSensitivity(t *testing.T) {
	tests := []string{
		"VACUUM",
		"vacuum",
		"Vacuum",
		"VaCuUm",
		"VACUUM INTO 'file.db'",
		"vacuum into 'file.db'",
		"VaCuUm InTo 'file.db'",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			parser := NewParser(sql)
			stmts, err := parser.Parse()
			if err != nil {
				t.Errorf("Parse(%q) error = %v", sql, err)
				return
			}

			if len(stmts) != 1 {
				t.Errorf("expected 1 statement, got %d", len(stmts))
				return
			}

			if _, ok := stmts[0].(*VacuumStmt); !ok {
				t.Errorf("expected VacuumStmt, got %T", stmts[0])
			}
		})
	}
}

func TestParseVacuum_AST(t *testing.T) {
	// Verify VACUUM statement implements required interfaces
	var _ Statement = (*VacuumStmt)(nil)
	var _ Node = (*VacuumStmt)(nil)

	// Test node and statement methods
	stmt := &VacuumStmt{}
	stmt.node()
	stmt.statement()

	// Test String() method
	simpleStmt := &VacuumStmt{}
	if simpleStmt.String() != "VACUUM" {
		t.Errorf("Simple VACUUM String() = %q, want %q", simpleStmt.String(), "VACUUM")
	}

	intoStmt := &VacuumStmt{Into: "file.db"}
	if intoStmt.String() != "VACUUM INTO" {
		t.Errorf("VACUUM INTO String() = %q, want %q", intoStmt.String(), "VACUUM INTO")
	}
}

func TestParseVacuum_WithParameter(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantParam   bool
	}{
		{
			name:      "vacuum into with ? parameter",
			sql:       "VACUUM INTO ?",
			wantErr:   false,
			wantParam: true,
		},
		{
			name:      "vacuum schema into with parameter",
			sql:       "VACUUM main INTO ?",
			wantErr:   false,
			wantParam: true,
		},
		{
			name:      "vacuum into with named parameter",
			sql:       "VACUUM INTO :filename",
			wantErr:   false,
			wantParam: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmts, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(stmts) != 1 {
					t.Fatalf("expected 1 statement, got %d", len(stmts))
				}
				stmt, ok := stmts[0].(*VacuumStmt)
				if !ok {
					t.Fatalf("expected VacuumStmt, got %T", stmts[0])
				}
				if stmt.IntoParam != tt.wantParam {
					t.Errorf("IntoParam = %v, want %v", stmt.IntoParam, tt.wantParam)
				}
			}
		})
	}
}
