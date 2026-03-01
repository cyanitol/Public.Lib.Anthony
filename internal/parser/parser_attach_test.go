// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

func TestParseAttach(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantSchema string
	}{
		{
			name:       "attach with DATABASE keyword",
			sql:        "ATTACH DATABASE 'file.db' AS mydb",
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:       "attach without DATABASE keyword",
			sql:        "ATTACH 'file.db' AS mydb",
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:       "attach with double quoted filename",
			sql:        `ATTACH DATABASE "file.db" AS mydb`,
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:       "attach with quoted schema name",
			sql:        "ATTACH 'file.db' AS \"my-schema\"",
			wantErr:    false,
			wantSchema: "my-schema",
		},
		{
			name:       "attach with expression",
			sql:        "ATTACH DATABASE 'dir/' || 'file.db' AS mydb",
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:    "attach missing AS",
			sql:     "ATTACH 'file.db' mydb",
			wantErr: true,
		},
		{
			name:    "attach missing schema name",
			sql:     "ATTACH 'file.db' AS",
			wantErr: true,
		},
		{
			name:    "attach missing filename",
			sql:     "ATTACH AS mydb",
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
			stmt, ok := stmts[0].(*AttachStmt)
			if !ok {
				t.Errorf("expected AttachStmt, got %T", stmts[0])
				return
			}
			if stmt.SchemaName != tt.wantSchema {
				t.Errorf("expected schema name %q, got %q", tt.wantSchema, stmt.SchemaName)
			}
			if stmt.Filename == nil {
				t.Errorf("expected filename expression, got nil")
			}
		})
	}
}

func TestParseDetach(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantSchema string
	}{
		{
			name:       "detach with DATABASE keyword",
			sql:        "DETACH DATABASE mydb",
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:       "detach without DATABASE keyword",
			sql:        "DETACH mydb",
			wantErr:    false,
			wantSchema: "mydb",
		},
		{
			name:       "detach with quoted schema name",
			sql:        "DETACH DATABASE \"my-schema\"",
			wantErr:    false,
			wantSchema: "my-schema",
		},
		{
			name:       "detach with backtick quoted name",
			sql:        "DETACH `my-schema`",
			wantErr:    false,
			wantSchema: "my-schema",
		},
		{
			name:    "detach missing schema name",
			sql:     "DETACH",
			wantErr: true,
		},
		{
			name:    "detach with DATABASE but missing schema",
			sql:     "DETACH DATABASE",
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
			stmt, ok := stmts[0].(*DetachStmt)
			if !ok {
				t.Errorf("expected DetachStmt, got %T", stmts[0])
				return
			}
			if stmt.SchemaName != tt.wantSchema {
				t.Errorf("expected schema name %q, got %q", tt.wantSchema, stmt.SchemaName)
			}
		})
	}
}

func TestParseAttachDetachCombined(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		stmtCount  int
		stmtTypes  []string
	}{
		{
			name:      "multiple attach statements",
			sql:       "ATTACH 'db1.db' AS db1; ATTACH 'db2.db' AS db2",
			wantErr:   false,
			stmtCount: 2,
			stmtTypes: []string{"ATTACH", "ATTACH"},
		},
		{
			name:      "attach then detach",
			sql:       "ATTACH 'temp.db' AS temp; DETACH temp",
			wantErr:   false,
			stmtCount: 2,
			stmtTypes: []string{"ATTACH", "DETACH"},
		},
		{
			name:      "detach then attach",
			sql:       "DETACH old; ATTACH 'new.db' AS new",
			wantErr:   false,
			stmtCount: 2,
			stmtTypes: []string{"DETACH", "ATTACH"},
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
			if len(stmts) != tt.stmtCount {
				t.Errorf("expected %d statements, got %d", tt.stmtCount, len(stmts))
				return
			}
			for i, stmt := range stmts {
				stmtType := stmt.String()
				if stmtType != tt.stmtTypes[i] {
					t.Errorf("statement %d: expected type %q, got %q", i, tt.stmtTypes[i], stmtType)
				}
			}
		})
	}
}
