// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

import (
	"testing"
)

func TestParseCreateVirtualTable(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantModule string
		wantArgs   []string
		wantExists bool
	}{
		{
			name:       "basic fts5",
			sql:        "CREATE VIRTUAL TABLE t1 USING fts5(content)",
			wantName:   "t1",
			wantModule: "fts5",
			wantArgs:   []string{"content"},
			wantExists: false,
		},
		{
			name:       "fts5 multiple columns",
			sql:        "CREATE VIRTUAL TABLE docs USING fts5(title, body, author)",
			wantName:   "docs",
			wantModule: "fts5",
			wantArgs:   []string{"title", "body", "author"},
			wantExists: false,
		},
		{
			name:       "rtree",
			sql:        "CREATE VIRTUAL TABLE rt1 USING rtree(id, minx, maxx, miny, maxy)",
			wantName:   "rt1",
			wantModule: "rtree",
			wantArgs:   []string{"id", "minx", "maxx", "miny", "maxy"},
			wantExists: false,
		},
		{
			name:       "if not exists",
			sql:        "CREATE VIRTUAL TABLE IF NOT EXISTS t1 USING fts5(content)",
			wantName:   "t1",
			wantModule: "fts5",
			wantArgs:   []string{"content"},
			wantExists: true,
		},
		{
			name:       "no args",
			sql:        "CREATE VIRTUAL TABLE t1 USING mymodule()",
			wantName:   "t1",
			wantModule: "mymodule",
			wantArgs:   []string{},
			wantExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			stmt, ok := stmts[0].(*CreateVirtualTableStmt)
			if !ok {
				t.Fatalf("expected *CreateVirtualTableStmt, got %T", stmts[0])
			}

			if stmt.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", stmt.Name, tt.wantName)
			}

			if stmt.Module != tt.wantModule {
				t.Errorf("Module = %q, want %q", stmt.Module, tt.wantModule)
			}

			if len(stmt.Args) != len(tt.wantArgs) {
				t.Errorf("len(Args) = %d, want %d", len(stmt.Args), len(tt.wantArgs))
			} else {
				for i, arg := range stmt.Args {
					if arg != tt.wantArgs[i] {
						t.Errorf("Args[%d] = %q, want %q", i, arg, tt.wantArgs[i])
					}
				}
			}

			if stmt.IfNotExists != tt.wantExists {
				t.Errorf("IfNotExists = %v, want %v", stmt.IfNotExists, tt.wantExists)
			}
		})
	}
}

func TestParseCreateVirtualTableErrors(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "missing using",
			sql:     "CREATE VIRTUAL TABLE t1 fts5(content)",
			wantErr: "expected USING",
		},
		{
			name:    "missing module",
			sql:     "CREATE VIRTUAL TABLE t1 USING",
			wantErr: "expected module name",
		},
		{
			name:    "missing table after virtual",
			sql:     "CREATE VIRTUAL t1 USING fts5(content)",
			wantErr: "expected TABLE after VIRTUAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.sql)
			_, err := p.Parse()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
