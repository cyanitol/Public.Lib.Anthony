// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func parseVirtualTableStmt(t *testing.T, sql string) *CreateVirtualTableStmt {
	t.Helper()
	p := NewParser(sql)
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
	return stmt
}

func assertVirtualTable(t *testing.T, stmt *CreateVirtualTableStmt, wantName, wantModule string, wantArgs []string, wantExists bool) {
	t.Helper()
	if stmt.Name != wantName {
		t.Errorf("Name = %q, want %q", stmt.Name, wantName)
	}
	if stmt.Module != wantModule {
		t.Errorf("Module = %q, want %q", stmt.Module, wantModule)
	}
	if len(stmt.Args) != len(wantArgs) {
		t.Errorf("len(Args) = %d, want %d", len(stmt.Args), len(wantArgs))
	} else {
		for i, arg := range stmt.Args {
			if arg != wantArgs[i] {
				t.Errorf("Args[%d] = %q, want %q", i, arg, wantArgs[i])
			}
		}
	}
	if stmt.IfNotExists != wantExists {
		t.Errorf("IfNotExists = %v, want %v", stmt.IfNotExists, wantExists)
	}
}

func TestParseCreateVirtualTable(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantModule string
		wantArgs   []string
		wantExists bool
	}{
		{"basic fts5", "CREATE VIRTUAL TABLE t1 USING fts5(content)", "t1", "fts5", []string{"content"}, false},
		{"fts5 multiple columns", "CREATE VIRTUAL TABLE docs USING fts5(title, body, author)", "docs", "fts5", []string{"title", "body", "author"}, false},
		{"rtree", "CREATE VIRTUAL TABLE rt1 USING rtree(id, minx, maxx, miny, maxy)", "rt1", "rtree", []string{"id", "minx", "maxx", "miny", "maxy"}, false},
		{"if not exists", "CREATE VIRTUAL TABLE IF NOT EXISTS t1 USING fts5(content)", "t1", "fts5", []string{"content"}, true},
		{"no args", "CREATE VIRTUAL TABLE t1 USING mymodule()", "t1", "mymodule", []string{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseVirtualTableStmt(t, tt.sql)
			assertVirtualTable(t, stmt, tt.wantName, tt.wantModule, tt.wantArgs, tt.wantExists)
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
