// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

func TestParseReindex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantSchema string
	}{
		{
			name:       "REINDEX all",
			sql:        "REINDEX",
			wantName:   "",
			wantSchema: "",
		},
		{
			name:       "REINDEX table",
			sql:        "REINDEX t1",
			wantName:   "t1",
			wantSchema: "",
		},
		{
			name:       "REINDEX index",
			sql:        "REINDEX idx1",
			wantName:   "idx1",
			wantSchema: "",
		},
		{
			name:       "REINDEX with schema",
			sql:        "REINDEX main.t1",
			wantName:   "t1",
			wantSchema: "main",
		},
		{
			name:       "REINDEX index with schema",
			sql:        "REINDEX main.idx1",
			wantName:   "idx1",
			wantSchema: "main",
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

			reindexStmt, ok := stmts[0].(*ReindexStmt)
			if !ok {
				t.Fatalf("expected *ReindexStmt, got %T", stmts[0])
			}

			if reindexStmt.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", reindexStmt.Name, tt.wantName)
			}

			if reindexStmt.Schema != tt.wantSchema {
				t.Errorf("Schema = %q, want %q", reindexStmt.Schema, tt.wantSchema)
			}
		})
	}
}
