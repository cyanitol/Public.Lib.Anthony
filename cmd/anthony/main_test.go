// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package main

import (
	"strings"
	"testing"
)

func TestCompatModeFlagDefault(t *testing.T) {
	if got := defaultCompatMode(); got != "hard-compat" {
		t.Fatalf("defaultCompatMode() = %q, want %q", got, "hard-compat")
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "leading comment and two statements",
			input: "-- heading\nSELECT 1;\nSELECT 2;",
			want:  []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:  "trailing statement without semicolon",
			input: "SELECT 1;\nSELECT 2",
			want:  []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:  "blank statements ignored",
			input: " ; \nSELECT 1;;\n",
			want:  []string{"SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.input)
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("splitStatements(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRunStatementQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	mustRunStatement(t, db, "CREATE TABLE items (id INTEGER, name TEXT)")
	mustRunStatement(t, db, "INSERT INTO items VALUES (1, 'alpha')")

	output := captureStdout(t, func() {
		if err := runStatement(db, "SELECT id, name FROM items"); err != nil {
			t.Fatalf("runStatement(select) error = %v", err)
		}
	})

	if !strings.Contains(output, "id\tname") {
		t.Fatalf("query output missing header: %q", output)
	}
	if !strings.Contains(output, "1\talpha") {
		t.Fatalf("query output missing row: %q", output)
	}
}

func TestReportResult(t *testing.T) {
	output := captureStdout(t, func() {
		if err := reportResult(stubResult{rowsAffected: 3}); err != nil {
			t.Fatalf("reportResult() error = %v", err)
		}
	})

	if !strings.Contains(output, "OK (rows affected: 3)") {
		t.Fatalf("exec output missing OK status: %q", output)
	}
}

func TestRunStatementBlank(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	output := captureStdout(t, func() {
		if err := runStatement(db, "   \n\t  "); err != nil {
			t.Fatalf("runStatement(blank) error = %v", err)
		}
	})

	if output != "" {
		t.Fatalf("blank statement produced output %q", output)
	}
}

func TestOpenCLIWithCompatMode(t *testing.T) {
	db := openCompatTestDB(t, "extended")
	defer db.Close()

	mustRunStatement(t, db, "CREATE TABLE items (id INTEGER)")
}
