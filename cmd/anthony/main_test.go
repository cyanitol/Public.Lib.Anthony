// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package main

import (
	"database/sql"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony"
)

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

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := anthony.Open(":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	return db
}

func mustRunStatement(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()

	if err := runStatement(db, stmt); err != nil {
		t.Fatalf("runStatement(%q) error = %v", stmt, err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}

	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	return string(output)
}

type stubResult struct {
	rowsAffected int64
}

func (r stubResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r stubResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
