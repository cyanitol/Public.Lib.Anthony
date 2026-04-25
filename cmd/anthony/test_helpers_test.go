// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package main

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony"
)

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
