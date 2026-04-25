// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package engine

import (
	"path/filepath"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

func openEngineTestDB(t *testing.T, name string) *Engine {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func mustEngineExec(t *testing.T, db *Engine, sql string) {
	t.Helper()
	if _, err := db.Execute(sql); err != nil {
		t.Fatalf("Failed to execute %q: %v", sql, err)
	}
}

func mustEngineTable(t *testing.T, db *Engine, name string) *schema.Table {
	t.Helper()
	table, ok := db.schema.GetTable(name)
	if !ok {
		t.Fatalf("Table %q not found", name)
	}
	return table
}
