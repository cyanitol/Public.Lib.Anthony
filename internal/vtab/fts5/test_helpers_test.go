// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// newFTS5TableForTest creates an FTS5 table with the given columns, failing on error.
func newFTS5TableForTest(t *testing.T, tableName string, columns []string) vtab.VirtualTable {
	t.Helper()
	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", tableName, columns)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return table
}

// insertFTS5Doc inserts a document into an FTS5 table, failing on error.
func insertFTS5Doc(t *testing.T, table vtab.VirtualTable, values ...interface{}) int64 {
	t.Helper()
	args := make([]interface{}, 0, len(values)+2)
	args = append(args, nil, nil)
	args = append(args, values...)
	rowid, err := table.Update(len(args), args)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return rowid
}

// insertFTS5Docs inserts multiple single-column documents.
func insertFTS5Docs(t *testing.T, table vtab.VirtualTable, docs []string) {
	t.Helper()
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}
}

// countQueryResults opens a cursor, filters with the query, and counts results.
func countQueryResults(t *testing.T, table vtab.VirtualTable, query string) int {
	t.Helper()
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()
	if err := cursor.Filter(1, "", []interface{}{query}); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	count := 0
	for !cursor.EOF() {
		count++
		cursor.Next()
	}
	return count
}

// openCursorForScan opens a cursor and does an unfiltered scan (Filter with idxNum=0).
func openCursorForScan(t *testing.T, table vtab.VirtualTable) vtab.VirtualCursor {
	t.Helper()
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := cursor.Filter(0, "", nil); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	return cursor
}
