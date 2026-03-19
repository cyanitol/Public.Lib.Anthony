// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// createIntegrationTable creates a single-column FTS5 table for integration tests.
func createIntegrationTable(t *testing.T) vtab.VirtualTable {
	t.Helper()
	module := NewFTS5Module()
	if module == nil {
		t.Fatal("Failed to create FTS5 module")
	}
	table, schema, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}
	if table == nil {
		t.Fatal("Created table is nil")
	}
	if schema == "" {
		t.Error("Expected non-empty schema")
	}
	return table
}

// insertFTS5Rows inserts text rows into an FTS5 table.
func insertFTS5Rows(t *testing.T, table vtab.VirtualTable, rows []string) {
	t.Helper()
	for _, text := range rows {
		_, err := table.Update(3, []interface{}{nil, nil, text})
		if err != nil {
			t.Fatalf("Failed to insert %q: %v", text, err)
		}
	}
}

// assertSingleMatchResult filters for a query and asserts exactly one result with the expected string.
func assertSingleMatchResult(t *testing.T, table vtab.VirtualTable, query, expected string) {
	t.Helper()
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	if err := cursor.Filter(1, "", []interface{}{query}); err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}
	if cursor.EOF() {
		t.Fatal("Expected at least one result, got EOF")
	}
	value, err := cursor.Column(0)
	if err != nil {
		t.Fatalf("Failed to get column value: %v", err)
	}
	strValue, ok := value.(string)
	if !ok {
		t.Fatalf("Expected string value, got %T", value)
	}
	if strValue != expected {
		t.Errorf("Expected %q, got %q", expected, strValue)
	}
	if err := cursor.Next(); err != nil {
		t.Fatalf("Failed to move to next: %v", err)
	}
	if !cursor.EOF() {
		t.Error("Expected EOF after first result")
	}
}

// TestBasicFTS5Integration tests basic FTS5 functionality end-to-end.
func TestBasicFTS5Integration(t *testing.T) {
	t.Parallel()
	table := createIntegrationTable(t)
	insertFTS5Rows(t, table, []string{"hello world", "foo bar"})
	assertSingleMatchResult(t, table, "hello", "hello world")
}

// TestFTS5MultipleTerms tests searching with multiple terms.
func TestFTS5MultipleTerms(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Insert test data
	testData := []string{
		"the quick brown fox",
		"jumps over the lazy dog",
		"hello world",
		"quick test",
	}

	for _, text := range testData {
		_, err = table.Update(3, []interface{}{nil, nil, text})
		if err != nil {
			t.Fatalf("Failed to insert %q: %v", text, err)
		}
	}

	// Search for "quick"
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	err = cursor.Filter(1, "", []interface{}{"quick"})
	if err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}

	// Count results
	count := 0
	for !cursor.EOF() {
		count++
		cursor.Next()
	}

	// Should match "the quick brown fox" and "quick test"
	if count != 2 {
		t.Errorf("Expected 2 results for 'quick', got %d", count)
	}
}

// TestFTS5EmptyQuery tests behavior with empty queries.
func TestFTS5EmptyQuery(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Insert test data
	_, err = table.Update(3, []interface{}{nil, nil, "test data"})
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	// Filter with empty string should return error
	err = cursor.Filter(1, "", []interface{}{""})
	if err == nil {
		t.Error("Expected error for empty query, got nil")
	}
}

// TestFTS5BestIndex tests the BestIndex implementation.
func TestFTS5BestIndex(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Test with MATCH constraint
	info := &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 0, Op: vtab.ConstraintMatch, Usable: true},
		},
		ConstraintUsage: make([]vtab.IndexConstraintUsage, 1),
	}

	err = table.BestIndex(info)
	if err != nil {
		t.Fatalf("BestIndex failed: %v", err)
	}

	if info.IdxNum != 1 {
		t.Errorf("Expected IdxNum=1 for MATCH, got %d", info.IdxNum)
	}

	if info.EstimatedCost >= 1000000.0 {
		t.Errorf("Expected low cost for FTS search, got %f", info.EstimatedCost)
	}

	// Test without MATCH constraint (full table scan)
	info2 := &vtab.IndexInfo{
		Constraints:     []vtab.IndexConstraint{},
		ConstraintUsage: []vtab.IndexConstraintUsage{},
	}

	err = table.BestIndex(info2)
	if err != nil {
		t.Fatalf("BestIndex failed: %v", err)
	}

	if info2.IdxNum != 0 {
		t.Errorf("Expected IdxNum=0 for full scan, got %d", info2.IdxNum)
	}

	if info2.EstimatedCost < 1000000.0 {
		t.Errorf("Expected high cost for full scan, got %f", info2.EstimatedCost)
	}
}

// matchHasResults returns true if filtering for query produces at least one result.
func matchHasResults(t *testing.T, table vtab.VirtualTable, query string) bool {
	t.Helper()
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()
	if err := cursor.Filter(1, "", []interface{}{query}); err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}
	return !cursor.EOF()
}

// TestFTS5Delete tests deleting documents from the FTS index.
func TestFTS5Delete(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	rowid, err := table.Update(3, []interface{}{nil, nil, "test document"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if !matchHasResults(t, table, "test") {
		t.Fatal("Expected document to exist before delete")
	}

	if _, err := table.Update(1, []interface{}{rowid}); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if matchHasResults(t, table, "test") {
		t.Error("Expected document to be deleted")
	}
}

// TestFTS5Update tests updating documents in the FTS index.
func TestFTS5Update(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	rowid, err := table.Update(3, []interface{}{nil, nil, "original text"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if _, err := table.Update(4, []interface{}{rowid, rowid, "updated text"}); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	if matchHasResults(t, table, "original") {
		t.Error("Should not find original text after update")
	}
	if !matchHasResults(t, table, "updated") {
		t.Error("Should find updated text after update")
	}
}

// TestFTS5CaseInsensitive tests case-insensitive search.
func TestFTS5CaseInsensitive(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "t1", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	// Insert with mixed case
	_, err = table.Update(3, []interface{}{nil, nil, "Hello WORLD"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Search with lowercase (should match due to tokenizer normalization)
	cursor, err := table.Open()
	if err != nil {
		t.Fatalf("Failed to open cursor: %v", err)
	}
	defer cursor.Close()

	err = cursor.Filter(1, "", []interface{}{"hello"})
	if err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}

	if cursor.EOF() {
		t.Error("Expected case-insensitive match for 'hello'")
	}
}
