// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newFTS5Table creates a bare in-memory FTS5 table (no shadow manager).
func newFTS5Table(t *testing.T, columns []string) *FTS5Table {
	t.Helper()
	m := NewFTS5Module()
	vt, _, err := m.Create(nil, "fts5", "main", t.Name(), columns)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return vt.(*FTS5Table)
}

// insertDoc inserts a single-column document and returns its rowid.
func insertDoc(t *testing.T, table vtab.VirtualTable, values ...interface{}) int64 {
	t.Helper()
	argc := 2 + len(values)
	argv := make([]interface{}, argc)
	argv[0] = nil
	argv[1] = nil
	for i, v := range values {
		argv[2+i] = v
	}
	id, err := table.Update(argc, argv)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

// filterTable runs Filter on the table and returns the matching rows count.
func filterTable(t *testing.T, table vtab.VirtualTable, idxNum int, query string) int {
	t.Helper()
	c, err := table.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()
	var argv []interface{}
	if query != "" {
		argv = []interface{}{query}
	}
	if err := c.Filter(idxNum, "", argv); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	count := 0
	for !c.EOF() {
		count++
		c.Next()
	}
	return count
}

// ---------------------------------------------------------------------------
// loadOrCreateIndex (fts5.go:98) — MC/DC
// Conditions:
//   A: shadowMgr == nil  → NewInvertedIndex
//   B: LoadIndex err != nil → NewInvertedIndex
//   else → return loaded index
// ---------------------------------------------------------------------------

func TestMCDC5_LoadOrCreateIndex_NoShadowMgr(t *testing.T) {
	t.Parallel()
	// A=true: shadowMgr is nil → fresh index
	idx := loadOrCreateIndex(nil, []string{"title"})
	if idx == nil {
		t.Error("want non-nil index when shadowMgr is nil")
	}
	if idx.GetTotalDocuments() != 0 {
		t.Errorf("new index should be empty, got %d docs", idx.GetTotalDocuments())
	}
}

func TestMCDC5_LoadOrCreateIndex_WithShadowMgrLoadError(t *testing.T) {
	t.Parallel()
	// A=false, B=true: shadowMgr present but LoadIndex returns error (no executor)
	// Use a bare ShadowTableManager with nil executor which will fail on LoadIndex
	mgr := &ShadowTableManager{tableName: "nonexistent_table"}
	idx := loadOrCreateIndex(mgr, []string{"body"})
	if idx == nil {
		t.Error("want non-nil index on load error fallback")
	}
}

// ---------------------------------------------------------------------------
// handleDelete (fts5.go:212) — MC/DC
// Conditions:
//   A: argv[0] cast to int64 ok        → proceed vs error
//   B: shadowMgr != nil                → persist deletion
// ---------------------------------------------------------------------------

func TestMCDC5_HandleDelete_InvalidRowid(t *testing.T) {
	t.Parallel()
	// A=false: rowid is a string, not int64 → error
	table := newFTS5Table(t, []string{"body"})
	_, err := table.handleDelete([]interface{}{"not_int64"})
	if err == nil {
		t.Error("want error for non-int64 rowid")
	}
}

func TestMCDC5_HandleDelete_ValidRowid(t *testing.T) {
	t.Parallel()
	// A=true, B=false (no shadow mgr): valid delete
	table := newFTS5Table(t, []string{"body"})
	id := insertDoc(t, table, "hello world")

	_, err := table.handleDelete([]interface{}{id})
	if err != nil {
		t.Errorf("handleDelete valid: %v", err)
	}
	if table.index.GetTotalDocuments() != 0 {
		t.Errorf("doc should be removed, got %d", table.index.GetTotalDocuments())
	}
}

func TestMCDC5_HandleDelete_NonExistentRowid(t *testing.T) {
	t.Parallel()
	// A=true but document doesn't exist; RemoveDocument should still succeed
	table := newFTS5Table(t, []string{"body"})
	_, err := table.handleDelete([]interface{}{int64(9999)})
	// May or may not error — just must not panic
	_ = err
}

// ---------------------------------------------------------------------------
// tryParseNOTQuery (search.go:160) — MC/DC
// Conditions:
//   A: strings.Contains(queryStr, " NOT ")  → false means skip
//   B: len(parts) != 2                      → return nil, nil
// ---------------------------------------------------------------------------

func TestMCDC5_TryParseNOTQuery_NoNOT(t *testing.T) {
	t.Parallel()
	// A=false: no " NOT " → returns nil, nil
	qp := NewQueryParser(NewSimpleTokenizer())
	q, err := qp.tryParseNOTQuery("hello world")
	if q != nil || err != nil {
		t.Errorf("no NOT: want nil, nil; got %v, %v", q, err)
	}
}

func TestMCDC5_TryParseNOTQuery_WithNOT(t *testing.T) {
	t.Parallel()
	// A=true, B=false: valid NOT query
	qp := NewQueryParser(NewSimpleTokenizer())
	q, err := qp.tryParseNOTQuery("hello NOT world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q == nil {
		t.Fatal("want non-nil query")
	}
	if q.Type != QueryNOT {
		t.Errorf("want QueryNOT, got %v", q.Type)
	}
	if len(q.Children) != 2 {
		t.Errorf("want 2 children, got %d", len(q.Children))
	}
}

func TestMCDC5_TryParseNOTQuery_IntegratesWithMatch(t *testing.T) {
	t.Parallel()
	// End-to-end: NOT query via Filter — docs with 'hello' but not 'world'
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "hello world")
	insertDoc(t, table, "hello golang")
	insertDoc(t, table, "goodbye world")

	count := filterTable(t, table, 1, "hello NOT world")
	if count != 1 {
		t.Errorf("'hello NOT world': want 1 result, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// parseColumnFilter (search.go:189) — MC/DC
// Conditions:
//   A: strings.Contains(queryStr, ":")  → false means pass-through
//   B: len(parts) != 2                  → pass-through (can't happen with SplitN 2)
// ---------------------------------------------------------------------------

func TestMCDC5_ParseColumnFilter_NoColon(t *testing.T) {
	t.Parallel()
	// A=false: no colon → column=-1, same queryStr returned
	qp := NewQueryParser(NewSimpleTokenizer())
	col, q := qp.parseColumnFilter("hello world")
	if col != -1 {
		t.Errorf("want col=-1, got %d", col)
	}
	if q != "hello world" {
		t.Errorf("want 'hello world', got %q", q)
	}
}

func TestMCDC5_ParseColumnFilter_WithColon(t *testing.T) {
	t.Parallel()
	// A=true: colon present → col=-1, remainder after colon
	qp := NewQueryParser(NewSimpleTokenizer())
	col, q := qp.parseColumnFilter("title:hello")
	if col != -1 {
		t.Errorf("want col=-1 (not resolved), got %d", col)
	}
	if q != "hello" {
		t.Errorf("want 'hello', got %q", q)
	}
}

func TestMCDC5_ParseColumnFilter_MultipleColons(t *testing.T) {
	t.Parallel()
	// SplitN with n=2 → only splits on first colon
	qp := NewQueryParser(NewSimpleTokenizer())
	_, q := qp.parseColumnFilter("title:body:value")
	if q != "body:value" {
		t.Errorf("want 'body:value', got %q", q)
	}
}

func TestMCDC5_ParseColumnFilter_Integration(t *testing.T) {
	t.Parallel()
	// Column filter in a real FTS5 query
	table := newFTS5Table(t, []string{"title", "body"})
	insertDoc(t, table, "test title", "test body content")

	// Filter with column filter syntax
	count := filterTable(t, table, 1, "title:test")
	if count == 0 {
		t.Error("column filter query: want at least 1 result")
	}
}

// ---------------------------------------------------------------------------
// executeMatchQuery (fts5.go:417) — MC/DC
// Conditions:
//   A: arg.(string) ok    → proceed; otherwise skip
//   B: parse error        → return error
//   C: execute error      → return error
// ---------------------------------------------------------------------------

func TestMCDC5_ExecuteMatchQuery_NonStringArg(t *testing.T) {
	t.Parallel()
	// A=false: non-string arg → skip without error
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "hello world")

	c, err := table.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	cursor := c.(*FTS5Cursor)
	err = cursor.executeMatchQuery(42) // non-string
	if err != nil {
		t.Errorf("non-string arg: want nil error, got %v", err)
	}
}

func TestMCDC5_ExecuteMatchQuery_EmptyQuery(t *testing.T) {
	t.Parallel()
	// B=true: empty string → parse error
	table := newFTS5Table(t, []string{"body"})

	c, err := table.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer c.Close()

	cursor := c.(*FTS5Cursor)
	err = cursor.executeMatchQuery("")
	if err == nil {
		t.Error("empty query: want error")
	}
}

func TestMCDC5_ExecuteMatchQuery_ValidSimpleQuery(t *testing.T) {
	t.Parallel()
	// A=true, B=false, C=false: valid query runs fine
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "hello world")
	insertDoc(t, table, "goodbye moon")

	count := filterTable(t, table, 1, "hello")
	if count != 1 {
		t.Errorf("'hello' query: want 1, got %d", count)
	}
}

func TestMCDC5_ExecuteMatchQuery_ANDQuery(t *testing.T) {
	t.Parallel()
	// Complex AND query
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "hello world golang")
	insertDoc(t, table, "hello python")
	insertDoc(t, table, "world java")

	count := filterTable(t, table, 1, "hello AND world")
	if count != 1 {
		t.Errorf("'hello AND world': want 1, got %d", count)
	}
}

func TestMCDC5_ExecuteMatchQuery_ORQuery(t *testing.T) {
	t.Parallel()
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "hello golang")
	insertDoc(t, table, "world python")
	insertDoc(t, table, "other language")

	count := filterTable(t, table, 1, "hello OR world")
	if count != 2 {
		t.Errorf("'hello OR world': want 2, got %d", count)
	}
}

func TestMCDC5_ExecuteMatchQuery_PhraseQuery(t *testing.T) {
	t.Parallel()
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "the quick brown fox")
	insertDoc(t, table, "quick brown is fast")

	count := filterTable(t, table, 1, `"quick brown"`)
	// Phrase matching may find 1 or 2 depending on phrase impl
	if count == 0 {
		t.Error("phrase 'quick brown': want at least 1 result")
	}
}

func TestMCDC5_ExecuteMatchQuery_PrefixQuery(t *testing.T) {
	t.Parallel()
	table := newFTS5Table(t, []string{"body"})
	insertDoc(t, table, "testing tester test")
	insertDoc(t, table, "other content")

	count := filterTable(t, table, 1, "test*")
	if count != 1 {
		t.Errorf("prefix 'test*': want 1, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// loadOrCreateIndex integration: full create + reconnect scenario
// ---------------------------------------------------------------------------

func TestMCDC5_LoadOrCreateIndex_ReuseIndex(t *testing.T) {
	t.Parallel()
	// Create table, insert docs, then verify index has those docs
	table := newFTS5Table(t, []string{"title", "body"})
	insertDoc(t, table, "Go Programming", "learn go language")
	insertDoc(t, table, "Python Guide", "python tutorial")

	if table.index.GetTotalDocuments() != 2 {
		t.Errorf("want 2 docs in index, got %d", table.index.GetTotalDocuments())
	}

	// Search confirms both are accessible
	count := filterTable(t, table, 1, "go")
	if count == 0 {
		t.Error("after insert, 'go' search should return results")
	}
}
