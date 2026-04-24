// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// mockDB implements DatabaseExecutor for unit testing.
type mockDB struct {
	ddlCalls []string
	dmlCalls []mockDMLCall
	tables   map[string][]mockRow
}

type mockDMLCall struct {
	sql  string
	args []interface{}
}

type mockRow = []interface{}

func newMockDB() *mockDB {
	return &mockDB{
		tables: make(map[string][]mockRow),
	}
}

func (m *mockDB) ExecDDL(sql string) error {
	m.ddlCalls = append(m.ddlCalls, sql)
	return nil
}

func (m *mockDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	m.dmlCalls = append(m.dmlCalls, mockDMLCall{sql: sql, args: args})

	// Simulate storage for round-trip tests.
	// Extract table name from INSERT OR REPLACE INTO <table>(...) VALUES(...)
	if strings.HasPrefix(sql, "INSERT OR REPLACE INTO ") {
		tableName := strings.SplitN(sql[len("INSERT OR REPLACE INTO "):], "(", 2)[0]
		m.tables[tableName] = append(m.tables[tableName], append([]interface{}{}, args...))
	}
	return 1, nil
}

func (m *mockDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	// Dispatch based on SQL pattern to return stored data.
	for tbl, rows := range m.tables {
		if !strings.Contains(sql, tbl) {
			continue
		}
		return m.queryTable(sql, tbl, rows, args)
	}
	return nil, fmt.Errorf("no data")
}

func (m *mockDB) queryTable(sql, tbl string, rows []mockRow, args []interface{}) ([][]interface{}, error) {
	if strings.Contains(sql, "WHERE") && len(args) > 0 {
		return m.queryWithFilter(tbl, rows, args)
	}
	// Return all rows (for docsize scan).
	var result [][]interface{}
	for _, row := range rows {
		result = append(result, row)
	}
	return result, nil
}

func (m *mockDB) queryWithFilter(tbl string, rows []mockRow, args []interface{}) ([][]interface{}, error) {
	wantID := args[0]
	var result [][]interface{}
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		if row[0] == wantID {
			// Return remaining columns after the id.
			result = append(result, row[1:])
		}
	}
	return result, nil
}

// TestPersistenceCreateShadowTables verifies that CreateShadowTables issues
// the expected DDL for all five shadow tables.
func TestPersistenceCreateShadowTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		columns []string
		wantDDL int
	}{
		{
			name:    "single column",
			columns: []string{"content"},
			wantDDL: 5,
		},
		{
			name:    "multiple columns",
			columns: []string{"title", "body", "tags"},
			wantDDL: 5,
		},
		{
			name:    "no columns",
			columns: []string{},
			wantDDL: 5,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newMockDB()
			mgr := NewShadowTableManager("test_ft", db)

			err := mgr.CreateShadowTables(tt.columns)
			if err != nil {
				t.Fatalf("CreateShadowTables() error: %v", err)
			}

			if len(db.ddlCalls) != tt.wantDDL {
				t.Errorf("expected %d DDL calls, got %d", tt.wantDDL, len(db.ddlCalls))
			}

			// Verify expected table suffixes appear in DDL.
			suffixes := []string{"_data", "_idx", "_config", "_docsize", "_content"}
			for i, suffix := range suffixes {
				if i >= len(db.ddlCalls) {
					break
				}
				if !strings.Contains(db.ddlCalls[i], "test_ft"+suffix) {
					t.Errorf("DDL[%d] missing table %q: %s", i, "test_ft"+suffix, db.ddlCalls[i])
				}
			}
		})
	}
}

// TestPersistenceNilDB verifies that nil db parameter results in graceful no-ops.
func TestPersistenceNilDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*ShadowTableManager) error
	}{
		{
			name: "CreateShadowTables",
			fn: func(m *ShadowTableManager) error {
				return m.CreateShadowTables([]string{"content"})
			},
		},
		{
			name: "DropShadowTables",
			fn: func(m *ShadowTableManager) error {
				return m.DropShadowTables()
			},
		},
		{
			name: "SaveIndex nil db",
			fn: func(m *ShadowTableManager) error {
				idx := NewInvertedIndex([]string{"content"})
				return m.SaveIndex(idx)
			},
		},
		{
			name: "SaveIndex nil index",
			fn: func(m *ShadowTableManager) error {
				return m.SaveIndex(nil)
			},
		},
		{
			name: "SaveContent",
			fn: func(m *ShadowTableManager) error {
				return m.SaveContent(1, []interface{}{"hello"})
			},
		},
		{
			name: "DeleteContent",
			fn: func(m *ShadowTableManager) error {
				return m.DeleteContent(1)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mgr := NewShadowTableManager("test_ft", nil)
			if err := tt.fn(mgr); err != nil {
				t.Errorf("expected nil error with nil db, got: %v", err)
			}
		})
	}
}

// TestPersistenceSaveIndex verifies that SaveIndex writes structure record,
// terms, and document sizes via DML calls.
func TestPersistenceSaveIndex(t *testing.T) {
	t.Parallel()

	db := newMockDB()
	mgr := NewShadowTableManager("ft", db)

	idx := NewInvertedIndex([]string{"content"})
	idx.totalDocs = 2
	idx.avgDocLength = 3.5
	idx.index["hello"] = []PostingList{
		{DocID: 1, Frequency: 1, Positions: []int{0}},
	}
	idx.docLengths[DocumentID(1)] = 3
	idx.docLengths[DocumentID(2)] = 4

	err := mgr.SaveIndex(idx)
	if err != nil {
		t.Fatalf("SaveIndex() error: %v", err)
	}

	if len(db.dmlCalls) == 0 {
		t.Fatal("SaveIndex() produced no DML calls")
	}

	// Should have: 1 structure record + 1 _idx + 1 _data (posting) + 2 docsize = 5 DML calls
	expectedMin := 5
	if len(db.dmlCalls) < expectedMin {
		t.Errorf("expected at least %d DML calls, got %d", expectedMin, len(db.dmlCalls))
	}

	// Verify structure record write (id=10).
	firstCall := db.dmlCalls[0]
	if !strings.Contains(firstCall.sql, "ft_data") {
		t.Errorf("first DML should target ft_data, got: %s", firstCall.sql)
	}
	if len(firstCall.args) >= 1 {
		if id, ok := firstCall.args[0].(int64); !ok || id != 10 {
			t.Errorf("structure record id should be 10, got: %v", firstCall.args[0])
		}
	}
}

// TestPersistenceLoadIndexNilDB verifies LoadIndex with nil db returns a fresh index.
func TestPersistenceLoadIndexNilDB(t *testing.T) {
	t.Parallel()

	mgr := NewShadowTableManager("ft", nil)
	idx, err := mgr.LoadIndex([]string{"content"})
	if err != nil {
		t.Fatalf("LoadIndex() error: %v", err)
	}
	if idx == nil {
		t.Fatal("LoadIndex() returned nil index")
	}
	if idx.totalDocs != 0 {
		t.Errorf("expected 0 totalDocs, got %d", idx.totalDocs)
	}
}

// buildTestIndex creates a test InvertedIndex with known data.
func buildTestIndex(columns []string) *InvertedIndex {
	original := NewInvertedIndex(columns)
	original.totalDocs = 3
	original.avgDocLength = 2.5
	original.index["world"] = []PostingList{
		{DocID: 1, Frequency: 2, Positions: []int{0, 5}},
		{DocID: 2, Frequency: 1, Positions: []int{3}},
	}
	original.index["hello"] = []PostingList{
		{DocID: 1, Frequency: 1, Positions: []int{0}},
	}
	original.docLengths[DocumentID(1)] = 4
	original.docLengths[DocumentID(2)] = 3
	original.docLengths[DocumentID(3)] = 1
	return original
}

// verifyIndexMatch asserts that the loaded index matches the original.
func verifyIndexMatch(t *testing.T, original, loaded *InvertedIndex) {
	t.Helper()
	if loaded.totalDocs != original.totalDocs {
		t.Errorf("totalDocs: want %d, got %d", original.totalDocs, loaded.totalDocs)
	}
	if loaded.avgDocLength != original.avgDocLength {
		t.Errorf("avgDocLength: want %f, got %f", original.avgDocLength, loaded.avgDocLength)
	}
	verifyIndexTerms(t, original, loaded)
	verifyDocLengths(t, original, loaded)
}

func verifyIndexTerms(t *testing.T, original, loaded *InvertedIndex) {
	t.Helper()
	for term, wantPostings := range original.index {
		gotPostings, ok := loaded.index[term]
		if !ok {
			t.Errorf("term %q not found in loaded index", term)
			continue
		}
		if !reflect.DeepEqual(gotPostings, wantPostings) {
			t.Errorf("postings for %q mismatch", term)
		}
	}
}

func verifyDocLengths(t *testing.T, original, loaded *InvertedIndex) {
	t.Helper()
	for docID, wantLen := range original.docLengths {
		gotLen, ok := loaded.docLengths[docID]
		if !ok {
			t.Errorf("docLength for %d not found", docID)
		} else if gotLen != wantLen {
			t.Errorf("docLength[%d]: want %d, got %d", docID, wantLen, gotLen)
		}
	}
}

// TestPersistenceRoundTrip verifies that saving and then loading an index
// preserves the structure record, terms, posting lists, and document sizes.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	db := newRoundTripDB()
	mgr := NewShadowTableManager("rt", db)

	columns := []string{"content"}
	if err := mgr.CreateShadowTables(columns); err != nil {
		t.Fatalf("CreateShadowTables() error: %v", err)
	}

	original := buildTestIndex(columns)
	if err := mgr.SaveIndex(original); err != nil {
		t.Fatalf("SaveIndex() error: %v", err)
	}

	loaded, err := mgr.LoadIndex(columns)
	if err != nil {
		t.Fatalf("LoadIndex() error: %v", err)
	}

	verifyIndexMatch(t, original, loaded)
}

// TestPersistenceBinarySerialization tests the encoding/decoding helpers directly.
func TestPersistenceBinarySerialization(t *testing.T) {
	t.Parallel()

	mgr := NewShadowTableManager("test", nil)

	t.Run("structure record round-trip", func(t *testing.T) {
		t.Parallel()
		testBinSerStructureRoundTrip(t, mgr)
	})
	t.Run("structure record short blob", func(t *testing.T) {
		t.Parallel()
		testBinSerStructureShortBlob(t, mgr)
	})
	t.Run("posting list round-trip", func(t *testing.T) {
		t.Parallel()
		testBinSerPostingRoundTrip(t, mgr)
	})
	t.Run("posting list empty", func(t *testing.T) {
		t.Parallel()
		testBinSerPostingEmpty(t, mgr)
	})
	t.Run("posting list short blob", func(t *testing.T) {
		t.Parallel()
		testBinSerPostingShortBlob(t, mgr)
	})
	t.Run("varint round-trip", func(t *testing.T) {
		t.Parallel()
		testBinSerVarintRoundTrip(t, mgr)
	})
}

func testBinSerStructureRoundTrip(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	blob := mgr.encodeStructureRecord(42, 3.14)
	docs, avg := mgr.decodeStructureRecord(blob)
	if docs != 42 {
		t.Errorf("totalDocs: want 42, got %d", docs)
	}
	if avg != 3.14 {
		t.Errorf("avgDocLength: want 3.14, got %f", avg)
	}
}

func testBinSerStructureShortBlob(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	docs, avg := mgr.decodeStructureRecord([]byte{1, 2, 3})
	if docs != 0 || avg != 0 {
		t.Errorf("short blob: want (0, 0), got (%d, %f)", docs, avg)
	}
}

func testBinSerPostingRoundTrip(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	postings := []PostingList{
		{DocID: 10, Frequency: 3, Positions: []int{0, 5, 12}},
		{DocID: 20, Frequency: 1, Positions: []int{7}},
	}
	blob := mgr.encodePostingList(postings)
	decoded := mgr.decodePostingList(blob)
	if !reflect.DeepEqual(decoded, postings) {
		t.Errorf("posting list round-trip failed: want %v, got %v", postings, decoded)
	}
}

func testBinSerPostingEmpty(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	blob := mgr.encodePostingList(nil)
	decoded := mgr.decodePostingList(blob)
	if len(decoded) != 0 {
		t.Errorf("expected empty posting list, got %v", decoded)
	}
}

func testBinSerPostingShortBlob(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	decoded := mgr.decodePostingList([]byte{1})
	if decoded != nil {
		t.Errorf("expected nil for short blob, got %v", decoded)
	}
}

func testBinSerVarintRoundTrip(t *testing.T, mgr *ShadowTableManager) {
	t.Helper()
	values := []int64{0, 1, -1, 127, 128, 1000000, -999999}
	for _, v := range values {
		blob := mgr.encodeVarint(v)
		decoded := mgr.decodeVarint(blob)
		if decoded != v {
			t.Errorf("varint %d: got %d", v, decoded)
		}
	}
}

// TestPersistenceHashTerm verifies hashTerm returns stable, non-conflicting IDs.
func TestPersistenceHashTerm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		term string
	}{
		{name: "simple", term: "hello"},
		{name: "empty", term: ""},
		{name: "unicode", term: "welt"},
		{name: "long", term: strings.Repeat("a", 1000)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id := hashTerm(tt.term)
			if id < 100 {
				t.Errorf("hashTerm(%q) = %d, want >= 100", tt.term, id)
			}
			// Verify determinism.
			if hashTerm(tt.term) != id {
				t.Error("hashTerm is not deterministic")
			}
		})
	}
}

// roundTripDB is a DatabaseExecutor that faithfully stores and retrieves data
// for round-trip testing. It stores rows as lists per table, keyed by the
// first argument (primary key). For tables like _idx where the first arg is
// not a unique key, rows accumulate under the same key.
type roundTripDB struct {
	ddlCalls []string
	// rows maps "tableName" -> list of all inserted rows (each row is args slice)
	rows map[string][][]interface{}
}

func newRoundTripDB() *roundTripDB {
	return &roundTripDB{
		rows: make(map[string][][]interface{}),
	}
}

func (r *roundTripDB) ExecDDL(sql string) error {
	r.ddlCalls = append(r.ddlCalls, sql)
	return nil
}

func (r *roundTripDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	tbl := r.extractTable(sql)
	if tbl == "" {
		return 1, nil
	}
	// For INSERT OR REPLACE, replace existing row with same first arg if present.
	row := append([]interface{}{}, args...)
	replaced := false
	for i, existing := range r.rows[tbl] {
		if len(existing) > 0 && len(row) > 0 && fmt.Sprintf("%v", existing[0]) == fmt.Sprintf("%v", row[0]) {
			// Only replace if it's a single-key table (like _data, _docsize).
			// For _idx (composite key), don't replace, append.
			if !strings.Contains(tbl, "_idx") {
				r.rows[tbl][i] = row
				replaced = true
				break
			}
		}
	}
	if !replaced {
		r.rows[tbl] = append(r.rows[tbl], row)
	}
	return 1, nil
}

func (r *roundTripDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	tbl := r.extractTable(sql)
	if tbl == "" {
		return nil, fmt.Errorf("unknown table in query")
	}
	allRows := r.rows[tbl]
	if len(allRows) == 0 {
		return nil, fmt.Errorf("no data in %s", tbl)
	}

	if strings.Contains(sql, "WHERE") && len(args) > 0 {
		return r.filterRows(allRows, args[0]), nil
	}

	result := make([][]interface{}, len(allRows))
	copy(result, allRows)
	return result, nil
}

func (r *roundTripDB) filterRows(allRows [][]interface{}, key interface{}) [][]interface{} {
	wantKey := fmt.Sprintf("%v", key)
	var result [][]interface{}
	for _, row := range allRows {
		if len(row) > 0 && fmt.Sprintf("%v", row[0]) == wantKey {
			result = append(result, row[1:])
		}
	}
	return result
}

func (r *roundTripDB) extractTable(sql string) string {
	for _, prefix := range []string{"INSERT OR REPLACE INTO ", "DELETE FROM "} {
		if idx := strings.Index(sql, prefix); idx >= 0 {
			rest := sql[idx+len(prefix):]
			return strings.SplitN(rest, "(", 2)[0]
		}
	}
	if idx := strings.Index(sql, "FROM "); idx >= 0 {
		rest := sql[idx+5:]
		rest = strings.SplitN(rest, " ", 2)[0]
		return rest
	}
	return ""
}
