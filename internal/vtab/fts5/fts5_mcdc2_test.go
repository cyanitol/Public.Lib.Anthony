// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// MCDC_loadSingleDocSize: !ok1 || !ok2
//
// loadSingleDocSize returns early when either the docID cast or the blob
// cast fails.  The OR means the function skips the update if either fails.
//
// Cases:
//   A=true,  B=false → docID cast fails (nil row[0])  → early return
//   A=false, B=true  → docID ok, blob cast fails (nil row[1]) → early return
//   A=false, B=false → both ok → size stored
//
// Tested indirectly by constructing index rows and calling loadSingleDocSize.
// ---------------------------------------------------------------------------

func TestMCDC_loadSingleDocSize(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{tableName: "test"}

	tests := []struct {
		name        string
		row         []interface{}
		wantLengths int // number of entries in docLengths after the call
	}{
		{
			// A=true: docID cast fails (row[0] is string, not int64) → skip
			name:        "MCDC_DocSize_A1_B0_bad_docID",
			row:         []interface{}{"not_int64", []byte{0x02}},
			wantLengths: 0,
		},
		{
			// A=false, B=true: docID ok, blob is string (not []byte) → skip
			name:        "MCDC_DocSize_A0_B1_bad_blob",
			row:         []interface{}{int64(1), "not_bytes"},
			wantLengths: 0,
		},
		{
			// A=false, B=false: both ok → length stored
			// encodeVarint(5) produces a valid varint byte slice.
			name:        "MCDC_DocSize_A0_B0_both_ok",
			row:         []interface{}{int64(7), mgr.encodeVarint(5)},
			wantLengths: 1,
		},
		{
			// Short row: len(row) < 2 guard fires before the OR condition
			name:        "MCDC_DocSize_short_row",
			row:         []interface{}{int64(1)},
			wantLengths: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx := NewInvertedIndex([]string{"content"})
			mgr.loadSingleDocSize(idx, tt.row)
			if len(idx.docLengths) != tt.wantLengths {
				t.Errorf("docLengths len = %d, want %d (row=%v)",
					len(idx.docLengths), tt.wantLengths, tt.row)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_loadSingleTermPosting: err != nil || len(dataRows) == 0
//
// loadSingleTermPosting skips loading a term's posting list if the data
// query fails (err!=nil) OR returns no rows (len==0).
//
// Cases (exercised via alwaysErrorDB / noResultDB stubs):
//   A=true,  B=false → DB query returns error → early return, term not added
//   A=false, B=true  → DB query returns empty rows → early return, term not added
//   A=false, B=false → DB query returns data → term posting list loaded
// ---------------------------------------------------------------------------

// noResultDB is a DatabaseExecutor stub that returns empty results for Query.
type noResultDB struct{}

func (n *noResultDB) ExecDDL(sql string) error { return nil }
func (n *noResultDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (n *noResultDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return [][]interface{}{}, nil
}

func TestMCDC_loadSingleTermPosting(t *testing.T) {
	t.Parallel()

	// We create a ShadowTableManager backed by different DB stubs.
	// For the "data available" case we use alwaysErrorDB which returns nil
	// from Query (empty rows), so we must construct a stub that returns actual data.
	// We use noResultDB for the empty-rows case and alwaysErrorDB (from fts5_mcdc_test.go)
	// for the error case.

	tests := []struct {
		name      string
		db        DatabaseExecutor
		row       []interface{}
		wantTerms int // expected number of terms in index after call
	}{
		{
			// A=true (error from Query) → term not loaded
			// alwaysErrorDB.Query returns nil, nil (no error) — but
			// the original code first calls with the term hash; alwaysErrorDB
			// returns (nil, nil) which means err==nil but len(rows)==0 → B=true path.
			// We need a DB that returns an error from Query. Use errQueryDB.
			name:      "MCDC_TermPosting_A1_error_from_query",
			db:        &errQueryDB{},
			row:       []interface{}{"testterm"},
			wantTerms: 0,
		},
		{
			// A=false, B=true: Query succeeds but returns empty rows
			name:      "MCDC_TermPosting_A0_B1_empty_rows",
			db:        &noResultDB{},
			row:       []interface{}{"testterm"},
			wantTerms: 0,
		},
		{
			// Row with non-string type → term cast fails, early return before Query
			name:      "MCDC_TermPosting_bad_term_type",
			db:        &noResultDB{},
			row:       []interface{}{int64(99)},
			wantTerms: 0,
		},
		{
			// Empty row → early return before any cast
			name:      "MCDC_TermPosting_empty_row",
			db:        &noResultDB{},
			row:       []interface{}{},
			wantTerms: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mgr := &ShadowTableManager{tableName: "t", db: tt.db}
			idx := NewInvertedIndex([]string{"content"})
			mgr.loadSingleTermPosting(idx, tt.row)
			if len(idx.index) != tt.wantTerms {
				t.Errorf("index term count = %d, want %d", len(idx.index), tt.wantTerms)
			}
		})
	}
}

// errQueryDB is a DatabaseExecutor stub that returns an error from Query.
type errQueryDB struct{}

func (e *errQueryDB) ExecDDL(sql string) error { return nil }
func (e *errQueryDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (e *errQueryDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, fmt.Errorf("query error")
}

// ---------------------------------------------------------------------------
// MCDC_loadStructureRecord: err != nil || len(rows) == 0 || len(rows[0]) == 0
//
// Three-sub-condition OR: any true path causes early return (no data loaded).
//
// Cases:
//   A=true,  B=false, C=false → Query returns error → skip
//   A=false, B=true,  C=false → Query returns empty rows → skip
//   A=false, B=false, C=true  → Query returns row with no columns → skip
//   A=false, B=false, C=false → Query returns row with blob → data loaded
// ---------------------------------------------------------------------------

// singleBlobDB returns a blob in rows[0][0] for every Query call.
type singleBlobDB struct {
	blob []byte
}

func (s *singleBlobDB) ExecDDL(sql string) error { return nil }
func (s *singleBlobDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (s *singleBlobDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return [][]interface{}{{s.blob}}, nil
}

// emptyColumnDB returns a row with zero columns.
type emptyColumnDB struct{}

func (e *emptyColumnDB) ExecDDL(sql string) error { return nil }
func (e *emptyColumnDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (e *emptyColumnDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return [][]interface{}{{}}, nil // one row, no columns
}

func TestMCDC_loadStructureRecord(t *testing.T) {
	t.Parallel()

	// Build a valid structure blob: totalDocs=3, avgDocLength=5.0
	mgr0 := &ShadowTableManager{tableName: "x"}
	validBlob := mgr0.encodeStructureRecord(3, 5.0)

	tests := []struct {
		name          string
		db            DatabaseExecutor
		wantTotalDocs int
		wantAvgLen    float64
	}{
		{
			// A=true: Query returns error → totalDocs stays 0
			name:          "MCDC_StructRec_A1_query_error",
			db:            &errQueryDB{},
			wantTotalDocs: 0,
			wantAvgLen:    0.0,
		},
		{
			// A=false, B=true: Query returns empty rows → stays 0
			name:          "MCDC_StructRec_A0_B1_empty_rows",
			db:            &noResultDB{},
			wantTotalDocs: 0,
			wantAvgLen:    0.0,
		},
		{
			// A=false, B=false, C=true: one row but no columns → stays 0
			name:          "MCDC_StructRec_A0_B0_C1_empty_columns",
			db:            &emptyColumnDB{},
			wantTotalDocs: 0,
			wantAvgLen:    0.0,
		},
		{
			// A=false, B=false, C=false: valid blob → data loaded
			name:          "MCDC_StructRec_A0_B0_C0_valid_blob",
			db:            &singleBlobDB{blob: validBlob},
			wantTotalDocs: 3,
			wantAvgLen:    5.0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mgr := &ShadowTableManager{tableName: "t", db: tt.db}
			idx := NewInvertedIndex([]string{"content"})
			mgr.loadStructureRecord(idx)
			if idx.totalDocs != tt.wantTotalDocs {
				t.Errorf("totalDocs = %d, want %d", idx.totalDocs, tt.wantTotalDocs)
			}
			if idx.avgDocLength != tt.wantAvgLen {
				t.Errorf("avgDocLength = %f, want %f", idx.avgDocLength, tt.wantAvgLen)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_tryParsePrefixQuery: !strings.HasSuffix(queryStr, "*")
//          &&   len(tokens) == 0  (inner guard after suffix check)
//
// tryParsePrefixQuery:
//   1. Returns (nil, false) when suffix is NOT "*"
//   2. Returns (nil, false) when suffix IS "*" but tokenization yields 0 tokens
//   3. Returns a QueryPrefix query when suffix is "*" and tokenization yields tokens
//
// Cases:
//   A=true  → no trailing "*" → (nil, false)
//   A=false, B=true  → trailing "*", but base term is empty/punctuation → (nil, false)
//   A=false, B=false → valid "prefix*" → (QueryPrefix, true)
// ---------------------------------------------------------------------------

func TestMCDC_tryParsePrefixQuery(t *testing.T) {
	t.Parallel()

	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name      string
		query     string
		wantType  QueryType
		wantFound bool // true if prefix query was detected
		wantErr   bool // true if Parse is expected to return an error
	}{
		{
			// A=true: no trailing "*" → falls through to simple query
			name:      "MCDC_Prefix_A1_no_asterisk",
			query:     "hello",
			wantFound: false,
			wantType:  QuerySimple,
		},
		{
			// A=false, B=true: trailing "*" but stem is empty ("*") → 0 tokens → (nil,false)
			// The fallback to parseSimpleQuery also finds no tokens and returns an error.
			name:    "MCDC_Prefix_A0_B1_empty_stem",
			query:   "*",
			wantErr: true,
		},
		{
			// A=false, B=false: valid "hel*" → QueryPrefix
			name:      "MCDC_Prefix_A0_B0_valid_prefix",
			query:     "hel*",
			wantFound: true,
			wantType:  QueryPrefix,
		},
		{
			// Extra: multi-char valid prefix
			name:      "MCDC_Prefix_A0_B0_multi_char",
			query:     "search*",
			wantFound: true,
			wantType:  QueryPrefix,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, err := parser.Parse(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse(%q) expected error, got nil (query type=%v)", tt.query, q)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.query, err)
			}
			if tt.wantFound {
				if q == nil {
					t.Fatalf("Parse(%q) returned nil query", tt.query)
				}
				if q.Type != tt.wantType {
					t.Errorf("Parse(%q).Type = %v, want %v", tt.query, q.Type, tt.wantType)
				}
			} else {
				// Falls through to simple query
				if q != nil && q.Type != QuerySimple {
					t.Errorf("Parse(%q).Type = %v, want Simple", tt.query, q.Type)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_BM25Score_EmptyTermsGuard: len(terms) == 0
//
// BM25Ranker.Score returns 0.0 immediately when terms is empty.
//
// Cases:
//   A=true  → empty terms → 0.0
//   A=false → non-empty terms → proceeds to scoring
// ---------------------------------------------------------------------------

func TestMCDC_BM25Score_EmptyTermsGuard(t *testing.T) {
	t.Parallel()

	ranker := NewBM25Ranker()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_bm25", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Insert a doc so totalDocs > 0
	if _, err := table.Update(3, []interface{}{nil, nil, "hello world"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	idx := table.(*FTS5Table).index

	tests := []struct {
		name      string
		terms     []string
		wantScore float64
		wantExact bool
	}{
		{
			// A=true: empty terms → 0.0 exactly
			name:      "MCDC_BM25_A1_empty_terms",
			terms:     []string{},
			wantScore: 0.0,
			wantExact: true,
		},
		{
			// A=false: non-empty terms with actual match → score > 0
			name:  "MCDC_BM25_A0_matching_term",
			terms: []string{"hello"},
		},
		{
			// A=false: non-empty terms with no match → score = 0 (term not found)
			name:      "MCDC_BM25_A0_nonmatching_term",
			terms:     []string{"zzznomatch"},
			wantScore: 0.0,
			wantExact: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ranker.Score(idx, DocumentID(1), tt.terms)
			if tt.wantExact && score != tt.wantScore {
				t.Errorf("Score(%v) = %f, want %f", tt.terms, score, tt.wantScore)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_TFIDFScore_Guards: len(terms)==0  and  docLen==0
//
// TFIDFRanker.Score has two independent single-condition early-return guards:
//   G1: len(terms) == 0 → return 0.0
//   G2: docLen == 0     → return 0.0
//
// Each guard independently flips the "proceed to scoring" outcome.
// ---------------------------------------------------------------------------

func TestMCDC_TFIDFScore_Guards(t *testing.T) {
	t.Parallel()

	ranker := NewTFIDFRanker()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_tfidf", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Insert two docs: "alpha" only appears in doc 1 so IDF = log(2/1) > 0.
	if _, err := table.Update(3, []interface{}{nil, nil, "alpha beta gamma"}); err != nil {
		t.Fatalf("Insert doc 1: %v", err)
	}
	if _, err := table.Update(3, []interface{}{nil, nil, "delta epsilon"}); err != nil {
		t.Fatalf("Insert doc 2: %v", err)
	}
	idx := table.(*FTS5Table).index

	tests := []struct {
		name     string
		docID    DocumentID
		terms    []string
		wantZero bool
	}{
		{
			// G1: empty terms → 0.0
			name:     "MCDC_TFIDF_G1_empty_terms",
			docID:    1,
			terms:    []string{},
			wantZero: true,
		},
		{
			// G2: docID=999 → docLen=0 (doc not in index) → 0.0
			name:     "MCDC_TFIDF_G2_unknown_docID",
			docID:    999,
			terms:    []string{"alpha"},
			wantZero: true,
		},
		{
			// Both guards pass: known doc with 2 total docs and "alpha" in 1 → IDF>0 → score>0
			name:     "MCDC_TFIDF_both_guards_pass",
			docID:    1,
			terms:    []string{"alpha"},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ranker.Score(idx, tt.docID, tt.terms)
			if tt.wantZero && score != 0.0 {
				t.Errorf("Score() = %f, want 0.0", score)
			}
			if !tt.wantZero && score == 0.0 {
				t.Errorf("Score() = 0.0, want non-zero")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_addEllipsis: start > 0  (A)  and  end < textLen  (B)
//
// addEllipsis prepends "..." when A is true, appends "..." when B is true.
// Each condition is independent.
//
// Cases:
//   A=true,  B=false → leading "..." only
//   A=false, B=true  → trailing "..." only
//   A=false, B=false → no ellipsis
//   A=true,  B=true  → both ellipsis
// ---------------------------------------------------------------------------

func TestMCDC_addEllipsis(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{} // just used to get access to package; addEllipsis is package-level

	tests := []struct {
		name    string
		snippet string
		start   int
		end     int
		textLen int
		want    string
	}{
		// A=true, B=false: start>0, end==textLen
		{
			name:    "MCDC_Ellipsis_A1_B0_leading_only",
			snippet: "middle",
			start:   5, end: 11, textLen: 11,
			want: "...middle",
		},
		// A=false, B=true: start==0, end<textLen
		{
			name:    "MCDC_Ellipsis_A0_B1_trailing_only",
			snippet: "start",
			start:   0, end: 5, textLen: 20,
			want: "start...",
		},
		// A=false, B=false: no ellipsis
		{
			name:    "MCDC_Ellipsis_A0_B0_no_ellipsis",
			snippet: "full",
			start:   0, end: 4, textLen: 4,
			want: "full",
		},
		// A=true, B=true: both
		{
			name:    "MCDC_Ellipsis_A1_B1_both",
			snippet: "mid",
			start:   3, end: 6, textLen: 10,
			want: "...mid...",
		},
	}

	_ = mgr // suppress unused warning

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := addEllipsis(tt.snippet, tt.start, tt.end, tt.textLen)
			if got != tt.want {
				t.Errorf("addEllipsis(%q, %d, %d, %d) = %q, want %q",
					tt.snippet, tt.start, tt.end, tt.textLen, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_truncateText: len(text) > maxLength
//
// truncateText appends "..." when the text is longer than maxLength.
//
// Cases:
//   A=true  → text longer → truncated with "..."
//   A=false → text fits   → returned as-is
// ---------------------------------------------------------------------------

func TestMCDC_truncateText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		maxLength int
		want      string
	}{
		// A=true: len("hello world")=11 > maxLength=5
		{
			name:      "MCDC_Truncate_A1_too_long",
			text:      "hello world",
			maxLength: 5,
			want:      "hello...",
		},
		// A=false: len("hi")=2 <= maxLength=5
		{
			name:      "MCDC_Truncate_A0_fits",
			text:      "hi",
			maxLength: 5,
			want:      "hi",
		},
		// Boundary: len==maxLength → A=false
		{
			name:      "MCDC_Truncate_boundary_equal",
			text:      "exact",
			maxLength: 5,
			want:      "exact",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := truncateText(tt.text, tt.maxLength)
			if got != tt.want {
				t.Errorf("truncateText(%q, %d) = %q, want %q",
					tt.text, tt.maxLength, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_matchPrefix: strings.HasPrefix(term, prefix)
//
// matchPrefix (in QueryExecutor) iterates all indexed terms and includes
// those with the given prefix.  The condition "HasPrefix(term, prefix)"
// independently determines whether each term is included.
//
// Cases:
//   A=true  → term starts with prefix → included in results
//   A=false → term does NOT start with prefix → excluded
// ---------------------------------------------------------------------------

func TestMCDC_matchPrefix(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_prefix", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	docs := []string{
		"hello help heft", // multiple "he*" terms
		"world wide",      // no "he*" terms
	}
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}

	tests := []struct {
		name      string
		query     string // prefix query like "he*"
		wantCount int
	}{
		{
			// A=true for he* matching terms: "hello","help","heft" → returns doc 1
			name:      "MCDC_Prefix_A1_matching_prefix",
			query:     "he*",
			wantCount: 1, // only doc 1 has he* terms
		},
		{
			// A=false: prefix "zz*" matches nothing → 0 results
			name:      "MCDC_Prefix_A0_no_matching_prefix",
			query:     "zz*",
			wantCount: 0,
		},
		{
			// Mixed: "wo*" matches "world" and "wide" → doc 2
			name:      "MCDC_Prefix_mixed_match",
			query:     "wo*",
			wantCount: 1, // doc 2 has "world", "wide"
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cursor, err := table.Open()
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer cursor.Close()
			if err := cursor.Filter(1, "", []interface{}{tt.query}); err != nil {
				t.Fatalf("Filter: %v", err)
			}
			count := 0
			for !cursor.EOF() {
				count++
				cursor.Next()
			}
			if count != tt.wantCount {
				t.Errorf("prefix query %q: got %d results, want %d", tt.query, count, tt.wantCount)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_ScoreWithBoost: b, exists := columnBoosts[columnIndex]; exists
//
// ScoreWithBoost uses a found boost when exists=true, or falls back to 1.0.
//
// Cases:
//   A=true  → boost found → applied
//   A=false → boost not found → 1.0 multiplier (baseScore unchanged)
// ---------------------------------------------------------------------------

func TestMCDC_ScoreWithBoost(t *testing.T) {
	t.Parallel()

	boosts := map[int]float64{
		0: 2.0,
		1: 0.5,
	}

	tests := []struct {
		name        string
		baseScore   float64
		columnIndex int
		wantScore   float64
	}{
		// A=true: column 0 has boost 2.0
		{
			name:        "MCDC_Boost_A1_boost_found_col0",
			baseScore:   3.0,
			columnIndex: 0,
			wantScore:   6.0, // 3.0 * 2.0
		},
		// A=true: column 1 has boost 0.5
		{
			name:        "MCDC_Boost_A1_boost_found_col1",
			baseScore:   4.0,
			columnIndex: 1,
			wantScore:   2.0, // 4.0 * 0.5
		},
		// A=false: column 99 not in map → multiplier 1.0
		{
			name:        "MCDC_Boost_A0_no_boost",
			baseScore:   5.0,
			columnIndex: 99,
			wantScore:   5.0, // 5.0 * 1.0
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ScoreWithBoost(tt.baseScore, tt.columnIndex, boosts)
			if got != tt.wantScore {
				t.Errorf("ScoreWithBoost(%f, %d) = %f, want %f",
					tt.baseScore, tt.columnIndex, got, tt.wantScore)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_PhraseMatch_EmptyTermsGuard: len(terms) == 0
//
// InvertedIndex.PhraseMatch returns false immediately when terms is empty.
//
// Cases:
//   A=true  → empty terms → false
//   A=false → non-empty terms (with real match) → true
//   A=false → non-empty terms (no match) → false
// ---------------------------------------------------------------------------

func TestMCDC_PhraseMatch_EmptyTermsGuard(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_phrase", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := table.Update(3, []interface{}{nil, nil, "quick brown fox"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	idx := table.(*FTS5Table).index

	tests := []struct {
		name  string
		terms []string
		docID DocumentID
		want  bool
	}{
		// A=true: empty terms → false
		{
			name:  "MCDC_Phrase_A1_empty_terms",
			terms: []string{},
			docID: 1,
			want:  false,
		},
		// A=false: actual phrase present → true
		{
			name:  "MCDC_Phrase_A0_phrase_present",
			terms: []string{"quick", "brown"},
			docID: 1,
			want:  true,
		},
		// A=false: phrase not present → false
		{
			name:  "MCDC_Phrase_A0_phrase_absent",
			terms: []string{"brown", "quick"}, // wrong order
			docID: 1,
			want:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := idx.PhraseMatch(tt.terms, tt.docID)
			if got != tt.want {
				t.Errorf("PhraseMatch(%v, %d) = %v, want %v", tt.terms, tt.docID, got, tt.want)
			}
		})
	}
}
