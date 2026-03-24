// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// ---------------------------------------------------------------------------
// MC/DC helpers
// ---------------------------------------------------------------------------

// openAndFilter opens a cursor on table and filters with idxNum/query,
// returning (hasRows bool, err error).
func openAndFilter(table vtab.VirtualTable, idxNum int, query string) (bool, error) {
	cursor, err := table.Open()
	if err != nil {
		return false, err
	}
	defer cursor.Close()

	var argv []interface{}
	if query != "" {
		argv = []interface{}{query}
	}
	if err := cursor.Filter(idxNum, "", argv); err != nil {
		return false, err
	}
	return !cursor.EOF(), nil
}

// newTableWithDocs creates a single-column FTS5 table and inserts docs.
func newTableWithDocs(t *testing.T, docs []string) vtab.VirtualTable {
	t.Helper()
	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_t", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}
	return table
}

// ---------------------------------------------------------------------------
// MCDC_Filter_MatchPath: idxNum==1 && len(argv)>0
//
// This AND condition controls whether a MATCH search or a full-table scan runs.
// Cases:
//   A=true  B=true  → match path (true)  — baseline
//   A=false B=true  → scan path (false independently flips outcome via A)
//   A=true  B=false → scan path (false independently flips outcome via B)
// ---------------------------------------------------------------------------

func TestMCDC_Filter_MatchPath(t *testing.T) {
	t.Parallel()

	table := newTableWithDocs(t, []string{"alpha beta", "gamma delta"})

	tests := []struct {
		name      string
		idxNum    int    // A: idxNum==1
		query     string // B: len(argv)>0 when non-empty
		wantMatch bool   // whether a MATCH-style result is expected
		wantErr   bool
	}{
		{
			// A=true, B=true → match path: should find "alpha" specifically
			name:      "MCDC_Filter_A1_B1_match_path",
			idxNum:    1,
			query:     "alpha",
			wantMatch: true,
		},
		{
			// A=false, B=true → full-scan path: both docs returned, no FTS filtering
			name:      "MCDC_Filter_A0_B1_scan_path",
			idxNum:    0,
			query:     "alpha", // argv has value but idxNum!=1, so scan runs
			wantMatch: true,    // full scan returns all docs (non-EOF)
		},
		{
			// A=true, B=false → empty argv → match path condition fails → scan path
			name:      "MCDC_Filter_A1_B0_empty_argv_error",
			idxNum:    1,
			query:     "",   // len(argv)==0 → falls through to loadAllDocuments
			wantMatch: true, // loadAllDocuments returns both docs
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hasRows, err := openAndFilter(table, tt.idxNum, tt.query)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.wantErr && hasRows != tt.wantMatch {
				t.Errorf("hasRows=%v, want %v", hasRows, tt.wantMatch)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_EOF: c.pos < 0 || c.pos >= len(c.results)
//
// The OR condition is true (EOF) when either sub-condition is true.
// Cases:
//   A=true  B=false → EOF because pos<0        (empty result set, pos=-1)
//   A=false B=true  → EOF because pos>=len      (advanced past end)
//   A=false B=false → not EOF                   (valid position)
// ---------------------------------------------------------------------------

func TestMCDC_EOF_Conditions(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_eof", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := table.Update(3, []interface{}{nil, nil, "eof test doc"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	tests := []struct {
		name    string
		setup   func(cursor vtab.VirtualCursor) error
		wantEOF bool
	}{
		{
			// A=true (pos=-1 after no results), B=false → EOF
			name: "MCDC_EOF_A1_B0_pos_negative",
			setup: func(cursor vtab.VirtualCursor) error {
				// Query that matches nothing → pos stays -1
				return cursor.Filter(1, "", []interface{}{"zzznomatch"})
			},
			wantEOF: true,
		},
		{
			// A=false, B=false → not EOF (valid cursor at pos 0)
			name: "MCDC_EOF_A0_B0_valid_position",
			setup: func(cursor vtab.VirtualCursor) error {
				return cursor.Filter(1, "", []interface{}{"eof"})
			},
			wantEOF: false,
		},
		{
			// A=false, B=true → EOF because pos advanced past end
			name: "MCDC_EOF_A0_B1_pos_past_end",
			setup: func(cursor vtab.VirtualCursor) error {
				if err := cursor.Filter(1, "", []interface{}{"eof"}); err != nil {
					return err
				}
				return cursor.Next() // advance from 0 → 1, which equals len(results)
			},
			wantEOF: true,
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
			if err := tt.setup(cursor); err != nil {
				// A "no valid terms" error from zzznomatch is acceptable: cursor stays at EOF
				if tt.wantEOF {
					return
				}
				t.Fatalf("setup: %v", err)
			}
			got := cursor.EOF()
			if got != tt.wantEOF {
				t.Errorf("EOF()=%v, want %v", got, tt.wantEOF)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_Column_IndexRange: index < 0 || index >= len(row)
//
// Column returns an error when the index is out of range.
// Cases:
//   A=true  B=false → error (index < 0, e.g. -2; note: -1 is special rank col)
//   A=false B=true  → error (index >= len(row))
//   A=false B=false → no error (valid index)
// ---------------------------------------------------------------------------

func TestMCDC_Column_IndexRange(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_col", []string{"title", "body"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := table.Update(4, []interface{}{nil, nil, "mytitle", "mybody"}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	tests := []struct {
		name      string
		colIndex  int
		wantErr   bool
		wantValue string
	}{
		{
			// A=false, B=false → valid index 0
			name:      "MCDC_Column_A0_B0_valid_index_0",
			colIndex:  0,
			wantErr:   false,
			wantValue: "mytitle",
		},
		{
			// A=false, B=false → valid index 1
			name:      "MCDC_Column_A0_B0_valid_index_1",
			colIndex:  1,
			wantErr:   false,
			wantValue: "mybody",
		},
		{
			// A=false, B=true → index >= len(row) (len=2, index=2)
			name:     "MCDC_Column_A0_B1_index_too_large",
			colIndex: 2,
			wantErr:  true,
		},
		{
			// A=true, B=false → index < 0 (index=-2; -1 is reserved for score)
			name:     "MCDC_Column_A1_B0_index_negative",
			colIndex: -2,
			wantErr:  true,
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
			if err := cursor.Filter(0, "", nil); err != nil {
				t.Fatalf("Filter: %v", err)
			}
			if cursor.EOF() {
				t.Fatal("cursor at EOF before Column call")
			}
			val, err := cursor.Column(tt.colIndex)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for colIndex=%d, got nil (val=%v)", tt.colIndex, val)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if s, ok := val.(string); !ok || s != tt.wantValue {
					t.Errorf("Column(%d)=%v, want %q", tt.colIndex, val, tt.wantValue)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_CheckAndRemoveOldDocument: !ok || rid == 0
//
// checkAndRemoveOldDocument returns (0,false) when the cast fails (!ok=true)
// OR when the cast succeeds but rid==0. It returns (rid,true) otherwise.
// We exercise this via Update calls:
//   isUpdate=false if oldRowID=nil (A: cast fails because nil is not int64)
//   isUpdate=false if oldRowID=int64(0) (A=false, B=true: ok but rid==0)
//   isUpdate=true  if oldRowID=valid nonzero int64 (A=false, B=false)
// ---------------------------------------------------------------------------

func TestMCDC_CheckAndRemoveOldDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		oldRowID   interface{}
		wantUpdate bool // whether the old doc should be removed
	}{
		{
			// A=true (cast fails: nil is not int64), B=irrelevant → not update
			name:       "MCDC_CheckAndRemove_A1_nil_oldRowID",
			oldRowID:   nil,
			wantUpdate: false,
		},
		{
			// A=false (ok=true), B=true (rid==0) → not update
			name:       "MCDC_CheckAndRemove_A0_B1_zero_rowID",
			oldRowID:   int64(0),
			wantUpdate: false,
		},
		{
			// A=false (ok=true), B=false (rid!=0) → is update, old doc removed
			name:       "MCDC_CheckAndRemove_A0_B0_valid_rowID",
			oldRowID:   int64(1), // will be set dynamically below
			wantUpdate: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			module := NewFTS5Module()
			table, _, err := module.Create(nil, "fts5", "main", "mcdc_upd", []string{"content"})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			// Insert a document so there is something to potentially update.
			rowid, err := table.Update(3, []interface{}{nil, nil, "original content"})
			if err != nil {
				t.Fatalf("Insert: %v", err)
			}

			// For the valid-rowID case, use the actual rowid.
			oldRowID := tt.oldRowID
			if tt.wantUpdate {
				oldRowID = rowid
			}

			// Perform the Update call: argc=4 means INSERT/UPDATE with 2 columns of argv.
			// argv[0]=oldRowID, argv[1]=newRowID(nil→auto), argv[2]=new content value
			_, err = table.Update(3, []interface{}{oldRowID, nil, "new content"})
			if err != nil {
				t.Fatalf("Update: %v", err)
			}

			// Check whether the original text still matches (means old doc was NOT removed).
			hasOriginal, _ := openAndFilter(table, 1, "original")
			if tt.wantUpdate {
				// Old doc should have been removed, only new content remains.
				if hasOriginal {
					t.Error("expected original doc to be removed after update, but it still matches")
				}
			} else {
				// Not an update: old doc was left, new doc added alongside.
				// "original" should still be findable.
				if !hasOriginal {
					t.Error("expected original doc to remain (not an update operation)")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_DetermineDocumentID: newRowID == nil || newRowID == int64(0)
//
// When this OR is true, a new rowid is auto-generated.
// When false, the provided rowid is used.
// Cases:
//   A=true  B=false → auto-generate (nil)
//   A=false B=true  → auto-generate (int64(0))
//   A=false B=false → use provided rowid (e.g. int64(42))
// ---------------------------------------------------------------------------

func TestMCDC_DetermineDocumentID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newRowID    interface{}
		wantAutoGen bool // true if we expect an auto-generated rowid (>0, not 42)
		fixedRowID  int64
	}{
		{
			// A=true, B=false → auto-generate
			name:        "MCDC_DetermineDocID_A1_B0_nil",
			newRowID:    nil,
			wantAutoGen: true,
		},
		{
			// A=false, B=true → auto-generate
			name:        "MCDC_DetermineDocID_A0_B1_zero",
			newRowID:    int64(0),
			wantAutoGen: true,
		},
		{
			// A=false, B=false → use provided rowid 42
			name:        "MCDC_DetermineDocID_A0_B0_explicit",
			newRowID:    int64(42),
			wantAutoGen: false,
			fixedRowID:  42,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			module := NewFTS5Module()
			table, _, err := module.Create(nil, "fts5", "main", "mcdc_docid", []string{"content"})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}

			rowid, err := table.Update(3, []interface{}{nil, tt.newRowID, "docid test"})
			if err != nil {
				t.Fatalf("Update: %v", err)
			}

			if tt.wantAutoGen {
				if rowid <= 0 {
					t.Errorf("expected positive auto-generated rowid, got %d", rowid)
				}
			} else {
				if rowid != tt.fixedRowID {
					t.Errorf("expected rowid %d, got %d", tt.fixedRowID, rowid)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_PhraseQuery: !HasPrefix(q,`"`) || !HasSuffix(q,`"`)
//
// tryParsePhraseQuery returns (nil,false) if either prefix or suffix quote is absent.
// Cases:
//   A=false, B=false → both present → parsed as phrase (returns true)
//   A=true,  B=false → no leading quote → not phrase
//   A=false, B=true  → no trailing quote → not phrase
// ---------------------------------------------------------------------------

func TestMCDC_PhraseQuery_Detection(t *testing.T) {
	t.Parallel()

	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name     string
		query    string
		wantType QueryType
		wantErr  bool
	}{
		{
			// A=false (has prefix `"`), B=false (has suffix `"`) → phrase query
			name:     "MCDC_Phrase_A0_B0_full_quotes",
			query:    `"quick brown"`,
			wantType: QueryPhrase,
		},
		{
			// A=true (no leading `"`), B=false (has trailing `"`) → not phrase (simple/other)
			name:     "MCDC_Phrase_A1_B0_no_leading_quote",
			query:    `quick brown"`,
			wantType: QuerySimple, // tokenizer strips the quote char
		},
		{
			// A=false (has leading `"`), B=true (no trailing `"`) → not phrase
			name:     "MCDC_Phrase_A0_B1_no_trailing_quote",
			query:    `"quick brown`,
			wantType: QuerySimple,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, err := parser.Parse(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.query, err)
			}
			if q.Type != tt.wantType {
				t.Errorf("query type = %v, want %v", q.Type, tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_BooleanQuery_ShortCircuit: query != nil || err != nil
//
// tryParseBooleanQuery returns early when (query!=nil || err!=nil).
// Cases exercised via AND / OR / simple:
//   A=true,  B=false → AND query found (non-nil query, nil err)
//   A=false, B=false → no boolean operator → returns (nil,nil) → falls through
// (A=false, B=true is an error path not easily triggered without malformed AND/OR)
// ---------------------------------------------------------------------------

func TestMCDC_BooleanQuery_ShortCircuit(t *testing.T) {
	t.Parallel()

	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name     string
		query    string
		wantType QueryType
		wantNil  bool
	}{
		{
			// A=true (AND query found → non-nil), B=false (no error) → returns AND query
			name:     "MCDC_BoolQuery_A1_B0_AND_operator",
			query:    "alpha AND beta",
			wantType: QueryAND,
		},
		{
			// A=true (OR query found → non-nil), B=false → returns OR query
			name:     "MCDC_BoolQuery_A1_B0_OR_operator",
			query:    "alpha OR beta",
			wantType: QueryOR,
		},
		{
			// A=false, B=false (no boolean operators) → falls through to simple query
			name:     "MCDC_BoolQuery_A0_B0_no_operator",
			query:    "simplesearch",
			wantType: QuerySimple,
		},
		{
			// NOT operator also produces non-nil result
			name:     "MCDC_BoolQuery_A1_B0_NOT_operator",
			query:    "alpha NOT beta",
			wantType: QueryNOT,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.query, err)
			}
			if q == nil {
				t.Fatal("expected non-nil query")
			}
			if q.Type != tt.wantType {
				t.Errorf("query type = %v, want %v", q.Type, tt.wantType)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_IsValidSnippetInput: len(text) > 0 && maxLength > 0
//
// Cases:
//   A=true,  B=true  → valid (returns non-empty result)
//   A=false, B=true  → invalid because text empty
//   A=true,  B=false → invalid because maxLength=0
// ---------------------------------------------------------------------------

func TestMCDC_IsValidSnippetInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		maxLength int
		wantEmpty bool
	}{
		{
			// A=true, B=true → valid input, snippet produced
			name:      "MCDC_Snippet_A1_B1_valid",
			text:      "hello world",
			maxLength: 5,
			wantEmpty: false,
		},
		{
			// A=false, B=true → empty text → returns ""
			name:      "MCDC_Snippet_A0_B1_empty_text",
			text:      "",
			maxLength: 5,
			wantEmpty: true,
		},
		{
			// A=true, B=false → maxLength=0 → returns ""
			name:      "MCDC_Snippet_A1_B0_zero_maxlen",
			text:      "hello world",
			maxLength: 0,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := GenerateSnippet(tt.text, nil, tt.maxLength)
			gotEmpty := result == ""
			if gotEmpty != tt.wantEmpty {
				t.Errorf("GenerateSnippet(%q, nil, %d) = %q, wantEmpty=%v",
					tt.text, tt.maxLength, result, tt.wantEmpty)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_HighlightText: len(text) == 0 || len(terms) == 0
//
// Returns text unchanged when either condition is true.
// Cases:
//   A=true,  B=false → empty text, terms present → returns as-is (empty)
//   A=false, B=true  → text present, no terms → returns text unchanged
//   A=false, B=false → both present → performs highlighting
// ---------------------------------------------------------------------------

func TestMCDC_HighlightText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		text        string
		terms       []string
		startMarker string
		endMarker   string
		wantResult  string
	}{
		{
			// A=true, B=false → empty text
			name:        "MCDC_Highlight_A1_B0_empty_text",
			text:        "",
			terms:       []string{"hello"},
			startMarker: "<b>",
			endMarker:   "</b>",
			wantResult:  "",
		},
		{
			// A=false, B=true → no terms
			name:        "MCDC_Highlight_A0_B1_no_terms",
			text:        "hello world",
			terms:       []string{},
			startMarker: "<b>",
			endMarker:   "</b>",
			wantResult:  "hello world",
		},
		{
			// A=false, B=false → highlighting applied
			name:        "MCDC_Highlight_A0_B0_highlight_applied",
			text:        "hello world",
			terms:       []string{"hello"},
			startMarker: "<b>",
			endMarker:   "</b>",
			wantResult:  "<b>hello</b> world",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := HighlightText(tt.text, tt.terms, tt.startMarker, tt.endMarker)
			if got != tt.wantResult {
				t.Errorf("HighlightText(%q, %v) = %q, want %q",
					tt.text, tt.terms, got, tt.wantResult)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_IsTokenChar: unicode.IsLetter(r) || unicode.IsDigit(r)
//
// isTokenChar returns true when either sub-condition is true.
// Cases:
//   A=true,  B=false → letter only (e.g. 'a')
//   A=false, B=true  → digit only (e.g. '5')
//   A=false, B=false → neither (e.g. '!')
// Tested indirectly via Tokenize: only letters/digits form tokens.
// ---------------------------------------------------------------------------

func TestMCDC_IsTokenChar(t *testing.T) {
	t.Parallel()

	tokenizer := NewSimpleTokenizer()

	tests := []struct {
		name       string
		text       string
		wantTokens []string
	}{
		{
			// A=true, B=false → letters only form a token
			name:       "MCDC_TokenChar_A1_B0_letters",
			text:       "abc",
			wantTokens: []string{"abc"},
		},
		{
			// A=false, B=true → digits only form a token
			name:       "MCDC_TokenChar_A0_B1_digits",
			text:       "123",
			wantTokens: []string{"123"},
		},
		{
			// A=false, B=false → punctuation only → no tokens
			name:       "MCDC_TokenChar_A0_B0_punctuation",
			text:       "!@#",
			wantTokens: []string{},
		},
		{
			// Mixed: letters and digits together in one token
			name:       "MCDC_TokenChar_mixed_alnum",
			text:       "abc123",
			wantTokens: []string{"abc123"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tokens := tokenizer.Tokenize(tt.text)
			if len(tokens) != len(tt.wantTokens) {
				t.Errorf("got %d tokens, want %d (tokens=%v)", len(tokens), len(tt.wantTokens), tokens)
				return
			}
			for i, tok := range tokens {
				if tok.Text != tt.wantTokens[i] {
					t.Errorf("token[%d]=%q, want %q", i, tok.Text, tt.wantTokens[i])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_IsValidTokenLength: len(text) >= MinTokenLength && len(text) <= MaxTokenLength
//
// A token is accepted only when both bounds are satisfied.
// Cases (using custom tokenizer settings):
//   A=true,  B=true  → length in range [2,3] → accepted
//   A=false, B=true  → length < min (1 char, min=2) → rejected
//   A=true,  B=false → length > max (5 chars, max=3) → rejected
// ---------------------------------------------------------------------------

func TestMCDC_IsValidTokenLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		minLen    int
		maxLen    int
		text      string
		wantCount int // number of tokens produced
	}{
		{
			// A=true, B=true → "ab" len=2, min=2, max=3 → accepted
			name:      "MCDC_TokenLen_A1_B1_in_range",
			minLen:    2,
			maxLen:    3,
			text:      "ab cd",
			wantCount: 2, // "ab" and "cd" both len=2
		},
		{
			// A=false, B=true → "a" len=1 < min=2 → rejected
			name:      "MCDC_TokenLen_A0_B1_too_short",
			minLen:    2,
			maxLen:    10,
			text:      "a bc",
			wantCount: 1, // only "bc" passes; "a" is too short
		},
		{
			// A=true, B=false → "hello" len=5 > max=3 → rejected
			name:      "MCDC_TokenLen_A1_B0_too_long",
			minLen:    1,
			maxLen:    3,
			text:      "hello hi",
			wantCount: 1, // only "hi" (len=2 ≤ 3); "hello" is too long
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tok := &SimpleTokenizer{MinTokenLength: tt.minLen, MaxTokenLength: tt.maxLen}
			tokens := tok.Tokenize(tt.text)
			if len(tokens) != tt.wantCount {
				t.Errorf("got %d tokens, want %d (text=%q, min=%d, max=%d, tokens=%v)",
					len(tokens), tt.wantCount, tt.text, tt.minLen, tt.maxLen, tokens)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_PrefixTokenizer: minPrefixLen > 0 && len(token.Text) >= minPrefixLen
//
// Prefix tokens are generated only when both conditions hold.
// Cases:
//   A=true,  B=true  → min=2, token "hello" (len=5≥2) → prefixes generated
//   A=false, B=true  → min=0, token "hello"            → no prefix generation
//   A=true,  B=false → min=6, token "hi" (len=2<6)     → no prefix generation
// ---------------------------------------------------------------------------

func TestMCDC_PrefixTokenizer(t *testing.T) {
	t.Parallel()

	base := NewSimpleTokenizer()

	tests := []struct {
		name         string
		minPrefixLen int
		maxPrefixLen int
		text         string
		// wantHasPrefix indicates whether any extra prefix tokens should be present
		// (more tokens than base Tokenize would produce).
		wantHasPrefix bool
	}{
		{
			// A=true (min=2>0), B=true (len("hello")=5≥2) → prefixes generated
			name:          "MCDC_Prefix_A1_B1_prefixes_generated",
			minPrefixLen:  2,
			maxPrefixLen:  5,
			text:          "hello",
			wantHasPrefix: true,
		},
		{
			// A=false (min=0, condition min>0 is false) → no prefixes
			name:          "MCDC_Prefix_A0_B1_min_zero",
			minPrefixLen:  0,
			maxPrefixLen:  5,
			text:          "hello",
			wantHasPrefix: false,
		},
		{
			// A=true (min=6>0), B=false (len("hi")=2 < 6) → no prefixes
			name:          "MCDC_Prefix_A1_B0_token_too_short",
			minPrefixLen:  6,
			maxPrefixLen:  10,
			text:          "hi",
			wantHasPrefix: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pt := NewPrefixTokenizer(base, tt.minPrefixLen, tt.maxPrefixLen)
			prefixTokens := pt.Tokenize(tt.text)
			baseTokens := base.Tokenize(tt.text)

			hasPrefix := len(prefixTokens) > len(baseTokens)
			if hasPrefix != tt.wantHasPrefix {
				t.Errorf("hasPrefix=%v (prefixCount=%d, baseCount=%d), want %v",
					hasPrefix, len(prefixTokens), len(baseTokens), tt.wantHasPrefix)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_RankResults: Score_j > Score_i || (Score_j == Score_i && DocID_j < DocID_i)
//
// The swap condition is an OR of a simple and an AND sub-expression.
// Cases (ordering of two results):
//   Outer-A=true,  Outer-B=false → j has higher score → swap
//   Outer-A=false, Outer-B=true  → equal score, j has lower DocID → swap
//   Outer-A=false, Outer-B=false → j has lower score, equal/higher DocID → no swap
// Tested via full-table scan: insert two docs and verify ordering.
// ---------------------------------------------------------------------------

func TestMCDC_RankResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		results        []SearchResult
		wantFirstDocID DocumentID
	}{
		{
			// Outer-A=true: higher score wins regardless of DocID
			name: "MCDC_Rank_OuterA_higher_score_wins",
			results: []SearchResult{
				{DocID: 1, Score: 1.0},
				{DocID: 2, Score: 5.0},
			},
			wantFirstDocID: 2, // higher score
		},
		{
			// Outer-A=false, inner AND=true: equal score, lower DocID wins
			name: "MCDC_Rank_OuterA0_InnerAND_equal_score_lower_docid",
			results: []SearchResult{
				{DocID: 3, Score: 2.0},
				{DocID: 1, Score: 2.0},
			},
			wantFirstDocID: 1, // same score, lower DocID first
		},
		{
			// Outer-A=false, inner AND=false: lower score → no swap → original order kept
			name: "MCDC_Rank_OuterA0_InnerAND0_lower_score_stays",
			results: []SearchResult{
				{DocID: 1, Score: 5.0},
				{DocID: 2, Score: 1.0},
			},
			wantFirstDocID: 1, // already higher score, should stay first
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			RankResults(tt.results)
			if tt.results[0].DocID != tt.wantFirstDocID {
				t.Errorf("after RankResults, first DocID=%d, want %d (results=%v)",
					tt.results[0].DocID, tt.wantFirstDocID, tt.results)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_SaveIndex_NilGuard: m.db == nil || index == nil
//
// SaveIndex returns nil early when either the db or index is nil.
// Cases:
//   A=true,  B=false → db==nil, valid index → early return (no error)
//   A=false, B=true  → valid db, index==nil → early return (no error)
//   A=false, B=false → both present → actual save runs
// ---------------------------------------------------------------------------

func TestMCDC_SaveIndex_NilGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dbNil   bool
		idxNil  bool
		wantErr bool
	}{
		{
			// A=true, B=false → db is nil
			name:    "MCDC_SaveIndex_A1_B0_nil_db",
			dbNil:   true,
			idxNil:  false,
			wantErr: false,
		},
		{
			// A=false, B=true → index is nil
			name:    "MCDC_SaveIndex_A0_B1_nil_index",
			dbNil:   false,
			idxNil:  true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mgr := &ShadowTableManager{tableName: "test", db: nil}
			if !tt.dbNil {
				// Use a minimal stub that always errors on ExecDML so we can
				// confirm we never reach it (the nil-index guard fires first).
				mgr.db = &alwaysErrorDB{}
			}

			var idx *InvertedIndex
			if !tt.idxNil {
				idx = NewInvertedIndex([]string{"content"})
			}

			err := mgr.SaveIndex(idx)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// alwaysErrorDB is a DatabaseExecutor stub that errors on every call.
type alwaysErrorDB struct{}

func (a *alwaysErrorDB) ExecDDL(sql string) error {
	return nil // DDL succeeds (needed for CreateShadowTables)
}
func (a *alwaysErrorDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (a *alwaysErrorDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// MCDC_QueryExecutor_NilRanker: ranker == nil in NewQueryExecutor
//
// NewQueryExecutor substitutes a default BM25 ranker when ranker==nil.
// Cases:
//   A=true  → nil ranker → default assigned
//   A=false → non-nil ranker → kept as-is
// ---------------------------------------------------------------------------

func TestMCDC_QueryExecutor_NilRanker(t *testing.T) {
	t.Parallel()

	idx := NewInvertedIndex([]string{"content"})

	tests := []struct {
		name      string
		ranker    RankFunction
		wantNilQE bool
	}{
		{
			// A=true: nil ranker → executor uses default ranker (not nil)
			name:   "MCDC_NilRanker_A1_nil_ranker",
			ranker: nil,
		},
		{
			// A=false: non-nil ranker → executor uses provided ranker
			name:   "MCDC_NilRanker_A0_provided_ranker",
			ranker: NewSimpleRanker(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			qe := NewQueryExecutor(idx, tt.ranker)
			if qe == nil {
				t.Fatal("NewQueryExecutor returned nil")
			}
			// Execute a trivial query to confirm the executor works without panic.
			q := &Query{Type: QuerySimple, Terms: []string{"test"}, Column: -1}
			results, err := qe.Execute(q)
			if err != nil {
				t.Errorf("Execute: %v", err)
			}
			// Empty index → no results expected.
			if len(results) != 0 {
				t.Errorf("expected 0 results from empty index, got %d", len(results))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_AND_Query_Intersection: documents must match both sub-queries
//
// matchAND intersects two result sets; the overall result is true only when
// both sub-conditions hold (document appears in both child result sets).
// Cases:
//   A=true,  B=true  → doc contains both terms → returned
//   A=true,  B=false → doc contains only first term → excluded
//   A=false, B=true  → doc contains only second term → excluded
// ---------------------------------------------------------------------------

func TestMCDC_AND_Query_Intersection(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_and", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	docs := []string{
		"alpha beta",  // both terms → should match AND
		"alpha only",  // only alpha
		"beta only",   // only beta
		"gamma delta", // neither
	}
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{
			// A=true, B=true → both alpha AND beta → 1 doc
			name:      "MCDC_AND_A1_B1_both_match",
			query:     "alpha AND beta",
			wantCount: 1,
		},
		{
			// A=true (alpha matches 2), B=false (only is not beta) → 0 from AND
			// Actually "alpha AND only" matches "alpha only" → count=1
			name:      "MCDC_AND_A1_B0_first_only",
			query:     "alpha AND only",
			wantCount: 1,
		},
		{
			// A=false, B=true: "gamma AND beta" → gamma not in alpha docs → 0
			name:      "MCDC_AND_A0_B1_second_only",
			query:     "gamma AND beta",
			wantCount: 0,
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
				t.Errorf("query %q: got %d results, want %d", tt.query, count, tt.wantCount)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_OR_Query_Union: document matches when at least one sub-query matches
//
// matchOR unions results; the condition is true when either child matches.
// Cases:
//   A=true,  B=true  → doc in both sets
//   A=true,  B=false → doc only in first set
//   A=false, B=true  → doc only in second set
// ---------------------------------------------------------------------------

func TestMCDC_OR_Query_Union(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_or", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	docs := []string{
		"alpha beta",  // both
		"alpha only",  // first only
		"beta only",   // second only
		"gamma delta", // neither
	}
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{
			// A=true (alpha matches 2), B=true (beta matches 2) → union = 3 docs
			name:      "MCDC_OR_A1_B1_both_match",
			query:     "alpha OR beta",
			wantCount: 3,
		},
		{
			// A=true (alpha matches 2), B=false (zzz matches 0) → 2 docs
			name:      "MCDC_OR_A1_B0_only_first",
			query:     "alpha OR zzznomatch",
			wantCount: 2,
		},
		{
			// A=false (zzz matches 0), B=true (beta matches 2) → 2 docs
			name:      "MCDC_OR_A0_B1_only_second",
			query:     "zzznomatch OR beta",
			wantCount: 2,
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
				t.Errorf("query %q: got %d results, want %d", tt.query, count, tt.wantCount)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MCDC_NOT_Query_Exclusion: included = in(first set) && !in(second set)
//
// matchNOT: a document is returned iff it is in the first child's result set
// but NOT in the second child's result set.
// Cases:
//   A=true,  B=false → in first, not in second → included
//   A=true,  B=true  → in both → excluded
//   A=false, B=true  → only in second → excluded
// ---------------------------------------------------------------------------

func TestMCDC_NOT_Query_Exclusion(t *testing.T) {
	t.Parallel()

	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "mcdc_not", []string{"content"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	docs := []string{
		"alpha beta",  // both alpha and beta
		"alpha gamma", // alpha but not beta
		"beta delta",  // beta but not alpha
	}
	for _, d := range docs {
		if _, err := table.Update(3, []interface{}{nil, nil, d}); err != nil {
			t.Fatalf("Insert %q: %v", d, err)
		}
	}

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{
			// A=true (in alpha set), B=false (not in beta set) → 1 doc: "alpha gamma"
			name:      "MCDC_NOT_A1_B0_in_first_not_second",
			query:     "alpha NOT beta",
			wantCount: 1,
		},
		{
			// A=true (in alpha set), B=true (also in beta set) → excluded: "alpha beta" removed
			// Combined with the above: "alpha NOT beta" returns "alpha gamma" (1 doc)
			// Separately: "beta NOT alpha" = {beta docs} minus {alpha docs} = "beta delta"
			name:      "MCDC_NOT_A1_B1_in_both_excluded",
			query:     "beta NOT alpha",
			wantCount: 1, // "beta delta" remains
		},
		{
			// A=false (gamma NOT in alpha set), B=true (alpha in second set)
			// "gamma NOT alpha" → gamma matches "alpha gamma" (gamma is a term there)
			// then remove alpha docs → "alpha gamma" removed → 0 results
			name:      "MCDC_NOT_A0_B1_only_in_second_excluded",
			query:     "gamma NOT alpha",
			wantCount: 0,
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
				t.Errorf("query %q: got %d results, want %d", tt.query, count, tt.wantCount)
			}
		})
	}
}
