package fts5

import (
	"fmt"
	"strings"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
)

// TestTokenizer tests the simple tokenizer.
func TestTokenizer(t *testing.T) {
	tokenizer := NewSimpleTokenizer()

	tests := []struct {
		name     string
		text     string
		expected []string
	}{
		{
			name:     "simple text",
			text:     "Hello World",
			expected: []string{"hello", "world"},
		},
		{
			name:     "text with punctuation",
			text:     "Hello, World! How are you?",
			expected: []string{"hello", "world", "how", "are", "you"},
		},
		{
			name:     "numbers and letters",
			text:     "test123 abc456",
			expected: []string{"test123", "abc456"},
		},
		{
			name:     "empty string",
			text:     "",
			expected: []string{},
		},
		{
			name:     "only punctuation",
			text:     "!@#$%",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := tokenizer.Tokenize(tt.text)
			if len(tokens) != len(tt.expected) {
				t.Errorf("Expected %d tokens, got %d", len(tt.expected), len(tokens))
			}

			for i, token := range tokens {
				if i >= len(tt.expected) {
					break
				}
				if token.Text != tt.expected[i] {
					t.Errorf("Token %d: expected %q, got %q", i, tt.expected[i], token.Text)
				}
			}
		})
	}
}

// TestStopWordTokenizer tests the stop word filtering tokenizer.
func TestStopWordTokenizer(t *testing.T) {
	base := NewSimpleTokenizer()
	tokenizer := NewStopWordTokenizer(base, StopWords)

	text := "the quick brown fox jumps over the lazy dog"
	tokens := tokenizer.Tokenize(text)

	// "the" and "over" should be filtered out as stop words
	expected := []string{"quick", "brown", "fox", "jumps", "lazy", "dog"}

	if len(tokens) != len(expected) {
		t.Errorf("Expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, token := range tokens {
		if i >= len(expected) {
			break
		}
		if token.Text != expected[i] {
			t.Errorf("Token %d: expected %q, got %q", i, expected[i], token.Text)
		}
	}
}

// TestInvertedIndex tests the inverted index functionality.
func TestInvertedIndex(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add documents
	doc1 := map[int]string{0: "the quick brown fox"}
	doc2 := map[int]string{0: "the lazy dog"}
	doc3 := map[int]string{0: "quick brown animals"}

	err := index.AddDocument(1, doc1, tokenizer)
	if err != nil {
		t.Fatalf("Failed to add document 1: %v", err)
	}

	err = index.AddDocument(2, doc2, tokenizer)
	if err != nil {
		t.Fatalf("Failed to add document 2: %v", err)
	}

	err = index.AddDocument(3, doc3, tokenizer)
	if err != nil {
		t.Fatalf("Failed to add document 3: %v", err)
	}

	// Test document count
	if index.GetTotalDocuments() != 3 {
		t.Errorf("Expected 3 documents, got %d", index.GetTotalDocuments())
	}

	// Test posting list
	postings := index.GetPostingList("quick")
	if len(postings) != 2 {
		t.Errorf("Expected 2 documents with 'quick', got %d", len(postings))
	}

	// Test document frequency
	df := index.GetDocumentFrequency("the")
	if df != 2 {
		t.Errorf("Expected document frequency 2 for 'the', got %d", df)
	}

	// Test remove document
	err = index.RemoveDocument(1)
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	if index.GetTotalDocuments() != 2 {
		t.Errorf("Expected 2 documents after removal, got %d", index.GetTotalDocuments())
	}

	// Test phrase match
	doc4 := map[int]string{0: "the quick brown fox jumps"}
	index.AddDocument(4, doc4, tokenizer)

	match := index.PhraseMatch([]string{"quick", "brown", "fox"}, 4)
	if !match {
		t.Error("Expected phrase match for 'quick brown fox' in doc 4")
	}

	noMatch := index.PhraseMatch([]string{"quick", "lazy"}, 4)
	if noMatch {
		t.Error("Did not expect phrase match for 'quick lazy' in doc 4")
	}
}

// TestBM25Ranker tests the BM25 ranking algorithm.
func TestBM25Ranker(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()

	// Add documents with varying relevance
	docs := []map[int]string{
		{0: "database systems"},
		{0: "database management systems"},
		{0: "distributed database systems architecture"},
		{0: "web development"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Search for "database systems"
	terms := []string{"database", "systems"}

	score1 := ranker.Score(index, 1, terms)
	score2 := ranker.Score(index, 2, terms)
	score3 := ranker.Score(index, 3, terms)
	score4 := ranker.Score(index, 4, terms)

	// Doc 4 shouldn't match
	if score4 != 0.0 {
		t.Errorf("Expected score 0 for non-matching document, got %f", score4)
	}

	// Docs 1, 2, 3 should all have positive scores
	if score1 <= 0 || score2 <= 0 || score3 <= 0 {
		t.Error("Expected positive scores for matching documents")
	}

	t.Logf("Scores: doc1=%f, doc2=%f, doc3=%f, doc4=%f", score1, score2, score3, score4)
}

// TestQueryParser tests query parsing.
func TestQueryParser(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name      string
		query     string
		queryType QueryType
		termCount int
	}{
		{
			name:      "simple query",
			query:     "hello world",
			queryType: QuerySimple,
			termCount: 2,
		},
		{
			name:      "phrase query",
			query:     `"hello world"`,
			queryType: QueryPhrase,
			termCount: 2,
		},
		{
			name:      "AND query",
			query:     "hello AND world",
			queryType: QueryAND,
			termCount: 0, // children have the terms
		},
		{
			name:      "OR query",
			query:     "hello OR world",
			queryType: QueryOR,
			termCount: 0,
		},
		{
			name:      "NOT query",
			query:     "hello NOT world",
			queryType: QueryNOT,
			termCount: 0,
		},
		{
			name:      "prefix query",
			query:     "hel*",
			queryType: QueryPrefix,
			termCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := parser.Parse(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			if query.Type != tt.queryType {
				t.Errorf("Expected query type %v, got %v", tt.queryType, query.Type)
			}

			if tt.termCount > 0 && len(query.Terms) != tt.termCount {
				t.Errorf("Expected %d terms, got %d", tt.termCount, len(query.Terms))
			}
		})
	}
}

// TestQueryExecution tests query execution and search.
func TestQueryExecution(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add test documents
	docs := []map[int]string{
		{0: "the quick brown fox jumps over the lazy dog"},
		{0: "a quick brown animal runs fast"},
		{0: "the lazy cat sleeps all day"},
		{0: "foxes are quick and clever animals"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Test simple query
	t.Run("simple query", func(t *testing.T) {
		query, _ := parser.Parse("quick brown")
		results, err := executor.Execute(query)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if len(results) < 2 {
			t.Errorf("Expected at least 2 results, got %d", len(results))
		}

		// Results should be ranked by score
		for i := 1; i < len(results); i++ {
			if results[i].Score > results[i-1].Score {
				t.Error("Results not properly ranked by score")
			}
		}
	})

	// Test phrase query
	t.Run("phrase query", func(t *testing.T) {
		query, _ := parser.Parse(`"quick brown"`)
		results, err := executor.Execute(query)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Should match docs with "quick brown" as consecutive terms
		expectedMatches := []DocumentID{1, 2}
		if len(results) != len(expectedMatches) {
			t.Errorf("Expected %d results, got %d", len(expectedMatches), len(results))
		}
	})

	// Test AND query
	t.Run("AND query", func(t *testing.T) {
		query, _ := parser.Parse("quick AND fox")
		results, err := executor.Execute(query)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Should only match docs containing both "quick" and "fox"
		if len(results) < 1 {
			t.Error("Expected at least 1 result for AND query")
		}
	})

	// Test OR query
	t.Run("OR query", func(t *testing.T) {
		query, _ := parser.Parse("cat OR dog")
		results, err := executor.Execute(query)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Should match docs containing either "cat" or "dog"
		if len(results) < 2 {
			t.Errorf("Expected at least 2 results for OR query, got %d", len(results))
		}
	})

	// Test NOT query
	t.Run("NOT query", func(t *testing.T) {
		query, _ := parser.Parse("quick NOT fox")
		results, err := executor.Execute(query)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Should match docs with "quick" but not "fox"
		for _, result := range results {
			// Check that result doesn't contain "fox"
			postings := index.GetPostingList("fox")
			for _, posting := range postings {
				if posting.DocID == result.DocID {
					t.Error("NOT query returned document containing excluded term")
				}
			}
		}
	})
}

// TestFTS5Module tests the FTS5 virtual table module.
func TestFTS5Module(t *testing.T) {
	module := NewFTS5Module()

	// Test Create
	table, schema, err := module.Create(nil, "fts5", "main", "test_fts", []string{"title", "body"})
	if err != nil {
		t.Fatalf("Failed to create FTS5 table: %v", err)
	}

	if table == nil {
		t.Fatal("Expected non-nil table")
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	ftsTable, ok := table.(*FTS5Table)
	if !ok {
		t.Fatal("Expected FTS5Table type")
	}

	// Test column count
	if len(ftsTable.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(ftsTable.columns))
	}

	// Test Insert
	t.Run("insert documents", func(t *testing.T) {
		// INSERT: argv[0]=NULL, argv[1]=NULL/rowid, argv[2+]=column values
		rowid, err := ftsTable.Update(4, []interface{}{nil, nil, "First Document", "This is the body of the first document"})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		if rowid <= 0 {
			t.Error("Expected positive rowid")
		}

		rowid2, err := ftsTable.Update(4, []interface{}{nil, nil, "Second Document", "Another body with different content"})
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		if rowid2 <= rowid {
			t.Error("Expected increasing rowids")
		}
	})

	// Test Open and Filter
	t.Run("query documents", func(t *testing.T) {
		cursor, err := ftsTable.Open()
		if err != nil {
			t.Fatalf("Failed to open cursor: %v", err)
		}
		defer cursor.Close()

		// Create BestIndex info with MATCH constraint
		info := vtab.NewIndexInfo(1)
		info.Constraints[0].Column = 0
		info.Constraints[0].Op = vtab.ConstraintMatch
		info.Constraints[0].Usable = true

		err = ftsTable.BestIndex(info)
		if err != nil {
			t.Fatalf("BestIndex failed: %v", err)
		}

		// Query with MATCH
		err = cursor.Filter(1, "", []interface{}{"document"})
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}

		// Count results
		count := 0
		for !cursor.EOF() {
			// Get column value
			val, err := cursor.Column(0)
			if err != nil {
				t.Errorf("Column failed: %v", err)
			}
			t.Logf("Result %d: %v", count, val)

			count++
			cursor.Next()
		}

		if count < 1 {
			t.Error("Expected at least 1 search result")
		}
	})

	// Test Delete
	t.Run("delete document", func(t *testing.T) {
		// DELETE: argc=1, argv[0]=rowid
		_, err := ftsTable.Update(1, []interface{}{int64(1)})
		if err != nil {
			t.Fatalf("DELETE failed: %v", err)
		}

		// Verify document was removed
		if ftsTable.index.GetTotalDocuments() != 1 {
			t.Errorf("Expected 1 document after delete, got %d", ftsTable.index.GetTotalDocuments())
		}
	})
}

// TestHighlightAndSnippet tests highlighting and snippet generation.
func TestHighlightAndSnippet(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog"
	terms := []string{"quick", "fox", "lazy"}

	// Test highlighting
	highlighted := HighlightText(text, terms, "<b>", "</b>")
	if highlighted == text {
		t.Error("Expected text to be highlighted")
	}
	t.Logf("Highlighted: %s", highlighted)

	// Test snippet generation
	snippet := GenerateSnippet(text, []int{4, 16, 36}, 20)
	if len(snippet) > 25 { // 20 + some for ellipsis
		t.Errorf("Snippet too long: %d characters", len(snippet))
	}
	t.Logf("Snippet: %s", snippet)
}

// TestRankResults tests result ranking.
func TestRankResults(t *testing.T) {
	results := []SearchResult{
		{DocID: 1, Score: 5.0},
		{DocID: 2, Score: 10.0},
		{DocID: 3, Score: 3.0},
		{DocID: 4, Score: 10.0},
	}

	RankResults(results)

	// Check that results are sorted by score descending
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Error("Results not properly sorted by score")
		}
		// For equal scores, check DocID is ascending
		if results[i].Score == results[i-1].Score && results[i].DocID < results[i-1].DocID {
			t.Error("Results with equal scores not sorted by DocID")
		}
	}

	t.Logf("Ranked results: %+v", results)
}

// TestConcurrentAccess tests thread-safe concurrent access to FTS5.
func TestConcurrentAccess(t *testing.T) {
	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "concurrent_test", []string{"content"})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	ftsTable := table.(*FTS5Table)

	// Concurrent inserts
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, err := ftsTable.Update(3, []interface{}{nil, nil, "Concurrent document"})
			if err != nil {
				t.Errorf("Concurrent insert failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all inserts
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all documents were added
	if ftsTable.index.GetTotalDocuments() != 10 {
		t.Errorf("Expected 10 documents, got %d", ftsTable.index.GetTotalDocuments())
	}
}

// TestIndexStats tests index statistics.
func TestIndexStats(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add documents
	for i := 1; i <= 5; i++ {
		doc := map[int]string{0: "test document with some content"}
		index.AddDocument(DocumentID(i), doc, tokenizer)
	}

	stats := index.Stats()

	if stats.TotalDocuments != 5 {
		t.Errorf("Expected 5 documents, got %d", stats.TotalDocuments)
	}

	if stats.TotalTerms == 0 {
		t.Error("Expected non-zero term count")
	}

	if stats.AverageDocLength == 0.0 {
		t.Error("Expected non-zero average document length")
	}

	t.Logf("Index stats: %s", stats.String())
}

// TestFTS5ModuleConnect tests the Connect method.
func TestFTS5ModuleConnect(t *testing.T) {
	module := NewFTS5Module()

	table, schema, err := module.Connect(nil, "fts5", "main", "test_fts", []string{"title", "body"})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if table == nil {
		t.Fatal("Expected non-nil table")
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	ftsTable, ok := table.(*FTS5Table)
	if !ok {
		t.Fatal("Expected FTS5Table type")
	}

	if len(ftsTable.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(ftsTable.columns))
	}
}

// TestFTS5Destroy tests the Destroy method.
func TestFTS5Destroy(t *testing.T) {
	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	ftsTable := table.(*FTS5Table)

	// Add some documents
	ftsTable.Update(4, []interface{}{nil, nil, "test content"})
	ftsTable.Update(4, []interface{}{nil, nil, "more content"})

	// Destroy the table
	err = ftsTable.Destroy()
	if err != nil {
		t.Errorf("Destroy failed: %v", err)
	}

	// Verify data is cleared
	if ftsTable.index.GetTotalDocuments() != 0 {
		t.Errorf("Expected 0 documents after Destroy, got %d", ftsTable.index.GetTotalDocuments())
	}

	if len(ftsTable.rows) != 0 {
		t.Errorf("Expected 0 rows after Destroy, got %d", len(ftsTable.rows))
	}
}

// TestFTS5CursorRowid tests the Rowid method.
func TestFTS5CursorRowid(t *testing.T) {
	module := NewFTS5Module()
	table, _, err := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	ftsTable := table.(*FTS5Table)
	ftsTable.Update(4, []interface{}{nil, nil, "test document"})

	cursor, err := ftsTable.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	// Query all documents
	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if !cursor.EOF() {
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}
		if rowid != 1 {
			t.Errorf("Expected rowid 1, got %d", rowid)
		}
	}

	// Test Rowid at EOF
	for !cursor.EOF() {
		cursor.Next()
	}

	_, err = cursor.Rowid()
	if err == nil {
		t.Error("Expected error when calling Rowid at EOF")
	}
}

// TestRegisterFTS5 tests FTS5 registration.
func TestRegisterFTS5(t *testing.T) {
	err := RegisterFTS5()
	if err != nil {
		t.Fatalf("RegisterFTS5 failed: %v", err)
	}

	// Verify module is registered
	if !vtab.HasModule("fts5") {
		t.Error("FTS5 module not registered")
	}
}

// TestBM25Function tests the BM25 auxiliary function.
func TestBM25Function(t *testing.T) {
	// Create index with documents
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	docs := []map[int]string{
		{0: "database systems"},
		{0: "database management"},
		{0: "distributed systems"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Test BM25 scoring
	score := BM25(index, 1, []string{"database", "systems"})
	if score <= 0 {
		t.Errorf("Expected positive BM25 score, got %f", score)
	}

	// Document without matching terms should score 0
	score = BM25(index, 2, []string{"nonexistent"})
	if score != 0 {
		t.Errorf("Expected 0 score for non-matching terms, got %f", score)
	}
}

// TestSnippetFunction tests the Snippet auxiliary function.
func TestSnippetFunction(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog. This is a longer piece of text for testing snippet generation."
	terms := []string{"quick", "lazy"}

	snippet := Snippet(text, terms, "<b>", "</b>", 10)

	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	t.Logf("Snippet: %s", snippet)
}

// TestHighlightFunction tests the Highlight auxiliary function.
func TestHighlightFunction(t *testing.T) {
	text := "The quick brown fox"
	terms := []string{"quick", "fox"}

	highlighted := Highlight(text, terms, "<mark>", "</mark>")

	if highlighted == text {
		t.Error("Expected text to be modified")
	}

	if !strings.Contains(highlighted, "<mark>") {
		t.Error("Expected highlight markers in text")
	}

	t.Logf("Highlighted: %s", highlighted)
}

// TestTFIDFRanker tests the TF-IDF ranking algorithm.
func TestTFIDFRanker(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewTFIDFRanker()

	docs := []map[int]string{
		{0: "database systems"},
		{0: "database management systems"},
		{0: "web development"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	terms := []string{"database", "systems"}

	score1 := ranker.Score(index, 1, terms)
	score2 := ranker.Score(index, 2, terms)
	score3 := ranker.Score(index, 3, terms)

	// Doc 3 shouldn't match
	if score3 != 0.0 {
		t.Errorf("Expected score 0 for non-matching document, got %f", score3)
	}

	// Docs 1 and 2 should have positive scores
	if score1 <= 0 || score2 <= 0 {
		t.Error("Expected positive scores for matching documents")
	}

	t.Logf("TF-IDF Scores: doc1=%f, doc2=%f, doc3=%f", score1, score2, score3)
}

// TestSimpleRanker tests the simple ranking algorithm.
func TestSimpleRanker(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewSimpleRanker()

	docs := []map[int]string{
		{0: "database systems"},
		{0: "database database systems"},
		{0: "web development"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	terms := []string{"database"}

	score1 := ranker.Score(index, 1, terms)
	score2 := ranker.Score(index, 2, terms)
	score3 := ranker.Score(index, 3, terms)

	// Doc 2 should have higher score (more occurrences)
	if score2 <= score1 {
		t.Error("Expected doc2 to have higher score than doc1")
	}

	// Doc 3 shouldn't match
	if score3 != 0.0 {
		t.Errorf("Expected score 0 for non-matching document, got %f", score3)
	}

	t.Logf("Simple Ranker Scores: doc1=%f, doc2=%f, doc3=%f", score1, score2, score3)
}

// TestScoreWithBoost tests scoring with column boosting.
func TestScoreWithBoost(t *testing.T) {
	baseScore := 10.0
	boost := map[int]float64{0: 2.0, 1: 1.0}

	// Test with boosted column
	score := ScoreWithBoost(baseScore, 0, boost)
	if score != 20.0 {
		t.Errorf("Expected score 20.0, got %f", score)
	}

	// Test with non-boosted column
	score = ScoreWithBoost(baseScore, 2, boost)
	if score != baseScore {
		t.Errorf("Expected score %f, got %f", baseScore, score)
	}

	t.Logf("Boosted score: %f", score)
}

// TestPrefixTokenizer tests the prefix tokenizer.
func TestPrefixTokenizer(t *testing.T) {
	base := NewSimpleTokenizer()
	tokenizer := NewPrefixTokenizer(base, 2, 5)

	text := "database"
	tokens := tokenizer.Tokenize(text)

	// Should generate prefixes: da, dat, data, datab, databa
	if len(tokens) == 0 {
		t.Error("Expected tokens from prefix tokenizer")
	}

	hasPrefix := false
	for _, token := range tokens {
		if len(token.Text) >= 2 && len(token.Text) <= 5 {
			hasPrefix = true
			break
		}
	}

	if !hasPrefix {
		t.Error("Expected prefix tokens")
	}

	t.Logf("Prefix tokens: %v", tokens)
}

// TestStopWordIsStopWord tests stop word filtering.
func TestStopWordIsStopWord(t *testing.T) {
	base := NewSimpleTokenizer()
	tokenizer := NewStopWordTokenizer(base, StopWords)

	// Test that stop words are filtered out
	text := "the quick brown fox and the lazy dog"
	tokens := tokenizer.Tokenize(text)

	// "the" and "and" should be filtered
	for _, token := range tokens {
		if token.Text == "the" || token.Text == "and" {
			t.Errorf("Stop word '%s' should have been filtered", token.Text)
		}
	}

	// Should contain non-stop words
	hasNonStop := false
	for _, token := range tokens {
		if token.Text == "quick" || token.Text == "brown" {
			hasNonStop = true
			break
		}
	}

	if !hasNonStop {
		t.Error("Expected non-stop words in result")
	}
}

// TestGetDocumentContent tests retrieving document content.
func TestGetDocumentContent(t *testing.T) {
	index := NewInvertedIndex([]string{"title", "body"})
	tokenizer := NewSimpleTokenizer()

	doc := map[int]string{
		0: "Title Text",
		1: "Body content",
	}
	index.AddDocument(1, doc, tokenizer)

	// Test getting specific column content
	content, exists := index.GetDocumentContent(1, 0)
	if !exists {
		t.Error("Expected content to exist for column 0")
	}

	if content != "Title Text" {
		t.Errorf("Expected title 'Title Text', got %s", content)
	}

	content, exists = index.GetDocumentContent(1, 1)
	if !exists {
		t.Error("Expected content to exist for column 1")
	}

	if content != "Body content" {
		t.Errorf("Expected body 'Body content', got %s", content)
	}

	// Test non-existent document
	_, exists = index.GetDocumentContent(999, 0)
	if exists {
		t.Error("Expected content not to exist for non-existent document")
	}
}

// TestGetColumnNames tests getting column names from index.
func TestGetColumnNames(t *testing.T) {
	columns := []string{"title", "body", "author"}
	index := NewInvertedIndex(columns)

	names := index.GetColumnNames()

	if len(names) != 3 {
		t.Errorf("Expected 3 column names, got %d", len(names))
	}

	for i, name := range names {
		if name != columns[i] {
			t.Errorf("Column %d: expected %s, got %s", i, columns[i], name)
		}
	}
}

// TestGetAllDocuments tests retrieving all document IDs.
func TestGetAllDocuments(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add multiple documents
	for i := 1; i <= 5; i++ {
		doc := map[int]string{0: fmt.Sprintf("document %d", i)}
		index.AddDocument(DocumentID(i), doc, tokenizer)
	}

	docIDs := index.GetAllDocuments()

	if len(docIDs) != 5 {
		t.Errorf("Expected 5 documents, got %d", len(docIDs))
	}

	// Check that all IDs are present
	for i := 1; i <= 5; i++ {
		found := false
		for _, id := range docIDs {
			if id == DocumentID(i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Document ID %d not found", i)
		}
	}
}

// TestGetTerms tests getting all terms from index.
func TestGetTerms(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	doc := map[int]string{0: "quick brown fox"}
	index.AddDocument(1, doc, tokenizer)

	terms := index.GetTerms()

	expectedTerms := []string{"quick", "brown", "fox"}
	if len(terms) != len(expectedTerms) {
		t.Errorf("Expected %d terms, got %d", len(expectedTerms), len(terms))
	}

	for _, expected := range expectedTerms {
		found := false
		for _, term := range terms {
			if term == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected term '%s' not found", expected)
		}
	}
}

// TestClearIndex tests clearing the index.
func TestClearIndex(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add documents
	for i := 1; i <= 3; i++ {
		doc := map[int]string{0: "test content"}
		index.AddDocument(DocumentID(i), doc, tokenizer)
	}

	if index.GetTotalDocuments() != 3 {
		t.Errorf("Expected 3 documents before clear, got %d", index.GetTotalDocuments())
	}

	// Clear index
	index.Clear()

	if index.GetTotalDocuments() != 0 {
		t.Errorf("Expected 0 documents after clear, got %d", index.GetTotalDocuments())
	}

	terms := index.GetTerms()
	if len(terms) != 0 {
		t.Errorf("Expected 0 terms after clear, got %d", len(terms))
	}
}

// TestMatchOperator tests the MATCH operator in queries.
func TestMatchOperator(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add documents
	docs := []map[int]string{
		{0: "quick brown fox"},
		{0: "lazy dog"},
		{0: "quick animal"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Test MATCH operator
	query, err := parser.Parse("quick")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestPrefixQuery tests prefix query execution.
func TestPrefixQuery(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add documents
	docs := []map[int]string{
		{0: "database systems"},
		{0: "data structures"},
		{0: "web development"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Test prefix query
	query, err := parser.Parse("dat*")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Should match "database" and "data"
	if len(results) != 2 {
		t.Errorf("Expected 2 results for prefix query, got %d", len(results))
	}
}

// TestColumnFilterQuery tests column-specific queries.
func TestColumnFilterQuery(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Test simple column filter syntax
	// Note: The actual implementation may vary, so we test what's supported
	query, err := parser.Parse("database")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Should parse as simple query
	if query.Type != QuerySimple {
		t.Errorf("Expected QuerySimple, got %v", query.Type)
	}

	if len(query.Terms) != 1 || query.Terms[0] != "database" {
		t.Errorf("Expected term 'database', got %v", query.Terms)
	}
}

// TestGenerateSnippet tests snippet generation with various inputs.
func TestGenerateSnippet(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog"

	// Test with match positions
	matchPositions := []int{4, 36} // "quick" and "lazy"
	snippet := GenerateSnippet(text, matchPositions, 20)

	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	t.Logf("Generated snippet: %s", snippet)

	// Test with empty positions
	snippet = GenerateSnippet(text, []int{}, 20)
	if len(snippet) > 25 {
		t.Errorf("Snippet too long: %d characters", len(snippet))
	}
}

// TestHighlightTextEdgeCases tests highlighting edge cases.
func TestHighlightTextEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		terms []string
	}{
		{
			name:  "empty text",
			text:  "",
			terms: []string{"test"},
		},
		{
			name:  "empty terms",
			text:  "some text",
			terms: []string{},
		},
		{
			name:  "no matches",
			text:  "some text",
			terms: []string{"nomatch"},
		},
		{
			name:  "multiple matches",
			text:  "test test test",
			terms: []string{"test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HighlightText(tt.text, tt.terms, "<b>", "</b>")

			// Should not crash and return valid string
			if tt.text == "" && result != "" {
				t.Errorf("Expected empty result for empty text, got %s", result)
			}
		})
	}
}

// TestFTS5UpdateEdgeCases tests Update method edge cases.
func TestFTS5UpdateEdgeCases(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Test UPDATE with mismatched rowids
	ftsTable.Update(4, []interface{}{nil, int64(1), "first"})

	// Try UPDATE changing rowid
	_, err := ftsTable.Update(4, []interface{}{int64(1), int64(2), "updated"})
	if err != nil {
		t.Logf("UPDATE with changed rowid: %v", err)
	}

	// Test DELETE non-existent
	_, err = ftsTable.Update(1, []interface{}{int64(999)})
	if err != nil {
		t.Logf("DELETE non-existent: %v", err)
	}
}

// TestFTS5ColumnEdgeCases tests cursor Column edge cases.
func TestFTS5ColumnEdgeCases(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"title", "body"})
	ftsTable := table.(*FTS5Table)

	// Insert with different value types
	ftsTable.Update(4, []interface{}{nil, nil, "Title", int64(123)})
	ftsTable.Update(4, []interface{}{nil, nil, []byte("Bytes"), 45.67})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	// Test various column access
	for !cursor.EOF() {
		for col := 0; col < 2; col++ {
			val, err := cursor.Column(col)
			if err != nil {
				t.Errorf("Column(%d) failed: %v", col, err)
			}
			t.Logf("Column %d: %v (type: %T)", col, val, val)
		}
		cursor.Next()
	}
}

// TestFTS5BestIndexWithoutMatch tests BestIndex without MATCH constraint.
func TestFTS5BestIndexWithoutMatch(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	info := vtab.NewIndexInfo(2)
	info.Constraints[0].Column = 0
	info.Constraints[0].Op = vtab.ConstraintEQ
	info.Constraints[0].Usable = true
	info.Constraints[1].Column = 1
	info.Constraints[1].Op = vtab.ConstraintGT
	info.Constraints[1].Usable = true

	err := ftsTable.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// Should handle non-MATCH constraints
	t.Logf("BestIndex estimated cost: %f", info.EstimatedCost)
}

// TestInvertedIndexEdgeCases tests index edge cases.
func TestInvertedIndexEdgeCases(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add document with empty content
	doc := map[int]string{0: ""}
	err := index.AddDocument(1, doc, tokenizer)
	if err != nil {
		t.Errorf("AddDocument with empty content failed: %v", err)
	}

	// Get posting list for non-existent term
	postings := index.GetPostingList("nonexistent")
	if len(postings) != 0 {
		t.Error("Expected empty postings for non-existent term")
	}

	// Get document frequency for non-existent term
	df := index.GetDocumentFrequency("nonexistent")
	if df != 0 {
		t.Errorf("Expected DF 0 for non-existent term, got %d", df)
	}

	// Remove non-existent document
	err = index.RemoveDocument(999)
	if err != nil {
		t.Logf("RemoveDocument non-existent: %v", err)
	}

	// Phrase match with non-existent document
	match := index.PhraseMatch([]string{"test"}, 999)
	if match {
		t.Error("Expected no phrase match for non-existent document")
	}
}

// TestQueryParserEmptyQuery tests parsing empty queries.
func TestQueryParserEmptyQuery(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	query, err := parser.Parse("")
	// Empty queries may return an error or a query with no terms
	if err != nil {
		t.Logf("Parse empty query returned error: %v", err)
		return
	}

	if query != nil {
		t.Logf("Query type: %v, terms: %v", query.Type, query.Terms)
	}
}

// TestQueryExecutorEdgeCases tests query execution edge cases.
func TestQueryExecutorEdgeCases(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "test content"}, tokenizer)

	// Test query with no results
	query, err := parser.Parse("nonexistent")
	if err != nil {
		t.Logf("Parse failed: %v", err)
		return
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}

	// Test with valid simple query
	query, err = parser.Parse("test")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	results, err = executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	t.Logf("Query results: %d", len(results))
}

// TestRankersWithEmptyIndex tests rankers with empty index.
func TestRankersWithEmptyIndex(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})

	rankers := []struct {
		name   string
		ranker RankFunction
	}{
		{"BM25", NewBM25Ranker()},
		{"TFIDF", NewTFIDFRanker()},
		{"Simple", NewSimpleRanker()},
	}

	for _, r := range rankers {
		t.Run(r.name, func(t *testing.T) {
			score := r.ranker.Score(index, 1, []string{"test"})
			if score != 0 {
				t.Errorf("Expected score 0 for empty index, got %f", score)
			}
		})
	}
}

// TestGenerateSnippetEdgeCases tests snippet generation edge cases.
func TestGenerateSnippetEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		positions []int
		maxLen    int
	}{
		{
			name:      "empty text",
			text:      "",
			positions: []int{0},
			maxLen:    20,
		},
		{
			name:      "no positions",
			text:      "some text",
			positions: []int{},
			maxLen:    20,
		},
		{
			name:      "position out of range",
			text:      "short",
			positions: []int{100},
			maxLen:    20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snippet := GenerateSnippet(tt.text, tt.positions, tt.maxLen)
			// Should not crash
			t.Logf("Snippet: %s", snippet)
		})
	}
}

// TestTokenizerEdgeCases tests tokenizer edge cases.
func TestTokenizerEdgeCases(t *testing.T) {
	tokenizer := NewSimpleTokenizer()

	tests := []struct {
		name string
		text string
	}{
		{"only spaces", "    "},
		{"unicode", "Hello 世界 مرحبا"},
		{"mixed case", "TeSt CaSe"},
		{"numbers only", "123 456"},
		{"special chars", "!@#$%^&*()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := tokenizer.Tokenize(tt.text)
			t.Logf("Tokens for '%s': %v", tt.name, tokens)
		})
	}
}
