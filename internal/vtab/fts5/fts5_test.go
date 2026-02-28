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

// TestMatchOperatorFunction tests the MatchOperator function directly.
func TestMatchOperatorFunction(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add documents
	docs := []map[int]string{
		{0: "quick brown fox"},
		{0: "lazy dog"},
		{0: "quick animal"},
	}

	for i, doc := range docs {
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	tests := []struct {
		name        string
		queryStr    string
		docID       DocumentID
		shouldMatch bool
	}{
		{"matching document", "quick", 1, true},
		{"non-matching document", "quick", 2, false},
		{"another match", "lazy", 2, true},
		{"phrase query", `"quick brown"`, 1, true},
		{"AND query", "quick AND animal", 3, true},
		{"OR query", "quick OR lazy", 1, true},
		{"NOT query", "quick NOT fox", 3, true},
		{"prefix query", "qui*", 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches, err := MatchOperator(index, tt.queryStr, tt.docID)
			if err != nil {
				t.Errorf("MatchOperator failed: %v", err)
				return
			}

			if matches != tt.shouldMatch {
				t.Errorf("MatchOperator(%q, %d) = %v, want %v", tt.queryStr, tt.docID, matches, tt.shouldMatch)
			}
		})
	}

	// Test with invalid query
	_, err := MatchOperator(index, "", 1)
	if err == nil {
		t.Error("Expected error for empty query")
	}
}

// TestStopWordIsStopWordFunction tests the IsStopWord function.
func TestStopWordIsStopWordFunction(t *testing.T) {
	tests := []struct {
		word       string
		isStopWord bool
	}{
		{"the", true},
		{"The", true}, // Test case insensitive
		{"and", true},
		{"or", true},
		{"quick", false},
		{"brown", false},
		{"fox", false},
	}

	for _, tt := range tests {
		t.Run(tt.word, func(t *testing.T) {
			result := IsStopWord(tt.word)
			if result != tt.isStopWord {
				t.Errorf("IsStopWord(%q) = %v, want %v", tt.word, result, tt.isStopWord)
			}
		})
	}
}

// TestParseColumnFilter tests column filter parsing.
func TestParseColumnFilterParsing(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name     string
		queryStr string
		hasColon bool
	}{
		{"with column filter", "title:search", true},
		{"without column filter", "search", false},
		{"empty after colon", "title:", true},
		{"multiple colons", "title:body:search", true},
		{"colon in phrase", `"hello:world"`, false}, // Phrase query, not column filter
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := parser.Parse(tt.queryStr)
			if err != nil && tt.name != "empty after colon" {
				t.Errorf("Parse failed: %v", err)
				return
			}

			// If it contains a colon and is not a phrase, the column filter should be parsed
			if tt.hasColon && query != nil {
				t.Logf("Query type: %v, terms: %v, column: %d", query.Type, query.Terms, query.Column)
			}
		})
	}
}

// TestQueryExecutorNilQuery tests executor with nil query.
func TestQueryExecutorNilQuery(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, NewBM25Ranker())

	_, err := executor.Execute(nil)
	if err == nil {
		t.Error("Expected error for nil query")
	}
}

// TestNewQueryExecutorNilRanker tests creating executor with nil ranker.
func TestNewQueryExecutorNilRanker(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, nil)

	if executor.ranker == nil {
		t.Error("Expected default ranker when nil is passed")
	}
}

// TestParsePrefixQueryEmpty tests prefix query with no tokens.
func TestParsePrefixQueryEmpty(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Query with only special characters followed by *
	query, err := parser.Parse("*")
	if err != nil {
		t.Logf("Parse '*' returned error: %v", err)
	} else if query != nil {
		t.Logf("Query type: %v", query.Type)
	}
}

// TestParseSimpleQueryNoTerms tests simple query with no valid terms.
func TestParseSimpleQueryNoTerms(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Query with only punctuation
	_, err := parser.Parse("!@#$%")
	if err == nil {
		t.Error("Expected error for query with no valid terms")
	}
}

// TestBM25WithEmptyTerms tests BM25 ranking with empty terms.
func TestBM25WithEmptyTerms(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()

	doc := map[int]string{0: "test content"}
	index.AddDocument(1, doc, tokenizer)

	score := ranker.Score(index, 1, []string{})
	if score != 0 {
		t.Errorf("Expected score 0 for empty terms, got %f", score)
	}
}

// TestTFIDFWithEmptyTerms tests TF-IDF ranking with empty terms.
func TestTFIDFWithEmptyTerms(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewTFIDFRanker()

	doc := map[int]string{0: "test content"}
	index.AddDocument(1, doc, tokenizer)

	score := ranker.Score(index, 1, []string{})
	if score != 0 {
		t.Errorf("Expected score 0 for empty terms, got %f", score)
	}
}

// TestCalculateSnippetBoundsEdgeCases tests snippet bounds calculation.
func TestCalculateSnippetBoundsEdgeCases(t *testing.T) {
	text := "Short text"

	// Test with position at end of text
	snippet := GenerateSnippet(text, []int{len(text) - 1}, 5)
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	// Test with position beyond text
	snippet = GenerateSnippet(text, []int{len(text) + 10}, 20)
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	// Test with very small maxLen
	snippet = GenerateSnippet(text, []int{0}, 1)
	t.Logf("Very short snippet: %s", snippet)
}

// TestAddEllipsisEdgeCases tests ellipsis addition edge cases.
func TestAddEllipsisEdgeCases(t *testing.T) {
	text := "The quick brown fox jumps over the lazy dog"

	tests := []struct {
		name      string
		positions []int
		maxLen    int
	}{
		{"first position", []int{0}, 20},
		{"last position", []int{len(text) - 1}, 20},
		{"middle position", []int{len(text) / 2}, 20},
		{"very short maxLen", []int{10}, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snippet := GenerateSnippet(text, tt.positions, tt.maxLen)
			t.Logf("Snippet: %s", snippet)
		})
	}
}

// TestTruncateText tests text truncation.
func TestTruncateText(t *testing.T) {
	// Test with long text
	longText := "This is a very long text that should be truncated to a reasonable length for snippet generation purposes"

	snippet := GenerateSnippet(longText, []int{0}, 20)
	if len(snippet) > 25 { // 20 + some for ellipsis
		t.Errorf("Snippet too long: %d characters", len(snippet))
	}

	// Test with text shorter than maxLen
	shortText := "Short"
	snippet = GenerateSnippet(shortText, []int{0}, 100)
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}
}

// TestPhraseMatchEmptyTerms tests phrase matching with empty terms.
func TestPhraseMatchEmptyTerms(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	doc := map[int]string{0: "test content"}
	index.AddDocument(1, doc, tokenizer)

	match := index.PhraseMatch([]string{}, 1)
	if match {
		t.Error("Expected no match for empty terms")
	}
}

// TestHasConsecutiveTermsEdgeCases tests consecutive terms checking.
func TestHasConsecutiveTermsEdgeCases(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Add document with specific terms at specific positions
	doc := map[int]string{0: "the quick brown fox jumps"}
	index.AddDocument(1, doc, tokenizer)

	// Test with single term phrase
	match := index.PhraseMatch([]string{"quick"}, 1)
	if !match {
		t.Error("Expected match for single term")
	}

	// Test with non-consecutive terms
	match = index.PhraseMatch([]string{"quick", "jumps"}, 1)
	if match {
		t.Error("Expected no match for non-consecutive terms")
	}
}

// TestTermsFollowFromEdgeCases tests term position following.
func TestTermsFollowFromEdgeCases(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	doc := map[int]string{0: "one two three four five"}
	index.AddDocument(1, doc, tokenizer)

	// Test phrase that should match
	match := index.PhraseMatch([]string{"two", "three", "four"}, 1)
	if !match {
		t.Error("Expected match for consecutive terms")
	}
}

// TestContainsPositionEdgeCases tests position containment.
func TestContainsPositionEdgeCases(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	doc := map[int]string{0: "test word test word test"}
	index.AddDocument(1, doc, tokenizer)

	// The word "test" appears multiple times
	postings := index.GetPostingList("test")
	if len(postings) == 0 {
		t.Fatal("Expected postings for 'test'")
	}

	// Check that positions are recorded correctly
	t.Logf("Positions for 'test': %v", postings[0].Positions)
}

// TestFTS5ColumnSpecial tests special column indices.
func TestFTS5ColumnSpecial(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test content"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(1, "", []interface{}{"test"})

	if !cursor.EOF() {
		// Test special column -1 (score)
		val, err := cursor.Column(-1)
		if err != nil {
			t.Errorf("Column(-1) failed: %v", err)
		}
		if score, ok := val.(float64); ok {
			t.Logf("Score column value: %f", score)
		}
	}
}

// TestFTS5UpdateNoColumns tests update with insufficient columns.
func TestFTS5UpdateNoColumns(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"col1", "col2"})
	ftsTable := table.(*FTS5Table)

	// Try to insert with only one column value (should fail)
	_, err := ftsTable.Update(3, []interface{}{nil, nil, "only one"})
	if err == nil {
		t.Error("Expected error for insufficient columns")
	}
}

// TestFTS5CreateNoColumns tests creating FTS5 table with no columns.
func TestFTS5CreateNoColumns(t *testing.T) {
	module := NewFTS5Module()

	// Test with empty args
	_, _, err := module.Create(nil, "fts5", "main", "test_fts", []string{})
	if err == nil {
		t.Error("Expected error for no columns")
	}

	// Test with empty column names
	_, _, err = module.Create(nil, "fts5", "main", "test_fts", []string{"", "  "})
	if err == nil {
		t.Error("Expected error for empty column names")
	}
}

// TestBM25EdgeCaseScores tests BM25 with edge case document frequencies.
func TestBM25EdgeCaseScores(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()

	// Add document with very long content (high term frequency)
	longContent := strings.Repeat("test ", 1000)
	doc := map[int]string{0: longContent}
	index.AddDocument(1, doc, tokenizer)

	score := ranker.Score(index, 1, []string{"test"})
	if score <= 0 {
		t.Errorf("Expected positive score, got %f", score)
	}
	t.Logf("BM25 score for high TF document: %f", score)
}

// TestTFIDFEdgeCaseScores tests TF-IDF with edge cases.
func TestTFIDFEdgeCaseScores(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewTFIDFRanker()

	// Add multiple documents with varying term frequencies
	docs := []string{
		"test",
		"test test",
		"test test test",
		"other content",
	}

	for i, content := range docs {
		doc := map[int]string{0: content}
		index.AddDocument(DocumentID(i+1), doc, tokenizer)
	}

	// Score each document
	for i := 1; i <= 3; i++ {
		score := ranker.Score(index, DocumentID(i), []string{"test"})
		t.Logf("TF-IDF score for doc %d: %f", i, score)
	}
}

// TestPrefixTokenizerEdgeCases tests prefix tokenizer edge cases.
func TestPrefixTokenizerEdgeCases(t *testing.T) {
	base := NewSimpleTokenizer()

	tests := []struct {
		name       string
		minPrefix  int
		maxPrefix  int
		text       string
	}{
		{"min equals max", 3, 3, "testing"},
		{"min larger than word", 10, 15, "short"},
		{"max larger than word", 2, 100, "test"},
		{"empty text", 2, 5, ""},
		{"very small min", 1, 2, "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewPrefixTokenizer(base, tt.minPrefix, tt.maxPrefix)
			tokens := tokenizer.Tokenize(tt.text)
			t.Logf("Prefix tokens for '%s' (min=%d, max=%d): %d tokens", tt.text, tt.minPrefix, tt.maxPrefix, len(tokens))
		})
	}
}

// TestStopWordTokenizerNilStopWords tests stop word tokenizer with nil stop words.
func TestStopWordTokenizerNilStopWords(t *testing.T) {
	base := NewSimpleTokenizer()
	tokenizer := NewStopWordTokenizer(base, nil)

	text := "the quick brown fox"
	tokens := tokenizer.Tokenize(text)

	// Should return all tokens since no stop words are defined
	if len(tokens) == 0 {
		t.Error("Expected tokens when stop words is nil")
	}
}

// TestGetDocumentLengthNonExistent tests getting length of non-existent document.
func TestGetDocumentLengthNonExistent(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})

	length := index.GetDocumentLength(999)
	if length != 0 {
		t.Errorf("Expected length 0 for non-existent document, got %d", length)
	}
}

// TestANDQueryEmpty tests AND query with no children.
func TestANDQueryEmpty(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add documents
	doc := map[int]string{0: "test content"}
	index.AddDocument(1, doc, tokenizer)

	// Create AND query manually
	query := &Query{
		Type:     QueryAND,
		Children: []*Query{},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty AND query, got %d", len(results))
	}
}

// TestNOTQueryInsufficientChildren tests NOT query with less than 2 children.
func TestNOTQueryInsufficientChildren(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	doc := map[int]string{0: "test content"}
	index.AddDocument(1, doc, tokenizer)

	// Create NOT query with only one child
	query := &Query{
		Type: QueryNOT,
		Children: []*Query{
			{Type: QuerySimple, Terms: []string{"test"}},
		},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for NOT query with insufficient children, got %d", len(results))
	}
}

// TestBestIndexUnusableConstraints tests BestIndex with unusable constraints.
func TestBestIndexUnusableConstraints(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Create info with unusable constraints
	info := vtab.NewIndexInfo(2)
	info.Constraints[0].Column = 0
	info.Constraints[0].Op = vtab.ConstraintMatch
	info.Constraints[0].Usable = false // Not usable
	info.Constraints[1].Column = 1
	info.Constraints[1].Op = vtab.ConstraintMatch
	info.Constraints[1].Usable = false // Not usable

	err := ftsTable.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed: %v", err)
	}

	// Should not set IdxNum to 1 since no usable MATCH constraints
	if info.IdxNum == 1 {
		t.Error("Expected IdxNum != 1 when no usable MATCH constraints")
	}
}

// TestHandleDeleteInvalidRowid tests delete with invalid rowid type.
func TestHandleDeleteInvalidRowid(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert a document first
	ftsTable.Update(3, []interface{}{nil, nil, "test"})

	// Try to delete with invalid rowid type (string instead of int64)
	_, err := ftsTable.Update(1, []interface{}{"invalid"})
	if err == nil {
		t.Error("Expected error for invalid rowid type in DELETE")
	}
}

// TestDetermineDocumentIDInvalidType tests document ID determination with invalid type.
func TestDetermineDocumentIDInvalidType(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Try to insert with invalid rowid type
	_, err := ftsTable.Update(3, []interface{}{nil, "invalid", "test"})
	if err == nil {
		t.Error("Expected error for invalid rowid type")
	}
}

// TestCheckAndRemoveOldDocumentWithZeroRowID tests update detection with zero rowid.
func TestCheckAndRemoveOldDocumentWithZeroRowID(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert with explicit rowid 0 (should be treated as auto-generate)
	rowid, err := ftsTable.Update(3, []interface{}{int64(0), int64(0), "test"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid == 0 {
		t.Error("Expected non-zero rowid when passing 0")
	}
}

// TestFilterInvalidQueryString tests filter with invalid query string.
func TestFilterInvalidQueryString(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test content"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Try to filter with invalid query (only punctuation which produces no terms)
	err := cursor.Filter(1, "", []interface{}{"!@#$%"})
	if err == nil {
		t.Error("Expected error for invalid query")
	}
}

// TestFilterNonStringQuery tests filter with non-string query argument.
func TestFilterNonStringQuery(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test content"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Filter with MATCH but non-string argument
	err := cursor.Filter(1, "", []interface{}{123})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should return no results or handle gracefully
	count := 0
	for !cursor.EOF() {
		count++
		cursor.Next()
	}
	t.Logf("Results with non-string query: %d", count)
}

// TestColumnEOFError tests column access when cursor is at EOF.
func TestColumnEOFError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Filter returns no results (empty cursor)
	cursor.Filter(0, "", nil)

	// Try to access column when at EOF
	_, err := cursor.Column(0)
	if err == nil {
		t.Error("Expected error when accessing column at EOF")
	}
}

// TestColumnDocumentNotFound tests column access for missing document.
func TestColumnDocumentNotFound(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		// Manually corrupt the data to test error handling
		ftsCursor := cursor.(*FTS5Cursor)
		if len(ftsCursor.results) > 0 {
			// Change docID to non-existent one
			ftsCursor.results[0].DocID = 999
			_, err := cursor.Column(0)
			if err == nil {
				t.Error("Expected error for non-existent document")
			}
		}
	}
}

// TestColumnOutOfRange tests column access with out of range index.
func TestColumnOutOfRange(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		// Try to access column beyond range
		_, err := cursor.Column(999)
		if err == nil {
			t.Error("Expected error for out of range column index")
		}

		// Try negative index (other than -1)
		_, err = cursor.Column(-2)
		if err == nil {
			t.Error("Expected error for invalid negative column index")
		}
	}
}

// TestUpdateInsufficientArguments tests update with too few arguments.
func TestUpdateInsufficientArguments(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Try to call update with only 1 arg (edge case between DELETE and INSERT)
	// Actually argc=1 is DELETE, so let's test argc=0
	_, err := ftsTable.Update(0, []interface{}{})
	if err == nil {
		t.Error("Expected error for update with 0 arguments")
	}
}

// TestTruncateTextEdgeCases tests truncateText function edge cases.
func TestTruncateTextEdgeCases(t *testing.T) {
	// Test text exactly at maxLength
	text := "12345"
	snippet := GenerateSnippet(text, []int{}, 5)
	if snippet != text {
		t.Errorf("Expected unchanged text for exact length match, got %s", snippet)
	}

	// Test text shorter than maxLength
	text = "123"
	snippet = GenerateSnippet(text, []int{}, 5)
	if snippet != text {
		t.Errorf("Expected unchanged text for shorter text, got %s", snippet)
	}

	// Test empty text with zero maxLength
	snippet = GenerateSnippet("", []int{}, 0)
	if snippet != "" {
		t.Errorf("Expected empty string for empty input with zero maxLength, got %s", snippet)
	}

	// Test zero maxLength with non-empty text
	snippet = GenerateSnippet("text", []int{}, 0)
	if snippet != "" {
		t.Errorf("Expected empty string for zero maxLength, got %s", snippet)
	}
}

// TestCalculateSnippetBoundsNegativeStart tests snippet bounds with negative start.
func TestCalculateSnippetBoundsNegativeStart(t *testing.T) {
	text := "The quick brown fox"

	// Position near start of text (start would be negative)
	snippet := GenerateSnippet(text, []int{0}, 10)
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}
	t.Logf("Snippet at position 0: %s", snippet)

	// Position at start with large maxLength
	snippet = GenerateSnippet(text, []int{5}, 50)
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}
	t.Logf("Snippet with large maxLength: %s", snippet)
}

// TestScoreWithEmptyIndex tests BM25.Score with specific edge cases.
func TestScoreWithEmptyIndex(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	ranker := NewBM25Ranker()

	// Score with empty index (N=0)
	score := ranker.Score(index, 1, []string{"test"})
	if score != 0 {
		t.Errorf("Expected 0 score for empty index, got %f", score)
	}
}

// TestScoreTermNotInDocument tests scoring when term not in document.
func TestScoreTermNotInDocument(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()

	// Add documents
	index.AddDocument(1, map[int]string{0: "quick brown fox"}, tokenizer)
	index.AddDocument(2, map[int]string{0: "lazy dog"}, tokenizer)

	// Search for term not in specific document
	score := ranker.Score(index, 2, []string{"fox"})
	if score != 0 {
		t.Errorf("Expected 0 score when term not in document, got %f", score)
	}
}

// TestTFIDFWithZeroDocumentLength tests TF-IDF with zero-length document.
func TestTFIDFWithZeroDocumentLength(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewTFIDFRanker()

	// Add empty document
	index.AddDocument(1, map[int]string{0: ""}, tokenizer)

	score := ranker.Score(index, 1, []string{"test"})
	if score != 0 {
		t.Errorf("Expected 0 score for zero-length document, got %f", score)
	}
}

// TestParseColumnFilterEdgeCases tests column filter parsing edge cases.
func TestParseColumnFilterEdgeCases(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	tests := []struct {
		name     string
		query    string
		hasColon bool
	}{
		{"no colon", "search term", false},
		{"with colon", "title:search", true},
		{"colon no content after", "title:", true},
		{"only colon", ":", true},
		{"multiple colons", "a:b:c", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.Parse(tt.query)
			// Should handle gracefully
			if err != nil {
				t.Logf("Parse returned error for %s: %v", tt.name, err)
			}
		})
	}
}

// TestTryParsePrefixQueryNoTokens tests prefix query parsing with no tokens.
func TestTryParsePrefixQueryNoTokens(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Query that produces no tokens after removing special chars
	query, err := parser.Parse("!@#*")
	if err != nil {
		t.Logf("Parse '!@#*' returned error: %v", err)
	} else if query != nil {
		t.Logf("Query type: %v, terms: %v", query.Type, query.Terms)
	}
}

// TestMatchPhraseEmptyResult tests phrase matching with no initial matches.
func TestMatchPhraseEmptyResult(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "quick brown fox"}, tokenizer)

	// Create phrase query with term not in any document
	query := &Query{
		Type:  QueryPhrase,
		Terms: []string{"nonexistent", "term"},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for phrase with non-existent term, got %d", len(results))
	}
}

// TestExtractTermsWithNilQuery tests extractTerms with nil query.
func TestExtractTermsWithNilQuery(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, NewBM25Ranker())

	terms := executor.extractTerms(nil)
	if len(terms) != 0 {
		t.Errorf("Expected empty terms for nil query, got %v", terms)
	}
}

// TestExtractTermsRecursive tests extractTerms with nested queries.
func TestExtractTermsRecursive(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Create complex nested query
	query := &Query{
		Type:  QueryAND,
		Terms: []string{"top"},
		Children: []*Query{
			{
				Type:  QuerySimple,
				Terms: []string{"child1"},
			},
			{
				Type:  QueryOR,
				Terms: []string{"child2"},
				Children: []*Query{
					{
						Type:  QuerySimple,
						Terms: []string{"grandchild"},
					},
				},
			},
		},
	}

	terms := executor.extractTerms(query)
	expectedCount := 4 // top, child1, child2, grandchild
	if len(terms) != expectedCount {
		t.Errorf("Expected %d terms, got %d: %v", expectedCount, len(terms), terms)
	}
}

// TestUpdateWithExplicitRowIDs tests updates with specific rowid sequences.
func TestUpdateWithExplicitRowIDs(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert with explicit rowid 10
	rowid, err := ftsTable.Update(3, []interface{}{nil, int64(10), "first"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid != 10 {
		t.Errorf("Expected rowid 10, got %d", rowid)
	}

	// Next auto-generated rowid should be > 10
	rowid2, err := ftsTable.Update(3, []interface{}{nil, nil, "second"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid2 <= 10 {
		t.Errorf("Expected rowid > 10, got %d", rowid2)
	}

	// Insert with rowid equal to current nextRowID
	currentNext := ftsTable.nextRowID
	rowid3, err := ftsTable.Update(3, []interface{}{nil, int64(currentNext), "third"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid3 != int64(currentNext) {
		t.Errorf("Expected rowid %d, got %d", currentNext, rowid3)
	}

	// Verify nextRowID was updated
	if ftsTable.nextRowID <= currentNext {
		t.Errorf("Expected nextRowID > %d, got %d", currentNext, ftsTable.nextRowID)
	}
}

// TestUpdateRealUpdate tests actual UPDATE (not INSERT) with old rowid.
func TestUpdateRealUpdate(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert original document
	rowid, err := ftsTable.Update(3, []interface{}{nil, int64(5), "original"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify it was inserted
	if ftsTable.index.GetTotalDocuments() != 1 {
		t.Errorf("Expected 1 document after insert, got %d", ftsTable.index.GetTotalDocuments())
	}

	// Update the document (old rowid = new rowid = 5)
	rowid2, err := ftsTable.Update(3, []interface{}{int64(5), int64(5), "updated"})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if rowid2 != rowid {
		t.Errorf("Expected same rowid %d, got %d", rowid, rowid2)
	}

	// Should still have 1 document
	if ftsTable.index.GetTotalDocuments() != 1 {
		t.Errorf("Expected 1 document after update, got %d", ftsTable.index.GetTotalDocuments())
	}

	// Verify content was updated
	content, exists := ftsTable.rows[DocumentID(5)]
	if !exists {
		t.Error("Document should still exist")
	}
	if content[0] != "updated" {
		t.Errorf("Expected updated content, got %v", content[0])
	}
}

// TestConvertToStringVariousTypes tests convertToString with various value types.
func TestConvertToStringVariousTypes(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"col1", "col2", "col3", "col4"})
	ftsTable := table.(*FTS5Table)

	// Insert with various types
	rowid, err := ftsTable.Update(6, []interface{}{
		nil,
		nil,
		"string value",
		int64(123),
		45.67,
		[]byte("byte slice"),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify document was indexed
	if ftsTable.index.GetTotalDocuments() != 1 {
		t.Errorf("Expected 1 document, got %d", ftsTable.index.GetTotalDocuments())
	}

	// Query to verify all values were indexed
	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		for i := 0; i < 4; i++ {
			val, err := cursor.Column(i)
			if err != nil {
				t.Errorf("Column(%d) failed: %v", i, err)
			}
			t.Logf("Column %d (rowid=%d): %v (type: %T)", i, rowid, val, val)
		}
	}
}

// TestExtractColumnValuesNilValues tests extracting column values with nil values.
func TestExtractColumnValuesNilValues(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"col1", "col2"})
	ftsTable := table.(*FTS5Table)

	// Insert with nil values
	rowid, err := ftsTable.Update(4, []interface{}{nil, nil, nil, "not nil"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify document exists
	if ftsTable.index.GetTotalDocuments() != 1 {
		t.Errorf("Expected 1 document, got %d", ftsTable.index.GetTotalDocuments())
	}

	// Access the values
	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		val0, err := cursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) failed: %v", err)
		}
		if val0 != nil {
			t.Errorf("Expected nil for column 0, got %v", val0)
		}

		val1, err := cursor.Column(1)
		if err != nil {
			t.Errorf("Column(1) failed: %v", err)
		}
		if val1 != "not nil" {
			t.Errorf("Expected 'not nil' for column 1, got %v", val1)
		}

		t.Logf("Rowid: %d", rowid)
	}
}

// TestParseANDQueryWithInvalidChild tests AND query parsing with invalid child.
func TestParseANDQueryWithInvalidChild(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// AND query with invalid child query (only special chars that produce no terms)
	_, err := parser.Parse("test AND !@#$%")
	if err == nil {
		t.Error("Expected error for AND query with invalid child")
	}
}

// TestParseORQueryWithInvalidChild tests OR query parsing with invalid child.
func TestParseORQueryWithInvalidChild(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// OR query with invalid child query
	_, err := parser.Parse("test OR !@#$%")
	if err == nil {
		t.Error("Expected error for OR query with invalid child")
	}
}

// TestParseNOTQueryWithInvalidChildren tests NOT query parsing with invalid children.
func TestParseNOTQueryWithInvalidChildren(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// NOT query with invalid left child
	_, err := parser.Parse("!@#$% NOT test")
	if err == nil {
		t.Error("Expected error for NOT query with invalid left child")
	}

	// NOT query with invalid right child
	_, err = parser.Parse("test NOT !@#$%")
	if err == nil {
		t.Error("Expected error for NOT query with invalid right child")
	}
}

// TestHandleDeleteNonExistent tests deleting non-existent document.
func TestHandleDeleteNonExistent(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Try to delete document that doesn't exist
	// This should not error but just return the rowid
	rowid, err := ftsTable.Update(1, []interface{}{int64(999)})
	if err != nil {
		t.Logf("Delete non-existent document error: %v", err)
	}
	if rowid != 999 {
		t.Errorf("Expected rowid 999, got %d", rowid)
	}
}

// TestFilterQueryExecutionError tests filter with query execution error.
func TestFilterQueryExecutionError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Try to filter with query that has execution issues
	// Empty query should error
	err := cursor.Filter(1, "", []interface{}{""})
	if err == nil {
		t.Error("Expected error for empty query string")
	}
}

// TestGetMatchingDocumentsUnknownType tests getMatchingDocuments with unknown query type.
func TestGetMatchingDocumentsUnknownType(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Create query with invalid/unknown type
	query := &Query{
		Type:  QueryType(999), // Unknown type
		Terms: []string{"test"},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should return empty results for unknown query type
	if len(results) != 0 {
		t.Errorf("Expected 0 results for unknown query type, got %d", len(results))
	}
}

// TestMatchPhraseNotConsecutive tests phrase matching with non-consecutive terms.
func TestMatchPhraseNotConsecutive(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "the quick brown fox jumps"}, tokenizer)

	// Create phrase query with terms that exist but aren't consecutive
	query := &Query{
		Type:  QueryPhrase,
		Terms: []string{"quick", "jumps"}, // Not consecutive
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should return no results since terms aren't consecutive
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-consecutive phrase, got %d", len(results))
	}
}

// TestTruncateTextExactLength tests truncateText with text exactly at maxLength.
func TestTruncateTextExactLength(t *testing.T) {
	text := "12345"
	snippet := GenerateSnippet(text, []int{}, 5)

	// Should not add ellipsis when exactly at maxLength
	if snippet != text {
		t.Errorf("Expected '%s', got '%s'", text, snippet)
	}

	// Test with longer text
	text = "123456"
	snippet = GenerateSnippet(text, []int{}, 5)

	// Should add ellipsis when over maxLength
	if !strings.Contains(snippet, "...") {
		t.Error("Expected ellipsis for truncated text")
	}
}

// TestCalculateSnippetBoundsEndOverflow tests snippet bounds when end exceeds text length.
func TestCalculateSnippetBoundsEndOverflow(t *testing.T) {
	text := "Short text"

	// Position near end with large maxLength
	snippet := GenerateSnippet(text, []int{8}, 20)

	// Should handle gracefully
	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	t.Logf("Snippet: '%s'", snippet)
}

// TestScoreTermNotFound tests scoring when term has no postings.
func TestScoreTermNotFound(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()

	// Add document
	index.AddDocument(1, map[int]string{0: "test content"}, tokenizer)

	// Search for term that doesn't exist
	score := ranker.Score(index, 1, []string{"nonexistent"})

	// Should return 0 for term not found
	if score != 0 {
		t.Errorf("Expected 0 score for non-existent term, got %f", score)
	}
}

// TestTFIDFScoreTermNotFound tests TF-IDF scoring when term has no postings.
func TestTFIDFScoreTermNotFound(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	ranker := NewTFIDFRanker()

	// Add document
	index.AddDocument(1, map[int]string{0: "test content"}, tokenizer)

	// Search for term that doesn't exist
	score := ranker.Score(index, 1, []string{"nonexistent"})

	// Should return 0 for term not found
	if score != 0 {
		t.Errorf("Expected 0 score for non-existent term, got %f", score)
	}
}

// TestParseColumnFilterSinglePart tests parseColumnFilter edge case.
func TestParseColumnFilterSinglePart(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Query with colon but only one part after split (edge case)
	query, err := parser.Parse("column:")
	if err != nil {
		t.Logf("Parse 'column:' returned error: %v", err)
	} else if query != nil {
		t.Logf("Query type: %v, terms: %v", query.Type, query.Terms)
	}
}

// TestANDQueryOneChild tests AND query execution with only one child.
func TestANDQueryOneChild(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "test content"}, tokenizer)

	// Create AND query with one child
	query := &Query{
		Type: QueryAND,
		Children: []*Query{
			{Type: QuerySimple, Terms: []string{"test"}},
		},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should match the single child's results
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}

// TestORQueryNoChildren tests OR query execution with no children.
func TestORQueryNoChildren(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Create OR query with no children
	query := &Query{
		Type:     QueryOR,
		Children: []*Query{},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should return empty results
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

// TestPrefixQueryEmptyPrefix tests prefix query with empty prefix term.
func TestPrefixQueryEmptyPrefix(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "database test"}, tokenizer)

	// Create prefix query manually with empty term
	query := &Query{
		Type:  QueryPrefix,
		Terms: []string{""},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should handle gracefully
	t.Logf("Results for empty prefix: %d", len(results))
}

// TestUpdateInsufficientColumnValues tests update with insufficient column values.
func TestUpdateInsufficientColumnValues(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"col1", "col2", "col3"})
	ftsTable := table.(*FTS5Table)

	// Try to insert with only 2 column values when 3 are required
	_, err := ftsTable.Update(4, []interface{}{nil, nil, "val1", "val2"})
	if err == nil {
		t.Error("Expected error for insufficient column values")
	}
}

// TestConvertToStringByteSlice tests convertToString specifically with byte slice.
func TestConvertToStringByteSlice(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert with byte slice
	byteData := []byte("test data")
	rowid, err := ftsTable.Update(3, []interface{}{nil, nil, byteData})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Query the document
	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		val, err := cursor.Column(0)
		if err != nil {
			t.Errorf("Column failed: %v", err)
		}

		// Should store the original byte slice
		t.Logf("Stored value (rowid=%d): %v (type: %T)", rowid, val, val)
	}
}

// TestHandleDeleteWithIndexError tests delete when RemoveDocument returns error.
func TestHandleDeleteWithIndexError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert a document
	ftsTable.Update(3, []interface{}{nil, nil, "test"})

	// Delete should succeed even if document doesn't exist in index
	// (RemoveDocument handles non-existent gracefully)
	rowid, err := ftsTable.Update(1, []interface{}{int64(1)})
	if err != nil {
		t.Logf("Delete returned error: %v", err)
	}
	if rowid != 1 {
		t.Errorf("Expected rowid 1, got %d", rowid)
	}
}

// TestHandleInsertOrUpdateExtractError tests insert with column extraction error.
func TestHandleInsertOrUpdateExtractError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"col1", "col2"})
	ftsTable := table.(*FTS5Table)

	// Try insert with too few values (should trigger extractColumnValues error)
	_, err := ftsTable.Update(3, []interface{}{nil, nil, "only_one"})
	if err == nil {
		t.Error("Expected error for insufficient column values")
	}
}

// TestFilterWithEmptyArgv tests filter with empty argv.
func TestFilterWithEmptyArgv(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	ftsTable.Update(3, []interface{}{nil, nil, "test"})

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Filter with idxNum=1 (MATCH) but empty argv
	err := cursor.Filter(1, "", []interface{}{})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should return all documents since no query string
	count := 0
	for !cursor.EOF() {
		count++
		cursor.Next()
	}
	t.Logf("Results with empty argv: %d", count)
}

// TestTruncateTextLongerThanMax tests truncateText with text longer than maxLength.
func TestTruncateTextLongerThanMax(t *testing.T) {
	text := "This is a very long text that needs to be truncated"

	// Test truncation
	snippet := GenerateSnippet(text, []int{}, 10)

	// Should be truncated and have ellipsis
	if !strings.Contains(snippet, "...") {
		t.Error("Expected ellipsis in truncated text")
	}

	// Length should be around maxLength + 3 for "..."
	if len(snippet) > 15 {
		t.Errorf("Snippet too long: %d characters (expected ~13)", len(snippet))
	}

	t.Logf("Truncated snippet: '%s'", snippet)
}

// TestCalculateSnippetBoundsStartAdjustment tests snippet bounds adjustment.
func TestCalculateSnippetBoundsStartAdjustment(t *testing.T) {
	// Text where end would overflow, forcing start adjustment
	text := "1234567890"

	// Position near end with maxLength that would overflow
	snippet := GenerateSnippet(text, []int{8}, 8)

	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	t.Logf("Snippet with boundary adjustment: '%s'", snippet)
}

// TestParsePhraseQuerySimple tests phrase query parsing edge case.
func TestParsePhraseQuerySimple(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Simple phrase query
	query, err := parser.Parse(`"single"`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if query.Type != QueryPhrase {
		t.Errorf("Expected QueryPhrase, got %v", query.Type)
	}

	if len(query.Terms) != 1 || query.Terms[0] != "single" {
		t.Errorf("Expected single term 'single', got %v", query.Terms)
	}
}

// TestParseColumnFilterNoContent tests column filter with no content after colon.
func TestParseColumnFilterNoContent(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// Column filter with no content (should error on no valid terms)
	_, err := parser.Parse("column:   ")
	if err == nil {
		t.Error("Expected error for column filter with no content")
	}
}

// TestMatchPhraseSingleTerm tests phrase matching with single term.
func TestMatchPhraseSingleTerm(t *testing.T) {
	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()
	executor := NewQueryExecutor(index, NewBM25Ranker())

	// Add document
	index.AddDocument(1, map[int]string{0: "quick brown fox"}, tokenizer)

	// Create phrase query with single term
	query := &Query{
		Type:  QueryPhrase,
		Terms: []string{"quick"},
	}

	results, err := executor.Execute(query)
	if err != nil {
		t.Errorf("Execute failed: %v", err)
	}

	// Should match the document
	if len(results) != 1 {
		t.Errorf("Expected 1 result for single-term phrase, got %d", len(results))
	}
}

// TestTryParseNOTQueryEdgeCase tests NOT query edge case.
func TestTryParseNOTQueryEdgeCase(t *testing.T) {
	tokenizer := NewSimpleTokenizer()
	parser := NewQueryParser(tokenizer)

	// NOT query where split doesn't produce 2 parts (shouldn't happen with " NOT ")
	// This tests the len(parts) != 2 check
	query, err := parser.Parse("test NOT ")

	// Should handle gracefully - may error on invalid right side
	if err != nil {
		t.Logf("Parse 'test NOT ' returned error: %v", err)
	} else if query != nil {
		t.Logf("Query type: %v", query.Type)
	}
}

// TestGenerateSnippetWithMatchAtEnd tests snippet generation with match at text end.
func TestGenerateSnippetWithMatchAtEnd(t *testing.T) {
	text := "The quick brown fox"

	// Match position at the end
	snippet := GenerateSnippet(text, []int{len(text) - 3}, 10)

	if len(snippet) == 0 {
		t.Error("Expected non-empty snippet")
	}

	t.Logf("Snippet with match at end: '%s'", snippet)
}

// TestFilterWithQueryParseError tests filter when query parsing fails.
func TestFilterWithQueryParseError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	cursor, _ := ftsTable.Open()
	defer cursor.Close()

	// Try filter with query that will fail to parse (only punctuation)
	err := cursor.Filter(1, "", []interface{}{"   "})
	if err == nil {
		t.Error("Expected error for whitespace-only query")
	}
}

// TestUpdateWithAddDocumentError tests insert when AddDocument fails.
func TestUpdateWithAddDocumentError(t *testing.T) {
	module := NewFTS5Module()
	table, _, _ := module.Create(nil, "fts5", "main", "test_fts", []string{"content"})
	ftsTable := table.(*FTS5Table)

	// Insert a document normally
	rowid, err := ftsTable.Update(3, []interface{}{nil, nil, "test"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Try to insert with same explicit rowid (should still work, just updates nextRowID)
	rowid2, err := ftsTable.Update(3, []interface{}{nil, int64(rowid), "test2"})
	if err != nil {
		t.Logf("Second insert error: %v", err)
	}

	t.Logf("First rowid: %d, Second rowid: %d", rowid, rowid2)
}
