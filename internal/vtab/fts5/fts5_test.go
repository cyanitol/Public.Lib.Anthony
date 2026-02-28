package fts5

import (
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
