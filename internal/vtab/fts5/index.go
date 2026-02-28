package fts5

import (
	"fmt"
	"sort"
	"sync"
)

// DocumentID represents a unique document identifier.
type DocumentID int64

// PostingList contains the positions where a term appears in documents.
type PostingList struct {
	DocID     DocumentID
	Positions []int // Token positions within the document
	Frequency int   // Number of times the term appears in this document
}

// InvertedIndex maps terms to their posting lists.
// This is the core data structure for FTS5.
type InvertedIndex struct {
	mu sync.RWMutex

	// index maps term -> list of documents containing the term
	index map[string][]PostingList

	// docLengths stores the length (in tokens) of each document
	docLengths map[DocumentID]int

	// docColumns stores which columns each document has data in
	// Maps docID -> columnIndex -> content
	docColumns map[DocumentID]map[int]string

	// columnNames stores the names of indexed columns
	columnNames []string

	// totalDocs is the total number of documents indexed
	totalDocs int

	// avgDocLength is the average document length (in tokens)
	avgDocLength float64
}

// NewInvertedIndex creates a new inverted index.
func NewInvertedIndex(columnNames []string) *InvertedIndex {
	return &InvertedIndex{
		index:       make(map[string][]PostingList),
		docLengths:  make(map[DocumentID]int),
		docColumns:  make(map[DocumentID]map[int]string),
		columnNames: columnNames,
		totalDocs:   0,
		avgDocLength: 0.0,
	}
}

// AddDocument adds a document to the index.
// columns is a map of column index to text content.
func (idx *InvertedIndex) AddDocument(docID DocumentID, columns map[int]string, tokenizer Tokenizer) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Store document columns
	if idx.docColumns[docID] == nil {
		idx.docColumns[docID] = make(map[int]string)
	}

	totalTokens := 0
	termPositions := make(map[string][]int)

	// Tokenize each column and build term positions
	for colIdx, content := range columns {
		// Store the original content
		idx.docColumns[docID][colIdx] = content

		tokens := tokenizer.Tokenize(content)
		for _, token := range tokens {
			termPositions[token.Text] = append(termPositions[token.Text], token.Position+totalTokens)
		}
		totalTokens += len(tokens)
	}

	// Update document length
	idx.docLengths[docID] = totalTokens

	// Add to inverted index
	for term, positions := range termPositions {
		posting := PostingList{
			DocID:     docID,
			Positions: positions,
			Frequency: len(positions),
		}

		// Find or create posting list for this term
		idx.index[term] = append(idx.index[term], posting)
	}

	// Update statistics
	idx.totalDocs++
	idx.updateAvgDocLength()

	return nil
}

// RemoveDocument removes a document from the index.
func (idx *InvertedIndex) RemoveDocument(docID DocumentID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove from doc lengths
	delete(idx.docLengths, docID)

	// Remove from doc columns
	delete(idx.docColumns, docID)

	// Remove from inverted index
	for term, postings := range idx.index {
		newPostings := []PostingList{}
		for _, posting := range postings {
			if posting.DocID != docID {
				newPostings = append(newPostings, posting)
			}
		}

		if len(newPostings) == 0 {
			delete(idx.index, term)
		} else {
			idx.index[term] = newPostings
		}
	}

	// Update statistics
	if idx.totalDocs > 0 {
		idx.totalDocs--
	}
	idx.updateAvgDocLength()

	return nil
}

// GetPostingList retrieves the posting list for a term.
func (idx *InvertedIndex) GetPostingList(term string) []PostingList {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	postings, exists := idx.index[term]
	if !exists {
		return []PostingList{}
	}

	// Return a copy to prevent external modification
	result := make([]PostingList, len(postings))
	copy(result, postings)
	return result
}

// GetDocumentContent retrieves the original content for a document and column.
func (idx *InvertedIndex) GetDocumentContent(docID DocumentID, colIdx int) (string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	docCols, exists := idx.docColumns[docID]
	if !exists {
		return "", false
	}

	content, exists := docCols[colIdx]
	return content, exists
}

// GetDocumentLength returns the length (in tokens) of a document.
func (idx *InvertedIndex) GetDocumentLength(docID DocumentID) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.docLengths[docID]
}

// GetDocumentFrequency returns the number of documents containing the term.
func (idx *InvertedIndex) GetDocumentFrequency(term string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	postings, exists := idx.index[term]
	if !exists {
		return 0
	}
	return len(postings)
}

// GetTotalDocuments returns the total number of documents in the index.
func (idx *InvertedIndex) GetTotalDocuments() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.totalDocs
}

// GetAverageDocumentLength returns the average document length in tokens.
func (idx *InvertedIndex) GetAverageDocumentLength() float64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.avgDocLength
}

// GetColumnNames returns the names of indexed columns.
func (idx *InvertedIndex) GetColumnNames() []string {
	return idx.columnNames
}

// updateAvgDocLength recalculates the average document length.
// Must be called with lock held.
func (idx *InvertedIndex) updateAvgDocLength() {
	if idx.totalDocs == 0 {
		idx.avgDocLength = 0.0
		return
	}

	totalLength := 0
	for _, length := range idx.docLengths {
		totalLength += length
	}

	idx.avgDocLength = float64(totalLength) / float64(idx.totalDocs)
}

// GetAllDocuments returns all document IDs in the index.
func (idx *InvertedIndex) GetAllDocuments() []DocumentID {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	docIDs := make([]DocumentID, 0, len(idx.docLengths))
	for docID := range idx.docLengths {
		docIDs = append(docIDs, docID)
	}

	// Sort for consistent ordering
	sort.Slice(docIDs, func(i, j int) bool {
		return docIDs[i] < docIDs[j]
	})

	return docIDs
}

// SearchResult represents a document matching a search query.
type SearchResult struct {
	DocID     DocumentID
	Score     float64
	Matches   []MatchInfo // Information about matched terms
}

// MatchInfo contains information about a matched term in a document.
type MatchInfo struct {
	Term      string
	Positions []int
	Frequency int
}

// PhraseMatch checks if a phrase (sequence of terms) appears in a document.
func (idx *InvertedIndex) PhraseMatch(terms []string, docID DocumentID) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(terms) == 0 {
		return false
	}

	// Get posting lists for all terms
	postingLists := make([][]PostingList, len(terms))
	for i, term := range terms {
		postingLists[i] = idx.index[term]
	}

	// Find the posting for this document in each term's list
	termPositions := make([][]int, len(terms))
	for i, postings := range postingLists {
		found := false
		for _, posting := range postings {
			if posting.DocID == docID {
				termPositions[i] = posting.Positions
				found = true
				break
			}
		}
		if !found {
			// Document doesn't contain one of the terms
			return false
		}
	}

	// Check if terms appear consecutively
	// For each position of the first term, check if subsequent terms follow
	for _, firstPos := range termPositions[0] {
		match := true
		for i := 1; i < len(terms); i++ {
			expectedPos := firstPos + i
			found := false
			for _, pos := range termPositions[i] {
				if pos == expectedPos {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}

	return false
}

// GetTerms returns all unique terms in the index.
func (idx *InvertedIndex) GetTerms() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	terms := make([]string, 0, len(idx.index))
	for term := range idx.index {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

// Clear removes all documents from the index.
func (idx *InvertedIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.index = make(map[string][]PostingList)
	idx.docLengths = make(map[DocumentID]int)
	idx.docColumns = make(map[DocumentID]map[int]string)
	idx.totalDocs = 0
	idx.avgDocLength = 0.0
}

// Stats returns statistics about the index.
func (idx *InvertedIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexStats{
		TotalDocuments:   idx.totalDocs,
		TotalTerms:       len(idx.index),
		AverageDocLength: idx.avgDocLength,
	}
}

// IndexStats contains statistics about the inverted index.
type IndexStats struct {
	TotalDocuments   int
	TotalTerms       int
	AverageDocLength float64
}

// String returns a string representation of the stats.
func (s IndexStats) String() string {
	return fmt.Sprintf("Documents: %d, Terms: %d, Avg Length: %.2f",
		s.TotalDocuments, s.TotalTerms, s.AverageDocLength)
}
