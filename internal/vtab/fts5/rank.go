// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package fts5

import (
	"math"
)

// RankFunction represents a function that scores documents for relevance.
type RankFunction interface {
	// Score calculates the relevance score for a document given search terms.
	Score(index *InvertedIndex, docID DocumentID, terms []string) float64
}

// BM25Ranker implements the BM25 ranking algorithm.
// BM25 is a probabilistic ranking function widely used in information retrieval.
type BM25Ranker struct {
	K1 float64 // Term frequency saturation parameter (typically 1.2)
	B  float64 // Length normalization parameter (typically 0.75)
}

// NewBM25Ranker creates a new BM25 ranker with default parameters.
func NewBM25Ranker() *BM25Ranker {
	return &BM25Ranker{
		K1: 1.2,
		B:  0.75,
	}
}

// Score calculates the BM25 score for a document.
// The BM25 formula is:
// score = sum over terms of: IDF(term) * (f(term, doc) * (k1 + 1)) / (f(term, doc) + k1 * (1 - b + b * |doc| / avgdl))
// where:
//   - IDF(term) = log((N - df + 0.5) / (df + 0.5) + 1)
//   - f(term, doc) = frequency of term in document
//   - |doc| = length of document
//   - avgdl = average document length
//   - N = total number of documents
//   - df = number of documents containing term
func (bm25 *BM25Ranker) Score(index *InvertedIndex, docID DocumentID, terms []string) float64 {
	if len(terms) == 0 {
		return 0.0
	}

	N := float64(index.GetTotalDocuments())
	if N == 0 {
		return 0.0
	}

	params := bm25ScoreParams{
		avgdl:  index.GetAverageDocumentLength(),
		docLen: float64(index.GetDocumentLength(docID)),
		N:      N,
		K1:     bm25.K1,
		B:      bm25.B,
	}

	return bm25.calculateTotalScore(index, docID, terms, params)
}

// bm25ScoreParams holds parameters for BM25 score calculation.
type bm25ScoreParams struct {
	avgdl  float64
	docLen float64
	N      float64
	K1     float64
	B      float64
}

// calculateTotalScore calculates the total BM25 score across all terms.
func (bm25 *BM25Ranker) calculateTotalScore(index *InvertedIndex, docID DocumentID, terms []string, params bm25ScoreParams) float64 {
	score := 0.0

	for _, term := range terms {
		postings := index.GetPostingList(term)
		if len(postings) == 0 {
			continue
		}

		termFreq, found := findTermFrequency(postings, docID)
		if !found {
			continue
		}

		score += bm25.calculateTermScore(termFreq, len(postings), params)
	}

	return score
}

// calculateTermScore calculates the BM25 score component for a single term.
func (bm25 *BM25Ranker) calculateTermScore(termFreq int, df int, params bm25ScoreParams) float64 {
	idf := calculateBM25IDF(params.N, float64(df))
	tf := float64(termFreq)

	numerator := tf * (params.K1 + 1.0)
	denominator := tf + params.K1*(1.0-params.B+params.B*params.docLen/params.avgdl)

	return idf * (numerator / denominator)
}

// calculateBM25IDF calculates the IDF component for BM25.
func calculateBM25IDF(N, df float64) float64 {
	return math.Log((N-df+0.5)/(df+0.5) + 1.0)
}

// TFIDFRanker implements TF-IDF ranking.
// TF-IDF is a simpler ranking algorithm that doesn't account for document length.
type TFIDFRanker struct{}

// NewTFIDFRanker creates a new TF-IDF ranker.
func NewTFIDFRanker() *TFIDFRanker {
	return &TFIDFRanker{}
}

// Score calculates the TF-IDF score for a document.
// TF-IDF = sum over terms of: TF(term, doc) * IDF(term)
// where:
//   - TF(term, doc) = frequency of term in document / total terms in document
//   - IDF(term) = log(N / df)
func (tfidf *TFIDFRanker) Score(index *InvertedIndex, docID DocumentID, terms []string) float64 {
	if len(terms) == 0 {
		return 0.0
	}

	N := float64(index.GetTotalDocuments())
	if N == 0 {
		return 0.0
	}

	docLen := float64(index.GetDocumentLength(docID))
	if docLen == 0 {
		return 0.0
	}

	return tfidf.calculateTotalScore(index, docID, terms, N, docLen)
}

// calculateTotalScore calculates the total TF-IDF score across all terms.
func (tfidf *TFIDFRanker) calculateTotalScore(index *InvertedIndex, docID DocumentID, terms []string, N, docLen float64) float64 {
	score := 0.0

	for _, term := range terms {
		postings := index.GetPostingList(term)
		if len(postings) == 0 {
			continue
		}

		termFreq, found := findTermFrequency(postings, docID)
		if !found {
			continue
		}

		score += tfidf.calculateTermScore(termFreq, len(postings), N, docLen)
	}

	return score
}

// calculateTermScore calculates the TF-IDF score component for a single term.
func (tfidf *TFIDFRanker) calculateTermScore(termFreq int, df int, N, docLen float64) float64 {
	tf := float64(termFreq) / docLen
	idf := calculateTFIDFIDF(N, float64(df))
	return tf * idf
}

// calculateTFIDFIDF calculates the IDF component for TF-IDF.
func calculateTFIDFIDF(N, df float64) float64 {
	return math.Log(N / df)
}

// findTermFrequency finds the frequency of a term in a document from posting list.
// Returns the frequency and whether it was found.
func findTermFrequency(postings []PostingList, docID DocumentID) (int, bool) {
	for _, posting := range postings {
		if posting.DocID == docID {
			return posting.Frequency, true
		}
	}
	return 0, false
}

// SimpleRanker implements a simple frequency-based ranking.
// It simply counts the number of term matches.
type SimpleRanker struct{}

// NewSimpleRanker creates a new simple ranker.
func NewSimpleRanker() *SimpleRanker {
	return &SimpleRanker{}
}

// Score calculates a simple score based on term frequency.
func (sr *SimpleRanker) Score(index *InvertedIndex, docID DocumentID, terms []string) float64 {
	score := 0.0

	for _, term := range terms {
		postings := index.GetPostingList(term)
		for _, posting := range postings {
			if posting.DocID == docID {
				score += float64(posting.Frequency)
				break
			}
		}
	}

	return score
}

// RankResults sorts search results by score in descending order.
func RankResults(results []SearchResult) {
	// Sort by score (descending), then by DocID (ascending) for stability
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Score > results[i].Score ||
				(results[j].Score == results[i].Score && results[j].DocID < results[i].DocID) {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

// HighlightMatch represents a highlighted portion of text.
type HighlightMatch struct {
	Text     string
	IsMatch  bool
	StartPos int
	EndPos   int
}

// GenerateSnippet creates a snippet of text around matched terms.
// maxLength specifies the maximum length of the snippet in characters.
func GenerateSnippet(text string, matchPositions []int, maxLength int) string {
	if !isValidSnippetInput(text, maxLength) {
		return ""
	}

	if len(text) <= maxLength {
		return text
	}

	if len(matchPositions) == 0 {
		return truncateText(text, maxLength)
	}

	return extractSnippetAroundMatch(text, matchPositions[0], maxLength)
}

// isValidSnippetInput checks if the input for snippet generation is valid.
func isValidSnippetInput(text string, maxLength int) bool {
	return len(text) > 0 && maxLength > 0
}

// truncateText returns the first maxLength characters with ellipsis if needed.
func truncateText(text string, maxLength int) string {
	if len(text) > maxLength {
		return text[:maxLength] + "..."
	}
	return text
}

// extractSnippetAroundMatch creates a snippet centered around a match position.
func extractSnippetAroundMatch(text string, matchPos, maxLength int) string {
	start, end := calculateSnippetBounds(text, matchPos, maxLength)
	snippet := text[start:end]
	return addEllipsis(snippet, start, end, len(text))
}

// calculateSnippetBounds calculates the start and end positions for a snippet.
func calculateSnippetBounds(text string, matchPos, maxLength int) (int, int) {
	// Try to center the window around the match
	start := matchPos - maxLength/2
	if start < 0 {
		start = 0
	}

	end := start + maxLength
	if end > len(text) {
		end = len(text)
		start = end - maxLength
		if start < 0 {
			start = 0
		}
	}

	return start, end
}

// addEllipsis adds ellipsis to the snippet if it's not at the text boundaries.
func addEllipsis(snippet string, start, end, textLen int) string {
	if start > 0 {
		snippet = "..." + snippet
	}
	if end < textLen {
		snippet = snippet + "..."
	}
	return snippet
}

// HighlightText highlights matched terms in text with markers.
// startMarker and endMarker are inserted around matched terms.
func HighlightText(text string, terms []string, startMarker, endMarker string) string {
	if len(text) == 0 || len(terms) == 0 {
		return text
	}

	highlights := findHighlightRanges(text, terms)
	if len(highlights) == 0 {
		return text
	}

	return buildHighlightedText(text, highlights, startMarker, endMarker)
}

// highlight represents a range of text to highlight.
type highlight struct {
	start int
	end   int
}

// findHighlightRanges finds all ranges of text that should be highlighted.
func findHighlightRanges(text string, terms []string) []highlight {
	tokenizer := NewSimpleTokenizer()
	tokens := tokenizer.Tokenize(text)

	termSet := buildTermSet(terms)
	highlights := []highlight{}

	for _, token := range tokens {
		if termSet[token.Text] {
			highlights = append(highlights, highlight{
				start: token.Offset,
				end:   token.Offset + token.Length,
			})
		}
	}

	return highlights
}

// buildTermSet creates a set of terms for fast lookup.
func buildTermSet(terms []string) map[string]bool {
	termSet := make(map[string]bool)
	for _, term := range terms {
		termSet[term] = true
	}
	return termSet
}

// buildHighlightedText builds the final highlighted text.
func buildHighlightedText(text string, highlights []highlight, startMarker, endMarker string) string {
	result := ""
	lastPos := 0

	for _, h := range highlights {
		// Add text before highlight
		if h.start > lastPos {
			result += text[lastPos:h.start]
		}

		// Add highlighted text
		result += startMarker + text[h.start:h.end] + endMarker
		lastPos = h.end
	}

	// Add remaining text
	if lastPos < len(text) {
		result += text[lastPos:]
	}

	return result
}

// ScoreWithBoost applies a column-specific boost to scores.
// columnBoosts maps column index to boost multiplier (default 1.0).
func ScoreWithBoost(baseScore float64, columnIndex int, columnBoosts map[int]float64) float64 {
	boost := 1.0
	if b, exists := columnBoosts[columnIndex]; exists {
		boost = b
	}
	return baseScore * boost
}
