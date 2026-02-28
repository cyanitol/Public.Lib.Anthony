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

	avgdl := index.GetAverageDocumentLength()
	docLen := float64(index.GetDocumentLength(docID))

	score := 0.0

	// Calculate score for each term
	for _, term := range terms {
		postings := index.GetPostingList(term)
		if len(postings) == 0 {
			continue
		}

		// Find posting for this document
		var termFreq int
		found := false
		for _, posting := range postings {
			if posting.DocID == docID {
				termFreq = posting.Frequency
				found = true
				break
			}
		}

		if !found {
			continue
		}

		// Calculate IDF
		df := float64(len(postings)) // document frequency
		idf := math.Log((N-df+0.5)/(df+0.5) + 1.0)

		// Calculate BM25 score component for this term
		tf := float64(termFreq)
		numerator := tf * (bm25.K1 + 1.0)
		denominator := tf + bm25.K1*(1.0-bm25.B+bm25.B*docLen/avgdl)

		score += idf * (numerator / denominator)
	}

	return score
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

	score := 0.0

	for _, term := range terms {
		postings := index.GetPostingList(term)
		if len(postings) == 0 {
			continue
		}

		// Find posting for this document
		var termFreq int
		found := false
		for _, posting := range postings {
			if posting.DocID == docID {
				termFreq = posting.Frequency
				found = true
				break
			}
		}

		if !found {
			continue
		}

		// Calculate TF
		tf := float64(termFreq) / docLen

		// Calculate IDF
		df := float64(len(postings))
		idf := math.Log(N / df)

		score += tf * idf
	}

	return score
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
	Text      string
	IsMatch   bool
	StartPos  int
	EndPos    int
}

// GenerateSnippet creates a snippet of text around matched terms.
// maxLength specifies the maximum length of the snippet in characters.
func GenerateSnippet(text string, matchPositions []int, maxLength int) string {
	if len(text) == 0 || maxLength <= 0 {
		return ""
	}

	// If text is shorter than max length, return it all
	if len(text) <= maxLength {
		return text
	}

	// If no matches, return first maxLength characters
	if len(matchPositions) == 0 {
		if len(text) > maxLength {
			return text[:maxLength] + "..."
		}
		return text
	}

	// Find the best window around matches
	// For simplicity, center around the first match
	firstMatch := matchPositions[0]

	// Try to center the window around the match
	start := firstMatch - maxLength/2
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

	snippet := text[start:end]

	// Add ellipsis if we're not at the beginning/end
	if start > 0 {
		snippet = "..." + snippet
	}
	if end < len(text) {
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

	// Create a tokenizer to find term positions
	tokenizer := NewSimpleTokenizer()
	tokens := tokenizer.Tokenize(text)

	// Build a set of terms to match
	termSet := make(map[string]bool)
	for _, term := range terms {
		termSet[term] = true
	}

	// Track which character ranges should be highlighted
	type highlight struct {
		start int
		end   int
	}
	highlights := []highlight{}

	for _, token := range tokens {
		if termSet[token.Text] {
			highlights = append(highlights, highlight{
				start: token.Offset,
				end:   token.Offset + token.Length,
			})
		}
	}

	if len(highlights) == 0 {
		return text
	}

	// Build the highlighted text
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
