package fts5

import (
	"fmt"
	"strings"
)

// QueryType represents the type of FTS query.
type QueryType int

const (
	QuerySimple  QueryType = iota // Simple term search
	QueryPhrase                   // Phrase search "term1 term2"
	QueryAND                      // Boolean AND: term1 AND term2
	QueryOR                       // Boolean OR: term1 OR term2
	QueryNOT                      // Boolean NOT: term1 NOT term2
	QueryPrefix                   // Prefix search: term*
)

// Query represents a parsed FTS search query.
type Query struct {
	Type     QueryType
	Terms    []string  // Search terms
	Children []*Query  // Child queries for boolean operations
	Column   int       // Specific column to search (-1 for all)
}

// QueryParser parses FTS query strings into Query objects.
type QueryParser struct {
	tokenizer Tokenizer
}

// NewQueryParser creates a new query parser.
func NewQueryParser(tokenizer Tokenizer) *QueryParser {
	return &QueryParser{
		tokenizer: tokenizer,
	}
}

// Parse parses a query string into a Query object.
// Supports:
//   - Simple terms: "search query"
//   - Phrases: "\"exact phrase\""
//   - Boolean operators: "term1 AND term2", "term1 OR term2", "term1 NOT term2"
//   - Prefix: "term*"
//   - Column filters: "column:term"
func (qp *QueryParser) Parse(queryStr string) (*Query, error) {
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Check for phrase query (quoted)
	if query, ok := qp.tryParsePhraseQuery(queryStr); ok {
		return query, nil
	}

	// Check for boolean operators
	query, err := qp.tryParseBooleanQuery(queryStr)
	if query != nil || err != nil {
		return query, err
	}

	// Parse column filter if present
	column, queryStr := qp.parseColumnFilter(queryStr)

	// Check for prefix query
	if query, ok := qp.tryParsePrefixQuery(queryStr, column); ok {
		return query, nil
	}

	// Simple term query
	return qp.parseSimpleQuery(queryStr, column)
}

// tryParseBooleanQuery attempts to parse any boolean query (AND, OR, NOT).
func (qp *QueryParser) tryParseBooleanQuery(queryStr string) (*Query, error) {
	// Check for AND
	if query, err := qp.tryParseANDQuery(queryStr); query != nil || err != nil {
		return query, err
	}

	// Check for OR
	if query, err := qp.tryParseORQuery(queryStr); query != nil || err != nil {
		return query, err
	}

	// Check for NOT
	return qp.tryParseNOTQuery(queryStr)
}

// tryParsePhraseQuery attempts to parse a phrase query (quoted string).
func (qp *QueryParser) tryParsePhraseQuery(queryStr string) (*Query, bool) {
	if !strings.HasPrefix(queryStr, "\"") || !strings.HasSuffix(queryStr, "\"") {
		return nil, false
	}

	phrase := strings.Trim(queryStr, "\"")
	tokens := qp.tokenizer.Tokenize(phrase)
	terms := make([]string, len(tokens))
	for i, token := range tokens {
		terms[i] = token.Text
	}

	return &Query{
		Type:   QueryPhrase,
		Terms:  terms,
		Column: -1,
	}, true
}

// tryParseANDQuery attempts to parse an AND boolean query.
func (qp *QueryParser) tryParseANDQuery(queryStr string) (*Query, error) {
	if !strings.Contains(queryStr, " AND ") {
		return nil, nil
	}

	parts := strings.Split(queryStr, " AND ")
	children := make([]*Query, len(parts))
	for i, part := range parts {
		child, err := qp.Parse(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		children[i] = child
	}

	return &Query{
		Type:     QueryAND,
		Children: children,
		Column:   -1,
	}, nil
}

// tryParseORQuery attempts to parse an OR boolean query.
func (qp *QueryParser) tryParseORQuery(queryStr string) (*Query, error) {
	if !strings.Contains(queryStr, " OR ") {
		return nil, nil
	}

	parts := strings.Split(queryStr, " OR ")
	children := make([]*Query, len(parts))
	for i, part := range parts {
		child, err := qp.Parse(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		children[i] = child
	}

	return &Query{
		Type:     QueryOR,
		Children: children,
		Column:   -1,
	}, nil
}

// tryParseNOTQuery attempts to parse a NOT boolean query.
func (qp *QueryParser) tryParseNOTQuery(queryStr string) (*Query, error) {
	if !strings.Contains(queryStr, " NOT ") {
		return nil, nil
	}

	parts := strings.SplitN(queryStr, " NOT ", 2)
	if len(parts) != 2 {
		return nil, nil
	}

	left, err := qp.Parse(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, err
	}

	right, err := qp.Parse(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}

	return &Query{
		Type:     QueryNOT,
		Children: []*Query{left, right},
		Column:   -1,
	}, nil
}

// parseColumnFilter extracts column filter from query string.
// Returns the column index and the remaining query string.
func (qp *QueryParser) parseColumnFilter(queryStr string) (int, string) {
	if !strings.Contains(queryStr, ":") {
		return -1, queryStr
	}

	parts := strings.SplitN(queryStr, ":", 2)
	if len(parts) != 2 {
		return -1, queryStr
	}

	// Note: In a real implementation, we'd resolve column name to index
	// For now, we just mark that a column filter was specified
	return -1, strings.TrimSpace(parts[1])
}

// tryParsePrefixQuery attempts to parse a prefix query (term*).
func (qp *QueryParser) tryParsePrefixQuery(queryStr string, column int) (*Query, bool) {
	if !strings.HasSuffix(queryStr, "*") {
		return nil, false
	}

	term := strings.TrimSuffix(queryStr, "*")
	tokens := qp.tokenizer.Tokenize(term)
	if len(tokens) == 0 {
		return nil, false
	}

	return &Query{
		Type:   QueryPrefix,
		Terms:  []string{tokens[0].Text},
		Column: column,
	}, true
}

// parseSimpleQuery parses a simple term query.
func (qp *QueryParser) parseSimpleQuery(queryStr string, column int) (*Query, error) {
	tokens := qp.tokenizer.Tokenize(queryStr)
	terms := make([]string, len(tokens))
	for i, token := range tokens {
		terms[i] = token.Text
	}

	if len(terms) == 0 {
		return nil, fmt.Errorf("no valid terms in query")
	}

	return &Query{
		Type:   QuerySimple,
		Terms:  terms,
		Column: column,
	}, nil
}

// QueryExecutor executes queries against an inverted index.
type QueryExecutor struct {
	index  *InvertedIndex
	ranker RankFunction
}

// NewQueryExecutor creates a new query executor.
func NewQueryExecutor(index *InvertedIndex, ranker RankFunction) *QueryExecutor {
	if ranker == nil {
		ranker = NewBM25Ranker()
	}
	return &QueryExecutor{
		index:  index,
		ranker: ranker,
	}
}

// Execute executes a query and returns ranked search results.
func (qe *QueryExecutor) Execute(query *Query) ([]SearchResult, error) {
	if query == nil {
		return []SearchResult{}, fmt.Errorf("nil query")
	}

	// Get matching documents
	matchingDocs := qe.getMatchingDocuments(query)

	// Score and rank results
	results := make([]SearchResult, 0, len(matchingDocs))
	for docID := range matchingDocs {
		score := qe.scoreDocument(docID, query)
		matches := qe.getMatches(docID, query)

		results = append(results, SearchResult{
			DocID:   docID,
			Score:   score,
			Matches: matches,
		})
	}

	// Rank by score
	RankResults(results)

	return results, nil
}

// getMatchingDocuments returns the set of documents matching a query.
func (qe *QueryExecutor) getMatchingDocuments(query *Query) map[DocumentID]bool {
	switch query.Type {
	case QuerySimple:
		return qe.matchSimple(query.Terms)

	case QueryPhrase:
		return qe.matchPhrase(query.Terms)

	case QueryAND:
		return qe.matchAND(query.Children)

	case QueryOR:
		return qe.matchOR(query.Children)

	case QueryNOT:
		return qe.matchNOT(query.Children)

	case QueryPrefix:
		return qe.matchPrefix(query.Terms[0])

	default:
		return make(map[DocumentID]bool)
	}
}

// matchSimple matches documents containing any of the terms.
func (qe *QueryExecutor) matchSimple(terms []string) map[DocumentID]bool {
	docs := make(map[DocumentID]bool)

	for _, term := range terms {
		postings := qe.index.GetPostingList(term)
		for _, posting := range postings {
			docs[posting.DocID] = true
		}
	}

	return docs
}

// matchPhrase matches documents containing the exact phrase.
func (qe *QueryExecutor) matchPhrase(terms []string) map[DocumentID]bool {
	if len(terms) == 0 {
		return make(map[DocumentID]bool)
	}

	// Start with documents containing the first term
	docs := qe.matchSimple(terms[:1])

	// Filter to only documents with the exact phrase
	result := make(map[DocumentID]bool)
	for docID := range docs {
		if qe.index.PhraseMatch(terms, docID) {
			result[docID] = true
		}
	}

	return result
}

// matchAND matches documents containing all child query results.
func (qe *QueryExecutor) matchAND(children []*Query) map[DocumentID]bool {
	if len(children) == 0 {
		return make(map[DocumentID]bool)
	}

	// Start with first child's results
	result := qe.getMatchingDocuments(children[0])

	// Intersect with remaining children
	for i := 1; i < len(children); i++ {
		childDocs := qe.getMatchingDocuments(children[i])

		// Keep only docs in both sets
		for docID := range result {
			if !childDocs[docID] {
				delete(result, docID)
			}
		}
	}

	return result
}

// matchOR matches documents containing any child query results.
func (qe *QueryExecutor) matchOR(children []*Query) map[DocumentID]bool {
	result := make(map[DocumentID]bool)

	for _, child := range children {
		childDocs := qe.getMatchingDocuments(child)
		for docID := range childDocs {
			result[docID] = true
		}
	}

	return result
}

// matchNOT matches documents in the first child but not in the second.
func (qe *QueryExecutor) matchNOT(children []*Query) map[DocumentID]bool {
	if len(children) < 2 {
		return make(map[DocumentID]bool)
	}

	// Get documents from first child
	result := qe.getMatchingDocuments(children[0])

	// Remove documents from second child
	excludeDocs := qe.getMatchingDocuments(children[1])
	for docID := range excludeDocs {
		delete(result, docID)
	}

	return result
}

// matchPrefix matches documents containing terms with the given prefix.
func (qe *QueryExecutor) matchPrefix(prefix string) map[DocumentID]bool {
	docs := make(map[DocumentID]bool)

	// Find all terms starting with the prefix
	for _, term := range qe.index.GetTerms() {
		if strings.HasPrefix(term, prefix) {
			postings := qe.index.GetPostingList(term)
			for _, posting := range postings {
				docs[posting.DocID] = true
			}
		}
	}

	return docs
}

// scoreDocument scores a document for a query.
func (qe *QueryExecutor) scoreDocument(docID DocumentID, query *Query) float64 {
	terms := qe.extractTerms(query)
	return qe.ranker.Score(qe.index, docID, terms)
}

// extractTerms extracts all search terms from a query.
func (qe *QueryExecutor) extractTerms(query *Query) []string {
	if query == nil {
		return []string{}
	}

	terms := make([]string, 0)

	// Add terms from this query
	terms = append(terms, query.Terms...)

	// Recursively add terms from children
	for _, child := range query.Children {
		terms = append(terms, qe.extractTerms(child)...)
	}

	return terms
}

// getMatches retrieves match information for a document and query.
func (qe *QueryExecutor) getMatches(docID DocumentID, query *Query) []MatchInfo {
	terms := qe.extractTerms(query)
	matches := make([]MatchInfo, 0)

	for _, term := range terms {
		postings := qe.index.GetPostingList(term)
		for _, posting := range postings {
			if posting.DocID == docID {
				matches = append(matches, MatchInfo{
					Term:      term,
					Positions: posting.Positions,
					Frequency: posting.Frequency,
				})
				break
			}
		}
	}

	return matches
}

// MatchOperator applies a MATCH operator for filtering.
func MatchOperator(index *InvertedIndex, queryStr string, docID DocumentID) (bool, error) {
	parser := NewQueryParser(NewSimpleTokenizer())
	query, err := parser.Parse(queryStr)
	if err != nil {
		return false, err
	}

	executor := NewQueryExecutor(index, nil)
	matchingDocs := executor.getMatchingDocuments(query)

	return matchingDocs[docID], nil
}
