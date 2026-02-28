package fts5

import (
	"strings"
	"unicode"
)

// Token represents a single token from tokenization.
type Token struct {
	Text     string // The token text (normalized)
	Position int    // The position in the original text (0-based)
	Offset   int    // Byte offset in the original text
	Length   int    // Length in bytes
}

// Tokenizer interface defines methods for tokenizing text.
type Tokenizer interface {
	// Tokenize breaks text into tokens.
	Tokenize(text string) []Token
}

// SimpleTokenizer is a basic tokenizer that splits on whitespace and punctuation.
// It normalizes tokens to lowercase and handles ASCII text.
type SimpleTokenizer struct {
	// MinTokenLength is the minimum length for a token (default: 1)
	MinTokenLength int
	// MaxTokenLength is the maximum length for a token (default: 100)
	MaxTokenLength int
}

// NewSimpleTokenizer creates a new simple tokenizer with default settings.
func NewSimpleTokenizer() *SimpleTokenizer {
	return &SimpleTokenizer{
		MinTokenLength: 1,
		MaxTokenLength: 100,
	}
}

// Tokenize breaks text into tokens by splitting on non-alphanumeric characters.
func (t *SimpleTokenizer) Tokenize(text string) []Token {
	tokens := []Token{}
	position := 0
	currentToken := strings.Builder{}
	tokenStart := 0

	for offset, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			// Part of a token
			if currentToken.Len() == 0 {
				tokenStart = offset
			}
			currentToken.WriteRune(unicode.ToLower(r))
		} else {
			// End of token
			if currentToken.Len() > 0 {
				tokenText := currentToken.String()
				if len(tokenText) >= t.MinTokenLength && len(tokenText) <= t.MaxTokenLength {
					tokens = append(tokens, Token{
						Text:     tokenText,
						Position: position,
						Offset:   tokenStart,
						Length:   len(tokenText),
					})
					position++
				}
				currentToken.Reset()
			}
		}
	}

	// Handle last token
	if currentToken.Len() > 0 {
		tokenText := currentToken.String()
		if len(tokenText) >= t.MinTokenLength && len(tokenText) <= t.MaxTokenLength {
			tokens = append(tokens, Token{
				Text:     tokenText,
				Position: position,
				Offset:   tokenStart,
				Length:   len(tokenText),
			})
		}
	}

	return tokens
}

// PrefixTokenizer wraps another tokenizer and also generates prefix tokens.
// This enables prefix matching queries like "hel*".
type PrefixTokenizer struct {
	base           Tokenizer
	minPrefixLen   int
	maxPrefixLen   int
}

// NewPrefixTokenizer creates a new prefix tokenizer.
func NewPrefixTokenizer(base Tokenizer, minPrefixLen, maxPrefixLen int) *PrefixTokenizer {
	return &PrefixTokenizer{
		base:         base,
		minPrefixLen: minPrefixLen,
		maxPrefixLen: maxPrefixLen,
	}
}

// Tokenize tokenizes text and generates prefix tokens.
func (pt *PrefixTokenizer) Tokenize(text string) []Token {
	baseTokens := pt.base.Tokenize(text)
	tokens := make([]Token, 0, len(baseTokens)*2)

	for _, token := range baseTokens {
		// Add the full token
		tokens = append(tokens, token)

		// Add prefix tokens if configured
		if pt.minPrefixLen > 0 && len(token.Text) >= pt.minPrefixLen {
			maxLen := pt.maxPrefixLen
			if maxLen > len(token.Text) {
				maxLen = len(token.Text)
			}

			for i := pt.minPrefixLen; i < maxLen; i++ {
				prefixToken := Token{
					Text:     token.Text[:i],
					Position: token.Position,
					Offset:   token.Offset,
					Length:   i,
				}
				tokens = append(tokens, prefixToken)
			}
		}
	}

	return tokens
}

// StopWords contains common English stop words that can be filtered.
var StopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "for": true, "if": true, "in": true,
	"into": true, "is": true, "it": true, "no": true, "not": true, "of": true,
	"on": true, "or": true, "over": true, "such": true, "that": true, "the": true, "their": true,
	"then": true, "there": true, "these": true, "they": true, "this": true,
	"to": true, "was": true, "will": true, "with": true,
}

// StopWordTokenizer wraps another tokenizer and filters out stop words.
type StopWordTokenizer struct {
	base      Tokenizer
	stopWords map[string]bool
}

// NewStopWordTokenizer creates a new stop word filtering tokenizer.
func NewStopWordTokenizer(base Tokenizer, stopWords map[string]bool) *StopWordTokenizer {
	if stopWords == nil {
		stopWords = StopWords
	}
	return &StopWordTokenizer{
		base:      base,
		stopWords: stopWords,
	}
}

// Tokenize tokenizes text and filters out stop words.
func (st *StopWordTokenizer) Tokenize(text string) []Token {
	baseTokens := st.base.Tokenize(text)
	tokens := make([]Token, 0, len(baseTokens))

	position := 0
	for _, token := range baseTokens {
		if !st.stopWords[token.Text] {
			// Update position to maintain sequential ordering
			token.Position = position
			tokens = append(tokens, token)
			position++
		}
	}

	return tokens
}

// IsStopWord returns true if the given word is a stop word.
func IsStopWord(word string) bool {
	return StopWords[strings.ToLower(word)]
}
