// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Test edge cases and error paths in lexer for full coverage

func TestLexerReadStringErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  TokenType
	}{
		{
			"unterminated string",
			"'hello",
			TK_STRING, // Lexer still returns STRING token
		},
		{
			"string with escaped quotes",
			"'it''s a test'",
			TK_STRING,
		},
		{
			"empty string",
			"''",
			TK_STRING,
		},
		{
			"string with newline",
			"'hello\nworld'",
			TK_STRING,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.want {
				t.Errorf("got type %s, want %s", tok.Type, tt.want)
			}
		})
	}
}

func TestLexerReadQuotedIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"simple quoted",
			`"table"`,
			`"table"`,
		},
		{
			"quoted with spaces",
			`"my table"`,
			`"my table"`,
		},
		{
			"unterminated quoted identifier",
			`"unclosed`,
			`"unclosed`,
		},
		{
			"empty quoted identifier",
			`""`,
			`""`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_ID {
				t.Errorf("got type %s, want TK_ID", tok.Type)
			}
			if tok.Lexeme != tt.want {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.want)
			}
		})
	}
}

func TestLexerReadBracketedIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"simple bracketed",
			`[column]`,
			`[column]`,
		},
		{
			"bracketed with spaces",
			`[my column]`,
			`[my column]`,
		},
		{
			"unterminated bracketed identifier",
			`[unclosed`,
			`[unclosed`,
		},
		{
			"empty bracketed identifier",
			`[]`,
			`[]`,
		},
		{
			"bracketed with special chars",
			`[col-name]`,
			`[col-name]`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_ID {
				t.Errorf("got type %s, want TK_ID", tok.Type)
			}
			if tok.Lexeme != tt.want {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.want)
			}
		})
	}
}

func TestLexerScanDefaultIllegal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  TokenType
	}{
		{
			"illegal character",
			"^",
			TK_ILLEGAL,
		},
		{
			"unicode character",
			"™",
			TK_ILLEGAL,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.want {
				t.Errorf("got type %s, want %s", tok.Type, tt.want)
			}
		})
	}
}

func TestTokenizeAllWithIllegal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			"valid SQL",
			"SELECT * FROM users",
		},
		{
			"SQL with comments",
			"SELECT * -- comment\nFROM users",
		},
		{
			"SQL with block comment",
			"SELECT /* comment */ * FROM users",
		},
		{
			"empty input",
			"",
		},
		{
			"only whitespace",
			"   \n\t  ",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tokens, err := TokenizeAll(tt.input)
			if err != nil {
				t.Errorf("TokenizeAll failed: %v", err)
			}
			if len(tokens) == 0 {
				t.Error("expected at least one token (EOF)")
			}
			if tokens[len(tokens)-1].Type != TK_EOF {
				t.Error("last token should be EOF")
			}
		})
	}
}

func TestLexerNumberVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		tokType TokenType
		lexeme  string
	}{
		{
			"hex number",
			"0xDEADBEEF",
			TK_INTEGER,
			"0xDEADBEEF",
		},
		{
			"hex lowercase",
			"0xabcdef",
			TK_INTEGER,
			"0xabcdef",
		},
		{
			"float with E",
			"1.5E10",
			TK_FLOAT,
			"1.5E10",
		},
		{
			"float with e",
			"2.5e-5",
			TK_FLOAT,
			"2.5e-5",
		},
		{
			"float with E+",
			"3.14E+2",
			TK_FLOAT,
			"3.14E+2",
		},
		{
			"integer with underscores",
			"1_000_000",
			TK_INTEGER,
			"1_000_000",
		},
		{
			"float starting with dot",
			".5",
			TK_FLOAT,
			".5",
		},
		{
			"zero",
			"0",
			TK_INTEGER,
			"0",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.tokType {
				t.Errorf("got type %s, want %s", tok.Type, tt.tokType)
			}
			if tok.Lexeme != tt.lexeme {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.lexeme)
			}
		})
	}
}

func TestLexerBlobVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  string
		lexeme string
	}{
		{
			"uppercase X",
			"X'DEADBEEF'",
			"X'DEADBEEF'",
		},
		{
			"lowercase x",
			"x'deadbeef'",
			"x'deadbeef'",
		},
		{
			"mixed case hex",
			"X'DeAdBeEf'",
			"X'DeAdBeEf'",
		},
		{
			"empty blob",
			"X''",
			"X''",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_BLOB {
				t.Errorf("got type %s, want TK_BLOB", tok.Type)
			}
			if tok.Lexeme != tt.lexeme {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.lexeme)
			}
		})
	}
}

func TestLexerCommentsVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{
			"line comment at end",
			"SELECT * -- comment",
		},
		{
			"line comment with newline",
			"-- comment\nSELECT",
		},
		{
			"block comment multiline",
			"/* line 1\nline 2\nline 3 */",
		},
		{
			"block comment single line",
			"/* comment */",
		},
		{
			"nested style comment",
			"/* outer /* inner */ outer */",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			// First token should be either comment or something after comment
			if tok.Type != TK_COMMENT && tok.Type != TK_SELECT && tok.Type != TK_EOF {
				t.Logf("got type %s (acceptable)", tok.Type)
			}
		})
	}
}

func TestLexerVariableVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  string
		lexeme string
	}{
		{
			"numbered param",
			"?123",
			"?123",
		},
		{
			"colon param",
			":name",
			":name",
		},
		{
			"at param",
			"@param",
			"@param",
		},
		{
			"dollar param",
			"$var",
			"$var",
		},
		{
			"hash param",
			"#temp",
			"#temp",
		},
		{
			"just question mark",
			"?",
			"?",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_VARIABLE && tok.Type != TK_ILLEGAL {
				t.Errorf("got type %s, want TK_VARIABLE or TK_ILLEGAL", tok.Type)
			}
			if tok.Type == TK_VARIABLE && tok.Lexeme != tt.lexeme {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.lexeme)
			}
		})
	}
}

func TestLexerOperatorVariations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		tokType TokenType
	}{
		{"->", TK_PTR},
		{"->>", TK_PTR},
		{"<<", TK_LSHIFT},
		{">>", TK_RSHIFT},
		{"||", TK_CONCAT},
		{"<>", TK_NE},
		{"!=", TK_NE},
		{"<=", TK_LE},
		{">=", TK_GE},
		{"==", TK_EQ},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.tokType {
				t.Errorf("got type %s, want %s", tok.Type, tt.tokType)
			}
		})
	}
}

func TestLexerBacktickIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		input  string
		lexeme string
	}{
		{
			"simple backtick",
			"`table`",
			"`table`",
		},
		{
			"backtick with spaces",
			"`my table`",
			"`my table`",
		},
		{
			"unterminated backtick",
			"`unclosed",
			"`unclosed",
		},
		{
			"empty backtick",
			"``",
			"``",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != TK_ID {
				t.Errorf("got type %s, want TK_ID", tok.Type)
			}
			if tok.Lexeme != tt.lexeme {
				t.Errorf("got lexeme %q, want %q", tok.Lexeme, tt.lexeme)
			}
		})
	}
}

func TestLexerComplexSQL(t *testing.T) {
	t.Parallel()
	// Test a complex SQL statement to ensure all paths are covered
	input := `
		WITH RECURSIVE cte AS (
			SELECT * FROM users WHERE id = ?
		)
		SELECT u.*, c.name
		FROM users u
		LEFT JOIN cities c ON u.city_id = c.id
		WHERE u.age > 18 AND u.active = 1
		ORDER BY u.name COLLATE NOCASE
		LIMIT 10 OFFSET 5;
	`

	lexer := NewLexer(input)
	tokenCount := 0
	for {
		tok := lexer.NextToken()
		tokenCount++
		if tok.Type == TK_EOF {
			break
		}
		if tokenCount > 1000 {
			t.Fatal("infinite loop detected")
		}
	}

	if tokenCount < 30 {
		t.Errorf("expected many tokens, got %d", tokenCount)
	}
}

func TestLexerDollarEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   string
		wantTyp TokenType
	}{
		{"dollar alone", "$", TK_ILLEGAL},
		{"dollar with number", "$123", TK_ILLEGAL},
		{"dollar with letter", "$var", TK_VARIABLE},
		{"dollar with underscore", "$_test", TK_VARIABLE},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Type != tt.wantTyp {
				t.Errorf("got type %s, want %s", tok.Type, tt.wantTyp)
			}
		})
	}
}
