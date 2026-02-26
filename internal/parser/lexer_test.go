package parser

import (
	"testing"
)

func TestLexerBasicTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{
			"SELECT * FROM users;",
			[]TokenType{TK_SELECT, TK_STAR, TK_FROM, TK_ID, TK_SEMI, TK_EOF},
		},
		{
			"INSERT INTO mytable VALUES (1, 2, 3)",
			[]TokenType{TK_INSERT, TK_INTO, TK_ID, TK_VALUES, TK_LP, TK_INTEGER, TK_COMMA, TK_INTEGER, TK_COMMA, TK_INTEGER, TK_RP, TK_EOF},
		},
		{
			"UPDATE users SET name = 'John'",
			[]TokenType{TK_UPDATE, TK_ID, TK_SET, TK_ID, TK_EQ, TK_STRING, TK_EOF},
		},
		{
			"DELETE FROM users WHERE id > 10",
			[]TokenType{TK_DELETE, TK_FROM, TK_ID, TK_WHERE, TK_ID, TK_GT, TK_INTEGER, TK_EOF},
		},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tokens := make([]TokenType, 0)

		for {
			tok := lexer.NextToken()
			tokens = append(tokens, tok.Type)
			if tok.Type == TK_EOF {
				break
			}
		}

		if len(tokens) != len(tt.expected) {
			t.Errorf("token count mismatch for %q: got %d, want %d", tt.input, len(tokens), len(tt.expected))
			continue
		}

		for i, tokType := range tokens {
			if tokType != tt.expected[i] {
				t.Errorf("token %d mismatch for %q: got %s, want %s", i, tt.input, tokType, tt.expected[i])
			}
		}
	}
}

func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"=", TK_EQ},
		{"==", TK_EQ},
		{"<>", TK_NE},
		{"!=", TK_NE},
		{"<", TK_LT},
		{"<=", TK_LE},
		{">", TK_GT},
		{">=", TK_GE},
		{"+", TK_PLUS},
		{"-", TK_MINUS},
		{"*", TK_STAR},
		{"/", TK_SLASH},
		{"%", TK_REM},
		{"||", TK_CONCAT},
		{"&", TK_BITAND},
		{"|", TK_BITOR},
		{"~", TK_BITNOT},
		{"<<", TK_LSHIFT},
		{">>", TK_RSHIFT},
		{"->", TK_PTR},
		{"->>", TK_PTR},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tok := lexer.NextToken()
		if tok.Type != tt.expected {
			t.Errorf("operator %q: got %s, want %s", tt.input, tok.Type, tt.expected)
		}
	}
}

func TestLexerLiterals(t *testing.T) {
	tests := []struct {
		input    string
		tokType  TokenType
		expected string
	}{
		{"123", TK_INTEGER, "123"},
		{"0x1A2B", TK_INTEGER, "0x1A2B"},
		{"3.14", TK_FLOAT, "3.14"},
		{"1.5e10", TK_FLOAT, "1.5e10"},
		{"1.5E-10", TK_FLOAT, "1.5E-10"},
		{"'hello'", TK_STRING, "'hello'"},
		{"'it''s'", TK_STRING, "'it''s'"},
		{"X'48656C6C6F'", TK_BLOB, "X'48656C6C6F'"},
		{"x'DEADBEEF'", TK_BLOB, "x'DEADBEEF'"},
		{"NULL", TK_NULL, "NULL"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tok := lexer.NextToken()
		if tok.Type != tt.tokType {
			t.Errorf("literal %q: got type %s, want %s", tt.input, tok.Type, tt.tokType)
		}
		if tok.Lexeme != tt.expected {
			t.Errorf("literal %q: got lexeme %q, want %q", tt.input, tok.Lexeme, tt.expected)
		}
	}
}

func TestLexerIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`users`, "users"},
		{`user_name`, "user_name"},
		{`_private`, "_private"},
		{`table123`, "table123"},
		{`"quoted id"`, `"quoted id"`},
		{"`backtick`", "`backtick`"},
		{`[bracketed]`, `[bracketed]`},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tok := lexer.NextToken()
		if tok.Type != TK_ID {
			t.Errorf("identifier %q: got type %s, want TK_ID", tt.input, tok.Type)
		}
		if tok.Lexeme != tt.expected {
			t.Errorf("identifier %q: got %q, want %q", tt.input, tok.Lexeme, tt.expected)
		}
	}
}

func TestLexerVariables(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"?", "?"},
		{"?1", "?1"},
		{"?123", "?123"},
		{":name", ":name"},
		{"@param", "@param"},
		{"$var", "$var"},
		{"#temp", "#temp"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tok := lexer.NextToken()
		if tok.Type != TK_VARIABLE {
			t.Errorf("variable %q: got type %s, want TK_VARIABLE", tt.input, tok.Type)
		}
		if tok.Lexeme != tt.want {
			t.Errorf("variable %q: got %q, want %q", tt.input, tok.Lexeme, tt.want)
		}
	}
}

func TestLexerComments(t *testing.T) {
	tests := []struct {
		input string
		want  TokenType
	}{
		{"-- line comment", TK_COMMENT},
		{"-- comment\nSELECT", TK_COMMENT},
		{"/* block comment */", TK_COMMENT},
		{"/* multi\nline\ncomment */", TK_COMMENT},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tok := lexer.NextToken()
		if tok.Type != tt.want {
			t.Errorf("comment %q: got type %s, want %s", tt.input, tok.Type, tt.want)
		}
	}
}

func TestLexerKeywords(t *testing.T) {
	keywords := map[string]TokenType{
		"SELECT":   TK_SELECT,
		"FROM":     TK_FROM,
		"WHERE":    TK_WHERE,
		"INSERT":   TK_INSERT,
		"INTO":     TK_INTO,
		"VALUES":   TK_VALUES,
		"UPDATE":   TK_UPDATE,
		"SET":      TK_SET,
		"DELETE":   TK_DELETE,
		"CREATE":   TK_CREATE,
		"TABLE":    TK_TABLE,
		"INDEX":    TK_INDEX,
		"DROP":     TK_DROP,
		"AND":      TK_AND,
		"OR":       TK_OR,
		"NOT":      TK_NOT,
		"NULL":     TK_NULL,
		"PRIMARY":  TK_PRIMARY,
		"KEY":      TK_KEY,
		"UNIQUE":   TK_UNIQUE,
		"ORDER":    TK_ORDER,
		"BY":       TK_BY,
		"LIMIT":    TK_LIMIT,
		"OFFSET":   TK_OFFSET,
		"JOIN":     TK_JOIN,
		"LEFT":     TK_LEFT,
		"INNER":    TK_INNER,
		"ON":       TK_ON,
		"DISTINCT": TK_DISTINCT,
		"ASC":      TK_ASC,
		"DESC":     TK_DESC,
	}

	for kw, expected := range keywords {
		// Test uppercase
		lexer := NewLexer(kw)
		tok := lexer.NextToken()
		if tok.Type != expected {
			t.Errorf("keyword %q: got %s, want %s", kw, tok.Type, expected)
		}

		// Test lowercase (should be case-insensitive)
		lexer = NewLexer(toLower(kw))
		tok = lexer.NextToken()
		if tok.Type != expected {
			t.Errorf("keyword %q (lowercase): got %s, want %s", kw, tok.Type, expected)
		}
	}
}

func TestLexerLineAndColumn(t *testing.T) {
	input := "SELECT\n  *\n  FROM users"
	lexer := NewLexer(input)

	tests := []struct {
		wantType TokenType
		wantLine int
		wantCol  int
	}{
		{TK_SELECT, 1, 1},
		{TK_STAR, 2, 3},
		{TK_FROM, 3, 3},
		{TK_ID, 3, 8},
	}

	for _, tt := range tests {
		tok := lexer.NextToken()
		if tok.Type != tt.wantType {
			t.Errorf("expected token type %s, got %s", tt.wantType, tok.Type)
		}
		if tok.Line != tt.wantLine {
			t.Errorf("expected line %d, got %d", tt.wantLine, tok.Line)
		}
	}
}

func TestTokenizeAll(t *testing.T) {
	input := "SELECT id, name FROM users WHERE age > 18 ORDER BY name;"
	tokens, err := TokenizeAll(input)
	if err != nil {
		t.Fatalf("TokenizeAll failed: %v", err)
	}

	// Should have tokens excluding whitespace
	if len(tokens) < 10 {
		t.Errorf("expected at least 10 tokens, got %d", len(tokens))
	}

	// Last token should be EOF
	if tokens[len(tokens)-1].Type != TK_EOF {
		t.Errorf("last token should be EOF, got %s", tokens[len(tokens)-1].Type)
	}
}

func TestUnquote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"quoted"`, "quoted"},
		{`'string'`, "string"},
		{"`backtick`", "backtick"},
		{`[bracketed]`, "bracketed"},
		{`'it''s'`, "it's"},
		{`"say ""hello"""`, `say "hello"`},
		{"unquoted", "unquoted"},
	}

	for _, tt := range tests {
		got := Unquote(tt.input)
		if got != tt.want {
			t.Errorf("Unquote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			result[i] = s[i] + 32
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}
