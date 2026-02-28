package parser

import "testing"

func TestTokenTypeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tokType TokenType
		want    string
	}{
		{TK_EOF, "EOF"},
		{TK_SELECT, "SELECT"},
		{TK_FROM, "FROM"},
		{TK_WHERE, "WHERE"},
		{TK_INTEGER, "INTEGER"},
		{TK_FLOAT, "FLOAT"},
		{TK_STRING, "STRING"},
		{TK_EQ, "EQ"},
		{TK_NE, "NE"},
		{TK_PLUS, "PLUS"},
		{TK_LP, "LP"},
		{TK_RP, "RP"},
		{TokenType(9999), "UNKNOWN"}, // Unknown token type
	}

	for _, tt := range tests {
		tt := tt
		got := tt.tokType.String()
		if got != tt.want {
			t.Errorf("TokenType(%d).String() = %q, want %q", tt.tokType, got, tt.want)
		}
	}
}

func TestTokenTypeIsKeyword(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tokType TokenType
		want    bool
	}{
		{TK_SELECT, true},
		{TK_FROM, true},
		{TK_WHERE, true},
		{TK_CREATE, true},
		{TK_DROP, true},
		{TK_ALTER, true},
		{TK_INSERT, true},
		{TK_UPDATE, true},
		{TK_DELETE, true},
		{TK_STORED, true}, // Last keyword in the range
		{TK_INSTEAD, false},  // After the keyword range
		{TK_DO, false},
		{TK_NOTHING, false},
		{TK_INTEGER, false},
		{TK_STRING, false},
		{TK_EOF, false},
		{TK_ILLEGAL, false},
		{TK_PTR, false}, // Special operator after keywords
	}

	for _, tt := range tests {
		tt := tt
		got := tt.tokType.IsKeyword()
		if got != tt.want {
			t.Errorf("TokenType(%s).IsKeyword() = %v, want %v", tt.tokType, got, tt.want)
		}
	}
}

func TestTokenTypeIsOperator(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tokType TokenType
		want    bool
	}{
		{TK_EQ, true},
		{TK_NE, true},
		{TK_LT, true},
		{TK_LE, true},
		{TK_GT, true},
		{TK_GE, true},
		{TK_PLUS, true},
		{TK_MINUS, true},
		{TK_STAR, true},
		{TK_SLASH, true},
		{TK_REM, true},
		{TK_BITAND, true},
		{TK_BITOR, true},
		{TK_BITNOT, true},
		{TK_LSHIFT, true},
		{TK_RSHIFT, true},
		{TK_CONCAT, true},
		{TK_ISNULL, true},
		{TK_NOTNULL, true},
		{TK_SELECT, false},
		{TK_INTEGER, false},
		{TK_LP, false},
		{TK_EOF, false},
	}

	for _, tt := range tests {
		tt := tt
		got := tt.tokType.IsOperator()
		if got != tt.want {
			t.Errorf("TokenType(%s).IsOperator() = %v, want %v", tt.tokType, got, tt.want)
		}
	}
}

func TestTokenTypeIsLiteral(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tokType TokenType
		want    bool
	}{
		{TK_INTEGER, true},
		{TK_FLOAT, true},
		{TK_STRING, true},
		{TK_BLOB, true},
		{TK_NULL, true},
		{TK_SELECT, false},
		{TK_EQ, false},
		{TK_LP, false},
		{TK_ID, false},
		{TK_EOF, false},
	}

	for _, tt := range tests {
		tt := tt
		got := tt.tokType.IsLiteral()
		if got != tt.want {
			t.Errorf("TokenType(%s).IsLiteral() = %v, want %v", tt.tokType, got, tt.want)
		}
	}
}

func TestTokenTypeIsPunctuation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tokType TokenType
		want    bool
	}{
		{TK_LP, true},
		{TK_RP, true},
		{TK_COMMA, true},
		{TK_SEMI, true},
		{TK_DOT, true},
		{TK_SELECT, false},
		{TK_INTEGER, false},
		{TK_EQ, false},
		{TK_EOF, false},
	}

	for _, tt := range tests {
		tt := tt
		got := tt.tokType.IsPunctuation()
		if got != tt.want {
			t.Errorf("TokenType(%s).IsPunctuation() = %v, want %v", tt.tokType, got, tt.want)
		}
	}
}
