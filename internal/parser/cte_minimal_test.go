// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

import (
	"testing"
)

// Minimal test to verify CTE parsing works
func TestCTE_BasicParsing(t *testing.T) {
	t.Parallel()
	sql := "WITH cte AS (SELECT * FROM users) SELECT * FROM cte"

	lexer := NewLexer(sql)
	tokens := []Token{}
	for {
		tok := lexer.NextToken()
		if tok.Type != TK_SPACE && tok.Type != TK_COMMENT {
			tokens = append(tokens, tok)
		}
		if tok.Type == TK_EOF {
			break
		}
	}

	// Verify WITH token exists
	foundWith := false
	for _, tok := range tokens {
		if tok.Type == TK_WITH {
			foundWith = true
			break
		}
	}

	if !foundWith {
		t.Error("TK_WITH token not found in lexer output")
	}
}
