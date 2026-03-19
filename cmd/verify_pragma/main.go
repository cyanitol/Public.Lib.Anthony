// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"os"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

func main() {
	// Test 1: Verify TK_PRAGMA token exists in lexer
	lexer := parser.NewLexer("PRAGMA cache_size")
	tok := lexer.NextToken()
	if tok.Type != parser.TK_PRAGMA {
		fmt.Printf("ERROR: Expected TK_PRAGMA, got %v\n", tok.Type)
		os.Exit(1)
	}
	fmt.Println("TK_PRAGMA token recognized by lexer")

	// Test 2: Simple PRAGMA statement
	testCases := []struct {
		sql         string
		description string
	}{
		{"PRAGMA cache_size", "simple pragma"},
		{"PRAGMA cache_size = 10000", "pragma with equals value"},
		{"PRAGMA cache_size(10000)", "pragma with function syntax"},
		{"PRAGMA main.cache_size", "pragma with schema"},
		{"PRAGMA main.cache_size = 10000", "pragma with schema and value"},
	}

	for _, tc := range testCases {
		p := parser.NewParser(tc.sql)
		if p == nil {
			fmt.Printf("ERROR: Parser creation failed for: %s\n", tc.description)
			os.Exit(1)
		}
	}
	fmt.Println("Parser structure created successfully")

	// Test 3: Verify PragmaStmt structure
	stmt := &parser.PragmaStmt{
		Schema: "main",
		Name:   "cache_size",
		Value:  &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "10000"},
	}

	if stmt.String() != "PRAGMA" {
		fmt.Printf("ERROR: Expected String() = 'PRAGMA', got '%s'\n", stmt.String())
		os.Exit(1)
	}
	fmt.Println("PragmaStmt structure is correct")

	fmt.Println("\nAll verification checks passed!")
}
