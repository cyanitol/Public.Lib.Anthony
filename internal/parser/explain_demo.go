// +build ignore

package main

import (
	"fmt"
	parser "github.com/yourusername/anthony/internal/parser"
)

func main() {
	testCases := []string{
		"EXPLAIN SELECT * FROM users",
		"EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18",
		"EXPLAIN INSERT INTO users VALUES (1, 'Alice')",
		"EXPLAIN QUERY PLAN UPDATE users SET name = 'Bob'",
		"EXPLAIN DELETE FROM users WHERE id = 1",
		"EXPLAIN QUERY PLAN CREATE INDEX idx_name ON users(name)",
	}

	for i, sql := range testCases {
		fmt.Printf("\n=== Test Case %d ===\n", i+1)
		fmt.Printf("SQL: %s\n", sql)

		p := parser.NewParser(sql)
		stmts, err := p.Parse()
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}

		if len(stmts) != 1 {
			fmt.Printf("ERROR: expected 1 statement, got %d\n", len(stmts))
			continue
		}

		stmt := stmts[0]
		explainStmt, ok := stmt.(*parser.ExplainStmt)
		if !ok {
			fmt.Printf("ERROR: expected ExplainStmt, got %T\n", stmt)
			continue
		}

		fmt.Printf("Result: %s\n", explainStmt.String())
		fmt.Printf("  QueryPlan: %v\n", explainStmt.QueryPlan)
		fmt.Printf("  Inner Statement: %s\n", explainStmt.Statement.String())
	}
}
