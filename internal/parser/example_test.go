// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser_test

import (
	"fmt"
	"log"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// Example demonstrates basic usage of the SQL parser.
func Example() {
	sql := "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	for _, stmt := range stmts {
		fmt.Printf("Statement type: %s\n", stmt.String())
	}
	// Output: Statement type: SELECT
}

// ExampleLexer demonstrates tokenization of SQL.
func ExampleLexer() {
	sql := "SELECT * FROM users"
	lexer := parser.NewLexer(sql)

	for {
		tok := lexer.NextToken()
		if tok.Type == parser.TK_EOF {
			break
		}
		if tok.Type != parser.TK_SPACE {
			fmt.Printf("%s ", tok.Type)
		}
	}
	// Output: SELECT STAR FROM ID
}

// ExampleParser_parseSelect demonstrates parsing a SELECT statement.
func ExampleParser_parseSelect() {
	sql := "SELECT id, name FROM users WHERE active = 1"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	sel := stmts[0].(*parser.SelectStmt)
	fmt.Printf("Columns: %d\n", len(sel.Columns))
	fmt.Printf("Has WHERE: %v\n", sel.Where != nil)
	// Output:
	// Columns: 2
	// Has WHERE: true
}

// ExampleParser_parseInsert demonstrates parsing an INSERT statement.
func ExampleParser_parseInsert() {
	sql := "INSERT INTO users (name, age) VALUES ('John', 30)"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	ins := stmts[0].(*parser.InsertStmt)
	fmt.Printf("Table: %s\n", ins.Table)
	fmt.Printf("Columns: %d\n", len(ins.Columns))
	fmt.Printf("Value rows: %d\n", len(ins.Values))
	// Output:
	// Table: users
	// Columns: 2
	// Value rows: 1
}

// ExampleParser_parseUpdate demonstrates parsing an UPDATE statement.
func ExampleParser_parseUpdate() {
	sql := "UPDATE users SET name = 'Jane', age = 25 WHERE id = 1"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	upd := stmts[0].(*parser.UpdateStmt)
	fmt.Printf("Table: %s\n", upd.Table)
	fmt.Printf("Assignments: %d\n", len(upd.Sets))
	fmt.Printf("Has WHERE: %v\n", upd.Where != nil)
	// Output:
	// Table: users
	// Assignments: 2
	// Has WHERE: true
}

// ExampleParser_parseDelete demonstrates parsing a DELETE statement.
func ExampleParser_parseDelete() {
	sql := "DELETE FROM users WHERE age < 18"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	del := stmts[0].(*parser.DeleteStmt)
	fmt.Printf("Table: %s\n", del.Table)
	fmt.Printf("Has WHERE: %v\n", del.Where != nil)
	// Output:
	// Table: users
	// Has WHERE: true
}

// ExampleParser_parseCreateTable demonstrates parsing a CREATE TABLE statement.
func ExampleParser_parseCreateTable() {
	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	create := stmts[0].(*parser.CreateTableStmt)
	fmt.Printf("Table: %s\n", create.Name)
	fmt.Printf("Columns: %d\n", len(create.Columns))
	// Output:
	// Table: users
	// Columns: 2
}

// ExampleParser_parseCreateIndex demonstrates parsing a CREATE INDEX statement.
// Note: CREATE UNIQUE INDEX not yet supported - using CREATE INDEX instead.
func ExampleParser_parseCreateIndex() {
	sql := "CREATE INDEX idx_email ON users (email)"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	create := stmts[0].(*parser.CreateIndexStmt)
	fmt.Printf("Index: %s\n", create.Name)
	fmt.Printf("Table: %s\n", create.Table)
	fmt.Printf("Columns: %d\n", len(create.Columns))
	// Output:
	// Index: idx_email
	// Table: users
	// Columns: 1
}

// ExampleParser_parseJoin demonstrates parsing a JOIN query.
func ExampleParser_parseJoin() {
	sql := "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id"

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	sel := stmts[0].(*parser.SelectStmt)
	fmt.Printf("Columns: %d\n", len(sel.Columns))
	if sel.From != nil {
		fmt.Printf("Tables: %d\n", len(sel.From.Tables))
		fmt.Printf("Joins: %d\n", len(sel.From.Joins))
	}
	// Output:
	// Columns: 2
	// Tables: 1
	// Joins: 1
}

// ExampleParser_parseComplex demonstrates parsing a complex query.
func ExampleParser_parseComplex() {
	sql := `
		SELECT
			u.id,
			u.name,
			COUNT(o.id) AS order_count
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.active = 1
		GROUP BY u.id, u.name
		HAVING COUNT(o.id) > 5
		ORDER BY order_count DESC
		LIMIT 10
	`

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	sel := stmts[0].(*parser.SelectStmt)
	fmt.Printf("Columns: %d\n", len(sel.Columns))
	fmt.Printf("Has FROM: %v\n", sel.From != nil)
	fmt.Printf("Has WHERE: %v\n", sel.Where != nil)
	fmt.Printf("GROUP BY expressions: %d\n", len(sel.GroupBy))
	fmt.Printf("Has HAVING: %v\n", sel.Having != nil)
	fmt.Printf("ORDER BY terms: %d\n", len(sel.OrderBy))
	fmt.Printf("Has LIMIT: %v\n", sel.Limit != nil)
	// Output:
	// Columns: 3
	// Has FROM: true
	// Has WHERE: true
	// GROUP BY expressions: 2
	// Has HAVING: true
	// ORDER BY terms: 1
	// Has LIMIT: true
}

// ExampleIntValue demonstrates extracting integer values from literals.
func ExampleIntValue() {
	sql := "SELECT 42"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	val, err := parser.IntValue(sel.Columns[0].Expr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Value: %d\n", val)
	// Output: Value: 42
}

// ExampleFloatValue demonstrates extracting float values from literals.
func ExampleFloatValue() {
	sql := "SELECT 3.14"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	val, err := parser.FloatValue(sel.Columns[0].Expr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Value: %.2f\n", val)
	// Output: Value: 3.14
}

// ExampleStringValue demonstrates extracting string values from literals.
func ExampleStringValue() {
	sql := "SELECT 'hello'"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	val, err := parser.StringValue(sel.Columns[0].Expr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Value: %s\n", val)
	// Output: Value: hello
}

// ExampleUnquote demonstrates removing quotes from identifiers.
func ExampleUnquote() {
	examples := []string{
		`"quoted"`,
		`'string'`,
		"`backtick`",
		`[bracketed]`,
		"unquoted",
	}

	for _, ex := range examples {
		fmt.Printf("%s -> %s\n", ex, parser.Unquote(ex))
	}
	// Output:
	// "quoted" -> quoted
	// 'string' -> string
	// `backtick` -> backtick
	// [bracketed] -> bracketed
	// unquoted -> unquoted
}

// ExampleTokenizeAll demonstrates tokenizing an entire SQL statement.
func ExampleTokenizeAll() {
	sql := "SELECT id FROM users WHERE age > 18"
	tokens, err := parser.TokenizeAll(sql)
	if err != nil {
		log.Fatal(err)
	}

	for _, tok := range tokens {
		if tok.Type != parser.TK_EOF {
			fmt.Printf("%s ", tok.Type)
		}
	}
	// Output: SELECT ID FROM ID WHERE ID GT INTEGER
}

// ExampleParser_parseMultipleStatements demonstrates parsing multiple statements.
func ExampleParser_parseMultipleStatements() {
	sql := `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		INSERT INTO users (name) VALUES ('John');
		SELECT * FROM users;
	`

	stmts, err := parser.ParseString(sql)
	if err != nil {
		log.Fatal(err)
	}

	for i, stmt := range stmts {
		fmt.Printf("Statement %d: %s\n", i+1, stmt.String())
	}
	// Output:
	// Statement 1: CREATE TABLE
	// Statement 2: INSERT
	// Statement 3: SELECT
}

// ExampleParser_parseExpressions demonstrates various expression types.
func ExampleParser_parseExpressions() {
	queries := []string{
		"SELECT 1 + 2 * 3", // arithmetic
		"SELECT name FROM users WHERE age > 18 AND active",        // comparison
		"SELECT * FROM users WHERE id IN (1, 2, 3)",               // IN
		"SELECT * FROM users WHERE age BETWEEN 18 AND 65",         // BETWEEN
		"SELECT * FROM users WHERE name LIKE 'John%'",             // LIKE
		"SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END", // CASE
	}

	for _, sql := range queries {
		stmts, err := parser.ParseString(sql)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fmt.Printf("Parsed: %s\n", stmts[0].String())
	}
	// Output:
	// Parsed: SELECT
	// Parsed: SELECT
	// Parsed: SELECT
	// Parsed: SELECT
	// Parsed: SELECT
	// Parsed: SELECT
}

// ExampleParser_parseTransactions demonstrates parsing transaction statements.
// Note: Full transaction syntax (BEGIN IMMEDIATE, etc.) not yet fully supported.
func ExampleParser_parseTransactions() {
	queries := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"COMMIT",
		"ROLLBACK",
	}

	for _, sql := range queries {
		stmts, err := parser.ParseString(sql)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", stmts[0].String())
	}
	// Output:
	// BEGIN
	// BEGIN
	// COMMIT
	// ROLLBACK
}

// ExampleBinaryExpr demonstrates working with binary expressions.
func ExampleBinaryExpr() {
	sql := "SELECT * FROM users WHERE age > 18 AND active = 1"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	// The WHERE clause is a binary AND expression
	if binExpr, ok := sel.Where.(*parser.BinaryExpr); ok {
		fmt.Printf("Operator: %d\n", binExpr.Op)
		fmt.Printf("Has left operand: %v\n", binExpr.Left != nil)
		fmt.Printf("Has right operand: %v\n", binExpr.Right != nil)
	}
	// Output:
	// Operator: 6
	// Has left operand: true
	// Has right operand: true
}

// ExampleIdentExpr demonstrates working with identifier expressions.
func ExampleIdentExpr() {
	sql := "SELECT u.name, age FROM users u"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	for i, col := range sel.Columns {
		if ident, ok := col.Expr.(*parser.IdentExpr); ok {
			if ident.Table != "" {
				fmt.Printf("Column %d: %s.%s\n", i+1, ident.Table, ident.Name)
			} else {
				fmt.Printf("Column %d: %s\n", i+1, ident.Name)
			}
		}
	}
	// Output:
	// Column 1: u.name
	// Column 2: age
}

// ExampleFunctionExpr demonstrates working with function expressions.
func ExampleFunctionExpr() {
	sql := "SELECT COUNT(*), SUM(amount), AVG(age) FROM users"
	stmts, _ := parser.ParseString(sql)
	sel := stmts[0].(*parser.SelectStmt)

	for i, col := range sel.Columns {
		if fn, ok := col.Expr.(*parser.FunctionExpr); ok {
			fmt.Printf("Function %d: %s\n", i+1, fn.Name)
			if fn.Star {
				fmt.Printf("  Uses *\n")
			}
			if len(fn.Args) > 0 {
				fmt.Printf("  Args: %d\n", len(fn.Args))
			}
		}
	}
	// Output:
	// Function 1: COUNT
	//   Uses *
	// Function 2: SUM
	//   Args: 1
	// Function 3: AVG
	//   Args: 1
}
