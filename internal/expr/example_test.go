// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr_test

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/expr"
)

// Example demonstrates creating and evaluating a simple arithmetic expression
func Example_arithmetic() {
	// Create expression: (10 + 20) * 2
	e := expr.NewBinaryExpr(expr.OpMultiply,
		expr.NewBinaryExpr(expr.OpPlus,
			expr.NewIntExpr(10),
			expr.NewIntExpr(20)),
		expr.NewIntExpr(2))

	fmt.Println("Expression:", e.String())
	fmt.Println("Height:", e.Height)
	fmt.Println("Is Constant:", e.IsConstant())

	// Evaluate the expression
	left := expr.EvaluateArithmetic(expr.OpPlus, int64(10), int64(20))
	result := expr.EvaluateArithmetic(expr.OpMultiply, left, int64(2))
	fmt.Println("Result:", result)

	// Output:
	// Expression: ((10 + 20) * 2)
	// Height: 3
	// Is Constant: true
	// Result: 60
}

// Example demonstrates SQLite type affinity
func Example_affinity() {
	// Column with INTEGER affinity
	col := &expr.Expr{
		Op:       expr.OpColumn,
		Affinity: expr.AFF_INTEGER,
	}

	// Apply affinity to string value
	value := expr.ApplyAffinity("42", expr.GetExprAffinity(col))
	fmt.Printf("Type: %T, Value: %v\n", value, value)

	// String with TEXT affinity stays as string
	textValue := expr.ApplyAffinity(123, expr.AFF_TEXT)
	fmt.Printf("Type: %T, Value: %v\n", textValue, textValue)

	// Output:
	// Type: int64, Value: 42
	// Type: string, Value: 123
}

// Example demonstrates comparison operations
func Example_comparison() {
	// Compare integers
	result1 := expr.CompareValues(int64(10), int64(20), expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("10 vs 20:", result1) // CmpLess

	// Compare strings (case-insensitive)
	result2 := expr.CompareValues("Hello", "hello", expr.AFF_TEXT, expr.CollSeqNoCase)
	fmt.Println("Hello vs hello (NOCASE):", result2) // CmpEqual

	// NULL handling
	isNull := expr.EvaluateComparison(expr.OpIs, nil, nil, expr.AFF_NONE, nil)
	fmt.Println("NULL IS NULL:", isNull) // true

	equals := expr.EvaluateComparison(expr.OpEq, nil, int64(42), expr.AFF_INTEGER, nil)
	fmt.Println("NULL = 42:", equals) // nil (NULL propagates)

	// Output:
	// 10 vs 20: -1
	// Hello vs hello (NOCASE): 0
	// NULL IS NULL: true
	// NULL = 42: <nil>
}

// Example demonstrates LIKE pattern matching
func Example_like() {
	// Basic wildcards
	fmt.Println(expr.EvaluateLike("h%", "hello", 0))           // true
	fmt.Println(expr.EvaluateLike("h_llo", "hello", 0))        // true
	fmt.Println(expr.EvaluateLike("%world", "hello world", 0)) // true

	// Case insensitive
	fmt.Println(expr.EvaluateLike("HELLO", "hello", 0)) // true

	// Escaped wildcard
	fmt.Println(expr.EvaluateLike("100\\%", "100%", '\\')) // true

	// Output:
	// true
	// true
	// true
	// true
	// true
}

// Example demonstrates BETWEEN operation
func Example_between() {
	// 15 BETWEEN 10 AND 20
	result1 := expr.EvaluateBetween(int64(15), int64(10), int64(20),
		expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("15 BETWEEN 10 AND 20:", result1)

	// 5 BETWEEN 10 AND 20
	result2 := expr.EvaluateBetween(int64(5), int64(10), int64(20),
		expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("5 BETWEEN 10 AND 20:", result2)

	// Output:
	// 15 BETWEEN 10 AND 20: true
	// 5 BETWEEN 10 AND 20: false
}

// Example demonstrates IN operation
func Example_in() {
	// 2 IN (1, 2, 3)
	list := []interface{}{int64(1), int64(2), int64(3)}
	result1 := expr.EvaluateIn(int64(2), list, expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("2 IN (1,2,3):", result1)

	// 5 IN (1, 2, 3)
	result2 := expr.EvaluateIn(int64(5), list, expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("5 IN (1,2,3):", result2)

	// 5 IN (1, NULL, 3) - returns NULL (unknown)
	listWithNull := []interface{}{int64(1), nil, int64(3)}
	result3 := expr.EvaluateIn(int64(5), listWithNull, expr.AFF_INTEGER, expr.CollSeqBinary)
	fmt.Println("5 IN (1,NULL,3):", result3)

	// Output:
	// 2 IN (1,2,3): true
	// 5 IN (1,2,3): false
	// 5 IN (1,NULL,3): <nil>
}

// Example demonstrates three-valued logic
func Example_threeValuedLogic() {
	// true AND true = true
	fmt.Println("true AND true:",
		expr.EvaluateLogical(expr.OpAnd, int64(1), int64(1)))

	// true AND false = false
	fmt.Println("true AND false:",
		expr.EvaluateLogical(expr.OpAnd, int64(1), int64(0)))

	// true AND NULL = NULL
	fmt.Println("true AND NULL:",
		expr.EvaluateLogical(expr.OpAnd, int64(1), nil))

	// false AND NULL = false
	fmt.Println("false AND NULL:",
		expr.EvaluateLogical(expr.OpAnd, int64(0), nil))

	// true OR NULL = true
	fmt.Println("true OR NULL:",
		expr.EvaluateLogical(expr.OpOr, int64(1), nil))

	// false OR NULL = NULL
	fmt.Println("false OR NULL:",
		expr.EvaluateLogical(expr.OpOr, int64(0), nil))

	// Output:
	// true AND true: true
	// true AND false: false
	// true AND NULL: <nil>
	// false AND NULL: false
	// true OR NULL: true
	// false OR NULL: <nil>
}

// Example demonstrates CAST operations
func Example_cast() {
	// String to INTEGER
	result1 := expr.EvaluateCast("42", "INTEGER")
	fmt.Printf("CAST('42' AS INTEGER): %T = %v\n", result1, result1)

	// Float to INTEGER
	result2 := expr.EvaluateCast(3.14, "INTEGER")
	fmt.Printf("CAST(3.14 AS INTEGER): %T = %v\n", result2, result2)

	// Integer to TEXT
	result3 := expr.EvaluateCast(int64(123), "TEXT")
	fmt.Printf("CAST(123 AS TEXT): %T = %v\n", result3, result3)

	// String to REAL
	result4 := expr.EvaluateCast("3.14", "REAL")
	fmt.Printf("CAST('3.14' AS REAL): %T = %v\n", result4, result4)

	// Output:
	// CAST('42' AS INTEGER): int64 = 42
	// CAST(3.14 AS INTEGER): int64 = 3
	// CAST(123 AS TEXT): string = 123
	// CAST('3.14' AS REAL): float64 = 3.14
}

// Example demonstrates complex expression building
func Example_complexExpression() {
	// Build: (age > 18 AND active = 1) OR admin = 1
	age := expr.NewColumnExpr("users", "age", 0, 0)
	active := expr.NewColumnExpr("users", "active", 0, 1)
	admin := expr.NewColumnExpr("users", "admin", 0, 2)

	condition := expr.NewBinaryExpr(expr.OpOr,
		expr.NewBinaryExpr(expr.OpAnd,
			expr.NewBinaryExpr(expr.OpGt, age, expr.NewIntExpr(18)),
			expr.NewBinaryExpr(expr.OpEq, active, expr.NewIntExpr(1))),
		expr.NewBinaryExpr(expr.OpEq, admin, expr.NewIntExpr(1)))

	fmt.Println("Expression:", condition.String())
	fmt.Println("Height:", condition.Height)
	fmt.Println("Is Constant:", condition.IsConstant())

	// Output:
	// Expression: (((users.age > 18) AND (users.active = 1)) OR (users.admin = 1))
	// Height: 4
	// Is Constant: false
}

// Example demonstrates expression cloning
func Example_clone() {
	// Create original expression
	original := expr.NewBinaryExpr(expr.OpPlus,
		expr.NewIntExpr(10),
		expr.NewIntExpr(20))

	// Clone it
	cloned := original.Clone()

	// Modify clone
	cloned.Left = expr.NewIntExpr(100)

	fmt.Println("Original:", original.String())
	fmt.Println("Cloned:", cloned.String())

	// Output:
	// Original: (10 + 20)
	// Cloned: (100 + 20)
}
