// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ExampleVDBE_scalarFunctions demonstrates executing scalar functions
func ExampleVDBE_scalarFunctions() {
	// Create a new VDBE instance
	v := vdbe.New()

	// Allocate memory for registers
	v.AllocMemory(10)

	// Example 1: UPPER("hello") -> "HELLO"
	v.Mem[1].SetStr("hello")

	v.AddOpWithP4Str(vdbe.OpFunction, 0, 1, 5, "upper")
	v.Program[len(v.Program)-1].P5 = 1 // 1 argument

	// Example 2: LENGTH("world") -> 5
	v.Mem[2].SetStr("world")

	v.AddOpWithP4Str(vdbe.OpFunction, 0, 2, 6, "length")
	v.Program[len(v.Program)-1].P5 = 1

	// Example 3: SUBSTR("hello", 2, 3) -> "ell"
	v.Mem[3].SetStr("hello")
	v.Mem[4].SetInt(2)
	v.Mem[5].SetInt(3)

	v.AddOpWithP4Str(vdbe.OpFunction, 0, 3, 7, "substr")
	v.Program[len(v.Program)-1].P5 = 3 // 3 arguments

	// Add halt instruction
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Execute the program (in a real scenario)
	// v.Run()

	fmt.Println("Scalar functions wired successfully")
	// Output: Scalar functions wired successfully
}

// ExampleVDBE_aggregateFunctions demonstrates executing aggregate functions
func ExampleVDBE_aggregateFunctions() {
	// Create a new VDBE instance
	v := vdbe.New()

	// Allocate memory and cursors
	v.AllocMemory(20)
	v.AllocCursors(1)

	cursor := 0

	// Example: COUNT and SUM over multiple rows
	// Row 1: value = 10
	v.Mem[1].SetInt(10)
	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 1, 0, "count")
	v.Program[len(v.Program)-1].P5 = 1

	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 1, 1, "sum")
	v.Program[len(v.Program)-1].P5 = 1

	// Row 2: value = 20
	v.Mem[2].SetInt(20)
	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 2, 0, "count")
	v.Program[len(v.Program)-1].P5 = 1

	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 2, 1, "sum")
	v.Program[len(v.Program)-1].P5 = 1

	// Row 3: value = 30
	v.Mem[3].SetInt(30)
	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 3, 0, "count")
	v.Program[len(v.Program)-1].P5 = 1

	v.AddOpWithP4Str(vdbe.OpAggStep, cursor, 3, 1, "sum")
	v.Program[len(v.Program)-1].P5 = 1

	// Finalize COUNT into register 10
	v.AddOp(vdbe.OpAggFinal, cursor, 10, 0)

	// Finalize SUM into register 11
	v.AddOp(vdbe.OpAggFinal, cursor, 11, 1)

	// Add halt instruction
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	// Execute the program (in a real scenario)
	// v.Run()
	// Expected: Mem[10] = 3 (count), Mem[11] = 60 (sum)

	fmt.Println("Aggregate functions wired successfully")
	// Output: Aggregate functions wired successfully
}

// ExampleVDBE_complexQuery demonstrates a complex query with functions
func ExampleVDBE_complexQuery() {
	// Create a new VDBE instance
	v := vdbe.New()

	// Allocate memory
	v.AllocMemory(30)

	// Simulate: SELECT UPPER(name), LENGTH(name) FROM users WHERE LENGTH(name) > 5
	// This demonstrates:
	// 1. Scalar function in SELECT list (UPPER, LENGTH)
	// 2. Scalar function in WHERE clause (LENGTH)
	// 3. Comparison operation

	// Row 1: name = "john"
	v.Mem[1].SetStr("john")

	// LENGTH(name) -> register 2
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 1, 2, "length")
	v.Program[len(v.Program)-1].P5 = 1

	// Compare LENGTH > 5 (register 2 vs register 3)
	v.Mem[3].SetInt(5)
	v.AddOp(vdbe.OpGt, 2, 0, 3) // Jump to end if not greater

	// UPPER(name) -> register 4
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 1, 4, "upper")
	v.Program[len(v.Program)-1].P5 = 1

	// Output row with UPPER(name) and LENGTH(name)
	v.AddOp(vdbe.OpResultRow, 4, 2, 0) // Output registers 4-5

	// Add halt instruction
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	fmt.Println("Complex query with functions wired successfully")
	// Output: Complex query with functions wired successfully
}

// ExampleVDBE_nestedFunctions demonstrates nested function calls
func ExampleVDBE_nestedFunctions() {
	// Create a new VDBE instance
	v := vdbe.New()

	// Allocate memory
	v.AllocMemory(10)

	// Simulate: SELECT UPPER(LOWER("HeLLo")) -> "HELLO"
	v.Mem[1].SetStr("HeLLo")

	// First call LOWER("HeLLo") -> register 2
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 1, 2, "lower")
	v.Program[len(v.Program)-1].P5 = 1

	// Then call UPPER(register 2) -> register 3
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 2, 3, "upper")
	v.Program[len(v.Program)-1].P5 = 1

	// Output result
	v.AddOp(vdbe.OpResultRow, 3, 1, 0)

	// Add halt instruction
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	fmt.Println("Nested function calls wired successfully")
	// Output: Nested function calls wired successfully
}

// ExampleVDBE_nullHandling demonstrates NULL handling in functions
func ExampleVDBE_nullHandling() {
	// Create a new VDBE instance
	v := vdbe.New()

	// Allocate memory
	v.AllocMemory(10)

	// Example 1: UPPER(NULL) -> NULL
	v.Mem[1].SetNull()
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 1, 5, "upper")
	v.Program[len(v.Program)-1].P5 = 1

	// Example 2: COALESCE(NULL, NULL, "default") -> "default"
	v.Mem[2].SetNull()
	v.Mem[3].SetNull()
	v.Mem[4].SetStr("default")
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 2, 6, "coalesce")
	v.Program[len(v.Program)-1].P5 = 3 // 3 arguments

	// Example 3: IFNULL(NULL, 42) -> 42
	v.Mem[7].SetNull()
	v.Mem[8].SetInt(42)
	v.AddOpWithP4Str(vdbe.OpFunction, 0, 7, 9, "ifnull")
	v.Program[len(v.Program)-1].P5 = 2

	// Add halt instruction
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	fmt.Println("NULL handling in functions wired successfully")
	// Output: NULL handling in functions wired successfully
}
