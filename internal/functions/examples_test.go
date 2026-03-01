// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions_test

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/functions"
)

// Example demonstrates basic usage of scalar functions
func Example_scalarFunctions() {
	registry := functions.DefaultRegistry()

	// String function: upper
	upperFunc, _ := registry.Lookup("upper")
	result, _ := upperFunc.Call([]functions.Value{
		functions.NewTextValue("hello world"),
	})
	fmt.Println(result.AsString())

	// String function: substr
	substrFunc, _ := registry.Lookup("substr")
	result, _ = substrFunc.Call([]functions.Value{
		functions.NewTextValue("hello"),
		functions.NewIntValue(2),
		functions.NewIntValue(3),
	})
	fmt.Println(result.AsString())

	// Math function: abs
	absFunc, _ := registry.Lookup("abs")
	result, _ = absFunc.Call([]functions.Value{
		functions.NewIntValue(-42),
	})
	fmt.Println(result.AsInt64())

	// Output:
	// HELLO WORLD
	// ell
	// 42
}

// Example demonstrates aggregate functions
func Example_aggregateFunctions() {
	// Create a sum aggregate
	sumFunc := &functions.SumFunc{}

	// Process multiple rows
	values := []int64{10, 20, 30, 40}
	for _, v := range values {
		sumFunc.Step([]functions.Value{
			functions.NewIntValue(v),
		})
	}

	// Get final result
	result, _ := sumFunc.Final()
	fmt.Println(result.AsInt64())

	// Output:
	// 100
}

// Example demonstrates date/time functions
func Example_dateFunctions() {
	registry := functions.DefaultRegistry()

	// Format current date
	dateFunc, _ := registry.Lookup("date")
	result, _ := dateFunc.Call([]functions.Value{
		functions.NewTextValue("2024-01-15"),
	})
	fmt.Println("Date:", result.AsString())

	// Julian day
	julianFunc, _ := registry.Lookup("julianday")
	result, _ = julianFunc.Call([]functions.Value{
		functions.NewTextValue("2000-01-01"),
	})
	fmt.Printf("Julian day: %.1f\n", result.AsFloat64())

	// Format with modifiers
	result, _ = dateFunc.Call([]functions.Value{
		functions.NewTextValue("2024-01-15"),
		functions.NewTextValue("+1 day"),
	})
	fmt.Println("Tomorrow:", result.AsString())

	// Output:
	// Date: 2024-01-15
	// Julian day: 2451545.0
	// Tomorrow: 2024-01-17
}

// Example demonstrates type functions
func Example_typeFunctions() {
	registry := functions.DefaultRegistry()

	// typeof
	typeofFunc, _ := registry.Lookup("typeof")

	values := []functions.Value{
		functions.NewIntValue(42),
		functions.NewFloatValue(3.14),
		functions.NewTextValue("hello"),
		functions.NewBlobValue([]byte{1, 2, 3}),
		functions.NewNullValue(),
	}

	for _, v := range values {
		result, _ := typeofFunc.Call([]functions.Value{v})
		fmt.Println(result.AsString())
	}

	// Output:
	// integer
	// real
	// text
	// blob
	// null
}

// Example demonstrates coalesce
func Example_coalesce() {
	registry := functions.DefaultRegistry()
	coalesceFunc, _ := registry.Lookup("coalesce")

	result, _ := coalesceFunc.Call([]functions.Value{
		functions.NewNullValue(),
		functions.NewNullValue(),
		functions.NewIntValue(42),
		functions.NewIntValue(100),
	})

	fmt.Println(result.AsInt64())

	// Output:
	// 42
}

// Example demonstrates string manipulation
func Example_stringManipulation() {
	registry := functions.DefaultRegistry()

	// Replace
	replaceFunc, _ := registry.Lookup("replace")
	result, _ := replaceFunc.Call([]functions.Value{
		functions.NewTextValue("hello world"),
		functions.NewTextValue("world"),
		functions.NewTextValue("Go"),
	})
	fmt.Println(result.AsString())

	// Instr
	instrFunc, _ := registry.Lookup("instr")
	result, _ = instrFunc.Call([]functions.Value{
		functions.NewTextValue("hello world"),
		functions.NewTextValue("world"),
	})
	fmt.Println(result.AsInt64())

	// Trim
	trimFunc, _ := registry.Lookup("trim")
	result, _ = trimFunc.Call([]functions.Value{
		functions.NewTextValue("  hello  "),
	})
	fmt.Println(result.AsString())

	// Output:
	// hello Go
	// 7
	// hello
}

// Example demonstrates hex encoding
func Example_hexFunctions() {
	registry := functions.DefaultRegistry()

	// Hex encode
	hexFunc, _ := registry.Lookup("hex")
	result, _ := hexFunc.Call([]functions.Value{
		functions.NewTextValue("Hello"),
	})
	fmt.Println(result.AsString())

	// Hex decode
	unhexFunc, _ := registry.Lookup("unhex")
	result, _ = unhexFunc.Call([]functions.Value{
		functions.NewTextValue("48656C6C6F"),
	})
	fmt.Println(string(result.AsBlob()))

	// Output:
	// 48656C6C6F
	// Hello
}

// Example demonstrates mathematical functions
func Example_mathFunctions() {
	registry := functions.DefaultRegistry()

	// Power
	powerFunc, _ := registry.Lookup("power")
	result, _ := powerFunc.Call([]functions.Value{
		functions.NewFloatValue(2),
		functions.NewFloatValue(10),
	})
	fmt.Printf("2^10 = %.0f\n", result.AsFloat64())

	// Sqrt
	sqrtFunc, _ := registry.Lookup("sqrt")
	result, _ = sqrtFunc.Call([]functions.Value{
		functions.NewFloatValue(16),
	})
	fmt.Printf("sqrt(16) = %.0f\n", result.AsFloat64())

	// Round
	roundFunc, _ := registry.Lookup("round")
	result, _ = roundFunc.Call([]functions.Value{
		functions.NewFloatValue(3.14159),
		functions.NewIntValue(2),
	})
	fmt.Printf("round(3.14159, 2) = %.2f\n", result.AsFloat64())

	// Output:
	// 2^10 = 1024
	// sqrt(16) = 4
	// round(3.14159, 2) = 3.14
}

// Example demonstrates custom function registration
func Example_customFunction() {
	registry := functions.NewRegistry()

	// Create a custom function that doubles a number
	doubleFunc := functions.NewScalarFunc("double", 1, func(args []functions.Value) (functions.Value, error) {
		if args[0].IsNull() {
			return functions.NewNullValue(), nil
		}

		switch args[0].Type() {
		case functions.TypeInteger:
			return functions.NewIntValue(args[0].AsInt64() * 2), nil
		case functions.TypeFloat:
			return functions.NewFloatValue(args[0].AsFloat64() * 2), nil
		default:
			return functions.NewFloatValue(args[0].AsFloat64() * 2), nil
		}
	})

	registry.Register(doubleFunc)

	// Use the custom function
	fn, _ := registry.Lookup("double")
	result, _ := fn.Call([]functions.Value{
		functions.NewIntValue(21),
	})
	fmt.Println(result.AsInt64())

	// Output:
	// 42
}

// Example demonstrates group_concat aggregate
func Example_groupConcat() {
	groupFunc := &functions.GroupConcatFunc{}

	// Add values
	names := []string{"Alice", "Bob", "Charlie", "Diana"}
	for _, name := range names {
		groupFunc.Step([]functions.Value{
			functions.NewTextValue(name),
		})
	}

	result, _ := groupFunc.Final()
	fmt.Println(result.AsString())

	// With custom separator
	groupFunc.Reset()
	for i, name := range names {
		if i == 0 {
			// First call with separator
			groupFunc.Step([]functions.Value{
				functions.NewTextValue(name),
				functions.NewTextValue(" | "),
			})
		} else {
			groupFunc.Step([]functions.Value{
				functions.NewTextValue(name),
			})
		}
	}

	result, _ = groupFunc.Final()
	fmt.Println(result.AsString())

	// Output:
	// Alice,Bob,Charlie,Diana
	// Alice | Bob | Charlie | Diana
}

// Example demonstrates quote function
func Example_quote() {
	registry := functions.DefaultRegistry()
	quoteFunc, _ := registry.Lookup("quote")

	values := []functions.Value{
		functions.NewIntValue(42),
		functions.NewFloatValue(3.14),
		functions.NewTextValue("hello"),
		functions.NewTextValue("it's quoted"),
		functions.NewBlobValue([]byte{0x12, 0x34}),
		functions.NewNullValue(),
	}

	for _, v := range values {
		result, _ := quoteFunc.Call([]functions.Value{v})
		fmt.Println(result.AsString())
	}

	// Output:
	// 42
	// 3.14
	// 'hello'
	// 'it''s quoted'
	// X'1234'
	// NULL
}

// Example demonstrates min/max as aggregate functions
func Example_minMaxScalarAndAggregate() {
	// Min and Max are aggregate functions
	// They find the minimum/maximum value across rows

	// Create a MinFunc aggregate
	minAgg := &functions.MinFunc{}
	values := []int64{30, 10, 20, 5, 15}
	for _, v := range values {
		minAgg.Step([]functions.Value{functions.NewIntValue(v)})
	}
	minResult, _ := minAgg.Final()
	fmt.Println("Aggregate min:", minResult.AsInt64())

	// Create a MaxFunc aggregate
	maxAgg := &functions.MaxFunc{}
	for _, v := range values {
		maxAgg.Step([]functions.Value{functions.NewIntValue(v)})
	}
	maxResult, _ := maxAgg.Final()
	fmt.Println("Aggregate max:", maxResult.AsInt64())

	// Output:
	// Aggregate min: 5
	// Aggregate max: 30
}

// Example demonstrates strftime formatting
func Example_strftime() {
	registry := functions.DefaultRegistry()
	strftimeFunc, _ := registry.Lookup("strftime")

	formats := []string{
		"%Y-%m-%d",
		"%H:%M:%S",
		"%Y-%m-%d %H:%M:%S",
	}

	dateValue := functions.NewTextValue("2024-01-15 14:30:45")

	for _, format := range formats {
		result, _ := strftimeFunc.Call([]functions.Value{
			functions.NewTextValue(format),
			dateValue,
		})
		fmt.Println(result.AsString())
	}

	// Output:
	// 2024-01-15
	// 14:30:45
	// 2024-01-15 14:30:45
}
