/*
Package functions implements SQLite's built-in SQL functions in pure Go.

This package is a pure Go implementation based on SQLite source code.
SQLite is in the public domain: https://sqlite.org/copyright.html

# Overview

This package provides a comprehensive implementation of SQLite's standard library
functions, including string manipulation, mathematical operations, aggregation,
and date/time handling. All functions are implemented based on the SQLite 3.51.2
reference implementation.

# Function Categories

The package includes 75+ functions organized into categories:

  - String Functions (21): length, substr, upper, lower, trim, replace, etc.
  - Type Functions (5): typeof, coalesce, ifnull, nullif, iif
  - Math Functions (30): abs, round, sqrt, power, trigonometry, etc.
  - Aggregate Functions (8): count, sum, avg, min, max, group_concat
  - Date/Time Functions (10): date, time, datetime, julianday, strftime
  - Blob Functions (1): zeroblob

# Quick Start

	// Create a registry with all standard functions
	registry := functions.DefaultRegistry()

	// Look up and call a scalar function
	upperFunc, _ := registry.Lookup("upper")
	result, _ := upperFunc.Call([]functions.Value{
	    functions.NewTextValue("hello world"),
	})
	fmt.Println(result.AsString())  // Output: HELLO WORLD

	// Use an aggregate function
	sumFunc := &functions.SumFunc{}
	for _, value := range values {
	    sumFunc.Step([]functions.Value{value})
	}
	result, _ := sumFunc.Final()
	fmt.Println(result.AsInt64())

# Architecture

The package is built around three core interfaces:

Value Interface - Represents SQL values with type information:

	type Value interface {
	    Type() ValueType
	    AsInt64() int64
	    AsFloat64() float64
	    AsString() string
	    AsBlob() []byte
	    IsNull() bool
	    Bytes() int
	}

Function Interface - Base interface for all SQL functions:

	type Function interface {
	    Name() string
	    NumArgs() int  // -1 for variadic
	    Call(args []Value) (Value, error)
	}

AggregateFunction Interface - Extended interface for aggregate functions:

	type AggregateFunction interface {
	    Function
	    Step(args []Value) error
	    Final() (Value, error)
	    Reset()
	}

# String Functions

String functions are fully UTF-8 aware and handle Unicode correctly:

	length("hello")              // 5
	length("世界")               // 2 (characters, not bytes)
	substr("hello", 2, 3)        // "ell"
	upper("hello")               // "HELLO"
	replace("hello", "l", "L")   // "heLLo"
	hex("ABC")                   // "414243"
	quote("it's")                // "'it''s'"

# Math Functions

Comprehensive mathematical operations:

	abs(-42)                     // 42
	round(3.14159, 2)            // 3.14
	sqrt(16)                     // 4.0
	power(2, 10)                 // 1024.0
	sin(pi()/2)                  // 1.0
	random()                     // random int64

# Aggregate Functions

Statistical and grouping operations:

	count(*)                     // count all rows
	count(column)                // count non-NULL values
	sum(amount)                  // sum (NULL if empty)
	total(amount)                // sum (0.0 if empty)
	avg(score)                   // average
	min(value), max(value)       // extremes
	group_concat(name, ', ')     // concatenate with separator

# Date/Time Functions

Full date and time manipulation based on Julian day numbers:

	date('now')                           // "2024-01-15"
	time('now')                           // "12:34:56"
	datetime('now')                       // "2024-01-15 12:34:56"
	julianday('2000-01-01')              // 2451544.5
	unixepoch('now')                     // 1705323296
	strftime('%Y-%m-%d', 'now')          // "2024-01-15"

Date modifiers:

	date('now', '+1 day')                // tomorrow
	date('now', '-1 month')              // last month
	date('2024-01-15', 'start of month') // "2024-01-01"
	datetime('now', 'start of day')      // today at 00:00:00

# Type System

The package implements SQLite's type system with five types:

  - TypeNull: SQL NULL value
  - TypeInteger: 64-bit signed integer
  - TypeFloat: 64-bit floating point
  - TypeText: UTF-8 string
  - TypeBlob: Byte array

Type affinity ordering: NULL < INTEGER < FLOAT < TEXT < BLOB

# NULL Handling

Most functions follow these rules:

  - f(NULL) returns NULL
  - Aggregates skip NULL values
  - Type functions may handle NULL specially

Exceptions:

	coalesce(NULL, NULL, 42)     // 42
	ifnull(NULL, "default")      // "default"
	typeof(NULL)                 // "null"
	count(*)                     // counts NULL rows

# Custom Functions

Register custom functions easily:

	registry := functions.NewRegistry()

	doubleFunc := functions.NewScalarFunc("double", 1,
	    func(args []functions.Value) (functions.Value, error) {
	        if args[0].IsNull() {
	            return functions.NewNullValue(), nil
	        }
	        return functions.NewIntValue(args[0].AsInt64() * 2), nil
	    })

	registry.Register(doubleFunc)

# Performance

The implementation is optimized for:

  - UTF-8 string operations with minimal allocations
  - Efficient aggregate state management
  - Lazy date/time computation
  - Zero-copy value passing where possible

# SQLite Compatibility

This implementation aims for full compatibility with SQLite 3.51.2:

  - All core scalar functions
  - All aggregate functions
  - Core date/time functions
  - SQLite type system and semantics

Not included:

  - Extension functions (FTS, JSON, R*Tree)
  - Window functions
  - Custom collations
  - Compiled regular expressions

# Error Handling

Functions return errors for:

  - Invalid argument counts
  - Type conversion failures
  - Domain errors (sqrt of negative)
  - Overflow conditions

Functions return NULL (not error) for:

  - NULL input
  - Invalid format strings
  - Parse failures

# Examples

See examples_test.go for comprehensive usage examples:

	go test -v -run Example

# Testing

Run the full test suite:

	go test -v ./core/sqlite/internal/functions/
	go test -cover ./core/sqlite/internal/functions/

# Documentation

  - README.md: User documentation and function reference
  - IMPLEMENTATION.md: Implementation details and design decisions
  - QUICK_REFERENCE.md: Quick lookup guide for all functions
  - examples_test.go: Runnable examples

# Thread Safety

Functions are not thread-safe:

  - Each function call is independent and safe
  - Aggregate functions maintain mutable state
  - Use separate instances per goroutine
  - Registry reads are safe, writes require synchronization

# Dependencies

The package uses only Go standard library:

  - strings, bytes: string operations
  - math: mathematical functions
  - time: date/time operations
  - crypto/rand: secure random generation

No external dependencies required.

# Version

Based on SQLite 3.51.2 reference implementation.

# References

  - SQLite Core Functions: https://sqlite.org/lang_corefunc.html
  - SQLite Date Functions: https://sqlite.org/lang_datefunc.html
  - SQLite Aggregate Functions: https://sqlite.org/lang_aggfunc.html
*/
package functions
