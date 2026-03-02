// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package constraint provides constraint validation and enforcement for SQLite databases.
//
// This package implements various SQL constraints including NOT NULL, UNIQUE, CHECK,
// foreign keys, and collation sequences.
//
// # NOT NULL Constraints
//
// The NOT NULL constraint ensures that a column cannot contain NULL values.
// The NotNullConstraint type validates INSERT and UPDATE operations.
//
// Example usage:
//
//	table := schema.GetTable("users")
//	nnc := constraint.NewNotNullConstraint(table)
//
//	// For INSERT operations
//	values := map[string]interface{}{
//	    "id": 1,
//	    "name": "John Doe",
//	}
//
//	// Apply defaults before validation
//	if err := nnc.ApplyDefaults(values, true); err != nil {
//	    return err
//	}
//
//	// Validate NOT NULL constraints
//	if err := nnc.ValidateInsert(values); err != nil {
//	    return err  // "NOT NULL constraint failed: column email"
//	}
//
// # Collation Sequences
//
// This package also implements collation sequences for string comparison operations.
// A collation sequence determines how strings are compared and sorted.
//
// # Built-in Collations
//
// The package provides three standard SQLite collations:
//
//   - BINARY: Byte-by-byte comparison (case-sensitive, default)
//   - NOCASE: Case-insensitive comparison for ASCII characters (A-Z = a-z)
//   - RTRIM: Ignores trailing spaces during comparison
//
// # Basic Usage
//
// Compare strings using a specific collation:
//
//	result := constraint.Compare("Hello", "hello", "NOCASE")
//	// result == 0 (equal, case-insensitive)
//
//	result = constraint.Compare("Hello", "hello", "BINARY")
//	// result < 0 (not equal, case-sensitive)
//
// # Custom Collations
//
// Register a custom collation function:
//
//	// Custom collation that sorts by string length
//	lengthCollation := func(a, b string) int {
//		if len(a) < len(b) {
//			return -1
//		} else if len(a) > len(b) {
//			return 1
//		}
//		return 0
//	}
//
//	err := constraint.RegisterCollation("LENGTH", lengthCollation)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Use the custom collation
//	result := constraint.Compare("cat", "elephant", "LENGTH")
//	// result < 0 (shorter string comes first)
//
// # Integration with Schema
//
// Columns can specify a collation using the COLLATE clause:
//
//	CREATE TABLE users (
//		name TEXT COLLATE NOCASE,
//		email TEXT COLLATE BINARY
//	);
//
// Retrieve collation from a column:
//
//	table, _ := schema.GetTable("users")
//	collation := table.GetColumnCollationByName("name")
//	// collation == "NOCASE"
//
// # Integration with VDBE
//
// Memory cells can be compared using collations:
//
//	mem1 := vdbe.NewMemStr("Hello")
//	mem2 := vdbe.NewMemStr("hello")
//
//	// Compare with default BINARY collation
//	result := mem1.Compare(mem2)
//	// result != 0
//
//	// Compare with NOCASE collation
//	result = mem1.CompareWithCollation(mem2, "NOCASE")
//	// result == 0
//
// # Thread Safety
//
// The global collation registry is thread-safe and can be accessed concurrently.
// Custom collation functions should also be thread-safe if they will be used
// in concurrent operations.
//
// # Performance
//
// Built-in collations are optimized for performance:
//
//   - BINARY: Direct string comparison (fastest)
//   - NOCASE: Single-pass byte comparison with case folding
//   - RTRIM: String trimming + binary comparison
//
// Custom collations should be implemented efficiently for best performance
// in sorting and comparison operations.
package constraint
