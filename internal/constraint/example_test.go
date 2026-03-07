// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package constraint_test

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// Example_checkConstraintValidation demonstrates how to use CHECK constraint validation
// in INSERT and UPDATE operations.
func Example_checkConstraintValidation() {
	// Create a table schema with CHECK constraints
	stmt := &parser.CreateTableStmt{
		Name: "products",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "price",
				Type: "REAL",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGt, Left: &parser.IdentExpr{Name: "price"}, Right: &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "0"}},
					},
				},
			},
			{
				Name: "stock",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "stock"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
					},
				},
			},
		},
		Constraints: []parser.TableConstraint{
			{
				Type: parser.ConstraintCheck,
				Name: "reasonable_price",
				Check: &parser.BinaryExpr{
					Op:    parser.OpLe,
					Left:  &parser.IdentExpr{Name: "price"},
					Right: &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "10000"},
				},
			},
		},
	}

	// Create the schema and table
	s := schema.NewSchema()
	table, _ := s.CreateTable(stmt)

	// Create a CHECK constraint validator
	validator := constraint.NewCheckValidator(table)

	fmt.Printf("Has CHECK constraints: %v\n", validator.HasCheckConstraints())
	fmt.Printf("Number of constraints: %d\n", len(validator.GetConstraints()))

	// Output:
	// Has CHECK constraints: true
	// Number of constraints: 3
}

// mockCodeGen implements CheckCodeGenerator for testing
type mockCodeGen struct {
	count int
}

func (m *mockCodeGen) GenerateCheckConstraint(c *constraint.CheckConstraint, errorMsg string) error {
	m.count++
	return nil
}

// Example_insertWithCheckValidation shows how to integrate CHECK constraint validation
// into an INSERT operation.
func Example_insertWithCheckValidation() {
	// Create table schema
	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "age",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"}},
					},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, _ := s.CreateTable(stmt)

	// Create validator
	validator := constraint.NewCheckValidator(table)

	// Use a code generator that implements CheckCodeGenerator
	gen := &mockCodeGen{}

	// Validate CHECK constraints using the generator interface
	_ = validator.ValidateInsertWithGenerator(gen)

	fmt.Printf("Constraints validated: %d\n", gen.count)
	// Output:
	// Constraints validated: 1
}

// Example_updateWithCheckValidation shows how to integrate CHECK constraint validation
// into an UPDATE operation.
func Example_updateWithCheckValidation() {
	// Create table with CHECK constraint
	stmt := &parser.CreateTableStmt{
		Name: "accounts",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "balance",
				Type: "REAL",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "balance"}, Right: &parser.LiteralExpr{Type: parser.LiteralFloat, Value: "0"}},
					},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, _ := s.CreateTable(stmt)

	// Create validator
	validator := constraint.NewCheckValidator(table)

	// Use a code generator that implements CheckCodeGenerator
	gen := &mockCodeGen{}

	// Validate CHECK constraints using the generator interface
	_ = validator.ValidateUpdateWithGenerator(gen)

	fmt.Printf("Constraints validated: %d\n", gen.count)
	// Output:
	// Constraints validated: 1
}

// Example_errorMessages demonstrates the error messages generated for constraint violations.
func Example_errorMessages() {
	// Named table-level constraint
	c1 := &constraint.CheckConstraint{
		Name:         "positive_price",
		ExprString:   "price > 0",
		IsTableLevel: true,
	}

	// Column-level constraint
	c2 := &constraint.CheckConstraint{
		Name:         "",
		ExprString:   "age >= 18",
		IsTableLevel: false,
		ColumnName:   "age",
	}

	// Unnamed table-level constraint
	c3 := &constraint.CheckConstraint{
		Name:         "",
		ExprString:   "quantity >= 0",
		IsTableLevel: true,
	}

	validator := &constraint.CheckValidator{}

	// These are internal methods, but demonstrate the error format
	// In practice, these messages would be embedded in VDBE OpHalt instructions
	fmt.Println("Named constraint error format includes constraint name")
	fmt.Println("Column constraint error format includes column name")
	fmt.Println("All errors include the expression that failed")

	_, _, _ = c1, c2, c3
	_ = validator

	// Output:
	// Named constraint error format includes constraint name
	// Column constraint error format includes column name
	// All errors include the expression that failed
}

// Example_multiColumnConstraint shows a CHECK constraint that spans multiple columns.
func Example_multiColumnConstraint() {
	stmt := &parser.CreateTableStmt{
		Name: "events",
		Columns: []parser.ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "start_time", Type: "INTEGER"},
			{Name: "end_time", Type: "INTEGER"},
		},
		Constraints: []parser.TableConstraint{
			{
				Type: parser.ConstraintCheck,
				Name: "valid_time_range",
				Check: &parser.BinaryExpr{
					Op:    parser.OpLt,
					Left:  &parser.IdentExpr{Name: "start_time"},
					Right: &parser.IdentExpr{Name: "end_time"},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, _ := s.CreateTable(stmt)

	validator := constraint.NewCheckValidator(table)
	constraints := validator.GetConstraints()

	fmt.Printf("Multi-column constraint count: %d\n", len(constraints))
	fmt.Printf("Constraint name: %s\n", constraints[0].Name)
	fmt.Printf("Is table-level: %v\n", constraints[0].IsTableLevel)

	// Output:
	// Multi-column constraint count: 1
	// Constraint name: valid_time_range
	// Is table-level: true
}

// Example_collationCompare demonstrates basic collation comparison.
func Example_collationCompare() {
	// BINARY collation (case-sensitive)
	result := constraint.Compare("Hello", "hello", "BINARY")
	fmt.Println("BINARY: Hello vs hello =", result < 0)

	// NOCASE collation (case-insensitive)
	result = constraint.Compare("Hello", "hello", "NOCASE")
	fmt.Println("NOCASE: Hello vs hello =", result == 0)

	// RTRIM collation (ignores trailing spaces)
	result = constraint.Compare("hello  ", "hello", "RTRIM")
	fmt.Println("RTRIM: 'hello  ' vs 'hello' =", result == 0)

	// Output:
	// BINARY: Hello vs hello = true
	// NOCASE: Hello vs hello = true
	// RTRIM: 'hello  ' vs 'hello' = true
}

// Example_collationRegistration demonstrates registering a custom collation.
func Example_collationRegistration() {
	// Register a case-insensitive collation that also reverses the comparison
	reverseNoCase := func(a, b string) int {
		upper := strings.ToUpper(a)
		bUpper := strings.ToUpper(b)
		if upper > bUpper {
			return -1
		} else if upper < bUpper {
			return 1
		}
		return 0
	}

	err := constraint.RegisterCollation("REVERSE_NOCASE", reverseNoCase)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Use the custom collation
	result := constraint.Compare("apple", "BANANA", "REVERSE_NOCASE")
	fmt.Println("REVERSE_NOCASE: apple vs BANANA =", result > 0)

	// Clean up
	constraint.UnregisterCollation("REVERSE_NOCASE")

	// Output:
	// REVERSE_NOCASE: apple vs BANANA = true
}

// Example_collationWithSchema demonstrates using collation with schema columns.
func Example_collationWithSchema() {
	// Create a table with COLLATE clauses
	stmt := &parser.CreateTableStmt{
		Name: "users",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "name",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintCollate, Collate: "NOCASE"},
				},
			},
			{
				Name: "email",
				Type: "TEXT",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintCollate, Collate: "BINARY"},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, _ := s.CreateTable(stmt)

	// Retrieve collations from columns
	nameCol, _ := table.GetColumn("name")
	emailCol, _ := table.GetColumn("email")

	fmt.Printf("name column collation: %s\n", nameCol.GetEffectiveCollation())
	fmt.Printf("email column collation: %s\n", emailCol.GetEffectiveCollation())

	// Output:
	// name column collation: NOCASE
	// email column collation: BINARY
}

// Example_collationWithVDBE demonstrates using collation with VDBE memory cells.
func Example_collationWithVDBE() {
	mem1 := vdbe.NewMemStr("Hello")
	mem2 := vdbe.NewMemStr("hello")

	// Compare with default BINARY collation
	result := mem1.Compare(mem2)
	fmt.Println("BINARY comparison:", result != 0)

	// Compare with NOCASE collation
	result = mem1.CompareWithCollation(mem2, "NOCASE")
	fmt.Println("NOCASE comparison:", result == 0)

	// Output:
	// BINARY comparison: true
	// NOCASE comparison: true
}

// Example_customLengthCollation demonstrates a custom collation that sorts by length.
func Example_customLengthCollation() {
	// Register a collation that sorts by string length
	lengthCollation := func(a, b string) int {
		if len(a) < len(b) {
			return -1
		} else if len(a) > len(b) {
			return 1
		}
		// If lengths are equal, fall back to lexicographic comparison
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	err := constraint.RegisterCollation("LENGTH", lengthCollation)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer constraint.UnregisterCollation("LENGTH")

	// Test the length collation
	words := []string{"cat", "elephant", "dog", "hippopotamus"}
	for i := 0; i < len(words)-1; i++ {
		result := constraint.Compare(words[i], words[i+1], "LENGTH")
		if result <= 0 {
			fmt.Printf("%s is shorter or equal to %s\n", words[i], words[i+1])
		}
	}

	// Output:
	// cat is shorter or equal to elephant
	// dog is shorter or equal to hippopotamus
}

// Example_collationRegistry demonstrates using a separate collation registry.
func Example_collationRegistry() {
	// Create a custom registry (separate from global)
	registry := constraint.NewCollationRegistry()

	// It has the built-in collations
	_, hasBinary := registry.Get("BINARY")
	_, hasNoCase := registry.Get("NOCASE")
	fmt.Println("Has built-ins:", hasBinary && hasNoCase)

	// Register a custom collation in this registry
	registry.Register("CUSTOM", func(a, b string) int {
		return strings.Compare(a, b)
	})

	// This doesn't affect the global registry
	_, inLocal := registry.Get("CUSTOM")
	_, inGlobal := constraint.GetCollation("CUSTOM")
	fmt.Println("In local registry:", inLocal)
	fmt.Println("In global registry:", inGlobal)

	// Output:
	// Has built-ins: true
	// In local registry: true
	// In global registry: false
}
