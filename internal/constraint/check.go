// Package constraint implements constraint checking for SQLite.
// This includes CHECK constraints, foreign keys, and other data integrity rules.
package constraint

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// CheckConstraint represents a CHECK constraint with its expression and metadata.
type CheckConstraint struct {
	// Name is the optional constraint name (may be empty for unnamed constraints)
	Name string

	// Expression is the parsed CHECK expression from the schema
	Expression parser.Expression

	// ExprString is the string representation of the expression (for error messages)
	ExprString string

	// IsTableLevel indicates if this is a table-level constraint (vs column-level)
	IsTableLevel bool

	// ColumnName is set for column-level constraints
	ColumnName string
}

// CheckValidator validates CHECK constraints during INSERT and UPDATE operations.
type CheckValidator struct {
	constraints []*CheckConstraint
	table       *schema.Table
}

// NewCheckValidator creates a new CHECK constraint validator for a table.
func NewCheckValidator(table *schema.Table) *CheckValidator {
	return &CheckValidator{
		constraints: extractCheckConstraints(table),
		table:       table,
	}
}

// extractCheckConstraints extracts all CHECK constraints from a table schema.
func extractCheckConstraints(table *schema.Table) []*CheckConstraint {
	var constraints []*CheckConstraint

	// Extract column-level CHECK constraints
	for _, col := range table.Columns {
		if col.Check != "" {
			// Parse the CHECK expression
			p := parser.NewParser(col.Check)
			expr, err := p.ParseExpression()
			if err != nil {
				// If parsing fails, skip this constraint
				// In a production system, we might want to log this
				continue
			}

			constraints = append(constraints, &CheckConstraint{
				Name:         "", // Column-level constraints typically don't have explicit names
				Expression:   expr,
				ExprString:   col.Check,
				IsTableLevel: false,
				ColumnName:   col.Name,
			})
		}
	}

	// Extract table-level CHECK constraints
	for _, tc := range table.Constraints {
		if tc.Type == schema.ConstraintCheck && tc.Expression != "" {
			// Parse the CHECK expression
			p := parser.NewParser(tc.Expression)
			expr, err := p.ParseExpression()
			if err != nil {
				// If parsing fails, skip this constraint
				continue
			}

			constraints = append(constraints, &CheckConstraint{
				Name:         tc.Name,
				Expression:   expr,
				ExprString:   tc.Expression,
				IsTableLevel: true,
				ColumnName:   "",
			})
		}
	}

	return constraints
}

// CheckCodeGenerator is an interface that allows CHECK constraint validation
// code generation without directly depending on the vdbe package.
// This breaks the import cycle between constraint and vdbe packages.
type CheckCodeGenerator interface {
	// GenerateCheckConstraint generates code to validate a single CHECK constraint.
	// Returns an error if code generation fails.
	GenerateCheckConstraint(constraint *CheckConstraint, errorMsg string) error
}

// ValidateInsertWithGenerator validates all CHECK constraints for an INSERT operation.
// It uses the provided code generator to emit validation bytecode.
//
// Parameters:
//   - gen: A code generator that implements CheckCodeGenerator
//
// Returns error if code generation fails.
func (cv *CheckValidator) ValidateInsertWithGenerator(gen CheckCodeGenerator) error {
	if len(cv.constraints) == 0 {
		return nil
	}

	// Validate each CHECK constraint
	for _, constraint := range cv.constraints {
		errorMsg := cv.formatErrorMessage(constraint)
		if err := gen.GenerateCheckConstraint(constraint, errorMsg); err != nil {
			return err
		}
	}

	return nil
}

// ValidateUpdateWithGenerator validates all CHECK constraints for an UPDATE operation.
// This is called after the new values have been computed but before the update is applied.
//
// Parameters:
//   - gen: A code generator that implements CheckCodeGenerator
//
// Returns error if code generation fails.
func (cv *CheckValidator) ValidateUpdateWithGenerator(gen CheckCodeGenerator) error {
	// UPDATE validation is the same as INSERT validation - we check the new values
	return cv.ValidateInsertWithGenerator(gen)
}

// formatErrorMessage creates a user-friendly error message for constraint violations.
func (cv *CheckValidator) formatErrorMessage(constraint *CheckConstraint) string {
	if constraint.Name != "" {
		return fmt.Sprintf("CHECK constraint failed: %s (%s)", constraint.Name, constraint.ExprString)
	}

	if constraint.IsTableLevel {
		return fmt.Sprintf("CHECK constraint failed: %s", constraint.ExprString)
	}

	// Column-level constraint
	return fmt.Sprintf("CHECK constraint failed for column %s: %s", constraint.ColumnName, constraint.ExprString)
}

// FormatErrorMessage is a public version of formatErrorMessage for use by external code generators.
func FormatErrorMessage(constraint *CheckConstraint) string {
	if constraint.Name != "" {
		return fmt.Sprintf("CHECK constraint failed: %s (%s)", constraint.Name, constraint.ExprString)
	}

	if constraint.IsTableLevel {
		return fmt.Sprintf("CHECK constraint failed: %s", constraint.ExprString)
	}

	// Column-level constraint
	return fmt.Sprintf("CHECK constraint failed for column %s: %s", constraint.ColumnName, constraint.ExprString)
}

// HasCheckConstraints returns true if the table has any CHECK constraints.
func (cv *CheckValidator) HasCheckConstraints() bool {
	return len(cv.constraints) > 0
}

// GetConstraints returns all CHECK constraints for the table.
func (cv *CheckValidator) GetConstraints() []*CheckConstraint {
	return cv.constraints
}
