// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package constraint implements constraint checking for SQLite.
// This includes CHECK constraints, foreign keys, and other data integrity rules.
package constraint

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
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
// Returns an error if any CHECK constraint expression fails to parse.
func NewCheckValidator(table *schema.Table) (*CheckValidator, error) {
	constraints, err := extractCheckConstraints(table)
	if err != nil {
		return nil, err
	}
	return &CheckValidator{
		constraints: constraints,
		table:       table,
	}, nil
}

// extractCheckConstraints extracts all CHECK constraints from a table schema.
func extractCheckConstraints(table *schema.Table) ([]*CheckConstraint, error) {
	var constraints []*CheckConstraint

	// Extract column-level CHECK constraints
	for _, col := range table.Columns {
		if col.Check == "" {
			continue
		}
		constraint, err := parseCheckConstraint(col.Check, "", col.Name, false)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, constraint)
	}

	// Extract table-level CHECK constraints
	for _, tc := range table.Constraints {
		if tc.Type != schema.ConstraintCheck || tc.Expression == "" {
			continue
		}
		constraint, err := parseCheckConstraint(tc.Expression, tc.Name, "", true)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

// parseCheckConstraint parses a CHECK expression and creates a constraint.
// Returns an error if parsing fails rather than silently skipping.
func parseCheckConstraint(exprStr, name, colName string, isTableLevel bool) (*CheckConstraint, error) {
	p := parser.NewParser(exprStr)
	expr, err := p.ParseExpression()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CHECK constraint %q: %w", exprStr, err)
	}

	return &CheckConstraint{
		Name:         name,
		Expression:   expr,
		ExprString:   exprStr,
		IsTableLevel: isTableLevel,
		ColumnName:   colName,
	}, nil
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
		errorMsg := FormatErrorMessage(constraint)
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

// formatErrorMessage is a method alias for FormatErrorMessage, kept for internal callers.
func (cv *CheckValidator) formatErrorMessage(constraint *CheckConstraint) string {
	return FormatErrorMessage(constraint)
}

// FormatErrorMessage creates a user-friendly error message for constraint violations.
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
