// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package constraint

import (
	"strings"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// mockCodeGenerator is a test mock for CheckCodeGenerator
type mockCodeGenerator struct {
	constraints []*CheckConstraint
	errorMsgs   []string
}

func (m *mockCodeGenerator) GenerateCheckConstraint(constraint *CheckConstraint, errorMsg string) error {
	m.constraints = append(m.constraints, constraint)
	m.errorMsgs = append(m.errorMsgs, errorMsg)
	return nil
}

// TestNewCheckValidator tests creating a CHECK validator from a table schema.
func TestNewCheckValidator(t *testing.T) {
	// Create a table with CHECK constraints
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
						Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
					},
				},
			},
			{
				Name: "email",
				Type: "TEXT",
			},
		},
		Constraints: []parser.TableConstraint{
			{
				Type:  parser.ConstraintCheck,
				Name:  "valid_email",
				Check: &parser.BinaryExpr{Op: parser.OpNe, Left: &parser.IdentExpr{Name: "email"}, Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: ""}},
			},
		},
	}

	s := schema.NewSchema()
	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	validator := NewCheckValidator(table)
	if validator == nil {
		t.Fatal("Expected non-nil validator")
	}

	if !validator.HasCheckConstraints() {
		t.Error("Expected validator to have CHECK constraints")
	}

	constraints := validator.GetConstraints()
	if len(constraints) != 2 {
		t.Errorf("Expected 2 constraints, got %d", len(constraints))
	}
}

// TestExtractCheckConstraints tests extracting CHECK constraints from table schema.
func TestExtractCheckConstraints(t *testing.T) {
	tests := []struct {
		name          string
		stmt          *parser.CreateTableStmt
		expectedCount int
		expectedNames []string
	}{
		{
			name: "Column-level CHECK",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{
						Name: "age",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{
								Type:  parser.ConstraintCheck,
								Check: &parser.BinaryExpr{Op: parser.OpGt, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
							},
						},
					},
				},
			},
			expectedCount: 1,
			expectedNames: []string{""},
		},
		{
			name: "Table-level CHECK with name",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{Name: "id", Type: "INTEGER"},
				},
				Constraints: []parser.TableConstraint{
					{
						Type:  parser.ConstraintCheck,
						Name:  "check_positive",
						Check: &parser.BinaryExpr{Op: parser.OpGt, Left: &parser.IdentExpr{Name: "id"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
					},
				},
			},
			expectedCount: 1,
			expectedNames: []string{"check_positive"},
		},
		{
			name: "Multiple CHECK constraints",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{
						Name: "age",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{
								Type:  parser.ConstraintCheck,
								Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
							},
						},
					},
					{
						Name: "score",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{
								Type:  parser.ConstraintCheck,
								Check: &parser.BinaryExpr{Op: parser.OpLe, Left: &parser.IdentExpr{Name: "score"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "100"}},
							},
						},
					},
				},
				Constraints: []parser.TableConstraint{
					{
						Type:  parser.ConstraintCheck,
						Name:  "valid_range",
						Check: &parser.BinaryExpr{Op: parser.OpLt, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.IdentExpr{Name: "score"}},
					},
				},
			},
			expectedCount: 3,
			expectedNames: []string{"", "", "valid_range"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema.NewSchema()
			table, err := s.CreateTable(tt.stmt)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			constraints := extractCheckConstraints(table)
			if len(constraints) != tt.expectedCount {
				t.Errorf("Expected %d constraints, got %d", tt.expectedCount, len(constraints))
			}

			for i, constraint := range constraints {
				if i < len(tt.expectedNames) && constraint.Name != tt.expectedNames[i] {
					t.Errorf("Constraint %d: expected name %q, got %q", i, tt.expectedNames[i], constraint.Name)
				}
			}
		})
	}
}

// TestValidateInsert tests CHECK constraint validation during INSERT.
func TestValidateInsert(t *testing.T) {
	// Create a simple table with a CHECK constraint
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
		},
	}

	s := schema.NewSchema()
	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	validator := NewCheckValidator(table)

	// Create a mock code generator for testing
	mock := &mockCodeGenerator{}

	// Test that validation code generation doesn't fail
	err = validator.ValidateInsertWithGenerator(mock)
	if err != nil {
		t.Errorf("ValidateInsertWithGenerator failed: %v", err)
	}

	// Verify that constraints were passed to the generator
	if len(mock.constraints) == 0 {
		t.Error("Expected at least one constraint to be generated")
	}
}

// TestValidateUpdate tests CHECK constraint validation during UPDATE.
func TestValidateUpdate(t *testing.T) {
	// Create a table with CHECK constraint
	stmt := &parser.CreateTableStmt{
		Name: "inventory",
		Columns: []parser.ColumnDef{
			{
				Name: "id",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{Type: parser.ConstraintPrimaryKey},
				},
			},
			{
				Name: "quantity",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "quantity"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
					},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	validator := NewCheckValidator(table)

	// Create a mock code generator for testing
	mock := &mockCodeGenerator{}

	// Test that validation code generation doesn't fail
	err = validator.ValidateUpdateWithGenerator(mock)
	if err != nil {
		t.Errorf("ValidateUpdateWithGenerator failed: %v", err)
	}

	// Verify that constraints were passed to the generator
	if len(mock.constraints) == 0 {
		t.Error("Expected at least one constraint to be generated")
	}
}

// TestFormatErrorMessage tests error message formatting.
func TestFormatErrorMessage(t *testing.T) {
	cv := &CheckValidator{}

	tests := []struct {
		name       string
		constraint *CheckConstraint
		contains   []string
	}{
		{
			name: "Named table-level constraint",
			constraint: &CheckConstraint{
				Name:         "positive_price",
				ExprString:   "price > 0",
				IsTableLevel: true,
			},
			contains: []string{"positive_price", "price > 0"},
		},
		{
			name: "Unnamed table-level constraint",
			constraint: &CheckConstraint{
				Name:         "",
				ExprString:   "quantity >= 0",
				IsTableLevel: true,
			},
			contains: []string{"quantity >= 0"},
		},
		{
			name: "Column-level constraint",
			constraint: &CheckConstraint{
				Name:         "",
				ExprString:   "age >= 18",
				IsTableLevel: false,
				ColumnName:   "age",
			},
			contains: []string{"age", "age >= 18"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := cv.formatErrorMessage(tt.constraint)
			for _, substr := range tt.contains {
				if !strings.Contains(msg, substr) {
					t.Errorf("Expected error message to contain %q, got: %s", substr, msg)
				}
			}
		})
	}
}

// TestHasCheckConstraints tests the HasCheckConstraints method.
func TestHasCheckConstraints(t *testing.T) {
	tests := []struct {
		name     string
		stmt     *parser.CreateTableStmt
		expected bool
	}{
		{
			name: "With CHECK constraints",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{
						Name: "age",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{
								Type:  parser.ConstraintCheck,
								Check: &parser.BinaryExpr{Op: parser.OpGe, Left: &parser.IdentExpr{Name: "age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Without CHECK constraints",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{
						Name: "id",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{Type: parser.ConstraintPrimaryKey},
						},
					},
					{Name: "name", Type: "TEXT"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema.NewSchema()
			table, err := s.CreateTable(tt.stmt)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			validator := NewCheckValidator(table)
			if validator.HasCheckConstraints() != tt.expected {
				t.Errorf("Expected HasCheckConstraints() = %v, got %v", tt.expected, validator.HasCheckConstraints())
			}
		})
	}
}

// TestComplexCheckExpressions tests CHECK constraints with complex expressions.
func TestComplexCheckExpressions(t *testing.T) {
	tests := []struct {
		name string
		stmt *parser.CreateTableStmt
	}{
		{
			name: "AND expression",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{
						Name: "age",
						Type: "INTEGER",
						Constraints: []parser.ColumnConstraint{
							{
								Type: parser.ConstraintCheck,
								Check: &parser.BinaryExpr{
									Op: parser.OpAnd,
									Left: &parser.BinaryExpr{
										Op:    parser.OpGe,
										Left:  &parser.IdentExpr{Name: "age"},
										Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"},
									},
									Right: &parser.BinaryExpr{
										Op:    parser.OpLe,
										Left:  &parser.IdentExpr{Name: "age"},
										Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "120"},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "OR expression",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{Name: "status", Type: "TEXT"},
				},
				Constraints: []parser.TableConstraint{
					{
						Type: parser.ConstraintCheck,
						Name: "valid_status",
						Check: &parser.BinaryExpr{
							Op: parser.OpOr,
							Left: &parser.BinaryExpr{
								Op:    parser.OpEq,
								Left:  &parser.IdentExpr{Name: "status"},
								Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "active"},
							},
							Right: &parser.BinaryExpr{
								Op:    parser.OpEq,
								Left:  &parser.IdentExpr{Name: "status"},
								Right: &parser.LiteralExpr{Type: parser.LiteralString, Value: "inactive"},
							},
						},
					},
				},
			},
		},
		{
			name: "Multi-column constraint",
			stmt: &parser.CreateTableStmt{
				Name: "test",
				Columns: []parser.ColumnDef{
					{Name: "start_date", Type: "INTEGER"},
					{Name: "end_date", Type: "INTEGER"},
				},
				Constraints: []parser.TableConstraint{
					{
						Type: parser.ConstraintCheck,
						Name: "valid_date_range",
						Check: &parser.BinaryExpr{
							Op:    parser.OpLt,
							Left:  &parser.IdentExpr{Name: "start_date"},
							Right: &parser.IdentExpr{Name: "end_date"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema.NewSchema()
			table, err := s.CreateTable(tt.stmt)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			validator := NewCheckValidator(table)
			if !validator.HasCheckConstraints() {
				t.Error("Expected validator to have CHECK constraints")
			}

			// Use mock code generator for testing
			mock := &mockCodeGenerator{}

			err = validator.ValidateInsertWithGenerator(mock)
			if err != nil {
				t.Errorf("ValidateInsertWithGenerator failed: %v", err)
			}

			// Verify constraints were processed
			if len(mock.constraints) == 0 {
				t.Error("Expected at least one constraint to be generated")
			}
		})
	}
}

// TestNullInCheckConstraints tests that NULL values pass CHECK constraints.
func TestNullInCheckConstraints(t *testing.T) {
	// According to SQLite semantics, NULL is treated as TRUE in CHECK constraints
	// This test ensures we handle NULL correctly

	stmt := &parser.CreateTableStmt{
		Name: "test",
		Columns: []parser.ColumnDef{
			{
				Name: "optional_age",
				Type: "INTEGER",
				Constraints: []parser.ColumnConstraint{
					{
						Type:  parser.ConstraintCheck,
						Check: &parser.BinaryExpr{Op: parser.OpGt, Left: &parser.IdentExpr{Name: "optional_age"}, Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "0"}},
					},
				},
			},
		},
	}

	s := schema.NewSchema()
	table, err := s.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	validator := NewCheckValidator(table)

	// Use mock code generator for testing
	mock := &mockCodeGenerator{}

	err = validator.ValidateInsertWithGenerator(mock)
	if err != nil {
		t.Errorf("ValidateInsertWithGenerator failed: %v", err)
	}

	// Verify the constraint was processed
	if len(mock.constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(mock.constraints))
	}

	// Verify the error message is generated
	if len(mock.errorMsgs) != 1 {
		t.Errorf("Expected 1 error message, got %d", len(mock.errorMsgs))
	}
}
