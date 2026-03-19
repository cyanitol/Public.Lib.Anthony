// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import "strings"

import (
	"testing"
)

func TestCompileUpdateWithIndex(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "users",
		Columns: []string{"name"},
		Values:  []Value{TextValue("John")},
	}

	prog, err := CompileUpdateWithIndex(stmt, 100, 2, []int{1})
	if err != nil {
		t.Fatalf("CompileUpdateWithIndex failed: %v", err)
	}

	if prog == nil {
		t.Error("Program should not be nil")
	}
}

func TestValidateUpdate(t *testing.T) {
	stmt := &UpdateStmt{
		Table:   "users",
		Columns: []string{"name"},
		Values:  []Value{TextValue("John")},
	}

	err := ValidateUpdate(stmt)
	if err != nil {
		t.Fatalf("ValidateUpdate failed: %v", err)
	}
}

func TestNewUpdateStmt(t *testing.T) {
	stmt := NewUpdateStmt("users", []string{"name"}, []Value{TextValue("John")}, nil)
	if stmt == nil {
		t.Fatal("NewUpdateStmt returned nil")
	}
	if stmt.Table != "users" {
		t.Errorf("Table = %s, want users", stmt.Table)
	}
}

func TestNewWhereClause(t *testing.T) {
	expr := &Expression{Type: ExprColumn, Column: "id"}
	where := NewWhereClause(expr)
	if where == nil {
		t.Fatal("NewWhereClause returned nil")
	}
	if where.Expr != expr {
		t.Error("Expression not set correctly")
	}
}

func TestNewBinaryExpression(t *testing.T) {
	left := &Expression{Type: ExprColumn, Column: "id"}
	right := &Expression{Type: ExprLiteral, Value: IntValue(1)}
	expr := NewBinaryExpression(left, "=", right)

	if expr == nil {
		t.Fatal("NewBinaryExpression returned nil")
	}
	if expr.Type != ExprBinary {
		t.Error("Type should be ExprBinary")
	}
	if expr.Operator != "=" {
		t.Errorf("Operator = %s, want =", expr.Operator)
	}
}

func TestNewColumnExpression(t *testing.T) {
	expr := NewColumnExpression("id")
	if expr == nil {
		t.Fatal("NewColumnExpression returned nil")
	}
	if expr.Type != ExprColumn {
		t.Error("Type should be ExprColumn")
	}
	if expr.Column != "id" {
		t.Errorf("Column = %s, want id", expr.Column)
	}
}

func TestNewLiteralExpression(t *testing.T) {
	expr := NewLiteralExpression(IntValue(42))
	if expr == nil {
		t.Fatal("NewLiteralExpression returned nil")
	}
	if expr.Type != ExprLiteral {
		t.Error("Type should be ExprLiteral")
	}
}

// Test ValidateUpdate comprehensive
func TestValidateUpdateComprehensive(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *UpdateStmt
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_update",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{"name"},
				Values:  []Value{TextValue("Alice")},
			},
			wantErr: false,
		},
		{
			name:    "nil_stmt",
			stmt:    nil,
			wantErr: true,
			errMsg:  "nil update statement",
		},
		{
			name: "empty_table",
			stmt: &UpdateStmt{
				Table:   "",
				Columns: []string{"name"},
				Values:  []Value{TextValue("Alice")},
			},
			wantErr: true,
			errMsg:  "table name is required",
		},
		{
			name: "no_columns",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{},
				Values:  []Value{},
			},
			wantErr: true,
			errMsg:  "no columns to update",
		},
		{
			name: "column_value_mismatch",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{"name", "email"},
				Values:  []Value{TextValue("Alice")}, // only 1 value for 2 columns
			},
			wantErr: true,
			errMsg:  "column count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUpdate(tt.stmt)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil && tt.errMsg != "" {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateUpdate() error = %v, want substring %q", err, tt.errMsg)
				}
			}
		})
	}
}

// TestValidateUpdateStmt tests the internal validateUpdateStmt function
func TestValidateUpdateStmt(t *testing.T) {
	tests := []struct {
		name    string
		stmt    *UpdateStmt
		wantErr bool
	}{
		{
			name: "valid",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{"name"},
				Values:  []Value{TextValue("test")},
			},
			wantErr: false,
		},
		{
			name:    "nil",
			stmt:    nil,
			wantErr: true,
		},
		{
			name: "no columns",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{},
				Values:  []Value{},
			},
			wantErr: true,
		},
		{
			name: "mismatched count",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{"a", "b"},
				Values:  []Value{TextValue("x")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpdateStmt(tt.stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateUpdateStmt() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCompileUpdateEdgeCases tests edge cases for CompileUpdate
func TestCompileUpdateEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		stmt      *UpdateStmt
		tableRoot int
		numCols   int
		wantErr   bool
	}{
		{
			name:      "nil stmt",
			stmt:      nil,
			tableRoot: 1,
			numCols:   2,
			wantErr:   true,
		},
		{
			name: "invalid stmt",
			stmt: &UpdateStmt{
				Table:   "users",
				Columns: []string{},
				Values:  []Value{},
			},
			tableRoot: 1,
			numCols:   2,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileUpdate(tt.stmt, tt.tableRoot, tt.numCols)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompileUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
