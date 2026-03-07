// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"strings"
	"testing"
)

func TestExprCreation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected string
	}{
		{
			name:     "Integer literal",
			expr:     NewIntExpr(42),
			expected: "42",
		},
		{
			name:     "Float literal",
			expr:     NewFloatExpr(3.14),
			expected: "3.14",
		},
		{
			name:     "String literal",
			expr:     NewStringExpr("hello"),
			expected: "'hello'",
		},
		{
			name:     "NULL literal",
			expr:     NewNullExpr(),
			expected: "NULL",
		},
		{
			name:     "Column reference",
			expr:     NewColumnExpr("users", "name", 0, 1),
			expected: "users.name",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.expr.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestBinaryExpressions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected string
	}{
		{
			name:     "Addition",
			expr:     NewBinaryExpr(OpPlus, NewIntExpr(1), NewIntExpr(2)),
			expected: "(1 + 2)",
		},
		{
			name:     "Multiplication",
			expr:     NewBinaryExpr(OpMultiply, NewIntExpr(3), NewIntExpr(4)),
			expected: "(3 * 4)",
		},
		{
			name:     "Comparison",
			expr:     NewBinaryExpr(OpEq, NewColumnExpr("", "age", 0, 0), NewIntExpr(25)),
			expected: "(age = 25)",
		},
		{
			name: "Complex expression",
			expr: NewBinaryExpr(OpPlus,
				NewBinaryExpr(OpMultiply, NewIntExpr(2), NewIntExpr(3)),
				NewIntExpr(4)),
			expected: "((2 * 3) + 4)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.expr.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestUnaryExpressions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected string
	}{
		{
			name:     "Negation",
			expr:     NewUnaryExpr(OpNegate, NewIntExpr(5)),
			expected: "(-5)",
		},
		{
			name:     "NOT",
			expr:     NewUnaryExpr(OpNot, NewColumnExpr("", "active", 0, 0)),
			expected: "(NOT active)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.expr.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestIsConstant(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected bool
	}{
		{
			name:     "Integer is constant",
			expr:     NewIntExpr(42),
			expected: true,
		},
		{
			name:     "Column is not constant",
			expr:     NewColumnExpr("t", "x", 0, 0),
			expected: false,
		},
		{
			name:     "Constant arithmetic",
			expr:     NewBinaryExpr(OpPlus, NewIntExpr(1), NewIntExpr(2)),
			expected: true,
		},
		{
			name:     "Non-constant arithmetic",
			expr:     NewBinaryExpr(OpPlus, NewIntExpr(1), NewColumnExpr("t", "x", 0, 0)),
			expected: false,
		},
		{
			name:     "NULL is constant",
			expr:     NewNullExpr(),
			expected: true,
		},
		{
			name:     "Float is constant",
			expr:     NewFloatExpr(3.14),
			expected: true,
		},
		{
			name:     "String is constant",
			expr:     NewStringExpr("hello"),
			expected: true,
		},
		{
			name:     "Unary negation of constant",
			expr:     NewUnaryExpr(OpNegate, NewIntExpr(5)),
			expected: true,
		},
		{
			name:     "Unary negation of non-constant",
			expr:     NewUnaryExpr(OpNegate, NewColumnExpr("t", "x", 0, 0)),
			expected: false,
		},
		{
			name:     "Function with constant args",
			expr:     NewFunctionExpr("abs", &ExprList{Items: []*ExprListItem{{Expr: NewIntExpr(-5)}}}),
			expected: true,
		},
		{
			name:     "Function with non-constant args",
			expr:     NewFunctionExpr("abs", &ExprList{Items: []*ExprListItem{{Expr: NewColumnExpr("t", "x", 0, 0)}}}),
			expected: false,
		},
		{
			name: "Function with mixed args",
			expr: NewFunctionExpr("coalesce", &ExprList{Items: []*ExprListItem{
				{Expr: NewIntExpr(1)},
				{Expr: NewColumnExpr("t", "x", 0, 0)},
			}}),
			expected: false,
		},
		{
			name: "Function with EP_HasFunc property",
			expr: &Expr{
				Op:    OpFunction,
				Token: "now",
				Flags: EP_HasFunc,
			},
			expected: false,
		},
		{
			name:     "nil expr is constant",
			expr:     nil,
			expected: true,
		},
		{
			name:     "Bitwise AND constant",
			expr:     NewBinaryExpr(OpBitAnd, NewIntExpr(12), NewIntExpr(10)),
			expected: true,
		},
		{
			name:     "Logical AND constant",
			expr:     NewBinaryExpr(OpAnd, NewIntExpr(1), NewIntExpr(1)),
			expected: true,
		},
		{
			name:     "Comparison constant",
			expr:     NewBinaryExpr(OpEq, NewIntExpr(5), NewIntExpr(5)),
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.expr.IsConstant()
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestExprHeight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected int
	}{
		{
			name:     "Literal has height 1",
			expr:     NewIntExpr(42),
			expected: 1,
		},
		{
			name:     "Binary expr has height 2",
			expr:     NewBinaryExpr(OpPlus, NewIntExpr(1), NewIntExpr(2)),
			expected: 2,
		},
		{
			name: "Nested expr has height 3",
			expr: NewBinaryExpr(OpPlus,
				NewBinaryExpr(OpMultiply, NewIntExpr(2), NewIntExpr(3)),
				NewIntExpr(4)),
			expected: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.expr.Height != tt.expected {
				t.Errorf("Expected height %d, got %d", tt.expected, tt.expr.Height)
			}
		})
	}
}

func TestExprClone(t *testing.T) {
	t.Parallel()
	original := NewBinaryExpr(OpPlus,
		NewIntExpr(1),
		NewBinaryExpr(OpMultiply, NewIntExpr(2), NewIntExpr(3)))

	clone := original.Clone()

	// Verify clone matches original
	if clone.String() != original.String() {
		t.Errorf("Clone doesn't match original: %v vs %v", clone.String(), original.String())
	}

	// Verify they're different objects
	if clone == original {
		t.Error("Clone is the same object as original")
	}

	// Modify clone
	clone.Left = NewIntExpr(10)

	// Verify original is unchanged
	if original.Left.IntValue != 1 {
		t.Error("Modifying clone affected original")
	}
}

func TestFunctionExpr(t *testing.T) {
	t.Parallel()
	args := &ExprList{
		Items: []*ExprListItem{
			{Expr: NewColumnExpr("", "name", 0, 0)},
			{Expr: NewIntExpr(5)},
		},
	}

	expr := NewFunctionExpr("substr", args)

	expected := "substr(name, 5)"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestExprListClone(t *testing.T) {
	t.Parallel()
	original := &ExprList{
		Items: []*ExprListItem{
			{Expr: NewIntExpr(1), Name: "col1"},
			{Expr: NewStringExpr("test"), Name: "col2"},
		},
	}

	clone := original.Clone()

	// Verify clone matches original
	if len(clone.Items) != len(original.Items) {
		t.Errorf("Clone length mismatch: %d vs %d", len(clone.Items), len(original.Items))
	}

	// Verify they're different objects
	if clone == original {
		t.Error("Clone is the same object as original")
	}

	// Modify clone
	clone.Items[0].Name = "modified"

	// Verify original is unchanged
	if original.Items[0].Name != "col1" {
		t.Error("Modifying clone affected original")
	}
}

func TestVectorExpressions(t *testing.T) {
	t.Parallel()
	// Scalar expression
	scalar := NewIntExpr(42)
	if scalar.IsVector() {
		t.Error("Scalar should not be a vector")
	}
	if scalar.VectorSize() != 1 {
		t.Errorf("Scalar vector size should be 1, got %d", scalar.VectorSize())
	}

	// Vector expression
	vector := &Expr{
		Op: OpVector,
		List: &ExprList{
			Items: []*ExprListItem{
				{Expr: NewIntExpr(1)},
				{Expr: NewIntExpr(2)},
				{Expr: NewIntExpr(3)},
			},
		},
	}
	vector.updateHeight()

	if !vector.IsVector() {
		t.Error("Vector should be a vector")
	}
	if vector.VectorSize() != 3 {
		t.Errorf("Vector size should be 3, got %d", vector.VectorSize())
	}
}

func TestExprFlags(t *testing.T) {
	t.Parallel()
	expr := NewIntExpr(42)

	// Initially should have EP_IntValue and EP_Leaf
	if !expr.HasProperty(EP_IntValue) {
		t.Error("Integer expr should have EP_IntValue")
	}
	if !expr.HasProperty(EP_Leaf) {
		t.Error("Integer expr should have EP_Leaf")
	}

	// Set a property
	expr.SetProperty(EP_Collate)
	if !expr.HasProperty(EP_Collate) {
		t.Error("Property not set correctly")
	}

	// Clear a property
	expr.ClearProperty(EP_Collate)
	if expr.HasProperty(EP_Collate) {
		t.Error("Property not cleared correctly")
	}
}

func TestExprComplexExpression(t *testing.T) {
	t.Parallel()
	// Build: (a + b * c) > 10
	expr := NewBinaryExpr(OpGt,
		NewBinaryExpr(OpPlus,
			NewColumnExpr("t", "a", 0, 0),
			NewBinaryExpr(OpMultiply,
				NewColumnExpr("t", "b", 0, 1),
				NewColumnExpr("t", "c", 0, 2))),
		NewIntExpr(10))

	expected := "((t.a + (t.b * t.c)) > 10)"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}

	// Should not be constant (contains columns)
	if expr.IsConstant() {
		t.Error("Expression with columns should not be constant")
	}

	// Should have height 4
	if expr.Height != 4 {
		t.Errorf("Expected height 4, got %d", expr.Height)
	}
}

func TestExprCastExpression(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op:    OpCast,
		Token: "INTEGER",
		Left:  NewStringExpr("123"),
	}
	expr.updateHeight()

	expected := "CAST('123' AS INTEGER)"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestCollateExpression(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op:      OpCollate,
		CollSeq: "NOCASE",
		Left:    NewColumnExpr("", "name", 0, 0),
	}
	expr.updateHeight()

	expected := "(name COLLATE NOCASE)"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestLikeExpression(t *testing.T) {
	t.Parallel()
	expr := NewBinaryExpr(OpLike,
		NewColumnExpr("", "name", 0, 0),
		NewStringExpr("%test%"))

	expected := "(name LIKE '%test%')"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestAndOrExpressions(t *testing.T) {
	t.Parallel()
	// age > 18 AND active = true
	expr := NewBinaryExpr(OpAnd,
		NewBinaryExpr(OpGt,
			NewColumnExpr("", "age", 0, 0),
			NewIntExpr(18)),
		NewBinaryExpr(OpEq,
			NewColumnExpr("", "active", 0, 1),
			NewIntExpr(1)))

	expected := "((age > 18) AND (active = 1))"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestIsNullExpression(t *testing.T) {
	t.Parallel()
	expr := &Expr{
		Op:   OpIsNull,
		Left: NewColumnExpr("", "email", 0, 0),
	}
	expr.updateHeight()

	expected := "(email IS NULL)"
	result := expr.String()

	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// TestExprStringEdgeCases tests edge cases for the String method.
func TestExprStringEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		contains string
	}{
		{
			name:     "Unknown OpCode",
			expr:     &Expr{Op: OpCode(200)},
			contains: "Expr<",
		},
		{
			name:     "Nil expression",
			expr:     nil,
			contains: "NULL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.expr.String()
			if !strings.Contains(result, tt.contains) {
				t.Errorf("Expected String() to contain %q, got %q", tt.contains, result)
			}
		})
	}
}

// Import strings for TestExprStringEdgeCases
