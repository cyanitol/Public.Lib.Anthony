// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"testing"
)

func TestAffinityFromType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typeName string
		expected Affinity
	}{
		{"", AFF_BLOB},
		{"INTEGER", AFF_INTEGER},
		{"INT", AFF_INTEGER},
		{"BIGINT", AFF_INTEGER},
		{"VARCHAR(255)", AFF_TEXT},
		{"TEXT", AFF_TEXT},
		{"CHAR(10)", AFF_TEXT},
		{"CLOB", AFF_TEXT},
		{"BLOB", AFF_BLOB},
		{"REAL", AFF_REAL},
		{"FLOAT", AFF_REAL},
		{"DOUBLE", AFF_REAL},
		{"NUMERIC", AFF_NUMERIC},
		{"DECIMAL", AFF_NUMERIC},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.typeName, func(t *testing.T) {
			t.Parallel()
			result := AffinityFromType(tt.typeName)
			if result != tt.expected {
				t.Errorf("AffinityFromType(%q) = %v, want %v",
					tt.typeName, result, tt.expected)
			}
		})
	}
}

func TestGetExprAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name:     "Integer literal",
			expr:     NewIntExpr(42),
			expected: AFF_NONE,
		},
		{
			name: "Column with INTEGER affinity",
			expr: &Expr{
				Op:       OpColumn,
				Affinity: AFF_INTEGER,
			},
			expected: AFF_INTEGER,
		},
		{
			name: "CAST to TEXT",
			expr: &Expr{
				Op:    OpCast,
				Token: "TEXT",
				Left:  NewIntExpr(42),
			},
			expected: AFF_TEXT,
		},
		{
			name: "CAST to REAL",
			expr: &Expr{
				Op:    OpCast,
				Token: "FLOAT",
				Left:  NewStringExpr("3.14"),
			},
			expected: AFF_REAL,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := GetExprAffinity(tt.expr)
			if result != tt.expected {
				t.Errorf("GetExprAffinity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCompareAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     *Expr
		right    *Expr
		expected Affinity
	}{
		{
			name:     "Both columns with numeric affinity",
			left:     &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
			right:    &Expr{Op: OpColumn, Affinity: AFF_REAL},
			expected: AFF_NUMERIC,
		},
		{
			name:     "Both columns with text affinity",
			left:     &Expr{Op: OpColumn, Affinity: AFF_TEXT},
			right:    &Expr{Op: OpColumn, Affinity: AFF_TEXT},
			expected: AFF_BLOB,
		},
		{
			name:     "Column vs literal",
			left:     &Expr{Op: OpColumn, Affinity: AFF_INTEGER},
			right:    NewIntExpr(42),
			expected: AFF_INTEGER,
		},
		{
			name:     "Literal vs literal",
			left:     NewIntExpr(1),
			right:    NewIntExpr(2),
			expected: AFF_NONE,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := CompareAffinity(tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("CompareAffinity() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestApplyAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		affinity Affinity
		expected interface{}
	}{
		{
			name:     "String to INTEGER",
			value:    "42",
			affinity: AFF_INTEGER,
			expected: int64(42),
		},
		{
			name:     "String to REAL",
			value:    "3.14",
			affinity: AFF_REAL,
			expected: 3.14,
		},
		{
			name:     "Integer to TEXT",
			value:    int64(123),
			affinity: AFF_TEXT,
			expected: "123",
		},
		{
			name:     "Float to TEXT",
			value:    3.14,
			affinity: AFF_TEXT,
			expected: "3.14",
		},
		{
			name:     "Non-numeric string to INTEGER",
			value:    "hello",
			affinity: AFF_INTEGER,
			expected: "hello", // Can't convert, keep as string
		},
		{
			name:     "Integer to REAL",
			value:    int64(42),
			affinity: AFF_REAL,
			expected: 42.0,
		},
		{
			name:     "NULL remains NULL",
			value:    nil,
			affinity: AFF_INTEGER,
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ApplyAffinity(tt.value, tt.affinity)
			if result != tt.expected {
				t.Errorf("ApplyAffinity(%v, %v) = %v, want %v",
					tt.value, tt.affinity, result, tt.expected)
			}
		})
	}
}

func TestPropagateAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		expr     *Expr
		expected Affinity
	}{
		{
			name:     "Addition produces NUMERIC",
			expr:     NewBinaryExpr(OpPlus, NewIntExpr(1), NewIntExpr(2)),
			expected: AFF_NUMERIC,
		},
		{
			name:     "Concatenation produces TEXT",
			expr:     NewBinaryExpr(OpConcat, NewStringExpr("a"), NewStringExpr("b")),
			expected: AFF_TEXT,
		},
		{
			name:     "Bitwise AND produces INTEGER",
			expr:     NewBinaryExpr(OpBitAnd, NewIntExpr(7), NewIntExpr(3)),
			expected: AFF_INTEGER,
		},
		{
			name:     "Comparison produces INTEGER (boolean)",
			expr:     NewBinaryExpr(OpEq, NewIntExpr(1), NewIntExpr(2)),
			expected: AFF_INTEGER,
		},
		{
			name:     "Logical AND produces INTEGER (boolean)",
			expr:     NewBinaryExpr(OpAnd, NewIntExpr(1), NewIntExpr(0)),
			expected: AFF_INTEGER,
		},
		{
			name:     "NOT produces INTEGER (boolean)",
			expr:     NewUnaryExpr(OpNot, NewIntExpr(1)),
			expected: AFF_INTEGER,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			PropagateAffinity(tt.expr)
			if tt.expr.Affinity != tt.expected {
				t.Errorf("After PropagateAffinity, affinity = %v, want %v",
					tt.expr.Affinity, tt.expected)
			}
		})
	}
}

func TestIsNumericAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		affinity Affinity
		expected bool
	}{
		{AFF_NONE, false},
		{AFF_BLOB, false},
		{AFF_TEXT, false},
		{AFF_NUMERIC, true},
		{AFF_INTEGER, true},
		{AFF_REAL, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.affinity.String(), func(t *testing.T) {
			t.Parallel()
			result := IsNumericAffinity(tt.affinity)
			if result != tt.expected {
				t.Errorf("IsNumericAffinity(%v) = %v, want %v",
					tt.affinity, result, tt.expected)
			}
		})
	}
}

func TestSetTableColumnAffinity(t *testing.T) {
	t.Parallel()
	expr := NewColumnExpr("users", "age", 0, 0)

	// Initially should have no affinity
	if expr.Affinity != AFF_NONE {
		t.Errorf("Initial affinity should be NONE, got %v", expr.Affinity)
	}

	// Set affinity based on column type
	SetTableColumnAffinity(expr, "INTEGER")

	if expr.Affinity != AFF_INTEGER {
		t.Errorf("After SetTableColumnAffinity, affinity = %v, want %v",
			expr.Affinity, AFF_INTEGER)
	}
}

func TestAffinityString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		affinity Affinity
		expected string
	}{
		{AFF_NONE, "NONE"},
		{AFF_BLOB, "BLOB"},
		{AFF_TEXT, "TEXT"},
		{AFF_NUMERIC, "NUMERIC"},
		{AFF_INTEGER, "INTEGER"},
		{AFF_REAL, "REAL"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			result := tt.affinity.String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestComplexAffinityPropagation(t *testing.T) {
	t.Parallel()
	// Build: (a + b) * 2
	// Should have NUMERIC affinity throughout
	expr := NewBinaryExpr(OpMultiply,
		NewBinaryExpr(OpPlus,
			NewColumnExpr("t", "a", 0, 0),
			NewColumnExpr("t", "b", 0, 1)),
		NewIntExpr(2))

	PropagateAffinity(expr)

	// Root should be NUMERIC
	if expr.Affinity != AFF_NUMERIC {
		t.Errorf("Root affinity = %v, want NUMERIC", expr.Affinity)
	}

	// Left child (addition) should be NUMERIC
	if expr.Left.Affinity != AFF_NUMERIC {
		t.Errorf("Left child affinity = %v, want NUMERIC", expr.Left.Affinity)
	}
}
