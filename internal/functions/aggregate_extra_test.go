// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"math"
	"testing"
)

// TestSumFunc_IntegerOverflow tests integer overflow in sum function
func TestSumFunc_IntegerOverflow(t *testing.T) {
	f := &SumFunc{}

	// Add a large positive integer
	err := f.Step([]Value{NewIntValue(math.MaxInt64 - 10)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	// Add another value that causes overflow
	err = f.Step([]Value{NewIntValue(20)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}

	// Should have switched to float
	if result.Type() != TypeFloat {
		t.Errorf("Final() type = %v, want TypeFloat after overflow", result.Type())
	}
}

// TestSumFunc_NegativeOverflow tests negative integer overflow
func TestSumFunc_NegativeOverflow(t *testing.T) {
	f := &SumFunc{}

	// Add a large negative integer
	err := f.Step([]Value{NewIntValue(math.MinInt64 + 10)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	// Add another negative value that causes underflow
	err = f.Step([]Value{NewIntValue(-20)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}

	// Should have switched to float
	if result.Type() != TypeFloat {
		t.Errorf("Final() type = %v, want TypeFloat after underflow", result.Type())
	}
}

// TestSumFunc_MixedTypes tests sum with mixed integer and float
func TestSumFunc_MixedTypes(t *testing.T) {
	f := &SumFunc{}

	err := f.Step([]Value{NewIntValue(10)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	err = f.Step([]Value{NewFloatValue(3.14)})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}

	if result.Type() != TypeFloat {
		t.Errorf("Final() type = %v, want TypeFloat", result.Type())
	}

	expected := 13.14
	got := result.AsFloat64()
	if math.Abs(got-expected) > 0.001 {
		t.Errorf("Final() = %v, want %v", got, expected)
	}
}

// TestSumFunc_TextValue tests sum with text value
func TestSumFunc_TextValue(t *testing.T) {
	f := &SumFunc{}

	err := f.Step([]Value{NewTextValue("42.5")})
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}

	if result.Type() != TypeFloat {
		t.Errorf("Final() type = %v, want TypeFloat", result.Type())
	}
}

// TestTotalFunc_EdgeCases tests edge cases for total function
func TestTotalFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		values []Value
		want   float64
	}{
		{
			name:   "empty set",
			values: []Value{},
			want:   0.0,
		},
		{
			name:   "all null",
			values: []Value{NewNullValue(), NewNullValue()},
			want:   0.0,
		},
		{
			name:   "mixed with null",
			values: []Value{NewIntValue(5), NewNullValue(), NewIntValue(10)},
			want:   15.0,
		},
		{
			name:   "text values",
			values: []Value{NewTextValue("10"), NewTextValue("20")},
			want:   30.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &TotalFunc{}
			for _, v := range tt.values {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("Step() error = %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("Final() error = %v", err)
			}
			got := result.AsFloat64()
			if got != tt.want {
				t.Errorf("Final() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAvgFunc_EdgeCases tests edge cases for avg function
func TestAvgFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		values   []Value
		want     float64
		wantNull bool
	}{
		{
			name:     "empty set",
			values:   []Value{},
			wantNull: true,
		},
		{
			name:     "all null",
			values:   []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:   "integers",
			values: []Value{NewIntValue(10), NewIntValue(20), NewIntValue(30)},
			want:   20.0,
		},
		{
			name:   "floats",
			values: []Value{NewFloatValue(1.5), NewFloatValue(2.5)},
			want:   2.0,
		},
		{
			name:   "text values",
			values: []Value{NewTextValue("10"), NewTextValue("20")},
			want:   15.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &AvgFunc{}
			for _, v := range tt.values {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("Step() error = %v", err)
				}
			}
			result, err := f.Final()
			if err != nil {
				t.Fatalf("Final() error = %v", err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("Final() = %v, want NULL", result)
				}
				return
			}
			got := result.AsFloat64()
			if math.Abs(got-tt.want) > 0.001 {
				t.Errorf("Final() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMinMaxFunc_EdgeCases tests edge cases for min/max functions
func TestMinMaxFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		fn       string
		values   []Value
		wantNull bool
		validate func(Value) bool
	}{
		{
			name:     "min empty set",
			fn:       "min",
			values:   []Value{},
			wantNull: true,
		},
		{
			name:     "max empty set",
			fn:       "max",
			values:   []Value{},
			wantNull: true,
		},
		{
			name:     "min all null",
			fn:       "min",
			values:   []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:     "max all null",
			fn:       "max",
			values:   []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:   "min with null",
			fn:     "min",
			values: []Value{NewIntValue(5), NewNullValue(), NewIntValue(3)},
			validate: func(v Value) bool {
				return v.AsInt64() == 3
			},
		},
		{
			name:   "max with null",
			fn:     "max",
			values: []Value{NewIntValue(5), NewNullValue(), NewIntValue(10)},
			validate: func(v Value) bool {
				return v.AsInt64() == 10
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f interface {
				Step([]Value) error
				Final() (Value, error)
			}

			if tt.fn == "min" {
				f = &MinFunc{}
			} else {
				f = &MaxFunc{}
			}

			for _, v := range tt.values {
				if err := f.Step([]Value{v}); err != nil {
					t.Fatalf("Step() error = %v", err)
				}
			}

			result, err := f.Final()
			if err != nil {
				t.Fatalf("Final() error = %v", err)
			}

			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("Final() = %v, want NULL", result)
				}
				return
			}

			if tt.validate != nil && !tt.validate(result) {
				t.Errorf("Final() validation failed for %v", result)
			}
		})
	}
}

// TestGroupConcatFunc_EdgeCases tests edge cases for group_concat function
func TestGroupConcatFunc_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		values   [][]Value
		want     string
		wantNull bool
		wantErr  bool
	}{
		{
			name:     "empty set",
			values:   [][]Value{},
			wantNull: true,
		},
		{
			name:     "all null",
			values:   [][]Value{{NewNullValue()}, {NewNullValue()}},
			wantNull: true,
		},
		{
			name:   "default separator",
			values: [][]Value{{NewTextValue("a")}, {NewTextValue("b")}, {NewTextValue("c")}},
			want:   "a,b,c",
		},
		{
			name:   "custom separator",
			values: [][]Value{{NewTextValue("a"), NewTextValue("|")}, {NewTextValue("b"), NewTextValue("|")}, {NewTextValue("c"), NewTextValue("|")}},
			want:   "a|b|c",
		},
		{
			name:   "null separator",
			values: [][]Value{{NewTextValue("a"), NewNullValue()}, {NewTextValue("b"), NewNullValue()}},
			want:   "a,b",
		},
		{
			name:   "mixed with null values",
			values: [][]Value{{NewTextValue("a")}, {NewNullValue()}, {NewTextValue("c")}},
			want:   "a,c",
		},
		{
			name:    "too many args",
			values:  [][]Value{{NewTextValue("a"), NewTextValue(","), NewTextValue("extra")}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &GroupConcatFunc{}

			for _, args := range tt.values {
				err := f.Step(args)
				if tt.wantErr {
					if err == nil {
						t.Error("Step() expected error, got nil")
					}
					return
				}
				if err != nil {
					t.Fatalf("Step() error = %v", err)
				}
			}

			result, err := f.Final()
			if err != nil {
				t.Fatalf("Final() error = %v", err)
			}

			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("Final() = %v, want NULL", result)
				}
				return
			}

			got := result.AsString()
			if got != tt.want {
				t.Errorf("Final() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAggregateFunc_Call tests that aggregate functions return error on Call
func TestAggregateFunc_Call(t *testing.T) {
	funcs := []struct {
		name string
		fn   interface {
			Call([]Value) (Value, error)
		}
	}{
		{"count", &CountFunc{}},
		{"count(*)", &CountStarFunc{}},
		{"sum", &SumFunc{}},
		{"total", &TotalFunc{}},
		{"avg", &AvgFunc{}},
		{"min", &MinFunc{}},
		{"max", &MaxFunc{}},
		{"group_concat", &GroupConcatFunc{}},
	}

	for _, tt := range funcs {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.fn.Call([]Value{})
			if err == nil {
				t.Errorf("%s.Call() expected error, got nil", tt.name)
			}
		})
	}
}

// TestAggregateFunc_Names tests aggregate function names
func TestAggregateFunc_Names(t *testing.T) {
	tests := []struct {
		fn   interface{ Name() string }
		want string
	}{
		{&CountFunc{}, "count"},
		{&CountStarFunc{}, "count(*)"},
		{&SumFunc{}, "sum"},
		{&TotalFunc{}, "total"},
		{&AvgFunc{}, "avg"},
		{&MinFunc{}, "min"},
		{&MaxFunc{}, "max"},
		{&GroupConcatFunc{}, "group_concat"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.fn.Name()
			if got != tt.want {
				t.Errorf("Name() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAggregateFunc_NumArgs tests aggregate function argument counts
func TestAggregateFunc_NumArgs(t *testing.T) {
	tests := []struct {
		fn   interface{ NumArgs() int }
		want int
	}{
		{&CountFunc{}, 1},
		{&CountStarFunc{}, 0},
		{&SumFunc{}, 1},
		{&TotalFunc{}, 1},
		{&AvgFunc{}, 1},
		{&MinFunc{}, 1},
		{&MaxFunc{}, 1},
		{&GroupConcatFunc{}, -1},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := tt.fn.NumArgs()
			if got != tt.want {
				t.Errorf("NumArgs() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestMinMaxScalarFunc tests scalar versions of min/max
func TestMinMaxScalarFunc(t *testing.T) {
	tests := []struct {
		name     string
		fn       func([]Value) (Value, error)
		fnName   string
		args     []Value
		want     int64
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "min no args",
			fn:      minScalarFunc,
			fnName:  "min",
			args:    []Value{},
			wantErr: true,
		},
		{
			name:     "min all null",
			fn:       minScalarFunc,
			fnName:   "min",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:   "min multiple values",
			fn:     minScalarFunc,
			fnName: "min",
			args:   []Value{NewIntValue(5), NewIntValue(2), NewIntValue(8)},
			want:   2,
		},
		{
			name:   "min with null",
			fn:     minScalarFunc,
			fnName: "min",
			args:   []Value{NewIntValue(5), NewNullValue(), NewIntValue(2)},
			want:   2,
		},
		{
			name:    "max no args",
			fn:      maxScalarFunc,
			fnName:  "max",
			args:    []Value{},
			wantErr: true,
		},
		{
			name:     "max all null",
			fn:       maxScalarFunc,
			fnName:   "max",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:   "max multiple values",
			fn:     maxScalarFunc,
			fnName: "max",
			args:   []Value{NewIntValue(5), NewIntValue(2), NewIntValue(8)},
			want:   8,
		},
		{
			name:   "max with null",
			fn:     maxScalarFunc,
			fnName: "max",
			args:   []Value{NewIntValue(5), NewNullValue(), NewIntValue(8)},
			want:   8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Errorf("%s() expected error, got nil", tt.fnName)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s() error = %v", tt.fnName, err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("%s() = %v, want NULL", tt.fnName, result)
				}
				return
			}
			got := result.AsInt64()
			if got != tt.want {
				t.Errorf("%s() = %d, want %d", tt.fnName, got, tt.want)
			}
		})
	}
}

// TestIsNaNHelper tests isNaN helper function
func TestIsNaNHelper(t *testing.T) {
	tests := []struct {
		input float64
		want  bool
	}{
		{0.0, false},
		{1.5, false},
		{math.NaN(), true},
		{math.Inf(1), false},
		{math.Inf(-1), false},
	}

	for _, tt := range tests {
		got := isNaN(tt.input)
		if got != tt.want {
			t.Errorf("isNaN(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestIsInfHelper tests isInf helper function
func TestIsInfHelper(t *testing.T) {
	tests := []struct {
		input float64
		want  bool
	}{
		{0.0, false},
		{1.5, false},
		{math.NaN(), false},
		{math.Inf(1), true},
		{math.Inf(-1), true},
	}

	for _, tt := range tests {
		got := isInf(tt.input)
		if got != tt.want {
			t.Errorf("isInf(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
