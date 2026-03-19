// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// TestCountFuncNumArgs tests CountFunc.NumArgs
func TestCountFuncNumArgs(t *testing.T) {
	f := &CountFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestCountFuncCall tests CountFunc.Call which should return error
func TestCountFuncCall(t *testing.T) {
	f := &CountFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestCountStarFuncNumArgs tests CountStarFunc.NumArgs
func TestCountStarFuncNumArgs(t *testing.T) {
	f := &CountStarFunc{}
	if got := f.NumArgs(); got != 0 {
		t.Errorf("NumArgs() = %d, want 0", got)
	}
}

// TestCountStarFuncCall tests CountStarFunc.Call
func TestCountStarFuncCall(t *testing.T) {
	f := &CountStarFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestCountStarFuncStep tests CountStarFunc.Step
func TestCountStarFuncStep(t *testing.T) {
	f := &CountStarFunc{}

	// Count star should count all rows regardless of NULL
	err := f.Step([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	err = f.Step([]Value{NewIntValue(1)})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("Final() error = %v", err)
	}

	if result.AsInt64() != 2 {
		t.Errorf("Final() = %d, want 2", result.AsInt64())
	}
}

// TestCountStarFuncReset tests CountStarFunc.Reset
func TestCountStarFuncReset(t *testing.T) {
	f := &CountStarFunc{}
	f.count = 42
	f.Reset()
	if f.count != 0 {
		t.Errorf("Reset() did not clear count")
	}
}

// TestSumFuncNumArgs tests SumFunc.NumArgs
func TestSumFuncNumArgs(t *testing.T) {
	f := &SumFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestSumFuncCall tests SumFunc.Call
func TestSumFuncCall(t *testing.T) {
	f := &SumFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestSumFuncAddFloat tests SumFunc.addFloat
func TestSumFuncAddFloat(t *testing.T) {
	f := &SumFunc{}
	f.hasValue = true
	f.intSum = 10

	// Adding float should convert to float mode
	f.addFloat(3.14)

	if !f.isFloat {
		t.Error("addFloat() should set isFloat flag")
	}

	if f.floatSum != 13.14 {
		t.Errorf("floatSum = %f, want 13.14", f.floatSum)
	}
}

// TestSumFuncAddInteger tests SumFunc.addInteger
func TestSumFuncAddInteger(t *testing.T) {
	tests := []struct {
		name      string
		initial   int64
		add       int64
		wantFloat bool
	}{
		{
			name:      "no overflow",
			initial:   10,
			add:       20,
			wantFloat: false,
		},
		{
			name:      "positive overflow",
			initial:   9223372036854775807, // max int64
			add:       1,
			wantFloat: true,
		},
		{
			name:      "negative overflow",
			initial:   -9223372036854775808, // min int64
			add:       -1,
			wantFloat: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &SumFunc{}
			f.hasValue = true
			f.intSum = tt.initial
			f.addInteger(tt.add)

			if f.isFloat != tt.wantFloat {
				t.Errorf("isFloat = %v, want %v", f.isFloat, tt.wantFloat)
			}
		})
	}
}

// TestTotalFuncNumArgs tests TotalFunc.NumArgs
func TestTotalFuncNumArgs(t *testing.T) {
	f := &TotalFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestTotalFuncCall tests TotalFunc.Call
func TestTotalFuncCall(t *testing.T) {
	f := &TotalFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestTotalFuncStep tests TotalFunc.Step
func TestTotalFuncStep(t *testing.T) {
	f := &TotalFunc{}

	// Step with integer
	err := f.Step([]Value{NewIntValue(10)})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	// Step with float
	err = f.Step([]Value{NewFloatValue(3.14)})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	// Step with null (should be ignored)
	err = f.Step([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("Final() error = %v", err)
	}

	expected := 13.14
	if math.Abs(result.AsFloat64()-expected) > 0.001 {
		t.Errorf("Final() = %f, want %f", result.AsFloat64(), expected)
	}
}

// TestTotalFuncReset tests TotalFunc.Reset
func TestTotalFuncReset(t *testing.T) {
	f := &TotalFunc{}
	f.sum = 42.5
	f.Reset()
	if f.sum != 0.0 {
		t.Errorf("Reset() did not clear sum")
	}
}

// TestAvgFuncNumArgs tests AvgFunc.NumArgs
func TestAvgFuncNumArgs(t *testing.T) {
	f := &AvgFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestAvgFuncCall tests AvgFunc.Call
func TestAvgFuncCall(t *testing.T) {
	f := &AvgFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestMinFuncNumArgs tests MinFunc.NumArgs
func TestMinFuncNumArgs(t *testing.T) {
	f := &MinFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestMinFuncCall tests MinFunc.Call
func TestMinFuncCall(t *testing.T) {
	f := &MinFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestMaxFuncNumArgs tests MaxFunc.NumArgs
func TestMaxFuncNumArgs(t *testing.T) {
	f := &MaxFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

// TestMaxFuncCall tests MaxFunc.Call
func TestMaxFuncCall(t *testing.T) {
	f := &MaxFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestGroupConcatFuncNumArgs tests GroupConcatFunc.NumArgs
func TestGroupConcatFuncNumArgs(t *testing.T) {
	f := &GroupConcatFunc{}
	if got := f.NumArgs(); got != -1 {
		t.Errorf("NumArgs() = %d, want -1", got)
	}
}

// TestGroupConcatFuncCall tests GroupConcatFunc.Call
func TestGroupConcatFuncCall(t *testing.T) {
	f := &GroupConcatFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

// TestGroupConcatFuncStepWithNullSeparator tests group_concat with null separator
func TestGroupConcatFuncStepWithNullSeparator(t *testing.T) {
	f := &GroupConcatFunc{}

	// First call with null separator
	err := f.Step([]Value{NewTextValue("hello"), NewNullValue()})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	// Second call
	err = f.Step([]Value{NewTextValue("world")})
	if err != nil {
		t.Errorf("Step() error = %v", err)
	}

	result, err := f.Final()
	if err != nil {
		t.Errorf("Final() error = %v", err)
	}

	expected := "hello,world"
	if result.AsString() != expected {
		t.Errorf("Final() = %q, want %q", result.AsString(), expected)
	}
}

// TestMinScalarFunc tests minScalarFunc
func TestMinScalarFunc(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  int64
		wantErr  bool
	}{
		{
			name:    "no args",
			args:    []Value{},
			wantErr: true,
		},
		{
			name:     "all null",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:    "mixed values",
			args:    []Value{NewIntValue(5), NewIntValue(2), NewIntValue(8)},
			wantVal: 2,
		},
		{
			name:    "with null",
			args:    []Value{NewIntValue(5), NewNullValue(), NewIntValue(2)},
			wantVal: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := minScalarFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("minScalarFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("Expected null result")
				}
			} else {
				if result.AsInt64() != tt.wantVal {
					t.Errorf("minScalarFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
				}
			}
		})
	}
}

// TestMaxScalarFunc tests maxScalarFunc
func TestMaxScalarFunc(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
		wantVal  int64
		wantErr  bool
	}{
		{
			name:    "no args",
			args:    []Value{},
			wantErr: true,
		},
		{
			name:     "all null",
			args:     []Value{NewNullValue(), NewNullValue()},
			wantNull: true,
		},
		{
			name:    "mixed values",
			args:    []Value{NewIntValue(5), NewIntValue(2), NewIntValue(8)},
			wantVal: 8,
		},
		{
			name:    "with null",
			args:    []Value{NewIntValue(5), NewNullValue(), NewIntValue(8)},
			wantVal: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := maxScalarFunc(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("maxScalarFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("Expected null result")
				}
			} else {
				if result.AsInt64() != tt.wantVal {
					t.Errorf("maxScalarFunc() = %d, want %d", result.AsInt64(), tt.wantVal)
				}
			}
		})
	}
}

// TestIsNaN tests isNaN helper
func TestIsNaN(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  bool
	}{
		{
			name:  "NaN",
			value: math.NaN(),
			want:  true,
		},
		{
			name:  "normal number",
			value: 3.14,
			want:  false,
		},
		{
			name:  "infinity",
			value: math.Inf(1),
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNaN(tt.value); got != tt.want {
				t.Errorf("isNaN() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsInf tests isInf helper
func TestIsInf(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  bool
	}{
		{
			name:  "positive infinity",
			value: math.Inf(1),
			want:  true,
		},
		{
			name:  "negative infinity",
			value: math.Inf(-1),
			want:  true,
		},
		{
			name:  "normal number",
			value: 3.14,
			want:  false,
		},
		{
			name:  "NaN",
			value: math.NaN(),
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isInf(tt.value); got != tt.want {
				t.Errorf("isInf() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSumFuncIntegerOverflow tests SumFunc.addInteger overflow handling
func TestSumFuncIntegerOverflow(t *testing.T) {
	f := &SumFunc{hasValue: true}

	// Test positive overflow
	f.intSum = math.MaxInt64
	f.addInteger(1)
	if !f.isFloat {
		t.Error("addInteger() should convert to float on positive overflow")
	}

	// Reset and test negative overflow
	f = &SumFunc{hasValue: true}
	f.intSum = math.MinInt64
	f.addInteger(-1)
	if !f.isFloat {
		t.Error("addInteger() should convert to float on negative overflow")
	}
}
