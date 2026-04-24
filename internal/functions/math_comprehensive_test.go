// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// Comprehensive tests for math.go functions

func TestRegisterMathFunctions(t *testing.T) {
	r := NewRegistry()
	RegisterMathFunctions(r)

	// Check that functions are registered
	funcs := []string{
		"abs", "round", "random", "randomblob",
		"ceil", "ceiling", "floor", "sqrt", "power", "pow",
		"exp", "ln", "log", "log10", "log2",
		"sin", "cos", "tan", "asin", "acos", "atan", "atan2",
		"sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
		"sign", "mod", "pi", "radians", "degrees",
	}

	for _, name := range funcs {
		if _, ok := r.Lookup(name); !ok {
			t.Errorf("Function %s not registered", name)
		}
	}
}

func TestAbsFuncInteger(t *testing.T) {
	tests := []struct {
		input    Value
		expected int64
	}{
		{NewIntValue(5), 5},
		{NewIntValue(-5), 5},
		{NewIntValue(0), 0},
		{NewIntValue(100), 100},
		{NewIntValue(-100), 100},
	}

	for _, tt := range tests {
		result, err := absFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("absFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsInt64() != tt.expected {
			t.Errorf("absFunc(%v) = %d, want %d", tt.input, result.AsInt64(), tt.expected)
		}
	}
}

func TestAbsFuncFloat(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(5.5), 5.5},
		{NewFloatValue(-5.5), 5.5},
		{NewFloatValue(0.0), 0.0},
		{NewFloatValue(-0.0), 0.0},
	}

	for _, tt := range tests {
		result, err := absFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("absFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsFloat64() != tt.expected {
			t.Errorf("absFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestAbsFuncOverflow(t *testing.T) {
	// Test MinInt64 overflow
	input := NewIntValue(math.MinInt64)
	_, err := absFunc([]Value{input})
	if err == nil {
		t.Error("Expected overflow error for MinInt64")
	}
}

func TestAbsFuncNull(t *testing.T) {
	result, err := absFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("absFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL result")
	}
}

func TestRoundFuncBasic(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(5.5), 6.0},
		{NewFloatValue(5.4), 5.0},
		{NewFloatValue(-5.5), -6.0},
		{NewFloatValue(-5.4), -5.0},
	}

	for _, tt := range tests {
		result, err := roundFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("roundFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsFloat64() != tt.expected {
			t.Errorf("roundFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestRoundFuncWithPrecision(t *testing.T) {
	tests := []struct {
		input     Value
		precision Value
		expected  float64
	}{
		{NewFloatValue(5.555), NewIntValue(2), 5.56},
		{NewFloatValue(5.554), NewIntValue(2), 5.55},
		{NewFloatValue(123.456), NewIntValue(1), 123.5},
		{NewFloatValue(123.456), NewIntValue(0), 123.0},
	}

	for _, tt := range tests {
		result, err := roundFunc([]Value{tt.input, tt.precision})
		if err != nil {
			t.Errorf("roundFunc(%v, %v) error: %v", tt.input, tt.precision, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 0.01 {
			t.Errorf("roundFunc(%v, %v) = %f, want %f", tt.input, tt.precision, result.AsFloat64(), tt.expected)
		}
	}
}

func TestRoundFuncSpecialValues(t *testing.T) {
	tests := []struct {
		name  string
		input Value
	}{
		{"NaN", NewFloatValue(math.NaN())},
		{"Inf", NewFloatValue(math.Inf(1))},
		{"-Inf", NewFloatValue(math.Inf(-1))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := roundFunc([]Value{tt.input})
			if err != nil {
				t.Errorf("roundFunc(%s) error: %v", tt.name, err)
			}
			// Should return the same value
			if math.IsNaN(tt.input.AsFloat64()) && !math.IsNaN(result.AsFloat64()) {
				t.Error("Expected NaN result")
			}
			if math.IsInf(tt.input.AsFloat64(), 1) && !math.IsInf(result.AsFloat64(), 1) {
				t.Error("Expected +Inf result")
			}
		})
	}
}

func TestRandomFunc(t *testing.T) {
	// Generate multiple random values
	for i := 0; i < 10; i++ {
		result, err := randomFunc([]Value{})
		if err != nil {
			t.Errorf("randomFunc() error: %v", err)
		}
		if result.Type() != TypeInteger {
			t.Error("Expected integer result")
		}
	}
}

func TestRandomBlobFunc(t *testing.T) {
	tests := []int64{1, 10, 100, 1000}

	for _, n := range tests {
		result, err := randomblobFunc([]Value{NewIntValue(n)})
		if err != nil {
			t.Errorf("randomblobFunc(%d) error: %v", n, err)
			continue
		}
		if result.Type() != TypeBlob {
			t.Error("Expected blob result")
		}
		blob := result.AsBlob()
		if int64(len(blob)) != n {
			t.Errorf("Expected blob of size %d, got %d", n, len(blob))
		}
	}
}

func TestRandomBlobFuncNull(t *testing.T) {
	result, err := randomblobFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("randomblobFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL result")
	}
}

func TestCeilFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(5.1), 6.0},
		{NewFloatValue(5.9), 6.0},
		{NewFloatValue(-5.1), -5.0},
		{NewFloatValue(-5.9), -5.0},
		{NewFloatValue(5.0), 5.0},
	}

	for _, tt := range tests {
		result, err := ceilFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("ceilFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsFloat64() != tt.expected {
			t.Errorf("ceilFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestFloorFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(5.1), 5.0},
		{NewFloatValue(5.9), 5.0},
		{NewFloatValue(-5.1), -6.0},
		{NewFloatValue(-5.9), -6.0},
		{NewFloatValue(5.0), 5.0},
	}

	for _, tt := range tests {
		result, err := floorFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("floorFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsFloat64() != tt.expected {
			t.Errorf("floorFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestSqrtFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(4.0), 2.0},
		{NewFloatValue(9.0), 3.0},
		{NewFloatValue(0.0), 0.0},
		{NewFloatValue(2.0), math.Sqrt(2.0)},
	}

	for _, tt := range tests {
		result, err := sqrtFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("sqrtFunc(%v) error: %v", tt.input, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("sqrtFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestSqrtFuncNegative(t *testing.T) {
	result, err := sqrtFunc([]Value{NewFloatValue(-1.0)})
	if err != nil {
		t.Errorf("sqrtFunc(-1) error: %v", err)
	}
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for sqrt of negative number")
	}
}

func TestPowerFunc(t *testing.T) {
	tests := []struct {
		base     Value
		exponent Value
		expected float64
	}{
		{NewFloatValue(2.0), NewFloatValue(3.0), 8.0},
		{NewFloatValue(10.0), NewFloatValue(2.0), 100.0},
		{NewFloatValue(2.0), NewFloatValue(0.0), 1.0},
		{NewFloatValue(5.0), NewFloatValue(-1.0), 0.2},
	}

	for _, tt := range tests {
		result, err := powerFunc([]Value{tt.base, tt.exponent})
		if err != nil {
			t.Errorf("powerFunc(%v, %v) error: %v", tt.base, tt.exponent, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("powerFunc(%v, %v) = %f, want %f", tt.base, tt.exponent, result.AsFloat64(), tt.expected)
		}
	}
}

func TestExpFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(0.0), 1.0},
		{NewFloatValue(1.0), math.E},
		{NewFloatValue(2.0), math.Exp(2.0)},
	}

	for _, tt := range tests {
		result, err := expFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("expFunc(%v) error: %v", tt.input, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("expFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestLnFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(1.0), 0.0},
		{NewFloatValue(math.E), 1.0},
		{NewFloatValue(math.E * math.E), 2.0},
	}

	for _, tt := range tests {
		result, err := lnFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("lnFunc(%v) error: %v", tt.input, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("lnFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestLnFuncNegative(t *testing.T) {
	result, err := lnFunc([]Value{NewFloatValue(-1.0)})
	if err != nil {
		t.Errorf("lnFunc(-1) error: %v", err)
	}
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for ln of negative number")
	}
}

func TestLog10Func(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(10.0), 1.0},
		{NewFloatValue(100.0), 2.0},
		{NewFloatValue(1000.0), 3.0},
	}

	for _, tt := range tests {
		result, err := log10Func([]Value{tt.input})
		if err != nil {
			t.Errorf("log10Func(%v) error: %v", tt.input, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("log10Func(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestLog2Func(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
	}{
		{NewFloatValue(2.0), 1.0},
		{NewFloatValue(4.0), 2.0},
		{NewFloatValue(8.0), 3.0},
	}

	for _, tt := range tests {
		result, err := log2Func([]Value{tt.input})
		if err != nil {
			t.Errorf("log2Func(%v) error: %v", tt.input, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("log2Func(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
		}
	}
}

func TestTrigFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       func([]Value) (Value, error)
		input    Value
		expected float64
	}{
		{"sin(0)", sinFunc, NewFloatValue(0.0), 0.0},
		{"sin(π/2)", sinFunc, NewFloatValue(math.Pi / 2), 1.0},
		{"cos(0)", cosFunc, NewFloatValue(0.0), 1.0},
		{"cos(π)", cosFunc, NewFloatValue(math.Pi), -1.0},
		{"tan(0)", tanFunc, NewFloatValue(0.0), 0.0},
		{"tan(π/4)", tanFunc, NewFloatValue(math.Pi / 4), 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn([]Value{tt.input})
			if err != nil {
				t.Errorf("%s error: %v", tt.name, err)
				return
			}
			if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
				t.Errorf("%s = %f, want %f", tt.name, result.AsFloat64(), tt.expected)
			}
		})
	}
}

func TestAsinFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
		isNaN    bool
	}{
		{NewFloatValue(0.0), 0.0, false},
		{NewFloatValue(1.0), math.Pi / 2, false},
		{NewFloatValue(-1.0), -math.Pi / 2, false},
		{NewFloatValue(2.0), 0.0, true}, // Out of range
	}

	for _, tt := range tests {
		result, err := asinFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("asinFunc(%v) error: %v", tt.input, err)
			continue
		}
		if tt.isNaN {
			if !math.IsNaN(result.AsFloat64()) {
				t.Errorf("asinFunc(%v) should be NaN", tt.input)
			}
		} else {
			if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
				t.Errorf("asinFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
			}
		}
	}
}

func TestAcosFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
		isNaN    bool
	}{
		{NewFloatValue(1.0), 0.0, false},
		{NewFloatValue(0.0), math.Pi / 2, false},
		{NewFloatValue(-1.0), math.Pi, false},
		{NewFloatValue(2.0), 0.0, true}, // Out of range
	}

	for _, tt := range tests {
		result, err := acosFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("acosFunc(%v) error: %v", tt.input, err)
			continue
		}
		if tt.isNaN {
			if !math.IsNaN(result.AsFloat64()) {
				t.Errorf("acosFunc(%v) should be NaN", tt.input)
			}
		} else {
			if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
				t.Errorf("acosFunc(%v) = %f, want %f", tt.input, result.AsFloat64(), tt.expected)
			}
		}
	}
}

func TestAtanFunc(t *testing.T) {
	result, err := atanFunc([]Value{NewFloatValue(1.0)})
	if err != nil {
		t.Errorf("atanFunc(1) error: %v", err)
	}
	expected := math.Pi / 4
	if math.Abs(result.AsFloat64()-expected) > 1e-10 {
		t.Errorf("atanFunc(1) = %f, want %f", result.AsFloat64(), expected)
	}
}

func TestAtan2Func(t *testing.T) {
	result, err := atan2Func([]Value{NewFloatValue(1.0), NewFloatValue(1.0)})
	if err != nil {
		t.Errorf("atan2Func(1, 1) error: %v", err)
	}
	expected := math.Pi / 4
	if math.Abs(result.AsFloat64()-expected) > 1e-10 {
		t.Errorf("atan2Func(1, 1) = %f, want %f", result.AsFloat64(), expected)
	}
}

func TestHyperbolicFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   func([]Value) (Value, error)
	}{
		{"sinh", sinhFunc},
		{"cosh", coshFunc},
		{"tanh", tanhFunc},
		{"asinh", asinhFunc},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn([]Value{NewFloatValue(0.5)})
			if err != nil {
				t.Errorf("%s(0.5) error: %v", tt.name, err)
			}
			if result.Type() != TypeFloat {
				t.Errorf("%s should return float", tt.name)
			}
		})
	}
}

func TestAcoshFunc(t *testing.T) {
	tests := []struct {
		input Value
		isNaN bool
	}{
		{NewFloatValue(1.0), false},
		{NewFloatValue(2.0), false},
		{NewFloatValue(0.5), true}, // Out of range
	}

	for _, tt := range tests {
		result, err := acoshFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("acoshFunc(%v) error: %v", tt.input, err)
			continue
		}
		if tt.isNaN && !math.IsNaN(result.AsFloat64()) {
			t.Errorf("acoshFunc(%v) should be NaN", tt.input)
		}
	}
}

func TestAtanhFunc(t *testing.T) {
	tests := []struct {
		input Value
		isNaN bool
	}{
		{NewFloatValue(0.0), false},
		{NewFloatValue(0.5), false},
		{NewFloatValue(1.0), true},  // Out of range
		{NewFloatValue(-1.0), true}, // Out of range
	}

	for _, tt := range tests {
		result, err := atanhFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("atanhFunc(%v) error: %v", tt.input, err)
			continue
		}
		if tt.isNaN && !math.IsNaN(result.AsFloat64()) {
			t.Errorf("atanhFunc(%v) should be NaN", tt.input)
		}
	}
}

func TestSignFunc(t *testing.T) {
	tests := []struct {
		input    Value
		expected int64
	}{
		{NewFloatValue(5.0), 1},
		{NewFloatValue(-5.0), -1},
		{NewFloatValue(0.0), 0},
		{NewIntValue(100), 1},
		{NewIntValue(-100), -1},
	}

	for _, tt := range tests {
		result, err := signFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("signFunc(%v) error: %v", tt.input, err)
			continue
		}
		if result.AsInt64() != tt.expected {
			t.Errorf("signFunc(%v) = %d, want %d", tt.input, result.AsInt64(), tt.expected)
		}
	}
}

func TestModFunc(t *testing.T) {
	tests := []struct {
		x        Value
		y        Value
		expected int64
		isNull   bool
	}{
		{NewIntValue(10), NewIntValue(3), 1, false},
		{NewIntValue(10), NewIntValue(5), 0, false},
		{NewIntValue(-10), NewIntValue(3), -1, false},
		{NewIntValue(10), NewIntValue(0), 0, true}, // Division by zero
	}

	for _, tt := range tests {
		result, err := modFunc([]Value{tt.x, tt.y})
		if err != nil {
			t.Errorf("modFunc(%v, %v) error: %v", tt.x, tt.y, err)
			continue
		}
		if tt.isNull {
			if !result.IsNull() {
				t.Errorf("modFunc(%v, %v) should be NULL", tt.x, tt.y)
			}
		} else {
			if result.AsInt64() != tt.expected {
				t.Errorf("modFunc(%v, %v) = %d, want %d", tt.x, tt.y, result.AsInt64(), tt.expected)
			}
		}
	}
}

func TestPiFunc(t *testing.T) {
	result, err := piFunc([]Value{})
	if err != nil {
		t.Errorf("piFunc() error: %v", err)
	}
	if math.Abs(result.AsFloat64()-math.Pi) > 1e-10 {
		t.Errorf("piFunc() = %f, want %f", result.AsFloat64(), math.Pi)
	}
}

func TestRadiansFunc(t *testing.T) {
	tests := []struct {
		degrees  Value
		expected float64
	}{
		{NewFloatValue(0.0), 0.0},
		{NewFloatValue(180.0), math.Pi},
		{NewFloatValue(90.0), math.Pi / 2},
	}

	for _, tt := range tests {
		result, err := radiansFunc([]Value{tt.degrees})
		if err != nil {
			t.Errorf("radiansFunc(%v) error: %v", tt.degrees, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("radiansFunc(%v) = %f, want %f", tt.degrees, result.AsFloat64(), tt.expected)
		}
	}
}

func TestDegreesFunc(t *testing.T) {
	tests := []struct {
		radians  Value
		expected float64
	}{
		{NewFloatValue(0.0), 0.0},
		{NewFloatValue(math.Pi), 180.0},
		{NewFloatValue(math.Pi / 2), 90.0},
	}

	for _, tt := range tests {
		result, err := degreesFunc([]Value{tt.radians})
		if err != nil {
			t.Errorf("degreesFunc(%v) error: %v", tt.radians, err)
			continue
		}
		if math.Abs(result.AsFloat64()-tt.expected) > 1e-10 {
			t.Errorf("degreesFunc(%v) = %f, want %f", tt.radians, result.AsFloat64(), tt.expected)
		}
	}
}

// Test helper functions
func TestRoundParsePrecision(t *testing.T) {
	tests := []struct {
		name      string
		args      []Value
		expectP   int64
		expectOK  bool
		expectErr bool
	}{
		{"one arg", []Value{NewFloatValue(5.5)}, 0, true, false},
		{"two args", []Value{NewFloatValue(5.5), NewIntValue(2)}, 2, true, false},
		{"null precision", []Value{NewFloatValue(5.5), NewNullValue()}, 0, false, false},
		{"large precision", []Value{NewFloatValue(5.5), NewIntValue(50)}, 30, true, false},
		{"negative precision", []Value{NewFloatValue(5.5), NewIntValue(-5)}, 0, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, ok, err := roundParsePrecision(tt.args)
			if (err != nil) != tt.expectErr {
				t.Errorf("Expected error=%v, got %v", tt.expectErr, err)
			}
			if ok != tt.expectOK {
				t.Errorf("Expected ok=%v, got %v", tt.expectOK, ok)
			}
			if ok && p != tt.expectP {
				t.Errorf("Expected precision=%d, got %d", tt.expectP, p)
			}
		})
	}
}

func TestRoundIsPassthrough(t *testing.T) {
	tests := []struct {
		value    float64
		expected bool
	}{
		{math.NaN(), true},
		{math.Inf(1), true},
		{math.Inf(-1), true},
		{4503599627370496.0, true},
		{123.456, false},
	}

	for _, tt := range tests {
		result := roundIsPassthrough(tt.value)
		if result != tt.expected {
			t.Errorf("roundIsPassthrough(%v) = %v, want %v", tt.value, result, tt.expected)
		}
	}
}

func TestRoundToIntValue(t *testing.T) {
	tests := []struct {
		rounded      float64
		expectedType ValueType
	}{
		{5.0, TypeInteger},
		{100.0, TypeInteger},
		{float64(math.MaxInt64), TypeInteger},
		{float64(math.MaxInt64) + 1e10, TypeFloat},
	}

	for _, tt := range tests {
		result := roundToIntValue(tt.rounded)
		if result.Type() != tt.expectedType {
			t.Errorf("roundToIntValue(%v) type = %v, want %v", tt.rounded, result.Type(), tt.expectedType)
		}
	}
}

// Test NULL handling for all math functions
func TestMathFunctionsNullHandling(t *testing.T) {
	tests := []struct {
		name string
		fn   func([]Value) (Value, error)
		args []Value
	}{
		{"ceil", ceilFunc, []Value{NewNullValue()}},
		{"floor", floorFunc, []Value{NewNullValue()}},
		{"exp", expFunc, []Value{NewNullValue()}},
		{"log10", log10Func, []Value{NewNullValue()}},
		{"log2", log2Func, []Value{NewNullValue()}},
		{"sin", sinFunc, []Value{NewNullValue()}},
		{"cos", cosFunc, []Value{NewNullValue()}},
		{"tan", tanFunc, []Value{NewNullValue()}},
		{"atan", atanFunc, []Value{NewNullValue()}},
		{"sinh", sinhFunc, []Value{NewNullValue()}},
		{"cosh", coshFunc, []Value{NewNullValue()}},
		{"tanh", tanhFunc, []Value{NewNullValue()}},
		{"asinh", asinhFunc, []Value{NewNullValue()}},
		{"power", powerFunc, []Value{NewNullValue(), NewFloatValue(2.0)}},
		{"power_null_exp", powerFunc, []Value{NewFloatValue(2.0), NewNullValue()}},
		{"atan2", atan2Func, []Value{NewNullValue(), NewFloatValue(1.0)}},
		{"atan2_null_x", atan2Func, []Value{NewFloatValue(1.0), NewNullValue()}},
		{"radians", radiansFunc, []Value{NewNullValue()}},
		{"degrees", degreesFunc, []Value{NewNullValue()}},
		{"mod", modFunc, []Value{NewNullValue(), NewIntValue(5)}},
		{"mod_null_y", modFunc, []Value{NewIntValue(10), NewNullValue()}},
		{"sign_null", signFunc, []Value{NewNullValue()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if err != nil {
				t.Errorf("%s error: %v", tt.name, err)
				return
			}
			if !result.IsNull() {
				t.Errorf("%s should return NULL for null input, got %v", tt.name, result)
			}
		})
	}
}

// Test edge cases for log functions with invalid inputs
func TestLogFunctionsInvalidInputs(t *testing.T) {
	tests := []struct {
		name  string
		fn    func([]Value) (Value, error)
		input Value
	}{
		{"log10_zero", log10Func, NewFloatValue(0.0)},
		{"log10_negative", log10Func, NewFloatValue(-1.0)},
		{"log2_zero", log2Func, NewFloatValue(0.0)},
		{"log2_negative", log2Func, NewFloatValue(-1.0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn([]Value{tt.input})
			if err != nil {
				t.Errorf("%s error: %v", tt.name, err)
				return
			}
			if !math.IsNaN(result.AsFloat64()) {
				t.Errorf("%s should return NaN for invalid input, got %v", tt.name, result.AsFloat64())
			}
		})
	}
}

// Test atanh edge cases for values at boundaries
func TestAtanhBoundaries(t *testing.T) {
	tests := []struct {
		input    Value
		expected float64
		isInf    bool
	}{
		{NewFloatValue(0.9999), 0.0, false}, // Valid value
		{NewFloatValue(2.0), 0.0, true},     // Out of range
	}

	for _, tt := range tests {
		result, err := atanhFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("atanhFunc(%v) error: %v", tt.input, err)
			continue
		}
		if tt.isInf {
			if !math.IsNaN(result.AsFloat64()) && !math.IsInf(result.AsFloat64(), 0) {
				t.Errorf("atanhFunc(%v) should be NaN or Inf for out of range value", tt.input)
			}
		}
	}
}

// TestAbsFuncText tests abs with text value
func TestAbsFuncText(t *testing.T) {
	result, err := absFunc([]Value{NewTextValue("-42")})
	if err != nil {
		t.Errorf("absFunc() with text error: %v", err)
	}
	if result.AsInt64() != 42 {
		t.Errorf("absFunc('-42') = %d, want 42", result.AsInt64())
	}
}

// TestRoundFuncText tests round with text value
func TestRoundFuncText(t *testing.T) {
	result, err := roundFunc([]Value{NewTextValue("5.5")})
	if err != nil {
		t.Errorf("roundFunc() with text error: %v", err)
	}
	if result.AsFloat64() != 6.0 {
		t.Errorf("roundFunc('5.5') = %f, want 6.0", result.AsFloat64())
	}
}

// TestSqrtFuncText tests sqrt with text value
func TestSqrtFuncText(t *testing.T) {
	result, err := sqrtFunc([]Value{NewTextValue("4")})
	if err != nil {
		t.Errorf("sqrtFunc() with text error: %v", err)
	}
	if result.AsFloat64() != 2.0 {
		t.Errorf("sqrtFunc('4') = %f, want 2.0", result.AsFloat64())
	}
}

// TestLnFuncText tests ln with text value
func TestLnFuncText(t *testing.T) {
	result, err := lnFunc([]Value{NewTextValue("2.718281828")})
	if err != nil {
		t.Errorf("lnFunc() with text error: %v", err)
	}
	expected := math.Log(2.718281828)
	if math.Abs(result.AsFloat64()-expected) > 0.01 {
		t.Errorf("lnFunc('2.718281828') = %f, want %f", result.AsFloat64(), expected)
	}
}

// TestTrigFuncText tests trig functions with text
func TestTrigFuncText(t *testing.T) {
	r1, e1 := asinFunc([]Value{NewTextValue("0.5")})
	assertTrigResult(t, "asinFunc", r1, e1)

	r2, e2 := acosFunc([]Value{NewTextValue("0.5")})
	assertTrigResult(t, "acosFunc", r2, e2)

	r3, e3 := acoshFunc([]Value{NewTextValue("2")})
	assertTrigResult(t, "acoshFunc", r3, e3)

	r4, e4 := atanhFunc([]Value{NewTextValue("0.5")})
	assertTrigResult(t, "atanhFunc", r4, e4)
}

// TestRandomBlobFuncZero tests randomblob with zero (becomes 1)
func TestRandomBlobFuncZero(t *testing.T) {
	result, err := randomblobFunc([]Value{NewIntValue(0)})
	if err != nil {
		t.Errorf("randomblobFunc(0) error: %v", err)
	}
	if result.Type() != TypeBlob {
		t.Error("randomblobFunc(0) should return blob")
	}
	blob := result.AsBlob()
	if len(blob) != 1 {
		t.Errorf("randomblobFunc(0) = blob of size %d, want 1", len(blob))
	}
}

// TestRandomFuncError tests random function error path
func TestRandomFuncError(t *testing.T) {
	// Random function takes no args, just test it executes
	result, err := randomFunc([]Value{})
	if err != nil {
		t.Errorf("randomFunc() error: %v", err)
	}
	if result.Type() != TypeInteger {
		t.Error("randomFunc() should return integer")
	}
}

// TestRoundParsePrecisionText tests roundParsePrecision with text precision
func TestRoundParsePrecisionText(t *testing.T) {
	_, ok, err := roundParsePrecision([]Value{NewFloatValue(5.5), NewTextValue("2")})
	if err != nil {
		t.Errorf("roundParsePrecision() with text precision error: %v", err)
	}
	if !ok {
		t.Error("roundParsePrecision() with text precision should succeed")
	}
}
