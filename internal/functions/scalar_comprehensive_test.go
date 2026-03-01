// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"testing"
)

// TestScalarFuncNumArgs tests NumArgs method
func TestScalarFuncNumArgs(t *testing.T) {
	f := NewScalarFunc("test", 2, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})

	// NumArgs returns the configured number
	if got := f.NumArgs(); got != 2 {
		t.Errorf("NumArgs() = %d, want 2", got)
	}
}

// TestValueAsFloat64EdgeCases tests AsFloat64 edge cases
func TestValueAsFloat64EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want float64
	}{
		{
			name: "null value",
			val:  NewNullValue(),
			want: 0.0,
		},
		{
			name: "text value",
			val:  NewTextValue("3.14"),
			want: 3.14,
		},
		{
			name: "blob value",
			val:  NewBlobValue([]byte("1.5")),
			want: 0.0, // Can't convert blob to float
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.val.AsFloat64()
			if got != tt.want {
				t.Errorf("AsFloat64() = %f, want %f", got, tt.want)
			}
		})
	}
}

// TestValueAsStringEdgeCases tests AsString edge cases
func TestValueAsStringEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want string
	}{
		{
			name: "null value",
			val:  NewNullValue(),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.val.AsString()
			if got != tt.want {
				t.Errorf("AsString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Bytes method is tested through scalar functions that use it

// Helper functions are internal implementation details
// and are tested through the public API functions

// TestLtrimFunc tests ltrimFunc
func TestLtrimFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "trim spaces",
			args: []Value{NewTextValue("   hello")},
			want: "hello",
		},
		{
			name: "no leading spaces",
			args: []Value{NewTextValue("hello")},
			want: "hello",
		},
		{
			name: "custom chars",
			args: []Value{NewTextValue("xxhello"), NewTextValue("x")},
			want: "hello",
		},
		{
			name: "all spaces",
			args: []Value{NewTextValue("   ")},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ltrimFunc(tt.args)
			if err != nil {
				t.Errorf("ltrimFunc() error = %v", err)
				return
			}

			if result.AsString() != tt.want {
				t.Errorf("ltrimFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestRtrimFunc tests rtrimFunc
func TestRtrimFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "trim spaces",
			args: []Value{NewTextValue("hello   ")},
			want: "hello",
		},
		{
			name: "no trailing spaces",
			args: []Value{NewTextValue("hello")},
			want: "hello",
		},
		{
			name: "custom chars",
			args: []Value{NewTextValue("helloxx"), NewTextValue("x")},
			want: "hello",
		},
		{
			name: "all spaces",
			args: []Value{NewTextValue("   ")},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rtrimFunc(tt.args)
			if err != nil {
				t.Errorf("rtrimFunc() error = %v", err)
				return
			}

			if result.AsString() != tt.want {
				t.Errorf("rtrimFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestUnicodeFunc tests unicodeFunc
func TestUnicodeFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want int64
	}{
		{
			name: "ASCII char",
			args: []Value{NewTextValue("A")},
			want: 65,
		},
		{
			name: "Unicode char",
			args: []Value{NewTextValue("😀")},
			want: 128512,
		},
		{
			name: "empty string",
			args: []Value{NewTextValue("")},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unicodeFunc(tt.args)
			if err != nil {
				t.Errorf("unicodeFunc() error = %v", err)
				return
			}

			if result.AsInt64() != tt.want {
				t.Errorf("unicodeFunc() = %d, want %d", result.AsInt64(), tt.want)
			}
		})
	}
}

// TestCharFunc tests charFunc
func TestCharFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want string
	}{
		{
			name: "single char",
			args: []Value{NewIntValue(65)},
			want: "A",
		},
		{
			name: "multiple chars",
			args: []Value{NewIntValue(72), NewIntValue(101), NewIntValue(108), NewIntValue(108), NewIntValue(111)},
			want: "Hello",
		},
		{
			name: "unicode char",
			args: []Value{NewIntValue(128512)},
			want: "😀",
		},
		{
			name: "null arg",
			args: []Value{NewNullValue(), NewIntValue(65)},
			want: "A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := charFunc(tt.args)
			if err != nil {
				t.Errorf("charFunc() error = %v", err)
				return
			}

			if result.AsString() != tt.want {
				t.Errorf("charFunc() = %q, want %q", result.AsString(), tt.want)
			}
		})
	}
}

// TestIfnullFunc tests ifnullFunc
func TestIfnullFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want int64
	}{
		{
			name: "first null",
			args: []Value{NewNullValue(), NewIntValue(42)},
			want: 42,
		},
		{
			name: "first not null",
			args: []Value{NewIntValue(10), NewIntValue(42)},
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ifnullFunc(tt.args)
			if err != nil {
				t.Errorf("ifnullFunc() error = %v", err)
				return
			}

			if result.AsInt64() != tt.want {
				t.Errorf("ifnullFunc() = %d, want %d", result.AsInt64(), tt.want)
			}
		})
	}
}

// TestIifFunc tests iifFunc
func TestIifFunc(t *testing.T) {
	tests := []struct {
		name string
		args []Value
		want int64
	}{
		{
			name: "condition true",
			args: []Value{NewIntValue(1), NewIntValue(10), NewIntValue(20)},
			want: 10,
		},
		{
			name: "condition false",
			args: []Value{NewIntValue(0), NewIntValue(10), NewIntValue(20)},
			want: 20,
		},
		{
			name: "condition null",
			args: []Value{NewNullValue(), NewIntValue(10), NewIntValue(20)},
			want: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := iifFunc(tt.args)
			if err != nil {
				t.Errorf("iifFunc() error = %v", err)
				return
			}

			if result.AsInt64() != tt.want {
				t.Errorf("iifFunc() = %d, want %d", result.AsInt64(), tt.want)
			}
		})
	}
}

// TestZeroblobFunc tests zeroblobFunc
func TestZeroblobFunc(t *testing.T) {
	tests := []struct {
		name    string
		args    []Value
		wantLen int
	}{
		{
			name:    "valid size",
			args:    []Value{NewIntValue(10)},
			wantLen: 10,
		},
		{
			name:    "zero size",
			args:    []Value{NewIntValue(0)},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := zeroblobFunc(tt.args)
			if err != nil {
				t.Errorf("zeroblobFunc() error = %v", err)
				return
			}

			if result.Type() != TypeBlob {
				t.Errorf("zeroblobFunc() type = %v, want TypeBlob", result.Type())
			}

			blob := result.AsBlob()
			if len(blob) != tt.wantLen {
				t.Errorf("zeroblobFunc() length = %d, want %d", len(blob), tt.wantLen)
			}

			// Verify all bytes are zero
			for i, b := range blob {
				if b != 0 {
					t.Errorf("zeroblobFunc()[%d] = %d, want 0", i, b)
				}
			}
		})
	}
}

// TestNullCompare tests nullCompare helper
func TestNullCompare(t *testing.T) {
	tests := []struct {
		name      string
		a         Value
		b         Value
		wantCmp   int
		wantValid bool
	}{
		{
			name:      "both null",
			a:         NewNullValue(),
			b:         NewNullValue(),
			wantCmp:   0,
			wantValid: true,
		},
		{
			name:      "a null",
			a:         NewNullValue(),
			b:         NewIntValue(1),
			wantCmp:   -1,
			wantValid: true,
		},
		{
			name:      "b null",
			a:         NewIntValue(1),
			b:         NewNullValue(),
			wantCmp:   1,
			wantValid: true,
		},
		{
			name:      "neither null",
			a:         NewIntValue(1),
			b:         NewIntValue(2),
			wantCmp:   0,
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp, valid := nullCompare(tt.a, tt.b)
			if valid != tt.wantValid {
				t.Errorf("nullCompare() valid = %v, want %v", valid, tt.wantValid)
			}
			if valid && cmp != tt.wantCmp {
				t.Errorf("nullCompare() cmp = %d, want %d", cmp, tt.wantCmp)
			}
		})
	}
}

// TestCompareValues tests compareValues helper
func TestCompareValues(t *testing.T) {
	tests := []struct {
		name string
		a    Value
		b    Value
		want int
	}{
		{
			name: "int equal",
			a:    NewIntValue(5),
			b:    NewIntValue(5),
			want: 0,
		},
		{
			name: "int less",
			a:    NewIntValue(3),
			b:    NewIntValue(5),
			want: -1,
		},
		{
			name: "int greater",
			a:    NewIntValue(7),
			b:    NewIntValue(5),
			want: 1,
		},
		{
			name: "text equal",
			a:    NewTextValue("abc"),
			b:    NewTextValue("abc"),
			want: 0,
		},
		{
			name: "text less",
			a:    NewTextValue("abc"),
			b:    NewTextValue("def"),
			want: -1,
		},
		{
			name: "text greater",
			a:    NewTextValue("def"),
			b:    NewTextValue("abc"),
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareValues(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("compareValues() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestHelperFunctions tests utility helper functions
func TestIsDigit(t *testing.T) {
	tests := []struct {
		char rune
		want bool
	}{
		{'0', true},
		{'5', true},
		{'9', true},
		{'a', false},
		{' ', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.char), func(t *testing.T) {
			if got := isDigit(tt.char); got != tt.want {
				t.Errorf("isDigit(%c) = %v, want %v", tt.char, got, tt.want)
			}
		})
	}
}

func TestIsSpace(t *testing.T) {
	tests := []struct {
		char rune
		want bool
	}{
		{' ', true},
		{'\t', true},
		{'\n', true},
		{'a', false},
		{'0', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.char), func(t *testing.T) {
			if got := isSpace(tt.char); got != tt.want {
				t.Errorf("isSpace(%c) = %v, want %v", tt.char, got, tt.want)
			}
		})
	}
}

func TestAbsHelper(t *testing.T) {
	tests := []struct {
		value int64
		want  int64
	}{
		{5, 5},
		{-5, 5},
		{0, 0},
	}

	for _, tt := range tests {
		if got := abs(tt.value); got != tt.want {
			t.Errorf("abs(%d) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestFabsHelper(t *testing.T) {
	tests := []struct {
		value float64
		want  float64
	}{
		{5.5, 5.5},
		{-5.5, 5.5},
		{0.0, 0.0},
	}

	for _, tt := range tests {
		if got := fabs(tt.value); got != tt.want {
			t.Errorf("fabs(%f) = %f, want %f", tt.value, got, tt.want)
		}
	}
}

// filterIgnoredChars is tested through unhexFunc

// TestLengthFuncEdgeCases tests length function with various types
func TestLengthFuncEdgeCases(t *testing.T) {
	// Test with float
	result, err := lengthFunc([]Value{NewFloatValue(3.14)})
	if err != nil {
		t.Errorf("lengthFunc() with float error = %v", err)
	}
	if result.IsNull() {
		t.Error("lengthFunc() with float should not return NULL")
	}

	// Test with integer
	result, err = lengthFunc([]Value{NewIntValue(42)})
	if err != nil {
		t.Errorf("lengthFunc() with integer error = %v", err)
	}
	if result.IsNull() {
		t.Error("lengthFunc() with integer should not return NULL")
	}
}

// TestSubstrFuncBlobEdgeCases tests substr with blob edge cases
func TestSubstrFuncBlobEdgeCases(t *testing.T) {
	// Test with blob and various positions
	blob := NewBlobValue([]byte{1, 2, 3, 4, 5})

	result, err := substrFunc([]Value{blob, NewIntValue(2), NewIntValue(3)})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}
	if result.Type() != TypeBlob {
		t.Errorf("substrFunc() on blob should return blob, got %v", result.Type())
	}
}

// TestTrimFuncEdgeCases tests trim with custom characters
func TestTrimFuncEdgeCases(t *testing.T) {
	// Test with second argument (chars to trim)
	result, err := trimFunc([]Value{NewTextValue("xxhelloxx"), NewTextValue("x")})
	if err != nil {
		t.Errorf("trimFunc() error = %v", err)
	}
	if result.AsString() != "hello" {
		t.Errorf("trimFunc() = %q, want 'hello'", result.AsString())
	}
}

// TestQuoteFuncBlob tests quote with blob
func TestQuoteFuncBlob(t *testing.T) {
	result, err := quoteFunc([]Value{NewBlobValue([]byte{65, 66, 67})})
	if err != nil {
		t.Errorf("quoteFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("quoteFunc() should not return NULL for blob")
	}
}

// TestCompareValuesEdgeCases tests compareValues with mixed types
func TestCompareValuesEdgeCases(t *testing.T) {
	// Test through nullif which uses compareValues
	result, err := nullifFunc([]Value{NewIntValue(1), NewIntValue(2)})
	if err != nil {
		t.Errorf("nullifFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("nullifFunc() should not return NULL when values differ")
	}

	// Test with equal values
	result, err = nullifFunc([]Value{NewIntValue(5), NewIntValue(5)})
	if err != nil {
		t.Errorf("nullifFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("nullifFunc() should return NULL when values are equal")
	}
}
