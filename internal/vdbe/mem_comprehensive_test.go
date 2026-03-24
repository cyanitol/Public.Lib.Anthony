// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// TestMemGetFlags tests the GetFlags method
func TestMemGetFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		setup    func() *Mem
		expected MemFlags
	}{
		{
			name:     "null mem",
			setup:    func() *Mem { return NewMemNull() },
			expected: MemNull,
		},
		{
			name:     "int mem",
			setup:    func() *Mem { return NewMemInt(42) },
			expected: MemInt,
		},
		{
			name:     "real mem",
			setup:    func() *Mem { return NewMemReal(3.14) },
			expected: MemReal,
		},
		{
			name:     "string mem",
			setup:    func() *Mem { return NewMemStr("test") },
			expected: MemStr | MemTerm,
		},
		{
			name:     "blob mem",
			setup:    func() *Mem { return NewMemBlob([]byte{1, 2, 3}) },
			expected: MemBlob,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := tt.setup()
			if got := m.GetFlags(); got != tt.expected {
				t.Errorf("GetFlags() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestMemStringValue tests the StringValue method
func TestMemStringValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mem      *Mem
		expected string
	}{
		{
			name:     "null",
			mem:      NewMemNull(),
			expected: "",
		},
		{
			name:     "string",
			mem:      NewMemStr("hello"),
			expected: "hello",
		},
		{
			name:     "int",
			mem:      NewMemInt(123),
			expected: "123",
		},
		{
			name:     "real",
			mem:      NewMemReal(3.14),
			expected: "3.14",
		},
		{
			name:     "blob",
			mem:      NewMemBlob([]byte("blob")),
			expected: "blob",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.mem.StringValue()
			if got != tt.expected {
				t.Errorf("StringValue() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// assertValueEquals checks that got matches expected for any supported type.
func assertValueEquals(t *testing.T, got, expected interface{}) {
	t.Helper()
	if expected == nil {
		if got != nil {
			t.Errorf("Value() = %v, want nil", got)
		}
		return
	}
	assertNonNilValueEquals(t, got, expected)
}

func assertNonNilValueEquals(t *testing.T, got, expected interface{}) {
	t.Helper()
	if !valuesMatchComprehensive(got, expected) {
		t.Errorf("Value() = %v (%T), want %v (%T)", got, got, expected, expected)
	}
}

func valuesMatchComprehensive(got, expected interface{}) bool {
	switch v := expected.(type) {
	case int64:
		gotInt, ok := got.(int64)
		return ok && gotInt == v
	case float64:
		gotFloat, ok := got.(float64)
		return ok && gotFloat == v
	case string:
		gotStr, ok := got.(string)
		return ok && gotStr == v
	case []byte:
		gotBytes, ok := got.([]byte)
		return ok && bytesMatchComprehensive(gotBytes, v)
	}
	return false
}

func bytesMatchComprehensive(got, expected []byte) bool {
	if len(got) != len(expected) {
		return false
	}
	for i, b := range got {
		if b != expected[i] {
			return false
		}
	}
	return true
}

// TestMemValue tests the Value method
func TestMemValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mem      *Mem
		expected interface{}
	}{
		{"null", NewMemNull(), nil},
		{"int", NewMemInt(42), int64(42)},
		{"real", NewMemReal(3.14), float64(3.14)},
		{"string", NewMemStr("test"), "test"},
		{"blob", NewMemBlob([]byte{1, 2, 3}), []byte{1, 2, 3}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertValueEquals(t, tt.mem.Value(), tt.expected)
		})
	}
}

// TestMemNumerify tests the Numerify method
func TestMemNumerify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		setup     func() *Mem
		wantInt   bool
		wantReal  bool
		wantValue interface{}
		wantErr   bool
	}{
		{
			name:      "null to int",
			setup:     func() *Mem { return NewMemNull() },
			wantInt:   true,
			wantValue: int64(0),
		},
		{
			name:      "already int",
			setup:     func() *Mem { return NewMemInt(42) },
			wantInt:   true,
			wantValue: int64(42),
		},
		{
			name:      "already real",
			setup:     func() *Mem { return NewMemReal(3.14) },
			wantReal:  true,
			wantValue: float64(3.14),
		},
		{
			name:      "string to int",
			setup:     func() *Mem { return NewMemStr("123") },
			wantInt:   true,
			wantValue: int64(123),
		},
		{
			name:      "string to real",
			setup:     func() *Mem { return NewMemStr("3.14") },
			wantReal:  true,
			wantValue: float64(3.14),
		},
		{
			name:    "invalid string",
			setup:   func() *Mem { return NewMemStr("not a number") },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := tt.setup()
			err := m.Numerify()
			if (err != nil) != tt.wantErr {
				t.Errorf("Numerify() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			checkNumerifyResult(t, m, tt.wantInt, tt.wantReal, tt.wantValue)
		})
	}
}

func checkNumerifyResult(t *testing.T, m *Mem, wantInt, wantReal bool, wantValue interface{}) {
	t.Helper()
	if wantInt && !m.IsInt() {
		t.Errorf("Expected int flag after Numerify()")
	}
	if wantReal && !m.IsReal() {
		t.Errorf("Expected real flag after Numerify()")
	}
	if wantInt {
		if got := m.IntValue(); got != wantValue.(int64) {
			t.Errorf("IntValue() = %v, want %v", got, wantValue)
		}
	}
	if wantReal {
		if got := m.RealValue(); got != wantValue.(float64) {
			t.Errorf("RealValue() = %v, want %v", got, wantValue)
		}
	}
}

// TestMemApplyAffinity tests the ApplyAffinity method
func TestMemApplyAffinity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mem      *Mem
		affinity byte
		wantErr  bool
		check    func(*Mem) bool
	}{
		{
			name:     "affinity A (none/blob)",
			mem:      NewMemInt(42),
			affinity: 'A',
			check:    func(m *Mem) bool { return m.IsInt() },
		},
		{
			name:     "affinity B (text)",
			mem:      NewMemInt(42),
			affinity: 'B',
			check:    func(m *Mem) bool { return m.IsStr() },
		},
		{
			name:     "affinity C (numeric)",
			mem:      NewMemStr("123"),
			affinity: 'C',
			check:    func(m *Mem) bool { return m.IsNumeric() },
		},
		{
			name:     "affinity D (integer)",
			mem:      NewMemReal(3.14),
			affinity: 'D',
			check:    func(m *Mem) bool { return m.IsInt() },
		},
		{
			name:     "affinity E (real)",
			mem:      NewMemInt(42),
			affinity: 'E',
			check:    func(m *Mem) bool { return m.IsReal() },
		},
		{
			name:     "unknown affinity",
			mem:      NewMemInt(42),
			affinity: 'Z',
			check:    func(m *Mem) bool { return m.IsInt() },
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.mem.ApplyAffinity(tt.affinity)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyAffinity() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.check(tt.mem) {
				t.Errorf("ApplyAffinity() did not apply expected affinity")
			}
		})
	}
}

// TestMemString tests the String method for debugging
func TestMemString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mem  *Mem
		want string
	}{
		{
			name: "null",
			mem:  NewMemNull(),
			want: "NULL",
		},
		{
			name: "int",
			mem:  NewMemInt(42),
			want: "INT(42)",
		},
		{
			name: "real",
			mem:  NewMemReal(3.14),
			want: "REAL(3.14)",
		},
		{
			name: "string",
			mem:  NewMemStr("test"),
			want: `STR("test")`,
		},
		{
			name: "blob",
			mem:  NewMemBlob([]byte{1, 2, 3}),
			want: "BLOB(3 bytes)",
		},
		{
			name: "undefined",
			mem:  &Mem{flags: MemUndefined},
			want: "UNDEFINED",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.mem.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMemCompareWithDirection tests the CompareWithDirection method
func TestMemCompareWithDirection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		m1        *Mem
		m2        *Mem
		direction int
		want      int
	}{
		{
			name:      "ASC 1 < 2",
			m1:        NewMemInt(1),
			m2:        NewMemInt(2),
			direction: 0,
			want:      -1,
		},
		{
			name:      "DESC 1 < 2",
			m1:        NewMemInt(1),
			m2:        NewMemInt(2),
			direction: 1,
			want:      1,
		},
		{
			name:      "ASC 2 > 1",
			m1:        NewMemInt(2),
			m2:        NewMemInt(1),
			direction: 0,
			want:      1,
		},
		{
			name:      "DESC 2 > 1",
			m1:        NewMemInt(2),
			m2:        NewMemInt(1),
			direction: 1,
			want:      -1,
		},
		{
			name:      "ASC equal",
			m1:        NewMemInt(5),
			m2:        NewMemInt(5),
			direction: 0,
			want:      0,
		},
		{
			name:      "DESC equal",
			m1:        NewMemInt(5),
			m2:        NewMemInt(5),
			direction: 1,
			want:      0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.m1.CompareWithDirection(tt.m2, tt.direction)
			if got != tt.want {
				t.Errorf("CompareWithDirection() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMemCompareStrings tests the compareStrings helper
func TestMemCompareStrings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m1   *Mem
		m2   *Mem
		want int
	}{
		{
			name: "equal strings",
			m1:   NewMemStr("abc"),
			m2:   NewMemStr("abc"),
			want: 0,
		},
		{
			name: "first less",
			m1:   NewMemStr("abc"),
			m2:   NewMemStr("def"),
			want: -1,
		},
		{
			name: "first greater",
			m1:   NewMemStr("def"),
			m2:   NewMemStr("abc"),
			want: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compareStrings(tt.m1, tt.m2)
			if got != tt.want {
				t.Errorf("compareStrings() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCompareNumericWithText tests numeric vs text comparison
func TestCompareNumericWithText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		numericVal float64
		textBytes  []byte
		wantResult int
		wantOk     bool
	}{
		{
			name:       "numeric < text-as-number",
			numericVal: 5.0,
			textBytes:  []byte("10"),
			wantResult: -1,
			wantOk:     true,
		},
		{
			name:       "numeric > text-as-number",
			numericVal: 10.0,
			textBytes:  []byte("5"),
			wantResult: 1,
			wantOk:     true,
		},
		{
			name:       "numeric = text-as-number",
			numericVal: 5.0,
			textBytes:  []byte("5"),
			wantResult: 0,
			wantOk:     true,
		},
		{
			name:       "text not numeric",
			numericVal: 5.0,
			textBytes:  []byte("not a number"),
			wantResult: 0,
			wantOk:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, ok := compareNumericWithText(tt.numericVal, tt.textBytes)
			if ok != tt.wantOk {
				t.Errorf("compareNumericWithText() ok = %v, want %v", ok, tt.wantOk)
			}
			if ok && result != tt.wantResult {
				t.Errorf("compareNumericWithText() result = %v, want %v", result, tt.wantResult)
			}
		})
	}
}

// TestCompareMixedNumericText tests the compareMixedNumericText helper
func TestCompareMixedNumericText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		m           *Mem
		other       *Mem
		mIsNumeric  bool
		mIsText     bool
		wantResult  int
		wantHandled bool
	}{
		{
			name:        "numeric < text-as-number",
			m:           NewMemInt(5),
			other:       NewMemStr("10"),
			mIsNumeric:  true,
			mIsText:     false,
			wantResult:  -1,
			wantHandled: true,
		},
		{
			name:        "numeric vs non-numeric text",
			m:           NewMemInt(5),
			other:       NewMemStr("not a number"),
			mIsNumeric:  true,
			mIsText:     false,
			wantResult:  -1,
			wantHandled: true,
		},
		{
			name:        "text-as-number > numeric",
			m:           NewMemStr("10"),
			other:       NewMemInt(5),
			mIsNumeric:  false,
			mIsText:     true,
			wantResult:  1,
			wantHandled: true,
		},
		{
			name:        "non-numeric text vs numeric",
			m:           NewMemStr("not a number"),
			other:       NewMemInt(5),
			mIsNumeric:  false,
			mIsText:     true,
			wantResult:  1,
			wantHandled: true,
		},
		{
			name:        "both not in expected state",
			m:           NewMemNull(),
			other:       NewMemNull(),
			mIsNumeric:  false,
			mIsText:     false,
			wantResult:  0,
			wantHandled: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, handled := compareMixedNumericText(tt.m, tt.other, tt.mIsNumeric, tt.mIsText)
			if handled != tt.wantHandled {
				t.Errorf("compareMixedNumericText() handled = %v, want %v", handled, tt.wantHandled)
			}
			if handled && result != tt.wantResult {
				t.Errorf("compareMixedNumericText() result = %v, want %v", result, tt.wantResult)
			}
		})
	}
}

// arithEdgeCaseTest describes an arithmetic edge case test.
type arithEdgeCaseTest struct {
	name    string
	setup   func() *Mem
	op      func(*Mem) error
	checkFn func(*testing.T, *Mem)
}

func arithEdgeCases() []arithEdgeCaseTest {
	return []arithEdgeCaseTest{
		{"Subtract with null", func() *Mem { return NewMemInt(10) }, func(m *Mem) error { return m.Subtract(NewMemNull()) }, func(t *testing.T, m *Mem) {
			if !m.IsNull() {
				t.Errorf("Expected null result")
			}
		}},
		{"Subtract with overflow", func() *Mem { return NewMemInt(-9223372036854775808) }, func(m *Mem) error { return m.Subtract(NewMemInt(1)) }, func(t *testing.T, m *Mem) {
			if !m.IsReal() {
				t.Errorf("Expected real after overflow")
			}
		}},
		{"Multiply with null", func() *Mem { return NewMemInt(10) }, func(m *Mem) error { return m.Multiply(NewMemNull()) }, func(t *testing.T, m *Mem) {
			if !m.IsNull() {
				t.Errorf("Expected null result")
			}
		}},
		{"Multiply with overflow", func() *Mem { return NewMemInt(9223372036854775807) }, func(m *Mem) error { return m.Multiply(NewMemInt(2)) }, func(t *testing.T, m *Mem) {
			if !m.IsReal() {
				t.Errorf("Expected real after overflow")
			}
		}},
		{"Remainder with null", func() *Mem { return NewMemInt(10) }, func(m *Mem) error { return m.Remainder(NewMemNull()) }, func(t *testing.T, m *Mem) {
			if !m.IsNull() {
				t.Errorf("Expected null result")
			}
		}},
		{"Remainder with zero divisor (int)", func() *Mem { return NewMemInt(10) }, func(m *Mem) error { return m.Remainder(NewMemInt(0)) }, func(t *testing.T, m *Mem) {
			if !m.IsNull() {
				t.Errorf("Expected null result for division by zero")
			}
		}},
		{"Remainder with zero divisor (real)", func() *Mem { return NewMemInt(10) }, func(m *Mem) error { return m.Remainder(NewMemReal(0.0)) }, func(t *testing.T, m *Mem) {
			if !m.IsNull() {
				t.Errorf("Expected null result for division by zero")
			}
		}},
		{"Remainder with real values", func() *Mem { return NewMemReal(10.5) }, func(m *Mem) error { return m.Remainder(NewMemReal(3.0)) }, func(t *testing.T, m *Mem) {
			if !m.IsReal() {
				t.Errorf("Expected real result")
			}
		}},
	}
}

// TestMemArithmeticEdgeCases tests edge cases for arithmetic operations
func TestMemArithmeticEdgeCases(t *testing.T) {
	t.Parallel()
	for _, tt := range arithEdgeCases() {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := tt.setup()
			err := tt.op(m)
			if err != nil {
				t.Errorf("operation error = %v", err)
			}
			tt.checkFn(t, m)
		})
	}
}
