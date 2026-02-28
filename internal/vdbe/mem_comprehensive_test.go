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

// TestMemValue tests the Value method
func TestMemValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mem      *Mem
		expected interface{}
	}{
		{
			name:     "null",
			mem:      NewMemNull(),
			expected: nil,
		},
		{
			name:     "int",
			mem:      NewMemInt(42),
			expected: int64(42),
		},
		{
			name:     "real",
			mem:      NewMemReal(3.14),
			expected: float64(3.14),
		},
		{
			name:     "string",
			mem:      NewMemStr("test"),
			expected: "test",
		},
		{
			name:     "blob",
			mem:      NewMemBlob([]byte{1, 2, 3}),
			expected: []byte{1, 2, 3},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			got := tt.mem.Value()
			switch v := tt.expected.(type) {
			case nil:
				if got != nil {
					t.Errorf("Value() = %v, want nil", got)
				}
			case int64:
				if gotInt, ok := got.(int64); !ok || gotInt != v {
					t.Errorf("Value() = %v, want %v", got, v)
				}
			case float64:
				if gotFloat, ok := got.(float64); !ok || gotFloat != v {
					t.Errorf("Value() = %v, want %v", got, v)
				}
			case string:
				if gotStr, ok := got.(string); !ok || gotStr != v {
					t.Errorf("Value() = %v, want %v", got, v)
				}
			case []byte:
				if gotBytes, ok := got.([]byte); !ok {
					t.Errorf("Value() type = %T, want []byte", got)
				} else {
					for i, b := range gotBytes {
						if b != v[i] {
							t.Errorf("Value()[%d] = %v, want %v", i, b, v[i])
						}
					}
				}
			}
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

			if tt.wantInt && !m.IsInt() {
				t.Errorf("Expected int flag after Numerify()")
			}
			if tt.wantReal && !m.IsReal() {
				t.Errorf("Expected real flag after Numerify()")
			}

			if tt.wantInt {
				if got := m.IntValue(); got != tt.wantValue.(int64) {
					t.Errorf("IntValue() = %v, want %v", got, tt.wantValue)
				}
			}
			if tt.wantReal {
				if got := m.RealValue(); got != tt.wantValue.(float64) {
					t.Errorf("RealValue() = %v, want %v", got, tt.wantValue)
				}
			}
		})
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

// TestMemArithmeticEdgeCases tests edge cases for arithmetic operations
func TestMemArithmeticEdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("Subtract with null", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(10)
		err := m.Subtract(NewMemNull())
		if err != nil {
			t.Errorf("Subtract() error = %v", err)
		}
		if !m.IsNull() {
			t.Errorf("Expected null result")
		}
	})

	t.Run("Subtract with overflow", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(-9223372036854775808) // min int64
		err := m.Subtract(NewMemInt(1))
		if err != nil {
			t.Errorf("Subtract() error = %v", err)
		}
		// Should convert to real on overflow
		if !m.IsReal() {
			t.Errorf("Expected real after overflow")
		}
	})

	t.Run("Multiply with null", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(10)
		err := m.Multiply(NewMemNull())
		if err != nil {
			t.Errorf("Multiply() error = %v", err)
		}
		if !m.IsNull() {
			t.Errorf("Expected null result")
		}
	})

	t.Run("Multiply with overflow", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(9223372036854775807) // max int64
		err := m.Multiply(NewMemInt(2))
		if err != nil {
			t.Errorf("Multiply() error = %v", err)
		}
		// Should convert to real on overflow
		if !m.IsReal() {
			t.Errorf("Expected real after overflow")
		}
	})

	t.Run("Remainder with null", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(10)
		err := m.Remainder(NewMemNull())
		if err != nil {
			t.Errorf("Remainder() error = %v", err)
		}
		if !m.IsNull() {
			t.Errorf("Expected null result")
		}
	})

	t.Run("Remainder with zero divisor (int)", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(10)
		err := m.Remainder(NewMemInt(0))
		if err != nil {
			t.Errorf("Remainder() error = %v", err)
		}
		if !m.IsNull() {
			t.Errorf("Expected null result for division by zero")
		}
	})

	t.Run("Remainder with zero divisor (real)", func(t *testing.T) {
		t.Parallel()
		m := NewMemInt(10)
		err := m.Remainder(NewMemReal(0.0))
		if err != nil {
			t.Errorf("Remainder() error = %v", err)
		}
		if !m.IsNull() {
			t.Errorf("Expected null result for division by zero")
		}
	})

	t.Run("Remainder with real values", func(t *testing.T) {
		t.Parallel()
		m := NewMemReal(10.5)
		err := m.Remainder(NewMemReal(3.0))
		if err != nil {
			t.Errorf("Remainder() error = %v", err)
		}
		if !m.IsReal() {
			t.Errorf("Expected real result")
		}
	})
}
