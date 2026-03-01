// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"testing"
)

func TestCompareValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		aff      Affinity
		coll     *CollSeq
		expected CompareResult
	}{
		{
			name:     "Equal integers",
			left:     int64(42),
			right:    int64(42),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: CmpEqual,
		},
		{
			name:     "Less than integers",
			left:     int64(10),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: CmpLess,
		},
		{
			name:     "Greater than integers",
			left:     int64(30),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: CmpGreater,
		},
		{
			name:     "Equal floats",
			left:     3.14,
			right:    3.14,
			aff:      AFF_REAL,
			coll:     CollSeqBinary,
			expected: CmpEqual,
		},
		{
			name:     "Integer vs float",
			left:     int64(10),
			right:    10.5,
			aff:      AFF_NUMERIC,
			coll:     CollSeqBinary,
			expected: CmpLess,
		},
		{
			name:     "Equal strings (binary)",
			left:     "hello",
			right:    "hello",
			aff:      AFF_TEXT,
			coll:     CollSeqBinary,
			expected: CmpEqual,
		},
		{
			name:     "Less than strings",
			left:     "apple",
			right:    "banana",
			aff:      AFF_TEXT,
			coll:     CollSeqBinary,
			expected: CmpLess,
		},
		{
			name:     "Case-insensitive equal",
			left:     "Hello",
			right:    "hello",
			aff:      AFF_TEXT,
			coll:     CollSeqNoCase,
			expected: CmpEqual,
		},
		{
			name:     "NULL comparison",
			left:     nil,
			right:    int64(42),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: CmpNull,
		},
		{
			name:     "Both NULL",
			left:     nil,
			right:    nil,
			aff:      AFF_NONE,
			coll:     CollSeqBinary,
			expected: CmpNull,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := CompareValues(tt.left, tt.right, tt.aff, tt.coll)
			if result != tt.expected {
				t.Errorf("CompareValues() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEvaluateComparison(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		op       OpCode
		left     interface{}
		right    interface{}
		aff      Affinity
		coll     *CollSeq
		expected interface{}
	}{
		{
			name:     "Equal: true",
			op:       OpEq,
			left:     int64(42),
			right:    int64(42),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Equal: false",
			op:       OpEq,
			left:     int64(10),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: false,
		},
		{
			name:     "Not equal: true",
			op:       OpNe,
			left:     int64(10),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Less than: true",
			op:       OpLt,
			left:     int64(10),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Less than: false",
			op:       OpLt,
			left:     int64(30),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: false,
		},
		{
			name:     "Greater than or equal: true",
			op:       OpGe,
			left:     int64(20),
			right:    int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "IS NULL: true",
			op:       OpIs,
			left:     nil,
			right:    nil,
			aff:      AFF_NONE,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "IS NOT NULL: true",
			op:       OpIsNot,
			left:     int64(42),
			right:    nil,
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Equal with NULL: NULL",
			op:       OpEq,
			left:     nil,
			right:    int64(42),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateComparison(tt.op, tt.left, tt.right, tt.aff, tt.coll)
			if result != tt.expected {
				t.Errorf("EvaluateComparison() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEvaluateLike(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		str      string
		escape   rune
		expected bool
	}{
		{
			name:     "Exact match",
			pattern:  "hello",
			str:      "hello",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard % at end",
			pattern:  "hello%",
			str:      "hello world",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard % at start",
			pattern:  "%world",
			str:      "hello world",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard % in middle",
			pattern:  "h%d",
			str:      "hello world",
			escape:   0,
			expected: true,
		},
		{
			name:     "Single char wildcard _",
			pattern:  "h_llo",
			str:      "hello",
			escape:   0,
			expected: true,
		},
		{
			name:     "Multiple wildcards",
			pattern:  "%e_l%",
			str:      "hello",
			escape:   0,
			expected: true,
		},
		{
			name:     "No match",
			pattern:  "goodbye",
			str:      "hello",
			escape:   0,
			expected: false,
		},
		{
			name:     "Case insensitive (LIKE)",
			pattern:  "HELLO",
			str:      "hello",
			escape:   0,
			expected: true,
		},
		{
			name:     "Escaped wildcard",
			pattern:  "100\\%",
			str:      "100%",
			escape:   '\\',
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateLike(tt.pattern, tt.str, tt.escape)
			if result != tt.expected {
				t.Errorf("EvaluateLike(%q, %q, %v) = %v, want %v",
					tt.pattern, tt.str, tt.escape, result, tt.expected)
			}
		})
	}
}

func TestEvaluateGlob(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{
			name:     "Exact match",
			pattern:  "hello",
			str:      "hello",
			expected: true,
		},
		{
			name:     "Wildcard * at end",
			pattern:  "hello*",
			str:      "hello world",
			expected: true,
		},
		{
			name:     "Single char wildcard ?",
			pattern:  "h?llo",
			str:      "hello",
			expected: true,
		},
		{
			name:     "Case sensitive",
			pattern:  "HELLO",
			str:      "hello",
			expected: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateGlob(tt.pattern, tt.str)
			if result != tt.expected {
				t.Errorf("EvaluateGlob(%q, %q) = %v, want %v",
					tt.pattern, tt.str, result, tt.expected)
			}
		})
	}
}

func TestEvaluateBetween(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		low      interface{}
		high     interface{}
		aff      Affinity
		coll     *CollSeq
		expected interface{}
	}{
		{
			name:     "In range",
			value:    int64(15),
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Below range",
			value:    int64(5),
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: false,
		},
		{
			name:     "Above range",
			value:    int64(25),
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: false,
		},
		{
			name:     "At lower bound",
			value:    int64(10),
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "At upper bound",
			value:    int64(20),
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "NULL value",
			value:    nil,
			low:      int64(10),
			high:     int64(20),
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateBetween(tt.value, tt.low, tt.high, tt.aff, tt.coll)
			if result != tt.expected {
				t.Errorf("EvaluateBetween() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEvaluateIn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		list     []interface{}
		aff      Affinity
		coll     *CollSeq
		expected interface{}
	}{
		{
			name:     "Found in list",
			value:    int64(2),
			list:     []interface{}{int64(1), int64(2), int64(3)},
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: true,
		},
		{
			name:     "Not found in list",
			value:    int64(5),
			list:     []interface{}{int64(1), int64(2), int64(3)},
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: false,
		},
		{
			name:     "NULL value",
			value:    nil,
			list:     []interface{}{int64(1), int64(2), int64(3)},
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: nil,
		},
		{
			name:     "NULL in list, not found",
			value:    int64(5),
			list:     []interface{}{int64(1), nil, int64(3)},
			aff:      AFF_INTEGER,
			coll:     CollSeqBinary,
			expected: nil,
		},
		{
			name:     "String in list",
			value:    "apple",
			list:     []interface{}{"apple", "banana", "cherry"},
			aff:      AFF_TEXT,
			coll:     CollSeqBinary,
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateIn(tt.value, tt.list, tt.aff, tt.coll)
			if result != tt.expected {
				t.Errorf("EvaluateIn() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCoerceToNumeric(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{
			name:     "Integer unchanged",
			value:    int64(42),
			expected: int64(42),
		},
		{
			name:     "Float unchanged",
			value:    3.14,
			expected: 3.14,
		},
		{
			name:     "String to integer",
			value:    "42",
			expected: int64(42),
		},
		{
			name:     "String to float",
			value:    "3.14",
			expected: 3.14,
		},
		{
			name:     "Non-numeric string unchanged",
			value:    "hello",
			expected: "hello",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := CoerceToNumeric(tt.value)
			if result != tt.expected {
				t.Errorf("CoerceToNumeric(%v) = %v, want %v",
					tt.value, result, tt.expected)
			}
		})
	}
}

func TestCoerceToInteger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected int64
		expectOk bool
	}{
		{
			name:     "Integer",
			value:    int64(42),
			expected: 42,
			expectOk: true,
		},
		{
			name:     "Float",
			value:    3.14,
			expected: 3,
			expectOk: true,
		},
		{
			name:     "String integer",
			value:    "42",
			expected: 42,
			expectOk: true,
		},
		{
			name:     "String float",
			value:    "3.14",
			expected: 3,
			expectOk: true,
		},
		{
			name:     "Non-numeric string",
			value:    "hello",
			expected: 0,
			expectOk: false,
		},
		{
			name:     "Boolean true",
			value:    true,
			expected: 1,
			expectOk: true,
		},
		{
			name:     "Boolean false",
			value:    false,
			expected: 0,
			expectOk: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result, ok := CoerceToInteger(tt.value)
			if result != tt.expected || ok != tt.expectOk {
				t.Errorf("CoerceToInteger(%v) = (%v, %v), want (%v, %v)",
					tt.value, result, ok, tt.expected, tt.expectOk)
			}
		})
	}
}

func TestCoerceToBoolean(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{
			name:     "Boolean true",
			value:    true,
			expected: true,
		},
		{
			name:     "Boolean false",
			value:    false,
			expected: false,
		},
		{
			name:     "Non-zero integer",
			value:    int64(42),
			expected: true,
		},
		{
			name:     "Zero integer",
			value:    int64(0),
			expected: false,
		},
		{
			name:     "Non-zero float",
			value:    3.14,
			expected: true,
		},
		{
			name:     "Zero float",
			value:    0.0,
			expected: false,
		},
		{
			name:     "Numeric string",
			value:    "42",
			expected: true,
		},
		{
			name:     "Zero string",
			value:    "0",
			expected: false,
		},
		{
			name:     "Non-numeric string",
			value:    "hello",
			expected: false,
		},
		{
			name:     "NULL",
			value:    nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := CoerceToBoolean(tt.value)
			if result != tt.expected {
				t.Errorf("CoerceToBoolean(%v) = %v, want %v",
					tt.value, result, tt.expected)
			}
		})
	}
}

func TestCollationSequences(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		collSeq  *CollSeq
		a        string
		b        string
		expected int
	}{
		{
			name:     "Binary: equal",
			collSeq:  CollSeqBinary,
			a:        "hello",
			b:        "hello",
			expected: 0,
		},
		{
			name:     "Binary: case sensitive",
			collSeq:  CollSeqBinary,
			a:        "Hello",
			b:        "hello",
			expected: -1, // 'H' < 'h' in ASCII
		},
		{
			name:     "NOCASE: case insensitive",
			collSeq:  CollSeqNoCase,
			a:        "Hello",
			b:        "hello",
			expected: 0,
		},
		{
			name:     "RTRIM: ignore trailing spaces",
			collSeq:  CollSeqRTrim,
			a:        "hello   ",
			b:        "hello",
			expected: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := tt.collSeq.Compare(tt.a, tt.b)
			if (result < 0 && tt.expected >= 0) ||
				(result > 0 && tt.expected <= 0) ||
				(result == 0 && tt.expected != 0) {
				t.Errorf("%s.Compare(%q, %q) = %v, want %v",
					tt.collSeq.Name, tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestLikeEdgeCases tests edge cases for LIKE pattern matching to improve stepMultiWildcard coverage.
func TestLikeEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		str      string
		escape   rune
		expected bool
	}{
		{
			name:     "Trailing wildcard matches empty",
			pattern:  "hello%",
			str:      "hello",
			escape:   0,
			expected: true,
		},
		{
			name:     "Trailing wildcard matches many chars",
			pattern:  "hello%",
			str:      "hello world and beyond",
			escape:   0,
			expected: true,
		},
		{
			name:     "Multiple consecutive wildcards",
			pattern:  "a%%b",
			str:      "axxxb",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard at start matches nothing",
			pattern:  "%test",
			str:      "test",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard in middle with backtracking",
			pattern:  "a%b%c",
			str:      "abbbbc",
			escape:   0,
			expected: true,
		},
		{
			name:     "Complex pattern with multiple wildcards",
			pattern:  "%abc%def%",
			str:      "xyzabcxxxdefghi",
			escape:   0,
			expected: true,
		},
		{
			name:     "Pattern requires backtracking",
			pattern:  "a%a%a",
			str:      "aaaaaa",
			escape:   0,
			expected: true,
		},
		{
			name:     "Wildcard doesn't match pattern continuation",
			pattern:  "a%bc",
			str:      "axxxxbd",
			escape:   0,
			expected: false,
		},
		{
			name:     "Empty string with wildcard",
			pattern:  "%",
			str:      "",
			escape:   0,
			expected: true,
		},
		{
			name:     "Multiple wildcards at end",
			pattern:  "test%%",
			str:      "test",
			escape:   0,
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateLike(tt.pattern, tt.str, tt.escape)
			if result != tt.expected {
				t.Errorf("EvaluateLike(%q, %q, %v) = %v, want %v",
					tt.pattern, tt.str, tt.escape, result, tt.expected)
			}
		})
	}
}

// TestGlobEdgeCases tests edge cases for GLOB pattern matching.
func TestGlobEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{
			name:     "Trailing wildcard matches empty",
			pattern:  "hello*",
			str:      "hello",
			expected: true,
		},
		{
			name:     "Wildcard at start",
			pattern:  "*world",
			str:      "world",
			expected: true,
		},
		{
			name:     "Multiple wildcards with backtracking",
			pattern:  "a*b*c",
			str:      "abbbbc",
			expected: true,
		},
		{
			name:     "Case sensitive GLOB",
			pattern:  "Hello",
			str:      "hello",
			expected: false,
		},
		{
			name:     "Empty string with wildcard",
			pattern:  "*",
			str:      "",
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := EvaluateGlob(tt.pattern, tt.str)
			if result != tt.expected {
				t.Errorf("EvaluateGlob(%q, %q) = %v, want %v",
					tt.pattern, tt.str, result, tt.expected)
			}
		})
	}
}

// TestGetBinaryCompareCollSeq tests the GetBinaryCompareCollSeq function.
func TestGetBinaryCompareCollSeq(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		left     *Expr
		right    *Expr
		expected *CollSeq
	}{
		{
			name:     "Both nil",
			left:     nil,
			right:    nil,
			expected: CollSeqBinary,
		},
		{
			name: "Left has collate property",
			left: &Expr{
				Op:      OpCollate,
				CollSeq: "NOCASE",
				Flags:   EP_Collate,
			},
			right:    NewIntExpr(5),
			expected: CollSeqNoCase,
		},
		{
			name: "Right has collate property",
			left: NewIntExpr(5),
			right: &Expr{
				Op:      OpCollate,
				CollSeq: "RTRIM",
				Flags:   EP_Collate,
			},
			expected: CollSeqRTrim,
		},
		{
			name:     "Neither has collate",
			left:     NewIntExpr(5),
			right:    NewIntExpr(10),
			expected: CollSeqBinary,
		},
		{
			name:     "Left nil, right has no collate",
			left:     nil,
			right:    NewIntExpr(5),
			expected: CollSeqBinary,
		},
		{
			name:     "Left has no collate, right nil",
			left:     NewIntExpr(5),
			right:    nil,
			expected: CollSeqBinary,
		},
		{
			name: "Left has non-binary collation from GetCollSeq",
			left: &Expr{
				Op:      OpColumn,
				CollSeq: "NOCASE",
			},
			right:    NewIntExpr(5),
			expected: CollSeqNoCase,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := GetBinaryCompareCollSeq(tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("GetBinaryCompareCollSeq() = %v, want %v", result.Name, tt.expected.Name)
			}
		})
	}
}
