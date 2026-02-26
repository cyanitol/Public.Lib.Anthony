package expr

import (
	"math"
	"testing"
)

func TestEvaluateArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		op       OpCode
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		// Addition
		{
			name:     "Add integers",
			op:       OpPlus,
			left:     int64(10),
			right:    int64(20),
			expected: int64(30),
		},
		{
			name:     "Add floats",
			op:       OpPlus,
			left:     3.14,
			right:    2.86,
			expected: 6.0,
		},
		{
			name:     "Add int and float",
			op:       OpPlus,
			left:     int64(10),
			right:    5.5,
			expected: 15.5,
		},
		// Subtraction
		{
			name:     "Subtract integers",
			op:       OpMinus,
			left:     int64(30),
			right:    int64(10),
			expected: int64(20),
		},
		{
			name:     "Subtract floats",
			op:       OpMinus,
			left:     5.5,
			right:    2.5,
			expected: 3.0,
		},
		// Multiplication
		{
			name:     "Multiply integers",
			op:       OpMultiply,
			left:     int64(6),
			right:    int64(7),
			expected: int64(42),
		},
		{
			name:     "Multiply floats",
			op:       OpMultiply,
			left:     2.5,
			right:    4.0,
			expected: 10.0,
		},
		// Division
		{
			name:     "Divide integers",
			op:       OpDivide,
			left:     int64(20),
			right:    int64(4),
			expected: int64(5),
		},
		{
			name:     "Divide by zero returns NULL",
			op:       OpDivide,
			left:     int64(10),
			right:    int64(0),
			expected: nil,
		},
		{
			name:     "Divide floats",
			op:       OpDivide,
			left:     10.0,
			right:    4.0,
			expected: 2.5,
		},
		// Remainder
		{
			name:     "Remainder integers",
			op:       OpRemainder,
			left:     int64(17),
			right:    int64(5),
			expected: int64(2),
		},
		{
			name:     "Remainder by zero returns NULL",
			op:       OpRemainder,
			left:     int64(10),
			right:    int64(0),
			expected: nil,
		},
		// NULL propagation
		{
			name:     "NULL left operand",
			op:       OpPlus,
			left:     nil,
			right:    int64(10),
			expected: nil,
		},
		{
			name:     "NULL right operand",
			op:       OpPlus,
			left:     int64(10),
			right:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateArithmetic(tt.op, tt.left, tt.right)
			if !compareResults(result, tt.expected) {
				t.Errorf("EvaluateArithmetic(%v, %v, %v) = %v, want %v",
					tt.op, tt.left, tt.right, result, tt.expected)
			}
		})
	}
}

func TestEvaluateUnary(t *testing.T) {
	tests := []struct {
		name     string
		op       OpCode
		operand  interface{}
		expected interface{}
	}{
		{
			name:     "Negate integer",
			op:       OpNegate,
			operand:  int64(42),
			expected: int64(-42),
		},
		{
			name:     "Negate float",
			op:       OpNegate,
			operand:  3.14,
			expected: -3.14,
		},
		{
			name:     "Negate negative",
			op:       OpNegate,
			operand:  int64(-10),
			expected: int64(10),
		},
		{
			name:     "Negate zero",
			op:       OpNegate,
			operand:  int64(0),
			expected: int64(0),
		},
		{
			name:     "Negate NULL",
			op:       OpNegate,
			operand:  nil,
			expected: nil,
		},
		{
			name:     "UnaryPlus integer",
			op:       OpUnaryPlus,
			operand:  int64(42),
			expected: int64(42),
		},
		{
			name:     "BitNot",
			op:       OpBitNot,
			operand:  int64(0),
			expected: int64(-1),
		},
		{
			name:     "BitNot positive",
			op:       OpBitNot,
			operand:  int64(5),  // 0b101
			expected: int64(-6), // 0b...11111010
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateUnary(tt.op, tt.operand)
			if !compareResults(result, tt.expected) {
				t.Errorf("EvaluateUnary(%v, %v) = %v, want %v",
					tt.op, tt.operand, result, tt.expected)
			}
		})
	}
}

func TestEvaluateBitwise(t *testing.T) {
	tests := []struct {
		name     string
		op       OpCode
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		{
			name:     "BitAnd",
			op:       OpBitAnd,
			left:     int64(12), // 0b1100
			right:    int64(10), // 0b1010
			expected: int64(8),  // 0b1000
		},
		{
			name:     "BitOr",
			op:       OpBitOr,
			left:     int64(12), // 0b1100
			right:    int64(10), // 0b1010
			expected: int64(14), // 0b1110
		},
		{
			name:     "BitXor",
			op:       OpBitXor,
			left:     int64(12), // 0b1100
			right:    int64(10), // 0b1010
			expected: int64(6),  // 0b0110
		},
		{
			name:     "LShift",
			op:       OpLShift,
			left:     int64(5),
			right:    int64(2),
			expected: int64(20), // 5 << 2 = 20
		},
		{
			name:     "RShift",
			op:       OpRShift,
			left:     int64(20),
			right:    int64(2),
			expected: int64(5), // 20 >> 2 = 5
		},
		{
			name:     "NULL propagation",
			op:       OpBitAnd,
			left:     nil,
			right:    int64(10),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateBitwise(tt.op, tt.left, tt.right)
			if !compareResults(result, tt.expected) {
				t.Errorf("EvaluateBitwise(%v, %v, %v) = %v, want %v",
					tt.op, tt.left, tt.right, result, tt.expected)
			}
		})
	}
}

func TestEvaluateConcat(t *testing.T) {
	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		{
			name:     "Concat strings",
			left:     "hello",
			right:    " world",
			expected: "hello world",
		},
		{
			name:     "Concat string and integer",
			left:     "value: ",
			right:    int64(42),
			expected: "value: 42",
		},
		{
			name:     "Concat integer and string",
			left:     int64(42),
			right:    " is the answer",
			expected: "42 is the answer",
		},
		{
			name:     "Concat floats",
			left:     3.14,
			right:    2.86,
			expected: "3.142.86",
		},
		{
			name:     "NULL propagation",
			left:     "hello",
			right:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateConcat(tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("EvaluateConcat(%v, %v) = %v, want %v",
					tt.left, tt.right, result, tt.expected)
			}
		})
	}
}

func TestEvaluateLogical(t *testing.T) {
	tests := []struct {
		name     string
		op       OpCode
		left     interface{}
		right    interface{}
		expected interface{}
	}{
		// AND tests
		{
			name:     "AND: true AND true",
			op:       OpAnd,
			left:     int64(1),
			right:    int64(1),
			expected: true,
		},
		{
			name:     "AND: true AND false",
			op:       OpAnd,
			left:     int64(1),
			right:    int64(0),
			expected: false,
		},
		{
			name:     "AND: false AND true",
			op:       OpAnd,
			left:     int64(0),
			right:    int64(1),
			expected: false,
		},
		{
			name:     "AND: false AND false",
			op:       OpAnd,
			left:     int64(0),
			right:    int64(0),
			expected: false,
		},
		{
			name:     "AND: true AND NULL",
			op:       OpAnd,
			left:     int64(1),
			right:    nil,
			expected: nil,
		},
		{
			name:     "AND: false AND NULL",
			op:       OpAnd,
			left:     int64(0),
			right:    nil,
			expected: false,
		},
		// OR tests
		{
			name:     "OR: true OR true",
			op:       OpOr,
			left:     int64(1),
			right:    int64(1),
			expected: true,
		},
		{
			name:     "OR: true OR false",
			op:       OpOr,
			left:     int64(1),
			right:    int64(0),
			expected: true,
		},
		{
			name:     "OR: false OR true",
			op:       OpOr,
			left:     int64(0),
			right:    int64(1),
			expected: true,
		},
		{
			name:     "OR: false OR false",
			op:       OpOr,
			left:     int64(0),
			right:    int64(0),
			expected: false,
		},
		{
			name:     "OR: true OR NULL",
			op:       OpOr,
			left:     int64(1),
			right:    nil,
			expected: true,
		},
		{
			name:     "OR: false OR NULL",
			op:       OpOr,
			left:     int64(0),
			right:    nil,
			expected: nil,
		},
		// NOT tests
		{
			name:     "NOT true",
			op:       OpNot,
			left:     int64(1),
			right:    nil,
			expected: false,
		},
		{
			name:     "NOT false",
			op:       OpNot,
			left:     int64(0),
			right:    nil,
			expected: true,
		},
		{
			name:     "NOT NULL",
			op:       OpNot,
			left:     nil,
			right:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateLogical(tt.op, tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("EvaluateLogical(%v, %v, %v) = %v, want %v",
					tt.op, tt.left, tt.right, result, tt.expected)
			}
		})
	}
}

func TestEvaluateCast(t *testing.T) {
	tests := []struct {
		name       string
		value      interface{}
		targetType string
		expected   interface{}
	}{
		{
			name:       "String to INTEGER",
			value:      "42",
			targetType: "INTEGER",
			expected:   int64(42),
		},
		{
			name:       "Float to INTEGER",
			value:      3.14,
			targetType: "INTEGER",
			expected:   int64(3),
		},
		{
			name:       "String to REAL",
			value:      "3.14",
			targetType: "REAL",
			expected:   3.14,
		},
		{
			name:       "Integer to TEXT",
			value:      int64(42),
			targetType: "TEXT",
			expected:   "42",
		},
		{
			name:       "Float to TEXT",
			value:      3.14,
			targetType: "TEXT",
			expected:   "3.14",
		},
		{
			name:       "String to BLOB",
			value:      "hello",
			targetType: "BLOB",
			expected:   []byte("hello"),
		},
		{
			name:       "NULL to INTEGER",
			value:      nil,
			targetType: "INTEGER",
			expected:   nil,
		},
		{
			name:       "Non-numeric string to INTEGER",
			value:      "hello",
			targetType: "INTEGER",
			expected:   int64(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateCast(tt.value, tt.targetType)
			if !compareResults(result, tt.expected) {
				t.Errorf("EvaluateCast(%v, %q) = %v, want %v",
					tt.value, tt.targetType, result, tt.expected)
			}
		})
	}
}

func TestValueToString(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "String",
			value:    "hello",
			expected: "hello",
		},
		{
			name:     "Integer",
			value:    int64(42),
			expected: "42",
		},
		{
			name:     "Float",
			value:    3.14,
			expected: "3.14",
		},
		{
			name:     "Float with .0",
			value:    42.0,
			expected: "42.0",
		},
		{
			name:     "Boolean true",
			value:    true,
			expected: "1",
		},
		{
			name:     "Boolean false",
			value:    false,
			expected: "0",
		},
		{
			name:     "Byte array",
			value:    []byte("test"),
			expected: "test",
		},
		{
			name:     "NULL",
			value:    nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToString(tt.value)
			if result != tt.expected {
				t.Errorf("valueToString(%v) = %q, want %q",
					tt.value, result, tt.expected)
			}
		})
	}
}

func TestArithmeticOverflow(t *testing.T) {
	tests := []struct {
		name    string
		op      OpCode
		left    interface{}
		right   interface{}
		checkFn func(interface{}) bool
	}{
		{
			name:  "Addition overflow to float",
			op:    OpPlus,
			left:  int64(math.MaxInt64),
			right: int64(1),
			checkFn: func(v interface{}) bool {
				_, isFloat := v.(float64)
				return isFloat
			},
		},
		{
			name:  "Multiplication overflow to float",
			op:    OpMultiply,
			left:  int64(math.MaxInt64 / 2),
			right: int64(3),
			checkFn: func(v interface{}) bool {
				_, isFloat := v.(float64)
				return isFloat
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateArithmetic(tt.op, tt.left, tt.right)
			if result == nil {
				t.Error("Expected result, got nil")
			} else if !tt.checkFn(result) {
				t.Errorf("Result check failed for %v", result)
			}
		})
	}
}

// Helper function to compare results, handling float comparison
func compareResults(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle float comparison with tolerance
	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return math.Abs(aFloat-bFloat) < 1e-9
	}

	// Handle byte array comparison
	aBytes, aIsBytes := a.([]byte)
	bBytes, bIsBytes := b.([]byte)
	if aIsBytes && bIsBytes {
		if len(aBytes) != len(bBytes) {
			return false
		}
		for i := range aBytes {
			if aBytes[i] != bBytes[i] {
				return false
			}
		}
		return true
	}

	return a == b
}
