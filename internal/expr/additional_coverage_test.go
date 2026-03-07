// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package expr

import (
	"math"
	"testing"
)

// TestExprEdgeCases tests edge cases not covered by existing tests
func TestExprEdgeCases(t *testing.T) {
	t.Parallel()
	// Test maxListHeight with nil list
	height := maxListHeight(nil, 5)
	if height != 5 {
		t.Errorf("Expected maxListHeight(nil, 5) to return 5, got %d", height)
	}

	// Test maxListHeight with actual list
	list := &ExprList{
		Items: []*ExprListItem{
			{Expr: &Expr{Height: 10}},
			{Expr: &Expr{Height: 3}},
		},
	}
	height = maxListHeight(list, 2)
	if height != 10 {
		t.Errorf("Expected maxListHeight to return 10, got %d", height)
	}
}

// TestIsFunctionConstantWithFlags tests function constant detection with flags
func TestIsFunctionConstantWithFlags(t *testing.T) {
	t.Parallel()
	// Function with EP_HasFunc flag should not be constant
	expr := &Expr{
		Op:    OpFunction,
		Flags: EP_HasFunc,
	}
	if expr.IsConstant() {
		t.Error("Function with EP_HasFunc should not be constant")
	}

	// Function with EP_VarSelect flag should not be constant
	expr = &Expr{
		Op:    OpFunction,
		Flags: EP_VarSelect,
	}
	if expr.IsConstant() {
		t.Error("Function with EP_VarSelect should not be constant")
	}
}

// TestFormatFloatSpecialValues tests formatFloat with special values
func TestFormatFloatSpecialValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value float64
		check func(string) bool
	}{
		{
			"NaN",
			math.NaN(),
			func(s string) bool { return s == "NaN" },
		},
		{
			"positive infinity",
			math.Inf(1),
			func(s string) bool { return s == "Inf" },
		},
		{
			"negative infinity",
			math.Inf(-1),
			func(s string) bool { return s == "-Inf" },
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := formatFloat(tt.value)
			if !tt.check(result) {
				t.Errorf("formatFloat(%f) = %q, expected special value", tt.value, result)
			}
		})
	}
}

// TestArithmeticOverflowEdgeCases tests additional overflow scenarios
func TestArithmeticOverflowEdgeCases(t *testing.T) {
	t.Parallel()
	// Test multiply with infinity result
	result := EvaluateArithmetic(OpMultiply, int64(math.MaxInt64), int64(math.MaxInt64))
	if _, ok := result.(float64); !ok {
		t.Error("Expected float64 result for overflow multiply")
	}

	// Test divide infinity result
	result = EvaluateDivide(10.0, 0.0)
	if result != nil {
		t.Errorf("Expected nil for divide by zero (infinity), got %v", result)
	}
}

// Helper function to test divide separately
func EvaluateDivide(left, right float64) interface{} {
	result := left / right
	if math.IsInf(result, 0) {
		return nil
	}
	return result
}

// TestBitwiseEdgeCases tests bitwise operation edge cases
func TestBitwiseEdgeCases(t *testing.T) {
	t.Parallel()
	// Test right shift with large positive value
	result := bitwiseRShift(math.MaxInt64, 100)
	if result != 0 {
		t.Errorf("Expected 0 for large right shift, got %d", result)
	}

	// Test left shift with large shift amount
	result = bitwiseLShift(1, 100)
	if result != 0 {
		t.Errorf("Expected 0 for large left shift, got %d", result)
	}
}

// TestCollSeqEdgeCases tests collation sequence edge cases
func TestCollSeqEdgeCases(t *testing.T) {
	t.Parallel()
	// Test collSeqFromName with lowercase
	coll := collSeqFromName("nocase")
	if coll.Name != "NOCASE" {
		t.Errorf("Expected NOCASE, got %s", coll.Name)
	}

	// Test collSeqFromName with invalid name returns BINARY
	coll = collSeqFromName("INVALID")
	if coll.Name != "BINARY" {
		t.Errorf("Expected BINARY for invalid name, got %s", coll.Name)
	}

	// Test collSeqFromColumn with non-column expr
	coll = collSeqFromColumn(&Expr{Op: OpInteger})
	if coll != nil {
		t.Error("Expected nil for non-column expression")
	}

	// Test collSeqFromColumn with empty CollSeq
	coll = collSeqFromColumn(&Expr{Op: OpColumn, CollSeq: ""})
	if coll != nil {
		t.Error("Expected nil for empty CollSeq")
	}

	// Test collSeqFromColumn with BINARY returns nil
	coll = collSeqFromColumn(&Expr{Op: OpColumn, CollSeq: "BINARY"})
	if coll != nil {
		t.Error("Expected nil for BINARY collation")
	}

	// Test nextCollSeqExpr
	expr := &Expr{
		Flags: EP_Collate,
		Left: &Expr{
			Flags: EP_Collate,
		},
	}
	next := nextCollSeqExpr(expr)
	if next != expr.Left {
		t.Error("Expected left child for nested collate")
	}

	// Test nextCollSeqExpr with no nested collate
	expr = &Expr{
		Flags: EP_Collate,
		Left:  &Expr{},
	}
	next = nextCollSeqExpr(expr)
	if next != nil {
		t.Error("Expected nil for non-nested collate")
	}
}

// TestCompareNumericsNaN tests NaN handling in numeric comparison
func TestCompareNumericsNaN(t *testing.T) {
	t.Parallel()
	result := compareNumerics(math.NaN(), 1.0)
	if result != CmpNull {
		t.Errorf("Expected CmpNull for NaN comparison, got %v", result)
	}

	result = compareNumerics(1.0, math.NaN())
	if result != CmpNull {
		t.Errorf("Expected CmpNull for NaN comparison, got %v", result)
	}
}

// TestMatchLikeEdgeCases tests LIKE matching edge cases
func TestMatchLikeEdgeCases(t *testing.T) {
	t.Parallel()
	// Test escape at end of pattern
	result := EvaluateLike("test\\", "test", '\\')
	if result {
		t.Error("Expected false for escape at pattern end")
	}

	// Test escape followed by non-matching char
	result = EvaluateLike("a\\bc", "adc", '\\')
	if result {
		t.Error("Expected false for escaped char mismatch")
	}

	// Test single wildcard at end with empty string
	result = EvaluateLike("_", "", 0)
	if result {
		t.Error("Expected false for _ with empty string")
	}

	// Test percent at end matching
	result = EvaluateLike("test%", "test", 0)
	if !result {
		t.Error("Expected true for trailing % with exact match")
	}
}

// TestStepResultFunctions tests step result helper functions
func TestStepResultFunctions(t *testing.T) {
	t.Parallel()
	pattern := []rune("a\\bc")
	str := []rune("abc")

	// Test stepEscape with escape at end
	step := stepEscape([]rune("a\\"), str, false, 1, 0)
	if !step.done || step.result {
		t.Error("Expected done=true, result=false for escape at end")
	}

	// Test stepEscape with str at end
	step = stepEscape(pattern, []rune(""), false, 1, 0)
	if !step.done || step.result {
		t.Error("Expected done=true, result=false for str at end")
	}

	// Test stepMultiWildcard at pattern end
	step = stepMultiWildcard([]rune("a%"), str, 0, false, 1, 0)
	if !step.done || !step.result {
		t.Error("Expected done=true, result=true for % at pattern end")
	}

	// Test stepSingleWildcard with str at end
	step = stepSingleWildcard([]rune(""), 0, 0)
	if !step.done || step.result {
		t.Error("Expected done=true, result=false for str at end")
	}

	// Test stepLiteral with str at end
	step = stepLiteral(pattern, []rune(""), false, 0, 0)
	if !step.done || step.result {
		t.Error("Expected done=true, result=false for str at end")
	}

	// Test stepLiteral with mismatch
	step = stepLiteral([]rune("x"), str, false, 0, 0)
	if !step.done || step.result {
		t.Error("Expected done=true, result=false for char mismatch")
	}
}

// TestIsMultiWildcard tests wildcard detection
func TestIsMultiWildcard(t *testing.T) {
	t.Parallel()
	if !isMultiWildcard('*', true) {
		t.Error("Expected * to be multi wildcard for GLOB")
	}
	if isMultiWildcard('*', false) {
		t.Error("Expected * not to be multi wildcard for LIKE")
	}
	if !isMultiWildcard('%', false) {
		t.Error("Expected % to be multi wildcard for LIKE")
	}
	if isMultiWildcard('%', true) {
		t.Error("Expected % not to be multi wildcard for GLOB")
	}
}

// TestIsSingleWildcard tests single wildcard detection
func TestIsSingleWildcard(t *testing.T) {
	t.Parallel()
	if !isSingleWildcard('?', true) {
		t.Error("Expected ? to be single wildcard for GLOB")
	}
	if isSingleWildcard('?', false) {
		t.Error("Expected ? not to be single wildcard for LIKE")
	}
	if !isSingleWildcard('_', false) {
		t.Error("Expected _ to be single wildcard for LIKE")
	}
	if isSingleWildcard('_', true) {
		t.Error("Expected _ not to be single wildcard for GLOB")
	}
}

// TestMatchChar tests character matching for LIKE/GLOB
func TestMatchChar(t *testing.T) {
	t.Parallel()
	// GLOB is case-sensitive
	if !matchChar('a', 'a', true) {
		t.Error("Expected 'a' == 'a' for GLOB")
	}
	if matchChar('a', 'A', true) {
		t.Error("Expected 'a' != 'A' for GLOB")
	}

	// LIKE is case-insensitive
	if !matchChar('a', 'a', false) {
		t.Error("Expected 'a' == 'a' for LIKE")
	}
	if !matchChar('a', 'A', false) {
		t.Error("Expected 'a' == 'A' for LIKE")
	}
}

// TestValueTypeOrdering tests value type ordering
func TestValueTypeOrdering(t *testing.T) {
	t.Parallel()
	// Test that value types are ordered correctly
	types := []struct {
		value interface{}
		order int
	}{
		{nil, 0},
		{int64(1), 1},
		{3.14, 2},
		{"test", 3},
		{[]byte("blob"), 4},
		{true, 5}, // Unknown type
	}

	for i, tt := range types {
		tt := tt
		order := valueType(tt.value)
		if order != tt.order {
			t.Errorf("Test %d: expected order %d, got %d", i, tt.order, order)
		}
	}
}

// TestCompareSameType tests compareSameType helper
func TestCompareSameType(t *testing.T) {
	t.Parallel()
	// Integer comparison
	result, ok := compareSameType(int64(5), int64(5), nil)
	if !ok || result != CmpEqual {
		t.Error("Expected CmpEqual for same integers")
	}

	// String comparison
	result, ok = compareSameType("abc", "abc", nil)
	if !ok || result != CmpEqual {
		t.Error("Expected CmpEqual for same strings")
	}

	// Blob comparison
	result, ok = compareSameType([]byte("abc"), []byte("abc"), nil)
	if !ok || result != CmpEqual {
		t.Error("Expected CmpEqual for same blobs")
	}

	// Different types
	result, ok = compareSameType(int64(5), "5", nil)
	if ok {
		t.Error("Expected ok=false for different types")
	}
}

// TestIsNumeric tests isNumeric helper
func TestIsNumeric(t *testing.T) {
	t.Parallel()
	if !isNumeric(int64(42)) {
		t.Error("Expected int64 to be numeric")
	}
	if !isNumeric(3.14) {
		t.Error("Expected float64 to be numeric")
	}
	if isNumeric("42") {
		t.Error("Expected string not to be numeric")
	}
	if isNumeric(nil) {
		t.Error("Expected nil not to be numeric")
	}
}

// TestToFloat64 tests toFloat64 helper
func TestToFloat64(t *testing.T) {
	t.Parallel()
	result := toFloat64(int64(42))
	if result != 42.0 {
		t.Errorf("Expected 42.0, got %f", result)
	}

	result = toFloat64(3.14)
	if result != 3.14 {
		t.Errorf("Expected 3.14, got %f", result)
	}
}

// TestCompareIntegersAndBlobs tests comparison helpers
func TestCompareIntegersAndBlobs(t *testing.T) {
	t.Parallel()
	// Integer comparison
	if compareIntegers(5, 5) != CmpEqual {
		t.Error("Expected CmpEqual for equal integers")
	}
	if compareIntegers(3, 5) != CmpLess {
		t.Error("Expected CmpLess for 3 < 5")
	}
	if compareIntegers(7, 5) != CmpGreater {
		t.Error("Expected CmpGreater for 7 > 5")
	}

	// Blob comparison
	if compareBlobs([]byte("abc"), []byte("abc")) != CmpEqual {
		t.Error("Expected CmpEqual for equal blobs")
	}
	if compareBlobs([]byte("abc"), []byte("abd")) != CmpLess {
		t.Error("Expected CmpLess for abc < abd")
	}
	if compareBlobs([]byte("abd"), []byte("abc")) != CmpGreater {
		t.Error("Expected CmpGreater for abd > abc")
	}
}

// TestCompareStringsWithCollation tests string comparison with different collations
func TestCompareStringsWithCollation(t *testing.T) {
	t.Parallel()
	result := compareStrings("abc", "ABC", CollSeqNoCase)
	if result != CmpEqual {
		t.Error("Expected CmpEqual for case-insensitive comparison")
	}

	result = compareStrings("abc  ", "abc", CollSeqRTrim)
	if result != CmpEqual {
		t.Error("Expected CmpEqual for rtrim comparison")
	}

	result = compareStrings("abc", "ABC", nil)
	if result != CmpGreater {
		t.Error("Expected CmpGreater for binary comparison (nil collation)")
	}
}

// TestIntToCompareResult tests intToCompareResult helper
func TestIntToCompareResult(t *testing.T) {
	t.Parallel()
	if intToCompareResult(-5) != CmpLess {
		t.Error("Expected CmpLess for negative")
	}
	if intToCompareResult(0) != CmpEqual {
		t.Error("Expected CmpEqual for zero")
	}
	if intToCompareResult(5) != CmpGreater {
		t.Error("Expected CmpGreater for positive")
	}
}

// TestContainsAnyAffinity tests containsAnyAffinity helper
func TestContainsAnyAffinity(t *testing.T) {
	t.Parallel()
	if !containsAnyAffinity("INTEGER", []string{"INT"}) {
		t.Error("Expected true for INT in INTEGER")
	}
	if containsAnyAffinity("REAL", []string{"INT", "TEXT"}) {
		t.Error("Expected false for INT/TEXT in REAL")
	}
	if !containsAnyAffinity("VARCHAR", []string{"CHAR", "TEXT"}) {
		t.Error("Expected true for CHAR in VARCHAR")
	}
}
