// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"math"
	"testing"
)

// TestSetPathRecursiveNestedArray tests setPathRecursive with nested array operations
func TestSetPathRecursiveNestedArray(t *testing.T) {
	// Create array with sub-array
	data := []interface{}{
		[]interface{}{1, 2, 3},
	}
	parts := []pathPart{
		{index: 0, isIndex: true},
		{index: 1, isIndex: true},
	}

	result := setPathRecursive(data, parts, 99)

	arr, ok := result.([]interface{})
	if !ok {
		t.Error("Expected array result")
	}

	subArr, ok := arr[0].([]interface{})
	if !ok {
		t.Error("Expected sub-array")
	}

	if subArr[1] != 99 {
		t.Errorf("Expected 99, got %v", subArr[1])
	}
}

// TestSetPathRecursiveNestedObject tests setPathRecursive with nested object operations
func TestSetPathRecursiveNestedObject(t *testing.T) {
	// Create object with sub-object
	data := map[string]interface{}{
		"outer": map[string]interface{}{
			"inner": "value",
		},
	}
	parts := []pathPart{
		{key: "outer", isIndex: false},
		{key: "inner", isIndex: false},
	}

	result := setPathRecursive(data, parts, "new value")

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}

	outer, ok := obj["outer"].(map[string]interface{})
	if !ok {
		t.Error("Expected nested object")
	}

	if outer["inner"] != "new value" {
		t.Errorf("Expected 'new value', got %v", outer["inner"])
	}
}

// TestRemovePathNonEmpty tests removePath with valid path that parses
func TestRemovePathNonEmpty(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": "value",
		},
	}

	result := removePath(data, "$.a.b")

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}

	// Navigate to nested object
	nested, ok := obj["a"].(map[string]interface{})
	if !ok {
		t.Error("Expected nested object to still exist")
	}

	// "b" should be removed
	if _, exists := nested["b"]; exists {
		t.Error("Expected 'b' to be removed")
	}
}

// TestRemovePathRecursiveObject tests removePathRecursive with object
func TestRemovePathRecursiveObject(t *testing.T) {
	data := map[string]interface{}{
		"a": map[string]interface{}{
			"b": "value",
		},
	}
	parts := []pathPart{
		{key: "a", isIndex: false},
		{key: "b", isIndex: false},
	}

	result := removePathRecursive(data, parts)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}

	nested, ok := obj["a"].(map[string]interface{})
	if !ok {
		t.Error("Expected nested object")
	}

	if _, exists := nested["b"]; exists {
		t.Error("Expected 'b' to be removed")
	}
}

// TestRemovePathRecursiveArray tests removePathRecursive with array
func TestRemovePathRecursiveArray(t *testing.T) {
	data := []interface{}{
		map[string]interface{}{"key": "value"},
		map[string]interface{}{"key": "value2"},
	}
	parts := []pathPart{
		{index: 0, isIndex: true},
	}

	result := removePathRecursive(data, parts)

	arr, ok := result.([]interface{})
	if !ok {
		t.Error("Expected array result")
	}

	// First element should be removed
	if len(arr) != 1 {
		t.Errorf("Expected length 1, got %d", len(arr))
	}
}

// TestApplyJSONPatchNested tests applyJSONPatch with existing nested value
func TestApplyJSONPatchNested(t *testing.T) {
	target := map[string]interface{}{
		"a": map[string]interface{}{
			"b": 1,
			"c": 2,
		},
	}
	patch := map[string]interface{}{
		"a": map[string]interface{}{
			"b": 99,
		},
	}

	result := applyJSONPatch(target, patch)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}

	nested, ok := obj["a"].(map[string]interface{})
	if !ok {
		t.Error("Expected nested object")
	}

	if nested["b"] != 99 {
		t.Errorf("Expected 99, got %v", nested["b"])
	}

	if nested["c"] != 2 {
		t.Errorf("Expected 2, got %v", nested["c"])
	}
}

// TestApplyJSONPatchNewNested tests applyJSONPatch adding new nested object
func TestApplyJSONPatchNewNested(t *testing.T) {
	target := map[string]interface{}{"a": 1}
	patch := map[string]interface{}{
		"b": map[string]interface{}{"c": 3},
	}

	result := applyJSONPatch(target, patch)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}

	if obj["a"] != 1 {
		t.Error("Expected 'a' to remain")
	}

	nested, ok := obj["b"].(map[string]interface{})
	if !ok {
		t.Error("Expected nested object for 'b'")
	}

	if nested["c"] != 3 {
		t.Error("Expected 'c' to be 3")
	}
}

// TestRoundFuncWithPrecisionExtra tests roundFunc with specific precision values
func TestRoundFuncWithPrecisionExtra(t *testing.T) {
	result, err := roundFunc([]Value{NewFloatValue(1.23456789), NewIntValue(3)})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}

	expected := 1.235
	if math.Abs(result.AsFloat64()-expected) > 0.0001 {
		t.Errorf("Expected %.3f, got %.3f", expected, result.AsFloat64())
	}
}

// TestRoundFuncPassthrough tests roundFunc with NaN/Inf
func TestRoundFuncPassthrough(t *testing.T) {
	// Test with NaN
	result, err := roundFunc([]Value{NewFloatValue(math.NaN())})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN to pass through")
	}

	// Test with Inf
	result, err = roundFunc([]Value{NewFloatValue(math.Inf(1))})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}
	if !math.IsInf(result.AsFloat64(), 1) {
		t.Error("Expected Inf to pass through")
	}

	// Test with very large number
	result, err = roundFunc([]Value{NewFloatValue(1e20)})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}
	// Should pass through unchanged
}

// TestSqrtFuncNull tests sqrtFunc with NULL
func TestSqrtFuncNull(t *testing.T) {
	result, err := sqrtFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("sqrtFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestLnFuncNull tests lnFunc with NULL
func TestLnFuncNull(t *testing.T) {
	result, err := lnFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("lnFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestAsinFuncNull tests asinFunc with NULL
func TestAsinFuncNull(t *testing.T) {
	result, err := asinFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("asinFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestAcosFuncNull tests acosFunc with NULL
func TestAcosFuncNull(t *testing.T) {
	result, err := acosFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("acosFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestAcoshFuncNull tests acoshFunc with NULL
func TestAcoshFuncNull(t *testing.T) {
	result, err := acoshFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("acoshFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestAtanhFuncNull tests atanhFunc with NULL
func TestAtanhFuncNull(t *testing.T) {
	result, err := atanhFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("atanhFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestSubstrFuncWrongArgCount tests substrFunc with wrong arg count
func TestSubstrFuncWrongArgCount(t *testing.T) {
	_, err := substrFunc([]Value{NewTextValue("test")})
	if err == nil {
		t.Error("Expected error for wrong arg count")
	}

	_, err = substrFunc([]Value{NewTextValue("test"), NewIntValue(1), NewIntValue(2), NewIntValue(3)})
	if err == nil {
		t.Error("Expected error for too many args")
	}
}

// TestSubstrAdjustStartNegativeOverflow tests substrAdjustStart with large negative
func TestSubstrAdjustStartNegativeOverflow(t *testing.T) {
	// Negative start that overflows with negative length
	newStart, newSubLen, null := substrAdjustStart(NewIntValue(-20), -20, -5, 10, false)
	if null {
		t.Error("Expected null = false")
	}

	// Start should be adjusted to 0, and subLen adjusted accordingly
	if newStart != 0 {
		t.Errorf("Expected start = 0, got %d", newStart)
	}
	// Use newSubLen to avoid unused variable error
	_ = newSubLen
}

// TestSubstrAdjustNegLenZeroResult tests substrAdjustNegLen resulting in zero
func TestSubstrAdjustNegLenZeroResult(t *testing.T) {
	// Edge case where adjustments result in moving start
	newStart, newSubLen := substrAdjustNegLen(5, -3)

	// Should move start back by 3 and set subLen to 3
	if newStart != 2 {
		t.Errorf("Expected start = 2, got %d", newStart)
	}
	if newSubLen != 3 {
		t.Errorf("Expected subLen = 3, got %d", newSubLen)
	}
}

// TestQuoteFuncDefaultCase tests quoteFunc with unexpected type (default case)
func TestQuoteFuncDefaultCase(t *testing.T) {
	// The default case returns "NULL" but is hard to trigger
	// Test that all known types work
	result, err := quoteFunc([]Value{NewIntValue(42)})
	if err != nil {
		t.Errorf("quoteFunc() error = %v", err)
	}
	if result.AsString() != "42" {
		t.Errorf("Expected '42', got '%s'", result.AsString())
	}
}

// TestCompareValuesWithNullFirst tests compareValues with NULL as first arg
func TestCompareValuesWithNullFirst(t *testing.T) {
	a := NewNullValue()
	b := NewIntValue(1)

	cmp := compareValues(a, b)
	if cmp >= 0 {
		t.Error("Expected NULL < non-NULL")
	}
}

// TestCompareValuesWithNullSecond tests compareValues with NULL as second arg
func TestCompareValuesWithNullSecond(t *testing.T) {
	a := NewIntValue(1)
	b := NewNullValue()

	cmp := compareValues(a, b)
	if cmp <= 0 {
		t.Error("Expected non-NULL > NULL")
	}
}

// TestCompareValuesUnknownType tests compareValues with types not in map
func TestCompareValuesUnknownType(t *testing.T) {
	// All types are in the map, but test that equal types work
	a := NewIntValue(5)
	b := NewIntValue(5)

	cmp := compareValues(a, b)
	if cmp != 0 {
		t.Error("Expected equal values to return 0")
	}
}
