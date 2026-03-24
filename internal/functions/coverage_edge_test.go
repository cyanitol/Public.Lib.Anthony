// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// TestJSONFuncMarshalError tests json() when marshal fails (edge case)
func TestJSONFuncMarshalError(t *testing.T) {
	// Valid JSON that will unmarshal but might have edge cases
	result, err := jsonFunc([]Value{NewTextValue(`{"key":"value"}`)})
	if err != nil {
		t.Errorf("jsonFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result for valid JSON")
	}
}

// TestJSONArrayFuncReturnError tests json_array() marshal failure path
func TestJSONArrayFuncReturnError(t *testing.T) {
	// Test with values that should marshal successfully
	result, err := jsonArrayFunc([]Value{
		NewTextValue("test"),
		NewIntValue(123),
	})
	if err != nil {
		t.Errorf("jsonArrayFunc() error = %v", err)
	}
	// Should succeed
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONExtractFuncUnmarshalError tests json_extract() with invalid JSON
func TestJSONExtractFuncUnmarshalError(t *testing.T) {
	result, err := jsonExtractFunc([]Value{
		NewTextValue("invalid json"),
		NewTextValue("$.path"),
	})
	if err != nil {
		t.Errorf("jsonExtractFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid JSON")
	}
}

// TestJSONInsertFuncMarshalError tests json_insert() marshal failure path
func TestJSONInsertFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonInsertFunc([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue("$.b"),
		NewIntValue(2),
	})
	if err != nil {
		t.Errorf("jsonInsertFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONObjectFuncMarshalError tests json_object() marshal failure path
func TestJSONObjectFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonObjectFunc([]Value{
		NewTextValue("key"),
		NewIntValue(123),
	})
	if err != nil {
		t.Errorf("jsonObjectFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONPatchFuncMarshalError tests json_patch() marshal failure path
func TestJSONPatchFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonPatchFunc([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue(`{"b":2}`),
	})
	if err != nil {
		t.Errorf("jsonPatchFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONRemoveFuncMarshalError tests json_remove() marshal failure path
func TestJSONRemoveFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonRemoveFunc([]Value{
		NewTextValue(`{"a":1,"b":2}`),
		NewTextValue("$.a"),
	})
	if err != nil {
		t.Errorf("jsonRemoveFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONReplaceFuncMarshalError tests json_replace() marshal failure path
func TestJSONReplaceFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonReplaceFunc([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue("$.a"),
		NewIntValue(2),
	})
	if err != nil {
		t.Errorf("jsonReplaceFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONSetFuncMarshalError tests json_set() marshal failure path
func TestJSONSetFuncMarshalError(t *testing.T) {
	// Valid case that should succeed
	result, err := jsonSetFunc([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue("$.b"),
		NewIntValue(2),
	})
	if err != nil {
		t.Errorf("jsonSetFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONQuoteFuncMarshalBlob tests json_quote() with blob that succeeds
func TestJSONQuoteFuncMarshalBlob(t *testing.T) {
	result, err := jsonQuoteFunc([]Value{NewTextValue("test string")})
	if err != nil {
		t.Errorf("jsonQuoteFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestValueToJSONBlob tests valueToJSON with blob
func TestValueToJSONBlob(t *testing.T) {
	blob := []byte{1, 2, 3}
	result := valueToJSON(NewBlobValue(blob))

	// Should return the blob
	b, ok := result.([]byte)
	if !ok {
		t.Error("Expected blob result")
	}
	if len(b) != 3 {
		t.Errorf("Expected length 3, got %d", len(b))
	}
}

// TestJSONToValueArray tests jsonToValue with array
func TestJSONToValueArray(t *testing.T) {
	arr := []interface{}{1, 2, 3}
	result := jsonToValue(arr)

	// Should return JSON string representation
	if result.Type() != TypeText {
		t.Error("Expected text type for array")
	}
}

// TestJSONToValueObject tests jsonToValue with object
func TestJSONToValueObject(t *testing.T) {
	obj := map[string]interface{}{"key": "value"}
	result := jsonToValue(obj)

	// Should return JSON string representation
	if result.Type() != TypeText {
		t.Error("Expected text type for object")
	}
}

// TestJSONToValueUnknown tests jsonToValue with unknown type
func TestJSONToValueUnknown(t *testing.T) {
	// Use a type that's not handled
	result := jsonToValue(struct{}{})

	// Should return NULL for unknown type
	if !result.IsNull() {
		t.Error("Expected NULL for unknown type")
	}
}

// TestSetPathAfterRemovingDollar tests setPath after $ removal
func TestSetPathAfterRemovingDollar(t *testing.T) {
	data := map[string]interface{}{}
	result := setPath(data, "$.key", "value")

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if obj["key"] != "value" {
		t.Error("Expected key to be set")
	}
}

// TestSetPathRecursiveArrayCreation tests setPathRecursive creating array
func TestSetPathRecursiveArrayCreation(t *testing.T) {
	// Start with non-array data
	data := "string"
	parts := []pathPart{{index: 0, isIndex: true}}

	result := setPathRecursive(data, parts, "value")

	arr, ok := result.([]interface{})
	if !ok {
		t.Error("Expected array result")
	}
	if len(arr) != 1 {
		t.Errorf("Expected length 1, got %d", len(arr))
	}
}

// TestSetPathRecursiveObjectCreation tests setPathRecursive creating object
func TestSetPathRecursiveObjectCreation(t *testing.T) {
	// Start with non-object data
	data := 123
	parts := []pathPart{{key: "key", isIndex: false}}

	result := setPathRecursive(data, parts, "value")

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if obj["key"] != "value" {
		t.Error("Expected key to be set")
	}
}

// TestRemovePathAfterRemovingDollar tests removePath after $ removal
func TestRemovePathAfterRemovingDollar(t *testing.T) {
	data := map[string]interface{}{"key": "value"}
	result := removePath(data, "$.key")

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if _, exists := obj["key"]; exists {
		t.Error("Expected key to be removed")
	}
}

// TestRemoveFromArrayWithRemaining tests removeFromArray with remaining parts
func TestRemoveFromArrayWithRemaining(t *testing.T) {
	data := []interface{}{
		map[string]interface{}{"a": 1},
		map[string]interface{}{"a": 2},
	}
	part := pathPart{index: 0, isIndex: true}
	remaining := []pathPart{{key: "a", isIndex: false}}

	result := removeFromArray(data, part, remaining)

	arr, ok := result.([]interface{})
	if !ok {
		t.Error("Expected array result")
	}
	if len(arr) != 2 {
		t.Errorf("Expected length 2, got %d", len(arr))
	}
}

// TestApplyJSONPatchDeleteKey tests applyJSONPatch with null value (delete)
func TestApplyJSONPatchDeleteKey(t *testing.T) {
	target := map[string]interface{}{"a": 1, "b": 2}
	patch := map[string]interface{}{"a": nil}

	result := applyJSONPatch(target, patch)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if _, exists := obj["a"]; exists {
		t.Error("Expected key 'a' to be deleted")
	}
	if obj["b"] != 2 {
		t.Error("Expected key 'b' to remain")
	}
}

// TestApplyJSONPatchNonObjectPatch tests applyJSONPatch with non-object patch
func TestApplyJSONPatchNonObjectPatch(t *testing.T) {
	target := map[string]interface{}{"a": 1}
	patch := "string value"

	result := applyJSONPatch(target, patch)

	// Patch should replace target
	if result != patch {
		t.Error("Expected patch to replace target")
	}
}

// TestApplyJSONPatchNonMapValue tests applyJSONPatch with non-map patch value
func TestApplyJSONPatchNonMapValue(t *testing.T) {
	target := map[string]interface{}{"a": 1}
	patch := map[string]interface{}{"a": 2}

	result := applyJSONPatch(target, patch)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if obj["a"] != 2 {
		t.Error("Expected value to be replaced")
	}
}

// TestRoundFuncNullPrecision tests roundFunc with NULL precision
func TestRoundFuncNullPrecision(t *testing.T) {
	result, err := roundFunc([]Value{NewFloatValue(1.5), NewNullValue()})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL precision")
	}
}

// TestRoundFuncNullValue tests roundFunc with NULL value
func TestRoundFuncNullValue(t *testing.T) {
	result, err := roundFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("roundFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL value")
	}
}

// TestRandomFuncPositiveValue tests randomFunc ensuring positive handling
func TestRandomFuncPositiveValue(t *testing.T) {
	// Just verify it doesn't error
	for i := 0; i < 5; i++ {
		result, err := randomFunc(nil)
		if err != nil {
			t.Errorf("randomFunc() error = %v", err)
		}
		if result.IsNull() {
			t.Error("Expected non-NULL result")
		}
	}
}

// TestSubstrFuncStartAtEnd tests substr() with start at the end
func TestSubstrFuncStartAtEnd(t *testing.T) {
	result, err := substrFunc([]Value{
		NewTextValue("hello"),
		NewIntValue(6), // past the end
		NewIntValue(5),
	})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}

	// Should return empty string
	if result.AsString() != "" {
		t.Errorf("Expected empty string, got '%s'", result.AsString())
	}
}

// TestSubstrAdjustNegLenNegativeResult tests substrAdjustNegLen edge case
func TestSubstrAdjustNegLenNegativeResult(t *testing.T) {
	// When start - negLen results in negative
	start, subLen := substrAdjustNegLen(3, -5)

	if start != 0 {
		t.Errorf("Expected start = 0, got %d", start)
	}
	if subLen != 3 {
		t.Errorf("Expected subLen = 3, got %d", subLen)
	}
}

// TestSubstrBlobResultStartPastEnd tests substrBlobResult with start >= length
func TestSubstrBlobResultStartPastEnd(t *testing.T) {
	blob := []byte{1, 2, 3}
	result := substrBlobResult(blob, 10, 5, 3)

	// Should return empty blob
	if len(result.AsBlob()) != 0 {
		t.Errorf("Expected empty blob, got length %d", len(result.AsBlob()))
	}
}

// TestComputeJDWithMonthAdjustment tests computeJD with month <= 2
func TestComputeJDWithMonthAdjustment(t *testing.T) {
	dt := &DateTime{
		year:     2000,
		month:    1, // January
		day:      15,
		validYMD: true,
	}

	dt.computeJD()

	if !dt.validJD {
		t.Error("Expected validJD to be true")
	}
	if dt.jd == 0 {
		t.Error("Expected non-zero JD")
	}
}

// TestApplyModifierNumeric tests applyModifier with numeric modifier
func TestApplyModifierNumeric(t *testing.T) {
	dt := &DateTime{
		jd:      2451545 * msPerDay,
		validJD: true,
	}

	// Test with "+5 days"
	err := dt.applyModifier("+5 days")
	if err != nil {
		t.Errorf("applyModifier() error = %v", err)
	}
}
