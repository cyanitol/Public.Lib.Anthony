// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"testing"
)

// TestSumFuncAddIntegerNoOverflow tests addInteger without overflow when already in float mode
func TestSumFuncAddIntegerNoOverflow(t *testing.T) {
	f := &SumFunc{}
	f.hasValue = true
	f.isFloat = true
	f.floatSum = 100.0

	// Adding an integer when already in float mode
	f.addInteger(50)

	if !f.isFloat {
		t.Error("Expected to remain in float mode")
	}

	expected := 150.0
	if f.floatSum != expected {
		t.Errorf("floatSum = %f, want %f", f.floatSum, expected)
	}
}

// TestComputeJDAlreadyValid tests computeJD when JD is already valid
func TestComputeJDAlreadyValid(t *testing.T) {
	dt := &DateTime{
		jd:      2451545 * msPerDay,
		validJD: true,
	}

	oldJD := dt.jd

	// Should not recompute
	dt.computeJD()

	if dt.jd != oldJD {
		t.Error("JD should not change when already valid")
	}
}

// TestComputeJDWithoutHMS tests computeJD without valid HMS
func TestComputeJDWithoutHMS(t *testing.T) {
	dt := &DateTime{
		year:     2000,
		month:    1,
		day:      1,
		validYMD: true,
		validHMS: false,
	}

	dt.computeJD()

	if !dt.validJD {
		t.Error("Expected validJD to be true")
	}

	// JD should be computed for date only (midnight)
	if dt.jd == 0 {
		t.Error("Expected non-zero JD")
	}
}

// TestComputeYMDAlreadyValid tests computeYMD when YMD is already valid
func TestComputeYMDAlreadyValid(t *testing.T) {
	dt := &DateTime{
		year:     2000,
		month:    1,
		day:      1,
		validYMD: true,
	}

	// Should not recompute
	dt.computeYMD()

	if dt.year != 2000 || dt.month != 1 || dt.day != 1 {
		t.Error("YMD should not change when already valid")
	}
}

// TestComputeHMSAlreadyValid tests computeHMS when HMS is already valid
func TestComputeHMSAlreadyValid(t *testing.T) {
	dt := &DateTime{
		hour:     12,
		minute:   30,
		second:   45.0,
		validHMS: true,
	}

	// Should not recompute
	dt.computeHMS()

	if dt.hour != 12 || dt.minute != 30 || dt.second != 45.0 {
		t.Error("HMS should not change when already valid")
	}
}

// TestApplyModifierSpecial tests applyModifier with special modifiers
func TestApplyModifierSpecial(t *testing.T) {
	dt := &DateTime{
		year:     2000,
		month:    1,
		day:      1,
		validYMD: true,
	}

	// Test UTC modifier (should be no-op)
	err := dt.applyModifier("utc")
	if err != nil {
		t.Errorf("applyModifier(utc) error = %v", err)
	}

	// Test localtime modifier (should be no-op)
	err = dt.applyModifier("localtime")
	if err != nil {
		t.Errorf("applyModifier(localtime) error = %v", err)
	}

	// Test auto modifier (should be no-op)
	err = dt.applyModifier("auto")
	if err != nil {
		t.Errorf("applyModifier(auto) error = %v", err)
	}

	// Test subsec modifier (should be no-op)
	err = dt.applyModifier("subsec")
	if err != nil {
		t.Errorf("applyModifier(subsec) error = %v", err)
	}
}

// TestAddUnknownUnit tests add with unknown time unit
func TestAddUnknownUnit(t *testing.T) {
	dt := &DateTime{
		jd:      2451545 * msPerDay,
		validJD: true,
	}

	err := dt.add(1, "fortnight")
	if err == nil {
		t.Error("Expected error for unknown time unit")
	}
}

// TestDateFuncNoArgs tests date() with no arguments
func TestDateFuncNoArgs(t *testing.T) {
	result, err := dateFunc([]Value{})
	if err != nil {
		t.Errorf("dateFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result for date() with no args")
	}
}

// TestDateFuncInvalidInput tests date() with invalid input
func TestDateFuncInvalidInput(t *testing.T) {
	result, err := dateFunc([]Value{NewTextValue("not a date")})
	if err != nil {
		t.Errorf("dateFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid date input")
	}
}

// TestTimeFuncNoArgs tests time() with no arguments
func TestTimeFuncNoArgs(t *testing.T) {
	result, err := timeFunc([]Value{})
	if err != nil {
		t.Errorf("timeFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result for time() with no args")
	}
}

// TestTimeFuncInvalidInput tests time() with invalid input
func TestTimeFuncInvalidInput(t *testing.T) {
	result, err := timeFunc([]Value{NewTextValue("not a time")})
	if err != nil {
		t.Errorf("timeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid time input")
	}
}

// TestDatetimeFuncNoArgs tests datetime() with no arguments
func TestDatetimeFuncNoArgs(t *testing.T) {
	result, err := datetimeFunc([]Value{})
	if err != nil {
		t.Errorf("datetimeFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result for datetime() with no args")
	}
}

// TestDatetimeFuncInvalidInput tests datetime() with invalid input
func TestDatetimeFuncInvalidInput(t *testing.T) {
	result, err := datetimeFunc([]Value{NewTextValue("not a datetime")})
	if err != nil {
		t.Errorf("datetimeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid datetime input")
	}
}

// TestStrftimeFuncInvalidModifier tests strftime() with invalid modifier
func TestStrftimeFuncInvalidModifier(t *testing.T) {
	result, err := strftimeFunc([]Value{
		NewTextValue("%Y"),
		NewTextValue("2023-01-01"),
		NewTextValue("invalid_modifier"),
	})
	if err != nil {
		t.Errorf("strftimeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid modifier")
	}
}

// TestIsValidDateMonthBoundary tests isValidDate at month boundary
func TestIsValidDateMonthBoundary(t *testing.T) {
	// Test December 31
	if !isValidDate(2023, 12, 31) {
		t.Error("Dec 31 should be valid")
	}

	// Test January 1
	if !isValidDate(2023, 1, 1) {
		t.Error("Jan 1 should be valid")
	}
}

// TestJSONArrayFuncMarshalError tests json_array() edge case
func TestJSONArrayFuncMarshalError(t *testing.T) {
	// Test with various types
	result, err := jsonArrayFunc([]Value{
		NewIntValue(1),
		NewFloatValue(2.5),
		NewTextValue("test"),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("jsonArrayFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestJSONQuoteFuncError tests json_quote() edge cases
func TestJSONQuoteFuncError(t *testing.T) {
	// Test with blob
	result, err := jsonQuoteFunc([]Value{NewBlobValue([]byte{1, 2, 3})})
	if err != nil {
		t.Errorf("jsonQuoteFunc() error = %v", err)
	}
	// Should succeed and return array representation
	if result.IsNull() {
		t.Error("Expected non-NULL result")
	}
}

// TestValueToJSONText tests valueToJSON with text that's not valid JSON
func TestValueToJSONText(t *testing.T) {
	val := valueToJSON(NewTextValue("plain text"))
	if val != "plain text" {
		t.Errorf("Expected 'plain text', got %v", val)
	}
}

// TestJSONToValueBool tests jsonToValue with boolean
func TestJSONToValueBool(t *testing.T) {
	result := jsonToValue(true)
	if result.AsInt64() != 1 {
		t.Errorf("Expected 1 for true, got %d", result.AsInt64())
	}

	result = jsonToValue(false)
	if result.AsInt64() != 0 {
		t.Errorf("Expected 0 for false, got %d", result.AsInt64())
	}
}

// TestJSONToValueFloat tests jsonToValue with float
func TestJSONToValueFloat(t *testing.T) {
	// Float that is exactly an integer
	result := jsonToValue(float64(42))
	if result.Type() != TypeInteger {
		t.Error("Expected integer type for 42.0")
	}

	// Float with decimal
	result = jsonToValue(float64(42.5))
	if result.Type() != TypeFloat {
		t.Error("Expected float type for 42.5")
	}
}

// TestSetPathRoot tests setPath with root path
func TestSetPathRoot(t *testing.T) {
	data := map[string]interface{}{"old": "value"}
	result := setPath(data, "$", "new value")

	if result != "new value" {
		t.Errorf("Expected 'new value', got %v", result)
	}

	// Test with empty path
	result = setPath(data, "", "another value")
	if result != "another value" {
		t.Errorf("Expected 'another value', got %v", result)
	}
}

// TestSetPathRecursiveEmptyParts tests setPathRecursive with no parts
func TestSetPathRecursiveEmptyParts(t *testing.T) {
	data := "original"
	result := setPathRecursive(data, []pathPart{}, "new value")

	if result != "new value" {
		t.Errorf("Expected 'new value', got %v", result)
	}
}

// TestRemovePathRoot tests removePath with root path
func TestRemovePathRoot(t *testing.T) {
	data := map[string]interface{}{"key": "value"}
	result := removePath(data, "$")

	if result != nil {
		t.Errorf("Expected nil for root path removal, got %v", result)
	}

	// Test with empty path
	result = removePath(data, "")
	if result != nil {
		t.Errorf("Expected nil for empty path removal, got %v", result)
	}
}

// TestRemovePathEmptyParts tests removePath with empty parts
func TestRemovePathEmptyParts(t *testing.T) {
	data := map[string]interface{}{"key": "value"}

	// After parsing, empty parts means we return original data
	result := removePath(data, "$.") // Will parse to empty parts

	// The function should handle this gracefully
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

// TestRemoveFromArrayNegativeIndex tests removeFromArray with negative index
func TestRemoveFromArrayNegativeIndex(t *testing.T) {
	data := []interface{}{1, 2, 3}
	part := pathPart{index: -1, isIndex: true}

	result := removeFromArray(data, part, nil)

	// Should return original array unchanged
	arr, ok := result.([]interface{})
	if !ok {
		t.Error("Expected array result")
	}
	if len(arr) != 3 {
		t.Errorf("Expected length 3, got %d", len(arr))
	}
}

// TestRemoveFromObjectNonExistent tests removeFromObject with non-existent key
func TestRemoveFromObjectNonExistent(t *testing.T) {
	data := map[string]interface{}{"a": 1, "b": 2}
	part := pathPart{key: "nonexistent", isIndex: false}
	remaining := []pathPart{{key: "nested", isIndex: false}}

	result := removeFromObject(data, part, remaining)

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	// Original object should be unchanged
	if len(obj) != 2 {
		t.Errorf("Expected length 2, got %d", len(obj))
	}
}

// TestApplyJSONPatchNonObject tests applyJSONPatch with non-object target
func TestApplyJSONPatchNonObject(t *testing.T) {
	target := "string value"
	patch := map[string]interface{}{"key": "value"}

	result := applyJSONPatch(target, patch)

	// When target is not an object, patch should create new object
	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Expected object result")
	}
	if obj["key"] != "value" {
		t.Error("Expected patch to be applied")
	}
}

// TestApplyJSONPatchRecursive tests applyJSONPatch with recursive merge
func TestApplyJSONPatchRecursive(t *testing.T) {
	target := map[string]interface{}{
		"a": map[string]interface{}{"b": 1, "c": 2},
	}
	patch := map[string]interface{}{
		"a": map[string]interface{}{"c": 3, "d": 4},
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

	if nested["b"] != 1 {
		t.Error("Expected b to remain")
	}
	if nested["c"] != 3 {
		t.Error("Expected c to be updated")
	}
	if nested["d"] != 4 {
		t.Error("Expected d to be added")
	}
}

// TestRoundParsePrecisionWrongArgCount tests roundParsePrecision with wrong arg count
func TestRoundParsePrecisionWrongArgCount(t *testing.T) {
	_, _, err := roundParsePrecision([]Value{})
	if err == nil {
		t.Error("Expected error for no arguments")
	}

	_, _, err = roundParsePrecision([]Value{NewIntValue(1), NewIntValue(2), NewIntValue(3)})
	if err == nil {
		t.Error("Expected error for too many arguments")
	}
}

// TestRoundParsePrecisionNull tests roundParsePrecision with NULL precision
func TestRoundParsePrecisionNull(t *testing.T) {
	_, ok, err := roundParsePrecision([]Value{NewIntValue(1), NewNullValue()})
	if err != nil {
		t.Errorf("roundParsePrecision() error = %v", err)
	}
	if ok {
		t.Error("Expected ok = false for NULL precision")
	}
}

// TestRandomFuncRange tests randomFunc generates values in correct range
func TestRandomFuncRange(t *testing.T) {
	// Test multiple times to ensure consistency
	for i := 0; i < 10; i++ {
		result, err := randomFunc(nil)
		if err != nil {
			t.Errorf("randomFunc() error = %v", err)
		}

		val := result.AsInt64()
		// Value should be in valid range (not exactly at boundaries)
		if val <= -9223372036854775807 || val > 9223372036854775807 {
			t.Errorf("Random value %d out of expected range", val)
		}
	}
}

// TestRandomblobFuncZero tests randomblob() with size 0
func TestRandomblobFuncZero(t *testing.T) {
	result, err := randomblobFunc([]Value{NewIntValue(0)})
	if err != nil {
		t.Errorf("randomblobFunc() error = %v", err)
	}

	// Should return blob of size 1 (minimum)
	blob := result.AsBlob()
	if len(blob) != 1 {
		t.Errorf("Expected blob of size 1, got %d", len(blob))
	}
}

// TestLengthFuncDefault tests length() with default case
func TestLengthFuncDefault(t *testing.T) {
	// Create a value with TypeNull (not blob, integer, float, or text)
	result, err := lengthFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("lengthFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL input")
	}
}

// TestSubstrAdjustStartPositive tests substrAdjustStart with positive start
func TestSubstrAdjustStartPositive(t *testing.T) {
	start, subLen, null := substrAdjustStart(NewIntValue(5), 5, 10, 20, false)
	if null {
		t.Error("Expected null = false")
	}
	if start != 4 { // converted to 0-based
		t.Errorf("Expected start = 4, got %d", start)
	}
	if subLen != 10 {
		t.Errorf("Expected subLen = 10, got %d", subLen)
	}
}

// TestSubstrAdjustNegLenLessThanStart tests substrAdjustNegLen edge case
func TestSubstrAdjustNegLenLessThanStart(t *testing.T) {
	start, subLen := substrAdjustNegLen(5, -10)

	// When negative length is larger than start
	if start != 0 {
		t.Errorf("Expected start = 0, got %d", start)
	}
	if subLen != 5 {
		t.Errorf("Expected subLen = 5, got %d", subLen)
	}
}

// TestSubstrBlobResultPastEnd tests substrBlobResult with end past length
func TestSubstrBlobResultPastEnd(t *testing.T) {
	blob := []byte{1, 2, 3, 4, 5}
	result := substrBlobResult(blob, 3, 10, 5)

	resultBlob := result.AsBlob()
	// Should only return bytes 3-4 (indices 3 and 4)
	if len(resultBlob) != 2 {
		t.Errorf("Expected length 2, got %d", len(resultBlob))
	}
}

// TestCompareValuesSameTypeBlob tests compareValues with blobs
func TestCompareValuesSameTypeBlob(t *testing.T) {
	a := NewBlobValue([]byte{1, 2, 3})
	b := NewBlobValue([]byte{1, 2, 4})

	cmp := compareValues(a, b)
	if cmp >= 0 {
		t.Error("Expected a < b")
	}
}

// TestCompareValuesSameTypeUnknown tests compareValues with unknown type
func TestCompareValuesSameTypeUnknown(t *testing.T) {
	// This is hard to test since all types are known
	// But we can test the default case by checking NULL comparison
	a := NewNullValue()
	b := NewNullValue()

	cmp := compareValues(a, b)
	if cmp != 0 {
		t.Error("Expected NULL == NULL")
	}
}
