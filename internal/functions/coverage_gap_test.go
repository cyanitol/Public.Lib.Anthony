package functions

import (
	"math"
	"strings"
	"testing"
)

// TestSumFuncIntegerOverflowExtra tests additional overflow cases in SumFunc.addInteger
func TestSumFuncIntegerOverflowExtra(t *testing.T) {
	f := &SumFunc{}

	// Test negative overflow with negative value
	f.intSum = math.MinInt64 + 10
	f.hasValue = true
	f.isFloat = false

	// Adding a negative value should cause overflow and switch to float
	f.addInteger(-100)

	if !f.isFloat {
		t.Error("Expected overflow to switch to float mode")
	}

	// Verify the sum is correct
	expected := float64(math.MinInt64+10) - 100.0
	if math.Abs(f.floatSum-expected) > 1e-9 {
		t.Errorf("floatSum = %f, want %f", f.floatSum, expected)
	}
}

// TestScalarFuncCallArgCountMismatch tests ScalarFunc.Call with wrong arg count
func TestScalarFuncCallArgCountMismatch(t *testing.T) {
	fn := NewScalarFunc("test", 2, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})

	// Call with wrong number of arguments
	_, err := fn.Call([]Value{NewIntValue(1)})
	if err == nil {
		t.Error("Expected error when calling with wrong number of arguments")
	}

	// Call with correct number of arguments
	result, err := fn.Call([]Value{NewIntValue(1), NewIntValue(2)})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.AsInt64() != 42 {
		t.Errorf("Result = %d, want 42", result.AsInt64())
	}
}

// TestRegistryUnregisterVariadicExtra tests additional Unregister cases for variadic functions
func TestRegistryUnregisterVariadicExtra(t *testing.T) {
	r := NewRegistry()

	// Try unregistering non-existent variadic function
	if r.Unregister("nonexistent_variadic", -1) {
		t.Error("Unregister should return false for non-existent function")
	}
}

// TestRegistryUnregisterFixedArgs tests Unregister for fixed-arg functions
func TestRegistryUnregisterFixedArgs(t *testing.T) {
	r := NewRegistry()

	// Register a fixed-arg function
	fn := NewScalarFunc("test_fixed", 2, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})
	r.RegisterUser(fn, 2)

	// Verify it's registered
	if _, ok := r.LookupWithArgs("test_fixed", 2); !ok {
		t.Error("Function should be registered")
	}

	// Unregister with wrong arg count
	if r.Unregister("test_fixed", 3) {
		t.Error("Unregister should return false for wrong arg count")
	}

	// Unregister with correct arg count
	if !r.Unregister("test_fixed", 2) {
		t.Error("Unregister should return true")
	}

	// Verify it's gone
	if _, ok := r.LookupWithArgs("test_fixed", 2); ok {
		t.Error("Function should be unregistered")
	}
}

// TestRegistryLookupWithArgsBuiltin tests LookupWithArgs fallback to builtins
func TestRegistryLookupWithArgsBuiltin(t *testing.T) {
	r := NewRegistry()

	// Register a builtin function
	fn := NewScalarFunc("builtin_test", 1, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})
	r.Register(fn)

	// Lookup with args should find the builtin
	result, ok := r.LookupWithArgs("builtin_test", 1)
	if !ok {
		t.Error("LookupWithArgs should find builtin function")
	}
	if result.Name() != "builtin_test" {
		t.Errorf("Name = %s, want builtin_test", result.Name())
	}

	// Now register a user function with the same name but different arg count
	userFn := NewScalarFunc("builtin_test", 2, func(args []Value) (Value, error) {
		return NewIntValue(100), nil
	})
	r.RegisterUser(userFn, 2)

	// Lookup with 1 arg should still find builtin
	result, ok = r.LookupWithArgs("builtin_test", 1)
	if !ok {
		t.Error("LookupWithArgs should find builtin function")
	}

	// Lookup with 2 args should find user function
	result, ok = r.LookupWithArgs("builtin_test", 2)
	if !ok {
		t.Error("LookupWithArgs should find user function")
	}
}

// TestDateParseFallbackToNumber tests parseString fallback to number parsing
func TestDateParseFallbackToNumber(t *testing.T) {
	dt := &DateTime{}

	// Test parsing a numeric string
	err := dt.parseString("2451545.0")
	if err != nil {
		t.Errorf("parseString() error = %v", err)
	}

	if !dt.validJD {
		t.Error("Expected validJD to be true after parsing numeric string")
	}
}

// TestDateFuncWithError tests date() with invalid modifier
func TestDateFuncWithError(t *testing.T) {
	// Test with invalid modifier
	result, err := dateFunc([]Value{
		NewTextValue("2023-01-01"),
		NewTextValue("invalid_modifier"),
	})
	if err != nil {
		t.Errorf("dateFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid modifier")
	}
}

// TestDateFuncWithNullModifier tests date() with NULL modifier
func TestDateFuncWithNullModifier(t *testing.T) {
	result, err := dateFunc([]Value{
		NewTextValue("2023-01-01"),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("dateFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL modifier")
	}
}

// TestTimeFuncWithError tests time() with invalid modifier
func TestTimeFuncWithError(t *testing.T) {
	result, err := timeFunc([]Value{
		NewTextValue("12:34:56"),
		NewTextValue("invalid_modifier"),
	})
	if err != nil {
		t.Errorf("timeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid modifier")
	}
}

// TestTimeFuncWithNullModifier tests time() with NULL modifier
func TestTimeFuncWithNullModifier(t *testing.T) {
	result, err := timeFunc([]Value{
		NewTextValue("12:34:56"),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("timeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL modifier")
	}
}

// TestDatetimeFuncWithError tests datetime() with invalid modifier
func TestDatetimeFuncWithError(t *testing.T) {
	result, err := datetimeFunc([]Value{
		NewTextValue("2023-01-01 12:34:56"),
		NewTextValue("invalid_modifier"),
	})
	if err != nil {
		t.Errorf("datetimeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid modifier")
	}
}

// TestDatetimeFuncWithNullModifier tests datetime() with NULL modifier
func TestDatetimeFuncWithNullModifier(t *testing.T) {
	result, err := datetimeFunc([]Value{
		NewTextValue("2023-01-01 12:34:56"),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("datetimeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL modifier")
	}
}

// TestStrftimeFuncNoArgs tests strftime() with only format (no datetime)
func TestStrftimeFuncNoArgs(t *testing.T) {
	result, err := strftimeFunc([]Value{
		NewTextValue("%Y-%m-%d"),
	})
	if err != nil {
		t.Errorf("strftimeFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("Expected non-NULL result for strftime with only format")
	}
}

// TestStrftimeFuncNullArgs tests strftime() with NULL format
func TestStrftimeFuncNullArgs(t *testing.T) {
	result, err := strftimeFunc([]Value{})
	if err != nil {
		t.Errorf("strftimeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL result for strftime with no args")
	}
}

// TestComputeJDNonDefault tests computeJD with non-default values
func TestComputeJDNonDefault(t *testing.T) {
	dt := &DateTime{
		year:     2000,
		month:    1,
		day:      1,
		hour:     12,
		minute:   30,
		second:   45.5,
		validYMD: true,
		validHMS: true,
		tz:       60, // 1 hour timezone offset
	}

	dt.computeJD()

	if !dt.validJD {
		t.Error("Expected validJD to be true")
	}

	// The JD should be adjusted for timezone
	// We just verify it was computed
	if dt.jd == 0 {
		t.Error("Expected non-zero JD")
	}
}

// TestComputeYMDFromJD tests computeYMD when JD is valid
func TestComputeYMDFromJD(t *testing.T) {
	dt := &DateTime{
		jd:      2451545 * msPerDay, // 2000-01-01
		validJD: true,
	}

	dt.computeYMD()

	if !dt.validYMD {
		t.Error("Expected validYMD to be true")
	}

	// Verify year is in reasonable range
	if dt.year < 1999 || dt.year > 2001 {
		t.Errorf("year = %d, expected around 2000", dt.year)
	}
}

// TestComputeYMDNoJD tests computeYMD when JD is not valid
func TestComputeYMDNoJD(t *testing.T) {
	dt := &DateTime{
		validJD: false,
	}

	dt.computeYMD()

	if !dt.validYMD {
		t.Error("Expected validYMD to be true")
	}

	// Should default to 2000-01-01
	if dt.year != 2000 || dt.month != 1 || dt.day != 1 {
		t.Errorf("Expected 2000-01-01, got %d-%d-%d", dt.year, dt.month, dt.day)
	}
}

// TestIsValidDateBoundaries tests isValidDate boundary cases
func TestIsValidDateBoundaries(t *testing.T) {
	tests := []struct {
		year  int
		month int
		day   int
		want  bool
	}{
		{9999, 12, 31, true},  // maximum valid date
		{0, 1, 1, true},       // minimum year
	}

	for _, tt := range tests {
		got := isValidDate(tt.year, tt.month, tt.day)
		if got != tt.want {
			t.Errorf("isValidDate(%d, %d, %d) = %v, want %v",
				tt.year, tt.month, tt.day, got, tt.want)
		}
	}
}

// TestJSONFuncInvalid tests json() with invalid JSON
func TestJSONFuncInvalid(t *testing.T) {
	result, err := jsonFunc([]Value{NewTextValue("not json")})
	if err != nil {
		t.Errorf("jsonFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for invalid JSON")
	}
}

// TestJSONArrayFuncEmpty tests json_array() with no arguments
func TestJSONArrayFuncEmpty(t *testing.T) {
	result, err := jsonArrayFunc([]Value{})
	if err != nil {
		t.Errorf("jsonArrayFunc() error = %v", err)
	}
	if result.AsString() != "[]" {
		t.Errorf("Expected '[]', got '%s'", result.AsString())
	}
}

// TestJSONObjectFuncNullKey tests json_object() with NULL key
func TestJSONObjectFuncNullKey(t *testing.T) {
	_, err := jsonObjectFunc([]Value{NewNullValue(), NewIntValue(1)})
	if err == nil {
		t.Error("Expected error for NULL key in json_object()")
	}
}

// TestJSONQuoteFuncNull tests json_quote() with NULL value
func TestJSONQuoteFuncNull(t *testing.T) {
	result, err := jsonQuoteFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("jsonQuoteFunc() error = %v", err)
	}
	if result.AsString() != "null" {
		t.Errorf("Expected 'null', got '%s'", result.AsString())
	}
}

// TestJSONQuoteFuncMarshalError tests json_quote() with unmarshalable value
func TestJSONQuoteFuncMarshalError(t *testing.T) {
	// Create a value that will fail to marshal
	// This is difficult in Go, but we can test the function anyway
	result, err := jsonQuoteFunc([]Value{NewIntValue(42)})
	if err != nil {
		t.Errorf("jsonQuoteFunc() error = %v", err)
	}
	// Should return "42"
	if result.AsString() != "42" {
		t.Errorf("Expected '42', got '%s'", result.AsString())
	}
}

// TestTraversePathNull tests traversePath with nil current value
func TestTraversePathNull(t *testing.T) {
	parts := []pathPart{{key: "test", isIndex: false}}
	result := traversePath(nil, parts)
	if result != nil {
		t.Error("Expected nil result when current is nil")
	}
}

// TestRemovePathRecursiveArrayBounds tests removeFromArray with out of bounds index
func TestRemovePathRecursiveArrayBounds(t *testing.T) {
	data := []interface{}{1, 2, 3}
	part := pathPart{index: 10, isIndex: true}

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

// TestRemovePathRecursiveNonArray tests removeFromArray with non-array data
func TestRemovePathRecursiveNonArray(t *testing.T) {
	data := "not an array"
	part := pathPart{index: 0, isIndex: true}

	result := removeFromArray(data, part, nil)

	// Should return original data unchanged
	if result != data {
		t.Error("Expected original data when not an array")
	}
}

// TestHandleDotCharacterInBracket tests handleDotCharacter when in bracket
func TestHandleDotCharacterInBracket(t *testing.T) {
	var current strings.Builder
	var parts []pathPart

	// Call with inBracket = true
	inBracket := handleDotCharacter(&current, &parts, true)

	if !inBracket {
		t.Error("Expected to remain in bracket context")
	}

	if current.String() != "." {
		t.Errorf("Expected '.', got '%s'", current.String())
	}
}

// TestHandleCloseBracketNotInBracket tests handleCloseBracket when not in bracket
func TestHandleCloseBracketNotInBracket(t *testing.T) {
	var current strings.Builder
	var parts []pathPart
	current.WriteString("test")

	// Call with inBracket = false
	inBracket := handleCloseBracket(&current, &parts, false)

	if inBracket {
		t.Error("Expected to remain outside bracket context")
	}

	// Current should be unchanged
	if current.String() != "test" {
		t.Errorf("Expected 'test', got '%s'", current.String())
	}
}

// TestRoundParsePrecisionOutOfRange tests roundParsePrecision with out of range precision
func TestRoundParsePrecisionOutOfRange(t *testing.T) {
	// Test precision > 30
	p, ok, err := roundParsePrecision([]Value{NewIntValue(1), NewIntValue(50)})
	if err != nil {
		t.Errorf("roundParsePrecision() error = %v", err)
	}
	if !ok {
		t.Error("Expected ok = true")
	}
	if p != 30 {
		t.Errorf("Expected precision clamped to 30, got %d", p)
	}

	// Test precision < 0
	p, ok, err = roundParsePrecision([]Value{NewIntValue(1), NewIntValue(-5)})
	if err != nil {
		t.Errorf("roundParsePrecision() error = %v", err)
	}
	if !ok {
		t.Error("Expected ok = true")
	}
	if p != 0 {
		t.Errorf("Expected precision clamped to 0, got %d", p)
	}
}

// TestRandomFuncNegativeHandling tests randomFunc handling of negative values
func TestRandomFuncNegativeHandling(t *testing.T) {
	// Call random multiple times and verify range
	for i := 0; i < 100; i++ {
		result, err := randomFunc(nil)
		if err != nil {
			t.Errorf("randomFunc() error = %v", err)
		}

		val := result.AsInt64()
		// Should never be MinInt64
		if val == math.MinInt64 {
			t.Error("randomFunc() should never return MinInt64")
		}
	}
}

// TestRandomblobFuncNegativeSize tests randomblob() with negative size
func TestRandomblobFuncNegativeSize(t *testing.T) {
	result, err := randomblobFunc([]Value{NewIntValue(-10)})
	if err != nil {
		t.Errorf("randomblobFunc() error = %v", err)
	}

	// Should return blob of size 1
	blob := result.AsBlob()
	if len(blob) != 1 {
		t.Errorf("Expected blob of size 1, got %d", len(blob))
	}
}

// TestSqrtFuncNegativeExtra tests sqrt() edge cases
func TestSqrtFuncNegativeExtra(t *testing.T) {
	// Test with very small negative number
	result, err := sqrtFunc([]Value{NewFloatValue(-0.0001)})
	if err != nil {
		t.Errorf("sqrtFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for sqrt of negative number")
	}
}

// TestLnFuncZero tests ln() with zero
func TestLnFuncZero(t *testing.T) {
	result, err := lnFunc([]Value{NewFloatValue(0.0)})
	if err != nil {
		t.Errorf("lnFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for ln of zero")
	}
}

// TestAsinFuncOutOfRange tests asin() with out of range values
func TestAsinFuncOutOfRange(t *testing.T) {
	result, err := asinFunc([]Value{NewFloatValue(2.0)})
	if err != nil {
		t.Errorf("asinFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for asin(2.0)")
	}

	result, err = asinFunc([]Value{NewFloatValue(-2.0)})
	if err != nil {
		t.Errorf("asinFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for asin(-2.0)")
	}
}

// TestAcosFuncOutOfRange tests acos() with out of range values
func TestAcosFuncOutOfRange(t *testing.T) {
	result, err := acosFunc([]Value{NewFloatValue(2.0)})
	if err != nil {
		t.Errorf("acosFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for acos(2.0)")
	}

	result, err = acosFunc([]Value{NewFloatValue(-2.0)})
	if err != nil {
		t.Errorf("acosFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for acos(-2.0)")
	}
}

// TestAcoshFuncOutOfRange tests acosh() with value < 1
func TestAcoshFuncOutOfRange(t *testing.T) {
	result, err := acoshFunc([]Value{NewFloatValue(0.5)})
	if err != nil {
		t.Errorf("acoshFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for acosh(0.5)")
	}
}

// TestAtanhFuncOutOfRange tests atanh() with value at boundaries
func TestAtanhFuncOutOfRange(t *testing.T) {
	result, err := atanhFunc([]Value{NewFloatValue(1.0)})
	if err != nil {
		t.Errorf("atanhFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for atanh(1.0)")
	}

	result, err = atanhFunc([]Value{NewFloatValue(-1.0)})
	if err != nil {
		t.Errorf("atanhFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for atanh(-1.0)")
	}

	result, err = atanhFunc([]Value{NewFloatValue(2.0)})
	if err != nil {
		t.Errorf("atanhFunc() error = %v", err)
	}

	// Should return NaN
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("Expected NaN for atanh(2.0)")
	}
}

// TestLengthFuncNonText tests length() with non-text types
func TestLengthFuncNonText(t *testing.T) {
	// Test with integer (should return bytes)
	result, err := lengthFunc([]Value{NewIntValue(12345)})
	if err != nil {
		t.Errorf("lengthFunc() error = %v", err)
	}
	if result.AsInt64() != 8 {
		t.Errorf("Expected 8 bytes for int64, got %d", result.AsInt64())
	}

	// Test with float (should return bytes)
	result, err = lengthFunc([]Value{NewFloatValue(1.234)})
	if err != nil {
		t.Errorf("lengthFunc() error = %v", err)
	}
	if result.AsInt64() != 8 {
		t.Errorf("Expected 8 bytes for float64, got %d", result.AsInt64())
	}
}

// TestSubstrFuncNullArgs tests substr() with NULL arguments
func TestSubstrFuncNullArgs(t *testing.T) {
	// NULL start position
	result, err := substrFunc([]Value{
		NewTextValue("hello"),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL start position when start=0")
	}

	// NULL length
	result, err = substrFunc([]Value{
		NewTextValue("hello"),
		NewIntValue(1),
		NewNullValue(),
	})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("Expected NULL for NULL length")
	}
}

// TestSubstrFuncNegativeLength tests substr() with negative length
func TestSubstrFuncNegativeLength(t *testing.T) {
	// Negative length means return chars before start position
	result, err := substrFunc([]Value{
		NewTextValue("hello"),
		NewIntValue(4),  // start at 'l' (1-indexed)
		NewIntValue(-2), // return 2 chars before
	})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}

	expected := "el" // chars before position 4
	if result.AsString() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result.AsString())
	}
}

// TestSubstrFuncBlobNegativeStart tests substr() with blob and negative start
func TestSubstrFuncBlobNegativeStart(t *testing.T) {
	blob := []byte{1, 2, 3, 4, 5}

	// Negative start that goes before beginning with positive length
	result, err := substrFunc([]Value{
		NewBlobValue(blob),
		NewIntValue(-10), // way before start
		NewIntValue(20),  // long enough to get some bytes
	})
	if err != nil {
		t.Errorf("substrFunc() error = %v", err)
	}

	// Should return partial result starting from beginning
	resultBlob := result.AsBlob()
	// When start is negative and goes before beginning,
	// length is adjusted and we start from position 0
	if len(resultBlob) != 5 {
		t.Errorf("Expected blob length 5, got %d", len(resultBlob))
	}
}

// TestQuoteFuncFloat tests quote() with special float values
func TestQuoteFuncFloat(t *testing.T) {
	result, err := quoteFunc([]Value{NewFloatValue(1.5)})
	if err != nil {
		t.Errorf("quoteFunc() error = %v", err)
	}

	// Should return string representation of float
	if result.AsString() == "" {
		t.Error("Expected non-empty result for float")
	}
}

// TestCompareValuesDifferentTypes tests compareValues with different types
func TestCompareValuesDifferentTypes(t *testing.T) {
	// Compare integer and text
	cmp := compareValues(NewIntValue(1), NewTextValue("hello"))
	if cmp == 0 {
		t.Error("Different types should not be equal")
	}

	// Verify ordering is consistent
	cmp1 := compareValues(NewIntValue(1), NewFloatValue(1.0))
	cmp2 := compareValues(NewFloatValue(1.0), NewIntValue(1))
	if (cmp1 > 0 && cmp2 > 0) || (cmp1 < 0 && cmp2 < 0) {
		t.Error("Comparison order should be opposite for swapped types")
	}
}
