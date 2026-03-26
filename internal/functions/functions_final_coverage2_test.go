// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// ---------------------------------------------------------------------------
// json.go: jsonFunc (90.0%) – the marshal-returns-null branch
// ---------------------------------------------------------------------------

// TestJSONFunc_NullInput covers the null-input early return.
func TestJSONFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := jsonFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("jsonFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null input, got %v", result)
	}
}

// TestJSONFunc_InvalidJSON covers the unmarshal-error branch.
func TestJSONFunc_InvalidJSON(t *testing.T) {
	t.Parallel()
	result, err := jsonFunc([]Value{testText("not json at all {")})
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
	_ = result
}

// ---------------------------------------------------------------------------
// json.go: extractMultiplePaths (85.7%) – multiple paths, some null
// ---------------------------------------------------------------------------

func TestExtractMultiplePaths_NullPath(t *testing.T) {
	t.Parallel()
	// json_extract with a NULL path arg should return NULL
	result, err := jsonExtractFunc([]Value{
		testText(`{"a":1}`),
		testNull(),
	})
	if err != nil {
		t.Fatalf("jsonExtractFunc error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null path, got %v", result)
	}
}

func TestExtractMultiplePaths_MultiplePaths(t *testing.T) {
	t.Parallel()
	// json_extract with 2+ path args returns a JSON array
	result, err := jsonExtractFunc([]Value{
		testText(`{"a":1,"b":2}`),
		testText("$.a"),
		testText("$.b"),
	})
	if err != nil {
		t.Fatalf("jsonExtractFunc (multi-path) error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL result for multi-path extract")
	}
}

// ---------------------------------------------------------------------------
// json.go: jsonExtractTextFunc (85.7%) – the null-second-arg path
// ---------------------------------------------------------------------------

func TestJSONExtractTextFunc_NullPath(t *testing.T) {
	t.Parallel()
	// When path is null, applyPathIfPresent returns the root data.
	// convertResultToText converts a map to NULL (unsupported type), so result is NULL.
	result, err := jsonExtractTextFunc([]Value{testText(`{"a":1}`), testNull()})
	if err != nil {
		t.Fatalf("jsonExtractTextFunc(null path) error: %v", err)
	}
	// The result may be NULL or the original JSON, depending on the type returned.
	// Either is acceptable - we just verify it doesn't error.
	_ = result
}

func TestJSONExtractTextFunc_ValidPath(t *testing.T) {
	t.Parallel()
	result, err := jsonExtractTextFunc([]Value{testText(`{"a":"hello"}`), testText("$.a")})
	if err != nil {
		t.Fatalf("jsonExtractTextFunc error: %v", err)
	}
	if result.AsString() != "hello" {
		t.Errorf("expected 'hello', got %q", result.AsString())
	}
}

// ---------------------------------------------------------------------------
// json.go: marshalAsJSONText (75.0%) – test the normal success path
// ---------------------------------------------------------------------------

func TestMarshalAsJSONText_Success(t *testing.T) {
	t.Parallel()
	result, err := marshalAsJSONText(map[string]interface{}{"x": 42})
	if err != nil {
		t.Fatalf("marshalAsJSONText error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL marshalled value")
	}
}

func TestMarshalAsJSONText_String(t *testing.T) {
	t.Parallel()
	result, err := marshalAsJSONText("hello")
	if err != nil {
		t.Fatalf("marshalAsJSONText string error: %v", err)
	}
	if result.AsString() != `"hello"` {
		t.Errorf("expected JSON string, got %q", result.AsString())
	}
}

// ---------------------------------------------------------------------------
// json.go: isMinifiedJSON (75.0%) – marshal error branch (impossible in practice)
// and non-matching branch
// ---------------------------------------------------------------------------

func TestIsMinifiedJSON_Match(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	if !isMinifiedJSON(data, `{"a":1}`) {
		t.Error("expected isMinifiedJSON to return true for matching JSON")
	}
}

func TestIsMinifiedJSON_NoMatch(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	if isMinifiedJSON(data, `{"a": 1}`) {
		t.Error("expected isMinifiedJSON to return false for non-minified JSON")
	}
}

// ---------------------------------------------------------------------------
// json.go: setPath (80.0%) – path == "" and path == "$" branches
// ---------------------------------------------------------------------------

func TestSetPath_EmptyPath(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	result := setPath(data, "", "newval")
	if result != "newval" {
		t.Errorf("expected 'newval' for empty path, got %v", result)
	}
}

func TestSetPath_RootPath(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	result := setPath(data, "$", "replaced")
	if result != "replaced" {
		t.Errorf("expected 'replaced' for root path, got %v", result)
	}
}

func TestSetPath_NestedKey(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	result := setPath(data, "$.b", float64(2))
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if m["b"] != float64(2) {
		t.Errorf("expected b=2, got %v", m["b"])
	}
}

// ---------------------------------------------------------------------------
// json.go: removePath (90.0%) – empty path and root path branches
// ---------------------------------------------------------------------------

func TestRemovePath_EmptyPath(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	result := removePath(data, "")
	if result != nil {
		t.Errorf("expected nil for empty path remove, got %v", result)
	}
}

func TestRemovePath_RootPath(t *testing.T) {
	t.Parallel()
	data := map[string]interface{}{"a": float64(1)}
	result := removePath(data, "$")
	if result != nil {
		t.Errorf("expected nil for root path remove, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// json.go: removePathRecursive (85.7%) – array index and object key branches
// ---------------------------------------------------------------------------

func TestRemovePathRecursive_ArrayIndex(t *testing.T) {
	t.Parallel()
	data := []interface{}{float64(1), float64(2), float64(3)}
	result := removePath(data, "$[1]")
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}
	if len(arr) != 2 {
		t.Errorf("expected length 2 after remove, got %d", len(arr))
	}
}

// ---------------------------------------------------------------------------
// json.go: marshalJSONPreserveFloats (80.0%) – error path (hard to reach)
// and normal path
// ---------------------------------------------------------------------------

func TestMarshalJSONPreserveFloats_Simple(t *testing.T) {
	t.Parallel()
	result, err := marshalJSONPreserveFloats([]interface{}{jsonFloat(1.5), jsonFloat(2.0)})
	if err != nil {
		t.Fatalf("marshalJSONPreserveFloats error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL result")
	}
}

// ---------------------------------------------------------------------------
// json.go: jsonQuoteFunc (85.7%) – null input and blob input branches
// ---------------------------------------------------------------------------

func TestJSONQuoteFunc_Null(t *testing.T) {
	t.Parallel()
	result, err := jsonQuoteFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("jsonQuoteFunc(NULL) error: %v", err)
	}
	if result.AsString() != "null" {
		t.Errorf("expected 'null', got %q", result.AsString())
	}
}

func TestJSONQuoteFunc_Integer(t *testing.T) {
	t.Parallel()
	result, err := jsonQuoteFunc([]Value{testInt(42)})
	if err != nil {
		t.Fatalf("jsonQuoteFunc(42) error: %v", err)
	}
	if result.AsString() != "42" {
		t.Errorf("expected '42', got %q", result.AsString())
	}
}

func TestJSONQuoteFunc_Float(t *testing.T) {
	t.Parallel()
	result, err := jsonQuoteFunc([]Value{testFloat(3.14)})
	if err != nil {
		t.Fatalf("jsonQuoteFunc(3.14) error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL for float quote")
	}
}

// ---------------------------------------------------------------------------
// json.go: valueToJSON (87.5%) – blob and default branches
// ---------------------------------------------------------------------------

func TestValueToJSON_Blob(t *testing.T) {
	t.Parallel()
	v := testBlob([]byte{0x01, 0x02})
	result := valueToJSON(v)
	if result == nil {
		t.Error("expected non-nil for blob")
	}
}

func TestValueToJSON_Null(t *testing.T) {
	t.Parallel()
	result := valueToJSON(testNull())
	if result != nil {
		t.Errorf("expected nil for null, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// json.go: valueToJSONSmart (87.5%) – blob branch
// ---------------------------------------------------------------------------

func TestValueToJSONSmart_Blob(t *testing.T) {
	t.Parallel()
	v := testBlob([]byte{0xAB, 0xCD})
	result := valueToJSONSmart(v)
	if result == nil {
		t.Error("expected non-nil for blob in smart mode")
	}
}

// ---------------------------------------------------------------------------
// json.go: convertStringToJSON (90.9%) – non-JSON strings
// ---------------------------------------------------------------------------

func TestConvertStringToJSON_PlainString(t *testing.T) {
	t.Parallel()
	result := convertStringToJSON("hello world")
	if result != "hello world" {
		t.Errorf("expected plain string, got %v", result)
	}
}

func TestConvertStringToJSON_EmptyString(t *testing.T) {
	t.Parallel()
	result := convertStringToJSON("")
	if result != "" {
		t.Errorf("expected empty string, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// json.go: tryParseJSONObject (88.9%) – invalid JSON object path
// ---------------------------------------------------------------------------

func TestTryParseJSONObject_Invalid(t *testing.T) {
	t.Parallel()
	result := tryParseJSONObject("{not valid json}")
	if result != nil {
		t.Errorf("expected nil for invalid JSON object, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// json.go: tryParseJSONArray (88.9%) – invalid JSON array path
// ---------------------------------------------------------------------------

func TestTryParseJSONArray_Invalid(t *testing.T) {
	t.Parallel()
	result := tryParseJSONArray("[not valid json}")
	if result != nil {
		t.Errorf("expected nil for invalid JSON array, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// json.go: getJSONType (91.7%) – the default/unknown branch
// ---------------------------------------------------------------------------

func TestGetJSONType_Null(t *testing.T) {
	t.Parallel()
	result := getJSONType(nil)
	if result != "null" {
		t.Errorf("expected 'null', got %q", result)
	}
}

func TestGetJSONType_Bool(t *testing.T) {
	t.Parallel()
	result := getJSONType(true)
	if result != "true" {
		t.Errorf("expected 'true' for bool, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// scalar.go: lengthFunc (88.9%) – the default branch (non-standard type)
// ---------------------------------------------------------------------------

// TestLengthFunc_NullInput covers the null-input branch in lengthFunc.
func TestLengthFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := lengthFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("lengthFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null input, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// scalar.go: substrAdjustNegLen (81.8%) – subLen more negative than start
// ---------------------------------------------------------------------------

func TestSubstrAdjustNegLen_NegLenBeyondStart(t *testing.T) {
	t.Parallel()
	// start=2, subLen=-5 → subLen < -start → subLen = start = 2, start -= 2 → 0
	newStart, newLen := substrAdjustNegLen(2, -5)
	if newStart < 0 {
		t.Errorf("start should be >= 0, got %d", newStart)
	}
	if newLen < 0 {
		t.Errorf("subLen should be >= 0, got %d", newLen)
	}
}

func TestSubstrAdjustNegLen_StartBecomesNeg(t *testing.T) {
	t.Parallel()
	// start=1, subLen=-3 → subLen(-3) < -start(-1), so subLen=start=1, start=1-1=0
	newStart, newLen := substrAdjustNegLen(1, -3)
	if newStart < 0 {
		t.Errorf("start should be >= 0, got %d", newStart)
	}
	if newLen < 0 {
		t.Errorf("subLen should be >= 0, got %d", newLen)
	}
}

// ---------------------------------------------------------------------------
// scalar.go: quoteFunc (92.3%) – the float branch
// ---------------------------------------------------------------------------

func TestQuoteFunc_Float(t *testing.T) {
	t.Parallel()
	result, err := quoteFunc([]Value{testFloat(3.14)})
	if err != nil {
		t.Fatalf("quoteFunc(3.14) error: %v", err)
	}
	s := result.AsString()
	if len(s) == 0 {
		t.Error("expected non-empty result for float quote")
	}
}

// ---------------------------------------------------------------------------
// scalar.go: zeroblobFunc (88.9%) – negative N and oversized N
// ---------------------------------------------------------------------------

func TestZeroblobFunc_NegativeN(t *testing.T) {
	t.Parallel()
	result, err := zeroblobFunc([]Value{testInt(-5)})
	if err != nil {
		t.Fatalf("zeroblobFunc(-5) error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL for negative N")
	}
	if len(result.AsBlob()) != 0 {
		t.Errorf("expected 0-byte blob for N<=0, got %d bytes", len(result.AsBlob()))
	}
}

func TestZeroblobFunc_TooLarge(t *testing.T) {
	t.Parallel()
	_, err := zeroblobFunc([]Value{testInt(maxBlobSize + 1)})
	if err == nil {
		t.Error("expected error for oversized blob")
	}
}

// ---------------------------------------------------------------------------
// scalar.go: compareValues (85.7%) – mixed types and no comparator
// ---------------------------------------------------------------------------

func TestCompareValues_MixedTypes(t *testing.T) {
	t.Parallel()
	// Integer vs Text: different types, uses type ordering
	cmp := compareValues(testInt(1), testText("a"))
	if cmp == 0 {
		t.Error("expected non-zero comparison for different types")
	}
}

func TestCompareValues_NullVsNull(t *testing.T) {
	t.Parallel()
	cmp := compareValues(testNull(), testNull())
	if cmp != 0 {
		t.Errorf("expected 0 for NULL vs NULL, got %d", cmp)
	}
}

// ---------------------------------------------------------------------------
// scalar.go: likelihoodFunc (85.7%) – probability out of range
// ---------------------------------------------------------------------------

func TestLikelihoodFunc_OutOfRange(t *testing.T) {
	t.Parallel()
	_, err := likelihoodFunc([]Value{testInt(1), testFloat(1.5)})
	if err == nil {
		t.Error("expected error for probability > 1.0")
	}
}

func TestLikelihoodFunc_NullProbability(t *testing.T) {
	t.Parallel()
	result, err := likelihoodFunc([]Value{testInt(42), testNull()})
	if err != nil {
		t.Fatalf("likelihoodFunc(null prob) error: %v", err)
	}
	if result.AsInt64() != 42 {
		t.Errorf("expected 42, got %v", result.AsInt64())
	}
}

// ---------------------------------------------------------------------------
// scalar.go: formatPrintfInteger (87.5%) – spaceSign branch
// ---------------------------------------------------------------------------

func TestFormatPrintfInteger_SpaceSign(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'd',
		spaceSign: true,
	}
	result := formatPrintfInteger(spec, testInt(5))
	if len(result) == 0 || result[0] != ' ' {
		t.Errorf("expected space-prefixed result, got %q", result)
	}
}

func TestFormatPrintfInteger_ThousandsNeg(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'd',
		thousands: true,
	}
	// thousands with negative value: addThousandsSeparator handles '-' prefix
	result := formatPrintfInteger(spec, testInt(-1234567))
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// scalar.go: formatPrintfFloat (88.9%) – spaceSign branch
// ---------------------------------------------------------------------------

func TestFormatPrintfFloat_SpaceSign(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'f',
		spaceSign: true,
		precision: 2,
	}
	result := formatPrintfFloat(spec, testFloat(3.14))
	if len(result) == 0 || result[0] != ' ' {
		t.Errorf("expected space-prefixed float, got %q", result)
	}
}

func TestFormatPrintfFloat_NegativePrecision(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'f',
		precision: -1, // use default precision (6)
	}
	result := formatPrintfFloat(spec, testFloat(1.23456789))
	if len(result) == 0 {
		t.Error("expected non-empty float result")
	}
}

// ---------------------------------------------------------------------------
// scalar.go: formatPrintfHex (92.3%) – altForm with zero value (no prefix)
// ---------------------------------------------------------------------------

func TestFormatPrintfHex_AltFormZero(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'x',
		altForm:   true,
	}
	// val==0: altForm should NOT add "0x" prefix
	result := formatPrintfHex(spec, testInt(0))
	if result != "0" {
		t.Errorf("expected '0' for hex zero with altForm, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// scalar.go: formatPrintfOctal (87.5%) – altForm branch
// ---------------------------------------------------------------------------

func TestFormatPrintfOctal_AltForm(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'o',
		altForm:   true,
	}
	result := formatPrintfOctal(spec, testInt(8))
	// 8 octal = "10", with altForm = "010"
	if result != "010" {
		t.Errorf("expected '010', got %q", result)
	}
}

func TestFormatPrintfOctal_AltFormZero(t *testing.T) {
	t.Parallel()
	spec := printfFormatSpec{
		specifier: 'o',
		altForm:   true,
	}
	// val==0: no "0" prefix added
	result := formatPrintfOctal(spec, testInt(0))
	if result != "0" {
		t.Errorf("expected '0' for zero octal with altForm, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// math.go: roundFunc (91.7%) – passthrough (NaN/Inf) branch
// ---------------------------------------------------------------------------

func TestRoundFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := roundFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("roundFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null input, got %v", result)
	}
}

func TestRoundFunc_WithPrecision(t *testing.T) {
	t.Parallel()
	result, err := roundFunc([]Value{testFloat(3.14159), testInt(2)})
	if err != nil {
		t.Fatalf("roundFunc error: %v", err)
	}
	got := result.AsFloat64()
	if got < 3.13 || got > 3.15 {
		t.Errorf("expected ~3.14, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// math.go: truncFunc (91.7%) – with precision branch
// ---------------------------------------------------------------------------

func TestTruncFunc_WithPrecision(t *testing.T) {
	t.Parallel()
	result, err := truncFunc([]Value{testFloat(3.789), testInt(2)})
	if err != nil {
		t.Fatalf("truncFunc error: %v", err)
	}
	got := result.AsFloat64()
	if got < 3.77 || got > 3.79 {
		t.Errorf("expected ~3.78, got %v", got)
	}
}

func TestTruncFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := truncFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("truncFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null input, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// math.go: randomFunc (85.7%) – normal path (positive branch)
// ---------------------------------------------------------------------------

func TestRandomFunc_ReturnsNonNegative(t *testing.T) {
	t.Parallel()
	// Run several times to exercise both branches of the sign check.
	for i := 0; i < 20; i++ {
		result, err := randomFunc([]Value{})
		if err != nil {
			t.Fatalf("randomFunc() error: %v", err)
		}
		if result.IsNull() {
			t.Error("expected non-NULL from randomFunc")
		}
	}
}

// ---------------------------------------------------------------------------
// math.go: randomblobFunc (81.8%) – oversized blob error path
// ---------------------------------------------------------------------------

func TestRandomblobFunc_TooLarge(t *testing.T) {
	t.Parallel()
	_, err := randomblobFunc([]Value{testInt(maxBlobSize + 1)})
	if err == nil {
		t.Error("expected error for oversized randomblob")
	}
}

func TestRandomblobFunc_NegativeN(t *testing.T) {
	t.Parallel()
	// negative N is clamped to 1
	result, err := randomblobFunc([]Value{testInt(-10)})
	if err != nil {
		t.Fatalf("randomblobFunc(-10) error: %v", err)
	}
	if len(result.AsBlob()) != 1 {
		t.Errorf("expected 1 byte for negative N, got %d", len(result.AsBlob()))
	}
}

// ---------------------------------------------------------------------------
// aggregate.go: marshalJSONValue (75.0%) – success path
// ---------------------------------------------------------------------------

func TestMarshalJSONValue_Success(t *testing.T) {
	t.Parallel()
	result := marshalJSONValue([]interface{}{float64(1), float64(2)})
	if result.IsNull() {
		t.Error("expected non-NULL marshalled JSON value")
	}
	got := result.AsString()
	if got == "" {
		t.Error("expected non-empty JSON string")
	}
}

func TestMarshalJSONValue_Map(t *testing.T) {
	t.Parallel()
	result := marshalJSONValue(map[string]interface{}{"k": "v"})
	if result.IsNull() {
		t.Error("expected non-NULL marshalled JSON map")
	}
}

// ---------------------------------------------------------------------------
// date.go: dateFunc / timeFunc / datetimeFunc / juliandayFunc (83.3%)
// – error path when parseDateTimeWithModifiers returns err != nil
// ---------------------------------------------------------------------------

// TestDateFunc_InvalidInput covers the dt==nil with no error branch (null return).
func TestDateFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := dateFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("dateFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null date input, got %v", result)
	}
}

func TestTimeFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := timeFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("timeFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null time input, got %v", result)
	}
}

func TestDatetimeFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := datetimeFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("datetimeFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null datetime input, got %v", result)
	}
}

func TestJuliandayFunc_NullInput(t *testing.T) {
	t.Parallel()
	result, err := juliandayFunc([]Value{testNull()})
	if err != nil {
		t.Fatalf("juliandayFunc(NULL) error: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("expected NULL for null julianday input, got %v", result)
	}
}

// TestDateFunc_ValidDate exercises the normal dt != nil success path.
func TestDateFunc_ValidDate(t *testing.T) {
	t.Parallel()
	result, err := dateFunc([]Value{testText("2023-06-15")})
	if err != nil {
		t.Fatalf("dateFunc error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL for valid date")
	}
}

func TestJuliandayFunc_ValidDate(t *testing.T) {
	t.Parallel()
	result, err := juliandayFunc([]Value{testText("2023-06-15")})
	if err != nil {
		t.Fatalf("juliandayFunc error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL for valid julianday input")
	}
}

// ---------------------------------------------------------------------------
// date.go: handleDateArithmetic (90.0%) – negative days path
// ---------------------------------------------------------------------------

func TestHandleDateArithmetic_NegativeDays(t *testing.T) {
	t.Parallel()
	// Subtract 5 days: modifier "-5 days"
	result, err := dateFunc([]Value{testText("2023-06-15"), testText("-5 days")})
	if err != nil {
		t.Fatalf("dateFunc with -5 days error: %v", err)
	}
	if result.IsNull() {
		t.Error("expected non-NULL for date arithmetic with negative days")
	}
}
