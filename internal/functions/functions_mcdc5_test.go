// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// ---------------------------------------------------------------------------
// marshalJSONValue (aggregate.go:463) — 75.0%
// The error branch (json.Marshal returns error) is unreachable for most types,
// but the happy path is the only realistic branch to exercise through the API.
// We exercise it via the json_group_object aggregate which calls marshalJSONValue.
// ---------------------------------------------------------------------------

func TestMarshalJSONValue_HappyPath(t *testing.T) {
	// marshalJSONValue returns NewTextValue("[]") on error, or the marshaled text.
	// Call it directly (it is package-private).
	v := marshalJSONValue(map[string]interface{}{"k": "v"})
	got := v.AsString()
	if got != `{"k":"v"}` {
		t.Errorf("marshalJSONValue map: want {\"k\":\"v\"}, got %s", got)
	}
}

func TestMarshalJSONValue_Array(t *testing.T) {
	v := marshalJSONValue([]interface{}{1, 2, 3})
	got := v.AsString()
	if got != `[1,2,3]` {
		t.Errorf("marshalJSONValue array: want [1,2,3], got %s", got)
	}
}

// ---------------------------------------------------------------------------
// handleDateArithmetic (date.go:473) — 90.0%
// Missing: len(parts) < 2 branch (parts split but fewer than 2 tokens).
// A modifier like " " splits to zero fields, so parts < 2.
// ---------------------------------------------------------------------------

func TestHandleDateArithmetic_NoSpace(t *testing.T) {
	dt := &DateTime{}
	dt.setNow()
	handled, err := dt.handleDateArithmetic("nospace")
	if handled || err != nil {
		t.Errorf("no space: want handled=false err=nil; got handled=%v err=%v", handled, err)
	}
}

func TestHandleDateArithmetic_InvalidNumber(t *testing.T) {
	dt := &DateTime{}
	dt.setNow()
	// parts[0] is not a number
	handled, err := dt.handleDateArithmetic("notanumber days")
	if handled || err != nil {
		t.Errorf("invalid number: want handled=false err=nil; got %v %v", handled, err)
	}
}

// ---------------------------------------------------------------------------
// dateFunc / timeFunc / datetimeFunc / juliandayFunc (date.go:804-846) — 83.3%
// The uncovered branch is err!=nil path (dt==nil && err!=nil).
// Pass an invalid date string to trigger parse failure.
// ---------------------------------------------------------------------------

func TestDateFunc_InvalidDate_ReturnsNull(t *testing.T) {
	// Invalid dates return NULL (not error) from date functions.
	v, err := dateFunc([]Value{NewTextValue("not-a-date")})
	if err != nil {
		// Some implementations may return error instead of null; both are acceptable.
		return
	}
	if v != nil && !v.IsNull() {
		t.Errorf("dateFunc with invalid date: expected null or error, got %v", v)
	}
}

func TestDateFunc_NullArg(t *testing.T) {
	v, err := dateFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("dateFunc null: unexpected error: %v", err)
	}
	if v == nil || !v.IsNull() {
		t.Error("dateFunc null: expected null result")
	}
}

func TestTimeFunc_InvalidDate_ReturnsNull(t *testing.T) {
	v, err := timeFunc([]Value{NewTextValue("not-a-date")})
	if err != nil {
		return
	}
	if v != nil && !v.IsNull() {
		t.Errorf("timeFunc with invalid date: expected null or error, got %v", v)
	}
}

func TestDatetimeFunc_InvalidDate_ReturnsNull(t *testing.T) {
	v, err := datetimeFunc([]Value{NewTextValue("not-a-date")})
	if err != nil {
		return
	}
	if v != nil && !v.IsNull() {
		t.Errorf("datetimeFunc with invalid date: expected null or error, got %v", v)
	}
}

func TestJuliandayFunc_InvalidDate_ReturnsNull(t *testing.T) {
	v, err := juliandayFunc([]Value{NewTextValue("not-a-date")})
	if err != nil {
		return
	}
	if v != nil && !v.IsNull() {
		t.Errorf("juliandayFunc with invalid date: expected null or error, got %v", v)
	}
}

// ---------------------------------------------------------------------------
// jsonFunc (json.go:31) — 90.0%
// Uncovered: the json.Marshal failure path after Unmarshal succeeds.
// That path is unreachable in practice; cover the null path instead.
// ---------------------------------------------------------------------------

func TestJsonFunc_Null(t *testing.T) {
	v, err := jsonFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("jsonFunc null: unexpected error: %v", err)
	}
	if !v.IsNull() {
		t.Error("jsonFunc null: expected null result")
	}
}

func TestJsonFunc_InvalidJSON(t *testing.T) {
	_, err := jsonFunc([]Value{NewTextValue("not json")})
	if err == nil {
		t.Error("jsonFunc invalid JSON: expected error")
	}
}

// ---------------------------------------------------------------------------
// extractMultiplePaths (json.go:150) — 85.7%
// Uncovered: null pathArg (extractPathOrNil null-arg branch).
// ---------------------------------------------------------------------------

func TestMCDC5_ExtractMultiplePaths_NullPath(t *testing.T) {
	data := map[string]interface{}{"a": "hello"}
	paths := []Value{NewNullValue(), NewTextValue("$.a")}
	v, err := extractMultiplePaths(data, paths)
	if err != nil {
		t.Errorf("extractMultiplePaths: unexpected error: %v", err)
	}
	got := v.AsString()
	// null path produces null in JSON array
	if got == "" {
		t.Error("extractMultiplePaths: expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// jsonExtractTextFunc (json.go:175) — 85.7%
// Uncovered: complex/object result path → marshalAsJSONText.
// ---------------------------------------------------------------------------

func TestJsonExtractTextFunc_ObjectResult(t *testing.T) {
	// json_extract_text on a nested object returns JSON text of the object
	args := []Value{
		NewTextValue(`{"a":{"b":1}}`),
		NewTextValue("$.a"),
	}
	v, err := jsonExtractTextFunc(args)
	if err != nil {
		t.Errorf("jsonExtractTextFunc object: unexpected error: %v", err)
	}
	got := v.AsString()
	if got == "" {
		t.Error("jsonExtractTextFunc object: expected non-empty result")
	}
}

func TestJsonExtractTextFunc_BoolResult(t *testing.T) {
	args := []Value{
		NewTextValue(`{"flag":true}`),
		NewTextValue("$.flag"),
	}
	v, err := jsonExtractTextFunc(args)
	if err != nil {
		t.Errorf("jsonExtractTextFunc bool: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "true" {
		t.Errorf("jsonExtractTextFunc bool: want 'true', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// marshalAsJSONText (json.go:238) — 75.0%
// Invoked via convertResultToText for complex types.
// ---------------------------------------------------------------------------

func TestMarshalAsJSONText_Array(t *testing.T) {
	v, err := marshalAsJSONText([]interface{}{1, 2, 3})
	if err != nil {
		t.Errorf("marshalAsJSONText array: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "[1,2,3]" {
		t.Errorf("marshalAsJSONText array: want [1,2,3], got %s", got)
	}
}

// ---------------------------------------------------------------------------
// jsonPatchFunc (json.go:282) — 94.1%
// Uncovered: Marshal error after patch (in practice unreachable).
// Cover the NULL-patch-returns-original path.
// ---------------------------------------------------------------------------

func TestJsonPatchFunc_NullPatch(t *testing.T) {
	// NULL patch returns original document
	args := []Value{NewTextValue(`{"a":1}`), NewNullValue()}
	v, err := jsonPatchFunc(args)
	if err != nil {
		t.Errorf("jsonPatchFunc null patch: unexpected error: %v", err)
	}
	if v.AsString() != `{"a":1}` {
		t.Errorf("jsonPatchFunc null patch: want {\"a\":1}, got %s", v.AsString())
	}
}

func TestJsonPatchFunc_InvalidTarget(t *testing.T) {
	// Invalid JSON target → returns null
	args := []Value{NewTextValue("not json"), NewTextValue(`{}`)}
	v, err := jsonPatchFunc(args)
	if err != nil {
		t.Errorf("jsonPatchFunc invalid target: unexpected error: %v", err)
	}
	if !v.IsNull() {
		t.Errorf("jsonPatchFunc invalid target: expected null, got %s", v.AsString())
	}
}

func TestJsonPatchFunc_InvalidPatch(t *testing.T) {
	// Invalid JSON patch → returns null
	args := []Value{NewTextValue(`{"a":1}`), NewTextValue("not json")}
	v, err := jsonPatchFunc(args)
	if err != nil {
		t.Errorf("jsonPatchFunc invalid patch: unexpected error: %v", err)
	}
	if !v.IsNull() {
		t.Errorf("jsonPatchFunc invalid patch: expected null, got %s", v.AsString())
	}
}

// ---------------------------------------------------------------------------
// jsonRemoveFunc (json.go:315) — 94.1%
// Uncovered: null path arg in the loop (args[i].IsNull() continue path).
// ---------------------------------------------------------------------------

func TestJsonRemoveFunc_NullPathArg(t *testing.T) {
	// json_remove with a null path arg (should skip it)
	args := []Value{
		NewTextValue(`{"a":1,"b":2}`),
		NewNullValue(),
		NewTextValue("$.b"),
	}
	v, err := jsonRemoveFunc(args)
	if err != nil {
		t.Errorf("jsonRemoveFunc null path: unexpected error: %v", err)
	}
	got := v.AsString()
	if got == "" {
		t.Error("jsonRemoveFunc null path: expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// processPathValuePairs (json.go:362) — 90.0%
// Uncovered: null first arg (parseJSONArg null path → NewNullValue).
// ---------------------------------------------------------------------------

func TestProcessPathValuePairs_NullFirstArg(t *testing.T) {
	// json_set with null JSON → returns null
	args := []Value{NewNullValue(), NewTextValue("$.a"), NewTextValue("x")}
	v, err := jsonSetFunc(args)
	if err != nil {
		t.Errorf("jsonSetFunc null first: unexpected error: %v", err)
	}
	if !v.IsNull() {
		t.Errorf("jsonSetFunc null first: expected null, got %s", v.AsString())
	}
}

// ---------------------------------------------------------------------------
// jsonSetFunc (json.go:400) — 90.0%
// Uncovered: null path arg in pathValueArgs (applyPathValuePairs null skip).
// ---------------------------------------------------------------------------

func TestJsonSetFunc_NullPathArg(t *testing.T) {
	args := []Value{
		NewTextValue(`{"a":1}`),
		NewNullValue(),
		NewTextValue("newval"),
	}
	v, err := jsonSetFunc(args)
	if err != nil {
		t.Errorf("jsonSetFunc null path arg: unexpected error: %v", err)
	}
	// null path arg is skipped, object unchanged
	got := v.AsString()
	if got == "" {
		t.Error("jsonSetFunc null path arg: expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// jsonQuoteFunc (json.go:495) — 85.7%
// Uncovered: the json.Marshal error path (unreachable for standard types).
// Cover the null → "null" path.
// ---------------------------------------------------------------------------

func TestJsonQuoteFunc_Null(t *testing.T) {
	v, err := jsonQuoteFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("jsonQuoteFunc null: unexpected error: %v", err)
	}
	if v.AsString() != "null" {
		t.Errorf("jsonQuoteFunc null: want 'null', got %s", v.AsString())
	}
}

func TestJsonQuoteFunc_Integer(t *testing.T) {
	v, err := jsonQuoteFunc([]Value{NewIntValue(42)})
	if err != nil {
		t.Errorf("jsonQuoteFunc int: unexpected error: %v", err)
	}
	if v.AsString() != "42" {
		t.Errorf("jsonQuoteFunc int: want '42', got %s", v.AsString())
	}
}

// ---------------------------------------------------------------------------
// valueToJSON (json.go:514) — 87.5%
// Uncovered: TypeBlob branch.
// ---------------------------------------------------------------------------

func TestMCDC5_ValueToJSON_Blob(t *testing.T) {
	v := NewBlobValue([]byte{0x01, 0x02})
	result := valueToJSON(v)
	if result == nil {
		t.Error("valueToJSON blob: expected non-nil result")
	}
}

// ---------------------------------------------------------------------------
// valueToJSONSmart (json.go:540) — 87.5%
// Uncovered: TypeBlob branch.
// ---------------------------------------------------------------------------

func TestMCDC5_ValueToJSONSmart_Blob(t *testing.T) {
	v := NewBlobValue([]byte{0x01, 0x02})
	result := valueToJSONSmart(v)
	if result == nil {
		t.Error("valueToJSONSmart blob: expected non-nil result")
	}
}

// ---------------------------------------------------------------------------
// tryParseJSONObject (json.go:588) — 88.9%
// Uncovered: non-object result from Unmarshal (e.g. array starting with '{' - impossible,
// but we can cover the !ok branch via a carefully crafted invalid JSON object).
// Cover the isMinifiedJSON=false path (pretty-printed JSON).
// ---------------------------------------------------------------------------

func TestTryParseJSONObject_NotMinified(t *testing.T) {
	// Pretty-printed JSON → isMinifiedJSON returns false → result nil
	result := tryParseJSONObject(`{ "a": 1 }`)
	if result != nil {
		t.Error("tryParseJSONObject not-minified: expected nil")
	}
}

func TestTryParseJSONObject_Minified(t *testing.T) {
	result := tryParseJSONObject(`{"a":1}`)
	if result == nil {
		t.Error("tryParseJSONObject minified: expected non-nil")
	}
}

// ---------------------------------------------------------------------------
// tryParseJSONArray (json.go:609) — 88.9%
// Uncovered: isMinifiedJSON=false path.
// ---------------------------------------------------------------------------

func TestTryParseJSONArray_NotMinified(t *testing.T) {
	result := tryParseJSONArray(`[ 1, 2 ]`)
	if result != nil {
		t.Error("tryParseJSONArray not-minified: expected nil")
	}
}

// ---------------------------------------------------------------------------
// isMinifiedJSON (json.go:629) — 75.0%
// Uncovered: json.Marshal error (unreachable for valid data).
// Cover the happy path: minified matches and does not match.
// ---------------------------------------------------------------------------

func TestMCDC5_IsMinifiedJSON_Match(t *testing.T) {
	data := map[string]interface{}{"k": "v"}
	if !isMinifiedJSON(data, `{"k":"v"}`) {
		t.Error("isMinifiedJSON: expected true for minified match")
	}
}

func TestMCDC5_IsMinifiedJSON_NoMatch(t *testing.T) {
	data := map[string]interface{}{"k": "v"}
	if isMinifiedJSON(data, `{ "k": "v" }`) {
		t.Error("isMinifiedJSON: expected false for non-minified")
	}
}

// ---------------------------------------------------------------------------
// setPath (json.go:789) — 80.0%
// Uncovered: path == "$" → return value directly.
// ---------------------------------------------------------------------------

func TestSetPath_DollarOnly(t *testing.T) {
	result := setPath(map[string]interface{}{"a": 1}, "$", "replaced")
	s, ok := result.(string)
	if !ok || s != "replaced" {
		t.Errorf("setPath '$': want 'replaced', got %v", result)
	}
}

func TestMCDC5_SetPath_EmptyPath(t *testing.T) {
	result := setPath(map[string]interface{}{"a": 1}, "", "replaced")
	s, ok := result.(string)
	if !ok || s != "replaced" {
		t.Errorf("setPath empty: want 'replaced', got %v", result)
	}
}

// ---------------------------------------------------------------------------
// removePath (json.go:863) — 90.0%
// Uncovered: path == "$" → returns nil.
// ---------------------------------------------------------------------------

func TestRemovePath_DollarOnly(t *testing.T) {
	result := removePath(map[string]interface{}{"a": 1}, "$")
	if result != nil {
		t.Errorf("removePath '$': want nil, got %v", result)
	}
}

func TestMCDC5_RemovePath_EmptyPath(t *testing.T) {
	result := removePath(map[string]interface{}{"a": 1}, "")
	if result != nil {
		t.Errorf("removePath empty: want nil, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// removePathRecursive / removeFromArray (json.go:887) — 85.7%
// Uncovered: index removal from array path.
// ---------------------------------------------------------------------------

func TestMCDC5_RemovePathRecursive_ArrayIndex(t *testing.T) {
	// Remove element at index 1 from array
	v, err := jsonRemoveFunc([]Value{
		NewTextValue(`[10,20,30]`),
		NewTextValue("$[1]"),
	})
	if err != nil {
		t.Errorf("jsonRemoveFunc array index: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "[10,30]" {
		t.Errorf("jsonRemoveFunc array index: want [10,30], got %s", got)
	}
}

// ---------------------------------------------------------------------------
// marshalJSONPreserveFloats (json.go:1077) — 80.0%
// Uncovered: the error path from json.Marshal (unreachable). Cover array path.
// ---------------------------------------------------------------------------

func TestMarshalJSONPreserveFloats_Array(t *testing.T) {
	v, err := marshalJSONPreserveFloats([]interface{}{1.5, 2.0, "text"})
	if err != nil {
		t.Errorf("marshalJSONPreserveFloats array: unexpected error: %v", err)
	}
	if v.AsString() == "" {
		t.Error("marshalJSONPreserveFloats array: expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// jsonValueToString (json_table.go:268) — 83.3%
// Uncovered: bool false branch.
// ---------------------------------------------------------------------------

func TestJsonValueToString_BoolFalse(t *testing.T) {
	got := jsonValueToString(false)
	if got != "false" {
		t.Errorf("jsonValueToString false: want 'false', got %s", got)
	}
}

func TestJsonValueToString_BoolTrue(t *testing.T) {
	got := jsonValueToString(true)
	if got != "true" {
		t.Errorf("jsonValueToString true: want 'true', got %s", got)
	}
}

func TestJsonValueToString_Nil(t *testing.T) {
	got := jsonValueToString(nil)
	if got != "null" {
		t.Errorf("jsonValueToString nil: want 'null', got %s", got)
	}
}

func TestJsonValueToString_Float_Integer(t *testing.T) {
	got := jsonValueToString(float64(42))
	if got != "42" {
		t.Errorf("jsonValueToString float-as-int: want '42', got %s", got)
	}
}

func TestJsonValueToString_Float_Decimal(t *testing.T) {
	got := jsonValueToString(3.14)
	if got == "" {
		t.Error("jsonValueToString float decimal: expected non-empty result")
	}
}

// ---------------------------------------------------------------------------
// makeEachRow (json_table.go:125) — 90.9%
// Uncovered: parent == nil branch (parentVal stays NewNullValue()).
// ---------------------------------------------------------------------------

func TestMakeEachRow_NilParent(t *testing.T) {
	id := 0
	row := makeEachRow(NewTextValue("key"), "val", "$.key", "$", &id, nil)
	if len(row) != 8 {
		t.Errorf("makeEachRow nil parent: want 8 columns, got %d", len(row))
	}
	// parent column (index 5) should be null
	if !row[5].IsNull() {
		t.Errorf("makeEachRow nil parent: parent col should be null, got %v", row[5])
	}
}

// ---------------------------------------------------------------------------
// truncFunc (math.go:140) — 91.7%
// Uncovered: precision != 0 with positive precision (non-passthrough, non-zero precision).
// ---------------------------------------------------------------------------

func TestMCDC5_TruncFunc_WithPrecision(t *testing.T) {
	v, err := truncFunc([]Value{NewFloatValue(3.14159), NewIntValue(2)})
	if err != nil {
		t.Errorf("truncFunc with precision: unexpected error: %v", err)
	}
	got := v.AsFloat64()
	if got != 3.14 {
		t.Errorf("truncFunc(3.14159, 2): want 3.14, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// randomFunc (math.go:161) — 85.7%
// Covers the r < 0 negation path by simply calling many times.
// ---------------------------------------------------------------------------

func TestMCDC5_RandomFunc_ReturnsValue(t *testing.T) {
	// randomFunc returns a pseudo-random int64; just verify no error and that
	// calling it multiple times covers both the negative-clamping and positive paths.
	for i := 0; i < 20; i++ {
		v, err := randomFunc([]Value{})
		if err != nil {
			t.Errorf("randomFunc: unexpected error: %v", err)
		}
		if v == nil {
			t.Error("randomFunc: returned nil value")
		}
	}
}

// ---------------------------------------------------------------------------
// randomblobFunc (math.go:180) — 90.9%
// Uncovered: n < 1 → clamp to 1.
// ---------------------------------------------------------------------------

func TestRandomblobFunc_ZeroN(t *testing.T) {
	v, err := randomblobFunc([]Value{NewIntValue(0)})
	if err != nil {
		t.Errorf("randomblobFunc(0): unexpected error: %v", err)
	}
	if len(v.AsBlob()) != 1 {
		t.Errorf("randomblobFunc(0): want 1 byte, got %d", len(v.AsBlob()))
	}
}

func TestMCDC5_RandomblobFunc_NegativeN(t *testing.T) {
	v, err := randomblobFunc([]Value{NewIntValue(-5)})
	if err != nil {
		t.Errorf("randomblobFunc(-5): unexpected error: %v", err)
	}
	if len(v.AsBlob()) != 1 {
		t.Errorf("randomblobFunc(-5): want 1 byte, got %d", len(v.AsBlob()))
	}
}

// ---------------------------------------------------------------------------
// lengthFunc (scalar.go:58) — 88.9%
// Uncovered: TypeInteger/TypeFloat branch.
// ---------------------------------------------------------------------------

func TestLengthFunc_IntegerArg(t *testing.T) {
	v, err := lengthFunc([]Value{NewIntValue(12345)})
	if err != nil {
		t.Errorf("lengthFunc integer: unexpected error: %v", err)
	}
	// "12345" has 5 characters
	if v.AsInt64() != 5 {
		t.Errorf("lengthFunc(12345): want 5, got %d", v.AsInt64())
	}
}

func TestLengthFunc_FloatArg(t *testing.T) {
	v, err := lengthFunc([]Value{NewFloatValue(3.14)})
	if err != nil {
		t.Errorf("lengthFunc float: unexpected error: %v", err)
	}
	if v.AsInt64() == 0 {
		t.Error("lengthFunc float: expected non-zero length")
	}
}

// ---------------------------------------------------------------------------
// substrFunc (scalar.go:80) — 94.4%
// Uncovered: isBlob path (TypeBlob → substrBlobResult).
// ---------------------------------------------------------------------------

func TestSubstrFunc_BlobArg(t *testing.T) {
	blob := NewBlobValue([]byte{0x41, 0x42, 0x43, 0x44}) // ABCD
	v, err := substrFunc([]Value{blob, NewIntValue(2), NewIntValue(2)})
	if err != nil {
		t.Errorf("substrFunc blob: unexpected error: %v", err)
	}
	if len(v.AsBlob()) != 2 {
		t.Errorf("substrFunc blob: want 2 bytes, got %d", len(v.AsBlob()))
	}
}

// ---------------------------------------------------------------------------
// substrAdjustNegLen (scalar.go:174) — 81.8%
// Uncovered: subLen < -start path (negative len bigger than start).
// ---------------------------------------------------------------------------

func TestSubstrAdjustNegLen_LargeNegLen(t *testing.T) {
	// start=2, subLen=-10 → subLen < -start(-2 < -10 is false, so subLen = start = 2)
	start, subLen := substrAdjustNegLen(2, -10)
	// subLen > start magnitude, so subLen = start = 2
	if subLen != 2 || start != 0 {
		t.Errorf("substrAdjustNegLen(2,-10): want start=0 subLen=2, got start=%d subLen=%d", start, subLen)
	}
}

func TestSubstrAdjustNegLen_SmallNegLen(t *testing.T) {
	// start=5, subLen=-3 → -3 >= -5 so subLen=-(-3)=3, start=5-3=2
	start, subLen := substrAdjustNegLen(5, -3)
	if subLen != 3 || start != 2 {
		t.Errorf("substrAdjustNegLen(5,-3): want start=2 subLen=3, got start=%d subLen=%d", start, subLen)
	}
}

// ---------------------------------------------------------------------------
// quoteFunc (scalar.go:419) — 92.3%
// Uncovered: default branch (TypeBlob).
// ---------------------------------------------------------------------------

func TestQuoteFunc_Blob(t *testing.T) {
	v, err := quoteFunc([]Value{NewBlobValue([]byte{0xDE, 0xAD})})
	if err != nil {
		t.Errorf("quoteFunc blob: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "X'DEAD'" {
		t.Errorf("quoteFunc blob: want X'DEAD', got %s", got)
	}
}

// ---------------------------------------------------------------------------
// compareValues (scalar.go:613) — 85.7%
// Uncovered: types differ → int(a.Type())-int(b.Type()).
// Also covers the typeComparators miss (unknown type → return 0).
// ---------------------------------------------------------------------------

func TestCompareValues_DifferentTypes(t *testing.T) {
	// Integer vs Text → type ordering
	result := compareValues(NewIntValue(1), NewTextValue("a"))
	if result == 0 {
		t.Error("compareValues int vs text: expected non-zero (type ordering)")
	}
}

func TestCompareValues_BothNull(t *testing.T) {
	result := compareValues(NewNullValue(), NewNullValue())
	if result != 0 {
		t.Errorf("compareValues null vs null: want 0, got %d", result)
	}
}

// ---------------------------------------------------------------------------
// likelihoodFunc (scalar.go:642) — 85.7%
// Uncovered: null probability arg (skips validation) and error path (prob out of range).
// ---------------------------------------------------------------------------

func TestLikelihoodFunc_NullProb(t *testing.T) {
	v, err := likelihoodFunc([]Value{NewIntValue(1), NewNullValue()})
	if err != nil {
		t.Errorf("likelihoodFunc null prob: unexpected error: %v", err)
	}
	if v.AsInt64() != 1 {
		t.Errorf("likelihoodFunc null prob: want 1, got %v", v.AsInt64())
	}
}

func TestLikelihoodFunc_OutOfRangeProb(t *testing.T) {
	_, err := likelihoodFunc([]Value{NewIntValue(1), NewFloatValue(1.5)})
	if err == nil {
		t.Error("likelihoodFunc out-of-range: expected error")
	}
}

// ---------------------------------------------------------------------------
// formatPrintfInteger (scalar.go:758) — 93.8%
// Uncovered: spaceSign path (val >= 0 && !showSign && spaceSign).
// ---------------------------------------------------------------------------

func TestMCDC5_FormatPrintfInteger_SpaceSign(t *testing.T) {
	v, err := printfFunc([]Value{NewTextValue("% d"), NewIntValue(42)})
	if err != nil {
		t.Errorf("printf space sign: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != " 42" {
		t.Errorf("printf space sign: want ' 42', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// formatPrintfFloat (scalar.go:840) — 94.4%
// Uncovered: spaceSign path for floats.
// ---------------------------------------------------------------------------

func TestMCDC5_FormatPrintfFloat_SpaceSign(t *testing.T) {
	v, err := printfFunc([]Value{NewTextValue("% f"), NewFloatValue(3.14)})
	if err != nil {
		t.Errorf("printf float space sign: unexpected error: %v", err)
	}
	got := v.AsString()
	if len(got) == 0 || got[0] != ' ' {
		t.Errorf("printf float space sign: want leading space, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// formatPrintfHex (scalar.go:876) — 92.3%
// Uncovered: altForm with uppercase 'X' → "0X" prefix.
// ---------------------------------------------------------------------------

func TestFormatPrintfHex_AltFormUpper(t *testing.T) {
	v, err := printfFunc([]Value{NewTextValue("%#X"), NewIntValue(255)})
	if err != nil {
		t.Errorf("printf #X: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "0XFF" {
		t.Errorf("printf #X 255: want '0XFF', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// formatPrintfOctal (scalar.go:903) — 87.5%
// Uncovered: altForm path (val != 0 → prefix "0").
// Also: null arg path.
// ---------------------------------------------------------------------------

func TestMCDC5_FormatPrintfOctal_AltForm(t *testing.T) {
	v, err := printfFunc([]Value{NewTextValue("%#o"), NewIntValue(8)})
	if err != nil {
		t.Errorf("printf #o: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "010" {
		t.Errorf("printf #o 8: want '010', got %q", got)
	}
}

func TestFormatPrintfOctal_NullArg(t *testing.T) {
	v, err := printfFunc([]Value{NewTextValue("%o"), NewNullValue()})
	if err != nil {
		t.Errorf("printf octal null: unexpected error: %v", err)
	}
	got := v.AsString()
	if got != "0" {
		t.Errorf("printf octal null: want '0', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// printfFunc (scalar.go:1051) — 94.7%
// Uncovered: widthStar or precStar (dynamic width/precision with *).
// ---------------------------------------------------------------------------

func TestPrintfFunc_NullFormat(t *testing.T) {
	v, err := printfFunc([]Value{NewNullValue()})
	if err != nil {
		t.Errorf("printf null format: unexpected error: %v", err)
	}
	if !v.IsNull() {
		t.Errorf("printf null format: expected null, got %v", v)
	}
}
