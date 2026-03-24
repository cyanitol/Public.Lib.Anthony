// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// --- generate_series ---

func TestGenerateSeriesOpen_OneArg(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	rows, err := f.Open([]Value{NewIntValue(3)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// generate_series(3) => 0,1,2,3
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if rows[0][0].AsInt64() != 0 {
		t.Errorf("first value = %d, want 0", rows[0][0].AsInt64())
	}
	if rows[3][0].AsInt64() != 3 {
		t.Errorf("last value = %d, want 3", rows[3][0].AsInt64())
	}
}

func TestGenerateSeriesOpen_TwoArgs(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	rows, err := f.Open([]Value{NewIntValue(2), NewIntValue(5)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// generate_series(2, 5) => 2,3,4,5
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if rows[0][0].AsInt64() != 2 {
		t.Errorf("first value = %d, want 2", rows[0][0].AsInt64())
	}
}

func TestGenerateSeriesOpen_ThreeArgsStep(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	rows, err := f.Open([]Value{NewIntValue(0), NewIntValue(10), NewIntValue(3)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// generate_series(0, 10, 3) => 0,3,6,9
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestGenerateSeriesOpen_NegativeStep(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	rows, err := f.Open([]Value{NewIntValue(5), NewIntValue(1), NewIntValue(-1)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// generate_series(5, 1, -1) => 5,4,3,2,1
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestGenerateSeriesOpen_ZeroStep(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	_, err := f.Open([]Value{NewIntValue(0), NewIntValue(10), NewIntValue(0)})
	if err == nil {
		t.Error("expected error for step = 0")
	}
}

func TestGenerateSeriesOpen_WrongArgCount(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	_, err := f.Open([]Value{})
	if err == nil {
		t.Error("expected error for no args")
	}
	_, err = f.Open([]Value{NewIntValue(1), NewIntValue(2), NewIntValue(3), NewIntValue(4)})
	if err == nil {
		t.Error("expected error for 4 args")
	}
}

func TestGenerateSeriesCall(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("expected error calling generate_series as scalar")
	}
}

func TestGenerateSeriesMetadata(t *testing.T) {
	t.Parallel()
	f := &generateSeriesFunc{}
	if f.Name() != "generate_series" {
		t.Errorf("Name() = %q, want 'generate_series'", f.Name())
	}
	if f.NumArgs() != -1 {
		t.Errorf("NumArgs() = %d, want -1", f.NumArgs())
	}
	cols := f.Columns()
	if len(cols) != 4 {
		t.Errorf("len(Columns()) = %d, want 4", len(cols))
	}
}

// --- json_each ---

func TestJSONEachOpen_Array(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{NewTextValue(`[10, 20, 30]`)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// key column (index 0) should be integer indices
	if rows[0][0].AsInt64() != 0 {
		t.Errorf("first key = %v, want 0", rows[0][0])
	}
}

func TestJSONEachOpen_Object(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{NewTextValue(`{"a":1,"b":2}`)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestJSONEachOpen_Scalar(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{NewTextValue(`42`)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for scalar, got %d", len(rows))
	}
}

func TestJSONEachOpen_WithPath(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{
		NewTextValue(`{"items":[1,2,3]}`),
		NewTextValue(`$.items`),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestJSONEachOpen_InvalidJSON(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{NewTextValue(`not json`)})
	if err != nil {
		t.Fatalf("Open() should not error for invalid JSON, got: %v", err)
	}
	if rows != nil {
		t.Error("expected nil rows for invalid JSON")
	}
}

func TestJSONEachOpen_NullPath(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{NewTextValue(`[1,2]`), NewNullValue()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestJSONEachOpen_PathNotFound(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	rows, err := f.Open([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue(`$.nonexistent`),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if rows != nil {
		t.Error("expected nil rows when path not found")
	}
}

func TestJSONEachCall(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("expected error calling json_each as scalar")
	}
}

func TestJSONEachMetadata(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	if f.Name() != "json_each" {
		t.Errorf("Name() = %q", f.Name())
	}
	if len(f.Columns()) != 8 {
		t.Errorf("Columns() len = %d, want 8", len(f.Columns()))
	}
}

func TestJSONEachOpen_WrongArgCount(t *testing.T) {
	t.Parallel()
	f := &jsonEachFunc{}
	_, err := f.Open([]Value{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
	_, err = f.Open([]Value{NewTextValue(`{}`), NewTextValue(`$`), NewTextValue(`extra`)})
	if err == nil {
		t.Error("expected error for 3 args")
	}
}

// --- json_tree ---

func TestJSONTreeOpen_Array(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{NewTextValue(`[1,2]`)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// root row + 2 element rows = 3
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestJSONTreeOpen_Object(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{NewTextValue(`{"a":1,"b":[2,3]}`)})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	// root + a + b + b[0] + b[1] = 5
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestJSONTreeOpen_WithPath(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{
		NewTextValue(`{"items":[1,2]}`),
		NewTextValue(`$.items`),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) == 0 {
		t.Error("expected rows when walking with path")
	}
}

func TestJSONTreeOpen_InvalidJSON(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{NewTextValue(`not json`)})
	if err != nil {
		t.Fatalf("Open() should not error, got: %v", err)
	}
	if rows != nil {
		t.Error("expected nil rows for invalid JSON")
	}
}

func TestJSONTreeCall(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	_, err := f.Call(nil)
	if err == nil {
		t.Error("expected error calling json_tree as scalar")
	}
}

func TestJSONTreeMetadata(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	if f.Name() != "json_tree" {
		t.Errorf("Name() = %q", f.Name())
	}
	if len(f.Columns()) != 8 {
		t.Errorf("Columns() len = %d, want 8", len(f.Columns()))
	}
}

func TestJSONTreeOpen_WrongArgCount(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	_, err := f.Open([]Value{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
}

func TestJSONTreeOpen_NullPath(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{NewTextValue(`[1,2]`), NewNullValue()})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if len(rows) == 0 {
		t.Error("expected rows for null path (use root)")
	}
}

func TestJSONTreeOpen_PathNotFound(t *testing.T) {
	t.Parallel()
	f := &jsonTreeFunc{}
	rows, err := f.Open([]Value{
		NewTextValue(`{"a":1}`),
		NewTextValue(`$.missing`),
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if rows != nil {
		t.Error("expected nil rows when path not found")
	}
}

// --- jsonValueToString ---

func TestJSONValueToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input interface{}
		want  string
	}{
		{nil, "null"},
		{"hello", `"hello"`},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
	}
	for _, tt := range tests {
		got := jsonValueToString(tt.input)
		if got != tt.want {
			t.Errorf("jsonValueToString(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- pattern: like and glob ---

func TestLikeFunc_Basic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value, pattern string
		want           int64
	}{
		{"hello", "hello", 1},
		{"hello", "hel%", 1},
		{"hello", "he_lo", 1},
		{"hello", "world", 0},
		{"HELLO", "hello", 1}, // LIKE is case-insensitive for ASCII
	}
	for _, tt := range tests {
		result, err := likeFunc([]Value{NewTextValue(tt.value), NewTextValue(tt.pattern)})
		if err != nil {
			t.Errorf("likeFunc(%q, %q) error = %v", tt.value, tt.pattern, err)
			continue
		}
		if result.AsInt64() != tt.want {
			t.Errorf("likeFunc(%q, %q) = %d, want %d", tt.value, tt.pattern, result.AsInt64(), tt.want)
		}
	}
}

func TestLikeFunc_NullArgs(t *testing.T) {
	t.Parallel()
	result, err := likeFunc([]Value{NewNullValue(), NewTextValue("abc")})
	if err != nil {
		t.Fatalf("likeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when first arg is NULL")
	}

	result, err = likeFunc([]Value{NewTextValue("abc"), NewNullValue()})
	if err != nil {
		t.Fatalf("likeFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when second arg is NULL")
	}
}

func TestLikeFunc_WithEscape(t *testing.T) {
	t.Parallel()
	// Escape '%' with '!'
	result, err := likeFunc([]Value{
		NewTextValue("50%"),
		NewTextValue("50!%"),
		NewTextValue("!"),
	})
	if err != nil {
		t.Fatalf("likeFunc() error = %v", err)
	}
	if result.AsInt64() != 1 {
		t.Error("expected match with escape char")
	}
}

func TestLikeFunc_EscapeNullChar(t *testing.T) {
	t.Parallel()
	// Escape char is empty string — no escaping
	result, err := likeFunc([]Value{
		NewTextValue("hello%"),
		NewTextValue("hello%"),
		NewTextValue(""),
	})
	if err != nil {
		t.Fatalf("likeFunc() error = %v", err)
	}
	// "hello%" with pattern "hello%" and no escape char matches
	if result.AsInt64() != 1 {
		t.Errorf("likeFunc with empty escape: got %d", result.AsInt64())
	}
}

func TestLikeFunc_WrongArgCount(t *testing.T) {
	t.Parallel()
	_, err := likeFunc([]Value{NewTextValue("a")})
	if err == nil {
		t.Error("expected error for 1 arg")
	}
	_, err = likeFunc([]Value{
		NewTextValue("a"), NewTextValue("b"),
		NewTextValue("c"), NewTextValue("d"),
	})
	if err == nil {
		t.Error("expected error for 4 args")
	}
}

func TestLikeFunc_NullEscape(t *testing.T) {
	t.Parallel()
	// NULL escape char is allowed — treated as no escape
	result, err := likeFunc([]Value{
		NewTextValue("hello"),
		NewTextValue("hello"),
		NewNullValue(),
	})
	if err != nil {
		t.Fatalf("likeFunc() error = %v", err)
	}
	if result.AsInt64() != 1 {
		t.Error("expected match")
	}
}

func TestGlobFunc_Basic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value, pattern string
		want           int64
	}{
		{"hello.txt", "*.txt", 1},
		{"hello.go", "*.txt", 0},
		{"HELLO", "HE*", 1},
		{"hello", "he?lo", 1},
	}
	for _, tt := range tests {
		result, err := globFunc([]Value{NewTextValue(tt.value), NewTextValue(tt.pattern)})
		if err != nil {
			t.Errorf("globFunc(%q, %q) error = %v", tt.value, tt.pattern, err)
			continue
		}
		if result.AsInt64() != tt.want {
			t.Errorf("globFunc(%q, %q) = %d, want %d", tt.value, tt.pattern, result.AsInt64(), tt.want)
		}
	}
}

func TestGlobFunc_NullArgs(t *testing.T) {
	t.Parallel()
	result, err := globFunc([]Value{NewNullValue(), NewTextValue("*")})
	if err != nil {
		t.Fatalf("globFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when first arg is NULL")
	}

	result, err = globFunc([]Value{NewTextValue("abc"), NewNullValue()})
	if err != nil {
		t.Fatalf("globFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when second arg is NULL")
	}
}

// --- minScalarFunc / maxScalarFunc ---

func TestMinScalarFunc_Basic(t *testing.T) {
	t.Parallel()
	result, err := minScalarFunc([]Value{NewIntValue(3), NewIntValue(1), NewIntValue(2)})
	if err != nil {
		t.Fatalf("minScalarFunc() error = %v", err)
	}
	if result.AsInt64() != 1 {
		t.Errorf("minScalarFunc() = %d, want 1", result.AsInt64())
	}
}

func TestMinScalarFunc_AllNull(t *testing.T) {
	t.Parallel()
	result, err := minScalarFunc([]Value{NewNullValue(), NewNullValue()})
	if err != nil {
		t.Fatalf("minScalarFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when all args are NULL")
	}
}

func TestMinScalarFunc_WithNulls(t *testing.T) {
	t.Parallel()
	result, err := minScalarFunc([]Value{NewNullValue(), NewIntValue(5), NewIntValue(2)})
	if err != nil {
		t.Fatalf("minScalarFunc() error = %v", err)
	}
	if result.AsInt64() != 2 {
		t.Errorf("minScalarFunc() = %d, want 2", result.AsInt64())
	}
}

func TestMinScalarFunc_NoArgs(t *testing.T) {
	t.Parallel()
	_, err := minScalarFunc([]Value{})
	if err == nil {
		t.Error("expected error for empty args")
	}
}

func TestMaxScalarFunc_Basic(t *testing.T) {
	t.Parallel()
	result, err := maxScalarFunc([]Value{NewIntValue(3), NewIntValue(1), NewIntValue(7)})
	if err != nil {
		t.Fatalf("maxScalarFunc() error = %v", err)
	}
	if result.AsInt64() != 7 {
		t.Errorf("maxScalarFunc() = %d, want 7", result.AsInt64())
	}
}

func TestMaxScalarFunc_AllNull(t *testing.T) {
	t.Parallel()
	result, err := maxScalarFunc([]Value{NewNullValue()})
	if err != nil {
		t.Fatalf("maxScalarFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL")
	}
}

func TestMaxScalarFunc_NoArgs(t *testing.T) {
	t.Parallel()
	_, err := maxScalarFunc([]Value{})
	if err == nil {
		t.Error("expected error for empty args")
	}
}

func TestMaxScalarFunc_TextValues(t *testing.T) {
	t.Parallel()
	result, err := maxScalarFunc([]Value{NewTextValue("apple"), NewTextValue("banana"), NewTextValue("cherry")})
	if err != nil {
		t.Fatalf("maxScalarFunc() error = %v", err)
	}
	if result.AsString() != "cherry" {
		t.Errorf("maxScalarFunc() = %q, want 'cherry'", result.AsString())
	}
}

// --- logVariadicFunc ---

func TestLogVariadicFunc_OneArg(t *testing.T) {
	t.Parallel()
	result, err := logVariadicFunc([]Value{NewFloatValue(math.E)})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if math.Abs(result.AsFloat64()-1.0) > 1e-9 {
		t.Errorf("logVariadicFunc(e) = %f, want ~1.0", result.AsFloat64())
	}
}

func TestLogVariadicFunc_TwoArgs(t *testing.T) {
	t.Parallel()
	// log(10, 100) = 2
	result, err := logVariadicFunc([]Value{NewFloatValue(10), NewFloatValue(100)})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if math.Abs(result.AsFloat64()-2.0) > 1e-9 {
		t.Errorf("logVariadicFunc(10,100) = %f, want ~2.0", result.AsFloat64())
	}
}

func TestLogVariadicFunc_OneArgNull(t *testing.T) {
	t.Parallel()
	result, err := logVariadicFunc([]Value{NewNullValue()})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL for null arg")
	}
}

func TestLogVariadicFunc_OneArgNonPositive(t *testing.T) {
	t.Parallel()
	result, err := logVariadicFunc([]Value{NewFloatValue(0)})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("expected NaN for log(0)")
	}
}

func TestLogVariadicFunc_TwoArgsNull(t *testing.T) {
	t.Parallel()
	result, err := logVariadicFunc([]Value{NewNullValue(), NewFloatValue(100)})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("expected NULL when base is NULL")
	}
}

func TestLogVariadicFunc_TwoArgsBaseOne(t *testing.T) {
	t.Parallel()
	// log(1, x) is undefined (base = 1 causes NaN)
	result, err := logVariadicFunc([]Value{NewFloatValue(1), NewFloatValue(10)})
	if err != nil {
		t.Fatalf("logVariadicFunc() error = %v", err)
	}
	if !math.IsNaN(result.AsFloat64()) {
		t.Error("expected NaN when base=1")
	}
}

func TestLogVariadicFunc_WrongArgCount(t *testing.T) {
	t.Parallel()
	_, err := logVariadicFunc([]Value{})
	if err == nil {
		t.Error("expected error for 0 args")
	}
	_, err = logVariadicFunc([]Value{NewIntValue(1), NewIntValue(2), NewIntValue(3)})
	if err == nil {
		t.Error("expected error for 3 args")
	}
}

// --- sortedKeys ---

func TestSortedKeys(t *testing.T) {
	t.Parallel()
	m := map[string]interface{}{"c": 3, "a": 1, "b": 2}
	keys := sortedKeys(m)
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("keys not sorted: %v", keys)
	}
}

// --- getAtomValue ---

func TestGetAtomValue_Scalar(t *testing.T) {
	t.Parallel()
	v := getAtomValue("hello")
	if v.IsNull() {
		t.Error("expected non-null atom for string scalar")
	}
}

func TestGetAtomValue_Array(t *testing.T) {
	t.Parallel()
	v := getAtomValue([]interface{}{1, 2})
	if !v.IsNull() {
		t.Error("expected null atom for array")
	}
}

func TestGetAtomValue_Object(t *testing.T) {
	t.Parallel()
	v := getAtomValue(map[string]interface{}{"x": 1})
	if !v.IsNull() {
		t.Error("expected null atom for object")
	}
}
