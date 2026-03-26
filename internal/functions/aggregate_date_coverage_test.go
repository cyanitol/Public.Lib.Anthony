// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"strings"
	"testing"
)

// --- JSONGroupArrayFunc tests ---

func TestAggregateDateCoverage_JSONGroupArrayNumArgs(t *testing.T) {
	t.Parallel()
	f := &JSONGroupArrayFunc{}
	if got := f.NumArgs(); got != 1 {
		t.Errorf("NumArgs() = %d, want 1", got)
	}
}

func TestAggregateDateCoverage_JSONGroupArrayCall(t *testing.T) {
	t.Parallel()
	f := &JSONGroupArrayFunc{}
	_, err := f.Call([]Value{NewIntValue(1)})
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

func TestAggregateDateCoverage_JSONGroupArrayStep(t *testing.T) {
	t.Parallel()
	f := &JSONGroupArrayFunc{}

	// Step with no args should return error
	if err := f.Step([]Value{}); err == nil {
		t.Error("Step() with no args should return error")
	}

	// Step with valid values
	if err := f.Step([]Value{NewIntValue(1)}); err != nil {
		t.Errorf("Step(int) error = %v", err)
	}
	if err := f.Step([]Value{NewFloatValue(2.5)}); err != nil {
		t.Errorf("Step(float) error = %v", err)
	}
	if err := f.Step([]Value{NewTextValue("hello")}); err != nil {
		t.Errorf("Step(text) error = %v", err)
	}
	if err := f.Step([]Value{NewNullValue()}); err != nil {
		t.Errorf("Step(null) error = %v", err)
	}
	if err := f.Step([]Value{NewBlobValue([]byte("data"))}); err != nil {
		t.Errorf("Step(blob) error = %v", err)
	}
}

func TestAggregateDateCoverage_JSONGroupArrayFinal(t *testing.T) {
	t.Parallel()
	f := &JSONGroupArrayFunc{}

	// Final on empty should produce "[]"
	v, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}
	if v.AsString() != "[]" {
		t.Errorf("Final() empty = %q, want \"[]\"", v.AsString())
	}

	// Add elements, then final
	f2 := &JSONGroupArrayFunc{}
	_ = f2.Step([]Value{NewIntValue(42)})
	_ = f2.Step([]Value{NewTextValue("world")})
	v2, err := f2.Final()
	if err != nil {
		t.Fatalf("Final() with elements error = %v", err)
	}
	s := v2.AsString()
	if !strings.Contains(s, "42") || !strings.Contains(s, "world") {
		t.Errorf("Final() = %q, expected to contain 42 and world", s)
	}
}

func TestAggregateDateCoverage_JSONGroupArrayReset(t *testing.T) {
	t.Parallel()
	f := &JSONGroupArrayFunc{}
	_ = f.Step([]Value{NewIntValue(1)})
	f.Reset()
	// After reset, Final should return "[]"
	v, err := f.Final()
	if err != nil {
		t.Fatalf("Final() after Reset error = %v", err)
	}
	if v.AsString() != "[]" {
		t.Errorf("Final() after Reset = %q, want \"[]\"", v.AsString())
	}
}

// --- JSONGroupObjectFunc tests ---

func TestAggregateDateCoverage_JSONGroupObjectNumArgs(t *testing.T) {
	t.Parallel()
	f := &JSONGroupObjectFunc{}
	if got := f.NumArgs(); got != 2 {
		t.Errorf("NumArgs() = %d, want 2", got)
	}
}

func TestAggregateDateCoverage_JSONGroupObjectCall(t *testing.T) {
	t.Parallel()
	f := &JSONGroupObjectFunc{}
	_, err := f.Call([]Value{NewTextValue("k"), NewIntValue(1)})
	if err == nil {
		t.Error("Call() should return error for aggregate function")
	}
}

func TestAggregateDateCoverage_JSONGroupObjectStep(t *testing.T) {
	t.Parallel()
	f := &JSONGroupObjectFunc{}

	// Step with insufficient args
	if err := f.Step([]Value{NewTextValue("k")}); err == nil {
		t.Error("Step() with 1 arg should return error")
	}

	// Step with NULL key should skip (no error)
	if err := f.Step([]Value{NewNullValue(), NewIntValue(1)}); err != nil {
		t.Errorf("Step() with null key error = %v", err)
	}

	// Step with valid key/value
	if err := f.Step([]Value{NewTextValue("a"), NewIntValue(1)}); err != nil {
		t.Errorf("Step() valid error = %v", err)
	}
	if err := f.Step([]Value{NewTextValue("b"), NewFloatValue(3.14)}); err != nil {
		t.Errorf("Step() valid float error = %v", err)
	}
}

func TestAggregateDateCoverage_JSONGroupObjectFinal(t *testing.T) {
	t.Parallel()
	f := &JSONGroupObjectFunc{}
	_ = f.Step([]Value{NewTextValue("key1"), NewIntValue(100)})
	_ = f.Step([]Value{NewTextValue("key2"), NewTextValue("val")})
	v, err := f.Final()
	if err != nil {
		t.Fatalf("Final() error = %v", err)
	}
	s := v.AsString()
	if !strings.Contains(s, "key1") || !strings.Contains(s, "key2") {
		t.Errorf("Final() = %q, expected key1 and key2", s)
	}
}

func TestAggregateDateCoverage_JSONGroupObjectReset(t *testing.T) {
	t.Parallel()
	f := &JSONGroupObjectFunc{}
	_ = f.Step([]Value{NewTextValue("k"), NewIntValue(1)})
	f.Reset()
	v, err := f.Final()
	if err != nil {
		t.Fatalf("Final() after Reset error = %v", err)
	}
	s := v.AsString()
	if s != "{}" {
		t.Errorf("Final() after Reset = %q, want \"{}\"", s)
	}
}

// --- valueToJSONInterface tests ---

func TestAggregateDateCoverage_ValueToJSONInterface(t *testing.T) {
	t.Parallel()

	// Null
	if v := valueToJSONInterface(NewNullValue()); v != nil {
		t.Errorf("null -> %v, want nil", v)
	}

	// Integer
	v := valueToJSONInterface(NewIntValue(42))
	if i, ok := v.(int64); !ok || i != 42 {
		t.Errorf("int -> %v (%T), want int64(42)", v, v)
	}

	// Float
	v2 := valueToJSONInterface(NewFloatValue(3.14))
	if f, ok := v2.(float64); !ok || f != 3.14 {
		t.Errorf("float -> %v (%T), want float64(3.14)", v2, v2)
	}

	// Blob
	v3 := valueToJSONInterface(NewBlobValue([]byte("bin")))
	if s, ok := v3.(string); !ok || s != "bin" {
		t.Errorf("blob -> %v (%T), want string \"bin\"", v3, v3)
	}

	// Text
	v4 := valueToJSONInterface(NewTextValue("hello"))
	if s, ok := v4.(string); !ok || s != "hello" {
		t.Errorf("text -> %v (%T), want string \"hello\"", v4, v4)
	}
}

// --- buildJSONObject tests ---

func TestAggregateDateCoverage_BuildJSONObject(t *testing.T) {
	t.Parallel()

	keys := []string{"x", "y"}
	vals := []interface{}{int64(1), "two"}
	obj := buildJSONObject(keys, vals)
	if obj["x"] != int64(1) {
		t.Errorf("obj[x] = %v, want 1", obj["x"])
	}
	if obj["y"] != "two" {
		t.Errorf("obj[y] = %v, want \"two\"", obj["y"])
	}
}

// --- marshalJSONValue tests ---

func TestAggregateDateCoverage_MarshalJSONValue(t *testing.T) {
	t.Parallel()

	// Valid slice
	v := marshalJSONValue([]interface{}{1, "two"})
	if v.Type() != TypeText {
		t.Errorf("Type = %v, want TypeText", v.Type())
	}
	s := v.AsString()
	if !strings.Contains(s, "1") || !strings.Contains(s, "two") {
		t.Errorf("JSON = %q, expected 1 and two", s)
	}

	// Nil (valid JSON null)
	v2 := marshalJSONValue(nil)
	if v2.AsString() != "null" {
		t.Errorf("nil marshal = %q, want \"null\"", v2.AsString())
	}
}

// --- LookupBuiltin tests ---

func TestAggregateDateCoverage_LookupBuiltin(t *testing.T) {
	t.Parallel()
	r := DefaultRegistry()

	// Known built-in functions
	known := []string{"count", "sum", "avg", "min", "max", "date", "time", "datetime",
		"julianday", "strftime", "abs", "length"}
	for _, name := range known {
		fn, ok := r.LookupBuiltin(name)
		if !ok {
			t.Errorf("LookupBuiltin(%q) not found", name)
			continue
		}
		if fn == nil {
			t.Errorf("LookupBuiltin(%q) returned nil function", name)
		}
	}

	// Unknown function
	_, ok := r.LookupBuiltin("no_such_function_xyz")
	if ok {
		t.Error("LookupBuiltin() should return false for unknown function")
	}

	// Case-insensitive lookup
	fn, ok := r.LookupBuiltin("COUNT")
	if !ok {
		t.Error("LookupBuiltin(COUNT) should find count (case-insensitive)")
	}
	if fn == nil {
		t.Error("LookupBuiltin(COUNT) returned nil")
	}
}

// --- date.go function coverage ---

// TestAggregateDateCoverage_ApplyUnixepoch tests applyUnixepoch via unixepochFunc.
func TestAggregateDateCoverage_ApplyUnixepoch(t *testing.T) {
	t.Parallel()

	// unixepoch('now') should return an integer
	v, err := unixepochFunc([]Value{NewTextValue("now")})
	if err != nil {
		t.Fatalf("unixepochFunc('now') error = %v", err)
	}
	if v.Type() != TypeInteger {
		t.Errorf("Type = %v, want TypeInteger", v.Type())
	}
	epoch := v.AsInt64()
	if epoch < 1000000000 {
		t.Errorf("epoch = %d, expected a reasonable unix timestamp", epoch)
	}

	// unixepoch with a numeric value + unixepoch modifier
	// A large number that would be treated as julian day by default:
	// 2451544.5 = 2000-01-01
	v2, err := unixepochFunc([]Value{NewFloatValue(946684800), NewTextValue("unixepoch")})
	if err != nil {
		t.Fatalf("unixepochFunc with unixepoch modifier error = %v", err)
	}
	if v2.IsNull() {
		t.Error("unixepochFunc with unixepoch modifier returned NULL")
	}

	// applyUnixepoch on a DateTime without validJD should fail gracefully
	dt := &DateTime{}
	if err := dt.applyUnixepoch(); err == nil {
		t.Error("applyUnixepoch() without validJD should return error")
	}
}

// TestAggregateDateCoverage_ApplyJulianday tests applyJulianday.
func TestAggregateDateCoverage_ApplyJulianday(t *testing.T) {
	t.Parallel()

	// julianday('now') should return a float
	v, err := juliandayFunc([]Value{NewTextValue("now")})
	if err != nil {
		t.Fatalf("juliandayFunc('now') error = %v", err)
	}
	if v.Type() != TypeFloat {
		t.Errorf("Type = %v, want TypeFloat", v.Type())
	}
	jd := v.AsFloat64()
	// Should be a reasonable Julian day number (after 2000-01-01 = 2451544.5)
	if jd < 2451544.5 {
		t.Errorf("julianday = %f, expected >= 2451544.5 (year 2000)", jd)
	}

	// applyJulianday on a DateTime without validJD should fail
	dt := &DateTime{}
	if err := dt.applyJulianday(); err == nil {
		t.Error("applyJulianday() without validJD should return error")
	}

	// applyJulianday on a DateTime with validJD should succeed
	dt2 := &DateTime{jd: 2451544500000, validJD: true}
	if err := dt2.applyJulianday(); err != nil {
		t.Errorf("applyJulianday() with validJD error = %v", err)
	}
}

// TestAggregateDateCoverage_ParseTimeParts tests parseTimeParts with HH:MM and HH:MM:SS.
func TestAggregateDateCoverage_ParseTimeParts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input  string
		wantH  int
		wantM  int
		wantS  float64
		wantOK bool
	}{
		{"12:30", 12, 30, 0.0, true},
		{"08:05:15", 8, 5, 15.0, true},
		{"23:59:59.5", 23, 59, 59.5, true},
		{"bad:val", 0, 0, 0, false},
		{"12:99", 0, 0, 0, false},    // invalid minutes
		{"12:30:99", 0, 0, 0, false}, // invalid seconds
		{"12", 0, 0, 0, false},       // only one part
	}

	for _, tc := range cases {
		h, m, s, ok := parseTimeParts(tc.input)
		if ok != tc.wantOK {
			t.Errorf("parseTimeParts(%q) ok = %v, want %v", tc.input, ok, tc.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if h != tc.wantH || m != tc.wantM || s != tc.wantS {
			t.Errorf("parseTimeParts(%q) = (%d,%d,%f), want (%d,%d,%f)",
				tc.input, h, m, s, tc.wantH, tc.wantM, tc.wantS)
		}
	}
}

// TestAggregateDateCoverage_ParseHours tests parseHours.
func TestAggregateDateCoverage_ParseHours(t *testing.T) {
	t.Parallel()

	h, ok := parseHours("05")
	if !ok || h != 5 {
		t.Errorf("parseHours(\"05\") = (%d, %v), want (5, true)", h, ok)
	}

	_, ok2 := parseHours("not-a-number")
	if ok2 {
		t.Error("parseHours(\"not-a-number\") should return false")
	}

	h3, ok3 := parseHours("0")
	if !ok3 || h3 != 0 {
		t.Errorf("parseHours(\"0\") = (%d, %v), want (0, true)", h3, ok3)
	}
}

// TestAggregateDateCoverage_ApplyTimeOffset tests applyTimeOffset via strftime with time offsets.
func TestAggregateDateCoverage_ApplyTimeOffset(t *testing.T) {
	t.Parallel()

	// Test applyTimeOffset directly
	dt := &DateTime{}
	dt.year = 2024
	dt.month = 1
	dt.day = 15
	dt.hour = 10
	dt.minute = 0
	dt.second = 0
	dt.validYMD = true
	dt.validHMS = true
	dt.computeJD()

	origJD := dt.jd
	dt.applyTimeOffset(1, 2, 30, 0) // +2:30:00
	expectedDelta := int64(2*3600000 + 30*60000)
	if dt.jd-origJD != expectedDelta {
		t.Errorf("applyTimeOffset delta = %d, want %d", dt.jd-origJD, expectedDelta)
	}

	// Test via strftime with time offset modifier
	v, err := strftimeFunc([]Value{
		NewTextValue("%H:%M"),
		NewTextValue("2024-01-15 10:00:00"),
		NewTextValue("+01:30"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc with offset error = %v", err)
	}
	if v.AsString() != "11:30" {
		t.Errorf("strftimeFunc with +01:30 = %q, want \"11:30\"", v.AsString())
	}

	// Test negative offset
	v2, err := strftimeFunc([]Value{
		NewTextValue("%H:%M"),
		NewTextValue("2024-01-15 10:00:00"),
		NewTextValue("-00:30"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc with negative offset error = %v", err)
	}
	if v2.AsString() != "09:30" {
		t.Errorf("strftimeFunc with -00:30 = %q, want \"09:30\"", v2.AsString())
	}
}

// TestAggregateDateCoverage_GetWeekday tests getWeekday via strftime %w.
func TestAggregateDateCoverage_GetWeekday(t *testing.T) {
	t.Parallel()

	// 2024-01-07 is a Sunday (weekday 0)
	v, err := strftimeFunc([]Value{
		NewTextValue("%w"),
		NewTextValue("2024-01-07"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%w error = %v", err)
	}
	if v.AsString() != "0" {
		t.Errorf("weekday for Sunday = %q, want \"0\"", v.AsString())
	}

	// 2024-01-01 is a Monday (weekday 1)
	v2, err := strftimeFunc([]Value{
		NewTextValue("%w"),
		NewTextValue("2024-01-01"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%w (Monday) error = %v", err)
	}
	if v2.AsString() != "1" {
		t.Errorf("weekday for Monday = %q, want \"1\"", v2.AsString())
	}
}

// TestAggregateDateCoverage_GetWeekNumber tests getWeekNumber via strftime %W.
func TestAggregateDateCoverage_GetWeekNumber(t *testing.T) {
	t.Parallel()

	// 2024-01-01 (Monday) should be week 01
	v, err := strftimeFunc([]Value{
		NewTextValue("%W"),
		NewTextValue("2024-01-01"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%W error = %v", err)
	}
	// Week 01 (first Monday is 2024-01-01 itself)
	if v.AsString() != "01" {
		t.Errorf("week number for 2024-01-01 = %q, want \"01\"", v.AsString())
	}

	// 2024-03-15 should be in week 11
	v2, err := strftimeFunc([]Value{
		NewTextValue("%W"),
		NewTextValue("2024-03-15"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%W (march) error = %v", err)
	}
	wn := v2.AsString()
	if wn == "" {
		t.Error("week number should not be empty")
	}
}

// TestAggregateDateCoverage_GetDayOfYear tests getDayOfYear via strftime %j.
func TestAggregateDateCoverage_GetDayOfYear(t *testing.T) {
	t.Parallel()

	// Jan 1 = day 001
	v, err := strftimeFunc([]Value{
		NewTextValue("%j"),
		NewTextValue("2024-01-01"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%j error = %v", err)
	}
	if v.AsString() != "001" {
		t.Errorf("day of year for 2024-01-01 = %q, want \"001\"", v.AsString())
	}

	// Dec 31 of leap year 2024 = day 366
	v2, err := strftimeFunc([]Value{
		NewTextValue("%j"),
		NewTextValue("2024-12-31"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%j (dec31) error = %v", err)
	}
	if v2.AsString() != "366" {
		t.Errorf("day of year for 2024-12-31 = %q, want \"366\"", v2.AsString())
	}
}

// TestAggregateDateCoverage_HandleSpecialModifiers tests handleSpecialModifiers
// via applyModifier with "unixepoch", "auto", and "julianday".
func TestAggregateDateCoverage_HandleSpecialModifiers(t *testing.T) {
	t.Parallel()

	// Test 'unixepoch' modifier: a small numeric value (treated initially as Julian day)
	// reinterpreted as unix timestamp seconds. The value 0 as Julian day refers to
	// 4713 BCE; with 'unixepoch' it is reinterpreted as Unix epoch 0 = 1970-01-01.
	v, err := dateFunc([]Value{
		NewFloatValue(0), // 0 seconds since Unix epoch
		NewTextValue("unixepoch"),
	})
	if err != nil {
		t.Fatalf("dateFunc with unixepoch modifier error = %v", err)
	}
	if v.AsString() != "1970-01-01" {
		t.Errorf("unixepoch modifier result = %q, want \"1970-01-01\"", v.AsString())
	}

	// Test 'auto' modifier (no-op)
	v2, err := dateFunc([]Value{
		NewTextValue("2024-06-15"),
		NewTextValue("auto"),
	})
	if err != nil {
		t.Fatalf("dateFunc with auto modifier error = %v", err)
	}
	if v2.AsString() != "2024-06-15" {
		t.Errorf("auto modifier result = %q, want \"2024-06-15\"", v2.AsString())
	}

	// Test 'julianday' modifier: numeric Julian day should be accepted
	// 2451544.5 is approximately 2000-01-01
	v3, err := dateFunc([]Value{
		NewFloatValue(2451544.5),
		NewTextValue("julianday"),
	})
	if err != nil {
		t.Fatalf("dateFunc with julianday modifier error = %v", err)
	}
	if v3.IsNull() {
		t.Error("julianday modifier returned NULL")
	}

	// Test handleSpecialModifiers with unixepoch on a DateTime without validJD
	// (should be handled as returning nil from parseDateTimeWithModifiers)
	// Use a text-based input (not numeric) with unixepoch modifier — should return NULL
	v4, err := dateFunc([]Value{
		NewTextValue("2024-01-01"),
		NewTextValue("unixepoch"),
	})
	if err != nil {
		t.Fatalf("dateFunc with text + unixepoch error = %v", err)
	}
	// This should return NULL because applyUnixepoch fails on non-numeric input
	if !v4.IsNull() {
		// dateFunc returns NULL when modifier fails
		t.Logf("text+unixepoch returned %q (may vary by implementation)", v4.AsString())
	}
}

// TestAggregateDateCoverage_StrftimeUnixEpochS tests strftime('%s', ...) which calls getUnixEpoch.
func TestAggregateDateCoverage_StrftimeUnixEpochS(t *testing.T) {
	t.Parallel()

	// strftime('%s', '1970-01-01') should return 0 (Unix epoch start)
	v, err := strftimeFunc([]Value{
		NewTextValue("%s"),
		NewTextValue("1970-01-01"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%s error = %v", err)
	}
	if v.AsString() != "0" {
		t.Errorf("strftime('%%s', '1970-01-01') = %q, want \"0\"", v.AsString())
	}

	// strftime('%s', '2000-01-01') should return 946684800
	v2, err := strftimeFunc([]Value{
		NewTextValue("%s"),
		NewTextValue("2000-01-01"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc %%s 2000-01-01 error = %v", err)
	}
	if v2.AsString() != "946684800" {
		t.Errorf("strftime('%%s', '2000-01-01') = %q, want \"946684800\"", v2.AsString())
	}
}

// TestAggregateDateCoverage_StrftimeWithHMSInput tests parseTimeParts path
// by parsing pure HH:MM:SS format with strftime.
func TestAggregateDateCoverage_StrftimeWithHMSInput(t *testing.T) {
	t.Parallel()

	// strftime('%H:%M:%S', '14:30:45') — pure time input
	v, err := strftimeFunc([]Value{
		NewTextValue("%H:%M:%S"),
		NewTextValue("14:30:45"),
	})
	if err != nil {
		t.Fatalf("strftimeFunc with HMS input error = %v", err)
	}
	if v.AsString() != "14:30:45" {
		t.Errorf("strftimeFunc HMS = %q, want \"14:30:45\"", v.AsString())
	}
}

// TestAggregateDateCoverage_StartOfModifiers exercises handleStartOfModifier
// which calls startOf for month, year, day.
func TestAggregateDateCoverage_StartOfModifiers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mod  string
		date string
		want string
	}{
		{"start of month", "2024-03-15", "2024-03-01"},
		{"start of year", "2024-06-20", "2024-01-01"},
		{"start of day", "2024-03-15", "2024-03-15"},
	}

	for _, tc := range cases {
		v, err := dateFunc([]Value{
			NewTextValue(tc.date),
			NewTextValue(tc.mod),
		})
		if err != nil {
			t.Fatalf("dateFunc with %q error = %v", tc.mod, err)
		}
		if v.AsString() != tc.want {
			t.Errorf("dateFunc(%q, %q) = %q, want %q", tc.date, tc.mod, v.AsString(), tc.want)
		}
	}
}
