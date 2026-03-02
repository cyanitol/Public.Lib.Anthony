// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"strings"
	"testing"
)

// TestDateTimeFunctions_Basic tests basic functionality of date/time functions
func TestDateTimeFunctions_Basic(t *testing.T) {
	tests := []struct {
		name     string
		fn       func([]Value) (Value, error)
		args     []Value
		wantType ValueType
		wantNull bool
	}{
		{
			name:     "date with now",
			fn:       dateFunc,
			args:     []Value{NewTextValue("now")},
			wantType: TypeText,
		},
		{
			name:     "time with now",
			fn:       timeFunc,
			args:     []Value{NewTextValue("now")},
			wantType: TypeText,
		},
		{
			name:     "datetime with now",
			fn:       datetimeFunc,
			args:     []Value{NewTextValue("now")},
			wantType: TypeText,
		},
		{
			name:     "current_date",
			fn:       currentDateFunc,
			args:     []Value{},
			wantType: TypeText,
		},
		{
			name:     "current_time",
			fn:       currentTimeFunc,
			args:     []Value{},
			wantType: TypeText,
		},
		{
			name:     "current_timestamp",
			fn:       currentTimestampFunc,
			args:     []Value{},
			wantType: TypeText,
		},
		{
			name:     "date with null returns null",
			fn:       dateFunc,
			args:     []Value{NewNullValue()},
			wantNull: true,
		},
		{
			name:     "time with invalid",
			fn:       timeFunc,
			args:     []Value{NewTextValue("invalid")},
			wantNull: true,
		},
		{
			name:     "datetime with invalid",
			fn:       datetimeFunc,
			args:     []Value{NewTextValue("invalid")},
			wantNull: true,
		},
		{
			name:     "julianday with invalid",
			fn:       juliandayFunc,
			args:     []Value{NewTextValue("invalid")},
			wantNull: true,
		},
		{
			name:     "unixepoch with invalid",
			fn:       unixepochFunc,
			args:     []Value{NewTextValue("invalid")},
			wantNull: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn(tt.args)
			if err != nil {
				t.Fatalf("%s error = %v", tt.name, err)
			}
			if tt.wantNull {
				if !result.IsNull() {
					t.Errorf("%s = %v, want NULL", tt.name, result)
				}
				return
			}
			if result.IsNull() {
				t.Fatalf("%s returned NULL", tt.name)
			}
			if result.Type() != tt.wantType {
				t.Errorf("%s type = %v, want %v", tt.name, result.Type(), tt.wantType)
			}
		})
	}
}

// TestStrftimeFunc_Basic tests basic strftime functionality
func TestStrftimeFunc_Basic(t *testing.T) {
	// Test that strftime works without panicking
	result, err := strftimeFunc([]Value{NewTextValue("%Y-%m-%d"), NewTextValue("now")})
	if err != nil {
		t.Fatalf("strftimeFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("strftimeFunc() returned NULL")
	}

	// Test with just format (should use current time)
	result, err = strftimeFunc([]Value{NewTextValue("%Y")})
	if err != nil {
		t.Fatalf("strftimeFunc() with just format error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("strftimeFunc() with just format returned NULL")
	}

	// Test with no args
	result, err = strftimeFunc([]Value{})
	if err != nil {
		t.Fatalf("strftimeFunc() with no args error = %v", err)
	}
	if !result.IsNull() {
		t.Errorf("strftimeFunc() with no args should return NULL, got %v", result)
	}

	// Test with invalid date
	result, err = strftimeFunc([]Value{NewTextValue("%Y"), NewTextValue("invalid")})
	if err != nil {
		t.Fatalf("strftimeFunc() with invalid date error = %v", err)
	}
	if !result.IsNull() {
		t.Errorf("strftimeFunc() with invalid date should return NULL, got %v", result)
	}

	// Test all format specifiers
	specs := []string{"%d", "%m", "%Y", "%H", "%M", "%S", "%J", "%s", "%%", "%X"}
	for _, spec := range specs {
		result, err := strftimeFunc([]Value{NewTextValue(spec), NewTextValue("now")})
		if err != nil {
			t.Errorf("strftimeFunc(%s) error = %v", spec, err)
		}
		if result.IsNull() {
			t.Errorf("strftimeFunc(%s) returned NULL", spec)
		}
	}
}

// TestDateTimeModifiers_Basic tests that modifiers work
func TestDateTimeModifiers_Basic(t *testing.T) {
	modifiers := []string{
		"start of day",
		"start of month",
		"start of year",
		"+1 day",
		"-1 day",
		"+1 month",
		"-1 month",
		"+1 year",
		"-1 year",
		"+1 hour",
		"+1 minute",
		"+1 second",
		"utc",
		"localtime",
		"auto",
		"subsec",
	}

	for _, mod := range modifiers {
		t.Run(mod, func(t *testing.T) {
			result, err := dateFunc([]Value{NewTextValue("now"), NewTextValue(mod)})
			if err != nil {
				t.Fatalf("dateFunc() with %s error = %v", mod, err)
			}
			// Most modifiers should work, though some may return specific formats
			_ = result
		})
	}

	// Test unknown modifier
	result, err := dateFunc([]Value{NewTextValue("now"), NewTextValue("unknown_modifier")})
	if err != nil {
		t.Fatalf("dateFunc() with unknown modifier error = %v", err)
	}
	if !result.IsNull() {
		t.Errorf("dateFunc() with unknown modifier should return NULL, got %v", result)
	}
}

// TestDateTimeHelpers tests helper functions
func TestDateTimeHelpers(t *testing.T) {
	// Test isLeapYear
	if !isLeapYear(2020) {
		t.Error("isLeapYear(2020) should be true")
	}
	if isLeapYear(2021) {
		t.Error("isLeapYear(2021) should be false")
	}
	if !isLeapYear(2000) {
		t.Error("isLeapYear(2000) should be true")
	}
	if isLeapYear(1900) {
		t.Error("isLeapYear(1900) should be false")
	}

	// Test daysInMonth
	if daysInMonth(2020, 2) != 29 {
		t.Errorf("daysInMonth(2020, 2) = %d, want 29", daysInMonth(2020, 2))
	}
	if daysInMonth(2021, 2) != 28 {
		t.Errorf("daysInMonth(2021, 2) = %d, want 28", daysInMonth(2021, 2))
	}
	if daysInMonth(2021, 1) != 31 {
		t.Errorf("daysInMonth(2021, 1) = %d, want 31", daysInMonth(2021, 1))
	}
	if daysInMonth(2021, 4) != 30 {
		t.Errorf("daysInMonth(2021, 4) = %d, want 30", daysInMonth(2021, 4))
	}

	// Test isValidDate
	if !isValidDate(2021, 1, 15) {
		t.Error("isValidDate(2021, 1, 15) should be true")
	}
	if isValidDate(2021, 2, 29) {
		t.Error("isValidDate(2021, 2, 29) should be false")
	}
	if !isValidDate(2020, 2, 29) {
		t.Error("isValidDate(2020, 2, 29) should be true")
	}
	if isValidDate(2021, 13, 1) {
		t.Error("isValidDate(2021, 13, 1) should be false")
	}
	if isValidDate(2021, 1, 32) {
		t.Error("isValidDate(2021, 1, 32) should be false")
	}

	// Test safeFloatToInt
	if safeFloatToInt(42.5) != 42 {
		t.Errorf("safeFloatToInt(42.5) = %d, want 42", safeFloatToInt(42.5))
	}
	if safeFloatToInt(1e20) < 0 {
		t.Error("safeFloatToInt(1e20) should be positive")
	}
	if safeFloatToInt(-1e20) > 0 {
		t.Error("safeFloatToInt(-1e20) should be negative")
	}
}

// TestUnixEpochSubsec tests subsec modifier for unixepoch
func TestUnixEpochSubsec(t *testing.T) {
	// Test without subsec - should return integer
	result, err := unixepochFunc([]Value{NewTextValue("now")})
	if err != nil {
		t.Fatalf("unixepochFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("unixepochFunc() returned NULL")
	}
	if result.Type() != TypeInteger {
		t.Errorf("unixepochFunc() without subsec type = %v, want TypeInteger", result.Type())
	}

	// Test with subsec - should return float
	result, err = unixepochFunc([]Value{NewTextValue("now"), NewTextValue("subsec")})
	if err != nil {
		t.Fatalf("unixepochFunc() with subsec error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("unixepochFunc() with subsec returned NULL")
	}
	if result.Type() != TypeFloat {
		t.Errorf("unixepochFunc() with subsec type = %v, want TypeFloat", result.Type())
	}

	// Test no args
	result, err = unixepochFunc([]Value{})
	if err != nil {
		t.Fatalf("unixepochFunc() with no args error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("unixepochFunc() with no args returned NULL")
	}
}

// TestJuliandayBasic tests julianday basic functionality
func TestJuliandayBasic(t *testing.T) {
	result, err := juliandayFunc([]Value{NewTextValue("now")})
	if err != nil {
		t.Fatalf("juliandayFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("juliandayFunc() returned NULL")
	}
	if result.Type() != TypeFloat {
		t.Errorf("juliandayFunc() type = %v, want TypeFloat", result.Type())
	}

	// Test no args
	result, err = juliandayFunc([]Value{})
	if err != nil {
		t.Fatalf("juliandayFunc() with no args error = %v", err)
	}
	if result.IsNull() {
		t.Fatal("juliandayFunc() with no args returned NULL")
	}
}

// TestDateTimeNullModifiers tests that null modifiers return null
func TestDateTimeNullModifiers(t *testing.T) {
	functions := []struct {
		name string
		fn   func([]Value) (Value, error)
	}{
		{"date", dateFunc},
		{"time", timeFunc},
		{"datetime", datetimeFunc},
		{"julianday", juliandayFunc},
		{"unixepoch", unixepochFunc},
		{"strftime", strftimeFunc},
	}

	for _, tt := range functions {
		t.Run(tt.name, func(t *testing.T) {
			var args []Value
			if tt.name == "strftime" {
				args = []Value{NewTextValue("%Y"), NewTextValue("now"), NewNullValue()}
			} else {
				args = []Value{NewTextValue("now"), NewNullValue()}
			}

			result, err := tt.fn(args)
			if err != nil {
				t.Fatalf("%s() error = %v", tt.name, err)
			}
			if !result.IsNull() {
				t.Errorf("%s() with null modifier should return NULL, got %v", tt.name, result)
			}
		})
	}
}

// TestFormatTimeSubsec tests formatTime with subsec flag
func TestFormatTimeSubsec(t *testing.T) {
	dt := &DateTime{
		hour:      14,
		minute:    30,
		second:    45.123,
		validHMS:  true,
		useSubsec: true,
	}

	result := dt.formatTime()
	if !strings.Contains(result, ".") {
		t.Errorf("formatTime() with subsec should contain decimal point, got %s", result)
	}

	dt.useSubsec = false
	result = dt.formatTime()
	if strings.Contains(result, ".") {
		t.Errorf("formatTime() without subsec should not contain decimal point, got %s", result)
	}
}

// TestDateTimeRawNumber tests setRawNumber for Julian day and Unix timestamp ranges
func TestDateTimeRawNumber(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		isJulian bool
	}{
		{"Julian day", 2451545.0, true},  // Valid Julian day
		{"Unix timestamp", 1609459200.0, false}, // Outside Julian range
		{"Small Julian", 100.5, true},    // Small Julian day
		{"Large timestamp", 9999999999.0, false}, // Large Unix timestamp
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.setRawNumber(tt.value)
			if !dt.validJD {
				t.Errorf("setRawNumber(%f) should set validJD", tt.value)
			}
		})
	}
}

// TestDateTimeParseString tests parseString with various formats
func TestDateTimeParseString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"Valid YMD", "2024-01-15", false},
		{"Valid with time", "2024-01-15 10:30:45", false},
		{"Invalid format", "not-a-date", true},
		{"Empty string", "", true},
		{"Just time", "10:30:45", false}, // This may actually succeed as HMS
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{}
			err := dt.parseString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// TestDateTimeComputeHMS tests computeHMS function
func TestDateTimeComputeHMS(t *testing.T) {
	dt := &DateTime{
		jd:      2451545000000, // Some Julian day value
		validJD: true,
	}

	dt.computeHMS()

	if !dt.validHMS {
		t.Error("computeHMS() should set validHMS")
	}

	// Test that calling again doesn't recompute
	dt.hour = 99
	dt.computeHMS()
	if dt.hour != 99 {
		t.Error("computeHMS() should not recompute when validHMS is true")
	}
}

// TestDateTimeNormalizeMonth tests normalizeMonth with edge cases
func TestDateTimeNormalizeMonth(t *testing.T) {
	tests := []struct {
		name       string
		initMonth  int
		initYear   int
		wantMonth  int
		wantYear   int
	}{
		{"Month 13", 13, 2020, 1, 2021},
		{"Month 0", 0, 2020, 12, 2019},
		{"Month -1", -1, 2020, 11, 2019},
		{"Month 25", 25, 2020, 1, 2022},
		{"Normal month", 6, 2020, 6, 2020},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{
				month: tt.initMonth,
				year:  tt.initYear,
			}
			dt.normalizeMonth()
			if dt.month != tt.wantMonth || dt.year != tt.wantYear {
				t.Errorf("normalizeMonth() = (month=%d, year=%d), want (month=%d, year=%d)",
					dt.month, dt.year, tt.wantMonth, tt.wantYear)
			}
		})
	}
}

// TestDateTimeParseTimeComponent tests parseTimeComponent with various inputs
func TestDateTimeParseTimeComponent(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		parts []string
	}{
		{"Valid parts", "2024-01-15 10:30:45", []string{"10:30:45"}},
		{"Space separator", "2024-01-15 10:30:45", []string{"extra"}},
		{"T separator", "2024-01-15T10:30:45", []string{}},
		{"No separator", "2024-01-15", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.parseTimeComponent(tt.s, tt.parts)
			// Just verify it doesn't panic
		})
	}
}

// TestDateTimeParsing tests parse field functions with invalid values
func TestDateTimeParseFields(t *testing.T) {
	// Test parseYearField with invalid input
	_, ok := parseYearField("invalid")
	if ok {
		t.Error("parseYearField('invalid') should return ok=false")
	}

	// Test parseMonthField with invalid input
	_, ok = parseMonthField("invalid")
	if ok {
		t.Error("parseMonthField('invalid') should return ok=false")
	}

	// Test parseDayField with invalid input
	_, ok = parseDayField("invalid")
	if ok {
		t.Error("parseDayField('invalid') should return ok=false")
	}

	// Test parseHourField with invalid input
	_, ok = parseHourField("invalid")
	if ok {
		t.Error("parseHourField('invalid') should return ok=false")
	}

	// Test parseMinuteField with invalid input
	_, ok = parseMinuteField("invalid")
	if ok {
		t.Error("parseMinuteField('invalid') should return ok=false")
	}

	// Test parseSecondField with invalid input
	_, ok = parseSecondField("invalid")
	if ok {
		t.Error("parseSecondField('invalid') should return ok=false")
	}
}

// TestDateTimeParseYMD tests parseYMD with various edge cases
func TestDateTimeParseYMD(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantOK bool
	}{
		{"Valid date", "2024-01-15", true},
		{"Invalid month", "2024-13-15", false},
		{"Invalid day", "2024-01-32", false},
		{"Too few parts", "2024-01", false},
		{"Invalid year", "abcd-01-15", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{}
			ok := dt.parseYMD(tt.input)
			if ok != tt.wantOK {
				t.Errorf("parseYMD(%q) = %v, want %v", tt.input, ok, tt.wantOK)
			}
		})
	}
}

// TestDateTimeParseHMS tests parseHMS with various edge cases
func TestDateTimeParseHMS(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantOK bool
	}{
		{"Valid time", "10:30:45", true},
		{"Invalid hour", "25:30:45", false},
		{"Invalid minute", "10:61:45", false},
		{"Invalid second", "10:30:61", false},
		{"Two parts", "10:30", true}, // May be valid as HH:MM
		{"Non-numeric", "aa:bb:cc", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{}
			ok := dt.parseHMS(tt.input)
			if ok != tt.wantOK {
				t.Errorf("parseHMS(%q) = %v, want %v", tt.input, ok, tt.wantOK)
			}
		})
	}
}

// TestDateTimeComputeJD tests computeJD edge cases
func TestDateTimeComputeJD(t *testing.T) {
	tests := []struct {
		name  string
		year  int
		month int
		day   int
	}{
		{"Leap year", 2020, 2, 29},
		{"Non-leap year", 2021, 2, 28},
		{"January", 2020, 1, 1},
		{"December", 2020, 12, 31},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := &DateTime{
				year:     tt.year,
				month:    tt.month,
				day:      tt.day,
				validYMD: true,
			}
			dt.computeJD()
			if !dt.validJD {
				t.Error("computeJD() should set validJD")
			}
		})
	}
}

// TestDateTimeComputeYMD tests computeYMD edge cases
func TestDateTimeComputeYMD(t *testing.T) {
	dt := &DateTime{
		jd:      2451545000000, // Julian day for 2000-01-01
		validJD: true,
	}

	dt.computeYMD()

	if !dt.validYMD {
		t.Error("computeYMD() should set validYMD")
	}

	// Verify month normalization path
	if dt.month < 1 || dt.month > 12 {
		t.Errorf("computeYMD() month = %d, should be 1-12", dt.month)
	}
}

// TestDateFuncEdgeCases tests date function with various input types
func TestDateFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{"Integer input", []Value{NewIntValue(1234567890)}, false},
		{"Float input", []Value{NewFloatValue(2451545.5)}, false},
		{"Invalid modifier", []Value{NewTextValue("2024-01-15"), NewTextValue("invalid_mod")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := dateFunc(tt.args)
			if err != nil {
				t.Errorf("dateFunc() error = %v", err)
				return
			}
			if tt.wantNull && !result.IsNull() {
				t.Errorf("dateFunc() should return NULL, got %v", result)
			}
		})
	}
}

// TestTimeFuncEdgeCases tests time function with various inputs
func TestTimeFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{"Integer input", []Value{NewIntValue(1234567890)}, false},
		{"Float input", []Value{NewFloatValue(2451545.5)}, false},
		{"With modifier", []Value{NewTextValue("2024-01-15 10:30:45"), NewTextValue("+1 hour")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := timeFunc(tt.args)
			if err != nil {
				t.Errorf("timeFunc() error = %v", err)
				return
			}
			if tt.wantNull && !result.IsNull() {
				t.Errorf("timeFunc() should return NULL, got %v", result)
			}
		})
	}
}

// TestDatetimeFuncEdgeCases tests datetime function with various inputs
func TestDatetimeFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{"Integer input", []Value{NewIntValue(1234567890)}, false},
		{"Float input", []Value{NewFloatValue(2451545.5)}, false},
		{"With modifier", []Value{NewTextValue("2024-01-15 10:30:45"), NewTextValue("+1 day")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := datetimeFunc(tt.args)
			if err != nil {
				t.Errorf("datetimeFunc() error = %v", err)
				return
			}
			if tt.wantNull && !result.IsNull() {
				t.Errorf("datetimeFunc() should return NULL, got %v", result)
			}
		})
	}
}

// TestJuliandayFuncEdgeCases tests julianday function edge cases
func TestJuliandayFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		args []Value
	}{
		{"Integer input", []Value{NewIntValue(1234567890)}},
		{"Float input", []Value{NewFloatValue(2451545.5)}},
		{"With modifier", []Value{NewTextValue("2024-01-15"), NewTextValue("+1 day")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := juliandayFunc(tt.args)
			if err != nil {
				t.Errorf("juliandayFunc() error = %v", err)
				return
			}
			if result.IsNull() {
				t.Error("juliandayFunc() should not return NULL for valid inputs")
			}
		})
	}
}

// TestApplyUnixEpochModifiers tests unixepoch modifier edge cases
func TestApplyUnixEpochModifiers(t *testing.T) {
	tests := []struct {
		name     string
		args     []Value
		wantNull bool
	}{
		{"With invalid modifier", []Value{NewTextValue("now"), NewTextValue("invalid")}, true},
		{"Multiple modifiers", []Value{NewTextValue("now"), NewTextValue("+1 day"), NewTextValue("start of day")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unixepochFunc(tt.args)
			if err != nil {
				t.Errorf("unixepochFunc() error = %v", err)
				return
			}
			if tt.wantNull && !result.IsNull() {
				t.Errorf("unixepochFunc() should return NULL for invalid modifier, got %v", result)
			}
		})
	}
}

// TestIsValidDateEdgeCases tests isValidDate with boundary conditions
func TestIsValidDateEdgeCases(t *testing.T) {
	tests := []struct {
		year  int
		month int
		day   int
		want  bool
	}{
		{2020, 2, 29, true},  // Leap year
		{2021, 2, 29, false}, // Non-leap year
		{2020, 0, 15, false}, // Invalid month
		{2020, 1, 0, false},  // Invalid day
	}

	for _, tt := range tests {
		result := isValidDate(tt.year, tt.month, tt.day)
		if result != tt.want {
			t.Errorf("isValidDate(%d, %d, %d) = %v, want %v", tt.year, tt.month, tt.day, result, tt.want)
		}
	}
}

// TestStartOfModifiers tests start of modifiers edge cases
func TestStartOfModifiers(t *testing.T) {
	tests := []struct {
		name     string
		modifier string
		wantNull bool
	}{
		{"Start of hour", "start of hour", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := dateFunc([]Value{NewTextValue("now"), NewTextValue(tt.modifier)})
			if err != nil {
				t.Errorf("dateFunc() with %s error = %v", tt.modifier, err)
				return
			}
			if tt.wantNull && !result.IsNull() {
				t.Errorf("dateFunc() with %s should return NULL", tt.modifier)
			}
		})
	}
}

// TestAddModifiers tests add modifiers with various units
func TestAddModifiers(t *testing.T) {
	modifiers := []string{
		"-1 hour",
		"-1 minute",
		"-1 second",
		"-1 month",
		"-1 year",
	}

	for _, mod := range modifiers {
		t.Run(mod, func(t *testing.T) {
			result, err := dateFunc([]Value{NewTextValue("2024-06-15"), NewTextValue(mod)})
			if err != nil {
				t.Fatalf("dateFunc() with %s error = %v", mod, err)
			}
			if result.IsNull() {
				t.Errorf("dateFunc() with %s should not return NULL", mod)
			}
		})
	}
}

// TestDateTimeFuncBlob tests date/time functions with blob input
func TestDateTimeFuncBlob(t *testing.T) {
	// Test with blob input - blob is not supported, should return NULL
	result, err := dateFunc([]Value{NewBlobValue([]byte("2024-01-15"))})
	if err != nil {
		t.Errorf("dateFunc() with blob error = %v", err)
	}
	if !result.IsNull() {
		t.Error("dateFunc() with blob should return NULL (unsupported type)")
	}

	result, err = timeFunc([]Value{NewBlobValue([]byte("10:30:45"))})
	if err != nil {
		t.Errorf("timeFunc() with blob error = %v", err)
	}
	if !result.IsNull() {
		t.Error("timeFunc() with blob should return NULL (unsupported type)")
	}

	result, err = datetimeFunc([]Value{NewBlobValue([]byte("2024-01-15 10:30:45"))})
	if err != nil {
		t.Errorf("datetimeFunc() with blob error = %v", err)
	}
	if !result.IsNull() {
		t.Error("datetimeFunc() with blob should return NULL (unsupported type)")
	}
}

// TestDateTimeFuncErrorPaths tests error paths in date/time functions
func TestDateTimeFuncErrorPaths(t *testing.T) {
	// Test parseDateTime with error during modifier application
	result, err := juliandayFunc([]Value{NewTextValue("now"), NewTextValue("weekday 10")})
	if err != nil {
		t.Errorf("juliandayFunc() error = %v", err)
	}
	if !result.IsNull() {
		t.Error("juliandayFunc() with invalid weekday should return NULL")
	}

	// Test computeJD path
	result, err = juliandayFunc([]Value{NewTextValue("2024-01-15")})
	if err != nil {
		t.Errorf("juliandayFunc() error = %v", err)
	}
	if result.IsNull() {
		t.Error("juliandayFunc() should not return NULL for valid date")
	}
}

// TestStrftimeFuncNullFormat tests strftime with null format
func TestStrftimeFuncNullFormat(t *testing.T) {
	// Null format becomes empty string, which is still a valid format
	result, err := strftimeFunc([]Value{NewNullValue(), NewTextValue("now")})
	if err != nil {
		t.Errorf("strftimeFunc() with null format error = %v", err)
	}
	// Empty format is valid, just returns empty string
	if result.Type() != TypeText {
		t.Errorf("strftimeFunc() with null format should return text, got %v", result.Type())
	}
}
