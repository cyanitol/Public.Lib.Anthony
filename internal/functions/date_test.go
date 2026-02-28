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
