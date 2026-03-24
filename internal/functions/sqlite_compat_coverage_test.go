// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"math"
	"testing"
)

// TestSqliteVersionFunc tests the sqlite_version() function.
func TestSqliteVersionFunc(t *testing.T) {
	result, err := sqliteVersionFunc([]Value{})
	if err != nil {
		t.Fatalf("sqliteVersionFunc returned error: %v", err)
	}
	if result.AsString() != sqliteCompatVersion {
		t.Errorf("sqlite_version() = %q, want %q", result.AsString(), sqliteCompatVersion)
	}
}

// TestConnStatePlaceholder tests the connection-state placeholder function.
func TestConnStatePlaceholder(t *testing.T) {
	result, err := connStatePlaceholder([]Value{})
	if err != nil {
		t.Fatalf("connStatePlaceholder returned error: %v", err)
	}
	if result.AsInt64() != 0 {
		t.Errorf("connStatePlaceholder() = %d, want 0", result.AsInt64())
	}
}

// TestSoundexFunc tests the soundex() SQL function.
func TestSoundexFunc(t *testing.T) {
	tests := []struct {
		input Value
		want  string
	}{
		{NewTextValue("Robert"), "R163"},
		{NewTextValue("Rupert"), "R163"},
		{NewTextValue("Euler"), "E460"},
		{NewTextValue("Ellery"), "E460"},
		{NewTextValue("Gauss"), "G200"},
		{NewTextValue("Ghosh"), "G200"},
		{NewTextValue("Hilbert"), "H416"},
		{NewTextValue("Heilbronn"), "H416"},
		{NewTextValue(""), "?000"},
		{NewTextValue("   "), "?000"},
		{NewNullValue(), "?000"},
		{NewTextValue("123"), "?000"},
		{NewTextValue("A"), "A000"},
		{NewTextValue("Tymczak"), "T522"},
	}

	for _, tt := range tests {
		got, err := soundexFunc([]Value{tt.input})
		if err != nil {
			t.Errorf("soundexFunc(%v) error: %v", tt.input, err)
			continue
		}
		if got.AsString() != tt.want {
			t.Errorf("soundexFunc(%v) = %q, want %q", tt.input.AsString(), got.AsString(), tt.want)
		}
	}
}

// TestSoundexFindFirstLetter tests the helper that finds the first letter.
func TestSoundexFindFirstLetter(t *testing.T) {
	letter, rest := soundexFindFirstLetter("Robert")
	if letter != 'R' {
		t.Errorf("first letter = %c, want R", letter)
	}
	if rest != "obert" {
		t.Errorf("rest = %q, want %q", rest, "obert")
	}

	letter2, rest2 := soundexFindFirstLetter("123")
	if letter2 != 0 {
		t.Errorf("expected 0 for no-letter input, got %c", letter2)
	}
	if rest2 != "" {
		t.Errorf("expected empty rest, got %q", rest2)
	}
}

// TestSoundexUpdateCode tests the lastCode tracking helper.
func TestSoundexUpdateCode(t *testing.T) {
	// When there is a code (ok == true), return that code.
	got := soundexUpdateCode('B', '1', true, '2')
	if got != '1' {
		t.Errorf("soundexUpdateCode with code = %c, want '1'", got)
	}

	// H/W leave lastCode unchanged.
	got = soundexUpdateCode('H', 0, false, '3')
	if got != '3' {
		t.Errorf("soundexUpdateCode H = %c, want '3'", got)
	}
	got = soundexUpdateCode('W', 0, false, '3')
	if got != '3' {
		t.Errorf("soundexUpdateCode W = %c, want '3'", got)
	}

	// Vowels reset to 0.
	got = soundexUpdateCode('A', 0, false, '3')
	if got != 0 {
		t.Errorf("soundexUpdateCode vowel = %c, want 0", got)
	}
}

// TestComputeSoundex tests the core Soundex computation.
func TestComputeSoundex(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Robert", "R163"},
		{"A", "A000"},
		{"Pfister", "P236"},
		{"Jackson", "J250"},
	}
	for _, tt := range tests {
		got := computeSoundex(tt.input)
		if got != tt.want {
			t.Errorf("computeSoundex(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestLogVariadicFunc tests log() with 1, 2, and invalid arg counts.
func TestLogVariadicFunc(t *testing.T) {
	// 1 arg: natural log
	res, err := logVariadicFunc([]Value{NewFloatValue(math.E)})
	if err != nil {
		t.Fatalf("log(e) error: %v", err)
	}
	if math.Abs(res.AsFloat64()-1.0) > 1e-9 {
		t.Errorf("log(e) = %v, want ~1.0", res.AsFloat64())
	}

	// 2 args: log base 10 of 100
	res, err = logVariadicFunc([]Value{NewFloatValue(10.0), NewFloatValue(100.0)})
	if err != nil {
		t.Fatalf("log(10,100) error: %v", err)
	}
	if math.Abs(res.AsFloat64()-2.0) > 1e-9 {
		t.Errorf("log(10,100) = %v, want ~2.0", res.AsFloat64())
	}

	// Wrong arg count
	_, err = logVariadicFunc([]Value{})
	if err == nil {
		t.Error("expected error for 0 args to log()")
	}
}

// TestLogOneArg tests the logOneArg helper directly.
func TestLogOneArg(t *testing.T) {
	// NULL input
	res, err := logOneArg(NewNullValue())
	if err != nil {
		t.Fatalf("logOneArg(null) error: %v", err)
	}
	if !res.IsNull() {
		t.Error("logOneArg(null) should return null")
	}

	// Positive value
	res, err = logOneArg(NewFloatValue(1.0))
	if err != nil {
		t.Fatalf("logOneArg(1) error: %v", err)
	}
	if math.Abs(res.AsFloat64()-0.0) > 1e-9 {
		t.Errorf("logOneArg(1) = %v, want 0", res.AsFloat64())
	}

	// Non-positive value returns NaN
	res, err = logOneArg(NewFloatValue(0.0))
	if err != nil {
		t.Fatalf("logOneArg(0) error: %v", err)
	}
	if !math.IsNaN(res.AsFloat64()) {
		t.Errorf("logOneArg(0) = %v, want NaN", res.AsFloat64())
	}

	res, err = logOneArg(NewFloatValue(-5.0))
	if err != nil {
		t.Fatalf("logOneArg(-5) error: %v", err)
	}
	if !math.IsNaN(res.AsFloat64()) {
		t.Errorf("logOneArg(-5) = %v, want NaN", res.AsFloat64())
	}
}

// TestLogTwoArgs tests the logTwoArgs helper directly.
func TestLogTwoArgs(t *testing.T) {
	// NULL base
	res, err := logTwoArgs(NewNullValue(), NewFloatValue(100.0))
	if err != nil {
		t.Fatalf("logTwoArgs(null, 100) error: %v", err)
	}
	if !res.IsNull() {
		t.Error("logTwoArgs(null, 100) should return null")
	}

	// NULL x
	res, err = logTwoArgs(NewFloatValue(10.0), NewNullValue())
	if err != nil {
		t.Fatalf("logTwoArgs(10, null) error: %v", err)
	}
	if !res.IsNull() {
		t.Error("logTwoArgs(10, null) should return null")
	}

	// log(10, 1000) = 3
	res, err = logTwoArgs(NewFloatValue(10.0), NewFloatValue(1000.0))
	if err != nil {
		t.Fatalf("logTwoArgs(10,1000) error: %v", err)
	}
	if math.Abs(res.AsFloat64()-3.0) > 1e-9 {
		t.Errorf("logTwoArgs(10,1000) = %v, want ~3.0", res.AsFloat64())
	}

	// Invalid: base == 1
	res, err = logTwoArgs(NewFloatValue(1.0), NewFloatValue(100.0))
	if err != nil {
		t.Fatalf("logTwoArgs(1,100) error: %v", err)
	}
	if !math.IsNaN(res.AsFloat64()) {
		t.Errorf("logTwoArgs(1,100) = %v, want NaN", res.AsFloat64())
	}

	// Invalid: base <= 0
	res, err = logTwoArgs(NewFloatValue(0.0), NewFloatValue(100.0))
	if err != nil {
		t.Fatalf("logTwoArgs(0,100) error: %v", err)
	}
	if !math.IsNaN(res.AsFloat64()) {
		t.Errorf("logTwoArgs(0,100) = %v, want NaN", res.AsFloat64())
	}

	// Invalid: x <= 0
	res, err = logTwoArgs(NewFloatValue(10.0), NewFloatValue(0.0))
	if err != nil {
		t.Fatalf("logTwoArgs(10,0) error: %v", err)
	}
	if !math.IsNaN(res.AsFloat64()) {
		t.Errorf("logTwoArgs(10,0) = %v, want NaN", res.AsFloat64())
	}
}
