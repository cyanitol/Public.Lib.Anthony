// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"testing"
)

// ---------------------------------------------------------------------------
// MC/DC for: year%4==0 && (year%100!=0 || year%400==0)
// Conditions: A=(year%4==0), B=(year%100!=0), C=(year%400==0)
// Short-circuit: if A is false the whole expression is false (B,C irrelevant).
// Independent-effect pairs:
//
//	A: {2024, 2023}  — B=T held constant, flipping A flips result
//	B: {2024, 1900}  — A=T held constant, flipping B flips result
//	C: {2000, 1900}  — A=T, B=F held constant, flipping C flips result
//
// ---------------------------------------------------------------------------
func TestIsLeapYear_MCDC(t *testing.T) {
	tests := []struct {
		name string
		year int
		want bool
	}{
		// Baseline where all relevant conditions are true → leap
		{"A=T B=T C=F: div4 not100 not400 → leap", 2024, true},
		// Flip A: not divisible by 4 → not leap (B,C irrelevant)
		{"A=F B=T C=F: not div4 → not leap", 2023, false},
		// Flip B (keep A=T): divisible by 100 but not 400 → not leap
		{"A=T B=F C=F: div4 is100 not400 → not leap", 1900, false},
		// Flip C (keep A=T, B=F): divisible by 400 → leap
		{"A=T B=F C=T: div4 is100 is400 → leap", 2000, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isLeapYear(tc.year)
			if got != tc.want {
				t.Errorf("isLeapYear(%d) = %v, want %v", tc.year, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: f >= 0.0 && f < maxJulianDay   (setRawNumber)
// Conditions: A=(f>=0.0), B=(f<maxJulianDay)
// Independent-effect pairs:
//
//	A: {1.0, -1.0}              — B=T held constant
//	B: {1.0, maxJulianDay+1}    — A=T held constant
//
// When A&&B is true → Julian-day branch (jd = f*msPerDay+0.5 rounded).
// When false → Unix-timestamp branch.
// ---------------------------------------------------------------------------
func TestSetRawNumber_MCDC(t *testing.T) {
	tests := []struct {
		name          string
		f             float64
		wantJulianDay bool // true → Julian branch; false → Unix branch
	}{
		// A=T B=T: positive, below max → Julian branch
		{"A=T B=T: f=1.0 in Julian range", 1.0, true},
		// A=F B=T: negative → Unix branch
		{"A=F B=T: f=-1.0 negative → Unix branch", -1.0, false},
		// A=T B=F: above max → Unix branch
		{"A=T B=F: f=maxJulianDay+1 above max → Unix branch", maxJulianDay + 1, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.setRawNumber(tc.f)

			if !dt.validJD {
				t.Fatal("validJD should always be set after setRawNumber")
			}

			if tc.wantJulianDay {
				// Julian branch: jd == round(f * msPerDay)
				wantJD := int64(tc.f*float64(msPerDay) + 0.5)
				if dt.jd != wantJD {
					t.Errorf("Julian branch: jd=%d, want %d", dt.jd, wantJD)
				}
			} else {
				// Unix branch: jd == round((f + unixEpochJD*86400)*1000)
				wantJD := int64((tc.f+unixEpochJD*86400.0)*1000.0 + 0.5)
				if dt.jd != wantJD {
					t.Errorf("Unix branch: jd=%d, want %d", dt.jd, wantJD)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || year < 0 || year > 9999  (parseYearField)
// Conditions: A=(err!=nil), B=(year<0), C=(year>9999)
// False outcome (valid year) requires A=F && B=F && C=F.
// Independent-effect pairs:
//
//	A: {"abc", "2000"}    — B=F, C=F held constant
//	B: {"-1",  "2000"}    — A=F, C=F held constant
//	C: {"10000", "2000"}  — A=F, B=F held constant
//
// ---------------------------------------------------------------------------
func TestParseYearField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		// A=F B=F C=F: valid year → ok
		{"A=F B=F C=F: valid year 2000", "2000", true, 2000},
		// A=T: non-numeric → not ok
		{"A=T: non-numeric input → not ok", "abc", false, 0},
		// A=F B=T: negative year → not ok
		{"A=F B=T: year=-1 → not ok", "-1", false, 0},
		// A=F B=F C=T: year>9999 → not ok
		{"A=F B=F C=T: year=10000 → not ok", "10000", false, 0},
		// Boundary: year=0 is valid (A=F B=F C=F)
		{"A=F B=F C=F: boundary year=0", "0", true, 0},
		// Boundary: year=9999 is valid
		{"A=F B=F C=F: boundary year=9999", "9999", true, 9999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseYearField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseYearField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseYearField(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || month < 1 || month > 12  (parseMonthField)
// Conditions: A=(err!=nil), B=(month<1), C=(month>12)
// Independent-effect pairs:
//
//	A: {"abc","6"}   — B=F, C=F
//	B: {"0","6"}     — A=F, C=F
//	C: {"13","6"}    — A=F, B=F
//
// ---------------------------------------------------------------------------
func TestParseMonthField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		// A=F B=F C=F: valid month
		{"A=F B=F C=F: month=6 valid", "6", true, 6},
		// A=T: non-numeric
		{"A=T: non-numeric → not ok", "abc", false, 0},
		// A=F B=T: month=0 below range
		{"A=F B=T: month=0 below range", "0", false, 0},
		// A=F B=F C=T: month=13 above range
		{"A=F B=F C=T: month=13 above range", "13", false, 0},
		// Boundary: month=1
		{"A=F B=F C=F: boundary month=1", "1", true, 1},
		// Boundary: month=12
		{"A=F B=F C=F: boundary month=12", "12", true, 12},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseMonthField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseMonthField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseMonthField(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || day < 1 || day > 31  (parseDayField)
// Conditions: A=(err!=nil), B=(day<1), C=(day>31)
// Independent-effect pairs:
//
//	A: {"abc","15"}  — B=F, C=F
//	B: {"0","15"}    — A=F, C=F
//	C: {"32","15"}   — A=F, B=F
//
// ---------------------------------------------------------------------------
func TestParseDayField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		// A=F B=F C=F: valid day
		{"A=F B=F C=F: day=15 valid", "15", true, 15},
		// A=T: non-numeric
		{"A=T: non-numeric → not ok", "abc", false, 0},
		// A=F B=T: day=0 below range
		{"A=F B=T: day=0 below range", "0", false, 0},
		// A=F B=F C=T: day=32 above range
		{"A=F B=F C=T: day=32 above range", "32", false, 0},
		// Boundary: day=1
		{"A=F B=F C=F: boundary day=1", "1", true, 1},
		// Boundary: day=31
		{"A=F B=F C=F: boundary day=31", "31", true, 31},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseDayField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseDayField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseDayField(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: idx > 0 && idx < len(s)-1  (parseTimeComponent fallback)
// Conditions: A=(idx>0), B=(idx<len(s)-1)
// The expression is true only when both hold; false on either.
// We exercise this indirectly via parseYMD with date+time strings where
// the time-component parsing falls through to the fallback scan.
//
// Cases:
//
//	A=T B=T: valid separator position → time parsed
//	A=F:     separator at index 0 (no real date part before it) → skipped
//	A=T B=F: separator is the last character → skipped
//
// ---------------------------------------------------------------------------
func TestParseTimeComponent_FallbackMCDC(t *testing.T) {
	tests := []struct {
		name         string
		s            string
		wantValidHMS bool
	}{
		// A=T B=T: well-formed "YYYY-MM-DD HH:MM:SS" — separator at idx>0 and not last
		{"A=T B=T: valid date-time string → HMS parsed", "2024-01-15 10:20:30", true},
		// A=F: string starts with separator ' ' before any date digits (idx==0)
		// parseYMD will fail before reaching parseTimeComponent at all, so HMS stays false.
		{"A=F: separator at index 0 → YMD fails → HMS not set", " 10:20:30", false},
		// A=T B=F: separator is the very last character → nothing after it
		// A date string ending in ' ' has the separator at len-1, so idx == len(s)-1.
		{"A=T B=F: separator at last position → HMS not parsed", "2024-01-15 ", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.parseYMD(tc.s) // result not checked; we inspect HMS validity
			if dt.validHMS != tc.wantValidHMS {
				t.Errorf("parseYMD(%q) validHMS=%v, want %v", tc.s, dt.validHMS, tc.wantValidHMS)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || h < 0 || h > 23  (parseHourField)
// Conditions: A=(err!=nil), B=(h<0), C=(h>23)
// Independent-effect pairs:
//
//	A: {"abc","12"}  — B=F, C=F
//	B: {"-1","12"}   — A=F, C=F
//	C: {"24","12"}   — A=F, B=F
//
// ---------------------------------------------------------------------------
func TestParseHourField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		{"A=F B=F C=F: hour=12 valid", "12", true, 12},
		{"A=T: non-numeric → not ok", "abc", false, 0},
		{"A=F B=T: hour=-1 below range", "-1", false, 0},
		{"A=F B=F C=T: hour=24 above range", "24", false, 0},
		{"A=F B=F C=F: boundary hour=0", "0", true, 0},
		{"A=F B=F C=F: boundary hour=23", "23", true, 23},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseHourField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseHourField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseHourField(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || m < 0 || m > 59  (parseMinuteField)
// Conditions: A=(err!=nil), B=(m<0), C=(m>59)
// ---------------------------------------------------------------------------
func TestParseMinuteField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		{"A=F B=F C=F: minute=30 valid", "30", true, 30},
		{"A=T: non-numeric → not ok", "abc", false, 0},
		{"A=F B=T: minute=-1 below range", "-1", false, 0},
		{"A=F B=F C=T: minute=60 above range", "60", false, 0},
		{"A=F B=F C=F: boundary minute=0", "0", true, 0},
		{"A=F B=F C=F: boundary minute=59", "59", true, 59},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseMinuteField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseMinuteField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseMinuteField(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || sec < 0 || sec >= 60  (parseSecondField)
// Conditions: A=(err!=nil), B=(sec<0), C=(sec>=60)
// ---------------------------------------------------------------------------
func TestParseSecondField_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal float64
	}{
		{"A=F B=F C=F: second=30.5 valid", "30.5", true, 30.5},
		{"A=T: non-numeric → not ok", "abc", false, 0},
		{"A=F B=T: second=-0.001 below range", "-0.001", false, 0},
		{"A=F B=F C=T: second=60 at upper bound → not ok", "60", false, 0},
		{"A=F B=F C=F: boundary second=0", "0", true, 0},
		{"A=F B=F C=F: boundary second=59.999", "59.999", true, 59.999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseSecondField(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseSecondField(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseSecondField(%q) = %v, want %v", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || targetDay < 0 || targetDay > 6  (handleWeekdayModifier)
// Conditions: A=(err!=nil), B=(targetDay<0), C=(targetDay>6)
// Tested through handleWeekdayModifier via the mod string.
// ---------------------------------------------------------------------------
func TestHandleWeekdayModifier_MCDC(t *testing.T) {
	tests := []struct {
		name        string
		mod         string
		wantHandled bool
		wantErr     bool
	}{
		// Prefix does not match → not handled at all
		{"no weekday prefix → not handled", "start of day", false, false},
		// A=F B=F C=F: valid weekday
		{"A=F B=F C=F: weekday=3 valid", "weekday 3", true, false},
		// A=T: non-numeric day string
		{"A=T: weekday non-numeric → error", "weekday abc", true, true},
		// A=F B=T: targetDay=-1
		{"A=F B=T: weekday=-1 below range → error", "weekday -1", true, true},
		// A=F B=F C=T: targetDay=7
		{"A=F B=F C=T: weekday=7 above range → error", "weekday 7", true, true},
		// Boundary: weekday=0 (Sunday)
		{"A=F B=F C=F: boundary weekday=0", "weekday 0", true, false},
		// Boundary: weekday=6 (Saturday)
		{"A=F B=F C=F: boundary weekday=6", "weekday 6", true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.setNow()
			handled, err := dt.handleWeekdayModifier(tc.mod)
			if handled != tc.wantHandled {
				t.Errorf("handleWeekdayModifier(%q) handled=%v, want %v", tc.mod, handled, tc.wantHandled)
			}
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("handleWeekdayModifier(%q) err=%v, wantErr=%v", tc.mod, err, tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: len(parts) >= 2 && len(parts) <= 3  (isValidTimePartCount)
// Conditions: A=(len>=2), B=(len<=3)
// Independent-effect pairs:
//
//	A: {["10","20"], ["10"]}      — B=T held (len<=3 true for both)
//	B: {["10","20","30"], ["10","20","30","40"]} — A=T held
//
// Note: len=1 makes A=F; len=4 makes B=F; len=2 or 3 makes both true.
// ---------------------------------------------------------------------------
func TestIsValidTimePartCount_MCDC(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  bool
	}{
		// A=T B=T: len=2 → valid
		{"A=T B=T: len=2 → valid", []string{"10", "20"}, true},
		// A=T B=T: len=3 → valid
		{"A=T B=T: len=3 → valid", []string{"10", "20", "30"}, true},
		// A=F B=T: len=1 → invalid (A flips result)
		{"A=F B=T: len=1 → invalid", []string{"10"}, false},
		// A=F B=T: len=0 → invalid
		{"A=F B=T: len=0 → invalid", []string{}, false},
		// A=T B=F: len=4 → invalid (B flips result)
		{"A=T B=F: len=4 → invalid", []string{"10", "20", "30", "40"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isValidTimePartCount(tc.parts)
			if got != tc.want {
				t.Errorf("isValidTimePartCount(%v) = %v, want %v", tc.parts, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || minutes < 0 || minutes > 59  (parseMinutes)
// Conditions: A=(err!=nil), B=(minutes<0), C=(minutes>59)
// ---------------------------------------------------------------------------
func TestParseMinutes_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		wantOK  bool
		wantVal int
	}{
		{"A=F B=F C=F: minutes=30 valid", "30", true, 30},
		{"A=T: non-numeric → not ok", "abc", false, 0},
		{"A=F B=T: minutes=-1 → not ok", "-1", false, 0},
		{"A=F B=F C=T: minutes=60 → not ok", "60", false, 0},
		{"A=F B=F C=F: boundary minutes=0", "0", true, 0},
		{"A=F B=F C=F: boundary minutes=59", "59", true, 59},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseMinutes(tc.s)
			if ok != tc.wantOK {
				t.Errorf("parseMinutes(%q) ok=%v, want %v", tc.s, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseMinutes(%q) = %d, want %d", tc.s, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: err != nil || seconds < 0 || seconds >= 60  (parseSeconds)
// Conditions: A=(err!=nil), B=(seconds<0), C=(seconds>=60)
// parseSeconds receives a []string; it only parses parts[2] when len>=3.
// When len<3 it returns (0, true) immediately without touching these conditions,
// so we always supply len>=3 to reach the compound guard.
// ---------------------------------------------------------------------------
func TestParseSeconds_MCDC(t *testing.T) {
	tests := []struct {
		name    string
		parts   []string
		wantOK  bool
		wantVal float64
	}{
		// Len < 3: short-circuit returns ok immediately (baseline control)
		{"len=2: short-circuit → ok=true seconds=0", []string{"10", "20"}, true, 0},
		// A=F B=F C=F: valid seconds
		{"A=F B=F C=F: seconds=45.5 valid", []string{"10", "20", "45.5"}, true, 45.5},
		// A=T: non-numeric parts[2]
		{"A=T: non-numeric seconds → not ok", []string{"10", "20", "abc"}, false, 0},
		// A=F B=T: seconds=-0.1
		{"A=F B=T: seconds=-0.1 → not ok", []string{"10", "20", "-0.1"}, false, 0},
		// A=F B=F C=T: seconds=60 at limit → not ok
		{"A=F B=F C=T: seconds=60 → not ok", []string{"10", "20", "60"}, false, 0},
		// Boundary: seconds=0
		{"A=F B=F C=F: boundary seconds=0", []string{"10", "20", "0"}, true, 0},
		// Boundary: seconds=59.999
		{"A=F B=F C=F: boundary seconds=59.999", []string{"10", "20", "59.999"}, true, 59.999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseSeconds(tc.parts)
			if ok != tc.wantOK {
				t.Errorf("parseSeconds(%v) ok=%v, want %v", tc.parts, ok, tc.wantOK)
			}
			if ok && got != tc.wantVal {
				t.Errorf("parseSeconds(%v) = %v, want %v", tc.parts, got, tc.wantVal)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: lowerMod == "subsec" || lowerMod == "subsecond"  (applyUnixEpochModifiers)
// Conditions: A=(lowerMod=="subsec"), B=(lowerMod=="subsecond")
// Short-circuit OR: if A is true, B is not evaluated.
// Independent-effect pairs:
//
//	A: {"subsec", "utc"}        — B=F held
//	B: {"subsecond", "utc"}     — A=F held (B flips)
//
// Tested indirectly: applyUnixEpochModifiers sets dt.useSubsec=true when matched.
// ---------------------------------------------------------------------------
func TestApplyUnixEpochModifiers_SubsecMCDC(t *testing.T) {
	tests := []struct {
		name       string
		mods       []Value
		wantSubsec bool
		wantErr    bool
	}{
		// A=F B=F: unrelated modifier → useSubsec stays false
		{"A=F B=F: mod=utc → no subsec", []Value{NewTextValue("utc")}, false, false},
		// A=T B=F: mod=subsec → useSubsec becomes true
		{"A=T B=F: mod=subsec → subsec set", []Value{NewTextValue("subsec")}, true, false},
		// A=F B=T: mod=subsecond → useSubsec becomes true
		{"A=F B=T: mod=subsecond → subsec set", []Value{NewTextValue("subsecond")}, true, false},
		// Case insensitive: SUBSEC
		{"A=T B=F: mod=SUBSEC upper → subsec set", []Value{NewTextValue("SUBSEC")}, true, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := &DateTime{}
			dt.setNow()
			err := applyUnixEpochModifiers(dt, tc.mods)
			gotErr := err != nil
			if gotErr != tc.wantErr {
				t.Errorf("applyUnixEpochModifiers err=%v, wantErr=%v", err, tc.wantErr)
			}
			if dt.useSubsec != tc.wantSubsec {
				t.Errorf("applyUnixEpochModifiers useSubsec=%v, want %v", dt.useSubsec, tc.wantSubsec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MC/DC for: format[i] == '%' && i+1 < len(format)  (strftime inner loop)
// Conditions: A=(format[i]=='%'), B=(i+1 < len(format))
// Independent-effect pairs:
//
//	A: {"%Y" at i=0, "XY" at i=0}         — B=T held constant
//	B: {"%" alone (trailing), "%Y" both at i=0} — A=T held constant
//
// When A&&B: specifier is consumed and rendered.
// When A=F: character is written verbatim.
// When A=T B=F: trailing '%' written verbatim (loop ends, no specifier).
// Tested through the exported strftimeFunc.
// ---------------------------------------------------------------------------
func TestStrftime_PercentGuardMCDC(t *testing.T) {
	makeArgs := func(format, datetime string) []Value {
		return []Value{NewTextValue(format), NewTextValue(datetime)}
	}

	tests := []struct {
		name string
		args []Value
		want string
	}{
		// A=T B=T: '%' followed by valid specifier 'Y' → year rendered
		{"A=T B=T: %Y → year 2024", makeArgs("%Y", "2024-06-15"), "2024"},
		// A=F B=T: non-'%' character → written verbatim
		{"A=F B=T: literal X → verbatim X", makeArgs("X", "2024-06-15"), "X"},
		// A=T B=F: trailing '%' at end of format string → written verbatim as '%'
		{"A=T B=F: trailing % → verbatim percent", makeArgs("%", "2024-06-15"), "%"},
		// A=T B=T: '%%' escape → single '%' in output
		{"A=T B=T: %% escape → single percent", makeArgs("%%", "2024-06-15"), "%"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := strftimeFunc(tc.args)
			if err != nil {
				t.Fatalf("strftimeFunc error: %v", err)
			}
			got := result.AsString()
			if got != tc.want {
				t.Errorf("strftimeFunc(%q, %q) = %q, want %q",
					tc.args[0].AsString(), tc.args[1].AsString(), got, tc.want)
			}
		})
	}
}
