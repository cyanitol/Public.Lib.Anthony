// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package functions

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// RegisterDateTimeFunctions registers all date/time functions.
func RegisterDateTimeFunctions(r *Registry) {
	r.Register(NewScalarFunc("date", -1, dateFunc))
	r.Register(NewScalarFunc("time", -1, timeFunc))
	r.Register(NewScalarFunc("datetime", -1, datetimeFunc))
	r.Register(NewScalarFunc("julianday", -1, juliandayFunc))
	r.Register(NewScalarFunc("unixepoch", -1, unixepochFunc))
	r.Register(NewScalarFunc("strftime", -1, strftimeFunc))
	r.Register(NewScalarFunc("current_date", 0, currentDateFunc))
	r.Register(NewScalarFunc("current_time", 0, currentTimeFunc))
	r.Register(NewScalarFunc("current_timestamp", 0, currentTimestampFunc))
}

// DateTime represents a date/time value in SQLite's internal format.
type DateTime struct {
	// Julian day number (milliseconds * 86400000)
	jd int64

	// Year, Month, Day, Hour, Minute, Second
	year   int
	month  int
	day    int
	hour   int
	minute int
	second float64

	// Timezone offset in minutes
	tz int

	// Validity flags
	validJD  bool
	validYMD bool
	validHMS bool

	// Other flags
	useSubsec bool
	isError   bool
}

const (
	// Julian day for 1970-01-01 00:00:00
	unixEpochJD = 2440587.5

	// Milliseconds per day
	msPerDay = 86400000
)

// parseDateTime parses a date/time string or value.
func parseDateTime(v Value) (*DateTime, error) {
	dt := &DateTime{}

	if v.IsNull() {
		return nil, fmt.Errorf("null value")
	}

	switch v.Type() {
	case TypeInteger, TypeFloat:
		// Numeric value - could be Julian day or Unix timestamp
		f := v.AsFloat64()
		dt.setRawNumber(f)

	case TypeText:
		s := v.AsString()
		if strings.ToLower(s) == "now" {
			dt.setNow()
		} else if err := dt.parseString(s); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("invalid date/time value")
	}

	return dt, nil
}

// setNow sets the DateTime to the current time.
func (dt *DateTime) setNow() {
	now := time.Now().UTC()
	dt.year = now.Year()
	dt.month = int(now.Month())
	dt.day = now.Day()
	dt.hour = now.Hour()
	dt.minute = now.Minute()
	dt.second = float64(now.Second()) + float64(now.Nanosecond())/1e9
	dt.validYMD = true
	dt.validHMS = true
	dt.computeJD()
}

// setRawNumber sets the DateTime from a numeric value.
func (dt *DateTime) setRawNumber(f float64) {
	// If in valid Julian day range, treat as Julian day
	if f >= 0.0 && f < 5373484.5 {
		dt.jd = int64(f*float64(msPerDay) + 0.5)
		dt.validJD = true
	} else {
		// Treat as Unix timestamp
		dt.jd = int64((f+unixEpochJD*86400.0)*1000.0 + 0.5)
		dt.validJD = true
	}
}

// parseString parses a date/time string.
func (dt *DateTime) parseString(s string) error {
	// Try YYYY-MM-DD format
	if dt.parseYMD(s) {
		return nil
	}

	// Try HH:MM:SS format
	if dt.parseHMS(s) {
		return nil
	}

	// Try as number
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		dt.setRawNumber(f)
		return nil
	}

	return fmt.Errorf("invalid date/time format: %s", s)
}

// splitDateTimeParts splits a date/time string on '-', ' ', and 'T' delimiters.
func splitDateTimeParts(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == '-' || r == ' ' || r == 'T'
	})
}

// parseYearField converts a string to a valid year integer.
// Returns the year and true on success, or 0 and false if invalid.
func parseYearField(s string) (int, bool) {
	year, err := strconv.Atoi(s)
	if err != nil || year < 0 || year > 9999 {
		return 0, false
	}
	return year, true
}

// parseMonthField converts a string to a valid month integer.
// Returns the month and true on success, or 0 and false if invalid.
func parseMonthField(s string) (int, bool) {
	month, err := strconv.Atoi(s)
	if err != nil || month < 1 || month > 12 {
		return 0, false
	}
	return month, true
}

// parseDayField converts a string to a valid day integer.
// Returns the day and true on success, or 0 and false if invalid.
func parseDayField(s string) (int, bool) {
	day, err := strconv.Atoi(s)
	if err != nil || day < 1 || day > 31 {
		return 0, false
	}
	return day, true
}

// parseTimeComponent attempts to parse a time component from a date/time string.
// It first tries joining the trailing parts[], and falls back to scanning s for a
// ' ' or 'T' separator and parsing the substring that follows.
func (dt *DateTime) parseTimeComponent(s string, parts []string) {
	timePart := strings.Join(parts, " ")
	if dt.parseHMS(timePart) {
		return
	}
	// Fallback: locate the separator in the original string
	idx := strings.IndexAny(s, " T")
	if idx > 0 && idx < len(s)-1 {
		dt.parseHMS(s[idx+1:])
	}
}

// parseYMD parses YYYY-MM-DD [HH:MM:SS] format.
func (dt *DateTime) parseYMD(s string) bool {
	parts := splitDateTimeParts(s)
	if len(parts) < 3 {
		return false
	}

	year, ok := parseYearField(parts[0])
	if !ok {
		return false
	}

	month, ok := parseMonthField(parts[1])
	if !ok {
		return false
	}

	day, ok := parseDayField(parts[2])
	if !ok {
		return false
	}

	dt.year = year
	dt.month = month
	dt.day = day
	dt.validYMD = true

	if len(parts) > 3 {
		dt.parseTimeComponent(s, parts[3:])
	}

	return true
}

func parseHourField(s string) (int, bool) {
	h, err := strconv.Atoi(s)
	if err != nil || h < 0 || h > 23 {
		return 0, false
	}
	return h, true
}

func parseMinuteField(s string) (int, bool) {
	m, err := strconv.Atoi(s)
	if err != nil || m < 0 || m > 59 {
		return 0, false
	}
	return m, true
}

func parseSecondField(s string) (float64, bool) {
	sec, err := strconv.ParseFloat(s, 64)
	if err != nil || sec < 0 || sec >= 60 {
		return 0, false
	}
	return sec, true
}

func (dt *DateTime) parseHMS(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) < 2 {
		return false
	}

	hour, ok := parseHourField(parts[0])
	if !ok {
		return false
	}

	minute, ok := parseMinuteField(parts[1])
	if !ok {
		return false
	}

	second := 0.0
	if len(parts) > 2 {
		sec, ok := parseSecondField(parts[2])
		if !ok {
			return false
		}
		second = sec
	}

	dt.hour = hour
	dt.minute = minute
	dt.second = second
	dt.validHMS = true

	return true
}

// computeJD computes the Julian day number from YMD and HMS.
func (dt *DateTime) computeJD() {
	if dt.validJD {
		return
	}

	year := dt.year
	month := dt.month
	day := dt.day

	if !dt.validYMD {
		year = 2000
		month = 1
		day = 1
	}

	// Meeus algorithm for Julian day calculation
	if month <= 2 {
		year--
		month += 12
	}

	a := year / 100
	b := 2 - a + a/4

	jd := int64(365.25*float64(year+4716)) +
		int64(30.6001*float64(month+1)) +
		int64(day) + int64(b) - 1524

	dt.jd = jd * msPerDay

	if dt.validHMS {
		dt.jd += int64(dt.hour)*3600000 +
			int64(dt.minute)*60000 +
			int64(dt.second*1000.0+0.5)
	}

	// Adjust for timezone
	if dt.tz != 0 {
		dt.jd -= int64(dt.tz) * 60000
	}

	dt.validJD = true
}

// computeYMD computes year, month, day from Julian day.
func (dt *DateTime) computeYMD() {
	if dt.validYMD {
		return
	}

	if !dt.validJD {
		dt.year = 2000
		dt.month = 1
		dt.day = 1
		dt.validYMD = true
		return
	}

	// Convert Julian day to calendar date (Meeus algorithm)
	z := int((dt.jd+43200000)/msPerDay) + 1
	alpha := int((float64(z) - 1867216.25) / 36524.25)
	a := z + 1 + alpha - alpha/4

	b := a + 1524
	c := int((float64(b) - 122.1) / 365.25)
	d := int(365.25 * float64(c))
	e := int(float64(b-d) / 30.6001)

	dt.day = b - d - int(30.6001*float64(e))
	if e < 14 {
		dt.month = e - 1
	} else {
		dt.month = e - 13
	}

	if dt.month > 2 {
		dt.year = c - 4716
	} else {
		dt.year = c - 4715
	}

	dt.validYMD = true
}

// computeHMS computes hour, minute, second from Julian day.
func (dt *DateTime) computeHMS() {
	if dt.validHMS {
		return
	}

	dt.computeJD()

	dayMs := int((dt.jd + 43200000) % msPerDay)
	dt.second = float64(dayMs%60000) / 1000.0
	dayMin := dayMs / 60000
	dt.minute = dayMin % 60
	dt.hour = dayMin / 60

	dt.validHMS = true
}

// applyModifier applies a modifier to the DateTime.
func (dt *DateTime) applyModifier(mod string) error {
	mod = strings.TrimSpace(strings.ToLower(mod))

	// Handle 'start of' modifiers
	if strings.HasPrefix(mod, "start of ") {
		unit := strings.TrimPrefix(mod, "start of ")
		return dt.startOf(unit)
	}

	// Handle numeric modifiers (+/- N units)
	if strings.Contains(mod, " ") {
		parts := strings.Fields(mod)
		if len(parts) >= 2 {
			amount, err := strconv.ParseFloat(parts[0], 64)
			if err == nil {
				unit := parts[1]
				if strings.HasSuffix(unit, "s") {
					unit = unit[:len(unit)-1]
				}
				return dt.add(amount, unit)
			}
		}
	}

	// Handle special modifiers
	switch mod {
	case "utc", "localtime", "auto", "subsec", "subsecond":
		// These would require more complex implementation
		return nil
	default:
		return fmt.Errorf("unknown modifier: %s", mod)
	}
}

// startOf sets the DateTime to the start of a time unit.
func (dt *DateTime) startOf(unit string) error {
	dt.computeYMD()
	dt.computeHMS()

	switch unit {
	case "day":
		dt.hour = 0
		dt.minute = 0
		dt.second = 0
		dt.validJD = false

	case "month":
		dt.day = 1
		dt.hour = 0
		dt.minute = 0
		dt.second = 0
		dt.validJD = false

	case "year":
		dt.month = 1
		dt.day = 1
		dt.hour = 0
		dt.minute = 0
		dt.second = 0
		dt.validJD = false

	default:
		return fmt.Errorf("invalid unit for 'start of': %s", unit)
	}

	return nil
}

// timeUnitMs maps time units to their millisecond multipliers.
var timeUnitMs = map[string]float64{
	"second": 1000,
	"minute": 60000,
	"hour":   3600000,
	"day":    float64(msPerDay),
}

// add adds an amount of time to the DateTime.
func (dt *DateTime) add(amount float64, unit string) error {
	dt.computeJD()

	if unit == "month" {
		return dt.addMonths(int(amount))
	}
	if unit == "year" {
		return dt.addYears(int(amount))
	}
	if mult, ok := timeUnitMs[unit]; ok {
		dt.jd += int64(amount * mult)
		dt.validYMD = false
		dt.validHMS = false
		return nil
	}
	return fmt.Errorf("unknown time unit: %s", unit)
}

// addMonths adds months to the DateTime.
func (dt *DateTime) addMonths(months int) error {
	dt.computeYMD()
	dt.month += months
	dt.normalizeMonth()
	dt.validJD = false
	return nil
}

// addYears adds years to the DateTime.
func (dt *DateTime) addYears(years int) error {
	dt.computeYMD()
	dt.year += years
	dt.validJD = false
	return nil
}

// normalizeMonth normalizes month to be within 1-12.
func (dt *DateTime) normalizeMonth() {
	for dt.month > 12 {
		dt.month -= 12
		dt.year++
	}
	for dt.month < 1 {
		dt.month += 12
		dt.year--
	}
}

// formatDate formats as YYYY-MM-DD.
func (dt *DateTime) formatDate() string {
	dt.computeYMD()
	return fmt.Sprintf("%04d-%02d-%02d", dt.year, dt.month, dt.day)
}

// formatTime formats as HH:MM:SS.
func (dt *DateTime) formatTime() string {
	dt.computeHMS()
	if dt.useSubsec {
		return fmt.Sprintf("%02d:%02d:%06.3f", dt.hour, dt.minute, dt.second)
	}
	return fmt.Sprintf("%02d:%02d:%02d", dt.hour, dt.minute, int(dt.second))
}

// formatDateTime formats as YYYY-MM-DD HH:MM:SS.
func (dt *DateTime) formatDateTime() string {
	return fmt.Sprintf("%s %s", dt.formatDate(), dt.formatTime())
}

// getJulianDay returns the Julian day number.
func (dt *DateTime) getJulianDay() float64 {
	dt.computeJD()
	return float64(dt.jd) / float64(msPerDay)
}

// getUnixEpoch returns seconds since Unix epoch.
func (dt *DateTime) getUnixEpoch() float64 {
	dt.computeJD()
	jdDays := float64(dt.jd) / float64(msPerDay)
	return (jdDays - unixEpochJD) * 86400.0
}

// Date/time function implementations

func dateFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		dt := &DateTime{}
		dt.setNow()
		return NewTextValue(dt.formatDate()), nil
	}

	dt, err := parseDateTime(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	// Apply modifiers
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return NewNullValue(), nil
		}
		if err := dt.applyModifier(args[i].AsString()); err != nil {
			return NewNullValue(), nil
		}
	}

	return NewTextValue(dt.formatDate()), nil
}

func timeFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		dt := &DateTime{}
		dt.setNow()
		return NewTextValue(dt.formatTime()), nil
	}

	dt, err := parseDateTime(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	// Apply modifiers
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return NewNullValue(), nil
		}
		if err := dt.applyModifier(args[i].AsString()); err != nil {
			return NewNullValue(), nil
		}
	}

	return NewTextValue(dt.formatTime()), nil
}

func datetimeFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		dt := &DateTime{}
		dt.setNow()
		return NewTextValue(dt.formatDateTime()), nil
	}

	dt, err := parseDateTime(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	// Apply modifiers
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return NewNullValue(), nil
		}
		if err := dt.applyModifier(args[i].AsString()); err != nil {
			return NewNullValue(), nil
		}
	}

	return NewTextValue(dt.formatDateTime()), nil
}

func juliandayFunc(args []Value) (Value, error) {
	if len(args) == 0 {
		dt := &DateTime{}
		dt.setNow()
		return NewFloatValue(dt.getJulianDay()), nil
	}

	dt, err := parseDateTime(args[0])
	if err != nil {
		return NewNullValue(), nil
	}

	// Apply modifiers
	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			return NewNullValue(), nil
		}
		if err := dt.applyModifier(args[i].AsString()); err != nil {
			return NewNullValue(), nil
		}
	}

	return NewFloatValue(dt.getJulianDay()), nil
}

func unixepochFunc(args []Value) (Value, error) {
	dt, err := getDateTimeForUnixEpoch(args)
	if err != nil {
		return NewNullValue(), nil
	}
	return formatEpochResult(dt), nil
}

// getDateTimeForUnixEpoch parses args and applies modifiers for unixepoch.
func getDateTimeForUnixEpoch(args []Value) (*DateTime, error) {
	if len(args) == 0 {
		dt := &DateTime{}
		dt.setNow()
		return dt, nil
	}

	dt, err := parseDateTime(args[0])
	if err != nil {
		return nil, err
	}

	if err := applyUnixEpochModifiers(dt, args[1:]); err != nil {
		return nil, err
	}
	return dt, nil
}

// applyUnixEpochModifiers applies modifiers including subsec handling.
func applyUnixEpochModifiers(dt *DateTime, modifiers []Value) error {
	for _, arg := range modifiers {
		if arg.IsNull() {
			return fmt.Errorf("null modifier")
		}
		mod := arg.AsString()
		lowerMod := strings.ToLower(mod)
		if lowerMod == "subsec" || lowerMod == "subsecond" {
			dt.useSubsec = true
		}
		if err := dt.applyModifier(mod); err != nil {
			return err
		}
	}
	return nil
}

// formatEpochResult returns the epoch as float or int based on subsec flag.
func formatEpochResult(dt *DateTime) Value {
	epoch := dt.getUnixEpoch()
	if dt.useSubsec {
		return NewFloatValue(epoch)
	}
	return NewIntValue(int64(epoch))
}

func strftimeFunc(args []Value) (Value, error) {
	if len(args) < 1 {
		return NewNullValue(), nil
	}

	format := args[0].AsString()

	var dt *DateTime
	if len(args) == 1 {
		dt = &DateTime{}
		dt.setNow()
	} else {
		var err error
		dt, err = parseDateTime(args[1])
		if err != nil {
			return NewNullValue(), nil
		}

		// Apply modifiers
		for i := 2; i < len(args); i++ {
			if args[i].IsNull() {
				return NewNullValue(), nil
			}
			if err := dt.applyModifier(args[i].AsString()); err != nil {
				return NewNullValue(), nil
			}
		}
	}

	dt.computeYMD()
	dt.computeHMS()

	result := dt.strftime(format)
	return NewTextValue(result), nil
}

// strftimeHandlers maps each format specifier byte to a function that
// renders the corresponding field of a DateTime as a string.
// Specifiers that need access to computed sub-fields (e.g. 's', 'J') call
// the appropriate getter; all others read already-computed struct fields.
var strftimeHandlers = map[byte]func(*DateTime) string{
	'd': func(dt *DateTime) string { return fmt.Sprintf("%02d", dt.day) },
	'm': func(dt *DateTime) string { return fmt.Sprintf("%02d", dt.month) },
	'Y': func(dt *DateTime) string { return fmt.Sprintf("%04d", dt.year) },
	'H': func(dt *DateTime) string { return fmt.Sprintf("%02d", dt.hour) },
	'M': func(dt *DateTime) string { return fmt.Sprintf("%02d", dt.minute) },
	'S': func(dt *DateTime) string { return fmt.Sprintf("%02d", int(dt.second)) },
	'f': func(dt *DateTime) string { return fmt.Sprintf("%06.3f", dt.second) },
	's': func(dt *DateTime) string { return fmt.Sprintf("%d", int64(dt.getUnixEpoch())) },
	'J': func(dt *DateTime) string { return fmt.Sprintf("%.16g", dt.getJulianDay()) },
}

// strftimeSpecifier resolves a single format specifier byte and appends its
// rendered value to result.  It handles the literal '%%' escape and falls back
// to writing the raw "%<c>" pair for any unrecognised specifier.
func (dt *DateTime) strftimeSpecifier(result *strings.Builder, spec byte) {
	if spec == '%' {
		result.WriteByte('%')
		return
	}
	if handler, ok := strftimeHandlers[spec]; ok {
		result.WriteString(handler(dt))
		return
	}
	// Unknown specifier: pass through verbatim.
	result.WriteByte('%')
	result.WriteByte(spec)
}

// strftime formats the DateTime according to the format string.
func (dt *DateTime) strftime(format string) string {
	var result strings.Builder

	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			i++
			dt.strftimeSpecifier(&result, format[i])
		} else {
			result.WriteByte(format[i])
		}
	}

	return result.String()
}

func currentDateFunc(args []Value) (Value, error) {
	dt := &DateTime{}
	dt.setNow()
	return NewTextValue(dt.formatDate()), nil
}

func currentTimeFunc(args []Value) (Value, error) {
	dt := &DateTime{}
	dt.setNow()
	return NewTextValue(dt.formatTime()), nil
}

func currentTimestampFunc(args []Value) (Value, error) {
	dt := &DateTime{}
	dt.setNow()
	return NewTextValue(dt.formatDateTime()), nil
}

// Helper to check for leap year
func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// Helper to get days in month
func daysInMonth(year, month int) int {
	switch month {
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 31
	}
}

// Helper to validate date
func isValidDate(year, month, day int) bool {
	if year < 0 || year > 9999 {
		return false
	}
	if month < 1 || month > 12 {
		return false
	}
	if day < 1 || day > daysInMonth(year, month) {
		return false
	}
	return true
}

// Helper for safe float to int conversion
func safeFloatToInt(f float64) int64 {
	if f > float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if f < float64(math.MinInt64) {
		return math.MinInt64
	}
	return int64(f)
}
