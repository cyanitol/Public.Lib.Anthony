// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions_test

import (
	"database/sql"
	"strings"
	"testing"
)

// queryStringArg is like queryOneString but accepts a single bound argument.
func queryStringArg(t *testing.T, db *sql.DB, query string, arg interface{}) (string, bool) {
	t.Helper()
	row := db.QueryRow(query, arg)
	var s sql.NullString
	if err := row.Scan(&s); err != nil {
		t.Fatalf("queryStringArg(%q): %v", query, err)
	}
	if !s.Valid {
		return "", true
	}
	return s.String, false
}

// queryFloat scans a single float64 column.
func queryFloat(t *testing.T, db *sql.DB, query string) float64 {
	t.Helper()
	row := db.QueryRow(query)
	var f float64
	if err := row.Scan(&f); err != nil {
		t.Fatalf("queryFloat(%q): %v", query, err)
	}
	return f
}

// TestDateAggregate_ApplyWeekdayAlreadyOnDay covers applyWeekday when the
// current day equals the target — daysToAdd==0 branch (line 546-548).
func TestDateAggregate_ApplyWeekdayAlreadyOnDay(t *testing.T) {
	db := openTestDB(t)

	// 2024-01-07 is a Sunday (weekday 0). Asking for 'weekday 0' should
	// return the same date because daysToAdd==0.
	got, isNull := queryOneString(t, db, `SELECT date('2024-01-07', 'weekday 0')`)
	if isNull {
		t.Fatal("date('2024-01-07', 'weekday 0') returned NULL")
	}
	if got != "2024-01-07" {
		t.Errorf("weekday 0 on Sunday = %q, want 2024-01-07", got)
	}

	// 2024-01-01 is a Monday (weekday 1). Asking for 'weekday 1' should keep
	// the same date.
	got2, isNull2 := queryOneString(t, db, `SELECT date('2024-01-01', 'weekday 1')`)
	if isNull2 {
		t.Fatal("date('2024-01-01', 'weekday 1') returned NULL")
	}
	if got2 != "2024-01-01" {
		t.Errorf("weekday 1 on Monday = %q, want 2024-01-01", got2)
	}
}

// TestDateAggregate_ApplyWeekdayAdvances covers applyWeekday advancing to a
// future weekday (daysToAdd > 0).
func TestDateAggregate_ApplyWeekdayAdvances(t *testing.T) {
	db := openTestDB(t)

	// 2024-01-01 is Monday (weekday 1). 'weekday 0' (Sunday) should advance 6 days.
	got, isNull := queryOneString(t, db, `SELECT date('2024-01-01', 'weekday 0')`)
	if isNull {
		t.Fatal("date('2024-01-01', 'weekday 0') returned NULL")
	}
	if got != "2024-01-07" {
		t.Errorf("weekday 0 from Monday = %q, want 2024-01-07", got)
	}

	// 'weekday 6' (Saturday) from Monday should advance 5 days.
	got2, isNull2 := queryOneString(t, db, `SELECT date('2024-01-01', 'weekday 6')`)
	if isNull2 {
		t.Fatal("date('2024-01-01', 'weekday 6') returned NULL")
	}
	if got2 != "2024-01-06" {
		t.Errorf("weekday 6 from Monday = %q, want 2024-01-06", got2)
	}
}

// TestDateAggregate_WeekdayAllDays tests 'weekday N' modifier for all values
// 0-6 via 'now' to ensure no panics and valid output dates.
func TestDateAggregate_WeekdayAllDays(t *testing.T) {
	db := openTestDB(t)

	for d := 0; d <= 6; d++ {
		mod := "weekday " + string(rune('0'+d))
		got, isNull := queryStringArg(t, db, `SELECT date('now', ?)`, mod)
		if isNull {
			t.Errorf("date('now', %q) returned NULL", mod)
			continue
		}
		// Result must look like YYYY-MM-DD
		if len(got) != 10 || got[4] != '-' || got[7] != '-' {
			t.Errorf("date('now', %q) = %q, not a valid date", mod, got)
		}
	}
}

// TestDateAggregate_HandleDateArithmeticMultipleModifiers covers
// handleDateArithmetic with consecutive modifiers of different unit types.
// assertDateQueryResult checks that sqlStr returns the expected non-NULL string value.
func assertDateQueryResult(t *testing.T, db *sql.DB, sqlStr, want string) {
	t.Helper()
	got, isNull := queryOneString(t, db, sqlStr)
	if isNull {
		t.Fatalf("%s returned NULL", sqlStr)
	}
	if got != want {
		t.Errorf("%s = %q, want %q", sqlStr, got, want)
	}
}

func TestDateAggregate_HandleDateArithmeticMultipleModifiers(t *testing.T) {
	db := openTestDB(t)

	assertDateQueryResult(t, db, `SELECT date('2024-01-01', '+1 month', '-3 days')`, "2024-01-29")
	assertDateQueryResult(t, db, `SELECT date('2022-03-15', '+2 years')`, "2024-03-15")
	assertDateQueryResult(t, db, `SELECT date('2024-03-01', '-3 months')`, "2023-12-01")
	assertDateQueryResult(t, db, `SELECT date('2024-01-01', '+48 hours')`, "2024-01-03")
}

// TestDateAggregate_HandleDateArithmeticMinutes covers the 'minutes' unit.
func TestDateAggregate_HandleDateArithmeticMinutes(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db, `SELECT time('10:00:00', '+90 minutes')`)
	if isNull {
		t.Fatal("time('+90 minutes') returned NULL")
	}
	if got != "11:30:00" {
		t.Errorf("time('+90 minutes') = %q, want 11:30:00", got)
	}
}

// TestDateAggregate_HandleDateArithmeticSeconds covers the 'seconds' unit.
func TestDateAggregate_HandleDateArithmeticSeconds(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db, `SELECT time('10:00:00', '+120 seconds')`)
	if isNull {
		t.Fatal("time('+120 seconds') returned NULL")
	}
	if got != "10:02:00" {
		t.Errorf("time('+120 seconds') = %q, want 10:02:00", got)
	}
}

// TestDateAggregate_DateFuncNullInput covers dateFunc returning NULL when the
// first argument is NULL.
func TestDateAggregate_DateFuncNullInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT date(NULL)`)
	if !isNull {
		t.Error("date(NULL) should return NULL, got non-NULL")
	}
}

// TestDateAggregate_DateFuncInvalidInput covers dateFunc returning NULL when
// the date string cannot be parsed.
func TestDateAggregate_DateFuncInvalidInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT date('invalid-date')`)
	if !isNull {
		t.Error("date('invalid-date') should return NULL, got non-NULL")
	}
}

// TestDateAggregate_TimeFuncNullInput covers timeFunc returning NULL for NULL input.
func TestDateAggregate_TimeFuncNullInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT time(NULL)`)
	if !isNull {
		t.Error("time(NULL) should return NULL, got non-NULL")
	}
}

// TestDateAggregate_TimeFuncInvalidInput covers timeFunc returning NULL for an
// invalid input.
func TestDateAggregate_TimeFuncInvalidInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT time('not-a-time')`)
	if !isNull {
		t.Error("time('not-a-time') should return NULL, got non-NULL")
	}
}

// TestDateAggregate_TimeFuncWithModifier exercises timeFunc with an arithmetic
// modifier to confirm the non-null path.
func TestDateAggregate_TimeFuncWithModifier(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db, `SELECT time('12:34:56', '+1 hour')`)
	if isNull {
		t.Fatal("time('12:34:56', '+1 hour') returned NULL")
	}
	if got != "13:34:56" {
		t.Errorf("time with +1 hour = %q, want 13:34:56", got)
	}
}

// TestDateAggregate_DatetimeFuncNullInput covers datetimeFunc returning NULL
// for NULL input.
func TestDateAggregate_DatetimeFuncNullInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT datetime(NULL)`)
	if !isNull {
		t.Error("datetime(NULL) should return NULL, got non-NULL")
	}
}

// TestDateAggregate_DatetimeFuncInvalidInput covers datetimeFunc returning NULL
// for invalid input.
func TestDateAggregate_DatetimeFuncInvalidInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT datetime('invalid')`)
	if !isNull {
		t.Error("datetime('invalid') should return NULL, got non-NULL")
	}
}

// TestDateAggregate_DatetimeFuncValid covers datetimeFunc successful path.
func TestDateAggregate_DatetimeFuncValid(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db, `SELECT datetime('2024-01-01 00:00:00')`)
	if isNull {
		t.Fatal("datetime('2024-01-01 00:00:00') returned NULL")
	}
	if got != "2024-01-01 00:00:00" {
		t.Errorf("datetime = %q, want 2024-01-01 00:00:00", got)
	}
}

// TestDateAggregate_JuliandayFuncNullInput covers juliandayFunc returning NULL
// for NULL input.
func TestDateAggregate_JuliandayFuncNullInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT julianday(NULL)`)
	if !isNull {
		t.Error("julianday(NULL) should return NULL, got non-NULL")
	}
}

// TestDateAggregate_JuliandayFuncInvalidInput covers juliandayFunc returning
// NULL for invalid input.
func TestDateAggregate_JuliandayFuncInvalidInput(t *testing.T) {
	db := openTestDB(t)

	_, isNull := queryOneString(t, db, `SELECT julianday('not-a-date')`)
	if !isNull {
		t.Error("julianday('not-a-date') should return NULL, got non-NULL")
	}
}

// TestDateAggregate_JuliandayFuncValid covers juliandayFunc successful path.
func TestDateAggregate_JuliandayFuncValid(t *testing.T) {
	db := openTestDB(t)

	jd := queryFloat(t, db, `SELECT julianday('2024-06-15')`)
	// 2024-06-15 should be around 2460476
	if jd < 2460000 || jd > 2461000 {
		t.Errorf("julianday('2024-06-15') = %f, expected ~2460476", jd)
	}
}

// TestDateAggregate_DatetimeFuncWithModifiers covers datetimeFunc with 'start
// of year' and '+6 months' modifiers.
func TestDateAggregate_DatetimeFuncWithModifiers(t *testing.T) {
	db := openTestDB(t)

	got, isNull := queryOneString(t, db, `SELECT datetime('now', 'start of year', '+6 months')`)
	if isNull {
		t.Fatal("datetime('now', 'start of year', '+6 months') returned NULL")
	}
	// Result should end in " 00:00:00" since start-of-year zeroes the time.
	if !strings.HasSuffix(got, " 00:00:00") {
		t.Errorf("datetime with start-of-year = %q, expected suffix ' 00:00:00'", got)
	}
	// Result should be a month 07 entry (January + 6 months = July).
	if !strings.Contains(got, "-07-01") {
		t.Errorf("datetime with +6 months from start-of-year = %q, expected -07-01", got)
	}
}

// TestDateAggregate_JSONGroupArrayWithBlob covers json_group_array with blob
// values, exercising the valueToJSONInterface TypeBlob branch and the
// marshalJSONValue success path.
func TestDateAggregate_JSONGroupArrayWithBlob(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Exec(`CREATE TABLE datest_blob (b BLOB)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO datest_blob VALUES (X'48656C6C6F')`); err != nil {
		t.Fatalf("INSERT blob: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO datest_blob VALUES (X'576F726C64')`); err != nil {
		t.Fatalf("INSERT blob 2: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(b) FROM datest_blob`)
	if isNull {
		t.Fatal("json_group_array(blob) returned NULL")
	}
	if !strings.HasPrefix(got, "[") || !strings.HasSuffix(got, "]") {
		t.Errorf("json_group_array(blob) = %q, want JSON array", got)
	}
}

// TestDateAggregate_JSONGroupArrayEmptyTable covers json_group_array producing
// "[]" for an empty result set (the nil-slice branch in Final()).
func TestDateAggregate_JSONGroupArrayEmptyTable(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Exec(`CREATE TABLE datest_empty (x INTEGER)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	got, isNull := queryOneString(t, db, `SELECT json_group_array(x) FROM datest_empty`)
	if isNull {
		t.Fatal("json_group_array on empty table returned NULL")
	}
	if got != "[]" {
		t.Errorf("json_group_array empty = %q, want []", got)
	}
}

// TestDateAggregate_ComputeJDWithHMSOnly covers computeJD's fallback path where
// validYMD is false (defaults to 2000-01-01) and validHMS is true.
func TestDateAggregate_ComputeJDWithHMSOnly(t *testing.T) {
	db := openTestDB(t)

	jd := queryFloat(t, db, `SELECT julianday('12:00:00')`)
	// 2000-01-01 12:00:00 UTC = Julian day 2451545.0
	const want = 2451545.0
	const epsilon = 0.0001
	diff := jd - want
	if diff < 0 {
		diff = -diff
	}
	if diff > epsilon {
		t.Errorf("julianday('12:00:00') = %f, want ~%f", jd, want)
	}
}
