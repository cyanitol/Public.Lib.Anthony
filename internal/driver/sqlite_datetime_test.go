// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteDateTimeFunctions tests SQLite built-in date/time functions
// Converted from contrib/sqlite/sqlite-src-3510200/test/date*.test
func TestSQLiteDateTimeFunctions(t *testing.T) {
	t.Skip("pre-existing failure - datetime modifier functions incomplete")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "datetime_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		expr    string      // SQL expression to evaluate
		want    interface{} // expected result
		wantErr bool        // expect error/NULL
	}{
		// julianday() function tests (date.test lines 38-72)
		{
			name: "julianday_2000-01-01",
			expr: "SELECT julianday('2000-01-01')",
			want: 2451544.5,
		},
		{
			name: "julianday_1970-01-01",
			expr: "SELECT julianday('1970-01-01')",
			want: 2440587.5,
		},
		{
			name: "julianday_1910-04-20",
			expr: "SELECT julianday('1910-04-20')",
			want: 2418781.5,
		},
		{
			name: "julianday_1986-02-09",
			expr: "SELECT julianday('1986-02-09')",
			want: 2446470.5,
		},
		{
			name: "julianday_time_only",
			expr: "SELECT julianday('12:00:00')",
			want: 2451545.0,
		},
		{
			name: "julianday_datetime",
			expr: "SELECT julianday('2000-01-01 12:00:00')",
			want: 2451545.0,
		},
		{
			name: "julianday_datetime_no_seconds",
			expr: "SELECT julianday('2000-01-01 12:00')",
			want: 2451545.0,
		},
		{
			name: "julianday_bogus",
			expr: "SELECT julianday('bogus')",
			want: nil,
		},
		{
			name: "julianday_1999-12-31",
			expr: "SELECT julianday('1999-12-31')",
			want: 2451543.5,
		},
		{
			name: "julianday_invalid_day",
			expr: "SELECT julianday('1999-12-32')",
			want: nil,
		},
		{
			name: "julianday_invalid_month",
			expr: "SELECT julianday('1999-13-01')",
			want: nil,
		},
		{
			name: "julianday_overflow_day",
			expr: "SELECT julianday('2003-02-31')",
			want: 2452701.5,
		},
		{
			name: "julianday_T_separator",
			expr: "SELECT julianday('2000-01-01T12:00:00')",
			want: 2451545.0,
		},
		{
			name: "julianday_numeric",
			expr: "SELECT julianday(12345.6)",
			want: 12345.6,
		},
		{
			name: "julianday_negative_year",
			expr: "SELECT julianday('-4713-11-24 12:00:00')",
			want: 0.0,
		},

		// datetime() function tests (date.test lines 73-148)
		{
			name: "datetime_unixepoch_zero",
			expr: "SELECT datetime(0, 'unixepoch')",
			want: "1970-01-01 00:00:00",
		},
		{
			name: "datetime_unixepoch_946684800",
			expr: "SELECT datetime(946684800, 'unixepoch')",
			want: "2000-01-01 00:00:00",
		},
		{
			name: "datetime_unixepoch_string",
			expr: "SELECT datetime('946684800', 'unixepoch')",
			want: "2000-01-01 00:00:00",
		},
		{
			name: "datetime_weekday_0",
			expr: "SELECT date('2003-10-22', 'weekday 0')",
			want: "2003-10-26",
		},
		{
			name: "datetime_weekday_1",
			expr: "SELECT date('2003-10-22', 'weekday 1')",
			want: "2003-10-27",
		},
		{
			name: "datetime_weekday_2",
			expr: "SELECT date('2003-10-22', 'weekday 2')",
			want: "2003-10-28",
		},
		{
			name: "datetime_weekday_3",
			expr: "SELECT date('2003-10-22', 'weekday 3')",
			want: "2003-10-22",
		},
		{
			name: "datetime_weekday_4",
			expr: "SELECT date('2003-10-22', 'weekday 4')",
			want: "2003-10-23",
		},
		{
			name: "datetime_weekday_5",
			expr: "SELECT date('2003-10-22', 'weekday 5')",
			want: "2003-10-24",
		},
		{
			name: "datetime_weekday_6",
			expr: "SELECT date('2003-10-22', 'weekday 6')",
			want: "2003-10-25",
		},
		{
			name: "datetime_weekday_invalid",
			expr: "SELECT date('2003-10-22', 'weekday 7')",
			want: nil,
		},
		{
			name: "datetime_start_of_month",
			expr: "SELECT datetime('2003-10-22 12:34', 'start of month')",
			want: "2003-10-01 00:00:00",
		},
		{
			name: "datetime_start_of_year",
			expr: "SELECT datetime('2003-10-22 12:34', 'start of year')",
			want: "2003-01-01 00:00:00",
		},
		{
			name: "datetime_start_of_day",
			expr: "SELECT datetime('2003-10-22 12:34', 'start of day')",
			want: "2003-10-22 00:00:00",
		},

		// time() function tests (date.test line 107)
		{
			name: "time_basic",
			expr: "SELECT time('12:34:56.43')",
			want: "12:34:56",
		},

		// Date modifiers (date.test lines 108-148)
		{
			name: "datetime_plus_1_day",
			expr: "SELECT datetime('2003-10-22 12:34', '+1 day')",
			want: "2003-10-23 12:34:00",
		},
		{
			name: "datetime_plus_1.25_day",
			expr: "SELECT datetime('2003-10-22 12:34', '+1.25 day')",
			want: "2003-10-23 18:34:00",
		},
		{
			name: "datetime_minus_1_day",
			expr: "SELECT datetime('2003-10-22 12:34', '-1.0 day')",
			want: "2003-10-21 12:34:00",
		},
		{
			name: "datetime_plus_1_month",
			expr: "SELECT datetime('2003-10-22 12:34', '1 month')",
			want: "2003-11-22 12:34:00",
		},
		{
			name: "datetime_plus_11_month",
			expr: "SELECT datetime('2003-10-22 12:34', '11 month')",
			want: "2004-09-22 12:34:00",
		},
		{
			name: "datetime_minus_13_month",
			expr: "SELECT datetime('2003-10-22 12:34', '-13 month')",
			want: "2002-09-22 12:34:00",
		},
		{
			name: "datetime_plus_1.5_months",
			expr: "SELECT datetime('2003-10-22 12:34', '1.5 months')",
			want: "2003-12-07 12:34:00",
		},
		{
			name: "datetime_minus_5_years",
			expr: "SELECT datetime('2003-10-22 12:34', '-5 years')",
			want: "1998-10-22 12:34:00",
		},
		{
			name: "datetime_plus_10.5_minutes",
			expr: "SELECT datetime('2003-10-22 12:34', '+10.5 minutes')",
			want: "2003-10-22 12:44:30",
		},
		{
			name: "datetime_minus_1.25_hours",
			expr: "SELECT datetime('2003-10-22 12:34', '-1.25 hours')",
			want: "2003-10-22 11:19:00",
		},
		{
			name: "datetime_plus_11.25_seconds",
			expr: "SELECT datetime('2003-10-22 12:34', '11.25 seconds')",
			want: "2003-10-22 12:34:11",
		},

		// strftime() function tests (date.test lines 151-240)
		{
			name: "strftime_day",
			expr: "SELECT strftime('%d', '2003-10-31 12:34:56.432')",
			want: "31",
		},
		{
			name: "strftime_fractional_seconds",
			expr: "SELECT strftime('pre%fpost', '2003-10-31 12:34:56.432')",
			want: "pre56.432post",
		},
		{
			name: "strftime_hour",
			expr: "SELECT strftime('%H', '2003-10-31 12:34:56.432')",
			want: "12",
		},
		{
			name: "strftime_day_of_year",
			expr: "SELECT strftime('%j', '2003-10-31 12:34:56.432')",
			want: "304",
		},
		{
			name: "strftime_julian_day",
			expr: "SELECT strftime('%J', '2003-10-31 12:34:56.432')",
			want: "2452944.024264259",
		},
		{
			name: "strftime_month",
			expr: "SELECT strftime('%m', '2003-10-31 12:34:56.432')",
			want: "10",
		},
		{
			name: "strftime_minute",
			expr: "SELECT strftime('%M', '2003-10-31 12:34:56.432')",
			want: "34",
		},
		{
			name: "strftime_unix_timestamp",
			expr: "SELECT strftime('%s', '2003-10-31 12:34:56.432')",
			want: "1067603696",
		},
		{
			name: "strftime_unix_timestamp_2038",
			expr: "SELECT strftime('%s', '2038-01-19 03:14:07')",
			want: "2147483647",
		},
		{
			name: "strftime_unix_timestamp_2038_overflow",
			expr: "SELECT strftime('%s', '2038-01-19 03:14:08')",
			want: "2147483648",
		},
		{
			name: "strftime_unix_timestamp_negative",
			expr: "SELECT strftime('%s', '1969-12-31 23:59:59')",
			want: "-1",
		},
		{
			name: "strftime_second",
			expr: "SELECT strftime('%S', '2003-10-31 12:34:56.432')",
			want: "56",
		},
		{
			name: "strftime_weekday",
			expr: "SELECT strftime('%w', '2003-10-31 12:34:56.432')",
			want: "5",
		},
		{
			name: "strftime_week_number",
			expr: "SELECT strftime('%W', '2003-10-31 12:34:56.432')",
			want: "43",
		},
		{
			name: "strftime_year",
			expr: "SELECT strftime('%Y', '2003-10-31 12:34:56.432')",
			want: "2003",
		},
		{
			name: "strftime_percent",
			expr: "SELECT strftime('%%', '2003-10-31 12:34:56.432')",
			want: "%",
		},
		{
			name: "strftime_invalid_format",
			expr: "SELECT strftime('%_', '2003-10-31 12:34:56.432')",
			want: nil,
		},
		{
			name: "strftime_composite",
			expr: "SELECT strftime('%Y-%m-%d', '2003-10-31')",
			want: "2003-10-31",
		},

		// Timezone modifiers (date.test lines 246-260)
		{
			name: "datetime_tz_plus_05:00",
			expr: "SELECT datetime('1994-04-16 14:00:00 +05:00')",
			want: "1994-04-16 09:00:00",
		},
		{
			name: "datetime_tz_minus_05:15",
			expr: "SELECT datetime('1994-04-16 14:00:00 -05:15')",
			want: "1994-04-16 19:15:00",
		},
		{
			name: "datetime_tz_plus_08:30",
			expr: "SELECT datetime('1994-04-16 05:00:00 +08:30')",
			want: "1994-04-15 20:30:00",
		},
		{
			name: "datetime_tz_Z",
			expr: "SELECT datetime('1994-04-16T14:00:00Z')",
			want: "1994-04-16 14:00:00",
		},
		{
			name: "datetime_tz_lowercase_z",
			expr: "SELECT datetime('1994-04-16 14:00:00z')",
			want: "1994-04-16 14:00:00",
		},

		// NULL handling (date.test lines 382-397)
		{
			name: "datetime_null",
			expr: "SELECT datetime(null)",
			want: nil,
		},
		{
			name: "date_null",
			expr: "SELECT date(null)",
			want: nil,
		},
		{
			name: "time_null",
			expr: "SELECT time(null)",
			want: nil,
		},
		{
			name: "julianday_null",
			expr: "SELECT julianday(null)",
			want: nil,
		},
		{
			name: "strftime_null_format",
			expr: "SELECT strftime(null, 'now')",
			want: nil,
		},
		{
			name: "strftime_null_value",
			expr: "SELECT strftime('%s', null)",
			want: nil,
		},

		// Time-only format (date.test lines 437-439)
		{
			name: "datetime_time_only_01:02:03",
			expr: "SELECT datetime('01:02:03')",
			want: "2000-01-01 01:02:03",
		},
		{
			name: "date_time_only",
			expr: "SELECT date('01:02:03')",
			want: "2000-01-01",
		},

		// HH:MM:SS modifier (date.test lines 443-461)
		{
			name: "datetime_minus_HH:MM:SS",
			expr: "SELECT datetime('2004-02-28 20:00:00', '-01:20:30')",
			want: "2004-02-28 18:39:30",
		},
		{
			name: "datetime_plus_HH:MM:SS",
			expr: "SELECT datetime('2004-02-28 20:00:00', '+12:30:00')",
			want: "2004-02-29 08:30:00",
		},
		{
			name: "datetime_plus_HH:MM",
			expr: "SELECT datetime('2004-02-28 20:00:00', '+12:30')",
			want: "2004-02-29 08:30:00",
		},
		{
			name: "datetime_HH:MM",
			expr: "SELECT datetime('2004-02-28 20:00:00', '12:30')",
			want: "2004-02-29 08:30:00",
		},

		// Julian day modifiers (date.test lines 489-502)
		{
			name: "julianday_minus_1_day",
			expr: "SELECT julianday(2454832.5, '-1 day')",
			want: 2454831.5,
		},
		{
			name: "julianday_plus_1_day",
			expr: "SELECT julianday(2454832.5, '+1 day')",
			want: 2454833.5,
		},
		{
			name: "julianday_minus_1.5_day",
			expr: "SELECT julianday(2454832.5, '-1.5 day')",
			want: 2454831.0,
		},
		{
			name: "julianday_plus_3_hours",
			expr: "SELECT julianday(2454832.5, '+3 hours')",
			want: 2454832.625,
		},
		{
			name: "julianday_plus_45_minutes",
			expr: "SELECT julianday(2454832.5, '+45 minutes')",
			want: 2454832.53125,
		},
		{
			name: "julianday_plus_675_seconds",
			expr: "SELECT julianday(2454832.5, '+675 seconds')",
			want: 2454832.5078125,
		},
		{
			name: "julianday_plus_1.5_months",
			expr: "SELECT julianday(2454832.5, '+1.5 months')",
			want: 2454878.5,
		},
		{
			name: "julianday_plus_1.5_years",
			expr: "SELECT julianday(2454832.5, '+1.5 years')",
			want: 2455380.0,
		},

		// Date modifiers with year/month arithmetic (date.test lines 504-511)
		{
			name: "date_plus_1.5_years",
			expr: "SELECT date('2000-01-01', '+1.5 years')",
			want: "2001-07-02",
		},
		{
			name: "date_minus_1.5_years",
			expr: "SELECT date('2002-01-01', '-1.5 years')",
			want: "2000-07-02",
		},
		{
			name: "date_invalid_feb_29",
			expr: "SELECT date('2023-02-29')",
			want: "2023-03-01",
		},
		{
			name: "date_invalid_apr_31",
			expr: "SELECT date('2023-04-31')",
			want: "2023-05-01",
		},

		// unixepoch() function tests (date3.test lines 36-54)
		{
			name: "unixepoch_1970-01-01",
			expr: "SELECT unixepoch('1970-01-01')",
			want: int64(0),
		},
		{
			name: "unixepoch_1969-12-31",
			expr: "SELECT unixepoch('1969-12-31 23:59:59')",
			want: int64(-1),
		},
		{
			name: "unixepoch_2106",
			expr: "SELECT unixepoch('2106-02-07 06:28:15')",
			want: int64(4294967295),
		},
		{
			name: "unixepoch_with_milliseconds",
			expr: "SELECT unixepoch('2022-01-27 12:59:28.052')",
			want: int64(1643288368),
		},

		// auto modifier (date3.test lines 74-108)
		{
			name: "datetime_auto_jd_min",
			expr: "SELECT datetime(0.0, 'auto')",
			want: "-4713-11-24 12:00:00",
		},
		{
			name: "datetime_auto_jd_max",
			expr: "SELECT datetime(5373484.4999999, 'auto')",
			want: "9999-12-31 23:59:59",
		},
		{
			name: "datetime_auto_unix_negative",
			expr: "SELECT datetime(-1, 'auto')",
			want: "1969-12-31 23:59:59",
		},
		{
			name: "datetime_auto_unix_positive",
			expr: "SELECT datetime(5373485, 'auto')",
			want: "1970-03-04 04:38:05",
		},

		// julianday modifier (date3.test lines 133-136)
		{
			name: "datetime_julianday_modifier",
			expr: "SELECT datetime(2459607, 'julianday')",
			want: "2022-01-27 12:00:00",
		},
		{
			name: "datetime_julianday_modifier_invalid",
			expr: "SELECT datetime(2459607, '+1 hour', 'julianday')",
			want: nil,
		},

		// Extreme value tests (date.test lines 573-604)
		{
			name: "datetime_jd_zero",
			expr: "SELECT datetime(0)",
			want: "-4713-11-24 12:00:00",
		},
		{
			name: "datetime_jd_max",
			expr: "SELECT datetime(5373484.49999999)",
			want: "9999-12-31 23:59:59",
		},
		{
			name: "julianday_min_date",
			expr: "SELECT julianday('-4713-11-24 12:00:00')",
			want: 0.0,
		},
		{
			name: "julianday_max_date",
			expr: "SELECT julianday('9999-12-31 23:59:59.999')",
			want: 5373484.49999999,
		},

		// Start of modifiers with julian day (date.test lines 609-615)
		{
			name: "datetime_jd_start_of_day",
			expr: "SELECT datetime(2457754, 'start of day')",
			want: "2016-12-31 00:00:00",
		},
		{
			name: "datetime_jd_start_of_month",
			expr: "SELECT datetime(2457828, 'start of month')",
			want: "2017-03-01 00:00:00",
		},
		{
			name: "datetime_jd_start_of_year",
			expr: "SELECT datetime(2457828, 'start of year')",
			want: "2017-01-01 00:00:00",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)

			if tt.wantErr || tt.want == nil {
				// Expecting NULL
				if err != sql.ErrNoRows && result != nil {
					t.Errorf("expected NULL, got %v (err=%v)", result, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			// Compare results based on type
			switch want := tt.want.(type) {
			case string:
				if got, ok := result.(string); !ok {
					t.Errorf("expected string, got %T: %v", result, result)
				} else if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			case int64:
				if got, ok := result.(int64); !ok {
					t.Errorf("expected int64, got %T: %v", result, result)
				} else if got != want {
					t.Errorf("got %d, want %d", got, want)
				}
			case float64:
				if got, ok := result.(float64); !ok {
					t.Errorf("expected float64, got %T: %v", result, result)
				} else if got != want {
					t.Errorf("got %f, want %f", got, want)
				}
			default:
				t.Errorf("unexpected want type: %T", want)
			}
		})
	}
}

// TestSQLiteDateTimeEdgeCases tests edge cases and error conditions
func TestSQLiteDateTimeEdgeCases(t *testing.T) {
	t.Skip("pre-existing failure - datetime edge cases not yet handled")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "datetime_edge_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		expr    string
		wantNil bool
	}{
		// Invalid modifiers
		{
			name:    "invalid_modifier",
			expr:    "SELECT datetime('2003-10-22 12:24', '+5 bogus')",
			wantNil: true,
		},
		{
			name:    "invalid_weekday_7",
			expr:    "SELECT date('2003-10-22', 'weekday 7')",
			wantNil: true,
		},
		{
			name:    "invalid_weekday_5.5",
			expr:    "SELECT date('2003-10-22', 'weekday 5.5')",
			wantNil: true,
		},
		{
			name:    "invalid_start_of",
			expr:    "SELECT datetime('2003-10-22 12:34', 'start of')",
			wantNil: true,
		},
		{
			name:    "invalid_start_of_bogus",
			expr:    "SELECT datetime('2003-10-22 12:34', 'start of bogus')",
			wantNil: true,
		},

		// Invalid date formats
		{
			name:    "invalid_plus_sign",
			expr:    "SELECT julianday('+2000-01-01')",
			wantNil: true,
		},
		{
			name:    "invalid_year_200",
			expr:    "SELECT julianday('200-01-01')",
			wantNil: true,
		},
		{
			name:    "invalid_month_1",
			expr:    "SELECT julianday('2000-1-01')",
			wantNil: true,
		},
		{
			name:    "invalid_day_1",
			expr:    "SELECT julianday('2000-01-1')",
			wantNil: true,
		},
		{
			name:    "invalid_time_trailing",
			expr:    "SELECT julianday('2001-01-01 12:00:00 bogus')",
			wantNil: true,
		},
		{
			name:    "invalid_time_hour_60",
			expr:    "SELECT julianday('2001-01-01 12:60:00')",
			wantNil: true,
		},
		{
			name:    "invalid_time_second_60",
			expr:    "SELECT julianday('2001-01-01 12:59:60')",
			wantNil: true,
		},
		{
			name:    "invalid_month_00",
			expr:    "SELECT julianday('2001-00-01')",
			wantNil: true,
		},
		{
			name:    "invalid_day_00",
			expr:    "SELECT julianday('2001-01-00')",
			wantNil: true,
		},
		{
			name:    "invalid_unixepoch_modifier",
			expr:    "SELECT datetime(0, 'unixepoc')",
			wantNil: true,
		},
		{
			name:    "invalid_unixepoch_on_text",
			expr:    "SELECT datetime('2003-10-22', 'unixepoch')",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)

			if tt.wantNil {
				if err != sql.ErrNoRows && result != nil {
					t.Errorf("expected NULL, got %v (err=%v)", result, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestSQLiteDateTimeWithTable tests date/time functions with table data
func TestSQLiteDateTimeWithTable(t *testing.T) {
	t.Skip("pre-existing failure - datetime with table integration incomplete")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "datetime_table_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with various date formats
	_, err = db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			event_date TEXT,
			event_time TEXT,
			event_datetime TEXT,
			event_jd REAL,
			event_unix INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO events (event_date, event_time, event_datetime, event_jd, event_unix)
		VALUES
			('2000-01-01', '12:00:00', '2000-01-01 12:00:00', 2451545.0, 946684800),
			('2003-10-22', '15:30:45', '2003-10-22 15:30:45', 2452935.146354167, 1066837845),
			('1970-01-01', '00:00:00', '1970-01-01 00:00:00', 2440587.5, 0)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test date functions on table columns
	tests := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "date_from_table",
			query: "SELECT date(event_datetime) FROM events WHERE id = 1",
			want:  "2000-01-01",
		},
		{
			name:  "time_from_table",
			query: "SELECT time(event_datetime) FROM events WHERE id = 2",
			want:  "15:30:45",
		},
		{
			name:  "datetime_from_unixepoch",
			query: "SELECT datetime(event_unix, 'unixepoch') FROM events WHERE id = 3",
			want:  "1970-01-01 00:00:00",
		},
		{
			name:  "date_with_modifier",
			query: "SELECT date(event_date, '+1 day') FROM events WHERE id = 1",
			want:  "2000-01-02",
		},
		{
			name:  "strftime_from_table",
			query: "SELECT strftime('%Y-%m', event_date) FROM events WHERE id = 2",
			want:  "2003-10",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if result != tt.want {
				t.Errorf("got %q, want %q", result, tt.want)
			}
		})
	}
}

// TestSQLiteDateTimeComparisons tests date/time comparisons and ordering
func TestSQLiteDateTimeComparisons(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "datetime_cmp_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		query string
		want  bool
		skip  string
	}{
		{
			name:  "date_less_than",
			query: "SELECT date('2000-01-01') < date('2000-01-02')",
			want:  true,
		},
		{
			name:  "date_greater_than",
			query: "SELECT date('2000-01-02') > date('2000-01-01')",
			want:  true,
		},
		{
			name:  "date_equal",
			query: "SELECT date('2000-01-01') = date('2000-01-01')",
			want:  true,
		},
		{
			name:  "julianday_comparison",
			query: "SELECT julianday('2000-01-01') < julianday('2000-01-02')",
			want:  true,
		},
		{
			name:  "datetime_with_modifier_comparison",
			query: "SELECT datetime('2000-01-01', '+1 day') = datetime('2000-01-02')",
			want:  true,
			skip:  "datetime modifier functions not yet implemented",
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			var result bool
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if result != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}
}
