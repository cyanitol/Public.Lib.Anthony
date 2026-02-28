package driver

import (
	"database/sql"
	"math"
	"path/filepath"
	"testing"
)

// TestSQLiteFunctions tests SQLite built-in scalar functions
// Converted from contrib/sqlite/sqlite-src-3510200/test/func*.test
func TestSQLiteFunctions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "func_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE test(t TEXT, n INTEGER, r REAL)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Insert test data
	testData := []struct {
		t string
		n interface{}
		r interface{}
	}{
		{"hello", 1, 1.5},
		{"world", 42, 3.14},
		{"test", -10, -2.5},
		{"SQLite", 0, 0.0},
		{"", nil, nil},
	}

	for _, td := range testData {
		_, err = db.Exec("INSERT INTO test(t, n, r) VALUES(?, ?, ?)", td.t, td.n, td.r)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	tests := []struct {
		name    string
		expr    string      // Function call expression
		want    interface{} // expected result
		wantErr bool
	}{
		// length() function tests (func.test lines 42-64)
		{
			name: "length_simple",
			expr: "SELECT length('hello')",
			want: int64(5),
		},
		{
			name: "length_empty_string",
			expr: "SELECT length('')",
			want: int64(0),
		},
		{
			name: "length_null",
			expr: "SELECT length(NULL)",
			want: nil,
		},
		{
			name: "length_from_table",
			expr: "SELECT length(t) FROM test WHERE t = 'hello'",
			want: int64(5),
		},

		// substr() function tests (func.test lines 87-129, func2.test)
		{
			name: "substr_basic",
			expr: "SELECT substr('hello', 1, 2)",
			want: "he",
		},
		{
			name: "substr_from_position",
			expr: "SELECT substr('hello', 2, 3)",
			want: "ell",
		},
		{
			name: "substr_negative_start",
			expr: "SELECT substr('hello', -2, 2)",
			want: "lo",
		},
		{
			name: "substr_no_length",
			expr: "SELECT substr('hello', 2)",
			want: "ello",
		},
		{
			name: "substr_zero_start",
			expr: "SELECT substr('Supercalifragilisticexpialidocious', 0, 2)",
			want: "S",
		},
		{
			name: "substr_position_one",
			expr: "SELECT substr('Supercalifragilisticexpialidocious', 1, 5)",
			want: "Super",
		},
		{
			name: "substr_negative_position",
			expr: "SELECT substr('Supercalifragilisticexpialidocious', -5, 5)",
			want: "cious",
		},

		// lower() and upper() functions (func.test lines 358-375)
		{
			name: "lower_basic",
			expr: "SELECT lower('HELLO')",
			want: "hello",
		},
		{
			name: "upper_basic",
			expr: "SELECT upper('hello')",
			want: "HELLO",
		},
		{
			name: "lower_mixed",
			expr: "SELECT lower('HeLLo WoRLd')",
			want: "hello world",
		},
		{
			name: "upper_mixed",
			expr: "SELECT upper('HeLLo WoRLd')",
			want: "HELLO WORLD",
		},
		{
			name: "lower_null",
			expr: "SELECT lower(NULL)",
			want: nil,
		},
		{
			name: "upper_null",
			expr: "SELECT upper(NULL)",
			want: nil,
		},

		// abs() function tests (func.test lines 207-234)
		{
			name: "abs_positive",
			expr: "SELECT abs(42)",
			want: int64(42),
		},
		{
			name: "abs_negative",
			expr: "SELECT abs(-42)",
			want: int64(42),
		},
		{
			name: "abs_zero",
			expr: "SELECT abs(0)",
			want: int64(0),
		},
		{
			name: "abs_float_positive",
			expr: "SELECT abs(3.14)",
			want: 3.14,
		},
		{
			name: "abs_float_negative",
			expr: "SELECT abs(-3.14)",
			want: 3.14,
		},
		{
			name: "abs_null",
			expr: "SELECT abs(NULL)",
			want: nil,
		},

		// round() function tests (func.test lines 236-354)
		{
			name: "round_no_precision",
			expr: "SELECT round(3.14)",
			want: 3.0,
		},
		{
			name: "round_with_precision",
			expr: "SELECT round(3.14159, 2)",
			want: 3.14,
		},
		{
			name: "round_negative",
			expr: "SELECT round(-2.7)",
			want: -3.0,
		},
		{
			name: "round_half_up",
			expr: "SELECT round(2.5)",
			want: 3.0,
		},
		{
			name: "round_null",
			expr: "SELECT round(NULL)",
			want: nil,
		},

		// typeof() function tests (func.test lines 1341-1363)
		{
			name: "typeof_integer",
			expr: "SELECT typeof(42)",
			want: "integer",
		},
		{
			name: "typeof_real",
			expr: "SELECT typeof(3.14)",
			want: "real",
		},
		{
			name: "typeof_text",
			expr: "SELECT typeof('hello')",
			want: "text",
		},
		{
			name: "typeof_null",
			expr: "SELECT typeof(NULL)",
			want: "null",
		},

		// coalesce() function tests (func.test lines 378-393)
		{
			name: "coalesce_first_non_null",
			expr: "SELECT coalesce(1, 2, 3)",
			want: int64(1),
		},
		{
			name: "coalesce_skip_null",
			expr: "SELECT coalesce(NULL, 2, 3)",
			want: int64(2),
		},
		{
			name: "coalesce_all_null",
			expr: "SELECT coalesce(NULL, NULL, NULL)",
			want: nil,
		},
		{
			name: "coalesce_text",
			expr: "SELECT coalesce(NULL, 'hello', 'world')",
			want: "hello",
		},

		// ifnull() function tests
		{
			name: "ifnull_first_not_null",
			expr: "SELECT ifnull(42, 0)",
			want: int64(42),
		},
		{
			name: "ifnull_first_null",
			expr: "SELECT ifnull(NULL, 42)",
			want: int64(42),
		},
		{
			name: "ifnull_both_null",
			expr: "SELECT ifnull(NULL, NULL)",
			want: nil,
		},

		// nullif() function tests (func.test lines 385-392)
		{
			name: "nullif_equal",
			expr: "SELECT nullif(1, 1)",
			want: nil,
		},
		{
			name: "nullif_not_equal",
			expr: "SELECT nullif(1, 2)",
			want: int64(1),
		},
		{
			name: "nullif_null_first",
			expr: "SELECT nullif(NULL, 1)",
			want: nil,
		},

		// replace() function tests (func.test lines 1019-1073)
		{
			name: "replace_basic",
			expr: "SELECT replace('hello world', 'world', 'SQLite')",
			want: "hello SQLite",
		},
		{
			name: "replace_multiple",
			expr: "SELECT replace('aaa', 'a', 'b')",
			want: "bbb",
		},
		{
			name: "replace_empty",
			expr: "SELECT replace('hello', 'x', 'y')",
			want: "hello",
		},
		{
			name: "replace_null_string",
			expr: "SELECT replace(NULL, 'a', 'b')",
			want: nil,
		},
		{
			name: "replace_null_pattern",
			expr: "SELECT replace('hello', NULL, 'x')",
			want: nil,
		},
		{
			name: "replace_null_replacement",
			expr: "SELECT replace('hello', 'l', NULL)",
			want: nil,
		},

		// trim(), ltrim(), rtrim() functions (func.test lines 1077-1144)
		{
			name: "trim_spaces",
			expr: "SELECT trim('  hello  ')",
			want: "hello",
		},
		{
			name: "ltrim_spaces",
			expr: "SELECT ltrim('  hello  ')",
			want: "hello  ",
		},
		{
			name: "rtrim_spaces",
			expr: "SELECT rtrim('  hello  ')",
			want: "  hello",
		},
		{
			name: "trim_custom_chars",
			expr: "SELECT trim('xyz', 'xyhelloxy')",
			want: "ello",
		},
		{
			name: "ltrim_custom_chars",
			expr: "SELECT ltrim('xyz', 'xyhelloxy')",
			want: "elloxy",
		},
		{
			name: "rtrim_custom_chars",
			expr: "SELECT rtrim('xyz', 'xyhelloxy')",
			want: "xyhello",
		},
		{
			name: "trim_null",
			expr: "SELECT trim(NULL)",
			want: nil,
		},

		// hex() function tests (func.test lines 519-545)
		{
			name: "hex_basic",
			expr: "SELECT hex('abc')",
			want: "616263",
		},
		{
			name: "hex_empty",
			expr: "SELECT hex('')",
			want: "",
		},
		{
			name: "hex_null",
			expr: "SELECT hex(NULL)",
			want: nil,
		},

		// quote() function tests (func.test lines 785-801)
		{
			name: "quote_string",
			expr: "SELECT quote('hello')",
			want: "'hello'",
		},
		{
			name: "quote_number",
			expr: "SELECT quote(42)",
			want: "42",
		},
		{
			name: "quote_null",
			expr: "SELECT quote(NULL)",
			want: "NULL",
		},
		{
			name: "quote_with_quotes",
			expr: "SELECT quote('it''s')",
			want: "'it''s'",
		},

		// min() and max() scalar functions
		{
			name: "min_two_args",
			expr: "SELECT min(1, 2)",
			want: int64(1),
		},
		{
			name: "min_three_args",
			expr: "SELECT min(3, 1, 2)",
			want: int64(1),
		},
		{
			name: "max_two_args",
			expr: "SELECT max(1, 2)",
			want: int64(2),
		},
		{
			name: "max_three_args",
			expr: "SELECT max(3, 1, 2)",
			want: int64(3),
		},
		{
			name: "min_with_null",
			expr: "SELECT min(1, NULL, 2)",
			want: int64(1),
		},
		{
			name: "max_with_null",
			expr: "SELECT max(1, NULL, 2)",
			want: int64(2),
		},

		// random() function tests (func.test lines 488-513)
		{
			name: "random_not_null",
			expr: "SELECT random() IS NOT NULL",
			want: int64(1),
		},
		{
			name: "random_typeof",
			expr: "SELECT typeof(random())",
			want: "integer",
		},

		// instr() function tests
		{
			name: "instr_found",
			expr: "SELECT instr('hello world', 'world')",
			want: int64(7),
		},
		{
			name: "instr_not_found",
			expr: "SELECT instr('hello', 'xyz')",
			want: int64(0),
		},
		{
			name: "instr_empty_needle",
			expr: "SELECT instr('hello', '')",
			want: int64(1),
		},
		{
			name: "instr_null",
			expr: "SELECT instr(NULL, 'test')",
			want: nil,
		},

		// unicode() and char() functions (func.test lines 1403-1425)
		{
			name: "unicode_char",
			expr: "SELECT unicode('A')",
			want: int64(65),
		},
		{
			name: "unicode_dollar",
			expr: "SELECT unicode('$')",
			want: int64(36),
		},
		{
			name: "char_single",
			expr: "SELECT char(65)",
			want: "A",
		},
		{
			name: "char_multiple",
			expr: "SELECT char(65, 66, 67)",
			want: "ABC",
		},
		{
			name: "char_empty",
			expr: "SELECT char()",
			want: "",
		},

		// printf() function tests
		{
			name: "printf_string",
			expr: "SELECT printf('Hello %s', 'World')",
			want: "Hello World",
		},
		{
			name: "printf_integer",
			expr: "SELECT printf('Number: %d', 42)",
			want: "Number: 42",
		},
		{
			name: "printf_float",
			expr: "SELECT printf('Pi: %.2f', 3.14159)",
			want: "Pi: 3.14",
		},

		// Date/time functions (basic tests)
		{
			name: "date_now",
			expr: "SELECT date('now') IS NOT NULL",
			want: int64(1),
		},
		{
			name: "time_now",
			expr: "SELECT time('now') IS NOT NULL",
			want: int64(1),
		},
		{
			name: "datetime_now",
			expr: "SELECT datetime('now') IS NOT NULL",
			want: int64(1),
		},

		// likelihood(), likely(), unlikely() functions (func3.test lines 76-198)
		{
			name: "likely_passthrough",
			expr: "SELECT likely(42)",
			want: int64(42),
		},
		{
			name: "unlikely_passthrough",
			expr: "SELECT unlikely(42)",
			want: int64(42),
		},
		{
			name: "likelihood_passthrough",
			expr: "SELECT likelihood(42, 0.5)",
			want: int64(42),
		},
		{
			name: "likely_null",
			expr: "SELECT likely(NULL)",
			want: nil,
		},
		{
			name: "unlikely_null",
			expr: "SELECT unlikely(NULL)",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				// NULL values might return sql.ErrNoRows in some cases
				if err == sql.ErrNoRows && tt.want == nil {
					return
				}
				t.Fatalf("query failed: %v", err)
			}

			// Handle NULL comparison
			if tt.want == nil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			// Handle different numeric types
			switch expected := tt.want.(type) {
			case int64:
				switch got := result.(type) {
				case int64:
					if got != expected {
						t.Errorf("expected %v, got %v", expected, got)
					}
				case float64:
					if int64(got) != expected {
						t.Errorf("expected %v, got %v (converted from float)", expected, got)
					}
				default:
					t.Errorf("expected int64 %v, got %T %v", expected, result, result)
				}
			case float64:
				switch got := result.(type) {
				case float64:
					if math.Abs(got-expected) > 0.001 {
						t.Errorf("expected %v, got %v", expected, got)
					}
				case int64:
					if math.Abs(float64(got)-expected) > 0.001 {
						t.Errorf("expected %v, got %v (converted from int)", expected, got)
					}
				default:
					t.Errorf("expected float64 %v, got %T %v", expected, result, result)
				}
			case string:
				got, ok := result.(string)
				if !ok {
					// Try converting bytes to string
					if bytes, ok := result.([]byte); ok {
						got = string(bytes)
					} else {
						t.Errorf("expected string %v, got %T %v", expected, result, result)
						return
					}
				}
				if got != expected {
					t.Errorf("expected %q, got %q", expected, got)
				}
			default:
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

// TestAggregateFunctions tests SQLite aggregate functions
func TestAggregateFunctions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "agg_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE numbers(value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Insert test data
	values := []int{1, 2, 3, 4, 5}
	for _, v := range values {
		_, err = db.Exec("INSERT INTO numbers(value) VALUES(?)", v)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	tests := []struct {
		name    string
		expr    string
		want    interface{}
		wantErr bool
	}{
		{
			name: "count_all",
			expr: "SELECT count(*) FROM numbers",
			want: int64(5),
		},
		{
			name: "count_column",
			expr: "SELECT count(value) FROM numbers",
			want: int64(5),
		},
		{
			name: "sum_integers",
			expr: "SELECT sum(value) FROM numbers",
			want: int64(15),
		},
		{
			name: "avg_integers",
			expr: "SELECT avg(value) FROM numbers",
			want: 3.0,
		},
		{
			name: "min_aggregate",
			expr: "SELECT min(value) FROM numbers",
			want: int64(1),
		},
		{
			name: "max_aggregate",
			expr: "SELECT max(value) FROM numbers",
			want: int64(5),
		},
		{
			name: "total_function",
			expr: "SELECT total(value) FROM numbers",
			want: 15.0,
		},
		{
			name: "group_concat_default",
			expr: "SELECT group_concat(value) FROM numbers",
			want: "1,2,3,4,5",
		},
		{
			name: "group_concat_custom_sep",
			expr: "SELECT group_concat(value, ' ') FROM numbers",
			want: "1 2 3 4 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			// Handle different numeric types
			switch expected := tt.want.(type) {
			case int64:
				switch got := result.(type) {
				case int64:
					if got != expected {
						t.Errorf("expected %v, got %v", expected, got)
					}
				case float64:
					if int64(got) != expected {
						t.Errorf("expected %v, got %v (converted from float)", expected, got)
					}
				default:
					t.Errorf("expected int64 %v, got %T %v", expected, result, result)
				}
			case float64:
				got, ok := result.(float64)
				if !ok {
					if intVal, ok := result.(int64); ok {
						got = float64(intVal)
					} else {
						t.Errorf("expected float64 %v, got %T %v", expected, result, result)
						return
					}
				}
				if math.Abs(got-expected) > 0.001 {
					t.Errorf("expected %v, got %v", expected, got)
				}
			case string:
				got, ok := result.(string)
				if !ok {
					if bytes, ok := result.([]byte); ok {
						got = string(bytes)
					} else {
						t.Errorf("expected string %v, got %T %v", expected, result, result)
						return
					}
				}
				if got != expected {
					t.Errorf("expected %q, got %q", expected, got)
				}
			default:
				if result != tt.want {
					t.Errorf("expected %v, got %v", tt.want, result)
				}
			}
		})
	}
}

// TestFunctionErrorCases tests error conditions for various functions
func TestFunctionErrorCases(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "error_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name string
		expr string
	}{
		{
			name: "length_wrong_args",
			expr: "SELECT length()",
		},
		{
			name: "length_too_many_args",
			expr: "SELECT length('a', 'b')",
		},
		{
			name: "substr_wrong_args",
			expr: "SELECT substr()",
		},
		{
			name: "substr_too_many_args",
			expr: "SELECT substr('a', 1, 2, 3)",
		},
		{
			name: "abs_wrong_args",
			expr: "SELECT abs()",
		},
		{
			name: "abs_too_many_args",
			expr: "SELECT abs(1, 2)",
		},
		{
			name: "round_wrong_args",
			expr: "SELECT round()",
		},
		{
			name: "round_too_many_args",
			expr: "SELECT round(1.5, 2, 3)",
		},
		{
			name: "replace_wrong_args",
			expr: "SELECT replace('a', 'b')",
		},
		{
			name: "replace_too_many_args",
			expr: "SELECT replace('a', 'b', 'c', 'd')",
		},
		{
			name: "trim_too_many_args",
			expr: "SELECT trim('a', 'b', 'c')",
		},
		{
			name: "coalesce_no_args",
			expr: "SELECT coalesce()",
		},
		{
			name: "coalesce_one_arg",
			expr: "SELECT coalesce(1)",
		},
		{
			name: "likelihood_wrong_args",
			expr: "SELECT likelihood(1)",
		},
		{
			name: "likelihood_invalid_probability",
			expr: "SELECT likelihood(1, 1.5)",
		},
		{
			name: "likelihood_negative_probability",
			expr: "SELECT likelihood(1, -0.5)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)
			if err == nil {
				t.Errorf("expected error for %q but got result: %v", tt.expr, result)
			}
		})
	}
}

// TestNullHandling tests how functions handle NULL values
func TestNullHandling(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "null_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE nulltest(a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	_, err = db.Exec("INSERT INTO nulltest VALUES(NULL, NULL)")
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	tests := []struct {
		name    string
		expr    string
		wantNil bool
	}{
		{"length_null", "SELECT length(a) FROM nulltest", true},
		{"substr_null", "SELECT substr(b, 1, 2) FROM nulltest", true},
		{"lower_null", "SELECT lower(b) FROM nulltest", true},
		{"upper_null", "SELECT upper(b) FROM nulltest", true},
		{"abs_null", "SELECT abs(a) FROM nulltest", true},
		{"round_null", "SELECT round(a) FROM nulltest", true},
		{"typeof_null", "SELECT typeof(a) FROM nulltest", false}, // Returns "null"
		{"trim_null", "SELECT trim(b) FROM nulltest", true},
		{"hex_null", "SELECT hex(b) FROM nulltest", true},
		{"quote_null", "SELECT quote(a) FROM nulltest", false}, // Returns "NULL"
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.expr).Scan(&result)
			if err != nil {
				if err != sql.ErrNoRows {
					t.Fatalf("query failed: %v", err)
				}
			}

			if tt.wantNil && result != nil {
				t.Errorf("expected nil, got %v", result)
			}
		})
	}
}
