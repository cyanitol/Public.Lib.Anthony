// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"testing"
)

// strftimeTestCase holds a single strftime format-specifier test.
type strftimeTestCase struct {
	name string
	expr string
	want interface{}
	skip string
}

// generateStrftimeTests builds combinatorial tests for %w, %u, %W, %j.
func generateStrftimeTests() []strftimeTestCase {
	type dateExpect struct {
		date string
		dow  string // day name for documentation
		w    string // %w  weekday 0-6, Sunday=0
		u    string // %u  weekday 1-7, Monday=1
		ww   string // %W  week number 00-53
		j    string // %j  day of year 001-366
	}

	dates := []dateExpect{
		{"2024-01-01", "Monday", "1", "1", "01", "001"},
		{"2024-03-15", "Friday", "5", "5", "11", "075"},
		{"2024-07-04", "Thursday", "4", "4", "27", "186"},
		{"2024-12-25", "Wednesday", "3", "3", "52", "360"},
		{"2023-01-01", "Sunday", "0", "7", "00", "001"},
	}

	specs := []struct {
		code  string
		label string
		field func(dateExpect) string
	}{
		{"%w", "weekday_0_6", func(d dateExpect) string { return d.w }},
		{"%u", "weekday_1_7", func(d dateExpect) string { return d.u }},
		{"%W", "week_number", func(d dateExpect) string { return d.ww }},
		{"%j", "day_of_year", func(d dateExpect) string { return d.j }},
	}

	skip := "strftime handlers for %w/%u/%W/%j not yet wired into strftimeHandlers map"

	var cases []strftimeTestCase
	for _, d := range dates {
		for _, s := range specs {
			cases = append(cases, strftimeTestCase{
				name: s.label + "_" + d.date,
				expr: "SELECT strftime('" + s.code + "', '" + d.date + "')",
				want: s.field(d),
				skip: skip,
			})
		}
	}
	return cases
}

// TestFunctionsGenStrftime tests strftime %w, %u, %W, %j specifiers.
func TestFunctionsGenStrftime(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	for _, tc := range generateStrftimeTests() {
		t.Run(tc.name, func(t *testing.T) {
			runStrftimeCase(t, db, tc)
		})
	}
}

// runStrftimeCase executes a single strftime test case.
func runStrftimeCase(t *testing.T, db *sql.DB, tc strftimeTestCase) {
	t.Helper()
	if tc.skip != "" {
		t.Skip(tc.skip)
	}

	var result interface{}
	err := db.QueryRow(tc.expr).Scan(&result)
	if err != nil {
		t.Fatalf("query %q failed: %v", tc.expr, err)
	}

	got := normalizeStrftimeResult(result)
	if got != tc.want {
		t.Errorf("got %q, want %q", got, tc.want)
	}
}

// normalizeStrftimeResult converts a scanned value to a string.
func normalizeStrftimeResult(v interface{}) string {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case int64:
		return formatInt64(val)
	case float64:
		return formatFloat64(val)
	default:
		return ""
	}
}

// ---------- NTH_VALUE window function tests ----------

// nthValueTestCase describes one NTH_VALUE test scenario.
type nthValueTestCase struct {
	name  string
	setup []string
	query string
	want  string
	skip  string
}

// generateNthValueTests builds NTH_VALUE test cases.
func generateNthValueTests() []nthValueTestCase {
	setup := []string{
		"CREATE TABLE nv(id INTEGER, val TEXT)",
		"INSERT INTO nv VALUES(1, 'a')",
		"INSERT INTO nv VALUES(2, 'b')",
		"INSERT INTO nv VALUES(3, 'c')",
	}

	return []nthValueTestCase{
		{
			name:  "nth_value_1_is_first",
			setup: setup,
			query: "SELECT COALESCE(NTH_VALUE(val, 1) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 'NULL') FROM nv",
			want:  "a a a",
			skip:  "window function NTH_VALUE not yet fully supported",
		},
		{
			name:  "nth_value_2_is_second",
			setup: setup,
			query: "SELECT COALESCE(NTH_VALUE(val, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 'NULL') FROM nv",
			want:  "b b b",
			skip:  "window function NTH_VALUE not yet fully supported",
		},
		{
			name:  "nth_value_beyond_frame",
			setup: setup,
			query: "SELECT COALESCE(NTH_VALUE(val, 10) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 'NULL') FROM nv",
			want:  "NULL NULL NULL",
			skip:  "window function NTH_VALUE not yet fully supported",
		},
		{
			name: "nth_value_partition_by",
			setup: []string{
				"CREATE TABLE nvp(grp TEXT, id INTEGER, val TEXT)",
				"INSERT INTO nvp VALUES('x', 1, 'a')",
				"INSERT INTO nvp VALUES('x', 2, 'b')",
				"INSERT INTO nvp VALUES('y', 1, 'c')",
				"INSERT INTO nvp VALUES('y', 2, 'd')",
			},
			query: "SELECT COALESCE(NTH_VALUE(val, 2) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 'NULL') FROM nvp ORDER BY grp, id",
			want:  "b b d d",
			skip:  "window function NTH_VALUE not yet fully supported",
		},
	}
}

// TestFunctionsGenNthValue tests NTH_VALUE window function.
func TestFunctionsGenNthValue(t *testing.T) {
	for _, tc := range generateNthValueTests() {
		t.Run(tc.name, func(t *testing.T) {
			runNthValueCase(t, tc)
		})
	}
}

// runNthValueCase executes one NTH_VALUE test.
func runNthValueCase(t *testing.T, tc nthValueTestCase) {
	t.Helper()
	if tc.skip != "" {
		t.Skip(tc.skip)
	}

	db := setupMemoryDB(t)
	defer db.Close()

	execAllSetup(t, db, tc.setup)

	got := collectSpaceSep(t, db, tc.query)
	if got != tc.want {
		t.Errorf("got %q, want %q", got, tc.want)
	}
}

// ---------- Named WINDOW clause tests ----------

// windowClauseTestCase holds a named-window test.
type windowClauseTestCase struct {
	name  string
	setup []string
	query string
	want  string
	skip  string
}

// generateWindowClauseTests builds WINDOW clause test cases.
func generateWindowClauseTests() []windowClauseTestCase {
	setup := []string{
		"CREATE TABLE wc(id INTEGER, val INTEGER)",
		"INSERT INTO wc VALUES(1, 10)",
		"INSERT INTO wc VALUES(2, 20)",
		"INSERT INTO wc VALUES(3, 30)",
	}

	return []windowClauseTestCase{
		{
			name:  "named_window_row_number",
			setup: setup,
			query: "SELECT val, ROW_NUMBER() OVER w FROM wc WINDOW w AS (ORDER BY val)",
			want:  "10|1 20|2 30|3",
			skip:  "named WINDOW clause not yet supported in parser/compiler",
		},
		{
			name:  "multiple_named_windows",
			setup: setup,
			query: "SELECT val, ROW_NUMBER() OVER w1, SUM(val) OVER w2 FROM wc WINDOW w1 AS (ORDER BY val), w2 AS (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
			want:  "10|1|10 20|2|30 30|3|60",
			skip:  "named WINDOW clause not yet supported in parser/compiler",
		},
	}
}

// TestFunctionsGenWindowClause tests named WINDOW clause.
func TestFunctionsGenWindowClause(t *testing.T) {
	for _, tc := range generateWindowClauseTests() {
		t.Run(tc.name, func(t *testing.T) {
			runWindowClauseCase(t, tc)
		})
	}
}

// runWindowClauseCase executes one named-window test.
func runWindowClauseCase(t *testing.T, tc windowClauseTestCase) {
	t.Helper()
	if tc.skip != "" {
		t.Skip(tc.skip)
	}

	db := setupMemoryDB(t)
	defer db.Close()

	execAllSetup(t, db, tc.setup)

	got := collectPipeSep(t, db, tc.query)
	if got != tc.want {
		t.Errorf("got %q, want %q", got, tc.want)
	}
}

// ---------- json_each table-valued function tests ----------

// jsonTVFTestCase holds a json_each / json_tree test.
type jsonTVFTestCase struct {
	name  string
	query string
	want  string
	skip  string
}

// generateJsonEachTests builds json_each test cases.
func generateJsonEachTests() []jsonTVFTestCase {
	return []jsonTVFTestCase{
		{
			name:  "json_each_array",
			query: "SELECT value FROM json_each('[1,2,3]')",
			want:  "1 2 3",
			skip:  "json_each table-valued function not yet registered as virtual table",
		},
		{
			name:  "json_each_object",
			query: "SELECT key, value FROM json_each('{\"a\":1,\"b\":2}')",
			want:  "a|1 b|2",
			skip:  "json_each table-valued function not yet registered as virtual table",
		},
		{
			name:  "json_each_with_path",
			query: "SELECT value FROM json_each('{\"x\":[10,20]}', '$.x')",
			want:  "10 20",
			skip:  "json_each table-valued function not yet registered as virtual table",
		},
	}
}

// generateJsonTreeTests builds json_tree test cases.
func generateJsonTreeTests() []jsonTVFTestCase {
	return []jsonTVFTestCase{
		{
			name:  "json_tree_nested_object",
			query: "SELECT key, value, type FROM json_tree('{\"a\":{\"b\":1}}')",
			want:  "NULL|{\"a\":{\"b\":1}}|object a|{\"b\":1}|object b|1|integer",
			skip:  "json_tree table-valued function not yet registered as virtual table",
		},
		{
			name:  "json_tree_nested_array",
			query: "SELECT key, value, type FROM json_tree('{\"arr\":[1,2]}')",
			want:  "NULL|{\"arr\":[1,2]}|object arr|[1,2]|array 0|1|integer 1|2|integer",
			skip:  "json_tree table-valued function not yet registered as virtual table",
		},
	}
}

// TestFunctionsGenJsonEach tests json_each table-valued function.
func TestFunctionsGenJsonEach(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	for _, tc := range generateJsonEachTests() {
		t.Run(tc.name, func(t *testing.T) {
			runJsonTVFCase(t, db, tc)
		})
	}
}

// TestFunctionsGenJsonTree tests json_tree table-valued function.
func TestFunctionsGenJsonTree(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	for _, tc := range generateJsonTreeTests() {
		t.Run(tc.name, func(t *testing.T) {
			runJsonTVFCase(t, db, tc)
		})
	}
}

// runJsonTVFCase executes one json_each or json_tree test.
func runJsonTVFCase(t *testing.T, db *sql.DB, tc jsonTVFTestCase) {
	t.Helper()
	if tc.skip != "" {
		t.Skip(tc.skip)
	}

	got := collectPipeSep(t, db, tc.query)
	if got != tc.want {
		t.Errorf("got %q, want %q", got, tc.want)
	}
}

// ---------- shared helpers ----------

// execAllSetup runs all setup SQL statements.
func execAllSetup(t *testing.T, db *sql.DB, stmts []string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q failed: %v", s, err)
		}
	}
}

// collectSpaceSep queries and joins single-column results with spaces.
func collectSpaceSep(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()

	return joinRows(t, rows, " ", false)
}

// collectPipeSep queries and joins multi-column results with pipes,
// rows with spaces.
func collectPipeSep(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	defer rows.Close()

	return joinRows(t, rows, " ", true)
}

// joinRows scans all rows and joins them. When multi is true each row's
// columns are pipe-separated; rows are joined by sep.
func joinRows(t *testing.T, rows *sql.Rows, sep string, multi bool) string {
	t.Helper()
	cols := mustGetColumns(t, rows)
	var parts []string

	for rows.Next() {
		vals := scanRowValues(t, rows, len(cols))
		rowStr := buildRowString(vals, multi)
		parts = append(parts, rowStr)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return joinStrings(parts, sep)
}

// scanRowValues scans one row of n columns into interface values.
func scanRowValues(t *testing.T, rows *sql.Rows, n int) []interface{} {
	t.Helper()
	vals := make([]interface{}, n)
	ptrs := make([]interface{}, n)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	return vals
}

// buildRowString converts scanned values to a display string.
func buildRowString(vals []interface{}, multi bool) string {
	if !multi {
		return formatValue(vals[0])
	}
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = formatValue(v)
	}
	return joinStrings(strs, "|")
}

// joinStrings joins with a separator without importing strings.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}
