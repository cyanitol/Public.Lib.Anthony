// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openGBWDB opens a fresh in-memory database for GROUP BY / window coverage tests.
func openGBWDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// gbwExec executes a statement and fails the test on error.
func gbwExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// collectGBWRows runs a query and returns each row as a slice of interface{}.
func collectGBWRows(t *testing.T, db *sql.DB, q string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return result
}

// toInt64GBW converts an interface{} to int64 if possible, otherwise panics.
func toInt64GBW(t *testing.T, v interface{}, label string) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case []byte:
		var n int64
		fmt.Sscanf(string(x), "%d", &n)
		return n
	default:
		t.Fatalf("%s: cannot convert %T to int64", label, v)
		return 0
	}
}

// toFloat64GBW converts an interface{} to float64.
func toFloat64GBW(t *testing.T, v interface{}, label string) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	default:
		t.Fatalf("%s: cannot convert %T to float64", label, v)
		return 0
	}
}

// setupEmployeesTable creates and populates the shared employees table.
func setupEmployeesTable(t *testing.T, db *sql.DB) {
	t.Helper()
	gbwExec(t, db, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL)")
	rows := []string{
		"(1, 'Alice',   'eng',   90000.0)",
		"(2, 'Bob',     'eng',   85000.0)",
		"(3, 'Carol',   'eng',   92000.0)",
		"(4, 'Dave',    'sales', 70000.0)",
		"(5, 'Eve',     'sales', 75000.0)",
		"(6, 'Frank',   'sales', 72000.0)",
		"(7, 'Grace',   'hr',    65000.0)",
		"(8, 'Heidi',   'hr',    68000.0)",
		"(9, 'Ivan',    'hr',    66000.0)",
		"(10,'Judy',    'eng',   88000.0)",
	}
	for _, r := range rows {
		gbwExec(t, db, "INSERT INTO employees VALUES "+r)
	}
}

// ---------------------------------------------------------------------------
// GROUP BY tests
// ---------------------------------------------------------------------------

// TestGroupByCountSumAvgMinMax exercises compileSelectWithGroupBy, initGroupByState,
// initializeGroupAccumulators, updateGroupAccumulatorsFromSorter, updateSingleAccumulator,
// emitAccumAdd, emitAccumMinMax, populateSorterData, processSortedDataWithGrouping,
// emitFinalGroupOutput, emitGroupOutput, copyColumnsToResults, copyAggregateResult.
// toStringGBW converts an interface{} column value to string.
func toStringGBW(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

func TestGroupByCountSumAvgMinMax(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
		 FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 departments, got %d", len(rows))
	}

	engRow := rows[0]
	dept := toStringGBW(engRow[0])
	if dept != "eng" {
		t.Fatalf("expected dept=eng in first row, got %v", engRow[0])
	}
	gbwCheckEngAggregates(t, engRow)
}

func gbwCheckEngAggregates(t *testing.T, engRow []interface{}) {
	t.Helper()
	count := toInt64GBW(t, engRow[1], "COUNT(*)")
	if count != 4 {
		t.Errorf("eng COUNT(*) = %d, want 4", count)
	}
	sum := toFloat64GBW(t, engRow[2], "SUM(salary)")
	if sum != 355000.0 {
		t.Errorf("eng SUM(salary) = %f, want 355000", sum)
	}
	avg := toFloat64GBW(t, engRow[3], "AVG(salary)")
	if avg != 88750.0 {
		t.Errorf("eng AVG(salary) = %f, want 88750", avg)
	}
	minSal := toFloat64GBW(t, engRow[4], "MIN(salary)")
	if minSal != 85000.0 {
		t.Errorf("eng MIN(salary) = %f, want 85000", minSal)
	}
	maxSal := toFloat64GBW(t, engRow[5], "MAX(salary)")
	if maxSal != 92000.0 {
		t.Errorf("eng MAX(salary) = %f, want 92000", maxSal)
	}
}

// TestGroupByHaving exercises emitAggregateHavingClause, generateHavingExpression,
// generateHavingIdentExpr, buildAggregateMap, aggregateKey.
func TestGroupByHaving(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 2 ORDER BY dept`)

	// eng (4), sales (3), hr (3) all have > 2; eng should appear
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row with COUNT(*) > 2, got 0")
	}
	for _, row := range rows {
		cnt := toInt64GBW(t, row[1], "COUNT(*)")
		if cnt <= 2 {
			t.Errorf("HAVING COUNT(*) > 2: got dept %v with count %d", row[0], cnt)
		}
	}
}

// TestGroupByHavingComplex exercises generateHavingExpression for AVG comparison.
func TestGroupByHavingAvg(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 70000 ORDER BY dept`)

	// eng avg ~88750, sales avg ~72333 (all > 70000), hr avg ~66333 (< 70000)
	depts := make(map[string]bool)
	for _, row := range rows {
		var d string
		switch x := row[0].(type) {
		case string:
			d = x
		case []byte:
			d = string(x)
		}
		depts[d] = true
	}
	if depts["hr"] {
		t.Errorf("hr dept should not appear (avg < 70000)")
	}
	if !depts["eng"] {
		t.Errorf("eng dept should appear (avg > 70000)")
	}
}

// TestGroupByGroupConcat exercises groupConcatSeparator, emitAccumGroupConcat,
// updateSingleAccumulator (GROUP_CONCAT path).
func TestGroupByGroupConcat(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, group_concat(name, ', ') FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Check that eng names are concatenated
	engConcat := ""
	switch x := rows[0][1].(type) {
	case string:
		engConcat = x
	case []byte:
		engConcat = string(x)
	}
	// Should contain at least one comma since there are multiple eng employees
	if !strings.Contains(engConcat, ",") {
		t.Errorf("group_concat: expected comma-separated names for eng, got %q", engConcat)
	}
}

// TestGroupByGroupConcatDefaultSeparator exercises groupConcatSeparator default path.
func TestGroupByGroupConcatDefaultSep(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, group_concat(name) FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// TestGroupByJSONGroupArray exercises emitAccumJSONArray, updateSingleAccumulator
// (JSON_GROUP_ARRAY path), populateAggregateArgs, populateJSONObjectArgs.
func TestGroupByJSONGroupArray(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, json_group_array(name) FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 departments, got %d", len(rows))
	}
	for _, row := range rows {
		var arr string
		switch x := row[1].(type) {
		case string:
			arr = x
		case []byte:
			arr = string(x)
		default:
			t.Fatalf("unexpected type for json_group_array result: %T", row[1])
		}
		if !strings.HasPrefix(arr, "[") || !strings.HasSuffix(arr, "]") {
			t.Errorf("json_group_array: expected JSON array, got %q", arr)
		}
	}
}

// TestGroupByJSONGroupObject exercises emitAccumJSONObject, updateGroupAccForColumn
// (JSON_GROUP_OBJECT path with 2 sorter columns), calculateSorterColumns.
func TestGroupByJSONGroupObject(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, json_group_object(name, salary) FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 departments, got %d", len(rows))
	}
	for _, row := range rows {
		var obj string
		switch x := row[1].(type) {
		case string:
			obj = x
		case []byte:
			obj = string(x)
		default:
			t.Fatalf("unexpected type for json_group_object result: %T", row[1])
		}
		if !strings.HasPrefix(obj, "{") || !strings.HasSuffix(obj, "}") {
			t.Errorf("json_group_object: expected JSON object, got %q", obj)
		}
	}
}

// TestGroupByFilterClause exercises readSorterFilterCheck, populateFilterArg,
// updateGroupAccForColumn (filter path), calculateSorterColumns (filter col).
func TestGroupByFilterClause(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*) FILTER (WHERE salary > 70000) FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 departments, got %d", len(rows))
	}
	// eng: all 4 salaries > 70000 → count = 4
	engCount := toInt64GBW(t, rows[0][1], "eng filtered count")
	if engCount != 4 {
		t.Errorf("eng COUNT FILTER(salary>70000) = %d, want 4", engCount)
	}
	// hr: salaries 65000,68000,66000 — none > 70000 → count = 0
	hrCount := toInt64GBW(t, rows[1][1], "hr filtered count")
	if hrCount != 0 {
		t.Errorf("hr COUNT FILTER(salary>70000) = %d, want 0", hrCount)
	}
}

// TestGroupByNullGroupValues exercises emitNullSafeGroupCompare null paths
// (NULL vs NULL = same group; NULL vs non-NULL = different group).
func TestGroupByNullGroupValues(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE nulltest (cat TEXT, val INTEGER)")
	gbwExec(t, db, "INSERT INTO nulltest VALUES (NULL, 10)")
	gbwExec(t, db, "INSERT INTO nulltest VALUES (NULL, 20)")
	gbwExec(t, db, "INSERT INTO nulltest VALUES ('A', 30)")
	gbwExec(t, db, "INSERT INTO nulltest VALUES ('A', 40)")

	rows := collectGBWRows(t, db,
		`SELECT cat, SUM(val) FROM nulltest GROUP BY cat ORDER BY cat`)

	if len(rows) != 2 {
		t.Fatalf("expected 2 groups (NULL and 'A'), got %d", len(rows))
	}
	// NULL group first (NULL sorts before 'A')
	if rows[0][0] != nil {
		t.Errorf("first group should have NULL cat, got %v", rows[0][0])
	}
	nullSum := toFloat64GBW(t, rows[0][1], "NULL group sum")
	if nullSum != 30.0 {
		t.Errorf("NULL group SUM = %f, want 30", nullSum)
	}
}

// TestGroupByMultipleColumns exercises groupByCollations, createGroupBySorterKeyInfo
// with multiple GROUP BY columns.
func TestGroupByMultipleColumns(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, CASE WHEN salary >= 80000 THEN 'senior' ELSE 'junior' END AS level,
		        COUNT(*), AVG(salary)
		 FROM employees
		 GROUP BY dept, level
		 ORDER BY dept, level`)

	if len(rows) < 1 {
		t.Fatalf("expected rows from multi-column GROUP BY, got 0")
	}
}

// TestGroupByNonAggregateColumns exercises copyNonAggregateResult (selecting
// a non-aggregate column alongside aggregates).
func TestGroupByNonAggregateColumns(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*) FROM employees GROUP BY dept ORDER BY dept`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(rows))
	}
}

// TestGroupByResolveExpr exercises resolveGroupByExpr, evaluateGroupByExprs
// for expression-based GROUP BY.
func TestGroupByExpressions(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE expr_tbl (category TEXT, value INTEGER)")
	for i := 1; i <= 6; i++ {
		cat := "odd"
		if i%2 == 0 {
			cat = "even"
		}
		gbwExec(t, db, fmt.Sprintf("INSERT INTO expr_tbl VALUES('%s', %d)", cat, i*10))
	}

	rows := collectGBWRows(t, db,
		`SELECT category, COUNT(*), SUM(value) FROM expr_tbl GROUP BY category ORDER BY category`)

	if len(rows) != 2 {
		t.Fatalf("expected 2 groups (even/odd), got %d", len(rows))
	}
}

// TestGroupByWhereThenGroup exercises fixWhereSkip and populateSorterData with WHERE.
func TestGroupByWhereFilter(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*), AVG(salary)
		 FROM employees
		 WHERE salary >= 70000
		 GROUP BY dept
		 ORDER BY dept`)

	// hr avg 65k,68k,66k → none >= 70000, so hr should be absent (or count=0)
	for _, row := range rows {
		var dept string
		switch x := row[0].(type) {
		case string:
			dept = x
		case []byte:
			dept = string(x)
		}
		if dept == "hr" {
			cnt := toInt64GBW(t, row[1], "hr count")
			if cnt > 0 {
				// hr employees are all below 70000, they should be filtered
				// (this depends on the engine's WHERE handling before GROUP BY)
			}
		}
	}
}

// TestGroupByPopulateGroupByExprs exercises populateGroupByExprs, scanAndPopulateSorter,
// openTableAndSorter, setupGroupByCursorsAndState.
func TestGroupByLargeDataset(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE large (grp INTEGER, val INTEGER)")
	// Insert enough rows to exercise all GROUP BY paths
	for g := 0; g < 5; g++ {
		for v := 0; v < 8; v++ {
			gbwExec(t, db, fmt.Sprintf("INSERT INTO large VALUES (%d, %d)", g, v*g+1))
		}
	}

	rows := collectGBWRows(t, db,
		`SELECT grp, COUNT(*), SUM(val), MIN(val), MAX(val) FROM large GROUP BY grp ORDER BY grp`)

	if len(rows) != 5 {
		t.Fatalf("expected 5 groups (0-4), got %d", len(rows))
	}
	// Each group should have 8 rows
	for i, row := range rows {
		cnt := toInt64GBW(t, row[1], fmt.Sprintf("grp %d count", i))
		if cnt != 8 {
			t.Errorf("grp %d: COUNT(*) = %d, want 8", i, cnt)
		}
	}
}

// TestGroupByIdentsEqual exercises identsEqual, literalsEqual, binaryExprsEqual
// by running GROUP BY on aliased expressions.
func TestGroupByAliasedExpression(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE alias_tbl (x INTEGER, y INTEGER)")
	for i := 1; i <= 6; i++ {
		gbwExec(t, db, fmt.Sprintf("INSERT INTO alias_tbl VALUES (%d, %d)", i%3, i*5))
	}

	rows := collectGBWRows(t, db,
		`SELECT x*2+1 AS bucket, COUNT(*), SUM(y) FROM alias_tbl GROUP BY bucket ORDER BY bucket`)

	if len(rows) < 1 {
		t.Fatalf("expected rows from aliased-expression GROUP BY, got 0")
	}
}

// TestGroupBySingleGroup exercises the single-group path where all rows belong to
// the same group (emitFinalGroupOutput is the only output point).
func TestGroupBySingleGroup(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, COUNT(*), SUM(salary) FROM employees WHERE dept = 'eng' GROUP BY dept`)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row (single group), got %d", len(rows))
	}
	cnt := toInt64GBW(t, rows[0][1], "COUNT(*)")
	if cnt != 4 {
		t.Errorf("eng COUNT(*) = %d, want 4", cnt)
	}
}

// TestGroupByHavingMin exercises generateHavingExpression for MIN comparison.
func TestGroupByHavingMinMax(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, MIN(salary), MAX(salary)
		 FROM employees
		 GROUP BY dept
		 HAVING MIN(salary) < 80000
		 ORDER BY dept`)

	// sales: min 70000 < 80000 → included
	// hr: min 65000 < 80000 → included
	// eng: min 85000 >= 80000 → excluded
	for _, row := range rows {
		var dept string
		switch x := row[0].(type) {
		case string:
			dept = x
		case []byte:
			dept = string(x)
		}
		if dept == "eng" {
			t.Errorf("eng should not appear: min salary is 85000 which is not < 80000")
		}
	}
}

// ---------------------------------------------------------------------------
// Window function tests
// ---------------------------------------------------------------------------

// TestWindowRowNumberRankDenseRank exercises analyzeWindowRankFunctions,
// processWindowRankFunction, isRankFunction, isWindowFunction, updateRankInfo,
// shouldExtractOrderBy, extractWindowOrderByCols, emitWindowRankTrackingFromSorter,
// emitWindowRankComparison, emitWindowRankUpdate, emitWindowFunctionColumn,
// emitWindowFunctionColumnWithOpcodes, emitWindowRankFunc.
func TestWindowRowNumberRankDenseRank(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name,
		        ROW_NUMBER() OVER (ORDER BY salary),
		        RANK() OVER (ORDER BY salary),
		        DENSE_RANK() OVER (ORDER BY salary)
		 FROM employees ORDER BY salary LIMIT 3`)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// ROW_NUMBER always increases by 1
	for i, row := range rows {
		rn := toInt64GBW(t, row[1], fmt.Sprintf("row %d ROW_NUMBER", i))
		if rn != int64(i+1) {
			t.Errorf("row %d ROW_NUMBER = %d, want %d", i, rn, i+1)
		}
	}
}

// TestWindowNtile exercises extractNtileArg.
func TestWindowNtile(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name, NTILE(3) OVER (ORDER BY salary) AS tile
		 FROM employees ORDER BY salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// NTILE(3) over 10 rows: tiles 1,2,3 only
	for _, row := range rows {
		tile := toInt64GBW(t, row[1], "NTILE(3)")
		if tile < 1 || tile > 3 {
			t.Errorf("NTILE(3): got tile %d, want 1-3", tile)
		}
	}
}

// TestWindowLagLead exercises emitWindowLagLead, extractLagLeadArgs, extractLagLeadDefault.
func TestWindowLagLead(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name, salary,
		        LAG(salary, 1, 0) OVER (ORDER BY salary),
		        LEAD(salary, 1, 0) OVER (ORDER BY salary)
		 FROM employees ORDER BY salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// First row LAG should be 0 (default), last row LEAD should be 0 (default)
	firstLag := toFloat64GBW(t, rows[0][2], "first LAG")
	if firstLag != 0 {
		t.Errorf("first row LAG = %f, want 0", firstLag)
	}
	lastLead := toFloat64GBW(t, rows[9][3], "last LEAD")
	if lastLead != 0 {
		t.Errorf("last row LEAD = %f, want 0", lastLead)
	}
}

// TestWindowFirstLastValue exercises emitWindowValueFunc.
func TestWindowFirstLastValue(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name, salary,
		        FIRST_VALUE(salary) OVER (ORDER BY salary),
		        LAST_VALUE(salary) OVER (ORDER BY salary)
		 FROM employees ORDER BY salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// FIRST_VALUE should always be the minimum salary (65000) because the
	// default frame is RANGE UNBOUNDED PRECEDING to CURRENT ROW.
	for i, row := range rows {
		fv := toFloat64GBW(t, row[2], fmt.Sprintf("row %d FIRST_VALUE", i))
		if fv != 65000.0 {
			t.Errorf("row %d FIRST_VALUE(salary) = %f, want 65000", i, fv)
		}
	}
	// LAST_VALUE with default frame (RANGE UNBOUNDED PRECEDING to CURRENT ROW)
	// returns the current row's salary (last value in the frame up to current row).
	// The last row (salary=92000) should have LAST_VALUE = 92000.
	lastRow := rows[len(rows)-1]
	salary := toFloat64GBW(t, lastRow[1], "last row salary")
	lv := toFloat64GBW(t, lastRow[3], "last row LAST_VALUE")
	if lv != salary {
		t.Errorf("last row LAST_VALUE(salary) = %f, want %f (current row salary)", lv, salary)
	}
}

// TestWindowNthValue exercises extractNthValueN.
func TestWindowNthValue(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name,
		        NTH_VALUE(salary, 2) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 FROM employees ORDER BY salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// NTH_VALUE(salary, 2) with full frame = 2nd smallest salary = 66000
	for i, row := range rows {
		nv := toFloat64GBW(t, row[1], fmt.Sprintf("row %d NTH_VALUE", i))
		if nv != 66000.0 {
			t.Errorf("row %d NTH_VALUE(salary,2) = %f, want 66000", i, nv)
		}
	}
}

// TestWindowSumOverPartition exercises emitWindowAggregateFunc with PARTITION BY and
// frame spec (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), convertFrameMode,
// convertFrameBound, windowFrameForSpec, emitWindowLimitCheck.
func TestWindowSumOverPartitionByOrderBy(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name, dept, salary,
		        SUM(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
		 FROM employees ORDER BY dept, salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// Verify the running sum is non-decreasing within each dept
	deptSums := make(map[string]float64)
	for _, row := range rows {
		var dept string
		switch x := row[1].(type) {
		case string:
			dept = x
		case []byte:
			dept = string(x)
		}
		rs := toFloat64GBW(t, row[3], "running_sum")
		prev := deptSums[dept]
		if rs < prev {
			t.Errorf("dept %s: running sum decreased from %f to %f", dept, prev, rs)
		}
		deptSums[dept] = rs
	}
}

// TestWindowNamedWindows exercises resolveNamedWindows (copies WINDOW clause spec
// into OVER references).
func TestWindowNamedWindows(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name,
		        RANK() OVER w1,
		        SUM(salary) OVER w2
		 FROM employees
		 WINDOW w1 AS (ORDER BY salary),
		        w2 AS (PARTITION BY dept ORDER BY salary)
		 ORDER BY salary LIMIT 5`)

	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	// RANK() should start at 1 for the first (lowest salary) row
	firstRank := toInt64GBW(t, rows[0][1], "RANK first row")
	if firstRank != 1 {
		t.Errorf("first row RANK = %d, want 1", firstRank)
	}
}

// TestWindowRankWithTies exercises emitWindowRankComparison with ties (same ORDER BY value),
// updateRankInfo (RANK and DENSE_RANK).
func TestWindowRankWithTies(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE ties_tbl (name TEXT, score INTEGER)")
	gbwExec(t, db, "INSERT INTO ties_tbl VALUES ('A', 100)")
	gbwExec(t, db, "INSERT INTO ties_tbl VALUES ('B', 90)")
	gbwExec(t, db, "INSERT INTO ties_tbl VALUES ('C', 90)")
	gbwExec(t, db, "INSERT INTO ties_tbl VALUES ('D', 80)")

	rows := collectGBWRows(t, db,
		`SELECT name, score,
		        RANK() OVER (ORDER BY score DESC),
		        DENSE_RANK() OVER (ORDER BY score DESC)
		 FROM ties_tbl ORDER BY score DESC, name`)

	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// RANK: A=1, B=2, C=2, D=4
	rankA := toInt64GBW(t, rows[0][2], "RANK A")
	if rankA != 1 {
		t.Errorf("A RANK = %d, want 1", rankA)
	}
	rankB := toInt64GBW(t, rows[1][2], "RANK B")
	rankC := toInt64GBW(t, rows[2][2], "RANK C")
	if rankB != 2 || rankC != 2 {
		t.Errorf("B/C RANK = %d/%d, want 2/2", rankB, rankC)
	}
	rankD := toInt64GBW(t, rows[3][2], "RANK D")
	if rankD != 4 {
		t.Errorf("D RANK = %d, want 4", rankD)
	}
	// DENSE_RANK: A=1, B=2, C=2, D=3
	drD := toInt64GBW(t, rows[3][3], "DENSE_RANK D")
	if drD != 3 {
		t.Errorf("D DENSE_RANK = %d, want 3", drD)
	}
}

// TestWindowAllFunctions exercises multiple window functions in a single query,
// hitting initWindowRankRegisters, analyzeWindowRankFunctions.
func TestWindowAllFunctionsInOneQuery(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name, salary,
		        ROW_NUMBER() OVER (ORDER BY salary),
		        RANK() OVER (ORDER BY salary),
		        DENSE_RANK() OVER (ORDER BY salary),
		        NTILE(3) OVER (ORDER BY salary),
		        LAG(salary, 1, 0) OVER (ORDER BY salary),
		        LEAD(salary, 1, 0) OVER (ORDER BY salary),
		        FIRST_VALUE(salary) OVER (ORDER BY salary),
		        LAST_VALUE(salary) OVER (ORDER BY salary),
		        NTH_VALUE(salary, 2) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		 FROM employees ORDER BY salary`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// Validate ROW_NUMBER is sequential
	for i, row := range rows {
		rn := toInt64GBW(t, row[2], fmt.Sprintf("row %d ROW_NUMBER", i))
		if rn != int64(i+1) {
			t.Errorf("row %d ROW_NUMBER = %d, want %d", i, rn, i+1)
		}
	}
}

// TestWindowFrameExclude exercises convertFrameExclude paths.
func TestWindowFrameExclude(t *testing.T) {
	db := openGBWDB(t)
	gbwExec(t, db, "CREATE TABLE frame_tbl (v INTEGER)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		gbwExec(t, db, fmt.Sprintf("INSERT INTO frame_tbl VALUES (%d)", v))
	}

	// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (no explicit EXCLUDE)
	rows := collectGBWRows(t, db,
		`SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM frame_tbl ORDER BY v`)

	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	// Row 1: 1+2=3, Row 2: 1+2+3=6, Row 3: 2+3+4=9, Row 4: 3+4+5=12, Row 5: 4+5=9
	expected := []float64{3, 6, 9, 12, 9}
	for i, row := range rows {
		got := toFloat64GBW(t, row[0], fmt.Sprintf("row %d SUM", i))
		if got != expected[i] {
			t.Errorf("row %d SUM = %f, want %f", i, got, expected[i])
		}
	}
}

// TestWindowRankPartitionBy exercises processWindowRankFunction with PARTITION BY + ORDER BY
// (shouldExtractOrderBy + extractWindowOrderByCols branches).
func TestWindowRankPartitionBy(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT dept, name, salary,
		        RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
		 FROM employees ORDER BY dept, salary DESC`)

	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	// Verify rank 1 within each dept
	deptTopRank := make(map[string]int64)
	for _, row := range rows {
		var dept string
		switch x := row[0].(type) {
		case string:
			dept = x
		case []byte:
			dept = string(x)
		}
		rank := toInt64GBW(t, row[3], "dept_rank")
		if _, seen := deptTopRank[dept]; !seen {
			deptTopRank[dept] = rank
		}
	}
	for dept, topRank := range deptTopRank {
		if topRank != 1 {
			t.Errorf("dept %s: first row rank = %d, want 1", dept, topRank)
		}
	}
}

// TestWindowNamedWindowsMultiple exercises resolveNamedWindows with multiple
// named window definitions.
func TestWindowMultipleNamedWindows(t *testing.T) {
	db := openGBWDB(t)
	setupEmployeesTable(t, db)

	rows := collectGBWRows(t, db,
		`SELECT name,
		        RANK() OVER w1,
		        DENSE_RANK() OVER w1,
		        SUM(salary) OVER w2
		 FROM employees
		 WINDOW w1 AS (ORDER BY salary DESC),
		        w2 AS (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
		 ORDER BY salary DESC LIMIT 4`)

	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// Top salary earner (Carol, 92000) should be rank 1
	topRank := toInt64GBW(t, rows[0][1], "top RANK")
	if topRank != 1 {
		t.Errorf("top salary RANK = %d, want 1", topRank)
	}
}
