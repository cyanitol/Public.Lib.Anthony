// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package slt

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// TestSetSkipOnError tests the SetSkipOnError method
func TestSetSkipOnError(t *testing.T) {
	runner := NewRunner(nil)
	runner.SetSkipOnError(true)
	if !runner.skipOnError {
		t.Error("SetSkipOnError(true) did not set skipOnError")
	}
	runner.SetSkipOnError(false)
	if runner.skipOnError {
		t.Error("SetSkipOnError(false) did not clear skipOnError")
	}
}

// TestRunFile tests running tests from a file
func TestRunFile(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	content := `statement ok
CREATE TABLE file_test (id INTEGER, name TEXT)

statement ok
INSERT INTO file_test VALUES (1, 'hello')

query IT
SELECT id, name FROM file_test
----
1	hello
`

	f, err := os.CreateTemp("", "slt_test_*.test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	f.Close()

	runner := NewRunner(db)
	results, err := runner.RunFile(f.Name())
	if err != nil {
		t.Fatalf("RunFile failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results from RunFile, got none")
	}
	for i, result := range results {
		if !result.Passed {
			t.Errorf("result %d failed: %v", i, result.Error)
		}
	}
}

// TestRunFileNotFound tests RunFile with a non-existent file
func TestRunFileNotFound(t *testing.T) {
	runner := NewRunner(nil)
	_, err := runner.RunFile("/nonexistent/path/test.slt")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

// TestPrintSummary tests PrintSummary output
func TestPrintSummary(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)
	_, err := runner.RunString(`statement ok
CREATE TABLE ps_test (id INTEGER)
`)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}

	// PrintSummary writes to stdout; just call it to ensure no panic
	runner.PrintSummary()
}

// TestPrintSummaryNoTests tests PrintSummary with zero tests (no pass rate branch)
func TestPrintSummaryNoTests(t *testing.T) {
	runner := NewRunner(nil)
	// totalTests == 0: the pass-rate line should be skipped
	runner.PrintSummary()
}

// TestCompareWithHash tests hash comparison when rows exceed threshold
func TestCompareWithHash(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)
	runner.SetHashThreshold(2) // set low so 3 rows triggers hash path

	_, err := runner.RunString(`statement ok
CREATE TABLE hash_test (id INTEGER)

statement ok
INSERT INTO hash_test VALUES (1)

statement ok
INSERT INTO hash_test VALUES (2)

statement ok
INSERT INTO hash_test VALUES (3)
`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Calculate expected hash manually
	actualRows := [][]string{{"1"}, {"2"}, {"3"}}
	expectedHash := calculateHash(actualRows)

	runner2 := NewRunner(db)
	runner2.SetHashThreshold(2)

	content := fmt.Sprintf(`query I
SELECT id FROM hash_test ORDER BY id
----
%s
`, expectedHash)

	results, err := runner2.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Passed {
		t.Errorf("hash comparison failed: expected=%s actual=%s err=%v",
			results[0].Expected, results[0].Actual, results[0].Error)
	}
}

// TestCompareWithHashMismatch tests hash mismatch path
func TestCompareWithHashMismatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE hm_test (id INTEGER)`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	for i := 1; i <= 3; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO hm_test VALUES (%d)`, i))
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	runner := NewRunner(db)
	runner.SetHashThreshold(2)

	content := `query I
SELECT id FROM hm_test ORDER BY id
----
wronghashvalue
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected hash mismatch to fail, but it passed")
	}
}

// TestExecuteTestEmptySQL tests that empty SQL produces a failed result
func TestExecuteTestEmptySQL(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)
	// A statement with only whitespace as SQL
	content := "statement ok\n   \n"
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected empty SQL to fail, but it passed")
	}
}

// TestExecuteStatementExpectErrorGotOK tests the case where error was expected but ok was returned
func TestExecuteStatementExpectErrorGotOK(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE exist_table (id INTEGER)`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	runner := NewRunner(db)
	// Expect error from a statement that actually succeeds
	content := `statement error
SELECT 1
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected test to fail when error expected but got ok")
	}
}

// TestHashThresholdInvalid tests invalid hash-threshold directive formats
func TestHashThresholdInvalid(t *testing.T) {
	runner := NewRunner(nil)

	// Invalid: no value
	result := runner.handleHashThreshold("hash-threshold")
	if result.Passed {
		t.Error("expected failure for hash-threshold with no value")
	}

	// Invalid: non-numeric value
	result = runner.handleHashThreshold("hash-threshold notanumber")
	if result.Passed {
		t.Error("expected failure for hash-threshold with non-numeric value")
	}
}

// TestFormatValueDefault tests the default case in formatValue
func TestFormatValueDefault(t *testing.T) {
	// Use a custom type that falls through to the default fmt.Sprintf case
	type myType struct{ x int }
	val := myType{x: 42}
	result := formatValue(val)
	if !strings.Contains(result, "42") {
		t.Errorf("formatValue default case: expected to contain '42', got %q", result)
	}
}

// TestExecuteQueryColumnMismatch tests column count mismatch in query execution
func TestExecuteQueryColumnMismatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE cm_test (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	_, err = db.Exec(`INSERT INTO cm_test VALUES (1, 'Alice')`)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	runner := NewRunner(db)
	// Query has 2 columns but we declare only 1 type
	content := `query I
SELECT id, name FROM cm_test
----
1
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected column count mismatch to fail, but it passed")
	}
}

// TestExecuteQueryError tests a query that fails at execution
func TestExecuteQueryError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)
	// Query from a non-existent table
	content := `query I
SELECT id FROM no_such_table_xyz
----
1
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected query error to fail, but it passed")
	}
}

// TestParseQueryDirectiveNoTypes tests query directive with no column types
func TestParseQueryDirectiveNoTypes(t *testing.T) {
	test := &Test{}
	test.parseQueryDirective("query")
	if test.ColumnTypes != "" {
		t.Errorf("expected empty ColumnTypes, got %q", test.ColumnTypes)
	}
	if test.State != "sql" {
		t.Errorf("expected state 'sql', got %q", test.State)
	}
}

// TestParseStatementDirectiveCustomLabel tests label without ok/error
func TestParseStatementDirectiveCustomLabel(t *testing.T) {
	test := &Test{}
	test.parseStatementDirective("statement mylabel")
	if !test.ExpectOK {
		t.Error("expected ExpectOK for custom label")
	}
	if test.Label != "mylabel" {
		t.Errorf("expected Label 'mylabel', got %q", test.Label)
	}
}

// TestRunnerVerboseMode tests SetVerbose (ensures it sets the field)
func TestRunnerVerboseMode(t *testing.T) {
	runner := NewRunner(nil)
	runner.SetVerbose(true)
	if !runner.verbose {
		t.Error("SetVerbose(true) did not set verbose")
	}
	runner.SetVerbose(false)
	if runner.verbose {
		t.Error("SetVerbose(false) did not clear verbose")
	}
}

// TestReconstructRowsPartial tests reconstructRows when values don't divide evenly
func TestReconstructRowsPartial(t *testing.T) {
	runner := NewRunner(nil)
	// 5 values with colCount=2 -> rows of [2,2,1]
	values := []string{"a", "b", "c", "d", "e"}
	rows := runner.reconstructRows(values, 2)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if len(rows[2]) != 1 || rows[2][0] != "e" {
		t.Errorf("unexpected last row: %v", rows[2])
	}
}

// TestRunnerMultipleHashThresholds tests multiple hash-threshold directives
func TestRunnerMultipleHashThresholds(t *testing.T) {
	runner := NewRunner(nil)
	content := `hash-threshold 10
hash-threshold 20
hash-threshold 30
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.Passed {
			t.Errorf("result %d failed: %v", i, r.Error)
		}
	}
	if runner.hashThreshold != 30 {
		t.Errorf("expected hashThreshold=30, got %d", runner.hashThreshold)
	}
}

// TestRunnerResultMismatch tests that result mismatch returns a failed result
func TestRunnerResultMismatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE mismatch_test (id INTEGER)`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	_, err = db.Exec(`INSERT INTO mismatch_test VALUES (42)`)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	runner := NewRunner(db)
	content := `query I
SELECT id FROM mismatch_test
----
99
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected result mismatch to fail, but it passed")
	}
}

// TestCheckScannerErrorPath exercises the error path in checkScannerError
// by passing a scanner that has already errored (via oversized token).
func TestCheckScannerErrorPath(t *testing.T) {
	// Create a very long line to trigger scanner buffer overflow
	longLine := strings.Repeat("x", 1024*1024) // 1MB - exceeds default scanner buffer
	reader := strings.NewReader("# comment\n" + longLine + "\n")

	runner := NewRunner(nil)
	runner.currentFile = "<test>"
	_, err := runner.runTests(reader)
	if err == nil {
		t.Error("expected scanner error for oversized token, got nil")
	}
	if !strings.Contains(err.Error(), "error reading test file") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestApplySortingUnknownMode tests that unknown sort mode is a no-op
func TestApplySortingUnknownMode(t *testing.T) {
	runner := NewRunner(nil)
	rows := [][]string{{"b"}, {"a"}}
	result := runner.applySorting(rows, "unknownsortmode")
	if result[0][0] != "b" {
		t.Errorf("unknown sort mode should not change order, got %v", result)
	}
}

// TestValueSortEmptyRows tests applyValueSort with empty input
func TestValueSortEmptyRows(t *testing.T) {
	runner := NewRunner(nil)
	result := runner.applyValueSort(nil)
	if len(result) != 0 {
		t.Errorf("expected empty result for nil input, got %v", result)
	}
}

// TestRunnerStatsCaptureBuffer exercises PrintSummary with non-zero stats
// by redirecting stdout temporarily.
func TestRunnerStatsSummaryWithTests(t *testing.T) {
	// Capture stdout to ensure PrintSummary doesn't panic with real stats
	old := os.Stdout
	r2, w, _ := os.Pipe()
	os.Stdout = w

	runner := &Runner{
		totalTests:  5,
		passedTests: 4,
		failedTests: 1,
	}
	runner.PrintSummary()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r2)
	out := buf.String()
	if !strings.Contains(out, "80.00%") {
		t.Errorf("expected pass rate 80.00%% in summary output, got: %s", out)
	}
}
