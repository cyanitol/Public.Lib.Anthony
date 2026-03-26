// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package slt

import (
	"strings"
	"testing"
)

// TestAccumulateTestData_ResultsState exercises the "results" state branch
// of accumulateTestData (the branch where State == "results").
// A query test in "results" state accumulates expected output lines.
func TestAccumulateTestData_ResultsState(t *testing.T) {
	runner := NewRunner(nil)

	test := &Test{
		Type:  "query",
		State: "results",
	}

	// passing nil for currentSQL is fine because results state doesn't use it
	runner.accumulateTestData("foo", test, nil)
	runner.accumulateTestData("bar", test, nil)

	if len(test.Expected) != 2 {
		t.Fatalf("expected 2 expected lines, got %d", len(test.Expected))
	}
	if test.Expected[0] != "foo" || test.Expected[1] != "bar" {
		t.Errorf("unexpected expected lines: %v", test.Expected)
	}
}

// TestAccumulateTestData_NilTest exercises the nil-test guard in accumulateTestData.
func TestAccumulateTestData_NilTest(t *testing.T) {
	runner := NewRunner(nil)
	// Must not panic
	runner.accumulateTestData("some line", nil, nil)
}

// TestExecuteTest_UnknownType exercises the "unknown test type" branch in
// executeTest (the final else branch that returns an error).
func TestExecuteTest_UnknownType(t *testing.T) {
	runner := NewRunner(nil)

	test := &Test{
		Type: "garbage",
		Line: 1,
	}

	result := runner.executeTest(test, "SELECT 1")
	if result.Passed {
		t.Error("expected unknown test type to fail")
	}
	if result.Error == nil {
		t.Error("expected non-nil error for unknown test type")
	}
}

// TestExecuteStatement_ExpectOK_WithError exercises the branch where
// ExpectOK=true but the statement fails at runtime.
func TestExecuteStatement_ExpectOK_WithError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)

	test := &Test{
		Type:     "statement",
		ExpectOK: true,
		Line:     1,
	}
	result := TestResult{
		File:     "<test>",
		Line:     1,
		TestType: "statement",
		SQL:      "INSERT INTO no_such_table VALUES (1)",
	}

	out := runner.executeStatement(test, "INSERT INTO no_such_table VALUES (1)", result)
	if out.Passed {
		t.Error("expected statement to fail when table does not exist")
	}
	if out.Error == nil {
		t.Error("expected non-nil error")
	}
}

// TestExecuteStatement_ExpectError_GotError exercises the happy-error path where
// ExpectError=true and the statement actually errors (result.Passed = true).
func TestExecuteStatement_ExpectError_GotError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	runner := NewRunner(db)

	test := &Test{
		Type:        "statement",
		ExpectError: true,
		Line:        1,
	}
	result := TestResult{
		File:     "<test>",
		Line:     1,
		TestType: "statement",
		SQL:      "INSERT INTO no_such_table VALUES (1)",
	}

	out := runner.executeStatement(test, "INSERT INTO no_such_table VALUES (1)", result)
	if !out.Passed {
		t.Errorf("expected statement-error to pass when error occurred: %v", out.Error)
	}
}

// TestAccumulateSQLOrResults_MultiLine tests that accumulateSQLOrResults joins
// SQL lines with newlines when the test type is "statement".
func TestAccumulateSQLOrResults_MultiLine(t *testing.T) {
	runner := NewRunner(nil)

	test := &Test{
		Type:  "statement",
		State: "sql",
	}

	var buf strings.Builder
	runner.accumulateSQLOrResults("SELECT 1", test, &buf)
	runner.accumulateSQLOrResults("FROM t", test, &buf)

	got := buf.String()
	if got != "SELECT 1\nFROM t" {
		t.Errorf("expected joined SQL, got %q", got)
	}
}

// TestCollectQueryRows_ViaRunString exercises collectQueryRows by executing a
// real query through RunString so rows.Next() and Scan() paths are hit.
func TestCollectQueryRows_ViaRunString(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE cr_test (x INTEGER, y TEXT)")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err = db.Exec("INSERT INTO cr_test VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO cr_test VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	runner := NewRunner(db)
	content := `query IT
SELECT x, y FROM cr_test ORDER BY x
----
1	a
2	b
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if !results[0].Passed {
		t.Errorf("query failed: expected=%q actual=%q err=%v",
			results[0].Expected, results[0].Actual, results[0].Error)
	}
}

// TestExecuteAndValidateQuery_ColumnsMismatch covers the column-count mismatch
// path inside executeAndValidateQuery (rows.Columns() count != len(ColumnTypes)).
func TestExecuteAndValidateQuery_ColumnsMismatch(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE cq_test (a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err = db.Exec("INSERT INTO cq_test VALUES (1, 'x')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	runner := NewRunner(db)
	// "query I" declares 1 column but SELECT returns 2 → mismatch
	content := `query I
SELECT a, b FROM cq_test
----
1
`
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected column-count mismatch to fail")
	}
}
