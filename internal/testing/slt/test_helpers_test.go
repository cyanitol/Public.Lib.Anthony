// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package slt

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openTestDB opens a test database connection.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	return db
}

// runTestString runs a test string and returns results.
func runTestString(t *testing.T, runner *Runner, content string) []TestResult {
	t.Helper()
	results, err := runner.RunString(content)
	if err != nil {
		t.Fatalf("RunString failed: %v", err)
	}
	return results
}

// verifyResultCount checks if result count matches expected.
func verifyResultCount(t *testing.T, results []TestResult, expected int) {
	t.Helper()
	if len(results) != expected {
		t.Fatalf("expected %d results, got %d", expected, len(results))
	}
}

// verifyAllPassed checks if all results passed.
func verifyAllPassed(t *testing.T, results []TestResult) {
	t.Helper()
	for i, result := range results {
		if !result.Passed {
			t.Errorf("test %d failed: %v\nSQL: %s", i, result.Error, result.SQL)
		}
	}
}

// verifyStats checks if runner stats match expected values.
func verifyStats(t *testing.T, runner *Runner, expectedTotal, expectedPassed, expectedFailed int) {
	t.Helper()
	total, passed, failed, _ := runner.GetStats()
	if total != expectedTotal || passed != expectedPassed || failed != expectedFailed {
		t.Errorf("stats mismatch: total=%d, passed=%d, failed=%d", total, passed, failed)
	}
}
