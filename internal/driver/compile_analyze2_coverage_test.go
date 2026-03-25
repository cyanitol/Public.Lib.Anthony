// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// TestAnalyzeToInt64Int exercises the int case of analyzeToInt64.
func TestAnalyzeToInt64Int(t *testing.T) {
	got := analyzeToInt64(int(42))
	if got != 42 {
		t.Errorf("analyzeToInt64(int(42)) = %d, want 42", got)
	}
}

// TestAnalyzeToInt64Float64 exercises the float64 case of analyzeToInt64.
func TestAnalyzeToInt64Float64(t *testing.T) {
	got := analyzeToInt64(float64(7.9))
	if got != 7 {
		t.Errorf("analyzeToInt64(float64(7.9)) = %d, want 7", got)
	}
}

// TestAnalyzeToInt64Float64Exact exercises float64 with an exact integer value.
func TestAnalyzeToInt64Float64Exact(t *testing.T) {
	got := analyzeToInt64(float64(100.0))
	if got != 100 {
		t.Errorf("analyzeToInt64(float64(100.0)) = %d, want 100", got)
	}
}

// TestAnalyzeToInt64String exercises the string case of analyzeToInt64.
func TestAnalyzeToInt64String(t *testing.T) {
	got := analyzeToInt64("55")
	if got != 55 {
		t.Errorf("analyzeToInt64(\"55\") = %d, want 55", got)
	}
}

// TestAnalyzeToInt64StringNonNumeric exercises the string case with non-numeric input.
func TestAnalyzeToInt64StringNonNumeric(t *testing.T) {
	got := analyzeToInt64("abc")
	if got != 0 {
		t.Errorf("analyzeToInt64(\"abc\") = %d, want 0", got)
	}
}

// TestAnalyzeToInt64Default exercises the default (unhandled type) case of analyzeToInt64.
func TestAnalyzeToInt64Default(t *testing.T) {
	got := analyzeToInt64(nil)
	if got != 0 {
		t.Errorf("analyzeToInt64(nil) = %d, want 0", got)
	}
}

// TestAnalyzeToInt64DefaultBool exercises the default case with a bool value.
func TestAnalyzeToInt64DefaultBool(t *testing.T) {
	got := analyzeToInt64(true)
	if got != 0 {
		t.Errorf("analyzeToInt64(true) = %d, want 0", got)
	}
}

// TestEstimateDistinctZero exercises the rowCount <= 0 branch of estimateDistinct.
func TestEstimateDistinctZero(t *testing.T) {
	got := estimateDistinct(0)
	if got != 1 {
		t.Errorf("estimateDistinct(0) = %d, want 1", got)
	}
}

// TestEstimateDistinctNegative exercises the rowCount < 0 branch of estimateDistinct.
func TestEstimateDistinctNegative(t *testing.T) {
	got := estimateDistinct(-5)
	if got != 1 {
		t.Errorf("estimateDistinct(-5) = %d, want 1", got)
	}
}

// TestEstimateDistinctPositive exercises the positive rowCount path of estimateDistinct.
func TestEstimateDistinctPositive(t *testing.T) {
	got := estimateDistinct(100)
	// estimateDistinct returns rowCount/10 for positive values.
	if got != 10 {
		t.Errorf("estimateDistinct(100) = %d, want 10", got)
	}
}

// TestEstimateDistinctSmallPositive exercises a small positive value where /10 rounds to 0.
// The function returns rowCount/10 which may be 0 for small values; no clamp is applied.
func TestEstimateDistinctSmallPositive(t *testing.T) {
	got := estimateDistinct(5)
	// 5/10 = 0; the function does not clamp to 1 for positive values.
	if got != 0 {
		t.Errorf("estimateDistinct(5) = %d, want 0", got)
	}
}

// TestComputeAvgRowsPerKeyZeroDistinct exercises the distinctCount <= 0 branch.
func TestComputeAvgRowsPerKeyZeroDistinct(t *testing.T) {
	got := computeAvgRowsPerKey(15, 0)
	if got != 15 {
		t.Errorf("computeAvgRowsPerKey(15, 0) = %d, want 15", got)
	}
}

// TestComputeAvgRowsPerKeyNegativeDistinct exercises the negative distinctCount branch.
func TestComputeAvgRowsPerKeyNegativeDistinct(t *testing.T) {
	got := computeAvgRowsPerKey(20, -1)
	if got != 20 {
		t.Errorf("computeAvgRowsPerKey(20, -1) = %d, want 20", got)
	}
}

// TestComputeAvgRowsPerKeyLowResult exercises the avg <= 0 clamp (rowCount < distinctCount).
func TestComputeAvgRowsPerKeyLowResult(t *testing.T) {
	// 1 row / 5 distinct = 0 avg, clamped to 1.
	got := computeAvgRowsPerKey(1, 5)
	if got != 1 {
		t.Errorf("computeAvgRowsPerKey(1, 5) = %d, want 1", got)
	}
}
