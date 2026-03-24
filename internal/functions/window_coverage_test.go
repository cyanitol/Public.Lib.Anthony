// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import (
	"strings"
	"testing"
)

// TestPercentRankFuncMethods exercises NumArgs and Call on PercentRankFunc.
func TestPercentRankFuncMethods(t *testing.T) {
	f := &PercentRankFunc{}

	if f.NumArgs() != 0 {
		t.Errorf("PercentRankFunc.NumArgs() = %d, want 0", f.NumArgs())
	}

	_, err := f.Call([]Value{})
	if err == nil {
		t.Error("PercentRankFunc.Call() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "percent_rank") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestCumeDistFuncMethods exercises NumArgs and Call on CumeDistFunc.
func TestCumeDistFuncMethods(t *testing.T) {
	f := &CumeDistFunc{}

	if f.NumArgs() != 0 {
		t.Errorf("CumeDistFunc.NumArgs() = %d, want 0", f.NumArgs())
	}

	_, err := f.Call([]Value{})
	if err == nil {
		t.Error("CumeDistFunc.Call() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "cume_dist") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestNthValueFuncMethods exercises NumArgs and Call on NthValueFunc.
func TestNthValueFuncMethods(t *testing.T) {
	f := &NthValueFunc{}

	if f.NumArgs() != 2 {
		t.Errorf("NthValueFunc.NumArgs() = %d, want 2", f.NumArgs())
	}

	_, err := f.Call([]Value{NewIntValue(1), NewIntValue(2)})
	if err == nil {
		t.Error("NthValueFunc.Call() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nth_value") {
		t.Errorf("unexpected error message: %v", err)
	}
}
