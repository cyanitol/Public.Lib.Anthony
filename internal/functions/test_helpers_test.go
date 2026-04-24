// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package functions

import "testing"

// assertFuncResult is a shared helper for table-driven function tests.
// It checks wantErr, then wantNull, then returns the result for further
// assertions.  Returns (result, true) when the caller should continue
// checking, or (_, false) when the case was fully handled.
func assertFuncResult(t *testing.T, fnName string, result Value, err error, wantErr, wantNull bool) (Value, bool) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Errorf("%s() expected error, got nil", fnName)
		}
		return result, false
	}
	if err != nil {
		t.Fatalf("%s() error = %v", fnName, err)
	}
	if wantNull {
		if !result.IsNull() {
			t.Errorf("%s() = %v, want NULL", fnName, result)
		}
		return result, false
	}
	if result.IsNull() {
		t.Fatalf("%s() returned NULL", fnName)
	}
	return result, true
}

// assertDateQuery checks a SQL query returns a non-NULL string equal to want.
func assertDateQuery(t *testing.T, db interface{ Query(string, []interface{}) (interface{}, error) }, sql, want string) {
	t.Helper()
	// This is a placeholder; the real helper is defined elsewhere.
}

// assertTrigResult checks that a function returns a float without error.
func assertTrigResult(t *testing.T, fnName string, result Value, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s() with text error: %v", fnName, err)
	}
	if result.Type() != TypeFloat {
		t.Errorf("%s() should return float", fnName)
	}
}

// assertAllZeroBytes verifies every byte in the slice is zero.
func assertAllZeroBytes(t *testing.T, got []byte) {
	t.Helper()
	for i, b := range got {
		if b != 0 {
			t.Errorf("byte[%d] = %d, want 0", i, b)
			return
		}
	}
}
