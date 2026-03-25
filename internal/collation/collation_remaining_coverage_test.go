// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package collation

import (
	"strings"
	"testing"
)

// TestRegisterCollation_GlobalFunction covers RegisterCollation (line 142-144),
// which was 0.0% covered.
func TestRegisterCollation_GlobalFunction(t *testing.T) {
	t.Parallel()

	fn := func(a, b string) int {
		return strings.Compare(a, b)
	}

	if err := RegisterCollation("MYTEST", fn); err != nil {
		t.Fatalf("RegisterCollation failed: %v", err)
	}

	coll, ok := GetCollation("MYTEST")
	if !ok {
		t.Fatal("expected MYTEST to be registered in global registry")
	}
	if coll.Name != "MYTEST" {
		t.Errorf("expected name MYTEST, got %q", coll.Name)
	}
}

// TestUnregisterCollation_GlobalFunction covers UnregisterCollation (line 152-154),
// which was 0.0% covered.
func TestUnregisterCollation_GlobalFunction(t *testing.T) {
	t.Parallel()

	fn := func(a, b string) int { return 0 }

	// Register first so we can unregister.
	if err := RegisterCollation("TMPTEST", fn); err != nil {
		t.Fatalf("RegisterCollation setup failed: %v", err)
	}

	if err := UnregisterCollation("TMPTEST"); err != nil {
		t.Fatalf("UnregisterCollation failed: %v", err)
	}

	// Confirm it was removed.
	_, ok := GetCollation("TMPTEST")
	if ok {
		t.Error("expected TMPTEST to be removed from global registry")
	}
}

// TestUnregisterCollation_BuiltinRejected covers the built-in protection path
// when UnregisterCollation is called on BINARY, NOCASE, or RTRIM.
func TestUnregisterCollation_BuiltinRejected(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"BINARY", "NOCASE", "RTRIM"} {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := UnregisterCollation(name)
			if err == nil {
				t.Errorf("expected error when unregistering built-in %q", name)
			}
		})
	}
}

// TestRegisterCollation_EmptyName covers RegisterCollation error for empty name.
func TestRegisterCollation_EmptyName(t *testing.T) {
	t.Parallel()

	err := RegisterCollation("", func(a, b string) int { return 0 })
	if err == nil {
		t.Error("expected error for empty collation name")
	}
}

// TestRegisterCollation_NilFunc covers RegisterCollation error for nil function.
func TestRegisterCollation_NilFunc(t *testing.T) {
	t.Parallel()

	err := RegisterCollation("NILTEST", nil)
	if err == nil {
		t.Error("expected error for nil collation function")
	}
}
