// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package builtin

import (
	"testing"
)

// TestParseFilterConstraints_NilArgSkipped covers the nil-arg branch in
// parseFilterConstraints (line 115: "if arg == nil { continue }").
// The function loops over argv and skips nil entries; without a nil arg this
// branch is never executed, leaving parseFilterConstraints at 83.3%.
func TestParseFilterConstraints_NilArgSkipped(t *testing.T) {
	t.Parallel()

	c := &SQLiteMasterCursor{}

	// Passing a nil followed by a real string confirms the nil branch is skipped
	// and the subsequent string is still processed correctly.
	c.parseFilterConstraints([]interface{}{nil, "table"})

	if c.typeFilter != "table" {
		t.Errorf("expected typeFilter='table' after skipping nil, got %q", c.typeFilter)
	}
	if c.nameFilter != "" {
		t.Errorf("expected nameFilter still empty, got %q", c.nameFilter)
	}
}

// TestParseFilterConstraints_AllNil verifies that all-nil argv leaves
// both filters empty (every iteration hits the continue branch).
func TestParseFilterConstraints_AllNil(t *testing.T) {
	t.Parallel()

	c := &SQLiteMasterCursor{}
	c.parseFilterConstraints([]interface{}{nil, nil, nil})

	if c.typeFilter != "" {
		t.Errorf("expected typeFilter empty for all-nil argv, got %q", c.typeFilter)
	}
	if c.nameFilter != "" {
		t.Errorf("expected nameFilter empty for all-nil argv, got %q", c.nameFilter)
	}
}

// TestParseFilterConstraints_NilThenBothFilters exercises nil skip followed by
// two non-nil strings, covering typeFilter and nameFilter assignment after nil.
func TestParseFilterConstraints_NilThenBothFilters(t *testing.T) {
	t.Parallel()

	c := &SQLiteMasterCursor{}
	c.parseFilterConstraints([]interface{}{nil, "index", "idx_users_name"})

	if c.typeFilter != "index" {
		t.Errorf("expected typeFilter='index', got %q", c.typeFilter)
	}
	if c.nameFilter != "idx_users_name" {
		t.Errorf("expected nameFilter='idx_users_name', got %q", c.nameFilter)
	}
}
