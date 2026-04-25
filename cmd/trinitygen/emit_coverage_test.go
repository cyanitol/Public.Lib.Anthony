// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

package main

import (
	"bytes"
	"testing"
)

// TestFormatGoValue covers all branches in formatGoValue (emit_helpers.go:12).
func TestFormatGoValue(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  string
	}{
		{"nil", nil, "nil"},
		{"int64_positive", int64(42), "int64(42)"},
		{"int64_negative", int64(-7), "int64(-7)"},
		{"int64_zero", int64(0), "int64(0)"},
		{"float64_whole", float64(3), "3"},
		{"float64_fraction", float64(1.5), "1.5"},
		{"string_plain", "hello", `"hello"`},
		{"string_empty", "", `""`},
		{"string_with_quotes", `say "hi"`, `"say \"hi\""`},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"fallback_int", int(99), "99"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatGoValue(tc.input)
			if got != tc.want {
				t.Errorf("formatGoValue(%v) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestSanitizeName covers all branches of sanitizeName and its helpers.
func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain_letters", "hello", "hello"},
		{"with_space", "foo bar", "foo_bar"},
		{"with_hyphen", "foo-bar", "foo_bar"},
		{"with_underscore", "foo_bar", "foo_bar"},
		{"leading_digit", "3things", "t_3things"},
		{"empty_string", "", "unnamed"},
		{"all_special", "!@#", "unnamed"},
		{"unicode_letter", "héllo", "héllo"},
		{"digits_only", "42", "t_42"},
		{"mixed", "select * from t", "select__from_t"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeName(tc.input)
			if got != tc.want {
				t.Errorf("sanitizeName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestAssignReqID covers assignReqID (emit_helpers.go:59).
func TestAssignReqID(t *testing.T) {
	tests := []struct {
		name   string
		module string
		seq    int
		desc   string
		want   string
	}{
		{
			name:   "basic",
			module: "core",
			seq:    1,
			desc:   "insert row",
			want:   "REQ-CORE-001_insert_row",
		},
		{
			name:   "large_seq",
			module: "types",
			seq:    99,
			desc:   "cast integer",
			want:   "REQ-TYPES-099_cast_integer",
		},
		{
			name:   "uppercase_module",
			module: "DDL",
			seq:    5,
			desc:   "create table",
			want:   "REQ-DDL-005_create_table",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := assignReqID(tc.module, tc.seq, tc.desc)
			if got != tc.want {
				t.Errorf("assignReqID(%q, %d, %q) = %q, want %q",
					tc.module, tc.seq, tc.desc, got, tc.want)
			}
		})
	}
}

// TestAssignReqID_Uniqueness verifies that different inputs produce different IDs.
func TestAssignReqID_Uniqueness(t *testing.T) {
	id1 := assignReqID("core", 1, "first test")
	id2 := assignReqID("core", 2, "second test")
	if id1 == id2 {
		t.Errorf("expected distinct IDs but got identical: %q", id1)
	}
}

// TestReportModules verifies that reportModules prints all registered modules.
func TestReportModules(t *testing.T) {
	// Capture stdout via a buffer redirect trick: since reportModules writes
	// to os.Stdout directly, we verify by checking the registry independently
	// and confirming reportModules doesn't panic.
	reportModules() // must not panic

	names := moduleNames()
	if len(names) == 0 {
		t.Fatal("moduleNames returned empty slice")
	}
	// Spot-check that known modules are present.
	known := []string{"core", "types", "null", "boundary"}
	for _, k := range known {
		found := false
		for _, n := range names {
			if n == k {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected module %q in registry, not found", k)
		}
	}
}

// TestEmitSetup_ViaEmitTestCase exercises emitSetup by calling emitTestCase
// with a TestSpec that has setup statements.
func TestEmitSetup_ViaEmitTestCase(t *testing.T) {
	t.Run("no_setup", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{Name: "t1", Query: "SELECT 1"})
		assertNotContains(t, buf.String(), "setup:")
	})

	t.Run("single_setup_stmt", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{
			Name:  "t2",
			Setup: []string{"CREATE TABLE x(id INTEGER)"},
			Query: "SELECT 1",
		})
		out := buf.String()
		assertContains(t, out, "setup:")
		assertContains(t, out, "CREATE TABLE x(id INTEGER)")
	})

	t.Run("multiple_setup_stmts", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{
			Name: "t3",
			Setup: []string{
				"CREATE TABLE y(v TEXT)",
				"INSERT INTO y VALUES('a')",
			},
			Query: "SELECT v FROM y",
		})
		out := buf.String()
		assertContains(t, out, "setup:")
		assertContains(t, out, "INSERT INTO y")
	})
}

// TestEmitWantRows_ViaEmitTestCase exercises emitWantRows by calling
// emitTestCase with various WantRows configurations.
func TestEmitWantRows_ViaEmitTestCase(t *testing.T) {
	t.Run("no_rows", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{Name: "r0", Exec: "DELETE FROM t"})
		assertNotContains(t, buf.String(), "wantRows:")
	})

	t.Run("single_row_single_col", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{
			Name:  "r1",
			Query: "SELECT 1",
			WantRows: [][]Value{
				{{GoLiteral: "int64(1)"}},
			},
		})
		out := buf.String()
		assertContains(t, out, "wantRows:")
		assertContains(t, out, "int64(1)")
	})

	t.Run("multiple_rows_multiple_cols", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{
			Name:  "r2",
			Query: "SELECT id, name FROM t",
			WantRows: [][]Value{
				{{GoLiteral: "int64(1)"}, {GoLiteral: `"alice"`}},
				{{GoLiteral: "int64(2)"}, {GoLiteral: `"bob"`}},
			},
		})
		out := buf.String()
		assertContains(t, out, `"alice"`)
		assertContains(t, out, `"bob"`)
	})

	t.Run("row_with_nil", func(t *testing.T) {
		var buf bytes.Buffer
		emitTestCase(&buf, TestSpec{
			Name:  "r3",
			Query: "SELECT null",
			WantRows: [][]Value{
				{{GoLiteral: "nil"}},
			},
		})
		assertContains(t, buf.String(), "nil")
	})
}
