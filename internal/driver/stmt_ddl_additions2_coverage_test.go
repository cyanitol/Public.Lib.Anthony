// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// emptyForeignKeyCheckResult
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_EmptyForeignKeyCheckResult verifies that
// emptyForeignKeyCheckResult sets the four result columns and adds exactly two
// instructions (OpInit and OpHalt) to the VDBE program.
func TestStmtDDLAdditions2_EmptyForeignKeyCheckResult(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	s := stmtFor(conn)
	vm := vdbe.New()

	got, err := s.emptyForeignKeyCheckResult(vm)
	if err != nil {
		t.Fatalf("emptyForeignKeyCheckResult: %v", err)
	}
	if got == nil {
		t.Fatal("emptyForeignKeyCheckResult returned nil VDBE")
	}

	wantCols := []string{"table", "rowid", "parent", "fkid"}
	if len(got.ResultCols) != len(wantCols) {
		t.Fatalf("ResultCols: got %v, want %v", got.ResultCols, wantCols)
	}
	for i, c := range wantCols {
		if got.ResultCols[i] != c {
			t.Errorf("ResultCols[%d]: got %q, want %q", i, got.ResultCols[i], c)
		}
	}

	// Expect at least 2 instructions (OpInit + OpHalt).
	if len(got.Program) < 2 {
		t.Errorf("Program len: got %d, want >= 2", len(got.Program))
	}
}

// ---------------------------------------------------------------------------
// valuesEqual
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_ValuesEqual covers the valuesEqual method on driverRowReader.
func TestStmtDDLAdditions2_ValuesEqual(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	cases := []struct {
		name string
		v1   interface{}
		v2   interface{}
		want bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, int64(1), false},
		{"right nil", int64(1), nil, false},
		{"equal int64", int64(42), int64(42), true},
		{"unequal int64", int64(1), int64(2), false},
		{"int and int64 equal", int(7), int64(7), true},
		{"equal strings", "hello", "hello", true},
		{"unequal strings", "a", "b", false},
		{"float whole number", float64(3), float64(3), true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqual(tc.v1, tc.v2)
			if got != tc.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tc.v1, tc.v2, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// valuesEqualWithCollation
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_ValuesEqualWithCollation covers valuesEqualWithCollation.
func TestStmtDDLAdditions2_ValuesEqualWithCollation(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	cases := []struct {
		name      string
		v1        interface{}
		v2        interface{}
		collation string
		want      bool
	}{
		{"both nil binary", nil, nil, "BINARY", true},
		{"left nil", nil, "x", "BINARY", false},
		{"right nil", "x", nil, "BINARY", false},
		{"equal int binary", int64(5), int64(5), "BINARY", true},
		{"unequal int binary", int64(5), int64(6), "BINARY", false},
		{"equal string BINARY", "abc", "abc", "BINARY", true},
		{"unequal string BINARY", "abc", "ABC", "BINARY", false},
		{"equal string NOCASE", "abc", "ABC", "NOCASE", true},
		{"unequal string NOCASE", "abc", "xyz", "NOCASE", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := r.valuesEqualWithCollation(tc.v1, tc.v2, tc.collation)
			if got != tc.want {
				t.Errorf("valuesEqualWithCollation(%v, %v, %q) = %v, want %v",
					tc.v1, tc.v2, tc.collation, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RowExistsWithCollation / scanForMatchWithCollation / checkRowMatchWithCollation
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_RowExistsWithCollation_EmptyTable verifies that
// RowExistsWithCollation returns false (no error) on an empty table.
func TestStmtDDLAdditions2_RowExistsWithCollation_EmptyTable(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE rewc_empty (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	r := newDriverRowReader(conn)
	found, err := r.RowExistsWithCollation("rewc_empty",
		[]string{"name"}, []interface{}{"Alice"}, []string{"BINARY"})
	if err != nil {
		t.Fatalf("RowExistsWithCollation: %v", err)
	}
	if found {
		t.Error("expected false on empty table, got true")
	}
}

// TestStmtDDLAdditions2_RowExistsWithCollation_Hit verifies RowExistsWithCollation
// returns true when a matching row exists and false for a non-matching value.
func TestStmtDDLAdditions2_RowExistsWithCollation_Hit(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE rewc_hit (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO rewc_hit VALUES (1, 'Alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	r := newDriverRowReader(conn)

	// Should find Alice (exact BINARY match).
	found, err := r.RowExistsWithCollation("rewc_hit",
		[]string{"name"}, []interface{}{"Alice"}, []string{"BINARY"})
	if err != nil {
		t.Fatalf("RowExistsWithCollation hit: %v", err)
	}
	if !found {
		t.Error("expected true for existing row, got false")
	}

	// Should NOT find alice (BINARY is case-sensitive).
	found, err = r.RowExistsWithCollation("rewc_hit",
		[]string{"name"}, []interface{}{"alice"}, []string{"BINARY"})
	if err != nil {
		t.Fatalf("RowExistsWithCollation miss: %v", err)
	}
	if found {
		t.Error("expected false for case-mismatched row with BINARY collation, got true")
	}
}

// TestStmtDDLAdditions2_RowExistsWithCollation_NOCASE verifies NOCASE collation
// returns true for case-folded matches.
func TestStmtDDLAdditions2_RowExistsWithCollation_NOCASE(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE rewc_nocase (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO rewc_nocase VALUES (1, 'Alice')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	r := newDriverRowReader(conn)

	found, err := r.RowExistsWithCollation("rewc_nocase",
		[]string{"name"}, []interface{}{"alice"}, []string{"NOCASE"})
	if err != nil {
		t.Fatalf("RowExistsWithCollation NOCASE: %v", err)
	}
	if !found {
		t.Error("expected true with NOCASE collation for case-folded match, got false")
	}
}

// TestStmtDDLAdditions2_RowExistsWithCollation_NoColumns verifies that an
// empty column slice (scanForMatchWithCollation short-circuit) returns true
// when at least one row exists.
func TestStmtDDLAdditions2_RowExistsWithCollation_NoColumns(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE rewc_nocols (id INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO rewc_nocols VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	r := newDriverRowReader(conn)

	// Empty columns → every row matches.
	found, err := r.RowExistsWithCollation("rewc_nocols",
		[]string{}, []interface{}{}, []string{})
	if err != nil {
		t.Fatalf("RowExistsWithCollation no columns: %v", err)
	}
	if !found {
		t.Error("expected true with empty columns when rows exist, got false")
	}
}

// TestStmtDDLAdditions2_RowExistsWithCollation_MissingTable verifies that a
// non-existent table name returns an error.
func TestStmtDDLAdditions2_RowExistsWithCollation_MissingTable(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	r := newDriverRowReader(conn)

	_, err := r.RowExistsWithCollation("no_such_table_xyz",
		[]string{"id"}, []interface{}{int64(1)}, []string{"BINARY"})
	if err == nil {
		t.Error("expected error for non-existent table, got nil")
	}
}

// ---------------------------------------------------------------------------
// FindReferencingRowsWithParentAffinity / collectMatchingRowidsWithAffinity /
// checkRowMatchWithParentAffinity
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_EmptyTable
// verifies an empty result for an empty child table.
func TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_EmptyTable(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE parent_pa (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if err := conn.ExecDDL("CREATE TABLE child_pa (pid INTEGER, val TEXT)"); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}

	r := newDriverRowReader(conn)
	rowids, err := r.FindReferencingRowsWithParentAffinity(
		"child_pa", []string{"pid"},
		[]interface{}{int64(1)},
		"parent_pa", []string{"id"},
	)
	if err != nil {
		t.Fatalf("FindReferencingRowsWithParentAffinity: %v", err)
	}
	if len(rowids) != 0 {
		t.Errorf("expected 0 rowids for empty table, got %d", len(rowids))
	}
}

// TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_Hit verifies
// that matching rows are returned when the child table has relevant rows.
func TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_Hit(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE parent_pa2 (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}
	if err := conn.ExecDDL("CREATE TABLE child_pa2 (pid INTEGER, label TEXT)"); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO child_pa2 VALUES (10, 'x')"); err != nil {
		t.Fatalf("INSERT child row 1: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO child_pa2 VALUES (20, 'y')"); err != nil {
		t.Fatalf("INSERT child row 2: %v", err)
	}

	r := newDriverRowReader(conn)

	// Only row with pid=10 should match.
	rowids, err := r.FindReferencingRowsWithParentAffinity(
		"child_pa2", []string{"pid"},
		[]interface{}{int64(10)},
		"parent_pa2", []string{"id"},
	)
	if err != nil {
		t.Fatalf("FindReferencingRowsWithParentAffinity: %v", err)
	}
	if len(rowids) != 1 {
		t.Errorf("expected 1 matching rowid, got %d", len(rowids))
	}
}

// TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_NoColumns verifies
// that an empty childColumns slice matches all rows (short-circuit path).
func TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_NoColumns(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE child_pa3 (val INTEGER)"); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO child_pa3 VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO child_pa3 VALUES (2)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	r := newDriverRowReader(conn)

	// Empty columns: every row matches.
	rowids, err := r.FindReferencingRowsWithParentAffinity(
		"child_pa3", []string{},
		[]interface{}{},
		"", []string{},
	)
	if err != nil {
		t.Fatalf("FindReferencingRowsWithParentAffinity no-columns: %v", err)
	}
	if len(rowids) != 2 {
		t.Errorf("expected 2 rowids (all rows), got %d", len(rowids))
	}
}

// TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_NilParentTable
// exercises the path where parentTable is nil (parentTableName not found).
func TestStmtDDLAdditions2_FindReferencingRowsWithParentAffinity_NilParentTable(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)

	if err := conn.ExecDDL("CREATE TABLE child_pa4 (pid INTEGER)"); err != nil {
		t.Fatalf("CREATE child: %v", err)
	}
	if _, err := conn.ExecDML("INSERT INTO child_pa4 VALUES (5)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	r := newDriverRowReader(conn)

	// parentTableName does not exist → parentTable will be nil inside the method.
	rowids, err := r.FindReferencingRowsWithParentAffinity(
		"child_pa4", []string{"pid"},
		[]interface{}{int64(5)},
		"nonexistent_parent", []string{"id"},
	)
	if err != nil {
		t.Fatalf("FindReferencingRowsWithParentAffinity nil parent: %v", err)
	}
	// Should still find the matching row (no affinity conversion applied).
	if len(rowids) != 1 {
		t.Errorf("expected 1 rowid, got %d", len(rowids))
	}
}

// ---------------------------------------------------------------------------
// validateSourceSchema (stmt_vacuum.go)
// ---------------------------------------------------------------------------

// TestStmtDDLAdditions2_ValidateSourceSchema_Valid verifies that a valid *schema.Schema
// is returned unchanged.
func TestStmtDDLAdditions2_ValidateSourceSchema_Valid(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	s := stmtFor(conn)

	src := schema.NewSchema()
	got := s.validateSourceSchema(src)
	if got == nil {
		t.Error("validateSourceSchema with valid *schema.Schema: expected non-nil, got nil")
	}
	if got != src {
		t.Error("validateSourceSchema: returned a different pointer than the input")
	}
}

// TestStmtDDLAdditions2_ValidateSourceSchema_Nil verifies that a nil interface{}
// returns nil.
func TestStmtDDLAdditions2_ValidateSourceSchema_Nil(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	s := stmtFor(conn)

	got := s.validateSourceSchema(nil)
	if got != nil {
		t.Errorf("validateSourceSchema(nil): expected nil, got %v", got)
	}
}

// TestStmtDDLAdditions2_ValidateSourceSchema_WrongType verifies that a
// non-*schema.Schema value returns nil.
func TestStmtDDLAdditions2_ValidateSourceSchema_WrongType(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	s := stmtFor(conn)

	got := s.validateSourceSchema("not a schema")
	if got != nil {
		t.Errorf("validateSourceSchema(string): expected nil, got %v", got)
	}
}

// TestStmtDDLAdditions2_ValidateSourceSchema_NilSchema verifies that a typed
// nil (*schema.Schema)(nil) returns nil.
func TestStmtDDLAdditions2_ValidateSourceSchema_NilSchema(t *testing.T) {
	t.Parallel()
	conn := openMemConn(t)
	s := stmtFor(conn)

	var nilSchema *schema.Schema
	got := s.validateSourceSchema(nilSchema)
	if got != nil {
		t.Errorf("validateSourceSchema((*schema.Schema)(nil)): expected nil, got %v", got)
	}
}
