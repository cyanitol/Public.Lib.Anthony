// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
)

// ============================================================================
// compileInSubquery coverage (compile_subquery.go)
// ============================================================================

// TestCompileInSubqueryBasic exercises the IN (SELECT ...) path end-to-end via
// the SQL engine, which causes compileInSubquery to be reached internally.
func TestCompileInSubqueryBasic(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE csq_t(x INTEGER)",
		"CREATE TABLE csq_t2(y INTEGER)",
		"INSERT INTO csq_t VALUES(1),(2),(3)",
		"INSERT INTO csq_t2 VALUES(1),(3)",
	})
	n := queryInt64(t, db, "SELECT x FROM csq_t WHERE x IN (SELECT y FROM csq_t2)")
	// We get the first row returned; just verify the query executes without error.
	_ = n
}

// TestCompileInSubqueryNoMatch exercises IN (SELECT ...) where nothing matches.
func TestCompileInSubqueryNoMatch(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE csq_a(x INTEGER)",
		"CREATE TABLE csq_b(y INTEGER)",
		"INSERT INTO csq_a VALUES(10),(20)",
		"INSERT INTO csq_b VALUES(99)",
	})
	rows, err := db.Query("SELECT x FROM csq_a WHERE x IN (SELECT y FROM csq_b)")
	if err != nil {
		t.Fatalf("IN subquery no-match: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		// consume rows without asserting count, query must not error
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
}

// TestCompileInSubqueryEmptySubquery exercises IN (SELECT ...) with empty subquery result.
func TestCompileInSubqueryEmptySubquery(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE csq_c(x INTEGER)",
		"CREATE TABLE csq_d(y INTEGER)",
		"INSERT INTO csq_c VALUES(1),(2)",
		// csq_d intentionally empty
	})
	rows, err := db.Query("SELECT x FROM csq_c WHERE x IN (SELECT y FROM csq_d)")
	if err != nil {
		t.Fatalf("IN subquery empty: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}
}

// ============================================================================
// fkActionToString coverage (stmt_ddl_additions.go)
// ============================================================================

// TestFkActionToStringAllCases calls fkActionToString with every known constant
// and verifies the returned string.
func TestFkActionToStringAllCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		action constraint.ForeignKeyAction
		want   string
	}{
		{constraint.FKActionCascade, "CASCADE"},
		{constraint.FKActionSetNull, "SET NULL"},
		{constraint.FKActionSetDefault, "SET DEFAULT"},
		{constraint.FKActionRestrict, "RESTRICT"},
		{constraint.FKActionNoAction, "NO ACTION"},
		// FKActionNone is not explicitly listed in the switch; it falls through to default.
		{constraint.FKActionNone, "NO ACTION"},
	}
	for _, tc := range cases {
		got := fkActionToString(tc.action)
		if got != tc.want {
			t.Errorf("fkActionToString(%v) = %q, want %q", tc.action, got, tc.want)
		}
	}
}

// TestFkActionToStringUnknown verifies the default branch for an out-of-range value.
func TestFkActionToStringUnknown(t *testing.T) {
	t.Parallel()
	got := fkActionToString(constraint.ForeignKeyAction(999))
	if got != "NO ACTION" {
		t.Errorf("fkActionToString(unknown) = %q, want %q", got, "NO ACTION")
	}
}

// ============================================================================
// compareAfterAffinityWithCollation coverage (stmt_ddl_additions.go)
// ============================================================================

// newTestRowReader builds a driverRowReader without a real connection, relying
// only on the fields accessed by compareAfterAffinityWithCollation (none).
func newTestRowReader() *driverRowReader {
	return &driverRowReader{conn: nil}
}

// TestCompareAfterAffinityWithCollationIntegers verifies integer comparison paths.
func TestCompareAfterAffinityWithCollationIntegers(t *testing.T) {
	t.Parallel()
	r := newTestRowReader()
	if !r.compareAfterAffinityWithCollation(int64(42), int64(42), "BINARY") {
		t.Error("equal int64 values must compare equal")
	}
	if r.compareAfterAffinityWithCollation(int64(1), int64(2), "BINARY") {
		t.Error("unequal int64 values must not compare equal")
	}
	// int type triggers toInt64Value int branch
	if !r.compareAfterAffinityWithCollation(int(7), int(7), "BINARY") {
		t.Error("equal int values must compare equal")
	}
}

// TestCompareAfterAffinityWithCollationFloats verifies float comparison paths.
func TestCompareAfterAffinityWithCollationFloats(t *testing.T) {
	t.Parallel()
	r := newTestRowReader()
	if !r.compareAfterAffinityWithCollation(float64(3.14), float64(3.14), "BINARY") {
		t.Error("equal float64 values must compare equal")
	}
	if r.compareAfterAffinityWithCollation(float64(1.0), float64(2.0), "BINARY") {
		t.Error("unequal float64 values must not compare equal")
	}
}

// TestCompareAfterAffinityWithCollationStrings verifies string comparison paths
// with both BINARY and NOCASE collations.
func TestCompareAfterAffinityWithCollationStrings(t *testing.T) {
	t.Parallel()
	r := newTestRowReader()
	if !r.compareAfterAffinityWithCollation("hello", "hello", "BINARY") {
		t.Error("identical strings must compare equal under BINARY")
	}
	if r.compareAfterAffinityWithCollation("abc", "ABC", "BINARY") {
		t.Error("different-case strings must not compare equal under BINARY")
	}
	if !r.compareAfterAffinityWithCollation("abc", "ABC", "NOCASE") {
		t.Error("same strings must compare equal under NOCASE")
	}
	if r.compareAfterAffinityWithCollation("hello", "world", "NOCASE") {
		t.Error("different strings must not compare equal under NOCASE")
	}
}

// TestCompareAfterAffinityWithCollationMixedTypes verifies fall-through to string
// comparison when types differ (non-numeric left side).
func TestCompareAfterAffinityWithCollationMixedTypes(t *testing.T) {
	t.Parallel()
	r := newTestRowReader()
	// nil is not int64/float64, falls through to string comparison via Sprintf
	if !r.compareAfterAffinityWithCollation(nil, nil, "BINARY") {
		t.Error("nil vs nil must compare equal via string fallback")
	}
}

// ============================================================================
// extractTableNameFromPragma coverage (stmt_ddl_additions.go)
// ============================================================================

// TestExtractTableNameFromPragmaNilValue covers the nil-value error branch.
func TestExtractTableNameFromPragmaNilValue(t *testing.T) {
	t.Parallel()
	stmt := &parser.PragmaStmt{Name: "table_info", Value: nil}
	_, err := extractTableNameFromPragma(stmt)
	if err == nil {
		t.Error("expected error for nil pragma value")
	}
}

// TestExtractTableNameFromPragmaLiteralExpr covers the *LiteralExpr branch.
func TestExtractTableNameFromPragmaLiteralExpr(t *testing.T) {
	t.Parallel()
	stmt := &parser.PragmaStmt{
		Name:  "table_info",
		Value: &parser.LiteralExpr{Value: "my_table"},
	}
	got, err := extractTableNameFromPragma(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "my_table" {
		t.Errorf("got %q, want %q", got, "my_table")
	}
}

// TestExtractTableNameFromPragmaIdentExpr covers the *IdentExpr branch.
func TestExtractTableNameFromPragmaIdentExpr(t *testing.T) {
	t.Parallel()
	stmt := &parser.PragmaStmt{
		Name:  "table_info",
		Value: &parser.IdentExpr{Name: "users"},
	}
	got, err := extractTableNameFromPragma(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "users" {
		t.Errorf("got %q, want %q", got, "users")
	}
}

// TestExtractTableNameFromPragmaInvalidExpr covers the unsupported expression type error branch.
func TestExtractTableNameFromPragmaInvalidExpr(t *testing.T) {
	t.Parallel()
	// BinaryExpr is neither LiteralExpr nor IdentExpr, triggering the error branch.
	stmt := &parser.PragmaStmt{
		Name:  "table_info",
		Value: &parser.BinaryExpr{},
	}
	_, err := extractTableNameFromPragma(stmt)
	if err == nil {
		t.Error("expected error for unsupported pragma value type")
	}
}
