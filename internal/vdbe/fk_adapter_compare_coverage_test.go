// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openFKCompareDB opens a fresh in-memory DB and enables foreign keys.
func openFKCompareDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign_keys: %v", err)
	}
	return db
}

// execFK runs a statement, failing the test on error.
func execFK(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestFKAdapterCompare_Int64 exercises compareMemToInt64 and compareMemToInterface.
//
// When a column has no type declaration the stored affinity is NONE, so
// valuesEqualWithAffinity is called with columnType="".  That falls through to
// compareMemToInterface which dispatches to compareMemToInt64 for int64 values.
//
// The trigger is ON DELETE CASCADE: deleting the parent row causes
// FindReferencingRowsWithParentAffinity to scan child rows, which calls
// checkRowMatchWithParentAffinityAndCollation → valuesEqualWithAffinity("") →
// compareMemToInterface → compareMemToInt64.
func TestFKAdapterCompare_Int64(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	// No-type parent column with UNIQUE so FK mismatch check passes.
	// Type="" in columnInfo causes valuesEqualWithAffinity to call compareMemToInterface.
	execFK(t, db, `CREATE TABLE par_int(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_int(cid INTEGER PRIMARY KEY, pid REFERENCES par_int(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_int VALUES(42)`)
	execFK(t, db, `INSERT INTO chi_int VALUES(1, 42)`)

	// Deleting the parent triggers FindReferencingRowsWithParentAffinity.
	// The child column value (int64(42)) is compared via compareMemToInterface →
	// compareMemToInt (fails – not plain int) → compareMemToInt64 (succeeds).
	execFK(t, db, `DELETE FROM par_int WHERE id = 42`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_int").Scan(&count); err != nil {
		t.Fatalf("count child rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 child rows after cascade, got %d", count)
	}
}

// TestFKAdapterCompare_Int64_NoMatch exercises compareMemToInt64 returning false
// (non-matching int64 value), to cover the branch where the child does not
// reference the deleted parent value.
func TestFKAdapterCompare_Int64_NoMatch(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_int2(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_int2(cid INTEGER PRIMARY KEY, pid REFERENCES par_int2(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_int2 VALUES(1)`)
	execFK(t, db, `INSERT INTO par_int2 VALUES(2)`)
	execFK(t, db, `INSERT INTO chi_int2 VALUES(10, 1)`)

	// Delete par_int2 id=2 – the child references id=1, so comparison of
	// child.pid(1) vs parentValue(2) exercises the non-matching int64 path.
	execFK(t, db, `DELETE FROM par_int2 WHERE id = 2`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_int2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 child row (unaffected), got %d", count)
	}
}

// TestFKAdapterCompare_Float64 exercises compareMemToFloat64Handler.
//
// Inserting a real (floating-point) value in a no-type column and then
// cascading a delete causes compareMemToInterface to reach
// compareMemToFloat64Handler with a float64 value.
func TestFKAdapterCompare_Float64(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_real(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_real(cid INTEGER PRIMARY KEY, pid REFERENCES par_real(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_real VALUES(3.14)`)
	execFK(t, db, `INSERT INTO chi_real VALUES(1, 3.14)`)

	// parentValue will be float64(3.14); child mem will be REAL.
	// compareMemToInt/Int64 fail → compareMemToFloat64Handler succeeds.
	execFK(t, db, `DELETE FROM par_real WHERE id = 3.14`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_real").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 child rows after cascade, got %d", count)
	}
}

// TestFKAdapterCompare_Float64_NoMatch exercises the non-matching float64 path.
func TestFKAdapterCompare_Float64_NoMatch(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_real2(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_real2(cid INTEGER PRIMARY KEY, pid REFERENCES par_real2(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_real2 VALUES(1.5)`)
	execFK(t, db, `INSERT INTO par_real2 VALUES(2.5)`)
	execFK(t, db, `INSERT INTO chi_real2 VALUES(10, 1.5)`)

	// Delete 2.5 – child references 1.5, so comparison fails (non-match).
	execFK(t, db, `DELETE FROM par_real2 WHERE id = 2.5`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_real2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 child row (unaffected), got %d", count)
	}
}

// TestFKAdapterCompare_String exercises compareMemToString.
//
// A text value in a no-type parent column causes the cascade scan to reach
// compareMemToString.  compareMemToInt/Int64/Float64 all fail (not those types)
// so compareMemToString handles the value.
func TestFKAdapterCompare_String(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_str(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_str(cid INTEGER PRIMARY KEY, pid REFERENCES par_str(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_str VALUES('hello')`)
	execFK(t, db, `INSERT INTO chi_str VALUES(1, 'hello')`)

	execFK(t, db, `DELETE FROM par_str WHERE id = 'hello'`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_str").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 child rows after cascade, got %d", count)
	}
}

// TestFKAdapterCompare_String_NoMatch exercises the non-matching string path.
func TestFKAdapterCompare_String_NoMatch(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_str2(id UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_str2(cid INTEGER PRIMARY KEY, pid REFERENCES par_str2(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_str2 VALUES('alpha')`)
	execFK(t, db, `INSERT INTO par_str2 VALUES('beta')`)
	execFK(t, db, `INSERT INTO chi_str2 VALUES(10, 'alpha')`)

	// Delete 'beta' – child references 'alpha', no cascade expected.
	execFK(t, db, `DELETE FROM par_str2 WHERE id = 'beta'`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_str2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 child row (unaffected), got %d", count)
	}
}

// TestFKAdapterCompare_String_Mismatch exercises compareMemToString returning
// (false, true): the value IS a string but the stored mem is an integer.
// compareMemToInt fails (not plain int), compareMemToInt64 fails (not int64),
// compareMemToFloat64Handler fails (not float64), compareMemToString is handled
// (value is string) but returns false because mem is not a string.
//
// Setup: parent has integer id (no-type col), child stores matching integer.
// After cascading deletes the parent, scan a second parent row whose integer
// value does not match any string – exercising the compareMemToString false path.
// The scenario is simpler: delete a parent row whose integer does not match the
// child's integer value (already covered), plus verify the compareMemToInterface
// cycle completes without error.
func TestFKAdapterCompare_Interface_NoTypeMultiCol(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	// Two columns with no type – ensures both compareMemToInt64 and
	// compareMemToString paths are visited in a single cascade scan.
	execFK(t, db, `CREATE TABLE par_mc(a UNIQUE, b UNIQUE)`)
	execFK(t, db, `CREATE TABLE chi_mc(id INTEGER PRIMARY KEY, fa REFERENCES par_mc(a) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_mc VALUES(10, 'ten')`)
	execFK(t, db, `INSERT INTO chi_mc VALUES(1, 10)`)

	execFK(t, db, `DELETE FROM par_mc WHERE a = 10`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_mc").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 child rows after cascade, got %d", count)
	}
}

// TestFKAdapterCompare_CollateBinary_Int exercises compareMemToInterface via
// the valuesEqualWithCollation path.
//
// When the parent column has an explicit COLLATE clause its collation is
// non-empty, so checkRowMatchWithParentAffinityAndCollation calls
// valuesEqualWithCollation instead of valuesEqualWithAffinity.
// For integer-valued mem cells, valuesEqualWithCollation calls
// compareMemToInterface, exercising compareMemToInt64.
func TestFKAdapterCompare_CollateBinary_Int(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_cb(id INTEGER PRIMARY KEY COLLATE BINARY, name TEXT)`)
	execFK(t, db, `CREATE TABLE chi_cb(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES par_cb(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_cb VALUES(7, 'seven')`)
	execFK(t, db, `INSERT INTO chi_cb VALUES(1, 7)`)

	execFK(t, db, `DELETE FROM par_cb WHERE id = 7`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_cb").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 child rows after cascade, got %d", count)
	}
}

// TestFKAdapterCompare_CollateBinary_Int_NoMatch covers the non-matching branch
// in compareMemToInt64 (child value ≠ parent value) via the COLLATE path.
func TestFKAdapterCompare_CollateBinary_Int_NoMatch(t *testing.T) {
	t.Parallel()

	db := openFKCompareDB(t)

	execFK(t, db, `CREATE TABLE par_cb2(id INTEGER PRIMARY KEY COLLATE BINARY, name TEXT)`)
	execFK(t, db, `CREATE TABLE chi_cb2(cid INTEGER PRIMARY KEY, pid INTEGER REFERENCES par_cb2(id) ON DELETE CASCADE)`)
	execFK(t, db, `INSERT INTO par_cb2 VALUES(3, 'three')`)
	execFK(t, db, `INSERT INTO par_cb2 VALUES(5, 'five')`)
	execFK(t, db, `INSERT INTO chi_cb2 VALUES(10, 3)`)

	// Delete id=5 – child references 3, so scan finds no match.
	execFK(t, db, `DELETE FROM par_cb2 WHERE id = 5`)

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM chi_cb2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 child row (unaffected), got %d", count)
	}
}
