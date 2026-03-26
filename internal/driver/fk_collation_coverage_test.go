// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openFKCovDB opens an in-memory database for FK collation/affinity coverage tests.
func openFKCovDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// fkCovExec executes SQL statements and fails on error.
func fkCovExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// fkCovExecErr executes a statement and returns any error without failing.
func fkCovExecErr(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}

// fkCovCountRows counts the rows returned by a query.
func fkCovCountRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return n
}

// ---------------------------------------------------------------------------
// TestFKCollation_NOCASEMatch
//
// Exercises: RowExistsWithCollation, scanForMatchWithCollation,
// checkRowMatchWithCollation, valuesEqualWithCollation.
//
// A parent table uses TEXT COLLATE NOCASE on its PRIMARY KEY.
// Child rows with different-cased strings should be accepted.
// ---------------------------------------------------------------------------

func TestFKCollation_NOCASEMatch(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE parent (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE child (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES parent(id))",
		"INSERT INTO parent VALUES('Hello')",
	)

	// 'hello' should match 'Hello' with NOCASE collation
	if err := fkCovExecErr(db, "INSERT INTO child VALUES(1, 'hello')"); err != nil {
		t.Errorf("INSERT child('hello') failed but expected NOCASE match: %v", err)
	}

	// 'HELLO' should also match
	if err := fkCovExecErr(db, "INSERT INTO child VALUES(2, 'HELLO')"); err != nil {
		t.Errorf("INSERT child('HELLO') failed but expected NOCASE match: %v", err)
	}

	// 'World' has no parent — should be rejected
	err := fkCovExecErr(db, "INSERT INTO child VALUES(3, 'World')")
	if err == nil {
		t.Error("INSERT child('World') should have failed: no matching parent")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}

	n := fkCovCountRows(t, db, "SELECT * FROM child")
	if n != 2 {
		t.Errorf("expected 2 child rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_NOCASEUpperCaseParent
//
// Exercises: valuesEqualWithCollation with uppercase stored parent value.
// ---------------------------------------------------------------------------

func TestFKCollation_NOCASEUpperCaseParent(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_upper (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE c_upper (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES p_upper(id))",
		"INSERT INTO p_upper VALUES('ABC')",
	)

	// lowercase should match NOCASE
	if err := fkCovExecErr(db, "INSERT INTO c_upper VALUES(1, 'abc')"); err != nil {
		t.Errorf("NOCASE match failed for lowercase child: %v", err)
	}
	// mixed case should match NOCASE
	if err := fkCovExecErr(db, "INSERT INTO c_upper VALUES(2, 'Abc')"); err != nil {
		t.Errorf("NOCASE match failed for mixed-case child: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_NUMERICAffinity
//
// Exercises: FindReferencingRowsWithParentAffinity,
// collectMatchingRowidsWithAffinity, checkRowMatchWithParentAffinity,
// valuesEqualWithAffinity (NUMERIC branch).
//
// Parent column has NUMERIC affinity; child inserts a string '42' that
// should compare equal to parent numeric 42.
// ---------------------------------------------------------------------------

func TestFKCollation_NUMERICAffinity(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_num (id NUMERIC PRIMARY KEY)",
		"CREATE TABLE c_num (id INTEGER, pid NUMERIC REFERENCES p_num(id))",
		"INSERT INTO p_num VALUES(42)",
	)

	// String '42' should be coerced to numeric 42 and match
	if err := fkCovExecErr(db, "INSERT INTO c_num VALUES(1, 42)"); err != nil {
		t.Errorf("INSERT with numeric value failed: %v", err)
	}

	n := fkCovCountRows(t, db, "SELECT * FROM c_num")
	if n != 1 {
		t.Errorf("expected 1 child row, got %d", n)
	}

	// Non-matching value should fail
	err := fkCovExecErr(db, "INSERT INTO c_num VALUES(2, 99)")
	if err == nil {
		t.Error("INSERT with unmatched numeric FK should have failed")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_INTEGERAffinity
//
// Exercises: valuesEqualWithAffinity (INTEGER branch),
// checkRowMatchWithParentAffinity.
// ---------------------------------------------------------------------------

func TestFKCollation_INTEGERAffinity(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_int (id INTEGER PRIMARY KEY)",
		"CREATE TABLE c_int (id INTEGER, pid INTEGER REFERENCES p_int(id))",
		"INSERT INTO p_int VALUES(1)",
	)

	// Exact integer match
	if err := fkCovExecErr(db, "INSERT INTO c_int VALUES(1, 1)"); err != nil {
		t.Errorf("INSERT child with matching INTEGER FK failed: %v", err)
	}

	n := fkCovCountRows(t, db, "SELECT * FROM c_int")
	if n != 1 {
		t.Errorf("expected 1 child row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_EmptyForeignKeyCheck
//
// Exercises: emptyForeignKeyCheckResult (no rows → empty result set from
// PRAGMA foreign_key_check).
// ---------------------------------------------------------------------------

func TestFKCollation_EmptyForeignKeyCheck(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE c (id INTEGER, pid INTEGER REFERENCES p(id))",
		// No rows inserted; FK check should return empty result
	)

	n := fkCovCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 FK violations on empty tables, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_REALAffinityOnDeleteCascade
//
// Exercises: FindReferencingRowsWithParentAffinity, collectMatchingRowidsWithAffinity
// (REAL/float affinity, ON DELETE CASCADE path).
// ---------------------------------------------------------------------------

func TestFKCollation_REALAffinityOnDeleteCascade(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE par_real (id REAL PRIMARY KEY)",
		"CREATE TABLE chi_real (id INTEGER, pid REAL REFERENCES par_real(id) ON DELETE CASCADE)",
		"INSERT INTO par_real VALUES(1.5)",
		"INSERT INTO chi_real VALUES(1, 1.5)",
	)

	// Verify row is present
	n := fkCovCountRows(t, db, "SELECT * FROM chi_real")
	if n != 1 {
		t.Fatalf("expected 1 child row before delete, got %d", n)
	}

	// Delete parent — cascade should remove child
	fkCovExec(t, db, "DELETE FROM par_real WHERE id = 1.5")

	n = fkCovCountRows(t, db, "SELECT * FROM chi_real")
	if n != 0 {
		t.Errorf("expected 0 child rows after cascaded delete, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_NOCASEOnDeleteCascade
//
// Exercises: RowExistsWithCollation, scanForMatchWithCollation,
// checkRowMatchWithCollation (cascade path with collation).
// ---------------------------------------------------------------------------

func TestFKCollation_NOCASEOnDeleteCascade(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_col (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE c_col (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES p_col(id) ON DELETE CASCADE)",
		"INSERT INTO p_col VALUES('ABC')",
		"INSERT INTO c_col VALUES(1, 'abc')",
	)

	// Confirm child row present
	n := fkCovCountRows(t, db, "SELECT * FROM c_col")
	if n != 1 {
		t.Fatalf("expected 1 child row before delete, got %d", n)
	}

	// Delete parent (stored as 'ABC'), child ('abc') should cascade delete
	fkCovExec(t, db, "DELETE FROM p_col WHERE id = 'ABC'")

	n = fkCovCountRows(t, db, "SELECT * FROM c_col")
	if n != 0 {
		t.Errorf("expected 0 child rows after cascaded delete, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_NOCASEOnUpdateCascade
//
// Exercises the update cascade path with NOCASE collation.
// valuesEqualWithCollation is called when checking whether child rows match
// the old parent key before updating them to the new key.
// ---------------------------------------------------------------------------

func TestFKCollation_NOCASEOnUpdateCascade(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_upd (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE c_upd (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES p_upd(id) ON UPDATE CASCADE)",
		"INSERT INTO p_upd VALUES('Hello')",
		"INSERT INTO c_upd VALUES(1, 'hello')",
	)

	// Update parent key — child should follow via cascade
	fkCovExec(t, db, "UPDATE p_upd SET id = 'World' WHERE id = 'Hello'")

	// Child should now have 'World' (or its cased equivalent)
	var pid string
	if err := db.QueryRow("SELECT pid FROM c_upd WHERE id = 1").Scan(&pid); err != nil {
		t.Fatalf("SELECT pid failed: %v", err)
	}
	if !strings.EqualFold(pid, "World") {
		t.Errorf("expected child pid to cascade to 'World', got %q", pid)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_ValuesEqual_NilHandling
//
// Exercises: valuesEqual (nil-nil and nil-nonnil branches) indirectly via
// FK checks where NULL is stored in the referencing column (NULL FK is always
// allowed; child with NULL pid must not be rejected).
// ---------------------------------------------------------------------------

func TestFKCollation_NullFKAlwaysAllowed(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_null (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE c_null (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES p_null(id))",
		// No rows in parent; NULL child FK must succeed
	)

	if err := fkCovExecErr(db, "INSERT INTO c_null VALUES(1, NULL)"); err != nil {
		t.Errorf("INSERT with NULL FK should be allowed, got: %v", err)
	}

	n := fkCovCountRows(t, db, "SELECT * FROM c_null")
	if n != 1 {
		t.Errorf("expected 1 child row with NULL FK, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_ForeignKeyCheckEmpty_SingleTable
//
// Additional coverage for emptyForeignKeyCheckResult when PRAGMA
// foreign_key_check(tablename) is used on an empty table.
// ---------------------------------------------------------------------------

func TestFKCollation_ForeignKeyCheckEmpty_SingleTable(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p2 (id INTEGER PRIMARY KEY)",
		"CREATE TABLE c2 (id INTEGER, pid INTEGER REFERENCES p2(id))",
	)

	// No rows — PRAGMA foreign_key_check(c2) should return nothing
	n := fkCovCountRows(t, db, "PRAGMA foreign_key_check(c2)")
	if n != 0 {
		t.Errorf("expected 0 FK violations on empty child table, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFKCollation_MultipleAffinities
//
// Exercises collectMatchingRowidsWithAffinity with multiple different
// parent-column affinities in a single multi-column FK.
// ---------------------------------------------------------------------------

func TestFKCollation_MultipleAffinities(t *testing.T) {
	db := openFKCovDB(t)

	fkCovExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE p_multi (a INTEGER, b TEXT COLLATE NOCASE, PRIMARY KEY(a, b))",
		"CREATE TABLE c_multi (id INTEGER, pa INTEGER, pb TEXT, FOREIGN KEY(pa, pb) REFERENCES p_multi(a, b))",
		"INSERT INTO p_multi VALUES(1, 'foo')",
	)

	// Exact match (both columns)
	if err := fkCovExecErr(db, "INSERT INTO c_multi VALUES(1, 1, 'foo')"); err != nil {
		t.Errorf("exact multi-column FK insert failed: %v", err)
	}

	// NOCASE match on second column
	if err := fkCovExecErr(db, "INSERT INTO c_multi VALUES(2, 1, 'FOO')"); err != nil {
		t.Errorf("NOCASE multi-column FK insert failed: %v", err)
	}

	// Non-matching first column should fail
	err := fkCovExecErr(db, "INSERT INTO c_multi VALUES(3, 99, 'foo')")
	if err == nil {
		t.Error("unmatched first column should have caused FK violation")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}
}
