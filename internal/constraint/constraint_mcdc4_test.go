// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// mcdc4OpenDB opens an in-memory database for MCDC4 SQL tests.
func mcdc4OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

// mcdc4Exec executes a statement, failing the test on error.
func mcdc4Exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// mcdc4ExecErr executes a statement and returns any error without failing.
func mcdc4ExecErr(t *testing.T, db *sql.DB, q string) error {
	t.Helper()
	_, err := db.Exec(q)
	return err
}

// mcdc4EnableFK enables foreign key enforcement on the connection.
func mcdc4EnableFK(t *testing.T, db *sql.DB) {
	t.Helper()
	mcdc4Exec(t, db, "PRAGMA foreign_keys = ON")
}

// mcdc4CountRows returns the row count for a table.
func mcdc4CountRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	// table is a test constant, not user input.
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count(%s): %v", table, err)
	}
	return n
}

// TestMCDC4_CascadeDelete_MultiLevel exercises cascadeDelete recursively
// through a three-level parent → child → grandchild hierarchy.
//
// Deleting the grandparent triggers:
//
//	cascadeDelete on "parent" rows  (validates multi-level cascade path in
//	cascadeDelete and validateDeleteRecursive).
func TestMCDC4_CascadeDelete_MultiLevel(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE grandparent(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE parent(
		id INTEGER PRIMARY KEY,
		gp_id INTEGER REFERENCES grandparent(id) ON DELETE CASCADE
	)`)
	mcdc4Exec(t, db, `CREATE TABLE child(
		id INTEGER PRIMARY KEY,
		p_id INTEGER REFERENCES parent(id) ON DELETE CASCADE
	)`)

	mcdc4Exec(t, db, `INSERT INTO grandparent VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO parent VALUES(10, 1)`)
	mcdc4Exec(t, db, `INSERT INTO child VALUES(100, 10)`)
	mcdc4Exec(t, db, `INSERT INTO child VALUES(101, 10)`)

	// Delete grandparent — cascades to parent, then parent cascade to child.
	mcdc4Exec(t, db, `DELETE FROM grandparent WHERE id = 1`)

	if got := mcdc4CountRows(t, db, "grandparent"); got != 0 {
		t.Errorf("grandparent: want 0 rows, got %d", got)
	}
	if got := mcdc4CountRows(t, db, "parent"); got != 0 {
		t.Errorf("parent: want 0 rows, got %d", got)
	}
	if got := mcdc4CountRows(t, db, "child"); got != 0 {
		t.Errorf("child: want 0 rows, got %d", got)
	}
}

// TestMCDC4_CascadeDelete_MultiRow exercises cascadeDelete with multiple
// child rows so the loop body is executed more than once per cascade.
func TestMCDC4_CascadeDelete_MultiRow(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE gp(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE par(
		id INTEGER PRIMARY KEY,
		gp_id INTEGER REFERENCES gp(id) ON DELETE CASCADE
	)`)

	mcdc4Exec(t, db, `INSERT INTO gp VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO par VALUES(1, 1)`)
	mcdc4Exec(t, db, `INSERT INTO par VALUES(2, 1)`)
	mcdc4Exec(t, db, `INSERT INTO par VALUES(3, 1)`)

	mcdc4Exec(t, db, `DELETE FROM gp WHERE id = 1`)

	if got := mcdc4CountRows(t, db, "par"); got != 0 {
		t.Errorf("par: want 0 rows, got %d", got)
	}
}

// TestMCDC4_DeleteCascade_WithoutRowID_Child exercises handleDeleteConstraint
// when the child table is WITHOUT ROWID, exercising the
// handleDeleteConstraintWithoutRowID code path and applyDeleteActionWithoutRowID
// with FKActionCascade.
func TestMCDC4_DeleteCascade_WithoutRowID_Child(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE p(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE c(
		a INTEGER,
		b INTEGER,
		p_id INTEGER REFERENCES p(id) ON DELETE CASCADE,
		PRIMARY KEY(a, b)
	) WITHOUT ROWID`)

	mcdc4Exec(t, db, `INSERT INTO p VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO c VALUES(1, 1, 1)`)
	mcdc4Exec(t, db, `INSERT INTO c VALUES(1, 2, 1)`)

	mcdc4Exec(t, db, `DELETE FROM p WHERE id = 1`)

	if got := mcdc4CountRows(t, db, "c"); got != 0 {
		t.Errorf("without-rowid child: want 0 rows, got %d", got)
	}
}

// TestMCDC4_DeleteSetNull exercises applyDeleteAction with FKActionSetNull.
// Deleting a parent row should set the child FK column to NULL.
func TestMCDC4_DeleteSetNull(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER REFERENCES par(id) ON DELETE SET NULL
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(11, 1)`)

	mcdc4Exec(t, db, `DELETE FROM par WHERE id = 1`)

	// Both child rows should now have par_id = NULL.
	var nullCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM chi WHERE par_id IS NULL`).Scan(&nullCount); err != nil {
		t.Fatalf("count null par_id: %v", err)
	}
	if nullCount != 2 {
		t.Errorf("SET NULL: want 2 null par_id rows, got %d", nullCount)
	}
}

// TestMCDC4_DeleteSetDefault exercises applyDeleteAction with FKActionSetDefault.
// Deleting a parent row should set the child FK column to its DEFAULT value.
func TestMCDC4_DeleteSetDefault(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER DEFAULT 99 REFERENCES par(id) ON DELETE SET DEFAULT
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO par VALUES(99)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)

	// Delete parent row 1; child par_id should revert to DEFAULT 99.
	mcdc4Exec(t, db, `DELETE FROM par WHERE id = 1`)

	var parID interface{}
	if err := db.QueryRow(`SELECT par_id FROM chi WHERE id = 10`).Scan(&parID); err != nil {
		t.Fatalf("select par_id: %v", err)
	}
	// par_id should now be 99 (the default).
	switch v := parID.(type) {
	case int64:
		if v != 99 {
			t.Errorf("SET DEFAULT: want par_id=99, got %d", v)
		}
	default:
		t.Errorf("SET DEFAULT: unexpected type %T value %v", parID, parID)
	}
}

// TestMCDC4_UpdateSetNull exercises applyUpdateAction with FKActionSetNull.
// Updating a parent PK should set the child FK column to NULL.
func TestMCDC4_UpdateSetNull(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER REFERENCES par(id) ON UPDATE SET NULL
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)

	mcdc4Exec(t, db, `UPDATE par SET id = 2 WHERE id = 1`)

	var parID interface{}
	if err := db.QueryRow(`SELECT par_id FROM chi WHERE id = 10`).Scan(&parID); err != nil {
		t.Fatalf("select par_id: %v", err)
	}
	if parID != nil {
		t.Errorf("ON UPDATE SET NULL: want par_id=nil, got %v", parID)
	}
}

// TestMCDC4_UpdateSetDefault exercises applyUpdateAction with FKActionSetDefault.
// Updating a parent PK should set the child FK column to its DEFAULT value.
func TestMCDC4_UpdateSetDefault(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER DEFAULT 0 REFERENCES par(id) ON UPDATE SET DEFAULT
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO par VALUES(0)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)

	mcdc4Exec(t, db, `UPDATE par SET id = 2 WHERE id = 1`)

	var parID interface{}
	if err := db.QueryRow(`SELECT par_id FROM chi WHERE id = 10`).Scan(&parID); err != nil {
		t.Fatalf("select par_id: %v", err)
	}
	switch v := parID.(type) {
	case int64:
		if v != 0 {
			t.Errorf("ON UPDATE SET DEFAULT: want par_id=0, got %d", v)
		}
	default:
		t.Errorf("ON UPDATE SET DEFAULT: unexpected type %T value %v", parID, parID)
	}
}

// TestMCDC4_ForeignKeyCheck_AllTables exercises findViolationsAllTables via
// PRAGMA foreign_key_check with multiple tables having FK constraints.
func TestMCDC4_ForeignKeyCheck_AllTables(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	// foreign_keys off so we can insert violations manually.

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER REFERENCES par(id)
	)`)
	mcdc4Exec(t, db, `CREATE TABLE par2(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi2(
		id INTEGER PRIMARY KEY,
		par2_id INTEGER REFERENCES par2(id)
	)`)

	// Insert child rows that reference non-existent parents (FK off).
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(1, 999)`)
	mcdc4Exec(t, db, `INSERT INTO chi2 VALUES(1, 888)`)

	// PRAGMA foreign_key_check (all tables) exercises findViolationsAllTables.
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count < 2 {
		t.Errorf("foreign_key_check: want at least 2 violations, got %d", count)
	}
}

// TestMCDC4_ForeignKeyCheck_SchemaMismatch exercises checkTableSchemaMismatch
// with a table that has constraints vs one that does not, ensuring the
// zero-constraints early-return path is reached.
func TestMCDC4_ForeignKeyCheck_SchemaMismatch(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()

	// Create a table with a FK — exercises checkTableSchemaMismatch with constraints.
	mcdc4Exec(t, db, `CREATE TABLE par3(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi3(
		id INTEGER PRIMARY KEY,
		par3_id INTEGER REFERENCES par3(id)
	)`)

	// PRAGMA foreign_key_check on chi3 (specific table): covers
	// findViolationsForTable → checkTableViolations path.
	rows, err := db.Query(`PRAGMA foreign_key_check(chi3)`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check(chi3): %v", err)
	}
	rows.Close()

	// PRAGMA foreign_key_check with no violations (all tables path).
	rows2, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check all: %v", err)
	}
	rows2.Close()
}

// TestMCDC4_DeleteRestrict_Error confirms that ON DELETE RESTRICT prevents
// deletion when a child row exists, hitting the restrict error path.
func TestMCDC4_DeleteRestrict_Error(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER REFERENCES par(id) ON DELETE RESTRICT
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)

	err := mcdc4ExecErr(t, db, `DELETE FROM par WHERE id = 1`)
	if err == nil {
		t.Error("ON DELETE RESTRICT: expected error when child references parent, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Errorf("ON DELETE RESTRICT: expected FK error, got: %v", err)
	}
}

// TestMCDC4_UpdateCascade_MultiLevel exercises applyUpdateAction with CASCADE
// through a two-level parent → child hierarchy.
func TestMCDC4_UpdateCascade_MultiLevel(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE par(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE chi(
		id INTEGER PRIMARY KEY,
		par_id INTEGER REFERENCES par(id) ON UPDATE CASCADE
	)`)

	mcdc4Exec(t, db, `INSERT INTO par VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(10, 1)`)
	mcdc4Exec(t, db, `INSERT INTO chi VALUES(11, 1)`)

	mcdc4Exec(t, db, `UPDATE par SET id = 2 WHERE id = 1`)

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM chi WHERE par_id = 2`).Scan(&count); err != nil {
		t.Fatalf("count cascaded: %v", err)
	}
	if count != 2 {
		t.Errorf("ON UPDATE CASCADE: want 2 child rows with par_id=2, got %d", count)
	}
}

// TestMCDC4_WithoutRowID_Restrict exercises applyDeleteActionWithoutRowID
// with FKActionRestrict — delete should fail when child references parent.
func TestMCDC4_WithoutRowID_Restrict(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE pr(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE cr(
		a INTEGER,
		b INTEGER,
		pr_id INTEGER REFERENCES pr(id) ON DELETE RESTRICT,
		PRIMARY KEY(a, b)
	) WITHOUT ROWID`)

	mcdc4Exec(t, db, `INSERT INTO pr VALUES(1)`)
	mcdc4Exec(t, db, `INSERT INTO cr VALUES(1, 1, 1)`)

	err := mcdc4ExecErr(t, db, `DELETE FROM pr WHERE id = 1`)
	if err == nil {
		t.Error("ON DELETE RESTRICT (without rowid child): expected error, got nil")
	}
}

// TestMCDC4_ForeignKeyCheck_WithViolation exercises findViolationsAllTables
// when rows exist that violate FK constraints, walking to the violations path.
func TestMCDC4_ForeignKeyCheck_WithViolation(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	// Keep FK off so we can insert bad data.

	mcdc4Exec(t, db, `CREATE TABLE pv(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE cv(
		id INTEGER PRIMARY KEY,
		pv_id INTEGER REFERENCES pv(id)
	)`)

	// Insert a child row that has no matching parent.
	mcdc4Exec(t, db, `INSERT INTO cv VALUES(1, 42)`)

	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check: %v", err)
	}
	defer rows.Close()

	var found int
	for rows.Next() {
		found++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if found == 0 {
		t.Error("foreign_key_check: expected at least one violation row")
	}
}

// TestMCDC4_DeleteCascade_NoChildRows exercises the early-return path in
// applyDeleteAction when no referencing rows exist (len==0 branch).
func TestMCDC4_DeleteCascade_NoChildRows(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()
	mcdc4EnableFK(t, db)

	mcdc4Exec(t, db, `CREATE TABLE pnc(id INTEGER PRIMARY KEY)`)
	mcdc4Exec(t, db, `CREATE TABLE cnc(
		id INTEGER PRIMARY KEY,
		pnc_id INTEGER REFERENCES pnc(id) ON DELETE CASCADE
	)`)

	mcdc4Exec(t, db, `INSERT INTO pnc VALUES(1)`)
	// No child rows — delete parent should succeed without cascade.
	mcdc4Exec(t, db, `DELETE FROM pnc WHERE id = 1`)

	if got := mcdc4CountRows(t, db, "pnc"); got != 0 {
		t.Errorf("pnc: want 0 rows, got %d", got)
	}
}

// TestMCDC4_CheckSchemaMismatch_NoConstraintsTable exercises the early-return
// in checkTableSchemaMismatch when called for a table with no FK constraints.
// This is reached via PRAGMA foreign_key_check on a table with no constraints.
func TestMCDC4_CheckSchemaMismatch_NoConstraintsTable(t *testing.T) {
	db := mcdc4OpenDB(t)
	defer db.Close()

	// Table with no FK constraints.
	mcdc4Exec(t, db, `CREATE TABLE plain(id INTEGER PRIMARY KEY, val TEXT)`)
	mcdc4Exec(t, db, `INSERT INTO plain VALUES(1, 'hello')`)

	// PRAGMA foreign_key_check on plain triggers checkTableSchemaMismatch with
	// len(constraints)==0 early return for "plain".
	rows, err := db.Query(`PRAGMA foreign_key_check(plain)`)
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check(plain): %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}
