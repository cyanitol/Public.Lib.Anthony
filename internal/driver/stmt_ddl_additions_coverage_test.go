// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openDDLCovDB opens a file-backed database for DDL-additions coverage tests.
// A file-backed database has a real btree, which enables the driverRowReader
// cursor path used by PRAGMA foreign_key_check.
func openDDLCovDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ddl_cov.db")
	db, err := sql.Open("sqlite_internal", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// ddlExec executes one or more SQL statements and fails the test on error.
func ddlExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// ddlExecErr executes a statement and returns any error without failing.
func ddlExecErr(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}

// ddlCountRows counts rows returned by a query, failing on query or scan error.
func ddlCountRows(t *testing.T, db *sql.DB, query string) int {
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
		t.Fatalf("rows.Err for %q: %v", query, err)
	}
	return n
}

// ddlQueryString queries a single string value.
func ddlQueryString(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// ---------------------------------------------------------------------------
// extractJournalModeValue – LiteralExpr branch
//
// The parser produces a *LiteralExpr when the pragma value is a quoted string.
// This exercises the first branch of extractJournalModeValue (line 605).
// Existing tests already cover the IdentExpr branch (unquoted WAL/DELETE etc).
// ---------------------------------------------------------------------------

func TestDDLAdditions_JournalMode_QuotedLiteralString(t *testing.T) {
	db := openDDLCovDB(t)

	// A quoted string ('WAL') parses to *parser.LiteralExpr, hitting the
	// lit.Value branch in extractJournalModeValue.
	rows, err := db.Query("PRAGMA journal_mode = 'WAL'")
	if err != nil {
		t.Fatalf("PRAGMA journal_mode = 'WAL': %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !strings.EqualFold(mode, "wal") {
			t.Errorf("journal_mode after 'WAL': want wal, got %q", mode)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

func TestDDLAdditions_JournalMode_QuotedDelete(t *testing.T) {
	db := openDDLCovDB(t)
	// Set WAL first, then go back to DELETE via quoted string.
	ddlExec(t, db, "PRAGMA journal_mode = WAL")

	rows, err := db.Query("PRAGMA journal_mode = 'DELETE'")
	if err != nil {
		t.Fatalf("PRAGMA journal_mode = 'DELETE': %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !strings.EqualFold(mode, "delete") {
			t.Errorf("journal_mode after 'DELETE': want delete, got %q", mode)
		}
	}
}

func TestDDLAdditions_JournalMode_QuotedMemory(t *testing.T) {
	db := openDDLCovDB(t)
	// 'MEMORY' as a quoted literal.
	rows, err := db.Query("PRAGMA journal_mode = 'MEMORY'")
	if err != nil {
		t.Fatalf("PRAGMA journal_mode = 'MEMORY': %v", err)
	}
	defer rows.Close()
	for rows.Next() {
	}
}

// ---------------------------------------------------------------------------
// emptyForeignKeyCheckResult
//
// emptyForeignKeyCheckResult is returned when s.conn.fkManager == nil.
// The normal connection path always creates an fkManager, so the fallback
// that exercises this is running PRAGMA foreign_key_check on a database
// with FK constraints registered but NO rows — the result must be empty.
//
// Note: the emitForeignKeyCheckResults path (fkManager != nil, 0 violations)
// exercises the empty-row output; emptyForeignKeyCheckResult (fkManager == nil)
// is unreachable from a standard sql.Open connection.  We include this test
// to document and exercise the nearest reachable code path.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FKCheck_EmptyTable_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE fkce_p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE fkce_c (id INTEGER, pid INTEGER REFERENCES fkce_p(id))",
	)

	// PRAGMA foreign_key_check on tables with no rows must return 0 violations.
	// This exercises compilePragmaForeignKeyCheck → FindViolations on a real btree.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 FK violations on empty tables, got %d", n)
	}
}

func TestDDLAdditions_FKCheck_EmptyTable_ByName(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE fkcen_p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE fkcen_c (id INTEGER, pid INTEGER REFERENCES fkcen_p(id))",
	)

	// Named table form: PRAGMA foreign_key_check(table) on empty child.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check(fkcen_c)")
	if n != 0 {
		t.Errorf("expected 0 FK violations on named empty table, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// driverRowReader file-backed scan path
//
// PRAGMA foreign_key_check on a file-backed database with actual rows exercises:
//   • initializeCursor (btree path, not the nil-btree early return)
//   • FindReferencingRows → collectMatchingRowids → checkRowMatch
//   • valuesEqualWithAffinity (for INTEGER/TEXT columns)
//   • getColumnValue (payload extraction)
//
// These are the reachable subset of the driverRowReader methods.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FKCheck_WithData_NoViolations(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE fkd_p (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE fkd_c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkd_p(id))",
		"INSERT INTO fkd_p VALUES(1, 'Alice')",
		"INSERT INTO fkd_p VALUES(2, 'Bob')",
		"INSERT INTO fkd_c VALUES(1, 1)",
		"INSERT INTO fkd_c VALUES(2, 2)",
	)

	// With all FKs satisfied, PRAGMA foreign_key_check scans child rows and
	// calls driverRowReader.RowExists for each to confirm no violations.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 FK violations, got %d", n)
	}
}

func TestDDLAdditions_FKCheck_WithData_MultipleRows(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE fkm_p (id INTEGER PRIMARY KEY)",
		"CREATE TABLE fkm_c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkm_p(id))",
		"INSERT INTO fkm_p VALUES(10)",
		"INSERT INTO fkm_p VALUES(20)",
		"INSERT INTO fkm_p VALUES(30)",
		"INSERT INTO fkm_c VALUES(1, 10)",
		"INSERT INTO fkm_c VALUES(2, 20)",
		"INSERT INTO fkm_c VALUES(3, 30)",
	)

	// Three child rows with valid parents — exercises collectMatchingRowids loop.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations with 3 valid child rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// RowExistsWithCollation / scanForMatchWithCollation / checkRowMatchWithCollation
// / valuesEqualWithCollation
//
// These driverRowReader methods are only called from validateReference during
// runtime FK enforcement (INSERT/UPDATE), where the rowReader is ConnRowReader
// (backed by VDBERowReader), not driverRowReader.  They are not exercised by
// PRAGMA foreign_key_check.  The tests below exercise the analogous SQL
// scenario (NOCASE parent column, FK insert with different case) to ensure the
// collation-aware FK logic is tested at the integration level, even though it
// runs through VDBERowReader rather than driverRowReader.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_NOCASEParent_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE ncp (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE ncc (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES ncp(id))",
		"INSERT INTO ncp VALUES('Hello')",
	)

	// Different-case child value must match NOCASE parent.
	if err := ddlExecErr(db, "INSERT INTO ncc VALUES(1, 'hello')"); err != nil {
		t.Errorf("NOCASE FK insert should succeed: %v", err)
	}
	if err := ddlExecErr(db, "INSERT INTO ncc VALUES(2, 'HELLO')"); err != nil {
		t.Errorf("NOCASE FK insert (uppercase) should succeed: %v", err)
	}

	// Non-matching value must fail.
	err := ddlExecErr(db, "INSERT INTO ncc VALUES(3, 'World')")
	if err == nil {
		t.Error("expected FK violation for non-matching value")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}

	// FK check on file-backed DB — scans real btree rows.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations, got %d", n)
	}
}

func TestDDLAdditions_FK_NOCASEParent_OnDeleteCascade_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE ncpc (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE nccc (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES ncpc(id) ON DELETE CASCADE)",
		"INSERT INTO ncpc VALUES('ABC')",
		"INSERT INTO nccc VALUES(1, 'abc')",
		"INSERT INTO nccc VALUES(2, 'Abc')",
	)

	// PRAGMA foreign_key_check before delete: 0 violations.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations before delete, got %d", n)
	}

	// Cascade delete exercises FindReferencingRowsWithParentAffinity via VDBERowReader.
	ddlExec(t, db, "DELETE FROM ncpc WHERE id = 'ABC'")

	after := ddlCountRows(t, db, "SELECT * FROM nccc")
	if after != 0 {
		t.Errorf("expected 0 child rows after cascade delete, got %d", after)
	}
}

func TestDDLAdditions_FK_NOCASEParent_OnUpdateCascade_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE ncpu (id TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE nccu (id INTEGER, pid TEXT COLLATE NOCASE REFERENCES ncpu(id) ON UPDATE CASCADE)",
		"INSERT INTO ncpu VALUES('Foo')",
		"INSERT INTO nccu VALUES(1, 'foo')",
	)

	// Cascade update: child pid should follow parent key change.
	ddlExec(t, db, "UPDATE ncpu SET id = 'Bar' WHERE id = 'Foo'")

	var pid string
	if err := db.QueryRow("SELECT pid FROM nccu WHERE id = 1").Scan(&pid); err != nil {
		t.Fatalf("SELECT pid: %v", err)
	}
	if !strings.EqualFold(pid, "Bar") {
		t.Errorf("expected child pid = Bar after cascade update, got %q", pid)
	}
}

// ---------------------------------------------------------------------------
// FindReferencingRowsWithParentAffinity / collectMatchingRowidsWithAffinity
// / checkRowMatchWithParentAffinity
//
// These are called via type assertion when the rowReader implements the
// affinityRowReader interface.  At runtime the rowReader is ConnRowReader
// (which delegates to VDBERowReader), not driverRowReader.  The tests below
// exercise the SQL scenario that causes these code paths to run.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_NUMERICAffinity_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE numpa (id NUMERIC PRIMARY KEY)",
		"CREATE TABLE numca (id INTEGER, pid NUMERIC REFERENCES numpa(id))",
		"INSERT INTO numpa VALUES(42)",
	)

	// Integer child value must match NUMERIC parent.
	if err := ddlExecErr(db, "INSERT INTO numca VALUES(1, 42)"); err != nil {
		t.Errorf("NUMERIC FK insert should succeed: %v", err)
	}

	// FK check on file-backed DB.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations, got %d", n)
	}
}

func TestDDLAdditions_FK_NUMERICAffinity_OnDeleteCascade_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE numpac (id NUMERIC PRIMARY KEY)",
		"CREATE TABLE numcac (id INTEGER, pid NUMERIC REFERENCES numpac(id) ON DELETE CASCADE)",
		"INSERT INTO numpac VALUES(10)",
		"INSERT INTO numcac VALUES(1, 10)",
		"INSERT INTO numcac VALUES(2, 10)",
	)

	// Verify 2 child rows before delete.
	n := ddlCountRows(t, db, "SELECT * FROM numcac")
	if n != 2 {
		t.Fatalf("expected 2 child rows before cascade delete, got %d", n)
	}

	// Cascade delete: exercises collectMatchingRowidsWithAffinity via VDBERowReader.
	ddlExec(t, db, "DELETE FROM numpac WHERE id = 10")

	after := ddlCountRows(t, db, "SELECT * FROM numcac")
	if after != 0 {
		t.Errorf("expected 0 child rows after cascade delete, got %d", after)
	}
}

func TestDDLAdditions_FK_INTEGERAffinity_OnUpdateCascade_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE intpu (id INTEGER PRIMARY KEY)",
		"CREATE TABLE intcu (id INTEGER, pid INTEGER REFERENCES intpu(id) ON UPDATE CASCADE)",
		"INSERT INTO intpu VALUES(1)",
		"INSERT INTO intcu VALUES(1, 1)",
		"INSERT INTO intcu VALUES(2, 1)",
	)

	// Update cascade exercises checkRowMatchWithParentAffinity via VDBERowReader.
	ddlExec(t, db, "UPDATE intpu SET id = 99 WHERE id = 1")

	n := ddlCountRows(t, db, "SELECT * FROM intcu WHERE pid = 99")
	if n != 2 {
		t.Errorf("expected 2 child rows with pid=99 after cascade update, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// valuesEqual
//
// valuesEqual is defined on driverRowReader but is not called from any code
// path in the current implementation (checkRowMatch calls valuesEqualWithAffinity
// instead).  It is dead code at the driverRowReader level.  This test exercises
// the nearest reachable FK check scenario to document the intent.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FKCheck_TextPK_NoViolations(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE textpk_p (id TEXT PRIMARY KEY)",
		"CREATE TABLE textpk_c (id INTEGER, pid TEXT REFERENCES textpk_p(id))",
		"INSERT INTO textpk_p VALUES('x1')",
		"INSERT INTO textpk_p VALUES('x2')",
		"INSERT INTO textpk_c VALUES(1, 'x1')",
		"INSERT INTO textpk_c VALUES(2, 'x2')",
	)

	// PRAGMA foreign_key_check scans child rows via driverRowReader.RowExists.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 FK violations, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// valuesEqualWithCollation
//
// valuesEqualWithCollation is called from checkRowMatchWithCollation, which is
// called from scanForMatchWithCollation, which is called from
// RowExistsWithCollation.  RowExistsWithCollation on driverRowReader is only
// invoked when the FK constraint package calls it via the RowReader interface
// — however, at runtime the rowReader passed to validateReference is
// ConnRowReader (backed by VDBERowReader), not driverRowReader.  The
// driverRowReader.RowExistsWithCollation path is therefore unreachable from
// SQL-level integration tests.
//
// The test below exercises the SQL intent: inserting a child row that must
// match the parent via NOCASE collation.  This reaches the analogous code in
// VDBERowReader.RowExistsWithCollation.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_NOCASECollation_InsertMatch(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE ncol_p (code TEXT COLLATE NOCASE PRIMARY KEY)",
		"CREATE TABLE ncol_c (id INTEGER, code TEXT COLLATE NOCASE REFERENCES ncol_p(code))",
		"INSERT INTO ncol_p VALUES('XYZ')",
	)

	// 'xyz' and 'XYZ' and 'Xyz' must all match under NOCASE.
	for _, v := range []string{"xyz", "XYZ", "Xyz"} {
		if err := ddlExecErr(db, "INSERT INTO ncol_c VALUES(1, '"+v+"')"); err != nil {
			t.Errorf("NOCASE insert %q should succeed: %v", v, err)
		}
		ddlExec(t, db, "DELETE FROM ncol_c")
	}

	// Non-matching code must fail.
	err := ddlExecErr(db, "INSERT INTO ncol_c VALUES(1, 'NOPE')")
	if err == nil {
		t.Error("expected FK violation for non-matching NOCASE code")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// toFloat64Value – REAL/float affinity path
//
// toFloat64Value is called from compareAfterAffinityWithCollation when the
// values are not int64-convertible.  With a REAL parent column, the stored
// value is float64, exercising the float64 branch.  The int64 branch is
// already hit by INTEGER FK checks.  The bare `int` branch is unreachable
// from database/sql because the driver always stores integers as int64.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_REALAffinity_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE realp (id REAL PRIMARY KEY)",
		"CREATE TABLE realc (id INTEGER, pid REAL REFERENCES realp(id))",
		"INSERT INTO realp VALUES(3.14)",
		"INSERT INTO realc VALUES(1, 3.14)",
	)

	// PRAGMA foreign_key_check triggers driverRowReader.RowExists →
	// checkRowMatch → valuesEqualWithAffinity →
	// compareAfterAffinityWithCollation (float64 branch of toFloat64Value).
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 FK violations for REAL FK, got %d", n)
	}
}

func TestDDLAdditions_FK_REALAffinity_OnDeleteCascade_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE realpc (id REAL PRIMARY KEY)",
		"CREATE TABLE realcc (id INTEGER, pid REAL REFERENCES realpc(id) ON DELETE CASCADE)",
		"INSERT INTO realpc VALUES(2.71)",
		"INSERT INTO realcc VALUES(1, 2.71)",
	)

	// Verify child row exists before cascade delete.
	n := ddlCountRows(t, db, "SELECT * FROM realcc")
	if n != 1 {
		t.Fatalf("expected 1 child row before delete, got %d", n)
	}

	// Delete parent — cascade removes child.
	ddlExec(t, db, "DELETE FROM realpc WHERE id = 2.71")

	after := ddlCountRows(t, db, "SELECT * FROM realcc")
	if after != 0 {
		t.Errorf("expected 0 child rows after cascade delete, got %d", after)
	}
}

// ---------------------------------------------------------------------------
// Multi-column FK — exercises multiple iterations in checkRowMatch and
// collectMatchingRowids loops on the file-backed btree path.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_MultiColumn_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE mcfp (a INTEGER, b TEXT COLLATE NOCASE, PRIMARY KEY(a, b))",
		"CREATE TABLE mcfc (id INTEGER, pa INTEGER, pb TEXT, FOREIGN KEY(pa, pb) REFERENCES mcfp(a, b))",
		"INSERT INTO mcfp VALUES(1, 'foo')",
		"INSERT INTO mcfp VALUES(2, 'bar')",
	)

	// Exact match.
	if err := ddlExecErr(db, "INSERT INTO mcfc VALUES(1, 1, 'foo')"); err != nil {
		t.Errorf("multi-column FK exact match should succeed: %v", err)
	}

	// NOCASE match on second column.
	if err := ddlExecErr(db, "INSERT INTO mcfc VALUES(2, 1, 'FOO')"); err != nil {
		t.Errorf("multi-column FK NOCASE match should succeed: %v", err)
	}

	// Non-matching first column must fail.
	err := ddlExecErr(db, "INSERT INTO mcfc VALUES(3, 99, 'foo')")
	if err == nil {
		t.Error("expected FK violation for non-matching first column")
	} else if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY error, got: %v", err)
	}

	// PRAGMA foreign_key_check on file-backed DB with multi-column FK.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations for valid multi-column FKs, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// PRAGMA journal_mode — ensure all standard modes round-trip via quoted string.
// This covers the LiteralExpr branch of extractJournalModeValue for each mode.
// ---------------------------------------------------------------------------

func TestDDLAdditions_JournalMode_AllModes_Quoted(t *testing.T) {
	modes := []string{"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "OFF", "WAL"}
	for _, mode := range modes {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			db := openDDLCovDB(t)
			q := "PRAGMA journal_mode = '" + mode + "'"
			rows, err := db.Query(q)
			if err != nil {
				t.Fatalf("query %q: %v", q, err)
			}
			defer rows.Close()
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PRAGMA journal_mode GET — exercises compilePragmaJournalModeGet /
// getCurrentJournalMode on a file-backed database.
// ---------------------------------------------------------------------------

func TestDDLAdditions_JournalMode_GetAfterSet(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db, "PRAGMA journal_mode = WAL")
	mode := ddlQueryString(t, db, "PRAGMA journal_mode")
	if !strings.EqualFold(mode, "wal") {
		t.Errorf("journal_mode after WAL: want wal, got %q", mode)
	}
}

// ---------------------------------------------------------------------------
// NULL FK value — NULL in referencing column must never be a FK violation.
// Exercises the nil-value early-return paths in checkRowMatch /
// valuesEqualWithAffinity.
// ---------------------------------------------------------------------------

func TestDDLAdditions_FK_NullChildValue_FileDB(t *testing.T) {
	db := openDDLCovDB(t)

	ddlExec(t, db,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE nullp (id INTEGER PRIMARY KEY)",
		"CREATE TABLE nullc (id INTEGER, pid INTEGER REFERENCES nullp(id))",
		"INSERT INTO nullp VALUES(1)",
		"INSERT INTO nullc VALUES(1, NULL)",
	)

	// NULL FK column: PRAGMA foreign_key_check must report 0 violations.
	n := ddlCountRows(t, db, "PRAGMA foreign_key_check")
	if n != 0 {
		t.Errorf("expected 0 violations for NULL FK, got %d", n)
	}
}
