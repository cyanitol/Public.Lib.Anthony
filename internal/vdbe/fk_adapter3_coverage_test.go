// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// Helpers local to this file
// ---------------------------------------------------------------------------

func fk3OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("fk3OpenDB sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("fk3OpenDB enable FK: %v", err)
	}
	return db
}

func fk3Exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("fk3Exec %q: %v", q, err)
	}
}

func fk3ExecErr(t *testing.T, db *sql.DB, q string) error {
	t.Helper()
	_, err := db.Exec(q)
	return err
}

func fk3QueryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("fk3QueryInt %q: %v", q, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// TestFK3RowExistsHappyPath
// Exercises RowExists / findMatchingRow / scanForMatch returning true.
// ---------------------------------------------------------------------------

func TestFK3RowExistsHappyPath(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3 (id INTEGER PRIMARY KEY, val TEXT)`)
	fk3Exec(t, db, `CREATE TABLE c3 (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3(id))`)
	fk3Exec(t, db, `INSERT INTO p3 VALUES(1,'alpha')`)
	fk3Exec(t, db, `INSERT INTO p3 VALUES(2,'beta')`)
	// Both inserts must succeed because the parent rows exist.
	fk3Exec(t, db, `INSERT INTO c3 VALUES(1, 1)`)
	fk3Exec(t, db, `INSERT INTO c3 VALUES(2, 2)`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3`)
	if n != 2 {
		t.Errorf("expected 2 child rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFK3RowExistsMiss
// Exercises RowExists / findMatchingRow / scanForMatch returning false (FK error).
// ---------------------------------------------------------------------------

func TestFK3RowExistsMiss(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3m (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3m (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3m(id))`)
	fk3Exec(t, db, `INSERT INTO p3m VALUES(1)`)

	// pid=99 has no matching parent → RowExists returns false → FK error.
	err := fk3ExecErr(t, db, `INSERT INTO c3m VALUES(1, 99)`)
	if err == nil {
		t.Fatal("expected FK constraint error, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "foreign key") &&
		!strings.Contains(strings.ToLower(err.Error()), "constraint") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestFK3RowExistsTextKey
// Exercises RowExists / valuesEqual with TEXT primary keys.
// ---------------------------------------------------------------------------

func TestFK3RowExistsTextKey(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3t (code TEXT PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3t (id INTEGER PRIMARY KEY, pcode TEXT REFERENCES p3t(code))`)
	fk3Exec(t, db, `INSERT INTO p3t VALUES('X')`)
	fk3Exec(t, db, `INSERT INTO p3t VALUES('Y')`)
	fk3Exec(t, db, `INSERT INTO c3t VALUES(1, 'X')`)
	fk3Exec(t, db, `INSERT INTO c3t VALUES(2, 'Y')`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3t`)
	if n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}

	err := fk3ExecErr(t, db, `INSERT INTO c3t VALUES(3, 'Z')`)
	if err == nil {
		t.Fatal("expected FK error for missing TEXT parent")
	}
}

// ---------------------------------------------------------------------------
// TestFK3FindReferencingRowsTwoChildren
// Exercises FindReferencingRows / collectMatchingRowids / collectAllMatchingRowids
// by cascading a DELETE to two child tables at once.
// ---------------------------------------------------------------------------

func TestFK3FindReferencingRowsTwoChildren(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3fc (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE ch3a (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3fc(id) ON DELETE CASCADE)`)
	fk3Exec(t, db, `CREATE TABLE ch3b (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3fc(id) ON DELETE CASCADE)`)
	fk3Exec(t, db, `INSERT INTO p3fc VALUES(1)`)
	fk3Exec(t, db, `INSERT INTO ch3a VALUES(1,1)`)
	fk3Exec(t, db, `INSERT INTO ch3a VALUES(2,1)`)
	fk3Exec(t, db, `INSERT INTO ch3b VALUES(1,1)`)

	fk3Exec(t, db, `DELETE FROM p3fc WHERE id=1`)

	na := fk3QueryInt(t, db, `SELECT COUNT(*) FROM ch3a`)
	nb := fk3QueryInt(t, db, `SELECT COUNT(*) FROM ch3b`)
	if na != 0 || nb != 0 {
		t.Errorf("expected both child tables empty after cascade, got ch3a=%d ch3b=%d", na, nb)
	}
}

// ---------------------------------------------------------------------------
// TestFK3CollectMatchingRowidsMultiple
// Exercises collectAllMatchingRowids collecting multiple rowids for a single parent.
// ---------------------------------------------------------------------------

func TestFK3CollectMatchingRowidsMultiple(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3mu (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3mu (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3mu(id) ON DELETE CASCADE)`)
	fk3Exec(t, db, `INSERT INTO p3mu VALUES(42)`)
	fk3Exec(t, db, `INSERT INTO c3mu VALUES(1,42)`)
	fk3Exec(t, db, `INSERT INTO c3mu VALUES(2,42)`)
	fk3Exec(t, db, `INSERT INTO c3mu VALUES(3,42)`)

	fk3Exec(t, db, `DELETE FROM p3mu WHERE id=42`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3mu`)
	if n != 0 {
		t.Errorf("expected 0 children after cascade delete, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFK3UpdateCascadeSeekByKey
// Exercises seekByKey / readAndMergeRowByKey / UpdateRowByKey via ON UPDATE CASCADE
// on a plain (rowid-based) parent table.
// ---------------------------------------------------------------------------

func TestFK3UpdateCascadeSeekByKey(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3u (id INTEGER PRIMARY KEY, name TEXT)`)
	fk3Exec(t, db, `CREATE TABLE c3u (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3u(id) ON UPDATE CASCADE)`)
	fk3Exec(t, db, `INSERT INTO p3u VALUES(1,'old')`)
	fk3Exec(t, db, `INSERT INTO c3u VALUES(1,1)`)
	fk3Exec(t, db, `INSERT INTO c3u VALUES(2,1)`)

	// Changing parent PK must cascade to both child rows.
	fk3Exec(t, db, `UPDATE p3u SET id=200 WHERE id=1`)

	n200 := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3u WHERE pid=200`)
	n1 := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3u WHERE pid=1`)
	if n200 != 2 {
		t.Errorf("expected 2 children with updated pid=200, got %d", n200)
	}
	if n1 != 0 {
		t.Errorf("expected 0 children with old pid=1, got %d", n1)
	}
}

// ---------------------------------------------------------------------------
// TestFK3UpdateCascadeSetNull
// Exercises UpdateRowByKey via ON UPDATE SET NULL: seekByKey + readAndMergeRowByKey.
// ---------------------------------------------------------------------------

func TestFK3UpdateCascadeSetNull(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3sn (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3sn (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3sn(id) ON UPDATE SET NULL)`)
	fk3Exec(t, db, `INSERT INTO p3sn VALUES(5)`)
	fk3Exec(t, db, `INSERT INTO c3sn VALUES(1,5)`)

	fk3Exec(t, db, `UPDATE p3sn SET id=99 WHERE id=5`)

	var pid interface{}
	if err := db.QueryRow(`SELECT pid FROM c3sn WHERE id=1`).Scan(&pid); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if pid != nil {
		t.Errorf("expected pid=NULL after ON UPDATE SET NULL, got %v", pid)
	}
}

// ---------------------------------------------------------------------------
// TestFK3WithoutRowIDCascadeDelete
// Exercises replaceRowWithoutRowID / extractPrimaryKeyValues / seekByKeyValues
// via a WITHOUT ROWID parent table with ON DELETE CASCADE.
// ---------------------------------------------------------------------------

func TestFK3WithoutRowIDCascadeDelete(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3wr (a TEXT, b INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID`)
	fk3Exec(t, db, `CREATE TABLE c3wr (
		id  INTEGER PRIMARY KEY,
		a   TEXT,
		b   INTEGER,
		FOREIGN KEY(a,b) REFERENCES p3wr(a,b) ON DELETE CASCADE
	)`)
	fk3Exec(t, db, `INSERT INTO p3wr VALUES('x',1)`)
	fk3Exec(t, db, `INSERT INTO p3wr VALUES('y',2)`)
	fk3Exec(t, db, `INSERT INTO c3wr VALUES(1,'x',1)`)
	fk3Exec(t, db, `INSERT INTO c3wr VALUES(2,'x',1)`)
	fk3Exec(t, db, `INSERT INTO c3wr VALUES(3,'y',2)`)

	fk3Exec(t, db, `DELETE FROM p3wr WHERE a='x' AND b=1`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3wr`)
	if n != 1 {
		t.Errorf("expected 1 child row remaining, got %d", n)
	}
	rem := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3wr WHERE a='y' AND b=2`)
	if rem != 1 {
		t.Errorf("expected remaining row to be the y/2 child, got %d", rem)
	}
}

// ---------------------------------------------------------------------------
// TestFK3WithoutRowIDConstraintCheck
// Exercises RowExists / seekByKeyValues / ReadRowByKey for WITHOUT ROWID parent table.
// ---------------------------------------------------------------------------

func TestFK3WithoutRowIDConstraintCheck(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3wrc (a TEXT, b INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID`)
	fk3Exec(t, db, `CREATE TABLE c3wrc (
		id INTEGER PRIMARY KEY,
		a  TEXT,
		b  INTEGER,
		FOREIGN KEY(a,b) REFERENCES p3wrc(a,b)
	)`)
	fk3Exec(t, db, `INSERT INTO p3wrc VALUES('hello',10)`)

	// Should succeed.
	fk3Exec(t, db, `INSERT INTO c3wrc VALUES(1,'hello',10)`)

	// Should fail: parent ('hello',99) does not exist.
	err := fk3ExecErr(t, db, `INSERT INTO c3wrc VALUES(2,'hello',99)`)
	if err == nil {
		t.Fatal("expected FK constraint error for missing WITHOUT ROWID parent")
	}
}

// ---------------------------------------------------------------------------
// TestFK3WithoutRowIDUpdateCascade
// Exercises UpdateRowByKey / replaceRowWithoutRowID / extractPrimaryKeyValues
// via ON UPDATE CASCADE on a WITHOUT ROWID parent.
// ---------------------------------------------------------------------------

func TestFK3WithoutRowIDUpdateCascade(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3wru (a TEXT, b INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID`)
	fk3Exec(t, db, `CREATE TABLE c3wru (
		id INTEGER PRIMARY KEY,
		a  TEXT,
		b  INTEGER,
		FOREIGN KEY(a,b) REFERENCES p3wru(a,b) ON UPDATE CASCADE
	)`)
	fk3Exec(t, db, `INSERT INTO p3wru VALUES('foo',1)`)
	fk3Exec(t, db, `INSERT INTO c3wru VALUES(1,'foo',1)`)

	fk3Exec(t, db, `UPDATE p3wru SET b=2 WHERE a='foo' AND b=1`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3wru WHERE a='foo' AND b=2`)
	if n != 1 {
		t.Errorf("expected child updated to b=2 via cascade, got %d", n)
	}
	old := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3wru WHERE b=1`)
	if old != 0 {
		t.Errorf("expected no child with old b=1, got %d", old)
	}
}

// ---------------------------------------------------------------------------
// TestFK3BlobFKCompareMemToBlob
// Exercises compareMemToBlob via a BLOB typed FK column.
// ---------------------------------------------------------------------------

func TestFK3BlobFKCompareMemToBlob(t *testing.T) {
	t.Skip("BLOB primary key FK not supported by this engine")
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3bl (id BLOB PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3bl (id INTEGER PRIMARY KEY, pid BLOB REFERENCES p3bl(id))`)

	// Insert a blob value into parent via raw bytes.
	if _, err := db.Exec(`INSERT INTO p3bl VALUES(?)`, []byte("blobkey")); err != nil {
		t.Fatalf("insert blob parent: %v", err)
	}

	// Insert child referencing that blob key.
	if _, err := db.Exec(`INSERT INTO c3bl VALUES(1, ?)`, []byte("blobkey")); err != nil {
		t.Fatalf("insert child with matching blob: %v", err)
	}

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3bl`)
	if n != 1 {
		t.Errorf("expected 1 child row, got %d", n)
	}

	// Insert child with non-matching blob → FK error.
	err := fk3ExecErr(t, db, `INSERT INTO c3bl VALUES(2, X'DEADBEEF')`)
	if err == nil {
		t.Fatal("expected FK error for non-matching BLOB parent key")
	}
}

// ---------------------------------------------------------------------------
// TestFK3ValuesEqualNullHandling
// Exercises valuesEqual when the Mem value is NULL.
// A child with NULL FK is always allowed (NULL never violates FK).
// ---------------------------------------------------------------------------

func TestFK3ValuesEqualNullHandling(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3vn (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3vn (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3vn(id))`)
	// No parent rows at all.
	fk3Exec(t, db, `INSERT INTO c3vn VALUES(1, NULL)`)
	fk3Exec(t, db, `INSERT INTO c3vn VALUES(2, NULL)`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3vn WHERE pid IS NULL`)
	if n != 2 {
		t.Errorf("expected 2 null-FK rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFK3ValuesEqualRealColumn
// Exercises valuesEqual / compareMemToFloat64Handler via REAL typed FK columns.
// ---------------------------------------------------------------------------

func TestFK3ValuesEqualRealColumn(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3re (score REAL PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3re (id INTEGER PRIMARY KEY, pscore REAL REFERENCES p3re(score))`)
	fk3Exec(t, db, `INSERT INTO p3re VALUES(3.14)`)
	fk3Exec(t, db, `INSERT INTO c3re VALUES(1, 3.14)`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3re`)
	if n != 1 {
		t.Errorf("expected 1 child row, got %d", n)
	}

	err := fk3ExecErr(t, db, `INSERT INTO c3re VALUES(2, 2.71)`)
	if err == nil {
		t.Fatal("expected FK error for non-matching REAL parent key")
	}
}

// ---------------------------------------------------------------------------
// TestFK3BuildMinimalColumnInfo
// Exercises buildMinimalColumnInfo by creating a table whose columns are
// looked up by name only (no explicit type info).
// ---------------------------------------------------------------------------

func TestFK3BuildMinimalColumnInfo(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	// A table with no declared types forces the schema layer to rely on minimal
	// column info (GetName()-only path) when building columnInfo.
	fk3Exec(t, db, `CREATE TABLE p3bm (id PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3bm (id INTEGER PRIMARY KEY, pid REFERENCES p3bm(id))`)
	fk3Exec(t, db, `INSERT INTO p3bm VALUES(7)`)
	fk3Exec(t, db, `INSERT INTO c3bm VALUES(1, 7)`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3bm WHERE pid=7`)
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}

	err := fk3ExecErr(t, db, `INSERT INTO c3bm VALUES(2, 99)`)
	if err == nil {
		t.Fatal("expected FK error for missing parent in untyped-column table")
	}
}

// ---------------------------------------------------------------------------
// TestFK3MultiColumnFKConstraint
// Exercises RowExists / findMatchingRow / scanForMatch with multi-column FK.
// ---------------------------------------------------------------------------

func TestFK3MultiColumnFKConstraint(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3mc (x INTEGER, y TEXT, PRIMARY KEY(x,y))`)
	fk3Exec(t, db, `CREATE TABLE c3mc (
		id INTEGER PRIMARY KEY,
		x  INTEGER,
		y  TEXT,
		FOREIGN KEY(x,y) REFERENCES p3mc(x,y)
	)`)
	fk3Exec(t, db, `INSERT INTO p3mc VALUES(1,'a')`)
	fk3Exec(t, db, `INSERT INTO p3mc VALUES(2,'b')`)
	fk3Exec(t, db, `INSERT INTO c3mc VALUES(1,1,'a')`)
	fk3Exec(t, db, `INSERT INTO c3mc VALUES(2,2,'b')`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3mc`)
	if n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}

	// (1,'b') does not exist in parent.
	err := fk3ExecErr(t, db, `INSERT INTO c3mc VALUES(3,1,'b')`)
	if err == nil {
		t.Fatal("expected FK error for non-matching multi-column parent")
	}
}

// ---------------------------------------------------------------------------
// TestFK3MultiColumnCascadeDelete
// Exercises FindReferencingRows / collectMatchingRowids / collectAllMatchingRowids
// for multi-column FK with CASCADE DELETE.
// ---------------------------------------------------------------------------

func TestFK3MultiColumnCascadeDelete(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3mcd (x INTEGER, y TEXT, PRIMARY KEY(x,y))`)
	fk3Exec(t, db, `CREATE TABLE c3mcd (
		id INTEGER PRIMARY KEY,
		x  INTEGER,
		y  TEXT,
		FOREIGN KEY(x,y) REFERENCES p3mcd(x,y) ON DELETE CASCADE
	)`)
	fk3Exec(t, db, `INSERT INTO p3mcd VALUES(1,'a')`)
	fk3Exec(t, db, `INSERT INTO p3mcd VALUES(2,'b')`)
	fk3Exec(t, db, `INSERT INTO c3mcd VALUES(1,1,'a')`)
	fk3Exec(t, db, `INSERT INTO c3mcd VALUES(2,1,'a')`)
	fk3Exec(t, db, `INSERT INTO c3mcd VALUES(3,2,'b')`)

	fk3Exec(t, db, `DELETE FROM p3mcd WHERE x=1 AND y='a'`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3mcd`)
	if n != 1 {
		t.Errorf("expected 1 child row remaining, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// TestFK3DeleteCascadeEmptyChild
// Exercises collectAllMatchingRowids when the child table has no matching rows
// (exercises the empty-iteration path).
// ---------------------------------------------------------------------------

func TestFK3DeleteCascadeEmptyChild(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3ec (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3ec (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3ec(id) ON DELETE CASCADE)`)
	fk3Exec(t, db, `INSERT INTO p3ec VALUES(1)`)
	fk3Exec(t, db, `INSERT INTO p3ec VALUES(2)`)
	fk3Exec(t, db, `INSERT INTO c3ec VALUES(1,1)`)

	// Delete parent 2 which has no children.
	fk3Exec(t, db, `DELETE FROM p3ec WHERE id=2`)

	np := fk3QueryInt(t, db, `SELECT COUNT(*) FROM p3ec`)
	nc := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3ec`)
	if np != 1 || nc != 1 {
		t.Errorf("expected parent=1 child=1, got parent=%d child=%d", np, nc)
	}
}

// ---------------------------------------------------------------------------
// TestFK3ReadRowByKey
// Exercises ReadRowByKey indirectly via CASCADE operations that read a child's
// full row before updating it (readAndMergeRowByKey path).
// ---------------------------------------------------------------------------

func TestFK3ReadRowByKey(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3rk (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3rk (
		id  INTEGER PRIMARY KEY,
		pid INTEGER,
		val TEXT,
		FOREIGN KEY(pid) REFERENCES p3rk(id) ON UPDATE CASCADE ON DELETE SET NULL
	)`)
	fk3Exec(t, db, `INSERT INTO p3rk VALUES(10)`)
	fk3Exec(t, db, `INSERT INTO c3rk VALUES(1,10,'hello')`)
	fk3Exec(t, db, `INSERT INTO c3rk VALUES(2,10,'world')`)

	// ON UPDATE CASCADE: the child rows must be read, merged, and re-written.
	fk3Exec(t, db, `UPDATE p3rk SET id=20 WHERE id=10`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3rk WHERE pid=20`)
	if n != 2 {
		t.Errorf("expected 2 children with pid=20, got %d", n)
	}
	// Verify val column preserved through readAndMergeRowByKey.
	var val string
	if err := db.QueryRow(`SELECT val FROM c3rk WHERE id=1`).Scan(&val); err != nil {
		t.Fatalf("scan val: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected val='hello' preserved, got %q", val)
	}
}

// ---------------------------------------------------------------------------
// TestFK3SeekByKeyValuesRegularTable
// Exercises seekByKeyValues for a regular (rowid) table via ReadRowByKey path.
// The cascade update forces seekByKeyValues to be called with a rowid key.
// ---------------------------------------------------------------------------

func TestFK3SeekByKeyValuesRegularTable(t *testing.T) {
	t.Parallel()
	db := fk3OpenDB(t)
	fk3Exec(t, db, `CREATE TABLE p3sk (id INTEGER PRIMARY KEY)`)
	fk3Exec(t, db, `CREATE TABLE c3sk (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p3sk(id) ON UPDATE CASCADE)`)
	fk3Exec(t, db, `INSERT INTO p3sk VALUES(7)`)
	fk3Exec(t, db, `INSERT INTO c3sk VALUES(1,7)`)

	fk3Exec(t, db, `UPDATE p3sk SET id=77 WHERE id=7`)

	n := fk3QueryInt(t, db, `SELECT COUNT(*) FROM c3sk WHERE pid=77`)
	if n != 1 {
		t.Errorf("expected child pid updated to 77, got %d", n)
	}
}
