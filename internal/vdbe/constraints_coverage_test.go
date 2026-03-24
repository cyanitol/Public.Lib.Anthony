// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func constraintsOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func constraintsExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func constraintsExecErr(t *testing.T, db *sql.DB, q string) error {
	t.Helper()
	_, err := db.Exec(q)
	return err
}

// TestConstraintsCheckGT exercises evaluateCheckConstraint / evalBinaryCheck /
// compareCheckValues / toFloat / literalToNumber / memToNumber via CHECK(a > 0).
func TestConstraintsCheckGT(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > 0))")

	// Valid insert passes.
	constraintsExec(t, db, "INSERT INTO t VALUES(1)")
	constraintsExec(t, db, "INSERT INTO t VALUES(100)")

	// Zero violates a > 0.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0)"); err == nil {
		t.Error("expected CHECK constraint error for a=0")
	}

	// Negative violates a > 0.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(-5)"); err == nil {
		t.Error("expected CHECK constraint error for a=-5")
	}
}

// TestConstraintsCheckGE exercises CHECK(a >= 1).
func TestConstraintsCheckGE(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a >= 1))")
	constraintsExec(t, db, "INSERT INTO t VALUES(1)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0)"); err == nil {
		t.Error("expected CHECK constraint error for a=0, CHECK(a>=1)")
	}
}

// TestConstraintsCheckLT exercises CHECK(a < 100).
func TestConstraintsCheckLT(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a < 100))")
	constraintsExec(t, db, "INSERT INTO t VALUES(50)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(100)"); err == nil {
		t.Error("expected CHECK constraint error for a=100, CHECK(a<100)")
	}
}

// TestConstraintsCheckLE exercises CHECK(a <= 10).
func TestConstraintsCheckLE(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a <= 10))")
	constraintsExec(t, db, "INSERT INTO t VALUES(10)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(11)"); err == nil {
		t.Error("expected CHECK constraint error for a=11, CHECK(a<=10)")
	}
}

// TestConstraintsCheckEQ exercises CHECK(a = 5).
func TestConstraintsCheckEQ(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a = 5))")
	constraintsExec(t, db, "INSERT INTO t VALUES(5)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(6)"); err == nil {
		t.Error("expected CHECK constraint error for a=6, CHECK(a=5)")
	}
}

// TestConstraintsCheckNE exercises CHECK(a != 0).
func TestConstraintsCheckNE(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a != 0))")
	constraintsExec(t, db, "INSERT INTO t VALUES(1)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0)"); err == nil {
		t.Error("expected CHECK constraint error for a=0, CHECK(a!=0)")
	}
}

// TestConstraintsCheckAnd exercises CHECK(a > 0 AND a < 100).
func TestConstraintsCheckAnd(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > 0 AND a < 100))")
	constraintsExec(t, db, "INSERT INTO t VALUES(50)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0)"); err == nil {
		t.Error("expected CHECK constraint error: a > 0 AND a < 100, inserting 0")
	}
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(100)"); err == nil {
		t.Error("expected CHECK constraint error: a > 0 AND a < 100, inserting 100")
	}
}

// TestConstraintsCheckOr exercises CHECK(a < 0 OR a > 10).
func TestConstraintsCheckOr(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a < 0 OR a > 10))")
	constraintsExec(t, db, "INSERT INTO t VALUES(-1)")
	constraintsExec(t, db, "INSERT INTO t VALUES(11)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(5)"); err == nil {
		t.Error("expected CHECK constraint error: a < 0 OR a > 10, inserting 5")
	}
}

// TestConstraintsCheckNullPasses exercises NULL passing through CHECK.
func TestConstraintsCheckNullPasses(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > 0))")
	// NULL always passes CHECK constraint per SQL standard.
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL)")
}

// TestConstraintsCheckFloat exercises CHECK with a float literal.
func TestConstraintsCheckFloat(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a REAL CHECK(a > 0.5))")
	constraintsExec(t, db, "INSERT INTO t VALUES(1.0)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0.4)"); err == nil {
		t.Error("expected CHECK constraint error for a=0.4, CHECK(a>0.5)")
	}
}

// TestConstraintsCheckNegLiteral exercises resolveUnaryOperand with negative literal.
func TestConstraintsCheckNegLiteral(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > -10))")
	constraintsExec(t, db, "INSERT INTO t VALUES(-5)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(-15)"); err == nil {
		t.Error("expected CHECK constraint error for a=-15, CHECK(a>-10)")
	}
}

// TestConstraintsUniqueMultiCol exercises checkMultiColUnique / scanForMultiColDuplicate /
// checkMultiColRow / GetRecordColumnIndex / GetColumnCollation.
func TestConstraintsUniqueMultiCol(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	constraintsExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a,b)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'x')")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'y')")
	constraintsExec(t, db, "INSERT INTO t VALUES(2,'x')")

	// Duplicate (1,'x') must fail.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1,'x')"); err == nil {
		t.Error("expected UNIQUE constraint error for duplicate (1,'x')")
	}
}

// TestConstraintsUniqueMultiColNullDistinct verifies that rows with NULL
// are always considered distinct in multi-column UNIQUE.
func TestConstraintsUniqueMultiColNullDistinct(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT, UNIQUE(a,b))")
	// Two rows with NULL in a column must both be allowed.
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL,'x')")
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL,'x')")
}

// TestConstraintsAutoincrement exercises isAutoincrementTable by inserting
// into a table with INTEGER PRIMARY KEY AUTOINCREMENT.
func TestConstraintsAutoincrement(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
	constraintsExec(t, db, "INSERT INTO t(val) VALUES('a')")
	constraintsExec(t, db, "INSERT INTO t(val) VALUES('b')")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestConstraintsCheckGTColumnRef exercises a check with a column identifier
// on both sides to hit the IdentExpr branch in resolveCheckOperand.
func TestConstraintsCheckGTColumnRef(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	// CHECK(a > 0) references column 'a' via an IdentExpr on the left side.
	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > 0))")
	constraintsExec(t, db, "INSERT INTO t VALUES(5)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(-1)"); err == nil {
		t.Error("expected CHECK constraint error for a=-1")
	}
}

// TestConstraintsUniqueIndexExplicit exercises checkIndexUniqueConstraints via
// CREATE UNIQUE INDEX instead of inline UNIQUE.
func TestConstraintsUniqueIndexExplicit(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b INTEGER)")
	constraintsExec(t, db, "CREATE UNIQUE INDEX idx_ab ON t(a,b)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,2)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,3)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1,2)"); err == nil {
		t.Error("expected UNIQUE constraint error via explicit unique index")
	}
}

// TestConstraintsCheckUnaryNot exercises evalUnaryCheck with NOT.
// NOT a is a unary expression wrapping an identifier; this hits the OpNot branch.
// The engine stores "NOT a" and re-parses it; any insertion is expected to fail
// the check because evalCheckExpr(IdentExpr) returns true and NOT true = false.
func TestConstraintsCheckUnaryNot(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	// CHECK(NOT a) always fails for non-null values — exercises the OpNot branch.
	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(NOT a))")

	// NULL still passes (NULL short-circuits before evalCheckExpr).
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL)")

	// Any non-null value should fail since NOT(true) = false.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1)"); err == nil {
		t.Error("expected CHECK constraint error for CHECK(NOT a) with non-null value")
	}
}

// TestConstraintsCheckMultipleColumns exercises CHECK on multiple columns in
// the same table to ensure the column-index loop works correctly.
func TestConstraintsCheckMultipleColumns(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, `CREATE TABLE t(
		x INTEGER CHECK(x > 0),
		y INTEGER CHECK(y < 100)
	)`)

	constraintsExec(t, db, "INSERT INTO t VALUES(1, 50)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0, 50)"); err == nil {
		t.Error("expected CHECK failure on x=0")
	}
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1, 100)"); err == nil {
		t.Error("expected CHECK failure on y=100")
	}
}
