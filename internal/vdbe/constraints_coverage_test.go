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

// TestConstraintsNotNullViolation exercises checkNotNullConstraints /
// validateNotNullColumns / checkColumnNotNull when inserting NULL into a
// NOT NULL column.
func TestConstraintsNotNullViolation(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER NOT NULL)")

	// Valid insert passes.
	constraintsExec(t, db, "INSERT INTO t VALUES(1)")

	// NULL must fail the NOT NULL constraint.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(NULL)"); err == nil {
		t.Error("expected NOT NULL constraint error")
	}
}

// TestConstraintsNotNullTextColumn exercises NOT NULL on a TEXT column.
func TestConstraintsNotNullTextColumn(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(name TEXT NOT NULL)")
	constraintsExec(t, db, "INSERT INTO t VALUES('hello')")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(NULL)"); err == nil {
		t.Error("expected NOT NULL constraint error on TEXT column")
	}
}

// TestConstraintsNotNullMultipleColumns exercises validateNotNullColumns loop
// when several NOT NULL columns exist.
func TestConstraintsNotNullMultipleColumns(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER NOT NULL, b TEXT NOT NULL)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1, 'x')")

	// First column NULL.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(NULL, 'x')"); err == nil {
		t.Error("expected NOT NULL error on first column")
	}
	// Second column NULL.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1, NULL)"); err == nil {
		t.Error("expected NOT NULL error on second column")
	}
}

// TestConstraintsNotNullNullableColumn verifies that nullable columns allow NULL.
func TestConstraintsNotNullNullableColumn(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT NOT NULL)")
	// a is nullable, so NULL is allowed there.
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL, 'required')")
}

// TestConstraintsCheckRealColumn exercises memToNumber for REAL values and
// the float literal path in literalToNumber / toFloat.
func TestConstraintsCheckRealColumn(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a REAL CHECK(a >= 1.5))")
	constraintsExec(t, db, "INSERT INTO t VALUES(2.0)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1.0)"); err == nil {
		t.Error("expected CHECK constraint error for a=1.0, CHECK(a>=1.5)")
	}
}

// TestConstraintsCheckIntegerEqualFloat exercises compareCheckValues across
// int and float operands (toFloat conversion).
func TestConstraintsCheckIntegerEqualFloat(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a REAL CHECK(a > 0.0))")
	constraintsExec(t, db, "INSERT INTO t VALUES(1.5)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(-0.1)"); err == nil {
		t.Error("expected CHECK constraint error for a=-0.1")
	}
}

// TestConstraintsUniqueMultiColThreeRows exercises checkMultiColRow with
// multiple existing rows to ensure the full scan loop is exercised.
func TestConstraintsUniqueMultiColThreeRows(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b INTEGER)")
	constraintsExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a,b)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,1)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,2)")
	constraintsExec(t, db, "INSERT INTO t VALUES(2,1)")

	// Duplicate of existing row (2,1) must fail.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(2,1)"); err == nil {
		t.Error("expected UNIQUE constraint error for (2,1)")
	}
}

// TestConstraintsPrimaryKeyUnique exercises checkCompositePKUnique / GetTablePrimaryKey
// via a composite primary key defined inline.
func TestConstraintsPrimaryKeyUnique(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT, PRIMARY KEY(a, b))")
	constraintsExec(t, db, "INSERT INTO t VALUES(1, 'x')")
	constraintsExec(t, db, "INSERT INTO t VALUES(1, 'y')")

	// Duplicate primary key must fail.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1, 'x')"); err == nil {
		t.Error("expected PRIMARY KEY constraint error for duplicate (1,'x')")
	}
}

// TestConstraintsNotNullWithCheck exercises both NOT NULL and CHECK constraints
// on the same column.
func TestConstraintsNotNullWithCheck(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(score INTEGER NOT NULL CHECK(score > 0))")
	constraintsExec(t, db, "INSERT INTO t VALUES(10)")

	// NULL fails NOT NULL.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(NULL)"); err == nil {
		t.Error("expected NOT NULL error")
	}
	// Zero fails CHECK.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(0)"); err == nil {
		t.Error("expected CHECK error for score=0")
	}
}

// TestConstraintsUniqueIndexNonUnique verifies non-unique index does not
// trigger a uniqueness error (exercises checkIndexUniqueConstraints skip branch).
func TestConstraintsUniqueIndexNonUnique(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	constraintsExec(t, db, "CREATE INDEX idx_t_a ON t(a)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'x')")
	// Same value should be allowed on a non-unique index.
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'y')")
}

// TestConstraintsCheckNullInBinaryExpr exercises the evalBinaryCheck NULL-in-
// comparison path: when one operand resolves to nil the check must pass.
// We trigger this by inserting NULL into a CHECK(a > 0) column since NULL
// is handled early in evaluateCheckConstraint — exercising the outer NULL guard.
func TestConstraintsCheckNullInBinaryExprPasses(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER CHECK(a > 0))")
	// NULL passes all CHECK constraints per SQL standard.
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL)")
}

// TestConstraintsAutoincrementSequence exercises getSequenceManager indirectly
// via an AUTOINCREMENT table, inserting multiple rows.
func TestConstraintsAutoincrementSequence(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)")
	constraintsExec(t, db, "INSERT INTO t(val) VALUES('first')")
	constraintsExec(t, db, "INSERT INTO t(val) VALUES('second')")
	constraintsExec(t, db, "INSERT INTO t(val) VALUES('third')")

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT(*): %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestConstraintsCheckGTWithNegatedFloat exercises the negative float literal
// path through resolveUnaryOperand → literalToNumber(float).
func TestConstraintsCheckGTWithNegatedFloat(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a REAL CHECK(a > -1.5))")
	constraintsExec(t, db, "INSERT INTO t VALUES(-1.0)")

	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(-2.0)"); err == nil {
		t.Error("expected CHECK error for a=-2.0, CHECK(a > -1.5)")
	}
}

// TestConstraintsUniqueIndexNullAllCols exercises the allNull branch in
// checkMultiColUnique: when ALL indexed columns are NULL the row is always
// distinct and the allNull early-return must be taken.
func TestConstraintsUniqueIndexNullAllCols(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	constraintsExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a,b)")
	// Multiple rows where BOTH a and b are NULL must all be allowed.
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL,NULL)")
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL,NULL)")
	constraintsExec(t, db, "INSERT INTO t VALUES(NULL,NULL)")
}

// TestConstraintsUniqueColCollation exercises GetColumnCollation during a
// multi-column unique scan by inserting case-distinct text values.
func TestConstraintsUniqueColCollation(t *testing.T) {
	db := constraintsOpenDB(t)
	defer db.Close()

	constraintsExec(t, db, "CREATE TABLE t(a INTEGER, b TEXT)")
	constraintsExec(t, db, "CREATE UNIQUE INDEX idx_t_ab ON t(a,b)")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'ABC')")
	constraintsExec(t, db, "INSERT INTO t VALUES(1,'abc')")

	// Exact duplicate must fail.
	if err := constraintsExecErr(t, db, "INSERT INTO t VALUES(1,'ABC')"); err == nil {
		t.Error("expected UNIQUE constraint error for (1,'ABC') duplicate")
	}
}
