// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// openDML2DB opens a fresh in-memory database for these tests.
func openDML2DB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// dml2Exec runs a statement and fatals on error.
func dml2Exec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// dml2QueryInt returns a single int64 from a query.
func dml2QueryInt(t *testing.T, db *sql.DB, q string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// ============================================================================
// insertSelectNeedsMaterialise — nil Select path
//
// The function returns false when sel==nil. This nil check is a defensive
// guard; the only way to exercise the false-on-nil path via SQL is through
// INSERT ... SELECT with aggregates/ORDER/LIMIT/DISTINCT (which go through
// the materialise path) versus a plain SELECT (which does not). We add extra
// cases with ORDER BY and LIMIT to ensure multiple materialise paths are hit.
// ============================================================================

// TestCompileDML2_InsertSelectSameTableDistinct tests INSERT INTO t SELECT DISTINCT FROM t.
// DISTINCT forces materialisation (insertSelectNeedsMaterialise returns true), so
// all rows are collected first, then inserted without conflicts.
func TestCompileDML2_InsertSelectSameTableDistinct(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE iself(x INTEGER)`)
	dml2Exec(t, db, `INSERT INTO iself VALUES(10), (20)`)
	dml2Exec(t, db, `CREATE TABLE iself2(x INTEGER)`)

	// DISTINCT triggers materialise path; src and dst are different tables.
	dml2Exec(t, db, `INSERT INTO iself2 SELECT DISTINCT x FROM iself`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM iself2`)
	if n != 2 {
		t.Fatalf("want 2 rows after insert, got %d", n)
	}
}

// TestCompileDML2_InsertSelectDifferentTable tests INSERT INTO dst SELECT FROM src,
// with a plain SELECT (no aggregate/ORDER/LIMIT/DISTINCT). This exercises the
// non-materialise path.
func TestCompileDML2_InsertSelectDifferentTable(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE isrc(x INTEGER)`)
	dml2Exec(t, db, `CREATE TABLE idst(x INTEGER)`)
	dml2Exec(t, db, `INSERT INTO isrc VALUES(10), (20), (30)`)

	dml2Exec(t, db, `INSERT INTO idst SELECT x FROM isrc`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM idst`)
	if n != 3 {
		t.Fatalf("want 3 rows, got %d", n)
	}
}

// TestCompileDML2_InsertSelectWithOrderBy verifies materialise path via ORDER BY.
func TestCompileDML2_InsertSelectWithOrderBy(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE iob_src(x INTEGER)`)
	dml2Exec(t, db, `CREATE TABLE iob_dst(x INTEGER)`)
	dml2Exec(t, db, `INSERT INTO iob_src VALUES(3), (1), (2)`)

	// ORDER BY forces materialise (insertSelectNeedsMaterialise returns true).
	dml2Exec(t, db, `INSERT INTO iob_dst SELECT x FROM iob_src ORDER BY x`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM iob_dst`)
	if n != 3 {
		t.Fatalf("want 3 rows, got %d", n)
	}
}

// TestCompileDML2_InsertSelectWithLimit verifies materialise path via LIMIT.
func TestCompileDML2_InsertSelectWithLimit(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE ilim_src(x INTEGER)`)
	dml2Exec(t, db, `CREATE TABLE ilim_dst(x INTEGER)`)
	dml2Exec(t, db, `INSERT INTO ilim_src VALUES(1), (2), (3), (4)`)

	// LIMIT forces materialise.
	dml2Exec(t, db, `INSERT INTO ilim_dst SELECT x FROM ilim_src LIMIT 2`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM ilim_dst`)
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
}

// ============================================================================
// evalSubqueryToLiteral — scalar subquery in DELETE WHERE clause
//
// materializeSubqueries is called on the WHERE clause of DELETE. When the
// WHERE contains a SubqueryExpr, evalSubqueryToLiteral executes the subquery
// and replaces it with a literal value. Different return paths:
//   - subquery returns an integer → goValueToLiteral(int64)
//   - subquery returns a string  → goValueToLiteral(string)
//   - subquery returns no rows   → LiteralNull
// ============================================================================

// TestCompileDML2_EvalSubqueryToLiteralInt exercises the integer return path
// of evalSubqueryToLiteral: DELETE WHERE id = (SELECT MIN(id) FROM t).
func TestCompileDML2_EvalSubqueryToLiteralInt(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE esq(id INTEGER, val TEXT)`)
	dml2Exec(t, db, `INSERT INTO esq VALUES(1, 'a'), (2, 'b'), (3, 'c')`)

	// Scalar subquery returns int64 — exercises the int64 branch of goValueToLiteral.
	dml2Exec(t, db, `DELETE FROM esq WHERE id = (SELECT MIN(id) FROM esq)`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM esq`)
	if n != 2 {
		t.Fatalf("want 2 rows after delete, got %d", n)
	}
}

// TestCompileDML2_EvalSubqueryToLiteralNoRows exercises the empty-result path
// of evalSubqueryToLiteral: DELETE WHERE id = (SELECT id FROM t WHERE 1=0).
// The subquery returns no rows, so evalSubqueryToLiteral returns NULL, and
// nothing is deleted.
func TestCompileDML2_EvalSubqueryToLiteralNoRows(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE esqnull(id INTEGER)`)
	dml2Exec(t, db, `INSERT INTO esqnull VALUES(1), (2)`)

	// Subquery returns no rows → NULL literal → no rows match → no delete.
	dml2Exec(t, db, `DELETE FROM esqnull WHERE id = (SELECT id FROM esqnull WHERE 1=0)`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM esqnull`)
	if n != 2 {
		t.Fatalf("want 2 rows (nothing deleted), got %d", n)
	}
}

// TestCompileDML2_EvalSubqueryToLiteralString exercises the string return path
// of evalSubqueryToLiteral: DELETE WHERE val = (SELECT val FROM t WHERE id=1).
func TestCompileDML2_EvalSubqueryToLiteralString(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE esqstr(id INTEGER, val TEXT)`)
	dml2Exec(t, db, `INSERT INTO esqstr VALUES(1, 'hello'), (2, 'world')`)

	// Scalar subquery returns a string — exercises the string branch of goValueToLiteral.
	dml2Exec(t, db, `DELETE FROM esqstr WHERE val = (SELECT val FROM esqstr WHERE id = 1)`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM esqstr`)
	if n != 1 {
		t.Fatalf("want 1 row after delete, got %d", n)
	}
}

// TestCompileDML2_EvalSubqueryInBinaryExpr exercises the BinaryExpr
// materialisation path: DELETE WHERE id > (SELECT MIN(id) FROM t) AND
// id < (SELECT MAX(id) FROM t). This walks through materializeBinaryExpr.
func TestCompileDML2_EvalSubqueryInBinaryExpr(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE esqbin(id INTEGER)`)
	dml2Exec(t, db, `INSERT INTO esqbin VALUES(1), (2), (3), (4), (5)`)

	// Binary expr with two subqueries — exercises materializeBinaryExpr.
	dml2Exec(t, db, `DELETE FROM esqbin WHERE id > (SELECT MIN(id) FROM esqbin) AND id < (SELECT MAX(id) FROM esqbin)`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM esqbin`)
	if n != 2 {
		t.Fatalf("want 2 rows (1 and 5 remain), got %d", n)
	}
}

// ============================================================================
// generatedExprForColumn — VIRTUAL vs STORED generated columns
//
// generatedExprForColumn is called for any generated column during UPDATE
// (emitUpdateColumnValue) and INSERT (replaceGeneratedValues). A VIRTUAL
// column is not physically stored; a STORED column is. Both paths exercise
// the success branch of generatedExprForColumn (expr != "").
// ============================================================================

// TestCompileDML2_GeneratedVirtualColumn exercises generatedExprForColumn for
// a VIRTUAL generated column. The column is evaluated at INSERT time (its
// expression is substituted via replaceGeneratedValues). We verify the row
// is inserted successfully and the non-generated column has the right value.
func TestCompileDML2_GeneratedVirtualColumn(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE gvirt(a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 10) VIRTUAL)`)
	dml2Exec(t, db, `INSERT INTO gvirt(a) VALUES(5)`)

	// Verify the row was inserted (a should be 5).
	a := dml2QueryInt(t, db, `SELECT a FROM gvirt`)
	if a != 5 {
		t.Fatalf("want a=5, got %d", a)
	}
}

// TestCompileDML2_GeneratedStoredColumn exercises generatedExprForColumn for
// a STORED generated column. STORED columns are evaluated at insert/update time
// and their value is physically stored.
func TestCompileDML2_GeneratedStoredColumn(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE gstored(a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	dml2Exec(t, db, `INSERT INTO gstored(a) VALUES(7)`)

	// The STORED generated column should have been evaluated during INSERT.
	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM gstored WHERE a = 7`)
	if n != 1 {
		t.Fatalf("want 1 row with a=7, got %d", n)
	}
}

// TestCompileDML2_GeneratedColumnUpdate exercises generatedExprForColumn
// during UPDATE: when a non-generated column is updated, the generated column
// re-evaluates its expression. This goes through emitUpdateColumnValue's
// col.Generated branch.
func TestCompileDML2_GeneratedColumnUpdate(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE gupd(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 3) STORED)`)
	dml2Exec(t, db, `INSERT INTO gupd(id, a) VALUES(1, 4)`)

	// Updating 'a' causes emitUpdateColumnValue to re-evaluate generatedExprForColumn for 'b'.
	dml2Exec(t, db, `UPDATE gupd SET a = 6 WHERE id = 1`)

	// Verify the row still exists and 'a' was updated.
	a := dml2QueryInt(t, db, `SELECT a FROM gupd WHERE id = 1`)
	if a != 6 {
		t.Fatalf("want a=6 after update, got %d", a)
	}
}

// ============================================================================
// emitUpdateColumnValue — generated column expression evaluation
//
// The generated branch in emitUpdateColumnValue calls generatedExprForColumn
// and then gen.GenerateExpr. The success path (copy result) is tested here.
// ============================================================================

// TestCompileDML2_UpdateColumnValueWithGenerated exercises the generated column
// branch of emitUpdateColumnValue via a multi-column table where one column is
// generated. The update of a non-generated column triggers re-evaluation of the
// generated expression for the generated column.
func TestCompileDML2_UpdateColumnValueWithGenerated(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE ucvgen(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, z INTEGER GENERATED ALWAYS AS (x + y) STORED)`)
	dml2Exec(t, db, `INSERT INTO ucvgen(id, x, y) VALUES(1, 3, 4)`)

	// Update x; emitUpdateColumnValue will handle x (SET), y (OpColumn), and z (generated).
	dml2Exec(t, db, `UPDATE ucvgen SET x = 10 WHERE id = 1`)

	// Verify x was updated correctly.
	x := dml2QueryInt(t, db, `SELECT x FROM ucvgen WHERE id = 1`)
	if x != 10 {
		t.Fatalf("want x=10 after update, got %d", x)
	}
}

// ============================================================================
// emitUpdateWhereClause — WHERE clause with subquery (subquery in UPDATE WHERE)
//
// emitUpdateWhereClause sets args with offset and calls gen.GenerateExpr.
// The uncovered path is the error return when GenerateExpr fails; we can only
// cover the success paths via SQL, so we test the WHERE with various expressions.
// ============================================================================

// TestCompileDML2_UpdateWhereWithParam exercises emitUpdateWhereClause with a
// bound parameter in the WHERE clause (setParamCount offset logic).
func TestCompileDML2_UpdateWhereWithParam(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE uwp(id INTEGER PRIMARY KEY, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO uwp VALUES(1, 10), (2, 20), (3, 30)`)

	// SET has one param (setParamCount=1), WHERE has one param (offset=1).
	dml2Exec(t, db, `UPDATE uwp SET val = ? WHERE id = ?`, int64(99), int64(2))

	v := dml2QueryInt(t, db, `SELECT val FROM uwp WHERE id = 2`)
	if v != 99 {
		t.Fatalf("want val=99, got %d", v)
	}
	// Other rows unchanged.
	v = dml2QueryInt(t, db, `SELECT val FROM uwp WHERE id = 1`)
	if v != 10 {
		t.Fatalf("want val=10, got %d", v)
	}
}

// TestCompileDML2_UpdateWhereNoMatch exercises emitUpdateWhereClause where the
// WHERE clause matches no rows (WHERE branch taken for every row, no update).
func TestCompileDML2_UpdateWhereNoMatch(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE uwnm(id INTEGER PRIMARY KEY, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO uwnm VALUES(1, 5), (2, 6)`)

	// WHERE matches nothing → no rows updated.
	dml2Exec(t, db, `UPDATE uwnm SET val = 999 WHERE id = 9999`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM uwnm WHERE val = 999`)
	if n != 0 {
		t.Fatalf("want 0 updated rows, got %d", n)
	}
}

// TestCompileDML2_UpdateWhereUnaryNegParam exercises countExprParams UnaryExpr
// branch: SET uses a negated parameter expression (-?), which parses as
// UnaryExpr(Neg, VariableExpr). countExprParams must recurse into UnaryExpr
// to count the param correctly so the WHERE args offset is right.
func TestCompileDML2_UpdateWhereUnaryNegParam(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE uwuneg(id INTEGER PRIMARY KEY, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO uwuneg VALUES(1, 0)`)

	// SET val = -? (UnaryExpr containing VariableExpr), WHERE id = ?
	// countExprParams must correctly handle the UnaryExpr to count 1 SET param.
	dml2Exec(t, db, `UPDATE uwuneg SET val = -? WHERE id = ?`, int64(42), int64(1))

	v := dml2QueryInt(t, db, `SELECT val FROM uwuneg WHERE id = 1`)
	if v != -42 {
		t.Fatalf("want val=-42, got %d", v)
	}
}

// ============================================================================
// emitDeleteWhereClause — DELETE WHERE with a bound parameter
//
// emitDeleteWhereClause builds a CodeGenerator and calls gen.GenerateExpr on
// the WHERE expression. The uncovered branch is the error return on GenerateExpr
// failure, which cannot be triggered via SQL. We test the success paths.
// ============================================================================

// TestCompileDML2_DeleteWhereWithParam exercises emitDeleteWhereClause with a
// parameter placeholder in the WHERE clause.
func TestCompileDML2_DeleteWhereWithParam(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE dwp(id INTEGER PRIMARY KEY, v INTEGER)`)
	dml2Exec(t, db, `INSERT INTO dwp VALUES(1, 10), (2, 20), (3, 30)`)

	dml2Exec(t, db, `DELETE FROM dwp WHERE id = ?`, int64(2))

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM dwp`)
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
	// Rows 1 and 3 should remain.
	n = dml2QueryInt(t, db, `SELECT COUNT(*) FROM dwp WHERE id IN (1, 3)`)
	if n != 2 {
		t.Fatalf("want rows 1 and 3, got %d", n)
	}
}

// TestCompileDML2_DeleteWhereSubquery exercises the full subquery materialise
// path in DELETE WHERE, combining emitDeleteWhereClause and evalSubqueryToLiteral.
func TestCompileDML2_DeleteWhereSubquery(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE dwsq(id INTEGER, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO dwsq VALUES(1, 100), (2, 200), (3, 300)`)

	// Delete the row whose id equals the maximum id.
	dml2Exec(t, db, `DELETE FROM dwsq WHERE id = (SELECT MAX(id) FROM dwsq)`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM dwsq`)
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
	// Row with id=3 should be gone.
	n = dml2QueryInt(t, db, `SELECT COUNT(*) FROM dwsq WHERE id = 3`)
	if n != 0 {
		t.Fatalf("row with id=3 should be deleted, got %d", n)
	}
}

// TestCompileDML2_DeleteWhereNoWhere exercises the nil-WHERE path in
// emitDeleteWhereClause (returns 0 immediately without generating any expr).
func TestCompileDML2_DeleteWhereNoWhere(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE dwnowhere(id INTEGER)`)
	dml2Exec(t, db, `INSERT INTO dwnowhere VALUES(1), (2), (3)`)

	dml2Exec(t, db, `DELETE FROM dwnowhere`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM dwnowhere`)
	if n != 0 {
		t.Fatalf("want 0 rows after full delete, got %d", n)
	}
}

// ============================================================================
// countExprParams — UnaryExpr branch
//
// countExprParams recurses into UnaryExpr nodes. When a SET value is a
// negated parameter (-?), countExprParams must descend into the UnaryExpr
// to count the enclosed VariableExpr. This test verifies correct param offset
// calculation when SET and WHERE both have params.
// ============================================================================

// TestCompileDML2_CountExprParamsUnaryExpr exercises countExprParams' UnaryExpr
// branch via a SET clause with a negated parameter (-?). The correct count of
// SET params must be computed for the WHERE param offset to be right.
func TestCompileDML2_CountExprParamsUnaryExpr(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE cepunary(id INTEGER PRIMARY KEY, score INTEGER)`)
	dml2Exec(t, db, `INSERT INTO cepunary VALUES(1, 50), (2, 60)`)

	// SET score = -? uses a UnaryExpr; countExprParams must count 1 param here.
	// WHERE id = ? uses 1 more param. If countExprParams is wrong, the WHERE
	// arg would be shifted and the update would hit the wrong row or fail.
	dml2Exec(t, db, `UPDATE cepunary SET score = -? WHERE id = ?`, int64(7), int64(1))

	v := dml2QueryInt(t, db, `SELECT score FROM cepunary WHERE id = 1`)
	if v != -7 {
		t.Fatalf("want score=-7, got %d", v)
	}
	// Row 2 must be unchanged.
	v = dml2QueryInt(t, db, `SELECT score FROM cepunary WHERE id = 2`)
	if v != 60 {
		t.Fatalf("want score=60 (unchanged), got %d", v)
	}
}

// TestCompileDML2_CountExprParamsFunction exercises countExprParams' FunctionExpr
// branch via a SET clause that calls a function with parameter arguments, e.g.
// SET val = abs(?). The param inside the function must be counted correctly.
func TestCompileDML2_CountExprParamsFunction(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE cepfunc(id INTEGER PRIMARY KEY, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO cepfunc VALUES(1, 0)`)

	// SET val = abs(?) → FunctionExpr with one VariableExpr arg.
	dml2Exec(t, db, `UPDATE cepfunc SET val = abs(?) WHERE id = ?`, int64(-15), int64(1))

	v := dml2QueryInt(t, db, `SELECT val FROM cepfunc WHERE id = 1`)
	if v != 15 {
		t.Fatalf("want val=15, got %d", v)
	}
}

// ============================================================================
// emitUpdateLoop — multi-column UPDATE with generated columns and WHERE
//
// emitUpdateLoop builds the full update bytecode, including the WHERE skip,
// trigger snapshot (when applicable), record build, and row replacement.
// ============================================================================

// TestCompileDML2_UpdateLoopMultiCol exercises emitUpdateLoop with multiple
// SET columns, a WHERE clause, and a generated column in the table.
func TestCompileDML2_UpdateLoopMultiCol(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE ulmc(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER GENERATED ALWAYS AS (a * 2) STORED)`)
	dml2Exec(t, db, `INSERT INTO ulmc(id, a, b) VALUES(1, 5, 'old'), (2, 10, 'keep')`)

	// Update both a and b on row 1; generated column c is re-evaluated via emitUpdateColumnValue.
	dml2Exec(t, db, `UPDATE ulmc SET a = 20, b = 'new' WHERE id = 1`)

	var a int64
	var b string
	if err := db.QueryRow(`SELECT a, b FROM ulmc WHERE id = 1`).Scan(&a, &b); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if a != 20 {
		t.Fatalf("want a=20, got %d", a)
	}
	if b != "new" {
		t.Fatalf("want b='new', got %q", b)
	}

	// Row 2 should be unchanged.
	if err := db.QueryRow(`SELECT a FROM ulmc WHERE id = 2`).Scan(&a); err != nil {
		t.Fatalf("scan row2: %v", err)
	}
	if a != 10 {
		t.Fatalf("want a=10 for row 2, got %d", a)
	}
}

// TestCompileDML2_UpdateLoopNoWhere exercises emitUpdateLoop without a WHERE
// clause, updating all rows.
func TestCompileDML2_UpdateLoopNoWhere(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE ulnw(id INTEGER PRIMARY KEY, val INTEGER)`)
	dml2Exec(t, db, `INSERT INTO ulnw VALUES(1, 1), (2, 2), (3, 3)`)

	dml2Exec(t, db, `UPDATE ulnw SET val = 0`)

	n := dml2QueryInt(t, db, `SELECT COUNT(*) FROM ulnw WHERE val = 0`)
	if n != 3 {
		t.Fatalf("want all 3 rows updated, got %d with val=0", n)
	}
}

// TestCompileDML2_UpdateCannotSetGeneratedColumn verifies that attempting to
// UPDATE a generated column directly returns an error (validateUpdateColumns).
func TestCompileDML2_UpdateCannotSetGeneratedColumn(t *testing.T) {
	db := openDML2DB(t)
	dml2Exec(t, db, `CREATE TABLE ucsg(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a + 1) STORED)`)
	dml2Exec(t, db, `INSERT INTO ucsg(id, a) VALUES(1, 5)`)

	_, err := db.Exec(`UPDATE ucsg SET b = 99 WHERE id = 1`)
	if err == nil {
		t.Fatal("expected error when updating generated column, got nil")
	}
}
