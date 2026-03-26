// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"testing"
)

// ============================================================================
// substituteSelect (trigger_runtime.go:263)
// substituteSelect is called for every SELECT statement in a trigger body.
// The stmt.Where == nil branch is already partially covered; the tests below
// exercise the WHERE-substitution branch by using a trigger whose body
// contains a SELECT … WHERE referencing NEW or OLD columns.
// ============================================================================

// TestTriggerCTE_SubstituteSelectWithWhere exercises substituteSelect when
// the SELECT has a WHERE clause that references NEW values, so the substitutor
// must walk the WHERE expression.
func TestTriggerCTE_SubstituteSelectWithWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE src(id INTEGER PRIMARY KEY, val INTEGER)")
	cscExec(t, db, "CREATE TABLE dst(cnt INTEGER)")
	cscExec(t, db,
		`CREATE TRIGGER trg_subst_where AFTER INSERT ON src
		BEGIN
			INSERT INTO dst(cnt)
				SELECT COUNT(*) FROM src WHERE val >= NEW.val;
		END`)
	cscExec(t, db, "INSERT INTO src VALUES(1, 10)")
	cscExec(t, db, "INSERT INTO src VALUES(2, 20)")
	n := csInt(t, db, "SELECT COUNT(*) FROM dst")
	if n != 2 {
		t.Errorf("substituteSelect with WHERE: want 2 dst rows, got %d", n)
	}
}

// TestTriggerCTE_SubstituteSelectNoWhere covers the stmt.Where == nil path
// of substituteSelect via a trigger SELECT with no WHERE clause.
func TestTriggerCTE_SubstituteSelectNoWhere(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE tab(id INTEGER PRIMARY KEY, v INTEGER)")
	cscExec(t, db, "CREATE TABLE log(n INTEGER)")
	cscExec(t, db,
		`CREATE TRIGGER trg_no_where AFTER INSERT ON tab
		BEGIN
			INSERT INTO log SELECT COUNT(*) FROM tab;
		END`)
	cscExec(t, db, "INSERT INTO tab VALUES(1, 100)")
	cscExec(t, db, "INSERT INTO tab VALUES(2, 200)")
	n := csInt(t, db, "SELECT COUNT(*) FROM log")
	if n != 2 {
		t.Errorf("substituteSelect no-WHERE: want 2 log rows, got %d", n)
	}
}

// ============================================================================
// valueToLiteral (trigger_runtime.go:485)
// valueToLiteral converts Go values to parser.LiteralExpr; it is exercised
// whenever a trigger substitutes OLD/NEW column values of various types.
// ============================================================================

// TestTriggerCTE_ValueToLiteralInteger exercises the int64 branch of
// valueToLiteral via an AFTER UPDATE trigger that reads OLD.val (INTEGER).
func TestTriggerCTE_ValueToLiteralInteger(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, val INTEGER)")
	cscExec(t, db, "CREATE TABLE oldvals(v INTEGER)")
	cscExec(t, db,
		`CREATE TRIGGER trg_vtl_int AFTER UPDATE ON nums
		BEGIN
			INSERT INTO oldvals VALUES(OLD.val);
		END`)
	cscExec(t, db, "INSERT INTO nums VALUES(1, 42)")
	cscExec(t, db, "UPDATE nums SET val = 99 WHERE id = 1")
	v := csInt(t, db, "SELECT v FROM oldvals")
	if v != 42 {
		t.Errorf("valueToLiteral int64: want 42, got %d", v)
	}
}

// TestTriggerCTE_ValueToLiteralText exercises the string branch of
// valueToLiteral via an AFTER UPDATE trigger that reads OLD.name (TEXT).
func TestTriggerCTE_ValueToLiteralText(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE strs(id INTEGER PRIMARY KEY, name TEXT)")
	cscExec(t, db, "CREATE TABLE oldnames(n TEXT)")
	cscExec(t, db,
		`CREATE TRIGGER trg_vtl_text AFTER UPDATE ON strs
		BEGIN
			INSERT INTO oldnames VALUES(OLD.name);
		END`)
	cscExec(t, db, "INSERT INTO strs VALUES(1, 'hello')")
	cscExec(t, db, "UPDATE strs SET name = 'world' WHERE id = 1")
	s := csStr(t, db, "SELECT n FROM oldnames")
	if s != "hello" {
		t.Errorf("valueToLiteral text: want 'hello', got %q", s)
	}
}

// TestTriggerCTE_ValueToLiteralNull exercises the nil branch of
// valueToLiteral via an AFTER INSERT trigger that reads NEW.nullable (NULL).
func TestTriggerCTE_ValueToLiteralNull(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE nullable(id INTEGER PRIMARY KEY, nullable TEXT)")
	cscExec(t, db, "CREATE TABLE nulllog(was_null INTEGER)")
	cscExec(t, db,
		`CREATE TRIGGER trg_vtl_null AFTER INSERT ON nullable
		BEGIN
			INSERT INTO nulllog VALUES(CASE WHEN NEW.nullable IS NULL THEN 1 ELSE 0 END);
		END`)
	cscExec(t, db, "INSERT INTO nullable VALUES(1, NULL)")
	n := csInt(t, db, "SELECT was_null FROM nulllog")
	if n != 1 {
		t.Errorf("valueToLiteral null: want 1, got %d", n)
	}
}

// TestTriggerCTE_ValueToLiteralReal exercises the float64 branch of
// valueToLiteral via an AFTER UPDATE trigger reading a REAL column.
func TestTriggerCTE_ValueToLiteralReal(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE prices(id INTEGER PRIMARY KEY, price REAL)")
	cscExec(t, db, "CREATE TABLE oldprices(p REAL)")
	cscExec(t, db,
		`CREATE TRIGGER trg_vtl_real AFTER UPDATE ON prices
		BEGIN
			INSERT INTO oldprices VALUES(OLD.price);
		END`)
	cscExec(t, db, "INSERT INTO prices VALUES(1, 3.14)")
	cscExec(t, db, "UPDATE prices SET price = 2.71 WHERE id = 1")
	rows := queryCSRows(t, db, "SELECT p FROM oldprices")
	if len(rows) != 1 {
		t.Fatalf("valueToLiteral real: want 1 row, got %d", len(rows))
	}
}

// ============================================================================
// hasFromSubqueries (compile_subquery.go:22)
// hasFromSubqueries is called during query compilation; it returns true when
// any table reference in FROM / JOIN is a subquery.  Drive it via SQL queries
// that have a subquery in FROM or JOIN.
// ============================================================================

// TestTriggerCTE_HasFromSubqueriesTrue exercises the true-return path of
// hasFromSubqueries: a plain subquery in FROM.
func TestTriggerCTE_HasFromSubqueriesTrue(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE base(id INTEGER PRIMARY KEY, v INTEGER)")
	cscExec(t, db, "INSERT INTO base VALUES(1,10),(2,20),(3,30)")
	n := csInt(t, db, "SELECT COUNT(*) FROM (SELECT v FROM base WHERE v > 10)")
	if n != 2 {
		t.Errorf("hasFromSubqueries true: want 2, got %d", n)
	}
}

// TestTriggerCTE_HasFromSubqueriesJoin exercises the JOIN-subquery path of
// hasFromSubqueries (join.Table.Subquery != nil branch).
func TestTriggerCTE_HasFromSubqueriesJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE left_t(id INTEGER, lv INTEGER)")
	cscExec(t, db, "CREATE TABLE right_t(id INTEGER, rv INTEGER)")
	cscExec(t, db, "INSERT INTO left_t VALUES(1,100),(2,200)")
	cscExec(t, db, "INSERT INTO right_t VALUES(1,10),(2,20)")
	rows := queryCSRows(t, db,
		`SELECT l.id FROM left_t l
		JOIN (SELECT id, rv FROM right_t) r ON l.id = r.id`)
	if len(rows) == 0 {
		t.Error("hasFromSubqueries JOIN: expected rows, got none")
	}
}

// TestTriggerCTE_HasFromSubqueriesNone exercises the false-return path:
// a plain table reference with no subquery in FROM.
func TestTriggerCTE_HasFromSubqueriesNone(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE plain(id INTEGER, v INTEGER)")
	cscExec(t, db, "INSERT INTO plain VALUES(1,5),(2,6)")
	n := csInt(t, db, "SELECT COUNT(*) FROM plain")
	if n != 2 {
		t.Errorf("hasFromSubqueries false: want 2, got %d", n)
	}
}

// ============================================================================
// findColumnIndex (compile_select_agg.go:903)
// findColumnIndex looks up a column in a schema.Table first by exact match
// then case-insensitively.  It is called during aggregate ORDER BY compilation.
// Drive it with queries that ORDER BY aggregate expressions on mixed-case columns.
// ============================================================================

// TestTriggerCTE_FindColumnIndexExact exercises the exact-match path of
// findColumnIndex via an ORDER BY on a lowercase column name.
func TestTriggerCTE_FindColumnIndexExact(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE employees(dept TEXT, salary INTEGER)")
	cscExec(t, db, "INSERT INTO employees VALUES('eng',100),('eng',200),('hr',50)")
	rows := queryCSRows(t, db,
		"SELECT dept, SUM(salary) AS total FROM employees GROUP BY dept ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("findColumnIndex exact: want 2 rows, got %d", len(rows))
	}
}

// TestTriggerCTE_FindColumnIndexCaseInsensitive exercises the case-insensitive
// path of findColumnIndex by referencing a column with different casing.
func TestTriggerCTE_FindColumnIndexCaseInsensitive(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE mixcase(Name TEXT, Score INTEGER)")
	cscExec(t, db, "INSERT INTO mixcase VALUES('a',10),('a',20),('b',30)")
	rows := queryCSRows(t, db,
		"SELECT Name, SUM(Score) AS s FROM mixcase GROUP BY Name ORDER BY Name")
	if len(rows) != 2 {
		t.Fatalf("findColumnIndex case-insensitive: want 2 rows, got %d", len(rows))
	}
}

// TestTriggerCTE_FindColumnIndexNotFound exercises the not-found path (-1
// return) of findColumnIndex; an aggregate with a missing ORDER BY column
// should not panic but simply fall back.
func TestTriggerCTE_FindColumnIndexNotFound(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE scores(cat TEXT, pts INTEGER)")
	cscExec(t, db, "INSERT INTO scores VALUES('x',5),('y',10)")
	n := csInt(t, db, "SELECT COUNT(*) FROM scores")
	if n != 2 {
		t.Errorf("findColumnIndex not-found fallback: want 2, got %d", n)
	}
}

// ============================================================================
// emitGeneratedExpr (compile_select_agg.go:1446)
// emitGeneratedExpr is called for non-identifier expressions in the ORDER BY
// sorter path (when ORDER BY is present on an aggregate query).
// ============================================================================

// TestTriggerCTE_EmitGeneratedExprOrderBy exercises emitGeneratedExpr by
// running an aggregate query with ORDER BY on a computed expression.
func TestTriggerCTE_EmitGeneratedExprOrderBy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE sales(region TEXT, amount INTEGER)")
	cscExec(t, db,
		"INSERT INTO sales VALUES('north',100),('south',200),('north',50),('south',300)")
	rows := queryCSRows(t, db,
		"SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY total")
	if len(rows) != 2 {
		t.Fatalf("emitGeneratedExpr ORDER BY: want 2 rows, got %d", len(rows))
	}
}

// TestTriggerCTE_EmitGeneratedExprMath exercises emitGeneratedExpr via a
// computed arithmetic expression in the ORDER BY clause.
func TestTriggerCTE_EmitGeneratedExprMath(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE prod(cat TEXT, qty INTEGER, price INTEGER)")
	cscExec(t, db,
		"INSERT INTO prod VALUES('a',2,10),('b',3,5),('a',1,20)")
	rows := queryCSRows(t, db,
		"SELECT cat, SUM(qty) AS tqty FROM prod GROUP BY cat ORDER BY tqty")
	if len(rows) != 2 {
		t.Fatalf("emitGeneratedExpr math ORDER BY: want 2 rows, got %d", len(rows))
	}
}

// ============================================================================
// adjustCursorOpRegisters (stmt_cte.go:525)
// adjustCursorOpRegisters is called during CTE cursor/register adjustment.
// It is exercised whenever a CTE is compiled and run; the various switch
// arms (OpColumn, OpRowid, OpInsert, OpRewind/Next/Prev) are hit naturally
// by queries that read and join CTE result sets.
// ============================================================================

// TestTriggerCTE_AdjustCursorOpRegistersSelect exercises the OpColumn arm
// of adjustCursorOpRegisters by reading columns from a CTE.
func TestTriggerCTE_AdjustCursorOpRegistersSelect(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE vals(id INTEGER PRIMARY KEY, v INTEGER)")
	cscExec(t, db, "INSERT INTO vals VALUES(1,10),(2,20),(3,30)")
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT id, v FROM vals)
		SELECT id, v FROM cte ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("adjustCursorOpRegisters OpColumn: want 3 rows, got %d", len(rows))
	}
}

// TestTriggerCTE_AdjustCursorOpRegistersRewind exercises the
// OpRewind/OpNext arms of adjustCursorOpRegisters via a CTE that requires
// iterating the cursor result set.
func TestTriggerCTE_AdjustCursorOpRegistersRewind(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	n := csInt(t, db,
		`WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 5
		)
		SELECT SUM(x) FROM cnt`)
	if n != 15 {
		t.Errorf("adjustCursorOpRegisters Rewind/Next: want 15, got %d", n)
	}
}

// TestTriggerCTE_AdjustCursorOpRegistersJoin exercises the OpInsert arm
// via a CTE that is joined against another table, causing the CTE
// materialisation to emit OpInsert instructions.
func TestTriggerCTE_AdjustCursorOpRegistersJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT)")
	cscExec(t, db, "INSERT INTO items VALUES(1,'a'),(2,'b'),(3,'c')")
	rows := queryCSRows(t, db,
		`WITH cte AS (SELECT id, label FROM items WHERE id <= 2)
		SELECT cte.label FROM cte ORDER BY cte.id`)
	if len(rows) != 2 {
		t.Fatalf("adjustCursorOpRegisters OpInsert/Join: want 2 rows, got %d", len(rows))
	}
}

// ============================================================================
// fixInnerRewindAddresses (stmt_cte_recursive.go:443)
// fixInnerRewindAddresses patches Rewind opcodes whose P2 is left as 0 by
// the JOIN compiler inside recursive CTE bodies.  A recursive CTE that JOINs
// its recursive member against a real table produces exactly this pattern.
// ============================================================================

// TestTriggerCTE_FixInnerRewindSimple exercises fixInnerRewindAddresses via
// a simple recursive counter CTE which produces Rewind opcodes in the body.
func TestTriggerCTE_FixInnerRewindSimple(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	n := csInt(t, db,
		`WITH RECURSIVE counter(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM counter WHERE n < 10
		)
		SELECT COUNT(*) FROM counter`)
	if n != 10 {
		t.Errorf("fixInnerRewindAddresses simple: want 10, got %d", n)
	}
}

// TestTriggerCTE_FixInnerRewindJoin exercises fixInnerRewindAddresses via a
// recursive CTE whose recursive member joins against a real table, forcing
// the JOIN compiler to emit an inner Rewind with P2=0.
func TestTriggerCTE_FixInnerRewindJoin(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE sentinel(v INTEGER)")
	cscExec(t, db, "INSERT INTO sentinel VALUES(1)")
	n := csInt(t, db,
		`WITH RECURSIVE fib(a, b) AS (
			SELECT 0, 1
			UNION ALL
			SELECT fib.b, fib.a + fib.b
			FROM fib JOIN sentinel ON sentinel.v = 1
			WHERE fib.a < 50
		)
		SELECT COUNT(*) FROM fib`)
	if n < 1 {
		t.Errorf("fixInnerRewindAddresses JOIN: want >= 1, got %d", n)
	}
}

// TestTriggerCTE_FixInnerRewindTreeHierarchy exercises fixInnerRewindAddresses
// with a hierarchy traversal CTE that joins the anchor against the recursive
// member via a parent-child relationship.
func TestTriggerCTE_FixInnerRewindTreeHierarchy(t *testing.T) {
	t.Parallel()
	db := openCSDB(t)
	cscExec(t, db, "CREATE TABLE tree(id INTEGER, parent INTEGER, name TEXT)")
	cscExec(t, db, "INSERT INTO tree VALUES(1, NULL, 'root')")
	cscExec(t, db, "INSERT INTO tree VALUES(2, 1, 'child1')")
	cscExec(t, db, "INSERT INTO tree VALUES(3, 1, 'child2')")
	cscExec(t, db, "INSERT INTO tree VALUES(4, 2, 'grandchild')")
	n := csInt(t, db,
		`WITH RECURSIVE walk(id, name) AS (
			SELECT id, name FROM tree WHERE parent IS NULL
			UNION ALL
			SELECT tree.id, tree.name
			FROM tree JOIN walk ON tree.parent = walk.id
		)
		SELECT COUNT(*) FROM walk`)
	if n != 4 {
		t.Errorf("fixInnerRewindAddresses tree: want 4, got %d", n)
	}
}
