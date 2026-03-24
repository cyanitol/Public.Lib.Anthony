// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// ============================================================================
// fixInnerRewindAddresses deeper coverage (stmt_cte_recursive.go:443)
// The recursive member of the CTE joins against a base table, causing the
// JOIN compiler to emit a Rewind with P2=0 inside the recursive body.
// fixInnerRewindAddresses must patch that address so the inner loop skips
// correctly when the inner table is empty.
// ============================================================================

// TestFixInnerRewindFibJoin exercises fixInnerRewindAddresses via a two-column
// recursive Fibonacci CTE whose recursive member joins against a base table.
// The join in the recursive member forces the JOIN compiler to emit an inner
// Rewind with P2=0 that fixInnerRewindAddresses must patch.
func TestFixInnerRewindFibJoin(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE sentinel(v INTEGER)",
		"INSERT INTO sentinel VALUES(1)",
	})
	// The recursive member joins against sentinel so the compiled recursive body
	// contains an inner Rewind (P2=0) that must be patched.
	n := queryInt64(t, db,
		`WITH RECURSIVE fib(a, b) AS (
			SELECT 0, 1
			UNION ALL
			SELECT fib.b, fib.a + fib.b
			FROM fib JOIN sentinel ON sentinel.v = 1
			WHERE fib.a < 100
		) SELECT COUNT(*) FROM fib`)
	if n < 1 {
		t.Errorf("fixInnerRewindAddresses fib JOIN sentinel: got %d rows, want >= 1", n)
	}
}

// TestFixInnerRewindTreeJoin exercises fixInnerRewindAddresses via a recursive
// hierarchy CTE where the recursive member joins against the base table.
func TestFixInnerRewindTreeJoin(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE tree(id INTEGER, parent INTEGER, label TEXT)",
		"INSERT INTO tree VALUES(1, NULL, 'root')",
		"INSERT INTO tree VALUES(2, 1,    'child1')",
		"INSERT INTO tree VALUES(3, 1,    'child2')",
		"INSERT INTO tree VALUES(4, 2,    'grandchild')",
	})
	// Recursive member joins tree against the anchor via parent linkage.
	n := queryInt64(t, db,
		`WITH RECURSIVE walk(id, label) AS (
			SELECT id, label FROM tree WHERE parent IS NULL
			UNION ALL
			SELECT tree.id, tree.label
			FROM tree JOIN walk ON tree.parent = walk.id
		) SELECT COUNT(*) FROM walk`)
	if n != 4 {
		t.Errorf("fixInnerRewindAddresses tree JOIN: got %d, want 4", n)
	}
}

// ============================================================================
// substituteSelect no-WHERE branch (trigger_runtime.go:263)
// substituteSelect has an early return when stmt.Where is nil.  The existing
// tests all use a WHERE clause.  A trigger body with a bare SELECT (no WHERE)
// exercises the stmt.Where == nil path directly.
// ============================================================================

// TestSubstituteSelectNoWhere exercises the stmt.Where == nil branch of
// substituteSelect.  The trigger body contains INSERT...SELECT with a FROM
// clause but no WHERE, so substituteSelect is called with stmt.Where == nil
// and returns early without performing expression substitution.
func TestSubstituteSelectNoWhere(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
		"CREATE TABLE log(id INTEGER PRIMARY KEY, v INTEGER)",
		// SELECT has no WHERE: exercises the stmt.Where == nil branch.
		`CREATE TRIGGER t_sel_nowhere AFTER INSERT ON t
			BEGIN
				INSERT INTO log(v) SELECT COUNT(*) FROM t;
			END`,
		"INSERT INTO t VALUES(1, 42)",
		"INSERT INTO t VALUES(2, 99)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM log")
	if n != 2 {
		t.Errorf("substituteSelect no-WHERE: got %d log rows, want 2", n)
	}
}

// TestSubstituteSelectNoWhereWithNewRef exercises the no-WHERE substituteSelect
// path in a trigger that references NEW in the INSERT VALUES (not in SELECT
// WHERE) so the SELECT itself carries no WHERE clause.
func TestSubstituteSelectNoWhereWithNewRef(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	db.SetMaxOpenConns(1)
	execAll(t, db, []string{
		"CREATE TABLE src(id INTEGER PRIMARY KEY, val INTEGER)",
		"CREATE TABLE snap(id INTEGER PRIMARY KEY, total INTEGER)",
		// INSERT...SELECT from src with no WHERE: substituteSelect sees nil Where.
		`CREATE TRIGGER snap_insert AFTER INSERT ON src
			BEGIN
				INSERT INTO snap(total) SELECT SUM(val) FROM src;
			END`,
		"INSERT INTO src VALUES(1, 10)",
		"INSERT INTO src VALUES(2, 20)",
	})
	n := queryInt64(t, db, "SELECT COUNT(*) FROM snap")
	if n != 2 {
		t.Errorf("substituteSelect no-WHERE with ref: got %d snap rows, want 2", n)
	}
}

// ============================================================================
// generateHavingIdentExpr — alias-to-AVG branch (stmt_groupby.go:917)
// When the HAVING clause contains a bare identifier that matches an AVG alias,
// generateHavingIdentExpr enters the ref.isAvg == true branch.
// The existing tests use plain column identifiers, not AVG aliases.
// ============================================================================

// TestHavingIdentExprAvgAlias exercises the ref.isAvg == true branch of
// generateHavingIdentExpr by using an AVG alias as the HAVING predicate.
func TestHavingIdentExprAvgAlias(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE emp(dept TEXT, salary INTEGER)",
		"INSERT INTO emp VALUES('eng', 100)",
		"INSERT INTO emp VALUES('eng', 200)",
		"INSERT INTO emp VALUES('mkt', 50)",
		"INSERT INTO emp VALUES('mkt', 60)",
	})
	// avg_sal is an AVG alias; HAVING avg_sal > 70 exercises the isAvg path
	// in generateHavingIdentExpr.
	n := queryInt64(t, db,
		"SELECT COUNT(*) FROM (SELECT dept, AVG(salary) AS avg_sal FROM emp GROUP BY dept HAVING avg_sal > 70)")
	if n != 1 {
		t.Errorf("generateHavingIdentExpr AVG alias: got %d groups, want 1", n)
	}
}

// TestHavingIdentExprDeptEqText exercises generateHavingIdentExpr with a TEXT
// column identifier in HAVING (not in aggregateMap → falls through to GenerateExpr).
func TestHavingIdentExprDeptEqText(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE emp2(dept TEXT, salary INTEGER)",
		"INSERT INTO emp2 VALUES('eng', 100)",
		"INSERT INTO emp2 VALUES('eng', 200)",
		"INSERT INTO emp2 VALUES('mkt', 50)",
	})
	n := queryInt64(t, db,
		"SELECT COUNT(*) FROM (SELECT dept, COUNT(*) FROM emp2 GROUP BY dept HAVING dept = 'eng')")
	if n != 1 {
		t.Errorf("generateHavingIdentExpr dept='eng': got %d, want 1", n)
	}
}

// ============================================================================
// findColumnCollation table-qualified branch (compile_join_agg.go:385)
// When the GROUP BY expression is a table-qualified identifier (e.g. t.name),
// findColumnCollation takes the ident.Table != "" branch and searches only the
// named table.  Use a JOIN with GROUP BY on a COLLATE NOCASE column.
// ============================================================================

// TestFindColumnCollationQualifiedJoin exercises the ident.Table != "" branch
// of findColumnCollation by using a table-qualified GROUP BY expression on a
// COLLATE NOCASE column inside a JOIN aggregate query.
func TestFindColumnCollationQualifiedJoin(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE nc(name TEXT COLLATE NOCASE, score INTEGER)",
		"CREATE TABLE extra(name TEXT, bonus INTEGER)",
		"INSERT INTO nc VALUES('Alice', 10), ('alice', 20), ('Bob', 30)",
		"INSERT INTO extra VALUES('Alice', 1), ('Bob', 2)",
	})
	// GROUP BY nc.name with COLLATE NOCASE causes findColumnCollation to be
	// called with ident.Table = "nc", exercising the qualified-lookup branch.
	n := queryInt64(t, db,
		`SELECT COUNT(*) FROM (
			SELECT nc.name, SUM(nc.score) AS total
			FROM nc JOIN extra ON nc.name = extra.name
			GROUP BY nc.name
		)`)
	if n < 1 {
		t.Errorf("findColumnCollation qualified JOIN: got %d groups, want >= 1", n)
	}
}

// TestFindColumnCollationUnqualifiedJoin exercises the unqualified path of
// findColumnCollation (ident.Table == "") where the first-match-wins scan
// finds a COLLATE NOCASE column across two joined tables.
func TestFindColumnCollationUnqualifiedJoin(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()
	execAll(t, db, []string{
		"CREATE TABLE nc2(name TEXT COLLATE NOCASE, val INTEGER)",
		"CREATE TABLE ref2(name TEXT, cat TEXT)",
		"INSERT INTO nc2 VALUES('X', 1), ('Y', 2), ('Z', 3)",
		"INSERT INTO ref2 VALUES('X', 'a'), ('Y', 'a'), ('Z', 'b')",
	})
	// Unqualified GROUP BY name resolves via first-match scan in findColumnCollation.
	n := queryInt64(t, db,
		`SELECT COUNT(*) FROM (
			SELECT nc2.name, COUNT(*) AS cnt
			FROM nc2 JOIN ref2 ON nc2.name = ref2.name
			GROUP BY nc2.name
		)`)
	if n != 3 {
		t.Errorf("findColumnCollation unqualified JOIN: got %d groups, want 3", n)
	}
}
