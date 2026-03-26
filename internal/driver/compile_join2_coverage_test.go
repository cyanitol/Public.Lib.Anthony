// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// TestCompileJoin2Coverage targets the uncovered branches in
// findColumnTableIndex and emitLeafRowSorter inside compile_join.go.
//
// findColumnTableIndex branches:
//   - col.Expr is not *parser.IdentExpr → returns 0 (expression columns like 1+1)
//   - ident.Table != "" matched by alias name (tbl.name == ident.Table)
//   - ident.Table != "" matched by table name (tbl.table.Name == ident.Table)
//   - ident.Table == "", falls through to column-index scan
//   - ident.Table != "" but no table matches, falls through to column-index scan
//
// emitLeafRowSorter branches:
//   - WHERE present (whereSkip != 0 path)
//   - WHERE absent (direct SorterInsert)
//
// All tests use JOIN + ORDER BY to trigger the sorter compilation path.
func TestCompileJoin2Coverage(t *testing.T) {
	runSQLTestsFreshDB(t, buildJoin2CoverageTests())
}

func buildJoin2CoverageTests() []sqlTestCase {
	var tests []sqlTestCase
	tests = append(tests, joinSorterNoWhereTests()...)
	tests = append(tests, joinSorterWithWhereTests()...)
	tests = append(tests, findColumnTableIndexTests()...)
	return tests
}

// joinSorterNoWhereTests exercises emitLeafRowSorter with no WHERE clause.
// The sorter path is triggered whenever a JOIN query has ORDER BY.
func joinSorterNoWhereTests() []sqlTestCase {
	setup := []string{
		"CREATE TABLE alpha(id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO alpha VALUES(1, 'B')",
		"INSERT INTO alpha VALUES(2, 'A')",
		"INSERT INTO alpha VALUES(3, 'C')",
		"CREATE TABLE beta(id INTEGER PRIMARY KEY, alpha_id INTEGER, score INTEGER)",
		"INSERT INTO beta VALUES(1, 1, 30)",
		"INSERT INTO beta VALUES(2, 2, 10)",
		"INSERT INTO beta VALUES(3, 3, 20)",
	}
	return []sqlTestCase{
		// INNER JOIN with ORDER BY — no WHERE — hits emitLeafRowSorter without whereSkip.
		{
			name:  "sorter_inner_join_order_by_no_where",
			setup: setup,
			query: "SELECT alpha.val, beta.score FROM alpha INNER JOIN beta ON alpha.id = beta.alpha_id ORDER BY beta.score",
			wantRows: [][]interface{}{
				{"A", int64(10)},
				{"C", int64(20)},
				{"B", int64(30)},
			},
		},
		// LEFT JOIN with ORDER BY — no WHERE — hits emitLeafRowSorter + emitNullEmissionSorter.
		{
			name:  "sorter_left_join_order_by_no_where",
			setup: setup,
			query: "SELECT alpha.val, beta.score FROM alpha LEFT JOIN beta ON alpha.id = beta.alpha_id ORDER BY alpha.val",
			wantRows: [][]interface{}{
				{"A", int64(10)},
				{"B", int64(30)},
				{"C", int64(20)},
			},
		},
		// LEFT JOIN with ORDER BY, right side has no match — null emission sorter.
		{
			name: "sorter_left_join_order_by_null_right",
			setup: []string{
				"CREATE TABLE p(id INTEGER PRIMARY KEY, name TEXT)",
				"INSERT INTO p VALUES(1, 'X')",
				"INSERT INTO p VALUES(2, 'Y')",
				"CREATE TABLE q(id INTEGER PRIMARY KEY, p_id INTEGER, extra TEXT)",
				"INSERT INTO q VALUES(1, 1, 'Z')",
			},
			query: "SELECT p.name, q.extra FROM p LEFT JOIN q ON p.id = q.p_id ORDER BY p.name",
			wantRows: [][]interface{}{
				{"X", "Z"},
				{"Y", nil},
			},
		},
	}
}

// joinSorterWithWhereTests exercises emitLeafRowSorter WHERE-present branch.
// Rows that fail the WHERE condition must be skipped even under ORDER BY.
func joinSorterWithWhereTests() []sqlTestCase {
	setup := []string{
		"CREATE TABLE cats(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
		"INSERT INTO cats VALUES(1, 'Felix', 3)",
		"INSERT INTO cats VALUES(2, 'Whiskers', 7)",
		"INSERT INTO cats VALUES(3, 'Tom', 5)",
		"CREATE TABLE owners(id INTEGER PRIMARY KEY, cat_id INTEGER, owner TEXT)",
		"INSERT INTO owners VALUES(1, 1, 'Alice')",
		"INSERT INTO owners VALUES(2, 2, 'Bob')",
		"INSERT INTO owners VALUES(3, 3, 'Carol')",
	}
	return []sqlTestCase{
		// INNER JOIN with ORDER BY and WHERE — hits whereSkip branch in emitLeafRowSorter.
		{
			name:  "sorter_inner_join_order_by_with_where",
			setup: setup,
			query: "SELECT cats.name, owners.owner FROM cats INNER JOIN owners ON cats.id = owners.cat_id WHERE cats.age > 4 ORDER BY cats.name",
			wantRows: [][]interface{}{
				{"Tom", "Carol"},
				{"Whiskers", "Bob"},
			},
		},
		// LEFT JOIN with ORDER BY and WHERE — whereSkip inside emitLeafRowSorter.
		{
			name:  "sorter_left_join_order_by_with_where",
			setup: setup,
			query: "SELECT cats.name, owners.owner FROM cats LEFT JOIN owners ON cats.id = owners.cat_id WHERE cats.age >= 5 ORDER BY cats.age",
			wantRows: [][]interface{}{
				{"Tom", "Carol"},
				{"Whiskers", "Bob"},
			},
		},
	}
}

// findColumnTableIndexTests targets the various branches inside
// findColumnTableIndex by exercising different column reference styles.
func findColumnTableIndexTests() []sqlTestCase {
	// Two tables with no column name overlap to ensure table-qualification is needed.
	setup := []string{
		"CREATE TABLE left_t(lid INTEGER PRIMARY KEY, lval TEXT)",
		"INSERT INTO left_t VALUES(1, 'one')",
		"INSERT INTO left_t VALUES(2, 'two')",
		"CREATE TABLE right_t(rid INTEGER PRIMARY KEY, rval TEXT)",
		"INSERT INTO right_t VALUES(1, 'uno')",
		"INSERT INTO right_t VALUES(2, 'dos')",
	}

	// Two tables sharing a column name; unqualified reference exercises the
	// column-index-scan fallthrough in findColumnTableIndex.
	sharedSetup := []string{
		"CREATE TABLE s1(id INTEGER PRIMARY KEY, name TEXT)",
		"INSERT INTO s1 VALUES(10, 'alpha')",
		"CREATE TABLE s2(id INTEGER PRIMARY KEY, score INTEGER)",
		"INSERT INTO s2 VALUES(10, 99)",
	}

	// Tables accessed via alias — hits tbl.name == ident.Table branch.
	aliasSetup := []string{
		"CREATE TABLE dept(id INTEGER PRIMARY KEY, dname TEXT)",
		"INSERT INTO dept VALUES(1, 'Eng')",
		"INSERT INTO dept VALUES(2, 'HR')",
		"CREATE TABLE emp(id INTEGER PRIMARY KEY, dept_id INTEGER, ename TEXT)",
		"INSERT INTO emp VALUES(1, 1, 'Anna')",
		"INSERT INTO emp VALUES(2, 2, 'Ben')",
	}

	return []sqlTestCase{
		// Table-qualified columns via table name (tbl.table.Name == ident.Table).
		// The LEFT JOIN triggers emitNullEmission which calls findColumnTableIndex.
		{
			name:  "findcol_qualified_by_table_name",
			setup: setup,
			query: "SELECT left_t.lval, right_t.rval FROM left_t LEFT JOIN right_t ON left_t.lid = right_t.rid ORDER BY left_t.lid",
			wantRows: [][]interface{}{
				{"one", "uno"},
				{"two", "dos"},
			},
		},
		// Table-qualified columns via alias (tbl.name == ident.Table branch).
		{
			name:  "findcol_qualified_by_alias",
			setup: aliasSetup,
			query: "SELECT e.ename, d.dname FROM emp e LEFT JOIN dept d ON e.dept_id = d.id ORDER BY e.ename",
			wantRows: [][]interface{}{
				{"Anna", "Eng"},
				{"Ben", "HR"},
			},
		},
		// Unqualified column where only one table owns it — column-index scan path.
		{
			name:  "findcol_unqualified_column_scan",
			setup: setup,
			query: "SELECT lval, rval FROM left_t LEFT JOIN right_t ON left_t.lid = right_t.rid ORDER BY left_t.lid",
			wantRows: [][]interface{}{
				{"one", "uno"},
				{"two", "dos"},
			},
		},
		// Non-ident expression column (e.g. literal or arithmetic) — returns 0.
		// Uses ORDER BY to trigger the sorter path and emitLeafRowSorter.
		{
			name:  "findcol_non_ident_expression",
			setup: sharedSetup,
			query: "SELECT s1.name, s1.id + s2.score FROM s1 INNER JOIN s2 ON s1.id = s2.id ORDER BY s1.id",
			wantRows: [][]interface{}{
				{"alpha", int64(109)},
			},
		},
		// Shared column name (id) unqualified — falls through to column-index scan,
		// hits the first table that owns the column (s1).
		{
			name:  "findcol_shared_colname_unqualified",
			setup: sharedSetup,
			query: "SELECT s1.id, s2.score FROM s1 INNER JOIN s2 ON s1.id = s2.id ORDER BY s1.id",
			wantRows: [][]interface{}{
				{int64(10), int64(99)},
			},
		},
	}
}
