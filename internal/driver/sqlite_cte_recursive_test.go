// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import "testing"

// TestCTERecursive tests recursive CTE bytecode execution through the driver layer.
func TestCTERecursive(t *testing.T) {
	tests := buildCTERecursiveTests()
	runSQLTestsFreshDB(t, tests)
}

func buildCTERecursiveTests() []sqlTestCase {
	var tests []sqlTestCase
	tests = append(tests, cteRecursiveCounterTests()...)
	tests = append(tests, cteRecursiveFibonacciTests()...)
	tests = append(tests, cteRecursiveHierarchyTests()...)
	tests = append(tests, cteRecursiveUnionTests()...)
	tests = append(tests, cteRecursiveEdgeCaseTests()...)
	tests = append(tests, cteRecursiveMultiColumnTests()...)
	return tests
}

func cteRecursiveCounterTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name:  "counter_1_to_10",
			query: "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt",
			wantRows: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)},
				{int64(6)}, {int64(7)}, {int64(8)}, {int64(9)}, {int64(10)},
			},
		},
		{
			name:  "counter_step_by_2",
			query: "WITH RECURSIVE cnt(x) AS (SELECT 0 UNION ALL SELECT x+2 FROM cnt WHERE x<8) SELECT x FROM cnt",
			wantRows: [][]interface{}{
				{int64(0)}, {int64(2)}, {int64(4)}, {int64(6)}, {int64(8)},
			},
		},
		{
			name:  "countdown",
			query: "WITH RECURSIVE cnt(x) AS (SELECT 5 UNION ALL SELECT x-1 FROM cnt WHERE x>1) SELECT x FROM cnt",
			wantRows: [][]interface{}{
				{int64(5)}, {int64(4)}, {int64(3)}, {int64(2)}, {int64(1)},
			},
		},
	}
}

func cteRecursiveFibonacciTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name:  "fibonacci_10_terms",
			query: "WITH RECURSIVE fib(n, a, b) AS (SELECT 0, 0, 1 UNION ALL SELECT n+1, b, a+b FROM fib WHERE n<9) SELECT a FROM fib",
			wantRows: [][]interface{}{
				{int64(0)}, {int64(1)}, {int64(1)}, {int64(2)}, {int64(3)},
				{int64(5)}, {int64(8)}, {int64(13)}, {int64(21)}, {int64(34)},
			},
		},
		{
			name:  "fibonacci_pairs",
			query: "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a+b FROM fib WHERE b<20) SELECT a, b FROM fib",
			wantRows: [][]interface{}{
				{int64(0), int64(1)},
				{int64(1), int64(1)},
				{int64(1), int64(2)},
				{int64(2), int64(3)},
				{int64(3), int64(5)},
				{int64(5), int64(8)},
				{int64(8), int64(13)},
				{int64(13), int64(21)},
			},
		},
	}
}

func cteRecursiveHierarchyTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "org_chart_walk",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, name TEXT, manager_id INTEGER)",
				"INSERT INTO employees VALUES(1, 'CEO', NULL)",
				"INSERT INTO employees VALUES(2, 'VP_Eng', 1)",
				"INSERT INTO employees VALUES(3, 'VP_Sales', 1)",
				"INSERT INTO employees VALUES(4, 'Dev_Lead', 2)",
				"INSERT INTO employees VALUES(5, 'Dev1', 4)",
			},
			query: `WITH RECURSIVE org(id, name, depth) AS (
				SELECT id, name, 0 FROM employees WHERE manager_id IS NULL
				UNION ALL
				SELECT e.id, e.name, o.depth+1
				FROM employees e JOIN org o ON e.manager_id = o.id
			) SELECT id, name, depth FROM org ORDER BY id`,
			wantRows: [][]interface{}{
				{int64(1), "CEO", int64(0)},
				{int64(2), "VP_Eng", int64(1)},
				{int64(3), "VP_Sales", int64(1)},
				{int64(4), "Dev_Lead", int64(2)},
				{int64(5), "Dev1", int64(3)},
			},
		},
		{
			name: "subtree_walk_from_node",
			setup: []string{
				"CREATE TABLE tree(id INTEGER, parent_id INTEGER)",
				"INSERT INTO tree VALUES(1, 0)",
				"INSERT INTO tree VALUES(2, 1)",
				"INSERT INTO tree VALUES(3, 1)",
				"INSERT INTO tree VALUES(4, 2)",
				"INSERT INTO tree VALUES(5, 4)",
			},
			query: `WITH RECURSIVE sub(id) AS (
				SELECT 2
				UNION ALL
				SELECT t.id FROM tree t JOIN sub s ON t.parent_id = s.id
			) SELECT id FROM sub ORDER BY id`,
			wantRows: [][]interface{}{
				{int64(2)}, {int64(4)}, {int64(5)},
			},
		},
	}
}

func cteRecursiveUnionTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name:  "union_all_allows_duplicates",
			query: "WITH RECURSIVE r(n, x) AS (SELECT 1, 5 UNION ALL SELECT n+1, 5 FROM r WHERE n<3) SELECT x FROM r",
			wantRows: [][]interface{}{
				{int64(5)}, {int64(5)}, {int64(5)},
			},
		},
		{
			name: "union_deduplicates",
			// UNION dedup now implemented
			query: `WITH RECURSIVE r(x) AS (
				SELECT 1
				UNION
				SELECT (x % 3) + 1 FROM r WHERE x < 5
			) SELECT x FROM r ORDER BY x`,
			wantRows: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(3)},
			},
		},
	}
}

func cteRecursiveEdgeCaseTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name:  "empty_recursive_result",
			query: "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<0) SELECT x FROM r",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name:  "anchor_only_no_recursion",
			query: "WITH RECURSIVE r(x) AS (SELECT 42 UNION ALL SELECT x+1 FROM r WHERE 0) SELECT x FROM r",
			wantRows: [][]interface{}{
				{int64(42)},
			},
		},
		{
			name:  "single_iteration",
			query: "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<2) SELECT x FROM r",
			wantRows: [][]interface{}{
				{int64(1)}, {int64(2)},
			},
		},
	}
}

func cteRecursiveMultiColumnTests() []sqlTestCase {
	return []sqlTestCase{
		{
			name: "three_columns_arithmetic",
			query: `WITH RECURSIVE t(a, b, c) AS (
				SELECT 1, 10, 100
				UNION ALL
				SELECT a+1, b+10, c+100 FROM t WHERE a<4
			) SELECT a, b, c FROM t`,
			wantRows: [][]interface{}{
				{int64(1), int64(10), int64(100)},
				{int64(2), int64(20), int64(200)},
				{int64(3), int64(30), int64(300)},
				{int64(4), int64(40), int64(400)},
			},
		},
		{
			name: "two_columns_string_and_int",
			query: `WITH RECURSIVE t(n, s) AS (
				SELECT 1, 'a'
				UNION ALL
				SELECT n+1, s || 'a' FROM t WHERE n<4
			) SELECT n, s FROM t`,
			wantRows: [][]interface{}{
				{int64(1), "a"},
				{int64(2), "aa"},
				{int64(3), "aaa"},
				{int64(4), "aaaa"},
			},
		},
		{
			name: "factorial_two_columns",
			query: `WITH RECURSIVE fact(n, f) AS (
				SELECT 1, 1
				UNION ALL
				SELECT n+1, f*(n+1) FROM fact WHERE n<6
			) SELECT n, f FROM fact`,
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(3), int64(6)},
				{int64(4), int64(24)},
				{int64(5), int64(120)},
				{int64(6), int64(720)},
			},
		},
	}
}
