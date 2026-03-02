// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteCTE tests Common Table Expressions (CTEs) including basic WITH, recursive, and complex queries
func TestSQLiteCTE(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
		skip     string
	}{
		// Basic WITH clause tests
		{
			name: "basic_with_single_select",
			setup: []string{
				"CREATE TABLE t1(x INTEGER, y INTEGER)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
			},
			query: "WITH tmp AS (SELECT * FROM t1) SELECT * FROM tmp",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(20)},
			},
		},
		{
			name: "with_clause_simple_constant",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
			},
			query: "WITH tmp(a) AS (SELECT 42) SELECT a FROM tmp",
			wantRows: [][]interface{}{
				{int64(42)},
			},
		},
		{
			name: "with_unused_cte",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1)",
			},
			query: "WITH tmp(a) AS (SELECT * FROM t1) SELECT 10",
			wantRows: [][]interface{}{
				{int64(10)},
			},
		},
		// CTE with column names
		{
			name: "cte_with_explicit_column_names",
			setup: []string{
				"CREATE TABLE t1(x INTEGER, y INTEGER)",
				"INSERT INTO t1 VALUES(1, 2)",
				"INSERT INTO t1 VALUES(3, 4)",
			},
			query: "WITH tmp(a, b) AS (SELECT x, y FROM t1) SELECT a, b FROM tmp",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
				{int64(3), int64(4)},
			},
		},
		{
			name: "cte_column_rename",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, price REAL)",
				"INSERT INTO products VALUES(1, 'Widget', 10.0)",
				"INSERT INTO products VALUES(2, 'Gadget', 20.0)",
			},
			query: "WITH renamed(product_id, product_name) AS (SELECT id, name FROM products) SELECT product_id, product_name FROM renamed",
			wantRows: [][]interface{}{
				{int64(1), "Widget"},
				{int64(2), "Gadget"},
			},
		},
		// Multiple CTEs in single query
		{
			name: "multiple_ctes_independent",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"CREATE TABLE t2(y INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
			},
			query: "WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM t2) SELECT x FROM cte1 UNION ALL SELECT y FROM cte2",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		{
			name: "multiple_ctes_values",
			setup: []string{},
			query: "WITH cte1(a) AS (VALUES(1)), cte2(b) AS (VALUES(2)) SELECT a+b FROM cte1, cte2",
			wantRows: [][]interface{}{
				{int64(3)},
			},
		},
		// CTE referencing another CTE
		{
			name: "cte_forward_reference",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "WITH tmp1(a) AS (SELECT * FROM t1), tmp2(b) AS (SELECT * FROM tmp1) SELECT * FROM tmp2",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		{
			name: "cte_chain_three_deep",
			setup: []string{},
			query: "WITH cte1(a) AS (VALUES(10)), cte2(b) AS (SELECT a*2 FROM cte1), cte3(c) AS (SELECT b*3 FROM cte2) SELECT c FROM cte3",
			wantRows: [][]interface{}{
				{int64(60)},
			},
		},
		// Recursive CTEs with UNION ALL
		{
			name: "recursive_simple_counter",
			setup: []string{},
			query: "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5) SELECT x FROM cnt",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{int64(4)},
				{int64(5)},
			},
		},
		{
			name: "recursive_counter_limit",
			setup: []string{},
			query: "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt LIMIT 3",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		{
			name: "recursive_counter_with_step",
			setup: []string{},
			query: "WITH RECURSIVE cnt(x) AS (VALUES(0) UNION ALL SELECT x+2 FROM cnt WHERE x<8) SELECT x FROM cnt",
			wantRows: [][]interface{}{
				{int64(0)},
				{int64(2)},
				{int64(4)},
				{int64(6)},
				{int64(8)},
			},
		},
		// Recursive CTE with UNION (removes duplicates)
		{
			name: "recursive_union_distinct",
			setup: []string{},
			query: "WITH RECURSIVE cnt(x) AS (VALUES(1) UNION SELECT (x+1)%5 FROM cnt WHERE x<10) SELECT x FROM cnt ORDER BY x",
			wantRows: [][]interface{}{
				{int64(0)},
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{int64(4)},
			},
		},
		// CTE in INSERT...SELECT
		{
			name: "cte_in_insert_select",
			setup: []string{
				"CREATE TABLE dest(id INTEGER, value INTEGER)",
				"CREATE TABLE source(x INTEGER, y INTEGER)",
				"INSERT INTO source VALUES(1, 100)",
				"INSERT INTO source VALUES(2, 200)",
			},
			query: "WITH filtered AS (SELECT x, y FROM source WHERE x > 0) INSERT INTO dest SELECT * FROM filtered",
			wantRows: [][]interface{}{
				{int64(1), int64(100)},
				{int64(2), int64(200)},
			},
		},
		{
			name: "cte_insert_with_values",
			setup: []string{
				"CREATE TABLE numbers(n INTEGER)",
			},
			query: "WITH cte(x) AS (VALUES(1),(2),(3)) INSERT INTO numbers SELECT x FROM cte",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// CTE in UPDATE
		{
			name: "cte_in_update",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
			},
			query: "WITH doubled AS (SELECT id, value*2 AS v2 FROM t1) UPDATE t1 SET value = (SELECT v2 FROM doubled WHERE doubled.id = t1.id)",
			wantRows: [][]interface{}{
				{int64(1), int64(20)},
				{int64(2), int64(40)},
				{int64(3), int64(60)},
			},
		},
		// CTE in DELETE
		{
			name: "cte_in_delete",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value INTEGER)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
				"INSERT INTO t1 VALUES(4, 40)",
			},
			query: "WITH to_delete AS (SELECT id FROM t1 WHERE value > 25) DELETE FROM t1 WHERE id IN (SELECT id FROM to_delete)",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(20)},
			},
		},
		// CTE with complex expressions
		{
			name: "cte_complex_expressions",
			setup: []string{
				"CREATE TABLE sales(product_id INTEGER, quantity INTEGER, price REAL)",
				"INSERT INTO sales VALUES(1, 10, 5.50)",
				"INSERT INTO sales VALUES(2, 5, 12.00)",
				"INSERT INTO sales VALUES(3, 8, 7.25)",
			},
			query: "WITH totals AS (SELECT product_id, quantity * price AS total FROM sales) SELECT product_id, total FROM totals WHERE total > 50",
			wantRows: [][]interface{}{
				{int64(1), 55.0},
				{int64(2), 60.0},
				{int64(3), 58.0},
			},
		},
		{
			name: "cte_aggregates",
			skip: "Known issue: GROUP BY aggregate in subquery/CTE/view causes infinite loop in VDBE",
			setup: []string{
				"CREATE TABLE orders(customer_id INTEGER, amount REAL)",
				"INSERT INTO orders VALUES(1, 100.0)",
				"INSERT INTO orders VALUES(1, 150.0)",
				"INSERT INTO orders VALUES(2, 200.0)",
			},
			query: "WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) SELECT * FROM totals ORDER BY customer_id",
			wantRows: [][]interface{}{
				{int64(1), 250.0},
				{int64(2), 200.0},
			},
		},
		// Hierarchical data traversal
		{
			name: "recursive_tree_traversal",
			skip: "Known issue: VDBE infinite loop with recursive CTE tree traversal using IS NULL",
			setup: []string{
				"CREATE TABLE tree(id INTEGER, parent_id INTEGER, name TEXT)",
				"INSERT INTO tree VALUES(1, NULL, 'root')",
				"INSERT INTO tree VALUES(2, 1, 'child1')",
				"INSERT INTO tree VALUES(3, 1, 'child2')",
				"INSERT INTO tree VALUES(4, 2, 'grandchild1')",
				"INSERT INTO tree VALUES(5, 2, 'grandchild2')",
			},
			query: "WITH RECURSIVE descendants(id, name, level) AS (SELECT id, name, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, d.level+1 FROM tree t JOIN descendants d ON t.parent_id = d.id) SELECT id, name, level FROM descendants ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), "root", int64(0)},
				{int64(2), "child1", int64(1)},
				{int64(3), "child2", int64(1)},
				{int64(4), "grandchild1", int64(2)},
				{int64(5), "grandchild2", int64(2)},
			},
		},
		{
			name: "recursive_subtree",
			setup: []string{
				"CREATE TABLE tree(id INTEGER, parent_id INTEGER)",
				"INSERT INTO tree VALUES(1, NULL)",
				"INSERT INTO tree VALUES(2, 1)",
				"INSERT INTO tree VALUES(3, 1)",
				"INSERT INTO tree VALUES(4, 2)",
				"INSERT INTO tree VALUES(5, 4)",
			},
			query: "WITH RECURSIVE subtree(id) AS (VALUES(2) UNION ALL SELECT t.id FROM tree t JOIN subtree s ON t.parent_id = s.id) SELECT id FROM subtree ORDER BY id",
			wantRows: [][]interface{}{
				{int64(2)},
				{int64(4)},
				{int64(5)},
			},
		},
		// Fibonacci sequence
		{
			name: "recursive_fibonacci",
			setup: []string{},
			query: "WITH RECURSIVE fib(n, a, b) AS (VALUES(0, 0, 1) UNION ALL SELECT n+1, b, a+b FROM fib WHERE n < 7) SELECT a FROM fib",
			wantRows: [][]interface{}{
				{int64(0)},
				{int64(1)},
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{int64(5)},
				{int64(8)},
				{int64(13)},
			},
		},
		// Factorial
		{
			name: "recursive_factorial",
			setup: []string{},
			query: "WITH RECURSIVE fact(n, f) AS (VALUES(1, 1) UNION ALL SELECT n+1, f*(n+1) FROM fact WHERE n < 5) SELECT n, f FROM fact",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
				{int64(3), int64(6)},
				{int64(4), int64(24)},
				{int64(5), int64(120)},
			},
		},
		// Nested CTEs
		{
			name: "nested_cte_basic",
			setup: []string{},
			query: "WITH outer_cte AS (WITH inner_cte(x) AS (VALUES(10)) SELECT x*2 AS y FROM inner_cte) SELECT y FROM outer_cte",
			wantRows: [][]interface{}{
				{int64(20)},
			},
		},
		{
			name: "nested_cte_multiple",
			setup: []string{},
			query: "WITH outer AS (WITH inner1(a) AS (VALUES(1)), inner2(b) AS (VALUES(2)) SELECT a+b AS c FROM inner1, inner2) SELECT c*3 FROM outer",
			wantRows: [][]interface{}{
				{int64(9)},
			},
		},
		// CTE with JOINs
		{
			name: "cte_with_join",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, name TEXT, dept_id INTEGER)",
				"CREATE TABLE departments(id INTEGER, name TEXT)",
				"INSERT INTO employees VALUES(1, 'Alice', 10)",
				"INSERT INTO employees VALUES(2, 'Bob', 20)",
				"INSERT INTO departments VALUES(10, 'Engineering')",
				"INSERT INTO departments VALUES(20, 'Sales')",
			},
			query: "WITH emp_dept AS (SELECT e.name AS emp_name, d.name AS dept_name FROM employees e JOIN departments d ON e.dept_id = d.id) SELECT * FROM emp_dept ORDER BY emp_name",
			wantRows: [][]interface{}{
				{"Alice", "Engineering"},
				{"Bob", "Sales"},
			},
		},
		// CTE with subquery
		{
			name: "cte_in_subquery",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM (WITH tmp AS (SELECT x*2 AS doubled FROM t1) SELECT doubled FROM tmp)",
			wantRows: [][]interface{}{
				{int64(2)},
				{int64(4)},
			},
		},
		// CTE with WHERE clause
		{
			name: "cte_with_where",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, price REAL)",
				"INSERT INTO products VALUES(1, 'Widget', 10.0)",
				"INSERT INTO products VALUES(2, 'Gadget', 20.0)",
				"INSERT INTO products VALUES(3, 'Doohickey', 15.0)",
			},
			query: "WITH expensive AS (SELECT * FROM products WHERE price >= 15.0) SELECT name, price FROM expensive ORDER BY price",
			wantRows: [][]interface{}{
				{"Doohickey", 15.0},
				{"Gadget", 20.0},
			},
		},
		// CTE with ORDER BY and LIMIT
		{
			name: "cte_with_order_limit",
			setup: []string{
				"CREATE TABLE scores(player TEXT, score INTEGER)",
				"INSERT INTO scores VALUES('Alice', 100)",
				"INSERT INTO scores VALUES('Bob', 85)",
				"INSERT INTO scores VALUES('Charlie', 95)",
				"INSERT INTO scores VALUES('Dave', 110)",
			},
			query: "WITH top_scores AS (SELECT * FROM scores ORDER BY score DESC LIMIT 2) SELECT player, score FROM top_scores ORDER BY score DESC",
			wantRows: [][]interface{}{
				{"Dave", int64(110)},
				{"Alice", int64(100)},
			},
		},
		// Graph traversal
		{
			name: "recursive_graph_traversal",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE edges(from_node INTEGER, to_node INTEGER)",
				"INSERT INTO edges VALUES(1, 2)",
				"INSERT INTO edges VALUES(1, 3)",
				"INSERT INTO edges VALUES(2, 4)",
				"INSERT INTO edges VALUES(3, 4)",
				"INSERT INTO edges VALUES(4, 5)",
			},
			query: "WITH RECURSIVE reachable(node) AS (VALUES(1) UNION SELECT e.to_node FROM edges e JOIN reachable r ON e.from_node = r.node) SELECT DISTINCT node FROM reachable ORDER BY node",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{int64(4)},
				{int64(5)},
			},
		},
		// Path finding
		{
			name: "recursive_path_building",
			skip: "Known issue: VDBE infinite loop with recursive CTE using IS NULL base case",
			setup: []string{
				"CREATE TABLE filesystem(id INTEGER, parent_id INTEGER, name TEXT)",
				"INSERT INTO filesystem VALUES(1, NULL, 'root')",
				"INSERT INTO filesystem VALUES(2, 1, 'home')",
				"INSERT INTO filesystem VALUES(3, 1, 'etc')",
				"INSERT INTO filesystem VALUES(4, 2, 'user')",
				"INSERT INTO filesystem VALUES(5, 4, 'docs')",
			},
			query: "WITH RECURSIVE paths(id, path) AS (SELECT id, name FROM filesystem WHERE parent_id IS NULL UNION ALL SELECT f.id, p.path || '/' || f.name FROM filesystem f JOIN paths p ON f.parent_id = p.id) SELECT path FROM paths WHERE id = 5",
			wantRows: [][]interface{}{
				{"root/home/user/docs"},
			},
		},
		// CTE with CASE expressions
		{
			name: "cte_with_case",
			setup: []string{
				"CREATE TABLE grades(student TEXT, score INTEGER)",
				"INSERT INTO grades VALUES('Alice', 95)",
				"INSERT INTO grades VALUES('Bob', 82)",
				"INSERT INTO grades VALUES('Charlie', 78)",
			},
			query: "WITH graded AS (SELECT student, score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END AS grade FROM grades) SELECT student, grade FROM graded ORDER BY score DESC",
			wantRows: [][]interface{}{
				{"Alice", "A"},
				{"Bob", "B"},
				{"Charlie", "C"},
			},
		},
		// Multiple recursive references
		{
			name: "recursive_multiple_base_cases",
			setup: []string{},
			query: "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT n+2 FROM nums WHERE n < 6) SELECT DISTINCT n FROM nums ORDER BY n",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
				{int64(4)},
				{int64(5)},
				{int64(6)},
				{int64(7)},
			},
		},
		// CTE with UNION of different sources
		{
			name: "cte_union_multiple_tables",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, name TEXT)",
				"CREATE TABLE t2(id INTEGER, name TEXT)",
				"INSERT INTO t1 VALUES(1, 'A')",
				"INSERT INTO t2 VALUES(2, 'B')",
			},
			query: "WITH combined AS (SELECT * FROM t1 UNION ALL SELECT * FROM t2) SELECT * FROM combined ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), "A"},
				{int64(2), "B"},
			},
		},
		// CTE with multiple columns and operations
		{
			name: "cte_multiple_columns_operations",
			setup: []string{
				"CREATE TABLE measurements(id INTEGER, temp REAL, humidity REAL)",
				"INSERT INTO measurements VALUES(1, 20.5, 65.0)",
				"INSERT INTO measurements VALUES(2, 22.0, 70.0)",
				"INSERT INTO measurements VALUES(3, 19.5, 60.0)",
			},
			query: "WITH stats AS (SELECT id, temp, humidity, temp * 1.8 + 32 AS temp_f FROM measurements) SELECT id, temp_f FROM stats WHERE temp_f > 68.0 ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), 68.9},
				{int64(2), 71.6},
			},
		},
		// Recursive depth calculation
		{
			name: "recursive_depth_calculation",
			skip: "Known issue: VDBE infinite loop with recursive CTE using IS NULL base case",
			setup: []string{
				"CREATE TABLE org(id INTEGER, manager_id INTEGER, name TEXT)",
				"INSERT INTO org VALUES(1, NULL, 'CEO')",
				"INSERT INTO org VALUES(2, 1, 'VP1')",
				"INSERT INTO org VALUES(3, 1, 'VP2')",
				"INSERT INTO org VALUES(4, 2, 'Manager1')",
				"INSERT INTO org VALUES(5, 4, 'Employee1')",
			},
			query: "WITH RECURSIVE org_depth(id, depth) AS (SELECT id, 0 FROM org WHERE manager_id IS NULL UNION ALL SELECT o.id, od.depth+1 FROM org o JOIN org_depth od ON o.manager_id = od.id) SELECT id, depth FROM org_depth ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), int64(0)},
				{int64(2), int64(1)},
				{int64(3), int64(1)},
				{int64(4), int64(2)},
				{int64(5), int64(3)},
			},
		},
		// CTE with string operations
		{
			name: "cte_string_operations",
			setup: []string{
				"CREATE TABLE names(first TEXT, last TEXT)",
				"INSERT INTO names VALUES('John', 'Doe')",
				"INSERT INTO names VALUES('Jane', 'Smith')",
			},
			query: "WITH full_names AS (SELECT first || ' ' || last AS full FROM names) SELECT full FROM full_names ORDER BY full",
			wantRows: [][]interface{}{
				{"Jane Smith"},
				{"John Doe"},
			},
		},
		// Recursive with multiple columns
		{
			name: "recursive_multiple_columns",
			setup: []string{},
			query: "WITH RECURSIVE nums(x, y) AS (SELECT 1, 10 UNION ALL SELECT x+1, y+10 FROM nums WHERE x < 3) SELECT x, y FROM nums",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(20)},
				{int64(3), int64(30)},
			},
		},
		// CTE with DISTINCT
		{
			name: "cte_with_distinct",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE duplicates(value INTEGER)",
				"INSERT INTO duplicates VALUES(1)",
				"INSERT INTO duplicates VALUES(2)",
				"INSERT INTO duplicates VALUES(1)",
				"INSERT INTO duplicates VALUES(3)",
				"INSERT INTO duplicates VALUES(2)",
			},
			query: "WITH unique_vals AS (SELECT DISTINCT value FROM duplicates) SELECT value FROM unique_vals ORDER BY value",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// Recursive number generation
		{
			name: "recursive_number_series",
			setup: []string{},
			query: "WITH RECURSIVE series(n) AS (SELECT 10 UNION ALL SELECT n-1 FROM series WHERE n > 5) SELECT n FROM series ORDER BY n",
			wantRows: [][]interface{}{
				{int64(5)},
				{int64(6)},
				{int64(7)},
				{int64(8)},
				{int64(9)},
				{int64(10)},
			},
		},
		// CTE with date/time calculations (using integers as timestamps)
		{
			name: "cte_time_calculations",
			setup: []string{
				"CREATE TABLE events(id INTEGER, timestamp INTEGER)",
				"INSERT INTO events VALUES(1, 1000)",
				"INSERT INTO events VALUES(2, 2000)",
				"INSERT INTO events VALUES(3, 3000)",
			},
			query: "WITH recent AS (SELECT id, timestamp FROM events WHERE timestamp > 1500) SELECT id FROM recent ORDER BY timestamp",
			wantRows: [][]interface{}{
				{int64(2)},
				{int64(3)},
			},
		},
		// Recursive with arithmetic progression
		{
			name: "recursive_arithmetic_progression",
			setup: []string{},
			query: "WITH RECURSIVE ap(n, term) AS (SELECT 1, 5 UNION ALL SELECT n+1, term+3 FROM ap WHERE n < 5) SELECT term FROM ap",
			wantRows: [][]interface{}{
				{int64(5)},
				{int64(8)},
				{int64(11)},
				{int64(14)},
				{int64(17)},
			},
		},
		// CTE with NULL handling
		{
			name: "cte_null_handling",
			setup: []string{
				"CREATE TABLE data(id INTEGER, value INTEGER)",
				"INSERT INTO data VALUES(1, 10)",
				"INSERT INTO data VALUES(2, NULL)",
				"INSERT INTO data VALUES(3, 30)",
			},
			query: "WITH filtered AS (SELECT id, COALESCE(value, 0) AS val FROM data) SELECT id, val FROM filtered ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), int64(10)},
				{int64(2), int64(0)},
				{int64(3), int64(30)},
			},
		},
		// Recursive with geometric progression
		{
			name: "recursive_geometric_progression",
			setup: []string{},
			query: "WITH RECURSIVE gp(n, term) AS (SELECT 1, 2 UNION ALL SELECT n+1, term*2 FROM gp WHERE n < 5) SELECT term FROM gp",
			wantRows: [][]interface{}{
				{int64(2)},
				{int64(4)},
				{int64(8)},
				{int64(16)},
				{int64(32)},
			},
		},
		// CTE with multiple JOINs
		{
			name: "cte_multiple_joins",
			setup: []string{
				"CREATE TABLE students(id INTEGER, name TEXT)",
				"CREATE TABLE courses(id INTEGER, title TEXT)",
				"CREATE TABLE enrollments(student_id INTEGER, course_id INTEGER)",
				"INSERT INTO students VALUES(1, 'Alice')",
				"INSERT INTO students VALUES(2, 'Bob')",
				"INSERT INTO courses VALUES(10, 'Math')",
				"INSERT INTO courses VALUES(20, 'Physics')",
				"INSERT INTO enrollments VALUES(1, 10)",
				"INSERT INTO enrollments VALUES(2, 20)",
			},
			query: "WITH enrolled AS (SELECT s.name, c.title FROM students s JOIN enrollments e ON s.id = e.student_id JOIN courses c ON e.course_id = c.id) SELECT * FROM enrolled ORDER BY name",
			wantRows: [][]interface{}{
				{"Alice", "Math"},
				{"Bob", "Physics"},
			},
		},
		// Recursive ancestors
		{
			name: "recursive_ancestors",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE person(id INTEGER, parent_id INTEGER, name TEXT)",
				"INSERT INTO person VALUES(1, NULL, 'Grandparent')",
				"INSERT INTO person VALUES(2, 1, 'Parent')",
				"INSERT INTO person VALUES(3, 2, 'Child')",
			},
			query: "WITH RECURSIVE ancestors(id, name) AS (SELECT id, name FROM person WHERE id = 3 UNION ALL SELECT p.id, p.name FROM person p JOIN ancestors a ON p.id = a.id OR p.id IN (SELECT parent_id FROM person WHERE id = a.id)) SELECT DISTINCT name FROM ancestors ORDER BY id",
			wantRows: [][]interface{}{
				{"Grandparent"},
				{"Parent"},
				{"Child"},
			},
		},
		// CTE with HAVING clause
		{
			name: "cte_with_having",
			skip: "Known issue: HAVING clause causes VDBE infinite loop",
			setup: []string{
				"CREATE TABLE sales(product_id INTEGER, amount REAL)",
				"INSERT INTO sales VALUES(1, 100.0)",
				"INSERT INTO sales VALUES(1, 150.0)",
				"INSERT INTO sales VALUES(2, 50.0)",
				"INSERT INTO sales VALUES(2, 75.0)",
			},
			query: "WITH totals AS (SELECT product_id, SUM(amount) AS total FROM sales GROUP BY product_id HAVING SUM(amount) > 150) SELECT * FROM totals",
			wantRows: [][]interface{}{
				{int64(1), 250.0},
			},
		},
		// Recursive binary tree traversal
		{
			name: "recursive_binary_tree",
			skip: "DISTINCT not yet implemented",
			setup: []string{
				"CREATE TABLE btree(id INTEGER, left_id INTEGER, right_id INTEGER, value TEXT)",
				"INSERT INTO btree VALUES(1, 2, 3, 'root')",
				"INSERT INTO btree VALUES(2, 4, 5, 'left')",
				"INSERT INTO btree VALUES(3, NULL, NULL, 'right')",
				"INSERT INTO btree VALUES(4, NULL, NULL, 'left-left')",
				"INSERT INTO btree VALUES(5, NULL, NULL, 'left-right')",
			},
			query: "WITH RECURSIVE traverse(id, value) AS (SELECT id, value FROM btree WHERE id = 1 UNION ALL SELECT b.id, b.value FROM btree b JOIN traverse t ON b.id = t.id OR b.id IN (SELECT left_id FROM btree WHERE id = t.id UNION SELECT right_id FROM btree WHERE id = t.id)) SELECT DISTINCT value FROM traverse",
			wantRows: [][]interface{}{
				{"root"},
				{"left"},
				{"right"},
				{"left-left"},
				{"left-right"},
			},
		},
		// CTE with IN clause
		{
			name: "cte_with_in_clause",
			setup: []string{
				"CREATE TABLE items(id INTEGER, category TEXT)",
				"INSERT INTO items VALUES(1, 'A')",
				"INSERT INTO items VALUES(2, 'B')",
				"INSERT INTO items VALUES(3, 'A')",
				"INSERT INTO items VALUES(4, 'C')",
			},
			query: "WITH selected AS (SELECT * FROM items WHERE category IN ('A', 'B')) SELECT id FROM selected ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// Recursive date range generation (using integers)
		{
			name: "recursive_date_range",
			setup: []string{},
			query: "WITH RECURSIVE dates(d) AS (SELECT 20230101 UNION ALL SELECT d+1 FROM dates WHERE d < 20230105) SELECT d FROM dates",
			wantRows: [][]interface{}{
				{int64(20230101)},
				{int64(20230102)},
				{int64(20230103)},
				{int64(20230104)},
				{int64(20230105)},
			},
		},
		// CTE with EXISTS
		{
			name: "cte_with_exists",
			setup: []string{
				"CREATE TABLE orders(id INTEGER, customer_id INTEGER)",
				"CREATE TABLE customers(id INTEGER, name TEXT)",
				"INSERT INTO customers VALUES(1, 'Alice')",
				"INSERT INTO customers VALUES(2, 'Bob')",
				"INSERT INTO orders VALUES(1, 1)",
			},
			query: "WITH has_orders AS (SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)) SELECT name FROM has_orders",
			wantRows: [][]interface{}{
				{"Alice"},
			},
		},
		// Multiple CTEs with different complexity
		{
			name: "multiple_ctes_complex",
			setup: []string{
				"CREATE TABLE base(id INTEGER, val INTEGER)",
				"INSERT INTO base VALUES(1, 10)",
				"INSERT INTO base VALUES(2, 20)",
			},
			query: "WITH cte1 AS (SELECT id, val FROM base), cte2 AS (SELECT id, val*2 AS double_val FROM cte1), cte3 AS (SELECT id, double_val*3 AS result FROM cte2) SELECT id, result FROM cte3 ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), int64(60)},
				{int64(2), int64(120)},
			},
		},
		// Recursive with string concatenation
		{
			name: "recursive_string_concat",
			setup: []string{},
			query: "WITH RECURSIVE str(n, s) AS (SELECT 1, 'A' UNION ALL SELECT n+1, s || 'B' FROM str WHERE n < 3) SELECT s FROM str",
			wantRows: [][]interface{}{
				{"A"},
				{"AB"},
				{"ABB"},
			},
		},
		// CTE with NOT IN
		{
			name: "cte_with_not_in",
			setup: []string{
				"CREATE TABLE all_items(id INTEGER)",
				"CREATE TABLE excluded(id INTEGER)",
				"INSERT INTO all_items VALUES(1)",
				"INSERT INTO all_items VALUES(2)",
				"INSERT INTO all_items VALUES(3)",
				"INSERT INTO excluded VALUES(2)",
			},
			query: "WITH filtered AS (SELECT id FROM all_items WHERE id NOT IN (SELECT id FROM excluded)) SELECT id FROM filtered ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(3)},
			},
		},
		// Recursive level-based filtering
		{
			name: "recursive_level_filter",
			skip: "Known issue: VDBE infinite loop with recursive CTE using IS NULL base case",
			setup: []string{
				"CREATE TABLE hierarchy(id INTEGER, parent_id INTEGER)",
				"INSERT INTO hierarchy VALUES(1, NULL)",
				"INSERT INTO hierarchy VALUES(2, 1)",
				"INSERT INTO hierarchy VALUES(3, 1)",
				"INSERT INTO hierarchy VALUES(4, 2)",
			},
			query: "WITH RECURSIVE levels(id, level) AS (SELECT id, 0 FROM hierarchy WHERE parent_id IS NULL UNION ALL SELECT h.id, l.level+1 FROM hierarchy h JOIN levels l ON h.parent_id = l.id) SELECT id FROM levels WHERE level < 2 ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
				{int64(3)},
			},
		},
		// CTE with MIN/MAX aggregates
		{
			name: "cte_min_max_aggregates",
			setup: []string{
				"CREATE TABLE values(category TEXT, amount REAL)",
				"INSERT INTO values VALUES('A', 10.0)",
				"INSERT INTO values VALUES('A', 20.0)",
				"INSERT INTO values VALUES('B', 15.0)",
				"INSERT INTO values VALUES('B', 25.0)",
			},
			query: "WITH ranges AS (SELECT category, MIN(amount) AS min_val, MAX(amount) AS max_val FROM values GROUP BY category) SELECT category, min_val, max_val FROM ranges ORDER BY category",
			wantRows: [][]interface{}{
				{"A", 10.0, 20.0},
				{"B", 15.0, 25.0},
			},
		},

		// Error cases
		{
			name:    "error_column_count_mismatch",
			setup:   []string{},
			query:   "WITH cte(a, b) AS (VALUES(1)) SELECT * FROM cte",
			wantErr: true,
		},
		{
			name: "error_duplicate_cte_name",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
			},
			query:   "WITH cte AS (SELECT 1), cte AS (SELECT 2) SELECT * FROM cte",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			db, err := sql.Open(DriverName, dbPath)
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			// Execute setup statements
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed on statement %q: %v", stmt, err)
				}
			}

			// For INSERT/UPDATE/DELETE queries, execute first and then query results
			isModifyingQuery := false
			var verifyQuery string

			if len(tt.wantRows) > 0 {
				// Check if this is a modifying query (INSERT, UPDATE, DELETE)
				if stmt := tt.query; len(stmt) >= 6 {
					prefix := stmt[:6]
					if prefix == "WITH d" || prefix == "WITH c" || prefix == "WITH f" || prefix == "WITH t" || prefix == "WITH u" {
						// Could be DELETE, INSERT, UPDATE with CTE
						if contains := func(s, substr string) bool {
							for i := 0; i+len(substr) <= len(s); i++ {
								if s[i:i+len(substr)] == substr {
									return true
								}
							}
							return false
						}; contains(stmt, "INSERT INTO") {
							isModifyingQuery = true
							// Extract table name from INSERT INTO
							start := 0
							for i := 0; i < len(stmt); i++ {
								if i+11 <= len(stmt) && stmt[i:i+11] == "INSERT INTO" {
									start = i + 12
									break
								}
							}
							if start > 0 {
								end := start
								for end < len(stmt) && stmt[end] != ' ' && stmt[end] != '(' {
									end++
								}
								tableName := stmt[start:end]
								verifyQuery = "SELECT * FROM " + tableName + " ORDER BY 1"
							}
						} else if contains(stmt, "UPDATE") {
							isModifyingQuery = true
							// Extract table name from UPDATE
							start := 0
							for i := 0; i < len(stmt); i++ {
								if i+6 <= len(stmt) && stmt[i:i+6] == "UPDATE" {
									start = i + 7
									break
								}
							}
							if start > 0 {
								end := start
								for end < len(stmt) && stmt[end] != ' ' {
									end++
								}
								tableName := stmt[start:end]
								verifyQuery = "SELECT * FROM " + tableName + " ORDER BY 1"
							}
						} else if contains(stmt, "DELETE FROM") {
							isModifyingQuery = true
							// Extract table name from DELETE FROM
							start := 0
							for i := 0; i < len(stmt); i++ {
								if i+11 <= len(stmt) && stmt[i:i+11] == "DELETE FROM" {
									start = i + 12
									break
								}
							}
							if start > 0 {
								end := start
								for end < len(stmt) && stmt[end] != ' ' {
									end++
								}
								tableName := stmt[start:end]
								verifyQuery = "SELECT * FROM " + tableName + " ORDER BY 1"
							}
						}
					}
				}
			}

			// Execute the main query
			if isModifyingQuery {
				_, err := db.Exec(tt.query)
				if tt.wantErr {
					if err == nil {
						t.Fatalf("expected error but got none")
					}
					return
				}
				if err != nil {
					t.Fatalf("exec failed: %v", err)
				}

				// Now query to verify results
				if verifyQuery != "" {
					rows, err := db.Query(verifyQuery)
					if err != nil {
						t.Fatalf("verify query failed: %v", err)
					}
					defer rows.Close()

					gotRows := scanAllRows(t, rows)
					compareRows(t, gotRows, tt.wantRows)
				}
				return
			}

			// Regular SELECT query
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			defer rows.Close()

			gotRows := scanAllRows(t, rows)
			compareRows(t, gotRows, tt.wantRows)
		})
	}
}

// scanAllRows is a helper function to scan all rows from a result set
func scanAllRows(t *testing.T, rows *sql.Rows) [][]interface{} {
	t.Helper()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	var gotRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		gotRows = append(gotRows, values)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return gotRows
}
