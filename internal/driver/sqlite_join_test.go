// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteJoin tests various JOIN operations including INNER, LEFT, CROSS, NATURAL, and USING
func TestSQLiteJoin(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
	}{
		// Basic NATURAL JOIN tests (from join.test)
		{
			name: "join-1.1 basic natural join",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(1,2,3)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(5)},
			},
		},
		{
			name: "join-1.3 reversed natural join",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t2 NATURAL JOIN t1",
			wantRows: [][]interface{}{
				{int64(2), int64(3), int64(4), int64(1)},
				{int64(3), int64(4), int64(5), int64(2)},
			},
		},
		// USING clause tests
		{
			name: "join-1.4 inner join using b,c",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 INNER JOIN t2 USING(b,c)",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(5)},
			},
		},
		{
			name: "join-1.5 inner join using b only",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 INNER JOIN t2 USING(b)",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(4), int64(5)},
			},
		},
		{
			name: "join-1.6 inner join using c only",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 INNER JOIN t2 USING(c)",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(2), int64(4)},
				{int64(2), int64(3), int64(4), int64(3), int64(5)},
			},
		},
		// CROSS JOIN tests
		{
			name: "join-1.8 natural cross join",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 NATURAL CROSS JOIN t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(5)},
			},
		},
		{
			name: "join-1.9 cross join with using",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 CROSS JOIN t2 USING(b,c)",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(5)},
			},
		},
		// Multi-table natural join
		{
			name: "join-1.16 three table natural join",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
				"INSERT INTO t2 VALUES(4,5,6)",
				"CREATE TABLE t3(c,d,e)",
				"INSERT INTO t3 VALUES(2,3,4)",
				"INSERT INTO t3 VALUES(3,4,5)",
				"INSERT INTO t3 VALUES(4,5,6)",
			},
			query: "SELECT * FROM t1 natural join t2 natural join t3",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4), int64(5)},
				{int64(2), int64(3), int64(4), int64(5), int64(6)},
			},
		},
		// LEFT JOIN tests (from join.test)
		{
			name: "join-2.1 natural left join",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t1 NATURAL LEFT JOIN t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
				{int64(2), int64(3), int64(4), int64(5)},
				{int64(3), int64(4), int64(5), nil},
			},
		},
		{
			name: "join-2.2 natural left outer join reversed",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(b,c,d)",
				"INSERT INTO t2 VALUES(1,2,3)",
				"INSERT INTO t2 VALUES(2,3,4)",
				"INSERT INTO t2 VALUES(3,4,5)",
			},
			query: "SELECT * FROM t2 NATURAL LEFT OUTER JOIN t1",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), nil},
				{int64(2), int64(3), int64(4), int64(1)},
				{int64(3), int64(4), int64(5), int64(2)},
			},
		},
		{
			name: "join-2.4 left join with ON clause",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(d,e,f)",
				"INSERT INTO t2 VALUES(1,2,3)",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(1), int64(2), int64(3)},
				{int64(2), int64(3), int64(4), nil, nil, nil},
				{int64(3), int64(4), int64(5), nil, nil, nil},
			},
		},
		{
			name: "join-2.5 left join with ON and WHERE",
			setup: []string{
				"CREATE TABLE t1(a,b,c)",
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT INTO t1 VALUES(2,3,4)",
				"INSERT INTO t1 VALUES(3,4,5)",
				"CREATE TABLE t2(d,e,f)",
				"INSERT INTO t2 VALUES(1,2,3)",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1",
			wantRows: [][]interface{}{
				{int64(2), int64(3), int64(4), nil, nil, nil},
				{int64(3), int64(4), int64(5), nil, nil, nil},
			},
		},
		// Error cases - JOIN with NATURAL and ON/USING
		{
			name:    "join-3.1 natural join with ON clause",
			setup:   []string{"CREATE TABLE t1(a)", "CREATE TABLE t2(b)"},
			query:   "SELECT * FROM t1 NATURAL JOIN t2 ON t1.a=t2.b",
			wantErr: true,
		},
		{
			name:    "join-3.2 natural join with USING clause",
			setup:   []string{"CREATE TABLE t1(a)", "CREATE TABLE t2(b)"},
			query:   "SELECT * FROM t1 NATURAL JOIN t2 USING(b)",
			wantErr: true,
		},
		{
			name:    "join-3.4.1 using with column not in both tables",
			setup:   []string{"CREATE TABLE t1(a,b)", "CREATE TABLE t2(b,c)"},
			query:   "SELECT * FROM t1 JOIN t2 USING(a)",
			wantErr: true,
		},
		// JOIN with NULL values (from join4.test)
		{
			name: "join4-1.1 left outer join with WHERE on right table",
			setup: []string{
				"CREATE TABLE t1(a integer, b varchar(10))",
				"INSERT INTO t1 VALUES(1,'one')",
				"INSERT INTO t1 VALUES(2,'two')",
				"INSERT INTO t1 VALUES(3,'three')",
				"INSERT INTO t1 VALUES(4,'four')",
				"CREATE TABLE t2(x integer, y varchar(10), z varchar(10))",
				"INSERT INTO t2 VALUES(2,'niban','ok')",
				"INSERT INTO t2 VALUES(4,'yonban','err')",
			},
			query: "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a=t2.x WHERE t2.z='ok'",
			wantRows: [][]interface{}{
				{int64(2), "two", int64(2), "niban", "ok"},
			},
		},
		{
			name: "join4-1.2 left join with ON and condition",
			setup: []string{
				"CREATE TABLE t1(a integer, b varchar(10))",
				"INSERT INTO t1 VALUES(1,'one')",
				"INSERT INTO t1 VALUES(2,'two')",
				"INSERT INTO t1 VALUES(3,'three')",
				"INSERT INTO t1 VALUES(4,'four')",
				"CREATE TABLE t2(x integer, y varchar(10), z varchar(10))",
				"INSERT INTO t2 VALUES(2,'niban','ok')",
				"INSERT INTO t2 VALUES(4,'yonban','err')",
			},
			query: "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.a=t2.x AND t2.z='ok'",
			wantRows: [][]interface{}{
				{int64(1), "one", nil, nil, nil},
				{int64(2), "two", int64(2), "niban", "ok"},
				{int64(3), "three", nil, nil, nil},
				{int64(4), "four", nil, nil, nil},
			},
		},
		// LEFT JOIN with ON clause restrictions (from join5.test)
		{
			name: "join5-1.2 left join with restricted ON clause",
			setup: []string{
				"CREATE TABLE t1(a integer primary key, b integer, c integer)",
				"CREATE TABLE t2(x integer primary key, y)",
				"INSERT INTO t1 VALUES(1, 5, 0)",
				"INSERT INTO t1 VALUES(2, 11, 2)",
				"INSERT INTO t1 VALUES(3, 12, 1)",
				"INSERT INTO t2 VALUES(11,'t2-11')",
				"INSERT INTO t2 VALUES(12,'t2-12')",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.b=t2.x AND t1.c=1",
			wantRows: [][]interface{}{
				{int64(1), int64(5), int64(0), nil, nil},
				{int64(2), int64(11), int64(2), nil, nil},
				{int64(3), int64(12), int64(1), int64(12), "t2-12"},
			},
		},
		{
			name: "join5-1.3 left join with WHERE filter",
			setup: []string{
				"CREATE TABLE t1(a integer primary key, b integer, c integer)",
				"CREATE TABLE t2(x integer primary key, y)",
				"INSERT INTO t1 VALUES(1, 5, 0)",
				"INSERT INTO t1 VALUES(2, 11, 2)",
				"INSERT INTO t1 VALUES(3, 12, 1)",
				"INSERT INTO t2 VALUES(11,'t2-11')",
				"INSERT INTO t2 VALUES(12,'t2-12')",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.b=t2.x WHERE t1.c=1",
			wantRows: [][]interface{}{
				{int64(3), int64(12), int64(1), int64(12), "t2-12"},
			},
		},
		// Self-join tests
		{
			name: "join-11.2 self join using USING",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)",
				"INSERT INTO t1 VALUES(1,'abc')",
				"INSERT INTO t1 VALUES(2,'def')",
			},
			query: "SELECT a FROM t1 JOIN t1 USING (a)",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		{
			name: "join-11.3 self join with alias",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)",
				"INSERT INTO t1 VALUES(1,'abc')",
				"INSERT INTO t1 VALUES(2,'def')",
			},
			query: "SELECT a FROM t1 JOIN t1 AS t2 USING (a)",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(2)},
			},
		},
		{
			name: "join-11.4 natural self join",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)",
				"INSERT INTO t1 VALUES(1,'abc')",
				"INSERT INTO t1 VALUES(2,'def')",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t1",
			wantRows: [][]interface{}{
				{int64(1), "abc"},
				{int64(2), "def"},
			},
		},
		// Multiple table joins (from join2.test)
		{
			name: "join2-1.4 three table natural join",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,11)",
				"INSERT INTO t1 VALUES(2,22)",
				"INSERT INTO t1 VALUES(3,33)",
				"CREATE TABLE t2(b,c)",
				"INSERT INTO t2 VALUES(11,111)",
				"INSERT INTO t2 VALUES(33,333)",
				"INSERT INTO t2 VALUES(44,444)",
				"CREATE TABLE t3(c,d)",
				"INSERT INTO t3 VALUES(111,1111)",
				"INSERT INTO t3 VALUES(444,4444)",
				"INSERT INTO t3 VALUES(555,5555)",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3",
			wantRows: [][]interface{}{
				{int64(1), int64(11), int64(111), int64(1111)},
			},
		},
		{
			name: "join2-1.5 three table join with left join",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,11)",
				"INSERT INTO t1 VALUES(2,22)",
				"INSERT INTO t1 VALUES(3,33)",
				"CREATE TABLE t2(b,c)",
				"INSERT INTO t2 VALUES(11,111)",
				"INSERT INTO t2 VALUES(33,333)",
				"INSERT INTO t2 VALUES(44,444)",
				"CREATE TABLE t3(c,d)",
				"INSERT INTO t3 VALUES(111,1111)",
				"INSERT INTO t3 VALUES(444,4444)",
				"INSERT INTO t3 VALUES(555,5555)",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t2 NATURAL LEFT OUTER JOIN t3",
			wantRows: [][]interface{}{
				{int64(1), int64(11), int64(111), int64(1111)},
				{int64(3), int64(33), int64(333), nil},
			},
		},
		// N-way join tests (from join6.test)
		{
			name: "join6-1.1 three way left join with USING",
			setup: []string{
				"CREATE TABLE t1(a)",
				"CREATE TABLE t2(a)",
				"CREATE TABLE t3(a,b)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t3 VALUES(1,2)",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 USING(a) LEFT JOIN t3 USING(a)",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "join6-2.1 three table join with USING on different columns",
			setup: []string{
				"CREATE TABLE t1(x,y)",
				"CREATE TABLE t2(y,z)",
				"CREATE TABLE t3(x,z)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t2 VALUES(2,3)",
				"INSERT INTO t2 VALUES(4,5)",
				"INSERT INTO t3 VALUES(1,3)",
				"INSERT INTO t3 VALUES(3,5)",
			},
			query: "SELECT * FROM t1 JOIN t2 USING (y) JOIN t3 USING(x)",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(3)},
				{int64(3), int64(4), int64(5), int64(5)},
			},
		},
		{
			name: "join6-2.2 three table natural join",
			setup: []string{
				"CREATE TABLE t1(x,y)",
				"CREATE TABLE t2(y,z)",
				"CREATE TABLE t3(x,z)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t2 VALUES(2,3)",
				"INSERT INTO t2 VALUES(4,5)",
				"INSERT INTO t3 VALUES(1,3)",
				"INSERT INTO t3 VALUES(3,5)",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(3), int64(4), int64(5)},
			},
		},
		// INNER JOIN with ON clause
		{
			name: "join basic inner join with ON",
			setup: []string{
				"CREATE TABLE customers(id INTEGER, name TEXT)",
				"CREATE TABLE orders(order_id INTEGER, customer_id INTEGER, amount REAL)",
				"INSERT INTO customers VALUES(1, 'Alice')",
				"INSERT INTO customers VALUES(2, 'Bob')",
				"INSERT INTO customers VALUES(3, 'Charlie')",
				"INSERT INTO orders VALUES(101, 1, 50.0)",
				"INSERT INTO orders VALUES(102, 2, 75.0)",
				"INSERT INTO orders VALUES(103, 1, 100.0)",
			},
			query: "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
			wantRows: [][]interface{}{
				{"Alice", 50.0},
				{"Bob", 75.0},
				{"Alice", 100.0},
			},
		},
		// LEFT JOIN showing NULL values
		{
			name: "join left join with unmatched rows",
			setup: []string{
				"CREATE TABLE customers(id INTEGER, name TEXT)",
				"CREATE TABLE orders(order_id INTEGER, customer_id INTEGER, amount REAL)",
				"INSERT INTO customers VALUES(1, 'Alice')",
				"INSERT INTO customers VALUES(2, 'Bob')",
				"INSERT INTO customers VALUES(3, 'Charlie')",
				"INSERT INTO orders VALUES(101, 1, 50.0)",
				"INSERT INTO orders VALUES(102, 2, 75.0)",
			},
			query: "SELECT customers.name, orders.amount FROM customers LEFT JOIN orders ON customers.id = orders.customer_id",
			wantRows: [][]interface{}{
				{"Alice", 50.0},
				{"Bob", 75.0},
				{"Charlie", nil},
			},
		},
		// CROSS JOIN (Cartesian product)
		{
			name: "join cross join cartesian product",
			setup: []string{
				"CREATE TABLE colors(color TEXT)",
				"CREATE TABLE sizes(size TEXT)",
				"INSERT INTO colors VALUES('Red')",
				"INSERT INTO colors VALUES('Blue')",
				"INSERT INTO sizes VALUES('Small')",
				"INSERT INTO sizes VALUES('Large')",
			},
			query: "SELECT * FROM colors CROSS JOIN sizes",
			wantRows: [][]interface{}{
				{"Red", "Small"},
				{"Red", "Large"},
				{"Blue", "Small"},
				{"Blue", "Large"},
			},
		},
		// JOIN with WHERE clause
		{
			name: "join with WHERE clause filter",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, price REAL)",
				"CREATE TABLE categories(id INTEGER, product_id INTEGER, category TEXT)",
				"INSERT INTO products VALUES(1, 'Widget', 10.0)",
				"INSERT INTO products VALUES(2, 'Gadget', 20.0)",
				"INSERT INTO products VALUES(3, 'Doohickey', 15.0)",
				"INSERT INTO categories VALUES(1, 1, 'Tools')",
				"INSERT INTO categories VALUES(2, 2, 'Electronics')",
				"INSERT INTO categories VALUES(3, 3, 'Tools')",
			},
			query: "SELECT products.name, products.price FROM products JOIN categories ON products.id = categories.product_id WHERE categories.category = 'Tools'",
			wantRows: [][]interface{}{
				{"Widget", 10.0},
				{"Doohickey", 15.0},
			},
		},
		// Multiple joins in sequence
		{
			name: "join chain of three tables",
			setup: []string{
				"CREATE TABLE authors(id INTEGER, name TEXT)",
				"CREATE TABLE books(id INTEGER, author_id INTEGER, title TEXT)",
				"CREATE TABLE reviews(id INTEGER, book_id INTEGER, rating INTEGER)",
				"INSERT INTO authors VALUES(1, 'Smith')",
				"INSERT INTO authors VALUES(2, 'Jones')",
				"INSERT INTO books VALUES(101, 1, 'Book A')",
				"INSERT INTO books VALUES(102, 2, 'Book B')",
				"INSERT INTO reviews VALUES(1, 101, 5)",
				"INSERT INTO reviews VALUES(2, 102, 4)",
			},
			query: "SELECT authors.name, books.title, reviews.rating FROM authors JOIN books ON authors.id = books.author_id JOIN reviews ON books.id = reviews.book_id",
			wantRows: [][]interface{}{
				{"Smith", "Book A", int64(5)},
				{"Jones", "Book B", int64(4)},
			},
		},
		// Subquery as join source
		{
			name: "join with subquery as source",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, name TEXT, dept_id INTEGER)",
				"CREATE TABLE departments(id INTEGER, name TEXT)",
				"INSERT INTO employees VALUES(1, 'Alice', 10)",
				"INSERT INTO employees VALUES(2, 'Bob', 20)",
				"INSERT INTO employees VALUES(3, 'Charlie', 10)",
				"INSERT INTO departments VALUES(10, 'Engineering')",
				"INSERT INTO departments VALUES(20, 'Sales')",
			},
			query: "SELECT e.name, d.name FROM (SELECT * FROM employees WHERE dept_id = 10) AS e JOIN departments AS d ON e.dept_id = d.id",
			wantRows: [][]interface{}{
				{"Alice", "Engineering"},
				{"Charlie", "Engineering"},
			},
		},
		// JOIN with aggregate in subquery
		{
			name: "join with aggregate subquery",
			setup: []string{
				"CREATE TABLE sales(id INTEGER, product_id INTEGER, amount REAL)",
				"CREATE TABLE products(id INTEGER, name TEXT)",
				"INSERT INTO products VALUES(1, 'Widget')",
				"INSERT INTO products VALUES(2, 'Gadget')",
				"INSERT INTO sales VALUES(1, 1, 100)",
				"INSERT INTO sales VALUES(2, 1, 150)",
				"INSERT INTO sales VALUES(3, 2, 200)",
			},
			query: "SELECT p.name, s.total FROM products p JOIN (SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id) s ON p.id = s.product_id",
			wantRows: [][]interface{}{
				{"Widget", 250.0},
				{"Gadget", 200.0},
			},
		},
		// Complex multi-condition ON clause
		{
			name: "join with complex ON condition",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"CREATE TABLE t2(c INTEGER, d INTEGER)",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 20)",
				"INSERT INTO t1 VALUES(3, 30)",
				"INSERT INTO t2 VALUES(1, 10)",
				"INSERT INTO t2 VALUES(2, 25)",
				"INSERT INTO t2 VALUES(3, 30)",
			},
			query: "SELECT * FROM t1 JOIN t2 ON t1.a = t2.c AND t1.b = t2.d",
			wantRows: [][]interface{}{
				{int64(1), int64(10), int64(1), int64(10)},
				{int64(3), int64(30), int64(3), int64(30)},
			},
		},
		// LEFT JOIN with multiple unmatched rows
		{
			name: "left join multiple unmatched",
			setup: []string{
				"CREATE TABLE t1(id INTEGER, value TEXT)",
				"CREATE TABLE t2(id INTEGER, t1_id INTEGER, data TEXT)",
				"INSERT INTO t1 VALUES(1, 'A')",
				"INSERT INTO t1 VALUES(2, 'B')",
				"INSERT INTO t1 VALUES(3, 'C')",
				"INSERT INTO t2 VALUES(1, 1, 'data1')",
			},
			query: "SELECT t1.id, t1.value, t2.data FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id",
			wantRows: [][]interface{}{
				{int64(1), "A", "data1"},
				{int64(2), "B", nil},
				{int64(3), "C", nil},
			},
		},
		// Multiple LEFT JOINs
		{
			name: "multiple left joins",
			setup: []string{
				"CREATE TABLE t1(id INTEGER)",
				"CREATE TABLE t2(id INTEGER, t1_id INTEGER)",
				"CREATE TABLE t3(id INTEGER, t2_id INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t2 VALUES(10, 1)",
				"INSERT INTO t3 VALUES(100, 10)",
			},
			query: "SELECT t1.id, t2.id, t3.id FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id LEFT JOIN t3 ON t2.id = t3.t2_id",
			wantRows: [][]interface{}{
				{int64(1), int64(10), int64(100)},
				{int64(2), nil, nil},
			},
		},
		// JOIN with ORDER BY
		{
			name: "join with order by",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, name TEXT, dept_id INTEGER)",
				"CREATE TABLE departments(id INTEGER, dept_name TEXT)",
				"INSERT INTO employees VALUES(1, 'Zoe', 10)",
				"INSERT INTO employees VALUES(2, 'Alice', 20)",
				"INSERT INTO employees VALUES(3, 'Bob', 10)",
				"INSERT INTO departments VALUES(10, 'Engineering')",
				"INSERT INTO departments VALUES(20, 'Sales')",
			},
			query: "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
			wantRows: [][]interface{}{
				{"Alice", "Sales"},
				{"Bob", "Engineering"},
				{"Zoe", "Engineering"},
			},
		},
		{
			name: "join with order by descending",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, category_id INTEGER)",
				"CREATE TABLE categories(id INTEGER, name TEXT)",
				"INSERT INTO products VALUES(1, 'Widget', 1)",
				"INSERT INTO products VALUES(2, 'Gadget', 2)",
				"INSERT INTO products VALUES(3, 'Tool', 1)",
				"INSERT INTO categories VALUES(1, 'Hardware')",
				"INSERT INTO categories VALUES(2, 'Electronics')",
			},
			query: "SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id ORDER BY p.name DESC",
			wantRows: [][]interface{}{
				{"Widget", "Hardware"},
				{"Tool", "Hardware"},
				{"Gadget", "Electronics"},
			},
		},
		{
			name: "join with order by multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(a INTEGER, c TEXT)",
				"INSERT INTO t1 VALUES(1, 'Z')",
				"INSERT INTO t1 VALUES(2, 'A')",
				"INSERT INTO t1 VALUES(2, 'B')",
				"INSERT INTO t2 VALUES(1, 'X')",
				"INSERT INTO t2 VALUES(2, 'Y')",
			},
			query: "SELECT t1.a, t1.b, t2.c FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a, t1.b",
			wantRows: [][]interface{}{
				{int64(1), "Z", "X"},
				{int64(2), "A", "Y"},
				{int64(2), "B", "Y"},
			},
		},
		// JOIN with GROUP BY and aggregate functions
		{
			name: "join with group by and count",
			setup: []string{
				"CREATE TABLE customers(id INTEGER, name TEXT)",
				"CREATE TABLE orders(id INTEGER, customer_id INTEGER, amount REAL)",
				"INSERT INTO customers VALUES(1, 'Alice')",
				"INSERT INTO customers VALUES(2, 'Bob')",
				"INSERT INTO customers VALUES(3, 'Charlie')",
				"INSERT INTO orders VALUES(1, 1, 100.0)",
				"INSERT INTO orders VALUES(2, 1, 200.0)",
				"INSERT INTO orders VALUES(3, 2, 150.0)",
			},
			query: "SELECT c.name, COUNT(o.id) FROM customers c LEFT JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name ORDER BY c.name",
			wantRows: [][]interface{}{
				{"Alice", int64(2)},
				{"Bob", int64(1)},
				{"Charlie", int64(0)},
			},
		},
		{
			name: "join with group by and sum",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, category_id INTEGER, price REAL)",
				"CREATE TABLE categories(id INTEGER, name TEXT)",
				"INSERT INTO products VALUES(1, 'Widget', 1, 10.0)",
				"INSERT INTO products VALUES(2, 'Gadget', 2, 20.0)",
				"INSERT INTO products VALUES(3, 'Tool', 1, 15.0)",
				"INSERT INTO categories VALUES(1, 'Hardware')",
				"INSERT INTO categories VALUES(2, 'Electronics')",
			},
			query: "SELECT c.name, SUM(p.price) FROM categories c LEFT JOIN products p ON c.id = p.category_id GROUP BY c.id, c.name ORDER BY c.name",
			wantRows: [][]interface{}{
				{"Electronics", 20.0},
				{"Hardware", 25.0},
			},
		},
		{
			name: "join with group by and avg",
			setup: []string{
				"CREATE TABLE students(id INTEGER, name TEXT, class_id INTEGER)",
				"CREATE TABLE classes(id INTEGER, name TEXT)",
				"CREATE TABLE grades(student_id INTEGER, score INTEGER)",
				"INSERT INTO classes VALUES(1, 'Math')",
				"INSERT INTO classes VALUES(2, 'Science')",
				"INSERT INTO students VALUES(1, 'Alice', 1)",
				"INSERT INTO students VALUES(2, 'Bob', 1)",
				"INSERT INTO students VALUES(3, 'Charlie', 2)",
				"INSERT INTO grades VALUES(1, 90)",
				"INSERT INTO grades VALUES(1, 85)",
				"INSERT INTO grades VALUES(2, 75)",
				"INSERT INTO grades VALUES(3, 95)",
			},
			query: "SELECT c.name, AVG(g.score) FROM classes c JOIN students s ON c.id = s.class_id JOIN grades g ON s.id = g.student_id GROUP BY c.id, c.name ORDER BY c.name",
			wantRows: [][]interface{}{
				{"Math", 83.33333333333333},
				{"Science", 95.0},
			},
		},
		{
			name: "join with group by having",
			setup: []string{
				"CREATE TABLE departments(id INTEGER, name TEXT)",
				"CREATE TABLE employees(id INTEGER, dept_id INTEGER, salary REAL)",
				"INSERT INTO departments VALUES(1, 'Engineering')",
				"INSERT INTO departments VALUES(2, 'Sales')",
				"INSERT INTO departments VALUES(3, 'Marketing')",
				"INSERT INTO employees VALUES(1, 1, 80000)",
				"INSERT INTO employees VALUES(2, 1, 90000)",
				"INSERT INTO employees VALUES(3, 2, 60000)",
				"INSERT INTO employees VALUES(4, 3, 70000)",
			},
			query: "SELECT d.name, COUNT(e.id) FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.id, d.name HAVING COUNT(e.id) >= 2 ORDER BY d.name",
			wantRows: [][]interface{}{
				{"Engineering", int64(2)},
			},
		},
		// Empty table JOIN tests
		{
			name: "join with empty left table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
			},
			query:    "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
			wantRows: [][]interface{}{},
		},
		{
			name: "join with empty right table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query:    "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
			wantRows: [][]interface{}{},
		},
		{
			name: "left join with empty right table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b",
			wantRows: [][]interface{}{
				{int64(1), nil},
				{int64(2), nil},
			},
		},
		{
			name: "cross join with empty table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:    "SELECT * FROM t1 CROSS JOIN t2",
			wantRows: [][]interface{}{},
		},
		{
			name: "natural join with empty table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(a INTEGER)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:    "SELECT * FROM t1 NATURAL JOIN t2",
			wantRows: [][]interface{}{},
		},
		// JOINs with NULL values in join columns
		{
			name: "join with null in left table join column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t2 VALUES(3)",
			},
			query: "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(3), int64(3)},
			},
		},
		{
			name: "join with null in right table join column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(NULL)",
				"INSERT INTO t2 VALUES(3)",
			},
			query: "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(3), int64(3)},
			},
		},
		{
			name: "left join with null in join column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER, c TEXT)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(NULL)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1, 'one')",
				"INSERT INTO t2 VALUES(3, 'three')",
			},
			query: "SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b ORDER BY t1.a",
			wantRows: [][]interface{}{
				{nil, nil, nil},
				{int64(1), int64(1), "one"},
				{int64(3), int64(3), "three"},
			},
		},
		// Four-way join
		{
			name: "four way join",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(a INTEGER, b INTEGER)",
				"CREATE TABLE t3(b INTEGER, c INTEGER)",
				"CREATE TABLE t4(c INTEGER, d INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(1, 2)",
				"INSERT INTO t3 VALUES(2, 3)",
				"INSERT INTO t4 VALUES(3, 4)",
			},
			query: "SELECT t1.a, t2.b, t3.c, t4.d FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.b = t3.b JOIN t4 ON t3.c = t4.c",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(3), int64(4)},
			},
		},
		// JOIN with IN subquery
		{
			name: "join with in subquery in where",
			setup: []string{
				"CREATE TABLE products(id INTEGER, name TEXT, category_id INTEGER)",
				"CREATE TABLE categories(id INTEGER, name TEXT)",
				"CREATE TABLE featured(product_id INTEGER)",
				"INSERT INTO products VALUES(1, 'Widget', 1)",
				"INSERT INTO products VALUES(2, 'Gadget', 2)",
				"INSERT INTO products VALUES(3, 'Tool', 1)",
				"INSERT INTO categories VALUES(1, 'Hardware')",
				"INSERT INTO categories VALUES(2, 'Electronics')",
				"INSERT INTO featured VALUES(1)",
				"INSERT INTO featured VALUES(3)",
			},
			query: "SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id WHERE p.id IN (SELECT product_id FROM featured) ORDER BY p.name",
			wantRows: [][]interface{}{
				{"Tool", "Hardware"},
				{"Widget", "Hardware"},
			},
		},
		// JOIN with DISTINCT
		{
			name: "join with distinct",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(a INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(1, 'x')",
				"INSERT INTO t2 VALUES(1, 'y')",
			},
			query: "SELECT DISTINCT t1.a FROM t1 JOIN t2 ON t1.a = t2.a",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		// Complex self-join scenarios
		{
			name: "self join to find pairs",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, name TEXT, manager_id INTEGER)",
				"INSERT INTO employees VALUES(1, 'Alice', NULL)",
				"INSERT INTO employees VALUES(2, 'Bob', 1)",
				"INSERT INTO employees VALUES(3, 'Charlie', 1)",
				"INSERT INTO employees VALUES(4, 'David', 2)",
			},
			query: "SELECT e.name, m.name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id ORDER BY e.id",
			wantRows: [][]interface{}{
				{"Alice", nil},
				{"Bob", "Alice"},
				{"Charlie", "Alice"},
				{"David", "Bob"},
			},
		},
		{
			name: "self join to find related rows",
			setup: []string{
				"CREATE TABLE items(id INTEGER, value INTEGER)",
				"INSERT INTO items VALUES(1, 10)",
				"INSERT INTO items VALUES(2, 20)",
				"INSERT INTO items VALUES(3, 30)",
			},
			query: "SELECT a.id, a.value, b.id, b.value FROM items a JOIN items b ON a.value + 10 = b.value ORDER BY a.id",
			wantRows: [][]interface{}{
				{int64(1), int64(10), int64(2), int64(20)},
				{int64(2), int64(20), int64(3), int64(30)},
			},
		},
		// JOIN with CASE expression
		{
			name: "join with case in select",
			setup: []string{
				"CREATE TABLE orders(id INTEGER, customer_id INTEGER, amount REAL)",
				"CREATE TABLE customers(id INTEGER, name TEXT)",
				"INSERT INTO customers VALUES(1, 'Alice')",
				"INSERT INTO customers VALUES(2, 'Bob')",
				"INSERT INTO orders VALUES(1, 1, 100)",
				"INSERT INTO orders VALUES(2, 1, 250)",
				"INSERT INTO orders VALUES(3, 2, 75)",
			},
			query: "SELECT c.name, CASE WHEN o.amount > 200 THEN 'Large' ELSE 'Small' END FROM customers c JOIN orders o ON c.id = o.customer_id ORDER BY o.id",
			wantRows: [][]interface{}{
				{"Alice", "Small"},
				{"Alice", "Large"},
				{"Bob", "Small"},
			},
		},
		// JOIN with LIMIT
		{
			name: "join with limit",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t1 VALUES(3)",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t2 VALUES(3)",
			},
			query: "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b ORDER BY t1.a LIMIT 2",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(2)},
			},
		},
		// Mixed JOIN types in one query
		{
			name: "mixed join types",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(a INTEGER, b INTEGER)",
				"CREATE TABLE t3(b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(2)",
				"INSERT INTO t2 VALUES(1, 10)",
				"INSERT INTO t3 VALUES(10, 100)",
				"INSERT INTO t3 VALUES(20, 200)",
			},
			query: "SELECT t1.a, t2.b, t3.c FROM t1 JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.b = t3.b ORDER BY t1.a",
			wantRows: [][]interface{}{
				{int64(1), int64(10), int64(100)},
			},
		},
		// JOIN on expression
		{
			name: "join on expression with math",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(5)",
				"INSERT INTO t1 VALUES(10)",
				"INSERT INTO t2 VALUES(10)",
				"INSERT INTO t2 VALUES(20)",
			},
			query: "SELECT * FROM t1 JOIN t2 ON t1.a * 2 = t2.b",
			wantRows: [][]interface{}{
				{int64(5), int64(10)},
				{int64(10), int64(20)},
			},
		},
		// NATURAL JOIN with no common columns
		{
			name: "natural join with no common columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(b INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
			},
			query: "SELECT * FROM t1 NATURAL JOIN t2",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		// JOIN with string concatenation
		{
			name: "join with string concat in select",
			setup: []string{
				"CREATE TABLE users(id INTEGER, first_name TEXT, last_name TEXT)",
				"CREATE TABLE orders(id INTEGER, user_id INTEGER)",
				"INSERT INTO users VALUES(1, 'John', 'Doe')",
				"INSERT INTO users VALUES(2, 'Jane', 'Smith')",
				"INSERT INTO orders VALUES(1, 1)",
			},
			query: "SELECT u.first_name || ' ' || u.last_name, o.id FROM users u JOIN orders o ON u.id = o.user_id",
			wantRows: [][]interface{}{
				{"John Doe", int64(1)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
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

			// Execute the query
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

			// Get column count
			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("failed to get columns: %v", err)
			}

			// Collect results
			var gotRows [][]interface{}
			for rows.Next() {
				// Create a slice of interface{} to hold the row values
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

			// Compare results
			if len(gotRows) != len(tt.wantRows) {
				t.Fatalf("row count mismatch: got %d rows, want %d rows\nGot: %v\nWant: %v",
					len(gotRows), len(tt.wantRows), gotRows, tt.wantRows)
			}

			for i, gotRow := range gotRows {
				wantRow := tt.wantRows[i]
				if len(gotRow) != len(wantRow) {
					t.Fatalf("row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
				}

				for j, gotVal := range gotRow {
					wantVal := wantRow[j]

					// Handle NULL comparison
					if wantVal == nil {
						if gotVal != nil {
							t.Errorf("row %d, col %d: got %v (%T), want nil", i, j, gotVal, gotVal)
						}
						continue
					}

					if gotVal == nil {
						t.Errorf("row %d, col %d: got nil, want %v (%T)", i, j, wantVal, wantVal)
						continue
					}

					// Type-specific comparison
					switch want := wantVal.(type) {
					case int64:
						if got, ok := gotVal.(int64); !ok {
							t.Errorf("row %d, col %d: type mismatch: got %T, want int64", i, j, gotVal)
						} else if got != want {
							t.Errorf("row %d, col %d: got %v, want %v", i, j, got, want)
						}
					case float64:
						if got, ok := gotVal.(float64); !ok {
							t.Errorf("row %d, col %d: type mismatch: got %T, want float64", i, j, gotVal)
						} else if got != want {
							t.Errorf("row %d, col %d: got %v, want %v", i, j, got, want)
						}
					case string:
						if got, ok := gotVal.(string); !ok {
							t.Errorf("row %d, col %d: type mismatch: got %T, want string", i, j, gotVal)
						} else if got != want {
							t.Errorf("row %d, col %d: got %q, want %q", i, j, got, want)
						}
					default:
						t.Errorf("row %d, col %d: unsupported type %T", i, j, wantVal)
					}
				}
			}
		})
	}
}
