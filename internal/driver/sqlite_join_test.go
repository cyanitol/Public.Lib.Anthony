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
	}

	for _, tt := range tests {
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
