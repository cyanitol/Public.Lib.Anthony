package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteSubquery tests various subquery operations including scalar subqueries, EXISTS, IN, and correlated subqueries
func TestSQLiteSubquery(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
	}{
		// Basic correlated subquery tests (from subquery.test)
		{
			name: "subquery-1.1 scalar subquery in SELECT",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t1 VALUES(5,6)",
				"INSERT INTO t1 VALUES(7,8)",
				"CREATE TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,1)",
				"INSERT INTO t2 VALUES(3,9)",
				"INSERT INTO t2 VALUES(5,25)",
				"INSERT INTO t2 VALUES(7,49)",
			},
			query: "SELECT a, (SELECT y FROM t2 WHERE x=a) FROM t1 WHERE b<8",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(3), int64(9)},
				{int64(5), int64(25)},
			},
		},
		{
			name: "subquery-1.3 EXISTS with correlated subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,3)",
				"INSERT INTO t1 VALUES(3,13)",
				"INSERT INTO t1 VALUES(5,31)",
				"INSERT INTO t1 VALUES(7,57)",
			},
			query: "SELECT b FROM t1 WHERE EXISTS(SELECT * FROM (SELECT 1 AS x) WHERE x=a)",
			wantRows: [][]interface{}{
				{int64(3)},
			},
		},
		{
			name: "subquery-1.4 NOT EXISTS with correlated subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,3)",
				"INSERT INTO t1 VALUES(3,13)",
				"INSERT INTO t1 VALUES(5,31)",
				"INSERT INTO t1 VALUES(7,57)",
			},
			query: "SELECT b FROM t1 WHERE NOT EXISTS(SELECT * FROM (SELECT 1 AS x) WHERE x=a)",
			wantRows: [][]interface{}{
				{int64(13)},
				{int64(31)},
				{int64(57)},
			},
		},
		{
			name: "subquery-1.8 aggregate in parent and subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t1 VALUES(5,6)",
				"INSERT INTO t1 VALUES(7,8)",
				"CREATE TABLE t2(x,y)",
				"INSERT INTO t2 VALUES(1,1)",
				"INSERT INTO t2 VALUES(3,9)",
				"INSERT INTO t2 VALUES(5,25)",
				"INSERT INTO t2 VALUES(7,49)",
			},
			query: "SELECT count(*) FROM t1 WHERE a > (SELECT count(*) FROM t2)",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		{
			name: "subquery-1.10.1 scalar subquery in SELECT list",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,3)",
				"INSERT INTO t1 VALUES(3,13)",
			},
			query: "SELECT (SELECT a), b FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(3)},
				{int64(3), int64(13)},
			},
		},
		{
			name: "subquery-1.10.3 aggregate subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t1 VALUES(5,6)",
				"INSERT INTO t1 VALUES(7,8)",
			},
			query: "SELECT * FROM (SELECT (SELECT sum(a) FROM t1))",
			wantRows: [][]interface{}{
				{int64(16)},
			},
		},
		{
			name: "subquery-2.1 simple scalar subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
			},
			query: "SELECT (SELECT 10)",
			wantRows: [][]interface{}{
				{int64(10)},
			},
		},
		{
			name: "subquery-2.2.2 IN with subquery",
			setup: []string{
				"CREATE TABLE t3(a PRIMARY KEY, b)",
				"INSERT INTO t3 VALUES(1, 2)",
				"INSERT INTO t3 VALUES(3, 1)",
			},
			query: "SELECT * FROM t3 WHERE a IN (SELECT b FROM t3)",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "subquery-2.4.2 IN with scalar string subquery",
			setup: []string{
				"CREATE TABLE t3(a TEXT)",
				"INSERT INTO t3 VALUES('XX')",
			},
			query: "SELECT count(*) FROM t3 WHERE a IN (SELECT 'XX')",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name: "subquery-3.2 scalar subquery with table reference",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
			},
			query: "SELECT (SELECT t1.a) FROM t1",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name: "subquery-3.3.1 scalar subquery in aggregate query",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
			},
			query: "SELECT a, (SELECT b) FROM t1 GROUP BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "subquery-3.3.2 correlated scalar subquery in aggregate",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"CREATE TABLE t2(c, d)",
				"INSERT INTO t2 VALUES(1, 'one')",
				"INSERT INTO t2 VALUES(2, 'two')",
			},
			query: "SELECT a, (SELECT d FROM t2 WHERE a=c) FROM t1 GROUP BY a",
			wantRows: [][]interface{}{
				{int64(1), "one"},
			},
		},
		{
			name: "subquery-3.3.3 scalar subquery with max aggregate",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,4)",
				"CREATE TABLE t2(c, d)",
				"INSERT INTO t2 VALUES(1, 'one')",
				"INSERT INTO t2 VALUES(2, 'two')",
			},
			query: "SELECT max(a), (SELECT d FROM t2 WHERE a=c) FROM t1",
			wantRows: [][]interface{}{
				{int64(2), "two"},
			},
		},
		{
			name: "subquery-3.3.4 nested scalar subquery in aggregate",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,4)",
				"CREATE TABLE t2(c, d)",
				"INSERT INTO t2 VALUES(1, 'one')",
				"INSERT INTO t2 VALUES(2, 'two')",
			},
			query: "SELECT a, (SELECT (SELECT d FROM t2 WHERE a=c)) FROM t1 GROUP BY a",
			wantRows: [][]interface{}{
				{int64(1), "one"},
				{int64(2), "two"},
			},
		},
		{
			name: "subquery-3.3.5 count in correlated subquery",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,4)",
				"CREATE TABLE t2(c, d)",
				"INSERT INTO t2 VALUES(1, 'one')",
				"INSERT INTO t2 VALUES(2, 'two')",
			},
			query: "SELECT a, (SELECT count(*) FROM t2 WHERE a=c) FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(1)},
				{int64(2), int64(1)},
			},
		},
		{
			name: "subquery-3.4.1 aggregate subquery with EXISTS",
			setup: []string{
				"CREATE TABLE t34(x,y)",
				"INSERT INTO t34 VALUES(106,4), (107,3), (106,5), (107,5)",
			},
			query: `SELECT a.x, avg(a.y)
				FROM t34 AS a
				GROUP BY a.x
				HAVING NOT EXISTS(
					SELECT b.x, avg(b.y)
					FROM t34 AS b
					GROUP BY b.x
					HAVING avg(a.y) > avg(b.y)
				)`,
			wantRows: [][]interface{}{
				{int64(107), 4.0},
			},
		},
		{
			name: "subquery-3.5.1 max with aggregate subquery",
			setup: []string{
				"CREATE TABLE t35a(x)",
				"INSERT INTO t35a VALUES(1),(2),(3)",
				"CREATE TABLE t35b(y)",
				"INSERT INTO t35b VALUES(98), (99)",
			},
			query: "SELECT max((SELECT avg(y) FROM t35b)) FROM t35a",
			wantRows: [][]interface{}{
				{98.5},
			},
		},
		{
			name: "subquery-3.5.2 max with count subquery",
			setup: []string{
				"CREATE TABLE t35a(x)",
				"INSERT INTO t35a VALUES(1),(2),(3)",
				"CREATE TABLE t35b(y)",
				"INSERT INTO t35b VALUES(98), (99)",
			},
			query: "SELECT max((SELECT count(y) FROM t35b)) FROM t35a",
			wantRows: [][]interface{}{
				{int64(2)},
			},
		},
		{
			name: "subquery-4.1.1 cached subquery execution",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
			},
			query: "SELECT (SELECT a FROM t1)",
			wantRows: [][]interface{}{
				{int64(1)},
			},
		},
		{
			name: "subquery-4.2 empty subquery result",
			setup: []string{
				"CREATE TABLE t1(a,b)",
			},
			query: "SELECT (SELECT a FROM t1)",
			wantRows: [][]interface{}{
				{nil},
			},
		},
		// Tests from subquery2.test
		{
			name: "subquery2-1.1 complex correlated subquery with DISTINCT",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(3,4)",
				"INSERT INTO t1 VALUES(5,6)",
				"INSERT INTO t1 VALUES(7,8)",
				"CREATE TABLE t3(e,f)",
				"INSERT INTO t3 VALUES(1,1)",
				"INSERT INTO t3 VALUES(3,27)",
				"INSERT INTO t3 VALUES(5,125)",
				"INSERT INTO t3 VALUES(7,343)",
			},
			query: "SELECT a FROM t1 WHERE b IN (SELECT x+1 FROM (SELECT DISTINCT f/(a*a) AS x FROM t3))",
			wantRows: [][]interface{}{
				{int64(1)},
				{int64(3)},
				{int64(5)},
				{int64(7)},
			},
		},
		{
			name: "subquery2-2.2 subquery with LIMIT and subquery",
			setup: []string{
				"CREATE TABLE t4(a, b)",
				"CREATE TABLE t5(a, b)",
				"INSERT INTO t5 VALUES(3, 5)",
				"INSERT INTO t4 VALUES(1, 1)",
				"INSERT INTO t4 VALUES(2, 3)",
				"INSERT INTO t4 VALUES(3, 6)",
				"INSERT INTO t4 VALUES(4, 10)",
				"INSERT INTO t4 VALUES(5, 15)",
			},
			query: "SELECT * FROM (SELECT * FROM t4 ORDER BY a LIMIT -1 OFFSET 1) LIMIT (SELECT a FROM t5)",
			wantRows: [][]interface{}{
				{int64(2), int64(3)},
				{int64(3), int64(6)},
				{int64(4), int64(10)},
			},
		},
		{
			name: "subquery2-3.0 three-way nested UNION ALL",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, data TEXT)",
				"INSERT INTO t1(id,data) VALUES(9,'nine-a')",
				"INSERT INTO t1(id,data) VALUES(10,'ten-a')",
				"INSERT INTO t1(id,data) VALUES(11,'eleven-a')",
				"CREATE TABLE t2(id INTEGER PRIMARY KEY, data TEXT)",
				"INSERT INTO t2(id,data) VALUES(9,'nine-b')",
				"INSERT INTO t2(id,data) VALUES(10,'ten-b')",
				"INSERT INTO t2(id,data) VALUES(11,'eleven-b')",
			},
			query: `SELECT id FROM (
				SELECT id,data FROM (
					SELECT * FROM t1 UNION ALL SELECT * FROM t2
				)
				WHERE id=10 ORDER BY data
			)`,
			wantRows: [][]interface{}{
				{int64(10)},
				{int64(10)},
			},
		},
		{
			name: "subquery2-3.1 nested UNION ALL with ORDER BY",
			setup: []string{
				"CREATE TABLE t1(data TEXT)",
				"INSERT INTO t1 VALUES('eleven-a')",
				"INSERT INTO t1 VALUES('nine-a')",
				"INSERT INTO t1 VALUES('ten-a')",
			},
			query: `SELECT data FROM (
				SELECT 'dummy', data FROM (
					SELECT data FROM t1 UNION ALL SELECT data FROM t1
				) ORDER BY data
			)`,
			wantRows: [][]interface{}{
				{"eleven-a"},
				{"eleven-a"},
				{"nine-a"},
				{"nine-a"},
				{"ten-a"},
				{"ten-a"},
			},
		},
		{
			name: "subquery2-3.2 UNION ALL with ORDER BY data",
			setup: []string{
				"CREATE TABLE t3(id INTEGER, data TEXT)",
				"CREATE TABLE t4(id INTEGER, data TEXT)",
				"INSERT INTO t3 VALUES(4, 'a'),(2,'c')",
				"INSERT INTO t4 VALUES(3, 'b'),(1,'d')",
			},
			query: `SELECT data, id FROM (
				SELECT id, data FROM (
					SELECT * FROM t3 UNION ALL SELECT * FROM t4
				) ORDER BY data
			)`,
			wantRows: [][]interface{}{
				{"a", int64(4)},
				{"b", int64(3)},
				{"c", int64(2)},
				{"d", int64(1)},
			},
		},
		{
			name: "subquery2-5.1 correlated subquery with ORDER BY",
			setup: []string{
				"CREATE TABLE t1(x TEXT)",
				"INSERT INTO t1 VALUES('ALFKI')",
				"INSERT INTO t1 VALUES('ANATR')",
				"CREATE TABLE t2(y TEXT, z TEXT)",
				"CREATE INDEX t2y ON t2(y)",
				"INSERT INTO t2 VALUES('ANATR', '1997-08-08 00:00:00')",
				"INSERT INTO t2 VALUES('ALFKI', '1997-08-25 00:00:00')",
			},
			query: "SELECT (SELECT y FROM t2 WHERE x = y ORDER BY y, z) FROM t1",
			wantRows: [][]interface{}{
				{"ALFKI"},
				{"ANATR"},
			},
		},
		{
			name: "subquery2-6.2 empty result from scalar subquery with LIMIT",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1234)",
			},
			query: "SELECT (SELECT 'string' FROM t1 LIMIT 1 OFFSET 5)",
			wantRows: [][]interface{}{
				{nil},
			},
		},
		{
			name: "subquery2-6.3 DISTINCT with LIMIT offset beyond result",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES(1234)",
			},
			query: "SELECT (SELECT DISTINCT 'string' FROM t1 LIMIT 1 OFFSET 5)",
			wantRows: [][]interface{}{
				{nil},
			},
		},
		{
			name: "subquery2-7.1 DISTINCT with indexed ORDER BY and offset",
			setup: []string{
				"CREATE TABLE t1(x)",
				"CREATE INDEX i1 ON t1(x)",
				"INSERT INTO t1 VALUES(1234)",
			},
			query: "SELECT (SELECT DISTINCT 'string' FROM t1 ORDER BY x LIMIT 1 OFFSET 5)",
			wantRows: [][]interface{}{
				{nil},
			},
		},
		{
			name: "subquery2-7.4 DISTINCT with unique index and ORDER BY",
			setup: []string{
				"CREATE TABLE t1(x)",
				"CREATE UNIQUE INDEX i1 ON t1(x)",
				"INSERT INTO t1 VALUES(1234)",
			},
			query: "SELECT (SELECT DISTINCT x FROM t1 ORDER BY +x LIMIT 1 OFFSET 0)",
			wantRows: [][]interface{}{
				{int64(1234)},
			},
		},
		// Additional comprehensive tests
		{
			name: "subquery-complex-1 IN with multiple values",
			setup: []string{
				"CREATE TABLE products(id INT, name TEXT, category TEXT)",
				"INSERT INTO products VALUES(1, 'Apple', 'Fruit')",
				"INSERT INTO products VALUES(2, 'Carrot', 'Vegetable')",
				"INSERT INTO products VALUES(3, 'Banana', 'Fruit')",
				"CREATE TABLE categories(name TEXT)",
				"INSERT INTO categories VALUES('Fruit')",
			},
			query: "SELECT name FROM products WHERE category IN (SELECT name FROM categories)",
			wantRows: [][]interface{}{
				{"Apple"},
				{"Banana"},
			},
		},
		{
			name: "subquery-complex-2 NOT IN with subquery",
			setup: []string{
				"CREATE TABLE employees(id INT, dept_id INT)",
				"INSERT INTO employees VALUES(1, 10)",
				"INSERT INTO employees VALUES(2, 20)",
				"INSERT INTO employees VALUES(3, 30)",
				"CREATE TABLE active_depts(id INT)",
				"INSERT INTO active_depts VALUES(10)",
				"INSERT INTO active_depts VALUES(20)",
			},
			query: "SELECT id FROM employees WHERE dept_id NOT IN (SELECT id FROM active_depts)",
			wantRows: [][]interface{}{
				{int64(3)},
			},
		},
		{
			name: "subquery-complex-3 EXISTS with correlated WHERE",
			setup: []string{
				"CREATE TABLE orders(id INT, customer_id INT, amount REAL)",
				"INSERT INTO orders VALUES(1, 100, 50.0)",
				"INSERT INTO orders VALUES(2, 101, 150.0)",
				"INSERT INTO orders VALUES(3, 100, 200.0)",
				"CREATE TABLE customers(id INT, name TEXT)",
				"INSERT INTO customers VALUES(100, 'Alice')",
				"INSERT INTO customers VALUES(101, 'Bob')",
				"INSERT INTO customers VALUES(102, 'Charlie')",
			},
			query: "SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)",
			wantRows: [][]interface{}{
				{"Alice"},
				{"Bob"},
			},
		},
		{
			name: "subquery-complex-4 scalar subquery with CASE",
			setup: []string{
				"CREATE TABLE inventory(product_id INT, qty INT)",
				"INSERT INTO inventory VALUES(1, 100)",
				"INSERT INTO inventory VALUES(2, 0)",
				"INSERT INTO inventory VALUES(3, 50)",
			},
			query: `SELECT product_id,
				CASE
					WHEN (SELECT qty FROM inventory i WHERE i.product_id = inventory.product_id) > 0
					THEN 'In Stock'
					ELSE 'Out of Stock'
				END as status
				FROM inventory`,
			wantRows: [][]interface{}{
				{int64(1), "In Stock"},
				{int64(2), "Out of Stock"},
				{int64(3), "In Stock"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.db")
			db, err := sql.Open("sqlite_internal", dbPath)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Execute setup statements
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("Setup failed for %q: %v", stmt, err)
				}
			}

			// Execute query
			rows, err := db.Query(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			// Get column count
			cols, err := rows.Columns()
			if err != nil {
				t.Fatalf("Failed to get columns: %v", err)
			}

			// Collect results
			var gotRows [][]interface{}
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("Scan failed: %v", err)
				}

				gotRows = append(gotRows, values)
			}

			if err := rows.Err(); err != nil {
				t.Fatalf("Rows iteration error: %v", err)
			}

			// Compare results
			if len(gotRows) != len(tt.wantRows) {
				t.Errorf("Row count mismatch: got %d, want %d", len(gotRows), len(tt.wantRows))
				t.Logf("Got rows: %v", gotRows)
				t.Logf("Want rows: %v", tt.wantRows)
				return
			}

			for i, gotRow := range gotRows {
				wantRow := tt.wantRows[i]
				if len(gotRow) != len(wantRow) {
					t.Errorf("Row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
					continue
				}

				for j, gotVal := range gotRow {
					wantVal := wantRow[j]
					// Handle type conversions
					if !compareValuesSubquery(gotVal, wantVal) {
						t.Errorf("Row %d, Col %d: got %v (%T), want %v (%T)", i, j, gotVal, gotVal, wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareValuesSubquery compares two values handling type conversions
func compareValuesSubquery(got, want interface{}) bool {
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}

	switch wv := want.(type) {
	case int64:
		if gv, ok := got.(int64); ok {
			return gv == wv
		}
	case float64:
		if gv, ok := got.(float64); ok {
			return gv == wv
		}
	case string:
		if gv, ok := got.(string); ok {
			return gv == wv
		}
		if gv, ok := got.([]byte); ok {
			return string(gv) == wv
		}
	case []byte:
		if gv, ok := got.([]byte); ok {
			return string(gv) == string(wv)
		}
		if gv, ok := got.(string); ok {
			return gv == string(wv)
		}
	}

	return false
}
