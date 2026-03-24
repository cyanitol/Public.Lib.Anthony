// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

// setupOrderByTestDB creates a temporary test database for ORDER BY tests
func setupOrderByTestDB(t *testing.T) *sql.DB {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db
}

// execSQLOrderBy is a helper to execute SQL statements for ORDER BY tests
func execSQLOrderBy(t *testing.T, db *sql.DB, statements []string) {
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("failed to execute statement %q: %v", stmt, err)
		}
	}
}

// queryOrderBy is a helper to execute a query and return all rows
func queryOrderBy(t *testing.T, db *sql.DB, query string) [][]interface{} {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var results [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}

		// Convert []byte to string for easier comparison
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		results = append(results, values)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	return results
}

// TestSQLiteOrderBySingleColumn tests ORDER BY with a single column
// Converted from orderby1.test, orderby2.test, and sort.test
func TestSQLiteOrderBySingleColumn(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY integer ASC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(3, 'three'), (1, 'one'), (2, 'two')",
			},
			query: "SELECT n FROM t1 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "ORDER BY integer DESC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(3, 'three'), (1, 'one'), (2, 'two')",
			},
			query: "SELECT n FROM t1 ORDER BY n DESC",
			want:  [][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
		},
		{
			name: "ORDER BY text ASC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')",
			},
			query: "SELECT v FROM t1 ORDER BY v",
			want:  [][]interface{}{{"one"}, {"three"}, {"two"}},
		},
		{
			name: "ORDER BY text DESC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')",
			},
			query: "SELECT v FROM t1 ORDER BY v DESC",
			want:  [][]interface{}{{"two"}, {"three"}, {"one"}},
		},
		{
			name: "ORDER BY float ASC",
			setup: []string{
				"CREATE TABLE t1(flt REAL)",
				"INSERT INTO t1 VALUES(3.14), (-1.6), (123.0), (-11.0), (2.15)",
			},
			query: "SELECT flt FROM t1 ORDER BY flt",
			want:  [][]interface{}{{-11.0}, {-1.6}, {2.15}, {3.14}, {123.0}},
		},
		{
			name: "ORDER BY float DESC",
			setup: []string{
				"CREATE TABLE t1(flt REAL)",
				"INSERT INTO t1 VALUES(3.14), (-1.6), (123.0), (-11.0), (2.15)",
			},
			query: "SELECT flt FROM t1 ORDER BY flt DESC",
			want:  [][]interface{}{{123.0}, {3.14}, {2.15}, {-1.6}, {-11.0}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			// Create a fresh database for each test
			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByMultipleColumns tests ORDER BY with multiple columns
// Converted from orderby1.test and orderby2.test
func TestSQLiteOrderByMultipleColumns(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY two columns ASC",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, flt REAL)",
				"INSERT INTO t1 VALUES(0, 3.14), (1, 2.15), (1, 4221.0), (2, -11.0), (2, -0.001), (2, 0.123), (2, 123.0), (3, -1.6)",
			},
			query: "SELECT log, flt FROM t1 ORDER BY log, flt",
			want: [][]interface{}{
				{int64(0), 3.14},
				{int64(1), 2.15},
				{int64(1), 4221.0},
				{int64(2), -11.0},
				{int64(2), -0.001},
				{int64(2), 0.123},
				{int64(2), 123.0},
				{int64(3), -1.6},
			},
		},
		{
			name: "ORDER BY two columns mixed ASC/DESC",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, flt REAL)",
				"INSERT INTO t1 VALUES(0, 3.14), (1, 2.15), (1, 4221.0), (2, -11.0), (2, -0.001), (2, 0.123), (2, 123.0), (3, -1.6)",
			},
			query: "SELECT log, flt FROM t1 ORDER BY log ASC, flt DESC",
			want: [][]interface{}{
				{int64(0), 3.14},
				{int64(1), 4221.0},
				{int64(1), 2.15},
				{int64(2), 123.0},
				{int64(2), 0.123},
				{int64(2), -0.001},
				{int64(2), -11.0},
				{int64(3), -1.6},
			},
		},
		{
			name: "ORDER BY two columns both DESC",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, flt REAL)",
				"INSERT INTO t1 VALUES(0, 3.14), (1, 2.15), (1, 4221.0), (2, -11.0), (2, -0.001), (2, 0.123), (2, 123.0), (3, -1.6)",
			},
			query: "SELECT log, flt FROM t1 ORDER BY log DESC, flt DESC",
			want: [][]interface{}{
				{int64(3), -1.6},
				{int64(2), 123.0},
				{int64(2), 0.123},
				{int64(2), -0.001},
				{int64(2), -11.0},
				{int64(1), 4221.0},
				{int64(1), 2.15},
				{int64(0), 3.14},
			},
		},
		{
			name: "ORDER BY three columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO t1 VALUES(1, 2, 3), (1, 2, 1), (1, 1, 5), (2, 3, 4), (2, 1, 2)",
			},
			query: "SELECT a, b, c FROM t1 ORDER BY a, b, c",
			want: [][]interface{}{
				{int64(1), int64(1), int64(5)},
				{int64(1), int64(2), int64(1)},
				{int64(1), int64(2), int64(3)},
				{int64(2), int64(1), int64(2)},
				{int64(2), int64(3), int64(4)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByColumnNumber tests ORDER BY with column numbers
// Converted from orderby4.test and sort.test
func TestSQLiteOrderByColumnNumber(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY column position 1",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(3, 30), (1, 10), (2, 20)",
			},
			query: "SELECT a, b FROM t1 ORDER BY 1",
			want:  [][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}, {int64(3), int64(30)}},
		},
		{
			name: "ORDER BY column position 2",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(3, 10), (1, 30), (2, 20)",
			},
			query: "SELECT a, b FROM t1 ORDER BY 2",
			want:  [][]interface{}{{int64(3), int64(10)}, {int64(2), int64(20)}, {int64(1), int64(30)}},
		},
		{
			name: "ORDER BY multiple column positions",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, x INTEGER)",
				"INSERT INTO t1 VALUES(1, 3), (1, 4)",
				"CREATE TABLE t2(a INTEGER, x INTEGER)",
				"INSERT INTO t2 VALUES(1, 3), (1, 4)",
			},
			query: "SELECT a, x FROM t1, t2 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByExpression tests ORDER BY with expressions
// Converted from sort.test
func TestSQLiteOrderByExpression(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY substr expression",
			setup: []string{
				"CREATE TABLE t1(v TEXT)",
				"INSERT INTO t1 VALUES('x-123.0'), ('x-2.15'), ('x-2b'), ('x11.0'), ('x1.6')",
			},
			query: "SELECT v FROM t1 ORDER BY substr(v, 2, 999)",
			want:  [][]interface{}{{"x-123.0"}, {"x-2.15"}, {"x-2b"}, {"x1.6"}, {"x11.0"}},
		},
		{
			name: "ORDER BY arithmetic expression",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (2)",
			},
			query: "SELECT n FROM t1 ORDER BY n + 0",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "ORDER BY concatenation",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"INSERT INTO t1 VALUES(10), (2), (11), (1)",
			},
			query: "SELECT n FROM t1 ORDER BY n || ''",
			want:  [][]interface{}{{int64(1)}, {int64(10)}, {int64(11)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByWithNulls tests NULL handling in ORDER BY
// Converted from sort.test
func TestSQLiteOrderByWithNulls(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY with NULLs ASC (NULLs first)",
			setup: []string{
				"CREATE TABLE t3(a INTEGER, b TEXT)",
				"INSERT INTO t3 VALUES(5, NULL), (6, NULL), (3, NULL), (4, 'cd'), (1, 'ab'), (2, NULL)",
			},
			query: "SELECT a FROM t3 ORDER BY b, a",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(5)}, {int64(6)}, {int64(1)}, {int64(4)}},
		},
		{
			name: "ORDER BY with NULLs mixed DESC/ASC",
			setup: []string{
				"CREATE TABLE t3(a INTEGER, b TEXT)",
				"INSERT INTO t3 VALUES(5, NULL), (6, NULL), (3, NULL), (4, 'cd'), (1, 'ab'), (2, NULL)",
			},
			query: "SELECT a FROM t3 ORDER BY b, a DESC",
			want:  [][]interface{}{{int64(6)}, {int64(5)}, {int64(3)}, {int64(2)}, {int64(1)}, {int64(4)}},
		},
		{
			name: "ORDER BY with NULLs DESC (NULLs last)",
			setup: []string{
				"CREATE TABLE t3(a INTEGER, b TEXT)",
				"INSERT INTO t3 VALUES(5, NULL), (6, NULL), (3, NULL), (4, 'cd'), (1, 'ab'), (2, NULL)",
			},
			query: "SELECT a FROM t3 ORDER BY b DESC, a",
			want:  [][]interface{}{{int64(4)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(5)}, {int64(6)}},
		},
		{
			name: "ORDER BY with NULLs both DESC",
			setup: []string{
				"CREATE TABLE t3(a INTEGER, b TEXT)",
				"INSERT INTO t3 VALUES(5, NULL), (6, NULL), (3, NULL), (4, 'cd'), (1, 'ab'), (2, NULL)",
			},
			query: "SELECT a FROM t3 ORDER BY b DESC, a DESC",
			want:  [][]interface{}{{int64(4)}, {int64(1)}, {int64(6)}, {int64(5)}, {int64(3)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByWithLimit tests ORDER BY combined with LIMIT
// Converted from orderby1.test
func TestSQLiteOrderByWithLimit(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY with LIMIT",
			setup: []string{
				"CREATE TABLE t10(a INTEGER, b INTEGER, c TEXT)",
				"INSERT INTO t10 VALUES(1, 2, '^'), (8, 9, '^'), (3, 4, '^'), (5, 4, '^'), (0, 7, '^')",
				"CREATE INDEX t10b ON t10(b)",
			},
			query: "SELECT b, a, c FROM t10 ORDER BY b, a LIMIT 4",
			want:  [][]interface{}{{int64(2), int64(1), "^"}, {int64(4), int64(3), "^"}, {int64(4), int64(5), "^"}, {int64(7), int64(0), "^"}},
		},
		{
			name: "ORDER BY with LIMIT top 3",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
			},
			query: "SELECT n FROM t1 ORDER BY n LIMIT 3",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "ORDER BY DESC with LIMIT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')",
			},
			query: "SELECT n FROM t1 ORDER BY n DESC LIMIT 3",
			want:  [][]interface{}{{int64(5)}, {int64(4)}, {int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByJoin tests ORDER BY with JOINs
// Converted from orderby1.test, orderby2.test, and orderby3.test
func TestSQLiteOrderByJoin(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "JOIN with ORDER BY",
			setup: []string{
				"CREATE TABLE album(aid INTEGER PRIMARY KEY, title TEXT UNIQUE NOT NULL)",
				"CREATE TABLE track(tid INTEGER PRIMARY KEY, aid INTEGER NOT NULL REFERENCES album, tn INTEGER NOT NULL, name TEXT, UNIQUE(aid, tn))",
				"INSERT INTO album VALUES(1, '1-one'), (2, '2-two'), (3, '3-three')",
				"INSERT INTO track VALUES(NULL, 1, 1, 'one-a'), (NULL, 2, 2, 'two-b'), (NULL, 3, 3, 'three-c'), (NULL, 1, 3, 'one-c'), (NULL, 2, 1, 'two-a'), (NULL, 3, 1, 'three-a')",
			},
			query: "SELECT name FROM album JOIN track USING (aid) ORDER BY title, tn",
			want:  [][]interface{}{{"one-a"}, {"one-c"}, {"two-a"}, {"two-b"}, {"three-a"}, {"three-c"}},
		},
		{
			name: "JOIN with ORDER BY DESC first column",
			setup: []string{
				"CREATE TABLE album(aid INTEGER PRIMARY KEY, title TEXT UNIQUE NOT NULL)",
				"CREATE TABLE track(tid INTEGER PRIMARY KEY, aid INTEGER NOT NULL REFERENCES album, tn INTEGER NOT NULL, name TEXT, UNIQUE(aid, tn))",
				"INSERT INTO album VALUES(1, '1-one'), (2, '2-two'), (3, '3-three')",
				"INSERT INTO track VALUES(NULL, 1, 1, 'one-a'), (NULL, 2, 2, 'two-b'), (NULL, 3, 3, 'three-c'), (NULL, 1, 3, 'one-c'), (NULL, 2, 1, 'two-a'), (NULL, 3, 1, 'three-a')",
			},
			query: "SELECT name FROM album JOIN track USING (aid) ORDER BY title DESC, tn",
			want:  [][]interface{}{{"three-a"}, {"three-c"}, {"two-a"}, {"two-b"}, {"one-a"}, {"one-c"}},
		},
		{
			name: "3-way join ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY)",
				"CREATE TABLE t2(b INTEGER PRIMARY KEY, c INTEGER)",
				"CREATE TABLE t3(d INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(3, 1), (4, 2), (5, 3)",
				"INSERT INTO t3 VALUES(4), (3), (5)",
			},
			query: "SELECT t1.a FROM t1, t2, t3 WHERE t1.a=t2.c AND t2.b=t3.d ORDER BY t1.a",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "3-way join ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY)",
				"CREATE TABLE t2(b INTEGER PRIMARY KEY, c INTEGER)",
				"CREATE TABLE t3(d INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(3, 1), (4, 2), (5, 3)",
				"INSERT INTO t3 VALUES(4), (3), (5)",
			},
			query: "SELECT t1.a FROM t1, t2, t3 WHERE t1.a=t2.c AND t2.b=t3.d ORDER BY t1.a DESC",
			want:  [][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
		},
		{
			name: "JOIN with WHERE and ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 11), (2, 22)",
				"CREATE TABLE t2(d INTEGER, e TEXT, UNIQUE(d, e))",
				"INSERT INTO t2 VALUES(10, 'ten'), (11, 'eleven'), (12, 'twelve'), (11, 'oneteen')",
			},
			query: "SELECT e FROM t1, t2 WHERE a=1 AND d=b ORDER BY d, e",
			want:  [][]interface{}{{"eleven"}, {"oneteen"}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByCompoundSelect tests ORDER BY with UNION
// Converted from orderby1.test and sort.test
func TestSQLiteOrderByCompoundSelect(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name:  "UNION ALL with ORDER BY",
			setup: []string{},
			query: "SELECT 5 UNION ALL SELECT 3 ORDER BY 1",
			want:  [][]interface{}{{int64(3)}, {int64(5)}},
		},
		{
			name: "UNION with ORDER BY from tables",
			setup: []string{
				"CREATE TABLE t4(a INTEGER, b TEXT)",
				"INSERT INTO t4 VALUES(1, '1'), (2, '2'), (11, '11'), (12, '12')",
			},
			query: "SELECT a FROM t4 UNION SELECT a FROM t4 ORDER BY 1",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)}},
		},
		{
			name: "UNION ALL multiple values ORDER BY",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (3), (5)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t2 VALUES(2), (4), (6)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			if len(tt.setup) > 0 {
				execSQLOrderBy(t, db, tt.setup)
			}
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByRowid tests ORDER BY with rowid
// Converted from sort.test
func TestSQLiteOrderByRowid(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY rowid DESC",
			setup: []string{
				"CREATE TABLE t7(c INTEGER PRIMARY KEY)",
				"INSERT INTO t7 VALUES(1), (2), (3), (4)",
			},
			query: "SELECT c FROM t7 WHERE c <= 3 ORDER BY c DESC",
			want:  [][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
		},
		{
			name: "ORDER BY rowid with condition",
			setup: []string{
				"CREATE TABLE t7(c INTEGER PRIMARY KEY)",
				"INSERT INTO t7 VALUES(1), (2), (3), (4)",
			},
			query: "SELECT c FROM t7 WHERE c < 3 ORDER BY c DESC",
			want:  [][]interface{}{{int64(2)}, {int64(1)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByTypeMixing tests ORDER BY with different types
// Converted from sort.test
func TestSQLiteOrderByTypeMixing(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "ORDER BY mixed integer storage",
			setup: []string{
				"CREATE TABLE t5(a REAL, b TEXT)",
				"INSERT INTO t5 VALUES(100, 'A1'), (100.0, 'A2')",
			},
			query: "SELECT b FROM t5 ORDER BY a, b",
			want:  [][]interface{}{{"A1"}, {"A2"}},
		},
		{
			name: "ORDER BY integer vs text",
			setup: []string{
				"CREATE TABLE t4(a INTEGER, b TEXT)",
				"INSERT INTO t4 VALUES(1, '1'), (2, '2'), (11, '11'), (12, '12')",
			},
			query: "SELECT a FROM t4 ORDER BY 1",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)}},
		},
		{
			name: "ORDER BY text representation of numbers",
			setup: []string{
				"CREATE TABLE t4(a INTEGER, b TEXT)",
				"INSERT INTO t4 VALUES(1, '1'), (2, '2'), (11, '11'), (12, '12')",
			},
			query: "SELECT b FROM t4 ORDER BY 1",
			want:  [][]interface{}{{"1"}, {"11"}, {"12"}, {"2"}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByComplexQueries tests complex ORDER BY scenarios
// Converted from orderby1.test and orderby2.test
func TestSQLiteOrderByComplexQueries(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Nested subquery with ORDER BY",
			setup: []string{
				"CREATE TABLE abc(a INTEGER, b INTEGER, c INTEGER)",
				"INSERT INTO abc VALUES(1, 2, 3), (4, 5, 6), (7, 8, 9)",
			},
			query: "SELECT (SELECT 'hardware' FROM (SELECT 'software' ORDER BY 'firmware' ASC) GROUP BY 1) FROM abc",
			want:  [][]interface{}{{"hardware"}, {"hardware"}, {"hardware"}},
		},
		{
			name: "Multi-table join with complex ORDER BY",
			setup: []string{
				"CREATE TABLE t31(a INTEGER, b INTEGER)",
				"CREATE INDEX t31ab ON t31(a, b)",
				"CREATE TABLE t32(c INTEGER, d INTEGER)",
				"CREATE INDEX t32cd ON t32(c, d)",
				"CREATE TABLE t33(e INTEGER, f INTEGER)",
				"CREATE INDEX t33ef ON t33(e, f)",
				"CREATE TABLE t34(g INTEGER, h INTEGER)",
				"CREATE INDEX t34gh ON t34(g, h)",
				"INSERT INTO t31 VALUES(1, 4), (2, 3), (1, 3)",
				"INSERT INTO t32 VALUES(4, 5), (3, 6), (3, 7), (4, 8)",
				"INSERT INTO t33 VALUES(5, 9), (7, 10), (6, 11), (8, 12), (8, 13), (7, 14)",
				"INSERT INTO t34 VALUES(11, 20), (10, 21), (12, 22), (9, 23), (13, 24), (14, 25), (12, 26)",
			},
			query: "SELECT a||','||c||','||e||','||g FROM t31, t32, t33, t34 WHERE c=b AND e=d AND g=f ORDER BY a ASC, c ASC, e ASC, g ASC",
			want: [][]interface{}{
				{"1,3,6,11"},
				{"1,3,7,10"},
				{"1,3,7,14"},
				{"1,4,5,9"},
				{"1,4,8,12"},
				{"1,4,8,12"},
				{"1,4,8,13"},
				{"2,3,6,11"},
				{"2,3,7,10"},
				{"2,3,7,14"},
			},
		},
		{
			name: "ORDER BY with indexed join",
			setup: []string{
				"CREATE TABLE t41(a INTEGER UNIQUE NOT NULL, b INTEGER NOT NULL)",
				"CREATE INDEX t41ba ON t41(b, a)",
				"CREATE TABLE t42(x INTEGER NOT NULL REFERENCES t41(a), y INTEGER NOT NULL)",
				"CREATE UNIQUE INDEX t42xy ON t42(x, y)",
				"INSERT INTO t41 VALUES(1, 1), (3, 1)",
				"INSERT INTO t42 VALUES(1, 13), (1, 15), (3, 14), (3, 16)",
			},
			query: "SELECT b, y FROM t41 CROSS JOIN t42 ON x=a ORDER BY b, y",
			want:  [][]interface{}{{int64(1), int64(13)}, {int64(1), int64(14)}, {int64(1), int64(15)}, {int64(1), int64(16)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByEdgeCases tests edge cases for ORDER BY
// Converted from orderby1.test and sort.test
func TestSQLiteOrderByEdgeCases(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name:  "ORDER BY constant value",
			setup: []string{},
			query: "SELECT 5 ORDER BY 1",
			want:  [][]interface{}{{int64(5)}},
		},
		{
			name: "ORDER BY with GROUP BY",
			setup: []string{
				"CREATE TABLE t1(x INTEGER)",
				"INSERT INTO t1 VALUES(986)",
			},
			query: "SELECT 986 AS x GROUP BY x ORDER BY x",
			want:  [][]interface{}{{int64(986)}},
		},
		{
			name: "Empty table ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
			},
			query: "SELECT a FROM t1 ORDER BY a",
			want:  nil,
		},
		{
			name:    "Invalid ORDER BY column name",
			setup:   []string{},
			query:   "VALUES(2) EXCEPT SELECT '' ORDER BY abc",
			wantErr: true,
		},
		{
			name: "ORDER BY with duplicate rows",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"INSERT INTO t1 VALUES(1, 1), (1, 1), (2, 2), (2, 2)",
			},
			query: "SELECT a FROM t1 ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(1)}, {int64(2)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			if len(tt.setup) > 0 {
				execSQLOrderBy(t, db, tt.setup)
			}

			if tt.wantErr {
				rows, err := db.Query(tt.query)
				if rows != nil {
					rows.Close()
				}
				if err == nil {
					t.Errorf("expected error for query %q, got none", tt.query)
				}
				return
			}

			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByCollation tests ORDER BY with different collations
func TestSQLiteOrderByCollation(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Case-sensitive text ordering",
			setup: []string{
				"CREATE TABLE t2(a TEXT, b INTEGER)",
				"INSERT INTO t2 VALUES('AGLIENTU', 1), ('AGLIE`', 2), ('AGNA', 3)",
			},
			query: "SELECT a, b FROM t2 ORDER BY a",
			want:  [][]interface{}{{"AGLIENTU", int64(1)}, {"AGLIE`", int64(2)}, {"AGNA", int64(3)}},
		},
		{
			name: "Case-sensitive text ordering DESC",
			setup: []string{
				"CREATE TABLE t2(a TEXT, b INTEGER)",
				"INSERT INTO t2 VALUES('AGLIENTU', 1), ('AGLIE`', 2), ('AGNA', 3)",
			},
			query: "SELECT a, b FROM t2 ORDER BY a DESC",
			want:  [][]interface{}{{"AGNA", int64(3)}, {"AGLIE`", int64(2)}, {"AGLIENTU", int64(1)}},
		},
		{
			name: "Lowercase text ordering",
			setup: []string{
				"CREATE TABLE t2(a TEXT, b INTEGER)",
				"INSERT INTO t2 VALUES('aglientu', 1), ('aglie`', 2), ('agna', 3)",
			},
			query: "SELECT a, b FROM t2 ORDER BY a",
			want:  [][]interface{}{{"aglie`", int64(2)}, {"aglientu", int64(1)}, {"agna", int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByStability tests that ORDER BY is stable
// Converted from sort.test
func TestSQLiteOrderByStability(t *testing.T) {

	db := setupOrderByTestDB(t)
	defer db.Close()

	// Create table
	_, err := db.Exec("CREATE TABLE t10(a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert 1000 rows (reduced from 100000 for test performance)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO t10 VALUES(?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for i := 0; i < 1000; i++ {
		_, err := stmt.Exec(i/10, i%10)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Test that ORDER BY a gives same results as ORDER BY a, b (stability test)
	rows1 := queryOrderBy(t, db, "SELECT a, b FROM t10 ORDER BY a, b")
	rows2 := queryOrderBy(t, db, "SELECT a, b FROM t10 ORDER BY a")

	if !reflect.DeepEqual(rows1, rows2) {
		t.Errorf("ORDER BY is not stable: sorted by 'a' differs from sorted by 'a, b'")
	}
}

// TestSQLiteOrderByASCExplicit tests explicit ASC keyword
func TestSQLiteOrderByASCExplicit(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Explicit ASC single column",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"INSERT INTO t1 VALUES(3), (1), (2)",
			},
			query: "SELECT n FROM t1 ORDER BY n ASC",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "Explicit ASC equals default",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')",
			},
			query: "SELECT ALL n FROM t1 ORDER BY n ASC",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "Mixed ASC explicit in multi-column",
			setup: []string{
				"CREATE TABLE t1(log INTEGER, flt REAL)",
				"INSERT INTO t1 VALUES(1, 3.14), (2, 1.6), (1, 2.15)",
			},
			query: "SELECT log, flt FROM t1 ORDER BY log ASC, flt ASC",
			want:  [][]interface{}{{int64(1), 2.15}, {int64(1), 3.14}, {int64(2), 1.6}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByJoinUnique tests ORDER BY with unique constraints in joins
// Converted from sort.test
func TestSQLiteOrderByJoinUnique(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Join with unique constraint - order matters",
			setup: []string{
				"CREATE TABLE t8(a INTEGER UNIQUE, b INTEGER, c INTEGER)",
				"INSERT INTO t8 VALUES(1, 2, 3), (2, 3, 4)",
				"CREATE TABLE t9(x INTEGER, y INTEGER)",
				"INSERT INTO t9 VALUES(2, 4), (2, 3)",
			},
			query: "SELECT y FROM t8, t9 WHERE a=1 ORDER BY a, y",
			want:  [][]interface{}{{int64(3)}, {int64(4)}},
		},
		{
			name: "Join with rowid ordering",
			setup: []string{
				"CREATE TABLE a(id INTEGER PRIMARY KEY)",
				"CREATE TABLE b(id INTEGER PRIMARY KEY, aId INTEGER, text TEXT)",
				"INSERT INTO a VALUES(1)",
				"INSERT INTO b VALUES(2, 1, 'xxx'), (1, 1, 'zzz'), (3, 1, 'yyy')",
			},
			query: "SELECT a.id, b.id, b.text FROM a JOIN b ON (a.id = b.aId) ORDER BY a.id, b.text",
			want:  [][]interface{}{{int64(1), int64(2), "xxx"}, {int64(1), int64(3), "yyy"}, {int64(1), int64(1), "zzz"}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// orderByBenchmarkSetup creates and populates a table with 100 rows for benchmark testing.
func orderByBenchmarkSetup(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("CREATE TABLE t1(a INTEGER, b INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("CREATE INDEX i1 ON t1(a)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO t1 VALUES(?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare: %v", err)
	}
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(i%2, i); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

// orderByBenchmarkVerify checks that rows are sorted by (a, b).
func orderByBenchmarkVerify(t *testing.T, rows [][]interface{}) {
	t.Helper()
	if len(rows) != 100 {
		t.Errorf("expected 100 rows, got %d", len(rows))
	}
	lastA, lastB := int64(-1), int64(-1)
	for _, row := range rows {
		a, b := row[0].(int64), row[1].(int64)
		if a < lastA || (a == lastA && b < lastB) {
			t.Errorf("rows not properly ordered: got (%d, %d) after (%d, %d)", a, b, lastA, lastB)
		}
		lastA, lastB = a, b
	}
}

// TestSQLiteOrderByBenchmark provides a simple benchmark-style test
func TestSQLiteOrderByBenchmark(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	orderByBenchmarkSetup(t, db)
	rows := queryOrderBy(t, db, "SELECT * FROM t1 ORDER BY a, b")
	orderByBenchmarkVerify(t, rows)
}

// TestSQLiteOrderByPrimaryKey tests ORDER BY with multi-column primary keys
// Converted from orderby4.test
func TestSQLiteOrderByPrimaryKey(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Multi-column primary key ORDER BY",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"INSERT INTO t1 VALUES(1, 1), (1, 2)",
				"CREATE TABLE t2(x INTEGER, y INTEGER, PRIMARY KEY(x, y))",
				"INSERT INTO t2 VALUES(3, 3), (4, 4)",
			},
			query: "SELECT a, x FROM t1, t2 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
		{
			name: "Multi-column primary key CROSS JOIN",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"INSERT INTO t1 VALUES(1, 1), (1, 2)",
				"CREATE TABLE t2(x INTEGER, y INTEGER, PRIMARY KEY(x, y))",
				"INSERT INTO t2 VALUES(3, 3), (4, 4)",
			},
			query: "SELECT a, x FROM t1 CROSS JOIN t2 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
		{
			name: "Multi-column primary key reversed join",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
				"INSERT INTO t1 VALUES(1, 1), (1, 2)",
				"CREATE TABLE t2(x INTEGER, y INTEGER, PRIMARY KEY(x, y))",
				"INSERT INTO t2 VALUES(3, 3), (4, 4)",
			},
			query: "SELECT a, x FROM t2 CROSS JOIN t1 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestSQLiteOrderByWithIndex tests ORDER BY using indexes
// Converted from orderby4.test
func TestSQLiteOrderByWithIndex(t *testing.T) {
	db := setupOrderByTestDB(t)
	defer db.Close()

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name: "Single column index ORDER BY",
			setup: []string{
				"CREATE TABLE t3(a INTEGER)",
				"INSERT INTO t3 VALUES(1), (1)",
				"CREATE INDEX t3a ON t3(a)",
				"CREATE TABLE t4(x INTEGER)",
				"INSERT INTO t4 VALUES(3), (4)",
				"CREATE INDEX t4x ON t4(x)",
			},
			query: "SELECT a, x FROM t3, t4 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
		{
			name: "Single column index CROSS JOIN ORDER BY",
			setup: []string{
				"CREATE TABLE t3(a INTEGER)",
				"INSERT INTO t3 VALUES(1), (1)",
				"CREATE INDEX t3a ON t3(a)",
				"CREATE TABLE t4(x INTEGER)",
				"INSERT INTO t4 VALUES(3), (4)",
				"CREATE INDEX t4x ON t4(x)",
			},
			query: "SELECT a, x FROM t3 CROSS JOIN t4 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
		{
			name: "Single column index reversed CROSS JOIN",
			setup: []string{
				"CREATE TABLE t3(a INTEGER)",
				"INSERT INTO t3 VALUES(1), (1)",
				"CREATE INDEX t3a ON t3(a)",
				"CREATE TABLE t4(x INTEGER)",
				"INSERT INTO t4 VALUES(3), (4)",
				"CREATE INDEX t4x ON t4(x)",
			},
			query: "SELECT a, x FROM t4 CROSS JOIN t3 ORDER BY 1, 2",
			want: [][]interface{}{
				{int64(1), int64(3)},
				{int64(1), int64(3)},
				{int64(1), int64(4)},
				{int64(1), int64(4)},
			},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {

			db := setupOrderByTestDB(t)
			defer db.Close()

			execSQLOrderBy(t, db, tt.setup)
			got := queryOrderBy(t, db, tt.query)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query %q\ngot:  %v\nwant: %v", tt.query, got, tt.want)
			}
		})
	}
}

// Test count - this file contains 48 distinct test cases covering:
// - ORDER BY single column (ASC/DESC) - 6 tests
// - ORDER BY multiple columns - 4 tests
// - ORDER BY by column number - 3 tests
// - ORDER BY expression - 3 tests
// - ORDER BY with NULLs - 4 tests
// - ORDER BY with LIMIT - 3 tests
// - ORDER BY with JOINs - 5 tests
// - ORDER BY with UNION - 3 tests
// - ORDER BY with rowid - 2 tests
// - ORDER BY type mixing - 3 tests
// - Complex ORDER BY queries - 3 tests
// - ORDER BY edge cases - 5 tests
// - ORDER BY collation - 3 tests
// - ORDER BY ASC explicit - 3 tests
// - ORDER BY join unique - 2 tests
// - ORDER BY primary key - 3 tests
// - ORDER BY with index - 3 tests
// Total: 51 test cases
