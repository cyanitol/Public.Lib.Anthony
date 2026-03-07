// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"testing"
)

// TestSQLiteCompoundBasic tests basic compound query operations (UNION, INTERSECT, EXCEPT)
// Converted from select5.test and related TCL tests
func TestSQLiteCompoundBasic(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		// UNION tests - removes duplicates
		{
			name: "UNION basic - two tables",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(x INTEGER, y TEXT)",
				"INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three')",
				"INSERT INTO t2 VALUES(2, 'two'), (3, 'three'), (4, 'four')",
			},
			query: "SELECT a FROM t1 UNION SELECT x FROM t2 ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "UNION with duplicate removal",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (2), (1)",
				"INSERT INTO t2 VALUES(3), (4), (5), (4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}},
		},
		{
			name: "UNION with same table",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
			},
			query: "SELECT a FROM t1 WHERE a < 3 UNION SELECT a FROM t1 WHERE a > 1 ORDER BY a",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "UNION with string values",
			setup: []string{
				"CREATE TABLE colors1(name TEXT)",
				"CREATE TABLE colors2(name TEXT)",
				"INSERT INTO colors1 VALUES('red'), ('blue'), ('green')",
				"INSERT INTO colors2 VALUES('blue'), ('yellow'), ('red')",
			},
			query: "SELECT name FROM colors1 UNION SELECT name FROM colors2 ORDER BY name",
			want:  [][]interface{}{{"blue"}, {"green"}, {"red"}, {"yellow"}},
		},
		{
			name: "UNION with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(x INTEGER, y TEXT)",
				"INSERT INTO t1 VALUES(1, 'a'), (2, 'b')",
				"INSERT INTO t2 VALUES(1, 'a'), (3, 'c')",
			},
			query: "SELECT a, b FROM t1 UNION SELECT x, y FROM t2 ORDER BY a, b",
			want:  [][]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}},
		},

		// UNION ALL tests - keeps duplicates
		{
			name: "UNION ALL basic",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(2)}, {int64(3)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "UNION ALL with duplicates kept",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (2), (3)",
				"INSERT INTO t2 VALUES(2), (2), (4)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(2)}, {int64(2)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "UNION ALL with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(x INTEGER, y TEXT)",
				"INSERT INTO t1 VALUES(1, 'x'), (2, 'y')",
				"INSERT INTO t2 VALUES(1, 'x'), (2, 'y')",
			},
			query: "SELECT a, b FROM t1 UNION ALL SELECT x, y FROM t2 ORDER BY a, b",
			want:  [][]interface{}{{int64(1), "x"}, {int64(1), "x"}, {int64(2), "y"}, {int64(2), "y"}},
		},

		// INTERSECT tests - common rows
		{
			name: "INTERSECT basic",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4)",
				"INSERT INTO t2 VALUES(2), (3), (4), (5)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "INTERSECT with no common rows",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2",
			want:  [][]interface{}{},
		},
		{
			name: "INTERSECT with duplicates removed",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (2), (3), (3), (3)",
				"INSERT INTO t2 VALUES(2), (2), (3), (3), (4)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			name: "INTERSECT with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(x INTEGER, y TEXT)",
				"INSERT INTO t1 VALUES(1, 'a'), (2, 'b'), (3, 'c')",
				"INSERT INTO t2 VALUES(2, 'b'), (3, 'c'), (4, 'd')",
			},
			query: "SELECT a, b FROM t1 INTERSECT SELECT x, y FROM t2 ORDER BY a",
			want:  [][]interface{}{{int64(2), "b"}, {int64(3), "c"}},
		},
		{
			name: "INTERSECT with all rows common",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(1), (2), (3), (1), (2)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},

		// EXCEPT tests - rows in first but not second
		{
			name: "EXCEPT basic",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4), (5)",
				"INSERT INTO t2 VALUES(2), (4), (6)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(3)}, {int64(5)}},
		},
		{
			name: "EXCEPT with no difference",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(1), (2), (3), (4)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2",
			want:  [][]interface{}{},
		},
		{
			name: "EXCEPT with all different",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "EXCEPT with duplicates removed",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (1), (2), (2), (3), (3)",
				"INSERT INTO t2 VALUES(2), (2)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(3)}},
		},
		{
			name: "EXCEPT with multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(x INTEGER, y TEXT)",
				"INSERT INTO t1 VALUES(1, 'a'), (2, 'b'), (3, 'c')",
				"INSERT INTO t2 VALUES(2, 'b'), (4, 'd')",
			},
			query: "SELECT a, b FROM t1 EXCEPT SELECT x, y FROM t2 ORDER BY a",
			want:  [][]interface{}{{int64(1), "a"}, {int64(3), "c"}},
		},
		{
			name: "EXCEPT reverse order",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4), (5)",
			},
			query: "SELECT n FROM t2 EXCEPT SELECT n FROM t1 ORDER BY n",
			want:  [][]interface{}{{int64(4)}, {int64(5)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			// Execute setup
			execSQL(t, db, tt.setup...)

			// Execute query
			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundWithOrderBy tests compound queries with ORDER BY clauses
func TestSQLiteCompoundWithOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "UNION with ORDER BY",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, name TEXT)",
				"CREATE TABLE t2(n INTEGER, name TEXT)",
				"INSERT INTO t1 VALUES(3, 'three'), (1, 'one')",
				"INSERT INTO t2 VALUES(4, 'four'), (2, 'two')",
			},
			query: "SELECT n, name FROM t1 UNION SELECT n, name FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1), "one"}, {int64(2), "two"}, {int64(3), "three"}, {int64(4), "four"}},
		},
		{
			name: "UNION with ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n DESC",
			want:  [][]interface{}{{int64(5)}, {int64(4)}, {int64(3)}, {int64(2)}, {int64(1)}},
		},
		{
			name: "UNION ALL with ORDER BY multiple columns",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(a INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1, 'z'), (2, 'a')",
				"INSERT INTO t2 VALUES(1, 'a'), (2, 'z')",
			},
			query: "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2 ORDER BY a, b",
			want:  [][]interface{}{{int64(1), "a"}, {int64(1), "z"}, {int64(2), "a"}, {int64(2), "z"}},
		},
		{
			name: "INTERSECT with ORDER BY",
			setup: []string{
				"CREATE TABLE t1(n INTEGER, v TEXT)",
				"CREATE TABLE t2(n INTEGER, v TEXT)",
				"INSERT INTO t1 VALUES(3, 'x'), (1, 'y'), (2, 'z')",
				"INSERT INTO t2 VALUES(2, 'z'), (3, 'x'), (4, 'w')",
			},
			query: "SELECT n, v FROM t1 INTERSECT SELECT n, v FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(2), "z"}, {int64(3), "x"}},
		},
		{
			name: "EXCEPT with ORDER BY DESC",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4), (5)",
				"INSERT INTO t2 VALUES(2), (4)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n DESC",
			want:  [][]interface{}{{int64(5)}, {int64(3)}, {int64(1)}},
		},
		{
			name: "UNION with ORDER BY on text column",
			setup: []string{
				"CREATE TABLE t1(name TEXT)",
				"CREATE TABLE t2(name TEXT)",
				"INSERT INTO t1 VALUES('charlie'), ('alice')",
				"INSERT INTO t2 VALUES('bob'), ('david')",
			},
			query: "SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name",
			want:  [][]interface{}{{"alice"}, {"bob"}, {"charlie"}, {"david"}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundWithLimit tests compound queries with LIMIT and OFFSET
func TestSQLiteCompoundWithLimit(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "UNION with LIMIT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n LIMIT 3",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "UNION with OFFSET",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n LIMIT -1 OFFSET 2",
			want:  [][]interface{}{{int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
		},
		{
			name: "UNION with LIMIT and OFFSET",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(4), (5), (6)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n LIMIT 2 OFFSET 2",
			want:  [][]interface{}{{int64(3)}, {int64(4)}},
		},
		{
			name: "UNION ALL with LIMIT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2), (3)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 ORDER BY n LIMIT 3",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(2)}},
		},
		{
			name: "INTERSECT with LIMIT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4), (5)",
				"INSERT INTO t2 VALUES(2), (3), (4), (5), (6)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 ORDER BY n LIMIT 2",
			want:  [][]interface{}{{int64(2)}, {int64(3)}},
		},
		{
			name: "EXCEPT with LIMIT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4), (5)",
				"INSERT INTO t2 VALUES(3), (4)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n LIMIT 2",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "UNION with LIMIT 0",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(3), (4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 LIMIT 0",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundMultiple tests multiple compound operators in one query
func TestSQLiteCompoundMultiple(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "UNION then UNION",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2), (3)",
				"INSERT INTO t3 VALUES(3), (4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 UNION SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "UNION ALL then UNION",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2), (3)",
				"INSERT INTO t3 VALUES(1), (3)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 UNION SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "INTERSECT then UNION",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4)",
				"INSERT INTO t3 VALUES(5), (6)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 UNION SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(5)}, {int64(6)}},
		},
		{
			name: "UNION then INTERSECT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2), (3)",
				"INSERT INTO t3 VALUES(2), (4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 INTERSECT SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name: "EXCEPT then UNION",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t3 VALUES(4), (5)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 UNION SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(3)}, {int64(4)}, {int64(5)}},
		},
		{
			name: "Three UNION operations",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"CREATE TABLE t4(n INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(2)",
				"INSERT INTO t3 VALUES(3)",
				"INSERT INTO t4 VALUES(4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 UNION SELECT n FROM t3 UNION SELECT n FROM t4 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "INTERSECT then EXCEPT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4)",
				"INSERT INTO t2 VALUES(2), (3), (4), (5)",
				"INSERT INTO t3 VALUES(3), (4)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 EXCEPT SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundErrors tests error cases for compound queries
func TestSQLiteCompoundErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		wantErr bool
		skip    string
	}{
		{
			name: "UNION with different column counts",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER)",
				"CREATE TABLE t2(x INTEGER)",
				"INSERT INTO t1 VALUES(1, 2)",
				"INSERT INTO t2 VALUES(1)",
			},
			query:   "SELECT a, b FROM t1 UNION SELECT x FROM t2",
			wantErr: true,
		},
		{
			name: "UNION ALL with different column counts",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(x INTEGER, y INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(1, 2)",
			},
			query:   "SELECT a FROM t1 UNION ALL SELECT x, y FROM t2",
			wantErr: true,
		},
		{
			name: "INTERSECT with different column counts",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)",
				"CREATE TABLE t2(x INTEGER, y INTEGER)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t2 VALUES(1, 2)",
			},
			query:   "SELECT a, b, c FROM t1 INTERSECT SELECT x, y FROM t2",
			wantErr: true,
		},
		{
			name: "EXCEPT with different column counts",
			setup: []string{
				"CREATE TABLE t1(a INTEGER)",
				"CREATE TABLE t2(x INTEGER, y INTEGER, z INTEGER)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t2 VALUES(1, 2, 3)",
			},
			query:   "SELECT a FROM t1 EXCEPT SELECT x, y, z FROM t2",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)
			expectQueryError(t, db, tt.query)
		})
	}
}

// TestSQLiteCompoundTypeCoercion tests type coercion in compound queries
func TestSQLiteCompoundTypeCoercion(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "UNION with integer and text",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(s TEXT)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES('3'), ('4')",
			},
			query: "SELECT n FROM t1 UNION SELECT s FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {"3"}, {"4"}},
		},
		{
			name: "UNION with integer and real",
			skip: "REAL type coercion in UNION not yet implemented",
			setup: []string{
				"CREATE TABLE t1(i INTEGER)",
				"CREATE TABLE t2(r REAL)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2.5), (3.5)",
			},
			query: "SELECT i FROM t1 UNION SELECT r FROM t2 ORDER BY i",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {2.5}, {3.5}},
		},
		{
			name: "UNION with text and blob",
			skip: "BLOB to TEXT conversion in UNION not yet implemented",
			setup: []string{
				"CREATE TABLE t1(t TEXT)",
				"CREATE TABLE t2(b BLOB)",
				"INSERT INTO t1 VALUES('hello')",
				"INSERT INTO t2 VALUES(X'776F726C64')",
			},
			query: "SELECT t FROM t1 UNION SELECT b FROM t2",
			want:  [][]interface{}{{"hello"}, {"world"}},
		},
		{
			name: "INTERSECT with mixed types",
			skip: "INTERSECT type coercion not yet implemented",
			setup: []string{
				"CREATE TABLE t1(v TEXT)",
				"CREATE TABLE t2(v INTEGER)",
				"INSERT INTO t1 VALUES('1'), ('2'), ('3')",
				"INSERT INTO t2 VALUES(2), (3), (4)",
			},
			query: "SELECT v FROM t1 INTERSECT SELECT v FROM t2 ORDER BY v",
			want:  [][]interface{}{{"2"}, {"3"}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundWithNulls tests compound queries with NULL values
func TestSQLiteCompoundWithNulls(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "UNION with NULLs",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (NULL), (3)",
				"INSERT INTO t2 VALUES(2), (NULL), (4)",
			},
			query: "SELECT n FROM t1 UNION SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{nil}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name: "UNION ALL with multiple NULLs",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(NULL), (NULL)",
				"INSERT INTO t2 VALUES(NULL), (1)",
			},
			query: "SELECT n FROM t1 UNION ALL SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{nil}, {nil}, {nil}, {int64(1)}},
		},
		{
			name: "INTERSECT with NULLs",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (NULL), (3)",
				"INSERT INTO t2 VALUES(NULL), (3), (4)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{nil}, {int64(3)}},
		},
		{
			name: "EXCEPT with NULLs in first set",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (NULL), (3)",
				"INSERT INTO t2 VALUES(3)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{nil}, {int64(1)}},
		},
		{
			name: "EXCEPT with NULLs in second set",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(NULL), (2)",
			},
			query: "SELECT n FROM t1 EXCEPT SELECT n FROM t2 ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(3)}},
		},
		{
			name: "UNION with NULL and non-NULL matching",
			setup: []string{
				"CREATE TABLE t1(a INTEGER, b TEXT)",
				"CREATE TABLE t2(a INTEGER, b TEXT)",
				"INSERT INTO t1 VALUES(1, 'x'), (NULL, 'y')",
				"INSERT INTO t2 VALUES(1, 'x'), (NULL, 'y')",
			},
			query: "SELECT a, b FROM t1 UNION SELECT a, b FROM t2 ORDER BY a, b",
			want:  [][]interface{}{{nil, "y"}, {int64(1), "x"}},
		},
		{
			name: "INTERSECT with all NULLs",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(NULL), (NULL)",
				"INSERT INTO t2 VALUES(NULL)",
			},
			query: "SELECT n FROM t1 INTERSECT SELECT n FROM t2",
			want:  [][]interface{}{{nil}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundNested tests nested compound queries
func TestSQLiteCompoundNested(t *testing.T) {
	t.Skip("nested compound queries (subquery with UNION) not yet implemented")
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "Parenthesized UNION",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2)",
				"INSERT INTO t2 VALUES(2), (3)",
				"INSERT INTO t3 VALUES(1), (3)",
			},
			query: "SELECT n FROM (SELECT n FROM t1 UNION SELECT n FROM t2) ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name: "Nested UNION with WHERE",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4)",
				"INSERT INTO t2 VALUES(3), (4), (5), (6)",
			},
			query: "SELECT n FROM (SELECT n FROM t1 UNION SELECT n FROM t2) WHERE n > 3 ORDER BY n",
			want:  [][]interface{}{{int64(4)}, {int64(5)}, {int64(6)}},
		},
		{
			name: "UNION of subqueries",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3), (4), (5)",
			},
			query: "SELECT n FROM (SELECT n FROM t1 WHERE n < 3) UNION SELECT n FROM (SELECT n FROM t1 WHERE n > 3) ORDER BY n",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(4)}, {int64(5)}},
		},
		{
			name: "INTERSECT in subquery",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4)",
				"INSERT INTO t3 VALUES(2), (5)",
			},
			query: "SELECT n FROM (SELECT n FROM t1 INTERSECT SELECT n FROM t2) UNION SELECT n FROM t3 ORDER BY n",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(5)}},
		},
		{
			name: "Complex nested compound",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4)",
			},
			query: `
				SELECT n FROM (
					SELECT n FROM (SELECT n FROM t1 WHERE n > 1)
					UNION
					SELECT n FROM (SELECT n FROM t2 WHERE n < 4)
				) ORDER BY n
			`,
			want: [][]interface{}{{int64(2)}, {int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundInSubquery tests compound queries used within subqueries
func TestSQLiteCompoundInSubquery(t *testing.T) {
	t.Skip("compound queries in subqueries not yet implemented")
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "IN with UNION subquery",
			setup: []string{
				"CREATE TABLE products(id INTEGER, category TEXT)",
				"CREATE TABLE active_cats(name TEXT)",
				"CREATE TABLE featured_cats(name TEXT)",
				"INSERT INTO products VALUES(1, 'electronics'), (2, 'books'), (3, 'toys')",
				"INSERT INTO active_cats VALUES('electronics')",
				"INSERT INTO featured_cats VALUES('books')",
			},
			query: "SELECT id FROM products WHERE category IN (SELECT name FROM active_cats UNION SELECT name FROM featured_cats) ORDER BY id",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "EXISTS with UNION subquery",
			setup: []string{
				"CREATE TABLE orders(id INTEGER, customer_id INTEGER)",
				"CREATE TABLE vip_customers(id INTEGER)",
				"CREATE TABLE premium_customers(id INTEGER)",
				"INSERT INTO orders VALUES(1, 100), (2, 200), (3, 300)",
				"INSERT INTO vip_customers VALUES(100)",
				"INSERT INTO premium_customers VALUES(200)",
			},
			query: `
				SELECT id FROM orders o
				WHERE EXISTS (
					SELECT id FROM vip_customers WHERE id = o.customer_id
					UNION
					SELECT id FROM premium_customers WHERE id = o.customer_id
				)
				ORDER BY id
			`,
			want: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "Scalar subquery with INTERSECT",
			setup: []string{
				"CREATE TABLE t1(n INTEGER)",
				"CREATE TABLE t2(n INTEGER)",
				"CREATE TABLE t3(n INTEGER)",
				"INSERT INTO t1 VALUES(1), (2), (3)",
				"INSERT INTO t2 VALUES(2), (3), (4)",
				"INSERT INTO t3 VALUES(3), (4), (5)",
			},
			query: "SELECT (SELECT COUNT(*) FROM (SELECT n FROM t1 INTERSECT SELECT n FROM t2))",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name: "NOT IN with EXCEPT subquery",
			setup: []string{
				"CREATE TABLE all_items(id INTEGER)",
				"CREATE TABLE sold_items(id INTEGER)",
				"CREATE TABLE reserved_items(id INTEGER)",
				"INSERT INTO all_items VALUES(1), (2), (3), (4), (5)",
				"INSERT INTO sold_items VALUES(2), (3)",
				"INSERT INTO reserved_items VALUES(4)",
			},
			query: `
				SELECT id FROM all_items
				WHERE id NOT IN (
					SELECT id FROM sold_items
					UNION
					SELECT id FROM reserved_items
				)
				ORDER BY id
			`,
			want: [][]interface{}{{int64(1)}, {int64(5)}},
		},
		{
			name: "Subquery with UNION ALL in FROM",
			setup: []string{
				"CREATE TABLE jan_sales(amount INTEGER)",
				"CREATE TABLE feb_sales(amount INTEGER)",
				"INSERT INTO jan_sales VALUES(100), (200)",
				"INSERT INTO feb_sales VALUES(150), (250)",
			},
			query: "SELECT SUM(amount) as total FROM (SELECT amount FROM jan_sales UNION ALL SELECT amount FROM feb_sales)",
			want:  [][]interface{}{{int64(700)}},
		},
		{
			name: "Correlated subquery with UNION",
			setup: []string{
				"CREATE TABLE employees(id INTEGER, dept_id INTEGER)",
				"CREATE TABLE dept1_bonus(emp_id INTEGER)",
				"CREATE TABLE dept2_bonus(emp_id INTEGER)",
				"INSERT INTO employees VALUES(1, 1), (2, 1), (3, 2), (4, 2)",
				"INSERT INTO dept1_bonus VALUES(1)",
				"INSERT INTO dept2_bonus VALUES(3)",
			},
			query: `
				SELECT id FROM employees e
				WHERE id IN (
					SELECT emp_id FROM dept1_bonus
					UNION
					SELECT emp_id FROM dept2_bonus
				)
				ORDER BY id
			`,
			want: [][]interface{}{{int64(1)}, {int64(3)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteCompoundComplex tests complex real-world compound query scenarios
func TestSQLiteCompoundComplex(t *testing.T) {
	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
		skip    string
	}{
		{
			name: "Symmetric difference using UNION and EXCEPT",
			skip: "complex multi-operator compound queries not yet supported",
			setup: []string{
				"CREATE TABLE set_a(value INTEGER)",
				"CREATE TABLE set_b(value INTEGER)",
				"INSERT INTO set_a VALUES(1), (2), (3), (4)",
				"INSERT INTO set_b VALUES(3), (4), (5), (6)",
			},
			query: `
				SELECT value FROM set_a EXCEPT SELECT value FROM set_b
				UNION
				SELECT value FROM set_b EXCEPT SELECT value FROM set_a
				ORDER BY value
			`,
			want: [][]interface{}{{int64(1)}, {int64(2)}, {int64(5)}, {int64(6)}},
		},
		{
			name: "UNION with aggregates",
			skip: "GROUP BY in compound queries not yet supported",
			setup: []string{
				"CREATE TABLE q1_sales(product TEXT, amount INTEGER)",
				"CREATE TABLE q2_sales(product TEXT, amount INTEGER)",
				"INSERT INTO q1_sales VALUES('A', 100), ('A', 150), ('B', 200)",
				"INSERT INTO q2_sales VALUES('A', 120), ('B', 180), ('C', 90)",
			},
			query: `
				SELECT product, SUM(amount) as total FROM q1_sales GROUP BY product
				UNION
				SELECT product, SUM(amount) as total FROM q2_sales GROUP BY product
				ORDER BY product
			`,
			want: [][]interface{}{{"A", int64(120)}, {"A", int64(250)}, {"B", int64(180)}, {"B", int64(200)}, {"C", int64(90)}},
		},
		{
			name: "UNION with DISTINCT and common table",
			// DISTINCT now implemented
			setup: []string{
				"CREATE TABLE events(id INTEGER, event_type TEXT, user_id INTEGER)",
				"INSERT INTO events VALUES(1, 'login', 100)",
				"INSERT INTO events VALUES(2, 'purchase', 100)",
				"INSERT INTO events VALUES(3, 'login', 200)",
				"INSERT INTO events VALUES(4, 'logout', 100)",
				"INSERT INTO events VALUES(5, 'purchase', 300)",
			},
			query: `
				SELECT DISTINCT user_id FROM events WHERE event_type = 'login'
				UNION
				SELECT DISTINCT user_id FROM events WHERE event_type = 'purchase'
				ORDER BY user_id
			`,
			want: [][]interface{}{{int64(100)}, {int64(200)}, {int64(300)}},
		},
		{
			name: "INTERSECT for common users",
			setup: []string{
				"CREATE TABLE newsletter_subscribers(email TEXT)",
				"CREATE TABLE app_users(email TEXT)",
				"INSERT INTO newsletter_subscribers VALUES('alice@example.com'), ('bob@example.com'), ('charlie@example.com')",
				"INSERT INTO app_users VALUES('bob@example.com'), ('charlie@example.com'), ('david@example.com')",
			},
			query: `
				SELECT email FROM newsletter_subscribers
				INTERSECT
				SELECT email FROM app_users
				ORDER BY email
			`,
			want: [][]interface{}{{"bob@example.com"}, {"charlie@example.com"}},
		},
		{
			name: "Three-way UNION with different sources",
			setup: []string{
				"CREATE TABLE online_orders(order_id INTEGER)",
				"CREATE TABLE phone_orders(order_id INTEGER)",
				"CREATE TABLE store_orders(order_id INTEGER)",
				"INSERT INTO online_orders VALUES(101), (102)",
				"INSERT INTO phone_orders VALUES(201), (202)",
				"INSERT INTO store_orders VALUES(301), (302)",
			},
			query: `
				SELECT order_id FROM online_orders
				UNION ALL
				SELECT order_id FROM phone_orders
				UNION ALL
				SELECT order_id FROM store_orders
				ORDER BY order_id
			`,
			want: [][]interface{}{
				{int64(101)}, {int64(102)},
				{int64(201)}, {int64(202)},
				{int64(301)}, {int64(302)},
			},
		},
		{
			name: "EXCEPT for missing prerequisites",
			setup: []string{
				"CREATE TABLE required_courses(course_id INTEGER)",
				"CREATE TABLE completed_courses(course_id INTEGER)",
				"INSERT INTO required_courses VALUES(101), (102), (103), (104)",
				"INSERT INTO completed_courses VALUES(101), (103)",
			},
			query: `
				SELECT course_id FROM required_courses
				EXCEPT
				SELECT course_id FROM completed_courses
				ORDER BY course_id
			`,
			want: [][]interface{}{{int64(102)}, {int64(104)}},
		},
		{
			name: "Complex multi-level compound",
			skip: "nested compound queries in subqueries not yet supported",
			setup: []string{
				"CREATE TABLE a(n INTEGER)",
				"CREATE TABLE b(n INTEGER)",
				"CREATE TABLE c(n INTEGER)",
				"CREATE TABLE d(n INTEGER)",
				"INSERT INTO a VALUES(1), (2), (3)",
				"INSERT INTO b VALUES(2), (3), (4)",
				"INSERT INTO c VALUES(3), (4), (5)",
				"INSERT INTO d VALUES(1), (5)",
			},
			query: `
				SELECT n FROM (
					SELECT n FROM a INTERSECT SELECT n FROM b
					UNION
					SELECT n FROM c INTERSECT SELECT n FROM d
				)
				ORDER BY n
			`,
			want: [][]interface{}{{int64(2)}, {int64(3)}, {int64(5)}},
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			db := setupMemoryDB(t)
			defer db.Close()

			execSQL(t, db, tt.setup...)

			if tt.wantErr {
				expectQueryError(t, db, tt.query)
				return
			}

			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}
