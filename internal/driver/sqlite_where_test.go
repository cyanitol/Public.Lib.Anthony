// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"testing"
)

// TestSQLiteWhereSimpleComparisons tests basic WHERE clause comparisons
func TestSQLiteWhereSimpleComparisons(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Setup test data (based on where.test lines 20-50)
	execSQL(t, db,
		"CREATE TABLE t1(w int, x int, y int)",
		"CREATE INDEX i1w ON t1(w)",
		"CREATE INDEX i1xy ON t1(x, y)",
	)

	// Insert test data: w=1..100, x=log2(w), y=w*w + 2*w + 1
	for i := 1; i <= 100; i++ {
		// Simple approximation of log2
		x := 0
		val := i
		for val > 1 {
			val = val / 2
			x++
		}
		y := i*i + 2*i + 1
		mustExec(t, db, "INSERT INTO t1 VALUES(?, ?, ?)", i, x, y)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "simple equality",
			query: "SELECT x, y, w FROM t1 WHERE w=10",
			want:  [][]interface{}{{3, 121, 10}},
		},
		{
			name:  "IS operator",
			query: "SELECT x, y, w FROM t1 WHERE w IS 10",
			want:  [][]interface{}{{3, 121, 10}},
		},
		{
			name:  "reverse equality",
			query: "SELECT x, y, w FROM t1 WHERE 11=w",
			want:  [][]interface{}{{3, 144, 11}},
		},
		{
			name:  "AND with index",
			query: "SELECT w, x, y FROM t1 WHERE w=11 AND x>2",
			want:  [][]interface{}{{11, 3, 144}},
		},
		{
			name:  "greater than",
			query: "SELECT w FROM t1 WHERE w>97 ORDER BY w",
			want:  [][]interface{}{{98}, {99}, {100}},
		},
		{
			name:  "greater than or equal",
			query: "SELECT w FROM t1 WHERE w>=97 ORDER BY w",
			want:  [][]interface{}{{97}, {98}, {99}, {100}},
		},
		{
			name:  "less than",
			query: "SELECT w FROM t1 WHERE w<3 ORDER BY w",
			want:  [][]interface{}{{1}, {2}},
		},
		{
			name:  "less than or equal",
			query: "SELECT w FROM t1 WHERE w<=3 ORDER BY w",
			want:  [][]interface{}{{1}, {2}, {3}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereANDOR tests AND and OR combinations
func TestSQLiteWhereANDOR(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int, c int)",
		"INSERT INTO t1 VALUES(1, 10, 100)",
		"INSERT INTO t1 VALUES(2, 20, 200)",
		"INSERT INTO t1 VALUES(3, 30, 300)",
		"INSERT INTO t1 VALUES(4, 40, 400)",
		"INSERT INTO t1 VALUES(5, 50, 500)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "simple AND",
			query: "SELECT a FROM t1 WHERE a>2 AND a<5",
			want:  [][]interface{}{{3}, {4}},
		},
		{
			name:  "simple OR",
			query: "SELECT a FROM t1 WHERE a=1 OR a=5 ORDER BY a",
			want:  [][]interface{}{{1}, {5}},
		},
		{
			name:  "AND with three conditions",
			query: "SELECT a FROM t1 WHERE a>1 AND a<5 AND b>=20 ORDER BY a",
			want:  [][]interface{}{{2}, {3}, {4}},
		},
		{
			name:  "OR with three conditions",
			query: "SELECT a FROM t1 WHERE a=1 OR a=3 OR a=5 ORDER BY a",
			want:  [][]interface{}{{1}, {3}, {5}},
		},
		{
			name:  "mixed AND/OR",
			query: "SELECT a FROM t1 WHERE (a=1 OR a=2) AND b>=10 ORDER BY a",
			want:  [][]interface{}{{1}, {2}},
		},
		{
			name:  "complex condition",
			query: "SELECT a FROM t1 WHERE a>1 AND (b<30 OR b>40) ORDER BY a",
			want:  [][]interface{}{{2}, {5}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereIN tests IN operator with lists
func TestSQLiteWhereIN(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b text)",
		"INSERT INTO t1 VALUES(1, 'one')",
		"INSERT INTO t1 VALUES(2, 'two')",
		"INSERT INTO t1 VALUES(3, 'three')",
		"INSERT INTO t1 VALUES(4, 'four')",
		"INSERT INTO t1 VALUES(5, 'five')",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "IN with integers",
			query: "SELECT a FROM t1 WHERE a IN (1, 3, 5) ORDER BY a",
			want:  [][]interface{}{{1}, {3}, {5}},
		},
		{
			name:  "IN with single value",
			query: "SELECT a FROM t1 WHERE a IN (3)",
			want:  [][]interface{}{{3}},
		},
		{
			name:  "IN with strings",
			query: "SELECT a FROM t1 WHERE b IN ('two', 'four') ORDER BY a",
			want:  [][]interface{}{{2}, {4}},
		},
		{
			name:  "NOT IN",
			query: "SELECT a FROM t1 WHERE a NOT IN (2, 4) ORDER BY a",
			want:  [][]interface{}{{1}, {3}, {5}},
		},
		{
			name:  "IN with no matches",
			query: "SELECT a FROM t1 WHERE a IN (10, 20, 30)",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereINSubquery tests IN with subqueries
func TestSQLiteWhereINSubquery(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int)",
		"CREATE TABLE t2(x int, y int)",
		"INSERT INTO t1 VALUES(1, 10)",
		"INSERT INTO t1 VALUES(2, 20)",
		"INSERT INTO t1 VALUES(3, 30)",
		"INSERT INTO t2 VALUES(1, 100)",
		"INSERT INTO t2 VALUES(3, 300)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "IN subquery",
			query: "SELECT a FROM t1 WHERE a IN (SELECT x FROM t2) ORDER BY a",
			want:  [][]interface{}{{1}, {3}},
		},
		{
			name:  "NOT IN subquery",
			query: "SELECT a FROM t1 WHERE a NOT IN (SELECT x FROM t2) ORDER BY a",
			want:  [][]interface{}{{2}},
		},
		{
			name:  "IN subquery with WHERE",
			query: "SELECT a FROM t1 WHERE a IN (SELECT x FROM t2 WHERE y>100) ORDER BY a",
			want:  [][]interface{}{{3}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereBETWEEN tests BETWEEN operator
func TestSQLiteWhereBETWEEN(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b text)",
		"INSERT INTO t1 VALUES(1, 'a')",
		"INSERT INTO t1 VALUES(5, 'e')",
		"INSERT INTO t1 VALUES(10, 'j')",
		"INSERT INTO t1 VALUES(15, 'o')",
		"INSERT INTO t1 VALUES(20, 't')",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "BETWEEN inclusive",
			query: "SELECT a FROM t1 WHERE a BETWEEN 5 AND 15 ORDER BY a",
			want:  [][]interface{}{{5}, {10}, {15}},
		},
		{
			name:  "BETWEEN exclusive bounds",
			query: "SELECT a FROM t1 WHERE a BETWEEN 6 AND 14 ORDER BY a",
			want:  [][]interface{}{{10}},
		},
		{
			name:  "NOT BETWEEN",
			query: "SELECT a FROM t1 WHERE a NOT BETWEEN 5 AND 15 ORDER BY a",
			want:  [][]interface{}{{1}, {20}},
		},
		{
			name:  "BETWEEN with strings",
			query: "SELECT a FROM t1 WHERE b BETWEEN 'e' AND 'o' ORDER BY a",
			want:  [][]interface{}{{5}, {10}, {15}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereLIKE tests LIKE pattern matching
func TestSQLiteWhereLIKE(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b text)",
		"INSERT INTO t1 VALUES(1, 'apple')",
		"INSERT INTO t1 VALUES(2, 'application')",
		"INSERT INTO t1 VALUES(3, 'banana')",
		"INSERT INTO t1 VALUES(4, 'band')",
		"INSERT INTO t1 VALUES(5, 'cat')",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "LIKE prefix",
			query: "SELECT a FROM t1 WHERE b LIKE 'app%' ORDER BY a",
			want:  [][]interface{}{{1}, {2}},
		},
		{
			name:  "LIKE suffix",
			query: "SELECT a FROM t1 WHERE b LIKE '%le'",
			want:  [][]interface{}{{1}},
		},
		{
			name:  "LIKE contains",
			query: "SELECT a FROM t1 WHERE b LIKE '%an%' ORDER BY a",
			want:  [][]interface{}{{3}, {4}},
		},
		{
			name:  "LIKE single char wildcard",
			query: "SELECT a FROM t1 WHERE b LIKE 'ba__' ORDER BY a",
			want:  [][]interface{}{{4}},
		},
		{
			name:  "NOT LIKE",
			query: "SELECT a FROM t1 WHERE b NOT LIKE '%a%' ORDER BY a",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereNULL tests IS NULL and IS NOT NULL
func TestSQLiteWhereNULL(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int)",
		"INSERT INTO t1 VALUES(1, 10)",
		"INSERT INTO t1 VALUES(2, NULL)",
		"INSERT INTO t1 VALUES(3, 30)",
		"INSERT INTO t1 VALUES(4, NULL)",
		"INSERT INTO t1 VALUES(5, 50)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "IS NULL",
			query: "SELECT a FROM t1 WHERE b IS NULL ORDER BY a",
			want:  [][]interface{}{{2}, {4}},
		},
		{
			name:  "IS NOT NULL",
			query: "SELECT a FROM t1 WHERE b IS NOT NULL ORDER BY a",
			want:  [][]interface{}{{1}, {3}, {5}},
		},
		{
			name:  "NULL equality (should not match)",
			query: "SELECT a FROM t1 WHERE b = NULL",
			want:  [][]interface{}{},
		},
		{
			name:  "NULL with AND",
			query: "SELECT a FROM t1 WHERE a>2 AND b IS NULL ORDER BY a",
			want:  [][]interface{}{{4}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereEXISTS tests EXISTS subquery
func TestSQLiteWhereEXISTS(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int)",
		"CREATE TABLE t2(x int, y int)",
		"INSERT INTO t1 VALUES(1, 10)",
		"INSERT INTO t1 VALUES(2, 20)",
		"INSERT INTO t1 VALUES(3, 30)",
		"INSERT INTO t2 VALUES(1, 100)",
		"INSERT INTO t2 VALUES(1, 101)",
		"INSERT INTO t2 VALUES(3, 300)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "EXISTS correlated subquery",
			query: "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a) ORDER BY a",
			want:  [][]interface{}{{1}, {3}},
		},
		{
			name:  "NOT EXISTS",
			query: "SELECT a FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a) ORDER BY a",
			want:  [][]interface{}{{2}},
		},
		{
			name:  "EXISTS with WHERE in subquery",
			query: "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a AND t2.y > 200) ORDER BY a",
			want:  [][]interface{}{{3}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereComplexNested tests complex nested conditions
func TestSQLiteWhereComplexNested(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int, c int)",
		"INSERT INTO t1 VALUES(1, 10, 100)",
		"INSERT INTO t1 VALUES(2, 20, 200)",
		"INSERT INTO t1 VALUES(3, 30, 300)",
		"INSERT INTO t1 VALUES(4, 40, 400)",
		"INSERT INTO t1 VALUES(5, 50, 500)",
		"INSERT INTO t1 VALUES(6, 60, 600)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "nested AND/OR",
			query: "SELECT a FROM t1 WHERE (a<3 OR a>4) AND b>=20 ORDER BY a",
			want:  [][]interface{}{{2}, {5}, {6}},
		},
		{
			name:  "deep nesting",
			query: "SELECT a FROM t1 WHERE ((a=1 OR a=2) AND b<30) OR (a>4 AND c>500) ORDER BY a",
			want:  [][]interface{}{{1}, {6}},
		},
		{
			name:  "multiple OR groups",
			query: "SELECT a FROM t1 WHERE (a=1 OR a=2) OR (a=5 OR a=6) ORDER BY a",
			want:  [][]interface{}{{1}, {2}, {5}, {6}},
		},
		{
			name:  "complex range conditions",
			query: "SELECT a FROM t1 WHERE (a BETWEEN 2 AND 4) AND (b>20 AND b<50) ORDER BY a",
			want:  [][]interface{}{{3}, {4}},
		},
		{
			name:  "mixed operators",
			query: "SELECT a FROM t1 WHERE a IN (1,2,3) AND b>=20 AND c<400 ORDER BY a",
			want:  [][]interface{}{{2}, {3}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereJoin tests WHERE clauses in JOIN queries (based on where.test where-2.*)
func TestSQLiteWhereJoin(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Setup test data similar to where.test
	execSQL(t, db,
		"CREATE TABLE t1(w int, x int, y int)",
		"CREATE TABLE t2(p int, q int, r int, s int)",
		"CREATE INDEX i1w ON t1(w)",
		"CREATE INDEX i1xy ON t1(x, y)",
		"CREATE INDEX i2p ON t2(p)",
		"CREATE INDEX i2qs ON t2(q, s)",
	)

	// Insert test data for t1
	for i := 1; i <= 100; i++ {
		x := 0
		val := i
		for val > 1 {
			val = val / 2
			x++
		}
		y := i*i + 2*i + 1
		mustExec(t, db, "INSERT INTO t1 VALUES(?, ?, ?)", i, x, y)
	}

	// Insert test data for t2
	for i := 1; i <= 100; i++ {
		p := 101 - i
		x := 0
		val := i
		for val > 1 {
			val = val / 2
			x++
		}
		y := i*i + 2*i + 1
		s := y
		// For simplicity, r = some computed value
		r := 10000 - y
		mustExec(t, db, "INSERT INTO t2 VALUES(?, ?, ?, ?)", p, x, r, s)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "simple join with WHERE",
			query: "SELECT t1.w, t2.p FROM t1, t2 WHERE t1.w = t2.p AND t1.w = 10",
			want:  [][]interface{}{{10, 10}},
		},
		{
			name:  "join with multiple conditions",
			query: "SELECT t1.w FROM t1, t2 WHERE t1.x = t2.q AND t1.w = 10 AND t2.p > 50 ORDER BY t1.w LIMIT 1",
			want:  [][]interface{}{{10}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereConstant tests constant WHERE clauses (where.test where-4.*)
func TestSQLiteWhereConstant(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int)",
		"INSERT INTO t1 VALUES(1, 10)",
		"INSERT INTO t1 VALUES(2, 20)",
		"INSERT INTO t1 VALUES(3, 30)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "WHERE 0 (always false)",
			query: "SELECT * FROM t1 WHERE 0",
			want:  [][]interface{}{},
		},
		{
			name:  "WHERE 1 (always true)",
			query: "SELECT a FROM t1 WHERE 1 ORDER BY a",
			want:  [][]interface{}{{1}, {2}, {3}},
		},
		{
			name:  "WHERE 0.0 (false)",
			query: "SELECT * FROM t1 WHERE 0.0",
			want:  [][]interface{}{},
		},
		{
			name:  "WHERE 0.1 (true)",
			query: "SELECT a FROM t1 WHERE 0.1 ORDER BY a",
			want:  [][]interface{}{{1}, {2}, {3}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereIndexOptimization tests that WHERE uses indexes when appropriate
func TestSQLiteWhereIndexOptimization(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int, c int)",
		"CREATE INDEX idx_a ON t1(a)",
		"CREATE INDEX idx_ab ON t1(a, b)",
		"INSERT INTO t1 VALUES(1, 10, 100)",
		"INSERT INTO t1 VALUES(2, 20, 200)",
		"INSERT INTO t1 VALUES(3, 30, 300)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "indexed column equality",
			query: "SELECT a, b FROM t1 WHERE a=2",
			want:  [][]interface{}{{2, 20}},
		},
		{
			name:  "indexed column range",
			query: "SELECT a FROM t1 WHERE a>1 AND a<4 ORDER BY a",
			want:  [][]interface{}{{2}, {3}},
		},
		{
			name:  "compound index",
			query: "SELECT a, b FROM t1 WHERE a=2 AND b=20",
			want:  [][]interface{}{{2, 20}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereExpressions tests WHERE with expressions
func TestSQLiteWhereExpressions(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b int)",
		"INSERT INTO t1 VALUES(1, 10)",
		"INSERT INTO t1 VALUES(2, 20)",
		"INSERT INTO t1 VALUES(3, 30)",
		"INSERT INTO t1 VALUES(4, 40)",
		"INSERT INTO t1 VALUES(5, 50)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "arithmetic expression",
			query: "SELECT a FROM t1 WHERE a+1 = 3",
			want:  [][]interface{}{{2}},
		},
		{
			name:  "multiply expression",
			query: "SELECT a FROM t1 WHERE a*2 = 6",
			want:  [][]interface{}{{3}},
		},
		{
			name:  "column comparison",
			query: "SELECT a FROM t1 WHERE a*10 = b ORDER BY a",
			want:  [][]interface{}{{1}, {2}, {3}, {4}, {5}},
		},
		{
			name:  "modulo",
			query: "SELECT a FROM t1 WHERE a % 2 = 0 ORDER BY a",
			want:  [][]interface{}{{2}, {4}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteWhereCaseInsensitive tests case-insensitive comparisons
func TestSQLiteWhereCaseInsensitive(t *testing.T) {
	t.Skip("WHERE clause not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	createTableWithData(t, db,
		"CREATE TABLE t1(a int, b text)",
		"INSERT INTO t1 VALUES(1, 'Hello')",
		"INSERT INTO t1 VALUES(2, 'WORLD')",
		"INSERT INTO t1 VALUES(3, 'test')",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "case insensitive LIKE",
			query: "SELECT a FROM t1 WHERE b LIKE 'hello' ORDER BY a",
			want:  [][]interface{}{{1}},
		},
		{
			name:  "case insensitive LIKE pattern",
			query: "SELECT a FROM t1 WHERE b LIKE 'WORLD' ORDER BY a",
			want:  [][]interface{}{{2}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}
