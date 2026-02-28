package driver

import (
	"testing"
)

// TestSQLiteLimitBasic tests basic LIMIT functionality from limit.test
func TestSQLiteLimitBasic(t *testing.T) {
	t.Skip("LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Create test data (from limit.test lines 22-32)
	// Creates a table with 32 rows where x ranges from 0-31 and y is calculated based on log2
	execSQL(t, db, "CREATE TABLE t1(x int, y int)")
	for i := 1; i <= 32; i++ {
		// Calculate j such that (1<<j) >= i
		j := 0
		for (1 << j) < i {
			j++
		}
		x := 32 - i
		y := 10 - j
		mustExec(t, db, "INSERT INTO t1 VALUES(?, ?)", x, y)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "count without limit",
			query: "SELECT count(*) FROM t1",
			want:  [][]interface{}{{int64(32)}},
		},
		{
			name:  "count with limit (limit doesn't affect aggregates)",
			query: "SELECT count(*) FROM t1 LIMIT 5",
			want:  [][]interface{}{{int64(32)}},
		},
		{
			name:  "basic limit",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 5",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "limit with offset",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 5 OFFSET 2",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
		},
		{
			name:  "negative offset treated as zero",
			query: "SELECT x FROM t1 ORDER BY x+1 LIMIT 5 OFFSET -2",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "alternate syntax offset, negative limit",
			query: "SELECT x FROM t1 ORDER BY x+1 LIMIT 2, -5",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}, {int64(9)}, {int64(10)}, {int64(11)}, {int64(12)}, {int64(13)}, {int64(14)}, {int64(15)}, {int64(16)}, {int64(17)}, {int64(18)}, {int64(19)}, {int64(20)}, {int64(21)}, {int64(22)}, {int64(23)}, {int64(24)}, {int64(25)}, {int64(26)}, {int64(27)}, {int64(28)}, {int64(29)}, {int64(30)}, {int64(31)}},
		},
		{
			name:  "negative offset, positive limit",
			query: "SELECT x FROM t1 ORDER BY x+1 LIMIT -2, 5",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "both negative (all rows)",
			query: "SELECT x FROM t1 ORDER BY x+1 LIMIT -2, -5",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}, {int64(9)}, {int64(10)}, {int64(11)}, {int64(12)}, {int64(13)}, {int64(14)}, {int64(15)}, {int64(16)}, {int64(17)}, {int64(18)}, {int64(19)}, {int64(20)}, {int64(21)}, {int64(22)}, {int64(23)}, {int64(24)}, {int64(25)}, {int64(26)}, {int64(27)}, {int64(28)}, {int64(29)}, {int64(30)}, {int64(31)}},
		},
		{
			name:  "alternate syntax offset, limit",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 2, 5",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}, {int64(6)}},
		},
		{
			name:  "limit with offset keyword",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 5 OFFSET 5",
			want:  [][]interface{}{{int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}, {int64(9)}},
		},
		{
			name:  "limit exceeds available rows",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 50 OFFSET 30",
			want:  [][]interface{}{{int64(30)}, {int64(31)}},
		},
		{
			name:  "limit exceeds available rows (alternate syntax)",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 30, 50",
			want:  [][]interface{}{{int64(30)}, {int64(31)}},
		},
		{
			name:  "offset exceeds all rows",
			query: "SELECT x FROM t1 ORDER BY x LIMIT 50 OFFSET 50",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitJoin tests LIMIT with joins
func TestSQLiteLimitJoin(t *testing.T) {
	t.Skip("LIMIT with JOIN not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Setup same data as TestSQLiteLimitBasic
	execSQL(t, db, "CREATE TABLE t1(x int, y int)")
	for i := 1; i <= 32; i++ {
		j := 0
		for (1 << j) < i {
			j++
		}
		x := 32 - i
		y := 10 - j
		mustExec(t, db, "INSERT INTO t1 VALUES(?, ?)", x, y)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "limit on cross join",
			query: "SELECT * FROM t1 AS a, t1 AS b ORDER BY a.x, b.x LIMIT 5",
			want: [][]interface{}{
				{int64(0), int64(5), int64(0), int64(5)},
				{int64(0), int64(5), int64(1), int64(5)},
				{int64(0), int64(5), int64(2), int64(5)},
				{int64(0), int64(5), int64(3), int64(5)},
				{int64(0), int64(5), int64(4), int64(5)},
			},
		},
		{
			name:  "limit with offset on cross join",
			query: "SELECT * FROM t1 AS a, t1 AS b ORDER BY a.x, b.x LIMIT 5 OFFSET 32",
			want: [][]interface{}{
				{int64(1), int64(5), int64(0), int64(5)},
				{int64(1), int64(5), int64(1), int64(5)},
				{int64(1), int64(5), int64(2), int64(5)},
				{int64(1), int64(5), int64(3), int64(5)},
				{int64(1), int64(5), int64(4), int64(5)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitSubquery tests LIMIT in subqueries
func TestSQLiteLimitSubquery(t *testing.T) {
	t.Skip("LIMIT in subqueries not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Setup data
	execSQL(t, db, "CREATE TABLE t1(x int, y int)")
	for i := 1; i <= 32; i++ {
		j := 0
		for (1 << j) < i {
			j++
		}
		x := 32 - i
		y := 10 - j
		mustExec(t, db, "INSERT INTO t1 VALUES(?, ?)", x, y)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "subquery with limit",
			query: "SELECT z FROM (SELECT y*10+x AS z FROM t1 ORDER BY x LIMIT 10) ORDER BY z LIMIT 5",
			want:  [][]interface{}{{int64(50)}, {int64(51)}, {int64(52)}, {int64(53)}, {int64(54)}},
		},
		{
			name:  "count from limited subquery",
			query: "SELECT count(*) FROM (SELECT * FROM t1 LIMIT 2)",
			want:  [][]interface{}{{int64(2)}},
		},
		{
			name:  "rowid in limited subquery",
			query: "SELECT count(*) FROM t1 WHERE rowid IN (SELECT rowid FROM t1 LIMIT 2)",
			want:  [][]interface{}{{int64(2)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitZero tests LIMIT 0 edge case
func TestSQLiteLimitZero(t *testing.T) {
	t.Skip("LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t6(a)",
		"INSERT INTO t6 VALUES(1)",
		"INSERT INTO t6 VALUES(2)",
		"INSERT INTO t6 SELECT a+2 FROM t6",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "select all",
			query: "SELECT * FROM t6",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "limit -1 offset -1 (all rows)",
			query: "SELECT * FROM t6 LIMIT -1 OFFSET -1",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "limit 2 offset negative (ignore offset)",
			query: "SELECT * FROM t6 LIMIT 2 OFFSET -123",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name:  "limit negative offset 2",
			query: "SELECT * FROM t6 LIMIT -432 OFFSET 2",
			want:  [][]interface{}{{int64(3)}, {int64(4)}},
		},
		{
			name:  "limit -1 (all rows)",
			query: "SELECT * FROM t6 LIMIT -1",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "limit -1 offset 1",
			query: "SELECT * FROM t6 LIMIT -1 OFFSET 1",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "limit 0 (no rows)",
			query: "SELECT * FROM t6 LIMIT 0",
			want:  [][]interface{}{},
		},
		{
			name:  "limit 0 offset 1 (no rows)",
			query: "SELECT * FROM t6 LIMIT 0 OFFSET 1",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitUnion tests LIMIT with UNION operations
func TestSQLiteLimitUnion(t *testing.T) {
	t.Skip("LIMIT with UNION not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t2(x int)",
		"INSERT INTO t2 VALUES(31)",
		"INSERT INTO t2 VALUES(30)",
		"CREATE TABLE t6(a)",
		"INSERT INTO t6 VALUES(1)",
		"INSERT INTO t6 VALUES(2)",
		"INSERT INTO t6 VALUES(3)",
		"INSERT INTO t6 VALUES(4)",
	)

	tests := []struct {
		name    string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name:    "limit before union all (error)",
			query:   "SELECT x FROM t2 LIMIT 5 UNION ALL SELECT a FROM t6",
			wantErr: true,
		},
		{
			name:    "limit before union (error)",
			query:   "SELECT x FROM t2 LIMIT 5 UNION SELECT a FROM t6",
			wantErr: true,
		},
		{
			name:    "limit before except (error)",
			query:   "SELECT x FROM t2 LIMIT 5 EXCEPT SELECT a FROM t6 LIMIT 3",
			wantErr: true,
		},
		{
			name:    "limit before intersect (error)",
			query:   "SELECT x FROM t2 LIMIT 0,5 INTERSECT SELECT a FROM t6",
			wantErr: true,
		},
		{
			name:  "union all with limit",
			query: "SELECT x FROM t2 UNION ALL SELECT a FROM t6 LIMIT 5",
			want:  [][]interface{}{{int64(31)}, {int64(30)}, {int64(1)}, {int64(2)}, {int64(3)}},
		},
		{
			name:  "union all with limit and offset",
			query: "SELECT x FROM t2 UNION ALL SELECT a FROM t6 LIMIT 3 OFFSET 1",
			want:  [][]interface{}{{int64(30)}, {int64(1)}, {int64(2)}},
		},
		{
			name:  "union all with order by and limit",
			query: "SELECT x FROM t2 UNION ALL SELECT a FROM t6 ORDER BY 1 LIMIT 3 OFFSET 1",
			want:  [][]interface{}{{int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "union with limit",
			query: "SELECT x FROM t2 UNION SELECT x+2 FROM t2 LIMIT 2 OFFSET 1",
			want:  [][]interface{}{{int64(31)}, {int64(32)}},
		},
		{
			name:  "union with order by desc and limit",
			query: "SELECT x FROM t2 UNION SELECT x+2 FROM t2 ORDER BY 1 DESC LIMIT 2 OFFSET 1",
			want:  [][]interface{}{{int64(32)}, {int64(31)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				expectQueryError(t, db, tt.query)
			} else {
				got := queryRows(t, db, tt.query)
				compareRows(t, got, tt.want)
			}
		})
	}
}

// TestSQLiteLimitDistinct tests LIMIT with DISTINCT
func TestSQLiteLimitDistinct(t *testing.T) {
	t.Skip("LIMIT with DISTINCT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Create large table for distinct testing
	execSQL(t, db, "CREATE TABLE t3(x)")
	for i := 1; i <= 10; i++ {
		mustExec(t, db, "INSERT INTO t3 SELECT ? FROM t3", i)
	}
	// Start with initial value
	mustExec(t, db, "INSERT INTO t3 VALUES(1)")
	for i := 2; i <= 10; i++ {
		mustExec(t, db, "INSERT INTO t3 SELECT ? FROM (SELECT 1 UNION SELECT 2)", i)
	}

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "distinct with limit",
			query: "SELECT DISTINCT cast(round(x/100) as integer) FROM t3 LIMIT 5",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}},
		},
		{
			name:  "distinct with limit and offset",
			query: "SELECT DISTINCT cast(round(x/100) as integer) FROM t3 LIMIT 5 OFFSET 5",
			want:  [][]interface{}{{int64(5)}, {int64(6)}, {int64(7)}, {int64(8)}, {int64(9)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitGroupBy tests LIMIT with GROUP BY
func TestSQLiteLimitGroupBy(t *testing.T) {
	t.Skip("LIMIT with GROUP BY not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t5(x, y)",
	)

	// Insert test data from limit.test
	mustExec(t, db, "INSERT INTO t5 VALUES(5, 15)")
	mustExec(t, db, "INSERT INTO t5 VALUES(6, 16)")

	tests := []struct {
		name  string
		setup []string
		query string
		want  [][]interface{}
	}{
		{
			name:  "insert with limit and where",
			setup: []string{},
			query: "SELECT * FROM t5 ORDER BY x",
			want:  [][]interface{}{{int64(5), int64(15)}, {int64(6), int64(16)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				mustExec(t, db, stmt)
			}
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimitExpressions tests LIMIT with expressions
func TestSQLiteLimitExpressions(t *testing.T) {
	t.Skip("LIMIT expressions not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db, "CREATE TABLE t1(x int)")
	for i := 1; i <= 10; i++ {
		mustExec(t, db, "INSERT INTO t1 VALUES(?)", i)
	}

	tests := []struct {
		name    string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name:  "literal offset 0",
			query: "SELECT 123 LIMIT 1 OFFSET 0",
			want:  [][]interface{}{{int64(123)}},
		},
		{
			name:  "literal offset 1",
			query: "SELECT 123 LIMIT 1 OFFSET 1",
			want:  [][]interface{}{},
		},
		{
			name:  "literal limit 0 offset 0",
			query: "SELECT 123 LIMIT 0 OFFSET 0",
			want:  [][]interface{}{},
		},
		{
			name:  "literal limit 0 offset 1",
			query: "SELECT 123 LIMIT 0 OFFSET 1",
			want:  [][]interface{}{},
		},
		{
			name:  "literal limit -1 offset 0",
			query: "SELECT 123 LIMIT -1 OFFSET 0",
			want:  [][]interface{}{{int64(123)}},
		},
		{
			name:  "literal limit -1 offset 1",
			query: "SELECT 123 LIMIT -1 OFFSET 1",
			want:  [][]interface{}{},
		},
		{
			name:    "limit with function error",
			query:   "SELECT * FROM t1 LIMIT replace(1)",
			wantErr: true,
		},
		{
			name:    "offset with function error",
			query:   "SELECT * FROM t1 LIMIT 5 OFFSET replace(1)",
			wantErr: true,
		},
		{
			name:    "limit with column reference error",
			query:   "SELECT * FROM t1 LIMIT x",
			wantErr: true,
		},
		{
			name:    "offset with column reference error",
			query:   "SELECT * FROM t1 LIMIT 1 OFFSET x",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				expectQueryError(t, db, tt.query)
			} else {
				got := queryRows(t, db, tt.query)
				compareRows(t, got, tt.want)
			}
		})
	}
}

// TestSQLiteLimit2OrderByOptimization tests ORDER BY LIMIT optimizations from limit2.test
func TestSQLiteLimit2OrderByOptimization(t *testing.T) {
	t.Skip("ORDER BY LIMIT optimization not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	// Create test tables with indexes for optimization testing
	execSQL(t, db,
		"CREATE TABLE t1(a, b)",
	)

	// Insert 1000 rows with pattern
	for i := 1; i <= 1000; i++ {
		b := (i*17)%1000 + 1000
		mustExec(t, db, "INSERT INTO t1(a, b) VALUES(1, ?)", b)
	}

	// Insert additional specific rows
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(2, 2)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(3, 1006)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(4, 4)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(5, 9999)")

	execSQL(t, db, "CREATE INDEX t1ab ON t1(a, b)")

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "order by with limit using index",
			query: "SELECT a, b FROM t1 WHERE a IN (2,4,5,3,1) ORDER BY b LIMIT 5",
			want: [][]interface{}{
				{int64(2), int64(2)},
				{int64(4), int64(4)},
				{int64(1), int64(1000)},
				{int64(1), int64(1001)},
				{int64(1), int64(1002)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2DescendingOrder tests descending ORDER BY with LIMIT
func TestSQLiteLimit2DescendingOrder(t *testing.T) {
	t.Skip("ORDER BY DESC with LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t1(a, b)",
	)

	// Insert 1000 rows
	for i := 1; i <= 1000; i++ {
		b := (i*17)%1000 + 1000
		mustExec(t, db, "INSERT INTO t1(a, b) VALUES(1, ?)", b)
	}

	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(2, 2)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(3, 1006)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(4, 4)")
	mustExec(t, db, "INSERT INTO t1(a, b) VALUES(5, 9999)")

	execSQL(t, db, "CREATE INDEX t1ab ON t1(a, b DESC)")

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "order by desc with limit",
			query: "SELECT a, b FROM t1 WHERE a IN (2,4,5,3,1) ORDER BY b DESC LIMIT 5",
			want: [][]interface{}{
				{int64(5), int64(9999)},
				{int64(1), int64(1999)},
				{int64(1), int64(1998)},
				{int64(1), int64(1997)},
				{int64(1), int64(1996)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2JoinOrdering tests LIMIT with joins and ORDER BY
func TestSQLiteLimit2JoinOrdering(t *testing.T) {
	t.Skip("LIMIT with JOIN and ORDER BY not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t200(a, b)",
	)

	for i := 1; i <= 1000; i++ {
		mustExec(t, db, "INSERT INTO t200(a, b) VALUES(?, ?)", i, i)
	}

	execSQL(t, db,
		"CREATE TABLE t201(x INTEGER PRIMARY KEY, y)",
		"INSERT INTO t201(x, y) VALUES(2, 12345)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "join with limit",
			query: "SELECT * FROM t200, t201 WHERE x=b ORDER BY y LIMIT 3",
			want:  [][]interface{}{{int64(2), int64(2), int64(2), int64(12345)}},
		},
		{
			name:  "left join with limit",
			query: "SELECT * FROM t200 LEFT JOIN t201 ON x=b ORDER BY y LIMIT 3",
			want: [][]interface{}{
				{int64(1), int64(1), nil, nil},
				{int64(3), int64(3), nil, nil},
				{int64(4), int64(4), nil, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2ComplexConditions tests LIMIT with complex WHERE conditions
func TestSQLiteLimit2ComplexConditions(t *testing.T) {
	t.Skip("LIMIT with complex WHERE not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t300(a, b, c)",
		"CREATE INDEX t300x ON t300(a, b, c)",
		"INSERT INTO t300 VALUES(0, 1, 99)",
		"INSERT INTO t300 VALUES(0, 1, 0)",
		"INSERT INTO t300 VALUES(0, 0, 0)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "complex where with order by desc",
			query: "SELECT * FROM t300 WHERE a=0 AND (c=0 OR c=99) ORDER BY c DESC",
			want: [][]interface{}{
				{int64(0), int64(1), int64(99)},
				{int64(0), int64(0), int64(0)},
				{int64(0), int64(1), int64(0)},
			},
		},
		{
			name:  "complex where with order by desc and limit",
			query: "SELECT * FROM t300 WHERE a=0 AND (c=0 OR c=99) ORDER BY c DESC LIMIT 1",
			want:  [][]interface{}{{int64(0), int64(1), int64(99)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2OrderDirection tests correct loop direction for ORDER BY
func TestSQLiteLimit2OrderDirection(t *testing.T) {
	t.Skip("ORDER BY direction with LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t400(a, b)",
		"CREATE INDEX t400_ab ON t400(a, b)",
		"INSERT INTO t400(a, b) VALUES(1, 90)",
		"INSERT INTO t400(a, b) VALUES(1, 40)",
		"INSERT INTO t400(a, b) VALUES(2, 80)",
		"INSERT INTO t400(a, b) VALUES(2, 30)",
		"INSERT INTO t400(a, b) VALUES(3, 70)",
		"INSERT INTO t400(a, b) VALUES(3, 20)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "order by desc with limit (indexed)",
			query: "SELECT * FROM t400 WHERE a IN (1,2,3) ORDER BY b DESC LIMIT 3",
			want: [][]interface{}{
				{int64(1), int64(90)},
				{int64(2), int64(80)},
				{int64(3), int64(70)},
			},
		},
		{
			name:  "order by desc with limit (non-indexed)",
			query: "SELECT * FROM t400 WHERE a IN (1,2,3) ORDER BY +b DESC LIMIT 3",
			want: [][]interface{}{
				{int64(1), int64(90)},
				{int64(2), int64(80)},
				{int64(3), int64(70)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2IntegerPrimaryKey tests LIMIT with INTEGER PRIMARY KEY
func TestSQLiteLimit2IntegerPrimaryKey(t *testing.T) {
	t.Skip("LIMIT with INTEGER PRIMARY KEY not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t500(i INTEGER PRIMARY KEY, j)",
		"INSERT INTO t500 VALUES(1, 1)",
		"INSERT INTO t500 VALUES(2, 2)",
		"INSERT INTO t500 VALUES(3, 3)",
		"INSERT INTO t500 VALUES(4, 0)",
		"INSERT INTO t500 VALUES(5, 5)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "order by desc with integer primary key",
			query: "SELECT j FROM t500 WHERE i IN (1,2,3,4,5) ORDER BY j DESC LIMIT 3",
			want:  [][]interface{}{{int64(5)}, {int64(3)}, {int64(2)}},
		},
		{
			name:  "order by asc with integer primary key",
			query: "SELECT j FROM t500 WHERE i IN (1,2,3,4,5) ORDER BY j LIMIT 3",
			want:  [][]interface{}{{int64(0)}, {int64(1)}, {int64(2)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2OrderedJoin tests index usage for ordered joins
func TestSQLiteLimit2OrderedJoin(t *testing.T) {
	t.Skip("Ordered JOIN with LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t1(a, b)",
		"INSERT INTO t1 VALUES(1, 2)",
		"CREATE TABLE t2(x, y)",
		"INSERT INTO t2 VALUES(1, 3)",
		"CREATE INDEX t1ab ON t1(a, b)",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "join with condition and order by",
			query: "SELECT y FROM t1, t2 WHERE a=x AND b<=y ORDER BY b DESC",
			want:  [][]interface{}{{int64(3)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}

// TestSQLiteLimit2NestedViews tests LIMIT with nested UNION ALL views
func TestSQLiteLimit2NestedViews(t *testing.T) {
	t.Skip("Nested views with UNION ALL and LIMIT not yet fully implemented in internal driver")
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db,
		"CREATE TABLE t13(x)",
		"INSERT INTO t13 VALUES(1)",
		"INSERT INTO t13 VALUES(2)",
		"CREATE VIEW v13a AS SELECT x AS y FROM t13",
		"CREATE VIEW v13b AS SELECT y AS z FROM v13a UNION ALL SELECT y+10 FROM v13a",
		"CREATE VIEW v13c AS SELECT z FROM v13b UNION ALL SELECT z+20 FROM v13b",
	)

	tests := []struct {
		name  string
		query string
		want  [][]interface{}
	}{
		{
			name:  "nested view limit 1",
			query: "SELECT z FROM v13c LIMIT 1",
			want:  [][]interface{}{{int64(1)}},
		},
		{
			name:  "nested view limit 2",
			query: "SELECT z FROM v13c LIMIT 2",
			want:  [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name:  "nested view limit 4",
			query: "SELECT z FROM v13c LIMIT 4",
			want:  [][]interface{}{{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)}},
		},
		{
			name:  "nested view limit 8",
			query: "SELECT z FROM v13c LIMIT 8",
			want: [][]interface{}{
				{int64(1)}, {int64(2)}, {int64(11)}, {int64(12)},
				{int64(21)}, {int64(22)}, {int64(31)}, {int64(32)},
			},
		},
		{
			name:  "nested view limit 2 offset 2",
			query: "SELECT z FROM v13c LIMIT 2 OFFSET 2",
			want:  [][]interface{}{{int64(11)}, {int64(12)}},
		},
		{
			name:  "nested view limit 3 offset 3",
			query: "SELECT z FROM v13c LIMIT 3 OFFSET 3",
			want:  [][]interface{}{{int64(12)}, {int64(21)}, {int64(22)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queryRows(t, db, tt.query)
			compareRows(t, got, tt.want)
		})
	}
}
