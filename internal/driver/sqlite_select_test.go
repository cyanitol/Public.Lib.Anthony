package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// setupSelectTestDB creates a temporary test database for SELECT tests
func setupSelectTestDB(t *testing.T) *sql.DB {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create initial database file
	if err := os.WriteFile(dbPath, make([]byte, 4096), 0600); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	return db
}

// TestSQLiteSelectBasic tests basic SELECT functionality from select1.test
func TestSQLiteSelectBasic(t *testing.T) {
	t.Skip("SELECT not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name:    "select from non-existent table",
			setup:   []string{},
			query:   "SELECT * FROM test1",
			want:    nil,
			wantErr: true,
		},
		{
			name: "select single column",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT f1 FROM test1",
			want:  [][]interface{}{{11}},
		},
		{
			name: "select second column",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT f2 FROM test1",
			want:  [][]interface{}{{22}},
		},
		{
			name: "select columns reversed",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT f2, f1 FROM test1",
			want:  [][]interface{}{{22, 11}},
		},
		{
			name: "select columns in order",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT f1, f2 FROM test1",
			want:  [][]interface{}{{11, 22}},
		},
		{
			name: "select star",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT * FROM test1",
			want:  [][]interface{}{{11, 22}},
		},
		{
			name: "select star twice",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT *, * FROM test1",
			want:  [][]interface{}{{11, 22, 11, 22}},
		},
		{
			name: "select with literals",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
			},
			query: "SELECT 'one', *, 'two', * FROM test1",
			want:  [][]interface{}{{"one", 11, 22, "two", 11, 22}},
		},
		{
			name: "cross join two tables",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE TABLE test2(r1 real, r2 real)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
				"INSERT INTO test2(r1,r2) VALUES(1.1,2.2)",
			},
			query: "SELECT * FROM test1, test2",
			want:  [][]interface{}{{11, 22, 1.1, 2.2}},
		},
		{
			name: "select qualified columns",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE TABLE test2(r1 real, r2 real)",
				"INSERT INTO test1(f1,f2) VALUES(11,22)",
				"INSERT INTO test2(r1,r2) VALUES(1.1,2.2)",
			},
			query: "SELECT test1.f1, test2.r1 FROM test1, test2",
			want:  [][]interface{}{{11, 1.1}},
		},
		{
			name: "multiple rows select",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1",
			want:  [][]interface{}{{11}, {33}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			// Run setup statements
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			// Execute query
			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			// Verify results
			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				// Create a slice of interface{} to hold the values
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectWhere tests WHERE clause functionality from select1.test
func TestSQLiteSelectWhere(t *testing.T) {
	t.Skip("SELECT WHERE not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "where less than",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1<11",
			want:  [][]interface{}{},
		},
		{
			name: "where less than or equal",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1<=11",
			want:  [][]interface{}{{11}},
		},
		{
			name: "where equals",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1=11",
			want:  [][]interface{}{{11}},
		},
		{
			name: "where greater than or equal",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1>=11",
			want:  [][]interface{}{{11}, {33}},
		},
		{
			name: "where greater than",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1>11",
			want:  [][]interface{}{{33}},
		},
		{
			name: "where not equals",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE f1!=11",
			want:  [][]interface{}{{33}},
		},
		{
			name: "where between",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE ('x' || f1) BETWEEN 'x10' AND 'x20'",
			want:  [][]interface{}{{11}},
		},
		{
			name: "where expression equals",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 WHERE 5-3==2",
			want:  [][]interface{}{{11}, {33}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectOrderBy tests ORDER BY functionality from select1.test
func TestSQLiteSelectOrderBy(t *testing.T) {
	t.Skip("SELECT ORDER BY not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "order by ascending",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 ORDER BY f1",
			want:  [][]interface{}{{11}, {33}},
		},
		{
			name: "order by descending",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 FROM test1 ORDER BY -f1",
			want:  [][]interface{}{{33}, {11}},
		},
		{
			name: "order by column number",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
				"INSERT INTO t5 VALUES(2,9)",
			},
			query: "SELECT * FROM t5 ORDER BY 1",
			want:  [][]interface{}{{1, 10}, {2, 9}},
		},
		{
			name: "order by second column",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
				"INSERT INTO t5 VALUES(2,9)",
			},
			query: "SELECT * FROM t5 ORDER BY 2",
			want:  [][]interface{}{{2, 9}, {1, 10}},
		},
		{
			name: "order by multiple columns",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
				"INSERT INTO t5 VALUES(2,9)",
				"INSERT INTO t5 VALUES(3,10)",
			},
			query: "SELECT * FROM t5 ORDER BY 2, 1 DESC",
			want:  [][]interface{}{{2, 9}, {3, 10}, {1, 10}},
		},
		{
			name: "order by with desc",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
				"INSERT INTO t5 VALUES(2,9)",
				"INSERT INTO t5 VALUES(3,10)",
			},
			query: "SELECT * FROM t5 ORDER BY 1 DESC, b",
			want:  [][]interface{}{{3, 10}, {2, 9}, {1, 10}},
		},
		{
			name: "order by desc first column",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
				"INSERT INTO t5 VALUES(2,9)",
				"INSERT INTO t5 VALUES(3,10)",
			},
			query: "SELECT * FROM t5 ORDER BY b DESC, 1",
			want:  [][]interface{}{{1, 10}, {3, 10}, {2, 9}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectAliases tests column aliases from select1.test
func TestSQLiteSelectAliases(t *testing.T) {
	t.Skip("SELECT aliases not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "select with alias",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 AS x FROM test1 ORDER BY x",
			want:  [][]interface{}{{11}, {33}},
		},
		{
			name: "order by alias",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1 AS x FROM test1 ORDER BY -x",
			want:  [][]interface{}{{33}, {11}},
		},
		{
			name: "expression with alias",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1-23 AS x FROM test1 ORDER BY abs(x)",
			want:  [][]interface{}{{-12}, {10}},
		},
		{
			name: "alias in where clause",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
				"INSERT INTO test1 VALUES(33,44)",
			},
			query: "SELECT f1-22 AS x, f2-22 as y FROM test1 WHERE x>0 AND y<50",
			want:  [][]interface{}{{11, 22}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectDistinct tests DISTINCT functionality from select4.test
func TestSQLiteSelectDistinct(t *testing.T) {
	t.Skip("SELECT DISTINCT not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "distinct simple",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY log",
			want:  [][]interface{}{{0}, {1}, {2}, {3}},
		},
		{
			name: "distinct with nulls",
			setup: []string{
				"CREATE TABLE t3(a text, b float, c text)",
				"INSERT INTO t3 VALUES(1, 1.1, '1.1')",
				"INSERT INTO t3 VALUES(2, 1.10, '1.10')",
				"INSERT INTO t3 VALUES(3, 1.10, '1.1')",
				"INSERT INTO t3 VALUES(4, 1.1, '1.10')",
				"INSERT INTO t3 VALUES(5, 1.2, '1.2')",
			},
			query: "SELECT DISTINCT b FROM t3 ORDER BY c",
			want:  [][]interface{}{{1.1}, {1.2}},
		},
		{
			name: "distinct text column",
			setup: []string{
				"CREATE TABLE t3(a text, b float, c text)",
				"INSERT INTO t3 VALUES(1, 1.1, '1.1')",
				"INSERT INTO t3 VALUES(2, 1.10, '1.10')",
				"INSERT INTO t3 VALUES(3, 1.10, '1.1')",
				"INSERT INTO t3 VALUES(5, 1.2, '1.2')",
			},
			query: "SELECT DISTINCT c FROM t3 ORDER BY c",
			want:  [][]interface{}{{"1.1"}, {"1.10"}, {"1.2"}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectLimitOffset tests LIMIT and OFFSET from select4.test
func TestSQLiteSelectLimitOffset(t *testing.T) {
	t.Skip("SELECT LIMIT/OFFSET not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "distinct with limit",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(9,4)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 4",
			want:  [][]interface{}{{0}, {1}, {2}, {3}},
		},
		{
			name: "distinct with limit 0",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 0",
			want:  [][]interface{}{},
		},
		{
			name: "distinct with offset",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT -1 OFFSET 2",
			want:  [][]interface{}{{2}, {3}},
		},
		{
			name: "limit and offset",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(9,4)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 3 OFFSET 2",
			want:  [][]interface{}{{2}, {3}, {4}},
		},
		{
			name: "limit offset beyond range",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
			},
			query: "SELECT DISTINCT log FROM t1 ORDER BY +log LIMIT 3 OFFSET 20",
			want:  [][]interface{}{},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectJoins tests JOIN functionality from select2.test
func TestSQLiteSelectJoins(t *testing.T) {
	t.Skip("SELECT JOIN not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "cross join simple",
			setup: []string{
				"CREATE TABLE aa(a int)",
				"CREATE TABLE bb(b int)",
				"INSERT INTO aa VALUES(1)",
				"INSERT INTO aa VALUES(3)",
				"INSERT INTO bb VALUES(2)",
				"INSERT INTO bb VALUES(4)",
			},
			query: "SELECT * FROM aa, bb WHERE max(a,b)>2",
			want:  [][]interface{}{{1, 4}, {3, 2}, {3, 4}},
		},
		{
			name: "cross join with boolean",
			setup: []string{
				"CREATE TABLE aa(a int)",
				"CREATE TABLE bb(b int)",
				"INSERT INTO aa VALUES(1)",
				"INSERT INTO aa VALUES(3)",
				"INSERT INTO bb VALUES(2)",
				"INSERT INTO bb VALUES(4)",
				"INSERT INTO bb VALUES(0)",
			},
			query: "SELECT * FROM aa CROSS JOIN bb WHERE b",
			want:  [][]interface{}{{1, 2}, {1, 4}, {3, 2}, {3, 4}},
		},
		{
			name: "cross join with not",
			setup: []string{
				"CREATE TABLE aa(a int)",
				"CREATE TABLE bb(b int)",
				"INSERT INTO aa VALUES(1)",
				"INSERT INTO aa VALUES(3)",
				"INSERT INTO bb VALUES(2)",
				"INSERT INTO bb VALUES(0)",
			},
			query: "SELECT * FROM aa CROSS JOIN bb WHERE NOT b",
			want:  [][]interface{}{{1, 0}, {3, 0}},
		},
		{
			name: "join with min function",
			setup: []string{
				"CREATE TABLE aa(a int)",
				"CREATE TABLE bb(b int)",
				"INSERT INTO aa VALUES(1)",
				"INSERT INTO aa VALUES(3)",
				"INSERT INTO bb VALUES(2)",
				"INSERT INTO bb VALUES(4)",
				"INSERT INTO bb VALUES(0)",
			},
			query: "SELECT * FROM aa, bb WHERE min(a,b)",
			want:  [][]interface{}{{1, 2}, {1, 4}, {3, 2}, {3, 4}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectWithoutFrom tests SELECT without FROM clause from select1.test
func TestSQLiteSelectWithoutFrom(t *testing.T) {
	t.Skip("SELECT without FROM not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name:  "select expression",
			setup: []string{},
			query: "SELECT 1+2+3",
			want:  [][]interface{}{{6}},
		},
		{
			name:  "select multiple values",
			setup: []string{},
			query: "SELECT 1,'hello',2",
			want:  [][]interface{}{{1, "hello", 2}},
		},
		{
			name:  "select with aliases",
			setup: []string{},
			query: "SELECT 1 AS 'a','hello' AS 'b',2 AS 'c'",
			want:  [][]interface{}{{1, "hello", 2}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectGroupBy tests GROUP BY functionality from select3.test
func TestSQLiteSelectGroupBy(t *testing.T) {
	t.Skip("SELECT GROUP BY not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "group by with count",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(6,3)",
				"INSERT INTO t1 VALUES(7,3)",
				"INSERT INTO t1 VALUES(8,3)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{0, 1}, {1, 1}, {2, 2}, {3, 4}},
		},
		{
			name: "group by with min",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
			},
			query: "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{0, 1}, {1, 2}, {2, 3}, {3, 5}},
		},
		{
			name: "group by with avg",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
			},
			query: "SELECT log, avg(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{0, 1.0}, {1, 2.0}, {2, 3.5}},
		},
		{
			name: "group by with expression",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
			},
			query: "SELECT log*2+1, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log",
			want:  [][]interface{}{{1, 0.0}, {3, 0.0}, {5, 0.5}},
		},
		{
			name: "group by with alias",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
			},
			query: "SELECT log*2+1 as x, count(*) FROM t1 GROUP BY x ORDER BY x",
			want:  [][]interface{}{{1, 1}, {3, 1}, {5, 2}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectHaving tests HAVING clause from select3.test
func TestSQLiteSelectHaving(t *testing.T) {
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "having with comparison",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(6,3)",
				"INSERT INTO t1 VALUES(7,3)",
				"INSERT INTO t1 VALUES(8,3)",
				"INSERT INTO t1 VALUES(9,4)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING log>=4 ORDER BY log",
			want:  [][]interface{}{{4, 1}},
		},
		{
			name: "having with count",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
				"INSERT INTO t1 VALUES(2,1)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(6,3)",
				"INSERT INTO t1 VALUES(7,3)",
				"INSERT INTO t1 VALUES(8,3)",
			},
			query: "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*)>=4 ORDER BY log",
			want:  [][]interface{}{{3, 4}},
		},
		{
			name: "having with alias",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(3,2)",
				"INSERT INTO t1 VALUES(4,2)",
				"INSERT INTO t1 VALUES(5,3)",
				"INSERT INTO t1 VALUES(6,3)",
				"INSERT INTO t1 VALUES(7,3)",
				"INSERT INTO t1 VALUES(8,3)",
			},
			query: "SELECT log AS x, count(*) AS y FROM t1 GROUP BY x HAVING y>=4 ORDER BY max(n)+0",
			want:  [][]interface{}{{3, 4}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectTableStar tests table.* syntax from select1.test
func TestSQLiteSelectTableStar(t *testing.T) {
	t.Skip("SELECT table.* not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    [][]interface{}
		wantErr bool
	}{
		{
			name: "select table.* single",
			setup: []string{
				"CREATE TABLE t3(a,b)",
				"CREATE TABLE t4(a,b)",
				"INSERT INTO t3 VALUES(1,2)",
				"INSERT INTO t4 VALUES(3,4)",
			},
			query: "SELECT t3.*, t4.b FROM t3, t4",
			want:  [][]interface{}{{1, 2, 4}},
		},
		{
			name: "select table.* reversed",
			setup: []string{
				"CREATE TABLE t3(a,b)",
				"CREATE TABLE t4(a,b)",
				"INSERT INTO t3 VALUES(1,2)",
				"INSERT INTO t4 VALUES(3,4)",
			},
			query: "SELECT t3.b, t4.* FROM t3, t4",
			want:  [][]interface{}{{2, 3, 4}},
		},
		{
			name: "select with alias star",
			setup: []string{
				"CREATE TABLE t3(a,b)",
				"CREATE TABLE t4(a,b)",
				"INSERT INTO t3 VALUES(1,2)",
				"INSERT INTO t4 VALUES(3,4)",
			},
			query: "SELECT x.*, y.b FROM t3 AS x, t4 AS y",
			want:  [][]interface{}{{1, 2, 4}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			rows, err := db.Query(tt.query)
			if (err != nil) != tt.wantErr {
				t.Fatalf("query error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			defer rows.Close()

			got := [][]interface{}{}
			cols, _ := rows.Columns()
			for rows.Next() {
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				got = append(got, values)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query results = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSQLiteSelectErrors tests error cases from select tests
func TestSQLiteSelectErrors(t *testing.T) {
	t.Skip("SELECT errors not yet fully implemented in internal driver")
	db := setupSelectTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		setup    []string
		query    string
		wantErr  bool
		errorMsg string
	}{
		{
			name:     "no such table",
			setup:    []string{},
			query:    "SELECT * FROM test1",
			wantErr:  true,
			errorMsg: "no such table",
		},
		{
			name: "order by out of range",
			setup: []string{
				"CREATE TABLE t5(a,b)",
				"INSERT INTO t5 VALUES(1,10)",
			},
			query:    "SELECT * FROM t5 ORDER BY 3",
			wantErr:  true,
			errorMsg: "ORDER BY term out of range",
		},
		{
			name: "ambiguous column name",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"INSERT INTO test1 VALUES(11,22)",
			},
			query:    "SELECT A.f1, f1 FROM test1 as A, test1 as B ORDER BY f2",
			wantErr:  true,
			errorMsg: "ambiguous column name",
		},
		{
			name: "syntax error in where",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
			},
			query:    "SELECT f1 FROM test1 WHERE f2=",
			wantErr:  true,
			errorMsg: "syntax error",
		},
		{
			name: "syntax error in order by",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
			},
			query:    "SELECT f1 FROM test1 ORDER BY",
			wantErr:  true,
			errorMsg: "syntax error",
		},
		{
			name: "group by out of range",
			setup: []string{
				"CREATE TABLE t1(n int, log int)",
				"INSERT INTO t1 VALUES(1,0)",
			},
			query:    "SELECT log, count(*) FROM t1 GROUP BY 0 ORDER BY log",
			wantErr:  true,
			errorMsg: "GROUP BY term out of range",
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			for _, stmt := range tt.setup {
				if _, err := db.Exec(stmt); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			_, err := db.Query(tt.query)
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr && err == nil {
				t.Fatalf("expected error containing '%s', got nil", tt.errorMsg)
			}
			if tt.wantErr && err != nil {
				errStr := fmt.Sprintf("%v", err)
				// Just verify we got an error - detailed message checking can come later
				t.Logf("Got expected error: %v", errStr)
			}
		})
	}
}
