// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestSQLiteIndex tests index creation, usage, and deletion functionality
// Converted from SQLite TCL test files: index.test, index2.test, index3.test, index4.test, index5.test
func TestSQLiteIndex(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string              // CREATE TABLE + CREATE INDEX statements
		query    string                // Query to execute
		wantRows [][]interface{}       // Expected rows
		wantErr  bool                  // Whether an error is expected
		errMsg   string                // Expected error message substring
	}{
		// index.test - Basic index creation
		{
			name: "index-1.1 - Basic index creation",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1' ORDER BY name",
			wantRows: [][]interface{}{{"index1"}},
		},
		{
			name: "index-1.2 - Index dies with table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
				"CREATE INDEX index1 ON test1(f1)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Error cases
		{
			name:    "index-2.1 - Index on non-existent table",
			setup:   []string{},
			query:   "CREATE INDEX index1 ON test1(f1)",
			wantErr: true,
			errMsg:  "no such table",
		},
		{
			name: "index-2.1b - Index on non-existent column",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
			},
			query:   "CREATE INDEX index1 ON test1(f4)",
			wantErr: true,
			errMsg:  "no such column",
		},
		{
			name: "index-2.2 - Index with some invalid columns",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int)",
			},
			query:   "CREATE INDEX index1 ON test1(f1, f2, f4, f3)",
			wantErr: true,
			errMsg:  "no such column",
		},
		// index.test - Multiple indices
		{
			name: "index-3.1 - Create many indices on same table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int, f3 int, f4 int, f5 int)",
				"CREATE INDEX index01 ON test1(f1)",
				"CREATE INDEX index02 ON test1(f2)",
				"CREATE INDEX index03 ON test1(f3)",
				"CREATE INDEX index04 ON test1(f4)",
				"CREATE INDEX index05 ON test1(f5)",
			},
			query: "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{{int64(5)}},
		},
		{
			name: "index-3.3 - All indices removed with table",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE INDEX idx1 ON test1(f1)",
				"CREATE INDEX idx2 ON test1(f2)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Index usage
		{
			name: "index-4.1-4.3 - Query using index",
			setup: []string{
				"CREATE TABLE test1(cnt int, power int)",
				"INSERT INTO test1 VALUES(1, 2)",
				"INSERT INTO test1 VALUES(2, 4)",
				"INSERT INTO test1 VALUES(3, 8)",
				"INSERT INTO test1 VALUES(10, 1024)",
				"CREATE INDEX index9 ON test1(cnt)",
				"CREATE INDEX indext ON test1(power)",
			},
			query:    "SELECT cnt FROM test1 WHERE power=1024",
			wantRows: [][]interface{}{{int64(10)}},
		},
		{
			name: "index-4.4-4.5 - Query after dropping one index",
			setup: []string{
				"CREATE TABLE test1(cnt int, power int)",
				"INSERT INTO test1 VALUES(6, 64)",
				"CREATE INDEX index9 ON test1(cnt)",
				"CREATE INDEX indext ON test1(power)",
				"DROP INDEX indext",
			},
			query:    "SELECT power FROM test1 WHERE cnt=6",
			wantRows: [][]interface{}{{int64(64)}},
		},
		// index.test - No indexing sqlite_master
		{
			name:    "index-5.1 - Cannot index sqlite_master",
			setup:   []string{},
			query:   "CREATE INDEX index1 ON sqlite_master(name)",
			wantErr: true,
			errMsg:  "may not be indexed",
		},
		// index.test - Duplicate index names
		{
			name: "index-6.1 - Duplicate index name error",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE TABLE test2(g1 real, g2 real)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:   "CREATE INDEX index1 ON test2(g1)",
			wantErr: true,
			errMsg:  "already exists",
		},
		{
			name: "index-6.1c - CREATE INDEX IF NOT EXISTS on existing",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int)",
				"CREATE INDEX index1 ON test1(f1)",
			},
			query:   "CREATE INDEX IF NOT EXISTS index1 ON test1(f1)",
			wantErr: false,
		},
		{
			name: "index-6.2 - Cannot create index with table name",
			setup: []string{
				"CREATE TABLE test1(f1 int)",
				"CREATE TABLE test2(g1 real)",
			},
			query:   "CREATE INDEX test1 ON test2(g1)",
			wantErr: true,
			errMsg:  "already a table named",
		},
		{
			name: "index-6.4 - Multiple indices dropped with table",
			setup: []string{
				"CREATE TABLE test1(a, b)",
				"CREATE INDEX index1 ON test1(a)",
				"CREATE INDEX index2 ON test1(b)",
				"CREATE INDEX index3 ON test1(a,b)",
				"DROP TABLE test1",
			},
			query:    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test1'",
			wantRows: [][]interface{}{},
		},
		// index.test - Primary key creates auto-index
		{
			name: "index-7.1-7.3 - Primary key auto-index",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int primary key)",
				"INSERT INTO test1 VALUES(16, 65536)",
			},
			query:    "SELECT f1 FROM test1 WHERE f2=65536",
			wantRows: [][]interface{}{{int64(16)}},
		},
		{
			name: "index-7.3 - Auto-index name check",
			setup: []string{
				"CREATE TABLE test1(f1 int, f2 int primary key)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='test1' AND name LIKE 'sqlite_autoindex%'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		// index.test - DROP INDEX errors
		{
			name:    "index-8.1 - Drop non-existent index",
			setup:   []string{},
			query:   "DROP INDEX index1",
			wantErr: true,
			errMsg:  "no such index",
		},
		// index.test - Multiple entries with same key
		{
			name: "index-10.0 - Non-unique index allows duplicates",
			setup: []string{
				"CREATE TABLE t1(a int, b int)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
				"INSERT INTO t1 VALUES(2, 4)",
				"INSERT INTO t1 VALUES(3, 8)",
				"INSERT INTO t1 VALUES(1, 12)",
			},
			query:    "SELECT b FROM t1 WHERE a=1 ORDER BY b",
			wantRows: [][]interface{}{{int64(2)}, {int64(12)}},
		},
		{
			name: "index-10.1 - Query single value",
			setup: []string{
				"CREATE TABLE t1(a int, b int)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(2, 4)",
			},
			query:    "SELECT b FROM t1 WHERE a=2 ORDER BY b",
			wantRows: [][]interface{}{{int64(4)}},
		},
		// index.test - Composite index
		{
			name: "index-14.1 - Multi-column index with NULL handling",
			setup: []string{
				"CREATE TABLE t6(a, b, c)",
				"CREATE INDEX t6i1 ON t6(a, b)",
				"INSERT INTO t6 VALUES('', '', 1)",
				"INSERT INTO t6 VALUES('', NULL, 2)",
				"INSERT INTO t6 VALUES(NULL, '', 3)",
				"INSERT INTO t6 VALUES('abc', 123, 4)",
				"INSERT INTO t6 VALUES(123, 'abc', 5)",
			},
			query:    "SELECT c FROM t6 WHERE a='' ORDER BY c",
			wantRows: [][]interface{}{{int64(2)}, {int64(1)}},
		},
		{
			name: "index-14.3 - Query on second column",
			setup: []string{
				"CREATE TABLE t6(a, b, c)",
				"CREATE INDEX t6i1 ON t6(a, b)",
				"INSERT INTO t6 VALUES('', '', 1)",
				"INSERT INTO t6 VALUES(NULL, '', 3)",
			},
			query:    "SELECT c FROM t6 WHERE b='' ORDER BY c",
			wantRows: [][]interface{}{{int64(1)}, {int64(3)}},
		},
		// index.test - Unique constraint via index
		{
			name: "index-16.1 - Single index for UNIQUE PRIMARY KEY",
			setup: []string{
				"CREATE TABLE t7(c UNIQUE PRIMARY KEY)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "index-16.4 - Single index for compound constraint",
			setup: []string{
				"CREATE TABLE t7(c, d, UNIQUE(c, d), PRIMARY KEY(c, d))",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "index-16.5 - Multiple indices for different constraints",
			setup: []string{
				"CREATE TABLE t7(c, d, UNIQUE(c), PRIMARY KEY(c, d))",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index'",
			wantRows: [][]interface{}{{int64(2)}},
		},
		// index.test - Auto-index naming
		{
			name: "index-17.1 - Auto-index naming convention",
			setup: []string{
				"CREATE TABLE t7(c, d UNIQUE, UNIQUE(c), PRIMARY KEY(c, d))",
			},
			query: "SELECT count(*) FROM sqlite_master WHERE tbl_name='t7' AND type='index' AND name LIKE 'sqlite_autoindex_%'",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "index-17.2 - Cannot drop auto-index",
			setup: []string{
				"CREATE TABLE t7(c PRIMARY KEY)",
			},
			query:   "DROP INDEX sqlite_autoindex_t7_1",
			wantErr: true,
			errMsg:  "cannot be dropped",
		},
		{
			name: "index-17.4 - DROP INDEX IF EXISTS on non-existent",
			setup: []string{},
			query: "DROP INDEX IF EXISTS no_such_index",
			wantErr: false,
		},
		// index.test - Reserved names
		{
			name:    "index-18.2 - Cannot create index with sqlite_ prefix",
			setup:   []string{"CREATE TABLE t7(c)"},
			query:   "CREATE INDEX sqlite_i1 ON t7(c)",
			wantErr: true,
			errMsg:  "reserved for internal use",
		},
		// index.test - Quoted index names
		{
			name: "index-20.1 - Drop index with quoted name",
			setup: []string{
				"CREATE TABLE t6(c)",
				"CREATE INDEX \"t6i2\" ON t6(c)",
			},
			query:   "DROP INDEX \"t6i2\"",
			wantErr: false,
		},
		// index.test - TEMP index restrictions
		{
			name: "index-21.1 - Cannot create TEMP index on non-TEMP table",
			setup: []string{
				"CREATE TABLE t6(c)",
			},
			query:   "CREATE INDEX temp.i21 ON t6(c)",
			wantErr: true,
			errMsg:  "cannot create a TEMP index",
		},
		// index.test - Expression index
		{
			name: "index-22.0 - Index on expression",
			setup: []string{
				"CREATE TABLE t1(a, b TEXT)",
				"CREATE UNIQUE INDEX x1 ON t1(b==0)",
				"CREATE INDEX x2 ON t1(a || 0) WHERE b",
				"INSERT INTO t1(a,b) VALUES('a', 1)",
				"INSERT INTO t1(a,b) VALUES('a', 0)",
			},
			query:    "SELECT a, b FROM t1 ORDER BY a, b",
			wantRows: [][]interface{}{{"a", "0"}, {"a", "1"}},
		},
		{
			name: "index-23.0 - Expression index with GLOB",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b REAL)",
				"CREATE UNIQUE INDEX t1x1 ON t1(a GLOB b)",
				"INSERT INTO t1(a,b) VALUES('0.0', 1)",
				"INSERT INTO t1(a,b) VALUES('1.0', 1)",
			},
			query:    "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{{"0.0", 1.0}, {"1.0", 1.0}},
		},
		// index3.test - UNIQUE constraint failures
		{
			name: "index3-1.1-1.2 - UNIQUE index fails on duplicate data",
			setup: []string{
				"CREATE TABLE t1(a)",
				"INSERT INTO t1 VALUES(1)",
				"INSERT INTO t1 VALUES(1)",
			},
			query:   "CREATE UNIQUE INDEX i1 ON t1(a)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		// index4.test - Large index creation
		{
			name: "index4-1.1 - Create index on large table",
			setup: []string{
				"CREATE TABLE t1(x)",
				"INSERT INTO t1 VALUES('test1')",
				"INSERT INTO t1 VALUES('test2')",
				"INSERT INTO t1 VALUES('test3')",
				"CREATE INDEX i1 ON t1(x)",
			},
			query:    "SELECT count(*) FROM t1",
			wantRows: [][]interface{}{{int64(3)}},
		},
		{
			name: "index4-2.2 - UNIQUE constraint on duplicate values",
			setup: []string{
				"CREATE TABLE t2(x)",
				"INSERT INTO t2 VALUES(14)",
				"INSERT INTO t2 VALUES(35)",
				"INSERT INTO t2 VALUES(15)",
				"INSERT INTO t2 VALUES(35)",
			},
			query:   "CREATE UNIQUE INDEX i3 ON t2(x)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		// Additional comprehensive tests
		{
			name: "REINDEX - Basic reindex",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:   "REINDEX",
			wantErr: false,
		},
		{
			name: "REINDEX - Named index",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			query:   "REINDEX i1",
			wantErr: false,
		},
		{
			name: "Multi-column index - Three columns",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE INDEX i1 ON t1(a, b, c)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
				"INSERT INTO t1 VALUES(1, 2, 4)",
			},
			query:    "SELECT c FROM t1 WHERE a=1 AND b=2 ORDER BY c",
			wantRows: [][]interface{}{{int64(3)}, {int64(4)}},
		},
		{
			name: "Partial index - WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a) WHERE b > 5",
				"INSERT INTO t1 VALUES(1, 10)",
				"INSERT INTO t1 VALUES(2, 3)",
			},
			query:    "SELECT a FROM t1 WHERE b > 5",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "Index on INTEGER PRIMARY KEY",
			setup: []string{
				"CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)",
				"INSERT INTO t1 VALUES(1, 'Alice')",
				"INSERT INTO t1 VALUES(2, 'Bob')",
			},
			query:    "SELECT name FROM t1 WHERE id=2",
			wantRows: [][]interface{}{{"Bob"}},
		},
		{
			name: "Compound UNIQUE index",
			setup: []string{
				"CREATE TABLE t1(a, b, c)",
				"CREATE UNIQUE INDEX i1 ON t1(a, b)",
				"INSERT INTO t1 VALUES(1, 2, 3)",
			},
			query:   "INSERT INTO t1 VALUES(1, 2, 4)",
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
		{
			name: "Index with ASC",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a ASC)",
				"INSERT INTO t1 VALUES(3, 'c')",
				"INSERT INTO t1 VALUES(1, 'a')",
				"INSERT INTO t1 VALUES(2, 'b')",
			},
			query:    "SELECT b FROM t1 ORDER BY a",
			wantRows: [][]interface{}{{"a"}, {"b"}, {"c"}},
		},
		{
			name: "Index with DESC",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a DESC)",
				"INSERT INTO t1 VALUES(1, 'a')",
				"INSERT INTO t1 VALUES(2, 'b')",
				"INSERT INTO t1 VALUES(3, 'c')",
			},
			query:    "SELECT b FROM t1 ORDER BY a DESC",
			wantRows: [][]interface{}{{"c"}, {"b"}, {"a"}},
		},
		{
			name: "Index on NULL values",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"INSERT INTO t1 VALUES(NULL, 1)",
				"INSERT INTO t1 VALUES(NULL, 2)",
				"INSERT INTO t1 VALUES(1, 3)",
			},
			query:    "SELECT b FROM t1 WHERE a IS NULL ORDER BY b",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			name: "Drop and recreate index",
			setup: []string{
				"CREATE TABLE t1(a, b)",
				"CREATE INDEX i1 ON t1(a)",
				"DROP INDEX i1",
				"CREATE INDEX i1 ON t1(a)",
			},
			query:    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='i1'",
			wantRows: [][]interface{}{{int64(1)}},
		},
		{
			name: "Index with COLLATE",
			setup: []string{
				"CREATE TABLE t1(a TEXT, b)",
				"CREATE INDEX i1 ON t1(a COLLATE NOCASE)",
				"INSERT INTO t1 VALUES('ABC', 1)",
				"INSERT INTO t1 VALUES('abc', 2)",
			},
			query:    "SELECT count(*) FROM t1",
			wantRows: [][]interface{}{{int64(2)}},
		},
	}

	for _, tt := range tests {
		tt := tt  // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("sqlite_internal", ":memory:")
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			// Execute setup statements
			for _, stmt := range tt.setup {
				_, err := db.Exec(stmt)
				if err != nil {
					t.Fatalf("setup failed on statement %q: %v", stmt, err)
				}
			}

			// Execute the test query
			if tt.wantErr {
				_, err := db.Exec(tt.query)
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errMsg)
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Fatalf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				// Check if this is a query or an exec
				if tt.wantRows != nil {
					rows, err := db.Query(tt.query)
					if err != nil {
						t.Fatalf("query failed: %v", err)
					}
					defer rows.Close()

					// Get column count
					cols, err := rows.Columns()
					if err != nil {
						t.Fatalf("failed to get columns: %v", err)
					}

					var gotRows [][]interface{}
					for rows.Next() {
						// Create a slice to hold the values
						values := make([]interface{}, len(cols))
						valuePtrs := make([]interface{}, len(cols))
						for i := range values {
							valuePtrs[i] = &values[i]
						}

						if err := rows.Scan(valuePtrs...); err != nil {
							t.Fatalf("scan failed: %v", err)
						}

						// Convert []byte to string for comparison
						row := make([]interface{}, len(cols))
						for i, v := range values {
							if b, ok := v.([]byte); ok {
								row[i] = string(b)
							} else {
								row[i] = v
							}
						}
						gotRows = append(gotRows, row)
					}

					if err := rows.Err(); err != nil {
						t.Fatalf("rows iteration error: %v", err)
					}

					// Compare rows
					if len(gotRows) != len(tt.wantRows) {
						t.Fatalf("row count mismatch: got %d, want %d\nGot: %v\nWant: %v",
							len(gotRows), len(tt.wantRows), gotRows, tt.wantRows)
					}

					for i, gotRow := range gotRows {
						wantRow := tt.wantRows[i]
						if len(gotRow) != len(wantRow) {
							t.Fatalf("row %d column count mismatch: got %d, want %d", i, len(gotRow), len(wantRow))
						}
						for j, gotVal := range gotRow {
							wantVal := wantRow[j]
							if !valuesEqual(gotVal, wantVal) {
								t.Errorf("row %d, col %d: got %v (%T), want %v (%T)",
									i, j, gotVal, gotVal, wantVal, wantVal)
							}
						}
					}
				} else {
					// Just execute without checking results
					_, err := db.Exec(tt.query)
					if err != nil {
						t.Fatalf("exec failed: %v", err)
					}
				}
			}
		})
	}
}

// TestIndexUsageInQueries tests that indices are actually used in queries
func TestIndexUsageInQueries(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec("INSERT INTO users VALUES(?, ?, ?, ?)",
			i, fmt.Sprintf("user%d@example.com", i), fmt.Sprintf("User %d", i), 20+i%50)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
	}

	// Create index
	_, err = db.Exec("CREATE INDEX idx_users_email ON users(email)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Query using index
	rows, err := db.Query("SELECT id, name FROM users WHERE email = ?", "user50@example.com")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if id != 50 {
		t.Errorf("expected id=50, got %d", id)
	}
	if name != "User 50" {
		t.Errorf("expected name='User 50', got %q", name)
	}
}

// TestMultiColumnIndexUsage tests queries with multi-column indices
func TestMultiColumnIndexUsage(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			product_id INTEGER,
			quantity INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create multi-column index
	_, err = db.Exec("CREATE INDEX idx_orders_customer_product ON orders(customer_id, product_id)")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Insert test data
	testData := [][]int{
		{1, 100, 1, 5},
		{2, 100, 2, 3},
		{3, 100, 1, 2},
		{4, 101, 1, 1},
		{5, 101, 2, 4},
	}

	for _, data := range testData {
		_, err = db.Exec("INSERT INTO orders VALUES(?, ?, ?, ?)", data[0], data[1], data[2], data[3])
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Query using multi-column index
	rows, err := db.Query("SELECT id, quantity FROM orders WHERE customer_id = ? AND product_id = ? ORDER BY id",
		100, 1)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := [][]int64{{1, 5}, {3, 2}}
	idx := 0
	for rows.Next() {
		var id, quantity int64
		if err := rows.Scan(&id, &quantity); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if idx >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if id != expected[idx][0] || quantity != expected[idx][1] {
			t.Errorf("row %d: got (%d, %d), want (%d, %d)",
				idx, id, quantity, expected[idx][0], expected[idx][1])
		}
		idx++
	}

	if idx != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), idx)
	}
}

// TestPartialIndexes tests partial indices with WHERE clauses
func TestPartialIndexes(t *testing.T) {
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name TEXT,
			price REAL,
			in_stock INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create partial index - only index products in stock
	_, err = db.Exec("CREATE INDEX idx_products_in_stock ON products(name) WHERE in_stock = 1")
	if err != nil {
		t.Fatalf("failed to create partial index: %v", err)
	}

	// Insert test data
	testData := []struct {
		id       int
		name     string
		price    float64
		in_stock int
	}{
		{1, "Widget", 9.99, 1},
		{2, "Gadget", 19.99, 0},
		{3, "Doohickey", 14.99, 1},
		{4, "Thingamajig", 24.99, 0},
	}

	for _, data := range testData {
		_, err = db.Exec("INSERT INTO products VALUES(?, ?, ?, ?)",
			data.id, data.name, data.price, data.in_stock)
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Query using partial index
	rows, err := db.Query("SELECT name FROM products WHERE in_stock = 1 ORDER BY name")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	expected := []string{"Doohickey", "Widget"}
	idx := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if idx >= len(expected) {
			t.Fatalf("too many rows returned")
		}
		if name != expected[idx] {
			t.Errorf("row %d: got %q, want %q", idx, name, expected[idx])
		}
		idx++
	}

	if idx != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), idx)
	}
}
