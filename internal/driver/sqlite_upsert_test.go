package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLiteUpsert tests UPSERT operations including INSERT ON CONFLICT, DO UPDATE, and DO NOTHING
func TestSQLiteUpsert(t *testing.T) {
	tests := []struct {
		name     string
		setup    []string
		query    string
		wantRows [][]interface{}
		wantErr  bool
		errMsg   string
	}{
		// Basic UPSERT tests (from upsert1.test)
		{
			name: "upsert1-100 DO NOTHING on primary key conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
				"CREATE UNIQUE INDEX t1x1 ON t1(b)",
				"INSERT INTO t1(a,b) VALUES(1,2) ON CONFLICT DO NOTHING",
				"INSERT INTO t1(a,b) VALUES(1,99),(99,2) ON CONFLICT DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(1), "2", int64(0)},
			},
		},
		{
			name: "upsert1-101 DO NOTHING with explicit conflict target",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
				"INSERT INTO t1(a,b) VALUES(2,3) ON CONFLICT(a) DO NOTHING",
				"INSERT INTO t1(a,b) VALUES(2,99) ON CONFLICT(a) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(2), "3", int64(0)},
			},
		},
		{
			name: "upsert1-102 DO NOTHING on unique index conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
				"CREATE UNIQUE INDEX t1x1 ON t1(b)",
				"INSERT INTO t1(a,b) VALUES(3,4) ON CONFLICT(b) DO NOTHING",
				"INSERT INTO t1(a,b) VALUES(99,4) ON CONFLICT(b) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(3), "4", int64(0)},
			},
		},
		{
			name: "upsert1-110 error on non-existent column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
			},
			query:   "INSERT INTO t1(a,b) VALUES(5,6) ON CONFLICT(x) DO NOTHING",
			wantErr: true,
			errMsg:  "no such column",
		},
		{
			name: "upsert1-120 error on non-unique column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
			},
			query:   "INSERT INTO t1(a,b) VALUES(5,6) ON CONFLICT(c) DO NOTHING",
			wantErr: true,
			errMsg:  "does not match any PRIMARY KEY or UNIQUE constraint",
		},
		{
			name: "upsert1-140 DO NOTHING with collation",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c DEFAULT 0)",
				"CREATE UNIQUE INDEX t1x1 ON t1(b COLLATE binary)",
				"INSERT INTO t1(a,b) VALUES(5,6) ON CONFLICT(b COLLATE binary) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(5), "6", int64(0)},
			},
		},
		{
			name: "upsert1-200 DO NOTHING on expression index",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)",
				"CREATE UNIQUE INDEX t1x1 ON t1(a+b)",
				"INSERT INTO t1(a,b) VALUES(7,8) ON CONFLICT(a+b) DO NOTHING",
				"INSERT INTO t1(a,b) VALUES(8,7),(9,6) ON CONFLICT(a+b) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(7), int64(8), int64(0)},
			},
		},
		{
			name: "upsert1-320 DO NOTHING with partial index WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)",
				"CREATE UNIQUE INDEX t1x1 ON t1(b) WHERE b>10",
				"INSERT INTO t1(a,b) VALUES(1,2),(3,2),(4,20),(5,20) ON CONFLICT(b) WHERE b>10 DO NOTHING",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(0)},
				{int64(3), int64(2), int64(0)},
				{int64(4), int64(20), int64(0)},
			},
		},
		{
			name: "upsert1-500 DO UPDATE with expression",
			setup: []string{
				"CREATE TABLE t1(x INTEGER PRIMARY KEY, y INT UNIQUE)",
				"INSERT INTO t1(x,y) SELECT 1,2 WHERE true ON CONFLICT(x) DO UPDATE SET y=max(t1.y,excluded.y) AND true",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(2)},
			},
		},
		{
			name: "upsert1-700 DO UPDATE on specific constraint",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c INT, d INT, e INT)",
				"CREATE UNIQUE INDEX t1b ON t1(b)",
				"CREATE UNIQUE INDEX t1e ON t1(e)",
				"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,3,4,5)",
				"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,33,44,5) ON CONFLICT(e) DO UPDATE SET c=excluded.c",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(33), int64(4), int64(5)},
			},
		},
		{
			name: "upsert1-710 DO UPDATE on primary key conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c INT, d INT, e INT)",
				"CREATE UNIQUE INDEX t1b ON t1(b)",
				"CREATE UNIQUE INDEX t1e ON t1(e)",
				"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,3,4,5)",
				"INSERT INTO t1(a,b,c,d,e) VALUES(1,2,33,44,5) ON CONFLICT(a) DO UPDATE SET c=excluded.c",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(33), int64(4), int64(5)},
			},
		},
		// Tests from upsert2.test
		{
			name: "upsert2-100 DO UPDATE with WHERE clause",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b int, c DEFAULT 0)",
				"INSERT INTO t1(a,b) VALUES(1,2),(3,4)",
				"INSERT INTO t1(a,b) VALUES(1,8),(2,11),(3,1) ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=c+1 WHERE t1.b<excluded.b",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(8), int64(1)},
				{int64(2), int64(11), int64(0)},
				{int64(3), int64(4), int64(0)},
			},
		},
		{
			name: "upsert2-110 DO UPDATE without rowid",
			setup: []string{
				"CREATE TABLE t1(a INT PRIMARY KEY, b int, c DEFAULT 0) WITHOUT ROWID",
				"INSERT INTO t1(a,b) VALUES(1,2),(3,4)",
				"INSERT INTO t1(a,b) VALUES(1,8),(2,11),(3,1) ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=c+1 WHERE t1.b<excluded.b",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(8), int64(1)},
				{int64(2), int64(11), int64(0)},
				{int64(3), int64(4), int64(0)},
			},
		},
		{
			name: "upsert2-200 DO UPDATE with CTE",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b int, c DEFAULT 0)",
				"INSERT INTO t1(a,b) VALUES(1,2),(3,4)",
				`WITH nx(a,b) AS (VALUES(1,8),(2,11),(3,1),(2,15),(1,4),(1,99))
				INSERT INTO t1(a,b) SELECT a, b FROM nx WHERE true
				ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=c+1 WHERE t1.b<excluded.b`,
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(99), int64(2)},
				{int64(2), int64(15), int64(1)},
				{int64(3), int64(4), int64(0)},
			},
		},
		{
			name: "upsert2-210 DO UPDATE with table alias",
			setup: []string{
				"CREATE TABLE t1(a INT PRIMARY KEY, b int, c DEFAULT 0) WITHOUT ROWID",
				"INSERT INTO t1(a,b) VALUES(1,2),(3,4)",
				`WITH nx(a,b) AS (VALUES(1,8),(2,11),(3,1),(2,15),(1,4),(1,99))
				INSERT INTO t1(a,b) SELECT a, b FROM nx WHERE true
				ON CONFLICT(a) DO UPDATE SET b=excluded.b, c=c+1 WHERE t1.b<excluded.b`,
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(99), int64(2)},
				{int64(2), int64(15), int64(1)},
				{int64(3), int64(4), int64(0)},
			},
		},
		// Tests from upsert3.test
		{
			name: "upsert3-110 error on partial constraint match",
			setup: []string{
				"CREATE TABLE t1(k int, v text)",
				"CREATE UNIQUE INDEX x1 ON t1(k, v)",
			},
			query:   "INSERT INTO t1 VALUES(0,'abcdefghij') ON CONFLICT(k) DO NOTHING",
			wantErr: true,
			errMsg:  "does not match any PRIMARY KEY or UNIQUE constraint",
		},
		{
			name: "upsert3-130 DO NOTHING with composite index",
			setup: []string{
				"CREATE TABLE t1(k int, v text)",
				"CREATE UNIQUE INDEX x1 ON t1(k, v)",
				"INSERT INTO t1 VALUES(0, 'abcdefghij') ON CONFLICT(k,v) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(0), "abcdefghij"},
			},
		},
		{
			name: "upsert3-140 DO NOTHING with reversed column order",
			setup: []string{
				"CREATE TABLE t1(k int, v text)",
				"CREATE UNIQUE INDEX x1 ON t1(k, v)",
				"INSERT INTO t1 VALUES(0, 'abcdefghij') ON CONFLICT(k,v) DO NOTHING",
				"INSERT INTO t1 VALUES(0, 'abcdefghij') ON CONFLICT(v,k) DO NOTHING",
			},
			query: "SELECT * FROM t1",
			wantRows: [][]interface{}{
				{int64(0), "abcdefghij"},
			},
		},
		{
			name: "upsert3-200 DO UPDATE with excluded table reference",
			setup: []string{
				"CREATE TABLE excluded(a INT, b INT, c INT DEFAULT 0)",
				"CREATE UNIQUE INDEX excludedab ON excluded(a,b)",
				"INSERT INTO excluded(a,b) VALUES(1,2),(1,2),(3,4),(1,2),(5,6),(3,4) ON CONFLICT(b,a) DO UPDATE SET c=excluded.c+1",
			},
			query: "SELECT * FROM excluded ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(2)},
				{int64(3), int64(4), int64(1)},
				{int64(5), int64(6), int64(0)},
			},
		},
		{
			name: "upsert3-210 DO UPDATE with WHERE clause on base table",
			setup: []string{
				"CREATE TABLE excluded(a INT, b INT, c INT DEFAULT 0)",
				"CREATE UNIQUE INDEX excludedab ON excluded(a,b)",
				"INSERT INTO excluded(a,b) VALUES(1,2),(1,2),(3,4),(1,2),(5,6),(3,4) ON CONFLICT(b,a) DO UPDATE SET c=excluded.c+1",
				"INSERT INTO excluded AS base(a,b,c) VALUES(1,2,8),(1,2,3) ON CONFLICT(b,a) DO UPDATE SET c=excluded.c+1 WHERE base.c<excluded.c",
			},
			query: "SELECT * FROM excluded ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), int64(2), int64(9)},
				{int64(3), int64(4), int64(1)},
				{int64(5), int64(6), int64(0)},
			},
		},
		// Tests from upsert4.test
		{
			name: "upsert4-1.1 DO NOTHING basic",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(3, NULL, 'three')",
				"INSERT INTO t1 VALUES(1, NULL, 'xyz') ON CONFLICT DO NOTHING",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(2), nil, "two"},
				{int64(3), nil, "three"},
			},
		},
		{
			name: "upsert4-1.2 DO NOTHING on unique constraint",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(3, NULL, 'three')",
				"INSERT INTO t1 VALUES(4, NULL, 'two') ON CONFLICT DO NOTHING",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(2), nil, "two"},
				{int64(3), nil, "three"},
			},
		},
		{
			name: "upsert4-1.3 DO UPDATE on unique column",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(3, NULL, 'three')",
				"INSERT INTO t1 VALUES(4, NULL, 'two') ON CONFLICT (c) DO UPDATE SET b = 1",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(2), int64(1), "two"},
				{int64(3), nil, "three"},
			},
		},
		{
			name: "upsert4-1.4 DO UPDATE on primary key",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(3, NULL, 'three')",
				"INSERT INTO t1 VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET b=2",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(2), int64(2), "two"},
				{int64(3), nil, "three"},
			},
		},
		{
			name: "upsert4-1.7 DO UPDATE with subquery",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET (b, c) = (SELECT 'x', 'y')",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(1), nil, "one"},
				{int64(2), "x", "y"},
			},
		},
		{
			name: "upsert4-1.8 DO UPDATE changing primary key",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c UNIQUE)",
				"INSERT INTO t1 VALUES(1, NULL, 'one')",
				"INSERT INTO t1 VALUES(2, NULL, 'two')",
				"INSERT INTO t1 VALUES(3, NULL, 'three')",
				"INSERT INTO t1 VALUES(1, NULL, NULL) ON CONFLICT (a) DO UPDATE SET (c, a) = ('four', 4)",
			},
			query: "SELECT * FROM t1 ORDER BY a",
			wantRows: [][]interface{}{
				{int64(2), nil, "two"},
				{int64(3), nil, "three"},
				{int64(4), nil, "four"},
			},
		},
		// Additional comprehensive upsert tests
		{
			name: "upsert-complex-1 multiple inserts with conflicts",
			setup: []string{
				"CREATE TABLE inventory(product_id INT PRIMARY KEY, name TEXT, quantity INT DEFAULT 0)",
				"INSERT INTO inventory VALUES(1, 'Widget', 10)",
				"INSERT INTO inventory VALUES(1, 'Widget', 5),(2, 'Gadget', 20) ON CONFLICT(product_id) DO UPDATE SET quantity = quantity + excluded.quantity",
			},
			query: "SELECT * FROM inventory ORDER BY product_id",
			wantRows: [][]interface{}{
				{int64(1), "Widget", int64(15)},
				{int64(2), "Gadget", int64(20)},
			},
		},
		{
			name: "upsert-complex-2 conditional update",
			setup: []string{
				"CREATE TABLE prices(item_id INT PRIMARY KEY, price REAL, updated_at INT)",
				"INSERT INTO prices VALUES(1, 10.0, 1000)",
				"INSERT INTO prices VALUES(1, 15.0, 2000) ON CONFLICT(item_id) DO UPDATE SET price = excluded.price, updated_at = excluded.updated_at WHERE excluded.updated_at > prices.updated_at",
			},
			query: "SELECT * FROM prices",
			wantRows: [][]interface{}{
				{int64(1), 15.0, int64(2000)},
			},
		},
		{
			name: "upsert-complex-3 update only if value increases",
			setup: []string{
				"CREATE TABLE scores(player_id INT PRIMARY KEY, score INT)",
				"INSERT INTO scores VALUES(1, 100)",
				"INSERT INTO scores VALUES(1, 80) ON CONFLICT(player_id) DO UPDATE SET score = excluded.score WHERE excluded.score > scores.score",
			},
			query: "SELECT * FROM scores",
			wantRows: [][]interface{}{
				{int64(1), int64(100)},
			},
		},
		{
			name: "upsert-complex-4 increment counter on conflict",
			setup: []string{
				"CREATE TABLE page_views(page TEXT UNIQUE, views INT DEFAULT 1)",
				"INSERT INTO page_views(page) VALUES('home')",
				"INSERT INTO page_views(page) VALUES('home') ON CONFLICT(page) DO UPDATE SET views = views + 1",
				"INSERT INTO page_views(page) VALUES('home') ON CONFLICT(page) DO UPDATE SET views = views + 1",
			},
			query: "SELECT * FROM page_views",
			wantRows: [][]interface{}{
				{"home", int64(3)},
			},
		},
		{
			name: "upsert-complex-5 upsert with WITHOUT ROWID",
			setup: []string{
				"CREATE TABLE kv_store(key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID",
				"INSERT INTO kv_store VALUES('config1', 'value1') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
				"INSERT INTO kv_store VALUES('config1', 'value2') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
			},
			query: "SELECT * FROM kv_store",
			wantRows: [][]interface{}{
				{"config1", "value2"},
			},
		},
		{
			name: "upsert-complex-6 multiple unique constraints",
			setup: []string{
				"CREATE TABLE users(id INT PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE)",
				"INSERT INTO users VALUES(1, 'user@test.com', 'user1')",
				"INSERT INTO users VALUES(1, 'new@test.com', 'user1') ON CONFLICT(id) DO UPDATE SET email = excluded.email",
			},
			query: "SELECT * FROM users",
			wantRows: [][]interface{}{
				{int64(1), "new@test.com", "user1"},
			},
		},
		{
			name: "upsert-complex-7 bulk upsert",
			setup: []string{
				"CREATE TABLE products(id INT PRIMARY KEY, stock INT DEFAULT 0)",
				"INSERT INTO products VALUES(1, 10),(2, 20),(3, 30)",
				"INSERT INTO products VALUES(1, 5),(2, 5),(4, 40) ON CONFLICT(id) DO UPDATE SET stock = stock + excluded.stock",
			},
			query: "SELECT * FROM products ORDER BY id",
			wantRows: [][]interface{}{
				{int64(1), int64(15)},
				{int64(2), int64(25)},
				{int64(3), int64(30)},
				{int64(4), int64(40)},
			},
		},
	}

	for _, tt := range tests {
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
			if tt.wantErr {
				_, err := db.Exec(tt.query)
				if err == nil {
					t.Errorf("Expected error containing %q but got none", tt.errMsg)
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("Error %q does not contain expected substring %q", err.Error(), tt.errMsg)
				}
				return
			}

			rows, err := db.Query(tt.query)
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
					if !compareUpsertValues(gotVal, wantVal) {
						t.Errorf("Row %d, Col %d: got %v (%T), want %v (%T)", i, j, gotVal, gotVal, wantVal, wantVal)
					}
				}
			}
		})
	}
}

// compareUpsertValues compares two values handling type conversions
func compareUpsertValues(got, want interface{}) bool {
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
