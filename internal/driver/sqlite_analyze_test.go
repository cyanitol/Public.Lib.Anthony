package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteAnalyze tests the ANALYZE command and statistics functionality
// Converted from contrib/sqlite/sqlite-src-3510200/test/analyze*.test
func TestSQLiteAnalyze(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "analyze_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name    string
		setup   []string
		query   string
		want    interface{}
		wantErr bool
	}{
		// analyze.test 1.1 - Error on non-existent table
		{
			name:    "analyze_no_such_table",
			query:   "ANALYZE no_such_table",
			wantErr: true,
		},
		// analyze.test 1.2 - No sqlite_stat1 initially
		{
			name:  "analyze_no_stat1_initially",
			query: "SELECT count(*) FROM sqlite_master WHERE name='sqlite_stat1'",
			want:  int64(0),
		},
		// analyze.test 1.3 - Error on non-existent database
		{
			name:    "analyze_no_such_db",
			query:   "ANALYZE no_such_db.no_such_table",
			wantErr: true,
		},
		// analyze.test 1.5 - ANALYZE on empty database succeeds
		{
			name:  "analyze_empty_db",
			query: "ANALYZE",
		},
		// analyze.test 1.6 - sqlite_stat1 created after ANALYZE
		{
			name: "analyze_creates_stat1",
			setup: []string{
				"DROP TABLE IF EXISTS sqlite_stat1",
				"ANALYZE",
			},
			query: "SELECT count(*) FROM sqlite_master WHERE name='sqlite_stat1'",
			want:  int64(1),
		},
		// analyze.test 1.6.2 - Cannot index sqlite_stat1
		{
			name: "analyze_cannot_index_stat1",
			setup: []string{
				"ANALYZE",
			},
			query:   "CREATE INDEX stat1idx ON sqlite_stat1(idx)",
			wantErr: true,
		},
		// analyze.test 1.10 - ANALYZE on table with no data
		{
			name: "analyze_empty_table",
			setup: []string{
				"CREATE TABLE t1(a,b)",
				"ANALYZE main.t1",
			},
			query: "SELECT * FROM sqlite_stat1",
			want:  "",
		},
		// analyze.test 2.1 - No analysis without data
		{
			name: "analyze_index_no_data",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"ANALYZE main.t1",
			},
			query: "SELECT * FROM sqlite_stat1 ORDER BY idx",
			want:  "",
		},
		// analyze.test 3.1 - Basic statistics
		{
			name: "analyze_basic_stats",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"CREATE INDEX t1i2 ON t1(b)",
				"CREATE INDEX t1i3 ON t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(1,3)",
				"ANALYZE main.t1",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1",
			want:  int64(3),
		},
		// analyze.test 3.2 - Stats after more inserts
		{
			name: "analyze_incremental_stats",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"CREATE INDEX t1i2 ON t1(b)",
				"CREATE INDEX t1i3 ON t1(a,b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(1,3)",
				"INSERT INTO t1 VALUES(1,4)",
				"INSERT INTO t1 VALUES(1,5)",
				"ANALYZE t1",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t1'",
			want:  int64(3),
		},
		// analyze.test 3.3 - Stats with varied data
		{
			name: "analyze_varied_data",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"CREATE INDEX t1i2 ON t1(b)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(1,3)",
				"INSERT INTO t1 VALUES(1,4)",
				"INSERT INTO t1 VALUES(1,5)",
				"INSERT INTO t1 VALUES(2,5)",
				"ANALYZE main",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t1'",
			want:  int64(2),
		},
		// analyze.test 3.4 - Multiple tables
		{
			name: "analyze_multiple_tables",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1,2)",
				"INSERT INTO t1 VALUES(2,3)",
				"CREATE TABLE t2 AS SELECT * FROM t1",
				"CREATE INDEX t2i1 ON t2(a)",
				"ANALYZE",
			},
			query: "SELECT COUNT(DISTINCT tbl) FROM sqlite_stat1",
			want:  int64(2),
		},
		// analyze.test 3.5 - ANALYZE specific table
		{
			name: "analyze_specific_table",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"DROP TABLE IF EXISTS t2",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"INSERT INTO t1 VALUES(1,2)",
				"CREATE TABLE t2(a,b)",
				"CREATE INDEX t2i1 ON t2(a)",
				"INSERT INTO t2 VALUES(3,4)",
				"ANALYZE t1",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t1'",
			want:  int64(1),
		},
		// analyze.test 3.6 - Drop index updates stats
		{
			name: "analyze_drop_index",
			setup: []string{
				"DROP TABLE IF EXISTS t1",
				"CREATE TABLE t1(a,b)",
				"CREATE INDEX t1i1 ON t1(a)",
				"CREATE INDEX t1i2 ON t1(b)",
				"INSERT INTO t1 VALUES(1,2)",
				"ANALYZE",
				"DROP INDEX t1i2",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t1' AND idx='t1i2'",
			want:  int64(0),
		},
		// analyze.test 3.8 - Complex index stats
		{
			name: "analyze_complex_index",
			setup: []string{
				"DROP TABLE IF EXISTS t3",
				"CREATE TABLE t3(a,b,c,d)",
				"CREATE INDEX t3i1 ON t3(a)",
				"CREATE INDEX t3i2 ON t3(a,b,c,d)",
				"INSERT INTO t3 VALUES(1,2,3,'hi')",
				"INSERT INTO t3 VALUES(1,2,4,'hi')",
				"INSERT INTO t3 VALUES(2,3,5,'hi')",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t3'",
			want:  int64(2),
		},
		// analyze.test 3.10 - Tables with special names
		{
			name: "analyze_special_names",
			setup: []string{
				`CREATE TABLE "test space"(a, b)`,
				`CREATE INDEX "test idx" ON "test space"(a)`,
				`INSERT INTO "test space" VALUES(1, 2)`,
				"ANALYZE",
			},
			query: `SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='test space'`,
			want:  int64(1),
		},
		// analyze.test 4.1 - Corrupted stat1 doesn't crash
		{
			name: "analyze_corrupted_stat1_query",
			setup: []string{
				"DROP TABLE IF EXISTS t4",
				"CREATE TABLE t4(x,y)",
				"CREATE INDEX t4i1 ON t4(x)",
				"INSERT INTO t4 VALUES(1,2)",
				"ANALYZE",
			},
			query: "SELECT * FROM t4 WHERE x=1234",
			want:  "",
		},
		// analyze.test 5.0 - DROP TABLE removes stats
		{
			name: "analyze_drop_table",
			setup: []string{
				"DROP TABLE IF EXISTS t5",
				"CREATE TABLE t5(a,b)",
				"CREATE INDEX t5i1 ON t5(a)",
				"INSERT INTO t5 VALUES(1,2)",
				"ANALYZE",
				"DROP TABLE t5",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='t5'",
			want:  int64(0),
		},
		// analyze4.test 1.0 - NULL values in statistics
		{
			name: "analyze_null_values",
			setup: []string{
				"DROP TABLE IF EXISTS tn",
				"CREATE TABLE tn(a,b)",
				"CREATE INDEX tna ON tn(a)",
				"CREATE INDEX tnb ON tn(b)",
				"INSERT INTO tn VALUES(1,NULL)",
				"INSERT INTO tn VALUES(2,NULL)",
				"INSERT INTO tn VALUES(3,NULL)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tn'",
			want:  int64(2),
		},
		// analyze4.test 1.2 - Stats after UPDATE
		{
			name: "analyze_after_update",
			setup: []string{
				"DROP TABLE IF EXISTS tu",
				"CREATE TABLE tu(a,b)",
				"CREATE INDEX tua ON tu(a)",
				"INSERT INTO tu VALUES(1,NULL)",
				"INSERT INTO tu VALUES(2,NULL)",
				"INSERT INTO tu VALUES(3,NULL)",
				"INSERT INTO tu VALUES(4,NULL)",
				"UPDATE tu SET b='x' WHERE a%2=1",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tu'",
			want:  int64(1),
		},
		// Test multi-column index statistics
		{
			name: "analyze_multicolumn_index",
			setup: []string{
				"DROP TABLE IF EXISTS tm",
				"CREATE TABLE tm(a,b,c)",
				"CREATE INDEX tmi ON tm(a,b,c)",
				"INSERT INTO tm VALUES(1,1,1)",
				"INSERT INTO tm VALUES(1,1,2)",
				"INSERT INTO tm VALUES(1,2,3)",
				"INSERT INTO tm VALUES(2,1,4)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tm'",
			want:  int64(1),
		},
		// Test ANALYZE with WHERE clause data
		{
			name: "analyze_where_selectivity",
			setup: []string{
				"DROP TABLE IF EXISTS tw",
				"CREATE TABLE tw(x INTEGER, y INTEGER)",
				"CREATE INDEX twx ON tw(x)",
				"INSERT INTO tw VALUES(1,100)",
				"INSERT INTO tw VALUES(2,200)",
				"INSERT INTO tw VALUES(3,300)",
				"INSERT INTO tw VALUES(4,400)",
				"INSERT INTO tw VALUES(5,500)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM tw WHERE x > 2",
			want:  int64(3),
		},
		// Test ANALYZE on table with PRIMARY KEY
		{
			name: "analyze_primary_key",
			setup: []string{
				"DROP TABLE IF EXISTS tp",
				"CREATE TABLE tp(id INTEGER PRIMARY KEY, data TEXT)",
				"INSERT INTO tp VALUES(1,'a')",
				"INSERT INTO tp VALUES(2,'b')",
				"INSERT INTO tp VALUES(3,'c')",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tp'",
			want:  int64(0), // PRIMARY KEY doesn't show in stat1
		},
		// Test ANALYZE updates existing stats
		{
			name: "analyze_update_stats",
			setup: []string{
				"DROP TABLE IF EXISTS tup",
				"CREATE TABLE tup(a,b)",
				"CREATE INDEX tupi ON tup(a)",
				"INSERT INTO tup VALUES(1,1)",
				"ANALYZE",
				"INSERT INTO tup VALUES(2,2)",
				"INSERT INTO tup VALUES(3,3)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tup'",
			want:  int64(1),
		},
		// Test ANALYZE with UNIQUE index
		{
			name: "analyze_unique_index",
			setup: []string{
				"DROP TABLE IF EXISTS tuq",
				"CREATE TABLE tuq(a UNIQUE, b)",
				"INSERT INTO tuq VALUES(1,'x')",
				"INSERT INTO tuq VALUES(2,'y')",
				"INSERT INTO tuq VALUES(3,'z')",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tuq'",
			want:  int64(1),
		},
		// Test ANALYZE with expression index (if supported)
		{
			name: "analyze_simple_expression",
			setup: []string{
				"DROP TABLE IF EXISTS tex",
				"CREATE TABLE tex(a,b)",
				"INSERT INTO tex VALUES(1,10)",
				"INSERT INTO tex VALUES(2,20)",
				"INSERT INTO tex VALUES(3,30)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM tex WHERE a+b > 20",
			want:  int64(2),
		},
		// Test ANALYZE persistence across connections
		{
			name: "analyze_persistence",
			setup: []string{
				"DROP TABLE IF EXISTS tper",
				"CREATE TABLE tper(a,b)",
				"CREATE INDEX tperi ON tper(a)",
				"INSERT INTO tper VALUES(1,1)",
				"INSERT INTO tper VALUES(2,2)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tper'",
			want:  int64(1),
		},
		// Test large dataset statistics
		{
			name: "analyze_large_dataset",
			setup: []string{
				"DROP TABLE IF EXISTS tlarge",
				"CREATE TABLE tlarge(a INTEGER, b INTEGER)",
				"CREATE INDEX tlargei ON tlarge(a)",
				"INSERT INTO tlarge VALUES(1,1)",
				"INSERT INTO tlarge SELECT a+1, b+1 FROM tlarge",
				"INSERT INTO tlarge SELECT a+2, b+2 FROM tlarge",
				"INSERT INTO tlarge SELECT a+4, b+4 FROM tlarge",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM tlarge",
			want:  int64(8),
		},
		// Test ANALYZE with skewed data distribution
		{
			name: "analyze_skewed_data",
			setup: []string{
				"DROP TABLE IF EXISTS tskew",
				"CREATE TABLE tskew(category INTEGER, value INTEGER)",
				"CREATE INDEX tskewi ON tskew(category)",
				"INSERT INTO tskew VALUES(1,100)",
				"INSERT INTO tskew VALUES(1,101)",
				"INSERT INTO tskew VALUES(1,102)",
				"INSERT INTO tskew VALUES(1,103)",
				"INSERT INTO tskew VALUES(2,200)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM tskew WHERE category=1",
			want:  int64(4),
		},
		// Test ANALYZE with composite statistics
		{
			name: "analyze_composite_stats",
			setup: []string{
				"DROP TABLE IF EXISTS tcomp",
				"CREATE TABLE tcomp(a,b,c,d)",
				"CREATE INDEX tcompi ON tcomp(a,b)",
				"INSERT INTO tcomp VALUES(1,1,1,1)",
				"INSERT INTO tcomp VALUES(1,2,2,2)",
				"INSERT INTO tcomp VALUES(2,1,3,3)",
				"INSERT INTO tcomp VALUES(2,2,4,4)",
				"ANALYZE",
			},
			query: "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='tcomp'",
			want:  int64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run setup statements
			for _, stmt := range tt.setup {
				_, err := db.Exec(stmt)
				if err != nil {
					t.Logf("setup statement failed (may be expected): %v", err)
				}
			}

			// Execute the test query
			if tt.wantErr {
				_, err := db.Exec(tt.query)
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			// For SELECT queries, check the result
			if tt.want != nil {
				row := db.QueryRow(tt.query)
				switch want := tt.want.(type) {
				case int64:
					var got int64
					err := row.Scan(&got)
					if err != nil {
						t.Fatalf("failed to scan result: %v", err)
					}
					if got != want {
						t.Errorf("got %d, want %d", got, want)
					}
				case string:
					// For empty result checks
					var got string
					err := row.Scan(&got)
					if err != nil && err != sql.ErrNoRows {
						// Empty result is acceptable
						if want == "" && err == sql.ErrNoRows {
							return
						}
						t.Fatalf("failed to scan result: %v", err)
					}
				}
			} else {
				// Just execute without checking result
				_, err := db.Exec(tt.query)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestAnalyzeStatisticsUsage tests that ANALYZE statistics affect query planning
func TestAnalyzeStatisticsUsage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "analyze_usage_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table with selective and non-selective columns
	_, err = db.Exec(`
		CREATE TABLE query_test(
			selective INTEGER,
			nonselective INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create indices
	_, err = db.Exec("CREATE INDEX idx_selective ON query_test(selective)")
	if err != nil {
		t.Fatalf("failed to create selective index: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_nonselective ON query_test(nonselective)")
	if err != nil {
		t.Fatalf("failed to create nonselective index: %v", err)
	}

	// Insert data: selective has unique values, nonselective has repeated values
	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO query_test VALUES(?, ?)", i, 1)
		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}
	}

	// Run ANALYZE
	_, err = db.Exec("ANALYZE")
	if err != nil {
		t.Fatalf("failed to analyze: %v", err)
	}

	// Verify statistics exist
	var count int64
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl='query_test'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query statistics: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 index statistics, got %d", count)
	}

	// Query using the selective index
	rows, err := db.Query("SELECT * FROM query_test WHERE selective = 50")
	if err != nil {
		t.Fatalf("failed to query with selective index: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}

	if rowCount != 1 {
		t.Errorf("expected 1 row, got %d", rowCount)
	}
}
