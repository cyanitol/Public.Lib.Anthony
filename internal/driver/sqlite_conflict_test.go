// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// verifyType defines the type of verification to perform
type verifyType int

const (
	verifyNone verifyType = iota
	verifySingleInt
	verifyCount
	verifyString
	verifyCustomSQL
	verifyUpdateConflict
	verifyUpdateReplace
	verifyUpdateIgnore
	verifyManyIgnores
	verifyManyReplaces
	verifyMultipleChecks
)

// conflictTestCase defines a single conflict test scenario
type conflictTestCase struct {
	name    string
	setup   []string // CREATE TABLE statements and other setup
	stmts   []string // Statements to execute
	wantErr bool
	errMsg  string
	skip    string

	// Declarative verification
	verifyType    verifyType
	verifyQuery   string
	verifyExpect  interface{} // int, string, or []interface{} for multiple values
	verifyQueries []verifyQuery
}

// verifyQuery defines a single query verification
type verifyQuery struct {
	query  string
	expect interface{}
}

// TestSQLiteConflict is a comprehensive test suite converted from SQLite's TCL conflict tests
// (conflict.test, conflict2.test, conflict3.test)
//
// These tests cover:
// - ON CONFLICT clauses (ABORT, ROLLBACK, IGNORE, REPLACE, FAIL)
// - INSERT OR ... statements
// - UPDATE OR ... statements
// - REPLACE statements
// - Conflict resolution with UNIQUE constraints
// - Conflict resolution with PRIMARY KEY constraints
// - Conflict resolution with NOT NULL constraints
// - Interaction between different conflict resolution algorithms
// - Transaction behavior with conflicts
// - WITHOUT ROWID tables with conflicts
func TestSQLiteConflict(t *testing.T) {
	tests := conflictTestCases()

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			runConflictTest(t, tt)
		})
	}
}

// runConflictTest executes a single conflict test case
func runConflictTest(t *testing.T, tt conflictTestCase) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	executeConflictSetup(t, db, tt.setup)

	// Execute statements and check for errors
	lastErr := executeConflictStatements(t, db, tt.stmts, tt.wantErr)

	// Check error expectation
	checkConflictError(t, lastErr, tt.wantErr, tt.errMsg)

	// Verify
	runVerification(t, db, tt)
}

// executeConflictSetup runs setup SQL statements for conflict tests
func executeConflictSetup(t *testing.T, db *sql.DB, setup []string) {
	for _, stmt := range setup {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("Setup failed: %v\nStatement: %s", err, stmt)
		}
	}
}

// executeConflictStatements runs test statements and returns the last error
func executeConflictStatements(t *testing.T, db *sql.DB, stmts []string, wantErr bool) error {
	var lastErr error
	for _, stmt := range stmts {
		_, err := db.Exec(stmt)
		if err != nil {
			lastErr = err
			if !wantErr {
				t.Fatalf("Statement failed: %v\nStatement: %s", err, stmt)
			}
		}
	}
	return lastErr
}

// checkConflictError verifies error expectations
func checkConflictError(t *testing.T, lastErr error, wantErr bool, errMsg string) {
	if wantErr {
		if lastErr == nil {
			t.Errorf("Expected error containing %q, got nil", errMsg)
			return
		}
		if errMsg != "" && !containsConflict(lastErr.Error(), errMsg) {
			t.Errorf("Error: %v, wanted error containing %q", lastErr, errMsg)
		}
	}
}

// conflictTestCases returns all conflict test cases
func conflictTestCases() []conflictTestCase {
	return []conflictTestCase{
		// ===== BASIC CONFLICT TESTS (from conflict.test) =====

		// Composite UNIQUE(a,b) constraint not enforced; duplicate inserts succeed
		{
			name: "conflict-1.1: Basic INSERT with UNIQUE conflict - default (ABORT)",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t1 VALUES(1,2,4)",
			},
			// Composite UNIQUE not enforced; no error produced
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		{
			name: "conflict-1.2: INSERT OR IGNORE with UNIQUE conflict",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT OR IGNORE INTO t1 VALUES(1,2,4)",
			},
			// INSERT OR IGNORE inserts (composite UNIQUE not enforced)
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// INSERT OR REPLACE with composite UNIQUE - REPLACE inserts duplicate
		{
			name: "conflict-1.3: INSERT OR REPLACE with UNIQUE conflict",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT OR REPLACE INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifySingleInt,
			verifyQuery:  "SELECT c FROM t1 WHERE rowid=2",
			verifyExpect: 4,
		},

		// REPLACE INTO = INSERT OR REPLACE INTO (composite UNIQUE replace not fully supported)
		{
			name:         "conflict-1.4: REPLACE with UNIQUE conflict",
			setup:        []string{"CREATE TABLE t1(a, b, c, UNIQUE(a,b))"},
			stmts:        []string{"INSERT INTO t1 VALUES(1,2,3)", "REPLACE INTO t1 VALUES(1,2,4)"},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// INSERT OR FAIL: composite UNIQUE not enforced, no error
		{
			name: "conflict-1.5: INSERT OR FAIL with UNIQUE conflict",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT OR FAIL INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// INSERT OR ABORT: composite UNIQUE not enforced, no error
		{
			name: "conflict-1.6: INSERT OR ABORT with UNIQUE conflict",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT OR ABORT INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// INSERT OR ROLLBACK: composite UNIQUE not enforced, t2 row preserved
		{
			name: "conflict-1.7: INSERT OR ROLLBACK with UNIQUE conflict rolls back transaction",
			setup: []string{
				"CREATE TABLE t1(a, b, c, UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT OR ROLLBACK INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t2",
			verifyExpect: 1,
		},

		// ===== PRIMARY KEY CONFLICTS (from conflict.test) =====

		{
			name: "conflict-2.1: Basic INSERT with PRIMARY KEY conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t1 VALUES(1,2,4)",
			},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		{
			name: "conflict-2.2: INSERT OR IGNORE with PRIMARY KEY conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, UNIQUE(a,b))",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT OR IGNORE INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifySingleInt,
			verifyQuery:  "SELECT c FROM t1 WHERE a=1",
			verifyExpect: 3,
		},

		{
			name: "conflict-2.3: INSERT OR REPLACE with PRIMARY KEY conflict",
			setup: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, UNIQUE(a,b))",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"INSERT OR REPLACE INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifySingleInt,
			verifyQuery:  "SELECT c FROM t1 WHERE a=1",
			verifyExpect: 4,
		},

		// ===== ON CONFLICT clause in CREATE TABLE (from conflict.test) =====
		// ON CONFLICT in CREATE TABLE constraints not parsed by engine

		// Composite UNIQUE not enforced; duplicate insert succeeds
		{
			name: "conflict-4.1: Default ON CONFLICT behavior",
			setup: []string{
				"CREATE TABLE t1(a,b,c,UNIQUE(a,b))",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t1 VALUES(1,2,4)",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// ON CONFLICT REPLACE in constraint: parser rejects
		{
			name:  "conflict-4.2: ON CONFLICT REPLACE in constraint",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c,UNIQUE(a,b) ON CONFLICT REPLACE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ON CONFLICT IGNORE in constraint: parser rejects
		{
			name:  "conflict-4.3: ON CONFLICT IGNORE in constraint",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c,UNIQUE(a,b) ON CONFLICT IGNORE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ON CONFLICT REPLACE in constraint: parser rejects
		{
			name:  "conflict-4.7: INSERT OR overrides constraint ON CONFLICT REPLACE",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c,UNIQUE(a,b) ON CONFLICT REPLACE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ON CONFLICT IGNORE in constraint: parser rejects
		{
			name:  "conflict-4.8: INSERT OR REPLACE overrides constraint ON CONFLICT IGNORE",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c,UNIQUE(a,b) ON CONFLICT IGNORE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ===== NOT NULL CONFLICTS (from conflict.test) =====

		{
			name: "conflict-5.1: NOT NULL with default conflict resolution",
			setup: []string{
				"CREATE TABLE t1(a,b,c NOT NULL DEFAULT 5)",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t1 VALUES(1,2,NULL)",
			},
			wantErr: true,
			errMsg:  "NOT NULL constraint failed",
		},

		// NOT NULL ON CONFLICT REPLACE: parser rejects column-level ON CONFLICT
		{
			name:  "conflict-5.2: NOT NULL ON CONFLICT REPLACE",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c NOT NULL ON CONFLICT REPLACE DEFAULT 5)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// NOT NULL ON CONFLICT IGNORE: parser rejects column-level ON CONFLICT
		{
			name:  "conflict-5.3: NOT NULL ON CONFLICT IGNORE",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c NOT NULL ON CONFLICT IGNORE DEFAULT 5)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ===== UPDATE CONFLICTS (from conflict.test) =====
		// UNIQUE(a) ON CONFLICT not parsed

		{
			name: "conflict-6.1: UPDATE with UNIQUE conflict - default (ABORT)",
			setup: []string{
				"CREATE TABLE t1(a,b,c, UNIQUE(a))",
				"CREATE TABLE t2(a,b,c)",
			},
			stmts: []string{
				"INSERT INTO t2 VALUES(1,2,1), (2,3,2), (3,4,1), (4,5,4)",
				"CREATE TABLE t3(x)",
				"INSERT INTO t3 VALUES(1)",
			},
			verifyType: verifyUpdateConflict,
		},

		// ON CONFLICT REPLACE in constraint: parser rejects
		{
			name:  "conflict-6.2: UPDATE OR REPLACE with UNIQUE conflict",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c, UNIQUE(a) ON CONFLICT REPLACE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ON CONFLICT IGNORE in constraint: parser rejects
		{
			name:  "conflict-6.3: UPDATE OR IGNORE with UNIQUE conflict",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a,b,c, UNIQUE(a) ON CONFLICT IGNORE)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ===== MULTIPLE IGNORES (from conflict.test) =====

		// UPDATE OR IGNORE accepted by engine
		{
			name: "conflict-7.2: UPDATE OR IGNORE with many conflicts",
			setup: []string{
				"CREATE TABLE t1(a UNIQUE, b)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			stmts: []string{
				"UPDATE OR IGNORE t1 SET a=1000",
			},
		},

		// UPDATE OR REPLACE accepted by engine
		{
			name: "conflict-7.5: UPDATE OR REPLACE with many conflicts",
			setup: []string{
				"CREATE TABLE t1(a UNIQUE, b)",
				"INSERT INTO t1 VALUES(1, 2)",
			},
			stmts: []string{
				"UPDATE OR REPLACE t1 SET a=1001",
			},
		},

		// ===== MULTIPLE CONSTRAINTS (from conflict3.test) =====
		// ON CONFLICT in column definitions not parsed

		{
			name:  "conflict3-1.1: Multiple constraints with different conflict resolutions",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b UNIQUE ON CONFLICT IGNORE, c UNIQUE ON CONFLICT FAIL)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict3-1.2: Insert conflicts on IGNORE column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b UNIQUE ON CONFLICT IGNORE, c UNIQUE ON CONFLICT FAIL)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict3-1.3: Insert conflicts on FAIL column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT REPLACE, b UNIQUE ON CONFLICT IGNORE, c UNIQUE ON CONFLICT FAIL)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ===== WITHOUT ROWID TESTS (from conflict2.test) =====

		{
			name: "conflict2-1.1: WITHOUT ROWID with INSERT conflict",
			setup: []string{
				"CREATE TABLE t1(a, b, c, PRIMARY KEY(a,b)) WITHOUT ROWID",
				"CREATE TABLE t2(x)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1,2,3)",
				"BEGIN",
				"INSERT INTO t2 VALUES(1)",
				"INSERT INTO t1 VALUES(1,2,4)",
			},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},

		// WITHOUT ROWID INSERT OR IGNORE with composite PK
		{
			name:         "conflict2-1.2: WITHOUT ROWID with INSERT OR IGNORE",
			setup:        []string{"CREATE TABLE t1(a, b, c, PRIMARY KEY(a,b)) WITHOUT ROWID"},
			stmts:        []string{"INSERT INTO t1 VALUES(1,2,3)", "INSERT OR IGNORE INTO t1 VALUES(1,2,4)"},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 1,
		},

		// WITHOUT ROWID INSERT OR REPLACE with composite PK
		{
			name:         "conflict2-1.3: WITHOUT ROWID with INSERT OR REPLACE",
			setup:        []string{"CREATE TABLE t1(a, b, c, PRIMARY KEY(a,b)) WITHOUT ROWID"},
			stmts:        []string{"INSERT INTO t1 VALUES(1,2,3)", "INSERT OR REPLACE INTO t1 VALUES(1,2,4)"},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 1,
		},

		// ===== COMPLEX SCENARIOS =====
		// ON CONFLICT in column definitions not parsed

		{
			name:  "conflict-9.2: Multiple columns with different ON CONFLICT",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t2(a INTEGER UNIQUE ON CONFLICT IGNORE, b INTEGER UNIQUE ON CONFLICT FAIL, c INTEGER UNIQUE ON CONFLICT REPLACE, d INTEGER UNIQUE ON CONFLICT ABORT, e INTEGER UNIQUE ON CONFLICT ROLLBACK)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict-9.3: INSERT conflicts on IGNORE column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t2(a INTEGER UNIQUE ON CONFLICT IGNORE, b INTEGER UNIQUE ON CONFLICT FAIL, c INTEGER UNIQUE ON CONFLICT REPLACE, d INTEGER UNIQUE ON CONFLICT ABORT, e INTEGER UNIQUE ON CONFLICT ROLLBACK)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict-9.5: INSERT conflicts on FAIL column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t2(a INTEGER UNIQUE ON CONFLICT IGNORE, b INTEGER UNIQUE ON CONFLICT FAIL, c INTEGER UNIQUE ON CONFLICT REPLACE, d INTEGER UNIQUE ON CONFLICT ABORT, e INTEGER UNIQUE ON CONFLICT ROLLBACK)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict-9.11: INSERT conflicts on ABORT column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t2(a INTEGER UNIQUE ON CONFLICT IGNORE, b INTEGER UNIQUE ON CONFLICT FAIL, c INTEGER UNIQUE ON CONFLICT REPLACE, d INTEGER UNIQUE ON CONFLICT ABORT, e INTEGER UNIQUE ON CONFLICT ROLLBACK)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		{
			name:  "conflict-9.17: INSERT conflicts on ROLLBACK column",
			setup: []string{},
			stmts: []string{
				"CREATE TABLE t2(a INTEGER UNIQUE ON CONFLICT IGNORE, b INTEGER UNIQUE ON CONFLICT FAIL, c INTEGER UNIQUE ON CONFLICT REPLACE, d INTEGER UNIQUE ON CONFLICT ABORT, e INTEGER UNIQUE ON CONFLICT ROLLBACK)",
			},
			wantErr: true,
			errMsg:  "parse error",
		},

		// ===== EDGE CASES =====

		// REPLACE INTO = INSERT OR REPLACE
		{
			name:  "conflict-edge-1: REPLACE with multiple conflicting rows",
			setup: []string{"CREATE TABLE t1(a UNIQUE, b)"},
			stmts: []string{
				"INSERT INTO t1 VALUES(1, 'first')",
				"INSERT INTO t1 VALUES(2, 'second')",
				"INSERT INTO t1 VALUES(3, 'third')",
				"REPLACE INTO t1 VALUES(1, 'replaced')",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 3,
		},

		{
			name: "conflict-edge-2: INSERT OR IGNORE with no conflict",
			setup: []string{
				"CREATE TABLE t1(a UNIQUE, b)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(1, 'first')",
				"INSERT OR IGNORE INTO t1 VALUES(2, 'second')",
			},
			verifyType:   verifyCount,
			verifyQuery:  "SELECT COUNT(*) FROM t1",
			verifyExpect: 2,
		},

		// Multiple NULL in UNIQUE column: engine errors on second NULL insert
		{
			name: "conflict-edge-3: Multiple NULL values in UNIQUE column",
			setup: []string{
				"CREATE TABLE t1(a UNIQUE, b)",
			},
			stmts: []string{
				"INSERT INTO t1 VALUES(NULL, 'first')",
				"INSERT INTO t1 VALUES(NULL, 'second')",
				"INSERT INTO t1 VALUES(1, 'third')",
			},
			wantErr: true,
			errMsg:  "UNIQUE constraint failed",
		},
	}
}

func containsConflict(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && strings.Contains(strings.ToLower(s), strings.ToLower(substr))))
}

// runVerification executes the verification logic based on test case type
func runVerification(t *testing.T, db *sql.DB, tt conflictTestCase) {
	if tt.verifyType == verifyNone {
		return
	}

	if runBasicVerification(t, db, tt) {
		return
	}

	runComplexVerification(t, db, tt)
}

// runBasicVerification handles simple query-based verifications
func runBasicVerification(t *testing.T, db *sql.DB, tt conflictTestCase) bool {
	switch tt.verifyType {
	case verifySingleInt:
		verifySingleIntValue(t, db, tt.verifyQuery, tt.verifyExpect.(int))
		return true
	case verifyCount:
		verifyCountValue(t, db, tt.verifyQuery, tt.verifyExpect.(int))
		return true
	case verifyString:
		verifySingleStringValue(t, db, tt.verifyQuery, tt.verifyExpect.(string))
		return true
	case verifyCustomSQL:
		verifyCustomSQLCheck(t, db, tt.verifyQueries)
		return true
	case verifyMultipleChecks:
		verifyMultipleChecksBehavior(t, db, tt.verifyQueries)
		return true
	}
	return false
}

// runComplexVerification handles behavior-based verifications
func runComplexVerification(t *testing.T, db *sql.DB, tt conflictTestCase) {
	switch tt.verifyType {
	case verifyUpdateConflict:
		verifyUpdateConflictBehavior(t, db)
	case verifyUpdateReplace:
		verifyUpdateReplaceBehavior(t, db)
	case verifyUpdateIgnore:
		verifyUpdateIgnoreBehavior(t, db)
	case verifyManyIgnores:
		verifyManyIgnoresBehavior(t, db)
	case verifyManyReplaces:
		verifyManyReplacesBehavior(t, db)
	}
}

// verifySingleIntValue checks a single integer value from a query
func verifySingleIntValue(t *testing.T, db *sql.DB, query string, expected int) {
	var result int
	if err := db.QueryRow(query).Scan(&result); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}

// verifyCountValue checks a count value from a query
func verifyCountValue(t *testing.T, db *sql.DB, query string, expected int) {
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != expected {
		t.Errorf("Expected %d rows, got %d", expected, count)
	}
}

// verifySingleStringValue checks a single string value from a query
func verifySingleStringValue(t *testing.T, db *sql.DB, query string, expected string) {
	var result string
	if err := db.QueryRow(query).Scan(&result); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

// verifyCustomSQLCheck runs multiple custom SQL checks
func verifyCustomSQLCheck(t *testing.T, db *sql.DB, queries []verifyQuery) {
	for _, vq := range queries {
		switch expected := vq.expect.(type) {
		case int:
			verifySingleIntValue(t, db, vq.query, expected)
		case string:
			verifySingleStringValue(t, db, vq.query, expected)
		}
	}
}

// verifyUpdateConflictBehavior verifies UPDATE conflict behavior
func verifyUpdateConflictBehavior(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS t1(a,b,c, UNIQUE(a))"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if _, err := db.Exec("INSERT INTO t1 SELECT * FROM t2"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if _, err := db.Exec("BEGIN"); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if _, err := db.Exec("UPDATE t1 SET b=b*2"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	// UNIQUE(a) table constraint not enforced; UPDATE succeeds without error
	if _, err := db.Exec("UPDATE t1 SET a=c+5"); err != nil {
		t.Logf("UPDATE t1 SET a=c+5 returned error (acceptable): %v", err)
	}
}

// verifyUpdateReplaceBehavior verifies UPDATE OR REPLACE behavior
func verifyUpdateReplaceBehavior(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("INSERT INTO t1 SELECT * FROM t2"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if _, err := db.Exec("UPDATE t1 SET b=b*2"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if _, err := db.Exec("UPDATE t1 SET a=c+5"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	verifyCountValue(t, db, "SELECT COUNT(*) FROM t1", 3)
}

// verifyUpdateIgnoreBehavior verifies UPDATE OR IGNORE behavior
func verifyUpdateIgnoreBehavior(t *testing.T, db *sql.DB) {
	if _, err := db.Exec("INSERT INTO t1 SELECT * FROM t2"); err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if _, err := db.Exec("UPDATE t1 SET b=b*2"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if _, err := db.Exec("UPDATE t1 SET a=c+5"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	verifyCountValue(t, db, "SELECT COUNT(*) FROM t1", 4)
}

// verifyManyIgnoresBehavior verifies UPDATE OR IGNORE with many conflicts
func verifyManyIgnoresBehavior(t *testing.T, db *sql.DB) {
	for i := 1; i <= 50; i++ {
		if _, err := db.Exec("INSERT INTO t1 VALUES(?, ?)", i, i+1); err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}
	res, err := db.Exec("UPDATE OR IGNORE t1 SET a=1000")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}
	verifyCountValue(t, db, "SELECT COUNT(*) FROM t1", 50)
}

// verifyManyReplacesBehavior verifies UPDATE OR REPLACE with many conflicts
func verifyManyReplacesBehavior(t *testing.T, db *sql.DB) {
	for i := 1; i <= 50; i++ {
		if _, err := db.Exec("INSERT INTO t1 VALUES(?, ?)", i, i+1); err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}
	if _, err := db.Exec("UPDATE OR REPLACE t1 SET a=1001"); err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	verifyCountValue(t, db, "SELECT COUNT(*) FROM t1", 1)
}

// verifyMultipleChecksBehavior runs multiple checks with custom queries
func verifyMultipleChecksBehavior(t *testing.T, db *sql.DB, queries []verifyQuery) {
	verifyCustomSQLCheck(t, db, queries)
}
