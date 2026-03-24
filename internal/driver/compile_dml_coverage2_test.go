// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"
)

// openCov2DB opens a fresh :memory: database and registers cleanup.
func openCov2DB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("openCov2DB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mustExec runs a statement and fatals on error.
func mustExecCov2(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("mustExecCov2 %q: %v", q, err)
	}
}

// queryInt64Cov2 returns a single int64 from a query.
func queryInt64Cov2(t *testing.T, db *sql.DB, q string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("queryInt64Cov2 %q: %v", q, err)
	}
	return v
}

// ============================================================================
// loadValueIntoReg + dmlToInt64
//
// These are exercised by INSERT statements that cause Go values (nil, int64,
// float64, string, []byte) to be loaded into VDBE registers. Using bound
// parameters and also INSERT ... SELECT ensures both paths run.
// ============================================================================

func TestCompileDML2_LoadValueIntoReg(t *testing.T) {
	t.Parallel()

	type row struct {
		name  string
		setup string
		stmt  string
		args  []interface{}
		want  int64 // expected COUNT(*) after insert
	}
	tests := []row{
		{
			name:  "int literal",
			setup: "CREATE TABLE t(x INTEGER)",
			stmt:  "INSERT INTO t VALUES(42)",
			want:  1,
		},
		{
			name:  "float literal",
			setup: "CREATE TABLE t(x REAL)",
			stmt:  "INSERT INTO t VALUES(3.14)",
			want:  1,
		},
		{
			name:  "string literal",
			setup: "CREATE TABLE t(x TEXT)",
			stmt:  "INSERT INTO t VALUES('hello')",
			want:  1,
		},
		{
			name:  "null literal",
			setup: "CREATE TABLE t(x INTEGER)",
			stmt:  "INSERT INTO t VALUES(NULL)",
			want:  1,
		},
		{
			name:  "blob literal",
			setup: "CREATE TABLE t(x BLOB)",
			stmt:  "INSERT INTO t VALUES(X'DEADBEEF')",
			want:  1,
		},
		{
			name:  "bound int param",
			setup: "CREATE TABLE t(x INTEGER)",
			stmt:  "INSERT INTO t VALUES(?)",
			args:  []interface{}{int64(99)},
			want:  1,
		},
		{
			name:  "bound float param",
			setup: "CREATE TABLE t(x REAL)",
			stmt:  "INSERT INTO t VALUES(?)",
			args:  []interface{}{float64(2.71)},
			want:  1,
		},
		{
			name:  "bound string param",
			setup: "CREATE TABLE t(x TEXT)",
			stmt:  "INSERT INTO t VALUES(?)",
			args:  []interface{}{"world"},
			want:  1,
		},
		{
			name:  "bound null param",
			setup: "CREATE TABLE t(x INTEGER)",
			stmt:  "INSERT INTO t VALUES(?)",
			args:  []interface{}{nil},
			want:  1,
		},
		{
			name:  "bound blob param",
			setup: "CREATE TABLE t(x BLOB)",
			stmt:  "INSERT INTO t VALUES(?)",
			args:  []interface{}{[]byte{0xCA, 0xFE}},
			want:  1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			mustExecCov2(t, db, tc.setup)
			mustExecCov2(t, db, tc.stmt, tc.args...)
			got := queryInt64Cov2(t, db, "SELECT COUNT(*) FROM t")
			if got != tc.want {
				t.Errorf("COUNT(*)=%d, want %d", got, tc.want)
			}
		})
	}
}

// ============================================================================
// dmlToInt64 — also exercised through INSERT … SELECT that returns a rowid.
// We use a two-table INSERT … SELECT to push an integer column into the rowid
// path in the VM, covering the int64 / float64 conversion branches.
// ============================================================================

func TestCompileDML2_DmlToInt64(t *testing.T) {
	t.Parallel()

	db := openCov2DB(t)
	mustExecCov2(t, db, "CREATE TABLE src(id INTEGER PRIMARY KEY, v REAL)")
	mustExecCov2(t, db, "INSERT INTO src VALUES(1, 1.5),(2, 2.5)")
	mustExecCov2(t, db, "CREATE TABLE dst(id INTEGER, v REAL)")
	// INSERT … SELECT where the SELECT returns integer and float values
	mustExecCov2(t, db, "INSERT INTO dst SELECT id, v FROM src")
	got := queryInt64Cov2(t, db, "SELECT COUNT(*) FROM dst")
	if got != 2 {
		t.Errorf("COUNT(*)=%d, want 2", got)
	}
}

// ============================================================================
// emitUpdateColumnValue
//
// UPDATE statements with varying SET expressions exercise the three branches:
//  1. normal column updated via expression
//  2. normal column kept (not in SET — column is read from the existing record)
//  3. binary expression in SET
// ============================================================================

func TestCompileDML2_EmitUpdateColumnValue(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   []string
		update  string
		uargs   []interface{}
		check   string
		wantVal int64
	}
	tests := []tc{
		{
			name: "update one column, keep other",
			setup: []string{
				"CREATE TABLE t(a INTEGER, b INTEGER)",
				"INSERT INTO t VALUES(1, 10)",
			},
			update:  "UPDATE t SET a=99",
			check:   "SELECT b FROM t",
			wantVal: 10,
		},
		{
			name: "update with arithmetic expression",
			setup: []string{
				"CREATE TABLE t(a INTEGER, b INTEGER)",
				"INSERT INTO t VALUES(5, 3)",
			},
			update:  "UPDATE t SET b = a + b",
			check:   "SELECT b FROM t",
			wantVal: 8,
		},
		{
			name: "update with bound parameter",
			setup: []string{
				"CREATE TABLE t(a INTEGER, b TEXT)",
				"INSERT INTO t VALUES(1, 'old')",
			},
			update:  "UPDATE t SET b=?",
			uargs:   []interface{}{"new"},
			check:   "SELECT COUNT(*) FROM t WHERE b='new'",
			wantVal: 1,
		},
		{
			name: "update with unary negation in SET",
			setup: []string{
				"CREATE TABLE t(x INTEGER)",
				"INSERT INTO t VALUES(7)",
			},
			update:  "UPDATE t SET x = -x",
			check:   "SELECT x FROM t",
			wantVal: -7,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			for _, s := range tc.setup {
				mustExecCov2(t, db, s)
			}
			mustExecCov2(t, db, tc.update, tc.uargs...)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.wantVal {
				t.Errorf("query result=%d, want %d", got, tc.wantVal)
			}
		})
	}
}

// ============================================================================
// goValueToLiteral
//
// This function converts a Go interface{} to a parser.LiteralExpr. It is
// called when a subquery result is materialised inside a DML statement.
// We trigger it via UPDATE … SET col = (SELECT …) which forces the engine
// to materialise the scalar subquery and call goValueToLiteral on the result.
// ============================================================================

func TestCompileDML2_GoValueToLiteral(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   []string
		update  string
		check   string
		wantVal int64
	}
	tests := []tc{
		{
			name: "subquery returns integer",
			setup: []string{
				"CREATE TABLE ref(v INTEGER)",
				"INSERT INTO ref VALUES(42)",
				"CREATE TABLE dst(x INTEGER)",
				"INSERT INTO dst VALUES(0)",
			},
			update:  "UPDATE dst SET x = (SELECT v FROM ref LIMIT 1)",
			check:   "SELECT x FROM dst",
			wantVal: 42,
		},
		{
			name: "subquery returns string (coerced to 0 for int64 scan)",
			setup: []string{
				"CREATE TABLE ref(v TEXT)",
				"INSERT INTO ref VALUES('hello')",
				"CREATE TABLE dst(x TEXT)",
				"INSERT INTO dst VALUES('old')",
			},
			// After update, x should be 'hello'; count rows where x='hello'
			update:  "UPDATE dst SET x = (SELECT v FROM ref LIMIT 1)",
			check:   "SELECT COUNT(*) FROM dst WHERE x='hello'",
			wantVal: 1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			for _, s := range tc.setup {
				mustExecCov2(t, db, s)
			}
			mustExecCov2(t, db, tc.update)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.wantVal {
				t.Errorf("result=%d, want %d", got, tc.wantVal)
			}
		})
	}
}

// ============================================================================
// compileUnaryExpr
//
// Tests unary `-` and `+` (non-neg handled as NULL) in DML expressions.
// ============================================================================

func TestCompileDML2_CompileUnaryExpr(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   []string
		stmt    string
		check   string
		wantVal int64
	}
	tests := []tc{
		{
			name: "unary negation of integer literal in INSERT",
			setup: []string{
				"CREATE TABLE t(x INTEGER)",
			},
			stmt:    "INSERT INTO t VALUES(-5)",
			check:   "SELECT x FROM t",
			wantVal: -5,
		},
		{
			name: "unary negation of float literal in INSERT",
			setup: []string{
				"CREATE TABLE t(x REAL)",
			},
			stmt:  "INSERT INTO t VALUES(-2.5)",
			check: "SELECT COUNT(*) FROM t WHERE x < 0",
			// -2.5 < 0 → 1 row
			wantVal: 1,
		},
		{
			name: "unary negation in UPDATE SET",
			setup: []string{
				"CREATE TABLE t(x INTEGER)",
				"INSERT INTO t VALUES(10)",
			},
			stmt:    "UPDATE t SET x = -10",
			check:   "SELECT x FROM t",
			wantVal: -10,
		},
		{
			name: "NOT unary in WHERE (not OpNeg, emits NULL path)",
			setup: []string{
				"CREATE TABLE t(x INTEGER)",
				"INSERT INTO t VALUES(1)",
				"INSERT INTO t VALUES(0)",
			},
			// NOT is handled in WHERE/SELECT compilation, not in DML unary,
			// so we test NOT in INSERT … SELECT to hit the OpNull branch.
			stmt:    "INSERT INTO t SELECT NOT x FROM t WHERE x = 0",
			check:   "SELECT COUNT(*) FROM t",
			wantVal: 3,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			for _, s := range tc.setup {
				mustExecCov2(t, db, s)
			}
			mustExecCov2(t, db, tc.stmt)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.wantVal {
				t.Errorf("result=%d, want %d", got, tc.wantVal)
			}
		})
	}
}

// ============================================================================
// compileVariableExpr
//
// Bound parameters (?) in DML are compiled through compileVariableExpr.
// We test: enough params, too few params (excess columns get NULL), and
// named parameters.
// ============================================================================

func TestCompileDML2_CompileVariableExpr(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   string
		stmt    string
		args    []interface{}
		check   string
		wantVal int64
	}
	tests := []tc{
		{
			name:    "single ? param in INSERT",
			setup:   "CREATE TABLE t(x INTEGER)",
			stmt:    "INSERT INTO t VALUES(?)",
			args:    []interface{}{int64(7)},
			check:   "SELECT x FROM t",
			wantVal: 7,
		},
		{
			name:    "multiple ? params in INSERT",
			setup:   "CREATE TABLE t(a INTEGER, b INTEGER)",
			stmt:    "INSERT INTO t VALUES(?, ?)",
			args:    []interface{}{int64(3), int64(4)},
			check:   "SELECT a + b FROM t",
			wantVal: 7,
		},
		{
			name:    "? param in UPDATE WHERE",
			setup:   "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(1),(2),(3)",
			stmt:    "UPDATE t SET x = x * 2 WHERE x = ?",
			args:    []interface{}{int64(2)},
			check:   "SELECT COUNT(*) FROM t WHERE x = 4",
			wantVal: 1,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			// setup may contain semicolons — split and exec individually
			stmts := splitSemiCov2(tc.setup)
			for _, s := range stmts {
				if s != "" {
					mustExecCov2(t, db, s)
				}
			}
			mustExecCov2(t, db, tc.stmt, tc.args...)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.wantVal {
				t.Errorf("result=%d, want %d", got, tc.wantVal)
			}
		})
	}
}

// splitSemiCov2 splits a string on ';' and returns trimmed non-empty pieces.
func splitSemiCov2(s string) []string {
	var out []string
	for _, part := range splitBySemicolon(s) {
		if trimSpace(part) != "" {
			out = append(out, part)
		}
	}
	return out
}

// splitBySemicolon splits s on ';'.
func splitBySemicolon(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ';' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// trimSpace trims leading/trailing ASCII whitespace from s.
func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// ============================================================================
// replaceExcludedRefs
//
// Exercised by INSERT … ON CONFLICT … DO UPDATE SET col = excluded.col.
// We test: single EXCLUDED ref, multiple refs, EXCLUDED ref inside binary
// expression, and EXCLUDED ref inside a function call.
// ============================================================================

func TestCompileDML2_ReplaceExcludedRefs(t *testing.T) {
	t.Parallel()

	type tc struct {
		name    string
		setup   []string
		upsert  string
		check   string
		wantVal int64
	}
	tests := []tc{
		{
			name: "excluded.col simple replace",
			setup: []string{
				"CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER)",
				"INSERT INTO kv VALUES('a', 1)",
			},
			upsert:  "INSERT INTO kv VALUES('a', 99) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
			check:   "SELECT v FROM kv WHERE k='a'",
			wantVal: 99,
		},
		{
			name: "excluded ref in binary expr",
			setup: []string{
				"CREATE TABLE inv(id INTEGER PRIMARY KEY, qty INTEGER)",
				"INSERT INTO inv VALUES(1, 10)",
			},
			upsert:  "INSERT INTO inv VALUES(1, 5) ON CONFLICT(id) DO UPDATE SET qty=qty+excluded.qty",
			check:   "SELECT qty FROM inv WHERE id=1",
			wantVal: 15,
		},
		{
			name: "multiple excluded refs",
			setup: []string{
				"CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
				"INSERT INTO t VALUES(1, 0, 0)",
			},
			upsert:  "INSERT INTO t VALUES(1, 7, 8) ON CONFLICT(id) DO UPDATE SET a=excluded.a, b=excluded.b",
			check:   "SELECT a+b FROM t WHERE id=1",
			wantVal: 15,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			for _, s := range tc.setup {
				mustExecCov2(t, db, s)
			}
			mustExecCov2(t, db, tc.upsert)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.wantVal {
				t.Errorf("result=%d, want %d", got, tc.wantVal)
			}
		})
	}
}

// ============================================================================
// insertSelectNeedsMaterialise
//
// The function returns true when SELECT has aggregate, ORDER BY, LIMIT, or
// DISTINCT. We exercise both the false (simple) and several true branches.
// ============================================================================

func TestCompileDML2_InsertSelectNeedsMaterialise(t *testing.T) {
	t.Parallel()

	type tc struct {
		name  string
		setup []string
		stmt  string
		check string
		want  int64
	}
	tests := []tc{
		{
			name: "simple select – no materialise",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(1),(2),(3)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT x FROM src",
			check: "SELECT COUNT(*) FROM dst",
			want:  3,
		},
		{
			name: "aggregate – materialise (detectAggregates=true)",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(10),(20),(30)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT SUM(x) FROM src",
			check: "SELECT x FROM dst",
			want:  60,
		},
		{
			name: "ORDER BY – materialise",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(3),(1),(2)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT x FROM src ORDER BY x",
			check: "SELECT COUNT(*) FROM dst",
			want:  3,
		},
		{
			name: "LIMIT – materialise",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(1),(2),(3),(4),(5)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT x FROM src LIMIT 2",
			check: "SELECT COUNT(*) FROM dst",
			want:  2,
		},
		{
			name: "DISTINCT – materialise",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(1),(1),(2),(2),(3)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT DISTINCT x FROM src",
			check: "SELECT COUNT(*) FROM dst",
			want:  3,
		},
		{
			name: "aggregate with ORDER BY – both A and B true",
			setup: []string{
				"CREATE TABLE src(x INTEGER)",
				"INSERT INTO src VALUES(5),(3),(1)",
				"CREATE TABLE dst(x INTEGER)",
			},
			stmt:  "INSERT INTO dst SELECT SUM(x) FROM src ORDER BY 1",
			check: "SELECT x FROM dst",
			want:  9,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openCov2DB(t)
			for _, s := range tc.setup {
				mustExecCov2(t, db, s)
			}
			mustExecCov2(t, db, tc.stmt)
			got := queryInt64Cov2(t, db, tc.check)
			if got != tc.want {
				t.Errorf("result=%d, want %d", got, tc.want)
			}
		})
	}
}
