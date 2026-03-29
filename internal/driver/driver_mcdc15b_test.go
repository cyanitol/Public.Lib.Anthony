// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// drv15bOpen opens an in-memory sqlite_internal database or fatals.
func drv15bOpen(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// drv15bExec executes SQL and fatals on error.
func drv15bExec(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// drv15bQueryInt runs a single-int query and returns the value.
func drv15bQueryInt(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

// TestMCDC15b_Vacuum_Basic exercises executeVacuum on a non-empty in-memory database
// (opts.IntoFile == "", btree != nil) covering the persistSchemaAfterVacuum branch.
func TestMCDC15b_Vacuum_Basic(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	drv15bExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 0; i < 10; i++ {
		drv15bExec(t, db, "INSERT INTO items (val) VALUES (?)", "v")
	}
	drv15bExec(t, db, "DELETE FROM items")

	// VACUUM should succeed.
	if _, err := db.Exec("VACUUM"); err != nil {
		t.Fatalf("VACUUM: %v", err)
	}
}

// TestMCDC15b_Vacuum_AfterDDL exercises executeVacuum after schema-changing DDL.
func TestMCDC15b_Vacuum_AfterDDL(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	drv15bExec(t, db, "CREATE TABLE t1 (x INTEGER)")
	drv15bExec(t, db, "INSERT INTO t1 VALUES (1)")
	drv15bExec(t, db, "DROP TABLE t1")

	if _, err := db.Exec("VACUUM"); err != nil {
		t.Fatalf("VACUUM after DROP TABLE: %v", err)
	}
}

// TestMCDC15b_CountExpr_Literal covers loadCountValueReg with a COUNT(literal) arg,
// which goes through the gen.GenerateExpr path and returns (exprReg, skipAddr).
func TestMCDC15b_CountExpr_Literal(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	drv15bExec(t, db, "CREATE TABLE nums (n INTEGER)")
	for i := 1; i <= 5; i++ {
		drv15bExec(t, db, "INSERT INTO nums (n) VALUES (?)", i)
	}

	// COUNT(1) uses a literal argument through loadCountValueReg's GenerateExpr path.
	n := drv15bQueryInt(t, db, "SELECT COUNT(1) FROM nums")
	if n != 5 {
		t.Errorf("COUNT(1) = %d, want 5", n)
	}
}

// TestMCDC15b_CountExpr_Arithmetic covers loadCountValueReg with an expression arg.
func TestMCDC15b_CountExpr_Arithmetic(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	drv15bExec(t, db, "CREATE TABLE data (x INTEGER, y INTEGER)")
	drv15bExec(t, db, "INSERT INTO data VALUES (1, 2)")
	drv15bExec(t, db, "INSERT INTO data VALUES (3, 4)")
	drv15bExec(t, db, "INSERT INTO data VALUES (NULL, 5)")

	// COUNT with column expression — non-NULL rows counted.
	n := drv15bQueryInt(t, db, "SELECT COUNT(x) FROM data")
	if n != 2 {
		t.Errorf("COUNT(x) = %d, want 2", n)
	}
}

// TestMCDC15b_CountExpr_NullSkip ensures loadCountValueReg handles NULL via IsNull skip.
func TestMCDC15b_CountExpr_NullSkip(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	drv15bExec(t, db, "CREATE TABLE vals (v TEXT)")
	drv15bExec(t, db, "INSERT INTO vals VALUES (NULL)")
	drv15bExec(t, db, "INSERT INTO vals VALUES ('a')")
	drv15bExec(t, db, "INSERT INTO vals VALUES (NULL)")

	n := drv15bQueryInt(t, db, "SELECT COUNT(v) FROM vals")
	if n != 1 {
		t.Errorf("COUNT(v) with NULLs = %d, want 1", n)
	}
}

// TestMCDC15b_CreateMemoryConn_ForeignKeys exercises createMemoryConnection with
// a config that includes foreign_keys pragma, covering the applyConfig code path
// that issues PRAGMAs against the newly-created memory connection.
func TestMCDC15b_CreateMemoryConn_ForeignKeys(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite_internal", ":memory:?foreign_keys=on")
	if err != nil {
		t.Fatalf("sql.Open with foreign_keys: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestMCDC15b_CreateMemoryConn_CacheSize exercises createMemoryConnection with a
// cache_size pragma, covering a second code path through applyConfig.
func TestMCDC15b_CreateMemoryConn_CacheSize(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite_internal", ":memory:?cache_size=200")
	if err != nil {
		t.Fatalf("sql.Open with cache_size: %v", err)
	}
	defer db.Close()

	drv15bExec(t, db, "CREATE TABLE x (id INTEGER)")
	n := drv15bQueryInt(t, db, "SELECT COUNT(*) FROM x")
	if n != 0 {
		t.Errorf("unexpected row count %d", n)
	}
}

// TestMCDC15b_TVF_JsonEach_IdentExpr covers resolveTVFValue IdentExpr branch
// via json_each() which returns columns (key, value, type, ...).
func TestMCDC15b_TVF_JsonEach_IdentExpr(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	rows, err := db.Query(`SELECT value FROM json_each('[1,2,3]')`)
	if err != nil {
		t.Fatalf("json_each query: %v", err)
	}
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		vals = append(vals, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(vals) != 3 {
		t.Errorf("expected 3 rows from json_each, got %d", len(vals))
	}
}

// TestMCDC15b_TVF_JsonEach_KeyColumn covers resolveTVFValue for the key column.
func TestMCDC15b_TVF_JsonEach_KeyColumn(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	rows, err := db.Query(`SELECT key FROM json_each('{"a":1,"b":2}')`)
	if err != nil {
		t.Fatalf("json_each key query: %v", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("expected 2 keys from json_each object, got %d", len(keys))
	}
}

// TestMCDC15b_TVF_JsonEach_WithWhere covers resolveTVFValue LiteralExpr branch
// via a WHERE clause filter against a TVF column compared to a literal.
func TestMCDC15b_TVF_JsonEach_WithWhere(t *testing.T) {
	t.Parallel()
	db := drv15bOpen(t)

	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM json_each('[10,20,30]') WHERE value > 15`).Scan(&count)
	if err != nil {
		t.Fatalf("json_each with WHERE: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows where value > 15, got %d", count)
	}
}
