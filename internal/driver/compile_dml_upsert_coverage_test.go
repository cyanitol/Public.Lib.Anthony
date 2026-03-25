// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func openUpsertDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func upsertExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestCompileDMLUpsert_ExcludedRef exercises replaceExcludedRefs and
// compileUpsertDoUpdate via INSERT ... ON CONFLICT(col) DO UPDATE SET col=excluded.col.
func TestCompileDMLUpsert_ExcludedRef(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE uex(id INTEGER PRIMARY KEY, val TEXT)`)
	upsertExec(t, db, `INSERT INTO uex VALUES(1, 'original')`)

	upsertExec(t, db,
		`INSERT INTO uex(id, val) VALUES(1, 'updated')
		 ON CONFLICT(id) DO UPDATE SET val = excluded.val`)

	var got string
	if err := db.QueryRow(`SELECT val FROM uex WHERE id=1`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != "updated" {
		t.Fatalf("want 'updated', got %q", got)
	}
}

// TestCompileDMLUpsert_ExcludedRefNewRow verifies that the INSERT OR IGNORE branch
// of compileUpsertDoUpdate inserts a new row when there is no conflict.
func TestCompileDMLUpsert_ExcludedRefNewRow(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE uexnew(id INTEGER PRIMARY KEY, val TEXT)`)

	upsertExec(t, db,
		`INSERT INTO uexnew(id, val) VALUES(42, 'fresh')
		 ON CONFLICT(id) DO UPDATE SET val = excluded.val`)

	var got string
	if err := db.QueryRow(`SELECT val FROM uexnew WHERE id=42`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != "fresh" {
		t.Fatalf("want 'fresh', got %q", got)
	}
}

// TestCompileDMLUpsert_WhereOnExcluded exercises buildConflictWhere and the
// DO UPDATE WHERE path in buildUpsertUpdateStmt.
func TestCompileDMLUpsert_WhereOnExcluded(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE uwhere(id INTEGER PRIMARY KEY, val INTEGER)`)
	upsertExec(t, db, `INSERT INTO uwhere VALUES(1, 10)`)
	upsertExec(t, db, `INSERT INTO uwhere VALUES(2, 20)`)

	// Only update row where the incoming val > 15 (excluded.val > 15).
	// Row 1: incoming val=5 → condition false → no update (stays 10)
	// Row 2: incoming val=99 → condition true → updates to 99
	upsertExec(t, db,
		`INSERT INTO uwhere(id, val) VALUES(1, 5)
		 ON CONFLICT(id) DO UPDATE SET val = excluded.val WHERE excluded.val > 15`)
	upsertExec(t, db,
		`INSERT INTO uwhere(id, val) VALUES(2, 99)
		 ON CONFLICT(id) DO UPDATE SET val = excluded.val WHERE excluded.val > 15`)

	var v1, v2 int
	if err := db.QueryRow(`SELECT val FROM uwhere WHERE id=1`).Scan(&v1); err != nil {
		t.Fatalf("scan id=1: %v", err)
	}
	if err := db.QueryRow(`SELECT val FROM uwhere WHERE id=2`).Scan(&v2); err != nil {
		t.Fatalf("scan id=2: %v", err)
	}
	if v1 != 10 {
		t.Fatalf("row 1: want 10 (unchanged), got %d", v1)
	}
	if v2 != 99 {
		t.Fatalf("row 2: want 99 (updated), got %d", v2)
	}
}

// TestCompileDMLUpsert_DoNothing exercises compileUpsertDoNothing (INSERT OR IGNORE).
func TestCompileDMLUpsert_DoNothing(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE udn(id INTEGER PRIMARY KEY, val TEXT)`)
	upsertExec(t, db, `INSERT INTO udn VALUES(1, 'keep')`)

	upsertExec(t, db,
		`INSERT INTO udn(id, val) VALUES(1, 'ignored') ON CONFLICT DO NOTHING`)

	var got string
	if err := db.QueryRow(`SELECT val FROM udn WHERE id=1`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != "keep" {
		t.Fatalf("want 'keep' (unchanged), got %q", got)
	}

	// New row should still be inserted.
	upsertExec(t, db,
		`INSERT INTO udn(id, val) VALUES(2, 'new') ON CONFLICT DO NOTHING`)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM udn`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
}

// TestCompileDMLUpsert_InsertSelectMaterialise exercises insertSelectNeedsMaterialise
// via INSERT INTO t SELECT ... FROM t2 with a correlated/parameter-bearing WHERE.
// Using ORDER BY forces materialisation.
func TestCompileDMLUpsert_InsertSelectMaterialise(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE src(id INTEGER PRIMARY KEY, val INTEGER)`)
	upsertExec(t, db, `CREATE TABLE dst(id INTEGER PRIMARY KEY, val INTEGER)`)
	upsertExec(t, db, `INSERT INTO src VALUES(1, 10), (2, 20), (3, 30)`)

	// ORDER BY forces insertSelectNeedsMaterialise → true
	upsertExec(t, db, `INSERT INTO dst SELECT id, val FROM src ORDER BY val DESC`)

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM dst`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 rows, got %d", n)
	}
}

// TestCompileDMLUpsert_InsertSelectNoMaterialise exercises insertSelectNeedsMaterialise
// returning false (simple SELECT, no aggregates, no ORDER BY, no LIMIT, no DISTINCT).
func TestCompileDMLUpsert_InsertSelectNoMaterialise(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE s2(x INTEGER)`)
	upsertExec(t, db, `CREATE TABLE d2(x INTEGER)`)
	upsertExec(t, db, `INSERT INTO s2 VALUES(7), (8)`)

	// Simple SELECT: insertSelectNeedsMaterialise returns false
	upsertExec(t, db, `INSERT INTO d2 SELECT x FROM s2`)

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM d2`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
}

// TestCompileDMLUpsert_GeneratedColumn exercises generatedExprForColumn via an
// INSERT with a GENERATED ALWAYS AS column specified in the column list.
func TestCompileDMLUpsert_GeneratedColumn(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db,
		`CREATE TABLE gentest(a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 3) STORED)`)

	// Insert specifying only 'a'; 'b' is generated.
	upsertExec(t, db, `INSERT INTO gentest(a) VALUES(5)`)

	var a int
	if err := db.QueryRow(`SELECT a FROM gentest`).Scan(&a); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if a != 5 {
		t.Fatalf("want a=5, got %d", a)
	}
}

// TestCompileDMLUpsert_DefaultValue exercises defaultExprForColumn via an INSERT
// that omits a column that has a DEFAULT value.
func TestCompileDMLUpsert_DefaultValue(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db,
		`CREATE TABLE deftest(id INTEGER PRIMARY KEY, status TEXT DEFAULT 'active', score INTEGER)`)

	// Insert without specifying 'status' — should use DEFAULT 'active'.
	upsertExec(t, db, `INSERT INTO deftest(id, score) VALUES(1, 42)`)

	var status string
	if err := db.QueryRow(`SELECT status FROM deftest WHERE id=1`).Scan(&status); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "active" {
		t.Fatalf("want 'active', got %q", status)
	}
}

// TestCompileDMLUpsert_DefaultValueNull exercises defaultExprForColumn returning
// NULL when no default is specified for an omitted column.
func TestCompileDMLUpsert_DefaultValueNull(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db,
		`CREATE TABLE defnull(id INTEGER PRIMARY KEY, notes TEXT)`)

	// Insert without specifying 'notes' — should be NULL.
	upsertExec(t, db, `INSERT INTO defnull(id) VALUES(1)`)

	var notes sql.NullString
	if err := db.QueryRow(`SELECT notes FROM defnull WHERE id=1`).Scan(&notes); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if notes.Valid {
		t.Fatalf("want NULL for notes, got %q", notes.String)
	}
}

// TestCompileDMLUpsert_InsertReturning exercises emitReturningRow via
// INSERT ... RETURNING col1, col2.
func TestCompileDMLUpsert_InsertReturning(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE rettest(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)`)

	rows, err := db.Query(
		`INSERT INTO rettest(id, name, score) VALUES(1, 'alice', 99) RETURNING id, name`)
	if err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	defer rows.Close()

	var gotID int
	var gotName string
	if !rows.Next() {
		t.Fatal("expected one returned row")
	}
	if err := rows.Scan(&gotID, &gotName); err != nil {
		t.Fatalf("scan RETURNING row: %v", err)
	}
	if gotID != 1 {
		t.Fatalf("RETURNING id: want 1, got %d", gotID)
	}
	if gotName != "alice" {
		t.Fatalf("RETURNING name: want 'alice', got %q", gotName)
	}
	if rows.Next() {
		t.Fatal("expected only one RETURNING row")
	}
}

// TestCompileDMLUpsert_UpdateReturning exercises emitReturningRow via
// UPDATE ... RETURNING col.
func TestCompileDMLUpsert_UpdateReturning(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE upret(id INTEGER PRIMARY KEY, val INTEGER)`)
	upsertExec(t, db, `INSERT INTO upret VALUES(1, 10), (2, 20)`)

	rows, err := db.Query(
		`UPDATE upret SET val = val + 5 WHERE id = 1 RETURNING val`)
	if err != nil {
		t.Fatalf("UPDATE RETURNING: %v", err)
	}
	defer rows.Close()

	var gotVal int
	if !rows.Next() {
		t.Fatal("expected one returned row from UPDATE RETURNING")
	}
	if err := rows.Scan(&gotVal); err != nil {
		t.Fatalf("scan RETURNING: %v", err)
	}
	if gotVal != 15 {
		t.Fatalf("RETURNING val: want 15, got %d", gotVal)
	}
	if rows.Next() {
		t.Fatal("expected only one RETURNING row")
	}
}

// TestCompileDMLUpsert_ArithmeticOnExcluded exercises replaceExcludedRefs with a
// BinaryExpr involving arithmetic on excluded columns.
func TestCompileDMLUpsert_ArithmeticOnExcluded(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE arith(id INTEGER PRIMARY KEY, total INTEGER)`)
	upsertExec(t, db, `INSERT INTO arith VALUES(1, 100)`)

	// total = excluded.total * 2 + 10
	upsertExec(t, db,
		`INSERT INTO arith(id, total) VALUES(1, 50)
		 ON CONFLICT(id) DO UPDATE SET total = excluded.total * 2 + 10`)

	var total int
	if err := db.QueryRow(`SELECT total FROM arith WHERE id=1`).Scan(&total); err != nil {
		t.Fatalf("scan: %v", err)
	}
	// 50 * 2 + 10 = 110
	if total != 110 {
		t.Fatalf("want 110, got %d", total)
	}
}

// TestCompileDMLUpsert_CompositeConflictTarget exercises buildConflictWhere with
// a composite conflict target (two columns).
func TestCompileDMLUpsert_CompositeConflictTarget(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db,
		`CREATE TABLE composite(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a, b))`)
	upsertExec(t, db, `INSERT INTO composite VALUES(1, 2, 'original')`)

	// Conflict on composite key (a, b)
	upsertExec(t, db,
		`INSERT INTO composite(a, b, val) VALUES(1, 2, 'replaced')
		 ON CONFLICT(a, b) DO UPDATE SET val = excluded.val`)

	var got string
	if err := db.QueryRow(
		`SELECT val FROM composite WHERE a=1 AND b=2`).Scan(&got); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != "replaced" {
		t.Fatalf("want 'replaced', got %q", got)
	}

	// New composite key should insert cleanly.
	upsertExec(t, db,
		`INSERT INTO composite(a, b, val) VALUES(3, 4, 'newrow')
		 ON CONFLICT(a, b) DO UPDATE SET val = excluded.val`)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM composite`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 rows, got %d", n)
	}
}

// TestCompileDMLUpsert_UpdateColumnValueGenerated exercises emitUpdateColumnValue
// for the generated column branch by updating a table with a generated column.
func TestCompileDMLUpsert_UpdateColumnValueGenerated(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db,
		`CREATE TABLE gencol(id INTEGER PRIMARY KEY, base INTEGER, derived INTEGER GENERATED ALWAYS AS (base + 1) STORED)`)
	upsertExec(t, db, `INSERT INTO gencol(id, base) VALUES(1, 10)`)

	// UPDATE triggers emitUpdateColumnValue for the generated column path.
	upsertExec(t, db, `UPDATE gencol SET base = 20 WHERE id = 1`)

	var base int
	if err := db.QueryRow(`SELECT base FROM gencol WHERE id=1`).Scan(&base); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if base != 20 {
		t.Fatalf("want base=20, got %d", base)
	}
}

// TestCompileDMLUpsert_UpdateColumnValueExisting exercises the OpColumn branch of
// emitUpdateColumnValue where an un-updated column reads its existing value.
func TestCompileDMLUpsert_UpdateColumnValueExisting(t *testing.T) {
	db := openUpsertDB(t)
	upsertExec(t, db, `CREATE TABLE partial(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)`)
	upsertExec(t, db, `INSERT INTO partial VALUES(1, 5, 99)`)

	// Only update 'a'; 'b' should keep its existing value via OpColumn.
	upsertExec(t, db, `UPDATE partial SET a = 50 WHERE id = 1`)

	var a, b int
	if err := db.QueryRow(`SELECT a, b FROM partial WHERE id=1`).Scan(&a, &b); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if a != 50 {
		t.Fatalf("want a=50, got %d", a)
	}
	if b != 99 {
		t.Fatalf("want b=99 (unchanged), got %d", b)
	}
}
