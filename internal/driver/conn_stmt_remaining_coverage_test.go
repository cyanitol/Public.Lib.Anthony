// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// csrOpenMem opens an in-memory database for remaining coverage tests.
func csrOpenMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("csrOpenMem: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// csrExec executes one or more SQL statements, fataling on any error.
func csrExec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("csrExec %q: %v", s, err)
		}
	}
}

// csrQueryInt64 runs a single-column integer query and returns the value.
func csrQueryInt64(t *testing.T, db *sql.DB, query string, args ...interface{}) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("csrQueryInt64 %q: %v", query, err)
	}
	return v
}

// csrQueryAllInt64 returns all rows from a single-column integer query.
func csrQueryAllInt64(t *testing.T, db *sql.DB, query string) []int64 {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("csrQueryAllInt64 %q: %v", query, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("csrQueryAllInt64 scan: %v", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("csrQueryAllInt64 rows.Err: %v", err)
	}
	return out
}

// ============================================================================
// Connection-level operations: multiple connections to same file-based DB
// ============================================================================

// TestConnStmtRemaining_MultiConnSameDB verifies that a second connection
// opened to the same file-based database sees data committed by the first.
// Connections are opened sequentially (the driver uses exclusive file access).
func TestConnStmtRemaining_MultiConnSameDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := fmt.Sprintf("%s/shared.db", dir)

	// First connection: create schema and insert data.
	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	db1.SetMaxOpenConns(1)
	csrExec(t, db1,
		"CREATE TABLE items (id INTEGER, label TEXT)",
		"INSERT INTO items VALUES (1, 'alpha')",
		"INSERT INTO items VALUES (2, 'beta')",
	)
	db1.Close()

	// Second connection: must see the committed data.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	count := csrQueryInt64(t, db2, "SELECT COUNT(*) FROM items")
	if count != 2 {
		t.Errorf("db2 saw %d rows, want 2", count)
	}
}

// TestConnStmtRemaining_ConnectionReuse exercises the database/sql pool
// by running many short queries so connections are reused across requests.
func TestConnStmtRemaining_ConnectionReuse(t *testing.T) {
	db := csrOpenMem(t)

	csrExec(t, db,
		"CREATE TABLE vals (n INTEGER)",
	)
	for i := 0; i < 20; i++ {
		csrExec(t, db, fmt.Sprintf("INSERT INTO vals VALUES (%d)", i))
	}

	// Run many queries in sequence; the pool will reuse connections,
	// exercising ResetSession and Ping code paths.
	for i := 0; i < 30; i++ {
		got := csrQueryInt64(t, db, "SELECT COUNT(*) FROM vals")
		if got != 20 {
			t.Fatalf("iter %d: got %d rows, want 20", i, got)
		}
	}
}

// TestConnStmtRemaining_MultiConnTransaction verifies that committed transaction
// data is visible to a subsequent connection opened to the same file.
func TestConnStmtRemaining_MultiConnTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := fmt.Sprintf("%s/txn.db", dir)

	// Connection 1: set up table, run a transaction, then close.
	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	db1.SetMaxOpenConns(1)
	csrExec(t, db1, "CREATE TABLE counters (v INTEGER)")
	csrExec(t, db1, "INSERT INTO counters VALUES (0)")

	tx, err := db1.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("UPDATE counters SET v = 42"); err != nil {
		tx.Rollback()
		t.Fatalf("update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	db1.Close()

	// Connection 2: must see the committed value.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	got := csrQueryInt64(t, db2, "SELECT v FROM counters")
	if got != 42 {
		t.Errorf("db2 v = %d, want 42", got)
	}
}

// TestConnStmtRemaining_MultiConnRollback verifies that a rolled-back
// transaction leaves no visible effect when a subsequent connection reads the DB.
func TestConnStmtRemaining_MultiConnRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := fmt.Sprintf("%s/rollback.db", dir)

	// Connection 1: set up data, start a transaction, insert, rollback, then close.
	db1, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db1: %v", err)
	}
	db1.SetMaxOpenConns(1)
	csrExec(t, db1, "CREATE TABLE rolltest (x INTEGER)")
	csrExec(t, db1, "INSERT INTO rolltest VALUES (1)")

	tx, err := db1.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("INSERT INTO rolltest VALUES (2)"); err != nil {
		tx.Rollback()
		t.Fatalf("insert in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	db1.Close()

	// Connection 2: only the pre-transaction row should be visible.
	db2, err := sql.Open("sqlite_internal", dbPath)
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()
	db2.SetMaxOpenConns(1)

	count := csrQueryInt64(t, db2, "SELECT COUNT(*) FROM rolltest")
	if count != 1 {
		t.Errorf("after rollback db2 sees %d rows, want 1", count)
	}
}

// ============================================================================
// Statement caching behavior
// ============================================================================

// TestConnStmtRemaining_StmtCacheHit verifies that preparing the same SQL
// multiple times returns valid statements (exercising cache hit path).
func TestConnStmtRemaining_StmtCacheHit(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE cache_t (id INTEGER, name TEXT)",
		"INSERT INTO cache_t VALUES (1, 'one')",
		"INSERT INTO cache_t VALUES (2, 'two')",
	)

	query := "SELECT COUNT(*) FROM cache_t"

	// Prepare and execute the same statement multiple times.
	for i := 0; i < 5; i++ {
		stmt, err := db.Prepare(query)
		if err != nil {
			t.Fatalf("iter %d prepare: %v", i, err)
		}
		var count int64
		if err := stmt.QueryRow().Scan(&count); err != nil {
			stmt.Close()
			t.Fatalf("iter %d query: %v", i, err)
		}
		stmt.Close()
		if count != 2 {
			t.Errorf("iter %d: got %d, want 2", i, count)
		}
	}
}

// TestConnStmtRemaining_StmtCacheInvalidation verifies that DDL operations
// do not corrupt cached statements — subsequent queries after DDL still work.
func TestConnStmtRemaining_StmtCacheInvalidation(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE inval (x INTEGER)",
		"INSERT INTO inval VALUES (10)",
	)

	// Prime any caches with a count query.
	got := csrQueryInt64(t, db, "SELECT COUNT(*) FROM inval")
	if got != 1 {
		t.Fatalf("initial count = %d, want 1", got)
	}

	// Perform DDL that should invalidate cached entries.
	csrExec(t, db, "CREATE TABLE inval2 (y INTEGER)")
	csrExec(t, db, "INSERT INTO inval VALUES (20)")

	// Repeat the original query — must still work after schema change.
	got = csrQueryInt64(t, db, "SELECT COUNT(*) FROM inval")
	if got != 2 {
		t.Errorf("after DDL count = %d, want 2", got)
	}
}

// TestConnStmtRemaining_PreparedStmtReuse reuses a single prepared statement
// across many executions, exercising the statement lifecycle paths.
func TestConnStmtRemaining_PreparedStmtReuse(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db, "CREATE TABLE reuse (n INTEGER)")

	stmt, err := db.Prepare("INSERT INTO reuse VALUES (?)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()

	const rows = 10
	for i := 0; i < rows; i++ {
		if _, err := stmt.Exec(i); err != nil {
			t.Fatalf("exec %d: %v", i, err)
		}
	}

	count := csrQueryInt64(t, db, "SELECT COUNT(*) FROM reuse")
	if count != rows {
		t.Errorf("count = %d, want %d", count, rows)
	}
}

// ============================================================================
// Complex trigger scenarios
// ============================================================================

// TestConnStmtRemaining_TriggerWithOldNewValues verifies that a trigger
// can reference OLD and NEW values in its body.
func TestConnStmtRemaining_TriggerWithOldNewValues(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE accounts (id INTEGER, balance INTEGER)",
		"CREATE TABLE audit_log (account_id INTEGER, old_bal INTEGER, new_bal INTEGER)",
		`CREATE TRIGGER log_balance_change
			AFTER UPDATE ON accounts
			BEGIN
				INSERT INTO audit_log VALUES(NEW.id, OLD.balance, NEW.balance);
			END`,
		"INSERT INTO accounts VALUES (1, 100)",
		"UPDATE accounts SET balance = 200 WHERE id = 1",
	)

	rows, err := db.Query("SELECT account_id, old_bal, new_bal FROM audit_log")
	if err != nil {
		t.Fatalf("query audit_log: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one audit row, got none")
	}
	var aid, oldBal, newBal int64
	if err := rows.Scan(&aid, &oldBal, &newBal); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if aid != 1 || oldBal != 100 || newBal != 200 {
		t.Errorf("audit row = (%d, %d, %d), want (1, 100, 200)", aid, oldBal, newBal)
	}
}

// TestConnStmtRemaining_TriggerDeleteWithOld verifies that a BEFORE DELETE
// trigger can reference OLD values from the row being deleted.
func TestConnStmtRemaining_TriggerDeleteWithOld(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE items2 (id INTEGER, name TEXT)",
		"CREATE TABLE deleted_items (id INTEGER, name TEXT)",
		`CREATE TRIGGER archive_on_delete
			BEFORE DELETE ON items2
			BEGIN
				INSERT INTO deleted_items VALUES(OLD.id, OLD.name);
			END`,
		"INSERT INTO items2 VALUES (1, 'widget')",
		"INSERT INTO items2 VALUES (2, 'gadget')",
		"DELETE FROM items2 WHERE id = 1",
	)

	count := csrQueryInt64(t, db, "SELECT COUNT(*) FROM deleted_items")
	if count != 1 {
		t.Errorf("deleted_items count = %d, want 1", count)
	}

	var name string
	if err := db.QueryRow("SELECT name FROM deleted_items").Scan(&name); err != nil {
		t.Fatalf("scan deleted name: %v", err)
	}
	if name != "widget" {
		t.Errorf("deleted name = %q, want \"widget\"", name)
	}
}

// TestConnStmtRemaining_TriggerWhenClause verifies that a trigger with a
// WHEN clause only fires when the condition is satisfied.
func TestConnStmtRemaining_TriggerWhenClause(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE prices (id INTEGER, price INTEGER)",
		"CREATE TABLE high_price_log (id INTEGER, price INTEGER)",
		`CREATE TRIGGER log_high_price
			AFTER INSERT ON prices
			WHEN NEW.price > 100
			BEGIN
				INSERT INTO high_price_log VALUES(NEW.id, NEW.price);
			END`,
		"INSERT INTO prices VALUES (1, 50)",   // below threshold — no trigger
		"INSERT INTO prices VALUES (2, 150)",  // above threshold — fires
		"INSERT INTO prices VALUES (3, 200)",  // above threshold — fires
	)

	count := csrQueryInt64(t, db, "SELECT COUNT(*) FROM high_price_log")
	if count != 2 {
		t.Errorf("high_price_log count = %d, want 2", count)
	}
}

// TestConnStmtRemaining_TriggerCascadeInsert verifies a trigger chain where
// an insert into table A triggers an insert into table B.
func TestConnStmtRemaining_TriggerCascadeInsert(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE primary_tbl (id INTEGER, val TEXT)",
		"CREATE TABLE secondary_tbl (primary_id INTEGER, info TEXT)",
		`CREATE TRIGGER cascade_insert
			AFTER INSERT ON primary_tbl
			BEGIN
				INSERT INTO secondary_tbl VALUES(NEW.id, 'auto-created');
			END`,
		"INSERT INTO primary_tbl VALUES (1, 'first')",
		"INSERT INTO primary_tbl VALUES (2, 'second')",
	)

	count := csrQueryInt64(t, db, "SELECT COUNT(*) FROM secondary_tbl")
	if count != 2 {
		t.Errorf("secondary_tbl count = %d, want 2", count)
	}
}

// TestConnStmtRemaining_TriggerUpdateOf exercises the UPDATE OF column-list
// trigger variation, where the trigger only fires when specific columns change.
func TestConnStmtRemaining_TriggerUpdateOf(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)",
		"CREATE TABLE salary_changes (emp_id INTEGER, new_salary INTEGER)",
		`CREATE TRIGGER track_salary
			AFTER UPDATE OF salary ON employees
			BEGIN
				INSERT INTO salary_changes VALUES(NEW.id, NEW.salary);
			END`,
		"INSERT INTO employees VALUES (1, 'Alice', 50000)",
		"UPDATE employees SET name = 'Alice Smith' WHERE id = 1", // no salary change
		"UPDATE employees SET salary = 60000 WHERE id = 1",       // salary change — fires
	)

	count := csrQueryInt64(t, db, "SELECT COUNT(*) FROM salary_changes")
	if count != 1 {
		t.Errorf("salary_changes count = %d, want 1", count)
	}

	var sal int64
	if err := db.QueryRow("SELECT new_salary FROM salary_changes").Scan(&sal); err != nil {
		t.Fatalf("scan salary: %v", err)
	}
	if sal != 60000 {
		t.Errorf("new_salary = %d, want 60000", sal)
	}
}

// TestConnStmtRemaining_TriggerMultipleBodyStmts exercises a trigger that has
// multiple statements in its body (exercising executeTriggerBody iteration).
func TestConnStmtRemaining_TriggerMultipleBodyStmts(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE source (id INTEGER)",
		"CREATE TABLE log1 (msg TEXT)",
		"CREATE TABLE log2 (msg TEXT)",
		`CREATE TRIGGER multi_body
			AFTER INSERT ON source
			BEGIN
				INSERT INTO log1 VALUES('log1_fired');
				INSERT INTO log2 VALUES('log2_fired');
			END`,
		"INSERT INTO source VALUES (1)",
	)

	c1 := csrQueryInt64(t, db, "SELECT COUNT(*) FROM log1")
	c2 := csrQueryInt64(t, db, "SELECT COUNT(*) FROM log2")
	if c1 != 1 || c2 != 1 {
		t.Errorf("log1=%d, log2=%d; want both 1", c1, c2)
	}
}

// ============================================================================
// Window function edge cases
// ============================================================================

// TestConnStmtRemaining_WindowDenseRankTies verifies DENSE_RANK behaviour
// with tied ORDER BY values (rank increments by 1, not by number of ties).
func TestConnStmtRemaining_WindowDenseRankTies(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE scores (name TEXT, score INTEGER)",
		"INSERT INTO scores VALUES ('Alice', 100)",
		"INSERT INTO scores VALUES ('Bob', 90)",
		"INSERT INTO scores VALUES ('Charlie', 90)",
		"INSERT INTO scores VALUES ('Dave', 80)",
	)

	vals := csrQueryAllInt64(t, db,
		"SELECT DENSE_RANK() OVER (ORDER BY score DESC) FROM scores ORDER BY score DESC, name")

	// Expected dense ranks: Alice=1, Bob=2, Charlie=2, Dave=3
	want := []int64{1, 2, 2, 3}
	if len(vals) != len(want) {
		t.Fatalf("row count = %d, want %d", len(vals), len(want))
	}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("row %d dense_rank = %d, want %d", i, v, want[i])
		}
	}
}

// TestConnStmtRemaining_WindowRankVsDenseRank compares RANK and DENSE_RANK
// side by side, confirming they diverge only on ties.
func TestConnStmtRemaining_WindowRankVsDenseRank(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE ranks_tbl (v INTEGER)",
		"INSERT INTO ranks_tbl VALUES (10)",
		"INSERT INTO ranks_tbl VALUES (20)",
		"INSERT INTO ranks_tbl VALUES (20)",
		"INSERT INTO ranks_tbl VALUES (30)",
	)

	type row struct{ rank, dense int64 }
	var results []row

	sqlRows, err := db.Query(
		"SELECT RANK() OVER (ORDER BY v), DENSE_RANK() OVER (ORDER BY v) FROM ranks_tbl ORDER BY v")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer sqlRows.Close()
	for sqlRows.Next() {
		var r, d int64
		if err := sqlRows.Scan(&r, &d); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, row{r, d})
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Expected: rank=[1,2,2,4], dense_rank=[1,2,2,3]
	wantRank := []int64{1, 2, 2, 4}
	wantDense := []int64{1, 2, 2, 3}
	if len(results) != 4 {
		t.Fatalf("got %d rows, want 4", len(results))
	}
	for i, r := range results {
		if r.rank != wantRank[i] {
			t.Errorf("row %d rank = %d, want %d", i, r.rank, wantRank[i])
		}
		if r.dense != wantDense[i] {
			t.Errorf("row %d dense_rank = %d, want %d", i, r.dense, wantDense[i])
		}
	}
}

// TestConnStmtRemaining_WindowRowNumberNoOrderBy verifies ROW_NUMBER()
// without an ORDER BY clause — row numbers must still be sequential
// (order is non-deterministic but all values 1..N must appear exactly once).
func TestConnStmtRemaining_WindowRowNumberNoOrderBy(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE seq_tbl (id INTEGER)",
		"INSERT INTO seq_tbl VALUES (10)",
		"INSERT INTO seq_tbl VALUES (20)",
		"INSERT INTO seq_tbl VALUES (30)",
	)

	vals := csrQueryAllInt64(t, db, "SELECT ROW_NUMBER() OVER () FROM seq_tbl")
	if len(vals) != 3 {
		t.Fatalf("got %d rows, want 3", len(vals))
	}

	seen := make(map[int64]bool)
	for _, v := range vals {
		if v < 1 || v > 3 {
			t.Errorf("row_number %d out of range [1,3]", v)
		}
		if seen[v] {
			t.Errorf("duplicate row_number %d", v)
		}
		seen[v] = true
	}
}

// TestConnStmtRemaining_WindowNtile verifies NTILE distributes rows into buckets.
func TestConnStmtRemaining_WindowNtile(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE ntile_src (v INTEGER)",
		"INSERT INTO ntile_src VALUES (1)",
		"INSERT INTO ntile_src VALUES (2)",
		"INSERT INTO ntile_src VALUES (3)",
		"INSERT INTO ntile_src VALUES (4)",
		"INSERT INTO ntile_src VALUES (5)",
		"INSERT INTO ntile_src VALUES (6)",
	)

	vals := csrQueryAllInt64(t, db,
		"SELECT NTILE(3) OVER (ORDER BY v) FROM ntile_src ORDER BY v")

	if len(vals) != 6 {
		t.Fatalf("got %d rows, want 6", len(vals))
	}

	// With 6 rows and 3 buckets: buckets are [1,1,2,2,3,3]
	for _, v := range vals {
		if v < 1 || v > 3 {
			t.Errorf("ntile value %d not in [1,3]", v)
		}
	}

	// First row must be bucket 1, last row must be bucket 3.
	if vals[0] != 1 {
		t.Errorf("first ntile = %d, want 1", vals[0])
	}
	if vals[5] != 3 {
		t.Errorf("last ntile = %d, want 3", vals[5])
	}
}

// TestConnStmtRemaining_WindowNamedWindowClause exercises SELECT statements
// that use a named WINDOW clause (OVER w ... WINDOW w AS (...) syntax).
func TestConnStmtRemaining_WindowNamedWindowClause(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE named_win (x INTEGER)",
		"INSERT INTO named_win VALUES (3)",
		"INSERT INTO named_win VALUES (1)",
		"INSERT INTO named_win VALUES (2)",
	)

	vals := csrQueryAllInt64(t, db,
		"SELECT ROW_NUMBER() OVER w FROM named_win WINDOW w AS (ORDER BY x) ORDER BY x")

	want := []int64{1, 2, 3}
	if len(vals) != len(want) {
		t.Fatalf("row count = %d, want %d", len(vals), len(want))
	}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("row %d = %d, want %d", i, v, want[i])
		}
	}
}

// TestConnStmtRemaining_WindowWithWhereClause exercises a window function query
// combined with a WHERE clause to confirm filtering still applies.
func TestConnStmtRemaining_WindowWithWhereClause(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE filter_win (id INTEGER, val INTEGER)",
		"INSERT INTO filter_win VALUES (1, 10)",
		"INSERT INTO filter_win VALUES (2, 20)",
		"INSERT INTO filter_win VALUES (3, 30)",
		"INSERT INTO filter_win VALUES (4, 40)",
	)

	vals := csrQueryAllInt64(t, db,
		"SELECT ROW_NUMBER() OVER (ORDER BY id) FROM filter_win WHERE val >= 20 ORDER BY id")

	// Only rows with val>=20 should appear; ROW_NUMBER across that filtered set.
	if len(vals) != 3 {
		t.Fatalf("got %d rows, want 3", len(vals))
	}
	for i, v := range vals {
		if v != int64(i+1) {
			t.Errorf("row %d row_number = %d, want %d", i, v, i+1)
		}
	}
}

// TestConnStmtRemaining_WindowWithLimit exercises a window function query
// combined with LIMIT to ensure limit interacts correctly with window output.
func TestConnStmtRemaining_WindowWithLimit(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE limit_win (n INTEGER)",
		"INSERT INTO limit_win VALUES (1)",
		"INSERT INTO limit_win VALUES (2)",
		"INSERT INTO limit_win VALUES (3)",
		"INSERT INTO limit_win VALUES (4)",
		"INSERT INTO limit_win VALUES (5)",
	)

	vals := csrQueryAllInt64(t, db,
		"SELECT ROW_NUMBER() OVER (ORDER BY n) FROM limit_win ORDER BY n LIMIT 3")

	if len(vals) != 3 {
		t.Fatalf("got %d rows with LIMIT 3, want 3", len(vals))
	}
	for i, v := range vals {
		if v != int64(i+1) {
			t.Errorf("row %d = %d, want %d", i, v, i+1)
		}
	}
}

// ============================================================================
// compile_select.go: remaining edge cases
// ============================================================================

// TestConnStmtRemaining_SelectDistinctWithOrderBy exercises SELECT DISTINCT
// combined with ORDER BY (distinct+sorter code path).
func TestConnStmtRemaining_SelectDistinctWithOrderBy(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE dup_data (v INTEGER)",
		"INSERT INTO dup_data VALUES (3)",
		"INSERT INTO dup_data VALUES (1)",
		"INSERT INTO dup_data VALUES (2)",
		"INSERT INTO dup_data VALUES (1)",
		"INSERT INTO dup_data VALUES (3)",
	)

	vals := csrQueryAllInt64(t, db, "SELECT DISTINCT v FROM dup_data ORDER BY v ASC")
	want := []int64{1, 2, 3}
	if len(vals) != len(want) {
		t.Fatalf("got %d distinct rows, want %d", len(vals), len(want))
	}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("row %d = %d, want %d", i, v, want[i])
		}
	}
}

// TestConnStmtRemaining_SelectOrderByColumnNumber exercises ORDER BY using
// a column position number (1-indexed) instead of a name.
func TestConnStmtRemaining_SelectOrderByColumnNumber(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE col_num (a INTEGER, b TEXT)",
		"INSERT INTO col_num VALUES (3, 'c')",
		"INSERT INTO col_num VALUES (1, 'a')",
		"INSERT INTO col_num VALUES (2, 'b')",
	)

	vals := csrQueryAllInt64(t, db, "SELECT a FROM col_num ORDER BY 1 ASC")
	want := []int64{1, 2, 3}
	if len(vals) != len(want) {
		t.Fatalf("got %d rows, want %d", len(vals), len(want))
	}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("row %d = %d, want %d", i, v, want[i])
		}
	}
}

// TestConnStmtRemaining_SelectLimitZero verifies that LIMIT 0 returns no rows.
func TestConnStmtRemaining_SelectLimitZero(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE limit0 (x INTEGER)",
		"INSERT INTO limit0 VALUES (1)",
		"INSERT INTO limit0 VALUES (2)",
	)

	vals := csrQueryAllInt64(t, db, "SELECT x FROM limit0 LIMIT 0")
	if len(vals) != 0 {
		t.Errorf("LIMIT 0 returned %d rows, want 0", len(vals))
	}
}

// TestConnStmtRemaining_SelectOffsetSkip verifies OFFSET skips the first N rows.
func TestConnStmtRemaining_SelectOffsetSkip(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE offset_tbl (v INTEGER)",
		"INSERT INTO offset_tbl VALUES (1)",
		"INSERT INTO offset_tbl VALUES (2)",
		"INSERT INTO offset_tbl VALUES (3)",
		"INSERT INTO offset_tbl VALUES (4)",
		"INSERT INTO offset_tbl VALUES (5)",
	)

	vals := csrQueryAllInt64(t, db, "SELECT v FROM offset_tbl ORDER BY v LIMIT 10 OFFSET 2")
	want := []int64{3, 4, 5}
	if len(vals) != len(want) {
		t.Fatalf("got %d rows, want %d", len(vals), len(want))
	}
	for i, v := range vals {
		if v != want[i] {
			t.Errorf("row %d = %d, want %d", i, v, want[i])
		}
	}
}

// TestConnStmtRemaining_SelectOrderByExtraColumn exercises the case where
// the ORDER BY expression references a column not in the SELECT list.
func TestConnStmtRemaining_SelectOrderByExtraColumn(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE extra_col (id INTEGER, name TEXT, priority INTEGER)",
		"INSERT INTO extra_col VALUES (1, 'low', 3)",
		"INSERT INTO extra_col VALUES (2, 'high', 1)",
		"INSERT INTO extra_col VALUES (3, 'mid', 2)",
	)

	// ORDER BY priority but SELECT only id and name.
	sqlRows, err := db.Query("SELECT id, name FROM extra_col ORDER BY priority ASC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer sqlRows.Close()

	type row struct {
		id   int64
		name string
	}
	var results []row
	for sqlRows.Next() {
		var r row
		if err := sqlRows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := sqlRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("got %d rows, want 3", len(results))
	}
	// Ordered by priority: 1=high, 2=mid, 3=low
	wantNames := []string{"high", "mid", "low"}
	for i, r := range results {
		if r.name != wantNames[i] {
			t.Errorf("row %d name = %q, want %q", i, r.name, wantNames[i])
		}
	}
}

// TestConnStmtRemaining_SelectCountWithHavingClause exercises aggregate
// queries with HAVING to filter groups post-aggregation.
func TestConnStmtRemaining_SelectCountWithHavingClause(t *testing.T) {
	db := csrOpenMem(t)
	csrExec(t, db,
		"CREATE TABLE grp (category TEXT, val INTEGER)",
		"INSERT INTO grp VALUES ('A', 1)",
		"INSERT INTO grp VALUES ('A', 2)",
		"INSERT INTO grp VALUES ('B', 3)",
		"INSERT INTO grp VALUES ('C', 4)",
		"INSERT INTO grp VALUES ('C', 5)",
		"INSERT INTO grp VALUES ('C', 6)",
	)

	// Only categories with more than 2 entries.
	sqlRows, err := db.Query("SELECT category, COUNT(*) FROM grp GROUP BY category HAVING COUNT(*) > 2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer sqlRows.Close()

	var cat string
	var cnt int64
	if !sqlRows.Next() {
		t.Fatal("expected at least one row from HAVING COUNT(*) > 2")
	}
	if err := sqlRows.Scan(&cat, &cnt); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cat != "C" || cnt != 3 {
		t.Errorf("got (%q, %d), want (\"C\", 3)", cat, cnt)
	}
}
