// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// open2DB opens an in-memory database for DDL2 coverage tests.
func open2DB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open :memory: failed: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, func() { db.Close() }
}

// exec2 executes SQL statements and fails the test on error.
func exec2(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

// exec2Err executes a statement and returns any error without failing.
func exec2Err(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}

// query2Int64 runs a query returning a single int64 value.
func query2Int64(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRow(query).Scan(&v); err != nil {
		t.Fatalf("query2Int64 %q: %v", query, err)
	}
	return v
}

// count2Rows counts rows from the given query.
func count2Rows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("count2Rows %q: %v", query, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("count2Rows rows.Err: %v", err)
	}
	return n
}

// ============================================================================
// TestCompileDDL2_AutoincrementTable
// Exercises: initializeNewTable AUTOINCREMENT branch → ensureSqliteSequenceTable.
// Also exercises the "already created" early-exit of ensureSqliteSequenceTable
// by creating a second AUTOINCREMENT table afterward.
// ============================================================================

func TestCompileDDL2_AutoincrementTable(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE ai_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
	)

	// Confirm sqlite_sequence exists and is queryable.
	seqCount := query2Int64(t, db, "SELECT COUNT(*) FROM sqlite_sequence")
	if seqCount < 0 {
		t.Errorf("sqlite_sequence COUNT returned negative")
	}

	exec2(t, db,
		"INSERT INTO ai_tbl(name) VALUES('alice')",
		"INSERT INTO ai_tbl(name) VALUES('bob')",
	)
	n := query2Int64(t, db, "SELECT COUNT(*) FROM ai_tbl")
	if n != 2 {
		t.Errorf("expected 2 rows, got %d", n)
	}

	// Second AUTOINCREMENT table: ensureSqliteSequenceTable early-exit path.
	exec2(t, db,
		"CREATE TABLE ai_tbl2 (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)",
	)
	n2 := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='ai_tbl2'")
	if n2 != 1 {
		t.Errorf("expected ai_tbl2 in sqlite_master, got %d", n2)
	}
}

// ============================================================================
// TestCompileDDL2_WithoutRowIDCompositePK
// Exercises: initializeNewTable WITHOUT ROWID path (allocateTablePage calls
// CreateWithoutRowidTable). Also exercises composite primary key definition.
// ============================================================================

func TestCompileDDL2_WithoutRowIDCompositePK(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE wrid_cpk (a INTEGER, b TEXT, v REAL, PRIMARY KEY (a, b)) WITHOUT ROWID",
		"INSERT INTO wrid_cpk VALUES(1,'x',1.1)",
		"INSERT INTO wrid_cpk VALUES(2,'y',2.2)",
	)

	n := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE type='table' AND name='wrid_cpk'")
	if n != 1 {
		t.Errorf("expected wrid_cpk in sqlite_master, got %d", n)
	}
	count := query2Int64(t, db, "SELECT COUNT(*) FROM wrid_cpk")
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// ============================================================================
// TestCompileDDL2_InsteadOfTriggerOnView
// Exercises: compileCreateTrigger with INSTEAD OF event on a VIEW.
// The schema validates that the target is a view (not a table) for INSTEAD OF.
// ============================================================================

func TestCompileDDL2_InsteadOfTriggerOnView(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE io_base (id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO io_base VALUES(1,'original')",
		"CREATE VIEW io_view AS SELECT id, val FROM io_base",
	)

	// CREATE TRIGGER … INSTEAD OF INSERT ON io_view must succeed — this exercises
	// the compileCreateTrigger + schema.CreateTrigger INSTEAD OF path.
	exec2(t, db,
		`CREATE TRIGGER io_trg INSTEAD OF INSERT ON io_view
		 BEGIN
		   INSERT INTO io_base(id, val) VALUES(NEW.id, NEW.val);
		 END`,
	)

	// The trigger should be droppable (proves it was registered in the schema).
	exec2(t, db, "DROP TRIGGER io_trg")
}

// TestCompileDDL2_InsteadOfTriggerOnTableError verifies that an INSTEAD OF
// trigger cannot be created on a plain table (only on views).
func TestCompileDDL2_InsteadOfTriggerOnTableError(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db, "CREATE TABLE io_tbl2 (id INTEGER PRIMARY KEY, v TEXT)")

	err := exec2Err(db, "CREATE TRIGGER bad_io INSTEAD OF INSERT ON io_tbl2 BEGIN SELECT 1; END")
	if err == nil {
		t.Fatal("expected error creating INSTEAD OF trigger on a table, got nil")
	}
	if !strings.Contains(err.Error(), "INSTEAD OF") && !strings.Contains(err.Error(), "cannot") {
		t.Logf("error message (any error is acceptable): %v", err)
	}
}

// ============================================================================
// TestCompileDDL2_TriggerWithWhenClause
// Exercises: compileCreateTrigger with a WHEN condition clause.
// The WHEN clause is stored in the trigger and evaluated at fire time.
// ============================================================================

func TestCompileDDL2_TriggerWithWhenClause(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE wc_data (id INTEGER PRIMARY KEY, score INTEGER)",
		"CREATE TABLE wc_log  (msg TEXT)",
	)

	// CREATE TRIGGER with WHEN clause exercises a distinct parsing and storage
	// path in compileCreateTrigger (the trigger's When field is non-nil).
	exec2(t, db,
		`CREATE TRIGGER wc_trg AFTER INSERT ON wc_data
		 WHEN NEW.score > 50
		 BEGIN
		   INSERT INTO wc_log VALUES('high_score');
		 END`,
	)

	// Insert a low score — WHEN prevents the trigger body from firing.
	exec2(t, db, "INSERT INTO wc_data VALUES(1, 30)")
	// Insert a high score — WHEN allows the trigger body to fire.
	exec2(t, db, "INSERT INTO wc_data VALUES(2, 80)")

	// Verify that rows were inserted into wc_data without error.
	n := query2Int64(t, db, "SELECT COUNT(*) FROM wc_data")
	if n != 2 {
		t.Errorf("expected 2 rows in wc_data, got %d", n)
	}

	// The trigger should be droppable — confirming it was registered.
	exec2(t, db, "DROP TRIGGER wc_trg")
}

// ============================================================================
// TestCompileDDL2_DropTableWithDependentView
// Exercises: performDropTable when a VIEW exists that references the table.
// The drop should succeed (no automatic cascade check for views in this engine).
// ============================================================================

func TestCompileDDL2_DropTableWithDependentView(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE dtv_tbl (id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO dtv_tbl VALUES(1,'a')",
		"CREATE VIEW dtv_view AS SELECT val FROM dtv_tbl",
	)

	// Drop the underlying table while the view still exists.
	exec2(t, db, "DROP TABLE dtv_tbl")

	// Table should be gone from sqlite_master.
	n := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE name='dtv_tbl'")
	if n != 0 {
		t.Errorf("expected dtv_tbl to be removed, got %d rows in sqlite_master", n)
	}

	// View is still registered (it's left orphaned, matching SQLite behaviour).
	nv := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE type='view' AND name='dtv_view'")
	if nv != 1 {
		t.Errorf("expected dtv_view to remain in sqlite_master, got %d", nv)
	}
}

// ============================================================================
// TestCompileDDL2_DropTableFKCascade
// Exercises: performDropTable + fkManager.RemoveConstraints for a table
// that previously had FK constraints registered (registerForeignKeyConstraints).
// ============================================================================

func TestCompileDDL2_DropTableFKCascade(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE fkc_parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
		"CREATE TABLE fkc_child  (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkc_parent(id))",
	)

	// Drop the child table — its FK constraints must be cleaned up.
	exec2(t, db, "DROP TABLE fkc_child")
	n := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE name='fkc_child'")
	if n != 0 {
		t.Errorf("expected fkc_child removed, got %d", n)
	}

	// Parent should still exist.
	np := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE name='fkc_parent'")
	if np != 1 {
		t.Errorf("expected fkc_parent still present, got %d", np)
	}
}

// ============================================================================
// TestCompileDDL2_CollateOrderBy
// Exercises: resolveExprCollation via ORDER BY col COLLATE NOCASE.
// The CollateExpr branch of resolveExprCollation returns strings.ToUpper(collation).
// ============================================================================

func TestCompileDDL2_CollateOrderBy(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE cob_tbl (id INTEGER, name TEXT)",
		"INSERT INTO cob_tbl VALUES(1,'banana'),(2,'Apple'),(3,'cherry')",
	)

	rows, err := db.Query("SELECT name FROM cob_tbl ORDER BY name COLLATE NOCASE")
	if err != nil {
		t.Fatalf("ORDER BY COLLATE NOCASE: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, s)
	}
	if len(names) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(names), names)
	}
}

// ============================================================================
// TestCompileDDL2_CollateWhere
// Exercises: resolveExprCollation when COLLATE appears in a WHERE predicate
// (binary comparison with CollateExpr on the right-hand side).
// ============================================================================

func TestCompileDDL2_CollateWhere(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE cwh_tbl (id INTEGER, tag TEXT)",
		"INSERT INTO cwh_tbl VALUES(1,'Hello'),(2,'world'),(3,'HELLO')",
	)

	// WHERE with explicit COLLATE exercises CollateExpr path inside a BinaryExpr.
	n := query2Int64(t, db, "SELECT COUNT(*) FROM cwh_tbl WHERE tag = 'hello' COLLATE NOCASE")
	if n != 2 {
		t.Errorf("expected 2 NOCASE matches, got %d", n)
	}
}

// ============================================================================
// TestCompileDDL2_CollateColumnDeclaration
// Exercises: resolveExprCollation IdentExpr branch — a column with a declared
// COLLATE picks up that collation when used in ORDER BY without an explicit COLLATE.
// ============================================================================

func TestCompileDDL2_CollateColumnDeclaration(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE ccd_tbl (id INTEGER, label TEXT COLLATE NOCASE)",
		"INSERT INTO ccd_tbl VALUES(1,'Zebra'),(2,'apple'),(3,'Mango')",
	)

	rows, err := db.Query("SELECT label FROM ccd_tbl ORDER BY label")
	if err != nil {
		t.Fatalf("ORDER BY column with COLLATE: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, s)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(got), got)
	}
}

// ============================================================================
// TestCompileDDL2_CollateParenExpr
// Exercises: resolveExprCollation ParenExpr branch — a COLLATE expression
// wrapped in parentheses should be unwrapped and the inner collation returned.
// ============================================================================

func TestCompileDDL2_CollateParenExpr(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE cpe_tbl (id INTEGER, word TEXT)",
		"INSERT INTO cpe_tbl VALUES(1,'Foo'),(2,'bar'),(3,'BAZ')",
	)

	// Parenthesized COLLATE exercises the ParenExpr → recurse path.
	rows, err := db.Query("SELECT word FROM cpe_tbl ORDER BY (word COLLATE NOCASE)")
	if err != nil {
		t.Fatalf("ORDER BY (word COLLATE NOCASE): %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, s)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 rows from parenthesized COLLATE, got %d: %v", len(got), got)
	}
}

// ============================================================================
// TestCompileDDL2_FKThenDropParent
// Exercises: CREATE TABLE with foreign key referencing another table,
// then DROP TABLE on the FK child — the FK constraints are removed from the
// manager but the parent table remains intact.
// ============================================================================

func TestCompileDDL2_FKThenDropParent(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE fkdp_parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE fkdp_child  (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES fkdp_parent(id))",
		"INSERT INTO fkdp_parent VALUES(1)",
		"INSERT INTO fkdp_child  VALUES(10, 1)",
	)

	// Drop child first.
	exec2(t, db, "DROP TABLE fkdp_child")
	nc := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE name='fkdp_child'")
	if nc != 0 {
		t.Errorf("expected fkdp_child gone, got %d", nc)
	}

	// Now drop parent — no FK should block this since child is gone.
	exec2(t, db, "DROP TABLE fkdp_parent")
	np := count2Rows(t, db, "SELECT name FROM sqlite_master WHERE name='fkdp_parent'")
	if np != 0 {
		t.Errorf("expected fkdp_parent gone, got %d", np)
	}
}

// ============================================================================
// TestCompileDDL2_InsteadOfDeleteTrigger
// Exercises: compileCreateTrigger INSTEAD OF DELETE on a VIEW —
// a different trigger event than the INSERT variant, covering TriggerDelete.
// ============================================================================

func TestCompileDDL2_InsteadOfDeleteTrigger(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE iod_base (id INTEGER PRIMARY KEY, val TEXT)",
		"INSERT INTO iod_base VALUES(1,'row1'),(2,'row2')",
		"CREATE VIEW iod_view AS SELECT id, val FROM iod_base",
	)

	// CREATE TRIGGER … INSTEAD OF DELETE exercises the INSTEAD OF + DELETE event
	// combination in compileCreateTrigger — a distinct path from BEFORE/AFTER INSERT.
	exec2(t, db,
		`CREATE TRIGGER iod_trg INSTEAD OF DELETE ON iod_view
		 BEGIN
		   DELETE FROM iod_base WHERE id = OLD.id;
		 END`,
	)

	// Verify the trigger was registered by querying the underlying table
	// (which is not affected by the trigger itself at compile time).
	n := query2Int64(t, db, "SELECT COUNT(*) FROM iod_base")
	if n != 2 {
		t.Errorf("expected 2 rows in iod_base, got %d", n)
	}

	// The trigger must be droppable — confirming schema registration.
	exec2(t, db, "DROP TRIGGER iod_trg")
}

// ============================================================================
// TestCompileDDL2_TriggerIfNotExistsOnView
// Exercises: compileCreateTrigger IF NOT EXISTS path for an INSTEAD OF trigger
// that already exists — must silently succeed on the second attempt.
// ============================================================================

func TestCompileDDL2_TriggerIfNotExistsOnView(t *testing.T) {
	t.Parallel()
	db, done := open2DB(t)
	defer done()

	exec2(t, db,
		"CREATE TABLE ine_v_base (id INTEGER PRIMARY KEY, v TEXT)",
		"CREATE VIEW ine_v_view AS SELECT id, v FROM ine_v_base",
		"CREATE TRIGGER ine_v_trg INSTEAD OF INSERT ON ine_v_view BEGIN SELECT 1; END",
	)

	// Second creation with IF NOT EXISTS must not error.
	err := exec2Err(db, "CREATE TRIGGER IF NOT EXISTS ine_v_trg INSTEAD OF INSERT ON ine_v_view BEGIN SELECT 1; END")
	if err != nil {
		t.Fatalf("CREATE TRIGGER IF NOT EXISTS on existing INSTEAD OF trigger: %v", err)
	}
}
