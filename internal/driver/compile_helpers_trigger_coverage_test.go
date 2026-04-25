// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"testing"
)

// chExec runs SQL and fatals on error.
func chExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// chOpenMem opens an in-memory database.
func chOpenMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// chQueryInt64 runs a scalar int query.
func chQueryInt64(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// chQueryString runs a scalar string query.
func chQueryString(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	var s string
	if err := db.QueryRow(q).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return s
}

// ---------------------------------------------------------------------------
// executeBeforeInsertTriggers — additional coverage (multiple triggers)
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerBeforeInsertMultiple exercises the non-empty trigger
// slice path in executeBeforeInsertTriggers.
func TestCompileHelpersTriggerBeforeInsertMultiple(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
	chExec(t, db, "CREATE TABLE audit (evt TEXT)")
	chExec(t, db, `CREATE TRIGGER bi1 BEFORE INSERT ON items BEGIN INSERT INTO audit VALUES('bi1'); END`)
	chExec(t, db, `CREATE TRIGGER bi2 BEFORE INSERT ON items BEGIN INSERT INTO audit VALUES('bi2'); END`)
	chExec(t, db, "INSERT INTO items VALUES(1,'apple',10)")
	chExec(t, db, "INSERT INTO items VALUES(2,'banana',5)")

	n := chQueryInt64(t, db, "SELECT COUNT(*) FROM audit")
	if n != 4 {
		t.Errorf("expected 4 audit rows (2 triggers × 2 inserts), got %d", n)
	}
}

// ---------------------------------------------------------------------------
// executeAfterInsertTriggers — additional coverage
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerAfterInsertLogsNewRow exercises AFTER INSERT trigger
// using NEW row references so that prepareNewRowForInsert is exercised too.
func TestCompileHelpersTriggerAfterInsertLogsNewRow(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE products (id INTEGER, label TEXT)")
	chExec(t, db, "CREATE TABLE log (info TEXT)")
	chExec(t, db, `CREATE TRIGGER ai_prod AFTER INSERT ON products BEGIN INSERT INTO log VALUES(NEW.label); END`)

	chExec(t, db, "INSERT INTO products VALUES(1,'widget')")
	chExec(t, db, "INSERT INTO products VALUES(2,'gadget')")

	s := chQueryString(t, db, "SELECT info FROM log ORDER BY rowid LIMIT 1")
	if s != "widget" {
		t.Errorf("expected 'widget', got %q", s)
	}
}

// ---------------------------------------------------------------------------
// executeBeforeUpdateTriggers — additional coverage (trigger body updates another table)
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerBeforeUpdateLogRow exercises executeBeforeUpdateTriggers
// with a trigger body that inserts into a separate log table.
func TestCompileHelpersTriggerBeforeUpdateLogRow(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
	chExec(t, db, "CREATE TABLE log (evt TEXT)")
	chExec(t, db, `CREATE TRIGGER bu_acc BEFORE UPDATE ON accounts BEGIN INSERT INTO log VALUES('before_update'); END`)

	chExec(t, db, "INSERT INTO accounts VALUES(1,100)")
	chExec(t, db, "INSERT INTO accounts VALUES(2,200)")
	chExec(t, db, "UPDATE accounts SET balance = balance + 10")

	n := chQueryInt64(t, db, "SELECT COUNT(*) FROM log")
	if n != 2 {
		t.Errorf("expected 2 BEFORE UPDATE log rows, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// executeAfterUpdateTriggers — additional coverage
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerAfterUpdateMirror exercises AFTER UPDATE with a
// trigger body that inserts into a log table, covering executeAfterUpdateTriggers.
func TestCompileHelpersTriggerAfterUpdateMirror(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE src (id INTEGER PRIMARY KEY, val TEXT)")
	chExec(t, db, "CREATE TABLE log (evt TEXT)")
	chExec(t, db, "INSERT INTO src VALUES(1,'old')")
	chExec(t, db, "INSERT INTO src VALUES(2,'x')")
	chExec(t, db, `CREATE TRIGGER au_src AFTER UPDATE ON src BEGIN INSERT INTO log VALUES('after_update_' || NEW.val); END`)

	chExec(t, db, "UPDATE src SET val='new' WHERE id=1")

	n := chQueryInt64(t, db, "SELECT COUNT(*) FROM log")
	if n != 1 {
		t.Errorf("expected 1 AFTER UPDATE log row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// executeBeforeDeleteTriggers — additional coverage
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerBeforeDeleteArchive exercises executeBeforeDeleteTriggers
// with a trigger that archives the old row.
func TestCompileHelpersTriggerBeforeDeleteArchive(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
	chExec(t, db, "CREATE TABLE archive (id INTEGER, total INTEGER)")
	chExec(t, db, "INSERT INTO orders VALUES(1,50)")
	chExec(t, db, "INSERT INTO orders VALUES(2,75)")
	chExec(t, db, `CREATE TRIGGER bd_ord BEFORE DELETE ON orders BEGIN INSERT INTO archive VALUES(OLD.id, OLD.total); END`)

	chExec(t, db, "DELETE FROM orders WHERE id=1")

	n := chQueryInt64(t, db, "SELECT COUNT(*) FROM archive")
	if n != 1 {
		t.Errorf("expected 1 archived row, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// executeAfterDeleteTriggers — additional coverage
// ---------------------------------------------------------------------------

// TestCompileHelpersTriggerAfterDeleteCounter exercises executeAfterDeleteTriggers
// with a trigger that updates a counter table.
func TestCompileHelpersTriggerAfterDeleteCounter(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")
	chExec(t, db, "CREATE TABLE counters (label TEXT, cnt INTEGER)")
	chExec(t, db, "INSERT INTO counters VALUES('deletes',0)")
	chExec(t, db, "INSERT INTO events VALUES(1,'e1')")
	chExec(t, db, "INSERT INTO events VALUES(2,'e2')")
	chExec(t, db, `CREATE TRIGGER ad_ev AFTER DELETE ON events BEGIN UPDATE counters SET cnt=cnt+1 WHERE label='deletes'; END`)

	chExec(t, db, "DELETE FROM events WHERE id=1")
	chExec(t, db, "DELETE FROM events WHERE id=2")

	n := chQueryInt64(t, db, "SELECT cnt FROM counters WHERE label='deletes'")
	if n != 2 {
		t.Errorf("expected counter=2, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// emitExtraOrderByColumnMultiTable — ORDER BY column from joined table
// ---------------------------------------------------------------------------

// TestCompileHelpersJoinOrderByJoinedColumn exercises emitExtraOrderByColumnMultiTable
// by ordering a JOIN result on a column that lives in the right-hand table.
func TestCompileHelpersJoinOrderByJoinedColumn(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE dept (did INTEGER PRIMARY KEY, dname TEXT)")
	chExec(t, db, "CREATE TABLE emp (eid INTEGER PRIMARY KEY, ename TEXT, dept_id INTEGER)")
	chExec(t, db, "INSERT INTO dept VALUES(1,'Engineering')")
	chExec(t, db, "INSERT INTO dept VALUES(2,'Marketing')")
	chExec(t, db, "INSERT INTO dept VALUES(3,'Sales')")
	chExec(t, db, "INSERT INTO emp VALUES(1,'Carol',2)")
	chExec(t, db, "INSERT INTO emp VALUES(2,'Alice',1)")
	chExec(t, db, "INSERT INTO emp VALUES(3,'Bob',1)")

	rows, err := db.Query(
		"SELECT emp.ename, dept.dname FROM emp INNER JOIN dept ON emp.dept_id = dept.did ORDER BY dept.dname, emp.ename",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct{ ename, dname string }
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ename, &r.dname); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []row{
		{"Alice", "Engineering"},
		{"Bob", "Engineering"},
		{"Carol", "Marketing"},
	}
	if len(got) != len(want) {
		t.Fatalf("want %d rows, got %d: %v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("row %d: want %+v, got %+v", i, w, got[i])
		}
	}
}

// TestCompileHelpersJoinOrderByRowidAlias exercises emitExtraOrderByColumnMultiTable
// for tables where the ORDER BY column resolves to a rowid alias.
func TestCompileHelpersJoinOrderByRowidAlias(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE cats (id INTEGER PRIMARY KEY, name TEXT)")
	chExec(t, db, "CREATE TABLE toys (tid INTEGER PRIMARY KEY, cat_id INTEGER, toy TEXT)")
	chExec(t, db, "INSERT INTO cats VALUES(3,'Whiskers')")
	chExec(t, db, "INSERT INTO cats VALUES(1,'Felix')")
	chExec(t, db, "INSERT INTO cats VALUES(2,'Garfield')")
	chExec(t, db, "INSERT INTO toys VALUES(10,1,'ball')")
	chExec(t, db, "INSERT INTO toys VALUES(20,2,'mouse')")
	chExec(t, db, "INSERT INTO toys VALUES(30,3,'string')")

	rows, err := db.Query(
		"SELECT cats.name, toys.toy FROM cats INNER JOIN toys ON cats.id = toys.cat_id ORDER BY cats.id",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name, toy string
		if err := rows.Scan(&name, &toy); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []string{"Felix", "Garfield", "Whiskers"}
	if len(names) != len(want) {
		t.Fatalf("want %v, got %v", want, names)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("row %d: want %q, got %q", i, w, names[i])
		}
	}
}

// ---------------------------------------------------------------------------
// findColumnTableIndex — table-qualified column resolution in LEFT JOIN
// ---------------------------------------------------------------------------

// chLeftJoinRow holds a row from the left join query.
type chLeftJoinRow struct {
	pname string
	cname sql.NullString
}

// chScanLeftJoinRows scans all rows from a left join query result.
func chScanLeftJoinRows(t *testing.T, rows *sql.Rows) []chLeftJoinRow {
	t.Helper()
	var got []chLeftJoinRow
	for rows.Next() {
		var r chLeftJoinRow
		if err := rows.Scan(&r.pname, &r.cname); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}

// TestCompileHelpersLeftJoinFindColumnTableIndex exercises findColumnTableIndex
// by selecting table-qualified columns from a LEFT JOIN so the NULL-emission
// path walks the table list to resolve column ownership.
func TestCompileHelpersLeftJoinFindColumnTableIndex(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE parent (pid INTEGER PRIMARY KEY, pname TEXT)")
	chExec(t, db, "CREATE TABLE child  (cid INTEGER PRIMARY KEY, pid INTEGER, cname TEXT)")
	chExec(t, db, "INSERT INTO parent VALUES(1,'Alpha')")
	chExec(t, db, "INSERT INTO parent VALUES(2,'Beta')")
	chExec(t, db, "INSERT INTO parent VALUES(3,'Gamma')")
	chExec(t, db, "INSERT INTO child  VALUES(10,1,'a1')")
	chExec(t, db, "INSERT INTO child  VALUES(20,1,'a2')")

	rows, err := db.Query(
		"SELECT parent.pname, child.cname FROM parent LEFT JOIN child ON parent.pid = child.pid ORDER BY parent.pid, child.cid",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	got := chScanLeftJoinRows(t, rows)

	// Alpha has 2 children, Beta and Gamma have none (NULL cname from LEFT JOIN).
	if len(got) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(got), got)
	}
	if got[0].pname != "Alpha" || got[0].cname.String != "a1" {
		t.Errorf("row 0: %+v", got[0])
	}
	if got[2].pname != "Beta" || got[2].cname.Valid {
		t.Errorf("row 2 (Beta, no child) should have NULL cname: %+v", got[2])
	}
}

// ---------------------------------------------------------------------------
// emitLeafRowSorter — multi-table JOIN with ORDER BY (sorter path)
// ---------------------------------------------------------------------------

// TestCompileHelpersLeafRowSorterWithWhere exercises emitLeafRowSorter including
// its WHERE-skip branch by using a filtered ORDER BY JOIN query.
func TestCompileHelpersLeafRowSorterWithWhere(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE regions (rid INTEGER PRIMARY KEY, rname TEXT)")
	chExec(t, db, "CREATE TABLE sales   (sid INTEGER PRIMARY KEY, rid INTEGER, amount INTEGER)")
	chExec(t, db, "INSERT INTO regions VALUES(1,'North')")
	chExec(t, db, "INSERT INTO regions VALUES(2,'South')")
	chExec(t, db, "INSERT INTO regions VALUES(3,'East')")
	chExec(t, db, "INSERT INTO sales VALUES(1,1,100)")
	chExec(t, db, "INSERT INTO sales VALUES(2,1,200)")
	chExec(t, db, "INSERT INTO sales VALUES(3,2,300)")
	chExec(t, db, "INSERT INTO sales VALUES(4,3,50)")

	rows, err := db.Query(
		`SELECT regions.rname, sales.amount
		 FROM regions INNER JOIN sales ON regions.rid = sales.rid
		 WHERE sales.amount > 100
		 ORDER BY regions.rname, sales.amount`,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type row struct {
		rname  string
		amount int64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.rname, &r.amount); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	want := []row{{"North", 200}, {"South", 300}}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("row %d: want %+v, got %+v", i, w, got[i])
		}
	}
}

// ---------------------------------------------------------------------------
// findColumnCollation / resolveExprCollationMultiTable — GROUP BY with COLLATE
// ---------------------------------------------------------------------------

// chGroupRow3 holds a 3-column group-by result row.
type chGroupRow3 struct {
	name  string
	cnt   int64
	total int64
}

// chScanGroupRows3 scans rows with (string, int64, int64) columns.
func chScanGroupRows3(t *testing.T, rows *sql.Rows) []chGroupRow3 {
	t.Helper()
	var got []chGroupRow3
	for rows.Next() {
		var r chGroupRow3
		if err := rows.Scan(&r.name, &r.cnt, &r.total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}

// chGroupRow2 holds a 2-column group-by result row.
type chGroupRow2 struct {
	name  string
	total int64
}

// chScanGroupRows2 scans rows with (string, int64) columns.
func chScanGroupRows2(t *testing.T, rows *sql.Rows) []chGroupRow2 {
	t.Helper()
	var got []chGroupRow2
	for rows.Next() {
		var r chGroupRow2
		if err := rows.Scan(&r.name, &r.total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return got
}

// TestCompileHelpersJoinAggGroupByCollate exercises resolveExprCollationMultiTable
// and findColumnCollation via a JOIN + GROUP BY with a COLLATE expression,
// hitting the CollateExpr, ParenExpr and IdentExpr branches.
func TestCompileHelpersJoinAggGroupByCollate(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE categories (cid INTEGER PRIMARY KEY, cname TEXT COLLATE NOCASE)")
	chExec(t, db, "CREATE TABLE products   (pid INTEGER PRIMARY KEY, cid INTEGER, price INTEGER)")
	chExec(t, db, "INSERT INTO categories VALUES(1,'widgets')")
	chExec(t, db, "INSERT INTO categories VALUES(2,'gadgets')")
	chExec(t, db, "INSERT INTO products VALUES(1,1,10)")
	chExec(t, db, "INSERT INTO products VALUES(2,1,20)")
	chExec(t, db, "INSERT INTO products VALUES(3,2,30)")

	rows, err := db.Query(
		`SELECT categories.cname, COUNT(*) AS cnt, SUM(products.price) AS total
		 FROM categories INNER JOIN products ON categories.cid = products.cid
		 GROUP BY categories.cname
		 ORDER BY categories.cname`,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	got := chScanGroupRows3(t, rows)

	if len(got) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(got), got)
	}
	want := []chGroupRow3{{"gadgets", 1, 30}, {"widgets", 2, 30}}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("group %d: got %+v, want %+v", i, got[i], w)
		}
	}
}

// TestCompileHelpersJoinAggGroupByColumnCollation exercises findColumnCollation
// with the unqualified-column branch (first-match path across tables).
func TestCompileHelpersJoinAggGroupByColumnCollation(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE teams  (tid INTEGER PRIMARY KEY, tname TEXT COLLATE NOCASE)")
	chExec(t, db, "CREATE TABLE scores (sid INTEGER PRIMARY KEY, tid INTEGER, pts INTEGER)")
	chExec(t, db, "INSERT INTO teams VALUES(1,'Alpha')")
	chExec(t, db, "INSERT INTO teams VALUES(2,'Beta')")
	chExec(t, db, "INSERT INTO scores VALUES(1,1,5)")
	chExec(t, db, "INSERT INTO scores VALUES(2,1,10)")
	chExec(t, db, "INSERT INTO scores VALUES(3,2,7)")

	rows, err := db.Query(
		`SELECT tname, SUM(pts) FROM teams INNER JOIN scores ON teams.tid = scores.tid GROUP BY tname ORDER BY tname`,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	got := chScanGroupRows2(t, rows)

	if len(got) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(got))
	}
	if got[0].name != "Alpha" || got[0].total != 15 {
		t.Errorf("Alpha: %+v", got[0])
	}
	if got[1].name != "Beta" || got[1].total != 7 {
		t.Errorf("Beta: %+v", got[1])
	}
}

// TestCompileHelpersJoinAggGroupByTableQualifiedCollation exercises
// findColumnCollation via the table-qualified identifier branch.
func TestCompileHelpersJoinAggGroupByTableQualifiedCollation(t *testing.T) {
	db := chOpenMem(t)

	chExec(t, db, "CREATE TABLE brands (bid INTEGER PRIMARY KEY, bname TEXT COLLATE NOCASE)")
	chExec(t, db, "CREATE TABLE items  (iid INTEGER PRIMARY KEY, bid INTEGER, qty INTEGER)")
	chExec(t, db, "INSERT INTO brands VALUES(1,'Acme')")
	chExec(t, db, "INSERT INTO brands VALUES(2,'Bravo')")
	chExec(t, db, "INSERT INTO items VALUES(1,1,3)")
	chExec(t, db, "INSERT INTO items VALUES(2,2,7)")
	chExec(t, db, "INSERT INTO items VALUES(3,1,2)")

	rows, err := db.Query(
		`SELECT brands.bname, SUM(items.qty) FROM brands INNER JOIN items ON brands.bid = items.bid GROUP BY brands.bname ORDER BY brands.bname`,
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	got := chScanGroupRows2(t, rows)

	if len(got) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(got))
	}
	if got[0].name != "Acme" || got[0].total != 5 {
		t.Errorf("Acme: %+v", got[0])
	}
	if got[1].name != "Bravo" || got[1].total != 7 {
		t.Errorf("Bravo: %+v", got[1])
	}
}
