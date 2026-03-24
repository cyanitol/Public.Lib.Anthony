// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

// ---------------------------------------------------------------------------
// findColumnCollation (compile_join_agg.go)
// ---------------------------------------------------------------------------

// TestFindColumnCollation_JoinOrderByCollatedColumn exercises the unqualified
// identifier path in findColumnCollation through a multi-table JOIN whose
// ORDER BY column carries a declared NOCASE collation on the table.
func TestFindColumnCollation_JoinOrderByCollatedColumn(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE colors(id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)",
		"CREATE TABLE items(id INTEGER PRIMARY KEY, color_id INTEGER, label TEXT)",
		"INSERT INTO colors VALUES(1,'Red'),(2,'green'),(3,'BLUE')",
		"INSERT INTO items VALUES(1,1,'apple'),(2,2,'leaf'),(3,3,'sky')",
	})

	rows, err := db.Query(
		"SELECT items.label, colors.name FROM items JOIN colors ON items.color_id = colors.id ORDER BY colors.name",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var label, name string
		if err := rows.Scan(&label, &name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestFindColumnCollation_TableQualifiedCollatedColumn exercises the
// table-qualified path in findColumnCollation.
func TestFindColumnCollation_TableQualifiedCollatedColumn(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE words(id INTEGER PRIMARY KEY, term TEXT COLLATE NOCASE)",
		"CREATE TABLE refs(id INTEGER PRIMARY KEY, word_id INTEGER)",
		"INSERT INTO words VALUES(1,'Alpha'),(2,'beta'),(3,'GAMMA')",
		"INSERT INTO refs VALUES(1,1),(2,2),(3,3)",
	})

	rows, err := db.Query(
		"SELECT words.term FROM words JOIN refs ON words.id = refs.word_id ORDER BY words.term",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var term string
		if err := rows.Scan(&term); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		results = append(results, term)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 rows, got %d", len(results))
	}
}

// ---------------------------------------------------------------------------
// findColumnTableIndex (compile_join.go)
// ---------------------------------------------------------------------------

// TestFindColumnTableIndex_ThreeTableJoin exercises the table-qualified path
// in findColumnTableIndex through a 3-table join with ORDER BY a column from
// a specific table.
func TestFindColumnTableIndex_ThreeTableJoin(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE dept(id INTEGER PRIMARY KEY, dname TEXT)",
		"CREATE TABLE emp(id INTEGER PRIMARY KEY, ename TEXT, dept_id INTEGER)",
		"CREATE TABLE proj(id INTEGER PRIMARY KEY, pname TEXT, emp_id INTEGER)",
		"INSERT INTO dept VALUES(1,'Engineering'),(2,'Marketing')",
		"INSERT INTO emp VALUES(1,'Alice',1),(2,'Bob',2),(3,'Carol',1)",
		"INSERT INTO proj VALUES(1,'Alpha',1),(2,'Beta',2),(3,'Gamma',3)",
	})

	rows, err := db.Query(
		"SELECT emp.ename, dept.dname, proj.pname " +
			"FROM emp JOIN dept ON emp.dept_id = dept.id " +
			"JOIN proj ON proj.emp_id = emp.id " +
			"ORDER BY dept.dname",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var ename, dname, pname string
		if err := rows.Scan(&ename, &dname, &pname); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// TestFindColumnTableIndex_UnqualifiedFallback exercises the unqualified
// column-search fallback in findColumnTableIndex.
func TestFindColumnTableIndex_UnqualifiedFallback(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE a(id INTEGER PRIMARY KEY, val TEXT)",
		"CREATE TABLE b(id INTEGER PRIMARY KEY, ref INTEGER, extra TEXT)",
		"CREATE TABLE c(id INTEGER PRIMARY KEY, b_ref INTEGER, score INTEGER)",
		"INSERT INTO a VALUES(1,'x'),(2,'y')",
		"INSERT INTO b VALUES(1,1,'p'),(2,2,'q')",
		"INSERT INTO c VALUES(1,1,10),(2,2,20)",
	})

	rows, err := db.Query(
		"SELECT a.val, b.extra, c.score " +
			"FROM a JOIN b ON a.id = b.ref " +
			"JOIN c ON b.id = c.b_ref " +
			"ORDER BY score",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var val, extra string
		var score int64
		if err := rows.Scan(&val, &extra, &score); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// extractPragmaTVFArg (compile_pragma_tvf.go)
// ---------------------------------------------------------------------------

// TestExtractPragmaTVFArg_TableInfo exercises extractPragmaTVFArg via
// pragma_table_info with a string literal argument.
func TestExtractPragmaTVFArg_TableInfo(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE mytable(id INTEGER PRIMARY KEY, name TEXT, value REAL)",
	})

	rows, err := db.Query("SELECT name FROM pragma_table_info('mytable') ORDER BY cid")
	if err != nil {
		t.Fatalf("pragma_table_info query failed: %v", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		cols = append(cols, name)
	}
	if len(cols) != 3 {
		t.Errorf("expected 3 columns, got %d", len(cols))
	}
}

// TestExtractPragmaTVFArg_IndexList exercises extractPragmaTVFArg via
// pragma_index_list.
func TestExtractPragmaTVFArg_IndexList(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE indexed_tbl(id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE INDEX idx_name ON indexed_tbl(name)",
	})

	rows, err := db.Query("SELECT name FROM pragma_index_list('indexed_tbl')")
	if err != nil {
		t.Fatalf("pragma_index_list query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count < 1 {
		t.Errorf("expected at least 1 index, got %d", count)
	}
}

// TestExtractPragmaTVFArg_ForeignKeyList exercises extractPragmaTVFArg via
// pragma_foreign_key_list.
func TestExtractPragmaTVFArg_ForeignKeyList(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE parent(id INTEGER PRIMARY KEY)",
		"CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
	})

	rows, err := db.Query("SELECT \"table\" FROM pragma_foreign_key_list('child')")
	if err != nil {
		t.Fatalf("pragma_foreign_key_list query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var tbl string
		if err := rows.Scan(&tbl); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count < 1 {
		t.Errorf("expected at least 1 foreign key, got %d", count)
	}
}

// TestExtractPragmaTVFArg_DatabaseList exercises pragma_database_list (no arg).
func TestExtractPragmaTVFArg_DatabaseList(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	rows, err := db.Query("SELECT name FROM pragma_database_list")
	if err != nil {
		t.Fatalf("pragma_database_list query failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		count++
	}
	if count < 1 {
		t.Errorf("expected at least 1 database, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// emitExtraOrderByColumnMultiTable (compile_helpers.go)
// ---------------------------------------------------------------------------

// TestEmitExtraOrderByColumnMultiTable_OrderByNonSelectedColumn exercises
// emitExtraOrderByColumnMultiTable through a multi-table SELECT with ORDER BY
// on a column that does not appear in the result set.
func TestEmitExtraOrderByColumnMultiTable_OrderByNonSelectedColumn(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
		"CREATE TABLE categories(id INTEGER PRIMARY KEY, cat TEXT, product_id INTEGER)",
		"INSERT INTO products VALUES(1,'Widget',100),(2,'Gadget',200),(3,'Doohickey',50)",
		"INSERT INTO categories VALUES(1,'Tools',1),(2,'Electronics',2),(3,'Misc',3)",
	})

	// ORDER BY price is not in the SELECT list; exercises emitExtraOrderByColumnMultiTable
	rows, err := db.Query(
		"SELECT products.name, categories.cat " +
			"FROM products JOIN categories ON products.id = categories.product_id " +
			"ORDER BY products.price",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name, cat string
		if err := rows.Scan(&name, &cat); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		names = append(names, name)
	}
	if len(names) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(names))
	}
	// Results ordered by price ascending: Doohickey(50), Widget(100), Gadget(200)
	if names[0] != "Doohickey" {
		t.Errorf("expected Doohickey first (lowest price), got %s", names[0])
	}
}

// TestEmitExtraOrderByColumnMultiTable_OrderByColumnFromSecondTable exercises
// ordering by a column from the second table not in the SELECT list.
func TestEmitExtraOrderByColumnMultiTable_OrderByColumnFromSecondTable(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT)",
		"CREATE TABLE shipments(id INTEGER PRIMARY KEY, order_id INTEGER, priority INTEGER)",
		"INSERT INTO orders VALUES(1,'Alice'),(2,'Bob'),(3,'Carol')",
		"INSERT INTO shipments VALUES(1,1,3),(2,2,1),(3,3,2)",
	})

	rows, err := db.Query(
		"SELECT orders.customer " +
			"FROM orders JOIN shipments ON orders.id = shipments.order_id " +
			"ORDER BY shipments.priority",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var customers []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		customers = append(customers, c)
	}
	if len(customers) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(customers))
	}
	// priority order: Bob(1), Carol(2), Alice(3)
	if customers[0] != "Bob" {
		t.Errorf("expected Bob first (priority=1), got %s", customers[0])
	}
}

// ---------------------------------------------------------------------------
// findColumnIndex (compile_select_agg.go)
// ---------------------------------------------------------------------------

// TestFindColumnIndex_WindowFunctionOrderByName exercises findColumnIndex via
// a window function whose OVER clause references a column by name.
func TestFindColumnIndex_WindowFunctionOrderByName(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE scores(id INTEGER PRIMARY KEY, player TEXT, score INTEGER)",
		"INSERT INTO scores VALUES(1,'Alice',80),(2,'Bob',95),(3,'Carol',70)",
	})

	vals := queryRows(t, db, "SELECT row_number() OVER (ORDER BY score) FROM scores")
	if len(vals) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(vals))
	}
}

// TestFindColumnIndex_WindowFunctionOrderByNameDesc exercises findColumnIndex
// via a window function with descending ORDER BY on a named column.
func TestFindColumnIndex_WindowFunctionOrderByNameDesc(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE events(id INTEGER PRIMARY KEY, ts INTEGER, kind TEXT)",
		"INSERT INTO events VALUES(1,100,'a'),(2,200,'b'),(3,150,'c')",
	})

	vals := queryRows(t, db, "SELECT row_number() OVER (ORDER BY ts DESC) FROM events")
	if len(vals) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(vals))
	}
	// row_number is positional 1,2,3 regardless of sort direction
	if vals[0][0] != int64(1) || vals[2][0] != int64(3) {
		t.Errorf("unexpected row_number values: %v", vals)
	}
}

// TestFindColumnIndex_WindowFunctionCaseInsensitiveColumn exercises the
// case-insensitive match path in findColumnIndex.
func TestFindColumnIndex_WindowFunctionCaseInsensitiveColumn(t *testing.T) {
	t.Parallel()
	db, done := openMemDB(t)
	defer done()

	execAll(t, db, []string{
		"CREATE TABLE metrics(id INTEGER PRIMARY KEY, Value INTEGER)",
		"INSERT INTO metrics VALUES(1,30),(2,10),(3,20)",
	})

	// ORDER BY value (lowercase) while column is declared as Value (mixed case)
	vals := queryRows(t, db, "SELECT row_number() OVER (ORDER BY value) FROM metrics")
	if len(vals) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(vals))
	}
}
