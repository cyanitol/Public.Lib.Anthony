// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// cj3OpenDB opens an in-memory database for compile_join3_coverage tests.
func cj3OpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatalf("cj3OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// cj3Exec executes one or more SQL statements, failing the test on error.
func cj3Exec(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("cj3Exec %q: %v", s, err)
		}
	}
}

// cj3QueryRows fetches all rows as [][]interface{}.
func cj3QueryRows(t *testing.T, db *sql.DB, query string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("cj3QueryRows %q: %v", query, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("cj3QueryRows scan: %v", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("cj3QueryRows err: %v", err)
	}
	return result
}

// ============================================================================
// findColumnTableIndex — column-to-table index resolution
// ============================================================================

// TestCompileJoin3CoverageFindColumnTableIndexSecondTable exercises the path
// where the column is found in the second table (not the first).  A LEFT JOIN
// with ORDER BY forces findColumnTableIndex to be called for each result column
// during emitLeafRowSorter, and "score" only exists in the second table.
func TestCompileJoin3CoverageFindColumnTableIndexSecondTable(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE fct3_a(id INTEGER, name TEXT)",
		"CREATE TABLE fct3_b(id INTEGER, ref INTEGER, score INTEGER)",
		"INSERT INTO fct3_a VALUES(1,'alpha'),(2,'beta'),(3,'gamma')",
		"INSERT INTO fct3_b VALUES(10,1,100),(11,2,200)",
	)
	// ORDER BY score forces emitLeafRowSorter -> findColumnTableIndex to locate
	// "score" in fct3_b (second table, index 1) rather than fct3_a (index 0).
	rows := cj3QueryRows(t, db,
		"SELECT fct3_a.name, fct3_b.score "+
			"FROM fct3_a LEFT JOIN fct3_b ON fct3_a.id = fct3_b.ref "+
			"ORDER BY fct3_b.score ASC")
	// Expect 3 rows (with id=3 -> NULL score)
	if len(rows) != 3 {
		t.Fatalf("LEFT JOIN rows: got %d, want 3", len(rows))
	}
}

// TestCompileJoin3CoverageFindColumnTableIndexQualifiedSecondTable exercises
// the table-qualified path in findColumnTableIndex where the table qualifier
// matches the second table by alias.
func TestCompileJoin3CoverageFindColumnTableIndexQualifiedSecondTable(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE fct3q_a(id INTEGER, label TEXT)",
		"CREATE TABLE fct3q_b(id INTEGER, ref INTEGER, rank INTEGER)",
		"INSERT INTO fct3q_a VALUES(1,'p'),(2,'q'),(3,'r')",
		"INSERT INTO fct3q_b VALUES(1,1,30),(2,3,10)",
	)
	// Use explicit table qualifier on the ORDER BY column from the second table.
	rows := cj3QueryRows(t, db,
		"SELECT fct3q_a.label, fct3q_b.rank "+
			"FROM fct3q_a LEFT JOIN fct3q_b ON fct3q_a.id = fct3q_b.ref "+
			"ORDER BY fct3q_b.rank ASC")
	if len(rows) != 3 {
		t.Fatalf("qualified LEFT JOIN rows: got %d, want 3", len(rows))
	}
}

// TestCompileJoin3CoverageFindColumnTableIndexNonIdentExpr exercises the branch
// where the result column expression is not an IdentExpr (e.g. an arithmetic
// expression), which causes findColumnTableIndex to return 0 immediately.
func TestCompileJoin3CoverageFindColumnTableIndexNonIdentExpr(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE fct3ni_a(id INTEGER, val INTEGER)",
		"CREATE TABLE fct3ni_b(id INTEGER, ref INTEGER, extra INTEGER)",
		"INSERT INTO fct3ni_a VALUES(1,10),(2,20),(3,30)",
		"INSERT INTO fct3ni_b VALUES(1,1,5),(2,2,15)",
	)
	// Arithmetic expression in SELECT forces a non-IdentExpr result column.
	rows := cj3QueryRows(t, db,
		"SELECT fct3ni_a.val + fct3ni_b.extra, fct3ni_a.id "+
			"FROM fct3ni_a LEFT JOIN fct3ni_b ON fct3ni_a.id = fct3ni_b.ref "+
			"ORDER BY fct3ni_a.id")
	if len(rows) != 3 {
		t.Fatalf("non-ident LEFT JOIN rows: got %d, want 3", len(rows))
	}
}

// ============================================================================
// resolveUsingJoin — USING clause resolution
// ============================================================================

// TestCompileJoin3CoverageResolveUsingJoinEmptyUsing covers the early-return
// branch of resolveUsingJoin when the USING list is empty.
func TestCompileJoin3CoverageResolveUsingJoinEmptyUsing(t *testing.T) {
	tbl := &schema.Table{
		Name:    "t1",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}},
	}
	tables := []stmtTableInfo{
		{name: "t1", table: tbl, cursorIdx: 0},
		{name: "t2", table: tbl, cursorIdx: 1},
	}

	join := &parser.JoinClause{
		Type:      parser.JoinInner,
		Condition: parser.JoinCondition{Using: nil}, // empty -> early return
	}

	resolveUsingJoin(join, tables, 1)

	// After early-return: On must still be nil.
	if join.Condition.On != nil {
		t.Errorf("expected On=nil after empty Using, got %v", join.Condition.On)
	}
}

// TestCompileJoin3CoverageResolveUsingJoinSingleColumn covers the non-empty
// USING path that calls buildEqualityChain and sets an equality expression.
func TestCompileJoin3CoverageResolveUsingJoinSingleColumn(t *testing.T) {
	tbl1 := &schema.Table{
		Name:    "left_tbl",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
	}
	tbl2 := &schema.Table{
		Name:    "right_tbl",
		Columns: []*schema.Column{{Name: "id", Type: "INTEGER"}, {Name: "score", Type: "INTEGER"}},
	}
	tables := []stmtTableInfo{
		{name: "left_tbl", table: tbl1, cursorIdx: 0},
		{name: "right_tbl", table: tbl2, cursorIdx: 1},
	}

	join := &parser.JoinClause{
		Type:      parser.JoinInner,
		Condition: parser.JoinCondition{Using: []string{"id"}},
	}

	resolveUsingJoin(join, tables, 1)

	if join.Condition.On == nil {
		t.Error("expected On != nil after resolveUsingJoin with USING(id)")
	}
}

// TestCompileJoin3CoverageResolveUsingJoinTwoColumns covers the multi-column
// USING path which builds an AND-chain of equalities.
func TestCompileJoin3CoverageResolveUsingJoinTwoColumns(t *testing.T) {
	tbl := &schema.Table{
		Name: "shared",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "cat", Type: "INTEGER"},
			{Name: "extra", Type: "TEXT"},
		},
	}
	tables := []stmtTableInfo{
		{name: "a", table: tbl, cursorIdx: 0},
		{name: "b", table: tbl, cursorIdx: 1},
	}

	join := &parser.JoinClause{
		Type:      parser.JoinInner,
		Condition: parser.JoinCondition{Using: []string{"id", "cat"}},
	}

	resolveUsingJoin(join, tables, 1)

	if join.Condition.On == nil {
		t.Error("expected On != nil after resolveUsingJoin with USING(id,cat)")
	}
	// The expression should be an AND chain: BinaryExpr with OpAnd at the root.
	binExpr, ok := join.Condition.On.(*parser.BinaryExpr)
	if !ok {
		t.Fatalf("expected *BinaryExpr, got %T", join.Condition.On)
	}
	if binExpr.Op != parser.OpAnd {
		t.Errorf("expected OpAnd at root for 2-column USING, got %v", binExpr.Op)
	}
}

// TestCompileJoin3CoverageResolveUsingJoinViaSQL exercises the full
// resolveUsingJoin code path end-to-end through SQL compilation.
func TestCompileJoin3CoverageResolveUsingJoinViaSQL(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE ru3_a(code TEXT, name TEXT)",
		"CREATE TABLE ru3_b(code TEXT, price INTEGER)",
		"INSERT INTO ru3_a VALUES('X','Xenon'),('Y','Yttrium'),('Z','Zinc')",
		"INSERT INTO ru3_b VALUES('X',100),('Z',300)",
	)
	rows := cj3QueryRows(t, db,
		"SELECT ru3_a.name, ru3_b.price FROM ru3_a JOIN ru3_b USING(code) ORDER BY ru3_a.code")
	if len(rows) != 2 {
		t.Fatalf("USING join rows: got %d, want 2", len(rows))
	}
	if rows[0][0] != "Xenon" {
		t.Errorf("row 0 name: got %v, want Xenon", rows[0][0])
	}
}

// ============================================================================
// emitExtraOrderByColumnMultiTable — ORDER BY column in multi-table context
// ============================================================================

// TestCompileJoin3CoverageEmitExtraOrderByColumnNormalCol exercises the
// non-rowid branch (OpColumn) of emitExtraOrderByColumnMultiTable via a JOIN
// query with ORDER BY referencing a plain non-pk column.
func TestCompileJoin3CoverageEmitExtraOrderByColumnNormalCol(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE eob3_a(id INTEGER, name TEXT)",
		"CREATE TABLE eob3_b(id INTEGER, ref INTEGER, weight INTEGER)",
		"INSERT INTO eob3_a VALUES(1,'alpha'),(2,'beta'),(3,'gamma')",
		"INSERT INTO eob3_b VALUES(1,1,50),(2,2,10),(3,3,30)",
	)
	// ORDER BY weight exercises emitExtraOrderByColumnMultiTable for the "weight"
	// column in eob3_b — a plain non-primary-key column (OpColumn path).
	rows := cj3QueryRows(t, db,
		"SELECT eob3_a.name, eob3_b.weight FROM eob3_a JOIN eob3_b ON eob3_a.id = eob3_b.ref "+
			"ORDER BY eob3_b.weight ASC")
	if len(rows) != 3 {
		t.Fatalf("ORDER BY weight rows: got %d, want 3", len(rows))
	}
	w0, _ := rows[0][1].(int64)
	if w0 != 10 {
		t.Errorf("first weight: got %d, want 10", w0)
	}
}

// TestCompileJoin3CoverageEmitExtraOrderByColumnRowidCol exercises the rowid
// (OpRowid) path of emitExtraOrderByColumnMultiTable via ORDER BY on an INTEGER
// PRIMARY KEY column in a multi-table JOIN.
func TestCompileJoin3CoverageEmitExtraOrderByColumnRowidCol(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE eobrk_a(id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE eobrk_b(id INTEGER PRIMARY KEY, ref INTEGER, note TEXT)",
		"INSERT INTO eobrk_a VALUES(1,'first'),(2,'second'),(3,'third')",
		"INSERT INTO eobrk_b VALUES(10,1,'n1'),(11,2,'n2'),(12,3,'n3')",
	)
	// ORDER BY eobrk_a.id where id is INTEGER PRIMARY KEY -> rowid alias -> OpRowid path.
	rows := cj3QueryRows(t, db,
		"SELECT eobrk_a.name, eobrk_b.note FROM eobrk_a JOIN eobrk_b ON eobrk_a.id = eobrk_b.ref "+
			"ORDER BY eobrk_a.id DESC")
	if len(rows) != 3 {
		t.Fatalf("rowid ORDER BY rows: got %d, want 3", len(rows))
	}
	// DESC order -> id=3 first
	if rows[0][0] != "third" {
		t.Errorf("first name DESC: got %v, want third", rows[0][0])
	}
}

// TestCompileJoin3CoverageEmitExtraOrderByColumnNotFound exercises the
// "column not found in any table -> emit NULL" path of
// emitExtraOrderByColumnMultiTable by calling the method directly.
func TestCompileJoin3CoverageEmitExtraOrderByColumnNotFound(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)
	vm := vdbe.New()

	tbl := &schema.Table{
		Name:    "t",
		Columns: []*schema.Column{{Name: "x", Type: "INTEGER"}},
	}
	tables := []stmtTableInfo{
		{name: "t", table: tbl, cursorIdx: 0},
	}

	// "nonexistent_col" is not in any table -> should emit OpNull.
	s.emitExtraOrderByColumnMultiTable(vm, tables, "nonexistent_col", 1)

	if len(vm.Program) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(vm.Program))
	}
	if vm.Program[0].Opcode != vdbe.OpNull {
		t.Errorf("expected OpNull for unknown column, got opcode %v", vm.Program[0].Opcode)
	}
}

// TestCompileJoin3CoverageEmitExtraOrderByColumnRowidAlias exercises the
// rowid-alias (tableColIdx == -2) path of emitExtraOrderByColumnMultiTable.
// GetColumnIndexWithRowidAliases returns -2 for "rowid"/"_rowid_"/"oid" when
// no INTEGER PRIMARY KEY exists on a regular (non-WITHOUT-ROWID) table.
func TestCompileJoin3CoverageEmitExtraOrderByColumnRowidAlias(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)
	vm := vdbe.New()

	tbl := &schema.Table{
		Name: "no_ipk",
		Columns: []*schema.Column{
			{Name: "val", Type: "TEXT"},
		},
		WithoutRowID: false, // regular rowid table — "rowid" alias applies
	}
	tables := []stmtTableInfo{
		{name: "no_ipk", table: tbl, cursorIdx: 0},
	}

	// "rowid" triggers GetColumnIndexWithRowidAliases returning -2.
	s.emitExtraOrderByColumnMultiTable(vm, tables, "rowid", 1)

	if len(vm.Program) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(vm.Program))
	}
	if vm.Program[0].Opcode != vdbe.OpRowid {
		t.Errorf("expected OpRowid for rowid alias, got opcode %v", vm.Program[0].Opcode)
	}
}

// TestCompileJoin3CoverageEmitExtraOrderByColumnNormalColDirect exercises
// the OpColumn branch directly: a normal (non-rowid) column found in tables.
func TestCompileJoin3CoverageEmitExtraOrderByColumnNormalColDirect(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)
	vm := vdbe.New()

	tbl := &schema.Table{
		Name: "tbl_direct",
		Columns: []*schema.Column{
			{Name: "name", Type: "TEXT"},
			{Name: "score", Type: "INTEGER"},
		},
	}
	tables := []stmtTableInfo{
		{name: "tbl_direct", table: tbl, cursorIdx: 2},
	}

	// "score" is at column index 1, a normal column -> should emit OpColumn.
	s.emitExtraOrderByColumnMultiTable(vm, tables, "score", 5)

	if len(vm.Program) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(vm.Program))
	}
	if vm.Program[0].Opcode != vdbe.OpColumn {
		t.Errorf("expected OpColumn for normal column, got opcode %v", vm.Program[0].Opcode)
	}
	if vm.Program[0].P1 != 2 {
		t.Errorf("OpColumn P1 (cursor): got %d, want 2", vm.Program[0].P1)
	}
	if vm.Program[0].P3 != 5 {
		t.Errorf("OpColumn P3 (targetReg): got %d, want 5", vm.Program[0].P3)
	}
}

// ============================================================================
// fixInnerRewindAddresses — patch Rewind P2 for inner-loop cursors
// ============================================================================

// TestCompileJoin3CoverageFixInnerRewindNoRewind covers the iteration where no
// Rewind with P2=0 exists — all instructions should be skipped untouched.
func TestCompileJoin3CoverageFixInnerRewindNoRewind(t *testing.T) {
	vm := vdbe.New()
	// Only a Next instruction — no Rewind at all.
	vm.AddOp(vdbe.OpNext, 0, 1, 0)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)

	fixInnerRewindAddresses(vm)

	// The Next P2 should remain 1 (unchanged).
	if vm.Program[0].P2 != 1 {
		t.Errorf("OpNext P2 changed unexpectedly: got %d", vm.Program[0].P2)
	}
}

// TestCompileJoin3CoverageFixInnerRewindAlreadyPatched covers the branch where
// a Rewind exists but P2 != 0 (already patched) — it must be skipped.
func TestCompileJoin3CoverageFixInnerRewindAlreadyPatched(t *testing.T) {
	vm := vdbe.New()
	vm.AddOp(vdbe.OpRewind, 0, 5, 0) // addr 0, P2=5 (non-zero, already patched)
	vm.AddOp(vdbe.OpNext, 0, 1, 0)   // addr 1
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)   // addr 2

	fixInnerRewindAddresses(vm)

	if vm.Program[0].P2 != 5 {
		t.Errorf("pre-patched Rewind P2 changed: got %d, want 5", vm.Program[0].P2)
	}
}

// TestCompileJoin3CoverageFixInnerRewindPatchedToNext covers the main happy
// path: a Rewind with P2=0 followed by a Next for the same cursor.
// fixInnerRewindAddresses should set Rewind.P2 = (Next address + 1).
func TestCompileJoin3CoverageFixInnerRewindPatchedToNext(t *testing.T) {
	vm := vdbe.New()
	vm.AddOp(vdbe.OpRewind, 1, 0, 0) // addr 0: Rewind cursor 1, P2=0 (unfixed)
	vm.AddOp(vdbe.OpNull, 0, 1, 0)   // addr 1: body
	vm.AddOp(vdbe.OpNext, 1, 1, 0)   // addr 2: Next cursor 1 — the matching Next
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)   // addr 3

	fixInnerRewindAddresses(vm)

	// Rewind P2 should now be 3 (one past the Next at addr 2).
	if vm.Program[0].P2 != 3 {
		t.Errorf("Rewind P2 after fix: got %d, want 3", vm.Program[0].P2)
	}
}

// TestCompileJoin3CoverageFixInnerRewindNoMatchingNext covers the branch where
// a Rewind(P2=0) exists but no matching Next is found for that cursor — P2
// must remain 0.
func TestCompileJoin3CoverageFixInnerRewindNoMatchingNext(t *testing.T) {
	vm := vdbe.New()
	vm.AddOp(vdbe.OpRewind, 1, 0, 0) // addr 0: Rewind cursor 1, P2=0
	vm.AddOp(vdbe.OpNext, 0, 1, 0)   // addr 1: Next cursor 0 (different cursor)
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)   // addr 2

	fixInnerRewindAddresses(vm)

	// No matching Next for cursor 1 -> P2 stays 0.
	if vm.Program[0].P2 != 0 {
		t.Errorf("Rewind P2 after no-match: got %d, want 0", vm.Program[0].P2)
	}
}

// TestCompileJoin3CoverageFixInnerRewindMultipleCursors covers multiple
// Rewind(P2=0) instructions for different cursors, each fixed independently.
func TestCompileJoin3CoverageFixInnerRewindMultipleCursors(t *testing.T) {
	vm := vdbe.New()
	vm.AddOp(vdbe.OpRewind, 0, 0, 0) // addr 0: Rewind cursor 0, P2=0
	vm.AddOp(vdbe.OpRewind, 1, 0, 0) // addr 1: Rewind cursor 1, P2=0
	vm.AddOp(vdbe.OpNull, 0, 1, 0)   // addr 2: body
	vm.AddOp(vdbe.OpNext, 1, 2, 0)   // addr 3: Next cursor 1
	vm.AddOp(vdbe.OpNext, 0, 1, 0)   // addr 4: Next cursor 0
	vm.AddOp(vdbe.OpHalt, 0, 0, 0)   // addr 5

	fixInnerRewindAddresses(vm)

	// Rewind cursor 0 (addr 0): first Next for cursor 0 is at addr 4 -> P2 = 5
	if vm.Program[0].P2 != 5 {
		t.Errorf("Rewind cursor 0 P2: got %d, want 5", vm.Program[0].P2)
	}
	// Rewind cursor 1 (addr 1): first Next for cursor 1 is at addr 3 -> P2 = 4
	if vm.Program[1].P2 != 4 {
		t.Errorf("Rewind cursor 1 P2: got %d, want 4", vm.Program[1].P2)
	}
}

// TestCompileJoin3CoverageFixInnerRewindViaRecursiveCTE exercises
// fixInnerRewindAddresses indirectly through a recursive CTE execution.
func TestCompileJoin3CoverageFixInnerRewindViaRecursiveCTE(t *testing.T) {
	t.Parallel()
	db := cj3OpenDB(t)
	cj3Exec(t, db,
		"CREATE TABLE fir3_node(id INTEGER, parent_id INTEGER, label TEXT)",
		"INSERT INTO fir3_node VALUES(1,NULL,'root'),(2,1,'child1'),(3,1,'child2'),(4,2,'leaf1')",
	)
	rows := cj3QueryRows(t, db, `
		WITH RECURSIVE tree(id, label) AS (
			SELECT id, label FROM fir3_node WHERE parent_id IS NULL
			UNION ALL
			SELECT n.id, n.label FROM fir3_node n JOIN tree t ON n.parent_id = t.id
		)
		SELECT label FROM tree ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("recursive CTE rows: got %d, want 4", len(rows))
	}
}
