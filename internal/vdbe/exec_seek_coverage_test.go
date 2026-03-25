// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ============================================================================
// Helpers
// ============================================================================

func seekOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("seekOpenDB: %v", err)
	}
	return db
}

func seekExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("seekExec %q: %v", q, err)
	}
}

func seekQueryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("seekQueryInt %q: %v", q, err)
	}
	return n
}

// seekQueryInts collects all rows of a single-integer query.
func seekQueryInts(t *testing.T, db *sql.DB, q string) []int {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("seekQueryInts %q: %v", q, err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("seekQueryInts scan: %v", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("seekQueryInts rows.Err: %v", err)
	}
	return out
}

// buildSeekTestBtree constructs a btree with a single page (page 1) containing
// rows with rowids 10, 20, 30, 40, 50. The layout mirrors the setup used in
// the internal seek tests.
func buildSeekTestBtree() *btree.Btree {
	bt := btree.NewBtree(4096)

	pageData := make([]byte, 4096)

	// SQLite file header on page 1 (100 bytes).
	copy(pageData[0:16], []byte("SQLite format 3\x00"))
	pageData[18] = 16
	pageData[19] = 0

	// Page header starts at byte 100 for page 1.
	hdr := 100
	pageData[hdr+0] = 0x0d // leaf table
	pageData[hdr+1] = 0x00
	pageData[hdr+2] = 0x00
	pageData[hdr+3] = 0x00
	pageData[hdr+4] = 0x05 // 5 cells
	pageData[hdr+5] = 0x00
	pageData[hdr+6] = 0xc8 // cell content area starts at 200
	pageData[hdr+7] = 0x00

	// Cell pointer array.
	pageData[hdr+8] = 0x00
	pageData[hdr+9] = 0xc8 // 200
	pageData[hdr+10] = 0x00
	pageData[hdr+11] = 0xf0 // 240
	pageData[hdr+12] = 0x01
	pageData[hdr+13] = 0x18 // 280
	pageData[hdr+14] = 0x01
	pageData[hdr+15] = 0x40 // 320
	pageData[hdr+16] = 0x01
	pageData[hdr+17] = 0x68 // 360

	// Encode a minimal SQLite record: header_size=3, serial_type_int8=1, serial_type_text1=15, int_byte, text_byte.
	buildRecord := func(intVal int64, textByte byte) []byte {
		return []byte{3, 1, 15, byte(intVal), textByte}
	}

	writeCell := func(offset, rowid int, data string) {
		rec := buildRecord(int64(rowid*10), data[0])
		pageData[offset] = byte(len(rec))
		offset++
		pageData[offset] = byte(rowid)
		offset++
		copy(pageData[offset:], rec)
	}

	writeCell(200, 10, "A")
	writeCell(240, 20, "B")
	writeCell(280, 30, "C")
	writeCell(320, 40, "D")
	writeCell(360, 50, "E")

	bt.SetPage(1, pageData)
	return bt
}

// buildEmptyBtree constructs a btree whose single page has page type 0x0d but
// zero cells, so MoveToFirst returns an error.
func buildEmptyBtree() *btree.Btree {
	bt := btree.NewBtree(4096)

	pageData := make([]byte, 4096)

	copy(pageData[0:16], []byte("SQLite format 3\x00"))
	pageData[18] = 16
	pageData[19] = 0

	hdr := 100
	pageData[hdr+0] = 0x0d // leaf table
	pageData[hdr+4] = 0x00 // 0 cells
	pageData[hdr+5] = 0x00
	pageData[hdr+6] = 0x00
	pageData[hdr+7] = 0x00

	bt.SetPage(1, pageData)
	return bt
}

// newSeekVDBE creates a ready VDBE backed by the supplied btree.
func newSeekVDBE(bt *btree.Btree, numMem, numCursors int) *vdbe.VDBE {
	v := vdbe.New()
	v.Ctx = &vdbe.VDBEContext{Btree: bt}
	v.AllocMemory(numMem)
	v.AllocCursors(numCursors)
	return v
}

// ============================================================================
// execSeekGT error-path coverage
// ============================================================================

// TestExecSeekGT_InvalidCursor covers the GetCursor error branch (P1 out of range).
func TestExecSeekGT_InvalidCursor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(5)
	v.AllocCursors(2)

	// P1=5 is outside [0,2), so GetCursor returns an error that propagates.
	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpSeekGT, 5, 3, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for SeekGT with out-of-range cursor, got nil")
	}
}

// TestExecSeekGT_InvalidRegister covers the GetMem error branch (P3 out of range).
func TestExecSeekGT_InvalidRegister(t *testing.T) {
	t.Parallel()
	bt := buildSeekTestBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpOpenRead, 0, 1, 2)
	// P3=99 is outside the allocated register range.
	v.AddOp(vdbe.OpSeekGT, 0, 5, 99)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for SeekGT with out-of-range register, got nil")
	}
}

// TestExecSeekGT_NilBtreeCursor covers the seekGetBtCursor-nil branch.
// The cursor exists but its BtreeCursor is a string (wrong type), so
// seekGetBtCursor returns nil and execSeekGT jumps to P2.
func TestExecSeekGT_NilBtreeCursor(t *testing.T) {
	t.Parallel()
	bt := buildSeekTestBtree()
	v := newSeekVDBE(bt, 5, 5)

	// Slot 0 holds a cursor with a non-BtCursor BtreeCursor value.
	v.Cursors[0] = &vdbe.Cursor{BtreeCursor: "not-a-btcursor"}

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpInteger, 25, 1, 0) // r1 = 25
	v.AddOp(vdbe.OpSeekGT, 0, 4, 1)  // seek > 25; jump to 4 if not found
	v.AddOp(vdbe.OpInteger, 1, 2, 0) // r2 = 1 (found — should not execute)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)    // 4: halt (not-found path)

	if err := v.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The cursor has no btree cursor, so the "not found" branch is taken (jump
	// to addr 4), and r2 is never set to 1.
	r2, _ := v.GetMem(2)
	if r2.IntValue() == 1 {
		t.Error("expected not-found path, but r2 was set to 1 (found path)")
	}
}

// TestExecSeekGT_EmptyTable covers the MoveToFirst-failure branch by using an
// empty table (0 cells), which causes descendToFirst to return an error.
func TestExecSeekGT_EmptyTable(t *testing.T) {
	t.Parallel()
	bt := buildEmptyBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpOpenRead, 0, 1, 2) // open cursor on the empty page
	v.AddOp(vdbe.OpInteger, 5, 1, 0)  // r1 = 5
	v.AddOp(vdbe.OpSeekGT, 0, 5, 1)  // jump to 5 if not found
	v.AddOp(vdbe.OpInteger, 1, 2, 0) // r2 = 1 (should not execute)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)    // 5: not-found path

	if err := v.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r2, _ := v.GetMem(2)
	if r2.IntValue() == 1 {
		t.Error("expected not-found path for empty table, but r2 was 1")
	}
}

// ============================================================================
// execSeekLT error-path coverage
// ============================================================================

// TestExecSeekLT_InvalidCursor covers the GetCursor error branch.
func TestExecSeekLT_InvalidCursor(t *testing.T) {
	t.Parallel()
	v := vdbe.New()
	v.AllocMemory(5)
	v.AllocCursors(2)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpSeekLT, 5, 3, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for SeekLT with out-of-range cursor, got nil")
	}
}

// TestExecSeekLT_InvalidRegister covers the GetMem error branch.
func TestExecSeekLT_InvalidRegister(t *testing.T) {
	t.Parallel()
	bt := buildSeekTestBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpOpenRead, 0, 1, 2)
	v.AddOp(vdbe.OpSeekLT, 0, 5, 99) // P3=99 out of range
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for SeekLT with out-of-range register, got nil")
	}
}

// TestExecSeekLT_NilBtreeCursor covers the seekGetBtCursor-nil branch.
func TestExecSeekLT_NilBtreeCursor(t *testing.T) {
	t.Parallel()
	bt := buildSeekTestBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.Cursors[0] = &vdbe.Cursor{BtreeCursor: "not-a-btcursor"}

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpInteger, 35, 1, 0) // r1 = 35
	v.AddOp(vdbe.OpSeekLT, 0, 4, 1)  // jump to 4 if not found
	v.AddOp(vdbe.OpInteger, 1, 2, 0) // r2 = 1 (should not execute)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)    // 4

	if err := v.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r2, _ := v.GetMem(2)
	if r2.IntValue() == 1 {
		t.Error("expected not-found path, but r2 was 1")
	}
}

// TestExecSeekLT_EmptyTable covers the findLastRowidLessThan → MoveToFirst
// error path. An empty table causes MoveToFirst to fail.
func TestExecSeekLT_EmptyTable(t *testing.T) {
	t.Parallel()
	bt := buildEmptyBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpOpenRead, 0, 1, 2) // empty table
	v.AddOp(vdbe.OpInteger, 50, 1, 0) // r1 = 50
	v.AddOp(vdbe.OpSeekLT, 0, 5, 1)  // jump to 5 if not found
	v.AddOp(vdbe.OpInteger, 1, 2, 0) // r2 = 1 (should not execute)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)    // 5

	if err := v.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r2, _ := v.GetMem(2)
	if r2.IntValue() == 1 {
		t.Error("expected not-found path for empty table, but r2 was 1")
	}
}

// ============================================================================
// execNotExists error-path coverage
// ============================================================================

// TestExecNotExists_InvalidRegister covers the GetMem error branch in execNotExists.
func TestExecNotExists_InvalidRegister(t *testing.T) {
	t.Parallel()
	bt := buildSeekTestBtree()
	v := newSeekVDBE(bt, 5, 5)

	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpOpenRead, 0, 1, 2)
	// P3=99 out of range → GetMem returns an error.
	v.AddOp(vdbe.OpNotExists, 0, 4, 99)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for NotExists with out-of-range register, got nil")
	}
}

// ============================================================================
// execClearEphemeral coverage
// ============================================================================

// TestExecClearEphemeral_NilCursorSlot covers the "cursor == nil" early-return
// path in execClearEphemeral.
func TestExecClearEphemeral_NilCursorSlot(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	v := vdbe.New()
	v.Ctx = &vdbe.VDBEContext{Btree: bt}
	v.AllocMemory(5)
	v.AllocCursors(5)

	// Slot 0 is explicitly nil (AllocCursors zero-initialises the slice, so
	// the slot is already nil — this exercises the cursor == nil branch).
	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpClearEphemeral, 0, 0, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err != nil {
		t.Fatalf("unexpected error for ClearEphemeral on nil cursor: %v", err)
	}
}

// TestExecClearEphemeral_InvalidIndex covers the "cursor index out of range"
// error path in execClearEphemeral.
func TestExecClearEphemeral_InvalidIndex(t *testing.T) {
	t.Parallel()
	bt := btree.NewBtree(4096)
	v := vdbe.New()
	v.Ctx = &vdbe.VDBEContext{Btree: bt}
	v.AllocMemory(5)
	v.AllocCursors(2)

	// P1=10 is outside the allocated cursor range.
	v.AddOp(vdbe.OpInit, 0, 1, 0)
	v.AddOp(vdbe.OpClearEphemeral, 10, 0, 0)
	v.AddOp(vdbe.OpHalt, 0, 0, 0)

	if err := v.Run(); err == nil {
		t.Error("expected error for ClearEphemeral with out-of-range cursor, got nil")
	}
}

// ============================================================================
// SQL-level: range scans exercising WHERE comparisons on rowid tables
// ============================================================================

// TestExecSeekSQL_RangeGT exercises a SELECT with WHERE id > ? style filtering.
// At the SQL level the engine performs a full scan with a filter expression;
// these tests verify that the result set is correct for boundary values.
func TestExecSeekSQL_RangeGT(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE nums(id INTEGER PRIMARY KEY, val TEXT)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		seekExec(t, db, "INSERT INTO nums VALUES("+itoa(v)+", 'v"+itoa(v)+"')")
	}

	t.Run("NoneFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums WHERE id > 5")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows, got %d: %v", len(rows), rows)
		}
	})

	t.Run("SomeFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums WHERE id > 3")
		if len(rows) != 2 {
			t.Errorf("expected 2 rows (4,5), got %d: %v", len(rows), rows)
		}
	})

	t.Run("AllFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums WHERE id > 0")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d: %v", len(rows), rows)
		}
	})
}

// TestExecSeekSQL_RangeLT exercises WHERE id < ? filtering.
func TestExecSeekSQL_RangeLT(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE nums2(id INTEGER PRIMARY KEY, val TEXT)")
	for _, v := range []int{10, 20, 30, 40, 50} {
		seekExec(t, db, "INSERT INTO nums2 VALUES("+itoa(v)+", 'v"+itoa(v)+"')")
	}

	t.Run("NoneFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums2 WHERE id < 10")
		if len(rows) != 0 {
			t.Errorf("expected 0 rows, got %d: %v", len(rows), rows)
		}
	})

	t.Run("SomeFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums2 WHERE id < 35")
		if len(rows) != 3 {
			t.Errorf("expected 3 rows (10,20,30), got %d: %v", len(rows), rows)
		}
	})

	t.Run("AllFound", func(t *testing.T) {
		rows := seekQueryInts(t, db, "SELECT id FROM nums2 WHERE id < 100")
		if len(rows) != 5 {
			t.Errorf("expected 5 rows, got %d: %v", len(rows), rows)
		}
	})
}

// TestExecSeekSQL_NotExistsSubquery exercises NOT EXISTS subquery patterns.
func TestExecSeekSQL_NotExistsSubquery(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE parent(id INTEGER PRIMARY KEY, name TEXT)")
	seekExec(t, db, "CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER)")
	seekExec(t, db, "INSERT INTO parent VALUES(1,'alice')")
	seekExec(t, db, "INSERT INTO parent VALUES(2,'bob')")
	seekExec(t, db, "INSERT INTO parent VALUES(3,'carol')")
	seekExec(t, db, "INSERT INTO child VALUES(10, 1)")
	seekExec(t, db, "INSERT INTO child VALUES(20, 3)")

	// Parents that have no children.
	rows := seekQueryInts(t, db,
		"SELECT id FROM parent WHERE NOT EXISTS (SELECT 1 FROM child WHERE child.pid = parent.id)")
	if len(rows) != 1 || rows[0] != 2 {
		t.Errorf("expected [2], got %v", rows)
	}
}

// TestExecSeekSQL_NotInSubquery exercises NOT IN which is related to the
// NOT EXISTS execution path.
func TestExecSeekSQL_NotInSubquery(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE items(id INTEGER PRIMARY KEY)")
	seekExec(t, db, "CREATE TABLE excluded(id INTEGER PRIMARY KEY)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		seekExec(t, db, "INSERT INTO items VALUES("+itoa(v)+")")
	}
	seekExec(t, db, "INSERT INTO excluded VALUES(2)")
	seekExec(t, db, "INSERT INTO excluded VALUES(4)")

	rows := seekQueryInts(t, db, "SELECT id FROM items WHERE id NOT IN (SELECT id FROM excluded)")
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows), rows)
	}
}

// ============================================================================
// SQL-level: ephemeral tables via recursive CTE (exercises execClearEphemeral)
// ============================================================================

// TestExecClearEphemeral_RecursiveCTE verifies that a recursive CTE works end-
// to-end. Recursive CTEs in this engine use OpClearEphemeral internally to
// reset the queue and next-row ephemeral tables between iterations.
func TestExecClearEphemeral_RecursiveCTE(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	// Generate numbers 1..5 using a recursive CTE.
	rows := seekQueryInts(t, db,
		"WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT n FROM cnt")
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d: %v", len(rows), rows)
	}
	for i, v := range rows {
		if v != i+1 {
			t.Errorf("rows[%d] = %d, want %d", i, v, i+1)
		}
	}
}

// TestExecClearEphemeral_RecursiveCTE_MultiStep uses more iterations so that
// the queue-swap (ClearEphemeral) executes multiple times.
func TestExecClearEphemeral_RecursiveCTE_MultiStep(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	rows := seekQueryInts(t, db,
		"WITH RECURSIVE fib(a,b) AS (SELECT 0,1 UNION ALL SELECT b, a+b FROM fib WHERE a < 50) SELECT a FROM fib")
	if len(rows) == 0 {
		t.Fatal("expected non-empty result from Fibonacci CTE")
	}
	if rows[0] != 0 {
		t.Errorf("first Fibonacci value should be 0, got %d", rows[0])
	}
}

// TestExecClearEphemeral_RecursiveCTE_WithTable exercises the CTE path where
// the recursive query references a real base table.
func TestExecClearEphemeral_RecursiveCTE_WithTable(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE tree(id INTEGER PRIMARY KEY, parent INTEGER, label TEXT)")
	seekExec(t, db, "INSERT INTO tree VALUES(1, 0, 'root')")
	seekExec(t, db, "INSERT INTO tree VALUES(2, 1, 'child1')")
	seekExec(t, db, "INSERT INTO tree VALUES(3, 1, 'child2')")
	seekExec(t, db, "INSERT INTO tree VALUES(4, 2, 'grandchild')")

	n := seekQueryInt(t, db, `
		WITH RECURSIVE subtree(id) AS (
			SELECT id FROM tree WHERE id = 1
			UNION ALL
			SELECT t.id FROM tree t JOIN subtree s ON t.parent = s.id
		)
		SELECT COUNT(*) FROM subtree`)
	if n != 4 {
		t.Errorf("expected 4 nodes in subtree, got %d", n)
	}
}

// ============================================================================
// SQL-level: range boundary queries (findLastRowidLessThan path)
// ============================================================================

// TestExecSeekSQL_RangeBoundary tests exact boundary values for range queries.
func TestExecSeekSQL_RangeBoundary(t *testing.T) {
	db := seekOpenDB(t)
	defer db.Close()

	seekExec(t, db, "CREATE TABLE bnd(id INTEGER PRIMARY KEY)")
	for _, v := range []int{1, 3, 5, 7, 9} {
		seekExec(t, db, "INSERT INTO bnd VALUES("+itoa(v)+")")
	}

	// Exactly at a stored value — should not be included by strict GT.
	rows := seekQueryInts(t, db, "SELECT id FROM bnd WHERE id > 5")
	if len(rows) != 2 {
		t.Errorf("expected [7,9], got %v", rows)
	}

	// Exactly at a stored value — should not be included by strict LT.
	rows = seekQueryInts(t, db, "SELECT id FROM bnd WHERE id < 5")
	if len(rows) != 2 {
		t.Errorf("expected [1,3], got %v", rows)
	}

	// Between stored values.
	rows = seekQueryInts(t, db, "SELECT id FROM bnd WHERE id > 4 AND id < 8")
	if len(rows) != 2 {
		t.Errorf("expected [5,7], got %v", rows)
	}
}

// itoa converts a non-negative integer to its decimal string representation
// without importing strconv (keeping this file self-contained).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}
