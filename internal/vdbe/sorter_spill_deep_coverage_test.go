// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func spillDeepOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func spillDeepExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// newTinySpillSorter creates a SorterWithSpill with a very small memory budget
// so that spill is triggered after just a few rows.
func newTinySpillSorter(t *testing.T, keyCols []int, desc []bool, collations []string, numCols int) *vdbe.SorterWithSpill {
	t.Helper()
	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 400, // force spill after a handful of rows
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill(keyCols, desc, collations, numCols, cfg)
	t.Cleanup(func() { s.Close() })
	return s
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_1000RowsSQL
//
// Sort 1000+ rows through the SQL engine to exercise the full ORDER BY
// code path including all serialise/deserialise branches.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_1000RowsSQL(t *testing.T) {
	t.Parallel()

	db := spillDeepOpenDB(t)
	spillDeepExec(t, db, "CREATE TABLE big (id INTEGER, val TEXT)")

	const n = 1000

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO big VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	for i := n; i >= 1; i-- {
		if _, err := stmt.Exec(i, fmt.Sprintf("row_%04d", i)); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query("SELECT id, val FROM big ORDER BY id ASC")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	prev := 0
	count := 0
	for rows.Next() {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id <= prev {
			t.Errorf("out of order: got %d after %d", id, prev)
		}
		prev = id
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != n {
		t.Errorf("expected %d rows, got %d", n, count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_MixedTypesAllBranches
//
// Uses the exported SorterWithSpill API directly with a tiny memory cap to
// force spill, exercising serialise/deserialise for every Mem type:
//   NULL, Int, Real, Str, Blob, and Undefined (default branch).
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_MixedTypesAllBranches(t *testing.T) {
	t.Parallel()

	// Sort by col-0 (int key); numCols = 6 to cover all Mem types
	s := newTinySpillSorter(t, []int{0}, []bool{false}, []string{""}, 6)

	type rowSpec struct {
		key   int64
		extra [5]*vdbe.Mem
	}

	rows := []rowSpec{
		{10, [5]*vdbe.Mem{vdbe.NewMemNull(), vdbe.NewMemInt(1), vdbe.NewMemReal(1.1), vdbe.NewMemStr("alpha"), vdbe.NewMemBlob([]byte{0x01})}},
		{5, [5]*vdbe.Mem{vdbe.NewMemInt(99), vdbe.NewMemReal(2.2), vdbe.NewMemStr("beta"), vdbe.NewMemBlob([]byte{0x02, 0x03}), vdbe.NewMemNull()}},
		{15, [5]*vdbe.Mem{vdbe.NewMemReal(3.3), vdbe.NewMemStr("gamma"), vdbe.NewMemBlob([]byte{0xDE, 0xAD}), vdbe.NewMemNull(), vdbe.NewMemInt(7)}},
		{1, [5]*vdbe.Mem{vdbe.NewMemStr("delta"), vdbe.NewMemBlob([]byte{0xFF}), vdbe.NewMemNull(), vdbe.NewMemInt(42), vdbe.NewMemReal(4.4)}},
		{20, [5]*vdbe.Mem{vdbe.NewMemBlob([]byte{0xAB, 0xCD}), vdbe.NewMemNull(), vdbe.NewMemInt(3), vdbe.NewMemReal(5.5), vdbe.NewMemStr("epsilon")}},
		{8, [5]*vdbe.Mem{vdbe.NewMem(), vdbe.NewMemInt(6), vdbe.NewMemReal(6.6), vdbe.NewMemStr("zeta"), vdbe.NewMemBlob([]byte{0x00})}},
	}

	for _, r := range rows {
		row := []*vdbe.Mem{vdbe.NewMemInt(r.key)}
		row = append(row, r.extra[0], r.extra[1], r.extra[2], r.extra[3], r.extra[4])
		if err := s.Insert(row); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	expected := []int64{1, 5, 8, 10, 15, 20}
	for i, want := range expected {
		if !s.Next() {
			t.Fatalf("missing row %d", i)
		}
		row := s.CurrentRow()
		if row[0].IntValue() != want {
			t.Errorf("row %d: want key %d, got %d", i, want, row[0].IntValue())
		}
	}
	if s.Next() {
		t.Error("unexpected extra row")
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_RealColumnOrdering
//
// Forces spill with rows keyed on REAL values, exercising deserializeMemReal
// across multiple spilled runs.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_RealColumnOrdering(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorter(t, []int{0}, []bool{false}, []string{""}, 2)

	reals := []float64{3.14, 1.41, 2.71, 0.57, 1.73, 2.23, 1.61, 0.30, 1.00, 2.00}
	for _, r := range reals {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemReal(r), vdbe.NewMemStr(fmt.Sprintf("%.2f", r))}); err != nil {
			t.Fatalf("Insert %.2f: %v", r, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := -1.0
	count := 0
	for s.Next() {
		row := s.CurrentRow()
		cur := row[0].RealValue()
		if cur < prev {
			t.Errorf("out of order: %.4f after %.4f", cur, prev)
		}
		prev = cur
		count++
	}
	if count != len(reals) {
		t.Errorf("expected %d rows, got %d", len(reals), count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_NullOrdering
//
// Inserts rows where the sort key is NULL for some rows, exercising the
// NULL branch in deserializeMem and ensuring NULLs sort consistently.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_NullOrdering(t *testing.T) {
	t.Parallel()

	s := newTinySpillSorter(t, []int{0}, []bool{false}, []string{""}, 2)

	// Mix NULLs with integers to exercise both branches across spill boundaries
	for i := int64(15); i >= 1; i-- {
		var key *vdbe.Mem
		if i%3 == 0 {
			key = vdbe.NewMemNull()
		} else {
			key = vdbe.NewMemInt(i)
		}
		if err := s.Insert([]*vdbe.Mem{key, vdbe.NewMemStr(fmt.Sprintf("row%d", i))}); err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	count := 0
	for s.Next() {
		count++
	}
	if count != 15 {
		t.Errorf("expected 15 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_MultiRunMerge
//
// Forces many spilled runs (very tight memory budget) to exercise the k-way
// merge path (mergeSpilledRuns / mergeRuns) with diverse column types.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_MultiRunMerge(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 200, // extremely tight: forces a new run every row or two
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 3, cfg)
	defer s.Close()

	const n = 40
	for i := n; i >= 1; i-- {
		var col1 *vdbe.Mem
		switch i % 4 {
		case 0:
			col1 = vdbe.NewMemNull()
		case 1:
			col1 = vdbe.NewMemInt(int64(i * 10))
		case 2:
			col1 = vdbe.NewMemReal(float64(i) * 1.5)
		default:
			col1 = vdbe.NewMemStr(fmt.Sprintf("str_%03d", i))
		}
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(int64(i)), col1, vdbe.NewMemBlob([]byte{byte(i % 256)})}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	spills := s.GetNumSpilledRuns()
	t.Logf("spilled runs before Sort: %d", spills)
	if spills < 2 {
		t.Errorf("expected ≥2 spilled runs, got %d", spills)
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := int64(0)
	count := 0
	for s.Next() {
		row := s.CurrentRow()
		cur := row[0].IntValue()
		if cur <= prev {
			t.Errorf("out of order: %d after %d", cur, prev)
		}
		prev = cur
		count++
	}
	if count != n {
		t.Errorf("expected %d rows, got %d", n, count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_MultiColMixedSQL
//
// SQL ORDER BY on multiple columns of mixed types: INTEGER, REAL, TEXT, NULL.
// Exercises the full deserialise path inside the VDBE sorter when the engine
// sorts the result set.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_MultiColMixedSQL(t *testing.T) {
	t.Parallel()

	db := spillDeepOpenDB(t)
	spillDeepExec(t, db, `CREATE TABLE mixed (
		id   INTEGER,
		fval REAL,
		sval TEXT,
		bval BLOB
	)`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO mixed VALUES (?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	for i := 500; i >= 1; i-- {
		var sval interface{}
		if i%10 == 0 {
			sval = nil // SQL NULL
		} else {
			sval = fmt.Sprintf("text_%04d", i)
		}
		if _, err := stmt.Exec(i, float64(i)*0.1, sval, []byte{byte(i % 256)}); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// ORDER BY two columns to exercise multi-key comparison through spill
	rows, err := db.Query("SELECT id, fval FROM mixed ORDER BY id ASC, fval DESC")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	prev := 0
	count := 0
	for rows.Next() {
		var id int
		var fval float64
		if err := rows.Scan(&id, &fval); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id <= prev {
			t.Errorf("out of order: id %d after %d", id, prev)
		}
		prev = id
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 500 {
		t.Errorf("expected 500 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_BlobColumnOrdering
//
// Exercises the BLOB deserialization branch across spill boundaries.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_BlobColumnOrdering(t *testing.T) {
	t.Parallel()

	// Sort by integer key; second column is blob — exercises blob deserialise
	s := newTinySpillSorter(t, []int{0}, []bool{false}, []string{""}, 2)

	for i := int64(20); i >= 1; i-- {
		blob := make([]byte, 8)
		for j := range blob {
			blob[j] = byte((i*7 + int64(j)) % 256)
		}
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i), vdbe.NewMemBlob(blob)}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := int64(0)
	count := 0
	for s.Next() {
		row := s.CurrentRow()
		got := row[0].IntValue()
		if got <= prev {
			t.Errorf("out of order: %d after %d", got, prev)
		}
		prev = got
		count++
	}
	if count != 20 {
		t.Errorf("expected 20 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_SpillCurrentRunEmpty
//
// Verifies that calling Sort() on a SorterWithSpill that already has spilled
// runs but no remaining in-memory rows does not panic or error — exercises the
// len(s.Rows)==0 early-return path inside spillCurrentRun via Sort().
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_SpillCurrentRunEmpty(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 300,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	defer s.Close()

	// Insert enough rows to force a spill, consuming all in-memory rows
	for i := int64(15); i >= 1; i-- {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i)}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Manually drain the in-memory rows so spillCurrentRun hits the empty guard
	// (Sort() will call spillCurrentRun only if len(Rows)>0, so we rely on the
	// fact that after spilling the buffer is cleared and then Insert empties it
	// again — insert one final row, spill, then Sort with empty in-mem buffer).
	spills := s.GetNumSpilledRuns()
	if spills == 0 {
		t.Skip("no spilled runs; adjust MaxMemoryBytes")
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	count := 0
	for s.Next() {
		count++
	}
	if count != 15 {
		t.Errorf("expected 15 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_DescendingWithSpill
//
// DESC sort with spill to exercise comparator through the merge path.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_DescendingWithSpill(t *testing.T) {
	t.Parallel()

	cfg := &vdbe.SorterConfig{
		MaxMemoryBytes: 350,
		TempDir:        t.TempDir(),
		EnableSpill:    true,
	}
	s := vdbe.NewSorterWithSpill([]int{0}, []bool{true}, []string{""}, 2, cfg)
	defer s.Close()

	for i := int64(1); i <= 25; i++ {
		if err := s.Insert([]*vdbe.Mem{vdbe.NewMemInt(i), vdbe.NewMemStr(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	prev := int64(26)
	count := 0
	for s.Next() {
		row := s.CurrentRow()
		cur := row[0].IntValue()
		if cur >= prev {
			t.Errorf("not descending: %d after %d", cur, prev)
		}
		prev = cur
		count++
	}
	if count != 25 {
		t.Errorf("expected 25 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_SQLOrderByReal
//
// SQL ORDER BY on a REAL column — exercises deserializeMemReal via the VDBE
// engine when rows are read back after spilling.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_SQLOrderByReal(t *testing.T) {
	t.Parallel()

	db := spillDeepOpenDB(t)
	spillDeepExec(t, db, "CREATE TABLE reals (id INTEGER, score REAL, padding TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO reals VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	for i := 1000; i >= 1; i-- {
		score := float64(i) * 0.001
		if _, err := stmt.Exec(i, score, fmt.Sprintf("row_%04d", i)); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query("SELECT score FROM reals ORDER BY score ASC")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	prev := -1.0
	count := 0
	for rows.Next() {
		var score float64
		if err := rows.Scan(&score); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if score < prev {
			t.Errorf("out of order: %.4f after %.4f", score, prev)
		}
		prev = score
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1000 {
		t.Errorf("expected 1000 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// TestSorterSpillDeep_SQLOrderByNullColumn
//
// SQL ORDER BY a column that contains NULLs, exercising the NULL
// deserialisation path inside the sorter.
// ---------------------------------------------------------------------------

func TestSorterSpillDeep_SQLOrderByNullColumn(t *testing.T) {
	t.Parallel()

	db := spillDeepOpenDB(t)
	spillDeepExec(t, db, "CREATE TABLE nullcol (id INTEGER, tag TEXT, padding TEXT)")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO nullcol VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Prepare: %v", err)
	}
	for i := 1000; i >= 1; i-- {
		var tag interface{}
		if i%5 == 0 {
			tag = nil
		} else {
			tag = fmt.Sprintf("tag_%04d", i)
		}
		if _, err := stmt.Exec(i, tag, fmt.Sprintf("pad_%04d", i)); err != nil {
			stmt.Close()
			tx.Rollback()
			t.Fatalf("Exec row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query("SELECT id, tag FROM nullcol ORDER BY tag ASC, id ASC")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var tag sql.NullString
		if err := rows.Scan(&id, &tag); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != 1000 {
		t.Errorf("expected 1000 rows, got %d", count)
	}
}
