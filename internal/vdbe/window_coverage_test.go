// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

const windowCovDSN = ":memory:"

func windowCovOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", windowCovDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func windowCovExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func windowCovSetup(t *testing.T) *sql.DB {
	t.Helper()
	db := windowCovOpenDB(t)
	windowCovExec(t, db, "CREATE TABLE scores (grp TEXT, val INTEGER)")
	for _, row := range []struct {
		g string
		v int
	}{
		{"A", 10}, {"A", 10}, {"A", 20}, {"A", 30},
		{"B", 5}, {"B", 5}, {"B", 15},
	} {
		if _, err := db.Exec("INSERT INTO scores VALUES (?,?)", row.g, row.v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return db
}

// TestWindowCovNthValue exercises GetNthValue via NTH_VALUE() SQL.
func TestWindowCovNthValue(t *testing.T) {
	db := windowCovSetup(t)
	defer db.Close()

	rows, err := db.Query(`
		SELECT val,
		       NTH_VALUE(val, 1) OVER (PARTITION BY grp ORDER BY val
		                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		       NTH_VALUE(val, 2) OVER (PARTITION BY grp ORDER BY val
		                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		FROM scores
		ORDER BY grp, val`)
	if err != nil {
		t.Fatalf("NTH_VALUE query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var val int
		var nth1, nth2 sql.NullInt64
		if err := rows.Scan(&val, &nth1, &nth2); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from NTH_VALUE query")
	}
}

// TestWindowCovRankWithPartition exercises ResetRanking and UpdateRankingFromRow
// via RANK()/DENSE_RANK() with PARTITION BY across multiple groups.
func TestWindowCovRankWithPartition(t *testing.T) {
	db := windowCovSetup(t)
	defer db.Close()

	rows, err := db.Query(`
		SELECT grp, val,
		       RANK() OVER (PARTITION BY grp ORDER BY val),
		       DENSE_RANK() OVER (PARTITION BY grp ORDER BY val),
		       ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val)
		FROM scores
		ORDER BY grp, val`)
	if err != nil {
		t.Fatalf("RANK query: %v", err)
	}
	defer rows.Close()

	type resultRow struct {
		grp       string
		val       int
		rank      int
		denseRank int
		rowNum    int
	}
	count := 0
	for rows.Next() {
		var r resultRow
		if err := rows.Scan(&r.grp, &r.val, &r.rank, &r.denseRank, &r.rowNum); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if r.rank < 1 {
			t.Errorf("grp=%s val=%d: rank must be >= 1, got %d", r.grp, r.val, r.rank)
		}
		if r.denseRank < 1 {
			t.Errorf("grp=%s val=%d: dense_rank must be >= 1, got %d", r.grp, r.val, r.denseRank)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Fatal("expected rows")
	}
}

// TestWindowCovExcludeCurrentRow exercises applyFrameExclude/shouldExcludeRow
// with EXCLUDE CURRENT ROW.
func TestWindowCovExcludeCurrentRow(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE ex (v INTEGER)")
	for _, v := range []int{10, 20, 30, 40, 50} {
		if _, err := db.Exec("INSERT INTO ex VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v
		                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		                    EXCLUDE CURRENT ROW)
		FROM ex ORDER BY v`)
	if err != nil {
		t.Fatalf("EXCLUDE CURRENT ROW query: %v", err)
	}
	defer rows.Close()

	totalSum := 10 + 20 + 30 + 40 + 50 // 150
	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if s.Valid {
			want := int64(totalSum - v)
			if s.Int64 != want {
				t.Errorf("v=%d: EXCLUDE CURRENT ROW sum want %d got %d", v, want, s.Int64)
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from EXCLUDE CURRENT ROW query")
	}
}

// TestWindowCovExcludeGroup exercises shouldExcludeRow with EXCLUDE GROUP.
func TestWindowCovExcludeGroup(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE eg (v INTEGER)")
	for _, v := range []int{10, 10, 20, 30} {
		if _, err := db.Exec("INSERT INTO eg VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v
		                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		                    EXCLUDE GROUP)
		FROM eg ORDER BY v`)
	if err != nil {
		t.Fatalf("EXCLUDE GROUP query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from EXCLUDE GROUP query")
	}
}

// TestWindowCovExcludeTies exercises shouldExcludeRow with EXCLUDE TIES.
func TestWindowCovExcludeTies(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE et (v INTEGER)")
	for _, v := range []int{10, 10, 20, 30} {
		if _, err := db.Exec("INSERT INTO et VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v
		                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		                    EXCLUDE TIES)
		FROM et ORDER BY v`)
	if err != nil {
		t.Fatalf("EXCLUDE TIES query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from EXCLUDE TIES query")
	}
}

// TestWindowCovFrameStartFollowing exercises calculateFrameStart BoundFollowing path.
func TestWindowCovFrameStartFollowing(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE ff (v INTEGER)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		if _, err := db.Exec("INSERT INTO ff VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING exercises BoundFollowing start.
	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
		FROM ff ORDER BY v`)
	if err != nil {
		t.Fatalf("BoundFollowing start query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows")
	}
}

// TestWindowCovFrameEndPreceding exercises calculateFrameEnd BoundPreceding path.
func TestWindowCovFrameEndPreceding(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE ep (v INTEGER)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		if _, err := db.Exec("INSERT INTO ep VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING exercises BoundPreceding end.
	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
		FROM ep ORDER BY v`)
	if err != nil {
		t.Fatalf("BoundPreceding end query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows")
	}
}

// TestWindowCovFrameCurrentRowStart exercises calculateFrameStart BoundCurrentRow path.
func TestWindowCovFrameCurrentRowStart(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE cr (v INTEGER)")
	for _, v := range []int{1, 2, 3, 4, 5} {
		if _, err := db.Exec("INSERT INTO cr VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING exercises BoundCurrentRow start.
	rows, err := db.Query(`
		SELECT v,
		       SUM(v) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
		FROM cr ORDER BY v`)
	if err != nil {
		t.Fatalf("BoundCurrentRow start query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var v int
		var s sql.NullInt64
		if err := rows.Scan(&v, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows")
	}
}

// TestWindowCovEntirePartition exercises EntirePartitionFrame via window without ORDER BY.
func TestWindowCovEntirePartition(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE ep2 (grp TEXT, v INTEGER)")
	for _, row := range []struct {
		g string
		v int
	}{
		{"X", 1}, {"X", 2}, {"X", 3},
		{"Y", 10}, {"Y", 20},
	} {
		if _, err := db.Exec("INSERT INTO ep2 VALUES (?,?)", row.g, row.v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// No ORDER BY → EntirePartitionFrame is used.
	rows, err := db.Query(`
		SELECT grp, v, SUM(v) OVER (PARTITION BY grp)
		FROM ep2 ORDER BY grp, v`)
	if err != nil {
		t.Fatalf("EntirePartitionFrame query: %v", err)
	}
	defer rows.Close()

	type row struct {
		grp string
		v   int
		sum int
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.grp, &r.v, &r.sum); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	// Partition X sum=6, partition Y sum=30.
	wantSums := map[string]int{"X": 6, "Y": 30}
	for _, r := range results {
		if r.sum != wantSums[r.grp] {
			t.Errorf("grp=%s v=%d: want sum=%d got %d", r.grp, r.v, wantSums[r.grp], r.sum)
		}
	}
	if len(results) == 0 {
		t.Error("expected rows")
	}
}

// TestWindowCovSameRowValuesUnit exercises SameRowValues directly.
func TestWindowCovSameRowValuesUnit(t *testing.T) {
	ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())

	r1 := []*vdbe.Mem{vdbe.NewMemInt(42), vdbe.NewMemStr("hello")}
	r2 := []*vdbe.Mem{vdbe.NewMemInt(42), vdbe.NewMemStr("hello")}
	r3 := []*vdbe.Mem{vdbe.NewMemInt(99), vdbe.NewMemStr("hello")}
	r4 := []*vdbe.Mem{vdbe.NewMemInt(42)}

	if !ws.SameRowValues(r1, r2) {
		t.Error("expected identical rows to be equal")
	}
	if ws.SameRowValues(r1, r3) {
		t.Error("expected differing rows to be unequal")
	}
	if ws.SameRowValues(r1, r4) {
		t.Error("expected rows of different length to be unequal")
	}
}

// TestWindowCovIncrementPartRowIfNewRowUnit exercises IncrementPartRowIfNewRow directly.
func TestWindowCovIncrementPartRowIfNewRowUnit(t *testing.T) {
	ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())

	r1 := []*vdbe.Mem{vdbe.NewMemInt(1)}
	r2 := []*vdbe.Mem{vdbe.NewMemInt(2)}

	// First call: nil LastRowCounterUpdate → should increment.
	startRow := ws.CurrentPartRow
	ws.IncrementPartRowIfNewRow(r1)
	if ws.CurrentPartRow != startRow+1 {
		t.Errorf("expected increment on first call: got %d want %d", ws.CurrentPartRow, startRow+1)
	}

	// Second call with same row: should NOT increment.
	before := ws.CurrentPartRow
	ws.IncrementPartRowIfNewRow(r1)
	if ws.CurrentPartRow != before {
		t.Errorf("expected no increment for same row: got %d want %d", ws.CurrentPartRow, before)
	}

	// Call with different row: should increment.
	ws.IncrementPartRowIfNewRow(r2)
	if ws.CurrentPartRow != before+1 {
		t.Errorf("expected increment for new row: got %d want %d", ws.CurrentPartRow, before+1)
	}
}

// TestWindowCovResetRankingUnit exercises ResetRanking directly.
func TestWindowCovResetRankingUnit(t *testing.T) {
	ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.DefaultWindowFrame())

	// Populate some ranking state.
	ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
	ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
	ws.NextRow()
	ws.UpdateRanking()
	ws.NextRow()
	ws.UpdateRanking()

	// Now reset.
	ws.ResetRanking()

	if ws.CurrentRank != 0 {
		t.Errorf("after ResetRanking: CurrentRank=%d want 0", ws.CurrentRank)
	}
	if ws.CurrentDenseRank != 0 {
		t.Errorf("after ResetRanking: CurrentDenseRank=%d want 0", ws.CurrentDenseRank)
	}
	if ws.RowsAtCurrentRank != 0 {
		t.Errorf("after ResetRanking: RowsAtCurrentRank=%d want 0", ws.RowsAtCurrentRank)
	}
	if ws.LastRankRow != nil {
		t.Error("after ResetRanking: LastRankRow should be nil")
	}
}

// TestWindowCovUpdateRankingFromRowUnit exercises UpdateRankingFromRow directly.
func TestWindowCovUpdateRankingFromRowUnit(t *testing.T) {
	ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.DefaultWindowFrame())

	r1 := []*vdbe.Mem{vdbe.NewMemInt(10)}
	r2 := []*vdbe.Mem{vdbe.NewMemInt(10)} // same value → same rank
	r3 := []*vdbe.Mem{vdbe.NewMemInt(20)} // different value → new rank

	// First call: initializes state.
	ws.UpdateRankingFromRow(r1)
	if ws.CurrentDenseRank != 1 {
		t.Errorf("after first call: DenseRank=%d want 1", ws.CurrentDenseRank)
	}

	// Second call with same row number should be a no-op (same generation).
	ws.UpdateRankingFromRow(r1)
	if ws.CurrentDenseRank != 1 {
		t.Errorf("after same-row repeat: DenseRank=%d want 1", ws.CurrentDenseRank)
	}

	// Advance row number to trigger new ranking update.
	ws.CurrentPartRow = 1
	ws.UpdateRankingFromRow(r2) // same value → RowsAtCurrentRank++
	if ws.RowsAtCurrentRank != 2 {
		t.Errorf("after same-value new row: RowsAtCurrentRank=%d want 2", ws.RowsAtCurrentRank)
	}

	ws.CurrentPartRow = 2
	ws.UpdateRankingFromRow(r3) // new value → CurrentDenseRank++
	if ws.CurrentDenseRank != 2 {
		t.Errorf("after new-value row: DenseRank=%d want 2", ws.CurrentDenseRank)
	}

	// Nil row should be a no-op.
	ws.UpdateRankingFromRow(nil)
}

// TestWindowCoverageAdditional exercises the remaining uncovered branches in window.go:
// - calculateFrameStart/End default branches
// - shouldExcludeRow default branch (ExcludeNoOthers via direct call)
// - sameOrderByValues with colIdx out of range
// - GetLagRow/GetLeadRow with no valid partition (CurrentPartIdx < 0)
// - GetFirstValue/GetLastValue with colIdx out of range
// - CurrentRow with CurrentPartRow < 0
// - LAG/LEAD with offset > partition size
// - FIRST_VALUE/LAST_VALUE with non-trivial ROWS frames
// - RANGE frames with ORDER BY
func TestWindowCoverageAdditional(t *testing.T) {
	t.Run("calculateFrameStartDefault", func(t *testing.T) {
		// Use a WindowFrameBound type value that hits the default case.
		// BoundUnboundedFollowing (4) on the Start triggers default in calculateFrameStart.
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})
		ws.NextRow()
		// GetFrameRows will call calculateFrameStart/End; the default branch returns 0.
		_ = ws.GetFirstValue(0)
	})

	t.Run("calculateFrameEndDefault", func(t *testing.T) {
		// BoundPreceding (1) on End with offset 0 → hits BoundPreceding case, not default.
		// Use an invalid FrameBoundType (e.g. 99) by casting — not possible without unsafe.
		// Instead confirm the default path in calculateFrameEnd is the BoundUnboundedFollowing
		// already covered; the default returns partitionSize-1.
		// We cover it indirectly by using a frame where End.Type is BoundUnboundedFollowing (4).
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.NextRow()
		got := ws.GetLastValue(0)
		if got == nil || got.IsNull() {
			t.Errorf("calculateFrameEndDefault: expected non-null last value")
		}
	})

	t.Run("sameOrderByValuesColIdxOutOfRange", func(t *testing.T) {
		// colIdx beyond row length → continue (treated as equal)
		ws := vdbe.NewWindowState(nil, []int{5}, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})
		ws.NextRow()
		ws.UpdateRanking() // exercises sameOrderByValues with colIdx=5 > row length
		ws.NextRow()
		ws.UpdateRanking()
		// If colIdx is out of range, both rows are treated as equal peers.
		if ws.CurrentRank != 0 {
			t.Errorf("out-of-range orderby col: expected rank to stay at 0 (same group), got %d", ws.CurrentRank)
		}
	})

	t.Run("getLagRowNoPartition", func(t *testing.T) {
		// CurrentPartIdx < 0 → GetLagRow returns nil
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		got := ws.GetLagRow(1)
		if got != nil {
			t.Errorf("GetLagRow with no partition: expected nil, got %v", got)
		}
	})

	t.Run("getLeadRowNoPartition", func(t *testing.T) {
		// CurrentPartIdx < 0 → GetLeadRow returns nil
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		got := ws.GetLeadRow(1)
		if got != nil {
			t.Errorf("GetLeadRow with no partition: expected nil, got %v", got)
		}
	})

	t.Run("getLagRowOffsetBeyondPartition", func(t *testing.T) {
		// offset > partition size → targetIdx < 0 → nil
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})
		ws.NextRow()           // CurrentPartRow = 0
		got := ws.GetLagRow(5) // 0 - 5 = -5 < 0
		if got != nil {
			t.Errorf("GetLagRow offset>partition: expected nil, got %v", got)
		}
	})

	t.Run("getLeadRowOffsetBeyondPartition", func(t *testing.T) {
		// offset > remaining rows → targetIdx >= len → nil
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})
		ws.NextRow()
		ws.NextRow()            // CurrentPartRow = 1 (last row)
		got := ws.GetLeadRow(5) // 1 + 5 = 6 >= 2
		if got != nil {
			t.Errorf("GetLeadRow offset>partition: expected nil, got %v", got)
		}
	})

	t.Run("getFirstValueColIdxOutOfRange", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)})
		ws.NextRow()
		got := ws.GetFirstValue(99) // colIdx out of range
		if got == nil || !got.IsNull() {
			t.Errorf("GetFirstValue colIdx out of range: expected NULL, got %v", got)
		}
	})

	t.Run("getLastValueColIdxOutOfRange", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)})
		ws.NextRow()
		got := ws.GetLastValue(99) // colIdx out of range
		if got == nil || !got.IsNull() {
			t.Errorf("GetLastValue colIdx out of range: expected NULL, got %v", got)
		}
	})

	t.Run("currentRowPartRowNegative", func(t *testing.T) {
		// CurrentPartRow < 0 → CurrentRow returns nil
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		// Manually position to a valid partition but invalid row index
		ws.CurrentPartIdx = 0
		ws.CurrentPartRow = -1
		got := ws.CurrentRow()
		if got != nil {
			t.Errorf("CurrentRow with CurrentPartRow=-1: expected nil, got %v", got)
		}
	})

	t.Run("shouldExcludeRowDefaultBranch", func(t *testing.T) {
		// ExcludeNoOthers: shouldExcludeRow returns false (default branch).
		// applyFrameExclude is only called when Exclude != ExcludeNoOthers,
		// so we test this by using ExcludeNoOthers but exercising the logic via
		// a frame that would call applyFrameExclude with an out-of-range exclude value.
		// Since we can't set an invalid exclude value easily, we verify the
		// ExcludeNoOthers short-circuit in GetFrameRows returns rawRows directly.
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.WindowFrame{
			Type:    vdbe.FrameRows,
			Start:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:     vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			Exclude: vdbe.ExcludeNoOthers,
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.NextRow()
		rows := ws.GetFrameRows()
		if len(rows) != 2 {
			t.Errorf("ExcludeNoOthers: expected 2 frame rows, got %d", len(rows))
		}
	})
}

// TestWindowCovLagLeadSQL exercises LAG/LEAD via SQL including offset > partition size.
func TestWindowCovLagLeadSQL(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE lagtest (grp TEXT, val INTEGER)")
	for _, row := range []struct {
		g string
		v int
	}{
		{"A", 10}, {"A", 20}, {"A", 30},
		{"B", 100},
	} {
		if _, err := db.Exec("INSERT INTO lagtest VALUES (?,?)", row.g, row.v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// LAG with offset 1 and offset 5 (beyond partition)
	rows, err := db.Query(`
		SELECT grp, val,
		       LAG(val, 1) OVER (PARTITION BY grp ORDER BY val),
		       LAG(val, 5) OVER (PARTITION BY grp ORDER BY val)
		FROM lagtest ORDER BY grp, val`)
	if err != nil {
		t.Fatalf("LAG query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var grp string
		var val int
		var lag1, lag5 sql.NullInt64
		if err := rows.Scan(&grp, &val, &lag1, &lag5); err != nil {
			t.Fatalf("scan: %v", err)
		}
		// First row in each partition: lag1 should be NULL
		if grp == "A" && val == 10 && lag1.Valid {
			t.Errorf("LAG(val,1) for first row in partition A: expected NULL")
		}
		// Offset 5 always exceeds partition size (3 rows max): should be NULL
		if lag5.Valid {
			t.Errorf("LAG(val,5) should always be NULL (offset>partition), got %d", lag5.Int64)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from LAG query")
	}
}

// TestWindowCovLeadSQL exercises LEAD with offset > remaining rows.
func TestWindowCovLeadSQL(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE leadtest (val INTEGER)")
	for _, v := range []int{1, 2, 3} {
		if _, err := db.Exec("INSERT INTO leadtest VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT val,
		       LEAD(val, 1) OVER (ORDER BY val),
		       LEAD(val, 10) OVER (ORDER BY val)
		FROM leadtest ORDER BY val`)
	if err != nil {
		t.Fatalf("LEAD query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var val int
		var lead1, lead10 sql.NullInt64
		if err := rows.Scan(&val, &lead1, &lead10); err != nil {
			t.Fatalf("scan: %v", err)
		}
		// offset 10 always exceeds partition (3 rows): should be NULL
		if lead10.Valid {
			t.Errorf("LEAD(val,10) should be NULL (offset>partition), got %d", lead10.Int64)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from LEAD query")
	}
}

// TestWindowCovFirstValueNonTrivialFrame exercises FIRST_VALUE with ROWS BETWEEN N PRECEDING AND CURRENT ROW.
func TestWindowCovFirstValueNonTrivialFrame(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE fvtest (val INTEGER)")
	for _, v := range []int{10, 20, 30, 40, 50} {
		if _, err := db.Exec("INSERT INTO fvtest VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT val,
		       FIRST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
		FROM fvtest ORDER BY val`)
	if err != nil {
		t.Fatalf("FIRST_VALUE query: %v", err)
	}
	defer rows.Close()

	type resultRow struct {
		val        int
		firstValue int
	}
	var results []resultRow
	for rows.Next() {
		var r resultRow
		if err := rows.Scan(&r.val, &r.firstValue); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected rows from FIRST_VALUE query")
	}
	// For row with val=10 (index 0): frame is rows 0..0, FIRST_VALUE=10
	// For row with val=30 (index 2): frame is rows 0..2, FIRST_VALUE=10
	// For row with val=50 (index 4): frame is rows 2..4, FIRST_VALUE=30
	wantFirst := map[int]int{10: 10, 20: 10, 30: 10, 40: 20, 50: 30}
	for _, r := range results {
		if want, ok := wantFirst[r.val]; ok {
			if r.firstValue != want {
				t.Errorf("FIRST_VALUE for val=%d: want %d got %d", r.val, want, r.firstValue)
			}
		}
	}
}

// TestWindowCovLastValueNonTrivialFrame exercises LAST_VALUE with ROWS BETWEEN CURRENT ROW AND N FOLLOWING.
func TestWindowCovLastValueNonTrivialFrame(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE lvtest (val INTEGER)")
	for _, v := range []int{10, 20, 30, 40, 50} {
		if _, err := db.Exec("INSERT INTO lvtest VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT val,
		       LAST_VALUE(val) OVER (ORDER BY val ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)
		FROM lvtest ORDER BY val`)
	if err != nil {
		t.Fatalf("LAST_VALUE query: %v", err)
	}
	defer rows.Close()

	type resultRow struct {
		val       int
		lastValue int
	}
	var results []resultRow
	for rows.Next() {
		var r resultRow
		if err := rows.Scan(&r.val, &r.lastValue); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected rows from LAST_VALUE query")
	}
	// For val=10 (idx 0): frame 0..2, LAST_VALUE=30
	// For val=40 (idx 3): frame 3..5 clamped to 3..4, LAST_VALUE=50
	// For val=50 (idx 4): frame 4..6 clamped to 4..4, LAST_VALUE=50
	wantLast := map[int]int{10: 30, 20: 40, 30: 50, 40: 50, 50: 50}
	for _, r := range results {
		if want, ok := wantLast[r.val]; ok {
			if r.lastValue != want {
				t.Errorf("LAST_VALUE for val=%d: want %d got %d", r.val, want, r.lastValue)
			}
		}
	}
}

// TestWindowCovRangeFrameSameOrderByValues exercises RANGE frames using sameOrderByValues comparisons.
func TestWindowCovRangeFrameSameOrderByValues(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE rangetest (val INTEGER)")
	// Include duplicates to exercise peer-group matching in RANGE mode
	for _, v := range []int{10, 10, 20, 20, 30} {
		if _, err := db.Exec("INSERT INTO rangetest VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(`
		SELECT val,
		       SUM(val) OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
		       COUNT(*) OVER (ORDER BY val RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
		FROM rangetest ORDER BY val`)
	if err != nil {
		t.Fatalf("RANGE frame query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var val int
		var sum, cnt sql.NullInt64
		if err := rows.Scan(&val, &sum, &cnt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from RANGE frame query")
	}
}

// TestWindowCovFrameStartBeyondPartition exercises calculateFrameStart BoundPreceding
// when offset is large enough that max(0, currentRow-offset) clamps to 0 but also
// covers the case where BoundFollowing start > partitionSize-1 (clamps to partitionSize-1).
func TestWindowCovFrameStartBeyondPartition(t *testing.T) {
	db := windowCovOpenDB(t)
	defer db.Close()
	windowCovExec(t, db, "CREATE TABLE fsedge (val INTEGER)")
	for _, v := range []int{1, 2, 3} {
		if _, err := db.Exec("INSERT INTO fsedge VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// ROWS BETWEEN 100 FOLLOWING AND UNBOUNDED FOLLOWING: start > partition end
	// calculateFrameStart returns min(partitionSize-1, currentRow+100)
	rows, err := db.Query(`
		SELECT val,
		       SUM(val) OVER (ORDER BY val ROWS BETWEEN 100 FOLLOWING AND UNBOUNDED FOLLOWING)
		FROM fsedge ORDER BY val`)
	if err != nil {
		t.Fatalf("frame start beyond partition query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var val int
		var s sql.NullInt64
		if err := rows.Scan(&val, &s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows")
	}
}

// TestWindowCovGetNthValueUnit exercises GetNthValue directly including edge cases.
func TestWindowCovGetNthValueUnit(t *testing.T) {
	ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
		Type:  vdbe.FrameRows,
		Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
		End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
	})

	ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(100)})
	ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(200)})
	ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(300)})

	ws.NextRow() // position at first row; frame = entire partition

	// N=1 → first row value 100.
	got := ws.GetNthValue(0, 1)
	if got == nil || got.IntValue() != 100 {
		t.Errorf("GetNthValue(0,1): want 100 got %v", got)
	}

	// N=2 → second row value 200.
	got = ws.GetNthValue(0, 2)
	if got == nil || got.IntValue() != 200 {
		t.Errorf("GetNthValue(0,2): want 200 got %v", got)
	}

	// N=0 (invalid) → NULL.
	got = ws.GetNthValue(0, 0)
	if got == nil || !got.IsNull() {
		t.Errorf("GetNthValue(0,0): want NULL got %v", got)
	}

	// N > frame size → NULL.
	got = ws.GetNthValue(0, 10)
	if got == nil || !got.IsNull() {
		t.Errorf("GetNthValue(0,10): want NULL got %v", got)
	}

	// colIdx out of range → NULL.
	got = ws.GetNthValue(99, 1)
	if got == nil || !got.IsNull() {
		t.Errorf("GetNthValue(99,1): want NULL got %v", got)
	}
}
