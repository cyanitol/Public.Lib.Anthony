// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe_test

import (
	"database/sql"
	"testing"

	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// openDB2 opens an in-memory database for window2 coverage tests.
func openDB2(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite_internal", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

func execDB2(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// TestWindowCoverage2 contains focused tests for the remaining coverage gaps.
func TestWindowCoverage2(t *testing.T) {

	// 1. calculateFrameStart: BoundPreceding where offset > currentRow → clamp to 0.
	// At row index 0 with "5 PRECEDING", max(0, 0-5)=0.
	t.Run("calculateFrameStartPrecedingClampToZero", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 5},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(3)})

		ws.NextRow() // CurrentPartRow=0; frameStart = max(0, 0-5) = 0
		got := ws.GetFirstValue(0)
		if got == nil || got.IsNull() {
			t.Fatal("expected non-null first value after clamped frame start")
		}
		if got.IntValue() != 1 {
			t.Errorf("frameStart clamped to 0: want first value=1, got %d", got.IntValue())
		}
	})

	// 2. calculateFrameEnd: BoundFollowing where offset pushes beyond partition end → clamp.
	// At row index 1 (last of 2) with "100 FOLLOWING", min(1, 1+100)=1.
	t.Run("calculateFrameEndFollowingClampToPartitionEnd", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundFollowing, Offset: 100},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})

		ws.NextRow() // row 0
		ws.NextRow() // row 1 (last); frameEnd = min(1, 1+100) = 1
		got := ws.GetLastValue(0)
		if got == nil || got.IsNull() {
			t.Fatal("expected non-null last value after clamped frame end")
		}
		if got.IntValue() != 20 {
			t.Errorf("frameEnd clamped to partition end: want last value=20, got %d", got.IntValue())
		}
	})

	// 3. sameOrderByValues: two rows with same ORDER BY value returns true (peer rows).
	// This exercises the "all comparisons pass → return true" path with actual equal values.
	t.Run("sameOrderByValuesTruePath", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)}) // same ORDER BY value
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(99)}) // different value

		ws.NextRow() // row 0, val=42
		ws.UpdateRanking()
		beforeRank := ws.CurrentRank

		ws.NextRow() // row 1, val=42 (same → RowsAtCurrentRank++)
		ws.UpdateRanking()

		// Rank should not have advanced (still same group)
		if ws.CurrentRank != beforeRank {
			t.Errorf("sameOrderByValues peers: rank advanced unexpectedly from %d to %d", beforeRank, ws.CurrentRank)
		}
		if ws.RowsAtCurrentRank != 2 {
			t.Errorf("sameOrderByValues: expected RowsAtCurrentRank=2, got %d", ws.RowsAtCurrentRank)
		}
	})

	// 4. shouldExcludeRow: ExcludeCurrentRow — the current row is excluded but peers are not.
	// Exercises the isCurrentRow=true path.
	t.Run("shouldExcludeCurrentRowDirect", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.WindowFrame{
			Type:    vdbe.FrameRows,
			Start:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:     vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			Exclude: vdbe.ExcludeCurrentRow,
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // CurrentPartRow=0
		frameRows := ws.GetFrameRows()
		// Current row (idx 0) excluded; rows 1 and 2 remain
		if len(frameRows) != 2 {
			t.Errorf("ExcludeCurrentRow at row 0: expected 2 frame rows, got %d", len(frameRows))
		}

		ws.NextRow() // CurrentPartRow=1
		frameRows = ws.GetFrameRows()
		// Row 1 excluded; rows 0 and 2 remain
		if len(frameRows) != 2 {
			t.Errorf("ExcludeCurrentRow at row 1: expected 2 frame rows, got %d", len(frameRows))
		}
	})

	// 5. shouldExcludeRow: ExcludeGroup — all peers (same ORDER BY value) are excluded.
	// Exercises isPeer=true path.
	t.Run("shouldExcludeGroupDirect", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.WindowFrame{
			Type:    vdbe.FrameRows,
			Start:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:     vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			Exclude: vdbe.ExcludeGroup,
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)}) // peer of row 0
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)}) // different value
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // CurrentPartRow=0, val=10
		frameRows := ws.GetFrameRows()
		// Rows 0 and 1 (val=10) are peers of current row; both excluded.
		// Rows 2 (val=20) and 3 (val=30) remain.
		if len(frameRows) != 2 {
			t.Errorf("ExcludeGroup: expected 2 non-peer rows, got %d", len(frameRows))
		}
	})

	// 6. shouldExcludeRow: ExcludeTies — peer rows (same ORDER BY value) OTHER than current row excluded.
	// isCurrentRow=false && isPeer=true → excluded.
	t.Run("shouldExcludeTiesDirect", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.WindowFrame{
			Type:    vdbe.FrameRows,
			Start:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:     vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			Exclude: vdbe.ExcludeTies,
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)}) // tied with row 0
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})

		ws.NextRow() // CurrentPartRow=0, val=10
		frameRows := ws.GetFrameRows()
		// Row 0 is current (not excluded by ExcludeTies).
		// Row 1 is a tie (same val=10, not current) → excluded.
		// Row 2 is not a peer → kept.
		if len(frameRows) != 2 {
			t.Errorf("ExcludeTies: expected 2 rows (current + non-peer), got %d", len(frameRows))
		}
	})

	// 7. GetFirstValue when frame is empty (frameStart > frameEnd).
	t.Run("getFirstValueEmptyFrame", func(t *testing.T) {
		// ROWS BETWEEN 3 FOLLOWING AND 1 FOLLOWING → start > end → empty frame
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundFollowing, Offset: 3},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundFollowing, Offset: 1},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(2)})

		ws.NextRow() // CurrentPartRow=0; frameStart=min(1, 0+3)=1, frameEnd=min(1, 0+1)=1
		// Actually that gives start=1,end=1 (not empty). Use a 3-row partition.
		// Reset and try again with only 2 rows but offset 3.
		ws2 := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundFollowing, Offset: 10},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundFollowing, Offset: 1},
		})
		ws2.AddRow([]*vdbe.Mem{vdbe.NewMemInt(5)})
		ws2.AddRow([]*vdbe.Mem{vdbe.NewMemInt(6)})
		ws2.NextRow() // frameStart=min(1,0+10)=1, frameEnd=min(1,0+1)=1 → start==end, not empty

		// Force truly empty: use BoundPreceding end with offset=1 on first row
		// frameEnd = max(0, 0-1)=0, frameStart=CurrentRow=0 → start==end
		// Use BoundPreceding end on row index 0 with offset>0 and start=BoundFollowing
		ws3 := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundCurrentRow},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 1},
		})
		ws3.AddRow([]*vdbe.Mem{vdbe.NewMemInt(99)})
		ws3.AddRow([]*vdbe.Mem{vdbe.NewMemInt(100)})
		ws3.NextRow() // row 0: frameStart=0, frameEnd=max(0,0-1)=0
		// start=0, end=0 → not empty. Let's try row 1: start=1, end=max(0,1-1)=0 → empty!
		ws3.NextRow() // row 1: frameStart=1, frameEnd=max(0,1-1)=0 → start(1) > end(0) → empty

		got := ws3.GetFirstValue(0)
		if got == nil || !got.IsNull() {
			t.Errorf("GetFirstValue with empty frame: expected NULL, got %v", got)
		}
	})

	// 8. GetLastValue when frame start > end (empty frame) → returns NULL.
	t.Run("getLastValueEmptyFrame", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundCurrentRow},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 1},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(7)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(8)})

		ws.NextRow() // row 0: start=0, end=max(0,0-1)=0 → not empty
		ws.NextRow() // row 1: start=1, end=max(0,1-1)=0 → start>end → empty

		got := ws.GetLastValue(0)
		if got == nil || !got.IsNull() {
			t.Errorf("GetLastValue with empty frame: expected NULL, got %v", got)
		}
	})

	// 9. GetLagRow with offset=0 returns current row.
	t.Run("getLagRowOffsetZero", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(55)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(66)})

		ws.NextRow() // CurrentPartRow=0
		ws.NextRow() // CurrentPartRow=1

		got := ws.GetLagRow(0) // offset=0 → targetIdx=1 → current row
		if got == nil {
			t.Fatal("GetLagRow(0): expected current row, got nil")
		}
		if got[0].IntValue() != 66 {
			t.Errorf("GetLagRow(0): expected value=66 (current row), got %d", got[0].IntValue())
		}
	})

	// 10. GetLeadRow with negative offset → targetIdx = currentRow + negative = currentRow-|n|.
	// Negative offsets on LEAD act like LAG. If targetIdx < 0, returns nil.
	t.Run("getLeadRowNegativeOffset", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // row 0
		// Negative offset from row 0: 0 + (-1) = -1 < 0 → nil
		got := ws.GetLeadRow(-1)
		if got != nil {
			t.Errorf("GetLeadRow(-1) from row 0: expected nil (out of bounds), got %v", got)
		}

		ws.NextRow() // row 1
		// Negative offset from row 1: 1 + (-1) = 0 → returns row 0
		got = ws.GetLeadRow(-1)
		if got == nil {
			t.Fatal("GetLeadRow(-1) from row 1: expected row 0, got nil")
		}
		if got[0].IntValue() != 10 {
			t.Errorf("GetLeadRow(-1) from row 1: expected value=10, got %d", got[0].IntValue())
		}
	})

	// 11. CurrentRow: CurrentPartIdx >= len(Partitions) → returns nil.
	t.Run("currentRowPartIdxBeyondEnd", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.CurrentPartIdx = 99 // force out-of-range
		got := ws.CurrentRow()
		if got != nil {
			t.Errorf("CurrentRow with CurrentPartIdx >= len(partitions): expected nil, got %v", got)
		}
	})

	// CurrentRow: multiple ORDER BY columns of different types (int vs real vs string).
	t.Run("currentRowMultipleOrderByDifferentTypes", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0, 1, 2}, nil, vdbe.DefaultWindowFrame())

		row1 := []*vdbe.Mem{vdbe.NewMemInt(1), vdbe.NewMemReal(3.14), vdbe.NewMemStr("alpha")}
		row2 := []*vdbe.Mem{vdbe.NewMemInt(1), vdbe.NewMemReal(3.14), vdbe.NewMemStr("beta")}
		row3 := []*vdbe.Mem{vdbe.NewMemInt(2), vdbe.NewMemReal(2.71), vdbe.NewMemStr("gamma")}

		ws.AddRow(row1)
		ws.AddRow(row2)
		ws.AddRow(row3)

		ws.NextRow() // row 0 (1, 3.14, "alpha")
		ws.UpdateRanking()

		ws.NextRow() // row 1 (1, 3.14, "beta") — col 2 differs → new rank
		ws.UpdateRanking()

		ws.NextRow() // row 2 (2, 2.71, "gamma") — col 0 differs → new rank
		ws.UpdateRanking()

		// After 3 rows all with different ORDER BY combos, dense rank should be 3
		if ws.CurrentDenseRank != 3 {
			t.Errorf("multi-type ORDER BY: expected DenseRank=3, got %d", ws.CurrentDenseRank)
		}

		// CurrentRow should return the third row
		cur := ws.CurrentRow()
		if cur == nil {
			t.Fatal("CurrentRow returned nil after advancing to row 2")
		}
		if cur[0].IntValue() != 2 {
			t.Errorf("CurrentRow multi-type ORDER BY: expected first col=2, got %d", cur[0].IntValue())
		}
	})

	// SQL-level: RANGE frame with ORDER BY that produces same-valued peers (sameOrderByValues=true).
	// This exercises the RANGE BoundCurrentRow handling via the window executor.
	t.Run("rangeFrameSameOrderByPeers", func(t *testing.T) {
		db := openDB2(t)
		defer db.Close()
		execDB2(t, db, "CREATE TABLE peers (v INTEGER)")
		for _, v := range []int{5, 5, 5, 10, 10} {
			if _, err := db.Exec("INSERT INTO peers VALUES (?)", v); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW with duplicate ORDER BY values.
		// All rows with v=5 are peers and form one group; all rows with v=10 form another.
		rows, err := db.Query(`
			SELECT v, SUM(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
			FROM peers ORDER BY v`)
		if err != nil {
			t.Fatalf("rangeFrameSameOrderByPeers query: %v", err)
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
			t.Error("expected rows from rangeFrameSameOrderByPeers query")
		}
	})

	// SQL-level: FIRST_VALUE/LAST_VALUE with a frame that is empty for some rows.
	// Uses "ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING" — empty when fewer than 2 rows follow.
	t.Run("firstLastValueEmptyFrameSQL", func(t *testing.T) {
		db := openDB2(t)
		defer db.Close()
		execDB2(t, db, "CREATE TABLE fvempty (v INTEGER)")
		for _, v := range []int{1, 2, 3} {
			if _, err := db.Exec("INSERT INTO fvempty VALUES (?)", v); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		// Row 1 (v=1): 2 following rows available; frameStart=idx 2, frameEnd=idx 3 clamped to 2
		// Row 2 (v=2): frameStart=idx 3 clamped to 2, frameEnd=idx 4 clamped to 2 → start==end
		// Row 3 (v=3): frameStart=idx 4 clamped to 2, frameEnd=idx 5 clamped to 2 → start==end
		rows, err := db.Query(`
			SELECT v,
			       FIRST_VALUE(v) OVER (ORDER BY v ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING),
			       LAST_VALUE(v) OVER (ORDER BY v ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING)
			FROM fvempty ORDER BY v`)
		if err != nil {
			t.Fatalf("firstLastValueEmptyFrameSQL query: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var v int
			var fv, lv sql.NullInt64
			if err := rows.Scan(&v, &fv, &lv); err != nil {
				t.Fatalf("scan: %v", err)
			}
			count++
			_ = fv
			_ = lv
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		if count == 0 {
			t.Error("expected rows from firstLastValueEmptyFrameSQL")
		}
	})

	// SQL-level: LAG with offset=0 returns the current row value.
	t.Run("lagOffsetZeroSQL", func(t *testing.T) {
		db := openDB2(t)
		defer db.Close()
		execDB2(t, db, "CREATE TABLE lago (v INTEGER)")
		for _, v := range []int{10, 20, 30} {
			if _, err := db.Exec("INSERT INTO lago VALUES (?)", v); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		rows, err := db.Query(`
			SELECT v, LAG(v, 0) OVER (ORDER BY v)
			FROM lago ORDER BY v`)
		if err != nil {
			t.Fatalf("lagOffsetZeroSQL query: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var v int
			var lag0 sql.NullInt64
			if err := rows.Scan(&v, &lag0); err != nil {
				t.Fatalf("scan: %v", err)
			}
			// LAG(v, 0) exercises the zero-offset code path; just verify it executes without error.
			_ = lag0
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		if count == 0 {
			t.Error("expected rows from lagOffsetZeroSQL")
		}
	})

	// SQL-level: LEAD with large offset returns NULL for all rows.
	t.Run("leadLargeOffsetSQL", func(t *testing.T) {
		db := openDB2(t)
		defer db.Close()
		execDB2(t, db, "CREATE TABLE leadlarge (v INTEGER)")
		for _, v := range []int{1, 2, 3} {
			if _, err := db.Exec("INSERT INTO leadlarge VALUES (?)", v); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		rows, err := db.Query(`
			SELECT v, LEAD(v, 100) OVER (ORDER BY v)
			FROM leadlarge ORDER BY v`)
		if err != nil {
			t.Fatalf("leadLargeOffsetSQL query: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var v int
			var lead sql.NullInt64
			if err := rows.Scan(&v, &lead); err != nil {
				t.Fatalf("scan: %v", err)
			}
			if lead.Valid {
				t.Errorf("LEAD(v,100) should be NULL (offset>partition), got %d", lead.Int64)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows err: %v", err)
		}
		if count == 0 {
			t.Error("expected rows from leadLargeOffsetSQL")
		}
	})

	// calculateFrameStart BoundPreceding: at the first row with large offset, frame start=0.
	// Verify via direct unit test that GetFrameRows returns the full frame starting at 0.
	t.Run("calculateFrameStartPrecedingAtFirstRowUnit", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 999},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundCurrentRow},
		})
		for _, v := range []int64{1, 2, 3, 4, 5} {
			ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(v)})
		}

		ws.NextRow() // row 0; frameStart = max(0, 0-999) = 0; frameEnd = 0
		frameRows := ws.GetFrameRows()
		if len(frameRows) != 1 {
			t.Errorf("frame at row 0 with 999 PRECEDING: expected 1 row, got %d", len(frameRows))
		}

		ws.NextRow() // row 1; frameStart = max(0, 1-999) = 0; frameEnd = 1
		frameRows = ws.GetFrameRows()
		if len(frameRows) != 2 {
			t.Errorf("frame at row 1 with 999 PRECEDING: expected 2 rows, got %d", len(frameRows))
		}
	})

	// calculateFrameEnd BoundPreceding: at row index 1 with offset=1, frameEnd=max(0,0)=0.
	t.Run("calculateFrameEndPrecedingClampUnit", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 999},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // row 0: frameEnd = max(0, 0-999) = 0; start=0; so 1 row [row0]
		ws.NextRow() // row 1: frameEnd = max(0, 1-999) = 0; frame = rows[0..0]
		frameRows := ws.GetFrameRows()
		if len(frameRows) != 1 {
			t.Errorf("UNBOUNDED PRECEDING TO 999 PRECEDING at row 1: expected 1 row, got %d", len(frameRows))
		}
		if frameRows[0][0].IntValue() != 10 {
			t.Errorf("expected first row value=10, got %d", frameRows[0][0].IntValue())
		}
	})

	// sameOrderByValues with multiple ORDER BY columns where one column has a string vs int comparison.
	t.Run("sameOrderByValuesMultiColMixed", func(t *testing.T) {
		// ORDER BY cols 0 (int) and 1 (string)
		ws := vdbe.NewWindowState(nil, []int{0, 1}, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("foo")})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("foo")}) // same → peers
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("bar")}) // col 1 differs

		ws.NextRow()
		ws.UpdateRanking()
		initialDenseRank := ws.CurrentDenseRank

		ws.NextRow()
		ws.UpdateRanking()
		// Should still be same dense rank (peers)
		if ws.CurrentDenseRank != initialDenseRank {
			t.Errorf("multi-col ORDER BY peers: DenseRank advanced unexpectedly")
		}

		ws.NextRow()
		ws.UpdateRanking()
		// col 1 differs → new dense rank
		if ws.CurrentDenseRank != initialDenseRank+1 {
			t.Errorf("multi-col ORDER BY: DenseRank should advance when col 1 differs, got %d", ws.CurrentDenseRank)
		}
	})
}
