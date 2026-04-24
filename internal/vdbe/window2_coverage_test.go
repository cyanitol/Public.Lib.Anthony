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

// TestWindowCoverage2_FrameStartClamp tests calculateFrameStart with
// BoundPreceding where offset > currentRow, clamping to 0.
func TestWindowCoverage2_FrameStartClamp(t *testing.T) {
	t.Run("PrecedingClampToZero", func(t *testing.T) {
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

	t.Run("PrecedingAtFirstRowUnit", func(t *testing.T) {
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
}

// TestWindowCoverage2_FrameEndClamp tests calculateFrameEnd with BoundFollowing
// and BoundPreceding clamping.
func TestWindowCoverage2_FrameEndClamp(t *testing.T) {
	t.Run("FollowingClampToPartitionEnd", func(t *testing.T) {
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

	t.Run("PrecedingClampUnit", func(t *testing.T) {
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
}

// TestWindowCoverage2_SameOrderByValues tests the sameOrderByValues path for
// peer detection with single and multiple ORDER BY columns.
func TestWindowCoverage2_SameOrderByValues(t *testing.T) {
	t.Run("TruePath", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(42)}) // same ORDER BY value
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(99)}) // different value

		ws.NextRow() // row 0, val=42
		ws.UpdateRanking()
		beforeRank := ws.CurrentRank

		ws.NextRow() // row 1, val=42 (same → RowsAtCurrentRank++)
		ws.UpdateRanking()

		if ws.CurrentRank != beforeRank {
			t.Errorf("sameOrderByValues peers: rank advanced unexpectedly from %d to %d", beforeRank, ws.CurrentRank)
		}
		if ws.RowsAtCurrentRank != 2 {
			t.Errorf("sameOrderByValues: expected RowsAtCurrentRank=2, got %d", ws.RowsAtCurrentRank)
		}
	})

	t.Run("MultiColMixed", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0, 1}, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("foo")})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("foo")}) // same → peers
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10), vdbe.NewMemStr("bar")}) // col 1 differs

		ws.NextRow()
		ws.UpdateRanking()
		initialDenseRank := ws.CurrentDenseRank

		ws.NextRow()
		ws.UpdateRanking()
		if ws.CurrentDenseRank != initialDenseRank {
			t.Errorf("multi-col ORDER BY peers: DenseRank advanced unexpectedly")
		}

		ws.NextRow()
		ws.UpdateRanking()
		if ws.CurrentDenseRank != initialDenseRank+1 {
			t.Errorf("multi-col ORDER BY: DenseRank should advance when col 1 differs, got %d", ws.CurrentDenseRank)
		}
	})
}

// TestWindowCoverage2_ShouldExcludeRow tests the ExcludeCurrentRow,
// ExcludeGroup, and ExcludeTies paths.
func TestWindowCoverage2_ShouldExcludeRow(t *testing.T) {
	t.Run("ExcludeCurrentRow", func(t *testing.T) {
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
		if len(frameRows) != 2 {
			t.Errorf("ExcludeCurrentRow at row 0: expected 2 frame rows, got %d", len(frameRows))
		}

		ws.NextRow() // CurrentPartRow=1
		frameRows = ws.GetFrameRows()
		if len(frameRows) != 2 {
			t.Errorf("ExcludeCurrentRow at row 1: expected 2 frame rows, got %d", len(frameRows))
		}
	})

	t.Run("ExcludeGroup", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0}, nil, vdbe.WindowFrame{
			Type:    vdbe.FrameRows,
			Start:   vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedPreceding},
			End:     vdbe.WindowFrameBound{Type: vdbe.BoundUnboundedFollowing},
			Exclude: vdbe.ExcludeGroup,
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)}) // peer of row 0
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // CurrentPartRow=0, val=10
		frameRows := ws.GetFrameRows()
		if len(frameRows) != 2 {
			t.Errorf("ExcludeGroup: expected 2 non-peer rows, got %d", len(frameRows))
		}
	})

	t.Run("ExcludeTies", func(t *testing.T) {
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
		if len(frameRows) != 2 {
			t.Errorf("ExcludeTies: expected 2 rows (current + non-peer), got %d", len(frameRows))
		}
	})
}

// TestWindowCoverage2_EmptyFrame tests GetFirstValue and GetLastValue when the
// frame is empty (frameStart > frameEnd).
func TestWindowCoverage2_EmptyFrame(t *testing.T) {
	t.Run("GetFirstValue", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundCurrentRow},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 1},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(99)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(100)})
		ws.NextRow() // row 0
		ws.NextRow() // row 1: frameStart=1, frameEnd=max(0,1-1)=0 → start(1) > end(0) → empty

		got := ws.GetFirstValue(0)
		if got == nil || !got.IsNull() {
			t.Errorf("GetFirstValue with empty frame: expected NULL, got %v", got)
		}
	})

	t.Run("GetLastValue", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.WindowFrame{
			Type:  vdbe.FrameRows,
			Start: vdbe.WindowFrameBound{Type: vdbe.BoundCurrentRow},
			End:   vdbe.WindowFrameBound{Type: vdbe.BoundPreceding, Offset: 1},
		})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(7)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(8)})

		ws.NextRow() // row 0
		ws.NextRow() // row 1: start>end → empty

		got := ws.GetLastValue(0)
		if got == nil || !got.IsNull() {
			t.Errorf("GetLastValue with empty frame: expected NULL, got %v", got)
		}
	})
}

// TestWindowCoverage2_LagLead tests GetLagRow and GetLeadRow edge cases.
func TestWindowCoverage2_LagLead(t *testing.T) {
	t.Run("LagOffsetZero", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(55)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(66)})

		ws.NextRow() // CurrentPartRow=0
		ws.NextRow() // CurrentPartRow=1

		got := ws.GetLagRow(0)
		if got == nil {
			t.Fatal("GetLagRow(0): expected current row, got nil")
		}
		if got[0].IntValue() != 66 {
			t.Errorf("GetLagRow(0): expected value=66 (current row), got %d", got[0].IntValue())
		}
	})

	t.Run("LeadNegativeOffset", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(10)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(20)})
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(30)})

		ws.NextRow() // row 0
		got := ws.GetLeadRow(-1)
		if got != nil {
			t.Errorf("GetLeadRow(-1) from row 0: expected nil (out of bounds), got %v", got)
		}

		ws.NextRow() // row 1
		got = ws.GetLeadRow(-1)
		if got == nil {
			t.Fatal("GetLeadRow(-1) from row 1: expected row 0, got nil")
		}
		if got[0].IntValue() != 10 {
			t.Errorf("GetLeadRow(-1) from row 1: expected value=10, got %d", got[0].IntValue())
		}
	})
}

// TestWindowCoverage2_CurrentRow tests CurrentRow boundary conditions and
// multi-type ORDER BY ranking.
func TestWindowCoverage2_CurrentRow(t *testing.T) {
	t.Run("PartIdxBeyondEnd", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, nil, nil, vdbe.DefaultWindowFrame())
		ws.AddRow([]*vdbe.Mem{vdbe.NewMemInt(1)})
		ws.CurrentPartIdx = 99 // force out-of-range
		got := ws.CurrentRow()
		if got != nil {
			t.Errorf("CurrentRow with CurrentPartIdx >= len(partitions): expected nil, got %v", got)
		}
	})

	t.Run("MultipleOrderByDifferentTypes", func(t *testing.T) {
		ws := vdbe.NewWindowState(nil, []int{0, 1, 2}, nil, vdbe.DefaultWindowFrame())

		row1 := []*vdbe.Mem{vdbe.NewMemInt(1), vdbe.NewMemReal(3.14), vdbe.NewMemStr("alpha")}
		row2 := []*vdbe.Mem{vdbe.NewMemInt(1), vdbe.NewMemReal(3.14), vdbe.NewMemStr("beta")}
		row3 := []*vdbe.Mem{vdbe.NewMemInt(2), vdbe.NewMemReal(2.71), vdbe.NewMemStr("gamma")}

		ws.AddRow(row1)
		ws.AddRow(row2)
		ws.AddRow(row3)

		ws.NextRow()
		ws.UpdateRanking()
		ws.NextRow()
		ws.UpdateRanking()
		ws.NextRow()
		ws.UpdateRanking()

		if ws.CurrentDenseRank != 3 {
			t.Errorf("multi-type ORDER BY: expected DenseRank=3, got %d", ws.CurrentDenseRank)
		}

		cur := ws.CurrentRow()
		if cur == nil {
			t.Fatal("CurrentRow returned nil after advancing to row 2")
		}
		if cur[0].IntValue() != 2 {
			t.Errorf("CurrentRow multi-type ORDER BY: expected first col=2, got %d", cur[0].IntValue())
		}
	})
}

// TestWindowCoverage2_SQLRangeFrame tests RANGE frame with ORDER BY that
// produces same-valued peers via SQL.
func TestWindowCoverage2_SQLRangeFrame(t *testing.T) {
	db := openDB2(t)
	defer db.Close()
	execDB2(t, db, "CREATE TABLE peers (v INTEGER)")
	for _, v := range []int{5, 5, 5, 10, 10} {
		if _, err := db.Exec("INSERT INTO peers VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
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
}

// TestWindowCoverage2_SQLFirstLastValueEmptyFrame tests FIRST_VALUE/LAST_VALUE
// with a frame that is empty for some rows via SQL.
func TestWindowCoverage2_SQLFirstLastValueEmptyFrame(t *testing.T) {
	db := openDB2(t)
	defer db.Close()
	execDB2(t, db, "CREATE TABLE fvempty (v INTEGER)")
	for _, v := range []int{1, 2, 3} {
		if _, err := db.Exec("INSERT INTO fvempty VALUES (?)", v); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
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
}

// TestWindowCoverage2_SQLLagOffsetZero tests LAG with offset=0 via SQL.
func TestWindowCoverage2_SQLLagOffsetZero(t *testing.T) {
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
		_ = lag0
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count == 0 {
		t.Error("expected rows from lagOffsetZeroSQL")
	}
}

// TestWindowCoverage2_SQLLeadLargeOffset tests LEAD with large offset via SQL.
func TestWindowCoverage2_SQLLeadLargeOffset(t *testing.T) {
	db := openDB2(t)
	defer db.Close()
	execDB2(t, db, "CREATE TABLE leadlarge (v INTEGER)")
	execDB2(t, db, "INSERT INTO leadlarge VALUES (1)")
	execDB2(t, db, "INSERT INTO leadlarge VALUES (2)")
	execDB2(t, db, "INSERT INTO leadlarge VALUES (3)")
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
}
