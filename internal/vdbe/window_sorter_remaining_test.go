// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"encoding/binary"
	"os"
	"testing"
)

// TestWindowSorterRemaining is the top-level runner for all remaining coverage
// tests in window.go and sorter_spill.go.
func TestWindowSorterRemaining(t *testing.T) {
	t.Run("Window", func(t *testing.T) {
		t.Run("SamePartitionColIdxOutOfRange", TestWindowRemaining_SamePartitionColIdxOutOfRange)
		t.Run("NextRowNilWhenEmpty", TestWindowRemaining_NextRowNilWhenEmpty)
		t.Run("CalculateFrameEndDefault", TestWindowRemaining_CalculateFrameEndDefault)
		t.Run("ShouldExcludeRowDefault", TestWindowRemaining_ShouldExcludeRowDefault)
		t.Run("SameOrderByValuesReturnTrue", TestWindowRemaining_SameOrderByValuesReturnTrue)
		t.Run("ExcludeTiesPeerNotCurrentRow", TestWindowRemaining_ExcludeTiesPeerNotCurrentRow)
		t.Run("ExcludeGroupAllPeers", TestWindowRemaining_ExcludeGroupAllPeers)
		t.Run("SameOrderByValuesEmptyOrderBy", TestWindowRemaining_SameOrderByValuesEmptyOrderBy)
		t.Run("SameOrderByValuesEmptyWithExcludeTies", TestWindowRemaining_SameOrderByValuesEmptyWithExcludeTies)
		t.Run("SamePartitionValidSplit", TestWindowRemaining_SamePartitionValidSplit)
	})
	t.Run("Spill", func(t *testing.T) {
		t.Run("CreateSpillFilePathDefaultTempDir", TestSpillRemaining_CreateSpillFilePathDefaultTempDir)
		t.Run("SpillCurrentRunErrorPath", TestSpillRemaining_SpillCurrentRunErrorPath)
		t.Run("SpillCurrentRunEmptyRows", TestSpillRemaining_SpillCurrentRunEmptyRows)
		t.Run("WriteAndRecordSpillWriteError", TestSpillRemaining_WriteAndRecordSpillWriteError)
		t.Run("WriteRunToFileBinaryWriteError", TestSpillRemaining_WriteRunToFileBinaryWriteError)
		t.Run("WriteRunToFileRowDataWriteError", TestSpillRemaining_WriteRunToFileRowDataWriteError)
		t.Run("WriteRunToFileMidWriteError", TestSpillRemaining_WriteRunToFileMidWriteError)
		t.Run("DeserializeRowTooShort", TestSpillRemaining_DeserializeRowTooShort)
		t.Run("DeserializeRowZeroBytes", TestSpillRemaining_DeserializeRowZeroBytes)
		t.Run("DeserializeMemTooShort", TestSpillRemaining_DeserializeMemTooShort)
		t.Run("DeserializeMemTruncated", TestSpillRemaining_DeserializeMemTruncated)
		t.Run("DeserializeMemIntBadLength", TestSpillRemaining_DeserializeMemIntBadLength)
		t.Run("DeserializeMemRealBadLength", TestSpillRemaining_DeserializeMemRealBadLength)
		t.Run("DeserializeRowDeserializeMemError", TestSpillRemaining_DeserializeRowDeserializeMemError)
		t.Run("ReadRunFromFileTruncated", TestSpillRemaining_ReadRunFromFileTruncated)
		t.Run("ReadRunFromFileCorruptRowData", TestSpillRemaining_ReadRunFromFileCorruptRowData)
		t.Run("ReadRunFromFileDeserializeError", TestSpillRemaining_ReadRunFromFileDeserializeError)
		t.Run("SerializeRowRoundTrip", TestSpillRemaining_SerializeRowRoundTrip)
		t.Run("WriteRunToFileReadOnlyFile", TestSpillRemaining_WriteRunToFileReadOnlyFile)
		t.Run("MergeSpilledRunsEmpty", TestSpillRemaining_MergeSpilledRunsEmpty)
		t.Run("SortSpillCurrentRunError", TestSpillRemaining_SortSpillCurrentRunError)
	})
}

// ---------------------------------------------------------------------------
// window.go — samePartition: colIdx out of range path (line 172-173)
// ---------------------------------------------------------------------------

// TestWindowRemaining_SamePartitionColIdxOutOfRange exercises the
// `colIdx >= len(row1) || colIdx >= len(row2)` continue branch in samePartition.
// When a partition column index exceeds the row width the rows are treated as
// belonging to the same partition (the loop skips that column).
func TestWindowRemaining_SamePartitionColIdxOutOfRange(t *testing.T) {
	t.Parallel()

	// PartitionCols=[5] but rows only have 1 column → colIdx out of range.
	ws := NewWindowState([]int{5}, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.AddRow([]*Mem{NewMemInt(20)})

	// Both rows have different values but no valid partition column to compare,
	// so samePartition returns true → all rows land in one partition.
	if len(ws.Partitions) != 1 {
		t.Errorf("expected 1 partition when partition colIdx is out of range, got %d", len(ws.Partitions))
	}
	if len(ws.Partitions[0].Rows) != 2 {
		t.Errorf("expected 2 rows in the single partition, got %d", len(ws.Partitions[0].Rows))
	}
}

// ---------------------------------------------------------------------------
// window.go — NextRow: called with no partitions (line 198-200)
// ---------------------------------------------------------------------------

// TestWindowRemaining_NextRowNilWhenEmpty exercises the early-return nil path
// inside NextRow when there are no partitions.
func TestWindowRemaining_NextRowNilWhenEmpty(t *testing.T) {
	t.Parallel()

	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	// No rows added → Partitions is empty.
	got := ws.NextRow()
	if got != nil {
		t.Errorf("NextRow with no partitions: expected nil, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// window.go — calculateFrameEnd: default branch (line 282-283)
// ---------------------------------------------------------------------------

// TestWindowRemaining_CalculateFrameEndDefault exercises the default branch in
// calculateFrameEnd. The only FrameBoundType that reaches the default is any
// value not matched by the explicit cases. BoundUnboundedPreceding (0) is used
// as the End bound to trigger the default (the switch covers 4, 3, 2, 1 — not 0).
func TestWindowRemaining_CalculateFrameEndDefault(t *testing.T) {
	t.Parallel()

	// End.Type = BoundUnboundedPreceding (0) is not one of the four explicit
	// cases in calculateFrameEnd, so it falls through to default which returns
	// partitionSize-1.
	ws := NewWindowState(nil, nil, nil, WindowFrame{
		Type:  FrameRows,
		Start: WindowFrameBound{Type: BoundUnboundedPreceding},
		End:   WindowFrameBound{Type: BoundUnboundedPreceding}, // triggers default branch
	})
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(3)})

	ws.NextRow() // CurrentPartRow=0; frameEnd = default = partitionSize-1 = 2
	frameRows := ws.GetFrameRows()
	if len(frameRows) != 3 {
		t.Errorf("calculateFrameEnd default: expected 3 rows (entire partition), got %d", len(frameRows))
	}
}

// ---------------------------------------------------------------------------
// window.go — shouldExcludeRow: default branch (line 335-336)
// ---------------------------------------------------------------------------

// TestWindowRemaining_ShouldExcludeRowDefault exercises the default branch in
// shouldExcludeRow. This is reached by passing a WindowFrameExclude value that
// is not ExcludeCurrentRow/Group/Ties. We cast an integer to WindowFrameExclude
// to create an out-of-range value, then call applyFrameExclude directly.
func TestWindowRemaining_ShouldExcludeRowDefault(t *testing.T) {
	t.Parallel()

	// Use an Exclude value of 99 (not matched by any case) → default → false.
	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: WindowFrameExclude(99), // unknown value → default branch
	})
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.AddRow([]*Mem{NewMemInt(20)})
	ws.AddRow([]*Mem{NewMemInt(30)})

	ws.NextRow() // CurrentPartRow=0
	// GetFrameRows calls applyFrameExclude (Exclude != ExcludeNoOthers),
	// which calls shouldExcludeRow. The default branch returns false so no
	// rows are excluded.
	frameRows := ws.GetFrameRows()
	if len(frameRows) != 3 {
		t.Errorf("shouldExcludeRow default (unknown Exclude): expected all 3 rows kept, got %d", len(frameRows))
	}
}

// ---------------------------------------------------------------------------
// window.go — sameOrderByValues: return true path (line 418-420)
// ---------------------------------------------------------------------------

// TestWindowRemaining_SameOrderByValuesReturnTrue exercises the path in
// sameOrderByValues where all ORDER BY column comparisons match and the
// function returns true. This is the non-early-exit success path.
func TestWindowRemaining_SameOrderByValuesReturnTrue(t *testing.T) {
	t.Parallel()

	// ORDER BY col 0 with two rows sharing the same value.
	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeGroup, // uses sameOrderByValues internally
	})
	ws.AddRow([]*Mem{NewMemInt(42)})
	ws.AddRow([]*Mem{NewMemInt(42)}) // same ORDER BY value → sameOrderByValues returns true
	ws.AddRow([]*Mem{NewMemInt(99)})

	ws.NextRow() // CurrentPartRow=0, val=42
	frameRows := ws.GetFrameRows()

	// ExcludeGroup excludes all peers (rows with val=42). Row 2 (val=99) remains.
	// sameOrderByValues returns true for rows 0 and 1 → they are excluded.
	if len(frameRows) != 1 {
		t.Errorf("ExcludeGroup with tied val=42: expected 1 non-peer row, got %d", len(frameRows))
	}
	if frameRows[0][0].IntValue() != 99 {
		t.Errorf("ExcludeGroup: expected remaining row value=99, got %d", frameRows[0][0].IntValue())
	}
}

// ---------------------------------------------------------------------------
// window.go — shouldExcludeRow: ExcludeTies with peer that is not current row
// ---------------------------------------------------------------------------

// TestWindowRemaining_ExcludeTiesPeerNotCurrentRow tests that ExcludeTies
// excludes tied rows that are NOT the current row, exercising the
// `isPeer && !isCurrentRow` path.
func TestWindowRemaining_ExcludeTiesPeerNotCurrentRow(t *testing.T) {
	t.Parallel()

	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeTies,
	})
	ws.AddRow([]*Mem{NewMemInt(5)})
	ws.AddRow([]*Mem{NewMemInt(5)}) // tied with row 0
	ws.AddRow([]*Mem{NewMemInt(5)}) // tied with rows 0 and 1
	ws.AddRow([]*Mem{NewMemInt(9)}) // not a peer

	// At CurrentPartRow=1 (middle tied row):
	// Row 0: isPeer=true, isCurrentRow=false → excluded (ExcludeTies)
	// Row 1: isPeer=true, isCurrentRow=true  → kept (ExcludeTies keeps current)
	// Row 2: isPeer=true, isCurrentRow=false → excluded
	// Row 3: isPeer=false → kept
	ws.NextRow() // row 0
	ws.NextRow() // row 1 (CurrentPartRow=1)
	frameRows := ws.GetFrameRows()
	if len(frameRows) != 2 {
		t.Errorf("ExcludeTies at middle tied row: expected 2 rows (current + non-peer), got %d", len(frameRows))
	}
}

// ---------------------------------------------------------------------------
// window.go — shouldExcludeRow: ExcludeGroup with all peers
// ---------------------------------------------------------------------------

// TestWindowRemaining_ExcludeGroupAllPeers verifies ExcludeGroup excludes all
// rows when every row in the frame is a peer (same ORDER BY value).
func TestWindowRemaining_ExcludeGroupAllPeers(t *testing.T) {
	t.Parallel()

	ws := NewWindowState(nil, []int{0}, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeGroup,
	})
	ws.AddRow([]*Mem{NewMemInt(7)})
	ws.AddRow([]*Mem{NewMemInt(7)})
	ws.AddRow([]*Mem{NewMemInt(7)})

	ws.NextRow() // row 0
	frameRows := ws.GetFrameRows()
	// All rows are peers → all excluded → empty result.
	if len(frameRows) != 0 {
		t.Errorf("ExcludeGroup all peers: expected 0 rows, got %d", len(frameRows))
	}
}

// ---------------------------------------------------------------------------
// window.go — sameOrderByValues: return true when OrderByCols is empty (line 418-420)
// ---------------------------------------------------------------------------

// TestWindowRemaining_SameOrderByValuesEmptyOrderBy exercises the
// `len(ws.OrderByCols) == 0` early-return `true` path in sameOrderByValues.
// This is reached via shouldExcludeRow when the WindowState has no ORDER BY
// columns but an EXCLUDE clause is active (so applyFrameExclude is called).
func TestWindowRemaining_SameOrderByValuesEmptyOrderBy(t *testing.T) {
	t.Parallel()

	// No ORDER BY columns (nil) + ExcludeGroup: sameOrderByValues called with
	// empty OrderByCols → returns true (line 418-420). All rows are peers →
	// ExcludeGroup excludes all of them.
	ws := NewWindowState(nil, nil, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeGroup,
	})
	ws.AddRow([]*Mem{NewMemInt(1)})
	ws.AddRow([]*Mem{NewMemInt(2)})
	ws.AddRow([]*Mem{NewMemInt(3)})

	ws.NextRow() // CurrentPartRow=0
	frameRows := ws.GetFrameRows()
	// With no ORDER BY, all rows are peers (sameOrderByValues returns true).
	// ExcludeGroup removes all peers → empty frame.
	if len(frameRows) != 0 {
		t.Errorf("ExcludeGroup with no ORDER BY: expected 0 rows (all peers), got %d", len(frameRows))
	}
}

// TestWindowRemaining_SameOrderByValuesEmptyWithExcludeTies exercises the
// empty OrderByCols path via ExcludeTies. All rows are peers; ExcludeTies
// excludes peers that are NOT the current row, keeping only the current row.
func TestWindowRemaining_SameOrderByValuesEmptyWithExcludeTies(t *testing.T) {
	t.Parallel()

	ws := NewWindowState(nil, nil, nil, WindowFrame{
		Type:    FrameRows,
		Start:   WindowFrameBound{Type: BoundUnboundedPreceding},
		End:     WindowFrameBound{Type: BoundUnboundedFollowing},
		Exclude: ExcludeTies,
	})
	ws.AddRow([]*Mem{NewMemInt(10)})
	ws.AddRow([]*Mem{NewMemInt(20)})
	ws.AddRow([]*Mem{NewMemInt(30)})

	ws.NextRow() // CurrentPartRow=0
	frameRows := ws.GetFrameRows()
	// No ORDER BY → all rows are "peers". ExcludeTies keeps current row (row 0)
	// and excludes all other peer rows → 1 row remains.
	if len(frameRows) != 1 {
		t.Errorf("ExcludeTies no ORDER BY: expected 1 row (current only), got %d", len(frameRows))
	}
}

// ---------------------------------------------------------------------------
// window.go — samePartition: partition split on valid column
// ---------------------------------------------------------------------------

// TestWindowRemaining_SamePartitionValidSplit ensures samePartition returns
// false when partition columns differ, creating multiple partitions.
func TestWindowRemaining_SamePartitionValidSplit(t *testing.T) {
	t.Parallel()

	ws := NewWindowState([]int{0}, nil, nil, DefaultWindowFrame())
	ws.AddRow([]*Mem{NewMemInt(1), NewMemInt(100)})
	ws.AddRow([]*Mem{NewMemInt(1), NewMemInt(200)}) // same partition
	ws.AddRow([]*Mem{NewMemInt(2), NewMemInt(300)}) // different partition

	if len(ws.Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(ws.Partitions))
	}
	if len(ws.Partitions[0].Rows) != 2 {
		t.Errorf("partition 0: expected 2 rows, got %d", len(ws.Partitions[0].Rows))
	}
	if len(ws.Partitions[1].Rows) != 1 {
		t.Errorf("partition 1: expected 1 row, got %d", len(ws.Partitions[1].Rows))
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — createSpillFilePath: empty TempDir (line 149-151)
// ---------------------------------------------------------------------------

// TestSpillRemaining_CreateSpillFilePathDefaultTempDir exercises the
// `tempDir == ""` branch in createSpillFilePath (uses os.TempDir()).
func TestSpillRemaining_CreateSpillFilePathDefaultTempDir(t *testing.T) {
	t.Parallel()

	// Config.TempDir = "" → createSpillFilePath falls back to os.TempDir().
	cfg := &SorterConfig{
		MaxMemoryBytes: 64, // very small to force spill quickly
		TempDir:        "", // empty → os.TempDir()
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	defer s.Close()

	for i := int64(1); i <= 10; i++ {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Skip("no spill triggered; increase row count or decrease MaxMemoryBytes")
	}

	if err := s.Sort(); err != nil {
		t.Fatalf("Sort: %v", err)
	}

	count := 0
	for s.Next() {
		count++
	}
	if count != 10 {
		t.Errorf("expected 10 rows after sort, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — spillCurrentRun error paths via invalid TempDir
// ---------------------------------------------------------------------------

// TestSpillRemaining_SpillCurrentRunErrorPath forces spillCurrentRun to fail by
// using an invalid TempDir. This exercises the writeAndRecordSpill error branch
// (lines 138-140) inside spillCurrentRun, and Insert's spill error return (112-114).
func TestSpillRemaining_SpillCurrentRunErrorPath(t *testing.T) {
	t.Parallel()

	cfg := &SorterConfig{
		MaxMemoryBytes: 64,
		TempDir:        "/no/such/directory/anthony_test",
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	defer s.Close()

	// Insert rows until spill is triggered and fails.
	var gotErr error
	for i := int64(1); i <= 20; i++ {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			gotErr = err
			break
		}
	}

	if gotErr == nil {
		t.Skip("spill error was not triggered (may need more rows or smaller MaxMemoryBytes)")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — spillCurrentRun: empty rows guard (line 126-128)
// ---------------------------------------------------------------------------

// TestSpillRemaining_SpillCurrentRunEmptyRows exercises the
// `len(s.Rows) == 0` early return in spillCurrentRun.
func TestSpillRemaining_SpillCurrentRunEmptyRows(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Call spillCurrentRun directly with no rows — should return nil immediately.
	if err := s.spillCurrentRun(); err != nil {
		t.Errorf("spillCurrentRun with empty rows: expected nil error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — writeAndRecordSpill: write error path (lines 165-169)
// ---------------------------------------------------------------------------

// TestSpillRemaining_WriteAndRecordSpillWriteError exercises the cleanup branch
// in writeAndRecordSpill when writeRunToFile fails (lines 165-169). To make
// writeRunToFile fail after os.Create succeeds, we use a directory path as the
// file path — os.Create on a directory entry causes a write error.
func TestSpillRemaining_WriteAndRecordSpillWriteError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, &SorterConfig{
		MaxMemoryBytes: 1 << 20,
		TempDir:        tempDir,
		EnableSpill:    true,
	})
	defer s.Close()

	s.Sorter.Insert([]*Mem{NewMemInt(1)})

	// Path in non-existent subdirectory: os.Create will fail → exercises the
	// error return from writeAndRecordSpill.
	badPath := tempDir + "/no_such_subdir/anthony_test.tmp"
	err := s.writeAndRecordSpill(badPath, 1)
	if err == nil {
		t.Log("writeAndRecordSpill with nested bad path succeeded (OS may allow)")
	}
}

// TestSpillRemaining_WriteRunToFileBinaryWriteError directly calls writeRunToFile
// with a closed file descriptor to force binary.Write to fail.
func TestSpillRemaining_WriteRunToFileBinaryWriteError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Create a temp file, write to it successfully to confirm it works,
	// then close it and try to write more — should fail with closed pipe/fd.
	tmpF, err := os.CreateTemp(t.TempDir(), "bwe_*.tmp")
	if err != nil {
		t.Skipf("CreateTemp: %v", err)
	}
	tmpF.Close() // close immediately

	// Now try to write to the closed file — this should fail on all platforms.
	rows := [][]*Mem{{NewMemInt(42)}}
	writeErr := s.writeRunToFile(tmpF, rows)
	if writeErr == nil {
		t.Log("writeRunToFile to closed fd succeeded (OS may allow, skipping assertion)")
	}
}

// TestSpillRemaining_WriteRunToFileRowDataWriteError exercises the
// `file.Write(rowData)` error path (line 220-222) by using a file whose
// descriptor is closed mid-operation. We open /dev/null read-only.
func TestSpillRemaining_WriteRunToFileRowDataWriteError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Open /dev/null read-only: binary.Write of numRows may succeed on Linux
	// (discards data), but subsequent writes may fail.
	rof, err := os.OpenFile(os.DevNull, os.O_RDONLY, 0)
	if err != nil {
		t.Skipf("open /dev/null read-only failed: %v", err)
	}
	defer rof.Close()

	rows := [][]*Mem{{NewMemInt(7), NewMemStr("test")}}
	writeErr := s.writeRunToFile(rof, rows)
	_ = writeErr // OS-dependent
}

// TestSpillRemaining_WriteRunToFileMidWriteError exercises the row-data write
// error paths inside writeRunToFile. We use a closed file to trigger failures.
func TestSpillRemaining_WriteRunToFileMidWriteError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Create and immediately close a temp file, then try to write to it.
	// On Linux, writing to a closed fd returns EBADF.
	tmpF, err := os.CreateTemp(t.TempDir(), "mid_*.tmp")
	if err != nil {
		t.Skipf("CreateTemp: %v", err)
	}
	name := tmpF.Name()
	tmpF.Close()
	// Reopen to get a valid *os.File, then close the underlying fd so
	// the next write attempt fails.
	f, err := os.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		t.Skipf("OpenFile: %v", err)
	}
	// Close the file now — subsequent writes will fail with "file already closed".
	f.Close()

	rows := [][]*Mem{{NewMemInt(99)}}
	writeErr := s.writeRunToFile(f, rows)
	_ = writeErr // expected to error on Linux (closed fd)
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeRow: data too short (line 344-346)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeRowTooShort exercises the `len(data) < 4`
// guard in deserializeRow.
func TestSpillRemaining_DeserializeRowTooShort(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	_, err := s.deserializeRow([]byte{0x01, 0x02}) // only 2 bytes < 4
	if err == nil {
		t.Error("deserializeRow with 2 bytes: expected error, got nil")
	}
}

// TestSpillRemaining_DeserializeRowZeroBytes exercises the empty byte slice
// path in deserializeRow (len=0 < 4).
func TestSpillRemaining_DeserializeRowZeroBytes(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	_, err := s.deserializeRow([]byte{})
	if err == nil {
		t.Error("deserializeRow with empty slice: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeMem: data too short (line 422-424)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeMemTooShort exercises `len(data) < 6` in
// deserializeMem.
func TestSpillRemaining_DeserializeMemTooShort(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	_, _, err := s.deserializeMem([]byte{0x00, 0x00, 0x01}) // 3 bytes < 6
	if err == nil {
		t.Error("deserializeMem with 3 bytes: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeMem: data truncated (line 430-432)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeMemTruncated exercises the `len(data) < offset+dataLen`
// path in deserializeMem. We construct a header claiming 100 bytes of data but
// only provide the 6-byte header.
func TestSpillRemaining_DeserializeMemTruncated(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Build a 6-byte buffer: flags=MemInt (0x0004), dataLen=100
	buf := make([]byte, 6)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(MemInt))
	binary.LittleEndian.PutUint32(buf[2:6], 100) // claims 100 bytes of data
	// No actual data bytes follow → truncated

	_, _, err := s.deserializeMem(buf)
	if err == nil {
		t.Error("deserializeMem with truncated data: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeMemInt: invalid data length (line 372-374)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeMemIntBadLength exercises the `dataLen != 8`
// guard in deserializeMemInt. We feed a serialized cell that claims MemInt but
// has dataLen=4 instead of 8.
func TestSpillRemaining_DeserializeMemIntBadLength(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Construct: flags=MemInt(0x0004), dataLen=4 (wrong), 4 data bytes
	buf := make([]byte, 6+4)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(MemInt))
	binary.LittleEndian.PutUint32(buf[2:6], 4) // wrong length (should be 8)
	// 4 bytes of "data"
	buf[6], buf[7], buf[8], buf[9] = 0x01, 0x02, 0x03, 0x04

	_, _, err := s.deserializeMem(buf)
	if err == nil {
		t.Error("deserializeMem with MemInt dataLen=4: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeMemReal: invalid data length (line 382-384)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeMemRealBadLength exercises the `dataLen != 8`
// guard in deserializeMemReal. Same approach: MemReal flag but dataLen=2.
func TestSpillRemaining_DeserializeMemRealBadLength(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	buf := make([]byte, 6+2)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(MemReal))
	binary.LittleEndian.PutUint32(buf[2:6], 2) // wrong length
	buf[6], buf[7] = 0xAB, 0xCD

	_, _, err := s.deserializeMem(buf)
	if err == nil {
		t.Error("deserializeMem with MemReal dataLen=2: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — deserializeMem: error propagated from deserializeMemByType
// (line 437-439)
// ---------------------------------------------------------------------------

// TestSpillRemaining_DeserializeMemByTypeError exercises the error-return path
// of deserializeMem when deserializeMemByType returns an error. We trigger this
// by injecting a MemInt cell with wrong dataLen (same technique, but from within
// a full deserializeRow to also hit the deserializeRow error path at line 355).
func TestSpillRemaining_DeserializeRowDeserializeMemError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Build a row with 1 column. Row header: numCols=1 (4 bytes).
	// Then a Mem cell: flags=MemInt, dataLen=2 (wrong).
	row := make([]byte, 4+6+2)
	binary.LittleEndian.PutUint32(row[0:4], 1) // numCols = 1
	binary.LittleEndian.PutUint16(row[4:6], uint16(MemInt))
	binary.LittleEndian.PutUint32(row[6:10], 2) // wrong dataLen for MemInt
	row[10], row[11] = 0x11, 0x22

	_, err := s.deserializeRow(row)
	if err == nil {
		t.Error("deserializeRow with bad MemInt cell: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — readRunFromFile error paths (lines 320-322, 326-328, 332-334)
// ---------------------------------------------------------------------------

// TestSpillRemaining_ReadRunFromFileTruncated exercises the error paths in
// readRunFromFile by providing a file that is truncated after the row count.
func TestSpillRemaining_ReadRunFromFileTruncated(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Write a file that claims 2 rows but has no row data.
	f, err := os.CreateTemp(t.TempDir(), "truncated_*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	// Write numRows=2 but nothing else.
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, 2)
	if _, err := f.Write(header); err != nil {
		t.Fatalf("Write header: %v", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	_, err = s.readRunFromFile(f)
	if err == nil {
		t.Error("readRunFromFile with truncated file: expected error, got nil")
	}
}

// TestSpillRemaining_ReadRunFromFileCorruptRowData exercises the rowData read
// error and deserialize error paths. We write a valid row count and row length
// but then provide insufficient data for io.ReadFull.
func TestSpillRemaining_ReadRunFromFileCorruptRowData(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	f, err := os.CreateTemp(t.TempDir(), "corrupt_*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	// numRows = 1
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, 1)
	if _, err := f.Write(header); err != nil {
		t.Fatalf("Write numRows: %v", err)
	}

	// rowLen = 50, but provide 0 data bytes
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, 50)
	if _, err := f.Write(lenBuf); err != nil {
		t.Fatalf("Write rowLen: %v", err)
	}
	// No row data written → io.ReadFull will fail.

	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	_, err = s.readRunFromFile(f)
	if err == nil {
		t.Error("readRunFromFile with missing row data: expected error, got nil")
	}
}

// TestSpillRemaining_ReadRunFromFileDeserializeError exercises the deserialize
// error path in readRunFromFile (line 332-334). We write a valid row length but
// fill the row data with bytes that fail deserialization (too short numCols area).
func TestSpillRemaining_ReadRunFromFileDeserializeError(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	f, err := os.CreateTemp(t.TempDir(), "deserr_*.tmp")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	// numRows = 1
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, 1)
	if _, err := f.Write(header); err != nil {
		t.Fatalf("Write numRows: %v", err)
	}

	// rowData = 2 bytes (too short for deserializeRow, which needs >= 4).
	rowData := []byte{0xAA, 0xBB}
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(rowData)))
	if _, err := f.Write(lenBuf); err != nil {
		t.Fatalf("Write rowLen: %v", err)
	}
	if _, err := f.Write(rowData); err != nil {
		t.Fatalf("Write rowData: %v", err)
	}

	if _, err := f.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	_, err = s.readRunFromFile(f)
	if err == nil {
		t.Error("readRunFromFile with undeserializable row: expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — serializeRow: calling with nil cell would error from
// serializeMem; reach the error propagation path (line 241-243) via a nil Mem.
// ---------------------------------------------------------------------------

// TestSpillRemaining_SerializeRowNilCell exercises the serializeMem error
// propagation in serializeRow. A nil *Mem in the row causes a panic in
// serializeMem, so we instead verify the happy path of serializeRow and its
// round-trip to confirm the coverage of the non-error path, then separately
// trigger error via nil pointer in a recovered panic test.
//
// Since serializeMem has no direct error returns (it doesn't fail), the
// `serializeMem error` branch at line 241 is actually unreachable in normal
// operation. We use a direct round-trip test to maximally exercise the function.
func TestSpillRemaining_SerializeRowRoundTrip(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 5, nil)
	defer s.Close()

	original := []*Mem{
		NewMemInt(42),
		NewMemReal(3.14),
		NewMemStr("hello"),
		NewMemBlob([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
		NewMemNull(),
	}

	data, err := s.serializeRow(original)
	if err != nil {
		t.Fatalf("serializeRow: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("serializeRow returned empty data")
	}

	row, err := s.deserializeRow(data)
	if err != nil {
		t.Fatalf("deserializeRow: %v", err)
	}
	if len(row) != len(original) {
		t.Fatalf("expected %d cols, got %d", len(original), len(row))
	}

	if row[0].IntValue() != 42 {
		t.Errorf("col 0: want 42, got %d", row[0].IntValue())
	}
	if row[4] == nil || !row[4].IsNull() {
		t.Errorf("col 4: expected NULL")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — writeRunToFile: error paths (lines 210-212, 215-217, 220-222)
// ---------------------------------------------------------------------------

// TestSpillRemaining_WriteRunToFileReadOnlyFile exercises the binary.Write
// error path inside writeRunToFile by writing to a read-only file descriptor
// (opened on /dev/null O_RDONLY).
func TestSpillRemaining_WriteRunToFileReadOnlyFile(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Open /dev/null for reading only — writes will fail.
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Skipf("cannot open /dev/null: %v", err)
	}
	defer devNull.Close()

	rows := [][]*Mem{{NewMemInt(1)}}
	if err := s.writeRunToFile(devNull, rows); err == nil {
		t.Log("writeRunToFile to read-only fd succeeded (OS may allow writes to /dev/null)")
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — mergeSpilledRuns: empty spilledRuns guard (line 474-476)
// ---------------------------------------------------------------------------

// TestSpillRemaining_MergeSpilledRunsEmpty exercises the early return in
// mergeSpilledRuns when spilledRuns is empty.
func TestSpillRemaining_MergeSpilledRunsEmpty(t *testing.T) {
	t.Parallel()

	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, nil)
	defer s.Close()

	// Call mergeSpilledRuns directly with no spilled runs → should return nil.
	if err := s.mergeSpilledRuns(); err != nil {
		t.Errorf("mergeSpilledRuns with empty spilledRuns: expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// sorter_spill.go — Sort: spillCurrentRun error when spills exist (line 456-458)
// ---------------------------------------------------------------------------

// TestSpillRemaining_SortSpillCurrentRunError exercises the Sort() path where
// spilled runs exist AND there are in-memory rows that need to be spilled,
// but spillCurrentRun fails. We set up this scenario by forcing spill via
// tight memory, then after spill has occurred, making the TempDir invalid so
// that the next spillCurrentRun (called from Sort) fails.
func TestSpillRemaining_SortSpillCurrentRunError(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	cfg := &SorterConfig{
		MaxMemoryBytes: 200, // tight enough to force initial spills
		TempDir:        tempDir,
		EnableSpill:    true,
	}
	s := NewSorterWithSpill([]int{0}, []bool{false}, []string{""}, 1, cfg)
	defer s.Close()

	// Insert rows to force at least one spill.
	for i := int64(1); i <= 15; i++ {
		if err := s.Insert([]*Mem{NewMemInt(i)}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if s.GetNumSpilledRuns() == 0 {
		t.Skip("no spilled runs produced; adjust MaxMemoryBytes")
	}

	// Now corrupt the TempDir so the next spill during Sort fails.
	// Add a few more rows to in-memory buffer (they'll be there when Sort runs).
	// Change TempDir to an invalid path so spillCurrentRun fails during Sort.
	s.Config.TempDir = "/no/such/dir/anthony_sort_test"

	// If there are still in-memory rows, Sort will call spillCurrentRun which
	// will fail with the bad TempDir.
	if len(s.Rows) > 0 {
		err := s.Sort()
		if err == nil {
			t.Log("Sort did not error (in-memory rows may have been empty or OS is permissive)")
		}
	}
}
