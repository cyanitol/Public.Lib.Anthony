// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

// WindowFrameType represents the type of window frame
type WindowFrameType uint8

const (
	FrameRows   WindowFrameType = 0 // ROWS frame type
	FrameRange  WindowFrameType = 1 // RANGE frame type
	FrameGroups WindowFrameType = 2 // GROUPS frame type
)

// FrameBoundType represents the type of frame boundary
type FrameBoundType uint8

const (
	BoundUnboundedPreceding FrameBoundType = 0 // UNBOUNDED PRECEDING
	BoundPreceding          FrameBoundType = 1 // N PRECEDING
	BoundCurrentRow         FrameBoundType = 2 // CURRENT ROW
	BoundFollowing          FrameBoundType = 3 // N FOLLOWING
	BoundUnboundedFollowing FrameBoundType = 4 // UNBOUNDED FOLLOWING
)

// WindowFrameBound represents a single frame boundary
type WindowFrameBound struct {
	Type   FrameBoundType // Type of boundary
	Offset int            // Offset value for PRECEDING/FOLLOWING (ignored for other types)
}

// WindowFrame defines the frame specification for a window function
type WindowFrame struct {
	Type  WindowFrameType  // ROWS, RANGE, or GROUPS
	Start WindowFrameBound // Starting boundary
	End   WindowFrameBound // Ending boundary
}

// WindowPartition represents a partition of rows for window functions
type WindowPartition struct {
	Rows         [][]*Mem // All rows in this partition
	CurrentIndex int      // Current row index within partition
	FrameStart   int      // Start of current frame (inclusive)
	FrameEnd     int      // End of current frame (inclusive)
}

// WindowState tracks the state of window function execution
type WindowState struct {
	// Partitioning and ordering
	PartitionCols []int  // Column indices for PARTITION BY
	OrderByCols   []int  // Column indices for ORDER BY
	OrderByDesc   []bool // Descending flags for ORDER BY columns

	// Frame specification
	Frame WindowFrame // Frame definition

	// Partition data
	Partitions     []*WindowPartition // All partitions
	CurrentPartIdx int                // Index of current partition
	CurrentPartRow int                // Row index within current partition
	TotalRowsSeen  int                // Total rows processed across all partitions

	// Ranking state (for RANK, DENSE_RANK)
	LastRankRow             []*Mem // Last row used for ranking comparison
	CurrentRank             int64  // Current rank value
	CurrentDenseRank        int64  // Current dense rank value
	RowsAtCurrentRank       int64  // Number of rows with current rank
	LastRowCounterUpdate    []*Mem // Track last row that incremented CurrentPartRow
	RankingUpdateGeneration int    // Increments on each UpdateRankingFromRow call
	LastRankingGeneration   int    // Generation at which ranking was last updated
	RowIncrementGeneration  int    // Tracks how many times we've been asked to increment
	LastIncrementedAt       int    // RowIncrementGeneration value when we last actually incremented

	// Shared state tracking for multiple window functions
	WindowFunctionCount int // Number of window functions sharing this state
	CallsThisRow        int // Call count within current row (resets when == WindowFunctionCount)

	// Row buffer for LAG/LEAD
	RowBuffer [][]*Mem // Buffer of rows for accessing past/future rows

	// Aggregate state for aggregates in window context
	AggState interface{} // Aggregate function state
}

// NewWindowState creates a new window state
func NewWindowState(partitionCols, orderByCols []int, orderByDesc []bool, frame WindowFrame) *WindowState {
	return &WindowState{
		PartitionCols:           partitionCols,
		OrderByCols:             orderByCols,
		OrderByDesc:             orderByDesc,
		Frame:                   frame,
		Partitions:              make([]*WindowPartition, 0),
		CurrentPartIdx:          -1,
		CurrentPartRow:          -1,
		TotalRowsSeen:           0,
		CurrentRank:             0,
		CurrentDenseRank:        0,
		RowsAtCurrentRank:       0,
		RowBuffer:               make([][]*Mem, 0),
		RankingUpdateGeneration: 0,
		LastRankingGeneration:   -999, // Marker for uninitialized
	}
}

// DefaultWindowFrame returns the default window frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
func DefaultWindowFrame() WindowFrame {
	return WindowFrame{
		Type: FrameRange,
		Start: WindowFrameBound{
			Type: BoundUnboundedPreceding,
		},
		End: WindowFrameBound{
			Type: BoundCurrentRow,
		},
	}
}

// AddRow adds a row to the window state, partitioning as needed
func (ws *WindowState) AddRow(row []*Mem) {
	// If no partitions yet, or row belongs to a new partition, create new partition
	if len(ws.Partitions) == 0 || !ws.samePartition(row, ws.Partitions[len(ws.Partitions)-1].Rows[0]) {
		partition := &WindowPartition{
			Rows:         make([][]*Mem, 0),
			CurrentIndex: 0,
			FrameStart:   0,
			FrameEnd:     0,
		}
		ws.Partitions = append(ws.Partitions, partition)
	}

	// Add row to current partition
	currentPartition := ws.Partitions[len(ws.Partitions)-1]
	rowCopy := ws.CopyRow(row)
	currentPartition.Rows = append(currentPartition.Rows, rowCopy)
	ws.RowBuffer = append(ws.RowBuffer, rowCopy)
	ws.TotalRowsSeen++
}

// samePartition checks if two rows belong to the same partition
func (ws *WindowState) samePartition(row1, row2 []*Mem) bool {
	// If no partition columns, all rows are in same partition
	if len(ws.PartitionCols) == 0 {
		return true
	}

	// Compare partition column values
	for _, colIdx := range ws.PartitionCols {
		if colIdx >= len(row1) || colIdx >= len(row2) {
			continue
		}
		if row1[colIdx].Compare(row2[colIdx]) != 0 {
			return false
		}
	}

	return true
}

// CopyRow creates a deep copy of a row
func (ws *WindowState) CopyRow(row []*Mem) []*Mem {
	rowCopy := make([]*Mem, len(row))
	for i, m := range row {
		newMem := &Mem{}
		newMem.Copy(m)
		rowCopy[i] = newMem
	}
	return rowCopy
}

// NextRow advances to the next row and returns it, or nil if no more rows
func (ws *WindowState) NextRow() []*Mem {
	// Move to first partition if not yet started
	if ws.CurrentPartIdx < 0 {
		if len(ws.Partitions) == 0 {
			return nil
		}
		ws.CurrentPartIdx = 0
		ws.CurrentPartRow = 0
	} else {
		// Advance within current partition
		ws.CurrentPartRow++

		// Check if we need to move to next partition
		currentPart := ws.Partitions[ws.CurrentPartIdx]
		if ws.CurrentPartRow >= len(currentPart.Rows) {
			ws.CurrentPartIdx++
			ws.CurrentPartRow = 0

			// Check if we're done with all partitions
			if ws.CurrentPartIdx >= len(ws.Partitions) {
				return nil
			}
		}
	}

	// Update frame boundaries for current row
	ws.updateFrame()

	// Return current row
	return ws.CurrentRow()
}

// CurrentRow returns the current row
func (ws *WindowState) CurrentRow() []*Mem {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return nil
	}

	partition := ws.Partitions[ws.CurrentPartIdx]
	if ws.CurrentPartRow < 0 || ws.CurrentPartRow >= len(partition.Rows) {
		return nil
	}

	return partition.Rows[ws.CurrentPartRow]
}

// updateFrame updates frame start and end positions for the current row
func (ws *WindowState) updateFrame() {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return
	}

	partition := ws.Partitions[ws.CurrentPartIdx]
	currentRow := ws.CurrentPartRow
	partitionSize := len(partition.Rows)

	partition.FrameStart = ws.calculateFrameStart(currentRow, partitionSize)
	partition.FrameEnd = ws.calculateFrameEnd(currentRow, partitionSize)
}

// calculateFrameStart computes the starting position of the frame
func (ws *WindowState) calculateFrameStart(currentRow, partitionSize int) int {
	switch ws.Frame.Start.Type {
	case BoundUnboundedPreceding:
		return 0
	case BoundPreceding:
		return max(0, currentRow-ws.Frame.Start.Offset)
	case BoundCurrentRow:
		return currentRow
	case BoundFollowing:
		return min(partitionSize-1, currentRow+ws.Frame.Start.Offset)
	default:
		return 0
	}
}

// calculateFrameEnd computes the ending position of the frame
func (ws *WindowState) calculateFrameEnd(currentRow, partitionSize int) int {
	switch ws.Frame.End.Type {
	case BoundUnboundedFollowing:
		return partitionSize - 1
	case BoundFollowing:
		return min(partitionSize-1, currentRow+ws.Frame.End.Offset)
	case BoundCurrentRow:
		return currentRow
	case BoundPreceding:
		return max(0, currentRow-ws.Frame.End.Offset)
	default:
		return partitionSize - 1
	}
}

// GetFrameRows returns all rows in the current frame
func (ws *WindowState) GetFrameRows() [][]*Mem {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return nil
	}

	partition := ws.Partitions[ws.CurrentPartIdx]
	start := partition.FrameStart
	end := partition.FrameEnd

	if start > end || start >= len(partition.Rows) {
		return nil
	}

	end = min(end, len(partition.Rows)-1)
	return partition.Rows[start : end+1]
}

// GetPartitionSize returns the size of the current partition
func (ws *WindowState) GetPartitionSize() int {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return 0
	}
	return len(ws.Partitions[ws.CurrentPartIdx].Rows)
}

// GetCurrentRowNumber returns the row number within the current partition (1-based)
func (ws *WindowState) GetCurrentRowNumber() int64 {
	return int64(ws.CurrentPartRow + 1)
}

// UpdateRanking updates ranking information based on current row
func (ws *WindowState) UpdateRanking() {
	currentRow := ws.CurrentRow()
	if currentRow == nil {
		return
	}

	// Check if order by columns match last rank row
	if ws.LastRankRow != nil && ws.sameOrderByValues(currentRow, ws.LastRankRow) {
		// Same rank as previous
		ws.RowsAtCurrentRank++
	} else {
		// New rank
		ws.CurrentRank += ws.RowsAtCurrentRank
		ws.CurrentDenseRank++
		ws.RowsAtCurrentRank = 1
		ws.LastRankRow = ws.CopyRow(currentRow)
	}
}

// UpdateRankingFromRow updates ranking information from a provided row (for streaming mode)
// This will be called by each window function opcode, but we need to ensure ranking is only
// updated once per actual database row. We use a generation counter to detect when we've
// moved to a new row (based on CurrentPartRow changes).
func (ws *WindowState) UpdateRankingFromRow(currentRow []*Mem) {
	if currentRow == nil {
		return
	}

	// Track the current row number to detect when we've moved to a new row
	currentRowNum := ws.CurrentPartRow

	// If we've already updated ranking for this row number, skip
	// Use a marker value of -999 for uninitialized state
	if ws.LastRankingGeneration == currentRowNum && ws.LastRankingGeneration != -999 {
		return
	}
	ws.LastRankingGeneration = currentRowNum

	// First time setup
	if ws.LastRankRow == nil {
		ws.LastRankRow = ws.CopyRow(currentRow)
		ws.CurrentRank = 0
		ws.CurrentDenseRank = 1
		ws.RowsAtCurrentRank = 1
		return
	}

	// Update ranking state based on ORDER BY column comparison
	// Check if order by columns match last rank row
	if ws.sameOrderByValues(currentRow, ws.LastRankRow) {
		// Same rank as previous
		ws.RowsAtCurrentRank++
	} else {
		// New rank
		ws.CurrentRank += ws.RowsAtCurrentRank
		ws.CurrentDenseRank++
		ws.RowsAtCurrentRank = 1
		ws.LastRankRow = ws.CopyRow(currentRow)
	}
}

// sameOrderByValues checks if two rows have the same ORDER BY values
func (ws *WindowState) sameOrderByValues(row1, row2 []*Mem) bool {
	// If no order by columns, all rows are considered equal
	if len(ws.OrderByCols) == 0 {
		return true
	}

	// Compare order by column values
	for _, colIdx := range ws.OrderByCols {
		if colIdx >= len(row1) || colIdx >= len(row2) {
			continue
		}
		if row1[colIdx].Compare(row2[colIdx]) != 0 {
			return false
		}
	}

	return true
}

// SameRowValues checks if two rows have identical values across all columns
func (ws *WindowState) SameRowValues(row1, row2 []*Mem) bool {
	if len(row1) != len(row2) {
		return false
	}

	for i := range row1 {
		if row1[i].Compare(row2[i]) != 0 {
			return false
		}
	}

	return true
}

// GetRank returns the current RANK value
func (ws *WindowState) GetRank() int64 {
	return ws.CurrentRank + 1
}

// GetDenseRank returns the current DENSE_RANK value
func (ws *WindowState) GetDenseRank() int64 {
	return ws.CurrentDenseRank
}

// IncrementPartRowIfNewRow increments CurrentPartRow only if the given row
// is different from the last row that caused an increment. This prevents
// multiple window functions from double-incrementing when they share state.
func (ws *WindowState) IncrementPartRowIfNewRow(currentRow []*Mem) {
	if ws.LastRowCounterUpdate == nil || !ws.SameRowValues(currentRow, ws.LastRowCounterUpdate) {
		ws.CurrentPartRow++
		ws.LastRowCounterUpdate = ws.CopyRow(currentRow)
	}
}

// IncrementPartRowOnFirstCall increments CurrentPartRow only on the first call
// within a row group when multiple window functions share this state.
// It tracks call count and resets when all functions have been called.
func (ws *WindowState) IncrementPartRowOnFirstCall() {
	ws.CallsThisRow++

	// Only increment on the first call of each row group
	if ws.CallsThisRow == 1 {
		ws.CurrentPartRow++
	}

	// Reset call count when all window functions have been called
	// (or if WindowFunctionCount is 0/1, meaning single function mode)
	if ws.WindowFunctionCount > 0 && ws.CallsThisRow >= ws.WindowFunctionCount {
		ws.CallsThisRow = 0
	}
}

// GetLagRow returns the row N positions before the current row
func (ws *WindowState) GetLagRow(offset int) []*Mem {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return nil
	}

	partition := ws.Partitions[ws.CurrentPartIdx]
	targetIdx := ws.CurrentPartRow - offset

	if targetIdx < 0 || targetIdx >= len(partition.Rows) {
		return nil
	}

	return partition.Rows[targetIdx]
}

// GetLeadRow returns the row N positions after the current row
func (ws *WindowState) GetLeadRow(offset int) []*Mem {
	if ws.CurrentPartIdx < 0 || ws.CurrentPartIdx >= len(ws.Partitions) {
		return nil
	}

	partition := ws.Partitions[ws.CurrentPartIdx]
	targetIdx := ws.CurrentPartRow + offset

	if targetIdx < 0 || targetIdx >= len(partition.Rows) {
		return nil
	}

	return partition.Rows[targetIdx]
}

// GetFirstValue returns the first value in the current frame
func (ws *WindowState) GetFirstValue(colIdx int) *Mem {
	frameRows := ws.GetFrameRows()
	if len(frameRows) == 0 {
		return NewMemNull()
	}

	firstRow := frameRows[0]
	if colIdx >= len(firstRow) {
		return NewMemNull()
	}

	result := &Mem{}
	result.Copy(firstRow[colIdx])
	return result
}

// GetLastValue returns the last value in the current frame
func (ws *WindowState) GetLastValue(colIdx int) *Mem {
	frameRows := ws.GetFrameRows()
	if len(frameRows) == 0 {
		return NewMemNull()
	}

	lastRow := frameRows[len(frameRows)-1]
	if colIdx >= len(lastRow) {
		return NewMemNull()
	}

	result := &Mem{}
	result.Copy(lastRow[colIdx])
	return result
}

// GetNthValue returns the Nth value (1-based) in the current frame
func (ws *WindowState) GetNthValue(colIdx int, n int) *Mem {
	frameRows := ws.GetFrameRows()
	if n < 1 || n > len(frameRows) {
		return NewMemNull()
	}

	row := frameRows[n-1]
	if colIdx >= len(row) {
		return NewMemNull()
	}

	result := &Mem{}
	result.Copy(row[colIdx])
	return result
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
