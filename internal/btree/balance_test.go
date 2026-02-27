package btree

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// TestIsOverfull tests the isOverfull function
func TestIsOverfull(t *testing.T) {
	tests := []struct {
		name       string
		pageSize   uint32
		numCells   int
		cellSizes  []int
		wantFull   bool
	}{
		{
			name:      "empty page",
			pageSize:  4096,
			numCells:  0,
			cellSizes: []int{},
			wantFull:  false,
		},
		{
			name:      "page with small cells",
			pageSize:  4096,
			numCells:  3,
			cellSizes: []int{10, 15, 20},
			wantFull:  false,
		},
		{
			name:      "nearly full page",
			pageSize:  512, // Small page for easier testing
			numCells:  10,
			cellSizes: []int{40, 40, 40, 40, 40, 40, 40, 40, 40, 40}, // 400 bytes of cells
			wantFull:  false,
		},
		{
			name:      "overfull page",
			pageSize:  512,
			numCells:  12,
			cellSizes: []int{40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40}, // 480 bytes of cells
			wantFull:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page := createBalanceTestPage(tt.pageSize, PageTypeLeafTable, tt.numCells, tt.cellSizes)
			got := isOverfull(page)

			if got != tt.wantFull {
				t.Errorf("isOverfull() = %v, want %v (free space: %d)", got, tt.wantFull, page.FreeSpace())
			}
		})
	}
}

// TestIsUnderfullBalance tests the isUnderfull function for balance
func TestIsUnderfullBalance(t *testing.T) {
	tests := []struct {
		name       string
		pageSize   uint32
		numCells   int
		cellSizes  []int
		wantUnder  bool
	}{
		{
			name:      "empty page",
			pageSize:  4096,
			numCells:  0,
			cellSizes: []int{},
			wantUnder: false, // Empty pages are not underfull
		},
		{
			name:      "well-filled page",
			pageSize:  4096,
			numCells:  50,
			cellSizes: make([]int, 50), // Will be filled with 30-byte cells
			wantUnder: false,
		},
		{
			name:      "underfull page",
			pageSize:  4096,
			numCells:  2,
			cellSizes: []int{10, 10},
			wantUnder: true,
		},
		{
			name:      "single small cell",
			pageSize:  4096,
			numCells:  1,
			cellSizes: []int{5},
			wantUnder: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill in default cell sizes if needed
			cellSizes := tt.cellSizes
			if tt.name == "well-filled page" {
				for i := range cellSizes {
					cellSizes[i] = 30
				}
			}

			page := createBalanceTestPage(tt.pageSize, PageTypeLeafTable, tt.numCells, cellSizes)
			got := isUnderfull(page)

			if got != tt.wantUnder {
				info, _ := GetBalanceInfo(&Btree{UsableSize: tt.pageSize, Pages: map[uint32][]byte{page.PageNum: page.Data}}, page.PageNum)
				t.Errorf("isUnderfull() = %v, want %v\n%s", got, tt.wantUnder, info.String())
			}
		})
	}
}

// TestDefragmentPage tests the defragmentPage function
func TestDefragmentPage(t *testing.T) {
	// Create a page with fragmented space
	// Use page 2 to avoid file header complications
	pageNum := uint32(2)
	pageSize := uint32(4096)
	pageData := make([]byte, pageSize)
	headerOffset := 0
	pageData[headerOffset+0] = PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[headerOffset+3:], 3) // NumCells = 3

	// Insert three cells with gaps between them
	cell1 := EncodeTableLeafCell(1, []byte("first"))
	cell2 := EncodeTableLeafCell(2, []byte("second"))
	cell3 := EncodeTableLeafCell(3, []byte("third"))

	// Place cells with gaps
	offset3 := int(pageSize) - len(cell3)
	copy(pageData[offset3:], cell3)

	offset2 := offset3 - len(cell2) - 20 // 20-byte gap
	copy(pageData[offset2:], cell2)

	offset1 := offset2 - len(cell1) - 15 // 15-byte gap
	copy(pageData[offset1:], cell1)

	binary.BigEndian.PutUint16(pageData[headerOffset+5:], uint16(offset1))  // CellContentStart
	binary.BigEndian.PutUint16(pageData[headerOffset+8:], uint16(offset1))  // Cell 1 pointer
	binary.BigEndian.PutUint16(pageData[headerOffset+10:], uint16(offset2)) // Cell 2 pointer
	binary.BigEndian.PutUint16(pageData[headerOffset+12:], uint16(offset3)) // Cell 3 pointer
	pageData[headerOffset+7] = 35                                           // FragmentedBytes = 35

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		t.Fatalf("NewBtreePage() error = %v", err)
	}

	// Defragment
	err = defragmentPage(page)
	if err != nil {
		t.Fatalf("defragmentPage() error = %v", err)
	}

	// Verify fragmented bytes is now 0
	if page.Header.FragmentedBytes != 0 {
		t.Errorf("FragmentedBytes = %d, want 0", page.Header.FragmentedBytes)
	}

	// Verify cells are now adjacent
	ptrs, err := page.Header.GetCellPointers(pageData)
	if err != nil {
		t.Fatalf("GetCellPointers() error = %v", err)
	}

	// Check that cells are compacted (no gaps)
	for i := 0; i < len(ptrs)-1; i++ {
		cell1, _ := ParseCell(PageTypeLeafTable, pageData[ptrs[i]:], pageSize)
		expectedNext := ptrs[i] + cell1.CellSize
		if ptrs[i+1] != expectedNext {
			t.Errorf("Gap between cells: cell[%d] ends at %d, cell[%d] starts at %d",
				i, expectedNext, i+1, ptrs[i+1])
		}
	}
}

// TestBalance tests the balance function
func TestBalance(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*Btree) (*BtCursor, error)
		wantError bool
		errorMsg  string
	}{
		{
			name: "balanced page",
			setup: func(bt *Btree) (*BtCursor, error) {
				rootPage, _ := bt.CreateTable()
				cursor := NewCursor(bt, rootPage)

				// Insert a moderate number of cells
				for i := int64(1); i <= 10; i++ {
					if err := cursor.Insert(i, []byte("data")); err != nil {
						return nil, err
					}
				}

				// Position cursor on a valid cell
				cursor.SeekRowid(5)
				return cursor, nil
			},
			wantError: false,
		},
		{
			name: "nearly full page",
			setup: func(bt *Btree) (*BtCursor, error) {
				rootPage, _ := bt.CreateTable()
				cursor := NewCursor(bt, rootPage)

				// Insert many cells to fill the page
				// With 4KB pages, we can fit many small cells
				for i := int64(1); i <= 100; i++ {
					if err := cursor.Insert(i, []byte("x")); err != nil {
						// If we get an overflow error, that's expected
						if err.Error() == fmt.Sprintf("page %d is full (need 9 bytes, have 0)", rootPage) {
							break
						}
						return nil, err
					}
				}

				cursor.SeekRowid(1)
				return cursor, nil
			},
			wantError: false, // Should handle near-full gracefully
		},
		{
			name: "page with fragmentation",
			setup: func(bt *Btree) (*BtCursor, error) {
				rootPage, _ := bt.CreateTable()
				cursor := NewCursor(bt, rootPage)

				// Insert and delete to create fragmentation
				for i := int64(1); i <= 20; i++ {
					cursor.Insert(i, []byte("data"))
				}

				// Delete every other cell to create fragmentation
				for i := int64(2); i <= 20; i += 2 {
					cursor.SeekRowid(i)
					cursor.Delete()
				}

				cursor.SeekRowid(1)
				return cursor, nil
			},
			wantError: false,
		},
		{
			name: "invalid cursor state",
			setup: func(bt *Btree) (*BtCursor, error) {
				rootPage, _ := bt.CreateTable()
				cursor := NewCursor(bt, rootPage)
				cursor.State = CursorInvalid
				return cursor, nil
			},
			wantError: true,
			errorMsg:  "cannot balance: cursor not in valid state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := NewBtree(4096)
			cursor, err := tt.setup(bt)
			if err != nil {
				t.Fatalf("setup() error = %v", err)
			}

			err = balance(cursor)

			if tt.wantError {
				if err == nil {
					t.Error("balance() expected error, got nil")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("balance() error = %v, want %v", err, tt.errorMsg)
				}
			} else {
				// For non-error cases, we allow "overfull" or "underfull" messages
				// since we're not implementing split/merge yet
				if err != nil {
					errMsg := err.Error()
					if errMsg != fmt.Sprintf("page %d is overfull and requires split", cursor.CurrentPage) &&
						errMsg != fmt.Sprintf("page %d is underfull and may need merge or redistribution", cursor.CurrentPage) {
						t.Errorf("balance() unexpected error = %v", err)
					}
				}
			}
		})
	}
}

// TestGetBalanceInfo tests the GetBalanceInfo function
func TestGetBalanceInfo(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Test empty page
	info, err := GetBalanceInfo(bt, rootPage)
	if err != nil {
		t.Fatalf("GetBalanceInfo() error = %v", err)
	}

	if info.NumCells != 0 {
		t.Errorf("NumCells = %d, want 0", info.NumCells)
	}

	if info.IsOverfull {
		t.Error("Empty page should not be overfull")
	}

	if info.IsUnderfull {
		t.Error("Empty page should not be underfull")
	}

	if !info.IsBalanced {
		t.Error("Empty page should be balanced")
	}

	// Add some data
	cursor := NewCursor(bt, rootPage)
	for i := int64(1); i <= 10; i++ {
		cursor.Insert(i, []byte("test data"))
	}

	// Test page with data
	info, err = GetBalanceInfo(bt, rootPage)
	if err != nil {
		t.Fatalf("GetBalanceInfo() error = %v", err)
	}

	if info.NumCells != 10 {
		t.Errorf("NumCells = %d, want 10", info.NumCells)
	}

	if info.FillFactor < 0 || info.FillFactor > 1 {
		t.Errorf("FillFactor = %f, want 0.0-1.0", info.FillFactor)
	}

	t.Logf("Balance info: %s", info.String())
}

// TestBalanceInfoString tests the String method of BalanceInfo
func TestBalanceInfoString(t *testing.T) {
	tests := []struct {
		name string
		info BalanceInfo
		want string
	}{
		{
			name: "balanced page",
			info: BalanceInfo{
				PageNum:      1,
				NumCells:     10,
				UsedSpace:    500,
				UsableSize:   4096,
				FillFactor:   0.122,
				IsBalanced:   true,
				FragmentedBytes: 0,
			},
			want: "Page 1: 10 cells, 500/4096 bytes used (12.2%), balanced, fragmented=0",
		},
		{
			name: "overfull page",
			info: BalanceInfo{
				PageNum:      2,
				NumCells:     100,
				UsedSpace:    4000,
				UsableSize:   4096,
				FillFactor:   0.977,
				IsOverfull:   true,
				FragmentedBytes: 5,
			},
			want: "Page 2: 100 cells, 4000/4096 bytes used (97.7%), OVERFULL, fragmented=5",
		},
		{
			name: "underfull page",
			info: BalanceInfo{
				PageNum:      3,
				NumCells:     2,
				UsedSpace:    50,
				UsableSize:   4096,
				FillFactor:   0.012,
				IsUnderfull:  true,
				FragmentedBytes: 0,
			},
			want: "Page 3: 2 cells, 50/4096 bytes used (1.2%), UNDERFULL, fragmented=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBalanceAfterInsert tests balance checking after insertions
func TestBalanceAfterInsert(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Track balance info as we insert
	insertCounts := []int{1, 5, 10, 20, 50}

	for _, count := range insertCounts {
		// Insert up to count
		for i := int64(len(insertCounts) + 1); i <= int64(count); i++ {
			err := cursor.Insert(i, []byte("test data for balancing"))
			if err != nil {
				// Stop if page is full
				break
			}
		}

		// Check balance
		cursor.SeekRowid(1)
		info, err := GetBalanceInfo(bt, rootPage)
		if err != nil {
			t.Fatalf("GetBalanceInfo() error = %v", err)
		}

		t.Logf("After %d insertions: %s", count, info.String())

		// Balance should work without panicking
		err = balance(cursor)
		if err != nil {
			// It's OK to get overfull/underfull errors since we're not implementing split/merge
			t.Logf("balance() returned: %v", err)
		}
	}
}

// TestBalanceAfterDelete tests balance checking after deletions
func TestBalanceAfterDelete(t *testing.T) {
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	cursor := NewCursor(bt, rootPage)

	// Insert many rows
	for i := int64(1); i <= 50; i++ {
		cursor.Insert(i, []byte("test data"))
	}

	// Delete progressively
	deleteCounts := []int{10, 20, 30, 40}

	for _, count := range deleteCounts {
		// Delete up to count
		for i := int64(1); i <= int64(count); i++ {
			found, _ := cursor.SeekRowid(i)
			if found {
				cursor.Delete()
			}
		}

		// Check balance after deletions
		cursor.SeekRowid(int64(count + 1))
		if cursor.State == CursorValid {
			info, err := GetBalanceInfo(bt, rootPage)
			if err != nil {
				t.Fatalf("GetBalanceInfo() error = %v", err)
			}

			t.Logf("After %d deletions: %s", count, info.String())

			err = balance(cursor)
			if err != nil {
				t.Logf("balance() returned: %v", err)
			}
		}
	}
}

// Helper function to create a test page with specific characteristics for balance tests
func createBalanceTestPage(pageSize uint32, pageType byte, numCells int, cellSizes []int) *BtreePage {
	pageData := make([]byte, pageSize)

	if len(cellSizes) != numCells {
		panic("cellSizes length must match numCells")
	}

	// Use page 2 to avoid file header complications (page 1 has 100-byte header)
	pageNum := uint32(2)
	headerOffset := 0

	// Set page type
	pageData[headerOffset] = pageType

	// Set number of cells
	binary.BigEndian.PutUint16(pageData[headerOffset+3:], uint16(numCells))

	if numCells == 0 {
		// Empty page - set cell content start to 0
		binary.BigEndian.PutUint16(pageData[headerOffset+5:], 0)
		page, err := NewBtreePage(pageNum, pageData, pageSize)
		if err != nil {
			panic(err)
		}
		return page
	}

	// Insert cells from end of page backwards
	cellOffset := int(pageSize)
	headerSize := PageHeaderSizeLeaf

	for i := numCells - 1; i >= 0; i-- {
		// Create a cell with the specified size
		var cell []byte
		if cellSizes[i] > 0 {
			payload := make([]byte, cellSizes[i])
			cell = EncodeTableLeafCell(int64(i+1), payload)
		} else {
			cell = EncodeTableLeafCell(int64(i+1), []byte{})
		}

		cellOffset -= len(cell)
		copy(pageData[cellOffset:], cell)

		// Write cell pointer
		ptrOffset := headerSize + (i * 2)
		binary.BigEndian.PutUint16(pageData[ptrOffset:], uint16(cellOffset))
	}

	// Set cell content start
	binary.BigEndian.PutUint16(pageData[headerOffset+5:], uint16(cellOffset))

	page, err := NewBtreePage(pageNum, pageData, pageSize)
	if err != nil {
		panic(err)
	}

	return page
}

// BenchmarkIsOverfull benchmarks the isOverfull function
func BenchmarkIsOverfull(b *testing.B) {
	page := createBalanceTestPage(4096, PageTypeLeafTable, 50, make([]int, 50))
	for i := range make([]int, 50) {
		page.Data[i] = 20
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = isOverfull(page)
	}
}

// BenchmarkIsUnderfull benchmarks the isUnderfull function
func BenchmarkIsUnderfull(b *testing.B) {
	cellSizes := make([]int, 10)
	for i := range cellSizes {
		cellSizes[i] = 20
	}
	page := createBalanceTestPage(4096, PageTypeLeafTable, 10, cellSizes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = isUnderfull(page)
	}
}

// BenchmarkBalance benchmarks the balance function
func BenchmarkBalance(b *testing.B) {
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	cursor := NewCursor(bt, rootPage)

	for i := int64(1); i <= 20; i++ {
		cursor.Insert(i, []byte("test data"))
	}

	cursor.SeekRowid(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = balance(cursor)
	}
}
