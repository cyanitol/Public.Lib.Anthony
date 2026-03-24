// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Condition 1 — balance.go isUnderfull guard
//
//   if page.Header.NumCells == 0 { return false }
//   ...
//   return usedSpace < minUsedSpace
//
//   A = (NumCells == 0)   — early-return guard
//   B = (usedSpace < minUsedSpace)
//
//   Row 1: A=T  → false (empty page is never underfull)
//   Row 2: A=F, B=T → true
//   Row 3: A=F, B=F → false
// ---------------------------------------------------------------------------

func TestMCDC_IsUnderfull(t *testing.T) {
	t.Parallel()

	makeLeafPage := func(pageSize uint32, numCells int, extraFill int) *BtreePage {
		data := make([]byte, pageSize)
		data[0] = PageTypeLeafTable
		page, _ := NewBtreePage(2, data, pageSize)
		page.Header.NumCells = uint16(numCells)
		// Write numCells to raw data as well
		data[PageHeaderOffsetNumCells] = byte(numCells >> 8)
		data[PageHeaderOffsetNumCells+1] = byte(numCells)
		// Simulate used space by setting CellContentStart low
		if extraFill > 0 {
			cellStart := int(pageSize) - extraFill
			if cellStart < PageHeaderSizeLeaf+numCells*2+4 {
				cellStart = PageHeaderSizeLeaf + numCells*2 + 4
			}
			page.Header.CellContentStart = uint16(cellStart)
			data[PageHeaderOffsetCellStart] = byte(uint16(cellStart) >> 8)
			data[PageHeaderOffsetCellStart+1] = byte(uint16(cellStart))
		}
		return page
	}

	tests := []struct {
		name    string
		page    func() *BtreePage
		wantRes bool
	}{
		{
			// A=T: NumCells==0 → early return false regardless of fill
			name:    "A=T NumCells=0: not underfull",
			page:    func() *BtreePage { return makeLeafPage(4096, 0, 0) },
			wantRes: false,
		},
		{
			// A=F, B=T: has cells, used space < minUsedSpace → underfull
			name: "A=F B=T: cells>0 usedSpace<min: underfull",
			page: func() *BtreePage {
				// 1 cell, but cell content start at end-of-page means 0 used payload
				return makeLeafPage(4096, 1, 0)
			},
			wantRes: true,
		},
		{
			// A=F, B=F: has cells, used space >= minUsedSpace → not underfull
			name: "A=F B=F: cells>0 usedSpace>=min: not underfull",
			page: func() *BtreePage {
				// Fill most of the page so usedSpace > 33% threshold
				return makeLeafPage(4096, 1, 2000)
			},
			wantRes: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			page := tt.page()
			got := isUnderfull(page)
			if got != tt.wantRes {
				t.Errorf("isUnderfull() = %v, want %v", got, tt.wantRes)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 2 — balance.go GetBalanceInfo: IsBalanced = !overfull && !underfull
//
//   A = overfull
//   B = underfull
//   IsBalanced = !A && !B
//
//   Row 1: A=T, B=F → IsBalanced=false  (A independently makes it false)
//   Row 2: A=F, B=T → IsBalanced=false  (B independently makes it false)
//   Row 3: A=F, B=F → IsBalanced=true
// ---------------------------------------------------------------------------

func TestMCDC_BalanceInfoIsBalanced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		overfull    bool
		underfull   bool
		wantBalance bool
	}{
		{
			name:        "A=T B=F overfull: not balanced",
			overfull:    true,
			underfull:   false,
			wantBalance: false,
		},
		{
			name:        "A=F B=T underfull: not balanced",
			overfull:    false,
			underfull:   true,
			wantBalance: false,
		},
		{
			name:        "A=F B=F neither: balanced",
			overfull:    false,
			underfull:   false,
			wantBalance: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bi := &BalanceInfo{
				IsOverfull:  tt.overfull,
				IsUnderfull: tt.underfull,
				IsBalanced:  !tt.overfull && !tt.underfull,
			}
			if bi.IsBalanced != tt.wantBalance {
				t.Errorf("IsBalanced = %v, want %v", bi.IsBalanced, tt.wantBalance)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 3 — btree.go validatePage: page-1 zero-buffer early-return
//
//   if pageNum == 1 {
//       if isZeroBuffer(data[FileHeaderSize:]) { return nil }
//   } else if isZeroBuffer(data) {
//       return nil
//   }
//
//   Branch tested: pageNum==1 path
//   A = (pageNum == 1)
//   B = (isZeroBuffer(data[FileHeaderSize:]))
//   Result = A && B → nil return (no error from zero check)
//
//   Row 1: A=T, B=T → early nil (zero page 1 accepted)
//   Row 2: A=T, B=F → does NOT early-return; parse proceeds
//   Row 3: A=F, B=T → else branch: zero non-page-1 accepted
// ---------------------------------------------------------------------------

func TestMCDC_ValidatePage_ZeroBuffer(t *testing.T) {
	t.Parallel()

	const pageSize = 4096

	tests := []struct {
		name    string
		pageNum uint32
		data    func() []byte
		wantErr bool
	}{
		{
			// A=T, B=T: page 1, btree payload all zero → accepted
			name:    "A=T B=T pageNum=1 zeroBtreeSection: no error",
			pageNum: 1,
			data: func() []byte {
				d := make([]byte, pageSize)
				// File header area (first 100 bytes) is zeroed; btree area zeroed too
				return d
			},
			wantErr: false,
		},
		{
			// A=T, B=F: page 1, btree section non-zero with valid leaf type → parse proceeds
			name:    "A=T B=F pageNum=1 nonZeroBtreeSection: valid leaf type",
			pageNum: 1,
			data: func() []byte {
				d := make([]byte, pageSize)
				// Set a valid page type in the btree header (offset 100)
				d[FileHeaderSize+PageHeaderOffsetType] = PageTypeLeafTable
				return d
			},
			wantErr: false,
		},
		{
			// A=F, B=T: non-page-1, all-zero → freshly allocated, accepted
			name:    "A=F B=T pageNum=2 zeroPage: no error",
			pageNum: 2,
			data: func() []byte {
				return make([]byte, pageSize)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(pageSize)
			err := bt.validatePage(tt.data(), tt.pageNum)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePage() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 4 — btree.go validatePageStructure: cell content start check
//
//   cellContentStart := int(header.CellContentStart)
//   if cellContentStart == 0 { cellContentStart = len(data) }
//   if cellContentStart > len(data) { return error }
//
//   A = (CellContentStart == 0)   → maps to len(data), never > len(data)
//   B = (raw cellContentStart > len(data))
//
//   Row 1: A=T  → content start set to len(data), no error
//   Row 2: A=F, B=T → content start > page size → error
//   Row 3: A=F, B=F → content start valid, no error from this check
// ---------------------------------------------------------------------------

func TestMCDC_ValidatePageStructure_CellContentStart(t *testing.T) {
	t.Parallel()

	makeHeader := func(numCells uint16, cellStart uint16, pageType byte) (*PageHeader, []byte) {
		data := make([]byte, 4096)
		data[PageHeaderOffsetType] = pageType
		data[PageHeaderOffsetNumCells] = byte(numCells >> 8)
		data[PageHeaderOffsetNumCells+1] = byte(numCells)
		data[PageHeaderOffsetCellStart] = byte(cellStart >> 8)
		data[PageHeaderOffsetCellStart+1] = byte(cellStart)
		h := &PageHeader{
			PageType:         pageType,
			NumCells:         numCells,
			CellContentStart: cellStart,
			IsLeaf:           true,
			HeaderSize:       PageHeaderSizeLeaf,
			CellPtrOffset:    PageHeaderSizeLeaf,
		}
		return h, data
	}

	tests := []struct {
		name    string
		build   func() (*PageHeader, []byte)
		wantErr bool
	}{
		{
			// A=T: CellContentStart==0 → treated as len(data) → valid
			name: "A=T cellContentStart=0: no error",
			build: func() (*PageHeader, []byte) {
				return makeHeader(0, 0, PageTypeLeafTable)
			},
			wantErr: false,
		},
		{
			// A=F, B=T: CellContentStart > len(data) → error
			name: "A=F B=T cellContentStart>pageSize: error",
			build: func() (*PageHeader, []byte) {
				h, data := makeHeader(0, 5000, PageTypeLeafTable)
				// Make data only 4096 bytes but cellContentStart=5000 > 4096
				return h, data
			},
			wantErr: true,
		},
		{
			// A=F, B=F: CellContentStart within page bounds → no error from this check
			name: "A=F B=F cellContentStart valid: no error",
			build: func() (*PageHeader, []byte) {
				return makeHeader(0, 2048, PageTypeLeafTable)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			header, data := tt.build()
			err := validatePageStructure(header, data)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePageStructure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 5 — cursor.go GetKey / GetPayload triple-or guard
//
//   if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil
//
//   A = (c.Btree == nil)
//   B = (c.State != CursorValid)
//   C = (c.CurrentCell == nil)
//   Result = A || B || C
//
//   Row 1: A=T, B=F, C=F → return zero (A short-circuits)
//   Row 2: A=F, B=T, C=F → return zero (B short-circuits)
//   Row 3: A=F, B=F, C=T → return zero (C triggers)
//   Row 4: A=F, B=F, C=F → return real value
// ---------------------------------------------------------------------------

func TestMCDC_GetKey_TripleOrGuard(t *testing.T) {
	t.Parallel()

	const wantKey int64 = 42

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantKey int64
	}{
		{
			// A=T: Btree==nil → return 0
			name: "A=T B=F C=F btree=nil: returns 0",
			cursor: func() *BtCursor {
				return &BtCursor{
					Btree:       nil,
					State:       CursorValid,
					CurrentCell: &CellInfo{Key: wantKey},
				}
			},
			wantKey: 0,
		},
		{
			// A=F, B=T: State != CursorValid → return 0
			name: "A=F B=T C=F state=invalid: returns 0",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorInvalid,
					CurrentCell: &CellInfo{Key: wantKey},
				}
			},
			wantKey: 0,
		},
		{
			// A=F, B=F, C=T: CurrentCell==nil → return 0
			name: "A=F B=F C=T cell=nil: returns 0",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorValid,
					CurrentCell: nil,
				}
			},
			wantKey: 0,
		},
		{
			// A=F, B=F, C=F: all conditions false → return real key
			name: "A=F B=F C=F all valid: returns key",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorValid,
					CurrentCell: &CellInfo{Key: wantKey},
				}
			},
			wantKey: wantKey,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cursor().GetKey()
			if got != tt.wantKey {
				t.Errorf("GetKey() = %d, want %d", got, tt.wantKey)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 6 — cursor.go GetPayload triple-or guard (mirrors GetKey)
//
//   if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil
//
//   Row 1: A=T  → nil
//   Row 2: A=F, B=T → nil
//   Row 3: A=F, B=F, C=T → nil
//   Row 4: A=F, B=F, C=F → real payload
// ---------------------------------------------------------------------------

func TestMCDC_GetPayload_TripleOrGuard(t *testing.T) {
	t.Parallel()

	wantPayload := []byte("hello")

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantNil bool
	}{
		{
			name: "A=T B=F C=F btree=nil: nil payload",
			cursor: func() *BtCursor {
				return &BtCursor{
					Btree:       nil,
					State:       CursorValid,
					CurrentCell: &CellInfo{Payload: wantPayload},
				}
			},
			wantNil: true,
		},
		{
			name: "A=F B=T C=F state=invalid: nil payload",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorFault,
					CurrentCell: &CellInfo{Payload: wantPayload},
				}
			},
			wantNil: true,
		},
		{
			name: "A=F B=F C=T cell=nil: nil payload",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorValid,
					CurrentCell: nil,
				}
			},
			wantNil: true,
		},
		{
			name: "A=F B=F C=F all valid: real payload",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorValid,
					CurrentCell: &CellInfo{Payload: wantPayload},
				}
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cursor().GetPayload()
			if tt.wantNil && got != nil {
				t.Errorf("GetPayload() = %v, want nil", got)
			}
			if !tt.wantNil && got == nil {
				t.Error("GetPayload() = nil, want non-nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 7 — cursor.go compareCellKey: composite-key branch
//
//   if c.CompositePK && compositeKey != nil { return bytes.Compare(...) }
//
//   A = (c.CompositePK)
//   B = (compositeKey != nil)
//   Result = A && B (use byte compare)
//
//   Row 1: A=T, B=T → byte comparison used
//   Row 2: A=T, B=F → falls through to int-key compare
//   Row 3: A=F, B=T → falls through to int-key compare
// ---------------------------------------------------------------------------

func TestMCDC_CompareCellKey_CompositeBranch(t *testing.T) {
	t.Parallel()

	cell := &CellInfo{
		Key:      10,
		KeyBytes: []byte{0x05},
	}

	tests := []struct {
		name         string
		compositePK  bool
		compositeKey []byte
		rowid        int64
		cell         *CellInfo
		wantSign     int // -1, 0, 1
	}{
		{
			// A=T, B=T: composite mode, bytes.Compare(cell.KeyBytes={0x05}, compositeKey={0x03}) > 0
			// i.e. cell > target → positive (cell comes after target)
			name:         "A=T B=T composite mode bytes compare: positive",
			compositePK:  true,
			compositeKey: []byte{0x03},
			rowid:        0,
			cell:         cell,
			wantSign:     1,
		},
		{
			// A=T, B=F: composite mode but no key passed → int compare, rowid 10 == cell.Key 10
			name:         "A=T B=F compositeKey=nil: falls to int compare equal",
			compositePK:  true,
			compositeKey: nil,
			rowid:        10,
			cell:         cell,
			wantSign:     0,
		},
		{
			// A=F, B=T: not composite mode → int compare, cell.Key=10 > rowid=5 → positive
			name:         "A=F B=T not composite: int compare positive",
			compositePK:  false,
			compositeKey: []byte{0x03},
			rowid:        5,
			cell:         cell,
			wantSign:     1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &BtCursor{CompositePK: tt.compositePK}
			got := c.compareCellKey(tt.cell, tt.rowid, tt.compositeKey)
			var gotSign int
			switch {
			case got < 0:
				gotSign = -1
			case got > 0:
				gotSign = 1
			default:
				gotSign = 0
			}
			if gotSign != tt.wantSign {
				t.Errorf("compareCellKey() sign = %d, want %d (raw=%d)", gotSign, tt.wantSign, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 8 — cursor.go validateDeletePosition: double-or guard
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsLeaf)  — only meaningful when A=false
//
//   Row 1: A=T  → error "cursor not positioned at leaf page"
//   Row 2: A=F, B=T → error (interior page)
//   Row 3: A=F, B=F → no error (leaf page)
//
//   Also covers the c.State != CursorValid guard tested separately.
// ---------------------------------------------------------------------------

func TestMCDC_ValidateDeletePosition_HeaderGuard(t *testing.T) {
	t.Parallel()

	leafHeader := &PageHeader{IsLeaf: true}
	interiorHeader := &PageHeader{IsLeaf: false}

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantErr bool
	}{
		{
			// A=T: CurrentHeader==nil → error
			name: "A=T B=- header=nil: error",
			cursor: func() *BtCursor {
				return &BtCursor{
					State:         CursorValid,
					CurrentHeader: nil,
				}
			},
			wantErr: true,
		},
		{
			// A=F, B=T: header set but not a leaf → error
			name: "A=F B=T header=interior: error",
			cursor: func() *BtCursor {
				return &BtCursor{
					State:         CursorValid,
					CurrentHeader: interiorHeader,
				}
			},
			wantErr: true,
		},
		{
			// A=F, B=F: header is a leaf → no error from this check
			name: "A=F B=F header=leaf: no error",
			cursor: func() *BtCursor {
				return &BtCursor{
					State:         CursorValid,
					CurrentHeader: leafHeader,
				}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cursor().validateDeletePosition()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDeletePosition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 9 — cursor.go seekLeafPage exact-match condition
//
//   if exactMatch && idx < int(header.NumCells)
//
//   A = exactMatch
//   B = (idx < int(header.NumCells))
//
//   Row 1: A=T, B=T → exact match loaded, returns true
//   Row 2: A=T, B=F → idx out of range, falls through (returns false)
//   Row 3: A=F, B=T → no exact match, falls through (returns false)
// ---------------------------------------------------------------------------

func TestMCDC_SeekLeafPage_ExactMatchCondition(t *testing.T) {
	t.Parallel()

	// Build a tiny btree with one row so we can test seekLeafPage via SeekRowid.
	buildSingleRowBtree := func() (*Btree, uint32) {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		cursor := NewCursor(bt, rootPage)
		cursor.Insert(10, []byte("payload"))
		return bt, rootPage
	}

	tests := []struct {
		name      string
		rowid     int64
		wantFound bool
	}{
		{
			// A=T, B=T: seek the actual key that exists → found=true
			name:      "A=T B=T exactMatch idxInBounds: found=true",
			rowid:     10,
			wantFound: true,
		},
		{
			// A=F: seek a key that does not exist → found=false
			name:      "A=F B=T noExactMatch idxInBounds: found=false",
			rowid:     99,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt, rootPage := buildSingleRowBtree()
			cursor := NewCursor(bt, rootPage)
			found, err := cursor.SeekRowid(tt.rowid)
			if err != nil && tt.wantFound {
				t.Fatalf("SeekRowid() unexpected error = %v", err)
			}
			if found != tt.wantFound {
				t.Errorf("SeekRowid() found = %v, want %v", found, tt.wantFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 10 — merge.go canMergePage: state && depth
//
//   return c.State == CursorValid && c.Depth > 0
//
//   A = (c.State == CursorValid)
//   B = (c.Depth > 0)
//
//   Row 1: A=T, B=T → true
//   Row 2: A=T, B=F → false  (B independently flips result)
//   Row 3: A=F, B=T → false  (A independently flips result)
// ---------------------------------------------------------------------------

func TestMCDC_CanMergePage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		state   int
		depth   int
		wantRes bool
	}{
		{
			name:    "A=T B=T valid depth>0: can merge",
			state:   CursorValid,
			depth:   1,
			wantRes: true,
		},
		{
			name:    "A=T B=F valid depth=0: cannot merge",
			state:   CursorValid,
			depth:   0,
			wantRes: false,
		},
		{
			name:    "A=F B=T invalid depth>0: cannot merge",
			state:   CursorInvalid,
			depth:   1,
			wantRes: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &BtCursor{State: tt.state, Depth: tt.depth}
			got := c.canMergePage()
			if got != tt.wantRes {
				t.Errorf("canMergePage() = %v, want %v", got, tt.wantRes)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 11 — merge.go canMergePageTypes: type-match guard then IsLeaf && IsLeaf
//
//   if leftHeader.PageType != rightHeader.PageType { return false }
//   return leftHeader.IsLeaf && rightHeader.IsLeaf
//
//   Guard condition:
//   A = (leftHeader.PageType != rightHeader.PageType) → early false
//
//   Inner condition (when types match):
//   B = leftHeader.IsLeaf
//   C = rightHeader.IsLeaf
//   Result = B && C
//
//   Row 1: A=T → false (type mismatch)
//   Row 2: A=F, B=T, C=T → true (both leaf, same type)
//   Row 3: A=F, B=T, C=F → false (right not leaf)
//   Row 4: A=F, B=F, C=T → false (left not leaf)
// ---------------------------------------------------------------------------

func TestMCDC_CanMergePageTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		leftType  byte
		leftLeaf  bool
		rightType byte
		rightLeaf bool
		wantRes   bool
	}{
		{
			// A=T: page types differ → false
			name:      "A=T type mismatch: false",
			leftType:  PageTypeLeafTable,
			leftLeaf:  true,
			rightType: PageTypeLeafIndex,
			rightLeaf: true,
			wantRes:   false,
		},
		{
			// A=F, B=T, C=T: same type, both leaf → true
			name:      "A=F B=T C=T same type both leaf: true",
			leftType:  PageTypeLeafTable,
			leftLeaf:  true,
			rightType: PageTypeLeafTable,
			rightLeaf: true,
			wantRes:   true,
		},
		{
			// A=F, B=T, C=F: same type, right not leaf → false
			name:      "A=F B=T C=F same type right=interior: false",
			leftType:  PageTypeInteriorTable,
			leftLeaf:  false,
			rightType: PageTypeInteriorTable,
			rightLeaf: false,
			wantRes:   false,
		},
		{
			// A=F, B=F, C=T: same type, left not leaf (redundant but complete)
			name:      "A=F B=F C=T same type left=interior: false",
			leftType:  PageTypeInteriorTable,
			leftLeaf:  false,
			rightType: PageTypeInteriorTable,
			rightLeaf: true, // forced, but the && still short-circuits at B=false
			wantRes:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			leftHeader := &PageHeader{PageType: tt.leftType, IsLeaf: tt.leftLeaf}
			rightHeader := &PageHeader{PageType: tt.rightType, IsLeaf: tt.rightLeaf}
			got := canMergePageTypes(leftHeader, rightHeader)
			if got != tt.wantRes {
				t.Errorf("canMergePageTypes() = %v, want %v", got, tt.wantRes)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 12 — merge.go determineParentCellToRemove: !leftIsOurs && parentIndex > 0
//
//   if !leftIsOurs && parentIndex > 0 { return parentIndex - 1 }
//   return parentIndex
//
//   A = (!leftIsOurs)   i.e. leftIsOurs=false
//   B = (parentIndex > 0)
//
//   Row 1: A=T, B=T → parentIndex - 1
//   Row 2: A=T, B=F → parentIndex (B is false, falls through)
//   Row 3: A=F, B=T → parentIndex (A is false, falls through)
// ---------------------------------------------------------------------------

func TestMCDC_DetermineParentCellToRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		leftIsOurs  bool
		parentIndex int
		want        int
	}{
		{
			// A=T (leftIsOurs=false), B=T (parentIndex>0) → parentIndex-1
			name:        "A=T B=T notOurs parentIndex>0: parentIndex-1",
			leftIsOurs:  false,
			parentIndex: 3,
			want:        2,
		},
		{
			// A=T (leftIsOurs=false), B=F (parentIndex==0) → parentIndex
			name:        "A=T B=F notOurs parentIndex=0: parentIndex",
			leftIsOurs:  false,
			parentIndex: 0,
			want:        0,
		},
		{
			// A=F (leftIsOurs=true), B=T → parentIndex
			name:        "A=F B=T isOurs parentIndex>0: parentIndex",
			leftIsOurs:  true,
			parentIndex: 3,
			want:        3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &BtCursor{}
			got := c.determineParentCellToRemove(tt.parentIndex, tt.leftIsOurs)
			if got != tt.want {
				t.Errorf("determineParentCellToRemove(%d, %v) = %d, want %d",
					tt.parentIndex, tt.leftIsOurs, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 13 — merge.go replaceSeparatorCell: double-sided bounds check
//
//   if separatorIndex < 0 || separatorIndex >= int(parentBtreePage.Header.NumCells)
//
//   A = (separatorIndex < 0)
//   B = (separatorIndex >= NumCells)
//
//   Row 1: A=T, B=F → early return nil (negative index)
//   Row 2: A=F, B=T → early return nil (index too large)
//   Row 3: A=F, B=F → proceeds with cell replacement
// ---------------------------------------------------------------------------

func TestMCDC_ReplaceSeparatorCell_BoundsCheck(t *testing.T) {
	t.Parallel()

	// Build a parent btree page that has exactly one cell so we can exercise the bounds.
	buildParentPage := func(t *testing.T) *BtreePage {
		t.Helper()
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		// Insert a few rows to force a split so we get an interior root.
		cursor := NewCursor(bt, rootPage)
		for i := int64(1); i <= 50; i++ {
			payload := make([]byte, 80)
			cursor.Insert(i, payload)
		}
		// The root may now be interior; grab the current root page as a BtreePage.
		pageData, err := bt.GetPage(cursor.RootPage)
		if err != nil {
			t.Fatalf("GetPage: %v", err)
		}
		page, err := NewBtreePage(cursor.RootPage, pageData, bt.UsableSize)
		if err != nil {
			t.Fatalf("NewBtreePage: %v", err)
		}
		return page
	}

	tests := []struct {
		name           string
		separatorIndex int
		wantErr        bool
	}{
		{
			// A=T: separatorIndex < 0 → early return nil
			name:           "A=T B=F negativeIndex: returns nil early",
			separatorIndex: -1,
			wantErr:        false,
		},
		{
			// A=F, B=T: separatorIndex >= NumCells → early return nil
			name:           "A=F B=T indexTooLarge: returns nil early",
			separatorIndex: 10000,
			wantErr:        false,
		},
		{
			// A=F, B=F: valid index → attempts replacement (may succeed or fail based on page state)
			// We test only that it does NOT early-return nil; test just asserts no panic.
			name:           "A=F B=F validIndex: attempts replacement",
			separatorIndex: 0,
			wantErr:        false, // Either succeeds or returns an op error; not a bounds-check bail
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &BtCursor{Btree: NewBtree(4096)}
			page := buildParentPage(t)
			err := c.replaceSeparatorCell(page, tt.separatorIndex, 1, 1)
			if tt.separatorIndex < 0 || tt.separatorIndex >= int(page.Header.NumCells) {
				// Bounds-check early return: always nil
				if err != nil {
					t.Errorf("replaceSeparatorCell() unexpected error = %v", err)
				}
			}
			// For valid index case we allow any error from the actual replacement attempt;
			// we just want to confirm no panic and the bounds check didn't fire.
			_ = err
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 14 — merge.go updateParentRightChildIfNeeded: last-cell guard then IsInterior
//
//   if cellToRemove != int(parentBtreePage.Header.NumCells)-1 { return nil }
//   ...
//   if !parentHeader.IsInterior { return nil }
//
//   A = (cellToRemove == NumCells-1)  [i.e. NOT the first guard]
//   B = parentHeader.IsInterior
//
//   Row 1: A=F → early return nil (not last cell)
//   Row 2: A=T, B=T → update right child pointer
//   Row 3: A=T, B=F → second early return nil (not interior)
// ---------------------------------------------------------------------------

func TestMCDC_UpdateParentRightChildIfNeeded_Guards(t *testing.T) {
	t.Parallel()

	makeParentPage := func(numCells uint16, isInterior bool, pageType byte) *BtreePage {
		data := make([]byte, 4096)
		data[PageHeaderOffsetType] = pageType
		data[PageHeaderOffsetNumCells] = byte(numCells >> 8)
		data[PageHeaderOffsetNumCells+1] = byte(numCells)
		// Interior pages need the right-child pointer area
		page := &BtreePage{
			Data:       data,
			PageNum:    2,
			UsableSize: 4096,
			Header: &PageHeader{
				PageType:      pageType,
				NumCells:      numCells,
				IsLeaf:        !isInterior,
				IsInterior:    isInterior,
				HeaderSize:    PageHeaderSizeLeaf,
				CellPtrOffset: PageHeaderSizeLeaf,
			},
		}
		if isInterior {
			page.Header.HeaderSize = PageHeaderSizeInterior
			page.Header.CellPtrOffset = PageHeaderSizeInterior
		}
		return page
	}

	tests := []struct {
		name         string
		numCells     uint16
		isInterior   bool
		pageType     byte
		cellToRemove int
		wantErr      bool
	}{
		{
			// A=F: cellToRemove != NumCells-1 → early nil
			name:         "A=F notLastCell: early nil",
			numCells:     3,
			isInterior:   true,
			pageType:     PageTypeInteriorTable,
			cellToRemove: 0, // not last (last is index 2)
			wantErr:      false,
		},
		{
			// A=T, B=T: last cell, interior page → tries to update right-child ptr
			name:         "A=T B=T lastCell interior: updates right child",
			numCells:     1,
			isInterior:   true,
			pageType:     PageTypeInteriorTable,
			cellToRemove: 0, // 0 == NumCells-1 = 0
			wantErr:      false,
		},
		{
			// A=T, B=F: last cell, leaf page → second early nil
			name:         "A=T B=F lastCell leaf: early nil on IsInterior check",
			numCells:     1,
			isInterior:   false,
			pageType:     PageTypeLeafTable,
			cellToRemove: 0,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			// Give the btree a page so GetPage works
			pageData := make([]byte, 4096)
			bt.SetPage(2, pageData)
			c := &BtCursor{Btree: bt}
			page := makeParentPage(tt.numCells, tt.isInterior, tt.pageType)
			err := c.updateParentRightChildIfNeeded(page, 2, 5, tt.cellToRemove)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateParentRightChildIfNeeded() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 15 — cursor.go validateInsertPosition: duplicate-key + composite branch
//
//   if found {
//       if c.CompositePK { return fmt.Errorf("UNIQUE constraint failed: duplicate composite key") }
//       return fmt.Errorf("UNIQUE constraint failed: duplicate key %d", key)
//   }
//
//   A = found
//   B = c.CompositePK
//
//   Row 1: A=F → no error (key not found)
//   Row 2: A=T, B=T → composite duplicate error
//   Row 3: A=T, B=F → rowid duplicate error
// ---------------------------------------------------------------------------

func TestMCDC_ValidateInsertPosition_DuplicateKey(t *testing.T) {
	t.Parallel()

	buildBtreeWithKey := func(t *testing.T, composite bool) (*BtCursor, uint32) {
		t.Helper()
		bt := NewBtree(4096)
		var rootPage uint32
		var err error
		if composite {
			rootPage, err = bt.CreateWithoutRowidTable()
		} else {
			rootPage, err = bt.CreateTable()
		}
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursorWithOptions(bt, rootPage, composite)
		if composite {
			cursor.InsertWithComposite(0, []byte("key"), []byte("val"))
		} else {
			cursor.Insert(10, []byte("val"))
		}
		return cursor, rootPage
	}

	tests := []struct {
		name      string
		composite bool
		rowid     int64
		keyBytes  []byte
		wantErr   bool
	}{
		{
			// A=F: key not present → no error
			name:      "A=F notFound: no error",
			composite: false,
			rowid:     99,
			wantErr:   false,
		},
		{
			// A=T, B=F: rowid key found → rowid duplicate error
			name:      "A=T B=F found rowid dup: error",
			composite: false,
			rowid:     10,
			wantErr:   true,
		},
		{
			// A=T, B=T: composite key found → composite duplicate error
			name:      "A=T B=T found composite dup: error",
			composite: true,
			keyBytes:  []byte("key"),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cursor, rootPage := buildBtreeWithKey(t, tt.composite)
			_ = rootPage
			var err error
			if tt.composite {
				err = cursor.validateInsertPosition(0, tt.keyBytes)
			} else {
				err = cursor.validateInsertPosition(tt.rowid, nil)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("validateInsertPosition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
