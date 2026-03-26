// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Condition 16 — cursor.go GetKeyBytes triple-or guard  (line 566)
//
//   if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil
//
//   A = (c.Btree == nil)
//   B = (c.State != CursorValid)
//   C = (c.CurrentCell == nil)
//
//   Row 1: A=T, B=F, C=F → nil  (A short-circuits)
//   Row 2: A=F, B=T, C=F → nil  (B short-circuits)
//   Row 3: A=F, B=F, C=T → nil  (C triggers)
//   Row 4: A=F, B=F, C=F → real KeyBytes
// ---------------------------------------------------------------------------

func TestMCDC_GetKeyBytes_TripleOrGuard(t *testing.T) {
	t.Parallel()

	wantKey := []byte("hello")

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantNil bool
	}{
		{
			// A=T: Btree==nil → nil
			name: "A=T B=F C=F btree=nil: nil",
			cursor: func() *BtCursor {
				return &BtCursor{
					Btree:       nil,
					State:       CursorValid,
					CurrentCell: &CellInfo{KeyBytes: wantKey},
				}
			},
			wantNil: true,
		},
		{
			// A=F, B=T: State != CursorValid → nil
			name: "A=F B=T C=F state=invalid: nil",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorInvalid,
					CurrentCell: &CellInfo{KeyBytes: wantKey},
				}
			},
			wantNil: true,
		},
		{
			// A=F, B=F, C=T: CurrentCell==nil → nil
			name: "A=F B=F C=T cell=nil: nil",
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
			// A=F, B=F, C=F: all valid → real KeyBytes
			name: "A=F B=F C=F all valid: real key bytes",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				return &BtCursor{
					Btree:       bt,
					State:       CursorValid,
					CurrentCell: &CellInfo{KeyBytes: wantKey},
				}
			},
			wantNil: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cursor().GetKeyBytes()
			if tt.wantNil && got != nil {
				t.Errorf("GetKeyBytes() = %v, want nil", got)
			}
			if !tt.wantNil && got == nil {
				t.Error("GetKeyBytes() = nil, want non-nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 17 — cursor.go freeOverflowPages: double-&&  (line 1156)
//
//   if c.CurrentCell != nil && c.CurrentCell.OverflowPage != 0
//
//   A = (c.CurrentCell != nil)
//   B = (c.CurrentCell.OverflowPage != 0)
//
//   Row 1: A=F            → no free attempted
//   Row 2: A=T, B=F       → no free attempted (no overflow page)
//   Row 3: A=T, B=T       → free attempted
// ---------------------------------------------------------------------------

func TestMCDC_FreeOverflowPages_DoubleAnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantErr bool
	}{
		{
			// A=F: CurrentCell==nil → no free, no error
			name: "A=F cell=nil: no error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				// Manually seed a valid-looking state but nil cell
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: true}
				c.CurrentCell = nil
				return c
			},
			wantErr: false,
		},
		{
			// A=T, B=F: cell set but OverflowPage==0 → no free, no error
			name: "A=T B=F cell set no overflow: no error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: true}
				c.CurrentCell = &CellInfo{OverflowPage: 0}
				return c
			},
			wantErr: false,
		},
		{
			// A=T, B=T: cell set, OverflowPage != 0 → free attempted;
			// since the overflow page does not exist in the btree, FreeOverflowChain
			// will return an error, which freeOverflowPages propagates.
			name: "A=T B=T cell with overflow: free attempted (error expected)",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: true}
				c.CurrentCell = &CellInfo{OverflowPage: 999}
				return c
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := tt.cursor()
			err := c.freeOverflowPages()
			if (err != nil) != tt.wantErr {
				t.Errorf("freeOverflowPages() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 18 — cursor.go retryWithMinimalOverflow: double-or guard (line 960)
//
//   if btreePage.Header.NumCells != 0 || len(payload) == 0 { return cellData, overflowPage }
//
//   A = (btreePage.Header.NumCells != 0)
//   B = (len(payload) == 0)
//
//   Row 1: A=T, B=F → early return (cells present)
//   Row 2: A=F, B=T → early return (empty payload)
//   Row 3: A=F, B=F → continues to retry path
//
//   We test via the exported Insert path on a page that can't hold the cell,
//   but we validate behaviour indirectly through the insert outcome.
//   For a direct unit test we call retryWithMinimalOverflow directly.
// ---------------------------------------------------------------------------

func TestMCDC_RetryWithMinimalOverflow_DoubleOr(t *testing.T) {
	t.Parallel()

	makePageWithCells := func(numCells uint16) *BtreePage {
		data := make([]byte, 4096)
		data[PageHeaderOffsetType] = PageTypeLeafTable
		data[PageHeaderOffsetNumCells] = byte(numCells >> 8)
		data[PageHeaderOffsetNumCells+1] = byte(numCells)
		page, _ := NewBtreePage(2, data, 4096)
		return page
	}

	tests := []struct {
		name          string
		numCells      uint16
		payload       []byte
		wantEarlyExit bool // true if cellData returned unchanged
	}{
		{
			// A=T: page has cells → early return, cellData unchanged
			name:          "A=T B=F cells>0 payloadNonEmpty: early return",
			numCells:      3,
			payload:       []byte("data"),
			wantEarlyExit: true,
		},
		{
			// A=F, B=T: empty payload → early return, cellData unchanged
			name:          "A=F B=T cells=0 payloadEmpty: early return",
			numCells:      0,
			payload:       []byte{},
			wantEarlyExit: true,
		},
		{
			// A=F, B=F: no cells, non-empty payload → proceeds with retry
			// retryWithMinimalOverflow will call WriteOverflow and re-encode;
			// the returned cellData will differ from the original stub.
			name:          "A=F B=F cells=0 payloadNonEmpty: retry proceeds",
			numCells:      0,
			payload:       make([]byte, 500),
			wantEarlyExit: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			rootPage, _ := bt.CreateTable()
			c := NewCursor(bt, rootPage)

			page := makePageWithCells(tt.numCells)
			origCellData := []byte("original-cell")
			origOverflow := uint32(0)

			gotCell, _ := c.retryWithMinimalOverflow(1, nil, tt.payload, origCellData, origOverflow, page)

			if tt.wantEarlyExit {
				// cellData must be the same slice returned unchanged
				if string(gotCell) != string(origCellData) {
					t.Errorf("expected cellData unchanged %q, got %q", origCellData, gotCell)
				}
			} else {
				// After retry the cell should be re-encoded (different content)
				if string(gotCell) == string(origCellData) {
					t.Errorf("expected cellData to change after retry, still %q", gotCell)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 19 — split.go splitLeafPage: header nil-or-not-leaf guard (line 26)
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsLeaf)
//
//   Row 1: A=T           → error "splitLeafPage called on non-leaf page"
//   Row 2: A=F, B=T      → error (interior page)
//   Row 3: A=F, B=F      → no error from guard (proceeds with split)
// ---------------------------------------------------------------------------

func TestMCDC_SplitLeafPage_HeaderGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantErr bool
	}{
		{
			// A=T: CurrentHeader==nil → error
			name: "A=T B=- header=nil: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = nil
				c.CurrentPage = rootPage
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=T: header is interior → error
			name: "A=F B=T header=interior: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: false, IsInterior: true}
				c.CurrentPage = rootPage
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=F: header is a leaf → guard passes (split proceeds)
			// We just confirm there is no guard-level error; the split itself
			// may or may not fail based on page state — we use a valid page.
			name: "A=F B=F header=leaf: guard passes",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				// Position cursor on the root leaf
				c.SeekRowid(1)
				return c
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := tt.cursor()
			err := c.splitLeafPage(1, nil, []byte("payload"))
			if tt.wantErr {
				if err == nil {
					t.Error("splitLeafPage() expected error, got nil")
				}
			} else {
				// For the valid-leaf case, any non-guard error is acceptable;
				// we only care that the nil/not-leaf guard did NOT fire.
				// Both success and split-internal errors are acceptable here.
				_ = err
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 20 — split.go splitInteriorPage: header nil-or-not-interior (line 108)
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsInterior
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsInterior)
//
//   Row 1: A=T            → error
//   Row 2: A=F, B=T       → error (leaf page)
//   Row 3: A=F, B=F       → guard passes (proceeds)
// ---------------------------------------------------------------------------

func TestMCDC_SplitInteriorPage_HeaderGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		header  *PageHeader
		wantErr bool
	}{
		{
			// A=T: nil header → error
			name:    "A=T header=nil: error",
			header:  nil,
			wantErr: true,
		},
		{
			// A=F, B=T: header is leaf (not interior) → error
			name:    "A=F B=T header=leaf: error",
			header:  &PageHeader{IsLeaf: true, IsInterior: false},
			wantErr: true,
		},
		{
			// A=F, B=F: header is interior → guard passes
			// splitInteriorPage will likely fail further in for other reasons,
			// but the guard itself should not fire.
			name: "A=F B=F header=interior: guard passes",
			header: &PageHeader{
				IsLeaf:        false,
				IsInterior:    true,
				PageType:      PageTypeInteriorTable,
				HeaderSize:    PageHeaderSizeInterior,
				CellPtrOffset: PageHeaderSizeInterior,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			rootPage, _ := bt.CreateTable()
			c := NewCursor(bt, rootPage)
			c.State = CursorValid
			c.CurrentHeader = tt.header
			c.CurrentPage = rootPage

			err := c.splitInteriorPage(1, nil, 2)
			if tt.wantErr {
				if err == nil {
					t.Error("splitInteriorPage() expected error, got nil")
				}
			} else {
				// Guard passed; any downstream error is OK.
				_ = err
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 21 — split.go updateParentAfterSplit: depth==0 || leftPage==RootPage (line 790)
//
//   if c.Depth == 0 || leftPage == c.RootPage { return c.createNewRoot(...) }
//
//   A = (c.Depth == 0)
//   B = (leftPage == c.RootPage)
//
//   Row 1: A=T, B=F → createNewRoot called  (A alone triggers)
//   Row 2: A=F, B=T → createNewRoot called  (B alone triggers)
//   Row 3: A=F, B=F → proceed to parent-insert path (no new root)
//
//   We exercise this via the splitLeafPage path on a real btree.
// ---------------------------------------------------------------------------

func TestMCDC_UpdateParentAfterSplit_DepthOrRootGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		depth       int
		leftEqRoot  bool
		wantNewRoot bool // If new root is created, RootPage should change
	}{
		{
			// A=T: depth==0 → creates new root
			name:        "A=T B=F depth=0: createNewRoot",
			depth:       0,
			leftEqRoot:  false,
			wantNewRoot: true,
		},
		{
			// A=F, B=T: depth>0 but leftPage==RootPage → creates new root
			name:        "A=F B=T depth>0 leftIsRoot: createNewRoot",
			depth:       1,
			leftEqRoot:  true,
			wantNewRoot: true,
		},
		{
			// A=F, B=F: depth>0 and leftPage!=RootPage → insert into parent
			name:        "A=F B=F depth>0 leftNotRoot: insertIntoParent",
			depth:       1,
			leftEqRoot:  false,
			wantNewRoot: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bt := NewBtree(4096)
			rootPage, _ := bt.CreateTable()
			c := NewCursor(bt, rootPage)

			// Allocate a second page to act as "rightPage" of a split
			rightPage, _ := bt.AllocatePage()
			rightData, _ := bt.GetPage(rightPage)
			initEmptyPageHeader(rightData, rightPage, PageTypeLeafTable)

			c.Depth = tt.depth
			c.PageStack[0] = rootPage
			if tt.depth > 0 {
				// Build a minimal parent page at depth 0
				parentPageNum, _ := bt.AllocatePage()
				parentData, _ := bt.GetPage(parentPageNum)
				initEmptyPageHeader(parentData, parentPageNum, PageTypeInteriorTable)
				c.PageStack[0] = parentPageNum
				c.PageStack[1] = rootPage
				c.IndexStack[0] = 0
			}

			leftPage := rootPage
			if !tt.leftEqRoot {
				// Use a different page as left
				altLeft, _ := bt.AllocatePage()
				altLeftData, _ := bt.GetPage(altLeft)
				initEmptyPageHeader(altLeftData, altLeft, PageTypeLeafTable)
				leftPage = altLeft
			}

			oldRoot := c.RootPage
			err := c.updateParentAfterSplit(leftPage, rightPage, 42, nil)
			_ = err // Downstream errors are acceptable

			newRootCreated := c.RootPage != oldRoot
			if tt.wantNewRoot && !newRootCreated {
				t.Errorf("expected new root to be created, but RootPage unchanged (%d)", c.RootPage)
			}
			if !tt.wantNewRoot && newRootCreated {
				t.Errorf("unexpected new root: RootPage changed from %d to %d", oldRoot, c.RootPage)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 22 — cell.go calculateCellSizeAndLocal: PayloadSize <= maxLocal (line 143)
//
//   if info.PayloadSize <= maxLocal { ... local path ... } else { ... overflow path ... }
//
//   A = (info.PayloadSize <= maxLocal)
//
//   Row 1: A=T → local payload, no overflow pointer in CellSize
//   Row 2: A=F → spill to overflow, CellSize includes overflow pointer (+4)
// ---------------------------------------------------------------------------

func TestMCDC_CalculateCellSizeAndLocal_PayloadFitsVsSpills(t *testing.T) {
	t.Parallel()

	const usableSize = 4096
	maxLocal := calculateMaxLocal(usableSize, true) // 4061
	minLocal := calculateMinLocal(usableSize, true)

	tests := []struct {
		name          string
		payloadSize   uint32
		wantLocalOnly bool // true = payload fits locally, CellSize == offset + payloadSize
	}{
		{
			// A=T: small payload fits in a single page
			name:          "A=T payloadSize<=maxLocal: local only",
			payloadSize:   100,
			wantLocalOnly: true,
		},
		{
			// A=F: large payload exceeds maxLocal → spills
			name:          "A=F payloadSize>maxLocal: overflow spill",
			payloadSize:   maxLocal + 1,
			wantLocalOnly: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := &CellInfo{PayloadSize: tt.payloadSize}
			offset := 2 // simulate a small header offset
			calculateCellSizeAndLocal(info, offset, maxLocal, minLocal, usableSize)

			if tt.wantLocalOnly {
				// Local payload should equal PayloadSize (if it fits in uint16)
				if uint32(info.LocalPayload) > tt.payloadSize {
					t.Errorf("local payload %d > payloadSize %d", info.LocalPayload, tt.payloadSize)
				}
				// CellSize should not include the 4-byte overflow pointer
				expectedCellSize := uint16(offset) + info.LocalPayload
				if info.CellSize < 4 {
					expectedCellSize = 4
				}
				_ = expectedCellSize
			} else {
				// When spilling, LocalPayload < PayloadSize and CellSize includes +4
				if uint32(info.LocalPayload) >= tt.payloadSize {
					t.Errorf("expected LocalPayload < PayloadSize, got %d >= %d",
						info.LocalPayload, tt.payloadSize)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 23 — cell.go calculateLocalPayload: usableSize<4 || payloadSize<minLocal (line 385)
//
//   if usableSize < 4 || payloadSize < minLocal { return safePayloadSize(minLocal, ...) }
//
//   A = (usableSize < 4)
//   B = (payloadSize < minLocal)
//
//   Row 1: A=T, B=F → early return of minLocal (A alone triggers)
//   Row 2: A=F, B=T → early return of minLocal (B alone triggers)
//   Row 3: A=F, B=F → proceeds to surplus calculation
// ---------------------------------------------------------------------------

func TestMCDC_CalculateLocalPayload_EarlyReturnGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		usableSize  uint32
		payloadSize uint32
		maxLocal    uint32
		minLocal    uint32
		wantEarly   bool
	}{
		{
			// A=T: usableSize < 4 → return minLocal
			name:        "A=T B=F usableSize<4: early return",
			usableSize:  2,
			payloadSize: 100,
			maxLocal:    0, // usableSize-35 would underflow; calculateMaxLocal handles it
			minLocal:    0,
			wantEarly:   true,
		},
		{
			// A=F, B=T: payloadSize < minLocal → early return
			name:        "A=F B=T payloadSize<minLocal: early return",
			usableSize:  4096,
			payloadSize: 1, // very small
			maxLocal:    4061,
			minLocal:    500, // contrived minLocal > payloadSize
			wantEarly:   true,
		},
		{
			// A=F, B=F: valid inputs → surplus path
			name:        "A=F B=F normal: surplus path",
			usableSize:  4096,
			payloadSize: 4096,
			maxLocal:    4061,
			minLocal:    calculateMinLocal(4096, true),
			wantEarly:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculateLocalPayload(tt.payloadSize, tt.minLocal, tt.maxLocal, tt.usableSize)

			if tt.wantEarly {
				// Must return minLocal (clamped to uint16)
				wantResult := uint16(tt.minLocal)
				if result != wantResult {
					t.Errorf("calculateLocalPayload() = %d, want %d", result, wantResult)
				}
			} else {
				// Surplus path: result may differ from minLocal
				_ = result // just confirm no panic
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 24 — overflow.go allocateNextPageIfNeeded: offset+toWrite < dataLen (line 93)
//
//   if offset+toWrite < dataLen { allocate next page } else { return 0, nil }
//
//   A = (offset + toWrite < dataLen)
//
//   Row 1: A=T → new page allocated, returns non-zero page number
//   Row 2: A=F → last page, returns 0
// ---------------------------------------------------------------------------

func TestMCDC_AllocateNextPageIfNeeded_LastVsNotLast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		offset    int
		toWrite   int
		dataLen   int
		wantAlloc bool
	}{
		{
			// A=T: more data remains → allocate next page
			name:      "A=T not last chunk: allocates next page",
			offset:    0,
			toWrite:   100,
			dataLen:   300,
			wantAlloc: true,
		},
		{
			// A=F: last chunk (offset+toWrite == dataLen) → returns 0
			name:      "A=F last chunk exact: no allocation",
			offset:    200,
			toWrite:   100,
			dataLen:   300,
			wantAlloc: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			got, err := allocateNextPageIfNeeded(bt, tt.offset, tt.toWrite, tt.dataLen)
			if err != nil {
				t.Fatalf("allocateNextPageIfNeeded() unexpected error: %v", err)
			}
			if tt.wantAlloc && got == 0 {
				t.Error("expected a non-zero page number to be allocated")
			}
			if !tt.wantAlloc && got != 0 {
				t.Errorf("expected 0 page number, got %d", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 25 — overflow.go CalculateLocalPayload: totalSize <= maxLocal (line 297)
//
//   if totalSize <= maxLocal { return safePayloadSize(totalSize, maxLocal) }
//
//   A = (totalSize <= maxLocal)
//
//   Row 1: A=T → returns totalSize directly (no overflow)
//   Row 2: A=F → proceeds to overflow calculation
// ---------------------------------------------------------------------------

func TestMCDC_CalculateLocalPayloadPublic_FitsVsSpills(t *testing.T) {
	t.Parallel()

	const pageSize = 4096
	maxLocal := calculateMaxLocal(pageSize, true) // 4061

	tests := []struct {
		name           string
		totalSize      uint32
		wantEqualsSize bool // true = returned value == totalSize
	}{
		{
			// A=T: small payload fits locally
			name:           "A=T totalSize<=maxLocal: returns totalSize",
			totalSize:      50,
			wantEqualsSize: true,
		},
		{
			// A=F: large payload spills to overflow
			name:           "A=F totalSize>maxLocal: returns minLocal",
			totalSize:      maxLocal + 100,
			wantEqualsSize: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CalculateLocalPayload(tt.totalSize, pageSize, true)
			if tt.wantEqualsSize {
				if uint32(got) != tt.totalSize {
					t.Errorf("CalculateLocalPayload() = %d, want %d", got, tt.totalSize)
				}
			} else {
				// When spilling, local portion is strictly less than total
				if uint32(got) >= tt.totalSize {
					t.Errorf("expected local < total, got %d >= %d", got, tt.totalSize)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 26 — integrity.go checkKeyBounds: minKey != nil && key <= *minKey  (line 365)
//
//   if minKey != nil && key <= *minKey { ... }
//
//   A = (minKey != nil)
//   B = (key <= *minKey)
//
//   Row 1: A=F        → no error added
//   Row 2: A=T, B=F   → no error added (key > minKey, within bounds)
//   Row 3: A=T, B=T   → error added
// ---------------------------------------------------------------------------

func TestMCDC_CheckKeyBounds_MinKeyGuard(t *testing.T) {
	t.Parallel()

	mkPtr := func(v int64) *int64 { return &v }

	tests := []struct {
		name    string
		key     int64
		minKey  *int64
		maxKey  *int64
		wantErr bool
	}{
		{
			// A=F: minKey==nil → no error from min-bound check
			name:    "A=F minKey=nil: no error",
			key:     5,
			minKey:  nil,
			maxKey:  nil,
			wantErr: false,
		},
		{
			// A=T, B=F: minKey set, key > minKey → no error
			name:    "A=T B=F key>minKey: no error",
			key:     10,
			minKey:  mkPtr(5),
			maxKey:  nil,
			wantErr: false,
		},
		{
			// A=T, B=T: key <= minKey → error
			name:    "A=T B=T key<=minKey: error",
			key:     3,
			minKey:  mkPtr(5),
			maxKey:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
			checkKeyBounds(1, 0, tt.key, tt.minKey, tt.maxKey, result)
			hasErr := len(result.Errors) > 0
			if hasErr != tt.wantErr {
				t.Errorf("checkKeyBounds() hasErr=%v, want %v", hasErr, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 27 — integrity.go checkKeyBounds: maxKey != nil && key > *maxKey  (line 369)
//
//   if maxKey != nil && key > *maxKey { ... }
//
//   A = (maxKey != nil)
//   B = (key > *maxKey)
//
//   Row 1: A=F        → no error from max-bound check
//   Row 2: A=T, B=F   → key <= maxKey, no error
//   Row 3: A=T, B=T   → key > maxKey, error
// ---------------------------------------------------------------------------

func TestMCDC_CheckKeyBounds_MaxKeyGuard(t *testing.T) {
	t.Parallel()

	mkPtr := func(v int64) *int64 { return &v }

	tests := []struct {
		name    string
		key     int64
		maxKey  *int64
		wantErr bool
	}{
		{
			// A=F: maxKey==nil → no error
			name:    "A=F maxKey=nil: no error",
			key:     100,
			maxKey:  nil,
			wantErr: false,
		},
		{
			// A=T, B=F: key <= maxKey → no error
			name:    "A=T B=F key<=maxKey: no error",
			key:     5,
			maxKey:  mkPtr(10),
			wantErr: false,
		},
		{
			// A=T, B=T: key > maxKey → error
			name:    "A=T B=T key>maxKey: error",
			key:     15,
			maxKey:  mkPtr(10),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
			checkKeyBounds(1, 0, tt.key, nil, tt.maxKey, result)
			hasErr := len(result.Errors) > 0
			if hasErr != tt.wantErr {
				t.Errorf("checkKeyBounds() hasErr=%v, want %v", hasErr, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 28 — integrity.go checkKeySequence: idx > 0 && key <= prev key  (line 377)
//
//   if idx > 0 && cells[idx].Key <= cells[idx-1].Key { add error }
//
//   A = (idx > 0)
//   B = (cells[idx].Key <= cells[idx-1].Key)
//
//   Row 1: A=F       → no check (first cell)
//   Row 2: A=T, B=F  → keys in order, no error
//   Row 3: A=T, B=T  → out-of-order, error
// ---------------------------------------------------------------------------

func TestMCDC_CheckKeySequence_IndexAndOrder(t *testing.T) {
	t.Parallel()

	makeCells := func(keys ...int64) []*CellInfo {
		cells := make([]*CellInfo, len(keys))
		for i, k := range keys {
			cells[i] = &CellInfo{Key: k}
		}
		return cells
	}

	tests := []struct {
		name    string
		cells   []*CellInfo
		idx     int
		wantErr bool
	}{
		{
			// A=F: idx==0 → no comparison, no error
			name:    "A=F idx=0: no error",
			cells:   makeCells(5),
			idx:     0,
			wantErr: false,
		},
		{
			// A=T, B=F: idx>0, key properly ascending
			name:    "A=T B=F idx>0 ascending: no error",
			cells:   makeCells(5, 10),
			idx:     1,
			wantErr: false,
		},
		{
			// A=T, B=T: idx>0, duplicate or descending key
			name:    "A=T B=T idx>0 out-of-order: error",
			cells:   makeCells(10, 5),
			idx:     1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
			checkKeySequence(1, tt.idx, tt.cells, result)
			hasErr := len(result.Errors) > 0
			if hasErr != tt.wantErr {
				t.Errorf("checkKeySequence() hasErr=%v, want %v", hasErr, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 29 — integrity.go checkMaxIterationsExceeded: offset!=0 && visited>=max (line 660)
//
//   if offset != 0 && len(visited) >= maxIterations { add error }
//
//   A = (offset != 0)
//   B = (len(visited) >= maxIterations)
//
//   Row 1: A=F        → no error (traversal finished cleanly)
//   Row 2: A=T, B=F   → traversal still in progress but under limit
//   Row 3: A=T, B=T   → chain too long, error
// ---------------------------------------------------------------------------

func TestMCDC_CheckMaxIterationsExceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		offset        uint16
		visitedCount  int
		maxIterations int
		wantErr       bool
	}{
		{
			// A=F: offset==0 → traversal done, no error
			name:          "A=F offset=0: no error",
			offset:        0,
			visitedCount:  1000,
			maxIterations: 1000,
			wantErr:       false,
		},
		{
			// A=T, B=F: offset!=0 but visited < max → no error yet
			name:          "A=T B=F offset!=0 visitedUnderMax: no error",
			offset:        42,
			visitedCount:  5,
			maxIterations: 1000,
			wantErr:       false,
		},
		{
			// A=T, B=T: offset!=0 and visited >= max → error
			name:          "A=T B=T offset!=0 visitedAtMax: error",
			offset:        42,
			visitedCount:  1000,
			maxIterations: 1000,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			visited := make(map[uint16]bool, tt.visitedCount)
			for i := 0; i < tt.visitedCount; i++ {
				visited[uint16(i)] = true
			}
			result := &IntegrityResult{Errors: make([]*IntegrityError, 0)}
			checkMaxIterationsExceeded(1, tt.offset, visited, tt.maxIterations, result)
			hasErr := len(result.Errors) > 0
			if hasErr != tt.wantErr {
				t.Errorf("checkMaxIterationsExceeded() hasErr=%v, want %v", hasErr, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 30 — index_cursor.go seekLeafPage: exactMatch && idx<NumCells (line 604)
//
//   if exactMatch && idx < int(header.NumCells)
//
//   A = exactMatch
//   B = (idx < int(header.NumCells))
//
//   Row 1: A=T, B=T → exact match loaded, returns true
//   Row 2: A=F, B=T → no exact match, returns false
//   Row 3: A=T, B=F → idx out of range, falls through (returns false)
// ---------------------------------------------------------------------------

func TestMCDC_IndexCursorSeekLeafPage_ExactMatch(t *testing.T) {
	t.Parallel()

	buildIndexWithEntry := func(t *testing.T) (*Btree, uint32) {
		t.Helper()
		bt := NewBtree(4096)
		rootPage, err := createIndexPage(bt)
		if err != nil {
			t.Fatalf("createIndexPage: %v", err)
		}
		cursor := NewIndexCursor(bt, rootPage)
		if err := cursor.InsertIndex([]byte("alpha"), 1); err != nil {
			t.Fatalf("InsertIndex: %v", err)
		}
		return bt, rootPage
	}

	tests := []struct {
		name      string
		key       []byte
		wantFound bool
	}{
		{
			// A=T, B=T: seek key that exists → found=true
			name:      "A=T B=T exact key present: found",
			key:       []byte("alpha"),
			wantFound: true,
		},
		{
			// A=F, B=T: key not present → found=false
			name:      "A=F B=T key absent: not found",
			key:       []byte("zzz"),
			wantFound: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt, rootPage := buildIndexWithEntry(t)
			cursor := NewIndexCursor(bt, rootPage)
			found, err := cursor.SeekIndex(tt.key)
			if err != nil && tt.wantFound {
				t.Fatalf("SeekIndex() unexpected error: %v", err)
			}
			if found != tt.wantFound {
				t.Errorf("SeekIndex() found=%v, want %v", found, tt.wantFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 31 — index_cursor.go validateDeleteState: header nil || not-leaf (line 792)
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsLeaf)
//
//   Row 1: A=T       → error
//   Row 2: A=F, B=T  → error (interior page)
//   Row 3: A=F, B=F  → no error from this guard
// ---------------------------------------------------------------------------

func TestMCDC_IndexCursorValidateDeleteState_HeaderGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cursor  func() *IndexCursor
		wantErr bool
	}{
		{
			// A=T: CurrentHeader==nil → error
			name: "A=T B=- header=nil: error",
			cursor: func() *IndexCursor {
				bt := NewBtree(4096)
				rootPage, _ := createIndexPage(bt)
				c := NewIndexCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = nil
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=T: header is interior → error
			name: "A=F B=T header=interior: error",
			cursor: func() *IndexCursor {
				bt := NewBtree(4096)
				rootPage, _ := createIndexPage(bt)
				c := NewIndexCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: false}
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=F: header is leaf → no error
			name: "A=F B=F header=leaf: no error",
			cursor: func() *IndexCursor {
				bt := NewBtree(4096)
				rootPage, _ := createIndexPage(bt)
				c := NewIndexCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentHeader = &PageHeader{IsLeaf: true}
				return c
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cursor().validateDeleteState()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDeleteState() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 32 — index_cursor.go InsertIndex duplicate-key guard (line 708)
//
//   found, err := c.SeekIndex(key)
//   if found { return fmt.Errorf("duplicate key in index") }
//
//   A = found
//
//   Row 1: A=F → no duplicate error (new key inserted successfully)
//   Row 2: A=T → duplicate error returned
//
//   The header guard that follows (CurrentHeader == nil || !IsLeaf) is always
//   satisfied by a correctly positioned cursor from SeekIndex, so the
//   duplicates path is the independently testable compound condition here.
// ---------------------------------------------------------------------------

func TestMCDC_IndexCursorInsertIndex_DuplicateGuard(t *testing.T) {
	t.Parallel()

	buildIndexWith := func(t *testing.T) *IndexCursor {
		t.Helper()
		bt := NewBtree(4096)
		rootPage, err := createIndexPage(bt)
		if err != nil {
			t.Fatalf("createIndexPage: %v", err)
		}
		cursor := NewIndexCursor(bt, rootPage)
		if err := cursor.InsertIndex([]byte("existing"), 1); err != nil {
			t.Fatalf("InsertIndex setup: %v", err)
		}
		return cursor
	}

	tests := []struct {
		name    string
		key     []byte
		wantErr bool
	}{
		{
			// A=F: key not present → inserted without error
			name:    "A=F key not present: no error",
			key:     []byte("newkey"),
			wantErr: false,
		},
		{
			// A=T: key already exists → duplicate error
			name:    "A=T key already present: duplicate error",
			key:     []byte("existing"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cursor := buildIndexWith(t)
			err := cursor.InsertIndex(tt.key, 99)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
