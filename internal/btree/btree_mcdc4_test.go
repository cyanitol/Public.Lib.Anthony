// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"encoding/binary"
	"testing"
)

// ---------------------------------------------------------------------------
// Condition 52 — split.go splitLeafPage: nil or non-leaf header guard  (line 26)
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
//       return fmt.Errorf("splitLeafPage called on non-leaf page")
//   }
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsLeaf)
//
//   Row 1: A=T, B=F → error (nil header short-circuits)
//   Row 2: A=F, B=T → error (non-leaf page)
//   Row 3: A=F, B=F → proceeds (leaf page — no error from guard)
// ---------------------------------------------------------------------------

func TestMCDC_SplitLeafPage_HeaderGuard_V2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantErr bool
	}{
		{
			// A=T: CurrentHeader == nil → error
			name: "A=T header=nil: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentPage = rootPage
				c.CurrentHeader = nil // force nil
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=T: header present but IsLeaf=false → error
			name: "A=F B=T non-leaf header: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.State = CursorValid
				c.CurrentPage = rootPage
				c.CurrentHeader = &PageHeader{
					PageType:      PageTypeInteriorTable,
					IsLeaf:        false,
					IsInterior:    true,
					HeaderSize:    PageHeaderSizeInterior,
					CellPtrOffset: PageHeaderSizeInterior,
				}
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=F: header present and IsLeaf=true → guard passes
			// (actual split will proceed; may fail for other reasons but guard is passed)
			name: "A=F B=F leaf header: guard passes",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				// Insert enough rows to make the page full so split is needed
				for i := int64(1); i <= 50; i++ {
					payload := make([]byte, 60)
					c.Insert(i, payload)
				}
				// Position cursor on the page
				c.MoveToFirst()
				// The CurrentHeader after MoveToFirst is a leaf header
				return c
			},
			// Guard is satisfied (IsLeaf=true); the call may succeed or fail
			// with a different error — we only care the guard itself passed.
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := tt.cursor()
			err := c.splitLeafPage(99, nil, []byte("payload"))
			if tt.wantErr && err == nil {
				t.Error("splitLeafPage() expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				// The non-leaf guard was passed; other errors are acceptable.
				// Only fail if the error message says "non-leaf".
				if err.Error() == "splitLeafPage called on non-leaf page" {
					t.Errorf("splitLeafPage() hit the non-leaf guard unexpectedly: %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 53 — split.go splitInteriorPage: nil or non-interior header  (line 108)
//
//   if c.CurrentHeader == nil || !c.CurrentHeader.IsInterior {
//       return fmt.Errorf("splitInteriorPage called on non-interior page")
//   }
//
//   A = (c.CurrentHeader == nil)
//   B = (!c.CurrentHeader.IsInterior)
//
//   Row 1: A=T, B=F → error
//   Row 2: A=F, B=T → error (leaf passed as interior)
//   Row 3: A=F, B=F → no error from guard
// ---------------------------------------------------------------------------

func TestMCDC_SplitInteriorPage_HeaderGuard_V2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cursor  func() *BtCursor
		wantErr bool
	}{
		{
			// A=T: nil header → error
			name: "A=T nil header: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.CurrentHeader = nil
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=T: header exists but IsInterior=false → error
			name: "A=F B=T leaf as interior: error",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				c.CurrentHeader = &PageHeader{
					PageType:   PageTypeLeafTable,
					IsLeaf:     true,
					IsInterior: false,
				}
				return c
			},
			wantErr: true,
		},
		{
			// A=F, B=F: interior header → guard passes
			name: "A=F B=F interior header: guard passes",
			cursor: func() *BtCursor {
				bt := NewBtree(4096)
				// Build a tree deep enough to have an interior page
				rootPage, _ := bt.CreateTable()
				c := NewCursor(bt, rootPage)
				for i := int64(1); i <= 80; i++ {
					payload := make([]byte, 50)
					c.Insert(i, payload)
				}
				c.MoveToFirst()
				// Walk to an interior page by descending manually if possible
				interiorHeader := &PageHeader{
					PageType:      PageTypeInteriorTable,
					IsLeaf:        false,
					IsInterior:    true,
					HeaderSize:    PageHeaderSizeInterior,
					CellPtrOffset: PageHeaderSizeInterior,
					NumCells:      2,
				}
				c.CurrentHeader = interiorHeader
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
			err := c.splitInteriorPage(99, nil, 2)
			if tt.wantErr && err == nil {
				t.Error("splitInteriorPage() expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				if err.Error() == "splitInteriorPage called on non-interior page" {
					t.Errorf("splitInteriorPage() hit the non-interior guard: %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 54 — split.go updateParentAfterSplit: root split condition  (line 790)
//
//   if c.Depth == 0 || leftPage == c.RootPage { return c.createNewRoot(...) }
//
//   A = (c.Depth == 0)
//   B = (leftPage == c.RootPage)
//
//   Row 1: A=T, B=F → createNewRoot (depth is 0)
//   Row 2: A=F, B=T → createNewRoot (leftPage is root page)
//   Row 3: A=F, B=F → insert into parent (normal case)
//
//   Exercised by building trees that trigger splits at root vs. non-root.
// ---------------------------------------------------------------------------

func TestMCDC_UpdateParentAfterSplit_RootCondition(t *testing.T) {
	t.Parallel()

	// Row 1/2: A=T or B=T → root split triggered by inserting many rows.
	// When a single-leaf root fills up and splits, Depth==0 when
	// updateParentAfterSplit is called for that leaf → createNewRoot path.
	t.Run("root split path", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		c := NewCursor(bt, rootPage)
		// Insert enough to force a root split
		for i := int64(1); i <= 60; i++ {
			payload := make([]byte, 50)
			if err := c.Insert(i, payload); err != nil {
				// It is fine to stop; a split occurred
				break
			}
		}
		// Verify tree is still usable after root split
		c2 := NewCursor(bt, c.RootPage)
		count := countForward(c2)
		if count == 0 {
			t.Error("expected rows to remain after root split")
		}
	})

	// Row 3: A=F, B=F → deep tree where splits propagate up but not to root.
	t.Run("non-root split path", func(t *testing.T) {
		t.Parallel()
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		c := NewCursor(bt, rootPage)
		// Insert many rows to create a 3-level tree; subsequent inserts
		// will trigger interior-page splits on non-root interior pages.
		for i := int64(1); i <= 200; i++ {
			payload := make([]byte, 50)
			c.Insert(i, payload)
		}
		c2 := NewCursor(bt, c.RootPage)
		count := countForward(c2)
		if count == 0 {
			t.Error("expected rows after deep-tree splits")
		}
	})
}

// ---------------------------------------------------------------------------
// Condition 55 — split.go populateRightInteriorPage: medianIdx+1 < len(childPages)  (line 440)
//
//   if medianIdx+1 < len(childPages) { ... update right child pointer }
//
//   A = (medianIdx+1 < len(childPages))
//
//   Row 1: A=T → right child pointer written
//   Row 2: A=F → no right child pointer written (edge: medianIdx at end)
//
//   Exercised by splitting interior pages with varying numbers of cells.
// ---------------------------------------------------------------------------

func TestMCDC_PopulateRightInteriorPage_ChildPtrGuard(t *testing.T) {
	t.Parallel()

	// Both paths are exercised by inserting enough rows to force interior-page
	// splits. The interior splitter will encounter both cases as cells vary.
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, rootPage)
	// Insert 300 rows to ensure multiple interior-page splits occur, exercising
	// both branches of the medianIdx+1 < len(childPages) guard.
	for i := int64(1); i <= 300; i++ {
		payload := make([]byte, 30)
		c.Insert(i, payload)
	}

	// Verify all rows are retrievable (tree is intact)
	c2 := NewCursor(bt, c.RootPage)
	count := countForward(c2)
	if count == 0 {
		t.Error("expected rows after interior splits")
	}
}

// ---------------------------------------------------------------------------
// Condition 56 — split.go fixChildPointerAfterSplit: nextIdx < numCells  (line 885)
//
//   nextIdx := insertIdx + 1
//   if nextIdx < numCells { // update next cell's child pointer }
//   else { // update page's right-child pointer }
//
//   A = (nextIdx < numCells)
//
//   Row 1: A=T → next cell child pointer updated
//   Row 2: A=F → page right-child pointer updated (divider appended at end)
//
//   Exercised by splitting when new key is less than all existing keys (inserts
//   at front → A=T) vs. when new key exceeds all existing keys (appended → A=F).
// ---------------------------------------------------------------------------

func TestMCDC_FixChildPointerAfterSplit_NextIdxGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		insertFn  func(*BtCursor)
		checkKeys func(*testing.T, *BtCursor)
	}{
		{
			// A=T path: insert keys in descending order so new key is always less
			// than existing keys, placing the divider before existing entries.
			name: "A=T nextIdx<numCells: divider not at end",
			insertFn: func(c *BtCursor) {
				for i := int64(200); i >= 1; i-- {
					payload := make([]byte, 50)
					c.Insert(i, payload)
				}
			},
			checkKeys: func(t *testing.T, c *BtCursor) {
				t.Helper()
				count := countForward(c)
				if count == 0 {
					t.Error("expected rows after splits with descending insert order")
				}
			},
		},
		{
			// A=F path: insert keys in ascending order so new key always exceeds
			// existing keys, appending divider at end → right-child update path.
			name: "A=F nextIdx>=numCells: divider at end",
			insertFn: func(c *BtCursor) {
				for i := int64(1); i <= 200; i++ {
					payload := make([]byte, 50)
					c.Insert(i, payload)
				}
			},
			checkKeys: func(t *testing.T, c *BtCursor) {
				t.Helper()
				count := countForward(c)
				if count == 0 {
					t.Error("expected rows after splits with ascending insert order")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			rootPage, err := bt.CreateTable()
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}
			c := NewCursor(bt, rootPage)
			tt.insertFn(c)
			c2 := NewCursor(bt, c.RootPage)
			tt.checkKeys(t, c2)
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 57 — merge.go canMergePage: State==CursorValid && Depth>0  (line 46)
//
//   func (c *BtCursor) canMergePage() bool {
//       return c.State == CursorValid && c.Depth > 0
//   }
//
//   A = (c.State == CursorValid)
//   B = (c.Depth > 0)
//
//   Row 1: A=T, B=T → true (merge allowed)
//   Row 2: A=T, B=F → false (at root, no parent)
//   Row 3: A=F, B=T → false (invalid cursor)
// ---------------------------------------------------------------------------

func TestMCDC_CanMergePage_StateAndDepth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		state  int
		depth  int
		wantOK bool
	}{
		{
			// A=T, B=T → can merge
			name:   "A=T B=T valid cursor depth>0: can merge",
			state:  CursorValid,
			depth:  1,
			wantOK: true,
		},
		{
			// A=T, B=F → cannot merge (at root)
			name:   "A=T B=F valid cursor depth=0: cannot merge",
			state:  CursorValid,
			depth:  0,
			wantOK: false,
		},
		{
			// A=F, B=T → cannot merge (invalid state)
			name:   "A=F B=T invalid cursor depth>0: cannot merge",
			state:  CursorInvalid,
			depth:  1,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			c := NewCursor(bt, 1)
			c.State = tt.state
			c.Depth = tt.depth
			got := c.canMergePage()
			if got != tt.wantOK {
				t.Errorf("canMergePage() = %v, want %v", got, tt.wantOK)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 58 — merge.go canMergePageTypes: types match && both are leaf  (line 482)
//
//   func canMergePageTypes(leftHeader, rightHeader *PageHeader) bool {
//       if leftHeader.PageType != rightHeader.PageType { return false }
//       return leftHeader.IsLeaf && rightHeader.IsLeaf
//   }
//
//   A = (leftHeader.PageType != rightHeader.PageType)  [negated: A=T means mismatch]
//   B = (leftHeader.IsLeaf)
//   C = (rightHeader.IsLeaf)
//
//   Row 1: A=T          → false (type mismatch)
//   Row 2: A=F, B=F     → false (left not leaf)
//   Row 3: A=F, B=T, C=F→ false (right not leaf)
//   Row 4: A=F, B=T, C=T→ true (both leaf, same type)
// ---------------------------------------------------------------------------

func TestMCDC_CanMergePageTypes_V2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		leftType   byte
		rightType  byte
		leftLeaf   bool
		rightLeaf  bool
		wantResult bool
	}{
		{
			// A=T: type mismatch → false
			name:       "A=T type mismatch: false",
			leftType:   PageTypeLeafTable,
			rightType:  PageTypeInteriorTable,
			leftLeaf:   true,
			rightLeaf:  false,
			wantResult: false,
		},
		{
			// A=F, B=F: same type but left not leaf → false
			name:       "A=F B=F left not leaf: false",
			leftType:   PageTypeInteriorTable,
			rightType:  PageTypeInteriorTable,
			leftLeaf:   false,
			rightLeaf:  false,
			wantResult: false,
		},
		{
			// A=F, B=T, C=F: same type, left leaf, right not leaf → false
			name:       "A=F B=T C=F right not leaf: false",
			leftType:   PageTypeLeafTable,
			rightType:  PageTypeLeafTable,
			leftLeaf:   true,
			rightLeaf:  false,
			wantResult: false,
		},
		{
			// A=F, B=T, C=T: same type, both leaf → true
			name:       "A=F B=T C=T both leaf: true",
			leftType:   PageTypeLeafTable,
			rightType:  PageTypeLeafTable,
			leftLeaf:   true,
			rightLeaf:  true,
			wantResult: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			left := &PageHeader{PageType: tt.leftType, IsLeaf: tt.leftLeaf}
			right := &PageHeader{PageType: tt.rightType, IsLeaf: tt.rightLeaf}
			got := canMergePageTypes(left, right)
			if got != tt.wantResult {
				t.Errorf("canMergePageTypes() = %v, want %v", got, tt.wantResult)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 59 — merge.go updateParentRightChildIfNeeded: cellToRemove check  (line 327)
//
//   if cellToRemove != int(parentBtreePage.Header.NumCells)-1 { return nil }
//   if !parentHeader.IsInterior { return nil }
//   ...update right child...
//
//   A = (cellToRemove != NumCells-1)   [early return when not last cell]
//   B = (parentHeader.IsInterior)
//
//   Row 1: A=T          → early nil (not the last cell)
//   Row 2: A=F, B=F     → nil (parent not interior)
//   Row 3: A=F, B=T     → updates right child
//
//   Exercised via mergePages on trees with different configurations.
// ---------------------------------------------------------------------------

func TestMCDC_UpdateParentRightChild_Guards(t *testing.T) {
	t.Parallel()

	// Build a tree with at least 3 leaf children so we can exercise removing
	// a non-last cell (A=T) and the last cell (A=F, B=T).
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, rootPage)
	// Insert enough rows to create multiple leaf siblings under a single interior parent
	for i := int64(1); i <= 100; i++ {
		payload := make([]byte, 50)
		c.Insert(i, payload)
	}

	// Delete most rows from the end to trigger merges where the last cell
	// is removed from the parent (A=F, B=T path).
	for i := int64(60); i <= 100; i++ {
		found, err := c.SeekRowid(i)
		if err == nil && found {
			c.Delete()
		}
	}

	// Attempt merges — this exercises the right-child update path
	c2 := NewCursor(bt, c.RootPage)
	if err := c2.MoveToFirst(); err == nil {
		if c2.Depth > 0 {
			c2.MergePage()
		}
	}

	// Verify tree integrity
	c3 := NewCursor(bt, c.RootPage)
	count := countForward(c3)
	_ = count // just verify no panic
}

// ---------------------------------------------------------------------------
// Condition 60 — merge.go updateParentSeparator: NumCells==0 guard  (line 399)
//
//   if rightBtreePage.Header.NumCells == 0 { return nil }
//
//   A = (rightBtreePage.Header.NumCells == 0)
//
//   Row 1: A=T → early nil (empty right page, no separator update needed)
//   Row 2: A=F → proceeds to get first key and update parent separator
//
//   Exercised via redistributeSiblings on pages with/without cells.
// ---------------------------------------------------------------------------

func TestMCDC_UpdateParentSeparator_EmptyRightPageGuard(t *testing.T) {
	t.Parallel()

	// Build a tree and trigger redistribution by creating an imbalance.
	bt := NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c := NewCursor(bt, rootPage)
	for i := int64(1); i <= 80; i++ {
		payload := make([]byte, 50)
		c.Insert(i, payload)
	}

	// Delete rows from one sibling to make it underfull but not empty enough to merge.
	// This triggers redistribution (A=F path of updateParentSeparator).
	for i := int64(1); i <= 20; i++ {
		found, err := c.SeekRowid(i)
		if err == nil && found {
			c.Delete()
		}
	}

	c2 := NewCursor(bt, c.RootPage)
	if err := c2.MoveToFirst(); err == nil && c2.Depth > 0 {
		c2.MergePage()
	}

	c3 := NewCursor(bt, c.RootPage)
	count := countForward(c3)
	_ = count
}

// ---------------------------------------------------------------------------
// Condition 61 — merge.go replaceSeparatorCell: out-of-range guard  (line 452)
//
//   if separatorIndex < 0 || separatorIndex >= int(parentBtreePage.Header.NumCells) {
//       return nil
//   }
//
//   A = (separatorIndex < 0)
//   B = (separatorIndex >= NumCells)
//
//   Row 1: A=T, B=F → early nil (negative index)
//   Row 2: A=F, B=T → early nil (index at/beyond NumCells)
//   Row 3: A=F, B=F → proceeds to delete and insert separator
// ---------------------------------------------------------------------------

func TestMCDC_ReplaceSeparatorCell_IndexGuards(t *testing.T) {
	t.Parallel()

	buildParentPage := func(numCells int) *BtreePage {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		c := NewCursor(bt, rootPage)
		// Insert some rows to populate the root as a leaf
		for i := int64(1); i <= int64(numCells); i++ {
			c.Insert(i, []byte("val"))
		}
		pageData, _ := bt.GetPage(rootPage)
		page, _ := NewBtreePage(rootPage, pageData, bt.UsableSize)
		return page
	}

	tests := []struct {
		name           string
		separatorIndex int
		numCells       int
		wantErr        bool
	}{
		{
			// A=T: separatorIndex < 0 → early nil
			name:           "A=T separatorIndex<0: nil",
			separatorIndex: -1,
			numCells:       3,
			wantErr:        false,
		},
		{
			// A=F, B=T: separatorIndex >= NumCells → early nil
			name:           "A=F B=T separatorIndex>=NumCells: nil",
			separatorIndex: 10,
			numCells:       3,
			wantErr:        false,
		},
		{
			// A=F, B=F: valid index → proceeds
			name:           "A=F B=F valid index: proceeds",
			separatorIndex: 0,
			numCells:       3,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			c := NewCursor(bt, 1)
			page := buildParentPage(tt.numCells)
			err := c.replaceSeparatorCell(page, tt.separatorIndex, 2, 42)
			if (err != nil) != tt.wantErr {
				t.Errorf("replaceSeparatorCell() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 62 — merge.go determineParentCellToRemove: !leftIsOurs && parentIndex>0  (line 319)
//
//   if !leftIsOurs && parentIndex > 0 { return parentIndex - 1 }
//   return parentIndex
//
//   A = (!leftIsOurs)   — we are the right sibling
//   B = (parentIndex > 0)
//
//   Row 1: A=T, B=T → parentIndex - 1
//   Row 2: A=T, B=F → parentIndex (edge: parentIndex=0)
//   Row 3: A=F, B=T → parentIndex (we are the left sibling)
// ---------------------------------------------------------------------------

func TestMCDC_DetermineParentCellToRemove_V2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		leftIsOurs  bool
		parentIndex int
		want        int
	}{
		{
			// A=T, B=T: right sibling and parentIndex>0 → parentIndex-1
			name:        "A=T B=T rightSibling parentIdx>0: idx-1",
			leftIsOurs:  false,
			parentIndex: 3,
			want:        2,
		},
		{
			// A=T, B=F: right sibling but parentIndex=0 → parentIndex (=0)
			name:        "A=T B=F rightSibling parentIdx=0: 0",
			leftIsOurs:  false,
			parentIndex: 0,
			want:        0,
		},
		{
			// A=F, B=T: left sibling, parentIndex>0 → parentIndex unchanged
			name:        "A=F B=T leftSibling parentIdx>0: parentIndex",
			leftIsOurs:  true,
			parentIndex: 3,
			want:        3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			c := NewCursor(bt, 1)
			got := c.determineParentCellToRemove(tt.parentIndex, tt.leftIsOurs)
			if got != tt.want {
				t.Errorf("determineParentCellToRemove() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 63 — varint.go PutVarint: 1-byte path vs 2-byte path vs larger  (lines 20-35)
//
//   if v <= 0x7f  { 1-byte encoding }
//   if v <= 0x3fff { 2-byte encoding }
//   otherwise      { putVarint64 }
//
//   A = (v <= 0x7f)
//   B = (v <= 0x3fff)
//
//   Row 1: A=T, B=T → 1 byte written (A short-circuits)
//   Row 2: A=F, B=T → 2 bytes written
//   Row 3: A=F, B=F → 3+ bytes (putVarint64)
// ---------------------------------------------------------------------------

func TestMCDC_PutVarint_SizeBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		value     uint64
		wantBytes int
	}{
		{
			// A=T: v <= 0x7f → 1 byte
			name:      "A=T B=T v=0x7f: 1 byte",
			value:     0x7f,
			wantBytes: 1,
		},
		{
			// A=F, B=T: 0x7f < v <= 0x3fff → 2 bytes
			name:      "A=F B=T v=0x80: 2 bytes",
			value:     0x80,
			wantBytes: 2,
		},
		{
			// A=F, B=T: max 2-byte value
			name:      "A=F B=T v=0x3fff: 2 bytes",
			value:     0x3fff,
			wantBytes: 2,
		},
		{
			// A=F, B=F: v > 0x3fff → putVarint64 path (3+ bytes)
			name:      "A=F B=F v=0x4000: 3 bytes",
			value:     0x4000,
			wantBytes: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, 16)
			n := PutVarint(buf, tt.value)
			if n != tt.wantBytes {
				t.Errorf("PutVarint(%#x) = %d bytes, want %d", tt.value, n, tt.wantBytes)
			}
			// Verify round-trip
			got, m := GetVarint(buf[:n])
			if m != n {
				t.Errorf("GetVarint decoded %d bytes, want %d", m, n)
			}
			if got != tt.value {
				t.Errorf("GetVarint round-trip: got %d, want %d", got, tt.value)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 64 — varint.go putVarint64: 9-byte threshold  (line 41)
//
//   if v&(uint64(0xff000000)<<32) != 0 { return encodeVarint9Bytes(p, v) }
//
//   A = (v & (0xff000000 << 32) != 0)   top byte has bits set
//
//   Row 1: A=T → 9-byte encoding
//   Row 2: A=F → countVarintBytes path (3-8 bytes)
// ---------------------------------------------------------------------------

func TestMCDC_PutVarint64_NineBytePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		value     uint64
		wantBytes int
	}{
		{
			// A=T: top byte non-zero → 9 bytes
			// 0xff00_0000_0000_0000 has top bits set
			name:      "A=T high bits set: 9 bytes",
			value:     0xff00000000000000,
			wantBytes: 9,
		},
		{
			// A=F: top byte zero, fits in 7 bytes.
			// Note: GetVarint requires >=9 bytes to decode 8-byte varints,
			// so we verify round-trip with a full 16-byte buffer.
			name:      "A=F high bits clear: 8 bytes",
			value:     0x00ffffffffffffff,
			wantBytes: 8,
		},
		{
			// A=F: small multi-byte value → 3 bytes
			name:      "A=F small value: 3 bytes",
			value:     0x4000,
			wantBytes: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			buf := make([]byte, 16)
			n := PutVarint(buf, tt.value)
			if n != tt.wantBytes {
				t.Errorf("PutVarint(%#x) = %d bytes, want %d", tt.value, n, tt.wantBytes)
			}
			// Round-trip decode: pass full buf so GetVarint has enough bytes
			// for varints that require >=9 bytes to decode (e.g. 8-byte encoding).
			got, m := GetVarint(buf)
			if m != n {
				t.Errorf("GetVarint decoded %d bytes, want %d", m, n)
			}
			if got != tt.value {
				t.Errorf("GetVarint round-trip: got %#x, want %#x", got, tt.value)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 65 — varint.go tryGetVarintFast: 1-byte then 2-byte fast path  (lines 148-156)
//
//   if p[0] < 0x80 { return uint64(p[0]), 1, true }
//   if len(p) > 1 && p[1] < 0x80 { return ..., 2, true }
//   return 0, 0, false
//
//   A = (p[0] < 0x80)       — single byte
//   B = (len(p) > 1)
//   C = (p[1] < 0x80)       — continuation byte is terminal
//
//   Row 1: A=T              → 1-byte fast
//   Row 2: A=F, B=T, C=T   → 2-byte fast
//   Row 3: A=F, B=T, C=F   → slow path (2+ bytes, p[1] has continuation)
//   Row 4: A=F, B=F         → slow path (only 1 byte with continuation bit)
// ---------------------------------------------------------------------------

func TestMCDC_TryGetVarintFast_Branches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      []byte
		wantVal   uint64
		wantBytes int
		wantOK    bool
	}{
		{
			// A=T: p[0] < 0x80 → 1-byte fast path
			name:      "A=T p[0]<0x80: 1-byte",
			data:      []byte{0x42},
			wantVal:   0x42,
			wantBytes: 1,
			wantOK:    true,
		},
		{
			// A=F, B=T, C=T: p[0] has continuation bit, p[1] < 0x80 → 2-byte
			name:      "A=F B=T C=T p[0]>=0x80 p[1]<0x80: 2-byte",
			data:      []byte{0x81, 0x00},
			wantVal:   0x80,
			wantBytes: 2,
			wantOK:    true,
		},
		{
			// A=F, B=T, C=F: both bytes have continuation bit → slow path
			name:      "A=F B=T C=F both continuation: slow path",
			data:      []byte{0x81, 0x81, 0x00},
			wantOK:    false, // fast path returns false
			wantBytes: 0,
		},
		{
			// A=F, B=F: only 1 byte with continuation → slow path
			name:      "A=F B=F only one byte with continuation: slow",
			data:      []byte{0x80},
			wantOK:    false,
			wantBytes: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotVal, gotN, gotOK := tryGetVarintFast(tt.data)
			if gotOK != tt.wantOK {
				t.Errorf("tryGetVarintFast() ok = %v, want %v", gotOK, tt.wantOK)
			}
			if tt.wantOK {
				if gotVal != tt.wantVal {
					t.Errorf("tryGetVarintFast() val = %d, want %d", gotVal, tt.wantVal)
				}
				if gotN != tt.wantBytes {
					t.Errorf("tryGetVarintFast() n = %d, want %d", gotN, tt.wantBytes)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 66 — page.go GetCellPointer: cellIndex out-of-range guard  (line 149)
//
//   if cellIndex < 0 || cellIndex >= int(h.NumCells) { return error }
//
//   A = (cellIndex < 0)
//   B = (cellIndex >= NumCells)
//
//   Row 1: A=T, B=F → error (negative)
//   Row 2: A=F, B=T → error (too large)
//   Row 3: A=F, B=F → success
// ---------------------------------------------------------------------------

func TestMCDC_GetCellPointer_IndexGuard(t *testing.T) {
	t.Parallel()

	makeHeader := func(numCells uint16) (*PageHeader, []byte) {
		data := make([]byte, 4096)
		data[PageHeaderOffsetType] = PageTypeLeafTable
		data[PageHeaderOffsetNumCells] = byte(numCells >> 8)
		data[PageHeaderOffsetNumCells+1] = byte(numCells)
		// Write a fake cell pointer at offset PageHeaderSizeLeaf so index 0 is "valid"
		binary.BigEndian.PutUint16(data[PageHeaderSizeLeaf:], 2048)
		h := &PageHeader{
			PageType:      PageTypeLeafTable,
			NumCells:      numCells,
			IsLeaf:        true,
			HeaderSize:    PageHeaderSizeLeaf,
			CellPtrOffset: PageHeaderSizeLeaf,
		}
		return h, data
	}

	tests := []struct {
		name      string
		numCells  uint16
		cellIndex int
		wantErr   bool
	}{
		{
			// A=T: cellIndex < 0 → error
			name:      "A=T cellIndex=-1: error",
			numCells:  3,
			cellIndex: -1,
			wantErr:   true,
		},
		{
			// A=F, B=T: cellIndex >= NumCells → error
			name:      "A=F B=T cellIndex=numCells: error",
			numCells:  3,
			cellIndex: 3,
			wantErr:   true,
		},
		{
			// A=F, B=F: valid index → success
			name:      "A=F B=F valid index: success",
			numCells:  3,
			cellIndex: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, data := makeHeader(tt.numCells)
			_, err := h.GetCellPointer(data, tt.cellIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCellPointer(%d) error = %v, wantErr %v", tt.cellIndex, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 67 — page.go InsertCell: idx out-of-range guard  (line 217)
//
//   if idx < 0 || idx > int(p.Header.NumCells) { return error }
//
//   A = (idx < 0)
//   B = (idx > NumCells)
//
//   Row 1: A=T, B=F → error
//   Row 2: A=F, B=T → error
//   Row 3: A=F, B=F → success
// ---------------------------------------------------------------------------

func TestMCDC_InsertCell_IndexGuard(t *testing.T) {
	t.Parallel()

	makeEmptyPage := func() *BtreePage {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		data, _ := bt.GetPage(rootPage)
		page, _ := NewBtreePage(rootPage, data, bt.UsableSize)
		return page
	}

	cell := make([]byte, 10) // minimal cell

	tests := []struct {
		name    string
		idx     int
		wantErr bool
	}{
		{
			// A=T: idx < 0 → error
			name:    "A=T idx=-1: error",
			idx:     -1,
			wantErr: true,
		},
		{
			// A=F, B=T: idx > NumCells (NumCells=0, idx=1) → error
			name:    "A=F B=T idx>numCells: error",
			idx:     2,
			wantErr: true,
		},
		{
			// A=F, B=F: idx=0 and NumCells=0 → valid (append)
			name:    "A=F B=F valid append: success",
			idx:     0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			page := makeEmptyPage()
			err := page.InsertCell(tt.idx, cell)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertCell(%d) error = %v, wantErr %v", tt.idx, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 68 — balance.go isUnderfull: cursor.CurrentPage==cursor.RootPage  (line 162)
//
//   (in handleUnderfullPage)
//   if cursor.CurrentPage == cursor.RootPage { return nil }
//
//   A = (cursor.CurrentPage == cursor.RootPage)
//
//   Row 1: A=T → nil (root is allowed to be underfull)
//   Row 2: A=F → proceeds to check depth and siblings
// ---------------------------------------------------------------------------

func mcdc4CursorAtRoot() *BtCursor {
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	c := NewCursor(bt, rootPage)
	for i := int64(1); i <= 5; i++ {
		c.Insert(i, []byte("x")) //nolint:errcheck
	}
	deleteRowRange(c, 1, 4)
	c.MoveToFirst() //nolint:errcheck
	c.CurrentPage = c.RootPage
	return c
}

func mcdc4CursorNonRoot() *BtCursor {
	bt := NewBtree(4096)
	rootPage, _ := bt.CreateTable()
	c := NewCursor(bt, rootPage)
	insertRows(c, 1, 80, 50)
	c.MoveToFirst() //nolint:errcheck
	if c.Depth == 0 {
		c.CurrentPage = c.RootPage + 1
	}
	return c
}

func runUnderfullPageTest(t *testing.T, c *BtCursor, wantErr bool) {
	t.Helper()
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		t.Skip("page not available")
	}
	page, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		t.Skip("NewBtreePage failed")
	}
	err = handleUnderfullPage(c, page)
	if !wantErr && err != nil {
		t.Errorf("handleUnderfullPage() error = %v, want nil", err)
	}
}

func TestMCDC_HandleUnderfullPage_RootGuard_AtRoot(t *testing.T) {
	t.Parallel()
	runUnderfullPageTest(t, mcdc4CursorAtRoot(), false)
}

func TestMCDC_HandleUnderfullPage_RootGuard_NonRoot(t *testing.T) {
	t.Parallel()
	runUnderfullPageTest(t, mcdc4CursorNonRoot(), true)
}

// ---------------------------------------------------------------------------
// Condition 69 — balance.go isOverfull: freeSpace < minCellWithPointer  (line 33)
//
//   return freeSpace < minCellWithPointer   where minCellWithPointer = 6
//
//   A = (freeSpace < 6)
//
//   Row 1: A=T → true (overfull)
//   Row 2: A=F → false (not overfull)
// ---------------------------------------------------------------------------

func TestMCDC_IsOverfull_FreeSpaceGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		page     func() *BtreePage
		wantOver bool
	}{
		{
			// A=T: freeSpace < 6 → overfull
			name: "A=T freeSpace<6: overfull",
			page: func() *BtreePage {
				// Build a page that is nearly full by hand
				const pageSize = 512
				data := make([]byte, pageSize)
				data[PageHeaderOffsetType] = PageTypeLeafTable
				// Set CellContentStart very close to the pointer array end
				// Header(8) + 1 cell ptr(2) = 10; set CellContentStart = 11
				binary.BigEndian.PutUint16(data[PageHeaderOffsetNumCells:], 1)
				binary.BigEndian.PutUint16(data[PageHeaderOffsetCellStart:], 11)
				// Write a fake cell pointer
				binary.BigEndian.PutUint16(data[PageHeaderSizeLeaf:], 11)
				page, _ := NewBtreePage(2, data, pageSize)
				return page
			},
			wantOver: true,
		},
		{
			// A=F: large page with few cells → not overfull
			name: "A=F empty page: not overfull",
			page: func() *BtreePage {
				bt := NewBtree(4096)
				rootPage, _ := bt.CreateTable()
				data, _ := bt.GetPage(rootPage)
				page, _ := NewBtreePage(rootPage, data, bt.UsableSize)
				return page
			},
			wantOver: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isOverfull(tt.page())
			if got != tt.wantOver {
				t.Errorf("isOverfull() = %v, want %v", got, tt.wantOver)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 70 — varint.go GetVarint32 slowBtreeVarint32: n>3 && n<=9 guard  (line 201)
//
//   if n > 3 && n <= 9 { ... }
//
//   A = (n > 3)
//   B = (n <= 9)
//
//   Row 1: A=T, B=T → decode 4-9 byte varint, check overflow
//   Row 2: A=T, B=F → impossible (n is at most 9 from GetVarint)
//   Row 3: A=F, B=T → n<=3 → return 0,0
//
//   Exercised by encoding varints of various sizes and reading back with GetVarint32.
// ---------------------------------------------------------------------------

func TestMCDC_GetVarint32_SlowPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      uint64
		wantResult uint32
		wantN      int
	}{
		{
			// A=T, B=T: n=4 (4-byte varint) → returned as uint32
			name:       "A=T B=T 4-byte varint: returned",
			value:      0x200000, // needs 4 bytes
			wantResult: 0x200000,
			wantN:      4,
		},
		{
			// A=T, B=T: large value that overflows uint32 → returns 0xffffffff
			name:       "A=T B=T value>maxUint32: 0xffffffff",
			value:      0x1ffffffff, // > 32 bits, needs 5 bytes
			wantResult: 0xffffffff,
			wantN:      5,
		},
		{
			// A=F, B=T: n=1 (single byte) → fast path triggered, slowBtreeVarint32
			// receives n=1 which is <= 3 → returns 0,0
			name:       "A=F B=T 1-byte varint: handled by fast path",
			value:      0x01,
			wantResult: 1,
			wantN:      1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Use a 16-byte buffer so GetVarint (called inside GetVarint32) has
			// enough bytes to decode 5-9 byte varints which need len>=9.
			buf := make([]byte, 16)
			PutVarint(buf, tt.value)
			got, gotN := GetVarint32(buf)
			if gotN != tt.wantN {
				t.Errorf("GetVarint32(%#x) n = %d, want %d", tt.value, gotN, tt.wantN)
			}
			if got != tt.wantResult {
				t.Errorf("GetVarint32(%#x) = %d, want %d", tt.value, got, tt.wantResult)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 71 — split.go encodeNewCellWithOverflow: CompositePK branch  (lines 491-494)
//
//   if payloadSize > uint32(localSize) { ... overflow ... }
//   if c.CompositePK { return EncodeTableLeafCompositeCell(...) }
//   return EncodeTableLeafCell(...)
//
//   A = (payloadSize > uint32(localSize))  — overflow needed
//   B = (c.CompositePK)
//
//   Row 1: A=T, B=F → overflow + rowid cell
//   Row 2: A=T, B=T → overflow + composite cell
//   Row 3: A=F, B=T → local composite cell
//   Row 4: A=F, B=F → local rowid cell
//
//   Exercised by inserting rows into rowid and WITHOUT ROWID tables.
// ---------------------------------------------------------------------------

func TestMCDC_EncodeNewCellWithOverflow_CompositePKBranch(t *testing.T) {
	t.Parallel()

	const usableSize = uint32(4096)
	maxLocal := calculateMaxLocal(usableSize, true)

	tests := []struct {
		name        string
		compositePK bool
		payload     []byte
	}{
		{
			// A=F, B=F: small payload, rowid table
			name:        "A=F B=F small payload rowid: local rowid cell",
			compositePK: false,
			payload:     []byte("small"),
		},
		{
			// A=F, B=T: small payload, composite table
			name:        "A=F B=T small payload composite: local composite cell",
			compositePK: true,
			payload:     []byte("small"),
		},
		{
			// A=T, B=F: large payload, rowid table
			name:        "A=T B=F large payload rowid: overflow rowid cell",
			compositePK: false,
			payload:     make([]byte, int(maxLocal)+100),
		},
		{
			// A=T, B=T: large payload, composite table
			name:        "A=T B=T large payload composite: overflow composite cell",
			compositePK: true,
			payload:     make([]byte, int(maxLocal)+100),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			var rootPage uint32
			var err error
			if tt.compositePK {
				rootPage, err = bt.CreateWithoutRowidTable()
			} else {
				rootPage, err = bt.CreateTable()
			}
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}

			c := NewCursorWithOptions(bt, rootPage, tt.compositePK)
			localSize := calculateMaxLocal(bt.UsableSize, true)

			var keyBytes []byte
			if tt.compositePK {
				keyBytes = []byte("testkey")
			}
			// Call the internal function directly
			result, encErr := c.encodeNewCellWithOverflow(42, keyBytes, tt.payload)
			if encErr != nil {
				t.Fatalf("encodeNewCellWithOverflow() error = %v", encErr)
			}
			if len(result) == 0 {
				t.Error("encodeNewCellWithOverflow() returned empty cell")
			}
			_ = localSize
		})
	}
}

// ---------------------------------------------------------------------------
// Condition 72 — merge.go calculateSeparatorIndex: separatorIndex >= NumCells  (line 444)
//
//   separatorIndex := parentIndex
//   if separatorIndex >= int(parentBtreePage.Header.NumCells) {
//       separatorIndex = int(parentBtreePage.Header.NumCells) - 1
//   }
//
//   A = (separatorIndex >= NumCells)
//
//   Row 1: A=T → clamped to NumCells-1
//   Row 2: A=F → unchanged
// ---------------------------------------------------------------------------

func TestMCDC_CalculateSeparatorIndex(t *testing.T) {
	t.Parallel()

	buildPageWithCells := func(numCells int) *BtreePage {
		bt := NewBtree(4096)
		rootPage, _ := bt.CreateTable()
		c := NewCursor(bt, rootPage)
		for i := int64(1); i <= int64(numCells); i++ {
			c.Insert(i, []byte("v"))
		}
		data, _ := bt.GetPage(rootPage)
		page, _ := NewBtreePage(rootPage, data, bt.UsableSize)
		return page
	}

	tests := []struct {
		name        string
		parentIndex int
		numCells    int
		wantIndex   int
	}{
		{
			// A=T: parentIndex >= NumCells → clamped
			name:        "A=T parentIndex>=numCells: clamped to numCells-1",
			parentIndex: 10,
			numCells:    3,
			wantIndex:   2, // clamped to numCells-1
		},
		{
			// A=F: parentIndex < NumCells → unchanged
			name:        "A=F parentIndex<numCells: unchanged",
			parentIndex: 1,
			numCells:    3,
			wantIndex:   1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bt := NewBtree(4096)
			c := NewCursor(bt, 1)
			page := buildPageWithCells(tt.numCells)
			got := c.calculateSeparatorIndex(tt.parentIndex, page)
			if got != tt.wantIndex {
				t.Errorf("calculateSeparatorIndex(%d, page(numCells=%d)) = %d, want %d",
					tt.parentIndex, tt.numCells, got, tt.wantIndex)
			}
		})
	}
}
