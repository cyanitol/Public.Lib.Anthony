// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SplitResult contains the information about a page split
type SplitResult struct {
	DividerKey   int64  // The key that divides the two pages
	NewPageNum   uint32 // The newly allocated page number
	LeftPageNum  uint32 // The left page (original page)
	RightPageNum uint32 // The right page (new page)
}

// splitLeafPage splits a full leaf page when inserting a new cell
// The algorithm:
// 1. Allocate a new sibling page
// 2. Find the median key among all cells (including the new one)
// 3. Move cells >= median to the new page
// 4. Insert the new cell in the appropriate page
// 5. Update parent with divider key
func (c *BtCursor) splitLeafPage(key int64, keyBytes []byte, payload []byte) error {
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("splitLeafPage called on non-leaf page")
	}

	if c.CompositePK {
		return c.splitLeafPageComposite(keyBytes, payload)
	}

	oldPage, cells, keys, newPage, newPageNum, err := c.prepareLeafSplitPages(key, payload)
	if err != nil {
		return err
	}

	medianIdx := len(cells) / 2
	if err := c.executeLeafSplit(oldPage, newPage, cells, keys, medianIdx, newPageNum, key, nil); err != nil {
		return err
	}

	_, err = c.SeekRowid(key)
	return err
}

// prepareLeafSplitPages prepares both old and new pages for splitting.
func (c *BtCursor) prepareLeafSplitPages(key int64, payload []byte) (*BtreePage, [][]byte, []int64, *BtreePage, uint32, error) {
	oldPage, cells, keys, err := c.prepareLeafSplit(key, payload)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	newPage, newPageNum, err := c.allocateAndInitializeLeafPage(c.CurrentHeader.PageType)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	return oldPage, cells, keys, newPage, newPageNum, nil
}

// prepareLeafSplitPagesComposite prepares split state for composite-key leaves.
func (c *BtCursor) prepareLeafSplitPagesComposite(keyBytes []byte, payload []byte) (*BtreePage, [][]byte, [][]byte, *BtreePage, uint32, error) {
	oldPage, cells, keys, err := c.prepareLeafSplitComposite(keyBytes, payload)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	newPage, newPageNum, err := c.allocateAndInitializeLeafPage(c.CurrentHeader.PageType)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	return oldPage, cells, keys, newPage, newPageNum, nil
}

// executeLeafSplit performs the actual leaf split operation.
func (c *BtCursor) executeLeafSplit(oldPage, newPage *BtreePage, cells [][]byte, keys []int64, medianIdx int, newPageNum uint32, key int64, keyBytes []byte) error {
	if err := c.markPagesAsDirty(c.CurrentPage, newPageNum); err != nil {
		return err
	}

	if err := c.redistributeLeafCells(oldPage, newPage, cells, medianIdx); err != nil {
		return err
	}

	dividerKey := keys[medianIdx]
	return c.updateParentAfterSplit(c.CurrentPage, newPageNum, dividerKey, keyBytes)
}

// executeLeafSplitComposite performs the split using composite keys.
func (c *BtCursor) executeLeafSplitComposite(oldPage, newPage *BtreePage, cells [][]byte, keys [][]byte, medianIdx int, newPageNum uint32) error {
	if err := c.markPagesAsDirty(c.CurrentPage, newPageNum); err != nil {
		return err
	}

	if err := c.redistributeLeafCells(oldPage, newPage, cells, medianIdx); err != nil {
		return err
	}

	dividerKey := keys[medianIdx]
	return c.updateParentAfterSplit(c.CurrentPage, newPageNum, 0, dividerKey)
}

// splitInteriorPage splits a full interior page when inserting a new cell
func (c *BtCursor) splitInteriorPage(key int64, keyBytes []byte, childPgno uint32) error {
	if c.CurrentHeader == nil || !c.CurrentHeader.IsInterior {
		return fmt.Errorf("splitInteriorPage called on non-interior page")
	}

	if c.CompositePK {
		return c.splitInteriorPageComposite(keyBytes, childPgno)
	}

	oldPage, cells, keys, childPages, newPage, newPageNum, err := c.prepareInteriorSplitPages(key, childPgno)
	if err != nil {
		return err
	}

	medianIdx := len(cells) / 2
	return c.executeInteriorSplit(oldPage, newPage, cells, keys, childPages, medianIdx, newPageNum, keyBytes)
}

// prepareInteriorSplitPages prepares both old and new pages for interior split.
func (c *BtCursor) prepareInteriorSplitPages(key int64, childPgno uint32) (*BtreePage, [][]byte, []int64, []uint32, *BtreePage, uint32, error) {
	oldPage, cells, keys, childPages, err := c.prepareInteriorSplit(key, childPgno)
	if err != nil {
		return nil, nil, nil, nil, nil, 0, err
	}

	newPage, newPageNum, err := c.allocateAndInitializeInteriorPage(c.CurrentHeader.PageType)
	if err != nil {
		return nil, nil, nil, nil, nil, 0, err
	}

	return oldPage, cells, keys, childPages, newPage, newPageNum, nil
}

// prepareInteriorSplitPagesComposite prepares split state for composite-key interior pages.
func (c *BtCursor) prepareInteriorSplitPagesComposite(keyBytes []byte, childPgno uint32) (*BtreePage, [][]byte, [][]byte, []uint32, *BtreePage, uint32, error) {
	oldPage, cells, keys, childPages, err := c.prepareInteriorSplitComposite(keyBytes, childPgno)
	if err != nil {
		return nil, nil, nil, nil, nil, 0, err
	}

	newPage, newPageNum, err := c.allocateAndInitializeInteriorPage(c.CurrentHeader.PageType)
	if err != nil {
		return nil, nil, nil, nil, nil, 0, err
	}

	return oldPage, cells, keys, childPages, newPage, newPageNum, nil
}

// executeInteriorSplit performs the actual interior split operation.
func (c *BtCursor) executeInteriorSplit(oldPage, newPage *BtreePage, cells [][]byte, keys []int64, childPages []uint32, medianIdx int, newPageNum uint32, keyBytes []byte) error {
	if err := c.markPagesAsDirty(c.CurrentPage, newPageNum); err != nil {
		return err
	}

	if err := c.redistributeInteriorCells(oldPage, newPage, cells, childPages, medianIdx, newPageNum); err != nil {
		return err
	}

	dividerKey := keys[medianIdx]
	return c.updateParentAfterSplit(c.CurrentPage, newPageNum, dividerKey, keyBytes)
}

// splitLeafPageComposite handles leaf splits for WITHOUT ROWID tables using composite keys.
func (c *BtCursor) splitLeafPageComposite(keyBytes []byte, payload []byte) error {
	oldPage, cells, keys, newPage, newPageNum, err := c.prepareLeafSplitPagesComposite(keyBytes, payload)
	if err != nil {
		return err
	}

	medianIdx := len(cells) / 2
	if err := c.executeLeafSplitComposite(oldPage, newPage, cells, keys, medianIdx, newPageNum); err != nil {
		return err
	}

	_, err = c.SeekComposite(keyBytes)
	return err
}

// splitInteriorPageComposite handles interior splits for WITHOUT ROWID tables.
func (c *BtCursor) splitInteriorPageComposite(keyBytes []byte, childPgno uint32) error {
	oldPage, cells, keys, childPages, newPage, newPageNum, err := c.prepareInteriorSplitPagesComposite(keyBytes, childPgno)
	if err != nil {
		return err
	}

	medianIdx := len(cells) / 2
	return c.executeInteriorSplitComposite(oldPage, newPage, cells, keys, childPages, medianIdx, newPageNum)
}

// executeInteriorSplitComposite performs interior split for composite-key trees.
func (c *BtCursor) executeInteriorSplitComposite(oldPage, newPage *BtreePage, cells [][]byte, keys [][]byte, childPages []uint32, medianIdx int, newPageNum uint32) error {
	if err := c.markPagesAsDirty(c.CurrentPage, newPageNum); err != nil {
		return err
	}

	if err := c.redistributeInteriorCells(oldPage, newPage, cells, childPages, medianIdx, newPageNum); err != nil {
		return err
	}

	dividerKey := keys[medianIdx]
	return c.updateParentAfterSplit(c.CurrentPage, newPageNum, 0, dividerKey)
}

// prepareLeafSplit prepares the current page for splitting by collecting all cells
func (c *BtCursor) prepareLeafSplit(key int64, payload []byte) (*BtreePage, [][]byte, []int64, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get current page: %w", err)
	}

	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create BtreePage: %w", err)
	}

	cells, keys, err := c.collectLeafCellsForSplit(oldPage, key, payload)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to collect cells: %w", err)
	}

	return oldPage, cells, keys, nil
}

// prepareLeafSplitComposite prepares split state for composite-key leaves.
func (c *BtCursor) prepareLeafSplitComposite(keyBytes []byte, payload []byte) (*BtreePage, [][]byte, [][]byte, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get current page: %w", err)
	}

	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create BtreePage: %w", err)
	}

	cells, keys, err := c.collectLeafCellsForSplitComposite(oldPage, keyBytes, payload)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to collect cells: %w", err)
	}

	return oldPage, cells, keys, nil
}

// prepareInteriorSplit prepares the current page for splitting by collecting all cells
func (c *BtCursor) prepareInteriorSplit(key int64, childPgno uint32) (*BtreePage, [][]byte, []int64, []uint32, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to get current page: %w", err)
	}

	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create BtreePage: %w", err)
	}

	cells, keys, childPages, err := c.collectInteriorCellsForSplit(oldPage, key, childPgno)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to collect cells: %w", err)
	}

	return oldPage, cells, keys, childPages, nil
}

// prepareInteriorSplitComposite prepares split collection for composite-key interior pages.
func (c *BtCursor) prepareInteriorSplitComposite(keyBytes []byte, childPgno uint32) (*BtreePage, [][]byte, [][]byte, []uint32, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to get current page: %w", err)
	}

	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create BtreePage: %w", err)
	}

	cells, keys, childPages, err := c.collectInteriorCellsForSplitComposite(oldPage, keyBytes, childPgno)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to collect cells: %w", err)
	}

	return oldPage, cells, keys, childPages, nil
}

// allocateAndInitializeLeafPage allocates and initializes a new leaf page
func (c *BtCursor) allocateAndInitializeLeafPage(pageType byte) (*BtreePage, uint32, error) {
	if pageType == 0 {
		pageType = PageTypeLeafTable
	}
	newPageNum, err := c.Btree.AllocatePage()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	newPageData, err := c.Btree.GetPage(newPageNum)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get new page: %w", err)
	}

	if err := initializeLeafPage(newPageData, newPageNum, c.Btree.UsableSize, pageType); err != nil {
		return nil, 0, fmt.Errorf("failed to initialize new page: %w", err)
	}

	newPage, err := NewBtreePage(newPageNum, newPageData, c.Btree.UsableSize)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create new BtreePage: %w", err)
	}

	return newPage, newPageNum, nil
}

// allocateAndInitializeInteriorPage allocates and initializes a new interior page
func (c *BtCursor) allocateAndInitializeInteriorPage(pageType byte) (*BtreePage, uint32, error) {
	if pageType == 0 {
		pageType = PageTypeInteriorTable
	}
	newPageNum, err := c.Btree.AllocatePage()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	newPageData, err := c.Btree.GetPage(newPageNum)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get new page: %w", err)
	}

	if err := initializeInteriorPage(newPageData, newPageNum, c.Btree.UsableSize, pageType); err != nil {
		return nil, 0, fmt.Errorf("failed to initialize new page: %w", err)
	}

	newPage, err := NewBtreePage(newPageNum, newPageData, c.Btree.UsableSize)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create new BtreePage: %w", err)
	}

	return newPage, newPageNum, nil
}

// markPagesAsDirty marks both pages as dirty in the pager
func (c *BtCursor) markPagesAsDirty(page1, page2 uint32) error {
	if c.Btree.Provider == nil {
		return nil
	}

	if err := c.Btree.Provider.MarkDirty(page1); err != nil {
		return err
	}
	if err := c.Btree.Provider.MarkDirty(page2); err != nil {
		return err
	}
	return nil
}

// redistributeLeafCells distributes cells between left and right leaf pages
func (c *BtCursor) redistributeLeafCells(oldPage, newPage *BtreePage, cells [][]byte, medianIdx int) error {
	if err := clearPageCells(oldPage); err != nil {
		return fmt.Errorf("failed to clear old page: %w", err)
	}

	if err := c.populateLeftPage(oldPage, cells, medianIdx); err != nil {
		return err
	}

	if err := c.populateRightPage(newPage, cells, medianIdx); err != nil {
		return err
	}

	return c.defragmentBothLeafPages(oldPage, newPage)
}

// populateLeftPage inserts cells into the left (old) page.
func (c *BtCursor) populateLeftPage(oldPage *BtreePage, cells [][]byte, medianIdx int) error {
	for i := 0; i < medianIdx; i++ {
		if err := oldPage.InsertCell(i, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into left page: %w", i, err)
		}
	}
	return nil
}

// populateRightPage inserts cells into the right (new) page.
func (c *BtCursor) populateRightPage(newPage *BtreePage, cells [][]byte, medianIdx int) error {
	for i := medianIdx; i < len(cells); i++ {
		if err := newPage.InsertCell(i-medianIdx, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into right page: %w", i, err)
		}
	}
	return nil
}

// defragmentBothLeafPages defragments both pages after redistribution.
func (c *BtCursor) defragmentBothLeafPages(oldPage, newPage *BtreePage) error {
	if err := oldPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment left page: %w", err)
	}
	if err := newPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment right page: %w", err)
	}
	return nil
}

// redistributeInteriorCells distributes cells between left and right interior pages
func (c *BtCursor) redistributeInteriorCells(oldPage, newPage *BtreePage, cells [][]byte, childPages []uint32, medianIdx int, newPageNum uint32) error {
	if err := clearPageCells(oldPage); err != nil {
		return fmt.Errorf("failed to clear old page: %w", err)
	}

	if err := c.populateLeftInteriorPage(oldPage, cells, childPages, medianIdx); err != nil {
		return err
	}

	if err := c.populateRightInteriorPage(newPage, newPageNum, cells, childPages, medianIdx); err != nil {
		return err
	}

	return c.defragmentBothPages(oldPage, newPage)
}

// populateLeftInteriorPage inserts cells into the left page and sets its right child.
func (c *BtCursor) populateLeftInteriorPage(oldPage *BtreePage, cells [][]byte, childPages []uint32, medianIdx int) error {
	for i := 0; i < medianIdx; i++ {
		if err := oldPage.InsertCell(i, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into left page: %w", i, err)
		}
	}

	headerOffset := getHeaderOffset(c.CurrentPage)
	binary.BigEndian.PutUint32(oldPage.Data[headerOffset+PageHeaderOffsetRightChild:], childPages[medianIdx])
	return nil
}

// populateRightInteriorPage inserts cells into the right page and sets its right child.
func (c *BtCursor) populateRightInteriorPage(newPage *BtreePage, newPageNum uint32, cells [][]byte, childPages []uint32, medianIdx int) error {
	for i := medianIdx + 1; i < len(cells); i++ {
		if err := newPage.InsertCell(i-medianIdx-1, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into right page: %w", i, err)
		}
	}

	newHeaderOffset := getHeaderOffset(newPageNum)
	if medianIdx+1 < len(childPages) {
		binary.BigEndian.PutUint32(newPage.Data[newHeaderOffset+PageHeaderOffsetRightChild:], childPages[len(childPages)-1])
	}
	return nil
}

// defragmentBothPages defragments both the old and new pages.
func (c *BtCursor) defragmentBothPages(oldPage, newPage *BtreePage) error {
	if err := oldPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment left page: %w", err)
	}
	if err := newPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment right page: %w", err)
	}
	return nil
}

// collectLeafCellsForSplit collects all existing cells plus the new cell to be inserted
// Returns cells in sorted order by key
// Properly handles overflow pages when encoding the new cell
func (c *BtCursor) collectLeafCellsForSplit(page *BtreePage, newKey int64, newPayload []byte) ([][]byte, []int64, error) {
	newCellData, err := c.encodeNewCellWithOverflow(newKey, nil, newPayload)
	if err != nil {
		return nil, nil, err
	}

	return c.mergeNewCellWithExisting(page, newKey, newCellData)
}

// collectLeafCellsForSplitComposite collects all existing cells plus the new composite-key cell.
func (c *BtCursor) collectLeafCellsForSplitComposite(page *BtreePage, newKey []byte, newPayload []byte) ([][]byte, [][]byte, error) {
	newCellData, err := c.encodeNewCellWithOverflow(0, newKey, newPayload)
	if err != nil {
		return nil, nil, err
	}

	return c.mergeNewCellWithExistingComposite(page, newKey, newCellData)
}

// encodeNewCellWithOverflow encodes the new cell with proper overflow handling.
func (c *BtCursor) encodeNewCellWithOverflow(newKey int64, keyBytes []byte, newPayload []byte) ([]byte, error) {
	payloadSize := uint32(len(newPayload))
	localSize := CalculateLocalPayload(payloadSize, c.Btree.UsableSize, true)

	if payloadSize > uint32(localSize) {
		overflowPage, err := c.WriteOverflow(newPayload, localSize, c.Btree.UsableSize)
		if err != nil {
			return nil, fmt.Errorf("failed to write overflow for split: %w", err)
		}
		return c.encodeTableLeafCellWithOverflow(newKey, keyBytes, newPayload[:localSize], overflowPage, payloadSize), nil
	}
	if c.CompositePK {
		return EncodeTableLeafCompositeCell(keyBytes, newPayload), nil
	}
	return EncodeTableLeafCell(newKey, newPayload), nil
}

// mergeNewCellWithExisting merges the new cell with existing cells in sorted order.
func (c *BtCursor) mergeNewCellWithExisting(page *BtreePage, newKey int64, newCellData []byte) ([][]byte, []int64, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([]int64, 0, numCells+1)
	inserted := false

	for i := 0; i < numCells; i++ {
		if !inserted {
			inserted = c.tryInsertNewCell(&cells, &keys, newKey, newCellData, i, page)
		}

		cellData, cellKey, err := c.copyExistingCell(page, i)
		if err != nil {
			return nil, nil, err
		}
		cells = append(cells, cellData)
		keys = append(keys, cellKey)
	}

	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, newKey)
	}

	return cells, keys, nil
}

// mergeNewCellWithExistingComposite merges cells for composite-key pages.
func (c *BtCursor) mergeNewCellWithExistingComposite(page *BtreePage, newKey []byte, newCellData []byte) ([][]byte, [][]byte, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([][]byte, 0, numCells+1)
	inserted := false

	for i := 0; i < numCells; i++ {
		if !inserted {
			inserted = c.tryInsertNewCellComposite(&cells, &keys, newKey, newCellData, i, page)
		}

		cellData, cellKey, err := c.copyExistingCellComposite(page, i)
		if err != nil {
			return nil, nil, err
		}
		cells = append(cells, cellData)
		keys = append(keys, cellKey)
	}

	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, append([]byte(nil), newKey...))
	}

	return cells, keys, nil
}

// tryInsertNewCell inserts the new cell if its key is less than the current cell's key.
func (c *BtCursor) tryInsertNewCell(cells *[][]byte, keys *[]int64, newKey int64, newCellData []byte, idx int, page *BtreePage) bool {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return false
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return false
	}

	if newKey < cellInfo.Key {
		*cells = append(*cells, newCellData)
		*keys = append(*keys, newKey)
		return true
	}
	return false
}

// tryInsertNewCellComposite inserts the new cell based on composite key ordering.
func (c *BtCursor) tryInsertNewCellComposite(cells *[][]byte, keys *[][]byte, newKey []byte, newCellData []byte, idx int, page *BtreePage) bool {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return false
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return false
	}

	if bytes.Compare(newKey, cellInfo.KeyBytes) < 0 {
		*cells = append(*cells, newCellData)
		*keys = append(*keys, append([]byte(nil), newKey...))
		return true
	}
	return false
}

// copyExistingCell copies an existing cell from the page.
func (c *BtCursor) copyExistingCell(page *BtreePage, idx int) ([]byte, int64, error) {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return nil, 0, err
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return nil, 0, err
	}

	cellData := make([]byte, cellInfo.CellSize)
	copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
	return cellData, cellInfo.Key, nil
}

// copyExistingCellComposite copies a composite-key leaf cell.
func (c *BtCursor) copyExistingCellComposite(page *BtreePage, idx int) ([]byte, []byte, error) {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return nil, nil, err
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return nil, nil, err
	}

	cellData := make([]byte, cellInfo.CellSize)
	copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
	return cellData, append([]byte(nil), cellInfo.KeyBytes...), nil
}

// collectInteriorCellsForSplit collects all existing interior cells plus the new cell
func (c *BtCursor) collectInteriorCellsForSplit(page *BtreePage, newKey int64, newChildPgno uint32) ([][]byte, []int64, []uint32, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([]int64, 0, numCells+1)
	childPages := make([]uint32, 0, numCells+2)

	newCellData := EncodeTableInteriorCell(newChildPgno, newKey)
	inserted := false

	for i := 0; i < numCells; i++ {
		inserted = c.tryInsertInteriorCell(&cells, &keys, &childPages, newKey, newChildPgno, newCellData, inserted, page, i)

		cellData, cellKey, childPage, err := c.copyExistingInteriorCell(page, i)
		if err != nil {
			return nil, nil, nil, err
		}
		cells = append(cells, cellData)
		keys = append(keys, cellKey)
		childPages = append(childPages, childPage)
	}

	return c.finalizeInteriorCellCollection(cells, keys, childPages, newCellData, newKey, newChildPgno, page.Header.RightChild, inserted)
}

// collectInteriorCellsForSplitComposite collects interior cells for composite-key trees.
func (c *BtCursor) collectInteriorCellsForSplitComposite(page *BtreePage, newKey []byte, newChildPgno uint32) ([][]byte, [][]byte, []uint32, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([][]byte, 0, numCells+1)
	childPages := make([]uint32, 0, numCells+2)

	newCellData := EncodeTableInteriorCompositeCell(newChildPgno, newKey)
	inserted := false

	for i := 0; i < numCells; i++ {
		inserted = c.tryInsertInteriorCellComposite(&cells, &keys, &childPages, newKey, newChildPgno, newCellData, inserted, page, i)

		cellData, cellKey, childPage, err := c.copyExistingInteriorCellComposite(page, i)
		if err != nil {
			return nil, nil, nil, err
		}
		cells = append(cells, cellData)
		keys = append(keys, cellKey)
		childPages = append(childPages, childPage)
	}

	return c.finalizeInteriorCellCollectionComposite(cells, keys, childPages, newCellData, newKey, newChildPgno, page.Header.RightChild, inserted)
}

// tryInsertInteriorCell attempts to insert the new interior cell in sorted position.
func (c *BtCursor) tryInsertInteriorCell(cells *[][]byte, keys *[]int64, childPages *[]uint32, newKey int64, newChildPgno uint32, newCellData []byte, inserted bool, page *BtreePage, idx int) bool {
	if inserted {
		return true
	}

	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return false
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return false
	}

	if newKey < cellInfo.Key {
		*cells = append(*cells, newCellData)
		*keys = append(*keys, newKey)
		*childPages = append(*childPages, newChildPgno)
		return true
	}
	return false
}

// tryInsertInteriorCellComposite inserts a composite-key interior cell at the correct position.
func (c *BtCursor) tryInsertInteriorCellComposite(cells *[][]byte, keys *[][]byte, childPages *[]uint32, newKey []byte, newChildPgno uint32, newCellData []byte, inserted bool, page *BtreePage, idx int) bool {
	if inserted {
		return true
	}

	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return false
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return false
	}

	if bytes.Compare(newKey, cellInfo.KeyBytes) < 0 {
		*cells = append(*cells, newCellData)
		*keys = append(*keys, append([]byte(nil), newKey...))
		*childPages = append(*childPages, newChildPgno)
		return true
	}
	return false
}

// copyExistingInteriorCell copies an existing interior cell from the page.
func (c *BtCursor) copyExistingInteriorCell(page *BtreePage, idx int) ([]byte, int64, uint32, error) {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return nil, 0, 0, err
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return nil, 0, 0, err
	}

	cellData := make([]byte, cellInfo.CellSize)
	copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
	return cellData, cellInfo.Key, cellInfo.ChildPage, nil
}

// copyExistingInteriorCellComposite copies an interior composite cell.
func (c *BtCursor) copyExistingInteriorCellComposite(page *BtreePage, idx int) ([]byte, []byte, uint32, error) {
	cellPtr, err := page.Header.GetCellPointer(page.Data, idx)
	if err != nil {
		return nil, nil, 0, err
	}

	cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
	if err != nil {
		return nil, nil, 0, err
	}

	cellData := make([]byte, cellInfo.CellSize)
	copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
	return cellData, append([]byte(nil), cellInfo.KeyBytes...), cellInfo.ChildPage, nil
}

// finalizeInteriorCellCollection finalizes the collection by adding the rightmost child and potentially the new cell.
func (c *BtCursor) finalizeInteriorCellCollection(cells [][]byte, keys []int64, childPages []uint32, newCellData []byte, newKey int64, newChildPgno uint32, rightChild uint32, inserted bool) ([][]byte, []int64, []uint32, error) {
	childPages = append(childPages, rightChild)

	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, newKey)
		childPages = append(childPages[:len(childPages)-1], newChildPgno, rightChild)
	}

	return cells, keys, childPages, nil
}

// finalizeInteriorCellCollectionComposite finalizes composite-key interior cell collection.
func (c *BtCursor) finalizeInteriorCellCollectionComposite(cells [][]byte, keys [][]byte, childPages []uint32, newCellData []byte, newKey []byte, newChildPgno uint32, rightChild uint32, inserted bool) ([][]byte, [][]byte, []uint32, error) {
	childPages = append(childPages, rightChild)

	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, append([]byte(nil), newKey...))
		childPages = append(childPages[:len(childPages)-1], newChildPgno, rightChild)
	}

	return cells, keys, childPages, nil
}

// updateParentAfterSplit updates the parent page after a split
// If the current page is root, creates a new root
func (c *BtCursor) updateParentAfterSplit(leftPage, rightPage uint32, dividerKey int64, dividerKeyBytes []byte) error {
	if c.Depth == 0 || leftPage == c.RootPage {
		return c.createNewRoot(leftPage, rightPage, dividerKey, dividerKeyBytes)
	}

	parentDepth := c.Depth - 1
	parentPage := c.PageStack[parentDepth]

	parent, dividerCell, err := c.loadParentAndCreateDivider(parentPage, leftPage, dividerKey, dividerKeyBytes)
	if err != nil {
		return err
	}

	if len(dividerCell) > parent.FreeSpace() {
		return c.splitParentRecursively(parentPage, parentDepth, dividerKey, dividerKeyBytes, leftPage, parent)
	}

	return c.insertDividerIntoParent(parent, parentPage, rightPage, dividerKey, dividerKeyBytes, dividerCell)
}

// loadParentAndCreateDivider loads the parent page and creates the divider cell.
func (c *BtCursor) loadParentAndCreateDivider(parentPage, leftPage uint32, dividerKey int64, dividerKeyBytes []byte) (*BtreePage, []byte, error) {
	parentData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get parent page: %w", err)
	}

	parent, err := NewBtreePage(parentPage, parentData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parent BtreePage: %w", err)
	}

	var dividerCell []byte
	if c.CompositePK {
		dividerCell = EncodeTableInteriorCompositeCell(leftPage, dividerKeyBytes)
	} else {
		dividerCell = EncodeTableInteriorCell(leftPage, dividerKey)
	}
	return parent, dividerCell, nil
}

// splitParentRecursively splits the parent page when it's full.
func (c *BtCursor) splitParentRecursively(parentPage uint32, parentDepth int, dividerKey int64, dividerKeyBytes []byte, leftPage uint32, parent *BtreePage) error {
	savedPage, savedIdx, savedDepth, savedHeader := c.saveCursorState()
	c.positionOnParent(parentPage, parentDepth, parent)
	err := c.splitInteriorPage(dividerKey, dividerKeyBytes, leftPage)
	c.restoreCursorState(savedPage, savedIdx, savedDepth, savedHeader)
	return err
}

// saveCursorState saves the current cursor state for restoration.
func (c *BtCursor) saveCursorState() (uint32, int, int, *PageHeader) {
	return c.CurrentPage, c.CurrentIndex, c.Depth, c.CurrentHeader
}

// restoreCursorState restores a previously saved cursor state.
func (c *BtCursor) restoreCursorState(savedPage uint32, savedIndex, savedDepth int, savedHeader *PageHeader) {
	c.CurrentPage = savedPage
	c.CurrentIndex = savedIndex
	c.Depth = savedDepth
	c.CurrentHeader = savedHeader
}

// positionOnParent positions the cursor on the parent page.
func (c *BtCursor) positionOnParent(parentPage uint32, parentDepth int, parent *BtreePage) {
	c.CurrentPage = parentPage
	c.Depth = parentDepth
	c.CurrentHeader = parent.Header
}

// insertDividerIntoParent inserts the divider cell into the parent page.
// After inserting the divider at insertIdx, the cell at insertIdx+1 (which was
// the old cell at insertIdx before shifting) must have its child pointer updated
// to point to the right page of the split. If the divider was appended at the
// end, the page's right-child pointer is updated instead.
func (c *BtCursor) insertDividerIntoParent(parent *BtreePage, parentPage, rightPage uint32, dividerKey int64, dividerKeyBytes []byte, dividerCell []byte) error {
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(parentPage); err != nil {
			return err
		}
	}

	insertIdx := c.findInsertionPoint(parent, dividerKey, dividerKeyBytes)

	if err := parent.InsertCell(insertIdx, dividerCell); err != nil {
		return fmt.Errorf("failed to insert divider into parent: %w", err)
	}

	return c.fixChildPointerAfterSplit(parent, parentPage, rightPage, insertIdx)
}

// fixChildPointerAfterSplit updates the child pointer that follows the newly
// inserted divider cell so it points to the right page of the split.
func (c *BtCursor) fixChildPointerAfterSplit(parent *BtreePage, parentPage, rightPage uint32, insertIdx int) error {
	numCells := int(parent.Header.NumCells)
	nextIdx := insertIdx + 1
	if nextIdx < numCells {
		// Update the left-child pointer of the next cell to rightPage.
		cellPtr, err := parent.Header.GetCellPointer(parent.Data, nextIdx)
		if err != nil {
			return fmt.Errorf("failed to get cell pointer after split: %w", err)
		}
		binary.BigEndian.PutUint32(parent.Data[cellPtr:], rightPage)
	} else {
		// Divider was appended at the end; update the page's right-child pointer.
		headerOffset := getHeaderOffset(parentPage)
		binary.BigEndian.PutUint32(parent.Data[headerOffset+PageHeaderOffsetRightChild:], rightPage)
	}
	return nil
}

// findInsertionPoint finds the correct insertion point for the divider key.
func (c *BtCursor) findInsertionPoint(parent *BtreePage, dividerKey int64, dividerKeyBytes []byte) int {
	for i := 0; i < int(parent.Header.NumCells); i++ {
		cellPtr, err := parent.Header.GetCellPointer(parent.Data, i)
		if err != nil {
			return i
		}

		cellInfo, err := ParseCell(parent.Header.PageType, parent.Data[cellPtr:], parent.UsableSize)
		if err != nil {
			return i
		}

		comp := c.compareKeys(dividerKey, dividerKeyBytes, cellInfo.Key, cellInfo.KeyBytes)
		if comp < 0 {
			return i
		}
	}
	return int(parent.Header.NumCells)
}

// createNewRoot creates a new root page after splitting the old root
func (c *BtCursor) createNewRoot(leftPage, rightPage uint32, dividerKey int64, dividerKeyBytes []byte) error {
	newRootNum, err := c.Btree.AllocatePage()
	if err != nil {
		return err
	}
	newRootData, err := c.Btree.GetPage(newRootNum)
	if err != nil {
		return err
	}
	pageType := byte(PageTypeInteriorTable)
	if c.CompositePK {
		pageType = byte(PageTypeInteriorTableNo)
	}
	if err := initializeInteriorPage(newRootData, newRootNum, c.Btree.UsableSize, pageType); err != nil {
		return err
	}
	if c.Btree.Provider != nil {
		_ = c.Btree.Provider.MarkDirty(newRootNum)
	}
	newRoot, err := NewBtreePage(newRootNum, newRootData, c.Btree.UsableSize)
	if err != nil {
		return err
	}
	if err := c.populateNewRoot(newRoot, newRootNum, leftPage, rightPage, dividerKey, dividerKeyBytes); err != nil {
		return err
	}

	c.RootPage = newRootNum
	return nil
}


// populateNewRoot inserts the divider cell and sets right child.
func (c *BtCursor) populateNewRoot(newRoot *BtreePage, newRootNum, leftPage, rightPage uint32, dividerKey int64, dividerKeyBytes []byte) error {
	var dividerCell []byte
	if c.CompositePK {
		dividerCell = EncodeTableInteriorCompositeCell(leftPage, dividerKeyBytes)
	} else {
		dividerCell = EncodeTableInteriorCell(leftPage, dividerKey)
	}
	if err := newRoot.InsertCell(0, dividerCell); err != nil {
		return fmt.Errorf("failed to insert divider into new root: %w", err)
	}

	headerOffset := getHeaderOffset(newRootNum)
	binary.BigEndian.PutUint32(newRoot.Data[headerOffset+PageHeaderOffsetRightChild:], rightPage)
	return nil
}

// Helper functions

// initializeLeafPage initializes a page as an empty leaf table page
func initializeLeafPage(pageData []byte, pageNum uint32, usableSize uint32, pageType byte) error {
	headerOffset := getHeaderOffset(pageNum)

	// Avoid clobbering an existing populated page (e.g., root) during reuse.
	if pageData[headerOffset+PageHeaderOffsetNumCells] != 0 || pageData[headerOffset+PageHeaderOffsetNumCells+1] != 0 {
		return nil
	}

	// Set page type to leaf table
	pageData[headerOffset+PageHeaderOffsetType] = pageType

	// Initialize header fields
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetFreeblock:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetCellStart:], 0)
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0

	return nil
}

// initializeInteriorPage initializes a page as an empty interior table page
func initializeInteriorPage(pageData []byte, pageNum uint32, usableSize uint32, pageType byte) error {
	headerOffset := getHeaderOffset(pageNum)

	// Avoid clobbering an existing populated page (e.g., root) during reuse.
	if pageData[headerOffset+PageHeaderOffsetNumCells] != 0 || pageData[headerOffset+PageHeaderOffsetNumCells+1] != 0 {
		return nil
	}

	// Set page type to interior table
	pageData[headerOffset+PageHeaderOffsetType] = pageType

	// Initialize header fields
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetFreeblock:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetCellStart:], 0)
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0
	binary.BigEndian.PutUint32(pageData[headerOffset+PageHeaderOffsetRightChild:], 0)

	return nil
}

// getHeaderOffset returns the offset where the page header starts
func getHeaderOffset(pageNum uint32) int {
	if pageNum == 1 {
		return FileHeaderSize
	}
	return 0
}

// clearPageCells removes all cells from a page, resetting it to empty
func clearPageCells(page *BtreePage) error {
	headerOffset := getHeaderOffset(page.PageNum)

	// Reset cell count
	binary.BigEndian.PutUint16(page.Data[headerOffset+PageHeaderOffsetNumCells:], 0)
	page.Header.NumCells = 0

	// Reset cell content start (0 means end of usable space)
	binary.BigEndian.PutUint16(page.Data[headerOffset+PageHeaderOffsetCellStart:], 0)
	page.Header.CellContentStart = 0

	// Reset fragmented bytes
	page.Data[headerOffset+PageHeaderOffsetFragmented] = 0
	page.Header.FragmentedBytes = 0

	return nil
}
