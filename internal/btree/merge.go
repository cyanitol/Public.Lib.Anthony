package btree

import (
	"encoding/binary"
	"fmt"
)

// MergePage attempts to merge an underfull page with a sibling or redistribute cells
// The cursor should be positioned at any cell on the underfull page
// Returns true if a merge/redistribution was performed
func (c *BtCursor) MergePage() (bool, error) {
	if !c.canMergePage() {
		return false, nil
	}

	currentPage, parentPage, parentIndex, err := c.getPageContext()
	if err != nil {
		return false, err
	}

	currentHeader, parentHeader, err := c.loadPageHeaders(currentPage, parentPage)
	if err != nil {
		return false, err
	}

	if !currentHeader.IsLeaf {
		return false, nil
	}

	leftPage, rightPage, leftIsOurs, err := c.findSiblingPages(currentPage, parentPage, parentIndex, parentHeader)
	if err != nil {
		return false, err
	}

	leftHeader, rightHeader, err := c.loadSiblingHeaders(leftPage, rightPage)
	if err != nil {
		return false, err
	}

	return c.mergeOrRedistribute(leftPage, rightPage, leftHeader, rightHeader, parentPage, parentIndex, leftIsOurs)
}

// canMergePage checks if the cursor is in a valid state for merging
func (c *BtCursor) canMergePage() bool {
	return c.State == CursorValid && c.Depth > 0
}

// getPageContext extracts the current page, parent page, and parent index
func (c *BtCursor) getPageContext() (currentPage, parentPage uint32, parentIndex int, err error) {
	currentPage = c.CurrentPage
	if _, err = c.Btree.GetPage(currentPage); err != nil {
		return 0, 0, 0, err
	}

	parentDepth := c.Depth - 1
	parentPage = c.PageStack[parentDepth]
	parentIndex = c.IndexStack[parentDepth]
	return currentPage, parentPage, parentIndex, nil
}

// loadPageHeaders loads and parses headers for current and parent pages
func (c *BtCursor) loadPageHeaders(currentPage, parentPage uint32) (*PageHeader, *PageHeader, error) {
	currentPageData, err := c.Btree.GetPage(currentPage)
	if err != nil {
		return nil, nil, err
	}
	currentHeader, err := ParsePageHeader(currentPageData, currentPage)
	if err != nil {
		return nil, nil, err
	}

	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return nil, nil, err
	}
	parentHeader, err := ParsePageHeader(parentPageData, parentPage)
	if err != nil {
		return nil, nil, err
	}

	return currentHeader, parentHeader, nil
}

// findSiblingPages determines the left and right sibling pages for merging
func (c *BtCursor) findSiblingPages(currentPage, parentPage uint32, parentIndex int, parentHeader *PageHeader) (leftPage, rightPage uint32, leftIsOurs bool, err error) {
	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return 0, 0, false, err
	}

	if parentIndex > 0 {
		return c.getSiblingWithLeftPage(currentPage, parentPageData, parentHeader, parentIndex)
	}
	if parentIndex < int(parentHeader.NumCells) {
		return c.getSiblingWithRightPage(currentPage, parentPageData, parentHeader, parentIndex)
	}
	return c.getSiblingAsRightmost(currentPage, parentPageData, parentHeader)
}

// getSiblingWithLeftPage handles case where current page has a left sibling
func (c *BtCursor) getSiblingWithLeftPage(currentPage uint32, parentPageData []byte, parentHeader *PageHeader, parentIndex int) (leftPage, rightPage uint32, leftIsOurs bool, err error) {
	leftPage, err = c.getChildPageAt(parentPageData, parentHeader, parentIndex-1)
	if err != nil {
		return 0, 0, false, err
	}
	return leftPage, currentPage, false, nil
}

// getSiblingWithRightPage handles case where current page is leftmost child
func (c *BtCursor) getSiblingWithRightPage(currentPage uint32, parentPageData []byte, parentHeader *PageHeader, parentIndex int) (leftPage, rightPage uint32, leftIsOurs bool, err error) {
	leftPage = currentPage
	if parentIndex == int(parentHeader.NumCells)-1 {
		rightPage = parentHeader.RightChild
	} else {
		rightPage, err = c.getChildPageAt(parentPageData, parentHeader, parentIndex+1)
		if err != nil {
			return 0, 0, false, err
		}
	}
	return leftPage, rightPage, true, nil
}

// getSiblingAsRightmost handles case where current page is rightmost child
func (c *BtCursor) getSiblingAsRightmost(currentPage uint32, parentPageData []byte, parentHeader *PageHeader) (leftPage, rightPage uint32, leftIsOurs bool, err error) {
	leftPage, err = c.getChildPageAt(parentPageData, parentHeader, int(parentHeader.NumCells)-1)
	if err != nil {
		return 0, 0, false, err
	}
	return leftPage, currentPage, false, nil
}

// loadSiblingHeaders loads and parses headers for sibling pages
func (c *BtCursor) loadSiblingHeaders(leftPage, rightPage uint32) (*PageHeader, *PageHeader, error) {
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return nil, nil, err
	}
	leftHeader, err := ParsePageHeader(leftPageData, leftPage)
	if err != nil {
		return nil, nil, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return nil, nil, err
	}
	rightHeader, err := ParsePageHeader(rightPageData, rightPage)
	if err != nil {
		return nil, nil, err
	}

	return leftHeader, rightHeader, nil
}

// mergeOrRedistribute decides whether to merge or redistribute cells
func (c *BtCursor) mergeOrRedistribute(leftPage, rightPage uint32, leftHeader, rightHeader *PageHeader, parentPage uint32, parentIndex int, leftIsOurs bool) (bool, error) {
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return false, err
	}
	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return false, err
	}

	canMerge, err := CanMerge(leftPageData, leftHeader, rightPageData, rightHeader, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	if canMerge {
		return c.mergePages(leftPage, rightPage, parentPage, parentIndex, leftIsOurs)
	}
	return c.redistributeSiblings(leftPage, rightPage, parentPage, parentIndex)
}

// getChildPageAt returns the child page number at the given index in an interior page
func (c *BtCursor) getChildPageAt(pageData []byte, header *PageHeader, index int) (uint32, error) {
	if index >= int(header.NumCells) {
		return header.RightChild, nil
	}

	cellOffset, err := header.GetCellPointer(pageData, index)
	if err != nil {
		return 0, err
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return 0, err
	}

	return cell.ChildPage, nil
}

// mergePages merges two sibling pages into one
func (c *BtCursor) mergePages(leftPage, rightPage, parentPage uint32, parentIndex int, leftIsOurs bool) (bool, error) {
	if err := c.markMergePagesAsDirty(leftPage, rightPage, parentPage); err != nil {
		return false, err
	}

	leftBtreePage, rightHeader, err := c.loadMergePages(leftPage, rightPage)
	if err != nil {
		return false, err
	}

	if err := c.copyRightCellsToLeft(leftBtreePage, rightHeader, rightPage); err != nil {
		return false, err
	}

	if err := c.updateParentAfterMerge(leftPage, parentPage, parentIndex, leftIsOurs); err != nil {
		return false, err
	}

	delete(c.Btree.Pages, rightPage)
	c.State = CursorInvalid
	return true, nil
}

// markMergePagesAsDirty marks the pages involved in merge as dirty
func (c *BtCursor) markMergePagesAsDirty(leftPage, rightPage, parentPage uint32) error {
	if c.Btree.Provider == nil {
		return nil
	}
	pages := []uint32{leftPage, rightPage, parentPage}
	for _, page := range pages {
		if err := c.Btree.Provider.MarkDirty(page); err != nil {
			return err
		}
	}
	return nil
}

// loadMergePages loads the left and right pages for merging
func (c *BtCursor) loadMergePages(leftPage, rightPage uint32) (*BtreePage, *PageHeader, error) {
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return nil, nil, err
	}
	leftBtreePage, err := NewBtreePage(leftPage, leftPageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return nil, nil, err
	}
	rightHeader, err := ParsePageHeader(rightPageData, rightPage)
	if err != nil {
		return nil, nil, err
	}

	return leftBtreePage, rightHeader, nil
}

// copyRightCellsToLeft copies all cells from right page to left page
func (c *BtCursor) copyRightCellsToLeft(leftBtreePage *BtreePage, rightHeader *PageHeader, rightPage uint32) error {
	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return err
	}

	for i := 0; i < int(rightHeader.NumCells); i++ {
		cellData, err := c.extractCellData(rightPageData, rightHeader, i)
		if err != nil {
			return err
		}

		if err := leftBtreePage.InsertCell(int(leftBtreePage.Header.NumCells), cellData); err != nil {
			return fmt.Errorf("failed to insert cell during merge: %w", err)
		}
	}
	return nil
}

// extractCellData extracts cell data at the given index
func (c *BtCursor) extractCellData(pageData []byte, header *PageHeader, index int) ([]byte, error) {
	cellOffset, err := header.GetCellPointer(pageData, index)
	if err != nil {
		return nil, err
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return nil, err
	}

	return pageData[cellOffset : cellOffset+uint16(cell.CellSize)], nil
}

// updateParentAfterMerge updates the parent page after merging children
func (c *BtCursor) updateParentAfterMerge(leftPage, parentPage uint32, parentIndex int, leftIsOurs bool) error {
	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return err
	}
	parentBtreePage, err := NewBtreePage(parentPage, parentPageData, c.Btree.UsableSize)
	if err != nil {
		return err
	}

	cellToRemove := c.determineParentCellToRemove(parentIndex, leftIsOurs)

	if err := c.updateParentRightChildIfNeeded(parentBtreePage, parentPage, leftPage, cellToRemove); err != nil {
		return err
	}

	if err := parentBtreePage.DeleteCell(cellToRemove); err != nil {
		return fmt.Errorf("failed to delete parent cell during merge: %w", err)
	}

	return nil
}

// determineParentCellToRemove determines which parent cell should be removed
func (c *BtCursor) determineParentCellToRemove(parentIndex int, leftIsOurs bool) int {
	if !leftIsOurs && parentIndex > 0 {
		return parentIndex - 1
	}
	return parentIndex
}

// updateParentRightChildIfNeeded updates parent's right child pointer if removing last cell
func (c *BtCursor) updateParentRightChildIfNeeded(parentBtreePage *BtreePage, parentPage, leftPage uint32, cellToRemove int) error {
	if cellToRemove != int(parentBtreePage.Header.NumCells)-1 {
		return nil
	}

	parentHeader := parentBtreePage.Header
	if !parentHeader.IsInterior {
		return nil
	}

	headerOffset := 0
	if parentPage == 1 {
		headerOffset = FileHeaderSize
	}

	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(parentPageData[headerOffset+PageHeaderOffsetRightChild:], leftPage)
	parentBtreePage.Header.RightChild = leftPage
	return nil
}

// redistributeSiblings redistributes cells between two sibling pages to balance them
func (c *BtCursor) redistributeSiblings(leftPage, rightPage, parentPage uint32, parentIndex int) (bool, error) {
	if err := c.markMergePagesAsDirty(leftPage, rightPage, parentPage); err != nil {
		return false, err
	}

	leftBtreePage, rightBtreePage, err := c.loadRedistributePages(leftPage, rightPage)
	if err != nil {
		return false, err
	}

	if err := RedistributeCells(leftBtreePage, rightBtreePage); err != nil {
		return false, err
	}

	if err := c.updateParentSeparator(rightBtreePage, leftPage, parentPage, parentIndex); err != nil {
		return false, err
	}

	c.State = CursorInvalid
	return true, nil
}

// loadRedistributePages loads both pages for redistribution
func (c *BtCursor) loadRedistributePages(leftPage, rightPage uint32) (*BtreePage, *BtreePage, error) {
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return nil, nil, err
	}
	leftBtreePage, err := NewBtreePage(leftPage, leftPageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return nil, nil, err
	}
	rightBtreePage, err := NewBtreePage(rightPage, rightPageData, c.Btree.UsableSize)
	if err != nil {
		return nil, nil, err
	}

	return leftBtreePage, rightBtreePage, nil
}

// updateParentSeparator updates the parent's separator key after redistribution
func (c *BtCursor) updateParentSeparator(rightBtreePage *BtreePage, leftPage, parentPage uint32, parentIndex int) error {
	if rightBtreePage.Header.NumCells == 0 {
		return nil
	}

	firstKey, err := c.getFirstKeyFromPage(rightBtreePage)
	if err != nil {
		return err
	}

	parentBtreePage, err := c.loadParentBtreePage(parentPage)
	if err != nil {
		return err
	}

	separatorIndex := c.calculateSeparatorIndex(parentIndex, parentBtreePage)
	return c.replaceSeparatorCell(parentBtreePage, separatorIndex, leftPage, firstKey)
}

// getFirstKeyFromPage extracts the first key from a page
func (c *BtCursor) getFirstKeyFromPage(btreePage *BtreePage) (int64, error) {
	cellOffset, err := btreePage.Header.GetCellPointer(btreePage.Data, 0)
	if err != nil {
		return 0, err
	}

	cell, err := ParseCell(btreePage.Header.PageType, btreePage.Data[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return 0, err
	}

	return cell.Key, nil
}

// loadParentBtreePage loads the parent page as a BtreePage for updating
func (c *BtCursor) loadParentBtreePage(parentPage uint32) (*BtreePage, error) {
	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return nil, err
	}
	return NewBtreePage(parentPage, parentPageData, c.Btree.UsableSize)
}

// calculateSeparatorIndex determines the separator index in the parent
func (c *BtCursor) calculateSeparatorIndex(parentIndex int, parentBtreePage *BtreePage) int {
	separatorIndex := parentIndex
	if separatorIndex >= int(parentBtreePage.Header.NumCells) {
		separatorIndex = int(parentBtreePage.Header.NumCells) - 1
	}
	return separatorIndex
}

// replaceSeparatorCell replaces the separator cell in the parent
func (c *BtCursor) replaceSeparatorCell(parentBtreePage *BtreePage, separatorIndex int, leftPage uint32, key int64) error {
	if separatorIndex < 0 || separatorIndex >= int(parentBtreePage.Header.NumCells) {
		return nil
	}

	if err := parentBtreePage.DeleteCell(separatorIndex); err != nil {
		return err
	}

	newSeparator := EncodeTableInteriorCell(leftPage, key)
	return parentBtreePage.InsertCell(separatorIndex, newSeparator)
}

// CanMerge checks if two pages can be merged into one
// Returns true if the combined cells would fit in a single page
func CanMerge(leftPageData []byte, leftHeader *PageHeader, rightPageData []byte, rightHeader *PageHeader, usableSize uint32) (bool, error) {
	// Both pages must be the same type
	if leftHeader.PageType != rightHeader.PageType {
		return false, nil
	}

	// Both must be leaf pages (interior page merging is more complex)
	if !leftHeader.IsLeaf || !rightHeader.IsLeaf {
		return false, nil
	}

	// Calculate total cells
	totalCells := leftHeader.NumCells + rightHeader.NumCells

	// Calculate total cell content size
	var totalContentSize int

	// Calculate left page cell content size
	for i := 0; i < int(leftHeader.NumCells); i++ {
		cellOffset, err := leftHeader.GetCellPointer(leftPageData, i)
		if err != nil {
			return false, err
		}

		cell, err := ParseCell(leftHeader.PageType, leftPageData[cellOffset:], usableSize)
		if err != nil {
			return false, err
		}

		totalContentSize += int(cell.CellSize)
	}

	// Calculate right page cell content size
	for i := 0; i < int(rightHeader.NumCells); i++ {
		cellOffset, err := rightHeader.GetCellPointer(rightPageData, i)
		if err != nil {
			return false, err
		}

		cell, err := ParseCell(rightHeader.PageType, rightPageData[cellOffset:], usableSize)
		if err != nil {
			return false, err
		}

		totalContentSize += int(cell.CellSize)
	}

	// Calculate space needed
	// Header size + cell pointer array + cell content
	headerSize := leftHeader.HeaderSize
	cellPointerSize := int(totalCells) * 2
	totalNeeded := headerSize + cellPointerSize + totalContentSize

	// Add some margin for safety (10%)
	totalNeeded = totalNeeded * 110 / 100

	return totalNeeded <= int(usableSize), nil
}

// RedistributeCells redistributes cells between two pages to balance them
// Moves cells from the fuller page to the emptier page until balanced
func RedistributeCells(leftPage, rightPage *BtreePage) error {
	totalCells := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	targetLeft := totalCells / 2
	currentLeft := int(leftPage.Header.NumCells)

	if currentLeft == targetLeft {
		return nil
	}

	if currentLeft < targetLeft {
		if err := moveRightToLeft(leftPage, rightPage, targetLeft-currentLeft); err != nil {
			return err
		}
	} else {
		if err := moveLeftToRight(leftPage, rightPage, currentLeft-targetLeft); err != nil {
			return err
		}
	}

	return defragmentPages(leftPage, rightPage)
}

// moveRightToLeft moves cells from right page to left page
func moveRightToLeft(leftPage, rightPage *BtreePage, numToMove int) error {
	for i := 0; i < numToMove; i++ {
		if rightPage.Header.NumCells == 0 {
			break
		}

		cellData, err := extractCellFromPage(rightPage, 0)
		if err != nil {
			return err
		}

		if err := leftPage.InsertCell(int(leftPage.Header.NumCells), cellData); err != nil {
			return err
		}

		if err := rightPage.DeleteCell(0); err != nil {
			return err
		}
	}
	return nil
}

// moveLeftToRight moves cells from left page to right page
func moveLeftToRight(leftPage, rightPage *BtreePage, numToMove int) error {
	for i := 0; i < numToMove; i++ {
		if leftPage.Header.NumCells == 0 {
			break
		}

		lastIndex := int(leftPage.Header.NumCells) - 1
		cellData, err := extractCellFromPage(leftPage, lastIndex)
		if err != nil {
			return err
		}

		if err := rightPage.InsertCell(0, cellData); err != nil {
			return err
		}

		if err := leftPage.DeleteCell(lastIndex); err != nil {
			return err
		}
	}
	return nil
}

// extractCellFromPage extracts a cell's data from a page at the given index
func extractCellFromPage(page *BtreePage, index int) ([]byte, error) {
	cellOffset, err := page.Header.GetCellPointer(page.Data, index)
	if err != nil {
		return nil, err
	}

	cell, err := ParseCell(page.Header.PageType, page.Data[cellOffset:], page.UsableSize)
	if err != nil {
		return nil, err
	}

	cellEnd := int(cellOffset) + int(cell.CellSize)
	if cellEnd > len(page.Data) {
		cellEnd = len(page.Data)
	}

	return page.Data[cellOffset:cellEnd], nil
}

// defragmentPages defragments both pages to reclaim space
func defragmentPages(leftPage, rightPage *BtreePage) error {
	if err := leftPage.Defragment(); err != nil {
		return err
	}
	return rightPage.Defragment()
}
