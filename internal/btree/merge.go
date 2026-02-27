package btree

import (
	"encoding/binary"
	"fmt"
)

// MergePage attempts to merge an underfull page with a sibling or redistribute cells
// The cursor should be positioned at any cell on the underfull page
// Returns true if a merge/redistribution was performed
func (c *BtCursor) MergePage() (bool, error) {
	if c.State != CursorValid {
		return false, fmt.Errorf("cursor not in valid state")
	}

	if c.Depth == 0 {
		// Root page - can't merge
		return false, nil
	}

	// Get current page info
	currentPage := c.CurrentPage
	currentPageData, err := c.Btree.GetPage(currentPage)
	if err != nil {
		return false, err
	}
	currentHeader, err := ParsePageHeader(currentPageData, currentPage)
	if err != nil {
		return false, err
	}

	// Only merge leaf pages (for now - interior page merging is more complex)
	if !currentHeader.IsLeaf {
		return false, nil
	}

	// Get parent page info
	parentDepth := c.Depth - 1
	parentPage := c.PageStack[parentDepth]
	parentIndex := c.IndexStack[parentDepth]

	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return false, err
	}
	parentHeader, err := ParsePageHeader(parentPageData, parentPage)
	if err != nil {
		return false, err
	}

	// Try to get left sibling first
	var leftPage, rightPage uint32
	var leftIsOurs bool

	if parentIndex > 0 {
		// We have a left sibling
		leftPage, err = c.getChildPageAt(parentPageData, parentHeader, parentIndex-1)
		if err != nil {
			return false, err
		}
		rightPage = currentPage
		leftIsOurs = false
	} else if parentIndex < int(parentHeader.NumCells) {
		// We are the leftmost child, use right sibling
		leftPage = currentPage
		if parentIndex == int(parentHeader.NumCells)-1 {
			// Use right child pointer
			rightPage = parentHeader.RightChild
		} else {
			rightPage, err = c.getChildPageAt(parentPageData, parentHeader, parentIndex+1)
			if err != nil {
				return false, err
			}
		}
		leftIsOurs = true
	} else {
		// We are the rightmost child (from right child pointer)
		// Use the last cell's child as left sibling
		leftPage, err = c.getChildPageAt(parentPageData, parentHeader, int(parentHeader.NumCells)-1)
		if err != nil {
			return false, err
		}
		rightPage = currentPage
		leftIsOurs = false
	}

	// Load sibling pages
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return false, err
	}
	leftHeader, err := ParsePageHeader(leftPageData, leftPage)
	if err != nil {
		return false, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return false, err
	}
	rightHeader, err := ParsePageHeader(rightPageData, rightPage)
	if err != nil {
		return false, err
	}

	// Check if we can merge
	canMerge, err := CanMerge(leftPageData, leftHeader, rightPageData, rightHeader, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	if canMerge {
		// Perform merge
		return c.mergePages(leftPage, rightPage, parentPage, parentIndex, leftIsOurs)
	}

	// Otherwise, redistribute cells
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
	// Mark all pages as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(leftPage); err != nil {
			return false, err
		}
		if err := c.Btree.Provider.MarkDirty(rightPage); err != nil {
			return false, err
		}
		if err := c.Btree.Provider.MarkDirty(parentPage); err != nil {
			return false, err
		}
	}

	// Load pages
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return false, err
	}
	leftBtreePage, err := NewBtreePage(leftPage, leftPageData, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return false, err
	}
	rightHeader, err := ParsePageHeader(rightPageData, rightPage)
	if err != nil {
		return false, err
	}

	// Copy all cells from right page to left page
	for i := 0; i < int(rightHeader.NumCells); i++ {
		cellOffset, err := rightHeader.GetCellPointer(rightPageData, i)
		if err != nil {
			return false, err
		}

		cell, err := ParseCell(rightHeader.PageType, rightPageData[cellOffset:], c.Btree.UsableSize)
		if err != nil {
			return false, err
		}

		// Extract the cell data
		cellData := rightPageData[cellOffset : cellOffset+uint16(cell.CellSize)]

		// Insert at end of left page
		if err := leftBtreePage.InsertCell(int(leftBtreePage.Header.NumCells), cellData); err != nil {
			return false, fmt.Errorf("failed to insert cell during merge: %w", err)
		}
	}

	// Remove the merged page's entry from parent
	parentPageData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return false, err
	}
	parentBtreePage, err := NewBtreePage(parentPage, parentPageData, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	// Determine which parent cell to remove
	cellToRemove := parentIndex
	if !leftIsOurs && parentIndex > 0 {
		cellToRemove = parentIndex - 1
	}

	// If we're removing the last cell, update the right child pointer
	if cellToRemove == int(parentBtreePage.Header.NumCells)-1 {
		// Update parent's right child pointer to point to the merged page
		parentHeader := parentBtreePage.Header
		if parentHeader.IsInterior {
			headerOffset := 0
			if parentPage == 1 {
				headerOffset = FileHeaderSize
			}
			binary.BigEndian.PutUint32(parentPageData[headerOffset+PageHeaderOffsetRightChild:], leftPage)
			parentBtreePage.Header.RightChild = leftPage
		}
	}

	// Delete the parent cell
	if err := parentBtreePage.DeleteCell(cellToRemove); err != nil {
		return false, fmt.Errorf("failed to delete parent cell during merge: %w", err)
	}

	// Free the right page
	delete(c.Btree.Pages, rightPage)

	// Invalidate cursor since page structure changed
	c.State = CursorInvalid

	return true, nil
}

// redistributeSiblings redistributes cells between two sibling pages to balance them
func (c *BtCursor) redistributeSiblings(leftPage, rightPage, parentPage uint32, parentIndex int) (bool, error) {
	// Mark all pages as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(leftPage); err != nil {
			return false, err
		}
		if err := c.Btree.Provider.MarkDirty(rightPage); err != nil {
			return false, err
		}
		if err := c.Btree.Provider.MarkDirty(parentPage); err != nil {
			return false, err
		}
	}

	// Load pages
	leftPageData, err := c.Btree.GetPage(leftPage)
	if err != nil {
		return false, err
	}
	leftBtreePage, err := NewBtreePage(leftPage, leftPageData, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	rightPageData, err := c.Btree.GetPage(rightPage)
	if err != nil {
		return false, err
	}
	rightBtreePage, err := NewBtreePage(rightPage, rightPageData, c.Btree.UsableSize)
	if err != nil {
		return false, err
	}

	err = RedistributeCells(leftBtreePage, rightBtreePage)
	if err != nil {
		return false, err
	}

	// Update the parent's separator key
	// The separator should be the first key of the right page
	if rightBtreePage.Header.NumCells > 0 {
		cellOffset, err := rightBtreePage.Header.GetCellPointer(rightBtreePage.Data, 0)
		if err != nil {
			return false, err
		}

		cell, err := ParseCell(rightBtreePage.Header.PageType, rightBtreePage.Data[cellOffset:], c.Btree.UsableSize)
		if err != nil {
			return false, err
		}

		// Update parent separator key
		parentPageData, err := c.Btree.GetPage(parentPage)
		if err != nil {
			return false, err
		}
		parentBtreePage, err := NewBtreePage(parentPage, parentPageData, c.Btree.UsableSize)
		if err != nil {
			return false, err
		}

		// Find the parent cell that separates these two pages
		separatorIndex := parentIndex
		if separatorIndex >= int(parentBtreePage.Header.NumCells) {
			separatorIndex = int(parentBtreePage.Header.NumCells) - 1
		}

		// Delete old separator and insert new one
		if separatorIndex >= 0 && separatorIndex < int(parentBtreePage.Header.NumCells) {
			if err := parentBtreePage.DeleteCell(separatorIndex); err != nil {
				return false, err
			}

			// Create new separator cell
			newSeparator := EncodeTableInteriorCell(leftPage, cell.Key)
			if err := parentBtreePage.InsertCell(separatorIndex, newSeparator); err != nil {
				return false, err
			}
		}
	}

	// Invalidate cursor since cell positions may have changed
	c.State = CursorInvalid

	return true, nil
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
	// Calculate how many cells each page should have
	totalCells := int(leftPage.Header.NumCells) + int(rightPage.Header.NumCells)
	targetLeft := totalCells / 2
	currentLeft := int(leftPage.Header.NumCells)

	if currentLeft == targetLeft {
		// Already balanced
		return nil
	}

	if currentLeft < targetLeft {
		// Move cells from right to left
		numToMove := targetLeft - currentLeft
		for i := 0; i < numToMove; i++ {
			if rightPage.Header.NumCells == 0 {
				break
			}

			// Get first cell from right page
			cellOffset, err := rightPage.Header.GetCellPointer(rightPage.Data, 0)
			if err != nil {
				return err
			}

			cell, err := ParseCell(rightPage.Header.PageType, rightPage.Data[cellOffset:], rightPage.UsableSize)
			if err != nil {
				return err
			}

			// Bounds check to prevent slice overflow
			cellEnd := int(cellOffset) + int(cell.CellSize)
			if cellEnd > len(rightPage.Data) {
				cellEnd = len(rightPage.Data)
			}
			cellData := rightPage.Data[cellOffset:cellEnd]

			// Insert at end of left page
			if err := leftPage.InsertCell(int(leftPage.Header.NumCells), cellData); err != nil {
				// If we can't insert, stop trying
				return err
			}

			// Delete from right page
			if err := rightPage.DeleteCell(0); err != nil {
				return err
			}
		}
	} else {
		// Move cells from left to right
		numToMove := currentLeft - targetLeft
		for i := 0; i < numToMove; i++ {
			if leftPage.Header.NumCells == 0 {
				break
			}

			// Get last cell from left page
			lastIndex := int(leftPage.Header.NumCells) - 1
			cellOffset, err := leftPage.Header.GetCellPointer(leftPage.Data, lastIndex)
			if err != nil {
				return err
			}

			cell, err := ParseCell(leftPage.Header.PageType, leftPage.Data[cellOffset:], leftPage.UsableSize)
			if err != nil {
				return err
			}

			// Bounds check to prevent slice overflow
			cellEnd := int(cellOffset) + int(cell.CellSize)
			if cellEnd > len(leftPage.Data) {
				cellEnd = len(leftPage.Data)
			}
			cellData := leftPage.Data[cellOffset:cellEnd]

			// Insert at beginning of right page
			if err := rightPage.InsertCell(0, cellData); err != nil {
				// If we can't insert, stop trying
				return err
			}

			// Delete from left page
			if err := leftPage.DeleteCell(lastIndex); err != nil {
				return err
			}
		}
	}

	// Defragment both pages to reclaim space
	if err := leftPage.Defragment(); err != nil {
		return err
	}
	if err := rightPage.Defragment(); err != nil {
		return err
	}

	return nil
}
