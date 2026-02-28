package btree

import (
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
func (c *BtCursor) splitLeafPage(key int64, payload []byte) error {
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("splitLeafPage called on non-leaf page")
	}

	// Get current page data
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return fmt.Errorf("failed to get current page: %w", err)
	}

	// Create BtreePage wrapper for current page
	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create BtreePage: %w", err)
	}

	// Collect all cells including the new one
	cells, keys, err := c.collectLeafCellsForSplit(oldPage, key, payload)
	if err != nil {
		return fmt.Errorf("failed to collect cells: %w", err)
	}

	// Find median index
	medianIdx := len(cells) / 2

	// Allocate new page
	newPageNum, err := c.Btree.AllocatePage()
	if err != nil {
		return fmt.Errorf("failed to allocate new page: %w", err)
	}

	// Get and initialize new page
	newPageData, err := c.Btree.GetPage(newPageNum)
	if err != nil {
		return fmt.Errorf("failed to get new page: %w", err)
	}

	// Initialize new page header
	if err := initializeLeafPage(newPageData, newPageNum, c.Btree.UsableSize); err != nil {
		return fmt.Errorf("failed to initialize new page: %w", err)
	}

	// Mark both pages as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(c.CurrentPage); err != nil {
			return err
		}
		if err := c.Btree.Provider.MarkDirty(newPageNum); err != nil {
			return err
		}
	}

	// Clear old page (we'll rebuild it)
	if err := clearPageCells(oldPage); err != nil {
		return fmt.Errorf("failed to clear old page: %w", err)
	}

	// Create new page wrapper
	newPage, err := NewBtreePage(newPageNum, newPageData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create new BtreePage: %w", err)
	}

	// Distribute cells: left page gets cells [0, medianIdx), right page gets [medianIdx, end)
	for i := 0; i < medianIdx; i++ {
		if err := oldPage.InsertCell(i, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into left page: %w", i, err)
		}
	}

	for i := medianIdx; i < len(cells); i++ {
		if err := newPage.InsertCell(i-medianIdx, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into right page: %w", i, err)
		}
	}

	// Defragment both pages
	if err := oldPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment left page: %w", err)
	}
	if err := newPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment right page: %w", err)
	}

	// The divider key is the first key in the right page
	dividerKey := keys[medianIdx]

	// Update parent page
	if err := c.updateParentAfterSplit(c.CurrentPage, newPageNum, dividerKey); err != nil {
		return fmt.Errorf("failed to update parent: %w", err)
	}

	// Reposition cursor to the newly inserted key
	_, err = c.SeekRowid(key)
	return err
}

// splitInteriorPage splits a full interior page when inserting a new cell
func (c *BtCursor) splitInteriorPage(key int64, childPgno uint32) error {
	if c.CurrentHeader == nil || !c.CurrentHeader.IsInterior {
		return fmt.Errorf("splitInteriorPage called on non-interior page")
	}

	// Get current page data
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return fmt.Errorf("failed to get current page: %w", err)
	}

	// Create BtreePage wrapper for current page
	oldPage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create BtreePage: %w", err)
	}

	// Collect all cells including the new one
	cells, keys, childPages, err := c.collectInteriorCellsForSplit(oldPage, key, childPgno)
	if err != nil {
		return fmt.Errorf("failed to collect cells: %w", err)
	}

	// Find median index
	medianIdx := len(cells) / 2

	// Allocate new page
	newPageNum, err := c.Btree.AllocatePage()
	if err != nil {
		return fmt.Errorf("failed to allocate new page: %w", err)
	}

	// Get and initialize new page
	newPageData, err := c.Btree.GetPage(newPageNum)
	if err != nil {
		return fmt.Errorf("failed to get new page: %w", err)
	}

	// Initialize new page header as interior
	if err := initializeInteriorPage(newPageData, newPageNum, c.Btree.UsableSize); err != nil {
		return fmt.Errorf("failed to initialize new page: %w", err)
	}

	// Mark both pages as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(c.CurrentPage); err != nil {
			return err
		}
		if err := c.Btree.Provider.MarkDirty(newPageNum); err != nil {
			return err
		}
	}

	// Clear old page
	if err := clearPageCells(oldPage); err != nil {
		return fmt.Errorf("failed to clear old page: %w", err)
	}

	// Create new page wrapper
	newPage, err := NewBtreePage(newPageNum, newPageData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create new BtreePage: %w", err)
	}

	// Distribute cells
	// Left page gets [0, medianIdx), median key goes to parent, right page gets [medianIdx+1, end)
	for i := 0; i < medianIdx; i++ {
		if err := oldPage.InsertCell(i, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into left page: %w", i, err)
		}
	}

	// Set right child of left page to the left child of the median cell
	headerOffset := getHeaderOffset(c.CurrentPage)
	binary.BigEndian.PutUint32(oldPage.Data[headerOffset+PageHeaderOffsetRightChild:], childPages[medianIdx])

	for i := medianIdx + 1; i < len(cells); i++ {
		if err := newPage.InsertCell(i-medianIdx-1, cells[i]); err != nil {
			return fmt.Errorf("failed to insert cell %d into right page: %w", i, err)
		}
	}

	// Set right child of new page
	newHeaderOffset := getHeaderOffset(newPageNum)
	if medianIdx+1 < len(childPages) {
		binary.BigEndian.PutUint32(newPage.Data[newHeaderOffset+PageHeaderOffsetRightChild:], childPages[len(childPages)-1])
	}

	// Defragment both pages
	if err := oldPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment left page: %w", err)
	}
	if err := newPage.Defragment(); err != nil {
		return fmt.Errorf("failed to defragment right page: %w", err)
	}

	// The divider key is the median key
	dividerKey := keys[medianIdx]

	// Update parent page
	if err := c.updateParentAfterSplit(c.CurrentPage, newPageNum, dividerKey); err != nil {
		return fmt.Errorf("failed to update parent: %w", err)
	}

	return nil
}

// collectLeafCellsForSplit collects all existing cells plus the new cell to be inserted
// Returns cells in sorted order by key
// Properly handles overflow pages when encoding the new cell
func (c *BtCursor) collectLeafCellsForSplit(page *BtreePage, newKey int64, newPayload []byte) ([][]byte, []int64, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([]int64, 0, numCells+1)

	// Encode new cell with proper overflow handling
	var newCellData []byte
	payloadSize := uint32(len(newPayload))
	localSize := CalculateLocalPayload(payloadSize, c.Btree.UsableSize, true)

	if payloadSize > uint32(localSize) {
		// Need overflow pages for new cell
		overflowPage, err := c.WriteOverflow(newPayload, localSize, c.Btree.UsableSize)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to write overflow for split: %w", err)
		}
		newCellData = c.encodeTableLeafCellWithOverflow(newKey, newPayload[:localSize], overflowPage, payloadSize)
	} else {
		// No overflow needed
		newCellData = EncodeTableLeafCell(newKey, newPayload)
	}

	inserted := false

	for i := 0; i < numCells; i++ {
		cellPtr, err := page.Header.GetCellPointer(page.Data, i)
		if err != nil {
			return nil, nil, err
		}

		cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
		if err != nil {
			return nil, nil, err
		}

		// Insert new cell in sorted position
		if !inserted && newKey < cellInfo.Key {
			cells = append(cells, newCellData)
			keys = append(keys, newKey)
			inserted = true
		}

		// Copy existing cell data
		cellData := make([]byte, cellInfo.CellSize)
		copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
		cells = append(cells, cellData)
		keys = append(keys, cellInfo.Key)
	}

	// If new cell wasn't inserted yet, it goes at the end
	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, newKey)
	}

	return cells, keys, nil
}

// collectInteriorCellsForSplit collects all existing interior cells plus the new cell
func (c *BtCursor) collectInteriorCellsForSplit(page *BtreePage, newKey int64, newChildPgno uint32) ([][]byte, []int64, []uint32, error) {
	numCells := int(page.Header.NumCells)
	cells := make([][]byte, 0, numCells+1)
	keys := make([]int64, 0, numCells+1)
	childPages := make([]uint32, 0, numCells+2) // +2 for new cell and rightmost child

	newCellData := EncodeTableInteriorCell(newChildPgno, newKey)
	inserted := false

	for i := 0; i < numCells; i++ {
		cellPtr, err := page.Header.GetCellPointer(page.Data, i)
		if err != nil {
			return nil, nil, nil, err
		}

		cellInfo, err := ParseCell(page.Header.PageType, page.Data[cellPtr:], page.UsableSize)
		if err != nil {
			return nil, nil, nil, err
		}

		// Insert new cell in sorted position
		if !inserted && newKey < cellInfo.Key {
			cells = append(cells, newCellData)
			keys = append(keys, newKey)
			childPages = append(childPages, newChildPgno)
			inserted = true
		}

		// Copy existing cell data
		cellData := make([]byte, cellInfo.CellSize)
		copy(cellData, page.Data[cellPtr:cellPtr+uint16(cellInfo.CellSize)])
		cells = append(cells, cellData)
		keys = append(keys, cellInfo.Key)
		childPages = append(childPages, cellInfo.ChildPage)
	}

	// Add rightmost child
	childPages = append(childPages, page.Header.RightChild)

	// If new cell wasn't inserted yet, insert before rightmost child
	if !inserted {
		cells = append(cells, newCellData)
		keys = append(keys, newKey)
		// Insert new child page before rightmost
		childPages = append([]uint32{}, childPages[:len(childPages)-1]...)
		childPages = append(childPages, newChildPgno)
		childPages = append(childPages, page.Header.RightChild)
	}

	return cells, keys, childPages, nil
}

// updateParentAfterSplit updates the parent page after a split
// If the current page is root, creates a new root
func (c *BtCursor) updateParentAfterSplit(leftPage, rightPage uint32, dividerKey int64) error {
	// If we're at the root, create a new root
	if c.Depth == 0 || leftPage == c.RootPage {
		return c.createNewRoot(leftPage, rightPage, dividerKey)
	}

	// Otherwise, insert divider into parent
	parentDepth := c.Depth - 1
	parentPage := c.PageStack[parentDepth]

	// Load parent page
	parentData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		return fmt.Errorf("failed to get parent page: %w", err)
	}

	parent, err := NewBtreePage(parentPage, parentData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create parent BtreePage: %w", err)
	}

	// Create interior cell for the divider
	dividerCell := EncodeTableInteriorCell(leftPage, dividerKey)

	// Check if parent has space
	if len(dividerCell) > parent.FreeSpace() {
		// Parent is full, need to split parent recursively
		// Save current state
		savedPage := c.CurrentPage
		savedIndex := c.CurrentIndex
		savedDepth := c.Depth
		savedHeader := c.CurrentHeader

		// Position cursor on parent
		c.CurrentPage = parentPage
		c.Depth = parentDepth
		c.CurrentHeader = parent.Header

		// Split parent
		err := c.splitInteriorPage(dividerKey, leftPage)

		// Restore cursor state
		c.CurrentPage = savedPage
		c.CurrentIndex = savedIndex
		c.Depth = savedDepth
		c.CurrentHeader = savedHeader

		return err
	}

	// Mark parent as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(parentPage); err != nil {
			return err
		}
	}

	// Find insertion point in parent
	insertIdx := 0
	for i := 0; i < int(parent.Header.NumCells); i++ {
		cellPtr, err := parent.Header.GetCellPointer(parent.Data, i)
		if err != nil {
			return err
		}

		cellInfo, err := ParseCell(parent.Header.PageType, parent.Data[cellPtr:], parent.UsableSize)
		if err != nil {
			return err
		}

		if dividerKey < cellInfo.Key {
			break
		}
		insertIdx++
	}

	// Insert divider cell
	if err := parent.InsertCell(insertIdx, dividerCell); err != nil {
		return fmt.Errorf("failed to insert divider into parent: %w", err)
	}

	// Update the cell that points to leftPage to now point to rightPage
	// Find the cell pointing to leftPage and update right child if needed
	if insertIdx == int(parent.Header.NumCells)-1 {
		// If this is the last cell, update rightmost child pointer
		headerOffset := getHeaderOffset(parentPage)
		binary.BigEndian.PutUint32(parent.Data[headerOffset+PageHeaderOffsetRightChild:], rightPage)
	}

	return nil
}

// createNewRoot creates a new root page after splitting the old root
func (c *BtCursor) createNewRoot(leftPage, rightPage uint32, dividerKey int64) error {
	// Allocate new root page
	newRootNum, err := c.Btree.AllocatePage()
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	// Get new root page data
	newRootData, err := c.Btree.GetPage(newRootNum)
	if err != nil {
		return fmt.Errorf("failed to get new root page: %w", err)
	}

	// Initialize as interior page
	if err := initializeInteriorPage(newRootData, newRootNum, c.Btree.UsableSize); err != nil {
		return fmt.Errorf("failed to initialize new root: %w", err)
	}

	// Mark as dirty
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(newRootNum); err != nil {
			return err
		}
	}

	// Create new root page wrapper
	newRoot, err := NewBtreePage(newRootNum, newRootData, c.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to create new root BtreePage: %w", err)
	}

	// Insert divider cell pointing to left page
	dividerCell := EncodeTableInteriorCell(leftPage, dividerKey)
	if err := newRoot.InsertCell(0, dividerCell); err != nil {
		return fmt.Errorf("failed to insert divider into new root: %w", err)
	}

	// Set rightmost child to right page
	headerOffset := getHeaderOffset(newRootNum)
	binary.BigEndian.PutUint32(newRoot.Data[headerOffset+PageHeaderOffsetRightChild:], rightPage)

	// Update cursor's root page
	c.RootPage = newRootNum

	return nil
}

// Helper functions

// initializeLeafPage initializes a page as an empty leaf table page
func initializeLeafPage(pageData []byte, pageNum uint32, usableSize uint32) error {
	headerOffset := getHeaderOffset(pageNum)

	// Set page type to leaf table
	pageData[headerOffset+PageHeaderOffsetType] = PageTypeLeafTable

	// Initialize header fields
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetFreeblock:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetNumCells:], 0)
	binary.BigEndian.PutUint16(pageData[headerOffset+PageHeaderOffsetCellStart:], 0)
	pageData[headerOffset+PageHeaderOffsetFragmented] = 0

	return nil
}

// initializeInteriorPage initializes a page as an empty interior table page
func initializeInteriorPage(pageData []byte, pageNum uint32, usableSize uint32) error {
	headerOffset := getHeaderOffset(pageNum)

	// Set page type to interior table
	pageData[headerOffset+PageHeaderOffsetType] = PageTypeInteriorTable

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
