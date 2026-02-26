package btree

import (
	"fmt"
)

// Cursor state constants
const (
	CursorValid       = 0 // Cursor points to a valid entry
	CursorInvalid     = 1 // Cursor does not point to a valid entry
	CursorSkipNext    = 2 // Next/Previous should be a no-op
	CursorRequireSeek = 3 // Cursor position needs to be restored
	CursorFault       = 4 // Unrecoverable error
)

// Maximum B-tree depth (to prevent infinite loops in corrupt databases)
const MaxBtreeDepth = 20

// BtCursor represents a cursor for traversing a B-tree
type BtCursor struct {
	Btree    *Btree // The B-tree this cursor belongs to
	RootPage uint32 // Root page number of the tree
	State    int    // Cursor state (valid, invalid, etc.)

	// Current position in the tree
	PageStack  [MaxBtreeDepth]uint32 // Stack of page numbers from root to current
	IndexStack [MaxBtreeDepth]int    // Stack of cell indices
	Depth      int                   // Current depth in tree (0 = root)

	// Current cell information
	CurrentPage   uint32      // Current page number
	CurrentIndex  int         // Current cell index in page
	CurrentCell   *CellInfo   // Parsed current cell
	CurrentHeader *PageHeader // Current page header

	// Navigation flags
	AtFirst bool // True if at first entry
	AtLast  bool // True if at last entry
}

// NewCursor creates a new cursor for the given B-tree and root page
func NewCursor(bt *Btree, rootPage uint32) *BtCursor {
	return &BtCursor{
		Btree:    bt,
		RootPage: rootPage,
		State:    CursorInvalid,
		Depth:    -1,
	}
}

func (c *BtCursor) resetToRoot() {
	c.Depth = -1
	c.AtFirst = false
	c.AtLast = false
}

// MoveToFirst moves the cursor to the first entry in the B-tree
func (c *BtCursor) MoveToFirst() error {
	c.resetToRoot()
	if err := c.descendToFirst(c.RootPage); err != nil {
		return err
	}
	c.AtFirst = true
	return nil
}

// MoveToLast moves the cursor to the last entry in the B-tree
func (c *BtCursor) MoveToLast() error {
	c.resetForMoveToLast()
	return c.navigateToRightmostLeaf(c.RootPage)
}

// resetForMoveToLast resets cursor state for MoveToLast.
func (c *BtCursor) resetForMoveToLast() {
	c.Depth = 0
	c.PageStack[0] = c.RootPage
	c.AtFirst = false
	c.AtLast = false
}

// navigateToRightmostLeaf navigates to the rightmost leaf page.
func (c *BtCursor) navigateToRightmostLeaf(pageNum uint32) error {
	for {
		pageData, header, err := c.getPageAndHeader(pageNum)
		if err != nil {
			return err
		}

		if header.IsLeaf {
			return c.positionAtLastCell(pageNum, pageData, header)
		}

		pageNum, err = c.descendToRightChild(pageNum, header)
		if err != nil {
			return err
		}
	}
}

// getPageAndHeader retrieves a page and parses its header.
func (c *BtCursor) getPageAndHeader(pageNum uint32) ([]byte, *PageHeader, error) {
	pageData, err := c.Btree.GetPage(pageNum)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, fmt.Errorf("failed to get page %d: %w", pageNum, err)
	}

	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, fmt.Errorf("failed to parse page %d: %w", pageNum, err)
	}
	return pageData, header, nil
}

// positionAtLastCell positions cursor at the last cell of a leaf page.
func (c *BtCursor) positionAtLastCell(pageNum uint32, pageData []byte, header *PageHeader) error {
	if header.NumCells == 0 {
		c.State = CursorInvalid
		return fmt.Errorf("empty leaf page %d", pageNum)
	}

	c.CurrentPage = pageNum
	c.CurrentIndex = int(header.NumCells) - 1
	c.CurrentHeader = header
	c.AtLast = true
	c.IndexStack[c.Depth] = c.CurrentIndex

	cellOffset, err := header.GetCellPointer(pageData, c.CurrentIndex)
	if err != nil {
		c.State = CursorInvalid
		return err
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return err
	}
	c.CurrentCell = cell
	c.State = CursorValid
	return nil
}

// descendToRightChild descends to the rightmost child of an interior page.
func (c *BtCursor) descendToRightChild(pageNum uint32, header *PageHeader) (uint32, error) {
	if header.RightChild == 0 {
		c.State = CursorInvalid
		return 0, fmt.Errorf("interior page %d has no right child", pageNum)
	}

	c.Depth++
	if c.Depth >= MaxBtreeDepth {
		c.State = CursorInvalid
		return 0, fmt.Errorf("btree depth exceeded (possible corruption)")
	}

	c.PageStack[c.Depth] = header.RightChild
	c.IndexStack[c.Depth] = -1
	return header.RightChild, nil
}

// Next moves the cursor to the next entry
func (c *BtCursor) Next() error {
	if c.State != CursorValid {
		return fmt.Errorf("cursor not in valid state")
	}
	c.AtFirst = false

	if advanced, err := c.advanceWithinPage(); advanced || err != nil {
		return err
	}

	childPage, found, err := c.climbToNextParent()
	if err != nil {
		return err
	}
	if found {
		return c.descendToFirst(childPage)
	}

	c.State = CursorInvalid
	c.AtLast = true
	return fmt.Errorf("end of btree")
}

func (c *BtCursor) advanceWithinPage() (bool, error) {
	if c.CurrentIndex >= int(c.CurrentHeader.NumCells)-1 {
		return false, nil
	}
	c.CurrentIndex++
	c.IndexStack[c.Depth] = c.CurrentIndex

	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		c.State = CursorInvalid
		return true, err
	}
	cellOffset, err := c.CurrentHeader.GetCellPointer(pageData, c.CurrentIndex)
	if err != nil {
		c.State = CursorInvalid
		return true, err
	}
	cell, err := ParseCell(c.CurrentHeader.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return true, err
	}
	c.CurrentCell = cell
	return true, nil
}

func (c *BtCursor) climbToNextParent() (uint32, bool, error) {
	for c.Depth > 0 {
		c.Depth--
		parentPage := c.PageStack[c.Depth]
		parentIndex := c.IndexStack[c.Depth]

		parentData, err := c.Btree.GetPage(parentPage)
		if err != nil {
			c.State = CursorInvalid
			return 0, false, err
		}
		parentHeader, err := ParsePageHeader(parentData, parentPage)
		if err != nil {
			c.State = CursorInvalid
			return 0, false, err
		}
		if parentIndex >= int(parentHeader.NumCells)-1 {
			continue
		}
		c.IndexStack[c.Depth] = parentIndex + 1
		cellOffset, err := parentHeader.GetCellPointer(parentData, parentIndex+1)
		if err != nil {
			c.State = CursorInvalid
			return 0, false, err
		}
		cell, err := ParseCell(parentHeader.PageType, parentData[cellOffset:], c.Btree.UsableSize)
		if err != nil {
			c.State = CursorInvalid
			return 0, false, err
		}
		return cell.ChildPage, true, nil
	}
	return 0, false, nil
}

// Previous moves the cursor to the previous entry
func (c *BtCursor) Previous() error {
	if c.State != CursorValid {
		return fmt.Errorf("cursor not in valid state")
	}
	c.AtLast = false
	if c.CurrentIndex > 0 {
		return c.prevInPage()
	}
	for c.Depth > 0 {
		found, err := c.prevViaParent()
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	c.State = CursorInvalid
	c.AtFirst = true
	return fmt.Errorf("beginning of btree")
}

func (c *BtCursor) prevInPage() error {
	c.CurrentIndex--
	c.IndexStack[c.Depth] = c.CurrentIndex
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		c.State = CursorInvalid
		return err
	}
	cellOffset, err := c.CurrentHeader.GetCellPointer(pageData, c.CurrentIndex)
	if err != nil {
		c.State = CursorInvalid
		return err
	}
	cell, err := ParseCell(c.CurrentHeader.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return err
	}
	c.CurrentCell = cell
	return nil
}

func (c *BtCursor) prevViaParent() (bool, error) {
	c.Depth--
	parentPage := c.PageStack[c.Depth]
	parentIndex := c.IndexStack[c.Depth]
	if parentIndex == 0 {
		return false, nil
	}
	c.IndexStack[c.Depth] = parentIndex - 1
	parentData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}
	parentHeader, err := ParsePageHeader(parentData, parentPage)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}
	cellOffset, err := parentHeader.GetCellPointer(parentData, parentIndex-1)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}
	cell, err := ParseCell(parentHeader.PageType, parentData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}
	return true, c.descendToLast(cell.ChildPage)
}

func (c *BtCursor) markInvalidAndReturn(err error) error {
	c.State = CursorInvalid
	return err
}

func (c *BtCursor) setupLeafFirst(pageNum uint32, pageData []byte, header *PageHeader) error {
	if header.NumCells == 0 {
		return c.markInvalidAndReturn(fmt.Errorf("empty leaf"))
	}

	c.CurrentPage = pageNum
	c.CurrentIndex = 0
	c.CurrentHeader = header

	cellOffset, err := header.GetCellPointer(pageData, 0)
	if err != nil {
		return c.markInvalidAndReturn(err)
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return c.markInvalidAndReturn(err)
	}
	c.CurrentCell = cell
	c.State = CursorValid
	return nil
}

// descendToFirst descends to the first (leftmost) entry starting from the given page
func (c *BtCursor) descendToFirst(pageNum uint32) error {
	for {
		c.Depth++
		if c.Depth >= MaxBtreeDepth {
			return c.markInvalidAndReturn(fmt.Errorf("btree depth exceeded"))
		}

		c.PageStack[c.Depth] = pageNum
		c.IndexStack[c.Depth] = 0

		pageData, header, err := c.getPageAndHeader(pageNum)
		if err != nil {
			return c.markInvalidAndReturn(err)
		}

		if header.IsLeaf {
			return c.setupLeafFirst(pageNum, pageData, header)
		}

		pageNum, err = c.getFirstChildPage(header, pageData)
		if err != nil {
			return c.markInvalidAndReturn(err)
		}
	}
}

func (c *BtCursor) getFirstChildPage(header *PageHeader, pageData []byte) (uint32, error) {
	cellOffset, err := header.GetCellPointer(pageData, 0)
	if err != nil {
		return 0, err
	}
	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return 0, err
	}
	return cell.ChildPage, nil
}

// descendToLast descends to the last (rightmost) entry starting from the given page
func (c *BtCursor) descendToLast(pageNum uint32) error {
	for {
		pageData, header, err := c.enterPage(pageNum)
		if err != nil {
			return err
		}
		if header.IsLeaf {
			return c.positionAtLastCell(pageNum, pageData, header)
		}
		c.IndexStack[c.Depth] = int(header.NumCells)
		pageNum = header.RightChild
	}
}

// enterPage increments depth and loads the page.
func (c *BtCursor) enterPage(pageNum uint32) ([]byte, *PageHeader, error) {
	c.Depth++
	if c.Depth >= MaxBtreeDepth {
		c.State = CursorInvalid
		return nil, nil, fmt.Errorf("btree depth exceeded")
	}
	c.PageStack[c.Depth] = pageNum

	pageData, err := c.Btree.GetPage(pageNum)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, err
	}
	header, err := ParsePageHeader(pageData, pageNum)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, err
	}
	return pageData, header, nil
}


// IsValid returns true if the cursor is pointing to a valid entry
func (c *BtCursor) IsValid() bool {
	return c.State == CursorValid
}

// GetKey returns the key of the current entry
func (c *BtCursor) GetKey() int64 {
	if c.State != CursorValid || c.CurrentCell == nil {
		return 0
	}
	return c.CurrentCell.Key
}

// GetPayload returns the payload of the current entry
func (c *BtCursor) GetPayload() []byte {
	if c.State != CursorValid || c.CurrentCell == nil {
		return nil
	}
	return c.CurrentCell.Payload
}

// String returns a string representation of the cursor
func (c *BtCursor) String() string {
	if c.State != CursorValid {
		return fmt.Sprintf("BtCursor{state=%d, invalid}", c.State)
	}
	return fmt.Sprintf("BtCursor{page=%d, index=%d, key=%d, depth=%d}",
		c.CurrentPage, c.CurrentIndex, c.GetKey(), c.Depth)
}

// SeekRowid seeks to the specified rowid in the table
// Returns true if the exact rowid is found, false otherwise
func (c *BtCursor) SeekRowid(rowid int64) (found bool, err error) {
	// Start from root
	c.Depth = 0
	c.PageStack[0] = c.RootPage
	c.IndexStack[0] = 0

	pageNum := c.RootPage

	// Navigate down the tree
	for {
		pageData, err := c.Btree.GetPage(pageNum)
		if err != nil {
			c.State = CursorInvalid
			return false, fmt.Errorf("failed to get page %d: %w", pageNum, err)
		}

		header, err := ParsePageHeader(pageData, pageNum)
		if err != nil {
			c.State = CursorInvalid
			return false, fmt.Errorf("failed to parse page %d: %w", pageNum, err)
		}

		idx, exactMatch := c.binarySearch(pageData, header, rowid)

		if header.IsLeaf {
			return c.seekLeafPage(pageData, header, pageNum, idx, exactMatch)
		}

		childPage, err := c.resolveChildPage(pageData, header, idx)
		if err != nil {
			return false, err
		}

		c.Depth++
		if c.Depth >= MaxBtreeDepth {
			c.State = CursorInvalid
			return false, fmt.Errorf("btree depth exceeded")
		}

		pageNum = childPage
		c.PageStack[c.Depth] = pageNum
		c.IndexStack[c.Depth] = 0
	}
}

// seekLeafPage positions the cursor on a leaf page after a binary search and
// returns whether the rowid was found exactly.
func (c *BtCursor) seekLeafPage(pageData []byte, header *PageHeader, pageNum uint32, idx int, exactMatch bool) (bool, error) {
	c.CurrentPage = pageNum
	c.CurrentIndex = idx
	c.CurrentHeader = header
	c.IndexStack[c.Depth] = idx

	if exactMatch && idx < int(header.NumCells) {
		return c.seekLeafExactMatch(pageData, header, idx)
	}

	// Rowid not found; position cursor at nearest entry for caller convenience
	c.State = CursorValid
	c.tryLoadCell(pageData, header, idx)
	return false, nil
}

// seekLeafExactMatch loads the cell at idx and marks the cursor valid on an
// exact-match hit inside a leaf page.
func (c *BtCursor) seekLeafExactMatch(pageData []byte, header *PageHeader, idx int) (bool, error) {
	cellOffset, err := header.GetCellPointer(pageData, idx)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return false, err
	}

	c.CurrentCell = cell
	c.State = CursorValid
	return true, nil
}

// tryLoadCell attempts a best-effort cell parse at idx; errors are silently
// ignored so the cursor remains positioned without a hard failure.
func (c *BtCursor) tryLoadCell(pageData []byte, header *PageHeader, idx int) {
	if idx >= int(header.NumCells) {
		return
	}
	cellOffset, err := header.GetCellPointer(pageData, idx)
	if err != nil {
		return
	}
	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		return
	}
	c.CurrentCell = cell
}

// resolveChildPage returns the child page number an interior node points to
// for the given binary-search result index.
func (c *BtCursor) resolveChildPage(pageData []byte, header *PageHeader, idx int) (uint32, error) {
	if idx >= int(header.NumCells) {
		return header.RightChild, nil
	}

	cellOffset, err := header.GetCellPointer(pageData, idx)
	if err != nil {
		c.State = CursorInvalid
		return 0, err
	}

	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return 0, err
	}

	return cell.ChildPage, nil
}

// binarySearch performs binary search for a rowid in a page
// Returns (index, exactMatch) where index is the position where the rowid should be
func (c *BtCursor) binarySearch(pageData []byte, header *PageHeader, rowid int64) (int, bool) {
	left := 0
	right := int(header.NumCells)

	for left < right {
		mid := (left + right) / 2

		// Get cell at mid
		cellOffset, err := header.GetCellPointer(pageData, mid)
		if err != nil {
			return left, false
		}

		cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
		if err != nil {
			return left, false
		}

		if cell.Key == rowid {
			return mid, true
		} else if cell.Key < rowid {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left, false
}

// Insert inserts a new row with the given key and payload
func (c *BtCursor) Insert(key int64, payload []byte) error {
	if err := c.validateInsertPosition(key); err != nil {
		return err
	}

	cellData := EncodeTableLeafCell(key, payload)
	btreePage, err := c.getCurrentBtreePage()
	if err != nil {
		return err
	}

	if len(cellData) > btreePage.FreeSpace() {
		return c.splitPage(key, payload)
	}

	// Mark page dirty BEFORE modification for journal support
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(c.CurrentPage); err != nil {
			return err
		}
	}

	if err := btreePage.InsertCell(c.CurrentIndex, cellData); err != nil {
		return err
	}

	_, err = c.SeekRowid(key)
	return err
}

// validateInsertPosition seeks to position and validates it's a valid leaf.
func (c *BtCursor) validateInsertPosition(key int64) error {
	found, err := c.SeekRowid(key)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("duplicate key: %d", key)
	}
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("cursor not positioned at leaf page")
	}
	return nil
}

// getCurrentBtreePage returns a BtreePage for the current cursor page.
func (c *BtCursor) getCurrentBtreePage() (*BtreePage, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, err
	}
	return NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
}

// Delete deletes the row at the current cursor position
func (c *BtCursor) Delete() error {
	if c.State != CursorValid {
		return fmt.Errorf("cursor not in valid state")
	}

	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("cursor not positioned at leaf page")
	}

	// Mark page dirty BEFORE modification for journal support
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(c.CurrentPage); err != nil {
			return err
		}
	}

	// Get the current page
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return err
	}

	// Wrap in BtreePage for write operations
	btreePage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return err
	}

	// Delete the cell
	if err := btreePage.DeleteCell(c.CurrentIndex); err != nil {
		return err
	}

	// Invalidate cursor
	c.State = CursorInvalid

	return nil
}

// splitPage splits a full page when inserting a new cell
// This is a simplified implementation - a full implementation would need to:
// 1. Allocate a new page
// 2. Distribute cells between old and new page
// 3. Update parent page (or create new root if splitting root)
// 4. Handle propagation of splits up the tree
func (c *BtCursor) splitPage(key int64, payload []byte) error {
	// For now, return an error indicating the page needs to be split
	// A full implementation would handle the split here
	return fmt.Errorf("page split not yet implemented (page %d is full)", c.CurrentPage)
}
