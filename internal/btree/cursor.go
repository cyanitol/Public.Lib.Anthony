// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"encoding/binary"
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
	Btree       *Btree // The B-tree this cursor belongs to
	RootPage    uint32 // Root page number of the tree
	State       int    // Cursor state (valid, invalid, etc.)
	CompositePK bool   // True if keys are composite byte keys (WITHOUT ROWID)

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

// NewCursorWithOptions creates a new cursor with optional composite key mode.
func NewCursorWithOptions(bt *Btree, rootPage uint32, compositePK bool) *BtCursor {
	cur := NewCursor(bt, rootPage)
	cur.CompositePK = compositePK
	return cur
}

// compareKeys compares two keys based on cursor mode.
// For composite cursors, compares byte keys; otherwise compares int rowids.
func (c *BtCursor) compareKeys(aKey int64, aBytes []byte, bKey int64, bBytes []byte) int {
	if c.CompositePK {
		return bytes.Compare(aBytes, bBytes)
	}
	switch {
	case aKey == bKey:
		return 0
	case aKey < bKey:
		return -1
	default:
		return 1
	}
}

// validateCursorState validates that the cursor is in a valid state for operations
func (c *BtCursor) validateCursorState() error {
	if c.Btree == nil {
		return fmt.Errorf("cursor has nil btree")
	}
	if c.RootPage == 0 {
		return fmt.Errorf("cursor has invalid root page")
	}
	if c.Depth >= MaxBtreeDepth {
		return fmt.Errorf("cursor depth exceeded maximum")
	}
	return nil
}

func (c *BtCursor) resetToRoot() {
	c.Depth = -1
	c.AtFirst = false
	c.AtLast = false
}

// MoveToFirst moves the cursor to the first entry in the B-tree
func (c *BtCursor) MoveToFirst() error {
	if err := c.validateCursorState(); err != nil {
		return err
	}
	c.resetToRoot()
	if err := c.descendToFirst(c.RootPage); err != nil {
		return err
	}
	c.AtFirst = true
	return nil
}

// MoveToLast moves the cursor to the last entry in the B-tree
func (c *BtCursor) MoveToLast() error {
	if err := c.validateCursorState(); err != nil {
		return err
	}
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

		// Record the right-child slot index at this interior level so that
		// prevViaParent can correctly determine how many left siblings remain.
		c.IndexStack[c.Depth] = int(header.NumCells)

		pageNum, err = c.descendToRightChild(pageNum, header)
		if err != nil {
			return err
		}
	}
}

// navigateToRightmostLeafComposite is an alias retained for test compatibility.
func (c *BtCursor) navigateToRightmostLeafComposite(pageNum uint32) error {
	return c.navigateToRightmostLeaf(pageNum)
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
	if header.PageType == PageTypeLeafTableNoInt || header.PageType == PageTypeInteriorTableNo {
		c.CompositePK = true
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

	cell, err := c.parseCellAt(pageData, header, c.CurrentIndex)
	if err != nil {
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
	cell, err := c.parseCellAt(pageData, c.CurrentHeader, c.CurrentIndex)
	if err != nil {
		return true, err
	}
	c.CurrentCell = cell
	return true, nil
}

func (c *BtCursor) climbToNextParent() (uint32, bool, error) {
	for c.Depth > 0 {
		c.Depth--
		childPage, found, err := c.tryAdvanceInParent()
		if err != nil {
			return 0, false, err
		}
		if found {
			return childPage, true, nil
		}
	}
	return 0, false, nil
}

// tryAdvanceInParent attempts to advance to the next cell in the parent page.
func (c *BtCursor) tryAdvanceInParent() (uint32, bool, error) {
	parentPage := c.PageStack[c.Depth]
	parentIndex := c.IndexStack[c.Depth]

	parentData, parentHeader, err := c.loadParentPage(parentPage)
	if err != nil {
		return 0, false, err
	}

	if parentIndex >= int(parentHeader.NumCells) {
		return 0, false, nil
	}

	if parentIndex < int(parentHeader.NumCells)-1 {
		c.IndexStack[c.Depth] = parentIndex + 1
		return c.getChildPageFromParent(parentData, parentHeader, parentIndex+1)
	}

	// When at the last cell, advance to the rightmost child.
	if parentHeader.RightChild != 0 {
		c.IndexStack[c.Depth] = int(parentHeader.NumCells)
		return parentHeader.RightChild, true, nil
	}

	return 0, false, nil
}

// loadParentPage loads and parses a parent page.
func (c *BtCursor) loadParentPage(parentPage uint32) ([]byte, *PageHeader, error) {
	parentData, err := c.Btree.GetPage(parentPage)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, err
	}
	parentHeader, err := ParsePageHeader(parentData, parentPage)
	if err != nil {
		c.State = CursorInvalid
		return nil, nil, err
	}
	return parentData, parentHeader, nil
}

// getChildPageFromParent extracts the child page number from a parent cell.
func (c *BtCursor) getChildPageFromParent(parentData []byte, parentHeader *PageHeader, cellIdx int) (uint32, bool, error) {
	cell, err := c.parseCellAt(parentData, parentHeader, cellIdx)
	if err != nil {
		return 0, false, err
	}
	return cell.ChildPage, true, nil
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
	cell, err := c.parseCellAt(pageData, c.CurrentHeader, c.CurrentIndex)
	if err != nil {
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
	parentData, parentHeader, err := c.loadParentPage(parentPage)
	if err != nil {
		return false, err
	}
	cell, err := c.parseCellAt(parentData, parentHeader, parentIndex-1)
	if err != nil {
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

	cell, err := c.parseCellAt(pageData, header, 0)
	if err != nil {
		return err
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
	cell, err := c.parseCellAt(pageData, header, 0)
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
	if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil {
		return 0
	}
	return c.CurrentCell.Key
}

// GetKeyBytes returns the composite key bytes for WITHOUT ROWID cursors.
func (c *BtCursor) GetKeyBytes() []byte {
	if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil {
		return nil
	}
	return c.CurrentCell.KeyBytes
}

// GetPayload returns the local payload of the current entry
// Note: This only returns the portion stored locally in the cell.
// For cells with overflow pages, use GetCompletePayload() to get the full payload.
func (c *BtCursor) GetPayload() []byte {
	if c.Btree == nil || c.State != CursorValid || c.CurrentCell == nil {
		return nil
	}
	return c.CurrentCell.Payload
}

// GetPayloadWithOverflow returns the complete payload of the current entry,
// automatically reading from overflow pages if necessary.
// This is an alias for GetCompletePayload() for convenience.
func (c *BtCursor) GetPayloadWithOverflow() ([]byte, error) {
	return c.GetCompletePayload()
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
	if err := c.validateCursorState(); err != nil {
		return false, err
	}

	c.initializeSeek()
	return c.navigateToKey(c.RootPage, rowid, nil)
}

// SeekComposite seeks to the specified composite key in the table.
// Returns true if the exact key is found, false otherwise.
func (c *BtCursor) SeekComposite(key []byte) (bool, error) {
	if err := c.validateCursorState(); err != nil {
		return false, err
	}
	c.initializeSeek()
	return c.navigateToKey(c.RootPage, 0, key)
}

// initializeSeek initializes cursor state for seeking.
func (c *BtCursor) initializeSeek() {
	c.Depth = 0
	c.PageStack[0] = c.RootPage
	c.IndexStack[0] = 0
}

// navigateToKey navigates down the tree to find the given rowid or composite key.
func (c *BtCursor) navigateToKey(pageNum uint32, rowid int64, compositeKey []byte) (bool, error) {
	for {
		pageData, header, err := c.loadPageForSeek(pageNum)
		if err != nil {
			return false, err
		}

		idx, exactMatch := c.binarySearchKey(pageData, header, rowid, compositeKey)

		if header.IsLeaf {
			return c.seekLeafPage(pageData, header, pageNum, idx, exactMatch)
		}

		childPage, err := c.advanceToChildPage(pageData, header, idx)
		if err != nil {
			return false, err
		}
		pageNum = childPage
	}
}

// loadPageForSeek loads and parses a page during seek operation.
func (c *BtCursor) loadPageForSeek(pageNum uint32) ([]byte, *PageHeader, error) {
	return c.getPageAndHeader(pageNum)
}

// advanceToChildPage advances to the child page and updates cursor depth.
func (c *BtCursor) advanceToChildPage(pageData []byte, header *PageHeader, idx int) (uint32, error) {
	childPage, err := c.resolveChildPage(pageData, header, idx)
	if err != nil {
		return 0, err
	}

	// Record which slot we are taking at this interior level so that
	// prevViaParent can navigate backwards correctly.
	slotIdx := idx
	if idx >= int(header.NumCells) {
		slotIdx = int(header.NumCells)
	}
	c.IndexStack[c.Depth] = slotIdx

	c.Depth++
	if c.Depth >= MaxBtreeDepth {
		c.State = CursorInvalid
		return 0, fmt.Errorf("btree depth exceeded")
	}

	c.PageStack[c.Depth] = childPage
	c.IndexStack[c.Depth] = 0
	return childPage, nil
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
	cell, err := c.parseCellAt(pageData, header, idx)
	if err != nil {
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
		if header.NumCells == 0 {
			c.CurrentCell = nil
			return
		}
		idx = int(header.NumCells) - 1
	}
	if idx < 0 {
		if header.NumCells == 0 {
			c.CurrentCell = nil
			return
		}
		idx = 0
	}
	cellOffset, err := header.GetCellPointer(pageData, idx)
	if err != nil {
		c.CurrentCell = nil
		return
	}
	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.CurrentCell = nil
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
	cell, err := c.parseCellAt(pageData, header, idx)
	if err != nil {
		return 0, err
	}
	return cell.ChildPage, nil
}

// binarySearchKey performs binary search for a rowid or composite key in a page.
// Returns (index, exactMatch) where index is the position where the key should be.
func (c *BtCursor) binarySearchKey(pageData []byte, header *PageHeader, rowid int64, compositeKey []byte) (int, bool) {
	left := 0
	right := int(header.NumCells)

	for left < right {
		mid := (left + right) / 2

		cellOffset, err := header.GetCellPointer(pageData, mid)
		if err != nil {
			return left, false
		}

		cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
		if err != nil {
			return left, false
		}

		comp := c.compareCellKey(cell, rowid, compositeKey)
		if comp == 0 {
			return mid, true
		} else if comp < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left, false
}

// compareCellKey compares a cell key to a target. When CompositePK is true,
// the compositeKey parameter is used and compared with KeyBytes; otherwise, rowid is used.
func (c *BtCursor) compareCellKey(cell *CellInfo, rowid int64, compositeKey []byte) int {
	if c.CompositePK && compositeKey != nil {
		return bytes.Compare(cell.KeyBytes, compositeKey)
	}
	if cell.Key == rowid {
		return 0
	}
	if cell.Key < rowid {
		return -1
	}
	return 1
}

// Insert inserts a new row with the given key and payload
// Automatically handles overflow pages if the payload is too large
func (c *BtCursor) Insert(key int64, payload []byte) error {
	return c.InsertWithComposite(key, nil, payload)
}

// InsertWithComposite inserts a row using either int64 keys or composite key bytes.
func (c *BtCursor) InsertWithComposite(key int64, keyBytes []byte, payload []byte) error {
	if err := c.validateInsertPosition(key, keyBytes); err != nil {
		return err
	}

	cellData, overflowPage, err := c.prepareCellData(key, keyBytes, payload)
	if err != nil {
		return err
	}

	btreePage, err := c.getCurrentBtreePage()
	if err != nil {
		c.cleanupOverflowOnError(overflowPage)
		return err
	}

	if len(cellData) > btreePage.FreeSpace() {
		cellData, overflowPage = c.retryWithMinimalOverflow(key, keyBytes, payload, cellData, overflowPage, btreePage)
		if len(cellData) > btreePage.FreeSpace() {
			c.cleanupOverflowOnError(overflowPage)
			return c.splitPage(key, keyBytes, payload)
		}
	}

	return c.finishInsert(key, keyBytes, cellData, overflowPage, btreePage)
}

// retryWithMinimalOverflow attempts to re-encode with minimal local payload
// when the page is empty and the cell doesn't fit.
func (c *BtCursor) retryWithMinimalOverflow(key int64, keyBytes, payload, cellData []byte, overflowPage uint32, btreePage *BtreePage) ([]byte, uint32) {
	if btreePage.Header.NumCells != 0 || len(payload) == 0 {
		return cellData, overflowPage
	}
	c.cleanupOverflowOnError(overflowPage)
	overflowPage = 0

	minLocal := calculateMinLocal(c.Btree.UsableSize, true)
	if minLocal > uint32(len(payload)) {
		minLocal = uint32(len(payload))
	}
	localSize := uint16(minLocal)

	newOverflow, err := c.WriteOverflow(payload, localSize, c.Btree.UsableSize)
	if err != nil {
		return cellData, overflowPage
	}
	return c.encodeTableLeafCellWithOverflow(key, keyBytes, payload[:localSize], newOverflow, uint32(len(payload))), newOverflow
}

// finishInsert marks the page dirty, inserts the cell, and repositions the cursor.
func (c *BtCursor) finishInsert(key int64, keyBytes, cellData []byte, overflowPage uint32, btreePage *BtreePage) error {
	if err := c.markPageDirty(); err != nil {
		c.cleanupOverflowOnError(overflowPage)
		return err
	}
	if err := btreePage.InsertCell(c.CurrentIndex, cellData); err != nil {
		c.cleanupOverflowOnError(overflowPage)
		return err
	}
	return c.seekAfterInsert(key, keyBytes)
}

// seekAfterInsert repositions the cursor after a successful insert.
func (c *BtCursor) seekAfterInsert(key int64, keyBytes []byte) error {
	if c.CompositePK {
		_, err := c.SeekComposite(keyBytes)
		return err
	}
	_, err := c.SeekRowid(key)
	return err
}

// prepareCellData encodes the cell data with optional overflow handling.
func (c *BtCursor) prepareCellData(key int64, keyBytes []byte, payload []byte) (cellData []byte, overflowPage uint32, err error) {
	payloadSize := uint32(len(payload))
	localSize := CalculateLocalPayload(payloadSize, c.Btree.UsableSize, true)

	if payloadSize > uint32(localSize) {
		overflowPage, err = c.WriteOverflow(payload, localSize, c.Btree.UsableSize)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to write overflow: %w", err)
		}
		cellData = c.encodeTableLeafCellWithOverflow(key, keyBytes, payload[:localSize], overflowPage, payloadSize)
	} else {
		if c.CompositePK {
			cellData = EncodeTableLeafCompositeCell(keyBytes, payload)
		} else {
			cellData = EncodeTableLeafCell(key, payload)
		}
	}
	return cellData, overflowPage, nil
}

// cleanupOverflowOnError cleans up overflow pages if they were allocated.
func (c *BtCursor) cleanupOverflowOnError(overflowPage uint32) {
	if overflowPage != 0 {
		c.FreeOverflowChain(overflowPage)
	}
}

// markPageDirty marks the current page as dirty if using a provider.
func (c *BtCursor) markPageDirty() error {
	if c.Btree.Provider != nil {
		return c.Btree.Provider.MarkDirty(c.CurrentPage)
	}
	return nil
}

// encodeTableLeafCellWithOverflow encodes a table leaf cell with overflow
// Supports both int rowid keys and composite keys (when keyBytes provided).
// Formats:
//
//	int key:     varint(total_payload_size), varint(rowid), local_payload, overflow_page_number
//	composite:   varint(total_payload_size), varint(key_len), key_bytes, local_payload, overflow_page_number
func (c *BtCursor) encodeTableLeafCellWithOverflow(rowid int64, keyBytes []byte, localPayload []byte, overflowPage uint32, totalPayloadSize uint32) []byte {
	if c != nil && c.CompositePK && len(keyBytes) > 0 {
		sizeVarintSize := VarintLen(uint64(totalPayloadSize))
		keyLenVarint := VarintLen(uint64(len(keyBytes)))
		bufSize := sizeVarintSize + keyLenVarint + len(keyBytes) + len(localPayload) + 4
		buf := make([]byte, bufSize)
		offset := 0

		n := PutVarint(buf[offset:], uint64(totalPayloadSize))
		offset += n
		n = PutVarint(buf[offset:], uint64(len(keyBytes)))
		offset += n
		copy(buf[offset:], keyBytes)
		offset += len(keyBytes)
		copy(buf[offset:], localPayload)
		offset += len(localPayload)
		binary.BigEndian.PutUint32(buf[offset:], overflowPage)
		offset += 4
		return buf[:offset]
	}

	// rowid path
	sizeVarintSize := VarintLen(uint64(totalPayloadSize))
	rowidVarintSize := VarintLen(uint64(rowid))

	bufSize := sizeVarintSize + rowidVarintSize + len(localPayload) + 4
	buf := make([]byte, bufSize)
	offset := 0

	n := PutVarint(buf[offset:], uint64(totalPayloadSize))
	offset += n

	n = PutVarint(buf[offset:], uint64(rowid))
	offset += n

	copy(buf[offset:], localPayload)
	offset += len(localPayload)

	binary.BigEndian.PutUint32(buf[offset:], overflowPage)
	offset += 4

	return buf[:offset]
}

// validateInsertPosition seeks to position and validates it's a valid leaf.
func (c *BtCursor) validateInsertPosition(key int64, keyBytes []byte) error {
	var found bool
	var err error
	if c.CompositePK {
		found, err = c.SeekComposite(keyBytes)
	} else {
		found, err = c.SeekRowid(key)
	}
	if err != nil {
		return err
	}
	if found {
		if c.CompositePK {
			return fmt.Errorf("UNIQUE constraint failed: duplicate composite key")
		}
		return fmt.Errorf("UNIQUE constraint failed: duplicate key %d", key)
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
// Automatically frees any overflow pages associated with the cell
func (c *BtCursor) Delete() error {
	if err := c.validateDeletePosition(); err != nil {
		return err
	}

	if err := c.freeOverflowPages(); err != nil {
		return err
	}

	if err := c.markPageDirty(); err != nil {
		return err
	}

	if err := c.performCellDeletion(); err != nil {
		return err
	}

	return c.adjustCursorAfterDelete()
}

// validateDeletePosition validates the cursor is in a valid position for deletion.
func (c *BtCursor) validateDeletePosition() error {
	if c.State != CursorValid {
		return fmt.Errorf("cursor not in valid state")
	}
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("cursor not positioned at leaf page")
	}
	return nil
}

// freeOverflowPages frees any overflow pages associated with the current cell.
func (c *BtCursor) freeOverflowPages() error {
	if c.CurrentCell != nil && c.CurrentCell.OverflowPage != 0 {
		if err := c.FreeOverflowChain(c.CurrentCell.OverflowPage); err != nil {
			return fmt.Errorf("failed to free overflow pages: %w", err)
		}
	}
	return nil
}

// performCellDeletion performs the actual cell deletion from the page.
func (c *BtCursor) performCellDeletion() error {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return err
	}

	btreePage, err := NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
	if err != nil {
		return err
	}

	return btreePage.DeleteCell(c.CurrentIndex)
}

// adjustCursorAfterDelete adjusts the cursor position after deletion.
// After deleting a cell at index i, all cells from index i+1 onwards shift down by one.
// We decrement the index so Next() will advance to the correct next cell.
func (c *BtCursor) adjustCursorAfterDelete() error {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		c.State = CursorInvalid
		return err
	}

	c.CurrentHeader, err = ParsePageHeader(pageData, c.CurrentPage)
	if err != nil {
		c.State = CursorInvalid
		return err
	}

	c.CurrentIndex--
	c.IndexStack[c.Depth] = c.CurrentIndex

	if c.CurrentIndex < 0 {
		c.CurrentCell = nil
		return nil
	}

	return c.loadCellAtCurrentIndex(pageData)
}

// parseCellAt parses the cell at the given index within a page, invalidating
// the cursor on error. This is the single helper for the repeated
// getCellPointer + ParseCell + invalidate pattern.
func (c *BtCursor) parseCellAt(pageData []byte, header *PageHeader, idx int) (*CellInfo, error) {
	cellOffset, err := header.GetCellPointer(pageData, idx)
	if err != nil {
		c.State = CursorInvalid
		return nil, err
	}
	cell, err := ParseCell(header.PageType, pageData[cellOffset:], c.Btree.UsableSize)
	if err != nil {
		c.State = CursorInvalid
		return nil, err
	}
	return cell, nil
}

// loadCellAtCurrentIndex loads the cell at the current cursor index.
func (c *BtCursor) loadCellAtCurrentIndex(pageData []byte) error {
	cell, err := c.parseCellAt(pageData, c.CurrentHeader, c.CurrentIndex)
	if err != nil {
		return err
	}
	c.CurrentCell = cell
	return nil
}

// splitPage splits a full page when inserting a new cell
// Delegates to splitLeafPage or splitInteriorPage based on page type
func (c *BtCursor) splitPage(key int64, keyBytes []byte, payload []byte) error {
	if c.CurrentHeader == nil {
		return fmt.Errorf("cursor not positioned at valid page")
	}

	if c.CurrentHeader.IsLeaf {
		return c.splitLeafPage(key, keyBytes, payload)
	}

	// For interior pages, we need the child page number
	// This is a simplified case - in practice, interior page splits
	// happen during parent updates in splitLeafPage
	return fmt.Errorf("direct interior page split not supported (page %d)", c.CurrentPage)
}
