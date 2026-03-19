// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"fmt"
)

// IndexCursor represents a cursor for traversing an index B-tree
// Index B-trees store (key) -> rowid mappings where key can be any byte sequence
type IndexCursor struct {
	Btree    *Btree // The B-tree this cursor belongs to
	RootPage uint32 // Root page number of the index tree
	State    int    // Cursor state (valid, invalid, etc.)

	// Current position in the tree
	PageStack  [MaxBtreeDepth]uint32 // Stack of page numbers from root to current
	IndexStack [MaxBtreeDepth]int    // Stack of cell indices
	Depth      int                   // Current depth in tree (0 = root)

	// Current cell information
	CurrentPage   uint32      // Current page number
	CurrentIndex  int         // Current cell index in page
	CurrentHeader *PageHeader // Current page header

	// Current index entry (parsed from payload)
	CurrentKey   []byte // The search key
	CurrentRowid int64  // The rowid associated with this key

	// Navigation flags
	AtFirst bool // True if at first entry
	AtLast  bool // True if at last entry
}

// NewIndexCursor creates a new cursor for the given index B-tree and root page
func NewIndexCursor(bt *Btree, rootPage uint32) *IndexCursor {
	return &IndexCursor{
		Btree:    bt,
		RootPage: rootPage,
		State:    CursorInvalid,
		Depth:    -1,
	}
}

// parseIndexPayload extracts key and rowid from an index cell payload
// Format: [key data...] [rowid varint]
// The key length is implied by subtracting the rowid varint length from the total payload size
func (c *IndexCursor) parseIndexPayload(payload []byte) (key []byte, rowid int64, err error) {
	if len(payload) == 0 {
		return nil, 0, fmt.Errorf("empty index payload")
	}

	// The rowid is stored as a varint at the end of the payload
	// Try scanning backwards, starting with the maximum varint length (9 bytes)
	// and working down to 1 byte
	maxStart := len(payload) - 9
	if maxStart < 0 {
		maxStart = 0
	}

	for start := maxStart; start < len(payload); start++ {
		rowid64, n := GetVarint(payload[start:])
		if n > 0 && start+n == len(payload) {
			// Found a valid varint that ends exactly at the end of the payload.
			// Bit pattern is preserved via the uint64->int64 cast even for
			// values exceeding MaxInt64.
			key = payload[:start]
			rowid = int64(rowid64)
			return key, rowid, nil
		}
	}

	return nil, 0, fmt.Errorf("failed to parse index payload: invalid format")
}

// encodeIndexPayload encodes key and rowid into index cell payload format
// Format: [key data...] [rowid varint]
func encodeIndexPayload(key []byte, rowid int64) []byte {
	// Calculate total size needed
	rowidLen := VarintLen(uint64(rowid))
	payload := make([]byte, len(key)+rowidLen)

	// Copy key
	copy(payload, key)

	// Encode rowid as varint
	PutVarint(payload[len(key):], uint64(rowid))

	return payload
}

// loadCurrentEntry loads the current key and rowid from the current cell
func (c *IndexCursor) loadCurrentEntry(cell *CellInfo) error {
	key, rowid, err := c.parseIndexPayload(cell.Payload)
	if err != nil {
		return fmt.Errorf("failed to parse index entry: %w", err)
	}

	c.CurrentKey = key
	c.CurrentRowid = rowid
	return nil
}

// MoveToFirst moves the cursor to the first entry in the index B-tree
func (c *IndexCursor) MoveToFirst() error {
	c.resetToRoot()
	if err := c.descendToFirst(c.RootPage); err != nil {
		return err
	}
	c.AtFirst = true
	return nil
}

// MoveToLast moves the cursor to the last entry in the index B-tree
func (c *IndexCursor) MoveToLast() error {
	c.resetForMoveToLast()
	return c.navigateToRightmostLeaf(c.RootPage)
}

func (c *IndexCursor) resetToRoot() {
	c.Depth = -1
	c.AtFirst = false
	c.AtLast = false
}

func (c *IndexCursor) resetForMoveToLast() {
	c.Depth = 0
	c.PageStack[0] = c.RootPage
	c.AtFirst = false
	c.AtLast = false
}

func (c *IndexCursor) navigateToRightmostLeaf(pageNum uint32) error {
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

func (c *IndexCursor) getPageAndHeader(pageNum uint32) ([]byte, *PageHeader, error) {
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

func (c *IndexCursor) positionAtLastCell(pageNum uint32, pageData []byte, header *PageHeader) error {
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

	if err := c.loadCurrentEntry(cell); err != nil {
		c.State = CursorInvalid
		return err
	}

	c.State = CursorValid
	return nil
}

func (c *IndexCursor) descendToRightChild(pageNum uint32, header *PageHeader) (uint32, error) {
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

func (c *IndexCursor) descendToFirst(pageNum uint32) error {
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

func (c *IndexCursor) setupLeafFirst(pageNum uint32, pageData []byte, header *PageHeader) error {
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

	if err := c.loadCurrentEntry(cell); err != nil {
		return c.markInvalidAndReturn(err)
	}

	c.State = CursorValid
	return nil
}

func (c *IndexCursor) getFirstChildPage(header *PageHeader, pageData []byte) (uint32, error) {
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

func (c *IndexCursor) markInvalidAndReturn(err error) error {
	c.State = CursorInvalid
	return err
}

// NextIndex moves the cursor to the next entry in the index
func (c *IndexCursor) NextIndex() error {
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
	return fmt.Errorf("end of index")
}

func (c *IndexCursor) advanceWithinPage() (bool, error) {
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

	if err := c.loadCurrentEntry(cell); err != nil {
		c.State = CursorInvalid
		return true, err
	}

	return true, nil
}

func (c *IndexCursor) climbToNextParent() (uint32, bool, error) {
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

// PrevIndex moves the cursor to the previous entry in the index
func (c *IndexCursor) PrevIndex() error {
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
	return fmt.Errorf("beginning of index")
}

func (c *IndexCursor) prevInPage() error {
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

	if err := c.loadCurrentEntry(cell); err != nil {
		c.State = CursorInvalid
		return err
	}

	return nil
}

func (c *IndexCursor) prevViaParent() (bool, error) {
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

func (c *IndexCursor) descendToLast(pageNum uint32) error {
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

func (c *IndexCursor) enterPage(pageNum uint32) ([]byte, *PageHeader, error) {
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

// SeekIndex seeks to a specific key in the index
// Returns true if the exact key is found, false otherwise
func (c *IndexCursor) SeekIndex(key []byte) (found bool, err error) {
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

		idx, exactMatch := c.binarySearchKey(pageData, header, key)

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

// binarySearchKey performs binary search for a key in a page
// Returns (index, exactMatch) where index is the position where the key should be
func (c *IndexCursor) binarySearchKey(pageData []byte, header *PageHeader, key []byte) (int, bool) {
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

		// Extract key from payload
		cellKey, _, err := c.parseIndexPayload(cell.Payload)
		if err != nil {
			return left, false
		}

		cmp := bytes.Compare(cellKey, key)
		if cmp == 0 {
			return mid, true
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left, false
}

func (c *IndexCursor) seekLeafPage(pageData []byte, header *PageHeader, pageNum uint32, idx int, exactMatch bool) (bool, error) {
	c.CurrentPage = pageNum
	c.CurrentIndex = idx
	c.CurrentHeader = header
	c.IndexStack[c.Depth] = idx

	if exactMatch && idx < int(header.NumCells) {
		return c.seekLeafExactMatch(pageData, header, idx)
	}

	// Key not found; position cursor at nearest entry for caller convenience
	c.State = CursorValid
	c.tryLoadCell(pageData, header, idx)
	return false, nil
}

func (c *IndexCursor) seekLeafExactMatch(pageData []byte, header *PageHeader, idx int) (bool, error) {
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

	if err := c.loadCurrentEntry(cell); err != nil {
		c.State = CursorInvalid
		return false, err
	}

	c.State = CursorValid
	return true, nil
}

func (c *IndexCursor) tryLoadCell(pageData []byte, header *PageHeader, idx int) {
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
	c.loadCurrentEntry(cell)
}

func (c *IndexCursor) resolveChildPage(pageData []byte, header *PageHeader, idx int) (uint32, error) {
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

// InsertIndex inserts a key-rowid pair into the index
func (c *IndexCursor) InsertIndex(key []byte, rowid int64) error {
	if err := c.validateInsertPosition(key); err != nil {
		return err
	}

	payload := encodeIndexPayload(key, rowid)
	cellData := EncodeIndexLeafCell(payload)
	btreePage, err := c.getCurrentBtreePage()
	if err != nil {
		return err
	}

	if len(cellData) > btreePage.FreeSpace() {
		return c.splitPage(key, rowid)
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

	_, err = c.SeekIndex(key)
	return err
}

func (c *IndexCursor) validateInsertPosition(key []byte) error {
	found, err := c.SeekIndex(key)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("duplicate key in index")
	}
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("cursor not positioned at leaf page")
	}
	return nil
}

func (c *IndexCursor) getCurrentBtreePage() (*BtreePage, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, err
	}
	return NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
}

// DeleteIndex deletes a specific key-rowid pair from the index
func (c *IndexCursor) DeleteIndex(key []byte, rowid int64) error {
	// Seek to the key
	found, err := c.SeekIndex(key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("key not found in index")
	}

	// Verify we're at the right rowid
	if c.CurrentRowid != rowid {
		// The key exists but with a different rowid
		// We need to scan for the exact key-rowid pair
		return c.deleteExactMatch(key, rowid)
	}

	return c.deleteCurrentEntry()
}

func (c *IndexCursor) deleteExactMatch(key []byte, rowid int64) error {
	// Start from current position and scan forward for matching key-rowid
	originalKey := c.CurrentKey
	for bytes.Equal(c.CurrentKey, key) {
		if c.CurrentRowid == rowid {
			return c.deleteCurrentEntry()
		}
		if err := c.NextIndex(); err != nil {
			// Reached end without finding exact match
			return fmt.Errorf("key-rowid pair not found in index")
		}
	}

	// The keys no longer match, so we didn't find it
	// Restore position by seeking back
	c.SeekIndex(originalKey)
	return fmt.Errorf("key-rowid pair not found in index")
}

func (c *IndexCursor) deleteCurrentEntry() error {
	if err := c.validateDeleteState(); err != nil {
		return err
	}

	if err := c.markPageDirtyForDelete(); err != nil {
		return err
	}

	btreePage, err := c.getBtreePageForDelete()
	if err != nil {
		return err
	}

	if err := btreePage.DeleteCell(c.CurrentIndex); err != nil {
		return err
	}

	c.State = CursorInvalid
	return nil
}

// validateDeleteState checks if cursor is in valid state for deletion
func (c *IndexCursor) validateDeleteState() error {
	if c.State != CursorValid {
		return fmt.Errorf("cursor not in valid state")
	}
	if c.CurrentHeader == nil || !c.CurrentHeader.IsLeaf {
		return fmt.Errorf("cursor not positioned at leaf page")
	}
	return nil
}

// markPageDirtyForDelete marks the page dirty before deletion
func (c *IndexCursor) markPageDirtyForDelete() error {
	if c.Btree.Provider != nil {
		if err := c.Btree.Provider.MarkDirty(c.CurrentPage); err != nil {
			return err
		}
	}
	return nil
}

// getBtreePageForDelete gets and wraps the page for deletion
func (c *IndexCursor) getBtreePageForDelete() (*BtreePage, error) {
	pageData, err := c.Btree.GetPage(c.CurrentPage)
	if err != nil {
		return nil, err
	}
	return NewBtreePage(c.CurrentPage, pageData, c.Btree.UsableSize)
}

func (c *IndexCursor) splitPage(key []byte, rowid int64) error {
	// For now, return an error indicating the page needs to be split
	// A full implementation would handle the split here
	return fmt.Errorf("index page split not yet implemented (page %d is full)", c.CurrentPage)
}

// IsValid returns true if the cursor is pointing to a valid entry
func (c *IndexCursor) IsValid() bool {
	return c.State == CursorValid
}

// GetKey returns the key of the current entry
func (c *IndexCursor) GetKey() []byte {
	if c.State != CursorValid {
		return nil
	}
	return c.CurrentKey
}

// GetRowid returns the rowid of the current entry
func (c *IndexCursor) GetRowid() int64 {
	if c.State != CursorValid {
		return 0
	}
	return c.CurrentRowid
}

// String returns a string representation of the cursor
func (c *IndexCursor) String() string {
	if c.State != CursorValid {
		return fmt.Sprintf("IndexCursor{state=%d, invalid}", c.State)
	}
	return fmt.Sprintf("IndexCursor{page=%d, index=%d, key=%q, rowid=%d, depth=%d}",
		c.CurrentPage, c.CurrentIndex, c.CurrentKey, c.CurrentRowid, c.Depth)
}
