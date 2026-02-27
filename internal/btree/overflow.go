package btree

import (
	"encoding/binary"
	"fmt"
)

// Overflow page format (per SQLite specification):
// - First 4 bytes: next overflow page number (0 if last page)
// - Remaining bytes: payload data

const (
	// OverflowHeaderSize is the size of the overflow page header (next page pointer)
	OverflowHeaderSize = 4
)

// WriteOverflow writes large payload to overflow pages
// Returns the page number of the first overflow page
func (c *BtCursor) WriteOverflow(payload []byte, localSize uint16, usableSize uint32) (uint32, error) {
	if c.Btree == nil {
		return 0, fmt.Errorf("cursor has no btree")
	}

	// Calculate how much payload goes to overflow
	overflowSize := len(payload) - int(localSize)
	if overflowSize <= 0 {
		return 0, nil // No overflow needed
	}

	overflowData := payload[localSize:]
	return writeOverflowChain(c.Btree, overflowData, usableSize)
}

// writeOverflowChain writes a chain of overflow pages for the given data
// Returns the page number of the first overflow page
func writeOverflowChain(bt *Btree, data []byte, usableSize uint32) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Calculate how much data fits in each overflow page
	overflowPageCapacity := int(usableSize) - OverflowHeaderSize

	// Allocate first overflow page
	firstPageNum, err := bt.AllocatePage()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate first overflow page: %w", err)
	}

	prevPageNum := firstPageNum
	offset := 0

	for offset < len(data) {
		// Get current page
		pageData, err := bt.GetPage(prevPageNum)
		if err != nil {
			return 0, fmt.Errorf("failed to get overflow page %d: %w", prevPageNum, err)
		}

		// Calculate how much data to write to this page
		remaining := len(data) - offset
		toWrite := remaining
		if toWrite > overflowPageCapacity {
			toWrite = overflowPageCapacity
		}

		// Check if we need another overflow page
		var nextPageNum uint32
		if offset+toWrite < len(data) {
			// Allocate next overflow page
			nextPageNum, err = bt.AllocatePage()
			if err != nil {
				return 0, fmt.Errorf("failed to allocate overflow page: %w", err)
			}
		}

		// Write the overflow page header (next page pointer)
		binary.BigEndian.PutUint32(pageData[0:4], nextPageNum)

		// Write the payload data
		copy(pageData[OverflowHeaderSize:], data[offset:offset+toWrite])

		// Mark page as dirty if using a provider
		if bt.Provider != nil {
			if err := bt.Provider.MarkDirty(prevPageNum); err != nil {
				return 0, fmt.Errorf("failed to mark overflow page %d dirty: %w", prevPageNum, err)
			}
		}

		// Move to next page
		offset += toWrite
		prevPageNum = nextPageNum
	}

	return firstPageNum, nil
}

// ReadOverflow reads payload from an overflow page chain
// firstOverflowPage is the page number of the first overflow page
// totalPayloadSize is the total size of the payload (including local part)
// localSize is the amount of payload stored locally in the cell
// Returns the complete payload (local + overflow)
func (c *BtCursor) ReadOverflow(localPayload []byte, firstOverflowPage uint32, totalPayloadSize uint32, usableSize uint32) ([]byte, error) {
	if firstOverflowPage == 0 {
		// No overflow, return local payload
		return localPayload, nil
	}

	if c.Btree == nil {
		return nil, fmt.Errorf("cursor has no btree")
	}

	// Allocate buffer for complete payload
	completePayload := make([]byte, totalPayloadSize)

	// Copy local payload
	copy(completePayload, localPayload)

	// Read overflow data
	overflowSize := int(totalPayloadSize) - len(localPayload)
	overflowData, err := readOverflowChain(c.Btree, firstOverflowPage, overflowSize, usableSize)
	if err != nil {
		return nil, err
	}

	// Copy overflow data
	copy(completePayload[len(localPayload):], overflowData)

	return completePayload, nil
}

// readOverflowChain reads data from a chain of overflow pages
// firstPage is the page number of the first overflow page
// dataSize is the number of bytes to read from the overflow chain
func readOverflowChain(bt *Btree, firstPage uint32, dataSize int, usableSize uint32) ([]byte, error) {
	if firstPage == 0 || dataSize <= 0 {
		return nil, nil
	}

	result := make([]byte, dataSize)
	currentPage := firstPage
	offset := 0
	overflowPageCapacity := int(usableSize) - OverflowHeaderSize

	// Limit chain traversal to prevent infinite loops in corrupt databases
	maxPages := (dataSize / overflowPageCapacity) + 2
	pageCount := 0

	for offset < dataSize && currentPage != 0 {
		pageCount++
		if pageCount > maxPages {
			return nil, fmt.Errorf("overflow chain too long (possible corruption), page count: %d", pageCount)
		}

		// Get the overflow page
		pageData, err := bt.GetPage(currentPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get overflow page %d: %w", currentPage, err)
		}

		// Read next page pointer from header
		nextPage := binary.BigEndian.Uint32(pageData[0:4])

		// Calculate how much to read from this page
		remaining := dataSize - offset
		toRead := remaining
		if toRead > overflowPageCapacity {
			toRead = overflowPageCapacity
		}

		// Check bounds
		if OverflowHeaderSize+toRead > len(pageData) {
			return nil, fmt.Errorf("overflow page %d data exceeds page bounds", currentPage)
		}

		// Copy data from this page
		copy(result[offset:offset+toRead], pageData[OverflowHeaderSize:OverflowHeaderSize+toRead])

		offset += toRead
		currentPage = nextPage
	}

	if offset < dataSize {
		return nil, fmt.Errorf("overflow chain ended prematurely, expected %d bytes, got %d", dataSize, offset)
	}

	return result, nil
}

// FreeOverflowChain frees all pages in an overflow chain
// firstOverflowPage is the page number of the first overflow page
// This should be called when deleting a cell with overflow pages
func (c *BtCursor) FreeOverflowChain(firstOverflowPage uint32) error {
	if firstOverflowPage == 0 {
		return nil // No overflow pages to free
	}

	if c.Btree == nil {
		return fmt.Errorf("cursor has no btree")
	}

	return freeOverflowChain(c.Btree, firstOverflowPage, c.Btree.UsableSize)
}

// freeOverflowChain frees all pages in an overflow chain
func freeOverflowChain(bt *Btree, firstPage uint32, usableSize uint32) error {
	currentPage := firstPage
	pageCount := 0
	maxPages := 1000 // Safety limit to prevent infinite loops

	for currentPage != 0 {
		pageCount++
		if pageCount > maxPages {
			return fmt.Errorf("overflow chain too long (possible corruption), freed %d pages", pageCount)
		}

		// Get the overflow page
		pageData, err := bt.GetPage(currentPage)
		if err != nil {
			return fmt.Errorf("failed to get overflow page %d: %w", currentPage, err)
		}

		// Read next page pointer before freeing
		nextPage := binary.BigEndian.Uint32(pageData[0:4])

		// Free the current page
		// In a full implementation, this would add the page to a freelist
		// For now, we just remove it from the page cache
		delete(bt.Pages, currentPage)

		// Move to next page
		currentPage = nextPage
	}

	return nil
}

// CalculateLocalPayload calculates how much of the payload should be stored
// locally in the cell vs in overflow pages
// This implements SQLite's overflow calculation algorithm
func CalculateLocalPayload(totalSize uint32, pageSize uint32, isTable bool) uint16 {
	usableSize := pageSize

	maxLocal := calculateMaxLocal(usableSize, isTable)
	minLocal := calculateMinLocal(usableSize, isTable)

	if totalSize <= maxLocal {
		// Entire payload fits locally
		return uint16(totalSize)
	}

	// Calculate surplus using SQLite's algorithm
	surplus := minLocal + (totalSize-minLocal)%(usableSize-4)
	if surplus <= maxLocal {
		return uint16(surplus)
	}

	return uint16(minLocal)
}

// Helper function to get complete payload including overflow
// This is a convenience function that automatically handles overflow reading
func (c *BtCursor) GetCompletePayload() ([]byte, error) {
	if c.State != CursorValid || c.CurrentCell == nil {
		return nil, fmt.Errorf("cursor not in valid state")
	}

	// If no overflow, return local payload
	if c.CurrentCell.OverflowPage == 0 {
		return c.CurrentCell.Payload, nil
	}

	// Read complete payload including overflow
	return c.ReadOverflow(
		c.CurrentCell.Payload,
		c.CurrentCell.OverflowPage,
		c.CurrentCell.PayloadSize,
		c.Btree.UsableSize,
	)
}
