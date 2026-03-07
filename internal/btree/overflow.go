// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
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

	overflowPageCapacity := int(usableSize) - OverflowHeaderSize
	firstPageNum, err := bt.AllocatePage()
	if err != nil {
		return 0, fmt.Errorf("failed to allocate first overflow page: %w", err)
	}

	err = writeOverflowPages(bt, data, firstPageNum, overflowPageCapacity)
	if err != nil {
		return 0, err
	}

	return firstPageNum, nil
}

// writeOverflowPages writes data across a chain of overflow pages
func writeOverflowPages(bt *Btree, data []byte, firstPageNum uint32, pageCapacity int) error {
	prevPageNum := firstPageNum
	offset := 0

	for offset < len(data) {
		toWrite := calculateWriteAmount(offset, len(data), pageCapacity)
		nextPageNum, err := allocateNextPageIfNeeded(bt, offset, toWrite, len(data))
		if err != nil {
			return err
		}

		err = writeSingleOverflowPage(bt, prevPageNum, data, offset, toWrite, nextPageNum)
		if err != nil {
			return err
		}

		offset += toWrite
		prevPageNum = nextPageNum
	}

	return nil
}

// calculateWriteAmount determines how many bytes to write to the current page
func calculateWriteAmount(offset, dataLen, pageCapacity int) int {
	remaining := dataLen - offset
	if remaining > pageCapacity {
		return pageCapacity
	}
	return remaining
}

// allocateNextPageIfNeeded allocates a new page if more data needs to be written
func allocateNextPageIfNeeded(bt *Btree, offset, toWrite, dataLen int) (uint32, error) {
	if offset+toWrite < dataLen {
		nextPageNum, err := bt.AllocatePage()
		if err != nil {
			return 0, fmt.Errorf("failed to allocate overflow page: %w", err)
		}
		return nextPageNum, nil
	}
	return 0, nil
}

// writeSingleOverflowPage writes data to a single overflow page
func writeSingleOverflowPage(bt *Btree, pageNum uint32, data []byte, offset, toWrite int, nextPageNum uint32) error {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return fmt.Errorf("failed to get overflow page %d: %w", pageNum, err)
	}

	binary.BigEndian.PutUint32(pageData[0:4], nextPageNum)
	copy(pageData[OverflowHeaderSize:], data[offset:offset+toWrite])

	if bt.Provider != nil {
		if err := bt.Provider.MarkDirty(pageNum); err != nil {
			return fmt.Errorf("failed to mark overflow page %d dirty: %w", pageNum, err)
		}
	}

	return nil
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
	overflowPageCapacity := int(usableSize) - OverflowHeaderSize
	maxPages := (dataSize / overflowPageCapacity) + 2

	offset, err := readOverflowPages(bt, firstPage, result, dataSize, overflowPageCapacity, maxPages)
	if err != nil {
		return nil, err
	}

	if offset < dataSize {
		return nil, fmt.Errorf("overflow chain ended prematurely, expected %d bytes, got %d", dataSize, offset)
	}

	return result, nil
}

// readOverflowPages reads data from overflow page chain into result buffer
func readOverflowPages(bt *Btree, firstPage uint32, result []byte, dataSize, pageCapacity, maxPages int) (int, error) {
	currentPage := firstPage
	offset := 0
	pageCount := 0

	for offset < dataSize && currentPage != 0 {
		pageCount++
		if pageCount > maxPages {
			return 0, fmt.Errorf("overflow chain too long (possible corruption), page count: %d", pageCount)
		}

		nextPage, bytesRead, err := readSingleOverflowPage(bt, currentPage, result, offset, dataSize, pageCapacity)
		if err != nil {
			return 0, err
		}

		offset += bytesRead
		currentPage = nextPage
	}

	return offset, nil
}

// readSingleOverflowPage reads data from a single overflow page
func readSingleOverflowPage(bt *Btree, pageNum uint32, result []byte, offset, dataSize, pageCapacity int) (uint32, int, error) {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get overflow page %d: %w", pageNum, err)
	}

	nextPage := binary.BigEndian.Uint32(pageData[0:4])
	toRead := calculateReadAmount(offset, dataSize, pageCapacity)

	if OverflowHeaderSize+toRead > len(pageData) {
		return 0, 0, fmt.Errorf("overflow page %d data exceeds page bounds", pageNum)
	}

	copy(result[offset:offset+toRead], pageData[OverflowHeaderSize:OverflowHeaderSize+toRead])
	return nextPage, toRead, nil
}

// calculateReadAmount determines how many bytes to read from the current page
func calculateReadAmount(offset, dataSize, pageCapacity int) int {
	remaining := dataSize - offset
	if remaining > pageCapacity {
		return pageCapacity
	}
	return remaining
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
		return safePayloadSize(totalSize, maxLocal)
	}

	if usableSize < 4 {
		return safePayloadSize(minLocal, 0)
	}

	surplus := minLocal + (totalSize-minLocal)%(usableSize-4)
	if surplus <= maxLocal {
		return safePayloadSizeWithFallback(surplus, minLocal)
	}

	return safePayloadSize(minLocal, 0)
}

// safePayloadSize safely converts a uint32 to uint16, returning fallback on error
func safePayloadSize(size uint32, fallback uint32) uint16 {
	result, err := security.SafeCastUint32ToUint16(size)
	if err != nil {
		if fallback > 0 {
			result, err = security.SafeCastUint32ToUint16(fallback)
			if err != nil {
				return 0
			}
			return result
		}
		return 0
	}
	return result
}

// safePayloadSizeWithFallback converts primary value, falling back to secondary on error
func safePayloadSizeWithFallback(primary uint32, fallback uint32) uint16 {
	result, err := security.SafeCastUint32ToUint16(primary)
	if err != nil {
		result, err = security.SafeCastUint32ToUint16(fallback)
		if err != nil {
			return 0
		}
		return result
	}
	return result
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

// GetOverflowThreshold returns the maximum payload size that can be stored
// locally (without overflow) for the given page size and page type
// This implements SQLite's overflow threshold calculation
func GetOverflowThreshold(pageSize uint32, isTable bool) uint32 {
	// For table b-trees (leaf pages): maxLocal = pageSize - 35
	// For index b-trees: maxLocal = pageSize - 35
	// This is the threshold above which overflow is required
	return calculateMaxLocal(pageSize, isTable)
}
