package btree

import (
	"fmt"
)

// Balance thresholds based on SQLite's balancing strategy
const (
	// MinFillFactor is the minimum percentage of page space that should be used
	// before considering the page underfull (approximately 33% for SQLite)
	MinFillFactor = 0.33

	// MaxFillFactor is the maximum percentage before overflow (100%)
	MaxFillFactor = 1.0

	// MinCellsPerPage is the minimum number of cells that must fit on a page
	// SQLite requires at least 4 cells per page
	MinCellsPerPage = 4
)

// isOverfull checks if a page has too many cells or too little free space
// A page is overfull if:
// 1. It cannot accommodate one more minimum-sized cell (4 bytes)
// 2. Free space is negative or insufficient for basic operations
func isOverfull(page *BtreePage) bool {
	freeSpace := page.FreeSpace()

	// A page is overfull if it can't fit even a minimum cell
	// Minimum cell size is 4 bytes plus 2 bytes for cell pointer
	minCellWithPointer := 4 + 2

	return freeSpace < minCellWithPointer
}

// isUnderfull checks if a page has too few cells or too much free space
// A page is underfull if:
// 1. Free space exceeds (1 - MinFillFactor) of usable size
// 2. The page has cells but is using less than MinFillFactor of space
func isUnderfull(page *BtreePage) bool {
	// Empty pages (no cells) are not considered underfull
	// They are valid leaf pages
	if page.Header.NumCells == 0 {
		return false
	}

	// Calculate used space
	usableSize := int(page.UsableSize)
	freeSpace := page.FreeSpace()
	usedSpace := usableSize - freeSpace - page.Header.HeaderSize - (int(page.Header.NumCells) * 2)

	// Page is underfull if used space is less than MinFillFactor of usable size
	minUsedSpace := int(float64(usableSize) * MinFillFactor)

	return usedSpace < minUsedSpace
}

// defragmentPage defragments a page by compacting cell content
// This removes fragmented free space and makes all free space contiguous
// This is a wrapper around BtreePage.Defragment() for consistency with the balance API
func defragmentPage(page *BtreePage) error {
	return page.Defragment()
}

// IsOverfull is a method wrapper for isOverfull for convenience
func (p *BtreePage) IsOverfull() bool {
	return isOverfull(p)
}

// IsUnderfull is a method wrapper for isUnderfull for convenience
func (p *BtreePage) IsUnderfull() bool {
	return isUnderfull(p)
}

// balance is the main entry point for balancing a B-tree after insert/delete operations
// It examines the cursor's current page and decides whether to split, merge, or redistribute
//
// The algorithm:
// 1. Check if the current page is overfull -> needs split
// 2. Check if the current page is underfull -> needs merge or redistribution
// 3. If neither, the page is balanced
//
// Note: This is a simplified implementation. A full implementation would:
// - Actually perform page splits and merges
// - Update parent pages with new keys
// - Propagate changes up to root
// - Handle root page splits (creating new root)
// - Redistribute cells among siblings
func balance(cursor *BtCursor) error {
	if cursor.State != CursorValid {
		return fmt.Errorf("cannot balance: cursor not in valid state")
	}

	// Get current page
	pageData, err := cursor.Btree.GetPage(cursor.CurrentPage)
	if err != nil {
		return fmt.Errorf("failed to get page %d: %w", cursor.CurrentPage, err)
	}

	// Wrap in BtreePage for analysis
	page, err := NewBtreePage(cursor.CurrentPage, pageData, cursor.Btree.UsableSize)
	if err != nil {
		return fmt.Errorf("failed to parse page %d: %w", cursor.CurrentPage, err)
	}

	// Check if page is overfull
	if isOverfull(page) {
		return handleOverfullPage(cursor, page)
	}

	// Check if page is underfull
	if isUnderfull(page) {
		return handleUnderfullPage(cursor, page)
	}

	// Page is balanced - may still benefit from defragmentation
	if page.Header.FragmentedBytes > 0 {
		if err := defragmentPage(page); err != nil {
			return fmt.Errorf("failed to defragment page %d: %w", cursor.CurrentPage, err)
		}
	}

	return nil
}

// handleOverfullPage handles the case when a page is overfull
// Returns an error indicating that a split is needed
func handleOverfullPage(cursor *BtCursor, page *BtreePage) error {
	// First, try defragmentation to reclaim fragmented space
	if page.Header.FragmentedBytes > 0 {
		if err := defragmentPage(page); err != nil {
			return fmt.Errorf("failed to defragment overfull page %d: %w", cursor.CurrentPage, err)
		}

		// Check again after defragmentation
		if !isOverfull(page) {
			return nil // Defragmentation solved the problem
		}
	}

	// Page is still overfull after defragmentation - needs split
	return fmt.Errorf("page %d is overfull and requires split", cursor.CurrentPage)
}

// handleUnderfullPage handles the case when a page is underfull
// Returns information about what action is needed
func handleUnderfullPage(cursor *BtCursor, page *BtreePage) error {
	// If this is the root page, it's allowed to be underfull
	if cursor.CurrentPage == cursor.RootPage {
		return nil
	}

	// Defragment to consolidate free space
	if page.Header.FragmentedBytes > 0 {
		if err := defragmentPage(page); err != nil {
			return fmt.Errorf("failed to defragment underfull page %d: %w", cursor.CurrentPage, err)
		}
	}

	// Check if we're at depth 0 (root only)
	if cursor.Depth == 0 {
		return nil
	}

	// For non-root pages, we need to check siblings for merge/redistribution
	// This is a simplified implementation - just report the state
	return fmt.Errorf("page %d is underfull and may need merge or redistribution", cursor.CurrentPage)
}

// BalanceInfo provides information about page balance state
type BalanceInfo struct {
	PageNum        uint32  // Page number
	NumCells       uint16  // Number of cells on page
	FreeSpace      int     // Free space in bytes
	UsedSpace      int     // Used space in bytes
	UsableSize     int     // Total usable size
	FillFactor     float64 // Percentage of space used (0.0 - 1.0)
	IsOverfull     bool    // True if page is overfull
	IsUnderfull    bool    // True if page is underfull
	IsBalanced     bool    // True if page is balanced
	FragmentedBytes byte   // Fragmented free bytes
}

// GetBalanceInfo returns detailed balance information for a page
func GetBalanceInfo(bt *Btree, pageNum uint32) (*BalanceInfo, error) {
	pageData, err := bt.GetPage(pageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get page %d: %w", pageNum, err)
	}

	page, err := NewBtreePage(pageNum, pageData, bt.UsableSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse page %d: %w", pageNum, err)
	}

	freeSpace := page.FreeSpace()
	usableSize := int(page.UsableSize)
	usedSpace := usableSize - freeSpace - page.Header.HeaderSize - (int(page.Header.NumCells) * 2)

	fillFactor := 0.0
	if usableSize > 0 {
		fillFactor = float64(usedSpace) / float64(usableSize)
	}

	overfull := isOverfull(page)
	underfull := isUnderfull(page)

	return &BalanceInfo{
		PageNum:         pageNum,
		NumCells:        page.Header.NumCells,
		FreeSpace:       freeSpace,
		UsedSpace:       usedSpace,
		UsableSize:      usableSize,
		FillFactor:      fillFactor,
		IsOverfull:      overfull,
		IsUnderfull:     underfull,
		IsBalanced:      !overfull && !underfull,
		FragmentedBytes: page.Header.FragmentedBytes,
	}, nil
}

// String returns a string representation of balance info
func (bi *BalanceInfo) String() string {
	status := "balanced"
	if bi.IsOverfull {
		status = "OVERFULL"
	} else if bi.IsUnderfull {
		status = "UNDERFULL"
	}

	return fmt.Sprintf("Page %d: %d cells, %d/%d bytes used (%.1f%%), %s, fragmented=%d",
		bi.PageNum, bi.NumCells, bi.UsedSpace, bi.UsableSize,
		bi.FillFactor*100, status, bi.FragmentedBytes)
}
