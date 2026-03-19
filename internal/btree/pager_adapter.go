// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"fmt"
)

// PagerInterface defines the minimal interface needed from the pager
type PagerInterface interface {
	Get(pgno uint32) (interface{}, error)
	Write(page interface{}) error
	PageSize() int
	PageCount() uint32
	AllocatePage() (uint32, error)
}

// DbPageInterface is what we need from a DbPage
type DbPageInterface interface {
	GetData() []byte
	GetPgno() uint32
}

// PagerAdapter adapts a pager to the PageProvider interface
type PagerAdapter struct {
	pager      PagerInterface
	pageSize   int
	nextPage   uint32
	dirtyPages map[uint32]bool
}

// NewPagerAdapter creates a new adapter
func NewPagerAdapter(pager PagerInterface) *PagerAdapter {
	return &PagerAdapter{
		pager:      pager,
		pageSize:   pager.PageSize(),
		nextPage:   pager.PageCount() + 1,
		dirtyPages: make(map[uint32]bool),
	}
}

// GetPageData retrieves page data from the pager
func (pa *PagerAdapter) GetPageData(pgno uint32) ([]byte, error) {
	page, err := pa.pager.Get(pgno)
	if err != nil {
		return nil, err
	}

	// Try to get the Data field via interface
	if dp, ok := page.(DbPageInterface); ok {
		return dp.GetData(), nil
	}

	// If we can't get the data, return an error
	return nil, fmt.Errorf("page does not implement DbPageInterface")
}

// AllocatePageData allocates a new page
func (pa *PagerAdapter) AllocatePageData() (uint32, []byte, error) {
	// Use the pager's AllocatePage method which handles free list
	pgno, err := pa.pager.AllocatePage()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to allocate page: %w", err)
	}

	// Update nextPage tracking if this page is beyond current count
	if pgno >= pa.nextPage {
		pa.nextPage = pgno + 1
	}

	// Create empty page data
	data := make([]byte, pa.pageSize)

	return pgno, data, nil
}

// MarkDirty marks a page as dirty by calling pager.Write() to journal it
func (pa *PagerAdapter) MarkDirty(pgno uint32) error {
	// Get the page from the pager
	page, err := pa.pager.Get(pgno)
	if err != nil {
		return fmt.Errorf("failed to get page %d: %w", pgno, err)
	}

	// Call Write() to journal the page before modification
	// This ensures the page state is saved for rollback/savepoint support
	if err := pa.pager.Write(page); err != nil {
		return fmt.Errorf("failed to write page %d: %w", pgno, err)
	}

	pa.dirtyPages[pgno] = true
	return nil
}
