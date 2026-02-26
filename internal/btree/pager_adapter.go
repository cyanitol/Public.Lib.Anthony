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
	// Get the next page number
	pgno := pa.nextPage
	pa.nextPage++

	// Create empty page data
	data := make([]byte, pa.pageSize)

	return pgno, data, nil
}

// MarkDirty marks a page as dirty
func (pa *PagerAdapter) MarkDirty(pgno uint32) error {
	pa.dirtyPages[pgno] = true
	return nil
}
