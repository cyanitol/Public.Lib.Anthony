package pager

// PagerInterface defines the common interface for both file-based and memory pagers.
// This allows the driver and btree layers to work with either type transparently.
type PagerInterface interface {
	// Get retrieves a page from the database
	Get(pgno Pgno) (*DbPage, error)

	// Put releases a reference to a page
	Put(page *DbPage)

	// Write marks a page as writeable and journals it if necessary
	Write(page *DbPage) error

	// Commit commits the current write transaction
	Commit() error

	// Rollback rolls back the current write transaction
	Rollback() error

	// Close closes the pager and releases all resources
	Close() error

	// PageSize returns the page size of the database
	PageSize() int

	// PageCount returns the number of pages in the database
	PageCount() Pgno

	// IsReadOnly returns true if the pager is read-only
	IsReadOnly() bool

	// GetHeader returns the database header
	GetHeader() *DatabaseHeader

	// AllocatePage allocates a new page
	AllocatePage() (Pgno, error)

	// FreePage adds a page to the free list for later reuse
	FreePage(pgno Pgno) error

	// GetFreePageCount returns the number of free pages in the database
	GetFreePageCount() uint32

	// BeginRead starts a read transaction
	BeginRead() error

	// BeginWrite starts a write transaction
	BeginWrite() error

	// Savepoint creates a savepoint for nested transaction support
	Savepoint(name string) error

	// Release releases a savepoint
	Release(name string) error

	// RollbackTo rolls back to a savepoint
	RollbackTo(name string) error

	// InWriteTransaction returns true if a write transaction is active
	InWriteTransaction() bool

	// EndRead ends a read transaction
	EndRead() error

	// Vacuum compacts and rebuilds the database
	Vacuum(opts *VacuumOptions) error
}

// Verify that both Pager and MemoryPager implement the interface
var _ PagerInterface = (*Pager)(nil)
var _ PagerInterface = (*MemoryPager)(nil)
