// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

// PagerReader defines read-only operations on a pager.
// This interface provides access to pages and metadata without modifying the database.
type PagerReader interface {
	// Get retrieves a page from the database
	Get(pgno Pgno) (*DbPage, error)

	// Put releases a reference to a page
	Put(page *DbPage)

	// PageSize returns the page size of the database
	PageSize() int

	// PageCount returns the number of pages in the database
	PageCount() Pgno

	// IsReadOnly returns true if the pager is read-only
	IsReadOnly() bool

	// GetHeader returns the database header
	GetHeader() *DatabaseHeader

	// GetFreePageCount returns the number of free pages in the database
	GetFreePageCount() uint32
}

// PagerWriter defines write operations on a pager.
// This interface allows modification of pages and allocation of new pages.
type PagerWriter interface {
	PagerReader

	// Write marks a page as writeable and journals it if necessary
	Write(page *DbPage) error

	// AllocatePage allocates a new page
	AllocatePage() (Pgno, error)

	// FreePage adds a page to the free list for later reuse
	FreePage(pgno Pgno) error

	// Vacuum compacts and rebuilds the database
	Vacuum(opts *VacuumOptions) error

	// SetUserVersion sets the user version in the database header
	SetUserVersion(version uint32) error

	// SetSchemaCookie sets the schema cookie in the database header
	SetSchemaCookie(cookie uint32) error

	// VerifyFreeList checks the integrity of the free list
	VerifyFreeList() error
}

// PagerTransaction defines transaction control operations.
// This interface manages transaction lifecycle including savepoints.
type PagerTransaction interface {
	// BeginRead starts a read transaction
	BeginRead() error

	// EndRead ends a read transaction
	EndRead() error

	// BeginWrite starts a write transaction
	BeginWrite() error

	// Commit commits the current write transaction
	Commit() error

	// Rollback rolls back the current write transaction
	Rollback() error

	// InWriteTransaction returns true if a write transaction is active
	InWriteTransaction() bool

	// Savepoint creates a savepoint for nested transaction support
	Savepoint(name string) error

	// Release releases a savepoint
	Release(name string) error

	// RollbackTo rolls back to a savepoint
	RollbackTo(name string) error
}

// PagerInterface defines the common interface for both file-based and memory pagers.
// This allows the driver and btree layers to work with either type transparently.
// It combines all three segregated interfaces for full pager functionality.
type PagerInterface interface {
	PagerReader
	PagerWriter
	PagerTransaction

	// Close closes the pager and releases all resources
	Close() error
}

// Verify that both Pager and MemoryPager implement the interface
var _ PagerInterface = (*Pager)(nil)
var _ PagerInterface = (*MemoryPager)(nil)

// Verify segregated interfaces are satisfied
var _ PagerReader = (*Pager)(nil)
var _ PagerReader = (*MemoryPager)(nil)
var _ PagerWriter = (*Pager)(nil)
var _ PagerWriter = (*MemoryPager)(nil)
var _ PagerTransaction = (*Pager)(nil)
var _ PagerTransaction = (*MemoryPager)(nil)
