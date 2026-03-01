// Package types provides typed interfaces to break import cycles between packages.
// These interfaces replace the use of interface{} in VDBEContext and other contexts
// where circular dependencies would otherwise occur.
package types

// BtreeAccess defines the methods needed for accessing the B-tree storage layer.
// This interface allows VDBE and other components to interact with the btree
// without importing the btree package directly, avoiding import cycles.
type BtreeAccess interface {
	// CreateTable creates a new table and returns its root page number
	CreateTable() (uint32, error)

	// AllocatePage allocates a new page and returns its page number
	AllocatePage() (uint32, error)

	// GetPage retrieves the data for a specific page
	GetPage(pageNum uint32) ([]byte, error)

	// SetPage updates the data for a specific page
	SetPage(pageNum uint32, data []byte) error

	// NewRowid generates a new unique rowid for the specified table
	NewRowid(rootPage uint32) (int64, error)
}

// PagerWriter defines the methods needed for writing to the pager layer.
// This interface allows components to interact with the pager for transaction
// management without importing the pager package directly.
type PagerWriter interface {
	// BeginRead starts a read transaction
	BeginRead() error

	// BeginWrite starts a write transaction
	BeginWrite() error

	// Commit commits the current transaction
	Commit() error

	// Rollback rolls back the current transaction
	Rollback() error

	// EndRead ends a read transaction
	EndRead() error

	// InTransaction returns true if any transaction is active
	InTransaction() bool

	// InWriteTransaction returns true if a write transaction is active
	InWriteTransaction() bool
}

// SavepointPager extends PagerWriter with savepoint support.
// Savepoints allow nested transactions and partial rollbacks.
type SavepointPager interface {
	PagerWriter

	// Savepoint creates a new savepoint with the given name
	Savepoint(name string) error

	// Release releases a savepoint and all savepoints created after it
	Release(name string) error

	// RollbackTo rolls back to a savepoint, undoing all changes after it
	RollbackTo(name string) error
}

// CookiePager extends PagerWriter with cookie (metadata) support.
// Cookies are used for schema versioning and other database metadata.
type CookiePager interface {
	PagerWriter

	// GetCookie retrieves a cookie value
	// dbIndex: database index (0 for main)
	// cookieType: type of cookie to retrieve
	GetCookie(dbIndex int, cookieType int) (uint32, error)

	// SetCookie sets a cookie value
	// dbIndex: database index (0 for main)
	// cookieType: type of cookie to set
	// value: new cookie value
	SetCookie(dbIndex int, cookieType int, value uint32) error
}

// SchemaAccess defines the methods needed for accessing schema metadata.
// This interface allows components to query and modify database schema
// without importing the schema package directly.
type SchemaAccess interface {
	// GetTable retrieves table metadata by name
	// Returns (table, true) if found, (nil, false) otherwise
	GetTable(name string) (interface{}, bool)

	// GetIndex retrieves index metadata by name
	// Returns (index, true) if found, (nil, false) otherwise
	GetIndex(name string) (interface{}, bool)

	// GetTableByRootPage retrieves table metadata by root page number
	// Returns (table, true) if found, (nil, false) otherwise
	GetTableByRootPage(rootPage uint32) (interface{}, bool)
}

// CollationRegistry defines the methods needed for managing collation sequences.
// This interface allows components to work with collations without importing
// the collation package directly, avoiding import cycles.
type CollationRegistry interface {
	// Register registers a new collation sequence
	Register(name string, fn interface{}) error

	// Get retrieves a collation by name
	// Returns (collation, true) if found, (nil, false) otherwise
	Get(name string) (interface{}, bool)

	// Unregister removes a collation sequence
	Unregister(name string) error
}
