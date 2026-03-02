// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

// PagerInterface defines the methods needed by VDBE for transaction management.
// This interface allows VDBE to interact with the pager without creating import cycles.
type PagerInterface interface {
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

// SavepointPagerInterface extends PagerInterface with savepoint support.
// Savepoints allow nested transactions and partial rollbacks.
type SavepointPagerInterface interface {
	PagerInterface

	// Savepoint creates a new savepoint with the given name
	Savepoint(name string) error

	// Release releases a savepoint and all savepoints created after it
	Release(name string) error

	// RollbackTo rolls back to a savepoint, undoing all changes after it
	RollbackTo(name string) error
}

// CookiePagerInterface extends PagerInterface with cookie (metadata) support.
// Cookies are used for schema versioning and other database metadata.
type CookiePagerInterface interface {
	PagerInterface

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
