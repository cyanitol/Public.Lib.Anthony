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
