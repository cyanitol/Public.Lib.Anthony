package vtab

import (
	"fmt"
)

// Module represents a virtual table module that can create/connect to virtual tables.
// This interface mirrors SQLite's sqlite3_module structure.
type Module interface {
	// Create creates a new virtual table instance.
	// Called when a CREATE VIRTUAL TABLE statement is executed.
	// Returns the virtual table instance and the SQL for the table schema.
	Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (VirtualTable, string, error)

	// Connect connects to an existing virtual table.
	// Called when a table is opened for use (may be called multiple times).
	// Returns the virtual table instance and the SQL for the table schema.
	Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (VirtualTable, string, error)
}

// VirtualTable represents an instance of a virtual table.
// This interface mirrors SQLite's sqlite3_vtab structure and methods.
type VirtualTable interface {
	// BestIndex analyzes query constraints and determines the best query plan.
	// This is called by the query planner to optimize virtual table access.
	BestIndex(info *IndexInfo) error

	// Open creates a new cursor for iterating over the virtual table.
	Open() (VirtualCursor, error)

	// Disconnect is called when the last reference to the table is closed.
	// For eponymous virtual tables, this may never be called.
	Disconnect() error

	// Destroy is called when a DROP TABLE statement is executed.
	// This should clean up any persistent state.
	Destroy() error

	// Update handles INSERT, UPDATE, and DELETE operations.
	// The operation type is determined by the arguments:
	// - DELETE: argc=1, argv[0]=rowid
	// - INSERT: argc>1, argv[0]=NULL or 0, argv[1]=new rowid or NULL
	// - UPDATE: argc>1, argv[0]=old rowid, argv[1]=new rowid
	// Returns the rowid of the inserted/updated row (for INSERT/UPDATE).
	Update(argc int, argv []interface{}) (int64, error)

	// Begin starts a multi-statement transaction (optional).
	Begin() error

	// Sync is called during the commit of a multi-statement transaction (optional).
	Sync() error

	// Commit commits a multi-statement transaction (optional).
	Commit() error

	// Rollback rolls back a multi-statement transaction (optional).
	Rollback() error

	// Rename is called when the table is renamed via ALTER TABLE (optional).
	Rename(newName string) error
}

// VirtualCursor represents a cursor for iterating over virtual table rows.
// This interface mirrors SQLite's sqlite3_vtab_cursor structure and methods.
type VirtualCursor interface {
	// Filter initializes a cursor for scanning.
	// idxNum and idxStr come from the BestIndex output.
	// argv contains the constraint values corresponding to the constraints enabled in BestIndex.
	Filter(idxNum int, idxStr string, argv []interface{}) error

	// Next advances the cursor to the next row.
	// Returns an error if the operation fails.
	Next() error

	// EOF returns true if the cursor has reached the end of the result set.
	EOF() bool

	// Column returns the value of the column at the given index for the current row.
	// The index is 0-based and corresponds to the table's column order.
	Column(index int) (interface{}, error)

	// Rowid returns the unique rowid for the current row.
	// For tables without an explicit rowid, this can be a synthesized value.
	Rowid() (int64, error)

	// Close closes the cursor and releases any resources.
	Close() error
}

// BaseModule provides default implementations for optional Module methods.
// Embedders can override specific methods as needed.
type BaseModule struct{}

// Create provides a default implementation that returns an error.
// Virtual table modules should override this if they support CREATE VIRTUAL TABLE.
func (bm *BaseModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (VirtualTable, string, error) {
	return nil, "", fmt.Errorf("CREATE VIRTUAL TABLE not supported for module %s", moduleName)
}

// Connect provides a default implementation that returns an error.
// Virtual table modules should override this to support table access.
func (bm *BaseModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (VirtualTable, string, error) {
	return nil, "", fmt.Errorf("CONNECT not supported for module %s", moduleName)
}

// BaseVirtualTable provides default implementations for optional VirtualTable methods.
type BaseVirtualTable struct{}

// BestIndex provides a default implementation that accepts all constraints.
func (bvt *BaseVirtualTable) BestIndex(info *IndexInfo) error {
	// Default: don't use any constraints, scan entire table
	info.EstimatedCost = 1000000.0
	info.EstimatedRows = 1000000
	return nil
}

// Open must be implemented by virtual table implementations.
func (bvt *BaseVirtualTable) Open() (VirtualCursor, error) {
	return nil, fmt.Errorf("Open not implemented")
}

// Disconnect provides a default no-op implementation.
func (bvt *BaseVirtualTable) Disconnect() error {
	return nil
}

// Destroy provides a default implementation that returns an error.
func (bvt *BaseVirtualTable) Destroy() error {
	return fmt.Errorf("DROP TABLE not supported for this virtual table")
}

// Update provides a default implementation that returns an error.
func (bvt *BaseVirtualTable) Update(argc int, argv []interface{}) (int64, error) {
	return 0, fmt.Errorf("virtual table is read-only")
}

// Begin provides a default no-op implementation.
func (bvt *BaseVirtualTable) Begin() error {
	return nil
}

// Sync provides a default no-op implementation.
func (bvt *BaseVirtualTable) Sync() error {
	return nil
}

// Commit provides a default no-op implementation.
func (bvt *BaseVirtualTable) Commit() error {
	return nil
}

// Rollback provides a default no-op implementation.
func (bvt *BaseVirtualTable) Rollback() error {
	return nil
}

// Rename provides a default implementation that returns an error.
func (bvt *BaseVirtualTable) Rename(newName string) error {
	return fmt.Errorf("ALTER TABLE RENAME not supported for this virtual table")
}

// BaseCursor provides default implementations for VirtualCursor methods.
type BaseCursor struct {
	eof bool
}

// Filter provides a default implementation that immediately sets EOF.
func (bc *BaseCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	bc.eof = true
	return nil
}

// Next provides a default implementation that sets EOF.
func (bc *BaseCursor) Next() error {
	bc.eof = true
	return nil
}

// EOF returns whether the cursor is at the end of the result set.
func (bc *BaseCursor) EOF() bool {
	return bc.eof
}

// Column must be implemented by cursor implementations.
func (bc *BaseCursor) Column(index int) (interface{}, error) {
	return nil, fmt.Errorf("Column not implemented")
}

// Rowid must be implemented by cursor implementations.
func (bc *BaseCursor) Rowid() (int64, error) {
	return 0, fmt.Errorf("Rowid not implemented")
}

// Close provides a default no-op implementation.
func (bc *BaseCursor) Close() error {
	return nil
}
