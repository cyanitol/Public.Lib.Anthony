# Virtual Table Package

This package implements virtual table support for the pure Go SQLite database engine.

## Overview

Virtual tables allow SQLite to access data that is not stored in the database file. This package provides the infrastructure for registering and using virtual table modules.

## Architecture

### Components

1. **VTabModule** - Interface for virtual table implementations
2. **VTabCursor** - Interface for virtual table cursors
3. **ModuleRegistry** - Thread-safe registry for virtual table modules
4. **IndexInfo** - Query planning and optimization information

## Virtual Table Module Interface

```go
type VTabModule interface {
    // Create creates a new virtual table instance
    Create(db *Database, moduleName, dbName, tableName string, args []string) (VTab, error)

    // Connect connects to an existing virtual table
    Connect(db *Database, moduleName, dbName, tableName string, args []string) (VTab, error)

    // Name returns the module name
    Name() string
}
```

## Virtual Table Interface

```go
type VTab interface {
    // BestIndex provides query planning information
    BestIndex(info *IndexInfo) error

    // Open creates a new cursor for scanning the virtual table
    Open() (VTabCursor, error)

    // Disconnect disconnects from the virtual table
    Disconnect() error

    // Destroy destroys the virtual table
    Destroy() error
}
```

## Virtual Table Cursor Interface

```go
type VTabCursor interface {
    // Filter begins a search of the virtual table
    Filter(idxNum int, idxStr string, values []interface{}) error

    // Next advances the cursor to the next row
    Next() error

    // EOF returns true if the cursor is at EOF
    EOF() bool

    // Column returns the value of the Nth column
    Column(n int) (interface{}, error)

    // Rowid returns the rowid of the current row
    Rowid() (int64, error)

    // Close closes the cursor
    Close() error
}
```

## Module Registry

### Registration

```go
// Register a virtual table module
err := vtab.RegisterModule("my_vtab", myModule)
if err != nil {
    log.Fatal(err)
}

// Get a registered module
module, err := vtab.GetModule("my_vtab")
if err != nil {
    log.Fatal(err)
}

// Unregister a module
err = vtab.UnregisterModule("my_vtab")
```

## SQL Usage

```sql
-- Create a virtual table
CREATE VIRTUAL TABLE temp.my_table USING my_vtab(arg1, arg2);

-- Query the virtual table
SELECT * FROM my_table WHERE col = 'value';

-- Drop the virtual table
DROP TABLE my_table;
```

## Index Planning

The BestIndex method provides query planning information to the SQLite query optimizer:

```go
type IndexInfo struct {
    Constraints []Constraint  // WHERE clause constraints
    OrderBy     []OrderByTerm // ORDER BY terms
    IndexNum    int           // Output: index number
    IndexStr    string        // Output: index strategy string
    OrderByConsumed bool      // Output: whether ORDER BY is satisfied by index
    EstimatedCost float64     // Output: estimated query cost
    EstimatedRows int64       // Output: estimated number of rows
}
```

## Example Implementation

```go
// Example: Simple CSV virtual table
type CSVModule struct {
    filePath string
}

func (m *CSVModule) Create(db *Database, moduleName, dbName, tableName string, args []string) (VTab, error) {
    // Parse CSV file and determine schema
    // Return virtual table instance
}

func (m *CSVModule) Connect(db *Database, moduleName, dbName, tableName string, args []string) (VTab, error) {
    return m.Create(db, moduleName, dbName, tableName, args)
}

func (m *CSVModule) Name() string {
    return "csv"
}

// Register the module
vtab.RegisterModule("csv", &CSVModule{})

// Use in SQL
// CREATE VIRTUAL TABLE data USING csv('data.csv');
```

## Thread Safety

The ModuleRegistry uses a sync.RWMutex to ensure thread-safe concurrent access to module registration and lookup.

## Implementation Status

### Completed
- Virtual table module interface
- Cursor interface for row iteration
- Module registry with thread-safe access
- Index planning infrastructure

### In Progress
- Integration with SQL parser for CREATE VIRTUAL TABLE
- Integration with query planner for constraint pushdown
- Built-in virtual table modules (e.g., csv, json)

## Built-in Virtual Tables

Future built-in virtual table modules may include:

- **csv** - Read CSV files as tables
- **json** - Query JSON data
- **series** - Generate integer sequences
- **pragma** - Access PRAGMA values as table
- **dbstat** - Database statistics

## References

- [SQLite Virtual Tables](https://www.sqlite.org/vtab.html)
- [SQLite Virtual Table Interface](https://www.sqlite.org/c3ref/module.html)
- [Virtual Table Best Index](https://www.sqlite.org/vtab.html#the_xbestindex_method)

## License

This implementation is part of the Anthony SQLite project and follows the project's public domain dedication.
