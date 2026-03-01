# Virtual Table Quick Start Guide

This is a quick reference for using the virtual table API in Anthony SQLite.

## 5-Minute Quick Start

### 1. Import Required Packages

```go
import (
    "context"
    "database/sql"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
)
```

### 2. Implement Your Module

```go
type MyModule struct {
    vtab.BaseModule  // Embed base for default behavior
}

func (m *MyModule) Create(db interface{}, moduleName, dbName, tableName string, args []string) (vtab.VirtualTable, string, error) {
    schema := "CREATE TABLE my_table(id INTEGER, value TEXT)"
    return &MyTable{}, schema, nil
}

func (m *MyModule) Connect(db interface{}, moduleName, dbName, tableName string, args []string) (vtab.VirtualTable, string, error) {
    return m.Create(db, moduleName, dbName, tableName, args)
}
```

### 3. Implement Your Virtual Table

```go
type MyTable struct {
    vtab.BaseVirtualTable  // Embed base for default behavior
}

func (t *MyTable) BestIndex(info *vtab.IndexInfo) error {
    info.EstimatedCost = 100.0
    info.EstimatedRows = 1000
    return nil
}

func (t *MyTable) Open() (vtab.VirtualCursor, error) {
    return &MyCursor{table: t}, nil
}
```

### 4. Implement Your Cursor

```go
type MyCursor struct {
    vtab.BaseCursor  // Embed base for default behavior
    table *MyTable
    pos   int
    data  []Row
}

func (c *MyCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
    c.data = []Row{{ID: 1, Value: "Alice"}, {ID: 2, Value: "Bob"}}
    c.pos = 0
    return nil
}

func (c *MyCursor) Next() error {
    c.pos++
    return nil
}

func (c *MyCursor) EOF() bool {
    return c.pos >= len(c.data)
}

func (c *MyCursor) Column(index int) (interface{}, error) {
    row := c.data[c.pos]
    switch index {
    case 0: return row.ID, nil
    case 1: return row.Value, nil
    default: return nil, fmt.Errorf("invalid column")
    }
}

func (c *MyCursor) Rowid() (int64, error) {
    return c.data[c.pos].ID, nil
}
```

### 5. Register and Use

```go
// Open database
db, err := sql.Open("sqlite_internal", ":memory:")
if err != nil {
    panic(err)
}
defer db.Close()

// Get connection
conn, err := db.Conn(context.Background())
if err != nil {
    panic(err)
}
defer conn.Close()

// Register module
err = conn.Raw(func(driverConn interface{}) error {
    c := driverConn.(*driver.Conn)
    return c.RegisterVirtualTableModule("my_module", &MyModule{})
})
if err != nil {
    panic(err)
}

// Now you can use: CREATE VIRTUAL TABLE foo USING my_module();
```

## Common Patterns

### Read-Only Table with Static Data

```go
type StaticModule struct {
    vtab.BaseModule
    data []Row
}

func (m *StaticModule) Create(...) (vtab.VirtualTable, string, error) {
    return &StaticTable{rows: m.data}, schema, nil
}
```

### Table with Constraint Optimization

```go
func (t *MyTable) BestIndex(info *vtab.IndexInfo) error {
    // Check for ID constraint
    idx := info.FindConstraint(0, vtab.ConstraintEQ)
    if idx >= 0 {
        info.SetConstraintUsage(idx, 1, true)
        info.EstimatedCost = 1.0
        info.IdxNum = 1  // Flag for ID lookup
        return nil
    }

    // Full table scan
    info.EstimatedCost = 100.0
    info.IdxNum = 0
    return nil
}

func (c *MyCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
    if idxNum == 1 && len(argv) > 0 {
        // ID-based lookup
        targetID := argv[0].(int64)
        c.data = findRowsByID(targetID)
    } else {
        // Full scan
        c.data = loadAllRows()
    }
    c.pos = 0
    return nil
}
```

### Writable Virtual Table

```go
func (t *MyTable) Update(argc int, argv []interface{}) (int64, error) {
    if argc == 1 {
        // DELETE
        rowid := argv[0].(int64)
        return 0, t.deleteRow(rowid)
    }

    if argc > 1 && argv[0] == nil {
        // INSERT
        return t.insertRow(argv[1:])
    }

    // UPDATE
    oldRowid := argv[0].(int64)
    newRowid := argv[1].(int64)
    return newRowid, t.updateRow(oldRowid, newRowid, argv[2:])
}
```

## Key Concepts

### Module vs Table vs Cursor

- **Module**: Factory that creates virtual tables
- **VirtualTable**: Represents a single virtual table instance
- **VirtualCursor**: Iterator for scanning rows

### BestIndex

The query planner calls this to determine the best way to query your table:

```go
type IndexInfo struct {
    // Input: What constraints are available
    Constraints []IndexConstraint

    // Output: How you'll handle them
    ConstraintUsage []IndexConstraintUsage
    IdxNum          int       // Your custom strategy ID
    EstimatedCost   float64   // Lower is better
    EstimatedRows   int64     // Expected result size
}
```

### Filter

The Filter method receives the query plan from BestIndex:

```go
func (c *MyCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
    // idxNum: The IdxNum you set in BestIndex
    // argv: Values for constraints you marked as used

    // Load/filter data based on the plan
    c.data = loadData(idxNum, argv)
    c.pos = 0
    return nil
}
```

## Constraint Types

Common constraint operators you can optimize for:

```go
vtab.ConstraintEQ         // =
vtab.ConstraintGT         // >
vtab.ConstraintLE         // <=
vtab.ConstraintLT         // <
vtab.ConstraintGE         // >=
vtab.ConstraintNE         // !=
vtab.ConstraintLike       // LIKE
vtab.ConstraintIsNull     // IS NULL
vtab.ConstraintIsNotNull  // IS NOT NULL
```

## Return Types

Column values should be one of:

- `int64` - Integer values
- `float64` - Floating point values
- `string` - Text values
- `[]byte` - Blob values
- `nil` - NULL values

## Error Handling

```go
// Check connection closed
if c.closed {
    return driver.ErrBadConn
}

// Return meaningful errors
if index >= len(columns) {
    return nil, fmt.Errorf("column index %d out of range", index)
}

// Validate cursor state
if c.EOF() {
    return nil, fmt.Errorf("cursor is at EOF")
}
```

## Testing

```go
func TestMyVirtualTable(t *testing.T) {
    // Create module
    module := &MyModule{}
    vtable, schema, err := module.Create(nil, "test", "main", "test", nil)
    if err != nil {
        t.Fatal(err)
    }

    // Open cursor
    cursor, err := vtable.Open()
    if err != nil {
        t.Fatal(err)
    }
    defer cursor.Close()

    // Filter and iterate
    err = cursor.Filter(0, "", nil)
    if err != nil {
        t.Fatal(err)
    }

    for !cursor.EOF() {
        val, err := cursor.Column(0)
        if err != nil {
            t.Fatal(err)
        }
        t.Logf("Value: %v", val)
        cursor.Next()
    }
}
```

## Complete Examples

- `/examples/virtual_table_example.go` - Complete CSV-like table
- `/internal/driver/vtable_test.go` - Test implementations
- `/internal/vtab/builtin/sqlite_master.go` - Built-in sqlite_master table

## Documentation

- `/docs/VIRTUAL_TABLES.md` - Full documentation
- `/PHASE_8.3_SUMMARY.md` - Implementation details

## Common Pitfalls

1. **Forgetting to check EOF** - Always check before accessing cursor data
2. **Wrong return types** - Use int64, float64, string, []byte, or nil
3. **Not embedding base types** - Use vtab.BaseModule, vtab.BaseVirtualTable, vtab.BaseCursor
4. **Poor BestIndex estimates** - Provide realistic cost estimates for good performance
5. **Not handling NULL** - Always handle nil values in argv

## Best Practices

1. Embed base types for default implementations
2. Provide accurate cost estimates in BestIndex
3. Use constraints to reduce data scanned
4. Clean up resources in Close()
5. Return proper error messages
6. Test with various query patterns
7. Consider thread safety if data is mutable

## Getting Help

- Read the full documentation: `/docs/VIRTUAL_TABLES.md`
- Check the examples: `/examples/virtual_table_example.go`
- Look at test cases: `/internal/driver/vtable_test.go`
- Review SQLite docs: https://www.sqlite.org/vtab.html
