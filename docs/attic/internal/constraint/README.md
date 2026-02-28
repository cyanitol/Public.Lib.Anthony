# Constraint Package

This package implements SQL constraint enforcement for the Anthony SQLite database engine.

## Overview

SQL constraints maintain data integrity by enforcing rules on table columns. This package provides:
- CHECK constraints
- UNIQUE constraints
- NOT NULL constraints (handled in schema layer)
- PRIMARY KEY constraints (handled in schema layer)
- FOREIGN KEY constraints (implementation in progress)
- Collation sequences for string comparison

## Constraint Types

### CHECK Constraints

CHECK constraints ensure that values in a column satisfy a Boolean expression.

**Features:**
- Column-level and table-level CHECK constraints
- NULL handling (NULL values pass CHECK constraints per SQL standard)
- Complex expressions using the VDBE expression evaluator
- Clear error messages with constraint names

**Usage:**
```go
// Create validator from table schema
validator := constraint.NewCheckValidator(table)

// Generate validation code for INSERT
if validator.HasCheckConstraints() {
    err := validator.ValidateInsert(
        vm,                // VDBE instance
        table.Name,        // table name
        cursorNum,         // cursor number
        recordStartReg,    // first register with row data
        numRecordCols,     // number of columns
    )
}
```

**SQL Examples:**
```sql
-- Column-level CHECK
CREATE TABLE products (
    price REAL CHECK(price > 0),
    stock INTEGER CHECK(stock >= 0)
);

-- Table-level CHECK
CREATE TABLE events (
    start_time INTEGER,
    end_time INTEGER,
    CHECK(start_time < end_time)
);

-- Named constraint
CREATE TABLE inventory (
    quantity INTEGER,
    CONSTRAINT positive_quantity CHECK(quantity >= 0)
);
```

**Implementation Details:**
- CHECK constraints are evaluated at the VDBE bytecode level
- Uses the expression code generator from `internal/expr`
- Generates 4-6 VDBE opcodes per constraint
- NULL semantics: NULL passes (treated as TRUE)

### UNIQUE Constraints

UNIQUE constraints ensure that all values in a column (or combination of columns) are distinct.

**Features:**
- Single-column and composite (multi-column) UNIQUE constraints
- Proper NULL handling (multiple NULLs allowed per SQL standard)
- Automatic backing index creation for efficient enforcement
- Named and unnamed constraints
- Detailed violation error messages

**Usage:**
```go
// Validate a row before insertion
err := constraint.ValidateTableRow(table, btree, values, rowid)
if err != nil {
    if verr, ok := err.(*constraint.UniqueViolationError); ok {
        // Handle UNIQUE violation
        fmt.Printf("UNIQUE constraint failed: %v\n", verr)
    }
}

// Extract all UNIQUE constraints from table
constraints := constraint.ExtractUniqueConstraints(table)

// Create backing indexes for UNIQUE constraints
err := constraint.EnsureUniqueIndexes(table, schema, btree)
```

**SQL Examples:**
```sql
-- Column-level UNIQUE
CREATE TABLE users (
    email TEXT UNIQUE,
    username TEXT UNIQUE
);

-- Composite UNIQUE
CREATE TABLE addresses (
    street TEXT,
    city TEXT,
    UNIQUE (street, city)
);

-- Named UNIQUE
CREATE TABLE records (
    code TEXT,
    CONSTRAINT uk_code UNIQUE (code)
);
```

**NULL Handling:**
Per SQL standard, NULL values are distinct from each other:
```sql
INSERT INTO users VALUES (1, NULL);  -- OK
INSERT INTO users VALUES (2, NULL);  -- OK (multiple NULLs allowed)
INSERT INTO users VALUES (3, 'a@b.c');  -- OK
INSERT INTO users VALUES (4, 'a@b.c');  -- ERROR: UNIQUE violation
```

**Backing Indexes:**
Each UNIQUE constraint automatically creates an index:
- Named constraints: `sqlite_autoindex_{table}_{constraint_name}`
- Unnamed constraints: `sqlite_autoindex_{table}_{column_names}`

### Collation Sequences

Collation sequences determine how strings are compared and sorted.

**Built-in Collations:**

1. **BINARY** (default)
   - Byte-by-byte comparison
   - Case-sensitive
   - Fastest performance
   - Example: `"Hello" != "hello"`

2. **NOCASE**
   - Case-insensitive for ASCII characters
   - Non-ASCII characters compared byte-by-byte
   - Example: `"Hello" == "hello"`

3. **RTRIM**
   - Ignores trailing spaces
   - Example: `"hello  " == "hello"`

**Usage:**
```go
// Register custom collation
lengthCollation := func(a, b string) int {
    if len(a) < len(b) {
        return -1
    } else if len(a) > len(b) {
        return 1
    }
    return 0
}
err := constraint.RegisterCollation("LENGTH", lengthCollation)

// Use in comparisons
result := constraint.Compare("hello", "HELLO", "NOCASE")  // returns 0

// VDBE integration
mem1 := vdbe.NewMemStr("Hello")
mem2 := vdbe.NewMemStr("hello")
result := mem1.CompareWithCollation(mem2, "NOCASE")
```

**SQL Examples:**
```sql
CREATE TABLE users (
    name TEXT COLLATE NOCASE,
    email TEXT COLLATE BINARY,
    notes TEXT COLLATE RTRIM
);

SELECT * FROM users ORDER BY name COLLATE NOCASE;
```

**Schema Integration:**
```go
// Get column's collation
collation := column.GetEffectiveCollation()  // Returns "BINARY" if not specified

// Get collation by column index
collation := table.GetColumnCollation(0)

// Get collation by column name
collation := table.GetColumnCollationByName("email")
```

## API Reference

### CHECK Constraints

**Types:**
- `CheckConstraint` - Represents a single CHECK constraint
- `CheckValidator` - Validates all CHECK constraints for a table

**Functions:**
- `NewCheckValidator(table *schema.Table) *CheckValidator`
- `(*CheckValidator).HasCheckConstraints() bool`
- `(*CheckValidator).ValidateInsert(vm, tableName, cursorNum, recordStartReg, numRecordCols) error`
- `(*CheckValidator).ValidateUpdate(vm, tableName, cursorNum, recordStartReg, numRecordCols) error`

### UNIQUE Constraints

**Types:**
- `UniqueConstraint` - Represents a single UNIQUE constraint
- `UniqueViolationError` - Error type for constraint violations

**Functions:**
- `NewUniqueConstraint(name, tableName string, columns []string) *UniqueConstraint`
- `ExtractUniqueConstraints(table *schema.Table) []*UniqueConstraint`
- `ValidateTableRow(table *schema.Table, bt *btree.Btree, values map[string]interface{}, rowid int64) error`
- `EnsureUniqueIndexes(table *schema.Table, schema *schema.Schema, bt *btree.Btree) error`

### Collations

**Types:**
- `CollationFunc` - Function signature: `func(a, b string) int`
- `CollationRegistry` - Thread-safe registry for collations

**Functions:**
- `RegisterCollation(name string, fn CollationFunc) error`
- `GetCollation(name string) (CollationFunc, error)`
- `UnregisterCollation(name string) error`
- `Compare(a, b string, collation string) int`
- `CompareBytes(a, b []byte, collation string) int`
- `NewCollationRegistry() *CollationRegistry`

## Integration with SQL Engine

### INSERT Statement

```go
func compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt) error {
    // 1. Load values into registers
    // ...

    // 2. Validate CHECK constraints
    checkValidator := constraint.NewCheckValidator(table)
    if checkValidator.HasCheckConstraints() {
        err := checkValidator.ValidateInsert(vm, table.Name, cursorNum, recordStartReg, numCols)
        if err != nil {
            return err
        }
    }

    // 3. Validate UNIQUE constraints
    err := constraint.ValidateTableRow(table, btree, values, rowid)
    if err != nil {
        return err
    }

    // 4. Perform INSERT
    // ...
}
```

### UPDATE Statement

```go
func compileUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt) error {
    // 1. Load new values into registers
    // ...

    // 2. Validate CHECK constraints
    checkValidator := constraint.NewCheckValidator(table)
    if checkValidator.HasCheckConstraints() {
        err := checkValidator.ValidateUpdate(vm, table.Name, cursorNum, recordStartReg, numCols)
        if err != nil {
            return err
        }
    }

    // 3. Validate UNIQUE constraints
    err := constraint.ValidateTableRow(table, btree, newValues, rowid)
    if err != nil {
        return err
    }

    // 4. Perform UPDATE
    // ...
}
```

### CREATE TABLE Statement

```go
func compileCreateTable(stmt *parser.CreateTableStmt) error {
    // 1. Create table schema
    table, err := schema.CreateTable(stmt)
    if err != nil {
        return err
    }

    // 2. Create backing indexes for UNIQUE constraints
    err = constraint.EnsureUniqueIndexes(table, schemaObj, btree)
    if err != nil {
        return err
    }

    // 3. Register table in schema
    // ...
}
```

## Error Messages

### CHECK Constraint Violations

```
CHECK constraint failed: constraint_name (expression)
CHECK constraint failed: expression
CHECK constraint failed for column column_name: expression
```

### UNIQUE Constraint Violations

```
UNIQUE constraint failed: users.email
UNIQUE constraint failed: users.uk_email
UNIQUE constraint failed: users.first_name,last_name
```

## Performance Characteristics

### CHECK Constraints
- **Compilation**: O(1) per constraint - minimal overhead
- **Execution**: O(1) expression evaluation per INSERT/UPDATE
- **Memory**: 4-6 VDBE opcodes per constraint

### UNIQUE Constraints
- **Validation**: O(log n) using B-tree index lookup
- **Index Creation**: O(n log n) where n = number of rows
- **Memory**: One index B-tree per UNIQUE constraint

### Collations
- **BINARY**: O(n) byte comparison (fastest)
- **NOCASE**: O(n) with case folding
- **RTRIM**: O(n) trim + O(n) comparison
- **Custom**: Depends on implementation

## Testing

Run all constraint tests:
```bash
go test -v ./internal/constraint/...
```

Run with coverage:
```bash
go test -cover ./internal/constraint/...
```

Run specific tests:
```bash
go test -v ./internal/constraint -run Check
go test -v ./internal/constraint -run Unique
go test -v ./internal/constraint -run Collation
```

## Implementation Status

### Completed
- CHECK constraints (column-level and table-level)
- UNIQUE constraints (single and composite)
- Collation sequences (BINARY, NOCASE, RTRIM)
- Custom collation registration
- Automatic backing index creation
- NULL handling per SQL standard
- Error message formatting
- Comprehensive test coverage

### Limitations
- FOREIGN KEY constraints not yet implemented
- No partial UNIQUE constraints (WHERE clause)
- No deferred constraint checking
- No locale-aware collations (requires ICU)

## References

- [SQLite CHECK Constraints](https://www.sqlite.org/lang_createtable.html#check_constraints)
- [SQLite UNIQUE Constraints](https://www.sqlite.org/lang_createtable.html#uniqueconst)
- [SQLite Collation Sequences](https://www.sqlite.org/datatype3.html#collation)
- [SQL Standard NULL Handling](https://modern-sql.com/concept/three-valued-logic)

## License

This implementation is part of the Anthony SQLite project and follows the project's public domain dedication.
