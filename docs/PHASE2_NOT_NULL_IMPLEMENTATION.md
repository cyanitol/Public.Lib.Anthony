# Phase 2: NOT NULL Constraint Implementation

## Overview

This document describes the implementation of NOT NULL constraint enforcement for the Anthony SQLite clone project. The implementation follows SQLite's behavior for validating NOT NULL constraints during INSERT and UPDATE operations, with proper integration of DEFAULT values.

## Files Created

### 1. `/internal/constraint/not_null.go`

Core implementation of NOT NULL constraint validation.

**Key Components:**

- `NotNullConstraint` struct: Main validator that holds a reference to the table schema
- `NewNotNullConstraint()`: Constructor that creates a validator for a specific table
- `ValidateInsert()`: Validates all NOT NULL columns have values for INSERT operations
- `ValidateUpdate()`: Validates that UPDATE operations don't set NOT NULL columns to NULL
- `ApplyDefaults()`: Applies DEFAULT values before constraint validation
- `ValidateRow()`: Convenience method combining default application and validation
- `GetNotNullColumns()`: Returns list of NOT NULL column names
- `HasNotNullConstraint()`: Checks if a specific column has NOT NULL constraint

**Design Decisions:**

1. **Separation of Concerns**: Default application is separate from validation, allowing flexibility in the compilation process
2. **Clear Error Messages**: All errors include the column name (e.g., "NOT NULL constraint failed: column email")
3. **Map-Based Interface**: Uses `map[string]interface{}` for values, matching the driver's internal representation
4. **Default Integration**: `ApplyDefaults()` is called before validation, ensuring DEFAULT values satisfy NOT NULL constraints

### 2. `/internal/constraint/not_null_test.go`

Comprehensive test suite with 15+ test cases covering:

- Valid INSERT operations with all required fields
- Valid INSERT operations with optional fields
- Missing NOT NULL columns in INSERT
- NULL values in NOT NULL columns for INSERT
- Valid UPDATE operations
- NULL values in NOT NULL columns for UPDATE
- DEFAULT value application
- DEFAULT value override behavior
- Empty tables (no constraints)
- Tables where all columns are NOT NULL
- Helper method tests

**Test Coverage:**

- `TestNewNotNullConstraint`: Constructor validation
- `TestValidateInsert_ValidData`: Valid INSERT scenarios
- `TestValidateInsert_MissingNotNullColumn`: Missing required columns
- `TestValidateInsert_NullNotNullColumn`: NULL in NOT NULL columns
- `TestValidateUpdate_ValidData`: Valid UPDATE scenarios
- `TestValidateUpdate_NullNotNullColumn`: NULL in NOT NULL columns during UPDATE
- `TestApplyDefaults`: DEFAULT value application logic
- `TestValidateRow`: Combined default + validation
- `TestGetNotNullColumns`: Helper method tests
- `TestHasNotNullConstraint`: Column constraint checking
- `TestValidateInsert_EmptyTable`: Edge case with no constraints
- `TestValidateInsert_AllNotNull`: Edge case with all columns NOT NULL

### 3. `/internal/constraint/doc.go`

Package documentation with:
- Package overview
- Usage examples for INSERT and UPDATE
- Integration with DEFAULT values
- Error message format

## Integration Points

### Schema Integration

The implementation leverages existing schema structures:

```go
// From internal/schema/schema.go
type Column struct {
    Name     string
    Type     string
    NotNull  bool        // Used by constraint validator
    Default  interface{} // Used by ApplyDefaults
    // ... other fields
}
```

The `NotNull` flag is already set during table creation via `schema.CreateTable()`, which processes the parsed CREATE TABLE statement.

### Driver Integration

To integrate with the driver's INSERT/UPDATE compilation:

**For INSERT operations** (in `internal/driver/stmt.go`):

```go
func (s *Stmt) compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
    // ... existing code to get table ...

    // Create constraint validator
    nnc := constraint.NewNotNullConstraint(table)

    // Build values map from INSERT statement
    values := buildValuesMap(stmt, table, args)

    // Apply defaults before validation
    if err := nnc.ApplyDefaults(values, true); err != nil {
        return nil, err
    }

    // Validate NOT NULL constraints
    if err := nnc.ValidateInsert(values); err != nil {
        return nil, err
    }

    // ... continue with existing VDBE compilation ...
}
```

**For UPDATE operations**:

```go
func (s *Stmt) compileUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
    // ... existing code to get table ...

    // Create constraint validator
    nnc := constraint.NewNotNullConstraint(table)

    // Build updates map from UPDATE statement
    updates := buildUpdatesMap(stmt, args)

    // Apply defaults for NULL values (optional, depends on desired behavior)
    if err := nnc.ApplyDefaults(updates, false); err != nil {
        return nil, err
    }

    // Validate NOT NULL constraints
    if err := nnc.ValidateUpdate(updates); err != nil {
        return nil, err
    }

    // ... continue with existing VDBE compilation ...
}
```

### Helper Functions Needed

Two helper functions would need to be added to `internal/driver/stmt.go`:

```go
// buildValuesMap creates a map of column names to values from INSERT statement
func buildValuesMap(stmt *parser.InsertStmt, table *schema.Table, args []driver.NamedValue) map[string]interface{} {
    values := make(map[string]interface{})
    colNames := resolveInsertColumns(stmt, table)

    if len(stmt.Values) > 0 {
        row := stmt.Values[0]
        for i, colName := range colNames {
            if i < len(row) {
                values[colName] = evaluateExpr(row[i], args)
            }
        }
    }

    return values
}

// buildUpdatesMap creates a map of column names to values from UPDATE statement
func buildUpdatesMap(stmt *parser.UpdateStmt, args []driver.NamedValue) map[string]interface{} {
    updates := make(map[string]interface{})

    for _, assign := range stmt.Sets {
        updates[assign.Column] = evaluateExpr(assign.Value, args)
    }

    return updates
}

// evaluateExpr evaluates an expression to a concrete value
func evaluateExpr(expr parser.Expression, args []driver.NamedValue) interface{} {
    switch e := expr.(type) {
    case *parser.LiteralExpr:
        return parseLiteral(e)
    case *parser.VariableExpr:
        // Get value from args
        return getArgValue(e, args)
    default:
        return nil
    }
}
```

## Constraint Validation Flow

### INSERT Flow

```
1. Parse INSERT statement
2. Extract column names and values
3. Build values map
4. Create NotNullConstraint validator
5. Apply DEFAULT values (fills in missing/NULL columns)
6. Validate NOT NULL constraints
7. If validation passes, compile VDBE bytecode
8. Execute INSERT
```

### UPDATE Flow

```
1. Parse UPDATE statement
2. Extract column assignments
3. Build updates map
4. Create NotNullConstraint validator
5. (Optional) Apply DEFAULT values for NULL updates
6. Validate NOT NULL constraints
7. If validation passes, compile VDBE bytecode
8. Execute UPDATE
```

## SQLite Compatibility

The implementation matches SQLite's behavior:

1. **Error Messages**: Format matches SQLite: `NOT NULL constraint failed: column_name`
2. **DEFAULT Integration**: DEFAULT values are applied before NOT NULL validation
3. **INSERT Validation**: All NOT NULL columns must have non-NULL values
4. **UPDATE Validation**: Cannot set NOT NULL columns to NULL
5. **Optional Columns**: Columns without NOT NULL can be NULL or omitted

## Testing

### Test Table Schema

```sql
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL DEFAULT 'unknown@example.com',
    age INTEGER,
    status TEXT NOT NULL DEFAULT 'active',
    description TEXT DEFAULT 'No description'
);
```

### Test Scenarios

**INSERT Tests:**
- ✅ All required columns provided
- ✅ Optional columns omitted
- ✅ Optional columns set to NULL
- ❌ Missing NOT NULL column without DEFAULT
- ❌ NULL in NOT NULL column
- ✅ DEFAULT fills in missing NOT NULL column

**UPDATE Tests:**
- ✅ Update non-NULL values
- ✅ Update nullable columns to NULL
- ❌ Update NOT NULL column to NULL
- ✅ Update multiple columns

**DEFAULT Tests:**
- ✅ Apply default for missing column
- ✅ Apply default for NULL value
- ✅ Don't override explicit values
- ✅ Apply multiple defaults

**Edge Cases:**
- ✅ Table with no NOT NULL constraints
- ✅ Table where all columns are NOT NULL
- ✅ Unknown columns (ignored)

## Future Enhancements

### Phase 3+ Enhancements:

1. **Expression Evaluation**: Support DEFAULT expressions (not just literal values)
2. **UNIQUE Constraint**: Similar validator for UNIQUE constraints
3. **CHECK Constraint**: Validate CHECK expressions
4. **Foreign Key Constraints**: Reference integrity validation
5. **Performance Optimization**: Cache constraint validators per table
6. **Batch Validation**: Validate multiple rows efficiently

### Integration with VDBE:

Currently validation happens during compilation. Future enhancement could:
- Add VDBE opcodes for constraint checking
- Runtime constraint validation
- Better error reporting with row numbers

## Error Handling

All validation errors return Go errors with descriptive messages:

```go
fmt.Errorf("NOT NULL constraint failed: column %s", col.Name)
```

The error format matches SQLite's output, making it familiar to users and easier to debug.

## Performance Considerations

1. **Validation Cost**: O(n) where n = number of columns in the table
2. **Default Application**: O(m) where m = number of columns with defaults
3. **Memory**: Single map allocation per INSERT/UPDATE operation
4. **Caching**: Constraint validators could be cached per table for repeated operations

## Conclusion

The NOT NULL constraint implementation provides:
- ✅ Complete validation for INSERT and UPDATE operations
- ✅ Proper DEFAULT value integration
- ✅ Clear, SQLite-compatible error messages
- ✅ Comprehensive test coverage
- ✅ Clean integration points with existing driver code
- ✅ Foundation for future constraint types (UNIQUE, CHECK, FK)

The implementation is ready for integration with the driver's INSERT/UPDATE compilation process.
