# CHECK Constraint Implementation

This package implements CHECK constraint enforcement for the Anthony SQLite clone project.

## Overview

CHECK constraints allow you to specify that values in a column must satisfy a Boolean expression. This implementation validates CHECK constraints during INSERT and UPDATE operations by generating VDBE bytecode to evaluate the constraint expressions.

## Features

- **Column-level CHECK constraints**: Constraints defined on individual columns
- **Table-level CHECK constraints**: Constraints that can reference multiple columns
- **NULL handling**: NULL values pass CHECK constraints per SQLite specification
- **Clear error messages**: Error messages include constraint name and expression
- **Expression evaluation**: Uses the VDBE expression code generator for consistent behavior
- **Integration with INSERT/UPDATE**: Can be integrated into existing INSERT and UPDATE compilation

## Architecture

### Components

1. **CheckConstraint**: Represents a single CHECK constraint with its expression and metadata
2. **CheckValidator**: Validates all CHECK constraints for a table
3. **Expression Evaluation**: Uses `internal/expr` code generator to compile constraint expressions to VDBE bytecode

### Workflow

```
Table Schema
    ↓
Extract CHECK Constraints (from Column.Check and TableConstraint)
    ↓
Parse Expressions (using parser.ParseExpression)
    ↓
Create CheckValidator
    ↓
During INSERT/UPDATE:
    - Generate VDBE code to evaluate expression
    - Check if result is TRUE (non-zero) or NULL (passes)
    - If FALSE (zero), execute OpHalt with error message
```

## Usage

### Creating a Validator

```go
import (
    "github.com/JuniperBible/Public.Lib.Anthony/internal/constraint"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
)

// Get table from schema
table, _ := schema.GetTable("users")

// Create validator
validator := constraint.NewCheckValidator(table)

// Check if table has constraints
if validator.HasCheckConstraints() {
    // Integrate validation into INSERT/UPDATE
}
```

### Integration with INSERT

```go
func compileInsert(vm *vdbe.VDBE, stmt *parser.InsertStmt) error {
    // ... load values into registers ...

    // Create validator
    validator := constraint.NewCheckValidator(table)

    // Validate CHECK constraints
    err := validator.ValidateInsert(
        vm,                  // VDBE instance
        stmt.Table,          // table name
        0,                   // cursor number
        recordStartReg,      // first register with row data
        numRecordCols,       // number of columns in record
    )
    if err != nil {
        return err
    }

    // ... continue with INSERT operation ...
}
```

### Integration with UPDATE

```go
func compileUpdate(vm *vdbe.VDBE, stmt *parser.UpdateStmt) error {
    // ... compute new values into registers ...

    // Create validator
    validator := constraint.NewCheckValidator(table)

    // Validate CHECK constraints with new values
    err := validator.ValidateUpdate(
        vm,                  // VDBE instance
        stmt.Table,          // table name
        0,                   // cursor number
        recordStartReg,      // first register with new row data
        numRecordCols,       // number of columns in record
    )
    if err != nil {
        return err
    }

    // ... continue with UPDATE operation ...
}
```

## Implementation Details

### CHECK Constraint Semantics

Per SQLite specification:

1. **TRUE (non-zero)**: Constraint passes
2. **FALSE (zero)**: Constraint fails, operation aborted
3. **NULL**: Constraint passes (NULL is treated as TRUE)

### VDBE Code Generation

For each CHECK constraint, the validator generates:

```
1. Generate code to evaluate expression → resultReg
2. OpIsNull resultReg, skipError     // NULL passes
3. OpIfNot resultReg, errorAddr      // FALSE (0) jumps to error
4. OpGoto continueAddr               // TRUE continues
5. errorAddr:
6.   OpHalt 1, 0, 0, "error message" // Abort with error
7. continueAddr:
   ... next constraint or operation ...
```

### Error Messages

Error messages follow this format:

- **Named table-level**: `CHECK constraint failed: constraint_name (expression)`
- **Unnamed table-level**: `CHECK constraint failed: expression`
- **Column-level**: `CHECK constraint failed for column column_name: expression`

### Expression Parsing

CHECK expressions are stored as strings in the schema and parsed on-demand:

```go
// Column-level from schema.Column.Check
p := parser.New(col.Check)
expr, err := p.ParseExpression()

// Table-level from schema.TableConstraint.Expression
p := parser.New(tc.Expression)
expr, err := p.ParseExpression()
```

## Testing

The implementation includes comprehensive tests:

- `TestNewCheckValidator`: Creating validators from schema
- `TestExtractCheckConstraints`: Extracting constraints from various table definitions
- `TestValidateInsert`: INSERT validation code generation
- `TestValidateUpdate`: UPDATE validation code generation
- `TestFormatErrorMessage`: Error message formatting
- `TestComplexCheckExpressions`: Complex expressions (AND, OR, multi-column)
- `TestNullInCheckConstraints`: NULL handling

Run tests:
```bash
go test ./internal/constraint/... -v
```

## Examples

### Column-level CHECK

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    price REAL CHECK(price > 0),
    stock INTEGER CHECK(stock >= 0)
);
```

### Table-level CHECK

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    start_time INTEGER,
    end_time INTEGER,
    CONSTRAINT valid_time_range CHECK(start_time < end_time)
);
```

### Complex CHECK with AND/OR

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    age INTEGER CHECK(age >= 0 AND age <= 120),
    status TEXT CHECK(status = 'active' OR status = 'inactive')
);
```

## Limitations

Current implementation:

1. **No subqueries**: CHECK expressions cannot contain subqueries
2. **No aggregate functions**: CHECK expressions cannot use SUM, COUNT, etc.
3. **Expression parsing**: Relies on parser.ParseExpression() which must support all expression types used in CHECK constraints

These are standard SQLite limitations for CHECK constraints.

## Future Enhancements

Potential improvements:

1. **Expression caching**: Cache parsed expressions for performance
2. **Constraint metadata**: Store more metadata (e.g., columns referenced)
3. **Optimization**: Detect constant expressions that always pass/fail
4. **Better error context**: Include row data in error messages for debugging

## References

- [SQLite CHECK Constraints](https://www.sqlite.org/lang_createtable.html#check_constraints)
- [SQLite Expression Evaluation](https://www.sqlite.org/lang_expr.html)
- Anthony VDBE Architecture: `internal/vdbe/`
- Anthony Expression Generator: `internal/expr/`
