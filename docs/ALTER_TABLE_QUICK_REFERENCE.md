# ALTER TABLE - Quick Reference

## Supported SQL Statements

### 1. RENAME TABLE
```sql
ALTER TABLE old_name RENAME TO new_name;
```

### 2. RENAME COLUMN
```sql
ALTER TABLE table_name RENAME COLUMN old_column TO new_column;
```

### 3. ADD COLUMN
```sql
-- Basic
ALTER TABLE table_name ADD COLUMN column_name type;

-- COLUMN keyword optional
ALTER TABLE table_name ADD column_name type;

-- With constraints
ALTER TABLE users ADD COLUMN email TEXT NOT NULL;
ALTER TABLE users ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT;
ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 18 CHECK(age >= 0);
ALTER TABLE users ADD COLUMN name TEXT COLLATE NOCASE;
```

### 4. DROP COLUMN
```sql
ALTER TABLE table_name DROP COLUMN column_name;
```

## Files Modified/Created

### New Files
- `internal/parser/parser_alter.go` - ALTER TABLE parser implementation
- `internal/parser/parser_alter_test.go` - Comprehensive test suite

### Modified Files
- `internal/parser/ast.go` - Added AST nodes
- `internal/parser/token.go` - Added TK_TO token
- `internal/parser/lexer.go` - Added TO keyword
- `internal/parser/parser.go` - Registered ALTER parser

## AST Node Structure

```go
// Main statement
type AlterTableStmt struct {
    Table  string
    Action AlterTableAction  // Interface for specific action
}

// Action interface
type AlterTableAction interface {
    Node
    alterTableAction()
}

// Concrete actions
type RenameTableAction struct {
    NewName string
}

type RenameColumnAction struct {
    OldName string
    NewName string
}

type AddColumnAction struct {
    Column ColumnDef
}

type DropColumnAction struct {
    ColumnName string
}
```

## Testing

Run all ALTER TABLE tests:
```bash
go test -v ./internal/parser -run TestAlterTable
```

Run specific test:
```bash
go test -v ./internal/parser -run TestAlterTableRenameTable
go test -v ./internal/parser -run TestAlterTableRenameColumn
go test -v ./internal/parser -run TestAlterTableAddColumn
go test -v ./internal/parser -run TestAlterTableDropColumn
```

## Test Coverage
- 7 test functions
- 25+ individual test cases
- Covers all variants and error conditions

## Implementation Status
✅ RENAME TO (rename table)
✅ RENAME COLUMN (rename column)
✅ ADD COLUMN (add column with full constraint support)
✅ DROP COLUMN (drop column)
✅ Token support (TK_TO)
✅ Parser integration
✅ Comprehensive tests
✅ Error handling
