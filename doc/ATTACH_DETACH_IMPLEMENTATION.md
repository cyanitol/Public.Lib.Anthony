# ATTACH/DETACH DATABASE Implementation Summary

## Overview
Phase 2 implementation of ATTACH DATABASE and DETACH DATABASE parsing for the Anthony SQLite clone project.

## Implementation Details

### 1. AST Nodes (ast.go)
Added two new statement types to represent ATTACH and DETACH operations:

#### AttachStmt
```go
type AttachStmt struct {
    Filename   Expression // String literal or expression for the database file path
    SchemaName string     // The schema name to attach as
}
```

#### DetachStmt
```go
type DetachStmt struct {
    SchemaName string // The schema name to detach
}
```

Both implement the `Statement` interface with `node()`, `statement()`, and `String()` methods.

### 2. Parser Functions (parser.go)

#### parseAttach()
Parses ATTACH DATABASE statements with the following syntax:
```sql
ATTACH [DATABASE] filename AS schema_name
```

Features:
- DATABASE keyword is optional
- Filename can be any expression (typically a string literal, but supports concatenation)
- AS keyword is required
- Schema name must be an identifier (can be quoted)

#### parseDetach()
Parses DETACH DATABASE statements with the following syntax:
```sql
DETACH [DATABASE] schema_name
```

Features:
- DATABASE keyword is optional
- Schema name must be an identifier (can be quoted)

### 3. Statement Parser Registration
Added both statements to the parser's statement dispatch system:

```go
var statementParsers = map[TokenType]statementParser{
    // ... existing parsers
    TK_ATTACH: func(p *Parser) (Statement, error) { return p.parseAttach() },
    TK_DETACH: func(p *Parser) (Statement, error) { return p.parseDetach() },
}

var statementParserOrder = []TokenType{
    // ... existing tokens
    TK_ATTACH, TK_DETACH,
}
```

### 4. Tokens
Verified that the required tokens are already defined in token.go:
- `TK_ATTACH` - ATTACH keyword
- `TK_DETACH` - DETACH keyword
- `TK_DATABASE` - DATABASE keyword

These tokens are already mapped in the lexer's keyword map.

### 5. Tests (parser_attach_test.go)
Created comprehensive test suite with 26+ test cases:

#### TestParseAttach
Tests ATTACH statement parsing with:
- DATABASE keyword present and absent
- Various quote types (single, double)
- Quoted schema names
- Expression-based filenames (concatenation)
- Error cases (missing AS, missing schema name, missing filename)

#### TestParseDetach
Tests DETACH statement parsing with:
- DATABASE keyword present and absent
- Various quoted identifier formats
- Error cases (missing schema name)

#### TestParseAttachDetachCombined
Tests multiple statements in sequence:
- Multiple ATTACH statements
- ATTACH followed by DETACH
- DETACH followed by ATTACH

## SQL Syntax Examples

### Valid ATTACH Statements
```sql
-- With DATABASE keyword
ATTACH DATABASE 'file.db' AS mydb

-- Without DATABASE keyword
ATTACH 'file.db' AS mydb

-- With double-quoted filename
ATTACH DATABASE "file.db" AS mydb

-- With quoted schema name
ATTACH 'file.db' AS "my-schema"

-- With expression for filename
ATTACH DATABASE 'dir/' || 'file.db' AS mydb
```

### Valid DETACH Statements
```sql
-- With DATABASE keyword
DETACH DATABASE mydb

-- Without DATABASE keyword
DETACH mydb

-- With quoted schema name
DETACH DATABASE "my-schema"

-- With backtick quoted name
DETACH `my-schema`
```

## File Changes

### New Files
1. `/internal/parser/parser_attach_test.go` - Comprehensive test suite
2. `/internal/parser/parser_attach_example.go` - Documentation and examples

### Modified Files
1. `/internal/parser/ast.go` - Added AttachStmt and DetachStmt types
2. `/internal/parser/parser.go` - Added parseAttach() and parseDetach() functions, registered statements

### Existing Files (No Changes Needed)
1. `/internal/parser/token.go` - TK_ATTACH, TK_DETACH, TK_DATABASE already defined
2. `/internal/parser/lexer.go` - Keywords already mapped

## Testing
To run the tests:
```bash
cd /home/justin/Programming/Workspace/cyanitol/Public.Lib.Anthony
go test -v ./internal/parser -run TestParseAttach
go test -v ./internal/parser -run TestParseDetach
go test -v ./internal/parser -run TestParseAttachDetachCombined
```

Or run all parser tests:
```bash
go test -v ./internal/parser
```

## Compliance with SQLite
The implementation follows SQLite's ATTACH and DETACH syntax:
- DATABASE keyword is optional in both statements
- ATTACH requires: expression AS identifier
- DETACH requires: identifier
- Schema names are unquoted after parsing (following SQLite behavior)
- Expressions are fully supported for filename (not just string literals)

## Future Considerations
- The implementation is ready for execution layer integration
- AttachStmt.Filename being an Expression allows for runtime evaluation
- Schema name conflicts should be checked at execution time, not parse time
- The AST can be extended with additional metadata if needed (e.g., KEY for encryption)
