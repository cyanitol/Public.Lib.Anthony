# EXPLAIN and EXPLAIN QUERY PLAN Implementation

## Overview

This document describes the implementation of EXPLAIN and EXPLAIN QUERY PLAN statement parsing for the Anthony SQLite clone project (Phase 2).

## Implementation Summary

### Files Modified

1. **internal/parser/ast.go**
   - Added `ExplainStmt` struct to represent EXPLAIN statements
   - Includes `QueryPlan` bool field to distinguish between EXPLAIN and EXPLAIN QUERY PLAN
   - Includes `Statement` field to hold the statement being explained
   - Implements required AST node interface methods

2. **internal/parser/parser.go**
   - Modified `parseStatement()` to check for TK_EXPLAIN token
   - Added `parseExplain()` function to handle EXPLAIN parsing
   - Removed obsolete `consumeExplainPrefix()` function

3. **internal/parser/token.go** (no changes needed)
   - TK_EXPLAIN, TK_QUERY, and TK_PLAN tokens already existed

### Files Created

1. **internal/parser/parser_explain_test.go**
   - Comprehensive test suite for EXPLAIN parsing
   - Tests for both EXPLAIN and EXPLAIN QUERY PLAN variants
   - Tests for all major statement types (SELECT, INSERT, UPDATE, DELETE, etc.)
   - Tests for nested EXPLAIN statements
   - Tests for error cases

## AST Node Structure

```go
// ExplainStmt represents an EXPLAIN or EXPLAIN QUERY PLAN statement.
type ExplainStmt struct {
    QueryPlan bool      // true for EXPLAIN QUERY PLAN, false for EXPLAIN
    Statement Statement // the statement being explained
}
```

### Methods

- `node()`: Implements the Node interface
- `statement()`: Implements the Statement interface
- `String()`: Returns "EXPLAIN" or "EXPLAIN QUERY PLAN" based on the QueryPlan flag

## Parser Function

### parseExplain()

```go
func (p *Parser) parseExplain() (*ExplainStmt, error)
```

**Syntax Supported:**
- `EXPLAIN statement`
- `EXPLAIN QUERY PLAN statement`

**Algorithm:**
1. Check if the next token is TK_QUERY
2. If yes, expect TK_PLAN and set `QueryPlan` to true
3. Recursively call `parseStatement()` to parse the inner statement
4. Return the constructed ExplainStmt

**Features:**
- Supports all statement types (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
- Supports nested EXPLAIN statements (e.g., "EXPLAIN EXPLAIN SELECT ...")
- Proper error handling for malformed EXPLAIN syntax

## Usage Examples

### Basic EXPLAIN

```sql
EXPLAIN SELECT * FROM users;
```

Result AST:
```
ExplainStmt {
    QueryPlan: false,
    Statement: SelectStmt { ... }
}
```

### EXPLAIN QUERY PLAN

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 18;
```

Result AST:
```
ExplainStmt {
    QueryPlan: true,
    Statement: SelectStmt { ... }
}
```

### Nested EXPLAIN

```sql
EXPLAIN EXPLAIN QUERY PLAN SELECT * FROM users;
```

Result AST:
```
ExplainStmt {
    QueryPlan: false,
    Statement: ExplainStmt {
        QueryPlan: true,
        Statement: SelectStmt { ... }
    }
}
```

## Test Coverage

The test suite (`parser_explain_test.go`) includes:

### TestParseExplain
Tests for:
- EXPLAIN SELECT
- EXPLAIN QUERY PLAN SELECT
- EXPLAIN INSERT
- EXPLAIN QUERY PLAN INSERT
- EXPLAIN UPDATE
- EXPLAIN QUERY PLAN UPDATE
- EXPLAIN DELETE
- EXPLAIN QUERY PLAN DELETE
- EXPLAIN CREATE TABLE
- EXPLAIN QUERY PLAN CREATE INDEX
- EXPLAIN SELECT with JOINs
- EXPLAIN QUERY PLAN with complex queries
- EXPLAIN DROP TABLE
- EXPLAIN BEGIN TRANSACTION
- Error case: EXPLAIN QUERY without PLAN

### TestParseExplainNested
Tests for:
- Nested EXPLAIN statements
- Nested EXPLAIN QUERY PLAN statements

### TestExplainStmtString
Tests for:
- String() method returning correct representation

## SQLite Compatibility

This implementation follows SQLite's EXPLAIN syntax:

**From SQLite Documentation:**
```
EXPLAIN [QUERY PLAN] statement
```

Our implementation correctly handles:
1. The optional QUERY PLAN modifier
2. Any valid SQL statement as the explained statement
3. Proper tokenization of EXPLAIN, QUERY, and PLAN keywords

## Integration Points

The EXPLAIN statement parsing integrates with the existing parser through:

1. **Token recognition**: Uses existing TK_EXPLAIN, TK_QUERY, and TK_PLAN tokens
2. **Statement parsing**: Integrates with the main `parseStatement()` function
3. **AST structure**: Follows the existing pattern for statement nodes
4. **Error handling**: Uses the existing parser error mechanism

## Future Enhancements

Potential future improvements:
1. Add execution layer support for EXPLAIN statements
2. Implement EXPLAIN output formatting
3. Add EXPLAIN support for more statement types as they are implemented
4. Add EXPLAIN QUERY PLAN optimization hints

## Implementation Notes

### Design Decisions

1. **Recursive parsing**: The `parseExplain()` function calls `parseStatement()` recursively, which allows for nested EXPLAIN statements and keeps the code simple.

2. **QueryPlan flag**: Using a boolean flag instead of separate statement types (ExplainStmt vs ExplainQueryPlanStmt) reduces code duplication and simplifies type checking.

3. **String() method**: Returns a descriptive string based on the QueryPlan flag, making debugging and testing easier.

### Edge Cases Handled

1. **EXPLAIN QUERY without PLAN**: Returns an error
2. **Nested EXPLAIN**: Supported through recursive parsing
3. **EXPLAIN on all statement types**: Works with any statement that implements the Statement interface

## Testing

To run the tests:

```bash
cd /path/to/Public.Lib.Anthony
go test ./internal/parser -run TestParseExplain -v
```

To run all parser tests:

```bash
go test ./internal/parser -v
```

## Verification

The implementation can be verified by:

1. Running the test suite
2. Checking AST structure for various EXPLAIN statements
3. Verifying String() method output
4. Testing with nested EXPLAIN statements
5. Testing error cases

## Summary

The EXPLAIN and EXPLAIN QUERY PLAN parsing implementation:
- ✅ Added ExplainStmt AST node
- ✅ Implemented parseExplain() function
- ✅ Uses existing TK_EXPLAIN, TK_QUERY, TK_PLAN tokens
- ✅ Comprehensive test suite (254 lines)
- ✅ Supports all statement types
- ✅ Handles nested EXPLAIN statements
- ✅ Proper error handling
- ✅ SQLite-compatible syntax

Total lines of code:
- ast.go: 15 lines (ExplainStmt definition)
- parser.go: 28 lines (parseExplain function)
- parser_explain_test.go: 254 lines (comprehensive tests)
- Total: ~297 lines of production and test code
