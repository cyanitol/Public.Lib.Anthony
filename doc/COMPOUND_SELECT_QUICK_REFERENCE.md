# Compound SELECT Quick Reference

## Overview
Quick reference for UNION, INTERSECT, and EXCEPT operations in the SQLite driver.

## SQL Syntax

```sql
-- UNION: Combine all rows (deduplicated)
SELECT a, b FROM table1
UNION
SELECT a, b FROM table2;

-- INTERSECT: Only rows in both
SELECT a, b FROM table1
INTERSECT
SELECT a, b FROM table2;

-- EXCEPT: Rows in first but not second
SELECT a, b FROM table1
EXCEPT
SELECT a, b FROM table2;
```

## Implementation Files

| File | Lines | Description |
|------|-------|-------------|
| `select.go` | 344-358 | `SRT_Union` and `SRT_Except` handling in `disposeResult()` |
| `select.go` | 468-483 | Compound SELECT dispatcher in `compileCompoundSelect()` |
| `select.go` | 486-524 | UNION implementation in `compileUnion()` |
| `select.go` | 526-641 | INTERSECT implementation in `compileIntersect()` |
| `select.go` | 643-711 | EXCEPT implementation in `compileExcept()` |

## Key Data Structures

### SelectDest Types
```go
SRT_Union  // Insert into ephemeral table (add to set)
SRT_Except // Delete from ephemeral table (remove from set)
```

### Select Token Types
```go
TK_UNION      // UNION operation
TK_UNION_ALL  // UNION ALL operation
TK_INTERSECT  // INTERSECT operation
TK_EXCEPT     // EXCEPT operation
```

## VDBE Opcodes Used

### Table Operations
```go
OP_OpenEphemeral // Create temporary table
OP_Close         // Close cursor
OP_Rewind        // Reset to first row
OP_Next          // Advance to next row
```

### Data Operations
```go
OP_Column       // Extract column value
OP_MakeRecord   // Pack columns into record
OP_ResultRow    // Output result row
```

### Index Operations
```go
OP_IdxInsert    // Insert key (deduplicates automatically)
OP_IdxDelete    // Delete key
OP_Found        // Check if key exists (jump if found)
OP_NotFound     // Check if key missing (jump if not found)
```

## Implementation Algorithms

### UNION
```
1. Create unionTab
2. Compile left -> SRT_Union -> unionTab
3. Compile right -> SRT_Union -> unionTab
4. Output from unionTab
```

### INTERSECT
```
1. Create leftTab, rightTab, resultTab
2. Compile left -> SRT_Union -> leftTab
3. Compile right -> SRT_Union -> rightTab
4. For each row in rightTab:
   - If exists in leftTab -> insert to resultTab
5. Output from resultTab
```

### EXCEPT
```
1. Create exceptTab
2. Compile left -> SRT_Union -> exceptTab
3. Compile right -> SRT_Except -> exceptTab
   (deletes matching rows)
4. Output from exceptTab
```

## Code Examples

### Using in Compiler
```go
// Parse creates Select with compound operation
sel := &Select{
    Op:    TK_INTERSECT,  // or TK_EXCEPT
    Prior: leftSelect,     // Left side
    EList: rightEList,     // Right side columns
    // ... other fields
}

// Compile
compiler := NewSelectCompiler(parse)
dest := &SelectDest{Dest: SRT_Output}
err := compiler.CompileSelect(sel, dest)
```

### Destination Handling
```go
// For left side of INTERSECT/EXCEPT
leftDest := &SelectDest{
    Dest:   SRT_Union,
    SDParm: tableId,  // Ephemeral table cursor
}

// For right side of EXCEPT
rightDest := &SelectDest{
    Dest:   SRT_Except,
    SDParm: tableId,  // Same ephemeral table
}
```

## Memory Usage

| Operation | Tables | Peak Memory |
|-----------|--------|-------------|
| UNION | 1 | O(unique rows) |
| INTERSECT | 3 | O(n + m + k) |
| EXCEPT | 2 | O(n) after deletion |

Where:
- n = left result size
- m = right result size
- k = intersection size

## Performance

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| UNION | O(n + m) | O(unique) |
| INTERSECT | O(n + m log n) | O(n + m + k) |
| EXCEPT | O(n + m log n) | O(n) |

## Common Patterns

### Multiple Operations
```sql
-- Chaining (requires parentheses)
(SELECT a FROM t1 INTERSECT SELECT a FROM t2)
EXCEPT
SELECT a FROM t3;
```

### With Sorting
```sql
-- ORDER BY applies to final result
SELECT a FROM t1
INTERSECT
SELECT a FROM t2
ORDER BY a DESC;
```

### With Limiting
```sql
-- LIMIT applies to final result
SELECT a FROM t1
EXCEPT
SELECT a FROM t2
LIMIT 10;
```

## Deduplication

All operations automatically deduplicate:

```sql
-- Input:
t1: [1, 2, 2, 3]
t2: [2, 2, 3, 4]

-- UNION: [1, 2, 3, 4]
-- INTERSECT: [2, 3]
-- EXCEPT: [1]
```

For non-deduplicated results, use ALL variants:
```sql
-- UNION ALL: [1, 2, 2, 3, 2, 2, 3, 4]
-- INTERSECT ALL: [2, 2, 3]  (not implemented yet)
-- EXCEPT ALL: [2]           (not implemented yet)
```

## Error Handling

### Column Count Mismatch
```go
// Should validate but currently doesn't
SELECT a FROM t1        -- 1 column
INTERSECT
SELECT a, b FROM t2;    -- 2 columns -> ERROR
```

### Type Compatibility
```go
// Currently not validated
SELECT intCol FROM t1
INTERSECT
SELECT textCol FROM t2;  // May cause runtime errors
```

## Debugging

### Inspect VDBE Program
```go
vdbe := parse.GetVdbe()
for i, op := range vdbe.Ops {
    fmt.Printf("%3d: %v\n", i, op)
}
```

### Common Issues

1. **Empty results**: Check both queries return data
2. **Unexpected duplicates**: Verify IdxInsert is used
3. **Missing rows**: Check OP_NotFound logic for INTERSECT
4. **Extra rows**: Check OP_IdxDelete for EXCEPT

## Testing Checklist

- [ ] Basic functionality (overlapping results)
- [ ] Empty result sets
- [ ] Identical queries
- [ ] NULL handling
- [ ] Single row results
- [ ] Large result sets
- [ ] With ORDER BY
- [ ] With LIMIT/OFFSET
- [ ] Nested operations
- [ ] Multiple columns
- [ ] Type compatibility

## Future Enhancements

### Short Term
- [ ] Implement INTERSECT ALL
- [ ] Implement EXCEPT ALL
- [ ] Add column count validation
- [ ] Add type compatibility checks

### Long Term
- [ ] Optimize for sorted inputs
- [ ] Disk-based ephemeral tables
- [ ] Parallel query execution
- [ ] Bitmap index optimization

## Resources

- Implementation: `internal/sql/select.go`
- Documentation: `internal/sql/INTERSECT_EXCEPT_IMPLEMENTATION.md`
- Tests: `internal/sql/compound_select_test.go`
- Bytecode Flow: `internal/sql/COMPOUND_SELECT_BYTECODE_FLOW.md`

## SQL Standard References

- SQL:2016 Section 7.17 (Query specification)
- SQL:2016 Section 7.18 (Table expression)
- SQLite documentation: https://www.sqlite.org/lang_select.html#compound_select_statements
