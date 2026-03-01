# INTERSECT and EXCEPT Implementation

## Overview

This document describes the implementation of INTERSECT and EXCEPT compound SELECT operations in the pure Go SQLite database driver.

## Implementation Summary

### Files Modified
- `/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/sql/select.go`

### Changes Made

#### 1. Enhanced `disposeResult()` Function
Added handling for `SRT_Union` and `SRT_Except` destination types:

```go
case SRT_Union:
    // Insert into union table (used for UNION/INTERSECT/EXCEPT)
    // This stores rows as keys in an ephemeral table for deduplication
    r1 := c.parse.AllocReg()
    vdbe.AddOp3(OP_MakeRecord, regResult, nResultCol, r1)
    vdbe.AddOp4Int(OP_IdxInsert, dest.SDParm, r1, regResult, nResultCol)
    c.parse.ReleaseReg(r1)

case SRT_Except:
    // Remove from union table (used for EXCEPT right side)
    // This deletes matching keys from the ephemeral table
    r1 := c.parse.AllocReg()
    vdbe.AddOp3(OP_MakeRecord, regResult, nResultCol, r1)
    vdbe.AddOp4Int(OP_IdxDelete, dest.SDParm, r1, regResult, nResultCol)
    c.parse.ReleaseReg(r1)
```

#### 2. Implemented `compileIntersect()` Function

**Algorithm:**
1. Create ephemeral table `leftTab` for left query results
2. Create ephemeral table `resultTab` for final results
3. Execute left query and store all results in `leftTab` using `SRT_Union`
4. Execute right query and store results in temporary `rightTab`
5. Iterate through `rightTab`:
   - For each row, check if it exists in `leftTab` using `OP_NotFound`
   - If found (exists in both), insert into `resultTab`
6. Output all rows from `resultTab`

**VDBE Bytecode Pattern:**
```
OP_OpenEphemeral leftTab, nCol
OP_OpenEphemeral resultTab, nCol
[compile left SELECT with SRT_Union -> leftTab]
OP_OpenEphemeral rightTab, nCol
[compile right SELECT with SRT_Union -> rightTab]
OP_Rewind rightTab, end
loop:
  OP_Column rightTab, i, regResult+i  (for each column)
  OP_MakeRecord regResult, nCol, regRecord
  OP_NotFound leftTab, notfound, regRecord
  OP_IdxInsert resultTab, regRecord, regResult, nCol
notfound:
  OP_Next rightTab, loop
end:
OP_Close leftTab
OP_Close rightTab
OP_Rewind resultTab, outputEnd
outputLoop:
  OP_Column resultTab, i, regOutput+i
  OP_ResultRow regOutput, nCol
  OP_Next resultTab, outputLoop
outputEnd:
OP_Close resultTab
```

#### 3. Implemented `compileExcept()` Function

**Algorithm:**
1. Create ephemeral table `exceptTab` for left query results
2. Execute left query and store all results in `exceptTab` using `SRT_Union`
3. Execute right query using `SRT_Except` destination:
   - Each matching row is deleted from `exceptTab` using `OP_IdxDelete`
4. Output remaining rows from `exceptTab` (these are rows that were in left but not in right)

**VDBE Bytecode Pattern:**
```
OP_OpenEphemeral exceptTab, nCol
[compile left SELECT with SRT_Union -> exceptTab]
[compile right SELECT with SRT_Except -> exceptTab]
  (this automatically removes matching rows)
OP_Rewind exceptTab, end
loop:
  OP_Column exceptTab, i, regResult+i
  OP_ResultRow regResult, nCol
  OP_Next exceptTab, loop
end:
OP_Close exceptTab
```

## Key Design Decisions

### 1. Use of Ephemeral Tables
Both operations use ephemeral tables (in-memory temporary tables) to store intermediate results. This follows the same pattern as UNION and provides efficient set operations.

### 2. Deduplication
Both INTERSECT and EXCEPT automatically deduplicate results because:
- Rows are stored as keys in ephemeral index tables
- `OP_IdxInsert` only inserts unique keys
- Duplicate rows naturally collapse to a single entry

### 3. INTERSECT Three-Table Approach
INTERSECT uses three ephemeral tables:
- `leftTab`: Stores left query results
- `rightTab`: Stores right query results
- `resultTab`: Stores intersection results

This approach was chosen because it provides:
- Clear separation of concerns
- Efficient lookup using `OP_NotFound` opcode
- Easy verification that rows exist in both sets

Alternative considered: Two-table approach where we iterate through one table and check against the other directly. However, the three-table approach is clearer and easier to maintain.

### 4. EXCEPT Two-Table Approach
EXCEPT uses a more efficient two-table approach:
- `exceptTab`: Initially contains all left query results
- Right query execution directly deletes matching rows using `SRT_Except`

This is more efficient than INTERSECT because:
- We don't need to build a separate result table
- Deletion is done during right query execution
- Final output is simply reading remaining rows

## Comparison with UNION

| Operation | Tables | Algorithm | Deduplication |
|-----------|--------|-----------|---------------|
| UNION | 1 | Both queries insert into same table | Automatic via IdxInsert |
| INTERSECT | 3 | Left->leftTab, Right->rightTab, check & insert->resultTab | Automatic via IdxInsert |
| EXCEPT | 2 | Left->exceptTab, Right deletes from exceptTab | Automatic via IdxInsert |

## VDBE Opcodes Used

### Common Opcodes
- `OP_OpenEphemeral`: Create temporary in-memory table
- `OP_Close`: Close table cursor
- `OP_Rewind`: Reset cursor to beginning
- `OP_Next`: Advance to next row
- `OP_Column`: Extract column value
- `OP_MakeRecord`: Create record from values
- `OP_ResultRow`: Output result row

### Set Operation Specific
- `OP_IdxInsert`: Insert key into index (used for adding to sets)
- `OP_IdxDelete`: Delete key from index (used for EXCEPT)
- `OP_NotFound`: Check if key doesn't exist in index (used for INTERSECT)
- `OP_Found`: Check if key exists in index (alternative to NotFound)

## Example Usage

### INTERSECT Example
```sql
SELECT id, name FROM users WHERE age > 18
INTERSECT
SELECT id, name FROM premium_users;
```

Returns only the users who are both over 18 AND premium members.

### EXCEPT Example
```sql
SELECT id, name FROM all_users
EXCEPT
SELECT id, name FROM banned_users;
```

Returns all users except those who are banned.

## Testing Recommendations

1. **Basic functionality:**
   - Two queries with overlapping results
   - Two queries with no overlapping results
   - Two queries with identical results

2. **Edge cases:**
   - Empty result sets
   - Single row results
   - NULL values in results
   - Different column types

3. **Complex scenarios:**
   - Nested compound operations (UNION within INTERSECT)
   - Operations with ORDER BY, LIMIT, OFFSET
   - Operations on large result sets

4. **Correctness:**
   - Verify deduplication works correctly
   - Verify NULL handling follows SQL standard
   - Compare results with SQLite reference implementation

## Performance Characteristics

### INTERSECT
- Time Complexity: O(n + m) where n, m are result set sizes
- Space Complexity: O(n + m + k) where k is intersection size
- Requires three ephemeral tables

### EXCEPT
- Time Complexity: O(n + m)
- Space Complexity: O(n) (only stores left side, deletes during right execution)
- Requires two ephemeral tables (more efficient than INTERSECT)

## Known Limitations

1. **No optimization for sorted inputs:** Could potentially optimize when both queries are already sorted
2. **No multi-way operations:** `A INTERSECT B INTERSECT C` creates nested operations rather than a single three-way intersection
3. **Memory usage:** Large result sets stored entirely in memory via ephemeral tables

## Future Enhancements

1. **ALL variants:** Implement `INTERSECT ALL` and `EXCEPT ALL` that preserve duplicates
2. **Optimization passes:** Detect when results are pre-sorted and use more efficient algorithms
3. **Disk-based operations:** For very large result sets, spill to disk
4. **Column count validation:** Add explicit validation that left and right queries have same number of columns
5. **Type compatibility checking:** Validate that corresponding columns have compatible types

## References

- SQLite documentation on compound SELECT: https://www.sqlite.org/lang_select.html#compound_select_statements
- SQL standard (ISO/IEC 9075) sections on set operations
- SQLite VDBE documentation: https://www.sqlite.org/opcode.html
