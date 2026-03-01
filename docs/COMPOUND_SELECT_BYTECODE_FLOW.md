# Compound SELECT VDBE Bytecode Flow

This document illustrates the VDBE bytecode flow for UNION, INTERSECT, and EXCEPT operations.

## UNION Operation

```
Query: SELECT a FROM t1 UNION SELECT a FROM t2

Bytecode Flow:
+-------------------------------------+
| OP_OpenEphemeral unionTab, nCol     |  Create temp table
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Left SELECT]               |
|   Destination: SRT_Union            |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxInsert    |  Insert into unionTab
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Right SELECT]              |
|   Destination: SRT_Union            |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxInsert    |  Insert into unionTab (deduplicates)
+-------------------------------------+
              |
+-------------------------------------+
| OP_Rewind unionTab, end             |  Start reading results
| loop:                               |
|   OP_Column (extract each column)   |
|   OP_ResultRow                      |  Output row
|   OP_Next unionTab, loop            |  Next row
| end:                                |
|   OP_Close unionTab                 |
+-------------------------------------+

Result: All rows from both queries, deduplicated
```

## INTERSECT Operation

```
Query: SELECT a FROM t1 INTERSECT SELECT a FROM t2

Bytecode Flow:
+-------------------------------------+
| OP_OpenEphemeral leftTab, nCol      |  Create temp table for left
| OP_OpenEphemeral resultTab, nCol    |  Create temp table for results
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Left SELECT]               |
|   Destination: SRT_Union -> leftTab  |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxInsert    |  Insert into leftTab
+-------------------------------------+
              |
+-------------------------------------+
| OP_OpenEphemeral rightTab, nCol     |  Create temp table for right
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Right SELECT]              |
|   Destination: SRT_Union -> rightTab |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxInsert    |  Insert into rightTab
+-------------------------------------+
              |
+-------------------------------------+
| OP_Rewind rightTab, end             |  Start intersect loop
| loop:                               |
|   OP_Column (extract each column)   |  Get row from rightTab
|   OP_MakeRecord                     |  Create record
|   OP_NotFound leftTab, skip, rec    |  Check if exists in leftTab
|   OP_IdxInsert resultTab, rec       |  If found, add to results
| skip:                               |
|   OP_Next rightTab, loop            |  Next row
| end:                                |
|   OP_Close leftTab                  |
|   OP_Close rightTab                 |
+-------------------------------------+
              |
+-------------------------------------+
| OP_Rewind resultTab, outputEnd      |  Start reading results
| outputLoop:                         |
|   OP_Column (extract each column)   |
|   OP_ResultRow                      |  Output row
|   OP_Next resultTab, outputLoop     |
| outputEnd:                          |
|   OP_Close resultTab                |
+-------------------------------------+

Result: Only rows present in BOTH queries
```

## EXCEPT Operation

```
Query: SELECT a FROM t1 EXCEPT SELECT a FROM t2

Bytecode Flow:
+-------------------------------------+
| OP_OpenEphemeral exceptTab, nCol    |  Create temp table
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Left SELECT]               |
|   Destination: SRT_Union            |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxInsert    |  Insert into exceptTab
+-------------------------------------+
              |
+-------------------------------------+
| [Compile Right SELECT]              |
|   Destination: SRT_Except           |
|   For each row:                     |
|     OP_MakeRecord -> OP_IdxDelete    |  Delete from exceptTab if exists
+-------------------------------------+
              |
+-------------------------------------+
| OP_Rewind exceptTab, end            |  Start reading results
| loop:                               |
|   OP_Column (extract each column)   |
|   OP_ResultRow                      |  Output row
|   OP_Next exceptTab, loop           |  Next row
| end:                                |
|   OP_Close exceptTab                |
+-------------------------------------+

Result: Rows from left query NOT in right query
```

## Operation Comparison

### Table Usage

```
UNION:
  unionTab: [Left rows] U [Right rows]
  Output from: unionTab

INTERSECT:
  leftTab:   [Left rows]
  rightTab:  [Right rows]
  resultTab: [Left rows] n [Right rows]
  Output from: resultTab

EXCEPT:
  exceptTab: [Left rows] - [Right rows]
  Output from: exceptTab
```

### Set Theory Visualization

```
UNION (A U B):
  A: {1, 2, 3, 4}
  B: {3, 4, 5, 6}
  Result: {1, 2, 3, 4, 5, 6}

INTERSECT (A n B):
  A: {1, 2, 3, 4}
  B: {3, 4, 5, 6}
  Result: {3, 4}

EXCEPT (A - B):
  A: {1, 2, 3, 4}
  B: {3, 4, 5, 6}
  Result: {1, 2}
```

## Key Opcodes

### Data Movement
- **OP_OpenEphemeral**: Create ephemeral (temporary) table
- **OP_Close**: Close cursor
- **OP_Rewind**: Reset cursor to beginning
- **OP_Next**: Advance to next row
- **OP_Column**: Extract column value from current row

### Record Operations
- **OP_MakeRecord**: Pack column values into a record
- **OP_IdxInsert**: Insert record as key in index (automatic dedup)
- **OP_IdxDelete**: Delete record from index

### Lookup Operations
- **OP_Found**: Check if record exists in index (jump if found)
- **OP_NotFound**: Check if record doesn't exist (jump if not found)

### Output
- **OP_ResultRow**: Output row to result set

## Deduplication Mechanism

All three operations use ephemeral tables with index-based storage:

```
Row {1, 'Alice'}:
  1. OP_MakeRecord creates record: [1, 'Alice']
  2. OP_IdxInsert inserts as key

Row {1, 'Alice'} (duplicate):
  1. OP_MakeRecord creates record: [1, 'Alice']
  2. OP_IdxInsert checks if key exists
  3. Key exists -> No insertion (silent ignore)

Result: Automatic deduplication!
```

## Performance Characteristics

### Time Complexity

| Operation | Compile Left | Compile Right | Processing | Total |
|-----------|--------------|---------------|------------|-------|
| UNION | O(n) | O(m) | O(1) | O(n + m) |
| INTERSECT | O(n) | O(m) | O(m log n) | O(n + m log n) |
| EXCEPT | O(n) | O(m) | O(m log n) | O(n + m log n) |

Where:
- n = number of rows in left query
- m = number of rows in right query
- log n factor from index lookup

### Space Complexity

| Operation | Tables | Space |
|-----------|--------|-------|
| UNION | 1 | O(n + m) worst case, O(min(n+m, unique)) typical |
| INTERSECT | 3 | O(n + m + k) where k = intersection size |
| EXCEPT | 2 | O(n) (deletes don't add space) |

## Optimization Opportunities

### 1. Sorted Input Optimization
If both queries are sorted, use merge algorithm:
```
Instead of:
  - Build leftTab
  - Build rightTab
  - Lookup for each right row

Use merge:
  - Cursor on left query
  - Cursor on right query
  - Advance in parallel
  - O(n + m) time, O(1) space
```

### 2. Small Right Set Optimization
If right set is small, use in-memory hash:
```
If sizeof(rightTab) < threshold:
  - Build hash table from right
  - Stream left query
  - Check each row against hash
  - No ephemeral table needed
```

### 3. EXCEPT Specific Optimization
Already optimal! Uses only 2 tables with in-place deletion.

## Example: Complex Nested Operation

```sql
-- Find: (A n B) - C
SELECT id FROM table_a
INTERSECT
SELECT id FROM table_b
EXCEPT
SELECT id FROM table_c
```

Bytecode structure:
```
1. INTERSECT (outer operation):
   - Left: Compile "SELECT id FROM table_a"
   - Right: Compile EXCEPT (becomes nested)

2. EXCEPT (nested operation):
   - Left: Compile "SELECT id FROM table_b"
   - Right: Compile "SELECT id FROM table_c"

Flow:
  +- table_a -+
  |           |
  +- INTERSECT -+- table_b -+
                |           |
                +- EXCEPT --+- table_c

Wait, this parses as: A INTERSECT (B EXCEPT C)
For (A INTERSECT B) EXCEPT C, need parentheses in SQL.
```

## Summary

### UNION
- **Simplest**: 1 table, both sides insert
- **Efficient**: O(n + m) time
- **Use case**: Combine all results

### INTERSECT
- **Medium complexity**: 3 tables, lookup-based
- **Moderate efficiency**: O(n + m log n) time
- **Use case**: Find common results

### EXCEPT
- **Clever design**: 2 tables, deletion-based
- **Good efficiency**: O(n + m log n) time
- **Use case**: Find difference

All operations provide automatic deduplication and follow SQL standard semantics.
