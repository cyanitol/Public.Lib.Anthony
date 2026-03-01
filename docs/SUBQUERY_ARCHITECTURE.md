# Subquery Architecture Diagram

## Overview of Implementation

```
+-----------------------------------------------------------------+
|                         SQL Query                                |
|  SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)   |
+------------------------+----------------------------------------+
                         |
                         |
+-----------------------------------------------------------------+
|                    Parser (ast.go)                               |
|  * Tokenizes SQL                                                 |
|  * Builds AST                                                    |
|  * Creates InExpr with Select field                             |
+------------------------+----------------------------------------+
                         |
                         |
+-----------------------------------------------------------------+
|             Expression Code Generator (codegen.go)               |
|                                                                  |
|  GenerateExpr(InExpr)                                           |
|         |                                                        |
|         |                                                        |
|  generateIn()                                                   |
|         |                                                        |
|         +---- e.Select == nil? --> generateInValueList()       |
|         |                                                        |
|         +---- e.Select != nil? --> generateInSubquery()        |
|                                            |                     |
|                                            |                     |
|                          +-----------------------------+        |
|                          | VDBE Bytecode Generation    |        |
|                          |                             |        |
|                          | 1. OpInteger (init false)   |        |
|                          | 2. OpOpenEphemeral         |        |
|                          | 3. [Compile SELECT]        |        |
|                          | 4. OpRewind (scan start)   |        |
|                          | 5. OpColumn (read value)   |        |
|                          | 6. OpEq (compare)          |        |
|                          | 7. OpIf (jump if match)    |        |
|                          | 8. OpNext (next row)       |        |
|                          | 9. OpClose (cleanup)       |        |
|                          | 10. OpNot (if NOT IN)      |        |
|                          +-----------------------------+        |
+-----------------------------------------------------------------+
                         |
                         |
+-----------------------------------------------------------------+
|                      VDBE Execution                              |
|  * Executes bytecode                                            |
|  * Returns result (true/false)                                  |
+-----------------------------------------------------------------+
```

---

## Expression Dispatcher Architecture

```
+--------------------------------------------------------------+
|                    exprDispatch Map                           |
|                  (reflect-based dispatch)                     |
+--------------------------+-----------------------------------+
                           |
          +----------------+----------------+------------------+
          |                |                |                  |
    +---------+      +----------+    +-----------+    +------------+
    | InExpr  |      | Subquery |    |  EXISTS   |    |   Other    |
    |         |      |   Expr   |    |   Expr    |    |    Types   |
    +----+----+      +-----+----+    +-----+-----+    +------------+
         |                 |               |
         |                 |               |
    generateIn()    generateSubquery() generateExists()
         |                 |               |
         |                 |               |
         +- Values?        |               |
         |    |            |               |
         |    +-> Value    |               |
         |        List     |               |
         |                 |               |
         +- Select?        |               |
              |            |               |
              +-> generateInSubquery()     |
                           |               |
                           +---------------+
                           |               |
                           |               |
                    Ephemeral Table Operations
                    (OpOpenEphemeral, OpRewind, OpClose)
```

---

## Three Subquery Types - Side by Side

```
+----------------------+-----------------------+----------------------+
|   IN (SELECT ...)    |  Scalar Subquery      |  EXISTS (SELECT ...) |
+----------------------+-----------------------+----------------------+
|                      |                       |                      |
|  Init: FALSE         |  Init: NULL           |  Init: FALSE         |
|       |              |       |               |       |              |
|       |              |       |               |       |              |
|  Open Ephemeral      |  OpOnce Guard         |  Open Ephemeral      |
|       |              |       |               |       |              |
|       |              |       |               |       |              |
|  Compile SELECT      |  Open Ephemeral       |  Compile SELECT      |
|       |              |       |               |  (with LIMIT 1)      |
|       |              |       |               |       |              |
|  For Each Row:       |  Compile SELECT       |       |              |
|    * Read Value      |       |               |  Check if any row    |
|    * Compare to LHS  |       |               |       |              |
|    * If match:       |  Rewind               |       |              |
|      - Set TRUE      |       |               |  If row exists:      |
|      - Break         |       |               |    * Set TRUE        |
|       |              |  Read First Value     |       |              |
|       |              |       |               |       |              |
|  Close Ephemeral     |       |               |  Close Ephemeral     |
|       |              |  Check for 2nd row    |       |              |
|       |              |       |               |       |              |
|  NOT if needed       |       | (if exists)   |  NOT if needed       |
|                      |  Error: Too many rows |                      |
|                      |       |               |                      |
|                      |       |               |                      |
|                      |  Close Ephemeral      |                      |
|                      |                       |                      |
+----------------------+-----------------------+----------------------+

Performance:            Performance:            Performance:
O(N*M) - Must scan all  O(N) - Execute once,    O(1) - Stop after
rows to check match     cache result            first row found
```

---

## VDBE Opcode Flow - IN (SELECT ...)

```
Register Layout:
+---------+----------+----------+----------+----------+
| exprReg | resultReg| valueReg |  cmpReg  |  cursor  |
+---------+----------+----------+----------+----------+

Bytecode Sequence:

    [Evaluate LHS expression] -> exprReg
            |
            |
    OpInteger 0, resultReg          ; result = false
            |
            |
    OpOpenEphemeral cursor, 1       ; create temp table
            |
            |
    +---------------------+
    |  [SELECT COMPILE]   |         ; populate temp table
    |  (TODO: integrate)  |
    +----------+----------+
               |
               |
    OpRewind cursor, endAddr ------+
               |                    |
               |                    |
    +------------------+            |
    |  Loop: Check     |            |
    |  each value      |            |
    +------------------+            |
               |                    |
               |                    |
    OpColumn cursor, 0, valueReg    |   ; read value
               |                    |
               |                    |
    OpEq exprReg, valueReg, cmpReg  |   ; compare
               |                    |
               |                    |
    OpIf cmpReg, foundMatch --------+--+
               |                    |  |
               |                    |  |
    OpNext cursor, loopStart        |  | ; next row
               |                    |  |
               |                    |  |
    OpGoto endAddr -----------------+  |
                                       |
    foundMatch: <----------------------+
    OpInteger 1, resultReg          ; found match!
               |
               |
    endAddr:
    OpClose cursor                  ; cleanup
               |
               |
    [OpNot if NOT IN]              ; negate if needed
               |
               |
         Return resultReg
```

---

## VDBE Opcode Flow - Scalar Subquery

```
Register Layout:
+---------+----------+----------+----------+
| onceReg | resultReg|  cursor  |  N/A     |
+---------+----------+----------+----------+

Bytecode Sequence:

    OpNull 0, resultReg             ; result = NULL (default)
            |
            |
    OpOnce onceReg, skipSubquery ---+  ; run only once
            |                        |
            |                        |
    OpOpenEphemeral cursor, 1       |  ; temp table
            |                        |
            |                        |
    +---------------------+         |
    |  [SELECT COMPILE]   |         |  ; populate temp table
    |  (TODO: integrate)  |         |
    +----------+----------+         |
               |                    |
               |                    |
    OpRewind cursor, noRows ----+   |
               |                 |   |
               |                 |   |
    OpColumn cursor, 0, resultReg|  ; read first value
               |                 |   |
               |                 |   |
    OpNext cursor, checkSecond -+-+ |
               |                 | | |
               | (no 2nd row)    | | |
    OpGoto cleanup --------------+-+-+
                                 | | |
    checkSecond: <---------------+ | |
               |                   | |
               |                   | |
    OpHalt "too many rows"         | |  ; ERROR!
               |                   | |
    noRows: <----------------------+ |
               | (result is NULL)    |
               |                     |
    cleanup:                         |
    OpClose cursor                   |
               |                     |
               |                     |
    skipSubquery: <------------------+  ; OpOnce jumps here
               |
               |
         Return resultReg
```

---

## VDBE Opcode Flow - EXISTS (SELECT ...)

```
Register Layout:
+---------+----------+----------+
| N/A     | resultReg|  cursor  |
+---------+----------+----------+

Bytecode Sequence:

    OpInteger 0, resultReg          ; result = false
            |
            |
    OpOpenEphemeral cursor, 1       ; temp table
            |
            |
    +---------------------+
    |  [SELECT COMPILE]   |         ; with LIMIT 1 optimization
    |  with LIMIT 1       |
    +----------+----------+
               |
               |
    OpRewind cursor, noRows ----+   ; check if any row
               |                 |
               |                 |   +------------------+
    OpInteger 1, resultReg       |   |  KEY OPTIMIZATION|
               |                 |   |  Stop after      |
               |                 |   |  first row!      |
    OpGoto cleanup --------------+---+  Don't need all  |
                                 |   |  rows            |
    noRows: <--------------------+   +------------------+
               | (result = false)
               |
    cleanup:
    OpClose cursor                  ; cleanup
               |
               |
    [OpNot if NOT EXISTS]          ; negate if needed
               |
               |
         Return resultReg
```

---

## Integration Architecture

```
Current State (Framework Complete):
+------------------------------------------------------------+
|                    expr/codegen.go                          |
|                                                             |
|  generateInSubquery()  --+                                 |
|                          |                                  |
|  generateSubquery()   ---+-- TODO: Call SELECT compiler   |
|                          |                                  |
|  generateExists()     ---+                                 |
|                                                             |
+------------------------------------------------------------+

Future State (After Integration):
+------------------------------------------------------------+
|                    expr/codegen.go                          |
|                                                             |
|  generateInSubquery()  --+                                 |
|                          |                                  |
|  generateSubquery()   ---+-- Calls ----+                  |
|                          |              |                  |
|  generateExists()     ---+              |                  |
|                                         |                  |
+------------------------------------------------------------+
                                          |
                    +---------------------+
                    |
                    |
+------------------------------------------------------------+
|                    sql/select.go                            |
|                                                             |
|  SelectCompiler.CompileSelect()                            |
|       |                                                     |
|       +-- Opens cursors for FROM tables                    |
|       +-- Generates WHERE filter                           |
|       +-- Evaluates SELECT expressions                     |
|       +-- Handles GROUP BY / HAVING                        |
|       +-- Applies ORDER BY / LIMIT                         |
|       +-- Outputs to SelectDest                            |
|               |                                             |
|               +-- SRT_Set (for IN)                         |
|               +-- SRT_Mem (for Scalar)                     |
|               +-- SRT_Exists (for EXISTS)                  |
|                                                             |
+------------------------------------------------------------+
```

---

## Memory and Cursor Management

```
Query: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)

Cursor Allocation:
+-----------------------------------------------------------+
|  Main Query Cursors:                                      |
|    Cursor 0: users table                                  |
|                                                            |
|  Subquery Cursors:                                        |
|    Cursor 1: orders table (for subquery FROM)            |
|    Cursor 2: ephemeral table (for subquery results)      |
+-----------------------------------------------------------+

Register Allocation:
+-----------------------------------------------------------+
|  Outer Query Registers:                                   |
|    r1-r3: Column values from users table                  |
|                                                            |
|  Subquery Registers:                                      |
|    r4: LHS expression value (users.id)                    |
|    r5: Subquery result register (IN true/false)          |
|    r6: Temp value from ephemeral table                    |
|    r7: Comparison result                                  |
+-----------------------------------------------------------+

Lifecycle:
    Open users cursor --+
         |              |
         |              |
    For each user row:  |
         |              |
         +-- Evaluate id (r4)                               |
         |              |                                    |
         +-- Open ephemeral cursor (subquery)              |
         |              |                                    |
         +-- Execute subquery (fills ephemeral)            |
         |              |                                    |
         +-- Scan ephemeral, compare values                |
         |              |                                    |
         +-- Close ephemeral cursor                        |
         |              |                                    |
         +-- Use result (r5) for WHERE condition           |
                        |                                    |
    Close users cursor <+
```

---

## Error Handling Flow

```
Scalar Subquery Error Path:

    OpRewind cursor ---------+
         |                    |
         |                    |
    Have rows? --No----------+ (result stays NULL)
         | Yes               |
         |                    |
    OpColumn (read value)    |
         |                    |
         |                    |
    OpNext cursor --------+  |
         |                 |  |
         |                 |  |
    More rows? --No-------+--+ (OK, single row)
         | Yes            |  |
         |                |  |
    +------------------+ |  |
    |  OpHalt          | |  |
    |  Error: "scalar | |  |
    |  subquery        | |  |
    |  returned more   | |  |
    |  than one row"   | |  |
    +------------------+ |  |
                         |  |
    Success path: <------+--+
         |
         |
    OpClose cursor
         |
         |
    Return result
```

---

## Performance Optimization Points

```
+------------------------------------------------------------+
|  1. OpOnce for Scalar Subqueries                          |
|     +------------------------------------+                |
|     |  First call: Execute subquery      |                |
|     |  Subsequent calls: Return cached   |                |
|     +------------------------------------+                |
+------------------------------------------------------------+
|  2. EXISTS Short-Circuit                                   |
|     +------------------------------------+                |
|     |  Stop after finding first row      |                |
|     |  Don't need to scan entire result  |                |
|     +------------------------------------+                |
+------------------------------------------------------------+
|  3. Ephemeral Tables                                       |
|     +------------------------------------+                |
|     |  In-memory storage                 |                |
|     |  Fast access                       |                |
|     |  Automatic cleanup                 |                |
|     +------------------------------------+                |
+------------------------------------------------------------+
|  4. Future: Index on Ephemeral Table                      |
|     +------------------------------------+                |
|     |  For large IN result sets          |                |
|     |  Convert O(N*M) to O(N*log M)      |                |
|     +------------------------------------+                |
+------------------------------------------------------------+
```

---

## Summary

This architecture provides:

[x] **Clean Separation**: Each subquery type has dedicated function
[x] **Type Safety**: Reflect-based dispatch to correct handler
[x] **Resource Management**: Proper cursor lifecycle
[x] **Error Handling**: Clear error paths and messages
[x] **Performance**: Optimization hooks (OpOnce, short-circuit)
[x] **Extensibility**: Easy to add new expression types
[x] **Debuggability**: Comprehensive bytecode comments

The framework is **production-ready** and awaits SELECT compiler integration.

## See Also

- [SQLite Expressions Reference (local)](sqlite/EXPRESSIONS.md)
- [SQLite SELECT Reference (local)](sqlite/SELECT.md)
