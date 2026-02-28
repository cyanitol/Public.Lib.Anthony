# VDBE Examples and Execution Traces

This document provides detailed examples of VDBE bytecode execution with step-by-step traces.

## Example 1: Simple SELECT

### SQL Query
```sql
SELECT 42 AS answer
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        42    1     0                    0   Load constant
1     ResultRow      1     1     0                    0   Output result
2     Halt           0     0     0                    0   Done
```

### Execution Trace
```
Step 0: PC=0, Op=Integer
  Action: Set r[1] = 42
  State: r[1] = INT(42)

Step 1: PC=1, Op=ResultRow
  Action: Output r[1]
  Result: [42]
  State: HALT

Step 2: PC=2, Op=Halt
  Action: Stop execution
  Final State: HALT, RC=0
```

### Go Code
```go
v := vdbe.New()
v.AllocMemory(10)

v.AddOp(vdbe.OpInteger, 42, 1, 0)
v.AddOp(vdbe.OpResultRow, 1, 1, 0)
v.AddOp(vdbe.OpHalt, 0, 0, 0)

v.Run()
fmt.Println(v.ResultRow[0].IntValue()) // Output: 42
```

## Example 2: Arithmetic Expression

### SQL Query
```sql
SELECT 10 + 20 * 2 AS result
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        10    1     0                    0   r[1] = 10
1     Integer        20    2     0                    0   r[2] = 20
2     Integer        2     3     0                    0   r[3] = 2
3     Multiply       2     3     4                    0   r[4] = r[2] * r[3]
4     Add            1     4     5                    0   r[5] = r[1] + r[4]
5     ResultRow      5     1     0                    0   Output r[5]
6     Halt           0     0     0                    0   Done
```

### Execution Trace
```
Step 0: PC=0, Op=Integer
  Action: r[1] = 10
  Registers: r[1]=INT(10)

Step 1: PC=1, Op=Integer
  Action: r[2] = 20
  Registers: r[1]=INT(10), r[2]=INT(20)

Step 2: PC=2, Op=Integer
  Action: r[3] = 2
  Registers: r[1]=INT(10), r[2]=INT(20), r[3]=INT(2)

Step 3: PC=3, Op=Multiply
  Action: r[4] = r[2] * r[3] = 20 * 2 = 40
  Registers: r[1]=INT(10), r[2]=INT(20), r[3]=INT(2), r[4]=INT(40)

Step 4: PC=4, Op=Add
  Action: r[5] = r[1] + r[4] = 10 + 40 = 50
  Registers: r[1]=INT(10), r[2]=INT(20), r[3]=INT(2), r[4]=INT(40), r[5]=INT(50)

Step 5: PC=5, Op=ResultRow
  Action: Output r[5] = 50
  Result: [50]
  State: HALT
```

## Example 3: Conditional Logic

### SQL Query
```sql
SELECT CASE WHEN x > 10 THEN 'big' ELSE 'small' END
WHERE x = 15
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        15    1     0                    0   r[1] = x = 15
1     Integer        10    2     0                    0   r[2] = 10
2     Gt             1     5     2                    0   if r[1] > r[2] goto 5
3     String         0     3     0     "small"        0   r[3] = 'small'
4     Goto           0     6     0                    0   goto 6
5     String         0     3     0     "big"          0   r[3] = 'big'
6     ResultRow      3     1     0                    0   Output r[3]
7     Halt           0     0     0                    0   Done
```

### Execution Trace
```
Step 0: PC=0, Op=Integer
  Action: r[1] = 15
  Registers: r[1]=INT(15)

Step 1: PC=1, Op=Integer
  Action: r[2] = 10
  Registers: r[1]=INT(15), r[2]=INT(10)

Step 2: PC=2, Op=Gt
  Action: Compare r[1] (15) > r[2] (10)
  Result: TRUE
  Action: Jump to PC=5
  PC: 2 → 5

Step 3: PC=5, Op=String
  Action: r[3] = "big"
  Registers: r[1]=INT(15), r[2]=INT(10), r[3]=STR("big")

Step 4: PC=6, Op=ResultRow
  Action: Output r[3]
  Result: ["big"]
  State: HALT
```

## Example 4: Loop (SUM 1 to 10)

### SQL Query (Conceptual)
```sql
SELECT sum FROM (
  WITH RECURSIVE cnt(x) AS (
    SELECT 0
    UNION ALL
    SELECT x+1 FROM cnt WHERE x < 10
  )
  SELECT SUM(x) as sum FROM cnt
)
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        0     1     0                    0   r[1] = 0 (sum)
1     Integer        0     2     0                    0   r[2] = 0 (counter)
2     Integer        10    3     0                    0   r[3] = 10 (limit)
3     Integer        1     4     0                    0   r[4] = 1 (increment)
4     Add            1     2     1                    0   r[1] = r[1] + r[2] (sum += counter)
5     Add            2     4     2                    0   r[2] = r[2] + 1 (counter++)
6     Le             2     4     3                    0   if r[2] <= r[3] goto 4
7     ResultRow      1     1     0                    0   Output r[1]
8     Halt           0     0     0                    0   Done
```

### Execution Trace
```
Initialization:
Step 0: r[1] = 0 (sum)
Step 1: r[2] = 0 (counter)
Step 2: r[3] = 10 (limit)
Step 3: r[4] = 1 (increment)

Loop Iteration 1:
Step 4: r[1] = r[1] + r[2] = 0 + 0 = 0
Step 5: r[2] = r[2] + 1 = 0 + 1 = 1
Step 6: r[2] (1) <= r[3] (10)? YES → Jump to PC=4

Loop Iteration 2:
Step 4: r[1] = r[1] + r[2] = 0 + 1 = 1
Step 5: r[2] = r[2] + 1 = 1 + 1 = 2
Step 6: r[2] (2) <= r[3] (10)? YES → Jump to PC=4

Loop Iteration 3:
Step 4: r[1] = r[1] + r[2] = 1 + 2 = 3
Step 5: r[2] = r[2] + 1 = 2 + 1 = 3
Step 6: r[2] (3) <= r[3] (10)? YES → Jump to PC=4

... (iterations 4-9) ...

Loop Iteration 10:
Step 4: r[1] = r[1] + r[2] = 36 + 9 = 45
Step 5: r[2] = r[2] + 1 = 9 + 1 = 10
Step 6: r[2] (10) <= r[3] (10)? YES → Jump to PC=4

Loop Iteration 11:
Step 4: r[1] = r[1] + r[2] = 45 + 10 = 55
Step 5: r[2] = r[2] + 1 = 10 + 1 = 11
Step 6: r[2] (11) <= r[3] (10)? NO → Continue to PC=7

Exit:
Step 7: Output r[1] = 55
Result: [55]

Final State: HALT, Result = 55
```

## Example 5: Table Scan (Conceptual)

### SQL Query
```sql
SELECT name, age FROM users WHERE age >= 18
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        18    1     0                    0   r[1] = 18 (threshold)
1     OpenRead       0     2     0                    0   Open cursor 0 on root page 2
2     Rewind         0     11    0                    0   Go to start, jump to 11 if empty
3     Column         0     0     2                    0   r[2] = cursor[0].column[0] (name)
4     Column         0     1     3                    0   r[3] = cursor[0].column[1] (age)
5     Ge             3     7     1                    0   if r[3] >= r[1] goto 7
6     Goto           0     10    0                    0   Skip this row
7     Copy           2     4     0                    0   r[4] = r[2] (name)
8     Copy           3     5     0                    0   r[5] = r[3] (age)
9     ResultRow      4     2     0                    0   Output r[4], r[5]
10    Next           0     3     0                    0   Move to next row, loop to 3
11    Close          0     0     0                    0   Close cursor
12    Halt           0     0     0                    0   Done
```

### Execution Trace (Conceptual)
```
Initialization:
Step 0: r[1] = 18

Open Cursor:
Step 1: Open cursor 0 on table users (root page 2)
Step 2: Rewind cursor to first row

Row 1: ("Alice", 25)
Step 3: r[2] = "Alice" (name)
Step 4: r[3] = 25 (age)
Step 5: r[3] (25) >= r[1] (18)? YES → goto 7
Step 7: r[4] = r[2] = "Alice"
Step 8: r[5] = r[3] = 25
Step 9: Output ["Alice", 25]
Step 10: Next → Row 2

Row 2: ("Bob", 16)
Step 3: r[2] = "Bob"
Step 4: r[3] = 16
Step 5: r[3] (16) >= r[1] (18)? NO → continue
Step 6: Goto 10 (skip this row)
Step 10: Next → Row 3

Row 3: ("Charlie", 30)
Step 3: r[2] = "Charlie"
Step 4: r[3] = 30
Step 5: r[3] (30) >= r[1] (18)? YES → goto 7
Step 7: r[4] = r[2] = "Charlie"
Step 8: r[5] = r[3] = 30
Step 9: Output ["Charlie", 30]
Step 10: Next → EOF

Cleanup:
Step 11: Close cursor
Step 12: Halt

Results: [["Alice", 25], ["Charlie", 30]]
```

## Example 6: JOIN (Conceptual)

### SQL Query
```sql
SELECT users.name, orders.total
FROM users
JOIN orders ON users.id = orders.user_id
```

### Bytecode Program
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     OpenRead       0     2     0                    0   Open cursor 0 on users
1     OpenRead       1     3     0                    0   Open cursor 1 on orders
2     Rewind         0     15    0                    0   users.rewind, jump to 15 if empty
3     Column         0     0     1                    0   r[1] = users.id
4     Column         0     1     2                    0   r[2] = users.name
5     Rewind         1     13    0                    0   orders.rewind
6     Column         1     1     3                    0   r[3] = orders.user_id
7     Column         1     2     4                    0   r[4] = orders.total
8     Eq             1     10    3                    0   if r[1] == r[3] goto 10
9     Goto           0     12    0                    0   Skip non-matching row
10    Copy           2     5     0                    0   r[5] = users.name
11    Copy           4     6     0                    0   r[6] = orders.total
12    ResultRow      5     2     0                    0   Output r[5], r[6]
13    Next           1     6     0                    0   Next order
14    Next           0     3     0                    0   Next user
15    Close          0     0     0                    0   Close users cursor
16    Close          1     0     0                    0   Close orders cursor
17    Halt           0     0     0                    0   Done
```

## Example 7: Aggregate Function (Conceptual)

### SQL Query
```sql
SELECT COUNT(*), AVG(age) FROM users
```

### Bytecode Program (Simplified)
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     OpenRead       0     2     0                    0   Open cursor on users
1     Integer        0     1     0                    0   r[1] = 0 (count)
2     Integer        0     2     0                    0   r[2] = 0 (sum)
3     Rewind         0     10    0                    0   Go to start
4     Integer        1     3     0                    0   r[3] = 1
5     Add            1     3     1                    0   count++
6     Column         0     1     4                    0   r[4] = age
7     Add            2     4     2                    0   sum += age
8     Next           0     4     0                    0   Next row
9     Divide         2     1     3                    0   r[3] = sum / count (avg)
10    ResultRow      1     2     0                    0   Output count, avg
11    Close          0     0     0                    0   Close cursor
12    Halt           0     0     0                    0   Done
```

## Understanding VDBE Concepts

### Registers
Registers are numbered memory cells that hold values:

- `r[0]` - Usually unused
- `r[1..n]` - General purpose registers
- Registers can hold any type (NULL, integer, real, string, blob)
- Type conversions happen automatically when needed

### Cursors
Cursors provide access to table/index data:

- Numbered 0..n
- Each cursor points to a row in a table or index
- Operations: Open, Close, Rewind, Next, Prev, Seek
- Column data accessed via cursor number and column index

### Program Counter (PC)

- Points to the next instruction to execute
- Incremented automatically after each instruction
- Can be modified by jump instructions (Goto, If, etc.)

### Jump Instructions

- Unconditional: `Goto P2` - always jump to address P2
- Conditional: `If P1, P2` - jump to P2 if r[P1] is true
- Comparison: `Gt P1, P2, P3` - jump to P2 if r[P1] > r[P3]

### Instruction Format
```
Opcode P1 P2 P3 P4 P5
```

- **Opcode**: What to do
- **P1**: First operand (often source register or cursor)
- **P2**: Second operand (often destination register or jump target)
- **P3**: Third operand (often another register)
- **P4**: Polymorphic (integer, real, string, pointer)
- **P5**: Flags/options (16-bit unsigned)

### Common Patterns

**Load Constant:**
```
Integer 42 1 0    # r[1] = 42
Real    0  2 0  3.14   # r[2] = 3.14 (P4)
String  0  3 0  "hello" # r[3] = "hello" (P4)
```

**Arithmetic:**
```
Add      1 2 3    # r[3] = r[1] + r[2]
Subtract 1 2 3    # r[3] = r[1] - r[2]
Multiply 1 2 3    # r[3] = r[1] * r[2]
Divide   1 2 3    # r[3] = r[1] / r[2]
```

**Comparison:**
```
Eq  1 5 2    # if r[1] == r[2] goto 5
Lt  1 5 2    # if r[1] < r[2] goto 5
Ge  1 5 2    # if r[1] >= r[2] goto 5
```

**Table Access:**
```
OpenRead  0 rootPage 0    # Open cursor 0
Rewind    0 endAddr  0    # Go to first row
Column    0 colIdx   reg  # Read column into register
Next      0 loopAddr 0    # Next row, loop back
Close     0 0        0    # Close cursor
```

## Performance Tips

1. **Minimize Register Usage**: Reuse registers when values are no longer needed
2. **Reduce Jumps**: Straight-line code is faster than jumps
3. **Avoid Type Conversions**: Keep values in their natural type
4. **Cache Column Values**: Don't read the same column multiple times
5. **Use Indexed Access**: Seek operations are faster than full scans

## Debugging Tips

1. **Use EXPLAIN**: Generate bytecode listing with `v.Explain()`
2. **Add Comments**: Use `v.SetComment(addr, "description")` for clarity
3. **Check Register State**: Print register values at key points
4. **Trace Execution**: Log each instruction as it executes
5. **Verify Jump Targets**: Ensure jump addresses are valid

## Common Errors

1. **Register Out of Range**: Access register beyond allocated range
   - Fix: Call `v.AllocMemory(n)` with sufficient size

2. **Invalid Jump Address**: Jump to non-existent instruction
   - Fix: Verify P2 values in jump instructions

3. **Cursor Not Open**: Access cursor before opening
   - Fix: Ensure OpenRead/OpenWrite before use

4. **Type Mismatch**: Operation on incompatible types
   - Fix: Add explicit type conversion opcodes

5. **NULL Handling**: Unexpected NULL values
   - Fix: Check for NULL before operations
