# VDBE - Virtual Database Engine

This package implements the Virtual Database Engine (VDBE) for the JuniperBible SQLite implementation. The VDBE is a bytecode virtual machine that executes SQL queries by running compiled bytecode programs.

## Overview

The VDBE is the core execution engine of SQLite. SQL statements are compiled into bytecode programs consisting of instructions (opcodes) that operate on:

- **Registers (Memory Cells)**: Store intermediate values
- **Cursors**: Point to rows in tables or indexes
- **Program Counter**: Tracks the current instruction

## Architecture

### Components

1. **opcode.go** - Opcode definitions
   - Defines all VDBE instruction opcodes
   - Control flow, data manipulation, arithmetic, comparison, etc.
   - P4 parameter type definitions

2. **mem.go** - Memory cell implementation
   - Mem structure that can hold NULL, integers, reals, strings, or blobs
   - Type conversion methods (Integerify, Realify, Stringify, Numerify)
   - Arithmetic operations (Add, Subtract, Multiply, Divide, Remainder)
   - Comparison and copy operations

3. **vdbe.go** - VDBE virtual machine
   - Main VDBE structure with program, registers, cursors
   - Instruction and cursor management
   - Program building and introspection

4. **exec.go** - Execution engine
   - Step() - Execute one instruction
   - Run() - Execute until halt
   - Individual opcode implementations

## Key Features

### Memory Cells (Mem)

Memory cells are the fundamental data storage unit in VDBE. Each cell can hold:

```go
// Create different types of memory cells
nullMem := NewMemNull()
intMem := NewMemInt(42)
realMem := NewMemReal(3.14159)
strMem := NewMemStr("hello")
blobMem := NewMemBlob([]byte{1, 2, 3})
```

**Type Conversion:**
```go
mem := NewMemStr("123")
mem.Integerify()  // Convert to integer
mem.Realify()     // Convert to real
mem.Stringify()   // Convert to string
```

**Arithmetic:**
```go
a := NewMemInt(10)
b := NewMemInt(20)
a.Add(b)  // a = 30
```

### Opcodes

The VDBE supports a comprehensive set of opcodes organized by category:

**Control Flow:**

- `OpInit` - Initialize program
- `OpGoto` - Unconditional jump
- `OpIf/OpIfNot` - Conditional jumps
- `OpHalt` - Stop execution

**Data Operations:**

- `OpInteger/OpReal/OpString/OpBlob` - Load constants
- `OpCopy/OpMove` - Register operations
- `OpNull` - Set NULL value

**Cursor Operations:**

- `OpOpenRead/OpOpenWrite` - Open table cursor
- `OpRewind/OpNext/OpPrev` - Navigate cursor
- `OpSeekGE/OpSeekLE` - Seek operations
- `OpClose` - Close cursor

**Data Retrieval:**

- `OpColumn` - Read column from cursor
- `OpRowid` - Get row ID
- `OpResultRow` - Output result row

**Data Modification:**

- `OpInsert` - Insert row
- `OpDelete` - Delete row
- `OpUpdate` - Update row

**Comparisons:**

- `OpEq/OpNe` - Equality comparisons
- `OpLt/OpLe/OpGt/OpGe` - Relational comparisons

**Arithmetic:**

- `OpAdd/OpSubtract/OpMultiply/OpDivide/OpRemainder`

**Functions:**

- `OpAggStep/OpAggFinal` - Aggregate functions
- `OpFunction` - Scalar functions

### Cursors

Cursors provide access to table data:

```go
// Open a cursor for reading
v.OpenCursor(0, CursorBTree, rootPage, true)

// Navigate
v.execRewind(...)   // Move to first
v.execNext(...)     // Move to next
v.execColumn(...)   // Read column
```

Cursor types:

- `CursorBTree` - B-tree table/index cursor
- `CursorSorter` - Temporary sorted data
- `CursorVTab` - Virtual table cursor
- `CursorPseudo` - Single-row pseudo-table

## Usage Examples

### Simple Constant Program

```go
v := vdbe.New()
v.AllocMemory(10)

// Load constant 42 into register 1
v.AddOp(OpInteger, 42, 1, 0)

// Halt
v.AddOp(OpHalt, 0, 0, 0)

// Execute
err := v.Run()

// Get result
mem, _ := v.GetMem(1)
value := mem.IntValue()  // 42
```

### Arithmetic Program

```go
v := vdbe.New()
v.AllocMemory(10)

// r[1] = 10
v.AddOp(OpInteger, 10, 1, 0)

// r[2] = 20
v.AddOp(OpInteger, 20, 2, 0)

// r[3] = r[1] + r[2]
v.AddOp(OpAdd, 1, 2, 3)

// Halt
v.AddOp(OpHalt, 0, 0, 0)

v.Run()
result, _ := v.GetMem(3)
// result = 30
```

### Conditional Program

```go
v := vdbe.New()
v.AllocMemory(10)

// r[1] = 1 (true)
v.AddOp(OpInteger, 1, 1, 0)

// If r[1] is true, jump to address 4
v.AddOp(OpIf, 1, 4, 0)

// r[2] = 99 (skipped)
v.AddOp(OpInteger, 99, 2, 0)

// r[2] = 42 (executed)
v.AddOp(OpInteger, 42, 2, 0)

v.AddOp(OpHalt, 0, 0, 0)

v.Run()
result, _ := v.GetMem(2)
// result = 42
```

### Simple Query (Conceptual)

```sql
SELECT x, y FROM table WHERE x > 10
```

Compiles to bytecode like:
```
0: OpenRead 0 rootPage  # Open cursor on table
1: Rewind 0 9           # Go to start, jump to 9 if empty
2: Column 0 0 1         # r[1] = column 0 (x)
3: Column 0 1 2         # r[2] = column 1 (y)
4: Integer 10 3         # r[3] = 10
5: Gt 1 2 3             # if r[1] > r[3] goto 2 (skip if false)
6: Goto 8               # Skip result row
7: ResultRow 1 2        # Output r[1], r[2]
8: Next 0 2             # Loop back to 2
9: Close 0              # Close cursor
10: Halt                # Done
```

## VDBE Instruction Format

Each instruction has:

- **Opcode**: What operation to perform
- **P1, P2, P3**: Integer operands (register numbers, jump addresses, counts)
- **P4**: Polymorphic operand (can be int32, int64, real, string, or pointer)
- **P5**: 16-bit unsigned flags/options

Example instruction breakdown:
```
Opcode: OpAdd
P1: 1        (left operand register)
P2: 2        (right operand register)
P3: 3        (result register)
P4: unused
P5: 0
```

This means: `r[3] = r[1] + r[2]`

## Debugging

### EXPLAIN Output

```go
v := vdbe.New()
v.AllocMemory(10)

v.AddOp(OpInteger, 42, 1, 0)
v.SetComment(0, "Load answer")
v.AddOp(OpResultRow, 1, 1, 0)
v.SetComment(1, "Output")
v.AddOp(OpHalt, 0, 0, 0)

fmt.Println(v.Explain())
```

Output:
```
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------
0     Integer        42    1     0                    0   Load answer
1     ResultRow      1     1     0                    0   Output
2     Halt           0     0     0                    0
```

### Memory Cell Debugging

```go
mem := NewMemInt(42)
fmt.Println(mem.String())  // "INT(42)"

mem = NewMemStr("hello")
fmt.Println(mem.String())  // "STR(\"hello\")"

mem = NewMemNull()
fmt.Println(mem.String())  // "NULL"
```

## Implementation Status

### Implemented

- ✅ Core opcode definitions (146 opcodes)
- ✅ Memory cell with all basic types (NULL, Int, Real, String, Blob)
- ✅ Type conversion (Integerify, Realify, Stringify, Numerify)
- ✅ Arithmetic operations (Add, Subtract, Multiply, Divide, Remainder)
- ✅ Comparison operations (Eq, Ne, Lt, Le, Gt, Ge)
- ✅ Control flow (Goto, If, IfNot, Halt)
- ✅ Register operations (Integer, Copy, Move)
- ✅ Basic cursor operations (Open, Close, Rewind, Next)
- ✅ EXPLAIN output
- ✅ Comprehensive test suite

### Partially Implemented

- ⚠️ Cursor navigation (stub implementation)
- ⚠️ Column extraction (stub implementation)
- ⚠️ Data modification (stub implementation)

### Not Yet Implemented

- ❌ Aggregate functions (AggStep, AggFinal)
- ❌ Scalar functions (Function, PureFunc)
- ❌ Sorting (Sorter opcodes)
- ❌ Virtual tables (VTab opcodes)
- ❌ Transactions (Transaction, Commit, Rollback)
- ❌ Index operations (IdxInsert, IdxDelete, etc.)
- ❌ Integration with B-tree storage layer
- ❌ Record encoding/decoding
- ❌ Collation sequences

## Integration Points

The VDBE integrates with other SQLite components:

1. **Parser/Compiler**: Generates VDBE bytecode from SQL
2. **B-tree Layer**: Cursors interact with B-tree storage
3. **Pager**: Transaction and page management
4. **Functions**: Built-in and user-defined functions
5. **Virtual Tables**: Extension mechanism

## Performance Considerations

1. **Register Allocation**: Minimize register usage during compilation
2. **Jump Optimization**: Reduce unnecessary jumps
3. **Type Affinity**: Avoid unnecessary type conversions
4. **Cursor Caching**: Cache column values to avoid re-parsing
5. **Memory Pooling**: Reuse memory cells when possible

## Testing

Run tests:
```bash
go test -v ./core/sqlite/internal/vdbe/
```

Run benchmarks:
```bash
go test -bench=. ./core/sqlite/internal/vdbe/
```

## References

- SQLite VDBE Documentation: https://www.sqlite.org/opcode.html
- SQLite Source Code: `/tmp/sqlite-src/sqlite-src-3510200/src/vdbe.c`
- VDBE Internals: `/tmp/sqlite-src/sqlite-src-3510200/src/vdbeInt.h`

## License

This implementation is part of JuniperBible and follows the project's license.
The original SQLite VDBE design is public domain.
