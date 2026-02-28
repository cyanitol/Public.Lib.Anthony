# VDBE Function Integration

This document describes how built-in SQL functions are integrated with the VDBE execution engine.

## Overview

The VDBE (Virtual Database Engine) now supports executing both scalar and aggregate SQL functions through a function registry system. Functions are implemented in the `internal/functions` package and wired to the VDBE through the `FunctionContext`.

## Architecture

### Components

1. **FunctionContext** (`functions.go`): Manages function registry and aggregate state
   - Holds the function registry with all available functions
   - Tracks aggregate function state per cursor
   - Handles function lookup and execution

2. **Function Registry** (`internal/functions/functions.go`): Central repository of all SQL functions
   - Scalar functions: Operate on single values
   - Aggregate functions: Accumulate values across multiple rows

3. **Opcodes** (`exec.go`):
   - `OP_Function`: Executes scalar functions
   - `OP_AggStep`: Steps through aggregate function execution
   - `OP_AggFinal`: Finalizes aggregate and produces result

## Function Categories

### Scalar Functions

Scalar functions operate on individual values and return a single result.

#### String Functions

- `upper(X)` - Convert to uppercase
- `lower(X)` - Convert to lowercase
- `length(X)` - Get string/blob length
- `substr(X, Y [, Z])` - Extract substring
- `trim(X [, Y])` - Remove characters from both ends
- `ltrim(X [, Y])` - Remove characters from left
- `rtrim(X [, Y])` - Remove characters from right
- `replace(X, Y, Z)` - Replace all occurrences
- `instr(X, Y)` - Find substring position
- `hex(X)` - Convert to hexadecimal
- `unhex(X [, Y])` - Decode hexadecimal
- `quote(X)` - SQL literal representation
- `unicode(X)` - Unicode code point of first character
- `char(X1, X2, ...)` - Characters from code points

#### Type Functions

- `typeof(X)` - Return type name
- `coalesce(X, Y, ...)` - First non-NULL value
- `ifnull(X, Y)` - Return X if not NULL, else Y
- `nullif(X, Y)` - Return NULL if X == Y, else X
- `iif(X, Y, Z)` - If X then Y else Z

#### Math Functions

- `abs(X)` - Absolute value
- `round(X [, Y])` - Round to Y decimal places
- `ceil(X)` / `ceiling(X)` - Round up
- `floor(X)` - Round down
- `sqrt(X)` - Square root
- `power(X, Y)` / `pow(X, Y)` - X raised to power Y
- `exp(X)` - e^X
- `ln(X)` / `log(X)` - Natural logarithm
- `log10(X)` - Base-10 logarithm
- `log2(X)` - Base-2 logarithm
- `sign(X)` - Sign of number (-1, 0, 1)
- `mod(X, Y)` - Modulo operation
- `pi()` - Value of π
- `radians(X)` - Degrees to radians
- `degrees(X)` - Radians to degrees
- `random()` - Random integer
- `randomblob(N)` - Random N-byte blob

#### Trigonometric Functions

- `sin(X)`, `cos(X)`, `tan(X)` - Basic trig functions
- `asin(X)`, `acos(X)`, `atan(X)` - Inverse trig functions
- `atan2(Y, X)` - Two-argument arctangent
- `sinh(X)`, `cosh(X)`, `tanh(X)` - Hyperbolic functions
- `asinh(X)`, `acosh(X)`, `atanh(X)` - Inverse hyperbolic functions

#### Date/Time Functions

- `date(...)` - Format as date (YYYY-MM-DD)
- `time(...)` - Format as time (HH:MM:SS)
- `datetime(...)` - Format as datetime
- `julianday(...)` - Julian day number
- `unixepoch(...)` - Unix timestamp
- `strftime(format, ...)` - Custom format
- `current_date()` - Current date
- `current_time()` - Current time
- `current_timestamp()` - Current timestamp

#### Blob Functions

- `zeroblob(N)` - Blob of N zero bytes

### Aggregate Functions

Aggregate functions accumulate values across multiple rows and return a single result.

- `count(X)` - Count non-NULL values
- `count(*)` - Count all rows
- `sum(X)` - Sum of values
- `total(X)` - Sum (returns 0.0 for empty set)
- `avg(X)` - Average of values
- `min(X)` - Minimum value
- `max(X)` - Maximum value
- `group_concat(X [, Y])` - Concatenate strings with separator

## Usage in VDBE

### Scalar Function Execution

To execute a scalar function, use the `OP_Function` opcode:

```go
// Example: UPPER("hello") -> register 5
v.Mem[1].SetStr("hello")
v.AddOpWithP4Str(OpFunction, 0, 1, 5, "upper")
v.Program[len(v.Program)-1].P5 = 1 // 1 argument
```

**Opcode Parameters:**

- **P1**: Constant mask (bit flags for constant arguments)
- **P2**: First argument register
- **P3**: Output register
- **P4**: Function name (string)
- **P5**: Number of arguments

### Aggregate Function Execution

Aggregate functions require two phases: stepping and finalization.

```go
// Step phase: accumulate values
for _, value := range values {
    v.Mem[i].SetInt(value)
    v.AddOpWithP4Str(OpAggStep, cursor, i, funcIndex, "sum")
    v.Program[len(v.Program)-1].P5 = 1
}

// Finalization: compute final result
v.AddOp(OpAggFinal, cursor, outputReg, funcIndex)
```

**OP_AggStep Parameters:**

- **P1**: Cursor (for grouping context)
- **P2**: First argument register
- **P3**: Aggregate function index
- **P4**: Function name (string)
- **P5**: Number of arguments

**OP_AggFinal Parameters:**

- **P1**: Cursor (for grouping context)
- **P2**: Output register
- **P3**: Aggregate function index

## Implementation Details

### Value Conversion

The VDBE uses `Mem` structures while functions use `Value` interfaces. Conversion functions handle this:

- `memToValue(*Mem) Value` - Convert VDBE Mem to function Value
- `valueToMem(Value) *Mem` - Convert function Value to VDBE Mem

### NULL Handling

Functions follow SQL NULL semantics:

- Most scalar functions return NULL if any argument is NULL
- `coalesce()` returns the first non-NULL argument
- Aggregate functions skip NULL values (except `count(*)`)

### Type Coercion

Functions perform automatic type coercion:

- Numeric operations convert strings to numbers
- String operations convert numbers to strings
- Type mismatches result in sensible defaults

### Aggregate State Management

Each cursor maintains separate aggregate state:

- Multiple aggregate functions can run concurrently
- State is tracked per function index
- Functions are reset between query executions

## Testing

Comprehensive tests verify:

1. **Scalar Functions** (`TestScalarFunctions`)
   - String manipulation (upper, lower, substr, etc.)
   - Type functions (typeof, coalesce, ifnull)
   - Math operations (abs, round, sqrt, etc.)

2. **Aggregate Functions** (`TestAggregateFunctions`)
   - Count, sum, avg, min, max
   - NULL handling in aggregates
   - Group concatenation

3. **Opcode Execution** (`TestOPFunction`, `TestOPAggStep`)
   - Direct opcode execution
   - Register management
   - Result verification

4. **Edge Cases** (`TestNestedFunctionCalls`, `TestNullHandling`)
   - Nested function calls
   - NULL propagation
   - Type conversions

## Example Queries

### Simple Function Call

```sql
SELECT UPPER(name) FROM users;
```

VDBE bytecode:
```
Column 0 1    # Read name into register 1
Function 1 5  # UPPER(r1) -> r5 (P4="upper", P5=1)
ResultRow 5   # Output register 5
```

### Aggregate Query

```sql
SELECT COUNT(*), SUM(salary) FROM employees;
```

VDBE bytecode:
```
AggStep cursor 0 0    # COUNT (P4="count(*)", P3=0)
AggStep cursor 1 1    # SUM(r1) -> function 1 (P4="sum", P3=1)
AggFinal cursor 5 0   # Finalize COUNT -> r5
AggFinal cursor 6 1   # Finalize SUM -> r6
ResultRow 5 2         # Output r5-r6
```

### Nested Functions

```sql
SELECT UPPER(LOWER("HeLLo"));
```

VDBE bytecode:
```
String "HeLLo" 1      # Load string into r1
Function 1 2          # LOWER(r1) -> r2
Function 2 3          # UPPER(r2) -> r3
ResultRow 3           # Output r3
```

## Performance Considerations

1. **Function Lookup**: Function names are looked up once per execution
2. **Type Conversion**: Minimized through direct Mem-to-Value conversion
3. **Aggregate State**: Efficient per-cursor storage
4. **Memory Management**: Go's GC handles cleanup automatically

## Future Enhancements

Potential improvements:

1. User-defined functions (UDFs)
2. Function result caching for pure functions
3. Compiled function optimization
4. Window functions support
5. Custom aggregates with plugin system

## References

- SQLite Function Documentation: https://www.sqlite.org/lang_corefunc.html
- VDBE Opcode Reference: https://www.sqlite.org/opcode.html
- SQLite Aggregate Functions: https://www.sqlite.org/lang_aggfunc.html
