# Expression Code Generation

This package provides VDBE bytecode generation for SQL expressions in the pure Go SQLite implementation.

## Overview

The `CodeGenerator` translates parser AST nodes into executable VDBE (Virtual Database Engine) bytecode instructions. It handles:

- **Literals**: NULL, integers, floats, strings, blobs
- **Arithmetic**: +, -, *, /, %
- **Comparisons**: =, !=, <, <=, >, >=
- **Logical operators**: AND, OR, NOT (with short-circuit evaluation)
- **Bitwise operators**: &, |, <<, >>, ~
- **Pattern matching**: LIKE, GLOB
- **Range operators**: IN, BETWEEN
- **Functions**: Built-in and user-defined functions
- **Special expressions**: CASE, CAST, IS NULL, IS NOT NULL
- **Column references**: Direct column access via cursors

## Architecture

### CodeGenerator

The main code generator struct:

```go
type CodeGenerator struct {
    vdbe      *vdbe.VDBE           // Target VDBE instance
    nextReg   int                  // Next available register
    cursorMap map[string]int       // Table name -> cursor mapping
}
```

### Key Methods

#### Expression Generation

```go
func (g *CodeGenerator) GenerateExpr(expr parser.Expression) (int, error)
```

Generates code for any expression and returns the register containing the result.

**Example Usage:**
```go
v := vdbe.New()
gen := NewCodeGenerator(v)

// Generate code for: a + b
expr := &parser.BinaryExpr{
    Left:  &parser.IdentExpr{Name: "a"},
    Op:    parser.OpPlus,
    Right: &parser.IdentExpr{Name: "b"},
}

resultReg, err := gen.GenerateExpr(expr)
// resultReg contains the sum
```

#### WHERE Clause Generation

```go
func (g *CodeGenerator) GenerateWhereClause(where parser.Expression, skipLabel int) error
```

Generates conditional code that jumps to `skipLabel` if the WHERE condition is false.

**Example:**
```go
// Generate: WHERE age > 18
where := &parser.BinaryExpr{
    Left:  &parser.IdentExpr{Name: "age"},
    Op:    parser.OpGt,
    Right: &parser.LiteralExpr{Type: parser.LiteralInteger, Value: "18"},
}

skipLabel := gen.CurrentAddr() + 100
err := gen.GenerateWhereClause(where, skipLabel)
```

#### Register Management

```go
func (g *CodeGenerator) AllocReg() int
func (g *CodeGenerator) AllocRegs(n int) int
```

Allocates VDBE registers for temporary values.

## Generated Code Examples

### Simple Arithmetic: `a + b`

```
0  Column     0  0  1       ; r[1]=a
1  Column     0  0  2       ; r[2]=b
2  Add        1  2  3       ; ADD
```

### Comparison with Jump: `x > 10`

```
0  Column     0  0  1       ; r[1]=x
1  Integer    10 2  0       ; INT 10
2  Gt         1  2  3       ; GT
3  IfNot      3  <skip> 0   ; Jump if false
```

### Short-Circuit AND: `a AND b`

```
0  Column     0  0  1       ; r[1]=a
1  Copy       1  3  0       ; Copy to result
2  IfNot      3  7  0       ; If false, skip right
3  Column     0  0  2       ; r[2]=b
4  Copy       2  3  0       ; Copy to result
```

### CASE Expression

```sql
CASE
  WHEN x = 1 THEN 'one'
  WHEN x = 2 THEN 'two'
  ELSE 'other'
END
```

Generates:
```
0  Column     0  0  1       ; r[1]=x
1  Integer    1  2  0       ; INT 1
2  Eq         1  2  3       ; EQ
3  IfNot      3  7  0       ; Jump to next WHEN
4  String8    0  4  'one'   ; STRING 'one'
5  Copy       4  5  0       ; Copy result
6  Goto       0  13 0       ; Jump to end
7  Column     0  0  1       ; r[1]=x
8  Integer    2  6  0       ; INT 2
9  Eq         1  6  7       ; EQ
10 IfNot      7  14 0       ; Jump to ELSE
11 String8    0  8  'two'   ; STRING 'two'
12 Copy       8  5  0       ; Copy result
13 Goto       0  16 0       ; Jump to end
14 String8    0  9  'other' ; STRING 'other'
15 Copy       9  5  0       ; Copy result
```

### BETWEEN Expression: `age BETWEEN 18 AND 65`

```
0  Column     0  0  1       ; r[1]=age
1  Integer    18 2  0       ; INT 18
2  Ge         1  2  3       ; >= lower
3  Column     0  0  1       ; r[1]=age
4  Integer    65 4  0       ; INT 65
5  Le         1  4  5       ; <= upper
6  And        3  5  6       ; Combine
```

### IN Expression: `x IN (1, 2, 3)`

```
0  Column     0  0  1       ; r[1]=x
1  Integer    0  2  0       ; Initialize result to false
2  Integer    1  3  0       ; INT 1
3  Eq         1  3  4       ; Compare
4  If         4  0  0       ; If match, set true
5  Integer    1  2  0       ; Set result true
6  Goto       0  <end> 0    ; Jump to end
7  Integer    2  5  0       ; INT 2
8  Eq         1  5  6       ; Compare
9  If         6  0  0       ; If match, set true
10 Integer    1  2  0       ; Set result true
11 Goto       0  <end> 0    ; Jump to end
...
```

## Short-Circuit Evaluation

The code generator implements proper short-circuit evaluation for AND and OR operators:

### AND Short-Circuit

For `A AND B`:

1. Evaluate A
2. If A is false, skip B and return false
3. Otherwise, evaluate B and return its value

### OR Short-Circuit

For `A OR B`:

1. Evaluate A
2. If A is true, skip B and return true
3. Otherwise, evaluate B and return its value

This optimization avoids unnecessary computation and side effects.

## Integration with VDBE

The code generator produces instructions for the VDBE virtual machine:

### Opcode Mapping

| Expression | VDBE Opcode |
|------------|-------------|
| `a + b` | `OpAdd` |
| `a - b` | `OpSubtract` |
| `a * b` | `OpMultiply` |
| `a / b` | `OpDivide` |
| `a % b` | `OpRemainder` |
| `a = b` | `OpEq` |
| `a != b` | `OpNe` |
| `a < b` | `OpLt` |
| `a <= b` | `OpLe` |
| `a > b` | `OpGt` |
| `a >= b` | `OpGe` |
| `a AND b` | `OpAnd` + `OpIfNot` |
| `a OR b` | `OpOr` + `OpIf` |
| `NOT a` | `OpNot` |
| `-a` | `OpSubtract` (0 - a) |
| `~a` | `OpBitNot` |
| `a & b` | `OpBitAnd` |
| `a \| b` | `OpBitOr` |
| `a << b` | `OpShiftLeft` |
| `a >> b` | `OpShiftRight` |
| `a \|\| b` | `OpConcat` |
| `func(...)` | `OpFunction` |
| `CAST(a AS t)` | `OpCast` |
| `a IS NULL` | `OpIsNull` |
| `a IS NOT NULL` | `OpNotNull` |

### Register Usage

Registers are allocated sequentially starting from 1:

- Temporary values use consecutive registers
- Result values are stored in allocated registers
- The generator ensures sufficient VDBE memory is allocated

### Cursor Integration

Column references are resolved via cursors:
```go
gen.RegisterCursor("users", 0)  // Map table "users" to cursor 0

// Later, "users.name" generates:
// OpColumn cursor=0, column=<index>, result=<reg>
```

## Testing

The package includes comprehensive tests covering:

- **TestSimpleArithmetic**: Basic arithmetic operations
- **TestComparisons**: All comparison operators
- **TestLogicalOperators**: AND/OR with short-circuit verification
- **TestFunctionCalls**: Function invocation including COUNT(*)
- **TestInExpression**: IN with value lists
- **TestBetweenExpression**: BETWEEN range checks
- **TestCaseExpression**: CASE WHEN THEN ELSE END
- **TestUnaryOperators**: Unary operators (NOT, -, ~)
- **TestNullChecks**: IS NULL and IS NOT NULL
- **TestCastExpression**: Type casting
- **TestLiteralValues**: All literal types
- **TestWhereClause**: WHERE clause generation
- **TestComplexExpression**: Nested expressions

### Running Tests

```bash
# Run all expr tests
go test ./core/sqlite/internal/expr

# Run with verbose output
go test -v ./core/sqlite/internal/expr

# Run specific tests
go test ./core/sqlite/internal/expr -run TestSimpleArithmetic

# Run with coverage
go test -cover ./core/sqlite/internal/expr
```

## Future Enhancements

### Not Yet Implemented

1. **Subquery Expressions**: Scalar subqueries in expressions
2. **Window Functions**: OVER clause support
3. **Aggregate Functions**: Proper aggregate handling with GROUP BY
4. **Collation Sequences**: Custom collation in comparisons
5. **Index Optimization**: Using indexes for WHERE clauses
6. **Expression Caching**: Reusing constant subexpressions

### Optimization Opportunities

1. **Constant Folding**: Evaluate constant expressions at compile time
2. **Register Reuse**: Free and reuse temporary registers
3. **Jump Optimization**: Eliminate redundant jumps
4. **Strength Reduction**: Replace expensive operations with cheaper ones
5. **Common Subexpression Elimination**: Avoid recalculating same values

## Implementation Notes

### Parser Integration

The code generator accepts `parser.Expression` interface types and handles all concrete implementations:

- `LiteralExpr`
- `IdentExpr` (column references)
- `BinaryExpr`
- `UnaryExpr`
- `FunctionExpr`
- `CaseExpr`
- `InExpr`
- `BetweenExpr`
- `CastExpr`
- `SubqueryExpr` (stub)

### VDBE Integration

Generated code uses the VDBE instruction set:

- Instructions are added via `vdbe.AddOp()`
- P4 operands for strings use `vdbe.AddOpWithP4Str()`
- Comments aid debugging via `vdbe.SetComment()`
- Jump targets are patched after forward references

### Error Handling

All generation functions return `(int, error)` or `error`:

- Unsupported expressions return descriptive errors
- Missing table/cursor mappings are caught early
- Invalid expressions are rejected with context

## Example Usage in SELECT

```go
// Parse: SELECT a + b AS sum FROM t WHERE x > 10

v := vdbe.New()
gen := NewCodeGenerator(v)

// Register table cursor
gen.RegisterCursor("t", 0)

// Open table cursor
v.AddOp(vdbe.OpOpenRead, 0, <rootpage>, 0)

// Rewind cursor
v.AddOp(vdbe.OpRewind, 0, <end>, 0)
loopStart := v.NumOps()

// Generate WHERE clause
whereExpr := /* x > 10 */
err := gen.GenerateWhereClause(whereExpr, <skip>)

// Generate SELECT expression: a + b
selectExpr := /* a + b */
resultReg, err := gen.GenerateExpr(selectExpr)

// Output result row
v.AddOp(vdbe.OpResultRow, resultReg, 1, 0)

// Next iteration
v.AddOp(vdbe.OpNext, 0, loopStart, 0)

// Close cursor
v.AddOp(vdbe.OpClose, 0, 0, 0)

// Halt
v.AddOp(vdbe.OpHalt, 0, 0, 0)
```

## References

- SQLite VDBE Documentation: https://www.sqlite.org/opcode.html
- SQLite Expression Evaluation: https://www.sqlite.org/c3ref/value.html
- Parser AST: `core/sqlite/internal/parser/ast.go`
- VDBE Implementation: `core/sqlite/internal/vdbe/vdbe.go`
- VDBE Opcodes: `core/sqlite/internal/vdbe/opcode.go`
