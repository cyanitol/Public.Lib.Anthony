# SQLite Expression Package

This package implements a pure Go SQLite expression evaluation and code generation system based on SQLite's `expr.c`.

## Overview

The expression system provides:

- **Expression AST**: Abstract syntax tree representation of SQL expressions
- **Type Affinity**: SQLite's type coercion rules (TEXT, NUMERIC, INTEGER, REAL, BLOB)
- **Comparison Operations**: Full comparison semantics with collation support
- **Arithmetic Operations**: Integer and floating-point arithmetic with overflow handling
- **Code Generation**: VDBE bytecode generation for expression evaluation

## Files

### expr.go
Core expression types and AST node definitions.

**Key Types:**

- `Expr`: Expression AST node with operator, operands, and metadata
- `OpCode`: Expression operation types (literals, operators, functions)
- `ExprFlags`: Properties like EP_IntValue, EP_Collate, EP_Leaf
- `Affinity`: Type affinity constants (AFF_TEXT, AFF_NUMERIC, etc.)
- `ExprList`: List of expressions for function arguments, CASE, etc.

**Expression Types Supported:**

- Literals: NULL, INTEGER, FLOAT, STRING, BLOB
- Column references: table.column
- Binary operators: +, -, *, /, ||, =, <>, <, <=, >, >=, AND, OR
- Unary operators: -, NOT, ~
- Pattern matching: LIKE, GLOB, REGEXP
- Range operators: IN, BETWEEN
- Functions: func(args...)
- CASE expressions: CASE WHEN ... THEN ... ELSE ... END
- CAST: CAST(expr AS type)
- Subqueries: (SELECT ...)

**Example:**
```go
// Create expression: (age + 10) > 18
expr := NewBinaryExpr(OpGt,
    NewBinaryExpr(OpPlus,
        NewColumnExpr("users", "age", 0, 0),
        NewIntExpr(10)),
    NewIntExpr(18))

fmt.Println(expr.String()) // "((users.age + 10) > 18)"
fmt.Println(expr.IsConstant()) // false (contains column)
fmt.Println(expr.Height) // 3
```

### affinity.go
Type affinity determination and application.

**SQLite Type Affinity Rules:**

- **INTEGER**: Contains "INT" → prefer integer representation
- **TEXT**: Contains "CHAR", "CLOB", or "TEXT" → prefer text
- **BLOB**: Contains "BLOB" or empty type → no conversion
- **REAL**: Contains "REAL", "FLOA", or "DOUB" → prefer float
- **NUMERIC**: Otherwise → convert to number if possible

**Key Functions:**

- `GetExprAffinity(e *Expr) Affinity`: Determine expression's affinity
- `AffinityFromType(typeName string) Affinity`: Parse type string
- `CompareAffinity(left, right *Expr) Affinity`: Choose comparison affinity
- `ApplyAffinity(value interface{}, aff Affinity) interface{}`: Convert value
- `PropagateAffinity(e *Expr)`: Set affinity throughout expression tree

**Example:**
```go
// Determine affinity for comparison
leftCol := &Expr{Op: OpColumn, Affinity: AFF_INTEGER}
rightCol := &Expr{Op: OpColumn, Affinity: AFF_TEXT}
aff := CompareAffinity(leftCol, rightCol)
// Result: AFF_NUMERIC (at least one is numeric)

// Apply affinity conversion
val := ApplyAffinity("42", AFF_INTEGER)
// Result: int64(42)
```

### compare.go
Comparison operations with SQLite semantics.

**Features:**

- Three-valued logic (true, false, NULL)
- Type-based comparison precedence
- Collation sequences (BINARY, NOCASE, RTRIM)
- Pattern matching (LIKE, GLOB)
- Range operations (BETWEEN, IN)

**Collation Sequences:**

- `CollSeqBinary`: Byte-by-byte comparison (case-sensitive)
- `CollSeqNoCase`: Case-insensitive comparison
- `CollSeqRTrim`: Comparison ignoring trailing spaces

**Key Functions:**

- `CompareValues(left, right interface{}, aff Affinity, coll *CollSeq) CompareResult`
- `EvaluateComparison(op OpCode, left, right interface{}, ...) interface{}`
- `EvaluateLike(pattern, str string, escape rune) bool`
- `EvaluateGlob(pattern, str string) bool`
- `EvaluateBetween(value, low, high interface{}, ...) interface{}`
- `EvaluateIn(value interface{}, list []interface{}, ...) interface{}`

**Example:**
```go
// Comparison with affinity
result := CompareValues(int64(10), 10.5, AFF_NUMERIC, CollSeqBinary)
// Result: CmpLess

// IS operator (doesn't propagate NULL)
result := EvaluateComparison(OpIs, nil, nil, AFF_NONE, nil)
// Result: true (NULL IS NULL)

// LIKE pattern matching
match := EvaluateLike("h%d", "hello world", 0)
// Result: true
```

### arithmetic.go
Arithmetic and logical operations.

**Features:**

- Integer arithmetic with overflow detection
- Float arithmetic
- Bitwise operations
- String concatenation
- Three-valued logical operations (AND, OR, NOT)
- Type casting

**Key Functions:**

- `EvaluateArithmetic(op OpCode, left, right interface{}) interface{}`
- `EvaluateUnary(op OpCode, operand interface{}) interface{}`
- `EvaluateBitwise(op OpCode, left, right interface{}) interface{}`
- `EvaluateConcat(left, right interface{}) interface{}`
- `EvaluateLogical(op OpCode, left, right interface{}) interface{}`
- `EvaluateCast(value interface{}, targetType string) interface{}`

**Overflow Handling:**
Integer operations that overflow are automatically converted to float.

**Example:**
```go
// Integer addition
result := EvaluateArithmetic(OpPlus, int64(10), int64(20))
// Result: int64(30)

// Overflow to float
result := EvaluateArithmetic(OpPlus, int64(math.MaxInt64), int64(1))
// Result: float64 (overflow detected)

// String concatenation
result := EvaluateConcat("hello", int64(42))
// Result: "hello42"

// Three-valued AND
result := EvaluateLogical(OpAnd, int64(1), nil)
// Result: nil (true AND NULL = NULL)
```

### codegen.go
VDBE bytecode generation for expressions.

**Features:**

- Register allocation and management
- Instruction emission
- Short-circuit evaluation for AND/OR
- Temporary register reuse
- Label resolution for jumps

**VDBE Instructions:**
The code generator produces VDBE instructions like:
```
OP_Column    cursor, col, reg     ; Load column value
OP_Integer   value, reg            ; Load integer constant
OP_Add       reg1, reg2, reg3      ; Add reg1 + reg2 -> reg3
OP_Multiply  reg1, reg2, reg3      ; Multiply reg1 * reg2 -> reg3
OP_Eq        reg1, reg2, label     ; Compare and jump
OP_Function  firstArg, nArgs, reg  ; Call function
```

**Example:**
```go
// Generate code for: a + b * c
ctx := NewCodeGenContext()

// Assume columns are in cursor 0, columns 0, 1, 2
exprA := NewColumnExpr("t", "a", 0, 0)
exprB := NewColumnExpr("t", "b", 0, 1)
exprC := NewColumnExpr("t", "c", 0, 2)

expr := NewBinaryExpr(OpPlus,
    exprA,
    NewBinaryExpr(OpMultiply, exprB, exprC))

targetReg := ctx.CodeExpr(expr, 0)

// Generated instructions:
// OP_Column  0, 0, 1      ; Load a into reg 1
// OP_Column  0, 1, 2      ; Load b into reg 2
// OP_Column  0, 2, 3      ; Load c into reg 3
// OP_Multiply 2, 3, 4     ; b * c -> reg 4
// OP_Add     1, 4, 5      ; a + (b*c) -> reg 5
```

## SQLite Semantics

### NULL Handling

- Arithmetic: NULL + x = NULL
- Comparison: NULL = NULL returns NULL (not true!)
- IS operator: NULL IS NULL returns true
- Logical: true AND NULL = NULL, false AND NULL = false

### Type Coercion
Values are coerced based on context:
```go
// Numeric context
"42" + 10 → 52 (string converted to number)

// Text context
42 || " items" → "42 items" (number converted to text)

// Comparison
SELECT * WHERE age = "25"
// "25" converted to 25 if age is INTEGER affinity
```

### Comparison Rules

1. NULL is distinct (NULL = NULL is NULL, not true)
2. Type precedence: NULL < INTEGER < REAL < TEXT < BLOB
3. For same-type comparisons:
   - Numbers: numeric comparison
   - Text: collation-based comparison
   - Blob: binary byte comparison

### Pattern Matching

**LIKE** (case-insensitive):

- `%` matches zero or more characters
- `_` matches exactly one character
- `ESCAPE 'x'` treats x as escape character

**GLOB** (case-sensitive):

- `*` matches zero or more characters
- `?` matches exactly one character
- Character sets: `[abc]`, `[a-z]`

## Testing

The package includes comprehensive tests:

```bash
go test -v
```

**Test Coverage:**

- Expression creation and manipulation
- Type affinity determination
- Comparison operations (all operators)
- Arithmetic operations (with overflow)
- Bitwise operations
- String operations
- Logical operations (three-valued logic)
- Pattern matching (LIKE, GLOB)
- NULL handling
- Type coercion
- Code generation

## Integration

This package integrates with:

- **Parser**: Builds expression AST from SQL
- **Planner**: Uses expressions in WHERE clause analysis
- **VDBE**: Executes generated bytecode
- **Schema**: Determines column affinities

## Performance Considerations

1. **Register Reuse**: Temporary registers are recycled to minimize allocations
2. **Short-Circuit Evaluation**: AND/OR operators skip unnecessary evaluations
3. **Integer Optimization**: Integer operations preferred over float when possible
4. **Constant Folding**: Could be added to evaluate constant expressions at compile time

## Future Enhancements

- [ ] Constant expression folding during code generation
- [ ] Subquery expression support
- [ ] Window function expressions
- [ ] Common table expression (CTE) support
- [ ] Expression indexing for optimization
- [ ] EXPLAIN support for expression plans

## References

Based on SQLite source code:

- `/tmp/sqlite-src/sqlite-src-3510200/src/expr.c` - Core expression handling
- `/tmp/sqlite-src/sqlite-src-3510200/src/sqliteInt.h` - Type definitions
- SQLite documentation on type affinity and expression evaluation

## License

This is a clean-room implementation following SQLite's public domain blessing:
```
May you do good and not evil.
May you find forgiveness for yourself and forgive others.
May you share freely, never taking more than you give.
```
