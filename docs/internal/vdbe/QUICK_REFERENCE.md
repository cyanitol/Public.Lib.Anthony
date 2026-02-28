# VDBE Function Quick Reference

## Adding a New Function

### 1. Define the Function (in `/internal/functions/`)

```go
// Scalar function
func myFunc(args []Value) (Value, error) {
    if args[0].IsNull() {
        return NewNullValue(), nil
    }
    // Implementation
    result := // ... compute result
    return NewTextValue(result), nil
}

// Aggregate function
type MyAggFunc struct {
    state int64
}

func (f *MyAggFunc) Name() string { return "myagg" }
func (f *MyAggFunc) NumArgs() int { return 1 }
func (f *MyAggFunc) Call([]Value) (Value, error) {
    return nil, fmt.Errorf("myagg() is an aggregate function")
}
func (f *MyAggFunc) Step(args []Value) error {
    f.state += args[0].AsInt64()
    return nil
}
func (f *MyAggFunc) Final() (Value, error) {
    result := NewIntValue(f.state)
    f.Reset()
    return result, nil
}
func (f *MyAggFunc) Reset() {
    f.state = 0
}
```

### 2. Register the Function

```go
// In RegisterScalarFunctions() or similar
r.Register(NewScalarFunc("myfunc", 1, myFunc))

// For aggregates
r.Register(&MyAggFunc{})
```

### 3. Use in VDBE

```go
// Scalar: MYFUNC(register1) -> register5
v.AddOpWithP4Str(OpFunction, 0, 1, 5, "myfunc")
v.Program[len(v.Program)-1].P5 = 1 // 1 argument

// Aggregate step
v.AddOpWithP4Str(OpAggStep, cursor, argReg, funcIdx, "myagg")
v.Program[len(v.Program)-1].P5 = 1

// Aggregate final
v.AddOp(OpAggFinal, cursor, outReg, funcIdx)
```

## Opcode Quick Reference

### OP_Function (Scalar)
| Parameter | Description |
|-----------|-------------|
| P1 | Constant mask (usually 0) |
| P2 | First argument register |
| P3 | Output register |
| P4 | Function name (string) |
| P5 | Number of arguments |

**Example**: `Function 0 1 5 "upper" 1`

- Call UPPER with arg from r1, store in r5

### OP_AggStep (Aggregate Step)
| Parameter | Description |
|-----------|-------------|
| P1 | Cursor (grouping context) |
| P2 | First argument register |
| P3 | Function index |
| P4 | Function name (string) |
| P5 | Number of arguments |

**Example**: `AggStep 0 1 0 "sum" 1`

- Step SUM (index 0) with arg from r1

### OP_AggFinal (Aggregate Finalize)
| Parameter | Description |
|-----------|-------------|
| P1 | Cursor (grouping context) |
| P2 | Output register |
| P3 | Function index |

**Example**: `AggFinal 0 5 0`

- Finalize aggregate 0 into r5

## Common Patterns

### Scalar Function with Multiple Args

```go
// SUBSTR(r1, r2, r3) -> r5
v.Mem[1].SetStr("hello")
v.Mem[2].SetInt(2)
v.Mem[3].SetInt(3)
v.AddOpWithP4Str(OpFunction, 0, 1, 5, "substr")
v.Program[len(v.Program)-1].P5 = 3
```

### Nested Functions

```go
// UPPER(LOWER(r1)) -> r3
v.Mem[1].SetStr("HeLLo")
// LOWER(r1) -> r2
v.AddOpWithP4Str(OpFunction, 0, 1, 2, "lower")
v.Program[len(v.Program)-1].P5 = 1
// UPPER(r2) -> r3
v.AddOpWithP4Str(OpFunction, 0, 2, 3, "upper")
v.Program[len(v.Program)-1].P5 = 1
```

### Multiple Aggregates

```go
cursor := 0

// For each row
for i, val := range values {
    v.Mem[i].SetInt(val)

    // COUNT - function index 0
    v.AddOpWithP4Str(OpAggStep, cursor, i, 0, "count")
    v.Program[len(v.Program)-1].P5 = 1

    // SUM - function index 1
    v.AddOpWithP4Str(OpAggStep, cursor, i, 1, "sum")
    v.Program[len(v.Program)-1].P5 = 1
}

// Finalize both
v.AddOp(OpAggFinal, cursor, 10, 0) // COUNT -> r10
v.AddOp(OpAggFinal, cursor, 11, 1) // SUM -> r11
```

## Value Type Conversion

### Mem to Value
```go
val := memToValue(mem)
// Automatically handles all types
```

### Value to Mem
```go
mem := valueToMem(value)
// Returns appropriate Mem type
```

### Manual Conversion
```go
// Create Values
v1 := functions.NewIntValue(42)
v2 := functions.NewTextValue("hello")
v3 := functions.NewNullValue()

// Create Mems
m1 := NewMemInt(42)
m2 := NewMemStr("hello")
m3 := NewMemNull()
```

## Error Handling

### In Functions
```go
func myFunc(args []Value) (Value, error) {
    // Return NULL for invalid input
    if args[0].IsNull() {
        return NewNullValue(), nil
    }

    // Return error for truly exceptional cases
    if someError {
        return nil, fmt.Errorf("detailed error message")
    }

    return result, nil
}
```

### In VDBE Execution
```go
result, err := v.funcCtx.ExecuteFunction("myfunc", args)
if err != nil {
    return fmt.Errorf("function myfunc failed: %w", err)
}
```

## Testing Patterns

### Test Scalar Function
```go
func TestMyFunction(t *testing.T) {
    fc := NewFunctionContext()
    args := []*Mem{NewMemStr("input")}

    result, err := fc.ExecuteFunction("myfunc", args)
    if err != nil {
        t.Fatalf("ExecuteFunction() error = %v", err)
    }

    if !result.IsStr() || result.StrValue() != "expected" {
        t.Errorf("got %v, want 'expected'", result.StrValue())
    }
}
```

### Test Aggregate Function
```go
func TestMyAggregate(t *testing.T) {
    fc := NewFunctionContext()
    fn, _ := fc.registry.Lookup("myagg")
    aggFn := createAggregateInstance(fn.(functions.AggregateFunction))

    // Step through values
    for _, val := range testValues {
        args := []functions.Value{memToValue(val)}
        aggFn.Step(args)
    }

    // Finalize and check
    result, err := aggFn.Final()
    if err != nil {
        t.Fatalf("Final() error = %v", err)
    }

    if result.AsInt64() != expectedResult {
        t.Errorf("got %d, want %d", result.AsInt64(), expectedResult)
    }
}
```

## Function Categories

### String Functions (14)
`upper` `lower` `length` `substr` `trim` `ltrim` `rtrim` `replace` `instr` `hex` `unhex` `quote` `unicode` `char`

### Type Functions (5)
`typeof` `coalesce` `ifnull` `nullif` `iif`

### Math Functions (15 basic)
`abs` `round` `ceil` `floor` `sqrt` `power` `exp` `ln` `log10` `log2` `sign` `mod` `pi` `random` `randomblob`

### Trigonometric (15)
`sin` `cos` `tan` `asin` `acos` `atan` `atan2` `sinh` `cosh` `tanh` `asinh` `acosh` `atanh` `radians` `degrees`

### Date/Time (11)
`date` `time` `datetime` `julianday` `unixepoch` `strftime` `current_date` `current_time` `current_timestamp`

### Aggregate (8)
`count` `count(*)` `sum` `total` `avg` `min` `max` `group_concat`

## Common Gotchas

### 1. Aggregate Function Instances
❌ **Wrong**: Reusing same aggregate instance
```go
aggFn := &SumFunc{}
// Reuse across multiple queries - BAD!
```

✅ **Right**: Create new instance per query
```go
aggFn := createAggregateInstance(originalFunc)
```

### 2. NULL Handling
❌ **Wrong**: Treating NULL as zero
```go
if args[0].IsNull() {
    return NewIntValue(0), nil  // Might not be correct
}
```

✅ **Right**: Propagate NULL
```go
if args[0].IsNull() {
    return NewNullValue(), nil
}
```

### 3. Type Conversion
❌ **Wrong**: Assuming type
```go
val := args[0].AsInt64()  // Might lose precision
```

✅ **Right**: Check type first
```go
if args[0].Type() == TypeInteger {
    val := args[0].AsInt64()
} else {
    val := args[0].AsFloat64()
}
```

### 4. Register Allocation
❌ **Wrong**: Using register without allocation
```go
v.Mem[100].SetInt(42)  // Might panic
```

✅ **Right**: Allocate first
```go
v.AllocMemory(101)
v.Mem[100].SetInt(42)
```

## Debugging Tips

### 1. Check Function Registration
```go
fn, ok := registry.Lookup("myfunc")
if !ok {
    // Function not registered
}
```

### 2. Inspect Mem Values
```go
fmt.Printf("Mem: %s\n", mem.String())
// Output: "INT(42)" or "STR("hello")" etc.
```

### 3. Trace Function Calls
```go
func (v *VDBE) opFunction(p1, p2, p3, p4, p5 int) error {
    funcName := instr.P4.Z
    fmt.Printf("Calling %s with %d args\n", funcName, numArgs)
    // ... rest of implementation
}
```

### 4. Verify Aggregate State
```go
aggState := v.funcCtx.GetOrCreateAggregateState(cursor)
fmt.Printf("Aggregate funcs: %d\n", len(aggState.funcs))
```

## Performance Tips

1. **Function Lookup**: Cached in registry (O(1) hash lookup)
2. **Type Conversion**: Direct Mem-to-Value conversion is fast
3. **Aggregates**: Use function index to avoid repeated lookups
4. **Memory**: Reuse registers when possible
5. **NULL Checks**: Early return on NULL for better performance

## Resources

- Full Documentation: `FUNCTIONS.md`
- Implementation Details: `IMPLEMENTATION_SUMMARY.md`
- Test Examples: `functions_test.go`
- Usage Examples: `functions_example_test.go`
