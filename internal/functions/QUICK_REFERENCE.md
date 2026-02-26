# SQLite Functions - Quick Reference

## Function Summary

### String Functions (21 functions)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `length(X)` | 1 | Character count (UTF-8) | `length('hello')` → 5 |
| `substr(X,Y[,Z])` | 2-3 | Substring | `substr('hello',2,3)` → 'ell' |
| `upper(X)` | 1 | Uppercase | `upper('hello')` → 'HELLO' |
| `lower(X)` | 1 | Lowercase | `lower('HELLO')` → 'hello' |
| `trim(X[,Y])` | 1-2 | Remove from both ends | `trim('  hi  ')` → 'hi' |
| `ltrim(X[,Y])` | 1-2 | Remove from left | `ltrim('  hi')` → 'hi' |
| `rtrim(X[,Y])` | 1-2 | Remove from right | `rtrim('hi  ')` → 'hi' |
| `replace(X,Y,Z)` | 3 | Replace all Y with Z | `replace('hi','i','o')` → 'ho' |
| `instr(X,Y)` | 2 | Find position | `instr('hello','ll')` → 3 |
| `hex(X)` | 1 | Hex encode | `hex('AB')` → '4142' |
| `unhex(X[,Y])` | 1-2 | Hex decode | `unhex('4142')` → 'AB' |
| `quote(X)` | 1 | SQL literal | `quote('it''s')` → "'it''s'" |
| `unicode(X)` | 1 | First char code | `unicode('A')` → 65 |
| `char(X,...)` | N | From code points | `char(65,66)` → 'AB' |

### Type Functions (5 functions)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `typeof(X)` | 1 | Type name | `typeof(42)` → 'integer' |
| `coalesce(X,...)` | N | First non-NULL | `coalesce(NULL,42)` → 42 |
| `ifnull(X,Y)` | 2 | NULL replacement | `ifnull(NULL,'x')` → 'x' |
| `nullif(X,Y)` | 2 | Conditional NULL | `nullif(5,5)` → NULL |
| `iif(X,Y,Z)` | 3 | Ternary if | `iif(1>0,'y','n')` → 'y' |

### Math Functions (30 functions)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `abs(X)` | 1 | Absolute value | `abs(-5)` → 5 |
| `round(X[,Y])` | 1-2 | Round to Y places | `round(3.14,1)` → 3.1 |
| `ceil(X)` | 1 | Ceiling | `ceil(3.1)` → 4.0 |
| `floor(X)` | 1 | Floor | `floor(3.9)` → 3.0 |
| `sqrt(X)` | 1 | Square root | `sqrt(16)` → 4.0 |
| `power(X,Y)` | 2 | X to power Y | `power(2,3)` → 8.0 |
| `exp(X)` | 1 | e^X | `exp(1)` → 2.718... |
| `ln(X)` | 1 | Natural log | `ln(2.718)` → 1.0 |
| `log10(X)` | 1 | Base-10 log | `log10(100)` → 2.0 |
| `log2(X)` | 1 | Base-2 log | `log2(8)` → 3.0 |
| `sin(X)` | 1 | Sine (radians) | `sin(0)` → 0.0 |
| `cos(X)` | 1 | Cosine (radians) | `cos(0)` → 1.0 |
| `tan(X)` | 1 | Tangent (radians) | `tan(0)` → 0.0 |
| `asin(X)` | 1 | Arcsine | `asin(0)` → 0.0 |
| `acos(X)` | 1 | Arccosine | `acos(1)` → 0.0 |
| `atan(X)` | 1 | Arctangent | `atan(0)` → 0.0 |
| `atan2(Y,X)` | 2 | Two-arg arctan | `atan2(0,1)` → 0.0 |
| `sinh(X)` | 1 | Hyperbolic sine | `sinh(0)` → 0.0 |
| `cosh(X)` | 1 | Hyperbolic cosine | `cosh(0)` → 1.0 |
| `tanh(X)` | 1 | Hyperbolic tangent | `tanh(0)` → 0.0 |
| `asinh(X)` | 1 | Inverse sinh | `asinh(0)` → 0.0 |
| `acosh(X)` | 1 | Inverse cosh | `acosh(1)` → 0.0 |
| `atanh(X)` | 1 | Inverse tanh | `atanh(0)` → 0.0 |
| `sign(X)` | 1 | Sign (-1,0,+1) | `sign(-5)` → -1 |
| `mod(X,Y)` | 2 | Modulo | `mod(10,3)` → 1 |
| `pi()` | 0 | π constant | `pi()` → 3.14159... |
| `radians(X)` | 1 | Deg to rad | `radians(180)` → π |
| `degrees(X)` | 1 | Rad to deg | `degrees(π)` → 180 |
| `random()` | 0 | Random int64 | `random()` |
| `randomblob(N)` | 1 | Random bytes | `randomblob(16)` |
### Aggregate Functions (8 functions)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `count(X)` | 1 | Count non-NULL | `SELECT count(x) FROM t` |
| `count(*)` | 0 | Count all rows | `SELECT count(*) FROM t` |
| `sum(X)` | 1 | Sum (NULL if empty) | `SELECT sum(x) FROM t` |
| `total(X)` | 1 | Sum (0.0 if empty) | `SELECT total(x) FROM t` |
| `avg(X)` | 1 | Average | `SELECT avg(x) FROM t` |
| `min(X)` | 1 | Minimum | `SELECT min(x) FROM t` |
| `max(X)` | 1 | Maximum | `SELECT max(x) FROM t` |
| `group_concat(X[,Y])` | 1-2 | Join with sep | `SELECT group_concat(x,';')` |

### Date/Time Functions (10 functions)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `date(T,...)` | N | YYYY-MM-DD | `date('now')` |
| `time(T,...)` | N | HH:MM:SS | `time('now')` |
| `datetime(T,...)` | N | YYYY-MM-DD HH:MM:SS | `datetime('now')` |
| `julianday(T,...)` | N | Julian day | `julianday('2000-01-01')` |
| `unixepoch(T,...)` | N | Unix timestamp | `unixepoch('now')` |
| `strftime(F,T,...)` | N | Custom format | `strftime('%Y',n'now')` |
| `current_date` | 0 | Current date | `SELECT current_date` |
| `current_time` | 0 | Current time | `SELECT current_time` |
| `current_timestamp` | 0 | Current datetime | `SELECT current_timestamp` |
### Blob Functions (1 function)
| Function | Args | Description | Example |
|----------|------|-------------|---------|
| `zeroblob(N)` | 1 | N zero bytes | `zeroblob(100)` |

## Total: 75 Functions

## Common Patterns

### String Manipulation
```sql
-- Uppercase
SELECT upper('hello');  -- 'HELLO'

-- Extract substring
SELECT substr('hello world', 7, 5);  -- 'world'

-- Find and replace
SELECT replace('hello world', 'world', 'SQLite');  -- 'hello SQLite'

-- Clean whitespace
SELECT trim('  hello  ');  -- 'hello'
```
### Type Checking and Conversion
```sql
-- Check type
SELECT typeof(42);  -- 'integer'

-- Handle NULL
SELECT coalesce(NULL, NULL, 42);  -- 42
SELECT ifnull(column, 'default');

-- Conditional NULL
SELECT nullif(a, b);  -- NULL if a=b, else a
```

### Math Operations
```sql
-- Basic
SELECT abs(-5);        -- 5
SELECT round(3.14159, 2);  -- 3.14

-- Power and roots
SELECT power(2, 10);   -- 1024
SELECT sqrt(16);       -- 4.0

-- Trigonometry
SELECT sin(pi()/2);    -- 1.0
SELECT degrees(pi());  -- 180.0
```

### Aggregation
```sql
-- Count rows
SELECT count(*) FROM users;

-- Statistics
SELECT avg(age), min(age), max(age) FROM users;

-- Sum with NULL handling
SELECT sum(amount), total(amount) FROM sales;

-- Concatenate strings
SELECT group_concat(name, ', ') FROM users;
```
### Date/Time Operations
```sql
-- Current values
SELECT date('now');           -- '2024-01-15'
SELECT datetime('now');       -- '2024-01-15 12:34:56'
SELECT unixepoch('now');      -- 1705323296

-- Date arithmetic
SELECT date('now', '+1 day');
SELECT date('now', '-1 month');
SELECT date('2024-01-15', 'start of month', '+1 month', '-1 day');

-- Formatting
SELECT strftime('%Y-%m-%d %H:%M', 'now');
SELECT strftime('%w', 'now');  -- Day of week
```

## Type Conversion Rules

```
NULL    → (any)   = NULL
INTEGER → FLOAT   = direct cast
INTEGER → TEXT    = string format
FLOAT   → INTEGER = truncate
FLOAT   → TEXT    = string format
TEXT    → INTEGER = parse or 0
TEXT    → FLOAT   = parse or 0.0
BLOB    → INTEGER = byte length
BLOB    → TEXT    = UTF-8 decode
```

## NULL Handling

Most functions:
```
f(NULL) = NULL
f(x, NULL) = NULL (for most f)
```

Exceptions:

- `coalesce(...)`: Returns first non-NULL
- `ifnull(NULL, y)`: Returns y
- `typeof(NULL)`: Returns 'null'
- Aggregates: Skip NULL values

## Error Behavior

Functions return:

- **NULL** for invalid input (parse errors, domain errors)
- **Error** for wrong argument count
- **Error** for type conversion failures
- **NaN** for math domain errors (sqrt(-1))

## Performance Notes

- String functions allocate new strings
- Aggregates use minimal state
- Date parsing is optimized
- Math functions are O(1)
- UTF-8 operations iterate runes

## Common Gotchas

1. **1-indexed positions**: `substr('hello', 1, 2)` → 'he'
2. **Integer division**: Use `1.0*a/b` for float result
3. **NULL propagation**: Check with `coalesce()` or `ifnull()`
4. **Case sensitivity**: `upper('ı')` may vary by locale
5. **Date formats**: Use ISO-8601 (YYYY-MM-DD)

## Quick Examples

```go
// Go usage
registry := functions.DefaultRegistry()

// String
fn, _ := registry.Lookup("upper")
result, _ := fn.Call([]functions.Value{
    functions.NewTextValue("hello"),
})
fmt.Println(result.AsString())  // HELLO

// Math
fn, _ = registry.Lookup("sqrt")
result, _ = fn.Call([]functions.Value{
    functions.NewFloatValue(16),
})
fmt.Println(result.AsFloat64())  // 4.0

// Aggregate
sumFunc := &functions.SumFunc{}
sumFunc.Step([]functions.Value{functions.NewIntValue(10)})
sumFunc.Step([]functions.Value{functions.NewIntValue(20)})
result, _ = sumFunc.Final()
fmt.Println(result.AsInt64())  // 30
```

## Resources

- Full documentation: `README.md`
- Implementation details: `IMPLEMENTATION.md`
- Examples: `examples_test.go`
- Tests: `functions_test.go`
