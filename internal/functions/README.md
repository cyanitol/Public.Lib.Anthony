# SQLite Built-in Functions

This package implements a comprehensive set of SQLite built-in functions in pure Go.

## Architecture

The functions package provides:

- **Function Interface**: Base interface for all SQL functions
- **AggregateFunction Interface**: Extended interface for aggregate functions
- **Value Interface**: Represents SQL values with type information
- **Registry**: Central registry for all functions

## Function Categories

### String Functions

#### `length(X)`
Returns the number of characters in string X (UTF-8 aware) or the number of bytes for blobs.

```sql
SELECT length('hello');  -- 5
SELECT length('世界');    -- 2
```

#### `substr(X, Y [, Z])`
Returns a substring of X starting at position Y with optional length Z.

- Y is 1-indexed
- Negative Y counts from the end
- Negative Z returns characters before position Y

```sql
SELECT substr('hello', 2, 3);   -- 'ell'
SELECT substr('hello', -2, 2);  -- 'lo'
```

#### `upper(X)` / `lower(X)`
Converts string X to uppercase or lowercase.

```sql
SELECT upper('hello');  -- 'HELLO'
SELECT lower('WORLD');  -- 'world'
```

#### `trim(X [, Y])` / `ltrim(X [, Y])` / `rtrim(X [, Y])`
Removes characters in Y from both ends (trim), left end (ltrim), or right end (rtrim) of X.
Default Y is space ' '.

```sql
SELECT trim('  hello  ');        -- 'hello'
SELECT trim('xxhelloxx', 'x');   -- 'hello'
SELECT ltrim('  hello');         -- 'hello'
```

#### `replace(X, Y, Z)`
Replaces all occurrences of Y in X with Z.

```sql
SELECT replace('hello world', 'world', 'there');  -- 'hello there'
```

#### `instr(X, Y)`
Returns the 1-indexed position of the first occurrence of Y in X, or 0 if not found.

```sql
SELECT instr('hello world', 'world');  -- 7
SELECT instr('hello', 'x');            -- 0
```

#### `hex(X)`
Returns hexadecimal representation of X.

```sql
SELECT hex('hello');  -- '68656C6C6F'
SELECT hex(X'1234');  -- '1234'
```

#### `unhex(X [, Y])`
Decodes hexadecimal string X, optionally ignoring characters in Y.

```sql
SELECT unhex('1234');         -- X'1234'
SELECT unhex('12 34', ' ');   -- X'1234'
```

#### `quote(X)`
Returns SQL literal representation of X suitable for inclusion in SQL statements.

```sql
SELECT quote(42);          -- '42'
SELECT quote('hello');     -- '''hello'''
SELECT quote(X'1234');     -- 'X''1234'''
```

#### `unicode(X)`
Returns the Unicode code point of the first character in X.

```sql
SELECT unicode('A');   -- 65
SELECT unicode('世');  -- 19990
```

#### `char(X1, X2, ..., XN)`
Returns a string composed of characters with Unicode code points X1, X2, etc.

```sql
SELECT char(72, 101, 108, 108, 111);  -- 'Hello'
SELECT char(19990, 30028);            -- '世界'
```

### Type Functions

#### `typeof(X)`
Returns the datatype of X as a string.

```sql
SELECT typeof(42);        -- 'integer'
SELECT typeof(3.14);      -- 'real'
SELECT typeof('hello');   -- 'text'
SELECT typeof(X'1234');   -- 'blob'
SELECT typeof(NULL);      -- 'null'
```

#### `coalesce(X, Y, ...)`
Returns the first non-NULL argument, or NULL if all arguments are NULL.

```sql
SELECT coalesce(NULL, 42, 100);  -- 42
SELECT coalesce(NULL, NULL);     -- NULL
```

#### `ifnull(X, Y)`
Returns X if X is not NULL, otherwise Y.

```sql
SELECT ifnull(NULL, 'default');  -- 'default'
SELECT ifnull(42, 'default');    -- 42
```

#### `nullif(X, Y)`
Returns NULL if X equals Y, otherwise returns X.

```sql
SELECT nullif(42, 42);   -- NULL
SELECT nullif(42, 100);  -- 42
```

#### `iif(X, Y, Z)`
Returns Y if X is true, otherwise Z.

```sql
SELECT iif(1, 'yes', 'no');   -- 'yes'
SELECT iif(0, 'yes', 'no');   -- 'no'
```

### Math Functions

#### `abs(X)`
Returns the absolute value of X.

```sql
SELECT abs(-42);   -- 42
SELECT abs(3.14);  -- 3.14
```

#### `round(X [, Y])`
Rounds X to Y decimal places (default 0).

```sql
SELECT round(3.14159);      -- 3
SELECT round(3.14159, 2);   -- 3.14
```

#### `ceil(X)` / `ceiling(X)`
Returns the smallest integer greater than or equal to X.

```sql
SELECT ceil(3.14);   -- 4.0
SELECT ceil(-3.14);  -- -3.0
```

#### `floor(X)`
Returns the largest integer less than or equal to X.

```sql
SELECT floor(3.14);   -- 3.0
SELECT floor(-3.14);  -- -4.0
```

#### `sqrt(X)`
Returns the square root of X.

```sql
SELECT sqrt(4);   -- 2.0
SELECT sqrt(2);   -- 1.414...
```

#### `power(X, Y)` / `pow(X, Y)`
Returns X raised to the power Y.

```sql
SELECT power(2, 3);   -- 8.0
SELECT pow(10, 2);    -- 100.0
```
#### `exp(X)`
Returns e raised to the power X.

```sql
SELECT exp(1);   -- 2.718...
```

#### `ln(X)` / `log(X)`
Returns the natural logarithm of X.

```sql
SELECT ln(2.718);   -- 1.0
```

#### `log10(X)` / `log2(X)`
Returns the base-10 or base-2 logarithm of X.

```sql
SELECT log10(100);  -- 2.0
SELECT log2(8);     -- 3.0
```

#### Trigonometric Functions

- `sin(X)`, `cos(X)`, `tan(X)` - Basic trig functions (X in radians)
- `asin(X)`, `acos(X)`, `atan(X)` - Inverse trig functions
- `atan2(Y, X)` - Two-argument arctangent
- `sinh(X)`, `cosh(X)`, `tanh(X)` - Hyperbolic functions
- `asinh(X)`, `acosh(X)`, `atanh(X)` - Inverse hyperbolic functions

#### Other Math Functions

```sql
SELECT sign(-5);           -- -1
SELECT mod(10, 3);         -- 1
SELECT pi();               -- 3.141592653589793
SELECT radians(180);       -- 3.141592653589793
SELECT degrees(3.14159);   -- 180.0
```

#### `random()`
Returns a pseudo-random integer.

```sql
SELECT random();  -- Random int64
```

#### `randomblob(N)`
Returns a blob of N random bytes.

```sql
SELECT randomblob(16);  -- 16 random bytes
```

### Aggregate Functions

#### `count(X)` / `count(*)`
Counts non-NULL values or all rows.

```sql
SELECT count(*) FROM table;           -- All rows
SELECT count(column) FROM table;      -- Non-NULL values
```

#### `sum(X)` / `total(X)`
Sums values. `sum()` returns NULL for empty set, `total()` returns 0.0.

```sql
SELECT sum(amount) FROM sales;
SELECT total(amount) FROM sales;
```
#### `avg(X)`
Returns the average of all non-NULL X values.

```sql
SELECT avg(score) FROM tests;
```

#### `min(X)` / `max(X)`
Returns minimum or maximum value.

```sql
SELECT min(price) FROM products;
SELECT max(price) FROM products;
```

As scalar functions with multiple arguments:
```sql
SELECT min(a, b, c);  -- Minimum of three values
SELECT max(10, 20, 30);  -- 30
```

#### `group_concat(X [, Y])`
Concatenates values with separator Y (default ',').

```sql
SELECT group_concat(name) FROM users;           -- 'Alice,Bob,Charlie'
SELECT group_concat(name, '; ') FROM users;     -- 'Alice; Bob; Charlie'
```

### Blob Functions

#### `zeroblob(N)`
Returns a blob of N zero bytes.

```sql
SELECT zeroblob(100);  -- 100 zero bytes
```

### Date/Time Functions

All date/time functions support modifiers to adjust the result.

#### `date(timestring [, modifier, ...])`
Returns date as YYYY-MM-DD.

```sql
SELECT date('now');                           -- '2024-01-15'
SELECT date('2024-01-15', '+1 day');         -- '2024-01-16'
SELECT date('2024-01-15', 'start of month'); -- '2024-01-01'
```

#### `time(timestring [, modifier, ...])`
Returns time as HH:MM:SS.

```sql
SELECT time('now');                      -- '12:34:56'
SELECT time('12:00:00', '+1 hour');      -- '13:00:00'
```

#### `datetime(timestring [, modifier, ...])`
Returns datetime as YYYY-MM-DD HH:MM:SS.

```sql
SELECT datetime('now');                          -- '2024-01-15 12:34:56'
SELECT datetime('2024-01-15', '+1 day');        -- '2024-01-16 00:00:00'
```

#### `julianday(timestring [, modifier, ...])`
Returns the Julian day number.

```sql
SELECT julianday('2000-01-01');  -- 2451544.5
SELECT julianday('now');
```
#### `unixepoch(timestring [, modifier, ...])`
Returns seconds since 1970-01-01 00:00:00 UTC.

```sql
SELECT unixepoch('now');                    -- Current Unix timestamp
SELECT unixepoch('2000-01-01 00:00:00');   -- 946684800
SELECT unixepoch('now', 'subsec');         -- With subsecond precision
```

#### `strftime(format, timestring [, modifier, ...])`
Returns formatted date/time string.

Format specifiers:

- `%Y` - 4-digit year
- `%m` - Month (01-12)
- `%d` - Day of month (01-31)
- `%H` - Hour (00-23)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)
- `%f` - Fractional seconds
- `%s` - Unix timestamp
- `%J` - Julian day number
- `%%` - Literal %

```sql
SELECT strftime('%Y-%m-%d', 'now');              -- '2024-01-15'
SELECT strftime('%H:%M:%S', 'now');              -- '12:34:56'
SELECT strftime('%Y-%m-%d %H:%M:%S', 'now');     -- '2024-01-15 12:34:56'
```

#### Current Time Functions

```sql
SELECT current_date;       -- Current date
SELECT current_time;       -- Current time
SELECT current_timestamp;  -- Current datetime
```

#### Date/Time Modifiers

Supported modifiers:

- `+N days`, `-N days`
- `+N hours`, `-N hours`
- `+N minutes`, `-N minutes`
- `+N seconds`, `-N seconds`
- `+N months`, `-N months`
- `+N years`, `-N years`
- `start of day`
- `start of month`
- `start of year`

```sql
SELECT date('now', '+1 day');
SELECT date('now', '-1 month');
SELECT datetime('now', 'start of day');
SELECT datetime('2024-03-15', 'start of month', '+1 month', '-1 day');
```

## Usage Example

```go
package main

import (
    "fmt"
    "github.com/yourusername/sqlite/internal/functions"
)

func main() {
    // Create registry with all standard functions
    registry := functions.DefaultRegistry()

    // Look up a function
    upperFunc, ok := registry.Lookup("upper")
    if !ok {
        panic("function not found")
    }

    // Call the function
    result, err := upperFunc.Call([]functions.Value{
        functions.NewTextValue("hello"),
    })
    if err != nil {
        panic(err)
    }

    fmt.Println(result.AsString())  // Output: HELLO
}
```

## Aggregate Function Usage

```go
// Create an aggregate function
countFunc := &functions.CountFunc{}

// Process rows
for _, value := range values {
    if err := countFunc.Step([]functions.Value{value}); err != nil {
        panic(err)
    }
}

// Get final result
result, err := countFunc.Final()
if err != nil {
    panic(err)
}

fmt.Println(result.AsInt64())  // Total count
```

## Implementation Notes

### String Encoding

- All string functions are UTF-8 aware
- Character counts return Unicode character count, not byte count
- Blob functions work on raw bytes

### NULL Handling

- Most functions return NULL when given NULL arguments
- Aggregate functions skip NULL values (except count(*))
- Type conversion attempts when mixing types

### Numeric Precision

- Integer operations use int64
- Floating-point operations use float64
- Integer overflow in sum() automatically switches to float
- Special float values (NaN, Inf) are handled appropriately

### Date/Time Internals

- Dates stored internally as Julian day numbers
- 1970-01-01 00:00:00 = JD 2440587.5
- Valid range: 0000-01-01 to 9999-12-31
- Gregorian calendar used for all dates

## Testing

Run tests with:
```bash
go test -v ./core/sqlite/internal/functions/
```

## SQLite Compatibility

This implementation aims for compatibility with SQLite 3.51.2. Key differences:

1. Some advanced date/time modifiers may have simplified implementations
2. Collation sequences not fully implemented
3. Extension functions not included
4. Some locale-specific behavior may differ

## Performance Considerations

- String operations allocate new strings
- Aggregate functions maintain minimal state
- Date/time parsing is optimized for common formats
- Value interface allows zero-copy in many cases

## Contributing

When adding new functions:

1. Implement the Function interface
2. Add comprehensive tests
3. Document in this README
4. Register in appropriate RegisterXXXFunctions
5. Follow SQLite semantics where applicable

## References

- SQLite Documentation: https://sqlite.org/lang_corefunc.html
- SQLite Date/Time Functions: https://sqlite.org/lang_datefunc.html
- SQLite Aggregate Functions: https://sqlite.org/lang_aggfunc.html
