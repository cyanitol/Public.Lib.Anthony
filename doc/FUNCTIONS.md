# SQL Functions Reference

This document provides comprehensive documentation for SQL functions available in Anthony, the Go SQLite implementation. Functions are organized by category with SQL examples and implementation status.

## Table of Contents

- [Core Functions](#core-functions)
- [Aggregate Functions](#aggregate-functions)
- [Date/Time Functions](#datetime-functions)
- [Math Functions](#math-functions)
- [String Functions](#string-functions)
- [Type Functions](#type-functions)
- [JSON Functions](#json-functions)
- [Implementation Status](#implementation-status)

## Core Functions

Core functions provide basic scalar operations that return a single value for each row.

### abs(X)

Returns the absolute value of numeric argument X.

**Syntax:**
```sql
abs(X)
```

**Examples:**
```sql
SELECT abs(-5);           -- Returns: 5
SELECT abs(3.14);         -- Returns: 3.14
SELECT abs(-42.7);        -- Returns: 42.7
SELECT abs(NULL);         -- Returns: NULL
```

**Implementation:** `internal/functions/math.go`

**Notes:**
- Returns NULL if X is NULL
- Returns 0.0 if X cannot be converted to a numeric value
- Throws integer overflow error for -9223372036854775808 (most negative int64)

### char(X1, X2, ..., XN)

Returns a string composed of characters having the Unicode code point values of integers X1 through XN.

**Syntax:**
```sql
char(X1, X2, ..., XN)
```

**Examples:**
```sql
SELECT char(72, 101, 108, 108, 111);  -- Returns: 'Hello'
SELECT char(65, 66, 67);              -- Returns: 'ABC'
SELECT char(0x48, 0x69);              -- Returns: 'Hi'
```

**Implementation:** `internal/functions/scalar.go`

### coalesce(X, Y, ...)

Returns the first non-NULL argument, or NULL if all arguments are NULL.

**Syntax:**
```sql
coalesce(X, Y, ...)
```

**Examples:**
```sql
SELECT coalesce(NULL, 1, 2);          -- Returns: 1
SELECT coalesce(NULL, NULL, 'hello'); -- Returns: 'hello'
SELECT coalesce(10, 20);              -- Returns: 10
SELECT coalesce(NULL, NULL);          -- Returns: NULL
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Requires at least 2 arguments
- Useful for providing default values

### hex(X)

Interprets X as a BLOB and returns the upper-case hexadecimal representation.

**Syntax:**
```sql
hex(X)
```

**Examples:**
```sql
SELECT hex('Hello');      -- Returns: '48656C6C6F'
SELECT hex(123);          -- Returns: '313233'
SELECT hex(X'0A0B0C');    -- Returns: '0A0B0C'
```

**Implementation:** `internal/functions/scalar.go`

**See also:** unhex()

### ifnull(X, Y)

Returns X if X is not NULL, otherwise returns Y. Equivalent to coalesce(X, Y).

**Syntax:**
```sql
ifnull(X, Y)
```

**Examples:**
```sql
SELECT ifnull(NULL, 'default');  -- Returns: 'default'
SELECT ifnull(42, 0);            -- Returns: 42
SELECT ifnull(NULL, NULL);       -- Returns: NULL
```

**Implementation:** `internal/functions/scalar.go`

### iif(B1, V1, ...)

Returns the value associated with the first true Boolean. Multi-argument variant of CASE expressions.

**Syntax:**
```sql
iif(boolean1, value1, [boolean2, value2, ...], [else_value])
```

**Examples:**
```sql
SELECT iif(1, 'true', 'false');              -- Returns: 'true'
SELECT iif(0, 'true', 'false');              -- Returns: 'false'
SELECT iif(age > 18, 'adult', 'minor') FROM users;
SELECT iif(x > 0, 'pos', x < 0, 'neg', 'zero');
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Short-circuit evaluation: only evaluates necessary arguments
- Odd number of arguments: last argument is the else value
- Even number of arguments: returns NULL if no condition is true
- Supported since SQLite 3.48.0 (multi-argument form in 3.49.0)

### instr(X, Y)

Finds the first occurrence of string Y within string X and returns the 1-based position.

**Syntax:**
```sql
instr(X, Y)
```

**Examples:**
```sql
SELECT instr('Hello World', 'World');  -- Returns: 7
SELECT instr('abcabc', 'bc');          -- Returns: 2
SELECT instr('hello', 'xyz');          -- Returns: 0
SELECT instr('test', 't');             -- Returns: 1
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Returns 0 if Y is not found in X
- Works with both strings and BLOBs
- 1-indexed (first character is position 1)

### length(X)

Returns the number of characters in string X (or bytes in BLOB X).

**Syntax:**
```sql
length(X)
```

**Examples:**
```sql
SELECT length('Hello');      -- Returns: 5
SELECT length('Hello World');  -- Returns: 11 (UTF-8 aware)
SELECT length(X'0A0B0C');    -- Returns: 3 (bytes)
SELECT length(NULL);         -- Returns: NULL
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- For strings: returns character count (UTF-8 code points)
- For BLOBs: returns byte count
- Returns NULL if X is NULL

### lower(X)

Returns a copy of string X with all ASCII characters converted to lowercase.

**Syntax:**
```sql
lower(X)
```

**Examples:**
```sql
SELECT lower('HELLO');       -- Returns: 'hello'
SELECT lower('Hello World'); -- Returns: 'hello world'
SELECT lower('ABC123');      -- Returns: 'abc123'
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Only converts ASCII characters
- For non-ASCII case conversion, use ICU extension

### ltrim(X, [Y])

Removes characters in Y from the left side of X. Defaults to removing spaces.

**Syntax:**
```sql
ltrim(X)
ltrim(X, Y)
```

**Examples:**
```sql
SELECT ltrim('  hello');         -- Returns: 'hello'
SELECT ltrim('xxxhello', 'x');   -- Returns: 'hello'
SELECT ltrim('aaabbbccc', 'ab'); -- Returns: 'ccc'
```

**Implementation:** `internal/functions/scalar.go`

### nullif(X, Y)

Returns NULL if X equals Y, otherwise returns X.

**Syntax:**
```sql
nullif(X, Y)
```

**Examples:**
```sql
SELECT nullif(5, 5);         -- Returns: NULL
SELECT nullif(5, 6);         -- Returns: 5
SELECT nullif('a', 'a');     -- Returns: NULL
SELECT nullif('a', 'b');     -- Returns: 'a'
```

**Implementation:** `internal/functions/scalar.go`

### quote(X)

Returns the text of an SQL literal representing the value of X.

**Syntax:**
```sql
quote(X)
```

**Examples:**
```sql
SELECT quote('hello');       -- Returns: '''hello'''
SELECT quote(123);           -- Returns: '123'
SELECT quote(NULL);          -- Returns: 'NULL'
SELECT quote(X'0A0B');       -- Returns: 'X''0A0B'''
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Strings are surrounded by single quotes with proper escaping
- BLOBs are encoded as hexadecimal literals
- Useful for generating dynamic SQL

### random()

Returns a pseudo-random integer between -9223372036854775807 and +9223372036854775807.

**Syntax:**
```sql
random()
```

**Examples:**
```sql
SELECT random();                    -- Returns: random integer
SELECT abs(random() % 100);         -- Returns: 0-99
SELECT lower(hex(randomblob(16)));  -- Returns: UUID-like string
```

**Implementation:** `internal/functions/math.go`

**Notes:**
- Deliberately avoids -9223372036854775808 for compatibility with abs()

### randomblob(N)

Returns an N-byte BLOB containing pseudo-random bytes.

**Syntax:**
```sql
randomblob(N)
```

**Examples:**
```sql
SELECT randomblob(16);              -- Returns: 16-byte random BLOB
SELECT hex(randomblob(4));          -- Returns: 8-char hex string
SELECT lower(hex(randomblob(16)));  -- Returns: UUID v4 (lowercase)
```

**Implementation:** `internal/functions/math.go`

**Notes:**
- If N < 1, returns 1-byte BLOB
- Useful for generating GUIDs/UUIDs

### replace(X, Y, Z)

Returns a string formed by substituting string Z for every occurrence of string Y in string X.

**Syntax:**
```sql
replace(X, Y, Z)
```

**Examples:**
```sql
SELECT replace('Hello World', 'World', 'Go');  -- Returns: 'Hello Go'
SELECT replace('aaa', 'a', 'b');               -- Returns: 'bbb'
SELECT replace('test', 'x', 'y');              -- Returns: 'test'
```

**Implementation:** `internal/functions/scalar.go`

### round(X, [Y])

Returns X rounded to Y digits to the right of the decimal point.

**Syntax:**
```sql
round(X)
round(X, Y)
```

**Examples:**
```sql
SELECT round(3.14159);       -- Returns: 3.0
SELECT round(3.14159, 2);    -- Returns: 3.14
SELECT round(3.14159, 4);    -- Returns: 3.1416
SELECT round(123.456, -1);   -- Returns: 120.0
```

**Implementation:** `internal/functions/math.go`

**Notes:**
- Y defaults to 0 if omitted
- Negative Y rounds to the left of decimal point

### rtrim(X, [Y])

Removes characters in Y from the right side of X. Defaults to removing spaces.

**Syntax:**
```sql
rtrim(X)
rtrim(X, Y)
```

**Examples:**
```sql
SELECT rtrim('hello  ');         -- Returns: 'hello'
SELECT rtrim('helloxxx', 'x');   -- Returns: 'hello'
SELECT rtrim('aaabbbccc', 'bc'); -- Returns: 'aaa'
```

**Implementation:** `internal/functions/scalar.go`

### substr(X, Y, [Z])

Returns a substring of X starting at position Y with length Z (or to end if Z omitted).

**Syntax:**
```sql
substr(X, Y)
substr(X, Y, Z)
substring(X, Y, Z)  -- Alias
```

**Examples:**
```sql
SELECT substr('Hello World', 7);     -- Returns: 'World'
SELECT substr('Hello World', 1, 5);  -- Returns: 'Hello'
SELECT substr('Hello', -3);          -- Returns: 'llo'
SELECT substr('Hello', 2, 3);        -- Returns: 'ell'
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Y is 1-indexed (first character is 1)
- Negative Y counts from the end
- Negative Z returns characters preceding position Y
- Works with both strings and BLOBs

### trim(X, [Y])

Removes characters in Y from both ends of X. Defaults to removing spaces.

**Syntax:**
```sql
trim(X)
trim(X, Y)
```

**Examples:**
```sql
SELECT trim('  hello  ');          -- Returns: 'hello'
SELECT trim('xxxhelloxxx', 'x');   -- Returns: 'hello'
SELECT trim('aaabbbccc', 'ac');    -- Returns: 'bbb'
```

**Implementation:** `internal/functions/scalar.go`

### typeof(X)

Returns a string indicating the datatype of expression X.

**Syntax:**
```sql
typeof(X)
```

**Examples:**
```sql
SELECT typeof(123);          -- Returns: 'integer'
SELECT typeof(3.14);         -- Returns: 'real'
SELECT typeof('hello');      -- Returns: 'text'
SELECT typeof(NULL);         -- Returns: 'null'
SELECT typeof(X'0A0B');      -- Returns: 'blob'
```

**Implementation:** `internal/functions/scalar.go`

**Returns:** One of: "null", "integer", "real", "text", "blob"

### unhex(X, [Y])

Returns a BLOB which is the decoding of hexadecimal string X.

**Syntax:**
```sql
unhex(X)
unhex(X, Y)
```

**Examples:**
```sql
SELECT unhex('48656C6C6F');      -- Returns: BLOB 'Hello'
SELECT unhex('0A0B0C');          -- Returns: BLOB X'0A0B0C'
SELECT unhex('48-65-6C', '-');   -- Returns: BLOB 'Hel'
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- X must contain only hex digits and characters in Y
- Characters in Y are ignored (used as separators)
- Returns NULL if X contains invalid characters

### unicode(X)

Returns the numeric Unicode code point of the first character in string X.

**Syntax:**
```sql
unicode(X)
```

**Examples:**
```sql
SELECT unicode('A');         -- Returns: 65
SELECT unicode('Hello');     -- Returns: 72 (code for 'H')
SELECT unicode('X');         -- Returns: 88
```

**Implementation:** `internal/functions/scalar.go`

### upper(X)

Returns a copy of string X with all lowercase ASCII characters converted to uppercase.

**Syntax:**
```sql
upper(X)
```

**Examples:**
```sql
SELECT upper('hello');       -- Returns: 'HELLO'
SELECT upper('Hello World'); -- Returns: 'HELLO WORLD'
SELECT upper('abc123');      -- Returns: 'ABC123'
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- Only converts ASCII characters
- For non-ASCII case conversion, use ICU extension

### zeroblob(N)

Returns a BLOB consisting of N bytes of 0x00.

**Syntax:**
```sql
zeroblob(N)
```

**Examples:**
```sql
SELECT zeroblob(10);         -- Returns: 10-byte zero BLOB
SELECT length(zeroblob(100)); -- Returns: 100
```

**Implementation:** `internal/functions/scalar.go`

**Notes:**
- SQLite manages zeroblobs efficiently
- Useful for reserving space for incremental BLOB I/O

## Aggregate Functions

Aggregate functions compute a single result from a set of input values.

### count(X) / count(*)

Counts the number of rows or non-NULL values.

**Syntax:**
```sql
count(*)      -- Count all rows
count(X)      -- Count non-NULL values
count(DISTINCT X)  -- Count distinct non-NULL values
```

**Examples:**
```sql
SELECT count(*) FROM users;
SELECT count(email) FROM users;
SELECT count(DISTINCT country) FROM users;
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- count(*) counts all rows including NULLs
- count(X) counts only non-NULL values

### sum(X)

Returns the sum of all non-NULL values in the group.

**Syntax:**
```sql
sum(X)
sum(DISTINCT X)
```

**Examples:**
```sql
SELECT sum(price) FROM orders;
SELECT sum(DISTINCT amount) FROM transactions;
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Returns NULL if all values are NULL
- Returns integer sum if all values are integers
- Returns floating-point sum if any value is float

### avg(X)

Returns the average of all non-NULL values in the group.

**Syntax:**
```sql
avg(X)
avg(DISTINCT X)
```

**Examples:**
```sql
SELECT avg(age) FROM users;
SELECT avg(DISTINCT score) FROM test_results;
SELECT avg(price) FROM products WHERE category = 'Electronics';
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Returns NULL if all values are NULL
- Always returns a floating-point result

### min(X)

Returns the minimum value of all values in the group.

**Syntax:**
```sql
min(X)
```

**Examples:**
```sql
SELECT min(price) FROM products;
SELECT min(created_at) FROM orders;
SELECT min(age) FROM users WHERE city = 'NYC';
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Can be used with any datatype
- NULL values are ignored
- With 2+ arguments, acts as scalar function returning minimum

### max(X)

Returns the maximum value of all values in the group.

**Syntax:**
```sql
max(X)
```

**Examples:**
```sql
SELECT max(price) FROM products;
SELECT max(created_at) FROM orders;
SELECT max(age) FROM users WHERE city = 'NYC';
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Can be used with any datatype
- NULL values are ignored
- With 2+ arguments, acts as scalar function returning maximum

### group_concat(X, [separator])

Returns a string which is the concatenation of all non-NULL values of X.

**Syntax:**
```sql
group_concat(X)
group_concat(X, separator)
```

**Examples:**
```sql
SELECT group_concat(name) FROM users;
SELECT group_concat(name, ', ') FROM users;
SELECT group_concat(tag, '; ') FROM tags WHERE post_id = 1;
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Default separator is comma (',')
- NULL values are ignored
- Returns NULL if all values are NULL

### total(X)

Like sum(X), but returns 0.0 instead of NULL when all values are NULL.

**Syntax:**
```sql
total(X)
```

**Examples:**
```sql
SELECT total(price) FROM orders WHERE user_id = 999;  -- Returns: 0.0
SELECT sum(price) FROM orders WHERE user_id = 999;    -- Returns: NULL
```

**Implementation:** `internal/functions/aggregate.go`

**Notes:**
- Always returns a floating-point number
- Never returns NULL (returns 0.0 instead)

## Date/Time Functions

Functions for working with dates and times. SQLite stores dates as ISO-8601 text, Julian day numbers, or Unix timestamps.

### date(timevalue, [modifier, ...])

Returns the date as YYYY-MM-DD.

**Syntax:**
```sql
date(timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT date('now');                          -- Returns: current date
SELECT date('2025-02-28');                   -- Returns: '2025-02-28'
SELECT date('now', 'start of month');        -- Returns: first day of current month
SELECT date('2025-02-15', '+1 month');       -- Returns: '2025-03-15'
SELECT date('now', 'start of year', '+9 months', 'weekday 2');  -- First Tuesday in October
```

**Implementation:** `internal/functions/date.go`

**Time Value Formats:**
- YYYY-MM-DD
- YYYY-MM-DD HH:MM:SS
- YYYY-MM-DDTHH:MM:SS
- 'now' (current UTC time)
- Julian day number
- Unix timestamp (with 'unixepoch' or 'auto' modifier)

**Common Modifiers:**
- 'start of month', 'start of year', 'start of day'
- '+N days', '+N months', '+N years'
- '+N hours', '+N minutes', '+N seconds'
- 'weekday N' (0=Sunday, 1=Monday, etc.)
- 'localtime', 'utc'

### time(timevalue, [modifier, ...])

Returns the time as HH:MM:SS.

**Syntax:**
```sql
time(timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT time('now');                          -- Returns: current time
SELECT time('12:30:00');                     -- Returns: '12:30:00'
SELECT time('now', 'localtime');             -- Returns: current local time
SELECT time('12:00:00', '+1 hour');          -- Returns: '13:00:00'
```

**Implementation:** `internal/functions/date.go`

### datetime(timevalue, [modifier, ...])

Returns the date and time as YYYY-MM-DD HH:MM:SS.

**Syntax:**
```sql
datetime(timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT datetime('now');                             -- Returns: current datetime
SELECT datetime('2025-02-28 14:30:00');            -- Returns: '2025-02-28 14:30:00'
SELECT datetime(1092941466, 'unixepoch');          -- Returns: '2004-08-19 18:51:06'
SELECT datetime(1092941466, 'unixepoch', 'localtime');  -- Convert to local time
SELECT datetime('now', '+1 day');                  -- Returns: tomorrow same time
```

**Implementation:** `internal/functions/date.go`

### julianday(timevalue, [modifier, ...])

Returns the Julian day number (fractional days since noon on November 24, 4714 BC).

**Syntax:**
```sql
julianday(timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT julianday('now');                     -- Returns: current Julian day
SELECT julianday('2025-02-28');             -- Returns: 2460825.5
SELECT julianday('now') - julianday('1776-07-04');  -- Days since US independence
```

**Implementation:** `internal/functions/date.go`

**Notes:**
- Returns a floating-point number
- Useful for calculating date differences

### unixepoch(timevalue, [modifier, ...])

Returns the Unix timestamp (seconds since 1970-01-01 00:00:00 UTC).

**Syntax:**
```sql
unixepoch(timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT unixepoch('now');                         -- Returns: current Unix timestamp
SELECT unixepoch('2025-02-28 00:00:00');        -- Returns: 1740700800
SELECT unixepoch('now', 'subsec');              -- Returns: timestamp with milliseconds
SELECT unixepoch() - unixepoch('2004-01-01');   -- Seconds since 2004
```

**Implementation:** `internal/functions/date.go`

**Notes:**
- Returns integer by default
- Use 'subsec' modifier for fractional seconds

### strftime(format, timevalue, [modifier, ...])

Returns the date formatted according to the format string.

**Syntax:**
```sql
strftime(format, timevalue, [modifier, ...])
```

**Examples:**
```sql
SELECT strftime('%Y-%m-%d', 'now');              -- Returns: current date
SELECT strftime('%H:%M:%S', 'now', 'localtime'); -- Returns: current local time
SELECT strftime('%Y-%m', 'now', 'start of month'); -- Returns: current year-month
SELECT strftime('%j', 'now');                    -- Returns: day of year
SELECT strftime('%w', 'now');                    -- Returns: day of week (0-6)
```

**Implementation:** `internal/functions/date.go`

**Format Specifiers:**
- `%Y` - Year (4 digits)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour (00-24)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)
- `%f` - Fractional seconds (SS.SSS)
- `%F` - ISO date (YYYY-MM-DD)
- `%T` - ISO time (HH:MM:SS)
- `%j` - Day of year (001-366)
- `%w` - Day of week (0-6, Sunday=0)
- `%s` - Unix timestamp
- `%J` - Julian day number

## Math Functions

Mathematical functions for numerical computations.

### abs(X)

See [Core Functions: abs(X)](#absx)

### ceil(X) / ceiling(X)

Returns the smallest integer value not less than X.

**Syntax:**
```sql
ceil(X)
ceiling(X)  -- Alias
```

**Examples:**
```sql
SELECT ceil(3.2);        -- Returns: 4
SELECT ceil(-3.2);       -- Returns: -3
SELECT ceiling(5.9);     -- Returns: 6
```

**Implementation:** `internal/functions/math.go`

### floor(X)

Returns the largest integer value not greater than X.

**Syntax:**
```sql
floor(X)
```

**Examples:**
```sql
SELECT floor(3.7);       -- Returns: 3
SELECT floor(-3.2);      -- Returns: -4
SELECT floor(5.0);       -- Returns: 5
```

**Implementation:** `internal/functions/math.go`

### round(X, [Y])

See [Core Functions: round(X, [Y])](#roundx-y)

### sign(X)

Returns -1, 0, or +1 depending on whether X is negative, zero, or positive.

**Syntax:**
```sql
sign(X)
```

**Examples:**
```sql
SELECT sign(42);         -- Returns: 1
SELECT sign(-42);        -- Returns: -1
SELECT sign(0);          -- Returns: 0
SELECT sign(NULL);       -- Returns: NULL
```

**Implementation:** `internal/functions/math.go`

### sqrt(X)

Returns the square root of X.

**Syntax:**
```sql
sqrt(X)
```

**Examples:**
```sql
SELECT sqrt(9);          -- Returns: 3.0
SELECT sqrt(2);          -- Returns: 1.4142135623730951
SELECT sqrt(0);          -- Returns: 0.0
```

**Implementation:** `internal/functions/math.go`

### power(X, Y) / pow(X, Y)

Returns X raised to the power Y.

**Syntax:**
```sql
power(X, Y)
pow(X, Y)    -- Alias
```

**Examples:**
```sql
SELECT power(2, 3);      -- Returns: 8.0
SELECT power(10, 2);     -- Returns: 100.0
SELECT pow(5, 0);        -- Returns: 1.0
```

**Implementation:** `internal/functions/math.go`

### exp(X)

Returns e (Euler's number) raised to the power X.

**Syntax:**
```sql
exp(X)
```

**Examples:**
```sql
SELECT exp(0);           -- Returns: 1.0
SELECT exp(1);           -- Returns: 2.718281828459045
SELECT exp(2);           -- Returns: 7.38905609893065
```

**Implementation:** `internal/functions/math.go`

### ln(X) / log(X)

Returns the natural logarithm of X.

**Syntax:**
```sql
ln(X)
log(X)    -- Alias
```

**Examples:**
```sql
SELECT ln(2.718281828);  -- Returns: ~1.0
SELECT ln(10);           -- Returns: 2.302585092994046
SELECT log(100);         -- Returns: 4.605170185988092
```

**Implementation:** `internal/functions/math.go`

### log10(X)

Returns the base-10 logarithm of X.

**Syntax:**
```sql
log10(X)
```

**Examples:**
```sql
SELECT log10(10);        -- Returns: 1.0
SELECT log10(100);       -- Returns: 2.0
SELECT log10(1000);      -- Returns: 3.0
```

**Implementation:** `internal/functions/math.go`

### log2(X)

Returns the base-2 logarithm of X.

**Syntax:**
```sql
log2(X)
```

**Examples:**
```sql
SELECT log2(2);          -- Returns: 1.0
SELECT log2(8);          -- Returns: 3.0
SELECT log2(1024);       -- Returns: 10.0
```

**Implementation:** `internal/functions/math.go`

### pi()

Returns the value of pi (3.14159...).

**Syntax:**
```sql
pi()
```

**Examples:**
```sql
SELECT pi();             -- Returns: 3.141592653589793
SELECT 2 * pi();         -- Returns: circumference of unit circle
```

**Implementation:** `internal/functions/math.go`

### Trigonometric Functions

**sin(X)**, **cos(X)**, **tan(X)** - Trigonometric functions (X in radians)

**Examples:**
```sql
SELECT sin(0);           -- Returns: 0.0
SELECT cos(0);           -- Returns: 1.0
SELECT tan(pi()/4);      -- Returns: 1.0
```

**asin(X)**, **acos(X)**, **atan(X)** - Inverse trigonometric functions

**Examples:**
```sql
SELECT asin(0.5);        -- Returns: 0.5235987755982989
SELECT acos(0.5);        -- Returns: 1.0471975511965976
SELECT atan(1);          -- Returns: 0.7853981633974483
```

**atan2(Y, X)** - Returns the arc tangent of Y/X in radians

**Examples:**
```sql
SELECT atan2(1, 1);      -- Returns: 0.7853981633974483
```

**Implementation:** `internal/functions/math.go`

### Hyperbolic Functions

**sinh(X)**, **cosh(X)**, **tanh(X)** - Hyperbolic functions
**asinh(X)**, **acosh(X)**, **atanh(X)** - Inverse hyperbolic functions

**Examples:**
```sql
SELECT sinh(0);          -- Returns: 0.0
SELECT cosh(0);          -- Returns: 1.0
SELECT tanh(0);          -- Returns: 0.0
```

**Implementation:** `internal/functions/math.go`

### radians(X) / degrees(X)

Convert between degrees and radians.

**Syntax:**
```sql
radians(X)  -- Convert degrees to radians
degrees(X)  -- Convert radians to degrees
```

**Examples:**
```sql
SELECT radians(180);     -- Returns: 3.141592653589793
SELECT degrees(pi());    -- Returns: 180.0
```

**Implementation:** `internal/functions/math.go`

### mod(X, Y)

Returns the remainder of X divided by Y.

**Syntax:**
```sql
mod(X, Y)
```

**Examples:**
```sql
SELECT mod(10, 3);       -- Returns: 1
SELECT mod(17, 5);       -- Returns: 2
SELECT mod(8.5, 2.5);    -- Returns: 1.0
```

**Implementation:** `internal/functions/math.go`

## String Functions

Functions for string manipulation and text processing.

### concat(X, ...)

Concatenates all non-NULL arguments into a single string.

**Syntax:**
```sql
concat(X, ...)
```

**Examples:**
```sql
SELECT concat('Hello', ' ', 'World');     -- Returns: 'Hello World'
SELECT concat('A', NULL, 'B');            -- Returns: 'AB'
SELECT concat(first_name, ' ', last_name) FROM users;
```

**Status:** PLANNED

### concat_ws(separator, X, ...)

Concatenates all non-NULL arguments (after first) with separator.

**Syntax:**
```sql
concat_ws(separator, X, ...)
```

**Examples:**
```sql
SELECT concat_ws(', ', 'Apple', 'Banana', 'Cherry');  -- Returns: 'Apple, Banana, Cherry'
SELECT concat_ws('-', '2025', '02', '28');            -- Returns: '2025-02-28'
```

**Status:** PLANNED

### format(FORMAT, ...) / printf(FORMAT, ...)

Formats arguments according to format string (like sprintf in C).

**Syntax:**
```sql
format(FORMAT, ...)
printf(FORMAT, ...)  -- Alias
```

**Examples:**
```sql
SELECT format('Hello %s', 'World');           -- Returns: 'Hello World'
SELECT format('Value: %d', 42);               -- Returns: 'Value: 42'
SELECT format('Pi: %.2f', 3.14159);           -- Returns: 'Pi: 3.14'
```

**Status:** PLANNED

**Format Specifiers:**
- `%d` - Integer
- `%f` - Float
- `%s` - String
- `%x` - Hexadecimal (lowercase)
- `%X` - Hexadecimal (uppercase)

### glob(X, Y)

Pattern matching using Unix-style glob patterns.

**Syntax:**
```sql
glob(pattern, text)
-- Equivalent to: text GLOB pattern
```

**Examples:**
```sql
SELECT glob('*hello*', 'world hello there');  -- Returns: 1
SELECT glob('test?.txt', 'test1.txt');        -- Returns: 1
SELECT glob('[0-9]*', '5files');              -- Returns: 1
```

**Status:** PLANNED

**Pattern Characters:**
- `*` - Matches any sequence of characters
- `?` - Matches any single character
- `[...]` - Matches any character in brackets
- Case-sensitive

### like(X, Y, [Z])

Pattern matching using SQL LIKE patterns.

**Syntax:**
```sql
like(pattern, text)
like(pattern, text, escape)
-- Equivalent to: text LIKE pattern [ESCAPE escape]
```

**Examples:**
```sql
SELECT like('%hello%', 'world hello there');  -- Returns: 1
SELECT like('test_', 'test1');                -- Returns: 1
SELECT like('100%', '100%', '\');             -- Returns: 1 (escaped %)
```

**Status:** PLANNED

**Pattern Characters:**
- `%` - Matches any sequence of characters
- `_` - Matches any single character
- Case-insensitive (by default)

### soundex(X)

Returns the Soundex encoding of string X.

**Syntax:**
```sql
soundex(X)
```

**Examples:**
```sql
SELECT soundex('Smith');     -- Returns: 'S530'
SELECT soundex('Smythe');    -- Returns: 'S530'
SELECT soundex('Robert');    -- Returns: 'R163'
```

**Status:** PLANNED (requires SQLITE_SOUNDEX compile option)

**Notes:**
- Useful for phonetic matching
- Returns '?000' for NULL or non-alphabetic input

### See Also

See [Core Functions](#core-functions) for additional string functions:
- length(X)
- substr(X, Y, [Z])
- upper(X) / lower(X)
- trim(X, [Y]) / ltrim(X, [Y]) / rtrim(X, [Y])
- replace(X, Y, Z)
- instr(X, Y)
- unicode(X)
- char(X1, X2, ...)

## Type Functions

Functions for type checking and conversion.

### typeof(X)

See [Core Functions: typeof(X)](#typeofx)

### coalesce(X, Y, ...)

See [Core Functions: coalesce(X, Y, ...)](#coalescex-y-)

### ifnull(X, Y)

See [Core Functions: ifnull(X, Y)](#ifnullx-y)

### nullif(X, Y)

See [Core Functions: nullif(X, Y)](#nullifx-y)

### quote(X)

See [Core Functions: quote(X)](#quotex)

### hex(X) / unhex(X)

See [Core Functions: hex(X)](#hexx) and [unhex(X)](#unhexx-y)

## JSON Functions

Functions for working with JSON data. JSON is stored as text in SQLite.

### json(X)

Validates and minifies JSON. Returns NULL if X is not valid JSON.

**Syntax:**
```sql
json(X)
```

**Examples:**
```sql
SELECT json('{"a":1,"b":2}');                    -- Returns: '{"a":1,"b":2}'
SELECT json(' { "a" : 1 , "b" : 2 } ');         -- Returns: '{"a":1,"b":2}'
SELECT json('invalid');                          -- Returns: NULL
```

**Implementation:** `internal/functions/json.go`

**Notes:**
- Removes unnecessary whitespace
- Validates JSON syntax
- Accepts JSON5 extensions (as of SQLite 3.42.0)

### json_array(X1, X2, ..., XN)

Creates a JSON array from arguments.

**Syntax:**
```sql
json_array(value1, value2, ...)
```

**Examples:**
```sql
SELECT json_array(1, 2, 3);                      -- Returns: '[1,2,3]'
SELECT json_array('a', 'b', 'c');                -- Returns: '["a","b","c"]'
SELECT json_array(1, null, 'test');              -- Returns: '[1,null,"test"]'
SELECT json_array();                             -- Returns: '[]'
```

**Implementation:** `internal/functions/json.go`

### json_array_length(X, [path])

Returns the length of a JSON array.

**Syntax:**
```sql
json_array_length(json)
json_array_length(json, path)
```

**Examples:**
```sql
SELECT json_array_length('[1,2,3,4]');                    -- Returns: 4
SELECT json_array_length('{"a":[1,2,3]}', '$.a');        -- Returns: 3
SELECT json_array_length('not-an-array');                 -- Returns: 0
```

**Implementation:** `internal/functions/json.go`

### json_extract(X, P1, P2, ...)

Extracts values from JSON using JSONPath.

**Syntax:**
```sql
json_extract(json, path1, path2, ...)
-- Also available as operators:
json -> path   -- Returns JSON
json ->> path  -- Returns SQL value
```

**Examples:**
```sql
SELECT json_extract('{"a":2,"c":[4,5]}', '$.a');          -- Returns: 2
SELECT json_extract('{"a":2,"c":[4,5]}', '$.c');          -- Returns: '[4,5]'
SELECT json_extract('{"a":2,"c":[4,5]}', '$.c[0]');       -- Returns: 4
SELECT '{"a":2,"c":[4,5]}' -> '$.c';                      -- Returns: '[4,5]'
SELECT '{"a":2,"c":[4,5]}' ->> '$.a';                     -- Returns: 2
```

**Implementation:** `internal/functions/json.go`

**JSONPath Syntax:**
- `$` - Root object
- `.key` - Object member
- `[n]` - Array element (0-indexed)
- `[#-n]` - Array element from end
- `[#]` - Array append position

### json_insert(X, P1, V1, ...) / json_replace(X, ...) / json_set(X, ...)

Modify JSON values at specified paths.

**Syntax:**
```sql
json_insert(json, path1, value1, path2, value2, ...)   -- Insert only if not exists
json_replace(json, path1, value1, path2, value2, ...)  -- Replace only if exists
json_set(json, path1, value1, path2, value2, ...)      -- Insert or replace
```

**Examples:**
```sql
-- json_insert: only adds new values
SELECT json_insert('{"a":1}', '$.b', 2);                 -- Returns: '{"a":1,"b":2}'
SELECT json_insert('{"a":1}', '$.a', 999);               -- Returns: '{"a":1}' (unchanged)

-- json_replace: only updates existing values
SELECT json_replace('{"a":1}', '$.a', 2);                -- Returns: '{"a":2}'
SELECT json_replace('{"a":1}', '$.b', 2);                -- Returns: '{"a":1}' (unchanged)

-- json_set: insert or replace
SELECT json_set('{"a":1}', '$.b', 2);                    -- Returns: '{"a":1,"b":2}'
SELECT json_set('{"a":1}', '$.a', 2);                    -- Returns: '{"a":2}'
```

**Implementation:** `internal/functions/json.go`

### json_object(K1, V1, K2, V2, ...)

Creates a JSON object from key-value pairs.

**Syntax:**
```sql
json_object(key1, value1, key2, value2, ...)
```

**Examples:**
```sql
SELECT json_object('a', 1, 'b', 2);                      -- Returns: '{"a":1,"b":2}'
SELECT json_object('name', 'Alice', 'age', 30);          -- Returns: '{"name":"Alice","age":30}'
SELECT json_object();                                     -- Returns: '{}'
```

**Implementation:** `internal/functions/json.go`

### json_patch(X, Y)

Applies RFC 7396 MergePatch algorithm.

**Syntax:**
```sql
json_patch(target, patch)
```

**Examples:**
```sql
SELECT json_patch('{"a":1,"b":2}', '{"c":3}');           -- Returns: '{"a":1,"b":2,"c":3}'
SELECT json_patch('{"a":1}', '{"a":null}');              -- Returns: '{}' (removes a)
SELECT json_patch('{"a":1}', '{"a":2}');                 -- Returns: '{"a":2}'
```

**Implementation:** `internal/functions/json.go`

### json_remove(X, P1, P2, ...)

Removes elements at specified paths.

**Syntax:**
```sql
json_remove(json, path1, path2, ...)
```

**Examples:**
```sql
SELECT json_remove('[0,1,2,3]', '$[1]');                 -- Returns: '[0,2,3]'
SELECT json_remove('{"a":1,"b":2}', '$.b');              -- Returns: '{"a":1}'
SELECT json_remove('{"a":1}', '$.b');                    -- Returns: '{"a":1}' (no change)
```

**Implementation:** `internal/functions/json.go`

### json_type(X, [path])

Returns the type of a JSON value.

**Syntax:**
```sql
json_type(json)
json_type(json, path)
```

**Examples:**
```sql
SELECT json_type('{"a":[1,2]}');                         -- Returns: 'object'
SELECT json_type('{"a":[1,2]}', '$.a');                  -- Returns: 'array'
SELECT json_type('{"a":[1,2]}', '$.a[0]');               -- Returns: 'integer'
SELECT json_type('null');                                -- Returns: 'null'
```

**Implementation:** `internal/functions/json.go`

**Return Values:** 'null', 'true', 'false', 'integer', 'real', 'text', 'array', 'object'

### json_valid(X)

Returns 1 if X is valid JSON, 0 otherwise.

**Syntax:**
```sql
json_valid(json)
```

**Examples:**
```sql
SELECT json_valid('{"a":1}');                            -- Returns: 1
SELECT json_valid('invalid');                            -- Returns: 0
SELECT json_valid(NULL);                                 -- Returns: NULL
```

**Implementation:** `internal/functions/json.go`

### json_quote(X)

Converts SQL value to JSON representation.

**Syntax:**
```sql
json_quote(value)
```

**Examples:**
```sql
SELECT json_quote('hello');                              -- Returns: '"hello"'
SELECT json_quote(123);                                  -- Returns: '123'
SELECT json_quote(NULL);                                 -- Returns: 'null'
```

**Implementation:** `internal/functions/json.go`

### JSON Aggregate Functions

**json_group_array(X)** - Aggregates values into a JSON array
**json_group_object(name, value)** - Aggregates key-value pairs into a JSON object

**Examples:**
```sql
SELECT json_group_array(name) FROM users;
-- Returns: '["Alice","Bob","Charlie"]'

SELECT json_group_object(name, score) FROM scores;
-- Returns: '{"Alice":95,"Bob":87,"Charlie":92}'
```

**Status:** PLANNED

### JSON Table-Valued Functions

**json_each(X, [path])** - Parse JSON array or object into rows
**json_tree(X, [path])** - Recursively parse JSON into rows

**Examples:**
```sql
SELECT * FROM json_each('[1,2,3]');
-- Returns rows with: key, value, type, atom, id, parent, fullkey, path

SELECT value FROM json_each('{"a":1,"b":2}');
-- Returns: 1, 2

SELECT fullkey, value FROM json_tree('{"a":{"b":{"c":1}}}');
-- Returns paths like: $.a, $.a.b, $.a.b.c with values
```

**Status:** IMPLEMENTED - Available as table-valued functions in FROM clauses

## Implementation Status

### Implemented Functions

The following functions are fully implemented in Anthony:

**Core/String Functions:**
- abs, char, coalesce, hex, ifnull, iif, instr, length, lower, ltrim
- nullif, quote, replace, round, rtrim, substr, trim, typeof
- unhex, unicode, upper, zeroblob

**Aggregate Functions:**
- count, count(*), sum, avg, min, max, group_concat, total

**Date/Time Functions:**
- date, time, datetime, julianday, unixepoch, strftime
- current_date, current_time, current_timestamp

**Math Functions:**
- abs, ceil/ceiling, floor, round, sign, sqrt, power/pow
- exp, ln/log, log10, log2, pi
- sin, cos, tan, asin, acos, atan, atan2
- sinh, cosh, tanh, asinh, acosh, atanh
- radians, degrees, mod, random, randomblob

**JSON Functions:**
- json, json_array, json_array_length, json_extract
- json_insert, json_replace, json_set, json_object
- json_patch, json_remove, json_type, json_valid, json_quote

### Planned Functions

The following standard SQLite functions are planned for future implementation:

**String Functions:**
- concat, concat_ws, format/printf
- glob, like, soundex

**JSON Functions:**
- json_group_array, json_group_object (aggregate)
- JSONB variants (jsonb, jsonb_array, etc.)

**JSON Table-Valued Functions (Implemented):**
- json_each - Parse JSON array/object into rows (FROM clause)
- json_tree - Recursively parse JSON into rows (FROM clause)

**Utility Functions:**
- changes, total_changes
- last_insert_rowid
- sqlite_version, sqlite_source_id
- sqlite_compileoption_get, sqlite_compileoption_used
- load_extension

**Window Functions:**
- row_number, rank, dense_rank, percent_rank, cume_dist
- ntile, lag, lead, first_value, last_value, nth_value

### Go Implementation Reference

All function implementations can be found in `internal/functions/`:

- `scalar.go` - Core string and type functions
- `aggregate.go` - Aggregate functions (count, sum, avg, etc.)
- `date.go` - Date and time functions
- `math.go` - Mathematical functions
- `json.go` - JSON functions
- `window.go` - Window functions (planned)
- `udf.go` - User-defined function framework

### Using Functions in Go

Anthony provides a function registry system for registering and calling SQL functions:

```go
import "github.com/your-org/anthony/internal/functions"

// Create a registry
registry := functions.NewRegistry()

// Register all built-in functions
functions.RegisterScalarFunctions(registry)
functions.RegisterAggregateFunctions(registry)
functions.RegisterDateTimeFunctions(registry)
functions.RegisterMathFunctions(registry)
functions.RegisterJSONFunctions(registry)

// Look up and call a function
fn := registry.Get("length")
result, err := fn.Call([]functions.Value{
    functions.NewTextValue("Hello"),
})
// result is 5
```

### User-Defined Functions

Anthony supports user-defined functions in Go:

```go
// Define a custom scalar function
customFunc := functions.NewScalarFunc("my_func", 1, func(args []Value) (Value, error) {
    // Implementation
    return functions.NewTextValue("result"), nil
})

// Register it
registry.Register(customFunc)

// Now available in SQL
// SELECT my_func('input') FROM table;
```

See `internal/functions/udf.go` for the complete UDF framework documentation.

## See Also

- [JSON Functions](JSON_FUNCTIONS.md) - Detailed documentation for JSON manipulation functions
- [API Reference](API.md) - Go API for using functions programmatically
- [PRAGMA Reference](PRAGMAS.md) - Database configuration and query options
- [Core Functions (local)](sqlite/CORE_FUNCTIONS.md) ([sqlite.org](https://sqlite.org/lang_corefunc.html))
- [Aggregate Functions (local)](sqlite/AGGREGATE_FUNCTIONS.md) ([sqlite.org](https://sqlite.org/lang_aggfunc.html))
- [Math Functions (local)](sqlite/LANG_MATHFUNC.md)
- [JSON Functions (local)](sqlite/JSON1.md) ([sqlite.org](https://sqlite.org/json1.html))
- [SQLite Date/Time Functions Documentation](https://sqlite.org/lang_datefunc.html)
- [Anthony SQL Reference](SQL_REFERENCE.md)
- [Anthony Architecture](ARCHITECTURE.md)

---

**Document Version:** 1.0
**Last Updated:** 2026-02-28
**Anthony Version:** Development
