# SQLite Type System - Go Implementation Guide

This document describes SQLite's unique dynamic type system and how it's implemented in Anthony, a pure Go SQLite implementation.

## Table of Contents

- [Overview](#overview)
- [Storage Classes](#storage-classes)
- [Type Affinity](#type-affinity)
- [Type Conversions](#type-conversions)
- [Column Affinity Rules](#column-affinity-rules)
- [Comparison and Sorting](#comparison-and-sorting)
- [STRICT Tables](#strict-tables)
- [Go Implementation](#go-implementation)

## Overview

SQLite uses a **dynamic type system** where the type is associated with the value itself, not its container (column). This differs from traditional SQL databases that use static typing.

**Key Principle:** Any value can be stored in any column, regardless of the column's declared type.

**Package Location:** `internal/schema`, `internal/expr`, `internal/vdbe`

### Dynamic vs Static Typing

```sql
-- SQLite allows this (dynamic typing):
CREATE TABLE flexible (
    num INTEGER,
    txt TEXT
);

INSERT INTO flexible VALUES ('hello', 42);  -- Works!
-- 'hello' stored in INTEGER column
-- 42 stored in TEXT column
```

Traditional databases would reject this, but SQLite accepts it due to type affinity (see below).

## Storage Classes

Every value in SQLite has one of five storage classes:

### NULL

Represents a missing or unknown value.

```go
const StorageClassNull = 0
```

**SQL Examples:**
```sql
INSERT INTO t VALUES (NULL);
SELECT x WHERE y IS NULL;
```

**Go Representation:**
```go
type Value struct {
    Class StorageClass
    // other fields...
}

func (v *Value) IsNull() bool {
    return v.Class == StorageClassNull
}
```

### INTEGER

Signed integer stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on magnitude.

```go
const StorageClassInteger = 1
```

**Range:** -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 (64-bit signed)

**SQL Examples:**
```sql
INSERT INTO t VALUES (42);
INSERT INTO t VALUES (-123456789);
INSERT INTO t VALUES (9223372036854775807);  -- MAX INT64
```

**Go Representation:**
```go
type IntegerValue struct {
    Class StorageClass  // StorageClassInteger
    Value int64
}

// Storage optimization on disk
func EncodeInteger(n int64) []byte {
    if n == 0 {
        return []byte{}  // Serial type 8
    } else if n == 1 {
        return []byte{}  // Serial type 9
    } else if n >= -128 && n <= 127 {
        return []byte{byte(n)}  // 1 byte
    }
    // ... more size optimizations
}
```

### REAL

64-bit IEEE 754 floating point number.

```go
const StorageClassReal = 2
```

**SQL Examples:**
```sql
INSERT INTO t VALUES (3.14159);
INSERT INTO t VALUES (1.0e10);
INSERT INTO t VALUES (-0.5);
```

**Go Representation:**
```go
type RealValue struct {
    Class StorageClass  // StorageClassReal
    Value float64
}

// Always stored as 8 bytes (IEEE 754)
func EncodeReal(f float64) []byte {
    bits := math.Float64bits(f)
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, bits)
    return buf
}
```

### TEXT

String stored using database encoding (UTF-8, UTF-16LE, or UTF-16BE).

```go
const StorageClassText = 3
```

**SQL Examples:**
```sql
INSERT INTO t VALUES ('hello');
INSERT INTO t VALUES ('Hello, 世界!');  -- Unicode
INSERT INTO t VALUES ('');  -- Empty string (different from NULL)
```

**Go Representation:**
```go
type TextValue struct {
    Class    StorageClass  // StorageClassText
    Value    string
    Encoding TextEncoding  // UTF8, UTF16LE, or UTF16BE
}

// Anthony uses UTF-8 by default
func EncodeText(s string) []byte {
    return []byte(s)  // Go strings are already UTF-8
}
```

### BLOB

Binary Large Object - raw bytes stored exactly as input.

```go
const StorageClassBlob = 4
```

**SQL Examples:**
```sql
INSERT INTO t VALUES (x'48656C6C6F');  -- "Hello" as hex
INSERT INTO t VALUES (zeroblob(1024)); -- 1KB of zeros
```

**Go Representation:**
```go
type BlobValue struct {
    Class StorageClass  // StorageClassBlob
    Value []byte
}

func EncodeBlob(data []byte) []byte {
    return data  // Stored as-is
}
```

## Type Affinity

**Type affinity** is the recommended storage class for a column. SQLite tries to convert inserted values to match the column's affinity, but doesn't enforce it.

### Five Affinities

```go
const (
    AffinityText    = "TEXT"
    AffinityNumeric = "NUMERIC"
    AffinityInteger = "INTEGER"
    AffinityReal    = "REAL"
    AffinityBlob    = "BLOB"  // Also called "NONE"
)
```

### Affinity Behavior

**TEXT Affinity:**
```sql
CREATE TABLE t (x TEXT);

INSERT INTO t VALUES (123);      -- Stored as '123' (TEXT)
INSERT INTO t VALUES (45.67);    -- Stored as '45.67' (TEXT)
INSERT INTO t VALUES ('hello');  -- Stored as 'hello' (TEXT)
INSERT INTO t VALUES (x'ABCD');  -- Stored as BLOB (not converted)
INSERT INTO t VALUES (NULL);     -- Stored as NULL
```

**NUMERIC Affinity:**
```sql
CREATE TABLE t (x NUMERIC);

INSERT INTO t VALUES ('123');    -- Stored as 123 (INTEGER)
INSERT INTO t VALUES ('45.67');  -- Stored as 45.67 (REAL)
INSERT INTO t VALUES ('3.0e5');  -- Stored as 300000 (INTEGER)
INSERT INTO t VALUES ('hello');  -- Stored as 'hello' (TEXT, can't convert)
```

**INTEGER Affinity:**
```sql
CREATE TABLE t (x INTEGER);

INSERT INTO t VALUES (123);      -- Stored as 123 (INTEGER)
INSERT INTO t VALUES (45.67);    -- Stored as 45 (INTEGER, truncated)
INSERT INTO t VALUES ('123');    -- Stored as 123 (INTEGER)
INSERT INTO t VALUES ('hello');  -- Stored as 'hello' (TEXT, can't convert)
```

**REAL Affinity:**
```sql
CREATE TABLE t (x REAL);

INSERT INTO t VALUES (123);      -- Stored as 123.0 (REAL)
INSERT INTO t VALUES (45.67);    -- Stored as 45.67 (REAL)
INSERT INTO t VALUES ('3.14');   -- Stored as 3.14 (REAL)
```

**BLOB Affinity:**
```sql
CREATE TABLE t (x BLOB);

INSERT INTO t VALUES (123);      -- Stored as 123 (INTEGER, no conversion)
INSERT INTO t VALUES ('hello');  -- Stored as 'hello' (TEXT, no conversion)
-- BLOB affinity doesn't convert anything
```

## Type Conversions

### Conversion Rules

**To INTEGER:**
1. REAL → INTEGER: Truncate fractional part
2. TEXT → INTEGER: Parse if well-formed integer literal
3. BLOB → INTEGER: Not converted
4. NULL → INTEGER: Remains NULL

**To REAL:**
1. INTEGER → REAL: Convert to float64
2. TEXT → REAL: Parse if well-formed real literal
3. BLOB → REAL: Not converted
4. NULL → REAL: Remains NULL

**To TEXT:**
1. INTEGER → TEXT: Format as decimal string
2. REAL → TEXT: Format as decimal string
3. BLOB → TEXT: Not converted (remains BLOB)
4. NULL → TEXT: Remains NULL

**To BLOB:**
1. No conversions (BLOB affinity doesn't convert)

### Go Implementation

```go
// ApplyAffinity converts a value to match column affinity
func ApplyAffinity(val *Value, affinity Affinity) *Value {
    if val.IsNull() {
        return val
    }

    switch affinity {
    case AffinityText:
        return convertToText(val)

    case AffinityNumeric:
        return convertToNumeric(val)

    case AffinityInteger:
        v := convertToNumeric(val)
        if v.Class == StorageClassReal {
            // Convert REAL to INTEGER if it's a whole number
            if float64(int64(v.RealValue)) == v.RealValue {
                return &Value{
                    Class:        StorageClassInteger,
                    IntegerValue: int64(v.RealValue),
                }
            }
        }
        return v

    case AffinityReal:
        v := convertToNumeric(val)
        if v.Class == StorageClassInteger {
            return &Value{
                Class:     StorageClassReal,
                RealValue: float64(v.IntegerValue),
            }
        }
        return v

    case AffinityBlob:
        return val  // No conversion
    }

    return val
}

func convertToText(val *Value) *Value {
    switch val.Class {
    case StorageClassInteger:
        return &Value{
            Class:     StorageClassText,
            TextValue: strconv.FormatInt(val.IntegerValue, 10),
        }
    case StorageClassReal:
        return &Value{
            Class:     StorageClassText,
            TextValue: strconv.FormatFloat(val.RealValue, 'g', -1, 64),
        }
    default:
        return val  // BLOB and NULL stay as-is
    }
}

func convertToNumeric(val *Value) *Value {
    if val.Class != StorageClassText {
        return val
    }

    text := val.TextValue

    // Try integer conversion
    if intVal, err := strconv.ParseInt(text, 10, 64); err == nil {
        return &Value{
            Class:        StorageClassInteger,
            IntegerValue: intVal,
        }
    }

    // Try real conversion
    if realVal, err := strconv.ParseFloat(text, 64); err == nil {
        // If it's a whole number, store as INTEGER
        if float64(int64(realVal)) == realVal && !strings.Contains(text, ".") {
            return &Value{
                Class:        StorageClassInteger,
                IntegerValue: int64(realVal),
            }
        }
        return &Value{
            Class:     StorageClassReal,
            RealValue: realVal,
        }
    }

    // Can't convert, keep as TEXT
    return val
}
```

## Column Affinity Rules

The affinity of a column is determined by its declared type using these rules (in order):

### Rule 1: INTEGER Affinity

If the declared type contains the string "INT", assign INTEGER affinity.

```sql
-- All get INTEGER affinity:
CREATE TABLE t (
    a INTEGER,
    b INT,
    c TINYINT,
    d BIGINT,
    e UNSIGNED BIG INT,
    f INT64
);
```

### Rule 2: TEXT Affinity

If the declared type contains "CHAR", "CLOB", or "TEXT", assign TEXT affinity.

```sql
-- All get TEXT affinity:
CREATE TABLE t (
    a TEXT,
    b VARCHAR(100),
    c CHARACTER(20),
    d NCHAR(50),
    e CLOB,
    f NVARCHAR2(255)
);
```

### Rule 3: BLOB Affinity

If the declared type contains "BLOB" or no type is specified, assign BLOB affinity.

```sql
-- All get BLOB affinity:
CREATE TABLE t (
    a BLOB,
    b,              -- No type specified
    c WHATEVER      -- Doesn't match other rules
);
```

### Rule 4: REAL Affinity

If the declared type contains "REAL", "FLOA", or "DOUB", assign REAL affinity.

```sql
-- All get REAL affinity:
CREATE TABLE t (
    a REAL,
    b FLOAT,
    c DOUBLE,
    d DOUBLE PRECISION,
    e FLOATING POINT
);
```

### Rule 5: NUMERIC Affinity

Otherwise, assign NUMERIC affinity.

```sql
-- All get NUMERIC affinity:
CREATE TABLE t (
    a NUMERIC,
    b DECIMAL(10,5),
    c BOOLEAN,
    d DATE,
    e DATETIME
);
```

### Go Implementation

```go
// DetermineAffinity determines column affinity from type name
func DetermineAffinity(typeName string) Affinity {
    upper := strings.ToUpper(typeName)

    // Rule 1: INTEGER
    if strings.Contains(upper, "INT") {
        return AffinityInteger
    }

    // Rule 2: TEXT
    if strings.Contains(upper, "CHAR") ||
       strings.Contains(upper, "CLOB") ||
       strings.Contains(upper, "TEXT") {
        return AffinityText
    }

    // Rule 3: BLOB
    if strings.Contains(upper, "BLOB") || typeName == "" {
        return AffinityBlob
    }

    // Rule 4: REAL
    if strings.Contains(upper, "REAL") ||
       strings.Contains(upper, "FLOA") ||
       strings.Contains(upper, "DOUB") {
        return AffinityReal
    }

    // Rule 5: NUMERIC (default)
    return AffinityNumeric
}
```

### Affinity Precedence Example

```sql
-- Column type "CHARINT" matches both rules 1 and 2
-- Rule 1 takes precedence → INTEGER affinity
CREATE TABLE t (x CHARINT);
```

### Common Type Names

| Declared Type | Affinity | Example Values |
|---------------|----------|----------------|
| `INTEGER`, `INT`, `BIGINT` | INTEGER | 1, -42, 9223372036854775807 |
| `TEXT`, `VARCHAR`, `CHAR` | TEXT | 'hello', '', 'UTF-8 text' |
| `REAL`, `FLOAT`, `DOUBLE` | REAL | 3.14, -0.5, 1.0e10 |
| `NUMERIC`, `DECIMAL` | NUMERIC | 123, 45.67, '3.14' |
| `BLOB`, (no type) | BLOB | x'ABCD', zeroblob(100) |
| `BOOLEAN` | NUMERIC | 0 (false), 1 (true) |
| `DATE`, `DATETIME` | NUMERIC | '2024-01-15', 1705276800 |

## Comparison and Sorting

### Sort Order

Values are compared using this precedence:

1. **NULL** - lowest value
2. **INTEGER** and **REAL** - compared numerically
3. **TEXT** - compared using collation sequence
4. **BLOB** - compared byte-by-byte

```sql
-- Sort order example
SELECT * FROM t ORDER BY x;
-- NULL values first
-- Then numbers: -1, 0, 1, 2.5, 10, 100
-- Then text: 'A', 'B', 'a', 'b' (depending on collation)
-- Then blobs: x'00', x'01', x'FF'
```

### Comparison Rules

```go
func CompareValues(a, b *Value) int {
    // NULL is less than everything
    if a.IsNull() {
        if b.IsNull() {
            return 0  // NULL == NULL
        }
        return -1  // NULL < anything
    }
    if b.IsNull() {
        return 1  // anything > NULL
    }

    // Numeric comparison (INTEGER and REAL)
    if a.IsNumeric() && b.IsNumeric() {
        return compareNumeric(a, b)
    }

    // Storage class ordering
    classOrder := map[StorageClass]int{
        StorageClassInteger: 1,
        StorageClassReal:    1,
        StorageClassText:    2,
        StorageClassBlob:    3,
    }

    if classOrder[a.Class] != classOrder[b.Class] {
        return classOrder[a.Class] - classOrder[b.Class]
    }

    // Same storage class
    switch a.Class {
    case StorageClassInteger:
        if a.IntegerValue < b.IntegerValue {
            return -1
        } else if a.IntegerValue > b.IntegerValue {
            return 1
        }
        return 0

    case StorageClassReal:
        if a.RealValue < b.RealValue {
            return -1
        } else if a.RealValue > b.RealValue {
            return 1
        }
        return 0

    case StorageClassText:
        return compareText(a.TextValue, b.TextValue, a.Collation)

    case StorageClassBlob:
        return bytes.Compare(a.BlobValue, b.BlobValue)
    }

    return 0
}

func compareNumeric(a, b *Value) int {
    aNum := toFloat64(a)
    bNum := toFloat64(b)

    if aNum < bNum {
        return -1
    } else if aNum > bNum {
        return 1
    }
    return 0
}
```

### Type Coercion in Comparisons

When comparing values of different storage classes, SQLite may convert one value:

```sql
-- INTEGER 5 compared to TEXT '10'
SELECT 5 < '10';           -- true (5 < 10, text converted to integer)

-- INTEGER 5 compared to TEXT 'hello'
SELECT 5 < 'hello';        -- true (number < text by storage class order)

-- REAL 3.14 compared to INTEGER 3
SELECT 3.14 > 3;           -- true (3.14 > 3.0, numeric comparison)
```

## STRICT Tables

Starting with SQLite 3.37.0 (and Anthony), you can create STRICT tables that enforce rigid type checking:

```sql
CREATE TABLE users (
    id     INTEGER PRIMARY KEY,
    name   TEXT NOT NULL,
    age    INTEGER,
    score  REAL
) STRICT;
```

### STRICT Mode Rules

1. Only five type names allowed: `INTEGER`, `TEXT`, `REAL`, `BLOB`, `ANY`
2. Values must match the column type (no automatic conversion)
3. `ANY` type accepts any storage class (like normal SQLite)
4. Attempts to insert wrong type raise an error

**Example:**
```sql
CREATE TABLE strict_example (
    id   INTEGER,
    name TEXT,
    data BLOB,
    misc ANY
) STRICT;

-- These work:
INSERT INTO strict_example VALUES (1, 'Alice', x'ABCD', 123);
INSERT INTO strict_example VALUES (2, 'Bob', x'1234', 'text');

-- These fail:
INSERT INTO strict_example VALUES ('three', 'Charlie', x'CAFE', NULL);
-- Error: cannot store TEXT in INTEGER column

INSERT INTO strict_example VALUES (4, 123, x'BEEF', NULL);
-- Error: cannot store INTEGER in TEXT column
```

### Go Implementation

```go
func ValidateStrictValue(val *Value, colType string, isStrict bool) error {
    if !isStrict {
        return nil  // No validation for non-strict tables
    }

    if val.IsNull() {
        return nil  // NULL is always allowed
    }

    switch strings.ToUpper(colType) {
    case "INTEGER":
        if val.Class != StorageClassInteger {
            return fmt.Errorf("cannot store %s in INTEGER column",
                val.Class)
        }

    case "TEXT":
        if val.Class != StorageClassText {
            return fmt.Errorf("cannot store %s in TEXT column",
                val.Class)
        }

    case "REAL":
        if val.Class != StorageClassReal {
            return fmt.Errorf("cannot store %s in REAL column",
                val.Class)
        }

    case "BLOB":
        if val.Class != StorageClassBlob {
            return fmt.Errorf("cannot store %s in BLOB column",
                val.Class)
        }

    case "ANY":
        return nil  // ANY accepts everything

    default:
        return fmt.Errorf("invalid type name in STRICT table: %s",
            colType)
    }

    return nil
}
```

## Boolean Values

SQLite doesn't have a native BOOLEAN type. Instead:

```sql
-- Boolean values are stored as INTEGER
CREATE TABLE t (flag BOOLEAN);

-- Keywords TRUE and FALSE are aliases for 1 and 0
INSERT INTO t VALUES (TRUE);   -- Stored as 1
INSERT INTO t VALUES (FALSE);  -- Stored as 0

-- Boolean expressions return 0 or 1
SELECT 5 > 3;                  -- Returns 1
SELECT 5 < 3;                  -- Returns 0
SELECT TRUE AND FALSE;         -- Returns 0
```

**Go Implementation:**
```go
const (
    BooleanFalse = int64(0)
    BooleanTrue  = int64(1)
)

func BoolToInt(b bool) int64 {
    if b {
        return BooleanTrue
    }
    return BooleanFalse
}

func IntToBool(i int64) bool {
    return i != 0
}
```

## Date and Time

SQLite doesn't have dedicated date/time types. Instead, store dates/times as:

```sql
-- 1. TEXT (ISO 8601 format) - most readable
INSERT INTO events VALUES ('2024-01-15 14:30:00');

-- 2. INTEGER (Unix timestamp) - most compact
INSERT INTO events VALUES (1705327800);

-- 3. REAL (Julian day number) - for calculations
INSERT INTO events VALUES (2460326.104166667);

-- Use built-in functions for conversion
SELECT datetime('now');
SELECT strftime('%Y-%m-%d', 'now');
SELECT julianday('2024-01-15');
```

**Go Implementation:**
```go
import "time"

// Store as TEXT (ISO 8601)
func FormatDateTime(t time.Time) string {
    return t.Format("2006-01-02 15:04:05")
}

// Store as INTEGER (Unix timestamp)
func FormatUnixTime(t time.Time) int64 {
    return t.Unix()
}

// Store as REAL (Julian day)
func FormatJulianDay(t time.Time) float64 {
    // Days since noon in Greenwich on November 24, 4714 B.C.
    return 2440587.5 + float64(t.Unix())/86400.0
}
```

## Performance Considerations

### Storage Efficiency

```go
// INTEGER values are variable-length encoded
// Choose smallest representation:
value := int64(42)

// On disk:
// 0-127:      1 byte (serial type 1)
// 128-32767:  2 bytes (serial type 2)
// etc.

// REAL values always use 8 bytes
realValue := float64(42.0)  // Always 8 bytes on disk

// Prefer INTEGER when possible for space savings
```

### Comparison Performance

```sql
-- Fast: Numeric comparison
SELECT * FROM t WHERE int_col = 42;

-- Slower: Text comparison with collation
SELECT * FROM t WHERE text_col = 'value';

-- Avoid forcing conversions in WHERE clauses
-- Bad: forces string to number conversion for every row
SELECT * FROM t WHERE int_col = '42';

-- Good: use proper type
SELECT * FROM t WHERE int_col = 42;
```

### Index Efficiency

```sql
-- Indexes work best with consistent types
CREATE INDEX idx ON t(col);

-- If col has mixed types (due to BLOB affinity),
-- index performance may suffer

-- Use STRICT tables for better index performance:
CREATE TABLE t (col INTEGER) STRICT;
CREATE INDEX idx ON t(col);  -- All values guaranteed to be INTEGER
```

## References

- **Package:** `internal/schema` - Affinity determination
- **Package:** `internal/expr` - Value conversions
- **Package:** `internal/vdbe` - Runtime type handling
- **SQLite Docs:** [Datatypes In SQLite](https://www.sqlite.org/datatype3.html)

## See Also

- [FILE_FORMAT.md](FILE_FORMAT.md) - Record encoding and serial types
- [SQL_LANGUAGE.md](SQL_LANGUAGE.md) - SQL syntax and semantics
- [COMPATIBILITY.md](COMPATIBILITY.md) - SQLite compatibility details
