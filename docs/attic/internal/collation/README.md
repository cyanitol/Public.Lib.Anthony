# Collation Package

This package implements collation sequences for string comparison in the pure Go SQLite database engine.

## Overview

Collation sequences determine how strings are compared and sorted in SQL operations. This package provides SQLite-compatible collation support with the ability to register custom collations.

## Built-in Collations

### BINARY (Default)
- Byte-by-byte comparison
- Case-sensitive
- Fastest performance
- Example: `"Hello" != "hello"`

### NOCASE
- Case-insensitive for ASCII characters (A-Z = a-z)
- Non-ASCII characters are compared byte-by-byte
- Matches SQLite's NOCASE behavior
- Example: `"Hello" == "hello"`

### RTRIM
- Ignores trailing spaces during comparison
- Uses binary comparison after trimming
- Example: `"hello  " == "hello"`

## API Reference

### CollationFunc Type

```go
type CollationFunc func(a, b string) int
```

Function that compares two strings:
- Returns -1 if a < b
- Returns 0 if a == b
- Returns +1 if a > b

### Global Functions

- `RegisterCollation(name, fn)` - Register in global registry
- `GetCollation(name)` - Retrieve from global registry
- `UnregisterCollation(name)` - Remove from global registry
- `Compare(a, b, collation)` - Compare strings with collation
- `CompareBytes(a, b, collation)` - Compare byte slices
- `GetCollationFunc(name)` - Get just the function
- `DefaultCollation()` - Returns "BINARY"

### Custom Collations

**Registering:**
```go
// Example: Sort by string length
lengthCollation := func(a, b string) int {
    if len(a) < len(b) {
        return -1
    } else if len(a) > len(b) {
        return 1
    }
    return 0
}

err := collation.RegisterCollation("LENGTH", lengthCollation)
```

**Unregistering:**
```go
err := collation.UnregisterCollation("LENGTH")
```

Note: Built-in collations (BINARY, NOCASE, RTRIM) cannot be unregistered.

## SQL Usage

```sql
-- Define collation on column
CREATE TABLE users (
    name TEXT COLLATE NOCASE,
    email TEXT COLLATE BINARY,
    notes TEXT COLLATE RTRIM
);

-- Use collation in query
SELECT * FROM users ORDER BY name COLLATE NOCASE;

-- Use collation in comparison
SELECT * FROM users WHERE name = 'Alice' COLLATE NOCASE;
```

## Integration

### With Schema Layer

```go
// Get column's collation
collation := column.GetEffectiveCollation()  // Returns "BINARY" if not specified

// Get collation by column index
collation := table.GetColumnCollation(0)

// Get collation by column name
collation := table.GetColumnCollationByName("email")
```

### With VDBE Layer

```go
mem1 := vdbe.NewMemStr("Hello")
mem2 := vdbe.NewMemStr("hello")

// Default BINARY comparison
result := mem1.Compare(mem2)

// NOCASE comparison
result = mem1.CompareWithCollation(mem2, "NOCASE")
```

## Performance Characteristics

### Built-in Collations
- **BINARY**: O(n) string comparison (fastest)
- **NOCASE**: O(n) with byte-wise case folding
- **RTRIM**: O(n) trimming + O(n) comparison

### Custom Collations
Performance depends on implementation. For best results:
- Minimize allocations
- Use byte-wise operations when possible
- Consider caching expensive computations
- Make functions thread-safe for concurrent use

## Thread Safety

The CollationRegistry uses a sync.RWMutex to ensure thread-safe concurrent access. Multiple goroutines can safely register, retrieve, and use collations.

## Testing

Run tests:
```bash
go test ./internal/collation/... -v
```

Run benchmarks:
```bash
go test ./internal/collation/... -bench=.
```

## Future Enhancements

- ICU Integration: Full Unicode collation support
- Locale-Specific Collations: Support for different languages
- Collation Parameters: Configurable case/accent sensitivity
- Collation Versioning: Track collation versions for compatibility
- Performance Optimization: SIMD for NOCASE comparisons

## References

- [SQLite Collation Documentation](https://www.sqlite.org/datatype3.html#collation)
- [SQLite COLLATE Clause](https://www.sqlite.org/lang_createtable.html#collateclause)
- [ICU Collation](https://unicode-org.github.io/icu/userguide/collation/)

## License

This implementation is part of the Anthony SQLite project and follows the project's public domain dedication.
