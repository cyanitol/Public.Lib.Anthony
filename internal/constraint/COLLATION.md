# Collation Support Implementation

This document describes the collation support implementation for the Anthony SQLite clone.

## Overview

Collation sequences determine how strings are compared and sorted in SQL operations. This implementation provides SQLite-compatible collation support with the ability to register custom collations.

## Files

- `collation.go` - Core collation implementation
- `collation_test.go` - Comprehensive test suite
- `doc.go` - Package documentation with usage examples
- `example_test.go` - Extended with collation usage examples

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

## Architecture

### CollationFunc Type
```go
type CollationFunc func(a, b string) int
```
- Returns -1 if a < b
- Returns 0 if a == b
- Returns +1 if a > b

### CollationRegistry
Thread-safe registry for managing collations:
- Global registry for application-wide collations
- Support for creating isolated registries
- Built-in collations are protected from unregistration

### Integration Points

#### 1. Schema Layer
Columns can specify collations:
```sql
CREATE TABLE users (
    name TEXT COLLATE NOCASE,
    email TEXT COLLATE BINARY
);
```

Column methods:
- `Column.GetEffectiveCollation()` - Returns collation or "BINARY" default
- `Table.GetColumnCollation(index)` - Get collation by column index
- `Table.GetColumnCollationByName(name)` - Get collation by column name

#### 2. VDBE Layer
Memory cells support collation-aware comparisons:
```go
mem1 := vdbe.NewMemStr("Hello")
mem2 := vdbe.NewMemStr("hello")

// Default BINARY comparison
result := mem1.Compare(mem2)

// NOCASE comparison
result = mem1.CompareWithCollation(mem2, "NOCASE")
```

#### 3. ORDER BY Operations
Collations are used automatically when:
- Sorting results with ORDER BY
- Comparing values in WHERE clauses
- Processing DISTINCT/GROUP BY operations

## Custom Collations

### Registering Custom Collations
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

err := constraint.RegisterCollation("LENGTH", lengthCollation)
```

### Unregistering Collations
```go
err := constraint.UnregisterCollation("LENGTH")
```
Note: Built-in collations (BINARY, NOCASE, RTRIM) cannot be unregistered.

## API Reference

### Global Functions
- `RegisterCollation(name, fn)` - Register in global registry
- `GetCollation(name)` - Retrieve from global registry
- `UnregisterCollation(name)` - Remove from global registry
- `Compare(a, b, collation)` - Compare strings with collation
- `CompareBytes(a, b, collation)` - Compare byte slices
- `GetCollationFunc(name)` - Get just the function
- `DefaultCollation()` - Returns "BINARY"

### Registry Methods
- `NewCollationRegistry()` - Create isolated registry
- `Register(name, fn)` - Register collation
- `Get(name)` - Retrieve collation
- `Unregister(name)` - Remove collation
- `List()` - List all registered collations

### Schema Integration
- `Column.GetEffectiveCollation()` - Get column's collation
- `Table.GetColumnCollation(index)` - Get by index
- `Table.GetColumnCollationByName(name)` - Get by name

### VDBE Integration
- `Mem.Compare(other)` - Default comparison
- `Mem.CompareWithCollation(other, collation)` - With collation

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

## Testing

### Test Coverage
- Basic collation comparison (BINARY, NOCASE, RTRIM)
- Registry operations (register, get, unregister, list)
- Global vs local registries
- Custom collation registration and usage
- Thread-safety (concurrent access)
- Error handling (empty names, nil functions)
- Schema integration
- VDBE integration
- Byte slice comparison

### Running Tests
```bash
go test ./internal/constraint/... -v
```

### Benchmarks
```bash
go test ./internal/constraint/... -bench=.
```

## Future Enhancements

### Potential Improvements
1. **ICU Integration**: Full Unicode collation support
2. **Locale-Specific Collations**: Support for different languages
3. **Collation Parameters**: Configurable case sensitivity, accent sensitivity
4. **Collation Versioning**: Track collation versions for compatibility
5. **Performance Optimization**: SIMD for NOCASE comparisons

### SQLite Compatibility
Current implementation matches SQLite's behavior for:
- BINARY collation (exact match)
- NOCASE collation (ASCII-only case folding)
- RTRIM collation (trailing space removal)

Not yet implemented:
- Locale-aware collations (requires ICU)
- Custom collation persistence in database files
- ALTER TABLE to change column collation

## Usage Examples

See:
- `collation_test.go` - Unit tests with examples
- `example_test.go` - Runnable examples
- `doc.go` - Package documentation with code samples

## References

- SQLite Collation Documentation: https://www.sqlite.org/datatype3.html#collation
- SQLite COLLATE Clause: https://www.sqlite.org/lang_createtable.html#collateclause
- ICU Collation: https://unicode-org.github.io/icu/userguide/collation/
