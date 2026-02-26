# UTF Package

This package provides UTF-8 and UTF-16 encoding/decoding utilities, string collation, and varint encoding for the SQLite database engine implementation.

## Features

### UTF-8 Operations (`utf8.go`)

- **Encoding/Decoding**: Convert between runes and UTF-8 byte sequences
- **Validation**: Check and clean invalid UTF-8 sequences
- **Character Counting**: Count UTF-8 characters (not bytes)
- **Case Operations**: ASCII case folding for NOCASE collation

Key Functions:

- `AppendRune()` - Append UTF-8 encoded rune to buffer
- `EncodeRune()` - Encode rune to fixed buffer
- `DecodeRune()` - Decode UTF-8 rune with validation
- `CharCount()` - Count UTF-8 characters in string
- `ValidateUTF8()` - Check if bytes are valid UTF-8
- `FoldCase()` - Convert ASCII to lowercase

### UTF-16 Support (`utf16.go`)

- **Encoding Detection**: Detect and handle BOM (Byte Order Mark)
- **Endianness**: Support for both little-endian and big-endian UTF-16
- **Surrogate Pairs**: Proper handling of UTF-16 surrogate pairs
- **Conversion**: Convert between UTF-8 and UTF-16

Key Functions:

- `UTF16ToUTF8()` - Convert UTF-16 to UTF-8
- `UTF8ToUTF16()` - Convert UTF-8 to UTF-16
- `DetectBOM()` - Detect byte order mark
- `SwapEndian()` - Convert between LE and BE

### Collation (`collation.go`)

Implements SQLite's standard collation sequences:

- **BINARY**: Byte-by-byte comparison (case-sensitive)
- **NOCASE**: Case-insensitive for ASCII (A-Z = a-z)
- **RTRIM**: Ignores trailing spaces

Pattern Matching:

- `Like()` - SQL LIKE operator (with % and _ wildcards)
- `Glob()` - SQL GLOB operator (with * and ? wildcards, character classes)

Key Functions:

- `CompareBinary()` - Binary string comparison
- `CompareNoCase()` - Case-insensitive comparison
- `CompareRTrim()` - Compare ignoring trailing spaces
- `StrICmp()` - Case-insensitive string comparison
- `StrIHash()` - Case-insensitive hash

### Varint Encoding (`varint.go`)

SQLite's variable-length integer encoding:

- 1-9 bytes to encode 64-bit integers
- Space-efficient for small values
- Compatible with SQLite's binary format

Format:

- **7 bits**: A (0xxxxxxx)
- **14 bits**: BA (1xxxxxxx 0xxxxxxx)
- **21 bits**: BBA
- **28 bits**: BBBA
- **35 bits**: BBBBA
- **42 bits**: BBBBBA
- **49 bits**: BBBBBBA
- **56 bits**: BBBBBBBA
- **64 bits**: BBBBBBBBC (9th byte has all 8 bits)

Key Functions:

- `PutVarint()` - Encode uint64 to varint
- `GetVarint()` - Decode varint to uint64
- `GetVarint32()` - Decode varint to uint32
- `VarintLen()` - Calculate varint length
- `Put4Byte()` / `Get4Byte()` - Big-endian 32-bit integers
- `Put8Byte()` / `Get8Byte()` - Big-endian 64-bit integers

## Usage Examples

### UTF-8 Encoding

```go
import "github.com/JuniperBible/juniper/core/sqlite/internal/utf"

// Encode a rune to UTF-8
buf := make([]byte, 4)
n := utf.EncodeRune(buf, '日')
// buf[:n] = []byte{0xE6, 0x97, 0xA5}

// Decode UTF-8 to rune
r, size := utf.DecodeRune([]byte{0xE6, 0x97, 0xA5})
// r = '日', size = 3

// Count UTF-8 characters
count := utf.CharCount("Hello 世界", -1)
// count = 8 (not 12 bytes)
```

### UTF-16 Conversion

```go
// Convert UTF-8 to UTF-16 Little-Endian
utf16le := utf.UTF8ToUTF16([]byte("Hello"), utf.UTF16LE)

// Convert UTF-16 to UTF-8
utf8 := utf.UTF16ToUTF8(utf16le, utf.UTF16LE)
// utf8 = []byte("Hello")

// Detect BOM
enc, hasBOM := utf.DetectBOM([]byte{0xFF, 0xFE, 0x41, 0x00})
// enc = UTF16LE, hasBOM = true
```

### String Collation

```go
// Case-insensitive comparison
result := utf.CompareNoCase("HELLO", "hello")
// result = 0 (equal)

// Pattern matching
matched := utf.Like("h%world", "hello world", 0)
// matched = true

// Glob matching
matched = utf.Glob("*.txt", "readme.txt")
// matched = true
```

### Varint Encoding

```go
// Encode integer to varint
buf := make([]byte, 9)
n := utf.PutVarint(buf, 1000)
// n = 2 (uses 2 bytes)

// Decode varint
value, size := utf.GetVarint(buf)
// value = 1000, size = 2

// Calculate varint length
len := utf.VarintLen(0xFFFFFFFF)
// len = 5
```

## Implementation Notes

### UTF-8 Validation Rules

Following SQLite's rules for UTF-8 validation:

1. **Overlong Encodings**: Multi-byte sequences encoding ASCII values (0x00-0x7F) are replaced with 0xFFFD
2. **Surrogates**: UTF-16 surrogate values (0xD800-0xDFFF) are replaced with 0xFFFD
3. **Invalid Values**: 0xFFFE and 0xFFFF are replaced with 0xFFFD
4. **Continuation Bytes**: Bytes 0x80-0xBF at the start of a sequence are treated as single-byte characters

### NOCASE Collation

SQLite's NOCASE collation is intentionally limited to ASCII:

- Only characters A-Z are folded to a-z
- Non-ASCII characters (including accented letters) are compared byte-by-byte
- This ensures consistent behavior across all platforms

### Varint Optimization

The varint encoding is optimized for:

- **Fast path**: 1-byte and 2-byte varints use simple conditionals
- **Slow path**: Larger varints use the full algorithm
- **9-byte encoding**: Values with the high 8 bits set use a special 9-byte format

## Testing

Run tests with:

```bash
go test -v
```

Run tests with coverage:

```bash
go test -v -cover
```

Current test coverage: **84.4%**

## Benchmarks

Run benchmarks with:

```bash
go test -bench=.
```

Key performance characteristics:

- UTF-8 encoding/decoding: ~10-50 ns/op
- Varint encoding: ~5-20 ns/op depending on size
- String comparison: ~20-100 ns/op depending on length

## References

- SQLite UTF-8 Implementation: `sqlite-src-3510200/src/utf.c`
- SQLite Utility Functions: `sqlite-src-3510200/src/util.c`
- SQLite Documentation: https://www.sqlite.org/
