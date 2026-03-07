// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package utf_test

import (
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/utf"
)

// Example demonstrates UTF-8 encoding and decoding
func ExampleEncodeRune() {
	buf := make([]byte, 4)

	// Encode various runes
	n := utf.EncodeRune(buf, 'A')
	fmt.Printf("ASCII: %d bytes: %X\n", n, buf[:n])

	n = utf.EncodeRune(buf, '日')
	fmt.Printf("Japanese: %d bytes: %X\n", n, buf[:n])

	n = utf.EncodeRune(buf, '🎉')
	fmt.Printf("Emoji: %d bytes: %X\n", n, buf[:n])

	// Output:
	// ASCII: 1 bytes: 41
	// Japanese: 3 bytes: E697A5
	// Emoji: 4 bytes: F09F8E89
}

// Example demonstrates UTF-8 character counting
func ExampleCharCount() {
	// Count characters (not bytes)
	s := "Hello, 世界! 🌍"

	count := utf.CharCount(s, -1)
	fmt.Printf("Characters: %d\n", count)
	fmt.Printf("Bytes: %d\n", len(s))

	// Output:
	// Characters: 12
	// Bytes: 19
}

// Example demonstrates case-insensitive string comparison
func ExampleCompareNoCase() {
	result := utf.CompareNoCase("HELLO", "hello")
	fmt.Printf("HELLO vs hello: %d (equal)\n", result)

	result = utf.CompareNoCase("apple", "BANANA")
	fmt.Printf("apple vs BANANA: %d (less than)\n", result)

	result = utf.CompareNoCase("ZEBRA", "apple")
	fmt.Printf("ZEBRA vs apple: %d (greater than)\n", result)

	// Output:
	// HELLO vs hello: 0 (equal)
	// apple vs BANANA: -1 (less than)
	// ZEBRA vs apple: 1 (greater than)
}

// Example demonstrates SQL LIKE pattern matching
func ExampleLike() {
	// Percent wildcard matches zero or more characters
	fmt.Println(utf.Like("h%", "hello", 0))          // true
	fmt.Println(utf.Like("h%o", "hello", 0))         // true
	fmt.Println(utf.Like("hello%", "helloworld", 0)) // true

	// Underscore matches exactly one character
	fmt.Println(utf.Like("h_llo", "hello", 0)) // true
	fmt.Println(utf.Like("h_llo", "hllo", 0))  // false

	// Case insensitive by default
	fmt.Println(utf.Like("HELLO", "hello", 0)) // true

	// Output:
	// true
	// true
	// true
	// true
	// false
	// true
}

// Example demonstrates SQL GLOB pattern matching
func ExampleGlob() {
	// Star wildcard matches zero or more characters
	fmt.Println(utf.Glob("*.txt", "readme.txt"))   // true
	fmt.Println(utf.Glob("test*", "test_file.go")) // true

	// Question mark matches exactly one character
	fmt.Println(utf.Glob("test?.go", "test1.go")) // true
	fmt.Println(utf.Glob("test?.go", "test.go"))  // false

	// Character classes
	fmt.Println(utf.Glob("[abc]test", "atest")) // true
	fmt.Println(utf.Glob("[a-z]test", "xtest")) // true
	fmt.Println(utf.Glob("[^0-9]*", "hello"))   // true

	// Case sensitive (unlike LIKE)
	fmt.Println(utf.Glob("HELLO", "hello")) // false

	// Output:
	// true
	// true
	// true
	// false
	// true
	// true
	// true
	// false
}

// Example demonstrates varint encoding
func ExamplePutVarint() {
	buf := make([]byte, 9)

	// Small values use fewer bytes
	n := utf.PutVarint(buf, 100)
	fmt.Printf("100 uses %d byte(s): %X\n", n, buf[:n])

	n = utf.PutVarint(buf, 1000)
	fmt.Printf("1000 uses %d byte(s): %X\n", n, buf[:n])

	n = utf.PutVarint(buf, 1000000)
	fmt.Printf("1000000 uses %d byte(s): %X\n", n, buf[:n])

	// Output:
	// 100 uses 1 byte(s): 64
	// 1000 uses 2 byte(s): 8768
	// 1000000 uses 3 byte(s): BD8440
}

// Example demonstrates varint decoding
func ExampleGetVarint() {
	// Decode various varints
	value, size := utf.GetVarint([]byte{0x64})
	fmt.Printf("Decoded: %d from %d byte(s)\n", value, size)

	value, size = utf.GetVarint([]byte{0x87, 0x68})
	fmt.Printf("Decoded: %d from %d byte(s)\n", value, size)

	value, size = utf.GetVarint([]byte{0xBD, 0x84, 0x40})
	fmt.Printf("Decoded: %d from %d byte(s)\n", value, size)

	// Output:
	// Decoded: 100 from 1 byte(s)
	// Decoded: 1000 from 2 byte(s)
	// Decoded: 1000000 from 3 byte(s)
}

// Example demonstrates UTF-16 conversion
func ExampleUTF8ToUTF16() {
	// Convert UTF-8 to UTF-16 Little-Endian
	utf8 := []byte("Hello")
	utf16le := utf.UTF8ToUTF16(utf8, utf.UTF16LE)

	fmt.Printf("UTF-8: %s (%d bytes)\n", utf8, len(utf8))
	fmt.Printf("UTF-16LE: %d bytes\n", len(utf16le))

	// Convert back
	result := utf.UTF16ToUTF8(utf16le, utf.UTF16LE)
	fmt.Printf("Round-trip: %s\n", result)

	// Output:
	// UTF-8: Hello (5 bytes)
	// UTF-16LE: 10 bytes
	// Round-trip: Hello
}

// Example demonstrates collation usage
func ExampleCollation_Compare() {
	// BINARY collation (case-sensitive)
	binary := utf.BuiltinCollations["BINARY"]
	fmt.Println(binary.Compare("Hello", "hello")) // -1

	// NOCASE collation (case-insensitive for ASCII)
	nocase := utf.BuiltinCollations["NOCASE"]
	fmt.Println(nocase.Compare("Hello", "hello")) // 0

	// RTRIM collation (ignores trailing spaces)
	rtrim := utf.BuiltinCollations["RTRIM"]
	fmt.Println(rtrim.Compare("hello  ", "hello")) // 0

	// Output:
	// -1
	// 0
	// 0
}

// Example demonstrates hex conversion
func ExampleHexToInt() {
	fmt.Printf("'0' -> %d\n", utf.HexToInt('0'))
	fmt.Printf("'9' -> %d\n", utf.HexToInt('9'))
	fmt.Printf("'a' -> %d\n", utf.HexToInt('a'))
	fmt.Printf("'f' -> %d\n", utf.HexToInt('f'))
	fmt.Printf("'A' -> %d\n", utf.HexToInt('A'))
	fmt.Printf("'F' -> %d\n", utf.HexToInt('F'))

	// Output:
	// '0' -> 0
	// '9' -> 9
	// 'a' -> 10
	// 'f' -> 15
	// 'A' -> 10
	// 'F' -> 15
}
