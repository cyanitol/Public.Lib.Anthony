// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package format_test

import (
	"fmt"
	"log"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/format"
)

// Example demonstrates creating a new SQLite database header.
func Example() {
	// Create a new database header with 4096-byte pages
	header := format.NewHeader(4096)

	// Set custom metadata
	header.UserVersion = 1
	header.AppID = 0x12345678

	// Serialize to bytes
	data := header.Serialize()

	// Parse it back
	header2 := &format.Header{}
	if err := header2.Parse(data); err != nil {
		log.Fatal(err)
	}

	// Validate the header
	if err := header2.Validate(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Page size: %d\n", header2.GetPageSize())
	fmt.Printf("User version: %d\n", header2.UserVersion)
	fmt.Printf("App ID: 0x%08x\n", header2.AppID)
	fmt.Printf("Text encoding: %d (UTF-8)\n", header2.TextEncoding)

	// Output:
	// Page size: 4096
	// User version: 1
	// App ID: 0x12345678
	// Text encoding: 1 (UTF-8)
}

// ExampleIsValidPageSize demonstrates page size validation.
func ExampleIsValidPageSize() {
	pageSizes := []int{512, 4096, 4000, 65536, 131072}

	for _, size := range pageSizes {
		valid := format.IsValidPageSize(size)
		fmt.Printf("Page size %6d: %v\n", size, valid)
	}

	// Output:
	// Page size    512: true
	// Page size   4096: true
	// Page size   4000: false
	// Page size  65536: true
	// Page size 131072: false
}

// ExampleHeader_Parse demonstrates parsing a database header.
func ExampleHeader_Parse() {
	// Create a sample header
	original := format.NewHeader(8192)
	original.DatabaseSize = 100
	original.UserVersion = 42

	// Serialize to bytes (as would be read from a file)
	data := original.Serialize()

	// Parse the header
	header := &format.Header{}
	if err := header.Parse(data); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Page size: %d\n", header.GetPageSize())
	fmt.Printf("Database size: %d pages\n", header.DatabaseSize)
	fmt.Printf("User version: %d\n", header.UserVersion)

	// Output:
	// Page size: 8192
	// Database size: 100 pages
	// User version: 42
}

// ExampleHeader_Serialize demonstrates serializing a database header.
func ExampleHeader_Serialize() {
	// Create a new header
	header := format.NewHeader(4096)
	header.DatabaseSize = 50
	header.FileChangeCounter = 123
	header.SchemaCookie = 7

	// Serialize to bytes (ready to write to file)
	data := header.Serialize()

	fmt.Printf("Serialized header size: %d bytes\n", len(data))
	fmt.Printf("Magic string matches: %v\n", string(data[0:16]) == format.MagicString)

	// Output:
	// Serialized header size: 100 bytes
	// Magic string matches: true
}

// ExampleHeader_Validate demonstrates header validation.
func ExampleHeader_Validate() {
	// Create a valid header
	validHeader := format.NewHeader(4096)
	if err := validHeader.Validate(); err != nil {
		fmt.Printf("Valid header error: %v\n", err)
	} else {
		fmt.Println("Valid header: OK")
	}

	// Create an invalid header (bad page size)
	invalidHeader := format.NewHeader(4096)
	invalidHeader.PageSize = 4000 // Not a power of 2
	if err := invalidHeader.Validate(); err != nil {
		fmt.Println("Invalid header: error detected")
	}

	// Output:
	// Valid header: OK
	// Invalid header: error detected
}

// ExampleHeader_GetPageSize demonstrates handling the special 65536 page size encoding.
func ExampleHeader_GetPageSize() {
	// Create a header with max page size
	header := format.NewHeader(65536)

	// The PageSize field stores this as 1 (special encoding)
	fmt.Printf("Stored PageSize value: %d\n", header.PageSize)

	// GetPageSize returns the actual page size
	fmt.Printf("Actual page size: %d\n", header.GetPageSize())

	// Output:
	// Stored PageSize value: 1
	// Actual page size: 65536
}

// Example_pageTypes demonstrates page type constants.
func Example_pageTypes() {
	pageTypes := map[string]byte{
		"Interior Index": format.PageTypeInteriorIndex,
		"Interior Table": format.PageTypeInteriorTable,
		"Leaf Index":     format.PageTypeLeafIndex,
		"Leaf Table":     format.PageTypeLeafTable,
	}

	for name, value := range pageTypes {
		fmt.Printf("%-16s: 0x%02x\n", name, value)
	}

	// Unordered output:
	// Interior Index  : 0x02
	// Interior Table  : 0x05
	// Leaf Index      : 0x0a
	// Leaf Table      : 0x0d
}

// Example_textEncodings demonstrates text encoding constants.
func Example_textEncodings() {
	encodings := []struct {
		name  string
		value uint32
	}{
		{"UTF-8", format.EncodingUTF8},
		{"UTF-16 LE", format.EncodingUTF16LE},
		{"UTF-16 BE", format.EncodingUTF16BE},
	}

	for _, enc := range encodings {
		fmt.Printf("%-10s: %d\n", enc.name, enc.value)
	}

	// Output:
	// UTF-8     : 1
	// UTF-16 LE : 2
	// UTF-16 BE : 3
}
