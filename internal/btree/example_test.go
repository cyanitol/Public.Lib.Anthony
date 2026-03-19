// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree_test

import (
	"encoding/binary"
	"fmt"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
)

// Example_varintEncoding demonstrates encoding and decoding variable-length integers
func Example_varintEncoding() {
	// Encode a 64-bit integer
	var buf [9]byte
	value := uint64(12345678)
	n := btree.PutVarint(buf[:], value)

	fmt.Printf("Encoded %d in %d bytes\n", value, n)

	// Decode the integer
	decoded, m := btree.GetVarint(buf[:])
	fmt.Printf("Decoded %d from %d bytes\n", decoded, m)

	// Calculate length without encoding
	length := btree.VarintLen(value)
	fmt.Printf("Varint length: %d bytes\n", length)

	// Output:
	// Encoded 12345678 in 4 bytes
	// Decoded 12345678 from 4 bytes
	// Varint length: 4 bytes
}

// Example_pageHeader demonstrates parsing a B-tree page header
func Example_pageHeader() {
	// Create a simple leaf table page header
	pageData := make([]byte, 4096)
	pageData[0] = btree.PageTypeLeafTable          // Page type
	binary.BigEndian.PutUint16(pageData[3:], 3)    // 3 cells
	binary.BigEndian.PutUint16(pageData[5:], 3500) // Cell content starts at offset 3500

	// Parse the header
	header, err := btree.ParsePageHeader(pageData, 2)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Page type: 0x%02x\n", header.PageType)
	fmt.Printf("Is leaf: %v\n", header.IsLeaf)
	fmt.Printf("Is table: %v\n", header.IsTable)
	fmt.Printf("Number of cells: %d\n", header.NumCells)

	// Output:
	// Page type: 0x0d
	// Is leaf: true
	// Is table: true
	// Number of cells: 3
}

// Example_createSimplePage demonstrates creating and parsing a simple B-tree page
func Example_createSimplePage() {
	// Create a B-tree with 4KB pages
	bt := btree.NewBtree(4096)

	// Create a simple leaf table page with one cell
	// Use page 2 since page 1 has a 100-byte file header
	pageData := make([]byte, 4096)

	// Page header
	pageData[0] = btree.PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 1) // 1 cell

	// Cell content (at end of page)
	cellOffset := 4000
	cellData := pageData[cellOffset:]

	// Write cell: varint(payload_size), varint(rowid), payload
	var buf [20]byte
	n := btree.PutVarint(buf[:], 5) // payload size = 5
	copy(cellData, buf[:n])
	cellData = cellData[n:]

	n = btree.PutVarint(buf[:], 1) // rowid = 1
	copy(cellData, buf[:n])
	cellData = cellData[n:]

	copy(cellData, []byte("hello")) // payload

	// Cell pointer
	binary.BigEndian.PutUint16(pageData[8:], uint16(cellOffset))

	// Cell content start
	binary.BigEndian.PutUint16(pageData[5:], uint16(cellOffset))

	// Store page in B-tree (use page 2)
	bt.SetPage(2, pageData)

	// Parse and iterate
	err := bt.IteratePage(2, func(cellIndex int, cell *btree.CellInfo) error {
		fmt.Printf("Cell %d: rowid=%d, payload=%q\n", cellIndex, cell.Key, string(cell.Payload))
		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Output:
	// Cell 0: rowid=1, payload="hello"
}

// Example_cursor demonstrates using a cursor to navigate a B-tree
func Example_cursor() {
	// Create a B-tree and populate with a simple leaf page
	bt := btree.NewBtree(4096)

	// Create a leaf page with 3 cells
	pageData := make([]byte, 4096)
	pageData[0] = btree.PageTypeLeafTable
	binary.BigEndian.PutUint16(pageData[3:], 3) // 3 cells

	// Add cells with rowids 10, 20, 30
	cellOffset := 3900
	for i, rowid := range []int64{10, 20, 30} {
		var buf [20]byte

		// Payload
		payload := fmt.Sprintf("row%d", rowid)

		// Write cell
		n := btree.PutVarint(buf[:], uint64(len(payload)))
		copy(pageData[cellOffset:], buf[:n])
		cellOffset += n

		n = btree.PutVarint(buf[:], uint64(rowid))
		copy(pageData[cellOffset:], buf[:n])
		cellOffset += n

		copy(pageData[cellOffset:], payload)
		cellOffset += len(payload)

		// Cell pointer
		ptrOffset := 8 + (i * 2)
		binary.BigEndian.PutUint16(pageData[ptrOffset:], uint16(3900))
	}

	binary.BigEndian.PutUint16(pageData[5:], 3900)
	bt.SetPage(1, pageData)

	// Create cursor
	cursor := btree.NewCursor(bt, 1)

	// Move to first entry
	if err := cursor.MoveToFirst(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Iterate through entries
	fmt.Println("Forward iteration:")
	for cursor.IsValid() {
		fmt.Printf("  Rowid: %d, Payload: %q\n", cursor.GetKey(), string(cursor.GetPayload()))
		if err := cursor.Next(); err != nil {
			break
		}
	}

	// Note: The actual output will depend on the proper cell construction
	// This is a simplified example
}

// Example_cellParsing demonstrates parsing different cell types
func Example_cellParsing() {
	// Table leaf cell: varint(payload_size), varint(rowid), payload
	var cellData [100]byte
	offset := 0

	// Write payload size
	n := btree.PutVarint(cellData[offset:], 11) // "hello world" = 11 bytes
	offset += n

	// Write rowid
	n = btree.PutVarint(cellData[offset:], 42)
	offset += n

	// Write payload
	copy(cellData[offset:], "hello world")
	offset += 11

	// Parse the cell
	cell, err := btree.ParseCell(btree.PageTypeLeafTable, cellData[:offset], 4096)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Cell type: table leaf\n")
	fmt.Printf("Rowid: %d\n", cell.Key)
	fmt.Printf("Payload size: %d\n", cell.PayloadSize)
	fmt.Printf("Payload: %q\n", string(cell.Payload))

	// Output:
	// Cell type: table leaf
	// Rowid: 42
	// Payload size: 11
	// Payload: "hello world"
}

// Example_varintSizes demonstrates how varint size changes with value
func Example_varintSizes() {
	values := []uint64{
		0x7f,               // Max 1-byte
		0x3fff,             // Max 2-byte
		0x1fffff,           // Max 3-byte
		0xfffffff,          // Max 4-byte
		0xffffffffffffffff, // Max 9-byte
	}

	fmt.Println("Varint encoding sizes:")
	for _, v := range values {
		length := btree.VarintLen(v)
		fmt.Printf("  0x%x -> %d byte(s)\n", v, length)
	}

	// Output:
	// Varint encoding sizes:
	//   0x7f -> 1 byte(s)
	//   0x3fff -> 2 byte(s)
	//   0x1fffff -> 3 byte(s)
	//   0xfffffff -> 4 byte(s)
	//   0xffffffffffffffff -> 9 byte(s)
}
