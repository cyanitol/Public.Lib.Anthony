// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package btree_test

import (
	"fmt"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

// Example demonstrating basic integrity checking
func ExampleCheckIntegrity() {
	// Create a new B-tree
	bt := btree.NewBtree(4096)

	// Create a simple valid table
	rootPage, _ := bt.CreateTable()

	// Check integrity of empty table
	result := btree.CheckIntegrity(bt, rootPage)

	if result.OK() {
		fmt.Println("Empty table: OK")
	}

	fmt.Printf("Pages checked: %d\n", result.PageCount)
	fmt.Printf("Rows found: %d\n", result.RowCount)

	// Output:
	// Empty table: OK
	// Pages checked: 1
	// Rows found: 0
}

// Example demonstrating integrity error detection
func ExampleCheckIntegrity_errors() {
	bt := btree.NewBtree(4096)

	// Create a corrupted page (invalid page type)
	corruptPage := make([]byte, 4096)
	corruptPage[0] = 0xFF // Invalid page type
	bt.Pages[1] = corruptPage

	// Check integrity - should find errors
	result := btree.CheckIntegrity(bt, 1)

	if !result.OK() {
		fmt.Printf("Found %d error(s)\n", len(result.Errors))
		for _, err := range result.Errors {
			fmt.Printf("Error type: %s\n", err.ErrorType)
		}
	}

	// Output:
	// Found 1 error(s)
	// Error type: invalid_header
}

// Example demonstrating single page integrity check
func ExampleCheckPageIntegrity() {
	bt := btree.NewBtree(4096)

	// Create a valid table
	rootPage, _ := bt.CreateTable()

	// Check just the root page
	result := btree.CheckPageIntegrity(bt, rootPage)

	if result.OK() {
		fmt.Println("Page integrity: OK")
	}

	// Output:
	// Page integrity: OK
}
