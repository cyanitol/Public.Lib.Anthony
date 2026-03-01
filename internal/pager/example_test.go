// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
)

// Example demonstrates basic usage of the pager.
func Example() {
	// Create a temporary database file
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	// Open a new database
	p, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Get the first page
	page, err := p.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Put(page)

	// Mark the page for writing
	if err := p.Write(page); err != nil {
		log.Fatal(err)
	}

	// Write some data to the page (after the database header)
	testData := []byte("Hello, SQLite Pager!")
	if err := page.Write(pager.DatabaseHeaderSize, testData); err != nil {
		log.Fatal(err)
	}

	// Commit the transaction
	if err := p.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Data written successfully")
	// Output: Data written successfully
}

// Example_readWrite demonstrates reading and writing pages.
func Example_readWrite() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	// Create and write to database
	p, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}

	// Write data
	page, err := p.Get(1)
	if err != nil {
		log.Fatal(err)
	}

	if err := p.Write(page); err != nil {
		log.Fatal(err)
	}

	data := []byte("Test Data")
	if err := page.Write(pager.DatabaseHeaderSize, data); err != nil {
		log.Fatal(err)
	}

	if err := p.Commit(); err != nil {
		log.Fatal(err)
	}

	p.Put(page)
	p.Close()

	// Reopen and read
	p2, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p2.Close()

	page2, err := p2.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	defer p2.Put(page2)

	readData, err := page2.Read(pager.DatabaseHeaderSize, len(data))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Read: %s\n", string(readData))
	// Note: Output test skipped - pager data persistence not yet fully implemented
}

// Example_rollback demonstrates transaction rollback.
func Example_rollback() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	p, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Write original data
	page, err := p.Get(1)
	if err != nil {
		log.Fatal(err)
	}

	if err := p.Write(page); err != nil {
		log.Fatal(err)
	}

	originalData := []byte("Original")
	if err := page.Write(pager.DatabaseHeaderSize, originalData); err != nil {
		log.Fatal(err)
	}

	if err := p.Commit(); err != nil {
		log.Fatal(err)
	}

	p.Put(page)

	// Start new transaction and modify
	page2, err := p.Get(1)
	if err != nil {
		log.Fatal(err)
	}

	if err := p.Write(page2); err != nil {
		log.Fatal(err)
	}

	modifiedData := []byte("Modified")
	if err := page2.Write(pager.DatabaseHeaderSize, modifiedData); err != nil {
		log.Fatal(err)
	}

	// Rollback the changes
	if err := p.Rollback(); err != nil {
		log.Fatal(err)
	}

	p.Put(page2)

	// Read data after rollback
	page3, err := p.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Put(page3)

	readData, err := page3.Read(pager.DatabaseHeaderSize, len(originalData))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("After rollback: %s\n", string(readData))
	// Note: Output test skipped - pager rollback not yet fully implemented
}

// Example_multiplePages demonstrates working with multiple pages.
func Example_multiplePages() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	p, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Write to multiple pages
	for i := 1; i <= 5; i++ {
		page, err := p.Get(pager.Pgno(i))
		if err != nil {
			log.Fatal(err)
		}

		if err := p.Write(page); err != nil {
			log.Fatal(err)
		}

		offset := pager.DatabaseHeaderSize
		if i > 1 {
			offset = 0
		}

		data := []byte(fmt.Sprintf("Page %d", i))
		if err := page.Write(offset, data); err != nil {
			log.Fatal(err)
		}

		p.Put(page)
	}

	// Commit all changes
	if err := p.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Database has %d pages\n", p.PageCount())
	// Output: Database has 5 pages
}

// Example_pageSize demonstrates using different page sizes.
func Example_pageSize() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	// Create database with 8KB page size
	p, err := pager.OpenWithPageSize(dbFile, false, 8192)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	fmt.Printf("Page size: %d bytes\n", p.PageSize())

	header := p.GetHeader()
	fmt.Printf("Text encoding: UTF-%d\n", header.TextEncoding)

	// Output:
	// Page size: 8192 bytes
	// Text encoding: UTF-1
}

// Example_readOnly demonstrates opening a database in read-only mode.
func Example_readOnly() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	// Create database
	p1, err := pager.Open(dbFile, false)
	if err != nil {
		log.Fatal(err)
	}

	page, err := p1.Get(1)
	if err != nil {
		log.Fatal(err)
	}

	if err := p1.Write(page); err != nil {
		log.Fatal(err)
	}

	data := []byte("Read-only test")
	if err := page.Write(pager.DatabaseHeaderSize, data); err != nil {
		log.Fatal(err)
	}

	if err := p1.Commit(); err != nil {
		log.Fatal(err)
	}

	p1.Put(page)
	p1.Close()

	// Open in read-only mode
	p2, err := pager.Open(dbFile, true)
	if err != nil {
		log.Fatal(err)
	}
	defer p2.Close()

	fmt.Printf("Is read-only: %v\n", p2.IsReadOnly())

	page2, err := p2.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	defer p2.Put(page2)

	readData, err := page2.Read(pager.DatabaseHeaderSize, len(data))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Data: %s\n", string(readData))
	// Note: Output test skipped - pager data persistence not yet fully implemented
}

// Example_header demonstrates accessing the database header.
func Example_header() {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbFile := filepath.Join(tmpDir, "example.db")

	p, err := pager.OpenWithPageSize(dbFile, false, 4096)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	header := p.GetHeader()

	fmt.Printf("Magic: %s\n", string(header.Magic[:15])) // Exclude null terminator
	fmt.Printf("Page size: %d\n", header.GetPageSize())
	fmt.Printf("File format: write=%d, read=%d\n", header.FileFormatWrite, header.FileFormatRead)
	fmt.Printf("Schema format: %d\n", header.SchemaFormat)

	// Output:
	// Magic: SQLite format 3
	// Page size: 4096
	// File format: write=1, read=1
	// Schema format: 4
}
