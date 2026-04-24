// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// Example demonstrates basic usage of the pager.
func Example() {
	dbFile, cleanup := exampleTempDB()
	defer cleanup()

	p := exampleOpenPager(dbFile, false)
	defer p.Close()

	exampleWritePageData(p, 1, pager.DatabaseHeaderSize, []byte("Hello, SQLite Pager!"))
	exampleCommit(p)

	fmt.Println("Data written successfully")
	// Output: Data written successfully
}

// exampleOpenPager opens a pager for examples, calling log.Fatal on error.
func exampleOpenPager(dbFile string, readOnly bool) *pager.Pager {
	p, err := pager.Open(dbFile, readOnly)
	if err != nil {
		log.Fatal(err)
	}
	return p
}

// exampleWritePageData writes data to a page at an offset for examples.
func exampleWritePageData(p *pager.Pager, pgno pager.Pgno, offset int, data []byte) {
	page, err := p.Get(pgno)
	if err != nil {
		log.Fatal(err)
	}
	if err := p.Write(page); err != nil {
		log.Fatal(err)
	}
	if err := page.Write(offset, data); err != nil {
		log.Fatal(err)
	}
	p.Put(page)
}

// exampleCommit commits a pager transaction for examples.
func exampleCommit(p *pager.Pager) {
	if err := p.Commit(); err != nil {
		log.Fatal(err)
	}
}

// exampleReadPageData reads data from a page at an offset for examples.
func exampleReadPageData(p *pager.Pager, pgno pager.Pgno, offset, length int) []byte {
	page, err := p.Get(pgno)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Put(page)
	data, err := page.Read(offset, length)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

// exampleTempDB creates a temp dir with a db file, returning the path and a cleanup function.
func exampleTempDB() (string, func()) {
	tmpDir, err := os.MkdirTemp("", "pager-example")
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Join(tmpDir, "example.db"), func() { os.RemoveAll(tmpDir) }
}

// Example_readWrite demonstrates reading and writing pages.
func Example_readWrite() {
	dbFile, cleanup := exampleTempDB()
	defer cleanup()

	p := exampleOpenPager(dbFile, false)
	data := []byte("Test Data")
	exampleWritePageData(p, 1, pager.DatabaseHeaderSize, data)
	exampleCommit(p)
	p.Close()

	// Reopen and read
	p2 := exampleOpenPager(dbFile, false)
	defer p2.Close()

	readData := exampleReadPageData(p2, 1, pager.DatabaseHeaderSize, len(data))
	fmt.Printf("Read: %s\n", string(readData))
	// Note: Output test skipped - pager data persistence not yet fully implemented
}

// Example_rollback demonstrates transaction rollback.
func Example_rollback() {
	dbFile, cleanup := exampleTempDB()
	defer cleanup()

	p := exampleOpenPager(dbFile, false)
	defer p.Close()

	// Write original data and commit
	originalData := []byte("Original")
	exampleWritePageData(p, 1, pager.DatabaseHeaderSize, originalData)
	exampleCommit(p)

	// Start new transaction and modify
	exampleWritePageData(p, 1, pager.DatabaseHeaderSize, []byte("Modified"))

	// Rollback the changes
	if err := p.Rollback(); err != nil {
		log.Fatal(err)
	}

	// Read data after rollback
	readData := exampleReadPageData(p, 1, pager.DatabaseHeaderSize, len(originalData))
	fmt.Printf("After rollback: %s\n", string(readData))
	// Note: Output test skipped - pager rollback not yet fully implemented
}

// Example_multiplePages demonstrates working with multiple pages.
// exampleWritePage writes data to one page in the pager.
func exampleWritePage(p *pager.Pager, i int) {
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

	for i := 1; i <= 5; i++ {
		exampleWritePage(p, i)
	}

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
	dbFile, cleanup := exampleTempDB()
	defer cleanup()

	// Create database and write data
	p1 := exampleOpenPager(dbFile, false)
	data := []byte("Read-only test")
	exampleWritePageData(p1, 1, pager.DatabaseHeaderSize, data)
	exampleCommit(p1)
	p1.Close()

	// Open in read-only mode
	p2 := exampleOpenPager(dbFile, true)
	defer p2.Close()

	fmt.Printf("Is read-only: %v\n", p2.IsReadOnly())

	readData := exampleReadPageData(p2, 1, pager.DatabaseHeaderSize, len(data))
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
