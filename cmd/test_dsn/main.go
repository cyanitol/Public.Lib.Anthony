// +build ignore

package main

import (
	"fmt"
	"time"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
	fmt.Println("Testing DSN Parser")
	fmt.Println("==================")

	tests := []string{
		":memory:",
		"test.db",
		"test.db?mode=ro",
		"test.db?journal_mode=wal&cache_size=10000",
		"test.db?foreign_keys=on&busy_timeout=5000&synchronous=normal",
		"test.db?journal_mode=wal&cache_size=10000&synchronous=normal&foreign_keys=on&busy_timeout=5000",
	}

	for _, dsnStr := range tests {
		fmt.Printf("\nParsing: %s\n", dsnStr)
		dsn, err := driver.ParseDSN(dsnStr)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			continue
		}

		fmt.Printf("  Filename: %s\n", dsn.Filename)
		fmt.Printf("  ReadOnly: %v\n", dsn.Config.Pager.ReadOnly)
		fmt.Printf("  JournalMode: %s\n", dsn.Config.Pager.JournalMode)
		fmt.Printf("  CacheSize: %d\n", dsn.Config.Pager.CacheSize)
		fmt.Printf("  SyncMode: %s\n", dsn.Config.Pager.SyncMode)
		fmt.Printf("  ForeignKeys: %v\n", dsn.Config.EnableForeignKeys)
		fmt.Printf("  BusyTimeout: %v\n", dsn.Config.Pager.BusyTimeout)
		fmt.Printf("  MemoryDB: %v\n", dsn.Config.Pager.MemoryDB)
	}

	fmt.Println("\n\nTesting Config Defaults")
	fmt.Println("=======================")
	config := driver.DefaultDriverConfig()
	fmt.Printf("  PageSize: %d\n", config.Pager.PageSize)
	fmt.Printf("  CacheSize: %d\n", config.Pager.CacheSize)
	fmt.Printf("  JournalMode: %s\n", config.Pager.JournalMode)
	fmt.Printf("  SyncMode: %s\n", config.Pager.SyncMode)
	fmt.Printf("  BusyTimeout: %v\n", config.Pager.BusyTimeout)
	fmt.Printf("  EnableForeignKeys: %v\n", config.EnableForeignKeys)
	fmt.Printf("  EnableTriggers: %v\n", config.EnableTriggers)

	fmt.Println("\n\nTesting Config Validation")
	fmt.Println("==========================")
	config.Pager.PageSize = 4096
	config.Pager.CacheSize = 5000
	config.Pager.BusyTimeout = 10 * time.Second
	if err := config.Validate(); err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		fmt.Println("  Valid configuration!")
	}

	fmt.Println("\nDone!")
}
