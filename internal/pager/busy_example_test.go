// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
)

// ExampleDefaultBusyHandler demonstrates using the default busy handler with exponential backoff.
func ExampleDefaultBusyHandler() {
	// Create a temporary database file
	tmpFile := "/tmp/example_busy.db"
	defer os.Remove(tmpFile)

	// Open a pager with a default busy handler (5 second timeout)
	p, err := pager.Open(tmpFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Set a busy handler with 5 second timeout
	handler := pager.NewDefaultBusyHandler(5 * time.Second)
	p.WithBusyHandler(handler)

	// Now operations will automatically retry with exponential backoff
	// if they encounter lock contention
	fmt.Println("Busy handler configured with 5 second timeout")
	// Output: Busy handler configured with 5 second timeout
}

// ExampleBusyTimeout demonstrates using a simple timeout-based busy handler.
func ExampleBusyTimeout() {
	tmpFile := "/tmp/example_timeout.db"
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Set a timeout-based busy handler (1 second)
	// This will retry every 10ms for up to 1 second
	handler := pager.BusyTimeout(1 * time.Second)
	p.WithBusyHandler(handler)

	fmt.Println("Timeout busy handler configured")
	// Output: Timeout busy handler configured
}

// ExampleBusyCallback demonstrates using a custom callback function as a busy handler.
func ExampleBusyCallback() {
	tmpFile := "/tmp/example_callback.db"
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Create a custom busy handler with callback
	maxRetries := 5
	handler := pager.BusyCallback(func(count int) bool {
		if count >= maxRetries {
			fmt.Printf("Giving up after %d retries\n", count)
			return false
		}
		fmt.Printf("Retry attempt %d\n", count+1)
		time.Sleep(50 * time.Millisecond)
		return true
	})

	p.WithBusyHandler(handler)

	// Simulate invoking the busy handler
	// In real usage, this happens automatically on lock contention
	for i := 0; i < maxRetries+1; i++ {
		if h, ok := handler.(interface{ Busy(int) bool }); ok {
			if !h.Busy(i) {
				break
			}
		}
	}

	// Output:
	// Retry attempt 1
	// Retry attempt 2
	// Retry attempt 3
	// Retry attempt 4
	// Retry attempt 5
	// Giving up after 5 retries
}

// ExamplePager_WithBusyHandler demonstrates setting and removing busy handlers.
func ExamplePager_WithBusyHandler() {
	tmpFile := "/tmp/example_with_handler.db"
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, false)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	// Initially no busy handler
	if p.GetBusyHandler() == nil {
		fmt.Println("No busy handler set initially")
	}

	// Set a busy handler
	handler := pager.NewDefaultBusyHandler(3 * time.Second)
	p.WithBusyHandler(handler)

	if p.GetBusyHandler() != nil {
		fmt.Println("Busy handler is now set")
	}

	// Remove the busy handler
	p.WithBusyHandler(nil)

	if p.GetBusyHandler() == nil {
		fmt.Println("Busy handler removed")
	}

	// Output:
	// No busy handler set initially
	// Busy handler is now set
	// Busy handler removed
}

// ExampleBusyHandler_lockContention demonstrates how busy handlers handle lock contention.
func ExampleBusyHandler_lockContention() {
	// This example shows the concept of how busy handlers work
	// In practice, lock contention happens automatically with concurrent access

	handler := pager.BusyTimeout(100 * time.Millisecond)

	// Simulate lock contention scenario
	retryCount := 0
	success := false

	for {
		// Try to acquire lock (simulated)
		locked := tryAcquireLock()

		if locked {
			success = true
			fmt.Println("Lock acquired successfully")
			break
		}

		// Lock failed, invoke busy handler
		if th, ok := handler.(interface{ Busy(int) bool }); ok {
			if !th.Busy(retryCount) {
				fmt.Println("Timeout exceeded, giving up")
				break
			}
		}

		retryCount++

		// For example purposes, succeed after a few retries
		if retryCount >= 3 {
			success = true
			fmt.Println("Lock acquired after retries")
			break
		}
	}

	if success {
		fmt.Println("Operation completed")
	}

	// Output:
	// Lock acquired after retries
	// Operation completed
}

// tryAcquireLock is a helper function for the example
func tryAcquireLock() bool {
	// Simulated lock acquisition that initially fails
	return false
}
