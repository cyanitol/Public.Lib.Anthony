//go:build stress

// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestStressTest performs stress testing with many concurrent operations
func TestStressTest(t *testing.T) {
	db, cleanup := setupConnTestDB(t)
	defer cleanup()

	// Create test table
	_, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	const (
		numWorkers   = 10
		opsPerWorker = 100
	)

	var wg sync.WaitGroup
	var successOps int64
	var failedOps int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWorker; i++ {
				conn, err := db.Conn(context.Background())
				if err != nil {
					atomic.AddInt64(&failedOps, 1)
					continue
				}

				// Perform some operations
				stmt, err := db.Prepare("INSERT INTO test (id, value) VALUES (?, ?)")
				if err == nil {
					stmt.Close()
				}

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}

				// Close connection
				conn.Close()
			}
		}(w)
	}

	wg.Wait()

	t.Logf("Stress test completed: %d successful ops, %d failed ops", successOps, failedOps)

	// At least some operations should succeed
	if successOps == 0 {
		t.Error("No operations succeeded in stress test")
	}
}
