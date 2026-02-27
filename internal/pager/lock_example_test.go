package pager_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
)

// Example demonstrates basic lock acquisition and release.
func ExampleLockManager() {
	// Create a temporary file
	dir := os.TempDir()
	path := filepath.Join(dir, "example_lock.db")
	defer os.Remove(path)

	// Open file
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer f.Close()

	// Write some data to make it a valid file
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Create lock manager
	lm, err := pager.NewLockManager(f)
	if err != nil {
		fmt.Printf("Error creating lock manager: %v\n", err)
		return
	}
	defer lm.Close()

	// Acquire a shared lock (for reading)
	if err := lm.AcquireLock(pager.LockLevel(pager.LockShared)); err != nil {
		fmt.Printf("Error acquiring shared lock: %v\n", err)
		return
	}
	fmt.Printf("Acquired lock: %s\n", lm.GetLockState())

	// Upgrade to reserved lock (planning to write)
	if err := lm.AcquireLock(pager.LockLevel(pager.LockReserved)); err != nil {
		fmt.Printf("Error acquiring reserved lock: %v\n", err)
		return
	}
	fmt.Printf("Acquired lock: %s\n", lm.GetLockState())

	// Release back to shared
	if err := lm.ReleaseLock(pager.LockLevel(pager.LockShared)); err != nil {
		fmt.Printf("Error releasing to shared: %v\n", err)
		return
	}
	fmt.Printf("Released to: %s\n", lm.GetLockState())

	// Release all locks
	if err := lm.ReleaseLock(pager.LockLevel(pager.LockNone)); err != nil {
		fmt.Printf("Error releasing all locks: %v\n", err)
		return
	}
	fmt.Printf("Released to: %s\n", lm.GetLockState())

	// Output:
	// Acquired lock: SHARED
	// Acquired lock: RESERVED
	// Released to: SHARED
	// Released to: NONE
}
