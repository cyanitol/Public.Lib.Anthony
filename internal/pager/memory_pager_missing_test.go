// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"testing"
)

// TestMemoryPagerBeginRead tests BeginRead on memory pager
func TestMemoryPagerBeginRead(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	err = mp.BeginRead()
	if err != nil {
		t.Errorf("BeginRead failed: %v", err)
	}

	// End read
	if err := mp.EndRead(); err != nil {
		t.Errorf("EndRead failed: %v", err)
	}
}

// TestMemoryPagerInWriteTransaction tests InWriteTransaction
func TestMemoryPagerInWriteTransaction(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	// Not in write transaction initially
	if mp.InWriteTransaction() {
		t.Error("should not be in write transaction initially")
	}

	// Begin write
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Should be in write transaction now
	if !mp.InWriteTransaction() {
		t.Error("should be in write transaction after BeginWrite")
	}

	// Commit
	if err := mp.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	// Not in write transaction after commit
	if mp.InWriteTransaction() {
		t.Error("should not be in write transaction after commit")
	}
}

// TestMemoryPagerEndRead tests EndRead
func TestMemoryPagerEndRead(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	// Begin read
	if err := mp.BeginRead(); err != nil {
		t.Fatalf("BeginRead failed: %v", err)
	}

	// End read should succeed
	if err := mp.EndRead(); err != nil {
		t.Errorf("EndRead failed: %v", err)
	}

	// End read without begin should be no-op
	if err := mp.EndRead(); err != nil {
		t.Errorf("EndRead without begin should not error: %v", err)
	}
}

// TestMemoryPagerVacuum tests Vacuum
func TestMemoryPagerVacuum(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	// Allocate and free some pages
	for i := 0; i < 10; i++ {
		if _, err := mp.AllocatePage(); err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Free half the pages
	for i := Pgno(5); i <= 10; i++ {
		if err := mp.FreePage(i); err != nil {
			t.Fatalf("FreePage failed: %v", err)
		}
	}

	if err := mp.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Vacuum should succeed (or be no-op for memory pager)
	if err := mp.Vacuum(nil); err != nil {
		t.Errorf("Vacuum failed: %v", err)
	}
}

// TestMemoryPagerRelease tests Release savepoint
func TestMemoryPagerRelease(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	// Begin write
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Create savepoint
	if err := mp.Savepoint("sp1"); err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Release savepoint
	if err := mp.Release("sp1"); err != nil {
		t.Errorf("Release failed: %v", err)
	}

	// Commit
	if err := mp.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

// TestMemoryPagerReleaseNonExistent tests releasing non-existent savepoint
func TestMemoryPagerReleaseNonExistent(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("OpenMemory failed: %v", err)
	}
	defer mp.Close()

	// Begin write
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Release non-existent savepoint should error
	if err := mp.Release("nonexistent"); err == nil {
		t.Error("expected error releasing non-existent savepoint")
	}

	// Commit
	if err := mp.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}
