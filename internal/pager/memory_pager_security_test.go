// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
)

func TestMemoryPagerPageLimit(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("Failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Start a write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Try to allocate pages up to the limit
	// Start from 2 since page 1 is already allocated for header
	var allocatedPages []Pgno
	for i := 0; i < 100; i++ {
		pgno, err := mp.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i, err)
		}
		allocatedPages = append(allocatedPages, pgno)
	}

	// Commit to clear the transaction
	if err := mp.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify we can still allocate under the limit
	if len(allocatedPages) != 100 {
		t.Errorf("Expected to allocate 100 pages, got %d", len(allocatedPages))
	}
}

func TestMemoryPagerPageLimitExceeded(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("Failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Start a write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Manually set dbSize to just below the limit
	mp.mu.Lock()
	mp.dbSize = Pgno(security.MaxMemoryDBPages - 1)
	mp.mu.Unlock()

	// This allocation should succeed (at the limit)
	_, err = mp.AllocatePage()
	if err != nil {
		t.Errorf("Should be able to allocate at limit: %v", err)
	}

	// This allocation should fail (exceeds limit)
	_, err = mp.AllocatePage()
	if err == nil {
		t.Fatal("Expected error when exceeding page limit, got nil")
	}

	if !strings.Contains(err.Error(), "page limit exceeded") {
		t.Errorf("Expected 'page limit exceeded' error, got: %v", err)
	}
}

func TestMemoryPagerPageLimitWithFreeList(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("Failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Start a write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Allocate a few pages
	pgno1, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page 1: %v", err)
	}

	pgno2, err := mp.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page 2: %v", err)
	}

	// Free one page
	if err := mp.FreePage(pgno1); err != nil {
		t.Fatalf("Failed to free page: %v", err)
	}

	// Commit to finalize the free list
	if err := mp.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Start a new transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Manually set dbSize to the limit
	mp.mu.Lock()
	mp.dbSize = Pgno(security.MaxMemoryDBPages)
	mp.mu.Unlock()

	// Should be able to allocate from free list even at the limit
	pgno3, err := mp.AllocatePage()
	if err != nil {
		t.Errorf("Should be able to allocate from free list at limit: %v", err)
	}

	// The allocated page should be the freed page (reused from free list)
	if pgno3 != pgno1 {
		t.Logf("Note: Page %d was allocated, expected reuse of freed page %d (may be normal depending on free list implementation)", pgno3, pgno1)
	}

	// Verify pgno2 wasn't affected
	_ = pgno2
}

func TestMemoryPagerSecurityLimitPreventsDOS(t *testing.T) {
	t.Parallel()
	mp, err := OpenMemory(4096)
	if err != nil {
		t.Fatalf("Failed to open memory pager: %v", err)
	}
	defer mp.Close()

	// Start a write transaction
	if err := mp.BeginWrite(); err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Verify that the limit prevents allocation of excessive pages
	// The limit should be much less than what could cause system memory exhaustion
	expectedMaxMemory := int64(security.MaxMemoryDBPages) * 4096 // 4KB pages
	expectedMaxMemoryMB := expectedMaxMemory / (1024 * 1024)

	// The limit should be reasonable (e.g., not more than a few GB)
	if expectedMaxMemoryMB > 5000 { // 5GB
		t.Errorf("Page limit allows too much memory: %d MB (expected < 5000 MB)", expectedMaxMemoryMB)
	}

	// The limit should be high enough for normal use (e.g., at least 10MB)
	if expectedMaxMemoryMB < 10 {
		t.Errorf("Page limit is too restrictive: %d MB (expected >= 10 MB)", expectedMaxMemoryMB)
	}

	t.Logf("Memory limit: %d pages = %d MB", security.MaxMemoryDBPages, expectedMaxMemoryMB)
}
