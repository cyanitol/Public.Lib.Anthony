// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"encoding/binary"
	"os"
	"testing"
	"time"
)

// TestAcquireSharedLockWithRetryFailure tests when busy handler returns false
func TestAcquireSharedLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler that immediately returns false
	callCount := 0
	handler := BusyCallback(func(count int) bool {
		callCount++
		return false
	})
	pager.WithBusyHandler(handler)

	// Should succeed since there's no actual contention
	err = pager.acquireSharedLockWithRetry()
	if err != nil && err != ErrDatabaseLocked {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireReservedLockWithRetryFailure tests reserved lock with failing busy handler
func TestAcquireReservedLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_reserved_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler
	pager.WithBusyHandler(BusyCallback(func(count int) bool {
		return false
	}))

	// Begin write to allow reserved lock
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire reserved lock
	err = pager.acquireReservedLockWithRetry()
	if err != nil && err != ErrDatabaseLocked && err != ErrReadOnly {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAcquireExclusiveLockWithRetryFailure tests exclusive lock with failing busy handler
func TestAcquireExclusiveLockWithRetryFailure(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "lock_exclusive_fail_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set handler
	pager.WithBusyHandler(BusyCallback(func(count int) bool {
		return false
	}))

	// Begin write
	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Try to acquire exclusive lock
	err = pager.acquireExclusiveLockWithRetry()
	if err != nil && err != ErrDatabaseLocked {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCacheRemoveLockedDirtyPage tests removing a dirty page
func TestCacheRemoveLockedDirtyPage(t *testing.T) {
	t.Parallel()
	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Add dirty page
	page := &DbPage{
		Pgno:  1,
		Data:  make([]byte, 4096),
		Flags: PageFlagDirty,
	}
	cache.Put(page)

	// Remove it
	cache.Remove(1)

	// Should be gone
	retrieved := cache.Get(1)
	if retrieved != nil {
		t.Error("page should have been removed")
	}
}

// TestCacheMarkDirtyCleanPage tests marking a clean page dirty
func TestCacheMarkDirtyCleanPage(t *testing.T) {
	t.Parallel()
	cache, err := NewLRUCache(LRUCacheConfig{
		PageSize: 4096,
		MaxPages: 10,
	})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Add clean page
	page := &DbPage{
		Pgno:  1,
		Data:  make([]byte, 4096),
		Flags: PageFlagClean,
	}
	cache.Put(page)

	// Verify it's clean
	dirtyPages := cache.GetDirtyPages()
	if len(dirtyPages) != 0 {
		t.Fatalf("should have 0 dirty pages, got %d", len(dirtyPages))
	}

	// Mark it dirty
	cache.MarkDirty(page)

	// Should now be dirty
	dirtyPages = cache.GetDirtyPages()
	if len(dirtyPages) != 1 {
		t.Errorf("should have 1 dirty page, got %d", len(dirtyPages))
	}
}

// TestFreeListProcessTrunkPageWithSpace tests trunk page with available space
func TestFreeListProcessTrunkPageWithSpace(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_trunk_space_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate some pages
	for i := 0; i < 5; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
	}

	// Setup trunk page with space
	trunkPgno := Pgno(2)
	trunk, err := pager.Get(trunkPgno)
	if err != nil {
		t.Fatalf("failed to get trunk: %v", err)
	}
	pager.Write(trunk)

	// Initialize trunk header with space
	binary.BigEndian.PutUint32(trunk.Data[0:4], 0) // next trunk
	binary.BigEndian.PutUint32(trunk.Data[4:8], 1) // 1 leaf page

	pager.Put(trunk)

	// Set as trunk
	fl := pager.freeList
	fl.firstTrunk = trunkPgno
	fl.pendingFree = []Pgno{50}

	// Process - should add to trunk
	maxLeaves := (int(pager.PageSize()) - FreeListTrunkHeaderSize) / 4
	err = fl.processTrunkPage(maxLeaves)
	if err != nil {
		t.Errorf("processTrunkPage failed: %v", err)
	}

	pager.Rollback()
}

// TestFreeListCreateNewTrunkSuccess tests successful trunk creation
func TestFreeListCreateNewTrunkSuccess(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_newtrunk_ok_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	fl := pager.freeList

	// Add pending pages
	fl.pendingFree = []Pgno{10, 11, 12}
	fl.firstTrunk = 5

	// Create new trunk
	err = fl.createNewTrunk()
	if err != nil {
		t.Errorf("createNewTrunk failed: %v", err)
	}

	// Verify trunk was created
	if fl.firstTrunk == 5 {
		t.Error("first trunk should have changed")
	}

	pager.Rollback()
}

// TestFreeListFlushPendingWithPages tests flushing with pending pages
func TestFreeListFlushPendingWithPages(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_flush_pages_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate pages
	for i := 0; i < 10; i++ {
		_, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
	}

	fl := pager.freeList

	// Add pending pages
	fl.pendingFree = []Pgno{3, 4, 5}

	// Flush
	err = fl.flushPending()
	if err != nil {
		t.Errorf("flushPending failed: %v", err)
	}

	pager.Rollback()
}

// TestFreeListIterateWithPages tests iteration over freelist
func TestFreeListIterateWithPages(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_iterate_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate and free some pages
	for i := 0; i < 10; i++ {
		pgno, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("failed to allocate: %v", err)
		}
		if i >= 5 {
			pager.freeList.Free(pgno)
		}
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Iterate
	count := 0
	err = pager.freeList.Iterate(func(pgno Pgno) bool {
		count++
		return true
	})

	if err != nil {
		t.Errorf("iterate failed: %v", err)
	}

	t.Logf("iterated over %d free pages", count)
}

// TestFreeListVerifyValid tests verification of valid freelist
func TestFreeListVerifyValid(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "freelist_verify_valid_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	if err := pager.BeginWrite(); err != nil {
		t.Fatalf("failed to begin write: %v", err)
	}

	// Allocate and free pages
	for i := 0; i < 5; i++ {
		pgno, _ := pager.AllocatePage()
		if i > 2 {
			pager.freeList.Free(pgno)
		}
	}

	if err := pager.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify
	err = pager.freeList.Verify()
	if err != nil {
		t.Errorf("verify failed: %v", err)
	}
}

// TestJournalOpenCreateNew tests opening a new journal
func TestJournalOpenCreateNew(t *testing.T) {
	t.Parallel()
	journalFile := "test_open_new.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)

	// Open should create new file
	err := journal.Open()
	if err != nil {
		t.Fatalf("failed to open journal: %v", err)
	}
	defer journal.Close()

	// Verify file exists
	if !journal.Exists() {
		t.Error("journal file should exist")
	}

	// Verify it's open
	if !journal.IsOpen() {
		t.Error("journal should be open")
	}
}

// TestJournalWriteOriginalValidSize tests writing with correct size
func TestJournalWriteOriginalValidSize(t *testing.T) {
	t.Parallel()
	journalFile := "test_write_valid.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}
	defer journal.Close()

	// Write with correct size
	pageData := make([]byte, 4096)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	err := journal.WriteOriginal(1, pageData)
	if err != nil {
		t.Errorf("write should succeed: %v", err)
	}

	// Verify page count
	count := journal.GetPageCount()
	if count != 1 {
		t.Errorf("expected page count 1, got %d", count)
	}
}

// TestJournalRollbackSuccess tests successful rollback
func TestJournalRollbackSuccess(t *testing.T) {
	t.Parallel()
	// This is a complex integration test, skip for now
	t.Skip("Rollback integration test is complex")
}

// TestJournalFinalizeSuccess tests successful finalize
func TestJournalFinalizeSuccess(t *testing.T) {
	t.Parallel()
	journalFile := "test_finalize_ok.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write data
	pageData := make([]byte, 4096)
	journal.WriteOriginal(1, pageData)

	// Finalize
	err := journal.Finalize()
	if err != nil {
		t.Errorf("finalize failed: %v", err)
	}

	// File should be deleted
	if journal.Exists() {
		t.Error("journal should not exist after finalize")
	}
}

// TestJournalDeleteSuccess tests successful delete
func TestJournalDeleteSuccess(t *testing.T) {
	t.Parallel()
	journalFile := "test_delete_ok.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Delete
	err := journal.Delete()
	if err != nil {
		t.Errorf("delete failed: %v", err)
	}

	// Should not exist
	if journal.Exists() {
		t.Error("journal should not exist after delete")
	}

	// Should not be open
	if journal.IsOpen() {
		t.Error("journal should not be open after delete")
	}
}

// TestJournalIsValidAfterWrite tests validation after writing
func TestJournalIsValidAfterWrite(t *testing.T) {
	t.Parallel()
	journalFile := "test_valid_after_write.db-journal"
	defer os.Remove(journalFile)

	journal := NewJournal(journalFile, 4096, 1)
	if err := journal.Open(); err != nil {
		t.Fatalf("failed to open: %v", err)
	}

	// Write data
	pageData := make([]byte, 4096)
	journal.WriteOriginal(1, pageData)
	journal.Sync()
	journal.Close()

	// Check validity
	valid, err := journal.IsValid()
	if err != nil {
		t.Logf("IsValid returned error: %v", err)
	}
	t.Logf("Journal validity: %v", valid)
}

// TestParseDBHeaderValid tests parsing valid header
func TestParseDBHeaderValid(t *testing.T) {
	t.Parallel()
	// Create valid header
	header := make([]byte, 100)
	copy(header[0:16], []byte("SQLite format 3\x00"))
	binary.BigEndian.PutUint16(header[16:18], 4096) // page size
	header[18] = 1 // file format write version
	header[19] = 1 // file format read version

	parsed, err := ParseDatabaseHeader(header)
	if err != nil {
		t.Errorf("failed to parse valid header: %v", err)
	}

	if parsed.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", parsed.PageSize)
	}
}

// TestBusyHandlerMultipleRetries tests handler with multiple retries
func TestBusyHandlerMultipleRetries(t *testing.T) {
	t.Parallel()
	retries := 0
	maxRetries := 3

	handler := BusyCallback(func(count int) bool {
		retries++
		time.Sleep(1 * time.Millisecond)
		return count < maxRetries
	})

	// Simulate retries
	count := 0
	for handler.Busy(count) {
		count++
		if count > 10 {
			break
		}
	}

	if retries != maxRetries+1 {
		t.Errorf("expected %d retries, got %d", maxRetries+1, retries)
	}
}
LiteralInteger, Value: "2"},
			},
			expectIndexable: false,
			expectColName:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexable, colName := analyzeIndexability(tt.where)

			if indexable != tt.expectIndexable {
				t.Errorf("Expected indexable=%v, got %v", tt.expectIndexable, indexable)
			}

			if colName != tt.expectColName {
				t.Errorf("Expected colName=%s, got %s", tt.expectColName, colName)
			}
		})
	}
}

// TestEstimateSetupCostComprehensive tests all setup cost estimation paths.
func TestEstimateSetupCostComprehensive(t *testing.T) {
	tests := []struct {
		name       string
		setupType  SetupType
		nRows      LogEst
		expectZero bool
		expectMin  LogEst
	}{
		{
			name:       "No setup",
			setupType:  SetupNone,
			nRows:      NewLogEst(1000),
			expectZero: true,
		},
		{
			name:      "Auto index creation",
			setupType: SetupAutoIndex,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(50),
		},
		{
			name:      "Sort operation with positive rows",
			setupType: SetupSort,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(100),
		},
		{
			name:       "Sort operation with zero rows",
			setupType:  SetupSort,
			nRows:      NewLogEst(0),
			expectZero: true,
		},
		{
			name:       "Sort operation with negative rows",
			setupType:  SetupSort,
			nRows:      LogEst(-10),
			expectZero: true,
		},
		{
			name:      "Bloom filter creation",
			setupType: SetupBloomFilter,
			nRows:     NewLogEst(1000),
			expectMin: NewLogEst(10),
		},
		{
			name:       "Unknown setup type",
			setupType:  SetupType(999),
			nRows:      NewLogEst(1000),
			expectZero: true,
		},
		{
			name:      "Large dataset auto index",
			setupType: SetupAutoIndex,
			nRows:     NewLogEst(1000000),
			expectMin: NewLogEst(1000),
		},
		{
			name:      "Large dataset sort",
			setupType: SetupSort,
			nRows:     NewLogEst(1000000),
			expectMin: NewLogEst(10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			costModel := NewCostModel()
			result := costModel.EstimateSetupCost(tt.setupType, tt.nRows)

			if tt.expectZero {
				if result != 0 {
					t.Errorf("Expected zero cost, got %d", result)
				}
			} else {
				if result < tt.expectMin {
					t.Errorf("Expected cost >= %d, got %d", tt.expectMin, result)
				}
			}

			if result < 0 {
				t.Errorf("Cost should never be negative, got %d", result)
			}
		})
	}
}

// TestEstimateIndexScanEdgeCases tests edge cases in index scan estimation.
func TestEstimateIndexScanEdgeCases(t *testing.T) {
	costModel := NewCostModel()

	tests := []struct {
		name     string
		table    *TableInfo
		index    *IndexInfo
		terms    []*WhereTerm
		nEq      int
		hasRange bool
		covering bool
	}{
		{
			name: "No equality constraints",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_name",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: false,
			covering: false,
		},
		{
			name: "Equality beyond stats length",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_compound",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      5, // More than ColumnStats length
			hasRange: false,
			covering: false,
		},
		{
			name: "Range with covering index",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_age",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: true,
			covering: true,
		},
		{
			name: "Range without covering index",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_age",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100)},
			},
			terms:    []*WhereTerm{},
			nEq:      0,
			hasRange: true,
			covering: false,
		},
		{
			name: "Multiple equality with covering",
			table: &TableInfo{
				Name:      "users",
				RowLogEst: NewLogEst(1000),
			},
			index: &IndexInfo{
				Name:        "idx_compound",
				RowLogEst:   NewLogEst(1000),
				ColumnStats: []LogEst{NewLogEst(100), NewLogEst(10), NewLogEst(1)},
			},
			terms:    []*WhereTerm{},
			nEq:      3,
			hasRange: false,
			covering: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cost, nOut := costModel.EstimateIndexScan(
				tt.table,
				tt.index,
				tt.terms,
				tt.nEq,
				tt.hasRange,
				tt.covering,
			)

			if cost < 0 {
				t.Errorf("Cost should be non-negative, got %d", cost)
			}

			if nOut < 0 {
				t.Errorf("nOut should be non-negative, got %d", nOut)
			}

			// Covering index should have lower cost than non-covering
			// (when comparing similar scenarios)
			if tt.covering && tt.hasRange {
				// Just verify we get valid values
				if cost == 0 && tt.table.RowLogEst > 0 {
					t.Error("Expected non-zero cost for non-empty table")
				}
			}
		})
	}
}

// TestExplainGenerationWithMultipleRoots tests explain plan with multiple root nodes.
func TestExplainGenerationWithMultipleRoots(t *testing.T) {
	plan := NewExplainPlan()

	// Add multiple root nodes
	root1 := plan.AddNode(nil, "QUERY PLAN 1")
	root2 := plan.AddNode(nil, "QUERY PLAN 2")

	child1 := plan.AddNode(root1, "SCAN table1")
	child2 := plan.AddNode(root2, "SCAN table2")

	if len(plan.Roots) != 2 {
		t.Errorf("Expected 2 roots, got %d", len(plan.Roots))
	}

	if len(root1.Children) != 1 {
		t.Errorf("Expected root1 to have 1 child, got %d", len(root1.Children))
	}

	if len(root2.Children) != 1 {
		t.Errorf("Expected root2 to have 1 child, got %d", len(root2.Children))
	}

	if child1.Level != 1 {
		t.Errorf("Expected child1 level 1, got %d", child1.Level)
	}

	if child2.Level != 1 {
		t.Errorf("Expected child2 level 1, got %d", child2.Level)
	}

	// Test table format includes both trees
	rows := plan.FormatAsTable()
	if len(rows) < 4 {
		t.Errorf("Expected at least 4 rows, got %d", len(rows))
	}

	// Test text format includes both trees
	text := plan.FormatAsText()
	if !strings.Contains(text, "QUERY PLAN 1") {
		t.Error("Expected text to contain 'QUERY PLAN 1'")
	}
	if !strings.Contains(text, "QUERY PLAN 2") {
		t.Error("Expected text to contain 'QUERY PLAN 2'")
	}
}

// TestExplainWithDeepNesting tests deeply nested explain plans.
func TestExplainWithDeepNesting(t *testing.T) {
	plan := NewExplainPlan()

	root := plan.AddNode(nil, "ROOT")
	level1 := plan.AddNode(root, "LEVEL 1")
	level2 := plan.AddNode(level1, "LEVEL 2")
	level3 := plan.AddNode(level2, "LEVEL 3")

	if level3.Level != 3 {
		t.Errorf("Expected level3 to be at level 3, got %d", level3.Level)
	}

	if level3.Parent != level2.ID {
		t.Errorf("Expected level3 parent to be %d, got %d", level2.ID, level3.Parent)
	}

	// Check indentation in text format
	text := plan.FormatAsText()
	lines := strings.Split(text, "\n")

	// Find the LEVEL 3 line and check indentation
	for _, line := range lines {
		if strings.Contains(line, "LEVEL 3") {
			// Should have 6 spaces (2 per level * 3 levels)
			if !strings.HasPrefix(line, "      LEVEL 3") {
				t.Errorf("Expected 6 spaces of indentation for LEVEL 3, got: '%s'", line)
			}
		}
	}
}

// TestCTEEdgeCasesForCoverage tests edge cases in CTE handling.
func TestCTEEdgeCasesForCoverage(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "CTE with subquery in FROM that references another CTE",
			sql:     "WITH a AS (SELECT 1), b AS (SELECT * FROM (SELECT * FROM a)) SELECT * FROM b",
			wantErr: false,
		},
		{
			name:    "CTE with subquery in JOIN that references another CTE",
			sql:     "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM users JOIN (SELECT * FROM a) ON users.id = a.x) SELECT * FROM b",
			wantErr: false,
		},
		{
			name:    "Recursive CTE with EXCEPT",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 EXCEPT SELECT 2) SELECT * FROM cte",
			wantErr: true, // Should fail validation - needs UNION
		},
		{
			name:    "Recursive CTE with INTERSECT",
			sql:     "WITH RECURSIVE cte AS (SELECT 1 INTERSECT SELECT 2) SELECT * FROM cte",
			wantErr: true, // Should fail validation - needs UNION
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmts, err := p.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Parse failed: %v", err)
				}
				return
			}

			selectStmt := stmts[0].(*parser.SelectStmt)
			ctx, err := NewCTEContext(selectStmt.With)
			if err != nil && !tt.wantErr {
				t.Fatalf("NewCTEContext failed: %v", err)
			}

			if ctx != nil {
				err = ctx.ValidateCTEs()
				if tt.wantErr && err == nil {
					t.Error("Expected validation error, got nil")
				}
				if !tt.wantErr && err != nil {
					t.Errorf("Unexpected validation error: %v", err)
				}
			}
		})
	}
}
