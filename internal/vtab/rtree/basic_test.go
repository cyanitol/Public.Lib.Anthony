// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// createTestRTree creates a 2D R-Tree table for testing, returning the RTree and asserting setup.
func createTestRTree(t *testing.T) *RTree {
	t.Helper()
	module := NewRTreeModule()
	if module == nil {
		t.Fatal("NewRTreeModule returned nil")
	}

	table, schema, err := module.Create(
		nil, "rtree", "main", "t1",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if table == nil {
		t.Fatal("Create returned nil table")
	}
	if schema == "" {
		t.Error("Create returned empty schema")
	}

	rt, ok := table.(*RTree)
	if !ok {
		t.Fatal("table is not *RTree")
	}
	return rt
}

// insertRTreeEntry inserts a row into the RTree and validates the result.
func insertRTreeEntry(t *testing.T, rt *RTree, id int64, minX, maxX, minY, maxY interface{}) {
	t.Helper()
	rowid, err := rt.Update(7, []interface{}{nil, id, minX, maxX, minY, maxY})
	if err != nil {
		t.Fatalf("Insert id=%d failed: %v", id, err)
	}
	if rowid != id {
		t.Errorf("Expected rowid %d, got %d", id, rowid)
	}
}

// TestBasicRTreeInsertAndQuery tests basic R-Tree insert and query operations
func TestBasicRTreeInsertAndQuery(t *testing.T) {
	t.Parallel()

	rt := createTestRTree(t)

	t.Run("insert_first", func(t *testing.T) {
		insertRTreeEntry(t, rt, 1, int64(0), int64(10), int64(0), int64(10))
		if rt.Count() != 1 {
			t.Errorf("Expected 1 entry, got %d", rt.Count())
		}
	})

	t.Run("insert_second", func(t *testing.T) {
		insertRTreeEntry(t, rt, 2, int64(5), int64(15), int64(5), int64(15))
		if rt.Count() != 2 {
			t.Errorf("Expected 2 entries, got %d", rt.Count())
		}
	})

	t.Run("query_full_scan", func(t *testing.T) {
		count := countRTreeFullScan(t, rt)
		if count != 2 {
			t.Errorf("Expected 2 results, got %d", count)
		}
	})
}

// countRTreeFullScan opens a cursor, filters with no constraints, and counts results.
func countRTreeFullScan(t *testing.T, rt *RTree) int {
	t.Helper()
	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open cursor failed: %v", err)
	}
	defer cursor.Close()

	rtreeCursor, ok := cursor.(*RTreeCursor)
	if !ok {
		t.Fatal("cursor is not *RTreeCursor")
	}

	err = rtreeCursor.Filter(0, "", []interface{}{})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	count := 0
	for !rtreeCursor.EOF() {
		rowid, err := rtreeCursor.Rowid()
		if err != nil {
			t.Fatalf("Rowid failed: %v", err)
		}
		id, err := rtreeCursor.Column(0)
		if err != nil {
			t.Fatalf("Column(0) failed: %v", err)
		}
		if id != rowid {
			t.Errorf("ID mismatch: expected %d, got %v", rowid, id)
		}
		count++
		rtreeCursor.Next()
	}
	return count
}

// TestRTreeOverlapQuery tests spatial overlap queries
func TestRTreeOverlapQuery(t *testing.T) {
	t.Parallel()

	// Create R-Tree
	module := NewRTreeModule()
	table, _, err := module.Create(
		nil, "rtree", "main", "t1",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Insert test data
	testData := []struct {
		id   int64
		rect [4]float64 // minX, maxX, minY, maxY
	}{
		{1, [4]float64{0, 10, 0, 10}},
		{2, [4]float64{5, 15, 5, 15}},
		{3, [4]float64{20, 30, 20, 30}},
		{4, [4]float64{-5, 5, -5, 5}},
	}

	for _, td := range testData {
		_, err := rtree.Update(7, []interface{}{
			nil,
			td.id,
			td.rect[0],
			td.rect[1],
			td.rect[2],
			td.rect[3],
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", td.id, err)
		}
	}

	// Test SearchOverlap
	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 0
	queryBox.Max[0] = 19
	queryBox.Min[1] = 0
	queryBox.Max[1] = 19

	results := rtree.SearchOverlap(queryBox)
	verifyOverlapResults(t, results, []int64{1, 2, 4}, []int64{3})

	// Test point containment
	point := []float64{7, 7}
	pointResults := rtree.SearchContains(point)
	if len(pointResults) != 2 {
		t.Errorf("SearchContains expected 2 results, got %d", len(pointResults))
	}
}

func verifyOverlapResults(t *testing.T, results []*Entry, expectedIDs []int64, excludedIDs []int64) {
	t.Helper()
	if len(results) != len(expectedIDs) {
		t.Errorf("SearchOverlap expected %d results, got %d", len(expectedIDs), len(results))
		for _, r := range results {
			t.Logf("  Found ID: %d, bbox: [%.0f,%.0f] x [%.0f,%.0f]",
				r.ID, r.BBox.Min[0], r.BBox.Max[0], r.BBox.Min[1], r.BBox.Max[1])
		}
	}
	foundIDs := make(map[int64]bool)
	for _, r := range results {
		foundIDs[r.ID] = true
	}
	for _, id := range expectedIDs {
		if !foundIDs[id] {
			t.Errorf("Expected to find ID %d in results", id)
		}
	}
	for _, id := range excludedIDs {
		if foundIDs[id] {
			t.Errorf("ID %d should not be in the results", id)
		}
	}
}

// TestRTreeBasicDelete tests basic deletion operations
func TestRTreeBasicDelete(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(
		nil, "rtree", "main", "t1",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entries
	for i := int64(1); i <= 5; i++ {
		_, err := rtree.Update(7, []interface{}{
			nil,
			i,
			float64(i * 10),
			float64(i*10 + 10),
			float64(i * 10),
			float64(i*10 + 10),
		})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	if rtree.Count() != 5 {
		t.Errorf("Expected 5 entries, got %d", rtree.Count())
	}

	// Delete entry with ID 3
	_, err = rtree.Update(1, []interface{}{int64(3)})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if rtree.Count() != 4 {
		t.Errorf("Expected 4 entries after delete, got %d", rtree.Count())
	}

	verifyDeletedEntry(t, rtree, 3, []int64{1, 2, 4, 5})
}

func verifyDeletedEntry(t *testing.T, rtree *RTree, deletedID int64, remainingIDs []int64) {
	t.Helper()
	_, exists := rtree.GetEntry(deletedID)
	if exists {
		t.Errorf("Entry %d should not exist after deletion", deletedID)
	}
	for _, id := range remainingIDs {
		_, exists := rtree.GetEntry(id)
		if !exists {
			t.Errorf("Entry %d should exist", id)
		}
	}
}

// TestRTreeBasicUpdate tests basic update operations
func TestRTreeBasicUpdate(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(
		nil, "rtree", "main", "t1",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entry
	_, err = rtree.Update(7, []interface{}{
		nil,
		int64(1),
		float64(0),
		float64(10),
		float64(0),
		float64(10),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Update the entry
	_, err = rtree.Update(7, []interface{}{
		int64(1), // old rowid
		int64(1), // new rowid (same)
		float64(5),
		float64(15),
		float64(5),
		float64(15),
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify the update
	entry, exists := rtree.GetEntry(1)
	if !exists {
		t.Fatal("Entry 1 should exist after update")
	}

	if entry.BBox.Min[0] != 5 || entry.BBox.Max[0] != 15 ||
		entry.BBox.Min[1] != 5 || entry.BBox.Max[1] != 15 {
		t.Errorf("Entry bounds not updated correctly: got (%v, %v, %v, %v)",
			entry.BBox.Min[0], entry.BBox.Max[0], entry.BBox.Min[1], entry.BBox.Max[1])
	}
}

// TestRTreeBestIndexSimple tests simple BestIndex scenarios
func TestRTreeBestIndexSimple(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(
		nil, "rtree", "main", "t1",
		[]string{"id", "minX", "maxX", "minY", "maxY"},
	)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Test with ID constraint
	info := &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 0, Op: vtab.ConstraintEQ, Usable: true},
		},
	}

	err = rtree.BestIndex(info)
	if err != nil {
		t.Fatalf("BestIndex failed: %v", err)
	}

	if info.IdxNum&1 == 0 {
		t.Error("Expected ID constraint bit to be set")
	}

	// Test with spatial constraints
	info2 := &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 1, Op: vtab.ConstraintGE, Usable: true},
			{Column: 2, Op: vtab.ConstraintLE, Usable: true},
		},
	}

	err = rtree.BestIndex(info2)
	if err != nil {
		t.Fatalf("BestIndex with spatial constraints failed: %v", err)
	}

	if info2.EstimatedCost <= 0 {
		t.Error("EstimatedCost should be > 0")
	}
}
