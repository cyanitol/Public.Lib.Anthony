// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// TestBasicRTreeInsertAndQuery tests basic R-Tree insert and query operations
func TestBasicRTreeInsertAndQuery(t *testing.T) {
	t.Parallel()

	// Create R-Tree module
	module := NewRTreeModule()
	if module == nil {
		t.Fatal("NewRTreeModule returned nil")
	}

	// Create a 2D R-Tree table
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

	rtree, ok := table.(*RTree)
	if !ok {
		t.Fatal("table is not *RTree")
	}

	// Test INSERT: INSERT INTO t1 VALUES(1, 0, 10, 0, 10)
	rowid, err := rtree.Update(7, []interface{}{
		nil,       // argv[0] - old rowid (nil for INSERT)
		int64(1),  // argv[1] - new rowid
		int64(0),  // argv[2] - minX
		int64(10), // argv[3] - maxX
		int64(0),  // argv[4] - minY
		int64(10), // argv[5] - maxY
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid != 1 {
		t.Errorf("Expected rowid 1, got %d", rowid)
	}

	// Verify the entry was inserted
	if rtree.Count() != 1 {
		t.Errorf("Expected 1 entry, got %d", rtree.Count())
	}

	// Test another INSERT
	rowid, err = rtree.Update(7, []interface{}{
		nil,       // argv[0] - old rowid (nil for INSERT)
		int64(2),  // argv[1] - new rowid
		int64(5),  // argv[2] - minX
		int64(15), // argv[3] - maxX
		int64(5),  // argv[4] - minY
		int64(15), // argv[5] - maxY
	})
	if err != nil {
		t.Fatalf("Second insert failed: %v", err)
	}
	if rowid != 2 {
		t.Errorf("Expected rowid 2, got %d", rowid)
	}

	if rtree.Count() != 2 {
		t.Errorf("Expected 2 entries, got %d", rtree.Count())
	}

	// Test SELECT * FROM t1
	cursor, err := rtree.Open()
	if err != nil {
		t.Fatalf("Open cursor failed: %v", err)
	}
	defer cursor.Close()

	rtreeCursor, ok := cursor.(*RTreeCursor)
	if !ok {
		t.Fatal("cursor is not *RTreeCursor")
	}

	// Filter with no constraints (full scan)
	err = rtreeCursor.Filter(0, "", []interface{}{})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Count results
	count := 0
	for !rtreeCursor.EOF() {
		rowid, err := rtreeCursor.Rowid()
		if err != nil {
			t.Fatalf("Rowid failed: %v", err)
		}

		// Check column values
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

	if count != 2 {
		t.Errorf("Expected 2 results, got %d", count)
	}
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
	// Query box (0, 19, 0, 19) should overlap with IDs 1, 2, and 4 only
	// ID 3 at (20, 30, 20, 30) should not overlap since 19 < 20
	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 0
	queryBox.Max[0] = 19
	queryBox.Min[1] = 0
	queryBox.Max[1] = 19

	results := rtree.SearchOverlap(queryBox)
	if len(results) != 3 {
		t.Errorf("SearchOverlap expected 3 results, got %d", len(results))
		for _, r := range results {
			t.Logf("  Found ID: %d, bbox: [%.0f,%.0f] x [%.0f,%.0f]",
				r.ID, r.BBox.Min[0], r.BBox.Max[0], r.BBox.Min[1], r.BBox.Max[1])
		}
	}

	// Verify the results contain IDs 1, 2, and 4
	foundIDs := make(map[int64]bool)
	for _, r := range results {
		foundIDs[r.ID] = true
	}

	expectedIDs := []int64{1, 2, 4}
	for _, id := range expectedIDs {
		if !foundIDs[id] {
			t.Errorf("Expected to find ID %d in results", id)
		}
	}

	// ID 3 should not be in the results
	if foundIDs[3] {
		t.Error("ID 3 should not be in the results")
	}

	// Test point containment
	point := []float64{7, 7}
	pointResults := rtree.SearchContains(point)
	if len(pointResults) != 2 {
		t.Errorf("SearchContains expected 2 results, got %d", len(pointResults))
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

	// Verify entry 3 is gone
	_, exists := rtree.GetEntry(3)
	if exists {
		t.Error("Entry 3 should not exist after deletion")
	}

	// Verify other entries still exist
	for _, id := range []int64{1, 2, 4, 5} {
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
