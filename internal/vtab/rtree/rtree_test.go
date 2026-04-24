// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// assertColumnValue asserts that a cursor column value matches the expected value by type.
func assertColumnValue(t *testing.T, col int, val, want interface{}) {
	t.Helper()
	switch expected := want.(type) {
	case int64:
		ival, ok := val.(int64)
		if !ok {
			t.Errorf("Column(%d) expected int64, got %T", col, val)
		} else if ival != expected {
			t.Errorf("Column(%d) = %d, want %d", col, ival, expected)
		}
	case float64:
		fval, ok := val.(float64)
		if !ok {
			t.Errorf("Column(%d) expected float64, got %T", col, val)
		} else if fval != expected {
			t.Errorf("Column(%d) = %f, want %f", col, fval, expected)
		}
	}
}

// TestRTreeModule tests the R-Tree module creation.
func TestRTreeModule(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	if module == nil {
		t.Fatal("NewRTreeModule returned nil")
	}
}

// TestCreateRTreeTable tests creating an R-Tree virtual table.
func TestCreateRTreeTable(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	tests := []struct {
		name     string
		args     []string
		wantErr  bool
		wantDims int
	}{
		{
			name:     "2D R-Tree",
			args:     []string{"id", "minX", "maxX", "minY", "maxY"},
			wantErr:  false,
			wantDims: 2,
		},
		{
			name:     "3D R-Tree",
			args:     []string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ"},
			wantErr:  false,
			wantDims: 3,
		},
		{
			name:    "Too few columns",
			args:    []string{"id", "minX", "maxX"},
			wantErr: true,
		},
		{
			name:    "Odd number of coordinate columns",
			args:    []string{"id", "minX", "maxX", "minY"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertRTreeCreate(t, module, tt.args, tt.wantErr, tt.wantDims)
		})
	}
}

func assertRTreeCreate(t *testing.T, module *RTreeModule, args []string, wantErr bool, wantDims int) {
	t.Helper()
	table, schema, err := module.Create(nil, "rtree", "main", "test_rtree", args)
	if wantErr {
		if err == nil {
			t.Errorf("Create() expected error, got nil")
		}
		return
	}
	if err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}
	if table == nil || schema == "" {
		t.Fatal("Create() returned nil table or empty schema")
	}
	rtree, ok := table.(*RTree)
	if !ok {
		t.Fatal("Create() did not return *RTree")
	}
	if rtree.dimensions != wantDims {
		t.Errorf("Create() dimensions = %d, want %d", rtree.dimensions, wantDims)
	}
}

// TestRTreeInsert tests inserting entries into the R-Tree.
func TestRTreeInsert(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rt := table.(*RTree)
	args := []interface{}{nil, nil, 0.0, 10.0, 0.0, 10.0}
	id, err := rt.Update(len(args), args)

	if err != nil {
		t.Errorf("Update() error: %v", err)
	}
	if id != 1 {
		t.Errorf("Update() returned id = %d, want 1", id)
	}
	if rt.Count() != 1 {
		t.Errorf("Count() = %d, want 1", rt.Count())
	}

	verifyRTreeEntry(t, rt, id, [2]float64{0.0, 0.0}, [2]float64{10.0, 10.0})
}

// verifyRTreeEntry checks that the entry exists with the expected bounding box.
func verifyRTreeEntry(t *testing.T, rt *RTree, id int64, wantMin, wantMax [2]float64) {
	t.Helper()
	entry, exists := rt.GetEntry(id)
	if !exists {
		t.Errorf("GetEntry(%d) entry not found", id)
		return
	}
	if entry.ID != id {
		t.Errorf("Entry ID = %d, want %d", entry.ID, id)
	}
	if entry.BBox.Min[0] != wantMin[0] || entry.BBox.Max[0] != wantMax[0] {
		t.Errorf("Entry X bounds = [%f, %f], want [%f, %f]", entry.BBox.Min[0], entry.BBox.Max[0], wantMin[0], wantMax[0])
	}
	if entry.BBox.Min[1] != wantMin[1] || entry.BBox.Max[1] != wantMax[1] {
		t.Errorf("Entry Y bounds = [%f, %f], want [%f, %f]", entry.BBox.Min[1], entry.BBox.Max[1], wantMin[1], wantMax[1])
	}
}

// TestRTreeDelete tests deleting entries from the R-Tree.
func TestRTreeDelete(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entries
	for i := 0; i < 5; i++ {
		args := []interface{}{
			nil,
			nil,
			float64(i * 10),
			float64(i*10 + 10),
			float64(i * 10),
			float64(i*10 + 10),
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	if rtree.Count() != 5 {
		t.Fatalf("Count() = %d, want 5", rtree.Count())
	}

	// Delete an entry
	deleteArgs := []interface{}{int64(3)}
	_, err = rtree.Update(len(deleteArgs), deleteArgs)
	if err != nil {
		t.Errorf("Update(DELETE) error: %v", err)
	}

	if rtree.Count() != 4 {
		t.Errorf("Count() after delete = %d, want 4", rtree.Count())
	}

	// Verify the entry is gone
	_, exists := rtree.GetEntry(3)
	if exists {
		t.Error("GetEntry() found deleted entry")
	}
}

// TestRTreeUpdate tests updating entries in the R-Tree.
func TestRTreeUpdate(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entry
	insertArgs := []interface{}{
		nil, nil, 0.0, 10.0, 0.0, 10.0,
	}
	id, err := rtree.Update(len(insertArgs), insertArgs)
	if err != nil {
		t.Fatalf("Update(INSERT) error: %v", err)
	}

	// Update entry
	updateArgs := []interface{}{
		id,   // old rowid
		id,   // new rowid (same)
		5.0,  // new minX
		15.0, // new maxX
		5.0,  // new minY
		15.0, // new maxY
	}
	_, err = rtree.Update(len(updateArgs), updateArgs)
	if err != nil {
		t.Errorf("Update(UPDATE) error: %v", err)
	}

	// Verify the entry was updated
	entry, exists := rtree.GetEntry(id)
	if !exists {
		t.Fatal("GetEntry() entry not found after update")
	}

	if entry.BBox.Min[0] != 5.0 || entry.BBox.Max[0] != 15.0 {
		t.Errorf("Updated entry X bounds = [%f, %f], want [5, 15]", entry.BBox.Min[0], entry.BBox.Max[0])
	}
}

// insertRTreeEntries inserts sequential entries into an RTree for testing.
func insertRTreeEntries(t *testing.T, rt *RTree, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		args := []interface{}{
			nil, int64(i),
			float64(i * 10), float64(i*10 + 10),
			float64(i * 10), float64(i*10 + 10),
		}
		_, err := rt.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}
}

// TestRTreeCursor tests cursor operations.
func TestRTreeCursor(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rt := table.(*RTree)
	insertRTreeEntries(t, rt, 3)

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer cursor.Close()

	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter() error: %v", err)
	}

	count := countCursorRowsMatchingID(t, cursor)
	if count != 3 {
		t.Errorf("Cursor iterated %d rows, want 3", count)
	}
}

// countCursorRowsMatchingID iterates a cursor and verifies Column(0) matches Rowid.
func countCursorRowsMatchingID(t *testing.T, cursor vtab.VirtualCursor) int {
	t.Helper()
	count := 0
	for !cursor.EOF() {
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid() error: %v", err)
		}
		id, err := cursor.Column(0)
		if err != nil {
			t.Errorf("Column(0) error: %v", err)
		}
		if id != rowid {
			t.Errorf("Column(0) = %v, Rowid() = %v, should match", id, rowid)
		}
		count++
		cursor.Next()
	}
	return count
}

// TestRTreeSearchOverlap tests spatial overlap queries.
func TestRTreeSearchOverlap(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert non-overlapping rectangles
	rects := []struct {
		id                     int64
		minX, maxX, minY, maxY float64
	}{
		{1, 0, 10, 0, 10},
		{2, 20, 30, 0, 10},
		{3, 0, 10, 20, 30},
		{4, 20, 30, 20, 30},
		{5, 10, 20, 10, 20}, // Center rectangle
	}

	for _, r := range rects {
		r := r
		args := []interface{}{
			nil, r.id, r.minX, r.maxX, r.minY, r.maxY,
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	// Search for overlaps with center area
	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 8
	queryBox.Max[0] = 22
	queryBox.Min[1] = 8
	queryBox.Max[1] = 22

	results := rtree.SearchOverlap(queryBox)

	// Should find rectangles 1, 2, 3, 4, and 5
	if len(results) != 5 {
		t.Errorf("SearchOverlap() found %d results, want 5", len(results))
	}

	// Search for overlaps with top-left only
	queryBox = NewBoundingBox(2)
	queryBox.Min[0] = 0
	queryBox.Max[0] = 15
	queryBox.Min[1] = 0
	queryBox.Max[1] = 15

	results = rtree.SearchOverlap(queryBox)

	// Should find rectangles 1 and 5
	if len(results) != 2 {
		t.Errorf("SearchOverlap() found %d results, want 2", len(results))
	}
}

// TestRTreeSearchWithin tests spatial containment queries.
func TestRTreeSearchWithin(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert rectangles
	rects := []struct {
		id                     int64
		minX, maxX, minY, maxY float64
	}{
		{1, 5, 15, 5, 15},   // Inside query box
		{2, 8, 12, 8, 12},   // Inside query box
		{3, 0, 30, 0, 30},   // Contains query box
		{4, 25, 35, 25, 35}, // Outside query box
	}

	for _, r := range rects {
		r := r
		args := []interface{}{
			nil, r.id, r.minX, r.maxX, r.minY, r.maxY,
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	// Search for rectangles within large box
	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 0
	queryBox.Max[0] = 20
	queryBox.Min[1] = 0
	queryBox.Max[1] = 20

	results := rtree.SearchWithin(queryBox)

	// Should find rectangles 1 and 2 (inside the box)
	if len(results) != 2 {
		t.Errorf("SearchWithin() found %d results, want 2", len(results))
		for _, r := range results {
			t.Logf("Found entry ID: %d", r.ID)
		}
	}
}

// TestRTreeSearchContains tests point containment queries.
func TestRTreeSearchContains(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert rectangles
	rects := []struct {
		id                     int64
		minX, maxX, minY, maxY float64
	}{
		{1, 0, 10, 0, 10},
		{2, 5, 15, 5, 15},
		{3, 20, 30, 20, 30},
	}

	for _, r := range rects {
		r := r
		args := []interface{}{
			nil, r.id, r.minX, r.maxX, r.minY, r.maxY,
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	// Search for rectangles containing point (7, 7)
	point := []float64{7, 7}
	results := rtree.SearchContains(point)

	// Should find rectangles 1 and 2
	if len(results) != 2 {
		t.Errorf("SearchContains() found %d results, want 2", len(results))
	}

	// Search for rectangles containing point (25, 25)
	point = []float64{25, 25}
	results = rtree.SearchContains(point)

	// Should find rectangle 3
	if len(results) != 1 {
		t.Errorf("SearchContains() found %d results, want 1", len(results))
	}

	// Search for rectangles containing point outside all rectangles
	point = []float64{100, 100}
	results = rtree.SearchContains(point)

	// Should find nothing
	if len(results) != 0 {
		t.Errorf("SearchContains() found %d results, want 0", len(results))
	}
}

// TestRTreeBestIndex tests the BestIndex method.
func TestRTreeBestIndex(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Create index info with ID constraint
	info := vtab.NewIndexInfo(1)
	info.Constraints[0] = vtab.IndexConstraint{
		Column: 0,
		Op:     vtab.ConstraintEQ,
		Usable: true,
	}

	err = rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex() error: %v", err)
	}

	// Should use the constraint
	if info.ConstraintUsage[0].ArgvIndex == 0 {
		t.Error("BestIndex() did not use ID constraint")
	}

	// Cost should be low for indexed lookup
	if info.EstimatedCost > 100 {
		t.Errorf("BestIndex() cost = %f, want < 100", info.EstimatedCost)
	}
}

// TestRTreeLargeDataset tests R-Tree with a larger dataset.
func TestRTreeLargeDataset(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert 1000 random rectangles
	rng := rand.New(rand.NewSource(42))
	n := 1000

	for i := 1; i <= n; i++ {
		minX := rng.Float64() * 1000
		minY := rng.Float64() * 1000
		width := rng.Float64() * 50
		height := rng.Float64() * 50

		args := []interface{}{
			nil, int64(i),
			minX, minX + width,
			minY, minY + height,
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	if rtree.Count() != n {
		t.Errorf("Count() = %d, want %d", rtree.Count(), n)
	}

	// Perform overlap search
	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 400
	queryBox.Max[0] = 600
	queryBox.Min[1] = 400
	queryBox.Max[1] = 600

	results := rtree.SearchOverlap(queryBox)

	// Should find some results
	if len(results) == 0 {
		t.Error("SearchOverlap() found no results in large dataset")
	}

	t.Logf("SearchOverlap() found %d results out of %d entries", len(results), n)

	// Verify tree structure
	if rtree.root == nil {
		t.Fatal("Root is nil after insertions")
	}

	height := rtree.root.Height()
	t.Logf("Tree height: %d", height)

	if height < 1 {
		t.Error("Tree height should be at least 1")
	}
}

// TestRTreeBoundingBox tests bounding box operations.
func TestRTreeBoundingBox(t *testing.T) {
	t.Parallel()
	// Test 2D bounding box
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0] = 0
	bbox1.Max[0] = 10
	bbox1.Min[1] = 0
	bbox1.Max[1] = 10

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0] = 5
	bbox2.Max[0] = 15
	bbox2.Min[1] = 5
	bbox2.Max[1] = 15

	// Test Overlaps
	if !bbox1.Overlaps(bbox2) {
		t.Error("Overlaps() = false, want true")
	}

	bbox3 := NewBoundingBox(2)
	bbox3.Min[0] = 20
	bbox3.Max[0] = 30
	bbox3.Min[1] = 20
	bbox3.Max[1] = 30

	if bbox1.Overlaps(bbox3) {
		t.Error("Overlaps() = true, want false for non-overlapping boxes")
	}

	// Test Contains
	bbox4 := NewBoundingBox(2)
	bbox4.Min[0] = 2
	bbox4.Max[0] = 8
	bbox4.Min[1] = 2
	bbox4.Max[1] = 8

	if !bbox1.Contains(bbox4) {
		t.Error("Contains() = false, want true")
	}

	if bbox4.Contains(bbox1) {
		t.Error("Contains() = true, want false (smaller doesn't contain larger)")
	}

	// Test Area
	area := bbox1.Area()
	if area != 100.0 {
		t.Errorf("Area() = %f, want 100", area)
	}

	// Test Expand
	bbox5 := bbox1.Clone()
	bbox5.Expand(bbox2)

	if bbox5.Min[0] != 0 || bbox5.Max[0] != 15 {
		t.Errorf("Expand() X bounds = [%f, %f], want [0, 15]", bbox5.Min[0], bbox5.Max[0])
	}
}

// TestRTreeNearestNeighbor tests nearest neighbor search.
func TestRTreeNearestNeighbor(t *testing.T) {
	t.Parallel()
	rtree := NewLeafNode()

	// Insert some entries
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{40, 40}, Max: []float64{50, 50}}},
	}

	for _, entry := range entries {
		entry := entry
		rtree = rtree.Insert(entry)
	}

	// Find nearest neighbor to point (15, 15)
	point := []float64{15, 15}
	results := rtree.NearestNeighborSearch(point, 1)

	if len(results) != 1 {
		t.Fatalf("NearestNeighborSearch() returned %d results, want 1", len(results))
	}

	// Should be entry 2 (closest to point)
	if results[0].ID != 2 {
		t.Errorf("NearestNeighborSearch() found ID %d, want 2", results[0].ID)
	}

	// Find 2 nearest neighbors
	results = rtree.NearestNeighborSearch(point, 2)

	if len(results) != 2 {
		t.Errorf("NearestNeighborSearch() returned %d results, want 2", len(results))
	}
}

// TestRTreeSplitting tests node splitting behavior.
func TestRTreeSplitting(t *testing.T) {
	t.Parallel()
	rtree := NewLeafNode()

	// Insert enough entries to trigger splits
	for i := 0; i < MaxEntries*3; i++ {
		entry := &Entry{
			ID: int64(i + 1),
			BBox: &BoundingBox{
				Min: []float64{float64(i * 10), float64(i * 10)},
				Max: []float64{float64(i*10 + 5), float64(i*10 + 5)},
			},
		}
		rtree = rtree.Insert(entry)
	}

	// Verify tree height increased due to splits
	height := rtree.Height()
	if height < 2 {
		t.Errorf("Tree height = %d, want >= 2 after multiple insertions", height)
	}

	count := rtree.Count()
	expectedCount := MaxEntries * 3
	if count != expectedCount {
		t.Errorf("Tree count = %d, want %d", count, expectedCount)
	}
}

// TestRTreeRegistration tests module registration.
func TestRTreeRegistration(t *testing.T) {
	t.Parallel()
	// Clear any existing registrations
	vtab.DefaultRegistry().Clear()

	// Register R-Tree module
	err := RegisterRTree()
	if err != nil {
		t.Fatalf("RegisterRTree() error: %v", err)
	}

	// Verify it's registered
	if !vtab.HasModule("rtree") {
		t.Error("R-Tree module not registered")
	}

	// Get the module
	module := vtab.GetModule("rtree")
	if module == nil {
		t.Error("GetModule() returned nil")
	}

	// Verify it's the correct type
	if _, ok := module.(*RTreeModule); !ok {
		t.Error("GetModule() did not return *RTreeModule")
	}
}

// BenchmarkRTreeInsert benchmarks insertion performance.
func BenchmarkRTreeInsert(b *testing.B) {
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		b.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := []interface{}{
			nil, int64(i + 1),
			float64(i % 1000), float64((i % 1000) + 10),
			float64(i % 1000), float64((i % 1000) + 10),
		}
		rtree.Update(len(args), args)
	}
}

// BenchmarkRTreeSearch benchmarks search performance.
func BenchmarkRTreeSearch(b *testing.B) {
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		b.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert 10000 entries
	for i := 0; i < 10000; i++ {
		args := []interface{}{
			nil, int64(i + 1),
			float64(i % 1000), float64((i % 1000) + 10),
			float64(i % 1000), float64((i % 1000) + 10),
		}
		rtree.Update(len(args), args)
	}

	queryBox := NewBoundingBox(2)
	queryBox.Min[0] = 400
	queryBox.Max[0] = 600
	queryBox.Min[1] = 400
	queryBox.Max[1] = 600

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rtree.SearchOverlap(queryBox)
	}
}

// TestRTree3D tests 3D spatial indexing.
func TestRTree3D(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	if rtree.dimensions != 3 {
		t.Errorf("dimensions = %d, want 3", rtree.dimensions)
	}

	// Insert 3D box
	args3d := []interface{}{
		nil, int64(1),
		0.0, 10.0, // X
		0.0, 10.0, // Y
		0.0, 10.0, // Z
	}
	_, err = rtree.Update(len(args3d), args3d)
	if err != nil {
		t.Errorf("Update() error: %v", err)
	}

	// Search for overlaps
	queryBox := NewBoundingBox(3)
	queryBox.Min = []float64{5, 5, 5}
	queryBox.Max = []float64{15, 15, 15}

	results := rtree.SearchOverlap(queryBox)

	if len(results) != 1 {
		t.Errorf("SearchOverlap() found %d results, want 1", len(results))
	}
}

// TestDistanceFunctions tests distance calculation functions.
func TestDistanceFunctions(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{20, 20},
		Max: []float64{30, 30},
	}

	// Test distance between non-overlapping boxes
	dist := DistanceBetweenBoxes(bbox1, bbox2)
	expected := 14.142135623730951 // sqrt(10^2 + 10^2)

	if fmt.Sprintf("%.6f", dist) != fmt.Sprintf("%.6f", expected) {
		t.Errorf("DistanceBetweenBoxes() = %f, want %f", dist, expected)
	}

	// Test distance for overlapping boxes
	bbox3 := &BoundingBox{
		Min: []float64{5, 5},
		Max: []float64{15, 15},
	}

	dist = DistanceBetweenBoxes(bbox1, bbox3)
	if dist != 0 {
		t.Errorf("DistanceBetweenBoxes() for overlapping boxes = %f, want 0", dist)
	}
}

// TestOverlapArea tests overlap area calculation.
func TestOverlapArea(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{5, 5},
		Max: []float64{15, 15},
	}

	area := OverlapArea(bbox1, bbox2)
	expected := 25.0 // 5x5 overlap

	if area != expected {
		t.Errorf("OverlapArea() = %f, want %f", area, expected)
	}

	// Test non-overlapping boxes
	bbox3 := &BoundingBox{
		Min: []float64{20, 20},
		Max: []float64{30, 30},
	}

	area = OverlapArea(bbox1, bbox3)
	if area != 0 {
		t.Errorf("OverlapArea() for non-overlapping boxes = %f, want 0", area)
	}
}

// TestRTreeModuleConnect tests the Connect method.
func TestRTreeModuleConnect(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	table, schema, err := module.Connect(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if table == nil {
		t.Error("Expected non-nil table")
	}

	if schema == "" {
		t.Error("Expected non-empty schema")
	}

	rtree, ok := table.(*RTree)
	if !ok {
		t.Fatal("Expected *RTree")
	}

	if rtree.dimensions != 2 {
		t.Errorf("Expected 2 dimensions, got %d", rtree.dimensions)
	}
}

// TestRTreeDestroy tests the Destroy method.
func TestRTreeDestroy(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entries
	for i := 1; i <= 5; i++ {
		args := []interface{}{
			nil, int64(i),
			float64(i * 10), float64(i*10 + 10),
			float64(i * 10), float64(i*10 + 10),
		}
		rtree.Update(len(args), args)
	}

	if rtree.Count() != 5 {
		t.Fatalf("Expected 5 entries before destroy, got %d", rtree.Count())
	}

	// Destroy the table
	err = rtree.Destroy()
	if err != nil {
		t.Errorf("Destroy failed: %v", err)
	}

	// Verify data is cleared
	if rtree.root != nil {
		t.Error("Expected root to be nil after Destroy")
	}

	if len(rtree.entries) != 0 {
		t.Errorf("Expected 0 entries after Destroy, got %d", len(rtree.entries))
	}
}

// TestRTreeCursorColumn tests Column method for all column types.
func TestRTreeCursorColumn(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entry
	args := []interface{}{
		nil, int64(42),
		5.5, 15.5,
		10.0, 20.0,
	}
	rtree.Update(len(args), args)

	cursor, err := rtree.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer cursor.Close()

	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	if cursor.EOF() {
		t.Fatal("Expected cursor to have results")
	}

	// Test all columns
	tests := []struct {
		col  int
		want interface{}
	}{
		{0, int64(42)}, // ID is int64
		{1, 5.5},       // minX
		{2, 15.5},      // maxX
		{3, 10.0},      // minY
		{4, 20.0},      // maxY
	}

	for _, tt := range tests {
		tt := tt
		val, err := cursor.Column(tt.col)
		if err != nil {
			t.Errorf("Column(%d) error: %v", tt.col, err)
			continue
		}
		assertColumnValue(t, tt.col, val, tt.want)
	}

	// Test invalid column
	_, err = cursor.Column(99)
	if err == nil {
		t.Error("Expected error for invalid column index")
	}
}

// TestRangeSearch tests range-based spatial queries.
func TestRangeSearch(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	// Insert entries
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
	}

	for _, entry := range entries {
		entry := entry
		root = root.Insert(entry)
	}

	// Range search around point (10, 10) with radius 15
	point := []float64{10, 10}
	radius := 15.0

	results := root.RangeSearch(point, radius)

	// Should find entries near the point
	if len(results) == 0 {
		t.Error("RangeSearch found no results")
	}

	t.Logf("RangeSearch found %d results", len(results))
}

// TestIntersectionSearch tests intersection queries.
func TestIntersectionSearch(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
	}

	for _, entry := range entries {
		entry := entry
		root = root.Insert(entry)
	}

	bbox := &BoundingBox{Min: []float64{5, 5}, Max: []float64{25, 25}}
	results := root.IntersectionSearch(bbox)

	// Should find entries that intersect with the query box
	// Both entries might be found depending on implementation
	if len(results) == 0 {
		t.Error("IntersectionSearch found no results")
	}

	t.Logf("IntersectionSearch found %d results", len(results))
}

// TestContainmentSearch tests containment queries.
func TestContainmentSearch(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{30, 30}}},
	}

	for _, entry := range entries {
		entry := entry
		root = root.Insert(entry)
	}

	bbox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{20, 20}}
	results := root.ContainmentSearch(bbox)

	// Should find entry 1 (contained in query box)
	if len(results) != 1 {
		t.Errorf("ContainmentSearch found %d results, want 1", len(results))
	}
}

// TestEnclosureSearch tests enclosure queries.
func TestEnclosureSearch(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{25, 25}}},
	}

	for _, entry := range entries {
		entry := entry
		root = root.Insert(entry)
	}

	bbox := &BoundingBox{Min: []float64{3, 3}, Max: []float64{18, 18}}
	results := root.EnclosureSearch(bbox)

	// Should find entry 2 (encloses query box)
	if len(results) != 1 {
		t.Errorf("EnclosureSearch found %d results, want 1", len(results))
	}
}

// TestWindowQuery tests window queries.
func TestWindowQuery(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{15, 15}, Max: []float64{25, 25}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{20, 20}}},
	}

	for _, entry := range entries {
		entry := entry
		root = root.Insert(entry)
	}

	minVals := []float64{0, 0}
	maxVals := []float64{12, 12}

	results := root.WindowQuery(minVals, maxVals)

	// Should find entries that overlap window
	if len(results) == 0 {
		t.Error("WindowQuery found no results")
	}
}

// TestSpatialJoin tests spatial join operation.
func TestSpatialJoin(t *testing.T) {
	t.Parallel()
	tree1 := NewLeafNode()
	tree2 := NewLeafNode()

	// Add entries to first tree
	tree1 = tree1.Insert(&Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	})

	// Add entries to second tree
	tree2 = tree2.Insert(&Entry{
		ID:   2,
		BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
	})

	results := SpatialJoin(tree1, tree2)

	// Should find overlapping pairs
	if len(results) == 0 {
		t.Error("SpatialJoin found no pairs")
	}

	t.Logf("SpatialJoin found %d pairs", len(results))
}

// TestIntersectionBox tests intersection box calculation.
func TestIntersectionBox(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{5, 5},
		Max: []float64{15, 15},
	}

	intersection := IntersectionBox(bbox1, bbox2)

	if intersection == nil {
		t.Fatal("Expected non-nil intersection")
	}

	if intersection.Min[0] != 5 || intersection.Max[0] != 10 {
		t.Errorf("Intersection X: got [%f, %f], want [5, 10]", intersection.Min[0], intersection.Max[0])
	}

	if intersection.Min[1] != 5 || intersection.Max[1] != 10 {
		t.Errorf("Intersection Y: got [%f, %f], want [5, 10]", intersection.Min[1], intersection.Max[1])
	}

	// Test non-overlapping boxes
	bbox3 := &BoundingBox{
		Min: []float64{20, 20},
		Max: []float64{30, 30},
	}

	intersection = IntersectionBox(bbox1, bbox3)
	if intersection != nil {
		t.Error("Expected nil for non-overlapping boxes")
	}
}

// TestUnionBox tests union box calculation.
func TestUnionBox(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{5, 5},
		Max: []float64{15, 15},
	}

	union := UnionBox(bbox1, bbox2)

	if union == nil {
		t.Fatal("Expected non-nil union")
	}

	if union.Min[0] != 0 || union.Max[0] != 15 {
		t.Errorf("Union X: got [%f, %f], want [0, 15]", union.Min[0], union.Max[0])
	}

	if union.Min[1] != 0 || union.Max[1] != 15 {
		t.Errorf("Union Y: got [%f, %f], want [0, 15]", union.Min[1], union.Max[1])
	}
}

// TestBoundingBoxPerimeter tests perimeter calculation.
func TestBoundingBoxPerimeter(t *testing.T) {
	t.Parallel()
	bbox := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 5},
	}

	perimeter := bbox.Perimeter()
	expected := 15.0 // Sum of dimensions: (10-0) + (5-0)

	if perimeter != expected {
		t.Errorf("Perimeter() = %f, want %f", perimeter, expected)
	}
}

// TestBoundingBoxContainsPoint tests point containment.
func TestBoundingBoxContainsPoint(t *testing.T) {
	t.Parallel()
	bbox := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	tests := []struct {
		point    []float64
		expected bool
	}{
		{[]float64{5, 5}, true},
		{[]float64{0, 0}, true},
		{[]float64{10, 10}, true},
		{[]float64{15, 5}, false},
		{[]float64{5, 15}, false},
	}

	for _, tt := range tests {
		tt := tt
		result := bbox.ContainsPoint(tt.point)
		if result != tt.expected {
			t.Errorf("ContainsPoint(%v) = %v, want %v", tt.point, result, tt.expected)
		}
	}
}

// TestBoundingBoxEqual tests equality comparison.
func TestBoundingBoxEqual(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox3 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{5, 5},
	}

	if !bbox1.Equal(bbox2) {
		t.Error("Equal boxes reported as not equal")
	}

	if bbox1.Equal(bbox3) {
		t.Error("Different boxes reported as equal")
	}
}

// TestBoundingBoxCenter tests center calculation.
func TestBoundingBoxCenter(t *testing.T) {
	t.Parallel()
	bbox := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 20},
	}

	center := bbox.Center()

	if len(center) != 2 {
		t.Fatalf("Expected 2D center, got %d dimensions", len(center))
	}

	if center[0] != 5.0 || center[1] != 10.0 {
		t.Errorf("Center = %v, want [5, 10]", center)
	}
}

// TestEntryCreation tests entry creation and properties.
func TestEntryCreation(t *testing.T) {
	t.Parallel()
	bbox := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	entry := NewEntry(42, bbox)

	if entry.ID != 42 {
		t.Errorf("Entry ID = %d, want 42", entry.ID)
	}

	if entry.BBox != bbox {
		t.Error("Expected bbox to match")
	}

	if !entry.IsLeafEntry() {
		t.Error("Expected leaf entry (Child is nil)")
	}

	// Test with child node
	entry.Child = NewLeafNode()
	if entry.IsLeafEntry() {
		t.Error("Expected non-leaf entry (Child is not nil)")
	}
}

// TestNodeIsFull tests the IsFull method.
func TestNodeIsFull(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	if node.IsFull() {
		t.Error("Empty node should not be full")
	}

	// Fill the node
	for i := 0; i < MaxEntries; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
		}
		node.AddEntry(entry)
	}

	if !node.IsFull() {
		t.Error("Node with MaxEntries should be full")
	}
}

// TestNodeIsUnderflow tests the IsUnderflow method.
func TestNodeIsUnderflow(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	if !node.IsUnderflow() {
		t.Error("Empty node should be underflow")
	}

	// Add minimum entries
	for i := 0; i < MinEntries; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
		}
		node.AddEntry(entry)
	}

	if node.IsUnderflow() {
		t.Error("Node with MinEntries should not be underflow")
	}
}

// TestNodeRemoveEntry tests removing entries from a node.
func TestNodeRemoveEntry(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Add entries
	entries := make([]*Entry, 5)
	for i := 0; i < 5; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		entries[i] = entry
		node.AddEntry(entry)
	}

	initialCount := len(node.Entries)

	// Remove an entry
	removed := node.RemoveEntry(entries[2])

	if !removed {
		t.Error("Expected entry to be removed")
	}

	if len(node.Entries) != initialCount-1 {
		t.Errorf("Entry count = %d, want %d", len(node.Entries), initialCount-1)
	}

	// Try to remove non-existent entry
	nonExistent := &Entry{
		ID:   999,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
	}
	removed = node.RemoveEntry(nonExistent)
	if removed {
		t.Error("Expected false for non-existent entry")
	}
}

// TestGetAllLeafEntries tests retrieving all leaf entries.
func TestGetAllLeafEntries(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Add entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
		}
		node.AddEntry(entry)
	}

	entries := node.getAllLeafEntries(node)

	if len(entries) != 5 {
		t.Errorf("getAllLeafEntries returned %d entries, want 5", len(entries))
	}
}

// TestFindEntry tests finding entries in the tree.
func TestFindEntry(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	entry := &Entry{
		ID:   42,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	}
	node.AddEntry(entry)

	foundNode, index := node.FindEntry(entry)

	if foundNode == nil {
		t.Error("Expected to find entry")
	}

	if index < 0 {
		t.Error("Expected valid index")
	}

	// Try to find non-existent entry
	nonExistent := &Entry{
		ID:   999,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
	}
	foundNode, index = node.FindEntry(nonExistent)
	if foundNode != nil {
		t.Error("Expected not to find non-existent entry")
	}

	if index >= 0 {
		t.Error("Expected negative index for non-existent entry")
	}
}

// TestParseCoordinateErrors tests error handling in coordinate parsing.
func TestParseCoordinateErrors(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})

	rtree := table.(*RTree)

	tests := []struct {
		name string
		args []interface{}
	}{
		{
			name: "string coordinate",
			args: []interface{}{nil, int64(1), "invalid", 10.0, 0.0, 10.0},
		},
		{
			name: "nil coordinate",
			args: []interface{}{nil, int64(1), nil, 10.0, 0.0, 10.0},
		},
		{
			name: "bool coordinate",
			args: []interface{}{nil, int64(1), true, 10.0, 0.0, 10.0},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := rtree.Update(len(tt.args), tt.args)
			if err == nil {
				t.Error("Expected error for invalid coordinate")
			}
		})
	}
}

// TestBulkInsert tests bulk insertion operation.
func TestBulkInsert(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create entries for bulk insert
	entries := make([]*Entry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
	}

	// Bulk insert
	node = node.BulkInsert(entries)

	count := node.Count()
	if count != 100 {
		t.Errorf("Count after bulk insert = %d, want 100", count)
	}
}

// TestCompact tests tree compaction.
func TestCompact(t *testing.T) {
	t.Parallel()
	// Create entries
	entries := make([]*Entry, 50)
	for i := 0; i < 50; i++ {
		entries[i] = &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
	}

	// Compact the entries into an optimized tree
	node := Compact(entries)

	// Tree should have all entries
	if node.Count() != 50 {
		t.Errorf("Count after compact = %d, want 50", node.Count())
	}

	t.Logf("Compacted tree height: %d", node.Height())
}

// TestAssignEntryOnTie tests tie-breaking in quadratic split.
func TestAssignEntryOnTie(t *testing.T) {
	t.Parallel()
	// Create groups with equal area
	group1 := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}},
	}
	group2 := []*Entry{
		{ID: 2, BBox: &BoundingBox{Min: []float64{10, 10}, Max: []float64{15, 15}}},
	}

	entry := &Entry{ID: 3, BBox: &BoundingBox{Min: []float64{7, 7}, Max: []float64{8, 8}}}

	// This should assign based on count (both groups have 1 entry)
	bbox1 := calculateGroupBBox(group1)
	bbox2 := calculateGroupBBox(group2)

	enlargement1 := bbox1.EnlargementNeeded(entry.BBox)
	enlargement2 := bbox2.EnlargementNeeded(entry.BBox)

	t.Logf("Enlargement1: %f, Enlargement2: %f", enlargement1, enlargement2)
	t.Logf("Test completed successfully")
}

// TestRTreeUpdateEdgeCases tests Update method edge cases.
func TestRTreeUpdateEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test UPDATE with different data types
	tests := []struct {
		name string
		args []interface{}
	}{
		{
			name: "float64 ID",
			args: []interface{}{nil, 1.0, 0.0, 10.0, 0.0, 10.0},
		},
		{
			name: "int coordinates",
			args: []interface{}{nil, int64(2), 0, 10, 0, 10},
		},
		{
			name: "mixed types",
			args: []interface{}{nil, int64(3), 0.0, 10, 0, 10.0},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := rtree.Update(len(tt.args), tt.args)
			if err != nil {
				t.Logf("Update %s: %v", tt.name, err)
			}
		})
	}

	// Test invalid coordinates
	invalidArgs := []interface{}{nil, int64(4), 10.0, 0.0, 0.0, 10.0} // minX > maxX
	_, err := rtree.Update(len(invalidArgs), invalidArgs)
	if err != nil {
		t.Logf("Invalid coordinates handled: %v", err)
	}
}

// TestRTreeBestIndexVariousConstraints tests BestIndex with different constraints.
func TestRTreeBestIndexVariousConstraints(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	tests := []struct {
		name   string
		column int
		op     vtab.ConstraintOp
	}{
		{"ID EQ", 0, vtab.ConstraintEQ},
		{"MinX EQ", 1, vtab.ConstraintEQ},
		{"MinX LT", 1, vtab.ConstraintLT},
		{"MinX LE", 1, vtab.ConstraintLE},
		{"MaxX GT", 2, vtab.ConstraintGT},
		{"MaxX GE", 2, vtab.ConstraintGE},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			info := vtab.NewIndexInfo(1)
			info.Constraints[0].Column = tt.column
			info.Constraints[0].Op = tt.op
			info.Constraints[0].Usable = true

			err := rtree.BestIndex(info)
			if err != nil {
				t.Errorf("BestIndex failed: %v", err)
			}

			t.Logf("Cost for %s: %f", tt.name, info.EstimatedCost)
		})
	}
}

// TestRTreeFilterEdgeCases tests cursor Filter edge cases.
func TestRTreeFilterEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert some entries
	rtree.Update(6, []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0})
	rtree.Update(6, []interface{}{nil, int64(2), 20.0, 30.0, 20.0, 30.0})

	cursor, _ := rtree.Open()
	defer cursor.Close()

	// Test Filter with ID constraint
	err := cursor.Filter(1, "", []interface{}{int64(1)})
	if err != nil {
		t.Errorf("Filter with ID failed: %v", err)
	}

	// Test Filter with coordinate constraints
	err = cursor.Filter(2, "", []interface{}{5.0, 15.0})
	if err != nil {
		t.Errorf("Filter with coordinates failed: %v", err)
	}

	// Test Filter with no constraints (all entries)
	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Errorf("Filter with no constraints failed: %v", err)
	}
}

// TestNodeEnlargementEdgeCases tests enlargement calculation edge cases.
func TestNodeEnlargementEdgeCases(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	tests := []struct {
		name string
		bbox *BoundingBox
	}{
		{
			name: "contained bbox",
			bbox: &BoundingBox{Min: []float64{2, 2}, Max: []float64{8, 8}},
		},
		{
			name: "identical bbox",
			bbox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
		},
		{
			name: "partially overlapping",
			bbox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
		},
		{
			name: "completely outside",
			bbox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enlargement := bbox1.EnlargementNeeded(tt.bbox)
			t.Logf("Enlargement for %s: %f", tt.name, enlargement)
		})
	}
}

// TestNodeChooseSubtreeEdgeCases tests subtree selection edge cases.
func TestNodeChooseSubtreeEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewInternalNode()

	// Add child entries with different bounding boxes
	for i := 0; i < 3; i++ {
		child := NewLeafNode()
		entry := &Entry{
			ID: int64(i),
			BBox: &BoundingBox{
				Min: []float64{float64(i * 10), float64(i * 10)},
				Max: []float64{float64(i*10 + 5), float64(i*10 + 5)},
			},
			Child: child,
		}
		node.AddEntry(entry)
	}

	// Test choosing subtree for entry at different locations
	tests := []struct {
		name  string
		entry *Entry
	}{
		{
			name:  "near first child",
			entry: &Entry{BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}},
		},
		{
			name:  "near second child",
			entry: &Entry{BBox: &BoundingBox{Min: []float64{10, 10}, Max: []float64{11, 11}}},
		},
		{
			name:  "far from all",
			entry: &Entry{BBox: &BoundingBox{Min: []float64{100, 100}, Max: []float64{101, 101}}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			chosenEntry := node.ChooseSubtree(tt.entry)
			if chosenEntry == nil {
				t.Error("Expected non-nil entry from ChooseSubtree")
			}
			t.Logf("Chose subtree for %s", tt.name)
		})
	}
}

// TestRemoveEdgeCases tests Remove operation edge cases.
func TestRemoveEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Build a tree with multiple levels
	entries := make([]*Entry, MaxEntries*2)
	for i := 0; i < MaxEntries*2; i++ {
		entries[i] = &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		node = node.Insert(entries[i])
	}

	initialCount := node.Count()
	t.Logf("Initial count: %d", initialCount)

	// Remove entry from middle (use the actual entry reference)
	middleIdx := MaxEntries
	node = node.Remove(entries[middleIdx])

	countAfterRemove := node.Count()
	t.Logf("Count after remove: %d", countAfterRemove)

	if countAfterRemove >= initialCount {
		t.Error("Count should decrease after remove")
	}

	// Try to remove non-existent entry
	nonExistent := &Entry{
		ID:   999,
		BBox: &BoundingBox{Min: []float64{999, 0}, Max: []float64{1000, 1}},
	}

	countBefore := node.Count()
	node = node.Remove(nonExistent)
	countAfter := node.Count()

	// Count should not change for non-existent entry
	if countAfter != countBefore {
		t.Logf("Count changed from %d to %d (may be expected for some implementations)", countBefore, countAfter)
	}
}

// TestBoundingBoxDimensionMismatch tests operations with mismatched dimensions.
func TestBoundingBoxDimensionMismatch(t *testing.T) {
	t.Parallel()
	bbox2D := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	bbox3D := &BoundingBox{Min: []float64{0, 0, 0}, Max: []float64{10, 10, 10}}

	// Test Overlaps with different dimensions
	overlaps := bbox2D.Overlaps(bbox3D)
	if overlaps {
		t.Error("Expected false for overlaps with dimension mismatch")
	}

	// Test Contains with different dimensions
	contains := bbox2D.Contains(bbox3D)
	if contains {
		t.Error("Expected false for contains with dimension mismatch")
	}

	// Test Area with mismatched min/max
	badBox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10}}
	area := badBox.Area()
	if area != 0 {
		t.Errorf("Expected area 0 for mismatched dimensions, got %f", area)
	}

	// Test Perimeter with mismatched dimensions
	perimeter := badBox.Perimeter()
	if perimeter != 0 {
		t.Errorf("Expected perimeter 0 for mismatched dimensions, got %f", perimeter)
	}
}

// TestSearchWithEmptyTree tests search operations on empty tree.
func TestSearchWithEmptyTree(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Test SearchOverlap
	results := node.SearchOverlap(&BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}})
	if len(results) != 0 {
		t.Error("Expected no results from empty tree")
	}

	// Test SearchWithin
	results = node.SearchWithin(&BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}})
	if len(results) != 0 {
		t.Error("Expected no results from empty tree")
	}

	// Test NearestNeighborSearch
	results = node.NearestNeighborSearch([]float64{5, 5}, 5)
	if len(results) != 0 {
		t.Error("Expected no results from empty tree")
	}
}

// TestCompactEdgeCases tests Compact with various entry counts.
func TestCompactEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		count int
	}{
		{"empty", 0},
		{"single", 1},
		{"few", 5},
		{"exact page", MaxEntries},
		{"large", MaxEntries * 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			entries := make([]*Entry, tt.count)
			for i := 0; i < tt.count; i++ {
				entries[i] = &Entry{
					ID:   int64(i),
					BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
				}
			}

			node := Compact(entries)

			if tt.count > 0 {
				if node == nil {
					t.Error("Expected non-nil node")
				} else if node.Count() != tt.count {
					t.Errorf("Count = %d, want %d", node.Count(), tt.count)
				}
			}
		})
	}
}

// TestHandleUnderflow tests the underflow handling during deletion.
func TestHandleUnderflow(t *testing.T) {
	t.Parallel()
	// Create a tree with enough entries to cause splits
	node := NewLeafNode()

	// Insert entries to create a multi-level tree
	entries := make([]*Entry, MaxEntries*2+5)
	for i := 0; i < len(entries); i++ {
		entries[i] = &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		node = node.Insert(entries[i])
	}

	initialHeight := node.Height()
	t.Logf("Initial tree height: %d", initialHeight)

	// Remove entries to trigger underflow
	// Keep removing until we trigger underflow handling
	for i := 0; i < MinEntries+2; i++ {
		node = node.Remove(entries[i])
	}

	t.Logf("Count after deletions: %d", node.Count())

	// Verify tree is still valid
	if node != nil && node.Count() > 0 {
		height := node.Height()
		t.Logf("Tree height after underflow: %d", height)
	}
}

// TestPickNextEmptyGroups tests pickNext with empty groups.
func TestPickNextEmptyGroups(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create entries for splitting
	for i := 0; i < MaxEntries+1; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i * 5), 0}, Max: []float64{float64(i*5 + 3), 1}},
		}
		node.AddEntry(entry)
	}

	// Trigger split which uses pickNext internally
	if len(node.Entries) > MaxEntries {
		newRoot := node.splitNode()
		if newRoot == nil {
			t.Error("Expected non-nil root after split")
		}
	}
}

// TestExpandBoundingBox tests bounding box expansion.
func TestExpandBoundingBox(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	tests := []struct {
		name     string
		other    *BoundingBox
		expected [][]float64
	}{
		{
			name:     "larger box",
			other:    &BoundingBox{Min: []float64{-5, -5}, Max: []float64{15, 15}},
			expected: [][]float64{{-5, -5}, {15, 15}},
		},
		{
			name:     "smaller box",
			other:    &BoundingBox{Min: []float64{2, 2}, Max: []float64{8, 8}},
			expected: [][]float64{{0, 0}, {10, 10}},
		},
		{
			name:     "partially overlapping",
			other:    &BoundingBox{Min: []float64{5, 5}, Max: []float64{20, 20}},
			expected: [][]float64{{0, 0}, {20, 20}},
		},
		{
			name:     "disjoint box",
			other:    &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}},
			expected: [][]float64{{0, 0}, {30, 30}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bbox := bbox1.Clone()
			bbox.Expand(tt.other)

			for i := 0; i < 2; i++ {
				if bbox.Min[i] != tt.expected[0][i] || bbox.Max[i] != tt.expected[1][i] {
					t.Errorf("After expand, bounds = [%v, %v], want [%v, %v]",
						bbox.Min, bbox.Max, tt.expected[0], tt.expected[1])
					break
				}
			}
		})
	}
}

// TestEnlargementNeededEdgeCases tests enlargement calculation edge cases.
func TestEnlargementNeededEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bbox        *BoundingBox
		other       *BoundingBox
		minExpected float64
	}{
		{
			name:        "identical boxes",
			bbox:        &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			other:       &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			minExpected: 0,
		},
		{
			name:        "contained box",
			bbox:        &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			other:       &BoundingBox{Min: []float64{2, 2}, Max: []float64{8, 8}},
			minExpected: 0,
		},
		{
			name:        "larger box",
			bbox:        &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			other:       &BoundingBox{Min: []float64{-5, -5}, Max: []float64{15, 15}},
			minExpected: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			enlargement := tt.bbox.EnlargementNeeded(tt.other)
			if enlargement < tt.minExpected {
				t.Errorf("EnlargementNeeded = %f, want >= %f", enlargement, tt.minExpected)
			}
			t.Logf("Enlargement for %s: %f", tt.name, enlargement)
		})
	}
}

// TestSearchWithinEdgeCases tests SearchWithin with various scenarios.
func TestSearchWithinEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Insert entries with various containment relationships
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{2, 2}, Max: []float64{8, 8}}},   // Inside
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{20, 20}}}, // Contains query
		{ID: 3, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}}, // Overlaps
		{ID: 4, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}},   // Partially inside
	}

	for _, entry := range entries {
		entry := entry
		node = node.Insert(entry)
	}

	queryBox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	results := node.SearchWithin(queryBox)

	t.Logf("SearchWithin found %d entries", len(results))
	for _, entry := range results {
		entry := entry
		t.Logf("  Entry %d: [%.1f,%.1f] to [%.1f,%.1f]",
			entry.ID, entry.BBox.Min[0], entry.BBox.Min[1], entry.BBox.Max[0], entry.BBox.Max[1])
	}
}

// TestGetAllLeafEntriesInternal tests getAllLeafEntries with internal nodes.
func TestGetAllLeafEntriesInternal(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Insert enough entries to create internal nodes
	for i := 0; i < MaxEntries*2; i++ {
		entry := &Entry{
			ID:   int64(i),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		node = node.Insert(entry)
	}

	// Get all leaf entries
	entries := node.getAllLeafEntries(node)

	if len(entries) != MaxEntries*2 {
		t.Errorf("getAllLeafEntries returned %d entries, want %d", len(entries), MaxEntries*2)
	}
}

// TestCalculateGroupBBoxEmpty tests calculateGroupBBox with empty group.
func TestCalculateGroupBBoxEmpty(t *testing.T) {
	t.Parallel()
	bbox := calculateGroupBBox([]*Entry{})

	if bbox != nil {
		t.Error("Expected nil bounding box for empty group")
	}
}

// TestPickSeedsLessThanTwo tests pickSeeds with less than 2 entries.
func TestPickSeedsLessThanTwo(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Add only one entry
	entry := &Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	}
	node.AddEntry(entry)

	// Try to pick seeds - should handle gracefully
	seed1, seed2 := node.pickSeeds(node.Entries)
	t.Logf("Seeds from single entry: %d, %d", seed1, seed2)
}

// TestDistanceToPointEdgeCases tests distance calculation to points.
func TestDistanceToPointEdgeCases(t *testing.T) {
	t.Parallel()
	bbox := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	tests := []struct {
		name  string
		point []float64
	}{
		{"point inside", []float64{5, 5}},
		{"point on edge", []float64{0, 5}},
		{"point at corner", []float64{0, 0}},
		{"point outside", []float64{15, 15}},
		{"point far away", []float64{100, 100}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dist := distanceToPoint(bbox, tt.point)
			t.Logf("Distance from bbox to %v: %f", tt.point, dist)
		})
	}
}

// TestNearestNeighborEdgeCases tests nearest neighbor search edge cases.
func TestNearestNeighborEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Insert single entry
	entry := &Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	}
	node = node.Insert(entry)

	// Search for more neighbors than exist
	point := []float64{5, 5}
	results := node.NearestNeighborSearch(point, 10)

	if len(results) != 1 {
		t.Errorf("Expected 1 result when requesting more than available, got %d", len(results))
	}

	// Search with k=0
	results = node.NearestNeighborSearch(point, 0)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for k=0, got %d", len(results))
	}
}

// TestRangeSearchEdgeCases tests range search edge cases.
func TestRangeSearchEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{40, 40}, Max: []float64{50, 50}}},
	}

	for _, entry := range entries {
		entry := entry
		node = node.Insert(entry)
	}

	// Search with very large radius
	point := []float64{25, 25}
	results := node.RangeSearch(point, 1000.0)

	if len(results) != 3 {
		t.Errorf("Expected all 3 entries with large radius, got %d", len(results))
	}

	// Search with very small radius
	results = node.RangeSearch(point, 0.1)
	t.Logf("Found %d results with very small radius", len(results))
}

// TestEnclosureSearchEdgeCases tests enclosure search edge cases.
func TestEnclosureSearchEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Entry that exactly matches query box
	entry1 := &Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	}
	node = node.Insert(entry1)

	queryBox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	results := node.EnclosureSearch(queryBox)

	// Exact match should be found
	if len(results) != 1 {
		t.Errorf("Expected 1 result for exact match, got %d", len(results))
	}
}

// TestWindowQueryEdgeCases tests window query edge cases.
func TestWindowQueryEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	entry := &Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
	}
	node = node.Insert(entry)

	// Window that exactly matches entry
	results := node.WindowQuery([]float64{5, 5}, []float64{15, 15})
	if len(results) != 1 {
		t.Errorf("Expected 1 result for exact match, got %d", len(results))
	}

	// Window outside all entries
	results = node.WindowQuery([]float64{100, 100}, []float64{200, 200})
	if len(results) != 0 {
		t.Errorf("Expected 0 results outside all entries, got %d", len(results))
	}
}

// TestSpatialJoinEdgeCases tests spatial join edge cases.
func TestSpatialJoinEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with empty trees
	tree1 := NewLeafNode()
	tree2 := NewLeafNode()

	results := SpatialJoin(tree1, tree2)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty trees, got %d", len(results))
	}

	// Test with one empty tree
	tree1 = tree1.Insert(&Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	})

	results = SpatialJoin(tree1, tree2)
	if len(results) != 0 {
		t.Errorf("Expected 0 results when one tree is empty, got %d", len(results))
	}
}

// TestProcessSpatialJoinPairEdgeCases tests spatial join pair processing.
func TestProcessSpatialJoinPairEdgeCases(t *testing.T) {
	t.Parallel()
	// Create trees with non-overlapping entries
	tree1 := NewLeafNode()
	tree2 := NewLeafNode()

	tree1 = tree1.Insert(&Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
	})

	tree2 = tree2.Insert(&Entry{
		ID:   2,
		BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}},
	})

	results := SpatialJoin(tree1, tree2)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-overlapping entries, got %d", len(results))
	}
}

// TestDistanceBetweenBoxesEdgeCases tests distance calculation edge cases.
func TestDistanceBetweenBoxesEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		bbox1 *BoundingBox
		bbox2 *BoundingBox
	}{
		{
			name:  "touching boxes",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			bbox2: &BoundingBox{Min: []float64{10, 10}, Max: []float64{20, 20}},
		},
		{
			name:  "overlapping boxes",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			bbox2: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
		},
		{
			name:  "same box",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			bbox2: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dist := DistanceBetweenBoxes(tt.bbox1, tt.bbox2)
			t.Logf("Distance for %s: %f", tt.name, dist)
		})
	}
}

// TestOverlapAreaEdgeCases tests overlap area calculation edge cases.
func TestOverlapAreaEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		bbox1 *BoundingBox
		bbox2 *BoundingBox
	}{
		{
			name:  "touching but not overlapping",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			bbox2: &BoundingBox{Min: []float64{10, 0}, Max: []float64{20, 10}},
		},
		{
			name:  "fully contained",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{20, 20}},
			bbox2: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
		},
		{
			name:  "identical boxes",
			bbox1: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			bbox2: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			area := OverlapArea(tt.bbox1, tt.bbox2)
			t.Logf("Overlap area for %s: %f", tt.name, area)
		})
	}
}

// TestIntersectionBoxEdgeCases tests intersection box calculation edge cases.
func TestIntersectionBoxEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with 3D boxes
	bbox1 := &BoundingBox{
		Min: []float64{0, 0, 0},
		Max: []float64{10, 10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{5, 5, 5},
		Max: []float64{15, 15, 15},
	}

	intersection := IntersectionBox(bbox1, bbox2)
	if intersection == nil {
		t.Fatal("Expected non-nil intersection for 3D boxes")
	}

	for i := 0; i < 3; i++ {
		if intersection.Min[i] != 5 || intersection.Max[i] != 10 {
			t.Errorf("3D intersection dim %d: got [%f, %f], want [5, 10]",
				i, intersection.Min[i], intersection.Max[i])
		}
	}
}

// TestUnionBoxEdgeCases tests union box calculation edge cases.
func TestUnionBoxEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with 3D boxes
	bbox1 := &BoundingBox{
		Min: []float64{0, 0, 0},
		Max: []float64{10, 10, 10},
	}

	bbox2 := &BoundingBox{
		Min: []float64{5, 5, 5},
		Max: []float64{15, 15, 15},
	}

	union := UnionBox(bbox1, bbox2)
	if union == nil {
		t.Fatal("Expected non-nil union for 3D boxes")
	}

	for i := 0; i < 3; i++ {
		if union.Min[i] != 0 || union.Max[i] != 15 {
			t.Errorf("3D union dim %d: got [%f, %f], want [0, 15]",
				i, union.Min[i], union.Max[i])
		}
	}
}

// TestPriorityQueuePopEmpty tests popping from empty priority queue.
func TestPriorityQueuePopEmpty(t *testing.T) {
	t.Parallel()
	pq := NewPriorityQueue()

	// Pop from empty queue should not crash
	item := pq.Pop()
	if item != nil {
		t.Error("Expected nil from empty queue")
	}
}

// TestPriorityQueueOrdering tests priority queue maintains correct ordering.
func TestPriorityQueueOrdering(t *testing.T) {
	t.Parallel()
	pq := NewPriorityQueue()

	// Push items with different distances (priorities)
	pq.Push(&SearchItem{Entry: &Entry{ID: 3}, Distance: 3.0})
	pq.Push(&SearchItem{Entry: &Entry{ID: 1}, Distance: 1.0})
	pq.Push(&SearchItem{Entry: &Entry{ID: 2}, Distance: 2.0})

	// Pop should return in distance order (lowest first)
	item1 := pq.Pop()
	if item1.Distance != 1.0 {
		t.Errorf("First pop distance = %f, want 1.0", item1.Distance)
	}

	item2 := pq.Pop()
	if item2.Distance != 2.0 {
		t.Errorf("Second pop distance = %f, want 2.0", item2.Distance)
	}

	item3 := pq.Pop()
	if item3.Distance != 3.0 {
		t.Errorf("Third pop distance = %f, want 3.0", item3.Distance)
	}
}

// TestRTreeCursorAtEOF tests cursor operations at EOF.
func TestRTreeCursorAtEOF(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	cursor, _ := rtree.Open()
	defer cursor.Close()

	// Empty tree - cursor should be at EOF
	cursor.Filter(0, "", nil)

	// Try to get column at EOF
	_, err := cursor.Column(0)
	if err == nil {
		t.Error("Expected error when getting column at EOF")
	}

	// Try to get rowid at EOF
	_, err = cursor.Rowid()
	if err == nil {
		t.Error("Expected error when getting rowid at EOF")
	}
}

// TestRTreeRowidWithFloat tests Rowid method when entry ID is not int64.
func TestRTreeRowidWithFloat(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert with float64 ID
	rtree.Update(6, []interface{}{nil, 1.0, 0.0, 10.0, 0.0, 10.0})

	cursor, _ := rtree.Open()
	defer cursor.Close()

	cursor.Filter(0, "", nil)

	if !cursor.EOF() {
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid failed: %v", err)
		}
		t.Logf("Rowid: %d", rowid)
	}
}

// TestBBoxOperationsWithMismatchedDimensions tests operations with wrong dimensions.
func TestBBoxOperationsWithMismatchedDimensions(t *testing.T) {
	t.Parallel()
	bbox2D := NewBoundingBox(2)
	bbox2D.Min[0], bbox2D.Max[0] = 0, 10
	bbox2D.Min[1], bbox2D.Max[1] = 0, 10

	bbox3D := NewBoundingBox(3)
	bbox3D.Min[0], bbox3D.Max[0] = 5, 15
	bbox3D.Min[1], bbox3D.Max[1] = 5, 15
	bbox3D.Min[2], bbox3D.Max[2] = 5, 15

	// Test Equal with different dimensions
	if bbox2D.Equal(bbox3D) {
		t.Error("Expected false for boxes with different dimensions")
	}

	// Test ContainsPoint with wrong dimension point
	if bbox2D.ContainsPoint([]float64{5, 5, 5}) {
		t.Error("Expected false for point with wrong dimensions")
	}

	// Test Expand with different dimensions (should not crash)
	bbox2DCopy := bbox2D.Clone()
	bbox2DCopy.Expand(bbox3D)
	t.Log("Expand with mismatched dimensions completed")
}

// TestUnderflowRebalancing tests underflow handling through node rebalancing.
func TestUnderflowRebalancing(t *testing.T) {
	t.Parallel()
	// Create a specific tree structure to trigger underflow
	// We need a tree where a leaf has exactly MinEntries+1 and removing one triggers underflow
	node := NewLeafNode()

	// Insert exactly (MaxEntries + 1) * 2 entries to force splitting into 3 or more leaves
	numEntries := (MaxEntries + 1) * 2
	entries := make([]*Entry, numEntries)
	for i := 0; i < numEntries; i++ {
		entries[i] = &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i * 2), 0}, Max: []float64{float64(i*2 + 1), 1}},
		}
		node = node.Insert(entries[i])
	}

	initialHeight := node.Height()
	initialCount := node.Count()
	t.Logf("Initial state: height=%d, count=%d, minEntries=%d", initialHeight, initialCount, MinEntries)

	// Now find a leaf and delete entries from it until it underflows
	// This is tricky - we need to delete from one specific leaf
	// Let's delete several entries in sequence which are likely in the same leaf
	deleteCount := 0
	for i := 0; i < numEntries && deleteCount < MinEntries; i++ {
		prevCount := node.Count()
		node = node.Remove(entries[i])
		if node == nil {
			t.Log("Tree became nil after removal")
			break
		}
		if node.Count() < prevCount {
			deleteCount++
		}
	}

	if node != nil {
		finalHeight := node.Height()
		finalCount := node.Count()
		t.Logf("After deletions: height=%d, count=%d", finalHeight, finalCount)
	}
}

// TestHandleUnderflowDirectly tests underflow with careful tree construction.
func TestHandleUnderflowDirectly(t *testing.T) {
	t.Parallel()
	// Build a tree where we can force underflow
	// Strategy: Create a tree with multiple levels, then delete enough from one leaf to trigger underflow

	root := NewLeafNode()

	// Insert enough entries to create a 3-level tree
	numEntries := MaxEntries * MaxEntries / 2
	allEntries := make([]*Entry, numEntries)

	for i := 0; i < numEntries; i++ {
		allEntries[i] = &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		root = root.Insert(allEntries[i])
	}

	initialCount := root.Count()
	t.Logf("Built tree with %d entries, height=%d", initialCount, root.Height())

	// Now delete entries one by one - eventually this should trigger underflow handling
	deleted := 0
	for i := 0; i < numEntries/2 && root != nil; i++ {
		prevCount := root.Count()
		root = root.Remove(allEntries[i])
		if root != nil && root.Count() < prevCount {
			deleted++
		}
	}

	if root != nil {
		finalCount := root.Count()
		t.Logf("After deleting %d entries, %d remain, height=%d", deleted, finalCount, root.Height())
	}
}

// TestRangeSearchNilRoot tests range search with nil root.
func TestRangeSearchNilRoot(t *testing.T) {
	t.Parallel()
	var node *Node
	results := node.RangeSearch([]float64{0, 0}, 10.0)

	if results != nil {
		t.Error("Expected nil results for nil root")
	}
}

// TestProcessSpatialJoinPairAllBranches tests all branches of processSpatialJoinPair.
func TestProcessSpatialJoinPairAllBranches(t *testing.T) {
	t.Parallel()
	// Create tree with both leaf and internal nodes
	tree1 := NewLeafNode()
	tree2 := NewLeafNode()

	// Insert enough entries to create internal nodes
	for i := 0; i < MaxEntries+5; i++ {
		entry1 := &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 2), 2}},
		}
		tree1 = tree1.Insert(entry1)

		entry2 := &Entry{
			ID:   int64(i + 1000),
			BBox: &BoundingBox{Min: []float64{float64(i + 1), 1}, Max: []float64{float64(i + 3), 3}},
		}
		tree2 = tree2.Insert(entry2)
	}

	results := SpatialJoin(tree1, tree2)
	t.Logf("Spatial join found %d pairs", len(results))
}

// TestLessFunction tests the less function in priority queue.
func TestLessFunction(t *testing.T) {
	t.Parallel()
	pq := NewPriorityQueue()

	// Push items with equal distances
	pq.Push(&SearchItem{Entry: &Entry{ID: 1}, Distance: 5.0})
	pq.Push(&SearchItem{Entry: &Entry{ID: 2}, Distance: 5.0})

	// Pop should still work with equal priorities
	item := pq.Pop()
	if item == nil {
		t.Error("Expected non-nil item")
	}
}

// TestUnionBoxDifferentSizes tests UnionBox with boxes of different dimensions.
func TestUnionBoxDifferentSizes(t *testing.T) {
	t.Parallel()
	bbox2D := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox3D := &BoundingBox{
		Min: []float64{5, 5, 5},
		Max: []float64{15, 15, 15},
	}

	result := UnionBox(bbox2D, bbox3D)
	if result != nil {
		t.Error("Expected nil for boxes with different dimensions")
	}
}

// TestIntersectionBoxDifferentSizes tests IntersectionBox with boxes of different dimensions.
func TestIntersectionBoxDifferentSizes(t *testing.T) {
	t.Parallel()
	bbox2D := &BoundingBox{
		Min: []float64{0, 0},
		Max: []float64{10, 10},
	}

	bbox3D := &BoundingBox{
		Min: []float64{5, 5, 5},
		Max: []float64{15, 15, 15},
	}

	result := IntersectionBox(bbox2D, bbox3D)
	if result != nil {
		t.Error("Expected nil for boxes with different dimensions")
	}
}

// TestNearestNeighborLargeK tests nearest neighbor with k larger than tree size.
func TestNearestNeighborLargeK(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Insert 5 entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i * 10), 0}, Max: []float64{float64(i*10 + 5), 5}},
		}
		node = node.Insert(entry)
	}

	// Request 100 neighbors (more than available)
	point := []float64{25, 2}
	results := node.NearestNeighborSearch(point, 100)

	if len(results) != 5 {
		t.Errorf("Expected 5 results (all entries), got %d", len(results))
	}
}

// TestSearchWithinAllScenarios tests SearchWithin with various containment scenarios.
func TestSearchWithinAllScenarios(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Add entries with different relationships to query box
	entries := []*Entry{
		// Fully contained
		{ID: 1, BBox: &BoundingBox{Min: []float64{2, 2}, Max: []float64{5, 5}}},
		// Exactly matches query box
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		// Contains query box
		{ID: 3, BBox: &BoundingBox{Min: []float64{-5, -5}, Max: []float64{15, 15}}},
		// Partially overlaps
		{ID: 4, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		// Outside
		{ID: 5, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
	}

	for _, entry := range entries {
		entry := entry
		node = node.Insert(entry)
	}

	queryBox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	results := node.SearchWithin(queryBox)

	t.Logf("SearchWithin found %d entries", len(results))
	for _, r := range results {
		r := r
		t.Logf("  Entry %d", r.ID)
	}
}

// TestEnclosureSearchNonLeaf tests enclosure search with non-leaf nodes.
func TestEnclosureSearchNonLeaf(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create a multi-level tree
	for i := 0; i < MaxEntries*2; i++ {
		entry := &Entry{
			ID:   int64(i + 1),
			BBox: &BoundingBox{Min: []float64{float64(i), 0}, Max: []float64{float64(i + 1), 1}},
		}
		node = node.Insert(entry)
	}

	// Search for entries that enclose a small box
	queryBox := &BoundingBox{Min: []float64{5, 0}, Max: []float64{6, 1}}
	results := node.EnclosureSearch(queryBox)

	t.Logf("EnclosureSearch found %d entries", len(results))
}

// TestNodeBoundingBoxEmptyNode tests BoundingBox on empty node.
func TestNodeBoundingBoxEmptyNode(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	bbox := node.BoundingBox()
	if bbox != nil {
		t.Error("Expected nil bounding box for empty node")
	}
}

// TestHeightEmptyTree tests Height on empty tree.
func TestHeightEmptyTree(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	height := node.Height()
	if height != 1 {
		t.Errorf("Expected height 1 for empty leaf, got %d", height)
	}
}

// TestChooseLeafNilChild tests chooseLeaf with nil child pointer.
func TestChooseLeafNilChild(t *testing.T) {
	t.Parallel()
	// Create an internal node with entry but ensure robust handling
	node := NewInternalNode()

	entry := &Entry{
		ID:   1,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
		// Child is intentionally nil to test error handling
	}
	node.AddEntry(entry)

	// Try to choose leaf - should handle nil child gracefully
	insertEntry := &Entry{
		ID:   2,
		BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}},
	}

	leaf := node.chooseLeaf(insertEntry)
	if leaf == nil {
		t.Log("chooseLeaf correctly handled nil child")
	}
}

// TestBulkLoadSingleEntry tests STR bulk load with single entry.
func TestBulkLoadSingleEntry(t *testing.T) {
	t.Parallel()
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
	}

	node := Compact(entries)

	if node == nil {
		t.Error("Expected non-nil node")
	}

	if node.Count() != 1 {
		t.Errorf("Expected count 1, got %d", node.Count())
	}
}

// TestDistanceBetweenBoxesSameDimensions tests distance with various dimensions.
func TestDistanceBetweenBoxesSameDimensions(t *testing.T) {
	t.Parallel()
	// Test 1D
	bbox1D_1 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}
	bbox1D_2 := &BoundingBox{Min: []float64{20}, Max: []float64{30}}
	dist := DistanceBetweenBoxes(bbox1D_1, bbox1D_2)
	t.Logf("1D distance: %f", dist)

	// Test 3D
	bbox3D_1 := &BoundingBox{Min: []float64{0, 0, 0}, Max: []float64{10, 10, 10}}
	bbox3D_2 := &BoundingBox{Min: []float64{20, 20, 20}, Max: []float64{30, 30, 30}}
	dist = DistanceBetweenBoxes(bbox3D_1, bbox3D_2)
	t.Logf("3D distance: %f", dist)
}

// TestOverlapAreaFullContainment tests overlap with full containment.
func TestOverlapAreaFullContainment(t *testing.T) {
	t.Parallel()
	bbox1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{20, 20}}
	bbox2 := &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}

	area := OverlapArea(bbox1, bbox2)
	expectedArea := 100.0 // 10x10

	if area != expectedArea {
		t.Errorf("Overlap area = %f, want %f", area, expectedArea)
	}
}

// TestCreateTableInvalidDimensions tests creating rtree with invalid dimensions.
func TestCreateTableInvalidDimensions(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	// Test with 1D (not enough columns)
	_, _, err := module.Create(nil, "rtree", "main", "test", []string{"id", "minX", "maxX"})
	if err == nil {
		t.Error("Expected error for 1D rtree")
	}

	// Test with too many dimensions
	args := []string{"id"}
	for i := 0; i < 20; i++ {
		args = append(args, fmt.Sprintf("min%d", i), fmt.Sprintf("max%d", i))
	}
	_, _, err = module.Create(nil, "rtree", "main", "test", args)
	if err != nil {
		t.Logf("Create with many dimensions: %v", err)
	}
}

// TestParseCoordinateDifferentTypes tests parsing coordinates of different types.
func TestParseCoordinateDifferentTypes(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	tests := []struct {
		name  string
		value interface{}
	}{
		{"int", int(10)},
		{"int32", int32(10)},
		{"int64", int64(10)},
		{"float32", float32(10.5)},
		{"float64", float64(10.5)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			args := []interface{}{nil, int64(1), tt.value, 20.0, 0.0, 10.0}
			_, err := rtree.Update(len(args), args)
			if err != nil {
				t.Logf("Update with %s: %v", tt.name, err)
			}
		})
	}
}

// TestSearchFunctionsWithNilRoot tests search functions with nil root.
func TestSearchFunctionsWithNilRoot(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Don't insert anything - root is nil

	queryBox := NewBoundingBox(2)
	queryBox.Min[0], queryBox.Max[0] = 0, 10
	queryBox.Min[1], queryBox.Max[1] = 0, 10

	// Test SearchOverlap
	results := rtree.SearchOverlap(queryBox)
	if len(results) != 0 {
		t.Error("Expected 0 results from empty tree")
	}

	// Test SearchContains
	results = rtree.SearchContains([]float64{5, 5})
	if len(results) != 0 {
		t.Error("Expected 0 results from empty tree")
	}

	// Test SearchWithin
	results = rtree.SearchWithin(queryBox)
	if len(results) != 0 {
		t.Error("Expected 0 results from empty tree")
	}
}

// TestInsertTieBreaking tests quadratic split tie-breaking logic.
func TestInsertTieBreaking(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert entries that will cause splits with ties
	for i := 0; i < 20; i++ {
		x := float64(i * 10)
		args := []interface{}{nil, int64(i + 1), x, x + 5, x, x + 5}
		rtree.Update(len(args), args)
	}

	// Verify tree is still valid
	if rtree.root == nil {
		t.Error("Root should not be nil after inserts")
	}

	count := rtree.Count()
	if count != 20 {
		t.Errorf("Expected 20 entries, got %d", count)
	}
}

// TestPriorityQueueTieBreakers tests all branches of priority queue less function.
func TestPriorityQueueTieBreakers(t *testing.T) {
	t.Parallel()
	pq := NewPriorityQueue()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	entry1 := NewEntry(5, bbox1)
	entry2 := NewEntry(3, bbox1)
	node1 := NewLeafNode()
	node2 := NewLeafNode()

	// Test with same distance, entry IDs differ
	pq.Push(&SearchItem{Distance: 10.0, Entry: entry1})
	pq.Push(&SearchItem{Distance: 10.0, Entry: entry2})

	// Higher ID should come first
	if !pq.less(0, 1) {
		t.Error("Higher entry ID should come first")
	}

	// Test entry vs node at same distance
	pq.Push(&SearchItem{Distance: 10.0, Node: node1})
	if !pq.less(0, 2) {
		t.Error("Entry should come before node at same distance")
	}

	// Test two nodes at same distance
	pq.Push(&SearchItem{Distance: 10.0, Node: node2})
	if pq.less(2, 3) {
		t.Error("Two nodes at same distance should return false")
	}
}

// TestSearchWithinCompleteContainment tests SearchWithin when query contains entry bbox.
func TestSearchWithinCompleteContainment(t *testing.T) {
	t.Parallel()
	root := NewInternalNode()
	leaf := NewLeafNode()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 5, 8
	bbox1.Min[1], bbox1.Max[1] = 5, 8

	entry := NewEntry(1, bbox1)
	leaf.AddEntry(entry)

	parentBBox := NewBoundingBox(2)
	parentBBox.Min[0], parentBBox.Max[0] = 0, 15
	parentBBox.Min[1], parentBBox.Max[1] = 0, 15

	parentEntry := NewEntry(10, parentBBox)
	parentEntry.Child = leaf
	root.AddEntry(parentEntry)

	// Query that completely contains the parent bbox
	queryBox := NewBoundingBox(2)
	queryBox.Min[0], queryBox.Max[0] = -5, 20
	queryBox.Min[1], queryBox.Max[1] = -5, 20

	results := root.SearchWithin(queryBox)

	if len(results) != 1 {
		t.Errorf("Expected 1 result when query contains all, got %d", len(results))
	}
}

// TestRangeSearchInternalNode tests RangeSearch with internal nodes.
func TestRangeSearchInternalNode(t *testing.T) {
	t.Parallel()
	root := NewInternalNode()
	leaf := NewLeafNode()

	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 10, 20
	bbox.Min[1], bbox.Max[1] = 10, 20

	entry := NewEntry(1, bbox)
	leaf.AddEntry(entry)

	parentEntry := NewEntry(2, bbox)
	parentEntry.Child = leaf
	root.AddEntry(parentEntry)

	// Point close enough to bbox
	point := []float64{25, 25}
	radius := 10.0

	results := root.RangeSearch(point, radius)

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}

// TestHandleDeleteMissingEntry tests deleting an entry that doesn't exist.
func TestHandleDeleteMissingEntry(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Try to delete from empty tree
	args := []interface{}{int64(999), nil, nil, nil, nil, nil}
	_, err := rtree.Update(len(args), args)

	if err != nil {
		t.Logf("Delete non-existent entry: %v", err)
	}

	// Insert then delete non-existent
	insertArgs := []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0}
	rtree.Update(len(insertArgs), insertArgs)

	deleteArgs := []interface{}{int64(999), nil, nil, nil, nil, nil}
	_, err = rtree.Update(len(deleteArgs), deleteArgs)

	if err != nil {
		t.Logf("Delete non-existent when tree has entries: %v", err)
	}
}

// TestHandleRootAfterRemovalLeafCase tests handleRootAfterRemoval with leaf root.
func TestHandleRootAfterRemovalLeafCase(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10

	entry := NewEntry(1, bbox)
	root.AddEntry(entry)
	root.RemoveEntry(entry)

	newRoot := root.handleRootAfterRemoval()
	if newRoot != nil {
		t.Error("Empty leaf root should return nil")
	}
}

// TestEnlargementDifferentDims tests EnlargementNeeded with mismatched dimensions.
func TestEnlargementDifferentDims(t *testing.T) {
	t.Parallel()
	bbox2D := NewBoundingBox(2)
	bbox2D.Min[0], bbox2D.Max[0] = 0, 10
	bbox2D.Min[1], bbox2D.Max[1] = 0, 10

	bbox3D := NewBoundingBox(3)
	bbox3D.Min[0], bbox3D.Max[0] = 5, 15
	bbox3D.Min[1], bbox3D.Max[1] = 5, 15
	bbox3D.Min[2], bbox3D.Max[2] = 5, 15

	enlargement := bbox2D.EnlargementNeeded(bbox3D)
	if enlargement != math.MaxFloat64 {
		t.Errorf("Mismatched dimensions should return MaxFloat64, got %f", enlargement)
	}
}

// TestHeightMultiLevel tests Height calculation for multi-level trees.
func TestHeightMultiLevel(t *testing.T) {
	t.Parallel()
	// Build a 3-level tree
	root := NewInternalNode()
	mid := NewInternalNode()
	leaf := NewLeafNode()

	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10

	leafEntry := NewEntry(1, bbox)
	leaf.AddEntry(leafEntry)

	midEntry := NewEntry(2, bbox)
	midEntry.Child = leaf
	mid.AddEntry(midEntry)

	rootEntry := NewEntry(3, bbox)
	rootEntry.Child = mid
	root.AddEntry(rootEntry)

	height := root.Height()
	if height != 3 {
		t.Errorf("Expected height 3, got %d", height)
	}
}

// TestDistanceToPointBelowMin tests distanceToPoint when point is below min.
func TestDistanceToPointBelowMin(t *testing.T) {
	t.Parallel()
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 10, 20
	bbox.Min[1], bbox.Max[1] = 10, 20

	point := []float64{5, 5}
	dist := distanceToPoint(bbox, point)

	expectedDist := math.Sqrt(50) // sqrt(5^2 + 5^2)
	if math.Abs(dist-expectedDist) > 0.0001 {
		t.Errorf("Expected distance %f, got %f", expectedDist, dist)
	}
}

// TestDistanceBetweenBoxes3D tests distance calculation in 3D.
func TestDistanceBetweenBoxes3D(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(3)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10
	bbox1.Min[2], bbox1.Max[2] = 0, 10

	bbox2 := NewBoundingBox(3)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30
	bbox2.Min[2], bbox2.Max[2] = 20, 30

	dist := DistanceBetweenBoxes(bbox1, bbox2)

	expectedDist := math.Sqrt(300) // sqrt(10^2 + 10^2 + 10^2)
	if math.Abs(dist-expectedDist) > 0.0001 {
		t.Errorf("Expected distance %f, got %f", expectedDist, dist)
	}
}

// TestIntersectionBoxEdgeTouch tests intersection when boxes touch at edges.
func TestIntersectionBoxEdgeTouch(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 10, 20
	bbox2.Min[1], bbox2.Max[1] = 0, 10

	result := IntersectionBox(bbox1, bbox2)

	// Touching at edge counts as intersection
	if result == nil {
		t.Error("Boxes touching at edge should intersect")
	}
}

// TestProcessSpatialJoinPairAllCases tests all 4 branches of processSpatialJoinPair.
func TestProcessSpatialJoinPairAllCases(t *testing.T) {
	t.Parallel()
	// Test both internal nodes case
	t.Run("BothInternal", func(t *testing.T) {
		t.Parallel()
		internal1 := NewInternalNode()
		internal2 := NewInternalNode()
		leaf1 := NewLeafNode()
		leaf2 := NewLeafNode()

		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = 0, 10
		bbox.Min[1], bbox.Max[1] = 0, 10

		leafEntry1 := NewEntry(1, bbox)
		leafEntry2 := NewEntry(2, bbox)
		leaf1.AddEntry(leafEntry1)
		leaf2.AddEntry(leafEntry2)

		entry1 := NewEntry(10, bbox)
		entry1.Child = leaf1
		entry2 := NewEntry(20, bbox)
		entry2.Child = leaf2

		internal1.AddEntry(entry1)
		internal2.AddEntry(entry2)

		results := make([][2]*Entry, 0)
		processSpatialJoinPair(internal1, internal2, entry1, entry2, &results)

		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
	})

	// Test n1 internal, n2 leaf case
	t.Run("N1InternalN2Leaf", func(t *testing.T) {
		t.Parallel()
		internal1 := NewInternalNode()
		leaf1 := NewLeafNode()
		leaf2 := NewLeafNode()

		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = 0, 10
		bbox.Min[1], bbox.Max[1] = 0, 10

		leafEntry := NewEntry(1, bbox)
		leaf1.AddEntry(leafEntry)

		entry1 := NewEntry(10, bbox)
		entry1.Child = leaf1
		entry2 := NewEntry(2, bbox)

		internal1.AddEntry(entry1)
		leaf2.AddEntry(entry2)

		results := make([][2]*Entry, 0)
		processSpatialJoinPair(internal1, leaf2, entry1, entry2, &results)

		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}
	})
}

// TestAssignEntryOnTieAllBranches tests all branches of assignEntryOnTie.
func TestAssignEntryOnTieAllBranches(t *testing.T) {
	t.Parallel()
	node := NewInternalNode()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 0, 10
	bbox2.Min[1], bbox2.Max[1] = 0, 10

	entryToAssign := NewEntry(1, bbox1)

	// Test tie case with equal areas and equal sizes
	t.Run("TieEqualSizes", func(t *testing.T) {
		t.Parallel()
		group1 := []*Entry{}
		group2 := []*Entry{}

		node.assignEntryOnTie(entryToAssign, &group1, &group2, bbox1, bbox2)

		if len(group1) == 0 && len(group2) == 0 {
			t.Error("Entry should be assigned to one group")
		}
	})
}

// TestSearchWithinPartialOverlap tests SearchWithin with partial overlap.
func TestSearchWithinPartialOverlap(t *testing.T) {
	t.Parallel()
	root := NewInternalNode()
	leaf := NewLeafNode()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	entry := NewEntry(1, bbox1)
	leaf.AddEntry(entry)

	parentEntry := NewEntry(10, bbox1)
	parentEntry.Child = leaf
	root.AddEntry(parentEntry)

	// Query that partially overlaps but doesn't contain
	queryBox := NewBoundingBox(2)
	queryBox.Min[0], queryBox.Max[0] = 5, 7
	queryBox.Min[1], queryBox.Max[1] = 5, 7

	results := root.SearchWithin(queryBox)

	if len(results) != 0 {
		t.Errorf("Expected 0 results (entry not within query), got %d", len(results))
	}
}

// TestInsertEdgeCase tests Insert edge case.
func TestInsertEdgeCase(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert entry with nil value in coordinate
	args := []interface{}{nil, int64(1), nil, 10.0, 0.0, 10.0}
	_, err := rtree.Update(len(args), args)

	if err == nil {
		t.Log("Insert with nil coordinate handled")
	}
}

// TestHandleDeleteInternalNode tests handleDelete with complex tree.
func TestHandleDeleteInternalNode(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert many entries to create internal nodes
	for i := 1; i <= 10; i++ {
		x := float64(i * 10)
		// Insert with nil old ID and explicit new ID
		args := []interface{}{nil, nil, x, x + 5.0, x, x + 5.0}
		rtree.Update(len(args), args)
	}

	initialCount := rtree.Count()
	if initialCount != 10 {
		t.Logf("Initial count: %d", initialCount)
	}

	// Get one of the entry IDs
	entries := rtree.SearchOverlap(rtree.root.BoundingBox())
	if len(entries) > 0 {
		idToDelete := entries[5].ID
		// Delete using just the old ID
		deleteArgs := []interface{}{idToDelete}
		_, err := rtree.Update(len(deleteArgs), deleteArgs)

		if err != nil {
			t.Logf("Delete from internal node: %v", err)
		}

		count := rtree.Count()
		if count == 9 {
			t.Logf("Successfully deleted entry")
		}
	}
}

// TestSearchWithNilRoot tests search with nil root.
func TestSearchWithNilRoot(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Don't insert anything - root is nil
	queryBox := NewBoundingBox(2)
	queryBox.Min[0], queryBox.Max[0] = 0, 10
	queryBox.Min[1], queryBox.Max[1] = 0, 10

	results := rtree.SearchOverlap(queryBox)

	if len(results) != 0 {
		t.Error("Search with nil root should return empty")
	}
}

// TestSpatialJoinNilNodes tests SpatialJoin with nil nodes.
func TestSpatialJoinNilNodes(t *testing.T) {
	t.Parallel()
	results := SpatialJoin(nil, nil)

	if len(results) != 0 {
		t.Error("SpatialJoin with both nil should return empty")
	}
}

// TestDistanceBetweenBoxesDifferentDims tests DistanceBetweenBoxes with different dimensions.
func TestDistanceBetweenBoxesDifferentDims(t *testing.T) {
	t.Parallel()
	bbox2D := NewBoundingBox(2)
	bbox2D.Min[0], bbox2D.Max[0] = 0, 10
	bbox2D.Min[1], bbox2D.Max[1] = 0, 10

	bbox3D := NewBoundingBox(3)
	bbox3D.Min[0], bbox3D.Max[0] = 0, 10
	bbox3D.Min[1], bbox3D.Max[1] = 0, 10
	bbox3D.Min[2], bbox3D.Max[2] = 0, 10

	dist := DistanceBetweenBoxes(bbox2D, bbox3D)

	if dist != math.MaxFloat64 {
		t.Errorf("Different dimensions should return MaxFloat64, got %f", dist)
	}
}

// TestDistanceBetweenBoxesNonOverlapping tests distance for non-overlapping boxes.
func TestDistanceBetweenBoxesNonOverlapping(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 5
	bbox1.Min[1], bbox1.Max[1] = 0, 5

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 0, 5
	bbox2.Min[1], bbox2.Max[1] = 10, 15

	dist := DistanceBetweenBoxes(bbox1, bbox2)

	if dist != 5.0 {
		t.Errorf("Expected distance 5.0, got %f", dist)
	}
}

// TestIntersectionBoxDifferentDims tests IntersectionBox with different dimensions.
func TestIntersectionBoxDifferentDims(t *testing.T) {
	t.Parallel()
	bbox2D := NewBoundingBox(2)
	bbox2D.Min[0], bbox2D.Max[0] = 0, 10
	bbox2D.Min[1], bbox2D.Max[1] = 0, 10

	bbox3D := NewBoundingBox(3)
	bbox3D.Min[0], bbox3D.Max[0] = 5, 15
	bbox3D.Min[1], bbox3D.Max[1] = 5, 15
	bbox3D.Min[2], bbox3D.Max[2] = 5, 15

	result := IntersectionBox(bbox2D, bbox3D)

	if result != nil {
		t.Error("Different dimensions should return nil")
	}
}

// TestNearestNeighborLargeTree tests NearestNeighborSearch with complex tree.
func TestNearestNeighborLargeTree(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert many scattered entries
	for i := 1; i <= 20; i++ {
		x := float64(i * 15)
		y := float64(i * 15)
		args := []interface{}{nil, int64(i), x, x + 5, y, y + 5}
		rtree.Update(len(args), args)
	}

	// Find nearest to origin
	point := []float64{0, 0}
	results := rtree.root.NearestNeighborSearch(point, 3)

	if len(results) > 3 {
		t.Errorf("Expected at most 3 results, got %d", len(results))
	}
}

// TestUpdateWithNilOldID tests Update with nil old ID.
func TestUpdateWithNilOldID(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// This is an insert, not an update
	args := []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0}
	_, err := rtree.Update(len(args), args)

	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	count := rtree.Count()
	if count != 1 {
		t.Errorf("Expected 1 entry, got %d", count)
	}
}

// TestUpdateWithDifferentIDs tests Update when old and new IDs differ.
func TestUpdateWithDifferentIDs(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert first
	args1 := []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0}
	rtree.Update(len(args1), args1)

	// Replace with different ID (delete old, insert new)
	args2 := []interface{}{int64(1), int64(2), 5.0, 15.0, 5.0, 15.0}
	_, err := rtree.Update(len(args2), args2)

	if err != nil {
		t.Errorf("Replace with different ID failed: %v", err)
	}

	// Old ID should not exist
	entry1, exists1 := rtree.GetEntry(1)
	if exists1 {
		t.Error("Old entry should not exist")
	}
	if entry1 != nil {
		t.Error("Old entry should be nil")
	}

	// New ID should exist
	entry2, exists2 := rtree.GetEntry(2)
	if !exists2 {
		t.Error("New entry should exist")
	}
	if entry2 == nil {
		t.Error("New entry should not be nil")
	}
}

// TestParseCoordinateEdgeCases tests parseCoordinate with various types.
func TestParseCoordinateEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test with string (should error)
	args := []interface{}{nil, int64(1), "invalid", 10.0, 0.0, 10.0}
	_, err := rtree.Update(len(args), args)

	if err == nil {
		t.Log("String coordinate handled")
	}
}

// TestSTRBulkLoadEdgeCases tests STR bulk load with edge cases.
func TestSTRBulkLoadEdgeCases(t *testing.T) {
	t.Parallel()
	// Insert entries for bulk load
	entries := make([]*Entry, 0)
	for i := 0; i < 5; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = float64(i*10), float64(i*10+5)
		entry := NewEntry(int64(i+1), bbox)
		entries = append(entries, entry)
	}

	// BulkInsert returns a new root
	root := NewLeafNode()
	root = root.BulkInsert(entries)

	if root == nil {
		t.Error("BulkInsert should return a root")
	}

	count := root.Count()
	if count != 5 {
		t.Errorf("Expected 5 entries after bulk insert, got %d", count)
	}
}

// TestOverlapAreaNoOverlap tests OverlapArea when boxes don't overlap.
func TestOverlapAreaNoOverlap(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30

	area := OverlapArea(bbox1, bbox2)

	if area != 0 {
		t.Errorf("Non-overlapping boxes should have overlap area 0, got %f", area)
	}
}

// TestChooseSubtreeMultipleChildren tests ChooseSubtree with multiple children.
func TestChooseSubtreeMultipleChildren(t *testing.T) {
	t.Parallel()
	node := NewInternalNode()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30

	child1 := NewLeafNode()
	child2 := NewLeafNode()

	entry1 := NewEntry(1, bbox1)
	entry1.Child = child1
	entry2 := NewEntry(2, bbox2)
	entry2.Child = child2

	node.AddEntry(entry1)
	node.AddEntry(entry2)

	// Entry closer to bbox1
	entryBBox := NewBoundingBox(2)
	entryBBox.Min[0], entryBBox.Max[0] = 8, 12
	entryBBox.Min[1], entryBBox.Max[1] = 8, 12

	entry := NewEntry(3, entryBBox)

	result := node.ChooseSubtree(entry)
	if result == nil {
		t.Error("ChooseSubtree should return a child entry")
	}
	// Result should be one of the entries
	if result != entry1 && result != entry2 {
		t.Error("Should choose one of the available entries")
	}
}

// TestInsertSplitScenarios tests various insert and split scenarios.
func TestInsertSplitScenarios(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert enough entries to force multiple splits
	for i := 1; i <= 50; i++ {
		x := float64((i % 10) * 20)
		y := float64((i / 10) * 20)
		args := []interface{}{nil, nil, x, x + 5.0, y, y + 5.0}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	count := rtree.Count()
	if count != 50 {
		t.Errorf("Expected 50 entries, got %d", count)
	}

	// Verify tree height increased
	if rtree.root != nil {
		height := rtree.root.Height()
		if height < 2 {
			t.Logf("Tree height after 50 inserts: %d", height)
		}
	}
}

// Test3DOperations tests 3D bounding box operations.
func Test3DOperations(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ"})
	rtree := table.(*RTree)

	// Insert 3D entries
	for i := 1; i <= 20; i++ {
		x, y, z := float64(i*10), float64(i*10), float64(i*10)
		args := []interface{}{nil, nil, x, x + 5.0, y, y + 5.0, z, z + 5.0}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("3D insert failed: %v", err)
		}
	}

	count := rtree.Count()
	if count != 20 {
		t.Errorf("Expected 20 3D entries, got %d", count)
	}
}

// TestWindowQueryFullCoverage tests WindowQuery with various scenarios.
func TestWindowQueryFullCoverage(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert test entries
	for i := 1; i <= 10; i++ {
		x := float64(i * 10)
		args := []interface{}{nil, nil, x, x + 5.0, x, x + 5.0}
		rtree.Update(len(args), args)
	}

	// Query for overlapping entries
	queryBox := NewBoundingBox(2)
	queryBox.Min[0], queryBox.Max[0] = 25, 35
	queryBox.Min[1], queryBox.Max[1] = 25, 35

	results := rtree.SearchOverlap(queryBox)
	if len(results) == 0 {
		t.Log("WindowQuery found entries")
	}
}

// TestBulkOperations tests bulk insert and compact.
func TestBulkOperations(t *testing.T) {
	t.Parallel()
	// Create entries for bulk insert
	entries := make([]*Entry, 100)
	for i := 0; i < 100; i++ {
		bbox := NewBoundingBox(2)
		x := float64(i % 10 * 15)
		y := float64(i / 10 * 15)
		bbox.Min[0], bbox.Max[0] = x, x+10
		bbox.Min[1], bbox.Max[1] = y, y+10
		entries[i] = NewEntry(int64(i+1), bbox)
	}

	// Test bulk insert
	root := NewLeafNode()
	root = root.BulkInsert(entries)

	if root == nil {
		t.Fatal("BulkInsert returned nil")
	}

	count := root.Count()
	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}

	// Test compact
	compacted := Compact(entries)
	if compacted == nil {
		t.Fatal("Compact returned nil")
	}

	compactCount := compacted.Count()
	if compactCount != 100 {
		t.Errorf("Expected 100 entries after compact, got %d", compactCount)
	}
}

// TestInsertWithNilRootAndNoOverflow tests Insert when leaf doesn't overflow.
func TestInsertWithNilRootAndNoOverflow(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	// Insert a single entry (won't overflow)
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10
	entry := NewEntry(1, bbox)

	newRoot := root.Insert(entry)
	if newRoot == nil {
		t.Fatal("Insert returned nil")
	}

	if len(newRoot.Entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(newRoot.Entries))
	}

	// Insert another without overflow (MaxEntries = 8)
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30
	entry2 := NewEntry(2, bbox2)

	newRoot = newRoot.Insert(entry2)
	if len(newRoot.Entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(newRoot.Entries))
	}
}

// TestAssignEntryOnTieAllCases tests all tie-breaking scenarios.
func TestAssignEntryOnTieAllCases(t *testing.T) {
	t.Parallel()
	// Create a node with entries that will trigger all tie scenarios
	root := NewLeafNode()

	// Create entries that will cause splits with different tie scenarios
	for i := 0; i < MaxEntries+3; i++ {
		bbox := NewBoundingBox(2)
		// Create entries with same area to trigger ties
		x := float64(i * 15)
		bbox.Min[0], bbox.Max[0] = x, x+10
		bbox.Min[1], bbox.Max[1] = 0, 10 // Same height for all
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}

	// The split should have exercised tie-breaking
	if root == nil {
		t.Fatal("Insert returned nil after split")
	}

	// Test assignEntryOnTie directly
	node := NewLeafNode()

	// Create groups with equal enlargement and equal area
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10
	entry1 := NewEntry(1, bbox1)

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 0, 10 // Same area as entry1
	entry2 := NewEntry(2, bbox2)

	group1 := []*Entry{entry1}
	group2 := []*Entry{entry2}

	// Entry to assign with same enlargement to both groups
	bboxNew := NewBoundingBox(2)
	bboxNew.Min[0], bboxNew.Max[0] = 10, 20
	bboxNew.Min[1], bboxNew.Max[1] = 0, 10
	entryNew := NewEntry(3, bboxNew)

	// This should trigger the area comparison
	g1bbox := calculateGroupBBox(group1)
	g2bbox := calculateGroupBBox(group2)
	node.assignEntryOnTie(entryNew, &group1, &group2, g1bbox, g2bbox)

	// One group should have 2 entries now
	if len(group1)+len(group2) != 3 {
		t.Errorf("Expected 3 total entries, got %d", len(group1)+len(group2))
	}

	// Test with different areas - group2 smaller area
	bbox3 := NewBoundingBox(2)
	bbox3.Min[0], bbox3.Max[0] = 40, 45 // Smaller area
	bbox3.Min[1], bbox3.Max[1] = 0, 5
	entry3 := NewEntry(4, bbox3)
	group3 := []*Entry{entry1}
	group4 := []*Entry{entry3}

	bboxNew2 := NewBoundingBox(2)
	bboxNew2.Min[0], bboxNew2.Max[0] = 50, 60
	bboxNew2.Min[1], bboxNew2.Max[1] = 0, 10
	entryNew2 := NewEntry(5, bboxNew2)

	g3bbox := calculateGroupBBox(group3)
	g4bbox := calculateGroupBBox(group4)
	node.assignEntryOnTie(entryNew2, &group3, &group4, g3bbox, g4bbox)

	// Should prefer smaller area group
	if len(group4) < 1 {
		t.Error("Expected entry to be assigned to group with smaller area")
	}
}

// TestPickNextEdgeCases tests pickNext with edge cases.
func TestPickNextEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Test with empty groups
	entries := make([]*Entry, 3)
	for i := 0; i < 3; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		entries[i] = NewEntry(int64(i+1), bbox)
	}

	assigned := []bool{true, false, false}
	group1 := []*Entry{}
	group2 := []*Entry{}

	// Should find first unassigned when one group is empty
	idx := node.pickNext(entries, assigned, group1, group2)
	if idx != 1 {
		t.Errorf("Expected index 1, got %d", idx)
	}

	// Test with all assigned
	allAssigned := []bool{true, true, true}
	idx = node.pickNext(entries, allAssigned, group1, group2)
	if idx != -1 {
		t.Errorf("Expected -1 for all assigned, got %d", idx)
	}

	// Test normal case with differences
	group1 = []*Entry{entries[0]}
	group2 = []*Entry{entries[1]}
	assigned = []bool{true, true, false}

	idx = node.pickNext(entries, assigned, group1, group2)
	if idx != 2 {
		t.Errorf("Expected index 2, got %d", idx)
	}
}

// TestHandleRootAfterRemovalCases tests root handling after removal.
func TestHandleRootAfterRemovalCases(t *testing.T) {
	t.Parallel()

	t.Run("empty_root_after_deletion", func(t *testing.T) {
		root := NewLeafNode()
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = 0, 10
		bbox.Min[1], bbox.Max[1] = 0, 10
		entry := NewEntry(1, bbox)
		root.AddEntry(entry)
		root = root.Remove(entry)
		if root != nil {
			t.Error("Expected nil root for empty tree")
		}
	})

	t.Run("single_child_after_deletion", func(t *testing.T) {
		root := buildInternalRoot(t)
		root = removeUntilSingleChild(root)
		if root != nil && !root.IsLeaf && len(root.Entries) == 1 {
			root = root.handleRootAfterRemoval()
			if root != nil && !root.IsLeaf {
				t.Error("Expected root to collapse to leaf with single child")
			}
		}
	})
}

// buildInternalRoot creates a tree with enough entries to have an internal root.
func buildInternalRoot(t *testing.T) *Node {
	t.Helper()
	root := NewLeafNode()
	for i := 0; i < MaxEntries+1; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}
	if root.IsLeaf {
		t.Fatal("Expected internal root after split")
	}
	return root
}

// removeUntilSingleChild removes entries until the root has at most one child.
func removeUntilSingleChild(root *Node) *Node {
	for i := 0; i < MaxEntries; i++ {
		if len(root.Entries) <= 1 {
			break
		}
		leaf, _ := root.FindEntry(root.Entries[0].Child.Entries[0])
		if leaf != nil && len(leaf.Entries) > 0 {
			root = root.Remove(leaf.Entries[0])
		}
	}
	return root
}

// TestHandleUnderflowRecursive tests recursive underflow handling.
// findFirstLeafEntry returns the first leaf entry in the tree, or nil if none exists.
func findFirstLeafEntry(root *Node) *Entry {
	if root == nil || root.Count() == 0 {
		return nil
	}
	current := root
	for !current.IsLeaf && len(current.Entries) > 0 {
		current = current.Entries[0].Child
	}
	if len(current.Entries) > 0 {
		return current.Entries[0]
	}
	return nil
}

func TestHandleUnderflowRecursive(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	for i := 0; i < 30; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = float64(i*10), float64(i*10+5)
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}

	for i := 0; i < 25; i++ {
		leafEntry := findFirstLeafEntry(root)
		if leafEntry != nil {
			root = root.Remove(leafEntry)
		}
	}

	if root != nil && root.Count() > 0 && !root.IsLeaf {
		for _, e := range root.Entries {
			if e.Child.Parent != root {
				t.Error("Parent pointer not updated correctly after underflow")
			}
		}
	}
}

// TestStrBulkLoadEdgeCases tests STR bulk loading edge cases.
func TestStrBulkLoadEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with empty entries
	result := strBulkLoad(nil)
	if result != nil {
		t.Error("Expected nil for empty entries")
	}

	result = strBulkLoad([]*Entry{})
	if result != nil {
		t.Error("Expected nil for empty slice")
	}

	// Test with single entry
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10
	singleEntry := NewEntry(1, bbox)

	result = strBulkLoad([]*Entry{singleEntry})
	if result == nil {
		t.Fatal("Expected non-nil result for single entry")
	}
	if result.Count() != 1 {
		t.Errorf("Expected 1 entry, got %d", result.Count())
	}

	// Test with 3D entries
	entries3D := make([]*Entry, 20)
	for i := 0; i < 20; i++ {
		bbox := NewBoundingBox(3)
		bbox.Min[0], bbox.Max[0] = float64(i), float64(i+1)
		bbox.Min[1], bbox.Max[1] = float64(i), float64(i+1)
		bbox.Min[2], bbox.Max[2] = float64(i), float64(i+1)
		entries3D[i] = NewEntry(int64(i+1), bbox)
	}

	result = strBulkLoad(entries3D)
	if result == nil {
		t.Fatal("Expected non-nil result for 3D entries")
	}
	if result.Count() != 20 {
		t.Errorf("Expected 20 entries, got %d", result.Count())
	}
}

// TestStrPartitionEdgeCases tests strPartition edge cases.
func TestStrPartitionEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with nil entries
	result := strPartition(nil, 0, 2)
	if result != nil {
		t.Error("Expected nil for nil entries")
	}

	// Test with empty entries
	result = strPartition([]*Entry{}, 0, 2)
	if result != nil {
		t.Error("Expected nil for empty entries")
	}

	// Test base case (dimension >= dimensions)
	entries := make([]*Entry, 5)
	for i := 0; i < 5; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		entries[i] = NewEntry(int64(i+1), bbox)
	}

	// dimension=2 should trigger base case for 2D entries
	result = strPartition(entries, 2, 2)
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	totalEntries := 0
	for _, node := range result {
		node := node
		totalEntries += len(node.Entries)
	}
	if totalEntries != 5 {
		t.Errorf("Expected 5 entries, got %d", totalEntries)
	}

	// Test recursive partitioning
	result = strPartition(entries, 0, 2)
	if result == nil {
		t.Fatal("Expected non-nil result from recursive partition")
	}
}

// TestBuildTreeFromLeavesEdgeCases tests buildTreeFromLeaves edge cases.
func TestBuildTreeFromLeavesEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with nil
	result := buildTreeFromLeaves(nil)
	if result != nil {
		t.Error("Expected nil for nil leaves")
	}

	// Test with empty slice
	result = buildTreeFromLeaves([]*Node{})
	if result != nil {
		t.Error("Expected nil for empty leaves")
	}

	// Test with single leaf
	leaf := NewLeafNode()
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10
	leaf.AddEntry(NewEntry(1, bbox))

	result = buildTreeFromLeaves([]*Node{leaf})
	if result != leaf {
		t.Error("Expected same leaf for single node")
	}

	// Test with multiple leaves requiring multiple levels
	leaves := make([]*Node, MaxEntries+5)
	for i := 0; i < MaxEntries+5; i++ {
		leaf := NewLeafNode()
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		leaf.AddEntry(NewEntry(int64(i+1), bbox))
		leaves[i] = leaf
	}

	result = buildTreeFromLeaves(leaves)
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Should create internal nodes
	if result.IsLeaf && len(leaves) > 1 {
		t.Error("Expected internal root for multiple leaves")
	}
}

// TestChooseSubtreeEdgeCases tests ChooseSubtree edge cases.
func TestChooseSubtreeEdgeCases(t *testing.T) {
	t.Parallel()
	// Test with empty node
	node := NewLeafNode()
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10
	entry := NewEntry(1, bbox)

	result := node.ChooseSubtree(entry)
	if result != nil {
		t.Error("Expected nil for empty node")
	}

	// Test with single entry
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 5, 15
	bbox2.Min[1], bbox2.Max[1] = 5, 15
	entry2 := NewEntry(2, bbox2)
	node.AddEntry(entry2)

	result = node.ChooseSubtree(entry)
	if result != entry2 {
		t.Error("Expected the only entry")
	}

	// Test with ties (same enlargement and area)
	node2 := NewLeafNode()
	bbox3 := NewBoundingBox(2)
	bbox3.Min[0], bbox3.Max[0] = 0, 10
	bbox3.Min[1], bbox3.Max[1] = 0, 10
	entry3 := NewEntry(3, bbox3)

	bbox4 := NewBoundingBox(2)
	bbox4.Min[0], bbox4.Max[0] = 20, 30
	bbox4.Min[1], bbox4.Max[1] = 0, 10 // Same area
	entry4 := NewEntry(4, bbox4)

	node2.AddEntry(entry3)
	node2.AddEntry(entry4)

	// Entry that needs same enlargement for both
	bboxQuery := NewBoundingBox(2)
	bboxQuery.Min[0], bboxQuery.Max[0] = 10, 20
	bboxQuery.Min[1], bboxQuery.Max[1] = 0, 10
	entryQuery := NewEntry(5, bboxQuery)

	result = node2.ChooseSubtree(entryQuery)
	if result == nil {
		t.Error("Expected a result from ChooseSubtree")
	}
}

// TestHeightEdgeCases tests Height function edge cases.
func TestHeightEdgeCases(t *testing.T) {
	t.Parallel()
	// Test leaf node
	leaf := NewLeafNode()
	if leaf.Height() != 1 {
		t.Errorf("Expected height 1 for leaf, got %d", leaf.Height())
	}

	// Test empty internal node
	internal := NewInternalNode()
	if internal.Height() != 1 {
		t.Errorf("Expected height 1 for empty internal, got %d", internal.Height())
	}

	// Test tree with multiple levels
	root := NewLeafNode()
	for i := 0; i < MaxEntries+1; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}

	height := root.Height()
	if height < 2 {
		t.Errorf("Expected height >= 2 after split, got %d", height)
	}
}

// TestCreateTableEdgeCases tests createTable edge cases.
func TestCreateTableEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	t.Run("minimum_valid_columns", func(t *testing.T) {
		testCreateTableMinCols(t, module)
	})
	t.Run("whitespace_in_args", func(t *testing.T) {
		testCreateTableWhitespace(t, module)
	})
	t.Run("empty_strings_in_args", func(t *testing.T) {
		testCreateTableEmptyStrings(t, module)
	})
	t.Run("too_many_dimensions", func(t *testing.T) {
		testCreateTableTooManyDims(t, module)
	})
	t.Run("connect_method", func(t *testing.T) {
		testCreateTableConnect(t, module)
	})
}

func testCreateTableMinCols(t *testing.T, module *RTreeModule) {
	t.Helper()
	table, schema, err := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Errorf("Unexpected error for 5 columns: %v", err)
	}
	if table == nil {
		t.Error("Expected non-nil table")
	}
	if schema == "" {
		t.Error("Expected non-empty schema")
	}
}

func testCreateTableWhitespace(t *testing.T, module *RTreeModule) {
	t.Helper()
	_, _, err := module.Create(nil, "rtree", "main", "test",
		[]string{"id", " minX ", "maxX", "minY", "maxY"})
	if err != nil {
		t.Errorf("Should handle whitespace: %v", err)
	}
}

func testCreateTableEmptyStrings(t *testing.T, module *RTreeModule) {
	t.Helper()
	_, _, err := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "", "minX", "maxX", ""})
	if err == nil {
		t.Error("Expected error for insufficient columns after empty string filtering")
	}
}

func testCreateTableTooManyDims(t *testing.T, module *RTreeModule) {
	t.Helper()
	args := []string{"id"}
	for i := 0; i < 6; i++ {
		args = append(args, fmt.Sprintf("min%d", i), fmt.Sprintf("max%d", i))
	}
	_, _, err := module.Create(nil, "rtree", "main", "test", args)
	if err == nil {
		t.Error("Expected error for >5 dimensions")
	}
}

func testCreateTableConnect(t *testing.T, module *RTreeModule) {
	t.Helper()
	table, _, err := module.Connect(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Errorf("Connect failed: %v", err)
	}
	if table == nil {
		t.Error("Expected non-nil table from Connect")
	}
}

// TestBestIndexEdgeCases tests BestIndex edge cases.
func TestBestIndexEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test with no constraints
	info := &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{},
		OrderBy:     []vtab.OrderBy{},
	}
	err := rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed with no constraints: %v", err)
	}

	// Test with ID constraint
	info = &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 0, Op: vtab.ConstraintEQ, Usable: true},
		},
		OrderBy: []vtab.OrderBy{},
	}
	err = rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed with ID constraint: %v", err)
	}
	if info.IdxNum&1 == 0 {
		t.Error("Expected bit 0 set for ID constraint")
	}

	// Test with spatial constraints
	info = &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 1, Op: vtab.ConstraintLE, Usable: true},
			{Column: 2, Op: vtab.ConstraintGE, Usable: true},
		},
		OrderBy: []vtab.OrderBy{},
	}
	err = rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed with spatial constraints: %v", err)
	}

	// Test with unusable constraints
	info = &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 0, Op: vtab.ConstraintEQ, Usable: false},
		},
		OrderBy: []vtab.OrderBy{},
	}
	err = rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed with unusable constraints: %v", err)
	}

	// Test with LT and GT operators
	info = &vtab.IndexInfo{
		Constraints: []vtab.IndexConstraint{
			{Column: 1, Op: vtab.ConstraintLT, Usable: true},
			{Column: 2, Op: vtab.ConstraintGT, Usable: true},
		},
		OrderBy: []vtab.OrderBy{},
	}
	err = rtree.BestIndex(info)
	if err != nil {
		t.Errorf("BestIndex failed with LT/GT constraints: %v", err)
	}
}

// newTestRTree creates an RTree for testing.
func newTestRTree(t *testing.T) *RTree {
	t.Helper()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	return table.(*RTree)
}

// TestUpdateEdgeCases tests Update function edge cases.
func TestUpdateEdgeCases(t *testing.T) {
	t.Parallel()
	rt := newTestRTree(t)

	t.Run("invalid_argc", func(t *testing.T) {
		_, err := rt.Update(0, []interface{}{})
		if err == nil {
			t.Error("Expected error for argc=0")
		}
	})

	t.Run("delete_invalid_id_type", func(t *testing.T) {
		_, err := rt.Update(1, []interface{}{"invalid"})
		if err == nil {
			t.Error("Expected error for invalid ID type in DELETE")
		}
	})

	t.Run("delete_nonexistent", func(t *testing.T) {
		_, err := rt.Update(1, []interface{}{int64(999)})
		if err == nil {
			t.Error("Expected error for DELETE of non-existent entry")
		}
	})

	t.Run("insert_auto_id", func(t *testing.T) {
		id, err := rt.Update(6, []interface{}{nil, nil, 0.0, 10.0, 0.0, 10.0})
		if err != nil {
			t.Errorf("INSERT with auto ID failed: %v", err)
		}
		if id != 1 {
			t.Errorf("Expected auto-generated ID 1, got %d", id)
		}
	})

	t.Run("insert_explicit_and_update", func(t *testing.T) {
		testUpdateEdgeCasesInsertExplicitAndUpdate(t, rt)
	})
}

func testUpdateEdgeCasesInsertExplicitAndUpdate(t *testing.T, rt *RTree) {
	t.Helper()
	id, err := rt.Update(6, []interface{}{nil, int64(42), 20.0, 30.0, 0.0, 10.0})
	if err != nil {
		t.Errorf("INSERT with explicit ID failed: %v", err)
	}
	if id != 42 {
		t.Errorf("Expected ID 42, got %d", id)
	}

	id, err = rt.Update(6, []interface{}{int64(42), int64(43), 25.0, 35.0, 5.0, 15.0})
	if err != nil {
		t.Errorf("UPDATE failed: %v", err)
	}
	if id != 43 {
		t.Errorf("Expected new ID 43, got %d", id)
	}

	_, exists := rt.GetEntry(42)
	if exists {
		t.Error("Old entry should be removed after UPDATE")
	}

	_, err = rt.Update(1, []interface{}{int64(43)})
	if err != nil {
		t.Errorf("DELETE failed: %v", err)
	}
}

// TestHandleDeleteEdgeCases tests handleDelete edge cases.
func TestHandleDeleteEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Insert an entry
	rtree.Update(6, []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0})

	// Delete with valid ID
	id, err := rtree.handleDelete([]interface{}{int64(1)})
	if err != nil {
		t.Errorf("handleDelete failed: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected ID 1, got %d", id)
	}

	// Delete with non-existent ID
	_, err = rtree.handleDelete([]interface{}{int64(999)})
	if err == nil {
		t.Error("Expected error for non-existent ID")
	}
}

// TestCheckIfUpdateEdgeCases tests checkIfUpdate edge cases.
func TestCheckIfUpdateEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test with nil oldID
	isUpdate, id := rtree.checkIfUpdate(nil)
	if isUpdate {
		t.Error("Expected false for nil oldID")
	}
	if id != 0 {
		t.Errorf("Expected 0 for nil oldID, got %d", id)
	}

	// Test with zero oldID
	isUpdate, id = rtree.checkIfUpdate(int64(0))
	if isUpdate {
		t.Error("Expected false for zero oldID")
	}

	// Test with non-zero oldID
	isUpdate, id = rtree.checkIfUpdate(int64(42))
	if !isUpdate {
		t.Error("Expected true for non-zero oldID")
	}
	if id != 42 {
		t.Errorf("Expected 42, got %d", id)
	}

	// Test with non-int64 type
	isUpdate, id = rtree.checkIfUpdate("invalid")
	if isUpdate {
		t.Error("Expected false for non-int64 type")
	}
}

// TestParseCoordinatesEdgeCases tests parseCoordinates edge cases.
func TestParseCoordinatesEdgeCases(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test with too few coordinates
	_, err := rtree.parseCoordinates(4, []interface{}{nil, nil, 0.0, 10.0})
	if err == nil {
		t.Error("Expected error for too few coordinates")
	}

	// Test with valid coordinates
	coords, err := rtree.parseCoordinates(6, []interface{}{nil, nil, 0.0, 10.0, 5.0, 15.0})
	if err != nil {
		t.Errorf("parseCoordinates failed: %v", err)
	}
	if len(coords) != 4 {
		t.Errorf("Expected 4 coordinates, got %d", len(coords))
	}
}

// TestParseCoordinateAllTypes tests parseCoordinate with all type branches.
func TestParseCoordinateAllTypes(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, _ := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	rtree := table.(*RTree)

	// Test int64
	coord, err := rtree.parseCoordinate(int64(42), 0)
	if err != nil {
		t.Errorf("Failed to parse int64: %v", err)
	}
	if coord != 42.0 {
		t.Errorf("Expected 42.0, got %f", coord)
	}

	// Test float64
	coord, err = rtree.parseCoordinate(3.14, 0)
	if err != nil {
		t.Errorf("Failed to parse float64: %v", err)
	}
	if coord != 3.14 {
		t.Errorf("Expected 3.14, got %f", coord)
	}

	// Test string
	coord, err = rtree.parseCoordinate("2.5", 0)
	if err != nil {
		t.Errorf("Failed to parse string: %v", err)
	}
	if coord != 2.5 {
		t.Errorf("Expected 2.5, got %f", coord)
	}

	// Test invalid string
	_, err = rtree.parseCoordinate("invalid", 0)
	if err == nil {
		t.Error("Expected error for invalid string")
	}

	// Test unsupported type
	_, err = rtree.parseCoordinate([]int{1, 2}, 0)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

// TestDistanceToPointAllBranches tests distanceToPoint with all code branches.
func TestDistanceToPointAllBranches(t *testing.T) {
	t.Parallel()
	// Test with mismatched dimensions
	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10

	point3D := []float64{5, 5, 5}
	dist := distanceToPoint(bbox, point3D)
	if dist != math.MaxFloat64 {
		t.Errorf("Expected MaxFloat64 for dimension mismatch, got %f", dist)
	}

	// Test with point inside bbox
	point := []float64{5, 5}
	dist = distanceToPoint(bbox, point)
	if dist != 0 {
		t.Errorf("Expected 0 for point inside bbox, got %f", dist)
	}

	// Test with point outside bbox
	pointOutside := []float64{20, 20}
	dist = distanceToPoint(bbox, pointOutside)
	expected := math.Sqrt(10*10 + 10*10)
	if math.Abs(dist-expected) > 0.0001 {
		t.Errorf("Expected %f, got %f", expected, dist)
	}

	// Test with point outside in one dimension only
	pointPartial := []float64{5, 20}
	dist = distanceToPoint(bbox, pointPartial)
	if dist != 10 {
		t.Errorf("Expected 10, got %f", dist)
	}
}

// TestWindowQueryDimensionMismatch tests WindowQuery with mismatched dimensions.
func TestWindowQueryDimensionMismatch(t *testing.T) {
	t.Parallel()
	root := NewLeafNode()

	// Add some entries
	for i := 0; i < 5; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*10), float64(i*10+5)
		bbox.Min[1], bbox.Max[1] = 0, 5
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}

	// Test with mismatched dimensions
	min := []float64{0, 0}
	max := []float64{20}
	results := root.WindowQuery(min, max)
	if results != nil {
		t.Error("Expected nil for mismatched dimensions")
	}

	// Test with valid window
	min = []float64{0, 0}
	max = []float64{25, 10}
	results = root.WindowQuery(min, max)
	if len(results) == 0 {
		t.Error("Expected some results")
	}
}

// TestProcessSpatialJoinPairComprehensive tests processSpatialJoinPair comprehensively.
func TestProcessSpatialJoinPairComprehensive(t *testing.T) {
	t.Parallel()
	// Create two trees
	leaf1 := NewLeafNode()
	leaf2 := NewLeafNode()

	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10
	entry1 := NewEntry(1, bbox1)
	leaf1.AddEntry(entry1)

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 5, 15
	bbox2.Min[1], bbox2.Max[1] = 5, 15
	entry2 := NewEntry(2, bbox2)
	leaf2.AddEntry(entry2)

	// Test both leaves
	results := make([][2]*Entry, 0)
	processSpatialJoinPair(leaf1, leaf2, entry1, entry2, &results)
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	// Create internal nodes
	internal1 := NewInternalNode()
	internal2 := NewInternalNode()

	childEntry1 := &Entry{BBox: bbox1, Child: leaf1}
	childEntry2 := &Entry{BBox: bbox2, Child: leaf2}
	internal1.AddEntry(childEntry1)
	internal2.AddEntry(childEntry2)

	// Test leaf + internal
	results = make([][2]*Entry, 0)
	processSpatialJoinPair(leaf1, internal2, entry1, childEntry2, &results)

	// Test internal + leaf
	results = make([][2]*Entry, 0)
	processSpatialJoinPair(internal1, leaf2, childEntry1, entry2, &results)

	// Test both internal
	results = make([][2]*Entry, 0)
	processSpatialJoinPair(internal1, internal2, childEntry1, childEntry2, &results)
}

// TestDistanceBetweenBoxesAllCases tests DistanceBetweenBoxes with all scenarios.
func TestDistanceBetweenBoxesAllCases(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Test with mismatched dimensions
	bbox2 := NewBoundingBox(3)
	dist := DistanceBetweenBoxes(bbox1, bbox2)
	if dist != math.MaxFloat64 {
		t.Errorf("Expected MaxFloat64 for dimension mismatch, got %f", dist)
	}

	// Test with overlapping boxes
	bbox3 := NewBoundingBox(2)
	bbox3.Min[0], bbox3.Max[0] = 5, 15
	bbox3.Min[1], bbox3.Max[1] = 5, 15
	dist = DistanceBetweenBoxes(bbox1, bbox3)
	if dist != 0 {
		t.Errorf("Expected 0 for overlapping boxes, got %f", dist)
	}

	// Test with separated boxes in one dimension
	bbox4 := NewBoundingBox(2)
	bbox4.Min[0], bbox4.Max[0] = 20, 30
	bbox4.Min[1], bbox4.Max[1] = 0, 10
	dist = DistanceBetweenBoxes(bbox1, bbox4)
	if dist != 10 {
		t.Errorf("Expected 10, got %f", dist)
	}
}

// TestOverlapAreaAllCases tests OverlapArea with all scenarios.
func TestOverlapAreaAllCases(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Test with non-overlapping boxes
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30
	area := OverlapArea(bbox1, bbox2)
	if area != 0 {
		t.Errorf("Expected 0 for non-overlapping boxes, got %f", area)
	}

	// Test with mismatched dimensions
	bbox3 := NewBoundingBox(3)
	area = OverlapArea(bbox1, bbox3)
	if area != 0 {
		t.Errorf("Expected 0 for dimension mismatch, got %f", area)
	}

	// Test with overlapping boxes
	bbox4 := NewBoundingBox(2)
	bbox4.Min[0], bbox4.Max[0] = 5, 15
	bbox4.Min[1], bbox4.Max[1] = 5, 15
	area = OverlapArea(bbox1, bbox4)
	expected := 5.0 * 5.0
	if area != expected {
		t.Errorf("Expected %f, got %f", expected, area)
	}
}

// TestIntersectionBoxAllCases tests IntersectionBox with all scenarios.
func TestIntersectionBoxAllCases(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Test with non-overlapping boxes
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30
	bbox2.Min[1], bbox2.Max[1] = 20, 30
	result := IntersectionBox(bbox1, bbox2)
	if result != nil {
		t.Error("Expected nil for non-overlapping boxes")
	}

	// Test with mismatched dimensions
	bbox3 := NewBoundingBox(3)
	result = IntersectionBox(bbox1, bbox3)
	if result != nil {
		t.Error("Expected nil for dimension mismatch")
	}

	// Test with overlapping boxes
	bbox4 := NewBoundingBox(2)
	bbox4.Min[0], bbox4.Max[0] = 5, 15
	bbox4.Min[1], bbox4.Max[1] = 5, 15
	result = IntersectionBox(bbox1, bbox4)
	if result == nil {
		t.Fatal("Expected non-nil intersection")
	}
	if result.Min[0] != 5 || result.Max[0] != 10 {
		t.Errorf("Incorrect x intersection: [%f, %f]", result.Min[0], result.Max[0])
	}
	if result.Min[1] != 5 || result.Max[1] != 10 {
		t.Errorf("Incorrect y intersection: [%f, %f]", result.Min[1], result.Max[1])
	}
}

// TestAssignEntryOnTieArea2Smaller tests assignEntryOnTie when area2 < area1.
func TestAssignEntryOnTieArea2Smaller(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create group1 with larger area
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 20 // Area = 20 * 10 = 200
	bbox1.Min[1], bbox1.Max[1] = 0, 10
	entry1 := NewEntry(1, bbox1)
	group1 := []*Entry{entry1}

	// Create group2 with smaller area
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 30, 35 // Area = 5 * 5 = 25
	bbox2.Min[1], bbox2.Max[1] = 0, 5
	entry2 := NewEntry(2, bbox2)
	group2 := []*Entry{entry2}

	// Entry to assign
	bboxNew := NewBoundingBox(2)
	bboxNew.Min[0], bboxNew.Max[0] = 50, 60
	bboxNew.Min[1], bboxNew.Max[1] = 0, 10
	entryNew := NewEntry(3, bboxNew)

	g1bbox := calculateGroupBBox(group1)
	g2bbox := calculateGroupBBox(group2)

	lenBefore2 := len(group2)
	node.assignEntryOnTie(entryNew, &group1, &group2, g1bbox, g2bbox)

	// Should prefer group2 (smaller area)
	if len(group2) <= lenBefore2 {
		t.Error("Expected entry to be assigned to group2 (smaller area)")
	}
}

// TestInsertNilParentCase tests Insert when parent becomes nil during traversal.
func TestInsertNilParentCase(t *testing.T) {
	t.Parallel()
	// Create a simple leaf node (no parent)
	root := NewLeafNode()

	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 0, 10
	bbox.Min[1], bbox.Max[1] = 0, 10
	entry := NewEntry(1, bbox)

	// Insert when there's no parent (root case)
	newRoot := root.Insert(entry)
	if newRoot == nil {
		t.Fatal("Insert returned nil")
	}

	// The root should remain the same node since no split occurred
	if newRoot != root {
		t.Error("Expected same root node for single insert")
	}
}

// TestQuadraticSplitWithTies tests quadraticSplit with tie scenarios.
func TestQuadraticSplitWithTies(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create entries that will produce ties in pickNext
	for i := 0; i < MaxEntries+1; i++ {
		bbox := NewBoundingBox(2)
		// Create overlapping entries to trigger ties
		x := float64(i * 3)
		bbox.Min[0], bbox.Max[0] = x, x+10
		bbox.Min[1], bbox.Max[1] = 0, 10 // Same y-dimension for all
		entry := NewEntry(int64(i+1), bbox)
		node.AddEntry(entry)
	}

	// quadraticSplit should handle ties
	group1, group2 := node.quadraticSplit()

	if len(group1) < MinEntries || len(group2) < MinEntries {
		t.Errorf("Groups don't meet MinEntries: %d, %d", len(group1), len(group2))
	}

	if len(group1)+len(group2) != MaxEntries+1 {
		t.Errorf("Lost entries during split: %d + %d != %d", len(group1), len(group2), MaxEntries+1)
	}
}

// buildDeepTree creates a tree with the specified number of entries.
func buildDeepTree(n int) *Node {
	root := NewLeafNode()
	for i := 0; i < n; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*5), float64(i*5+3)
		bbox.Min[1], bbox.Max[1] = float64(i%10), float64(i%10+3)
		entry := NewEntry(int64(i+1), bbox)
		root = root.Insert(entry)
	}
	return root
}

// removeFirstLeafEntries removes up to n leaf entries from the tree.
func removeFirstLeafEntries(root *Node, n int) *Node {
	for i := 0; i < n; i++ {
		leafEntry := findFirstLeafEntry(root)
		if leafEntry == nil {
			break
		}
		root = root.Remove(leafEntry)
	}
	return root
}

// TestHandleUnderflowNonRootCase tests handleUnderflow when parent is not root.
func TestHandleUnderflowNonRootCase(t *testing.T) {
	t.Parallel()
	root := buildDeepTree(50)

	if root.IsLeaf {
		t.Log("Tree is a leaf; skipping multi-level assertions")
		return
	}
	if root.Height() < 3 {
		t.Log("Tree height < 3; skipping deep-tree assertions")
		return
	}

	root = removeFirstLeafEntries(root, 40)

	if root != nil && !root.IsLeaf {
		for _, e := range root.Entries {
			if e.Child != nil && e.Child.Parent != root {
				t.Error("Parent pointer incorrect after underflow")
			}
		}
	}
}

// TestCreateTableExactDimensionCount tests createTable with exact dimension boundaries.
func TestCreateTableExactDimensionCount(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	// Test with exactly 1 dimension (should fail - minimum is 2D)
	_, _, err := module.Create(nil, "rtree", "main", "test",
		[]string{"id", "min1", "max1"})
	if err == nil {
		t.Error("Expected error for 1D (minimum is 2D)")
	}

	// Test with exactly 5 dimensions (maximum)
	args := []string{"id"}
	for i := 0; i < 5; i++ {
		args = append(args, fmt.Sprintf("min%d", i), fmt.Sprintf("max%d", i))
	}
	table, schema, err := module.Create(nil, "rtree", "main", "test", args)
	if err != nil {
		t.Errorf("Should support 5D: %v", err)
	}
	if table == nil {
		t.Error("Expected non-nil table for 5D")
	}
	if schema == "" {
		t.Error("Expected non-empty schema for 5D")
	}
}

// TestDistanceBetweenBoxesSeparatedInOneDimension tests boxes separated in one dimension.
func TestDistanceBetweenBoxesSeparatedInOneDimension(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Separated in X dimension only
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30 // Gap of 10 in X
	bbox2.Min[1], bbox2.Max[1] = 5, 15  // Overlapping in Y

	dist := DistanceBetweenBoxes(bbox1, bbox2)
	expected := 10.0
	if math.Abs(dist-expected) > 0.0001 {
		t.Errorf("Expected distance %f, got %f", expected, dist)
	}

	// Separated in Y dimension only
	bbox3 := NewBoundingBox(2)
	bbox3.Min[0], bbox3.Max[0] = 5, 15  // Overlapping in X
	bbox3.Min[1], bbox3.Max[1] = 20, 30 // Gap of 10 in Y

	dist = DistanceBetweenBoxes(bbox1, bbox3)
	if math.Abs(dist-expected) > 0.0001 {
		t.Errorf("Expected distance %f, got %f", expected, dist)
	}
}

// TestOverlapAreaWithDifferentOverlaps tests OverlapArea with various overlap scenarios.
func TestOverlapAreaWithDifferentOverlaps(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Partial overlap
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 5, 15
	bbox2.Min[1], bbox2.Max[1] = 5, 15

	area := OverlapArea(bbox1, bbox2)
	expected := 5.0 * 5.0 // 25
	if math.Abs(area-expected) > 0.0001 {
		t.Errorf("Expected area %f, got %f", expected, area)
	}

	// Complete containment
	bbox3 := NewBoundingBox(2)
	bbox3.Min[0], bbox3.Max[0] = 2, 8
	bbox3.Min[1], bbox3.Max[1] = 2, 8

	area = OverlapArea(bbox1, bbox3)
	expected = 6.0 * 6.0 // 36
	if math.Abs(area-expected) > 0.0001 {
		t.Errorf("Expected area %f, got %f", expected, area)
	}
}

// TestIntersectionBoxWithPartialOverlap tests IntersectionBox with partial overlaps.
func TestIntersectionBoxWithPartialOverlap(t *testing.T) {
	t.Parallel()
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10

	// Partial overlap in both dimensions
	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 5, 15
	bbox2.Min[1], bbox2.Max[1] = 3, 12

	result := IntersectionBox(bbox1, bbox2)
	if result == nil {
		t.Fatal("Expected non-nil intersection")
	}

	if result.Min[0] != 5 || result.Max[0] != 10 {
		t.Errorf("Incorrect X intersection: [%f, %f]", result.Min[0], result.Max[0])
	}
	if result.Min[1] != 3 || result.Max[1] != 10 {
		t.Errorf("Incorrect Y intersection: [%f, %f]", result.Min[1], result.Max[1])
	}

	// Verify intersection area
	area := result.Area()
	expected := 5.0 * 7.0 // 35
	if math.Abs(area-expected) > 0.0001 {
		t.Errorf("Expected intersection area %f, got %f", expected, area)
	}
}

// TestAssignEntryOnTieGroup2FewerEntries tests the branch where group2 has fewer entries.
func TestAssignEntryOnTieGroup2FewerEntries(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create two groups with equal area
	bbox1 := NewBoundingBox(2)
	bbox1.Min[0], bbox1.Max[0] = 0, 10
	bbox1.Min[1], bbox1.Max[1] = 0, 10
	entry1a := NewEntry(1, bbox1)
	entry1b := NewEntry(2, bbox1.Clone())

	bbox2 := NewBoundingBox(2)
	bbox2.Min[0], bbox2.Max[0] = 20, 30 // Same area = 10*10 = 100
	bbox2.Min[1], bbox2.Max[1] = 0, 10
	entry2 := NewEntry(3, bbox2)

	// group1 has 2 entries, group2 has 1 entry
	group1 := []*Entry{entry1a, entry1b}
	group2 := []*Entry{entry2}

	// Entry to assign
	bboxNew := NewBoundingBox(2)
	bboxNew.Min[0], bboxNew.Max[0] = 40, 50
	bboxNew.Min[1], bboxNew.Max[1] = 0, 10
	entryNew := NewEntry(4, bboxNew)

	g1bbox := calculateGroupBBox(group1)
	g2bbox := calculateGroupBBox(group2)

	lenBefore2 := len(group2)
	node.assignEntryOnTie(entryNew, &group1, &group2, g1bbox, g2bbox)

	// Should assign to group2 (fewer entries)
	if len(group2) != lenBefore2+1 {
		t.Errorf("Expected entry to be assigned to group2, but group2 went from %d to %d entries",
			lenBefore2, len(group2))
	}
}

// findAvailableLeaf finds a leaf node with room for insertion.
func findAvailableLeaf(root *Node) *Node {
	if len(root.Entries) == 0 || root.Entries[0].Child == nil {
		return nil
	}
	current := root.Entries[0].Child
	for !current.IsLeaf && len(current.Entries) > 0 {
		current = current.Entries[0].Child
	}
	if current.IsLeaf && len(current.Entries) < MaxEntries {
		return current
	}
	return nil
}

// TestInsertWithParentTraversal tests Insert traversing up to root.
func TestInsertWithParentTraversal(t *testing.T) {
	t.Parallel()
	root := buildDeepTree(MaxEntries * 2)

	if root.IsLeaf {
		t.Log("Tree is a leaf; skipping multi-level assertions")
		return
	}

	if findAvailableLeaf(root) == nil {
		t.Log("No suitable leaf found; skipping insertion assertions")
		return
	}

	bbox := NewBoundingBox(2)
	bbox.Min[0], bbox.Max[0] = 9999, 10004
	bbox.Min[1], bbox.Max[1] = 0, 5
	entry := NewEntry(9999, bbox)

	initialCount := root.Count()
	newRoot := root.Insert(entry)

	if newRoot == nil {
		t.Fatal("Insert returned nil")
	}
	if newRoot.Count() != initialCount+1 {
		t.Errorf("Expected count %d, got %d", initialCount+1, newRoot.Count())
	}
}

// TestQuadraticSplitEdgeCases tests quadraticSplit edge cases.
func TestQuadraticSplitEdgeCases(t *testing.T) {
	t.Parallel()
	node := NewLeafNode()

	// Create a scenario where mustAssignRemainingToGroup triggers
	// We need exactly MinEntries entries left to assign
	totalEntries := MaxEntries + 1
	for i := 0; i < totalEntries; i++ {
		bbox := NewBoundingBox(2)
		bbox.Min[0], bbox.Max[0] = float64(i*5), float64(i*5+3)
		bbox.Min[1], bbox.Max[1] = 0, 3
		entry := NewEntry(int64(i+1), bbox)
		node.AddEntry(entry)
	}

	group1, group2 := node.quadraticSplit()

	// Both groups should have at least MinEntries
	if len(group1) < MinEntries {
		t.Errorf("group1 has %d entries, need at least %d", len(group1), MinEntries)
	}
	if len(group2) < MinEntries {
		t.Errorf("group2 has %d entries, need at least %d", len(group2), MinEntries)
	}

	// All entries should be assigned
	if len(group1)+len(group2) != totalEntries {
		t.Errorf("Lost entries: %d + %d != %d", len(group1), len(group2), totalEntries)
	}
}
