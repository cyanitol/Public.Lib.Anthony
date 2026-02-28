package rtree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
)

// TestRTreeModule tests the R-Tree module creation.
func TestRTreeModule(t *testing.T) {
	module := NewRTreeModule()
	if module == nil {
		t.Fatal("NewRTreeModule returned nil")
	}
}

// TestCreateRTreeTable tests creating an R-Tree virtual table.
func TestCreateRTreeTable(t *testing.T) {
	module := NewRTreeModule()

	tests := []struct {
		name      string
		args      []string
		wantErr   bool
		wantDims  int
	}{
		{
			name:      "2D R-Tree",
			args:      []string{"id", "minX", "maxX", "minY", "maxY"},
			wantErr:   false,
			wantDims:  2,
		},
		{
			name:      "3D R-Tree",
			args:      []string{"id", "minX", "maxX", "minY", "maxY", "minZ", "maxZ"},
			wantErr:   false,
			wantDims:  3,
		},
		{
			name:      "Too few columns",
			args:      []string{"id", "minX", "maxX"},
			wantErr:   true,
		},
		{
			name:      "Odd number of coordinate columns",
			args:      []string{"id", "minX", "maxX", "minY"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, schema, err := module.Create(nil, "rtree", "main", "test_rtree", tt.args)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Create() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Create() unexpected error: %v", err)
				return
			}

			if table == nil {
				t.Error("Create() returned nil table")
				return
			}

			if schema == "" {
				t.Error("Create() returned empty schema")
			}

			rtree, ok := table.(*RTree)
			if !ok {
				t.Error("Create() did not return *RTree")
				return
			}

			if rtree.dimensions != tt.wantDims {
				t.Errorf("Create() dimensions = %d, want %d", rtree.dimensions, tt.wantDims)
			}
		})
	}
}

// TestRTreeInsert tests inserting entries into the R-Tree.
func TestRTreeInsert(t *testing.T) {
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert a rectangle
	args := []interface{}{
		nil,       // old rowid (nil for INSERT)
		nil,       // new rowid (nil for auto-generate)
		0.0,       // minX
		10.0,      // maxX
		0.0,       // minY
		10.0,      // maxY
	}
	id, err := rtree.Update(len(args), args)

	if err != nil {
		t.Errorf("Update() error: %v", err)
	}

	if id != 1 {
		t.Errorf("Update() returned id = %d, want 1", id)
	}

	// Verify the entry was inserted
	if rtree.Count() != 1 {
		t.Errorf("Count() = %d, want 1", rtree.Count())
	}

	entry, exists := rtree.GetEntry(id)
	if !exists {
		t.Error("GetEntry() entry not found")
	}

	if entry.ID != id {
		t.Errorf("Entry ID = %d, want %d", entry.ID, id)
	}

	// Verify bounding box
	if entry.BBox.Min[0] != 0.0 || entry.BBox.Max[0] != 10.0 {
		t.Errorf("Entry X bounds = [%f, %f], want [0, 10]", entry.BBox.Min[0], entry.BBox.Max[0])
	}
	if entry.BBox.Min[1] != 0.0 || entry.BBox.Max[1] != 10.0 {
		t.Errorf("Entry Y bounds = [%f, %f], want [0, 10]", entry.BBox.Min[1], entry.BBox.Max[1])
	}
}

// TestRTreeDelete tests deleting entries from the R-Tree.
func TestRTreeDelete(t *testing.T) {
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

// TestRTreeCursor tests cursor operations.
func TestRTreeCursor(t *testing.T) {
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert entries
	for i := 1; i <= 3; i++ {
		args := []interface{}{
			nil, int64(i),
			float64(i * 10), float64(i*10 + 10),
			float64(i * 10), float64(i*10 + 10),
		}
		_, err := rtree.Update(len(args), args)
		if err != nil {
			t.Fatalf("Update() error: %v", err)
		}
	}

	// Open cursor
	cursor, err := rtree.Open()
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer cursor.Close()

	// Filter (no constraints - return all)
	err = cursor.Filter(0, "", nil)
	if err != nil {
		t.Fatalf("Filter() error: %v", err)
	}

	// Iterate through results
	count := 0
	for !cursor.EOF() {
		// Get rowid
		rowid, err := cursor.Rowid()
		if err != nil {
			t.Errorf("Rowid() error: %v", err)
		}

		// Get columns
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

	if count != 3 {
		t.Errorf("Cursor iterated %d rows, want 3", count)
	}
}

// TestRTreeSearchOverlap tests spatial overlap queries.
func TestRTreeSearchOverlap(t *testing.T) {
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert non-overlapping rectangles
	rects := []struct {
		id   int64
		minX, maxX, minY, maxY float64
	}{
		{1, 0, 10, 0, 10},
		{2, 20, 30, 0, 10},
		{3, 0, 10, 20, 30},
		{4, 20, 30, 20, 30},
		{5, 10, 20, 10, 20}, // Center rectangle
	}

	for _, r := range rects {
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
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert rectangles
	rects := []struct {
		id   int64
		minX, maxX, minY, maxY float64
	}{
		{1, 5, 15, 5, 15},     // Inside query box
		{2, 8, 12, 8, 12},     // Inside query box
		{3, 0, 30, 0, 30},     // Contains query box
		{4, 25, 35, 25, 35},   // Outside query box
	}

	for _, r := range rects {
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
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "test_rtree",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	rtree := table.(*RTree)

	// Insert rectangles
	rects := []struct {
		id   int64
		minX, maxX, minY, maxY float64
	}{
		{1, 0, 10, 0, 10},
		{2, 5, 15, 5, 15},
		{3, 20, 30, 20, 30},
	}

	for _, r := range rects {
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
	rtree := NewLeafNode()

	// Insert some entries
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{40, 40}, Max: []float64{50, 50}}},
	}

	for _, entry := range entries {
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
		0.0, 10.0,  // X
		0.0, 10.0,  // Y
		0.0, 10.0,  // Z
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
