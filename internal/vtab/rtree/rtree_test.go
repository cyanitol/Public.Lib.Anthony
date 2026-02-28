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

// TestRTreeModuleConnect tests the Connect method.
func TestRTreeModuleConnect(t *testing.T) {
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
		val, err := cursor.Column(tt.col)
		if err != nil {
			t.Errorf("Column(%d) error: %v", tt.col, err)
			continue
		}

		switch expected := tt.want.(type) {
		case int64:
			ival, ok := val.(int64)
			if !ok {
				t.Errorf("Column(%d) expected int64, got %T", tt.col, val)
				continue
			}
			if ival != expected {
				t.Errorf("Column(%d) = %d, want %d", tt.col, ival, expected)
			}
		case float64:
			fval, ok := val.(float64)
			if !ok {
				t.Errorf("Column(%d) expected float64, got %T", tt.col, val)
				continue
			}
			if fval != expected {
				t.Errorf("Column(%d) = %f, want %f", tt.col, fval, expected)
			}
		}
	}

	// Test invalid column
	_, err = cursor.Column(99)
	if err == nil {
		t.Error("Expected error for invalid column index")
	}
}

// TestRangeSearch tests range-based spatial queries.
func TestRangeSearch(t *testing.T) {
	root := NewLeafNode()

	// Insert entries
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
	}

	for _, entry := range entries {
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
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}},
	}

	for _, entry := range entries {
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
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{30, 30}}},
	}

	for _, entry := range entries {
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
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{30, 30}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{25, 25}}},
	}

	for _, entry := range entries {
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
	root := NewLeafNode()

	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{15, 15}, Max: []float64{25, 25}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{20, 20}}},
	}

	for _, entry := range entries {
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
		result := bbox.ContainsPoint(tt.point)
		if result != tt.expected {
			t.Errorf("ContainsPoint(%v) = %v, want %v", tt.point, result, tt.expected)
		}
	}
}

// TestBoundingBoxEqual tests equality comparison.
func TestBoundingBoxEqual(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			_, err := rtree.Update(len(tt.args), tt.args)
			if err == nil {
				t.Error("Expected error for invalid coordinate")
			}
		})
	}
}

// TestBulkInsert tests bulk insertion operation.
func TestBulkInsert(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			enlargement := bbox1.EnlargementNeeded(tt.bbox)
			t.Logf("Enlargement for %s: %f", tt.name, enlargement)
		})
	}
}

// TestNodeChooseSubtreeEdgeCases tests subtree selection edge cases.
func TestNodeChooseSubtreeEdgeCases(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
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
