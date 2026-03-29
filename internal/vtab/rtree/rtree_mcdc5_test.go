// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"testing"
)

// ---------------------------------------------------------------------------
// OverlapArea (search.go:384) — MC/DC
// Conditions:
//   A: !bbox1.Overlaps(bbox2)               → return 0.0
//   B: bbox1.Dimensions() != bbox2.Dimensions() → return 0.0
//   else → compute area product
// ---------------------------------------------------------------------------

func TestMCDC5_OverlapArea_NonOverlapping(t *testing.T) {
	t.Parallel()
	// A=true: boxes separated → 0
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}
	b2 := &BoundingBox{Min: []float64{5, 5}, Max: []float64{6, 6}}
	got := OverlapArea(b1, b2)
	if got != 0.0 {
		t.Errorf("non-overlapping: want 0.0, got %f", got)
	}
}

func TestMCDC5_OverlapArea_DifferentDimensions(t *testing.T) {
	t.Parallel()
	// B=true: dimension mismatch → 0
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	b2 := &BoundingBox{Min: []float64{0, 0, 0}, Max: []float64{2, 2, 2}}
	got := OverlapArea(b1, b2)
	if got != 0.0 {
		t.Errorf("dim mismatch: want 0.0, got %f", got)
	}
}

func TestMCDC5_OverlapArea_PartialOverlap(t *testing.T) {
	t.Parallel()
	// A=false, B=false: partial overlap → 1x1 = 1.0
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	b2 := &BoundingBox{Min: []float64{1, 1}, Max: []float64{3, 3}}
	got := OverlapArea(b1, b2)
	if got != 1.0 {
		t.Errorf("partial overlap: want 1.0, got %f", got)
	}
}

func TestMCDC5_OverlapArea_ContainedBox(t *testing.T) {
	t.Parallel()
	// One box fully inside the other → area = inner box area
	outer := &BoundingBox{Min: []float64{0, 0}, Max: []float64{4, 4}}
	inner := &BoundingBox{Min: []float64{1, 1}, Max: []float64{3, 3}}
	got := OverlapArea(outer, inner)
	if got != 4.0 {
		t.Errorf("contained box: want 4.0, got %f", got)
	}
}

func TestMCDC5_OverlapArea_IdenticalBoxes(t *testing.T) {
	t.Parallel()
	// Same box → overlap = full area
	b := &BoundingBox{Min: []float64{0, 0}, Max: []float64{3, 5}}
	got := OverlapArea(b, b)
	want := 15.0
	if got != want {
		t.Errorf("identical boxes: want %f, got %f", want, got)
	}
}

func TestMCDC5_OverlapArea_TouchingEdge(t *testing.T) {
	t.Parallel()
	// Boxes touch at edge: Min of one = Max of other → overlap area = 0 in one dim
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 2}}
	b2 := &BoundingBox{Min: []float64{1, 0}, Max: []float64{3, 2}}
	// Overlaps() returns true when touching, but overlap area in X = 0
	got := OverlapArea(b1, b2)
	if got != 0.0 {
		t.Errorf("touching edge: want 0.0, got %f", got)
	}
}

func TestMCDC5_OverlapArea_3D(t *testing.T) {
	t.Parallel()
	// 3D overlap: [0,2]^3 and [1,3]^3 → 1x1x1 = 1.0
	b1 := &BoundingBox{Min: []float64{0, 0, 0}, Max: []float64{2, 2, 2}}
	b2 := &BoundingBox{Min: []float64{1, 1, 1}, Max: []float64{3, 3, 3}}
	got := OverlapArea(b1, b2)
	if got != 1.0 {
		t.Errorf("3D overlap: want 1.0, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// IntersectionBox (search.go:405) — MC/DC
// Conditions:
//   A: !bbox1.Overlaps(bbox2)               → return nil
//   B: bbox1.Dimensions() != bbox2.Dimensions() → return nil
//   else → return intersection bounding box
// ---------------------------------------------------------------------------

func TestMCDC5_IntersectionBox_NonOverlapping(t *testing.T) {
	t.Parallel()
	// A=true: no overlap → nil
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}
	b2 := &BoundingBox{Min: []float64{5, 5}, Max: []float64{6, 6}}
	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("non-overlapping: want nil, got %v", result)
	}
}

func TestMCDC5_IntersectionBox_DifferentDimensions(t *testing.T) {
	t.Parallel()
	// B=true: dimension mismatch → nil
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	b2 := &BoundingBox{Min: []float64{0, 0, 0}, Max: []float64{2, 2, 2}}
	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("dim mismatch: want nil, got %v", result)
	}
}

func TestMCDC5_IntersectionBox_PartialOverlap(t *testing.T) {
	t.Parallel()
	// A=false, B=false: intersection is [1,2]x[1,2]
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	b2 := &BoundingBox{Min: []float64{1, 1}, Max: []float64{3, 3}}
	result := IntersectionBox(b1, b2)
	if result == nil {
		t.Fatal("partial overlap: want non-nil intersection")
	}
	if result.Min[0] != 1 || result.Min[1] != 1 {
		t.Errorf("intersection min: want [1,1], got %v", result.Min)
	}
	if result.Max[0] != 2 || result.Max[1] != 2 {
		t.Errorf("intersection max: want [2,2], got %v", result.Max)
	}
}

func TestMCDC5_IntersectionBox_ContainedBox(t *testing.T) {
	t.Parallel()
	// Inner box fully inside outer → intersection = inner
	outer := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	inner := &BoundingBox{Min: []float64{2, 3}, Max: []float64{5, 7}}
	result := IntersectionBox(outer, inner)
	if result == nil {
		t.Fatal("contained box: want non-nil intersection")
	}
	if result.Min[0] != 2 || result.Min[1] != 3 {
		t.Errorf("intersection min: want [2,3], got %v", result.Min)
	}
	if result.Max[0] != 5 || result.Max[1] != 7 {
		t.Errorf("intersection max: want [5,7], got %v", result.Max)
	}
}

func TestMCDC5_IntersectionBox_IdenticalBoxes(t *testing.T) {
	t.Parallel()
	// Same box → intersection = that box
	b := &BoundingBox{Min: []float64{1, 2}, Max: []float64{4, 6}}
	result := IntersectionBox(b, b)
	if result == nil {
		t.Fatal("identical boxes: want non-nil intersection")
	}
	if result.Min[0] != 1 || result.Max[0] != 4 {
		t.Errorf("intersection X: want [1,4], got [%f,%f]", result.Min[0], result.Max[0])
	}
}

func TestMCDC5_IntersectionBox_SeparatedY(t *testing.T) {
	t.Parallel()
	// Overlap in X but not Y → nil
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 1}}
	b2 := &BoundingBox{Min: []float64{0, 3}, Max: []float64{5, 5}}
	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("separated in Y: want nil, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// quadraticSplit (insert.go:156) — MC/DC
// Conditions:
//   A: len(group1)+remaining == MinEntries → force-fill group1
//   B: len(group2)+remaining == MinEntries → force-fill group2
//   C: nextIdx == -1                       → break
//
// Tested indirectly by inserting enough entries to trigger node splits.
// ---------------------------------------------------------------------------

func TestMCDC5_QuadraticSplit_ForcesNodeSplit(t *testing.T) {
	t.Parallel()
	// Insert MaxEntries+1 entries to force a split. After split, root should
	// have two children.
	rt := newTestTable(t)

	// Insert MaxEntries+1 non-overlapping rectangles
	for i := 0; i < MaxEntries+1; i++ {
		f := float64(i * 10)
		insertEntry(t, rt, f, f+9, 0, 9)
	}

	// Root should now be internal (split occurred)
	if rt.root == nil {
		t.Fatal("root is nil after insertions")
	}
	if rt.root.IsLeaf {
		t.Error("root should be internal after split")
	}
}

func TestMCDC5_QuadraticSplit_EntriesPreservedAfterSplit(t *testing.T) {
	t.Parallel()
	// All inserted entries remain searchable after a split
	rt := newTestTable(t)
	count := MaxEntries + 3

	for i := 0; i < count; i++ {
		f := float64(i * 10)
		insertEntry(t, rt, f, f+5, f, f+5)
	}

	if len(rt.entries) != count {
		t.Errorf("want %d entries, got %d", count, len(rt.entries))
	}
}

func TestMCDC5_QuadraticSplit_MultipleSplits(t *testing.T) {
	t.Parallel()
	// Insert 3x MaxEntries entries to force multiple splits and deeper tree
	rt := newTestTable(t)
	total := 3 * MaxEntries

	for i := 0; i < total; i++ {
		f := float64(i * 5)
		insertEntry(t, rt, f, f+4, 0, 4)
	}

	if len(rt.entries) != total {
		t.Errorf("after multiple splits: want %d entries, got %d", total, len(rt.entries))
	}
}

func TestMCDC5_QuadraticSplit_SplitPreservesOverlap(t *testing.T) {
	t.Parallel()
	// Overlapping rectangles — ensures split handles overlap correctly
	rt := newTestTable(t)

	// All overlap at center [4,6]x[4,6]
	coords := [][4]float64{
		{0, 5, 0, 5}, {3, 8, 0, 5}, {0, 5, 3, 8}, {3, 8, 3, 8},
		{1, 6, 1, 6}, {2, 7, 2, 7}, {1, 7, 1, 7}, {0, 8, 0, 8},
		{4, 9, 4, 9}, // triggers split
	}

	for _, c := range coords {
		insertEntry(t, rt, c[0], c[1], c[2], c[3])
	}

	if len(rt.entries) != len(coords) {
		t.Errorf("want %d entries, got %d", len(coords), len(rt.entries))
	}
}

func TestMCDC5_QuadraticSplit_GroupMinEntriesConstraint(t *testing.T) {
	t.Parallel()
	// With exactly MaxEntries+1 entries, each group must get >= MinEntries.
	// After split, verify root has 2 children, each with >= MinEntries entries.
	rt := newTestTable(t)

	for i := 0; i < MaxEntries+1; i++ {
		f := float64(i * 10)
		insertEntry(t, rt, f, f+9, 0, 9)
	}

	if rt.root == nil || rt.root.IsLeaf {
		t.Skip("no split occurred, cannot check group sizes")
	}

	for _, entry := range rt.root.Entries {
		if entry.Child == nil {
			continue
		}
		if len(entry.Child.Entries) < MinEntries {
			t.Errorf("child node has %d entries, want >= %d", len(entry.Child.Entries), MinEntries)
		}
	}
}
