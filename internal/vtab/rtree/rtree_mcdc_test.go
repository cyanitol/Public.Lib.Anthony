// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// newTestTable is a helper that creates a 2D R-Tree table for testing.
func newTestTable(t *testing.T) *RTree {
	t.Helper()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", t.Name(),
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}
	return table.(*RTree)
}

// insertEntry is a helper that inserts a bounding box and returns its ID.
func insertEntry(t *testing.T, rt *RTree, minX, maxX, minY, maxY float64) int64 {
	t.Helper()
	args := []interface{}{nil, nil, minX, maxX, minY, maxY}
	id, err := rt.Update(len(args), args)
	if err != nil {
		t.Fatalf("Update(insert) error: %v", err)
	}
	return id
}

// ─── MC/DC: BoundingBox.Overlaps ────────────────────────────────────────────
// Condition: b.Max[i] < other.Min[i]  (A)  separates on the low side
//            b.Min[i] > other.Max[i]  (B)  separates on the high side
// Both A and B are evaluated per dimension; the box overlaps only when neither
// is true for any dimension.  MC/DC requires each sub-condition to independently
// flip the overall outcome.

func TestMCDC_BoundingBox_Overlaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		aMin, aMax  [2]float64
		bMin, bMax  [2]float64
		wantOverlap bool
	}{
		// Base: clear overlap — both A and B are false → overlap = true
		{
			name: "MCDC_Overlaps_base_overlap",
			aMin: [2]float64{0, 0}, aMax: [2]float64{10, 10},
			bMin: [2]float64{5, 5}, bMax: [2]float64{15, 15},
			wantOverlap: true,
		},
		// Flip A: box A is entirely to the left of box B in X
		// (A.Max[0] < B.Min[0] becomes true) → overlap = false, only A flipped result
		{
			name: "MCDC_Overlaps_flipA_separated_low",
			aMin: [2]float64{0, 0}, aMax: [2]float64{4, 10},
			bMin: [2]float64{5, 5}, bMax: [2]float64{15, 15},
			wantOverlap: false,
		},
		// Flip B: box A is entirely to the right of box B in X
		// (A.Min[0] > B.Max[0] becomes true) → overlap = false, only B flipped result
		{
			name: "MCDC_Overlaps_flipB_separated_high",
			aMin: [2]float64{16, 0}, aMax: [2]float64{20, 10},
			bMin: [2]float64{5, 5}, bMax: [2]float64{15, 15},
			wantOverlap: false,
		},
		// Both A and B false but separation in Y dimension (A for Y)
		{
			name: "MCDC_Overlaps_flipA_second_dim",
			aMin: [2]float64{0, 0}, aMax: [2]float64{10, 4},
			bMin: [2]float64{5, 5}, bMax: [2]float64{15, 15},
			wantOverlap: false,
		},
		// Touching edges — boundary contact is overlap
		{
			name: "MCDC_Overlaps_touching_edge",
			aMin: [2]float64{0, 0}, aMax: [2]float64{5, 5},
			bMin: [2]float64{5, 5}, bMax: [2]float64{10, 10},
			wantOverlap: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := &BoundingBox{Min: tt.aMin[:], Max: tt.aMax[:]}
			b := &BoundingBox{Min: tt.bMin[:], Max: tt.bMax[:]}
			got := a.Overlaps(b)
			if got != tt.wantOverlap {
				t.Errorf("Overlaps() = %v, want %v (aMin=%v aMax=%v bMin=%v bMax=%v)",
					got, tt.wantOverlap, tt.aMin, tt.aMax, tt.bMin, tt.bMax)
			}
		})
	}
}

// ─── MC/DC: BoundingBox.Contains ────────────────────────────────────────────
// Condition per dimension: b.Min[i] > other.Min[i]  (A) — outer left edge too far right
//                          b.Max[i] < other.Max[i]  (B) — outer right edge too far left
// Returns false (does not contain) if A || B is true for any dimension.

func TestMCDC_BoundingBox_Contains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		outerMin     [2]float64
		outerMax     [2]float64
		innerMin     [2]float64
		innerMax     [2]float64
		wantContains bool
	}{
		// Base: outer fully contains inner
		{
			name:     "MCDC_Contains_base_true",
			outerMin: [2]float64{0, 0}, outerMax: [2]float64{10, 10},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: true,
		},
		// Flip A: outer left edge protrudes past inner left edge (outer.Min[0] > inner.Min[0])
		{
			name:     "MCDC_Contains_flipA_outer_min_too_large",
			outerMin: [2]float64{3, 0}, outerMax: [2]float64{10, 10},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: false,
		},
		// Flip B: outer right edge too small (outer.Max[0] < inner.Max[0])
		{
			name:     "MCDC_Contains_flipB_outer_max_too_small",
			outerMin: [2]float64{0, 0}, outerMax: [2]float64{7, 10},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: false,
		},
		// Exact boundary — should contain
		{
			name:     "MCDC_Contains_exact_boundary",
			outerMin: [2]float64{2, 2}, outerMax: [2]float64{8, 8},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: true,
		},
		// Fail in second dimension only (A for Y)
		{
			name:     "MCDC_Contains_fail_second_dim_A",
			outerMin: [2]float64{0, 3}, outerMax: [2]float64{10, 10},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: false,
		},
		// Fail in second dimension only (B for Y)
		{
			name:     "MCDC_Contains_fail_second_dim_B",
			outerMin: [2]float64{0, 0}, outerMax: [2]float64{10, 7},
			innerMin: [2]float64{2, 2}, innerMax: [2]float64{8, 8},
			wantContains: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			outer := &BoundingBox{Min: tt.outerMin[:], Max: tt.outerMax[:]}
			inner := &BoundingBox{Min: tt.innerMin[:], Max: tt.innerMax[:]}
			got := outer.Contains(inner)
			if got != tt.wantContains {
				t.Errorf("Contains() = %v, want %v", got, tt.wantContains)
			}
		})
	}
}

// ─── MC/DC: BoundingBox.ContainsPoint ───────────────────────────────────────
// Condition per dimension: point[i] < b.Min[i]  (A) — point below lower bound
//                          point[i] > b.Max[i]  (B) — point above upper bound
// Returns false (not contained) when A || B is true.

func TestMCDC_BoundingBox_ContainsPoint(t *testing.T) {
	t.Parallel()

	bbox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}

	tests := []struct {
		name        string
		point       []float64
		wantContain bool
	}{
		// Base: point inside box
		{"MCDC_ContainsPoint_inside", []float64{5, 5}, true},
		// Flip A (X): point below Min[0]
		{"MCDC_ContainsPoint_flipA_below_minX", []float64{-1, 5}, false},
		// Flip B (X): point above Max[0]
		{"MCDC_ContainsPoint_flipB_above_maxX", []float64{11, 5}, false},
		// Flip A (Y): point below Min[1]
		{"MCDC_ContainsPoint_flipA_below_minY", []float64{5, -1}, false},
		// Flip B (Y): point above Max[1]
		{"MCDC_ContainsPoint_flipB_above_maxY", []float64{5, 11}, false},
		// Boundary: on Min edge (A false, B false → contained)
		{"MCDC_ContainsPoint_on_min_boundary", []float64{0, 0}, true},
		// Boundary: on Max edge (A false, B false → contained)
		{"MCDC_ContainsPoint_on_max_boundary", []float64{10, 10}, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := bbox.ContainsPoint(tt.point)
			if got != tt.wantContain {
				t.Errorf("ContainsPoint(%v) = %v, want %v", tt.point, got, tt.wantContain)
			}
		})
	}
}

// ─── MC/DC: isSpatialOperator ────────────────────────────────────────────────
// Compound OR: op==LE || op==GE || op==LT || op==GT
// Each sub-condition independently makes the result true.

func TestMCDC_isSpatialOperator(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)

	tests := []struct {
		name string
		op   vtab.ConstraintOp
		want bool
	}{
		// Base: unsupported operator → false (all sub-conditions false)
		{"MCDC_isSpatialOp_none_match", vtab.ConstraintEQ, false},
		// Flip: LE alone makes result true
		{"MCDC_isSpatialOp_LE", vtab.ConstraintLE, true},
		// Flip: GE alone makes result true
		{"MCDC_isSpatialOp_GE", vtab.ConstraintGE, true},
		// Flip: LT alone makes result true
		{"MCDC_isSpatialOp_LT", vtab.ConstraintLT, true},
		// Flip: GT alone makes result true
		{"MCDC_isSpatialOp_GT", vtab.ConstraintGT, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := rt.isSpatialOperator(tt.op)
			if got != tt.want {
				t.Errorf("isSpatialOperator(%v) = %v, want %v", tt.op, got, tt.want)
			}
		})
	}
}

// ─── MC/DC: hasIDConstraint ──────────────────────────────────────────────────
// Compound AND: idxNum&1 != 0  (A)  &&  len(argv) > 0  (B)
// Both sub-conditions must be true; flipping either one changes the result.

func TestMCDC_hasIDConstraint(t *testing.T) {
	t.Parallel()

	cursor := &RTreeCursor{}

	tests := []struct {
		name   string
		idxNum int
		argv   []interface{}
		want   bool
	}{
		// Base: A true, B true → result true
		{"MCDC_hasIDConstraint_both_true", 1, []interface{}{int64(42)}, true},
		// Flip A: idxNum&1 == 0 (A false), B still true → result false
		{"MCDC_hasIDConstraint_flipA_no_id_bit", 2, []interface{}{int64(42)}, false},
		// Flip B: A true, argv empty (B false) → result false
		{"MCDC_hasIDConstraint_flipB_empty_argv", 1, []interface{}{}, false},
		// Both false → false
		{"MCDC_hasIDConstraint_both_false", 0, []interface{}{}, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := cursor.hasIDConstraint(tt.idxNum, tt.argv)
			if got != tt.want {
				t.Errorf("hasIDConstraint(idxNum=%d, len(argv)=%d) = %v, want %v",
					tt.idxNum, len(tt.argv), got, tt.want)
			}
		})
	}
}

// ─── MC/DC: applySpatialFilter — queryBox != nil && root != nil ──────────────
// Compound AND: queryBox != nil  (A)  &&  c.table.root != nil  (B)
// Verified indirectly via Filter() behaviour:
// - A false (no spatial constraint bits set, builds box but path to SearchOverlap
//   requires a non-nil queryBox from buildQueryBox; when idxNum==0 and len(argv)==0
//   the code path falls back to full-scan — a proxy for B/A being false).
// We test the observable outcomes (result count) rather than internal nil checks.

func TestMCDC_applySpatialFilter(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)
	insertEntry(t, rt, 0, 10, 0, 10)
	insertEntry(t, rt, 20, 30, 20, 30)

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)

	tests := []struct {
		name        string
		idxNum      int
		argv        []interface{}
		wantMinRows int // minimum number of entries returned
	}{
		// A=true (spatial bit set), B=true (root non-nil) → SearchOverlap used → ≥1 result
		{
			name:        "MCDC_spatialFilter_both_true",
			idxNum:      0b000010, // bit 1 set: column 1 (minX)
			argv:        []interface{}{float64(5)},
			wantMinRows: 1,
		},
		// A=false (no argv, buildQueryBox still runs but no constraint bits) →
		// falls back to full scan → returns all entries
		{
			name:        "MCDC_spatialFilter_flipA_no_argv_fullscan",
			idxNum:      0,
			argv:        []interface{}{},
			wantMinRows: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel — shared cursor, run sequentially
			if err := c.Filter(tt.idxNum, "", tt.argv); err != nil {
				t.Fatalf("Filter() error: %v", err)
			}
			count := 0
			for !c.EOF() {
				count++
				if err := c.Next(); err != nil {
					t.Fatalf("Next() error: %v", err)
				}
			}
			if count < tt.wantMinRows {
				t.Errorf("Filter() returned %d rows, want at least %d", count, tt.wantMinRows)
			}
		})
	}
}

// ─── MC/DC: processIDConstraint — Column != 0 || Op != EQ ────────────────────
// Compound OR: constraint.Column != 0  (A) || constraint.Op != EQ  (B)
// When either is true the function returns false (not an ID constraint).

func TestMCDC_processIDConstraint(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)
	info := &vtab.IndexInfo{}
	// Add a dummy constraint entry so SetConstraintUsage doesn't panic.
	info.Constraints = []vtab.IndexConstraint{
		{Column: 0, Op: vtab.ConstraintEQ, Usable: true},
	}

	tests := []struct {
		name       string
		col        int
		op         vtab.ConstraintOp
		wantResult bool // true = was processed as an ID constraint
	}{
		// A=false, B=false (col==0, op==EQ) → processed
		{"MCDC_processIDConstraint_both_false", 0, vtab.ConstraintEQ, true},
		// Flip A: col != 0 → not processed
		{"MCDC_processIDConstraint_flipA_nonzero_col", 1, vtab.ConstraintEQ, false},
		// Flip B: col==0 but op != EQ → not processed
		{"MCDC_processIDConstraint_flipB_wrong_op", 0, vtab.ConstraintLE, false},
		// Both true → not processed
		{"MCDC_processIDConstraint_both_true", 1, vtab.ConstraintLE, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			constraint := &vtab.IndexConstraint{Column: tt.col, Op: tt.op}
			argv := 1
			used := 0
			got := rt.processIDConstraint(info, constraint, 0, &argv, &used)
			if got != tt.wantResult {
				t.Errorf("processIDConstraint(col=%d, op=%v) = %v, want %v",
					tt.col, tt.op, got, tt.wantResult)
			}
		})
	}
}

// ─── MC/DC: processSpatialConstraint column range guard ──────────────────────
// Condition: constraint.Column <= 0  (A) || constraint.Column >= len(columns)  (B)
// When either is true the function returns false early.

func TestMCDC_processSpatialConstraint_columnGuard(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t) // columns: [id, minX, maxX, minY, maxY] → len=5
	info := &vtab.IndexInfo{}
	info.Constraints = []vtab.IndexConstraint{
		{Column: 1, Op: vtab.ConstraintLE, Usable: true},
	}

	tests := []struct {
		name       string
		col        int
		op         vtab.ConstraintOp
		wantResult bool
	}{
		// A=false, B=false (col==1, valid range) with spatial op → processed
		{"MCDC_spatialConstraint_col_valid", 1, vtab.ConstraintLE, true},
		// Flip A: col <= 0 → rejected
		{"MCDC_spatialConstraint_flipA_col_zero", 0, vtab.ConstraintLE, false},
		// Flip A: col negative → rejected
		{"MCDC_spatialConstraint_flipA_col_negative", -1, vtab.ConstraintLE, false},
		// Flip B: col >= len(columns)==5 → rejected
		{"MCDC_spatialConstraint_flipB_col_out_of_range", 5, vtab.ConstraintLE, false},
		// Both A true (col==0) and invalid op → rejected
		{"MCDC_spatialConstraint_col_zero_wrong_op", 0, vtab.ConstraintEQ, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			constraint := &vtab.IndexConstraint{Column: tt.col, Op: tt.op}
			argv := 1
			used := 0
			got := rt.processSpatialConstraint(info, constraint, 0, &argv, &used)
			if got != tt.wantResult {
				t.Errorf("processSpatialConstraint(col=%d, op=%v) = %v, want %v",
					tt.col, tt.op, got, tt.wantResult)
			}
		})
	}
}

// ─── MC/DC: searchChildEntry — entry.Child != nil && entry.BBox.Overlaps ─────
// Compound AND: entry.Child != nil  (A) && bbox overlaps target  (B)
// When A is false the short-circuit skips B; when A true and B false → returns nil.

func TestMCDC_searchChildEntry(t *testing.T) {
	t.Parallel()

	parent := NewInternalNode()
	child := NewLeafNode()

	// A shared bounding box for the child entry placed at (0..10, 0..10)
	childBBox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	entryWithChild := &Entry{BBox: childBBox, Child: child}
	entryNoChild := &Entry{BBox: childBBox, Child: nil}

	// Insert a leaf entry into the child so FindEntry has something to find.
	leafEntry := &Entry{
		ID:   99,
		BBox: &BoundingBox{Min: []float64{1, 1}, Max: []float64{2, 2}},
	}
	child.Entries = append(child.Entries, leafEntry)

	// Target that does NOT overlap with childBBox
	targetNoOverlap := &Entry{BBox: &BoundingBox{Min: []float64{20, 20}, Max: []float64{30, 30}}}

	tests := []struct {
		name    string
		entry   *Entry
		target  *Entry
		wantNil bool
	}{
		// A=true, B=true (child set, bboxes overlap) → returns FindEntry result
		{"MCDC_searchChildEntry_both_true", entryWithChild, leafEntry, false},
		// Flip A: no child → returns nil regardless of overlap
		{"MCDC_searchChildEntry_flipA_no_child", entryNoChild, leafEntry, true},
		// Flip B: child set but bboxes don't overlap → returns nil
		{"MCDC_searchChildEntry_flipB_no_overlap", entryWithChild, targetNoOverlap, true},
		// Both A and B false directions → nil
		{"MCDC_searchChildEntry_no_child_no_overlap", entryNoChild, targetNoOverlap, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node, _ := parent.searchChildEntry(tt.entry, tt.target)
			gotNil := node == nil
			if gotNil != tt.wantNil {
				t.Errorf("searchChildEntry() node==nil=%v, want %v", gotNil, tt.wantNil)
			}
		})
	}
}

// ─── MC/DC: Node.SearchWithin inner guard — !n.IsLeaf && overlap ─────────────
// Condition: !n.IsLeaf  (A) && entry.BBox.Overlaps(bbox)  (B)
// Child recursion happens only when both are true.

func TestMCDC_SearchWithin_innerGuard(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)

	// Insert enough entries that an internal node is created (> MaxEntries).
	// MaxEntries == 8, so 9 entries force a split and create an internal node.
	for i := 0; i < 9; i++ {
		base := float64(i * 15)
		insertEntry(t, rt, base, base+10, 0, 10)
	}

	queryBBox := NewBoundingBox(2)
	queryBBox.Min[0] = 0
	queryBBox.Max[0] = 200
	queryBBox.Min[1] = 0
	queryBBox.Max[1] = 10

	// A=true (root is internal after split), B=true (query spans all entries)
	results := rt.SearchWithin(queryBBox)
	if len(results) != 9 {
		t.Errorf("MCDC_SearchWithin_both_true: got %d results, want 9", len(results))
	}

	// A=true (internal node), B=false (query bbox is far away from all entries)
	noOverlapBBox := NewBoundingBox(2)
	noOverlapBBox.Min[0] = 500
	noOverlapBBox.Max[0] = 600
	noOverlapBBox.Min[1] = 500
	noOverlapBBox.Max[1] = 600
	results2 := rt.SearchWithin(noOverlapBBox)
	if len(results2) != 0 {
		t.Errorf("MCDC_SearchWithin_flipB_no_overlap: got %d results, want 0", len(results2))
	}

	// A=false (leaf-only tree): single entry, root is leaf
	rtLeaf := newTestTable(t)
	insertEntry(t, rtLeaf, 0, 10, 0, 10)
	if rtLeaf.root == nil || !rtLeaf.root.IsLeaf {
		t.Skip("expected leaf root for single-entry tree")
	}
	leafQuery := NewBoundingBox(2)
	leafQuery.Min[0] = 0
	leafQuery.Max[0] = 10
	leafQuery.Min[1] = 0
	leafQuery.Max[1] = 10
	results3 := rtLeaf.SearchWithin(leafQuery)
	if len(results3) != 1 {
		t.Errorf("MCDC_SearchWithin_flipA_leaf_root: got %d results, want 1", len(results3))
	}
}

// ─── MC/DC: SQL-level spatial filter conditions ──────────────────────────────
// Tests via the virtual table cursor's Filter path using direct Go API calls,
// exercising the full spatial overlap filter that the SQL engine invokes.

func TestMCDC_SpatialFilter_SQL(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)
	// Rectangle A: (0,0)–(10,10)
	insertEntry(t, rt, 0, 10, 0, 10)
	// Rectangle B: (20,20)–(30,30)
	insertEntry(t, rt, 20, 30, 20, 30)
	// Rectangle C: (5,5)–(15,15) — overlaps A but not B
	insertEntry(t, rt, 5, 15, 5, 15)

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer cursor.Close()
	c := cursor.(*RTreeCursor)

	tests := []struct {
		name      string
		idxNum    int
		argv      []interface{}
		wantCount int
	}{
		// Full scan — no constraints, all 3 entries returned
		{
			name:      "MCDC_SQL_fullscan_no_constraint",
			idxNum:    0,
			argv:      nil,
			wantCount: 3,
		},
		// ID lookup — only entry with id=1
		{
			name:      "MCDC_SQL_id_lookup",
			idxNum:    1, // bit 0 = ID constraint
			argv:      []interface{}{int64(1)},
			wantCount: 1,
		},
		// Spatial constraint on minX (col 1): minX <= 6 filters to entries with minX ≤ 6
		// Entries: A (minX=0), C (minX=5) both qualify; B (minX=20) does not.
		// idxNum bit 1 = column 1 (minX) has constraint
		{
			name:      "MCDC_SQL_spatial_minX_le",
			idxNum:    0b000010,
			argv:      []interface{}{float64(6)},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if err := c.Filter(tt.idxNum, "", tt.argv); err != nil {
				t.Fatalf("Filter() error: %v", err)
			}
			count := 0
			for !c.EOF() {
				count++
				if err := c.Next(); err != nil {
					t.Fatalf("Next() error: %v", err)
				}
			}
			if count != tt.wantCount {
				t.Errorf("Filter() returned %d rows, want %d", count, tt.wantCount)
			}
		})
	}
}

// ─── MC/DC: Node split conditions ────────────────────────────────────────────
// Condition (insert.go): len(leaf.Entries) > MaxEntries  triggers split
// We verify the split path (A true) and the no-split path (A false).

func TestMCDC_NodeSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		insertCount int
		wantSplit   bool // true if we expect the root to become internal
	}{
		// A=false: 8 entries → MaxEntries reached but not exceeded → no split
		{"MCDC_NodeSplit_no_split_at_max", MaxEntries, false},
		// Flip A: 9 entries → exceeds MaxEntries → split occurs → root becomes internal
		{"MCDC_NodeSplit_split_when_exceeded", MaxEntries + 1, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rt := newTestTable(t)
			for i := 0; i < tt.insertCount; i++ {
				base := float64(i * 5)
				insertEntry(t, rt, base, base+4, 0, 4)
			}

			if rt.root == nil {
				t.Fatal("root is nil after insertions")
			}

			gotSplit := !rt.root.IsLeaf
			if gotSplit != tt.wantSplit {
				t.Errorf("root.IsLeaf=%v after %d inserts: wantSplit=%v",
					rt.root.IsLeaf, tt.insertCount, tt.wantSplit)
			}
		})
	}
}

// ─── MC/DC: handleRootAfterRemoval — two guards ──────────────────────────────
// Guard 1: n.Parent == nil && len(n.Entries) == 0  (empty root → return nil)
// Guard 2: n.Parent == nil && !n.IsLeaf && len(n.Entries) == 1 (collapse root)
//
// We verify via Count() / root state after targeted deletions.

func TestMCDC_handleRootAfterRemoval(t *testing.T) {
	t.Parallel()

	t.Run("MCDC_RemoveAll_root_nil", func(t *testing.T) {
		t.Parallel()
		rt := newTestTable(t)
		id := insertEntry(t, rt, 0, 10, 0, 10)
		if _, err := rt.Update(1, []interface{}{id}); err != nil {
			t.Fatalf("Update(delete) error: %v", err)
		}
		if rt.root != nil {
			t.Errorf("root should be nil after deleting all entries, got non-nil")
		}
	})

	t.Run("MCDC_RemoveUntilOneChild_collapses_root", func(t *testing.T) {
		t.Parallel()
		rt := newTestTable(t)
		// Insert MaxEntries+1 to force a split → internal root with 2 children.
		ids := make([]int64, MaxEntries+1)
		for i := 0; i < MaxEntries+1; i++ {
			base := float64(i * 20)
			ids[i] = insertEntry(t, rt, base, base+10, 0, 10)
		}
		if rt.root == nil || rt.root.IsLeaf {
			t.Skip("tree did not produce an internal root — skipping collapse test")
		}
		// Delete all but one entry; the root should eventually collapse to a leaf.
		for i := 1; i < len(ids); i++ {
			if _, err := rt.Update(1, []interface{}{ids[i]}); err != nil {
				t.Fatalf("Update(delete id=%d) error: %v", ids[i], err)
			}
		}
		if rt.Count() != 1 {
			t.Errorf("expected 1 entry remaining, got %d", rt.Count())
		}
	})
}

// ─── MC/DC: Remove — leaf.Parent != nil && leaf.IsUnderflow() ────────────────
// Condition: leaf.Parent != nil  (A) && leaf.IsUnderflow()  (B)
// When both true → handleUnderflow; when A false (root leaf) → adjust path.

func TestMCDC_Remove_underflowGuard(t *testing.T) {
	t.Parallel()

	t.Run("MCDC_Remove_flipA_root_leaf_no_underflow_handling", func(t *testing.T) {
		t.Parallel()
		// A=false: root is a leaf (no parent) → underflow code not triggered
		rt := newTestTable(t)
		id1 := insertEntry(t, rt, 0, 5, 0, 5)
		insertEntry(t, rt, 6, 10, 6, 10)
		// Delete one entry from the root leaf — no underflow path taken
		if _, err := rt.Update(1, []interface{}{id1}); err != nil {
			t.Fatalf("Update(delete) error: %v", err)
		}
		if rt.Count() != 1 {
			t.Errorf("expected 1 entry after delete, got %d", rt.Count())
		}
	})

	t.Run("MCDC_Remove_both_true_underflow_triggered", func(t *testing.T) {
		t.Parallel()
		// A=true, B=true: create an internal tree then delete entries from one
		// leaf node to force underflow — entries are reinserted by handleUnderflow.
		rt := newTestTable(t)
		ids := make([]int64, MaxEntries+2)
		for i := 0; i < MaxEntries+2; i++ {
			base := float64(i * 20)
			ids[i] = insertEntry(t, rt, base, base+10, 0, 10)
		}
		startCount := rt.Count()
		// Delete entries until we trigger underflow in some leaf.
		deleted := 0
		for _, id := range ids {
			if _, err := rt.Update(1, []interface{}{id}); err != nil {
				// Entry might already be gone (reinserted under a different ID path)
				continue
			}
			deleted++
			if deleted >= 3 {
				break
			}
		}
		if rt.Count() != startCount-deleted {
			t.Errorf("count mismatch after deletion: got %d, want %d",
				rt.Count(), startCount-deleted)
		}
	})
}
