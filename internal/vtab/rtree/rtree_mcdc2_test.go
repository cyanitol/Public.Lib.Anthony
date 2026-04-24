// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"math"
	"testing"
)

// ─── MC/DC: ChooseSubtree tiebreaker ────────────────────────────────────────
// Condition: enlargement < bestEnlargement  (A)
//            || (enlargement == bestEnlargement && e.BBox.Area() < bestArea)  (B && C)
//
// This is an OR of a simple sub-condition (A) and an AND of two sub-conditions (B && C).
// MC/DC requires:
//   A=true,  (B&&C)=false → A alone selects new best entry
//   A=false, (B&&C)=true  → B&&C alone selects new best entry  (only B && C flip needed)
//   A=false, (B&&C)=false → no replacement
//
// For the inner AND (B && C):
//   B=true,  C=true  → inner AND true (covered by the outer A=false case above)
//   B=false, C=true  → inner AND false (equal enlargement but larger area → no swap)
//   B=true,  C=false → equal enlargement but area is NOT smaller → no swap

func TestMCDC_ChooseSubtree_Tiebreaker(t *testing.T) {
	t.Parallel()

	// We build a parent internal node with two child entries and test which child
	// ChooseSubtree selects for a new entry to be inserted.

	// Helper to make a square bounding box.
	makeBBox := func(min, max float64) *BoundingBox {
		return &BoundingBox{Min: []float64{min, min}, Max: []float64{max, max}}
	}

	tests := []struct {
		name            string
		entry0BBox      *BoundingBox // first entry in parent
		entry1BBox      *BoundingBox // second entry in parent
		newEntryBBox    *BoundingBox // the entry being inserted
		wantBestEntryID int          // 0 or 1, which entry should be chosen
		// MC/DC annotation
		aFlip  bool // A flips outcome (enlargement for entry1 < enlargement for entry0)
		bcFlip bool // B&&C flip outcome (equal enlargement, entry1 has smaller area)
	}{
		{
			// A=true: entry1 needs less enlargement than entry0 → entry1 selected
			// Outer A flips: enlargement(entry1) < enlargement(entry0)
			name:            "MCDC_Choose_A1_less_enlargement",
			entry0BBox:      makeBBox(0, 10), // area=100; for new (8,12) → enlargement needed
			entry1BBox:      makeBBox(8, 12), // area=16; already mostly covers new entry
			newEntryBBox:    makeBBox(9, 11), // fits inside entry1 exactly
			wantBestEntryID: 1,
			aFlip:           true,
		},
		{
			// A=false, B=true, C=true: equal enlargement, entry1 has smaller area
			// Both entries have equal enlargement for new entry, but entry1 is smaller.
			name:         "MCDC_Choose_A0_BC1_equal_enl_smaller_area",
			entry0BBox:   makeBBox(0, 10), // area=100
			entry1BBox:   makeBBox(0, 5),  // area=25; same enlargement for (6,7) → both expand same
			newEntryBBox: makeBBox(0, 3),  // inside both; enlargement 0 for both? test picks smaller
			// When both have zero enlargement, smaller area wins → entry1 (area 25) wins over entry0 (area 100)
			wantBestEntryID: 1,
			bcFlip:          true,
		},
		{
			// A=false, B=false: entry1 needs MORE enlargement → entry0 stays best
			name:            "MCDC_Choose_A0_B0_more_enlargement",
			entry0BBox:      makeBBox(0, 10),  // area=100; new (1,9) needs 0 enlargement
			entry1BBox:      makeBBox(20, 30), // area=100; new (1,9) needs lots of enlargement
			newEntryBBox:    makeBBox(1, 9),
			wantBestEntryID: 0,
			aFlip:           false,
			bcFlip:          false,
		},
		{
			// A=false, B=true, C=false: equal enlargement, entry1 has LARGER area → no swap
			// entry0 and entry1 identical enlargement, but entry1 is larger → entry0 stays best
			name:            "MCDC_Choose_A0_B1_C0_equal_enl_larger_area",
			entry0BBox:      makeBBox(0, 5),  // area=25; new (0,3) → 0 enlargement
			entry1BBox:      makeBBox(0, 10), // area=100; new (0,3) → 0 enlargement, larger area
			newEntryBBox:    makeBBox(0, 3),
			wantBestEntryID: 0, // entry0 stays (entry1 has same enlargement but bigger area)
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parent := NewInternalNode()

			child0 := NewLeafNode()
			e0 := &Entry{ID: 0, BBox: tt.entry0BBox, Child: child0}
			parent.AddEntry(e0)

			child1 := NewLeafNode()
			e1 := &Entry{ID: 1, BBox: tt.entry1BBox, Child: child1}
			parent.AddEntry(e1)

			newEntry := &Entry{ID: 99, BBox: tt.newEntryBBox}
			chosen := parent.ChooseSubtree(newEntry)

			if chosen == nil {
				t.Fatalf("ChooseSubtree returned nil")
			}

			var gotID int64
			if chosen == e0 {
				gotID = 0
			} else if chosen == e1 {
				gotID = 1
			} else {
				t.Fatalf("ChooseSubtree returned unknown entry")
			}

			if int(gotID) != tt.wantBestEntryID {
				t.Errorf("ChooseSubtree returned entry %d, want %d", gotID, tt.wantBestEntryID)
			}
		})
	}
}

// ─── MC/DC: NearestNeighborSearch nil/k guard ────────────────────────────────
// Condition: n == nil || k <= 0
//
// Returns nil immediately when either sub-condition is true.
//
// Cases:
//
//	A=true,  B=false → node is nil → returns nil (A flips outcome)
//	A=false, B=true  → node valid, k<=0 → returns nil (B flips outcome)
//	A=false, B=false → proceeds with search
func TestMCDC_NearestNeighborSearch_NilGuard(t *testing.T) {
	t.Parallel()

	leaf := NewLeafNode()
	bbox := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	leaf.AddEntry(&Entry{ID: 1, BBox: bbox})

	tests := []struct {
		name    string
		node    *Node
		k       int
		wantNil bool
	}{
		// A=true: nil node → nil result
		{name: "MCDC_NNS_A1_nil_node", node: nil, k: 1, wantNil: true},
		// A=false, B=true: valid node, k=0 → nil result
		{name: "MCDC_NNS_A0_B1_k_zero", node: leaf, k: 0, wantNil: true},
		// A=false, B=true: valid node, k=-1 → nil result
		{name: "MCDC_NNS_A0_B1_k_negative", node: leaf, k: -1, wantNil: true},
		// A=false, B=false: valid node and k>0 → returns results
		{name: "MCDC_NNS_A0_B0_valid", node: leaf, k: 1, wantNil: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results := tt.node.NearestNeighborSearch([]float64{2, 2}, tt.k)
			if tt.wantNil && results != nil {
				t.Errorf("NearestNeighborSearch() = %v, want nil", results)
			}
			if !tt.wantNil && results == nil {
				t.Errorf("NearestNeighborSearch() = nil, want non-nil")
			}
		})
	}
}

// ─── MC/DC: BoundingBox.Equal inner condition ────────────────────────────────
// Condition per dimension: b.Min[i] != other.Min[i]  (A) || b.Max[i] != other.Max[i]  (B)
// Returns false when A || B is true for any dimension.
//
// Cases:
//
//	A=true,  B=false → Min differs in dimension 0 → not equal
//	A=false, B=true  → Min same, Max differs → not equal
//	A=false, B=false → both same in all dims → equal
func TestMCDC_BoundingBox_Equal(t *testing.T) {
	t.Parallel()

	base := &BoundingBox{Min: []float64{1, 2}, Max: []float64{5, 6}}

	tests := []struct {
		name      string
		otherMin  []float64
		otherMax  []float64
		wantEqual bool
	}{
		// A=false, B=false: identical → equal
		{
			name:      "MCDC_Equal_A0_B0_identical",
			otherMin:  []float64{1, 2},
			otherMax:  []float64{5, 6},
			wantEqual: true,
		},
		// A=true, B=false: Min[0] differs → not equal (A flips outcome)
		{
			name:      "MCDC_Equal_A1_B0_min_differs",
			otherMin:  []float64{0, 2},
			otherMax:  []float64{5, 6},
			wantEqual: false,
		},
		// A=false, B=true: Max[0] differs → not equal (B flips outcome)
		{
			name:      "MCDC_Equal_A0_B1_max_differs",
			otherMin:  []float64{1, 2},
			otherMax:  []float64{4, 6},
			wantEqual: false,
		},
		// Both A and B true for one dim
		{
			name:      "MCDC_Equal_A1_B1_both_differ",
			otherMin:  []float64{0, 2},
			otherMax:  []float64{4, 6},
			wantEqual: false,
		},
		// Failure in second dimension only
		{
			name:      "MCDC_Equal_fail_second_dim_min",
			otherMin:  []float64{1, 0},
			otherMax:  []float64{5, 6},
			wantEqual: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			other := &BoundingBox{Min: tt.otherMin, Max: tt.otherMax}
			got := base.Equal(other)
			if got != tt.wantEqual {
				t.Errorf("Equal() = %v, want %v", got, tt.wantEqual)
			}
		})
	}
}

// ─── MC/DC: pq.less entry-vs-node tiebreaker ────────────────────────────────
// Condition 1: pq.items[i].Entry != nil && pq.items[j].Entry != nil  (A && B)
//   → use entry ID as tiebreaker when both are entries
// Condition 2: pq.items[i].Entry != nil  (C)  (after condition 1 is false)
//   → prefer entry over node when only i has an entry
//
// MC/DC for Condition 1 (A && B):
//   A=true,  B=true  → use ID tiebreaker
//   A=true,  B=false → falls through to Condition 2
//   A=false, B=true  → short-circuits; Condition 2 checked
//
// MC/DC for Condition 2 (C):
//   C=true  → item i preferred (entry over node)
//   C=false → item i NOT preferred (node vs node, or neither has entry)

func TestMCDC_PriorityQueue_less(t *testing.T) {
	t.Parallel()

	node1 := NewLeafNode()

	e1 := &Entry{ID: 10, BBox: &BoundingBox{Min: []float64{0}, Max: []float64{1}}}
	e2 := &Entry{ID: 5, BBox: &BoundingBox{Min: []float64{0}, Max: []float64{1}}}

	tests := []struct {
		name       string
		itemI      *SearchItem
		itemJ      *SearchItem
		wantLessIJ bool // whether less(i, j) should return true
	}{
		// Different distances: i < j → true (basic comparison)
		{
			name:       "MCDC_Less_diff_dist_i_smaller",
			itemI:      &SearchItem{Distance: 1.0, Entry: e1},
			itemJ:      &SearchItem{Distance: 2.0, Entry: e2},
			wantLessIJ: true,
		},
		// Different distances: i > j → false
		{
			name:       "MCDC_Less_diff_dist_i_larger",
			itemI:      &SearchItem{Distance: 3.0, Entry: e1},
			itemJ:      &SearchItem{Distance: 2.0, Entry: e2},
			wantLessIJ: false,
		},
		// Equal distance, A=true, B=true: both entries, e1.ID(10) > e2.ID(5) → i preferred
		{
			name:       "MCDC_Less_A1_B1_both_entries_higher_ID_wins",
			itemI:      &SearchItem{Distance: 1.0, Entry: e1}, // ID=10
			itemJ:      &SearchItem{Distance: 1.0, Entry: e2}, // ID=5
			wantLessIJ: true,                                  // e1.ID(10) > e2.ID(5) → item i less (preferred)
		},
		// Equal distance, A=true, B=true: i has lower ID → not preferred
		{
			name:       "MCDC_Less_A1_B1_both_entries_lower_ID_loses",
			itemI:      &SearchItem{Distance: 1.0, Entry: e2}, // ID=5
			itemJ:      &SearchItem{Distance: 1.0, Entry: e1}, // ID=10
			wantLessIJ: false,                                 // e2.ID(5) < e1.ID(10) → item i not preferred
		},
		// Equal distance, A=true, B=false (j has no entry, is a node): C=true → i (entry) preferred
		{
			name:       "MCDC_Less_A1_B0_C1_entry_vs_node",
			itemI:      &SearchItem{Distance: 1.0, Entry: e1},
			itemJ:      &SearchItem{Distance: 1.0, Node: node1},
			wantLessIJ: true, // entry preferred over node
		},
		// Equal distance, A=false (i has no entry), B=true: C=false → i NOT preferred
		{
			name:       "MCDC_Less_A0_B1_C0_node_vs_entry",
			itemI:      &SearchItem{Distance: 1.0, Node: node1},
			itemJ:      &SearchItem{Distance: 1.0, Entry: e1},
			wantLessIJ: false, // node not preferred over entry
		},
		// Equal distance, both nodes (A=false, B=false, C=false) → false
		{
			name:       "MCDC_Less_A0_B0_both_nodes",
			itemI:      &SearchItem{Distance: 1.0, Node: node1},
			itemJ:      &SearchItem{Distance: 1.0, Node: node1},
			wantLessIJ: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pq := NewPriorityQueue()
			pq.Push(tt.itemI)
			pq.Push(tt.itemJ)
			// less(0, 1) compares items[0] vs items[1]
			// After Push, the heap may reorder; we compare directly via less.
			// We use a fresh pq just to call less().
			pq2 := &PriorityQueue{items: []*SearchItem{tt.itemI, tt.itemJ}}
			got := pq2.less(0, 1)
			if got != tt.wantLessIJ {
				t.Errorf("less(i, j) = %v, want %v (itemI.Entry=%v, itemJ.Entry=%v)",
					got, tt.wantLessIJ, tt.itemI.Entry, tt.itemJ.Entry)
			}
		})
	}
}

// ─── MC/DC: RTree.checkIfUpdate: ok && id != 0 ───────────────────────────────
// Condition: ok  (A) && id != 0  (B)
// Returns (true, id) when both are true; (false, 0) otherwise.
//
// Cases:
//
//	A=true,  B=true  → is update (flips outcome)
//	A=true,  B=false → ok but id==0 → not update (B flips outcome from above)
//	A=false, B=false → cast fails → not update (A flips outcome from A=true,B=true)
func TestMCDC_checkIfUpdate(t *testing.T) {
	t.Parallel()

	rt := newTestTable(t)

	tests := []struct {
		name       string
		oldID      interface{}
		wantUpdate bool
		wantID     int64
	}{
		// A=true, B=true: int64(5) → is update with id=5
		{
			name:       "MCDC_checkIfUpdate_A1_B1_valid_nonzero",
			oldID:      int64(5),
			wantUpdate: true,
			wantID:     5,
		},
		// A=true, B=false: int64(0) → ok but id==0 → not update
		{
			name:       "MCDC_checkIfUpdate_A1_B0_zero_id",
			oldID:      int64(0),
			wantUpdate: false,
			wantID:     0,
		},
		// A=false: nil → cast fails → not update
		{
			name:       "MCDC_checkIfUpdate_A0_nil",
			oldID:      nil,
			wantUpdate: false,
			wantID:     0,
		},
		// A=false: string type → cast fails → not update
		{
			name:       "MCDC_checkIfUpdate_A0_wrong_type",
			oldID:      "notanint",
			wantUpdate: false,
			wantID:     0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotUpdate, gotID := rt.checkIfUpdate(tt.oldID)
			if gotUpdate != tt.wantUpdate {
				t.Errorf("checkIfUpdate(%v) isUpdate = %v, want %v", tt.oldID, gotUpdate, tt.wantUpdate)
			}
			if gotID != tt.wantID {
				t.Errorf("checkIfUpdate(%v) id = %d, want %d", tt.oldID, gotID, tt.wantID)
			}
		})
	}
}

// ─── MC/DC: RTree.determineEntryID: newID == nil || newID == int64(0) ────────
// This mirrors the FTS5 MCDC_DetermineDocumentID test for the R-Tree equivalent.
//
// Cases:
//
//	A=true,  B=false → nil → auto-generate
//	A=false, B=true  → int64(0) → auto-generate
//	A=false, B=false → valid non-zero int64 → use provided id
func TestMCDC_determineEntryID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newID       interface{}
		wantAutoGen bool
		fixedID     int64
	}{
		// A=true, B=false → auto-generate
		{
			name:        "MCDC_EntryID_A1_B0_nil",
			newID:       nil,
			wantAutoGen: true,
		},
		// A=false, B=true → auto-generate
		{
			name:        "MCDC_EntryID_A0_B1_zero",
			newID:       int64(0),
			wantAutoGen: true,
		},
		// A=false, B=false → explicit id
		{
			name:        "MCDC_EntryID_A0_B0_explicit",
			newID:       int64(77),
			wantAutoGen: false,
			fixedID:     77,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rt := newTestTable(t)
			rt.nextID = 1
			id, err := rt.determineEntryID(tt.newID)
			if err != nil {
				t.Fatalf("determineEntryID(%v) unexpected error: %v", tt.newID, err)
			}
			if tt.wantAutoGen {
				if id <= 0 {
					t.Errorf("expected positive auto-generated id, got %d", id)
				}
			} else {
				if id != tt.fixedID {
					t.Errorf("expected explicit id %d, got %d", tt.fixedID, id)
				}
			}
		})
	}
}

// ─── MC/DC: SpatialJoin nil guard ────────────────────────────────────────────
// Condition: node1 == nil || node2 == nil
//
// SpatialJoin returns an empty (non-nil) slice when either node is nil.
//
// Cases:
//
//	A=true,  B=false → node1 nil → empty results
//	A=false, B=true  → node2 nil → empty results
//	A=false, B=false → both valid → proceeds with join
func TestMCDC_SpatialJoin_NilGuard(t *testing.T) {
	t.Parallel()

	// A small leaf node with one entry.
	makeSingleLeaf := func(id int64, minX, maxX float64) *Node {
		n := NewLeafNode()
		n.AddEntry(&Entry{
			ID:   id,
			BBox: &BoundingBox{Min: []float64{minX, 0}, Max: []float64{maxX, 10}},
		})
		return n
	}

	leaf1 := makeSingleLeaf(1, 0, 10)
	leaf2 := makeSingleLeaf(2, 5, 15)

	tests := []struct {
		name      string
		node1     *Node
		node2     *Node
		wantEmpty bool
	}{
		// A=true: node1==nil → empty
		{name: "MCDC_SpatialJoin_A1_node1_nil", node1: nil, node2: leaf2, wantEmpty: true},
		// A=false, B=true: node2==nil → empty
		{name: "MCDC_SpatialJoin_A0_B1_node2_nil", node1: leaf1, node2: nil, wantEmpty: true},
		// A=false, B=false: both valid and overlap → non-empty
		{name: "MCDC_SpatialJoin_A0_B0_both_valid", node1: leaf1, node2: leaf2, wantEmpty: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results := SpatialJoin(tt.node1, tt.node2)
			if tt.wantEmpty && len(results) != 0 {
				t.Errorf("SpatialJoin() = %v, want empty", results)
			}
			if !tt.wantEmpty && len(results) == 0 {
				t.Errorf("SpatialJoin() = empty, want non-empty")
			}
		})
	}
}

// ─── MC/DC: processSpatialJoinPair conditions ────────────────────────────────
// The function has three sequential AND conditions:
//   Cond1: n1.IsLeaf && n2.IsLeaf           (both leaf)
//   Cond2: n1.IsLeaf && !n2.IsLeaf          (n1 leaf, n2 internal)
//   Cond3: !n1.IsLeaf && n2.IsLeaf          (n1 internal, n2 leaf)
//   Default: both internal
//
// Each case independently determines one branch of the function.
// MC/DC coverage:
//   Cond1: IsLeaf(n1)=true  AND IsLeaf(n2)=true  → add pair to results
//   Cond2: IsLeaf(n1)=true  AND IsLeaf(n2)=false → recurse into n2
//   Cond3: IsLeaf(n1)=false AND IsLeaf(n2)=true  → recurse into n1
//   Default: both false → recurse into both children

func TestMCDC_processSpatialJoinPair(t *testing.T) {
	t.Parallel()

	// Helper to build an overlapping leaf for testing.
	leafWithEntry := func(id int64) *Node {
		n := NewLeafNode()
		n.AddEntry(&Entry{
			ID:   id,
			BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
		})
		return n
	}

	// Build an internal node whose single child is a leaf.
	internalWith := func(child *Node) *Node {
		parent := NewInternalNode()
		childEntry := &Entry{
			ID:    0,
			BBox:  &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}},
			Child: child,
		}
		parent.AddEntry(childEntry)
		return parent
	}

	leaf1 := leafWithEntry(1)
	leaf2 := leafWithEntry(2)
	internal1 := internalWith(leaf1)
	internal2 := internalWith(leaf2)

	// Shared entries for the pair calls.
	e1 := &Entry{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}, Child: leaf1}
	e2 := &Entry{ID: 2, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}, Child: leaf2}

	tests := []struct {
		name      string
		n1        *Node
		n2        *Node
		e1        *Entry
		e2        *Entry
		wantPairs int // expected result pair count
	}{
		{
			// Cond1: both leaf → adds one pair directly
			name: "MCDC_SpatialJoin_both_leaf",
			n1:   leaf1, n2: leaf2,
			e1: leaf1.Entries[0], e2: leaf2.Entries[0],
			wantPairs: 1,
		},
		{
			// Cond2: n1 leaf, n2 internal → recurse into n2 child (leaf2) → 1 pair
			name: "MCDC_SpatialJoin_n1_leaf_n2_internal",
			n1:   leaf1, n2: internal2,
			e1: leaf1.Entries[0], e2: e2,
			wantPairs: 1,
		},
		{
			// Cond3: n1 internal, n2 leaf → recurse into n1 child (leaf1) → 1 pair
			name: "MCDC_SpatialJoin_n1_internal_n2_leaf",
			n1:   internal1, n2: leaf2,
			e1: e1, e2: leaf2.Entries[0],
			wantPairs: 1,
		},
		{
			// Default: both internal → recurse into both children → 1 pair
			name: "MCDC_SpatialJoin_both_internal",
			n1:   internal1, n2: internal2,
			e1: e1, e2: e2,
			wantPairs: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results := make([][2]*Entry, 0)
			processSpatialJoinPair(tt.n1, tt.n2, tt.e1, tt.e2, &results)
			if len(results) != tt.wantPairs {
				t.Errorf("processSpatialJoinPair: got %d pairs, want %d", len(results), tt.wantPairs)
			}
		})
	}
}

// ─── MC/DC: Node.SearchWithin inner compound guard ───────────────────────────
// Condition: !n.IsLeaf  (A) && entry.BBox.Overlaps(bbox)  (B)
//
// This is the guard for descending into children when the entry's bbox is not
// fully contained.  Child recursion happens only when both A and B are true.
//
// Cases:
//   A=true,  B=true  → descend into child
//   A=true,  B=false → child bbox doesn't overlap → skip child
//   A=false, B=true  → leaf node → no child to descend
//
// Tested by constructing trees with known layout and verifying result counts.

// buildInternalTree inserts enough entries to force a split producing an internal node.
func buildInternalTree(t *testing.T) *RTree {
	t.Helper()
	rt := newTestTable(t)
	for i := 0; i < MaxEntries+1; i++ {
		base := float64(i * 15)
		insertEntry(t, rt, base, base+10, 0, 10)
	}
	return rt
}

// requireInternalRoot skips if the tree root is not internal.
func requireInternalRoot(t *testing.T, rt *RTree) {
	t.Helper()
	if rt.root == nil || rt.root.IsLeaf {
		t.Skip("tree root is not internal — skipping")
	}
}

func TestMCDC_SearchWithin_innerCompoundGuard(t *testing.T) {
	t.Parallel()

	t.Run("descend", func(t *testing.T) {
		t.Parallel()
		rt := buildInternalTree(t)
		requireInternalRoot(t, rt)
		query := NewBoundingBox(2)
		query.Min[0], query.Max[0] = 0, float64(MaxEntries*15+10)
		query.Min[1], query.Max[1] = 0, 10
		if results := rt.root.SearchWithin(query); len(results) != MaxEntries+1 {
			t.Errorf("got %d results, want %d", len(results), MaxEntries+1)
		}
	})

	t.Run("no_overlap", func(t *testing.T) {
		t.Parallel()
		rt := buildInternalTree(t)
		requireInternalRoot(t, rt)
		query := NewBoundingBox(2)
		query.Min[0], query.Max[0] = 5000, 6000
		query.Min[1], query.Max[1] = 5000, 6000
		if results := rt.root.SearchWithin(query); len(results) != 0 {
			t.Errorf("got %d results, want 0", len(results))
		}
	})

	t.Run("leaf_no_descend", func(t *testing.T) {
		t.Parallel()
		rt := newTestTable(t)
		insertEntry(t, rt, 0, 10, 0, 10)
		if rt.root == nil || !rt.root.IsLeaf {
			t.Skip("expected single-leaf root — skipping")
		}
		query := NewBoundingBox(2)
		query.Min[0], query.Max[0] = 0, 10
		query.Min[1], query.Max[1] = 0, 10
		if results := rt.root.SearchWithin(query); len(results) != 1 {
			t.Errorf("got %d results, want 1", len(results))
		}
	})
}

// ─── MC/DC: RangeSearch internal node guard: entry.Child != nil && dist <= radius ─
// Condition: entry.Child != nil  (A) && dist <= radius  (B)
// Children are only searched when both conditions hold.
//
// Cases:
//   A=true,  B=true  → child is non-nil and within radius → recurse
//   A=true,  B=false → child exists but too far → skip
//   A=false, B=true  → leaf entry within radius → added directly (different path)

func TestMCDC_RangeSearch_ChildGuard(t *testing.T) {
	t.Parallel()

	// Build a tree with enough entries to produce an internal node,
	// then do range searches at various radii.
	rt := newTestTable(t)
	for i := 0; i < MaxEntries+1; i++ {
		base := float64(i * 20)
		insertEntry(t, rt, base, base+5, 0, 5) // entries at x=[0..5], [20..25], [40..45], ...
	}

	if rt.root == nil || rt.root.IsLeaf {
		t.Skip("tree root is not internal — skipping RangeSearch guard test")
	}

	point := []float64{2.5, 2.5} // center of first entry

	tests := []struct {
		name        string
		radius      float64
		wantMinRows int // minimum rows expected
	}{
		// A=true, B=true: radius large enough to reach some children
		{
			name:        "MCDC_RangeSearch_A1_B1_child_in_range",
			radius:      10.0,
			wantMinRows: 1,
		},
		// A=true, B=false: radius too small to reach any child node from origin
		// point is at (2.5, 2.5), first entry is (0..5, 0..5) so dist=0
		// radius=0 still finds first entry (dist=0 <= 0)
		{
			name:        "MCDC_RangeSearch_A1_B0_far_entries_skipped",
			radius:      1.0, // only first entry is within radius
			wantMinRows: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results := rt.root.RangeSearch(point, tt.radius)
			if len(results) < tt.wantMinRows {
				t.Errorf("RangeSearch(radius=%f): got %d results, want at least %d",
					tt.radius, len(results), tt.wantMinRows)
			}
		})
	}
}

// ─── MC/DC: DistanceBetweenBoxes dimension mismatch guard ────────────────────
// Condition: bbox1.Dimensions() != bbox2.Dimensions()
//
// Returns MaxFloat64 when dimensions mismatch (A=true).
//
// Cases:
//
//	A=true  → dimensions differ → MaxFloat64
//	A=false → same dimensions → actual distance computed
func TestMCDC_DistanceBetweenBoxes_DimGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bbox1   *BoundingBox
		bbox2   *BoundingBox
		wantMax bool
	}{
		// A=true: dimension mismatch → MaxFloat64
		{
			name:    "MCDC_DistBetween_A1_dim_mismatch",
			bbox1:   &BoundingBox{Min: []float64{0}, Max: []float64{5}},
			bbox2:   &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}},
			wantMax: true,
		},
		// A=false: same dimensions, non-overlapping → actual positive distance
		{
			name:    "MCDC_DistBetween_A0_same_dims_separated",
			bbox1:   &BoundingBox{Min: []float64{0, 0}, Max: []float64{3, 3}},
			bbox2:   &BoundingBox{Min: []float64{5, 5}, Max: []float64{8, 8}},
			wantMax: false,
		},
		// A=false: same dimensions, overlapping → distance=0
		{
			name:    "MCDC_DistBetween_A0_same_dims_overlapping",
			bbox1:   &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}},
			bbox2:   &BoundingBox{Min: []float64{3, 3}, Max: []float64{8, 8}},
			wantMax: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := DistanceBetweenBoxes(tt.bbox1, tt.bbox2)
			if tt.wantMax && got != math.MaxFloat64 {
				t.Errorf("DistanceBetweenBoxes() = %f, want MaxFloat64", got)
			}
			if !tt.wantMax && got < 0 {
				t.Errorf("DistanceBetweenBoxes() = %f, want >= 0", got)
			}
		})
	}
}
