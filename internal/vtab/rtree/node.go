// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package rtree

import (
	"math"
)

const (
	// MinEntries is the minimum number of entries in a node (except root)
	MinEntries = 2

	// MaxEntries is the maximum number of entries in a node before splitting
	MaxEntries = 8
)

// BoundingBox represents a multi-dimensional bounding box.
type BoundingBox struct {
	Min []float64
	Max []float64
}

// NewBoundingBox creates a new bounding box with the specified number of dimensions.
func NewBoundingBox(dimensions int) *BoundingBox {
	return &BoundingBox{
		Min: make([]float64, dimensions),
		Max: make([]float64, dimensions),
	}
}

// Dimensions returns the number of dimensions in the bounding box.
func (b *BoundingBox) Dimensions() int {
	return len(b.Min)
}

// Area calculates the area (volume in n-dimensions) of the bounding box.
func (b *BoundingBox) Area() float64 {
	if len(b.Min) != len(b.Max) {
		return 0
	}

	area := 1.0
	for i := 0; i < len(b.Min); i++ {
		area *= (b.Max[i] - b.Min[i])
	}
	return area
}

// Perimeter calculates the perimeter (margin in n-dimensions) of the bounding box.
func (b *BoundingBox) Perimeter() float64 {
	if len(b.Min) != len(b.Max) {
		return 0
	}

	perimeter := 0.0
	for i := 0; i < len(b.Min); i++ {
		perimeter += (b.Max[i] - b.Min[i])
	}
	return perimeter
}

// Overlaps checks if this bounding box overlaps with another.
func (b *BoundingBox) Overlaps(other *BoundingBox) bool {
	if len(b.Min) != len(other.Min) {
		return false
	}

	for i := 0; i < len(b.Min); i++ {
		// No overlap if they're separated in any dimension
		if b.Max[i] < other.Min[i] || b.Min[i] > other.Max[i] {
			return false
		}
	}
	return true
}

// Contains checks if this bounding box completely contains another.
func (b *BoundingBox) Contains(other *BoundingBox) bool {
	if len(b.Min) != len(other.Min) {
		return false
	}

	for i := 0; i < len(b.Min); i++ {
		if b.Min[i] > other.Min[i] || b.Max[i] < other.Max[i] {
			return false
		}
	}
	return true
}

// ContainsPoint checks if this bounding box contains a point.
func (b *BoundingBox) ContainsPoint(point []float64) bool {
	if len(b.Min) != len(point) {
		return false
	}

	for i := 0; i < len(b.Min); i++ {
		if point[i] < b.Min[i] || point[i] > b.Max[i] {
			return false
		}
	}
	return true
}

// Expand expands this bounding box to include another bounding box.
func (b *BoundingBox) Expand(other *BoundingBox) {
	if len(b.Min) != len(other.Min) {
		return
	}

	for i := 0; i < len(b.Min); i++ {
		if other.Min[i] < b.Min[i] {
			b.Min[i] = other.Min[i]
		}
		if other.Max[i] > b.Max[i] {
			b.Max[i] = other.Max[i]
		}
	}
}

// EnlargementNeeded calculates the area increase needed to include another box.
func (b *BoundingBox) EnlargementNeeded(other *BoundingBox) float64 {
	if len(b.Min) != len(other.Min) {
		return math.MaxFloat64
	}

	// Calculate the area of the expanded box
	enlarged := b.Clone()
	enlarged.Expand(other)

	return enlarged.Area() - b.Area()
}

// Clone creates a copy of this bounding box.
func (b *BoundingBox) Clone() *BoundingBox {
	clone := NewBoundingBox(len(b.Min))
	copy(clone.Min, b.Min)
	copy(clone.Max, b.Max)
	return clone
}

// Equal checks if two bounding boxes are equal.
func (b *BoundingBox) Equal(other *BoundingBox) bool {
	if len(b.Min) != len(other.Min) {
		return false
	}

	for i := 0; i < len(b.Min); i++ {
		if b.Min[i] != other.Min[i] || b.Max[i] != other.Max[i] {
			return false
		}
	}
	return true
}

// Center calculates the center point of the bounding box.
func (b *BoundingBox) Center() []float64 {
	center := make([]float64, len(b.Min))
	for i := 0; i < len(b.Min); i++ {
		center[i] = (b.Min[i] + b.Max[i]) / 2.0
	}
	return center
}

// Entry represents an entry in the R-Tree (either a data entry or a child node reference).
type Entry struct {
	ID    int64         // ID of the data entry (0 for internal node entries)
	BBox  *BoundingBox  // Bounding box of this entry
	Child *Node         // Child node (nil for leaf entries)
}

// NewEntry creates a new entry with the given bounding box.
func NewEntry(id int64, bbox *BoundingBox) *Entry {
	return &Entry{
		ID:   id,
		BBox: bbox,
	}
}

// IsLeafEntry returns true if this is a leaf entry (data entry).
func (e *Entry) IsLeafEntry() bool {
	return e.Child == nil
}

// Node represents a node in the R-Tree.
type Node struct {
	Entries []*Entry
	IsLeaf  bool
	Parent  *Node
}

// NewLeafNode creates a new leaf node.
func NewLeafNode() *Node {
	return &Node{
		Entries: make([]*Entry, 0, MaxEntries),
		IsLeaf:  true,
	}
}

// NewInternalNode creates a new internal node.
func NewInternalNode() *Node {
	return &Node{
		Entries: make([]*Entry, 0, MaxEntries),
		IsLeaf:  false,
	}
}

// BoundingBox calculates the bounding box that encompasses all entries in this node.
func (n *Node) BoundingBox() *BoundingBox {
	if len(n.Entries) == 0 {
		return nil
	}

	bbox := n.Entries[0].BBox.Clone()
	for i := 1; i < len(n.Entries); i++ {
		bbox.Expand(n.Entries[i].BBox)
	}
	return bbox
}

// IsFull returns true if the node is at maximum capacity.
func (n *Node) IsFull() bool {
	return len(n.Entries) >= MaxEntries
}

// IsUnderflow returns true if the node has too few entries.
func (n *Node) IsUnderflow() bool {
	return len(n.Entries) < MinEntries
}

// AddEntry adds an entry to this node.
func (n *Node) AddEntry(entry *Entry) {
	n.Entries = append(n.Entries, entry)
	if entry.Child != nil {
		entry.Child.Parent = n
	}
}

// RemoveEntry removes an entry from this node.
func (n *Node) RemoveEntry(entry *Entry) bool {
	for i, e := range n.Entries {
		if e == entry {
			n.Entries = append(n.Entries[:i], n.Entries[i+1:]...)
			return true
		}
	}
	return false
}

// ChooseSubtree selects the best child node to insert a new entry.
// Uses the heuristic of choosing the child whose bounding box needs the least enlargement.
func (n *Node) ChooseSubtree(entry *Entry) *Entry {
	if len(n.Entries) == 0 {
		return nil
	}

	// For leaf nodes, choose the entry with minimum enlargement
	// For internal nodes, if all children are leaves, choose minimum enlargement
	// Otherwise, choose the child with minimum overlap increase

	bestEntry := n.Entries[0]
	bestEnlargement := bestEntry.BBox.EnlargementNeeded(entry.BBox)
	bestArea := bestEntry.BBox.Area()

	for i := 1; i < len(n.Entries); i++ {
		e := n.Entries[i]
		enlargement := e.BBox.EnlargementNeeded(entry.BBox)

		// Choose the entry with minimum enlargement
		// In case of tie, choose the one with smaller area
		if enlargement < bestEnlargement ||
			(enlargement == bestEnlargement && e.BBox.Area() < bestArea) {
			bestEntry = e
			bestEnlargement = enlargement
			bestArea = e.BBox.Area()
		}
	}

	return bestEntry
}

// SearchOverlap searches for all entries that overlap with the given bounding box.
func (n *Node) SearchOverlap(bbox *BoundingBox) []*Entry {
	results := make([]*Entry, 0)

	for _, entry := range n.Entries {
		if !entry.BBox.Overlaps(bbox) {
			continue
		}

		if n.IsLeaf {
			// Leaf entry - add to results
			results = append(results, entry)
		} else {
			// Internal node - recursively search child
			childResults := entry.Child.SearchOverlap(bbox)
			results = append(results, childResults...)
		}
	}

	return results
}

// SearchWithin searches for all entries that are completely within the given bounding box.
func (n *Node) SearchWithin(bbox *BoundingBox) []*Entry {
	results := make([]*Entry, 0)

	for _, entry := range n.Entries {
		if !bbox.Contains(entry.BBox) {
			// If the bounding box doesn't contain the entry's bbox,
			// we might still find contained entries in children
			if !n.IsLeaf && entry.BBox.Overlaps(bbox) {
				childResults := entry.Child.SearchWithin(bbox)
				results = append(results, childResults...)
			}
			continue
		}

		if n.IsLeaf {
			// Leaf entry completely within bbox
			results = append(results, entry)
		} else {
			// Internal node - all entries in this subtree are within bbox
			results = append(results, n.getAllLeafEntries(entry.Child)...)
		}
	}

	return results
}

// getAllLeafEntries recursively collects all leaf entries from a subtree.
func (n *Node) getAllLeafEntries(node *Node) []*Entry {
	if node.IsLeaf {
		return node.Entries
	}

	results := make([]*Entry, 0)
	for _, entry := range node.Entries {
		results = append(results, n.getAllLeafEntries(entry.Child)...)
	}
	return results
}

// FindEntry finds an entry in this node and its subtree.
func (n *Node) FindEntry(target *Entry) (*Node, int) {
	for i, entry := range n.Entries {
		if entry == target {
			return n, i
		}
	}

	return n.findEntryInChildren(target)
}

// findEntryInChildren searches for entry in child nodes
func (n *Node) findEntryInChildren(target *Entry) (*Node, int) {
	if n.IsLeaf {
		return nil, -1
	}

	for _, entry := range n.Entries {
		if node, idx := n.searchChildEntry(entry, target); node != nil {
			return node, idx
		}
	}

	return nil, -1
}

// searchChildEntry searches a specific child for the target entry
func (n *Node) searchChildEntry(entry *Entry, target *Entry) (*Node, int) {
	if entry.Child != nil && entry.BBox.Overlaps(target.BBox) {
		return entry.Child.FindEntry(target)
	}
	return nil, -1
}

// AdjustBoundingBoxes updates bounding boxes up the tree after modifications.
func (n *Node) AdjustBoundingBoxes() {
	if n.Parent == nil {
		return
	}

	// Find this node's entry in the parent
	for _, entry := range n.Parent.Entries {
		if entry.Child == n {
			// Recalculate the bounding box for this entry
			newBBox := n.BoundingBox()
			if newBBox != nil {
				entry.BBox = newBBox
			}
			break
		}
	}

	// Propagate up the tree
	n.Parent.AdjustBoundingBoxes()
}

// Height returns the height of the tree rooted at this node.
func (n *Node) Height() int {
	if n.IsLeaf {
		return 1
	}

	if len(n.Entries) == 0 {
		return 1
	}

	// All children should have the same height
	return 1 + n.Entries[0].Child.Height()
}

// Count returns the number of leaf entries in the tree rooted at this node.
func (n *Node) Count() int {
	if n.IsLeaf {
		return len(n.Entries)
	}

	count := 0
	for _, entry := range n.Entries {
		if entry.Child != nil {
			count += entry.Child.Count()
		}
	}
	return count
}
