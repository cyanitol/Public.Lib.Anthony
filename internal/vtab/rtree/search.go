package rtree

import (
	"math"
)

// SearchResult represents a search result with distance or score.
type SearchResult struct {
	Entry    *Entry
	Distance float64
}

// NearestNeighborSearch finds the k nearest entries to the given point.
// This uses a priority queue-based algorithm for efficient k-NN search.
func (n *Node) NearestNeighborSearch(point []float64, k int) []*Entry {
	if n == nil || k <= 0 {
		return nil
	}

	// Priority queue for search (min-heap by distance)
	pq := NewPriorityQueue()

	// Initialize with root node
	rootDist := n.minDistanceToPoint(point)
	pq.Push(&SearchItem{
		Node:     n,
		Distance: rootDist,
	})

	results := make([]*Entry, 0, k)

	for pq.Len() > 0 {
		item := pq.Pop()

		// If this is a leaf node entry
		if item.Entry != nil {
			results = append(results, item.Entry)
			if len(results) >= k {
				break
			}
			continue
		}

		// Expand node
		node := item.Node
		if node.IsLeaf {
			// Add all leaf entries to priority queue
			for _, entry := range node.Entries {
				dist := distanceToPoint(entry.BBox, point)
				pq.Push(&SearchItem{
					Entry:    entry,
					Distance: dist,
				})
			}
		} else {
			// Add child nodes to priority queue
			for _, entry := range node.Entries {
				dist := entry.Child.minDistanceToPoint(point)
				pq.Push(&SearchItem{
					Node:     entry.Child,
					Distance: dist,
				})
			}
		}
	}

	return results
}

// minDistanceToPoint calculates the minimum distance from this node's bounding box to a point.
func (n *Node) minDistanceToPoint(point []float64) float64 {
	if len(n.Entries) == 0 {
		return math.MaxFloat64
	}

	bbox := n.BoundingBox()
	return distanceToPoint(bbox, point)
}

// distanceToPoint calculates the minimum distance from a bounding box to a point.
func distanceToPoint(bbox *BoundingBox, point []float64) float64 {
	if len(bbox.Min) != len(point) {
		return math.MaxFloat64
	}

	sumSquares := 0.0
	for i := 0; i < len(point); i++ {
		if point[i] < bbox.Min[i] {
			diff := bbox.Min[i] - point[i]
			sumSquares += diff * diff
		} else if point[i] > bbox.Max[i] {
			diff := point[i] - bbox.Max[i]
			sumSquares += diff * diff
		}
		// If point[i] is within [bbox.Min[i], bbox.Max[i]], distance in this dimension is 0
	}

	return math.Sqrt(sumSquares)
}

// RangeSearch finds all entries within a given distance from a point.
func (n *Node) RangeSearch(point []float64, radius float64) []*Entry {
	if n == nil {
		return nil
	}

	results := make([]*Entry, 0)

	for _, entry := range n.Entries {
		dist := distanceToPoint(entry.BBox, point)

		// Check if this entry is within range
		if n.IsLeaf {
			if dist <= radius {
				results = append(results, entry)
			}
		} else {
			// For internal nodes, search children if they might contain results
			if entry.Child != nil && dist <= radius {
				childResults := entry.Child.RangeSearch(point, radius)
				results = append(results, childResults...)
			}
		}
	}

	return results
}

// IntersectionSearch finds all entries whose bounding boxes intersect with the query box.
// This is similar to SearchOverlap but with more detailed intersection checking.
func (n *Node) IntersectionSearch(bbox *BoundingBox) []*Entry {
	return n.SearchOverlap(bbox)
}

// ContainmentSearch finds all entries that are completely contained within the query box.
// This is an alias for SearchWithin for clarity.
func (n *Node) ContainmentSearch(bbox *BoundingBox) []*Entry {
	return n.SearchWithin(bbox)
}

// EnclosureSearch finds all entries that completely enclose the query box.
func (n *Node) EnclosureSearch(bbox *BoundingBox) []*Entry {
	results := make([]*Entry, 0)

	for _, entry := range n.Entries {
		if !entry.BBox.Overlaps(bbox) {
			continue
		}

		if n.IsLeaf {
			// Check if this entry's bbox contains the query bbox
			if entry.BBox.Contains(bbox) {
				results = append(results, entry)
			}
		} else {
			// Recursively search children
			if entry.Child != nil {
				childResults := entry.Child.EnclosureSearch(bbox)
				results = append(results, childResults...)
			}
		}
	}

	return results
}

// WindowQuery performs a rectangular window query.
// Returns all entries that overlap with the given window.
func (n *Node) WindowQuery(min, max []float64) []*Entry {
	if len(min) != len(max) {
		return nil
	}

	bbox := NewBoundingBox(len(min))
	copy(bbox.Min, min)
	copy(bbox.Max, max)

	return n.SearchOverlap(bbox)
}

// SearchItem represents an item in the priority queue for nearest neighbor search.
type SearchItem struct {
	Entry    *Entry
	Node     *Node
	Distance float64
}

// PriorityQueue implements a min-heap for nearest neighbor search.
type PriorityQueue struct {
	items []*SearchItem
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		items: make([]*SearchItem, 0),
	}
}

// Len returns the number of items in the queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// Push adds an item to the queue.
func (pq *PriorityQueue) Push(item *SearchItem) {
	pq.items = append(pq.items, item)
	pq.bubbleUp(len(pq.items) - 1)
}

// Pop removes and returns the item with minimum distance.
func (pq *PriorityQueue) Pop() *SearchItem {
	if len(pq.items) == 0 {
		return nil
	}

	item := pq.items[0]
	lastIdx := len(pq.items) - 1

	if lastIdx > 0 {
		pq.items[0] = pq.items[lastIdx]
		pq.items = pq.items[:lastIdx]
		pq.bubbleDown(0)
	} else {
		pq.items = pq.items[:0]
	}

	return item
}

// bubbleUp moves an item up the heap to maintain heap property.
func (pq *PriorityQueue) bubbleUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !pq.less(idx, parent) {
			break
		}
		pq.items[idx], pq.items[parent] = pq.items[parent], pq.items[idx]
		idx = parent
	}
}

// less compares two items in the priority queue.
// Returns true if item at index i should come before item at index j.
func (pq *PriorityQueue) less(i, j int) bool {
	if pq.items[i].Distance < pq.items[j].Distance {
		return true
	}
	if pq.items[i].Distance > pq.items[j].Distance {
		return false
	}
	// Distances are equal - use entry ID as tiebreaker (prefer higher ID)
	if pq.items[i].Entry != nil && pq.items[j].Entry != nil {
		return pq.items[i].Entry.ID > pq.items[j].Entry.ID
	}
	// If one is an entry and one is a node, prefer the entry
	if pq.items[i].Entry != nil {
		return true
	}
	return false
}

// bubbleDown moves an item down the heap to maintain heap property.
func (pq *PriorityQueue) bubbleDown(idx int) {
	for {
		left := 2*idx + 1
		right := 2*idx + 2
		smallest := idx

		if left < len(pq.items) && pq.less(left, smallest) {
			smallest = left
		}
		if right < len(pq.items) && pq.less(right, smallest) {
			smallest = right
		}

		if smallest == idx {
			break
		}

		pq.items[idx], pq.items[smallest] = pq.items[smallest], pq.items[idx]
		idx = smallest
	}
}

// SpatialJoin performs a spatial join between two R-Trees.
// Returns pairs of entries from tree1 and tree2 that overlap.
func SpatialJoin(node1, node2 *Node) [][2]*Entry {
	results := make([][2]*Entry, 0)

	if node1 == nil || node2 == nil {
		return results
	}

	spatialJoinRecursive(node1, node2, &results)
	return results
}

// spatialJoinRecursive is the recursive helper for spatial join.
func spatialJoinRecursive(n1, n2 *Node, results *[][2]*Entry) {
	// Check all pairs of entries
	for _, e1 := range n1.Entries {
		for _, e2 := range n2.Entries {
			if !e1.BBox.Overlaps(e2.BBox) {
				continue
			}
			processSpatialJoinPair(n1, n2, e1, e2, results)
		}
	}
}

// processSpatialJoinPair processes a single pair of entries in spatial join.
func processSpatialJoinPair(n1, n2 *Node, e1, e2 *Entry, results *[][2]*Entry) {
	// Both are leaf entries - add to results
	if n1.IsLeaf && n2.IsLeaf {
		*results = append(*results, [2]*Entry{e1, e2})
		return
	}

	// n1 is leaf, n2 is internal - recurse into n2
	if n1.IsLeaf && !n2.IsLeaf {
		spatialJoinRecursive(n1, e2.Child, results)
		return
	}

	// n1 is internal, n2 is leaf - recurse into n1
	if !n1.IsLeaf && n2.IsLeaf {
		spatialJoinRecursive(e1.Child, n2, results)
		return
	}

	// Both are internal - recurse into both
	spatialJoinRecursive(e1.Child, e2.Child, results)
}

// DistanceBetweenBoxes calculates the minimum distance between two bounding boxes.
func DistanceBetweenBoxes(bbox1, bbox2 *BoundingBox) float64 {
	if bbox1.Dimensions() != bbox2.Dimensions() {
		return math.MaxFloat64
	}

	sumSquares := 0.0
	for i := 0; i < bbox1.Dimensions(); i++ {
		// Calculate distance in this dimension
		var dist float64
		if bbox1.Max[i] < bbox2.Min[i] {
			dist = bbox2.Min[i] - bbox1.Max[i]
		} else if bbox2.Max[i] < bbox1.Min[i] {
			dist = bbox1.Min[i] - bbox2.Max[i]
		}
		// else boxes overlap in this dimension, distance is 0

		sumSquares += dist * dist
	}

	return math.Sqrt(sumSquares)
}

// OverlapArea calculates the overlapping area between two bounding boxes.
func OverlapArea(bbox1, bbox2 *BoundingBox) float64 {
	if !bbox1.Overlaps(bbox2) {
		return 0.0
	}

	if bbox1.Dimensions() != bbox2.Dimensions() {
		return 0.0
	}

	area := 1.0
	for i := 0; i < bbox1.Dimensions(); i++ {
		overlapMin := math.Max(bbox1.Min[i], bbox2.Min[i])
		overlapMax := math.Min(bbox1.Max[i], bbox2.Max[i])
		area *= (overlapMax - overlapMin)
	}

	return area
}

// IntersectionBox calculates the bounding box of the intersection of two boxes.
// Returns nil if the boxes don't overlap.
func IntersectionBox(bbox1, bbox2 *BoundingBox) *BoundingBox {
	if !bbox1.Overlaps(bbox2) {
		return nil
	}

	if bbox1.Dimensions() != bbox2.Dimensions() {
		return nil
	}

	result := NewBoundingBox(bbox1.Dimensions())
	for i := 0; i < bbox1.Dimensions(); i++ {
		result.Min[i] = math.Max(bbox1.Min[i], bbox2.Min[i])
		result.Max[i] = math.Min(bbox1.Max[i], bbox2.Max[i])
	}

	return result
}

// UnionBox calculates the bounding box that encompasses both boxes.
func UnionBox(bbox1, bbox2 *BoundingBox) *BoundingBox {
	if bbox1.Dimensions() != bbox2.Dimensions() {
		return nil
	}

	result := bbox1.Clone()
	result.Expand(bbox2)
	return result
}
