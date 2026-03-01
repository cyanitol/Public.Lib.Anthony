// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package rtree

import (
	"math"
)

// Insert inserts an entry into the R-Tree and returns the new root.
// This implements the R-Tree insertion algorithm with quadratic split.
func (n *Node) Insert(entry *Entry) *Node {
	// Choose the appropriate leaf node for insertion
	leaf := n.chooseLeaf(entry)

	// Add the entry to the leaf
	leaf.AddEntry(entry)

	// If the leaf overflows, split it
	if len(leaf.Entries) > MaxEntries {
		return leaf.splitNode()
	}

	// Adjust bounding boxes up the tree
	leaf.AdjustBoundingBoxes()

	// Return the root (traverse up from the leaf to find it)
	root := n
	for root.Parent != nil {
		root = root.Parent
	}
	return root
}

// chooseLeaf finds the best leaf node to insert the entry.
func (n *Node) chooseLeaf(entry *Entry) *Node {
	current := n

	// Traverse down the tree until we reach a leaf
	for !current.IsLeaf {
		// Choose the child that needs the least enlargement
		bestChild := current.ChooseSubtree(entry)
		if bestChild == nil || bestChild.Child == nil {
			// This shouldn't happen in a well-formed tree
			break
		}
		current = bestChild.Child
	}

	return current
}

// splitNode splits a node that has overflowed and returns the new root.
// This implements the quadratic split algorithm.
func (n *Node) splitNode() *Node {
	newNode := n.createSplitNode()
	n.updateChildParentPointers()
	newNode.updateChildParentPointers()

	// If this was the root, create a new root
	if n.Parent == nil {
		return n.createNewRootForSplit(newNode)
	}

	return n.handleNonRootSplit(newNode)
}

// createSplitNode creates a new node and distributes entries between nodes.
func (n *Node) createSplitNode() *Node {
	newNode := &Node{
		Entries: make([]*Entry, 0, MaxEntries),
		IsLeaf:  n.IsLeaf,
	}

	// Use quadratic split to distribute entries
	group1, group2 := n.quadraticSplit()

	// Assign entries to the two nodes
	n.Entries = group1
	newNode.Entries = group2

	return newNode
}

// updateChildParentPointers updates parent pointers for all child entries.
func (n *Node) updateChildParentPointers() {
	for _, entry := range n.Entries {
		if entry.Child != nil {
			entry.Child.Parent = n
		}
	}
}

// createNewRootForSplit creates a new root node after splitting the old root.
func (n *Node) createNewRootForSplit(newNode *Node) *Node {
	newRoot := NewInternalNode()

	// Create entries for both nodes
	entry1 := &Entry{
		BBox:  n.BoundingBox(),
		Child: n,
	}
	entry2 := &Entry{
		BBox:  newNode.BoundingBox(),
		Child: newNode,
	}

	newRoot.AddEntry(entry1)
	newRoot.AddEntry(entry2)

	return newRoot
}

// handleNonRootSplit handles splitting a non-root node.
func (n *Node) handleNonRootSplit(newNode *Node) *Node {
	// Insert the new node into the parent
	newEntry := &Entry{
		BBox:  newNode.BoundingBox(),
		Child: newNode,
	}
	n.Parent.AddEntry(newEntry)

	// Update the original entry's bounding box
	n.updateParentEntryBBox()

	// If parent overflows, split it recursively
	if len(n.Parent.Entries) > MaxEntries {
		return n.Parent.splitNode()
	}

	// Adjust bounding boxes up the tree
	n.Parent.AdjustBoundingBoxes()

	return n.findRoot()
}

// updateParentEntryBBox updates the bounding box for this node in the parent.
func (n *Node) updateParentEntryBBox() {
	for _, entry := range n.Parent.Entries {
		if entry.Child == n {
			entry.BBox = n.BoundingBox()
			break
		}
	}
}

// findRoot finds and returns the root node by traversing up the tree.
func (n *Node) findRoot() *Node {
	root := n.Parent
	for root.Parent != nil {
		root = root.Parent
	}
	return root
}

// quadraticSplit implements the quadratic split algorithm.
// It divides entries into two groups to minimize overlap and total area.
func (n *Node) quadraticSplit() ([]*Entry, []*Entry) {
	entries := n.Entries

	// Pick seeds - find the pair of entries with maximum wasted space
	seed1, seed2 := n.pickSeeds(entries)

	group1 := []*Entry{entries[seed1]}
	group2 := []*Entry{entries[seed2]}

	// Track which entries have been assigned
	assigned := make([]bool, len(entries))
	assigned[seed1] = true
	assigned[seed2] = true

	// Count unassigned entries
	countUnassigned := func() int {
		count := 0
		for i := 0; i < len(assigned); i++ {
			if !assigned[i] {
				count++
			}
		}
		return count
	}

	// Assign remaining entries
	for countUnassigned() > 0 {
		// Check if we must assign all remaining entries to one group
		if n.mustAssignRemainingToGroup(countUnassigned(), &group1, &group2, entries, assigned) {
			break
		}

		// Pick next entry - choose the one with maximum preference for one group
		nextIdx := n.pickNext(entries, assigned, group1, group2)
		if nextIdx == -1 {
			break
		}

		// Assign to the best group
		n.assignEntryToGroup(entries[nextIdx], &group1, &group2)
		assigned[nextIdx] = true
	}

	return group1, group2
}

// mustAssignRemainingToGroup checks if we must assign all remaining entries to one group.
func (n *Node) mustAssignRemainingToGroup(remaining int, group1, group2 *[]*Entry, entries []*Entry, assigned []bool) bool {
	// Ensure each group has at least MinEntries
	if len(*group1)+remaining == MinEntries {
		// Must assign all remaining to group1
		n.assignRemainingEntries(entries, assigned, group1)
		return true
	}
	if len(*group2)+remaining == MinEntries {
		// Must assign all remaining to group2
		n.assignRemainingEntries(entries, assigned, group2)
		return true
	}
	return false
}

// assignRemainingEntries assigns all remaining unassigned entries to a group.
func (n *Node) assignRemainingEntries(entries []*Entry, assigned []bool, group *[]*Entry) {
	for j := 0; j < len(entries); j++ {
		if !assigned[j] {
			*group = append(*group, entries[j])
			assigned[j] = true
		}
	}
}

// assignEntryToGroup assigns an entry to the best group based on enlargement.
func (n *Node) assignEntryToGroup(entry *Entry, group1, group2 *[]*Entry) {
	bbox1 := calculateGroupBBox(*group1)
	bbox2 := calculateGroupBBox(*group2)

	enlargement1 := bbox1.EnlargementNeeded(entry.BBox)
	enlargement2 := bbox2.EnlargementNeeded(entry.BBox)

	if enlargement1 < enlargement2 {
		*group1 = append(*group1, entry)
	} else if enlargement2 < enlargement1 {
		*group2 = append(*group2, entry)
	} else {
		// Tie - use tie-breaking logic
		n.assignEntryOnTie(entry, group1, group2, bbox1, bbox2)
	}
}

// assignEntryOnTie assigns an entry when there's a tie in enlargement.
func (n *Node) assignEntryOnTie(entry *Entry, group1, group2 *[]*Entry, bbox1, bbox2 *BoundingBox) {
	// Tie - choose the group with smaller area
	area1 := bbox1.Area()
	area2 := bbox2.Area()

	if area1 < area2 {
		*group1 = append(*group1, entry)
	} else if area2 < area1 {
		*group2 = append(*group2, entry)
	} else {
		// Tie - choose the group with fewer entries
		if len(*group1) <= len(*group2) {
			*group1 = append(*group1, entry)
		} else {
			*group2 = append(*group2, entry)
		}
	}
}

// pickSeeds finds the pair of entries with maximum wasted space.
func (n *Node) pickSeeds(entries []*Entry) (int, int) {
	if len(entries) < 2 {
		return 0, 0
	}

	maxWaste := -1.0
	seed1, seed2 := 0, 1

	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			// Calculate wasted space for this pair
			bbox := entries[i].BBox.Clone()
			bbox.Expand(entries[j].BBox)

			combinedArea := bbox.Area()
			area1 := entries[i].BBox.Area()
			area2 := entries[j].BBox.Area()

			waste := combinedArea - area1 - area2

			if waste > maxWaste {
				maxWaste = waste
				seed1 = i
				seed2 = j
			}
		}
	}

	return seed1, seed2
}

// pickNext finds the next entry to assign during split.
// Returns the index of the entry with maximum difference in enlargement.
func (n *Node) pickNext(entries []*Entry, assigned []bool, group1, group2 []*Entry) int {
	if len(group1) == 0 || len(group2) == 0 {
		// Find first unassigned entry
		for i := 0; i < len(entries); i++ {
			if !assigned[i] {
				return i
			}
		}
		return -1
	}

	bbox1 := calculateGroupBBox(group1)
	bbox2 := calculateGroupBBox(group2)

	maxDiff := -1.0
	bestIdx := -1

	for i := 0; i < len(entries); i++ {
		if assigned[i] {
			continue
		}

		entry := entries[i]
		enlargement1 := bbox1.EnlargementNeeded(entry.BBox)
		enlargement2 := bbox2.EnlargementNeeded(entry.BBox)

		diff := math.Abs(enlargement1 - enlargement2)

		if diff > maxDiff {
			maxDiff = diff
			bestIdx = i
		}
	}

	return bestIdx
}

// calculateGroupBBox calculates the bounding box for a group of entries.
func calculateGroupBBox(entries []*Entry) *BoundingBox {
	if len(entries) == 0 {
		return nil
	}

	bbox := entries[0].BBox.Clone()
	for i := 1; i < len(entries); i++ {
		bbox.Expand(entries[i].BBox)
	}
	return bbox
}

// Remove removes an entry from the R-Tree and returns the new root.
func (n *Node) Remove(entry *Entry) *Node {
	// Find the leaf containing the entry
	leaf, idx := n.FindEntry(entry)
	if leaf == nil {
		// Entry not found
		return n
	}

	// Remove the entry from the leaf
	leaf.Entries = append(leaf.Entries[:idx], leaf.Entries[idx+1:]...)

	// Handle underflow
	if leaf.Parent != nil && leaf.IsUnderflow() {
		return n.handleUnderflow(leaf)
	}

	// Adjust bounding boxes
	if leaf.Parent != nil {
		leaf.AdjustBoundingBoxes()
	}

	return n.handleRootAfterRemoval()
}

// handleRootAfterRemoval handles special cases for the root node after removal.
func (n *Node) handleRootAfterRemoval() *Node {
	// If root is empty after deletion, return nil
	if n.Parent == nil && len(n.Entries) == 0 {
		return nil
	}

	// If root has only one child after deletion, make that child the new root
	if n.Parent == nil && !n.IsLeaf && len(n.Entries) == 1 {
		return n.Entries[0].Child
	}

	return n
}

// handleUnderflow handles node underflow after deletion.
func (n *Node) handleUnderflow(node *Node) *Node {
	// Collect entries from the underflowed node
	orphanedEntries := make([]*Entry, len(node.Entries))
	copy(orphanedEntries, node.Entries)

	// Remove the node from its parent
	parent := node.Parent
	for i, entry := range parent.Entries {
		if entry.Child == node {
			parent.Entries = append(parent.Entries[:i], parent.Entries[i+1:]...)
			break
		}
	}

	// If parent underflows, handle recursively
	if parent.Parent != nil && parent.IsUnderflow() {
		n = n.handleUnderflow(parent)
	} else if parent.Parent != nil {
		parent.AdjustBoundingBoxes()
	}

	// Reinsert orphaned entries
	root := n
	for root.Parent != nil {
		root = root.Parent
	}

	for _, entry := range orphanedEntries {
		root = root.Insert(entry)
	}

	return root
}

// BulkInsert performs bulk insertion of multiple entries.
// This is more efficient than inserting entries one at a time.
func (n *Node) BulkInsert(entries []*Entry) *Node {
	// For simplicity, use sequential insertion
	// A full implementation would use more sophisticated bulk loading algorithms
	// like Sort-Tile-Recursive (STR) or Hilbert R-Tree packing

	root := n
	for _, entry := range entries {
		root = root.Insert(entry)
	}
	return root
}

// Compact reorganizes the tree to improve query performance.
// This rebuilds the tree from scratch using bulk loading.
func Compact(entries []*Entry) *Node {
	if len(entries) == 0 {
		return nil
	}

	// Use STR (Sort-Tile-Recursive) bulk loading
	return strBulkLoad(entries)
}

// strBulkLoad implements Sort-Tile-Recursive bulk loading.
func strBulkLoad(entries []*Entry) *Node {
	if len(entries) == 0 {
		return nil
	}

	// Calculate the number of leaf nodes needed
	leafCount := (len(entries) + MaxEntries - 1) / MaxEntries

	// Determine slice count for each dimension
	// For 2D, we use sqrt(leafCount) slices per dimension
	dimensions := entries[0].BBox.Dimensions()
	slicesPerDim := int(math.Ceil(math.Pow(float64(leafCount), 1.0/float64(dimensions))))

	// Sort and partition recursively
	leaves := strPartition(entries, 0, slicesPerDim)

	// Build the tree bottom-up
	return buildTreeFromLeaves(leaves)
}

// strPartition recursively partitions entries for STR bulk loading.
func strPartition(entries []*Entry, dimension int, slices int) []*Node {
	if len(entries) == 0 {
		return nil
	}

	dimensions := entries[0].BBox.Dimensions()

	// Base case: create leaf nodes
	if dimension >= dimensions {
		leaves := make([]*Node, 0)
		for i := 0; i < len(entries); i += MaxEntries {
			end := i + MaxEntries
			if end > len(entries) {
				end = len(entries)
			}

			leaf := NewLeafNode()
			leaf.Entries = entries[i:end]
			leaves = append(leaves, leaf)
		}
		return leaves
	}

	// Sort by center of bounding box in current dimension
	sortEntriesByDimension(entries, dimension)

	// Partition into slices
	sliceSize := (len(entries) + slices - 1) / slices
	allLeaves := make([]*Node, 0)

	for i := 0; i < len(entries); i += sliceSize {
		end := i + sliceSize
		if end > len(entries) {
			end = len(entries)
		}

		slice := entries[i:end]
		leaves := strPartition(slice, dimension+1, slices)
		allLeaves = append(allLeaves, leaves...)
	}

	return allLeaves
}

// sortEntriesByDimension sorts entries by the center of their bounding box in the given dimension.
func sortEntriesByDimension(entries []*Entry, dimension int) {
	// Simple insertion sort (good for small arrays)
	for i := 1; i < len(entries); i++ {
		key := entries[i]
		keyCenter := (key.BBox.Min[dimension] + key.BBox.Max[dimension]) / 2.0
		j := i - 1

		for j >= 0 {
			jCenter := (entries[j].BBox.Min[dimension] + entries[j].BBox.Max[dimension]) / 2.0
			if jCenter <= keyCenter {
				break
			}
			entries[j+1] = entries[j]
			j--
		}
		entries[j+1] = key
	}
}

// buildTreeFromLeaves builds an R-Tree bottom-up from leaf nodes.
func buildTreeFromLeaves(leaves []*Node) *Node {
	if len(leaves) == 0 {
		return nil
	}

	if len(leaves) == 1 {
		return leaves[0]
	}

	// Create parent level
	parents := make([]*Node, 0)

	for i := 0; i < len(leaves); i += MaxEntries {
		end := i + MaxEntries
		if end > len(leaves) {
			end = len(leaves)
		}

		parent := NewInternalNode()
		for j := i; j < end; j++ {
			entry := &Entry{
				BBox:  leaves[j].BoundingBox(),
				Child: leaves[j],
			}
			parent.AddEntry(entry)
		}

		parents = append(parents, parent)
	}

	// Recursively build upper levels
	return buildTreeFromLeaves(parents)
}
