// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// RTreeModule implements the R-Tree virtual table module for spatial indexing.
// R-Tree is a data structure for spatial access methods, used to index
// multi-dimensional information such as geographical coordinates, rectangles, or polygons.
type RTreeModule struct {
	vtab.BaseModule
}

// NewRTreeModule creates a new R-Tree module.
func NewRTreeModule() *RTreeModule {
	return &RTreeModule{}
}

// Create creates a new R-Tree virtual table.
// Syntax: CREATE VIRTUAL TABLE name USING rtree(id, minX, maxX, minY, maxY, ...)
// The first column must be an integer primary key.
// Each coordinate pair (minX, maxX) represents one dimension.
func (m *RTreeModule) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.createTable(db, moduleName, dbName, tableName, args)
}

// Connect connects to an existing R-Tree virtual table.
func (m *RTreeModule) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.createTable(db, moduleName, dbName, tableName, args)
}

// createTable creates or connects to an R-Tree table.
func (m *RTreeModule) createTable(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	columns, err := parseRTreeColumns(args)
	if err != nil {
		return nil, "", err
	}

	dimensions, err := validateRTreeDimensions(columns)
	if err != nil {
		return nil, "", err
	}

	schema := fmt.Sprintf("CREATE TABLE %s(%s)", tableName, strings.Join(columns, ", ")) // nosec: tableName and columns are from validated CREATE VIRTUAL TABLE DDL, not user input

	// Create shadow table manager if db supports persistence
	var shadowMgr *ShadowTableManager
	if dbExec, ok := db.(DatabaseExecutor); ok {
		shadowMgr = NewShadowTableManager(tableName, dbExec, dimensions)
		if err := shadowMgr.CreateShadowTables(); err != nil {
			shadowMgr = nil
		}
	}

	table := &RTree{
		tableName:  tableName,
		columns:    columns,
		idColumn:   columns[0],
		dimensions: dimensions,
		root:       nil,
		entries:    make(map[int64]*Entry),
		nextID:     1,
		shadowMgr:  shadowMgr,
	}

	// Load persisted entries if available
	if shadowMgr != nil {
		table.loadFromShadowTables()
	}

	return table, schema, nil
}

// parseRTreeColumns parses and validates the column arguments
func parseRTreeColumns(args []string) ([]string, error) {
	if len(args) < 5 {
		return nil, fmt.Errorf("R-Tree table requires at least 5 columns (id, minX, maxX, minY, maxY)")
	}

	columns := make([]string, 0, len(args))
	for _, arg := range args {
		colName := strings.TrimSpace(arg)
		if colName != "" {
			columns = append(columns, colName)
		}
	}

	if len(columns) < 5 {
		return nil, fmt.Errorf("R-Tree table requires at least 5 columns")
	}

	return columns, nil
}

// validateRTreeDimensions validates the coordinate columns and returns dimensions
func validateRTreeDimensions(columns []string) (int, error) {
	coordColumns := columns[1:]
	if len(coordColumns)%2 != 0 {
		return 0, fmt.Errorf("R-Tree coordinate columns must come in min/max pairs")
	}

	dimensions := len(coordColumns) / 2
	if dimensions < 1 || dimensions > 5 {
		return 0, fmt.Errorf("R-Tree supports 1-5 dimensions, got %d", dimensions)
	}

	return dimensions, nil
}

// RTree represents an R-Tree virtual table instance.
// It implements spatial indexing for efficient range and overlap queries.
type RTree struct {
	vtab.BaseVirtualTable

	mu         sync.RWMutex
	tableName  string
	columns    []string
	idColumn   string
	dimensions int

	// R-Tree structure
	root    *Node
	entries map[int64]*Entry // Maps ID to entry for quick lookup
	nextID  int64

	// Persistence layer for shadow tables
	shadowMgr *ShadowTableManager
}

// loadFromShadowTables reconstructs the R-Tree from persisted shadow tables.
func (t *RTree) loadFromShadowTables() {
	entries, err := t.shadowMgr.LoadEntries()
	if err != nil || len(entries) == 0 {
		return
	}

	t.entries = entries

	// Rebuild the R-Tree from loaded entries
	for _, entry := range entries {
		if t.root == nil {
			t.root = NewLeafNode()
		}
		t.root = t.root.Insert(entry)
	}

	// Load the next ID counter
	if nextID, err := t.shadowMgr.LoadNextID(); err == nil {
		t.nextID = nextID
	}

	// Ensure nextID is greater than any loaded entry ID
	for id := range entries {
		if id >= t.nextID {
			t.nextID = id + 1
		}
	}
}

// BestIndex analyzes the query and determines the best index strategy.
// For R-Tree, we look for spatial constraints like range queries and overlaps.
func (t *RTree) BestIndex(info *vtab.IndexInfo) error {
	argvIndex := 1
	usedConstraints := 0

	for i, constraint := range info.Constraints {
		if !constraint.Usable {
			continue
		}

		if t.processConstraint(info, &constraint, i, &argvIndex, &usedConstraints) {
			continue
		}
	}

	t.setIndexCostEstimates(info, usedConstraints)
	return nil
}

// processConstraint processes a single constraint and updates index info.
// Returns true if the constraint was processed.
func (t *RTree) processConstraint(info *vtab.IndexInfo, constraint *vtab.IndexConstraint, constraintIdx int, argvIndex, usedConstraints *int) bool {
	if t.processIDConstraint(info, constraint, constraintIdx, argvIndex, usedConstraints) {
		return true
	}

	return t.processSpatialConstraint(info, constraint, constraintIdx, argvIndex, usedConstraints)
}

// processIDConstraint handles ID column constraints.
func (t *RTree) processIDConstraint(info *vtab.IndexInfo, constraint *vtab.IndexConstraint, constraintIdx int, argvIndex, usedConstraints *int) bool {
	if constraint.Column != 0 || constraint.Op != vtab.ConstraintEQ {
		return false
	}

	info.SetConstraintUsage(constraintIdx, *argvIndex, true)
	*argvIndex++
	*usedConstraints++
	info.IdxNum |= (1 << 0) // Set bit 0 for ID constraint
	return true
}

// processSpatialConstraint handles spatial coordinate column constraints.
func (t *RTree) processSpatialConstraint(info *vtab.IndexInfo, constraint *vtab.IndexConstraint, constraintIdx int, argvIndex, usedConstraints *int) bool {
	if constraint.Column <= 0 || constraint.Column >= len(t.columns) {
		return false
	}

	if !t.isSpatialOperator(constraint.Op) {
		return false
	}

	info.SetConstraintUsage(constraintIdx, *argvIndex, true)
	*argvIndex++
	*usedConstraints++
	// Mark which column has a constraint (bit offset by column number)
	info.IdxNum |= (1 << constraint.Column)
	return true
}

// isSpatialOperator checks if the operator is valid for spatial queries.
func (t *RTree) isSpatialOperator(op vtab.ConstraintOp) bool {
	return op == vtab.ConstraintLE || op == vtab.ConstraintGE ||
		op == vtab.ConstraintLT || op == vtab.ConstraintGT
}

// setIndexCostEstimates sets the cost estimates for the index.
func (t *RTree) setIndexCostEstimates(info *vtab.IndexInfo, usedConstraints int) {
	if usedConstraints > 0 {
		// Spatial index lookup is very efficient
		// Cost decreases with more constraints
		info.EstimatedCost = 10.0 / float64(usedConstraints)
		info.EstimatedRows = 100 / int64(usedConstraints)
	} else {
		// Full scan of all entries
		info.EstimatedCost = float64(len(t.entries))
		info.EstimatedRows = int64(len(t.entries))
	}
}

// Open creates a new cursor for scanning the R-Tree table.
func (t *RTree) Open() (vtab.VirtualCursor, error) {
	return &RTreeCursor{
		table:   t,
		results: make([]*Entry, 0),
		pos:     -1,
	}, nil
}

// Update handles INSERT, UPDATE, and DELETE operations.
func (t *RTree) Update(argc int, argv []interface{}) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// DELETE: argc=1, argv[0]=rowid
	if argc == 1 {
		return t.handleDelete(argv)
	}

	// INSERT or UPDATE
	if argc < 2 {
		return 0, fmt.Errorf("invalid number of arguments for UPDATE/INSERT")
	}

	return t.handleInsertOrUpdate(argc, argv)
}

// handleDelete handles DELETE operations.
func (t *RTree) handleDelete(argv []interface{}) (int64, error) {
	id, ok := argv[0].(int64)
	if !ok {
		return 0, fmt.Errorf("invalid ID for DELETE")
	}

	entry, exists := t.entries[id]
	if !exists {
		return 0, fmt.Errorf("entry with ID %d not found", id)
	}

	// Remove from R-Tree
	if t.root != nil {
		t.root = t.root.Remove(entry)
	}

	delete(t.entries, id)

	// Persist changes to shadow tables
	if t.shadowMgr != nil {
		t.shadowMgr.SaveEntries(t.entries)
		t.shadowMgr.SaveNextID(t.nextID)
	}

	return id, nil
}

// handleInsertOrUpdate handles INSERT and UPDATE operations.
func (t *RTree) handleInsertOrUpdate(argc int, argv []interface{}) (int64, error) {
	oldID := argv[0]
	newID := argv[1]

	// Check if this is an UPDATE
	isUpdate, oldEntryID := t.checkIfUpdate(oldID)

	// Determine the entry ID
	entryID, err := t.determineEntryID(newID)
	if err != nil {
		return 0, err
	}

	// If this is an update, remove the old entry
	if isUpdate {
		t.removeOldEntry(oldEntryID)
	}

	// Parse coordinates
	coords, err := t.parseCoordinates(argc, argv)
	if err != nil {
		return 0, err
	}

	// Create and insert entry
	entry, err := t.createEntry(entryID, coords)
	if err != nil {
		return 0, err
	}

	// Insert into R-Tree
	if t.root == nil {
		t.root = NewLeafNode()
	}
	t.root = t.root.Insert(entry)

	// Store entry
	t.entries[entryID] = entry

	// Persist changes to shadow tables
	if t.shadowMgr != nil {
		t.shadowMgr.SaveEntries(t.entries)
		t.shadowMgr.SaveNextID(t.nextID)
	}

	return entryID, nil
}

// checkIfUpdate determines if the operation is an UPDATE.
func (t *RTree) checkIfUpdate(oldID interface{}) (bool, int64) {
	if oldID == nil {
		return false, 0
	}

	if id, ok := oldID.(int64); ok && id != 0 {
		return true, id
	}

	return false, 0
}

// determineEntryID determines the entry ID for the operation.
func (t *RTree) determineEntryID(newID interface{}) (int64, error) {
	if newID == nil || newID == int64(0) {
		// Auto-generate ID
		entryID := t.nextID
		t.nextID++
		return entryID, nil
	}

	id, ok := newID.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid ID type")
	}

	// Update nextID if needed
	if id >= t.nextID {
		t.nextID = id + 1
	}

	return id, nil
}

// removeOldEntry removes an old entry during UPDATE.
func (t *RTree) removeOldEntry(oldEntryID int64) {
	if oldEntry, exists := t.entries[oldEntryID]; exists {
		if t.root != nil {
			t.root = t.root.Remove(oldEntry)
		}
		delete(t.entries, oldEntryID)
	}
}

// parseCoordinates parses coordinate values from the argv array.
func (t *RTree) parseCoordinates(argc int, argv []interface{}) ([]float64, error) {
	expectedCoords := t.dimensions * 2
	if argc-2 < expectedCoords {
		return nil, fmt.Errorf("not enough coordinate values, expected %d", expectedCoords)
	}

	coords := make([]float64, expectedCoords)
	for i := 0; i < expectedCoords; i++ {
		coord, err := t.parseCoordinate(argv[i+2], i)
		if err != nil {
			return nil, err
		}
		coords[i] = coord
	}

	return coords, nil
}

// parseCoordinate parses a single coordinate value.
func (t *RTree) parseCoordinate(val interface{}, position int) (float64, error) {
	switch v := val.(type) {
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		coord, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid coordinate value at position %d: %v", position, err)
		}
		return coord, nil
	default:
		return 0, fmt.Errorf("unsupported coordinate type at position %d", position)
	}
}

// createEntry creates a new Entry with the given ID and coordinates.
func (t *RTree) createEntry(entryID int64, coords []float64) (*Entry, error) {
	// Validate that min <= max for each dimension
	if err := t.validateCoordinates(coords); err != nil {
		return nil, err
	}

	// Create bounding box
	bbox := t.createBoundingBox(coords)

	// Create entry
	entry := &Entry{
		ID:   entryID,
		BBox: bbox,
	}

	return entry, nil
}

// validateCoordinates validates that min <= max for each dimension.
func (t *RTree) validateCoordinates(coords []float64) error {
	for i := 0; i < t.dimensions; i++ {
		minIdx := i * 2
		maxIdx := i*2 + 1
		if coords[minIdx] > coords[maxIdx] {
			return fmt.Errorf("dimension %d: min (%f) > max (%f)", i, coords[minIdx], coords[maxIdx])
		}
	}
	return nil
}

// createBoundingBox creates a bounding box from coordinates.
func (t *RTree) createBoundingBox(coords []float64) *BoundingBox {
	bbox := NewBoundingBox(t.dimensions)
	for i := 0; i < t.dimensions; i++ {
		bbox.Min[i] = coords[i*2]
		bbox.Max[i] = coords[i*2+1]
	}
	return bbox
}

// Destroy is called when the table is dropped.
func (t *RTree) Destroy() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.root = nil
	t.entries = make(map[int64]*Entry)

	// Drop shadow tables
	if t.shadowMgr != nil {
		return t.shadowMgr.DropShadowTables()
	}
	return nil
}

// RTreeCursor represents a cursor for scanning R-Tree results.
type RTreeCursor struct {
	vtab.BaseCursor
	table   *RTree
	results []*Entry
	pos     int
	// SCAFFOLDING: queryBBox for spatial range queries (overlap, contains, within)
	queryBBox *BoundingBox
	queryID   *int64 // For ID queries
	// SCAFFOLDING: queryType for distinguishing spatial query operations
	queryType  QueryType
	idxNum     int           // From BestIndex
	constraint []interface{} // Constraint values
}

// QueryType represents the type of spatial query.
type QueryType int

const (
	QueryTypeAll QueryType = iota
	QueryTypeID
	QueryTypeOverlap
	QueryTypeContains
	QueryTypeWithin
)

// Filter initializes the cursor with query constraints.
func (c *RTreeCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	c.table.mu.RLock()
	defer c.table.mu.RUnlock()

	c.idxNum = idxNum
	c.constraint = argv
	c.results = make([]*Entry, 0)

	c.applyFilterConstraints(idxNum, argv)
	c.positionCursor()

	return nil
}

// applyFilterConstraints applies the appropriate filter based on constraints.
func (c *RTreeCursor) applyFilterConstraints(idxNum int, argv []interface{}) {
	if c.hasIDConstraint(idxNum, argv) {
		c.applyIDFilter(argv)
		return
	}

	if len(argv) > 0 {
		c.applySpatialFilter()
	} else {
		c.applyFullScan()
	}
}

// hasIDConstraint checks if there's an ID constraint.
func (c *RTreeCursor) hasIDConstraint(idxNum int, argv []interface{}) bool {
	return idxNum&1 != 0 && len(argv) > 0
}

// applyIDFilter applies an ID-based filter.
func (c *RTreeCursor) applyIDFilter(argv []interface{}) {
	id, ok := argv[0].(int64)
	if !ok {
		return
	}

	c.queryID = &id
	if entry, exists := c.table.entries[id]; exists {
		c.results = append(c.results, entry)
	}
}

// applySpatialFilter applies a spatial query filter.
func (c *RTreeCursor) applySpatialFilter() {
	// Build a query bounding box from the constraints
	// This implementation handles basic range queries
	queryBox := c.buildQueryBox()

	if queryBox != nil && c.table.root != nil {
		// Use the R-Tree search for efficient spatial queries
		c.results = c.table.root.SearchOverlap(queryBox)
	} else {
		// Fall back to full scan if we can't build a proper query box
		for _, entry := range c.table.entries {
			c.results = append(c.results, entry)
		}
	}
}

// buildQueryBox constructs a bounding box from the query constraints.
func (c *RTreeCursor) buildQueryBox() *BoundingBox {
	// Initialize with infinite bounds
	bbox := NewBoundingBox(c.table.dimensions)
	for i := 0; i < c.table.dimensions; i++ {
		bbox.Min[i] = -1e308 // Approximate negative infinity
		bbox.Max[i] = 1e308  // Approximate positive infinity
	}

	// Parse constraints and refine the bounding box
	argIdx := 0
	for col := 1; col <= c.table.dimensions*2; col++ {
		if c.idxNum&(1<<col) == 0 {
			continue // This column doesn't have a constraint
		}

		if argIdx >= len(c.constraint) {
			break
		}

		// Extract the constraint value
		val := c.extractConstraintValue(c.constraint[argIdx])

		// Update the appropriate bound
		dimIndex := (col - 1) / 2
		isMaxCol := (col-1)%2 == 1

		if dimIndex < c.table.dimensions {
			if isMaxCol {
				// This is a maxX/maxY/etc column
				// Constraint: maxX >= value → query box min = value
				if val > bbox.Min[dimIndex] {
					bbox.Min[dimIndex] = val
				}
			} else {
				// This is a minX/minY/etc column
				// Constraint: minX <= value → query box max = value
				if val < bbox.Max[dimIndex] {
					bbox.Max[dimIndex] = val
				}
			}
		}

		argIdx++
	}

	return bbox
}

// extractConstraintValue extracts a float64 value from a constraint.
func (c *RTreeCursor) extractConstraintValue(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	default:
		return 0
	}
}

// applyFullScan performs a full scan of all entries.
func (c *RTreeCursor) applyFullScan() {
	for _, entry := range c.table.entries {
		c.results = append(c.results, entry)
	}
}

// positionCursor positions the cursor at the first result or EOF.
func (c *RTreeCursor) positionCursor() {
	if len(c.results) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}
}

// Next advances to the next result.
func (c *RTreeCursor) Next() error {
	c.pos++
	return nil
}

// EOF returns true if we're past the last result.
func (c *RTreeCursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.results)
}

// Column returns the value of a column for the current row.
func (c *RTreeCursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, fmt.Errorf("cursor at EOF")
	}

	entry := c.results[c.pos]

	// Column 0 is the ID
	if index == 0 {
		return entry.ID, nil
	}

	// Coordinate columns (1-based index)
	coordIndex := index - 1
	dimIndex := coordIndex / 2
	isMax := coordIndex%2 == 1

	if dimIndex >= c.table.dimensions {
		return nil, fmt.Errorf("column index out of range: %d", index)
	}

	if isMax {
		return entry.BBox.Max[dimIndex], nil
	}
	return entry.BBox.Min[dimIndex], nil
}

// Rowid returns the rowid of the current row.
func (c *RTreeCursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, fmt.Errorf("cursor at EOF")
	}

	return c.results[c.pos].ID, nil
}

// Close closes the cursor.
func (c *RTreeCursor) Close() error {
	c.results = nil
	c.queryBBox = nil
	c.queryID = nil
	c.constraint = nil
	return nil
}

// RegisterRTree registers the R-Tree module with the virtual table registry.
func RegisterRTree() error {
	return vtab.RegisterModule("rtree", NewRTreeModule())
}

// SearchOverlap searches for all entries that overlap with the given bounding box.
func (t *RTree) SearchOverlap(bbox *BoundingBox) []*Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil
	}

	return t.root.SearchOverlap(bbox)
}

// SearchContains searches for all entries that contain the given point.
func (t *RTree) SearchContains(point []float64) []*Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil
	}

	// Create a point bounding box
	bbox := NewBoundingBox(len(point))
	copy(bbox.Min, point)
	copy(bbox.Max, point)

	return t.root.SearchOverlap(bbox)
}

// SearchWithin searches for all entries that are completely within the given bounding box.
func (t *RTree) SearchWithin(bbox *BoundingBox) []*Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return nil
	}

	return t.root.SearchWithin(bbox)
}

// GetEntry retrieves an entry by ID.
func (t *RTree) GetEntry(id int64) (*Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry, exists := t.entries[id]
	return entry, exists
}

// Count returns the number of entries in the R-Tree.
func (t *RTree) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.entries)
}
