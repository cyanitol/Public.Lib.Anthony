package rtree

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
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
	// R-Tree requires at least 3 columns: id, minX, maxX
	// For 2D: id, minX, maxX, minY, maxY (5 columns)
	// For 3D: id, minX, maxX, minY, maxY, minZ, maxZ (7 columns)
	if len(args) < 5 {
		return nil, "", fmt.Errorf("R-Tree table requires at least 5 columns (id, minX, maxX, minY, maxY)")
	}

	// Parse column definitions
	columns := make([]string, 0, len(args))
	for _, arg := range args {
		colName := strings.TrimSpace(arg)
		if colName != "" {
			columns = append(columns, colName)
		}
	}

	if len(columns) < 5 {
		return nil, "", fmt.Errorf("R-Tree table requires at least 5 columns")
	}

	// First column must be the ID
	idColumn := columns[0]

	// Validate that we have pairs of min/max coordinates
	// Format: id, min1, max1, min2, max2, [min3, max3, ...]
	coordColumns := columns[1:]
	if len(coordColumns)%2 != 0 {
		return nil, "", fmt.Errorf("R-Tree coordinate columns must come in min/max pairs")
	}

	dimensions := len(coordColumns) / 2
	if dimensions < 1 || dimensions > 5 {
		return nil, "", fmt.Errorf("R-Tree supports 1-5 dimensions, got %d", dimensions)
	}

	// Build schema SQL
	schema := fmt.Sprintf("CREATE TABLE %s(%s)", tableName, strings.Join(columns, ", "))

	// Create the R-Tree table
	table := &RTree{
		tableName:  tableName,
		columns:    columns,
		idColumn:   idColumn,
		dimensions: dimensions,
		root:       nil,
		entries:    make(map[int64]*Entry),
		nextID:     1,
	}

	return table, schema, nil
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
}

// BestIndex analyzes the query and determines the best index strategy.
// For R-Tree, we look for spatial constraints like range queries and overlaps.
func (t *RTree) BestIndex(info *vtab.IndexInfo) error {
	// Track which constraints we can use
	argvIndex := 1
	usedConstraints := 0

	// Look for constraints on coordinate columns
	// We can optimize queries like:
	// - WHERE minX <= ? AND maxX >= ? AND minY <= ? AND maxY >= ?
	// - WHERE id = ?
	for i, constraint := range info.Constraints {
		if !constraint.Usable {
			continue
		}

		// ID column (column 0) - exact match
		if constraint.Column == 0 && constraint.Op == vtab.ConstraintEQ {
			info.SetConstraintUsage(i, argvIndex, true)
			argvIndex++
			usedConstraints++
			info.IdxNum |= (1 << 0) // Set bit 0 for ID constraint
		}

		// Spatial constraints on coordinate columns (columns 1+)
		// We support LE (<=), GE (>=), LT (<), GT (>) for range queries
		if constraint.Column > 0 && constraint.Column < len(t.columns) {
			switch constraint.Op {
			case vtab.ConstraintLE, vtab.ConstraintGE, vtab.ConstraintLT, vtab.ConstraintGT:
				info.SetConstraintUsage(i, argvIndex, true)
				argvIndex++
				usedConstraints++
				// Mark which column has a constraint (bit offset by column number)
				info.IdxNum |= (1 << constraint.Column)
			}
		}
	}

	// Set cost estimates
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

	return nil
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
	return nil
}

// RTreeCursor represents a cursor for scanning R-Tree results.
type RTreeCursor struct {
	vtab.BaseCursor
	table      *RTree
	results    []*Entry
	pos        int
	queryBBox  *BoundingBox // For spatial queries
	queryID    *int64       // For ID queries
	queryType  QueryType    // Type of spatial query
	idxNum     int          // From BestIndex
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

	// Check if we have an ID constraint (bit 0 set)
	if idxNum&1 != 0 && len(argv) > 0 {
		// ID lookup
		if id, ok := argv[0].(int64); ok {
			c.queryID = &id
			if entry, exists := c.table.entries[id]; exists {
				c.results = append(c.results, entry)
			}
		}
	} else if len(argv) > 0 {
		// Spatial query - build bounding box from constraints
		// This is a simplified version that assumes range query format
		// A full implementation would parse the specific constraints used

		// For now, perform a full scan (will be optimized with proper constraint parsing)
		for _, entry := range c.table.entries {
			c.results = append(c.results, entry)
		}
	} else {
		// No constraints - return all entries
		for _, entry := range c.table.entries {
			c.results = append(c.results, entry)
		}
	}

	// Position at first result
	if len(c.results) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}

	return nil
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
