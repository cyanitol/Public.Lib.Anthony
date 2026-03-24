// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// ---------------------------------------------------------------------------
// mockDB — in-memory DatabaseExecutor for testing ShadowTableManager
// ---------------------------------------------------------------------------

// mockTable is an in-memory key-value store mimicking a single DB table.
type mockTable struct {
	// rows keyed by the first column (int64 pk)
	rows map[int64][]interface{}
}

func newMockTable() *mockTable {
	return &mockTable{rows: make(map[int64][]interface{})}
}

// mockDB implements DatabaseExecutor using in-memory maps.
type mockDB struct {
	mu     sync.Mutex
	tables map[string]*mockTable
	// if execDDLErr is set, ExecDDL returns that error
	execDDLErr error
	// if execDMLErr is set, ExecDML returns that error
	execDMLErr error
}

func newMockDB() *mockDB {
	return &mockDB{tables: make(map[string]*mockTable)}
}

// ExecDDL processes CREATE TABLE IF NOT EXISTS and DROP TABLE IF EXISTS statements.
func (m *mockDB) ExecDDL(sql string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.execDDLErr != nil {
		return m.execDDLErr
	}
	// Parse enough to create or drop a table entry.
	// We only need to track existence; we don't validate full SQL.
	name, op := parseDDLStatement(sql)
	if op == "create" {
		if _, exists := m.tables[name]; !exists {
			m.tables[name] = newMockTable()
		}
	} else if op == "drop" {
		delete(m.tables, name)
	}
	return nil
}

// parseDDLStatement extracts the table name and operation from simple DDL.
// Recognises: "CREATE TABLE IF NOT EXISTS name(...)" and "DROP TABLE IF EXISTS name".
func parseDDLStatement(sql string) (string, string) {
	if len(sql) == 0 {
		return "", ""
	}
	// CREATE TABLE IF NOT EXISTS <name>
	var name string
	if n, err := fmt.Sscanf(sql, "CREATE TABLE IF NOT EXISTS %s", &name); n == 1 && err == nil {
		// name may include "(…)" – strip at first '('
		for i, c := range name {
			if c == '(' {
				name = name[:i]
				break
			}
		}
		return name, "create"
	}
	// DROP TABLE IF EXISTS <name>
	if n, err := fmt.Sscanf(sql, "DROP TABLE IF EXISTS %s", &name); n == 1 && err == nil {
		return name, "drop"
	}
	// DELETE FROM <name>
	if n, err := fmt.Sscanf(sql, "DELETE FROM %s", &name); n == 1 && err == nil {
		return name, "delete"
	}
	return "", ""
}

// ExecDML handles INSERT OR REPLACE INTO … and DELETE FROM … statements.
func (m *mockDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.execDMLErr != nil {
		return 0, m.execDMLErr
	}

	// INSERT OR REPLACE INTO <table>(…) VALUES(?,?)
	var tableName string
	if n, _ := fmt.Sscanf(sql, "INSERT OR REPLACE INTO %s", &tableName); n == 1 {
		// strip trailing "(..." if present
		for i, c := range tableName {
			if c == '(' {
				tableName = tableName[:i]
				break
			}
		}
		tbl := m.getOrCreateTable(tableName)
		if len(args) >= 2 {
			pk, ok := args[0].(int64)
			if ok {
				row := make([]interface{}, len(args))
				copy(row, args)
				tbl.rows[pk] = row
			}
		}
		return 1, nil
	}

	// DELETE FROM <table>
	if n, _ := fmt.Sscanf(sql, "DELETE FROM %s", &tableName); n == 1 {
		if tbl, exists := m.tables[tableName]; exists {
			tbl.rows = make(map[int64][]interface{})
		}
		return 0, nil
	}

	return 0, nil
}

func (m *mockDB) getOrCreateTable(name string) *mockTable {
	if tbl, ok := m.tables[name]; ok {
		return tbl
	}
	tbl := newMockTable()
	m.tables[name] = tbl
	return tbl
}

// Query handles SELECT statements for the shadow tables.
func (m *mockDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// SELECT nodeno, data FROM <table>_node
	var tableName string
	if n, _ := fmt.Sscanf(sql, "SELECT nodeno, data FROM %s", &tableName); n == 1 {
		tbl, exists := m.tables[tableName]
		if !exists {
			return nil, nil
		}
		var result [][]interface{}
		for _, row := range tbl.rows {
			result = append(result, row)
		}
		return result, nil
	}

	// SELECT parentnode FROM <table>_parent WHERE nodeno = ?
	if n, _ := fmt.Sscanf(sql, "SELECT parentnode FROM %s", &tableName); n == 1 {
		// strip " WHERE ..." suffix from tableName
		for i, c := range tableName {
			if c == ' ' {
				tableName = tableName[:i]
				break
			}
		}
		tbl, exists := m.tables[tableName]
		if !exists || len(args) == 0 {
			return nil, nil
		}
		pk, ok := args[0].(int64)
		if !ok {
			return nil, nil
		}
		row, exists := tbl.rows[pk]
		if !exists || len(row) < 2 {
			return nil, nil
		}
		// row[1] is the parentnode value
		return [][]interface{}{{row[1]}}, nil
	}

	return nil, nil
}

// ---------------------------------------------------------------------------
// Tests for ShadowTableManager
// ---------------------------------------------------------------------------

// TestNewShadowTableManager verifies basic construction.
func TestNewShadowTableManager(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("myrtree", db, 2)
	if mgr == nil {
		t.Fatal("NewShadowTableManager returned nil")
	}
}

// TestShadowTableManagerNilDB verifies nil-db fast paths.
func TestShadowTableManagerNilDB(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("t", nil, 2)

	if err := mgr.CreateShadowTables(); err != nil {
		t.Errorf("CreateShadowTables with nil db: %v", err)
	}
	if err := mgr.DropShadowTables(); err != nil {
		t.Errorf("DropShadowTables with nil db: %v", err)
	}
	if err := mgr.SaveEntries(map[int64]*Entry{}); err != nil {
		t.Errorf("SaveEntries with nil db: %v", err)
	}
	if err := mgr.SaveNextID(42); err != nil {
		t.Errorf("SaveNextID with nil db: %v", err)
	}
	entries, err := mgr.LoadEntries()
	if err != nil {
		t.Errorf("LoadEntries with nil db: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
	nextID, err := mgr.LoadNextID()
	if err != nil {
		t.Errorf("LoadNextID with nil db: %v", err)
	}
	if nextID != 1 {
		t.Errorf("expected nextID=1, got %d", nextID)
	}
}

// TestShadowTableManagerCreateAndDrop verifies create + drop round-trip.
func TestShadowTableManagerCreateAndDrop(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)

	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	// All three shadow tables should now exist.
	for _, suffix := range []string{"_node", "_rowid", "_parent"} {
		if _, ok := db.tables["geo"+suffix]; !ok {
			t.Errorf("shadow table geo%s not created", suffix)
		}
	}

	if err := mgr.DropShadowTables(); err != nil {
		t.Fatalf("DropShadowTables: %v", err)
	}

	for _, suffix := range []string{"_node", "_rowid", "_parent"} {
		if _, ok := db.tables["geo"+suffix]; ok {
			t.Errorf("shadow table geo%s should be dropped", suffix)
		}
	}
}

// TestShadowTableManagerCreateDDLError verifies error propagation from ExecDDL.
func TestShadowTableManagerCreateDDLError(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	db.execDDLErr = fmt.Errorf("ddl failure")
	mgr := NewShadowTableManager("geo", db, 2)
	err := mgr.CreateShadowTables()
	if err == nil {
		t.Error("expected error from CreateShadowTables when DDL fails")
	}
}

// TestShadowTableManagerSaveAndLoadEntries exercises SaveEntries + LoadEntries round-trip.
func TestShadowTableManagerSaveAndLoadEntries(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)
	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	entries := map[int64]*Entry{
		1: {ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
		2: {ID: 2, BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}},
	}

	if err := mgr.SaveEntries(entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	loaded, err := mgr.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries: %v", err)
	}
	if len(loaded) != len(entries) {
		t.Fatalf("loaded %d entries, want %d", len(loaded), len(entries))
	}
	for id, want := range entries {
		got, ok := loaded[id]
		if !ok {
			t.Errorf("entry %d not found after load", id)
			continue
		}
		if !want.BBox.Equal(got.BBox) {
			t.Errorf("entry %d BBox mismatch: want %v got %v", id, want.BBox, got.BBox)
		}
	}
}

// TestShadowTableManagerSaveEntriesDMLError verifies DML errors bubble up.
func TestShadowTableManagerSaveEntriesDMLError(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)
	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	db.execDMLErr = fmt.Errorf("dml failure")

	entries := map[int64]*Entry{
		1: {ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}},
	}
	err := mgr.SaveEntries(entries)
	if err == nil {
		t.Error("expected error from SaveEntries when DML fails")
	}
}

// TestShadowTableManagerSaveAndLoadNextID exercises SaveNextID + LoadNextID.
func TestShadowTableManagerSaveAndLoadNextID(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)
	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	const want int64 = 42
	if err := mgr.SaveNextID(want); err != nil {
		t.Fatalf("SaveNextID: %v", err)
	}

	got, err := mgr.LoadNextID()
	if err != nil {
		t.Fatalf("LoadNextID: %v", err)
	}
	if got != want {
		t.Errorf("LoadNextID = %d, want %d", got, want)
	}
}

// TestShadowTableManagerLoadNextIDDefault verifies default when no row exists.
func TestShadowTableManagerLoadNextIDDefault(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)
	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	// Don't save a nextID; LoadNextID should return 1.
	got, err := mgr.LoadNextID()
	if err != nil {
		t.Fatalf("LoadNextID: %v", err)
	}
	if got != 1 {
		t.Errorf("LoadNextID (default) = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Tests for createTable with a DatabaseExecutor (covers persistence branch)
// ---------------------------------------------------------------------------

// TestCreateTableWithDatabaseExecutor verifies shadow table creation via module.Create.
func TestCreateTableWithDatabaseExecutor(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()

	table, schema, err := module.Create(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if table == nil {
		t.Fatal("Create returned nil table")
	}
	if schema == "" {
		t.Error("Create returned empty schema")
	}

	rt := table.(*RTree)
	if rt.shadowMgr == nil {
		t.Error("shadowMgr should be set when db implements DatabaseExecutor")
	}

	// Shadow tables must have been created.
	for _, suffix := range []string{"_node", "_rowid", "_parent"} {
		if _, ok := db.tables["geo"+suffix]; !ok {
			t.Errorf("shadow table geo%s not found after Create", suffix)
		}
	}
}

// TestCreateTableAndConnectWithExecutor verifies Connect also uses persistence.
func TestCreateTableAndConnectWithExecutor(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()

	_, _, err := module.Connect(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests for loadFromShadowTables (exercises the load path via Create)
// ---------------------------------------------------------------------------

// TestLoadFromShadowTables verifies that data persisted in shadow tables is
// loaded back when the module reconnects to the same db.
func TestLoadFromShadowTables(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()

	// First creation: insert entries so they get persisted.
	table1, _, err := module.Create(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt1 := table1.(*RTree)

	_, err = rt1.Update(7, []interface{}{nil, int64(1), 0.0, 10.0, 0.0, 10.0})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	_, err = rt1.Update(7, []interface{}{nil, int64(2), 5.0, 15.0, 5.0, 15.0})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Second connection: should load persisted entries.
	table2, _, err := module.Connect(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rt2 := table2.(*RTree)

	if rt2.Count() != 2 {
		t.Errorf("loadFromShadowTables: want 2 entries, got %d", rt2.Count())
	}

	for _, id := range []int64{1, 2} {
		if _, ok := rt2.GetEntry(id); !ok {
			t.Errorf("entry %d not reloaded from shadow tables", id)
		}
	}
}

// TestLoadFromShadowTablesAdjustsNextID verifies nextID is adjusted after load.
func TestLoadFromShadowTablesAdjustsNextID(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()

	// Insert entries with explicit large IDs to stress nextID adjustment.
	table1, _, err := module.Create(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt1 := table1.(*RTree)
	_, err = rt1.Update(7, []interface{}{nil, int64(100), 0.0, 1.0, 0.0, 1.0})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Reconnect — the loaded nextID must be > 100.
	table2, _, err := module.Connect(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	rt2 := table2.(*RTree)
	if rt2.nextID <= 100 {
		t.Errorf("nextID should be >100 after load, got %d", rt2.nextID)
	}
}

// ---------------------------------------------------------------------------
// Tests for Destroy with shadow tables
// ---------------------------------------------------------------------------

// TestDestroyWithShadowTables verifies Destroy drops shadow tables.
func TestDestroyWithShadowTables(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()

	table, _, err := module.Create(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	if err := rt.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	for _, suffix := range []string{"_node", "_rowid", "_parent"} {
		if _, ok := db.tables["geo"+suffix]; ok {
			t.Errorf("shadow table geo%s should be dropped after Destroy", suffix)
		}
	}
}

// ---------------------------------------------------------------------------
// Tests for applySpatialFilter / extractConstraintValue
// ---------------------------------------------------------------------------

// makeRTreeWithEntries creates a 2D RTree and inserts given rectangles.
func makeRTreeWithEntries(t *testing.T, rects [][4]float64) *RTree {
	t.Helper()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)
	for i, r := range rects {
		_, err := rt.Update(7, []interface{}{nil, int64(i + 1), r[0], r[1], r[2], r[3]})
		if err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	return rt
}

// filterWithConstraints opens a cursor and calls Filter with the given idxNum and argv.
func filterWithConstraints(t *testing.T, rt *RTree, idxNum int, argv []interface{}) []*Entry {
	t.Helper()
	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)
	if err := c.Filter(idxNum, "", argv); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	var results []*Entry
	for !c.EOF() {
		rowid, err := c.Rowid()
		if err != nil {
			t.Fatalf("Rowid: %v", err)
		}
		e, _ := rt.GetEntry(rowid)
		if e != nil {
			results = append(results, e)
		}
		c.Next()
	}
	return results
}

// TestApplySpatialFilterFloat64Constraint tests applySpatialFilter with float64 constraints.
func TestApplySpatialFilterFloat64Constraint(t *testing.T) {
	t.Parallel()

	rects := [][4]float64{
		{0, 10, 0, 10},   // id=1, overlaps [2,8]x[2,8]
		{20, 30, 20, 30}, // id=2, does NOT overlap
	}
	rt := makeRTreeWithEntries(t, rects)

	// Build idxNum that marks column 1 (minX) and column 2 (maxX).
	// BestIndex sets bit (1<<col) for each constrained column.
	// We constrain minX and maxX so both bits 1 and 2 are set.
	idxNum := (1 << 1) | (1 << 2)
	// argv[0] → constraint on col 1 (minX ≤ 12)
	// argv[1] → constraint on col 2 (maxX ≥ 1)
	argv := []interface{}{float64(12), float64(1)}

	results := filterWithConstraints(t, rt, idxNum, argv)
	found := make(map[int64]bool)
	for _, e := range results {
		found[e.ID] = true
	}
	if !found[1] {
		t.Error("expected entry 1 to be returned by spatial filter")
	}
}

// TestApplySpatialFilterInt64Constraint tests extractConstraintValue with int64.
func TestApplySpatialFilterInt64Constraint(t *testing.T) {
	t.Parallel()

	rects := [][4]float64{
		{0, 10, 0, 10},
		{50, 60, 50, 60},
	}
	rt := makeRTreeWithEntries(t, rects)

	// constrain col 1 with int64
	idxNum := 1 << 1
	argv := []interface{}{int64(20)} // minX ≤ 20 → query max[0] = 20

	results := filterWithConstraints(t, rt, idxNum, argv)
	found := make(map[int64]bool)
	for _, e := range results {
		found[e.ID] = true
	}
	if !found[1] {
		t.Error("expected entry 1 in results for int64 constraint")
	}
}

// TestApplySpatialFilterIntConstraint tests extractConstraintValue with plain int.
func TestApplySpatialFilterIntConstraint(t *testing.T) {
	t.Parallel()

	rects := [][4]float64{
		{0, 10, 0, 10},
		{50, 60, 50, 60},
	}
	rt := makeRTreeWithEntries(t, rects)

	// Directly invoke extractConstraintValue for all branches.
	cursor := &RTreeCursor{table: rt}

	if v := cursor.extractConstraintValue(int64(5)); v != 5.0 {
		t.Errorf("int64 branch: want 5.0, got %v", v)
	}
	if v := cursor.extractConstraintValue(float64(7.5)); v != 7.5 {
		t.Errorf("float64 branch: want 7.5, got %v", v)
	}
	if v := cursor.extractConstraintValue(int(3)); v != 3.0 {
		t.Errorf("int branch: want 3.0, got %v", v)
	}
	if v := cursor.extractConstraintValue("unknown"); v != 0 {
		t.Errorf("default branch: want 0, got %v", v)
	}
}

// TestApplySpatialFilterFallbackFullScan verifies the nil-root fallback path.
func TestApplySpatialFilterFallbackFullScan(t *testing.T) {
	t.Parallel()

	// Create table with entries so entries map is non-empty but simulate
	// a case where buildQueryBox returns a box and root is nil (empty tree).
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Insert then immediately access cursor before removing from tree to test
	// the branch where queryBox != nil but root == nil.
	// We can achieve nil root by not inserting anything.
	// With argv but nil root, the code should fall back to full scan of entries.
	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)
	// idxNum has col 1 set; argv has one value → applySpatialFilter is triggered.
	// Since root == nil the fall-through full scan executes (entries is empty).
	idxNum := 1 << 1
	argv := []interface{}{float64(10)}
	if err := c.Filter(idxNum, "", argv); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if !c.EOF() {
		t.Error("expected EOF on empty tree with spatial filter")
	}
}

// TestApplySpatialFilterWithNilQueryBox verifies the path when idxNum has no
// column bits set but argv is non-empty (buildQueryBox returns bbox with infinite
// bounds, not nil — so the tree search runs).
func TestApplySpatialFilterAllColumnsUnconstrained(t *testing.T) {
	t.Parallel()

	rects := [][4]float64{
		{0, 10, 0, 10},
		{5, 15, 5, 15},
	}
	rt := makeRTreeWithEntries(t, rects)

	// idxNum == 0 means no column has a constraint bit.
	// But len(argv) > 0 triggers applySpatialFilter, not full scan.
	// buildQueryBox loops over cols but none match, so bbox retains infinite bounds.
	// With root != nil and infinite query box all entries should be returned.
	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)
	argv := []interface{}{float64(5)} // non-empty argv, idxNum=0
	if err := c.Filter(0, "", argv); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	count := 0
	for !c.EOF() {
		count++
		c.Next()
	}
	if count < 1 {
		t.Errorf("expected at least 1 result with unconstrained spatial filter, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Tests for DistanceBetweenBoxes edge cases
// ---------------------------------------------------------------------------

// TestDistanceBetweenBoxesDimensionMismatch verifies MaxFloat64 is returned.
func TestDistanceBetweenBoxesDimensionMismatch(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}
	b2 := &BoundingBox{Min: []float64{0}, Max: []float64{1}}
	got := DistanceBetweenBoxes(b1, b2)
	if got != math.MaxFloat64 {
		t.Errorf("want MaxFloat64, got %v", got)
	}
}

// TestDistanceBetweenBoxesOverlapping verifies overlapping boxes have distance 0.
func TestDistanceBetweenBoxesOverlapping(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}
	got := DistanceBetweenBoxes(b1, b2)
	if got != 0 {
		t.Errorf("overlapping boxes: want 0, got %v", got)
	}
}

// TestDistanceBetweenBoxesTouchingEdge verifies adjacent boxes return 0.
func TestDistanceBetweenBoxesTouchingEdge(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	b2 := &BoundingBox{Min: []float64{5, 0}, Max: []float64{10, 5}}
	got := DistanceBetweenBoxes(b1, b2)
	if got != 0 {
		t.Errorf("touching boxes: want 0, got %v", got)
	}
}

// TestDistanceBetweenBoxesSeparated verifies Pythagorean distance.
func TestDistanceBetweenBoxesSeparated(t *testing.T) {
	t.Parallel()
	// b1 ends at x=0; b2 starts at x=3; b1 ends at y=0, b2 starts at y=4
	// distance = sqrt(3^2 + 4^2) = 5
	b1 := &BoundingBox{Min: []float64{-1, -1}, Max: []float64{0, 0}}
	b2 := &BoundingBox{Min: []float64{3, 4}, Max: []float64{6, 7}}
	got := DistanceBetweenBoxes(b1, b2)
	if math.Abs(got-5.0) > 1e-9 {
		t.Errorf("separated boxes: want 5.0, got %v", got)
	}
}

// TestDistanceBetweenBoxesB2BeforeB1 verifies the branch where b2.Max < b1.Min.
func TestDistanceBetweenBoxesB2BeforeB1(t *testing.T) {
	t.Parallel()
	// b2 is entirely to the left of b1
	b1 := &BoundingBox{Min: []float64{10, 0}, Max: []float64{20, 5}}
	b2 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	got := DistanceBetweenBoxes(b1, b2)
	if math.Abs(got-5.0) > 1e-9 {
		t.Errorf("b2 before b1: want 5.0, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Tests for OverlapArea edge cases
// ---------------------------------------------------------------------------

// TestOverlapAreaNoOverlap verifies 0 returned for non-overlapping boxes.
func TestCoverageOverlapAreaNoOverlap(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	b2 := &BoundingBox{Min: []float64{10, 10}, Max: []float64{20, 20}}
	got := OverlapArea(b1, b2)
	if got != 0 {
		t.Errorf("non-overlapping: want 0, got %v", got)
	}
}

// TestOverlapAreaDimensionMismatch verifies 0 returned on mismatch.
func TestOverlapAreaDimensionMismatch(t *testing.T) {
	t.Parallel()
	// Make b1 and b2 "overlap" by raw Overlaps check but differ in dims.
	// Overlaps returns false for mismatched dims, so OverlapArea returns 0.
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}
	got := OverlapArea(b1, b2)
	if got != 0 {
		t.Errorf("dim mismatch: want 0, got %v", got)
	}
}

// TestOverlapAreaFullOverlap verifies exact overlap area computation.
func TestOverlapAreaFullOverlap(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{2, 3}, Max: []float64{7, 8}}
	got := OverlapArea(b1, b2)
	// overlap is [2,7] x [3,8] → 5 * 5 = 25
	if math.Abs(got-25.0) > 1e-9 {
		t.Errorf("full overlap: want 25.0, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Tests for IntersectionBox edge cases
// ---------------------------------------------------------------------------

// TestIntersectionBoxNoOverlap verifies nil returned for non-overlapping boxes.
func TestIntersectionBoxNoOverlap(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	b2 := &BoundingBox{Min: []float64{10, 10}, Max: []float64{20, 20}}
	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("no overlap: want nil, got %v", result)
	}
}

// TestIntersectionBoxDimensionMismatch verifies nil on dimension mismatch.
func TestIntersectionBoxDimensionMismatch(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}
	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("dim mismatch: want nil, got %v", result)
	}
}

// TestIntersectionBoxPartialOverlap verifies correct intersection box.
func TestIntersectionBoxPartialOverlap(t *testing.T) {
	t.Parallel()
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{5, 5}, Max: []float64{15, 15}}
	result := IntersectionBox(b1, b2)
	if result == nil {
		t.Fatal("partial overlap: expected non-nil result")
	}
	if result.Min[0] != 5 || result.Min[1] != 5 || result.Max[0] != 10 || result.Max[1] != 10 {
		t.Errorf("wrong intersection: got Min=%v Max=%v", result.Min, result.Max)
	}
}

// TestIntersectionBoxIdentical verifies a box intersected with itself.
func TestIntersectionBoxIdentical(t *testing.T) {
	t.Parallel()
	b := &BoundingBox{Min: []float64{1, 2}, Max: []float64{3, 4}}
	result := IntersectionBox(b, b)
	if result == nil {
		t.Fatal("identical boxes: expected non-nil result")
	}
	if !result.Equal(b) {
		t.Errorf("identical intersection: want %v, got %v", b, result)
	}
}

// ---------------------------------------------------------------------------
// Integration: spatial filter constraints via BestIndex + Filter
// ---------------------------------------------------------------------------

// TestSpatialFilterViaConstraints exercises the full BestIndex → Filter path
// with various constraint operators to cover applySpatialFilter more deeply.
func TestSpatialFilterViaConstraints(t *testing.T) {
	t.Parallel()

	rects := [][4]float64{
		{0, 5, 0, 5},     // id=1
		{10, 20, 10, 20}, // id=2
		{30, 40, 30, 40}, // id=3
	}
	rt := makeRTreeWithEntries(t, rects)

	tests := []struct {
		name       string
		constraint vtab.IndexConstraint
		argv       []interface{}
		wantIDs    []int64
		notWantIDs []int64
	}{
		{
			name:       "minX GE constraint",
			constraint: vtab.IndexConstraint{Column: 1, Op: vtab.ConstraintGE, Usable: true},
			argv:       []interface{}{float64(0)},
			wantIDs:    []int64{1},
		},
		{
			name:       "maxX LE constraint",
			constraint: vtab.IndexConstraint{Column: 2, Op: vtab.ConstraintLE, Usable: true},
			argv:       []interface{}{float64(25)},
			// col 2 is maxX; buildQueryBox sets bbox.Min[0]=25, so only entries
			// whose maxX overlaps [25, +inf) are returned — that's rect 3 (30-40).
			wantIDs: []int64{3},
		},
		{
			name:       "minX LT constraint",
			constraint: vtab.IndexConstraint{Column: 1, Op: vtab.ConstraintLT, Usable: true},
			argv:       []interface{}{float64(5)},
			wantIDs:    []int64{1},
		},
		{
			name:       "maxX GT constraint",
			constraint: vtab.IndexConstraint{Column: 2, Op: vtab.ConstraintGT, Usable: true},
			argv:       []interface{}{float64(25)},
			wantIDs:    []int64{3},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			info := &vtab.IndexInfo{
				Constraints: []vtab.IndexConstraint{tt.constraint},
			}
			if err := rt.BestIndex(info); err != nil {
				t.Fatalf("BestIndex: %v", err)
			}

			results := filterWithConstraints(t, rt, info.IdxNum, tt.argv)
			found := make(map[int64]bool)
			for _, e := range results {
				found[e.ID] = true
			}
			for _, id := range tt.wantIDs {
				if !found[id] {
					t.Errorf("expected entry %d in results", id)
				}
			}
			for _, id := range tt.notWantIDs {
				if found[id] {
					t.Errorf("entry %d should NOT be in results", id)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tests for createTable error paths (too few / bad dimensions)
// ---------------------------------------------------------------------------

func TestCoverageCreateTableInvalidDimensions(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()

	// Too many dimensions (> 5)
	_, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11"})
	if err == nil {
		t.Error("expected error for > 5 dimensions")
	}

	// Odd number of coordinate columns
	_, _, err = module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY"})
	if err == nil {
		t.Error("expected error for odd number of coordinate columns")
	}
}

// ---------------------------------------------------------------------------
// Tests for DropShadowTables DDL error path
// ---------------------------------------------------------------------------

func TestDropShadowTablesDDLError(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	mgr := NewShadowTableManager("geo", db, 2)
	// Ensure table entries exist so drop is attempted.
	if err := mgr.CreateShadowTables(); err != nil {
		t.Fatalf("CreateShadowTables: %v", err)
	}

	db.execDDLErr = fmt.Errorf("drop ddl failure")
	err := mgr.DropShadowTables()
	if err == nil {
		t.Error("expected error from DropShadowTables when DDL fails")
	}
}
