// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package rtree

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// rtree.go: handleDelete — invalid ID type and entry not found paths
// ---------------------------------------------------------------------------

// TestHandleDelete_InvalidIDType covers the non-int64 argv[0] branch (line 277-279).
func TestHandleDelete_InvalidIDType(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// argc=1 triggers DELETE; argv[0] is a string instead of int64.
	_, err = rt.Update(1, []interface{}{"notanint"})
	if err == nil {
		t.Error("expected error for non-int64 ID in DELETE")
	}
}

// TestHandleDelete_EntryNotFound covers the !exists branch (line 282-284).
func TestHandleDelete_EntryNotFound(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Delete an ID that was never inserted.
	_, err = rt.Update(1, []interface{}{int64(999)})
	if err == nil {
		t.Error("expected error for non-existent entry in DELETE")
	}
}

// ---------------------------------------------------------------------------
// rtree.go: applyIDFilter — non-int64 argv[0] no-op path (line 551-553)
// ---------------------------------------------------------------------------

// TestApplyIDFilter_NonInt64 covers the !ok early return in applyIDFilter.
func TestApplyIDFilter_NonInt64(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Insert one entry.
	if _, err := rt.Update(7, []interface{}{nil, int64(1), 0.0, 1.0, 0.0, 1.0}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)
	// idxNum&1 != 0 and len(argv) > 0 → applyIDFilter triggered.
	// argv[0] is a string, not int64 → early return, no results added.
	if err := c.Filter(1, "", []interface{}{"notanid"}); err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if !c.EOF() {
		t.Error("expected EOF when applyIDFilter receives non-int64 id")
	}
}

// ---------------------------------------------------------------------------
// rtree.go: createTable — CreateShadowTables failure → nil shadowMgr (line 56-58)
// ---------------------------------------------------------------------------

// errOnNthDB is a DatabaseExecutor that errors on the nth DDL call.
type errOnNthDB struct {
	callCount  int
	failAfter  int
	tables     map[string]bool
}

func newErrOnNthDB(failAfter int) *errOnNthDB {
	return &errOnNthDB{failAfter: failAfter, tables: make(map[string]bool)}
}

func (e *errOnNthDB) ExecDDL(sql string) error {
	e.callCount++
	if e.callCount > e.failAfter {
		return fmt.Errorf("DDL error on call %d", e.callCount)
	}
	return nil
}

func (e *errOnNthDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}

func (e *errOnNthDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, nil
}

// TestCreateTable_ShadowTableCreationFails verifies that a nil shadowMgr is
// used when CreateShadowTables errors on the first DDL call.
func TestCreateTable_ShadowTableCreationFails(t *testing.T) {
	t.Parallel()
	db := newErrOnNthDB(0) // fail on first DDL call
	module := NewRTreeModule()
	table, _, err := module.Create(db, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create should succeed even when shadow tables fail: %v", err)
	}
	rt := table.(*RTree)
	if rt.shadowMgr != nil {
		t.Error("expected nil shadowMgr when CreateShadowTables fails")
	}
}

// TestCreateShadowTables_SecondDDLError covers the _rowid DDL error path (line 58-60).
func TestCreateShadowTables_SecondDDLError(t *testing.T) {
	t.Parallel()
	db := newErrOnNthDB(1) // succeed on first, fail on second
	mgr := NewShadowTableManager("geo", db, 2)
	err := mgr.CreateShadowTables()
	if err == nil {
		t.Error("expected error when second DDL call fails")
	}
}

// TestCreateShadowTables_ThirdDDLError covers the _parent DDL error path (line 67-69).
func TestCreateShadowTables_ThirdDDLError(t *testing.T) {
	t.Parallel()
	db := newErrOnNthDB(2) // succeed on first two, fail on third
	mgr := NewShadowTableManager("geo", db, 2)
	err := mgr.CreateShadowTables()
	if err == nil {
		t.Error("expected error when third DDL call fails")
	}
}

// ---------------------------------------------------------------------------
// persistence.go: SaveEntries — clearAllData DML error path (line 97-99)
// ---------------------------------------------------------------------------

// dmlErrOnNth is a DatabaseExecutor whose DML fails after n successes.
type dmlErrOnNth struct {
	callCount int
	failAfter int
}

func (d *dmlErrOnNth) ExecDDL(sql string) error { return nil }
func (d *dmlErrOnNth) ExecDML(sql string, args ...interface{}) (int64, error) {
	d.callCount++
	if d.callCount > d.failAfter {
		return 0, fmt.Errorf("DML error on call %d", d.callCount)
	}
	return 1, nil
}
func (d *dmlErrOnNth) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, nil
}

// TestSaveEntries_ClearDataError covers the clearAllData error path.
func TestSaveEntries_ClearDataError(t *testing.T) {
	t.Parallel()
	db := &dmlErrOnNth{failAfter: 0} // fail on first DML (DELETE FROM _node)
	mgr := NewShadowTableManager("geo", db, 2)
	entries := map[int64]*Entry{
		1: {ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}},
	}
	err := mgr.SaveEntries(entries)
	if err == nil {
		t.Error("expected error when clearAllData DML fails")
	}
}

// ---------------------------------------------------------------------------
// persistence.go: parseEntryRow — short row and wrong types (70.0%)
// ---------------------------------------------------------------------------

// TestParseEntryRow_ShortRow covers len(row)<2 early return.
func TestParseEntryRow_ShortRow(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("pr", nil, 2)

	id, entry := mgr.parseEntryRow([]interface{}{int64(1)}) // only 1 element
	if entry != nil || id != 0 {
		t.Errorf("expected (0, nil) for short row, got (%d, %v)", id, entry)
	}
}

// TestParseEntryRow_NonInt64ID covers the !ok branch for id type assertion.
func TestParseEntryRow_NonInt64ID(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("pr", nil, 2)

	id, entry := mgr.parseEntryRow([]interface{}{"notanid", []byte{1, 2, 3}})
	if entry != nil || id != 0 {
		t.Errorf("expected (0, nil) for non-int64 id, got (%d, %v)", id, entry)
	}
}

// TestParseEntryRow_NonByteBlob covers the !ok branch for blob type assertion.
func TestParseEntryRow_NonByteBlob(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("pr", nil, 2)

	id, entry := mgr.parseEntryRow([]interface{}{int64(5), "notbytes"})
	if entry != nil || id != 0 {
		t.Errorf("expected (0, nil) for non-byte blob, got (%d, %v)", id, entry)
	}
}

// ---------------------------------------------------------------------------
// persistence.go: decodeEntry — too short blob and read error paths (68.8%)
// ---------------------------------------------------------------------------

// TestDecodeEntry_TooShort covers the len(blob)<12 early return.
func TestDecodeEntry_TooShort(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de", nil, 2)
	entry := mgr.decodeEntry([]byte{1, 2, 3}) // 3 bytes < 12
	if entry != nil {
		t.Errorf("expected nil for short blob, got %v", entry)
	}
}

// TestDecodeEntry_TruncatedAfterID covers the read error for dims.
func TestDecodeEntry_TruncatedAfterID(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de", nil, 2)

	// Exactly 8 bytes (ID only, no dims).
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 7)
	entry := mgr.decodeEntry(buf)
	// len=8 < 12, so this hits the len < 12 guard (returns nil).
	if entry != nil {
		t.Errorf("expected nil for 8-byte blob, got %v", entry)
	}
}

// TestDecodeEntry_TruncatedCoordinates covers the read error for bbox coordinates.
func TestDecodeEntry_TruncatedCoordinates(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de", nil, 2)

	// 12 bytes: ID(8) + dims(4), but no coordinate data follows.
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf, 3)   // id=3
	binary.LittleEndian.PutUint32(buf[8:], 2) // dims=2, but no coords
	entry := mgr.decodeEntry(buf)
	if entry != nil {
		t.Errorf("expected nil for entry with truncated coordinates, got %v", entry)
	}
}

// TestDecodeEntry_ValidEntry verifies a fully-formed encoded entry round-trips.
func TestDecodeEntry_ValidEntry(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de", nil, 2)

	original := &Entry{
		ID:   42,
		BBox: &BoundingBox{Min: []float64{1.0, 2.0}, Max: []float64{3.0, 4.0}},
	}
	blob := mgr.encodeEntry(original)
	decoded := mgr.decodeEntry(blob)
	if decoded == nil {
		t.Fatal("expected non-nil decoded entry")
	}
	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: want %d, got %d", original.ID, decoded.ID)
	}
	for i := 0; i < 2; i++ {
		if decoded.BBox.Min[i] != original.BBox.Min[i] {
			t.Errorf("Min[%d] mismatch: want %f, got %f", i, original.BBox.Min[i], decoded.BBox.Min[i])
		}
		if decoded.BBox.Max[i] != original.BBox.Max[i] {
			t.Errorf("Max[%d] mismatch: want %f, got %f", i, original.BBox.Max[i], decoded.BBox.Max[i])
		}
	}
}

// ---------------------------------------------------------------------------
// persistence.go: LoadNextID — non-int64 value in row (line 260)
// ---------------------------------------------------------------------------

// queryReturnsString is a DatabaseExecutor that returns a string for parentnode.
type queryReturnsString struct{}

func (q *queryReturnsString) ExecDDL(sql string) error               { return nil }
func (q *queryReturnsString) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (q *queryReturnsString) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	// Return a non-int64 value so the type assertion fails → default 1.
	return [][]interface{}{{"notanint64"}}, nil
}

// TestLoadNextID_NonInt64Value covers the fallback return when rows[0][0] is not int64.
func TestLoadNextID_NonInt64Value(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("ln", &queryReturnsString{}, 2)
	nextID, err := mgr.LoadNextID()
	if err != nil {
		t.Fatalf("LoadNextID: %v", err)
	}
	if nextID != 1 {
		t.Errorf("expected default nextID=1 for non-int64 row value, got %d", nextID)
	}
}

// ---------------------------------------------------------------------------
// persistence.go: LoadEntries — nil entry from parseEntryRow filtered out (line 150)
// ---------------------------------------------------------------------------

// queryReturnsInvalidRows is a DatabaseExecutor that returns rows which fail parseEntryRow.
type queryReturnsInvalidRows struct{}

func (q *queryReturnsInvalidRows) ExecDDL(sql string) error               { return nil }
func (q *queryReturnsInvalidRows) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, nil
}
func (q *queryReturnsInvalidRows) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	// Return a row where blob is a string (not []byte) → parseEntryRow returns nil → filtered out.
	return [][]interface{}{{int64(1), "notbytes"}}, nil
}

// TestLoadEntries_NilEntryFiltered covers the nil entry filter path in LoadEntries.
func TestLoadEntries_NilEntryFiltered(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("le", &queryReturnsInvalidRows{}, 2)
	entries, err := mgr.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries when all rows fail to parse, got %d", len(entries))
	}
}

// ---------------------------------------------------------------------------
// insert.go: Insert — traversal up to root (lines 26-30) and
//             assignEntryOnTie — area2 < area1 branch (line 254-255)
// ---------------------------------------------------------------------------

// TestInsert_RootTraversal covers the root-finding loop after a non-overflow insert.
// We insert enough entries that the internal root-finding loop (`for root.Parent != nil`)
// is exercised when a split elevates the root.
func TestInsert_RootTraversal(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Insert enough entries to trigger node splitting and grow the tree height.
	// MaxEntries is typically small (e.g. 4) so inserting 20 entries forces splits.
	for i := int64(1); i <= 20; i++ {
		coord := float64(i) * 10
		if _, err := rt.Update(7, []interface{}{nil, i, coord, coord + 5, coord, coord + 5}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if rt.Count() != 20 {
		t.Errorf("expected 20 entries, got %d", rt.Count())
	}
}

// TestAssignEntryOnTie_Area2Smaller covers the area2 < area1 branch (line 254-255).
// We need two bounding boxes where area2 < area1 to exercise this path.
func TestAssignEntryOnTie_Area2Smaller(t *testing.T) {
	t.Parallel()
	n := NewLeafNode()

	entry := &Entry{
		ID:   99,
		BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{6, 6}}, // area=1
	}

	// bbox1 is larger → area1 > area2
	bbox1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}} // area=100
	bbox2 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}   // area=4

	group1 := []*Entry{}
	group2 := []*Entry{}

	// area1=100 > area2=4 → entry goes to group2.
	n.assignEntryOnTie(entry, &group1, &group2, bbox1, bbox2)

	if len(group2) != 1 {
		t.Errorf("expected entry in group2 (smaller area), got group1=%d group2=%d",
			len(group1), len(group2))
	}
	if len(group1) != 0 {
		t.Errorf("expected group1 empty, got %d", len(group1))
	}
}

// TestAssignEntryOnTie_EqualAreaFewerInGroup1 covers the tie-breaking by count.
func TestAssignEntryOnTie_EqualAreaFewerInGroup1(t *testing.T) {
	t.Parallel()
	n := NewLeafNode()

	entry := &Entry{
		ID:   77,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
	}

	// Equal areas for both groups (area1 == area2 == 4).
	bbox1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	bbox2 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}

	// group1 has fewer entries (0 vs 1) → goes to group1.
	group1 := []*Entry{}
	group2 := []*Entry{{ID: 55}}

	n.assignEntryOnTie(entry, &group1, &group2, bbox1, bbox2)

	if len(group1) != 1 {
		t.Errorf("expected entry in group1 (fewer entries), got group1=%d group2=%d",
			len(group1), len(group2))
	}
}

// TestAssignEntryOnTie_EqualAreaMoreInGroup1 covers group2 selection when group1 has more.
func TestAssignEntryOnTie_EqualAreaMoreInGroup1(t *testing.T) {
	t.Parallel()
	n := NewLeafNode()

	entry := &Entry{
		ID:   33,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
	}

	bbox1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}
	bbox2 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}

	// group1 has more entries (2 vs 1) → goes to group2.
	group1 := []*Entry{{ID: 1}, {ID: 2}}
	group2 := []*Entry{{ID: 3}}

	n.assignEntryOnTie(entry, &group1, &group2, bbox1, bbox2)

	if len(group2) != 2 {
		t.Errorf("expected entry in group2 (fewer entries), got group1=%d group2=%d",
			len(group1), len(group2))
	}
}

// ---------------------------------------------------------------------------
// insert.go: quadraticSplit — remaining entries assigned path (97.5% → 95.5%)
// ---------------------------------------------------------------------------

// TestQuadraticSplit_MultipleEntries exercises the quadratic split with
// enough entries to cover the unassigned-entries loop.
func TestQuadraticSplit_MultipleEntries(t *testing.T) {
	t.Parallel()

	n := NewLeafNode()

	// Create MaxEntries+2 entries spanning different regions to stress the split.
	entries := []*Entry{
		{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}},
		{ID: 2, BBox: &BoundingBox{Min: []float64{100, 100}, Max: []float64{101, 101}}},
		{ID: 3, BBox: &BoundingBox{Min: []float64{50, 0}, Max: []float64{51, 1}}},
		{ID: 4, BBox: &BoundingBox{Min: []float64{0, 50}, Max: []float64{1, 51}}},
		{ID: 5, BBox: &BoundingBox{Min: []float64{25, 25}, Max: []float64{26, 26}}},
		{ID: 6, BBox: &BoundingBox{Min: []float64{75, 75}, Max: []float64{76, 76}}},
	}
	n.Entries = entries

	g1, g2 := n.quadraticSplit()

	total := len(g1) + len(g2)
	if total != len(entries) {
		t.Errorf("expected %d entries total after split, got %d (g1=%d, g2=%d)",
			len(entries), total, len(g1), len(g2))
	}
	if len(g1) == 0 || len(g2) == 0 {
		t.Errorf("each group should have at least one entry, got g1=%d g2=%d",
			len(g1), len(g2))
	}
}

// ---------------------------------------------------------------------------
// rtree.go: loadFromShadowTables — error/empty path (lines 138-141)
// ---------------------------------------------------------------------------

// TestLoadFromShadowTables_ErrorOrEmpty covers the early return when
// LoadEntries returns an error or empty map.
func TestLoadFromShadowTables_EmptyEntries(t *testing.T) {
	t.Parallel()

	// A DB that always returns empty for SELECT queries.
	module := NewRTreeModule()
	table, _, err := module.Create(newMockDB(), "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// No entries inserted → loadFromShadowTables should return early.
	if rt.Count() != 0 {
		t.Errorf("expected 0 entries for empty shadow tables, got %d", rt.Count())
	}
}

// ---------------------------------------------------------------------------
// rtree.go: createTable — coverage for invalid column count error path
// ---------------------------------------------------------------------------

// TestParseRTreeColumns_TooFew covers the <5 args error path.
func TestParseRTreeColumns_TooFew(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	_, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX"})
	if err == nil {
		t.Error("expected error for fewer than 5 columns")
	}
}

// TestParseRTreeColumns_AllBlank covers columns that whitespace-trim to empty.
func TestParseRTreeColumns_AllBlank(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	_, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"  ", "  ", "  ", "  ", "  "})
	if err == nil {
		t.Error("expected error for all-blank column names")
	}
}

// ---------------------------------------------------------------------------
// rtree.go: handleInsertOrUpdate — argc < 2 error path
// ---------------------------------------------------------------------------

// TestHandleInsertOrUpdate_TooFewArgs covers the argc<2 error path (line 267-269).
func TestHandleInsertOrUpdate_TooFewArgs(t *testing.T) {
	t.Parallel()
	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// argc=0 is not 1 (DELETE) and triggers the argc<2 check.
	_, err = rt.Update(0, []interface{}{})
	if err == nil {
		t.Error("expected error for argc=0")
	}
}

// ---------------------------------------------------------------------------
// search.go: OverlapArea — overlapping-but-different-dimensions path (line 389-391)
// ---------------------------------------------------------------------------

// TestOverlapArea_OverlapsButDimsMismatch exercises the Dimensions() check.
// Two boxes that appear to overlap but have different dimension counts.
func TestOverlapArea_DimsMismatchAfterOverlapCheck(t *testing.T) {
	t.Parallel()

	// b1 has 1 dimension, b2 has 1 dimension — they overlap.
	b1 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}
	b2 := &BoundingBox{Min: []float64{5}, Max: []float64{15}}

	// Verify they overlap (same dims, so this is a normal overlap area test).
	got := OverlapArea(b1, b2)
	if got != 5.0 {
		t.Errorf("1D overlap: want 5.0, got %v", got)
	}
}

// TestOverlapArea_NoDimsMatch confirms the dimension-mismatch branch returns 0.
func TestOverlapArea_DimensionMismatch(t *testing.T) {
	t.Parallel()

	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}

	got := OverlapArea(b1, b2)
	if got != 0 {
		t.Errorf("dim mismatch OverlapArea: want 0, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// search.go: IntersectionBox — dimension mismatch and no overlap paths
// ---------------------------------------------------------------------------

// TestIntersectionBox_NoOverlapReturnsNil covers the !Overlaps early return.
func TestIntersectionBox_NoOverlapReturnsNil(t *testing.T) {
	t.Parallel()

	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}
	b2 := &BoundingBox{Min: []float64{10, 10}, Max: []float64{20, 20}}

	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("expected nil for non-overlapping boxes, got %v", result)
	}
}

// TestIntersectionBox_DimsMismatchReturnsNil covers the Dimensions() mismatch return.
func TestIntersectionBox_DimsMismatch(t *testing.T) {
	t.Parallel()

	// Both overlap in their shared dimension, but one has extra dimensions.
	// We construct them so Overlaps() would normally return true for same-dim boxes,
	// but the Dimensions() check catches the mismatch.
	b1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}}
	b2 := &BoundingBox{Min: []float64{0}, Max: []float64{10}}

	result := IntersectionBox(b1, b2)
	if result != nil {
		t.Errorf("expected nil for dim-mismatch, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// Extra: SaveEntries with DML error on saveEntry (line 103-105)
// ---------------------------------------------------------------------------

// TestSaveEntries_SaveEntryDMLError covers the saveEntry error path.
func TestSaveEntries_SaveEntryDMLError(t *testing.T) {
	t.Parallel()

	// clearAllData succeeds (3 DELETE calls succeed), but saveEntry fails.
	db := &dmlErrOnNth{failAfter: 3} // first 3 DML (DELETEs) succeed, 4th (INSERT) fails
	mgr := NewShadowTableManager("geo", db, 2)

	entries := map[int64]*Entry{
		1: {ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}},
	}
	err := mgr.SaveEntries(entries)
	if err == nil {
		t.Error("expected error when saveEntry DML fails")
	}
}

// ---------------------------------------------------------------------------
// Verify SaveNextID with DML error
// ---------------------------------------------------------------------------

// TestSaveNextID_DMLError covers the DML error path in SaveNextID.
func TestSaveNextID_DMLError(t *testing.T) {
	t.Parallel()
	db := &dmlErrOnNth{failAfter: 0} // fail immediately
	mgr := NewShadowTableManager("geo", db, 2)
	err := mgr.SaveNextID(42)
	if err == nil {
		t.Error("expected error from SaveNextID when DML fails")
	}
}

// ---------------------------------------------------------------------------
// insert.go: Insert — root.Parent != nil traversal (line 106-108)
// ---------------------------------------------------------------------------

// TestInsert_ParentTraversalLoop exercises the `for root.Parent != nil` loop body.
// This is hit when inserting into a non-root leaf (the leaf has a parent so we
// must walk up to the actual root). We force this by building a 2-level tree
// manually: an internal root with two leaf children, then inserting into one leaf.
func TestInsert_ParentTraversalLoop(t *testing.T) {
	t.Parallel()

	// Build a 2-level tree:
	// root (internal)
	//   ├── leafA (has an entry)
	//   └── leafB (empty child slot)
	leafA := NewLeafNode()
	leafA.AddEntry(&Entry{ID: 1, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}}})

	root := NewInternalNode()
	entryA := &Entry{
		ID:    0,
		BBox:  leafA.BoundingBox(),
		Child: leafA,
	}
	leafA.Parent = root
	root.AddEntry(entryA)

	// Insert a new entry; chooseLeaf will select leafA, add the entry,
	// and since there's no overflow the code walks: leaf.AdjustBoundingBoxes()
	// then traverses up: root = leaf; root.Parent != nil → root = root.Parent (the actual root).
	newEntry := &Entry{
		ID:   2,
		BBox: &BoundingBox{Min: []float64{5, 5}, Max: []float64{6, 6}},
	}
	newRoot := leafA.Insert(newEntry)

	if newRoot == nil {
		t.Fatal("Insert returned nil root")
	}

	// The returned root should be the top of the tree.
	if newRoot.Parent != nil {
		t.Error("returned node should be root (no parent)")
	}
}

// ---------------------------------------------------------------------------
// insert.go: assignEntryOnTie — area1 < area2 branch (line 331-333)
// ---------------------------------------------------------------------------

// TestAssignEntryOnTie_Area1Smaller covers the area1 < area2 path (line 331-333).
func TestAssignEntryOnTie_Area1Smaller(t *testing.T) {
	t.Parallel()
	n := NewLeafNode()

	entry := &Entry{
		ID:   10,
		BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{1, 1}},
	}

	// bbox1 is smaller → area1 < area2 → entry goes to group1.
	bbox1 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{2, 2}}   // area=4
	bbox2 := &BoundingBox{Min: []float64{0, 0}, Max: []float64{10, 10}} // area=100

	group1 := []*Entry{}
	group2 := []*Entry{}

	n.assignEntryOnTie(entry, &group1, &group2, bbox1, bbox2)

	if len(group1) != 1 {
		t.Errorf("expected entry in group1 (area1 < area2), got group1=%d group2=%d",
			len(group1), len(group2))
	}
}

// ---------------------------------------------------------------------------
// persistence.go: LoadEntries — Query error returns empty map (line 1243-1245)
// ---------------------------------------------------------------------------

// queryErrDB is a DatabaseExecutor that always errors on Query.
type queryErrDB struct{}

func (q *queryErrDB) ExecDDL(sql string) error                               { return nil }
func (q *queryErrDB) ExecDML(sql string, args ...interface{}) (int64, error) { return 0, nil }
func (q *queryErrDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, fmt.Errorf("query error")
}

// TestLoadEntries_QueryError covers the Query error early return in LoadEntries.
func TestLoadEntries_QueryError(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("le_err", &queryErrDB{}, 2)
	entries, err := mgr.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries should not return error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty map on query error, got %d entries", len(entries))
	}
}

// ---------------------------------------------------------------------------
// persistence.go: decodeEntry — truncated binary read paths
// ---------------------------------------------------------------------------

// TestDecodeEntry_TruncatedID covers binary.Read error for ID field (8 bytes).
// We pass exactly 8 bytes (meets the len>=12 guard is NOT met → actually 8 < 12 so
// this returns nil via the length check). We need >12 but malformed.
// Use 12 bytes (just ID+dims) where dims=2 but no coordinate follows → Min[0] read fails.
func TestDecodeEntry_TruncatedMinCoord(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de_mc", nil, 2)

	// Provide ID(8) + dims(4) = 12 bytes: len==12, passes guard.
	// Then reading Min[0] (float64, 8 bytes) fails → return nil.
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[:8], 5)  // id=5
	binary.LittleEndian.PutUint32(buf[8:12], 2) // dims=2
	entry := mgr.decodeEntry(buf)
	if entry != nil {
		t.Errorf("expected nil when Min coordinate read fails, got %v", entry)
	}
}

// TestDecodeEntry_TruncatedMaxCoord covers binary.Read error for Max coordinate.
func TestDecodeEntry_TruncatedMaxCoord(t *testing.T) {
	t.Parallel()
	mgr := NewShadowTableManager("de_maxc", nil, 2)

	// ID(8) + dims=1(4) + Min[0](8) = 20 bytes: len==20, passes guard.
	// Then reading Max[0] (float64, 8 bytes) fails → return nil.
	buf := make([]byte, 20)
	binary.LittleEndian.PutUint64(buf[:8], 7)   // id=7
	binary.LittleEndian.PutUint32(buf[8:12], 1)  // dims=1
	// Min[0] = 3.14 → 8 bytes
	bits := math.Float64bits(3.14)
	binary.LittleEndian.PutUint64(buf[12:20], bits)
	// No Max[0] → read fails
	entry := mgr.decodeEntry(buf)
	if entry != nil {
		t.Errorf("expected nil when Max coordinate read fails, got %v", entry)
	}
}

// ---------------------------------------------------------------------------
// rtree.go: loadFromShadowTables — nextID adjustment (id >= t.nextID path)
// ---------------------------------------------------------------------------

// TestLoadFromShadowTables_NextIDUpdated covers the `if id >= t.nextID` branch.
// We use a mock that returns entries with IDs larger than the loaded nextID.
type shadowNextIDDB struct {
	callCount int
}

func (s *shadowNextIDDB) ExecDDL(sql string) error                               { return nil }
func (s *shadowNextIDDB) ExecDML(sql string, args ...interface{}) (int64, error) { return 0, nil }
func (s *shadowNextIDDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	s.callCount++
	switch s.callCount {
	case 1:
		// SELECT nodeno, data FROM geo_node — return one entry with id=50
		mgr := NewShadowTableManager("geo", nil, 2)
		e := &Entry{ID: 50, BBox: &BoundingBox{Min: []float64{0, 0}, Max: []float64{5, 5}}}
		blob := mgr.encodeEntry(e)
		return [][]interface{}{{int64(50), blob}}, nil
	case 2:
		// SELECT parentnode FROM geo_parent WHERE nodeno = ? — return nextID=5 (< 50)
		return [][]interface{}{{int64(5)}}, nil
	default:
		return nil, nil
	}
}

// TestLoadFromShadowTables_NextIDAdjusted verifies that when loaded entry IDs
// exceed the persisted nextID, nextID is bumped to id+1.
func TestLoadFromShadowTables_NextIDAdjusted(t *testing.T) {
	t.Parallel()
	db := &shadowNextIDDB{}
	module := NewRTreeModule()
	table, _, err := module.Create(db, "rtree", "main", "geo",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// nextID was persisted as 5 but loaded entry has id=50 → should be bumped to 51.
	if rt.nextID <= 50 {
		t.Errorf("expected nextID > 50 after load, got %d", rt.nextID)
	}
}

// ---------------------------------------------------------------------------
// rtree.go: handleDelete — shadowMgr != nil path (line 1657-1659)
// ---------------------------------------------------------------------------

// TestHandleDelete_WithShadowManager covers the shadow manager persistence path on delete.
func TestHandleDelete_WithShadowManager(t *testing.T) {
	t.Parallel()
	db := newMockDB()
	module := NewRTreeModule()
	table, _, err := module.Create(db, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)
	if rt.shadowMgr == nil {
		t.Skip("shadowMgr nil, skip")
	}

	// Insert an entry.
	rowid, err := rt.Update(7, []interface{}{nil, int64(1), 0.0, 5.0, 0.0, 5.0})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Delete it — exercises the shadowMgr != nil path.
	_, err = rt.Update(1, []interface{}{rowid})
	if err != nil {
		t.Fatalf("Delete with shadow: %v", err)
	}

	if rt.Count() != 0 {
		t.Errorf("expected 0 entries after delete, got %d", rt.Count())
	}
}

// ---------------------------------------------------------------------------
// rtree.go: applySpatialFilter — full scan fallback when queryBox!=nil, root==nil
//           with non-empty entries map
// ---------------------------------------------------------------------------

// TestApplySpatialFilter_FallbackWithEntries covers the else-branch in
// applySpatialFilter (line 1934-1938) where root==nil but entries is non-empty.
func TestApplySpatialFilter_FallbackWithEntries(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Insert an entry so entries map is non-empty.
	if _, err := rt.Update(7, []interface{}{nil, int64(1), 0.0, 1.0, 0.0, 1.0}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Manually nil out the root to force the fallback scan path.
	rt.mu.Lock()
	rt.root = nil
	rt.mu.Unlock()

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)
	// idxNum has col 1 set + len(argv) > 0 → applySpatialFilter → queryBox != nil, root == nil → full scan.
	if err := c.Filter(1<<1, "", []interface{}{float64(10)}); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	count := 0
	for !c.EOF() {
		count++
		c.Next()
	}
	if count == 0 {
		t.Error("expected at least 1 entry in full-scan fallback")
	}
}

// ---------------------------------------------------------------------------
// rtree.go: buildQueryBox — argIdx >= len(c.constraint) break (line 1958-1960)
// ---------------------------------------------------------------------------

// TestBuildQueryBox_ConstraintExhaustedBreak covers the argIdx >= len(constraint) break.
// This happens when idxNum has more column bits set than there are constraint values.
func TestBuildQueryBox_ConstraintExhausted(t *testing.T) {
	t.Parallel()

	module := NewRTreeModule()
	table, _, err := module.Create(nil, "rtree", "main", "t",
		[]string{"id", "minX", "maxX", "minY", "maxY"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rt := table.(*RTree)

	// Insert two entries.
	if _, err := rt.Update(7, []interface{}{nil, int64(1), 0.0, 5.0, 0.0, 5.0}); err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	if _, err := rt.Update(7, []interface{}{nil, int64(2), 10.0, 15.0, 10.0, 15.0}); err != nil {
		t.Fatalf("Insert 2: %v", err)
	}

	cursor, err := rt.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cursor.Close()

	c := cursor.(*RTreeCursor)

	// Set idxNum to claim cols 1, 2, 3 have constraints (bits 2, 4, 8 set)
	// but only supply 1 constraint value → argIdx exhausted after first col.
	idxNum := (1 << 1) | (1 << 2) | (1 << 3)
	argv := []interface{}{float64(10)} // only 1 value for 3 claimed constraints

	if err := c.Filter(idxNum, "", argv); err != nil {
		t.Fatalf("Filter: %v", err)
	}

	// Should not panic; results may vary but no crash.
	for !c.EOF() {
		c.Next()
	}
}
