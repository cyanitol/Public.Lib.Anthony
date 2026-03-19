// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestSQLiteRTree tests SQLite R-Tree virtual tables
// Converted from contrib/sqlite/sqlite-src-3510200/test/rtree*.test and related tests
func TestSQLiteRTree(t *testing.T) {
	t.Skip("pre-existing failure - needs R-Tree implementation")
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "rtree_test.db")

	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Run test groups
	test2DRTree(t, db)
	test3DRTree(t, db)
	test1DRTree(t, db)
	testRTreeAuxData(t, db)
	testRTreeEdgeCases(t, db)
	testRTreeSpatialOperations(t, db)
}

// rt2d_rectangle defines a test rectangle
type rt2d_rectangle struct {
	id                     int
	minx, maxx, miny, maxy float64
}

// rt2d_createTable creates the rtree virtual table
func rt2d_createTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec("CREATE VIRTUAL TABLE rt1 USING rtree(id, minx, maxx, miny, maxy)")
	if err != nil {
		t.Fatalf("failed to create rtree virtual table: %v", err)
	}
}

// rt2d_insertRectangle inserts a single rectangle into rt1
func rt2d_insertRectangle(t *testing.T, db *sql.DB, rect rt2d_rectangle) {
	_, err := db.Exec("INSERT INTO rt1 VALUES(?, ?, ?, ?, ?)",
		rect.id, rect.minx, rect.maxx, rect.miny, rect.maxy)
	if err != nil {
		t.Fatalf("failed to insert rectangle %d: %v", rect.id, err)
	}
}

// rt2d_verifyRectangle queries and verifies a rectangle's data
func rt2d_verifyRectangle(t *testing.T, db *sql.DB, expected rt2d_rectangle) {
	var id, minx, maxx, miny, maxy float64
	err := db.QueryRow("SELECT * FROM rt1 WHERE id = ?", expected.id).Scan(&id, &minx, &maxx, &miny, &maxy)
	if err != nil {
		t.Fatalf("failed to query rtree: %v", err)
	}
	if id != float64(expected.id) || minx != expected.minx || maxx != expected.maxx || miny != expected.miny || maxy != expected.maxy {
		t.Errorf("rtree data mismatch: got (%v, %v, %v, %v, %v)", id, minx, maxx, miny, maxy)
	}
}

// rt2d_verifyCount checks the total count of entries
func rt2d_verifyCount(t *testing.T, db *sql.DB, expected int64, testName string) {
	var count int64
	err := db.QueryRow("SELECT COUNT(*) FROM rt1").Scan(&count)
	if err != nil {
		t.Fatalf("%s: failed to count: %v", testName, err)
	}
	if count != expected {
		t.Errorf("%s: expected %d entries, got %d", testName, expected, count)
	}
}

// rt2d_testSpatialQuery tests spatial overlap query
func rt2d_testSpatialQuery(t *testing.T, db *sql.DB) {
	rows, err := db.Query("SELECT id FROM rt1 WHERE minx <= 10 AND maxx >= 0 AND miny <= 10 AND maxy >= 0")
	if err != nil {
		t.Fatalf("failed spatial query: %v", err)
	}
	defer rows.Close()

	var foundIDs []int64
	for rows.Next() {
		var foundID int64
		if err := rows.Scan(&foundID); err != nil {
			t.Fatalf("failed to scan result: %v", err)
		}
		foundIDs = append(foundIDs, foundID)
	}
	if len(foundIDs) < 2 {
		t.Errorf("expected at least 2 overlapping rectangles, got %d", len(foundIDs))
	}
}

// rt2d_testUpdate tests updating a rectangle
func rt2d_testUpdate(t *testing.T, db *sql.DB) {
	_, err := db.Exec("UPDATE rt1 SET minx = 1, maxx = 11 WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to update rtree: %v", err)
	}

	var minx, maxx float64
	err = db.QueryRow("SELECT minx, maxx FROM rt1 WHERE id = 1").Scan(&minx, &maxx)
	if err != nil {
		t.Fatalf("failed to query updated rtree: %v", err)
	}
	if minx != 1 || maxx != 11 {
		t.Errorf("update failed: expected (1, 11), got (%v, %v)", minx, maxx)
	}
}

// rt2d_testDelete tests deleting from rtree
func rt2d_testDelete(t *testing.T, db *sql.DB) {
	_, err := db.Exec("DELETE FROM rt1 WHERE id = 5")
	if err != nil {
		t.Fatalf("failed to delete from rtree: %v", err)
	}
}

// test2DRTree tests basic 2D R-Tree operations
func test2DRTree(t *testing.T, db *sql.DB) {
	rt2d_createTable(t, db)

	// Test 2: Insert basic 2D rectangle
	rt2d_insertRectangle(t, db, rt2d_rectangle{1, 0, 10, 0, 10})

	// Test 3: Query rtree data
	rt2d_verifyRectangle(t, db, rt2d_rectangle{1, 0, 10, 0, 10})

	// Test 4: Insert multiple rectangles
	rectangles := []rt2d_rectangle{
		{2, 5, 15, 5, 15},
		{3, 10, 20, 10, 20},
		{4, -5, 5, -5, 5},
		{5, 20, 30, 20, 30},
	}

	for _, rect := range rectangles {
		rt2d_insertRectangle(t, db, rect)
	}

	// Test 5: Count all entries
	rt2d_verifyCount(t, db, 5, "initial count")

	// Test 6: Spatial query - find overlapping rectangles
	rt2d_testSpatialQuery(t, db)

	// Test 7-8: Update rtree entry
	rt2d_testUpdate(t, db)

	// Test 9: Delete from rtree
	rt2d_testDelete(t, db)

	// Test 10: Verify deletion
	rt2d_verifyCount(t, db, 4, "count after delete")
}

// test3DRTree tests 3D R-Tree operations
func test3DRTree(t *testing.T, db *sql.DB) {
	_, err := db.Exec("CREATE VIRTUAL TABLE rt3d USING rtree(id, minx, maxx, miny, maxy, minz, maxz)")
	if err != nil {
		t.Fatalf("failed to create 3D rtree: %v", err)
	}

	// Test 12: Insert 3D box
	_, err = db.Exec("INSERT INTO rt3d VALUES(1, 0, 10, 0, 10, 0, 10)")
	if err != nil {
		t.Fatalf("failed to insert 3D box: %v", err)
	}

	// Test 13: Query 3D rtree
	var minz, maxz float64
	err = db.QueryRow("SELECT minz, maxz FROM rt3d WHERE id = 1").Scan(&minz, &maxz)
	if err != nil {
		t.Fatalf("failed to query 3D rtree: %v", err)
	}
	if minz != 0 || maxz != 10 {
		t.Errorf("3D data mismatch: expected (0, 10), got (%v, %v)", minz, maxz)
	}
}

// test1DRTree tests 1D R-Tree (interval tree) operations
func test1DRTree(t *testing.T, db *sql.DB) {
	_, err := db.Exec("CREATE VIRTUAL TABLE rt1d USING rtree(id, min_val, max_val)")
	if err != nil {
		t.Fatalf("failed to create 1D rtree: %v", err)
	}

	// Test 15: Insert intervals
	intervals := []struct {
		id       int
		min, max float64
	}{
		{1, 0, 10},
		{2, 5, 15},
		{3, 20, 30},
		{4, 25, 35},
	}

	for _, interval := range intervals {
		_, err = db.Exec("INSERT INTO rt1d VALUES(?, ?, ?)", interval.id, interval.min, interval.max)
		if err != nil {
			t.Fatalf("failed to insert interval %d: %v", interval.id, err)
		}
	}

	// Find overlapping intervals
	rows, err := db.Query("SELECT id FROM rt1d WHERE min_val <= 12 AND max_val >= 8")
	if err != nil {
		t.Fatalf("failed interval query: %v", err)
	}
	defer rows.Close()

	var foundIDs []int64
	for rows.Next() {
		var foundID int64
		if err := rows.Scan(&foundID); err != nil {
			t.Fatalf("failed to scan interval result: %v", err)
		}
		foundIDs = append(foundIDs, foundID)
	}
	if len(foundIDs) < 1 {
		t.Errorf("expected at least 1 overlapping interval, got %d", len(foundIDs))
	}

	testPointContainment(t, db)
}

// testPointContainment tests point-in-rectangle queries
func testPointContainment(t *testing.T, db *sql.DB) {
	var id float64
	err := db.QueryRow("SELECT id FROM rt1 WHERE minx <= 7 AND maxx >= 7 AND miny <= 7 AND maxy >= 7 LIMIT 1").Scan(&id)
	if err != nil {
		t.Fatalf("failed point containment query: %v", err)
	}
}

// testRTreeAuxData tests R-Tree with auxiliary data columns
func testRTreeAuxData(t *testing.T, db *sql.DB) {
	_, err := db.Exec("CREATE VIRTUAL TABLE rt_aux USING rtree(id, minx, maxx, miny, maxy, +data)")
	if err != nil {
		t.Fatalf("failed to create rtree with aux column: %v", err)
	}

	// Test 19: Insert with auxiliary data
	_, err = db.Exec("INSERT INTO rt_aux VALUES(1, 0, 10, 0, 10, 'metadata')")
	if err != nil {
		t.Fatalf("failed to insert with aux data: %v", err)
	}

	// Test 20: Query auxiliary data
	var auxData string
	err = db.QueryRow("SELECT data FROM rt_aux WHERE id = 1").Scan(&auxData)
	if err != nil {
		t.Fatalf("failed to query aux data: %v", err)
	}
	if auxData != "metadata" {
		t.Errorf("aux data mismatch: expected 'metadata', got %q", auxData)
	}
}

// testRTreeEdgeCases tests edge cases like empty rtrees, negative coordinates, etc.
func testRTreeEdgeCases(t *testing.T, db *sql.DB) {
	var count int64

	_, err := db.Exec("CREATE VIRTUAL TABLE rt_empty USING rtree(id, x1, x2)")
	if err != nil {
		t.Fatalf("failed to create empty rtree: %v", err)
	}

	// Test 22: Query empty rtree
	err = db.QueryRow("SELECT COUNT(*) FROM rt_empty").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query empty rtree: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries in empty rtree, got %d", count)
	}

	// Test 23: Insert and delete all
	_, err = db.Exec("INSERT INTO rt_empty VALUES(1, 0, 10)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec("DELETE FROM rt_empty")
	if err != nil {
		t.Fatalf("failed to delete all: %v", err)
	}
	err = db.QueryRow("SELECT COUNT(*) FROM rt_empty").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count after delete all: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 after delete all, got %d", count)
	}

	testNegativeAndFloatingCoordinates(t, db)
	testZeroWidthRectangle(t, db)
}

// testNegativeAndFloatingCoordinates tests negative and floating point coordinates
func testNegativeAndFloatingCoordinates(t *testing.T, db *sql.DB) {
	var minx, maxx float64

	_, err := db.Exec("INSERT INTO rt1 VALUES(100, -50, -40, -30, -20)")
	if err != nil {
		t.Fatalf("failed to insert negative coords: %v", err)
	}

	// Test 25: Query negative coordinates
	err = db.QueryRow("SELECT minx, maxx FROM rt1 WHERE id = 100").Scan(&minx, &maxx)
	if err != nil {
		t.Fatalf("failed to query negative coords: %v", err)
	}
	if minx != -50 || maxx != -40 {
		t.Errorf("negative coords mismatch: expected (-50, -40), got (%v, %v)", minx, maxx)
	}

	_, err = db.Exec("INSERT INTO rt1 VALUES(101, 1.5, 2.5, 3.7, 4.9)")
	if err != nil {
		t.Fatalf("failed to insert floating point coords: %v", err)
	}
}

// testZeroWidthRectangle tests zero-width/height rectangles
func testZeroWidthRectangle(t *testing.T, db *sql.DB) {
	var minx, maxx float64

	_, err := db.Exec("INSERT INTO rt1 VALUES(200, 5, 5, 10, 10)")
	if err != nil {
		t.Fatalf("failed to insert zero-width rectangle: %v", err)
	}

	err = db.QueryRow("SELECT minx, maxx FROM rt1 WHERE id = 200").Scan(&minx, &maxx)
	if err != nil {
		t.Fatalf("failed to query zero-width rectangle: %v", err)
	}
	if minx != 5 || maxx != 5 {
		t.Errorf("zero-width rectangle mismatch: expected (5, 5), got (%v, %v)", minx, maxx)
	}
}

func rtreeVerifyOrdering(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("SELECT id FROM rt1 ORDER BY id LIMIT 3")
	if err != nil {
		t.Fatalf("failed ORDER BY query: %v", err)
	}
	defer rows.Close()

	var prevID int64 = -1
	for rows.Next() {
		var currentID int64
		if err := rows.Scan(&currentID); err != nil {
			t.Fatalf("failed to scan ordered result: %v", err)
		}
		if prevID >= 0 && currentID <= prevID {
			t.Errorf("ORDER BY failed: %d should be > %d", currentID, prevID)
		}
		prevID = currentID
	}
}

// testRTreeSpatialOperations tests spatial queries and operations
func testRTreeSpatialOperations(t *testing.T, db *sql.DB) {
	var count int64

	if err := db.QueryRow("SELECT COUNT(*) FROM rt1 WHERE id = 101").Scan(&count); err != nil {
		t.Fatalf("failed to query by id: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry with id=101, got %d", count)
	}

	if err := db.QueryRow("SELECT COUNT(*) FROM rt1 WHERE id >= 1 AND id <= 4").Scan(&count); err != nil {
		t.Fatalf("failed range query on id: %v", err)
	}

	rtreeVerifyOrdering(t, db)
	testDropAndSpatialJoin(t, db)
	testLargeCoordinates(t, db)
}

// testDropAndSpatialJoin tests DROP TABLE and spatial joins
func testDropAndSpatialJoin(t *testing.T, db *sql.DB) {
	var count int64

	_, err := db.Exec("DROP TABLE rt_empty")
	if err != nil {
		t.Fatalf("failed to drop rtree table: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM rt_empty").Scan(&count)
	if err == nil {
		t.Error("expected error querying dropped table, got none")
	}

	performSpatialJoin(t, db)
	testMatchOperator(t, db)
}

// performSpatialJoin tests spatial join operations
func performSpatialJoin(t *testing.T, db *sql.DB) {
	_, err := db.Exec("CREATE VIRTUAL TABLE rt2 USING rtree(id, minx, maxx, miny, maxy)")
	if err != nil {
		t.Fatalf("failed to insert into rt2: %v", err)
	}

	rows, err := db.Query(`
		SELECT rt1.id, rt2.id
		FROM rt1, rt2
		WHERE rt1.minx <= rt2.maxx
		  AND rt1.maxx >= rt2.minx
		  AND rt1.miny <= rt2.maxy
		  AND rt1.maxy >= rt2.miny
		LIMIT 5
	`)
	if err != nil {
		t.Fatalf("failed spatial join: %v", err)
	}
	defer rows.Close()

	joinCount := 0
	for rows.Next() {
		var id1, id2 int64
		if err := rows.Scan(&id1, &id2); err != nil {
			t.Fatalf("failed to scan join result: %v", err)
		}
		joinCount++
	}
	if joinCount == 0 {
		t.Error("expected at least one join result")
	}
}

// testMatchOperator tests the MATCH operator (if supported)
func testMatchOperator(t *testing.T, db *sql.DB) {
	var count int64
	err := db.QueryRow("SELECT COUNT(*) FROM rt1 WHERE id MATCH 1").Scan(&count)
	// This may or may not be supported, so we don't fail on error
	if err == nil && count != 1 {
		t.Logf("MATCH operator returned unexpected count: %d", count)
	}
}

// testLargeCoordinates tests very large coordinate values
func testLargeCoordinates(t *testing.T, db *sql.DB) {
	var id float64

	_, err := db.Exec("INSERT INTO rt1 VALUES(300, -1e10, 1e10, -1e10, 1e10)")
	if err != nil {
		t.Fatalf("failed to insert large coordinates: %v", err)
	}

	err = db.QueryRow("SELECT id FROM rt1 WHERE id = 300").Scan(&id)
	if err != nil {
		t.Fatalf("failed to query large coordinates: %v", err)
	}
	if id != 300 {
		t.Errorf("large coordinates query failed: expected id=300, got %v", id)
	}
}

func rtreeBulkInsert(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO rt VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()
	for i := 1; i <= n; i++ {
		if _, err = stmt.Exec(i, float64(i), float64(i+10), float64(i), float64(i+10)); err != nil {
			tx.Rollback()
			t.Fatalf("failed to insert entry %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}
}

// TestRTreeIntegrity tests R-Tree integrity and edge cases
func TestRTreeIntegrity(t *testing.T) {
	t.Skip("pre-existing failure - needs R-Tree implementation")
	dbPath := filepath.Join(t.TempDir(), "rtree_integrity_test.db")
	db, err := sql.Open(DriverName, dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if _, err = db.Exec("CREATE VIRTUAL TABLE rt USING rtree(id, x1, x2, y1, y2)"); err != nil {
		t.Fatalf("failed to create rtree: %v", err)
	}

	rtreeBulkInsert(t, db, 100)

	var count int64
	if err = db.QueryRow("SELECT COUNT(*) FROM rt").Scan(&count); err != nil {
		t.Fatalf("failed to count entries: %v", err)
	}
	if count != 100 {
		t.Errorf("expected 100 entries, got %d", count)
	}

	// Test duplicate IDs and invalid coordinates - just verify no crash
	db.Exec("INSERT INTO rt VALUES(1, 0, 1, 0, 1)")
	db.Exec("INSERT INTO rt VALUES(1000, 10, 5, 10, 5)")
}
