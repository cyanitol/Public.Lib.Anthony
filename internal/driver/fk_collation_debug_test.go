package driver

import (
	"testing"
)

func TestFKCollationDebug(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Enable foreign keys
	_, err := db.Exec("PRAGMA foreign_keys = ON")
	if err != nil {
		t.Fatal(err)
	}

	// Create parent with NOCASE collation
	_, err = db.Exec("CREATE TABLE parent(name TEXT COLLATE NOCASE PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE parent: %v", err)
	}

	// Create child referencing parent
	_, err = db.Exec("CREATE TABLE child(cid INTEGER PRIMARY KEY, pname TEXT REFERENCES parent(name))")
	if err != nil {
		t.Fatalf("CREATE child: %v", err)
	}

	// Insert parent row
	_, err = db.Exec("INSERT INTO parent VALUES('SQLite')")
	if err != nil {
		t.Fatalf("INSERT parent: %v", err)
	}

	// Insert child row with lowercase
	_, err = db.Exec("INSERT INTO child VALUES(1, 'sqlite')")
	if err != nil {
		t.Fatalf("INSERT child: %v", err)
	}
	t.Log("Insert with NOCASE collation succeeded")

	// Verify data
	var parentCount int
	db.QueryRow("SELECT COUNT(*) FROM parent").Scan(&parentCount)
	t.Logf("Parent count: %d", parentCount)

	var childCount int
	db.QueryRow("SELECT COUNT(*) FROM child").Scan(&childCount)
	t.Logf("Child count: %d", childCount)

	// Now try to delete parent with different case
	t.Log("Attempting DELETE FROM parent WHERE name = 'SQLITE'...")
	result, err := db.Exec("DELETE FROM parent WHERE name = 'SQLITE'")
	if err != nil {
		t.Logf("DELETE failed (expected): %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		t.Logf("DELETE succeeded (unexpected), rows affected: %d", rowsAffected)

		// Check parent count after delete
		var countAfter int
		db.QueryRow("SELECT COUNT(*) FROM parent").Scan(&countAfter)
		t.Logf("Parent count after DELETE: %d", countAfter)

		t.Error("DELETE should have failed due to FK constraint")
	}
}
