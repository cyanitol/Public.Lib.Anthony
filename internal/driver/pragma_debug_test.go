package driver

import (
	"database/sql"
	"testing"
)

func TestPRAGMAForeignKeysDebug(t *testing.T) {
	db, err := sql.Open(DriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// First query
	var val1 int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&val1)
	t.Logf("First query: val=%d, err=%v", val1, err)

	// Exec
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	t.Logf("Exec result: err=%v", err)

	// Second query
	var val2 int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&val2)
	t.Logf("Second query: val=%d, err=%v", val2, err)
}
