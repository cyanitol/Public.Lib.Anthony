// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"testing"
)

func TestAutoVacuumDefaultIsNone(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	row := querySingle(t, db, "PRAGMA auto_vacuum")
	if row == nil {
		t.Fatal("expected a result from PRAGMA auto_vacuum")
	}
	if val, ok := row.(int64); !ok || val != 0 {
		t.Errorf("expected auto_vacuum=0 (none), got %v (%T)", row, row)
	}
}

func TestAutoVacuumSetIncremental(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db, "PRAGMA auto_vacuum=INCREMENTAL")

	row := querySingle(t, db, "PRAGMA auto_vacuum")
	if row == nil {
		t.Fatal("expected a result from PRAGMA auto_vacuum")
	}
	if val, ok := row.(int64); !ok || val != 2 {
		t.Errorf("expected auto_vacuum=2 (incremental), got %v (%T)", row, row)
	}
}

func TestAutoVacuumSetFull(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db, "PRAGMA auto_vacuum=FULL")

	row := querySingle(t, db, "PRAGMA auto_vacuum")
	if row == nil {
		t.Fatal("expected a result from PRAGMA auto_vacuum")
	}
	if val, ok := row.(int64); !ok || val != 1 {
		t.Errorf("expected auto_vacuum=1 (full), got %v (%T)", row, row)
	}
}

func TestAutoVacuumSetNone(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// First set to incremental, then back to none
	execSQL(t, db, "PRAGMA auto_vacuum=INCREMENTAL")
	execSQL(t, db, "PRAGMA auto_vacuum=NONE")

	row := querySingle(t, db, "PRAGMA auto_vacuum")
	if row == nil {
		t.Fatal("expected a result from PRAGMA auto_vacuum")
	}
	if val, ok := row.(int64); !ok || val != 0 {
		t.Errorf("expected auto_vacuum=0 (none), got %v (%T)", row, row)
	}
}

func TestAutoVacuumSetByNumber(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	execSQL(t, db, "PRAGMA auto_vacuum=2")

	row := querySingle(t, db, "PRAGMA auto_vacuum")
	if row == nil {
		t.Fatal("expected a result from PRAGMA auto_vacuum")
	}
	if val, ok := row.(int64); !ok || val != 2 {
		t.Errorf("expected auto_vacuum=2, got %v (%T)", row, row)
	}
}

func TestIncrementalVacuumExecution(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	// Set incremental mode and run incremental_vacuum
	execSQL(t, db, "PRAGMA auto_vacuum=INCREMENTAL")

	// This should succeed even with no pages to free
	execSQL(t, db, "PRAGMA incremental_vacuum(10)")
}

func TestAutoVacuumRoundtrip(t *testing.T) {
	db := setupMemoryDB(t)
	defer db.Close()

	modes := []struct {
		name string
		val  string
		want int64
	}{
		{"none", "NONE", 0},
		{"full", "FULL", 1},
		{"incremental", "INCREMENTAL", 2},
		{"back_to_none", "0", 0},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			execSQL(t, db, "PRAGMA auto_vacuum="+m.val)
			row := querySingle(t, db, "PRAGMA auto_vacuum")
			if row == nil {
				t.Fatal("expected a result")
			}
			if val, ok := row.(int64); !ok || val != m.want {
				t.Errorf("expected %d, got %v (%T)", m.want, row, row)
			}
		})
	}
}
