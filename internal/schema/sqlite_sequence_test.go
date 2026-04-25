// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import (
	"testing"
)

func TestNewSequenceManager(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()
	if sm == nil {
		t.Fatal("NewSequenceManager() returned nil")
	}
	if sm.sequences == nil {
		t.Error("sequences map is nil")
	}
}

func TestGetSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// Non-existent sequence should return 0
	val := sm.GetSequence("users")
	if val != 0 {
		t.Errorf("GetSequence() = %d, want 0 for non-existent sequence", val)
	}

	// Set a sequence value
	sm.sequences["users"] = 42

	val = sm.GetSequence("users")
	if val != 42 {
		t.Errorf("GetSequence() = %d, want 42", val)
	}
}

func TestInitSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	sm.InitSequence("users")

	if !sm.HasSequence("users") {
		t.Error("Sequence not initialized")
	}

	val := sm.GetSequence("users")
	if val != 0 {
		t.Errorf("Initial sequence value = %d, want 0", val)
	}

	// Initializing again should not change existing value
	sm.sequences["users"] = 10
	sm.InitSequence("users")
	val = sm.GetSequence("users")
	if val != 10 {
		t.Errorf("Re-initializing changed sequence value to %d, want 10", val)
	}
}

// assertNextSequence calls NextSequence and checks the returned value.
func assertNextSequence(t *testing.T, sm *SequenceManager, table string, currentMax int64, want int64) {
	t.Helper()
	next, err := sm.NextSequence(table, currentMax)
	if err != nil {
		t.Fatalf("NextSequence() error = %v", err)
	}
	if next != want {
		t.Errorf("NextSequence(%q, %d) = %d, want %d", table, currentMax, next, want)
	}
}

func TestNextSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// First call with no existing sequence
	assertNextSequence(t, sm, "users", 0, 1)

	// Sequence should be updated
	if val := sm.GetSequence("users"); val != 1 {
		t.Errorf("Sequence value = %d, want 1", val)
	}

	// Second call
	assertNextSequence(t, sm, "users", 1, 2)

	// With explicit higher rowid
	assertNextSequence(t, sm, "users", 10, 11)

	if val := sm.GetSequence("users"); val != 11 {
		t.Errorf("Sequence value = %d, want 11", val)
	}
}

func TestUpdateSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// Update non-existent sequence
	sm.UpdateSequence("users", 5)
	val := sm.GetSequence("users")
	if val != 5 {
		t.Errorf("UpdateSequence() set value to %d, want 5", val)
	}

	// Update with higher value
	sm.UpdateSequence("users", 10)
	val = sm.GetSequence("users")
	if val != 10 {
		t.Errorf("UpdateSequence() set value to %d, want 10", val)
	}

	// Update with lower value (should not change)
	sm.UpdateSequence("users", 8)
	val = sm.GetSequence("users")
	if val != 10 {
		t.Errorf("UpdateSequence() changed value to %d, should stay at 10", val)
	}
}

func TestDropSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	sm.InitSequence("users")
	if !sm.HasSequence("users") {
		t.Error("Sequence should exist before drop")
	}

	sm.DropSequence("users")
	if sm.HasSequence("users") {
		t.Error("Sequence should not exist after drop")
	}

	// Dropping non-existent sequence should not error
	sm.DropSequence("nonexistent")
}

func TestHasSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	if sm.HasSequence("users") {
		t.Error("HasSequence() should return false for non-existent sequence")
	}

	sm.InitSequence("users")
	if !sm.HasSequence("users") {
		t.Error("HasSequence() should return true after InitSequence")
	}
}

func TestListSequences(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// Empty list
	list := sm.ListSequences()
	if len(list) != 0 {
		t.Errorf("ListSequences() returned %d sequences, want 0", len(list))
	}

	// Add sequences
	sm.InitSequence("users")
	sm.InitSequence("orders")
	sm.InitSequence("products")

	list = sm.ListSequences()
	if len(list) != 3 {
		t.Fatalf("ListSequences() returned %d sequences, want 3", len(list))
	}

	// Verify all are present
	listMap := make(map[string]bool)
	for _, name := range list {
		name := name
		listMap[name] = true
	}

	if !listMap["users"] || !listMap["orders"] || !listMap["products"] {
		t.Error("Not all sequences listed")
	}
}

func TestGetAllSequences(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// Empty map
	all := sm.GetAllSequences()
	if len(all) != 0 {
		t.Errorf("GetAllSequences() returned %d sequences, want 0", len(all))
	}

	// Add sequences with values
	sm.sequences["users"] = 10
	sm.sequences["orders"] = 20
	sm.sequences["products"] = 30

	all = sm.GetAllSequences()
	if len(all) != 3 {
		t.Fatalf("GetAllSequences() returned %d sequences, want 3", len(all))
	}

	if all["users"] != 10 {
		t.Errorf("users sequence = %d, want 10", all["users"])
	}
	if all["orders"] != 20 {
		t.Errorf("orders sequence = %d, want 20", all["orders"])
	}
	if all["products"] != 30 {
		t.Errorf("products sequence = %d, want 30", all["products"])
	}

	// Modifying returned map should not affect original
	all["users"] = 999
	if sm.GetSequence("users") == 999 {
		t.Error("Modifying returned map should not affect original")
	}
}

func TestSetSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	sm.SetSequence("users", 42)
	val := sm.GetSequence("users")
	if val != 42 {
		t.Errorf("SetSequence() set value to %d, want 42", val)
	}

	// Overwrite existing value
	sm.SetSequence("users", 100)
	val = sm.GetSequence("users")
	if val != 100 {
		t.Errorf("SetSequence() set value to %d, want 100", val)
	}
}

func TestHasAutoincrementColumn(t *testing.T) {
	t.Parallel()
	table := &Table{
		Columns: []*Column{
			{Name: "id", Autoincrement: false},
			{Name: "name", Autoincrement: false},
		},
	}

	col, has := table.HasAutoincrementColumn()
	if has {
		t.Error("HasAutoincrementColumn() should return false")
	}
	if col != nil {
		t.Error("Column should be nil when no AUTOINCREMENT")
	}

	// Add AUTOINCREMENT column
	table.Columns[0].Autoincrement = true

	col, has = table.HasAutoincrementColumn()
	if !has {
		t.Error("HasAutoincrementColumn() should return true")
	}
	if col == nil {
		t.Error("Column should not be nil")
	}
	if col.Name != "id" {
		t.Errorf("Column name = %q, want %q", col.Name, "id")
	}
}

func TestGetAutoincrementColumnIndex(t *testing.T) {
	t.Parallel()
	table := &Table{
		Columns: []*Column{
			{Name: "id", Autoincrement: false},
			{Name: "name", Autoincrement: false},
			{Name: "seq", Autoincrement: true},
		},
	}

	idx := table.GetAutoincrementColumnIndex()
	if idx != 2 {
		t.Errorf("GetAutoincrementColumnIndex() = %d, want 2", idx)
	}

	// No AUTOINCREMENT column
	table.Columns[2].Autoincrement = false
	idx = table.GetAutoincrementColumnIndex()
	if idx != -1 {
		t.Errorf("GetAutoincrementColumnIndex() = %d, want -1", idx)
	}
}

func TestValidateAutoincrementColumn(t *testing.T) {
	t.Parallel()
	// Valid: INTEGER PRIMARY KEY AUTOINCREMENT
	table := &Table{
		Columns: []*Column{
			{
				Name:          "id",
				Type:          "INTEGER",
				PrimaryKey:    true,
				Autoincrement: true,
			},
		},
	}

	err := table.ValidateAutoincrementColumn()
	if err != nil {
		t.Errorf("ValidateAutoincrementColumn() error = %v, want nil", err)
	}

	// Valid: INT PRIMARY KEY AUTOINCREMENT (INT is also allowed)
	table.Columns[0].Type = "INT"
	err = table.ValidateAutoincrementColumn()
	if err != nil {
		t.Errorf("ValidateAutoincrementColumn() error = %v, want nil for INT", err)
	}

	// Invalid: TEXT AUTOINCREMENT
	table.Columns[0].Type = "TEXT"
	err = table.ValidateAutoincrementColumn()
	if err == nil {
		t.Error("Expected error for AUTOINCREMENT on TEXT column")
	}

	// Invalid: INTEGER AUTOINCREMENT (not primary key)
	table.Columns[0].Type = "INTEGER"
	table.Columns[0].PrimaryKey = false
	err = table.ValidateAutoincrementColumn()
	if err == nil {
		t.Error("Expected error for AUTOINCREMENT without PRIMARY KEY")
	}
}

// assertAutoincRowid calls GenerateAutoincrementRowid and checks the result.
func assertAutoincRowid(t *testing.T, sm *SequenceManager, table string, explicit int64, hasExplicit bool, currentMax int64, want int64) {
	t.Helper()
	rowid, err := GenerateAutoincrementRowid(sm, table, explicit, hasExplicit, currentMax)
	if err != nil {
		t.Fatalf("GenerateAutoincrementRowid() error = %v", err)
	}
	if rowid != want {
		t.Errorf("GenerateAutoincrementRowid() = %d, want %d", rowid, want)
	}
}

func TestGenerateAutoincrementRowid(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	t.Run("auto_generate_first", func(t *testing.T) {
		assertAutoincRowid(t, sm, "users", 0, false, 0, 1)
	})

	t.Run("explicit_rowid", func(t *testing.T) {
		assertAutoincRowid(t, sm, "users", 10, true, 1, 10)
		if val := sm.GetSequence("users"); val != 10 {
			t.Errorf("Sequence value = %d, want 10", val)
		}
	})

	t.Run("next_after_explicit", func(t *testing.T) {
		assertAutoincRowid(t, sm, "users", 0, false, 10, 11)
	})

	t.Run("explicit_zero_as_null", func(t *testing.T) {
		assertAutoincRowid(t, sm, "users", 0, true, 11, 12)
	})

	t.Run("high_current_max", func(t *testing.T) {
		assertAutoincRowid(t, sm, "orders", 0, false, 100, 101)
	})
}

func TestConcurrentSequenceAccess(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()
	sm.InitSequence("users")

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_, _ = sm.NextSequence("users", int64(i))
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = sm.GetSequence("users")
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}
