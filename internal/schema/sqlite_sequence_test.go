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

func TestNextSequence(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// First call with no existing sequence
	next := sm.NextSequence("users", 0)
	if next != 1 {
		t.Errorf("NextSequence() = %d, want 1", next)
	}

	// Sequence should be updated
	val := sm.GetSequence("users")
	if val != 1 {
		t.Errorf("Sequence value = %d, want 1", val)
	}

	// Second call
	next = sm.NextSequence("users", 1)
	if next != 2 {
		t.Errorf("NextSequence() = %d, want 2", next)
	}

	// With explicit higher rowid
	next = sm.NextSequence("users", 10)
	if next != 11 {
		t.Errorf("NextSequence() with higher currentMaxRowid = %d, want 11", next)
	}

	val = sm.GetSequence("users")
	if val != 11 {
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

func TestGenerateAutoincrementRowid(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()

	// No explicit rowid - should generate next sequence
	rowid := GenerateAutoincrementRowid(sm, "users", 0, false, 0)
	if rowid != 1 {
		t.Errorf("GenerateAutoincrementRowid() = %d, want 1", rowid)
	}

	// Explicit rowid provided
	rowid = GenerateAutoincrementRowid(sm, "users", 10, true, 1)
	if rowid != 10 {
		t.Errorf("GenerateAutoincrementRowid() with explicit rowid = %d, want 10", rowid)
	}

	// Sequence should be updated to 10
	val := sm.GetSequence("users")
	if val != 10 {
		t.Errorf("Sequence value = %d, want 10", val)
	}

	// Next auto-generated should be 11
	rowid = GenerateAutoincrementRowid(sm, "users", 0, false, 10)
	if rowid != 11 {
		t.Errorf("GenerateAutoincrementRowid() = %d, want 11", rowid)
	}

	// Explicit rowid = 0 should be treated as NULL
	rowid = GenerateAutoincrementRowid(sm, "users", 0, true, 11)
	if rowid != 12 {
		t.Errorf("GenerateAutoincrementRowid() with explicit 0 = %d, want 12", rowid)
	}

	// Higher currentMaxRowid
	rowid = GenerateAutoincrementRowid(sm, "orders", 0, false, 100)
	if rowid != 101 {
		t.Errorf("GenerateAutoincrementRowid() with high currentMaxRowid = %d, want 101", rowid)
	}
}

func TestConcurrentSequenceAccess(t *testing.T) {
	t.Parallel()
	sm := NewSequenceManager()
	sm.InitSequence("users")

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			sm.NextSequence("users", int64(i))
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
