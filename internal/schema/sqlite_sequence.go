// Package schema provides AUTOINCREMENT support via the sqlite_sequence table.
package schema

import (
	"fmt"
	"sync"
)

// SequenceManager manages the sqlite_sequence table for AUTOINCREMENT support.
// The sqlite_sequence table stores the maximum rowid for each table with
// AUTOINCREMENT columns, ensuring rowids are never reused even after deletion.
type SequenceManager struct {
	sequences map[string]int64 // tableName -> max rowid
	mu        sync.RWMutex
}

// NewSequenceManager creates a new sequence manager.
func NewSequenceManager() *SequenceManager {
	return &SequenceManager{
		sequences: make(map[string]int64),
	}
}

// GetSequence retrieves the current sequence value for a table.
// Returns 0 if the table has no sequence entry.
func (sm *SequenceManager) GetSequence(tableName string) int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sequences[tableName]
}

// NextSequence generates the next sequence value for a table.
// This increments the sequence and returns the new value.
// The sequence is initialized to 0 if it doesn't exist, so the first value is 1.
func (sm *SequenceManager) NextSequence(tableName string, currentMaxRowid int64) int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get current sequence value
	current := sm.sequences[tableName]

	// The next value should be max(current, currentMaxRowid) + 1
	// This handles both:
	// - Normal increment (current is already the max)
	// - Explicit INSERT with larger rowid (currentMaxRowid > current)
	next := current
	if currentMaxRowid > current {
		next = currentMaxRowid
	}
	next++

	// Update sequence
	sm.sequences[tableName] = next
	return next
}

// UpdateSequence updates the sequence value for a table if the new value is greater.
// This is called when an explicit rowid is inserted.
func (sm *SequenceManager) UpdateSequence(tableName string, rowid int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if current, exists := sm.sequences[tableName]; !exists || rowid > current {
		sm.sequences[tableName] = rowid
	}
}

// InitSequence initializes a sequence for a table with AUTOINCREMENT.
// This should be called when a table with AUTOINCREMENT is created.
func (sm *SequenceManager) InitSequence(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Only initialize if not already present
	if _, exists := sm.sequences[tableName]; !exists {
		sm.sequences[tableName] = 0
	}
}

// DropSequence removes a sequence entry for a table.
// This should be called when a table is dropped.
func (sm *SequenceManager) DropSequence(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sequences, tableName)
}

// HasSequence checks if a table has a sequence entry.
func (sm *SequenceManager) HasSequence(tableName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, exists := sm.sequences[tableName]
	return exists
}

// ListSequences returns a list of all table names with sequences.
func (sm *SequenceManager) ListSequences() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	tables := make([]string, 0, len(sm.sequences))
	for tableName := range sm.sequences {
		tables = append(tables, tableName)
	}
	return tables
}

// GetAllSequences returns a copy of all sequences as a map.
// This is useful for persistence and testing.
func (sm *SequenceManager) GetAllSequences() map[string]int64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]int64, len(sm.sequences))
	for k, v := range sm.sequences {
		result[k] = v
	}
	return result
}

// SetSequence sets the sequence value for a table.
// This is useful for loading persisted sequences.
func (sm *SequenceManager) SetSequence(tableName string, value int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sequences[tableName] = value
}

// HasAutoincrementColumn checks if a table has an AUTOINCREMENT column.
// Returns the column and true if found, nil and false otherwise.
func (t *Table) HasAutoincrementColumn() (*Column, bool) {
	for _, col := range t.Columns {
		if col.Autoincrement {
			return col, true
		}
	}
	return nil, false
}

// GetAutoincrementColumnIndex returns the index of the AUTOINCREMENT column, or -1 if none.
func (t *Table) GetAutoincrementColumnIndex() int {
	for i, col := range t.Columns {
		if col.Autoincrement {
			return i
		}
	}
	return -1
}

// ValidateAutoincrementColumn validates that AUTOINCREMENT is only used correctly.
// AUTOINCREMENT can only be used on INTEGER PRIMARY KEY columns.
func (t *Table) ValidateAutoincrementColumn() error {
	for _, col := range t.Columns {
		if col.Autoincrement {
			// Must be INTEGER type
			if col.Type != "INTEGER" && col.Type != "INT" {
				return fmt.Errorf("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY")
			}
			// Must be primary key
			if !col.PrimaryKey {
				return fmt.Errorf("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY")
			}
		}
	}
	return nil
}

// GenerateAutoincrementRowid generates the next rowid for an AUTOINCREMENT column.
// This differs from regular INTEGER PRIMARY KEY in that it never reuses deleted rowids.
//
// Parameters:
// - sm: The sequence manager
// - tableName: The table name
// - explicitRowid: The explicit rowid value (if provided), or 0 if NULL
// - hasExplicitRowid: Whether an explicit rowid was provided
// - currentMaxRowid: The current maximum rowid in the table (from btree)
//
// Returns the rowid to use for the insert.
func GenerateAutoincrementRowid(sm *SequenceManager, tableName string, explicitRowid int64, hasExplicitRowid bool, currentMaxRowid int64) int64 {
	if hasExplicitRowid && explicitRowid != 0 {
		// Explicit rowid provided - use it and update sequence
		sm.UpdateSequence(tableName, explicitRowid)
		return explicitRowid
	}

	// NULL or no rowid provided - generate next sequence value
	return sm.NextSequence(tableName, currentMaxRowid)
}
