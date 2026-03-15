// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package rtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DatabaseExecutor is the interface for executing SQL statements.
// This allows the R-Tree module to create and query shadow tables.
type DatabaseExecutor interface {
	// ExecDDL executes a DDL statement and returns any error.
	ExecDDL(sql string) error
	// ExecDML executes a DML statement with args and returns rows affected.
	ExecDML(sql string, args ...interface{}) (int64, error)
	// Query executes a SELECT and returns results as a slice of rows.
	Query(sql string, args ...interface{}) ([][]interface{}, error)
}

// ShadowTableManager handles R-Tree shadow table operations.
// Shadow tables store the persistent state of the R-Tree.
type ShadowTableManager struct {
	tableName  string
	db         DatabaseExecutor
	dimensions int
}

// NewShadowTableManager creates a shadow table manager for an R-Tree table.
func NewShadowTableManager(tableName string, db DatabaseExecutor, dims int) *ShadowTableManager {
	return &ShadowTableManager{
		tableName:  tableName,
		db:         db,
		dimensions: dims,
	}
}

// CreateShadowTables creates the R-Tree shadow tables for persistent storage.
func (m *ShadowTableManager) CreateShadowTables() error {
	if m.db == nil {
		return nil
	}

	// _node stores serialized R-tree nodes as blobs
	nodeSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_node(nodeno INTEGER PRIMARY KEY, data BLOB)",
		m.tableName,
	)
	if err := m.db.ExecDDL(nodeSQL); err != nil {
		return fmt.Errorf("failed to create _node shadow table: %w", err)
	}

	// _rowid maps entry IDs to the leaf node containing them
	rowidSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_rowid(rowid INTEGER PRIMARY KEY, nodeno INTEGER)",
		m.tableName,
	)
	if err := m.db.ExecDDL(rowidSQL); err != nil {
		return fmt.Errorf("failed to create _rowid shadow table: %w", err)
	}

	// _parent stores parent node pointers for tree traversal
	parentSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_parent(nodeno INTEGER PRIMARY KEY, parentnode INTEGER)",
		m.tableName,
	)
	if err := m.db.ExecDDL(parentSQL); err != nil {
		return fmt.Errorf("failed to create _parent shadow table: %w", err)
	}

	return nil
}

// DropShadowTables removes all R-Tree shadow tables.
func (m *ShadowTableManager) DropShadowTables() error {
	if m.db == nil {
		return nil
	}

	tables := []string{"_node", "_rowid", "_parent"}
	for _, suffix := range tables {
		sql := fmt.Sprintf("DROP TABLE IF EXISTS %s%s", m.tableName, suffix)
		if err := m.db.ExecDDL(sql); err != nil {
			return fmt.Errorf("failed to drop shadow table %s%s: %w", m.tableName, suffix, err)
		}
	}
	return nil
}

// SaveEntries persists all R-Tree entries to shadow tables.
func (m *ShadowTableManager) SaveEntries(entries map[int64]*Entry) error {
	if m.db == nil {
		return nil
	}

	// Clear existing data
	if err := m.clearAllData(); err != nil {
		return err
	}

	// Save each entry
	for id, entry := range entries {
		if err := m.saveEntry(id, entry); err != nil {
			return fmt.Errorf("failed to save entry %d: %w", id, err)
		}
	}

	return nil
}

// clearAllData removes all rows from shadow tables.
func (m *ShadowTableManager) clearAllData() error {
	tables := []string{"_node", "_rowid", "_parent"}
	for _, suffix := range tables {
		sql := fmt.Sprintf("DELETE FROM %s%s", m.tableName, suffix)
		if _, err := m.db.ExecDML(sql); err != nil {
			return fmt.Errorf("failed to clear %s%s: %w", m.tableName, suffix, err)
		}
	}
	return nil
}

// saveEntry persists a single entry to the _node shadow table.
func (m *ShadowTableManager) saveEntry(id int64, entry *Entry) error {
	blob := m.encodeEntry(entry)
	_, err := m.db.ExecDML(
		fmt.Sprintf("INSERT OR REPLACE INTO %s_node(nodeno, data) VALUES(?, ?)", m.tableName),
		id, blob,
	)
	return err
}

// LoadEntries loads all R-Tree entries from shadow tables.
func (m *ShadowTableManager) LoadEntries() (map[int64]*Entry, error) {
	if m.db == nil {
		return make(map[int64]*Entry), nil
	}

	rows, err := m.db.Query(
		fmt.Sprintf("SELECT nodeno, data FROM %s_node", m.tableName),
	)
	if err != nil {
		return make(map[int64]*Entry), nil
	}

	entries := make(map[int64]*Entry)
	for _, row := range rows {
		id, entry := m.parseEntryRow(row)
		if entry != nil {
			entries[id] = entry
		}
	}

	return entries, nil
}

// parseEntryRow parses a single row from the _node table.
func (m *ShadowTableManager) parseEntryRow(row []interface{}) (int64, *Entry) {
	if len(row) < 2 {
		return 0, nil
	}

	id, ok := row[0].(int64)
	if !ok {
		return 0, nil
	}

	blob, ok := row[1].([]byte)
	if !ok {
		return 0, nil
	}

	entry := m.decodeEntry(blob)
	return id, entry
}

// encodeEntry serializes an Entry to bytes.
func (m *ShadowTableManager) encodeEntry(entry *Entry) []byte {
	buf := new(bytes.Buffer)

	// Write entry ID
	binary.Write(buf, binary.LittleEndian, entry.ID)
	// Write dimensions count
	binary.Write(buf, binary.LittleEndian, int32(m.dimensions))
	// Write bounding box coordinates
	for i := 0; i < m.dimensions; i++ {
		binary.Write(buf, binary.LittleEndian, entry.BBox.Min[i])
		binary.Write(buf, binary.LittleEndian, entry.BBox.Max[i])
	}

	return buf.Bytes()
}

// decodeEntry deserializes an Entry from bytes.
func (m *ShadowTableManager) decodeEntry(blob []byte) *Entry {
	if len(blob) < 12 { // minimum: 8 bytes ID + 4 bytes dims
		return nil
	}

	buf := bytes.NewReader(blob)

	var id int64
	var dims int32

	if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
		return nil
	}
	if err := binary.Read(buf, binary.LittleEndian, &dims); err != nil {
		return nil
	}

	bbox := NewBoundingBox(int(dims))
	for i := 0; i < int(dims); i++ {
		if err := binary.Read(buf, binary.LittleEndian, &bbox.Min[i]); err != nil {
			return nil
		}
		if err := binary.Read(buf, binary.LittleEndian, &bbox.Max[i]); err != nil {
			return nil
		}
	}

	return &Entry{
		ID:   id,
		BBox: bbox,
	}
}

// SaveNextID persists the next ID counter via a sentinel row.
func (m *ShadowTableManager) SaveNextID(nextID int64) error {
	if m.db == nil {
		return nil
	}

	// Use _parent table with nodeno=0 as sentinel for metadata
	_, err := m.db.ExecDML(
		fmt.Sprintf("INSERT OR REPLACE INTO %s_parent(nodeno, parentnode) VALUES(?, ?)", m.tableName),
		int64(0), nextID,
	)
	return err
}

// LoadNextID loads the persisted next ID counter.
func (m *ShadowTableManager) LoadNextID() (int64, error) {
	if m.db == nil {
		return 1, nil
	}

	rows, err := m.db.Query(
		fmt.Sprintf("SELECT parentnode FROM %s_parent WHERE nodeno = ?", m.tableName),
		int64(0),
	)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return 1, nil
	}

	if nextID, ok := rows[0][0].(int64); ok {
		return nextID, nil
	}
	return 1, nil
}
