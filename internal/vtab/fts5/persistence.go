// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ShadowTableManager handles FTS5 shadow table operations.
// Shadow tables store the persistent state of the FTS5 index.
type ShadowTableManager struct {
	tableName string
	db        DatabaseExecutor
}

// DatabaseExecutor is the interface for executing SQL statements.
// This allows the FTS5 module to create and query shadow tables.
type DatabaseExecutor interface {
	// ExecDDL executes a DDL statement and returns any error.
	ExecDDL(sql string) error
	// ExecDML executes a DML statement with args and returns rows affected.
	ExecDML(sql string, args ...interface{}) (int64, error)
	// Query executes a SELECT and returns results as a slice of rows.
	Query(sql string, args ...interface{}) ([][]interface{}, error)
}

// NewShadowTableManager creates a shadow table manager for an FTS5 table.
func NewShadowTableManager(tableName string, db DatabaseExecutor) *ShadowTableManager {
	return &ShadowTableManager{
		tableName: tableName,
		db:        db,
	}
}

// CreateShadowTables creates the FTS5 shadow tables for persistent storage.
// These tables store the inverted index, configuration, and document data.
func (m *ShadowTableManager) CreateShadowTables(columns []string) error {
	if m.db == nil {
		return nil // No persistence - in-memory only
	}

	// Create _data table - stores the full-text index blobs
	dataTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_data(id INTEGER PRIMARY KEY, block BLOB)",
		m.tableName,
	)
	if err := m.db.ExecDDL(dataTableSQL); err != nil {
		return fmt.Errorf("failed to create _data shadow table: %w", err)
	}

	// Create _idx table - term index metadata
	idxTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_idx(segid INTEGER, term TEXT, pgno INTEGER, PRIMARY KEY(segid, term))",
		m.tableName,
	)
	if err := m.db.ExecDDL(idxTableSQL); err != nil {
		return fmt.Errorf("failed to create _idx shadow table: %w", err)
	}

	// Create _config table - persistent configuration
	configTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_config(k TEXT PRIMARY KEY, v TEXT)",
		m.tableName,
	)
	if err := m.db.ExecDDL(configTableSQL); err != nil {
		return fmt.Errorf("failed to create _config shadow table: %w", err)
	}

	// Create _docsize table - document size tracking
	docsizeTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_docsize(id INTEGER PRIMARY KEY, sz BLOB)",
		m.tableName,
	)
	if err := m.db.ExecDDL(docsizeTableSQL); err != nil {
		return fmt.Errorf("failed to create _docsize shadow table: %w", err)
	}

	// Create _content table - original document content
	contentColumns := "id INTEGER PRIMARY KEY"
	for i := range columns {
		contentColumns += fmt.Sprintf(", c%d", i)
	}
	contentTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s_content(%s)",
		m.tableName, contentColumns,
	)
	if err := m.db.ExecDDL(contentTableSQL); err != nil {
		return fmt.Errorf("failed to create _content shadow table: %w", err)
	}

	return nil
}

// DropShadowTables removes all FTS5 shadow tables.
func (m *ShadowTableManager) DropShadowTables() error {
	if m.db == nil {
		return nil
	}

	tables := []string{"_data", "_idx", "_config", "_docsize", "_content"}
	for _, suffix := range tables {
		sql := fmt.Sprintf("DROP TABLE IF EXISTS %s%s", m.tableName, suffix)
		if err := m.db.ExecDDL(sql); err != nil {
			return fmt.Errorf("failed to drop shadow table %s%s: %w", m.tableName, suffix, err)
		}
	}
	return nil
}

// SaveIndex persists the inverted index to shadow tables.
func (m *ShadowTableManager) SaveIndex(index *InvertedIndex) error {
	if m.db == nil || index == nil {
		return nil
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	// Save structure record (id=10): total docs and avg length
	structBlob := m.encodeStructureRecord(index.totalDocs, index.avgDocLength)
	if _, err := m.db.ExecDML(
		fmt.Sprintf("INSERT OR REPLACE INTO %s_data(id, block) VALUES(?, ?)", m.tableName),
		int64(10), structBlob,
	); err != nil {
		return fmt.Errorf("failed to save structure record: %w", err)
	}

	// Save inverted index terms
	segid := int64(1) // Single segment for simplicity
	for term, postings := range index.index {
		blob := m.encodePostingList(postings)
		pgno := int64(len(blob)) // Use blob size as page number placeholder

		if _, err := m.db.ExecDML(
			fmt.Sprintf("INSERT OR REPLACE INTO %s_idx(segid, term, pgno) VALUES(?, ?, ?)", m.tableName),
			segid, term, pgno,
		); err != nil {
			return fmt.Errorf("failed to save term %s: %w", term, err)
		}

		// Store the actual posting list in _data
		termID := hashTerm(term)
		if _, err := m.db.ExecDML(
			fmt.Sprintf("INSERT OR REPLACE INTO %s_data(id, block) VALUES(?, ?)", m.tableName),
			termID, blob,
		); err != nil {
			return fmt.Errorf("failed to save posting list for %s: %w", term, err)
		}
	}

	// Save document sizes
	for docID, length := range index.docLengths {
		szBlob := m.encodeVarint(int64(length))
		if _, err := m.db.ExecDML(
			fmt.Sprintf("INSERT OR REPLACE INTO %s_docsize(id, sz) VALUES(?, ?)", m.tableName),
			int64(docID), szBlob,
		); err != nil {
			return fmt.Errorf("failed to save docsize for %d: %w", docID, err)
		}
	}

	return nil
}

// LoadIndex loads the inverted index from shadow tables.
func (m *ShadowTableManager) LoadIndex(columns []string) (*InvertedIndex, error) {
	if m.db == nil {
		return NewInvertedIndex(columns), nil
	}

	index := NewInvertedIndex(columns)

	// Load structure record (total docs, avg doc length)
	m.loadStructureRecord(index)

	// Load terms and their posting lists
	m.loadTermPostings(index)

	// Load document sizes
	m.loadDocumentSizes(index)

	return index, nil
}

// loadStructureRecord loads the structure metadata from the _data table.
func (m *ShadowTableManager) loadStructureRecord(index *InvertedIndex) {
	// nosec: table name from internal FTS5 module state, not user input
	rows, err := m.db.Query(
		fmt.Sprintf("SELECT block FROM %s_data WHERE id = ?", m.tableName),
		int64(10),
	)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return
	}

	if blob, ok := rows[0][0].([]byte); ok {
		totalDocs, avgLen := m.decodeStructureRecord(blob)
		index.totalDocs = totalDocs
		index.avgDocLength = avgLen
	}
}

// loadTermPostings loads all terms and their posting lists from shadow tables.
func (m *ShadowTableManager) loadTermPostings(index *InvertedIndex) {
	// nosec: table name from internal FTS5 module state, not user input
	termRows, err := m.db.Query(
		fmt.Sprintf("SELECT term FROM %s_idx WHERE segid = ?", m.tableName),
		int64(1),
	)
	if err != nil {
		return
	}

	for _, row := range termRows {
		m.loadSingleTermPosting(index, row)
	}
}

// loadSingleTermPosting loads the posting list for one term.
func (m *ShadowTableManager) loadSingleTermPosting(index *InvertedIndex, row []interface{}) {
	if len(row) == 0 {
		return
	}
	term, ok := row[0].(string)
	if !ok {
		return
	}

	termID := hashTerm(term)
	// nosec: table name from internal FTS5 module state, not user input
	dataRows, err := m.db.Query(
		fmt.Sprintf("SELECT block FROM %s_data WHERE id = ?", m.tableName),
		termID,
	)
	if err != nil || len(dataRows) == 0 {
		return
	}

	if blob, ok := dataRows[0][0].([]byte); ok {
		postings := m.decodePostingList(blob)
		index.index[term] = postings
	}
}

// loadDocumentSizes loads document sizes from the _docsize table.
func (m *ShadowTableManager) loadDocumentSizes(index *InvertedIndex) {
	// nosec: table name from internal FTS5 module state, not user input
	sizeRows, err := m.db.Query(
		fmt.Sprintf("SELECT id, sz FROM %s_docsize", m.tableName),
	)
	if err != nil {
		return
	}

	for _, row := range sizeRows {
		m.loadSingleDocSize(index, row)
	}
}

// loadSingleDocSize loads the size for one document.
func (m *ShadowTableManager) loadSingleDocSize(index *InvertedIndex, row []interface{}) {
	if len(row) < 2 {
		return
	}
	docID, ok1 := row[0].(int64)
	szBlob, ok2 := row[1].([]byte)
	if !ok1 || !ok2 {
		return
	}
	length := m.decodeVarint(szBlob)
	index.docLengths[DocumentID(docID)] = int(length)
}

// SaveContent saves document content to the _content shadow table.
func (m *ShadowTableManager) SaveContent(docID DocumentID, values []interface{}) error {
	if m.db == nil {
		return nil
	}

	// Build INSERT statement with proper number of columns
	cols := "id"
	placeholders := "?"
	args := []interface{}{int64(docID)}

	for i, val := range values {
		cols += fmt.Sprintf(", c%d", i)
		placeholders += ", ?"
		args = append(args, val)
	}

	sql := fmt.Sprintf(
		"INSERT OR REPLACE INTO %s_content(%s) VALUES(%s)",
		m.tableName, cols, placeholders,
	)

	_, err := m.db.ExecDML(sql, args...)
	return err
}

// LoadContent loads document content from the _content shadow table.
func (m *ShadowTableManager) LoadContent(docID DocumentID, numColumns int) ([]interface{}, error) {
	if m.db == nil {
		return nil, fmt.Errorf("no database connection")
	}

	cols := "id"
	for i := 0; i < numColumns; i++ {
		cols += fmt.Sprintf(", c%d", i)
	}

	rows, err := m.db.Query(
		fmt.Sprintf("SELECT %s FROM %s_content WHERE id = ?", cols, m.tableName),
		int64(docID),
	)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("document %d not found", docID)
	}

	// Skip the id column, return the rest
	if len(rows[0]) > 1 {
		return rows[0][1:], nil
	}
	return nil, nil
}

// DeleteContent removes document content from the _content shadow table.
func (m *ShadowTableManager) DeleteContent(docID DocumentID) error {
	if m.db == nil {
		return nil
	}

	_, err := m.db.ExecDML(
		fmt.Sprintf("DELETE FROM %s_content WHERE id = ?", m.tableName),
		int64(docID),
	)
	return err
}

// encodeStructureRecord encodes the structure record (total docs, avg length).
func (m *ShadowTableManager) encodeStructureRecord(totalDocs int, avgLength float64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int64(totalDocs))
	binary.Write(buf, binary.LittleEndian, avgLength)
	return buf.Bytes()
}

// decodeStructureRecord decodes the structure record.
func (m *ShadowTableManager) decodeStructureRecord(blob []byte) (int, float64) {
	if len(blob) < 16 {
		return 0, 0
	}
	buf := bytes.NewReader(blob)
	var totalDocs int64
	var avgLength float64
	binary.Read(buf, binary.LittleEndian, &totalDocs)
	binary.Read(buf, binary.LittleEndian, &avgLength)
	return int(totalDocs), avgLength
}

// encodePostingList encodes a posting list to bytes.
func (m *ShadowTableManager) encodePostingList(postings []PostingList) []byte {
	buf := new(bytes.Buffer)

	// Write count
	binary.Write(buf, binary.LittleEndian, int32(len(postings)))

	for _, p := range postings {
		// Write doc ID
		binary.Write(buf, binary.LittleEndian, int64(p.DocID))
		// Write frequency
		binary.Write(buf, binary.LittleEndian, int32(p.Frequency))
		// Write positions count
		binary.Write(buf, binary.LittleEndian, int32(len(p.Positions)))
		// Write positions
		for _, pos := range p.Positions {
			binary.Write(buf, binary.LittleEndian, int32(pos))
		}
	}

	return buf.Bytes()
}

// decodePostingList decodes a posting list from bytes.
func (m *ShadowTableManager) decodePostingList(blob []byte) []PostingList {
	if len(blob) < 4 {
		return nil
	}

	buf := bytes.NewReader(blob)
	var count int32
	binary.Read(buf, binary.LittleEndian, &count)

	postings := make([]PostingList, 0, count)
	for i := int32(0); i < count; i++ {
		var docID int64
		var freq, posCount int32

		if err := binary.Read(buf, binary.LittleEndian, &docID); err != nil {
			break
		}
		if err := binary.Read(buf, binary.LittleEndian, &freq); err != nil {
			break
		}
		if err := binary.Read(buf, binary.LittleEndian, &posCount); err != nil {
			break
		}

		positions := make([]int, 0, posCount)
		for j := int32(0); j < posCount; j++ {
			var pos int32
			if err := binary.Read(buf, binary.LittleEndian, &pos); err != nil {
				break
			}
			positions = append(positions, int(pos))
		}

		postings = append(postings, PostingList{
			DocID:     DocumentID(docID),
			Frequency: int(freq),
			Positions: positions,
		})
	}

	return postings
}

// encodeVarint encodes an int64 as a varint.
func (m *ShadowTableManager) encodeVarint(v int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, v)
	return buf[:n]
}

// decodeVarint decodes a varint from bytes.
func (m *ShadowTableManager) decodeVarint(blob []byte) int64 {
	v, _ := binary.Varint(blob)
	return v
}

// hashTerm creates a hash ID for a term (for storage in _data table).
func hashTerm(term string) int64 {
	// Simple FNV-1a hash
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(term); i++ {
		hash ^= uint64(term[i])
		hash *= prime64
	}

	// Ensure positive and avoid collision with special IDs (1, 10)
	id := int64(hash & 0x7FFFFFFFFFFFFFFF)
	if id < 100 {
		id += 100
	}
	return id
}
