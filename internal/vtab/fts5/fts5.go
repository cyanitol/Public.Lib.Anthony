// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/vtab"
)

// FTS5Module implements the FTS5 virtual table module.
type FTS5Module struct {
	vtab.BaseModule
}

// NewFTS5Module creates a new FTS5 module.
func NewFTS5Module() *FTS5Module {
	return &FTS5Module{}
}

// Create creates a new FTS5 virtual table.
// Syntax: CREATE VIRTUAL TABLE name USING fts5(col1, col2, ...)
func (m *FTS5Module) Create(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.createTable(db, moduleName, dbName, tableName, args)
}

// Connect connects to an existing FTS5 virtual table.
func (m *FTS5Module) Connect(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	return m.createTable(db, moduleName, dbName, tableName, args)
}

// createTable creates or connects to an FTS5 table.
func (m *FTS5Module) createTable(db interface{}, moduleName string, dbName string, tableName string, args []string) (vtab.VirtualTable, string, error) {
	columns, err := parseFTS5Columns(args)
	if err != nil {
		return nil, "", err
	}

	schema := fmt.Sprintf("CREATE TABLE %s(%s)", tableName, strings.Join(columns, ", ")) // nosec: tableName and columns are from validated CREATE VIRTUAL TABLE DDL, not user input
	shadowMgr := initShadowManager(db, tableName, columns)
	index := loadOrCreateIndex(shadowMgr, columns)

	table := &FTS5Table{
		tableName: tableName,
		columns:   columns,
		index:     index,
		tokenizer: NewSimpleTokenizer(),
		ranker:    NewBM25Ranker(),
		nextRowID: computeNextRowID(index),
		rows:      make(map[DocumentID][]interface{}),
		shadowMgr: shadowMgr,
	}

	// Load content from shadow tables if available
	if shadowMgr != nil {
		table.loadContentFromShadow()
	}

	return table, schema, nil
}

// parseFTS5Columns parses and validates column definitions from args.
func parseFTS5Columns(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("FTS5 table requires at least one column")
	}

	columns := make([]string, 0, len(args))
	for _, arg := range args {
		colName := strings.TrimSpace(arg)
		if colName != "" {
			columns = append(columns, colName)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("FTS5 table requires at least one column")
	}
	return columns, nil
}

// initShadowManager creates and initializes a shadow table manager if possible.
func initShadowManager(db interface{}, tableName string, columns []string) *ShadowTableManager {
	dbExec, ok := db.(DatabaseExecutor)
	if !ok {
		return nil
	}

	mgr := NewShadowTableManager(tableName, dbExec)
	if err := mgr.CreateShadowTables(columns); err != nil {
		return nil
	}
	return mgr
}

// loadOrCreateIndex loads an existing index from shadow tables or creates a new one.
func loadOrCreateIndex(shadowMgr *ShadowTableManager, columns []string) *InvertedIndex {
	if shadowMgr == nil {
		return NewInvertedIndex(columns)
	}

	loadedIndex, err := shadowMgr.LoadIndex(columns)
	if err != nil {
		return NewInvertedIndex(columns)
	}
	return loadedIndex
}

// computeNextRowID determines the next row ID from loaded index data.
func computeNextRowID(index *InvertedIndex) DocumentID {
	nextRowID := DocumentID(1)
	for docID := range index.docLengths {
		if docID >= nextRowID {
			nextRowID = docID + 1
		}
	}
	return nextRowID
}

// loadContentFromShadow loads document content from shadow tables into memory.
func (t *FTS5Table) loadContentFromShadow() {
	for docID := range t.index.docLengths {
		if content, err := t.shadowMgr.LoadContent(docID, len(t.columns)); err == nil {
			t.rows[docID] = content
		}
	}
}

// FTS5Table represents an FTS5 virtual table instance.
type FTS5Table struct {
	vtab.BaseVirtualTable

	mu        sync.RWMutex
	tableName string
	columns   []string
	index     *InvertedIndex
	tokenizer Tokenizer
	ranker    RankFunction

	// Storage for actual row data
	nextRowID DocumentID
	rows      map[DocumentID][]interface{}

	// Persistence layer for shadow tables
	shadowMgr *ShadowTableManager
}

// BestIndex analyzes the query and determines the best index strategy.
func (t *FTS5Table) BestIndex(info *vtab.IndexInfo) error {
	// Look for MATCH constraints
	hasMatch := false
	argvIndex := 1

	for i, constraint := range info.Constraints {
		if !constraint.Usable {
			continue
		}

		// FTS5 tables primarily use the MATCH operator
		if constraint.Op == vtab.ConstraintMatch {
			info.SetConstraintUsage(i, argvIndex, true)
			argvIndex++
			hasMatch = true
			info.IdxNum = 1 // Indicate we're using FTS
		}
	}

	// Set cost estimates
	if hasMatch {
		// FTS search is efficient
		info.EstimatedCost = 100.0
		info.EstimatedRows = 100
	} else {
		// Full table scan
		info.EstimatedCost = 1000000.0
		info.EstimatedRows = int64(t.index.GetTotalDocuments())
	}

	return nil
}

// Open creates a new cursor for scanning the FTS5 table.
func (t *FTS5Table) Open() (vtab.VirtualCursor, error) {
	return &FTS5Cursor{
		table:   t,
		results: []SearchResult{},
		pos:     -1,
	}, nil
}

// Update handles INSERT, UPDATE, and DELETE operations.
func (t *FTS5Table) Update(argc int, argv []interface{}) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// DELETE: argc=1, argv[0]=rowid
	if argc == 1 {
		return t.handleDelete(argv)
	}

	// INSERT: argc>1, argv[0]=NULL or 0, argv[1]=new rowid or NULL
	// UPDATE: argc>1, argv[0]=old rowid, argv[1]=new rowid
	if argc < 2 {
		return 0, fmt.Errorf("invalid number of arguments for UPDATE/INSERT")
	}

	return t.handleInsertOrUpdate(argc, argv)
}

// handleDelete handles DELETE operations.
func (t *FTS5Table) handleDelete(argv []interface{}) (int64, error) {
	rowid, ok := argv[0].(int64)
	if !ok {
		return 0, fmt.Errorf("invalid rowid for DELETE")
	}

	docID := DocumentID(rowid)
	if err := t.index.RemoveDocument(docID); err != nil {
		return 0, err
	}

	delete(t.rows, docID)

	// Remove from shadow tables
	if t.shadowMgr != nil {
		t.shadowMgr.DeleteContent(docID)
		// Save updated index state
		t.shadowMgr.SaveIndex(t.index)
	}

	return rowid, nil
}

// handleInsertOrUpdate handles INSERT and UPDATE operations.
func (t *FTS5Table) handleInsertOrUpdate(argc int, argv []interface{}) (int64, error) {
	oldRowID := argv[0]
	newRowID := argv[1]

	// Check if this is an UPDATE and remove old document if needed
	oldDocID, isUpdate := t.checkAndRemoveOldDocument(oldRowID)

	// Determine the new document ID
	docID, err := t.determineDocumentID(newRowID)
	if err != nil {
		return 0, err
	}

	// Remove old document if this is an update
	if isUpdate {
		t.removeDocument(oldDocID)
		// Also remove from shadow tables
		if t.shadowMgr != nil {
			t.shadowMgr.DeleteContent(oldDocID)
		}
	}

	// Extract and index column values
	columnValues, columnTexts, err := t.extractColumnValues(argc, argv)
	if err != nil {
		return 0, err
	}

	// Add to index
	if err := t.index.AddDocument(docID, columnTexts, t.tokenizer); err != nil {
		return 0, err
	}

	// Store row data
	t.rows[docID] = columnValues

	// Persist to shadow tables
	if t.shadowMgr != nil {
		if err := t.shadowMgr.SaveContent(docID, columnValues); err != nil {
			// Log warning but don't fail the operation
			// In production might want to handle this differently
		}
		// Save index state (could be deferred to transaction commit)
		t.shadowMgr.SaveIndex(t.index)
	}

	return int64(docID), nil
}

// checkAndRemoveOldDocument checks if this is an update operation.
func (t *FTS5Table) checkAndRemoveOldDocument(oldRowID interface{}) (DocumentID, bool) {
	if oldRowID == nil {
		return 0, false
	}

	rid, ok := oldRowID.(int64)
	if !ok || rid == 0 {
		return 0, false
	}

	return DocumentID(rid), true
}

// determineDocumentID determines the document ID for the new/updated document.
func (t *FTS5Table) determineDocumentID(newRowID interface{}) (DocumentID, error) {
	if newRowID == nil || newRowID == int64(0) {
		// Auto-generate rowid
		docID := t.nextRowID
		t.nextRowID++
		return docID, nil
	}

	rid, ok := newRowID.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid rowid type")
	}

	docID := DocumentID(rid)

	// Update nextRowID if needed
	if docID >= t.nextRowID {
		t.nextRowID = docID + 1
	}

	return docID, nil
}

// removeDocument removes a document from the index and storage.
func (t *FTS5Table) removeDocument(docID DocumentID) {
	t.index.RemoveDocument(docID)
	delete(t.rows, docID)
}

// extractColumnValues extracts column values from argv and prepares them for indexing.
func (t *FTS5Table) extractColumnValues(argc int, argv []interface{}) ([]interface{}, map[int]string, error) {
	columnCount := len(t.columns)
	if argc-2 < columnCount {
		return nil, nil, fmt.Errorf("not enough values for columns")
	}

	columnValues := make([]interface{}, columnCount)
	columnTexts := make(map[int]string)

	for i := 0; i < columnCount; i++ {
		value := argv[i+2]
		columnValues[i] = value

		// Convert to string for indexing
		if value != nil {
			columnTexts[i] = t.convertToString(value)
		}
	}

	return columnValues, columnTexts, nil
}

// convertToString converts a value to string for indexing.
func (t *FTS5Table) convertToString(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", value)
}

// Destroy is called when the table is dropped.
func (t *FTS5Table) Destroy() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.index.Clear()
	t.rows = make(map[DocumentID][]interface{})

	// Drop shadow tables
	if t.shadowMgr != nil {
		return t.shadowMgr.DropShadowTables()
	}
	return nil
}

// FTS5Cursor represents a cursor for scanning FTS5 results.
type FTS5Cursor struct {
	vtab.BaseCursor
	table   *FTS5Table
	results []SearchResult
	pos     int
	query   string
	// rowSnap holds a snapshot of row data captured under the read lock
	// during Filter, so that Column can serve results without re-locking.
	rowSnap map[DocumentID][]interface{}
}

// Filter initializes the cursor with search constraints.
func (c *FTS5Cursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
	c.table.mu.RLock()
	defer c.table.mu.RUnlock()

	if idxNum == 1 && len(argv) > 0 {
		if err := c.executeMatchQuery(argv[0]); err != nil {
			return err
		}
	} else {
		c.loadAllDocuments()
		c.positionAtFirst()
	}

	c.snapshotRows()
	return nil
}

// snapshotRows captures row data for all results so that Column can
// serve values without re-acquiring the table lock.
func (c *FTS5Cursor) snapshotRows() {
	c.rowSnap = make(map[DocumentID][]interface{}, len(c.results))
	for _, r := range c.results {
		if row, ok := c.table.rows[r.DocID]; ok {
			c.rowSnap[r.DocID] = row
		}
	}
}

// executeMatchQuery executes a MATCH query and populates results.
func (c *FTS5Cursor) executeMatchQuery(arg interface{}) error {
	queryStr, ok := arg.(string)
	if !ok {
		return nil
	}

	c.query = queryStr

	query, err := c.parseQuery(queryStr)
	if err != nil {
		return err
	}

	results, err := c.executeQuery(query)
	if err != nil {
		return err
	}

	c.results = results
	c.positionAtFirst()
	return nil
}

// parseQuery parses the query string.
func (c *FTS5Cursor) parseQuery(queryStr string) (*Query, error) {
	parser := NewQueryParser(c.table.tokenizer)
	query, err := parser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("invalid FTS query: %v", err)
	}
	return query, nil
}

// executeQuery executes the parsed query.
func (c *FTS5Cursor) executeQuery(query *Query) ([]SearchResult, error) {
	executor := NewQueryExecutor(c.table.index, c.table.ranker)
	results, err := executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}
	return results, nil
}

// loadAllDocuments loads all documents when no MATCH constraint is present.
func (c *FTS5Cursor) loadAllDocuments() {
	docIDs := c.table.index.GetAllDocuments()
	c.results = make([]SearchResult, len(docIDs))
	for i, docID := range docIDs {
		c.results[i] = SearchResult{
			DocID: docID,
			Score: 0.0,
		}
	}
}

// positionAtFirst positions the cursor at the first result.
func (c *FTS5Cursor) positionAtFirst() {
	if len(c.results) > 0 {
		c.pos = 0
	} else {
		c.pos = -1
	}
}

// Next advances to the next result.
func (c *FTS5Cursor) Next() error {
	c.pos++
	return nil
}

// EOF returns true if we're past the last result.
func (c *FTS5Cursor) EOF() bool {
	return c.pos < 0 || c.pos >= len(c.results)
}

// Column returns the value of a column for the current row.
func (c *FTS5Cursor) Column(index int) (interface{}, error) {
	if c.EOF() {
		return nil, fmt.Errorf("cursor at EOF")
	}

	result := c.results[c.pos]

	if index == -1 {
		return result.Score, nil
	}

	row, exists := c.rowSnap[result.DocID]
	if !exists {
		return nil, fmt.Errorf("document not found")
	}

	if index < 0 || index >= len(row) {
		return nil, fmt.Errorf("column index out of range: %d", index)
	}

	return row[index], nil
}

// Rowid returns the rowid of the current row.
func (c *FTS5Cursor) Rowid() (int64, error) {
	if c.EOF() {
		return 0, fmt.Errorf("cursor at EOF")
	}

	return int64(c.results[c.pos].DocID), nil
}

// Close closes the cursor.
func (c *FTS5Cursor) Close() error {
	c.results = nil
	c.rowSnap = nil
	return nil
}

// RegisterFTS5 registers the FTS5 module with the virtual table registry.
func RegisterFTS5() error {
	return vtab.RegisterModule("fts5", NewFTS5Module())
}

// Helper functions for using FTS5 features

// BM25 returns the BM25 rank for a document.
// This would typically be exposed as a SQL function.
func BM25(index *InvertedIndex, docID DocumentID, terms []string) float64 {
	ranker := NewBM25Ranker()
	return ranker.Score(index, docID, terms)
}

// Snippet generates a snippet of text highlighting matches.
// This would typically be exposed as a SQL function.
func Snippet(text string, matchTerms []string, startMark, endMark string, maxTokens int) string {
	// For simplicity, use the highlight function
	return HighlightText(text, matchTerms, startMark, endMark)
}

// Highlight highlights matching terms in text.
// This would typically be exposed as a SQL function.
func Highlight(text string, matchTerms []string, startMark, endMark string) string {
	return HighlightText(text, matchTerms, startMark, endMark)
}
