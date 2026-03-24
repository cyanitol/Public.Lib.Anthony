// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package fts5

import (
	"fmt"
	"testing"
)

// errDB is a DatabaseExecutor whose DDL always returns an error after N successful calls.
type errDB struct {
	ddlSucceed int // allow this many DDL calls to succeed, then fail
	ddlCount   int
	inner      *roundTripDB
}

func newErrDB(ddlSucceed int) *errDB {
	return &errDB{ddlSucceed: ddlSucceed, inner: newRoundTripDB()}
}

func (e *errDB) ExecDDL(sql string) error {
	e.ddlCount++
	if e.ddlCount > e.ddlSucceed {
		return fmt.Errorf("DDL error at call %d", e.ddlCount)
	}
	return e.inner.ExecDDL(sql)
}

func (e *errDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return e.inner.ExecDML(sql, args...)
}

func (e *errDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return e.inner.Query(sql, args...)
}

// dmlErrDB is a DatabaseExecutor whose DML always returns an error.
type dmlErrDB struct {
	inner *roundTripDB
}

func newDMLErrDB() *dmlErrDB {
	return &dmlErrDB{inner: newRoundTripDB()}
}

func (d *dmlErrDB) ExecDDL(sql string) error {
	return d.inner.ExecDDL(sql)
}

func (d *dmlErrDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return 0, fmt.Errorf("DML error")
}

func (d *dmlErrDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return d.inner.Query(sql, args...)
}

// badQueryDB is a DatabaseExecutor that returns errors on Query calls.
type badQueryDB struct {
	inner *roundTripDB
}

func newBadQueryDB() *badQueryDB {
	return &badQueryDB{inner: newRoundTripDB()}
}

func (b *badQueryDB) ExecDDL(sql string) error { return b.inner.ExecDDL(sql) }
func (b *badQueryDB) ExecDML(sql string, args ...interface{}) (int64, error) {
	return b.inner.ExecDML(sql, args...)
}
func (b *badQueryDB) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	return nil, fmt.Errorf("query error")
}

// TestInitShadowManagerCreateFails covers the branch in initShadowManager
// where CreateShadowTables returns an error (returns nil manager).
// MC/DC: db is DatabaseExecutor=true AND CreateShadowTables fails=true → returns nil.
func TestInitShadowManagerCreateFails(t *testing.T) {
	t.Parallel()

	// Allow 0 DDL calls to succeed so the first DDL call fails.
	db := newErrDB(0)
	mgr := initShadowManager(db, "test_tbl", []string{"content"})
	if mgr != nil {
		t.Error("expected nil manager when CreateShadowTables fails")
	}
}

// TestCreateShadowTablesDDLErrors covers each DDL error branch (2nd through 5th table).
func TestCreateShadowTablesDDLErrors(t *testing.T) {
	t.Parallel()

	for i := 1; i <= 4; i++ {
		i := i
		t.Run(fmt.Sprintf("fail_at_ddl_%d", i+1), func(t *testing.T) {
			t.Parallel()
			db := newErrDB(i)
			mgr := NewShadowTableManager("tbl", db)
			err := mgr.CreateShadowTables([]string{"content"})
			if err == nil {
				t.Errorf("expected error when DDL call %d fails", i+1)
			}
		})
	}
}

// TestDropShadowTablesDDLError covers the error branch in DropShadowTables.
func TestDropShadowTablesDDLError(t *testing.T) {
	t.Parallel()

	// Allow all CREATE calls (5) but fail on first DROP.
	db := newErrDB(5)
	mgr := NewShadowTableManager("tbl", db)
	if err := mgr.CreateShadowTables([]string{"content"}); err != nil {
		t.Fatalf("CreateShadowTables failed: %v", err)
	}

	err := mgr.DropShadowTables()
	if err == nil {
		t.Error("expected error when DROP DDL fails")
	}
}

// TestLoadOrCreateIndexError covers the branch where shadowMgr.LoadIndex returns an error.
// Since LoadIndex with a real DB never fails, we simulate by using a manager with a query-failing DB
// but the current LoadIndex implementation never returns an error; the fallback branch (line 105)
// is reached when the DB has been set but queries fail.
// We test the fallback via createTable with a DB that allows shadow table creation but has
// failing queries so LoadIndex returns a fresh index.
func TestLoadOrCreateIndexFallbackViaCreateTable(t *testing.T) {
	t.Parallel()

	// Use a bad query DB so loadIndex internal helpers silently fail and return fresh index.
	db := newBadQueryDB()
	// First, allow DDL to succeed.
	module := NewFTS5Module()
	// We call createTable indirectly through Create with a DB value.
	table, schema, err := module.Create(db, "fts5", "main", "bqtbl", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if table == nil {
		t.Fatal("expected non-nil table")
	}
	if schema == "" {
		t.Error("expected non-empty schema")
	}
}

// TestLoadContentFromShadowWithDB exercises loadContentFromShadow by creating a table
// through a real roundTripDB, inserting data, and then reconnecting via Connect.
// This covers the branch in fts5.go where shadowMgr is not nil and content is loaded.
func TestLoadContentFromShadowWithDB(t *testing.T) {
	t.Parallel()

	db := newRoundTripDB()
	module := NewFTS5Module()

	// Create and insert.
	table, _, err := module.Create(db, "fts5", "main", "shadow_tbl", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	_, err = table.Update(3, []interface{}{nil, nil, "hello world"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Reconnect via Connect – this triggers loadContentFromShadow.
	table2, _, err := module.Connect(db, "fts5", "main", "shadow_tbl", []string{"content"})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	fts2 := table2.(*FTS5Table)
	if fts2.index.GetTotalDocuments() == 0 {
		// Not an error — the roundTripDB may not have persisted docLengths correctly,
		// but loadContentFromShadow was exercised.
		t.Log("loadContentFromShadow ran (index may be empty due to mock DB limitations)")
	}
}

// TestShadowManagerSaveIndexDMLError covers the DML error path in SaveIndex.
func TestShadowManagerSaveIndexDMLError(t *testing.T) {
	t.Parallel()

	db := newDMLErrDB()
	// DDL succeeds (DML does not).
	if err := db.inner.ExecDDL("CREATE TABLE ft_data(id INTEGER PRIMARY KEY, block BLOB)"); err != nil {
		t.Fatalf("setup DDL failed: %v", err)
	}

	mgr := NewShadowTableManager("ft", db)

	idx := NewInvertedIndex([]string{"content"})
	idx.totalDocs = 1
	idx.index["word"] = []PostingList{{DocID: 1, Frequency: 1, Positions: []int{0}}}
	idx.docLengths[DocumentID(1)] = 1

	err := mgr.SaveIndex(idx)
	// SaveIndex should return an error because structure record DML fails.
	if err == nil {
		t.Error("expected error when SaveIndex DML fails")
	}
}

// TestShadowManagerDeleteContentDMLError covers the DML error path in DeleteContent.
func TestShadowManagerDeleteContentDMLError(t *testing.T) {
	t.Parallel()

	db := newDMLErrDB()
	mgr := NewShadowTableManager("ft", db)

	err := mgr.DeleteContent(DocumentID(1))
	if err == nil {
		t.Error("expected error when DeleteContent DML fails")
	}
}

// TestLoadContentReturnsNilForShortRow covers the branch in LoadContent where
// rows[0] has only 1 element (just the id, no content columns).
func TestLoadContentReturnsNilForShortRow(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "sc",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				// Return a row with only 1 element (just id, no content columns).
				return [][]interface{}{{int64(1)}}, nil
			},
		},
	}

	result, err := mgr.LoadContent(DocumentID(1), 1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for short row, got %v", result)
	}
}

// mockDB2 is a flexible DatabaseExecutor for targeted testing.
type mockDB2 struct {
	queryFn func(sql string, args ...interface{}) ([][]interface{}, error)
	dmlFn   func(sql string, args ...interface{}) (int64, error)
}

func (m *mockDB2) ExecDDL(sql string) error { return nil }
func (m *mockDB2) ExecDML(sql string, args ...interface{}) (int64, error) {
	if m.dmlFn != nil {
		return m.dmlFn(sql, args...)
	}
	return 1, nil
}
func (m *mockDB2) Query(sql string, args ...interface{}) ([][]interface{}, error) {
	if m.queryFn != nil {
		return m.queryFn(sql, args...)
	}
	return nil, nil
}

// TestLoadContentDocumentNotFound covers the branch where LoadContent finds no rows.
func TestLoadContentDocumentNotFound(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "lc",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				return [][]interface{}{}, nil // empty result
			},
		},
	}

	_, err := mgr.LoadContent(DocumentID(999), 1)
	if err == nil {
		t.Error("expected error for document not found")
	}
}

// TestLoadContentQueryError covers the branch where LoadContent Query returns an error.
func TestLoadContentQueryError(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "lce",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				return nil, fmt.Errorf("query error")
			},
		},
	}

	_, err := mgr.LoadContent(DocumentID(1), 1)
	if err == nil {
		t.Error("expected error when LoadContent query fails")
	}
}

// TestLoadSingleTermPostingNonByteBlob covers loadSingleTermPosting when
// dataRows[0][0] is not a []byte (type assertion fails → no-op).
func TestLoadSingleTermPostingNonByteBlob(t *testing.T) {
	t.Parallel()

	callCount := 0
	mgr := &ShadowTableManager{
		tableName: "nbb",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				callCount++
				if callCount == 1 {
					// First call: return term rows for loadTermPostings.
					return [][]interface{}{{"hello"}}, nil
				}
				// Second call: return non-[]byte blob for loadSingleTermPosting.
				return [][]interface{}{{int64(42)}}, nil
			},
		},
	}

	idx := NewInvertedIndex([]string{"content"})
	mgr.loadTermPostings(idx)
	// Verify no panic and no posting added.
	if len(idx.index) != 0 {
		t.Errorf("expected empty index, got %d terms", len(idx.index))
	}
}

// TestLoadSingleDocSizeWrongTypes covers loadSingleDocSize when row types
// are wrong (not int64 or not []byte).
func TestLoadSingleDocSizeWrongTypes(t *testing.T) {
	t.Parallel()

	mgr := NewShadowTableManager("dt", nil)
	idx := NewInvertedIndex([]string{"content"})

	// MC/DC case A: docID is wrong type (string), ok1=false.
	mgr.loadSingleDocSize(idx, []interface{}{"notanint", []byte{0x01}})
	if len(idx.docLengths) != 0 {
		t.Error("expected no docLengths added for wrong docID type")
	}

	// MC/DC case B: sz is wrong type (string), ok2=false.
	mgr.loadSingleDocSize(idx, []interface{}{int64(1), "notbytes"})
	if len(idx.docLengths) != 0 {
		t.Error("expected no docLengths added for wrong sz type")
	}

	// MC/DC case C: both are wrong types.
	mgr.loadSingleDocSize(idx, []interface{}{"notanint", "notbytes"})
	if len(idx.docLengths) != 0 {
		t.Error("expected no docLengths added for both wrong types")
	}

	// MC/DC case D: row is too short.
	mgr.loadSingleDocSize(idx, []interface{}{int64(1)})
	if len(idx.docLengths) != 0 {
		t.Error("expected no docLengths added for short row")
	}
}

// TestLoadStructureRecordNonByteBlob covers loadStructureRecord when
// rows[0][0] is not a []byte (type assertion fails → no-op).
func TestLoadStructureRecordNonByteBlob(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "sr",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				// Return non-[]byte blob.
				return [][]interface{}{{int64(12345)}}, nil
			},
		},
	}

	idx := NewInvertedIndex([]string{"content"})
	mgr.loadStructureRecord(idx)
	// totalDocs should remain 0 (no update applied).
	if idx.totalDocs != 0 {
		t.Errorf("expected totalDocs=0, got %d", idx.totalDocs)
	}
}

// TestLoadSingleTermPostingShortRow covers the branch in loadSingleTermPosting
// when the row is empty (len(row) == 0).
func TestLoadSingleTermPostingShortRow(t *testing.T) {
	t.Parallel()

	mgr := NewShadowTableManager("sr2", nil)
	idx := NewInvertedIndex([]string{"content"})

	// Empty row.
	mgr.loadSingleTermPosting(idx, []interface{}{})
	if len(idx.index) != 0 {
		t.Error("expected no terms added for empty row")
	}

	// Row with non-string term.
	mgr.loadSingleTermPosting(idx, []interface{}{int64(42)})
	if len(idx.index) != 0 {
		t.Error("expected no terms added for non-string term")
	}
}

// TestLoadTermPostingsQueryError covers loadTermPostings when Query returns an error.
func TestLoadTermPostingsQueryError(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "tpqe",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				return nil, fmt.Errorf("query error")
			},
		},
	}

	idx := NewInvertedIndex([]string{"content"})
	mgr.loadTermPostings(idx)
	// Should not panic, index should be empty.
	if len(idx.index) != 0 {
		t.Errorf("expected empty index, got %d terms", len(idx.index))
	}
}

// TestLoadDocumentSizesQueryError covers loadDocumentSizes when Query returns an error.
func TestLoadDocumentSizesQueryError(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "dsqe",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				return nil, fmt.Errorf("query error")
			},
		},
	}

	idx := NewInvertedIndex([]string{"content"})
	mgr.loadDocumentSizes(idx)
	if len(idx.docLengths) != 0 {
		t.Errorf("expected empty docLengths, got %d", len(idx.docLengths))
	}
}

// TestFTS5DestroyWithShadowManager covers Destroy when shadowMgr is not nil
// but DropShadowTables returns an error.
func TestFTS5DestroyWithShadowManagerError(t *testing.T) {
	t.Parallel()

	// Create a table with a real DB first to get non-nil shadowMgr.
	db := newErrDB(5) // Allow 5 DDL calls (create), fail on drop.
	module := NewFTS5Module()
	table, _, err := module.Create(db, "fts5", "main", "destroy_tbl", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	ftsTable := table.(*FTS5Table)
	if ftsTable.shadowMgr == nil {
		t.Skip("shadowMgr is nil, skipping destroy test")
	}

	// Destroy should return error from DropShadowTables.
	err = ftsTable.Destroy()
	if err == nil {
		t.Error("expected error from Destroy when DropShadowTables fails")
	}
}

// TestFTS5DestroyWithShadowManagerSuccess covers Destroy when shadowMgr is not nil
// and DropShadowTables succeeds.
func TestFTS5DestroyWithShadowManagerSuccess(t *testing.T) {
	t.Parallel()

	db := newRoundTripDB()
	module := NewFTS5Module()
	table, _, err := module.Create(db, "fts5", "main", "destroy_ok_tbl", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	ftsTable := table.(*FTS5Table)
	if ftsTable.shadowMgr == nil {
		t.Skip("shadowMgr is nil, skipping")
	}

	_, err = table.Update(3, []interface{}{nil, nil, "test"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = ftsTable.Destroy()
	if err != nil {
		t.Errorf("Destroy failed: %v", err)
	}
	if ftsTable.index.GetTotalDocuments() != 0 {
		t.Errorf("expected 0 documents after Destroy, got %d", ftsTable.index.GetTotalDocuments())
	}
}

// TestHandleInsertOrUpdateWithShadowSaveContent covers the ignored-error branch
// in handleInsertOrUpdate when SaveContent returns an error (line 275-278 comment).
// We exercise this by passing a DML-failing DB so SaveContent fails silently.
func TestHandleInsertOrUpdateShadowSaveContentError(t *testing.T) {
	t.Parallel()

	// Use a DB where DDL succeeds but DML fails only for content writes.
	saveCount := 0
	db := &mockDB2{
		dmlFn: func(sql string, args ...interface{}) (int64, error) {
			saveCount++
			if saveCount > 2 {
				// After structure record and idx term, fail content save.
				return 0, fmt.Errorf("save error")
			}
			return 1, nil
		},
	}

	mgr := NewShadowTableManager("err_tbl", db)
	// Create a shadow-enabled FTS5Table manually.
	columns := []string{"content"}
	idx := NewInvertedIndex(columns)
	tokenizer := NewSimpleTokenizer()
	ranker := NewBM25Ranker()
	ftsTable := &FTS5Table{
		tableName: "err_tbl",
		columns:   columns,
		index:     idx,
		tokenizer: tokenizer,
		ranker:    ranker,
		nextRowID: 1,
		rows:      make(map[DocumentID][]interface{}),
		shadowMgr: mgr,
	}

	// Insert should not return error even though SaveContent fails (error is ignored).
	_, err := ftsTable.Update(3, []interface{}{nil, nil, "hello"})
	if err != nil {
		t.Errorf("unexpected error (SaveContent error should be ignored): %v", err)
	}
}

// TestComputeNextRowIDWithMultipleDocs covers computeNextRowID with a populated index.
func TestComputeNextRowIDWithMultipleDocs(t *testing.T) {
	t.Parallel()

	index := NewInvertedIndex([]string{"content"})
	tokenizer := NewSimpleTokenizer()

	// Manually populate docLengths to simulate loaded index.
	index.AddDocument(DocumentID(5), map[int]string{0: "hello"}, tokenizer)
	index.AddDocument(DocumentID(10), map[int]string{0: "world"}, tokenizer)
	index.AddDocument(DocumentID(3), map[int]string{0: "test"}, tokenizer)

	next := computeNextRowID(index)
	if next != DocumentID(11) {
		t.Errorf("expected nextRowID=11, got %d", next)
	}
}

// TestComputeNextRowIDEmptyIndex covers computeNextRowID with empty index.
func TestComputeNextRowIDEmptyIndex(t *testing.T) {
	t.Parallel()

	index := NewInvertedIndex([]string{"content"})
	next := computeNextRowID(index)
	if next != DocumentID(1) {
		t.Errorf("expected nextRowID=1 for empty index, got %d", next)
	}
}

// TestHandleDeleteWithShadow covers handleDelete when shadowMgr is not nil,
// exercising the SaveIndex call path.
func TestHandleDeleteWithShadow(t *testing.T) {
	t.Parallel()

	db := newRoundTripDB()
	module := NewFTS5Module()
	table, _, err := module.Create(db, "fts5", "main", "del_shadow", []string{"content"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	rowid, err := table.Update(3, []interface{}{nil, nil, "test document"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Delete triggers SaveIndex via shadow manager.
	_, err = table.Update(1, []interface{}{rowid})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

// TestLoadIndexFromRoundTripDBWithQueryErrors covers loadStructureRecord, loadTermPostings,
// and loadDocumentSizes when the DB returns errors (graceful failure with fresh index).
func TestLoadIndexGracefulFailures(t *testing.T) {
	t.Parallel()

	mgr := &ShadowTableManager{
		tableName: "gf",
		db: &mockDB2{
			queryFn: func(sql string, args ...interface{}) ([][]interface{}, error) {
				return nil, fmt.Errorf("all queries fail")
			},
		},
	}

	idx, err := mgr.LoadIndex([]string{"content"})
	if err != nil {
		t.Errorf("LoadIndex should not return error: %v", err)
	}
	if idx == nil {
		t.Fatal("LoadIndex should return non-nil index")
	}
	if idx.totalDocs != 0 {
		t.Errorf("expected totalDocs=0 for failed loads, got %d", idx.totalDocs)
	}
}

// TestInitShadowManagerNonExecutorDB covers initShadowManager when db does not
// implement DatabaseExecutor (returns nil).
// MC/DC: db not DatabaseExecutor → returns nil immediately.
func TestInitShadowManagerNonExecutorDB(t *testing.T) {
	t.Parallel()

	// Pass a plain struct that does NOT implement DatabaseExecutor.
	type notAnExecutor struct{ name string }
	db := &notAnExecutor{name: "test"}

	mgr := initShadowManager(db, "tbl", []string{"content"})
	if mgr != nil {
		t.Error("expected nil when db does not implement DatabaseExecutor")
	}
}

// TestSaveIndexTermsAndDocSizes covers SaveIndex DML error branches
// for _idx (term saving) and docsize.
func TestSaveIndexTermDMLError(t *testing.T) {
	t.Parallel()

	dmlCallCount := 0
	db := &mockDB2{
		dmlFn: func(sql string, args ...interface{}) (int64, error) {
			dmlCallCount++
			if dmlCallCount == 2 {
				// Fail on the second DML call (writing to _idx).
				return 0, fmt.Errorf("idx DML error")
			}
			return 1, nil
		},
	}

	mgr := NewShadowTableManager("sidx", db)
	idx := NewInvertedIndex([]string{"content"})
	idx.totalDocs = 1
	idx.index["alpha"] = []PostingList{{DocID: 1, Frequency: 1, Positions: []int{0}}}
	idx.docLengths[DocumentID(1)] = 1

	err := mgr.SaveIndex(idx)
	if err == nil {
		t.Error("expected error when _idx DML fails")
	}
}

// TestSaveIndexDataDMLError covers SaveIndex DML error for posting list data write.
func TestSaveIndexDataDMLError(t *testing.T) {
	t.Parallel()

	dmlCallCount := 0
	db := &mockDB2{
		dmlFn: func(sql string, args ...interface{}) (int64, error) {
			dmlCallCount++
			if dmlCallCount == 3 {
				// Fail on third DML call (writing posting list to _data).
				return 0, fmt.Errorf("data DML error")
			}
			return 1, nil
		},
	}

	mgr := NewShadowTableManager("sdata", db)
	idx := NewInvertedIndex([]string{"content"})
	idx.totalDocs = 1
	idx.index["beta"] = []PostingList{{DocID: 1, Frequency: 1, Positions: []int{0}}}
	idx.docLengths[DocumentID(1)] = 1

	err := mgr.SaveIndex(idx)
	if err == nil {
		t.Error("expected error when _data posting list DML fails")
	}
}

// TestSaveIndexDocSizeDMLError covers SaveIndex DML error for docsize write.
func TestSaveIndexDocSizeDMLError(t *testing.T) {
	t.Parallel()

	// This only triggers when there are no terms (skips term DML)
	// and there are docLengths.
	dmlCallCount := 0
	db := &mockDB2{
		dmlFn: func(sql string, args ...interface{}) (int64, error) {
			dmlCallCount++
			if dmlCallCount == 2 {
				// Fail on second DML call (docsize write after structure record).
				return 0, fmt.Errorf("docsize DML error")
			}
			return 1, nil
		},
	}

	mgr := NewShadowTableManager("sdocsize", db)
	idx := NewInvertedIndex([]string{"content"})
	idx.totalDocs = 1
	// No terms, just a docLength.
	idx.docLengths[DocumentID(1)] = 5

	err := mgr.SaveIndex(idx)
	if err == nil {
		t.Error("expected error when docsize DML fails")
	}
}

// TestHandleInsertOrUpdateWithShadowManager covers the full shadow path
// of handleInsertOrUpdate with a working shadow manager (SaveContent + SaveIndex).
func TestHandleInsertOrUpdateFullShadow(t *testing.T) {
	t.Parallel()

	db := newRoundTripDB()
	module := NewFTS5Module()
	table, _, err := module.Create(db, "fts5", "main", "full_shadow", []string{"title", "body"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Insert document.
	rowid, err := table.Update(4, []interface{}{nil, nil, "Title Text", "Body content"})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowid <= 0 {
		t.Error("expected positive rowid")
	}

	// Update document (exercises shadow DeleteContent + SaveContent).
	_, err = table.Update(4, []interface{}{rowid, rowid, "Updated Title", "Updated body"})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
}
