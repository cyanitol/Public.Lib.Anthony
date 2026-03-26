// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// stubPager is a minimal PagerInterface implementation that lets tests control
// exactly which methods succeed or fail.
// ---------------------------------------------------------------------------

type stubPager struct {
	inWriteTx    bool
	commitErr    error
	endReadErr   error
}

func (p *stubPager) Get(_ pager.Pgno) (*pager.DbPage, error)    { return nil, nil }
func (p *stubPager) Put(_ *pager.DbPage)                        {}
func (p *stubPager) PageSize() int                              { return 4096 }
func (p *stubPager) PageCount() pager.Pgno                      { return 0 }
func (p *stubPager) IsReadOnly() bool                           { return false }
func (p *stubPager) GetHeader() *pager.DatabaseHeader           { return nil }
func (p *stubPager) GetFreePageCount() uint32                   { return 0 }
func (p *stubPager) Write(_ *pager.DbPage) error                { return nil }
func (p *stubPager) AllocatePage() (pager.Pgno, error)          { return 0, nil }
func (p *stubPager) FreePage(_ pager.Pgno) error                { return nil }
func (p *stubPager) Vacuum(_ *pager.VacuumOptions) error        { return nil }
func (p *stubPager) SetUserVersion(_ uint32) error              { return nil }
func (p *stubPager) SetSchemaCookie(_ uint32) error             { return nil }
func (p *stubPager) VerifyFreeList() error                      { return nil }
func (p *stubPager) BeginRead() error                           { return nil }
func (p *stubPager) BeginWrite() error                          { return nil }
func (p *stubPager) Rollback() error                            { return nil }
func (p *stubPager) Savepoint(_ string) error                   { return nil }
func (p *stubPager) Release(_ string) error                     { return nil }
func (p *stubPager) RollbackTo(_ string) error                  { return nil }
func (p *stubPager) Close() error                               { return nil }

func (p *stubPager) InWriteTransaction() bool {
	return p.inWriteTx
}

func (p *stubPager) Commit() error {
	return p.commitErr
}

func (p *stubPager) EndRead() error {
	return p.endReadErr
}

// ---------------------------------------------------------------------------
// TestStmtVacuum2Coverage — validateVacuumContext branches
// ---------------------------------------------------------------------------

// TestStmtVacuum2Coverage_InTx exercises the "cannot VACUUM inside a
// transaction" branch (line 50-52 of stmt_vacuum.go).
// When database/sql begins a transaction, the driver sets conn.inTx = true.
// Executing VACUUM from inside that transaction must return an error.
func TestStmtVacuum2Coverage_InTx(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "intx.db")

	d := &Driver{}
	raw, err := d.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c := raw.(*Conn)
	defer c.Close()

	// Simulate being inside a transaction.
	c.inTx = true

	s := stmtFor(c)
	err = s.validateVacuumContext()
	if err == nil {
		t.Fatal("expected error for VACUUM inside transaction, got nil")
	}
	if !strings.Contains(err.Error(), "cannot VACUUM inside a transaction") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestStmtVacuum2Coverage_EndReadError exercises the EndRead error branch
// (lines 63-67 of stmt_vacuum.go).
// When EndRead returns an error that is neither "no transaction active" nor
// "no read transaction to end", validateVacuumContext must propagate it.
func TestStmtVacuum2Coverage_EndReadError(t *testing.T) {
	c := openMemConn(t)

	// Replace the pager with a stub that returns a fatal EndRead error.
	stub := &stubPager{
		inWriteTx:  false,
		endReadErr: errors.New("disk fault during EndRead"),
	}
	c.pager = stub

	s := stmtFor(c)
	err := s.validateVacuumContext()
	if err == nil {
		t.Fatal("expected error from EndRead, got nil")
	}
	if !strings.Contains(err.Error(), "disk fault during EndRead") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestStmtVacuum2Coverage_EndReadIgnored_NoTransaction exercises the branch
// where EndRead returns the "no transaction active" error, which must be
// silently ignored (lines 64-67 of stmt_vacuum.go).
func TestStmtVacuum2Coverage_EndReadIgnored_NoTransaction(t *testing.T) {
	c := openMemConn(t)

	stub := &stubPager{
		inWriteTx:  false,
		endReadErr: errors.New("no transaction active"),
	}
	c.pager = stub

	s := stmtFor(c)
	if err := s.validateVacuumContext(); err != nil {
		t.Errorf("expected nil for ignored EndRead error, got %v", err)
	}
}

// TestStmtVacuum2Coverage_EndReadIgnored_NoReadTx exercises the second
// ignored-error variant: "no read transaction to end".
func TestStmtVacuum2Coverage_EndReadIgnored_NoReadTx(t *testing.T) {
	c := openMemConn(t)

	stub := &stubPager{
		inWriteTx:  false,
		endReadErr: errors.New("no read transaction to end"),
	}
	c.pager = stub

	s := stmtFor(c)
	if err := s.validateVacuumContext(); err != nil {
		t.Errorf("expected nil for ignored EndRead error, got %v", err)
	}
}

// TestStmtVacuum2Coverage_CommitError exercises the InWriteTransaction +
// Commit-failure branch (lines 55-58 of stmt_vacuum.go).
func TestStmtVacuum2Coverage_CommitError(t *testing.T) {
	c := openMemConn(t)

	stub := &stubPager{
		inWriteTx:  true,
		commitErr:  errors.New("commit failed: i/o error"),
		endReadErr: errors.New("no transaction active"),
	}
	c.pager = stub

	s := stmtFor(c)
	err := s.validateVacuumContext()
	if err == nil {
		t.Fatal("expected commit error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to commit pending write transaction") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestStmtVacuum2Coverage — setupVacuumIntoSchema branches
// ---------------------------------------------------------------------------

// TestStmtVacuum2Coverage_SetupInto_NilSchema exercises the early-return
// branch in setupVacuumIntoSchema when sourceSchema is nil (line 189-191).
func TestStmtVacuum2Coverage_SetupInto_NilSchema(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)

	// Passing nil schema must return nil without error.
	if err := s.setupVacuumIntoSchema("irrelevant.db", nil); err != nil {
		t.Errorf("expected nil error for nil sourceSchema, got %v", err)
	}
}

// TestStmtVacuum2Coverage_SetupInto_NilBtree exercises the
// "target database state not found" branch (lines 205-207) by pre-populating
// the driver's dbs map with a state that has a nil btree.
// registerTargetSchema will find the entry and update its schema field, but
// dbState.btree remains nil, which triggers the error.
func TestStmtVacuum2Coverage_SetupInto_NilBtree(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)

	// Create a schema with a table so setupVacuumIntoSchema has real work to do.
	src := schema.NewSchema()

	// Ensure conn.driver.dbs is initialised.
	c.driver.mu.Lock()
	if c.driver.dbs == nil {
		c.driver.dbs = make(map[string]*dbState)
	}
	targetKey := "ghost_target.db"
	// Pre-register the target with a nil btree so the existence check passes
	// but the btree-nil check fires.
	c.driver.dbs[targetKey] = &dbState{btree: nil, pager: &stubPager{}}
	c.driver.mu.Unlock()

	err := s.setupVacuumIntoSchema(targetKey, src)
	if err == nil {
		t.Fatal("expected error for nil btree in dbState, got nil")
	}
	if !strings.Contains(err.Error(), "target database state not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}
