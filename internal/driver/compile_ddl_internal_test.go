// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/constraint"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// newMinimalStmt builds the smallest Stmt+Conn needed to call DDL helpers directly.
// The caller is responsible for setting conn fields like btree/fkManager/schema.
func newMinimalStmt(sc *schema.Schema) *Stmt {
	wmu := &sync.Mutex{}
	wv := uint64(0)
	c := &Conn{
		schema:       sc,
		writeMu:      wmu,
		writeVersion: &wv,
		stmts:        make(map[*Stmt]struct{}),
	}
	return &Stmt{conn: c}
}

// TestCompileDDLInternal_PerformDropTable_DropTableError exercises the error
// return when DropTable fails because the table does not exist in the schema.
func TestCompileDDLInternal_PerformDropTable_DropTableError(t *testing.T) {
	sc := schema.NewSchema()
	s := newMinimalStmt(sc)

	// "missing_table" was never added to the schema, so DropTable must return an error.
	err := s.performDropTable("missing_table", nil)
	if err == nil {
		t.Fatal("expected error when dropping a table that does not exist in schema, got nil")
	}
}

// TestCompileDDLInternal_PerformDropTable_BtreeNil confirms that when btree is
// nil the function succeeds (no SaveToMaster call is made).
func TestCompileDDLInternal_PerformDropTable_BtreeNil(t *testing.T) {
	sc := schema.NewSchema()
	// Add the table so DropTable succeeds.
	tbl := &schema.Table{Name: "t1"}
	sc.Tables = map[string]*schema.Table{"t1": tbl}

	s := newMinimalStmt(sc)
	// conn.btree is nil by default — SaveToMaster must be skipped.

	err := s.performDropTable("t1", tbl)
	if err != nil {
		t.Fatalf("unexpected error with nil btree: %v", err)
	}
}

// TestCompileDDLInternal_PerformDropTable_BtreeNilWithFK confirms that when
// fkManager is set and btree is nil the constraints are removed and no error
// is returned.
func TestCompileDDLInternal_PerformDropTable_BtreeNilWithFK(t *testing.T) {
	sc := schema.NewSchema()
	tbl := &schema.Table{Name: "t1"}
	sc.Tables = map[string]*schema.Table{"t1": tbl}

	s := newMinimalStmt(sc)
	s.conn.fkManager = constraint.NewForeignKeyManager()

	err := s.performDropTable("t1", tbl)
	if err != nil {
		t.Fatalf("unexpected error with fkManager set and nil btree: %v", err)
	}
}

// failingPageProvider is a PageProvider whose AllocatePageData and MarkDirty
// always succeed, but GetPageData always returns an error for any page beyond
// the initial seed.  We use it to verify that SaveToMaster propagates write
// errors when the btree cannot retrieve pages.
type failingPageProvider struct {
	// pages is a tiny seed so page 1 can be "found" and overwritten,
	// but any subsequent access to page 1 via the provider is blocked.
	failGet bool
}

func (p *failingPageProvider) GetPageData(pgno uint32) ([]byte, error) {
	if p.failGet {
		return nil, fmt.Errorf("injected page provider error for pgno %d", pgno)
	}
	// Return a zero-filled page of 4096 bytes (valid size, uninitialized header).
	return make([]byte, 4096), nil
}

func (p *failingPageProvider) AllocatePageData() (uint32, []byte, error) {
	return 0, nil, fmt.Errorf("injected allocate error")
}

func (p *failingPageProvider) MarkDirty(_ uint32) error {
	return fmt.Errorf("injected mark-dirty error")
}

// TestCompileDDLInternal_PerformDropTable_SaveToMasterError exercises the
// error path inside "if s.conn.btree != nil" when SaveToMaster fails.
// We use a MarkDirty-failing provider: ensureMasterPageInitialized calls
// MarkDirty when a provider is present, so the error bubbles through
// SaveToMaster → performDropTable.
func TestCompileDDLInternal_PerformDropTable_SaveToMasterError(t *testing.T) {
	sc := schema.NewSchema()
	tbl := &schema.Table{Name: "t1"}
	sc.Tables = map[string]*schema.Table{"t1": tbl}

	s := newMinimalStmt(sc)

	// Build a btree whose provider makes MarkDirty fail.
	// ensureMasterPageInitialized will call MarkDirty(1) when Provider != nil.
	bt := btree.NewBtree(4096)
	bt.Provider = &failingPageProvider{}
	s.conn.btree = bt

	err := s.performDropTable("t1", tbl)
	if err == nil {
		t.Fatal("expected error from SaveToMaster via failing provider, got nil")
	}
}

// TestCompileDDLInternal_EnsureSqliteSequenceTable_AlreadyExists verifies the
// early return when the sqlite_sequence table is already present.
func TestCompileDDLInternal_EnsureSqliteSequenceTable_AlreadyExists(t *testing.T) {
	sc := schema.NewSchema()
	// Pre-populate sqlite_sequence so the early-exit branch is taken.
	sc.Tables = map[string]*schema.Table{
		"sqlite_sequence": {Name: "sqlite_sequence"},
	}

	s := newMinimalStmt(sc)
	// btree is nil so CreateTable would panic if reached — confirms early return.

	err := s.ensureSqliteSequenceTable()
	if err != nil {
		t.Fatalf("unexpected error when sqlite_sequence already exists: %v", err)
	}
}

// TestCompileDDLInternal_EnsureSqliteSequenceTable_BtreeNil confirms the
// in-memory placeholder path (btree == nil, creates table with rootPage 3).
func TestCompileDDLInternal_EnsureSqliteSequenceTable_BtreeNil(t *testing.T) {
	sc := schema.NewSchema()
	s := newMinimalStmt(sc)
	// btree remains nil → placeholder root-page path.

	err := s.ensureSqliteSequenceTable()
	if err != nil {
		t.Fatalf("unexpected error with nil btree: %v", err)
	}

	_, exists := sc.GetTable("sqlite_sequence")
	if !exists {
		t.Fatal("expected sqlite_sequence table to be created")
	}
}

// TestCompileDDLInternal_EnsureSqliteSequenceTable_BtreeWithPages confirms the
// path where btree is non-nil and CreateTable succeeds (btree has a valid
// in-memory provider via a real memory connection).
func TestCompileDDLInternal_EnsureSqliteSequenceTable_BtreeWithPages(t *testing.T) {
	drv := &Driver{}
	driverConn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open memory db: %v", err)
	}
	defer driverConn.Close()

	c := driverConn.(*Conn)

	s := &Stmt{conn: c}
	err = s.ensureSqliteSequenceTable()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, exists := c.schema.GetTable("sqlite_sequence")
	if !exists {
		t.Fatal("expected sqlite_sequence to exist after creation")
	}
}

// TestCompileDDLInternal_CompileCreateTrigger_IfNotExistsSilent verifies the
// branch where a trigger already exists and IfNotExists is true — the function
// should succeed silently without returning an error.
func TestCompileDDLInternal_CompileCreateTrigger_IfNotExistsSilent(t *testing.T) {
	drv := &Driver{}
	driverConn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open memory db: %v", err)
	}
	defer driverConn.Close()

	c := driverConn.(*Conn)

	// Create the table the trigger references.
	c.schema.Tables = map[string]*schema.Table{
		"t1": {Name: "t1"},
	}

	// Create the trigger the first time.
	trigStmt := &parser.CreateTriggerStmt{
		Name:     "trg1",
		Table:    "t1",
		Timing:   parser.TriggerAfter,
		Event:    parser.TriggerInsert,
		IfNotExists: false,
	}
	_, err = c.schema.CreateTrigger(trigStmt)
	if err != nil {
		t.Fatalf("failed to create trigger: %v", err)
	}

	// Now compile a CREATE TRIGGER IF NOT EXISTS for the same name — should
	// enter the IfNotExists silent-success branch.
	s := &Stmt{conn: c}
	vm := vdbe.New()

	ifNotExistsStmt := &parser.CreateTriggerStmt{
		Name:        "trg1",
		Table:       "t1",
		Timing:      parser.TriggerAfter,
		Event:       parser.TriggerInsert,
		IfNotExists: true,
	}

	result, err := s.compileCreateTrigger(vm, ifNotExistsStmt, nil)
	if err != nil {
		t.Fatalf("expected nil error for IF NOT EXISTS on existing trigger, got: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil VDBE result")
	}
}

// TestCompileDDLInternal_CompileCreateTrigger_TableNotFound verifies that
// creating a trigger on a non-existent table returns an error.
func TestCompileDDLInternal_CompileCreateTrigger_TableNotFound(t *testing.T) {
	drv := &Driver{}
	driverConn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open memory db: %v", err)
	}
	defer driverConn.Close()

	c := driverConn.(*Conn)

	s := &Stmt{conn: c}
	vm := vdbe.New()

	trigStmt := &parser.CreateTriggerStmt{
		Name:        "trg_no_table",
		Table:       "does_not_exist",
		Timing:      parser.TriggerAfter,
		Event:       parser.TriggerInsert,
		IfNotExists: false,
	}

	_, err = s.compileCreateTrigger(vm, trigStmt, nil)
	if err == nil {
		t.Fatal("expected error when trigger table does not exist, got nil")
	}
}
