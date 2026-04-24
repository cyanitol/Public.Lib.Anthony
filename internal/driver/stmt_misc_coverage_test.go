// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// ---------------------------------------------------------------------------
// checkConnClosed
// ---------------------------------------------------------------------------

func TestCheckConnClosed(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		if err := s.checkConnClosed(); err != nil {
			t.Errorf("expected nil for open connection, got %v", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		c.closed = true
		if err := s.checkConnClosed(); err != driver.ErrBadConn {
			t.Errorf("expected ErrBadConn for closed connection, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// selectFromTableRef
// ---------------------------------------------------------------------------

func TestSelectFromTableRef(t *testing.T) {
	t.Run("no_from", func(t *testing.T) {
		stmt := &parser.SelectStmt{}
		_, err := selectFromTableRef(stmt)
		if err == nil {
			t.Error("expected error for SELECT without FROM, got nil")
		}
	})

	t.Run("empty_tables", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}
		_, err := selectFromTableRef(stmt)
		if err == nil {
			t.Error("expected error for FROM with no tables, got nil")
		}
	})

	t.Run("success", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{
				Tables: []parser.TableOrSubquery{{TableName: "foo"}},
			},
		}
		ref, err := selectFromTableRef(stmt)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ref.TableName != "foo" {
			t.Errorf("got table name %q, want %q", ref.TableName, "foo")
		}
	})
}

// ---------------------------------------------------------------------------
// emitAggregateFunction
// ---------------------------------------------------------------------------

func TestEmitAggregateFunction(t *testing.T) {
	t.Run("countStar", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "COUNT", Star: true}
		if err := emitAggregateFunction(vm, fn, 1, nil); err != nil {
			t.Errorf("COUNT(*) should return nil, got %v", err)
		}
	})

	t.Run("knownAggregate", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "SUM"}
		if err := emitAggregateFunction(vm, fn, 1, nil); err != nil {
			t.Errorf("SUM() should return nil, got %v", err)
		}
	})

	t.Run("unknownNilGen", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "UNKNOWN_FUNC"}
		err := emitAggregateFunction(vm, fn, 1, nil)
		if err == nil {
			t.Error("expected error for unknown function with nil generator, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// validateDatabasePath
// ---------------------------------------------------------------------------

func TestValidateDatabasePath(t *testing.T) {
	t.Run("nil_securityConfig", func(t *testing.T) {
		c := openMemConn(t)
		c.securityConfig = nil
		s := stmtFor(c)
		path, err := s.validateDatabasePath("somefile.db")
		if err != nil {
			t.Errorf("nil securityConfig: expected path returned as-is, got error %v", err)
		}
		if path != "somefile.db" {
			t.Errorf("nil securityConfig: expected %q, got %q", "somefile.db", path)
		}
	})

	t.Run("with_traversal", func(t *testing.T) {
		c := openMemConn(t)
		c.securityConfig = security.DefaultSecurityConfig()
		s := stmtFor(c)
		_, err := s.validateDatabasePath("../../etc/passwd")
		if err == nil {
			t.Error("expected error for path traversal, got nil")
		}
	})

	t.Run("valid_relative", func(t *testing.T) {
		c := openMemConn(t)
		cfg := security.DefaultSecurityConfig()
		cfg.EnforceSandbox = false
		cfg.BlockAbsolutePaths = false
		c.securityConfig = cfg
		s := stmtFor(c)
		_, err := s.validateDatabasePath("output.db")
		if err != nil {
			t.Errorf("expected valid path to succeed, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// StmtCache Put
// ---------------------------------------------------------------------------

func TestStmtCachePut(t *testing.T) {
	t.Run("new_entry", func(t *testing.T) {
		cache := NewStmtCache(5)
		cache.Put("SELECT 42", nil)
		if cache.Size() != 1 {
			t.Errorf("expected Size=1 after Put, got %d", cache.Size())
		}
	})

	t.Run("update_existing", func(t *testing.T) {
		cache := NewStmtCache(5)
		cache.Put("SELECT 1", nil)
		cache.Put("SELECT 1", nil)
		if cache.Size() != 1 {
			t.Errorf("expected Size=1 after duplicate Put, got %d", cache.Size())
		}
	})

	t.Run("evict_when_full", func(t *testing.T) {
		cache := NewStmtCache(2)
		cache.Put("SELECT 1", nil)
		cache.Put("SELECT 2", nil)
		cache.Put("SELECT 3", nil)
		if cache.Size() != 2 {
			t.Errorf("expected Size=2 after eviction, got %d", cache.Size())
		}
	})
}

// ---------------------------------------------------------------------------
// validateRecursiveCTE
// ---------------------------------------------------------------------------

func TestValidateRecursiveCTE(t *testing.T) {
	t.Run("nil_compound", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		def := &planner.CTEDefinition{
			Select: &parser.SelectStmt{Compound: nil},
		}
		_, err := s.validateRecursiveCTE(def, "my_cte")
		if err == nil {
			t.Error("expected error for CTE without UNION/UNION ALL compound, got nil")
		}
	})

	t.Run("intersect_compound", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		def := &planner.CTEDefinition{
			Select: &parser.SelectStmt{
				Compound: &parser.CompoundSelect{Op: parser.CompoundIntersect},
			},
		}
		_, err := s.validateRecursiveCTE(def, "my_cte")
		if err == nil {
			t.Error("expected error for INTERSECT compound (not UNION/UNION ALL), got nil")
		}
	})

	t.Run("union_success", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		def := &planner.CTEDefinition{
			Select: &parser.SelectStmt{
				Compound: &parser.CompoundSelect{Op: parser.CompoundUnion},
			},
		}
		compound, err := s.validateRecursiveCTE(def, "my_cte")
		if err != nil {
			t.Errorf("expected success for UNION compound, got %v", err)
		}
		if compound == nil {
			t.Error("expected non-nil compound")
		}
	})

	t.Run("unionAll_success", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		def := &planner.CTEDefinition{
			Select: &parser.SelectStmt{
				Compound: &parser.CompoundSelect{Op: parser.CompoundUnionAll},
			},
		}
		compound, err := s.validateRecursiveCTE(def, "my_cte")
		if err != nil {
			t.Errorf("expected success for UNION ALL compound, got %v", err)
		}
		if compound == nil {
			t.Error("expected non-nil compound")
		}
	})
}

// ---------------------------------------------------------------------------
// adjustCursorOpRegisters
// ---------------------------------------------------------------------------

func TestAdjustCursorOpRegistersColumn(t *testing.T) {
	p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpColumn, 0, 1, 2, 10)
	if p1 != 0 || p2 != 1 || p3 != 12 {
		t.Errorf("OpColumn: got (%d,%d,%d), want (0,1,12)", p1, p2, p3)
	}
}

func TestAdjustCursorOpRegistersRowid(t *testing.T) {
	p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpRowid, 0, 3, 0, 10)
	if p1 != 0 || p2 != 13 || p3 != 0 {
		t.Errorf("OpRowid: got (%d,%d,%d), want (0,13,0)", p1, p2, p3)
	}
}

func TestAdjustCursorOpRegistersInsert(t *testing.T) {
	t.Run("with_p3", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpInsert, 0, 2, 3, 10)
		if p1 != 0 || p2 != 12 || p3 != 13 {
			t.Errorf("OpInsert with p3>0: got (%d,%d,%d), want (0,12,13)", p1, p2, p3)
		}
	})

	t.Run("p3_zero", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpInsert, 0, 2, 0, 10)
		if p1 != 0 || p2 != 12 || p3 != 0 {
			t.Errorf("OpInsert with p3=0: got (%d,%d,%d), want (0,12,0)", p1, p2, p3)
		}
	})
}

func TestAdjustCursorOpRegistersRewind(t *testing.T) {
	p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpRewind, 0, 5, 0, 10)
	if p1 != 0 || p2 != 5 || p3 != 0 {
		t.Errorf("OpRewind: got (%d,%d,%d), want (0,5,0)", p1, p2, p3)
	}
}

func TestAdjustCursorOpRegistersDefault(t *testing.T) {
	p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpDelete, 0, 2, 3, 10)
	if p1 != 0 || p2 != 12 || p3 != 13 {
		t.Errorf("OpDelete with p3>0: got (%d,%d,%d), want (0,12,13)", p1, p2, p3)
	}
}

// ---------------------------------------------------------------------------
// fixInnerRewindAddresses
// ---------------------------------------------------------------------------

func TestFixInnerRewindAddresses(t *testing.T) {
	t.Run("no_rewind", func(t *testing.T) {
		vm := vdbe.New()
		vm.AddOp(vdbe.OpNext, 0, 5, 0)
		fixInnerRewindAddresses(vm)
	})

	t.Run("rewind_p2_nonzero", func(t *testing.T) {
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 0, 9, 0)
		vm.AddOp(vdbe.OpNext, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		if vm.Program[0].P2 != 9 {
			t.Errorf("Rewind with P2!=0 should not be changed, got P2=%d", vm.Program[0].P2)
		}
	})

	t.Run("rewind_p2_zero_with_next", func(t *testing.T) {
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 2, 0, 0)
		vm.AddOp(vdbe.OpColumn, 2, 0, 1)
		vm.AddOp(vdbe.OpNext, 2, 1, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		if vm.Program[0].P2 != 3 {
			t.Errorf("Rewind P2 should be 3 after fix, got %d", vm.Program[0].P2)
		}
	})

	t.Run("rewind_p2_zero_no_next", func(t *testing.T) {
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 5, 0, 0)
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		if vm.Program[0].P2 != 0 {
			t.Errorf("Rewind with no matching Next should stay P2=0, got %d", vm.Program[0].P2)
		}
	})
}

// ---------------------------------------------------------------------------
// findColumnIndex
// ---------------------------------------------------------------------------

func TestFindColumnIndex(t *testing.T) {
	t.Run("nil_table", func(t *testing.T) {
		idx := findColumnIndex(nil, "id")
		if idx != -1 {
			t.Errorf("nil table: expected -1, got %d", idx)
		}
	})

	t.Run("found", func(t *testing.T) {
		tbl := &schema.Table{
			Columns: []*schema.Column{
				{Name: "id"},
				{Name: "name"},
			},
		}
		idx := findColumnIndex(tbl, "name")
		if idx != 1 {
			t.Errorf("expected column index 1 for 'name', got %d", idx)
		}
	})

	t.Run("not_found", func(t *testing.T) {
		tbl := &schema.Table{
			Columns: []*schema.Column{
				{Name: "id"},
			},
		}
		idx := findColumnIndex(tbl, "missing")
		if idx != -1 {
			t.Errorf("expected -1 for missing column, got %d", idx)
		}
	})

	t.Run("case_insensitive", func(t *testing.T) {
		tbl := &schema.Table{
			Columns: []*schema.Column{
				{Name: "MyCol"},
			},
		}
		idx := findColumnIndex(tbl, "mycol")
		if idx != 0 {
			t.Errorf("case-insensitive match: expected 0, got %d", idx)
		}
	})
}

// ---------------------------------------------------------------------------
// runIntegrityCheck
// ---------------------------------------------------------------------------

func TestRunIntegrityCheck(t *testing.T) {
	c := openMemConn(t)
	s := stmtFor(c)
	result := s.runIntegrityCheck()
	if result != "ok" {
		t.Errorf("expected 'ok' from runIntegrityCheck, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// ReadRowByRowid
// ---------------------------------------------------------------------------

func TestReadRowByRowid(t *testing.T) {
	t.Run("table_not_found", func(t *testing.T) {
		c := openMemConn(t)
		r := newDriverRowReader(c)
		_, err := r.ReadRowByRowid("no_such_table", 1)
		if err == nil {
			t.Error("expected error for non-existent table, got nil")
		}
	})

	t.Run("success", func(t *testing.T) {
		c := openMemConn(t)
		execOnConn(t, c, "CREATE TABLE rr_test (id INTEGER PRIMARY KEY, val TEXT)")
		if _, err := c.ExecDML("INSERT INTO rr_test VALUES (1, 'hello')"); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		r := newDriverRowReader(c)
		row, err := r.ReadRowByRowid("rr_test", 1)
		if err != nil {
			t.Fatalf("ReadRowByRowid: %v", err)
		}
		if row == nil {
			t.Error("expected non-nil row")
		}
	})

	t.Run("row_not_found", func(t *testing.T) {
		c := openMemConn(t)
		execOnConn(t, c, "CREATE TABLE rr_miss (id INTEGER PRIMARY KEY, val TEXT)")
		r := newDriverRowReader(c)
		_, err := r.ReadRowByRowid("rr_miss", 999)
		if err == nil {
			t.Error("expected error for missing rowid, got nil")
		}
	})
}

// ---------------------------------------------------------------------------
// validateVacuumContext
// ---------------------------------------------------------------------------

func TestValidateVacuumContext(t *testing.T) {
	t.Run("inTx", func(t *testing.T) {
		c := openMemConn(t)
		c.inTx = true
		s := stmtFor(c)
		err := s.validateVacuumContext()
		if err == nil {
			t.Error("expected error for VACUUM inside transaction, got nil")
		}
	})

	t.Run("not_inTx", func(t *testing.T) {
		c := openMemConn(t)
		c.inTx = false
		s := stmtFor(c)
		err := s.validateVacuumContext()
		if err != nil {
			if err.Error() == "cannot VACUUM inside a transaction" {
				t.Error("got unexpected transaction error when inTx=false")
			}
		}
	})
}

// ---------------------------------------------------------------------------
// getIntoFilenameFromArgs
// ---------------------------------------------------------------------------

func TestGetIntoFilenameFromArgs(t *testing.T) {
	t.Run("no_args", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		_, err := s.getIntoFilenameFromArgs(nil)
		if err == nil {
			t.Error("expected error for no args, got nil")
		}
	})

	t.Run("non_string", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		args := []driver.NamedValue{{Ordinal: 1, Value: 42}}
		_, err := s.getIntoFilenameFromArgs(args)
		if err == nil {
			t.Error("expected error for non-string filename, got nil")
		}
	})

	t.Run("success", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		args := []driver.NamedValue{{Ordinal: 1, Value: "output.db"}}
		name, err := s.getIntoFilenameFromArgs(args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if name != "output.db" {
			t.Errorf("expected %q, got %q", "output.db", name)
		}
	})
}
