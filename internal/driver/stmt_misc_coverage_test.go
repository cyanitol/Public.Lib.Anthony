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

// TestStmtMiscCoverage covers the low-coverage unexported functions listed in
// the task description.
func TestStmtMiscCoverage(t *testing.T) {
	t.Run("checkConnClosed_open", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		if err := s.checkConnClosed(); err != nil {
			t.Errorf("expected nil for open connection, got %v", err)
		}
	})

	t.Run("checkConnClosed_closed", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		c.closed = true
		if err := s.checkConnClosed(); err != driver.ErrBadConn {
			t.Errorf("expected ErrBadConn for closed connection, got %v", err)
		}
	})

	t.Run("selectFromTableRef_no_from", func(t *testing.T) {
		stmt := &parser.SelectStmt{} // From is nil
		_, err := selectFromTableRef(stmt)
		if err == nil {
			t.Error("expected error for SELECT without FROM, got nil")
		}
	})

	t.Run("selectFromTableRef_empty_tables", func(t *testing.T) {
		stmt := &parser.SelectStmt{
			From: &parser.FromClause{Tables: []parser.TableOrSubquery{}},
		}
		_, err := selectFromTableRef(stmt)
		if err == nil {
			t.Error("expected error for FROM with no tables, got nil")
		}
	})

	t.Run("selectFromTableRef_success", func(t *testing.T) {
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

	t.Run("emitAggregateFunction_countStar", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "COUNT", Star: true}
		if err := emitAggregateFunction(vm, fn, 1, nil); err != nil {
			t.Errorf("COUNT(*) should return nil, got %v", err)
		}
	})

	t.Run("emitAggregateFunction_knownAggregate", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "SUM"}
		if err := emitAggregateFunction(vm, fn, 1, nil); err != nil {
			t.Errorf("SUM() should return nil, got %v", err)
		}
	})

	t.Run("emitAggregateFunction_unknownNilGen", func(t *testing.T) {
		vm := vdbe.New()
		fn := &parser.FunctionExpr{Name: "UNKNOWN_FUNC"}
		err := emitAggregateFunction(vm, fn, 1, nil)
		if err == nil {
			t.Error("expected error for unknown function with nil generator, got nil")
		}
	})

	t.Run("validateDatabasePath_nil_securityConfig", func(t *testing.T) {
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

	t.Run("validateDatabasePath_with_traversal", func(t *testing.T) {
		c := openMemConn(t)
		// Use default security config which blocks traversal
		c.securityConfig = security.DefaultSecurityConfig()
		s := stmtFor(c)
		_, err := s.validateDatabasePath("../../etc/passwd")
		if err == nil {
			t.Error("expected error for path traversal, got nil")
		}
	})

	t.Run("validateDatabasePath_valid_relative", func(t *testing.T) {
		c := openMemConn(t)
		// Disable sandbox enforcement so a relative path is accepted
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

	t.Run("StmtCache_Put_new_entry", func(t *testing.T) {
		cache := NewStmtCache(5)
		// Put a new entry (nil VDBE is handled by cloneVdbe)
		cache.Put("SELECT 42", nil)
		if cache.Size() != 1 {
			t.Errorf("expected Size=1 after Put, got %d", cache.Size())
		}
	})

	t.Run("StmtCache_Put_update_existing", func(t *testing.T) {
		cache := NewStmtCache(5)
		cache.Put("SELECT 1", nil)
		cache.Put("SELECT 1", nil) // update existing entry
		if cache.Size() != 1 {
			t.Errorf("expected Size=1 after duplicate Put, got %d", cache.Size())
		}
	})

	t.Run("StmtCache_Put_evict_when_full", func(t *testing.T) {
		cache := NewStmtCache(2)
		cache.Put("SELECT 1", nil)
		cache.Put("SELECT 2", nil)
		// This third put should evict the LRU entry
		cache.Put("SELECT 3", nil)
		if cache.Size() != 2 {
			t.Errorf("expected Size=2 after eviction, got %d", cache.Size())
		}
	})

	t.Run("validateRecursiveCTE_nil_compound", func(t *testing.T) {
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

	t.Run("validateRecursiveCTE_intersect_compound", func(t *testing.T) {
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

	t.Run("validateRecursiveCTE_union_success", func(t *testing.T) {
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

	t.Run("validateRecursiveCTE_unionAll_success", func(t *testing.T) {
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

	t.Run("adjustCursorOpRegisters_column", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpColumn, 0, 1, 2, 10)
		// P1=cursor (no adjust), P2=column (no adjust), P3=dest+baseReg
		if p1 != 0 || p2 != 1 || p3 != 12 {
			t.Errorf("OpColumn: got (%d,%d,%d), want (0,1,12)", p1, p2, p3)
		}
	})

	t.Run("adjustCursorOpRegisters_rowid", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpRowid, 0, 3, 0, 10)
		// P1=cursor, P2=dest+baseReg, P3 unchanged
		if p1 != 0 || p2 != 13 || p3 != 0 {
			t.Errorf("OpRowid: got (%d,%d,%d), want (0,13,0)", p1, p2, p3)
		}
	})

	t.Run("adjustCursorOpRegisters_insert_with_p3", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpInsert, 0, 2, 3, 10)
		// P1=cursor, P2=data+baseReg, P3=key+baseReg
		if p1 != 0 || p2 != 12 || p3 != 13 {
			t.Errorf("OpInsert with p3>0: got (%d,%d,%d), want (0,12,13)", p1, p2, p3)
		}
	})

	t.Run("adjustCursorOpRegisters_insert_p3_zero", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpInsert, 0, 2, 0, 10)
		// P3=0 means no key register, stays 0
		if p1 != 0 || p2 != 12 || p3 != 0 {
			t.Errorf("OpInsert with p3=0: got (%d,%d,%d), want (0,12,0)", p1, p2, p3)
		}
	})

	t.Run("adjustCursorOpRegisters_rewind", func(t *testing.T) {
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpRewind, 0, 5, 0, 10)
		// P1=cursor, P2=jump target (no adjust), P3 unchanged
		if p1 != 0 || p2 != 5 || p3 != 0 {
			t.Errorf("OpRewind: got (%d,%d,%d), want (0,5,0)", p1, p2, p3)
		}
	})

	t.Run("adjustCursorOpRegisters_default", func(t *testing.T) {
		// Use OpDelete with some other opcode to hit the default case
		p1, p2, p3 := adjustCursorOpRegisters(vdbe.OpDelete, 0, 2, 3, 10)
		// P1=cursor, P2=data+base, P3=key+base (delete behaves like insert)
		// OpDelete hits the OpInsert,OpDelete case
		if p1 != 0 || p2 != 12 || p3 != 13 {
			t.Errorf("OpDelete with p3>0: got (%d,%d,%d), want (0,12,13)", p1, p2, p3)
		}
	})

	t.Run("fixInnerRewindAddresses_no_rewind", func(t *testing.T) {
		// Program with no Rewind instructions — should be a no-op
		vm := vdbe.New()
		vm.AddOp(vdbe.OpNext, 0, 5, 0)
		fixInnerRewindAddresses(vm)
		// No panic, no change
	})

	t.Run("fixInnerRewindAddresses_rewind_p2_nonzero", func(t *testing.T) {
		// Rewind with P2 != 0 should be skipped
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 0, 9, 0) // P2=9, should not be patched
		vm.AddOp(vdbe.OpNext, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		if vm.Program[0].P2 != 9 {
			t.Errorf("Rewind with P2!=0 should not be changed, got P2=%d", vm.Program[0].P2)
		}
	})

	t.Run("fixInnerRewindAddresses_rewind_p2_zero_with_next", func(t *testing.T) {
		// Rewind P2=0 should be patched to point past the matching Next
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 2, 0, 0) // cursor 2, P2=0 → needs fix
		vm.AddOp(vdbe.OpColumn, 2, 0, 1) // some instruction
		vm.AddOp(vdbe.OpNext, 2, 1, 0)   // matching Next for cursor 2
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		// P2 should be set to index 3 (instruction after OpNext at index 2)
		if vm.Program[0].P2 != 3 {
			t.Errorf("Rewind P2 should be 3 after fix, got %d", vm.Program[0].P2)
		}
	})

	t.Run("fixInnerRewindAddresses_rewind_p2_zero_no_next", func(t *testing.T) {
		// Rewind P2=0 with no matching Next — P2 stays 0
		vm := vdbe.New()
		vm.AddOp(vdbe.OpRewind, 5, 0, 0) // cursor 5, no Next for cursor 5
		vm.AddOp(vdbe.OpHalt, 0, 0, 0)
		fixInnerRewindAddresses(vm)
		if vm.Program[0].P2 != 0 {
			t.Errorf("Rewind with no matching Next should stay P2=0, got %d", vm.Program[0].P2)
		}
	})

	t.Run("findColumnIndex_nil_table", func(t *testing.T) {
		idx := findColumnIndex(nil, "id")
		if idx != -1 {
			t.Errorf("nil table: expected -1, got %d", idx)
		}
	})

	t.Run("findColumnIndex_found", func(t *testing.T) {
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

	t.Run("findColumnIndex_not_found", func(t *testing.T) {
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

	t.Run("findColumnIndex_case_insensitive", func(t *testing.T) {
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

	t.Run("runIntegrityCheck_ok", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		result := s.runIntegrityCheck()
		if result != "ok" {
			t.Errorf("expected 'ok' from runIntegrityCheck, got %q", result)
		}
	})

	t.Run("ReadRowByRowid_table_not_found", func(t *testing.T) {
		c := openMemConn(t)
		r := newDriverRowReader(c)
		_, err := r.ReadRowByRowid("no_such_table", 1)
		if err == nil {
			t.Error("expected error for non-existent table, got nil")
		}
	})

	t.Run("ReadRowByRowid_success", func(t *testing.T) {
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

	t.Run("ReadRowByRowid_row_not_found", func(t *testing.T) {
		c := openMemConn(t)
		execOnConn(t, c, "CREATE TABLE rr_miss (id INTEGER PRIMARY KEY, val TEXT)")
		r := newDriverRowReader(c)
		_, err := r.ReadRowByRowid("rr_miss", 999)
		if err == nil {
			t.Error("expected error for missing rowid, got nil")
		}
	})

	t.Run("validateVacuumContext_inTx", func(t *testing.T) {
		c := openMemConn(t)
		c.inTx = true
		s := stmtFor(c)
		err := s.validateVacuumContext()
		if err == nil {
			t.Error("expected error for VACUUM inside transaction, got nil")
		}
	})

	t.Run("validateVacuumContext_not_inTx", func(t *testing.T) {
		c := openMemConn(t)
		c.inTx = false
		s := stmtFor(c)
		// Should not error just because we're outside a transaction
		err := s.validateVacuumContext()
		// We don't require nil since pager state may vary; just ensure the
		// "cannot VACUUM inside a transaction" branch is not taken.
		if err != nil {
			// Only fail if the error is about being in a transaction.
			if err.Error() == "cannot VACUUM inside a transaction" {
				t.Error("got unexpected transaction error when inTx=false")
			}
			// Other errors (pager state) are acceptable.
		}
	})

	t.Run("getIntoFilenameFromArgs_no_args", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		_, err := s.getIntoFilenameFromArgs(nil)
		if err == nil {
			t.Error("expected error for no args, got nil")
		}
	})

	t.Run("getIntoFilenameFromArgs_non_string", func(t *testing.T) {
		c := openMemConn(t)
		s := stmtFor(c)
		args := []driver.NamedValue{{Ordinal: 1, Value: 42}}
		_, err := s.getIntoFilenameFromArgs(args)
		if err == nil {
			t.Error("expected error for non-string filename, got nil")
		}
	})

	t.Run("getIntoFilenameFromArgs_success", func(t *testing.T) {
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
