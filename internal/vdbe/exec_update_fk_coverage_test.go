// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Mock helpers for FK / schema access
// ---------------------------------------------------------------------------

// fkMockSchema implements GetTableByName for tests.
type fkMockSchema struct {
	tables map[string]interface{}
}

func (s *fkMockSchema) GetTableByName(name string) (interface{}, bool) {
	if t, ok := s.tables[name]; ok {
		return t, true
	}
	return nil, false
}

// fkMockTable implements GetColumns, HasRowID, GetPrimaryKey, GetRootPage.
type fkMockTable struct {
	columns    []interface{}
	hasRowID   bool
	primaryKey []string
	rootPage   uint32
}

func (t *fkMockTable) GetColumns() []interface{} { return t.columns }
func (t *fkMockTable) HasRowID() bool            { return t.hasRowID }
func (t *fkMockTable) GetPrimaryKey() []string   { return t.primaryKey }
func (t *fkMockTable) GetRootPage() uint32       { return t.rootPage }

// fkMockFKManager implements ValidateUpdate so getFKManager succeeds.
type fkMockFKManager struct {
	validateUpdateErr error
}

func (m *fkMockFKManager) ValidateUpdate(
	_ string,
	_ map[string]interface{},
	_ map[string]interface{},
	_ interface{},
	_ interface{},
	_ interface{},
) error {
	return m.validateUpdateErr
}

// ---------------------------------------------------------------------------
// compareReals — direct unit tests (accessible because package vdbe)
// ---------------------------------------------------------------------------

// TestCompareReals exercises all three branches of compareReals.
func TestCompareReals(t *testing.T) {
	cases := []struct {
		name string
		a, b float64
		want int
	}{
		{"less", 1.0, 2.0, -1},
		{"greater", 2.0, 1.0, 1},
		{"equal", 3.14, 3.14, 0},
		{"neg_equal", -1.5, -1.5, 0},
		{"zero_less", 0.0, 0.1, -1},
		{"zero_greater", 0.1, 0.0, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := compareReals(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("compareReals(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// shouldValidateUpdate — all branches
// ---------------------------------------------------------------------------

// TestExecUpdateFKShouldValidateUpdate exercises every early-return branch of
// shouldValidateUpdate.
func TestExecUpdateFKShouldValidateUpdate(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if v.shouldValidateUpdate("t") {
			t.Error("expected false for nil ctx")
		}
	})

	t.Run("FKDisabled", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: false}
		if v.shouldValidateUpdate("t") {
			t.Error("expected false when FK disabled")
		}
	})

	t.Run("NilFKManager", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: true, FKManager: nil}
		if v.shouldValidateUpdate("t") {
			t.Error("expected false when FKManager nil")
		}
	})

	t.Run("NilPendingUpdate", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: true, FKManager: struct{}{}}
		v.pendingFKUpdate = nil
		if v.shouldValidateUpdate("t") {
			t.Error("expected false when pendingFKUpdate nil")
		}
	})

	t.Run("TableMismatch", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             struct{}{},
		}
		v.pendingFKUpdate = &fkUpdateContext{table: "other"}
		if v.shouldValidateUpdate("t") {
			t.Error("expected false when table name mismatches")
		}
	})

	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             nil,
		}
		v.pendingFKUpdate = &fkUpdateContext{table: "t"}
		if v.shouldValidateUpdate("t") {
			t.Error("expected false when schema nil")
		}
	})

	t.Run("AllSet", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             &fkMockSchema{},
		}
		v.pendingFKUpdate = &fkUpdateContext{table: "t"}
		if !v.shouldValidateUpdate("t") {
			t.Error("expected true when all conditions met")
		}
	})
}

// ---------------------------------------------------------------------------
// getFKManager — error and success paths
// ---------------------------------------------------------------------------

// TestExecUpdateFKGetFKManager exercises getFKManager for invalid and valid types.
func TestExecUpdateFKGetFKManager(t *testing.T) {
	t.Run("InvalidType", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{FKManager: struct{}{}}
		_, err := v.getFKManager()
		if err == nil {
			t.Error("expected error when FKManager doesn't implement ValidateUpdate")
		}
	})

	t.Run("ValidType", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{FKManager: &fkMockFKManager{}}
		mgr, err := v.getFKManager()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mgr == nil {
			t.Error("expected non-nil FK manager")
		}
	})
}

// ---------------------------------------------------------------------------
// isWithoutRowidTable — all branches
// ---------------------------------------------------------------------------

// TestExecUpdateFKIsWithoutRowidTable covers all code paths.
func TestExecUpdateFKIsWithoutRowidTable(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if v.isWithoutRowidTable("t") {
			t.Error("expected false for nil ctx")
		}
	})

	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		if v.isWithoutRowidTable("t") {
			t.Error("expected false for nil schema")
		}
	})

	t.Run("EmptyTableName", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &fkMockSchema{}}
		if v.isWithoutRowidTable("") {
			t.Error("expected false for empty table name")
		}
	})

	t.Run("SchemaNotImplemented", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: struct{}{}}
		if v.isWithoutRowidTable("t") {
			t.Error("expected false when schema doesn't implement GetTableByName")
		}
	})

	t.Run("TableNotFound", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &fkMockSchema{tables: map[string]interface{}{}}}
		if v.isWithoutRowidTable("missing") {
			t.Error("expected false when table not found")
		}
	})

	t.Run("WithRowID", func(t *testing.T) {
		tbl := &fkMockTable{hasRowID: true}
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
		}
		if v.isWithoutRowidTable("t") {
			t.Error("expected false for table with rowid")
		}
	})

	t.Run("WithoutRowID", func(t *testing.T) {
		tbl := &fkMockTable{hasRowID: false}
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
		}
		if !v.isWithoutRowidTable("t") {
			t.Error("expected true for WITHOUT ROWID table")
		}
	})
}

// ---------------------------------------------------------------------------
// getTablePrimaryKeyOrder — all branches
// ---------------------------------------------------------------------------

// TestExecUpdateFKGetTablePrimaryKeyOrder covers every early-return.
func fkPKOrderExpectNil(t *testing.T, v *VDBE, table string) {
	t.Helper()
	if got := v.getTablePrimaryKeyOrder(table); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestExecUpdateFKGetTablePrimaryKeyOrder(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		fkPKOrderExpectNil(t, v, "t")
	})

	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		fkPKOrderExpectNil(t, v, "t")
	})

	t.Run("SchemaNotImplemented", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: struct{}{}}
		fkPKOrderExpectNil(t, v, "t")
	})

	t.Run("TableNotFound", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &fkMockSchema{tables: map[string]interface{}{}}}
		fkPKOrderExpectNil(t, v, "missing")
	})

	t.Run("TableNoPKMethod", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": struct{}{}}},
		}
		fkPKOrderExpectNil(t, v, "t")
	})

	t.Run("WithPK", func(t *testing.T) {
		tbl := &fkMockTable{primaryKey: []string{"code", "grp"}}
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
		}
		got := v.getTablePrimaryKeyOrder("t")
		if len(got) != 2 || got[0] != "code" || got[1] != "grp" {
			t.Errorf("expected [code grp], got %v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// getTableRootPage — all branches
// ---------------------------------------------------------------------------

// TestExecUpdateFKGetTableRootPage exercises all error and success paths.
func TestExecUpdateFKGetTableRootPage(t *testing.T) {
	t.Run("SchemaNotImplemented", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: struct{}{}}
		_, err := v.getTableRootPage("t")
		if err == nil {
			t.Error("expected error when schema has no GetTableByName")
		}
	})

	t.Run("TableNotFound", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: &fkMockSchema{tables: map[string]interface{}{}}}
		_, err := v.getTableRootPage("missing")
		if err == nil {
			t.Error("expected error for missing table")
		}
	})

	t.Run("TableNoRootPageMethod", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": struct{}{}}},
		}
		_, err := v.getTableRootPage("t")
		if err == nil {
			t.Error("expected error when table lacks GetRootPage")
		}
	})

	t.Run("Success", func(t *testing.T) {
		tbl := &fkMockTable{rootPage: 42}
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
		}
		rp, err := v.getTableRootPage("t")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rp != 42 {
			t.Errorf("expected root page 42, got %d", rp)
		}
	})
}

// ---------------------------------------------------------------------------
// buildPKMap
// ---------------------------------------------------------------------------

// TestExecUpdateFKBuildPKMap exercises buildPKMap for nil/empty and populated cases.
func TestExecUpdateFKBuildPKMap(t *testing.T) {
	v := New()

	t.Run("EmptyCols", func(t *testing.T) {
		result := v.buildPKMap(nil, nil, "t")
		if result != nil {
			t.Errorf("expected nil for empty pkCols, got %v", result)
		}
	})

	t.Run("WithValues", func(t *testing.T) {
		cols := []string{"a", "b"}
		vals := []interface{}{"x", int64(1)}
		result := v.buildPKMap(cols, vals, "t")
		if result == nil {
			t.Fatal("expected non-nil map")
		}
		if result["a"] != "x" || result["b"] != int64(1) {
			t.Errorf("unexpected map contents: %v", result)
		}
	})
}

// ---------------------------------------------------------------------------
// resolvePKOrder
// ---------------------------------------------------------------------------

// TestExecUpdateFKResolvePKOrder exercises resolvePKOrder with and without schema order.
func TestExecUpdateFKResolvePKOrder(t *testing.T) {
	t.Run("FallbackToPKCols", func(t *testing.T) {
		// Schema nil → getTablePrimaryKeyOrder returns nil → fall back to pkCols.
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		pkCols := []string{"x", "y"}
		got := v.resolvePKOrder("t", pkCols)
		if len(got) != 2 || got[0] != "x" || got[1] != "y" {
			t.Errorf("expected fallback to pkCols, got %v", got)
		}
	})

	t.Run("UsesSchemaOrder", func(t *testing.T) {
		tbl := &fkMockTable{primaryKey: []string{"b", "a"}}
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
		}
		got := v.resolvePKOrder("t", []string{"a", "b"})
		if len(got) != 2 || got[0] != "b" || got[1] != "a" {
			t.Errorf("expected schema order [b a], got %v", got)
		}
	})
}

// ---------------------------------------------------------------------------
// orderAndValidatePKValues
// ---------------------------------------------------------------------------

// TestExecUpdateFKOrderAndValidatePKValues exercises orderAndValidatePKValues.
func TestExecUpdateFKOrderAndValidatePKValues(t *testing.T) {
	v := New()

	t.Run("MissingColumn", func(t *testing.T) {
		pkMap := map[string]interface{}{"a": "x"}
		_, err := v.orderAndValidatePKValues(pkMap, []string{"a", "missing"})
		if err == nil {
			t.Error("expected error for missing column")
		}
	})

	t.Run("NullValue", func(t *testing.T) {
		pkMap := map[string]interface{}{"a": nil}
		_, err := v.orderAndValidatePKValues(pkMap, []string{"a"})
		if err == nil {
			t.Error("expected error for NULL primary key value")
		}
	})

	t.Run("Success", func(t *testing.T) {
		pkMap := map[string]interface{}{"a": "hello", "b": int64(42)}
		vals, err := v.orderAndValidatePKValues(pkMap, []string{"b", "a"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(vals) != 2 || vals[0] != int64(42) || vals[1] != "hello" {
			t.Errorf("unexpected ordered values: %v", vals)
		}
	})
}

// ---------------------------------------------------------------------------
// restorePendingUpdate — nil / non-btree paths
// ---------------------------------------------------------------------------

// TestExecUpdateFKRestorePendingUpdate exercises the early-return paths.
func TestExecUpdateFKRestorePendingUpdate(t *testing.T) {
	t.Run("NilPendingUpdate", func(t *testing.T) {
		v := New()
		v.pendingFKUpdate = nil
		// Must not panic.
		v.restorePendingUpdate()
	})

	t.Run("NilBtree", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Btree: nil}
		v.pendingFKUpdate = &fkUpdateContext{
			table:    "t",
			rowid:    1,
			payload:  []byte{0x02, 0x09},
			rootPage: 1,
		}
		// Must not panic (btree type assertion fails → returns early).
		v.restorePendingUpdate()
		if v.pendingFKUpdate != nil {
			t.Error("expected pendingFKUpdate to be cleared")
		}
	})
}

// ---------------------------------------------------------------------------
// checkPKUniquenessIfChanged — rowid table (no-op path)
// ---------------------------------------------------------------------------

// TestExecUpdateFKCheckPKUniquenessIfChanged exercises the non-WITHOUT-ROWID path.
func TestExecUpdateFKCheckPKUniquenessIfChanged(t *testing.T) {
	// For a rowid table isWithoutRowidTable returns false → returns nil immediately.
	tbl := &fkMockTable{hasRowID: true}
	v := New()
	v.Ctx = &VDBEContext{
		Schema: &fkMockSchema{tables: map[string]interface{}{"t": tbl}},
	}
	v.pendingFKUpdate = &fkUpdateContext{
		table:     "t",
		oldValues: map[string]interface{}{"id": int64(1)},
	}
	err := v.checkPKUniquenessIfChanged("t", []byte{0x01, 0x09}, map[string]interface{}{"id": int64(2)})
	if err != nil {
		t.Errorf("expected nil for rowid table, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// checkForeignKeyConstraintsWithoutRowID — early returns
// ---------------------------------------------------------------------------

// TestExecUpdateFKCheckFKWithoutRowIDEarlyReturns exercises guard clauses.
func TestExecUpdateFKCheckFKWithoutRowIDEarlyReturns(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if err := v.checkForeignKeyConstraintsWithoutRowID("t", []byte{}); err != nil {
			t.Errorf("expected nil for nil ctx, got: %v", err)
		}
	})

	t.Run("FKDisabled", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: false}
		if err := v.checkForeignKeyConstraintsWithoutRowID("t", []byte{}); err != nil {
			t.Errorf("expected nil when FK disabled, got: %v", err)
		}
	})

	t.Run("NilFKManager", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: true, FKManager: nil}
		if err := v.checkForeignKeyConstraintsWithoutRowID("t", []byte{}); err != nil {
			t.Errorf("expected nil when FKManager nil, got: %v", err)
		}
	})

	t.Run("FKManagerBadType", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: true, FKManager: struct{}{}, Schema: nil}
		if err := v.checkForeignKeyConstraintsWithoutRowID("t", []byte{}); err != nil {
			t.Errorf("expected nil when FKManager wrong type, got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// checkForeignKeyDeleteConstraints — early returns
// ---------------------------------------------------------------------------

// TestExecUpdateFKCheckFKDeleteEarlyReturns exercises guard clauses.
func TestExecUpdateFKCheckFKDeleteEarlyReturns(t *testing.T) {
	t.Run("NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if err := v.checkForeignKeyDeleteConstraints("t", map[string]interface{}{}); err != nil {
			t.Errorf("expected nil for nil ctx, got: %v", err)
		}
	})

	t.Run("FKDisabled", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: false}
		if err := v.checkForeignKeyDeleteConstraints("t", map[string]interface{}{}); err != nil {
			t.Errorf("expected nil when FK disabled, got: %v", err)
		}
	})

	t.Run("NilFKManager", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{ForeignKeysEnabled: true, FKManager: nil}
		if err := v.checkForeignKeyDeleteConstraints("t", map[string]interface{}{}); err != nil {
			t.Errorf("expected nil when FKManager nil, got: %v", err)
		}
	})

	t.Run("FKManagerBadType", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             &fkMockSchema{},
		}
		if err := v.checkForeignKeyDeleteConstraints("t", map[string]interface{}{}); err != nil {
			t.Errorf("expected nil when FKManager has no ValidateDelete, got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// validateUpdateConstraintsWithRowid / validateUpdateConstraints — early returns
// ---------------------------------------------------------------------------

// TestExecUpdateFKValidateUpdateConstraintsEarlyReturn exercises the
// shouldValidateUpdate=false path and the getFKManager failure path.
func TestExecUpdateFKValidateUpdateConstraintsEarlyReturn(t *testing.T) {
	t.Run("WithRowid_NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if err := v.validateUpdateConstraintsWithRowid("t", []byte{0x01, 0x09}, 1); err != nil {
			t.Errorf("expected nil when no validation needed, got: %v", err)
		}
	})

	t.Run("WithRowid_FKManagerBadType", func(t *testing.T) {
		// shouldValidateUpdate=true but getFKManager fails → returns nil.
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             &fkMockSchema{},
		}
		v.pendingFKUpdate = &fkUpdateContext{
			table:     "t",
			oldValues: map[string]interface{}{},
		}
		if err := v.validateUpdateConstraintsWithRowid("t", []byte{0x01, 0x09}, 1); err != nil {
			t.Errorf("expected nil when getFKManager fails silently, got: %v", err)
		}
	})

	t.Run("WithoutRowid_NilCtx", func(t *testing.T) {
		v := New()
		v.Ctx = nil
		if err := v.validateUpdateConstraints("t", []byte{0x01, 0x09}); err != nil {
			t.Errorf("expected nil when no validation needed, got: %v", err)
		}
	})

	t.Run("WithoutRowid_FKManagerBadType", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			ForeignKeysEnabled: true,
			FKManager:          struct{}{},
			Schema:             &fkMockSchema{},
		}
		v.pendingFKUpdate = &fkUpdateContext{
			table:     "t",
			oldValues: map[string]interface{}{},
		}
		if err := v.validateUpdateConstraints("t", []byte{0x01, 0x09}); err != nil {
			t.Errorf("expected nil when getFKManager fails silently, got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// extractPKColumns — error paths
// ---------------------------------------------------------------------------

// TestExecUpdateFKExtractPKColumnsErrors exercises extractPKColumns error paths.
func TestExecUpdateFKExtractPKColumnsErrors(t *testing.T) {
	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		_, _, err := v.extractPKColumns("t", []byte{})
		if err == nil {
			t.Error("expected error when schema nil")
		}
	})

	t.Run("TableNotFound", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{}},
		}
		_, _, err := v.extractPKColumns("missing", []byte{})
		if err == nil {
			t.Error("expected error for missing table")
		}
	})

	t.Run("TableNoPKMethod", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{"t": struct{}{}}},
		}
		_, _, err := v.extractPKColumns("t", []byte{})
		if err == nil {
			t.Error("expected error when table lacks GetPrimaryKey")
		}
	})
}

// ---------------------------------------------------------------------------
// computeCompositeKeyBytes — early error paths
// ---------------------------------------------------------------------------

// TestExecUpdateFKComputeCompositeKeyBytesErrors exercises early error paths.
func TestExecUpdateFKComputeCompositeKeyBytesErrors(t *testing.T) {
	t.Run("NilSchema", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{Schema: nil}
		_, err := v.computeCompositeKeyBytes("t", []byte{})
		if err == nil {
			t.Error("expected error when schema nil")
		}
	})

	t.Run("TableNotFound", func(t *testing.T) {
		v := New()
		v.Ctx = &VDBEContext{
			Schema: &fkMockSchema{tables: map[string]interface{}{}},
		}
		_, err := v.computeCompositeKeyBytes("missing", []byte{})
		if err == nil {
			t.Error("expected error for missing table")
		}
	})
}
