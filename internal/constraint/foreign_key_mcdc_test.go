// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"strings"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// MC/DC helpers
// ---------------------------------------------------------------------------

func mcdcSchema() *schema.Schema {
	sch := schema.NewSchema()
	sch.Tables["parent"] = &schema.Table{
		Name:       "parent",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Tables["child"] = &schema.Table{
		Name: "child",
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "parent_id", Type: "INTEGER"},
		},
		PrimaryKey: []string{"id"},
	}
	return sch
}

func mcdcFK() *ForeignKeyConstraint {
	return &ForeignKeyConstraint{
		Table:      "child",
		Columns:    []string{"parent_id"},
		RefTable:   "parent",
		RefColumns: []string{"id"},
	}
}

// ---------------------------------------------------------------------------
// Condition: extractForeignKeyValues — `!ok || val == nil`
//
//   A = column key not present in map (!ok)
//   B = value is nil
//
//   hasNull is true when A || B is true.
//
//   Case 1 (A=T, B=–): A alone makes outcome true   → hasNull = true
//   Case 2 (A=F, B=T): B alone makes outcome true   → hasNull = true
//   Case 3 (A=F, B=F): both false                   → hasNull = false
// ---------------------------------------------------------------------------

func TestMCDC_extractForeignKeyValues_MissingKey(t *testing.T) {
	// Case 1: A=true (!ok) — key absent → hasNull must be true
	values := map[string]interface{}{"other_col": 1}
	_, hasNull := extractForeignKeyValues(values, []string{"parent_id"})
	if !hasNull {
		t.Error("MCDC case1: missing key must set hasNull=true")
	}
}

func TestMCDC_extractForeignKeyValues_NilValue(t *testing.T) {
	// Case 2: A=false, B=true (val==nil) → hasNull must be true
	values := map[string]interface{}{"parent_id": nil}
	_, hasNull := extractForeignKeyValues(values, []string{"parent_id"})
	if !hasNull {
		t.Error("MCDC case2: nil value must set hasNull=true")
	}
}

func TestMCDC_extractForeignKeyValues_PresentNonNil(t *testing.T) {
	// Case 3: A=false, B=false → hasNull must be false
	values := map[string]interface{}{"parent_id": 42}
	_, hasNull := extractForeignKeyValues(values, []string{"parent_id"})
	if hasNull {
		t.Error("MCDC case3: present non-nil value must set hasNull=false")
	}
}

// ---------------------------------------------------------------------------
// Condition: validateInsertConstraint — deferred branch
//   `fk.Deferrable == DeferrableInitiallyDeferred && m.IsInTransaction()`
//
//   A = fk.Deferrable == DeferrableInitiallyDeferred
//   B = m.IsInTransaction()
//
//   defer happens only when A && B = true.
//
//   Case 1 (A=T, B=T): deferred → no immediate validation error
//   Case 2 (A=T, B=F): A alone cannot defer → immediate validation happens
//   Case 3 (A=F, B=T): B alone cannot defer → immediate validation happens
// ---------------------------------------------------------------------------

func TestMCDC_ValidateInsert_DeferBothTrue(t *testing.T) {
	// Case 1: A=T, B=T → constraint deferred, no error even without parent row
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)

	sch := mcdcSchema()
	fk := mcdcFK()
	fk.Deferrable = DeferrableInitiallyDeferred
	mgr.AddConstraint(fk)

	reader := NewMockRowReader() // empty, parent id=1 does not exist

	err := mgr.ValidateInsert("child", map[string]interface{}{"id": 1, "parent_id": 1}, sch, reader)
	if err != nil {
		t.Errorf("MCDC case1: deferred insert should not error; got %v", err)
	}
}

func TestMCDC_ValidateInsert_DeferrableButNoTransaction(t *testing.T) {
	// Case 2: A=T, B=F → not deferred, immediate check fires, parent missing → error
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(false)

	sch := mcdcSchema()
	fk := mcdcFK()
	fk.Deferrable = DeferrableInitiallyDeferred
	mgr.AddConstraint(fk)

	reader := NewMockRowReader() // empty

	err := mgr.ValidateInsert("child", map[string]interface{}{"id": 1, "parent_id": 1}, sch, reader)
	if err == nil {
		t.Error("MCDC case2: no-transaction deferrable must still validate immediately")
	}
}

func TestMCDC_ValidateInsert_NotDeferrableInTransaction(t *testing.T) {
	// Case 3: A=F, B=T → not deferred, immediate check fires, parent missing → error
	mgr := NewForeignKeyManager()
	mgr.SetEnabled(true)
	mgr.SetInTransaction(true)

	sch := mcdcSchema()
	fk := mcdcFK()
	fk.Deferrable = DeferrableNone
	mgr.AddConstraint(fk)

	reader := NewMockRowReader() // empty

	err := mgr.ValidateInsert("child", map[string]interface{}{"id": 1, "parent_id": 1}, sch, reader)
	if err == nil {
		t.Error("MCDC case3: non-deferrable in-transaction must validate immediately")
	}
}

// ---------------------------------------------------------------------------
// Condition: shouldDeferDeleteViolation — first guard
//   `fk.Deferrable != DeferrableInitiallyDeferred || !m.IsInTransaction()`
//   (early-return false = no deferral)
//
//   A = fk.Deferrable != DeferrableInitiallyDeferred
//   B = !m.IsInTransaction()
//
//   Returns false (no deferral) when A || B is true.
//
//   Case 1 (A=T, B=–): A alone forces no-deferral
//   Case 2 (A=F, B=T): B alone forces no-deferral
//   Case 3 (A=F, B=F): neither → proceed to inner check (action check)
// ---------------------------------------------------------------------------

func TestMCDC_ShouldDeferDeleteViolation_NotDeferrable(t *testing.T) {
	// Case 1: A=T → fk is not DeferrableInitiallyDeferred → returns false
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Deferrable: DeferrableNone,
		OnDelete:   FKActionNone,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if result {
		t.Error("MCDC case1: non-deferrable FK must not defer delete violation")
	}
}

func TestMCDC_ShouldDeferDeleteViolation_DeferrableOutsideTransaction(t *testing.T) {
	// Case 2: A=F, B=T → deferrable but no transaction → returns false
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(false)
	fk := &ForeignKeyConstraint{
		Deferrable: DeferrableInitiallyDeferred,
		OnDelete:   FKActionNone,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if result {
		t.Error("MCDC case2: deferrable outside transaction must not defer")
	}
}

func TestMCDC_ShouldDeferDeleteViolation_DeferrableInTransactionNoAction(t *testing.T) {
	// Case 3: A=F, B=F → fully deferred; OnDelete=FKActionNone passes inner check → returns true
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		Deferrable: DeferrableInitiallyDeferred,
		OnDelete:   FKActionNone,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if !result {
		t.Error("MCDC case3: deferrable in-transaction with FKActionNone must defer")
	}
}

// ---------------------------------------------------------------------------
// Condition: shouldDeferDeleteViolation — inner action guard
//   `action != FKActionNone && action != FKActionNoAction`
//   (when true → NOT deferrable for cascade/set-null actions)
//
//   A = action != FKActionNone
//   B = action != FKActionNoAction
//
//   Returns false from deferral when A && B is true.
//
//   Case 1 (A=T, B=T): action=Cascade → should NOT defer
//   Case 2 (A=F, B=–): action=FKActionNone → A is false → defers
//   Case 3 (A=T, B=F): action=FKActionNoAction → B is false → defers
// ---------------------------------------------------------------------------

func TestMCDC_ShouldDeferDeleteViolation_ActionCascade(t *testing.T) {
	// Case 1: action=Cascade → A=T, B=T → inner check prevents deferral → returns false
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		Deferrable: DeferrableInitiallyDeferred,
		OnDelete:   FKActionCascade,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if result {
		t.Error("MCDC case1: Cascade action must not defer delete violation")
	}
}

func TestMCDC_ShouldDeferDeleteViolation_ActionNone(t *testing.T) {
	// Case 2: action=FKActionNone → A=F → defers → returns true
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		Deferrable: DeferrableInitiallyDeferred,
		OnDelete:   FKActionNone,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if !result {
		t.Error("MCDC case2: FKActionNone must defer delete violation")
	}
}

func TestMCDC_ShouldDeferDeleteViolation_ActionNoAction(t *testing.T) {
	// Case 3: action=FKActionNoAction → B=F → defers → returns true
	mgr := NewForeignKeyManager()
	mgr.SetInTransaction(true)
	fk := &ForeignKeyConstraint{
		Table:      "child",
		Deferrable: DeferrableInitiallyDeferred,
		OnDelete:   FKActionNoAction,
	}
	result := mgr.shouldDeferDeleteViolation(fk, []interface{}{1})
	if !result {
		t.Error("MCDC case3: FKActionNoAction must defer delete violation")
	}
}

// ---------------------------------------------------------------------------
// Condition: hasRealAffinity
//   `Contains(upper,"REAL") || Contains(upper,"FLOA") || Contains(upper,"DOUB")`
//
//   A = Contains REAL
//   B = Contains FLOA
//   C = Contains DOUB
//
//   N=3 sub-conditions → need 4 test cases.
//
//   Case 1 (A=T): "REAL"    → true
//   Case 2 (A=F, B=T): "FLOAT"  → true
//   Case 3 (A=F, B=F, C=T): "DOUBLE" → true
//   Case 4 (A=F, B=F, C=F): "TEXT"   → false
// ---------------------------------------------------------------------------

func TestMCDC_hasRealAffinity_Real(t *testing.T) {
	if !hasRealAffinity("REAL") {
		t.Error("MCDC case1: REAL must be real affinity")
	}
}

func TestMCDC_hasRealAffinity_Float(t *testing.T) {
	if !hasRealAffinity("FLOAT") {
		t.Error("MCDC case2: FLOAT must be real affinity")
	}
}

func TestMCDC_hasRealAffinity_Double(t *testing.T) {
	if !hasRealAffinity("DOUBLE") {
		t.Error("MCDC case3: DOUBLE must be real affinity")
	}
}

func TestMCDC_hasRealAffinity_Text(t *testing.T) {
	if hasRealAffinity("TEXT") {
		t.Error("MCDC case4: TEXT must NOT be real affinity")
	}
}

// ---------------------------------------------------------------------------
// Condition: hasNumericAffinity
//   `Contains(upper,"NUM") || Contains(upper,"DEC")`
//
//   A = Contains NUM
//   B = Contains DEC
//
//   Case 1 (A=T): "NUMERIC"  → true
//   Case 2 (A=F, B=T): "DECIMAL" → true
//   Case 3 (A=F, B=F): "TEXT"    → false
// ---------------------------------------------------------------------------

func TestMCDC_hasNumericAffinity_Numeric(t *testing.T) {
	if !hasNumericAffinity("NUMERIC") {
		t.Error("MCDC case1: NUMERIC must be numeric affinity")
	}
}

func TestMCDC_hasNumericAffinity_Decimal(t *testing.T) {
	if !hasNumericAffinity("DECIMAL") {
		t.Error("MCDC case2: DECIMAL must be numeric affinity")
	}
}

func TestMCDC_hasNumericAffinity_Text(t *testing.T) {
	if hasNumericAffinity("TEXT") {
		t.Error("MCDC case3: TEXT must NOT be numeric affinity")
	}
}

// ---------------------------------------------------------------------------
// Condition: hasUniqueIndex
//   `idx.Unique && strings.EqualFold(idx.Table, tableName) && columnsMatchUnordered(...)`
//
//   A = idx.Unique
//   B = EqualFold(idx.Table, tableName)
//   C = columnsMatchUnordered(...)
//
//   N=3 → need 4 cases.
//
//   Case 1 (A=F): non-unique index                  → false
//   Case 2 (A=T, B=F): unique, wrong table          → false
//   Case 3 (A=T, B=T, C=F): unique, right table, wrong columns → false
//   Case 4 (A=T, B=T, C=T): all true               → true
// ---------------------------------------------------------------------------

func buildSchemaWithIndex(unique bool, idxTable, idxCol string) *schema.Schema {
	sch := schema.NewSchema()
	sch.Tables["parent"] = &schema.Table{
		Name:       "parent",
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	}
	sch.Indexes["idx_test"] = &schema.Index{
		Name:    "idx_test",
		Table:   idxTable,
		Columns: []string{idxCol},
		Unique:  unique,
	}
	return sch
}

func TestMCDC_hasUniqueIndex_NotUnique(t *testing.T) {
	// Case 1: A=F (non-unique) → false
	sch := buildSchemaWithIndex(false, "parent", "id")
	result := hasUniqueIndex(sch, "parent", []string{"id"})
	if result {
		t.Error("MCDC case1: non-unique index must return false")
	}
}

func TestMCDC_hasUniqueIndex_WrongTable(t *testing.T) {
	// Case 2: A=T, B=F (wrong table) → false
	sch := buildSchemaWithIndex(true, "other_table", "id")
	result := hasUniqueIndex(sch, "parent", []string{"id"})
	if result {
		t.Error("MCDC case2: unique index on different table must return false")
	}
}

func TestMCDC_hasUniqueIndex_WrongColumns(t *testing.T) {
	// Case 3: A=T, B=T, C=F (wrong columns) → false
	sch := buildSchemaWithIndex(true, "parent", "other_col")
	result := hasUniqueIndex(sch, "parent", []string{"id"})
	if result {
		t.Error("MCDC case3: unique index with wrong columns must return false")
	}
}

func TestMCDC_hasUniqueIndex_AllMatch(t *testing.T) {
	// Case 4: A=T, B=T, C=T → true
	sch := buildSchemaWithIndex(true, "parent", "id")
	result := hasUniqueIndex(sch, "parent", []string{"id"})
	if !result {
		t.Error("MCDC case4: matching unique index must return true")
	}
}

// ---------------------------------------------------------------------------
// Condition: filterDeleteSelfRef
//   `strings.EqualFold(fk.Table, fk.RefTable) && selfReferenceMatches(...)`
//
//   A = fk.Table == fk.RefTable (case-insensitive)
//   B = selfReferenceMatches(...)
//
//   Filter only applied when A && B is true.
//
//   Case 1 (A=F): different tables → no filter applied, rows unchanged
//   Case 2 (A=T, B=F): self-referential table, but values don't match → no filter
//   Case 3 (A=T, B=T): self-referential and values match → filter applied
// ---------------------------------------------------------------------------

func TestMCDC_filterDeleteSelfRef_DifferentTables(t *testing.T) {
	// Case 1: A=F → rows returned unchanged
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:    "child",
		RefTable: "parent",
		Columns:  []string{"parent_id"},
	}
	rows := []int64{1, 2, 3}
	values := map[string]interface{}{"id": int64(1), "parent_id": int64(1)}
	result := mgr.filterDeleteSelfRef(fk, rows, values, []string{"id"})
	if len(result) != 3 {
		t.Errorf("MCDC case1: different tables must not filter; got %d rows", len(result))
	}
}

func TestMCDC_filterDeleteSelfRef_SelfRefNoMatch(t *testing.T) {
	// Case 2: A=T, B=F → self-referential but PK != FK value → no filter
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "node",
		RefTable:   "node",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
	}
	// Row being deleted has id=1, parent_id=99 → FK value doesn't match PK
	rows := []int64{1, 2}
	values := map[string]interface{}{"id": int64(1), "parent_id": int64(99)}
	result := mgr.filterDeleteSelfRef(fk, rows, values, []string{"id"})
	if len(result) != 2 {
		t.Errorf("MCDC case2: self-ref no-match must not filter; got %d rows", len(result))
	}
}

func TestMCDC_filterDeleteSelfRef_SelfRefMatches(t *testing.T) {
	// Case 3: A=T, B=T → self-referential, values match → filter removes self-reference
	mgr := NewForeignKeyManager()
	fk := &ForeignKeyConstraint{
		Table:      "node",
		RefTable:   "node",
		Columns:    []string{"parent_id"},
		RefColumns: []string{"id"},
	}
	// Row being deleted has id=1, parent_id=1 → references itself
	rows := []int64{1, 2}
	values := map[string]interface{}{"id": int64(1), "parent_id": int64(1)}
	result := mgr.filterDeleteSelfRef(fk, rows, values, []string{"id"})
	// Row 1 should be filtered out (it's the self-reference)
	for _, r := range result {
		if r == 1 {
			t.Error("MCDC case3: self-referencing row must be filtered out")
		}
	}
}

// ---------------------------------------------------------------------------
// Condition: columnsAreUnique — primary key check
//   `len(columns) == len(table.PrimaryKey) && columnsMatch(columns, table.PrimaryKey)`
//
//   A = len(columns) == len(table.PrimaryKey)
//   B = columnsMatch(columns, table.PrimaryKey)
//
//   Case 1 (A=F): different lengths → false (skip to other checks)
//   Case 2 (A=T, B=F): same length, different names → false
//   Case 3 (A=T, B=T): matches PK → true
// ---------------------------------------------------------------------------

func TestMCDC_columnsAreUnique_PKLenMismatch(t *testing.T) {
	// Case 1: A=F (length mismatch)
	tbl := &schema.Table{
		Name:       "t",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "name", Type: "TEXT"}},
	}
	sch := schema.NewSchema()
	sch.Tables["t"] = tbl
	// columns has 2 elements vs PK has 1 → no PK match
	result := columnsAreUnique(tbl, []string{"id", "name"}, sch)
	if result {
		t.Error("MCDC case1: length mismatch must not match PK uniqueness")
	}
}

func TestMCDC_columnsAreUnique_PKLenMatchButDifferentCol(t *testing.T) {
	// Case 2: A=T (same len), B=F (different column name)
	tbl := &schema.Table{
		Name:       "t",
		PrimaryKey: []string{"id"},
		Columns: []*schema.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "other", Type: "TEXT"},
		},
	}
	sch := schema.NewSchema()
	sch.Tables["t"] = tbl
	result := columnsAreUnique(tbl, []string{"other"}, sch)
	if result {
		t.Error("MCDC case2: same len but wrong column must not match PK")
	}
}

func TestMCDC_columnsAreUnique_PKMatch(t *testing.T) {
	// Case 3: A=T, B=T → exact PK match → true
	tbl := &schema.Table{
		Name:       "t",
		PrimaryKey: []string{"id"},
		Columns:    []*schema.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	sch := schema.NewSchema()
	sch.Tables["t"] = tbl
	result := columnsAreUnique(tbl, []string{"id"}, sch)
	if !result {
		t.Error("MCDC case3: PK column must be unique")
	}
}

// ---------------------------------------------------------------------------
// Condition: applyAffinityToDefault — INT branch
//   `strings.Contains(upper, "INT")`  (first branch, single sub-condition)
//
//   Case 1: "INTEGER" → parse as int
//   Case 2: "TEXT"    → no int parse, return original string
// ---------------------------------------------------------------------------

func TestMCDC_applyAffinityToDefault_IntegerType(t *testing.T) {
	// A=T: contains INT
	result := applyAffinityToDefault("42", "INTEGER")
	if _, ok := result.(int64); !ok {
		t.Errorf("MCDC case1: INTEGER type with '42' must return int64, got %T", result)
	}
}

func TestMCDC_applyAffinityToDefault_TextType(t *testing.T) {
	// A=F: does not contain INT
	result := applyAffinityToDefault("hello", "TEXT")
	s, ok := result.(string)
	if !ok || !strings.HasPrefix(s, "hello") {
		t.Errorf("MCDC case2: TEXT type must return original string, got %v", result)
	}
}
