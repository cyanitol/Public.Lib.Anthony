// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/btree"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// ---------------------------------------------------------------------------
// Condition: ShouldApplyDefault — `!valueProvided`
//   (single sub-condition, first guard branch)
//
//   A = !valueProvided  (no value was given at all)
//
//   Case 1 (A=T): value not provided → always apply default
//   Case 2 (A=F): value provided → falls through to next check
// ---------------------------------------------------------------------------

func TestMCDC_ShouldApplyDefault_NotProvided(t *testing.T) {
	// Case 1: A=T — value not provided → true
	if !ShouldApplyDefault(false, false, true) {
		t.Error("MCDC case1: !valueProvided must return true")
	}
}

func TestMCDC_ShouldApplyDefault_ProvidedNonNull(t *testing.T) {
	// Case 2: A=F — value provided, non-null, allowsNull → false
	if ShouldApplyDefault(true, false, true) {
		t.Error("MCDC case2: provided non-null value must return false")
	}
}

// ---------------------------------------------------------------------------
// Condition: ShouldApplyDefault — `valueIsNull && !columnAllowsNull`
//   (second branch, reached when valueProvided == true)
//
//   A = valueIsNull
//   B = !columnAllowsNull
//
//   Returns true when A && B is true.
//
//   Case 1 (A=T, B=T): NULL in NOT NULL column → apply default
//   Case 2 (A=T, B=F): NULL in nullable column → do NOT apply default
//   Case 3 (A=F, B=T): non-null in NOT NULL column → do NOT apply default
// ---------------------------------------------------------------------------

func TestMCDC_ShouldApplyDefault_NullNotNullColumn(t *testing.T) {
	// Case 1: A=T, B=T → true
	if !ShouldApplyDefault(true, true, false) {
		t.Error("MCDC case1: NULL in NOT NULL column must apply default")
	}
}

func TestMCDC_ShouldApplyDefault_NullNullableColumn(t *testing.T) {
	// Case 2: A=T, B=F → false
	if ShouldApplyDefault(true, true, true) {
		t.Error("MCDC case2: NULL in nullable column must NOT apply default")
	}
}

func TestMCDC_ShouldApplyDefault_NonNullNotNullColumn(t *testing.T) {
	// Case 3: A=F, B=T → false
	if ShouldApplyDefault(true, false, false) {
		t.Error("MCDC case3: non-null value in NOT NULL column must NOT apply default")
	}
}

// ---------------------------------------------------------------------------
// Condition: parseStringValue — quote-stripping guard
//   `len(s) >= 2 && (s[0] == '\'' || s[0] == '"')`
//
//   A = len(s) >= 2
//   B = s[0] == '\'' || s[0] == '"'
//
//   Stripping only happens when A && B is true.
//
//   Case 1 (A=F): empty/1-char string → no stripping
//   Case 2 (A=T, B=F): len >= 2, first char not quote → no stripping
//   Case 3 (A=T, B=T): quoted string → stripped
// ---------------------------------------------------------------------------

func TestMCDC_parseStringValue_TooShort(t *testing.T) {
	// Case 1: A=F — single char, can't be quoted
	result := parseStringValue("x")
	if result != "x" {
		t.Errorf("MCDC case1: single-char must be unchanged, got %q", result)
	}
}

func TestMCDC_parseStringValue_NoQuote(t *testing.T) {
	// Case 2: A=T, B=F — len>=2 but starts with non-quote
	result := parseStringValue("ab")
	if result != "ab" {
		t.Errorf("MCDC case2: unquoted string must be unchanged, got %q", result)
	}
}

func TestMCDC_parseStringValue_SingleQuoted(t *testing.T) {
	// Case 3a: A=T, B=T (single quote) → stripped
	result := parseStringValue("'hello'")
	if result != "hello" {
		t.Errorf("MCDC case3a: single-quoted string must be stripped, got %q", result)
	}
}

func TestMCDC_parseStringValue_DoubleQuoted(t *testing.T) {
	// Case 3b: A=T, B=T (double quote) → stripped
	result := parseStringValue(`"world"`)
	if result != "world" {
		t.Errorf("MCDC case3b: double-quoted string must be stripped, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// Condition: valuesEqual — `bothNil(v1, v2)`
//   (single sub-condition; if both nil → return true immediately)
//
//   Case 1 (A=T): v1=nil, v2=nil → true
//   Case 2 (A=F): at least one non-nil → falls through
// ---------------------------------------------------------------------------

func TestMCDC_valuesEqual_BothNil(t *testing.T) {
	// Case 1: A=T → true
	if !valuesEqual(nil, nil) {
		t.Error("MCDC case1: both nil must be equal")
	}
}

func TestMCDC_valuesEqual_OneNil(t *testing.T) {
	// Case 2: A=F → falls to eitherNil check → false
	if valuesEqual(nil, 42) {
		t.Error("MCDC case2: nil vs non-nil must not be equal")
	}
}

// ---------------------------------------------------------------------------
// Condition: valuesEqual — `eitherNil(v1, v2)`
//   (reached when bothNil is false; single sub-condition, A || B)
//
//   A = v1 == nil
//   B = v2 == nil
//
//   Case 1 (A=T): v1 nil, v2 non-nil → false (not equal)
//   Case 2 (A=F, B=T): v1 non-nil, v2 nil → false
//   Case 3 (A=F, B=F): both non-nil → proceed to comparison
// ---------------------------------------------------------------------------

func TestMCDC_valuesEqual_V1Nil(t *testing.T) {
	// Case 1: A=T → false
	if valuesEqual(nil, "x") {
		t.Error("MCDC case1: nil v1 vs non-nil v2 must not be equal")
	}
}

func TestMCDC_valuesEqual_V2Nil(t *testing.T) {
	// Case 2: A=F, B=T → false
	if valuesEqual("x", nil) {
		t.Error("MCDC case2: non-nil v1 vs nil v2 must not be equal")
	}
}

func TestMCDC_valuesEqual_BothNonNil(t *testing.T) {
	// Case 3: A=F, B=F → compare typed → equal
	if !valuesEqual("hello", "hello") {
		t.Error("MCDC case3: equal non-nil strings must be equal")
	}
}

// ---------------------------------------------------------------------------
// Condition: compareBytes — `len(a) != len(b)`
//   (early-return false when lengths differ)
//
//   A = len(a) != len(b)
//
//   Case 1 (A=T): different lengths → false
//   Case 2 (A=F): same length → compare element-wise
// ---------------------------------------------------------------------------

func TestMCDC_compareBytes_LengthMismatch(t *testing.T) {
	// Case 1: A=T → false immediately
	if compareBytes([]byte{1, 2}, []byte{1}) {
		t.Error("MCDC case1: different-length slices must not be equal")
	}
}

func TestMCDC_compareBytes_SameLengthEqual(t *testing.T) {
	// Case 2: A=F, bytes match → true
	if !compareBytes([]byte{1, 2, 3}, []byte{1, 2, 3}) {
		t.Error("MCDC case2: identical byte slices must be equal")
	}
}

func TestMCDC_compareBytes_SameLengthNotEqual(t *testing.T) {
	// Case 2b: A=F, bytes differ → false
	if compareBytes([]byte{1, 2, 3}, []byte{1, 2, 4}) {
		t.Error("MCDC case2b: byte slices with differing content must not be equal")
	}
}

// ---------------------------------------------------------------------------
// Condition: NotNullConstraint.ValidateInsert — `!exists`
//   (first NULL check: column value not in map)
//
//   A = !exists  (column not present in values map)
//
//   Case 1 (A=T): column absent → NOT NULL violation
//   Case 2 (A=F): column present → falls through to nil check
// ---------------------------------------------------------------------------

func TestMCDC_NotNullValidateInsert_Missing(t *testing.T) {
	// Case 1: A=T — column not in map → error
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateInsert(map[string]interface{}{})
	if err == nil {
		t.Error("MCDC case1: missing NOT NULL column must return error")
	}
}

func TestMCDC_NotNullValidateInsert_Present(t *testing.T) {
	// Case 2: A=F — column present, non-nil → no error
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateInsert(map[string]interface{}{"a": "hello"})
	if err != nil {
		t.Errorf("MCDC case2: present non-nil value must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: NotNullConstraint.ValidateInsert — `val == nil`
//   (second NULL check: column present but nil)
//
//   A = val == nil
//
//   Case 1 (A=T): column present with nil value → NOT NULL violation
//   Case 2 (A=F): column present with non-nil → no violation
// ---------------------------------------------------------------------------

func TestMCDC_NotNullValidateInsert_NilValue(t *testing.T) {
	// Case 1: A=T — column present but nil → error
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateInsert(map[string]interface{}{"a": nil})
	if err == nil {
		t.Error("MCDC case1: nil value for NOT NULL column must return error")
	}
}

func TestMCDC_NotNullValidateInsert_NonNilValue(t *testing.T) {
	// Case 2: A=F — non-nil value → no error
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateInsert(map[string]interface{}{"a": "ok"})
	if err != nil {
		t.Errorf("MCDC case2: non-nil value must not error; got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Condition: NotNullConstraint.ApplyDefaults — `exists && val != nil`
//   (guard that skips default application when value is already present+non-nil)
//
//   A = exists (key present in values map)
//   B = val != nil
//
//   Skip only when A && B is true.
//
//   Case 1 (A=T, B=T): present non-nil → skip (default not applied)
//   Case 2 (A=T, B=F): present but nil → do NOT skip (default applied)
//   Case 3 (A=F, B=–): absent → do NOT skip (default applied)
// ---------------------------------------------------------------------------

func TestMCDC_ApplyDefaults_PresentNonNil(t *testing.T) {
	// Case 1: A=T, B=T → skip; existing value preserved
	defaultVal := "default_text"
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", Default: defaultVal},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	vals := map[string]interface{}{"a": "explicit"}
	_ = nnc.ApplyDefaults(vals, true)
	if vals["a"] != "explicit" {
		t.Error("MCDC case1: present non-nil must not be overwritten by default")
	}
}

func TestMCDC_ApplyDefaults_PresentNil(t *testing.T) {
	// Case 2: A=T, B=F → apply default
	defaultVal := "filled"
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", Default: defaultVal},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	vals := map[string]interface{}{"a": nil}
	_ = nnc.ApplyDefaults(vals, true)
	if vals["a"] != defaultVal {
		t.Errorf("MCDC case2: nil value must be replaced by default, got %v", vals["a"])
	}
}

func TestMCDC_ApplyDefaults_Absent(t *testing.T) {
	// Case 3: A=F → apply default
	defaultVal := int64(99)
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "n", Type: "INTEGER", Default: defaultVal},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	vals := map[string]interface{}{}
	_ = nnc.ApplyDefaults(vals, true)
	if vals["n"] != defaultVal {
		t.Errorf("MCDC case3: absent column must be filled by default, got %v", vals["n"])
	}
}

// ---------------------------------------------------------------------------
// Condition: NotNullConstraint.ValidateUpdate — `!col.NotNull`
//   (early-continue for nullable columns)
//
//   A = !col.NotNull
//
//   Case 1 (A=T): nullable column updated to nil → no error
//   Case 2 (A=F): NOT NULL column updated to nil → error
// ---------------------------------------------------------------------------

func TestMCDC_NotNullValidateUpdate_NullableColumnNilOK(t *testing.T) {
	// Case 1: A=T → nil update to nullable column is fine
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "opt", Type: "TEXT", NotNull: false},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateUpdate(map[string]interface{}{"opt": nil})
	if err != nil {
		t.Errorf("MCDC case1: nil update to nullable column must not error; got %v", err)
	}
}

func TestMCDC_NotNullValidateUpdate_NotNullColumnNilError(t *testing.T) {
	// Case 2: A=F → nil update to NOT NULL column → error
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "req", Type: "TEXT", NotNull: true},
		},
	}
	nnc := NewNotNullConstraint(tbl)
	err := nnc.ValidateUpdate(map[string]interface{}{"req": nil})
	if err == nil {
		t.Error("MCDC case2: nil update to NOT NULL column must error")
	}
}

// ---------------------------------------------------------------------------
// Condition: UniqueConstraint.valuesMatch — `val1 == nil || val2 == nil`
//   (NULL distinctness guard — OR of two sub-conditions)
//
//   A = val1 == nil
//   B = val2 == nil
//
//   Returns false (distinct) when A || B is true.
//
//   Case 1 (A=T): first value nil → false (distinct)
//   Case 2 (A=F, B=T): second value nil → false (distinct)
//   Case 3 (A=F, B=F): both non-nil → compare values
// ---------------------------------------------------------------------------

func TestMCDC_valuesMatch_Val1Nil(t *testing.T) {
	// Case 1: A=T → false (NULLs are always distinct)
	uc := &UniqueConstraint{Columns: []string{"x"}}
	if uc.valuesMatch(map[string]interface{}{"x": nil}, map[string]interface{}{"x": 1}) {
		t.Error("MCDC case1: nil first value must be distinct")
	}
}

func TestMCDC_valuesMatch_Val2Nil(t *testing.T) {
	// Case 2: A=F, B=T → false
	uc := &UniqueConstraint{Columns: []string{"x"}}
	if uc.valuesMatch(map[string]interface{}{"x": 1}, map[string]interface{}{"x": nil}) {
		t.Error("MCDC case2: nil second value must be distinct")
	}
}

func TestMCDC_valuesMatch_BothNonNilEqual(t *testing.T) {
	// Case 3: A=F, B=F, values equal → true (conflict)
	uc := &UniqueConstraint{Columns: []string{"x"}}
	if !uc.valuesMatch(map[string]interface{}{"x": int64(5)}, map[string]interface{}{"x": int64(5)}) {
		t.Error("MCDC case3: matching non-nil values must be a conflict")
	}
}

// ---------------------------------------------------------------------------
// Condition: generateIndexName — `constraintName != ""`
//   (single sub-condition determining index name format)
//
//   A = constraintName != ""
//
//   Case 1 (A=T): named constraint → uses constraint name in index name
//   Case 2 (A=F): unnamed constraint → uses column names
// ---------------------------------------------------------------------------

func TestMCDC_generateIndexName_Named(t *testing.T) {
	// Case 1: A=T
	name := generateIndexName("mycon", "mytable", []string{"col1"})
	if name != "sqlite_autoindex_mytable_mycon" {
		t.Errorf("MCDC case1: named constraint index name wrong, got %q", name)
	}
}

func TestMCDC_generateIndexName_Unnamed(t *testing.T) {
	// Case 2: A=F → uses column names
	name := generateIndexName("", "mytable", []string{"col1", "col2"})
	if name != "sqlite_autoindex_mytable_col1_col2" {
		t.Errorf("MCDC case2: unnamed constraint index name wrong, got %q", name)
	}
}

// ---------------------------------------------------------------------------
// Condition: UniqueViolationError.Error — `e.ConstraintName != ""`
//   (single sub-condition: named vs unnamed error message)
//
//   A = e.ConstraintName != ""
//
//   Case 1 (A=T): named constraint → message uses constraint name
//   Case 2 (A=F): unnamed → message uses column list
// ---------------------------------------------------------------------------

func TestMCDC_UniqueViolationError_Named(t *testing.T) {
	// Case 1: A=T
	err := &UniqueViolationError{
		ConstraintName: "uq_email",
		TableName:      "users",
		Columns:        []string{"email"},
	}
	msg := err.Error()
	if msg != "UNIQUE constraint failed: users.uq_email" {
		t.Errorf("MCDC case1: named constraint error wrong, got %q", msg)
	}
}

func TestMCDC_UniqueViolationError_Unnamed(t *testing.T) {
	// Case 2: A=F
	err := &UniqueViolationError{
		ConstraintName: "",
		TableName:      "users",
		Columns:        []string{"email", "phone"},
	}
	msg := err.Error()
	if msg != "UNIQUE constraint failed: users.email,phone" {
		t.Errorf("MCDC case2: unnamed constraint error wrong, got %q", msg)
	}
}

// ---------------------------------------------------------------------------
// Condition: isIntegerPrimaryKey — `col.PrimaryKey && (col.Type == "INTEGER" || col.Type == "INT")`
//   (compound condition inside isIntegerPrimaryKey)
//
//   A = col.PrimaryKey
//   B = col.Type == "INTEGER"
//   C = col.Type == "INT"
//
//   Returns true only when A && (B || C) is true.
//   N = 3 sub-conditions → 4 test cases.
//
//   Case 1 (A=F): column not marked PrimaryKey → false
//   Case 2 (A=T, B=F, C=F): PrimaryKey but wrong type → false
//   Case 3 (A=T, B=T): PrimaryKey + INTEGER type → true
//   Case 4 (A=T, B=F, C=T): PrimaryKey + INT type → true
// ---------------------------------------------------------------------------

func buildPKConstraint(pkColName, colType string, isPK bool) *PrimaryKeyConstraint {
	col := &schema.Column{Name: pkColName, Type: colType, PrimaryKey: isPK}
	tbl := &schema.Table{
		Name:       "t",
		Columns:    []*schema.Column{col},
		PrimaryKey: []string{pkColName},
	}
	return &PrimaryKeyConstraint{Table: tbl}
}

func TestMCDC_isIntegerPrimaryKey_NotPKColumn(t *testing.T) {
	// Case 1: A=F — col.PrimaryKey == false
	pk := buildPKConstraint("id", "INTEGER", false)
	if pk.isIntegerPrimaryKey() {
		t.Error("MCDC case1: non-PK column must not be INTEGER PRIMARY KEY")
	}
}

func TestMCDC_isIntegerPrimaryKey_WrongType(t *testing.T) {
	// Case 2: A=T, B=F, C=F — PrimaryKey but TEXT type
	pk := buildPKConstraint("id", "TEXT", true)
	if pk.isIntegerPrimaryKey() {
		t.Error("MCDC case2: PK column with TEXT type must not be INTEGER PRIMARY KEY")
	}
}

func TestMCDC_isIntegerPrimaryKey_IntegerType(t *testing.T) {
	// Case 3: A=T, B=T — PrimaryKey + INTEGER
	pk := buildPKConstraint("id", "INTEGER", true)
	if !pk.isIntegerPrimaryKey() {
		t.Error("MCDC case3: PK column with INTEGER type must be INTEGER PRIMARY KEY")
	}
}

func TestMCDC_isIntegerPrimaryKey_IntType(t *testing.T) {
	// Case 4: A=T, C=T — PrimaryKey + INT
	pk := buildPKConstraint("id", "INT", true)
	if !pk.isIntegerPrimaryKey() {
		t.Error("MCDC case4: PK column with INT type must be INTEGER PRIMARY KEY")
	}
}

// ---------------------------------------------------------------------------
// Condition: handleCompositePrimaryKey — `!exists || val == nil`
//   (NULL guard for composite PK columns)
//
//   A = !exists  (column not in values map)
//   B = val == nil
//
//   Error returned when A || B is true.
//
//   Case 1 (A=T): column absent → error
//   Case 2 (A=F, B=T): column present but nil → error
//   Case 3 (A=F, B=F): column present and non-nil → no error from this check
// ---------------------------------------------------------------------------

func TestMCDC_handleCompositePK_ColumnAbsent(t *testing.T) {
	// Case 1: A=T — missing PK column
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", PrimaryKey: true},
			{Name: "b", Type: "TEXT", PrimaryKey: true},
		},
		PrimaryKey: []string{"a", "b"},
	}
	pk := &PrimaryKeyConstraint{Table: tbl}
	// Only provide "a", not "b"
	_, err := pk.handleCompositePrimaryKey(map[string]interface{}{"a": "x"}, false, 0)
	if err == nil {
		t.Error("MCDC case1: absent PK column must error")
	}
}

func TestMCDC_handleCompositePK_ColumnNil(t *testing.T) {
	// Case 2: A=F, B=T — present but nil
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", PrimaryKey: true},
		},
		PrimaryKey: []string{"a"},
	}
	pk := &PrimaryKeyConstraint{Table: tbl}
	_, err := pk.handleCompositePrimaryKey(map[string]interface{}{"a": nil}, false, 0)
	if err == nil {
		t.Error("MCDC case2: nil PK column must error")
	}
}

func TestMCDC_handleCompositePK_ColumnPresent(t *testing.T) {
	// Case 3: A=F, B=F — present non-nil, no rowid → auto-generate rowid (no error)
	bt := btree.NewBtree(4096)
	rootPage, err := bt.CreateTable()
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	tbl := &schema.Table{
		Name: "t",
		Columns: []*schema.Column{
			{Name: "a", Type: "TEXT", PrimaryKey: true},
		},
		PrimaryKey: []string{"a"},
		RootPage:   rootPage,
	}
	pk := &PrimaryKeyConstraint{Table: tbl, Btree: bt}
	_, err = pk.handleCompositePrimaryKey(map[string]interface{}{"a": "x"}, false, 0)
	if err != nil {
		t.Errorf("MCDC case3: present non-nil PK must not error; got %v", err)
	}
}
