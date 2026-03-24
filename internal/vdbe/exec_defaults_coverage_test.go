// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

// Tests for the DEFAULT-value helper functions in exec.go:
//   applyDefaultValueIfAvailable, recordIdxToSchemaIdx, parseDefaultValue,
//   tryParseAsInteger, tryParseAsFloat, tryParseAsQuotedString, isQuotedWith.
//
// applyDefaultValueIfAvailable is triggered when parseColumnIntoMem reads a
// NULL column that was added via ALTER TABLE ADD COLUMN: the VDBE reads the
// schema's DEFAULT string and writes the parsed value into the destination
// register.  We drive it here by wiring up lightweight mock types that
// satisfy the three private interfaces checked inside the function.

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Mock helpers
// ---------------------------------------------------------------------------

// mockColumn implements the colDefault and colIsRowid interfaces expected by
// applyDefaultValueIfAvailable and recordIdxToSchemaIdx.
type mockColumn struct {
	defaultVal interface{}
	isIPK      bool
}

func (m *mockColumn) GetDefault() interface{}   { return m.defaultVal }
func (m *mockColumn) IsIntegerPrimaryKey() bool { return m.isIPK }

// mockTable implements the tblColumns interface.
type mockTable struct {
	cols []interface{}
}

func (m *mockTable) GetColumns() []interface{} { return m.cols }

// newV returns a minimal VDBE suitable for calling the helper methods.
func newV() *VDBE {
	return NewTestVDBE(4)
}

// ---------------------------------------------------------------------------
// recordIdxToSchemaIdx
// ---------------------------------------------------------------------------

func TestRecordIdxToSchemaIdx_NoIPK(t *testing.T) {
	t.Parallel()
	cols := []interface{}{
		&mockColumn{},
		&mockColumn{},
		&mockColumn{},
	}
	for ri, want := range []int{0, 1, 2} {
		got := recordIdxToSchemaIdx(cols, ri)
		if got != want {
			t.Errorf("recordIdx=%d: want %d, got %d", ri, want, got)
		}
	}
}

func TestRecordIdxToSchemaIdx_WithIPK(t *testing.T) {
	t.Parallel()
	// col[0] is IPK → not in record; record index 0 maps to schema index 1.
	cols := []interface{}{
		&mockColumn{isIPK: true},
		&mockColumn{},
		&mockColumn{},
	}
	if got := recordIdxToSchemaIdx(cols, 0); got != 1 {
		t.Errorf("want 1, got %d", got)
	}
	if got := recordIdxToSchemaIdx(cols, 1); got != 2 {
		t.Errorf("want 2, got %d", got)
	}
}

func TestRecordIdxToSchemaIdx_OutOfRange(t *testing.T) {
	t.Parallel()
	cols := []interface{}{&mockColumn{}}
	if got := recordIdxToSchemaIdx(cols, 5); got != -1 {
		t.Errorf("want -1, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// tryParseAsInteger
// ---------------------------------------------------------------------------

func TestTryParseAsInteger_Valid(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if !v.tryParseAsInteger("42", dst) {
		t.Fatal("expected true for '42'")
	}
	if dst.IntValue() != 42 {
		t.Errorf("want 42, got %d", dst.IntValue())
	}
}

func TestTryParseAsInteger_Negative(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if !v.tryParseAsInteger("-7", dst) {
		t.Fatal("expected true for '-7'")
	}
	if dst.IntValue() != -7 {
		t.Errorf("want -7, got %d", dst.IntValue())
	}
}

func TestTryParseAsInteger_NotInteger(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if v.tryParseAsInteger("abc", dst) {
		t.Fatal("expected false for 'abc'")
	}
}

// ---------------------------------------------------------------------------
// tryParseAsFloat
// ---------------------------------------------------------------------------

func TestTryParseAsFloat_Valid(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if !v.tryParseAsFloat("3.14", dst) {
		t.Fatal("expected true for '3.14'")
	}
	r := dst.RealValue()
	if r < 3.13 || r > 3.15 {
		t.Errorf("want ~3.14, got %f", r)
	}
}

func TestTryParseAsFloat_NotFloat(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if v.tryParseAsFloat("hello", dst) {
		t.Fatal("expected false for 'hello'")
	}
}

// ---------------------------------------------------------------------------
// isQuotedWith
// ---------------------------------------------------------------------------

func TestIsQuotedWith_SingleQuote(t *testing.T) {
	t.Parallel()
	if !isQuotedWith("'hello'", '\'') {
		t.Error("expected true for single-quoted string")
	}
}

func TestIsQuotedWith_DoubleQuote(t *testing.T) {
	t.Parallel()
	if !isQuotedWith(`"world"`, '"') {
		t.Error("expected true for double-quoted string")
	}
}

func TestIsQuotedWith_Mismatch(t *testing.T) {
	t.Parallel()
	if isQuotedWith("'hello\"", '\'') {
		t.Error("expected false when closing quote differs")
	}
}

// ---------------------------------------------------------------------------
// tryParseAsQuotedString
// ---------------------------------------------------------------------------

func TestTryParseAsQuotedString_SingleQuote(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if !v.tryParseAsQuotedString("'hello'", dst) {
		t.Fatal("expected true for single-quoted string")
	}
	if dst.StrValue() != "hello" {
		t.Errorf("want 'hello', got %q", dst.StrValue())
	}
}

func TestTryParseAsQuotedString_DoubleQuote(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if !v.tryParseAsQuotedString(`"world"`, dst) {
		t.Fatal("expected true for double-quoted string")
	}
	if dst.StrValue() != "world" {
		t.Errorf("want 'world', got %q", dst.StrValue())
	}
}

func TestTryParseAsQuotedString_TooShort(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if v.tryParseAsQuotedString("x", dst) {
		t.Fatal("expected false for single-char string")
	}
}

func TestTryParseAsQuotedString_NotQuoted(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	if v.tryParseAsQuotedString("hello", dst) {
		t.Fatal("expected false for unquoted string")
	}
}

// ---------------------------------------------------------------------------
// parseDefaultValue
// ---------------------------------------------------------------------------

func TestParseDefaultValue_Integer(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	v.parseDefaultValue("100", dst)
	if dst.IntValue() != 100 {
		t.Errorf("want 100, got %d", dst.IntValue())
	}
}

func TestParseDefaultValue_Float(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	v.parseDefaultValue("2.718", dst)
	r := dst.RealValue()
	if r < 2.71 || r > 2.72 {
		t.Errorf("want ~2.718, got %f", r)
	}
}

func TestParseDefaultValue_NullLiteral(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	v.parseDefaultValue("NULL", dst)
	if !dst.IsNull() {
		t.Error("want NULL")
	}
}

func TestParseDefaultValue_QuotedString(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	v.parseDefaultValue("'abc'", dst)
	if dst.StrValue() != "abc" {
		t.Errorf("want 'abc', got %q", dst.StrValue())
	}
}

func TestParseDefaultValue_UnquotedFallback(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	v.parseDefaultValue("bareword", dst)
	if dst.StrValue() != "bareword" {
		t.Errorf("want 'bareword', got %q", dst.StrValue())
	}
}

// ---------------------------------------------------------------------------
// applyDefaultValueIfAvailable
// ---------------------------------------------------------------------------

func TestApplyDefault_IntegerDefault(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	tbl := &mockTable{cols: []interface{}{&mockColumn{defaultVal: "42"}}}
	v.applyDefaultValueIfAvailable(0, dst, tbl)
	if dst.IntValue() != 42 {
		t.Errorf("want 42, got %d", dst.IntValue())
	}
}

func TestApplyDefault_QuotedStringDefault(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	tbl := &mockTable{cols: []interface{}{&mockColumn{defaultVal: "'hello'"}}}
	v.applyDefaultValueIfAvailable(0, dst, tbl)
	if dst.StrValue() != "hello" {
		t.Errorf("want 'hello', got %q", dst.StrValue())
	}
}

func TestApplyDefault_NilDefault(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	dst.SetInt(99)
	tbl := &mockTable{cols: []interface{}{&mockColumn{defaultVal: nil}}}
	v.applyDefaultValueIfAvailable(0, dst, tbl)
	// nil default → no change
	if dst.IntValue() != 99 {
		t.Errorf("want 99 unchanged, got %d", dst.IntValue())
	}
}

func TestApplyDefault_NoTableInterface(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	dst.SetInt(77)
	// Pass something that doesn't implement tblColumns.
	v.applyDefaultValueIfAvailable(0, dst, "not-a-table")
	if dst.IntValue() != 77 {
		t.Errorf("want 77 unchanged, got %d", dst.IntValue())
	}
}

func TestApplyDefault_OutOfRangeIdx(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	dst.SetInt(55)
	tbl := &mockTable{cols: []interface{}{&mockColumn{defaultVal: "1"}}}
	// record index 5 maps to schema index -1 → no change
	v.applyDefaultValueIfAvailable(5, dst, tbl)
	if dst.IntValue() != 55 {
		t.Errorf("want 55 unchanged, got %d", dst.IntValue())
	}
}

func TestApplyDefault_WithIPKSkipped(t *testing.T) {
	t.Parallel()
	v := newV()
	dst := NewMem()
	// col[0] is IPK (not in record), col[1] has default '99'
	tbl := &mockTable{cols: []interface{}{
		&mockColumn{isIPK: true},
		&mockColumn{defaultVal: "99"},
	}}
	// record index 0 → schema index 1 (IPK skipped)
	v.applyDefaultValueIfAvailable(0, dst, tbl)
	if dst.IntValue() != 99 {
		t.Errorf("want 99, got %d", dst.IntValue())
	}
}
