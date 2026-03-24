// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"math"
	"testing"
)

// ============================================================
// MC/DC tests for vdbe non-exec files:
//   mem.go, vdbe.go, record.go, window.go, functions.go
//
// For each compound condition A&&B or A||B, N+1 test cases
// are written so that each sub-condition independently flips
// the overall outcome.
// Test names contain "MCDC" for -run MCDC selection.
// ============================================================

// ============================================================
// mem.go: Add / Subtract / Multiply – m.IsNull() || other.IsNull()
// Source condition (mem.go ~756): if m.IsNull() || other.IsNull()
// Outcome: result is set to NULL
// Cases:
//   A=T, B=* → outcome=true  (m is NULL)
//   A=F, B=T → outcome=true  (other is NULL)
//   A=F, B=F → outcome=false (no NULL, arithmetic runs)
// ============================================================

func TestMCDC_MemAdd_MIsNull(t *testing.T) {
	t.Parallel()
	// A=true (m is NULL), B=false (other is non-null) → result NULL
	m := NewMemNull()
	other := NewMemInt(5)
	_ = m.Add(other)
	if !m.IsNull() {
		t.Error("Expected NULL when m is NULL")
	}
}

func TestMCDC_MemAdd_OtherIsNull(t *testing.T) {
	t.Parallel()
	// A=false (m non-null), B=true (other is NULL) → result NULL
	m := NewMemInt(3)
	other := NewMemNull()
	_ = m.Add(other)
	if !m.IsNull() {
		t.Error("Expected NULL when other is NULL")
	}
}

func TestMCDC_MemAdd_NeitherNull(t *testing.T) {
	t.Parallel()
	// A=false, B=false → arithmetic runs, result is 8
	m := NewMemInt(3)
	other := NewMemInt(5)
	_ = m.Add(other)
	if m.IsNull() {
		t.Error("Expected non-null result when neither operand is NULL")
	}
	if m.IntValue() != 8 {
		t.Errorf("Expected 8, got %d", m.IntValue())
	}
}

// ============================================================
// mem.go: Add – m.IsInt() && other.IsInt()
// Source condition (mem.go ~762): if m.IsInt() && other.IsInt()
// Outcome: integer addition path taken
// Cases:
//   A=F, B=* → outcome=false (m is real → float path)
//   A=T, B=F → outcome=false (other is real → float path)
//   A=T, B=T → outcome=true  (both int → integer addition)
// ============================================================

func TestMCDC_MemAdd_MNotInt(t *testing.T) {
	t.Parallel()
	// A=false: m is real → float path taken
	m := NewMemReal(2.5)
	other := NewMemInt(3)
	_ = m.Add(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	// result must be 5.5 as real
	if m.IsInt() {
		t.Error("Expected real result when m is real")
	}
}

func TestMCDC_MemAdd_OtherNotInt(t *testing.T) {
	t.Parallel()
	// A=true (m is int), B=false (other is real) → float path
	m := NewMemInt(2)
	other := NewMemReal(3.5)
	_ = m.Add(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	// result 5.5 as real
	if m.IsInt() {
		t.Error("Expected real result when other is real")
	}
}

func TestMCDC_MemAdd_BothInt(t *testing.T) {
	t.Parallel()
	// A=true, B=true → integer addition
	m := NewMemInt(10)
	other := NewMemInt(7)
	_ = m.Add(other)
	if !m.IsInt() {
		t.Error("Expected integer result when both are integers")
	}
	if m.IntValue() != 17 {
		t.Errorf("Expected 17, got %d", m.IntValue())
	}
}

// ============================================================
// mem.go: Divide – m.i == math.MinInt64 && other.i == -1
// Source condition (mem.go ~834): if m.i == math.MinInt64 && other.i == -1
// Outcome: overflow edge case → result stored as real
// Cases:
//   A=F, B=* → outcome=false (integer division)
//   A=T, B=F → outcome=false (integer division by non-(-1))
//   A=T, B=T → outcome=true  (overflow: result becomes real)
// ============================================================

func TestMCDC_MemDivide_NotMinInt64(t *testing.T) {
	t.Parallel()
	// A=false: m.i is not MinInt64 → normal integer division
	m := NewMemInt(10)
	other := NewMemInt(-1)
	_ = m.Divide(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	if m.IsReal() {
		t.Error("Expected integer result, got real (no overflow)")
	}
	if m.IntValue() != -10 {
		t.Errorf("Expected -10, got %d", m.IntValue())
	}
}

func TestMCDC_MemDivide_MinInt64DivNonMinusOne(t *testing.T) {
	t.Parallel()
	// A=true (m is MinInt64), B=false (other is not -1) → integer division
	m := NewMemInt(math.MinInt64)
	other := NewMemInt(2)
	_ = m.Divide(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	if m.IsReal() {
		t.Error("Expected integer result for MinInt64/2")
	}
}

func TestMCDC_MemDivide_MinInt64DivMinusOne(t *testing.T) {
	t.Parallel()
	// A=true, B=true → overflow: real result
	m := NewMemInt(math.MinInt64)
	other := NewMemInt(-1)
	_ = m.Divide(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	if !m.IsReal() {
		t.Error("Expected real result for MinInt64 / -1 (overflow)")
	}
}

// ============================================================
// mem.go: release – m.flags&MemDyn != 0 && m.xDel != nil
// Source condition (mem.go ~672): if m.flags&MemDyn != 0 && m.xDel != nil
// Outcome: destructor is called
// Cases:
//   A=F, B=* → outcome=false (not dynamic → no destructor)
//   A=T, B=F → outcome=false (dynamic but no destructor)
//   A=T, B=T → outcome=true  (dynamic + destructor → called)
// ============================================================

func TestMCDC_Release_NotDynamic(t *testing.T) {
	t.Parallel()
	// A=false: flag MemDyn not set → no destructor call
	called := false
	m := NewMemStr("hello")
	// MemStr sets MemTerm, NOT MemDyn
	m.xDel = func(interface{}) { called = true }
	m.release()
	if called {
		t.Error("Destructor should NOT be called when MemDyn flag is not set")
	}
}

func TestMCDC_Release_DynamicNoDestructor(t *testing.T) {
	t.Parallel()
	// A=true (MemDyn set), B=false (xDel is nil) → no call
	called := false
	m := &Mem{
		flags: MemDyn | MemStr,
		z:     []byte("data"),
		xDel:  nil, // B is false
	}
	_ = called
	// Should not panic when MemDyn is set but xDel is nil
	m.release()
	// No destructor should have been called
}

func TestMCDC_Release_DynamicWithDestructor(t *testing.T) {
	t.Parallel()
	// A=true, B=true → destructor is called
	called := false
	m := &Mem{
		flags: MemDyn | MemStr,
		z:     []byte("data"),
		xDel:  func(interface{}) { called = true },
	}
	m.release()
	if !called {
		t.Error("Expected destructor to be called when MemDyn is set and xDel is non-nil")
	}
}

// ============================================================
// mem.go: shouldCompareNumeric – mIsNumeric && otherIsNumeric
// Source condition (mem.go ~620): return mIsNumeric && otherIsNumeric
// Outcome: numeric comparison used
// Cases:
//   A=F, B=* → outcome=false
//   A=T, B=F → outcome=false
//   A=T, B=T → outcome=true
// ============================================================

func TestMCDC_ShouldCompareNumeric_MNotNumeric(t *testing.T) {
	t.Parallel()
	// A=false: m is string, other is int
	m := NewMemStr("5")
	other := NewMemInt(5)
	// Compare should use string/mixed path, not pure numeric
	result := m.Compare(other)
	_ = result // main check is no panic and correct outcome
	if shouldCompareNumeric(false, true) {
		t.Error("shouldCompareNumeric(false,true) must return false")
	}
}

func TestMCDC_ShouldCompareNumeric_OtherNotNumeric(t *testing.T) {
	t.Parallel()
	// A=true, B=false
	if shouldCompareNumeric(true, false) {
		t.Error("shouldCompareNumeric(true,false) must return false")
	}
}

func TestMCDC_ShouldCompareNumeric_BothNumeric(t *testing.T) {
	t.Parallel()
	// A=true, B=true → true
	if !shouldCompareNumeric(true, true) {
		t.Error("shouldCompareNumeric(true,true) must return true")
	}
	// Verify end-to-end: int vs int comparison uses numeric path
	m := NewMemInt(3)
	other := NewMemInt(7)
	if m.Compare(other) != -1 {
		t.Error("Expected -1 for 3 < 7")
	}
}

// ============================================================
// mem.go: shouldCompareMixed – (mIsNumeric && otherIsText) || (otherIsNumeric && mIsText)
// Source condition (mem.go ~625): (mIsNumeric && otherIsText) || (otherIsNumeric && mIsText)
// Outcome: mixed numeric/text comparison
// Cases:
//   A=F, B=F → outcome=false
//   A=T, B=F → outcome=true  (mIsNumeric && otherIsText)
//   A=F, B=T → outcome=true  (otherIsNumeric && mIsText)
// ============================================================

func TestMCDC_ShouldCompareMixed_NeitherMixed(t *testing.T) {
	t.Parallel()
	// A=false, B=false: neither is (numeric+text) pairing
	// both are strings → false
	if shouldCompareMixed(false, false, true, true) {
		t.Error("shouldCompareMixed(false,false,true,true) must be false")
	}
}

func TestMCDC_ShouldCompareMixed_MNumericOtherText(t *testing.T) {
	t.Parallel()
	// A=true: mIsNumeric=true, otherIsText=true → true
	if !shouldCompareMixed(true, false, false, true) {
		t.Error("shouldCompareMixed(true,false,false,true) must be true")
	}
}

func TestMCDC_ShouldCompareMixed_OtherNumericMText(t *testing.T) {
	t.Parallel()
	// B=true: otherIsNumeric=true, mIsText=true → true
	if !shouldCompareMixed(false, true, true, false) {
		t.Error("shouldCompareMixed(false,true,true,false) must be true")
	}
}

// ============================================================
// mem.go: compareMixedNumericText – mIsNumeric && !mIsText
// Source condition (mem.go ~580): if mIsNumeric && !mIsText
// Outcome: m is numeric, other is text → compare numeric
// Cases:
//   A=F, B=* → skip this branch (check second branch)
//   A=T, B=T → true: mIsNumeric=true, mIsText=false → numeric < text case
//
// And: !mIsNumeric && mIsText (line ~589)
// Cases:
//   A=T, B=F → false (mIsNumeric=true)
//   A=F, B=T → false (mIsText=false)  -- NOTE: complementary branch
//   A=F, B=T → true (!mIsNumeric=true, mIsText=true) → text vs numeric
// ============================================================

func TestMCDC_CompareMixed_NumericVsText_NumericIsM(t *testing.T) {
	t.Parallel()
	// mIsNumeric=true, mIsText=false: numeric m vs text other
	// parseable text → numeric comparison
	m := NewMemInt(42)
	other := NewMemStr("42")
	result := m.Compare(other)
	// "42" parses as 42, so 42==42 → 0
	if result != 0 {
		t.Errorf("Expected 0 for numeric 42 vs text '42', got %d", result)
	}
}

func TestMCDC_CompareMixed_NumericVsText_NumericIsOther(t *testing.T) {
	t.Parallel()
	// !mIsNumeric && mIsText: m is text, other is numeric
	m := NewMemStr("10")
	other := NewMemInt(10)
	result := m.Compare(other)
	// "10" parses as 10, 10==10 → 0
	if result != 0 {
		t.Errorf("Expected 0 for text '10' vs numeric 10, got %d", result)
	}
}

func TestMCDC_CompareMixed_TextNotParseable_NumericFirst(t *testing.T) {
	t.Parallel()
	// mIsNumeric=true, other is unparseable text → numeric < text → -1
	m := NewMemInt(5)
	other := NewMemStr("hello")
	result := m.Compare(other)
	if result != -1 {
		t.Errorf("Expected -1 (numeric < unparseable text), got %d", result)
	}
}

// ============================================================
// mem.go: compareNulls – !mNull && !oNull (returns early false)
//                      – mNull && oNull     (both null → 0, true)
// Source condition (mem.go ~503-506):
//   if !mNull && !oNull { return 0, false }
//   if mNull && oNull   { return 0, true  }
// Cases for !mNull && !oNull:
//   A=F, B=* → go to next check
//   A=T, B=F → go to next check
//   A=T, B=T → return false (no null handling needed)
// Cases for mNull && oNull:
//   A=F, B=* → continue
//   A=T, B=F → m is null, other is not null → handled by subsequent if
//   A=T, B=T → both null → 0, true
// ============================================================

func TestMCDC_CompareNulls_MNotNullOtherNotNull(t *testing.T) {
	t.Parallel()
	// A=true (!mNull), B=true (!oNull) → returns (0, false): no null handling
	m := NewMemInt(1)
	other := NewMemInt(2)
	result, handled := compareNulls(m, other)
	if handled {
		t.Error("Expected handled=false when neither is NULL")
	}
	if result != 0 {
		t.Error("Expected result=0 placeholder when not handled")
	}
}

func TestMCDC_CompareNulls_MNullOtherNotNull(t *testing.T) {
	t.Parallel()
	// mNull=true, oNull=false → returns (-1, true): m is less
	m := NewMemNull()
	other := NewMemInt(5)
	result, handled := compareNulls(m, other)
	if !handled {
		t.Error("Expected handled=true when m is NULL")
	}
	if result != -1 {
		t.Errorf("Expected -1 (null < non-null), got %d", result)
	}
}

func TestMCDC_CompareNulls_BothNull(t *testing.T) {
	t.Parallel()
	// mNull=true, oNull=true → returns (0, true): equal
	m := NewMemNull()
	other := NewMemNull()
	result, handled := compareNulls(m, other)
	if !handled {
		t.Error("Expected handled=true when both NULL")
	}
	if result != 0 {
		t.Errorf("Expected 0 for NULL==NULL, got %d", result)
	}
}

func TestMCDC_CompareNulls_MNotNullOtherNull(t *testing.T) {
	t.Parallel()
	// mNull=false, oNull=true → returns (1, true): other is less
	m := NewMemInt(5)
	other := NewMemNull()
	result, handled := compareNulls(m, other)
	if !handled {
		t.Error("Expected handled=true when other is NULL")
	}
	if result != 1 {
		t.Errorf("Expected 1 (non-null > null), got %d", result)
	}
}

// ============================================================
// vdbe.go: Sorter.Sort – s.Sorted || len(s.Rows) <= 1
// Source condition (vdbe.go ~308): if s.Sorted || len(s.Rows) <= 1
// Outcome: early return without sorting
// Cases:
//   A=T, B=* → outcome=true  (already sorted → skip)
//   A=F, B=T → outcome=true  (0 or 1 row → skip)
//   A=F, B=F → outcome=false (>1 row, not sorted → sort runs)
// ============================================================

func TestMCDC_SorterSort_AlreadySorted(t *testing.T) {
	t.Parallel()
	// A=true: already marked sorted → Sort() is a no-op
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(5)})
	_ = s.Insert([]*Mem{NewMemInt(2)})
	s.Sorted = true // manually mark sorted to force A=true

	_ = s.Sort()
	// Rows should remain in insertion order (not resorted)
	if s.Rows[0][0].IntValue() != 5 {
		t.Error("Expected insertion order preserved when already sorted")
	}
}

func TestMCDC_SorterSort_OneRow(t *testing.T) {
	t.Parallel()
	// A=false, B=true: only 1 row → Sort() returns immediately
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(42)})
	// len(s.Rows) == 1 → B is true

	_ = s.Sort()
	if !s.Sorted {
		t.Error("Expected Sorted=true after Sort() with 1 row")
	}
}

func TestMCDC_SorterSort_MultipleRowsNotSorted(t *testing.T) {
	t.Parallel()
	// A=false, B=false: >1 row, not sorted → actual sort runs
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	_ = s.Insert([]*Mem{NewMemInt(5)})
	_ = s.Insert([]*Mem{NewMemInt(1)})
	_ = s.Insert([]*Mem{NewMemInt(3)})

	_ = s.Sort()
	if !s.Sorted {
		t.Error("Expected Sorted=true after Sort()")
	}
	// Verify ascending order
	if s.Rows[0][0].IntValue() != 1 || s.Rows[1][0].IntValue() != 3 || s.Rows[2][0].IntValue() != 5 {
		t.Errorf("Expected sorted [1,3,5], got [%d,%d,%d]",
			s.Rows[0][0].IntValue(), s.Rows[1][0].IntValue(), s.Rows[2][0].IntValue())
	}
}

// ============================================================
// vdbe.go: Sorter.isColumnInBounds – colIdx < len(a) && colIdx < len(b)
// Source condition (vdbe.go ~329): return colIdx < len(a) && colIdx < len(b)
// Outcome: column is accessible in both rows
// Cases:
//   A=F, B=* → outcome=false (colIdx >= len(a))
//   A=T, B=F → outcome=false (colIdx >= len(b))
//   A=T, B=T → outcome=true  (colIdx valid in both)
// ============================================================

func TestMCDC_IsColumnInBounds_AOutOfRange(t *testing.T) {
	t.Parallel()
	// A=false: colIdx >= len(a)
	s := NewSorter([]int{0}, nil, nil, 1)
	a := []*Mem{NewMemInt(1)}               // len=1, valid indices: [0]
	b := []*Mem{NewMemInt(2), NewMemInt(3)} // len=2
	if s.isColumnInBounds(1, a, b) {
		t.Error("Expected false: colIdx=1 >= len(a)=1")
	}
}

func TestMCDC_IsColumnInBounds_BOutOfRange(t *testing.T) {
	t.Parallel()
	// A=true (colIdx < len(a)), B=false (colIdx >= len(b))
	s := NewSorter([]int{0}, nil, nil, 1)
	a := []*Mem{NewMemInt(1), NewMemInt(2)} // len=2
	b := []*Mem{NewMemInt(3)}               // len=1, valid indices: [0]
	if s.isColumnInBounds(1, a, b) {
		t.Error("Expected false: colIdx=1 >= len(b)=1")
	}
}

func TestMCDC_IsColumnInBounds_BothInRange(t *testing.T) {
	t.Parallel()
	// A=true, B=true → true
	s := NewSorter([]int{0}, nil, nil, 1)
	a := []*Mem{NewMemInt(1), NewMemInt(2)}
	b := []*Mem{NewMemInt(3), NewMemInt(4)}
	if !s.isColumnInBounds(0, a, b) {
		t.Error("Expected true: colIdx=0 is valid in both rows")
	}
	if !s.isColumnInBounds(1, a, b) {
		t.Error("Expected true: colIdx=1 is valid in both rows")
	}
}

// ============================================================
// vdbe.go: Sorter.compareRowNull – !aNull && !bNull (early exit)
//                                 – aNull && bNull  (both null)
// Source condition (vdbe.go ~362-365): !aNull && !bNull → (0, false)
//                                      aNull && bNull  → (0, true)
// Cases:
//   !aNull && !bNull: A=T,B=T → (0,false)
//   !aNull && !bNull: A=F,B=* → go to next check
//   aNull && bNull:   A=T,B=T → (0,true)
//   aNull && bNull:   A=T,B=F → mNull only → (-1 or 1 from subsequent)
// ============================================================

func TestMCDC_SorterCompareRowNull_NeitherNull(t *testing.T) {
	t.Parallel()
	// !aNull && !bNull → (0, false)
	s := NewSorter([]int{0}, nil, nil, 1)
	aVal := NewMemInt(1)
	bVal := NewMemInt(2)
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if isNull {
		t.Error("Expected isNull=false when neither value is NULL")
	}
	if result != 0 {
		t.Error("Expected result=0 placeholder")
	}
}

func TestMCDC_SorterCompareRowNull_BothNull(t *testing.T) {
	t.Parallel()
	// aNull=true, bNull=true → (0, true)
	s := NewSorter([]int{0}, nil, nil, 1)
	aVal := NewMemNull()
	bVal := NewMemNull()
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if !isNull {
		t.Error("Expected isNull=true when both values are NULL")
	}
	if result != 0 {
		t.Errorf("Expected result=0 for NULL==NULL, got %d", result)
	}
}

func TestMCDC_SorterCompareRowNull_ANullBNotNull(t *testing.T) {
	t.Parallel()
	// aNull=true, bNull=false → isNull=true, with nullsFirst=true → result=-1
	s := NewSorter([]int{0}, []bool{false}, nil, 1) // ASC → NULLs first
	aVal := NewMemNull()
	bVal := NewMemInt(5)
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if !isNull {
		t.Error("Expected isNull=true when a is NULL")
	}
	if result != -1 {
		t.Errorf("Expected -1 (NULL sorts first in ASC), got %d", result)
	}
}

func TestMCDC_SorterCompareRowNull_ANotNullBNull(t *testing.T) {
	t.Parallel()
	// !aNull (false for !aNull check) → !aNull && !bNull=false → next check
	// aNull && bNull: aNull=false → (result,true) with b null
	s := NewSorter([]int{0}, []bool{false}, nil, 1)
	aVal := NewMemInt(5)
	bVal := NewMemNull()
	result, isNull := s.compareRowNull(aVal, bVal, 0)
	if !isNull {
		t.Error("Expected isNull=true when b is NULL")
	}
	if result != 1 {
		t.Errorf("Expected 1 (non-null > NULL in ASC), got %d", result)
	}
}

// ============================================================
// record.go: decodeInt24Value – offset < 0 || offset+3 > len(data)
// Source condition (record.go ~149): if offset < 0 || offset+3 > len(data)
// Outcome: ErrBufferOverflow
// Cases:
//   A=T, B=* → outcome=true  (negative offset)
//   A=F, B=T → outcome=true  (not enough data)
//   A=F, B=F → outcome=false (valid access)
// ============================================================

func TestMCDC_DecodeInt24_NegativeOffset(t *testing.T) {
	t.Parallel()
	// A=true: negative offset
	data := []byte{0x01, 0x02, 0x03}
	_, err := decodeInt24Value(data, -1)
	if err != ErrBufferOverflow {
		t.Errorf("Expected ErrBufferOverflow for negative offset, got %v", err)
	}
}

func TestMCDC_DecodeInt24_InsufficientData(t *testing.T) {
	t.Parallel()
	// A=false, B=true: offset+3 > len(data) (only 2 bytes available)
	data := []byte{0x01, 0x02}
	_, err := decodeInt24Value(data, 0)
	if err != ErrBufferOverflow {
		t.Errorf("Expected ErrBufferOverflow for insufficient data, got %v", err)
	}
}

func TestMCDC_DecodeInt24_ValidAccess(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid 3-byte decode
	data := []byte{0x00, 0x00, 0x2A} // 42
	val, err := decodeInt24Value(data, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}
}

// ============================================================
// record.go: decodeInt48Value – offset < 0 || offset+6 > len(data)
// Source condition (record.go ~161): if offset < 0 || offset+6 > len(data)
// Outcome: ErrBufferOverflow
// Cases:
//   A=T, B=* → ErrBufferOverflow
//   A=F, B=T → ErrBufferOverflow
//   A=F, B=F → valid decode
// ============================================================

func TestMCDC_DecodeInt48_NegativeOffset(t *testing.T) {
	t.Parallel()
	data := []byte{0, 0, 0, 0, 0, 0}
	_, err := decodeInt48Value(data, -1)
	if err != ErrBufferOverflow {
		t.Errorf("Expected ErrBufferOverflow for negative offset, got %v", err)
	}
}

func TestMCDC_DecodeInt48_InsufficientData(t *testing.T) {
	t.Parallel()
	// A=false, B=true: only 4 bytes, need 6
	data := []byte{0, 0, 0, 0}
	_, err := decodeInt48Value(data, 0)
	if err != ErrBufferOverflow {
		t.Errorf("Expected ErrBufferOverflow for insufficient data, got %v", err)
	}
}

func TestMCDC_DecodeInt48_ValidAccess(t *testing.T) {
	t.Parallel()
	// A=false, B=false: exactly 6 bytes
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	val, err := decodeInt48Value(data, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}
}

// ============================================================
// record.go: decodeValue – serialType >= 1 && serialType <= 6
// Source condition (record.go ~202): if serialType >= 1 && serialType <= 6
// Outcome: fixed integer decoding used
// Cases:
//   A=F, B=* → outcome=false (serialType == 0 → NULL)
//   A=T, B=F → outcome=false (serialType == 7 → float64)
//   A=T, B=T → outcome=true  (serialType in [1,6] → integer)
// ============================================================

func TestMCDC_DecodeValue_SerialTypeZero(t *testing.T) {
	t.Parallel()
	// A=false: serialType=0 → NULL (zero-width const)
	val, n, err := decodeValue([]byte{}, 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected n=0 for NULL type, got %d", n)
	}
	if val != nil {
		t.Errorf("Expected nil for NULL type, got %v", val)
	}
}

func TestMCDC_DecodeValue_SerialTypeSeven(t *testing.T) {
	t.Parallel()
	// A=true (>=1), B=false (>6): serialType=7 → float64 decode
	// Encode float64(1.5) as 8 bytes big-endian
	data := make([]byte, 8)
	bits := math.Float64bits(1.5)
	for i := 7; i >= 0; i-- {
		data[i] = byte(bits)
		bits >>= 8
	}
	val, n, err := decodeValue(data, 0, 7)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 8 {
		t.Errorf("Expected n=8 for float64, got %d", n)
	}
	if val.(float64) != 1.5 {
		t.Errorf("Expected 1.5, got %v", val)
	}
}

func TestMCDC_DecodeValue_SerialTypeFixed(t *testing.T) {
	t.Parallel()
	// A=true, B=true: serialType=1 (int8) → fixed integer
	data := []byte{0x2A} // 42
	val, n, err := decodeValue(data, 0, 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("Expected n=1 for int8, got %d", n)
	}
	if val.(int64) != 42 {
		t.Errorf("Expected 42, got %v", val)
	}
}

// ============================================================
// window.go: WindowState.AddRow – len(ws.Partitions) == 0 || !ws.samePartition(...)
// Source condition (window.go ~145):
//   if len(ws.Partitions) == 0 || !ws.samePartition(row, ws.Partitions[...].Rows[0])
// Outcome: new partition is created
// Cases:
//   A=T, B=* → outcome=true  (no partitions yet → create first)
//   A=F, B=T → outcome=true  (existing partition but different key → create new)
//   A=F, B=F → outcome=false (same partition → append to existing)
// ============================================================

func TestMCDC_WindowAddRow_NoPartitions(t *testing.T) {
	t.Parallel()
	// A=true: no partitions exist yet → first partition created
	ws := NewWindowState([]int{0}, nil, nil, DefaultWindowFrame())
	row := []*Mem{NewMemInt(1), NewMemInt(10)}
	ws.AddRow(row)
	if len(ws.Partitions) != 1 {
		t.Errorf("Expected 1 partition after first AddRow, got %d", len(ws.Partitions))
	}
}

func TestMCDC_WindowAddRow_DifferentPartition(t *testing.T) {
	t.Parallel()
	// A=false (partitions exist), B=true (different partition key) → new partition
	ws := NewWindowState([]int{0}, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(1), NewMemInt(10)}
	row2 := []*Mem{NewMemInt(2), NewMemInt(20)} // different partition key (col 0)
	ws.AddRow(row1)
	ws.AddRow(row2)
	if len(ws.Partitions) != 2 {
		t.Errorf("Expected 2 partitions for different keys, got %d", len(ws.Partitions))
	}
}

func TestMCDC_WindowAddRow_SamePartition(t *testing.T) {
	t.Parallel()
	// A=false, B=false (same partition key) → append to existing partition
	ws := NewWindowState([]int{0}, nil, nil, DefaultWindowFrame())
	row1 := []*Mem{NewMemInt(1), NewMemInt(10)}
	row2 := []*Mem{NewMemInt(1), NewMemInt(20)} // same partition key (col 0 = 1)
	ws.AddRow(row1)
	ws.AddRow(row2)
	if len(ws.Partitions) != 1 {
		t.Errorf("Expected 1 partition for same key, got %d", len(ws.Partitions))
	}
	if len(ws.Partitions[0].Rows) != 2 {
		t.Errorf("Expected 2 rows in partition, got %d", len(ws.Partitions[0].Rows))
	}
}

// ============================================================
// window.go: GetFrameRows – start > end || start >= len(partition.Rows)
// Source condition (window.go ~297): if start > end || start >= len(partition.Rows)
// Outcome: returns nil (empty frame)
// Cases:
//   A=T, B=* → outcome=true  (start > end → nil)
//   A=F, B=T → outcome=true  (start >= len(rows) → nil)
//   A=F, B=F → outcome=false (valid frame)
// ============================================================

func buildWindowStateWithRows(numRows int) *WindowState {
	ws := NewWindowState(nil, nil, nil, DefaultWindowFrame())
	for i := 0; i < numRows; i++ {
		row := []*Mem{NewMemInt(int64(i + 1))}
		ws.AddRow(row)
	}
	// Advance to first row so CurrentPartIdx and CurrentPartRow are set
	ws.NextRow()
	return ws
}

func TestMCDC_GetFrameRows_StartGreaterThanEnd(t *testing.T) {
	t.Parallel()
	// A=true: force start > end by setting frame
	ws := buildWindowStateWithRows(3)
	// Manually set FrameStart > FrameEnd
	ws.Partitions[0].FrameStart = 2
	ws.Partitions[0].FrameEnd = 0
	result := ws.GetFrameRows()
	if result != nil {
		t.Errorf("Expected nil when start > end, got %v", result)
	}
}

func TestMCDC_GetFrameRows_StartOutOfBounds(t *testing.T) {
	t.Parallel()
	// A=false (start <= end), B=true (start >= len(rows))
	ws := buildWindowStateWithRows(2)
	// Set start beyond the partition rows
	ws.Partitions[0].FrameStart = 10
	ws.Partitions[0].FrameEnd = 15
	result := ws.GetFrameRows()
	if result != nil {
		t.Errorf("Expected nil when start >= len(rows), got %v", result)
	}
}

func TestMCDC_GetFrameRows_ValidFrame(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid frame → returns rows
	ws := buildWindowStateWithRows(3)
	ws.Partitions[0].FrameStart = 0
	ws.Partitions[0].FrameEnd = 1
	result := ws.GetFrameRows()
	if result == nil {
		t.Error("Expected non-nil frame rows for valid start/end")
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 frame rows, got %d", len(result))
	}
}

// ============================================================
// functions.go: connStateFuncLastInsertRowID – v.Ctx != nil && v.Ctx.ConnState != nil
// Source condition (functions.go ~179):
//   if v.Ctx != nil && v.Ctx.ConnState != nil
// Outcome: uses ConnState.LastInsertRowID() vs v.LastInsertID fallback
// Cases:
//   A=F, B=* → outcome=false (Ctx nil → use VDBE fallback)
//   A=T, B=F → outcome=false (ConnState nil → use VDBE fallback)
//   A=T, B=T → outcome=true  (ConnState provided → use it)
// ============================================================

// mockConnState implements ConnStateProvider for testing
type mockConnState struct {
	lastInsertRowID int64
	changes         int64
	totalChanges    int64
}

func (m *mockConnState) LastInsertRowID() int64 { return m.lastInsertRowID }
func (m *mockConnState) Changes() int64         { return m.changes }
func (m *mockConnState) TotalChanges() int64    { return m.totalChanges }

func TestMCDC_ConnStateFunc_CtxNil(t *testing.T) {
	t.Parallel()
	// A=false: Ctx is nil → falls back to v.LastInsertID
	v := NewTestVDBE(5)
	v.Ctx = nil
	v.LastInsertID = 42

	result := v.connStateFuncLastInsertRowID()
	if result.IntValue() != 42 {
		t.Errorf("Expected fallback LastInsertID=42, got %d", result.IntValue())
	}
}

func TestMCDC_ConnStateFunc_ConnStateNil(t *testing.T) {
	t.Parallel()
	// A=true (Ctx non-nil), B=false (ConnState nil) → fallback
	v := NewTestVDBE(5)
	v.Ctx = &VDBEContext{ConnState: nil}
	v.LastInsertID = 99

	result := v.connStateFuncLastInsertRowID()
	if result.IntValue() != 99 {
		t.Errorf("Expected fallback LastInsertID=99, got %d", result.IntValue())
	}
}

func TestMCDC_ConnStateFunc_ConnStateProvided(t *testing.T) {
	t.Parallel()
	// A=true, B=true → uses ConnState
	v := NewTestVDBE(5)
	v.LastInsertID = 1 // fallback value, should be ignored
	v.Ctx = &VDBEContext{
		ConnState: &mockConnState{lastInsertRowID: 777},
	}

	result := v.connStateFuncLastInsertRowID()
	if result.IntValue() != 777 {
		t.Errorf("Expected ConnState.LastInsertRowID()=777, got %d", result.IntValue())
	}
}

// ============================================================
// functions.go: connStateFuncChanges – v.Ctx != nil && v.Ctx.ConnState != nil
// Same structure as LastInsertRowID but for Changes().
// Cases:
//   A=F, B=* → fallback NumChanges
//   A=T, B=F → fallback NumChanges
//   A=T, B=T → ConnState.Changes()
// ============================================================

func TestMCDC_ConnStateFuncChanges_CtxNil(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Ctx = nil
	v.NumChanges = 5

	result := v.connStateFuncChanges()
	if result.IntValue() != 5 {
		t.Errorf("Expected fallback NumChanges=5, got %d", result.IntValue())
	}
}

func TestMCDC_ConnStateFuncChanges_ConnStateNil(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.Ctx = &VDBEContext{ConnState: nil}
	v.NumChanges = 12

	result := v.connStateFuncChanges()
	if result.IntValue() != 12 {
		t.Errorf("Expected fallback NumChanges=12, got %d", result.IntValue())
	}
}

func TestMCDC_ConnStateFuncChanges_ConnStateProvided(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(5)
	v.NumChanges = 0 // fallback
	v.Ctx = &VDBEContext{
		ConnState: &mockConnState{changes: 100},
	}

	result := v.connStateFuncChanges()
	if result.IntValue() != 100 {
		t.Errorf("Expected ConnState.Changes()=100, got %d", result.IntValue())
	}
}

// ============================================================
// mem.go: Remainder – m.IsNull() || other.IsNull() and m.IsInt() && other.IsInt()
// Source condition (mem.go ~855, 860):
//   if m.IsNull() || other.IsNull()
//   if m.IsInt()  && other.IsInt()
// ============================================================

func TestMCDC_MemRemainder_MIsNull(t *testing.T) {
	t.Parallel()
	// A=true (m is NULL) → result NULL
	m := NewMemNull()
	other := NewMemInt(3)
	_ = m.Remainder(other)
	if !m.IsNull() {
		t.Error("Expected NULL when m is NULL")
	}
}

func TestMCDC_MemRemainder_OtherIsNull(t *testing.T) {
	t.Parallel()
	// A=false, B=true (other is NULL) → result NULL
	m := NewMemInt(7)
	other := NewMemNull()
	_ = m.Remainder(other)
	if !m.IsNull() {
		t.Error("Expected NULL when other is NULL")
	}
}

func TestMCDC_MemRemainder_BothIntegerPath(t *testing.T) {
	t.Parallel()
	// Both non-null, A=true, B=true (IsInt&&IsInt) → integer modulo
	m := NewMemInt(10)
	other := NewMemInt(3)
	_ = m.Remainder(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	if m.IntValue() != 1 {
		t.Errorf("Expected 10%%3=1, got %d", m.IntValue())
	}
}

func TestMCDC_MemRemainder_FloatPath(t *testing.T) {
	t.Parallel()
	// Both non-null, A=true (IsInt=false), B=true (IsInt=false) → real modulo
	m := NewMemReal(10.5)
	other := NewMemReal(3.0)
	_ = m.Remainder(other)
	if m.IsNull() {
		t.Fatal("Unexpected NULL")
	}
	if !m.IsReal() {
		t.Error("Expected real result for float modulo")
	}
}

// ============================================================
// mem.go: Multiply overflow check – m.i == 0 || result/m.i == other.i
// Source condition (mem.go ~810): if m.i == 0 || result/m.i == other.i
// Outcome: no overflow → stay in integer path
// Cases:
//   A=T, B=* → outcome=true  (m is 0 → result is 0, no overflow)
//   A=F, B=T → outcome=true  (m non-zero, result divides back correctly)
//   A=F, B=F → outcome=false (overflow → fall through to real multiply)
// ============================================================

func TestMCDC_MemMultiply_MIsZero(t *testing.T) {
	t.Parallel()
	// A=true: m is 0 → result is 0, stays integer
	m := NewMemInt(0)
	other := NewMemInt(math.MaxInt64)
	_ = m.Multiply(other)
	if !m.IsInt() {
		t.Error("Expected integer result when m=0")
	}
	if m.IntValue() != 0 {
		t.Errorf("Expected 0*MaxInt64=0, got %d", m.IntValue())
	}
}

func TestMCDC_MemMultiply_NoOverflow(t *testing.T) {
	t.Parallel()
	// A=false, B=true: m non-zero, result divides back → no overflow
	m := NewMemInt(6)
	other := NewMemInt(7)
	_ = m.Multiply(other)
	if !m.IsInt() {
		t.Error("Expected integer result for 6*7")
	}
	if m.IntValue() != 42 {
		t.Errorf("Expected 42, got %d", m.IntValue())
	}
}

func TestMCDC_MemMultiply_Overflow(t *testing.T) {
	t.Parallel()
	// A=false, B=false: overflow → falls to real multiply
	m := NewMemInt(math.MaxInt64)
	other := NewMemInt(2)
	_ = m.Multiply(other)
	// Result overflows int64: falls through to real path
	if !m.IsReal() {
		t.Error("Expected real result on integer overflow in multiply")
	}
}
