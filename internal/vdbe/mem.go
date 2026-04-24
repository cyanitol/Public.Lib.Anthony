// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
)

// MemFlags represents the type and state flags for a Mem structure.
type MemFlags uint16

// Memory cell type flags - these indicate what type of value is stored.
const (
	MemUndefined MemFlags = 0x0000 // Value is undefined
	MemNull      MemFlags = 0x0001 // Value is NULL
	MemStr       MemFlags = 0x0002 // Value is a string
	MemInt       MemFlags = 0x0004 // Value is an integer
	MemReal      MemFlags = 0x0008 // Value is a real number
	MemBlob      MemFlags = 0x0010 // Value is a BLOB
	MemIntReal   MemFlags = 0x0020 // Integer that stringifies like real
	MemAffMask   MemFlags = 0x003f // Mask of affinity bits

	// Extra modifier flags
	MemFromBind MemFlags = 0x0040 // Value originates from sqlite3_bind()
	MemCleared  MemFlags = 0x0100 // NULL set by OP_Null
	MemTerm     MemFlags = 0x0200 // String is zero-terminated
	MemZero     MemFlags = 0x0400 // Mem.i contains count of 0s appended
	MemSubtype  MemFlags = 0x0800 // Subtype is valid
	MemTypeMask MemFlags = 0x0dbf // Mask of type bits

	// Storage type flags for string/blob data
	MemDyn    MemFlags = 0x1000 // Need to call destructor on z
	MemStatic MemFlags = 0x2000 // z points to static string
	MemEphem  MemFlags = 0x4000 // z points to ephemeral string
	MemAgg    MemFlags = 0x8000 // z points to aggregate context
)

// Mem represents a memory cell in the VDBE.
// It can hold various types of values: NULL, integer, real, string, or blob.
type Mem struct {
	// Value storage - only one of these is valid based on flags
	i int64   // Integer value (MEM_Int)
	r float64 // Real value (MEM_Real)
	z []byte  // String or blob value (MEM_Str, MEM_Blob)

	// Metadata
	flags   MemFlags // Type and state flags
	n       int      // Number of characters in string (excluding null terminator)
	subtype uint8    // Subtype value (if MEM_Subtype flag set)

	// For MEM_Zero: number of zero bytes to append to blob
	nZero int

	// Destructor function for dynamic memory (if MEM_Dyn flag set)
	// In Go, we typically don't need this as GC handles cleanup,
	// but we keep it for compatibility with the C implementation
	xDel func(interface{})
}

// NewMem creates a new undefined memory cell.
func NewMem() *Mem {
	return &Mem{
		flags: MemUndefined,
	}
}

// NewMemNull creates a new NULL memory cell.
func NewMemNull() *Mem {
	return &Mem{
		flags: MemNull,
	}
}

// NewMemInt creates a new integer memory cell.
func NewMemInt(val int64) *Mem {
	return &Mem{
		flags: MemInt,
		i:     val,
	}
}

// NewMemReal creates a new real memory cell.
func NewMemReal(val float64) *Mem {
	return &Mem{
		flags: MemReal,
		r:     val,
	}
}

// NewMemStr creates a new string memory cell.
func NewMemStr(val string) *Mem {
	data := []byte(val)
	return &Mem{
		flags: MemStr | MemTerm,
		z:     data,
		n:     len(data),
	}
}

// NewMemBlob creates a new blob memory cell.
func NewMemBlob(val []byte) *Mem {
	return &Mem{
		flags: MemBlob,
		z:     val,
		n:     len(val),
	}
}

// IsNull returns true if the memory cell contains NULL.
func (m *Mem) IsNull() bool {
	return m.flags&MemNull != 0
}

// IsInt returns true if the memory cell contains an integer.
func (m *Mem) IsInt() bool {
	return m.flags&MemInt != 0
}

// IsReal returns true if the memory cell contains a real number.
func (m *Mem) IsReal() bool {
	return m.flags&MemReal != 0
}

// IsStr returns true if the memory cell contains a string.
func (m *Mem) IsStr() bool {
	return m.flags&MemStr != 0
}

// IsString returns true if the memory cell contains a string.
// This is an alias for IsStr() provided for compatibility.
func (m *Mem) IsString() bool {
	return m.flags&MemStr != 0
}

// IsBlob returns true if the memory cell contains a blob.
func (m *Mem) IsBlob() bool {
	return m.flags&MemBlob != 0
}

// IsNumeric returns true if the memory cell contains a numeric value.
func (m *Mem) IsNumeric() bool {
	return m.flags&(MemInt|MemReal|MemIntReal) != 0
}

// GetFlags returns the flags for the memory cell.
func (m *Mem) GetFlags() MemFlags {
	return m.flags
}

// SetNull sets the memory cell to NULL.
func (m *Mem) SetNull() {
	m.release()
	m.flags = MemNull
	m.i = 0
	m.r = 0
	m.z = nil
	m.n = 0
}

// SetInt sets the memory cell to an integer value.
func (m *Mem) SetInt(val int64) {
	m.release()
	m.flags = MemInt
	m.i = val
}

// SetReal sets the memory cell to a real value.
func (m *Mem) SetReal(val float64) {
	m.release()
	m.flags = MemReal
	m.r = val
}

// SetStr sets the memory cell to a string value.
func (m *Mem) SetStr(val string) error {
	m.release()
	data := []byte(val)
	m.flags = MemStr | MemTerm
	m.z = data
	m.n = len(data)
	return nil
}

// SetBlob sets the memory cell to a blob value.
func (m *Mem) SetBlob(val []byte) error {
	m.release()
	m.flags = MemBlob
	m.z = make([]byte, len(val))
	copy(m.z, val)
	m.n = len(val)
	return nil
}

// IntValue returns the integer value of the memory cell.
// If the cell doesn't contain an integer, it attempts to convert it.
func (m *Mem) IntValue() int64 {
	if m.flags&MemInt != 0 {
		return m.i
	}
	if m.flags&MemReal != 0 {
		return int64(m.r)
	}
	if m.flags&(MemStr|MemBlob) != 0 {
		s := string(m.z)
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		// SQLite extracts leading numeric prefix from strings like "5abc"
		if prefix := extractLeadingNumeric(s); prefix != "" {
			if val, err := strconv.ParseFloat(prefix, 64); err == nil {
				return int64(val)
			}
		}
	}
	return 0
}

// RealValue returns the real value of the memory cell.
// If the cell doesn't contain a real number, it attempts to convert it.
func (m *Mem) RealValue() float64 {
	if m.flags&MemReal != 0 {
		return m.r
	}
	if m.flags&MemInt != 0 {
		return float64(m.i)
	}
	if m.flags&(MemStr|MemBlob) != 0 {
		s := string(m.z)
		if val, err := strconv.ParseFloat(s, 64); err == nil {
			return val
		}
		// SQLite extracts leading numeric prefix from strings like "5abc"
		if prefix := extractLeadingNumeric(s); prefix != "" {
			if val, err := strconv.ParseFloat(prefix, 64); err == nil {
				return val
			}
		}
	}
	return 0.0
}

// StrValue returns the string value of the memory cell.
// If the cell doesn't contain a string, it attempts to convert it.
func (m *Mem) StrValue() string {
	if m.flags&MemStr != 0 {
		return string(m.z)
	}
	if m.flags&MemBlob != 0 {
		return string(m.z)
	}
	if m.flags&MemInt != 0 {
		return strconv.FormatInt(m.i, 10)
	}
	if m.flags&MemReal != 0 {
		return strconv.FormatFloat(m.r, 'g', -1, 64)
	}
	if m.flags&MemNull != 0 {
		return ""
	}
	return ""
}

// StringValue returns the string value of the memory cell.
// This is an alias for StrValue() provided for compatibility.
func (m *Mem) StringValue() string {
	return m.StrValue()
}

// BlobValue returns the blob value of the memory cell.
func (m *Mem) BlobValue() []byte {
	if m.flags&MemBlob != 0 {
		return m.z
	}
	if m.flags&MemStr != 0 {
		return m.z
	}
	return nil
}

// Value returns the value as an interface{}.
// This provides a generic way to access the stored value.
func (m *Mem) Value() interface{} {
	if m.flags&MemNull != 0 {
		return nil
	}
	if m.flags&MemInt != 0 {
		return m.i
	}
	if m.flags&MemReal != 0 {
		return m.r
	}
	if m.flags&MemStr != 0 {
		return string(m.z)
	}
	if m.flags&MemBlob != 0 {
		return m.z
	}
	return nil
}

// Stringify converts the memory cell to a string representation.
// This modifies the cell to have the MEM_Str flag set.
func (m *Mem) Stringify() error {
	if m.flags&MemStr != 0 {
		return nil // Already a string
	}

	var str string
	if m.flags&MemInt != 0 {
		str = strconv.FormatInt(m.i, 10)
	} else if m.flags&MemReal != 0 {
		str = strconv.FormatFloat(m.r, 'g', -1, 64)
	} else if m.flags&MemBlob != 0 {
		// Blob to string conversion
		str = string(m.z)
	} else if m.flags&MemNull != 0 {
		str = ""
	} else {
		return fmt.Errorf("cannot stringify undefined value")
	}

	m.z = []byte(str)
	m.n = len(str)
	// Clear affinity flags first, then set string flags
	m.flags = (m.flags &^ MemAffMask) | MemStr | MemTerm
	return nil
}

// setAffinityInt sets the integer affinity, clearing other type flags.
func (m *Mem) setAffinityInt(val int64) {
	m.i = val
	m.flags = (m.flags &^ MemAffMask) | MemInt
}

// setAffinityReal sets the real affinity, clearing other type flags.
func (m *Mem) setAffinityReal(val float64) {
	m.r = val
	m.flags = (m.flags &^ MemAffMask) | MemReal
}

// Integerify converts the memory cell to an integer.
// This modifies the cell to have the MEM_Int flag set.
func (m *Mem) Integerify() error {
	if m.flags&MemInt != 0 {
		return nil // Already an integer
	}

	if m.flags&MemReal != 0 {
		m.setAffinityInt(int64(m.r))
		return nil
	}

	if m.flags&(MemStr|MemBlob) != 0 {
		val, err := strconv.ParseInt(string(m.z), 10, 64)
		if err != nil {
			// Try parsing as float first, then convert to int
			if fval, ferr := strconv.ParseFloat(string(m.z), 64); ferr == nil {
				m.setAffinityInt(int64(fval))
				return nil
			}
			return fmt.Errorf("cannot convert to integer: %w", err)
		}
		m.setAffinityInt(val)
		return nil
	}

	if m.flags&MemNull != 0 {
		m.setAffinityInt(0)
		return nil
	}

	return fmt.Errorf("cannot integerify value")
}

// Realify converts the memory cell to a real number.
// This modifies the cell to have the MEM_Real flag set.
func (m *Mem) Realify() error {
	if m.flags&MemReal != 0 {
		return nil // Already a real
	}

	if m.flags&MemInt != 0 {
		m.setAffinityReal(float64(m.i))
		return nil
	}

	if m.flags&(MemStr|MemBlob) != 0 {
		val, err := strconv.ParseFloat(string(m.z), 64)
		if err != nil {
			return fmt.Errorf("cannot convert to real: %w", err)
		}
		m.setAffinityReal(val)
		return nil
	}

	if m.flags&MemNull != 0 {
		m.setAffinityReal(0.0)
		return nil
	}

	return fmt.Errorf("cannot realify value")
}

// Numerify attempts to convert the value to numeric (int or real).
func (m *Mem) Numerify() error {
	if m.flags&MemNull != 0 {
		m.i = 0
		m.flags = MemInt
		return nil
	}

	if m.IsNumeric() {
		return nil
	}

	if m.flags&(MemStr|MemBlob) != 0 {
		// Try integer first
		if val, err := strconv.ParseInt(string(m.z), 10, 64); err == nil {
			m.i = val
			m.flags |= MemInt
			return nil
		}
		// Try real
		if val, err := strconv.ParseFloat(string(m.z), 64); err == nil {
			m.r = val
			m.flags |= MemReal
			return nil
		}
	}

	return fmt.Errorf("cannot numerify value")
}

// Copy makes a deep copy of the source memory cell into this cell.
func (m *Mem) Copy(src *Mem) error {
	m.release()

	m.flags = src.flags
	m.i = src.i
	m.r = src.r
	m.n = src.n
	m.nZero = src.nZero
	m.subtype = src.subtype

	// Deep copy the byte slice
	if src.z != nil {
		m.z = make([]byte, len(src.z))
		copy(m.z, src.z)
	} else {
		m.z = nil
	}

	// Clear dynamic flags since we made a copy
	m.flags &= ^(MemDyn | MemEphem | MemStatic)

	return nil
}

// ShallowCopy performs a shallow copy (doesn't duplicate string/blob data).
func (m *Mem) ShallowCopy(src *Mem) {
	m.flags = src.flags
	m.i = src.i
	m.r = src.r
	m.z = src.z // Shallow copy - shares the same byte slice
	m.n = src.n
	m.nZero = src.nZero
	m.subtype = src.subtype

	// Mark as ephemeral since we're sharing data
	if src.flags&(MemStr|MemBlob) != 0 {
		m.flags = (m.flags & ^(MemDyn | MemStatic)) | MemEphem
	}
}

// Move transfers the value from source to this cell, leaving source as undefined.
func (m *Mem) Move(src *Mem) {
	m.release()

	m.flags = src.flags
	m.i = src.i
	m.r = src.r
	m.z = src.z
	m.n = src.n
	m.nZero = src.nZero
	m.subtype = src.subtype
	m.xDel = src.xDel

	// Clear the source
	src.flags = MemUndefined
	src.z = nil
	src.xDel = nil
}

func compareNulls(m, other *Mem) (int, bool) {
	mNull := m.IsNull()
	oNull := other.IsNull()
	if !mNull && !oNull {
		return 0, false
	}
	if mNull && oNull {
		return 0, true
	}
	if mNull {
		return -1, true
	}
	return 1, true
}

func compareNumeric(m, other *Mem) int {
	v1, v2 := m.RealValue(), other.RealValue()
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func compareStrings(m, other *Mem) int {
	return compareStringsWithCollation(m, other, "")
}

func compareStringsWithCollation(m, other *Mem, collName string) int {
	return compareStringsWithCollationRegistry(m, other, collName, nil)
}

func compareStringsWithCollationRegistry(m, other *Mem, collName string, registry interface{}) int {
	s1, s2 := m.StrValue(), other.StrValue()

	// Use specified collation, or default to BINARY
	if collName == "" {
		collName = collation.DefaultCollation()
	}

	// Try connection-specific registry first (if provided)
	if registry != nil {
		if reg, ok := registry.(*collation.CollationRegistry); ok {
			if coll, found := reg.Get(collName); found {
				return coll.Func(s1, s2)
			}
		}
	}

	// Fall back to global registry
	return collation.Compare(s1, s2, collName)
}

func (m *Mem) Compare(other *Mem) int {
	return m.CompareWithCollation(other, "")
}

// compareNumericWithText tries to compare a numeric value with text.
// Returns the comparison result and true if comparison was successful,
// or 0 and false if text cannot be parsed as a number.
func compareNumericWithText(numericVal float64, textBytes []byte) (int, bool) {
	textVal, err := strconv.ParseFloat(string(textBytes), 64)
	if err != nil {
		// Text cannot be parsed as numeric
		return 0, false
	}
	// Compare numerically
	if numericVal < textVal {
		return -1, true
	}
	if numericVal > textVal {
		return 1, true
	}
	return 0, true
}

// compareMixedNumericText handles comparison when one value is numeric and the other is text.
func compareMixedNumericText(m, other *Mem, mIsNumeric, mIsText bool) (int, bool) {
	if mIsNumeric && !mIsText {
		// m is numeric, other is text
		if result, ok := compareNumericWithText(m.RealValue(), other.z); ok {
			return result, true
		}
		// Text cannot be numeric, numeric < text
		return -1, true
	}

	if !mIsNumeric && mIsText {
		// m is text, other is numeric
		if result, ok := compareNumericWithText(other.RealValue(), m.z); ok {
			return -result, true // Invert result since we compared in reverse order
		}
		// Text cannot be numeric, numeric < text
		return 1, true
	}

	return 0, false
}

// CompareWithCollation compares two memory cells using the specified collation.
// The collation is only used for string comparisons.
// If collation is empty, BINARY collation is used.
//
// This follows SQLite's type affinity comparison rules:
// 1. NULL is less than everything else
// 2. If both are numeric (INT or REAL), compare numerically
// 3. If one is numeric and one is text, try to convert text to numeric:
//   - If successful, compare numerically
//   - If unsuccessful, numeric < text (return -1 or 1)
//
// 4. If both are text, compare using collation
// 5. If one is text and one is blob, text < blob
func (m *Mem) CompareWithCollation(other *Mem, collName string) int {
	return m.CompareWithCollationRegistry(other, collName, nil)
}

// shouldCompareNumeric returns true if both values are numeric.
func shouldCompareNumeric(mIsNumeric, otherIsNumeric bool) bool {
	return mIsNumeric && otherIsNumeric
}

// shouldCompareMixed returns true if one value is numeric and the other is text.
func shouldCompareMixed(mIsNumeric, otherIsNumeric, mIsText, otherIsText bool) bool {
	return (mIsNumeric && otherIsText) || (otherIsNumeric && mIsText)
}

// CompareWithCollationRegistry compares two memory cells using the specified collation,
// with support for connection-specific collation registries.
// The registry parameter allows using custom collations registered on a specific connection.
func (m *Mem) CompareWithCollationRegistry(other *Mem, collName string, registry interface{}) int {
	// Handle NULL comparisons
	if result, handled := compareNulls(m, other); handled {
		return result
	}

	mIsNumeric := m.IsNumeric()
	otherIsNumeric := other.IsNumeric()
	mIsText := (m.flags & MemStr) != 0
	otherIsText := (other.flags & MemStr) != 0

	// Both numeric: numeric comparison
	if shouldCompareNumeric(mIsNumeric, otherIsNumeric) {
		return compareNumeric(m, other)
	}

	// One numeric, one text: try to convert text to numeric
	if shouldCompareMixed(mIsNumeric, otherIsNumeric, mIsText, otherIsText) {
		if result, handled := compareMixedNumericText(m, other, mIsNumeric, mIsText); handled {
			return result
		}
	}

	// Both text or mixed text/blob: string comparison
	return compareStringsWithCollationRegistry(m, other, collName, registry)
}

// CompareWithDirection compares two memory cells with a sort direction.
// direction: 0 = ASC (ascending), 1 = DESC (descending)
// Returns: -1 if m < other, 0 if m == other, 1 if m > other (accounting for direction)
func (m *Mem) CompareWithDirection(other *Mem, direction int) int {
	cmp := m.Compare(other)
	// If DESC (direction = 1), invert the comparison result
	if direction == 1 {
		return -cmp
	}
	return cmp
}

// release releases any dynamic memory associated with this cell.
func (m *Mem) release() {
	if m.flags&MemDyn != 0 && m.xDel != nil {
		m.xDel(m.z)
	}
	m.z = nil
	m.xDel = nil
	m.flags &= ^(MemDyn | MemStatic | MemEphem)
}

// Release releases any resources held by this memory cell.
func (m *Mem) Release() {
	m.release()
	m.flags = MemUndefined
	m.i = 0
	m.r = 0
	m.n = 0
	m.nZero = 0
}

// String returns a string representation of the memory cell for debugging.
func (m *Mem) String() string {
	if m.flags&MemNull != 0 {
		return "NULL"
	}
	if m.flags&MemInt != 0 {
		return fmt.Sprintf("INT(%d)", m.i)
	}
	if m.flags&MemReal != 0 {
		return fmt.Sprintf("REAL(%g)", m.r)
	}
	if m.flags&MemStr != 0 {
		return fmt.Sprintf("STR(%q)", string(m.z))
	}
	if m.flags&MemBlob != 0 {
		return fmt.Sprintf("BLOB(%d bytes)", len(m.z))
	}
	return "UNDEFINED"
}

// ApplyAffinity applies type affinity to the memory cell.
func (m *Mem) ApplyAffinity(affinity byte) error {
	switch affinity {
	case 'A': // NONE/BLOB - no conversion
		return nil
	case 'B': // TEXT
		return m.Stringify()
	case 'C': // NUMERIC
		return m.Numerify()
	case 'D': // INTEGER
		return m.Integerify()
	case 'E': // REAL
		return m.Realify()
	default:
		return nil
	}
}

// extractLeadingNumeric returns the leading numeric prefix of s.
// For example, "5abc" → "5", "3.14xyz" → "3.14", "abc" → "".
func extractLeadingNumeric(s string) string {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	start := i
	i = skipDigits(s, i)
	if i < len(s) && s[i] == '.' {
		i = skipDigits(s, i+1)
	}
	if i == start {
		return ""
	}
	return s[:i]
}

// skipDigits advances past consecutive ASCII digits.
func skipDigits(s string, i int) int {
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return i
}

// Add adds two memory cells (this = this + other).
func (m *Mem) Add(other *Mem) error {
	if m.IsNull() || other.IsNull() {
		m.SetNull()
		return nil
	}

	// If both are integers and won't overflow
	if m.IsInt() && other.IsInt() {
		// Check for overflow
		result := m.i + other.i
		if (result > m.i) == (other.i > 0) { // No overflow
			m.i = result
			return nil
		}
		// Overflow, convert to real
	}

	// Convert to real and add
	v1 := m.RealValue()
	v2 := other.RealValue()
	m.SetReal(v1 + v2)
	return nil
}

// Subtract subtracts other from this cell (this = this - other).
func (m *Mem) Subtract(other *Mem) error {
	if m.IsNull() || other.IsNull() {
		m.SetNull()
		return nil
	}

	if m.IsInt() && other.IsInt() {
		result := m.i - other.i
		if (result < m.i) == (other.i > 0) { // No overflow
			m.i = result
			return nil
		}
	}

	v1 := m.RealValue()
	v2 := other.RealValue()
	m.SetReal(v1 - v2)
	return nil
}

// Multiply multiplies two memory cells (this = this * other).
func (m *Mem) Multiply(other *Mem) error {
	if m.IsNull() || other.IsNull() {
		m.SetNull()
		return nil
	}

	if m.IsInt() && other.IsInt() {
		// Check for overflow using int64 arithmetic
		result := m.i * other.i
		if m.i == 0 || result/m.i == other.i { // No overflow
			m.i = result
			return nil
		}
	}

	v1 := m.RealValue()
	v2 := other.RealValue()
	m.SetReal(v1 * v2)
	return nil
}

// Divide divides this cell by other (this = this / other).
func (m *Mem) Divide(other *Mem) error {
	if m.IsNull() || other.IsNull() {
		m.SetNull()
		return nil
	}
	if m.IsInt() && other.IsInt() {
		return m.divideInt(other)
	}
	return m.divideReal(other.RealValue())
}

func (m *Mem) divideInt(other *Mem) error {
	if other.i == 0 {
		m.SetNull()
		return nil
	}
	if m.i == math.MinInt64 && other.i == -1 {
		m.SetReal(float64(m.i) / float64(other.i))
		return nil
	}
	m.i /= other.i
	return nil
}

func (m *Mem) divideReal(divisor float64) error {
	if divisor == 0.0 {
		m.SetNull()
		return nil
	}
	m.SetReal(m.RealValue() / divisor)
	return nil
}

// Remainder computes the remainder (this = this % other).
func (m *Mem) Remainder(other *Mem) error {
	if m.IsNull() || other.IsNull() {
		m.SetNull()
		return nil
	}

	if m.IsInt() && other.IsInt() {
		if other.i == 0 {
			m.SetNull()
			return nil
		}
		m.i = m.i % other.i
		return nil
	}

	v1 := m.RealValue()
	v2 := other.RealValue()
	if v2 == 0.0 {
		m.SetNull()
		return nil
	}
	m.SetReal(math.Mod(v1, v2))
	return nil
}

// ToDistinctKey converts a Mem value to a string key for DISTINCT deduplication.
// This ensures different types with the same logical value are treated distinctly.
func (m *Mem) ToDistinctKey() string {
	return m.ToDistinctKeyWithCollation("", nil)
}

// ToDistinctKeyWithCollation converts a Mem value to a distinct key using collation rules.
func (m *Mem) ToDistinctKeyWithCollation(collName string, registry interface{}) string {
	if m.IsNull() {
		return "NULL"
	}
	if m.IsInt() {
		return fmt.Sprintf("I:%d", m.i)
	}
	if m.IsReal() {
		return fmt.Sprintf("R:%g", m.r)
	}
	if m.IsStr() {
		s := string(m.z)
		if collName != "" {
			s = normalizeDistinctText(s, collName, registry)
		}
		return fmt.Sprintf("S:%s", s)
	}
	if m.IsBlob() {
		return fmt.Sprintf("B:%x", m.z)
	}
	return "UNDEFINED"
}

func normalizeDistinctText(s, collName string, registry interface{}) string {
	coll := strings.ToUpper(collName)
	switch coll {
	case "NOCASE":
		return strings.ToLower(s)
	case "RTRIM":
		return strings.TrimRight(s, " ")
	default:
		return s
	}
}
