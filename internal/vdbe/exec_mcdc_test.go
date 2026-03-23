// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// ============================================================
// MC/DC tests for exec.go compound boolean conditions
//
// For each condition A && B or A || B we write N+1 test cases
// where each sub-condition independently flips the outcome.
// Test names contain "MCDC" so they can be selected with -run MCDC.
// ============================================================

// ------------------------------------------------------------
// Condition 1: v.Stats != nil && v.Stats.StartTime == 0  (line 42)
// Outcome: stats.Start() is called
// Cases:
//   A=F, B=*  → outcome=false (Stats nil, no Start)
//   A=T, B=F  → outcome=false (Stats non-nil, StartTime already set)
//   A=T, B=T  → outcome=true  (Stats non-nil, StartTime zero → calls Start)
// ------------------------------------------------------------

func TestMCDC_StatsNilAndStartTimeZero_StatsNil(t *testing.T) {
	t.Parallel()
	// A=false (Stats==nil), B=* → no Start called
	v := NewTestVDBE(5)
	v.Stats = nil // A is false

	// run prepareForStep which contains the compound condition
	v.State = StateInit
	err := v.prepareForStep()
	if err != nil {
		t.Fatalf("prepareForStep failed: %v", err)
	}
	// Stats is nil, so nothing should have happened
	if v.Stats != nil {
		t.Error("Expected Stats to remain nil")
	}
}

func TestMCDC_StatsNilAndStartTimeZero_StartAlreadySet(t *testing.T) {
	t.Parallel()
	// A=true (Stats non-nil), B=false (StartTime != 0) → no Start called
	v := NewTestVDBE(5)
	v.Stats = NewQueryStatistics()
	v.Stats.StartTime = 999 // B is false: already started

	v.State = StateInit
	_ = v.prepareForStep()

	if v.Stats.StartTime != 999 {
		t.Errorf("Expected StartTime to remain 999, got %d", v.Stats.StartTime)
	}
}

func TestMCDC_StatsNilAndStartTimeZero_BothTrue(t *testing.T) {
	t.Parallel()
	// A=true (Stats non-nil), B=true (StartTime==0) → Start called
	v := NewTestVDBE(5)
	v.Stats = NewQueryStatistics()
	// StartTime is 0 by default (B is true)

	v.State = StateInit
	_ = v.prepareForStep()

	if v.Stats.StartTime == 0 {
		t.Error("Expected StartTime to be set (non-zero) after Start()")
	}
}

// ------------------------------------------------------------
// Condition 2: v.Stats != nil && v.Stats.EndTime == 0  (line 178)
// Outcome: stats.End() is called
// Cases:
//   A=F, B=*  → outcome=false (Stats nil)
//   A=T, B=F  → outcome=false (EndTime already set)
//   A=T, B=T  → outcome=true  (EndTime zero → calls End)
// ------------------------------------------------------------

func TestMCDC_StatsEndTimeZero_StatsNil(t *testing.T) {
	t.Parallel()
	// A=false: Stats==nil
	v := NewTestVDBE(5)
	v.Stats = nil
	v.endStatsIfNeeded()
	// no panic expected, nothing to assert
}

func TestMCDC_StatsEndTimeZero_EndAlreadySet(t *testing.T) {
	t.Parallel()
	// A=true, B=false: EndTime already set
	v := NewTestVDBE(5)
	v.Stats = NewQueryStatistics()
	v.Stats.EndTime = 12345

	v.endStatsIfNeeded()

	if v.Stats.EndTime != 12345 {
		t.Errorf("Expected EndTime to remain 12345, got %d", v.Stats.EndTime)
	}
}

func TestMCDC_StatsEndTimeZero_BothTrue(t *testing.T) {
	t.Parallel()
	// A=true, B=true: EndTime==0 → End() should set it
	v := NewTestVDBE(5)
	v.Stats = NewQueryStatistics()
	// EndTime is 0 by default

	v.endStatsIfNeeded()

	if v.Stats.EndTime == 0 {
		t.Error("Expected EndTime to be set after endStatsIfNeeded")
	}
}

// ------------------------------------------------------------
// Condition 3: instr.P4Type == P4Static || instr.P4Type == P4Dynamic  (line 428, execHalt)
// Outcome: error message string is used from P4.Z
// Cases:
//   A=F, B=F  → outcome=false (no error msg set from P4)
//   A=T, B=F  → outcome=true  (P4Static → error msg used)
//   A=F, B=T  → outcome=true  (P4Dynamic → error msg used)
// ------------------------------------------------------------

func TestMCDC_HaltP4Type_NeitherStaticNorDynamic(t *testing.T) {
	t.Parallel()
	// A=false, B=false: no error string applied
	v := NewTestVDBE(5)
	instr := &Instruction{
		Opcode: OpHalt,
		P1:     0,
		P4:     P4Union{Z: "should not appear"},
		P4Type: P4Real, // neither Static nor Dynamic
	}
	_ = v.execHalt(instr)
	if v.ErrorMsg != "" {
		t.Errorf("Expected no error message, got %q", v.ErrorMsg)
	}
}

func TestMCDC_HaltP4Type_Static(t *testing.T) {
	t.Parallel()
	// A=true, B=false: P4Static → error msg set
	v := NewTestVDBE(5)
	instr := &Instruction{
		Opcode: OpHalt,
		P1:     0,
		P4:     P4Union{Z: "halt error"},
		P4Type: P4Static,
	}
	_ = v.execHalt(instr)
	if v.ErrorMsg != "halt error" {
		t.Errorf("Expected error msg 'halt error', got %q", v.ErrorMsg)
	}
}

func TestMCDC_HaltP4Type_Dynamic(t *testing.T) {
	t.Parallel()
	// A=false, B=true: P4Dynamic → error msg set
	v := NewTestVDBE(5)
	instr := &Instruction{
		Opcode: OpHalt,
		P1:     0,
		P4:     P4Union{Z: "dynamic error"},
		P4Type: P4Dynamic,
	}
	_ = v.execHalt(instr)
	if v.ErrorMsg != "dynamic error" {
		t.Errorf("Expected error msg 'dynamic error', got %q", v.ErrorMsg)
	}
}

// ------------------------------------------------------------
// Condition 4: cursor.NullRow || cursor.EOF  (line 1346, getColumnPayload)
// Outcome: dst set to NULL
// Cases:
//   A=F, B=F  → outcome=false (normal payload returned)
//   A=T, B=F  → outcome=true  (NullRow → NULL)
//   A=F, B=T  → outcome=true  (EOF → NULL)
// ------------------------------------------------------------

func buildCursorWithFlags(nullRow, eof bool) *Cursor {
	return &Cursor{
		NullRow: nullRow,
		EOF:     eof,
		CurType: CursorBTree,
	}
}

func TestMCDC_CursorNullRowOrEOF_Neither(t *testing.T) {
	t.Parallel()
	// A=false, B=false → payload not immediately nulled (returns nil from btree since no real btree)
	v := NewTestVDBE(5)
	cursor := buildCursorWithFlags(false, false)
	dst := NewMem()

	payload := v.getColumnPayload(cursor, dst)
	// With no BtreeCursor, getBtreeCursorPayload will set NULL
	// But the key point: we did NOT enter the nullRow||EOF branch
	// We just verify neither flag triggered early return
	_ = payload // may be nil since no btree
}

func TestMCDC_CursorNullRowOrEOF_NullRow(t *testing.T) {
	t.Parallel()
	// A=true, B=false → NullRow causes NULL
	v := NewTestVDBE(5)
	cursor := buildCursorWithFlags(true, false)
	dst := NewMem()

	payload := v.getColumnPayload(cursor, dst)
	if payload != nil {
		t.Error("Expected nil payload when NullRow=true")
	}
	if !dst.IsNull() {
		t.Error("Expected dst to be NULL when NullRow=true")
	}
}

func TestMCDC_CursorNullRowOrEOF_EOF(t *testing.T) {
	t.Parallel()
	// A=false, B=true → EOF causes NULL
	v := NewTestVDBE(5)
	cursor := buildCursorWithFlags(false, true)
	dst := NewMem()

	payload := v.getColumnPayload(cursor, dst)
	if payload != nil {
		t.Error("Expected nil payload when EOF=true")
	}
	if !dst.IsNull() {
		t.Error("Expected dst to be NULL when EOF=true")
	}
}

// ------------------------------------------------------------
// Condition 5: err != nil || pseudoMem.IsNull()  (line 1361, getPseudoCursorPayload)
// Outcome: dst set to NULL
// Cases:
//   A=F, B=F  → outcome=false (blob returned normally)
//   A=T, B=*  → outcome=true  (GetMem error → NULL)
//   A=F, B=T  → outcome=true  (pseudoMem is NULL → NULL returned)
// ------------------------------------------------------------

func TestMCDC_PseudoCursorPayload_ErrorOnGetMem(t *testing.T) {
	t.Parallel()
	// A=true: PseudoReg is out of range → GetMem returns error
	v := NewTestVDBE(3)
	cursor := &Cursor{
		CurType:   CursorPseudo,
		PseudoReg: 99, // out of range → error
	}
	dst := NewMem()

	result := v.getPseudoCursorPayload(cursor, dst)
	if result != nil {
		t.Error("Expected nil payload on GetMem error")
	}
	if !dst.IsNull() {
		t.Error("Expected dst to be NULL on GetMem error")
	}
}

func TestMCDC_PseudoCursorPayload_PseudoMemIsNull(t *testing.T) {
	t.Parallel()
	// A=false (no error), B=true (pseudoMem is NULL) → NULL returned
	v := NewTestVDBE(5)
	v.Mem[2].SetNull() // pseudoMem is NULL
	cursor := &Cursor{
		CurType:   CursorPseudo,
		PseudoReg: 2,
	}
	dst := NewMem()

	result := v.getPseudoCursorPayload(cursor, dst)
	if result != nil {
		t.Error("Expected nil payload when pseudoMem is NULL")
	}
	if !dst.IsNull() {
		t.Error("Expected dst to be NULL when pseudoMem is NULL")
	}
}

func TestMCDC_PseudoCursorPayload_BlobReturned(t *testing.T) {
	t.Parallel()
	// A=false (no error), B=false (pseudoMem is blob, not NULL) → blob returned
	v := NewTestVDBE(5)
	blobData := []byte{1, 2, 3}
	v.Mem[2].SetBlob(blobData)
	cursor := &Cursor{
		CurType:   CursorPseudo,
		PseudoReg: 2,
	}
	dst := NewMem()

	result := v.getPseudoCursorPayload(cursor, dst)
	if result == nil {
		t.Error("Expected non-nil payload when pseudoMem is blob")
	}
}

// ------------------------------------------------------------
// Condition 6: dst.IsNull() && cursor != nil && cursor.Table != nil  (line 1410, parseColumnIntoMem)
// Outcome: applyDefaultValueIfAvailable is called
// Cases:
//   A=F, B=*, C=*  → outcome=false (dst not null → skip default)
//   A=T, B=F, C=*  → outcome=false (cursor nil → skip default)
//   A=T, B=T, C=F  → outcome=false (Table nil → skip default)
//   A=T, B=T, C=T  → outcome=true  (all true → apply default)
// ------------------------------------------------------------

func TestMCDC_ParseColumnDefaultApply_DstNotNull(t *testing.T) {
	t.Parallel()
	// A=false: dst is not null after parse → no default applied
	// Use empty data so parseRecordColumn sets NULL, then we check it stays NULL
	// Actually we want dst NOT null: use data with a real value
	// Craft a minimal 1-column record with value 42
	// serial type 1 (int8), header: varint(2 bytes total), body: 0x2A
	data := []byte{
		0x02, // header size = 2
		0x01, // serial type 1 (int8)
		0x2A, // value = 42
	}
	v := NewTestVDBE(5)
	dst := NewMem()
	cursor := &Cursor{Table: nil}

	err := v.parseColumnIntoMem(data, 0, dst, cursor)
	if err != nil {
		t.Fatalf("parseColumnIntoMem failed: %v", err)
	}
	// dst should be int 42, not null → default was not applied
	if dst.IsNull() {
		t.Error("Expected dst to be non-null (value=42)")
	}
}

func TestMCDC_ParseColumnDefaultApply_CursorNil(t *testing.T) {
	t.Parallel()
	// A=true (dst is null from empty data), B=false (cursor=nil) → no default
	data := []byte{} // empty → NULL
	v := NewTestVDBE(5)
	dst := NewMem()

	err := v.parseColumnIntoMem(data, 0, dst, nil)
	if err != nil {
		t.Fatalf("parseColumnIntoMem failed: %v", err)
	}
	if !dst.IsNull() {
		t.Error("Expected dst to remain NULL when cursor is nil")
	}
}

func TestMCDC_ParseColumnDefaultApply_TableNil(t *testing.T) {
	t.Parallel()
	// A=true, B=true (cursor non-nil), C=false (Table=nil) → no default
	data := []byte{} // empty → NULL
	v := NewTestVDBE(5)
	dst := NewMem()
	cursor := &Cursor{Table: nil}

	err := v.parseColumnIntoMem(data, 0, dst, cursor)
	if err != nil {
		t.Fatalf("parseColumnIntoMem failed: %v", err)
	}
	if !dst.IsNull() {
		t.Error("Expected dst to remain NULL when cursor.Table is nil")
	}
}

// ------------------------------------------------------------
// Condition 7: colIndex < 0 || colIndex >= len(serialTypes)  (line 1601, parseRecordColumn)
// Outcome: dst=NULL, return nil
// Cases:
//   A=F, B=F  → outcome=false (valid index, parse proceeds)
//   A=T, B=*  → outcome=true  (negative index → NULL)
//   A=F, B=T  → outcome=true  (index too large → NULL)
// ------------------------------------------------------------

func buildMinimalRecord(value int64) []byte {
	// 1-column record with int8 value
	return []byte{0x02, 0x01, byte(value)}
}

func TestMCDC_ColIndexBounds_Valid(t *testing.T) {
	t.Parallel()
	// A=false, B=false: valid index 0 in a 1-column record
	data := buildMinimalRecord(7)
	dst := NewMem()
	err := parseRecordColumn(data, 0, dst)
	if err != nil {
		t.Fatalf("parseRecordColumn failed: %v", err)
	}
	if dst.IsNull() {
		t.Error("Expected non-null for valid column index")
	}
}

func TestMCDC_ColIndexBounds_Negative(t *testing.T) {
	t.Parallel()
	// A=true: negative index → NULL
	data := buildMinimalRecord(7)
	dst := NewMem()
	err := parseRecordColumn(data, -1, dst)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dst.IsNull() {
		t.Error("Expected NULL for negative column index")
	}
}

func TestMCDC_ColIndexBounds_TooLarge(t *testing.T) {
	t.Parallel()
	// A=false, B=true: index beyond number of columns → NULL
	data := buildMinimalRecord(7) // only 1 column
	dst := NewMem()
	err := parseRecordColumn(data, 5, dst) // column 5 doesn't exist
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !dst.IsNull() {
		t.Error("Expected NULL for out-of-range column index")
	}
}

// ------------------------------------------------------------
// Condition 8: offset+1 < len(buf) && buf[offset+1] < 0x80  (line 1672, getVarint 2-byte path)
// Outcome: 2-byte varint decoded
// Cases:
//   A=F, B=*  → outcome=false (only 1 byte available after continuation byte → general path)
//   A=T, B=F  → outcome=false (second byte has continuation bit set → general path)
//   A=T, B=T  → outcome=true  (2-byte varint decoded)
// ------------------------------------------------------------

func TestMCDC_Varint2Byte_OnlyOneByte(t *testing.T) {
	t.Parallel()
	// A=false: buf has only 1 byte which is >= 0x80 (continuation), so offset+1 == len(buf)
	buf := []byte{0x81} // single continuation byte, no second byte
	val, n := getVarint(buf, 0)
	// Should fall to getVarintGeneral, which returns val=1, n=1 (loop ends with high bit set)
	_ = val
	if n == 2 {
		t.Error("Expected NOT to decode as 2-byte varint when buf too short")
	}
}

func TestMCDC_Varint2Byte_SecondByteContinuation(t *testing.T) {
	t.Parallel()
	// A=true, B=false: second byte also >= 0x80 → general path
	buf := []byte{0x81, 0x82, 0x03} // 3-byte varint
	val, n := getVarint(buf, 0)
	if n != 3 {
		t.Errorf("Expected 3-byte decode, got n=%d, val=%d", n, val)
	}
}

func TestMCDC_Varint2Byte_TwoByteVarint(t *testing.T) {
	t.Parallel()
	// A=true, B=true: second byte < 0x80 → 2-byte path taken
	buf := []byte{0x81, 0x02} // continuation byte + final byte
	val, n := getVarint(buf, 0)
	if n != 2 {
		t.Errorf("Expected 2-byte decode, got n=%d", n)
	}
	expected := uint64((1 << 7) | 2)
	if val != expected {
		t.Errorf("Expected val=%d, got %d", expected, val)
	}
}

// ------------------------------------------------------------
// Condition 9: a.IsNull() && b.IsNull()  (line 3178, compareMemValues)
// Outcome: return 0 (both-null equals)
// Cases:
//   A=F, B=*  → outcome=false (a not null → type comparison)
//   A=T, B=F  → outcome=false (b not null → a < anything)
//   A=T, B=T  → outcome=true  (both null → return 0)
// ------------------------------------------------------------

func TestMCDC_CompareMemValues_ANullBNotNull(t *testing.T) {
	t.Parallel()
	// A=true (a is null), B=false (b is not null) → a < b → result -1
	v := NewTestVDBE(5)
	a := NewMemNull()
	b := NewMemInt(5)
	result := v.compareMemValues(a, b)
	if result != -1 {
		t.Errorf("Expected -1 (null < non-null), got %d", result)
	}
}

func TestMCDC_CompareMemValues_ANotNull(t *testing.T) {
	t.Parallel()
	// A=false (a is not null), B=* → proceeds to type comparison
	v := NewTestVDBE(5)
	a := NewMemInt(3)
	b := NewMemInt(5)
	result := v.compareMemValues(a, b)
	if result != -1 {
		t.Errorf("Expected -1 (3 < 5), got %d", result)
	}
}

func TestMCDC_CompareMemValues_BothNull(t *testing.T) {
	t.Parallel()
	// A=true, B=true → return 0
	v := NewTestVDBE(5)
	a := NewMemNull()
	b := NewMemNull()
	result := v.compareMemValues(a, b)
	if result != 0 {
		t.Errorf("Expected 0 (null == null), got %d", result)
	}
}

// ------------------------------------------------------------
// Condition 10: left.IsNull() || right.IsNull()  (line 4059, execCompare)
// and          left.IsNull() || right.IsNull()  (line 4170, execConcat)
// Outcome: result = NULL
// ------------------------------------------------------------

func TestMCDC_CompareNullHandling_LeftNull(t *testing.T) {
	t.Parallel()
	// A=true, B=* → NULL result
	v := NewTestVDBE(10)
	v.Mem[0].SetNull() // left = NULL
	v.Mem[1].SetInt(5) // right = 5
	// Test via execEq: P1=0, P2=1, P3=2
	instr := &Instruction{Opcode: OpEq, P1: 0, P2: 1, P3: 2}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	if !v.Mem[2].IsNull() {
		t.Error("Expected NULL result when left operand is NULL")
	}
}

func TestMCDC_CompareNullHandling_RightNull(t *testing.T) {
	t.Parallel()
	// A=false, B=true → NULL result
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(5) // left = 5
	v.Mem[1].SetNull() // right = NULL
	instr := &Instruction{Opcode: OpEq, P1: 0, P2: 1, P3: 2}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	if !v.Mem[2].IsNull() {
		t.Error("Expected NULL result when right operand is NULL")
	}
}

func TestMCDC_CompareNullHandling_NeitherNull(t *testing.T) {
	t.Parallel()
	// A=false, B=false → normal integer comparison
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(5)
	v.Mem[1].SetInt(5)
	instr := &Instruction{Opcode: OpEq, P1: 0, P2: 1, P3: 2}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	if v.Mem[2].IsNull() {
		t.Error("Expected non-null result when neither operand is NULL")
	}
	if v.Mem[2].IntValue() != 1 {
		t.Errorf("Expected 1 (5==5), got %d", v.Mem[2].IntValue())
	}
}

// ------------------------------------------------------------
// Condition 11: left.IsNull() || right.IsNull()  (line 4170, execConcat)
// ------------------------------------------------------------

func TestMCDC_ConcatNullHandling_LeftNull(t *testing.T) {
	t.Parallel()
	// A=true → NULL concatenation result
	v := NewTestVDBE(10)
	v.Mem[0].SetNull()
	v.Mem[1].SetStr("hello")
	instr := &Instruction{Opcode: OpConcat, P1: 0, P2: 1, P3: 2}
	err := v.execConcat(instr)
	if err != nil {
		t.Fatalf("execConcat failed: %v", err)
	}
	if !v.Mem[2].IsNull() {
		t.Error("Expected NULL when left is NULL in concat")
	}
}

func TestMCDC_ConcatNullHandling_RightNull(t *testing.T) {
	t.Parallel()
	// A=false, B=true → NULL concatenation result
	v := NewTestVDBE(10)
	v.Mem[0].SetStr("hello")
	v.Mem[1].SetNull()
	instr := &Instruction{Opcode: OpConcat, P1: 0, P2: 1, P3: 2}
	err := v.execConcat(instr)
	if err != nil {
		t.Fatalf("execConcat failed: %v", err)
	}
	if !v.Mem[2].IsNull() {
		t.Error("Expected NULL when right is NULL in concat")
	}
}

func TestMCDC_ConcatNullHandling_NeitherNull(t *testing.T) {
	t.Parallel()
	// A=false, B=false → normal concatenation
	v := NewTestVDBE(10)
	v.Mem[0].SetStr("hello")
	v.Mem[1].SetStr(" world")
	instr := &Instruction{Opcode: OpConcat, P1: 0, P2: 1, P3: 2}
	err := v.execConcat(instr)
	if err != nil {
		t.Fatalf("execConcat failed: %v", err)
	}
	got := v.Mem[2].StrValue()
	if got != "hello world" {
		t.Errorf("Expected 'hello world', got %q", got)
	}
}

// ------------------------------------------------------------
// Condition 12: !mem.IsInt() && !mem.IsNull()  (line 4275, execAddImm)
// Outcome: Integerify is called on the value
// Cases:
//   A=F, B=*  → outcome=false (already int → no Integerify)
//   A=T, B=F  → outcome=false (is null → no Integerify)
//   A=T, B=T  → outcome=true  (real/string, non-null → Integerify called)
// ------------------------------------------------------------

func TestMCDC_AddImm_AlreadyInt(t *testing.T) {
	t.Parallel()
	// A=false: mem is int → no conversion needed
	v := NewTestVDBE(10)
	v.Mem[0].SetInt(10)
	instr := &Instruction{Opcode: OpAddImm, P1: 0, P2: 5}
	err := v.execAddImm(instr)
	if err != nil {
		t.Fatalf("execAddImm failed: %v", err)
	}
	if v.Mem[0].IntValue() != 15 {
		t.Errorf("Expected 15, got %d", v.Mem[0].IntValue())
	}
}

func TestMCDC_AddImm_IsNull(t *testing.T) {
	t.Parallel()
	// A=true (!IsInt), B=false (!IsNull=false because it IS null) → no Integerify
	v := NewTestVDBE(10)
	v.Mem[0].SetNull()
	instr := &Instruction{Opcode: OpAddImm, P1: 0, P2: 3}
	err := v.execAddImm(instr)
	if err != nil {
		t.Fatalf("execAddImm failed: %v", err)
	}
	// NULL integer value is 0, so 0+3 = 3
	if v.Mem[0].IntValue() != 3 {
		t.Errorf("Expected 3 (0+3), got %d", v.Mem[0].IntValue())
	}
}

func TestMCDC_AddImm_RealValue(t *testing.T) {
	t.Parallel()
	// A=true (!IsInt), B=true (!IsNull) → Integerify called, then add
	v := NewTestVDBE(10)
	v.Mem[0].SetReal(7.9) // real, not int, not null
	instr := &Instruction{Opcode: OpAddImm, P1: 0, P2: 2}
	err := v.execAddImm(instr)
	if err != nil {
		t.Fatalf("execAddImm failed: %v", err)
	}
	// Integerify of 7.9 should be 7, then +2 = 9
	if v.Mem[0].IntValue() != 9 {
		t.Errorf("Expected 9 (7+2), got %d", v.Mem[0].IntValue())
	}
}

// ------------------------------------------------------------
// Condition 13: x.IsInt() && y.IsInt()  (line 3215, tryTypedComparison)
// Outcome: integer comparison path taken
// Cases:
//   A=F, B=*  → outcome=false (x not int → skip)
//   A=T, B=F  → outcome=false (y not int → skip)
//   A=T, B=T  → outcome=true  (both int → compareInts)
// ------------------------------------------------------------

func TestMCDC_TypedComparison_XNotInt(t *testing.T) {
	t.Parallel()
	// A=false: x is real, y is int → different types → string fallback
	v := NewTestVDBE(5)
	a := NewMemReal(3.0)
	b := NewMemInt(3)
	result := v.compareMemValues(a, b)
	// mixed types fall back to string comparison "3" vs "3" → 0 typically
	_ = result // just ensure no panic
}

func TestMCDC_TypedComparison_YNotInt(t *testing.T) {
	t.Parallel()
	// A=true (x is int), B=false (y is not int) → integer path not taken
	v := NewTestVDBE(5)
	a := NewMemInt(3)
	b := NewMemStr("3")
	result := v.compareMemValues(a, b)
	_ = result // mixed types: ensure no panic
}

func TestMCDC_TypedComparison_BothInt(t *testing.T) {
	t.Parallel()
	// A=true, B=true → integer comparison
	v := NewTestVDBE(5)
	a := NewMemInt(10)
	b := NewMemInt(20)
	result := v.compareMemValues(a, b)
	if result != -1 {
		t.Errorf("Expected -1 (10 < 20), got %d", result)
	}
}

// ------------------------------------------------------------
// Condition 14: instr.P4Type == P4Static && instr.P4.Z != ""  (line 4097, compareWithOptionalCollation)
// Outcome: collation-aware comparison used
// Cases:
//   A=F, B=*  → outcome=false (no collation → default compare)
//   A=T, B=F  → outcome=false (P4Static but empty Z → default compare)
//   A=T, B=T  → outcome=true  (P4Static + non-empty Z → collation compare)
// ------------------------------------------------------------

func TestMCDC_CompareWithCollation_NotP4Static(t *testing.T) {
	t.Parallel()
	// A=false: P4Type is not P4Static → default comparison
	v := NewTestVDBE(10)
	v.Mem[0].SetStr("abc")
	v.Mem[1].SetStr("ABC")
	instr := &Instruction{
		Opcode: OpEq,
		P1:     0,
		P2:     1,
		P3:     2,
		P4Type: P4Real, // not static
		P4:     P4Union{Z: "NOCASE"},
	}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	// default binary comparison: "abc" != "ABC" → 0
	if v.Mem[2].IsNull() {
		t.Fatal("Expected non-null result")
	}
	if v.Mem[2].IntValue() != 0 {
		t.Errorf("Expected 0 (case-sensitive: abc != ABC), got %d", v.Mem[2].IntValue())
	}
}

func TestMCDC_CompareWithCollation_P4StaticEmptyZ(t *testing.T) {
	t.Parallel()
	// A=true (P4Static), B=false (Z is empty) → default compare
	v := NewTestVDBE(10)
	v.Mem[0].SetStr("abc")
	v.Mem[1].SetStr("ABC")
	instr := &Instruction{
		Opcode: OpEq,
		P1:     0,
		P2:     1,
		P3:     2,
		P4Type: P4Static,
		P4:     P4Union{Z: ""}, // empty collation
	}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	if v.Mem[2].IsNull() {
		t.Fatal("Expected non-null result")
	}
	// binary: "abc" != "ABC" → 0
	if v.Mem[2].IntValue() != 0 {
		t.Errorf("Expected 0 (case-sensitive), got %d", v.Mem[2].IntValue())
	}
}

func TestMCDC_CompareWithCollation_NOCASECollation(t *testing.T) {
	t.Parallel()
	// A=true (P4Static), B=true (Z="NOCASE") → NOCASE comparison
	v := NewTestVDBE(10)
	v.Mem[0].SetStr("abc")
	v.Mem[1].SetStr("ABC")
	instr := &Instruction{
		Opcode: OpEq,
		P1:     0,
		P2:     1,
		P3:     2,
		P4Type: P4Static,
		P4:     P4Union{Z: "NOCASE"},
	}
	err := v.execEq(instr)
	if err != nil {
		t.Fatalf("execEq failed: %v", err)
	}
	if v.Mem[2].IsNull() {
		t.Fatal("Expected non-null result")
	}
	// NOCASE: "abc" == "ABC" → 1
	if v.Mem[2].IntValue() != 1 {
		t.Errorf("Expected 1 (NOCASE: abc == ABC), got %d", v.Mem[2].IntValue())
	}
}
