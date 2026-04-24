// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"encoding/binary"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// Direct unit tests for decode functions (package-private, accessible here)
// ---------------------------------------------------------------------------

// TestDecodeInt8 covers decodeInt8 including boundary values and short-input guard.
func TestDecodeInt8(t *testing.T) {
	cases := []struct {
		data    []byte
		wantVal int64
		wantSz  int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, 1, 1},
		{[]byte{0x7f}, 127, 1},
		{[]byte{0x80}, -128, 1},
		{[]byte{0xff}, -1, 1},
	}
	for _, tc := range cases {
		got, sz := decodeInt8(tc.data)
		if sz != tc.wantSz {
			t.Errorf("decodeInt8(%v) size=%d, want %d", tc.data, sz, tc.wantSz)
		}
		if got != tc.wantVal {
			t.Errorf("decodeInt8(%v) val=%v, want %d", tc.data, got, tc.wantVal)
		}
	}
	v, sz := decodeInt8([]byte{})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt8(empty) expected nil,0, got %v,%d", v, sz)
	}
}

// TestDecodeInt16 covers decodeInt16 including boundary values and short-input guard.
func TestDecodeInt16(t *testing.T) {
	cases := []struct {
		data    []byte
		wantVal int64
	}{
		{[]byte{0x00, 0x00}, 0},
		{[]byte{0x00, 0x01}, 1},
		{[]byte{0x7f, 0xff}, 32767},
		{[]byte{0x80, 0x00}, -32768},
		{[]byte{0xff, 0xff}, -1},
	}
	for _, tc := range cases {
		got, sz := decodeInt16(tc.data)
		if sz != 2 {
			t.Errorf("decodeInt16 size=%d, want 2", sz)
		}
		if got != tc.wantVal {
			t.Errorf("decodeInt16(%v) val=%v, want %d", tc.data, got, tc.wantVal)
		}
	}
	v, sz := decodeInt16([]byte{0x01})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt16(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestDecodeInt24 covers decodeInt24 including sign extension and short-input guard.
func TestDecodeInt24(t *testing.T) {
	cases := []struct {
		data    []byte
		wantVal int64
	}{
		{[]byte{0x00, 0x00, 0x00}, 0},
		{[]byte{0x00, 0x00, 0x01}, 1},
		{[]byte{0x7f, 0xff, 0xff}, 8388607},
		{[]byte{0x80, 0x00, 0x00}, -8388608},
		{[]byte{0xff, 0xff, 0xff}, -1},
	}
	for _, tc := range cases {
		got, sz := decodeInt24(tc.data)
		if sz != 3 {
			t.Errorf("decodeInt24 size=%d, want 3", sz)
		}
		if got != tc.wantVal {
			t.Errorf("decodeInt24(%v) val=%v, want %d", tc.data, got, tc.wantVal)
		}
	}
	v, sz := decodeInt24([]byte{0x00, 0x00})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt24(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestDecodeInt32 covers decodeInt32 including sign extension and short-input guard.
func TestDecodeInt32(t *testing.T) {
	cases := []struct {
		wantVal int64
	}{
		{0}, {1}, {2147483647}, {-2147483648}, {-1},
	}
	for _, tc := range cases {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(int32(tc.wantVal)))
		got, sz := decodeInt32(buf)
		if sz != 4 {
			t.Errorf("decodeInt32 size=%d, want 4", sz)
		}
		if got != tc.wantVal {
			t.Errorf("decodeInt32 val=%v, want %d", got, tc.wantVal)
		}
	}
	v, sz := decodeInt32([]byte{0, 0, 0})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt32(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestDecodeInt48 covers decodeInt48 including sign extension and short-input guard.
func TestDecodeInt48(t *testing.T) {
	cases := []struct {
		wantVal int64
	}{
		{0}, {1}, {140737488355327}, {-140737488355328}, {-1},
	}
	for _, tc := range cases {
		raw := tc.wantVal
		buf := []byte{
			byte(raw >> 40),
			byte(raw >> 32),
			byte(raw >> 24),
			byte(raw >> 16),
			byte(raw >> 8),
			byte(raw),
		}
		got, sz := decodeInt48(buf)
		if sz != 6 {
			t.Errorf("decodeInt48 size=%d, want 6", sz)
		}
		if got != tc.wantVal {
			t.Errorf("decodeInt48 val=%v, want %d", got, tc.wantVal)
		}
	}
	v, sz := decodeInt48([]byte{0, 0, 0, 0, 0})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt48(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestDecodeInt64 covers decodeInt64 including short-input guard.
func TestDecodeInt64(t *testing.T) {
	cases := []int64{0, 1, math.MaxInt64, math.MinInt64, -1}
	for _, wantVal := range cases {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(wantVal))
		got, sz := decodeInt64(buf)
		if sz != 8 {
			t.Errorf("decodeInt64 size=%d, want 8", sz)
		}
		if got != wantVal {
			t.Errorf("decodeInt64 val=%v, want %d", got, wantVal)
		}
	}
	v, sz := decodeInt64([]byte{0, 0, 0, 0, 0, 0, 0})
	if v != nil || sz != 0 {
		t.Errorf("decodeInt64(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestSerialDecodeFloat64 covers serialDecodeFloat64 including short-input guard.
func TestSerialDecodeFloat64(t *testing.T) {
	cases := []float64{0.0, 1.0, -1.0, math.Pi, math.MaxFloat64}
	for _, wantVal := range cases {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(wantVal))
		got, sz := serialDecodeFloat64(buf)
		if sz != 8 {
			t.Errorf("serialDecodeFloat64 size=%d, want 8", sz)
		}
		if got != wantVal {
			t.Errorf("serialDecodeFloat64 val=%v, want %f", got, wantVal)
		}
	}
	v, sz := serialDecodeFloat64([]byte{0, 0, 0})
	if v != nil || sz != 0 {
		t.Errorf("serialDecodeFloat64(short) expected nil,0, got %v,%d", v, sz)
	}
}

// TestSerialDecodeBlobOrText covers serialDecodeBlobOrText for blob (even) and
// text (odd) serial types, including the short-data guard.
func TestSerialDecodeBlobOrText_Blob(t *testing.T) {
	for _, tc := range []struct {
		serialType uint64
		data       []byte
	}{
		{12, []byte{}},
		{14, []byte{0xAB, 0x00}},
		{16, []byte{0x01, 0x02}},
	} {
		got, sz := serialDecodeBlobOrText(tc.serialType, tc.data)
		expLen := int((tc.serialType - 12) / 2)
		if sz != expLen {
			t.Errorf("serialDecodeBlobOrText(%d) size=%d, want %d", tc.serialType, sz, expLen)
		}
		if expLen > 0 {
			b, ok := got.([]byte)
			if !ok {
				t.Errorf("serialDecodeBlobOrText(%d) expected []byte, got %T", tc.serialType, got)
			} else if len(b) != expLen {
				t.Errorf("serialDecodeBlobOrText(%d) blob len=%d, want %d", tc.serialType, len(b), expLen)
			}
		}
	}
}

func TestSerialDecodeBlobOrText_TextAndShort(t *testing.T) {
	for _, tc := range []struct {
		serialType uint64
		data       []byte
		wantStr    string
	}{
		{13, []byte{}, ""},
		{15, []byte{'A', 'B'}, "A"},
		{17, []byte{'h', 'i', '!'}, "hi"},
	} {
		got, _ := serialDecodeBlobOrText(tc.serialType, tc.data)
		s, ok := got.(string)
		if !ok {
			t.Errorf("serialDecodeBlobOrText(%d) expected string, got %T", tc.serialType, got)
			continue
		}
		if s != tc.wantStr {
			t.Errorf("serialDecodeBlobOrText(%d) got %q, want %q", tc.serialType, s, tc.wantStr)
		}
	}

	v, sz := serialDecodeBlobOrText(16, []byte{0x01})
	if v != nil || sz != 0 {
		t.Errorf("serialDecodeBlobOrText(short) expected nil,0, got %v,%d", v, sz)
	}
}

func TestDecodeSerialValue_NullAndConstants(t *testing.T) {
	vdbeInst := NewTestVDBE(1)

	v, sz := vdbeInst.decodeSerialValue(0, []byte{})
	if v != nil || sz != 0 {
		t.Errorf("serial 0 (NULL): got %v,%d", v, sz)
	}
	v, sz = vdbeInst.decodeSerialValue(8, []byte{})
	if v != int64(0) || sz != 0 {
		t.Errorf("serial 8 (const 0): got %v,%d", v, sz)
	}
	v, sz = vdbeInst.decodeSerialValue(9, []byte{})
	if v != int64(1) || sz != 0 {
		t.Errorf("serial 9 (const 1): got %v,%d", v, sz)
	}
}

func TestDecodeSerialValue_IntFloatText(t *testing.T) {
	vdbeInst := NewTestVDBE(1)

	v, sz := vdbeInst.decodeSerialValue(1, []byte{0x42})
	if v != int64(0x42) || sz != 1 {
		t.Errorf("serial 1 (int8): got %v,%d", v, sz)
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(3.14))
	v, sz = vdbeInst.decodeSerialValue(7, buf)
	if sz != 8 {
		t.Errorf("serial 7 (float64) size=%d, want 8", sz)
	}
	if v != 3.14 {
		t.Errorf("serial 7 (float64) val=%v, want 3.14", v)
	}

	v, sz = vdbeInst.decodeSerialValue(15, []byte{'Z', 'X'})
	if s, ok := v.(string); !ok || s != "Z" {
		t.Errorf("serial 15 (text len=1): got %v,%d", v, sz)
	}
}

// TestShouldValidateWithoutRowIDUpdate directly tests all early-return branches.
func TestShouldValidateWithoutRowIDUpdate(t *testing.T) {
	vdbeInst := NewTestVDBE(1)

	// nil Ctx => false.
	vdbeInst.Ctx = nil
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when Ctx is nil")
	}

	// ForeignKeysEnabled=false => false.
	vdbeInst.Ctx = &VDBEContext{ForeignKeysEnabled: false}
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when ForeignKeysEnabled=false")
	}

	// FKManager=nil => false.
	vdbeInst.Ctx.ForeignKeysEnabled = true
	vdbeInst.Ctx.FKManager = nil
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when FKManager is nil")
	}

	// pendingFKUpdate=nil => false.
	vdbeInst.Ctx.FKManager = struct{}{}
	vdbeInst.pendingFKUpdate = nil
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when pendingFKUpdate is nil")
	}

	// pendingFKUpdate.table mismatch => false.
	vdbeInst.pendingFKUpdate = &fkUpdateContext{table: "other"}
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when table names mismatch")
	}

	// Schema=nil => false.
	vdbeInst.pendingFKUpdate = &fkUpdateContext{table: "t"}
	vdbeInst.Ctx.Schema = nil
	if vdbeInst.shouldValidateWithoutRowIDUpdate("t") {
		t.Error("expected false when Schema is nil")
	}
}

// TestGetTypedFKManagerInvalid tests the error path when FKManager lacks ValidateUpdate.
func TestGetTypedFKManagerInvalid(t *testing.T) {
	vdbeInst := NewTestVDBE(1)
	vdbeInst.Ctx = &VDBEContext{
		FKManager: struct{}{}, // does not implement ValidateUpdate
	}
	_, err := vdbeInst.getTypedFKManager()
	if err == nil {
		t.Error("expected error from getTypedFKManager with incompatible FK manager type")
	}
}
