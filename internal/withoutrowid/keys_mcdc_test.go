// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package withoutrowid

import (
	"bytes"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// DecodeCompositeKey – empty input
// ---------------------------------------------------------------------------

func TestMCDC_DecodeCompositeKey_EmptyInput(t *testing.T) {
	// Condition: len(data)==0 → return nil, nil (no error)
	vals, err := DecodeCompositeKey([]byte{})
	if err != nil {
		t.Fatalf("A=T (empty): unexpected error: %v", err)
	}
	if vals != nil {
		t.Fatalf("A=T (empty): expected nil values, got %v", vals)
	}
}

// ---------------------------------------------------------------------------
// decodeOneValue – unknown prefix
// ---------------------------------------------------------------------------

func TestMCDC_DecodeOneValue_UnknownPrefix(t *testing.T) {
	// Condition: prefix not in known set → error path
	_, _, err := decodeOneValue([]byte{0xFF, 0x01})
	if err == nil {
		t.Fatal("A=T (unknown prefix): expected error, got nil")
	}
}

func TestMCDC_DecodeOneValue_EmptyData(t *testing.T) {
	// Condition: len(data)==0 → error path
	_, _, err := decodeOneValue([]byte{})
	if err == nil {
		t.Fatal("A=T (empty data): expected error, got nil")
	}
}

// ---------------------------------------------------------------------------
// decodeFixed8 – truncation guard
// ---------------------------------------------------------------------------

func TestMCDC_DecodeFixed8_TruncatedInteger(t *testing.T) {
	// Condition: len(data) < 8 (only 4 bytes after prefix 0x10)
	bad := []byte{0x10, 0x01, 0x02, 0x03, 0x04}
	_, err := DecodeCompositeKey(bad)
	if err == nil {
		t.Fatal("A=T (truncated int): expected error for truncated integer data")
	}
}

func TestMCDC_DecodeFixed8_TruncatedFloat(t *testing.T) {
	// Condition: len(data) < 8 (only 3 bytes after prefix 0x20)
	bad := []byte{0x20, 0x01, 0x02, 0x03}
	_, err := DecodeCompositeKey(bad)
	if err == nil {
		t.Fatal("A=T (truncated float): expected error for truncated float data")
	}
}

// ---------------------------------------------------------------------------
// decodeNullTermString – missing terminator
// ---------------------------------------------------------------------------

func TestMCDC_DecodeNullTermString_Unterminated(t *testing.T) {
	// Condition: end == -1 (no 0x00 terminator) → error
	// 0x30 = text prefix, no null terminator
	bad := []byte{0x30, 'h', 'i'}
	_, err := DecodeCompositeKey(bad)
	if err == nil {
		t.Fatal("A=T (unterminated text): expected error, got nil")
	}
}

func TestMCDC_DecodeNullTermString_Terminated(t *testing.T) {
	// Condition: end != -1 → success path
	data := []byte{0x30, 'h', 'i', 0x00}
	vals, err := DecodeCompositeKey(data)
	if err != nil {
		t.Fatalf("A=F (terminated text): unexpected error: %v", err)
	}
	if len(vals) != 1 || vals[0] != "hi" {
		t.Fatalf("A=F (terminated text): expected [\"hi\"], got %v", vals)
	}
}

// ---------------------------------------------------------------------------
// decodeNullTermBlob – missing terminator
// ---------------------------------------------------------------------------

func TestMCDC_DecodeNullTermBlob_Unterminated(t *testing.T) {
	// Condition: end == -1 → error
	bad := []byte{0x40, 0xDE, 0xAD}
	_, err := DecodeCompositeKey(bad)
	if err == nil {
		t.Fatal("A=T (unterminated blob): expected error, got nil")
	}
}

func TestMCDC_DecodeNullTermBlob_Terminated(t *testing.T) {
	// Condition: end != -1 → success path
	data := []byte{0x40, 0xDE, 0xAD, 0x00}
	vals, err := DecodeCompositeKey(data)
	if err != nil {
		t.Fatalf("A=F (terminated blob): unexpected error: %v", err)
	}
	blob, ok := vals[0].([]byte)
	if !ok || !bytes.Equal(blob, []byte{0xDE, 0xAD}) {
		t.Fatalf("A=F (terminated blob): expected [0xDE 0xAD], got %v", vals[0])
	}
}

// ---------------------------------------------------------------------------
// decodeFloat64 – MC/DC on sign bit branch
// MC/DC condition: u&(1<<63) != 0 (T) vs == 0 (F)
// ---------------------------------------------------------------------------

func TestMCDC_DecodeFloat64_PositiveFloat_SignBitSet(t *testing.T) {
	// A=T: positive float encoded → sign bit is set in encoded form
	original := 3.14
	encoded := encodeFloat64(original)
	decoded := decodeFloat64(encoded)
	if decoded != original {
		t.Fatalf("A=T (positive float): round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeFloat64_NegativeFloat_SignBitClear(t *testing.T) {
	// A=F: negative float encoded → sign bit is clear in encoded form (bitwise NOT used)
	original := -2.71
	encoded := encodeFloat64(original)
	decoded := decodeFloat64(encoded)
	if decoded != original {
		t.Fatalf("A=F (negative float): round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeFloat64_Zero(t *testing.T) {
	original := 0.0
	encoded := encodeFloat64(original)
	decoded := decodeFloat64(encoded)
	if decoded != original {
		t.Fatalf("zero float round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeFloat64_NegativeZero(t *testing.T) {
	// -0.0 is a negative value, so the negative float branch (^bits) is taken.
	// IEEE 754 encodes -0.0 and +0.0 as equal in value; verify round-trip is lossless.
	original := math.Copysign(0, -1) // -0.0
	encoded := encodeFloat64(original)
	decoded := decodeFloat64(encoded)
	// Both are zero by value; confirm no panic and correct magnitude.
	if decoded != 0.0 {
		t.Fatalf("negative zero round-trip: expected 0, got %v", decoded)
	}
}

// ---------------------------------------------------------------------------
// decodeInt64 – round-trip for representative values
// ---------------------------------------------------------------------------

func TestMCDC_DecodeInt64_Positive(t *testing.T) {
	original := int64(42)
	encoded := encodeInt64(original)
	decoded := decodeInt64(encoded)
	if decoded != original {
		t.Fatalf("positive int64 round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeInt64_Negative(t *testing.T) {
	original := int64(-99)
	encoded := encodeInt64(original)
	decoded := decodeInt64(encoded)
	if decoded != original {
		t.Fatalf("negative int64 round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeInt64_Zero(t *testing.T) {
	original := int64(0)
	encoded := encodeInt64(original)
	decoded := decodeInt64(encoded)
	if decoded != original {
		t.Fatalf("zero int64 round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeInt64_MinInt64(t *testing.T) {
	original := int64(math.MinInt64)
	encoded := encodeInt64(original)
	decoded := decodeInt64(encoded)
	if decoded != original {
		t.Fatalf("MinInt64 round-trip failed: got %v want %v", decoded, original)
	}
}

func TestMCDC_DecodeInt64_MaxInt64(t *testing.T) {
	original := int64(math.MaxInt64)
	encoded := encodeInt64(original)
	decoded := decodeInt64(encoded)
	if decoded != original {
		t.Fatalf("MaxInt64 round-trip failed: got %v want %v", decoded, original)
	}
}

// ---------------------------------------------------------------------------
// formatUnknown – invoked via EncodeCompositeKey default branch
// ---------------------------------------------------------------------------

type unknownType struct{ val int }

func (u unknownType) String() string { return "unknown" }

func TestMCDC_FormatUnknown_DefaultBranch(t *testing.T) {
	// Exercise the default branch of EncodeCompositeKey which calls formatUnknown.
	v := unknownType{val: 7}
	encoded := EncodeCompositeKey([]interface{}{v})
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoding for unknown type")
	}
	// Prefix 0x50 should be present for unknown types.
	if encoded[0] != 0x50 {
		t.Fatalf("expected prefix 0x50 for unknown type, got 0x%x", encoded[0])
	}
}

func TestMCDC_FormatUnknown_NulBytesReplaced(t *testing.T) {
	// formatUnknown must replace embedded 0x00 bytes with 0x01.
	// We call it directly since it is package-internal.
	result := formatUnknown("\x00embedded\x00")
	for i, b := range []byte(result) {
		if b == 0x00 {
			t.Fatalf("null byte survived formatUnknown at index %d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// EncodeCompositeKey – full type coverage
// ---------------------------------------------------------------------------

func TestMCDC_EncodeCompositeKey_AllTypes(t *testing.T) {
	tests := []struct {
		name   string
		values []interface{}
	}{
		{"null value", []interface{}{nil}},
		{"int value", []interface{}{int(5)}},
		{"int64 value", []interface{}{int64(5)}},
		{"float64 positive", []interface{}{float64(1.0)}},
		{"float64 negative", []interface{}{float64(-1.0)}},
		{"string value", []interface{}{"hello"}},
		{"blob value", []interface{}{[]byte{0x01, 0x02}}},
		{"unknown type", []interface{}{unknownType{3}}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			enc := EncodeCompositeKey(tc.values)
			if len(enc) == 0 {
				t.Fatalf("%s: encoded to empty bytes", tc.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Full round-trip: EncodeCompositeKey → DecodeCompositeKey
// ---------------------------------------------------------------------------

func TestMCDC_RoundTrip_Null(t *testing.T) {
	in := []interface{}{nil}
	enc := EncodeCompositeKey(in)
	out, err := DecodeCompositeKey(enc)
	if err != nil {
		t.Fatalf("round-trip null: %v", err)
	}
	if len(out) != 1 || out[0] != nil {
		t.Fatalf("round-trip null: got %v", out)
	}
}

func TestMCDC_RoundTrip_Int64(t *testing.T) {
	cases := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	for _, v := range cases {
		in := []interface{}{v}
		enc := EncodeCompositeKey(in)
		out, err := DecodeCompositeKey(enc)
		if err != nil {
			t.Fatalf("round-trip int64 %d: %v", v, err)
		}
		got, ok := out[0].(int64)
		if !ok || got != v {
			t.Fatalf("round-trip int64 %d: got %v (%T)", v, out[0], out[0])
		}
	}
}

func TestMCDC_RoundTrip_Int(t *testing.T) {
	// int is encoded as int64 but decoded back as int64; just verify no error.
	in := []interface{}{int(42)}
	enc := EncodeCompositeKey(in)
	out, err := DecodeCompositeKey(enc)
	if err != nil {
		t.Fatalf("round-trip int: %v", err)
	}
	got, ok := out[0].(int64)
	if !ok || got != 42 {
		t.Fatalf("round-trip int: got %v (%T)", out[0], out[0])
	}
}

func TestMCDC_RoundTrip_Float64Positive(t *testing.T) {
	v := 3.14159
	in := []interface{}{v}
	enc := EncodeCompositeKey(in)
	out, err := DecodeCompositeKey(enc)
	if err != nil {
		t.Fatalf("round-trip float64 positive: %v", err)
	}
	got, ok := out[0].(float64)
	if !ok || got != v {
		t.Fatalf("round-trip float64 positive: got %v (%T)", out[0], out[0])
	}
}

func TestMCDC_RoundTrip_Float64Negative(t *testing.T) {
	v := -2.71828
	in := []interface{}{v}
	enc := EncodeCompositeKey(in)
	out, err := DecodeCompositeKey(enc)
	if err != nil {
		t.Fatalf("round-trip float64 negative: %v", err)
	}
	got, ok := out[0].(float64)
	if !ok || got != v {
		t.Fatalf("round-trip float64 negative: got %v (%T)", out[0], out[0])
	}
}

func TestMCDC_RoundTrip_String(t *testing.T) {
	cases := []string{"", "hello", "world\x01embedded"}
	for _, v := range cases {
		in := []interface{}{v}
		enc := EncodeCompositeKey(in)
		out, err := DecodeCompositeKey(enc)
		if err != nil {
			t.Fatalf("round-trip string %q: %v", v, err)
		}
		got, ok := out[0].(string)
		if !ok || got != v {
			t.Fatalf("round-trip string %q: got %v (%T)", v, out[0], out[0])
		}
	}
}

func TestMCDC_RoundTrip_Blob(t *testing.T) {
	cases := [][]byte{{}, {0x01}, {0xDE, 0xAD, 0xBE, 0xEF}}
	for _, v := range cases {
		in := []interface{}{v}
		enc := EncodeCompositeKey(in)
		out, err := DecodeCompositeKey(enc)
		if err != nil {
			t.Fatalf("round-trip blob %x: %v", v, err)
		}
		got, ok := out[0].([]byte)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("round-trip blob %x: got %v (%T)", v, out[0], out[0])
		}
	}
}

func TestMCDC_RoundTrip_MultipleValues(t *testing.T) {
	// Composite key with all supported types.
	in := []interface{}{nil, int64(7), float64(-0.5), "text", []byte{0xAB}}
	enc := EncodeCompositeKey(in)
	out, err := DecodeCompositeKey(enc)
	if err != nil {
		t.Fatalf("round-trip multi: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("round-trip multi: got %d values, want %d", len(out), len(in))
	}
	if out[0] != nil {
		t.Fatalf("round-trip multi [0]: expected nil, got %v", out[0])
	}
	if out[1].(int64) != 7 {
		t.Fatalf("round-trip multi [1]: expected 7, got %v", out[1])
	}
	if out[2].(float64) != -0.5 {
		t.Fatalf("round-trip multi [2]: expected -0.5, got %v", out[2])
	}
	if out[3].(string) != "text" {
		t.Fatalf("round-trip multi [3]: expected \"text\", got %v", out[3])
	}
	if !bytes.Equal(out[4].([]byte), []byte{0xAB}) {
		t.Fatalf("round-trip multi [4]: expected [0xAB], got %v", out[4])
	}
}

// ---------------------------------------------------------------------------
// 0x50 prefix (unknown type encoded as string) – decode path
// ---------------------------------------------------------------------------

func TestMCDC_Decode_0x50Prefix(t *testing.T) {
	// 0x50 prefix is decoded via decodeNullTermString (same branch as 0x30).
	data := []byte{0x50, 'f', 'o', 'o', 0x00}
	vals, err := DecodeCompositeKey(data)
	if err != nil {
		t.Fatalf("0x50 decode: unexpected error: %v", err)
	}
	if len(vals) != 1 || vals[0] != "foo" {
		t.Fatalf("0x50 decode: got %v", vals)
	}
}
