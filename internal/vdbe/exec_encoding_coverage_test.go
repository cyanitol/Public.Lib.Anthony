// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"bytes"
	"testing"
)

// TestExecEncoding_VarintLenHighBytes covers the 6-, 7-, 8-, and 9-byte
// return branches of varintLen, which are not reached by existing tests that
// only go up through the 5-byte threshold (0x7ffffffff = 34359738367).
func TestExecEncoding_VarintLenHighBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
		want  int
	}{
		// 5-byte boundary (already covered elsewhere, included for symmetry)
		{"5-byte-max", 0x7ffffffff, 5},
		// 6-byte range: 0x800000000 .. 0x3ffffffffff
		{"6-byte-min", 0x800000000, 6},
		{"6-byte-mid", 0x1000000000, 6},
		{"6-byte-max", 0x3ffffffffff, 6},
		// 7-byte range: 0x40000000000 .. 0x1ffffffffffff
		{"7-byte-min", 0x40000000000, 7},
		{"7-byte-mid", 0x100000000000, 7},
		{"7-byte-max", 0x1ffffffffffff, 7},
		// 8-byte range: 0x20000000000000 .. 0xffffffffffffff
		{"8-byte-min", 0x20000000000000, 8},
		{"8-byte-mid", 0x80000000000000, 8},
		{"8-byte-max", 0xffffffffffffff, 8},
		// 9-byte range: anything above 0xffffffffffffff
		{"9-byte-min", 0x100000000000000, 9},
		{"9-byte-large", 0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := varintLen(tt.value)
			if got != tt.want {
				t.Errorf("varintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

// TestExecEncoding_EncodeVarintNNineBytes covers the n==9 special-case branch
// in encodeVarintN, which uses a different encoding path than n<9.
// The branch is entered only when the caller passes n=9, which happens when
// varintLen returns 9 (values > 0xffffffffffffff).
func TestExecEncoding_EncodeVarintNNineBytes(t *testing.T) {
	t.Parallel()

	// Values that trigger varintLen == 9.
	cases := []uint64{
		0x100000000000000,  // smallest 9-byte value
		0x0102030405060708, // distinctive byte pattern
		0xffffffffffffffff, // all bits set
	}

	for _, v := range cases {
		v := v
		t.Run("", func(t *testing.T) {
			t.Parallel()

			n := varintLen(v)
			if n != 9 {
				t.Fatalf("varintLen(0x%x) = %d, want 9", v, n)
			}

			encoded := encodeVarintN(v, 9)
			if len(encoded) != 9 {
				t.Fatalf("encodeVarintN length = %d, want 9", len(encoded))
			}

			// The first 8 bytes must have the continuation bit (0x80) set.
			for i := 0; i < 8; i++ {
				if encoded[i]&0x80 == 0 {
					t.Errorf("byte[%d] = 0x%02x: continuation bit not set", i, encoded[i])
				}
			}

			// Round-trip: decode back and compare.
			decoded, consumed := getVarint(encoded, 0)
			if consumed != 9 {
				t.Errorf("getVarint consumed %d bytes, want 9", consumed)
			}
			if decoded != v {
				t.Errorf("round-trip: encoded 0x%x, decoded 0x%x", v, decoded)
			}
		})
	}
}

// TestExecEncoding_EncodeValueDefault covers the default (unknown-type) branch
// in encodeValue, which returns (0, nil) for any type that is not nil, int64,
// float64, string, or []byte.
func TestExecEncoding_EncodeValueDefault(t *testing.T) {
	t.Parallel()

	// Various Go types that the switch does not handle.
	unknowns := []interface{}{
		int(42), // plain int, not int64
		uint64(1),
		bool(true),
		struct{}{},
		[]int{1, 2},
	}

	for _, u := range unknowns {
		u := u
		t.Run("", func(t *testing.T) {
			t.Parallel()
			st, data := encodeValue(u)
			if st != 0 {
				t.Errorf("encodeValue(%T): serial type = %d, want 0", u, st)
			}
			if data != nil {
				t.Errorf("encodeValue(%T): data = %v, want nil", u, data)
			}
		})
	}
}

// TestExecEncoding_EncodeValueAllBranches covers every typed branch in
// encodeValue (nil, int64 with sub-variants, float64, string, []byte) and
// verifies the returned serial type and data length.
func TestExecEncoding_EncodeValueAllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		val        interface{}
		wantSerial uint64
		wantLen    int // -1 means "don't check length"
	}{
		// nil → serial 0, no data
		{"nil", nil, 0, 0},

		// int64 special constants
		{"int64-zero", int64(0), 8, 0},
		{"int64-one", int64(1), 9, 0},

		// int64 in each serial-type size band
		{"int64-int8-pos", int64(100), 1, 1},
		{"int64-int8-neg", int64(-100), 1, 1},
		{"int64-int16", int64(1000), 2, 2},
		{"int64-int24", int64(100000), 3, 3},
		{"int64-int32", int64(10000000), 4, 4},
		{"int64-int64", int64(10000000000), 6, 8},

		// float64 → serial 7, always 8 bytes
		{"float64-pi", float64(3.14159), 7, 8},
		{"float64-neg", float64(-1.0), 7, 8},
		{"float64-zero", float64(0.0), 7, 8},

		// string → serial = 2*len+13
		{"string-empty", "", 13, 0},
		{"string-hello", "hello", 23, 5},

		// []byte → serial = 2*len+12
		{"blob-empty", []byte{}, 12, 0},
		{"blob-4", []byte{1, 2, 3, 4}, 20, 4},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st, data := encodeValue(tt.val)
			if st != tt.wantSerial {
				t.Errorf("serial type: got %d, want %d", st, tt.wantSerial)
			}
			if tt.wantLen >= 0 && len(data) != tt.wantLen {
				t.Errorf("data length: got %d, want %d", len(data), tt.wantLen)
			}
		})
	}
}

// TestExecEncoding_ParseSerialBlobOrTextTruncated covers the error branch in
// parseSerialBlobOrText where offset+length exceeds len(data).
// The function must set the Mem to NULL and return a non-nil error.
func TestExecEncoding_ParseSerialBlobOrTextTruncated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		data   []byte
		offset int
		st     uint64 // serial type: even→blob, odd→text; length = (st-12)/2
	}{
		// st=14 → length=1; offset=0; data empty → truncated
		{"blob-1-empty-data", []byte{}, 0, 14},
		// st=15 → length=1; offset=5; data only 4 bytes → truncated
		{"text-1-short-data", []byte{0x41, 0x42, 0x43, 0x44}, 5, 15},
		// st=20 → length=4; offset=2; only 2 bytes available → truncated
		{"blob-4-partial", []byte{0x01, 0x02}, 2, 20},
		// st=21 → length=4; offset=0; data is 3 bytes → truncated
		{"text-4-short", []byte{'a', 'b', 'c'}, 0, 21},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMem()
			m.SetInt(99) // pre-set to non-NULL to verify it becomes NULL

			err := parseSerialBlobOrText(tt.data, tt.offset, tt.st, m)
			if err == nil {
				t.Error("expected non-nil error for truncated data, got nil")
			}
			if !m.IsNull() {
				t.Error("Mem should be NULL after truncation error")
			}
		})
	}
}

// TestExecEncoding_ParseSerialBlobOrTextRoundTrip covers both the blob (even st)
// and text (odd st) success branches of parseSerialBlobOrText directly.
func TestExecEncoding_ParseSerialBlobOrTextRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		data   []byte
		offset int
		st     uint64
		isBlob bool
		wantS  string // for text
		wantB  []byte // for blob
	}{
		{
			name:   "blob-even-st-14",
			data:   []byte{0xAB},
			offset: 0, st: 14, isBlob: true,
			wantB: []byte{0xAB},
		},
		{
			name:   "text-odd-st-15",
			data:   []byte{'G', 'o'},
			offset: 0, st: 15, isBlob: false,
			wantS: "G",
		},
		{
			name:   "blob-with-offset",
			data:   []byte{0x00, 0x00, 0xDE, 0xAD},
			offset: 2, st: 16, isBlob: true,
			wantB: []byte{0xDE, 0xAD},
		},
		{
			name:   "text-with-offset",
			data:   []byte{0x00, 0x00, 'h', 'i'},
			offset: 2, st: 17, isBlob: false,
			wantS: "hi",
		},
		{
			name:   "empty-blob-st-12",
			data:   []byte{},
			offset: 0, st: 12, isBlob: true,
			wantB: []byte{},
		},
		{
			name:   "empty-text-st-13",
			data:   []byte{},
			offset: 0, st: 13, isBlob: false,
			wantS: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := NewMem()
			err := parseSerialBlobOrText(tt.data, tt.offset, tt.st, m)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.isBlob {
				if !m.IsBlob() {
					t.Error("expected Blob Mem type")
				}
				if !bytes.Equal(m.BlobValue(), tt.wantB) {
					t.Errorf("blob mismatch: got %v, want %v", m.BlobValue(), tt.wantB)
				}
			} else {
				if !m.IsString() {
					t.Error("expected String Mem type")
				}
				if m.StrValue() != tt.wantS {
					t.Errorf("text mismatch: got %q, want %q", m.StrValue(), tt.wantS)
				}
			}
		})
	}
}

// TestExecEncoding_VarintRoundTripHighBytes verifies that encodeVarint +
// getVarint round-trips correctly for 6-, 7-, 8-, and 9-byte varint values.
// This exercises encodeVarintN for n=6,7,8 (the generic loop path) and n=9
// (the special-case path) together.
func TestExecEncoding_VarintRoundTripHighBytes(t *testing.T) {
	t.Parallel()

	cases := []uint64{
		// 6-byte
		0x800000000,
		0x3ffffffffff,
		// 7-byte
		0x40000000000,
		0x1ffffffffffff,
		// 8-byte
		0x20000000000000,
		0xffffffffffffff,
		// 9-byte
		0x100000000000000,
		0x7fffffffffffffff,
		0xffffffffffffffff,
	}

	for _, v := range cases {
		v := v
		t.Run("", func(t *testing.T) {
			t.Parallel()
			encoded := encodeVarint(v)
			wantLen := varintLen(v)
			if len(encoded) != wantLen {
				t.Fatalf("encodeVarint(0x%x): got %d bytes, want %d", v, len(encoded), wantLen)
			}
			decoded, consumed := getVarint(encoded, 0)
			if consumed != wantLen {
				t.Errorf("getVarint consumed %d bytes, want %d", consumed, wantLen)
			}
			if decoded != v {
				t.Errorf("round-trip: 0x%x → encoded → 0x%x", v, decoded)
			}
		})
	}
}
