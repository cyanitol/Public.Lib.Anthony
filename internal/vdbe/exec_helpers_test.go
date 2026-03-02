// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"math"
	"testing"
)

// TestGetVarint tests the getVarint and getVarintGeneral functions
func TestGetVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		data   []byte
		offset int
		want   uint64
		wantN  int
	}{
		{
			name:   "1-byte varint",
			data:   []byte{0x42},
			offset: 0,
			want:   0x42,
			wantN:  1,
		},
		{
			name:   "2-byte varint",
			data:   []byte{0x81, 0x02},
			offset: 0,
			want:   (1 << 7) | 2,
			wantN:  2,
		},
		{
			name:   "3-byte varint",
			data:   []byte{0x81, 0x82, 0x03},
			offset: 0,
			want:   (1 << 14) | (2 << 7) | 3,
			wantN:  3,
		},
		{
			name:   "offset buffer",
			data:   []byte{0xFF, 0xFF, 0x42},
			offset: 2,
			want:   0x42,
			wantN:  1,
		},
		{
			name:   "empty buffer",
			data:   []byte{},
			offset: 0,
			want:   0,
			wantN:  0,
		},
		{
			name:   "offset beyond buffer",
			data:   []byte{0x42},
			offset: 5,
			want:   0,
			wantN:  0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			got, gotN := getVarint(tt.data, tt.offset)
			if got != tt.want || gotN != tt.wantN {
				t.Errorf("getVarint(%v, %d) = (%d, %d), want (%d, %d)",
					tt.data, tt.offset, got, gotN, tt.want, tt.wantN)
			}
		})
	}
}

// TestParseSignedInt24 tests the parseSignedInt24 function
func TestParseSignedInt24(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
		want int64
	}{
		{
			name: "positive small",
			data: []byte{0x00, 0x00, 0x42},
			want: 0x42,
		},
		{
			name: "positive large",
			data: []byte{0x12, 0x34, 0x56},
			want: 0x123456,
		},
		{
			name: "negative (sign bit set)",
			data: []byte{0xFF, 0xFF, 0xFF},
			want: -1,
		},
		{
			name: "negative small",
			data: []byte{0xFF, 0xFF, 0xFE},
			want: -2,
		},
		{
			name: "max positive",
			data: []byte{0x7F, 0xFF, 0xFF},
			want: 0x7FFFFF,
		},
		{
			name: "min negative",
			data: []byte{0x80, 0x00, 0x00},
			want: -0x800000,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			got := parseSignedInt24(tt.data)
			if got != tt.want {
				t.Errorf("parseSignedInt24(%v) = %d, want %d", tt.data, got, tt.want)
			}
		})
	}
}

// TestParseSignedInt48 tests the parseSignedInt48 function
func TestParseSignedInt48(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
		want int64
	}{
		{
			name: "positive small",
			data: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x42},
			want: 0x42,
		},
		{
			name: "positive large",
			data: []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC},
			want: 0x123456789ABC,
		},
		{
			name: "negative",
			data: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			want: -1,
		},
		{
			name: "max positive",
			data: []byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			want: 0x7FFFFFFFFFFF,
		},
		{
			name: "min negative",
			data: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00},
			want: -0x800000000000,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			got := parseSignedInt48(tt.data)
			if got != tt.want {
				t.Errorf("parseSignedInt48(%v) = %d, want %d", tt.data, got, tt.want)
			}
		})
	}
}

// TestParseSerialFloat tests the parseSerialFloat function
func TestParseSerialFloat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		data   []byte
		offset int
		want   float64
		errNil bool
	}{
		{
			name:   "zero",
			data:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset: 0,
			want:   0.0,
			errNil: true,
		},
		{
			name:   "one",
			data:   []byte{0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset: 0,
			want:   1.0,
			errNil: true,
		},
		{
			name:   "pi",
			data:   []byte{0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18},
			offset: 0,
			want:   math.Pi,
			errNil: true,
		},
		{
			name:   "truncated",
			data:   []byte{0x40, 0x09, 0x21},
			offset: 0,
			want:   0,
			errNil: false,
		},
		{
			name:   "with offset",
			data:   []byte{0xFF, 0xFF, 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			offset: 2,
			want:   1.0,
			errNil: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			mem := NewMem()
			err := parseSerialFloat(tt.data, tt.offset, mem)

			if tt.errNil && err != nil {
				t.Errorf("parseSerialFloat() error = %v, want nil", err)
			}
			if !tt.errNil && err == nil {
				t.Error("parseSerialFloat() error = nil, want error")
			}
			if tt.errNil && mem.RealValue() != tt.want {
				t.Errorf("parseSerialFloat() = %f, want %f", mem.RealValue(), tt.want)
			}
		})
	}
}

// TestEncodeVarint tests the encodeVarint function
func TestEncodeVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
	}{
		{"small", 127},
		{"medium", 16383},
		{"large", 2097151},
		{"very large", 268435455},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			buf := encodeVarint(tt.value)

			// Decode it back
			decoded, decodedN := getVarint(buf, 0)

			if decoded != tt.value {
				t.Errorf("encode/decode mismatch: encoded %d, decoded %d", tt.value, decoded)
			}

			if len(buf) != decodedN {
				t.Errorf("byte count mismatch: encoded %d bytes, decoded %d bytes", len(buf), decodedN)
			}
		})
	}
}

// TestVarintLen tests the varintLen function
func TestVarintLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		want  int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{2097151, 3},
		{2097152, 4},
		{268435455, 4},
		{268435456, 5},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
		t.Parallel()
			got := varintLen(tt.value)
			if got != tt.want {
				t.Errorf("varintLen(%d) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

// TestSerialTypeLen tests the serialTypeLen function
func TestSerialTypeLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		serialType uint64
		want       int
	}{
		{0, 0},  // NULL
		{1, 1},  // int8
		{2, 2},  // int16
		{3, 3},  // int24
		{4, 4},  // int32
		{5, 6},  // int48
		{6, 8},  // int64
		{7, 8},  // float64
		{8, 0},  // constant 0
		{9, 0},  // constant 1
		{10, 0}, // reserved
		{11, 0}, // reserved
		{12, 0}, // blob/text length 0
		{13, 0}, // blob/text length 0
		{14, 1}, // blob length 1
		{15, 1}, // text length 1
		{16, 2}, // blob length 2
		{17, 2}, // text length 2
		{100, 44}, // (100-12)/2 = 44
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(t *testing.T) {
		t.Parallel()
			got := serialTypeLen(tt.serialType)
			if got != tt.want {
				t.Errorf("serialTypeLen(%d) = %d, want %d", tt.serialType, got, tt.want)
			}
		})
	}
}

// TestMemToInterface tests the memToInterface helper function
func TestMemToInterface(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func() *Mem
		want  interface{}
	}{
		{
			name: "null",
			setup: func() *Mem {
				m := NewMem()
				m.SetNull()
				return m
			},
			want: nil,
		},
		{
			name: "integer",
			setup: func() *Mem {
				m := NewMem()
				m.SetInt(42)
				return m
			},
			want: int64(42),
		},
		{
			name: "real",
			setup: func() *Mem {
				m := NewMem()
				m.SetReal(3.14)
				return m
			},
			want: 3.14,
		},
		{
			name: "string",
			setup: func() *Mem {
				m := NewMem()
				m.SetStr("hello")
				return m
			},
			want: "hello",
		},
		{
			name: "blob",
			setup: func() *Mem {
				m := NewMem()
				m.SetBlob([]byte{1, 2, 3})
				return m
			},
			want: []byte{1, 2, 3},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
		t.Parallel()
			mem := tt.setup()
			got := memToInterface(mem)

			// For byte slices, do a deep comparison
			if wantBytes, ok := tt.want.([]byte); ok {
				if gotBytes, ok := got.([]byte); ok {
					if len(wantBytes) != len(gotBytes) {
						t.Errorf("length mismatch: got %d, want %d", len(gotBytes), len(wantBytes))
						return
					}
					for i := range wantBytes {
						if wantBytes[i] != gotBytes[i] {
							t.Errorf("byte mismatch at %d: got %d, want %d", i, gotBytes[i], wantBytes[i])
							return
						}
					}
					return
				}
			}

			if got != tt.want {
				t.Errorf("memToInterface() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}
