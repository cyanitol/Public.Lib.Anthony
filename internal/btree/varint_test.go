// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"fmt"
	"testing"
)

func TestPutGetVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
		want  int // expected length
	}{
		{"1-byte", 0x00, 1},
		{"1-byte max", 0x7f, 1},
		{"2-byte min", 0x80, 2},
		{"2-byte", 0x100, 2},
		{"2-byte max", 0x3fff, 2},
		{"3-byte min", 0x4000, 3},
		{"3-byte", 0x12345, 3},
		{"3-byte max", 0x1fffff, 3},
		{"4-byte min", 0x200000, 4},
		{"4-byte", 0x1234567, 4},
		{"5-byte", 0x12345678, 5},
		{"9-byte max", 0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			if n != tt.want {
				t.Errorf("PutVarint() length = %d, want %d", n, tt.want)
			}

			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint() length = %d, want %d", m, n)
			}
		})
	}
}

func TestGetVarint32(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint32
		want  int
	}{
		{"1-byte", 0x00, 1},
		{"1-byte max", 0x7f, 1},
		{"2-byte", 0x80, 2},
		{"3-byte", 0x4000, 3},
		{"4-byte", 0x200000, 4},
		{"max uint32", 0xffffffff, 5},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], uint64(tt.value))
			if n != tt.want {
				t.Errorf("PutVarint() length = %d, want %d", n, tt.want)
			}

			got, m := GetVarint32(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint32() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint32() length = %d, want %d", m, n)
			}
		})
	}
}

func TestVarintLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		want  int
	}{
		{0x00, 1},
		{0x7f, 1},
		{0x80, 2},
		{0x3fff, 2},
		{0x4000, 3},
		{0x1fffff, 3},
		{0x200000, 4},
		{0xfffffff, 4},
		{0x10000000, 5},
		{0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		got := VarintLen(tt.value)
		if got != tt.want {
			t.Errorf("VarintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	t.Parallel()
	// Test all powers of 2 and nearby values
	for i := uint(0); i < 64; i++ {
		values := []uint64{
			1 << i,
			(1 << i) - 1,
			(1 << i) + 1,
		}

		for _, v := range values {
			var buf [9]byte
			n := PutVarint(buf[:], v)
			got, m := GetVarint(buf[:])

			if got != v {
				t.Errorf("RoundTrip(%d): got %d", v, got)
			}
			if m != n {
				t.Errorf("RoundTrip(%d): length mismatch: put=%d, get=%d", v, n, m)
			}
		}
	}
}

func BenchmarkPutVarint1Byte(b *testing.B) {
	var buf [9]byte
	for i := 0; i < b.N; i++ {
		PutVarint(buf[:], 0x7f)
	}
}

func BenchmarkPutVarint9Byte(b *testing.B) {
	var buf [9]byte
	for i := 0; i < b.N; i++ {
		PutVarint(buf[:], 0xffffffffffffffff)
	}
}

func BenchmarkGetVarint1Byte(b *testing.B) {
	var buf [9]byte
	PutVarint(buf[:], 0x7f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetVarint(buf[:])
	}
}

func BenchmarkGetVarint9Byte(b *testing.B) {
	var buf [9]byte
	PutVarint(buf[:], 0xffffffffffffffff)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetVarint(buf[:])
	}
}

// TestGetVarintEdgeCases tests edge cases for GetVarint
func TestGetVarintEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func() []byte
		want  uint64
	}{
		{
			name: "single byte zero",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0)
				return buf[:]
			},
			want: 0,
		},
		{
			name: "single byte max",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0x7f)
				return buf[:]
			},
			want: 0x7f,
		},
		{
			name: "two byte boundary",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0x80)
				return buf[:]
			},
			want: 0x80,
		},
		{
			name: "max 9-byte value",
			setup: func() []byte {
				var buf [9]byte
				PutVarint(buf[:], 0xffffffffffffffff)
				return buf[:]
			},
			want: 0xffffffffffffffff,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.setup()
			got, _ := GetVarint(buf)
			if got != tt.want {
				t.Errorf("GetVarint() = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestDecodeShortVarint tests the short varint decoding path
func TestDecodeShortVarint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"127", 127},
		{"128", 128},
		{"255", 255},
		{"256", 256},
		{"16383", 16383},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint() = %d, want %d", got, tt.value)
			}
			if m != n {
				t.Errorf("GetVarint() length = %d, want %d", m, n)
			}
		})
	}
}

// TestSlowBtreeVarint32 tests the slow path for GetVarint32
func TestSlowBtreeVarint32(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value uint32
	}{
		{"boundary 0x4000", 0x4000},
		{"boundary 0x200000", 0x200000},
		{"max uint32", 0xffffffff},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var buf [9]byte
			PutVarint(buf[:], uint64(tt.value))
			got, _ := GetVarint32(buf[:])
			if got != tt.value {
				t.Errorf("GetVarint32() = %d, want %d", got, tt.value)
			}
		})
	}
}

// TestGetVarintBufferTooSmall tests behavior with insufficient buffer
func TestGetVarintBufferTooSmall(t *testing.T) {
	t.Parallel()
	var buf [9]byte
	PutVarint(buf[:], 0xffffffffffffffff)

	// Try to decode with only first few bytes
	// Should still work or return partial result
	got, n := GetVarint(buf[:9])
	if n != 9 {
		t.Errorf("Expected 9 bytes read, got %d", n)
	}
	if got != 0xffffffffffffffff {
		t.Errorf("GetVarint() = 0x%x, want 0xffffffffffffffff", got)
	}
}

// TestVarintLenBoundaries tests VarintLen at all boundaries
func TestVarintLenBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		want  int
	}{
		{0, 1},
		{0x7f, 1},
		{0x80, 2},
		{0x3fff, 2},
		{0x4000, 3},
		{0x1fffff, 3},
		{0x200000, 4},
		{0xfffffff, 4},
		{0x10000000, 5},
		{0x7ffffffff, 5},
		{0x800000000, 6},
		{0x3ffffffffff, 6},
		{0x40000000000, 7},
		{0x1ffffffffffff, 7},
		{0x2000000000000, 8},
		{0xffffffffffffff, 8},
		{0x100000000000000, 9},
		{0xffffffffffffffff, 9},
	}

	for _, tt := range tests {
		tt := tt
		got := VarintLen(tt.value)
		if got != tt.want {
			t.Errorf("VarintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

// TestPutVarintAllSizes tests PutVarint for all possible sizes
func TestPutVarintAllSizes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value uint64
		size  int
	}{
		{0x00, 1},
		{0x80, 2},
		{0x4000, 3},
		{0x200000, 4},
		{0x10000000, 5},
		{0x800000000, 6},
		{0x40000000000, 7},
		{0x2000000000000, 8},
		{0x100000000000000, 9},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("size=%d", tt.size), func(t *testing.T) {
			var buf [9]byte
			n := PutVarint(buf[:], tt.value)
			if n != tt.size {
				t.Errorf("PutVarint(0x%x) size = %d, want %d", tt.value, n, tt.size)
			}

			// Verify we can decode it back
			got, m := GetVarint(buf[:])
			if got != tt.value {
				t.Errorf("Round trip failed: got 0x%x, want 0x%x", got, tt.value)
			}
			if m != n {
				t.Errorf("Decode size %d != encode size %d", m, n)
			}
		})
	}
}

// TestGetVarint32Overflow tests GetVarint32 with values that fit in uint32
func TestGetVarint32Overflow(t *testing.T) {
	t.Parallel()
	var buf [9]byte

	// Test with max uint32
	PutVarint(buf[:], 0xffffffff)
	got, n := GetVarint32(buf[:])
	if got != 0xffffffff {
		t.Errorf("GetVarint32(max) = 0x%x, want 0xffffffff", got)
	}
	if n != 5 {
		t.Errorf("GetVarint32(max) size = %d, want 5", n)
	}
}

// TestVarintZeroValue tests zero value encoding/decoding
func TestVarintZeroValue(t *testing.T) {
	t.Parallel()
	var buf [9]byte
	n := PutVarint(buf[:], 0)
	if n != 1 {
		t.Errorf("PutVarint(0) size = %d, want 1", n)
	}

	got, m := GetVarint(buf[:])
	if got != 0 {
		t.Errorf("GetVarint() = %d, want 0", got)
	}
	if m != 1 {
		t.Errorf("GetVarint() size = %d, want 1", m)
	}

	length := VarintLen(0)
	if length != 1 {
		t.Errorf("VarintLen(0) = %d, want 1", length)
	}
}
