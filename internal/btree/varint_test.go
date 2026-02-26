package btree

import (
	"testing"
)

func TestPutGetVarint(t *testing.T) {
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
		got := VarintLen(tt.value)
		if got != tt.want {
			t.Errorf("VarintLen(0x%x) = %d, want %d", tt.value, got, tt.want)
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
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
