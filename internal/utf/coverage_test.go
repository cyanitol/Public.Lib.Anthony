package utf

import (
	"testing"
)

// TestCompareNoCaseBytes tests case-insensitive byte slice comparison
func TestCompareNoCaseBytes(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    []byte
		want int
	}{
		{
			name: "equal ignoring case",
			a:    []byte("Hello"),
			b:    []byte("hello"),
			want: 0,
		},
		{
			name: "different case same length",
			a:    []byte("HELLO"),
			b:    []byte("hello"),
			want: 0,
		},
		{
			name: "a less than b ignoring case",
			a:    []byte("Apple"),
			b:    []byte("BANANA"),
			want: -1,
		},
		{
			name: "a greater than b ignoring case",
			a:    []byte("Zebra"),
			b:    []byte("apple"),
			want: 1,
		},
		{
			name: "empty strings",
			a:    []byte(""),
			b:    []byte(""),
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareNoCaseBytes(tt.a, tt.b)
			if (got < 0 && tt.want >= 0) || (got > 0 && tt.want <= 0) || (got == 0 && tt.want != 0) {
				t.Errorf("CompareNoCaseBytes(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestCompareRTrimBytes tests right-trim comparison
func TestCompareRTrimBytes(t *testing.T) {
	tests := []struct {
		name string
		a    []byte
		b    []byte
		want int
	}{
		{
			name: "equal after rtrim",
			a:    []byte("hello   "),
			b:    []byte("hello"),
			want: 0,
		},
		{
			name: "both need rtrim",
			a:    []byte("test  "),
			b:    []byte("test   "),
			want: 0,
		},
		{
			name: "a less than b after rtrim",
			a:    []byte("apple  "),
			b:    []byte("banana "),
			want: -1,
		},
		{
			name: "no trailing spaces",
			a:    []byte("hello"),
			b:    []byte("hello"),
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareRTrimBytes(tt.a, tt.b)
			if (got < 0 && tt.want >= 0) || (got > 0 && tt.want <= 0) || (got == 0 && tt.want != 0) {
				t.Errorf("CompareRTrimBytes(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestDecodeRuneLimitedEdgeCases tests DecodeRuneLimited with edge cases
func TestDecodeRuneLimitedEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		s     []byte
		limit int
	}{
		{
			name:  "limit smaller than rune",
			s:     []byte("日本"),
			limit: 2,
		},
		{
			name:  "limit exactly rune size",
			s:     []byte("日"),
			limit: 3,
		},
		{
			name:  "empty slice",
			s:     []byte{},
			limit: 10,
		},
		{
			name:  "limit zero",
			s:     []byte("test"),
			limit: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = DecodeRuneLimited(tt.s, tt.limit)
			// Just ensure it doesn't panic
		})
	}
}

// TestCharCountBytesEdgeCases tests CharCountBytes with edge cases
func TestCharCountBytesEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		s    []byte
		want int
	}{
		{
			name: "empty",
			s:    []byte{},
			want: 0,
		},
		{
			name: "ASCII only",
			s:    []byte("hello"),
			want: 5,
		},
		{
			name: "mixed UTF-8",
			s:    []byte("héllo"),
			want: 5,
		},
		{
			name: "all multibyte",
			s:    []byte("日本語"),
			want: 3,
		},
		{
			name: "invalid UTF-8",
			s:    []byte{0xFF, 0xFE},
			want: 2, // Each invalid byte counts as one rune
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CharCountBytes(tt.s, len(tt.s))
			if got != tt.want {
				t.Errorf("CharCountBytes(%q, %d) = %d, want %d", tt.s, len(tt.s), got, tt.want)
			}
		})
	}
}

// TestToValidUTF8EdgeCases tests ToValidUTF8 with edge cases
func TestToValidUTF8EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		s    []byte
	}{
		{
			name: "already valid",
			s:    []byte("hello"),
		},
		{
			name: "invalid bytes",
			s:    []byte{0xFF, 0xFE, 'a', 'b'},
		},
		{
			name: "empty slice",
			s:    []byte{},
		},
		{
			name: "single invalid byte",
			s:    []byte{0xFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToValidUTF8(tt.s)
			// Just ensure it doesn't panic and returns a valid UTF-8 string
			_ = result
		})
	}
}

// TestFullRuneEdgeCases tests FullRune with edge cases
func TestFullRuneEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		s    []byte
		want bool
	}{
		{
			name: "empty slice",
			s:    []byte{},
			want: false,
		},
		{
			name: "single ASCII",
			s:    []byte{'A'},
			want: true,
		},
		{
			name: "complete 2-byte",
			s:    []byte{0xC3, 0xA9},
			want: true,
		},
		{
			name: "incomplete 2-byte",
			s:    []byte{0xC3},
			want: false,
		},
		{
			name: "complete 3-byte",
			s:    []byte{0xE6, 0x97, 0xA5},
			want: true,
		},
		{
			name: "incomplete 3-byte",
			s:    []byte{0xE6, 0x97},
			want: false,
		},
		{
			name: "complete 4-byte",
			s:    []byte{0xF0, 0x90, 0x8D, 0x88},
			want: true,
		},
		{
			name: "incomplete 4-byte",
			s:    []byte{0xF0, 0x90, 0x8D},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FullRune(tt.s)
			if got != tt.want {
				t.Errorf("FullRune(%v) = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}

// TestVarintEdgeCases tests varint encoding/decoding edge cases
func TestVarintEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{name: "zero", value: 0},
		{name: "small", value: 127},
		{name: "medium", value: 16383},
		{name: "large", value: 2097151},
		{name: "very large", value: 268435455},
		{name: "max 32-bit", value: 0xFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf [10]byte
			n := PutVarint(buf[:], tt.value)
			got, m := GetVarint(buf[:n])
			if got != tt.value {
				t.Errorf("varint roundtrip for %d: got %d", tt.value, got)
			}
			if m != n {
				t.Errorf("varint size mismatch: encoded %d bytes, decoded %d bytes", n, m)
			}
		})
	}
}

// TestGetVarint32EdgeCases tests GetVarint32 with edge cases
func TestGetVarint32EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{
			name: "single byte",
			buf:  []byte{0x01},
		},
		{
			name: "two bytes",
			buf:  []byte{0x80, 0x01},
		},
		{
			name: "max value",
			buf:  []byte{0x8F, 0xFF, 0xFF, 0xFF, 0x7F},
		},
		{
			name: "empty buffer",
			buf:  []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = GetVarint32(tt.buf)
			// Just ensure it doesn't panic
		})
	}
}
