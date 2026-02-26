package utf

import (
	"testing"
)

func TestPutVarint(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected []byte
		size     int
	}{
		{"zero", 0, []byte{0x00}, 1},
		{"small 1-byte", 0x7F, []byte{0x7F}, 1},
		{"max 1-byte", 0x7F, []byte{0x7F}, 1},
		{"min 2-byte", 0x80, []byte{0x81, 0x00}, 2},
		{"2-byte", 0x3FFF, []byte{0xFF, 0x7F}, 2},
		{"3-byte", 0x4000, []byte{0x81, 0x80, 0x00}, 3},
		{"large", 0x1FFFFF, []byte{0xFF, 0xFF, 0x7F}, 3},
		{"max 32-bit", 0xFFFFFFFF, []byte{0x8F, 0xFF, 0xFF, 0xFF, 0x7F}, 5},
		{"max 64-bit", 0xFFFFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 9)
			n := PutVarint(buf, tt.value)
			if n != tt.size {
				t.Errorf("PutVarint(%d) returned size %d, want %d", tt.value, n, tt.size)
			}
			for i := 0; i < n; i++ {
				if buf[i] != tt.expected[i] {
					t.Errorf("PutVarint(%d)[%d] = 0x%02X, want 0x%02X", tt.value, i, buf[i], tt.expected[i])
				}
			}
		})
	}
}

func TestGetVarint(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint64
		size     int
	}{
		{"zero", []byte{0x00}, 0, 1},
		{"small", []byte{0x7F}, 0x7F, 1},
		{"2-byte", []byte{0x81, 0x00}, 0x80, 2},
		{"2-byte max", []byte{0xFF, 0x7F}, 0x3FFF, 2},
		{"3-byte", []byte{0x81, 0x80, 0x00}, 0x4000, 3},
		{"large", []byte{0xFF, 0xFF, 0x7F}, 0x1FFFFF, 3},
		{"9-byte", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFFFFFFFFFF, 9},
		{"empty buffer", []byte{}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, size := GetVarint(tt.data)
			if value != tt.expected {
				t.Errorf("GetVarint() value = %d, want %d", value, tt.expected)
			}
			if size != tt.size {
				t.Errorf("GetVarint() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestVarintRoundTrip(t *testing.T) {
	tests := []uint64{
		0,
		1,
		127,
		128,
		255,
		256,
		16383,
		16384,
		0xFFFF,
		0x10000,
		0xFFFFFF,
		0x1000000,
		0xFFFFFFFF,
		0x100000000,
		0xFFFFFFFFFFFFFFFF,
	}

	for _, value := range tests {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 9)
			n := PutVarint(buf, value)
			decoded, size := GetVarint(buf[:n])
			if decoded != value {
				t.Errorf("Round trip failed: put %d, got %d", value, decoded)
			}
			if size != n {
				t.Errorf("Size mismatch: encoded %d bytes, decoded %d bytes", n, size)
			}
		})
	}
}

func TestGetVarint32(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
		size     int
	}{
		{"zero", []byte{0x00}, 0, 1},
		{"small", []byte{0x7F}, 0x7F, 1},
		{"2-byte", []byte{0x81, 0x00}, 0x80, 2},
		{"3-byte", []byte{0x81, 0x80, 0x00}, 0x4000, 3},
		{"max 32-bit", []byte{0x8F, 0xFF, 0xFF, 0xFF, 0x7F}, 0xFFFFFFFF, 5},
		{"overflow", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFF, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, size := GetVarint32(tt.data)
			if value != tt.expected {
				t.Errorf("GetVarint32() value = %d, want %d", value, tt.expected)
			}
			if size != tt.size {
				t.Errorf("GetVarint32() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestVarintLen(t *testing.T) {
	tests := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{0x7F, 1},
		{0x80, 2},
		{0x3FFF, 2},
		{0x4000, 3},
		{0x1FFFFF, 3},
		{0x200000, 4},
		{0xFFFFFFF, 4},
		{0x10000000, 5},
		{0xFFFFFFFF, 5},
		{0x100000000, 5},
		{0xFFFFFFFFFFFFFFFF, 9},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := VarintLen(tt.value)
			if result != tt.expected {
				t.Errorf("VarintLen(%d) = %d, want %d", tt.value, result, tt.expected)
			}

			// Verify with actual encoding
			buf := make([]byte, 9)
			n := PutVarint(buf, tt.value)
			if n != result {
				t.Errorf("VarintLen(%d) = %d, but PutVarint encoded %d bytes", tt.value, result, n)
			}
		})
	}
}

func TestPut4Byte(t *testing.T) {
	tests := []struct {
		name     string
		value    uint32
		expected []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"small", 0x01020304, []byte{0x01, 0x02, 0x03, 0x04}},
		{"max", 0xFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"one", 1, []byte{0x00, 0x00, 0x00, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			Put4Byte(buf, tt.value)
			for i := 0; i < 4; i++ {
				if buf[i] != tt.expected[i] {
					t.Errorf("Put4Byte(%d)[%d] = 0x%02X, want 0x%02X", tt.value, i, buf[i], tt.expected[i])
				}
			}
		})
	}
}

func TestGet4Byte(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00}, 0},
		{"small", []byte{0x01, 0x02, 0x03, 0x04}, 0x01020304},
		{"max", []byte{0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFF},
		{"one", []byte{0x00, 0x00, 0x00, 0x01}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Get4Byte(tt.data)
			if result != tt.expected {
				t.Errorf("Get4Byte() = 0x%08X, want 0x%08X", result, tt.expected)
			}
		})
	}
}

func Test4ByteRoundTrip(t *testing.T) {
	tests := []uint32{
		0,
		1,
		0x01020304,
		0x7FFFFFFF,
		0x80000000,
		0xFFFFFFFF,
	}

	for _, value := range tests {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 4)
			Put4Byte(buf, value)
			decoded := Get4Byte(buf)
			if decoded != value {
				t.Errorf("Round trip failed: put 0x%08X, got 0x%08X", value, decoded)
			}
		})
	}
}

func TestPut8Byte(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"small", 0x0102030405060708, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
		{"max", 0xFFFFFFFFFFFFFFFF, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 8)
			Put8Byte(buf, tt.value)
			for i := 0; i < 8; i++ {
				if buf[i] != tt.expected[i] {
					t.Errorf("Put8Byte(%d)[%d] = 0x%02X, want 0x%02X", tt.value, i, buf[i], tt.expected[i])
				}
			}
		})
	}
}

func TestGet8Byte(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint64
	}{
		{"zero", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0},
		{"small", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 0x0102030405060708},
		{"max", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, 0xFFFFFFFFFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Get8Byte(tt.data)
			if result != tt.expected {
				t.Errorf("Get8Byte() = 0x%016X, want 0x%016X", result, tt.expected)
			}
		})
	}
}

func Test8ByteRoundTrip(t *testing.T) {
	tests := []uint64{
		0,
		1,
		0x0102030405060708,
		0x7FFFFFFFFFFFFFFF,
		0x8000000000000000,
		0xFFFFFFFFFFFFFFFF,
	}

	for _, value := range tests {
		t.Run("", func(t *testing.T) {
			buf := make([]byte, 8)
			Put8Byte(buf, value)
			decoded := Get8Byte(buf)
			if decoded != value {
				t.Errorf("Round trip failed: put 0x%016X, got 0x%016X", value, decoded)
			}
		})
	}
}

// Benchmarks
func BenchmarkPutVarint1Byte(b *testing.B) {
	buf := make([]byte, 9)
	for i := 0; i < b.N; i++ {
		PutVarint(buf, 0x7F)
	}
}

func BenchmarkPutVarint2Byte(b *testing.B) {
	buf := make([]byte, 9)
	for i := 0; i < b.N; i++ {
		PutVarint(buf, 0x3FFF)
	}
}

func BenchmarkPutVarint9Byte(b *testing.B) {
	buf := make([]byte, 9)
	for i := 0; i < b.N; i++ {
		PutVarint(buf, 0xFFFFFFFFFFFFFFFF)
	}
}

func BenchmarkGetVarint1Byte(b *testing.B) {
	buf := []byte{0x7F}
	for i := 0; i < b.N; i++ {
		GetVarint(buf)
	}
}

func BenchmarkGetVarint2Byte(b *testing.B) {
	buf := []byte{0xFF, 0x7F}
	for i := 0; i < b.N; i++ {
		GetVarint(buf)
	}
}

func BenchmarkGetVarint9Byte(b *testing.B) {
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	for i := 0; i < b.N; i++ {
		GetVarint(buf)
	}
}

func BenchmarkVarintLen(b *testing.B) {
	for i := 0; i < b.N; i++ {
		VarintLen(0xFFFFFFFF)
	}
}

func BenchmarkPut4Byte(b *testing.B) {
	buf := make([]byte, 4)
	for i := 0; i < b.N; i++ {
		Put4Byte(buf, 0x01020304)
	}
}

func BenchmarkGet4Byte(b *testing.B) {
	buf := []byte{0x01, 0x02, 0x03, 0x04}
	for i := 0; i < b.N; i++ {
		Get4Byte(buf)
	}
}

func BenchmarkPut8Byte(b *testing.B) {
	buf := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		Put8Byte(buf, 0x0102030405060708)
	}
}

func BenchmarkGet8Byte(b *testing.B) {
	buf := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	for i := 0; i < b.N; i++ {
		Get8Byte(buf)
	}
}
