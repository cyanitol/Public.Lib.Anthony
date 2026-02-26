package utf

import (
	"bytes"
	"testing"
)

func TestEncodeUTF16LE(t *testing.T) {
	tests := []struct {
		name     string
		r        rune
		expected []byte
	}{
		{"ASCII", 'A', []byte{0x41, 0x00}},
		{"BMP", '日', []byte{0xE5, 0x65}},
		{"surrogate pair", '𐍈', []byte{0x00, 0xD8, 0x48, 0xDF}},
		{"max BMP", '\uFFFF', []byte{0xFF, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			n := EncodeUTF16LE(buf, tt.r)
			if n != len(tt.expected) {
				t.Errorf("EncodeUTF16LE(%U) returned %d bytes, want %d", tt.r, n, len(tt.expected))
			}
			if !bytes.Equal(buf[:n], tt.expected) {
				t.Errorf("EncodeUTF16LE(%U) = %v, want %v", tt.r, buf[:n], tt.expected)
			}
		})
	}
}

func TestEncodeUTF16BE(t *testing.T) {
	tests := []struct {
		name     string
		r        rune
		expected []byte
	}{
		{"ASCII", 'A', []byte{0x00, 0x41}},
		{"BMP", '日', []byte{0x65, 0xE5}},
		{"surrogate pair", '𐍈', []byte{0xD8, 0x00, 0xDF, 0x48}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			n := EncodeUTF16BE(buf, tt.r)
			if n != len(tt.expected) {
				t.Errorf("EncodeUTF16BE(%U) returned %d bytes, want %d", tt.r, n, len(tt.expected))
			}
			if !bytes.Equal(buf[:n], tt.expected) {
				t.Errorf("EncodeUTF16BE(%U) = %v, want %v", tt.r, buf[:n], tt.expected)
			}
		})
	}
}

func TestDecodeUTF16LE(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected rune
		size     int
	}{
		{"ASCII", []byte{0x41, 0x00}, 'A', 2},
		{"BMP", []byte{0xE5, 0x65}, '日', 2},
		{"surrogate pair", []byte{0x00, 0xD8, 0x48, 0xDF}, '𐍈', 4},
		{"incomplete", []byte{0x41}, ReplacementChar, 0},
		{"invalid surrogate", []byte{0x00, 0xD8}, ReplacementChar, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, size := DecodeUTF16LE(tt.data)
			if r != tt.expected {
				t.Errorf("DecodeUTF16LE() rune = %U, want %U", r, tt.expected)
			}
			if size != tt.size {
				t.Errorf("DecodeUTF16LE() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestDecodeUTF16BE(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected rune
		size     int
	}{
		{"ASCII", []byte{0x00, 0x41}, 'A', 2},
		{"BMP", []byte{0x65, 0xE5}, '日', 2},
		{"surrogate pair", []byte{0xD8, 0x00, 0xDF, 0x48}, '𐍈', 4},
		{"incomplete", []byte{0x41}, ReplacementChar, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, size := DecodeUTF16BE(tt.data)
			if r != tt.expected {
				t.Errorf("DecodeUTF16BE() rune = %U, want %U", r, tt.expected)
			}
			if size != tt.size {
				t.Errorf("DecodeUTF16BE() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestUTF16ToUTF8(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		enc      Encoding
		expected string
	}{
		{"ASCII LE", []byte{0x41, 0x00, 0x42, 0x00}, UTF16LE, "AB"},
		{"ASCII BE", []byte{0x00, 0x41, 0x00, 0x42}, UTF16BE, "AB"},
		{"BMP LE", []byte{0xE5, 0x65, 0x2C, 0x67}, UTF16LE, "日本"},
		{"BMP BE", []byte{0x65, 0xE5, 0x67, 0x2C}, UTF16BE, "日本"},
		{"empty", []byte{}, UTF16LE, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UTF16ToUTF8(tt.data, tt.enc)
			if string(result) != tt.expected {
				t.Errorf("UTF16ToUTF8() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func TestUTF8ToUTF16(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		enc      Encoding
		expected []byte
	}{
		{"ASCII LE", "AB", UTF16LE, []byte{0x41, 0x00, 0x42, 0x00}},
		{"ASCII BE", "AB", UTF16BE, []byte{0x00, 0x41, 0x00, 0x42}},
		{"BMP LE", "日本", UTF16LE, []byte{0xE5, 0x65, 0x2C, 0x67}},
		{"BMP BE", "日本", UTF16BE, []byte{0x65, 0xE5, 0x67, 0x2C}},
		{"empty", "", UTF16LE, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UTF8ToUTF16([]byte(tt.data), tt.enc)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("UTF8ToUTF16() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDetectBOM(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantEnc Encoding
		wantBOM bool
	}{
		{"LE BOM", []byte{0xFF, 0xFE, 0x41, 0x00}, UTF16LE, true},
		{"BE BOM", []byte{0xFE, 0xFF, 0x00, 0x41}, UTF16BE, true},
		{"no BOM", []byte{0x41, 0x00}, UTF8, false},
		{"too short", []byte{0xFF}, UTF8, false},
		{"empty", []byte{}, UTF8, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, hasBOM := DetectBOM(tt.data)
			if enc != tt.wantEnc {
				t.Errorf("DetectBOM() enc = %v, want %v", enc, tt.wantEnc)
			}
			if hasBOM != tt.wantBOM {
				t.Errorf("DetectBOM() hasBOM = %v, want %v", hasBOM, tt.wantBOM)
			}
		})
	}
}

func TestStripBOM(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectedEnc Encoding
		expectedLen int
	}{
		{"LE BOM", []byte{0xFF, 0xFE, 0x41, 0x00}, UTF16LE, 2},
		{"BE BOM", []byte{0xFE, 0xFF, 0x00, 0x41}, UTF16BE, 2},
		{"no BOM", []byte{0x41, 0x00}, UTF8, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, enc := StripBOM(tt.data)
			if enc != tt.expectedEnc {
				t.Errorf("StripBOM() enc = %v, want %v", enc, tt.expectedEnc)
			}
			if len(result) != tt.expectedLen {
				t.Errorf("StripBOM() len = %d, want %d", len(result), tt.expectedLen)
			}
		})
	}
}

func TestSwapEndian(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected []byte
	}{
		{"simple", []byte{0x01, 0x02, 0x03, 0x04}, []byte{0x02, 0x01, 0x04, 0x03}},
		{"ASCII", []byte{0x41, 0x00, 0x42, 0x00}, []byte{0x00, 0x41, 0x00, 0x42}},
		{"odd length", []byte{0x01, 0x02, 0x03}, []byte{0x02, 0x01, 0x03}},
		{"empty", []byte{}, []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, len(tt.data))
			copy(data, tt.data)
			SwapEndian(data)
			if !bytes.Equal(data, tt.expected) {
				t.Errorf("SwapEndian() = %v, want %v", data, tt.expected)
			}
		})
	}
}

func TestUTF16CharCount(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		enc      Encoding
		nChar    int
		expected int
	}{
		{"ASCII LE", []byte{0x41, 0x00, 0x42, 0x00, 0x43, 0x00}, UTF16LE, 10, 3},
		{"with surrogate LE", []byte{0x00, 0xD8, 0x48, 0xDF, 0x41, 0x00}, UTF16LE, 10, 2},
		{"limited", []byte{0x41, 0x00, 0x42, 0x00, 0x43, 0x00}, UTF16LE, 2, 2},
		{"empty", []byte{}, UTF16LE, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UTF16CharCount(tt.data, tt.enc, tt.nChar)
			if result != tt.expected {
				t.Errorf("UTF16CharCount() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestUTF16ByteLen(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		enc      Encoding
		nChar    int
		expected int
	}{
		{"ASCII LE", []byte{0x41, 0x00, 0x42, 0x00, 0x43, 0x00}, UTF16LE, 2, 4},
		{"with surrogate LE", []byte{0x00, 0xD8, 0x48, 0xDF, 0x41, 0x00}, UTF16LE, 1, 4},
		{"all chars", []byte{0x41, 0x00, 0x42, 0x00, 0x43, 0x00}, UTF16LE, 10, 6},
		{"zero chars", []byte{0x41, 0x00, 0x42, 0x00}, UTF16LE, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UTF16ByteLen(tt.data, tt.enc, tt.nChar)
			if result != tt.expected {
				t.Errorf("UTF16ByteLen() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestRoundTripUTF8UTF16(t *testing.T) {
	tests := []string{
		"Hello",
		"Hello, World!",
		"日本語",
		"Hello 世界",
		"𐍈𐍉𐍊",
		"",
		"A",
	}

	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			// UTF-8 -> UTF-16LE -> UTF-8
			utf16le := UTF8ToUTF16([]byte(tt), UTF16LE)
			utf8 := UTF16ToUTF8(utf16le, UTF16LE)
			if string(utf8) != tt {
				t.Errorf("Round trip UTF16LE failed: got %q, want %q", string(utf8), tt)
			}

			// UTF-8 -> UTF-16BE -> UTF-8
			utf16be := UTF8ToUTF16([]byte(tt), UTF16BE)
			utf8 = UTF16ToUTF8(utf16be, UTF16BE)
			if string(utf8) != tt {
				t.Errorf("Round trip UTF16BE failed: got %q, want %q", string(utf8), tt)
			}
		})
	}
}

// Benchmarks
func BenchmarkEncodeUTF16LE(b *testing.B) {
	buf := make([]byte, 4)
	for i := 0; i < b.N; i++ {
		EncodeUTF16LE(buf, '日')
	}
}

func BenchmarkDecodeUTF16LE(b *testing.B) {
	data := []byte{0xE5, 0x65}
	for i := 0; i < b.N; i++ {
		DecodeUTF16LE(data)
	}
}

func BenchmarkUTF16ToUTF8(b *testing.B) {
	data := []byte{0x41, 0x00, 0x42, 0x00, 0x43, 0x00}
	for i := 0; i < b.N; i++ {
		UTF16ToUTF8(data, UTF16LE)
	}
}

func BenchmarkUTF8ToUTF16(b *testing.B) {
	data := []byte("ABC")
	for i := 0; i < b.N; i++ {
		UTF8ToUTF16(data, UTF16LE)
	}
}
