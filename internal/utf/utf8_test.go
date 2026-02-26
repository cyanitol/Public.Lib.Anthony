package utf

import (
	"testing"
	"unicode/utf8"
)

func TestAppendRune(t *testing.T) {
	tests := []struct {
		name     string
		r        rune
		expected []byte
	}{
		{"ASCII", 'A', []byte{0x41}},
		{"2-byte", 'é', []byte{0xC3, 0xA9}},
		{"3-byte", '日', []byte{0xE6, 0x97, 0xA5}},
		{"4-byte", '𐍈', []byte{0xF0, 0x90, 0x8D, 0x88}},
		{"null", 0, []byte{0}},
		{"max 2-byte", '\u07FF', []byte{0xDF, 0xBF}},
		{"min 3-byte", '\u0800', []byte{0xE0, 0xA0, 0x80}},
		{"max 3-byte", '\uFFFF', []byte{0xEF, 0xBF, 0xBF}},
		{"min 4-byte", '\U00010000', []byte{0xF0, 0x90, 0x80, 0x80}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AppendRune(nil, tt.r)
			if len(result) != len(tt.expected) {
				t.Errorf("AppendRune(%U) length = %d, want %d", tt.r, len(result), len(tt.expected))
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("AppendRune(%U)[%d] = 0x%02X, want 0x%02X", tt.r, i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestEncodeRune(t *testing.T) {
	tests := []struct {
		name     string
		r        rune
		expected []byte
	}{
		{"ASCII", 'A', []byte{0x41}},
		{"2-byte", 'é', []byte{0xC3, 0xA9}},
		{"3-byte", '日', []byte{0xE6, 0x97, 0xA5}},
		{"4-byte", '𐍈', []byte{0xF0, 0x90, 0x8D, 0x88}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 4)
			n := EncodeRune(buf, tt.r)
			if n != len(tt.expected) {
				t.Errorf("EncodeRune(%U) returned %d bytes, want %d", tt.r, n, len(tt.expected))
			}
			for i := 0; i < n; i++ {
				if buf[i] != tt.expected[i] {
					t.Errorf("EncodeRune(%U)[%d] = 0x%02X, want 0x%02X", tt.r, i, buf[i], tt.expected[i])
				}
			}
		})
	}
}

func TestDecodeRune(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected rune
		size     int
	}{
		{"ASCII", []byte{0x41}, 'A', 1},
		{"2-byte", []byte{0xC3, 0xA9}, 'é', 2},
		{"3-byte", []byte{0xE6, 0x97, 0xA5}, '日', 3},
		{"4-byte", []byte{0xF0, 0x90, 0x8D, 0x88}, '𐍈', 4},
		{"null", []byte{0}, 0, 1},
		{"invalid surrogate", []byte{0xED, 0xA0, 0x80}, ReplacementChar, 3},
		{"overlong ASCII", []byte{0xC0, 0x80}, ReplacementChar, 2},
		{"invalid FFFE", []byte{0xEF, 0xBF, 0xBE}, ReplacementChar, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, size := DecodeRune(tt.data)
			if r != tt.expected {
				t.Errorf("DecodeRune() rune = %U, want %U", r, tt.expected)
			}
			if size != tt.size {
				t.Errorf("DecodeRune() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestDecodeRuneLimited(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		n        int
		expected rune
		size     int
	}{
		{"ASCII limited", []byte{0x41, 0x42}, 1, 'A', 1},
		{"2-byte limited", []byte{0xC3, 0xA9, 0x00}, 2, 'é', 2},
		{"truncated", []byte{0xC3}, 1, 0x03, 1},
		{"3-byte full", []byte{0xE6, 0x97, 0xA5}, 4, '日', 3},
		{"empty", []byte{}, 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, size := DecodeRuneLimited(tt.data, tt.n)
			if r != tt.expected {
				t.Errorf("DecodeRuneLimited() rune = %U, want %U", r, tt.expected)
			}
			if size != tt.size {
				t.Errorf("DecodeRuneLimited() size = %d, want %d", size, tt.size)
			}
		})
	}
}

func TestCharCount(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		nByte    int
		expected int
	}{
		{"ASCII", "hello", -1, 5},
		{"ASCII limited", "hello", 3, 3},
		{"mixed", "héllo", -1, 5},
		{"japanese", "日本語", -1, 3},
		{"emoji", "Hello 👋 World", -1, 13},
		{"null terminated", "hello\x00world", -1, 5},
		{"empty", "", -1, 0},
		{"bytes limited", "hello", 100, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CharCount(tt.s, tt.nByte)
			if result != tt.expected {
				t.Errorf("CharCount(%q, %d) = %d, want %d", tt.s, tt.nByte, result, tt.expected)
			}
		})
	}
}

func TestCharCountBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		nByte    int
		expected int
	}{
		{"ASCII", []byte("hello"), -1, 5},
		{"mixed", []byte("héllo"), -1, 5},
		{"null terminated", []byte("hello\x00world"), -1, 5},
		{"empty", []byte{}, -1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CharCountBytes(tt.data, tt.nByte)
			if result != tt.expected {
				t.Errorf("CharCountBytes() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestValidateUTF8(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"valid ASCII", []byte("hello"), true},
		{"valid UTF-8", []byte("日本語"), true},
		{"invalid", []byte{0xFF, 0xFE}, false},
		{"truncated", []byte{0xC3}, false},
		{"empty", []byte{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateUTF8(tt.data)
			if result != tt.expected {
				t.Errorf("ValidateUTF8() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestToValidUTF8(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		valid bool
	}{
		{"already valid", []byte("hello"), true},
		{"invalid sequences", []byte{0xFF, 0xFE}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToValidUTF8(tt.data)
			if !utf8.Valid(result) {
				t.Errorf("ToValidUTF8() produced invalid UTF-8")
			}
		})
	}
}

func TestUpperToLower(t *testing.T) {
	for i := 'A'; i <= 'Z'; i++ {
		expected := byte(i - 'A' + 'a')
		if UpperToLower[i] != expected {
			t.Errorf("UpperToLower[%c] = %c, want %c", i, UpperToLower[i], expected)
		}
	}

	// Non-uppercase should be unchanged
	for i := 'a'; i <= 'z'; i++ {
		if UpperToLower[i] != byte(i) {
			t.Errorf("UpperToLower[%c] = %c, want %c", i, UpperToLower[i], i)
		}
	}
}

func TestToUpper(t *testing.T) {
	tests := []struct {
		input    rune
		expected rune
	}{
		{'a', 'A'},
		{'z', 'Z'},
		{'A', 'A'},
		{'é', 'é'}, // Non-ASCII unchanged
		{'1', '1'},
	}

	for _, tt := range tests {
		result := ToUpper(tt.input)
		if result != tt.expected {
			t.Errorf("ToUpper(%c) = %c, want %c", tt.input, result, tt.expected)
		}
	}
}

func TestToLower(t *testing.T) {
	tests := []struct {
		input    rune
		expected rune
	}{
		{'A', 'a'},
		{'Z', 'z'},
		{'a', 'a'},
		{'É', 'É'}, // Non-ASCII unchanged
		{'1', '1'},
	}

	for _, tt := range tests {
		result := ToLower(tt.input)
		if result != tt.expected {
			t.Errorf("ToLower(%c) = %c, want %c", tt.input, result, tt.expected)
		}
	}
}

func TestIsSpace(t *testing.T) {
	spaces := []byte{' ', '\t', '\n', '\r', '\v', '\f'}
	for _, c := range spaces {
		if !IsSpace(c) {
			t.Errorf("IsSpace(%q) = false, want true", c)
		}
	}

	nonSpaces := []byte{'a', '0', '_', '.'}
	for _, c := range nonSpaces {
		if IsSpace(c) {
			t.Errorf("IsSpace(%q) = true, want false", c)
		}
	}
}

func TestIsDigit(t *testing.T) {
	for c := byte('0'); c <= '9'; c++ {
		if !IsDigit(c) {
			t.Errorf("IsDigit(%c) = false, want true", c)
		}
	}

	nonDigits := []byte{'a', 'A', ' ', '/', ':'}
	for _, c := range nonDigits {
		if IsDigit(c) {
			t.Errorf("IsDigit(%c) = true, want false", c)
		}
	}
}

func TestIsXDigit(t *testing.T) {
	hexChars := []byte{'0', '9', 'a', 'f', 'A', 'F'}
	for _, c := range hexChars {
		if !IsXDigit(c) {
			t.Errorf("IsXDigit(%c) = false, want true", c)
		}
	}

	nonHex := []byte{'g', 'G', 'z', ' '}
	for _, c := range nonHex {
		if IsXDigit(c) {
			t.Errorf("IsXDigit(%c) = true, want false", c)
		}
	}
}

func TestHexToInt(t *testing.T) {
	tests := []struct {
		input    byte
		expected byte
	}{
		{'0', 0},
		{'9', 9},
		{'a', 10},
		{'f', 15},
		{'A', 10},
		{'F', 15},
	}

	for _, tt := range tests {
		result := HexToInt(tt.input)
		if result != tt.expected {
			t.Errorf("HexToInt(%c) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestFoldCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"HELLO", "hello"},
		{"Hello", "hello"},
		{"hello", "hello"},
		{"HeLLo", "hello"},
		{"123", "123"},
		{"", ""},
	}

	for _, tt := range tests {
		result := FoldCase(tt.input)
		if result != tt.expected {
			t.Errorf("FoldCase(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestEqualFold(t *testing.T) {
	tests := []struct {
		s        string
		t        string
		expected bool
	}{
		{"hello", "HELLO", true},
		{"Hello", "hello", true},
		{"hello", "hello", true},
		{"hello", "world", false},
		{"hello", "hello!", false},
		{"", "", true},
	}

	for _, tt := range tests {
		result := EqualFold(tt.s, tt.t)
		if result != tt.expected {
			t.Errorf("EqualFold(%q, %q) = %v, want %v", tt.s, tt.t, result, tt.expected)
		}
	}
}

func TestRuneLen(t *testing.T) {
	tests := []struct {
		r        rune
		expected int
	}{
		{'A', 1},
		{'é', 2},
		{'日', 3},
		{'𐍈', 4},
		{0x7F, 1},
		{0x80, 2},
		{0x7FF, 2},
		{0x800, 3},
		{0xFFFF, 3},
		{0x10000, 4},
	}

	for _, tt := range tests {
		result := RuneLen(tt.r)
		if result != tt.expected {
			t.Errorf("RuneLen(%U) = %d, want %d", tt.r, result, tt.expected)
		}
	}
}

func TestFullRune(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"ASCII", []byte{'A'}, true},
		{"2-byte complete", []byte{0xC3, 0xA9}, true},
		{"2-byte incomplete", []byte{0xC3}, false},
		{"3-byte complete", []byte{0xE6, 0x97, 0xA5}, true},
		{"3-byte incomplete", []byte{0xE6, 0x97}, false},
		{"4-byte complete", []byte{0xF0, 0x90, 0x8D, 0x88}, true},
		{"4-byte incomplete", []byte{0xF0, 0x90, 0x8D}, false},
		{"empty", []byte{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FullRune(tt.data)
			if result != tt.expected {
				t.Errorf("FullRune() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Benchmarks
func BenchmarkAppendRune(b *testing.B) {
	buf := make([]byte, 0, 4)
	for i := 0; i < b.N; i++ {
		buf = AppendRune(buf[:0], '日')
	}
}

func BenchmarkEncodeRune(b *testing.B) {
	buf := make([]byte, 4)
	for i := 0; i < b.N; i++ {
		EncodeRune(buf, '日')
	}
}

func BenchmarkDecodeRune(b *testing.B) {
	data := []byte("日本語")
	for i := 0; i < b.N; i++ {
		DecodeRune(data)
	}
}

func BenchmarkCharCount(b *testing.B) {
	s := "Hello, 世界! This is a test string with mixed ASCII and Unicode characters."
	for i := 0; i < b.N; i++ {
		CharCount(s, -1)
	}
}
