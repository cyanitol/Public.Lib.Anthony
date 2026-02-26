package utf

import "testing"

func TestCompareBinary(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		expected int
	}{
		{"equal", "hello", "hello", 0},
		{"less", "hello", "world", -1},
		{"greater", "world", "hello", 1},
		{"empty", "", "", 0},
		{"empty vs non-empty", "", "a", -1},
		{"case sensitive", "Hello", "hello", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareBinary(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareBinary(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareNoCase(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		expected int
	}{
		{"equal same case", "hello", "hello", 0},
		{"equal diff case", "HELLO", "hello", 0},
		{"equal mixed case", "HeLLo", "hElLo", 0},
		{"less", "apple", "BANANA", -1},
		{"greater", "ZEBRA", "apple", 1},
		{"empty", "", "", 0},
		{"prefix", "hello", "helloworld", -1},
		{"non-ASCII unchanged", "café", "CAFÉ", 1}, // é != É in binary
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareNoCase(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareNoCase(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCompareRTrim(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		expected int
	}{
		{"equal no spaces", "hello", "hello", 0},
		{"equal with trailing spaces", "hello   ", "hello", 0},
		{"both have spaces", "hello  ", "hello   ", 0},
		{"different strings", "hello", "world", -1},
		{"trailing space matters not", "hello", "hello ", 0},
		{"empty", "", "   ", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareRTrim(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareRTrim(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestCollationCompare(t *testing.T) {
	tests := []struct {
		name      string
		collation Collation
		a, b      string
		expected  int
	}{
		{"BINARY equal", BuiltinCollations["BINARY"], "hello", "hello", 0},
		{"BINARY case different", BuiltinCollations["BINARY"], "Hello", "hello", -1},
		{"NOCASE equal", BuiltinCollations["NOCASE"], "Hello", "hello", 0},
		{"NOCASE different", BuiltinCollations["NOCASE"], "apple", "BANANA", -1},
		{"RTRIM with spaces", BuiltinCollations["RTRIM"], "hello  ", "hello", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.collation.Compare(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("%s.Compare(%q, %q) = %d, want %d",
					tt.collation.Name, tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestStrICmp(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		expected int
	}{
		{"equal", "hello", "hello", 0},
		{"case insensitive", "HELLO", "hello", 0},
		{"less", "apple", "BANANA", -1},
		{"greater", "ZEBRA", "apple", 1},
		{"empty both", "", "", 0},
		{"empty a", "", "hello", -1},
		{"empty b", "hello", "", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StrICmp(tt.a, tt.b)
			// We only care about the sign
			if (result < 0 && tt.expected >= 0) ||
				(result > 0 && tt.expected <= 0) ||
				(result == 0 && tt.expected != 0) {
				t.Errorf("StrICmp(%q, %q) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestStrNICmp(t *testing.T) {
	tests := []struct {
		name     string
		a, b     string
		n        int
		expected int
	}{
		{"equal within n", "hello", "hello", 5, 0},
		{"case insensitive", "HELLO", "hello", 5, 0},
		{"different after n", "hello", "helloworld", 5, 0},
		{"less within n", "apple", "BANANA", 1, -1},
		{"n exceeds length", "hi", "HI", 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StrNICmp(tt.a, tt.b, tt.n)
			// We only care about the sign
			if (result < 0 && tt.expected >= 0) ||
				(result > 0 && tt.expected <= 0) ||
				(result == 0 && tt.expected != 0) {
				t.Errorf("StrNICmp(%q, %q, %d) = %d, want %d", tt.a, tt.b, tt.n, result, tt.expected)
			}
		})
	}
}

func TestStrIHash(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		caseDiff string
	}{
		{"lowercase", "hello", "HELLO"},
		{"mixed", "HeLLo", "hElLo"},
		{"with numbers", "test123", "TEST123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h1 := StrIHash(tt.s)
			h2 := StrIHash(tt.caseDiff)
			if h1 != h2 {
				t.Errorf("StrIHash(%q) = %d, StrIHash(%q) = %d, should be equal",
					tt.s, h1, tt.caseDiff, h2)
			}
		})
	}
}

func TestLike(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{"exact match", "hello", "hello", true},
		{"no match", "hello", "world", false},
		{"percent wildcard", "h%", "hello", true},
		{"percent middle", "h%o", "hello", true},
		{"percent end", "hello%", "helloworld", true},
		{"underscore single", "h_llo", "hello", true},
		{"underscore no match", "h_llo", "hllo", false},
		{"multiple percent", "%ll%", "hello", true},
		{"case insensitive", "HELLO", "hello", true},
		{"empty pattern", "", "", true},
		{"empty string", "hello", "", false},
		{"percent only", "%", "anything", true},
		{"underscore count", "___", "abc", true},
		{"underscore count fail", "___", "ab", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Like(tt.pattern, tt.str, 0)
			if result != tt.expected {
				t.Errorf("Like(%q, %q) = %v, want %v", tt.pattern, tt.str, result, tt.expected)
			}
		})
	}
}

func TestLikeWithEscape(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		escape   rune
		expected bool
	}{
		{"escaped percent", "100\\%", "100%", '\\', true},
		{"escaped underscore", "a\\_b", "a_b", '\\', true},
		{"not escaped", "a\\%b", "axb", '\\', false},
		{"escape at end invalid", "hello\\", "hello", '\\', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Like(tt.pattern, tt.str, tt.escape)
			if result != tt.expected {
				t.Errorf("Like(%q, %q, %c) = %v, want %v",
					tt.pattern, tt.str, tt.escape, result, tt.expected)
			}
		})
	}
}

func TestLikeCase(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{"exact match", "hello", "hello", true},
		{"case sensitive fail", "HELLO", "hello", false},
		{"case sensitive match", "Hello", "Hello", true},
		{"wildcard works", "h%", "hello", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := LikeCase(tt.pattern, tt.str, 0)
			if result != tt.expected {
				t.Errorf("LikeCase(%q, %q) = %v, want %v", tt.pattern, tt.str, result, tt.expected)
			}
		})
	}
}

func TestGlob(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		str      string
		expected bool
	}{
		{"exact match", "hello", "hello", true},
		{"no match", "hello", "world", false},
		{"star wildcard", "h*", "hello", true},
		{"star middle", "h*o", "hello", true},
		{"star end", "hello*", "helloworld", true},
		{"question single", "h?llo", "hello", true},
		{"question no match", "h?llo", "hllo", false},
		{"multiple star", "*ll*", "hello", true},
		{"case sensitive", "HELLO", "hello", false},
		{"star only", "*", "anything", true},
		{"char class", "h[ae]llo", "hello", true},
		{"char class no match", "h[ae]llo", "hillo", false},
		{"char range", "[a-z]", "m", true},
		{"char range no match", "[a-z]", "M", false},
		{"inverted class", "h[^ae]llo", "hillo", true},
		{"inverted class no match", "h[^ae]llo", "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Glob(tt.pattern, tt.str)
			if result != tt.expected {
				t.Errorf("Glob(%q, %q) = %v, want %v", tt.pattern, tt.str, result, tt.expected)
			}
		})
	}
}

func TestBuiltinCollations(t *testing.T) {
	expected := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range expected {
		if _, ok := BuiltinCollations[name]; !ok {
			t.Errorf("Missing builtin collation: %s", name)
		}
	}
}

// Benchmarks
func BenchmarkCompareBinary(b *testing.B) {
	s1, s2 := "hello world", "hello world"
	for i := 0; i < b.N; i++ {
		CompareBinary(s1, s2)
	}
}

func BenchmarkCompareNoCase(b *testing.B) {
	s1, s2 := "Hello World", "hello world"
	for i := 0; i < b.N; i++ {
		CompareNoCase(s1, s2)
	}
}

func BenchmarkLike(b *testing.B) {
	pattern, str := "h%world", "hello world"
	for i := 0; i < b.N; i++ {
		Like(pattern, str, 0)
	}
}

func BenchmarkGlob(b *testing.B) {
	pattern, str := "h*world", "hello world"
	for i := 0; i < b.N; i++ {
		Glob(pattern, str)
	}
}

func BenchmarkStrICmp(b *testing.B) {
	s1, s2 := "Hello World", "hello world"
	for i := 0; i < b.N; i++ {
		StrICmp(s1, s2)
	}
}
