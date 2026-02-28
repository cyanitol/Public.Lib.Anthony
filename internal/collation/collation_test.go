package collation

import (
	"strings"
	"testing"
)

func TestNewCollationRegistry(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()
	if cr == nil {
		t.Fatal("NewCollationRegistry() returned nil")
	}

	// Check built-in collations are registered
	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		name := name
		if _, ok := cr.Get(name); !ok {
			t.Errorf("Built-in collation %q not registered", name)
		}
	}
}

func TestGlobalRegistry(t *testing.T) {
	t.Parallel()
	gr := GlobalRegistry()
	if gr == nil {
		t.Fatal("GlobalRegistry() returned nil")
	}

	// Should have built-in collations
	if _, ok := gr.Get("BINARY"); !ok {
		t.Error("Global registry missing BINARY collation")
	}
}

func TestRegisterCollation(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	// Register custom collation
	customFunc := func(a, b string) int {
		return strings.Compare(strings.ToUpper(a), strings.ToUpper(b))
	}

	err := cr.Register("CUSTOM", customFunc)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Verify it was registered
	coll, ok := cr.Get("CUSTOM")
	if !ok {
		t.Fatal("Custom collation not found after registration")
	}
	if coll.Name != "CUSTOM" {
		t.Errorf("Collation name = %q, want %q", coll.Name, "CUSTOM")
	}

	// Test the custom function
	result := coll.Func("hello", "HELLO")
	if result != 0 {
		t.Errorf("Custom collation compare = %d, want 0", result)
	}
}

func TestRegisterErrors(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	tests := []struct {
		name    string
		colName string
		fn      CollationFunc
		wantErr bool
	}{
		{"empty name", "", func(a, b string) int { return 0 }, true},
		{"nil function", "TEST", nil, true},
		{"valid", "VALID", func(a, b string) int { return 0 }, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			err := cr.Register(tt.colName, tt.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnregisterCollation(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	// Register custom collation
	err := cr.Register("TEMP", func(a, b string) int { return 0 })
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	// Unregister it
	err = cr.Unregister("TEMP")
	if err != nil {
		t.Fatalf("Unregister() error = %v", err)
	}

	// Verify it's gone
	if _, ok := cr.Get("TEMP"); ok {
		t.Error("Collation still exists after unregister")
	}
}

func TestUnregisterBuiltinProtection(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		name := name
		t.Run(name, func(t *testing.T) {
				t.Parallel()
			err := cr.Unregister(name)
			if err == nil {
				t.Errorf("Unregister(%q) should error for built-in collation", name)
			}

			// Verify it still exists
			if _, ok := cr.Get(name); !ok {
				t.Errorf("Built-in collation %q was removed", name)
			}
		})
	}
}

func TestListCollations(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	// Should have at least the 3 built-in collations
	names := cr.List()
	if len(names) < 3 {
		t.Errorf("List() returned %d collations, want at least 3", len(names))
	}

	// Check for built-ins
	found := make(map[string]bool)
	for _, name := range names {
		name := name
		found[name] = true
	}

	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		name := name
		if !found[name] {
			t.Errorf("List() missing built-in collation %q", name)
		}
	}
}

func TestGetCollation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		colName string
		wantOk  bool
	}{
		{"BINARY", "BINARY", true},
		{"NOCASE", "NOCASE", true},
		{"RTRIM", "RTRIM", true},
		{"nonexistent", "NONEXISTENT", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			coll, ok := GetCollation(tt.colName)
			if ok != tt.wantOk {
				t.Errorf("GetCollation(%q) ok = %v, want %v", tt.colName, ok, tt.wantOk)
			}
			if ok && coll == nil {
				t.Error("GetCollation() returned nil collation with ok=true")
			}
		})
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		a, b      string
		collation string
		want      int
	}{
		// BINARY tests
		{"BINARY equal", "hello", "hello", "BINARY", 0},
		{"BINARY less", "apple", "banana", "BINARY", -1},
		{"BINARY greater", "zebra", "apple", "BINARY", 1},
		{"BINARY case sensitive", "Hello", "hello", "BINARY", -1},

		// NOCASE tests
		{"NOCASE equal same case", "hello", "hello", "NOCASE", 0},
		{"NOCASE equal diff case", "HELLO", "hello", "NOCASE", 0},
		{"NOCASE equal mixed", "HeLLo", "hElLo", "NOCASE", 0},
		{"NOCASE less", "apple", "BANANA", "NOCASE", -1},
		{"NOCASE greater", "ZEBRA", "apple", "NOCASE", 1},

		// RTRIM tests
		{"RTRIM no spaces", "hello", "hello", "RTRIM", 0},
		{"RTRIM trailing a", "hello   ", "hello", "RTRIM", 0},
		{"RTRIM trailing b", "hello", "hello   ", "RTRIM", 0},
		{"RTRIM both", "hello  ", "hello   ", "RTRIM", 0},
		{"RTRIM different", "hello", "world", "RTRIM", -1},
		{"RTRIM empty vs spaces", "", "   ", "RTRIM", 0},

		// Default (empty) should use BINARY
		{"default uses BINARY", "Hello", "hello", "", -1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := Compare(tt.a, tt.b, tt.collation)
			// Check sign of result
			if (result < 0) != (tt.want < 0) || (result > 0) != (tt.want > 0) || (result == 0) != (tt.want == 0) {
				t.Errorf("Compare(%q, %q, %q) = %d, want %d", tt.a, tt.b, tt.collation, result, tt.want)
			}
		})
	}
}

func TestCompareBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		a, b      []byte
		collation string
		want      int
	}{
		{"BINARY equal", []byte("hello"), []byte("hello"), "BINARY", 0},
		{"NOCASE equal", []byte("HELLO"), []byte("hello"), "NOCASE", 0},
		{"RTRIM trailing", []byte("hello  "), []byte("hello"), "RTRIM", 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
			result := CompareBytes(tt.a, tt.b, tt.collation)
			if (result < 0) != (tt.want < 0) || (result > 0) != (tt.want > 0) || (result == 0) != (tt.want == 0) {
				t.Errorf("CompareBytes(%q, %q, %q) = %d, want %d", tt.a, tt.b, tt.collation, result, tt.want)
			}
		})
	}
}

func TestGetCollationFunc(t *testing.T) {
	t.Parallel()
	fn := GetCollationFunc("BINARY")
	if fn == nil {
		t.Error("GetCollationFunc(BINARY) returned nil")
	}

	// Test the function
	result := fn("hello", "world")
	if result >= 0 {
		t.Errorf("BINARY collation: hello vs world = %d, want < 0", result)
	}

	// Nonexistent collation should return nil
	fn = GetCollationFunc("NONEXISTENT")
	if fn != nil {
		t.Error("GetCollationFunc(NONEXISTENT) should return nil")
	}
}

func TestDefaultCollation(t *testing.T) {
	t.Parallel()
	def := DefaultCollation()
	if def != "BINARY" {
		t.Errorf("DefaultCollation() = %q, want %q", def, "BINARY")
	}
}

func TestCompareFallback(t *testing.T) {
	t.Parallel()
	// Compare with nonexistent collation should fall back to BINARY
	result := Compare("Hello", "hello", "NONEXISTENT")
	if result >= 0 {
		t.Error("Compare with nonexistent collation should fall back to BINARY (case-sensitive)")
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	cr := NewCollationRegistry()

	// Test concurrent reads and writes
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			name := "COLL" + string(rune('A'+i%26))
			cr.Register(name, func(a, b string) int { return 0 })
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = cr.List()
			cr.Get("BINARY")
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}

// Benchmarks
func BenchmarkCompareBinary(b *testing.B) {
	s1, s2 := "hello world", "hello world"
	for i := 0; i < b.N; i++ {
		Compare(s1, s2, "BINARY")
	}
}

func BenchmarkCompareNoCase(b *testing.B) {
	s1, s2 := "Hello World", "hello world"
	for i := 0; i < b.N; i++ {
		Compare(s1, s2, "NOCASE")
	}
}

func BenchmarkCompareRTrim(b *testing.B) {
	s1, s2 := "hello world  ", "hello world"
	for i := 0; i < b.N; i++ {
		Compare(s1, s2, "RTRIM")
	}
}

func BenchmarkGetCollation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetCollation("BINARY")
	}
}
