// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package constraint

import (
	"strings"
	"testing"
)

func TestNewCollationRegistry(t *testing.T) {
	cr := NewCollationRegistry()

	// Check that built-in collations are registered
	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		if _, ok := cr.Get(name); !ok {
			t.Errorf("Built-in collation %s not registered", name)
		}
	}
}

func TestRegisterCollation(t *testing.T) {
	cr := NewCollationRegistry()

	// Register a custom collation
	customFunc := func(a, b string) int {
		return strings.Compare(a, b)
	}

	err := cr.Register("CUSTOM", customFunc)
	if err != nil {
		t.Errorf("Failed to register custom collation: %v", err)
	}

	// Verify it was registered
	coll, ok := cr.Get("CUSTOM")
	if !ok {
		t.Error("Custom collation not found after registration")
	}
	if coll.Name != "CUSTOM" {
		t.Errorf("Expected name CUSTOM, got %s", coll.Name)
	}

	// Test error cases
	err = cr.Register("", customFunc)
	if err == nil {
		t.Error("Expected error for empty collation name")
	}

	err = cr.Register("TEST", nil)
	if err == nil {
		t.Error("Expected error for nil collation function")
	}
}

func TestGetCollation(t *testing.T) {
	cr := NewCollationRegistry()

	// Test getting built-in collations
	tests := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range tests {
		coll, ok := cr.Get(name)
		if !ok {
			t.Errorf("Failed to get collation %s", name)
		}
		if coll.Name != name {
			t.Errorf("Expected name %s, got %s", name, coll.Name)
		}
	}

	// Test getting non-existent collation
	_, ok := cr.Get("NONEXISTENT")
	if ok {
		t.Error("Expected false for non-existent collation")
	}
}

func TestUnregisterCollation(t *testing.T) {
	cr := NewCollationRegistry()

	// Register a custom collation
	customFunc := func(a, b string) int { return 0 }
	cr.Register("CUSTOM", customFunc)

	// Unregister it
	err := cr.Unregister("CUSTOM")
	if err != nil {
		t.Errorf("Failed to unregister custom collation: %v", err)
	}

	// Verify it's gone
	_, ok := cr.Get("CUSTOM")
	if ok {
		t.Error("Collation still exists after unregistration")
	}

	// Test that built-in collations cannot be unregistered
	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		err := cr.Unregister(name)
		if err == nil {
			t.Errorf("Expected error when unregistering built-in collation %s", name)
		}

		// Verify it still exists
		if _, ok := cr.Get(name); !ok {
			t.Errorf("Built-in collation %s was removed", name)
		}
	}
}

func TestListCollations(t *testing.T) {
	cr := NewCollationRegistry()

	names := cr.List()
	if len(names) < 3 {
		t.Errorf("Expected at least 3 collations, got %d", len(names))
	}

	// Register a custom collation and verify the list grows
	cr.Register("CUSTOM", func(a, b string) int { return 0 })
	names2 := cr.List()
	if len(names2) != len(names)+1 {
		t.Errorf("Expected %d collations after registration, got %d", len(names)+1, len(names2))
	}
}

func TestCompareFunction(t *testing.T) {
	tests := []struct {
		name      string
		a, b      string
		collation string
		expected  int
	}{
		// BINARY collation tests
		{"binary equal", "hello", "hello", "BINARY", 0},
		{"binary less", "hello", "world", "BINARY", -1},
		{"binary greater", "world", "hello", "BINARY", 1},
		{"binary case sensitive", "Hello", "hello", "BINARY", -1},

		// NOCASE collation tests
		{"nocase equal", "hello", "HELLO", "NOCASE", 0},
		{"nocase mixed case", "HeLLo", "hElLo", "NOCASE", 0},
		{"nocase less", "apple", "BANANA", "NOCASE", -1},
		{"nocase greater", "ZEBRA", "apple", "NOCASE", 1},

		// RTRIM collation tests
		{"rtrim equal with spaces", "hello   ", "hello", "RTRIM", 0},
		{"rtrim both spaces", "hello  ", "hello   ", "RTRIM", 0},
		{"rtrim different strings", "hello", "world", "RTRIM", -1},
		{"rtrim empty vs spaces", "", "   ", "RTRIM", 0},

		// Default collation (empty string should use BINARY)
		{"default collation", "hello", "hello", "", 0},

		// Non-existent collation (should fall back to BINARY)
		{"nonexistent collation", "hello", "hello", "NONEXISTENT", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Compare(tt.a, tt.b, tt.collation)

			// Normalize result to -1, 0, or 1
			if result < 0 {
				result = -1
			} else if result > 0 {
				result = 1
			}

			if result != tt.expected {
				t.Errorf("Compare(%q, %q, %q) = %d, want %d",
					tt.a, tt.b, tt.collation, result, tt.expected)
			}
		})
	}
}

func TestCompareBytesFunction(t *testing.T) {
	tests := []struct {
		name      string
		a, b      []byte
		collation string
		expected  int
	}{
		{"binary equal", []byte("hello"), []byte("hello"), "BINARY", 0},
		{"nocase equal", []byte("hello"), []byte("HELLO"), "NOCASE", 0},
		{"rtrim with spaces", []byte("hello  "), []byte("hello"), "RTRIM", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareBytes(tt.a, tt.b, tt.collation)

			// Normalize result
			if result < 0 {
				result = -1
			} else if result > 0 {
				result = 1
			}

			if result != tt.expected {
				t.Errorf("CompareBytes(%q, %q, %q) = %d, want %d",
					tt.a, tt.b, tt.collation, result, tt.expected)
			}
		})
	}
}

func TestGetCollationFunc(t *testing.T) {
	// Test getting function for built-in collations
	builtins := []string{"BINARY", "NOCASE", "RTRIM"}
	for _, name := range builtins {
		fn := GetCollationFunc(name)
		if fn == nil {
			t.Errorf("GetCollationFunc(%s) returned nil", name)
		}

		// Test that the function works
		result := fn("hello", "hello")
		if result != 0 {
			t.Errorf("Collation function for %s returned %d for equal strings", name, result)
		}
	}

	// Test non-existent collation
	fn := GetCollationFunc("NONEXISTENT")
	if fn != nil {
		t.Error("Expected nil for non-existent collation function")
	}
}

func TestDefaultCollation(t *testing.T) {
	if DefaultCollation() != "BINARY" {
		t.Errorf("Expected default collation to be BINARY, got %s", DefaultCollation())
	}
}

func TestGlobalRegistry(t *testing.T) {
	// Test that global functions work with the global registry
	customFunc := func(a, b string) int {
		return strings.Compare(strings.ToUpper(a), strings.ToUpper(b))
	}

	err := RegisterCollation("GLOBAL_TEST", customFunc)
	if err != nil {
		t.Errorf("Failed to register collation in global registry: %v", err)
	}

	coll, ok := GetCollation("GLOBAL_TEST")
	if !ok {
		t.Error("Failed to get collation from global registry")
	}
	if coll.Name != "GLOBAL_TEST" {
		t.Errorf("Expected name GLOBAL_TEST, got %s", coll.Name)
	}

	// Clean up
	UnregisterCollation("GLOBAL_TEST")
}

func TestCustomCollationFunctionality(t *testing.T) {
	// Test a custom reverse collation (reverses comparison)
	reverseFunc := func(a, b string) int {
		if a < b {
			return 1
		} else if a > b {
			return -1
		}
		return 0
	}

	err := RegisterCollation("REVERSE", reverseFunc)
	if err != nil {
		t.Fatalf("Failed to register REVERSE collation: %v", err)
	}
	defer UnregisterCollation("REVERSE")

	// Test the reverse collation
	// In REVERSE order, "apple" > "banana" (opposite of normal)
	// So Compare("apple", "banana") should return positive (a > b means > 0)
	result := Compare("apple", "banana", "REVERSE")
	if result <= 0 {
		t.Error("REVERSE collation should make 'apple' > 'banana' (expect positive)")
	}

	// In REVERSE order, "banana" < "apple"
	// So Compare("banana", "apple") should return negative (a < b means < 0)
	result = Compare("banana", "apple", "REVERSE")
	if result >= 0 {
		t.Error("REVERSE collation should make 'banana' < 'apple' (expect negative)")
	}
}

func TestCollationReplacement(t *testing.T) {
	cr := NewCollationRegistry()

	// Register a collation
	func1 := func(a, b string) int { return 1 }
	cr.Register("TEST", func1)

	// Replace it with another
	func2 := func(a, b string) int { return -1 }
	cr.Register("TEST", func2)

	// Verify the new function is used
	coll, _ := cr.Get("TEST")
	result := coll.Func("a", "b")
	if result != -1 {
		t.Error("Collation was not replaced properly")
	}
}

// Benchmark tests
func BenchmarkCompareBinary(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Compare("hello world", "hello world", "BINARY")
	}
}

func BenchmarkCompareNoCase(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Compare("Hello World", "hello world", "NOCASE")
	}
}

func BenchmarkCompareRTrim(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Compare("hello  ", "hello", "RTRIM")
	}
}

func BenchmarkGetCollation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GetCollation("BINARY")
	}
}

func BenchmarkRegisterCollation(b *testing.B) {
	cr := NewCollationRegistry()
	fn := func(a, b string) int { return 0 }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cr.Register("BENCH", fn)
	}
}
