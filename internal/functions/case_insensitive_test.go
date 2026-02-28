package functions

import (
	"testing"
)

// TestCaseInsensitiveLookup tests that function names are case-insensitive
func TestCaseInsensitiveLookup(t *testing.T) {
	r := DefaultRegistry()

	tests := []struct {
		name     string
		funcName string
	}{
		{"lowercase", "upper"},
		{"uppercase", "UPPER"},
		{"mixed case", "UpPeR"},
		{"lowercase lower", "lower"},
		{"uppercase LOWER", "LOWER"},
		{"lowercase length", "length"},
		{"uppercase LENGTH", "LENGTH"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, ok := r.Lookup(tt.funcName)
			if !ok {
				t.Errorf("Lookup(%q) failed - function not found", tt.funcName)
				return
			}

			// Test that the function actually works
			var result Value
			var err error

			// Normalize the function name to lowercase for comparison
			normalizedName := tt.funcName
			if len(normalizedName) >= 5 && (normalizedName[:5] == "upper" || normalizedName[:5] == "UPPER" || normalizedName[:5] == "UpPeR") {
				result, err = fn.Call([]Value{NewTextValue("hello")})
				if err != nil {
					t.Errorf("Call(%q) failed: %v", tt.funcName, err)
					return
				}
				expected := "HELLO"
				if result.AsString() != expected {
					t.Errorf("Call(%q, 'hello') = %q, want %q", tt.funcName, result.AsString(), expected)
				}
			}
		})
	}
}

// TestCaseInsensitiveLookupWithArgs tests that LookupWithArgs is also case-insensitive
func TestCaseInsensitiveLookupWithArgs(t *testing.T) {
	r := DefaultRegistry()

	tests := []struct {
		name     string
		funcName string
		numArgs  int
	}{
		{"UPPER with 1 arg", "UPPER", 1},
		{"upper with 1 arg", "upper", 1},
		{"LENGTH with 1 arg", "LENGTH", 1},
		{"length with 1 arg", "length", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, ok := r.LookupWithArgs(tt.funcName, tt.numArgs)
			if !ok {
				t.Errorf("LookupWithArgs(%q, %d) failed - function not found", tt.funcName, tt.numArgs)
			}
			if fn == nil {
				t.Errorf("LookupWithArgs(%q, %d) returned nil function", tt.funcName, tt.numArgs)
			}
		})
	}
}

// TestUnregisterCaseInsensitive tests that Unregister is also case-insensitive
func TestUnregisterCaseInsensitive(t *testing.T) {
	r := NewRegistry()

	// Register a user function with lowercase name
	testFunc := NewScalarFunc("testfunc", 1, func(args []Value) (Value, error) {
		return NewIntValue(42), nil
	})
	r.RegisterUser(testFunc, 1)

	// Verify it can be found with lowercase
	_, ok := r.LookupWithArgs("testfunc", 1)
	if !ok {
		t.Error("Function should be registered")
	}

	// Unregister with uppercase name
	removed := r.Unregister("TESTFUNC", 1)
	if !removed {
		t.Error("Unregister() with uppercase should work")
	}

	// Verify it's gone
	_, ok = r.LookupWithArgs("testfunc", 1)
	if ok {
		t.Error("Function should be unregistered")
	}
}
