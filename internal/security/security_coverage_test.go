// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package security

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// TestSafeCastIntToUint32_OverflowHighBit covers the int64(v) > math.MaxUint32 branch
// which is only reachable on 64-bit systems where int is wider than uint32.
func TestSafeCastIntToUint32_OverflowHighBit(t *testing.T) {
	tests := []struct {
		name    string
		v       int
		wantErr bool
	}{
		{"negative", -1, true},
		{"zero", 0, false},
		{"max uint32", math.MaxUint32, false},
		{"max uint32 + 1", math.MaxUint32 + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SafeCastIntToUint32(tt.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeCastIntToUint32(%d) error=%v, wantErr=%v", tt.v, err, tt.wantErr)
			}
		})
	}
}

// TestSecurityConfigClone covers the Clone method, including nil receiver,
// non-nil with empty AllowedSubdirs, and non-nil with populated AllowedSubdirs.
func TestSecurityConfigClone(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var c *SecurityConfig
		clone := c.Clone()
		if clone != nil {
			t.Errorf("Clone() on nil should return nil, got %v", clone)
		}
	})

	t.Run("no allowed subdirs", func(t *testing.T) {
		c := DefaultSecurityConfig()
		clone := c.Clone()
		if clone == c {
			t.Error("Clone() should return a new pointer")
		}
		if clone.MaxPathLength != c.MaxPathLength {
			t.Errorf("MaxPathLength mismatch: %d vs %d", clone.MaxPathLength, c.MaxPathLength)
		}
		if clone.AllowedSubdirs != nil {
			t.Errorf("expected nil AllowedSubdirs, got %v", clone.AllowedSubdirs)
		}
	})

	t.Run("with allowed subdirs", func(t *testing.T) {
		c := DefaultSecurityConfig()
		c.AllowedSubdirs = []string{"data", "logs"}
		clone := c.Clone()
		if len(clone.AllowedSubdirs) != 2 {
			t.Errorf("expected 2 AllowedSubdirs, got %d", len(clone.AllowedSubdirs))
		}
		// Mutating clone should not affect original
		clone.AllowedSubdirs[0] = "mutated"
		if c.AllowedSubdirs[0] != "data" {
			t.Error("Clone() did not deep-copy AllowedSubdirs")
		}
	})
}

// TestCheckAllowlist_NoDatabaseRoot covers the os.Getwd() path inside checkAllowlist
// when config.DatabaseRoot is empty but AllowedSubdirs is set.
func TestCheckAllowlist_NoDatabaseRoot(t *testing.T) {
	// We need a path that is actually under cwd/allowed to pass.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	allowedDir := filepath.Join(cwd, "allowed_subdir_test")
	if err := os.MkdirAll(allowedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	defer os.RemoveAll(allowedDir)

	config := &SecurityConfig{
		BlockNullBytes:     true,
		BlockTraversal:     true,
		BlockSymlinks:      false,
		BlockAbsolutePaths: false,
		EnforceSandbox:     false,
		AllowedSubdirs:     []string{"allowed_subdir_test"},
		MaxPathLength:      4096,
	}

	t.Run("path in allowed subdir", func(t *testing.T) {
		// Use an absolute path that resolves into cwd/allowed_subdir_test
		testPath := filepath.Join(allowedDir, "test.db")
		err := checkAllowlist(testPath, config)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
	})

	t.Run("path not in allowed subdir", func(t *testing.T) {
		testPath := filepath.Join(cwd, "other", "test.db")
		err := checkAllowlist(testPath, config)
		if err != ErrNotInAllowlist {
			t.Errorf("expected ErrNotInAllowlist, got: %v", err)
		}
	})
}
