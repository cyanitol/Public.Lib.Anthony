// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package security

import (
	"math"
	"strings"
	"testing"
)

func TestPathTraversalVectors(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	vectors := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Path traversal attacks
		{"basic traversal", "../../../etc/passwd", true},
		{"relative traversal", "foo/../../../etc/passwd", true},
		{"windows traversal", "foo/..\\..\\..\\windows", true},
		{"double dot encoded", "foo/..%2f..%2fetc/passwd", true},    // contains ".."
		{"url encoded dots", "foo/%2e%2e/%2e%2e/etc/passwd", false}, // no literal ".."
		{"mixed encoding", "foo/%2E%2E/%2e%2e/etc/passwd", false},   // no literal ".."

		// Null byte injection
		{"null byte injection", "foo\x00bar.db", true},
		{"null in middle", "valid\x00../etc/passwd", true},

		// Absolute paths
		{"absolute linux", "/absolute/path.db", true},

		// Valid paths
		{"simple valid", "valid.db", false},
		{"subdir valid", "subdir/valid.db", false},
		{"deep valid", "a/b/c/d/e/f/valid.db", false},
		{"underscores", "my_database.db", false},
		{"dashes", "my-database.db", false},

		// Edge cases
		{"dot only", ".", false},
		{"current dir", "./valid.db", false},
	}

	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(v.path, cfg)
			if (err != nil) != v.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error = %v, wantErr %v", v.path, err, v.wantErr)
			}
		})
	}
}

func TestPathTraversalWithAbsoluteAllowed(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.BlockAbsolutePaths = false
	cfg.EnforceSandbox = false
	cfg.DatabaseRoot = ""

	_, err := ValidateDatabasePath("/tmp/test.db", cfg)
	if err != nil {
		t.Errorf("Expected absolute path to be allowed, got error: %v", err)
	}
}

func TestPathLengthLimit(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.MaxPathLength = 100
	cfg.EnforceSandbox = false

	// Create a path longer than the limit
	longPath := strings.Repeat("a", 101)
	_, err := ValidateDatabasePath(longPath, cfg)
	if err == nil {
		t.Error("Expected error for path exceeding length limit")
	}

	// Path at exactly the limit should work
	exactPath := strings.Repeat("a", 100)
	_, err = ValidateDatabasePath(exactPath, cfg)
	if err != nil {
		t.Errorf("Expected path at exact limit to work, got: %v", err)
	}
}

func TestPathEscapesRoot(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	// These should all fail because they try to escape the root
	escapePaths := []string{
		"../outside.db",
		"subdir/../../outside.db",
		"a/b/c/../../../../../../../etc/passwd",
	}

	for _, path := range escapePaths {
		t.Run(path, func(t *testing.T) {
			_, err := ValidateDatabasePath(path, cfg)
			if err == nil {
				t.Errorf("Expected error for path escaping root: %s", path)
			}
		})
	}
}

func TestIntegerOverflowBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		val     uint32
		wantErr bool
	}{
		{"zero", 0, false},
		{"small", 100, false},
		{"max uint16", math.MaxUint16, false},
		{"max uint16 + 1", math.MaxUint16 + 1, true},
		{"max uint32", math.MaxUint32, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SafeCastUint32ToUint16(tt.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeCastUint32ToUint16(%d) error = %v, wantErr %v", tt.val, err, tt.wantErr)
			}
			if err == nil && result != uint16(tt.val) {
				t.Errorf("SafeCastUint32ToUint16(%d) = %d, want %d", tt.val, result, uint16(tt.val))
			}
		})
	}
}

func TestSafeAddOverflow(t *testing.T) {
	tests := []struct {
		name    string
		a, b    uint32
		want    uint32
		wantErr bool
	}{
		{"small numbers", 10, 20, 30, false},
		{"zero sum", 0, 0, 0, false},
		{"max safe", math.MaxUint32 - 1, 1, math.MaxUint32, false},
		{"overflow by 1", math.MaxUint32, 1, 0, true},
		{"large overflow", math.MaxUint32 / 2, math.MaxUint32/2 + 2, 0, true},
		{"both max", math.MaxUint32, math.MaxUint32, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SafeAddUint32(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeAddUint32(%d, %d) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
			}
			if err == nil && result != tt.want {
				t.Errorf("SafeAddUint32(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestSafeMultiplyOverflow(t *testing.T) {
	tests := []struct {
		name    string
		a, b    uint32
		want    uint32
		wantErr bool
	}{
		{"small numbers", 10, 20, 200, false},
		{"zero product", 0, 100, 0, false},
		{"one factor zero", 100, 0, 0, false},
		{"squares", 1000, 1000, 1000000, false},
		{"max safe", 65536, 65535, 4294901760, false},
		{"overflow", 65536, 65536, 0, true},
		{"large overflow", math.MaxUint32, 2, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SafeMultiplyUint32(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeMultiplyUint32(%d, %d) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
			}
			if err == nil && result != tt.want {
				t.Errorf("SafeMultiplyUint32(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.want)
			}
		})
	}
}

func TestBufferOverflowValidation(t *testing.T) {
	buffer := make([]byte, 100)

	tests := []struct {
		name    string
		offset  int
		length  int
		wantErr bool
	}{
		{"valid access", 0, 50, false},
		{"valid at end", 90, 10, false},
		{"exact buffer size", 0, 100, false},
		{"negative offset", -1, 10, true},
		{"negative length", 0, -1, true},
		{"offset beyond buffer", 101, 10, true},
		{"length beyond buffer", 90, 20, true},
		{"both beyond buffer", 90, 100, true},
		{"offset at boundary", 100, 0, false},
		{"offset at boundary + 1", 100, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBufferAccess(buffer, tt.offset, tt.length)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBufferAccess(buf[100], %d, %d) error = %v, wantErr %v",
					tt.offset, tt.length, err, tt.wantErr)
			}
		})
	}
}

func TestBufferTruncatedData(t *testing.T) {
	// Simulate truncated record data
	tests := []struct {
		name   string
		data   []byte
		offset int
		length int
	}{
		{"empty buffer", []byte{}, 0, 10},
		{"partial header", []byte{0x01, 0x02}, 0, 10},
		{"truncated at offset", []byte{0x01, 0x02, 0x03}, 2, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBufferAccess(tt.data, tt.offset, tt.length)
			if err == nil {
				t.Errorf("Expected error for truncated data access")
			}
		})
	}
}

func TestExpressionDepthLimit(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.MaxExpressionDepth = 10

	// Within limit
	err := ValidateExpressionDepth(cfg, 5)
	if err != nil {
		t.Errorf("Expected depth 5 to be valid, got: %v", err)
	}

	// At limit
	err = ValidateExpressionDepth(cfg, 10)
	if err != nil {
		t.Errorf("Expected depth 10 to be valid, got: %v", err)
	}

	// Exceeds limit
	err = ValidateExpressionDepth(cfg, 11)
	if err == nil {
		t.Error("Expected error for depth exceeding limit")
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		ident   string
		wantErr bool
	}{
		{"valid simple", "table_name", false},
		{"valid with numbers", "table123", false},
		{"valid underscore", "my_table", false},
		{"empty identifier", "", true},
		{"null byte", "table\x00name", true},
		{"control char", "table\x01name", true},
		{"newline allowed", "table\nname", false},
		{"tab allowed", "table\tname", false},
		{"bell character", "table\x07", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SanitizeIdentifier(tt.ident)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizeIdentifier(%q) error = %v, wantErr %v", tt.ident, err, tt.wantErr)
			}
			if err == nil && result != tt.ident {
				t.Errorf("SanitizeIdentifier(%q) = %q, want %q", tt.ident, result, tt.ident)
			}
		})
	}
}

func TestNilConfigDefaults(t *testing.T) {
	// ValidateDatabasePath should use defaults when config is nil
	_, err := ValidateDatabasePath("valid.db", nil)
	if err != nil {
		t.Errorf("Expected nil config to use defaults, got error: %v", err)
	}

	// ValidateExpressionDepth should use defaults when config is nil
	err = ValidateExpressionDepth(nil, 50)
	if err != nil {
		t.Errorf("Expected nil config to use defaults, got error: %v", err)
	}
}

func TestSecurityConfigDefaults(t *testing.T) {
	cfg := DefaultSecurityConfig()

	if !cfg.BlockAbsolutePaths {
		t.Error("Expected absolute paths to be blocked by default")
	}

	if !cfg.BlockTraversal {
		t.Error("Expected path traversal to be blocked by default")
	}

	if cfg.MaxPathLength <= 0 {
		t.Error("Expected positive max path length")
	}

	if cfg.MaxExpressionDepth <= 0 {
		t.Error("Expected positive max expression depth")
	}
}

// Benchmark tests for performance validation
func BenchmarkSecurityValidateDatabasePath(b *testing.B) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ValidateDatabasePath("test/database.db", cfg)
	}
}

func BenchmarkSecuritySafeAddUint32(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SafeAddUint32(1000, 2000)
	}
}

func BenchmarkSecurityValidateBufferAccess(b *testing.B) {
	buffer := make([]byte, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateBufferAccess(buffer, 10, 100)
	}
}
