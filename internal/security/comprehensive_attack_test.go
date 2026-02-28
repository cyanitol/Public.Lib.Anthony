package security

import (
	"math"
	"strings"
	"testing"
	"time"
)

// TestComprehensivePathTraversalVectors tests extensive path traversal attacks
func TestComprehensivePathTraversalVectors(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"
	cfg.EnforceSandbox = true
	cfg.BlockTraversal = true

	vectors := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Path traversal attacks
		{"basic traversal", "../../../etc/passwd", true},
		{"relative traversal", "foo/../../../etc/passwd", true},

		// Null byte injection
		{"null byte injection", "foo\x00bar.db", true},
		{"null in middle", "valid\x00../etc/passwd", true},

		// Control characters
		{"bell character", "foo\x07bar.db", true},
		{"escape character", "foo\x1bbar.db", true},

		// Absolute paths
		{"absolute linux", "/absolute/path.db", true},

		// Valid paths
		{"simple valid", "valid.db", false},
		{"subdir valid", "subdir/valid.db", false},
		{"deep valid", "a/b/c/d/e/f/valid.db", false},
		{"underscores", "my_database.db", false},
		{"dashes", "my-database.db", false},
		{"dots in name", "my.db.sqlite", false},

		// Edge cases
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

// TestComprehensiveIntegerOverflow tests comprehensive integer overflow scenarios
func TestComprehensiveIntegerOverflow(t *testing.T) {
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
		{"boundary - 1", math.MaxUint16 - 1, false},
		{"boundary + 1", math.MaxUint16 + 1, true},
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

// TestComprehensiveSafeAdd tests addition overflow comprehensively
func TestComprehensiveSafeAdd(t *testing.T) {
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
		{"large overflow", math.MaxUint32 / 2, math.MaxUint32 / 2 + 2, 0, true},
		{"both max", math.MaxUint32, math.MaxUint32, 0, true},
		{"boundary test", math.MaxUint32 - 100, 100, math.MaxUint32, false},
		{"just over boundary", math.MaxUint32 - 100, 101, 0, true},
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

// TestUnicodeSecurityVectors tests unicode-based attacks
func TestUnicodeSecurityVectors(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	unicodeVectors := []struct {
		name string
		path string
	}{
		{"chinese chars", "数据库.db"},
		{"emoji", "test🔥.db"},
		{"rtl override", "test\u202Edb.txt"}, // Right-to-left override
		{"zero width", "test\u200Bdb.db"},    // Zero-width space
		{"combining chars", "te\u0301st.db"}, // Combining acute accent
	}

	for _, v := range unicodeVectors {
		t.Run(v.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(v.path, cfg)
			// These should be handled gracefully
			_ = err
		})
	}
}

// TestConcurrentSecurityValidation tests concurrent access patterns
func TestConcurrentSecurityValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race condition test in short mode")
	}

	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	// Run multiple validations concurrently
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func(id int) {
			path := strings.Repeat("a/", id%10) + "test.db"
			_, _ = ValidateDatabasePath(path, cfg)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}
}

// TestDenialOfServiceResistance tests DoS attack resistance
func TestDenialOfServiceResistance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping DoS test in short mode")
	}

	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	dosVectors := []struct {
		name string
		path string
	}{
		{"extremely long path", strings.Repeat("a", 10000)},
		{"many slashes", strings.Repeat("/", 1000)},
		{"many dots", strings.Repeat(".", 1000)},
		{"deeply nested", strings.Repeat("a/", 1000)},
		{"repeated pattern", strings.Repeat("../", 1000)},
	}

	for _, v := range dosVectors {
		t.Run(v.name, func(t *testing.T) {
			// Set a reasonable timeout
			done := make(chan bool, 1)
			go func() {
				_, _ = ValidateDatabasePath(v.path, cfg)
				done <- true
			}()

			// Wait briefly - validation should be fast
			select {
			case <-done:
				// Good, validation completed
			case <-time.After(100 * time.Millisecond):
				t.Error("Validation took too long - potential DoS vulnerability")
			}
		})
	}
}

// TestPlatformSpecificVectors tests platform-specific attack vectors
func TestPlatformSpecificVectors(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	platformVectors := []struct {
		name string
		path string
	}{
		// Windows-specific
		{"windows device", "CON"},
		{"windows device with ext", "CON.db"},
		{"windows backslash", "foo\\bar.db"},

		// Unix-specific
		{"unix hidden file", ".hidden.db"},
		{"unix tilde", "~/test.db"},
	}

	for _, v := range platformVectors {
		t.Run(v.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(v.path, cfg)
			// Document behavior but don't enforce specific results
			_ = err
		})
	}
}

// TestSecurityConfigurationVariations tests different security configs
func TestSecurityConfigurationVariations(t *testing.T) {
	testCases := []struct {
		name     string
		setupCfg func() *SecurityConfig
		path     string
		wantErr  bool
	}{
		{
			name: "permissive config",
			setupCfg: func() *SecurityConfig {
				cfg := &SecurityConfig{
					BlockNullBytes:     false,
					BlockTraversal:     false,
					BlockSymlinks:      false,
					BlockAbsolutePaths: false,
					EnforceSandbox:     false,
				}
				return cfg
			},
			path:    "../test.db",
			wantErr: false,
		},
		{
			name: "strict config",
			setupCfg: func() *SecurityConfig {
				return DefaultSecurityConfig()
			},
			path:    "../test.db",
			wantErr: true,
		},
		{
			name: "nil config uses defaults",
			setupCfg: func() *SecurityConfig {
				return nil
			},
			path:    "valid.db",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.setupCfg()
			if cfg != nil && cfg.EnforceSandbox {
				cfg.DatabaseRoot = "/tmp/sandbox"
			}
			_, err := ValidateDatabasePath(tc.path, cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error = %v, wantErr %v", tc.path, err, tc.wantErr)
			}
		})
	}
}

// TestEdgeCaseBoundaryConditions tests edge cases and boundaries
func TestEdgeCaseBoundaryConditions(t *testing.T) {
	cfg := DefaultSecurityConfig()
	cfg.DatabaseRoot = "/tmp/sandbox"

	edgeCases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"single char", "a", false},
		{"single slash", "/", true}, // Absolute path
		{"double slash", "//", true},
		{"trailing slash", "test/", false},
		{"multiple trailing slashes", "test///", false},
		{"dot", ".", false},
		{"double dot", "..", true},
		{"dot dot slash", "../", true},
		{"slash dot", "/.", true},
		{"slash dot dot", "/..", true},
	}

	for _, ec := range edgeCases {
		t.Run(ec.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(ec.path, cfg)
			if (err != nil) != ec.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error = %v, wantErr %v", ec.path, err, ec.wantErr)
			}
		})
	}
}
