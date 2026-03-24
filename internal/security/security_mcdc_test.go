// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package security

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// SafeCastInt64ToInt32
//
// Compound condition:  v > math.MaxInt32 || v < math.MinInt32
//
// Sub-conditions:
//   A: v > math.MaxInt32
//   B: v < math.MinInt32
//
// MC/DC truth table (|| short-circuits, so we need A independently true,
// B independently true, and both false):
//   A=T B=?  → error  (A alone decides)
//   A=F B=T  → error  (B alone decides)
//   A=F B=F  → ok     (neither condition triggers)
// ---------------------------------------------------------------------------

func TestMCDC_SafeCastInt64ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		wantErr bool
	}{
		// A=T (v > MaxInt32), B=F → overflow via upper bound
		{name: "MCDC_A_true_B_false_upper_overflow", input: math.MaxInt32 + 1, wantErr: true},
		{name: "MCDC_A_true_B_false_max_int64", input: math.MaxInt64, wantErr: true},

		// A=F (v == MaxInt32), B=F → exact boundary, ok
		{name: "MCDC_A_false_B_false_at_max_int32", input: math.MaxInt32, wantErr: false},

		// A=F, B=T (v < MinInt32) → overflow via lower bound
		{name: "MCDC_A_false_B_true_lower_overflow", input: math.MinInt32 - 1, wantErr: true},
		{name: "MCDC_A_false_B_true_min_int64", input: math.MinInt64, wantErr: true},

		// A=F, B=F → exact lower boundary, ok
		{name: "MCDC_A_false_B_false_at_min_int32", input: math.MinInt32, wantErr: false},

		// A=F, B=F → well within range, ok
		{name: "MCDC_A_false_B_false_zero", input: 0, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SafeCastInt64ToInt32(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeCastInt64ToInt32(%d) error=%v wantErr=%v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SafeCastIntToUint16
//
// Compound condition:  v < 0 || v > math.MaxUint16
//
// Sub-conditions:
//   A: v < 0
//   B: v > math.MaxUint16
//
// MC/DC:
//   A=T B=?  → error
//   A=F B=T  → error
//   A=F B=F  → ok
// ---------------------------------------------------------------------------

func TestMCDC_SafeCastIntToUint16(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		wantErr bool
	}{
		// A=T, B=F → negative value
		{name: "MCDC_A_true_B_false_minus_one", input: -1, wantErr: true},
		{name: "MCDC_A_true_B_false_large_negative", input: -65536, wantErr: true},

		// A=F, B=T → value above MaxUint16
		{name: "MCDC_A_false_B_true_max_plus_one", input: math.MaxUint16 + 1, wantErr: true},
		{name: "MCDC_A_false_B_true_large_positive", input: 1_000_000, wantErr: true},

		// A=F, B=F → valid range
		{name: "MCDC_A_false_B_false_zero", input: 0, wantErr: false},
		{name: "MCDC_A_false_B_false_at_max", input: math.MaxUint16, wantErr: false},
		{name: "MCDC_A_false_B_false_mid_range", input: 1000, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SafeCastIntToUint16(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeCastIntToUint16(%d) error=%v wantErr=%v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SafeMultiplyUint32
//
// Compound condition:  a == 0 || b == 0
//
// Sub-conditions:
//   A: a == 0
//   B: b == 0
//
// MC/DC:
//   A=T B=?  → returns (0, nil) early
//   A=F B=T  → returns (0, nil) early
//   A=F B=F  → proceeds to overflow check
// ---------------------------------------------------------------------------

func TestMCDC_SafeMultiplyUint32_ZeroGuard(t *testing.T) {
	tests := []struct {
		name    string
		a, b    uint32
		want    uint32
		wantErr bool
	}{
		// A=T, B=F → a is zero, returns 0 immediately
		{name: "MCDC_A_true_B_false_a_zero", a: 0, b: 1000, want: 0, wantErr: false},
		// A=T, B=T → both zero
		{name: "MCDC_A_true_B_true_both_zero", a: 0, b: 0, want: 0, wantErr: false},
		// A=F, B=T → b is zero, returns 0 immediately
		{name: "MCDC_A_false_B_true_b_zero", a: 1000, b: 0, want: 0, wantErr: false},
		// A=F, B=F → neither is zero, normal multiplication (no overflow)
		{name: "MCDC_A_false_B_false_normal", a: 100, b: 200, want: 20000, wantErr: false},
		// A=F, B=F → neither is zero, causes overflow
		{name: "MCDC_A_false_B_false_overflow", a: math.MaxUint32/2 + 1, b: 3, want: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeMultiplyUint32(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("SafeMultiplyUint32(%d,%d) error=%v wantErr=%v", tt.a, tt.b, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("SafeMultiplyUint32(%d,%d) = %d want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidateBufferAccess — first compound condition
//
// Condition 1:  offset < 0 || length < 0
//
// Sub-conditions:
//   A: offset < 0
//   B: length < 0
//
// MC/DC:
//   A=T B=?  → ErrBufferOverflow
//   A=F B=T  → ErrBufferOverflow
//   A=F B=F  → proceed to second check
// ---------------------------------------------------------------------------

func TestMCDC_ValidateBufferAccess_NegativeGuard(t *testing.T) {
	buf := make([]byte, 100)

	tests := []struct {
		name    string
		offset  int
		length  int
		wantErr bool
	}{
		// A=T, B=F → negative offset
		{name: "MCDC_A_true_B_false_neg_offset", offset: -1, length: 10, wantErr: true},
		// A=F, B=T → negative length
		{name: "MCDC_A_false_B_true_neg_length", offset: 0, length: -1, wantErr: true},
		// A=T, B=T → both negative
		{name: "MCDC_A_true_B_true_both_neg", offset: -5, length: -5, wantErr: true},
		// A=F, B=F → both non-negative, within bounds → ok
		{name: "MCDC_A_false_B_false_valid", offset: 0, length: 10, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBufferAccess(buf, tt.offset, tt.length)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBufferAccess offset=%d length=%d error=%v wantErr=%v",
					tt.offset, tt.length, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ValidateBufferAccess — second compound condition
//
// Condition 2:  offset > len(buf) || length > len(buf)-offset
//
// Sub-conditions:
//   A: offset > len(buf)
//   B: length > len(buf)-offset
//
// MC/DC:
//   A=T B=?  → ErrBufferOverflow
//   A=F B=T  → ErrBufferOverflow
//   A=F B=F  → nil
// ---------------------------------------------------------------------------

func TestMCDC_ValidateBufferAccess_BoundsGuard(t *testing.T) {
	buf := make([]byte, 10)

	tests := []struct {
		name    string
		offset  int
		length  int
		wantErr bool
	}{
		// A=T, B=? → offset beyond buffer length
		{name: "MCDC_A_true_offset_past_end", offset: 11, length: 0, wantErr: true},
		{name: "MCDC_A_true_offset_equals_len_plus_one", offset: 11, length: 1, wantErr: true},

		// A=F, B=T → offset within bounds but length overruns tail
		{name: "MCDC_A_false_B_true_length_overrun", offset: 5, length: 6, wantErr: true},
		{name: "MCDC_A_false_B_true_length_one_past", offset: 0, length: 11, wantErr: true},

		// A=F, B=F → valid access
		{name: "MCDC_A_false_B_false_exact_fit", offset: 0, length: 10, wantErr: false},
		{name: "MCDC_A_false_B_false_mid_access", offset: 3, length: 5, wantErr: false},
		{name: "MCDC_A_false_B_false_empty_read", offset: 0, length: 0, wantErr: false},
		// offset == len(buf) with length==0 is allowed (points at end)
		{name: "MCDC_A_false_B_false_offset_at_end_zero_length", offset: 10, length: 0, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBufferAccess(buf, tt.offset, tt.length)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBufferAccess len=10 offset=%d length=%d error=%v wantErr=%v",
					tt.offset, tt.length, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isControlChar
//
// Compound condition:  char < 0x20 && char != '\t' && char != '\n' && char != '\r'
//
// Sub-conditions:
//   A: char < 0x20
//   B: char != '\t'   (i.e. not tab, 0x09)
//   C: char != '\n'   (i.e. not newline, 0x0A)
//   D: char != '\r'   (i.e. not carriage return, 0x0D)
//
// For an AND chain we need each condition to independently flip the outcome.
// All conditions must be true for the function to return true.
// We achieve MC/DC by holding three conditions true while the fourth flips:
//
//   A=F → false (printable ASCII like space or 'a')
//   A=T, B=F (char=='\t') → false
//   A=T, B=T, C=F (char=='\n') → false
//   A=T, B=T, C=T, D=F (char=='\r') → false
//   A=T, B=T, C=T, D=T (e.g. char==0x01) → true
//
// We test via SanitizeIdentifier which calls isControlChar internally.
// ---------------------------------------------------------------------------

func TestMCDC_IsControlChar(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// A=F: printable character ≥ 0x20 → not a control char → ok
		{name: "MCDC_A_false_printable_a", input: "a", wantErr: false},
		{name: "MCDC_A_false_space_0x20", input: " test", wantErr: false},
		{name: "MCDC_A_false_tilde", input: "~", wantErr: false},

		// A=T, B=F: char == '\t' (0x09, < 0x20 but explicitly excluded)
		{name: "MCDC_A_true_B_false_tab", input: "col\tname", wantErr: false},

		// A=T, B=T, C=F: char == '\n' (0x0A)
		{name: "MCDC_A_true_B_true_C_false_newline", input: "col\nname", wantErr: false},

		// A=T, B=T, C=T, D=F: char == '\r' (0x0D)
		{name: "MCDC_A_true_B_true_C_true_D_false_cr", input: "col\rname", wantErr: false},

		// A=T, B=T, C=T, D=T: SOH (0x01) — all four conditions satisfied → control char
		{name: "MCDC_A_true_B_true_C_true_D_true_SOH", input: "col\x01name", wantErr: true},

		// Additional control chars to confirm the true branch
		{name: "MCDC_A_true_all_true_BEL_0x07", input: "\x07", wantErr: true},
		{name: "MCDC_A_true_all_true_ESC_0x1B", input: "\x1b", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SanitizeIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitizeIdentifier(%q) error=%v wantErr=%v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// checkAbsolutePath
//
// Compound condition:
//   config.BlockAbsolutePaths && !config.EnforceSandbox && filepath.IsAbs(path)
//
// Sub-conditions:
//   A: config.BlockAbsolutePaths
//   B: !config.EnforceSandbox  (i.e. EnforceSandbox == false)
//   C: filepath.IsAbs(path)
//
// MC/DC truth table for AND chain (need all true for error):
//   A=F → no error (BlockAbsolutePaths off)
//   A=T, B=F (EnforceSandbox on) → no error (sandbox handles it)
//   A=T, B=T, C=F (relative path) → no error
//   A=T, B=T, C=T → ErrAbsolutePath
//
// We call ValidateDatabasePath with carefully crafted configs. We disable
// other layers (traversal, null bytes) so only checkAbsolutePath is decisive.
// ---------------------------------------------------------------------------

func TestMCDC_CheckAbsolutePath(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		blockAbsolute  bool
		enforceSandbox bool
		wantErr        error
	}{
		// A=F: BlockAbsolutePaths=false → absolute path is allowed even without sandbox
		{
			name:           "MCDC_A_false_block_off",
			path:           "/tmp/test.db",
			blockAbsolute:  false,
			enforceSandbox: false,
			wantErr:        nil,
		},
		// A=T, B=F (EnforceSandbox=true) → sandbox takes over, no AbsolutePath error
		// (path is within a sandbox root that we set to /tmp)
		{
			name:           "MCDC_A_true_B_false_sandbox_on",
			path:           "/tmp/test.db",
			blockAbsolute:  true,
			enforceSandbox: true,
			wantErr:        nil, // sandbox allows /tmp when root is /tmp
		},
		// A=T, B=T, C=F (relative path) → no absolute path error
		{
			name:           "MCDC_A_true_B_true_C_false_relative",
			path:           "relative.db",
			blockAbsolute:  true,
			enforceSandbox: false,
			wantErr:        nil,
		},
		// A=T, B=T, C=T → ErrAbsolutePath
		{
			name:           "MCDC_A_true_B_true_C_true_absolute",
			path:           "/tmp/test.db",
			blockAbsolute:  true,
			enforceSandbox: false,
			wantErr:        ErrAbsolutePath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SecurityConfig{
				BlockNullBytes:     false, // disable other layers
				BlockTraversal:     false,
				BlockSymlinks:      false,
				BlockAbsolutePaths: tt.blockAbsolute,
				EnforceSandbox:     tt.enforceSandbox,
				DatabaseRoot:       "/tmp", // used when sandbox is on
				MaxPathLength:      0,      // no length limit
			}

			_, err := ValidateDatabasePath(tt.path, cfg)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error=%v wantErr=%v", tt.path, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleNoSandboxRoot
//
// Compound condition:  config.BlockAbsolutePaths && filepath.IsAbs(path)
//
// Sub-conditions:
//   A: config.BlockAbsolutePaths
//   B: filepath.IsAbs(path)
//
// This path is reached when EnforceSandbox=true and DatabaseRoot=="".
//
// MC/DC:
//   A=F → no error
//   A=T, B=F → no error
//   A=T, B=T → ErrAbsolutePath
// ---------------------------------------------------------------------------

func TestMCDC_HandleNoSandboxRoot(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		blockAbsolute bool
		wantErr       error
	}{
		// A=F: BlockAbsolutePaths=false → absolute path passes
		{name: "MCDC_A_false_block_off_absolute", path: "/tmp/db.sqlite", blockAbsolute: false, wantErr: nil},
		// A=T, B=F: relative path → passes
		{name: "MCDC_A_true_B_false_relative", path: "db.sqlite", blockAbsolute: true, wantErr: nil},
		// A=T, B=T: absolute path blocked
		{name: "MCDC_A_true_B_true_absolute_blocked", path: "/tmp/db.sqlite", blockAbsolute: true, wantErr: ErrAbsolutePath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SecurityConfig{
				BlockNullBytes:     false,
				BlockTraversal:     false,
				BlockSymlinks:      false,
				BlockAbsolutePaths: tt.blockAbsolute,
				EnforceSandbox:     true,
				DatabaseRoot:       "", // empty root triggers handleNoSandboxRoot
				MaxPathLength:      0,
			}

			_, err := ValidateDatabasePath(tt.path, cfg)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error=%v wantErr=%v", tt.path, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// validateSandboxPrefix
//
// Compound condition:
//   cleanPath != cleanRoot && !strings.HasPrefix(cleanPath, cleanRoot+sep)
//
// Sub-conditions:
//   A: cleanPath != cleanRoot
//   B: !strings.HasPrefix(cleanPath, cleanRoot+sep)
//
// MC/DC for AND:
//   A=F (path equals root) → no error (short-circuit)
//   A=T, B=F (path starts with root+sep) → no error
//   A=T, B=T → ErrEscapesSandbox
// ---------------------------------------------------------------------------

func TestMCDC_ValidateSandboxPrefix(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		// A=F: path exactly equals the sandbox root
		{name: "MCDC_A_false_path_equals_root", path: "/sandbox", wantErr: nil},
		// A=T, B=F: path starts with root + separator
		{name: "MCDC_A_true_B_false_path_inside_root", path: "/sandbox/db.sqlite", wantErr: nil},
		{name: "MCDC_A_true_B_false_nested_inside_root", path: "/sandbox/sub/db.sqlite", wantErr: nil},
		// A=T, B=T: path outside sandbox
		{name: "MCDC_A_true_B_true_path_escapes", path: "/other/db.sqlite", wantErr: ErrEscapesSandbox},
		{name: "MCDC_A_true_B_true_root_sibling_prefix", path: "/sandboxExtra/db.sqlite", wantErr: ErrEscapesSandbox},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SecurityConfig{
				BlockNullBytes:     false,
				BlockTraversal:     false,
				BlockSymlinks:      false,
				BlockAbsolutePaths: false,
				EnforceSandbox:     true,
				DatabaseRoot:       "/sandbox",
				MaxPathLength:      0,
			}

			_, err := ValidateDatabasePath(tt.path, cfg)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error=%v wantErr=%v", tt.path, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// checkAllowlist (inner path-match condition)
//
// Compound condition:
//   path == allowedPath || strings.HasPrefix(path, allowedPath+sep)
//
// Sub-conditions:
//   A: path == allowedPath
//   B: strings.HasPrefix(path, allowedPath+sep)
//
// MC/DC for OR:
//   A=T → allowed (short-circuit)
//   A=F, B=T → allowed
//   A=F, B=F → ErrNotInAllowlist (after exhausting all subdirs)
// ---------------------------------------------------------------------------

func TestMCDC_CheckAllowlist(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		// A=T: path exactly matches the allowed subdir resolved path
		// sandbox=/sandbox, allowedSubdir="data" → allowedPath=/sandbox/data
		{name: "MCDC_A_true_exact_match", path: "/sandbox/data", wantErr: nil},

		// A=F, B=T: path has allowedPath as a prefix
		{name: "MCDC_A_false_B_true_nested_file", path: "/sandbox/data/db.sqlite", wantErr: nil},
		{name: "MCDC_A_false_B_true_deep_nested", path: "/sandbox/data/sub/a.db", wantErr: nil},

		// A=F, B=F: path is outside the allowed subdir
		{name: "MCDC_A_false_B_false_sibling_dir", path: "/sandbox/other/db.sqlite", wantErr: ErrNotInAllowlist},
		{name: "MCDC_A_false_B_false_prefix_only_no_sep", path: "/sandbox/dataExtra", wantErr: ErrNotInAllowlist},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SecurityConfig{
				BlockNullBytes:     false,
				BlockTraversal:     false,
				BlockSymlinks:      false,
				BlockAbsolutePaths: false,
				EnforceSandbox:     true,
				DatabaseRoot:       "/sandbox",
				AllowedSubdirs:     []string{"data"},
				MaxPathLength:      0,
			}

			_, err := ValidateDatabasePath(tt.path, cfg)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath(%q) error=%v wantErr=%v", tt.path, err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isRootPath
//
// Compound condition:  parent == path || parent == "." || parent == "/"
//
// Sub-conditions:
//   A: parent == path
//   B: parent == "."
//   C: parent == "/"
//
// isRootPath is exercised indirectly via walkPathForSymlinks which is called
// by checkSymlinks inside ValidateDatabasePath. We test it by supplying paths
// that exercise the loop termination. Since the function is package-private
// we call it directly.
// ---------------------------------------------------------------------------

func TestMCDC_IsRootPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantRoot bool
	}{
		// A=T: filepath.Dir(path) == path → filesystem root "/"
		{name: "MCDC_A_true_slash", path: "/", wantRoot: true},

		// A=F, B=T: parent == "." → single filename with no directory component
		{name: "MCDC_A_false_B_true_single_filename", path: "file.db", wantRoot: true},

		// A=F, B=F, C=F: path has a real parent directory
		{name: "MCDC_A_false_B_false_C_false_nested", path: "/tmp/sub/file.db", wantRoot: false},
		{name: "MCDC_A_false_B_false_C_false_two_levels", path: "/home/user", wantRoot: false},

		// A=F, B=F, C=T: parent is "/" → one-level-deep absolute path
		{name: "MCDC_A_false_B_false_C_true_top_level_abs", path: "/file.db", wantRoot: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRootPath(tt.path)
			if got != tt.wantRoot {
				t.Errorf("isRootPath(%q) = %v want %v", tt.path, got, tt.wantRoot)
			}
		})
	}
}
