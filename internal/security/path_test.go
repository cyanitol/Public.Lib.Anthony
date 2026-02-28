package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateDatabasePath_NullBytes(t *testing.T) {
	config := DefaultSecurityConfig()

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "null byte in middle",
			path:    "test\x00.db",
			wantErr: ErrNullByte,
		},
		{
			name:    "null byte at end",
			path:    "test.db\x00",
			wantErr: ErrNullByte,
		},
		{
			name:    "null byte at start",
			path:    "\x00test.db",
			wantErr: ErrNullByte,
		},
		{
			name:    "clean path",
			path:    "test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_PathTraversal(t *testing.T) {
	config := DefaultSecurityConfig()
	config.DatabaseRoot = "/tmp/testdb"

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "basic traversal",
			path:    "../../../etc/passwd",
			wantErr: ErrTraversal,
		},
		{
			name:    "traversal with directory",
			path:    "subdir/../../etc/passwd",
			wantErr: ErrTraversal,
		},
		{
			name:    "hidden traversal",
			path:    "test..db",
			wantErr: ErrTraversal,
		},
		{
			name:    "traversal with dots",
			path:    "./../../test.db",
			wantErr: ErrTraversal,
		},
		{
			name:    "clean relative path",
			path:    "test.db",
			wantErr: nil,
		},
		{
			name:    "clean subdirectory",
			path:    "subdir/test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_AbsolutePaths(t *testing.T) {
	// Test with sandbox disabled - absolute paths should be blocked
	config := DefaultSecurityConfig()
	config.EnforceSandbox = false // Disable sandbox so BlockAbsolutePaths takes effect

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "absolute unix path",
			path:    "/etc/passwd",
			wantErr: ErrAbsolutePath,
		},
		{
			name:    "absolute unix path 2",
			path:    "/tmp/test.db",
			wantErr: ErrAbsolutePath,
		},
		{
			name:    "relative path",
			path:    "test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_Sandbox(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultSecurityConfig()
	config.DatabaseRoot = tmpDir
	config.BlockAbsolutePaths = false // Allow absolute paths for this test

	tests := []struct {
		name       string
		path       string
		wantErr    error
		wantPrefix string
	}{
		{
			name:       "path within sandbox",
			path:       "test.db",
			wantErr:    nil,
			wantPrefix: tmpDir,
		},
		{
			name:       "subdirectory within sandbox",
			path:       "subdir/test.db",
			wantErr:    nil,
			wantPrefix: tmpDir,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && tt.wantPrefix != "" {
				if !strings.HasPrefix(result, tt.wantPrefix) {
					t.Errorf("ValidateDatabasePath() result = %v, want prefix %v", result, tt.wantPrefix)
				}
			}
		})
	}
}

func TestValidateDatabasePath_Allowlist(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create allowed subdirectories
	allowedDir := filepath.Join(tmpDir, "allowed")
	if err := os.MkdirAll(allowedDir, 0755); err != nil {
		t.Fatalf("Failed to create allowed dir: %v", err)
	}

	config := DefaultSecurityConfig()
	config.DatabaseRoot = tmpDir
	config.AllowedSubdirs = []string{"allowed"}

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "path in allowed directory",
			path:    "allowed/test.db",
			wantErr: nil,
		},
		{
			name:    "path in allowed subdirectory",
			path:    "allowed/sub/test.db",
			wantErr: nil,
		},
		{
			name:    "path not in allowlist",
			path:    "notallowed/test.db",
			wantErr: ErrNotInAllowlist,
		},
		{
			name:    "path at root not in allowlist",
			path:    "test.db",
			wantErr: ErrNotInAllowlist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_Symlinks(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a real directory and a symlink to it
	realDir := filepath.Join(tmpDir, "real")
	if err := os.MkdirAll(realDir, 0755); err != nil {
		t.Fatalf("Failed to create real dir: %v", err)
	}

	symlinkDir := filepath.Join(tmpDir, "symlink")
	if err := os.Symlink(realDir, symlinkDir); err != nil {
		t.Skipf("Cannot create symlinks on this system: %v", err)
	}

	config := DefaultSecurityConfig()
	config.DatabaseRoot = tmpDir
	config.BlockSymlinks = true

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "path through symlink",
			path:    "symlink/test.db",
			wantErr: ErrSymlink,
		},
		{
			name:    "path in real directory",
			path:    "real/test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_ControlCharacters(t *testing.T) {
	config := DefaultSecurityConfig()

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "control character 0x01",
			path:    "test\x01.db",
			wantErr: ErrNullByte,
		},
		{
			name:    "control character 0x1F",
			path:    "test\x1F.db",
			wantErr: ErrNullByte,
		},
		{
			name:    "clean path",
			path:    "test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_DisabledChecks(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &SecurityConfig{
		DatabaseRoot:       tmpDir,
		BlockNullBytes:     false,
		BlockTraversal:     false,
		BlockSymlinks:      false,
		BlockAbsolutePaths: false,
		EnforceSandbox:     false,
		AllowedSubdirs:     nil,
		CreateMode:         0600,
		DirMode:            0700,
		MaxAttachedDBs:     10,
	}

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "null byte allowed when disabled",
			path:    "test\x00.db",
			wantErr: nil,
		},
		{
			name:    "clean path",
			path:    "test.db",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateDatabasePath_RealWorldPaths(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultSecurityConfig()
	config.DatabaseRoot = tmpDir

	tests := []struct {
		name    string
		path    string
		wantErr error
	}{
		{
			name:    "simple database file",
			path:    "myapp.db",
			wantErr: nil,
		},
		{
			name:    "database in subdirectory",
			path:    "data/myapp.db",
			wantErr: nil,
		},
		{
			name:    "database with version",
			path:    "data/myapp-v1.0.db",
			wantErr: nil,
		},
		{
			name:    "attack: directory traversal",
			path:    "data/../../etc/passwd",
			wantErr: ErrTraversal,
		},
		{
			name:    "attack: absolute path outside sandbox",
			path:    "/etc/passwd",
			wantErr: ErrEscapesSandbox, // Absolute paths outside sandbox are rejected as sandbox escape
		},
		{
			name:    "attack: null byte injection",
			path:    "data/test.db\x00.txt",
			wantErr: ErrNullByte,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateDatabasePath(tt.path, config)
			if err != tt.wantErr {
				t.Errorf("ValidateDatabasePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
