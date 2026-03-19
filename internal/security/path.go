// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package security

import (
	"os"
	"path/filepath"
	"strings"
)

// ValidateDatabasePath validates a database file path using a 4-layer security model.
// Layer 1: Block malicious characters (null bytes, control characters, path traversal patterns)
// Layer 2: Resolve within sandbox and verify prefix
// Layer 3: Check allowlist if configured
// Layer 4: Detect and block symlinks if configured
func ValidateDatabasePath(path string, config *SecurityConfig) (string, error) {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	// Layer 1: Block malicious characters and patterns
	if err := validatePathCharacters(path, config); err != nil {
		return "", err
	}

	// Layer 2: Resolve within sandbox
	resolvedPath, err := resolveSandboxPath(path, config)
	if err != nil {
		return "", err
	}

	// Layer 3: Check allowlist
	if err := checkAllowlist(resolvedPath, config); err != nil {
		return "", err
	}

	// Layer 4: Detect symlinks
	if err := checkSymlinks(resolvedPath, config); err != nil {
		return "", err
	}

	return resolvedPath, nil
}

// validatePathCharacters checks for malicious characters and patterns (Layer 1).
func validatePathCharacters(path string, config *SecurityConfig) error {
	if err := checkNullAndControlBytes(path, config); err != nil {
		return err
	}

	if err := checkTraversalPattern(path, config); err != nil {
		return err
	}

	if err := checkAbsolutePath(path, config); err != nil {
		return err
	}

	return nil
}

// checkNullAndControlBytes validates null bytes and control characters.
func checkNullAndControlBytes(path string, config *SecurityConfig) error {
	if !config.BlockNullBytes {
		return nil
	}

	if strings.Contains(path, "\x00") {
		return ErrNullByte
	}

	return checkControlCharacters(path)
}

// checkControlCharacters validates control characters in the path.
func checkControlCharacters(path string) error {
	for _, char := range path {
		if isControlChar(char) {
			return ErrNullByte // Using same error for all control characters
		}
	}
	return nil
}

// isControlChar returns true if the character is a control character.
func isControlChar(char rune) bool {
	return char < 0x20 && char != '\t' && char != '\n' && char != '\r'
}

// checkTraversalPattern validates path traversal patterns.
func checkTraversalPattern(path string, config *SecurityConfig) error {
	if !config.BlockTraversal {
		return nil
	}

	if strings.Contains(path, "..") {
		return ErrTraversal
	}

	return nil
}

// checkAbsolutePath validates absolute path restrictions.
func checkAbsolutePath(path string, config *SecurityConfig) error {
	// Block absolute paths only if sandbox is not enforced
	// When sandbox is enforced, the sandbox resolution handles path security
	if config.BlockAbsolutePaths && !config.EnforceSandbox && filepath.IsAbs(path) {
		return ErrAbsolutePath
	}

	return nil
}

// resolveSandboxPath resolves the path within the sandbox (Layer 2).
func resolveSandboxPath(path string, config *SecurityConfig) (string, error) {
	if !config.EnforceSandbox {
		return filepath.Clean(path), nil
	}

	sandboxRoot := config.DatabaseRoot
	if sandboxRoot == "" {
		return handleNoSandboxRoot(path, config)
	}

	return resolveSandboxedPath(path, filepath.Clean(sandboxRoot))
}

// handleNoSandboxRoot handles path resolution when no sandbox root is configured.
func handleNoSandboxRoot(path string, config *SecurityConfig) (string, error) {
	if config.BlockAbsolutePaths && filepath.IsAbs(path) {
		return "", ErrAbsolutePath
	}
	return filepath.Clean(path), nil
}

// resolveSandboxedPath resolves a path within a sandbox root.
func resolveSandboxedPath(path, sandboxRoot string) (string, error) {
	resolvedPath := computeResolvedPath(path, sandboxRoot)
	if err := validateSandboxPrefix(resolvedPath, sandboxRoot); err != nil {
		return "", err
	}
	return resolvedPath, nil
}

// computeResolvedPath computes the resolved path based on whether it's absolute or relative.
func computeResolvedPath(path, sandboxRoot string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(sandboxRoot, path)
}

// validateSandboxPrefix validates that the resolved path is within the sandbox.
func validateSandboxPrefix(resolvedPath, sandboxRoot string) error {
	if !strings.HasPrefix(resolvedPath, sandboxRoot) {
		return ErrEscapesSandbox
	}
	if !strings.HasPrefix(filepath.Clean(resolvedPath), filepath.Clean(sandboxRoot)) {
		return ErrEscapesSandbox
	}
	return nil
}

// checkAllowlist verifies the path is in an allowed subdirectory (Layer 3).
func checkAllowlist(path string, config *SecurityConfig) error {
	if len(config.AllowedSubdirs) == 0 {
		// No allowlist configured, all paths allowed
		return nil
	}

	// Get the sandbox root for comparison
	sandboxRoot := config.DatabaseRoot
	if sandboxRoot == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		sandboxRoot = cwd
	}
	sandboxRoot = filepath.Clean(sandboxRoot)

	// Check if path is in any allowed subdirectory
	for _, allowedSubdir := range config.AllowedSubdirs {
		allowedPath := filepath.Join(sandboxRoot, allowedSubdir)
		allowedPath = filepath.Clean(allowedPath)

		// Check if path is under this allowed directory
		if strings.HasPrefix(path, allowedPath) {
			return nil
		}

		// Also allow exact match with the allowed path
		if path == allowedPath {
			return nil
		}
	}

	return ErrNotInAllowlist
}

// checkSymlinks detects and blocks symlinks (Layer 4).
func checkSymlinks(path string, config *SecurityConfig) error {
	if !config.BlockSymlinks {
		return nil
	}

	// Check if the path (or any parent) is a symlink
	return walkPathForSymlinks(path)
}

// walkPathForSymlinks walks up the path hierarchy checking for symlinks.
func walkPathForSymlinks(path string) error {
	currentPath := path

	for !isRootPath(currentPath) {
		if err := checkPathSymlink(currentPath); err != nil {
			if os.IsNotExist(err) {
				currentPath = filepath.Dir(currentPath)
				continue
			}
			return err
		}

		currentPath = filepath.Dir(currentPath)
	}

	return nil
}

// checkPathSymlink checks if the given path is a symlink.
func checkPathSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}

	return nil
}

// isRootPath returns true if the path is a root path.
func isRootPath(path string) bool {
	parent := filepath.Dir(path)
	return parent == path || parent == "." || parent == "/"
}
