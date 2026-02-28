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
	// Block null bytes
	if config.BlockNullBytes {
		if strings.Contains(path, "\x00") {
			return ErrNullByte
		}

		// Block control characters (0x00-0x1F except tab, newline, carriage return which are unlikely in paths)
		for _, char := range path {
			if char < 0x20 && char != '\t' && char != '\n' && char != '\r' {
				return ErrNullByte // Using same error for all control characters
			}
		}
	}

	// Block path traversal patterns
	if config.BlockTraversal {
		// Check for .. patterns
		if strings.Contains(path, "..") {
			return ErrTraversal
		}
	}

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
		// If sandbox not enforced, just clean the path
		return filepath.Clean(path), nil
	}

	// Get the sandbox root
	sandboxRoot := config.DatabaseRoot
	if sandboxRoot == "" {
		// No sandbox root configured - if absolute paths are blocked, reject them
		// Otherwise allow relative paths only
		if config.BlockAbsolutePaths && filepath.IsAbs(path) {
			return "", ErrAbsolutePath
		}
		return filepath.Clean(path), nil
	}

	// Clean the sandbox root
	sandboxRoot = filepath.Clean(sandboxRoot)

	var resolvedPath string
	if filepath.IsAbs(path) {
		// If path is already absolute, check if it's within sandbox
		resolvedPath = filepath.Clean(path)
	} else {
		// Join the relative path with the sandbox root
		resolvedPath = filepath.Join(sandboxRoot, path)
	}

	// Ensure the resolved path is within the sandbox
	// We need to check that the resolved path has the sandbox as a prefix
	if !strings.HasPrefix(resolvedPath, sandboxRoot) {
		return "", ErrEscapesSandbox
	}

	// Additional check: ensure no path traversal escaped the sandbox
	// by comparing the cleaned joined path with a clean version of the concatenation
	if !strings.HasPrefix(filepath.Clean(resolvedPath), filepath.Clean(sandboxRoot)) {
		return "", ErrEscapesSandbox
	}

	return resolvedPath, nil
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
	// We need to check each component of the path
	currentPath := path

	for {
		// Check if current path exists and is a symlink
		info, err := os.Lstat(currentPath)
		if err != nil {
			// If the path doesn't exist yet, that's okay (it's a new database file)
			// Check the parent directory instead
			if os.IsNotExist(err) {
				parent := filepath.Dir(currentPath)
				if parent == currentPath || parent == "." || parent == "/" {
					// Reached the root, no symlinks found
					break
				}
				currentPath = parent
				continue
			}
			// Other errors are real problems
			return err
		}

		// Check if it's a symlink
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrSymlink
		}

		// Move to parent directory
		parent := filepath.Dir(currentPath)
		if parent == currentPath || parent == "." || parent == "/" {
			// Reached the root
			break
		}
		currentPath = parent
	}

	return nil
}
