package security

import "os"

type SecurityConfig struct {
	DatabaseRoot       string
	BlockNullBytes     bool
	BlockTraversal     bool
	BlockSymlinks      bool
	BlockAbsolutePaths bool
	EnforceSandbox     bool
	AllowedSubdirs     []string
	CreateMode         os.FileMode
	DirMode            os.FileMode
	MaxAttachedDBs     int
}

func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		BlockNullBytes:     true,
		BlockTraversal:     true,
		BlockSymlinks:      true,
		BlockAbsolutePaths: true,
		EnforceSandbox:     true,
		CreateMode:         0600,
		DirMode:            0700,
		MaxAttachedDBs:     10,
	}
}

// Clone creates a deep copy of the SecurityConfig.
func (c *SecurityConfig) Clone() *SecurityConfig {
	if c == nil {
		return nil
	}
	clone := &SecurityConfig{
		DatabaseRoot:       c.DatabaseRoot,
		BlockNullBytes:     c.BlockNullBytes,
		BlockTraversal:     c.BlockTraversal,
		BlockSymlinks:      c.BlockSymlinks,
		BlockAbsolutePaths: c.BlockAbsolutePaths,
		EnforceSandbox:     c.EnforceSandbox,
		CreateMode:         c.CreateMode,
		DirMode:            c.DirMode,
		MaxAttachedDBs:     c.MaxAttachedDBs,
	}
	if len(c.AllowedSubdirs) > 0 {
		clone.AllowedSubdirs = make([]string, len(c.AllowedSubdirs))
		copy(clone.AllowedSubdirs, c.AllowedSubdirs)
	}
	return clone
}
