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
