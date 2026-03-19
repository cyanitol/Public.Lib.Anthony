// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"strconv"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
	"github.com/cyanitol/Public.Lib.Anthony/internal/security"
)

// DriverConfig represents configuration options for the SQLite driver.
type DriverConfig struct {
	// Pager configuration
	Pager *pager.PagerConfig

	// Security configuration
	Security *security.SecurityConfig

	// QueryTimeout is the maximum duration for a query to execute.
	// 0 means no timeout.
	// Default is 0.
	QueryTimeout time.Duration

	// MaxConnections is the maximum number of open connections to the database.
	// 0 means no limit.
	// Default is 0.
	MaxConnections int

	// MaxIdleConnections is the maximum number of idle connections in the pool.
	// Default is 2.
	MaxIdleConnections int

	// ConnectionMaxLifetime is the maximum duration a connection can be reused.
	// 0 means connections are reused forever.
	// Default is 0.
	ConnectionMaxLifetime time.Duration

	// ConnectionMaxIdleTime is the maximum duration a connection can be idle.
	// 0 means connections can be idle forever.
	// Default is 0.
	ConnectionMaxIdleTime time.Duration

	// EnableForeignKeys enables foreign key constraint enforcement.
	// Default is false (matches SQLite behavior).
	EnableForeignKeys bool

	// EnableTriggers enables trigger execution.
	// Default is true.
	EnableTriggers bool

	// EnableQueryLog enables logging of all SQL queries.
	// Default is false.
	EnableQueryLog bool

	// CaseSensitiveLike makes LIKE operator case-sensitive.
	// Default is false (case-insensitive).
	CaseSensitiveLike bool

	// RecursiveTriggers enables recursive trigger execution.
	// Default is false.
	RecursiveTriggers bool

	// AutoVacuum specifies the auto-vacuum mode.
	// Valid values: "none", "full", "incremental"
	// Default is "none".
	AutoVacuum string

	// SharedCache enables shared cache mode for connections.
	// Default is false.
	SharedCache bool

	// Extensions is a list of SQLite extensions to load on connection.
	// Default is empty.
	Extensions []string
}

// DefaultDriverConfig returns a DriverConfig with default values.
func DefaultDriverConfig() *DriverConfig {
	return &DriverConfig{
		Pager:                 pager.DefaultPagerConfig(),
		Security:              security.DefaultSecurityConfig(),
		QueryTimeout:          0,
		MaxConnections:        0,
		MaxIdleConnections:    2,
		ConnectionMaxLifetime: 0,
		ConnectionMaxIdleTime: 0,
		EnableForeignKeys:     false,
		EnableTriggers:        true,
		EnableQueryLog:        false,
		CaseSensitiveLike:     false,
		RecursiveTriggers:     false,
		AutoVacuum:            "none",
		SharedCache:           false,
		Extensions:            []string{},
	}
}

// Validate checks if the configuration values are valid.
func (c *DriverConfig) Validate() error {
	// Validate pager config
	if c.Pager == nil {
		c.Pager = pager.DefaultPagerConfig()
	}
	if err := c.Pager.Validate(); err != nil {
		return err
	}

	// Validate security config
	if c.Security == nil {
		c.Security = security.DefaultSecurityConfig()
	}

	// Validate max connections
	if c.MaxConnections < 0 {
		c.MaxConnections = 0
	}

	// Validate max idle connections
	if c.MaxIdleConnections < 0 {
		c.MaxIdleConnections = 2
	}

	// Validate auto-vacuum mode
	switch c.AutoVacuum {
	case "none", "full", "incremental":
		// Valid
	default:
		c.AutoVacuum = "none"
	}

	return nil
}

// Clone creates a deep copy of the DriverConfig.
func (c *DriverConfig) Clone() *DriverConfig {
	clone := &DriverConfig{
		Pager:                 c.Pager.Clone(),
		Security:              c.Security.Clone(),
		QueryTimeout:          c.QueryTimeout,
		MaxConnections:        c.MaxConnections,
		MaxIdleConnections:    c.MaxIdleConnections,
		ConnectionMaxLifetime: c.ConnectionMaxLifetime,
		ConnectionMaxIdleTime: c.ConnectionMaxIdleTime,
		EnableForeignKeys:     c.EnableForeignKeys,
		EnableTriggers:        c.EnableTriggers,
		EnableQueryLog:        c.EnableQueryLog,
		CaseSensitiveLike:     c.CaseSensitiveLike,
		RecursiveTriggers:     c.RecursiveTriggers,
		AutoVacuum:            c.AutoVacuum,
		SharedCache:           c.SharedCache,
		Extensions:            make([]string, len(c.Extensions)),
	}
	copy(clone.Extensions, c.Extensions)
	return clone
}

// pragmaHandler is a function that conditionally appends a pragma to the list.
type pragmaHandler func(*DriverConfig, *[]string)

// ApplyPragmas returns a list of PRAGMA statements to execute on connection.
func (c *DriverConfig) ApplyPragmas() []string {
	pragmas := []string{}

	// Table-driven approach: each handler checks one condition and appends if needed
	handlers := []pragmaHandler{
		c.applyForeignKeys,
		c.applyJournalMode,
		c.applySyncMode,
		c.applyCacheSize,
		c.applyLockingMode,
		c.applyAutoVacuum,
		c.applyCaseSensitiveLike,
		c.applyRecursiveTriggers,
		c.applyTempStore,
	}

	for _, handler := range handlers {
		handler(c, &pragmas)
	}

	return pragmas
}

func (c *DriverConfig) applyForeignKeys(_ *DriverConfig, pragmas *[]string) {
	if c.EnableForeignKeys {
		*pragmas = append(*pragmas, "PRAGMA foreign_keys = ON")
	} else {
		*pragmas = append(*pragmas, "PRAGMA foreign_keys = OFF")
	}
}

func (c *DriverConfig) applyJournalMode(_ *DriverConfig, pragmas *[]string) {
	if c.Pager.JournalMode != "" {
		*pragmas = append(*pragmas, "PRAGMA journal_mode = "+c.Pager.JournalMode)
	}
}

func (c *DriverConfig) applySyncMode(_ *DriverConfig, pragmas *[]string) {
	if c.Pager.SyncMode != "" {
		*pragmas = append(*pragmas, "PRAGMA synchronous = "+c.Pager.SyncMode)
	}
}

func (c *DriverConfig) applyCacheSize(_ *DriverConfig, pragmas *[]string) {
	if c.Pager.CacheSize != 0 {
		*pragmas = append(*pragmas, "PRAGMA cache_size = "+strconv.Itoa(c.Pager.CacheSize))
	}
}

func (c *DriverConfig) applyLockingMode(_ *DriverConfig, pragmas *[]string) {
	if c.Pager.LockingMode != "" {
		*pragmas = append(*pragmas, "PRAGMA locking_mode = "+c.Pager.LockingMode)
	}
}

func (c *DriverConfig) applyAutoVacuum(_ *DriverConfig, pragmas *[]string) {
	if c.AutoVacuum != "none" {
		*pragmas = append(*pragmas, "PRAGMA auto_vacuum = "+c.AutoVacuum)
	}
}

func (c *DriverConfig) applyCaseSensitiveLike(_ *DriverConfig, pragmas *[]string) {
	if c.CaseSensitiveLike {
		*pragmas = append(*pragmas, "PRAGMA case_sensitive_like = ON")
	}
}

func (c *DriverConfig) applyRecursiveTriggers(_ *DriverConfig, pragmas *[]string) {
	if c.RecursiveTriggers {
		*pragmas = append(*pragmas, "PRAGMA recursive_triggers = ON")
	}
}

func (c *DriverConfig) applyTempStore(_ *DriverConfig, pragmas *[]string) {
	if c.Pager.TempStore != "default" {
		*pragmas = append(*pragmas, "PRAGMA temp_store = "+c.Pager.TempStore)
	}
}
