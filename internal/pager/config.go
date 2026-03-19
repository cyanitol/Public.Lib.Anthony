// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import "time"

// PagerConfig represents configuration options for the pager.
type PagerConfig struct {
	// PageSize is the size of each database page in bytes.
	// Must be a power of 2 between 512 and 65536.
	// Default is 4096.
	PageSize int

	// CacheSize is the number of pages to keep in the page cache.
	// Default is 2000 pages.
	CacheSize int

	// JournalMode specifies the journaling mode for transactions.
	// Valid values: "delete", "truncate", "persist", "memory", "wal", "off"
	// Default is "delete".
	JournalMode string

	// SyncMode specifies the synchronization mode for file writes.
	// Valid values: "off", "normal", "full", "extra"
	// Default is "full".
	SyncMode string

	// LockingMode specifies the locking mode.
	// Valid values: "normal", "exclusive"
	// Default is "normal".
	LockingMode string

	// TempStore specifies where temporary tables and indices are stored.
	// Valid values: "default", "file", "memory"
	// Default is "default".
	TempStore string

	// BusyTimeout is the duration to wait when the database is locked.
	// Default is 5 seconds.
	BusyTimeout time.Duration

	// WALAutocheckpoint is the number of pages in WAL file before auto-checkpoint.
	// Default is 1000.
	WALAutocheckpoint int

	// MaxPageCount is the maximum number of pages in the database.
	// 0 means no limit.
	// Default is 0.
	MaxPageCount int

	// ReadOnly indicates if the database should be opened in read-only mode.
	// Default is false.
	ReadOnly bool

	// MemoryDB indicates if this is an in-memory database.
	// Default is false.
	MemoryDB bool

	// NoLock disables file locking (for testing or special cases).
	// Default is false.
	NoLock bool
}

// DefaultPagerConfig returns a PagerConfig with default values.
func DefaultPagerConfig() *PagerConfig {
	return &PagerConfig{
		PageSize:          4096,
		CacheSize:         2000,
		JournalMode:       "delete",
		SyncMode:          "full",
		LockingMode:       "normal",
		TempStore:         "default",
		BusyTimeout:       5 * time.Second,
		WALAutocheckpoint: 1000,
		MaxPageCount:      0,
		ReadOnly:          false,
		MemoryDB:          false,
		NoLock:            false,
	}
}

// stringValidator defines a validation rule for a string field.
type stringValidator struct {
	field        *string
	validValues  []string
	defaultValue string
}

// intValidator defines a validation rule for an integer field.
type intValidator struct {
	field        *int
	minValue     int
	defaultValue int
}

// durationValidator defines a validation rule for a duration field.
type durationValidator struct {
	field        *time.Duration
	minValue     time.Duration
	defaultValue time.Duration
}

// Validate checks if the configuration values are valid.
func (c *PagerConfig) Validate() error {
	// Validate page size (must be power of 2 between 512 and 65536)
	if c.PageSize < 512 || c.PageSize > 65536 {
		return ErrInvalidPageSize
	}
	if c.PageSize&(c.PageSize-1) != 0 {
		return ErrInvalidPageSize
	}

	// Table-driven string field validations
	stringRules := []stringValidator{
		{&c.JournalMode, []string{"delete", "truncate", "persist", "memory", "wal", "off"}, "delete"},
		{&c.SyncMode, []string{"off", "normal", "full", "extra"}, "full"},
		{&c.LockingMode, []string{"normal", "exclusive"}, "normal"},
		{&c.TempStore, []string{"default", "file", "memory"}, "default"},
	}

	for _, rule := range stringRules {
		validateStringField(rule)
	}

	// Table-driven integer field validations
	intRules := []intValidator{
		{&c.CacheSize, 1, DefaultCacheSize},
		{&c.WALAutocheckpoint, 1, 1000},
	}

	for _, rule := range intRules {
		validateIntField(rule)
	}

	// Duration field validation
	durationRules := []durationValidator{
		{&c.BusyTimeout, 0, 5 * time.Second},
	}

	for _, rule := range durationRules {
		validateDurationField(rule)
	}

	return nil
}

// validateStringField validates and sets default for a string field.
func validateStringField(rule stringValidator) {
	for _, valid := range rule.validValues {
		if *rule.field == valid {
			return
		}
	}
	*rule.field = rule.defaultValue
}

// validateIntField validates and sets default for an integer field.
func validateIntField(rule intValidator) {
	if *rule.field < rule.minValue {
		*rule.field = rule.defaultValue
	}
}

// validateDurationField validates and sets default for a duration field.
func validateDurationField(rule durationValidator) {
	if *rule.field < rule.minValue {
		*rule.field = rule.defaultValue
	}
}

// JournalModeValue returns the integer value for the journal mode.
func (c *PagerConfig) JournalModeValue() int {
	switch c.JournalMode {
	case "delete":
		return JournalModeDelete
	case "persist":
		return JournalModePersist
	case "off":
		return JournalModeOff
	case "truncate":
		return JournalModeTruncate
	case "memory":
		return JournalModeMemory
	case "wal":
		return JournalModeWAL
	default:
		return JournalModeDelete
	}
}

// Clone creates a deep copy of the PagerConfig.
func (c *PagerConfig) Clone() *PagerConfig {
	clone := *c
	return &clone
}
