package driver

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
)

// DSN represents a parsed Data Source Name with all configuration options.
type DSN struct {
	// Filename is the path to the database file
	Filename string

	// Config holds all driver and pager configuration options
	Config *DriverConfig
}

// ParseDSN parses a SQLite DSN string and returns a DSN struct.
// The DSN format is: filename?param1=value1&param2=value2&...
//
// Supported parameters:
//   - mode: "ro", "rw", "rwc", "memory" (read-only, read-write, read-write-create, memory)
//   - cache: "shared", "private" (shared cache mode)
//   - journal_mode: "delete", "truncate", "persist", "memory", "wal", "off"
//   - synchronous: "off", "normal", "full", "extra"
//   - cache_size: number of pages (positive) or KB (negative)
//   - page_size: bytes (must be power of 2 between 512 and 65536)
//   - locking_mode: "normal", "exclusive"
//   - temp_store: "default", "file", "memory"
//   - foreign_keys: "on", "off", "true", "false", "1", "0"
//   - triggers: "on", "off", "true", "false", "1", "0"
//   - busy_timeout: milliseconds
//   - auto_vacuum: "none", "full", "incremental"
//   - case_sensitive_like: "on", "off", "true", "false", "1", "0"
//   - recursive_triggers: "on", "off", "true", "false", "1", "0"
//   - wal_autocheckpoint: number of pages
//   - query_timeout: milliseconds
//   - max_page_count: maximum number of pages
//
// Examples:
//   - ParseDSN("file.db")
//   - ParseDSN("file.db?mode=ro")
//   - ParseDSN("file.db?journal_mode=wal&cache_size=10000")
//   - ParseDSN("file.db?foreign_keys=on&busy_timeout=5000")
//   - ParseDSN(":memory:")
func ParseDSN(dsn string) (*DSN, error) {
	// Start with default configuration
	config := DefaultDriverConfig()

	// Check for special cases
	if dsn == "" || dsn == ":memory:" {
		config.Pager.MemoryDB = true
		return &DSN{
			Filename: ":memory:",
			Config:   config,
		}, nil
	}

	// Split DSN into filename and query parameters
	filename := dsn
	var query string

	if idx := strings.Index(dsn, "?"); idx >= 0 {
		filename = dsn[:idx]
		query = dsn[idx+1:]
	}

	// Parse query parameters
	if query != "" {
		if err := parseQueryParameters(config, query); err != nil {
			return nil, fmt.Errorf("invalid DSN parameter: %w", err)
		}
	}

	// Validate the configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &DSN{
		Filename: filename,
		Config:   config,
	}, nil
}

// parseQueryParameters parses the query string and updates the config.
func parseQueryParameters(config *DriverConfig, query string) error {
	// Parse as URL query parameters
	values, err := url.ParseQuery(query)
	if err != nil {
		return fmt.Errorf("failed to parse query parameters: %w", err)
	}

	for key, vals := range values {
		if len(vals) == 0 {
			continue
		}
		value := vals[0]

		switch strings.ToLower(key) {
		case "mode":
			if err := parseModeParameter(config, value); err != nil {
				return err
			}

		case "cache":
			if err := parseCacheParameter(config, value); err != nil {
				return err
			}

		case "journal_mode", "journalmode":
			config.Pager.JournalMode = strings.ToLower(value)

		case "synchronous", "sync":
			config.Pager.SyncMode = strings.ToLower(value)

		case "cache_size", "cachesize":
			cacheSize, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid cache_size: %w", err)
			}
			config.Pager.CacheSize = cacheSize

		case "page_size", "pagesize":
			pageSize, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid page_size: %w", err)
			}
			config.Pager.PageSize = pageSize

		case "locking_mode", "lockingmode":
			config.Pager.LockingMode = strings.ToLower(value)

		case "temp_store", "tempstore":
			config.Pager.TempStore = strings.ToLower(value)

		case "foreign_keys", "foreignkeys":
			fk, err := parseBoolParameter(value)
			if err != nil {
				return fmt.Errorf("invalid foreign_keys: %w", err)
			}
			config.EnableForeignKeys = fk

		case "triggers":
			triggers, err := parseBoolParameter(value)
			if err != nil {
				return fmt.Errorf("invalid triggers: %w", err)
			}
			config.EnableTriggers = triggers

		case "busy_timeout", "busytimeout":
			timeout, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid busy_timeout: %w", err)
			}
			config.Pager.BusyTimeout = time.Duration(timeout) * time.Millisecond

		case "auto_vacuum", "autovacuum":
			config.AutoVacuum = strings.ToLower(value)

		case "case_sensitive_like", "casesensitivelike":
			csl, err := parseBoolParameter(value)
			if err != nil {
				return fmt.Errorf("invalid case_sensitive_like: %w", err)
			}
			config.CaseSensitiveLike = csl

		case "recursive_triggers", "recursivetriggers":
			rt, err := parseBoolParameter(value)
			if err != nil {
				return fmt.Errorf("invalid recursive_triggers: %w", err)
			}
			config.RecursiveTriggers = rt

		case "wal_autocheckpoint", "walautocheckpoint":
			checkpoint, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid wal_autocheckpoint: %w", err)
			}
			config.Pager.WALAutocheckpoint = checkpoint

		case "query_timeout", "querytimeout":
			timeout, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid query_timeout: %w", err)
			}
			config.QueryTimeout = time.Duration(timeout) * time.Millisecond

		case "max_page_count", "maxpagecount":
			maxPages, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid max_page_count: %w", err)
			}
			config.Pager.MaxPageCount = maxPages

		case "_query_only", "queryonly":
			// Extension: query-only mode (read-only with no locking)
			qo, err := parseBoolParameter(value)
			if err != nil {
				return fmt.Errorf("invalid _query_only: %w", err)
			}
			if qo {
				config.Pager.ReadOnly = true
				config.Pager.NoLock = true
			}

		default:
			// Ignore unknown parameters for forward compatibility
			// Could also return an error if strict validation is desired
			continue
		}
	}

	return nil
}

// parseModeParameter parses the mode parameter.
func parseModeParameter(config *DriverConfig, value string) error {
	switch strings.ToLower(value) {
	case "ro", "readonly":
		config.Pager.ReadOnly = true

	case "rw", "readwrite":
		config.Pager.ReadOnly = false

	case "rwc", "readwritecreate":
		config.Pager.ReadOnly = false

	case "memory":
		config.Pager.MemoryDB = true

	default:
		return fmt.Errorf("invalid mode: %s (expected ro, rw, rwc, or memory)", value)
	}

	return nil
}

// parseCacheParameter parses the cache parameter.
func parseCacheParameter(config *DriverConfig, value string) error {
	switch strings.ToLower(value) {
	case "shared":
		config.SharedCache = true

	case "private":
		config.SharedCache = false

	default:
		return fmt.Errorf("invalid cache: %s (expected shared or private)", value)
	}

	return nil
}

// parseBoolParameter parses a boolean parameter value.
func parseBoolParameter(value string) (bool, error) {
	switch strings.ToLower(value) {
	case "on", "true", "yes", "1":
		return true, nil
	case "off", "false", "no", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %s", value)
	}
}

// FormatDSN formats a DSN struct back into a DSN string.
func FormatDSN(dsn *DSN) string {
	if dsn.Filename == ":memory:" || dsn.Config.Pager.MemoryDB {
		return ":memory:"
	}

	// Build query parameters from config
	params := url.Values{}

	if dsn.Config.Pager.ReadOnly {
		params.Add("mode", "ro")
	}

	if dsn.Config.SharedCache {
		params.Add("cache", "shared")
	}

	if dsn.Config.Pager.JournalMode != "delete" && dsn.Config.Pager.JournalMode != "" {
		params.Add("journal_mode", dsn.Config.Pager.JournalMode)
	}

	if dsn.Config.Pager.SyncMode != "full" && dsn.Config.Pager.SyncMode != "" {
		params.Add("synchronous", dsn.Config.Pager.SyncMode)
	}

	if dsn.Config.Pager.CacheSize != pager.DefaultCacheSize {
		params.Add("cache_size", strconv.Itoa(dsn.Config.Pager.CacheSize))
	}

	if dsn.Config.Pager.PageSize != 4096 {
		params.Add("page_size", strconv.Itoa(dsn.Config.Pager.PageSize))
	}

	if !dsn.Config.EnableForeignKeys {
		params.Add("foreign_keys", "off")
	}

	if !dsn.Config.EnableTriggers {
		params.Add("triggers", "off")
	}

	if dsn.Config.Pager.BusyTimeout != 5*time.Second {
		params.Add("busy_timeout", strconv.Itoa(int(dsn.Config.Pager.BusyTimeout.Milliseconds())))
	}

	// Build final DSN
	if len(params) == 0 {
		return dsn.Filename
	}

	return dsn.Filename + "?" + params.Encode()
}
