// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cyanitol/Public.Lib.Anthony/internal/pager"
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

		if err := parseParameter(config, strings.ToLower(key), value); err != nil {
			return err
		}
	}

	return nil
}

// parseParameter parses a single DSN parameter and updates the config.
func parseParameter(config *DriverConfig, key, value string) error {
	// Handle string parameters
	if parseStringParameter(config, key, value) {
		return nil
	}

	// Handle int/timeout parameters
	if err := parseNumericParameter(config, key, value); err == nil {
		return nil
	} else if err.Error() != "unknown parameter" {
		return err
	}

	// Handle bool parameters
	if err := parseBoolParameter2(config, key, value); err == nil {
		return nil
	} else if err.Error() != "unknown parameter" {
		return err
	}

	// Handle special parameters
	return parseSpecialParameter(config, key, value)
}

// parseStringParameter handles string-valued parameters.
func parseStringParameter(config *DriverConfig, key, value string) bool {
	lowerValue := strings.ToLower(value)
	switch key {
	case "journal_mode", "journalmode":
		config.Pager.JournalMode = lowerValue
	case "synchronous", "sync":
		config.Pager.SyncMode = lowerValue
	case "locking_mode", "lockingmode":
		config.Pager.LockingMode = lowerValue
	case "temp_store", "tempstore":
		config.Pager.TempStore = lowerValue
	case "auto_vacuum", "autovacuum":
		config.AutoVacuum = lowerValue
	default:
		return false
	}
	return true
}

// parseNumericParameter handles integer and timeout parameters.
func parseNumericParameter(config *DriverConfig, key, value string) error {
	switch key {
	case "cache_size", "cachesize":
		return parseIntParameter(value, "cache_size", &config.Pager.CacheSize)
	case "page_size", "pagesize":
		return parseIntParameter(value, "page_size", &config.Pager.PageSize)
	case "wal_autocheckpoint", "walautocheckpoint":
		return parseIntParameter(value, "wal_autocheckpoint", &config.Pager.WALAutocheckpoint)
	case "max_page_count", "maxpagecount":
		return parseIntParameter(value, "max_page_count", &config.Pager.MaxPageCount)
	case "busy_timeout", "busytimeout":
		return parseTimeoutParameter(value, "busy_timeout", &config.Pager.BusyTimeout)
	case "query_timeout", "querytimeout":
		return parseTimeoutParameter(value, "query_timeout", &config.QueryTimeout)
	}
	return fmt.Errorf("unknown parameter")
}

// parseBoolParameter2 handles boolean parameters.
func parseBoolParameter2(config *DriverConfig, key, value string) error {
	switch key {
	case "foreign_keys", "foreignkeys":
		return parseBoolConfigParameter(value, "foreign_keys", &config.EnableForeignKeys)
	case "triggers":
		return parseBoolConfigParameter(value, "triggers", &config.EnableTriggers)
	case "case_sensitive_like", "casesensitivelike":
		return parseBoolConfigParameter(value, "case_sensitive_like", &config.CaseSensitiveLike)
	case "recursive_triggers", "recursivetriggers":
		return parseBoolConfigParameter(value, "recursive_triggers", &config.RecursiveTriggers)
	}
	return fmt.Errorf("unknown parameter")
}

// parseSpecialParameter handles special case parameters.
func parseSpecialParameter(config *DriverConfig, key, value string) error {
	switch key {
	case "mode":
		return parseModeParameter(config, value)
	case "cache":
		return parseCacheParameter(config, value)
	case "_query_only", "queryonly":
		return parseQueryOnlyParameter(config, value)
	}
	return nil
}

// parseIntParameter parses an integer parameter.
func parseIntParameter(value, name string, target *int) error {
	result, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", name, err)
	}
	*target = result
	return nil
}

// parseTimeoutParameter parses a timeout parameter in milliseconds.
func parseTimeoutParameter(value, name string, target *time.Duration) error {
	timeout, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", name, err)
	}
	*target = time.Duration(timeout) * time.Millisecond
	return nil
}

// parseBoolConfigParameter parses a boolean parameter and updates config.
func parseBoolConfigParameter(value, name string, target *bool) error {
	result, err := parseBoolParameter(value)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", name, err)
	}
	*target = result
	return nil
}

// parseQueryOnlyParameter parses the _query_only parameter.
func parseQueryOnlyParameter(config *DriverConfig, value string) error {
	qo, err := parseBoolParameter(value)
	if err != nil {
		return fmt.Errorf("invalid _query_only: %w", err)
	}
	if qo {
		config.Pager.ReadOnly = true
		config.Pager.NoLock = true
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

	params := buildDSNParameters(dsn.Config)
	if len(params) == 0 {
		return dsn.Filename
	}
	return dsn.Filename + "?" + params.Encode()
}

// buildDSNParameters builds URL query parameters from driver config.
func buildDSNParameters(config *DriverConfig) url.Values {
	params := url.Values{}

	addModeParameter(params, config)
	addCacheParameter(params, config)
	addPagerParameters(params, config)
	addDriverParameters(params, config)

	return params
}

// addModeParameter adds the mode parameter if needed.
func addModeParameter(params url.Values, config *DriverConfig) {
	if config.Pager.ReadOnly {
		params.Add("mode", "ro")
	}
}

// addCacheParameter adds the cache parameter if needed.
func addCacheParameter(params url.Values, config *DriverConfig) {
	if config.SharedCache {
		params.Add("cache", "shared")
	}
}

// addPagerParameters adds pager-related parameters.
func addPagerParameters(params url.Values, config *DriverConfig) {
	addJournalModeParam(params, config)
	addSyncModeParam(params, config)
	addCacheSizeParam(params, config)
	addPageSizeParam(params, config)
	addBusyTimeoutParam(params, config)
}

// addJournalModeParam adds journal mode parameter if non-default
func addJournalModeParam(params url.Values, config *DriverConfig) {
	if config.Pager.JournalMode != "delete" && config.Pager.JournalMode != "" {
		params.Add("journal_mode", config.Pager.JournalMode)
	}
}

// addSyncModeParam adds sync mode parameter if non-default
func addSyncModeParam(params url.Values, config *DriverConfig) {
	if config.Pager.SyncMode != "full" && config.Pager.SyncMode != "" {
		params.Add("synchronous", config.Pager.SyncMode)
	}
}

// addCacheSizeParam adds cache size parameter if non-default
func addCacheSizeParam(params url.Values, config *DriverConfig) {
	if config.Pager.CacheSize != pager.DefaultCacheSize {
		params.Add("cache_size", strconv.Itoa(config.Pager.CacheSize))
	}
}

// addPageSizeParam adds page size parameter if non-default
func addPageSizeParam(params url.Values, config *DriverConfig) {
	if config.Pager.PageSize != 4096 {
		params.Add("page_size", strconv.Itoa(config.Pager.PageSize))
	}
}

// addBusyTimeoutParam adds busy timeout parameter if non-default
func addBusyTimeoutParam(params url.Values, config *DriverConfig) {
	if config.Pager.BusyTimeout != 5*time.Second {
		params.Add("busy_timeout", strconv.Itoa(int(config.Pager.BusyTimeout.Milliseconds())))
	}
}

// addDriverParameters adds driver-specific parameters.
func addDriverParameters(params url.Values, config *DriverConfig) {
	if !config.EnableForeignKeys {
		params.Add("foreign_keys", "off")
	}
	if !config.EnableTriggers {
		params.Add("triggers", "off")
	}
}
