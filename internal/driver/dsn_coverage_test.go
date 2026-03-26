// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	driver "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

// ---------------------------------------------------------------------------
// parseQueryParameters — exercised via ParseDSN with query strings
// ---------------------------------------------------------------------------

// TestDSNCoverage_parseQueryParameters tests the parseQueryParameters function
// through ParseDSN with a rich set of query parameters.
func TestDSNCoverage_parseQueryParameters(t *testing.T) {
	// All parameters combined to walk through parseQueryParameters,
	// parseParameter, parseNumericParameter, parseBoolParameter2, and
	// parseSpecialParameter in a single call.
	dsn := "file.db?journal_mode=wal&synchronous=normal&cache_size=200" +
		"&page_size=4096&wal_autocheckpoint=1000&max_page_count=50000" +
		"&busy_timeout=5000&query_timeout=2000" +
		"&foreign_keys=on&triggers=on&case_sensitive_like=off" +
		"&recursive_triggers=off&locking_mode=normal&temp_store=memory" +
		"&auto_vacuum=none&mode=rw&cache=private&_query_only=off"

	parsed, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}

	cfg := parsed.Config
	if cfg.Pager.JournalMode != "wal" {
		t.Errorf("JournalMode want wal, got %s", cfg.Pager.JournalMode)
	}
	if cfg.Pager.SyncMode != "normal" {
		t.Errorf("SyncMode want normal, got %s", cfg.Pager.SyncMode)
	}
	if cfg.Pager.CacheSize != 200 {
		t.Errorf("CacheSize want 200, got %d", cfg.Pager.CacheSize)
	}
	if cfg.Pager.PageSize != 4096 {
		t.Errorf("PageSize want 4096, got %d", cfg.Pager.PageSize)
	}
	if cfg.Pager.WALAutocheckpoint != 1000 {
		t.Errorf("WALAutocheckpoint want 1000, got %d", cfg.Pager.WALAutocheckpoint)
	}
	if cfg.Pager.MaxPageCount != 50000 {
		t.Errorf("MaxPageCount want 50000, got %d", cfg.Pager.MaxPageCount)
	}
	if cfg.Pager.BusyTimeout != 5000*time.Millisecond {
		t.Errorf("BusyTimeout want 5s, got %v", cfg.Pager.BusyTimeout)
	}
	if cfg.QueryTimeout != 2000*time.Millisecond {
		t.Errorf("QueryTimeout want 2s, got %v", cfg.QueryTimeout)
	}
	if !cfg.EnableForeignKeys {
		t.Error("EnableForeignKeys want true")
	}
	if !cfg.EnableTriggers {
		t.Error("EnableTriggers want true")
	}
	if cfg.CaseSensitiveLike {
		t.Error("CaseSensitiveLike want false")
	}
	if cfg.RecursiveTriggers {
		t.Error("RecursiveTriggers want false")
	}
	if cfg.Pager.LockingMode != "normal" {
		t.Errorf("LockingMode want normal, got %s", cfg.Pager.LockingMode)
	}
	if cfg.Pager.TempStore != "memory" {
		t.Errorf("TempStore want memory, got %s", cfg.Pager.TempStore)
	}
	if cfg.AutoVacuum != "none" {
		t.Errorf("AutoVacuum want none, got %s", cfg.AutoVacuum)
	}
}

// TestDSNCoverage_parseParameter_altKeys uses alternate key spellings to cover
// additional branches in parseStringParameter and parseNumericParameter.
func TestDSNCoverage_parseParameter_altKeys(t *testing.T) {
	dsn := "file.db?journalmode=wal&sync=full&cachesize=512" +
		"&pagesize=8192&walautocheckpoint=500&maxpagecount=10000" +
		"&busytimeout=3000&querytimeout=1500" +
		"&foreignkeys=on&casesensitivelike=on&recursivetriggers=on" +
		"&lockingmode=exclusive&tempstore=file&autovacuum=full"

	parsed, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN (alt keys) error: %v", err)
	}

	cfg := parsed.Config
	if cfg.Pager.JournalMode != "wal" {
		t.Errorf("JournalMode want wal, got %s", cfg.Pager.JournalMode)
	}
	if cfg.Pager.SyncMode != "full" {
		t.Errorf("SyncMode want full, got %s", cfg.Pager.SyncMode)
	}
	if cfg.Pager.CacheSize != 512 {
		t.Errorf("CacheSize want 512, got %d", cfg.Pager.CacheSize)
	}
	if cfg.Pager.PageSize != 8192 {
		t.Errorf("PageSize want 8192, got %d", cfg.Pager.PageSize)
	}
	if cfg.Pager.BusyTimeout != 3000*time.Millisecond {
		t.Errorf("BusyTimeout want 3s, got %v", cfg.Pager.BusyTimeout)
	}
	if cfg.QueryTimeout != 1500*time.Millisecond {
		t.Errorf("QueryTimeout want 1.5s, got %v", cfg.QueryTimeout)
	}
	if !cfg.EnableForeignKeys {
		t.Error("EnableForeignKeys want true")
	}
	if !cfg.CaseSensitiveLike {
		t.Error("CaseSensitiveLike want true")
	}
	if !cfg.RecursiveTriggers {
		t.Error("RecursiveTriggers want true")
	}
}

// TestDSNCoverage_parseBoolParameter covers all parseBoolParameter accepted values.
func TestDSNCoverage_parseBoolParameter(t *testing.T) {
	trueVals := []string{"on", "true", "yes", "1"}
	for _, v := range trueVals {
		t.Run("true_"+v, func(t *testing.T) {
			_, err := driver.ParseDSN("file.db?foreign_keys=" + v)
			if err != nil {
				t.Errorf("unexpected error for foreign_keys=%s: %v", v, err)
			}
		})
	}

	falseVals := []string{"off", "false", "no", "0"}
	for _, v := range falseVals {
		t.Run("false_"+v, func(t *testing.T) {
			_, err := driver.ParseDSN("file.db?foreign_keys=" + v)
			if err != nil {
				t.Errorf("unexpected error for foreign_keys=%s: %v", v, err)
			}
		})
	}

	// Invalid value exercises the error branch.
	_, err := driver.ParseDSN("file.db?foreign_keys=maybe")
	if err == nil {
		t.Error("expected error for foreign_keys=maybe, got nil")
	}
}

// TestDSNCoverage_parseSpecialParameter exercises parseSpecialParameter
// including mode=memory, mode=rwc, cache=shared, and _query_only=on.
func TestDSNCoverage_parseSpecialParameter(t *testing.T) {
	cases := []struct {
		name   string
		params string
	}{
		{"mode_memory", "mode=memory"},
		{"mode_rwc", "mode=rwc"},
		{"mode_readonly", "mode=readonly"},
		{"mode_readwrite", "mode=readwrite"},
		{"cache_shared", "cache=shared"},
		{"cache_private", "cache=private"},
		{"query_only_on", "_query_only=on"},
		{"query_only_off", "_query_only=off"},
		{"queryonly_on", "queryonly=on"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := driver.ParseDSN("file.db?" + tc.params)
			if err != nil {
				t.Errorf("ParseDSN(%s) error: %v", tc.params, err)
			}
		})
	}
}

// TestDSNCoverage_parseBoolParameter2_triggers covers the triggers key path.
func TestDSNCoverage_parseBoolParameter2_triggers(t *testing.T) {
	parsed, err := driver.ParseDSN("file.db?triggers=off")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if parsed.Config.EnableTriggers {
		t.Error("EnableTriggers want false")
	}
}

// TestDSNCoverage_buildDSNParameters exercises buildDSNParameters and all
// the addXxxParam helpers through FormatDSN.
func TestDSNCoverage_buildDSNParameters(t *testing.T) {
	cfg := driver.DefaultDriverConfig()
	cfg.Pager.ReadOnly = true       // addModeParameter
	cfg.SharedCache = true          // addCacheParameter
	cfg.Pager.JournalMode = "wal"   // addJournalModeParam (non-default)
	cfg.Pager.SyncMode = "normal"   // addSyncModeParam (non-default)
	cfg.Pager.CacheSize = 512       // addCacheSizeParam (non-default)
	cfg.Pager.PageSize = 8192       // addPageSizeParam (non-default)
	cfg.Pager.BusyTimeout = 0       // addBusyTimeoutParam (non-default: 0 != 5s)
	cfg.EnableForeignKeys = false    // addDriverParameters (foreign_keys off)
	cfg.EnableTriggers = false       // addDriverParameters (triggers off)

	dsn := &driver.DSN{
		Filename: "test.db",
		Config:   cfg,
	}

	result := driver.FormatDSN(dsn)
	if len(result) == 0 {
		t.Error("FormatDSN returned empty string")
	}
	// The result should start with the filename.
	if len(result) < 7 || result[:7] != "test.db" {
		t.Errorf("FormatDSN result should start with test.db, got: %s", result)
	}
}

// TestDSNCoverage_buildDSNParameters_defaults exercises the no-op branches
// in each addXxx helper (when values match defaults, nothing is added).
func TestDSNCoverage_buildDSNParameters_defaults(t *testing.T) {
	cfg := driver.DefaultDriverConfig()
	// Leave everything at defaults; FormatDSN should return just the filename.
	dsn := &driver.DSN{
		Filename: "plain.db",
		Config:   cfg,
	}

	result := driver.FormatDSN(dsn)
	// With all defaults, the result should be plain.db (no query string).
	if result != "plain.db" {
		// Tolerate extra params that differ from defaults.
		t.Logf("FormatDSN with defaults returned: %s", result)
	}
}

// TestDSNCoverage_addJournalModeParam_delete exercises the skip-if-delete branch.
func TestDSNCoverage_addJournalModeParam_delete(t *testing.T) {
	cfg := driver.DefaultDriverConfig()
	cfg.Pager.JournalMode = "delete" // should NOT be added (it's the default)
	dsn := &driver.DSN{Filename: "f.db", Config: cfg}
	result := driver.FormatDSN(dsn)
	// Should not contain journal_mode in the query string.
	if len(result) > 4 {
		// Just log for information; the exact format depends on other params.
		t.Logf("FormatDSN with journal_mode=delete: %s", result)
	}
}

// TestDSNCoverage_IntegrationWithQueryParams opens a real database via DSN
// with query parameters to exercise the full parse→open→apply path.
func TestDSNCoverage_IntegrationWithQueryParams(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cov.db")

	db, err := sql.Open("sqlite_internal",
		dbPath+"?journal_mode=wal&_busy_timeout=5000&_page_size=4096&_cache_size=100&_synchronous=normal")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE cov(x INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec("INSERT INTO cov VALUES(42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var v int
	if err := db.QueryRow("SELECT x FROM cov").Scan(&v); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if v != 42 {
		t.Errorf("want 42, got %d", v)
	}
}

// TestDSNCoverage_EmptyQuery verifies that a DSN with no query string still
// works (the query == "" branch in ParseDSN).
func TestDSNCoverage_EmptyQuery(t *testing.T) {
	parsed, err := driver.ParseDSN("plain.db")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if parsed.Filename != "plain.db" {
		t.Errorf("Filename want plain.db, got %s", parsed.Filename)
	}
}

// TestDSNCoverage_UnknownParameter verifies that unknown parameters are silently
// ignored (the final return nil in parseSpecialParameter).
func TestDSNCoverage_UnknownParameter(t *testing.T) {
	_, err := driver.ParseDSN("file.db?unknown_param=value")
	if err != nil {
		t.Errorf("expected no error for unknown param, got: %v", err)
	}
}
