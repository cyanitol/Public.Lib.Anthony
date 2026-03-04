package driver

import (
	"testing"
)

func TestApplyPragmas(t *testing.T) {
	config := DefaultDriverConfig()
	config.EnableForeignKeys = true
	config.CaseSensitiveLike = true
	config.RecursiveTriggers = true
	config.AutoVacuum = "full"
	config.Pager.JournalMode = "WAL"
	config.Pager.SyncMode = "NORMAL"
	config.Pager.CacheSize = -2000
	config.Pager.LockingMode = "NORMAL"
	config.Pager.TempStore = "memory"

	pragmas := config.ApplyPragmas()

	expected := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -2000",
		"PRAGMA locking_mode = NORMAL",
		"PRAGMA auto_vacuum = full",
		"PRAGMA case_sensitive_like = ON",
		"PRAGMA recursive_triggers = ON",
		"PRAGMA temp_store = memory",
	}

	if len(pragmas) != len(expected) {
		t.Fatalf("expected %d pragmas, got %d", len(expected), len(pragmas))
	}

	for i, exp := range expected {
		if pragmas[i] != exp {
			t.Errorf("pragma %d: expected %q, got %q", i, exp, pragmas[i])
		}
	}
}

func TestApplyPragmasDefaults(t *testing.T) {
	config := DefaultDriverConfig()
	pragmas := config.ApplyPragmas()

	// Let's check what we get
	t.Logf("Got %d pragmas:", len(pragmas))
	for i, p := range pragmas {
		t.Logf("  %d: %s", i, p)
	}

	// With defaults, we should get just foreign_keys = ON
	if len(pragmas) == 0 {
		t.Fatal("expected at least one pragma")
	}

	if pragmas[0] != "PRAGMA foreign_keys = ON" {
		t.Errorf("expected first pragma to be foreign_keys = ON, got %q", pragmas[0])
	}
}

func TestApplyPragmasForeignKeysOff(t *testing.T) {
	config := DefaultDriverConfig()
	config.EnableForeignKeys = false
	pragmas := config.ApplyPragmas()

	if len(pragmas) == 0 {
		t.Fatal("expected at least one pragma")
	}

	if pragmas[0] != "PRAGMA foreign_keys = OFF" {
		t.Errorf("expected first pragma to be foreign_keys = OFF, got %q", pragmas[0])
	}
}
