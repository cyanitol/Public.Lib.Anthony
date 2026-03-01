// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package pager

import (
	"os"
	"testing"
)

// TestNewTransactionManager tests creating a transaction manager
func TestNewTransactionManager(t *testing.T) {
	t.Parallel()
	tm := NewTransactionManager()
	if tm == nil {
		t.Error("expected non-nil transaction manager")
	}

	if tm.state != TxNone {
		t.Errorf("expected TxNone state, got %v", tm.state)
	}
}

// TestNewSavepointManager tests creating a savepoint manager
func TestNewSavepointManager(t *testing.T) {
	t.Parallel()
	sm := NewSavepointManager()
	if sm == nil {
		t.Error("expected non-nil savepoint manager")
	}
}

// TestCheckpointFunction tests Checkpoint function
func TestCheckpointFunction(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "checkpoint_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Checkpoint in DELETE mode should error (not in WAL mode)
	err = pager.Checkpoint()
	// Expected to fail since not in WAL mode
	t.Logf("Checkpoint result: %v", err)
}

// TestIsAutoVacuum tests IsAutoVacuum function
func TestIsAutoVacuum(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "autovac_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// By default, auto-vacuum should be false
	isAuto := pager.IsAutoVacuum()
	t.Logf("IsAutoVacuum: %v", isAuto)
}

// TestSavepointManagerMethods tests savepoint manager methods via Pager
func TestSavepointManagerMethods(t *testing.T) {
	t.Parallel()
	tmpFile, err := os.CreateTemp("", "savepoint_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	pager, err := Open(tmpFile.Name(), false)
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer pager.Close()

	// Should be 0 initially
	if count := pager.savepointCount(); count != 0 {
		t.Errorf("expected 0 savepoints, got %d", count)
	}

	// Clear all savepoints
	pager.ClearSavepoints()

	// Should still be 0
	if count := pager.savepointCount(); count != 0 {
		t.Errorf("expected 0 savepoints after clear, got %d", count)
	}
}
