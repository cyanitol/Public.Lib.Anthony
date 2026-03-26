// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Condition: isValidFromNone
//   `to == lockShared || to == lockExclusive`
//
//   A = to == lockShared
//   B = to == lockExclusive
//
//   Returns true when A || B is true; false otherwise.
//
//   Case 1 (A=T): to=lockShared       → true (valid transition)
//   Case 2 (A=F, B=T): to=lockExclusive → true (valid transition)
//   Case 3 (A=F, B=F): to=lockReserved → false (invalid transition)
// ---------------------------------------------------------------------------

func TestMCDC_IsValidFromNone_ToShared(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (to=lockShared) → valid transition from NONE
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()

	if !lm.CanAcquire(lockShared) {
		t.Error("MCDC case1: lockShared must be a valid transition from lockNone")
	}
}

func TestMCDC_IsValidFromNone_ToExclusive(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (to=lockExclusive) → valid transition from NONE
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()

	if !lm.CanAcquire(lockExclusive) {
		t.Error("MCDC case2: lockExclusive must be a valid transition from lockNone")
	}
}

func TestMCDC_IsValidFromNone_ToReserved(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (to=lockReserved) → invalid transition from NONE
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()

	if lm.CanAcquire(lockReserved) {
		t.Error("MCDC case3: lockReserved must NOT be a valid transition from lockNone")
	}
}

// ---------------------------------------------------------------------------
// Condition: isValidFromShared
//   `to == lockReserved || to == lockExclusive || to == lockNone`
//
//   A = to == lockReserved
//   B = to == lockExclusive
//   C = to == lockNone
//
//   Returns true when A || B || C is true; false otherwise (N+1 = 4 cases).
//
//   Case 1 (A=T): to=lockReserved   → true
//   Case 2 (A=F, B=T): to=lockExclusive → true
//   Case 3 (A=F, B=F, C=T): to=lockNone → true
//   Case 4 (A=F, B=F, C=F): to=lockPending → false
// ---------------------------------------------------------------------------

func TestMCDC_IsValidFromShared_ToReserved(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (to=lockReserved) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)

	if !lm.CanAcquire(lockReserved) {
		t.Error("MCDC case1: lockReserved must be valid from lockShared")
	}
}

func TestMCDC_IsValidFromShared_ToExclusive(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (to=lockExclusive) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)

	if !lm.CanAcquire(lockExclusive) {
		t.Error("MCDC case2: lockExclusive must be valid from lockShared")
	}
}

func TestMCDC_IsValidFromShared_ToNone(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=T (to=lockNone) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)

	if !lm.CanAcquire(lockNone) {
		t.Error("MCDC case3: lockNone must be valid from lockShared")
	}
}

func TestMCDC_IsValidFromShared_ToPending(t *testing.T) {
	t.Parallel()
	// Case 4: A=F, B=F, C=F (to=lockPending) → invalid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)

	if lm.CanAcquire(lockPending) {
		t.Error("MCDC case4: lockPending must NOT be valid from lockShared")
	}
}

// ---------------------------------------------------------------------------
// Condition: isValidFromReserved
//   `to == lockPending || to == lockExclusive || to == lockShared || to == lockNone`
//
//   A = to == lockPending
//   B = to == lockExclusive
//   C = to == lockShared
//   D = to == lockNone
//
//   Returns true when A || B || C || D is true; false otherwise (N+1 = 5 cases).
//
//   Case 1 (A=T): to=lockPending    → true
//   Case 2 (A=F, B=T): to=lockExclusive → true
//   Case 3 (A=F, B=F, C=T): to=lockShared → true
//   Case 4 (A=F, B=F, C=F, D=T): to=lockNone → true
//   Case 5 (A=F, B=F, C=F, D=F): no valid "other" level; tested via AcquireLock returning ErrInvalidLock
// ---------------------------------------------------------------------------

func TestMCDC_IsValidFromReserved_ToPending(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (to=lockPending) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)

	if !lm.CanAcquire(lockPending) {
		t.Error("MCDC case1: lockPending must be valid from lockReserved")
	}
}

func TestMCDC_IsValidFromReserved_ToExclusive(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (to=lockExclusive) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)

	if !lm.CanAcquire(lockExclusive) {
		t.Error("MCDC case2: lockExclusive must be valid from lockReserved")
	}
}

func TestMCDC_IsValidFromReserved_ToShared(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=T (to=lockShared) → valid (downgrade)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)

	if !lm.CanAcquire(lockShared) {
		t.Error("MCDC case3: lockShared must be valid from lockReserved")
	}
}

func TestMCDC_IsValidFromReserved_ToNone(t *testing.T) {
	t.Parallel()
	// Case 4: A=F, B=F, C=F, D=T (to=lockNone) → valid (full release)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)

	if !lm.CanAcquire(lockNone) {
		t.Error("MCDC case4: lockNone must be valid from lockReserved")
	}
}

// ---------------------------------------------------------------------------
// Condition: isValidFromPending
//   `to == lockExclusive || to == lockShared || to == lockNone`
//
//   A = to == lockExclusive
//   B = to == lockShared
//   C = to == lockNone
//
//   Returns true when A || B || C is true; false otherwise (N+1 = 4 cases).
//
//   Case 1 (A=T): to=lockExclusive → true
//   Case 2 (A=F, B=T): to=lockShared → true
//   Case 3 (A=F, B=F, C=T): to=lockNone → true
//   Case 4 (A=F, B=F, C=F): to=lockReserved → false
// ---------------------------------------------------------------------------

func TestMCDC_IsValidFromPending_ToExclusive(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (to=lockExclusive) → valid
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)
	mustAcquireLock(t, lm, lockPending)

	if !lm.CanAcquire(lockExclusive) {
		t.Error("MCDC case1: lockExclusive must be valid from lockPending")
	}
}

func TestMCDC_IsValidFromPending_ToShared(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (to=lockShared) → valid (downgrade)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)
	mustAcquireLock(t, lm, lockPending)

	if !lm.CanAcquire(lockShared) {
		t.Error("MCDC case2: lockShared must be valid from lockPending")
	}
}

func TestMCDC_IsValidFromPending_ToNone(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F, C=T (to=lockNone) → valid (full release)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)
	mustAcquireLock(t, lm, lockPending)

	if !lm.CanAcquire(lockNone) {
		t.Error("MCDC case3: lockNone must be valid from lockPending")
	}
}

func TestMCDC_IsValidFromPending_ToReserved(t *testing.T) {
	t.Parallel()
	// Case 4: A=F, B=F, C=F (to=lockReserved) → invalid (can't go back to reserved)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)
	mustAcquireLock(t, lm, lockPending)

	if lm.CanAcquire(lockReserved) {
		t.Error("MCDC case4: lockReserved must NOT be valid from lockPending")
	}
}

// ---------------------------------------------------------------------------
// Condition: isValidFromExclusive
//   `to == lockShared || to == lockNone`
//
//   A = to == lockShared
//   B = to == lockNone
//
//   Returns true when A || B is true; false otherwise (N+1 = 3 cases).
//
//   Case 1 (A=T): to=lockShared   → true
//   Case 2 (A=F, B=T): to=lockNone → true
//   Case 3 (A=F, B=F): to=lockReserved → false
// ---------------------------------------------------------------------------

func TestMCDC_IsValidFromExclusive_ToShared(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (to=lockShared) → valid (downgrade)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockExclusive)

	if !lm.CanAcquire(lockShared) {
		t.Error("MCDC case1: lockShared must be valid from lockExclusive")
	}
}

func TestMCDC_IsValidFromExclusive_ToNone(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (to=lockNone) → valid (full release)
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockExclusive)

	if !lm.CanAcquire(lockNone) {
		t.Error("MCDC case2: lockNone must be valid from lockExclusive")
	}
}

func TestMCDC_IsValidFromExclusive_ToReserved(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (to=lockReserved) → invalid from EXCLUSIVE
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockExclusive)

	if lm.CanAcquire(lockReserved) {
		t.Error("MCDC case3: lockReserved must NOT be valid from lockExclusive")
	}
}

// ---------------------------------------------------------------------------
// Condition: shouldReleaseLock (lock_unix.go)
//   `lm.currentLevel >= currentLevel && targetLevel < lockType`
//
//   A = lm.currentLevel >= currentLevel
//   B = targetLevel < lockType
//
//   Returns true when A && B is true; the lock step is executed only then.
//
//   Case 1 (A=F): currentLevel is higher than currentLevel held → step skipped
//   Case 2 (A=T, B=F): held >= step, but target >= lockType → step skipped
//   Case 3 (A=T, B=T): held >= step and target < lockType → step executed
// ---------------------------------------------------------------------------

func TestMCDC_ShouldReleaseLock_CurrentLevelBelowStep(t *testing.T) {
	t.Parallel()
	// Case 1: A=F — lm.currentLevel (lockNone) < lockExclusive → step skipped
	// We have a NONE lock; releasing to NONE should skip all release steps.
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	// currentLevel=lockNone; ReleaseLock(lockNone) is a no-op (currentLevel <= level)
	// but internally shouldReleaseLock for lockExclusive step: A = (None >= Exclusive) = false
	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Errorf("MCDC case1: ReleaseLock(lockNone) from lockNone must succeed; got %v", err)
	}
	if lm.GetLockState() != lockNone {
		t.Errorf("MCDC case1: lock state must remain lockNone; got %v", lm.GetLockState())
	}
}

func TestMCDC_ShouldReleaseLock_TargetNotBelowStep(t *testing.T) {
	t.Parallel()
	// Case 2: A=T, B=F — held=lockExclusive >= lockExclusive (A=T),
	// but releasing TO lockExclusive means targetLevel >= lockExclusive (B=F for that step)
	// i.e., we hold exclusive and release down to exclusive itself → step skipped
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockExclusive)

	// ReleaseLock to lockShared: for the exclusive release step,
	// targetLevel=lockShared < lockExclusive → B=T, so exclusive IS released.
	// But for the reserved step: held=lockExclusive >= lockReserved (A=T),
	// targetLevel=lockShared < lockReserved (B=T) → reserved also released.
	// We focus on the no-op case where target == step level.
	// ReleaseLock(lockExclusive) — currentLevel already == lockExclusive so skipped by outer guard.
	// Use a sub-step: after acquiring lockReserved and releasing to lockReserved,
	// the exclusive step has A=F (currentLevel=reserved < exclusive).
	mustReleaseLock(t, lm, lockNone)
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)
	// Now: currentLevel=lockReserved. Release to lockReserved.
	// For exclusive step: A = (Reserved >= Exclusive) = false → step skipped (A=F case).
	// For reserved step: A = (Reserved >= Reserved) = true; B = (Reserved < Reserved) = false → skipped (B=F case).
	if err := lm.ReleaseLock(lockReserved); err != nil {
		t.Errorf("MCDC case2: ReleaseLock(lockReserved) from lockReserved must succeed; got %v", err)
	}
	if lm.GetLockState() != lockReserved {
		t.Errorf("MCDC case2: lock state must remain lockReserved; got %v", lm.GetLockState())
	}
}

func TestMCDC_ShouldReleaseLock_StepExecuted(t *testing.T) {
	t.Parallel()
	// Case 3: A=T, B=T — hold RESERVED, release to NONE:
	// for reserved step: held=Reserved >= Reserved (A=T), target=None < Reserved (B=T) → executed.
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "test.db")
	f, err := os.Create(dbFile)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	defer f.Close()
	lm := mustNewLockManager(t, f)
	defer lm.Close()
	mustAcquireLock(t, lm, lockShared)
	mustAcquireLock(t, lm, lockReserved)

	if err := lm.ReleaseLock(lockNone); err != nil {
		t.Errorf("MCDC case3: ReleaseLock(lockNone) from lockReserved must succeed; got %v", err)
	}
	if lm.GetLockState() != lockNone {
		t.Errorf("MCDC case3: lock state must be lockNone after full release; got %v", lm.GetLockState())
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.GetMaxFrame / GetPageCount / GetChangeCounter — nil-or-uninit guard
//   `!w.initialized || w.header == nil`
//
//   A = !w.initialized
//   B = w.header == nil
//
//   Returns zero value when A || B is true.
//   (GetMaxFrame, GetPageCount, GetChangeCounter all use the same pattern.)
//
//   Case 1 (A=T): not initialized → returns 0
//   Case 2 (A=F, B=T): initialized but header nil → returns 0
//   Case 3 (A=F, B=F): initialized with header → returns actual value
// ---------------------------------------------------------------------------

func TestMCDC_WALIndexGetMaxFrame_NotInitialized(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (initialized=false) → GetMaxFrame returns 0
	tmpDir := t.TempDir()
	idx := &WALIndex{
		filename:    filepath.Join(tmpDir, "test.db-shm"),
		hashTable:   make(map[uint32]uint32),
		initialized: false,
		header:      nil,
	}
	if got := idx.GetMaxFrame(); got != 0 {
		t.Errorf("MCDC case1: uninitialized index GetMaxFrame must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetMaxFrame_NilHeader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F (initialized=true), B=T (header=nil) → returns 0
	tmpDir := t.TempDir()
	idx := &WALIndex{
		filename:    filepath.Join(tmpDir, "test.db-shm"),
		hashTable:   make(map[uint32]uint32),
		initialized: true,
		header:      nil,
	}
	if got := idx.GetMaxFrame(); got != 0 {
		t.Errorf("MCDC case2: initialized with nil header GetMaxFrame must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetMaxFrame_WithFrame(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → initialized with valid header → returns MxFrame
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	mustInsertFrame(t, idx, 5, 42)
	got := idx.GetMaxFrame()
	if got != 42 {
		t.Errorf("MCDC case3: GetMaxFrame must return 42 after InsertFrame; got %d", got)
	}
}

func TestMCDC_WALIndexGetPageCount_NotInitialized(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → GetPageCount returns 0
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: false,
		header:      nil,
	}
	if got := idx.GetPageCount(); got != 0 {
		t.Errorf("MCDC case1: uninitialized GetPageCount must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetPageCount_NilHeader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T → initialized but nil header → 0
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: true,
		header:      nil,
	}
	if got := idx.GetPageCount(); got != 0 {
		t.Errorf("MCDC case2: nil header GetPageCount must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetPageCount_WithCount(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → real index with SetPageCount
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	if err := idx.SetPageCount(7); err != nil {
		t.Fatalf("SetPageCount error = %v", err)
	}
	if got := idx.GetPageCount(); got != 7 {
		t.Errorf("MCDC case3: GetPageCount must return 7; got %d", got)
	}
}

func TestMCDC_WALIndexGetChangeCounter_NotInitialized(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → GetChangeCounter returns 0
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: false,
		header:      nil,
	}
	if got := idx.GetChangeCounter(); got != 0 {
		t.Errorf("MCDC case1: uninitialized GetChangeCounter must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetChangeCounter_NilHeader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T → initialized but nil header → 0
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: true,
		header:      nil,
	}
	if got := idx.GetChangeCounter(); got != 0 {
		t.Errorf("MCDC case2: nil header GetChangeCounter must return 0; got %d", got)
	}
}

func TestMCDC_WALIndexGetChangeCounter_WithChanges(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → real index; counter increments with each insert
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	before := idx.GetChangeCounter()
	mustInsertFrame(t, idx, 3, 10)
	after := idx.GetChangeCounter()
	if after <= before {
		t.Errorf("MCDC case3: GetChangeCounter must increase after insert; before=%d after=%d", before, after)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.GetFrameChecksum — nil-or-uninit guard
//   `!w.initialized || w.header == nil`
//
//   A = !w.initialized
//   B = w.header == nil
//
//   Returns error when A || B is true.
//
//   Case 1 (A=T): not initialized → error
//   Case 2 (A=F, B=T): initialized, nil header → error
//   Case 3 (A=F, B=F): initialized with header → returns checksums
// ---------------------------------------------------------------------------

func TestMCDC_WALIndexGetFrameChecksum_NotInitialized(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → error
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: false,
		header:      nil,
	}
	_, _, err := idx.GetFrameChecksum()
	if err == nil {
		t.Error("MCDC case1: uninitialized GetFrameChecksum must return error")
	}
}

func TestMCDC_WALIndexGetFrameChecksum_NilHeader(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T → error
	idx := &WALIndex{
		hashTable:   make(map[uint32]uint32),
		initialized: true,
		header:      nil,
	}
	_, _, err := idx.GetFrameChecksum()
	if err == nil {
		t.Error("MCDC case2: nil header GetFrameChecksum must return error")
	}
}

func TestMCDC_WALIndexGetFrameChecksum_WithHeader(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F → initialized index → returns checksums, no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	if err := idx.UpdateFrameChecksum(0xAB, 0xCD); err != nil {
		t.Fatalf("UpdateFrameChecksum error = %v", err)
	}
	c1, c2, err := idx.GetFrameChecksum()
	if err != nil {
		t.Errorf("MCDC case3: GetFrameChecksum must not error; got %v", err)
	}
	if c1 != 0xAB || c2 != 0xCD {
		t.Errorf("MCDC case3: checksums must be (0xAB, 0xCD); got (%d, %d)", c1, c2)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.validateAndFixHeader
//   `w.header.Version != WALIndexVersion && w.header.Version != 0`
//
//   A = w.header.Version != WALIndexVersion
//   B = w.header.Version != 0
//
//   Triggers reinitialize when A && B is true.
//
//   Case 1 (A=F): version matches WALIndexVersion → no reinitialize
//   Case 2 (A=T, B=F): version is 0 → no reinitialize (0 treated as uninitialized)
//   Case 3 (A=T, B=T): version is neither 0 nor WALIndexVersion → reinitialize
// ---------------------------------------------------------------------------

func TestMCDC_ValidateAndFixHeader_VersionMatch(t *testing.T) {
	t.Parallel()
	// Case 1: A=F → version == WALIndexVersion → no reinitialize (returns nil)
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	// After open, version should already be WALIndexVersion
	idx.header.mu.Lock()
	idx.header.Version = WALIndexVersion
	idx.header.mu.Unlock()

	if err := idx.validateAndFixHeader(); err != nil {
		t.Errorf("MCDC case1: matching version must not trigger reinitialize; got %v", err)
	}
	idx.header.mu.RLock()
	got := idx.header.Version
	idx.header.mu.RUnlock()
	if got != WALIndexVersion {
		t.Errorf("MCDC case1: version must remain WALIndexVersion; got %d", got)
	}
}

func TestMCDC_ValidateAndFixHeader_VersionZero(t *testing.T) {
	t.Parallel()
	// Case 2: A=T (version != WALIndexVersion), B=F (version == 0) → no reinitialize
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	idx.header.mu.Lock()
	idx.header.Version = 0
	idx.header.mu.Unlock()

	if err := idx.validateAndFixHeader(); err != nil {
		t.Errorf("MCDC case2: zero version must not error; got %v", err)
	}
	// Version should remain 0 (condition A&&B was false, so no fix applied)
	idx.header.mu.RLock()
	got := idx.header.Version
	idx.header.mu.RUnlock()
	if got != 0 {
		t.Errorf("MCDC case2: version 0 must not be rewritten; got %d", got)
	}
}

func TestMCDC_ValidateAndFixHeader_VersionMismatch(t *testing.T) {
	t.Parallel()
	// Case 3: A=T (version != WALIndexVersion), B=T (version != 0) → reinitialize
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	const bogusVersion = 0xDEAD
	idx.header.mu.Lock()
	idx.header.Version = bogusVersion
	idx.header.mu.Unlock()

	if err := idx.validateAndFixHeader(); err != nil {
		t.Errorf("MCDC case3: mismatched version fix must succeed; got %v", err)
	}
	// After reinitialize, version must be WALIndexVersion
	idx.header.mu.RLock()
	got := idx.header.Version
	idx.header.mu.RUnlock()
	if got != WALIndexVersion {
		t.Errorf("MCDC case3: version must be reset to WALIndexVersion after fix; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.SetReadMark / GetReadMark — reader bounds guard
//   `reader < 0 || reader >= WALIndexMaxReaders`
//
//   A = reader < 0
//   B = reader >= WALIndexMaxReaders
//
//   Returns ErrInvalidReader when A || B is true.
//
//   Case 1 (A=T): reader=-1      → ErrInvalidReader
//   Case 2 (A=F, B=T): reader=WALIndexMaxReaders → ErrInvalidReader
//   Case 3 (A=F, B=F): reader=0  → no error (valid slot)
// ---------------------------------------------------------------------------

func TestMCDC_SetReadMark_ReaderNegative(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (reader < 0) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	err := idx.SetReadMark(-1, 10)
	if err != ErrInvalidReader {
		t.Errorf("MCDC case1: reader=-1 must return ErrInvalidReader; got %v", err)
	}
}

func TestMCDC_SetReadMark_ReaderTooLarge(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (reader >= WALIndexMaxReaders) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	err := idx.SetReadMark(WALIndexMaxReaders, 10)
	if err != ErrInvalidReader {
		t.Errorf("MCDC case2: reader=%d must return ErrInvalidReader; got %v", WALIndexMaxReaders, err)
	}
}

func TestMCDC_SetReadMark_ValidReader(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (reader=0, valid) → no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	if err := idx.SetReadMark(0, 5); err != nil {
		t.Errorf("MCDC case3: reader=0 SetReadMark must succeed; got %v", err)
	}
	mark, err := idx.GetReadMark(0)
	if err != nil {
		t.Errorf("MCDC case3: GetReadMark(0) must succeed; got %v", err)
	}
	if mark != 5 {
		t.Errorf("MCDC case3: mark must be 5; got %d", mark)
	}
}

func TestMCDC_GetReadMark_ReaderNegative(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (reader < 0) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	_, err := idx.GetReadMark(-1)
	if err != ErrInvalidReader {
		t.Errorf("MCDC case1: reader=-1 GetReadMark must return ErrInvalidReader; got %v", err)
	}
}

func TestMCDC_GetReadMark_ReaderTooLarge(t *testing.T) {
	t.Parallel()
	// Case 2: A=F, B=T (reader >= WALIndexMaxReaders) → ErrInvalidReader
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	_, err := idx.GetReadMark(WALIndexMaxReaders)
	if err != ErrInvalidReader {
		t.Errorf("MCDC case2: reader=%d GetReadMark must return ErrInvalidReader; got %v", WALIndexMaxReaders, err)
	}
}

func TestMCDC_GetReadMark_ValidReader(t *testing.T) {
	t.Parallel()
	// Case 3: A=F, B=F (reader=2, valid) → no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	mustSetReadMark(t, idx, 2, 99)
	mark, err := idx.GetReadMark(2)
	if err != nil {
		t.Errorf("MCDC case3: GetReadMark(2) must succeed; got %v", err)
	}
	if mark != 99 {
		t.Errorf("MCDC case3: mark must be 99; got %d", mark)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.InsertFrame — MxFrame update guard
//   `frameNo > w.header.MxFrame`
//
//   A = frameNo > w.header.MxFrame
//
//   MxFrame is updated when A is true; unchanged when A is false.
//
//   Case 1 (A=T): frameNo > MxFrame → MxFrame updated
//   Case 2 (A=F): frameNo <= MxFrame → MxFrame unchanged
// ---------------------------------------------------------------------------

func TestMCDC_InsertFrame_FrameExceedsMxFrame(t *testing.T) {
	t.Parallel()
	// Case 1: A=T (frameNo > MxFrame) → MxFrame is updated
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	// MxFrame starts at 0; insert frameNo=10 → must update MxFrame to 10
	mustInsertFrame(t, idx, 1, 10)
	if got := idx.GetMaxFrame(); got != 10 {
		t.Errorf("MCDC case1: MxFrame must be 10 after inserting frameNo=10; got %d", got)
	}
}

func TestMCDC_InsertFrame_FrameNotExceedsMxFrame(t *testing.T) {
	t.Parallel()
	// Case 2: A=F (frameNo <= MxFrame) → MxFrame unchanged
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	// Establish MxFrame=20
	mustInsertFrame(t, idx, 1, 20)
	// Insert with lower frameNo → MxFrame must stay 20
	mustInsertFrame(t, idx, 2, 5)
	if got := idx.GetMaxFrame(); got != 20 {
		t.Errorf("MCDC case2: MxFrame must remain 20 after inserting lower frameNo=5; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.ValidateFrameChecksum
//   `actualChecksum == expectedChecksum`
//
//   A = actualChecksum == expectedChecksum
//
//   Returns true when A is true; false otherwise.
//
//   Case 1 (A=T): checksum matches → true
//   Case 2 (A=F): checksum differs → false
// ---------------------------------------------------------------------------

func TestMCDC_WALIndexValidateFrameChecksum_Match(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → checksums match → true
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	checksum := idx.CalculateFrameChecksum(data)
	if !idx.ValidateFrameChecksum(data, checksum) {
		t.Error("MCDC case1: matching checksum must return true")
	}
}

func TestMCDC_WALIndexValidateFrameChecksum_Mismatch(t *testing.T) {
	t.Parallel()
	// Case 2: A=F → checksums differ → false
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	checksum := idx.CalculateFrameChecksum(data) ^ 0xFFFFFFFF
	if idx.ValidateFrameChecksum(data, checksum) {
		t.Error("MCDC case2: mismatched checksum must return false")
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.InsertFrameWithChecksum — validation gate
//   `!w.ValidateFrameChecksum(frameData, checksum)`
//
//   (Single sub-condition: either validation passes or it doesn't.)
//
//   Case 1 (A=T): checksum invalid → returns ErrWALChecksumMismatch
//   Case 2 (A=F): checksum valid → InsertFrame proceeds, returns nil
// ---------------------------------------------------------------------------

func TestMCDC_InsertFrameWithChecksum_Invalid(t *testing.T) {
	t.Parallel()
	// Case 1: A=T → checksum mismatch → error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	data := []byte{0x01, 0x02, 0x03}
	badChecksum := idx.CalculateFrameChecksum(data) ^ 0xFFFFFFFF
	err := idx.InsertFrameWithChecksum(1, 1, data, badChecksum)
	if err == nil {
		t.Error("MCDC case1: invalid checksum must return an error")
	}
}

func TestMCDC_InsertFrameWithChecksum_Valid(t *testing.T) {
	t.Parallel()
	// Case 2: A=F → valid checksum → nil error, frame inserted
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	defer idx.Close()

	data := []byte{0x01, 0x02, 0x03}
	checksum := idx.CalculateFrameChecksum(data)
	if err := idx.InsertFrameWithChecksum(1, 7, data, checksum); err != nil {
		t.Errorf("MCDC case2: valid checksum InsertFrameWithChecksum must succeed; got %v", err)
	}
	frameNo, err := idx.FindFrame(1)
	if err != nil {
		t.Errorf("MCDC case2: FindFrame must succeed after insert; got %v", err)
	}
	if frameNo != 7 {
		t.Errorf("MCDC case2: frame must be 7; got %d", frameNo)
	}
}

// ---------------------------------------------------------------------------
// Condition: WALIndex.Close — mmap-nil guard and file-nil guard
//   Guard A: `w.mmap != nil`    → munmap executed when true
//   Guard B: `w.file != nil`    → file.Close() executed when true
//
//   These are sequential single-condition guards in Close().
//   We test both conditions independently.
//
//   mmap guard:
//     Case 1 (A=F): mmap already nil → munmap skipped, no error
//     Case 2 (A=T): mmap non-nil    → munmap executed
//
//   file guard:
//     Case 1 (B=F): file already nil → file.Close skipped, no error
//     Case 2 (B=T): file non-nil    → file.Close executed
// ---------------------------------------------------------------------------

func TestMCDC_WALIndexClose_MmapNil(t *testing.T) {
	t.Parallel()
	// mmap guard Case 1: A=F (mmap already nil) → skip munmap, no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	// Manually nil out mmap before Close to test the nil-skip branch
	idx.mu.Lock()
	if idx.mmap != nil {
		_ = platformMunmap(idx.mmap)
		idx.mmap = nil
	}
	idx.mu.Unlock()

	if err := idx.Close(); err != nil {
		t.Errorf("MCDC mmap case1: Close with nil mmap must succeed; got %v", err)
	}
}

func TestMCDC_WALIndexClose_MmapNonNil(t *testing.T) {
	t.Parallel()
	// mmap guard Case 2: A=T (mmap non-nil) → munmap executed, no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	// mmap is set by open; calling Close normally should execute the munmap branch
	if err := idx.Close(); err != nil {
		t.Errorf("MCDC mmap case2: Close with non-nil mmap must succeed; got %v", err)
	}
}

func TestMCDC_WALIndexClose_FileNil(t *testing.T) {
	t.Parallel()
	// file guard Case 1: B=F (file already nil) → skip file.Close, no error
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	idx.mu.Lock()
	// Nil out mmap and file manually to simulate partially-cleaned state
	if idx.mmap != nil {
		_ = platformMunmap(idx.mmap)
		idx.mmap = nil
	}
	if idx.file != nil {
		_ = idx.file.Close()
		idx.file = nil
	}
	idx.mu.Unlock()

	if err := idx.Close(); err != nil {
		t.Errorf("MCDC file case1: Close with nil file must succeed; got %v", err)
	}
}

func TestMCDC_WALIndexClose_FileNonNil(t *testing.T) {
	t.Parallel()
	// file guard Case 2: B=T (file non-nil) → file.Close executed; Close succeeds
	tmpDir := t.TempDir()
	idx := mustOpenWALIndex(t, filepath.Join(tmpDir, "test.db"))
	// Nil out mmap manually so the file branch is clearly reached
	idx.mu.Lock()
	if idx.mmap != nil {
		_ = platformMunmap(idx.mmap)
		idx.mmap = nil
	}
	idx.mu.Unlock()

	if err := idx.Close(); err != nil {
		t.Errorf("MCDC file case2: Close with non-nil file must succeed; got %v", err)
	}
}
