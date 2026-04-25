// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"fmt"
	"testing"
)

// MockPager implements PagerInterface, SavepointPagerInterface, and CookiePagerInterface
// for testing transaction opcodes.
type MockPager struct {
	inTransaction      bool
	inWriteTransaction bool
	savepoints         []string
	cookies            map[string]uint32 // key: "db:type"
}

func NewMockPager() *MockPager {
	return &MockPager{
		savepoints: make([]string, 0),
		cookies:    make(map[string]uint32),
	}
}

// PagerInterface methods
func (m *MockPager) BeginRead() error {
	if m.inTransaction {
		return fmt.Errorf("transaction already active")
	}
	m.inTransaction = true
	m.inWriteTransaction = false
	return nil
}

func (m *MockPager) BeginWrite() error {
	if m.inWriteTransaction {
		return fmt.Errorf("write transaction already active")
	}
	m.inTransaction = true
	m.inWriteTransaction = true
	return nil
}

func (m *MockPager) Commit() error {
	if !m.inWriteTransaction {
		return fmt.Errorf("no write transaction to commit")
	}
	m.inTransaction = false
	m.inWriteTransaction = false
	m.savepoints = nil
	return nil
}

func (m *MockPager) Rollback() error {
	if !m.inWriteTransaction {
		return fmt.Errorf("no write transaction to rollback")
	}
	m.inTransaction = false
	m.inWriteTransaction = false
	m.savepoints = nil
	return nil
}

func (m *MockPager) EndRead() error {
	if !m.inTransaction || m.inWriteTransaction {
		return fmt.Errorf("no read transaction to end")
	}
	m.inTransaction = false
	return nil
}

func (m *MockPager) InTransaction() bool {
	return m.inTransaction
}

func (m *MockPager) InWriteTransaction() bool {
	return m.inWriteTransaction
}

// SavepointPagerInterface methods
func (m *MockPager) Savepoint(name string) error {
	if !m.inWriteTransaction {
		return fmt.Errorf("savepoint requires active write transaction")
	}
	if name == "" {
		return fmt.Errorf("savepoint name cannot be empty")
	}
	for _, sp := range m.savepoints {
		if sp == name {
			return fmt.Errorf("savepoint %s already exists", name)
		}
	}
	m.savepoints = append(m.savepoints, name)
	return nil
}

func (m *MockPager) Release(name string) error {
	if !m.inWriteTransaction {
		return fmt.Errorf("release requires active write transaction")
	}
	for i, sp := range m.savepoints {
		if sp == name {
			m.savepoints = m.savepoints[:i]
			return nil
		}
	}
	return fmt.Errorf("no such savepoint: %s", name)
}

func (m *MockPager) RollbackTo(name string) error {
	if !m.inWriteTransaction {
		return fmt.Errorf("rollback to savepoint requires active write transaction")
	}
	for i, sp := range m.savepoints {
		if sp == name {
			// Keep this savepoint and earlier ones
			m.savepoints = m.savepoints[:i+1]
			return nil
		}
	}
	return fmt.Errorf("no such savepoint: %s", name)
}

// CookiePagerInterface methods
func (m *MockPager) GetCookie(dbIndex int, cookieType int) (uint32, error) {
	key := fmt.Sprintf("%d:%d", dbIndex, cookieType)
	if value, ok := m.cookies[key]; ok {
		return value, nil
	}
	return 0, nil
}

func (m *MockPager) SetCookie(dbIndex int, cookieType int, value uint32) error {
	key := fmt.Sprintf("%d:%d", dbIndex, cookieType)
	m.cookies[key] = value
	return nil
}

// verifyTransactionState checks transaction state after execTransaction.
func verifyTransactionState(t *testing.T, pager *MockPager, p2 int, setupTxn bool, expectError bool) {
	t.Helper()
	if expectError || setupTxn {
		return
	}
	if !pager.InTransaction() {
		t.Error("transaction should be active")
	}
	if p2 != 0 && !pager.InWriteTransaction() {
		t.Error("write transaction should be active")
	}
}

// Test OpTransaction
func TestOpTransaction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		p1, p2, p3  int
		setupTxn    bool
		expectError bool
	}{
		{"read transaction", 0, 0, 0, false, false},
		{"write transaction", 0, 1, 0, false, false},
		{"nested transaction fails", 0, 0, 0, true, true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			pager := NewMockPager()
			v.Ctx = &VDBEContext{Pager: pager}
			if tt.setupTxn {
				pager.BeginRead()
			}
			err := v.execTransaction(&Instruction{Opcode: OpTransaction, P1: tt.p1, P2: tt.p2, P3: tt.p3})
			checkExpectedError(t, err, tt.expectError)
			verifyTransactionState(t, pager, tt.p2, tt.setupTxn, tt.expectError)
		})
	}
}

// verifyAutoCommitState checks the transaction state after autocommit.
func verifyAutoCommitState(t *testing.T, pager *MockPager, p1 int, setupTxn bool, expectError bool) {
	t.Helper()
	if p1 == 0 && !expectError {
		if !pager.InTransaction() {
			t.Error("transaction should be active after begin")
		}
	} else if p1 == 1 && setupTxn && !expectError {
		if pager.InTransaction() {
			t.Error("transaction should not be active after commit/rollback")
		}
	}
}

// Test OpAutoCommit
func TestOpAutoCommit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		p1          int
		p2          int
		setupTxn    bool
		expectError bool
	}{
		{"begin transaction", 0, 0, false, false},
		{"commit transaction", 1, 0, true, false},
		{"rollback transaction", 1, 1, true, false},
		{"commit without transaction", 1, 0, false, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			pager := NewMockPager()
			v.Ctx = &VDBEContext{Pager: pager}
			if tt.setupTxn {
				pager.BeginWrite()
			}
			err := v.execAutoCommit(&Instruction{Opcode: OpAutocommit, P1: tt.p1, P2: tt.p2})
			checkExpectedError(t, err, tt.expectError)
			verifyAutoCommitState(t, pager, tt.p1, tt.setupTxn, tt.expectError)
		})
	}
}

// Test OpSavepoint
func TestOpSavepoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		operation   int // 0=begin, 1=release, 2=rollback
		spName      string
		setupTxn    bool
		setupSp     []string // savepoints to create before test
		expectError bool
		checkSp     []string // expected savepoints after operation
	}{
		{"create savepoint", 0, "sp1", true, nil, false, []string{"sp1"}},
		{"create savepoint no transaction", 0, "sp1", false, nil, true, nil},
		{"create duplicate savepoint", 0, "sp1", true, []string{"sp1"}, true, []string{"sp1"}},
		{"create empty name", 0, "", true, nil, true, nil},
		{"release savepoint", 1, "sp1", true, []string{"sp1", "sp2"}, false, []string{}},
		{"release non-existent", 1, "sp3", true, []string{"sp1"}, true, []string{"sp1"}},
		{"rollback to savepoint", 2, "sp1", true, []string{"sp1", "sp2"}, false, []string{"sp1"}},
		{"rollback to non-existent", 2, "sp3", true, []string{"sp1"}, true, []string{"sp1"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			pager := NewMockPager()
			v.Ctx = &VDBEContext{Pager: pager}
			if tt.setupTxn {
				pager.BeginWrite()
			}
			for _, sp := range tt.setupSp {
				pager.Savepoint(sp)
			}
			err := v.execSavepoint(&Instruction{Opcode: OpSavepoint, P1: tt.operation, P4: P4Union{Z: tt.spName}, P4Type: P4Static})
			checkExpectedError(t, err, tt.expectError)
			verifySavepoints(t, pager, tt.checkSp, tt.expectError)
		})
	}
}

func checkExpectedError(t *testing.T, err error, expectError bool) {
	t.Helper()
	if expectError && err == nil {
		t.Error("expected error but got none")
	}
	if !expectError && err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func verifySavepoints(t *testing.T, pager *MockPager, expected []string, hadError bool) {
	t.Helper()
	if expected == nil || hadError {
		return
	}
	if len(pager.savepoints) != len(expected) {
		t.Errorf("expected %d savepoints, got %d", len(expected), len(pager.savepoints))
	}
	for i, sp := range expected {
		if i >= len(pager.savepoints) {
			break
		}
		if pager.savepoints[i] != sp {
			t.Errorf("savepoint[%d]: expected %s, got %s", i, sp, pager.savepoints[i])
		}
	}
}

// checkErrorMessage checks if an error message contains expected string.
func checkErrorMessage(t *testing.T, err error, expected string) {
	t.Helper()
	if expected != "" && err != nil && !contains(err.Error(), expected) {
		t.Errorf("expected error containing %q, got %q", expected, err.Error())
	}
}

// Test OpVerifyCookie
func TestOpVerifyCookie(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                             string
		dbIndex, cookieType, expectedVal int
		actualVal                        uint32
		expectError                      bool
		errorMessage                     string
	}{
		{"cookie matches", 0, 0, 42, 42, false, ""},
		{"cookie mismatch", 0, 0, 42, 43, true, "schema changed"},
		{"cookie zero", 0, 0, 0, 0, false, ""},
		{"different cookie type", 0, 1, 100, 100, false, ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			pager := NewMockPager()
			v.Ctx = &VDBEContext{Pager: pager}
			pager.SetCookie(tt.dbIndex, tt.cookieType, tt.actualVal)
			err := v.execVerifyCookie(&Instruction{Opcode: OpVerifyCookie, P1: tt.dbIndex, P2: tt.cookieType, P3: tt.expectedVal})
			checkExpectedError(t, err, tt.expectError)
			checkErrorMessage(t, err, tt.errorMessage)
		})
	}
}

// Test OpSetCookie
func TestOpSetCookie(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		dbIndex    int
		cookieType int
		value      int
	}{
		{"set cookie", 0, 0, 42},
		{"set to zero", 0, 0, 0},
		{"set different type", 0, 1, 100},
		{"set large value", 0, 0, 999999},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := New()
			pager := NewMockPager()
			v.Ctx = &VDBEContext{Pager: pager}

			instr := &Instruction{
				Opcode: OpSetCookie,
				P1:     tt.dbIndex,
				P2:     tt.cookieType,
				P3:     tt.value,
			}

			err := v.execSetCookie(instr)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Verify cookie was set
			actual, err := pager.GetCookie(tt.dbIndex, tt.cookieType)
			if err != nil {
				t.Errorf("failed to get cookie: %v", err)
			}
			if actual != uint32(tt.value) {
				t.Errorf("expected cookie value %d, got %d", tt.value, actual)
			}
		})
	}
}

// txnFlowExecSavepoint is a helper that creates a savepoint instruction and executes it.
func txnFlowExecSavepoint(t *testing.T, v *VDBE, op int, name string) {
	t.Helper()
	instr := &Instruction{Opcode: OpSavepoint, P1: op, P4: P4Union{Z: name}, P4Type: P4Static}
	if err := v.execSavepoint(instr); err != nil {
		t.Fatalf("execSavepoint(%d, %s) failed: %v", op, name, err)
	}
}

func txnFlowExpectSavepoints(t *testing.T, pager *MockPager, want int) {
	t.Helper()
	if len(pager.savepoints) != want {
		t.Errorf("expected %d savepoint(s), got %d", want, len(pager.savepoints))
	}
}

// Test transaction flow with multiple opcodes
func TestTransactionFlow(t *testing.T) {
	t.Parallel()
	v := New()
	pager := NewMockPager()
	v.Ctx = &VDBEContext{Pager: pager}

	if err := v.execTransaction(&Instruction{Opcode: OpTransaction, P1: 0, P2: 1, P3: 0}); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if !pager.InWriteTransaction() {
		t.Error("write transaction should be active")
	}

	txnFlowExecSavepoint(t, v, 0, "sp1")
	txnFlowExpectSavepoints(t, pager, 1)

	txnFlowExecSavepoint(t, v, 0, "sp2")
	txnFlowExpectSavepoints(t, pager, 2)

	txnFlowExecSavepoint(t, v, 2, "sp1")
	txnFlowExpectSavepoints(t, pager, 1)

	if err := v.execCommit(&Instruction{Opcode: OpCommit}); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if pager.InTransaction() {
		t.Error("transaction should not be active after commit")
	}
	txnFlowExpectSavepoints(t, pager, 0)
}

// Test schema cookie verification
func TestSchemaCookieVerification(t *testing.T) {
	t.Parallel()
	v := New()
	pager := NewMockPager()
	v.Ctx = &VDBEContext{Pager: pager}

	// Set initial schema cookie
	schemaVersion := uint32(5)
	pager.SetCookie(0, 0, schemaVersion)

	// Verify cookie - should succeed
	instr := &Instruction{
		Opcode: OpVerifyCookie,
		P1:     0, // db index
		P2:     0, // cookie type
		P3:     int(schemaVersion),
	}
	if err := v.execVerifyCookie(instr); err != nil {
		t.Errorf("verification should succeed: %v", err)
	}

	// Change schema cookie
	pager.SetCookie(0, 0, schemaVersion+1)

	// Verify with old version - should fail
	if err := v.execVerifyCookie(instr); err == nil {
		t.Error("verification should fail with changed schema")
	}

	// Update to new version
	instr = &Instruction{
		Opcode: OpSetCookie,
		P1:     0,
		P2:     0,
		P3:     int(schemaVersion + 2),
	}
	if err := v.execSetCookie(instr); err != nil {
		t.Errorf("failed to set cookie: %v", err)
	}

	// Verify with new version
	instr = &Instruction{
		Opcode: OpVerifyCookie,
		P1:     0,
		P2:     0,
		P3:     int(schemaVersion + 2),
	}
	if err := v.execVerifyCookie(instr); err != nil {
		t.Errorf("verification should succeed with updated cookie: %v", err)
	}
}

// Test error cases
func TestTransactionErrors(t *testing.T) {
	t.Parallel()
	// Test without pager context
	t.Run("no pager context", func(t *testing.T) {
		t.Parallel()
		v := New()
		v.Ctx = &VDBEContext{Pager: nil}

		instr := &Instruction{Opcode: OpTransaction, P1: 0, P2: 0, P3: 0}
		if err := v.execTransaction(instr); err == nil {
			t.Error("expected error with no pager")
		}
	})

	// Test savepoint without proper interface
	t.Run("pager without savepoint support", func(t *testing.T) {
		t.Parallel()
		v := New()
		// Use a pager that doesn't implement SavepointPagerInterface
		basicPager := NewMockPager()
		v.Ctx = &VDBEContext{Pager: PagerInterface(basicPager)}

		instr := &Instruction{
			Opcode: OpSavepoint,
			P1:     0,
			P4:     P4Union{Z: "sp1"},
			P4Type: P4Static,
		}
		// This should succeed because MockPager implements SavepointPagerInterface
		basicPager.BeginWrite()
		if err := v.execSavepoint(instr); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// Test cookie operations without proper interface
	t.Run("pager without cookie support", func(t *testing.T) {
		t.Parallel()
		v := New()
		basicPager := NewMockPager()
		v.Ctx = &VDBEContext{Pager: PagerInterface(basicPager)}

		instr := &Instruction{Opcode: OpVerifyCookie, P1: 0, P2: 0, P3: 0}
		// This should succeed because MockPager implements CookiePagerInterface
		if err := v.execVerifyCookie(instr); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && (s[:len(substr)] == substr || contains(s[1:], substr))))
}
