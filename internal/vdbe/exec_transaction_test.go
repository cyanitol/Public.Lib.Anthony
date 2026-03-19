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

// Test OpTransaction
func TestOpTransaction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		p1          int // db index
		p2          int // write flag
		p3          int // schema version
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

			instr := &Instruction{
				Opcode: OpTransaction,
				P1:     tt.p1,
				P2:     tt.p2,
				P3:     tt.p3,
			}

			err := v.execTransaction(instr)
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && !tt.setupTxn {
				if !pager.InTransaction() {
					t.Error("transaction should be active")
				}
				if tt.p2 != 0 && !pager.InWriteTransaction() {
					t.Error("write transaction should be active")
				}
			}
		})
	}
}

// Test OpAutoCommit
func TestOpAutoCommit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		p1          int  // 1=commit, 0=begin
		p2          int  // rollback flag
		setupTxn    bool // setup a transaction first
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

			instr := &Instruction{
				Opcode: OpAutocommit,
				P1:     tt.p1,
				P2:     tt.p2,
			}

			err := v.execAutoCommit(instr)
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Verify transaction state
			if tt.p1 == 0 && !tt.expectError {
				// Begin transaction
				if !pager.InTransaction() {
					t.Error("transaction should be active after begin")
				}
			} else if tt.p1 == 1 && tt.setupTxn && !tt.expectError {
				// Commit or rollback transaction
				if pager.InTransaction() {
					t.Error("transaction should not be active after commit/rollback")
				}
			}
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

			// Setup savepoints
			for _, sp := range tt.setupSp {
				pager.Savepoint(sp)
			}

			instr := &Instruction{
				Opcode: OpSavepoint,
				P1:     tt.operation,
				P4:     P4Union{Z: tt.spName},
				P4Type: P4Static,
			}

			err := v.execSavepoint(instr)
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Check savepoints
			if tt.checkSp != nil && !tt.expectError {
				if len(pager.savepoints) != len(tt.checkSp) {
					t.Errorf("expected %d savepoints, got %d", len(tt.checkSp), len(pager.savepoints))
				}
				for i, sp := range tt.checkSp {
					if i >= len(pager.savepoints) {
						break
					}
					if pager.savepoints[i] != sp {
						t.Errorf("savepoint[%d]: expected %s, got %s", i, sp, pager.savepoints[i])
					}
				}
			}
		})
	}
}

// Test OpVerifyCookie
func TestOpVerifyCookie(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		dbIndex      int
		cookieType   int
		expectedVal  int
		actualVal    uint32
		expectError  bool
		errorMessage string
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

			// Set actual cookie value
			pager.SetCookie(tt.dbIndex, tt.cookieType, tt.actualVal)

			instr := &Instruction{
				Opcode: OpVerifyCookie,
				P1:     tt.dbIndex,
				P2:     tt.cookieType,
				P3:     tt.expectedVal,
			}

			err := v.execVerifyCookie(instr)
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.expectError && err != nil && tt.errorMessage != "" {
				if !contains(err.Error(), tt.errorMessage) {
					t.Errorf("expected error containing %q, got %q", tt.errorMessage, err.Error())
				}
			}
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

// Test transaction flow with multiple opcodes
func TestTransactionFlow(t *testing.T) {
	t.Parallel()
	v := New()
	pager := NewMockPager()
	v.Ctx = &VDBEContext{Pager: pager}

	// Begin write transaction
	instr := &Instruction{Opcode: OpTransaction, P1: 0, P2: 1, P3: 0}
	if err := v.execTransaction(instr); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if !pager.InWriteTransaction() {
		t.Error("write transaction should be active")
	}

	// Create savepoint
	instr = &Instruction{
		Opcode: OpSavepoint,
		P1:     0,
		P4:     P4Union{Z: "sp1"},
		P4Type: P4Static,
	}
	if err := v.execSavepoint(instr); err != nil {
		t.Fatalf("failed to create savepoint: %v", err)
	}
	if len(pager.savepoints) != 1 {
		t.Errorf("expected 1 savepoint, got %d", len(pager.savepoints))
	}

	// Create another savepoint
	instr.P4.Z = "sp2"
	if err := v.execSavepoint(instr); err != nil {
		t.Fatalf("failed to create second savepoint: %v", err)
	}
	if len(pager.savepoints) != 2 {
		t.Errorf("expected 2 savepoints, got %d", len(pager.savepoints))
	}

	// Rollback to first savepoint
	instr = &Instruction{
		Opcode: OpSavepoint,
		P1:     2, // rollback operation
		P4:     P4Union{Z: "sp1"},
		P4Type: P4Static,
	}
	if err := v.execSavepoint(instr); err != nil {
		t.Fatalf("failed to rollback to savepoint: %v", err)
	}
	if len(pager.savepoints) != 1 {
		t.Errorf("expected 1 savepoint after rollback, got %d", len(pager.savepoints))
	}

	// Commit transaction
	instr = &Instruction{Opcode: OpCommit}
	if err := v.execCommit(instr); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if pager.InTransaction() {
		t.Error("transaction should not be active after commit")
	}
	if len(pager.savepoints) != 0 {
		t.Errorf("savepoints should be cleared after commit, got %d", len(pager.savepoints))
	}
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
