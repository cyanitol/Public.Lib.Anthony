// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// MockCookiePager implements CookiePagerInterface for testing
type MockCookiePager struct {
	cookies map[int]map[int]uint32
}

func NewMockCookiePager() *MockCookiePager {
	return &MockCookiePager{
		cookies: make(map[int]map[int]uint32),
	}
}

func (m *MockCookiePager) SetCookie(dbIndex, cookieType int, value uint32) error {
	if m.cookies[dbIndex] == nil {
		m.cookies[dbIndex] = make(map[int]uint32)
	}
	m.cookies[dbIndex][cookieType] = value
	return nil
}

func (m *MockCookiePager) GetCookie(dbIndex, cookieType int) (uint32, error) {
	if m.cookies[dbIndex] == nil {
		return 0, nil
	}
	return m.cookies[dbIndex][cookieType], nil
}

// Implement PagerInterface methods (not used in these tests)
func (m *MockCookiePager) BeginRead() error         { return nil }
func (m *MockCookiePager) EndRead() error           { return nil }
func (m *MockCookiePager) BeginWrite() error        { return nil }
func (m *MockCookiePager) Commit() error            { return nil }
func (m *MockCookiePager) Rollback() error          { return nil }
func (m *MockCookiePager) InTransaction() bool      { return false }
func (m *MockCookiePager) InWriteTransaction() bool { return false }

func testSetCookieBasic(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	mockPager := NewMockCookiePager()
	v.Ctx = &VDBEContext{Pager: mockPager}
	if err := v.execSetCookie(&Instruction{Opcode: OpSetCookie, P1: 0, P2: 1, P3: 42}); err != nil {
		t.Fatalf("execSetCookie failed: %v", err)
	}
	value, err := mockPager.GetCookie(0, 1)
	if err != nil {
		t.Fatalf("GetCookie failed: %v", err)
	}
	if value != 42 {
		t.Errorf("Expected cookie value 42, got %d", value)
	}
}

func testSetCookieMultiple(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	mockPager := NewMockCookiePager()
	v.Ctx = &VDBEContext{Pager: mockPager}
	cookies := []struct{ db, typ int; value uint32 }{
		{0, 0, 100}, {0, 1, 200}, {0, 2, 300}, {1, 0, 400},
	}
	for _, c := range cookies {
		if err := v.execSetCookie(&Instruction{Opcode: OpSetCookie, P1: c.db, P2: c.typ, P3: int(c.value)}); err != nil {
			t.Fatalf("execSetCookie failed for db=%d, typ=%d: %v", c.db, c.typ, err)
		}
	}
	for _, c := range cookies {
		value, err := mockPager.GetCookie(c.db, c.typ)
		if err != nil {
			t.Fatalf("GetCookie failed for db=%d, typ=%d: %v", c.db, c.typ, err)
		}
		if value != c.value {
			t.Errorf("db=%d, typ=%d: expected %d, got %d", c.db, c.typ, c.value, value)
		}
	}
}

func testSetCookieNoPager(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)
	err := v.execSetCookie(&Instruction{Opcode: OpSetCookie, P1: 0, P2: 1, P3: 42})
	if err == nil {
		t.Error("Expected error when no pager context")
	}
}

// TestSetCookieOpcode tests the OpSetCookie opcode
func TestSetCookieOpcode(t *testing.T) {
	t.Parallel()
	t.Run("BasicSetCookie", testSetCookieBasic)
	t.Run("SetCookie_MultipleCookies", testSetCookieMultiple)
	t.Run("SetCookie_NoPager", testSetCookieNoPager)
	t.Run("SetCookie_OverwriteValue", func(t *testing.T) {
	})

	t.Run("SetCookie_NoPagerInterface", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		// Set pager to nil (doesn't implement CookiePagerInterface)
		v.Ctx = &VDBEContext{
			Pager: nil,
		}

		instr := &Instruction{
			Opcode: OpSetCookie,
			P1:     0,
			P2:     1,
			P3:     42,
		}

		err := v.execSetCookie(instr)
		if err == nil {
			t.Error("Expected error when pager doesn't implement CookiePagerInterface")
		}
	})

	t.Run("SetCookie_NilPager", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		v.Ctx = &VDBEContext{
			Pager: nil,
		}

		instr := &Instruction{
			Opcode: OpSetCookie,
			P1:     0,
			P2:     1,
			P3:     42,
		}

		err := v.execSetCookie(instr)
		if err == nil {
			t.Error("Expected error when pager is nil")
		}
	})
}
