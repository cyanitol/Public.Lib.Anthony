// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

func TestCompatibilityModeQueryLocking(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		dsnSuffix   string
		shouldBlock bool
	}{
		{name: "hard compat blocks query behind write mutex", dsnSuffix: "", shouldBlock: true},
		{name: "extended allows query past write mutex", dsnSuffix: "?compat_mode=extended", shouldBlock: false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := setupOCCDatabase(t, "compat_query.db")
			conn := openRawConnDSN(t, dbPath+tt.dsnSuffix)
			defer conn.Close()

			stmt, err := conn.PrepareContext(context.Background(), "SELECT 1")
			if err != nil {
				t.Fatalf("PrepareContext: %v", err)
			}
			defer stmt.Close()

			conn.writeMu.Lock()
			defer conn.writeMu.Unlock()

			done := make(chan error, 1)
			go func() {
				rows, err := stmt.(*Stmt).QueryContext(context.Background(), nil)
				if err == nil {
					_ = rows.Close()
				}
				done <- err
			}()

			assertCompatBlocking(t, done, tt.shouldBlock)
		})
	}
}

func TestCompatibilityModeReadOnlyBeginLocking(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		dsnSuffix   string
		shouldBlock bool
	}{
		{name: "hard compat blocks readonly begin behind write mutex", dsnSuffix: "", shouldBlock: true},
		{name: "extended allows readonly begin past write mutex", dsnSuffix: "?compat_mode=extended", shouldBlock: false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := setupOCCDatabase(t, "compat_readonly.db")
			conn := openRawConnDSN(t, dbPath+tt.dsnSuffix)
			defer conn.Close()

			conn.writeMu.Lock()
			defer conn.writeMu.Unlock()

			done := make(chan error, 1)
			go func() {
				tx, err := conn.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true})
				if err == nil {
					err = tx.Rollback()
				}
				done <- err
			}()

			assertCompatBlocking(t, done, tt.shouldBlock)
		})
	}
}

func assertCompatBlocking(t *testing.T, done <-chan error, shouldBlock bool) {
	t.Helper()

	select {
	case err := <-done:
		if shouldBlock {
			t.Fatal("operation completed while write mutex was held")
		}
		if err != nil {
			t.Fatalf("operation returned error: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		if !shouldBlock {
			t.Fatal("operation blocked behind write mutex in extended mode")
		}
	}
}
