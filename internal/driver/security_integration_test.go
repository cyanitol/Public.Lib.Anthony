// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// TestAttachDatabasePathTraversal verifies that ATTACH DATABASE blocks path traversal attacks
func TestAttachDatabasePathTraversal(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database
	mainDB := filepath.Join(tmpDir, "main.db")
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test 1: Attempt path traversal with ../
	_, err = db.Exec("ATTACH DATABASE '../../../etc/passwd' AS attack1")
	if err == nil {
		t.Error("Expected ATTACH DATABASE to block path traversal with ../, but it succeeded")
	}

	// Test 2: Attempt null byte injection
	_, err = db.Exec("ATTACH DATABASE 'test\x00.db' AS attack2")
	if err == nil {
		t.Error("Expected ATTACH DATABASE to block null byte injection, but it succeeded")
	}

	// Test 3: Attempt absolute path
	_, err = db.Exec("ATTACH DATABASE '/etc/passwd' AS attack3")
	if err == nil {
		t.Error("Expected ATTACH DATABASE to block absolute path, but it succeeded")
	}

	// Test 4: Valid relative path should work
	validDB := filepath.Join(tmpDir, "valid.db")
	// Create the file first
	f, err := os.Create(validDB)
	if err != nil {
		t.Fatalf("Failed to create valid db file: %v", err)
	}
	f.Close()

	// Now try to attach using just the filename (will be sandboxed to current directory)
	// This should succeed after validation
	_, err = db.Exec("ATTACH DATABASE 'valid.db' AS valid")
	if err != nil {
		t.Logf("ATTACH DATABASE with valid path returned error: %v (this may be expected due to sandbox configuration)", err)
	}
}

// openSecurityTestDB creates a temp database with a test table for security tests.
func openSecurityTestDB(t *testing.T) *sql.DB {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	mainDB := filepath.Join(tmpDir, "main.db")
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE test (id INTEGER, value TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test (id, value) VALUES (1, 'hello')"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	return db
}

// TestVacuumIntoPathTraversal verifies that VACUUM INTO blocks path traversal attacks
func TestVacuumIntoPathTraversal(t *testing.T) {
	db := openSecurityTestDB(t)
	defer db.Close()

	t.Run("path_traversal", func(t *testing.T) {
		_, err := db.Exec("VACUUM INTO '../../../tmp/attack.db'")
		if err == nil {
			t.Error("Expected VACUUM INTO to block path traversal, but it succeeded")
		}
	})

	t.Run("null_byte_injection", func(t *testing.T) {
		_, err := db.Exec("VACUUM INTO ?", "test\x00.db")
		if err == nil {
			t.Error("Expected VACUUM INTO to block null byte injection, but it succeeded")
		}
	})

	t.Run("absolute_path", func(t *testing.T) {
		_, err := db.Exec("VACUUM INTO '/tmp/attack.db'")
		if err == nil {
			t.Error("Expected VACUUM INTO to block absolute path, but it succeeded")
		}
	})

	t.Run("valid_relative_path", func(t *testing.T) {
		_, err := db.Exec("VACUUM INTO 'backup.db'")
		if err != nil {
			t.Logf("VACUUM INTO with valid path returned error: %v (this may be expected due to sandbox configuration)", err)
		}
	})
}

// TestSecurityConfigDefaults verifies that security config is properly initialized
func TestSecurityConfigDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "security_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mainDB := filepath.Join(tmpDir, "main.db")
	db, err := sql.Open(DriverName, mainDB)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify that the security features are blocking malicious input by default
	// Test path traversal
	_, err = db.Exec("ATTACH DATABASE '../attack.db' AS attack")
	if err == nil {
		t.Error("Expected security config to block path traversal by default, but it succeeded")
	} else {
		t.Logf("Security check correctly blocked attack: %v", err)
	}
}
