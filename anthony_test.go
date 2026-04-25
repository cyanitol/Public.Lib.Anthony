// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package anthony

import (
	"testing"

	sqliteDriver "github.com/cyanitol/Public.Lib.Anthony/internal/driver"
)

func TestOpenWithCompatibilityRejectsInvalidMode(t *testing.T) {
	if _, err := OpenWithCompatibility(":memory:", "invalid-mode"); err == nil {
		t.Fatal("OpenWithCompatibility() error = nil, want validation error")
	}
}

func TestOpenWithCompatibilityFormatsExtendedDSN(t *testing.T) {
	db, err := OpenWithCompatibility(":memory:", string(sqliteDriver.CompatibilityModeExtended))
	if err != nil {
		t.Fatalf("OpenWithCompatibility(): %v", err)
	}
	defer db.Close()
}
