// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"os"
	"testing"
)

// TestMain provides global test setup and teardown for the driver package.
func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
