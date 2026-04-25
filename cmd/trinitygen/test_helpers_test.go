// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package main

import (
	"strings"
	"testing"
)

func assertContains(t *testing.T, out, want string) {
	t.Helper()
	if !strings.Contains(out, want) {
		t.Fatalf("expected %q in output, got: %s", want, out)
	}
}

func assertNotContains(t *testing.T, out, want string) {
	t.Helper()
	if strings.Contains(out, want) {
		t.Fatalf("did not expect %q in output, got: %s", want, out)
	}
}
