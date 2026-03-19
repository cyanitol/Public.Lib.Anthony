// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package example_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestExamples builds and runs each example subdirectory, verifying that
// they exit successfully. This catches compilation errors and runtime panics
// in the example code.
func TestExamples(t *testing.T) {
	examples := []string{
		"basic",
		"json_tvf",
		"window",
		"cte",
		"triggers",
		"views",
		"foreign_keys",
		"compound",
		"datetime",
	}

	exampleDir := exampleRoot(t)

	for _, name := range examples {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(exampleDir, name)
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				t.Skipf("example dir %s not found", dir)
			}

			cmd := exec.Command("go", "run", ".")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOFLAGS=")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("example %s failed:\n%s\nerror: %v", name, out, err)
			}
			t.Logf("example %s output (%d bytes)", name, len(out))
		})
	}
}

// exampleRoot returns the absolute path to the example/ directory.
func exampleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Dir(file)
}
