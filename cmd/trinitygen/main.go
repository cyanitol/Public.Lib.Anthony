// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

// trinitygen generates Trinity-style _test.go files for the SQLite driver test suite.
//
// Usage:
//
//	trinitygen [-module=core] [-dry-run] [-report] [-output-dir=path]
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	module := flag.String("module", "", "generate only this module (default: all)")
	dryRun := flag.Bool("dry-run", false, "print generated files to stdout")
	report := flag.Bool("report", false, "print module registry summary")
	outputDir := flag.String("output-dir", "internal/driver", "output directory")
	flag.Parse()

	if *report {
		reportModules()
		return
	}

	if err := run(*module, *outputDir, *dryRun); err != nil {
		fmt.Fprintf(os.Stderr, "trinitygen: %v\n", err)
		os.Exit(1)
	}
}

func run(module, outputDir string, dryRun bool) error {
	if module != "" {
		return generateModule(module, outputDir, dryRun)
	}
	return generateAll(outputDir, dryRun)
}

func generateModule(name, outputDir string, dryRun bool) error {
	spec := lookupModule(name)
	if spec == nil {
		return fmt.Errorf("unknown module %q; known: %v", name, moduleNames())
	}
	return emitFile(outputDir, *spec, dryRun)
}

func generateAll(outputDir string, dryRun bool) error {
	for _, spec := range moduleRegistry() {
		if err := emitFile(outputDir, spec, dryRun); err != nil {
			return err
		}
	}
	fmt.Printf("generated %d module files in %s\n", len(moduleRegistry()), outputDir)
	return nil
}
