// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)

package main

// TestSpec describes a single test case to be generated.
type TestSpec struct {
	Name     string        // t.Run name (must contain REQ-MODULE-NNN_desc)
	Setup    []string      // DDL/DML to run before the test
	Exec     string        // statement to execute (non-query)
	Query    string        // SELECT to run and compare results
	Args     []interface{} // bind parameters
	WantRows [][]Value     // expected rows from query
	WantErr  bool          // expect an error
	ErrLike  string        // error message substring
	Skip     string        // skip reason (empty = don't skip)
}

// Value wraps an expected value for code generation.
type Value struct {
	GoLiteral string // literal Go code, e.g. `int64(42)`, `"hello"`, `nil`
}

// ModuleSpec describes a test module (maps to one _test.go file).
type ModuleSpec struct {
	Module      string     // short name, e.g. "core", "types"
	TestFunc    string     // TestTrinity_Core
	BuildFunc   string     // buildTrinityCoreTests
	SubFuncs    []SubFunc  // sub-generator functions
	Comment     string     // doc comment for TestFunc
	Tests       []TestSpec // all test cases (if generated inline)
	NeedsFmt    bool       // whether "fmt" import is needed
	NeedsMath   bool       // whether "math" import is needed
	NeedsString bool       // whether "strings" import is needed
}

// SubFunc describes a sub-generator function called from buildTrinityXTests.
type SubFunc struct {
	Name    string // function name
	Comment string // doc comment
}
