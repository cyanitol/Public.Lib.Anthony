// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package planner_test

import (
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/planner"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

// expandView calls ExpandViewsInSelect and fails on error.
func expandView(t *testing.T, stmt *parser.SelectStmt, s *schema.Schema) *parser.SelectStmt {
	t.Helper()
	result, err := planner.ExpandViewsInSelect(stmt, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return result
}

// optimizeSubqueryOK calls OptimizeSubquery and asserts no error and non-nil result.
func optimizeSubqueryOK(t *testing.T, opt *planner.SubqueryOptimizer, info *planner.SubqueryInfo, parent *planner.WhereInfo) *planner.WhereInfo {
	t.Helper()
	result, err := opt.OptimizeSubquery(info, parent)
	if err != nil {
		t.Fatalf("OptimizeSubquery returned error: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result from OptimizeSubquery")
	}
	return result
}

// materializeOK calls MaterializeSubquery and asserts no error and non-nil result.
func materializeOK(t *testing.T, opt *planner.SubqueryOptimizer, info *planner.SubqueryInfo) *planner.SubqueryInfo {
	t.Helper()
	m, err := opt.MaterializeSubquery(info)
	if err != nil {
		t.Fatalf("MaterializeSubquery error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil materialized info")
	}
	return m
}
