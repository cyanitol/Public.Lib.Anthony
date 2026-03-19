// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
)

func resolveExprCollation(expr parser.Expression, table *schema.Table) string {
	switch e := expr.(type) {
	case *parser.CollateExpr:
		return strings.ToUpper(e.Collation)
	case *parser.ParenExpr:
		return resolveExprCollation(e.Expr, table)
	case *parser.IdentExpr:
		if table == nil {
			return ""
		}
		colIdx := table.GetColumnIndex(e.Name)
		if colIdx < 0 {
			return ""
		}
		return table.Columns[colIdx].Collation
	default:
		return ""
	}
}
