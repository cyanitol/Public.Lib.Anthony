// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package parser

// parseAnalyze parses an ANALYZE statement.
// Syntax:
//
//	ANALYZE
//	ANALYZE schema-name
//	ANALYZE table-or-index-name
//	ANALYZE schema-name.table-or-index-name
func (p *Parser) parseAnalyze() (*AnalyzeStmt, error) {
	stmt := &AnalyzeStmt{}

	// Check if there's a name (schema, table, or index name)
	if p.check(TK_ID) {
		name := p.consumeSchemaIdentifier()

		// Check if there's a dot (schema qualifier)
		if p.match(TK_DOT) {
			stmt.Schema = name
			if !p.check(TK_ID) {
				return nil, p.error("expected table or index name after schema qualifier")
			}
			stmt.Name = p.consumeSchemaIdentifier()
		} else {
			stmt.Name = name
		}
	}

	return stmt, nil
}
