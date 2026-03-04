// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package parser

// parseReindex parses a REINDEX statement.
// Syntax:
//   REINDEX
//   REINDEX collation-name
//   REINDEX table-or-index-name
//   REINDEX database-name.table-or-index-name
func (p *Parser) parseReindex() (*ReindexStmt, error) {
	stmt := &ReindexStmt{}

	// Check if there's a name (table, index, or collation name)
	if p.check(TK_ID) {
		// Could be:
		// 1. schema.name (e.g., main.t1)
		// 2. just name (e.g., t1 or i1)
		name := p.consumeSchemaIdentifier()

		// Check if there's a dot (schema qualifier)
		if p.match(TK_DOT) {
			// Previous identifier was the schema name
			stmt.Schema = name
			// Next should be the table/index name
			if !p.check(TK_ID) {
				return nil, p.error("expected table or index name after schema qualifier")
			}
			stmt.Name = p.consumeSchemaIdentifier()
		} else {
			// Just a table or index name (no schema qualifier)
			stmt.Name = name
		}
	}
	// If no name is provided, it's REINDEX (all databases)

	return stmt, nil
}
